import fs from "fs";
import path from "path";
import os from "os";
import crypto from "crypto";
import express from "express";
import multer from "multer";
import cors from "cors";
import { fileURLToPath } from "url";
import { createClient } from "@supabase/supabase-js";
import ffmpeg from "ffmpeg-static";
import { spawn } from "child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));

const MAX_UPLOAD_BYTES = Number(process.env.MAX_UPLOAD_BYTES || 50 * 1024 * 1024);
const upload = multer({
  dest: path.join(os.tmpdir(), "calibrate_uploads"),
  limits: { fileSize: MAX_UPLOAD_BYTES },
});

const PORT = process.env.PORT || 3000;

function cleanEnv(v) {
  return String(v || "").trim().replace(/^"+|"+$/g, "");
}

const SUPABASE_URL = cleanEnv(
  process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || ""
);
const SUPABASE_ANON_KEY = cleanEnv(
  process.env.SUPABASE_ANON_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || ""
);
const SUPABASE_SERVICE_ROLE_KEY = cleanEnv(process.env.SUPABASE_SERVICE_ROLE_KEY || "");
const STORAGE_BUCKET = "uploads";

const ASSEMBLYAI_API_KEY = process.env.ASSEMBLYAI_API_KEY || "";
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || "";

if (!SUPABASE_URL || !SUPABASE_ANON_KEY || !SUPABASE_SERVICE_ROLE_KEY) {
  console.warn(
    "⚠ Missing SUPABASE_URL / SUPABASE_ANON_KEY / SUPABASE_SERVICE_ROLE_KEY env vars"
  );
}

const supabaseAnon = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});
const supabaseAdmin = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});
console.log("[supabase_env]", {
  url: Boolean(SUPABASE_URL),
  anonLen: SUPABASE_ANON_KEY.length,
  serviceLen: SUPABASE_SERVICE_ROLE_KEY.length,
});

function sha1(buf) {
  return crypto.createHash("sha1").update(buf).digest("hex");
}

async function uploadJsonArtifact(bucket, path, payload) {
  try {
    const body = typeof payload === "string" ? payload : JSON.stringify(payload ?? {});
    const { error } = await supabaseAdmin.storage.from(bucket).upload(path, body, {
      contentType: "application/json",
      upsert: true,
    });
    if (error) {
      console.warn("[artifact_upload_failed]", { path });
    }
  } catch {
    console.warn("[artifact_upload_failed]", { path });
  }
}

function safeJsonParse(s) {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

function nowIso() {
  return new Date().toISOString();
}

const runJobQueue = [];
let runJobWorking = false;

function truncateErrorText(err, maxLen = 300) {
  const msg = String(err?.message || err || "");
  return msg.length > maxLen ? msg.slice(0, maxLen) : msg;
}

async function updateRunStatus(runId, userId, updates) {
  const { error } = await supabaseAdmin
    .from("runs")
    .update(updates)
    .eq("id", runId)
    .eq("user_id", userId);
  if (error) {
    console.warn("[run_status_update_failed]", { run_id: runId });
  }
}

async function processRunJob(job) {
  const {
    runId,
    userId,
    filePath,
    originalname,
    mimetype,
    scenario,
    context,
    entityId,
  } = job;

  let inputPath = filePath;
  const cleanupPaths = [filePath];
  try {
    await updateRunStatus(runId, userId, { progress_step: "transcribing" });

    const isMp4 =
      String(mimetype || "").includes("video") ||
      String(originalname || "").toLowerCase().endsWith(".mp4");
    if (isMp4) {
      const audioPath = await extractAudioFromMp4(filePath);
      inputPath = audioPath;
      cleanupPaths.push(audioPath);
    }

    const { transcriptText, transcriptJson, transcriptLines } = await assemblyTranscribe(inputPath);
    const artifactRoot = `artifacts/${runId}`;
    await uploadJsonArtifact(
      STORAGE_BUCKET,
      `${artifactRoot}/assembly_transcript_json.json`,
      transcriptJson
    );
    await uploadJsonArtifact(
      STORAGE_BUCKET,
      `${artifactRoot}/transcript_lines.json`,
      transcriptLines
    );

    const rawLines = transcriptLines || [];
    const finalTranscriptLines = finalizeTranscriptLines(rawLines);
    const transcriptHash = sha1(Buffer.from(transcriptText, "utf-8"));

    await updateRunStatus(runId, userId, { progress_step: "analyzing" });

    let entityAggregate = null;
    if (entityId) {
      entityAggregate = await buildEntityAggregate(userId, entityId);
    }

    const prompt = buildAnalysisPrompt({
      transcript: transcriptText,
      transcriptLines: finalTranscriptLines,
      context,
      scenario,
      entityAggregate,
    });

    const deepseekResult = await deepseekJSONWithRaw(prompt.system, prompt.user, 0.25);
    await uploadJsonArtifact(
      STORAGE_BUCKET,
      `${artifactRoot}/deepseek_raw.json`,
      { raw: deepseekResult.raw }
    );
    const rawAnalysis = deepseekResult.parsed;
    const outcomeLabel = await inferOutcomeLabel(rawAnalysis);
    let { fixedJson: finalAnalysis, errors } = validateAndCoerceAnalysisJson(
      rawAnalysis,
      finalTranscriptLines,
      outcomeLabel
    );
    let evidenceCount = finalAnalysis?.call_result?.evidence?.length || 0;

    if (errors.length > 0 || evidenceCount !== 36) {
      const errorLines = errors.slice(0, 10);
      if (evidenceCount !== 36) {
        errorLines.push(`evidence_count=${evidenceCount} (must be 36)`);
      }
      const numberedTranscript = buildNumberedTranscriptLines(finalTranscriptLines).join("\n");
      const repairSystem = `
You are a strict JSON fixer. Return a corrected FULL analysis JSON only.
Follow the required keys and types, and ensure evidence has exactly 36 items.
Each evidence item must reference a valid transcript line and include an exact substring quote from that line (keep quotes short).
`;
      const repairUser = `
Errors:
${errorLines.map((e) => `- ${e}`).join("\n")}

Transcript:
${numberedTranscript}

Return the corrected JSON only.
`;
      const repaired = await deepseekJSON(repairSystem.trim(), repairUser.trim(), 0.2);
      const repairedOutcome = await inferOutcomeLabel(repaired) || outcomeLabel;
      const repairedResult = validateAndCoerceAnalysisJson(
        repaired,
        finalTranscriptLines,
        repairedOutcome
      );
      finalAnalysis = repairedResult.fixedJson;
      evidenceCount = finalAnalysis?.call_result?.evidence?.length || 0;
      if (repairedResult.errors.length > 0 || evidenceCount !== 36) {
        finalAnalysis = buildFallbackAnalysisJson(repairedOutcome, finalTranscriptLines);
      }
    }

    const outcomeLabelFinal =
      finalAnalysis?.call_result?.label || outcomeLabel || "Unknown";

    await updateRunStatus(runId, userId, { progress_step: "saving" });

    await uploadJsonArtifact(
      STORAGE_BUCKET,
      `${artifactRoot}/analysis_final.json`,
      finalAnalysis
    );

    await updateRunStatus(runId, userId, {
      status: "complete",
      progress_step: null,
      error_text: null,
      transcript_text: transcriptText,
      transcript_lines: finalTranscriptLines,
      transcript_json: transcriptJson,
      transcript_hash: transcriptHash,
      outcome_label: outcomeLabelFinal,
      analysis_json: finalAnalysis,
    });
  } catch (err) {
    await updateRunStatus(runId, userId, {
      status: "failed",
      progress_step: null,
      error_text: truncateErrorText(err),
    });
  } finally {
    cleanupPaths.forEach((p) => {
      try { fs.unlinkSync(p); } catch {}
    });
  }
}

async function processRunJobQueue() {
  if (runJobWorking) return;
  runJobWorking = true;
  try {
    while (runJobQueue.length) {
      const job = runJobQueue.shift();
      if (!job) continue;
      await processRunJob(job);
    }
  } finally {
    runJobWorking = false;
  }
}

function enqueueRunJob(job) {
  runJobQueue.push(job);
  processRunJobQueue();
}

async function requireUser(req, res) {
  if (
    process.env.NODE_ENV === "test"
    && process.env.SMOKE_BYPASS_USER_ID
    && process.env.SMOKE_BYPASS_TOKEN
    && req.headers["x-smoke-bypass"] === process.env.SMOKE_BYPASS_TOKEN
  ) {
    return { id: process.env.SMOKE_BYPASS_USER_ID };
  }

  const user = await getUserFromReq(req);
  if (!user) {
    res.status(401).json({ error: "Unauthorized" });
    return null;
  }
  return user;
}

function handleMissingUserId(res, table, error) {
  const msg = String(error?.message || "");
  if (msg.includes("user_id") && msg.toLowerCase().includes("column")) {
    res.status(500).json({ error: `Missing user_id column on ${table}` });
    return true;
  }
  return false;
}

async function getUserFromReq(req) {
  const authHeader =
    (req.headers && (req.headers.authorization || req.headers.Authorization)) ||
    (typeof req.get === "function" ? (req.get("authorization") || req.get("Authorization")) : "") ||
    "";
  const token = authHeader.toLowerCase().startsWith("bearer ")
    ? authHeader.slice(7).trim()
    : authHeader.trim();
  if (!token) return null;

  const { data, error } = await supabaseAnon.auth.getUser(token);
  if (!error && data?.user) return data.user;
  if (error) {
    console.warn("[auth_failed]", {
      step: "supabaseAnon.auth.getUser",
      msg: String(error?.message || error || ""),
    });
  }

  async function getUserViaRest(tokenValue) {
    const url = cleanEnv(
      process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || ""
    ).replace(/\/+$/, "");
    const apikey = cleanEnv(
      process.env.SUPABASE_ANON_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || ""
    );
    const res = await fetch(url + "/auth/v1/user", {
      method: "GET",
      headers: {
        apikey,
        Authorization: "Bearer " + tokenValue,
      },
    });
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      return { user: null, err: { status: res.status, body: txt.slice(0, 200) } };
    }
    const json = await res.json().catch(() => ({}));
    return { user: json, err: null };
  }

  const rest = await getUserViaRest(token);
  if (rest?.user?.id) return rest.user;
  if (rest?.err) {
    console.warn("[auth_failed]", { step: "rest_auth_v1_user", ...rest.err });
  }
  return null;
}

app.get("/api/config", (req, res) => {
  const supabaseUrl = cleanEnv(
    process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || ""
  );
  const supabaseAnonKey = cleanEnv(
    process.env.SUPABASE_ANON_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || ""
  );
  if (!supabaseUrl || !supabaseAnonKey) {
    return res.status(500).json({
      error: "Missing Supabase env vars",
      supabaseUrlPresent: Boolean(supabaseUrl),
      supabaseAnonKeyPresent: Boolean(supabaseAnonKey),
    });
  }
  res.setHeader("Cache-Control", "no-store");
  return res.json({
    supabaseUrl,
    supabaseAnonKey,
  });
});

app.get("/api/health", (req, res) => {
  res.json({ ok: true });
});

async function assemblyTranscribe(filePath) {
  if (!ASSEMBLYAI_API_KEY) throw new Error("Missing ASSEMBLYAI_API_KEY");

  const audioData = fs.readFileSync(filePath);

  const startTranscript = async (payload) => {
    const tr = await fetch("https://api.assemblyai.com/v2/transcript", {
      method: "POST",
      headers: {
        authorization: ASSEMBLYAI_API_KEY,
        "content-type": "application/json",
      },
      body: JSON.stringify(payload),
    });
    if (!tr.ok) throw new Error(`AssemblyAI transcript start failed: ${await tr.text()}`);
    const trJson = await tr.json();
    return trJson.id;
  };

  const waitTranscript = async (id) => {
    for (let i = 0; i < 120; i++) {
      await new Promise((r) => setTimeout(r, 2500));
      const st = await fetch(`https://api.assemblyai.com/v2/transcript/${id}`, {
        headers: { authorization: ASSEMBLYAI_API_KEY },
      });
      if (!st.ok) throw new Error(`AssemblyAI transcript status failed: ${await st.text()}`);
      const stJson = await st.json();
      if (stJson.status === "completed") return stJson;
      if (stJson.status === "error") throw new Error(`AssemblyAI error: ${stJson.error}`);
    }
    throw new Error("AssemblyAI transcript timed out");
  };

  const buildCandidate = (stJson) => {
    const utterances = stJson?.utterances || [];
    const rawText = stJson?.text || "";
    const cleanedUtterances = utterances
      .map((u) => ({
        speakerKey: u?.speaker == null ? "unknown" : String(u.speaker),
        text: String(u?.text || "").trim(),
      }))
      .filter((u) => u.text.length > 0);
    const speakerWordCounts = new Map();
    let totalWords = 0;
    let switches = 0;
    let prevSpeaker = null;
    cleanedUtterances.forEach((u) => {
      const words = u.text.match(/\S+/g)?.length || 0;
      totalWords += words;
      speakerWordCounts.set(u.speakerKey, (speakerWordCounts.get(u.speakerKey) || 0) + words);
      if (prevSpeaker !== null && u.speakerKey !== prevSpeaker) switches += 1;
      prevSpeaker = u.speakerKey;
    });
    const speakerCount = speakerWordCounts.size;
    const topShare = totalWords
      ? Math.max(...Array.from(speakerWordCounts.values())) / totalWords
      : 1;
    return {
      utterances,
      cleanedUtterances,
      rawText,
      metrics: { speakerCount, totalWords, topShare, switches },
      speechModel: stJson?.speech_model || stJson?.speech_models?.[0] || null,
    };
  };

  const isUnreliable = ({ speakerCount, totalWords, topShare, switches }) => {
    if (totalWords < 80) return false;
    return speakerCount < 2 || topShare > 0.9 || switches < 4;
  };

  const chooseBetter = (a, b) => {
    const aTwo = a.metrics.speakerCount === 2;
    const bTwo = b.metrics.speakerCount === 2;
    if (aTwo !== bTwo) {
      return aTwo ? a : b;
    }
    if (a.metrics.switches !== b.metrics.switches) {
      return a.metrics.switches > b.metrics.switches ? a : b;
    }
    if (a.metrics.topShare !== b.metrics.topShare) {
      return a.metrics.topShare < b.metrics.topShare ? a : b;
    }
    return a;
  };

  // 1) upload file
  const up = await fetch("https://api.assemblyai.com/v2/upload", {
    method: "POST",
    headers: {
      authorization: ASSEMBLYAI_API_KEY,
      "content-type": "application/octet-stream",
    },
    body: audioData,
  });
  if (!up.ok) throw new Error(`AssemblyAI upload failed: ${await up.text()}`);
  const upJson = await up.json();

  // 2) request transcript
  const initialId = await startTranscript({
    audio_url: upJson.upload_url,
    speaker_labels: true,
    speaker_options: { min_speakers: 1, max_speakers: 2 },
    punctuate: true,
    format_text: true,
  });
  const initialJson = await waitTranscript(initialId);
  const initialCandidate = buildCandidate(initialJson);
  const baseBad = isUnreliable(initialCandidate.metrics);

  let usedRetry = false;
  let chosen = initialCandidate;
  let chosenConfig = "base";
  let retryCandidate = null;
  let retryBad = false;

  if (baseBad) {
    usedRetry = true;
    const retryId = await startTranscript({
      audio_url: upJson.upload_url,
      speaker_labels: true,
      speakers_expected: 2,
      punctuate: true,
      format_text: true,
    });
    const retryJson = await waitTranscript(retryId);
    retryCandidate = buildCandidate(retryJson);
    retryBad = isUnreliable(retryCandidate.metrics);
    chosen = chooseBetter(initialCandidate, retryCandidate);
    chosenConfig = chosen === retryCandidate ? "expected_2" : "base";
  }

  const { cleanedUtterances, rawText, metrics } = chosen;
  const transcriptJson = {
    utterances: chosen.utterances,
    diarization_meta: {
      used_retry: usedRetry,
      chosen_config: chosenConfig,
      metrics_base: initialCandidate.metrics,
      metrics_retry: retryCandidate ? retryCandidate.metrics : null,
      speech_model_used: chosen.speechModel,
      low_confidence: baseBad && retryBad,
    },
  };

  const mergeAdjacent = (segments) => {
    if (!segments.length) return [];
    const merged = [segments[0]];
    for (let i = 1; i < segments.length; i++) {
      const prev = merged[merged.length - 1];
      const curr = segments[i];
      if (prev.speakerKey === curr.speakerKey) {
        prev.text = `${prev.text} ${curr.text}`.trim();
        prev.start = prev.start ?? curr.start;
        prev.end = curr.end ?? prev.end;
      } else {
        merged.push(curr);
      }
    }
    return merged;
  };

  const isShortBlip = (text) => {
    const cleaned = String(text || "").replace(/[^\w\s]+/g, " ").trim();
    const words = cleaned ? cleaned.split(/\s+/).filter(Boolean) : [];
    return words.length <= 2;
  };

  const smoothSegments = (segments) => {
    return (segments || [])
      .map((s) => ({
        ...s,
        text: String(s.text || "").replace(/\s+/g, " ").trim(),
      }))
      .filter((s) => s.text.length > 0);
  };

  const computeMetrics = (segments) => {
    const speakerWordCounts = new Map();
    let switches = 0;
    let prevSpeaker = null;
    segments.forEach((s) => {
      const words = s.text.match(/\S+/g)?.length || 0;
      speakerWordCounts.set(s.speakerKey, (speakerWordCounts.get(s.speakerKey) || 0) + words);
      if (prevSpeaker !== null && s.speakerKey !== prevSpeaker) switches += 1;
      prevSpeaker = s.speakerKey;
    });
    const speakerCount = speakerWordCounts.size;
    const totalWords = Array.from(speakerWordCounts.values()).reduce((a, b) => a + b, 0);
    const topShare = totalWords
      ? Math.max(...Array.from(speakerWordCounts.values())) / totalWords
      : 1;
    return { speakerCount, topShare, switches };
  };

  const endsWithTerminal = (text) => /[.!?]["')\]]*\s*$/.test(text);
  const startsWithLower = (text) => /^[a-z]/.test(text);
  const startsWithContinuation = (text) =>
    /^(and|but|or|so|because|that|which)\b/i.test(text);
  const startsWithContinuationToken = (text) => {
    const trimmed = String(text || "").trim();
    if (!trimmed) return false;
    if (/^(and|but|or|so|because|that|which|do|a|an|the)\b/i.test(trimmed)) return true;
    return /^[a-z0-9]/i.test(trimmed);
  };
  const endsWithFragment = (text) => {
    const trimmed = String(text || "").trim();
    if (/[,:;]\s*$/.test(trimmed)) return true;
    if (/[-–—]\s*$/.test(trimmed)) return true;
    if (/\b(no|gonna|to|a|an|the|and|but|so)\.\s*$/i.test(trimmed)) return true;
    if (/\b(to do|plus|because)\.\s*$/i.test(trimmed)) return true;
    const tokens = trimmed.match(/[a-z0-9]+/gi) || [];
    const last = tokens[tokens.length - 1] || "";
    return last.length > 0 && last.length <= 2;
  };

  const stitchSegments = (segments) => {
    let stitched = mergeAdjacent(segments);
    let fragmentsStitchedCount = 0;
    for (let pass = 0; pass < 3; pass++) {
      let changed = false;
      const next = [];
      for (let i = 0; i < stitched.length; i++) {
        const curr = stitched[i];
        const prev = next[next.length - 1];
        const upcoming = stitched[i + 1];
        const prevFragment = prev && endsWithFragment(prev.text);
        if (
          prev
          && prev.speakerKey !== curr.speakerKey
          && (
            prevFragment
            || !endsWithTerminal(prev.text)
            || isShortBlip(prev.text)
          )
          && (
            startsWithLower(curr.text)
            || startsWithContinuation(curr.text)
            || startsWithContinuationToken(curr.text)
            || prevFragment
          )
        ) {
          prev.text = `${prev.text} ${curr.text}`.trim();
          prev.end = curr.end ?? prev.end;
          changed = true;
          fragmentsStitchedCount += 1;
          continue;
        }
        if (
          prev
          && upcoming
          && prev.speakerKey === upcoming.speakerKey
          && curr.speakerKey !== prev.speakerKey
          && isShortBlip(curr.text)
        ) {
          const gapPrev = typeof curr.start === "number" && typeof prev.end === "number"
            ? curr.start - prev.end
            : null;
          const gapNext = typeof upcoming.start === "number" && typeof curr.end === "number"
            ? upcoming.start - curr.end
            : null;
          if (gapPrev == null || gapNext == null || (gapPrev <= 1500 && gapNext <= 1500)) {
            prev.text = `${prev.text} ${curr.text}`.trim();
            prev.end = curr.end ?? prev.end;
            changed = true;
            fragmentsStitchedCount += 1;
            continue;
          }
        }
        next.push(curr);
      }
      stitched = mergeAdjacent(next);
      if (!changed) break;
    }
    return { stitched, fragmentsStitchedCount };
  };

  const rawSegments = (chosen.utterances || [])
    .map((u) => ({
      speakerKey: u?.speaker == null ? "unknown" : String(u.speaker),
      text: String(u?.text || "").trim(),
      start: u?.start,
      end: u?.end,
    }))
    .filter((s) => s.text.length > 0);

  const wordCount = (text) => (String(text || "").match(/[a-z0-9]+/gi) || []).length;
  const isTinySegment = (text) => {
    const cleaned = String(text || "").trim();
    if (!cleaned) return true;
    if (wordCount(cleaned) <= 3) return true;
    if (cleaned.length <= 18) return true;
    if (/^(so|yeah|ok(ay)?|right|sure|alright|perfect)\b/i.test(cleaned)) return true;
    return false;
  };

  const absorbTinyTurns = (segments) => {
    const next = [];
    let tinyAbsorbedCount = 0;
    for (let i = 0; i < segments.length; i++) {
      const curr = segments[i];
      const prev = next[next.length - 1];
      const upcoming = segments[i + 1];
      if (!curr || !curr.text) continue;
      if (isTinySegment(curr.text)) {
        if (prev && upcoming && prev.speakerKey === upcoming.speakerKey) {
          prev.text = `${prev.text} ${curr.text}`.trim();
          prev.end = curr.end ?? prev.end;
          tinyAbsorbedCount += 1;
          continue;
        }
        if (prev && endsWithFragment(prev.text)) {
          prev.text = `${prev.text} ${curr.text}`.trim();
          prev.end = curr.end ?? prev.end;
          tinyAbsorbedCount += 1;
          continue;
        }
        if (upcoming && endsWithFragment(curr.text)) {
          upcoming.text = `${curr.text} ${upcoming.text}`.trim();
          upcoming.start = curr.start ?? upcoming.start;
          tinyAbsorbedCount += 1;
          continue;
        }
      }
      next.push(curr);
    }
    return { segments: mergeAdjacent(next), tinyAbsorbedCount };
  };

  const repairPunctuation = (text) => {
    let repairs = 0;
    let next = String(text || "");
    const rules = [
      { regex: /\b(no|to|a|an|the|and|but|so)\.\s+([A-Z])/g, replace: "$1 $2" },
      { regex: /\b(to do|plus|because)\.\s+([A-Z])/g, replace: "$1 $2" },
    ];
    rules.forEach((rule) => {
      next = next.replace(rule.regex, (match, p1, p2) => {
        repairs += 1;
        return `${p1} ${p2.toLowerCase()}`;
      });
    });
    return { text: next, repairs };
  };

  let transcriptText = rawText;
  let transcriptLines = [];
  const utterances = Array.isArray(transcriptJson?.utterances) ? transcriptJson.utterances : [];
  const stitchedResult = stitchSegments(rawSegments);
  const smoothedSegments = smoothSegments(stitchedResult.stitched).filter((s) => s.text.length > 0);
  const tinyResult = absorbTinyTurns(smoothedSegments);
  const segments = tinyResult.segments;
  const splitSentences = (text) => {
    const parts = String(text || "")
      .match(/[^.!?]+[.!?]+|[^.!?]+$/g)
      || [];
    return parts.map((p) => p.trim()).filter((p) => p.length > 0);
  };
  const normalizeSpeaker = (rawSpeaker) => {
    if (rawSpeaker === 0 || rawSpeaker === "0") return "Speaker A";
    if (rawSpeaker === 1 || rawSpeaker === "1") return "Speaker B";
    const key = String(rawSpeaker ?? "").trim();
    if (!key || key.toLowerCase() === "speaker") return "Speaker A";
    if (/speaker\s*b/i.test(key) || key.toLowerCase().endsWith("b")) return "Speaker B";
    if (/speaker\s*a/i.test(key) || key.toLowerCase().endsWith("a")) return "Speaker A";
    return "Speaker A";
  };
  const allocTimes = (start, end, i, n) => {
    if (typeof start === "number" && typeof end === "number" && end > start) {
      const span = end - start;
      let segStart = start + Math.floor((i / n) * span);
      let segEnd = i === n - 1 ? end : start + Math.floor(((i + 1) / n) * span);
      if (segEnd <= segStart) segEnd = segStart + 1;
      return { segStart, segEnd };
    }
    return { segStart: null, segEnd: null };
  };
  const normalizeTiming = (lines, minStepMs = 1) => {
    let prevEnd = null;
    return lines.map((line) => {
      let start = typeof line.start_ms === "number" ? line.start_ms : null;
      let end = typeof line.end_ms === "number" ? line.end_ms : null;
      if (start != null && end != null) {
        if (prevEnd != null && start <= prevEnd) start = prevEnd + minStepMs;
        if (end <= start) end = start + minStepMs;
        prevEnd = end;
      }
      return { ...line, start_ms: start, end_ms: end };
    });
  };
  const subdivideDuplicateTimeRanges = (lines) => {
    const groups = new Map();
    lines.forEach((line, idx) => {
      if (typeof line.start_ms !== "number" || typeof line.end_ms !== "number") return;
      const key = `${line.start_ms}|${line.end_ms}`;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(idx);
    });
    const updated = lines.slice();
    groups.forEach((indices) => {
      if (indices.length <= 1) return;
      const sample = updated[indices[0]];
      const start = sample.start_ms;
      const end = sample.end_ms;
      if (!(typeof start === "number" && typeof end === "number") || end <= start) return;
      const span = end - start;
      const count = indices.length;
      indices.forEach((lineIdx, j) => {
        let newStart = start + Math.floor((j / count) * span);
        let newEnd = j === count - 1 ? end : start + Math.floor(((j + 1) / count) * span);
        if (newEnd <= newStart) newEnd = newStart + 1;
        updated[lineIdx] = {
          ...updated[lineIdx],
          start_ms: newStart,
          end_ms: newEnd,
        };
      });
    });
    return updated;
  };
  const buildTranscriptLinesFromUtterances = (sourceUtterances) => {
    const lines = [];
    let tempIndex = 1;
    (sourceUtterances || []).forEach((u) => {
      const text = String(u?.text || "").trim();
      if (!text) return;
      const sentences = splitSentences(text);
      if (!sentences.length) return;
      const speaker = normalizeSpeaker(u?.speaker);
      sentences.forEach((sentence, i) => {
        const { segStart, segEnd } = allocTimes(u?.start, u?.end, i, sentences.length);
        lines.push({
          line: tempIndex++,
          speaker,
          text: sentence,
          start_ms: segStart,
          end_ms: segEnd,
        });
      });
    });
    const sorted = lines.slice().sort((a, b) => {
      const aStart = typeof a.start_ms === "number" ? a.start_ms : Number.MAX_SAFE_INTEGER;
      const bStart = typeof b.start_ms === "number" ? b.start_ms : Number.MAX_SAFE_INTEGER;
      return aStart - bStart;
    });
    let prevEnd = null;
    const monotonic = sorted.map((line) => {
      let start = typeof line.start_ms === "number" ? line.start_ms : null;
      let end = typeof line.end_ms === "number" ? line.end_ms : null;
      if (start != null && end != null) {
        if (prevEnd != null && start <= prevEnd) start = prevEnd + 1;
        if (end <= start) end = start + 1;
        prevEnd = end;
      }
      return {
        ...line,
        start_ms: start,
        end_ms: end,
      };
    });
    return monotonic.map((line, i) => ({
      ...line,
      line: i + 1,
      speaker: normalizeSpeaker(line.speaker),
    }));
  };

  if (segments.length > 0) {
    const smoothedMetrics = computeMetrics(segments);
    const lowConfidence =
      smoothedMetrics.speakerCount < 2
      || smoothedMetrics.topShare > 0.92
      || smoothedMetrics.switches < 2;

    const speakerOrder = [];
    const speakerText = new Map();
    segments.forEach((s) => {
      if (!speakerText.has(s.speakerKey)) speakerOrder.push(s.speakerKey);
      speakerText.set(
        s.speakerKey,
        `${speakerText.get(s.speakerKey) || ""} ${s.text}`.trim()
      );
    });

    const sentenceSegments = [];
    let punctuationRepairsCount = 0;
    segments.forEach((s) => {
      const repair = repairPunctuation(s.text);
      punctuationRepairsCount += repair.repairs;
      const sentences = splitSentences(repair.text);
      sentences.forEach((t) => {
        sentenceSegments.push({
          text: t,
          approxStart: s.start ?? null,
          approxEnd: s.end ?? null,
          speakerKey: s.speakerKey,
        });
      });
    });
    const assignSpeakerKeysByTime = (sentences, timedSegments) => {
      const segmentsWithTime = timedSegments.filter(
        (seg) => typeof seg.start === "number" || typeof seg.end === "number"
      );
      let lastSpeakerKey = segmentsWithTime[0]?.speakerKey || null;
      sentences.forEach((s) => {
        const start = typeof s.approxStart === "number" ? s.approxStart : null;
        const end = typeof s.approxEnd === "number" ? s.approxEnd : null;
        let mid = null;
        if (start != null && end != null) mid = (start + end) / 2;
        else if (start != null) mid = start;
        else if (end != null) mid = end;
        if (mid == null || !segmentsWithTime.length) {
          s.speakerKey = s.speakerKey || lastSpeakerKey || "unknown";
          lastSpeakerKey = s.speakerKey;
          return;
        }
        let match = null;
        for (const seg of segmentsWithTime) {
          const segStart = typeof seg.start === "number" ? seg.start : null;
          const segEnd = typeof seg.end === "number" ? seg.end : null;
          if (segStart != null && segEnd != null && mid >= segStart && mid <= segEnd) {
            match = seg;
            break;
          }
        }
        if (!match) {
          let best = null;
          let bestDist = Number.POSITIVE_INFINITY;
          for (const seg of segmentsWithTime) {
            const segStart = typeof seg.start === "number" ? seg.start : null;
            const segEnd = typeof seg.end === "number" ? seg.end : null;
            let dist = null;
            if (segStart != null && segEnd != null) {
              if (mid < segStart) dist = segStart - mid;
              else if (mid > segEnd) dist = mid - segEnd;
              else dist = 0;
            } else if (segStart != null) {
              dist = Math.abs(mid - segStart);
            } else if (segEnd != null) {
              dist = Math.abs(mid - segEnd);
            }
            if (dist != null && dist < bestDist) {
              bestDist = dist;
              best = seg;
            }
          }
          match = best;
        }
        s.speakerKey = match?.speakerKey || s.speakerKey || lastSpeakerKey || "unknown";
        lastSpeakerKey = s.speakerKey;
      });
    };
    if (!utterances.length) {
      assignSpeakerKeysByTime(sentenceSegments, segments);
    }

    const agentPattern =
      /\b(this is|i'?m with|calling|quick one|do you have|can i|i'?m not calling to sell|what'?s better|best email|i'?ll send|calendar|we'?re seeing|would it be crazy)\b/gi;
    const prospectPattern =
      /\b(who'?s this|stop you|already have|not interested|busy|don'?t want|we filed|denied|storm chaser|commission|not doing that|just text me)\b/gi;

    const scores = sentenceSegments.map((s) => {
      const agentScore = (s.text.match(agentPattern) || []).length;
      const prospectScore = (s.text.match(prospectPattern) || []).length;
      return agentScore - prospectScore;
    });

    const dp = [];
    const back = [];
    const switchPenalty = 2;
    scores.forEach((score, i) => {
      const emitYou = score;
      const emitProspect = -score;
      if (i === 0) {
        dp[i] = [emitYou, emitProspect];
        back[i] = [0, 1];
        return;
      }
      dp[i] = [0, 0];
      back[i] = [0, 0];
      const stayYou = dp[i - 1][0] + emitYou;
      const switchToYou = dp[i - 1][1] + emitYou - switchPenalty;
      if (stayYou >= switchToYou) {
        dp[i][0] = stayYou;
        back[i][0] = 0;
      } else {
        dp[i][0] = switchToYou;
        back[i][0] = 1;
      }
      const stayProspect = dp[i - 1][1] + emitProspect;
      const switchToProspect = dp[i - 1][0] + emitProspect - switchPenalty;
      if (stayProspect >= switchToProspect) {
        dp[i][1] = stayProspect;
        back[i][1] = 1;
      } else {
        dp[i][1] = switchToProspect;
        back[i][1] = 0;
      }
    });

    const labelsBySentence = new Array(scores.length);
    let state = dp.length ? (dp[dp.length - 1][0] >= dp[dp.length - 1][1] ? 0 : 1) : 0;
    for (let i = scores.length - 1; i >= 0; i--) {
      labelsBySentence[i] = state === 0 ? "You" : "Prospect";
      state = back[i]?.[state] ?? 0;
    }

    const avgAbsScore = scores.length
      ? scores.reduce((sum, s) => sum + Math.abs(s), 0) / scores.length
      : 0;
    const youCount = labelsBySentence.filter((l) => l === "You").length;
    const prospectCount = labelsBySentence.length - youCount;
    const lowSignal = avgAbsScore < 0.6 || scores.every((s) => s === 0);
    const nearlyAllOne = labelsBySentence.length > 0
      && (youCount === 0 || prospectCount === 0);
    const useDpLabels = !lowSignal && !nearlyAllOne;

    let methodUsed = "speaker_single_stitched";
    let lines = [];
    let sentenceLines = [];

    if (useDpLabels && smoothedMetrics.speakerCount >= 2) {
      const grouped = [];
      sentenceSegments.forEach((s, i) => {
        const label = labelsBySentence[i];
        const prev = grouped[grouped.length - 1];
        if (prev && prev.label === label) {
          prev.text = `${prev.text} ${s.text}`.trim();
          if (prev.start == null) prev.start = s.approxStart ?? prev.start;
          prev.end = s.approxEnd ?? prev.end;
        } else {
          grouped.push({
            label,
            text: s.text,
            start: s.approxStart ?? null,
            end: s.approxEnd ?? null,
          });
        }
      });
      lines = grouped.map((g) => `${g.label}: ${g.text}`);
      methodUsed = "you_prospect_dp";
      sentenceLines = sentenceSegments.map((s, i) => ({
        label: labelsBySentence[i],
        text: s.text,
        start: s.approxStart ?? null,
        end: s.approxEnd ?? null,
      }));
    } else if (smoothedMetrics.speakerCount >= 2) {
      const labels = new Map();
      labels.set(speakerOrder[0], "Speaker A");
      labels.set(speakerOrder[1], "Speaker B");
      const grouped = [];
      segments.forEach((s) => {
        const label = labels.get(s.speakerKey) || "Speaker A";
        const prev = grouped[grouped.length - 1];
        if (prev && prev.label === label) {
          prev.text = `${prev.text} ${s.text}`.trim();
          if (prev.start == null) prev.start = s.start ?? prev.start;
          prev.end = s.end ?? prev.end;
        } else {
          grouped.push({
            label,
            text: s.text,
            start: s.start ?? null,
            end: s.end ?? null,
          });
        }
      });
      lines = grouped.map((g) => `${g.label}: ${g.text}`);
      methodUsed = "speaker_ab_stitched";
      sentenceLines = sentenceSegments.map((s) => ({
        label: labels.get(s.speakerKey) || "Speaker A",
        text: s.text,
        start: s.approxStart ?? null,
        end: s.approxEnd ?? null,
      }));
    } else {
      const grouped = [];
      segments.forEach((s) => {
        const prev = grouped[grouped.length - 1];
        if (prev) {
          prev.text = `${prev.text} ${s.text}`.trim();
          if (prev.start == null) prev.start = s.start ?? prev.start;
          prev.end = s.end ?? prev.end;
        } else {
          grouped.push({
            label: "Speaker A",
            text: s.text,
            start: s.start ?? null,
            end: s.end ?? null,
          });
        }
      });
      lines = grouped.map((g) => `${g.label}: ${g.text}`);
      methodUsed = "speaker_single_stitched";
      sentenceLines = sentenceSegments.map((s) => ({
        label: "Speaker A",
        text: s.text,
        start: s.approxStart ?? null,
        end: s.approxEnd ?? null,
      }));
    }

    transcriptJson.diarization_meta = {
      ...(transcriptJson.diarization_meta || {}),
      speakerCount: smoothedMetrics.speakerCount,
      topShare: smoothedMetrics.topShare,
      switches: smoothedMetrics.switches,
      smoothed: true,
      chunks_before: rawSegments.length,
      chunks_after: segments.length,
      fragments_stitched_count: stitchedResult.fragmentsStitchedCount,
      tiny_absorbed_count: tinyResult.tinyAbsorbedCount,
      punctuation_repairs_count: punctuationRepairsCount,
      sentences_count: sentenceSegments.length,
      switches_after_dp: labelsBySentence.reduce((acc, l, i, arr) => {
        if (i === 0) return acc;
        return acc + (l !== arr[i - 1] ? 1 : 0);
      }, 0),
      avg_abs_score: avgAbsScore,
      method_used: methodUsed,
    };

    transcriptText = lines.join("\n");

    transcriptLines = utterances.length
      ? buildTranscriptLinesFromUtterances(utterances)
      : sentenceLines.map((s, i) => ({
          line: i + 1,
          speaker: s.label || "Speaker A",
          text: s.text || "",
          start_ms: typeof s.start === "number" ? s.start : null,
          end_ms: typeof s.end === "number" ? s.end : null,
        }));
  }

  const normalizedSpeaker = (label) => normalizeSpeaker(label);

  if (!transcriptLines.length && utterances.length) {
    transcriptLines = buildTranscriptLinesFromUtterances(utterances);
  }
  if (!transcriptLines.length) {
    const sentences = splitSentences(transcriptText);
    transcriptLines = sentences.map((text, i) => ({
      line: i + 1,
      speaker: "Speaker A",
      text,
      start_ms: null,
      end_ms: null,
    }));
  }

  transcriptLines = normalizeTiming(subdivideDuplicateTimeRanges(transcriptLines), 1).map(
    (line, i) => ({
    ...line,
    line: i + 1,
    speaker: normalizedSpeaker(line.speaker),
  }));
  if (transcriptLines.length) {
    const counts = transcriptLines.reduce((acc, line) => {
      const key = line.speaker || "Speaker A";
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});
    const pairCounts = transcriptLines.reduce((acc, line) => {
      const key = `${line.start_ms ?? "null"}|${line.end_ms ?? "null"}`;
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});
    const startEndReuseCount = Object.values(pairCounts).reduce((sum, count) => {
      if (count > 1) return sum + count;
      return sum;
    }, 0);
    transcriptJson.diarization_meta = {
      ...(transcriptJson.diarization_meta || {}),
      transcript_lines_total: transcriptLines.length,
      counts_by_speaker: counts,
      start_end_reuse_count: startEndReuseCount,
    };
  }

  return { transcriptText, transcriptJson, transcriptLines };
}

function extractAudioFromMp4(mp4Path) {
  return new Promise((resolve, reject) => {
    const outPath = path.join(os.tmpdir(), `calibrate_audio_${Date.now()}.m4a`);
    const args = [
      "-y",
      "-i",
      mp4Path,
      "-vn",
      "-acodec",
      "aac",
      "-b:a",
      "128k",
      outPath,
    ];
    const p = spawn(ffmpeg, args, { stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    p.stderr.on("data", (d) => (stderr += d.toString()));
    p.on("close", (code) => {
      if (code === 0) resolve(outPath);
      else reject(new Error("ffmpeg failed: " + stderr.slice(-4000)));
    });
  });
}

async function deepseekJSON(system, user, temperature = 0.2) {
  if (!DEEPSEEK_API_KEY) throw new Error("Missing DEEPSEEK_API_KEY");

  const resp = await fetch("https://api.deepseek.com/chat/completions", {
    method: "POST",
    headers: {
      authorization: `Bearer ${DEEPSEEK_API_KEY}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "deepseek-chat",
      temperature,
      messages: [
        { role: "system", content: system },
        { role: "user", content: user },
      ],
      response_format: { type: "json_object" },
    }),
  });

  const txt = await resp.text();
  if (!resp.ok) throw new Error(`DeepSeek error: ${txt}`);

  const data = safeJsonParse(txt);
  const content = data?.choices?.[0]?.message?.content || "{}";
  const parsed = safeJsonParse(content);
  if (!parsed) throw new Error("DeepSeek returned invalid JSON");
  return parsed;
}

async function deepseekJSONWithRaw(system, user, temperature = 0.2) {
  if (!DEEPSEEK_API_KEY) throw new Error("Missing DEEPSEEK_API_KEY");

  const resp = await fetch("https://api.deepseek.com/chat/completions", {
    method: "POST",
    headers: {
      authorization: `Bearer ${DEEPSEEK_API_KEY}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "deepseek-chat",
      temperature,
      messages: [
        { role: "system", content: system },
        { role: "user", content: user },
      ],
      response_format: { type: "json_object" },
    }),
  });

  const txt = await resp.text();
  if (!resp.ok) throw new Error(`DeepSeek error: ${txt}`);

  const data = safeJsonParse(txt);
  const content = data?.choices?.[0]?.message?.content || "{}";
  const parsed = safeJsonParse(content);
  if (!parsed) throw new Error("DeepSeek returned invalid JSON");
  return { parsed, raw: txt };
}

function buildAnalysisPrompt({ transcript, transcriptLines, context, scenario, entityAggregate }) {
  const numberedTranscript = Array.isArray(transcriptLines) && transcriptLines.length
    ? transcriptLines
      .map((line, i) => {
        const lineNum = Number(line?.line) || i + 1;
        const speaker = line?.speaker || "Speaker A";
        const text = line?.text || "";
        return `${lineNum}) ${speaker}: ${text}`;
      })
      .join("\n")
    : String(transcript || "")
      .split("\n")
      .map((line, i) => `${i + 1}) ${line}`)
      .join("\n");
  const base = `
You are Calibrate — a post-call decision engine for cold calls.

Output MUST be valid JSON.

Return keys:
- report_title: string (2-5 words, Title Case, no punctuation)
- call_result: { label: string, why: string, evidence: [{ line: number, quote: string, why: string }] }
- score: number (0-100)
- signals: string[] (short badges)
- follow_up: { text: string }
- top_fixes: [{ title: string, why: string, do_instead: string }]
- notes: { quotes: [{ quote: string, why: string }] }

Rules:
- Be concise and actionable.
- Use transcript as source of truth.
- report_title must use both user context and transcript.
- Follow-up should be 2 lines max.
- Top fixes must be exactly 3 items.
- Quotes: include 2-4 short quotes, max 18 words each.
- Evidence must be exactly 36 items.
- Evidence line must reference the numbered transcript line.
- Evidence quote must be an exact substring from that line, max ~18 words.
- If transcript has 36+ lines, evidence must use 36 distinct line numbers.
- If transcript has fewer than 36 lines, repeats are allowed.
- If the transcript does not support it, do not include it.
`;

  const extra = entityAggregate
    ? `
Entity aggregate context (patterns across calls):
${entityAggregate}
`
    : "";

  const user = `
Scenario: ${scenario || "None"}
User context: ${context || ""}
${extra}

Transcript:
${numberedTranscript || ""}
`;

  return { system: base.trim(), user: user.trim() };
}

function buildNumberedTranscriptLines(transcriptLines) {
  return (Array.isArray(transcriptLines) ? transcriptLines : []).map((line, i) => {
    const lineNum = Number(line?.line) || i + 1;
    const speaker = line?.speaker || "Speaker A";
    const text = line?.text || "";
    return `${lineNum}) ${speaker}: ${text}`;
  });
}

function normalizeEvidenceItems(evidence, transcriptLines) {
  if (!Array.isArray(evidence)) return [];
  const maxLine = Array.isArray(transcriptLines) ? transcriptLines.length : 0;
  const seen = new Set();
  return evidence
    .map((item) => {
      const lineNum = Number(
        item?.line != null ? item.line : item?.line_index != null ? item.line_index : NaN
      );
      if (!Number.isFinite(lineNum)) return null;
      const idx = Math.floor(lineNum);
      if (idx < 1 || idx > maxLine) return null;
      const quote = String(item?.quote || "").trim();
      const why = String(item?.why || "").trim();
      if (!quote) return null;
      const line = transcriptLines[idx - 1];
      const lineText = `${line?.speaker || "Speaker A"}: ${line?.text || ""}`;
      if (!lineText.includes(quote)) return null;
      const key = `${idx}|${quote}`;
      if (seen.has(key)) return null;
      seen.add(key);
      return { line: idx, quote, why };
    })
    .filter(Boolean);
}

function validateAndCoerceAnalysisJson(analysisJson, transcriptLines, outcomeLabel) {
  const errors = [];
  const isObject =
    analysisJson && typeof analysisJson === "object" && !Array.isArray(analysisJson);
  const base = isObject ? analysisJson : {};
  if (!isObject) errors.push("analysis_json not object");

  const callResultRaw =
    base?.call_result && typeof base.call_result === "object" && !Array.isArray(base.call_result)
      ? base.call_result
      : null;
  if (!callResultRaw) errors.push("call_result missing or invalid");

  const reportTitle = String(
    base?.report_title || callResultRaw?.label || outcomeLabel || "Call"
  ).trim() || String(outcomeLabel || "Call");
  if (typeof base?.report_title !== "string") errors.push("report_title missing or invalid");

  const label = String(callResultRaw?.label || outcomeLabel || "Unknown").trim()
    || String(outcomeLabel || "Unknown");
  const why = String(callResultRaw?.why || "").trim();
  if (typeof callResultRaw?.label !== "string") errors.push("call_result.label missing or invalid");

  const scoreRaw = Number(base?.score ?? base?.overall_score);
  if (!Number.isFinite(scoreRaw)) errors.push("score missing or invalid");
  const score = Number.isFinite(scoreRaw) ? Math.max(0, Math.min(100, scoreRaw)) : 0;

  let signals = [];
  if (Array.isArray(base?.signals)) {
    signals = base.signals.map((s) => String(s || "").trim()).filter((s) => s);
  } else if (typeof base?.signals === "string") {
    signals = [base.signals.trim()].filter((s) => s);
    errors.push("signals not array");
  } else if (base?.signals != null) {
    errors.push("signals not array");
  }

  let followText = "";
  if (base?.follow_up && typeof base.follow_up === "object" && !Array.isArray(base.follow_up)) {
    followText = String(base.follow_up.text || "").trim();
  } else if (typeof base?.follow_up === "string") {
    followText = base.follow_up.trim();
    errors.push("follow_up not object");
  } else if (base?.follow_up != null) {
    errors.push("follow_up not object");
  }

  const topFixes = Array.isArray(base?.top_fixes) ? base.top_fixes : [];
  if (!Array.isArray(base?.top_fixes) && base?.top_fixes != null) {
    errors.push("top_fixes not array");
  }

  if (!Array.isArray(callResultRaw?.evidence)) errors.push("evidence missing or invalid");
  let evidence = normalizeEvidenceItems(callResultRaw?.evidence, transcriptLines);
  if (evidence.length > 36) evidence = evidence.slice(0, 36);
  if (evidence.length < 36) errors.push("evidence_short");

  const fixedJson = {
    ...(base && typeof base === "object" && !Array.isArray(base) ? base : {}),
    report_title: reportTitle,
    call_result: {
      ...(callResultRaw && typeof callResultRaw === "object" && !Array.isArray(callResultRaw)
        ? callResultRaw
        : {}),
      label,
      why,
      evidence,
    },
    score,
    signals,
    follow_up: { text: followText },
    top_fixes: topFixes,
  };

  return { fixedJson, errors };
}

function buildFallbackEvidence(transcriptLines) {
  const lines = Array.isArray(transcriptLines) ? transcriptLines : [];
  const safeLines = lines.length ? lines : [{ line: 1, speaker: "Speaker A", text: "" }];
  const out = [];
  for (let i = 0; i < 36; i += 1) {
    const line = safeLines[i] || safeLines[safeLines.length - 1] || {};
    const lineNum = Number(line.line) || Math.min(i + 1, safeLines.length);
    const text = String(line.text || "");
    const quote = text.trim() ? firstWords(text, 18) : "";
    out.push({ line: lineNum, quote, why: "Evidence fallback" });
  }
  return out;
}

function buildFallbackAnalysisJson(outcomeLabel, transcriptLines) {
  const label = String(outcomeLabel || "Call").trim() || "Call";
  return {
    report_title: label,
    call_result: { label, why: "", evidence: buildFallbackEvidence(transcriptLines) },
    score: 0,
    signals: [],
    follow_up: { text: "" },
    top_fixes: [],
  };
}

async function inferOutcomeLabel(analysisJson) {
  return analysisJson?.call_result?.label || "Unknown";
}

function toNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function normalizeSpeakerFinal(raw) {
  const s = String(raw ?? "").trim().toLowerCase();
  if (!s || s === "speaker" || s === "unknown") return "Speaker A";
  if (raw === 0 || s === "0") return "Speaker A";
  if (raw === 1 || s === "1") return "Speaker B";
  if (s.includes("speaker b") || s.endsWith("b")) return "Speaker B";
  if (s.includes("speaker a") || s.endsWith("a")) return "Speaker A";
  return "Speaker A";
}

function subdivideDuplicateTimeRanges(lines) {
  const groups = new Map();
  lines.forEach((ln, idx) => {
    const s = ln.start_ms;
    const e = ln.end_ms;
    if (typeof s !== "number" || typeof e !== "number") return;
    if (!(e > s)) return;
    const k = `${s}|${e}`;
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(idx);
  });

  for (const [k, idxs] of groups.entries()) {
    if (idxs.length <= 1) continue;
    const [s0, e0] = k.split("|").map(Number);
    const span = e0 - s0;
    const n = idxs.length;
    idxs.forEach((idx, j) => {
      let s = s0 + Math.floor((j / n) * span);
      let e = j === n - 1 ? e0 : s0 + Math.floor(((j + 1) / n) * span);
      if (e <= s) e = s + 1;
      lines[idx].start_ms = s;
      lines[idx].end_ms = e;
    });
  }
  return lines;
}

function monotonicizeAndRenumber(lines) {
  const sorted = lines.slice().sort((a, b) => {
    const as = typeof a.start_ms === "number" ? a.start_ms : Number.MAX_SAFE_INTEGER;
    const bs = typeof b.start_ms === "number" ? b.start_ms : Number.MAX_SAFE_INTEGER;
    return as - bs;
  });
  let prevEnd = null;
  for (const ln of sorted) {
    if (typeof ln.start_ms === "number" && typeof ln.end_ms === "number") {
      if (prevEnd != null && ln.start_ms <= prevEnd) ln.start_ms = prevEnd + 1;
      if (ln.end_ms <= ln.start_ms) ln.end_ms = ln.start_ms + 1;
      prevEnd = ln.end_ms;
    }
  }
  return sorted.map((ln, i) => ({ ...ln, line: i + 1 }));
}

function finalizeTranscriptLines(rawLines) {
  const base = (Array.isArray(rawLines) ? rawLines : []).map((l) => ({
    line: Number(l?.line) || Number(l?.line_index) || 0,
    speaker: normalizeSpeakerFinal(l?.speaker),
    text: String(l?.text ?? ""),
    start_ms: toNum(l?.start_ms),
    end_ms: toNum(l?.end_ms),
  }));
  subdivideDuplicateTimeRanges(base);
  return monotonicizeAndRenumber(base);
}

function firstWords(text, maxWords = 16) {
  const words = String(text || "").trim().split(/\s+/).filter(Boolean);
  return words.slice(0, maxWords).join(" ");
}

function enforceEvidence36(evidence, finalTranscriptLines) {
  const lines = Array.isArray(finalTranscriptLines) ? finalTranscriptLines : [];
  const maxLine = lines.length;
  const seen = new Set();
  let ev = Array.isArray(evidence) ? evidence : [];

  ev = ev.map((x) => {
    const line = Math.floor(Number(x?.line ?? x?.line_index ?? x?.lineNumber));
    const quote = String(x?.quote ?? x?.text ?? "").trim();
    const why = String(x?.why ?? x?.reason ?? "").trim();
    return { line, quote, why };
  }).filter((x) => Number.isFinite(x.line) && x.line >= 1 && x.line <= maxLine && x.quote);

  ev = ev.filter((x) => {
    const lineObj = lines[x.line - 1];
    const lineText = String(lineObj?.text ?? "");
    if (!lineText.includes(x.quote)) return false;
    const key = `${x.line}|${x.quote}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  if (ev.length > 36) return ev.slice(0, 36);

  const usedLines = new Set(ev.map((x) => x.line));
  for (let i = 1; i <= maxLine && ev.length < 36; i++) {
    if (usedLines.has(i)) continue;
    const txt = String(lines[i - 1]?.text ?? "").trim();
    if (!txt) continue;
    const q = firstWords(txt, 16);
    if (!q) continue;
    ev.push({ line: i, quote: q, why: "" });
    usedLines.add(i);
  }

  for (let i = 1; i <= maxLine && ev.length < 36; i++) {
    const txt = String(lines[i - 1]?.text ?? "").trim();
    if (!txt) continue;
    const q = firstWords(txt, 16);
    ev.push({ line: i, quote: q, why: "" });
  }

  return ev.slice(0, 36);
}

function normalizeKey(str) {
  return String(str || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function quoteAppearsInTranscript(quote, transcript) {
  const nq = normalizeKey(quote);
  const nt = normalizeKey(transcript);
  if (nq.length < 12) return false;
  if (!nq || !nt) return false;
  return nt.includes(nq);
}

function jaccardSimilarity(a, b) {
  const na = normalizeKey(a);
  const nb = normalizeKey(b);
  if (!na || !nb) return 0;
  const setA = new Set(na.split(" "));
  const setB = new Set(nb.split(" "));
  let intersect = 0;
  setA.forEach((t) => {
    if (setB.has(t)) intersect += 1;
  });
  const union = new Set([...setA, ...setB]).size || 1;
  return intersect / union;
}

function mergeObservedObjections(existing, incoming) {
  const merged = Array.isArray(existing) ? existing.map((o) => ({ ...o })) : [];
  (Array.isArray(incoming) ? incoming : []).forEach((obs) => {
    const obsText = obs?.objection || "";
    if (!obsText) return;
    const idx = merged.findIndex((m) => {
      const keyA = normalizeKey(m?.objection);
      const keyB = normalizeKey(obsText);
      if (!keyA || !keyB) return false;
      if (keyA === keyB) return true;
      return jaccardSimilarity(keyA, keyB) >= 0.6;
    });
    if (idx === -1) {
      const evidence =
        obs?.evidence_quote && obs?.run_id
          ? [{ evidence_quote: obs.evidence_quote, run_id: obs.run_id }]
          : [];
      merged.push({
        ...obs,
        count: 1,
        variants: [obsText],
        evidence,
      });
      return;
    }
    const current = merged[idx];
    const nextCount = (current?.count || 1) + 1;
    const existingVariants = Array.isArray(current?.variants)
      ? current.variants.slice()
      : [current?.objection].filter(Boolean);
    const obsKey = normalizeKey(obsText);
    if (obsKey && !existingVariants.some((v) => normalizeKey(v) === obsKey)) {
      existingVariants.push(obsText);
    }
    let evidence = Array.isArray(current?.evidence) ? current.evidence.slice() : [];
    if (!evidence.length && current?.evidence_quote && current?.run_id) {
      evidence = [{ evidence_quote: current.evidence_quote, run_id: current.run_id }];
    }
    if (obs?.evidence_quote && obs?.run_id) {
      const evKey = `${normalizeKey(obs.evidence_quote)}|${obs.run_id}`;
      const exists = evidence.some(
        (e) => `${normalizeKey(e?.evidence_quote)}|${e?.run_id}` === evKey
      );
      if (!exists) {
        evidence.push({ evidence_quote: obs.evidence_quote, run_id: obs.run_id });
      }
    }
    if (evidence.length > 5) evidence = evidence.slice(-5);
    const currentObjection = String(current?.objection || "");
    const currentLen = currentObjection.length;
    const obsLen = String(obsText).length;
    const useObs = obsLen > currentLen && obsLen <= 140;
    const nextObjection = useObs ? obsText : currentObjection || obsText;
    const currentResp = String(current?.best_response || "");
    const obsResp = String(obs?.best_response || "");
    const nextResp = obsResp.length > currentResp.length + 20 ? obsResp : currentResp || obsResp;
    const latestEvidence = evidence.length ? evidence[evidence.length - 1] : null;
    merged[idx] = {
      ...current,
      ...obs,
      objection: nextObjection,
      best_response: nextResp,
      count: nextCount,
      variants: existingVariants,
      evidence,
      evidence_quote: latestEvidence?.evidence_quote || obs?.evidence_quote || current?.evidence_quote,
      run_id: latestEvidence?.run_id || obs?.run_id || current?.run_id,
    };
  });
  return merged;
}

// -------- Entities API --------

app.get("/api/entities", async (req, res) => {
  const user = await requireUser(req, res);
  if (!user) return;

  const { data, error } = await supabaseAdmin
    .from("entities")
    .select("*")
    .eq("user_id", user.id)
    .order("created_at", { ascending: false });

  if (error) {
    if (handleMissingUserId(res, "entities", error)) return;
    return res.status(400).json({ error: error.message });
  }
  res.json({ entities: data || [] });
});

app.post("/api/entities", async (req, res) => {
  const user = await requireUser(req, res);
  if (!user) return;

  const { id, name, offer, industry, notes } = req.body || {};
  if (!name) return res.status(400).json({ error: "Missing name" });

  if (id) {
    const { error } = await supabaseAdmin
      .from("entities")
      .update({ name, offer, industry, notes })
      .eq("id", id)
      .eq("user_id", user.id);

    if (error) {
      if (handleMissingUserId(res, "entities", error)) return;
      return res.status(400).json({ error: error.message });
    }
    return res.json({ ok: true });
  }

  const { data, error } = await supabaseAdmin
    .from("entities")
    .insert([{ user_id: user.id, name, offer, industry, notes }])
    .select()
    .single();

  if (error) {
    if (handleMissingUserId(res, "entities", error)) return;
    return res.status(400).json({ error: error.message });
  }
  res.json({ entity: data });
});

async function buildEntityAggregate(userId, entityId) {
  const { data, error } = await supabaseAdmin
    .from("runs")
    .select("transcript_text, analysis_json, created_at")
    .eq("user_id", userId)
    .eq("entity_id", entityId)
    .order("created_at", { ascending: false })
    .limit(12);

  if (error) return null;
  if (!data?.length) return null;

  // lightweight summary for prompt
  const snippets = data
    .map((r, i) => {
      const label = r.analysis_json?.call_result?.label || "Unknown";
      const fixes = (r.analysis_json?.top_fixes || [])
        .slice(0, 2)
        .map((f) => f.title)
        .filter(Boolean)
        .join(", ");
      return `Call ${i + 1}: result=${label}; recurring_fixes=${fixes}`;
    })
    .join("\n");

  return snippets;
}

app.post("/api/entities/:id/playbook", async (req, res) => {
  const user = await requireUser(req, res);
  if (!user) return;

  const entityId = req.params.id;

  const { data: entityCheck, error: entityCheckErr } = await supabaseAdmin
    .from("entities")
    .select("id")
    .eq("id", entityId)
    .eq("user_id", user.id)
    .maybeSingle();
  if (entityCheckErr) {
    if (handleMissingUserId(res, "entities", entityCheckErr)) return;
    return res.status(400).json({ error: entityCheckErr.message });
  }
  if (!entityCheck) return res.status(404).json({ error: "Not found" });

  const { data: latestRun, error: lrErr } = await supabaseAdmin
    .from("runs")
    .select("created_at")
    .eq("user_id", user.id)
    .eq("entity_id", entityId)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (lrErr) {
    if (handleMissingUserId(res, "runs", lrErr)) return;
    return res.status(400).json({ error: lrErr.message });
  }
  if (!latestRun) return res.status(400).json({ error: "No runs found for this entity yet." });

  const latestRunCreatedAt = latestRun.created_at;

  const { data: cached, error: cErr } = await supabaseAdmin
    .from("entity_playbooks")
    .select("playbook_json, last_run_created_at, updated_at")
    .eq("entity_id", entityId)
    .eq("user_id", user.id)
    .maybeSingle();

  if (cErr) {
    if (handleMissingUserId(res, "entity_playbooks", cErr)) return;
    return res.status(400).json({ error: cErr.message });
  }
  if (
    cached?.playbook_json
    && cached.last_run_created_at
    && cached.last_run_created_at >= latestRunCreatedAt
  ) {
    return res.json({
      playbook: cached.playbook_json,
      updated_at: cached.updated_at,
      last_run_created_at: cached.last_run_created_at,
    });
  }

  const { data: entity, error: eErr } = await supabaseAdmin
    .from("entities")
    .select("*")
    .eq("id", entityId)
    .eq("user_id", user.id)
    .single();

  if (eErr) {
    if (handleMissingUserId(res, "entities", eErr)) return;
    return res.status(400).json({ error: eErr.message });
  }

  const { data: runs, error: rErr } = await supabaseAdmin
    .from("runs")
    .select("id, transcript_text, analysis_json, created_at")
    .eq("user_id", user.id)
    .eq("entity_id", entityId)
    .order("created_at", { ascending: false })
    .limit(18);

  if (rErr) {
    if (handleMissingUserId(res, "runs", rErr)) return;
    return res.status(400).json({ error: rErr.message });
  }
  if (!runs?.length) return res.status(400).json({ error: "No runs found for this entity yet." });

  const runTextById = new Map(runs.map((r) => [r.id, r.transcript_text || ""]));
  const transcripts = runs
    .map((r) => (r.transcript_text ? `run_id=${r.id}\n${r.transcript_text}` : ""))
    .filter(Boolean);
  const system = `
You are Calibrate. Generate an MVP playbook for a cold-calling entity.

Return valid JSON:
{
  "entity": { "name": string, "offer": string, "industry": string },
  "ultimate_script": { "opener": string, "pitch": string, "qualify": string, "close": string },
  "observed_objections": [
    { "objection": string, "best_response": string, "evidence_quote": string, "run_id": string }
  ],
  "potential_objections": [
    { "objection": string, "best_response": string }
  ],
  "dont_say": string[],
  "say_instead": string[],
  "patterns": [{ "pattern": string, "impact": string, "fix": string }]
}

Rules:
- Observed objections MUST come only from the transcripts provided.
- Each observed objection MUST include a short direct evidence_quote and the run_id it came from.
- run_id MUST exactly match the UUID shown after run_id= in the transcript header.
- evidence_quote MUST be an exact substring from that transcript.
- If no objections appear in transcripts, observed_objections must be [].
- Potential objections are predicted and may not appear in transcripts.
- Keep each script section under ~90 words.
- Objection responses should be 2-4 sentences.
- Focus on what repeatedly worked/failed across calls.
`.trim();

  const userPrompt = `
Entity:
name=${entity.name}
offer=${entity.offer || ""}
industry=${entity.industry || ""}

Recent transcripts (most recent first):
${transcripts.slice(0, 15).map((t, i) => `--- TRANSCRIPT ${i + 1} ---\n${t}`).join("\n\n")}
`.trim();

  const playbook = await deepseekJSON(system, userPrompt, 0.25);
  const oldObserved = Array.isArray(cached?.playbook_json?.observed_objections)
    ? cached.playbook_json.observed_objections
    : [];
  const newObserved = Array.isArray(playbook?.observed_objections)
    ? playbook.observed_objections.filter((o) => {
        const runText = runTextById.get(o?.run_id);
        if (!runText) return false;
        if (!o?.objection || !o?.best_response) return false;
        return quoteAppearsInTranscript(o?.evidence_quote, runText);
      })
    : [];
  playbook.observed_objections = mergeObservedObjections(
    oldObserved,
    mergeObservedObjections([], newObserved)
  );
  playbook.potential_objections = Array.isArray(playbook?.potential_objections)
    ? playbook.potential_objections
    : [];

  // store (optional)
  const { data: savedPlaybook, error: pErr } = await supabaseAdmin
    .from("entity_playbooks")
    .upsert(
      [
        {
          user_id: user.id,
          entity_id: entityId,
          title: `Playbook ${new Date().toISOString().slice(0, 10)}`,
          playbook_json: playbook,
          updated_at: nowIso(),
          last_run_created_at: latestRunCreatedAt,
        },
      ],
      { onConflict: "user_id,entity_id" }
    )
    .select("playbook_json, updated_at, last_run_created_at")
    .maybeSingle();

  if (pErr) {
    if (handleMissingUserId(res, "entity_playbooks", pErr)) return;
    return res.status(500).json({
      error: "Playbook store misconfigured",
      code: "PLAYBOOK_UPSERT_SCHEMA",
    });
  }
  res.json({
    playbook: savedPlaybook?.playbook_json || playbook,
    updated_at: savedPlaybook?.updated_at || null,
    last_run_created_at: savedPlaybook?.last_run_created_at || latestRunCreatedAt,
  });
});

// -------- Runs API --------

app.get("/api/runs", async (req, res) => {
  const user = await requireUser(req, res);
  if (!user) return;

  const entityId = (req.query.entity_id || "").trim();
  let runsQuery = supabaseAdmin
    .from("runs")
    .select("id, created_at, scenario, outcome_label, analysis_json, transcript_lines, entity_id, entities(name), status, progress_step, error_text")
    .eq("user_id", user.id);
  if (entityId) runsQuery = runsQuery.eq("entity_id", entityId);
  const { data, error } = await runsQuery
    .order("created_at", { ascending: false })
    .limit(50);

  if (error) {
    if (handleMissingUserId(res, "runs", error)) return;
    return res.status(400).json({ error: error.message });
  }

  const runs = (data || []).map((r) => ({
    ...r,
    report_title: r.analysis_json?.report_title || r.outcome_label || "Call",
    entity_name: r.entities?.name || "",
  }));

  res.json({ runs });
});

app.get("/api/runs/:id", async (req, res) => {
  const user = await requireUser(req, res);
  if (!user) return;

  const id = req.params.id;

  if (
    process.env.NODE_ENV === "test"
    && process.env.SMOKE_BYPASS_TOKEN
    && req.headers["x-smoke-bypass"] === process.env.SMOKE_BYPASS_TOKEN
  ) {
    return res.status(404).json({ error: "Not found", code: "NOT_FOUND" });
  }

  const { data, error } = await supabaseAdmin
    .from("runs")
    .select("*")
    .eq("id", id)
    .eq("user_id", user.id)
    .maybeSingle();

  if (error) {
    if (handleMissingUserId(res, "runs", error)) return;
    return res.status(500).json({ error: "Server error", code: "SERVER_ERROR" });
  }
  if (!data) {
    return res.status(404).json({ error: "Not found", code: "NOT_FOUND" });
  }

  let entityName = "";
  if (data.entity_id) {
    const { data: entityRow, error: eErr } = await supabaseAdmin
      .from("entities")
      .select("name")
      .eq("id", data.entity_id)
      .eq("user_id", user.id)
      .maybeSingle();
    if (eErr) {
      if (handleMissingUserId(res, "entities", eErr)) return;
      return res.status(500).json({ error: "Server error", code: "SERVER_ERROR" });
    }
    if (!entityRow) {
      return res.status(404).json({ error: "Not found", code: "NOT_FOUND" });
    }
    entityName = entityRow?.name || "";
  }

  const run = {
    ...data,
    entity_name: entityName,
  };
  res.json({ run });
});

app.post("/api/runs/:id/regen_followup", async (req, res) => {
  const user = await requireUser(req, res);
  if (!user) return;

  const id = req.params.id;

  const { data: run, error } = await supabaseAdmin
    .from("runs")
    .select("*")
    .eq("id", id)
    .eq("user_id", user.id)
    .single();

  if (error) {
    if (handleMissingUserId(res, "runs", error)) return;
    return res.status(400).json({ error: error.message });
  }

  // Ensure we have entity_name when entity_id exists
  let entityName = run.entity_name || "";
  if (!entityName && run.entity_id) {
    const { data: ent } = await supabaseAdmin
      .from("entities")
      .select("name")
      .eq("id", run.entity_id)
      .eq("user_id", user.id)
      .maybeSingle();
    entityName = ent?.name || "";
  }
  const scenario = run.scenario || "";
  const callWhy = run.analysis_json?.call_result?.why || "";
  const topFixes = Array.isArray(run.analysis_json?.top_fixes)
    ? run.analysis_json.top_fixes
        .map((f) => f?.title || f?.fix || f?.problem || f?.do_instead || f?.instead || "")
        .filter(Boolean)
        .slice(0, 12)
    : [];

  const stopwords = new Set([
    "the", "and", "for", "with", "that", "this", "from", "your", "you", "our",
    "are", "was", "were", "have", "has", "had", "will", "would", "could", "should",
    "into", "about", "their", "they", "them", "then", "than", "just", "also",
  ]);
  const topicTerms = []
    .concat(entityName, scenario, run.context_text || "")
    .join(" ")
    .toLowerCase()
    .split(/[^a-z]+/)
    .filter((w) => w.length >= 4 && !stopwords.has(w));

  const containsPlaceholders = (text) => {
    const t = String(text || "");
    return (
      /\[[^\]]+\]/.test(t)
      || /\{[^}]+\}/.test(t)
      || /\bTBD\b/i.test(t)
      || /\blorem\b/i.test(t)
      || /\bplaceholder\b/i.test(t)
    );
  };

  const containsTopicTerm = (text) => {
    if (!topicTerms.length) return true;
    const t = String(text || "").toLowerCase();
    return topicTerms.some((term) => t.includes(term));
  };

  const containsNextStep = (text) => (
    /(tomorrow|thursday|next week|this week|schedule|calendar|time to|available|quick call|follow[- ]?up|chat|meet|15[- ]?min|10[- ]?min)/i
      .test(String(text || ""))
  );

  const hasBannedPhrases = (text) => {
    const t = String(text || "").toLowerCase();
    return (
      t.includes("i'll let my wife")
      || t.includes("my wife")
      || t.includes("birthday party")
      || t.includes("party packages")
    );
  };

  const includesEntityReference = (text, name) => {
    const nm = String(name || "").toLowerCase();
    if (!nm) return true;
    const tokens = nm.split(/[^a-z0-9]+/).filter((t) => t.length >= 3 || t === "ai");
    if (!tokens.length) return true;
    const t = String(text || "").toLowerCase();
    return tokens.some((tok) => {
      if (tok.length >= 5) return t.includes(tok);
      return new RegExp(`\\b${tok.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\\\$&")}\\b`, "i").test(t);
    });
  };

  const containsPersonalLife = (text) => {
    const t = String(text || "");
    return (
      /\bmy (wife|husband|kid|kids|mom|dad|birthday|party)\b/i.test(t)
      || /\bI'?ll let my (wife|husband)\b/i.test(t)
    );
  };

  const hasCoachingLeakage = (text) => {
    const t = String(text || "").toLowerCase();
    const banned = [
      "quick improvements",
      "top fixes",
      "call result",
      "rejected",
      "qualified lead",
      "score",
      "signals",
      "evidence",
    ];
    if (banned.some((term) => t.includes(term))) return true;
    if (topFixes.length) {
      const words = topFixes.join(" ").toLowerCase().split(/[^a-z]+/).filter(Boolean);
      return words.some((w) => w.length >= 4 && t.includes(w));
    }
    return false;
  };

  const looksLikeTranscript = (text) => {
    const t = String(text || "").trim();
    if (!t) return false;
    const linePrefix = /^(speaker|agent|prospect|rep|customer|caller)\s*[ab]?:/i;
    if (linePrefix.test(t)) return true;
    if (/\bSpeaker\s*[AB]:/i.test(t)) return true;
    const prefixed = t.split(/\r?\n/).filter((l) => linePrefix.test(l.trim())).length;
    if (prefixed > 0) return true;
    const quoteCount = (t.match(/["“”]/g) || []).length;
    return quoteCount >= 4;
  };

  const isFollowupValid = (text) => {
    const t = String(text || "").trim();
    if (!t) return false;
    if (t.length > 1200) return false;
    if (containsPlaceholders(t)) return false;
    if (looksLikeTranscript(t)) return false;
    if (hasBannedPhrases(t)) return false;
    if (hasCoachingLeakage(t)) return false;
    if (containsPersonalLife(t)) return false;
    if (!containsNextStep(t)) return false;
    if (!includesEntityReference(t, entityName)) return false;
    if (!t.includes("?")) return false;
    if (!containsTopicTerm(t)) return false;
    return true;
  };

  const buildFallbackFollowup = () => {
    const topic = entityName || scenario || "your team";
    const msg = `Hi - thanks again for taking the call about ${topic}. If it makes sense, can we do a quick 10-min follow-up tomorrow or Thursday? Happy to send a short recap first.`;
    return msg.slice(0, 1200);
  };

  const system = `You are Calibrate. Return valid JSON only.`.trim();
  const basePrompt = `
Write a concise follow-up message from the SALES REP to the PROSPECT.
Return JSON: { "follow_up": { "text": string } }
Rules:
- One message only, no analysis.
- <=1200 characters.
- No placeholders like [Your Name], {company}, TBD, lorem.
- Do not quote the transcript verbatim or include speaker labels.
- Professional and direct, with a concrete next step.
- Write as the sales rep.
- Mention the offer/company topic (entity_name) explicitly.
- Propose 2 specific time options or ask for availability.
- Do not ask for product/package details or sound like the prospect replying.
- Do not include personal-life lines (wife/birthday/party/etc).
- Do not mention internal fixes, diagnostics, scores, call outcomes, or anything like top fixes.
Context:
entity_name=${entityName}
scenario=${scenario}
call_why=${callWhy}
top_fixes=${topFixes.join(" | ")}
notes=${run.context_text || ""}
`.trim();

  let followText = "";
  try {
    const result = await deepseekJSON(system, basePrompt, 0.2);
    followText = String(result?.follow_up?.text || result?.follow_up || "").trim();
    if (!isFollowupValid(followText)) {
      const repairPrompt = `
Fix the follow-up. Return JSON only: { "follow_up": { "text": string } }
Hard rules: no placeholders, no transcript quotes, no speaker labels, <=1200 chars, one message only.
Must include a concrete next step or scheduling ask and a CTA question.
Must include the entity_name or scenario topic.
Do not ask for product/package details or sound like the prospect replying.
Do not include personal-life lines (wife/birthday/party/etc).
Do not mention internal fixes, diagnostics, scores, call outcomes, or anything like top fixes.
Context:
entity_name=${entityName}
scenario=${scenario}
call_why=${callWhy}
top_fixes=${topFixes.join(" | ")}
notes=${run.context_text || ""}
`.trim();
      const repaired = await deepseekJSON(system, repairPrompt, 0.2);
      followText = String(repaired?.follow_up?.text || repaired?.follow_up || "").trim();
    }
  } catch {
    followText = "";
  }
  if (!isFollowupValid(followText)) {
    followText = buildFallbackFollowup();
  }

  const updated = {
    ...(run.analysis_json || {}),
    follow_up: { text: followText },
  };

  const { data: saved, error: uErr } = await supabaseAdmin
    .from("runs")
    .update({ analysis_json: updated })
    .eq("id", id)
    .eq("user_id", user.id)
    .select()
    .single();

  if (uErr) {
    if (handleMissingUserId(res, "runs", uErr)) return;
    return res.status(400).json({ error: uErr.message });
  }
  res.json({ run: saved });
});

// -------- Async Run Endpoints --------

app.post("/api/run_async", upload.single("file"), async (req, res) => {
  const user = await requireUser(req, res);
  if (!user) return;

  const runId = crypto.randomUUID();
  const file = req.file;
  if (!file) return res.status(400).json({ error: "Missing file" });

  const scenario = (req.body.scenario || "").trim();
  const context = (req.body.context || "").trim();
  const entityId = (req.body.entityId || "").trim() || null;

  const { error: insertErr } = await supabaseAdmin
    .from("runs")
    .insert([
      {
        id: runId,
        user_id: user.id,
        status: "processing",
        progress_step: "queued",
        scenario,
        context_text: context,
        entity_id: entityId,
      },
    ]);
  if (insertErr) {
    if (handleMissingUserId(res, "runs", insertErr)) return;
    return res.status(400).json({ error: insertErr.message });
  }

  enqueueRunJob({
    runId,
    userId: user.id,
    filePath: file.path,
    originalname: file.originalname,
    mimetype: file.mimetype,
    scenario,
    context,
    entityId,
  });

  res.status(202).json({ run_id: runId, status: "processing" });
});

app.get("/api/run_status/:id", async (req, res) => {
  const user = await requireUser(req, res);
  if (!user) return;

  const id = req.params.id;
  const { data, error } = await supabaseAdmin
    .from("runs")
    .select("id, status, progress_step, error_text, created_at, outcome_label, transcript_lines, analysis_json")
    .eq("id", id)
    .eq("user_id", user.id)
    .maybeSingle();

  if (error) {
    if (handleMissingUserId(res, "runs", error)) return;
    return res.status(400).json({ error: error.message });
  }
  if (!data) return res.status(404).json({ error: "Not found" });

  const response = {
    run_id: data.id,
    status: data.status || "unknown",
    progress_step: data.progress_step || null,
    error_text: data.error_text || null,
    created_at: data.created_at || null,
  };
  if (data.status === "complete") {
    response.outcome_label = data.outcome_label || "Unknown";
    response.analysis_json_present = Boolean(data.analysis_json);
    response.transcript_lines_count = Array.isArray(data.transcript_lines)
      ? data.transcript_lines.length
      : 0;
  }
  res.json(response);
});

// -------- Main Run Endpoint --------

app.post("/api/run", upload.single("file"), async (req, res) => {
  const user = await requireUser(req, res);
  if (!user) return;

  const runId = crypto.randomUUID();
  const file = req.file;
  if (!file) return res.status(400).json({ error: "Missing file" });

  const scenario = (req.body.scenario || "").trim();
  const context = (req.body.context || "").trim();

  const entityId = (req.body.entityId || "").trim() || null;
  const entityName = (req.body.entityName || "").trim();
  const entityOffer = (req.body.entityOffer || "").trim();
  const entityIndustry = (req.body.entityIndustry || "").trim();

  let finalEntityId = entityId;

  try {
    let inputPath = file.path;
    let cleanupPaths = [file.path];

    // if mp4, extract audio
    const isMp4 =
      (file.mimetype || "").includes("video") ||
      (file.originalname || "").toLowerCase().endsWith(".mp4");

    if (isMp4) {
      const audioPath = await extractAudioFromMp4(file.path);
      inputPath = audioPath;
      cleanupPaths.push(audioPath);
    }

    // Create entity on the fly if provided
    if (finalEntityId) {
      const { data: entityRow, error: eErr } = await supabaseAdmin
        .from("entities")
        .select("id")
        .eq("id", finalEntityId)
        .eq("user_id", user.id)
        .maybeSingle();
      if (eErr) {
        if (handleMissingUserId(res, "entities", eErr)) return;
        throw new Error(eErr.message);
      }
      if (!entityRow) return res.status(404).json({ error: "Not found" });
    }

    if (!finalEntityId && entityName) {
      const { data: created, error: cErr } = await supabaseAdmin
        .from("entities")
        .insert([
          {
            user_id: user.id,
            name: entityName,
            offer: entityOffer,
            industry: entityIndustry,
          },
        ])
        .select()
        .single();

      if (cErr) {
        if (handleMissingUserId(res, "entities", cErr)) return;
        throw new Error(cErr.message);
      }
      finalEntityId = created.id;
    }

    // Transcribe
    const { transcriptText, transcriptJson, transcriptLines } = await assemblyTranscribe(inputPath);
    const artifactRoot = `artifacts/${runId}`;
    await uploadJsonArtifact(
      STORAGE_BUCKET,
      `${artifactRoot}/assembly_transcript_json.json`,
      transcriptJson
    );
    await uploadJsonArtifact(
      STORAGE_BUCKET,
      `${artifactRoot}/transcript_lines.json`,
      transcriptLines
    );
    const rawLines = transcriptLines || [];
    const finalTranscriptLines = finalizeTranscriptLines(rawLines);
    const transcriptHash = sha1(Buffer.from(transcriptText, "utf-8"));

    // Optional aggregate context for entity
    let entityAggregate = null;
    if (finalEntityId) {
      entityAggregate = await buildEntityAggregate(user.id, finalEntityId);
    }

    // Analyze
    const prompt = buildAnalysisPrompt({
      transcript: transcriptText,
      transcriptLines: finalTranscriptLines,
      context,
      scenario,
      entityAggregate,
    });

    const deepseekResult = await deepseekJSONWithRaw(prompt.system, prompt.user, 0.25);
    await uploadJsonArtifact(
      STORAGE_BUCKET,
      `${artifactRoot}/deepseek_raw.json`,
      { raw: deepseekResult.raw }
    );
    const rawAnalysis = deepseekResult.parsed;
    const outcomeLabel = await inferOutcomeLabel(rawAnalysis);
    let { fixedJson: finalAnalysis, errors } = validateAndCoerceAnalysisJson(
      rawAnalysis,
      finalTranscriptLines,
      outcomeLabel
    );
    let evidenceCount = finalAnalysis?.call_result?.evidence?.length || 0;

    if (errors.length > 0 || evidenceCount !== 36) {
      const errorLines = errors.slice(0, 10);
      if (evidenceCount !== 36) {
        errorLines.push(`evidence_count=${evidenceCount} (must be 36)`);
      }
      const numberedTranscript = buildNumberedTranscriptLines(finalTranscriptLines).join("\n");
      const repairSystem = `
You are a strict JSON fixer. Return a corrected FULL analysis JSON only.
Follow the required keys and types, and ensure evidence has exactly 36 items.
Each evidence item must reference a valid transcript line and include an exact substring quote from that line (keep quotes short).
`;
      const repairUser = `
Errors:
${errorLines.map((e) => `- ${e}`).join("\n")}

Transcript:
${numberedTranscript}

Return the corrected JSON only.
`;
      const repaired = await deepseekJSON(repairSystem.trim(), repairUser.trim(), 0.2);
      const repairedOutcome = await inferOutcomeLabel(repaired) || outcomeLabel;
      const repairedResult = validateAndCoerceAnalysisJson(
        repaired,
        finalTranscriptLines,
        repairedOutcome
      );
      finalAnalysis = repairedResult.fixedJson;
      evidenceCount = finalAnalysis?.call_result?.evidence?.length || 0;
      if (repairedResult.errors.length > 0 || evidenceCount !== 36) {
        finalAnalysis = buildFallbackAnalysisJson(repairedOutcome, finalTranscriptLines);
      }
    }

    const outcomeLabelFinal =
      finalAnalysis?.call_result?.label || outcomeLabel || "Unknown";
    await uploadJsonArtifact(
      STORAGE_BUCKET,
      `${artifactRoot}/analysis_final.json`,
      finalAnalysis
    );

    // Store run
    const runRow = {
      user_id: user.id,
      scenario,
      context_text: context,
      transcript_text: transcriptText,
      transcript_lines: finalTranscriptLines,
      transcript_json: transcriptJson,
      transcript_hash: transcriptHash,
      outcome_label: outcomeLabelFinal,
      analysis_json: finalAnalysis,
      entity_id: finalEntityId,
    };

    const { data: saved, error: sErr } = await supabaseAdmin
      .from("runs")
      .insert([runRow])
      .select()
      .single();

    if (sErr) {
      if (handleMissingUserId(res, "runs", sErr)) return;
      throw new Error(sErr.message);
    }

    const insertedId = saved?.id;
    const { data: savedFixed, error: fixErr } = await supabaseAdmin
      .from("runs")
      .update({ transcript_lines: finalTranscriptLines, analysis_json: finalAnalysis })
      .eq("id", insertedId)
      .select("*")
      .single();
    if (fixErr) {
      if (handleMissingUserId(res, "runs", fixErr)) return;
      throw new Error(fixErr.message);
    }

    // cleanup temp files
    cleanupPaths.forEach((p) => {
      try { fs.unlinkSync(p); } catch {}
    });

    const transcriptSpeakerCounts = finalTranscriptLines.reduce((acc, line) => {
      const key = normalizeSpeakerFinal(line?.speaker);
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});
    const worstPairCount = finalTranscriptLines.reduce((acc, line) => {
      const key = `${line.start_ms ?? "null"}|${line.end_ms ?? "null"}`;
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});
    const maxPairCount = Object.values(worstPairCount).reduce(
      (max, count) => (count > max ? count : max),
      0
    );
    // QA verification payload for transcript lines + speaker distribution.
    const artifactPaths = {
      assembly_transcript_json: `${artifactRoot}/assembly_transcript_json.json`,
      transcript_lines: `${artifactRoot}/transcript_lines.json`,
      deepseek_raw: `${artifactRoot}/deepseek_raw.json`,
      analysis_final: `${artifactRoot}/analysis_final.json`,
    };
    const diagnostics = {
      evidence_len: finalAnalysis.call_result.evidence.length,
      speaker_counts: transcriptSpeakerCounts,
      worst_pair_count: maxPairCount,
    };
    res.json({
      run: {
        ...savedFixed,
        analysis_json: finalAnalysis,
        transcript_lines: finalTranscriptLines,
        diagnostics: {
          ...diagnostics,
          artifacts: artifactPaths,
        },
      },
    });
  } catch (err) {
    try { fs.unlinkSync(file.path); } catch {}
    return res.status(400).json({ error: err.message || String(err) });
  }
});

app.use((err, req, res, next) => {
  if (!err) return next();
  if (res.headersSent) return next(err);

  if (err.code === "LIMIT_FILE_SIZE") {
    return res.status(413).json({
      error: "File too large",
      code: "FILE_TOO_LARGE",
      maxBytes: MAX_UPLOAD_BYTES,
    });
  }

  if (err.type === "entity.too.large" || err.status === 413) {
    return res.status(413).json({
      error: "File too large",
      code: "FILE_TOO_LARGE",
      maxBytes: MAX_UPLOAD_BYTES,
    });
  }

  return res.status(500).json({ error: "Server error", code: "SERVER_ERROR" });
});

// Serve SPA index for all GET routes (Express 5-safe)
const webDir = path.join(__dirname);
const indexPath = path.join(webDir, "index.html");

app.get(/^\/(?!api\/).*/, (req, res) => {
  res.sendFile(indexPath);
});

app.listen(PORT, () => {
  console.log(`Calibrate MVP running on :${PORT}`);
});
