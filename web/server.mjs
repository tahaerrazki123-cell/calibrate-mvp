// FILE: web/server.mjs
import express from "express";
import multer from "multer";
import path from "path";
import { fileURLToPath } from "url";
import { createClient } from "@supabase/supabase-js";

import { runCalibrate, ENFORCER_VERSION } from "../calibrate.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.set("trust proxy", 1); // Render/proxies
app.use(express.json());

// ---- Limits (server truth) ----
const MAX_UPLOAD_BYTES = 25 * 1024 * 1024; // 25MB
const MAX_RUNS_PER_24H = 10;

// One-run-at-a-time per user (simple in-memory lock)
const activeRunByUser = new Map(); // userId -> startedAtMs

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: MAX_UPLOAD_BYTES },
});

// serve static files (index.html, etc.)
app.use(express.static(__dirname, { extensions: ["html"] }));

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    enforcer: ENFORCER_VERSION,
    commit: process.env.RENDER_GIT_COMMIT || null,
  });
});

// Expose anon config for frontend (SAFE: anon key is meant for browser use)
app.get("/api/config", (req, res) => {
  res.json({
    supabaseUrl: process.env.SUPABASE_URL || "",
    supabaseAnonKey: process.env.SUPABASE_ANON_KEY || "",
    enforcer: ENFORCER_VERSION,
  });
});

// ---- Supabase clients (lazy singletons) ----
let _sbAnon = null;
let _sbService = null;

function supabaseAnon() {
  if (_sbAnon) return _sbAnon;
  const url = process.env.SUPABASE_URL;
  const anon = process.env.SUPABASE_ANON_KEY;
  if (!url || !anon) throw new Error("Missing SUPABASE_URL or SUPABASE_ANON_KEY");
  _sbAnon = createClient(url, anon, { auth: { persistSession: false } });
  return _sbAnon;
}

function supabaseService() {
  if (_sbService) return _sbService;
  const url = process.env.SUPABASE_URL;
  const service = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !service) throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  _sbService = createClient(url, service, { auth: { persistSession: false } });
  return _sbService;
}

async function requireUserId(req) {
  const auth = req.headers.authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
  if (!token) return null;

  const sb = supabaseAnon();
  const { data, error } = await sb.auth.getUser(token);
  if (error) return null;
  return data?.user?.id || null;
}

function pick(obj, keys, fallback = null) {
  for (const k of keys) {
    if (obj && obj[k] !== undefined && obj[k] !== null && obj[k] !== "") return obj[k];
  }
  return fallback;
}

// Normalize mismatch across shapes
function normalizeScenarioMismatch(row) {
  if (!row || typeof row !== "object") return null;
  if ("scenario_mismatch" in row) return row.scenario_mismatch;
  if ("mismatch" in row) return row.mismatch;
  if ("scenarioMismatch" in row) return row.scenarioMismatch;
  if ("scenario_mismatch_flag" in row) return row.scenario_mismatch_flag;
  if ("scenario_mismatch_text" in row) return Boolean(row.scenario_mismatch_text);
  if ("context_conflict_banner" in row) return Boolean(row.context_conflict_banner);
  return null;
}

// Insert, and if DB schema is missing a column, drop it and retry
async function insertRunResilient(sb, payload) {
  let p = { ...payload };

  for (let i = 0; i < 20; i++) {
    const { data, error } = await sb.from("runs").insert([p]).select("id").maybeSingle();
    if (!error) return data;

    const msg = String(error.message || "");

    // Postgres style: column runs.foo does not exist
    let m = msg.match(/column\s+\w+\.(\w+)\s+does not exist/i);
    if (m && m[1]) {
      delete p[m[1]];
      continue;
    }

    // PostgREST schema cache style:
    // "Could not find the 'transcript_text' column of 'runs' in the schema cache"
    m = msg.match(/Could not find the '(\w+)' column of '(\w+)' in the schema cache/i);
    if (m && m[1]) {
      delete p[m[1]];
      continue;
    }

    throw new Error(msg);
  }

  throw new Error("Insert failed after retries (schema mismatch).");
}

function wordCount(text) {
  return String(text || "").trim().split(/\s+/).filter(Boolean).length;
}

function cleanUtteranceText(t) {
  return String(t || "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Map AssemblyAI diarization speakers to:
 * - "You"/"Prospect" ONLY when confidence is high
 * - otherwise "Speaker A/B/C..." (neutral, trust-first)
 *
 * Returns:
 * {
 *   mapping: Map<speakerIdString, displayLabel>,
 *   diarization_confident: boolean,
 *   role_confident: boolean,
 *   reason: string
 * }
 */
function mapSpeakersTrustFirst(utterances) {
  const us = Array.isArray(utterances) ? utterances : [];
  const speakerOrder = [];
  const seen = new Set();

  for (const u of us) {
    const sid = String(u?.speaker ?? "");
    if (!sid) continue;
    if (!seen.has(sid)) {
      seen.add(sid);
      speakerOrder.push(sid);
    }
  }

  const stats = new Map(); // sid -> { words, turns }
  for (const u of us) {
    const sid = String(u?.speaker ?? "");
    if (!sid) continue;
    const txt = cleanUtteranceText(u?.text || "");
    const wc = wordCount(txt);
    const cur = stats.get(sid) || { words: 0, turns: 0 };
    cur.words += wc;
    cur.turns += 1;
    stats.set(sid, cur);
  }

  const speakers = [...stats.entries()]
    .map(([sid, s]) => ({ sid, words: s.words, turns: s.turns }))
    .sort((a, b) => b.words - a.words);

  const totalWords = speakers.reduce((sum, s) => sum + s.words, 0) || 1;

  // Build neutral labels by order of appearance (stable + honest).
  const neutralMapping = new Map();
  let code = "A".charCodeAt(0);
  for (const sid of speakerOrder) {
    neutralMapping.set(sid, `Speaker ${String.fromCharCode(code++)}`);
  }

  if (speakers.length < 2) {
    return {
      mapping: neutralMapping,
      diarization_confident: false,
      role_confident: false,
      reason: "Only one diarized speaker",
    };
  }

  const top = speakers[0];
  const second = speakers[1];
  const topShare = top.words / totalWords;
  const secondShare = second.words / totalWords;

  // Confidence that diarization is meaningfully 2-speaker and not totally lopsided.
  // (If this fails, we do NOT pretend we know who is who.)
  const diarization_confident =
    topShare <= 0.88 &&
    secondShare >= 0.12;

  // Role mapping heuristics (only used if diarization_confident)
  const repSignals = [
    /\bmy name is\b/i,
    /\bthis is\b/i,
    /\bi'm\b/i,
    /\bcalling\b/i,
    /\breaching out\b/i,
    /\bquick one\b/i,
    /\bkeep this\b/i,
    /\bdo you have\b/i,
    /\bis this\b/i,
    /\bfront desk\b/i,
    /\bcan i\b/i,
  ];
  const prospectSignals = [
    /\bnot interested\b/i,
    /\bjust email\b/i,
    /\bhow much\b/i,
    /\bwe already\b/i,
    /\bwe're good\b/i,
    /\bstop calling\b/i,
    /\bwho is this\b/i,
  ];

  function scoreSpeaker(sid) {
    const txt = us
      .filter(u => String(u?.speaker ?? "") === sid)
      .slice(0, 30)
      .map(u => cleanUtteranceText(u?.text || ""))
      .join(" ")
      .slice(0, 4000);

    let rep = 0, pro = 0;
    for (const r of repSignals) if (r.test(txt)) rep++;
    for (const p of prospectSignals) if (p.test(txt)) pro++;
    return { rep, pro, net: rep - pro };
  }

  const firstMeaningful = (() => {
    for (const u of us) {
      const txt = cleanUtteranceText(u?.text || "");
      if (wordCount(txt) >= 2) return String(u?.speaker ?? "");
    }
    return "";
  })();

  let role_confident = false;
  let youSid = "";
  let prospectSid = "";

  if (diarization_confident) {
    const a = scoreSpeaker(top.sid);
    const b = scoreSpeaker(second.sid);
    const diff = Math.abs(a.net - b.net);

    // If one speaker is strongly "rep-like", use that.
    if (diff >= 2) {
      role_confident = true;
      if (a.net > b.net) {
        youSid = top.sid;
        prospectSid = second.sid;
      } else {
        youSid = second.sid;
        prospectSid = top.sid;
      }
    } else if (firstMeaningful && (firstMeaningful === top.sid || firstMeaningful === second.sid)) {
      // Cold-call assumption: rep usually starts.
      // Only apply if the first speaker's early text looks like a greeting/intro.
      const firstTxt = us
        .filter(u => String(u?.speaker ?? "") === firstMeaningful)
        .slice(0, 2)
        .map(u => cleanUtteranceText(u?.text || ""))
        .join(" ")
        .toLowerCase();

      const looksLikeIntro =
        firstTxt.includes("hi") ||
        firstTxt.includes("hello") ||
        firstTxt.includes("hey") ||
        firstTxt.includes("my name") ||
        firstTxt.includes("this is") ||
        firstTxt.includes("calling");

      if (looksLikeIntro) {
        role_confident = true;
        youSid = firstMeaningful;
        prospectSid = (youSid === top.sid) ? second.sid : top.sid;
      }
    }
  }

  const mapping = new Map(neutralMapping);

  if (role_confident && youSid && prospectSid) {
    mapping.set(youSid, "You");
    mapping.set(prospectSid, "Prospect");
  }

  const reason = !diarization_confident
    ? "Imbalanced/unclear diarization (neutral speaker labels used)"
    : (role_confident ? "Two-speaker + role mapping passed" : "Two-speaker likely, but role mapping not confident (neutral labels used)");

  return { mapping, diarization_confident, role_confident, reason };
}

function utterancesToTranscriptLines(utterances) {
  const us = Array.isArray(utterances) ? utterances : [];
  const speakerMapInfo = mapSpeakersTrustFirst(us);
  const mapping = speakerMapInfo.mapping;

  // Merge consecutive utterances from same display speaker (cleaner transcript)
  const lines = [];
  let cur = null;

  for (const u of us) {
    const text = cleanUtteranceText(u?.text || "");
    if (!text) continue;

    const sid = String(u?.speaker ?? "");
    const speaker = mapping.get(sid) || "Speaker";
    if (cur && cur.speaker === speaker) {
      cur.text += " " + text;
    } else {
      cur = { speaker, text };
      lines.push(cur);
    }
  }

  return { transcript_lines: lines, speaker_map: speakerMapInfo };
}

function linesToTranscriptText(lines) {
  return (lines || [])
    .map(l => `${l.speaker}: ${l.text}`)
    .join("\n")
    .trim();
}

// -------------------- Runs API (history + details) --------------------

app.get("/api/runs", async (req, res) => {
  try {
    const userId = await requireUserId(req);
    if (!userId) return res.status(401).json({ error: "Unauthorized" });

    const sb = supabaseService();

    const { data, error } = await sb
      .from("runs")
      .select("*")
      .eq("user_id", userId)
      .order("created_at", { ascending: false })
      .limit(50);

    if (error) return res.status(500).json({ error: error.message });

    const runs = (data || []).map((r) => {
      const mismatch = Boolean(normalizeScenarioMismatch(r));
      return {
        id: r.id,
        created_at: r.created_at,
        scenario_template: r.scenario_template ?? r.scenarioTemplate ?? r.category ?? null,
        call_outcome: r.call_outcome ?? r.callOutcome ?? r.outcome ?? null,
        enforcer: r.enforcer ?? r.enforcer_version ?? r.enforcerVersion ?? ENFORCER_VERSION,
        mismatch,
        scenario_mismatch: mismatch,
      };
    });

    return res.json({ runs });
  } catch (err) {
    return res.status(500).json({ error: err?.message || String(err) });
  }
});

app.get("/api/runs/:id", async (req, res) => {
  try {
    const userId = await requireUserId(req);
    if (!userId) return res.status(401).json({ error: "Unauthorized" });

    const runId = req.params.id;
    const sb = supabaseService();

    const { data, error } = await sb
      .from("runs")
      .select("*")
      .eq("id", runId)
      .eq("user_id", userId)
      .maybeSingle();

    if (error) return res.status(500).json({ error: error.message });
    if (!data) return res.status(404).json({ error: "Run not found" });

    const mismatch = Boolean(normalizeScenarioMismatch(data));
    const run = { ...data, mismatch, scenario_mismatch: mismatch };

    return res.json({ run });
  } catch (err) {
    return res.status(500).json({ error: err?.message || String(err) });
  }
});

// -------------------- Calibrate endpoint (PERSIST RUNS) --------------------

function prettifyTranscript(raw) {
  let t = (raw ?? "").toString().trim();
  if (!t) return "";

  t = t.replace(/\r\n/g, "\n");

  // Normalize speaker labels (handles Speaker A/B AND Speaker 0/1/2...)
  t = t.replace(/\s*(Speaker\s*(?:[A-Za-z]|\d+)\s*:)\s*/gim, "\n$1 ");

  // Normalize common role labels into their own lines
  t = t.replace(/\s*(Prospect|Rep|Caller|Agent|Customer)\s*[:.]\s*/gi, "\n$1: ");

  // If sentence continues then hits a label, force a new line
  t = t.replace(/([.!?])\s+(Prospect|Rep|Caller|Agent|Customer)\s+(?=[A-Za-z0-9])/gi, "$1\n$2: ");
  t = t.replace(/([.!?])\s+(Prospect|Rep|Caller|Agent|Customer)\s*[:.]\s*/gi, "$1\n$2: ");

  t = t
    .replace(/[ \t]+/g, " ")
    .replace(/\n[ \t]+/g, "\n")
    .replace(/\n{2,}/g, "\n")
    .trim();

  let lines = t.split("\n").map((l) => l.trim()).filter(Boolean);

  // Accept Speaker A/B OR Speaker 0/1/2...
  const labelRe = /^(Speaker\s*(?:[A-Za-z]|\d+)|Prospect|Rep|Caller|Agent|Customer)\s*:\s*(.*)$/i;

  lines = lines.map((line) => {
    const m = line.match(labelRe);
    if (!m) return line;

    let label = (m[1] || "").replace(/\s+/g, " ").trim();
    const text = (m[2] || "").trim();

    // Standardize "Speaker0" -> "Speaker 0"
    label = label.replace(/^speaker\s*([a-z]|\d+)$/i, "Speaker $1");

    const lower = label.toLowerCase();

    // Keep diarization speakers neutral (frontend will style, but we avoid lying)
    if (lower.startsWith("speaker")) return `${label}: ${text}`.trim();

    if (lower === "prospect" || lower === "customer") return `Prospect: ${text}`.trim();

    // Rep/Caller/Agent => You (role labels are explicit)
    return `You: ${text}`.trim();
  });

  // Prevent accidental double-prefixes
  lines = lines.map((l) =>
    l.replace(/^You:\s*Prospect:\s*/i, "Prospect: ").replace(/^Prospect:\s*You:\s*/i, "You: ")
  );

  return lines.join("\n");
}

async function transcribeWithAssemblyAI(audioBuffer) {
  const apiKey = process.env.ASSEMBLYAI_API_KEY;
  if (!apiKey) throw new Error("Missing ASSEMBLYAI_API_KEY in environment.");

  const uploadRes = await fetch("https://api.assemblyai.com/v2/upload", {
    method: "POST",
    headers: { authorization: apiKey },
    body: audioBuffer,
  });
  if (!uploadRes.ok) throw new Error("AssemblyAI upload failed: " + (await uploadRes.text().catch(() => "")));
  const { upload_url: audio_url } = await uploadRes.json();

  const createRes = await fetch("https://api.assemblyai.com/v2/transcript", {
    method: "POST",
    headers: { authorization: apiKey, "content-type": "application/json" },
    body: JSON.stringify({
      audio_url,
      punctuate: true,
      format_text: true,
      speaker_labels: true,
    }),
  });
  if (!createRes.ok) throw new Error("AssemblyAI transcript create failed: " + (await createRes.text().catch(() => "")));
  const { id } = await createRes.json();

  const started = Date.now();
  while (true) {
    const pollRes = await fetch(`https://api.assemblyai.com/v2/transcript/${id}`, {
      headers: { authorization: apiKey },
    });
    if (!pollRes.ok) throw new Error("AssemblyAI transcript poll failed: " + (await pollRes.text().catch(() => "")));
    const pollJson = await pollRes.json();

    if (pollJson.status === "completed") {
      return {
        text: (pollJson.text || "").trim(),
        utterances: Array.isArray(pollJson.utterances) ? pollJson.utterances : [],
        id,
      };
    }

    if (pollJson.status === "error") throw new Error("AssemblyAI transcription error: " + (pollJson.error || "unknown"));
    if (Date.now() - started > 4 * 60 * 1000) throw new Error("AssemblyAI transcription timed out.");

    await new Promise((r) => setTimeout(r, 1500));
  }
}

app.post("/api/run", upload.single("audio"), async (req, res) => {
  const userId = await requireUserId(req);
  if (!userId) return res.status(401).json({ error: "Unauthorized" });

  if (activeRunByUser.has(userId)) {
    return res.status(429).json({ error: "A run is already in progress. Please wait." });
  }

  try {
    activeRunByUser.set(userId, Date.now());

    const file = req.file;
    if (!file?.buffer) return res.status(400).json({ error: "No audio file uploaded (field name must be 'audio')." });

    const userContext = (req.body?.context || "").trim();
    const category = (req.body?.category || "").trim();
    const legacyScenario = (req.body?.legacyScenario || "").trim();
    if (userContext.length < 10) return res.status(400).json({ error: "Context is required (10+ characters)." });

    // per-24h limit
    const cutoffIso = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    const sb = supabaseService();

    const { count, error: countErr } = await sb
      .from("runs")
      .select("id", { count: "exact", head: true })
      .eq("user_id", userId)
      .gte("created_at", cutoffIso);

    if (countErr) return res.status(500).json({ error: countErr.message });
    if ((count || 0) >= MAX_RUNS_PER_24H) {
      return res.status(429).json({ error: `Daily limit reached (${MAX_RUNS_PER_24H}/24h). Try again later.` });
    }

    const tx = await transcribeWithAssemblyAI(file.buffer);

    // Prefer utterances (best shot at diarization), but remain trust-first.
    let transcript_lines = [];
    let speaker_map = null;

    if (Array.isArray(tx.utterances) && tx.utterances.length) {
      const built = utterancesToTranscriptLines(tx.utterances);
      transcript_lines = built.transcript_lines;
      speaker_map = built.speaker_map;
    } else {
      const prettified = prettifyTranscript(tx.text || "");
      transcript_lines = prettified
        .split("\n")
        .map(s => s.trim())
        .filter(Boolean)
        .map(line => {
          const m = line.match(/^([^:]{1,40}):\s*(.*)$/);
          if (!m) return { speaker: "Other", text: line };
          const speaker = (m[1] || "Other").replace(/\s+/g, " ").trim();
          return { speaker, text: (m[2] || "").trim() };
        });
      speaker_map = {
        diarization_confident: false,
        role_confident: false,
        reason: "No utterances returned; used plain transcript text",
      };
    }

    const transcript = linesToTranscriptText(transcript_lines);

    const result = await runCalibrate({ transcript, userContext, category, legacyScenario });

    // attach diarization meta (frontend uses this for trust warnings + scorecard)
    const diagnostics = Object.assign({}, result, { speaker_map });

    const reportText = pick(result, ["report", "coachingReport", "report_md", "reportMarkdown"], "") || "";
    const callOutcome = pick(result, ["call_outcome", "callOutcome", "outcome"], null);

    const mismatchReason =
      pick(result, ["context_conflict_banner", "contextConflictBanner", "scenarioMismatch", "scenario_mismatch"], "") || "";
    const mismatch = Boolean(mismatchReason);

    const row = {
      user_id: userId,
      user_context: userContext,
      scenario_template: category || null,
      legacy_scenario: legacyScenario || null,
      transcript_lines,
      report_text: reportText,
      diagnostics,
      call_outcome: callOutcome,
      enforcer: ENFORCER_VERSION,
      scenario_mismatch: mismatch,
      mismatch_reason: mismatchReason || null,
      // NOTE: DO NOT send transcript_text unless you actually add that column in Supabase
    };

    const inserted = await insertRunResilient(sb, row);
    return res.json({ run_id: inserted?.id || null });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err?.message || String(err) });
  } finally {
    activeRunByUser.delete(userId);
  }
});

// If someone hits an unknown /api route, return JSON (NOT index.html)
app.use("/api", (req, res) => res.status(404).json({ error: "Unknown API route" }));

// SPA fallback for non-API routes (works on Render/Node 22; avoids "*" path issues)
app.get(/^(?!\/api\/).*/, (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
app.listen(PORT, () => {
  console.log(`Calibrate MVP running on port ${PORT}`);
  console.log(`Enforcer loaded: ${ENFORCER_VERSION}`);
});
