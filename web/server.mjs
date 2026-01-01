import express from "express";
import multer from "multer";
import fs from "fs";
import fsp from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import crypto from "crypto";
import { spawn } from "child_process";
import ffmpegStatic from "ffmpeg-static";
import { createClient } from "@supabase/supabase-js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = process.env.PORT || 3000;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;

const ASSEMBLYAI_API_KEY = process.env.ASSEMBLYAI_API_KEY;

const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY;
const DEEPSEEK_BASE_URL = process.env.DEEPSEEK_BASE_URL || "https://api.deepseek.com";

const MAX_UPLOAD_MB = Number(process.env.MAX_UPLOAD_MB || 250);
const PLAYBOOK_MAX_RUNS = Number(process.env.PLAYBOOK_MAX_RUNS || 30);

function requireEnv(name, value) {
  if (!value) throw new Error(`Missing required env var: ${name}`);
}
requireEnv("SUPABASE_URL", SUPABASE_URL);
requireEnv("SUPABASE_SERVICE_ROLE_KEY", SUPABASE_SERVICE_ROLE_KEY);
requireEnv("SUPABASE_ANON_KEY", SUPABASE_ANON_KEY);
requireEnv("ASSEMBLYAI_API_KEY", ASSEMBLYAI_API_KEY);
requireEnv("DEEPSEEK_API_KEY", DEEPSEEK_API_KEY);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const app = express();
app.use(express.json({ limit: "2mb" }));
app.use(express.urlencoded({ extended: true }));

const TMP_DIR = path.join(__dirname, "tmp");
await fsp.mkdir(TMP_DIR, { recursive: true });

function safeId(len = 12) {
  return crypto.randomBytes(len).toString("hex");
}

function extFromMimetype(mime) {
  if (!mime) return "";
  if (mime.includes("mpeg")) return ".mp3";
  if (mime.includes("wav")) return ".wav";
  if (mime.includes("mp4")) return ".mp4";
  if (mime.includes("quicktime")) return ".mov";
  if (mime.includes("m4a")) return ".m4a";
  return "";
}

function isVideoMimetype(mime) {
  return (mime || "").startsWith("video/");
}

function isAudioMimetype(mime) {
  return (mime || "").startsWith("audio/");
}

const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, TMP_DIR),
    filename: (req, file, cb) => {
      const ext = path.extname(file.originalname) || extFromMimetype(file.mimetype) || "";
      cb(null, `${Date.now()}_${safeId(8)}${ext}`);
    },
  }),
  limits: { fileSize: MAX_UPLOAD_MB * 1024 * 1024 },
});

// ---- AssemblyAI helpers ----
async function assemblyUpload(filepath) {
  const stream = fs.createReadStream(filepath);
  const res = await fetch("https://api.assemblyai.com/v2/upload", {
    method: "POST",
    headers: { authorization: ASSEMBLYAI_API_KEY },
    body: stream,
    duplex: "half",
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`AssemblyAI upload failed (${res.status}): ${txt}`);
  }
  const data = await res.json();
  if (!data.upload_url) throw new Error("AssemblyAI upload: missing upload_url");
  return data.upload_url;
}

async function assemblyCreateTranscript(uploadUrl) {
  const res = await fetch("https://api.assemblyai.com/v2/transcript", {
    method: "POST",
    headers: {
      authorization: ASSEMBLYAI_API_KEY,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      audio_url: uploadUrl,
      speaker_labels: true,
      punctuate: true,
      format_text: true,
    }),
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`AssemblyAI create transcript failed (${res.status}): ${txt}`);
  }
  const data = await res.json();
  if (!data.id) throw new Error("AssemblyAI create transcript: missing id");
  return data.id;
}

async function assemblyPollTranscript(transcriptId, timeoutMs = 12 * 60 * 1000) {
  const start = Date.now();
  while (true) {
    if (Date.now() - start > timeoutMs) throw new Error("AssemblyAI transcript timed out");

    const res = await fetch(`https://api.assemblyai.com/v2/transcript/${transcriptId}`, {
      headers: { authorization: ASSEMBLYAI_API_KEY },
    });

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`AssemblyAI poll failed (${res.status}): ${txt}`);
    }

    const data = await res.json();
    if (data.status === "completed") return data;
    if (data.status === "error") throw new Error(`AssemblyAI error: ${data.error}`);

    await new Promise((r) => setTimeout(r, 2500));
  }
}

// ---- Video -> audio extraction ----
async function extractAudioToWav(videoPath) {
  const ffmpegPath = ffmpegStatic;
  if (!ffmpegPath) throw new Error("ffmpeg-static did not provide a binary path");

  const outPath = path.join(TMP_DIR, `${Date.now()}_${safeId(8)}.wav`);
  const args = ["-y", "-i", videoPath, "-vn", "-ac", "1", "-ar", "16000", "-f", "wav", outPath];

  await new Promise((resolve, reject) => {
    const p = spawn(ffmpegPath, args, { stdio: ["ignore", "ignore", "pipe"] });
    let err = "";
    p.stderr.on("data", (d) => (err += d.toString()));
    p.on("error", reject);
    p.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`ffmpeg failed (code ${code}): ${err.slice(0, 1200)}`));
    });
  });

  return outPath;
}

// ---- Transcript formatting ----
function normalizeTranscript(assemblyData) {
  const utterances = Array.isArray(assemblyData.utterances) ? assemblyData.utterances : [];
  if (utterances.length === 0) return { text: assemblyData.text || "", turns: [] };

  const firstSpeaker = utterances[0]?.speaker;
  const turns = utterances.map((u) => ({
    speakerRaw: u.speaker,
    speaker: u.speaker === firstSpeaker ? "You" : "Prospect",
    start: u.start,
    end: u.end,
    text: (u.text || "").trim(),
  }));

  const text = turns
    .filter((t) => t.text)
    .map((t) => `${t.speaker}: ${t.text}`)
    .join("\n");

  return { text, turns };
}

// ---- DeepSeek chat ----
async function deepseekChat({ messages, temperature = 0.2, max_tokens = 1200, model = "deepseek-chat" }) {
  const res = await fetch(`${DEEPSEEK_BASE_URL}/chat/completions`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${DEEPSEEK_API_KEY}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model,
      messages,
      temperature,
      max_tokens,
      response_format: { type: "json_object" },
    }),
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`DeepSeek failed (${res.status}): ${txt}`);
  }
  const data = await res.json();
  return data?.choices?.[0]?.message?.content || "";
}

function safeJsonParse(maybeJson) {
  try {
    return JSON.parse(maybeJson);
  } catch {
    const m = String(maybeJson).match(/\{[\s\S]*\}/);
    if (!m) throw new Error("Model did not return JSON.");
    return JSON.parse(m[0]);
  }
}

const ENFORCER_VERSION = "ENFORCER_V6_2025-12-31";

function buildCallAnalysisPrompt({ context, scenario, transcriptText }) {
  return [
    {
      role: "system",
      content:
        `You are Calibrate: a brutally practical post-call decision engine for sales calls.
Return ONLY valid JSON (no markdown). No fluff. Make it immediately actionable.
If transcript is short or unclear, say so and still give best actions.
Enforcer=${ENFORCER_VERSION}.`,
    },
    {
      role: "user",
      content: JSON.stringify(
        {
          scenario: scenario || null,
          user_context: context || "",
          transcript: transcriptText || "",
          output_contract: {
            call_result: "One of: Booked Meeting | Rejected | Connected | No Show | Hostile | Voicemail | Other",
            call_result_reason: "One sentence why",
            overall_grade: "A|B|C|D|F",
            overall_score_0_100: 0,
            signal_badges: ["short badges, 2-6 items"],
            followup: {
              two_line: "two lines max, no emojis unless asked",
              alt_versions: ["optional, up to 2"],
            },
            fix_these_first_top3: [
              {
                issue: "short name",
                why_it_hurt: "short explanation",
                do_this_instead: "specific behavior replacement",
                example_line: "a single sentence example",
                evidence_quote: "short quote from transcript if possible",
              },
            ],
            repackaged_sections: {
              section_1_call_summary: "tight paragraph",
              section_2_objections: [{ objection: "string", best_response: "string", avoid: "string" }],
              section_3_offer_clarity: { what_was_clear: "string", what_was_missing: "string" },
              section_4_script_upgrade: { opener: "string", discovery_questions: ["strings"], close: "string" },
              section_5_moments_to_review: [{ moment: "string", why: "string", timestamp_hint: "optional" }],
              section_6_next_call_micro_plan: ["step 1", "step 2", "step 3"],
            },
          },
        },
        null,
        2
      ),
    },
  ];
}

function buildPlaybookPrompt({ entity, runs }) {
  const compactRuns = runs.map((r) => ({
    created_at: r.created_at,
    call_result: r.call_result,
    transcript: r.transcript_text,
  }));

  return [
    { role: "system", content: `You are Calibrate Playbook Engine. Return ONLY valid JSON. No fluff.` },
    {
      role: "user",
      content: JSON.stringify(
        {
          entity: {
            id: entity.id,
            name: entity.name,
            offer: entity.offer || "",
            industry: entity.industry || "",
            notes: entity.notes || "",
          },
          calls: compactRuns,
          output_contract: {
            ultimate_script: {
              opener: "string",
              permission_based_bridge: "string",
              qualifying_block: ["questions"],
              value_proof_block: "string",
              objection_map: [
                { objection: "string", best_response: "string", fallback: "string", dont_say: "string" },
              ],
              close: "string",
            },
            what_to_say_more: ["bullets"],
            what_to_stop_saying: ["bullets"],
            patterns_across_calls: ["bullets"],
            quick_drills: ["bullets"],
            next_experiments: [{ experiment: "string", hypothesis: "string", success_metric: "string" }],
          },
        },
        null,
        2
      ),
    },
  ];
}

// ---- API ----
app.get("/health", (req, res) => res.json({ ok: true, service: "calibrate", enforcer: ENFORCER_VERSION }));

// Frontend needs anon info for auth (safe to expose anon + url)
app.get("/api/config", (req, res) => {
  res.json({
    supabaseUrl: SUPABASE_URL,
    supabaseAnonKey: SUPABASE_ANON_KEY,
  });
});

app.get("/api/entities", async (req, res) => {
  try {
    const { data, error } = await supabase.from("entities").select("*").order("created_at", { ascending: false });
    if (error) throw error;
    res.json({ entities: data || [] });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

app.post("/api/entities", async (req, res) => {
  try {
    const { name, offer, industry, notes } = req.body || {};
    if (!name || !String(name).trim()) return res.status(400).json({ error: "Missing entity name" });

    const payload = {
      name: String(name).trim(),
      offer: offer ? String(offer).trim() : null,
      industry: industry ? String(industry).trim() : null,
      notes: notes ? String(notes).trim() : null,
    };

    const { data, error } = await supabase.from("entities").insert(payload).select("*").single();
    if (error) throw error;

    res.json({ entity: data });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

app.get("/api/entities/:id", async (req, res) => {
  try {
    const id = req.params.id;

    const { data: entity, error: e1 } = await supabase.from("entities").select("*").eq("id", id).single();
    if (e1) throw e1;

    const { data: runs, error: e2 } = await supabase
      .from("runs")
      .select("id, created_at, filename, call_result, overall_score, overall_grade")
      .eq("entity_id", id)
      .order("created_at", { ascending: false })
      .limit(50);
    if (e2) throw e2;

    const { data: pb, error: e3 } = await supabase
      .from("entity_playbooks")
      .select("*")
      .eq("entity_id", id)
      .order("created_at", { ascending: false })
      .limit(1);
    if (e3) throw e3;

    res.json({ entity, runs: runs || [], latest_playbook: (pb && pb[0]) || null });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

app.post("/api/entities/:id/playbook", async (req, res) => {
  try {
    const id = req.params.id;

    const { data: entity, error: e1 } = await supabase.from("entities").select("*").eq("id", id).single();
    if (e1) throw e1;

    const { data: runs, error: e2 } = await supabase
      .from("runs")
      .select("created_at, call_result, transcript_text")
      .eq("entity_id", id)
      .order("created_at", { ascending: false })
      .limit(PLAYBOOK_MAX_RUNS);
    if (e2) throw e2;

    const usableRuns = (runs || []).filter((r) => r.transcript_text && String(r.transcript_text).trim().length > 40);
    if (usableRuns.length === 0) return res.status(400).json({ error: "Not enough transcripts under this entity yet." });

    const prompt = buildPlaybookPrompt({ entity, runs: usableRuns });
    const raw = await deepseekChat({ messages: prompt, temperature: 0.25, max_tokens: 1600 });
    const playbook = safeJsonParse(raw);

    const row = {
      entity_id: id,
      model: "deepseek-chat",
      prompt_version: "PLAYBOOK_V1",
      playbook_json: playbook,
    };

    const { data: saved, error: e3 } = await supabase.from("entity_playbooks").insert(row).select("*").single();
    if (e3) throw e3;

    res.json({ playbook: saved });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

app.get("/api/runs", async (req, res) => {
  try {
    const entityId = req.query.entity_id || null;

    let q = supabase
      .from("runs")
      .select("id, created_at, filename, call_result, overall_grade, overall_score, entity_id")
      .order("created_at", { ascending: false })
      .limit(200);

    if (entityId) q = q.eq("entity_id", entityId);

    const { data, error } = await q;
    if (error) throw error;

    res.json({ runs: data || [] });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

app.get("/api/runs/:id", async (req, res) => {
  try {
    const id = req.params.id;
    const { data, error } = await supabase.from("runs").select("*").eq("id", id).single();
    if (error) throw error;
    res.json({ run: data });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

app.post("/api/run", upload.single("file"), async (req, res) => {
  const cleanup = [];
  try {
    if (!req.file) return res.status(400).json({ error: "Missing file" });

    const {
      context = "",
      scenario = "",
      legacyScenario = "",
      entityId = "",
      entityName = "",
      entityOffer = "",
      entityIndustry = "",
    } = req.body || {};

    let entity_id = entityId || null;

    if (!entity_id && entityName && String(entityName).trim()) {
      const name = String(entityName).trim();

      const { data: found, error: fe } = await supabase.from("entities").select("*").eq("name", name).limit(1);
      if (fe) throw fe;

      if (found && found[0]) {
        entity_id = found[0].id;
      } else {
        const { data: created, error: ce } = await supabase
          .from("entities")
          .insert({
            name,
            offer: entityOffer ? String(entityOffer).trim() : null,
            industry: entityIndustry ? String(entityIndustry).trim() : null,
          })
          .select("*")
          .single();
        if (ce) throw ce;
        entity_id = created.id;
      }
    }

    const inPath = req.file.path;
    cleanup.push(inPath);

    const mime = req.file.mimetype || "";
    const original = req.file.originalname || path.basename(inPath);

    let audioPath = inPath;

    if (isVideoMimetype(mime) || /\.(mp4|mov|mkv|webm)$/i.test(original)) {
      audioPath = await extractAudioToWav(inPath);
      cleanup.push(audioPath);
    } else if (!isAudioMimetype(mime) && !/\.(mp3|wav|m4a|aac|ogg)$/i.test(original)) {
      return res.status(400).json({ error: "Unsupported file type. Upload audio (mp3/wav/m4a) or video (mp4/mov)." });
    }

    const uploadUrl = await assemblyUpload(audioPath);
    const transcriptId = await assemblyCreateTranscript(uploadUrl);
    const assemblyData = await assemblyPollTranscript(transcriptId);

    const { text: transcriptText, turns } = normalizeTranscript(assemblyData);

    const prompt = buildCallAnalysisPrompt({ context, scenario: scenario || legacyScenario, transcriptText });
    const raw = await deepseekChat({ messages: prompt, temperature: 0.2, max_tokens: 1700 });
    const analysis = safeJsonParse(raw);

    const call_result = analysis?.call_result || "Other";
    const call_result_reason = analysis?.call_result_reason || "";
    const overall_grade = analysis?.overall_grade || "C";
    const overall_score = Number(analysis?.overall_score_0_100 ?? 60);

    const runRow = {
      filename: original,
      scenario: scenario || null,
      legacy_scenario: legacyScenario || null,
      context: context || "",
      entity_id,
      transcript_text: transcriptText || "",
      transcript_json: { turns, assembly: { id: transcriptId } },
      analysis_json: analysis,
      call_result,
      call_result_reason,
      overall_grade,
      overall_score,
      enforcer_version: ENFORCER_VERSION,
    };

    const { data: saved, error: se } = await supabase.from("runs").insert(runRow).select("*").single();
    if (se) throw se;

    res.json({ run: saved });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  } finally {
    for (const p of cleanup) {
      try { await fsp.unlink(p); } catch {}
    }
  }
});

// Serve frontend
app.use(express.static(path.join(__dirname)));

// SPA fallback (REGEX to avoid path-to-regexp wildcard crash)
app.get(/^\/(?!api\/).*/, (req, res, next) => {
  if (req.path.includes(".")) return next();
  return res.sendFile(path.join(__dirname, "index.html"));
});

app.listen(PORT, () => {
  console.log(`Calibrate listening on :${PORT}`);
});
