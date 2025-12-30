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
app.set("trust proxy", 1);
app.use(express.json({ limit: "200kb" }));

// ---- Limits (server truth) ----
const MAX_UPLOAD_BYTES = 25 * 1024 * 1024; // 25MB
const MAX_RUNS_PER_24H = 10;

// Support anti-spam
const MAX_SUPPORT_PER_24H = 20;

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

async function requireUserInfo(req) {
  const auth = req.headers.authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
  if (!token) return null;

  const sb = supabaseAnon();
  const { data, error } = await sb.auth.getUser(token);
  if (error) return null;

  const user = data?.user || null;
  if (!user?.id) return null;

  return { userId: user.id, email: user.email || null };
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

    let m = msg.match(/column\s+\w+\.(\w+)\s+does not exist/i);
    if (m && m[1]) {
      delete p[m[1]];
      continue;
    }

    m = msg.match(/Could not find the '(\w+)' column of '(\w+)' in the schema cache/i);
    if (m && m[1]) {
      delete p[m[1]];
      continue;
    }

    throw new Error(msg);
  }

  throw new Error("Insert failed after retries (schema mismatch).");
}

// -------------------- Support API (reliable) --------------------
app.post("/api/support", async (req, res) => {
  try {
    const info = await requireUserInfo(req);
    if (!info) return res.status(401).json({ error: "Unauthorized" });

    const message = String(req.body?.message ?? "").trim();
    const contact = String(req.body?.contact ?? "").trim();
    const page = String(req.body?.page ?? "").trim();

    if (message.length < 5) return res.status(400).json({ error: "Message too short" });
    if (message.length > 4000) return res.status(400).json({ error: "Message too long" });
    if (contact.length > 200) return res.status(400).json({ error: "Contact too long" });
    if (page.length > 300) return res.status(400).json({ error: "Page too long" });

    const sb = supabaseService();

    // rate limit: max N per 24h
    const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    const { count, error: countErr } = await sb
      .from("support_tickets")
      .select("id", { count: "exact", head: true })
      .eq("user_id", info.userId)
      .gte("created_at", since);

    if (countErr) {
      return res.status(500).json({
        error: "Support storage not ready",
        detail: countErr.message || String(countErr),
      });
    }

    if ((count || 0) >= MAX_SUPPORT_PER_24H) {
      return res.status(429).json({ error: `Too many support messages today (${MAX_SUPPORT_PER_24H}/24h).` });
    }

    const payload = {
      user_id: info.userId,
      user_email: info.email,
      contact: contact || null,
      message,
      page: page || null,
      user_agent: req.headers["user-agent"] || null,
      app_version: process.env.RENDER_GIT_COMMIT || process.env.GIT_COMMIT || null,
      status: "new",
    };

    const { data: inserted, error: insErr } = await sb
      .from("support_tickets")
      .insert(payload)
      .select("id, created_at")
      .single();

    if (insErr) {
      return res.status(500).json({ error: "Failed to save support message", detail: insErr.message || String(insErr) });
    }

    return res.json({ ok: true, ticket_id: inserted.id, created_at: inserted.created_at });
  } catch (err) {
    return res.status(500).json({ error: err?.message || String(err) });
  }
});

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
      const runTypeRaw = r.run_type ?? r.runType ?? r.type ?? "upload";
      const type =
        String(runTypeRaw).toLowerCase() === "upload" ? "Upload" :
        String(runTypeRaw).toLowerCase() === "record" ? "Record" :
        String(runTypeRaw).toLowerCase() === "practice" ? "Practice" :
        "Upload";

      return {
        id: r.id,
        created_at: r.created_at,
        scenario_template: r.scenario_template ?? r.scenarioTemplate ?? r.category ?? null,
        call_outcome: r.call_outcome ?? r.callOutcome ?? r.outcome ?? null,
        type,
        run_type: runTypeRaw,
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
    const runTypeRaw = data.run_type ?? data.runType ?? data.type ?? "upload";
    const type =
      String(runTypeRaw).toLowerCase() === "upload" ? "Upload" :
      String(runTypeRaw).toLowerCase() === "record" ? "Record" :
      String(runTypeRaw).toLowerCase() === "practice" ? "Practice" :
      "Upload";

    const run = { ...data, mismatch, scenario_mismatch: mismatch, run_type: runTypeRaw, type };

    return res.json({ run });
  } catch (err) {
    return res.status(500).json({ error: err?.message || String(err) });
  }
});

// -------------------- Calibrate endpoint (PERSIST RUNS) --------------------

// NEW: Stable speaker labeling (never "Speaker X") + optional You/Prospect inference
const SPEAKER_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".split("");

function normalizeSpeakerKey(raw) {
  if (raw === null || raw === undefined) return "unknown";
  const s = String(raw).trim();
  return s ? s : "unknown";
}

function cleanText(s) {
  return String(s || "")
    .replace(/\s+/g, " ")
    .trim();
}

// Heuristic role inference: only if exactly 2 speakers and confidence is decent.
// Returns a map { "Speaker A": "You", "Speaker B": "Prospect" } or null.
function inferYouProspectMap(lines) {
  const speakers = [...new Set(lines.map(l => l.speaker).filter(Boolean))];
  if (speakers.length !== 2) return null;

  const textFor = (sp) =>
    lines.filter(l => l.speaker === sp).map(l => l.text || "").join(" ").slice(0, 5000);

  const repSignals = [
    /\bmy name is\b/i,
    /\bthis is\b/i,
    /\bi['’]?m calling\b/i,
    /\breaching out\b/i,
    /\bquick (one|question|thing)\b/i,
    /\bdo you have (a moment|a minute)\b/i,
    /\bcan i\b/i,
    /\bwould you\b/i,
    /\bnext step\b/i,
    /\bbook\b/i,
    /\bschedule\b/i,
    /\bcalendar\b/i,
    /\bwalkthrough\b/i,
    /\b15 minutes\b/i,
  ];

  const prospectSignals = [
    /\bwho is this\b/i,
    /\bnot interested\b/i,
    /\bstop calling\b/i,
    /\bjust email\b/i,
    /\bhow much\b/i,
    /\bwe already\b/i,
    /\bwe're good\b/i,
    /\bbusy\b/i,
  ];

  const score = (txt) => {
    let rep = 0, pro = 0;
    for (const r of repSignals) if (r.test(txt)) rep++;
    for (const p of prospectSignals) if (p.test(txt)) pro++;
    return { rep, pro, net: rep - pro };
  };

  const aSp = speakers[0];
  const bSp = speakers[1];

  const a = score(textFor(aSp));
  const b = score(textFor(bSp));

  const diff = Math.abs(a.net - b.net);
  if (diff < 2) return null; // not confident enough

  const aIsYou = a.net > b.net;
  return {
    [aSp]: aIsYou ? "You" : "Prospect",
    [bSp]: aIsYou ? "Prospect" : "You",
  };
}

// Converts AssemblyAI utterances -> stable Speaker A/B/C labels (never X),
// then optionally upgrades to You/Prospect if confident.
function normalizeAssemblyUtterances(utterances) {
  const speakerMap = new Map(); // rawKey -> "Speaker A"
  let idx = 0;

  const lines = [];
  for (const u of utterances || []) {
    const key = normalizeSpeakerKey(u?.speaker);
    if (!speakerMap.has(key)) {
      const label =
        idx < SPEAKER_LETTERS.length
          ? `Speaker ${SPEAKER_LETTERS[idx]}`
          : `Speaker ${idx + 1}`;
      speakerMap.set(key, label);
      idx++;
    }

    const speaker = speakerMap.get(key);
    const text = cleanText(u?.text);
    if (!text) continue;

    lines.push({ speaker, text });
  }

  // Optional role inference (only if 2 speakers and confident)
  const map = inferYouProspectMap(lines);
  if (map) {
    for (const l of lines) {
      l.speaker = map[l.speaker] || l.speaker;
    }
  }

  const transcript = lines.map(l => `${l.speaker}: ${l.text}`).join("\n");

  return {
    transcript,
    lines,
    meta: {
      utterances: Array.isArray(utterances) ? utterances.length : 0,
      speakers: speakerMap.size,
      speaker_map: Object.fromEntries(speakerMap.entries()),
      role_map: map || null,
    },
  };
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
      // Best path: utterances exist (diarized)
      if (Array.isArray(pollJson.utterances) && pollJson.utterances.length) {
        return normalizeAssemblyUtterances(pollJson.utterances);
      }

      // Fallback: no utterances (no diarization) — still return something stable
      const text = cleanText(pollJson.text || "");
      const lines = text ? [{ speaker: "Speaker A", text }] : [];
      return {
        transcript: lines.map(l => `${l.speaker}: ${l.text}`).join("\n"),
        lines,
        meta: { utterances: 0, speakers: lines.length ? 1 : 0, speaker_map: {}, role_map: null },
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

    // Legacy scenarios removed from MVP
    const legacyScenario = null;

    if (userContext.length < 10) return res.status(400).json({ error: "Context is required (10+ characters)." });

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

    // Stable diarized transcript + structured transcript_lines
    const asr = await transcribeWithAssemblyAI(file.buffer);
    const transcript = asr.transcript;         // "Speaker A: ..." or "You/Prospect: ..."
    const transcriptLines = asr.lines;         // [{speaker,text}, ...]

    const result = await runCalibrate({ transcript, userContext, category, legacyScenario });

    const reportText = pick(result, ["report", "coachingReport", "report_md", "reportMarkdown"], "") || "";
    const callOutcome = pick(result, ["call_outcome", "callOutcome", "outcome"], null);

    const mismatchReason =
      pick(result, ["context_conflict_banner", "contextConflictBanner", "scenarioMismatch", "scenario_mismatch"], "") || "";
    const mismatch = Boolean(mismatchReason);

    const row = {
      user_id: userId,
      run_type: "upload",
      user_context: userContext,
      scenario_template: category || null,
      transcript_lines: transcriptLines,
      report_text: reportText,
      diagnostics: { ...result, _transcript_meta: asr.meta },
      call_outcome: callOutcome,
      enforcer: ENFORCER_VERSION,
      scenario_mismatch: mismatch,
      mismatch_reason: mismatchReason || null,
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

// SPA fallback for non-API routes
app.get(/^(?!\/api\/).*/, (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
app.listen(PORT, () => {
  console.log(`Calibrate MVP running on port ${PORT}`);
  console.log(`Enforcer loaded: ${ENFORCER_VERSION}`);
});
