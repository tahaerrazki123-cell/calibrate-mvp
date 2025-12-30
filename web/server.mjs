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
app.use(express.json({ limit: "1mb" }));

// ---- Limits (server truth) ----
const MAX_UPLOAD_BYTES = 25 * 1024 * 1024; // 25MB
const MAX_RUNS_PER_24H = 10;
const MAX_SUPPORT_MESSAGE_CHARS = 4000;

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

async function requireUser(req) {
  const auth = req.headers.authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
  if (!token) return null;

  const sb = supabaseAnon();
  const { data, error } = await sb.auth.getUser(token);
  if (error) return null;

  const u = data?.user;
  if (!u?.id) return null;

  return { id: u.id, email: u.email || null };
}

function pick(obj, keys, fallback = null) {
  for (const k of keys) {
    if (obj && obj[k] !== undefined && obj[k] !== null && obj[k] !== "") return obj[k];
  }
  return fallback;
}

function normalizeKey(k) {
  return String(k || "")
    .trim()
    .toUpperCase()
    .replace(/[^\w]+/g, "_")
    .replace(/^_+|_+$/g, "");
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

function transcriptToLines(transcript) {
  const lines = String(transcript || "")
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map((s) => s.trim())
    .filter(Boolean);

  return lines.map((line) => {
    const m = line.match(/^([^:]{1,40}):\s*(.*)$/);
    if (!m) return { speaker: "Other", text: line };
    const speakerRaw = (m[1] || "Other").replace(/\s+/g, " ").trim();
    const speaker = speakerRaw.replace(/^speaker\s*([a-z]|\d+)$/i, "Speaker $1");
    return { speaker, text: (m[2] || "").trim() };
  });
}

// Generic "insert and drop missing columns" helper
async function insertResilient(sb, table, payload, selectCols = "id") {
  let p = { ...payload };

  for (let i = 0; i < 20; i++) {
    const { data, error } = await sb.from(table).insert([p]).select(selectCols).maybeSingle();
    if (!error) return data;

    const msg = String(error.message || "");

    // Missing column patterns
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

  throw new Error(`Insert into '${table}' failed after retries (schema mismatch).`);
}

// -------------------- SUPPORT (stores tickets in Supabase) --------------------
// Table suggestion (create in Supabase SQL editor):
// create table if not exists support_tickets (
//   id uuid primary key default gen_random_uuid(),
//   created_at timestamptz not null default now(),
//   user_id uuid,
//   user_email text,
//   reply_to text,
//   message text not null,
//   page text,
//   run_id uuid,
//   meta jsonb
// );
app.post("/api/support", async (req, res) => {
  try {
    const user = await requireUser(req);
    if (!user) return res.status(401).json({ error: "Unauthorized" });

    const messageRaw = String(req.body?.message || "").trim();
    const replyTo = String(req.body?.reply_to || "").trim();
    const page = String(req.body?.page || "").trim();
    const runId = String(req.body?.run_id || "").trim();

    if (!messageRaw) return res.status(400).json({ error: "Message is required." });
    if (messageRaw.length > MAX_SUPPORT_MESSAGE_CHARS) {
      return res.status(400).json({ error: `Message too long (max ${MAX_SUPPORT_MESSAGE_CHARS} chars).` });
    }

    const sb = supabaseService();

    const row = {
      user_id: user.id,
      user_email: user.email,
      reply_to: replyTo || null,
      message: messageRaw,
      page: page || null,
      run_id: runId || null,
      meta: {
        ua: req.headers["user-agent"] || null,
        ip: req.ip || null,
      },
    };

    const inserted = await insertResilient(sb, "support_tickets", row, "id");
    return res.json({ ok: true, ticket_id: inserted?.id || null });
  } catch (err) {
    const msg = err?.message || String(err);
    // Helpful error if table doesn't exist
    if (/relation .*support_tickets.* does not exist/i.test(msg)) {
      return res.status(500).json({
        error:
          "Support table missing. Create a Supabase table named 'support_tickets' (see comment in server.mjs).",
      });
    }
    return res.status(500).json({ error: msg });
  }
});

// -------------------- Runs API (history + details) --------------------

app.get("/api/runs", async (req, res) => {
  try {
    const user = await requireUser(req);
    if (!user) return res.status(401).json({ error: "Unauthorized" });

    const sb = supabaseService();

    const { data, error } = await sb
      .from("runs")
      .select("*")
      .eq("user_id", user.id)
      .order("created_at", { ascending: false })
      .limit(50);

    if (error) return res.status(500).json({ error: error.message });

    const runs = (data || []).map((r) => {
      const mismatch = Boolean(normalizeScenarioMismatch(r));
      const runTypeRaw = r.run_type ?? r.runType ?? r.type ?? "upload";
      const type =
        String(runTypeRaw).toLowerCase() === "upload"
          ? "Upload"
          : String(runTypeRaw).toLowerCase() === "record"
          ? "Record"
          : String(runTypeRaw).toLowerCase() === "practice"
          ? "Practice"
          : "Upload";

      const title =
        r.run_title ??
        r.title ??
        r.report_title ??
        r.name ??
        // fallback: shorten user_context if title missing
        shortenTitle(String(r.user_context || "").trim());

      return {
        id: r.id,
        created_at: r.created_at,
        scenario_template: r.scenario_template ?? r.scenarioTemplate ?? r.category ?? null,
        call_outcome: r.call_outcome ?? r.callOutcome ?? r.outcome ?? null,
        call_result: r.call_outcome ?? r.callOutcome ?? r.outcome ?? null,
        call_outcome_reason: r.call_outcome_reason ?? r.outcome_reason ?? null,
        type,
        run_type: runTypeRaw,
        mismatch,
        scenario_mismatch: mismatch,
        mismatch_reason: r.mismatch_reason ?? null,
        run_title: title,
      };
    });

    return res.json({ runs });
  } catch (err) {
    return res.status(500).json({ error: err?.message || String(err) });
  }
});

app.get("/api/runs/:id", async (req, res) => {
  try {
    const user = await requireUser(req);
    if (!user) return res.status(401).json({ error: "Unauthorized" });

    const runId = req.params.id;
    const sb = supabaseService();

    const { data, error } = await sb
      .from("runs")
      .select("*")
      .eq("id", runId)
      .eq("user_id", user.id)
      .maybeSingle();

    if (error) return res.status(500).json({ error: error.message });
    if (!data) return res.status(404).json({ error: "Run not found" });

    const mismatch = Boolean(normalizeScenarioMismatch(data));
    const runTypeRaw = data.run_type ?? data.runType ?? data.type ?? "upload";
    const type =
      String(runTypeRaw).toLowerCase() === "upload"
        ? "Upload"
        : String(runTypeRaw).toLowerCase() === "record"
        ? "Record"
        : String(runTypeRaw).toLowerCase() === "practice"
        ? "Practice"
        : "Upload";

    const title =
      data.run_title ??
      data.title ??
      data.report_title ??
      data.name ??
      shortenTitle(String(data.user_context || "").trim());

    const run = {
      ...data,
      mismatch,
      scenario_mismatch: mismatch,
      run_type: runTypeRaw,
      type,
      run_title: title,
      call_result: data.call_outcome ?? data.callOutcome ?? data.outcome ?? null,
    };

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
  const speakers = [...new Set(lines.map((l) => l.speaker).filter(Boolean))];
  if (speakers.length !== 2) return null;

  const textFor = (sp) => lines.filter((l) => l.speaker === sp).map((l) => l.text || "").join(" ").slice(0, 5000);

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

  const prospectSignals = [/\bwho is this\b/i, /\bnot interested\b/i, /\bstop calling\b/i, /\bjust email\b/i, /\bhow much\b/i, /\bwe already\b/i, /\bwe're good\b/i, /\bbusy\b/i];

  const score = (txt) => {
    let rep = 0,
      pro = 0;
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
      const label = idx < SPEAKER_LETTERS.length ? `Speaker ${SPEAKER_LETTERS[idx]}` : `Speaker ${idx + 1}`;
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

  const transcript = lines.map((l) => `${l.speaker}: ${l.text}`).join("\n");

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

      // Fallback: no utterances (no diarization)
      const text = cleanText(pollJson.text || "");
      const lines = text ? [{ speaker: "Speaker A", text }] : [];
      return {
        transcript: lines.map((l) => `${l.speaker}: ${l.text}`).join("\n"),
        lines,
        meta: { utterances: 0, speakers: lines.length ? 1 : 0, speaker_map: {}, role_map: null },
      };
    }

    if (pollJson.status === "error") throw new Error("AssemblyAI transcription error: " + (pollJson.error || "unknown"));
    if (Date.now() - started > 4 * 60 * 1000) throw new Error("AssemblyAI transcription timed out.");

    await new Promise((r) => setTimeout(r, 1500));
  }
}

// ---- Call result override logic (Transcript-first + priority) ----
function outcomePriority(key) {
  const k = normalizeKey(key);
  return (
    {
      BOOKED_MEETING: 100,
      HOSTILE: 90,
      REJECTED: 80,
      CALLBACK: 70,
      GATEKEEPER: 60,
      VOICEMAIL: 50,
      NO_ANSWER: 40,
      CONNECTED: 10,
    }[k] ?? 0
  );
}

function classifyOutcomeFromTranscript(transcript) {
  const t = String(transcript || "").toLowerCase();

  // HOSTILE
  if (/\b(stop calling|don't call|do not call|never call|remove me|take me off your list)\b/.test(t) || /\b(fuck you|go to hell)\b/.test(t)) {
    return { key: "HOSTILE", reason: "Detected hostile language / do-not-call intent." };
  }

  // VOICEMAIL
  if (/\b(voicemail|leave (a )?message|at the tone|mailbox is full)\b/.test(t)) {
    return { key: "VOICEMAIL", reason: "Detected voicemail language." };
  }

  // GATEKEEPER
  if (/\b(front desk|receptionist|assistant|admin)\b/.test(t) && /\b(they('re| are) (not available|busy)|can i take a message|who is this for)\b/.test(t)) {
    return { key: "GATEKEEPER", reason: "Detected gatekeeper interaction." };
  }

  // CALLBACK
  if (/\b(call( me)? back|reach back out|try again|later today|tomorrow|next week)\b/.test(t) && /\b(busy|in a meeting|not a good time)\b/.test(t)) {
    return { key: "CALLBACK", reason: "Detected callback request / timing deferral." };
  }

  // REJECTED (non-hostile)
  if (/\b(not interested|no thanks|we're good|we are good|not a fit|don't need|already have|we already use)\b/.test(t) && !/\b(schedule|book|set up|let's do|lets do|zoom|calendar|invite)\b/.test(t)) {
    return { key: "REJECTED", reason: "Detected clear rejection language without a next step." };
  }

  // BOOKED MEETING (expanded + prioritized)
  const hasZoomOrMeetWord = /\bzoom\b/.test(t) || /\b(meeting|demo|walkthrough|consultation( call)?|call)\b/.test(t);

  const hasBookingVerb = /\b(schedule|book|set up|lock in|put (it )?on the calendar|calendar|invite|send (me )?(a )?(link|invite)|calendly)\b/.test(t);

  const hasAgreement = /\b(yeah|yes|sure|sounds good|that works|perfect|okay|ok|let's|lets)\b/.test(t);

  const hasTimeCue = /\b(\d{1,2}:\d{2}|\d{1,2}\s?(am|pm)|today|tomorrow|monday|tuesday|wednesday|thursday|friday|next week)\b/.test(t);

  const hasInviteLink = /\b(calendly|calendar link|invite)\b/.test(t) && /\b(send|sent|shoot|text|email)\b/.test(t);

  const explicitAgreementToMeet = /\b(let's|lets|we can|can we|how about|would you be open to)\b.*\b(zoom|consultation( call)?|demo|meeting|call)\b/.test(t);

  if (hasInviteLink || (hasZoomOrMeetWord && hasBookingVerb && (hasAgreement || hasTimeCue)) || (explicitAgreementToMeet && (hasAgreement || hasBookingVerb || hasTimeCue))) {
    return { key: "BOOKED_MEETING", reason: "Detected agreement to a scheduled next step (zoom/consultation/meeting language)." };
  }

  return null; // fall back to model outcome (often CONNECTED)
}

// ---- Title helpers ----
function shortenTitle(s, max = 56) {
  const cleaned = String(s || "")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/^["']|["']$/g, "");
  if (!cleaned) return "Untitled Call";
  if (cleaned.length <= max) return cleaned;
  return cleaned.slice(0, max - 1).trimEnd() + "…";
}

function generateRunTitle(userContext, transcript) {
  // Keep it simple + stable: start from context, optionally nudge with one inferred cue
  const ctx = shortenTitle(String(userContext || "").trim(), 64);

  // lightweight add-on: if transcript hints at industry, append short tag (but NEVER outcome)
  const t = String(transcript || "").toLowerCase();
  let tag = "";

  if (/\b(shopify|e-?com(merce)?|cart|checkout)\b/.test(t)) tag = " (Ecom)";
  else if (/\b(dentist|dental|clinic|patient)\b/.test(t)) tag = " (Clinic)";
  else if (/\b(real estate|realtor|listing|buyer|seller)\b/.test(t)) tag = " (Real Estate)";
  else if (/\b(seo|google rankings|keywords)\b/.test(t)) tag = " (SEO)";
  else if (/\b(saas|software|platform|tool)\b/.test(t)) tag = " (Software)";

  // Ensure we don't overflow and keep readable
  return shortenTitle(ctx + tag, 68);
}

app.post("/api/run", upload.single("audio"), async (req, res) => {
  const user = await requireUser(req);
  if (!user) return res.status(401).json({ error: "Unauthorized" });

  if (activeRunByUser.has(user.id)) {
    return res.status(429).json({ error: "A run is already in progress. Please wait." });
  }

  try {
    activeRunByUser.set(user.id, Date.now());

    const file = req.file;
    if (!file?.buffer) return res.status(400).json({ error: "No audio file uploaded (field name must be 'audio')." });

    const userContext = (req.body?.context || "").trim();
    const category = (req.body?.category || "").trim(); // scenario template
    // legacy scenario intentionally ignored (already removed in UI), but keep backwards-compat if posted
    const legacyScenario = (req.body?.legacyScenario || "").trim();

    if (userContext.length < 10) return res.status(400).json({ error: "Context is required (10+ characters)." });

    const cutoffIso = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    const sb = supabaseService();

    const { count, error: countErr } = await sb
      .from("runs")
      .select("id", { count: "exact", head: true })
      .eq("user_id", user.id)
      .gte("created_at", cutoffIso);

    if (countErr) return res.status(500).json({ error: countErr.message });
    if ((count || 0) >= MAX_RUNS_PER_24H) {
      return res.status(429).json({ error: `Daily limit reached (${MAX_RUNS_PER_24H}/24h). Try again later.` });
    }

    // Stable diarized transcript + structured transcript_lines
    const asr = await transcribeWithAssemblyAI(file.buffer);
    const transcript = asr.transcript; // "Speaker A: ..." or "You/Prospect: ..."
    const transcriptLines = asr.lines; // [{speaker,text}, ...]

    // Run LLM report
    const result = await runCalibrate({
      transcript,
      userContext,
      category,
      legacyScenario, // ignored by frontend, but safe to pass for back-compat
    });

    const reportText = pick(result, ["report", "coachingReport", "report_md", "reportMarkdown"], "") || "";

    let callOutcome = pick(result, ["call_outcome", "callOutcome", "outcome"], null);
    let callOutcomeReason = pick(result, ["call_outcome_reason", "outcomeReason", "outcome_reason"], "") || "";

    // Scenario mismatch should ONLY matter when a scenario template is selected
    const rawMismatchReason =
      pick(result, ["context_conflict_banner", "contextConflictBanner", "scenarioMismatch", "scenario_mismatch"], "") || "";
    const mismatchReason = category ? rawMismatchReason : "";
    const mismatch = Boolean(category && mismatchReason);

    // Transcript-first outcome classifier with priority override
    const forced = classifyOutcomeFromTranscript(transcript);
    const modelKey = normalizeKey(callOutcome);
    const forcedKey = normalizeKey(forced?.key);

    if (forcedKey && outcomePriority(forcedKey) > outcomePriority(modelKey)) {
      callOutcome = forcedKey;
      callOutcomeReason = forced.reason;

      // Also inject into diagnostics so UI can show "why"
      result.call_outcome = callOutcome;
      result.call_outcome_reason = callOutcomeReason;
    }

    // Title (no outcome suffix)
    const runTitle = generateRunTitle(userContext, transcript);

    const row = {
      user_id: user.id,
      run_type: "upload",
      user_context: userContext,
      scenario_template: category || null,
      legacy_scenario: legacyScenario || null,
      transcript_lines: transcriptLines,
      report_text: reportText,
      diagnostics: { ...result, _transcript_meta: asr.meta },
      call_outcome: callOutcome,
      call_outcome_reason: callOutcomeReason || null,
      enforcer: ENFORCER_VERSION,
      scenario_mismatch: mismatch,
      mismatch_reason: mismatchReason || null,
      run_title: runTitle,
    };

    const inserted = await insertResilient(sb, "runs", row, "id");
    return res.json({ run_id: inserted?.id || null });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err?.message || String(err) });
  } finally {
    activeRunByUser.delete(user.id);
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
