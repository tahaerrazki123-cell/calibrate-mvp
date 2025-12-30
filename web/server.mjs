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
app.use(express.json());

// ---- Limits ----
const MAX_UPLOAD_BYTES = 25 * 1024 * 1024; // 25MB
const MAX_RUNS_PER_24H = 10;

// One-run-at-a-time per user (simple in-memory lock)
const activeRunByUser = new Map(); // userId -> startedAtMs

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: MAX_UPLOAD_BYTES },
});

app.use(express.static(__dirname, { extensions: ["html"] }));

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    enforcer: ENFORCER_VERSION,
    commit: process.env.RENDER_GIT_COMMIT || null,
  });
});

app.get("/api/config", (req, res) => {
  res.json({
    supabaseUrl: process.env.SUPABASE_URL || "",
    supabaseAnonKey: process.env.SUPABASE_ANON_KEY || "",
    enforcer: ENFORCER_VERSION,
  });
});

// ---- Supabase clients ----
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

// Generic resilient insert for schema drift (drops unknown columns)
async function insertResilient(sb, table, payload) {
  let p = { ...payload };

  for (let i = 0; i < 20; i++) {
    const { data, error } = await sb.from(table).insert([p]).select("id").maybeSingle();
    if (!error) return data;

    const msg = String(error.message || "");

    // table missing
    if (/relation .* does not exist/i.test(msg) || /schema cache/i.test(msg) && /does not exist/i.test(msg)) {
      throw new Error(`Missing table "${table}". Create it in Supabase first.`);
    }

    let m = msg.match(/column\s+\w+\.(\w+)\s+does not exist/i);
    if (m && m[1]) { delete p[m[1]]; continue; }

    m = msg.match(/Could not find the '(\w+)' column of '(\w+)' in the schema cache/i);
    if (m && m[1]) { delete p[m[1]]; continue; }

    throw new Error(msg);
  }

  throw new Error("Insert failed after retries (schema mismatch).");
}

// -------------------- Title generation --------------------
function cleanSpaces(s) { return String(s || "").replace(/\s+/g, " ").trim(); }

function stripOutcomeSuffix(title) {
  let t = cleanSpaces(title);
  if (!t) return t;

  const outcomeWords = [
    "booked meeting","booked demo","booked call","connected","rejected","hostile","voicemail","no answer","callback","gatekeeper"
  ];
  const re = new RegExp(`\\s*(—|\\-|\\||:)+\\s*(${outcomeWords.join("|")})\\s*$`, "i");
  t = t.replace(re, "");
  return cleanSpaces(t);
}

function clampTitle(title, maxLen = 62) {
  const t = cleanSpaces(title);
  if (t.length <= maxLen) return t;
  return t.slice(0, Math.max(0, maxLen - 1)).trimEnd() + "…";
}

function extractInferred(diagnostics) {
  const inferred = pick(diagnostics, ["inferred_from_transcript","inferredFromTranscript","inferred","inferred_lines","inferredLines"], null);

  let prospectType = "";
  let offerKeywords = [];

  if (Array.isArray(inferred)) {
    for (const s of inferred) {
      const line = String(s || "");
      const m1 = line.match(/prospect\s*type:\s*(.+)$/i);
      if (m1) prospectType = cleanSpaces(m1[1]);
      const m2 = line.match(/offer\s*keywords:\s*(.+)$/i);
      if (m2) offerKeywords = cleanSpaces(m2[1]).split(",").map(x => cleanSpaces(x)).filter(Boolean);
    }
  } else if (inferred && typeof inferred === "object") {
    prospectType = cleanSpaces(pick(inferred, ["prospectType","prospect_type"], "")) || "";
    const ok = pick(inferred, ["offerKeywords","offer_keywords"], []);
    if (Array.isArray(ok)) offerKeywords = ok.map(x => cleanSpaces(x)).filter(Boolean);
    else if (ok) offerKeywords = cleanSpaces(ok).split(",").map(x => cleanSpaces(x)).filter(Boolean);
  }

  return { prospectType, offerKeywords };
}

function titleFromContextAndInference({ userContext, diagnostics, scenarioTemplate }) {
  const ctx = stripOutcomeSuffix(userContext || "");
  const { prospectType, offerKeywords } = extractInferred(diagnostics || {});
  const offer = offerKeywords[0] || "";
  const pt = prospectType || "";

  if (offer && pt) return `${offer.toUpperCase()} → ${pt}`;
  if (offer && ctx) return `${offer.toUpperCase()} → ${ctx}`;
  if (pt && ctx) return `${pt} → ${ctx}`;

  const c = cleanSpaces(ctx);
  if (!c) return "Call Analysis";

  let compact = c.replace(/^(selling|offering|pitched|offered|calling)\s+/i, "");
  if (scenarioTemplate) {
    const prefix = scenarioTemplate.toLowerCase().split("_").map(w => w ? (w[0].toUpperCase() + w.slice(1)) : "").join(" ");
    compact = `${prefix}: ${compact}`;
  }
  return compact;
}

function buildTitleShapes({ userContext, diagnostics, scenarioTemplate }) {
  const full = stripOutcomeSuffix(titleFromContextAndInference({ userContext, diagnostics, scenarioTemplate }));
  const short = clampTitle(full, 58);
  return { title_full: full, title_short: short, title: short };
}

// -------------------- Outcome override (booked meeting fix) --------------------
function normalizeKey(s) {
  const t = String(s || "").trim();
  if (!t) return "";
  if (/^[A-Z0-9_]+$/.test(t)) return t;
  return t.replace(/[^\w\s-]/g, "").replace(/\s+/g, "_").replace(/-+/g, "_").toUpperCase();
}

function detectBookedMeeting(transcript) {
  const t = String(transcript || "").toLowerCase();

  const hasInvite = /\b(calendar|calendly|invite)\b/.test(t) && /\b(send|sent|shoot)\b/.test(t);
  const hasZoom = /\bzoom\b/.test(t);
  const hasMeetingWords = /\b(meeting|demo|walkthrough|call)\b/.test(t);
  const hasSchedule = /\b(schedule|book|set up|lock in)\b/.test(t);

  // "what time works" + "tomorrow/next week" etc.
  const hasTimeNegotiation =
    /\b(what time works|does (?:tomorrow|monday|tuesday|wednesday|thursday|friday|next week) work|how about)\b/.test(t) ||
    /\b(\d{1,2}:\d{2}|\d{1,2}\s?(am|pm))\b/.test(t);

  // Acceptance language
  const hasAgreement = /\b(yeah|sure|sounds good|that works|perfect|okay|ok)\b/.test(t);

  // Strong booking if they mention invite/zoom + scheduling + agreement OR time negotiation
  const strong =
    (hasInvite && (hasAgreement || hasTimeNegotiation)) ||
    (hasZoom && hasSchedule && (hasAgreement || hasTimeNegotiation)) ||
    (hasMeetingWords && hasSchedule && hasTimeNegotiation && hasAgreement);

  if (!strong) return null;

  return {
    key: "BOOKED_MEETING",
    reason: "Transcript includes meeting scheduling language (invite/zoom/time agreement).",
  };
}

// -------------------- Runs API --------------------
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
      const scenarioTemplate = r.scenario_template ?? r.scenarioTemplate ?? r.category ?? null;

      const mismatchRaw = Boolean(normalizeScenarioMismatch(r));
      const mismatch = Boolean(scenarioTemplate) && mismatchRaw;

      const diag = r.diagnostics || {};
      const titles = buildTitleShapes({
        userContext: r.user_context || "",
        diagnostics: diag,
        scenarioTemplate: scenarioTemplate || "",
      });

      const callOutcome = r.call_outcome ?? r.callOutcome ?? r.outcome ?? null;

      return {
        id: r.id,
        created_at: r.created_at,
        scenario_template: scenarioTemplate,
        call_outcome: callOutcome,
        mismatch,
        scenario_mismatch: mismatch,
        mismatch_reason: r.mismatch_reason ?? null,

        title: titles.title,
        title_short: titles.title_short,
        title_full: titles.title_full,
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

    const scenarioTemplate = data.scenario_template ?? data.scenarioTemplate ?? data.category ?? null;
    const mismatchRaw = Boolean(normalizeScenarioMismatch(data));
    const mismatch = Boolean(scenarioTemplate) && mismatchRaw;

    const diag = data.diagnostics || {};
    const titles = buildTitleShapes({
      userContext: data.user_context || "",
      diagnostics: diag,
      scenarioTemplate: scenarioTemplate || "",
    });

    const callOutcome = data.call_outcome ?? data.callOutcome ?? data.outcome ?? null;
    const callOutcomeReason =
      data.call_outcome_reason ??
      pick(diag, ["call_outcome_reason","outcomeReason","outcome_reason"], "") ??
      null;

    const run = {
      ...data,
      scenario_template: scenarioTemplate,
      mismatch,
      scenario_mismatch: mismatch,

      title: titles.title,
      title_short: titles.title_short,
      title_full: titles.title_full,

      call_outcome: callOutcome,
      call_outcome_reason: callOutcomeReason,
    };

    return res.json({ run });
  } catch (err) {
    return res.status(500).json({ error: err?.message || String(err) });
  }
});

// -------------------- Contact Support (real) --------------------
app.post("/api/support", async (req, res) => {
  try {
    const userId = await requireUserId(req);
    if (!userId) return res.status(401).json({ error: "Unauthorized" });

    const message = String(req.body?.message || "").trim();
    const page = String(req.body?.page || "").trim();

    if (message.length < 10) return res.status(400).json({ error: "Message too short (10+ chars)." });

    const sb = supabaseService();

    // Email is useful, but we can also derive it from auth token if needed later; store what we have.
    const auth = req.headers.authorization || "";
    const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
    let email = null;

    try {
      const sbAnon = supabaseAnon();
      const { data } = await sbAnon.auth.getUser(token);
      email = data?.user?.email || null;
    } catch { /* ignore */ }

    const payload = {
      user_id: userId,
      email,
      message,
      page,
      status: "open",
    };

    const inserted = await insertResilient(sb, "support_tickets", payload);
    return res.json({ ok: true, ticket_id: inserted?.id || null });
  } catch (err) {
    return res.status(500).json({ error: err?.message || String(err) });
  }
});

// -------------------- Calibrate endpoint --------------------
const SPEAKER_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".split("");

function normalizeSpeakerKey(raw) {
  if (raw === null || raw === undefined) return "unknown";
  const s = String(raw).trim();
  return s ? s : "unknown";
}

function cleanText(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

function inferYouProspectMap(lines) {
  const speakers = [...new Set(lines.map(l => l.speaker).filter(Boolean))];
  if (speakers.length !== 2) return null;

  const textFor = (sp) => lines.filter(l => l.speaker === sp).map(l => l.text || "").join(" ").slice(0, 5000);

  const repSignals = [
    /\bmy name is\b/i, /\bthis is\b/i, /\bi['’]?m calling\b/i, /\breaching out\b/i,
    /\bquick\b/i, /\bdo you have\b/i, /\bnext step\b/i, /\bbook\b/i, /\bschedule\b/i,
    /\bcalendar\b/i, /\bwalkthrough\b/i, /\bdemo\b/i
  ];

  const prospectSignals = [
    /\bwho is this\b/i, /\bnot interested\b/i, /\bstop calling\b/i, /\bjust email\b/i,
    /\bhow much\b/i, /\bwe already\b/i, /\bwe're good\b/i, /\bbusy\b/i
  ];

  const score = (txt) => {
    let rep = 0, pro = 0;
    for (const r of repSignals) if (r.test(txt)) rep++;
    for (const p of prospectSignals) if (p.test(txt)) pro++;
    return { net: rep - pro };
  };

  const aSp = speakers[0];
  const bSp = speakers[1];

  const a = score(textFor(aSp));
  const b = score(textFor(bSp));

  const diff = Math.abs(a.net - b.net);
  if (diff < 2) return null;

  const aIsYou = a.net > b.net;
  return { [aSp]: aIsYou ? "You" : "Prospect", [bSp]: aIsYou ? "Prospect" : "You" };
}

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

  const map = inferYouProspectMap(lines);
  if (map) for (const l of lines) l.speaker = map[l.speaker] || l.speaker;

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
      if (Array.isArray(pollJson.utterances) && pollJson.utterances.length) {
        return normalizeAssemblyUtterances(pollJson.utterances);
      }

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

    const asr = await transcribeWithAssemblyAI(file.buffer);
    const transcript = asr.transcript;
    const transcriptLines = asr.lines;

    const result = await runCalibrate({
      transcript,
      userContext,
      category,
      legacyScenario: "",
    });

    const reportText = pick(result, ["report", "coachingReport", "report_md", "reportMarkdown"], "") || "";

    // Pull outcome from any common key (this was likely part of the mis-display)
    let callOutcome = pick(result, ["call_outcome","callOutcome","outcome","call_result","callResult","result"], null);
    let callOutcomeReason = pick(result, ["call_outcome_reason","outcomeReason","outcome_reason"], "") || "";

    // HARD FIX: if transcript clearly books a meeting, override bad "CONNECTED"
    const booked = detectBookedMeeting(transcript);
    if (booked) {
      const currentKey = normalizeKey(callOutcome);
      if (currentKey !== "BOOKED_MEETING") {
        callOutcome = booked.key;
        callOutcomeReason = booked.reason;
        // also inject into diagnostics so UI always has it even if column missing
        result.call_outcome = callOutcome;
        result.call_outcome_reason = callOutcomeReason;
      }
    }

    const mismatchReasonRaw = pick(result, ["context_conflict_banner","contextConflictBanner","scenarioMismatch","scenario_mismatch"], "") || "";
    const mismatchReason = category ? mismatchReasonRaw : "";
    const mismatch = Boolean(category) && Boolean(mismatchReason);

    const titles = buildTitleShapes({
      userContext,
      diagnostics: result,
      scenarioTemplate: category || "",
    });

    const row = {
      user_id: userId,
      run_type: "upload",
      user_context: userContext,
      scenario_template: category || null,
      transcript_lines: transcriptLines,
      report_text: reportText,
      diagnostics: { ...result, _transcript_meta: asr.meta },

      call_outcome: callOutcome,
      call_outcome_reason: callOutcomeReason || null,

      enforcer: ENFORCER_VERSION,
      scenario_mismatch: mismatch,
      mismatch_reason: mismatchReason || null,

      run_title: titles.title_full,
    };

    const inserted = await insertResilient(sb, "runs", row);
    return res.json({ run_id: inserted?.id || null });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err?.message || String(err) });
  } finally {
    activeRunByUser.delete(userId);
  }
});

app.use("/api", (req, res) => res.status(404).json({ error: "Unknown API route" }));

app.get(/^(?!\/api\/).*/, (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
app.listen(PORT, () => {
  console.log(`Calibrate MVP running on port ${PORT}`);
  console.log(`Enforcer loaded: ${ENFORCER_VERSION}`);
});
