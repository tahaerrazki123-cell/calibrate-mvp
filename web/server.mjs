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

// Parse JSON + form bodies (support endpoint uses JSON)
app.use(express.json({ limit: "1mb" }));
app.use(express.urlencoded({ extended: true, limit: "1mb" }));

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

// -------------------- Transcript normalization --------------------

// NEW: Stable speaker labeling (never "Speaker X") + optional You/Prospect inference
const SPEAKER_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".split("");

function normalizeSpeakerKey(raw) {
  if (raw === null || raw === undefined) return "unknown";
  const s = String(raw).trim();
  return s ? s : "unknown";
}

function cleanText(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

// Heuristic role inference: only if exactly 2 speakers and confidence is decent.
// Returns a map { "Speaker A": "You", "Speaker B": "Prospect" } or null.
function inferYouProspectMap(lines) {
  const speakers = [...new Set(lines.map((l) => l.speaker).filter(Boolean))];
  if (speakers.length !== 2) return null;

  const textFor = (sp) =>
    lines
      .filter((l) => l.speaker === sp)
      .map((l) => l.text || "")
      .join(" ")
      .slice(0, 5000);

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
  if (diff < 2) return null;

  const aIsYou = a.net > b.net;
  return {
    [aSp]: aIsYou ? "You" : "Prospect",
    [bSp]: aIsYou ? "Prospect" : "You",
  };
}

// Converts AssemblyAI utterances -> stable Speaker A/B/C labels,
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

  const map = inferYouProspectMap(lines);
  if (map) {
    for (const l of lines) l.speaker = map[l.speaker] || l.speaker;
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
      if (Array.isArray(pollJson.utterances) && pollJson.utterances.length) {
        return normalizeAssemblyUtterances(pollJson.utterances);
      }
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

// -------------------- Titles (AI: 3–5 words) --------------------

function stripOutcomeSuffix(title) {
  const s = String(title || "").trim();
  return s.replace(/\s*(—|-|\|)\s*(booked meeting|connected|rejected|hostile|voicemail|no answer)\s*$/i, "").trim();
}

function toTitleCase(s) {
  return String(s || "")
    .toLowerCase()
    .split(/\s+/g)
    .filter(Boolean)
    .map((w) => (w.length ? w[0].toUpperCase() + w.slice(1) : w))
    .join(" ");
}

function enforce3to5Words(raw) {
  const cleaned = stripOutcomeSuffix(String(raw || ""))
    .replace(/[“”"]/g, "")
    .replace(/[^\p{L}\p{N}\s'-]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();

  let words = cleaned.split(" ").filter(Boolean);
  if (words.length > 5) words = words.slice(0, 5);

  // If model gives 1–2 words, we still accept, but we try to expand via fallback caller
  return toTitleCase(words.join(" "));
}

function fallbackShortTitle({ userContext, category }) {
  const base = stripOutcomeSuffix(String(userContext || "")).trim() || String(category || "").replace(/_/g, " ").trim() || "Call Analysis";
  const cleaned = base
    .replace(/[^\p{L}\p{N}\s'-]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
  const words = cleaned.split(" ").filter(Boolean);
  const picked = (words.length >= 3 ? words.slice(0, 5) : words.slice(0, 5));
  return toTitleCase(picked.join(" ")) || "Call Analysis";
}

function titleLLMConfig() {
  // You can set any OpenAI-compatible provider here.
  // Recommended env:
  // TITLE_LLM_API_KEY, TITLE_LLM_BASE_URL, TITLE_LLM_MODEL
  // If not set, we try DEEPSEEK_API_KEY (OpenAI-compatible) as a fallback.
  const apiKey = process.env.TITLE_LLM_API_KEY || process.env.DEEPSEEK_API_KEY || process.env.OPENAI_API_KEY || "";
  const baseUrl =
    process.env.TITLE_LLM_BASE_URL ||
    process.env.DEEPSEEK_BASE_URL ||
    process.env.OPENAI_BASE_URL ||
    ""; // if empty -> no LLM call

  const model =
    process.env.TITLE_LLM_MODEL ||
    process.env.DEEPSEEK_MODEL ||
    process.env.OPENAI_MODEL ||
    "deepseek-chat";

  return { apiKey, baseUrl, model };
}

async function generateAiShortTitle({ userContext, transcript, category }) {
  const { apiKey, baseUrl, model } = titleLLMConfig();

  // If not configured, never break runs — just fallback.
  if (!apiKey || !baseUrl) return fallbackShortTitle({ userContext, category });

  const t = String(transcript || "");
  const ctx = String(userContext || "");

  // Keep payload small + focused.
  const transcriptSnippet = t.split("\n").slice(0, 40).join("\n").slice(0, 3500);

  const prompt = [
    "Create a SHORT title for this call analysis.",
    "Rules:",
    "- Output ONLY the title text (no quotes, no bullets, no punctuation at the end).",
    "- 3 to 5 words maximum.",
    "- Title Case.",
    "- Do NOT include the call result/outcome words (Booked/Connected/Rejected/Hostile/etc).",
    "",
    `Scenario (optional): ${String(category || "None")}`,
    "",
    `User context: ${ctx}`,
    "",
    "Transcript (excerpt):",
    transcriptSnippet,
  ].join("\n");

  try {
    const url = baseUrl.replace(/\/+$/, "") + "/v1/chat/completions";
    const r = await fetch(url, {
      method: "POST",
      headers: {
        authorization: `Bearer ${apiKey}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model,
        temperature: 0.2,
        max_tokens: 24,
        messages: [
          {
            role: "system",
            content:
              "You write ultra-short, high-signal titles for call analyses. Follow the rules exactly.",
          },
          { role: "user", content: prompt },
        ],
      }),
    });

    const j = await r.json().catch(() => ({}));
    const raw = j?.choices?.[0]?.message?.content || j?.choices?.[0]?.text || "";

    const enforced = enforce3to5Words(raw);
    if (enforced && enforced.split(/\s+/g).filter(Boolean).length >= 3) return enforced;

    // If too short, fall back (still safe)
    return fallbackShortTitle({ userContext, category });
  } catch {
    return fallbackShortTitle({ userContext, category });
  }
}

// -------------------- Outcome detection (override for Booked Meeting accuracy) --------------------

function normalizeOutcomeKey(raw) {
  const s = String(raw || "").trim();
  if (!s) return "";
  const up = s.toUpperCase().replace(/\s+/g, "_").replace(/-+/g, "_");

  const map = {
    BOOKED: "BOOKED_MEETING",
    BOOKEDCALL: "BOOKED_MEETING",
    BOOKED_CALL: "BOOKED_MEETING",
    MEETING_BOOKED: "BOOKED_MEETING",
    BOOKED_MEETING: "BOOKED_MEETING",

    CONNECTED: "CONNECTED",
    CONVERSATION: "CONNECTED",

    HOSTILE: "HOSTILE",
    ABUSIVE: "HOSTILE",

    REJECTED: "REJECTED",
    DECLINED: "REJECTED",

    VOICEMAIL: "VOICEMAIL",
    NO_ANSWER: "NO_ANSWER",
    NOANSWER: "NO_ANSWER",
  };

  return map[up] || up;
}

function firstRegexHit(text, regexes) {
  for (const r of regexes) {
    const m = text.match(r);
    if (m && m[0]) return m[0];
  }
  return "";
}

function detectBookedMeeting(transcriptLower) {
  const platform = firstRegexHit(transcriptLower, [
    /\bzoom\b/i,
    /\bgoogle meet\b/i,
    /\bmeet link\b/i,
    /\bmicrosoft teams\b/i,
    /\bteams link\b/i,
    /\bcalendar invite\b/i,
    /\bcalendly\b/i,
    /\bcalendar link\b/i,
    /\binvite you\b/i,
    /\bsend (you|ya) (a|the) (link|invite)\b/i,
    /\bi['’]?ll (send|text|email) (you|ya) (a|the) (zoom|meet|teams)? ?(link|invite)\b/i,
    /\blet['’]?s (do|hop on|jump on)\b.*\b(zoom|call|meeting)\b/i,
    /\b(discovery|consultation|intro)\s+call\b/i,
  ]);

  const scheduling = firstRegexHit(transcriptLower, [
    /\b(at|around)\s+\d{1,2}(:\d{2})?\s*(am|pm)\b/i,
    /\b\d{1,2}(:\d{2})?\s*(am|pm)\b/i,
    /\b(tomorrow|today|next week|next monday|next tuesday|next wednesday|next thursday|next friday)\b/i,
    /\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b/i,
    /\b(once|when) (you|ya) (get|receive) (the )?(invite|link)\b/i,
    /\b(send|text|email) (me|him|her|you) (the )?(invite|link)\b/i,
    /\b(i['’]?ll|we['’]?ll) (schedule|set up|book)\b/i,
  ]);

  const acceptance = firstRegexHit(transcriptLower, [
    /\bthat works\b/i,
    /\bsounds good\b/i,
    /\bokay\b/i,
    /\byes\b/i,
    /\bperfect\b/i,
    /\bdeal\b/i,
    /\blet['’]?s do it\b/i,
  ]);

  let score = 0;
  if (platform) score += 2;
  if (scheduling) score += 2;
  if (acceptance) score += 1;

  const hit = score >= 4;
  let evidence = "";
  if (hit) evidence = [platform, scheduling, acceptance].filter(Boolean).join(" • ");
  return { hit, evidence };
}

function detectHostile(transcriptLower) {
  const hit = firstRegexHit(transcriptLower, [
    /\bfuck (off|you)\b/i,
    /\bgo to hell\b/i,
    /\bstop calling\b/i,
    /\bdon't call (me|us) again\b/i,
    /\btake (me|us) off (your|the) list\b/i,
    /\basshole\b/i,
    /\bidiot\b/i,
  ]);
  return { hit: !!hit, evidence: hit };
}

function detectRejected(transcriptLower) {
  const hit = firstRegexHit(transcriptLower, [
    /\bnot interested\b/i,
    /\bno thanks\b/i,
    /\bwe['’]?re good\b/i,
    /\bwe already have\b/i,
    /\bdo not need\b/i,
    /\bnot right now\b/i,
    /\bmaybe later\b/i,
    /\bjust email\b/i,
  ]);
  return { hit: !!hit, evidence: hit };
}

function detectVoicemail(transcriptLower) {
  const hit = firstRegexHit(transcriptLower, [
    /\bvoicemail\b/i,
    /\bleave (a )?message\b/i,
    /\bafter the tone\b/i,
    /\bbeep\b/i,
  ]);
  return { hit: !!hit, evidence: hit };
}

function computeOutcome({ llmOutcome, transcript }) {
  const raw = String(transcript || "");
  const lower = raw.toLowerCase();

  const booked = detectBookedMeeting(lower);
  if (booked.hit) {
    return {
      key: "BOOKED_MEETING",
      reason: `Booked meeting detected from transcript evidence: ${booked.evidence || "meeting language + scheduling cues"}.`,
      evidence: booked.evidence || "",
    };
  }

  const hostile = detectHostile(lower);
  if (hostile.hit) {
    return {
      key: "HOSTILE",
      reason: `Hostile rejection detected from transcript evidence: ${hostile.evidence}.`,
      evidence: hostile.evidence || "",
    };
  }

  const rejected = detectRejected(lower);
  if (rejected.hit) {
    return {
      key: "REJECTED",
      reason: `Rejection detected from transcript evidence: ${rejected.evidence}.`,
      evidence: rejected.evidence || "",
    };
  }

  const vm = detectVoicemail(lower);
  if (vm.hit) {
    return {
      key: "VOICEMAIL",
      reason: `Voicemail detected from transcript evidence: ${vm.evidence}.`,
      evidence: vm.evidence || "",
    };
  }

  const norm = normalizeOutcomeKey(llmOutcome || "");
  if (norm) {
    return {
      key: norm,
      reason: `Classified as ${norm.replace(/_/g, " ").toLowerCase()} based on overall call content.`,
      evidence: "",
    };
  }

  return {
    key: "CONNECTED",
    reason: "Reached a real person and had a conversation. No strong evidence of a booked meeting or explicit rejection was detected.",
    evidence: "",
  };
}

// -------------------- Report sanitization (remove internal jargon) --------------------

function sanitizeReportForUsers(reportText) {
  let t = String(reportText || "");

  const replacements = [
    [/multiple label turns detected/gi, "Multiple speakers were detected in the call"],
    [/label turns detected/gi, "speaker turns were detected"],
    [/diarization/gi, "speaker detection"],
    [/token limit/gi, "length limit"],
  ];

  for (const [re, rep] of replacements) t = t.replace(re, rep);
  return t;
}

// -------------------- Support endpoint --------------------

app.post("/api/support", async (req, res) => {
  try {
    const userId = await requireUserId(req);
    if (!userId) return res.status(401).json({ error: "Unauthorized" });

    const message = String(req.body?.message || "").trim();
    const contact = String(req.body?.contact || "").trim();
    const page_url = String(req.body?.page_url || "").trim();

    if (!message || message.length < 5) {
      return res.status(400).json({ error: "Message is required (5+ characters)." });
    }

    const sb = supabaseService();

    const payload = {
      user_id: userId,
      message,
      contact: contact || null,
      page_url: page_url || null,
      created_at: new Date().toISOString(),
    };

    const { error } = await sb.from("support_tickets").insert([payload]);
    if (error) {
      return res.status(500).json({
        error:
          "Support submission failed. If this is your first time using support, make sure the 'support_tickets' table exists in Supabase.",
        detail: error.message,
      });
    }

    return res.json({ ok: true });
  } catch (err) {
    return res.status(500).json({ error: err?.message || String(err) });
  }
});

// -------------------- Runs API (history + details) --------------------

function enforceShortDisplayTitle(title) {
  const cleaned = String(title || "").trim();
  if (!cleaned) return "Call Analysis";
  const words = cleaned.replace(/[^\p{L}\p{N}\s'-]/gu, " ").replace(/\s+/g, " ").trim().split(" ").filter(Boolean);
  if (words.length <= 5) return toTitleCase(words.join(" "));
  return toTitleCase(words.slice(0, 5).join(" "));
}

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
        String(runTypeRaw).toLowerCase() === "upload"
          ? "Upload"
          : String(runTypeRaw).toLowerCase() === "record"
          ? "Record"
          : String(runTypeRaw).toLowerCase() === "practice"
          ? "Practice"
          : "Upload";

      const scenario_template = r.scenario_template ?? r.scenarioTemplate ?? r.category ?? null;

      const titleFromDb = pick(r, ["run_title", "title", "report_title"], "");
      const titleComputed = enforceShortDisplayTitle(titleFromDb || pick(r, ["user_context", "userContext"], "") || "Call Analysis");

      const call_outcome = pick(r, ["call_outcome", "callOutcome", "outcome"], null);
      const call_outcome_reason = pick(r, ["call_outcome_reason", "outcome_reason", "callOutcomeReason"], null);

      return {
        id: r.id,
        created_at: r.created_at,

        // Titles: return BOTH to prevent frontend regressions
        title: titleComputed,
        run_title: titleComputed,

        scenario_template,
        call_outcome,
        call_outcome_reason,

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
      String(runTypeRaw).toLowerCase() === "upload"
        ? "Upload"
        : String(runTypeRaw).toLowerCase() === "record"
        ? "Record"
        : String(runTypeRaw).toLowerCase() === "practice"
        ? "Practice"
        : "Upload";

    const scenario_template = data.scenario_template ?? data.scenarioTemplate ?? data.category ?? null;

    const titleFromDb = pick(data, ["run_title", "title", "report_title"], "");
    const titleComputed = enforceShortDisplayTitle(titleFromDb || pick(data, ["user_context", "userContext"], "") || "Call Analysis");

    const run = {
      ...data,
      mismatch,
      scenario_mismatch: mismatch,
      run_type: runTypeRaw,
      type,

      // Titles: return BOTH to prevent frontend regressions
      title: titleComputed,
      run_title: titleComputed,

      scenario_template,
    };

    return res.json({ run });
  } catch (err) {
    return res.status(500).json({ error: err?.message || String(err) });
  }
});

// -------------------- Calibrate endpoint (PERSIST RUNS) --------------------

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

    // 24h limit
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

    // Transcribe
    const asr = await transcribeWithAssemblyAI(file.buffer);
    const transcript = asr.transcript;
    const transcriptLines = asr.lines;

    // Run model + generate AI title in parallel (title uses transcript+context)
    const [result, aiTitleRaw] = await Promise.all([
      runCalibrate({ transcript, userContext, category, legacyScenario: "" }),
      generateAiShortTitle({ userContext, transcript, category }),
    ]);

    const computedTitle = enforceShortDisplayTitle(aiTitleRaw || "");

    // report text (sanitized)
    const reportRaw = pick(result, ["report", "coachingReport", "report_md", "reportMarkdown"], "") || "";
    const reportText = sanitizeReportForUsers(reportRaw);

    // mismatch: only meaningful when category selected
    const mismatchReasonRaw =
      pick(result, ["context_conflict_banner", "contextConflictBanner", "scenarioMismatch", "scenario_mismatch"], "") ||
      "";
    const mismatchReason = category ? String(mismatchReasonRaw || "").trim() : "";
    const mismatch = Boolean(category && mismatchReason);

    // outcome: override booked-meeting reliably
    const llmOutcome = pick(result, ["call_outcome", "callOutcome", "outcome"], "");
    const outcome = computeOutcome({ llmOutcome, transcript });

    const row = {
      user_id: userId,
      run_type: "upload",

      user_context: userContext,
      scenario_template: category || null,

      // titles (short AI)
      run_title: computedTitle,
      title: computedTitle,

      // content
      transcript_lines: transcriptLines,
      report_text: reportText,

      // diagnostics
      diagnostics: { ...result, _transcript_meta: asr.meta },

      // outcome
      call_outcome: outcome.key,
      call_outcome_reason: outcome.reason,
      call_outcome_evidence: outcome.evidence || null,

      // misc
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
