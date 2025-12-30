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

// serve static files
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

// -------------------- Transcript building + smoothing --------------------

function speakerLabelFromUtterance(u) {
  const sp = u?.speaker;

  if (typeof sp === "number" && Number.isFinite(sp)) return `Speaker ${sp}`;

  if (typeof sp === "string") {
    const s = sp.trim();
    if (!s) return "Speaker X";
    if (/^\d+$/.test(s)) return `Speaker ${parseInt(s, 10)}`;
    if (/^[A-Za-z]$/.test(s)) return `Speaker ${s.toUpperCase()}`;
    return `Speaker ${s}`;
  }

  return "Speaker X";
}

function isLikelyContinuation(prev, curr) {
  const prevText = String(prev?.text || "").trim();
  const currText = String(curr?.text || "").trim();
  if (!prevText || !currText) return false;

  const gap = Number.isFinite(curr?.start) && Number.isFinite(prev?.end) ? (curr.start - prev.end) : 0;
  const gapSmall = !Number.isFinite(gap) ? true : gap <= 900; // ms

  const prevShort = prevText.length <= 80;

  const contStarters = /^(and|but|so|because|at|according|until|then|also|or|if|when|where|which|that|with|for|in|on|to|from)\b/i;
  const startsContinuationWord = contStarters.test(currText);

  const startsLowercase = /^[a-z]/.test(currText);

  const prevEndsClosed = /[.!?]["')\]]?$/.test(prevText);
  const prevEndsOpen = !prevEndsClosed || /[,;:—-]$/.test(prevText);

  // We accept continuation if:
  // - very small gap AND (prev is "open" OR prev is short) AND current looks like a continuation start
  return gapSmall && (prevEndsOpen || prevShort) && (startsContinuationWord || startsLowercase);
}

function smoothAndMergeUtterances(utterances) {
  const items = (utterances || []).map((u) => ({
    speaker: speakerLabelFromUtterance(u),
    text: String(u?.text || "").trim(),
    start: typeof u?.start === "number" ? u.start : null,
    end: typeof u?.end === "number" ? u.end : null,
  })).filter(x => x.text);

  if (items.length === 0) return [];

  // Pass 1: fix obvious "continuation flips"
  for (let i = 1; i < items.length; i++) {
    const prev = items[i - 1];
    const curr = items[i];
    if (curr.speaker !== prev.speaker && isLikelyContinuation(prev, curr)) {
      curr.speaker = prev.speaker;
    }
  }

  // Pass 2: merge adjacent same-speaker fragments
  const merged = [];
  for (const it of items) {
    const last = merged[merged.length - 1];
    if (last && last.speaker === it.speaker) {
      const joiner = last.text.endsWith("-") ? "" : " ";
      last.text = (last.text + joiner + it.text).replace(/\s+/g, " ").trim();
      last.end = it.end ?? last.end;
    } else {
      merged.push({ ...it });
    }
  }

  return merged;
}

function inferRepProspectMap(twoSpeakers, mergedItems) {
  const [s1, s2] = twoSpeakers;

  const textFor = (speaker) =>
    mergedItems.filter(x => x.speaker === speaker).map(x => x.text).join(" ").slice(0, 6000);

  const repSignals = [
    /\bmy name is\b/i,
    /\bi'?m\b/i,
    /\bi am\b/i,
    /\bcalling\b/i,
    /\breaching out\b/i,
    /\bquick\b/i,
    /\bseconds?\b/i,
    /\bwork with\b/i,
    /\bcan i\b/i,
    /\bdo you have\b/i,
    /\bare you\b/i,
    /\bwould you\b/i,
  ];

  const prospectSignals = [
    /\bnot interested\b/i,
    /\bprobably not\b/i,
    /\bwe'?re good\b/i,
    /\balready (have|using|working)\b/i,
    /\bjust email\b/i,
    /\bhow much\b/i,
    /\bno\b/i,
    /\byes\b/i,
  ];

  const score = (txt) => {
    let rep = 0, pro = 0;
    for (const r of repSignals) if (r.test(txt)) rep++;
    for (const p of prospectSignals) if (p.test(txt)) pro++;
    return { rep, pro, net: rep - pro };
  };

  const a = score(textFor(s1));
  const b = score(textFor(s2));

  // Strong preference: whoever contains "my name is" is rep
  const aHasIntro = /\bmy name is\b/i.test(textFor(s1));
  const bHasIntro = /\bmy name is\b/i.test(textFor(s2));
  if (aHasIntro && !bHasIntro) return { [s1]: "You", [s2]: "Prospect" };
  if (bHasIntro && !aHasIntro) return { [s2]: "You", [s1]: "Prospect" };

  const diff = Math.abs(a.net - b.net);

  // If we have any signal at all, map; otherwise keep Speaker A/B
  if (diff >= 1) {
    const repSpeaker = a.net >= b.net ? s1 : s2;
    const proSpeaker = repSpeaker === s1 ? s2 : s1;
    return { [repSpeaker]: "You", [proSpeaker]: "Prospect" };
  }

  return null;
}

function buildTranscriptFromUtterances(utterances) {
  const merged = smoothAndMergeUtterances(utterances);
  if (merged.length === 0) return "";

  const speakers = [...new Set(merged.map(x => x.speaker))];

  // If exactly 2 speakers, map consistently to You/Prospect for ALL lines
  let roleMap = null;
  if (speakers.length === 2) {
    roleMap = inferRepProspectMap(speakers, merged);
  }

  return merged.map((x) => {
    const label = roleMap?.[x.speaker] || x.speaker;
    return `${label}: ${x.text}`;
  }).join("\n");
}

// Light formatting (no “smart” relabeling anymore; server transcript is already consistent)
function prettifyTranscript(raw) {
  let t = (raw ?? "").toString().trim();
  if (!t) return "";

  t = t.replace(/\r\n/g, "\n");
  t = t.replace(/[ \t]+/g, " ").replace(/\n[ \t]+/g, "\n").replace(/\n{2,}/g, "\n").trim();

  // normalize "Speaker0:" -> "Speaker 0:"
  t = t.replace(/^speaker\s*([a-z]|\d+)\s*:/gim, "Speaker $1:");

  return t;
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
      // IMPORTANT: build from utterances with smoothing + consistent You/Prospect mapping
      if (Array.isArray(pollJson.utterances) && pollJson.utterances.length) {
        const built = buildTranscriptFromUtterances(pollJson.utterances);
        return prettifyTranscript(built);
      }
      return prettifyTranscript((pollJson.text || "").trim());
    }

    if (pollJson.status === "error") throw new Error("AssemblyAI transcription error: " + (pollJson.error || "unknown"));
    if (Date.now() - started > 4 * 60 * 1000) throw new Error("AssemblyAI transcription timed out.");

    await new Promise((r) => setTimeout(r, 1500));
  }
}

// -------------------- Calibrate endpoint --------------------

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

    const transcript = await transcribeWithAssemblyAI(file.buffer);

    const result = await runCalibrate({ transcript, userContext, category, legacyScenario });

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
      transcript_lines: transcriptToLines(transcript),
      report_text: reportText,
      diagnostics: result,
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

// unknown /api route
app.use("/api", (req, res) => res.status(404).json({ error: "Unknown API route" }));

// SPA fallback
app.get(/^(?!\/api\/).*/, (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
app.listen(PORT, () => {
  console.log(`Calibrate MVP running on port ${PORT}`);
  console.log(`Enforcer loaded: ${ENFORCER_VERSION}`);
});
