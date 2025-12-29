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

const ALLOWED_MIME = new Set([
  "audio/mpeg", // mp3
  "audio/mp3",
  "audio/mp4", // m4a (often audio/mp4)
  "audio/x-m4a",
  "audio/wav",
  "audio/x-wav",
  "audio/ogg",
  "application/ogg",
]);

const ALLOWED_EXT = new Set([".mp3", ".m4a", ".wav", ".ogg"]);

// simple per-user in-flight lock (Render WEB_CONCURRENCY=1 but still useful)
const activeRunByUser = new Map(); // userId -> startedAtMs

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: MAX_UPLOAD_BYTES },
});

// serve static files (index.html, etc.)
app.use(express.static(__dirname, { extensions: ["html"] }));

app.get("/health", (req, res) =>
  res.json({
    ok: true,
    enforcer: ENFORCER_VERSION,
    commit: process.env.RENDER_GIT_COMMIT || null,
  })
);

// Expose anon config for frontend (SAFE: anon key is meant for browser use)
app.get("/api/config", (req, res) => {
  res.json({
    supabaseUrl: process.env.SUPABASE_URL || "",
    supabaseAnonKey: process.env.SUPABASE_ANON_KEY || "",
    enforcer: ENFORCER_VERSION,
  });
});

// ---- Supabase clients (lazy) ----
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

// Normalize mismatch even if your DB column name changes or doesn't exist
function normalizeScenarioMismatch(row) {
  if (!row || typeof row !== "object") return null;
  if ("scenario_mismatch" in row) return row.scenario_mismatch;
  if ("mismatch" in row) return row.mismatch; // keep compatibility
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
    const m = line.match(/^([^:]{1,30}):\s*(.*)$/);
    if (!m) return { speaker: "Other", text: line };
    return { speaker: (m[1] || "Other").trim(), text: (m[2] || "").trim() };
  });
}

async function insertRunResilient(sb, payload) {
  // tries to insert, and if schema is missing a column, removes it and retries
  let p = { ...payload };
  for (let i = 0; i < 12; i++) {
    const { data, error } = await sb.from("runs").insert([p]).select("id").maybeSingle();
    if (!error) return data;

    const msg = String(error.message || "");
    const m = msg.match(/column\s+\w+\.(\w+)\s+does not exist/i);
    if (m && m[1]) {
      delete p[m[1]];
      continue;
    }
    throw new Error(msg);
  }
  throw new Error("Insert failed after retries (schema mismatch).");
}

// -------------------- Runs API (history + details) --------------------

// List recent runs for signed-in user
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
        // return BOTH names so the frontend never breaks
        mismatch,
        scenario_mismatch: mismatch,
      };
    });

    return res.json({ runs });
  } catch (err) {
    return res.status(500).json({ error: err?.message || String(err) });
  }
});

// Get one run (details page)
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
};

// -------------------- Existing Calibrate endpoint --------------------

function prettifyTranscript(raw) {
  let t = (raw ?? "").toString().trim();
  if (!t) return "";

  t = t.replace(/\r\n/g, "\n");
  t = t.replace(/\s*(Speaker\s*[AB]\s*:)\s*/gi, "\n$1 ");
  t = t.replace(/\s*(Prospect|Rep|Caller|Agent|Customer)\s*[:.]\s*/gi, "\n$1: ");
  t = t.replace(/([.!?])\s+(Prospect|Rep|Caller|Agent|Customer)\s+(?=[A-Za-z0-9])/gi, "$1\n$2: ");
  t = t.replace(/([.!?])\s+(Prospect|Rep|Caller|Agent|Customer)\s*[:.]\s*/gi, "$1\n$2: ");

  t = t
    .replace(/[ \t]+/g, " ")
    .replace(/\n[ \t]+/g, "\n")
    .replace(/\n{2,}/g, "\n")
    .trim();

  let lines = t.split("\n").map((l) => l.trim()).filter(Boolean);

  const labelRe = /^(Speaker\s*[AB]|Prospect|Rep|Caller|Agent|Customer)\s*:\s*(.*)$/i;
  lines = lines.map((line) => {
    const m = line.match(labelRe);
    if (!m) return line;

    const label = (m[1] || "").toLowerCase();
    const text = (m[2] || "").trim();

    if (label.startsWith("speaker")) return `${m[1]}: ${text}`;
    if (label === "prospect" || label === "customer") return `Prospect: ${text}`;
    return `You: ${text}`;
  });

  // NOTE: if AssemblyAI only gave one speaker, everything will be Speaker A.
  // We leave it as-is (You/Prospect mapping only happens when both speakers are present).
  const speakerALines = lines.filter((l) => /^Speaker\s*A\s*:/i.test(l));
  const speakerBLines = lines.filter((l) => /^Speaker\s*B\s*:/i.test(l));

  if (speakerALines.length && speakerBLines.length) {
    const aText = speakerALines.map((l) => l.replace(/^Speaker\s*A\s*:\s*/i, "")).join(" ");
    const bText = speakerBLines.map((l) => l.replace(/^Speaker\s*B\s*:\s*/i, "")).join(" ");

    const repSignals = [
      /\bhey\b/i, /\bhi\b/i, /\bthis\s+is\b/i, /\bmy\s+name\s+is\b/i, /\bi['’]?m\b/i,
      /\bi\s+help\b/i, /\bwe\s+help\b/i, /\bi['’]?ll\b/i, /\bquick\b/i, /\bseconds?\b/i,
      /\bcan\s+i\b/i, /\bcalling\b/i,
    ];
    const prospectSignals = [
      /\bwho\s+is\s+this\b/i, /\bhow\s+did\s+you\s+get\b/i, /\bnot\s+interested\b/i,
      /\bmake\s+it\s+fast\b/i, /\bjust\s+email\b/i, /\bwhat\s+does\s+it\s+cost\b/i,
      /\bwe['’]?ve\s+tried\b/i, /\bwe\s+already\b/i, /\bwe['’]?ve\s+been\s+burned\b/i,
    ];

    const score = (txt) => {
      let rep = 0, pro = 0;
      for (const r of repSignals) if (r.test(txt)) rep++;
      for (const p of prospectSignals) if (p.test(txt)) pro++;
      return { rep, pro, net: rep - pro };
    };

    const a = score(aText);
    const b = score(bText);

    let aIsYou = a.net !== b.net ? a.net > b.net : null;

    if (aIsYou !== null) {
      lines = lines.map((l) => {
        if (/^Speaker\s*A\s*:/i.test(l)) return l.replace(/^Speaker\s*A\s*:/i, aIsYou ? "You:" : "Prospect:");
        if (/^Speaker\s*B\s*:/i.test(l)) return l.replace(/^Speaker\s*B\s*:/i, aIsYou ? "Prospect:" : "You:");
        return l;
      });
    }
  }

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
    body: JSON.stringify({ audio_url, punctuate: true, format_text: true, speaker_labels: true }),
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
        return pollJson.utterances
          .map((u) => `${u.speaker === 0 ? "Speaker A:" : "Speaker B:"} ${(u.text || "").trim()}`)
          .join("\n");
      }
      return (pollJson.text || "").trim();
    }

    if (pollJson.status === "error") throw new Error("AssemblyAI transcription error: " + (pollJson.error || "unknown"));
    if (Date.now() - started > 4 * 60 * 1000) throw new Error("AssemblyAI transcription timed out.");

    await new Promise((r) => setTimeout(r, 1500));
  }
}

app.post("/api/run", upload.single("audio"), async (req, res) => {
  const userId = await requireUserId(req);
  if (!userId) return res.status(401).json({ error: "Unauthorized" });

  // one-run-at-a-time per user
  if (activeRunByUser.has(userId)) {
    return res.status(429).json({ error: "A run is already in progress for this account. Please wait." });
  }

  try {
    activeRunByUser.set(userId, Date.now());

    const file = req.file;
    if (!file?.buffer) return res.status(400).json({ error: "No audio file uploaded (field name must be 'audio')." });

    const ext = path.extname(file.originalname || "").toLowerCase();
    if (ext && !ALLOWED_EXT.has(ext)) {
      return res.status(400).json({ error: "Unsupported audio format. Use mp3, m4a, wav, or ogg." });
    }
    if (file.mimetype && !ALLOWED_MIME.has(file.mimetype)) {
      // some browsers send weird mimetypes; extension check above helps
      return res.status(400).json({ error: "Unsupported audio format. Use mp3, m4a, wav, or ogg." });
    }

    const userContext = (req.body?.context || "").trim();
    const category = (req.body?.category || "").trim();
    const legacyScenario = (req.body?.legacyScenario || "").trim();
    if (userContext.length < 10) return res.status(400).json({ error: "Context is required (10+ characters)." });

    // per-24h limit
    const cutoff = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    const sb = supabaseService();
    const { count, error: countErr } = await sb
      .from("runs")
      .select("id", { count: "exact", head: true })
      .eq("user_id", userId)
      .gte("created_at", cutoff);

    if (countErr) return res.status(500).json({ error: countErr.message });
    if ((count || 0) >= MAX_RUNS_PER_24H) {
      return res.status(429).json({ error: `Daily limit reached (${MAX_RUNS_PER_24H}/24h). Try again later.` });
    }

    const transcriptRaw = await transcribeWithAssemblyAI(file.buffer);
    const transcript = prettifyTranscript(transcriptRaw);

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
      // optional future-proof fields (safe-removed if schema doesn't have them)
      transcript_text: transcript,
    };

    const inserted = await insertRunResilient(sb, row);
    const runId = inserted?.id;

    return res.json({ run_id: runId });
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
app.get(/^(?!\/api\/).*/, (req, res) => res.sendFile(path.join(__dirname, "index.html")));

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
app.listen(PORT, () => {
  console.log(`Calibrate MVP running on port ${PORT}`);
  console.log(`Enforcer loaded: ${ENFORCER_VERSION}`);
});
