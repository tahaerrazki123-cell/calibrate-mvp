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
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 }, // 50MB
});

app.use(express.static(__dirname, { extensions: ["html"] }));

app.get("/health", (req, res) => res.json({ ok: true, enforcer: ENFORCER_VERSION }));

// ---- Supabase env ----
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "";
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";

if (!SUPABASE_URL) console.warn("⚠ Missing SUPABASE_URL in env");
if (!SUPABASE_ANON_KEY) console.warn("⚠ Missing SUPABASE_ANON_KEY in env");
if (!SUPABASE_SERVICE_ROLE_KEY) console.warn("⚠ Missing SUPABASE_SERVICE_ROLE_KEY in env");

const supabaseAdmin =
  SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
    ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
        auth: { persistSession: false, autoRefreshToken: false },
      })
    : null;

// SAFE public config (browser needs this)
app.get("/api/config", (req, res) => {
  res.json({
    supabaseUrl: SUPABASE_URL,
    supabaseAnonKey: SUPABASE_ANON_KEY,
    enforcer: ENFORCER_VERSION,
  });
});

function getBearerToken(req) {
  const h = req.headers.authorization || "";
  const m = h.match(/^Bearer\s+(.+)$/i);
  return m ? m[1].trim() : "";
}

async function getUserFromRequest(req) {
  if (!supabaseAdmin) return null;
  const token = getBearerToken(req);
  if (!token) return null;

  const { data, error } = await supabaseAdmin.auth.getUser(token);
  if (error || !data?.user) return null;
  return data.user;
}

async function requireUser(req, res) {
  const user = await getUserFromRequest(req);
  if (!user) {
    res.status(401).json({ error: "Not signed in (missing/invalid Authorization bearer token)." });
    return null;
  }
  return user;
}

function transcriptToLines(transcript) {
  const lines = (transcript || "")
    .split("\n")
    .map((s) => s.trim())
    .filter(Boolean);

  return lines.map((line, i) => {
    const m = line.match(/^(You|Prospect)\s*:\s*(.*)$/i);
    if (m) {
      return {
        n: i + 1,
        speaker: m[1][0].toUpperCase() + m[1].slice(1).toLowerCase(),
        text: (m[2] || "").trim(),
      };
    }
    return { n: i + 1, speaker: "Unknown", text: line };
  });
}

function pick(obj, keys, fallback = null) {
  for (const k of keys) {
    if (obj && obj[k] !== undefined && obj[k] !== null && String(obj[k]).trim() !== "") return obj[k];
  }
  return fallback;
}

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

  const speakerALines = lines.filter((l) => /^Speaker\s*A\s*:/i.test(l));
  const speakerBLines = lines.filter((l) => /^Speaker\s*B\s*:/i.test(l));

  if (speakerALines.length || speakerBLines.length) {
    const aText = speakerALines.map((l) => l.replace(/^Speaker\s*A\s*:\s*/i, "")).join(" ");
    const bText = speakerBLines.map((l) => l.replace(/^Speaker\s*B\s*:\s*/i, "")).join(" ");

    const repSignals = [
      /\bhey\b/i,
      /\bhi\b/i,
      /\bthis\s+is\b/i,
      /\bmy\s+name\s+is\b/i,
      /\bi['’]?m\b/i,
      /\bi\s+help\b/i,
      /\bwe\s+help\b/i,
      /\bi['’]?ll\b/i,
      /\bquick\b/i,
      /\bseconds?\b/i,
      /\bcan\s+i\b/i,
      /\bcalling\b/i,
    ];
    const prospectSignals = [
      /\bwho\s+is\s+this\b/i,
      /\bhow\s+did\s+you\s+get\b/i,
      /\bnot\s+interested\b/i,
      /\bmake\s+it\s+fast\b/i,
      /\bjust\s+email\b/i,
      /\bwhat\s+does\s+it\s+cost\b/i,
      /\bwe['’]?ve\s+tried\b/i,
      /\bwe\s+already\b/i,
      /\bwe['’]?ve\s+been\s+burned\b/i,
    ];

    const score = (txt) => {
      let rep = 0,
        pro = 0;
      for (const r of repSignals) if (r.test(txt)) rep++;
      for (const p of prospectSignals) if (p.test(txt)) pro++;
      return { rep, pro, net: rep - pro };
    };

    const a = score(aText);
    const b = score(bText);

    let aIsYou = null;

    if (a.net !== b.net) {
      aIsYou = a.net > b.net;
    } else {
      const firstSpeakerLine = lines.find((l) => /^Speaker\s*[AB]\s*:/i.test(l)) || "";
      if (/^Speaker\s*[AB]\s*:\s*(hey|hi)\b/i.test(firstSpeakerLine)) {
        aIsYou = /^Speaker\s*A\s*:/i.test(firstSpeakerLine);
      }
    }

    if (aIsYou !== null) {
      lines = lines.map((l) => {
        if (/^Speaker\s*A\s*:/i.test(l))
          return l.replace(/^Speaker\s*A\s*:/i, aIsYou ? "You:" : "Prospect:");
        if (/^Speaker\s*B\s*:/i.test(l))
          return l.replace(/^Speaker\s*B\s*:/i, aIsYou ? "Prospect:" : "You:");
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
  if (!uploadRes.ok) {
    const t = await uploadRes.text().catch(() => "");
    throw new Error("AssemblyAI upload failed: " + t);
  }
  const uploadJson = await uploadRes.json();
  const audio_url = uploadJson.upload_url;

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
  if (!createRes.ok) {
    const t = await createRes.text().catch(() => "");
    throw new Error("AssemblyAI transcript create failed: " + t);
  }
  const createJson = await createRes.json();
  const id = createJson.id;

  const started = Date.now();
  while (true) {
    const pollRes = await fetch(`https://api.assemblyai.com/v2/transcript/${id}`, {
      headers: { authorization: apiKey },
    });
    if (!pollRes.ok) {
      const t = await pollRes.text().catch(() => "");
      throw new Error("AssemblyAI transcript poll failed: " + t);
    }
    const pollJson = await pollRes.json();

    if (pollJson.status === "completed") {
      if (Array.isArray(pollJson.utterances) && pollJson.utterances.length) {
        const lines = pollJson.utterances.map((u) => {
          const speaker = u.speaker === 0 ? "Speaker A:" : "Speaker B:";
          const text = (u.text || "").trim();
          return `${speaker} ${text}`;
        });
        return lines.join("\n");
      }
      return (pollJson.text || "").trim();
    }

    if (pollJson.status === "error") {
      throw new Error("AssemblyAI transcription error: " + (pollJson.error || "unknown"));
    }

    if (Date.now() - started > 4 * 60 * 1000) {
      throw new Error("AssemblyAI transcription timed out.");
    }

    await new Promise((r) => setTimeout(r, 1500));
  }
}

// ---- Auth + history APIs ----
app.get("/api/me", async (req, res) => {
  const user = await requireUser(req, res);
  if (!user) return;
  res.json({ user: { id: user.id, email: user.email } });
});

app.get("/api/runs", async (req, res) => {
  const user = await requireUser(req, res);
  if (!user) return;
  if (!supabaseAdmin) return res.status(500).json({ error: "Supabase not configured." });

  const { data, error } = await supabaseAdmin
    .from("runs")
    .select("id, created_at, scenario_template, call_outcome, enforcer, mismatch")
    .eq("user_id", user.id)
    .order("created_at", { ascending: false })
    .limit(50);

  if (error) return res.status(500).json({ error: error.message });
  return res.json({ runs: data || [] });
});

app.get("/api/runs/:id", async (req, res) => {
  const user = await requireUser(req, res);
  if (!user) return;
  if (!supabaseAdmin) return res.status(500).json({ error: "Supabase not configured." });

  const runId = (req.params.id || "").trim();
  const { data, error } = await supabaseAdmin
    .from("runs")
    .select("*")
    .eq("id", runId)
    .eq("user_id", user.id)
    .maybeSingle();

  if (error) return res.status(500).json({ error: error.message });
  if (!data) return res.status(404).json({ error: "Run not found." });

  return res.json({ run: data });
});

// ---- Main pipeline (saves if logged in) ----
app.post("/api/run", upload.single("audio"), async (req, res) => {
  try {
    const file = req.file;
    if (!file?.buffer) {
      return res.status(400).json({ error: "No audio file uploaded (field name must be 'audio')." });
    }

    const userContext = (req.body?.context || "").trim();
    const category = (req.body?.category || "").trim();
    const legacyScenario = (req.body?.legacyScenario || "").trim();

    if (userContext.length < 10) {
      return res.status(400).json({ error: "Context is required (10+ characters)." });
    }

    const transcriptRaw = await transcribeWithAssemblyAI(file.buffer);
    const transcript = prettifyTranscript(transcriptRaw);

    const result = await runCalibrate({
      transcript,
      userContext,
      category,
      legacyScenario,
    });

    let run_id = null;

    // Save to DB only if logged in
    const user = await getUserFromRequest(req);
    if (user && supabaseAdmin) {
      try {
        const callOutcome = pick(result, ["callOutcome", "call_outcome", "outcome"], null);
        const outcomeReason = pick(result, ["outcomeReason", "outcome_reason"], null);
        const reportText = pick(result, ["reportText", "report", "coachingReport"], "");
        const bestScript = pick(result, ["bestScript", "best_script", "script"], "");
        const section5Pass = pick(result, ["section5Pass", "section5_pass"], null);
        const scriptWords = pick(result, ["scriptWords", "script_words"], null);
        const mismatch = !!pick(result, ["mismatch", "scenarioMismatch"], false);
        const mismatchReason = pick(result, ["mismatchReason", "mismatch_reason"], null);

        const transcript_lines = transcriptToLines(transcript);

        const row = {
          user_id: user.id,
          scenario_template: category || null,
          legacy_scenario: legacyScenario || null,
          user_context: userContext,

          call_outcome: callOutcome,
          outcome_reason: outcomeReason,

          transcript_lines,
          report_text: reportText || "",
          best_script: bestScript || "",
          section5_pass: section5Pass,
          script_words: typeof scriptWords === "number" ? scriptWords : null,

          enforcer: ENFORCER_VERSION,
          mismatch,
          mismatch_reason: mismatchReason,

          diagnostics: result,
        };

        const { data: insData, error: insErr } = await supabaseAdmin
          .from("runs")
          .insert(row)
          .select("id")
          .single();

        if (insErr) console.warn("⚠ Failed to insert run:", insErr.message);
        run_id = insData?.id || null;
      } catch (e) {
        console.warn("⚠ Save run failed (non-fatal):", e?.message || String(e));
      }
    }

    return res.json({ transcript, run_id, ...result });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err?.message || String(err) });
  }
});

// SPA routes
app.get("/login", (req, res) => res.sendFile(path.join(__dirname, "index.html")));
app.get("/home", (req, res) => res.sendFile(path.join(__dirname, "index.html")));
app.get("/calibrate", (req, res) => res.sendFile(path.join(__dirname, "index.html")));
app.get("/runs/:id", (req, res) => res.sendFile(path.join(__dirname, "index.html")));
app.get("/", (req, res) => res.sendFile(path.join(__dirname, "index.html")));

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
app.listen(PORT, () => {
  console.log(`Calibrate MVP running at http://localhost:${PORT}`);
  console.log(`Enforcer loaded: ${ENFORCER_VERSION}`);
});
