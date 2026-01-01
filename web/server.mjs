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

const upload = multer({ dest: path.join(os.tmpdir(), "calibrate_uploads") });

const PORT = process.env.PORT || 3000;

const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "";
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";

const ASSEMBLYAI_API_KEY = process.env.ASSEMBLYAI_API_KEY || "";
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || "";

if (!SUPABASE_URL || !SUPABASE_ANON_KEY || !SUPABASE_SERVICE_ROLE_KEY) {
  console.warn(
    "⚠ Missing SUPABASE_URL / SUPABASE_ANON_KEY / SUPABASE_SERVICE_ROLE_KEY env vars"
  );
}

const supabaseAdmin = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

function sha1(buf) {
  return crypto.createHash("sha1").update(buf).digest("hex");
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

function requireUser(req) {
  const auth = req.headers.authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
  if (!token) return null;
  return token;
}

async function getUserFromReq(req) {
  const auth = req.headers.authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
  if (!token) return null;

  const { data, error } = await supabaseAdmin.auth.getUser(token);
  if (error) return null;
  return data.user || null;
}

app.get("/api/config", (req, res) => {
  res.json({
    supabaseUrl: SUPABASE_URL,
    supabaseAnonKey: SUPABASE_ANON_KEY,
  });
});

app.get("/api/health", (req, res) => {
  res.json({ ok: true, time: nowIso() });
});

async function assemblyTranscribe(filePath) {
  if (!ASSEMBLYAI_API_KEY) throw new Error("Missing ASSEMBLYAI_API_KEY");

  const audioData = fs.readFileSync(filePath);

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
  const tr = await fetch("https://api.assemblyai.com/v2/transcript", {
    method: "POST",
    headers: {
      authorization: ASSEMBLYAI_API_KEY,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      audio_url: upJson.upload_url,
      speaker_labels: true,
      punctuate: true,
      format_text: true,
    }),
  });
  if (!tr.ok) throw new Error(`AssemblyAI transcript start failed: ${await tr.text()}`);
  const trJson = await tr.json();

  // 3) poll status
  const id = trJson.id;
  for (let i = 0; i < 120; i++) {
    await new Promise((r) => setTimeout(r, 2500));
    const st = await fetch(`https://api.assemblyai.com/v2/transcript/${id}`, {
      headers: { authorization: ASSEMBLYAI_API_KEY },
    });
    if (!st.ok) throw new Error(`AssemblyAI transcript status failed: ${await st.text()}`);
    const stJson = await st.json();
    if (stJson.status === "completed") {
      // Build a readable speaker transcript if utterances exist
      const utterances = stJson.utterances || [];
      if (utterances.length) {
        const lines = utterances.map((u) => {
          const sp = u.speaker != null ? `Speaker ${u.speaker}` : "Speaker";
          return `${sp}: ${u.text}`;
        });
        return { text: lines.join("\n"), raw: stJson };
      }
      return { text: stJson.text || "", raw: stJson };
    }
    if (stJson.status === "error") throw new Error(`AssemblyAI error: ${stJson.error}`);
  }

  throw new Error("AssemblyAI transcript timed out");
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

function buildAnalysisPrompt({ transcript, context, scenario, entityAggregate }) {
  const base = `
You are Calibrate — a post-call decision engine for cold calls.

Output MUST be valid JSON.

Return keys:
- call_result: { label: string, why: string }
- score: number (0-100)
- signals: string[] (short badges)
- follow_up: { text: string }
- top_fixes: [{ title: string, why: string, do_instead: string }]
- notes: { quotes: [{ quote: string, why: string }] }

Rules:
- Be concise and actionable.
- Use transcript as source of truth.
- Follow-up should be 2 lines max.
- Top fixes must be exactly 3 items.
- Quotes: include 2-4 short quotes, max 18 words each.
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
${transcript || ""}
`;

  return { system: base.trim(), user: user.trim() };
}

async function inferOutcomeLabel(analysisJson) {
  return analysisJson?.call_result?.label || "Unknown";
}

// -------- Entities API --------

app.get("/api/entities", async (req, res) => {
  const user = await getUserFromReq(req);
  if (!user) return res.status(401).json({ error: "Unauthorized" });

  const { data, error } = await supabaseAdmin
    .from("entities")
    .select("*")
    .eq("user_id", user.id)
    .order("created_at", { ascending: false });

  if (error) return res.status(400).json({ error: error.message });
  res.json({ entities: data || [] });
});

app.post("/api/entities", async (req, res) => {
  const user = await getUserFromReq(req);
  if (!user) return res.status(401).json({ error: "Unauthorized" });

  const { id, name, offer, industry, notes } = req.body || {};
  if (!name) return res.status(400).json({ error: "Missing name" });

  if (id) {
    const { error } = await supabaseAdmin
      .from("entities")
      .update({ name, offer, industry, notes })
      .eq("id", id)
      .eq("user_id", user.id);

    if (error) return res.status(400).json({ error: error.message });
    return res.json({ ok: true });
  }

  const { data, error } = await supabaseAdmin
    .from("entities")
    .insert([{ user_id: user.id, name, offer, industry, notes }])
    .select()
    .single();

  if (error) return res.status(400).json({ error: error.message });
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
  const user = await getUserFromReq(req);
  if (!user) return res.status(401).json({ error: "Unauthorized" });

  const entityId = req.params.id;

  const { data: entity, error: eErr } = await supabaseAdmin
    .from("entities")
    .select("*")
    .eq("id", entityId)
    .eq("user_id", user.id)
    .single();

  if (eErr) return res.status(400).json({ error: eErr.message });

  const { data: runs, error: rErr } = await supabaseAdmin
    .from("runs")
    .select("transcript_text, analysis_json, created_at")
    .eq("user_id", user.id)
    .eq("entity_id", entityId)
    .order("created_at", { ascending: false })
    .limit(18);

  if (rErr) return res.status(400).json({ error: rErr.message });
  if (!runs?.length) return res.status(400).json({ error: "No runs found for this entity yet." });

  const transcripts = runs.map((r) => r.transcript_text).filter(Boolean);
  const system = `
You are Calibrate. Generate an MVP playbook for a cold-calling entity.

Return valid JSON:
{
  "entity": { "name": string, "offer": string, "industry": string },
  "ultimate_script": { "opener": string, "pitch": string, "qualify": string, "close": string },
  "common_objections": [{ "objection": string, "best_response": string }],
  "dont_say": string[],
  "say_instead": string[],
  "patterns": [{ "pattern": string, "impact": string, "fix": string }]
}

Rules:
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
${transcripts.slice(0, 10).map((t, i) => `--- TRANSCRIPT ${i + 1} ---\n${t}`).join("\n\n")}
`.trim();

  const playbook = await deepseekJSON(system, userPrompt, 0.25);

  // store (optional)
  await supabaseAdmin
    .from("entity_playbooks")
    .insert([
      {
        user_id: user.id,
        entity_id: entityId,
        title: `Playbook ${new Date().toISOString().slice(0, 10)}`,
        playbook_json: playbook,
      },
    ])
    .select()
    .maybeSingle();

  res.json({ playbook });
});

// -------- Runs API --------

app.get("/api/runs", async (req, res) => {
  const user = await getUserFromReq(req);
  if (!user) return res.status(401).json({ error: "Unauthorized" });

  const { data, error } = await supabaseAdmin
    .from("runs")
    .select("id, created_at, scenario, outcome_label, entity_id, entities(name)")
    .eq("user_id", user.id)
    .order("created_at", { ascending: false })
    .limit(50);

  if (error) return res.status(400).json({ error: error.message });

  const runs = (data || []).map((r) => ({
    ...r,
    entity_name: r.entities?.name || "",
  }));

  res.json({ runs });
});

app.get("/api/runs/:id", async (req, res) => {
  const user = await getUserFromReq(req);
  if (!user) return res.status(401).json({ error: "Unauthorized" });

  const id = req.params.id;

  const { data, error } = await supabaseAdmin
    .from("runs")
    .select("*")
    .eq("id", id)
    .eq("user_id", user.id)
    .single();

  if (error) return res.status(400).json({ error: error.message });
  res.json({ run: data });
});

app.post("/api/runs/:id/regen_followup", async (req, res) => {
  const user = await getUserFromReq(req);
  if (!user) return res.status(401).json({ error: "Unauthorized" });

  const id = req.params.id;

  const { data: run, error } = await supabaseAdmin
    .from("runs")
    .select("*")
    .eq("id", id)
    .eq("user_id", user.id)
    .single();

  if (error) return res.status(400).json({ error: error.message });

  const prompt = `
Generate a new 2-line follow-up message based on this call transcript and context.
Return JSON: { "follow_up": { "text": string } }
Be concise, confident, and specific.
Transcript:
${run.transcript_text || ""}
Context:
${run.context_text || ""}
`.trim();

  const system = `You are Calibrate. Return valid JSON only.`.trim();
  const result = await deepseekJSON(system, prompt, 0.35);

  const updated = {
    ...(run.analysis_json || {}),
    follow_up: result.follow_up || { text: "" },
  };

  const { data: saved, error: uErr } = await supabaseAdmin
    .from("runs")
    .update({ analysis_json: updated })
    .eq("id", id)
    .eq("user_id", user.id)
    .select()
    .single();

  if (uErr) return res.status(400).json({ error: uErr.message });
  res.json({ run: saved });
});

// -------- Main Run Endpoint --------

app.post("/api/run", upload.single("file"), async (req, res) => {
  const user = await getUserFromReq(req);
  if (!user) return res.status(401).json({ error: "Unauthorized" });

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

      if (cErr) throw new Error(cErr.message);
      finalEntityId = created.id;
    }

    // Transcribe
    const tr = await assemblyTranscribe(inputPath);
    const transcriptText = tr.text || "";
    const transcriptHash = sha1(Buffer.from(transcriptText, "utf-8"));

    // Optional aggregate context for entity
    let entityAggregate = null;
    if (finalEntityId) {
      entityAggregate = await buildEntityAggregate(user.id, finalEntityId);
    }

    // Analyze
    const prompt = buildAnalysisPrompt({
      transcript: transcriptText,
      context,
      scenario,
      entityAggregate,
    });

    const analysisJson = await deepseekJSON(prompt.system, prompt.user, 0.25);
    const outcomeLabel = await inferOutcomeLabel(analysisJson);

    // Store run
    const runRow = {
      user_id: user.id,
      scenario,
      context_text: context,
      transcript_text: transcriptText,
      transcript_hash: transcriptHash,
      outcome_label: outcomeLabel,
      analysis_json: analysisJson,
      entity_id: finalEntityId,
    };

    const { data: saved, error: sErr } = await supabaseAdmin
      .from("runs")
      .insert([runRow])
      .select()
      .single();

    if (sErr) throw new Error(sErr.message);

    // cleanup temp files
    cleanupPaths.forEach((p) => {
      try { fs.unlinkSync(p); } catch {}
    });

    res.json({ run: saved });
  } catch (err) {
    try { fs.unlinkSync(file.path); } catch {}
    return res.status(400).json({ error: err.message || String(err) });
  }
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
