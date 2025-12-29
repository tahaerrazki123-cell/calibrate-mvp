// FILE: web/server.mjs
import express from "express";
import multer from "multer";
import path from "path";
import { fileURLToPath } from "url";

import { runCalibrate, ENFORCER_VERSION } from "../calibrate.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 }, // 50MB
});

app.use(express.json({ limit: "2mb" }));
app.use(express.urlencoded({ extended: true }));

// Serve static files from /web (index.html, css, etc.)
app.use(express.static(__dirname, { extensions: ["html"] }));

app.get("/health", (req, res) => res.json({ ok: true, enforcer: ENFORCER_VERSION }));

// Frontend config for Supabase client init
app.get("/api/config", (req, res) => {
  const supabaseUrl = process.env.SUPABASE_URL || "";
  const supabaseAnonKey = process.env.SUPABASE_ANON_KEY || "";
  res.json({ supabaseUrl, supabaseAnonKey, enforcer: ENFORCER_VERSION });
});

function prettifyTranscript(raw) {
  let t = (raw ?? "").toString().trim();
  if (!t) return "";

  // Normalize newlines
  t = t.replace(/\r\n/g, "\n");

  // Put labels on their own lines (Speaker A/B)
  t = t.replace(/\s*(Speaker\s*[AB]\s*:)\s*/gi, "\n$1 ");

  // Put common labels on their own lines when they appear as "Prospect." or "Prospect:"
  t = t.replace(/\s*(Prospect|Rep|Caller|Agent|Customer)\s*[:.]\s*/gi, "\n$1: ");

  // Also split when a label appears mid-line after punctuation: "... ? Prospect ..."
  t = t.replace(/([.!?])\s+(Prospect|Rep|Caller|Agent|Customer)\s+(?=[A-Za-z0-9])/gi, "$1\n$2: ");
  t = t.replace(/([.!?])\s+(Prospect|Rep|Caller|Agent|Customer)\s*[:.]\s*/gi, "$1\n$2: ");

  // Clean spaces WITHOUT destroying newlines
  t = t
    .replace(/[ \t]+/g, " ")
    .replace(/\n[ \t]+/g, "\n")
    .replace(/\n{2,}/g, "\n")
    .trim();

  let lines = t.split("\n").map((l) => l.trim()).filter(Boolean);

  // Convert Rep/Agent/Caller -> You, Prospect/Customer -> Prospect, keep Speaker A/B for now
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

  // Map Speaker A/B -> You/Prospect (heuristic scoring)
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
      // tie-breaker: if the very first speaker line starts with "Hey/Hi", it's likely the rep
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

  // Final cleanup for rare double-label artifacts
  lines = lines.map((l) =>
    l.replace(/^You:\s*Prospect:\s*/i, "Prospect: ").replace(/^Prospect:\s*You:\s*/i, "You: ")
  );

  return lines.join("\n");
}

/**
 * AssemblyAI transcription (polling).
 * Requires: ASSEMBLYAI_API_KEY in env.
 */
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

    return res.json({ transcript, ...result });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err?.message || String(err) });
  }
});

/**
 * SPA fallback:
 * Render/Express will 404 on /login and /home unless we serve index.html for those paths.
 * IMPORTANT: this must be AFTER /api routes.
 */
const INDEX = path.join(__dirname, "index.html");
app.get(["/", "/login", "/home", "/run/:id"], (req, res) => res.sendFile(INDEX));
app.get("*", (req, res) => res.sendFile(INDEX));

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
app.listen(PORT, () => {
  console.log(`Calibrate MVP running at http://localhost:${PORT}`);
  console.log(`Enforcer loaded: ${ENFORCER_VERSION}`);
});
