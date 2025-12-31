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

// Parse JSON bodies for non-upload endpoints
app.use(express.json({ limit: "1mb" }));

// Static UI
app.use(express.static(__dirname));

// Upload handling
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 60 * 1024 * 1024 } });

// Supabase
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.warn("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY. Server will fail DB operations.");
}
const supabase = createClient(SUPABASE_URL || "", SUPABASE_SERVICE_ROLE_KEY || "");

// Helpers
function normalizeOutcomeKey(outcome) {
  const t = String(outcome || "").toLowerCase();
  if (t.includes("pass")) return "pass";
  if (t.includes("book")) return "booked";
  if (t.includes("meeting")) return "booked";
  if (t.includes("follow")) return "followup";
  if (t.includes("no_show")) return "no_show";
  if (t.includes("objection")) return "objection";
  if (t.includes("fail")) return "fail";
  return t || "unknown";
}

// Simple in-memory anti-duplicate (optional)
const activeRunByUser = new Map(); // key: ip, value: timestamp

// API: run analysis
app.post("/api/run", upload.single("audio"), async (req, res) => {
  try {
    const ip = req.ip || "unknown";
    const now = Date.now();
    const last = activeRunByUser.get(ip);
    if (last && now - last < 1500) {
      return res.status(429).send("Too many requests. Try again in a moment.");
    }
    activeRunByUser.set(ip, now);

    const file = req.file;
    const context = String(req.body?.context || "").trim();
    const scenario = String(req.body?.scenario || "none").trim() || "none";
    const legacyScenario = String(req.body?.legacy_scenario || "none").trim() || "none";

    if (!file) return res.status(400).send("Missing audio file.");
    if (!context) return res.status(400).send("Missing context.");

    // Generate a title from filename
    const title = (file.originalname || "Call").replace(/\.[^.]+$/, "");

    // Run calibrate pipeline
    const result = await runCalibrate({
      audioBuffer: file.buffer,
      filename: file.originalname,
      context,
      scenario,
      legacyScenario,
    });

    // Expect result fields:
    // transcript_text, report_text, call_outcome, call_outcome_reason, call_outcome_evidence,
    // scenario_mismatch, mismatch_reason, speaker_map, word_count, etc.
    const runRow = {
      title,
      scenario,
      legacy_scenario: legacyScenario,
      context,
      transcript_text: result.transcript_text || "",
      report_text: result.report_text || "",
      call_outcome: result.call_outcome || "unknown",
      call_outcome_reason: result.call_outcome_reason || "",
      call_outcome_evidence: result.call_outcome_evidence || "",
      scenario_mismatch: !!result.scenario_mismatch,
      mismatch_reason: result.mismatch_reason || "",
      speaker_map_json: result.speaker_map_json || null,
      enforcer_version: ENFORCER_VERSION || result.enforcer_version || null,
      word_count: Number.isFinite(result.word_count) ? result.word_count : null,
      target_words: Number.isFinite(result.target_words) ? result.target_words : null,
      capped_words: Number.isFinite(result.capped_words) ? result.capped_words : null,
      meta_json: result.meta_json || null,
    };

    const { data, error } = await supabase
      .from("call_runs")
      .insert(runRow)
      .select("id")
      .single();

    if (error) {
      console.error("Supabase insert error:", error);
      return res.status(500).send("DB insert failed.");
    }

    return res.json({ ok: true, run_id: data.id });
  } catch (e) {
    console.error(e);
    return res.status(500).send(e?.message || "Run failed.");
  } finally {
    try {
      const ip = req.ip || "unknown";
      activeRunByUser.delete(ip);
    } catch {}
  }
});

// API: list runs
app.get("/api/runs", async (_req, res) => {
  try {
    const { data, error } = await supabase
      .from("call_runs")
      .select("id, created_at, title, scenario, legacy_scenario, call_outcome, call_outcome_reason")
      .order("created_at", { ascending: false })
      .limit(50);

    if (error) {
      console.error(error);
      return res.status(500).send("DB read failed.");
    }

    return res.json({
      runs: (data || []).map((r) => ({
        id: r.id,
        created_at: r.created_at,
        title: r.title,
        scenario: r.scenario,
        legacy_scenario: r.legacy_scenario,
        call_outcome: r.call_outcome,
        call_outcome_reason: r.call_outcome_reason || "",
      })),
    });
  } catch (e) {
    console.error(e);
    return res.status(500).send("DB read failed.");
  }
});

// API: get one run
app.get("/api/runs/:id", async (req, res) => {
  try {
    const id = req.params.id;

    const { data, error } = await supabase
      .from("call_runs")
      .select("*")
      .eq("id", id)
      .single();

    if (error) {
      console.error(error);
      return res.status(404).send("Run not found.");
    }

    // normalize outcome on server too
    data.call_outcome = normalizeOutcomeKey(data.call_outcome);

    return res.json({ run: data });
  } catch (e) {
    console.error(e);
    return res.status(500).send("DB read failed.");
  }
});

// ----------------- PLAYBOOK -----------------

// list scripts
app.get("/api/playbook/scripts", async (_req, res) => {
  try {
    const { data, error } = await supabase
      .from("playbook_scripts")
      .select("id, title, updated_at, version_count")
      .order("updated_at", { ascending: false })
      .limit(200);

    if (error) {
      console.error(error);
      return res.status(500).send("DB read failed.");
    }

    return res.json({ scripts: data || [] });
  } catch (e) {
    console.error(e);
    return res.status(500).send("DB read failed.");
  }
});

// create a new script (creates version 1)
app.post("/api/playbook/scripts", async (req, res) => {
  try {
    const { title, text, source_run_id, notes } = req.body || {};
    const t = String(title || "").trim();
    const body = String(text || "").trim();

    if (!t) return res.status(400).send("Missing title.");
    if (!body) return res.status(400).send("Missing text.");

    // enforce max 5 words in title
    const safeTitle = t.split(/\s+/).filter(Boolean).slice(0, 5).join(" ");

    // create script row
    const { data: script, error: e1 } = await supabase
      .from("playbook_scripts")
      .insert({
        title: safeTitle,
        current_text: body,
        version_count: 1,
        source_run_id: source_run_id || null,
      })
      .select("id")
      .single();

    if (e1) {
      console.error(e1);
      return res.status(500).send("DB insert failed.");
    }

    // create version 1
    const initialNotes = String(notes || "").trim() || "Initial version";
    const { error: e2 } = await supabase.from("playbook_versions").insert({
      script_id: script.id,
      version_number: 1,
      text: body,
      notes: initialNotes,
    });

    if (e2) {
      console.error(e2);
      return res.status(500).send("DB insert failed (version).");
    }

    return res.json({ ok: true, script_id: script.id });
  } catch (e) {
    console.error(e);
    return res.status(500).send("DB insert failed.");
  }
});

// read one script + versions
app.get("/api/playbook/scripts/:id", async (req, res) => {
  try {
    const id = req.params.id;

    const { data: script, error: e1 } = await supabase
      .from("playbook_scripts")
      .select("*")
      .eq("id", id)
      .single();

    if (e1) {
      console.error(e1);
      return res.status(404).send("Not found.");
    }

    const { data: versions, error: e2 } = await supabase
      .from("playbook_versions")
      .select("*")
      .eq("script_id", id)
      .order("version_number", { ascending: false });

    if (e2) {
      console.error(e2);
      return res.status(500).send("DB read failed.");
    }

    return res.json({ script, versions: versions || [] });
  } catch (e) {
    console.error(e);
    return res.status(500).send("DB read failed.");
  }
});

// add new version
app.post("/api/playbook/scripts/:id/versions", async (req, res) => {
  try {
    const id = req.params.id;
    const text = String(req.body?.text || "").trim();
    const notes = String(req.body?.notes || "").trim();

    if (!text) return res.status(400).send("Missing text.");

    // find next version number
    const { data: script, error: e1 } = await supabase
      .from("playbook_scripts")
      .select("version_count")
      .eq("id", id)
      .single();

    if (e1) {
      console.error(e1);
      return res.status(404).send("Not found.");
    }

    const nextVer = (script.version_count || 0) + 1;

    const { error: e2 } = await supabase.from("playbook_versions").insert({
      script_id: id,
      version_number: nextVer,
      text,
      notes: notes || null,
    });

    if (e2) {
      console.error(e2);
      return res.status(500).send("DB insert failed.");
    }

    const { error: e3 } = await supabase
      .from("playbook_scripts")
      .update({ current_text: text, version_count: nextVer })
      .eq("id", id);

    if (e3) {
      console.error(e3);
      return res.status(500).send("DB update failed.");
    }

    return res.json({ ok: true, version_number: nextVer });
  } catch (e) {
    console.error(e);
    return res.status(500).send("DB insert failed.");
  }
});

// fallback: serve SPA
app.get("*", (_req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

// Start
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Calibrate web server listening on :${PORT}`);
});
