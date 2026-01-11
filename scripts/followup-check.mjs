// scripts/followup-check.mjs
const baseUrl = process.env.BASE_URL || "http://127.0.0.1:3000";
const runId = process.env.RUN_ID;
const authToken = process.env.AUTH_TOKEN || ""; // Supabase access token

if (!runId) {
  console.error("FAIL missing RUN_ID");
  process.exit(1);
}

const headers = {};
if (authToken) headers["Authorization"] = `Bearer ${authToken}`;

const containsPlaceholders = (text) => {
  const t = String(text || "");
  return (
    /\[[^\]]+\]/.test(t) ||
    /\{[^}]+\}/.test(t) ||
    /\bTBD\b/i.test(t) ||
    /\blorem\b/i.test(t) ||
    /\bplaceholder\b/i.test(t)
  );
};

const looksLikeTranscript = (text) => {
  const t = String(text || "").trim();
  if (!t) return false;

  const linePrefix = /^(speaker|agent|prospect|rep|customer|caller)\s*[ab]?:/i;
  if (linePrefix.test(t)) return true;
  if (/\bSpeaker\s*[AB]:/i.test(t)) return true;

  const prefixed = t
    .split(/\r?\n/)
    .filter((l) => linePrefix.test(l.trim())).length;
  if (prefixed > 0) return true;

  const quoteCount = (t.match(/["“”]/g) || []).length;
  return quoteCount >= 4;
};

const containsNextStep = (text) =>
  /(tomorrow|thursday|next week|this week|schedule|calendar|time to|available|quick call|follow[- ]?up|chat|meet|15[- ]?min|10[- ]?min)/i.test(
    String(text || "")
  );

const includesEntityReference = (text, entityName) => {
  const name = String(entityName || "").toLowerCase();
  if (!name) return true;
  const tokens = name
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= 3 || t === "ai");
  if (!tokens.length) return true;
  const t = String(text || "").toLowerCase();
  return tokens.some((tok) => {
    if (tok.length >= 5) return t.includes(tok);
    return new RegExp(`\\b${tok.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\\\$&")}\\b`, "i").test(t);
  });
};

const containsPersonalLife = (text) => {
  const t = String(text || "");
  return (
    /\bmy (wife|husband|kid|kids|mom|dad|birthday|party)\b/i.test(t) ||
    /\bI'?ll let my (wife|husband)\b/i.test(t)
  );
};

async function main() {
  const res = await fetch(`${baseUrl}/api/runs/${runId}/regen_followup`, {
    method: "POST",
    headers,
  });

  const data = await res.json().catch(() => ({}));

  if (!res.ok) {
    console.error("FAIL", res.status, data?.error || "request failed");
    process.exit(1);
  }

  const text = data?.run?.analysis_json?.follow_up?.text || "";
  const entityName = data?.run?.entity_name || "";
  const entityId = data?.run?.entity_id || "";
  if (entityId && !entityName) {
    console.error("FAIL missing entity_name for entity run");
    console.log("FOLLOW_UP_TEXT:", text);
    process.exit(1);
  }
  if (!text) {
    console.error("FAIL empty follow_up");
    console.log("FOLLOW_UP_TEXT:", text);
    process.exit(1);
  }

  if (
    containsPlaceholders(text) ||
    looksLikeTranscript(text) ||
    !containsNextStep(text) ||
    !includesEntityReference(text, entityName) ||
    containsPersonalLife(text)
  ) {
    console.error("FAIL invalid follow_up");
    console.log("FOLLOW_UP_TEXT:", text);
    process.exit(1);
  }

  console.log("PASS follow_up");
  console.log("FOLLOW_UP_TEXT:", text);
}

main().catch((err) => {
  console.error("FAIL", err?.message || err);
  process.exit(1);
});
