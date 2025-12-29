// FILE: calibrate.mjs
// Calibrate MVP - Context-first enforcer + report generator

export const ENFORCER_VERSION = "ENFORCER_V9_2025-12-27";

// -------------------------
// Utilities
// -------------------------
function clampStr(s, max = 1200) {
  const t = (s ?? "").toString();
  return t.length > max ? t.slice(0, max) + "…" : t;
}
function wordCount(s) {
  const t = (s ?? "").trim();
  if (!t) return 0;
  return t.split(/\s+/).filter(Boolean).length;
}
function sliceWords(s, n) {
  const words = (s ?? "").trim().split(/\s+/).filter(Boolean);
  if (words.length <= n) return (s ?? "").trim();
  return words.slice(0, n).join(" ").replace(/\s+([,.!?;:])/g, "$1").trim();
}
function normalize(s) { return (s ?? "").toLowerCase(); }
function hasAny(text, patterns) { return patterns.some((p) => p.test(text ?? "")); }

// -------------------------
// Template-derived nudges
// -------------------------
function templateDerived(category) {
  const c = (category || "").trim();

  if (c === "LOCAL_SERVICE") {
    return {
      title: "Local service → local business",
      notes: "Use lead/call language. Anchor to service area, missed calls, maps ranking, reviews, and simple next steps (text an example, quick audit). Avoid ecom/CAC jargon.",
      example_phrases: [
        "service area pages", "calls/leads", "Google Maps", "reviews", "estimate requests"
      ]
    };
  }
  if (c === "B2B_SERVICE") {
    return {
      title: "B2B service / software",
      notes: "Use ROI/time-saved/pipeline language. Focus on qualifying decision-maker, current tool/process, measurable outcome, and low-friction next step (10-min walkthrough).",
      example_phrases: [
        "pipeline", "ROI", "time saved", "team workflow", "demo"
      ]
    };
  }
  if (c === "ECOM_MARKETING") {
    return {
      title: "Ecommerce / marketing",
      notes: "Use CAC/ROAS/product/collection page language. Tie to revenue, buyer keywords, technical fixes, internal linking, and proof-based audit.",
      example_phrases: [
        "CAC", "ROAS", "collection pages", "product pages", "buyer keywords"
      ]
    };
  }
  if (c === "HOME_SERVICES") {
    return {
      title: "Home services",
      notes: "Use homeowner-intent keywords, emergency searches, service areas, and calls booked. Avoid ecom framing unless transcript indicates ecom.",
      example_phrases: [
        "emergency repair", "service areas", "calls booked", "local keywords"
      ]
    };
  }
  if (c === "APPT_BASED") {
    return {
      title: "Appointment-based business",
      notes: "Use appointment/booking language only when relevant. Focus on bookings, no-shows, conversion from mobile, and frictionless scheduling.",
      example_phrases: [
        "appointments", "booking link", "schedule online", "no-shows"
      ]
    };
  }
  if (c === "OTHER") {
    return {
      title: "Other",
      notes: "Keep examples generic: permission opener, value anchor, one proof point, one low-commitment next step.",
      example_phrases: ["quick question", "proof", "audit", "10 minutes"]
    };
  }
  return {
    title: "Auto (no template)",
    notes: "(none)",
    example_phrases: []
  };
}

// -------------------------
// Call outcome detection (robust for 1-line transcripts)
// -------------------------
function detectCallOutcome(transcriptRaw) {
  const raw = transcriptRaw ?? "";
  const wc = wordCount(raw);

  const voicemailHints = [
    /\bvoicemail\b/i,
    /\bleave (a )?message\b/i,
    /\bafter the tone\b/i,
    /\bhas been forwarded\b/i,
    /\bplease record\b/i,
    /\bmailbox\b/i,
    /\bnot available\b/i,
    /\bthe person you are trying to reach\b/i,
  ];
  if (hasAny(raw, voicemailHints)) {
    return { call_outcome: "VOICEMAIL", reason: "Voicemail indicators present in transcript." };
  }

  const hostileHints = [
    /\bget out\b/i,
    /\bget away\b/i,
    /\bstop calling\b/i,
    /\bdo(?:n|')t call\b/i,
    /\bfuck\b/i,
    /\bbitch\b/i,
    /\basshole\b/i,
  ];
  if (hasAny(raw, hostileHints)) {
    return { call_outcome: "HOSTILE", reason: "Hostility indicators present." };
  }

  const firstChunk = raw.split(/\s+/).slice(0, 35).join(" ").toLowerCase();
  const earlyExitHints = [
    "not interested",
    "no thanks",
    "goodbye",
    "hang up",
    "wrong number",
    "don't want to talk",
    "don’t want to talk",
    "busy",
    "stop",
  ];
  if (earlyExitHints.some((k) => firstChunk.includes(k))) {
    return { call_outcome: "EARLY_EXIT", reason: "Immediate rejection detected in opening words." };
  }

  // If transcript has explicit "You:"/"Prospect:" lines, use that
  const lineCount = (raw.split("\n").map(l => l.trim()).filter(Boolean)).length;
  const labeledLines = (raw.split("\n").filter(l => /^You:\s|^Prospect:\s|^Speaker\s*[AB]\s*:/i.test(l.trim()))).length;
  if (labeledLines >= 3) {
    return { call_outcome: "CONNECTED", reason: "Multiple labeled turns detected." };
  }

  // Fallback: dialogue markers + length
  const dialogMarkers = (raw.match(/\b(Prospect|Rep|Caller|Agent|Speaker\s*[AB])\b/gi) || []).length;
  if (dialogMarkers >= 2 && wc >= 25) {
    return { call_outcome: "CONNECTED", reason: "Dialogue markers detected (multi-party conversation)." };
  }

  // Longer Q/A tends to mean connected
  const qMarks = (raw.match(/\?/g) || []).length;
  if (wc >= 60 && qMarks >= 2) {
    return { call_outcome: "CONNECTED", reason: "Length + Q/A structure suggests a connected call." };
  }

  // If there are many lines but not labeled, still likely connected
  if (lineCount >= 6 && wc >= 60) {
    return { call_outcome: "CONNECTED", reason: "Multi-line long transcript suggests a connected call." };
  }

  return { call_outcome: "UNCLEAR", reason: "Not enough signal to classify outcome." };
}

// -------------------------
// Inference from transcript (conservative)
// - booking only triggers for appointment/booking-link language
// - ecommerce vs appointment-based distinction
// -------------------------
function inferFromTranscript(transcriptRaw) {
  const raw = transcriptRaw ?? "";

  const ecommerceSignals = [
    /\bshopify\b/i,
    /\bwoocommerce\b/i,
    /\bproduct pages?\b/i,
    /\bcollection pages?\b/i,
    /\badd to cart\b/i,
    /\bcheckout\b/i,
    /\bcart\b/i,
  ];

  const apptSignals = [
    /\bappointments?\b/i,
    /\bbook(ing)?\s+(an\s+)?appointment\b/i,
    /\bschedule\s+(an\s+)?appointment\b/i,
    /\bbook\s+online\b/i,
    /\bonline booking\b/i,
    /\bbooking link\b/i,
    /\breserve\s+an?\s+(appointment|spot|time)\b/i,
  ];

  const ecommerceScore = ecommerceSignals.filter((re) => re.test(raw)).length;
  const apptScore = apptSignals.filter((re) => re.test(raw)).length;

  const prospectPatterns = [
    { re: /\b(barber|barbershop)\b/i, v: "barbershop" },
    { re: /\b(dental|dentist|orthodont\w*)\b/i, v: "dental office" },
    { re: /\b(roof|roofing|roofer|roofers)\b/i, v: "roofing company" },
    { re: /\b(restaurant|diner|cafe|coffee\s+shop)\b/i, v: "restaurant / cafe" },
    { re: /\b(gym|fitness)\b/i, v: "gym / fitness business" },
    { re: /\b(real\s+estate|realtor|brokerage)\b/i, v: "real estate" },
  ];

  let prospect_type = "";
  if (ecommerceScore >= 1 && ecommerceScore > apptScore) prospect_type = "ecommerce business";
  else if (apptScore >= 1 && apptScore > ecommerceScore) prospect_type = "appointment-based business";
  else {
    for (const p of prospectPatterns) {
      if (p.re.test(raw)) { prospect_type = p.v; break; }
    }
    if (!prospect_type && ecommerceScore >= 1) prospect_type = "ecommerce business";
    if (!prospect_type && apptScore >= 1) prospect_type = "appointment-based business";
  }

  const bookingRegex = new RegExp(apptSignals.map(r => r.source).join("|"), "i");
  const offerPatterns = [
    { key: "website", re: /\b(website|web\s*site|web\s*design|landing\s*page|site\s*rebuild)\b/i },
    { key: "seo", re: /\bseo\b/i },
    { key: "marketing", re: /\b(marketing|google\s*ads|facebook\s*ads|ads)\b/i },
    { key: "ai_receptionist", re: /\b(ai\s*receptionist|receptionist|front\s*desk|answer\s*calls|phone\s*calls)\b/i },
    { key: "booking", re: bookingRegex },
    { key: "software", re: /\b(software|saas|platform|integration|crm)\b/i },
  ];

  const offer_hits = offerPatterns.filter((x) => x.re.test(raw)).map((x) => x.key);

  let location = "";
  const loc1 = raw.match(/\b(in)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\b/);
  const loc2 = raw.match(
    /\b(in)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*(Florida|Texas|California|New York|Virginia|Georgia|North Carolina|South Carolina|Illinois|Ohio|Pennsylvania)\b/
  );
  if (loc1) location = `${loc1[2]}, ${loc1[3]}`;
  else if (loc2) location = `${loc2[2]}, ${loc2[3]}`;

  let prospect_name = "";
  const nameMatch = raw.match(/\b(at|with)\s+([A-Z][A-Za-z0-9&' -]{2,45})(?=[?.!,\n]|$)/);
  if (nameMatch) {
    const cand = nameMatch[2].trim();
    if (cand.length >= 3 && cand.length <= 45) prospect_name = cand;
  }

  return { prospect_type, prospect_name, location, offer_hits };
}

function inferOfferFromText(text) {
  const t = normalize(text);
  const hits = [];
  if (/\bseo\b/i.test(text)) hits.push("seo");
  if (t.includes("website") || t.includes("web design") || t.includes("landing page")) hits.push("website");
  if (/\bads\b/i.test(text) || t.includes("marketing")) hits.push("marketing");
  if (t.includes("ai receptionist") || t.includes("answer calls") || t.includes("phone")) hits.push("ai_receptionist");
  if (t.includes("software") || t.includes("saas")) hits.push("software");
  if (/\bappointments?\b/i.test(text) || /\bbooking link\b/i.test(text)) hits.push("booking");
  return hits;
}

function computeContextConflict(userContext, inferred, transcriptRaw) {
  const userHits = inferOfferFromText(userContext);
  const trHits = inferred.offer_hits || [];
  if (!userHits.length || !trHits.length) return "";

  const userWebsiteish = userHits.includes("website") || userHits.includes("seo") || userHits.includes("marketing");
  const trReception = trHits.includes("ai_receptionist");
  const trWebsiteish = trHits.includes("website") || trHits.includes("seo") || trHits.includes("marketing");

  if (userWebsiteish && trReception && !trWebsiteish) {
    return "⚠ CONTEXT CONFLICT: Your context is website/marketing, but transcript suggests AI receptionist / phone-handling. Rewrite the context in one sentence if needed.";
  }
  const userReception = userHits.includes("ai_receptionist");
  if (userReception && trWebsiteish && !trReception) {
    return "⚠ CONTEXT CONFLICT: Your context suggests AI receptionist, but transcript looks like website/marketing. Rewrite the context in one sentence if needed.";
  }
  return "";
}

function computeMissingInfo(userContext, inferred) {
  const missing = [];
  const offerFromUser = inferOfferFromText(userContext);
  const offerFromTr = inferred.offer_hits || [];
  if (!offerFromUser.length && !offerFromTr.length) missing.push("What are you selling? (one sentence)");

  const userHasProspectHint = /barber|dent|roof|restaurant|gym|ecom|shopify|local business|company|shop|office/i.test(userContext);
  const trHasProspect = Boolean(inferred.prospect_type || inferred.prospect_name);
  if (!userHasProspectHint && !trHasProspect) missing.push("Who did you call? (industry in a few words)");

  return missing.slice(0, 2);
}

function turnCount(transcript) {
  const lines = (transcript ?? "").split("\n").map(l => l.trim()).filter(Boolean);
  const labeled = lines.filter(l => /^(You:|Prospect:|Speaker\s*[AB]\s*:)/i.test(l)).length;
  return labeled || lines.length || 0;
}

function inferredLines(inferred) {
  const out = [];
  if (inferred.prospect_name) out.push(`Prospect name: ${inferred.prospect_name}`);
  if (inferred.prospect_type) out.push(`Prospect type: ${inferred.prospect_type}`);
  if (inferred.location) out.push(`Location: ${inferred.location}`);
  if (inferred.offer_hits?.length) out.push(`Offer keywords: ${inferred.offer_hits.join(", ")}`);
  return out;
}

// -------------------------
// LLM call (OpenAI-compatible)
// -------------------------
async function chatCompletion({ messages, temperature = 0.2 }) {
  const openaiKey = process.env.OPENAI_API_KEY;
  const deepseekKey = process.env.DEEPSEEK_API_KEY;
  const apiKey = openaiKey || deepseekKey;
  if (!apiKey) throw new Error("Missing OPENAI_API_KEY or DEEPSEEK_API_KEY in environment.");

  const baseUrl =
    process.env.OPENAI_BASE_URL ||
    process.env.DEEPSEEK_BASE_URL ||
    (openaiKey ? "https://api.openai.com/v1" : "https://api.deepseek.com/v1");

  const model =
    process.env.OPENAI_MODEL ||
    process.env.DEEPSEEK_MODEL ||
    (openaiKey ? "gpt-4o-mini" : "deepseek-chat");

  const resp = await fetch(`${baseUrl}/chat/completions`, {
    method: "POST",
    headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
    body: JSON.stringify({ model, messages, temperature }),
  });

  if (!resp.ok) {
    const t = await resp.text().catch(() => "");
    throw new Error(`LLM request failed: ${t}`);
  }

  const json = await resp.json();
  const content = json?.choices?.[0]?.message?.content;
  if (!content) throw new Error("LLM returned empty content.");
  return content;
}

// -------------------------
// Section 5 enforcement
// -------------------------
function enforceScript(script) {
  let s = (script ?? "").trim();
  if (wordCount(s) > 90) s = sliceWords(s, 90);
  if (!/[?.!]$/.test(s)) s += "?";
  const finalWc = wordCount(s);
  return { script: s, script_words: finalWc, section5: finalWc > 0 && finalWc <= 90 ? "PASS" : "FAIL" };
}

// -------------------------
// Main exported function
// -------------------------
export async function runCalibrate({ transcript, userContext, category = "", legacyScenario = "" }) {
  const tpl = templateDerived(category);
  const inferred = inferFromTranscript(transcript);
  const outcome = detectCallOutcome(transcript);

  const conflict = computeContextConflict(userContext, inferred, transcript);
  const missing = computeMissingInfo(userContext, inferred);
  const turns = turnCount(transcript);

  let call_outcome_banner = "";
  if (outcome.call_outcome === "VOICEMAIL") call_outcome_banner = "📞 VOICEMAIL: Coaching focuses on voicemail structure and first 10 seconds.";
  else if (outcome.call_outcome === "EARLY_EXIT") call_outcome_banner = "⚠ EARLY EXIT: Coaching focuses on opener/frame control (not deep discovery/close).";
  else if (outcome.call_outcome === "HOSTILE") call_outcome_banner = "⚠ HOSTILE: Coaching focuses on de-escalation + permission + clean exit.";
  else if (outcome.call_outcome === "UNCLEAR") call_outcome_banner = "⚠ OUTCOME UNCLEAR: Transcript is short/ambiguous. Coaching will be more generic.";

  const system = [
    "You are Calibrate, a brutally practical cold-call coach.",
    "You generate a coaching report and a compliant 45-second script.",
    "Do NOT ask the user questions in the report. If info is missing, write generic but actionable coaching.",
    "Context + transcript are the truth. The template only nudges examples/wording; never contradict transcript.",
    "If call outcome is VOICEMAIL / EARLY_EXIT / HOSTILE, explicitly state scoring is limited and why.",
    "Keep the 45-second script under 90 words.",
    "",
    "Scenario template (nudge):",
    `- Title: ${tpl.title}`,
    `- Notes: ${tpl.notes}`,
    tpl.example_phrases.length ? `- Example phrases: ${tpl.example_phrases.join(", ")}` : "",
    "",
    "Output format (markdown, exactly these sections):",
    "## 0) Context Check",
    "## 1) Scorecard (0-10)",
    "## 2) What To Fix First (Top 3)",
    "## 3) Best Objection Responses (word-for-word)",
    "## 4) Rewrite Pack (10 lines)",
    "## 5) 45-Second Script (single best version)"
  ].filter(Boolean).join("\n");

  const user = [
    "User context:",
    clampStr(userContext, 600),
    "",
    "Inferred from transcript:",
    inferredLines(inferred).length ? inferredLines(inferred).map(x => `- ${x}`).join("\n") : "- (none)",
    "",
    `Call outcome: ${outcome.call_outcome} (${outcome.reason})`,
    "",
    "Transcript:",
    clampStr(transcript, 7000)
  ].join("\n");

  const report = await chatCompletion({
    messages: [
      { role: "system", content: system },
      { role: "user", content: user }
    ],
    temperature: 0.2
  });

  let script = "";
  const m = report.match(/##\s*5\)\s*45-Second Script[\s\S]*?\n([\s\S]*)$/i);
  if (m && m[1]) script = m[1].trim().replace(/^[-*>\s]+/, "").trim();
  else {
    script = (await chatCompletion({
      messages: [
        { role: "system", content: "Write ONE 45-second cold call script under 90 words. No headings, no bullets." },
        { role: "user", content: user }
      ],
      temperature: 0.2
    })).trim();
  }

  const enforced = enforceScript(script);

  return {
    enforcer_version: ENFORCER_VERSION,

    // Summary fields for the UI
    scenario_template: category || "",
    user_context: userContext,
    inferred_lines: inferredLines(inferred),
    derived_from_template: tpl.notes === "(none)" ? "(none)" : `${tpl.title}: ${tpl.notes}`,
    turn_count: turns,

    // Banners and extras
    context_conflict_banner: conflict,
    missing_info: missing,
    call_outcome: outcome.call_outcome,
    call_outcome_reason: outcome.reason,
    call_outcome_banner,

    // Existing outputs
    report: report.trim(),
    script_words: enforced.script_words,
    section5: enforced.section5
  };
}
