// ============================================================
// env.js — قراءة متغيرات Railway بأمان + فحص اتصال Supabase
// ============================================================
// يعالج أخطاء النسخ الشائعة في Railway مثل لصق السطر كاملاً:
//   APP_BACKEND_PUBLISHABLE_KEY=sb_publishable_...
// بدلاً من لصق القيمة فقط.
// ولا يطبع أي مفتاح في اللوجات.
// ============================================================

const fetchImpl = globalThis.fetch || require("node-fetch");

// نفس قيم مشروع تيسير الظاهرة في لوحة الإدارة. مفتاح publishable عام وليس سرياً.
// تُستخدم كخطة إنقاذ إذا كانت Railway Variables مفقودة أو ملصوقة بشكل خاطئ.
const DEFAULT_SUPABASE_URL = "https://exnujjebqhqrabrqfkhk.supabase.co";
const DEFAULT_SUPABASE_PUBLISHABLE_KEY = "sb_publishable_uEWDwOjtjsSFzKmYIVfItg_fqPp-iWT";

function stripWrappingQuotes(value) {
  let out = String(value || "").trim();
  for (let i = 0; i < 3; i += 1) {
    if ((out.startsWith('"') && out.endsWith('"')) || (out.startsWith("'") && out.endsWith("'"))) {
      out = out.slice(1, -1).trim();
      continue;
    }
    break;
  }
  return out;
}

function cleanEnvValue(name) {
  const rawValue = process.env[name];
  if (rawValue === undefined || rawValue === null) return "";

  let raw = String(rawValue).trim();
  if (!raw) return "";

  const lines = raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  const exactLine = lines.find((line) => new RegExp(`^(?:export\\s+)?${name}\\s*=`).test(line));
  raw = exactLine || lines[0] || raw;

  raw = raw.replace(/^export\s+/i, "").trim();
  const assignment = raw.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([\s\S]*)$/);
  if (assignment) raw = assignment[2];

  return stripWrappingQuotes(raw);
}

function firstEnv(...names) {
  for (const name of names) {
    const value = cleanEnvValue(name);
    if (value) return { name, value };
  }
  return { name: "", value: "" };
}

function looksLikeSupabaseKey(value) {
  const key = String(value || "").trim();
  return key.startsWith("sb_publishable_") || key.startsWith("sb_secret_") || key.startsWith("eyJ");
}

function readSupabaseUrl() {
  const selected = cleanEnvValue("__TAYSIR_SELECTED_SUPABASE_URL");
  if (selected) return { name: cleanEnvValue("__TAYSIR_SELECTED_SUPABASE_URL_NAME") || "selected Supabase URL", value: selected.replace(/\/+$/, "") };
  const source = firstEnv("APP_BACKEND_URL", "APP_SUPABASE_URL", "SUPABASE_URL");
  if (!source.value) return { name: "built-in Taysir Supabase URL", value: DEFAULT_SUPABASE_URL };
  return { ...source, value: source.value.replace(/\/+$/, "") };
}

function getSupabaseUrlCandidates() {
  const names = ["APP_BACKEND_URL", "APP_SUPABASE_URL", "SUPABASE_URL"];
  const seen = new Set();
  const out = [];
  for (const name of names) {
    const value = cleanEnvValue(name).replace(/\/+$/, "");
    if (!value || seen.has(value)) continue;
    seen.add(value);
    out.push({ name, value });
  }
  if (!seen.has(DEFAULT_SUPABASE_URL)) out.push({ name: "built-in Taysir Supabase URL", value: DEFAULT_SUPABASE_URL });
  return out;
}

function getSupabaseKeyCandidates() {
  const names = [
    // إن وُجد مفتاح service role صحيح نستخدمه أولاً لأن خادم Railway موثوق ومغلق بـ SERVICE_TOKEN.
    "APP_SUPABASE_SECRET_KEY",
    "SUPABASE_SERVICE_ROLE_KEY",
    "SUPABASE_SECRET_KEY",
    // النسخة الآمنة الافتراضية: publishable key + x-service-token عبر RLS.
    "APP_BACKEND_PUBLISHABLE_KEY",
    "APP_SUPABASE_PUBLISHABLE_KEY",
    "SUPABASE_PUBLISHABLE_KEY",
    "SUPABASE_ANON_KEY",
  ];
  const seen = new Set();
  const out = [];
  for (const name of names) {
    const value = cleanEnvValue(name);
    if (!value || seen.has(value)) continue;
    seen.add(value);
    out.push({ name, value });
  }
  if (!seen.has(DEFAULT_SUPABASE_PUBLISHABLE_KEY)) {
    out.push({ name: "built-in Taysir publishable key", value: DEFAULT_SUPABASE_PUBLISHABLE_KEY });
  }
  return out;
}

function readSupabaseConfig() {
  const url = readSupabaseUrl();
  const selectedKey = cleanEnvValue("__TAYSIR_SELECTED_SUPABASE_KEY");
  const selectedName = cleanEnvValue("__TAYSIR_SELECTED_SUPABASE_KEY_NAME") || "selected Supabase key";
  const key = selectedKey ? { name: selectedName, value: selectedKey } : getSupabaseKeyCandidates()[0] || { name: "", value: "" };
  return { url: url.value, urlName: url.name, key: key.value, keyName: key.name };
}

function createSupabaseFetch(supabaseKey) {
  return (input, init) => {
    const headers = new Headers((init && init.headers) || undefined);
    if (
      (supabaseKey.startsWith("sb_publishable_") || supabaseKey.startsWith("sb_secret_")) &&
      headers.get("Authorization") === `Bearer ${supabaseKey}`
    ) {
      headers.delete("Authorization");
    }
    headers.set("apikey", supabaseKey);
    const serviceToken = cleanEnvValue("SERVICE_TOKEN");
    if (serviceToken) headers.set("x-service-token", serviceToken);
    return fetch(input, { ...init, headers });
  };
}

async function validateSupabaseConnection() {
  const urls = getSupabaseUrlCandidates();
  const candidates = getSupabaseKeyCandidates();
  if (!candidates.length) throw new Error("APP_BACKEND_PUBLISHABLE_KEY غير مضبوط.");

  const failures = [];
  for (const url of urls) {
    if (!/^https:\/\//i.test(url.value)) {
      failures.push(`${url.name}: الرابط يجب أن يبدأ بـ https:// ويشير إلى Supabase`);
      continue;
    }
    let parsed;
    try {
      parsed = new URL(url.value);
    } catch (_) {
      failures.push(`${url.name}: الرابط غير صالح`);
      continue;
    }
    if (!/\.supabase\.co$/i.test(parsed.host)) {
      failures.push(`${url.name}: هذا ليس رابط Supabase (${url.value.replace(/^https?:\/\//, "")})`);
      continue;
    }
    for (const candidate of candidates) {
    if (!looksLikeSupabaseKey(candidate.value)) {
      failures.push(`${candidate.name}: صيغة المفتاح غير صحيحة`);
      continue;
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 8000);
    try {
      const res = await fetchImpl(`${url.value}/rest/v1/`, {
        signal: controller.signal,
        headers: { apikey: candidate.value, Accept: "application/json" },
      });
      if (res.ok) {
        process.env.__TAYSIR_SELECTED_SUPABASE_URL = url.value;
        process.env.__TAYSIR_SELECTED_SUPABASE_URL_NAME = url.name;
        process.env.__TAYSIR_SELECTED_SUPABASE_KEY = candidate.value;
        process.env.__TAYSIR_SELECTED_SUPABASE_KEY_NAME = candidate.name;
        return { ok: true, urlName: url.name, keyName: candidate.name };
      }
      let body = "";
      try {
        body = (await res.text()).slice(0, 300);
      } catch (_) {}
      failures.push(`${candidate.name}: HTTP ${res.status}${body ? ` ${body}` : ""}`);
    } catch (e) {
      failures.push(`${candidate.name}: ${e.name === "AbortError" ? "timeout" : e.message}`);
    } finally {
      clearTimeout(timer);
    }
    }
  }

  throw new Error(
    [
      "Invalid Supabase API key.",
      "السبب الحقيقي: مفتاح Supabase الموجود في Railway غير صالح أو من مشروع آخر.",
      "افتح Railway → Variables واستبدل APP_BACKEND_PUBLISHABLE_KEY بالقيمة فقط مثل sb_publishable_... بدون اسم المتغير وبدون علامات اقتباس.",
      `تفاصيل الفحص: ${failures.join(" | ")}`,
    ].join(" "),
  );
}

module.exports = {
  cleanEnvValue,
  firstEnv,
  looksLikeSupabaseKey,
  readSupabaseConfig,
  createSupabaseFetch,
  validateSupabaseConnection,
};