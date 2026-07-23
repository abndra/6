// ============================================================
// perf-settings-reader.js — يقرأ إعدادات الأداء من Supabase
// يعطي القيم الفعّالة لخادم واتساب وعامل الذكاء بدون إعادة نشر.
// ============================================================
const fetch = require("node-fetch");
const { readSupabaseConfig, cleanEnvValue } = require("./env");

const { url: SUPABASE_URL, key: SUPABASE_KEY } = readSupabaseConfig();

const DEFAULTS = {
  AI_POLL_INTERVAL_MS: 15000,
  OUTBOX_POLL_INTERVAL_MS: 15000,
  SNAPSHOT_POLL_INTERVAL_MS: 60000,
  SNAPSHOT_LISTENERS_ENABLED: false,
  AI_HEARTBEAT_WRITE_MS: 300000,
  OUTBOX_HEARTBEAT_WRITE_MS: 300000,
  CONNECTION_VERIFY_INTERVAL_MS: 60000,
  SUPABASE_CONFIG_CACHE_MS: 60000,
  MESSAGE_SWEEP_ENABLED: false,
  REMOTE_SESSION_BACKUP_MS: 300000,
  EVENT_LOG_ENABLED: false,
  AI_CONFIG_REFRESH_MS: 300000,
};

let cached = { ...DEFAULTS };
let lastFetchAt = 0;
const REFRESH_MS = 60000; // نحدّث القيم كل دقيقة على الأكثر

async function refresh() {
  if (!SUPABASE_URL || !SUPABASE_KEY) return cached;
  const now = Date.now();
  if (now - lastFetchAt < REFRESH_MS) return cached;
  lastFetchAt = now;
  try {
    const url = `${SUPABASE_URL}/rest/v1/documents?path=eq.runtime/perfSettings&select=data`;
    const headers = { apikey: SUPABASE_KEY, Accept: "application/json" };
    const serviceToken = cleanEnvValue("SERVICE_TOKEN");
    if (serviceToken) headers["x-service-token"] = serviceToken;
    const res = await fetch(url, {
      headers,
    });
    if (!res.ok) return cached;
    const rows = await res.json();
    const data = Array.isArray(rows) && rows[0]?.data ? rows[0].data : null;
    if (data && typeof data === "object") {
      cached = { ...DEFAULTS, ...cached, ...data };
    }
  } catch (_) {
    // نستخدم آخر قيم معروفة عند فشل الشبكة
  }
  return cached;
}

// تحديث تلقائي في الخلفية
setInterval(() => {
  refresh().catch(() => {});
}, REFRESH_MS).unref?.();
refresh().catch(() => {});

function getPerf(key, fallback) {
  const envRaw = process.env[key];
  if (envRaw !== undefined && envRaw !== "") {
    if (typeof DEFAULTS[key] === "boolean") return String(envRaw).toLowerCase() === "true";
    const n = Number(envRaw);
    if (!Number.isNaN(n)) return n;
  }
  if (cached && key in cached) return cached[key];
  return fallback !== undefined ? fallback : DEFAULTS[key];
}

module.exports = { getPerf, refresh, DEFAULTS };
