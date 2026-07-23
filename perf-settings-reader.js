// ============================================================
// perf-settings-reader.js — يقرأ إعدادات الأداء من Supabase
// يعطي القيم الفعّالة لخادم واتساب وعامل الذكاء بدون إعادة نشر.
// ============================================================
const fetch = require("node-fetch");
const { readSupabaseConfig, cleanEnvValue } = require("./env");

const { url: SUPABASE_URL, key: SUPABASE_KEY } = readSupabaseConfig();

// نمط فوري ثابت: كل الفواصل الزمنية مضبوطة على أسرع قيمة عملية
// حتى تنعكس التغييرات في لوحة تحكم كل متجر مباشرةً بلا تأخير.
const DEFAULTS = {
  AI_POLL_INTERVAL_MS: 100,
  OUTBOX_POLL_INTERVAL_MS: 100,
  SNAPSHOT_POLL_INTERVAL_MS: 500,
  SNAPSHOT_LISTENERS_ENABLED: true,
  AI_HEARTBEAT_WRITE_MS: 60000,
  OUTBOX_HEARTBEAT_WRITE_MS: 60000,
  CONNECTION_VERIFY_INTERVAL_MS: 30000,
  SUPABASE_CONFIG_CACHE_MS: 500,
  MESSAGE_SWEEP_ENABLED: true,
  MESSAGE_SWEEP_INTERVAL_MS: 15000,
  MESSAGE_SWEEP_LIMIT: 8,
  MESSAGE_SWEEP_CHAT_LIMIT: 50,
  REMOTE_SESSION_BACKUP_MS: 300000,
  EVENT_LOG_ENABLED: false,
  AI_CONFIG_REFRESH_MS: 3000,
};

let cached = { ...DEFAULTS };
let lastFetchAt = 0;
const REFRESH_MS = 5000; // نحدّث القيم من Supabase كل 5 ثوانٍ لتطبيق تغييرات الإعدادات بسرعة.

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
