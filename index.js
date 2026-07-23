// ============================================================
// index.js — مُشغّل واحد يُشغّل الجسر + عامل الذكاء في عملية واحدة
// ============================================================
// هذا هو أمر البدء الافتراضي على Railway (npm start).
// الوحدتان منفصلتان تماماً في الكود: خادم واتساب لا يعرف Groq،
// وعامل الذكاء لا يعرف واتساب — يتواصلان فقط عبر Supabase.
//
// إن أردت نشرهما كخدمتين منفصلتين على Railway:
//   خدمة 1: npm run start:bridge   (خادم واتساب)
//   خدمة 2: npm run start:ai       (عامل الذكاء)
// ============================================================

console.log("🚀 تشغيل تيسير: خادم واتساب + عامل الذكاء (Groq)");

function firstEnv(...names) {
  for (const name of names) {
    const value = String(process.env[name] || "").trim();
    if (value) return value;
  }
  return "";
}

function startConfigurationGuard(missing) {
  const http = require("http");
  const port = process.env.PORT || 3000;
  const message = `إعداد Railway ناقص: ${missing.join(", ")}. أضف المتغيرات من لوحة تيسير ثم أعد Deploy.`;

  console.error("❌ " + message);
  console.error("SERVICE_TOKEN يجب أن يكون نفس القيمة المحفوظة داخل إعدادات البوت في لوحة تيسير.");

  http
    .createServer((req, res) => {
      res.writeHead(req.url === "/health" ? 503 : 200, { "content-type": "application/json; charset=utf-8" });
      res.end(JSON.stringify({ ok: false, configured: false, missing, message }));
    })
    .listen(port, () => {
      console.log(`🛑 خادم الحماية يعمل على المنفذ ${port} بانتظار ضبط المتغيرات بدون Crash Loop.`);
    });
}

const missing = [];
if (!firstEnv("APP_BACKEND_URL", "APP_SUPABASE_URL", "SUPABASE_URL")) missing.push("APP_BACKEND_URL");
if (!firstEnv("APP_BACKEND_PUBLISHABLE_KEY", "APP_SUPABASE_PUBLISHABLE_KEY", "SUPABASE_PUBLISHABLE_KEY", "SUPABASE_ANON_KEY", "APP_SUPABASE_SECRET_KEY", "SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_SECRET_KEY")) missing.push("APP_BACKEND_PUBLISHABLE_KEY");
if (!firstEnv("SERVICE_TOKEN")) missing.push("SERVICE_TOKEN");

if (missing.length) {
  startConfigurationGuard(missing);
  return;
}

// خادم واتساب (استقبال + إرسال عبر Supabase)
require("./server");

// عامل الذكاء (Groq) — معزول: أي خطأ فيه لا يُسقط خادم واتساب
try {
  require("./ai-worker");
} catch (e) {
  console.error("⚠️ فشل تشغيل عامل الذكاء (سيستمر خادم واتساب بالحفظ):", e.message);
}
