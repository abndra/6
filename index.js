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

const { firstEnv, validateSupabaseConnection } = require("./env");

function startConfigurationGuard(missing, detail = "") {
  const http = require("http");
  const port = process.env.PORT || 3000;
  const message = detail || `إعداد Railway ناقص: ${missing.join(", ")}. أضف المتغيرات من لوحة تيسير ثم أعد Deploy.`;

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

async function main() {
  const missing = [];
  if (!firstEnv("APP_BACKEND_URL", "APP_SUPABASE_URL", "SUPABASE_URL").value) missing.push("APP_BACKEND_URL");
  if (!firstEnv("APP_BACKEND_PUBLISHABLE_KEY", "APP_SUPABASE_PUBLISHABLE_KEY", "SUPABASE_PUBLISHABLE_KEY", "SUPABASE_ANON_KEY", "APP_SUPABASE_SECRET_KEY", "SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_SECRET_KEY").value) missing.push("APP_BACKEND_PUBLISHABLE_KEY");
  if (!firstEnv("SERVICE_TOKEN").value) missing.push("SERVICE_TOKEN");

  if (missing.length) {
    startConfigurationGuard(missing);
    return;
  }

  try {
    const checked = await validateSupabaseConnection();
    console.log(`✅ اتصال Supabase صحيح عبر ${checked.keyName}`);
  } catch (e) {
    startConfigurationGuard(["APP_BACKEND_PUBLISHABLE_KEY"], e.message);
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
}

main().catch((e) => startConfigurationGuard(["runtime"], e.message));
