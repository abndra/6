// ============================================================
// server.js — خادم واتساب (جسر فقط) — لا علاقة له بالذكاء الاصطناعي
// ============================================================
// مسؤوليته الوحيدة:
//   (1) استقبال رسائل واتساب  → حفظها فوراً في Firestore.
//   (2) مراقبة طابور الإرسال (outbox) في Firestore → إرسال أي رد جديد
//       إلى الرقم فور ظهوره.
//
// هو لا يعرف شيئاً عن Groq ولا يبني أي رد. الذكاء يعمل في ai-worker.js.
//
// المتغيرات المطلوبة في Railway → Variables:
//   FIREBASE_SERVICE_ACCOUNT, TAYSIR_STORE_ID, TAYSIR_BOT_ID
//   SERVICE_TOKEN (اختياري) = نفس قيمة railwayApiKey في إعدادات البوت
//
// dependencies: whatsapp-web.js qrcode-terminal qrcode firebase-admin express
// ============================================================

const { Client, LocalAuth } = require("whatsapp-web.js");
const qrcodeTerminal = require("qrcode-terminal");
const QRCode = require("qrcode");
const express = require("express");
const {
  admin,
  botRef,
  upsertCustomer,
  saveIncomingMessage,
  saveManualOutgoing,
  markOutboxSent,
  markOutboxError,
  logEvent,
  setConnectionState,
} = require("./firestore-writer");

const SERVICE_TOKEN = process.env.SERVICE_TOKEN || "";

// ---- عميل واتساب ----
const client = new Client({
  authStrategy: new LocalAuth({ dataPath: "./.wwebjs_auth" }),
  puppeteer: {
    headless: true,
    // على Railway يُثبَّت Chromium عبر Dockerfile ويُمرَّر مساره هنا.
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
  },
});

let latestQrRaw = null;
let latestQrDataUrl = null;
let connectionState = "disconnected";
// وقت اكتمال الربط — نتجاهل أي رسالة أقدم منه حتى لا نستورد المحادثات القديمة.
let readyAtSec = 0;


client.on("qr", async (qr) => {
  latestQrRaw = qr;
  connectionState = "qr";
  qrcodeTerminal.generate(qr, { small: true });
  try {
    latestQrDataUrl = await QRCode.toDataURL(qr, { margin: 1, width: 320 });
  } catch {
    latestQrDataUrl = null;
  }
  await logEvent("qr", {});
  await setConnectionState({ lastQr: qr, connectionState: "qr", status: "pending", qrUpdatedAt: admin.firestore.FieldValue.serverTimestamp() });
});

client.on("ready", async () => {
  connectionState = "connected";
  latestQrRaw = null;
  latestQrDataUrl = null;
  readyAtSec = Math.floor(Date.now() / 1000);
  console.log("✓ WhatsApp متصل — سيتم حفظ الرسائل الجديدة فقط بعد هذه اللحظة");
  await logEvent("connected");

  await setConnectionState({
    connectionState: "connected",
    status: "connected",
    waConnected: true,
    lastQr: null,
    phoneNumber: client.info?.wid?.user || null,
    connectedAt: admin.firestore.FieldValue.serverTimestamp(),
  });
});

client.on("auth_failure", async (m) => {
  connectionState = "disconnected";
  await logEvent("auth_failure", { message: String(m) });
  await setConnectionState({ connectionState: "disconnected", status: "pending", waConnected: false });
});

client.on("disconnected", async (reason) => {
  connectionState = "disconnected";
  await logEvent("disconnected", { reason });
  await setConnectionState({ connectionState: "disconnected", status: "pending", waConnected: false });
});

// ---- استقبال الرسائل: الرسائل الجديدة فقط (لا استيراد للقديمة) ----
client.on("message", async (msg) => {
  try {
    if (!/@c\.us$/.test(msg.from || "")) return; // تجاهل المجموعات/البث/@lid
    if (msg.isStatus) return;
    // تجاهل أي رسالة وصلت قبل اكتمال الربط (رسائل قديمة/مزامنة أولية)
    if (readyAtSec && Number(msg.timestamp) && Number(msg.timestamp) < readyAtSec) return;
    await upsertCustomer(msg);
    await saveIncomingMessage(msg); // → يضعها في aiQueue ليقرأها عامل الذكاء
  } catch (e) {
    console.error("message handler error:", e);
    await logEvent("error", { where: "incoming", message: e.message });
  }

});

// ---- الرسائل الصادرة يدوياً من الهاتف نفسه: حفظ للعرض (الجديدة فقط) ----
client.on("message_create", async (msg) => {
  if (!msg.fromMe) return;
  if (!/@c\.us$/.test(msg.to || "")) return;
  if (readyAtSec && Number(msg.timestamp) && Number(msg.timestamp) < readyAtSec) return;
  try {
    await saveManualOutgoing(msg);
  } catch (e) {
    console.error("message_create error:", e.message);
  }
});

client.initialize();

// ============================================================
// مراقبة طابور الإرسال (outbox) — إرسال أي رد جديد فوراً
// ============================================================
const sending = new Set(); // منع الإرسال المزدوج لنفس الوثيقة

botRef()
  .collection("outbox")
  .where("status", "==", "pending")
  .onSnapshot(
    async (snap) => {
      snap.docChanges().forEach((change) => {
        if (change.type !== "added") return;
        const doc = change.doc;
        if (sending.has(doc.id)) return;
        sending.add(doc.id);
        sendOne(doc).finally(() => sending.delete(doc.id));
      });
    },
    (err) => console.error("outbox listener error:", err.message),
  );

async function sendOne(doc) {
  const { phone, text, convMsgId } = doc.data() || {};
  if (!phone || !text) {
    await markOutboxError(doc.id, "missing phone/text");
    return;
  }
  if (connectionState !== "connected") {
    // واتساب غير متصل الآن — نتركها pending لتُرسل عند عودة الاتصال
    return;
  }
  try {
    await client.sendMessage(`${phone}@c.us`, text);
    await markOutboxSent(doc.id, phone, convMsgId);
    console.log(`→ أُرسلت رسالة إلى ${phone}`);
  } catch (e) {
    console.error("send failed:", e.message);
    await markOutboxError(doc.id, e.message);
    await logEvent("send_error", { phone, message: e.message });
  }
}

// عند عودة الاتصال، أعد فحص أي رسائل بقيت pending
client.on("ready", async () => {
  try {
    const pend = await botRef().collection("outbox").where("status", "==", "pending").get();
    pend.forEach((d) => {
      if (sending.has(d.id)) return;
      sending.add(d.id);
      sendOne(d).finally(() => sending.delete(d.id));
    });
  } catch (e) {
    console.error("resend pending error:", e.message);
  }
});

// ============================================================
// HTTP endpoints (للوحة المتجر: الحالة + QR + إعادة التهيئة)
// ============================================================
const app = express();
app.use(express.json());

function auth(req) {
  if (!SERVICE_TOKEN) return true;
  const h = req.headers.authorization || "";
  return h === `Bearer ${SERVICE_TOKEN}`;
}

app.get("/", (_req, res) => res.json({ ok: true, service: "whatsapp-bridge" }));

app.get("/status", (_req, res) => {
  res.json({
    connectionState,
    state: connectionState === "connected" ? "open" : connectionState,
    connected: connectionState === "connected",
    ready: connectionState === "connected",
    hasQr: !!latestQrRaw,
    qr: latestQrRaw,
    qrDataUrl: latestQrDataUrl,
  });
});

app.get("/qr", (_req, res) => res.json({ qr: latestQrRaw, qrDataUrl: latestQrDataUrl }));

app.post("/logout", async (req, res) => {
  if (!auth(req)) return res.status(401).json({ error: "unauthorized" });
  try {
    await client.logout().catch(() => {});
    connectionState = "disconnected";
    await setConnectionState({ connectionState: "disconnected", status: "pending", waConnected: false, lastQr: null });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post("/restart", async (req, res) => {
  if (!auth(req)) return res.status(401).json({ error: "unauthorized" });
  res.json({ ok: true });
  setTimeout(() => process.exit(0), 300); // Railway يعيد التشغيل تلقائياً
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`WhatsApp bridge on :${port}`));
