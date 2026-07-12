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
const fs = require("fs/promises");
const path = require("path");
const {
  admin,
  STORE_ID,
  BOT_ID,
  botRef,
  upsertCustomer,
  saveIncomingMessage,
  saveManualOutgoing,
  queueOutgoingMessage,
  markOutboxSent,
  markOutboxError,
  logEvent,
  setConnectionState,
} = require("./firestore-writer");

const SERVICE_TOKEN = process.env.SERVICE_TOKEN || "";
// نقبل الرسائل القريبة من لحظة الربط حتى لا تُرفض بسبب فرق توقيت واتساب،
// وفي نفس الوقت لا نستورد المحادثات القديمة جداً.
const NEW_MESSAGE_GRACE_SEC = Number(process.env.NEW_MESSAGE_GRACE_SEC || 45);

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
let lastError = null;
const recentAutoSends = new Set();
// وقت اكتمال الربط — نتجاهل أي رسالة أقدم منه حتى لا نستورد المحادثات القديمة.
let readyAtSec = 0;

function errorMessage(error) {
  return String(error?.message || error || "unknown error").slice(0, 500);
}

process.on("unhandledRejection", (error) => {
  lastError = errorMessage(error);
  console.error("unhandledRejection:", lastError);
});

process.on("uncaughtException", (error) => {
  lastError = errorMessage(error);
  console.error("uncaughtException:", lastError);
});


client.on("qr", async (qr) => {
  lastError = null;
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
  lastError = null;
  connectionState = "connected";
  latestQrRaw = null;
  latestQrDataUrl = null;
  readyAtSec = Math.floor(Date.now() / 1000) - NEW_MESSAGE_GRACE_SEC;
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
  lastError = errorMessage(m || "auth_failure");
  connectionState = "disconnected";
  await logEvent("auth_failure", { message: String(m) });
  await setConnectionState({ connectionState: "disconnected", status: "pending", waConnected: false });
});

client.on("disconnected", async (reason) => {
  lastError = errorMessage(reason || "disconnected");
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
    const saved = await saveIncomingMessage(msg); // → يضعها في aiQueue ليقرأها عامل الذكاء
    if (saved) await logEvent("incoming_saved", { phone: saved.phone, msgId: saved.msgId, type: msg.type || "text" });
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
  const manualKey = `${String(msg.to || "").replace(/@.*/, "")}:${msg.body || ""}`;
  if (recentAutoSends.has(manualKey)) return;
  try {
    await saveManualOutgoing(msg);
  } catch (e) {
    console.error("message_create error:", e.message);
  }
});

client.initialize().catch(async (error) => {
  lastError = errorMessage(error);
  connectionState = "init_error";
  console.error("client.initialize failed:", lastError);
  await setConnectionState({ connectionState: "init_error", status: "pending", waConnected: false, lastError }).catch(() => {});
});

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
    const autoKey = `${phone}:${text}`;
    recentAutoSends.add(autoKey);
    setTimeout(() => recentAutoSends.delete(autoKey), 30000).unref?.();
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

// السماح للوحة تيسير (على دومين مختلف) بقراءة الحالة والـ QR من Railway.
// بدون CORS سيظهر في اللوحة "غير متصل" حتى لو كان السيرفر يعمل فعلياً.
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Max-Age", "86400");
  if (req.method === "OPTIONS") return res.status(204).end();
  next();
});

app.use(express.json());

function auth(req) {
  if (!SERVICE_TOKEN) return true;
  const h = req.headers.authorization || "";
  return h === `Bearer ${SERVICE_TOKEN}`;
}

app.get("/", (_req, res) => res.json({ ok: true, service: "whatsapp-bridge", storeId: STORE_ID, botId: BOT_ID }));

app.get("/health", async (_req, res) => {
  try {
    await Promise.race([
      botRef().get(),
      new Promise((_, reject) => setTimeout(() => reject(new Error("Firestore healthcheck timeout")), 8000)),
    ]);
    res.json({ ok: true, service: "whatsapp-bridge", firestore: true, storeId: STORE_ID, botId: BOT_ID, connectionState });
  } catch (error) {
    lastError = errorMessage(error);
    res.status(500).json({ ok: false, service: "whatsapp-bridge", firestore: false, error: lastError, storeId: STORE_ID, botId: BOT_ID });
  }
});

app.get("/status", (_req, res) => {
  res.json({
    ok: true,
    service: "whatsapp-bridge",
    storeId: STORE_ID,
    botId: BOT_ID,
    connectionState,
    status: connectionState,
    connection: connectionState,
    state: connectionState === "connected" ? "open" : connectionState,
    connected: connectionState === "connected",
    ready: connectionState === "connected",
    hasQr: !!latestQrRaw,
    qr: latestQrRaw,
    qrDataUrl: latestQrDataUrl,
    lastError,
    readyAtSec,
    newMessageGraceSec: NEW_MESSAGE_GRACE_SEC,
    uptimeSec: Math.floor(process.uptime()),
    serverTime: new Date().toISOString(),
  });
});

app.get("/qr", (_req, res) => res.json({ qr: latestQrRaw, qrDataUrl: latestQrDataUrl }));

app.post("/send", async (req, res) => {
  if (!auth(req)) return res.status(401).json({ error: "unauthorized" });
  try {
    const phone = String(req.body?.phone || "").replace(/\D/g, "");
    const text = String(req.body?.text || req.body?.message || "").trim();
    if (!/^\d{8,15}$/.test(phone) || !text) return res.status(400).json({ ok: false, error: "phone/text required" });
    const convMsgId = await queueOutgoingMessage(phone, text, { source: "api" });
    res.json({ ok: true, queued: true, phone, convMsgId });
  } catch (e) {
    lastError = errorMessage(e);
    res.status(500).json({ ok: false, error: lastError });
  }
});

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
  setTimeout(() => process.exit(1), 300); // Railway يعيد التشغيل تلقائياً عبر restartPolicy
});

app.post("/reset-session", async (req, res) => {
  if (!auth(req)) return res.status(401).json({ error: "unauthorized" });
  try {
    await client.destroy().catch(() => {});
    await fs.rm(path.resolve("./.wwebjs_auth"), { recursive: true, force: true });
    await fs.rm(path.resolve("./.wwebjs_cache"), { recursive: true, force: true });
    latestQrRaw = null;
    latestQrDataUrl = null;
    connectionState = "resetting";
    lastError = null;
    await setConnectionState({ connectionState: "resetting", status: "pending", waConnected: false, lastQr: null });
    res.json({ ok: true, restarting: true });
    setTimeout(() => process.exit(1), 300); // Railway يعيد التشغيل ويولد QR جديد
  } catch (e) {
    lastError = errorMessage(e);
    res.status(500).json({ error: lastError });
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`WhatsApp bridge on :${port}`));
