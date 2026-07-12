// ============================================================
// server.js — خادم واتساب (جسر فقط) — لا علاقة له بالذكاء الاصطناعي
// ============================================================
// مسؤوليته الوحيدة:
//   (1) استقبال رسائل واتساب  → حفظها فوراً في Supabase.
//   (2) مراقبة طابور الإرسال (outbox) في Supabase → إرسال أي رد جديد
//       إلى الرقم فور ظهوره.
//
// هو لا يعرف شيئاً عن Groq ولا يبني أي رد. الذكاء يعمل في ai-worker.js.
//
// المتغيرات المطلوبة في Railway → Variables:
//   APP_SUPABASE_URL, APP_SUPABASE_SECRET_KEY, TAYSIR_STORE_ID, TAYSIR_BOT_ID
//   SERVICE_TOKEN (اختياري) = نفس قيمة railwayApiKey في إعدادات البوت
//
// dependencies: whatsapp-web.js qrcode-terminal qrcode @supabase/supabase-js express
// ============================================================

const { Client, RemoteAuth } = require("whatsapp-web.js");
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
  isRealCustomer,
  customerIdFromJid,
} = require("./firestore-writer");
const { createFirestoreRemoteStore, deleteRemoteSessionById } = require("./firestore-session-store");

const SERVICE_TOKEN = process.env.SERVICE_TOKEN || "";
// نقبل الرسائل القريبة من لحظة الربط حتى لا تُرفض بسبب فرق توقيت واتساب،
// وفي نفس الوقت لا نستورد المحادثات القديمة جداً.
const NEW_MESSAGE_GRACE_SEC = Number(process.env.NEW_MESSAGE_GRACE_SEC || 45);
// حفظ جلسة RemoteAuth عملية ضغط/رفع ثقيلة. كل دقيقة كان يسبب تداخل حفظ + استهلاك ذاكرة.
// خمس دقائق كافية لحفظ الجلسة باستمرار وتمنع OOM على خطط Railway الصغيرة.
const REMOTE_SESSION_BACKUP_MS = Math.max(60000, Number(process.env.REMOTE_SESSION_BACKUP_MS || 300000));
const REMOTE_SESSION_CLIENT_ID = String(process.env.REMOTE_SESSION_CLIENT_ID || `${STORE_ID}_${BOT_ID}`).replace(/[^a-z0-9_-]/gi, "_");
const OUTBOX_POLL_INTERVAL_MS = Math.max(250, Number(process.env.OUTBOX_POLL_INTERVAL_MS || 500));
// getChats/fetchMessages يحمّل بيانات كثيرة من واتساب. نجعله شبكة أمان خفيفة فقط، لا فحصاً كل 30 ثانية.
const MESSAGE_SWEEP_ENABLED = String(process.env.MESSAGE_SWEEP_ENABLED || "false").toLowerCase() === "true";
const MESSAGE_SWEEP_INTERVAL_MS = Math.max(300000, Number(process.env.MESSAGE_SWEEP_INTERVAL_MS || 600000));
const MESSAGE_SWEEP_LIMIT = Math.max(3, Math.min(10, Number(process.env.MESSAGE_SWEEP_LIMIT || 5)));
const WA_STATE_TIMEOUT_MS = Math.max(3000, Number(process.env.WA_STATE_TIMEOUT_MS || 7000));
const CONNECTION_VERIFY_INTERVAL_MS = Math.max(30000, Number(process.env.CONNECTION_VERIFY_INTERVAL_MS || 60000));
const OUTBOX_HEARTBEAT_WRITE_MS = Math.max(5000, Number(process.env.OUTBOX_HEARTBEAT_WRITE_MS || 30000));

// ---- عميل واتساب ----
const client = new Client({
  authStrategy: new RemoteAuth({
    clientId: REMOTE_SESSION_CLIENT_ID,
    dataPath: "./.wwebjs_auth",
    store: createFirestoreRemoteStore(),
    backupSyncIntervalMs: REMOTE_SESSION_BACKUP_MS,
  }),
  puppeteer: {
    headless: true,
    // على Railway يُثبَّت Chromium عبر Dockerfile ويُمرَّر مساره هنا.
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
      "--disable-extensions",
      "--no-first-run",
      "--disable-background-timer-throttling",
      "--disable-backgrounding-occluded-windows",
      "--disable-renderer-backgrounding",
      "--js-flags=--max-old-space-size=128",
    ],
  },
});

let latestQrRaw = null;
let latestQrDataUrl = null;
let connectionState = "disconnected";
let lastWaState = null;
let lastError = null;
let remoteSessionSaved = false;
let savingRemoteSession = false;
let statusVerifyInFlight = null;
const recentAutoSends = new Set();
let lastOutboxHeartbeatAt = 0;
// وقت اكتمال الربط — نتجاهل أي رسالة أقدم منه حتى لا نستورد المحادثات القديمة.
let readyAtSec = 0;

function errorMessage(error) {
  return String(error?.message || error || "unknown error").slice(0, 500);
}

function isHealthyWaState(state) {
  const value = String(state || "").toUpperCase();
  return ["CONNECTED", "OPEN", "PAIRING", "TIMEOUT"].includes(value);
}

function installRemoteSessionSaveLock() {
  const strategy = client.authStrategy;
  if (!strategy || typeof strategy.storeRemoteSession !== "function" || strategy.__taysirSaveLockInstalled) return;
  const originalStoreRemoteSession = strategy.storeRemoteSession.bind(strategy);
  let saveChain = Promise.resolve();

  strategy.storeRemoteSession = async (options) => {
    const run = async () => {
      try {
        await originalStoreRemoteSession(options);
      } catch (error) {
        lastError = errorMessage(error);
        console.error("remote session save skipped:", lastError);
        await setConnectionState({ lastError, remoteSessionSaveErrorAt: admin.firestore.FieldValue.serverTimestamp() }).catch(() => {});
        // لا نرمي الخطأ هنا: RemoteAuth يستدعي الحفظ من interval داخلي، والرمي كان يصنع
        // unhandledRejection ويترك العملية بحالة غير مستقرة. سيُعاد الحفظ في الدورة التالية.
      }
    };

    const next = saveChain.catch(() => {}).then(run);
    saveChain = next.catch(() => {});
    return next;
  };
  strategy.__taysirSaveLockInstalled = true;
}

installRemoteSessionSaveLock();

async function verifyConnectionState({ persist = false } = {}) {
  if (connectionState !== "connected") return { connected: false, state: lastWaState || connectionState };
  try {
    const state = await Promise.race([
      client.getState(),
      new Promise((_, reject) => setTimeout(() => reject(new Error("WhatsApp getState timeout")), WA_STATE_TIMEOUT_MS)),
    ]);
    lastWaState = state || null;
    if (state && !isHealthyWaState(state)) {
      connectionState = "disconnected";
      lastError = `WhatsApp state: ${state}`;
      if (persist) await setConnectionState({ connectionState: "disconnected", status: "pending", waConnected: false, lastError, waState: state });
      return { connected: false, state };
    }
    return { connected: true, state };
  } catch (error) {
    lastError = errorMessage(error);
    if (persist) await setConnectionState({ lastError, waState: lastWaState });
    return { connected: connectionState === "connected", state: lastWaState, error: lastError };
  }
}

function triggerConnectionVerify() {
  if (statusVerifyInFlight) return;
  statusVerifyInFlight = verifyConnectionState({ persist: true })
    .catch(() => {})
    .finally(() => {
      statusVerifyInFlight = null;
    });
}

async function saveRemoteSessionNow() {
  if (savingRemoteSession) return;
  if (typeof client.authStrategy?.storeRemoteSession !== "function") return;
  savingRemoteSession = true;
  try {
    await client.authStrategy.storeRemoteSession({ emit: true });
  } finally {
    savingRemoteSession = false;
  }
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
  lastWaState = "CONNECTED";
  console.log("✓ WhatsApp متصل — سيتم حفظ الرسائل الجديدة فقط بعد هذه اللحظة");
  await logEvent("connected");

  await setConnectionState({
    connectionState: "connected",
    status: "connected",
    waConnected: true,
    lastQr: null,
    phoneNumber: client.info?.wid?.user || null,
    connectedAt: admin.firestore.FieldValue.serverTimestamp(),
    remoteSessionSaved,
    waState: lastWaState,
  });

  // RemoteAuth يحفظ أول نسخة مستقرة بعد دقيقة ثم دورياً. الحفظ المبكر المتكرر كان
  // يتداخل مع الحفظ الداخلي ويولد ENOENT لملف RemoteAuth zip، لذلك نتركه مقفلاً ومتسلسلاً.
});

client.on("remote_session_saved", async () => {
  remoteSessionSaved = true;
  console.log("✓ تم حفظ جلسة واتساب في Supabase — لن تضيع بعد Restart/Deploy");
  await logEvent("remote_session_saved", {});
  await setConnectionState({ remoteSessionSaved: true });
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
  lastWaState = String(reason || "disconnected");
  await logEvent("disconnected", { reason });
  await setConnectionState({ connectionState: "disconnected", status: "pending", waConnected: false });
  setTimeout(() => {
    console.log("↻ إعادة تشغيل العملية بعد فصل واتساب حتى يسترجع RemoteAuth الجلسة تلقائياً");
    process.exit(1);
  }, 1200).unref?.();
});

setInterval(() => {
  triggerConnectionVerify();
}, CONNECTION_VERIFY_INTERVAL_MS).unref?.();

async function handleIncomingMessage(msg, source = "event") {
  try {
    if (!isRealCustomer(msg.from || "")) return; // تجاهل المجموعات/البث فقط، واقبل @c.us و @lid للعملاء
    if (msg.fromMe) return;
    if (msg.isStatus) return;
    // تجاهل أي رسالة وصلت قبل اكتمال الربط (رسائل قديمة/مزامنة أولية)
    if (readyAtSec && Number(msg.timestamp) && Number(msg.timestamp) < readyAtSec) return;
    const saved = await saveIncomingMessage(msg); // → يضعها في aiQueue ليقرأها عامل الذكاء
    if (saved) {
      await upsertCustomer(msg);
      await logEvent("incoming_saved", { phone: saved.phone, msgId: saved.msgId, type: msg.type || "text", source });
    }
  } catch (e) {
    console.error("message handler error:", e);
    await logEvent("error", { where: "incoming", message: e.message });
  }
}

// ---- استقبال الرسائل: الرسائل الجديدة فقط (لا استيراد للقديمة) ----
client.on("message", async (msg) => {
  await handleIncomingMessage(msg, "event");
});

async function sweepRecentMessages() {
  if (connectionState !== "connected") return;
  const live = await verifyConnectionState({ persist: false });
  if (live.connected === false) return;
  try {
    const chats = await client.getChats();
    let seen = 0;
    for (const chat of chats.slice(0, 50)) {
      const chatId = chat?.id?._serialized || "";
      if (!isRealCustomer(chatId)) continue;
      const messages = await chat.fetchMessages({ limit: MESSAGE_SWEEP_LIMIT });
      for (const msg of messages) {
        if (msg.fromMe || msg.isStatus) continue;
        await handleIncomingMessage(msg, "sweep");
        seen += 1;
      }
    }
    await setConnectionState({ messageSweepAt: admin.firestore.FieldValue.serverTimestamp(), messageSweepSeen: seen });
  } catch (e) {
    console.error("message sweep error:", e.message);
    await logEvent("message_sweep_error", { message: e.message }).catch(() => {});
  }
}

if (MESSAGE_SWEEP_ENABLED) {
  setInterval(() => {
    sweepRecentMessages().catch((e) => console.error("message sweep failed:", e.message));
  }, MESSAGE_SWEEP_INTERVAL_MS).unref?.();
}

// ---- الرسائل الصادرة يدوياً من الهاتف نفسه: حفظ للعرض (الجديدة فقط) ----
client.on("message_create", async (msg) => {
  if (!msg.fromMe) return;
  if (!isRealCustomer(msg.to || "")) return;
  if (readyAtSec && Number(msg.timestamp) && Number(msg.timestamp) < readyAtSec) return;
  const manualKey = `${customerIdFromJid(msg.to)}:${msg.body || ""}`;
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
  setTimeout(() => {
    console.log("↻ إعادة تشغيل العملية بعد فشل تهيئة واتساب");
    process.exit(1);
  }, 3000).unref?.();
});

// ============================================================
// مراقبة طابور الإرسال (outbox) — إرسال أي رد جديد فوراً
// ============================================================
const sending = new Set(); // منع الإرسال المزدوج لنفس الوثيقة
let unsubscribeOutbox = null;
let outboxListenerRetryTimer = null;

function startOutboxDoc(doc) {
  if (!doc?.id || sending.has(doc.id)) return false;
  sending.add(doc.id);
  sendOne(doc).finally(() => sending.delete(doc.id));
  return true;
}

function attachOutboxListener() {
  try {
    if (typeof unsubscribeOutbox === "function") unsubscribeOutbox();
  } catch {}
  unsubscribeOutbox = botRef()
    .collection("outbox")
    .where("status", "==", "pending")
    .onSnapshot(
      async (snap) => {
        snap.docChanges().forEach((change) => {
          if (change.type !== "added" && change.type !== "modified") return;
          startOutboxDoc(change.doc);
        });
      },
      (err) => {
        console.error("outbox listener error:", err.message);
        logEvent("outbox_listener_error", { message: err.message }).catch(() => {});
        clearTimeout(outboxListenerRetryTimer);
        outboxListenerRetryTimer = setTimeout(attachOutboxListener, 5000);
        outboxListenerRetryTimer.unref?.();
      },
    );
}

async function drainPendingOutbox(reason = "poll") {
  if (connectionState !== "connected") return;
  const pend = await botRef().collection("outbox").where("status", "==", "pending").limit(10).get();
  pend.forEach((d) => startOutboxDoc(d));
  const t = Date.now();
  if (t - lastOutboxHeartbeatAt >= OUTBOX_HEARTBEAT_WRITE_MS) {
    lastOutboxHeartbeatAt = t;
    await setConnectionState({ outboxHeartbeatAt: admin.firestore.FieldValue.serverTimestamp(), outboxPendingSeen: pend.size, outboxLastDrainReason: reason });
  }
}

attachOutboxListener();

setInterval(() => {
  drainPendingOutbox("interval").catch((e) => console.error("outbox drain error:", e.message));
}, OUTBOX_POLL_INTERVAL_MS).unref?.();

async function sendOne(doc) {
  const { phone, chatId, text, convMsgId } = doc.data() || {};
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
    const destination = chatId || (/^\d{8,15}$/.test(String(phone)) ? `${phone}@c.us` : null);
    if (!destination) throw new Error("missing chatId for non-phone WhatsApp contact");
    await client.sendMessage(destination, text);
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
    await drainPendingOutbox("ready");
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
  // لا نقرأ قاعدة البيانات هنا: Railway/المراقبات تضرب /health كثيراً.
  res.json({ ok: true, service: "whatsapp-bridge", database: "not_checked_to_save_resources", storeId: STORE_ID, botId: BOT_ID, connectionState });
});

app.get("/status", async (_req, res) => {
  triggerConnectionVerify();
  const isConnected = connectionState === "connected";
  res.json({
    ok: true,
    service: "whatsapp-bridge",
    storeId: STORE_ID,
    botId: BOT_ID,
    connectionState,
    status: connectionState,
    connection: connectionState,
    state: isConnected ? "open" : connectionState,
    connected: isConnected,
    ready: isConnected,
    waState: lastWaState,
    hasQr: !!latestQrRaw,
    qr: latestQrRaw,
    qrDataUrl: latestQrDataUrl,
    lastError,
    remoteSessionSaved,
    remoteSessionClientId: REMOTE_SESSION_CLIENT_ID,
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
    const chatId = String(req.body?.chatId || "").trim() || null;
    const text = String(req.body?.text || req.body?.message || "").trim();
    if (!/^\d{8,15}$/.test(phone) || !text) return res.status(400).json({ ok: false, error: "phone/text required" });
    const convMsgId = await queueOutgoingMessage(phone, text, { source: "api", chatId });
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
    await client.logout().catch(() => {});
    await client.destroy().catch(() => {});
    await deleteRemoteSessionById(client.authStrategy?.sessionName || `RemoteAuth-${REMOTE_SESSION_CLIENT_ID}`).catch(() => {});
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
