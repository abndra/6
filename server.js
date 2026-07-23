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
//   APP_SUPABASE_URL, APP_SUPABASE_SECRET_KEY
//   SERVICE_TOKEN = نفس قيمة railwayApiKey في إعدادات البوت
//
// dependencies: whatsapp-web.js qrcode-terminal qrcode @supabase/supabase-js express
// ============================================================

const { Client, RemoteAuth, MessageMedia } = require("whatsapp-web.js");
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
const { createFirestoreRemoteStore, deleteRemoteSessionById, deleteAllRemoteSessions } = require("./firestore-session-store");
const { getPerf } = require("./perf-settings-reader");
const { cleanEnvValue } = require("./env");

const SERVICE_TOKEN = cleanEnvValue("SERVICE_TOKEN");
if (!SERVICE_TOKEN) {
  console.error("SERVICE_TOKEN is required. أضفه في Railway بنفس قيمة railwayApiKey في لوحة تيسير.");
}
const NEW_MESSAGE_GRACE_SEC = Number(process.env.NEW_MESSAGE_GRACE_SEC || 45);
// هذه ثلاث قيم تُقرأ مرة واحدة عند الإقلاع (تتطلب إعادة نشر لتغييرها):
const REMOTE_SESSION_BACKUP_MS = Math.max(60000, Number(process.env.REMOTE_SESSION_BACKUP_MS || getPerf("REMOTE_SESSION_BACKUP_MS")));
const REMOTE_SESSION_CLIENT_ID = String(process.env.REMOTE_SESSION_CLIENT_ID || `${STORE_ID}_${BOT_ID}`).replace(/[^a-z0-9_-]/gi, "_");
const MESSAGE_SWEEP_ENABLED = getPerf("MESSAGE_SWEEP_ENABLED");
const MESSAGE_SWEEP_INTERVAL_MS = Math.max(300000, Number(process.env.MESSAGE_SWEEP_INTERVAL_MS || 600000));
const MESSAGE_SWEEP_LIMIT = Math.max(3, Math.min(10, Number(process.env.MESSAGE_SWEEP_LIMIT || 5)));
const WA_STATE_TIMEOUT_MS = Math.max(3000, Number(process.env.WA_STATE_TIMEOUT_MS || 7000));
let shutdownRequested = false;

// ---- عميل واتساب ----
// الاسم الذي يظهر في «الأجهزة المرتبطة» = navigator.platform (النظام) + متصفح مستخرَج من UA.
// نستخدم UA قياسي كامل حتى يتعرّف واتساب على المتصفح (Chrome) بدلاً من إظهار null،
// ثم نُبدّل navigator.platform إلى "Taysir" قبل تحميل صفحة WhatsApp Web عبر
// evaluateOnNewDocument — هذا يجعل الجهاز يظهر باسم «Taysir» بدل «Mac OS».
const TAYSIR_DEVICE_NAME = process.env.TAYSIR_DEVICE_NAME || "Taysir";
const TAYSIR_USER_AGENT =
  process.env.TAYSIR_USER_AGENT ||
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

const client = new Client({
  authStrategy: new RemoteAuth({
    clientId: REMOTE_SESSION_CLIENT_ID,
    dataPath: "./.wwebjs_auth",
    store: createFirestoreRemoteStore(),
    backupSyncIntervalMs: REMOTE_SESSION_BACKUP_MS,
  }),
  userAgent: TAYSIR_USER_AGENT,
  // حل جذري لمشكلة «تسجيل خروج فور مسح QR»: إذا كانت هناك جلسة سابقة/متعارضة على
  // نفس الرقم، نأخذ الأولوية بدل أن يطردنا واتساب. takeoverTimeoutMs=0 يعني فوراً.
  takeoverOnConflict: true,
  takeoverTimeoutMs: 0,
  puppeteer: {
    headless: true,
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
      "--disable-background-networking",
      "--disable-sync",
      "--disable-features=Translate,BackForwardCache,AcceptCHFrame,MediaRouter,OptimizationHints",
      "--renderer-process-limit=1",
      `--user-agent=${TAYSIR_USER_AGENT}`,
      "--js-flags=--max-old-space-size=96",
    ],
  },
});

// نُعدّل navigator.platform إلى اسم البراند قبل أن تُحمّل صفحة WhatsApp Web،
// لأن قائمة «الأجهزة المرتبطة» تعرض هذه القيمة كاسم النظام.
async function applyTaysirBranding(page) {
  if (!page) return;
  try {
    await page.setUserAgent(TAYSIR_USER_AGENT);
  } catch (_) {}
  try {
    await page.evaluateOnNewDocument((deviceName) => {
      try {
        Object.defineProperty(navigator, "platform", { get: () => deviceName, configurable: true });
      } catch (_) {}
      try {
        Object.defineProperty(navigator, "userAgentData", {
          get: () => ({
            brands: [{ brand: "Chromium", version: "124" }, { brand: deviceName, version: "1" }],
            mobile: false,
            platform: deviceName,
          }),
          configurable: true,
        });
      } catch (_) {}
    }, TAYSIR_DEVICE_NAME);
  } catch (_) {}
}

// نلتقط الصفحة فور إنشائها (قبل initialize يفتح WhatsApp Web) عبر hook داخلي.
const _originalInitialize = client.initialize.bind(client);
client.initialize = async function () {
  const result = await _originalInitialize();
  await applyTaysirBranding(client.pupPage);
  return result;
};

// طبقة أمان إضافية عند ready.
client.on("ready", async () => {
  await applyTaysirBranding(client.pupPage);
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
        const message = errorMessage(error);
        // خطأ ناعم متوقع: ملف الـ zip لم يجهز بعد (تهيئة المتصفح/أول دقائق بعد الربط).
        // نتخطّاه بهدوء دون تلويث حالة الاتصال — الحفظ ينجح في الدورة التالية.
        const isBenign = error?.benign || error?.name === "SessionZipNotReadyError" || /ENOENT/.test(message);
        if (isBenign) {
          console.log("ℹ️ تأجيل حفظ الجلسة (لم يجهز ملف النسخة بعد) — ستُحفظ في الدورة التالية");
          return;
        }
        lastError = message;
        console.error("remote session save skipped:", message);
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

async function clearLocalAndRemoteSession() {
  const sessionName = client.authStrategy?.sessionName || `RemoteAuth-${REMOTE_SESSION_CLIENT_ID}`;
  await Promise.allSettled([
    client.logout().catch(() => {}),
    deleteRemoteSessionById(sessionName),
    deleteRemoteSessionById(`RemoteAuth-${REMOTE_SESSION_CLIENT_ID}`),
    deleteRemoteSessionById(REMOTE_SESSION_CLIENT_ID),
    deleteAllRemoteSessions(),
    fs.rm(path.resolve("./.wwebjs_auth"), { recursive: true, force: true }),
    fs.rm(path.resolve("./.wwebjs_cache"), { recursive: true, force: true }),
    fs.rm(path.resolve(process.cwd(), `${sessionName}.zip`), { force: true }),
    fs.rm(path.resolve(process.cwd(), `RemoteAuth-${REMOTE_SESSION_CLIENT_ID}.zip`), { force: true }),
  ]);
  latestQrRaw = null;
  latestQrDataUrl = null;
  remoteSessionSaved = false;
  readyAtSec = 0;
  lastWaState = null;
}

async function requestRestart(reason = "restart") {
  if (shutdownRequested) return;
  shutdownRequested = true;
  console.log(`↻ إعادة تشغيل العملية: ${reason}`);
  await Promise.race([
    saveRemoteSessionNow().catch((error) => console.warn("session save before restart skipped:", errorMessage(error))),
    new Promise((resolve) => setTimeout(resolve, 5000)),
  ]).catch(() => {});
  try {
    if (typeof unsubscribeOutbox === "function") unsubscribeOutbox();
  } catch {}
  setTimeout(() => process.exit(1), 500).unref?.();
}

async function gracefulExit(signal) {
  if (shutdownRequested) return;
  shutdownRequested = true;
  console.log(`↻ إيقاف Railway (${signal}) — محاولة حفظ آخر جلسة قبل الخروج`);
  await Promise.race([
    saveRemoteSessionNow().catch((error) => console.warn("session save on shutdown skipped:", errorMessage(error))),
    new Promise((resolve) => setTimeout(resolve, 8000)),
  ]).catch(() => {});
  process.exit(0);
}

process.on("SIGTERM", () => gracefulExit("SIGTERM"));
process.on("SIGINT", () => gracefulExit("SIGINT"));


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
  remoteSessionSaved = false;
  await deleteAllRemoteSessions().catch(() => {});
  await logEvent("auth_failure", { message: String(m) });
  await setConnectionState({ connectionState: "disconnected", status: "pending", waConnected: false, remoteSessionSaved: false, lastQr: null });
});

client.on("disconnected", async (reason) => {
  lastError = errorMessage(reason || "disconnected");
  connectionState = "disconnected";
  lastWaState = String(reason || "disconnected");
  await logEvent("disconnected", { reason });
  await setConnectionState({ connectionState: "disconnected", status: "pending", waConnected: false });
  setTimeout(() => requestRestart("whatsapp disconnected"), 1200).unref?.();
});

function scheduleConnectionVerify() {
  const delay = Math.max(15000, getPerf("CONNECTION_VERIFY_INTERVAL_MS"));
  setTimeout(() => { triggerConnectionVerify(); scheduleConnectionVerify(); }, delay).unref?.();
}
scheduleConnectionVerify();

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
    requestRestart("whatsapp initialize failed");
  }, 3000).unref?.();
});

// ============================================================
// مراقبة طابور الإرسال (outbox) — إرسال أي رد جديد فوراً
// ============================================================
const sending = new Set(); // منع الإرسال المزدوج لنفس الوثيقة
let unsubscribeOutbox = null;
let outboxListenerRetryTimer = null;
let outboxListenerRetryDelayMs = 30000;

function startOutboxDoc(doc) {
  if (!doc?.id || sending.has(doc.id)) return false;
  sending.add(doc.id);
  sendOne(doc).finally(() => sending.delete(doc.id));
  return true;
}

function attachOutboxListener() {
  if (!getPerf("SNAPSHOT_LISTENERS_ENABLED")) return;
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
        if (!/restricted|egress|quota/i.test(String(err.message || ""))) {
          logEvent("outbox_listener_error", { message: err.message }).catch(() => {});
        }
        clearTimeout(outboxListenerRetryTimer);
        outboxListenerRetryTimer = setTimeout(attachOutboxListener, outboxListenerRetryDelayMs);
        outboxListenerRetryDelayMs = Math.min(outboxListenerRetryDelayMs * 2, 5 * 60 * 1000);
        outboxListenerRetryTimer.unref?.();
      },
    );
}

async function drainPendingOutbox(reason = "poll") {
  if (connectionState !== "connected") return;
  const pend = await botRef().collection("outbox").where("status", "==", "pending").limit(10).get();
  pend.forEach((d) => startOutboxDoc(d));
  const t = Date.now();
  const hb = Math.max(60000, getPerf("OUTBOX_HEARTBEAT_WRITE_MS"));
  if (t - lastOutboxHeartbeatAt >= hb) {
    lastOutboxHeartbeatAt = t;
    await setConnectionState({ outboxHeartbeatAt: admin.firestore.FieldValue.serverTimestamp(), outboxPendingSeen: pend.size, outboxLastDrainReason: reason });
  }
}

attachOutboxListener();

function scheduleOutboxDrain() {
  const delay = Math.max(3000, getPerf("OUTBOX_POLL_INTERVAL_MS"));
  setTimeout(() => {
    drainPendingOutbox("interval").catch((e) => console.error("outbox drain error:", e.message));
    scheduleOutboxDrain();
  }, delay).unref?.();
}
scheduleOutboxDrain();

// تحميل الصورة في Node (مع مهلة وحد أقصى للحجم) بدل MessageMedia.fromUrl —
// لأن fromUrl يحمّل الصورة داخل متصفح واتساب وقد يعلّق الجلسة أو يقطع الاتصال.
const IMAGE_FETCH_TIMEOUT_MS = 15000;
const IMAGE_MAX_BYTES = 5 * 1024 * 1024; // 5MB — أكبر من ذلك قد يُثقل جلسة واتساب

async function loadImageMedia(mediaUrl) {
  if (/^data:/i.test(mediaUrl)) {
    const m = mediaUrl.match(/^data:([^;]+);base64,(.+)$/);
    if (!m) throw new Error("invalid data URL");
    return new MessageMedia(m[1], m[2], "product.jpg");
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), IMAGE_FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(mediaUrl, { signal: controller.signal, redirect: "follow" });
    if (!res.ok) throw new Error(`image HTTP ${res.status}`);
    const len = Number(res.headers.get("content-length") || 0);
    if (len && len > IMAGE_MAX_BYTES) throw new Error(`image too large (${len} bytes)`);
    const buf = Buffer.from(await res.arrayBuffer());
    if (buf.length > IMAGE_MAX_BYTES) throw new Error(`image too large (${buf.length} bytes)`);
    let mime = (res.headers.get("content-type") || "").split(";")[0].trim();
    if (!/^image\//i.test(mime)) mime = "image/jpeg";
    return new MessageMedia(mime, buf.toString("base64"), "product.jpg");
  } finally {
    clearTimeout(timer);
  }
}

// حارس مهلة عام حول أي إرسال حتى لا يعلّق الطابور أبداً.
function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`${label} timeout`)), ms).unref?.()),
  ]);
}

async function sendOne(doc) {
  const data = doc.data() || {};
  const { phone, chatId, text, convMsgId, mediaUrl, type, caption } = data;
  if (!phone || (!text && !mediaUrl)) {
    await markOutboxError(doc.id, "missing phone/text");
    return;
  }
  if (connectionState !== "connected") {
    // واتساب غير متصل الآن — نتركها pending لتُرسل عند عودة الاتصال
    return;
  }
  try {
    const autoKey = `${phone}:${text || mediaUrl}`;
    recentAutoSends.add(autoKey);
    setTimeout(() => recentAutoSends.delete(autoKey), 30000).unref?.();
    const destination = chatId || (/^\d{8,15}$/.test(String(phone)) ? `${phone}@c.us` : null);
    if (!destination) throw new Error("missing chatId for non-phone WhatsApp contact");

    const isImage = mediaUrl && (type === "image" || /^data:image|\.(jpe?g|png|webp|gif)(\?|$)/i.test(mediaUrl));
    if (isImage) {
      const cap = caption || text || undefined;
      try {
        const media = await loadImageMedia(mediaUrl);
        await withTimeout(
          client.sendMessage(destination, media, { caption: cap }),
          30000,
          "sendImage",
        );
      } catch (imgErr) {
        // فشل إرسال الصورة يجب ألا يقطع المحادثة — أرسل التعليق كنص بديل
        console.error("image send failed, falling back to text:", imgErr.message);
        await logEvent("image_send_fallback", { phone, message: imgErr.message }).catch(() => {});
        if (cap) await withTimeout(client.sendMessage(destination, cap), 20000, "sendText");
      }
    } else {
      await withTimeout(client.sendMessage(destination, text), 20000, "sendText");
    }
    await markOutboxSent(doc.id, phone, convMsgId);
    console.log(`→ أُرسلت ${mediaUrl ? "صورة" : "رسالة"} إلى ${phone}`);
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
  if (!SERVICE_TOKEN) return false;
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
    memory: process.memoryUsage(),
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
    await clearLocalAndRemoteSession();
    connectionState = "disconnected";
    await setConnectionState({ connectionState: "disconnected", status: "pending", waConnected: false, lastQr: null, remoteSessionSaved: false });
    res.json({ ok: true, unlinked: true, restarting: true });
    setTimeout(() => requestRestart("logout requested"), 300).unref?.();
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post("/restart", async (req, res) => {
  if (!auth(req)) return res.status(401).json({ error: "unauthorized" });
  res.json({ ok: true });
  setTimeout(() => requestRestart("manual restart"), 300); // Railway يعيد التشغيل تلقائياً عبر restartPolicy
});

app.post("/reset-session", async (req, res) => {
  if (!auth(req)) return res.status(401).json({ error: "unauthorized" });
  try {
    await clearLocalAndRemoteSession();
    await client.destroy().catch(() => {});
    connectionState = "resetting";
    lastError = null;
    await setConnectionState({ connectionState: "resetting", status: "pending", waConnected: false, lastQr: null, remoteSessionSaved: false });
    res.json({ ok: true, unlinked: true, remoteSessionDeleted: true, restarting: true });
    setTimeout(() => requestRestart("session reset requested"), 300); // Railway يعيد التشغيل ويولد QR جديد
  } catch (e) {
    lastError = errorMessage(e);
    res.status(500).json({ error: lastError });
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`WhatsApp bridge on :${port}`));
