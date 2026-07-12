// ============================================================
// firestore-writer.js — طبقة البيانات المشتركة (Supabase)
// ============================================================
// هذا الملف هو الجسر الوحيد بين خادم واتساب وعامل الذكاء و Supabase.
// لا يحتوي على أي منطق واتساب ولا أي منطق Groq — فقط قراءة/كتابة.
//
// المتغيرات المطلوبة في Railway → Variables:
//   APP_SUPABASE_URL         = رابط مشروع Supabase
//   APP_SUPABASE_SECRET_KEY  = المفتاح السري / service role من Supabase
//   TAYSIR_STORE_ID          = id المتجر  (stores/{storeId})
//   TAYSIR_BOT_ID            = id البوت   (stores/{storeId}/bots/{botId})
// ============================================================

// ---- تهيئة طبقة التوافق (Supabase تحت الغطاء) ----
const { admin, getFirestore, FieldValue } = require("./firestore-compat");
const crypto = require("crypto");

if (!admin.apps.length) admin.initializeApp();

function readEmbeddedConfig() {
  try {
    return require("./bot.config.json") || {};
  } catch {
    return {};
  }
}
const embeddedConfig = readEmbeddedConfig();
const STORE_ID = process.env.TAYSIR_STORE_ID || embeddedConfig.storeId;
const BOT_ID = process.env.TAYSIR_BOT_ID || embeddedConfig.botId;
const db = getFirestore();


if (!STORE_ID || !BOT_ID) {
  throw new Error(
    "TAYSIR_STORE_ID / TAYSIR_BOT_ID missing. حمّل ملفات GitHub من جديد بعد حفظ البوت (تحتوي bot.config.json) أو أضِف المتغيرين في Railway.",
  );
}


const storeRef = () => db.collection("stores").doc(STORE_ID);
const botRef = () => storeRef().collection("bots").doc(BOT_ID);
const botSecretsRef = () => db.collection("stores").doc(STORE_ID).collection("botSecrets").doc(BOT_ID);
const now = () => FieldValue.serverTimestamp();

// تقليل استهلاك قاعدة البيانات: لا نسجل أحداثاً تفصيلية إلا عند تفعيلها صراحة،
// ونحتفظ بإعدادات البوت في الذاكرة حتى لا ينهار الرد عند ضغط/انقطاع مؤقت في الكوتا.
const EVENT_LOG_ENABLED = String(process.env.EVENT_LOG_ENABLED || "false").toLowerCase() === "true";
const CONFIG_CACHE_MS = Math.max(60_000, Number(process.env.SUPABASE_CONFIG_CACHE_MS || process.env.FIRESTORE_CONFIG_CACHE_MS || 600_000));
const CONNECTION_STATE_MIN_WRITE_MS = Math.max(15_000, Number(process.env.CONNECTION_STATE_MIN_WRITE_MS || 60_000));
let botSecretsCache = { data: null, expiresAt: 0, lastErrorLogAt: 0 };
let storeConfigCache = { data: null, expiresAt: 0, lastErrorLogAt: 0 };
let lastConnectionStateWriteAt = 0;

function isQuotaError(e) {
  return e?.code === 8 || /RESOURCE_EXHAUSTED|Quota limit exceeded/i.test(String(e?.message || e || ""));
}

function logReadFailure(cache, label, error) {
  const t = Date.now();
  if (t - cache.lastErrorLogAt < 60_000) return;
  cache.lastErrorLogAt = t;
  console.error(`${label} read failed:`, error.message);
}

async function readCachedDoc(ref, cache, label) {
  const t = Date.now();
  if (cache.data && t < cache.expiresAt) return cache.data;
  try {
    const snap = await ref.get();
    cache.data = snap.exists ? snap.data() : {};
    cache.expiresAt = t + CONFIG_CACHE_MS;
    return cache.data;
  } catch (e) {
    logReadFailure(cache, label, e);
    if (cache.data) {
      // عند ضغط قاعدة البيانات نستمر بآخر إعداد معروف بدلاً من إسقاط Groq/المعرفة إلى قيم فارغة.
      cache.expiresAt = t + (isQuotaError(e) ? CONFIG_CACHE_MS : 30_000);
      return cache.data;
    }
    return {};
  }
}

function hasImportantConnectionPatch(patch = {}) {
  return ["connectionState", "status", "waConnected", "lastQr", "phoneNumber", "lastError", "remoteSessionSaved", "waState"].some((key) =>
    Object.prototype.hasOwnProperty.call(patch, key),
  );
}

// ---- أدوات مساعدة ----
function phoneFromJid(jid) {
  return String(jid || "").replace(/@.*/, "");
}
function jidDomain(jid) {
  return String(jid || "").split("@")[1] || "";
}
function safeDocId(value) {
  return String(value || "")
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .slice(0, 120);
}
function customerIdFromJid(jid) {
  const raw = phoneFromJid(jid);
  if (/@c\.us$/.test(String(jid || "")) && /^\d{8,15}$/.test(raw)) return raw;
  if (/@lid$/.test(String(jid || "")) && raw) return `lid_${safeDocId(raw)}`;
  return safeDocId(raw);
}
function isRealCustomer(jid) {
  const value = String(jid || "");
  if (/@c\.us$/.test(value)) return /^\d{8,15}$/.test(phoneFromJid(value));
  // واتساب بدأ يُرجع بعض العملاء بصيغة @lid بدلاً من رقم الهاتف.
  // تجاهله يعني أن الرسائل تصل للسيرفر لكن لا تُحفظ ولا يرد الذكاء.
  if (/@lid$/.test(value)) return /^[a-zA-Z0-9_.-]{4,80}$/.test(phoneFromJid(value));
  return false;
}
function stableMessageDocId(msg, jid, body) {
  const raw = String(msg?.id?._serialized || "").trim();
  if (raw) return `wa_${safeDocId(raw)}`;
  const hash = crypto
    .createHash("sha1")
    .update([jid, msg?.timestamp || "", body || "", msg?.type || "text"].join("|"))
    .digest("hex");
  return `wa_${hash}`;
}

// ============================================================
// 1) حفظ / تحديث العميل
// ============================================================
async function upsertCustomer(msg) {
  const jid = msg.from || msg.to;
  if (!isRealCustomer(jid)) return;
  const phone = customerIdFromJid(jid);
  const rawPhone = phoneFromJid(jid);
  await botRef()
    .collection("customers")
    .doc(phone)
    .set(
      {
        phone,
        rawPhone,
        chatId: jid,
        jidDomain: jidDomain(jid),
        name: msg._data?.notifyName || msg.pushName || phone,
        lastSeenAt: now(),
        lastMessage: msg.body || "",
        messagesCount: FieldValue.increment(1),
        firstSeenAt: now(), // يُثبّت مرة واحدة عبر merge
      },
      { merge: true },
    );
}

// ============================================================
// 2) حفظ رسالة واردة + وضعها في طابور الذكاء (aiQueue)
//    هذه الدالة يستدعيها خادم واتساب فقط. لا علاقة لها بالذكاء.
// ============================================================
async function saveIncomingMessage(msg) {
  const jid = msg.from;
  if (!isRealCustomer(jid)) return null;
  const phone = customerIdFromJid(jid);
  const rawPhone = phoneFromJid(jid);
  const chatId = String(jid || "");
  const name = msg._data?.notifyName || msg.pushName || phone;
  const body = msg.body || "";
  const messageDocId = stableMessageDocId(msg, jid, body);

  // 2.a) رأس المحادثة
  const convRef = botRef().collection("conversations").doc(phone);
  const msgDoc = convRef.collection("messages").doc(messageDocId);

  // Batch واحد بدون قراءة مسبقة: يقلل قراءات قاعدة البيانات لكل رسالة.
  const batch = db.batch();
  try {
    batch.create(msgDoc, {
      from: jid,
      chatId,
      fromMe: false,
      body,
      type: msg.type || "text",
      mediaUrl: null,
      timestamp: now(),
      aiStatus: "pending",
      raw: { id: msg.id?._serialized || null },
    });
    batch.set(
      convRef,
      {
        phone,
        rawPhone,
        chatId,
        jidDomain: jidDomain(jid),
        name,
        lastMessage: body || `[${msg.type}]`,
        updatedAt: now(),
        unreadCount: FieldValue.increment(1),
      },
      { merge: true },
    );
    batch.set(botRef().collection("messages").doc(messageDocId), {
      conversationId: phone,
      chatId,
      fromMe: false,
      body,
      timestamp: now(),
    });
    batch.set(botRef(), { messagesCount: FieldValue.increment(1), lastMessageAt: now() }, { merge: true });
    batch.set(botRef().collection("aiQueue").doc(messageDocId), {
      phone,
      chatId,
      name,
      body,
      msgId: msgDoc.id,
      status: "pending",
      createdAt: now(),
    });
    await batch.commit();
  } catch (e) {
    if (e?.code === 6 || /ALREADY_EXISTS/i.test(String(e?.message || ""))) return null;
    throw e;
  }

  return { phone, name, body, msgId: msgDoc.id };
}

async function markIncomingAiDone(phone, msgId, patch = {}) {
  if (!phone || !msgId) return;
  await botRef()
    .collection("conversations")
    .doc(phone)
    .collection("messages")
    .doc(msgId)
    .set({ aiStatus: "done", ...patch }, { merge: true })
    .catch(() => {});
}

async function markIncomingAiError(phone, msgId, message) {
  if (!phone || !msgId) return;
  await botRef()
    .collection("conversations")
    .doc(phone)
    .collection("messages")
    .doc(msgId)
    .set({ aiStatus: "error", aiError: String(message || "").slice(0, 300) }, { merge: true })
    .catch(() => {});
}

// ============================================================
// 3) حفظ رسالة صادرة يدوية (من الهاتف نفسه) — للعرض فقط
// ============================================================
async function saveManualOutgoing(msg) {
  const jid = msg.to;
  if (!isRealCustomer(jid)) return;
  const phone = customerIdFromJid(jid);
  const chatId = String(jid || "");
  const convRef = botRef().collection("conversations").doc(phone);
  await convRef.set({ phone, chatId, lastMessage: msg.body || "", updatedAt: now(), unreadCount: 0 }, { merge: true });
  await convRef.collection("messages").add({
    from: "me",
    chatId,
    fromMe: true,
    body: msg.body || "",
    type: msg.type || "text",
    timestamp: now(),
    manual: true,
  });
}

// ============================================================
// 4) عامل الذكاء: كتابة الرد في المحادثة + وضعه في طابور الإرسال
// ============================================================
async function queueAiReply(phone, text, { source = "ai", chatId = null } = {}) {
  return queueOutgoingMessage(phone, text, { source, chatId });
}

async function queueOutgoingMessage(phone, text, { source = "api", chatId = null } = {}) {
  const convRef = botRef().collection("conversations").doc(phone);

  // 4.a) الرسالة الصادرة داخل المحادثة (تظهر فوراً في اللوحة)
  const msgDoc = await convRef.collection("messages").add({
    from: "bot",
    fromMe: true,
    body: text,
    chatId,
    type: "text",
    timestamp: now(),
    aiHandled: true,
    source,
    deliveryStatus: "queued",
  });
  await convRef.set({ lastMessage: text, updatedAt: now(), ...(chatId ? { chatId } : {}) }, { merge: true });

  // 4.b) طابور الإرسال — خادم واتساب يستمع لهذه المجموعة ويرسل فوراً
  await botRef().collection("outbox").add({
    phone,
    chatId,
    text,
    convMsgId: msgDoc.id,
    status: "pending",
    createdAt: now(),
    source,
  });

  return msgDoc.id;
}

// ============================================================
// 5) تحديث حالة رسالة في الطابور
// ============================================================
async function markOutboxSent(outboxId, phone, convMsgId) {
  await botRef().collection("outbox").doc(outboxId).set({ status: "sent", sentAt: now() }, { merge: true });
  if (convMsgId) {
    await botRef()
      .collection("conversations")
      .doc(phone)
      .collection("messages")
      .doc(convMsgId)
      .set({ deliveryStatus: "sent" }, { merge: true })
      .catch(() => {});
  }
}
async function markOutboxError(outboxId, message) {
  await botRef().collection("outbox").doc(outboxId).set({ status: "error", error: String(message || "").slice(0, 300), erroredAt: now() }, { merge: true });
}

// ============================================================
// 6) سجل الأحداث
// ============================================================
async function logEvent(type, payload = {}) {
  if (!EVENT_LOG_ENABLED) return;
  try {
    await botRef().collection("events").add({ type, payload, at: now() });
  } catch (e) {
    console.error("logEvent failed:", e.message);
  }
}

// ============================================================
// 7) حالة الاتصال + QR (يكتبها خادم واتساب)
// ============================================================
async function setConnectionState(patch, opts = {}) {
  const t = Date.now();
  if (!opts.force && !hasImportantConnectionPatch(patch) && t - lastConnectionStateWriteAt < CONNECTION_STATE_MIN_WRITE_MS) return;
  lastConnectionStateWriteAt = t;
  await botRef().set({ ...patch, updatedAt: now() }, { merge: true }).catch((e) => console.error("setConnectionState:", e.message));
}

// ============================================================
// 8) قراءة إعدادات البوت + مفتاح Groq (يستخدمها عامل الذكاء)
// ============================================================
async function readBotSecrets() {
  return readCachedDoc(botSecretsRef(), botSecretsCache, "botSecrets");
}

async function readStoreConfig() {
  return readCachedDoc(storeRef(), storeConfigCache, "store config");
}

module.exports = {
  admin,
  db,
  FieldValue,
  STORE_ID,
  BOT_ID,
  storeRef,
  botRef,
  botSecretsRef,
  phoneFromJid,
  customerIdFromJid,
  isRealCustomer,
  upsertCustomer,
  saveIncomingMessage,
  saveManualOutgoing,
  queueAiReply,
  queueOutgoingMessage,
  markIncomingAiDone,
  markIncomingAiError,
  markOutboxSent,
  markOutboxError,
  logEvent,
  setConnectionState,
  readBotSecrets,
  readStoreConfig,
};
