// ============================================================
// firestore-writer.js — طبقة البيانات المشتركة (Firestore)
// ============================================================
// هذا الملف هو الجسر الوحيد بين خادم واتساب وعامل الذكاء و Firestore.
// لا يحتوي على أي منطق واتساب ولا أي منطق Groq — فقط قراءة/كتابة.
//
// المتغيرات المطلوبة في Railway → Variables:
//   FIREBASE_SERVICE_ACCOUNT = محتوى ملف serviceAccountKey.json كامل (JSON)
//   TAYSIR_STORE_ID          = id المتجر  (stores/{storeId})
//   TAYSIR_BOT_ID            = id البوت   (stores/{storeId}/bots/{botId})
// ============================================================

const admin = require("firebase-admin");
const { getFirestore } = require("firebase-admin/firestore");
const crypto = require("crypto");

// ---- تهيئة Firebase Admin مرة واحدة فقط ----
if (!admin.apps.length) {
  const raw = process.env.FIREBASE_SERVICE_ACCOUNT;
  if (!raw) {
    throw new Error("FIREBASE_SERVICE_ACCOUNT is missing in Railway Variables");
  }
  let svc;
  try {
    svc = JSON.parse(raw);
  } catch (e) {
    throw new Error("FIREBASE_SERVICE_ACCOUNT is not valid JSON: " + e.message);
  }
  admin.initializeApp({ credential: admin.credential.cert(svc) });
}

const FieldValue = admin.firestore.FieldValue;

// معرّفات المتجر/البوت: تُقرأ من متغيرات Railway، وإن لم توجد تُقرأ تلقائياً
// من bot.config.json المرفق داخل ملفات GitHub — فلا حاجة لإضافتها يدوياً.
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
const FIRESTORE_DATABASE_ID = process.env.FIRESTORE_DATABASE_ID || embeddedConfig.firestoreDatabaseId || "default";
const db = getFirestore(admin.app(), FIRESTORE_DATABASE_ID);

if (!STORE_ID || !BOT_ID) {
  throw new Error(
    "TAYSIR_STORE_ID / TAYSIR_BOT_ID missing. حمّل ملفات GitHub من جديد بعد حفظ البوت (تحتوي bot.config.json) أو أضِف المتغيرين في Railway.",
  );
}


const storeRef = () => db.collection("stores").doc(STORE_ID);
const botRef = () => storeRef().collection("bots").doc(BOT_ID);
const botSecretsRef = () => db.collection("stores").doc(STORE_ID).collection("botSecrets").doc(BOT_ID);
const now = () => FieldValue.serverTimestamp();

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
  const alreadySaved = await msgDoc.get();
  if (alreadySaved.exists) return null;

  await convRef.set(
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

  // 2.b) الرسالة داخل المحادثة (للعرض في لوحة المتجر)
  await msgDoc.set({
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

  // 2.c) نسخة مسطّحة للعدّاد السريع
  await botRef().collection("messages").doc(messageDocId).set({
    conversationId: phone,
    chatId,
    fromMe: false,
    body,
    timestamp: now(),
  });

  await botRef().set({ messagesCount: FieldValue.increment(1), lastMessageAt: now() }, { merge: true });

  // 2.d) طابور الذكاء — العامل المنفصل يستمع لهذه المجموعة
  await botRef().collection("aiQueue").doc(messageDocId).set({
    phone,
    chatId,
    name,
    body,
    msgId: msgDoc.id,
    status: "pending",
    createdAt: now(),
  });

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
  try {
    await botRef().collection("events").add({ type, payload, at: now() });
  } catch (e) {
    console.error("logEvent failed:", e.message);
  }
}

// ============================================================
// 7) حالة الاتصال + QR (يكتبها خادم واتساب)
// ============================================================
async function setConnectionState(patch) {
  await botRef().set({ ...patch, updatedAt: now() }, { merge: true }).catch((e) => console.error("setConnectionState:", e.message));
}

// ============================================================
// 8) قراءة إعدادات البوت + مفتاح Groq (يستخدمها عامل الذكاء)
// ============================================================
async function readBotSecrets() {
  try {
    const snap = await botSecretsRef().get();
    return snap.exists ? snap.data() : {};
  } catch (e) {
    console.error("botSecrets read failed:", e.message);
    return {};
  }
}

async function readStoreConfig() {
  try {
    const snap = await storeRef().get();
    return snap.exists ? snap.data() : {};
  } catch (e) {
    console.error("store config read failed:", e.message);
    return {};
  }
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
