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

const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;
const STORE_ID = process.env.TAYSIR_STORE_ID;
const BOT_ID = process.env.TAYSIR_BOT_ID;

if (!STORE_ID || !BOT_ID) {
  throw new Error("TAYSIR_STORE_ID / TAYSIR_BOT_ID are required in Railway Variables");
}

const botRef = () => db.collection("stores").doc(STORE_ID).collection("bots").doc(BOT_ID);
const now = () => FieldValue.serverTimestamp();

// ---- أدوات مساعدة ----
function phoneFromJid(jid) {
  return String(jid || "").replace(/@.*/, "");
}
function isRealCustomer(jid) {
  if (!/@c\.us$/.test(String(jid || ""))) return false;
  const phone = phoneFromJid(jid);
  return /^\d{8,15}$/.test(phone);
}

// ============================================================
// 1) حفظ / تحديث العميل
// ============================================================
async function upsertCustomer(msg) {
  const jid = msg.from || msg.to;
  if (!isRealCustomer(jid)) return;
  const phone = phoneFromJid(jid);
  await botRef()
    .collection("customers")
    .doc(phone)
    .set(
      {
        phone,
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
  const phone = phoneFromJid(jid);
  const name = msg._data?.notifyName || msg.pushName || phone;
  const body = msg.body || "";

  // 2.a) رأس المحادثة
  const convRef = botRef().collection("conversations").doc(phone);
  await convRef.set(
    {
      phone,
      name,
      lastMessage: body || `[${msg.type}]`,
      updatedAt: now(),
      unreadCount: FieldValue.increment(1),
    },
    { merge: true },
  );

  // 2.b) الرسالة داخل المحادثة (للعرض في لوحة المتجر)
  const msgDoc = await convRef.collection("messages").add({
    from: jid,
    fromMe: false,
    body,
    type: msg.type || "text",
    mediaUrl: null,
    timestamp: now(),
    aiStatus: "pending",
    raw: { id: msg.id?._serialized || null },
  });

  // 2.c) نسخة مسطّحة للعدّاد السريع
  await botRef().collection("messages").add({
    conversationId: phone,
    fromMe: false,
    body,
    timestamp: now(),
  });

  await botRef().set({ messagesCount: FieldValue.increment(1), lastMessageAt: now() }, { merge: true });

  // 2.d) طابور الذكاء — العامل المنفصل يستمع لهذه المجموعة
  await botRef().collection("aiQueue").add({
    phone,
    name,
    body,
    msgId: msgDoc.id,
    status: "pending",
    createdAt: now(),
  });

  return { phone, name, body, msgId: msgDoc.id };
}

// ============================================================
// 3) حفظ رسالة صادرة يدوية (من الهاتف نفسه) — للعرض فقط
// ============================================================
async function saveManualOutgoing(msg) {
  const jid = msg.to;
  if (!isRealCustomer(jid)) return;
  const phone = phoneFromJid(jid);
  const convRef = botRef().collection("conversations").doc(phone);
  await convRef.set({ phone, lastMessage: msg.body || "", updatedAt: now(), unreadCount: 0 }, { merge: true });
  await convRef.collection("messages").add({
    from: "me",
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
async function queueAiReply(phone, text, { source = "ai" } = {}) {
  const convRef = botRef().collection("conversations").doc(phone);

  // 4.a) الرسالة الصادرة داخل المحادثة (تظهر فوراً في اللوحة)
  const msgDoc = await convRef.collection("messages").add({
    from: "bot",
    fromMe: true,
    body: text,
    type: "text",
    timestamp: now(),
    aiHandled: true,
    source,
    deliveryStatus: "queued",
  });
  await convRef.set({ lastMessage: text, updatedAt: now() }, { merge: true });

  // 4.b) طابور الإرسال — خادم واتساب يستمع لهذه المجموعة ويرسل فوراً
  await botRef().collection("outbox").add({
    phone,
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
    const snap = await db.collection("stores").doc(STORE_ID).collection("botSecrets").doc(BOT_ID).get();
    return snap.exists ? snap.data() : {};
  } catch (e) {
    console.error("botSecrets read failed:", e.message);
    return {};
  }
}

module.exports = {
  admin,
  db,
  FieldValue,
  STORE_ID,
  BOT_ID,
  botRef,
  phoneFromJid,
  isRealCustomer,
  upsertCustomer,
  saveIncomingMessage,
  saveManualOutgoing,
  queueAiReply,
  markOutboxSent,
  markOutboxError,
  logEvent,
  setConnectionState,
  readBotSecrets,
};
