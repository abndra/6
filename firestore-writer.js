// ============================================================
// firestore-writer.js — أضفه إلى مشروع Railway (whatsapp-web.js)
// ============================================================
// يكتب كل رسالة/عميل/حدث إلى Firestore حتى تظهر مباشرة
// في لوحة تيسير (العملاء + الوارد + الإحصائيات).
// ============================================================

const admin = require("firebase-admin");

// ---- 1) تهيئة Firebase Admin (مرة واحدة) ----
// ضع في Railway → Variables:
//   FIREBASE_SERVICE_ACCOUNT = محتوى ملف serviceAccountKey.json كامل (JSON string)
//   TAYSIR_STORE_ID          = id المتجر في Firestore
//   TAYSIR_BOT_ID            = id البوت في Firestore
if (!admin.apps.length) {
  const svc = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  admin.initializeApp({ credential: admin.credential.cert(svc) });
}
const db = admin.firestore();
const STORE_ID = process.env.TAYSIR_STORE_ID;
const BOT_ID = process.env.TAYSIR_BOT_ID;
const botRef = () => db.collection("stores").doc(STORE_ID).collection("bots").doc(BOT_ID);

// ---- 2) حفظ عميل ----
async function upsertCustomer(msg) {
  const jid = msg.from || msg.to;
  if (!jid) return;
  const phone = jid.replace(/@.*/, "");
  const ref = botRef().collection("customers").doc(phone);
  const now = admin.firestore.FieldValue.serverTimestamp();
  await ref.set(
    {
      phone,
      name: msg._data?.notifyName || msg.pushName || phone,
      lastSeenAt: now,
      lastMessage: msg.body || "",
      messagesCount: admin.firestore.FieldValue.increment(1),
      firstSeenAt: now, // set-once via merge
    },
    { merge: true },
  );
}

// ---- 3) حفظ محادثة + رسالة ----
async function saveMessage(msg) {
  const jid = msg.fromMe ? msg.to : msg.from;
  if (!jid) return;
  const phone = jid.replace(/@.*/, "");
  const now = admin.firestore.FieldValue.serverTimestamp();

  const convRef = botRef().collection("conversations").doc(phone);
  await convRef.set(
    {
      phone,
      name: msg._data?.notifyName || msg.pushName || phone,
      lastMessage: msg.body || `[${msg.type}]`,
      updatedAt: now,
      unreadCount: msg.fromMe
        ? 0
        : admin.firestore.FieldValue.increment(1),
    },
    { merge: true },
  );

  await convRef.collection("messages").add({
    from: jid,
    fromMe: !!msg.fromMe,
    body: msg.body || "",
    type: msg.type || "text",
    mediaUrl: null,
    timestamp: now,
    raw: {
      id: msg.id?._serialized || null,
      author: msg.author || null,
    },
  });

  // نسخة مسطّحة اختيارية لعدّاد الرسائل السريع
  await botRef().collection("messages").add({
    conversationId: phone,
    fromMe: !!msg.fromMe,
    body: msg.body || "",
    timestamp: now,
  });

  // زيادة عدّاد البوت
  await botRef().set(
    {
      messagesCount: admin.firestore.FieldValue.increment(1),
      lastMessageAt: now,
    },
    { merge: true },
  );
}

// ---- 4) حفظ حدث (اتصال/قطع/QR/خطأ) ----
async function logEvent(type, payload = {}) {
  await botRef().collection("events").add({
    type,
    payload,
    at: admin.firestore.FieldValue.serverTimestamp(),
  });
}

// ---- 5) حفظ رد الذكاء الاصطناعي كرسالة صادرة ----
async function saveAiReply(toJid, text) {
  const phone = toJid.replace(/@.*/, "");
  const now = admin.firestore.FieldValue.serverTimestamp();
  const convRef = botRef().collection("conversations").doc(phone);
  await convRef.set({ lastMessage: text, updatedAt: now }, { merge: true });
  await convRef.collection("messages").add({
    from: "bot",
    fromMe: true,
    body: text,
    type: "text",
    timestamp: now,
    aiHandled: true,
  });
}

module.exports = { upsertCustomer, saveMessage, logEvent, saveAiReply };