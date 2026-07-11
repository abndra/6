import admin from 'firebase-admin';
const STORE_ID = process.env.TAYSIR_STORE_ID || "zj4KW4k2kiInawdlofxD";
const BOT_ID = process.env.TAYSIR_BOT_ID || "6fZIB8yfDE2QCzn21JKM";
let db = null;
export let FIRE_READY = false;
try {
  const raw = process.env.FIREBASE_SERVICE_ACCOUNT;
  if (raw && STORE_ID && BOT_ID) {
    const svc = JSON.parse(raw);
    if (!admin.apps.length) admin.initializeApp({ credential: admin.credential.cert(svc) });
    db = admin.firestore();
    FIRE_READY = true;
    console.log('[firestore] ready store=' + STORE_ID + ' bot=' + BOT_ID);
  } else {
    console.warn('[firestore] disabled: set FIREBASE_SERVICE_ACCOUNT + TAYSIR_STORE_ID + TAYSIR_BOT_ID');
  }
} catch (e) { console.error('[firestore] init failed:', e?.message); }
const botRef = () => db && db.collection('stores').doc(STORE_ID).collection('bots').doc(BOT_ID);
const now = () => admin.firestore.FieldValue.serverTimestamp();
const inc = (n) => admin.firestore.FieldValue.increment(n);
export async function upsertCustomer({ jid, name, lastMessage, lastSeenAt }) {
  if (!FIRE_READY || !jid) return;
  const phone = String(jid).replace(/@.*/, '');
  await botRef().collection('customers').doc(phone).set({
    phone, jid, name: name || phone,
    lastMessage: lastMessage || '',
    lastSeenAt: lastSeenAt ? new Date(lastSeenAt) : now(),
    messagesCount: inc(1), firstSeenAt: now(), updatedAt: now(),
  }, { merge: true });
}
export async function saveMessage({ conversationId, messageId, from, fromMe, body, type, timestamp, raw, aiHandled, aiModel, source }) {
  if (!FIRE_READY || !conversationId) return;
  const when = timestamp ? new Date(timestamp) : new Date();
  const convRef = botRef().collection('conversations').doc(conversationId);
  await convRef.set({ phone: conversationId, lastMessage: body || '[' + (type || 'msg') + ']', updatedAt: when, unreadCount: fromMe ? 0 : inc(1) }, { merge: true });
  const msgRef = messageId ? convRef.collection('messages').doc(messageId) : convRef.collection('messages').doc();
  await msgRef.set({ from: from || null, fromMe: !!fromMe, body: body || '', type: type || 'text', timestamp: when, raw: raw || null, aiHandled: !!aiHandled, aiModel: aiModel || null, source: source || 'whatsapp' }, { merge: true });
  await botRef().collection('messages').add({ conversationId, fromMe: !!fromMe, body: body || '', timestamp: when });
}
export async function logEvent(type, payload = {}) { if (!FIRE_READY) return; await botRef().collection('events').add({ type, payload, at: now() }); }
export async function incrBotCounter(n = 1) { if (!FIRE_READY) return; await botRef().set({ messagesCount: inc(n), lastMessageAt: now() }, { merge: true }); }
export async function getBotConfig() {
  if (!FIRE_READY) return null;
  const snap = await botRef().get();
  if (!snap.exists) return null;
  return snap.data();
}
