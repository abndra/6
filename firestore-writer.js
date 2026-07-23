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
const { getPerf } = require("./perf-settings-reader");

if (!admin.apps.length) admin.initializeApp();

function readEmbeddedConfig() {
  try {
    return require("./bot.config.json") || {};
  } catch {
    return {};
  }
}
const embeddedConfig = readEmbeddedConfig();
// ملفات GitHub الجديدة تضع معرف المتجر/البوت داخل bot.config.json.
// نجعله المصدر الأول حتى لا تُبقي Railway متغيرات قديمة فتربط السيرفر ببوت/جلسة قديمة
// بعد تحديث GitHub وإعادة النشر.
const STORE_ID = embeddedConfig.storeId || process.env.TAYSIR_STORE_ID;
const BOT_ID = embeddedConfig.botId || process.env.TAYSIR_BOT_ID;
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

// تقليل استهلاك قاعدة البيانات: EVENT_LOG_ENABLED والكاش تُقرأ ديناميكياً من perfSettings.
function isEventLogEnabled() { return getPerf("EVENT_LOG_ENABLED"); }
function configCacheMs() { return Math.max(0, getPerf("SUPABASE_configCacheMs()")); }
const CONNECTION_STATE_MIN_WRITE_MS = Math.max(1000, Number(process.env.CONNECTION_STATE_MIN_WRITE_MS || 30000));
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
    cache.expiresAt = t + configCacheMs();
    return cache.data;
  } catch (e) {
    logReadFailure(cache, label, e);
    if (cache.data) {
      // عند ضغط قاعدة البيانات نستمر بآخر إعداد معروف بدلاً من إسقاط Groq/المعرفة إلى قيم فارغة.
      cache.expiresAt = t + (isQuotaError(e) ? configCacheMs() : 30_000);
      return cache.data;
    }
    return {};
  }
}

async function readFreshDoc(ref, cache, label, options = {}) {
  try {
    const snap = await ref.get();
    cache.data = snap.exists ? snap.data() : {};
    cache.expiresAt = Date.now() + configCacheMs();
    return cache.data;
  } catch (e) {
    logReadFailure(cache, label, e);
    if (options.noStale) return {};
    return cache.data || {};
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

  try {
    await msgDoc.create({
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
  } catch (e) {
    if (e?.code === 6 || /ALREADY_EXISTS/i.test(String(e?.message || ""))) return null;
    throw e;
  }

  await Promise.all([
    convRef.set(
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
    ),
    botRef().collection("messages").doc(messageDocId).set({
      conversationId: phone,
      chatId,
      fromMe: false,
      body,
      timestamp: now(),
    }),
    botRef().set({ messagesCount: FieldValue.increment(1), lastMessageAt: now() }, { merge: true }),
    botRef().collection("aiQueue").doc(messageDocId).set({
      phone,
      chatId,
      name,
      body,
      msgId: msgDoc.id,
      status: "pending",
      createdAt: now(),
    }),
  ]);

  // سجّل قيداً في سجل استهلاك التوكن — قراءة رسالة عميل
  appendTokenLedger({
    type: "msgIn",
    cost: 0.0375,
    phone,
    chatId,
    name,
    preview: (body || `[${msg.type}]`).slice(0, 80),
    direction: "in",
  }).catch(() => {});

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
  await Promise.all([
    convRef.set({ lastMessage: text, updatedAt: now(), ...(chatId ? { chatId } : {}) }, { merge: true }),
    // 4.b) طابور الإرسال — خادم واتساب يستمع لهذه المجموعة ويرسل فوراً
    botRef().collection("outbox").add({
      phone,
      chatId,
      text,
      convMsgId: msgDoc.id,
      status: "pending",
      createdAt: now(),
      source,
    }),
  ]);

  // سجّل قيد استهلاك — إرسال رد
  appendTokenLedger({
    type: "msgOut",
    cost: 0.0375,
    phone,
    chatId,
    preview: String(text || "").slice(0, 80),
    direction: "out",
    source,
  }).catch(() => {});

  return msgDoc.id;
}

// ============================================================
// سجل استهلاك التوكن — يُكتب في مجموعة فرعية للبوت
// يستخدمه لوحة الأدمين لعرض تفاصيل كل عملية استهلاك
// ============================================================
const TOKEN_LEDGER_ENABLED = String(process.env.TOKEN_LEDGER_ENABLED || "false").toLowerCase() === "true";
async function appendTokenLedger(entry = {}) {
  if (!TOKEN_LEDGER_ENABLED) return; // معطل افتراضياً لتقليل استهلاك Supabase egress
  try {
    const doc = {
      at: now(),
      type: String(entry.type || "msgOut"),
      cost: Number(entry.cost || 0),
      phone: entry.phone ? String(entry.phone) : null,
      chatId: entry.chatId ? String(entry.chatId) : null,
      name: entry.name ? String(entry.name).slice(0, 80) : null,
      preview: entry.preview ? String(entry.preview).slice(0, 200) : "",
      direction: entry.direction === "in" ? "in" : "out",
      source: entry.source ? String(entry.source) : null,
    };
    await botRef().collection("tokenLedger").add(doc);
  } catch (e) {
    console.error("appendTokenLedger:", e.message);
  }
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
  if (!isEventLogEnabled()) return;
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
async function readBotSecrets(options = {}) {
  return options.force ? readFreshDoc(botSecretsRef(), botSecretsCache, "botSecrets", { noStale: true }) : readCachedDoc(botSecretsRef(), botSecretsCache, "botSecrets");
}

async function readStoreConfig(options = {}) {
  return options.force ? readFreshDoc(storeRef(), storeConfigCache, "store config") : readCachedDoc(storeRef(), storeConfigCache, "store config");
}

// عطّل مفتاح Groq معيّن داخل botSecrets.groqKeys عند فشله (429/401/403)
async function markGroqKeyDisabled(key, error) {
  const target = String(key || "").trim();
  if (!target) return;
  try {
    const ref = botSecretsRef();
    const snap = await ref.get();
    const data = snap.exists ? snap.data() : {};
    const list = Array.isArray(data.groqKeys) ? [...data.groqKeys] : [];
    let idx = list.findIndex((item) => {
      if (!item) return false;
      if (typeof item === "string") return item.trim() === target;
      return String(item.key || "").trim() === target;
    });
    const stamp = { key: target, disabled: true, error: String(error || "").slice(0, 400), disabledAt: Date.now() };
    if (idx === -1) list.push(stamp);
    else list[idx] = { ...(typeof list[idx] === "string" ? { key: target } : list[idx]), ...stamp };
    // إذا كان المفتاح المعطّل هو المفتاح المفرد القديم، نمسحه لتفادي إعادة استخدامه
    const legacy = String(data.groqApiKey || "").trim();
    const patch = { groqKeys: list, updatedAt: now() };
    if (legacy && legacy === target) patch.groqApiKey = "";
    await ref.set(patch, { merge: true });
    // امسح الكاش حتى تُقرأ الحالة الجديدة فوراً
    botSecretsCache = { data: null, expiresAt: 0, lastErrorLogAt: 0 };
  } catch (e) {
    console.error("markGroqKeyDisabled failed:", e.message);
  }
}

// سجّل المفتاح الفعّال حالياً (آخر مفتاح Groq نجح فعلياً في توليد رد)
let _lastActiveKeyWrite = { key: "", at: 0 };
async function markGroqKeyActive(key) {
  const target = String(key || "").trim();
  if (!target) return;
  // لا نكتب كل مرة — مرة كل 30 ثانية إن لم يتغير المفتاح
  const t = Date.now();
  if (_lastActiveKeyWrite.key === target && t - _lastActiveKeyWrite.at < 30_000) return;
  _lastActiveKeyWrite = { key: target, at: t };
  try {
    await botSecretsRef().set(
      { activeGroqKey: target, activeGroqKeyAt: now(), updatedAt: now() },
      { merge: true },
    );
    botSecretsCache = { data: null, expiresAt: 0, lastErrorLogAt: 0 };
  } catch (e) {
    console.error("markGroqKeyActive failed:", e.message);
  }
}

// ============================================================
// 9) مخزن مفاتيح Groq المشترك (pool_groq/{id})
//    - كل مفتاح يُخزَّن مرة واحدة في المخزن.
//    - أي بوت يشير له عبر { poolId } داخل groqKeys[].
// ============================================================
const poolGroqRef = () => db.collection("pool_groq");

async function listPoolGroqKeys() {
  try {
    const snap = await poolGroqRef().get();
    return snap.docs.map((d) => ({ id: d.id, ...d.data() }));
  } catch (e) {
    console.error("listPoolGroqKeys failed:", e.message);
    return [];
  }
}

async function markPoolGroqDisabled(id, reason, isAuthError) {
  if (!id) return;
  try {
    await poolGroqRef().doc(id).set({
      status: isAuthError ? "disabled_auth" : "disabled_daily_quota",
      disabledAt: Date.now(),
      disabledReason: String(reason || "").slice(0, 400),
    }, { merge: true });
  } catch (e) {
    console.error("markPoolGroqDisabled failed:", e.message);
  }
}

async function markPoolGroqActive(id) {
  if (!id) return;
  try {
    await poolGroqRef().doc(id).set({ lastActiveAt: Date.now() }, { merge: true });
  } catch {}
}

// تفعيل يومي تلقائي: كل مفتاح disabled_daily_quota يعود active بعد
// انقضاء 24 ساعة من disabledAt والوصول إلى ساعة/دقيقة التجديد.
// تفعيل تلقائي: أي مفتاح disabled_daily_quota يعود active بعد مرور 24 ساعة
// كاملة (بالضبط) من لحظة تعطيله — بدون أي تدخل يدوي.
async function runPoolGroqAutoRenewal() {
  try {
    const list = await listPoolGroqKeys();
    const now = Date.now();
    const DAY_MS = 24 * 60 * 60 * 1000;
    for (const k of list) {
      if (k.status !== "disabled_daily_quota") continue;
      const disabledAt = Number(k.disabledAt || 0);
      if (!disabledAt) continue;
      if (now - disabledAt >= DAY_MS) {
        await poolGroqRef().doc(k.id).set({
          status: "active",
          disabledAt: null,
          disabledReason: "",
        }, { merge: true });
        console.log(`🔄 pool_groq/${k.id} أُعيد تفعيله تلقائياً بعد 24 ساعة.`);
      }
    }
  } catch (e) {
    console.error("runPoolGroqAutoRenewal failed:", e.message);
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
  markGroqKeyDisabled,
  markGroqKeyActive,
  listPoolGroqKeys,
  markPoolGroqDisabled,
  markPoolGroqActive,
  runPoolGroqAutoRenewal,
  appendTokenLedger,
};
