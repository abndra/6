// ============================================================
// ai-worker.js — عامل الذكاء الاصطناعي (Groq) — منفصل تماماً عن واتساب
// ============================================================
// مسؤوليته الوحيدة:
//   يستمع لطابور الذكاء (aiQueue) في Supabase. عند وصول رسالة جديدة:
//     1) يقرأها بسرعة.
//     2) يبحث في إعدادات البوت: هل مغلق؟ هل يوجد رد جاهز/سؤال شائع مطابق؟
//        (رد فوري بدون استدعاء أي API — سرعة قصوى).
//     3) إن لم يجد شيئاً → يستدعي Groq مع قاعدة المعرفة والقوانين كسياق.
//     4) يكتب الرد في Supabase (المحادثة + طابور الإرسال outbox).
//   خادم واتساب هو من يلتقط الرد من outbox ويرسله.
//
// مفتاح Groq يُقرأ لكل بوت من إعداداته في Supabase فقط (botSecrets/{botId}.groqApiKey
// أو bots/{botId}.groqApiKey). لا نستخدم GROQ_API_KEY من Railway حتى لا يستمر البوت بالرد
// بعد حذف المفتاح من لوحة التحكم.
//
// dependencies: @supabase/supabase-js node-fetch@2
// ============================================================

const fetch = require("node-fetch");
const {
  storeRef,
  botRef,
  botSecretsRef,
  FieldValue,
  queueAiReply,
  readBotSecrets,
  readStoreConfig,
  markIncomingAiDone,
  markIncomingAiError,
  logEvent,
} = require("./firestore-writer");

// llama-3.1-8b-instant أسرع بكثير من 70b مع جودة ممتازة للردود القصيرة (يمكن تغييره من Railway env)
const GROQ_MODEL = process.env.GROQ_MODEL || "llama-3.1-8b-instant";
const STALE_PROCESSING_MS = Number(process.env.AI_STALE_PROCESSING_MS || 120000);
// السرعة أولاً: onSnapshot يلتقط الرسالة فوراً، polling كشبكة أمان بأدنى تأخير
const AI_POLL_INTERVAL_MS = Math.max(150, Number(process.env.AI_POLL_INTERVAL_MS || 250));
const AI_RECOVER_INTERVAL_MS = Math.max(15000, Number(process.env.AI_RECOVER_INTERVAL_MS || 30000));
const AI_GROQ_TIMEOUT_MS = Math.max(4000, Number(process.env.AI_GROQ_TIMEOUT_MS || 15000));
const AI_MAX_CONCURRENT = Math.max(1, Math.min(20, Number(process.env.AI_MAX_CONCURRENT || 10)));
const AI_CONFIG_REFRESH_MS = Math.max(1000, Number(process.env.AI_CONFIG_REFRESH_MS || 5000));
const AI_HEARTBEAT_WRITE_MS = Math.max(5000, Number(process.env.AI_HEARTBEAT_WRITE_MS || 30000));
const DEFAULT_CLOSED_MESSAGE = "نعتذر، المتجر مغلق حالياً. سنعود إليك عند الفتح.";

// ---- إعدادات البوت الحيّة (تُحدّث دورياً من Supabase) ----
let botConfig = {
  greeting: "",
  closedMessage: "",
  fallbackMessage: "تم استلام رسالتك، وسنرد عليك قريباً.",
  isOpen: true,
  persona: "أنت مساعد ودود في متجر إلكتروني. رد باللغة العربية بشكل قصير ومفيد.",
  systemInstructions: "",
  tone: "ودود ومحترف",
  rules: [],
  knowledge: [],
  products: [],
  quickReplies: [],
  faqs: [],
  groqApiKey: "",
  storeName: "المتجر",
  botName: "المساعد",
  language: "ar",
  temperature: 0.3,
  maxTokens: 220,
  workingHours: null,
  offHoursMessage: "",
  humanHandoff: false,
  humanHandoffTrigger: "",
};
let lastAiHeartbeatAt = 0;

function asArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function textValue(...values) {
  for (const value of values) {
    const text = String(value || "").trim();
    if (text) return text;
  }
  return "";
}

function mergeConfig(d = {}, secrets = {}, store = {}) {
  const storeName = textValue(store.name, store.storeName, store.slug, "المتجر");
  const botName = textValue(d.name, store.botName, "المساعد");
  const defaultPersona = `أنت ${botName}، مساعد ذكي لمتجر ${storeName}. رد بلغة العميل باختصار ووضوح، واعتمد فقط على معلومات المتجر المحفوظة.`;
  botConfig = {
    greeting: textValue(d.greeting, store.greeting),
    closedMessage: textValue(d.closedMessage, d.offHoursMessage, store.closedMessage, store.offHoursMessage),
    fallbackMessage: textValue(d.fallbackMessage, store.fallbackMessage, "تم استلام رسالتك، وسنرد عليك قريباً."),
    isOpen: d.isOpen !== false && d.active !== false && store.isOpen !== false,
    persona: textValue(d.persona, store.persona, defaultPersona),
    systemInstructions: textValue(d.systemInstructions, store.systemInstructions),
    tone: textValue(d.tone, store.tone, botConfig.tone),
    rules: asArray(d.rules).length ? asArray(d.rules) : asArray(store.rules),
    knowledge: [...asArray(store.knowledge), ...asArray(d.knowledge)],
    products: [...asArray(store.products), ...asArray(d.products)],
    quickReplies: [...asArray(store.quickReplies), ...asArray(d.quickReplies)],
    faqs: [...asArray(store.faqs), ...asArray(d.faqs)],
    // مهم: لا نرجع للمفتاح السابق ولا لمتغير Railway. إذا حذفه المستخدم من اللوحة يجب أن يتوقف فوراً.
    groqApiKey: textValue(secrets.groqApiKey, d.groqApiKey),
    storeName,
    botName,
    language: textValue(d.language, store.language, "ar"),
    temperature: typeof d.temperature === "number" ? Math.max(0, Math.min(1, d.temperature)) : botConfig.temperature,
    maxTokens: typeof d.maxTokens === "number" ? Math.max(80, Math.min(1200, d.maxTokens)) : botConfig.maxTokens,
    workingHours: d.workingHours || store.workingHours || null,
    offHoursMessage: textValue(d.offHoursMessage, d.closedMessage, store.offHoursMessage, store.closedMessage),
    humanHandoff: !!(d.humanHandoff || store.humanHandoff),
    humanHandoffTrigger: textValue(d.humanHandoffTrigger, store.humanHandoffTrigger),
  };
}

async function refreshConfig(d, options = {}) {
  const [secrets, store] = await Promise.all([readBotSecrets(options), readStoreConfig(options)]);
  mergeConfig(d, secrets, store);
  console.log(
    "✓ إعدادات الذكاء محدّثة | Groq key:",
    botConfig.groqApiKey ? "موجود" : "غير موجود",
    "| معرفة:",
    botConfig.knowledge.length,
    "| منتجات:",
    botConfig.products.length,
    "| ردود:",
    botConfig.quickReplies.length,
    "| FAQ:",
    botConfig.faqs.length,
  );
}

async function refreshConfigFromSupabase(reason = "interval") {
  try {
    const botSnap = await botRef().get();
    await refreshConfig(botSnap.exists ? botSnap.data() : {}, { force: reason === "message" || reason === "config-listener" });
  } catch (e) {
    console.error(`config refresh failed (${reason}):`, e.message);
    // لا نفرّغ الإعدادات عند نفاد الكوتا؛ نكمل بآخر إعداد محفوظ في الذاكرة.
  }
}

// مهم: لا نستخدم مراقبة مباشرة على وثيقة البوت لأنها تتغير مع كل health/status heartbeat.
refreshConfigFromSupabase("startup").catch(() => {});
setInterval(() => {
  refreshConfigFromSupabase("interval").catch(() => {});
}, AI_CONFIG_REFRESH_MS).unref?.();

// ============================================================
// مطابقة فورية من إعدادات البوت (بدون Groq)
// ============================================================
function normalize(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[أإآ]/g, "ا")
    .replace(/ة/g, "ه")
    .replace(/ى/g, "ي")
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function instantMatch(text) {
  const t = normalize(text);
  if (!t) return null;

  // 1) الردود الجاهزة (trigger → reply)
  for (const q of botConfig.quickReplies) {
    const trig = normalize(q.trigger);
    const reply = textValue(q.reply, q.answer, q.a, q.text);
    if (trig && reply && (t === trig || t.includes(trig) || trig.includes(t) || tokenScore(t, trig) >= 0.5)) return reply;
  }

  // 2) الأسئلة الشائعة (تطابق قوي بالكلمات)
  let best = null;
  let bestScore = 0;
  for (const f of botConfig.faqs) {
    const q = normalize(f.q);
    if (!q) continue;
    const answer = textValue(f.a, f.answer, f.reply);
    if (!answer) continue;
    if (t === q) return answer;
    const words = q.split(" ").filter((w) => w.length > 2);
    if (!words.length) continue;
    const hit = words.filter((w) => t.includes(w)).length;
    const score = hit / words.length;
    if (score > bestScore) {
      bestScore = score;
      best = answer;
    }
  }
  if (bestScore >= 0.45) return best;

  return null;
}

function tokenScore(a, b) {
  const aw = new Set(String(a || "").split(" ").filter((w) => w.length > 2));
  const bw = String(b || "").split(" ").filter((w) => w.length > 2);
  if (!aw.size || !bw.length) return 0;
  return bw.filter((w) => aw.has(w) || [...aw].some((x) => x.includes(w) || w.includes(x))).length / bw.length;
}

function shouldHandOffToHuman(text) {
  if (!botConfig.humanHandoff || !botConfig.humanHandoffTrigger) return false;
  return normalize(text).includes(normalize(botConfig.humanHandoffTrigger));
}

function isWithinWorkingHours() {
  const wh = botConfig.workingHours;
  if (!wh?.enabled || !wh.from || !wh.to) return true;
  try {
    const timezone = wh.timezone || "Asia/Muscat";
    const formatter = new Intl.DateTimeFormat("en-GB", { timeZone: timezone, hour: "2-digit", minute: "2-digit", hour12: false });
    const parts = Object.fromEntries(formatter.formatToParts(new Date()).map((p) => [p.type, p.value]));
    const nowMinutes = Number(parts.hour) * 60 + Number(parts.minute);
    const [fromHour, fromMinute] = String(wh.from).split(":").map(Number);
    const [toHour, toMinute] = String(wh.to).split(":").map(Number);
    const fromMinutes = fromHour * 60 + fromMinute;
    const toMinutes = toHour * 60 + toMinute;
    if (fromMinutes <= toMinutes) return nowMinutes >= fromMinutes && nowMinutes <= toMinutes;
    return nowMinutes >= fromMinutes || nowMinutes <= toMinutes;
  } catch {
    return true;
  }
}

// ============================================================
// بناء system prompt من كل ما تعلّمه المستخدم
// ============================================================
function buildSystemPrompt() {
  const parts = [
    botConfig.persona,
    `\n=== هوية المتجر والوكيل ===\nاسم المتجر: ${botConfig.storeName || "المتجر"}\nاسم الوكيل: ${botConfig.botName || "المساعد"}`,
  ];
  if (botConfig.systemInstructions) parts.push("\n=== تعليمات النظام ===\n" + botConfig.systemInstructions);
  if (botConfig.tone) parts.push(`\n=== النبرة ===\n${botConfig.tone}`);
  if (botConfig.rules.length) {
    parts.push("\n=== قوانين يجب الالتزام بها ===\n" + botConfig.rules.map((r, i) => `${i + 1}. ${r}`).join("\n"));
  }
  if (botConfig.knowledge.length) {
    parts.push("\n=== قاعدة معرفة المتجر ===\n" + botConfig.knowledge.map((k) => `• ${k.title}: ${k.content}`).join("\n"));
  }
  if (botConfig.products.length) {
    parts.push("\n=== المنتجات والأسعار ===\n" + botConfig.products.map((p) => `• ${p.name}${p.price ? ` (${p.price})` : ""}: ${p.description || ""}`).join("\n"));
  }
  if (botConfig.faqs.length) {
    parts.push("\n=== أسئلة شائعة ===\n" + botConfig.faqs.map((f) => `س: ${f.q}\nج: ${f.a}`).join("\n"));
  }
  parts.push("\n=== تعليمات ===\nرد بلغة العميل، وباختصار (سطر أو سطرين). لا تخترع معلومات غير موجودة أعلاه.");
  return parts.join("\n");
}

async function askGroq(userMessage, history = []) {
  const key = String(botConfig.groqApiKey || "").trim();
  if (!key) throw new Error("GROQ_API_KEY missing (أضف مفتاح Groq في إعدادات البوت)");

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), AI_GROQ_TIMEOUT_MS);
  try {
    const res = await fetch("https://api.groq.com/openai/v1/chat/completions", {
      method: "POST",
      signal: controller.signal,
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${key}` },
      body: JSON.stringify({
        model: GROQ_MODEL,
        messages: [
          { role: "system", content: buildSystemPrompt() },
          ...history.slice(-6),
          { role: "user", content: userMessage },
        ],
        temperature: botConfig.temperature,
        max_tokens: botConfig.maxTokens,
      }),
    });
    if (!res.ok) throw new Error(`Groq ${res.status}: ${await res.text()}`);
    const j = await res.json();
    return j.choices?.[0]?.message?.content?.trim() || "";
  } catch (error) {
    if (error?.name === "AbortError") throw new Error(`Groq timeout after ${Math.round(AI_GROQ_TIMEOUT_MS / 1000)}s`);
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function loadHistory(phone) {
  try {
    const snap = await botRef()
      .collection("conversations").doc(phone)
      .collection("messages").orderBy("timestamp", "desc").limit(4).get();
    return snap.docs
      .reverse()
      .map((d) => {
        const m = d.data();
        return { role: m.fromMe ? "assistant" : "user", content: m.body || "" };
      })
      .filter((m) => m.content);
  } catch {
    return [];
  }
}

async function isFirstMessage(phone) {
  try {
    const d = await botRef().collection("customers").doc(phone).get();
    return !d.exists || (d.data().messagesCount || 0) <= 1;
  } catch {
    return false;
  }
}

// ============================================================
// معالجة رسالة واحدة من الطابور
// ============================================================
async function processJob(phone, body, msgId, chatId = null) {
  // إطلاق تحديث الإعدادات وتحميل السياق بالتوازي منذ اللحظة الأولى — أقصى سرعة ممكنة
  const configPromise = refreshConfigFromSupabase("message").catch(() => {});
  const historyPromise = loadHistory(phone).catch(() => []);
  const text = (body || "").trim();
  if (!text) return;
  await configPromise;

  // 1) المتجر/البوت مغلق أو خارج ساعات العمل — أعلى أولوية دائماً ولا يتجاوزها أي ذكاء/تحويل.
  if (!botConfig.isOpen) {
    const closed = botConfig.closedMessage || botConfig.offHoursMessage || DEFAULT_CLOSED_MESSAGE;
    await queueAiReply(phone, closed, { source: "closed", chatId });
    await markIncomingAiDone(phone, msgId, { aiResponse: closed, aiSource: "closed" });
    return;
  }
  if (!isWithinWorkingHours() && botConfig.offHoursMessage) {
    await queueAiReply(phone, botConfig.offHoursMessage, { source: "closed", chatId });
    await markIncomingAiDone(phone, msgId, { aiResponse: botConfig.offHoursMessage, aiSource: "closed" });
    return;
  }

  if (shouldHandOffToHuman(text)) {
    await logEvent("human_handoff", { phone, msgId });
    await markIncomingAiDone(phone, msgId, { aiResponse: null, aiSource: "human_handoff" });
    return;
  }

  // 2) ترحيب أول مرة (يُرسل كرسالة مستقلة سريعة)
  if (botConfig.greeting && (await isFirstMessage(phone))) {
    await queueAiReply(phone, botConfig.greeting, { source: "greeting", chatId });
  }

  // 3) مطابقة فورية من الإعدادات (بدون Groq — أسرع رد ممكن)
  const instant = instantMatch(text);
  if (instant) {
    await queueAiReply(phone, instant, { source: "instant", chatId });
    await markIncomingAiDone(phone, msgId, { aiResponse: instant, aiSource: "instant" });
    return;
  }

  // 4) الذكاء الاصطناعي (Groq) مع كامل السياق
  try {
    const history = await historyPromise;
    const reply = await askGroq(text, history);
    const finalReply = reply || botConfig.fallbackMessage;
    await queueAiReply(phone, finalReply, { source: "groq", chatId });
    await markIncomingAiDone(phone, msgId, { aiResponse: finalReply, aiSource: "groq", aiModel: GROQ_MODEL });
  } catch (e) {
    console.error("Groq failed:", e.message);
    await logEvent("groq_error", { phone, message: e.message });
    if (/GROQ_API_KEY missing/i.test(String(e.message || ""))) {
      await markIncomingAiError(phone, msgId, e.message);
      return;
    }
    await queueAiReply(phone, botConfig.fallbackMessage, { source: "fallback", chatId });
    await markIncomingAiError(phone, msgId, e.message);
  }
}

// ============================================================
// الاستماع لطابور الذكاء — onSnapshot للسرعة + polling دائم كشبكة أمان
// ============================================================
const processing = new Set();
let unsubscribeAiQueue = null;
let aiListenerRetryTimer = null;
let drainingQueue = false;

function canStartMoreJobs() {
  return processing.size < AI_MAX_CONCURRENT;
}

function startQueueDoc(doc) {
  if (!doc?.id || processing.has(doc.id) || !canStartMoreJobs()) return false;
  processing.add(doc.id);
  handleQueueDoc(doc).finally(() => processing.delete(doc.id));
  return true;
}

function attachAiQueueListener() {
  try {
    if (typeof unsubscribeAiQueue === "function") unsubscribeAiQueue();
  } catch {}
  unsubscribeAiQueue = botRef()
    .collection("aiQueue")
    .where("status", "==", "pending")
    .onSnapshot(
      (snap) => {
        snap.docChanges().forEach((change) => {
          if (change.type !== "added" && change.type !== "modified") return;
          startQueueDoc(change.doc);
        });
      },
      (err) => {
        console.error("aiQueue listener error:", err.message);
        logEvent("ai_listener_error", { message: err.message }).catch(() => {});
        clearTimeout(aiListenerRetryTimer);
        aiListenerRetryTimer = setTimeout(attachAiQueueListener, 5000);
        aiListenerRetryTimer.unref?.();
      },
    );
}

async function recoverStuckJobs() {
  const stuck = await botRef().collection("aiQueue").where("status", "==", "processing").limit(100).get();
  const cutoff = Date.now() - STALE_PROCESSING_MS;
  const writes = [];
  stuck.forEach((d) => {
    if (processing.has(d.id)) return;
    const claimed = d.data().claimedAt;
    const claimedMs = typeof claimed?.toMillis === "function" ? claimed.toMillis() : claimed instanceof Date ? claimed.getTime() : 0;
    if (claimedMs && claimedMs > cutoff) return;
    writes.push(d.ref.set({ status: "pending", recoveredAt: FieldValue.serverTimestamp() }, { merge: true }));
  });
  await Promise.all(writes);
}

async function drainPendingJobs(reason = "poll") {
  if (drainingQueue) return;
  drainingQueue = true;
  try {
    await recoverStuckJobs();
    while (canStartMoreJobs()) {
      const limit = Math.max(1, AI_MAX_CONCURRENT - processing.size);
      const pend = await botRef().collection("aiQueue").where("status", "==", "pending").limit(limit).get();
      if (pend.empty) break;
      let started = 0;
      pend.forEach((d) => {
        if (startQueueDoc(d)) started += 1;
      });
      if (!started) break;
    }
    const t = Date.now();
    if (t - lastAiHeartbeatAt >= AI_HEARTBEAT_WRITE_MS) {
      lastAiHeartbeatAt = t;
      await botRef().set(
        {
          aiWorkerStatus: "running",
          aiWorkerHeartbeatAt: FieldValue.serverTimestamp(),
          aiWorkerProcessing: processing.size,
          aiWorkerLastDrainReason: reason,
        },
        { merge: true },
      );
    }
  } catch (e) {
    console.error("ai queue drain error:", e.message);
    await logEvent("ai_queue_drain_error", { message: e.message }).catch(() => {});
  } finally {
    drainingQueue = false;
  }
}

async function handleQueueDoc(doc) {
  const ref = doc.ref;
  // نطالب بالوثيقة عبر transaction لمنع المعالجة المزدوجة
  let claimed = false;
  try {
    await botRef().firestore.runTransaction(async (tx) => {
      const fresh = await tx.get(ref);
      if (!fresh.exists) return;
      if (fresh.data().status !== "pending") return;
      tx.update(ref, { status: "processing", claimedAt: FieldValue.serverTimestamp() });
      claimed = true;
    });
  } catch (e) {
    console.error("claim failed:", e.message);
    return;
  }
  if (!claimed) return;

  const { phone, body, msgId, chatId } = doc.data() || {};
  try {
    await processJob(phone, body, msgId, chatId || null);
    await ref.set({ status: "done", doneAt: new Date() }, { merge: true });
  } catch (e) {
    console.error("processJob error:", e.message);
    await ref.set({ status: "error", error: String(e.message).slice(0, 300) }, { merge: true }).catch(() => {});
  }
}

console.log("🤖 عامل الذكاء (Groq) يعمل ويستمع لطابور الرسائل...");
attachAiQueueListener();

setInterval(() => {
  drainPendingJobs("interval").catch(() => {});
}, AI_POLL_INTERVAL_MS).unref?.();

setInterval(() => {
  recoverStuckJobs().catch((e) => console.error("recover stuck jobs error:", e.message));
}, AI_RECOVER_INTERVAL_MS).unref?.();

// عند الإقلاع، عالِج أي رسائل بقيت pending
(async () => {
  try {
    await drainPendingJobs("startup");
  } catch (e) {
    console.error("startup drain error:", e.message);
  }
})();
