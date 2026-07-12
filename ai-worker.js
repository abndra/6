// ============================================================
// ai-worker.js — عامل الذكاء الاصطناعي (Groq) — منفصل تماماً عن واتساب
// ============================================================
// مسؤوليته الوحيدة:
//   يستمع لطابور الذكاء (aiQueue) في Firestore. عند وصول رسالة جديدة:
//     1) يقرأها بسرعة.
//     2) يبحث في إعدادات البوت: هل مغلق؟ هل يوجد رد جاهز/سؤال شائع مطابق؟
//        (رد فوري بدون استدعاء أي API — سرعة قصوى).
//     3) إن لم يجد شيئاً → يستدعي Groq مع قاعدة المعرفة والقوانين كسياق.
//     4) يكتب الرد في Firestore (المحادثة + طابور الإرسال outbox).
//   خادم واتساب هو من يلتقط الرد من outbox ويرسله.
//
// مفتاح Groq يُقرأ لكل بوت من إعداداته في Firestore (botSecrets/{botId}.groqApiKey
// أو bots/{botId}.groqApiKey)، ويمكن أيضاً تعيينه في GROQ_API_KEY كخيار احتياطي.
//
// dependencies: firebase-admin node-fetch@2
// ============================================================

const fetch = require("node-fetch");
const {
  storeRef,
  botRef,
  botSecretsRef,
  queueAiReply,
  readBotSecrets,
  readStoreConfig,
  markIncomingAiDone,
  markIncomingAiError,
  logEvent,
} = require("./firestore-writer");

const GROQ_MODEL = process.env.GROQ_MODEL || "llama-3.3-70b-versatile";
const STALE_PROCESSING_MS = Number(process.env.AI_STALE_PROCESSING_MS || 120000);

// ---- إعدادات البوت الحيّة (تُحدّث لحظياً من Firestore) ----
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
  groqApiKey: process.env.GROQ_API_KEY || "",
  language: "ar",
  temperature: 0.4,
  maxTokens: 350,
  workingHours: null,
  offHoursMessage: "",
  humanHandoff: false,
  humanHandoffTrigger: "",
};

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
  const previousGroqKey = botConfig.groqApiKey || process.env.GROQ_API_KEY || "";
  const storeName = textValue(store.name, store.storeName, store.slug, "المتجر");
  const defaultPersona = `أنت مساعد ذكي لمتجر ${storeName}. رد بلغة العميل باختصار ووضوح، واعتمد فقط على معلومات المتجر المحفوظة.`;
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
    groqApiKey: textValue(secrets.groqApiKey, d.groqApiKey, store.groqApiKey, process.env.GROQ_API_KEY, previousGroqKey),
    language: textValue(d.language, store.language, "ar"),
    temperature: typeof d.temperature === "number" ? Math.max(0, Math.min(1, d.temperature)) : botConfig.temperature,
    maxTokens: typeof d.maxTokens === "number" ? Math.max(80, Math.min(1200, d.maxTokens)) : botConfig.maxTokens,
    workingHours: d.workingHours || store.workingHours || null,
    offHoursMessage: textValue(d.offHoursMessage, d.closedMessage, store.offHoursMessage, store.closedMessage),
    humanHandoff: !!(d.humanHandoff || store.humanHandoff),
    humanHandoffTrigger: textValue(d.humanHandoffTrigger, store.humanHandoffTrigger),
  };
}

async function refreshConfig(d) {
  const [secrets, store] = await Promise.all([readBotSecrets(), readStoreConfig()]);
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

botRef().onSnapshot(
  (snap) => {
    if (snap.exists) refreshConfig(snap.data()).catch((e) => console.error("refreshConfig:", e.message));
  },
  (err) => console.error("config listener error:", err.message),
);

botSecretsRef().onSnapshot(
  async (snap) => {
    const [botSnap, store] = await Promise.all([botRef().get(), readStoreConfig()]);
    mergeConfig(botSnap.exists ? botSnap.data() : {}, snap.exists ? snap.data() : {}, store);
    console.log("✓ أسرار البوت محدّثة | Groq key:", botConfig.groqApiKey ? "موجود" : "غير موجود");
  },
  (err) => console.error("botSecrets listener error:", err.message),
);

storeRef().onSnapshot(
  async (snap) => {
    const [botSnap, secrets] = await Promise.all([botRef().get(), readBotSecrets()]);
    mergeConfig(botSnap.exists ? botSnap.data() : {}, secrets, snap.exists ? snap.data() : {});
    console.log("✓ إعدادات المتجر محدّثة | Groq key:", botConfig.groqApiKey ? "موجود" : "غير موجود");
  },
  (err) => console.error("store listener error:", err.message),
);

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
    if (trig && (t === trig || t.includes(trig) || trig.includes(t))) return q.reply;
  }

  // 2) الأسئلة الشائعة (تطابق قوي بالكلمات)
  let best = null;
  let bestScore = 0;
  for (const f of botConfig.faqs) {
    const q = normalize(f.q);
    if (!q) continue;
    if (t === q) return f.a;
    const words = q.split(" ").filter((w) => w.length > 2);
    if (!words.length) continue;
    const hit = words.filter((w) => t.includes(w)).length;
    const score = hit / words.length;
    if (score > bestScore) {
      bestScore = score;
      best = f.a;
    }
  }
  if (bestScore >= 0.7) return best;

  return null;
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
  const parts = [botConfig.persona];
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

  const res = await fetch("https://api.groq.com/openai/v1/chat/completions", {
    method: "POST",
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
}

async function loadHistory(phone) {
  try {
    const snap = await botRef()
      .collection("conversations").doc(phone)
      .collection("messages").orderBy("timestamp", "desc").limit(8).get();
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
async function processJob(phone, body, msgId) {
  const text = (body || "").trim();
  if (!text) return;

  if (shouldHandOffToHuman(text)) {
    await logEvent("human_handoff", { phone, msgId });
    await markIncomingAiDone(phone, msgId, { aiResponse: null, aiSource: "human_handoff" });
    return;
  }

  // 1) المتجر/البوت مغلق أو خارج ساعات العمل
  if (!botConfig.isOpen && botConfig.closedMessage) {
    await queueAiReply(phone, botConfig.closedMessage, { source: "closed" });
    await markIncomingAiDone(phone, msgId, { aiResponse: botConfig.closedMessage, aiSource: "closed" });
    return;
  }
  if (!isWithinWorkingHours() && botConfig.offHoursMessage) {
    await queueAiReply(phone, botConfig.offHoursMessage, { source: "closed" });
    await markIncomingAiDone(phone, msgId, { aiResponse: botConfig.offHoursMessage, aiSource: "closed" });
    return;
  }

  // 2) ترحيب أول مرة (يُرسل كرسالة مستقلة سريعة)
  if (botConfig.greeting && (await isFirstMessage(phone))) {
    await queueAiReply(phone, botConfig.greeting, { source: "greeting" });
  }

  // 3) مطابقة فورية من الإعدادات (بدون Groq — أسرع رد ممكن)
  const instant = instantMatch(text);
  if (instant) {
    await queueAiReply(phone, instant, { source: "instant" });
    await markIncomingAiDone(phone, msgId, { aiResponse: instant, aiSource: "instant" });
    return;
  }

  // 4) الذكاء الاصطناعي (Groq) مع كامل السياق
  try {
    const history = await loadHistory(phone);
    const reply = await askGroq(text, history);
    const finalReply = reply || botConfig.fallbackMessage;
    await queueAiReply(phone, finalReply, { source: "groq" });
    await markIncomingAiDone(phone, msgId, { aiResponse: finalReply, aiSource: "groq", aiModel: GROQ_MODEL });
  } catch (e) {
    console.error("Groq failed:", e.message);
    await logEvent("groq_error", { phone, message: e.message });
    await queueAiReply(phone, botConfig.fallbackMessage, { source: "fallback" });
    await markIncomingAiError(phone, msgId, e.message);
  }
}

// ============================================================
// الاستماع لطابور الذكاء — سرعة فورية عبر onSnapshot
// ============================================================
const processing = new Set();

botRef()
  .collection("aiQueue")
  .where("status", "==", "pending")
  .onSnapshot(
    (snap) => {
      snap.docChanges().forEach((change) => {
        if (change.type !== "added") return;
        const doc = change.doc;
        if (processing.has(doc.id)) return;
        processing.add(doc.id);
        handleQueueDoc(doc).finally(() => processing.delete(doc.id));
      });
    },
    (err) => console.error("aiQueue listener error:", err.message),
  );

async function handleQueueDoc(doc) {
  const ref = doc.ref;
  // نطالب بالوثيقة عبر transaction لمنع المعالجة المزدوجة
  let claimed = false;
  try {
    await botRef().firestore.runTransaction(async (tx) => {
      const fresh = await tx.get(ref);
      if (!fresh.exists) return;
      if (fresh.data().status !== "pending") return;
      tx.update(ref, { status: "processing", claimedAt: new Date() });
      claimed = true;
    });
  } catch (e) {
    console.error("claim failed:", e.message);
    return;
  }
  if (!claimed) return;

  const { phone, body, msgId } = doc.data() || {};
  try {
    await processJob(phone, body, msgId);
    await ref.set({ status: "done", doneAt: new Date() }, { merge: true });
  } catch (e) {
    console.error("processJob error:", e.message);
    await ref.set({ status: "error", error: String(e.message).slice(0, 300) }, { merge: true }).catch(() => {});
  }
}

console.log("🤖 عامل الذكاء (Groq) يعمل ويستمع لطابور الرسائل...");

// عند الإقلاع، عالِج أي رسائل بقيت pending
(async () => {
  try {
    const pend = await botRef().collection("aiQueue").where("status", "==", "pending").get();
    pend.forEach((d) => {
      if (processing.has(d.id)) return;
      processing.add(d.id);
      handleQueueDoc(d).finally(() => processing.delete(d.id));
    });
    const stuck = await botRef().collection("aiQueue").where("status", "==", "processing").get();
    const cutoff = Date.now() - STALE_PROCESSING_MS;
    stuck.forEach((d) => {
      const claimed = d.data().claimedAt;
      const claimedMs = typeof claimed?.toMillis === "function" ? claimed.toMillis() : claimed instanceof Date ? claimed.getTime() : 0;
      if (claimedMs && claimedMs > cutoff) return;
      d.ref.set({ status: "pending", recoveredAt: new Date() }, { merge: true }).catch((e) => console.error("recover stuck job:", e.message));
    });
  } catch (e) {
    console.error("startup drain error:", e.message);
  }
})();
