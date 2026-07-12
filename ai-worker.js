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
  botRef,
  queueAiReply,
  readBotSecrets,
  logEvent,
} = require("./firestore-writer");

const GROQ_MODEL = process.env.GROQ_MODEL || "llama-3.3-70b-versatile";

// ---- إعدادات البوت الحيّة (تُحدّث لحظياً من Firestore) ----
let botConfig = {
  greeting: "",
  closedMessage: "",
  fallbackMessage: "تم استلام رسالتك، وسنرد عليك قريباً.",
  isOpen: true,
  persona: "أنت مساعد ودود في متجر إلكتروني. رد باللغة العربية بشكل قصير ومفيد.",
  rules: [],
  knowledge: [],
  quickReplies: [],
  faqs: [],
  groqApiKey: process.env.GROQ_API_KEY || "",
  language: "ar",
};

async function refreshConfig(d) {
  const secrets = await readBotSecrets();
  botConfig = {
    greeting: d.greeting || "",
    closedMessage: d.closedMessage || "",
    fallbackMessage: d.fallbackMessage || "عذراً، لم أفهم طلبك.",
    isOpen: d.isOpen !== false,
    persona: d.persona || botConfig.persona,
    rules: d.rules || [],
    knowledge: d.knowledge || [],
    quickReplies: d.quickReplies || [],
    faqs: d.faqs || [],
    groqApiKey: secrets.groqApiKey || d.groqApiKey || process.env.GROQ_API_KEY || "",
    language: d.language || "ar",
  };
  console.log("✓ إعدادات الذكاء محدّثة | Groq key:", botConfig.groqApiKey ? "موجود" : "غير موجود");
}

botRef().onSnapshot(
  (snap) => {
    if (snap.exists) refreshConfig(snap.data()).catch((e) => console.error("refreshConfig:", e.message));
  },
  (err) => console.error("config listener error:", err.message),
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

// ============================================================
// بناء system prompt من كل ما تعلّمه المستخدم
// ============================================================
function buildSystemPrompt() {
  const parts = [botConfig.persona];
  if (botConfig.rules.length) {
    parts.push("\n=== قوانين يجب الالتزام بها ===\n" + botConfig.rules.map((r, i) => `${i + 1}. ${r}`).join("\n"));
  }
  if (botConfig.knowledge.length) {
    parts.push("\n=== قاعدة معرفة المتجر ===\n" + botConfig.knowledge.map((k) => `• ${k.title}: ${k.content}`).join("\n"));
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
      temperature: 0.6,
      max_tokens: 450,
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
async function processJob(phone, body) {
  const text = (body || "").trim();
  if (!text) return;

  // 1) المتجر مغلق
  if (!botConfig.isOpen && botConfig.closedMessage) {
    await queueAiReply(phone, botConfig.closedMessage, { source: "closed" });
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
    return;
  }

  // 4) الذكاء الاصطناعي (Groq) مع كامل السياق
  try {
    const history = await loadHistory(phone);
    const reply = await askGroq(text, history);
    await queueAiReply(phone, reply || botConfig.fallbackMessage, { source: "groq" });
  } catch (e) {
    console.error("Groq failed:", e.message);
    await logEvent("groq_error", { phone, message: e.message });
    await queueAiReply(phone, botConfig.fallbackMessage, { source: "fallback" });
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

  const { phone, body } = doc.data() || {};
  try {
    await processJob(phone, body);
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
  } catch (e) {
    console.error("startup drain error:", e.message);
  }
})();
