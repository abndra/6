// ============================================================
// ai-worker.js — عامل الذكاء الاصطناعي (Groq) منفصل عن واتساب
// ============================================================

const fetch = require('node-fetch');
const { botRef, queueAiReply, readBotSecrets, logEvent } = require('./firestore-writer');

const GROQ_MODEL = process.env.GROQ_MODEL || 'llama-3.3-70b-versatile';

let botConfig = {
  greeting: '',
  closedMessage: '',
  fallbackMessage: 'تم استلام رسالتك، وسنرد عليك قريباً.',
  isOpen: true,
  persona: "أنت وكيل واتساب ذكي من تيسير. أجب بالعربية بوضوح، لا تخترع معلومات غير موجودة، وحوّل للمالك عند الحاجة.",
  rules: [],
  knowledge: [],
  quickReplies: [],
  faqs: [],
  groqApiKey: process.env.GROQ_API_KEY || '',
  language: 'ar',
};

async function refreshConfig(d) {
  const secrets = await readBotSecrets();
  botConfig = {
    greeting: d.greeting || d.welcomeMessage || '',
    closedMessage: d.closedMessage || '',
    fallbackMessage: d.fallbackMessage || 'عذراً، لم أفهم طلبك.',
    isOpen: d.isOpen !== false,
    persona: d.persona || d.systemPrompt || botConfig.persona,
    rules: d.rules || [],
    knowledge: d.knowledge || [],
    quickReplies: d.quickReplies || [],
    faqs: d.faqs || [],
    groqApiKey: secrets.groqApiKey || d.groqApiKey || process.env.GROQ_API_KEY || '',
    language: d.language || 'ar',
  };
  console.log('✓ إعدادات الذكاء محدّثة | Groq key:', botConfig.groqApiKey ? 'موجود' : 'غير موجود');
}

botRef().onSnapshot(
  (snap) => {
    if (snap.exists) refreshConfig(snap.data()).catch((e) => console.error('refreshConfig:', e.message));
  },
  (err) => console.error('config listener error:', err.message),
);

function normalize(s) {
  return String(s || '')
    .toLowerCase()
    .replace(/[أإآ]/g, 'ا')
    .replace(/ة/g, 'ه')
    .replace(/ى/g, 'ي')
    .replace(/[^\u0600-\u06FFa-z0-9\s]/gi, ' ')
    .replace(/s+/g, ' ')
    .trim();
}

function instantMatch(text) {
  const t = normalize(text);
  if (!t) return null;

  for (const q of botConfig.quickReplies) {
    const trig = normalize(q.trigger);
    if (trig && (t === trig || t.includes(trig) || trig.includes(t))) return q.reply;
  }

  let best = null;
  let bestScore = 0;
  for (const f of botConfig.faqs) {
    const q = normalize(f.q);
    if (!q) continue;
    if (t === q) return f.a;
    const words = q.split(' ').filter((w) => w.length > 2);
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

function buildSystemPrompt() {
  const parts = [botConfig.persona];
  if (botConfig.rules.length) parts.push('
=== قوانين يجب الالتزام بها ===
' + botConfig.rules.map((r, i) => String(i + 1) + '. ' + r).join('
'));
  if (botConfig.knowledge.length) parts.push('
=== قاعدة معرفة المتجر ===
' + botConfig.knowledge.map((k) => '• ' + (k.title || '') + ': ' + (k.content || '')).join('
'));
  if (botConfig.faqs.length) parts.push('
=== أسئلة شائعة ===
' + botConfig.faqs.map((f) => 'س: ' + f.q + '
ج: ' + f.a).join('
'));
  parts.push('
=== تعليمات ===
رد بلغة العميل، وباختصار. افهم القصد حتى لو كانت صياغة العميل مختلفة، ولا تخترع معلومات غير موجودة أعلاه.');
  return parts.join('
');
}

async function askGroq(userMessage, history = []) {
  const key = String(botConfig.groqApiKey || '').trim();
  if (!key) throw new Error('GROQ_API_KEY missing');

  const res = await fetch('https://api.groq.com/openai/v1/chat/completions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + key },
    body: JSON.stringify({
      model: GROQ_MODEL,
      messages: [
        { role: 'system', content: buildSystemPrompt() },
        ...history.slice(-6),
        { role: 'user', content: userMessage },
      ],
      temperature: 0.6,
      max_tokens: 450,
    }),
  });
  if (!res.ok) throw new Error('Groq ' + res.status + ': ' + (await res.text()).slice(0, 300));
  const j = await res.json();
  return (j.choices && j.choices[0] && j.choices[0].message && j.choices[0].message.content || '').trim();
}

async function loadHistory(phone) {
  try {
    const snap = await botRef().collection('conversations').doc(phone).collection('messages').orderBy('timestamp', 'desc').limit(8).get();
    return snap.docs.reverse().map((d) => {
      const m = d.data();
      return { role: m.fromMe ? 'assistant' : 'user', content: m.body || '' };
    }).filter((m) => m.content);
  } catch {
    return [];
  }
}

async function isFirstMessage(phone) {
  try {
    const d = await botRef().collection('customers').doc(phone).get();
    return !d.exists || (d.data().messagesCount || 0) <= 1;
  } catch {
    return false;
  }
}

async function processJob(phone, body) {
  const text = String(body || '').trim();
  if (!text) return;

  if (!botConfig.isOpen && botConfig.closedMessage) {
    await queueAiReply(phone, botConfig.closedMessage, { source: 'closed' });
    return;
  }

  if (botConfig.greeting && (await isFirstMessage(phone))) {
    await queueAiReply(phone, botConfig.greeting, { source: 'greeting' });
  }

  const instant = instantMatch(text);
  if (instant) {
    await queueAiReply(phone, instant, { source: 'instant' });
    return;
  }

  try {
    const history = await loadHistory(phone);
    const reply = await askGroq(text, history);
    await queueAiReply(phone, reply || botConfig.fallbackMessage, { source: 'groq' });
  } catch (e) {
    console.error('Groq failed:', e.message);
    await logEvent('groq_error', { phone, message: e.message });
    await queueAiReply(phone, botConfig.fallbackMessage, { source: 'fallback' });
  }
}

const processing = new Set();

botRef()
  .collection('aiQueue')
  .where('status', '==', 'pending')
  .onSnapshot(
    (snap) => {
      snap.docChanges().forEach((change) => {
        if (change.type !== 'added') return;
        const doc = change.doc;
        if (processing.has(doc.id)) return;
        processing.add(doc.id);
        handleQueueDoc(doc).finally(() => processing.delete(doc.id));
      });
    },
    (err) => console.error('aiQueue listener error:', err.message),
  );

async function handleQueueDoc(doc) {
  const ref = doc.ref;
  let claimed = false;
  try {
    await botRef().firestore.runTransaction(async (tx) => {
      const fresh = await tx.get(ref);
      if (!fresh.exists) return;
      if (fresh.data().status !== 'pending') return;
      tx.update(ref, { status: 'processing', claimedAt: new Date() });
      claimed = true;
    });
  } catch (e) {
    console.error('claim failed:', e.message);
    return;
  }
  if (!claimed) return;

  const data = doc.data() || {};
  try {
    await processJob(data.phone, data.body);
    await ref.set({ status: 'done', doneAt: new Date() }, { merge: true });
  } catch (e) {
    await ref.set({ status: 'error', error: e.message, erroredAt: new Date() }, { merge: true });
  }
}

console.log('AI worker listening for pending aiQueue jobs');
