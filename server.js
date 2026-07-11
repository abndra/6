import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import qrcode from 'qrcode';
import P from 'pino';
import fs from 'fs/promises';
import makeWASocket, { DisconnectReason, fetchLatestBaileysVersion, useMultiFileAuthState } from '@whiskeysockets/baileys';
import { Boom } from '@hapi/boom';

const app = express();
app.use(cors({ origin: true }));
app.use(express.json({ limit: '2mb' }));

const PORT = process.env.PORT || 3000;
const SERVICE_TOKEN = process.env.SERVICE_TOKEN || '';
const BOT_NAME = process.env.BOT_NAME || "Taysir WhatsApp";
const SYSTEM_PROMPT = process.env.SYSTEM_PROMPT || "أنت وكيل واتساب ذكي من تيسير. أجب بالعربية بوضوح، لا تخترع معلومات غير موجودة، وحوّل للمالك عند الحاجة.";
const GROQ_API_KEY = process.env.GROQ_API_KEY || '';
const GROQ_MODEL = process.env.GROQ_MODEL || 'llama-3.3-70b-versatile';
const AUTH_DIR = process.env.AUTH_DIR || './auth';

let sock = null;
let qrDataUrl = null;
let qrText = null;
let state = 'starting';
let connected = false;
let starting = false;
let lastError = '';
let messagesCount = 0;
let sessionsCount = 0;
let reconnectFailures = 0;

function requireAuth(req, res, next) {
  if (!SERVICE_TOKEN) return res.status(500).json({ error: 'SERVICE_TOKEN missing' });
  const header = req.headers.authorization || '';
  if (header !== 'Bearer ' + SERVICE_TOKEN) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

function extractText(message) {
  return message?.conversation
    || message?.extendedTextMessage?.text
    || message?.imageMessage?.caption
    || message?.videoMessage?.caption
    || '';
}

async function askGroq(text) {
  if (!GROQ_API_KEY) return 'تم استلام رسالتك، وسنرد عليك قريباً.';
  const response = await fetch('https://api.groq.com/openai/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: 'Bearer ' + GROQ_API_KEY
    },
    body: JSON.stringify({
      model: GROQ_MODEL,
      temperature: 0.6,
      messages: [
        { role: 'system', content: SYSTEM_PROMPT },
        { role: 'user', content: text }
      ]
    })
  });
  if (!response.ok) throw new Error('Groq failed: ' + response.status);
  const data = await response.json();
  return data?.choices?.[0]?.message?.content || 'لم أفهم رسالتك، هل يمكنك توضيح المطلوب؟';
}

async function clearSession() {
  try { if (sock?.logout) await sock.logout(); } catch (_) {}
  try { if (sock?.end) sock.end(); } catch (_) {}
  sock = null;
  connected = false;
  qrDataUrl = null;
  qrText = null;
  reconnectFailures = 0;
  await fs.rm(AUTH_DIR, { recursive: true, force: true }).catch(() => {});
}

async function startWhatsApp(options = {}) {
  if (starting) return;
  starting = true;
  try {
    if (options.forceFresh) await clearSession();
    state = 'starting';
    lastError = '';
    const { state: authState, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
    const versionInfo = await fetchLatestBaileysVersion().catch(() => null);
    sock = makeWASocket({
      auth: authState,
      ...(versionInfo?.version ? { version: versionInfo.version } : {}),
      logger: P({ level: 'silent' }),
      printQRInTerminal: false,
      browser: ['Taysir WhatsApp', 'Chrome', '1.0.0'],
      syncFullHistory: false
    });

    sock.ev.on('creds.update', saveCreds);
    sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr } = update;
      if (qr) {
        qrText = qr;
        qrDataUrl = await qrcode.toDataURL(qr, { margin: 1, width: 360, errorCorrectionLevel: 'M' });
        state = 'qr';
        connected = false;
        reconnectFailures = 0;
      }
      if (connection === 'open') {
        state = 'open';
        connected = true;
        qrDataUrl = null;
        qrText = null;
        reconnectFailures = 0;
        sessionsCount += 1;
      }
      if (connection === 'close') {
        connected = false;
        const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
        reconnectFailures += 1;
        lastError = lastDisconnect?.error?.message || '';
        const needsFreshQr = !qrDataUrl && reconnectFailures >= 3 && /connection failure|restart required|timed out|closed|bad session/i.test(lastError);
        state = shouldReconnect ? (needsFreshQr ? 'resetting_for_qr' : 'reconnecting') : 'logged_out';
        if (shouldReconnect) setTimeout(() => startWhatsApp({ forceFresh: needsFreshQr }).catch(console.error), needsFreshQr ? 900 : 2500);
      }
    });

    sock.ev.on('messages.upsert', async ({ messages }) => {
      const msg = messages?.[0];
      if (!msg || msg.key.fromMe) return;
      const from = msg.key.remoteJid;
      const text = extractText(msg.message);
      if (!from || !text.trim()) return;
      try {
        const reply = await askGroq(text);
        await sock.sendMessage(from, { text: reply });
        messagesCount += 1;
      } catch (err) {
        console.error('reply failed', err);
        await sock.sendMessage(from, { text: 'حدث خطأ مؤقت، حاول مرة أخرى.' }).catch(() => {});
      }
    });
  } catch (err) {
    state = 'error';
    connected = false;
    lastError = err?.message || String(err);
    console.error(err);
  } finally {
    starting = false;
  }
}

app.get('/', (_, res) => res.json({ ok: true, bot: BOT_NAME, state, connected }));
app.get('/health', (_, res) => res.json({ ok: true }));
app.get('/status', requireAuth, (_, res) => res.json({
  ok: true,
  ready: connected,
  connected,
  state,
  qr: qrDataUrl,
  qrImage: qrDataUrl,
  qrText,
  hasQR: !!qrDataUrl,
  bot: BOT_NAME,
  messagesCount,
  sessionsCount,
  lastError
}));

app.post('/send', requireAuth, async (req, res) => {
  if (!connected || !sock) return res.status(409).json({ error: 'WhatsApp not connected', state, qr: qrDataUrl });
  const to = String(req.body.to || '').replace(/D/g, '');
  const message = String(req.body.message || '');
  if (!to || !message) return res.status(400).json({ error: 'to and message required' });
  await sock.sendMessage(to + '@s.whatsapp.net', { text: message });
  messagesCount += 1;
  res.json({ ok: true });
});

app.post('/restart', requireAuth, async (_, res) => {
  await startWhatsApp();
  res.json({ ok: true, state, qr: qrDataUrl, qrImage: qrDataUrl, hasQR: !!qrDataUrl, lastError });
});

app.post('/logout', requireAuth, async (_, res) => {
  await clearSession();
  state = 'starting';
  setTimeout(() => startWhatsApp({ forceFresh: true }).catch(console.error), 500);
  res.json({ ok: true, state: 'resetting_for_qr', hasQR: false });
});

app.post('/reset-session', requireAuth, async (_, res) => {
  await clearSession();
  state = 'starting';
  setTimeout(() => startWhatsApp({ forceFresh: true }).catch(console.error), 500);
  res.json({ ok: true, state: 'resetting_for_qr', hasQR: false });
});

startWhatsApp().catch((err) => { state = 'error'; lastError = err?.message || String(err); console.error(err); });
app.listen(PORT, () => console.log(BOT_NAME + ' running on :' + PORT));
