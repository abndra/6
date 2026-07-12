// ============================================================
// server.js — خادم واتساب (جسر فقط) — لا يبني ردود الذكاء
// ============================================================
// يستقبل رسائل واتساب ويحفظها فوراً في Firestore، ثم يراقب outbox
// ويرسل الردود التي يكتبها ai-worker.js.

const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcodeTerminal = require('qrcode-terminal');
const QRCode = require('qrcode');
const express = require('express');
const {
  admin,
  botRef,
  upsertCustomer,
  saveIncomingMessage,
  saveManualOutgoing,
  markOutboxSent,
  markOutboxError,
  logEvent,
  setConnectionState,
} = require('./firestore-writer');

const SERVICE_TOKEN = process.env.SERVICE_TOKEN || '';
const BOT_NAME = process.env.BOT_NAME || "6";

const client = new Client({
  authStrategy: new LocalAuth({ dataPath: './.wwebjs_auth' }),
  puppeteer: {
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
  },
});

let latestQrRaw = null;
let latestQrDataUrl = null;
let connectionState = 'disconnected';

client.on('qr', async (qr) => {
  latestQrRaw = qr;
  connectionState = 'qr';
  qrcodeTerminal.generate(qr, { small: true });
  try {
    latestQrDataUrl = await QRCode.toDataURL(qr, { margin: 1, width: 320 });
  } catch {
    latestQrDataUrl = null;
  }
  await logEvent('qr', {});
  await setConnectionState({ lastQr: qr, connectionState: 'qr', status: 'pending', qrUpdatedAt: admin.firestore.FieldValue.serverTimestamp() });
});

client.on('ready', async () => {
  connectionState = 'connected';
  latestQrRaw = null;
  latestQrDataUrl = null;
  console.log('✓ WhatsApp متصل');
  await logEvent('connected');
  await setConnectionState({
    connectionState: 'connected',
    status: 'connected',
    waConnected: true,
    lastQr: null,
    phoneNumber: client.info && client.info.wid ? client.info.wid.user : null,
    connectedAt: admin.firestore.FieldValue.serverTimestamp(),
  });
});

client.on('auth_failure', async (m) => {
  connectionState = 'disconnected';
  await logEvent('auth_failure', { message: String(m) });
  await setConnectionState({ connectionState: 'disconnected', status: 'pending', waConnected: false });
});

client.on('disconnected', async (reason) => {
  connectionState = 'disconnected';
  await logEvent('disconnected', { reason });
  await setConnectionState({ connectionState: 'disconnected', status: 'pending', waConnected: false });
});

client.on('message', async (msg) => {
  try {
    if (!/@c\.us$/.test(msg.from || '')) return;
    if (msg.isStatus) return;
    await upsertCustomer(msg);
    await saveIncomingMessage(msg);
  } catch (e) {
    console.error('message handler error:', e);
    await logEvent('error', { where: 'incoming', message: e.message });
  }
});

client.on('message_create', async (msg) => {
  if (!msg.fromMe) return;
  if (!/@c\.us$/.test(msg.to || '')) return;
  try {
    await saveManualOutgoing(msg);
  } catch (e) {
    console.error('message_create error:', e.message);
  }
});

client.initialize();

const sending = new Set();

botRef()
  .collection('outbox')
  .where('status', '==', 'pending')
  .onSnapshot(
    async (snap) => {
      snap.docChanges().forEach((change) => {
        if (change.type !== 'added') return;
        const doc = change.doc;
        if (sending.has(doc.id)) return;
        sending.add(doc.id);
        sendOne(doc).finally(() => sending.delete(doc.id));
      });
    },
    (err) => console.error('outbox listener error:', err.message),
  );

async function sendOne(doc) {
  const data = doc.data() || {};
  const phone = data.phone;
  const text = data.text;
  const convMsgId = data.convMsgId;
  if (!phone || !text) {
    await markOutboxError(doc.id, 'missing phone/text');
    return;
  }
  if (connectionState !== 'connected') return;
  try {
    await client.sendMessage(phone + '@c.us', text);
    await markOutboxSent(doc.id, phone, convMsgId);
    console.log('→ أُرسلت رسالة إلى ' + phone);
  } catch (e) {
    console.error('send failed:', e.message);
    await markOutboxError(doc.id, e.message);
    await logEvent('send_error', { phone, message: e.message });
  }
}

client.on('ready', async () => {
  try {
    const pend = await botRef().collection('outbox').where('status', '==', 'pending').get();
    pend.forEach((d) => {
      if (sending.has(d.id)) return;
      sending.add(d.id);
      sendOne(d).finally(() => sending.delete(d.id));
    });
  } catch (e) {
    console.error('resend pending error:', e.message);
  }
});

const app = express();
app.use(express.json({ limit: '2mb' }));

function auth(req) {
  if (!SERVICE_TOKEN) return true;
  const h = req.headers.authorization || '';
  return h === 'Bearer ' + SERVICE_TOKEN;
}

app.get('/', (_req, res) => res.json({ ok: true, service: 'whatsapp-bridge', bot: BOT_NAME }));
app.get('/health', (_req, res) => res.json({ ok: true }));
app.get('/status', (_req, res) => {
  res.json({
    ok: true,
    connectionState,
    state: connectionState === 'connected' ? 'open' : connectionState,
    connected: connectionState === 'connected',
    ready: connectionState === 'connected',
    hasQr: !!latestQrRaw,
    hasQR: !!latestQrRaw,
    qr: latestQrRaw,
    qrDataUrl: latestQrDataUrl,
    qrImage: latestQrDataUrl,
  });
});

app.get('/qr', (_req, res) => res.json({ qr: latestQrRaw, qrDataUrl: latestQrDataUrl, qrImage: latestQrDataUrl }));

app.post('/send', async (req, res) => {
  if (!auth(req)) return res.status(401).json({ error: 'unauthorized' });
  if (connectionState !== 'connected') return res.status(409).json({ error: 'WhatsApp not connected', state: connectionState, qr: latestQrDataUrl });
  const to = String(req.body.to || req.body.phone || '').replace(/D/g, '');
  const message = String(req.body.message || req.body.text || '').trim();
  if (!to || !message) return res.status(400).json({ error: 'to/message required' });
  try {
    await client.sendMessage(to + '@c.us', message);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/logout', async (req, res) => {
  if (!auth(req)) return res.status(401).json({ error: 'unauthorized' });
  try {
    await client.logout().catch(() => {});
    connectionState = 'disconnected';
    await setConnectionState({ connectionState: 'disconnected', status: 'pending', waConnected: false, lastQr: null });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/restart', async (req, res) => {
  if (!auth(req)) return res.status(401).json({ error: 'unauthorized' });
  res.json({ ok: true });
  setTimeout(() => process.exit(0), 300);
});

app.post('/reset-session', async (req, res) => {
  if (!auth(req)) return res.status(401).json({ error: 'unauthorized' });
  res.json({ ok: true });
  setTimeout(() => process.exit(0), 300);
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log('WhatsApp bridge on :' + port));
