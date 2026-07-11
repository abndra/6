# ربط سيرفر Railway بقاعدة بيانات تيسير — خطوات نهائية

## لماذا لا تظهر الرسائل الآن؟

صفحة المتجر (العملاء + الوارد) تقرأ من Firestore مباشرة عبر `onSnapshot`.
سيرفر Railway (whatsapp-web.js) يستقبل الرسائل من واتساب لكنه **لا يكتبها في
Firestore**، لذلك تبقى الصفحة فارغة.

الحل النهائي: نجعل السيرفر يكتب كل رسالة/عميل/حدث فور استلامه.

---

## الخطوات (5 دقائق)

### 1) توليد مفتاح Service Account
Firebase Console → ⚙️ Project Settings → **Service accounts** →
**Generate new private key** → سيتنزل ملف JSON.

### 2) إضافة المتغيرات في Railway
في Railway → مشروع البوت → **Variables** أضف:

| المتغير | القيمة |
|---|---|
| `FIREBASE_SERVICE_ACCOUNT` | الصق **محتوى ملف JSON كاملاً** (سطر واحد) |
| `TAYSIR_STORE_ID` | id المتجر من Firestore (`stores/xxxxxx`) |
| `TAYSIR_BOT_ID` | id البوت (`stores/xxx/bots/yyy`) |

### 3) تثبيت المكتبة في مستودع Railway
```bash
npm install firebase-admin
```

### 4) انسخ `firestore-writer.js` إلى جذر مستودع Railway
هو الملف الموجود بجانب هذا الدليل.

### 5) استخدمه في ملف البوت الرئيسي (`index.js` أو ما شابه)
```js
const { Client } = require("whatsapp-web.js");
const {
  upsertCustomer,
  saveMessage,
  logEvent,
  saveAiReply,
} = require("./firestore-writer");

const client = new Client({ /* ... إعداداتك ... */ });

client.on("qr", (qr) => logEvent("qr", { qr }));
client.on("ready", () => logEvent("connected"));
client.on("disconnected", (r) => logEvent("disconnected", { reason: r }));

// ✅ كل رسالة واردة → Firestore
client.on("message", async (msg) => {
  try {
    await upsertCustomer(msg);
    await saveMessage(msg);

    // ... منطق الرد بـ Groq الحالي ...
    // const reply = await askGroq(msg.body);
    // await msg.reply(reply);
    // await saveAiReply(msg.from, reply);
  } catch (e) {
    console.error("firestore write failed:", e);
    await logEvent("error", { message: e.message });
  }
});

// ✅ كل رسالة صادرة يدوياً → Firestore أيضاً
client.on("message_create", async (msg) => {
  if (!msg.fromMe) return;
  try {
    await upsertCustomer(msg);
    await saveMessage(msg);
  } catch (e) {
    console.error(e);
  }
});

client.initialize();
```

### 6) أعد نشر السيرفر على Railway
بمجرد إعادة النشر → أرسل رسالة تجريبية لرقم البوت →
ستظهر فوراً في تبويب **الوارد** و**العملاء** في لوحة تيسير.

---

## كيف تعرف أنها تعمل؟

1. افتح Firebase Console → Firestore → تصفّح:
   `stores/{STORE_ID}/bots/{BOT_ID}/conversations`
   يجب أن ترى وثائق بأرقام واتساب.
2. افتح `.../events` — ستجد سجل `connected`, `qr`, إلخ.
3. صفحة المتجر → **الوارد** → تظهر المحادثات لحظياً.

## ملاحظة أمنية
`FIREBASE_SERVICE_ACCOUNT` صلاحيته كاملة على قاعدة البيانات ويتخطى قواعد
الأمان — احتفظ به في Railway Variables فقط، لا ترفعه إلى git أبداً.