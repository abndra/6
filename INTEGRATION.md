# خادم Railway لتيسير — البنية الجديدة

## آلية العمل

1. `server.js` يستقبل رسالة واتساب ويحفظها فوراً في Firestore.
2. يضيف الرسالة إلى `aiQueue`.
3. `ai-worker.js` يستمع إلى `aiQueue` لحظياً.
4. العامل يبحث في الردود الجاهزة والأسئلة الشائعة ومعرفة البوت؛ وإن لم يجد يستدعي Groq.
5. يكتب الرد في `outbox`.
6. `server.js` يستمع إلى `outbox` ويرسل الرد فوراً إلى واتساب.

## Railway Variables

```env
FIREBASE_SERVICE_ACCOUNT=الصق JSON كامل
TAYSIR_STORE_ID=zj4KW4k2kiInawdlofxD
TAYSIR_BOT_ID=6fZIB8yfDE2QCzn21JKM
SERVICE_TOKEN=نفس التوكن في لوحة تيسير
GROQ_API_KEY=اختياري إذا كان محفوظاً داخل تيسير
BOT_NAME=6
GROQ_MODEL=llama-3.3-70b-versatile
```

## التشغيل

```bash
npm install
npm start
```

بعد النشر انسخ رابط Railway إلى لوحة تيسير، ثم افحص الحالة وامسح QR.
