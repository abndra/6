# 6

ملفات سيرفر واتساب الجديدة للرفع على GitHub ثم Railway.

البنية: واتساب يستقبل ويحفظ في Firestore، عامل الذكاء يقرأ `aiQueue` ويكتب الرد في `outbox`، ثم واتساب يرسل الرد فوراً.

## Railway Variables

```env
SERVICE_TOKEN=نفس التوكن الذي تحفظه في لوحة تيسير
GROQ_API_KEY=اختياري إذا كان مفتاح Groq محفوظاً داخل إعدادات البوت
BOT_NAME=6
GROQ_MODEL=llama-3.3-70b-versatile
FIREBASE_SERVICE_ACCOUNT=الصق JSON كامل
# اختياريان الآن إذا حمّلت ZIP بعد حفظ البوت؛ القيم موجودة داخل bot.config.json
TAYSIR_STORE_ID=zj4KW4k2kiInawdlofxD
TAYSIR_BOT_ID=6fZIB8yfDE2QCzn21JKM
```

## التشغيل

1. ارفع كل الملفات كما هي إلى GitHub.
2. اربط المستودع مع Railway.
3. تأكد أن Start command هو `npm start`.
4. بعد النشر انسخ رابط Railway وضعه في لوحة تيسير مع نفس `SERVICE_TOKEN`.
5. اضغط فحص الحالة / تحديث QR ثم امسح الباركود من واتساب.

## Endpoints

- `GET /status` يعرض الحالة والـ QR.
- `GET /qr` يعرض QR فقط.
- `POST /send` يرسل رسالة واتساب يدوياً.
- `POST /restart` يعيد تشغيل جلسة واتساب.
- `POST /logout` يصفّر جلسة واتساب.
- `POST /reset-session` يعيد تشغيل جلسة الربط.
