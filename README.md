# 6

ملفات سيرفر واتساب الجاهزة للرفع على GitHub ثم Railway.

## Railway Variables

ضع هذه القيم في Railway > Variables:

```env
SERVICE_TOKEN=نفس التوكن الذي تحفظه في لوحة تيسير
GROQ_API_KEY=اختياري إذا كان مفتاح Groq محفوظاً في إعدادات البوت داخل تيسير
BOT_NAME=6
SYSTEM_PROMPT=أنت وكيل واتساب ذكي من تيسير. أجب بالعربية بوضوح، لا تخترع معلومات غير موجودة، وحوّل للمالك عند الحاجة.
GROQ_MODEL=llama-3.3-70b-versatile
FIREBASE_SERVICE_ACCOUNT=الصق JSON كامل
TAYSIR_STORE_ID=zj4KW4k2kiInawdlofxD
TAYSIR_BOT_ID=6fZIB8yfDE2QCzn21JKM
SYNC_FULL_HISTORY=true
```

## التشغيل

1. ارفع كل الملفات كما هي إلى GitHub.
2. اربط المستودع مع Railway.
3. تأكد أن Start command هو `npm start`.
4. بعد النشر انسخ رابط Railway وضعه في لوحة تيسير مع نفس `SERVICE_TOKEN`.
5. اضغط فحص الحالة / تحديث QR ثم امسح الباركود من واتساب.

## Endpoints

- `GET /status` يعرض الحالة والـ QR.
- `POST /send` يرسل رسالة واتساب.
- `POST /restart` يعيد تشغيل جلسة واتساب.
- `POST /logout` يصفّر جلسة واتساب ويفرض ظهور QR جديد.
- `POST /reset-session` نفس وظيفة التصفير عند تعلّق الحالة على reconnecting.

كل الطلبات المحمية تستخدم:

```http
Authorization: Bearer SERVICE_TOKEN
```
