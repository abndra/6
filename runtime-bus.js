// ============================================================
// runtime-bus.js — إشارات فورية داخل نفس عملية Railway
// ============================================================
// عندما يعمل index.js كخدمة واحدة، هذا الجسر يختصر انتظار polling:
// - saveIncomingMessage يرسل إشارة لعامل الذكاء فور إنشاء aiQueue.
// - queueOutgoingMessage يرسل إشارة لخادم واتساب فور إنشاء outbox.
// عند تشغيل الجسر والذكاء كخدمتين منفصلتين يبقى polling الاحتياطي يعمل طبيعياً.
// ============================================================
const { EventEmitter } = require("events");

const bus = global.__TAYSIR_RUNTIME_BUS__ || new EventEmitter();
bus.setMaxListeners(50);
global.__TAYSIR_RUNTIME_BUS__ = bus;

module.exports = bus;