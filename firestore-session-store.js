// ============================================================
// firestore-session-store.js — حفظ جلسة واتساب داخل Supabase
// ============================================================
// نستخدم مجموعة `whatsappSessions/default` تحت البوت. الجلسة كلها
// (base64 من ملف zip) تُحفظ في وثيقة واحدة داخل حقل data — لا حاجة
// للتقسيم لأن jsonb في Postgres يقبل قيمًا كبيرة.
// ============================================================

const fs = require("fs/promises");
const path = require("path");
const { botRef, FieldValue } = require("./firestore-writer");

// خطأ ناعم: ملف الـ zip لم يجهز بعد (تهيئة المتصفح/الجلسة). ليس عطلاً حقيقياً.
class SessionZipNotReadyError extends Error {
  constructor(message) {
    super(message);
    this.name = "SessionZipNotReadyError";
    this.benign = true;
  }
}

async function fileExists(p) {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

// ننتظر ظهور ملف الـ zip الذي تنشئه RemoteAuth قبل رفعه. في أول دقائق بعد الربط
// قد يتأخر إنشاؤه لحظة، فبدل الفشل الفوري (ENOENT) ننتظر قليلاً ثم نقرأه.
async function waitForZip(zipPath, { tries = 8, delayMs = 400 } = {}) {
  for (let i = 0; i < tries; i += 1) {
    if (await fileExists(zipPath)) return true;
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
  return false;
}

function safeSessionId(session) {
  return String(session || "default").replace(/[^a-z0-9_-]/gi, "_");
}

function sessionRef(session) {
  return botRef().collection("whatsappSessions").doc(safeSessionId(session));
}

class FirestoreRemoteStore {
  async sessionExists({ session }) {
    try {
      const snap = await sessionRef(session).get();
      if (!snap.exists) return false;
      const data = snap.data() || {};
      return data.saved === true && typeof data.blob === "string" && data.blob.length > 0;
    } catch (e) {
      console.error("remote session check failed, forcing fresh QR:", e.message);
      return false;
    }
  }

  async save({ session }) {
    const ref = sessionRef(session);
    // نحسم المسار المطلق بالنسبة لمجلد العمل حتى لا يختلف عن حيث تنشئه RemoteAuth.
    const zipPath = path.resolve(process.cwd(), `${session}.zip`);
    // ننتظر ظهور الملف بدل الفشل فوراً بـ ENOENT في أول دقائق بعد الربط.
    const ready = await waitForZip(zipPath);
    if (!ready) {
      // خطأ ناعم: ستُعاد المحاولة في دورة النسخ الاحتياطي التالية بنجاح.
      throw new SessionZipNotReadyError(`session zip not ready yet: ${session}.zip`);
    }
    const buf = await fs.readFile(zipPath);
    const blob = buf.toString("base64");
    await ref.set(
      {
        saved: true,
        sessionName: String(session),
        byteLength: buf.length,
        blob,
        updatedAt: FieldValue.serverTimestamp(),
      },
      { merge: true },
    );
  }


  async extract({ session, path }) {
    const ref = sessionRef(session);
    const snap = await ref.get();
    const data = snap.exists ? snap.data() : null;
    if (!data || data.saved !== true || !data.blob) {
      throw new Error("Remote WhatsApp session is not saved yet");
    }
    await fs.writeFile(path, Buffer.from(String(data.blob), "base64"));
  }

  async delete({ session }) {
    await sessionRef(session).delete().catch(() => {});
  }
}

function createFirestoreRemoteStore() {
  return new FirestoreRemoteStore();
}

async function deleteRemoteSessionById(session) {
  await sessionRef(session).delete().catch(() => {});
}

module.exports = { createFirestoreRemoteStore, deleteRemoteSessionById, SessionZipNotReadyError };
