// ============================================================
// firestore-session-store.js — حفظ جلسة واتساب داخل Supabase
// ============================================================
// نستخدم مجموعة `whatsappSessions/default` تحت البوت. الجلسة كلها
// (base64 من ملف zip) تُحفظ في وثيقة واحدة داخل حقل data — لا حاجة
// للتقسيم لأن jsonb في Postgres يقبل قيمًا كبيرة.
// ============================================================

const fs = require("fs/promises");
const { botRef, FieldValue } = require("./firestore-writer");

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
    const zipPath = `${session}.zip`;
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

module.exports = { createFirestoreRemoteStore, deleteRemoteSessionById };
