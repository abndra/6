// ============================================================
// firestore-session-store.js — حفظ جلسة واتساب داخل Firestore
// ============================================================
// Railway لا يضمن بقاء ملفات القرص بعد إعادة التشغيل أو إعادة النشر.
// لذلك LocalAuth قد يجعل واتساب يظهر «آخر نشاط» أو يحتاج QR من جديد.
// هذا المتجر يستخدم RemoteAuth ويخزن ملف الجلسة مضغوطاً على أجزاء داخل Firestore.
// ============================================================

const fs = require("fs/promises");
const { botRef, db, FieldValue } = require("./firestore-writer");

// أقل من حد وثيقة Firestore (1MiB) بعد حساب أسماء الحقول والـ metadata.
const CHUNK_CHARS = 700_000;

function safeSessionId(session) {
  return String(session || "default").replace(/[^a-z0-9_-]/gi, "_");
}

function sessionRef(session) {
  return botRef().collection("whatsappSessions").doc(safeSessionId(session));
}

async function deleteChunkDocs(ref) {
  const docs = await ref.collection("chunks").listDocuments();
  for (let i = 0; i < docs.length; i += 400) {
    const batch = db.batch();
    docs.slice(i, i + 400).forEach((docRef) => batch.delete(docRef));
    await batch.commit();
  }
}

class FirestoreRemoteStore {
  async sessionExists({ session }) {
    const snap = await sessionRef(session).get();
    if (!snap.exists) return false;
    const data = snap.data() || {};
    return data.saved === true && Number(data.chunkCount || 0) > 0;
  }

  async save({ session }) {
    const ref = sessionRef(session);
    const zipPath = `${session}.zip`;
    const buffer = await fs.readFile(zipPath);
    const encoded = buffer.toString("base64");
    const chunks = [];
    for (let i = 0; i < encoded.length; i += CHUNK_CHARS) {
      chunks.push(encoded.slice(i, i + CHUNK_CHARS));
    }

    await ref.set(
      {
        saved: false,
        sessionName: String(session),
        byteLength: buffer.length,
        chunkCount: chunks.length,
        savingAt: FieldValue.serverTimestamp(),
      },
      { merge: true },
    );

    await deleteChunkDocs(ref);

    for (let i = 0; i < chunks.length; i += 400) {
      const batch = db.batch();
      chunks.slice(i, i + 400).forEach((chunk, offset) => {
        const index = i + offset;
        batch.set(ref.collection("chunks").doc(String(index).padStart(5, "0")), {
          index,
          data: chunk,
        });
      });
      await batch.commit();
    }

    await ref.set(
      {
        saved: true,
        byteLength: buffer.length,
        chunkCount: chunks.length,
        updatedAt: FieldValue.serverTimestamp(),
      },
      { merge: true },
    );
  }

  async extract({ session, path }) {
    const ref = sessionRef(session);
    const meta = await ref.get();
    if (!meta.exists || meta.data()?.saved !== true) throw new Error("Remote WhatsApp session is not saved yet");

    const snap = await ref.collection("chunks").orderBy("index", "asc").get();
    const expected = Number(meta.data()?.chunkCount || 0);
    if (!snap.size || snap.size !== expected) throw new Error("Remote WhatsApp session is incomplete");

    const encoded = snap.docs.map((doc) => String(doc.data().data || "")).join("");
    await fs.writeFile(path, Buffer.from(encoded, "base64"));
  }

  async delete({ session }) {
    const ref = sessionRef(session);
    await deleteChunkDocs(ref);
    await ref.delete().catch(() => {});
  }
}

function createFirestoreRemoteStore() {
  return new FirestoreRemoteStore();
}

module.exports = { createFirestoreRemoteStore };