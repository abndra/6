// ============================================================
// firestore-session-store.js — حفظ جلسة واتساب داخل Firestore
// ============================================================
// Railway لا يضمن بقاء ملفات القرص بعد إعادة التشغيل أو إعادة النشر.
// لذلك LocalAuth قد يجعل واتساب يظهر «آخر نشاط» أو يحتاج QR من جديد.
// هذا المتجر يستخدم RemoteAuth ويخزن ملف الجلسة مضغوطاً على أجزاء داخل Firestore.
// ============================================================

const fs = require("fs/promises");
const fsSync = require("fs");
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
    const stat = await fs.stat(zipPath);

    await ref.set(
      {
        saved: false,
        sessionName: String(session),
        byteLength: stat.size,
        savingAt: FieldValue.serverTimestamp(),
      },
      { merge: true },
    );

    await deleteChunkDocs(ref);

    let current = "";
    let index = 0;
    let batch = db.batch();
    let batchOps = 0;

    async function commitIfNeeded(force = false) {
      if (!force && batchOps < 400) return;
      if (!batchOps) return;
      await batch.commit();
      batch = db.batch();
      batchOps = 0;
    }

    async function flushChunk() {
      if (!current) return;
      batch.set(ref.collection("chunks").doc(String(index).padStart(5, "0")), {
        index,
        data: current,
      });
      index += 1;
      current = "";
      batchOps += 1;
      await commitIfNeeded(false);
    }

    async function appendEncoded(encoded) {
      let rest = encoded;
      while (rest.length) {
        const space = CHUNK_CHARS - current.length;
        current += rest.slice(0, space);
        rest = rest.slice(space);
        if (current.length >= CHUNK_CHARS) await flushChunk();
      }
    }

    let carry = Buffer.alloc(0);
    for await (const rawChunk of fsSync.createReadStream(zipPath, { highWaterMark: 384 * 1024 })) {
      const chunk = carry.length ? Buffer.concat([carry, rawChunk]) : rawChunk;
      const encodableLength = chunk.length - (chunk.length % 3);
      if (encodableLength > 0) await appendEncoded(chunk.subarray(0, encodableLength).toString("base64"));
      carry = chunk.subarray(encodableLength);
    }
    if (carry.length) await appendEncoded(carry.toString("base64"));
    await flushChunk();
    await commitIfNeeded(true);

    await ref.set(
      {
        saved: true,
        byteLength: stat.size,
        chunkCount: index,
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

    const handle = await fs.open(path, "w");
    let carry = "";
    try {
      for (const doc of snap.docs) {
        const encoded = carry + String(doc.data().data || "");
        const decodableLength = encoded.length - (encoded.length % 4);
        if (decodableLength > 0) {
          await handle.write(Buffer.from(encoded.slice(0, decodableLength), "base64"));
        }
        carry = encoded.slice(decodableLength);
      }
      if (carry) await handle.write(Buffer.from(carry, "base64"));
    } finally {
      await handle.close();
    }
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