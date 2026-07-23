// ============================================================
// firestore-compat.js — طبقة توافق قاعدة البيانات فوق Supabase
// ============================================================
// تنفّذ واجهة القراءة/الكتابة القديمة (chained collection/doc/where/…)
// لكنها تخزّن كل شيء داخل جدول واحد في Supabase:
//   public.documents(path text pk, data jsonb, updated_at timestamptz,
//                    collection text generated, parent text generated)
//
// هذا يسمح لبقية ملفات railway-server بالعمل فوق Supabase فقط.
// ============================================================

const { createClient } = require("@supabase/supabase-js");
const WebSocket = require("ws");
const { readSupabaseConfig, createSupabaseFetch, looksLikeSupabaseKey } = require("./env");

const { url: SUPABASE_URL, key: SUPABASE_KEY, keyName: SUPABASE_KEY_NAME } = readSupabaseConfig();
const SERVICE_TOKEN = process.env.SERVICE_TOKEN || "";
// ملاحظة مهمة: هذه الطبقة لا تستخدم Realtime حقيقي؛ onSnapshot هنا كان polling كل 500ms.
// هذا سبّب استهلاك egress عالي جداً على Railway/Supabase حتى بدون رسائل.
// لذلك نعطّله افتراضياً ونترك المعالجة لفواصل AI/OUTBOX الخفيفة أدناه.
const SNAPSHOT_LISTENERS_ENABLED = String(process.env.SNAPSHOT_LISTENERS_ENABLED || "false").toLowerCase() === "true";
const SNAPSHOT_POLL_INTERVAL_MS = Math.max(15000, Number(process.env.SNAPSHOT_POLL_INTERVAL_MS || 60000));

if (!SUPABASE_URL || !SUPABASE_KEY) {
  throw new Error(
    "Backend env missing: set APP_BACKEND_URL و APP_BACKEND_PUBLISHABLE_KEY في Railway Variables",
  );
}

if (!looksLikeSupabaseKey(SUPABASE_KEY)) {
  throw new Error(
    `${SUPABASE_KEY_NAME || "Supabase key"} invalid format. ضع قيمة المفتاح فقط مثل sb_publishable_... وليس السطر كاملاً KEY=value.`,
  );
}

if (!SERVICE_TOKEN) {
  console.error("SERVICE_TOKEN missing: ضعه في Railway بنفس القيمة المحفوظة داخل إعدادات البوت");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  global: { fetch: createSupabaseFetch(SUPABASE_KEY) },
  auth: { persistSession: false, autoRefreshToken: false },
  // Railway may run on Node versions without a built-in WebSocket implementation.
  // Passing an explicit transport prevents Supabase Realtime from crashing before
  // whatsapp-web.js has a chance to emit and save the QR/barcode.
  realtime: { transport: WebSocket },
});

const TABLE = "documents";

// ---- sentinel markers stored inside `data` ---------------------------------
const SENTINEL = "__fv__";
const S = {
  serverTimestamp: () => ({ [SENTINEL]: "serverTimestamp" }),
  increment: (n) => ({ [SENTINEL]: "increment", n: Number(n) || 0 }),
  arrayUnion: (...values) => ({ [SENTINEL]: "arrayUnion", values }),
  arrayRemove: (...values) => ({ [SENTINEL]: "arrayRemove", values }),
  delete: () => ({ [SENTINEL]: "delete" }),
};

function isSentinel(v) {
  return v && typeof v === "object" && !Array.isArray(v) && v[SENTINEL];
}

function resolvePatch(patch, existing = {}) {
  const now = new Date().toISOString();
  const out = Array.isArray(patch) ? [] : {};
  for (const [key, value] of Object.entries(patch || {})) {
    if (isSentinel(value)) {
      const kind = value[SENTINEL];
      if (kind === "serverTimestamp") out[key] = now;
      else if (kind === "increment") out[key] = Number(existing[key] || 0) + Number(value.n || 0);
      else if (kind === "arrayUnion") {
        const cur = Array.isArray(existing[key]) ? existing[key].slice() : [];
        for (const v of value.values) if (!cur.includes(v)) cur.push(v);
        out[key] = cur;
      } else if (kind === "arrayRemove") {
        const cur = Array.isArray(existing[key]) ? existing[key].slice() : [];
        out[key] = cur.filter((x) => !value.values.includes(x));
      } else if (kind === "delete") {
        out[key] = null;
      }
    } else if (value && typeof value === "object" && !Array.isArray(value) && !(value instanceof Date)) {
      out[key] = resolvePatch(value, (existing && existing[key]) || {});
    } else if (value instanceof Date) {
      out[key] = value.toISOString();
    } else {
      out[key] = value;
    }
  }
  return out;
}

// ---- id helpers ------------------------------------------------------------
function genId() {
  const A = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let s = "";
  for (let i = 0; i < 20; i++) s += A[Math.floor(Math.random() * A.length)];
  return s;
}

// ---- SQL layer -------------------------------------------------------------
async function dbGet(path) {
  const { data, error } = await supabase.from(TABLE).select("path, data, updated_at").eq("path", path).maybeSingle();
  if (error) throw error;
  return data;
}

async function dbUpsert(path, data) {
  const { error } = await supabase.from(TABLE).upsert({ path, data, updated_at: new Date().toISOString() });
  if (error) throw error;
}

async function dbInsertIfMissing(path, data) {
  const existing = await dbGet(path);
  if (existing) {
    const err = new Error(`ALREADY_EXISTS: ${path}`);
    err.code = 6;
    throw err;
  }
  await dbUpsert(path, data);
}

async function dbDelete(path) {
  await supabase.from(TABLE).delete().eq("path", path);
  // also delete descendants: path/*
  await supabase.from(TABLE).delete().like("path", `${path}/%`);
}

function applyFilters(builder, coll) {
  if (coll.cgroup) builder = builder.eq("collection", coll.cgroup);
  else builder = builder.eq("parent", coll.path);

  for (const w of coll.filters) {
    const col = `data->>${w.field}`;
    const jcol = `data->${w.field}`;
    if (w.op === "==") builder = builder.filter(col, "eq", String(w.value));
    else if (w.op === "!=") builder = builder.filter(col, "neq", String(w.value));
    else if (w.op === "<") builder = builder.filter(col, "lt", String(w.value));
    else if (w.op === "<=") builder = builder.filter(col, "lte", String(w.value));
    else if (w.op === ">") builder = builder.filter(col, "gt", String(w.value));
    else if (w.op === ">=") builder = builder.filter(col, "gte", String(w.value));
    else if (w.op === "in") builder = builder.filter(col, "in", `(${w.value.map((v) => String(v)).join(",")})`);
    else if (w.op === "array-contains") builder = builder.filter(jcol, "cs", JSON.stringify([w.value]));
  }
  for (const o of coll.orders) {
    builder = builder.order(`data->>${o.field}`, { ascending: o.dir === "asc" });
  }
  if (coll.limitN) builder = builder.limit(coll.limitN);
  return builder;
}

async function runQuery(coll) {
  let q = supabase.from(TABLE).select("path, data, updated_at");
  q = applyFilters(q, coll);
  const { data, error } = await q;
  if (error) throw error;
  return data || [];
}

// ---- snapshot helpers ------------------------------------------------------
function makeDocSnap(row, path) {
  const id = path.split("/").pop();
  return {
    id,
    ref: docRef(path),
    exists: row ? true : false,
    // Some legacy callers use `.exists` as a boolean property while other code
    // calls `snap.exists()`. Support both styles.
    _existsBool: !!row,
    data: () => (row ? row.data : undefined),
    get: (field) => (row && row.data ? row.data[field] : undefined),
  };
}

// snapshot returned from getDocs / onSnapshot supports .forEach, .docs, .size, .empty, .docChanges()
function makeQuerySnap(rows) {
  const docs = rows.map((r) => makeDocSnap(r, r.path));
  return {
    size: docs.length,
    empty: docs.length === 0,
    docs,
    forEach: (cb) => docs.forEach(cb),
    docChanges: () => docs.map((d) => ({ type: "added", doc: d })),
  };
}

// ---- reference builders ----------------------------------------------------
function docRef(path) {
  const id = path.split("/").pop();
  return {
    __type: "doc",
    path,
    id,
    get firestore() { return getFirestore(); },
    collection(name) {
      return collRef(`${path}/${name}`);
    },
    async get() {
      const row = await dbGet(path);
      return makeDocSnap(row, path);
    },
    async set(data, opts = {}) {
      if (opts.merge) {
        const existing = (await dbGet(path)) || { data: {} };
        const merged = { ...existing.data, ...resolvePatch(data, existing.data || {}) };
        await dbUpsert(path, merged);
      } else {
        await dbUpsert(path, resolvePatch(data));
      }
    },
    async create(data) {
      await dbInsertIfMissing(path, resolvePatch(data));
    },
    async update(data) {
      const existing = (await dbGet(path)) || { data: {} };
      const merged = { ...existing.data, ...resolvePatch(data, existing.data || {}) };
      await dbUpsert(path, merged);
    },
    async delete() {
      await dbDelete(path);
    },
  };
}

function collRef(path, cgroup) {
  const state = { __type: "coll", path, cgroup, filters: [], orders: [], limitN: undefined };
  const api = {
    __type: "coll",
    get path() { return path; },
    get cgroup() { return state.cgroup; },
    get filters() { return state.filters; },
    get orders() { return state.orders; },
    get limitN() { return state.limitN; },
    doc(id) {
      return docRef(`${path}/${id || genId()}`);
    },
    async add(data) {
      const id = genId();
      const full = `${path}/${id}`;
      await dbUpsert(full, resolvePatch(data));
      return docRef(full);
    },
    where(field, op, value) {
      state.filters.push({ field, op, value });
      return api;
    },
    orderBy(field, dir = "asc") {
      state.orders.push({ field, dir });
      return api;
    },
    limit(n) {
      state.limitN = n;
      return api;
    },
    async get() {
      const rows = await runQuery(state);
      return makeQuerySnap(rows);
    },
    async listDocuments() {
      const rows = await runQuery({ path, cgroup, filters: [], orders: [] });
      return rows.map((r) => docRef(r.path));
    },
    onSnapshot(onNext, onError) {
      if (!SNAPSHOT_LISTENERS_ENABLED) {
        // لا polling خفي افتراضياً. الدوال الدورية في ai-worker/server هي المسؤولة عن السحب.
        return () => {};
      }
      let stopped = false;
      let last = new Map(); // path -> JSON hash
      const tick = async () => {
        if (stopped) return;
        try {
          const rows = await runQuery(state);
          const next = new Map(rows.map((r) => [r.path, JSON.stringify(r.data)]));
          const changes = [];
          for (const [p, h] of next) {
            if (!last.has(p)) changes.push({ type: "added", path: p });
            else if (last.get(p) !== h) changes.push({ type: "modified", path: p });
          }
          for (const p of last.keys()) if (!next.has(p)) changes.push({ type: "removed", path: p });
          last = next;
          const docs = rows.map((r) => makeDocSnap(r, r.path));
          const snap = {
            size: docs.length,
            empty: docs.length === 0,
            docs,
            forEach: (cb) => docs.forEach(cb),
            docChanges: () => changes.map((c) => ({
              type: c.type,
              doc: makeDocSnap(rows.find((r) => r.path === c.path) || null, c.path),
            })),
          };
          onNext(snap);
        } catch (e) {
          if (onError) onError(e);
        } finally {
          if (!stopped) setTimeout(tick, SNAPSHOT_POLL_INTERVAL_MS).unref?.();
        }
      };
      setTimeout(tick, 50).unref?.();
      return () => { stopped = true; };
    },
  };
  return api;
}

// ---- batch -----------------------------------------------------------------
function makeBatch() {
  const ops = [];
  return {
    set(ref, data, opts = {}) { ops.push({ kind: opts.merge ? "merge" : "set", ref, data }); },
    create(ref, data) { ops.push({ kind: "create", ref, data }); },
    update(ref, data) { ops.push({ kind: "merge", ref, data }); },
    delete(ref) { ops.push({ kind: "delete", ref }); },
    async commit() {
      for (const op of ops) {
        if (op.kind === "delete") await op.ref.delete();
        else if (op.kind === "create") await op.ref.create(op.data);
        else if (op.kind === "merge") await op.ref.set(op.data, { merge: true });
        else await op.ref.set(op.data);
      }
    },
  };
}

function makeTransaction() {
  return {
    async get(ref) { return ref.get(); },
    set(ref, data, opts = {}) { return ref.set(data, opts); },
    update(ref, data) { return ref.update(data); },
    create(ref, data) { return ref.create(data); },
    delete(ref) { return ref.delete(); },
  };
}

// ---- top-level db + admin --------------------------------------------------
function getFirestore() {
  return {
    collection(name) { return collRef(name); },
    collectionGroup(name) { return collRef(name, name); },
    doc(path) { return docRef(path); },
    batch: makeBatch,
    async runTransaction(callback) {
      return callback(makeTransaction());
    },
  };
}

const _apps = [];
const admin = {
  get apps() { return _apps; },
  initializeApp() {
    const app = { name: "[DEFAULT]" };
    _apps.push(app);
    return app;
  },
  app() { return _apps[0] || { name: "[DEFAULT]" }; },
  credential: { cert: () => ({}) },
  firestore: Object.assign(() => getFirestore(), {
    FieldValue: S,
    Timestamp: {
      now: () => ({ seconds: Math.floor(Date.now() / 1000), nanoseconds: 0, toDate: () => new Date() }),
      fromDate: (d) => ({ seconds: Math.floor(d.getTime() / 1000), nanoseconds: 0, toDate: () => d }),
    },
  }),
};

module.exports = {
  admin,
  supabase,
  getFirestore,
  FieldValue: S,
  Timestamp: admin.firestore.Timestamp,
};
