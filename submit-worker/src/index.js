// tyo-submit: Cloudflare Worker that receives Tag You're Out results from submit.html
// and hands them to the poller (tyo_submissions_poll.py --ci) for processing.
//
// Storage is Workers KV (binding SUBMISSIONS):
//   log:<id>      raw uploaded log bytes
//   sub:<id>      submission record (JSON), the authoritative copy
//   sha:<sha256>  id of the submission with that exact log (dedupe)
//   index         JSON array of records, newest first, capped at MAX_INDEX
//
// Public:  POST /submit, GET /status, GET /
// Poller:  GET /pending, GET /log/<id>, POST /status/<id>, POST /reindex
//          (Authorization: Bearer <POLLER_TOKEN>)
//
// See README.md for setup and the endpoint reference.

import { validateTyoText } from './validate.js';
const STATUSES = ['received', 'processed', 'failed', 'published', 'duplicate', 'superseded'];
const ID_RE = /^\d{8}T\d{4}-[0-9a-f]{8}$/;
const STATUS_LIMIT = 50;

export default {
  async fetch(request, env) {
    const origin = request.headers.get('Origin');
    const cors = corsHeaders(origin, env);
    try {
      return await route(request, env, cors, origin);
    } catch (e) {
      console.error('internal error', e && e.stack ? e.stack : e);
      return err(cors, 500, 'internal', 'Something went wrong on the server. Try again later.');
    }
  },
};

async function route(request, env, cors, origin) {
  const url = new URL(request.url);
  const path = url.pathname.replace(/\/+$/, '') || '/';
  const method = request.method.toUpperCase();

  if (method === 'OPTIONS') {
    if (origin && !cors['Access-Control-Allow-Origin']) {
      return err({}, 403, 'bad_origin', 'This origin may not call the submit service.');
    }
    return new Response(null, { status: 204, headers: cors });
  }

  if (method === 'GET' && path === '/') {
    return json(cors, 200, { ok: true, service: 'tyo-submit' });
  }
  if (method === 'POST' && path === '/submit') return submit(request, env, cors, origin);
  if (method === 'GET' && path === '/status') return publicStatus(env, cors);

  // Everything below is for the poller only.
  if (method === 'GET' && path === '/pending') {
    const denied = await authorize(request, env, cors);
    if (denied) return denied;
    const index = await readIndex(env);
    return json(cors, 200, { ok: true, submissions: index.filter(r => r.status === 'received' || r.status === 'processed') });
  }
  let m = path.match(/^\/log\/([^/]+)$/);
  if (method === 'GET' && m) {
    const denied = await authorize(request, env, cors);
    if (denied) return denied;
    if (!ID_RE.test(m[1])) return err(cors, 404, 'not_found', 'No such submission.');
    const got = await env.SUBMISSIONS.getWithMetadata('log:' + m[1], 'arrayBuffer');
    if (!got || got.value === null) return err(cors, 404, 'not_found', 'No such submission.');
    const headers = { ...cors, 'Content-Type': 'application/octet-stream', 'Cache-Control': 'no-store' };
    if (got.metadata && got.metadata.sha256) headers['X-Sha256'] = got.metadata.sha256;
    return new Response(got.value, { status: 200, headers });
  }
  m = path.match(/^\/status\/([^/]+)$/);
  if (method === 'POST' && m) {
    const denied = await authorize(request, env, cors);
    if (denied) return denied;
    return updateStatus(request, env, cors, m[1]);
  }
  if (method === 'POST' && path === '/reindex') {
    const denied = await authorize(request, env, cors);
    if (denied) return denied;
    const n = await reindex(env);
    return json(cors, 200, { ok: true, records: n });
  }
  return err(cors, 404, 'not_found', 'Unknown endpoint.');
}

// ── POST /submit ──────────────────────────────────────────────────────

async function submit(request, env, cors, origin) {
  if (!origin || !cors['Access-Control-Allow-Origin']) {
    return err(cors, 403, 'bad_origin', 'Submissions are only accepted from the TyO site.');
  }
  const maxBytes = intVar(env.MAX_BYTES, 1024 * 1024);
  const declared = Number(request.headers.get('Content-Length') || 0);
  if (declared > maxBytes + 64 * 1024) {
    return err(cors, 413, 'too_large', 'The log is too big. The limit is ' + mb(maxBytes) + '.');
  }

  let form;
  try {
    form = await request.formData();
  } catch (e) {
    return err(cors, 400, 'bad_multipart', 'The upload could not be read. Reload the page and try again.');
  }

  const file = form.get('file');
  if (!file || typeof file === 'string' || typeof file.arrayBuffer !== 'function') {
    return err(cors, 400, 'missing_file', 'No log file was attached.');
  }
  if (file.size < 1 || file.size > maxBytes) {
    return err(cors, 413, 'too_large', 'The log must be between 1 byte and ' + mb(maxBytes) + '.');
  }
  const bytes = await file.arrayBuffer();
  const text = new TextDecoder('utf-8', { ignoreBOM: true }).decode(bytes);
  const validation = validateTyoText(text);
  if (!validation.ok) return err(cors, 422, validation.code, validation.message);

  const fields = validateFields(form);
  if (fields.error) return err(cors, 400, 'bad_field', fields.error, { field: fields.field });

  const ts = await verifyTurnstile(env, form.get('cf-turnstile-response'), request.headers.get('CF-Connecting-IP'));
  if (!ts.ok) {
    return err(cors, 403, 'turnstile_failed', 'The anti-bot check failed. Complete it again and resubmit.', { codes: ts.codes });
  }

  const index = await readIndex(env);
  const cap = intVar(env.DAILY_SUBMIT_CAP, 20);
  const dayAgo = Date.now() - 24 * 3600 * 1000;
  if (index.filter(r => Date.parse(r.created) > dayAgo).length >= cap) {
    return err(cors, 429, 'rate_limited', 'Too many submissions in the last 24 hours. Try again tomorrow.');
  }

  const sha256 = await sha256Hex(bytes);
  const existing = await env.SUBMISSIONS.get('sha:' + sha256);
  const now = new Date().toISOString();
  const id = makeId();
  const rec = {
    id,
    event: fields.event,
    date: fields.date,
    submitter: fields.submitter,
    created: now,
    updated: now,
    status: 'received',
    size: bytes.byteLength,
    sha256,
    n_rounds: validation.nRounds,
    preview: fields.preview,
    summary: null,
    note: '',
  };

  // Every submission is kept on record (aizpun, 2026-10-22), so a resent
  // file gets its own record as 'duplicate'. The bytes are not stored again.
  // Sent under another cup number than the first time = REVIEW.
  if (existing) {
    const first = await readJson(env, 'sub:' + existing);
    rec.status = 'duplicate';
    rec.same_file_as = existing;
    rec.note = 'same file as ' + existing;
    if (first && first.event !== rec.event) {
      rec.note = 'REVIEW: ' + rec.note + ', which was sent as cup ' + first.event;
    }
    await env.SUBMISSIONS.put('sub:' + id, JSON.stringify(rec));
    await writeIndexWith(env, rec);
    return json(cors, 200, { ok: true, id, status: 'duplicate', same_file_as: existing,
      message: 'This exact file was already sent (' + existing + '). This submission is recorded as a duplicate.' });
  }

  await env.SUBMISSIONS.put('log:' + id, bytes, { metadata: { event: rec.event, sha256, size: rec.size } });
  await env.SUBMISSIONS.put('sub:' + id, JSON.stringify(rec));
  await env.SUBMISSIONS.put('sha:' + sha256, id);
  await writeIndexWith(env, rec);

  return json(cors, 200, { ok: true, id, status: 'received' });
}

function validateFields(form) {
  const str = k => {
    const v = form.get(k);
    return typeof v === 'string' ? v.trim() : '';
  };
  // Reject pipeline flags and control characters before trimming values.
  const unsafe = v => v.trim().startsWith('--') || /[\r\n\0]/.test(v);
  for (const [key, value] of form.entries()) {
    if (typeof value === 'string' && unsafe(value)) {
      return bad(key, 'Values must not start with "--" or contain line breaks or NUL characters.');
    }
  }

  const event = Number(str('event'));
  if (!Number.isInteger(event) || event < 58 || event > 999) return bad('event', 'Cup number must be a whole number between 58 and 999.');

  const date = str('date');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return bad('date', 'Date must be YYYY-MM-DD.');
  const dt = Date.parse(date + 'T00:00:00Z');
  if (Number.isNaN(dt) || new Date(dt).toISOString().slice(0, 10) !== date) return bad('date', 'That date does not exist.');
  const today = Date.parse(new Date().toISOString().slice(0, 10) + 'T00:00:00Z');
  if (dt < today - 60 * 86400000 || dt > today + 86400000) return bad('date', 'Date must be within the last 60 days.');

  const submitter = str('submitter');
  if (submitter.length > 40) return bad('submitter', 'Your name must be at most 40 characters.');

  let preview = null;
  const rawPreview = str('preview');
  if (rawPreview && rawPreview.length <= 2048) {
    try {
      const p = JSON.parse(rawPreview);
      preview = {
        winner: typeof p.winner === 'string' ? p.winner.slice(0, 60) : null,
        players: Number.isInteger(p.players) ? p.players : null,
        rounds: Number.isInteger(p.rounds) ? p.rounds : null,
      };
    } catch (e) { preview = null; }
  }

  return { event, date, submitter, preview };
}

function bad(field, message) {
  return { error: message, field };
}

async function verifyTurnstile(env, token, ip) {
  // Trimmed so a secret stored with a stray newline or space still works.
  const secret = (env.TURNSTILE_SECRET || '').trim();
  if (!secret) return { ok: false, codes: ['missing-secret'] };
  if (typeof token !== 'string' || !token) return { ok: false, codes: ['missing-input-response'] };
  const body = new URLSearchParams({ secret, response: token });
  if (ip) body.set('remoteip', ip);
  let out;
  try {
    const r = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', { method: 'POST', body });
    out = await r.json();
  } catch (e) {
    return { ok: false, codes: ['siteverify-unreachable'] };
  }
  if (!out || out.success !== true) return { ok: false, codes: (out && out['error-codes']) || [] };
  const allowed = csv(env.TURNSTILE_HOSTNAMES);
  if (allowed.length && !allowed.includes(out.hostname)) return { ok: false, codes: ['hostname-mismatch'] };
  return { ok: true };
}

// ── GET /status ───────────────────────────────────────────────────────

async function publicStatus(env, cors) {
  const index = await readIndex(env);
  const rows = index.slice(0, STATUS_LIMIT).map(r => ({
    id: r.id, event: r.event, date: r.date,
    submitter: r.submitter, created: r.created, updated: r.updated, status: r.status,
    n_rounds: r.n_rounds, summary: r.summary, note: r.note,
  }));
  return json({ ...cors, 'Cache-Control': 'no-store' }, 200, { ok: true, submissions: rows });
}

// ── POST /status/<id> ─────────────────────────────────────────────────

async function updateStatus(request, env, cors, id) {
  if (!ID_RE.test(id)) return err(cors, 404, 'not_found', 'No such submission.');
  let body;
  try { body = await request.json(); } catch (e) { return err(cors, 400, 'bad_json', 'Body must be JSON.'); }
  if (!body || !STATUSES.includes(body.status)) {
    return err(cors, 400, 'bad_status', 'status must be one of ' + STATUSES.join(', ') + '.');
  }
  const raw = await env.SUBMISSIONS.get('sub:' + id);
  if (!raw) return err(cors, 404, 'not_found', 'No such submission.');
  const rec = JSON.parse(raw);
  rec.status = body.status;
  if (typeof body.note === 'string') rec.note = body.note.slice(0, 500);
  if (body.summary !== undefined) rec.summary = cleanSummary(body.summary);
  rec.updated = new Date().toISOString();
  await env.SUBMISSIONS.put('sub:' + id, JSON.stringify(rec));
  await writeIndexWith(env, rec);
  return json(cors, 200, { ok: true, id, status: rec.status });
}

function cleanSummary(s) {
  if (!s || typeof s !== 'object') return null;
  const name = v => (typeof v === 'string' ? v.slice(0, 60) : null);
  const time = v => (Number.isInteger(v) || v === 'DNF' ? v : null);
  return {
    winner: name(s.winner),
    podium: Array.isArray(s.podium) ? s.podium.slice(0, 3).map(p => ({ name: name(p && p.name), time: time(p && p.time) })) : [],
    players: Number.isInteger(s.players) ? s.players : null,
    rounds: Number.isInteger(s.rounds) ? s.rounds : null,
    warnings: Array.isArray(s.warnings) ? s.warnings.filter(w => typeof w === 'string').slice(0, 20).map(w => w.slice(0, 300)) : [],
  };
}

// ── index helpers ─────────────────────────────────────────────────────
// The index is read-modify-written, so two writes landing together can drop
// one record from it. sub:<id> stays authoritative: /status/<id> re-inserts a
// missing record and /reindex rebuilds the whole index from sub: keys.

async function readJson(env, key) {
  const raw = await env.SUBMISSIONS.get(key);
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

async function readIndex(env) {
  const raw = await env.SUBMISSIONS.get('index');
  if (!raw) return [];
  try {
    const v = JSON.parse(raw);
    return Array.isArray(v) ? v : [];
  } catch (e) {
    return [];
  }
}

async function writeIndexWith(env, rec) {
  const index = (await readIndex(env)).filter(r => r.id !== rec.id);
  index.push(rec);
  index.sort((a, b) => (a.created < b.created ? 1 : a.created > b.created ? -1 : 0));
  const cap = intVar(env.MAX_INDEX, 200);
  await env.SUBMISSIONS.put('index', JSON.stringify(index.slice(0, cap)));
}

async function reindex(env) {
  const records = [];
  let cursor;
  do {
    const page = await env.SUBMISSIONS.list({ prefix: 'sub:', cursor });
    for (const k of page.keys) {
      const raw = await env.SUBMISSIONS.get(k.name);
      if (raw) {
        try { records.push(JSON.parse(raw)); } catch (e) { /* skip a corrupt record */ }
      }
    }
    cursor = page.list_complete ? undefined : page.cursor;
  } while (cursor);
  records.sort((a, b) => (a.created < b.created ? 1 : a.created > b.created ? -1 : 0));
  await env.SUBMISSIONS.put('index', JSON.stringify(records.slice(0, intVar(env.MAX_INDEX, 200))));
  return records.length;
}

// ── auth, CORS, responses ─────────────────────────────────────────────

async function authorize(request, env, cors) {
  const h = request.headers.get('Authorization') || '';
  const m = h.match(/^Bearer (.+)$/);
  if (!env.POLLER_TOKEN || !m || !(await safeEqual(m[1], env.POLLER_TOKEN))) {
    return err(cors, 401, 'unauthorized', 'Missing or wrong poller token.');
  }
  return null;
}

async function safeEqual(a, b) {
  const enc = new TextEncoder();
  // Hash both sides first so the comparison length never depends on input.
  const [ha, hb] = await Promise.all([
    crypto.subtle.digest('SHA-256', enc.encode(a)),
    crypto.subtle.digest('SHA-256', enc.encode(b)),
  ]);
  if (crypto.subtle.timingSafeEqual) return crypto.subtle.timingSafeEqual(ha, hb);
  const x = new Uint8Array(ha), y = new Uint8Array(hb);
  let diff = 0;
  for (let i = 0; i < x.length; i++) diff |= x[i] ^ y[i];
  return diff === 0;
}

function corsHeaders(origin, env) {
  const h = { Vary: 'Origin' };
  if (origin && csv(env.ALLOWED_ORIGINS).includes(origin)) {
    h['Access-Control-Allow-Origin'] = origin;
    h['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS';
    h['Access-Control-Allow-Headers'] = 'Content-Type, Authorization';
    h['Access-Control-Max-Age'] = '86400';
  }
  return h;
}

function json(headers, status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...headers, 'Content-Type': 'application/json; charset=utf-8' },
  });
}

function err(headers, status, code, message, details) {
  const body = { ok: false, error: code, message };
  if (details) body.details = details;
  return json(headers, status, body);
}

// ── small utilities ───────────────────────────────────────────────────

function csv(v) {
  return String(v || '').split(',').map(s => s.trim()).filter(Boolean);
}

function intVar(v, dflt) {
  const n = parseInt(v, 10);
  return Number.isFinite(n) && n > 0 ? n : dflt;
}

function mb(n) {
  return Math.round(n / 1024 / 1024) + ' MB';
}

function hex(buf) {
  return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, '0')).join('');
}

async function sha256Hex(bytes) {
  return hex(await crypto.subtle.digest('SHA-256', bytes));
}

function makeId() {
  const t = new Date().toISOString(); // 2026-09-27T21:01:33.123Z
  const stamp = t.slice(0, 4) + t.slice(5, 7) + t.slice(8, 10) + 'T' + t.slice(11, 13) + t.slice(14, 16);
  return stamp + '-' + hex(crypto.getRandomValues(new Uint8Array(4)));
}
