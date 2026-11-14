// env-vault.js (RELAY)
// -----------------------------------------------------------------------------
// ENV VAULT: isang admin page (env.html) kung saan inilalagay ang mga key/secret ng RELAY, kapalit o dagdag ng Environment
// ng Render. Naka-hide ang values (write-only: hindi na ito ibinabalik sa browser kailanman).
//   * Prioridad: ang ENV ng host (Render) at .env file ang NANANAIG. Ang vault ay pumupuno lang ng mga variable na WALA.
//   * Naka-encrypt (AES-256-GCM) sa local file relay-env-vault.json, at may kopya sa database (relay_kv_store) para
//     hindi mawala sa Render redeploy/restart (ephemeral filesystem).
//   * Ang encryption key ay galing sa (1) RELAY_ENV_MASTER_KEY, o (2) RELAY private key (env o relay-private-key.pem),
//     o (3) local .relay-env.key (hindi stable: walang database copy).
//   * Nilo-load nang SYNC sa pinakaitaas ng server.js, bago basahin ng kahit anong module ang process.env.
// HINDI puwedeng ilagay sa vault: RELAY_PRIVATE_KEY_PEM at RELAY_ENV_MASTER_KEY (sila ang susi nito), at mga system
// variable (PATH, NODE_OPTIONS...). Ang DATABASE_URL / REDIS_URL ay pwedeng ilagay pero para sa database copy kailangan pa rin
// itong nasa host ENV (para makuha ang vault pagka-redeploy).
// -----------------------------------------------------------------------------
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { execFileSync } = require('child_process');

const VAULT_FILE = 'relay-env-vault.json';
const KEY_FILE = '.relay-env.key';
const DB_KEY = 'env-vault';
const NAME_RE = /^[A-Z][A-Z0-9_]{1,79}$/;
const MAX_VALUE_LEN = 20000;
const NEVER_VAULT = new Set([
    'RELAY_PRIVATE_KEY_PEM', 'RELAY_ENV_MASTER_KEY',
    'PATH', 'HOME', 'PWD', 'SHELL', 'USER', 'NODE_OPTIONS', 'NODE_PATH', 'LD_PRELOAD', 'LD_LIBRARY_PATH', 'NODE_EXTRA_CA_CERTS'
]);
const BOOTSTRAP = new Set(['DATABASE_URL', 'RELAY_DEVICES_DATABASE_URL', 'REDIS_URL', 'RELAY_ADMIN_KEY']);
const PLATFORM = new Set(['PORT', 'NODE_ENV', 'RENDER', 'TMPDIR', 'TEMP', 'TMP', 'HOSTNAME', 'LANG', 'TZ', 'UV_THREADPOOL_SIZE', 'CI']);

const state = {
    dir: null, values: {}, hostKeys: new Set(), applied: new Set(),
    key: null, keyMode: 'none', stable: false, loadedFrom: null, updatedAt: null, loadError: null
};

function has(v) { return v !== undefined && v !== null && String(v) !== ''; }

function deriveKey(dir) {
    const norm = (s) => String(s).replace(/\r\n/g, '\n').trim();
    let material = null, mode = 'none', stable = true;
    if (has(process.env.RELAY_ENV_MASTER_KEY) && String(process.env.RELAY_ENV_MASTER_KEY).length >= 16) {
        material = String(process.env.RELAY_ENV_MASTER_KEY); mode = 'master-key';
    } else if (has(process.env.RELAY_PRIVATE_KEY_PEM)) {
        material = norm(String(process.env.RELAY_PRIVATE_KEY_PEM).replace(/\\n/g, '\n')); mode = 'private-key-env';
    } else {
        try { material = norm(fs.readFileSync(path.join(dir, 'relay-private-key.pem'), 'utf8')); mode = 'private-key-file'; } catch (_e) { /* wala */ }
    }
    if (!material) {
        const kp = path.join(dir, KEY_FILE);
        try { material = fs.readFileSync(kp, 'utf8').trim(); } catch (_e) { /* gagawa */ }
        if (!material) {
            material = crypto.randomBytes(32).toString('hex');
            try { fs.writeFileSync(kp, material, { mode: 0o600 }); } catch (_e) { /* read-only fs */ }
        }
        mode = 'local-key-file'; stable = false;
    }
    const key = Buffer.from(crypto.hkdfSync('sha256', Buffer.from(material), Buffer.from('omnipos-relay-env-vault'), Buffer.from('v1'), 32));
    return { key, mode, stable };
}

function encrypt(values, key, updatedAt) {
    const iv = crypto.randomBytes(12);
    const c = crypto.createCipheriv('aes-256-gcm', key, iv);
    const data = Buffer.concat([c.update(Buffer.from(JSON.stringify({ values }), 'utf8')), c.final()]);
    return { v: 1, iv: iv.toString('hex'), tag: c.getAuthTag().toString('hex'), data: data.toString('base64'), updatedAt: updatedAt || Date.now() };
}
function decrypt(blob, key) {
    if (!blob || blob.v !== 1 || !blob.iv || !blob.tag || !blob.data) throw new Error('Mali ang format ng vault.');
    const d = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(blob.iv, 'hex'));
    d.setAuthTag(Buffer.from(blob.tag, 'hex'));
    const txt = Buffer.concat([d.update(Buffer.from(blob.data, 'base64')), d.final()]).toString('utf8');
    const j = JSON.parse(txt);
    const out = {};
    for (const [k, v] of Object.entries((j && j.values) || {})) { if (NAME_RE.test(k) && typeof v === 'string' && !NEVER_VAULT.has(k)) out[k] = v; }
    return out;
}

function writeLocal(blob) {
    const f = path.join(state.dir, VAULT_FILE);
    const tmp = f + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(blob), { mode: 0o600 });
    fs.renameSync(tmp, f);
}
function readLocal() {
    try { return JSON.parse(fs.readFileSync(path.join(state.dir, VAULT_FILE), 'utf8')); } catch (_e) { return null; }
}
function fetchFromDatabaseSync() {
    try {
        const out = execFileSync(process.execPath, [path.join(__dirname, 'env-vault-fetch.js')], {
            env: process.env, timeout: 20000, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], maxBuffer: 4 * 1024 * 1024
        });
        const j = JSON.parse(String(out || '').trim() || 'null');
        return j && j.v === 1 ? j : null;
    } catch (_e) { return null; }
}

/** Tawagin NANG PINAKAUNA (pagkatapos ng .env loader). Hindi kailanman nagla-log ng values. */
function loadSync(dir, opts) {
    const o = opts || {};
    state.dir = dir;
    state.hostKeys = new Set(Object.keys(process.env).filter((k) => has(process.env[k])));
    state.applied = new Set(); state.values = {}; state.loadError = null; state.loadedFrom = null;
    try {
        const k = deriveKey(dir);
        state.key = k.key; state.keyMode = k.mode; state.stable = k.stable;
        let blob = readLocal(), from = null, values = null;
        if (blob) {
            try { values = decrypt(blob, state.key); from = 'file'; } catch (e) { state.loadError = 'Hindi ma-decrypt ang local vault (nagbago ang key?): ' + e.message; }
        }
        if (!values && o.fetchDb !== false && state.stable && (has(process.env.RELAY_DEVICES_DATABASE_URL) || has(process.env.DATABASE_URL))) {
            const dbBlob = fetchFromDatabaseSync();
            if (dbBlob) {
                try { values = decrypt(dbBlob, state.key); from = 'database'; blob = dbBlob; try { writeLocal(dbBlob); } catch (_e) { /* ok */ } }
                catch (e) { state.loadError = 'Hindi ma-decrypt ang vault sa database (iba ang key?): ' + e.message; }
            }
        }
        if (values) {
            state.values = values; state.loadedFrom = from; state.updatedAt = blob && blob.updatedAt || null;
            for (const [name, val] of Object.entries(values)) {
                if (!has(process.env[name])) { process.env[name] = val; state.applied.add(name); }
            }
        }
    } catch (e) { state.loadError = e.message; }
    const n = Object.keys(state.values).length;
    console.log(`🔐 ENV vault: ${n} variable sa vault, ${state.applied.size} ang ginamit (wala sa host ENV)${state.loadedFrom ? ' — mula sa ' + state.loadedFrom : ''}${state.loadError ? ' — ⚠️ ' + state.loadError : ''}.`);
    return { count: n, applied: [...state.applied], loadedFrom: state.loadedFrom, keyMode: state.keyMode };
}

function validateEntry(name, value) {
    if (!NAME_RE.test(String(name))) throw new Error(`Mali ang pangalan ng variable: ${String(name).slice(0, 40)}`);
    if (NEVER_VAULT.has(name)) throw new Error(`${name} ay hindi puwedeng ilagay sa vault (ilagay ito sa host ENV).`);
    if (typeof value !== 'string' || !value.length) throw new Error(`Walang laman ang ${name}.`);
    if (value.length > MAX_VALUE_LEN) throw new Error(`Masyadong mahaba ang ${name}.`);
    if (/\u0000/.test(value)) throw new Error(`Mali ang karakter sa ${name}.`);
}

/** Nagbabalik ng encrypted blob para sa database copy (o null kung hindi stable ang key). */
function persist() {
    state.updatedAt = Date.now();
    const blob = encrypt(state.values, state.key, state.updatedAt);
    writeLocal(blob);
    return state.stable ? blob : null;
}
function setValues(obj) {
    const entries = Object.entries(obj || {});
    for (const [n, v] of entries) validateEntry(n, v);
    for (const [n, v] of entries) {
        state.values[n] = v;
        if (!state.hostKeys.has(n)) { process.env[n] = v; state.applied.add(n); }
    }
    return { blob: persist(), names: entries.map(([n]) => n) };
}
function deleteNames(names) {
    const done = [];
    for (const n of names || []) {
        if (!Object.prototype.hasOwnProperty.call(state.values, n)) continue;
        delete state.values[n]; done.push(n);
        if (state.applied.has(n) && !state.hostKeys.has(n)) { delete process.env[n]; state.applied.delete(n); }
    }
    return { blob: persist(), names: done };
}
function sourceOf(name) {
    const host = state.hostKeys.has(name), inVault = Object.prototype.hasOwnProperty.call(state.values, name);
    return host && inVault ? 'host (Render) — nananaig sa vault' : host ? 'host (Render/.env)' : inVault ? 'vault' : 'wala';
}
function hasVault() { return Object.keys(state.values).length > 0; }
function dbBlob() { return state.stable && hasVault() ? encrypt(state.values, state.key, state.updatedAt || Date.now()) : null; }

// ---- catalog: kusang binabasa ang source ng RELAY para malaman kung anong env ang ginagamit ----
let scanCache = { at: 0, map: null };
function scanSources(dir) {
    if (scanCache.map && Date.now() - scanCache.at < 60000) return scanCache.map;
    const map = new Map();
    const add = (name, file, fallback) => {
        if (!NAME_RE.test(name)) return;
        let e = map.get(name);
        if (!e) { e = { name, files: new Set(), hasFallback: false }; map.set(name, e); }
        e.files.add(file); if (fallback) e.hasFallback = true;
    };
    let files = [];
    try { files = fs.readdirSync(dir).filter((f) => f.endsWith('.js') && f !== 'env-vault.js' && f !== 'env-vault-fetch.js' && f !== 'generate-keys.js'); } catch (_e) { /* */ }
    for (const f of files) {
        let src = '';
        try { src = fs.readFileSync(path.join(dir, f), 'utf8'); } catch (_e) { continue; }
        for (const m of src.matchAll(/process\.env\.([A-Z][A-Z0-9_]*)(\s*(\|\||\?\?))?/g)) add(m[1], f, !!m[3]);
        for (const m of src.matchAll(/process\.env\[\s*['"]([A-Z][A-Z0-9_]*)['"]\s*\](\s*(\|\||\?\?))?/g)) add(m[1], f, !!m[3]);
        for (const m of src.matchAll(/envPresent\(\s*['"]([A-Z][A-Z0-9_]*)['"]\s*\)/g)) add(m[1], f, false);
    }
    scanCache = { at: Date.now(), map };
    return map;
}
function groupOf(n) {
    if (/^(DATABASE|RELAY_DEVICES_DATABASE|REDIS|PG|NEON)/.test(n)) return 'Database';
    if (/^(CF_|CLOUDFLARE)/.test(n)) return 'Cloudflare / AI';
    if (/(PAYMONGO|STRIPE|XENDIT|GCASH|PAYMAYA)/.test(n)) return 'Payments';
    if (/(MAIL|SMTP|GMAIL|RESEND|SENDGRID)/.test(n)) return 'Email';
    if (/^RELAY_/.test(n)) return 'RELAY core';
    if (/^BIR_|_BIR_/.test(n)) return 'BIR';
    return 'Other';
}
function catalog(dir) {
    const scanned = scanSources(dir || state.dir);
    const names = new Set([...scanned.keys(), ...Object.keys(state.values)]);
    const rows = [];
    for (const name of names) {
        const s = scanned.get(name);
        const inVault = Object.prototype.hasOwnProperty.call(state.values, name);
        const host = state.hostKeys.has(name);
        let st = 'missing';
        if (host && inVault) st = 'both'; else if (host) st = 'host'; else if (inVault) st = 'vault';
        rows.push({
            name, group: groupOf(name), state: st, inVault,
            files: s ? [...s.files].sort() : [], discovered: !!s,
            required: s ? !s.hasFallback : false,
            platform: PLATFORM.has(name) || /^RENDER_/.test(name),
            neverVault: NEVER_VAULT.has(name), bootstrap: BOOTSTRAP.has(name)
        });
    }
    rows.sort((a, b) => a.group.localeCompare(b.group) || a.name.localeCompare(b.name));
    return rows;
}
function status() {
    return { keyMode: state.keyMode, stable: state.stable, loadedFrom: state.loadedFrom, count: Object.keys(state.values).length, updatedAt: state.updatedAt, loadError: state.loadError };
}

module.exports = { sourceOf, DB_KEY, NAME_RE, NEVER_VAULT, loadSync, setValues, deleteNames, hasVault, dbBlob, catalog, status, encrypt, decrypt, deriveKey, _state: state };
