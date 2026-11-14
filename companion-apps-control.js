// companion-apps-control.js (RELAY)
// -----------------------------------------------------------------------------
// REMOTE CONTROL ng pag-install/pag-download ng COMPANION APPS (OmniKiosk, Kitchen Kiosk, Customer Display) sa OmniPOS Native.
// Ang tatlong APK ay naka-bundle sa loob ng app, kaya DEFAULT ay LOCKED ang lahat: walang client ang makakapag-install hangga't
// hindi mo ina-allow dito (admin page companion-apps.html). Dalawang antas:
//   - global[APP] = true            -> pinapayagan ang APP sa LAHAT ng client
//   - installations[id].apps[APP]   -> 'allow' | 'deny' (override para sa ISANG client/store; nananaig sa global)
// Ang sagot sa client ay SIGNED (ed25519, parehong key ng unlock tokens), may expiry, at kusang babalik sa LOCKED kapag hindi
// na maabot ang RELAY nang lampas sa expiry.
// PAALALA: ang APK ay nasa loob ng app package; ito ay control sa app (UI/installer), hindi DRM laban sa taong kayang
// mag-extract ng APK.
// -----------------------------------------------------------------------------
'use strict';
const crypto = require('crypto');

const APPS = Object.freeze(['OMNIKIOSK', 'KITCHENKIOSK', 'OMNIPOSDISPLAY']);
const APP_LABELS = Object.freeze({ OMNIKIOSK: 'OmniKiosk', KITCHENKIOSK: 'Kitchen Kiosk', OMNIPOSDISPLAY: 'Customer Display' });
const DEFAULT_TTL_MS = 12 * 60 * 60 * 1000;
const MAX_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const MAX_OVERRIDES = 5000;

function emptyState() { return { global: {}, installations: {}, note: '', updatedAt: null }; }

function normalizeState(raw) {
    const s = emptyState();
    if (!raw || typeof raw !== 'object') return s;
    s.note = typeof raw.note === 'string' ? raw.note.slice(0, 300) : '';
    s.updatedAt = typeof raw.updatedAt === 'number' ? raw.updatedAt : null;
    const g = raw.global && typeof raw.global === 'object' ? raw.global : {};
    for (const a of APPS) if (g[a] === true) s.global[a] = true;
    const src = raw.installations && typeof raw.installations === 'object' ? raw.installations : {};
    for (const id of Object.keys(src).slice(0, MAX_OVERRIDES)) {
        const apps = src[id] && typeof src[id].apps === 'object' && src[id].apps ? src[id].apps : {};
        const clean = {};
        for (const a of APPS) if (apps[a] === 'allow' || apps[a] === 'deny') clean[a] = apps[a];
        if (Object.keys(clean).length) s.installations[id] = { apps: clean, at: typeof src[id].at === 'number' ? src[id].at : null };
    }
    return s;
}

/** Listahan ng APP na pinapayagan para sa client na ito. Per-client override ang nananaig; default LOCKED. */
function resolve(state, installationId) {
    const s = normalizeState(state);
    const o = installationId ? s.installations[String(installationId)] : null;
    const allowed = [];
    for (const a of APPS) {
        const ov = o && o.apps[a];
        if (ov === 'allow' || (ov !== 'deny' && s.global[a] === true)) allowed.push(a);
    }
    return { allowed };
}

function appList(app) {
    if (app === 'all') return APPS.slice();
    if (APPS.includes(app)) return [app];
    throw new Error('Mali ang app (' + APPS.join(' | ') + ' | all).');
}

function applyChange(state, change, now) {
    const s = normalizeState(state);
    const t = typeof now === 'number' ? now : Date.now();
    const c = change || {};
    if (c.scope === 'global') {
        for (const a of appList(c.app)) { if (c.allowed === true) s.global[a] = true; else delete s.global[a]; }
    } else if (c.scope === 'installation') {
        const id = String(c.installationId || '').trim();
        if (!id || id.length > 200) throw new Error('Kailangan ng wastong installationId.');
        const list = appList(c.app);
        if (!['allow', 'deny', 'inherit'].includes(c.mode)) throw new Error('Mali ang mode (allow | deny | inherit).');
        if (!s.installations[id] && Object.keys(s.installations).length >= MAX_OVERRIDES) throw new Error('Sobra na ang dami ng per-client override.');
        const cur = s.installations[id] || { apps: {}, at: t };
        for (const a of list) { if (c.mode === 'inherit') delete cur.apps[a]; else cur.apps[a] = c.mode; }
        cur.at = t;
        if (Object.keys(cur.apps).length) s.installations[id] = cur; else delete s.installations[id];
    } else if (c.scope === 'clear-overrides') {
        s.installations = {};
    } else throw new Error('Mali ang scope.');
    if (typeof c.note === 'string') s.note = c.note.slice(0, 300);
    s.updatedAt = t;
    return s;
}

/** Signed command para sa isang client. Ang LOCKED (walang allowed) na sagot ay pipirmahan din. */
function signCommand(privateKey, installationId, allowed, ttlMs, now) {
    const t = typeof now === 'number' ? now : Date.now();
    const ttl = Math.min(MAX_TTL_MS, Math.max(60 * 1000, Number(ttlMs) || DEFAULT_TTL_MS));
    const list = APPS.filter((a) => (allowed || []).includes(a));
    const payload = { installationId: String(installationId), purpose: 'companion-apps', allowed: list, issuedAt: t, expiresAt: t + ttl };
    const signature = crypto.sign(null, Buffer.from(JSON.stringify(payload)), privateKey).toString('base64');
    return { payload, signature };
}

module.exports = { APPS, APP_LABELS, DEFAULT_TTL_MS, MAX_TTL_MS, emptyState, normalizeState, resolve, applyChange, signCommand };
