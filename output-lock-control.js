// output-lock-control.js (RELAY)
// -----------------------------------------------------------------------------
// REMOTE CONTROL ng "Inventory-only mode" (naka-lock na print / preview / share / email / download ng resibo).
// Default ng LAHAT ng client: LOCKED. Dito sa RELAY (admin page output-lock.html) ikaw ang magtatanggal o maglalagay
// ulit ng lock, anumang oras:
//   - globalUnlocked: true   -> naka-unlock ang LAHAT ng client
//   - installations[id]      -> 'unlock' | 'lock'  (override para sa ISANG client; nananaig sa global)
// Ang sagot sa client ay SIGNED (ed25519, parehong key ng unlock tokens) at may expiry, kaya hindi ito mapepeke ng
// client at kusang babalik sa LOCKED kapag hindi na nakakausap ang RELAY nang lampas sa grace period.
// -----------------------------------------------------------------------------
'use strict';
const crypto = require('crypto');

const DEFAULT_TTL_MS = 12 * 60 * 60 * 1000;          // gaano katagal valid ang isang "unlock" na sagot kapag offline ang client
const MAX_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const MAX_OVERRIDES = 5000;

function emptyState() { return { globalUnlocked: false, installations: {}, note: '', updatedAt: null }; }

function normalizeState(raw) {
    const s = emptyState();
    if (!raw || typeof raw !== 'object') return s;
    s.globalUnlocked = raw.globalUnlocked === true;
    s.note = typeof raw.note === 'string' ? raw.note.slice(0, 300) : '';
    s.updatedAt = typeof raw.updatedAt === 'number' ? raw.updatedAt : null;
    const src = raw.installations && typeof raw.installations === 'object' ? raw.installations : {};
    for (const id of Object.keys(src).slice(0, MAX_OVERRIDES)) {
        const v = src[id] && typeof src[id] === 'object' ? src[id].mode : src[id];
        if (v === 'unlock' || v === 'lock') {
            s.installations[id] = { mode: v, at: (src[id] && typeof src[id].at === 'number') ? src[id].at : null };
        }
    }
    return s;
}

/** Per-client override ang nananaig; kung wala, ang global; default LOCKED. */
function resolve(state, installationId) {
    const s = normalizeState(state);
    const o = installationId ? s.installations[String(installationId)] : null;
    if (o) return { unlocked: o.mode === 'unlock', source: 'installation' };
    return { unlocked: s.globalUnlocked === true, source: s.globalUnlocked ? 'global' : 'default' };
}

function applyChange(state, change, now) {
    const s = normalizeState(state);
    const t = typeof now === 'number' ? now : Date.now();
    const c = change || {};
    if (c.scope === 'global') {
        s.globalUnlocked = c.unlocked === true;
    } else if (c.scope === 'installation') {
        const id = String(c.installationId || '').trim();
        if (!id || id.length > 200) throw new Error('Kailangan ng wastong installationId.');
        if (c.mode === 'unlock' || c.mode === 'lock') {
            if (!s.installations[id] && Object.keys(s.installations).length >= MAX_OVERRIDES) throw new Error('Sobra na ang dami ng per-client override.');
            s.installations[id] = { mode: c.mode, at: t };
        } else if (c.mode === 'inherit') {
            delete s.installations[id];
        } else throw new Error('Mali ang mode (unlock | lock | inherit).');
    } else if (c.scope === 'clear-overrides') {
        s.installations = {};
    } else throw new Error('Mali ang scope.');
    if (typeof c.note === 'string') s.note = c.note.slice(0, 300);
    s.updatedAt = t;
    return s;
}

/** Signed command para sa isang client. Ang LOCKED na sagot ay pipirmahan din (para agad bumalik sa lock). */
function signCommand(privateKey, installationId, unlocked, ttlMs, now) {
    const t = typeof now === 'number' ? now : Date.now();
    const ttl = Math.min(MAX_TTL_MS, Math.max(60 * 1000, Number(ttlMs) || DEFAULT_TTL_MS));
    const payload = { installationId: String(installationId), purpose: 'output-lock', unlocked: unlocked === true, issuedAt: t, expiresAt: t + ttl };
    const payloadString = JSON.stringify(payload);
    const signature = crypto.sign(null, Buffer.from(payloadString), privateKey).toString('base64');
    return { payload, signature };
}

module.exports = { DEFAULT_TTL_MS, MAX_TTL_MS, emptyState, normalizeState, resolve, applyChange, signCommand };
