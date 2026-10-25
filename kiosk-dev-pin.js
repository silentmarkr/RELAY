// kiosk-dev-pin.js (RELAY)
// -----------------------------------------------------------------------------
// Isang Developer PIN para sa OMNIKiosk ng LAHAT ng client. Dito (sa RELAY) itinatakda ng developer; ang client
// OMNIPOS ay nagtatanong lang sa RELAY kung tama ang PIN (hindi kailanman ipinapadala ang PIN o ang hash sa client).
// Kailangan ng PIN para palitan ang "Uri ng negosyo" ng kiosk (nagtatakda ng buttons/rules/order types).
// Naka-hash (scrypt). May lockout bawat installation: 5 maling try = 15 minuto.
// -----------------------------------------------------------------------------
'use strict';
const crypto = require('crypto');

const MIN_LEN = 6, MAX_LEN = 12;
const MAX_FAILS = 5, LOCK_MS = 15 * 60 * 1000;

function validatePin(pin) {
    const p = String(pin == null ? '' : pin);
    return p.length >= MIN_LEN && p.length <= MAX_LEN && !/\s/.test(p);
}
function makeRecord(pin, now) {
    const salt = crypto.randomBytes(16);
    return { salt: salt.toString('hex'), hash: crypto.scryptSync(String(pin), salt, 32).toString('hex'), updatedAt: typeof now === 'number' ? now : Date.now() };
}
function matches(record, pin) {
    try {
        if (!record || !record.salt || !record.hash) return false;
        const h = crypto.scryptSync(String(pin == null ? '' : pin), Buffer.from(record.salt, 'hex'), 32);
        const want = Buffer.from(record.hash, 'hex');
        return h.length === want.length && crypto.timingSafeEqual(h, want);
    } catch (e) { return false; }
}
function createGuard(options = {}) {
    const now = typeof options.now === 'function' ? options.now : () => Date.now();
    const fails = new Map(); // installationId -> { n, until }
    return {
        check(installationId, record, pin) {
            const f = fails.get(installationId) || { n: 0, until: 0 };
            if (f.until > now()) return { ok: false, code: 'LOCKED', retryAfterSec: Math.ceil((f.until - now()) / 1000) };
            if (!record) return { ok: false, code: 'UNSET' };
            if (matches(record, pin)) { fails.delete(installationId); return { ok: true }; }
            f.n += 1;
            if (f.n >= MAX_FAILS) { f.n = 0; f.until = now() + LOCK_MS; }
            fails.set(installationId, f);
            if (fails.size > 5000) fails.delete(fails.keys().next().value);
            return { ok: false, code: 'BAD_PIN' };
        }
    };
}
module.exports = { MIN_LEN, MAX_LEN, validatePin, makeRecord, matches, createGuard };
