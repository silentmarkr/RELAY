// bir-test-wipe.js (RELAY)
// -----------------------------------------------------------------------------
// Remote "BIR test wipe": the developer presses a button in the RELAY admin page, RELAY signs a short-lived,
// one-time command for ONE installation, and that OmniPOS client only has to CONFIRM it on its screen.
// RELAY decides when; the client never starts this by itself.
//
// The command is signed with the same Ed25519 private key as the BIR pass keys, but under its OWN domain label,
// so a pass-key signature can never be replayed as a wipe command (and the other way round).
// canonicalCommandString() MUST stay byte-for-byte identical to the one in OMNIPOS/server.js.
// The client additionally refuses it unless the device has the local opt-in file database/ALLOW_BIR_TEST_WIPE.
// -----------------------------------------------------------------------------
'use strict';

const crypto = require('crypto');

const SIGNING_DOMAIN = 'omnipos-bir-testwipe-v1\n';
const DEFAULT_TTL_MINUTES = 15;
const MIN_TTL_MINUTES = 2;
const MAX_TTL_MINUTES = 60;
const KEEP_FINISHED_MS = 60 * 60 * 1000;
const MAX_ENTRIES = 200;

function canonicalCommandString(p) {
    return JSON.stringify({
        installationId: p.installationId,
        commandId: p.commandId,
        nonce: p.nonce,
        issuedAt: p.issuedAt,
        expiresAt: p.expiresAt,
        reference: p.reference
    });
}
function signedMessage(p) {
    return Buffer.from(SIGNING_DOMAIN + canonicalCommandString(p), 'utf8');
}
function clampMinutes(v) {
    const n = Math.round(Number(v));
    if (!Number.isFinite(n)) return DEFAULT_TTL_MINUTES;
    return Math.min(MAX_TTL_MINUTES, Math.max(MIN_TTL_MINUTES, n));
}

function issueCommand({ installationId, privateKey, reference, ttlMinutes, now }) {
    const ts = typeof now === 'number' ? now : Date.now();
    if (!installationId || typeof installationId !== 'string' || installationId.length > 200 || !privateKey) {
        const e = new Error('Missing installation ID or signing key.'); e.code = 'BAD_INPUT'; throw e;
    }
    const cleanReference = String(reference || '').trim();
    if (cleanReference.length < 3 || cleanReference.length > 160) {
        const e = new Error('Enter a reference (3-160 characters), e.g. "test run 12".'); e.code = 'REFERENCE_REQUIRED'; throw e;
    }
    const payload = {
        installationId,
        commandId: `BIRWIPE-${ts.toString(36)}-${crypto.randomBytes(4).toString('hex')}`,
        nonce: crypto.randomBytes(16).toString('hex'),
        issuedAt: ts,
        expiresAt: ts + clampMinutes(ttlMinutes) * 60 * 1000,
        reference: cleanReference
    };
    const signature = crypto.sign(null, signedMessage(payload), privateKey).toString('base64');
    return { payload, signature };
}

// In-memory only (like the reset inbox): one command per installation; a new one replaces the old one.
function createCommandStore(options = {}) {
    const now = typeof options.now === 'function' ? options.now : () => Date.now();
    const entries = new Map(); // installationId -> { payload, signature, status, createdAt, deliveredAt, finishedAt }

    function sweep() {
        const t = now();
        for (const [id, e] of entries) {
            if ((e.status === 'pending' || e.status === 'delivered') && e.payload.expiresAt <= t) { e.status = 'expired'; e.finishedAt = t; }
            if (e.finishedAt && t - e.finishedAt >= KEEP_FINISHED_MS) entries.delete(id);
        }
    }
    function put(issued) {
        sweep();
        if (!entries.has(issued.payload.installationId) && entries.size >= MAX_ENTRIES) return { ok: false, code: 'STORE_FULL' };
        entries.set(issued.payload.installationId, { payload: issued.payload, signature: issued.signature, status: 'pending', createdAt: now(), deliveredAt: null, finishedAt: null });
        return { ok: true };
    }
    // Client poll: returns the signed command (and marks it delivered) while it is still valid.
    function deliver(installationId) {
        sweep();
        const e = entries.get(installationId);
        if (!e || (e.status !== 'pending' && e.status !== 'delivered')) return null;
        if (!e.deliveredAt) e.deliveredAt = now();
        e.status = 'delivered';
        return { payload: e.payload, signature: e.signature };
    }
    function finish(installationId, commandId, result) {
        sweep();
        const e = entries.get(installationId);
        if (!e || e.payload.commandId !== commandId) return false;
        if (e.status !== 'pending' && e.status !== 'delivered') return false;
        e.status = result === 'done' ? 'done' : 'declined';
        e.finishedAt = now();
        return true;
    }
    function cancel(installationId) {
        sweep();
        const e = entries.get(installationId);
        if (!e || (e.status !== 'pending' && e.status !== 'delivered')) return false;
        e.status = 'cancelled';
        e.finishedAt = now();
        return true;
    }
    function list() {
        sweep();
        return Array.from(entries.values()).map((e) => ({
            installationId: e.payload.installationId,
            commandId: e.payload.commandId,
            reference: e.payload.reference,
            issuedAt: e.payload.issuedAt,
            expiresAt: e.payload.expiresAt,
            status: e.status,
            deliveredAt: e.deliveredAt,
            finishedAt: e.finishedAt
        })).sort((a, b) => b.issuedAt - a.issuedAt);
    }
    return { put, deliver, finish, cancel, list };
}

module.exports = { SIGNING_DOMAIN, DEFAULT_TTL_MINUTES, MIN_TTL_MINUTES, MAX_TTL_MINUTES, canonicalCommandString, signedMessage, issueCommand, createCommandStore };
