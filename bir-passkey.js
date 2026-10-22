// bir-passkey.js (RELAY)
// -----------------------------------------------------------------------------
// Developer-side half of the OmniPOS "BIR reset pass key" flow.
//
//   OmniPOS (client)  --request code (BIRRQ1.…)-->  developer  --pass key (BIRPK1.…)-->  OmniPOS
//
// The request code is created by the client's OmniPOS (OMNIPOS/bir-retention.js). The developer pastes it into the
// RELAY admin page, checks who is asking and why, and RELAY signs a one-time pass key with the same Ed25519 private key
// that already signs feature tokens. OmniPOS verifies the signature with the matching public key it ships with, so
// the whole exchange works offline - nothing has to be reachable between the client and RELAY.
//
// A pass key is bound to: the installation, the device tag, the exact action (purpose), whether the BIR counters are
// wiped, and the one nonce the client app generated. It expires (default 30 minutes, hard maximum 4 hours) and the app
// accepts it only once.
//
// SECURITY NOTE: everything inside a request code is chosen by the CLIENT. It is shown to the developer as information
// only and is never trusted - always escape it when rendering. The developer remains the gatekeeper.
//
// canonicalTicketString() MUST stay byte-for-byte identical to the one in OMNIPOS/bir-retention.js (same keys, same
// order). Changing one without the other makes every pass key fail verification.
// -----------------------------------------------------------------------------
'use strict';

const crypto = require('crypto');

const REQUEST_PREFIX = 'BIRRQ1.';
const PASSKEY_PREFIX = 'BIRPK1.';
const PURPOSES = Object.freeze(['bir-hard-reset', 'bir-agt-reset']);
const MAX_INPUT_LENGTH = 8192;
const DEFAULT_VALIDITY_MINUTES = 30;
const MIN_VALIDITY_MINUTES = 5;
const MAX_VALIDITY_MINUTES = 240;

function canonicalTicketString(p) {
    return JSON.stringify({
        installationId: p.installationId,
        deviceTag: p.deviceTag,
        purpose: p.purpose,
        nonce: p.nonce,
        includeBirData: p.includeBirData,
        reference: p.reference,
        ticketId: p.ticketId,
        issuedAt: p.issuedAt,
        expiresAt: p.expiresAt
    });
}

// DOMAIN SEPARATION: the signed bytes are prefixed with a fixed label, so a signature RELAY makes for anything else
// with the same Ed25519 key (feature tokens, licences) can never be accepted as a BIR pass key, and the other way round.
// MUST be identical to SIGNING_DOMAIN in OMNIPOS/bir-retention.js.
const SIGNING_DOMAIN = 'omnipos-bir-passkey-v1\n';
function signedMessage(p) {
    return Buffer.from(SIGNING_DOMAIN + canonicalTicketString(p), 'utf8');
}

function isPlainObject(v) {
    return !!v && typeof v === 'object' && !Array.isArray(v);
}

function bad(code, message) {
    return { ok: false, code, message };
}

// Parses and validates a request code. Returns { ok: true, request } or { ok: false, code, message }.
function decodeRequestCode(input) {
    if (typeof input !== 'string') return bad('REQUEST_CODE_REQUIRED', 'Paste the request code from the client.');
    const compact = input.replace(/\s+/g, '');
    if (!compact) return bad('REQUEST_CODE_REQUIRED', 'Paste the request code from the client.');
    if (compact.length > MAX_INPUT_LENGTH || !compact.startsWith(REQUEST_PREFIX)) {
        return bad('REQUEST_CODE_MALFORMED', 'That is not an OmniPOS BIR request code (it must start with BIRRQ1.).');
    }
    let body;
    try {
        body = JSON.parse(Buffer.from(compact.slice(REQUEST_PREFIX.length), 'base64url').toString('utf8'));
    } catch (_e) {
        return bad('REQUEST_CODE_MALFORMED', 'The request code is damaged. Ask the client to copy it again.');
    }
    const ok = isPlainObject(body) && body.v === 1 &&
        typeof body.installationId === 'string' && body.installationId.length > 0 && body.installationId.length <= 200 &&
        typeof body.deviceTag === 'string' && body.deviceTag.length > 0 && body.deviceTag.length <= 64 &&
        PURPOSES.includes(body.purpose) &&
        typeof body.nonce === 'string' && /^[0-9a-f]{32}$/.test(body.nonce) &&
        typeof body.includeBirData === 'boolean' &&
        Number.isFinite(body.createdAt) && Number.isFinite(body.expiresAt);
    if (!ok) return bad('REQUEST_CODE_MALFORMED', 'The request code is incomplete or damaged. Ask the client to generate a new one.');
    if (body.purpose === 'bir-agt-reset' && body.includeBirData !== false) {
        return bad('REQUEST_CODE_MALFORMED', 'The request code is inconsistent (AGT reset cannot include BIR counters).');
    }
    return {
        ok: true,
        request: {
            installationId: body.installationId,
            deviceTag: body.deviceTag,
            purpose: body.purpose,
            nonce: body.nonce,
            includeBirData: body.includeBirData,
            storeName: typeof body.storeName === 'string' ? body.storeName.slice(0, 120) : '',
            createdAt: body.createdAt,
            expiresAt: body.expiresAt,
            summary: isPlainObject(body.summary) ? body.summary : {}
        }
    };
}

function clampMinutes(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return DEFAULT_VALIDITY_MINUTES;
    return Math.max(MIN_VALIDITY_MINUTES, Math.min(MAX_VALIDITY_MINUTES, Math.round(n)));
}

// Signs a pass key for a decoded request. Throws an Error with .code for expected problems.
function issuePassKey({ request, privateKey, reference, ttlMinutes, now }) {
    const ts = typeof now === 'number' ? now : Date.now();
    if (!request || !privateKey) {
        const e = new Error('Missing request or signing key.'); e.code = 'BAD_INPUT'; throw e;
    }
    const cleanReference = String(reference || '').trim();
    if (cleanReference.length < 3 || cleanReference.length > 160) {
        const e = new Error('Enter a reference (3-160 characters): the support ticket, invoice or reason that justifies this reset.');
        e.code = 'REFERENCE_REQUIRED';
        throw e;
    }
    if (request.expiresAt <= ts) {
        const e = new Error('This request code has expired. Ask the client to generate a new one.');
        e.code = 'REQUEST_EXPIRED';
        throw e;
    }
    const issuedAt = ts;
    // Never outlive the client's own request.
    const expiresAt = Math.min(ts + clampMinutes(ttlMinutes) * 60 * 1000, request.expiresAt);
    const payload = {
        installationId: request.installationId,
        deviceTag: request.deviceTag,
        purpose: request.purpose,
        nonce: request.nonce,
        includeBirData: request.includeBirData,
        reference: cleanReference,
        ticketId: `BIRPK-${ts.toString(36)}-${crypto.randomBytes(4).toString('hex')}`,
        issuedAt,
        expiresAt
    };
    const signature = crypto.sign(null, signedMessage(payload), privateKey).toString('base64');
    const passKey = PASSKEY_PREFIX + Buffer.from(JSON.stringify({ payload, signature }), 'utf8').toString('base64url');
    return { passKey, payload };
}

// SECOND SECRET. Issuing a pass key needs the RELAY admin key AND this separate developer passkey (env
// RELAY_BIR_RESET_PASSKEY, at least 8 characters mixing 3 of: lowercase, UPPERCASE, digits, symbols; never sent to any client).
// Stealing one is not enough. Because 8 characters is short, RELAY also locks issuing after repeated wrong guesses (see server.js).
// Both values are hashed first so the comparison is constant-time regardless of length.
const DEVELOPER_PASSKEY_MIN_LENGTH = 8;
// Strength rule for a short passkey: 8+ characters that mix at least 3 of 4 character classes.
function isStrongDeveloperPasskey(value) {
    if (typeof value !== 'string' || value.length < DEVELOPER_PASSKEY_MIN_LENGTH) return false;
    const classes = [/[a-z]/, /[A-Z]/, /\d/, /[^A-Za-z0-9]/].filter((re) => re.test(value)).length;
    return classes >= 3;
}
function verifyDeveloperPasskey(candidate, configured) {
    const supplied = Buffer.from(typeof candidate === 'string' ? candidate : '', 'utf8');
    const expected = Buffer.from(typeof configured === 'string' ? configured : '', 'utf8');
    const suppliedHash = crypto.createHash('sha256').update(supplied).digest();
    const expectedHash = crypto.createHash('sha256').update(expected).digest();
    return isStrongDeveloperPasskey(configured) && crypto.timingSafeEqual(suppliedHash, expectedHash);
}

// The developer passkey and the signed key must never travel over plain HTTP (localhost is allowed for local development).
function isSecureAdminRequest(req) {
    const address = String((req && req.socket && req.socket.remoteAddress) || '').replace(/^::ffff:/, '');
    const isLoopback = address === '127.0.0.1' || address === '::1';
    return !!(req && req.secure) || isLoopback;
}

// Verifies the signature of a pass key (used by tests and available for support tooling).
function verifyPassKeySignature(passKey, publicKey) {
    try {
        const compact = String(passKey || '').replace(/\s+/g, '');
        if (!compact.startsWith(PASSKEY_PREFIX)) return false;
        const parsed = JSON.parse(Buffer.from(compact.slice(PASSKEY_PREFIX.length), 'base64url').toString('utf8'));
        return crypto.verify(null, signedMessage(parsed.payload), publicKey, Buffer.from(parsed.signature, 'base64'));
    } catch (_e) {
        return false;
    }
}

module.exports = {
    REQUEST_PREFIX,
    PASSKEY_PREFIX,
    PURPOSES,
    DEFAULT_VALIDITY_MINUTES,
    MIN_VALIDITY_MINUTES,
    MAX_VALIDITY_MINUTES,
    canonicalTicketString,
    SIGNING_DOMAIN,
    signedMessage,
    decodeRequestCode,
    issuePassKey,
    verifyPassKeySignature,
    DEVELOPER_PASSKEY_MIN_LENGTH,
    isStrongDeveloperPasskey,
    verifyDeveloperPasskey,
    isSecureAdminRequest
};
