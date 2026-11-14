// receipt-branding.js (RELAY)
// -----------------------------------------------------------------------------
// SIGNED RECEIPT BRANDING. Ang TRADE NAME na lumalabas sa resibo ng bawat client ay kontrolado LANG ng RELAY, at ang RELAY
// din ang nag-a-authorize kung puwede silang maglagay ng BRANDING LOGO sa tabi ng trade name.
//
//   - tradeName: kung may ACTIVE na BIR profile ang client (bir-profile.js), ang `businessName` doon ang ginagamit
//     (isang source lang, hindi na nadodoble). Kung wala pang BIR profile (hal. hindi pa naka-unlock ang BIR), ang
//     tradeName na itinakda sa admin page (Trade name & Branding Logo) ang gagamitin.
//   - logoAllowed: kung true, puwedeng mag-upload ang client ng logo at pumili kung KALIWA, KANAN o ITAAS ng trade name.
//     Iyon lang ang puwede nilang baguhin; hindi nila mababago ang pangalan.
//
// Pinipirmahan ng private key ng RELAY (Ed25519, kapareho ng BIR profile) at naka-bind sa installationId. Ang OmniPOS ay
// nagba-verify gamit ang RELAY_PUBLIC_KEY. canonicalBrandingString() at SIGNING_DOMAIN ay DAPAT byte-for-byte na kapareho
// ng nasa OMNIPOS/receipt-branding.js.
// -----------------------------------------------------------------------------
'use strict';

const crypto = require('crypto');

const SIGNING_DOMAIN = 'omnipos-receipt-branding-v1\n';
const TRADE_NAME_MAX = 80;
const MAX_RECORDS = 5000;
const MAX_AUDIT = 1000;

function emptyState() { return { records: {}, audit: [], updatedAt: null }; }

function cleanText(val, maxLen) {
    if (typeof val !== 'string') return '';
    return val.replace(/[\u0000-\u001f\u007f]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, maxLen);
}

function normalizeState(raw) {
    const s = emptyState();
    if (!raw || typeof raw !== 'object') return s;
    s.updatedAt = typeof raw.updatedAt === 'number' ? raw.updatedAt : null;
    const src = raw.records && typeof raw.records === 'object' ? raw.records : {};
    for (const id of Object.keys(src).slice(0, MAX_RECORDS)) {
        const rec = src[id];
        if (!rec || typeof rec !== 'object' || id.length > 200) continue;
        s.records[id] = {
            tradeName: cleanText(rec.tradeName, TRADE_NAME_MAX),
            logoAllowed: rec.logoAllowed === true,
            label: cleanText(rec.label, 80),
            createdAt: typeof rec.createdAt === 'number' ? rec.createdAt : null,
            updatedAt: typeof rec.updatedAt === 'number' ? rec.updatedAt : null
        };
    }
    s.audit = Array.isArray(raw.audit) ? raw.audit.filter((a) => a && typeof a === 'object').slice(-MAX_AUDIT) : [];
    return s;
}

function pushAudit(state, entry) {
    state.audit.push(Object.assign({ at: Date.now() }, entry));
    if (state.audit.length > MAX_AUDIT) state.audit.splice(0, state.audit.length - MAX_AUDIT);
}

// Gumawa/palitan ang branding record ng isang client.
function upsertRecord(state, { installationId, tradeName, logoAllowed, label, reason, now }) {
    const s = normalizeState(state);
    const t = typeof now === 'number' ? now : Date.now();
    const id = String(installationId || '').trim();
    if (!id || id.length > 200) throw new Error('A valid installationId is required.');
    const name = cleanText(tradeName, TRADE_NAME_MAX);
    const prev = s.records[id];
    if (!prev && Object.keys(s.records).length >= MAX_RECORDS) throw new Error('Too many branding records.');
    s.records[id] = {
        tradeName: name,
        logoAllowed: logoAllowed === true,
        label: cleanText(label, 80) || (prev ? prev.label : ''),
        createdAt: prev ? prev.createdAt : t,
        updatedAt: t
    };
    s.updatedAt = t;
    const changed = [];
    if (!prev || prev.tradeName !== name) changed.push('tradeName');
    if (!prev || prev.logoAllowed !== (logoAllowed === true)) changed.push('logoAllowed');
    pushAudit(s, { action: prev ? 'update' : 'create', installationId: id, changedFields: changed, reason: cleanText(reason, 160) });
    return s;
}

function removeRecord(state, { installationId, reason, now }) {
    const s = normalizeState(state);
    const id = String(installationId || '').trim();
    if (!s.records[id]) throw new Error('No branding record exists for that installationId.');
    delete s.records[id];
    s.updatedAt = typeof now === 'number' ? now : Date.now();
    pushAudit(s, { action: 'delete', installationId: id, reason: cleanText(reason, 160) });
    return s;
}

// Ang trade name na talagang ipapadala: ACTIVE na BIR profile muna (business name), kung wala, ang tinakda sa branding record.
function effectiveTradeName(rec, birRec) {
    const fromBir = birRec && birRec.status === 'active' && birRec.fields ? cleanText(birRec.fields.businessName, TRADE_NAME_MAX) : '';
    if (fromBir) return { name: fromBir, source: 'bir-profile' };
    const own = rec ? cleanText(rec.tradeName, TRADE_NAME_MAX) : '';
    return { name: own, source: own ? 'branding' : 'none' };
}

// Fixed key order. Huwag baguhin nang hindi binabago ang OMNIPOS/receipt-branding.js.
function canonicalBrandingString(p) {
    return JSON.stringify({
        installationId: p.installationId,
        status: p.status,
        version: p.version,
        issuedAt: p.issuedAt,
        tradeName: p.tradeName,
        logoAllowed: p.logoAllowed === true
    });
}
function signedMessage(p) {
    return Buffer.from(SIGNING_DOMAIN + canonicalBrandingString(p), 'utf8');
}

// Pipirmahan ang kasalukuyang branding ng isang client. `birState` ay ang state ng bir-profile.js (para sa business name).
// Returns null kung wala talagang mapagkukunan ng trade name (walang record at walang active na BIR profile).
function signBrandingFor(privateKey, brandingState, birState, installationId, now) {
    const s = normalizeState(brandingState);
    const id = String(installationId || '').trim();
    const rec = s.records[id] || null;
    const birRec = birState && birState.profiles ? birState.profiles[id] : null;
    const eff = effectiveTradeName(rec, birRec);
    if (!eff.name) return null;
    // Monotonic na version (ms timestamp) para hindi mag-rollback ang client kahit BIR profile ang nagbago ng pangalan.
    const version = Math.max(
        rec && rec.updatedAt ? rec.updatedAt : 0,
        birRec && birRec.updatedAt ? birRec.updatedAt : 0,
        s.updatedAt || 0,
        birState && typeof birState.updatedAt === 'number' ? birState.updatedAt : 0,
        1
    );
    const payload = {
        installationId: id,
        status: 'active',
        version,
        issuedAt: typeof now === 'number' ? now : Date.now(),
        tradeName: eff.name,
        logoAllowed: !!(rec && rec.logoAllowed === true)
    };
    const signature = crypto.sign(null, signedMessage(payload), privateKey).toString('base64');
    return { payload, signature };
}

function verifyBrandingSignature(branding, publicKey) {
    try {
        return crypto.verify(null, signedMessage(branding.payload), publicKey, Buffer.from(branding.signature, 'base64'));
    } catch (_e) { return false; }
}

module.exports = {
    SIGNING_DOMAIN,
    TRADE_NAME_MAX,
    emptyState,
    normalizeState,
    upsertRecord,
    removeRecord,
    effectiveTradeName,
    canonicalBrandingString,
    signedMessage,
    signBrandingFor,
    verifyBrandingSignature
};
