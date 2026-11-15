// bir-profile.js (RELAY)
// -----------------------------------------------------------------------------
// SIGNED BIR PROFILE. Ang BIR identity ng bawat client (business name, TIN, VAT status, MIN, serial no., PTU no./date,
// POS-supplier info) ay GINAGAWA at PINIPIRMAHAN dito sa RELAY gamit ang private key ng developer (Ed25519, parehong key
// na pumipirma sa feature tokens). Ang OmniPOS ay nagba-verify gamit ang RELAY_PUBLIC_KEY na kasama ng app at
// naka-bind sa installationId, kaya:
//   - hindi maieedit ng client ang mga field na ito sa UI, sa API, sa backup file o sa database (walang private key ang client);
//   - kapag invalid, nawala o na-revoke ang profile, FAIL-CLOSED ang BIR sa app.
//
// canonicalProfileString() at SIGNING_DOMAIN ay DAPAT byte-for-byte na kapareho ng nasa OMNIPOS/bir-profile.js.
// Ang pagbabago sa isa nang hindi sa kabila ay magpapa-fail sa verification ng lahat ng profile.
// -----------------------------------------------------------------------------
'use strict';

const crypto = require('crypto');

const SIGNING_DOMAIN = 'omnipos-bir-profile-v1\n';
const PROFILE_FIELDS = Object.freeze([
    'businessName', 'businessAddress', 'tin', 'vatStatus', 'ptuNo', 'ptuDate', 'min', 'serialNo', 'supplierName', 'supplierAddress', 'supplierTin'
]);
const STATUSES = Object.freeze(['active', 'revoked']);
const MAX_PROFILES = 5000;
const MAX_AUDIT = 1000;
const FIELD_LIMITS = Object.freeze({
    businessName: 80, businessAddress: 160, tin: 20, ptuNo: 40, min: 40, serialNo: 40, supplierName: 80, supplierAddress: 120, supplierTin: 20
});

// Default / enforced tax (VAT) rate na PINIPIRMAHAN kasama ng profile. OPTIONAL: null = walang itinakda (kapareho ng dati:
// ang client ang may hawak ng rate nito). Kapag may numero, ito na ang rate na gagamitin ng OmniPOS at hindi na maiba ng client.
// Nalalagay lang sa signed payload kapag may numero (tingnan ang canonicalProfileString) kaya hindi naaapektuhan ang mga lumang profile.
function parseTaxRate(raw) {
    if (raw === undefined || raw === null || raw === '') return { ok: true, value: null };
    const n = typeof raw === 'number' ? raw : Number(String(raw).trim());
    if (!Number.isFinite(n) || n < 0 || n > 100) return { ok: false, message: 'Tax rate must be a number from 0 to 100 (e.g. 12), or left blank.' };
    return { ok: true, value: Math.round(n * 100) / 100 };
}

function emptyState() { return { profiles: {}, audit: [], updatedAt: null }; }

function cleanText(val, maxLen) {
    if (typeof val !== 'string') return '';
    return val.replace(/[\u0000-\u001f\u007f]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, maxLen);
}

function tinProblem(value, label, required) {
    if (!value) return required ? `${label} is required.` : null;
    const digits = value.replace(/\D/g, '');
    if (/[^0-9\-\s]/.test(value) || digits.length < 9 || digits.length > 14) {
        return `${label} must be numbers only (dashes/spaces allowed), 9 to 14 digits, e.g. 123-456-789-00000.`;
    }
    return null;
}

// Linisin + i-validate ang input ng admin form. Returns { ok:true, fields } | { ok:false, message }.
function validateFields(raw) {
    const r = (raw && typeof raw === 'object') ? raw : {};
    const f = {
        businessName: cleanText(r.businessName, FIELD_LIMITS.businessName),
        businessAddress: cleanText(r.businessAddress, FIELD_LIMITS.businessAddress),
        tin: cleanText(r.tin, FIELD_LIMITS.tin),
        vatStatus: r.vatStatus === 'NON_VAT' ? 'NON_VAT' : (r.vatStatus === 'VAT' ? 'VAT' : ''),
        ptuNo: cleanText(r.ptuNo, FIELD_LIMITS.ptuNo),
        ptuDate: /^\d{4}-\d{2}-\d{2}$/.test(String(r.ptuDate || '')) ? String(r.ptuDate) : '',
        min: cleanText(r.min, FIELD_LIMITS.min),
        serialNo: cleanText(r.serialNo, FIELD_LIMITS.serialNo),
        supplierName: cleanText(r.supplierName, FIELD_LIMITS.supplierName),
        supplierAddress: cleanText(r.supplierAddress, FIELD_LIMITS.supplierAddress),
        supplierTin: cleanText(r.supplierTin, FIELD_LIMITS.supplierTin)
    };
    const tr = parseTaxRate(r.taxRate);
    if (!tr.ok) return { ok: false, message: tr.message };
    // NON_VAT: walang VAT na kukuwentahin, kaya hindi nagdadala ng rate.
    f.taxRate = f.vatStatus === 'VAT' ? tr.value : null;
    if (!f.businessName) return { ok: false, message: 'Business name is required.' };
    if (!f.vatStatus) return { ok: false, message: 'Choose a VAT status (VAT or NON_VAT).' };
    if (!f.ptuNo) return { ok: false, message: 'PTU No. is required.' };
    if (!f.ptuDate) return { ok: false, message: 'PTU date issued is required (YYYY-MM-DD).' };
    const tinErr = tinProblem(f.tin, 'TIN', true) || tinProblem(f.supplierTin, 'Supplier TIN', false);
    if (tinErr) return { ok: false, message: tinErr };
    return { ok: true, fields: f };
}

// Fixed key order. Huwag baguhin nang hindi binabago ang OMNIPOS/bir-profile.js.
function canonicalProfileString(p) {
    const o = {
        installationId: p.installationId,
        deviceTag: p.deviceTag,
        status: p.status,
        version: p.version,
        issuedAt: p.issuedAt,
        businessName: p.businessName,
        tin: p.tin,
        vatStatus: p.vatStatus,
        ptuNo: p.ptuNo,
        ptuDate: p.ptuDate,
        min: p.min,
        serialNo: p.serialNo,
        supplierName: p.supplierName,
        supplierAddress: p.supplierAddress,
        supplierTin: p.supplierTin
    };
    // Must stay identical to OMNIPOS/bir-profile.js: businessAddress is signed only when it has a value.
    if (typeof p.businessAddress === 'string' && p.businessAddress !== '') o.businessAddress = p.businessAddress;
    // taxRate: signed ONLY when it is a number (Must stay identical to OMNIPOS/bir-profile.js). Always appended AFTER businessAddress.
    if (typeof p.taxRate === 'number' && Number.isFinite(p.taxRate)) o.taxRate = p.taxRate;
    return JSON.stringify(o);
}
function signedMessage(p) {
    return Buffer.from(SIGNING_DOMAIN + canonicalProfileString(p), 'utf8');
}

function normalizeState(raw) {
    const s = emptyState();
    if (!raw || typeof raw !== 'object') return s;
    s.updatedAt = typeof raw.updatedAt === 'number' ? raw.updatedAt : null;
    const src = raw.profiles && typeof raw.profiles === 'object' ? raw.profiles : {};
    for (const id of Object.keys(src).slice(0, MAX_PROFILES)) {
        const rec = src[id];
        if (!rec || typeof rec !== 'object' || id.length > 200) continue;
        const v = validateFields(rec.fields);
        const status = STATUSES.includes(rec.status) ? rec.status : 'active';
        // Ang revoked na record ay puwedeng wala nang kumpletong fields; panatilihin ang kaya.
        const fields = v.ok ? v.fields : Object.assign({}, rec.fields && typeof rec.fields === 'object' ? rec.fields : {});
        if (!v.ok && status !== 'revoked') continue;
        s.profiles[id] = {
            status,
            version: Number.isInteger(rec.version) && rec.version > 0 ? rec.version : 1,
            fields,
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

// Gumawa/palitan ang profile ng isang client. Ang version ay laging tumataas (anti-rollback sa client).
function upsertProfile(state, { installationId, fields, label, reason, now }) {
    const s = normalizeState(state);
    const t = typeof now === 'number' ? now : Date.now();
    const id = String(installationId || '').trim();
    if (!id || id.length > 200) throw new Error('A valid installationId is required.');
    const v = validateFields(fields);
    if (!v.ok) throw new Error(v.message);
    const prev = s.profiles[id];
    if (!prev && Object.keys(s.profiles).length >= MAX_PROFILES) throw new Error('Too many BIR profiles.');
    s.profiles[id] = {
        status: 'active',
        version: prev ? prev.version + 1 : 1,
        fields: v.fields,
        label: cleanText(label, 80) || (prev ? prev.label : ''),
        createdAt: prev ? prev.createdAt : t,
        updatedAt: t
    };
    s.updatedAt = t;
    pushAudit(s, {
        action: prev ? (prev.status === 'revoked' ? 'reinstate' : 'update') : 'create',
        installationId: id,
        previousVersion: prev ? prev.version : 0,
        version: s.profiles[id].version,
        // Pangalan lang ng binagong field ang itinatala (hindi ang laman), para ma-audit nang hindi ginagawang kopya ng TIN ang log.
        changedFields: prev ? Object.keys(v.fields).filter((k) => String((prev.fields || {})[k] ?? '') !== String(v.fields[k] ?? '')) : Object.keys(v.fields).filter((k) => String(v.fields[k] ?? '') !== ''),
        reason: cleanText(reason, 160)
    });
    return s;
}

// I-revoke: ang client ay magfa-fail-closed (walang BIR identity) hanggang may bagong active na profile.
function revokeProfile(state, { installationId, reason, now }) {
    const s = normalizeState(state);
    const t = typeof now === 'number' ? now : Date.now();
    const id = String(installationId || '').trim();
    const prev = s.profiles[id];
    if (!prev) throw new Error('No BIR profile exists for that installationId.');
    prev.status = 'revoked';
    prev.version += 1;
    prev.updatedAt = t;
    s.updatedAt = t;
    pushAudit(s, { action: 'revoke', installationId: id, previousVersion: prev.version - 1, version: prev.version, reason: cleanText(reason, 160) });
    return s;
}

function removeProfile(state, { installationId, reason, now }) {
    const s = normalizeState(state);
    const id = String(installationId || '').trim();
    if (!s.profiles[id]) throw new Error('No BIR profile exists for that installationId.');
    delete s.profiles[id];
    s.updatedAt = typeof now === 'number' ? now : Date.now();
    pushAudit(s, { action: 'delete', installationId: id, reason: cleanText(reason, 160) });
    return s;
}

// Pipirmahan ang kasalukuyang profile ng isang client. Returns null kung wala talagang record.
function signProfileFor(privateKey, state, installationId, now) {
    const s = normalizeState(state);
    const id = String(installationId || '').trim();
    const rec = s.profiles[id];
    if (!rec) return null;
    const f = rec.fields || {};
    const payload = {
        installationId: id,
        deviceTag: '*',
        status: rec.status,
        version: rec.version,
        issuedAt: typeof now === 'number' ? now : Date.now(),
        businessName: cleanText(f.businessName, FIELD_LIMITS.businessName),
        businessAddress: cleanText(f.businessAddress, FIELD_LIMITS.businessAddress),
        tin: cleanText(f.tin, FIELD_LIMITS.tin),
        vatStatus: f.vatStatus === 'NON_VAT' ? 'NON_VAT' : 'VAT',
        ptuNo: cleanText(f.ptuNo, FIELD_LIMITS.ptuNo),
        ptuDate: /^\d{4}-\d{2}-\d{2}$/.test(String(f.ptuDate || '')) ? String(f.ptuDate) : '',
        min: cleanText(f.min, FIELD_LIMITS.min),
        serialNo: cleanText(f.serialNo, FIELD_LIMITS.serialNo),
        supplierName: cleanText(f.supplierName, FIELD_LIMITS.supplierName),
        supplierAddress: cleanText(f.supplierAddress, FIELD_LIMITS.supplierAddress),
        supplierTin: cleanText(f.supplierTin, FIELD_LIMITS.supplierTin)
    };
    // Optional enforced tax rate: kasama lang sa payload kapag may numero at VAT ang status.
    const trParsed = parseTaxRate(f.taxRate);
    if (trParsed.ok && trParsed.value !== null && payload.vatStatus === 'VAT') payload.taxRate = trParsed.value;
    if (rec.status === 'revoked') {
        // Ang revoked na utos ay hindi na nagdadala ng identity.
        for (const k of PROFILE_FIELDS) payload[k] = k === 'vatStatus' ? 'VAT' : '';
        delete payload.taxRate;
    }
    const signature = crypto.sign(null, signedMessage(payload), privateKey).toString('base64');
    return { payload, signature };
}

function verifyProfileSignature(profile, publicKey) {
    try {
        return crypto.verify(null, signedMessage(profile.payload), publicKey, Buffer.from(profile.signature, 'base64'));
    } catch (_e) { return false; }
}

module.exports = {
    SIGNING_DOMAIN,
    PROFILE_FIELDS,
    emptyState,
    normalizeState,
    validateFields,
    parseTaxRate,
    canonicalProfileString,
    signedMessage,
    upsertProfile,
    revokeProfile,
    removeProfile,
    signProfileFor,
    verifyProfileSignature
};
