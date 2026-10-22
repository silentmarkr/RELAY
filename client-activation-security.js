'use strict';
const crypto = require('crypto');
function hashClientSecret(value) { return crypto.createHash('sha256').update(String(value || '')).digest('hex'); }
// Cryptographically secure six-digit OTP (100000-999999). Math.random() is predictable and must never be used for OTPs.
function generateSecureOtp() { return String(crypto.randomInt(100000, 1000000)); }
function secureRandomSecret(prefix) { return `${prefix}_${crypto.randomBytes(32).toString('base64url')}`; }
// SHORT ACTIVATION KEYS: 12 characters shown as XXXX-XXXX-XXXX (14 with the dashes). The alphabet leaves out look-alike
// characters (no 0/O, 1/I), so a customer can read and type it easily. 32 symbols ^ 12 positions = 60 bits of randomness,
// drawn with crypto.randomInt. Safety does not rest on length alone: RELAY rate-limits activation attempts, a key works
// for one device only, and short keys are stored as a keyed hash (HMAC with a server-side pepper), so a leaked database
// alone cannot be brute-forced offline.
const SHORT_KEY_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const SHORT_KEY_LENGTH = 12;
function generateShortActivationKey() {
  let raw = '';
  for (let i = 0; i < SHORT_KEY_LENGTH; i++) raw += SHORT_KEY_ALPHABET[crypto.randomInt(SHORT_KEY_ALPHABET.length)];
  return `${raw.slice(0, 4)}-${raw.slice(4, 8)}-${raw.slice(8, 12)}`;
}
// Separators that a customer (or a chat app / word processor) may put between the groups: ordinary and typographic dashes
// (en/em dash, non-breaking hyphen, minus sign, full-width hyphen), underscores, dots, slashes, quotes, and invisible characters
// (zero-width space, BOM, soft hyphen). All of them are ignored; none of them can be part of a key.
const KEY_SEPARATORS = /[\s\u00AD\u200B-\u200D\u2060\uFEFF\u2010-\u2015\u2212\uFE58\uFE63\uFF0D\-_.,\/\\"'`\u2018\u2019\u201C\u201D]+/g;
function compactKeyInput(input) {
  return String(input == null ? '' : input).normalize('NFKC').replace(KEY_SEPARATORS, '').toUpperCase();
}
// Accepts what a customer might type (lowercase, spaces, typographic dashes, with or without dashes). Returns the 12-character
// canonical form, or null when the input is not a short key (long legacy keys such as OMNI-ACT_... are not affected).
function normalizeShortActivationKey(input) {
  const compact = compactKeyInput(input);
  return /^[A-HJ-NP-Z2-9]{12}$/.test(compact) ? compact : null;
}
// When an input is NOT a valid short key but clearly was meant to be one, say what is probably wrong so the customer can fix a typo
// instead of thinking the key is revoked. Returns null for anything else (legacy long keys, random text).
function diagnoseShortKeyInput(input) {
  const compact = compactKeyInput(input);
  if (!/^[A-Z0-9]{9,15}$/.test(compact)) return null;
  if (compact.length !== SHORT_KEY_LENGTH) return 'ACTIVATION_KEY_WRONG_LENGTH';
  return /[01IO]/.test(compact) ? 'ACTIVATION_KEY_AMBIGUOUS_CHARACTERS' : null;
}
// PAIRING CODES: 8 characters shown as XXXX-XXXX, same easy alphabet. The activation screen of a waiting client shows one so the
// developer can pick the right device when sending a key. It is a routing label only; it never activates anything.
const PAIRING_CODE_LENGTH = 8;
function generatePairingCode() {
  let raw = '';
  for (let i = 0; i < PAIRING_CODE_LENGTH; i++) raw += SHORT_KEY_ALPHABET[crypto.randomInt(SHORT_KEY_ALPHABET.length)];
  return `${raw.slice(0, 4)}-${raw.slice(4, 8)}`;
}
function normalizePairingCode(input) {
  const compact = compactKeyInput(input);
  return /^[A-HJ-NP-Z2-9]{8}$/.test(compact) ? `${compact.slice(0, 4)}-${compact.slice(4, 8)}` : null;
}
function hashShortActivationKey(normalizedKey, pepper) {
  return crypto.createHmac('sha256', String(pepper || '')).update(`omnipos-activation-v2:${normalizedKey}`).digest('hex');
}
function validateActivation(license, installationId, fingerprint, now = Date.now()) {
  if (!license || license.revokedAt || (license.expiresAt && license.expiresAt <= now)) return { ok: false, code: 'ACTIVATION_KEY_INVALID' };
  if (license.activatedInstallationId && license.activatedInstallationId !== installationId) return { ok: false, code: 'DEVICE_REPLACEMENT_REQUIRES_APPROVAL' };
  if (license.deviceFingerprint && license.deviceFingerprint !== fingerprint) return { ok: false, code: 'DEVICE_REPLACEMENT_REQUIRES_APPROVAL' };
  return { ok: true };
}
function validateInstallationScope(credentialInstallationId, targetInstallationIds) {
  if (!credentialInstallationId) return { ok: false, code: 'CLIENT_DEVICE_MISMATCH' };
  const targets = (Array.isArray(targetInstallationIds) ? targetInstallationIds : []).filter(v => v !== undefined && v !== null && String(v).trim() !== '').map(v => String(v).trim());
  return targets.some(id => id !== credentialInstallationId)
    ? { ok: false, code: 'CLIENT_INSTALLATION_SCOPE_MISMATCH' }
    : { ok: true };
}
function validateCredential(record, installationId, fingerprint, now = Date.now()) {
  if (!record || record.revokedAt || (record.expiresAt && record.expiresAt <= now)) return { ok: false, code: 'CLIENT_CREDENTIAL_INVALID' };
  if (!installationId || installationId !== record.installationId) return { ok: false, code: 'CLIENT_DEVICE_MISMATCH' };
  if (!fingerprint || !record.fingerprint || fingerprint !== record.fingerprint) return { ok: false, code: 'CLIENT_DEVICE_MISMATCH' };
  return { ok: true };
}
module.exports = { hashClientSecret, secureRandomSecret, generateSecureOtp, generateShortActivationKey, normalizeShortActivationKey, diagnoseShortKeyInput, generatePairingCode, normalizePairingCode, hashShortActivationKey, SHORT_KEY_LENGTH, validateActivation, validateCredential, validateInstallationScope };
