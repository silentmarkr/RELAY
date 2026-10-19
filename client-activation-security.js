'use strict';
const crypto = require('crypto');
function hashClientSecret(value) { return crypto.createHash('sha256').update(String(value || '')).digest('hex'); }
function secureRandomSecret(prefix) { return `${prefix}_${crypto.randomBytes(32).toString('base64url')}`; }
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
module.exports = { hashClientSecret, secureRandomSecret, validateActivation, validateCredential, validateInstallationScope };
