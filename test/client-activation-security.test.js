'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { hashClientSecret, secureRandomSecret, generateSecureOtp, generateShortActivationKey, normalizeShortActivationKey, hashShortActivationKey, diagnoseShortKeyInput, generatePairingCode, normalizePairingCode, validateActivation, validateCredential, validateInstallationScope } = require('../client-activation-security');
test('activation keys are stored as one-way hashes', () => {
  const key = secureRandomSecret('OMNI-ACT');
  assert.match(key, /^OMNI-ACT_[A-Za-z0-9_-]{43}$/);
  assert.notEqual(hashClientSecret(key), key);
  assert.equal(hashClientSecret(key), hashClientSecret(key));
});
test('invalid, revoked and expired activation keys are rejected', () => {
  assert.equal(validateActivation(null, 'install-12345678', 'a'.repeat(64)).code, 'ACTIVATION_KEY_INVALID');
  assert.equal(validateActivation({ revokedAt: 1 }, 'install-12345678', 'a'.repeat(64)).code, 'ACTIVATION_KEY_INVALID');
  assert.equal(validateActivation({ expiresAt: 99 }, 'install-12345678', 'a'.repeat(64), 100).code, 'ACTIVATION_KEY_INVALID');
});
test('device replacement requires approval and unchanged device can activate', () => {
  const license = { activatedInstallationId: 'old-install', deviceFingerprint: 'a'.repeat(64) };
  assert.equal(validateActivation(license, 'new-install', 'b'.repeat(64)).code, 'DEVICE_REPLACEMENT_REQUIRES_APPROVAL');
  assert.equal(validateActivation(license, 'old-install', 'a'.repeat(64)).ok, true);
});
test('invalid, revoked, expired and cross-device credentials are rejected', () => {
  assert.equal(validateCredential(null, 'install-12345678').code, 'CLIENT_CREDENTIAL_INVALID');
  assert.equal(validateCredential({ revokedAt: 1, installationId: 'install-12345678', fingerprint: 'a'.repeat(64) }, 'install-12345678', 'a'.repeat(64)).code, 'CLIENT_CREDENTIAL_INVALID');
  assert.equal(validateCredential({ expiresAt: 99, installationId: 'install-12345678', fingerprint: 'a'.repeat(64) }, 'install-12345678', 'a'.repeat(64), 100).code, 'CLIENT_CREDENTIAL_INVALID');
  assert.equal(validateCredential({ installationId: 'other-install', fingerprint: 'a'.repeat(64) }, 'install-12345678', 'a'.repeat(64)).code, 'CLIENT_DEVICE_MISMATCH');
  assert.equal(validateCredential({ installationId: 'install-12345678', fingerprint: 'a'.repeat(64) }, 'install-12345678', 'b'.repeat(64)).code, 'CLIENT_DEVICE_MISMATCH');
  assert.equal(validateCredential({ installationId: 'install-12345678', fingerprint: 'a'.repeat(64) }, 'install-12345678', 'a'.repeat(64)).ok, true);
});
test('server keeps master-key compatibility while supporting scoped client credentials', () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  assert.match(server, /RELAY_API_KEY && safeCompare\(provided, RELAY_API_KEY\)/);
  assert.match(server, /validateCredential\(record, suppliedInstallationId, suppliedFingerprint\)/);
  assert.match(server, /DEVICE_REPLACEMENT_REQUIRES_APPROVAL/);
  assert.match(server, /client-device-replacements\/approve/);
  assert.match(server, /x-relay-installation-id/);
  assert.match(server, /x-relay-device-fingerprint/);
  assert.match(server, /validateInstallationScope/);
  assert.match(server, /pg_advisory_xact_lock/);
  assert.match(server, /client-activations\/renew/);
  assert.match(server, /client-credentials\/reissue/);
  assert.match(server, /app.get\('\/relay\/admin\/api\/client-activations'/);
  const adminPage = fs.readFileSync(path.join(__dirname, '..', 'public', 'admin', 'client-activations.html'), 'utf8');
  assert.match(adminPage, /Generate key\(s\)/);
  assert.match(adminPage, /Approve replacement/);
});

test('client credential cannot target another installation through body, query, or route IDs', () => {
  assert.equal(validateInstallationScope('install-A', ['install-A']).ok, true);
  assert.equal(validateInstallationScope('install-A', [undefined, 'install-A']).ok, true);
  assert.equal(validateInstallationScope('install-A', ['install-B']).code, 'CLIENT_INSTALLATION_SCOPE_MISMATCH');
  assert.equal(validateInstallationScope('install-A', ['install-A', 'install-B']).ok, false);
});

test('OTP generator is cryptographic (six digits) and server.js never uses Math.random for OTPs', () => {
  for (let i = 0; i < 200; i++) assert.match(generateSecureOtp(), /^[1-9]\d{5}$/);
  const fs = require('node:fs');
  const path = require('node:path');
  const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  assert.doesNotMatch(server, /Math\.random\(\)\s*\*\s*900000/);
  assert.match(server, /\/relay\/admin\/api\/bir-passkeys\/issue/);
  assert.match(server, /RELAY_BIR_RESET_PASSKEY/);
});

test('short activation keys are 14 characters (12 + 2 dashes), easy to type, unique and normalised', () => {
  const seen = new Set();
  for (let i = 0; i < 5000; i++) {
    const key = generateShortActivationKey();
    assert.match(key, /^[A-HJ-NP-Z2-9]{4}-[A-HJ-NP-Z2-9]{4}-[A-HJ-NP-Z2-9]{4}$/);
    assert.ok(key.length >= 8 && key.length <= 15);
    assert.equal(/[01IO]/.test(key), false); // no look-alike characters
    seen.add(key);
  }
  assert.equal(seen.size, 5000);
  const key = generateShortActivationKey();
  const compact = key.replace(/-/g, '');
  assert.equal(normalizeShortActivationKey(key), compact);
  assert.equal(normalizeShortActivationKey(key.toLowerCase()), compact);
  assert.equal(normalizeShortActivationKey(` ${key.slice(0, 4)} ${key.slice(5, 9)} ${key.slice(10)} `), compact);
  assert.equal(normalizeShortActivationKey('OMNI-ACT_' + 'a'.repeat(43)), null); // legacy long keys are not short keys
  assert.equal(normalizeShortActivationKey('ABCD-EFGH'), null);
  assert.equal(normalizeShortActivationKey('ABCD-EFGH-JKL0'), null);
});

test('short activation keys are stored as a keyed hash (pepper matters)', () => {
  const k = 'ABCDEFGH2345';
  assert.equal(hashShortActivationKey(k, 'pepper-1'), hashShortActivationKey(k, 'pepper-1'));
  assert.notEqual(hashShortActivationKey(k, 'pepper-1'), hashShortActivationKey(k, 'pepper-2'));
  assert.notEqual(hashShortActivationKey(k, 'pepper-1'), hashClientSecret(k));
  assert.match(hashShortActivationKey(k, 'pepper-1'), /^[0-9a-f]{64}$/);
});

test('keys pasted from chat apps or word processors are still understood (typographic dashes, invisible characters, quotes)', () => {
  const key = generateShortActivationKey();
  const compact = key.replace(/-/g, '');
  const variants = [
    key.replace(/-/g, '\u2013'), key.replace(/-/g, '\u2014'), key.replace(/-/g, '\u2011'), key.replace(/-/g, '\u2212'), key.replace(/-/g, '\uFF0D'),
    key.slice(0, 4) + '\u200b' + key.slice(4), '\ufeff' + key, '\u201c' + key + '\u201d', "'" + key + "'", key + '.', key.replace(/-/g, '_'),
    key.replace(/-/g, ' '), key.toLowerCase(), key.replace(/-/g, '/')
  ];
  for (const v of variants) assert.equal(normalizeShortActivationKey(v), compact, JSON.stringify(v));
  // legacy long keys must still never be mistaken for short keys
  assert.equal(normalizeShortActivationKey(secureRandomSecret('OMNI-ACT')), null);
  assert.equal(diagnoseShortKeyInput(secureRandomSecret('OMNI-ACT')), null);
});

test('a mistyped short key gets a helpful diagnosis; unrelated text does not', () => {
  assert.equal(diagnoseShortKeyInput('ABCD-EFGH-JKL0'), 'ACTIVATION_KEY_AMBIGUOUS_CHARACTERS');
  assert.equal(diagnoseShortKeyInput('abcd efgh 1klm'), 'ACTIVATION_KEY_AMBIGUOUS_CHARACTERS');
  assert.equal(diagnoseShortKeyInput('ABCD-EFGH-JK'), 'ACTIVATION_KEY_WRONG_LENGTH');
  assert.equal(diagnoseShortKeyInput('ABCD-EFGH-JKLMN'), 'ACTIVATION_KEY_WRONG_LENGTH');
  assert.equal(diagnoseShortKeyInput('ABCD-EFGH-JKLM'), null);   // valid format, simply not issued
  assert.equal(diagnoseShortKeyInput('hello'), null);
  assert.equal(diagnoseShortKeyInput(''), null);
});

test('pairing codes are 8 characters (XXXX-XXXX) and are normalised like keys', () => {
  for (let i = 0; i < 500; i++) assert.match(generatePairingCode(), /^[A-HJ-NP-Z2-9]{4}-[A-HJ-NP-Z2-9]{4}$/);
  assert.equal(normalizePairingCode('abcd efgh'), 'ABCD-EFGH');
  assert.equal(normalizePairingCode('ABCD\u2013EFGH'), 'ABCD-EFGH');
  assert.equal(normalizePairingCode('ABCD-EFGH-JKLM'), null);
  assert.equal(normalizePairingCode('ABCD-EFG0'), null);
});

test('waiting-room wiring: public poll route, admin list/send routes, hint on mistyped keys, inbox cleared after activation', () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  assert.match(server, /app\.post\('\/relay\/client\/activation-inbox'/);
  assert.match(server, /app\.get\('\/relay\/admin\/api\/client-activation-inbox', requireAdminKey/);
  assert.match(server, /app\.post\('\/relay\/admin\/api\/client-activations\/send', requireAdminKey/);
  assert.match(server, /activationInbox\.clearDevice\(installationId, fingerprint\)/);
  assert.match(server, /diagnoseShortKeyInput\(activationKey\)/);
  assert.match(server, /RELAY_ACTIVATION_PEPPER is not set/);
  const adminPage = fs.readFileSync(path.join(__dirname, '..', 'public', 'admin', 'client-activations.html'), 'utf8');
  assert.match(adminPage, /Send to client/);
  assert.match(adminPage, /client-activations\/send/);
});
