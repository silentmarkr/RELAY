'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { hashClientSecret, secureRandomSecret, validateActivation, validateCredential, validateInstallationScope } = require('../client-activation-security');
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
