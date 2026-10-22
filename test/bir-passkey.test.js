'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const bir = require('../bir-passkey');

function makeRequestCode(overrides = {}) {
  const body = {
    v: 1, installationId: 'install-A', deviceTag: 'abcd1234', purpose: 'bir-hard-reset',
    nonce: crypto.randomBytes(16).toString('hex'), includeBirData: true, storeName: 'Test Store',
    createdAt: Date.now(), expiresAt: Date.now() + 3600 * 1000, summary: {}, ...overrides
  };
  return bir.REQUEST_PREFIX + Buffer.from(JSON.stringify(body)).toString('base64url');
}

test('request code is validated and the AGT reset can never carry BIR counters', () => {
  assert.equal(bir.decodeRequestCode(makeRequestCode()).ok, true);
  assert.equal(bir.decodeRequestCode('nope').code, 'REQUEST_CODE_MALFORMED');
  assert.equal(bir.decodeRequestCode(makeRequestCode({ purpose: 'other' })).ok, false);
  assert.equal(bir.decodeRequestCode(makeRequestCode({ purpose: 'bir-agt-reset', includeBirData: true })).ok, false);
});

test('pass key is signed with a domain-separated message and bound to the request', () => {
  const { publicKey, privateKey } = crypto.generateKeyPairSync('ed25519');
  const decoded = bir.decodeRequestCode(makeRequestCode());
  const issued = bir.issuePassKey({ request: decoded.request, privateKey, reference: 'TICKET-42', ttlMinutes: 99999 });
  assert.equal(bir.verifyPassKeySignature(issued.passKey, publicKey), true);
  // never longer than the hard maximum or the client's own request
  assert.ok(issued.payload.expiresAt - issued.payload.issuedAt <= bir.MAX_VALIDITY_MINUTES * 60 * 1000);
  assert.ok(issued.payload.expiresAt <= decoded.request.expiresAt);
  // a signature over the bare canonical string (no domain label) must NOT verify as a pass key
  const rawSig = crypto.sign(null, Buffer.from(bir.canonicalTicketString(issued.payload)), privateKey).toString('base64');
  const forged = bir.PASSKEY_PREFIX + Buffer.from(JSON.stringify({ payload: issued.payload, signature: rawSig })).toString('base64url');
  assert.equal(bir.verifyPassKeySignature(forged, publicKey), false);
  // tampering with the payload breaks the signature
  const parsed = JSON.parse(Buffer.from(issued.passKey.slice(bir.PASSKEY_PREFIX.length), 'base64url').toString());
  parsed.payload.includeBirData = !parsed.payload.includeBirData;
  const tampered = bir.PASSKEY_PREFIX + Buffer.from(JSON.stringify(parsed)).toString('base64url');
  assert.equal(bir.verifyPassKeySignature(tampered, publicKey), false);
});

test('a reference is mandatory and expired requests are refused', () => {
  const { privateKey } = crypto.generateKeyPairSync('ed25519');
  const decoded = bir.decodeRequestCode(makeRequestCode());
  assert.throws(() => bir.issuePassKey({ request: decoded.request, privateKey, reference: ' ' }), (e) => e.code === 'REFERENCE_REQUIRED');
  const expired = bir.decodeRequestCode(makeRequestCode({ expiresAt: Date.now() - 1000 }));
  assert.throws(() => bir.issuePassKey({ request: expired.request, privateKey, reference: 'TICKET-1' }), (e) => e.code === 'REQUEST_EXPIRED');
});

test('short developer passkey must be 8+ chars with 3 character classes and is compared in constant time', () => {
  const good = 'Kp7#mQ2x';
  assert.equal(bir.verifyDeveloperPasskey(good, good), true);
  assert.equal(bir.verifyDeveloperPasskey('Kp7#mQ2y', good), false);
  assert.equal(bir.verifyDeveloperPasskey(undefined, good), false);
  assert.equal(bir.isStrongDeveloperPasskey('abcdefgh'), false);   // one class
  assert.equal(bir.isStrongDeveloperPasskey('Abcdefgh'), false);   // two classes
  assert.equal(bir.isStrongDeveloperPasskey('Abcdef1'), false);    // 7 chars
  assert.equal(bir.isStrongDeveloperPasskey('Abcdef12'), true);
  assert.equal(bir.verifyDeveloperPasskey('abcdefgh', 'abcdefgh'), false); // weak configured secret is never accepted
});
