'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { createActivationInbox, ONLINE_MS, WAIT_ENTRY_TTL_MS, DELIVERY_TTL_MS, MAX_ENTRIES } = require('../client-activation-inbox');

const FP = 'a'.repeat(64);
const mk = () => { let t = 1_000_000; const inbox = createActivationInbox({ now: () => t }); return { inbox, tick: ms => { t += ms; }, at: () => t }; };
const dev = (n = 1, fp = FP) => ({ installationId: `install-${String(n).padStart(8, '0')}`, fingerprint: fp, ip: '1.2.3.4' });

test('a polling client is registered once and keeps the same pairing code', () => {
  const { inbox, tick } = mk();
  const a = inbox.poll(dev());
  assert.equal(a.ok, true);
  assert.match(a.pairingCode, /^[A-HJ-NP-Z2-9]{4}-[A-HJ-NP-Z2-9]{4}$/);
  assert.equal(a.delivery, null);
  tick(4000);
  assert.equal(inbox.poll(dev()).pairingCode, a.pairingCode);
  assert.equal(inbox._size(), 1);
});

test('invalid identifiers are refused', () => {
  const { inbox } = mk();
  assert.equal(inbox.poll({ installationId: 'x', fingerprint: FP }).code, 'INVALID_INPUT');
  assert.equal(inbox.poll({ installationId: 'install-00000001', fingerprint: 'zz' }).code, 'INVALID_INPUT');
  assert.equal(inbox.poll({}).code, 'INVALID_INPUT');
});

test('admin sends to a pairing code (any case, spaces) and the client receives it', () => {
  const { inbox } = mk();
  const { pairingCode } = inbox.poll(dev());
  const sent = inbox.send(pairingCode.toLowerCase().replace('-', ' '), 'ABCD-EFGH-JKLM', 'lic-1');
  assert.equal(sent.ok, true);
  const got = inbox.poll(dev());
  assert.equal(got.delivery.activationKey, 'ABCD-EFGH-JKLM');
  assert.equal(got.delivery.deliveryId, sent.deliveryId);
});

test('only the targeted device gets the delivery (other devices and a spoofed installationId do not)', () => {
  const { inbox } = mk();
  const victim = inbox.poll(dev(1));
  const other = inbox.poll(dev(2));
  const spoof = inbox.poll(dev(1, 'b'.repeat(64))); // knows installationId, not the fingerprint
  assert.notEqual(spoof.pairingCode, victim.pairingCode);
  inbox.send(victim.pairingCode, 'SECRET-KEY-1', 'lic');
  assert.equal(inbox.poll(dev(2)).delivery, null);
  assert.equal(inbox.poll(dev(1, 'b'.repeat(64))).delivery, null);
  assert.equal(inbox.poll(dev(1)).delivery.activationKey, 'SECRET-KEY-1');
  assert.notEqual(other.pairingCode, victim.pairingCode);
});

test('a second send replaces the first and is delivered as a new deliveryId', () => {
  const { inbox } = mk();
  const { pairingCode } = inbox.poll(dev());
  const s1 = inbox.send(pairingCode, 'FIRST-KEY-0001', 'l1');
  const first = inbox.poll(dev());
  const s2 = inbox.send(pairingCode, 'SECOND-KEY-002', 'l2');
  const second = inbox.poll(dev());
  assert.notEqual(s1.deliveryId, s2.deliveryId);
  assert.equal(first.delivery.activationKey, 'FIRST-KEY-0001');
  assert.equal(second.delivery.activationKey, 'SECOND-KEY-002');
  assert.equal(second.delivery.deliveryId, s2.deliveryId);
});

test('the delivery survives a page reload (repeat poll) until activation, then is wiped', () => {
  const { inbox } = mk();
  const { pairingCode } = inbox.poll(dev());
  inbox.send(pairingCode, 'KEEP-ME-123456', 'l');
  assert.ok(inbox.poll(dev()).delivery);
  assert.ok(inbox.poll(dev()).delivery);
  inbox.clearDevice(dev().installationId, dev().fingerprint);
  assert.equal(inbox._size(), 0);
  assert.equal(inbox.poll(dev()).delivery, null);
});

test('list() shows status but never the secret', () => {
  const { inbox } = mk();
  const { pairingCode } = inbox.poll(dev());
  inbox.send(pairingCode, 'TOP-SECRET-KEY', 'lic-9');
  assert.equal(JSON.stringify(inbox.list()).includes('TOP-SECRET-KEY'), false);
  assert.equal(inbox.list()[0].delivery.pickedUpAt, null);
  inbox.poll(dev());
  assert.ok(inbox.list()[0].delivery.pickedUpAt);
  assert.equal(inbox.list()[0].online, true);
});

test('cannot send to an unknown code, a malformed code, or a client that stopped polling', () => {
  const { inbox, tick } = mk();
  assert.equal(inbox.send('ABCD-EFGH', 'K', 'l').code, 'PAIRING_CODE_NOT_FOUND');
  assert.equal(inbox.send('nope', 'K', 'l').code, 'PAIRING_CODE_INVALID');
  const { pairingCode } = inbox.poll(dev());
  tick(ONLINE_MS + 1000);
  assert.equal(inbox.send(pairingCode, 'K', 'l').code, 'CLIENT_NOT_WAITING');
  assert.equal(inbox.list()[0].online, false);
});

test('idle entries expire; undelivered secrets expire after the TTL; a sent entry outlives its idle timeout', () => {
  const { inbox, tick } = mk();
  inbox.poll(dev(1));
  tick(WAIT_ENTRY_TTL_MS + 1);
  assert.equal(inbox.list().length, 0);
  const { pairingCode } = inbox.poll(dev(2));
  inbox.send(pairingCode, 'LATE-PICKUP-12', 'l');
  tick(WAIT_ENTRY_TTL_MS + 5000);                 // client closed the page, comes back later
  assert.equal(inbox.poll(dev(2)).delivery.activationKey, 'LATE-PICKUP-12');
  tick(DELIVERY_TTL_MS + 1);
  assert.equal(inbox.poll(dev(2)).delivery, null);
});

test('memory is bounded: oldest idle entry is evicted, entries holding a delivery are protected', () => {
  const { inbox, tick } = mk();
  const first = inbox.poll(dev(0));
  inbox.send(first.pairingCode, 'PROTECTED-KEY1', 'l');
  for (let i = 1; i < MAX_ENTRIES; i++) { tick(1); assert.equal(inbox.poll(dev(i)).ok, true); }
  assert.equal(inbox._size(), MAX_ENTRIES);
  tick(1);
  assert.equal(inbox.poll(dev(MAX_ENTRIES + 5)).ok, true);
  assert.equal(inbox._size(), MAX_ENTRIES);
  assert.equal(inbox.poll(dev(0)).delivery.activationKey, 'PROTECTED-KEY1');
});

test('pairing codes are unique among live entries', () => {
  const { inbox } = mk();
  const seen = new Set();
  for (let i = 0; i < 200; i++) seen.add(inbox.poll(dev(i)).pairingCode);
  assert.equal(seen.size, 200);
});
