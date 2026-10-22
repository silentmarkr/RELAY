'use strict';
const crypto = require('crypto');
const { generatePairingCode, normalizePairingCode } = require('./client-activation-security');

// ACTIVATION INBOX (kept in RELAY memory only).
// A not-yet-activated client keeps polling RELAY from its activation screen. RELAY registers it as "waiting" and gives it a pairing
// code to display. The developer picks the waiting device by that code and sends it an activation key (or a reissued credential);
// the client's next poll receives it. Nothing here activates anything: the key still has to be submitted to /relay/client/activate,
// which performs every real check. RELAY stays in control: the client can only receive what the admin sent to its own pairing code.
//
// Safety properties:
//  - entries are keyed by installationId + fingerprint, so a third party who only knows an installationId cannot read or block the
//    delivery meant for the real device;
//  - the plaintext secret lives only in memory, only until it is activated, replaced or expires (DELIVERY_TTL_MS), and is never
//    returned by list();
//  - an admin can only send to a client that polled within ONLINE_MS (it is really waiting right now);
//  - the number of entries is capped so unauthenticated polling cannot grow memory without limit.
const ONLINE_MS = 45 * 1000;            // a client that polled within this window counts as waiting
const WAIT_ENTRY_TTL_MS = 2 * 60 * 1000; // an entry with nothing to deliver disappears this long after its last poll
const DELIVERY_TTL_MS = 15 * 60 * 1000;  // a sent secret is kept this long (survives a page reload on the client)
const MAX_ENTRIES = 300;
const ID_RE = /^[a-zA-Z0-9._:-]{8,200}$/;
const FP_RE = /^[a-fA-F0-9]{32,128}$/;

function createActivationInbox(options = {}) {
  const now = typeof options.now === 'function' ? options.now : () => Date.now();
  const entries = new Map(); // entryId -> entry

  const entryIdOf = (installationId, fingerprint) =>
    crypto.createHash('sha256').update(`${installationId}\n${String(fingerprint).toLowerCase()}`).digest('hex');

  function sweep() {
    const t = now();
    for (const [id, e] of entries) {
      if (e.delivery && e.delivery.expiresAt <= t) e.delivery = null;
      if (!e.delivery && t - e.lastSeenAt >= WAIT_ENTRY_TTL_MS) entries.delete(id);
    }
  }
  function codeInUse(code) {
    for (const e of entries.values()) if (e.pairingCode === code) return true;
    return false;
  }
  function makeRoom() {
    if (entries.size < MAX_ENTRIES) return true;
    let oldestId = null; let oldestSeen = Infinity;
    for (const [id, e] of entries) if (!e.delivery && e.lastSeenAt < oldestSeen) { oldestSeen = e.lastSeenAt; oldestId = id; }
    if (oldestId === null) return false;
    entries.delete(oldestId);
    return true;
  }

  // Called by the waiting client. Returns its pairing code and, when the admin has sent something, the delivery.
  function poll({ installationId, fingerprint, ip }) {
    if (!ID_RE.test(String(installationId || '')) || !FP_RE.test(String(fingerprint || ''))) return { ok: false, code: 'INVALID_INPUT' };
    sweep();
    const t = now();
    const id = entryIdOf(installationId, fingerprint);
    let e = entries.get(id);
    if (!e) {
      if (!makeRoom()) return { ok: false, code: 'INBOX_FULL' };
      let code = generatePairingCode();
      for (let i = 0; i < 50 && codeInUse(code); i++) code = generatePairingCode();
      if (codeInUse(code)) return { ok: false, code: 'INBOX_FULL' };
      e = { pairingCode: code, installationId: String(installationId), fingerprint: String(fingerprint).toLowerCase(), firstSeenAt: t, lastSeenAt: t, ip: ip || null, delivery: null };
      entries.set(id, e);
    }
    e.lastSeenAt = t;
    if (ip) e.ip = ip;
    if (e.delivery && !e.delivery.pickedUpAt) e.delivery.pickedUpAt = t;
    return {
      ok: true,
      pairingCode: e.pairingCode,
      delivery: e.delivery ? { deliveryId: e.delivery.deliveryId, activationKey: e.delivery.secret } : null
    };
  }

  // Admin side. Returns the waiting device behind a pairing code (never any secret), or an error code.
  function lookup(pairingCodeInput) {
    sweep();
    const code = normalizePairingCode(pairingCodeInput);
    if (!code) return { ok: false, code: 'PAIRING_CODE_INVALID' };
    for (const e of entries.values()) {
      if (e.pairingCode !== code) continue;
      if (now() - e.lastSeenAt > ONLINE_MS) return { ok: false, code: 'CLIENT_NOT_WAITING' };
      return { ok: true, device: { pairingCode: e.pairingCode, installationId: e.installationId, fingerprint: e.fingerprint } };
    }
    return { ok: false, code: 'PAIRING_CODE_NOT_FOUND' };
  }

  // Admin side. The caller has already validated the secret against the license/credential store.
  // Sending again to the same device replaces whatever was sent before (and is delivered as a new deliveryId).
  function send(pairingCodeInput, secret, licenseId) {
    const found = lookup(pairingCodeInput);
    if (!found.ok) return found;
    const t = now();
    for (const e of entries.values()) {
      if (e.pairingCode !== found.device.pairingCode) continue;
      e.delivery = { deliveryId: crypto.randomUUID(), secret: String(secret), licenseId: licenseId || null, sentAt: t, pickedUpAt: null, expiresAt: t + DELIVERY_TTL_MS };
      return { ok: true, deliveryId: e.delivery.deliveryId, installationId: e.installationId, expiresAt: e.delivery.expiresAt };
    }
    return { ok: false, code: 'PAIRING_CODE_NOT_FOUND' };
  }

  // Admin side: what the dashboard shows. Contains no secrets.
  function list() {
    sweep();
    const t = now();
    return Array.from(entries.values())
      .sort((a, b) => b.lastSeenAt - a.lastSeenAt)
      .map(e => ({
        pairingCode: e.pairingCode,
        installationId: e.installationId,
        ip: e.ip,
        firstSeenAt: e.firstSeenAt,
        lastSeenAt: e.lastSeenAt,
        online: t - e.lastSeenAt <= ONLINE_MS,
        delivery: e.delivery ? { sentAt: e.delivery.sentAt, pickedUpAt: e.delivery.pickedUpAt, expiresAt: e.delivery.expiresAt, licenseId: e.delivery.licenseId } : null
      }));
  }

  // The device activated successfully: its waiting entry (and any stored secret) is removed immediately.
  function clearDevice(installationId, fingerprint) {
    if (!installationId || !fingerprint) return;
    entries.delete(entryIdOf(installationId, fingerprint));
  }
  function clearInstallation(installationId) {
    for (const [id, e] of entries) if (e.installationId === installationId) entries.delete(id);
  }

  return { poll, lookup, send, list, clearDevice, clearInstallation, _size: () => entries.size };
}

module.exports = { createActivationInbox, ONLINE_MS, WAIT_ENTRY_TTL_MS, DELIVERY_TTL_MS, MAX_ENTRIES };
