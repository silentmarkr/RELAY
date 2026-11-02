'use strict';
// AUTO-ACTIVATION STATE (persisted by server.js through getPersistentJSON/setPersistentJSON).
// Pure helpers only: no I/O, so they can be unit-tested. They convert the in-memory trackers to/from plain JSON and drop stale or malformed
// entries, so a corrupted or tampered stored value can never crash RELAY or grow memory without limit.
//
// What is kept across a RELAY restart:
//  - issuedAt     global list of issue timestamps (per-hour global limit)
//  - ipIssued     ip -> issue timestamps (per-IP hourly limit)
//  - issuedFor    installationId -> { licenseId, deviceName, deviceModel, at } (detects a name/model change after a key was issued)
//  - skipUntil    installationId -> time until which the device is not re-checked (it already has a license)
const HOUR_MS = 60 * 60 * 1000;
const ISSUED_FOR_TTL_MS = 24 * HOUR_MS;   // same as the unused-license TTL: after this the entry has no use
const MAX_ISSUED_AT = 500;
const MAX_IPS = 500;
const MAX_IP_TIMES = 50;
const MAX_ISSUED_FOR = 500;
const MAX_SKIP = 500;
const ID_RE = /^[a-zA-Z0-9._:-]{8,200}$/;
const TEXT_MAX = 60;

const isTime = (x, now) => typeof x === 'number' && Number.isFinite(x) && x > 0 && x <= now + 60 * 1000;   // a future time (clock skew > 1 min) is junk
const cleanText = (v) => (typeof v === 'string' ? v.slice(0, TEXT_MAX) : '');

// Empty trackers.
function emptyState() {
  return { issuedAt: [], ipIssued: new Map(), issuedFor: new Map(), skipUntil: new Map() };
}

// Saved JSON (or anything) -> trackers. Never throws.
function hydrateState(saved, now = Date.now()) {
  const out = emptyState();
  if (!saved || typeof saved !== 'object') return out;
  if (Array.isArray(saved.issuedAt)) {
    out.issuedAt = saved.issuedAt.filter((x) => isTime(x, now) && now - x < HOUR_MS).slice(-MAX_ISSUED_AT);
  }
  if (saved.ipIssued && typeof saved.ipIssued === 'object' && !Array.isArray(saved.ipIssued)) {
    for (const [ip, list] of Object.entries(saved.ipIssued)) {
      if (out.ipIssued.size >= MAX_IPS) break;
      if (!Array.isArray(list) || ip.length > 100) continue;
      const fresh = list.filter((x) => isTime(x, now) && now - x < HOUR_MS).slice(-MAX_IP_TIMES);
      if (fresh.length) out.ipIssued.set(ip, fresh);
    }
  }
  if (saved.issuedFor && typeof saved.issuedFor === 'object' && !Array.isArray(saved.issuedFor)) {
    const rows = [];
    for (const [id, v] of Object.entries(saved.issuedFor)) {
      if (!ID_RE.test(id) || !v || typeof v !== 'object' || typeof v.licenseId !== 'string' || !isTime(v.at, now) || now - v.at >= ISSUED_FOR_TTL_MS) continue;
      rows.push([id, { licenseId: v.licenseId.slice(0, 100), deviceName: cleanText(v.deviceName), deviceModel: cleanText(v.deviceModel), at: v.at }]);
    }
    rows.sort((a, b) => b[1].at - a[1].at);
    for (const [id, v] of rows.slice(0, MAX_ISSUED_FOR)) out.issuedFor.set(id, v);
  }
  if (saved.skipUntil && typeof saved.skipUntil === 'object' && !Array.isArray(saved.skipUntil)) {
    for (const [id, until] of Object.entries(saved.skipUntil)) {
      if (out.skipUntil.size >= MAX_SKIP) break;
      if (ID_RE.test(id) && typeof until === 'number' && Number.isFinite(until) && until > now && until <= now + 24 * HOUR_MS) out.skipUntil.set(id, until);
    }
  }
  return out;
}

// Trackers -> plain JSON (stale entries dropped). Never throws.
function serializeState(state, now = Date.now()) {
  const h = hydrateState({
    issuedAt: state.issuedAt,
    ipIssued: Object.fromEntries(state.ipIssued || []),
    issuedFor: Object.fromEntries(state.issuedFor || []),
    skipUntil: Object.fromEntries(state.skipUntil || []),
  }, now);
  return {
    v: 1,
    issuedAt: h.issuedAt,
    ipIssued: Object.fromEntries(h.ipIssued),
    issuedFor: Object.fromEntries(h.issuedFor),
    skipUntil: Object.fromEntries(h.skipUntil),
  };
}

module.exports = { emptyState, hydrateState, serializeState, HOUR_MS, ISSUED_FOR_TTL_MS };
