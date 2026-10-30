'use strict';
/*
 * Language pack validator (display-only translations).
 *
 * A pack is DATA only: { lang, name, native, version, exact:{English:Translated}, rules:[[regex, replacement]], rev:{Translated:English} }.
 * Nothing in a pack can run code, touch the database or change a setting. This module rebuilds a clean copy
 * of an untrusted pack (only known fields, bounded sizes) and rejects anything suspicious, so a broken or
 * tampered pack can never be installed. Same file is used by OMNIPOS and RELAY.
 */

const LANG_CODE_RE = /^[a-z]{2,3}(?:-[a-z0-9]{2,8})?$/;
const MAX_EXACT = 30000;
const MAX_RULES = 800;
const MAX_KEY = 400;
const MAX_VALUE = 600;
const MAX_PATTERN = 200;
const CONTROL_CHARS = /[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/;
// Nested quantifiers such as (a+)+ or (.*)* are the classic catastrophic-backtracking shapes.
const NESTED_QUANTIFIER = /\((?:[^()\\]|\\.)*[+*](?:[^()\\]|\\.)*\)[+*{]/;

function cleanText(v, max) {
  if (typeof v !== 'string') return null;
  const s = v.replace(/\s+/g, ' ').trim();
  if (!s || s.length > max || CONTROL_CHARS.test(s)) return null;
  return s;
}

function validateLangPack(pack, expectedCode) {
  if (!LANG_CODE_RE.test(String(expectedCode || ''))) return { ok: false, error: 'Invalid language code.' };
  if (!pack || typeof pack !== 'object' || Array.isArray(pack)) return { ok: false, error: 'The language pack is not a valid object.' };
  if (String(pack.lang || '').toLowerCase() !== expectedCode) return { ok: false, error: 'The pack does not match the language code.' };
  if (!pack.exact || typeof pack.exact !== 'object' || Array.isArray(pack.exact)) return { ok: false, error: 'The pack has no dictionary.' };

  const exact = {};
  let count = 0;
  for (const k of Object.keys(pack.exact)) {
    const key = cleanText(k, MAX_KEY);
    const val = cleanText(pack.exact[k], MAX_VALUE);
    if (!key || !val) continue;                 // skip bad entries instead of failing the whole pack
    exact[key] = val;
    if (++count > MAX_EXACT) return { ok: false, error: 'The pack is too large.' };
  }
  if (count < 20) return { ok: false, error: 'The pack has too few translations.' };

  const rules = [];
  for (const r of Array.isArray(pack.rules) ? pack.rules : []) {
    if (!Array.isArray(r) || r.length !== 2 || typeof r[0] !== 'string' || typeof r[1] !== 'string') continue;
    const pat = r[0];
    const rep = r[1];
    if (pat.length > MAX_PATTERN || rep.length > MAX_VALUE || CONTROL_CHARS.test(pat) || CONTROL_CHARS.test(rep)) continue;
    if (!pat.startsWith('^') || !pat.endsWith('$') || NESTED_QUANTIFIER.test(pat)) continue;
    let groups;
    try { groups = new RegExp(pat + '|').exec('').length - 1; } catch (e) { continue; }
    if (/\$[&'`$<]/.test(rep)) continue;        // only numbered references are allowed in the replacement
    let refsOk = true;
    rep.replace(/\\(\d)|\$(\d)/g, (m, a, b) => { if (Number(a || b) > groups || Number(a || b) < 1) refsOk = false; return m; });
    if (!refsOk) continue;
    rules.push([pat, rep]);
    if (rules.length >= MAX_RULES) break;
  }

  const rev = {};
  if (pack.rev && typeof pack.rev === 'object' && !Array.isArray(pack.rev)) {
    let n = 0;
    for (const k of Object.keys(pack.rev)) {
      const a = cleanText(k, MAX_VALUE);
      const b = cleanText(pack.rev[k], MAX_KEY);
      if (!a || !b) continue;
      rev[a] = b;
      if (++n >= MAX_EXACT) break;
    }
  }

  return {
    ok: true,
    count,
    pack: {
      lang: expectedCode,
      name: cleanText(pack.name, 40) || expectedCode,
      native: cleanText(pack.native, 40) || cleanText(pack.name, 40) || expectedCode,
      version: cleanText(pack.version, 40) || new Date().toISOString().slice(0, 10),
      exact, rules, rev
    }
  };
}

module.exports = { validateLangPack, LANG_CODE_RE };
