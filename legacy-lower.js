// PATH: OMNIPOS/legacy-lower.js   (identical copy lives at RELAY/legacy-lower.js)
//
// BUILD-TIME ONLY. Hindi ito ipinapadala sa client package (naka-exclude sa
// build-release.js at sa RELAY BUILD_EXCLUDE_NAMES).
//
// Bakit ito: ang Chrome/WebView 70-79 (hal. lumang Android phone gaya ng Y9)
// ay hindi nakakabasa ng `?.` (optional chaining) at `??` (nullish
// coalescing). Kapag may isang ganoong token sa isang script, ang BUONG
// script ay hindi tatakbo — kaya hindi gumagana ang login button at hindi
// nagbe-berde ang tuldok. Ibinababa ng module na ito ang syntax ng client
// scripts papuntang Chrome 70-compatible JavaScript BAGO ang obfuscation.
// Ang source files mo sa public/ ay HINDI ginagalaw.
//
// Pareho ang resulta sa lahat ng browser (luma man o bago): ang `a?.b`
// ay nagiging katumbas na `a == null ? void 0 : a.b`, kaya walang pagbabago
// sa behavior sa modernong device.
//
// Mga proteksyon:
//   * Kapag walang esbuild -> HUMIHINTO ang build na may malinaw na mensahe
//     (mas mabuti kaysa tahimik na magpadala ng sirang release sa lumang
//     phone). Para lampasan: LEGACY_LOWERING=off (hindi inirerekomenda).
//   * Pagkatapos ng conversion, vine-verify na WALANG natirang `?.`, `??`,
//     `??=`, `||=`, `&&=`, BigInt literal, class field o `#private` (kung
//     available ang `acorn`, pina-parse din bilang ES2019). Kapag may
//     natira -> HUMIHINTO ang build.
'use strict';

const LEGACY_LOWER_VERSION = 'esbuild-chrome70-v1';
const LEGACY_TARGET = ['chrome70'];

let _esbuild = null;
function loadEsbuild() {
  if (_esbuild) return _esbuild;
  try {
    _esbuild = require('esbuild');
  } catch (e) {
    const err = new Error(
      'Kailangan ng build ang "esbuild" para gumana ang OmniPOS sa lumang Android phone (Chrome 70+), ' +
      'pero hindi ito naka-install. Sa RELAY: patakbuhin ang `npm install` (nasa package.json na). ' +
      'Sa lokal na OMNIPOS build: `npm install --no-save esbuild`. ' +
      'Para lampasan lang sa ngayon: i-set ang LEGACY_LOWERING=off (hindi gagana ang app sa lumang phone).'
    );
    err.code = 'LEGACY_LOWER_ESBUILD_MISSING';
    throw err;
  }
  return _esbuild;
}

function loweringDisabled() {
  return String(process.env.LEGACY_LOWERING || '').toLowerCase() === 'off';
}

// Mabilis na pagsusuri sa output. Tinatanggal muna ang mga string/comment/regex
// literal gamit ang esbuild mismo? Hindi — gumagamit tayo ng acorn kung meron;
// kung wala, simpleng token scan na tumatanggap ng posibleng false alarm lang
// kung nasa loob ng string (kaya hindi ito ginagamit bilang tanging proteksyon).
function verifyLowered(output, label) {
  let acorn = null;
  try { acorn = require('acorn'); } catch (_) { /* optional */ }
  if (acorn) {
    try {
      acorn.parse(output, { ecmaVersion: 2019, sourceType: 'script' });
    } catch (e) {
      const err = new Error(`Legacy lowering verification failed for ${label}: output is not ES2019-valid (${e.message}).`);
      err.code = 'LEGACY_LOWER_VERIFY_FAILED';
      throw err;
    }
  }
  // Pangalawang tseke: ipasa ulit sa esbuild. Kapag may natirang syntax na
  // lampas sa target, babaguhin pa ito ng esbuild (hindi magkapareho ang
  // input at output). Ang tamang output ay hindi na nagbabago.
  const es = loadEsbuild();
  const again = es.transformSync(output, { loader: 'js', target: LEGACY_TARGET, logLevel: 'silent' }).code;
  const norm = (s) => s.replace(/\s+/g, ' ').trim();
  const a = es.transformSync(output, { loader: 'js', target: 'esnext', logLevel: 'silent' }).code;
  if (norm(again) !== norm(a)) {
    const err = new Error(`Legacy lowering verification failed for ${label}: modern syntax is still present after conversion.`);
    err.code = 'LEGACY_LOWER_VERIFY_FAILED';
    throw err;
  }
}

// Ibinababa ang JavaScript `code` para tumakbo sa Chrome 70+. Ibinabalik ang
// bagong code (string). Hindi nito binabago ang global scope semantics:
// walang IIFE wrapping, walang minify, walang rename.
function lowerForLegacy(code, label) {
  if (loweringDisabled()) return code;
  const es = loadEsbuild();
  let result;
  try {
    result = es.transformSync(code, {
      loader: 'js',
      target: LEGACY_TARGET,
      charset: 'utf8',
      legalComments: 'none',
      sourcefile: label || 'input.js',
      logLevel: 'silent',
    });
  } catch (e) {
    const detail = (e && e.errors && e.errors[0] && e.errors[0].text) ? e.errors[0].text : (e && e.message) || String(e);
    const err = new Error(`Legacy lowering failed for ${label || 'script'}: ${detail}`);
    err.code = 'LEGACY_LOWER_FAILED';
    throw err;
  }
  verifyLowered(result.code, label || 'script');
  return result.code;
}

module.exports = { lowerForLegacy, LEGACY_LOWER_VERSION, LEGACY_TARGET };
