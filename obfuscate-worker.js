// PATH: RELAY/obfuscate-worker.js
//
// Tumatakbo ito sa loob ng isang worker_threads Worker, HIWALAY sa main
// thread ng RELAY server.js. Ito ang fix para sa "nagfi-freeze ang build
// progress bar": ang JavaScriptObfuscator.obfuscate() ay isang mabigat,
// SYNCHRONOUS/CPU-bound na tawag — kung tatakbo ito diretso sa main
// thread ng RELAY, hinaharang nito ang buong event loop (kasama ang
// GET /relay/admin/api/build-progress na pina-poll ng admin panel) hanggang
// matapos ang pag-obfuscate ng bawat malaking file (hal. server.js,
// public/app.js). Dito, tumatakbo ito sa sariling thread, kaya malaya pa
// rin sumagot ang main thread habang tumatagal ang isang malaking file.
//
// Tumatanggap ito ng DALAWANG paraan ng input (workerData):
//   1. File mode:  { srcPath, destPath, options } — babasahin ang
//      srcPath, io-obfuscate, at isusulat ang resulta sa destPath
//      (pwedeng magkapareho ang dalawa para sa "in place" na file).
//   2. Code mode:  { code, options } (walang srcPath/destPath) —
//      direktang io-obfuscate ang string na ibinigay at ibabalik ang
//      resulta sa parent (ginagamit para sa inline <script> content ng
//      HTML files, na wala namang sarili nitong file sa disk).
const fs = require('fs');
const path = require('path');
const { parentPort, workerData } = require('worker_threads');
const JavaScriptObfuscator = require('javascript-obfuscator');

try {
    const { code, srcPath, destPath, options } = workerData;
    const source = typeof code === 'string' ? code : fs.readFileSync(srcPath, 'utf8');
    const result = JavaScriptObfuscator.obfuscate(source, options);
    const obfuscatedCode = result.getObfuscatedCode();

    if (destPath) {
        fs.mkdirSync(path.dirname(destPath), { recursive: true });
        fs.writeFileSync(destPath, obfuscatedCode, 'utf8');
        parentPort.postMessage({ ok: true });
    } else {
        parentPort.postMessage({ ok: true, obfuscatedCode });
    }
} catch (err) {
    parentPort.postMessage({ ok: false, error: (err && err.message) ? err.message : String(err) });
}
