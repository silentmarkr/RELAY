// apk-info.js (RELAY)
// -----------------------------------------------------------------------------
// Binabasa ang package name, versionCode at versionName MISMO SA LOOB ng APK (binary AndroidManifest.xml),
// para i-upload mo lang ang .apk at hindi ka na maglalagay ng version details. Walang external library.
// -----------------------------------------------------------------------------
'use strict';
const zlib = require('zlib');

function readZipEntry(buf, wanted) {
    let eocd = -1;
    for (let i = buf.length - 22; i >= Math.max(0, buf.length - 22 - 65535); i--) {
        if (buf.readUInt32LE(i) === 0x06054b50) { eocd = i; break; }
    }
    if (eocd < 0) throw new Error('Hindi valid na APK/zip (walang central directory).');
    const count = buf.readUInt16LE(eocd + 10);
    let p = buf.readUInt32LE(eocd + 16);
    for (let n = 0; n < count; n++) {
        if (p + 46 > buf.length || buf.readUInt32LE(p) !== 0x02014b50) break;
        const method = buf.readUInt16LE(p + 10);
        const compSize = buf.readUInt32LE(p + 20);
        const nameLen = buf.readUInt16LE(p + 28), extraLen = buf.readUInt16LE(p + 30), commentLen = buf.readUInt16LE(p + 32);
        const local = buf.readUInt32LE(p + 42);
        const name = buf.toString('utf8', p + 46, p + 46 + nameLen);
        if (name === wanted) {
            if (buf.readUInt32LE(local) !== 0x04034b50) throw new Error('Sira ang zip entry.');
            const start = local + 30 + buf.readUInt16LE(local + 26) + buf.readUInt16LE(local + 28);
            const data = buf.subarray(start, start + compSize);
            if (method === 0) return Buffer.from(data);
            if (method === 8) return zlib.inflateRawSync(data);
            throw new Error('Hindi suportadong compression sa APK.');
        }
        p += 46 + nameLen + extraLen + commentLen;
    }
    return null;
}

function parseManifest(x) {
    if (x.length < 16 || x.readUInt16LE(0) !== 0x0003) throw new Error('Hindi binary AndroidManifest ang laman.');
    let strings = [], resIds = [];
    let p = x.readUInt16LE(2);
    const end = Math.min(x.length, x.readUInt32LE(4));
    const readStr = (off, utf8) => {
        if (utf8) {
            let q = off;
            let l = x[q++]; if (l & 0x80) l = ((l & 0x7f) << 8) | x[q++];
            let b = x[q++]; if (b & 0x80) b = ((b & 0x7f) << 8) | x[q++];
            return x.toString('utf8', q, q + b);
        }
        let l = x.readUInt16LE(off), q = off + 2;
        if (l & 0x8000) { l = ((l & 0x7fff) << 16) | x.readUInt16LE(q); q += 2; }
        return x.toString('utf16le', q, q + l * 2);
    };
    while (p + 8 <= end) {
        const type = x.readUInt16LE(p), hs = x.readUInt16LE(p + 2), size = x.readUInt32LE(p + 4);
        if (size < 8) break;
        if (type === 0x0001) {
            const cnt = x.readUInt32LE(p + 8), flags = x.readUInt32LE(p + 16), sStart = x.readUInt32LE(p + 20);
            for (let i = 0; i < cnt; i++) strings.push(readStr(p + sStart + x.readUInt32LE(p + hs + i * 4), (flags & 0x100) !== 0));
        } else if (type === 0x0180) {
            for (let i = 0; i < (size - hs) / 4; i++) resIds.push(x.readUInt32LE(p + hs + i * 4));
        } else if (type === 0x0102) {
            const name = strings[x.readInt32LE(p + 20)];
            if (name === 'manifest') {
                const aStart = x.readUInt16LE(p + 24), aSize = x.readUInt16LE(p + 26), aCount = x.readUInt16LE(p + 28);
                const out = {};
                for (let i = 0; i < aCount; i++) {
                    const a = p + 16 + aStart + i * aSize;
                    const ni = x.readInt32LE(a + 4);
                    const an = strings[ni];
                    const rid = resIds[ni];
                    const raw = x.readInt32LE(a + 8), dtype = x[a + 15], data = x.readUInt32LE(a + 16);
                    const str = () => (raw >= 0 ? strings[raw] : (dtype === 0x03 ? strings[data] : null));
                    if (an === 'package') out.packageName = str();
                    else if (an === 'versionCode' || rid === 0x0101021b) out.versionCode = data;
                    else if (an === 'versionName' || rid === 0x0101021c) out.versionName = str();
                }
                return out;
            }
        }
        p += size;
    }
    throw new Error('Walang <manifest> sa AndroidManifest.');
}

/** @returns {{packageName:string, versionCode:number, versionName:string}} */
function readApkInfo(buf) {
    const xml = readZipEntry(buf, 'AndroidManifest.xml');
    if (!xml) throw new Error('Walang AndroidManifest.xml sa loob. Hindi ito APK.');
    const m = parseManifest(xml);
    if (!m.packageName || !Number.isInteger(m.versionCode) || m.versionCode < 1) throw new Error('Hindi mabasa ang package/versionCode sa APK.');
    return { packageName: m.packageName, versionCode: m.versionCode, versionName: String(m.versionName || m.versionCode).slice(0, 30) };
}

module.exports = { readApkInfo };
