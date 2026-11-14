

const crypto = require('crypto');

function decodeCbor(buf, offset = 0) {
    const first = buf.readUInt8(offset);
    const majorType = first >> 5;
    const infoBits = first & 0x1f;
    let pos = offset + 1;

    function readLength(info) {
        if (info < 24) return { len: info, pos };
        if (info === 24) { const v = buf.readUInt8(pos); pos += 1; return { len: v, pos }; }
        if (info === 25) { const v = buf.readUInt16BE(pos); pos += 2; return { len: v, pos }; }
        if (info === 26) { const v = buf.readUInt32BE(pos); pos += 4; return { len: v, pos }; }
        if (info === 27) {
            const v = buf.readBigUInt64BE(pos); pos += 8;
            return { len: Number(v), pos };
        }
        throw new Error(`CBOR: hindi suportadong length encoding (info=${info})`);
    }

    switch (majorType) {
        case 0: { 
            const { len, pos: p } = readLength(infoBits);
            pos = p;
            return { value: len, pos };
        }
        case 1: { 
            const { len, pos: p } = readLength(infoBits);
            pos = p;
            return { value: -1 - len, pos };
        }
        case 2: { 
            const { len, pos: p } = readLength(infoBits);
            pos = p;
            const value = buf.subarray(pos, pos + len);
            return { value, pos: pos + len };
        }
        case 3: { 
            const { len, pos: p } = readLength(infoBits);
            pos = p;
            const value = buf.toString('utf8', pos, pos + len);
            return { value, pos: pos + len };
        }
        case 4: { 
            const { len, pos: p } = readLength(infoBits);
            pos = p;
            const arr = [];
            for (let i = 0; i < len; i++) {
                const r = decodeCbor(buf, pos);
                arr.push(r.value);
                pos = r.pos;
            }
            return { value: arr, pos };
        }
        case 5: { 
            const { len, pos: p } = readLength(infoBits);
            pos = p;
            const map = new Map();
            for (let i = 0; i < len; i++) {
                const k = decodeCbor(buf, pos);
                pos = k.pos;
                const v = decodeCbor(buf, pos);
                pos = v.pos;
                map.set(k.value, v.value);
            }
            return { value: map, pos };
        }
        case 7: { 
            if (infoBits === 20) return { value: false, pos };
            if (infoBits === 21) return { value: true, pos };
            if (infoBits === 22) return { value: null, pos };
            throw new Error(`CBOR: hindi suportadong simple value (info=${infoBits})`);
        }
        default:
            throw new Error(`CBOR: hindi suportadong major type ${majorType}`);
    }
}

function parseAuthenticatorData(authData) {
    if (authData.length < 37) throw new Error('authenticatorData: masyadong maikli.');

    const rpIdHash = authData.subarray(0, 32);
    const flagsByte = authData.readUInt8(32);
    const counter = authData.readUInt32BE(33);

    const flags = {
        userPresent: !!(flagsByte & 0x01),
        userVerified: !!(flagsByte & 0x04),
        attestedCredentialData: !!(flagsByte & 0x40),
        extensionData: !!(flagsByte & 0x80),
    };

    let pos = 37;
    let credentialId = null;
    let credentialPublicKey = null;

    if (flags.attestedCredentialData) {
        
        pos += 16;
        const credIdLen = authData.readUInt16BE(pos);
        pos += 2;
        credentialId = authData.subarray(pos, pos + credIdLen);
        pos += credIdLen;

        const { value: coseMap, pos: afterKey } = decodeCbor(authData, pos);
        credentialPublicKey = coseMap;
        pos = afterKey;

    }

    return { rpIdHash, flags, counter, credentialId, credentialPublicKey };
}

const COSE_EC_CURVES = { 1: 'P-256', 2: 'P-384', 3: 'P-521' };

function coseKeyToPublicKeyObject(coseMap) {
    const kty = coseMap.get(1); 
    const alg = coseMap.get(3);

    if (kty === 2) { 
        const crvId = coseMap.get(-1);
        const x = coseMap.get(-2);
        const y = coseMap.get(-3);
        const crv = COSE_EC_CURVES[crvId];
        if (!crv) throw new Error(`WebAuthn: hindi suportadong EC curve id (${crvId}).`);
        const jwk = {
            kty: 'EC',
            crv,
            x: Buffer.from(x).toString('base64url'),
            y: Buffer.from(y).toString('base64url'),
        };
        const keyObject = crypto.createPublicKey({ key: jwk, format: 'jwk' });
        return { keyObject, alg: 'ES256' };
    }

    if (kty === 3) { 
        const n = coseMap.get(-1);
        const e = coseMap.get(-2);
        const jwk = {
            kty: 'RSA',
            n: Buffer.from(n).toString('base64url'),
            e: Buffer.from(e).toString('base64url'),
        };
        const keyObject = crypto.createPublicKey({ key: jwk, format: 'jwk' });
        return { keyObject, alg: 'RS256' };
    }

    throw new Error(`WebAuthn: hindi suportadong COSE key type (kty=${kty}).`);
}

function verifySignature(keyObject, signedData, signature) {
    return crypto.createVerify('SHA256').update(signedData).verify(keyObject, signature);
}

function sha256(buf) {
    return crypto.createHash('sha256').update(buf).digest();
}

function randomChallenge() {
    return crypto.randomBytes(32).toString('base64url');
}

module.exports = {
    decodeCbor,
    parseAuthenticatorData,
    coseKeyToPublicKeyObject,
    verifySignature,
    sha256,
    randomChallenge,
};
