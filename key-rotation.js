// key-rotation.js (RELAY)
// -----------------------------------------------------------------------------
// KEY ROTATION ng signing key. Gumagawa ng pirmadong "announcement" ng bagong public key para tanggapin ito ng mga naka-install
// na OmniPOS client nang hindi kailangan ng bagong APK. canonicalAnnouncement() at SIGNING_DOMAIN ay DAPAT byte-for-byte na
// kapareho ng nasa OMNIPOS/relay-keyring.js.
//
// BAGO MAG-ROTATE: kung ang ENV vault ay naka-encrypt gamit ang private key (tingnan ang env.html > Vault status), HINDI na ito
// made-decrypt kapag pinalitan ang private key. I-export muna ang laman ng vault, o gumamit ng RELAY_ENV_MASTER_KEY.
//
// PAMAMARAAN (i-set ang env sa Render):
//   1) npm run generate-next-key            -> gumagawa ng bagong keypair (relay-next-private-key.pem + public key sa screen)
//   2) Ilagay ang PUBLIC key sa RELAY_NEXT_PUBLIC_KEY_PEM at i-redeploy. Ina-announce na ito ng RELAY (pirmado ng KASALUKUYANG key).
//   3) Maghintay ng ilang araw hanggang ma-fetch ito ng lahat ng client (online). Tingnan ang /relay/admin/api/key-rotation/status.
//   4) Palitan ang RELAY_PRIVATE_KEY_PEM ng bagong private key, tanggalin ang RELAY_NEXT_PUBLIC_KEY_PEM, i-redeploy.
//   5) (Opsyonal, pagkatapos ng ilang linggo) RELAY_RETIRE_KIDS=<kid ng luma>: nire-retire ng bagong key ang luma sa mga client.
// HUWAG mag-switch ng private key (hakbang 4) hangga't hindi pa nakukuha ng mga client ang announcement; ang client na hindi pa
// nakakakuha ay mawawalan ng bisa ang lahat ng pirma hanggang mag-update ng APK.
// -----------------------------------------------------------------------------
'use strict';

const crypto = require('crypto');

const SIGNING_DOMAIN = 'omnipos-relay-key-announcement-v1\n';

function canonicalAnnouncement(p) {
    return JSON.stringify({
        type: p.type,
        kid: p.kid,
        publicKeyPem: p.publicKeyPem,
        issuedAt: p.issuedAt,
        retireKids: Array.isArray(p.retireKids) ? p.retireKids : []
    });
}
function signedMessage(p) { return Buffer.from(SIGNING_DOMAIN + canonicalAnnouncement(p), 'utf8'); }

function kidOfKey(keyObject) {
    const der = keyObject.export({ type: 'spki', format: 'der' });
    return crypto.createHash('sha256').update(der).digest('hex').slice(0, 16);
}

// Kinukuha ang isa o higit pang PEM block mula sa env string (tinatanggap ang "\n" na literal).
function parsePems(text) {
    const src = String(text || '').replace(/\\n/g, '\n');
    const blocks = src.match(/-----BEGIN PUBLIC KEY-----[\s\S]*?-----END PUBLIC KEY-----/g) || [];
    const out = [];
    for (const b of blocks) {
        try {
            const k = crypto.createPublicKey(b);
            if (k.asymmetricKeyType !== 'ed25519') continue;
            out.push({ key: k, pem: k.export({ type: 'spki', format: 'pem' }), kid: kidOfKey(k) });
        } catch (_e) { /* skip invalid */ }
    }
    return out;
}

function parseKids(text) {
    return String(text || '').split(/[\s,;]+/).map((s) => s.trim().toLowerCase()).filter((s) => /^[0-9a-f]{16}$/.test(s)).slice(0, 10);
}

// Gumagawa ng pirmadong announcement para sa bawat "next" key. `retireKids` ay ipinapadala kasama (kung mayroon).
function buildAnnouncements(privateKey, { nextPublicKeysText, retireKidsText, now } = {}) {
    const t = typeof now === 'number' ? now : Date.now();
    const nexts = parsePems(nextPublicKeysText).slice(0, 5);
    const retire = parseKids(retireKidsText);
    const list = nexts.length ? nexts : [];
    const out = [];
    for (const n of list) {
        const payload = { type: 'relay-key-announcement', kid: n.kid, publicKeyPem: n.pem, issuedAt: t, retireKids: retire };
        const signature = crypto.sign(null, signedMessage(payload), privateKey).toString('base64');
        out.push({ payload, signature });
    }
    // Retire-only: walang bagong key pero may lilimutin. Gamitin ang sariling public key ng signer bilang "kid" para tumugma ang hugis.
    if (!list.length && retire.length) {
        const own = crypto.createPublicKey(privateKey);
        const pem = own.export({ type: 'spki', format: 'pem' });
        const payload = { type: 'relay-key-announcement', kid: kidOfKey(own), publicKeyPem: pem, issuedAt: t, retireKids: retire };
        const signature = crypto.sign(null, signedMessage(payload), privateKey).toString('base64');
        out.push({ payload, signature });
    }
    return out;
}

function describe(privateKey, { nextPublicKeysText, retireKidsText } = {}) {
    const own = crypto.createPublicKey(privateKey);
    return {
        currentKid: kidOfKey(own),
        nextKids: parsePems(nextPublicKeysText).map((k) => k.kid),
        retireKids: parseKids(retireKidsText)
    };
}

module.exports = { SIGNING_DOMAIN, canonicalAnnouncement, signedMessage, kidOfKey, parsePems, parseKids, buildAnnouncements, describe };
