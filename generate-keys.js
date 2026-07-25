// ====================================================================
// generate-keys.js — Isang beses lang dapat patakbuhin ito (o kung
// gusto mong "i-revoke" ang lahat ng dating naibigay na unlock token
// at magsimula nang panibago).
//
// PAALALA: kung magpapatakbo ka nito ULIT sa isang relay na GUMAGANA
// NA (may mga kliyenteng may na-unlock na themes gamit ang LUMANG
// keypair), MAWAWALAN ng bisa ang lahat ng dating naibigay na unlock
// token — dahil hindi na tutugma ang public key na naka-embed sa mga
// lumang client server.js. Gamitin lang ito kung sinasadya mong i-reset
// ang LAHAT ng kliyente, o kung ito pa lang ang unang beses.
// ====================================================================
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const { publicKey, privateKey } = crypto.generateKeyPairSync('ed25519');

const privPem = privateKey.export({ type: 'pkcs8', format: 'pem' });
const pubPem = publicKey.export({ type: 'spki', format: 'pem' });

const privPath = path.join(__dirname, 'relay-private-key.pem');

if (fs.existsSync(privPath)) {
    console.error(`⚠️  MAYROON NANG '${privPath}'. Kung sadya mong papalitan ito, burahin muna ito manually bago patakbuhin ulit ang script na ito. Hindi awtomatikong ino-overwrite dahil DESTRUCTIVE ang aksyon na ito sa lahat ng existing na unlock token.`);
    process.exit(1);
}

fs.writeFileSync(privPath, privPem, { mode: 0o600 });

console.log('✅ Nagawa ang bagong keypair.\n');
console.log(`Private key na-save sa: ${privPath}`);
console.log('   -> IWAN ito dito sa relay server LANG. HUWAG kailanman isama sa client package o i-commit sa git.\n');
console.log('Public key (I-COPY ito at ilagay sa RELAY_PUBLIC_KEY_PEM constant sa client server.js):\n');
console.log(pubPem);
