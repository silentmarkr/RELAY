// generate-next-key.js — gumagawa ng BAGONG keypair para sa key rotation (hindi ginagalaw ang kasalukuyang relay-private-key.pem).
// Tingnan ang key-rotation.js para sa buong pamamaraan.
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { kidOfKey } = require('./key-rotation');

const outPath = path.join(__dirname, 'relay-next-private-key.pem');
if (fs.existsSync(outPath)) {
    console.error(`⚠️  MAYROON NANG '${outPath}'. Burahin muna ito manually kung sadya mong gagawa ng panibago.`);
    process.exit(1);
}
const { publicKey, privateKey } = crypto.generateKeyPairSync('ed25519');
fs.writeFileSync(outPath, privateKey.export({ type: 'pkcs8', format: 'pem' }), { mode: 0o600 });
console.log('✅ Nagawa ang bagong keypair para sa rotation.\n');
console.log(`Private key: ${outPath}  (IWAN sa RELAY lang. HUWAG i-commit o ibahagi. Ito ang ilalagay sa RELAY_PRIVATE_KEY_PEM sa hakbang 4.)`);
console.log(`kid: ${kidOfKey(publicKey)}\n`);
console.log('Ilagay ito sa RELAY_NEXT_PUBLIC_KEY_PEM (hakbang 2):\n');
console.log(publicKey.export({ type: 'spki', format: 'pem' }));
