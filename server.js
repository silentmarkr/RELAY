// ====================================================================
// OmniPOS Unlock Relay — HIWALAY na maliit na service, hosted lang ng
// developer/owner (HINDI ito kasama sa client package na binebenta/
// dinideploy sa mga kliyente).
//
// LAYUNIN: dating nangyayari LAHAT (OTP generation, storage,
// verification) sa loob ng server ng bawat kliyente — kaya kahit
// technical lang ang isang kliyente, kayang basahin ang sariling
// database nila at makita ang OTP code mismo. Dito, LUMILIPAT ang
// buong desisyon kung "totoo ba ang unlock na ito" papunta rito — sa
// makinang HAWAK lang ng developer. Ang client server ay tumatawag
// lang dito sa network, at nagve-verify ng SIGNATURE gamit ang isang
// PUBLIC key (ligtas ipamahagi) — hindi nito kayang gumawa ng sarili
// niyang balidong approval kahit basahin niya lahat ng sariling code.
// ====================================================================

const express = require('express');
const crypto = require('crypto');
const nodemailer = require('nodemailer');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');
const fs = require('fs');

// I-load ang .env file papunta sa process.env — gamit ang BUILT-IN na
// loader ng Node (available sa Node 20.12+/22+, kapareho ng bersyon na
// kailangan para sa node:sqlite sa client server). Kung wala/hindi
// mabasa ang .env (hal. sa Render, kung saan sa dashboard mismo inilagay
// ang env vars), ituloy pa rin nang tahimik — babagsak na lang sa
// ibaba ang malinaw na "Kulang ang env vars" na check kung talagang
// wala pa ring nakuhang value.
try {
    process.loadEnvFile();
} catch (err) {
    // Walang nakitang .env file sa direktoryo na ito — okay lang, baka
    // ibang paraan (Render/Railway env vars dashboard) ang ginamit.
}

const app = express();
app.use(express.json());

// Static dev-only page (WebRTC viewer) — hindi ito naka-link kahit saan
// sa publiko, at protektado pa rin ng DEV_VIEWER_KEY sa loob ng WS
// connection mismo (ang static HTML/JS na ito ay walang access sa
// kahit anong session hangga't hindi tama ang key).
app.use('/relay/dev', express.static(path.join(__dirname, 'public')));

// --------------------------------------------------------------
// CONFIG — lahat ito ay dapat manggaling sa environment variables ng
// hosting mo (Render/VPS/atbp.), HINDI hardcoded dito sa source code.
// Tingnan ang .env.example para sa listahan.
// --------------------------------------------------------------
const PORT = process.env.PORT || 4477;
const RELAY_API_KEY = process.env.RELAY_API_KEY || null; // shared secret — pumipigil sa random tao (hindi mo kliyente) na mag-spam sa relay mo
const MAIL_USER = process.env.RELAY_MAIL_USER;
const MAIL_PASS = process.env.RELAY_MAIL_PASS;
const RECIPIENT_EMAIL = process.env.RELAY_RECIPIENT_EMAIL; // ang TOTOONG email mo — dito lang ito nakatira ngayon, hindi na sa client
const OTP_TTL_MS = 10 * 60 * 1000; // 10 minuto

// --------------------------------------------------------------
// DEVICE STORE — dalawang bagay ang tina-track dito:
//
// 1. "Allowed devices" (allowlist) — mga installationId na PWEDENG
//    gumamit ng relay. Naka-save sa isang JSON file (allowed-devices.json)
//    para hindi mawala kahit mag-restart/matulog ang free instance ng
//    Render. Nase-seed ito paunang beses mula sa RELAY_ALLOWED_DEVICES
//    env var (kung meron), pero pagkatapos non, ang FILE na ang
//    "source of truth" — dito nagagawa ang mga pagbabago mula sa admin
//    panel (hindi na kailangang balikan ang Render dashboard).
//
// 2. "Seen devices" — LAHAT ng installationId na kailanman gumawa ng
//    request dito, kasama ang huling nakitang storeName/username at
//    petsa. In-memory lang ito (nawawala kapag nag-restart ang
//    service) — gamit lang ito para makita mo sa admin panel kung anong
//    mga bagong device ang humihiling ng unlock, para madali mo silang
//    ma-"Allow" nang isang click na lang.
//
// PAALALA: dahil walang persistent disk add-on ang Render free tier,
// ang allowed-devices.json ay MAWAWALA sa susunod na REDEPLOY (git push)
// — hindi ito mawawala sa ordinaryong pagtulog/paggising (spin down/up)
// ng free instance, redeploy lang talaga. Kaya kung nag-set ka na ng
// allowlist via admin panel, tandaan/i-note ang mga ID bago ka mag-push
// ng panibagong code change, at i-restore mo ulit pagkatapos.
// --------------------------------------------------------------
const DEVICE_STORE_PATH = path.join(__dirname, 'allowed-devices.json');

function loadAllowedDevices() {
    try {
        const raw = fs.readFileSync(DEVICE_STORE_PATH, 'utf8');
        return new Set(JSON.parse(raw));
    } catch (err) {
        // Walang file pa (unang beses) — i-seed mula sa env var kung meron.
        const seed = (process.env.RELAY_ALLOWED_DEVICES || '')
            .split(',')
            .map(id => id.trim())
            .filter(Boolean);
        return new Set(seed);
    }
}

function saveAllowedDevices(set) {
    try {
        fs.writeFileSync(DEVICE_STORE_PATH, JSON.stringify([...set], null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang allowed-devices.json:', err);
    }
}

let allowedDevices = loadAllowedDevices();

// In-memory lang, para lang sa "recently seen" view sa admin panel.
const seenDevices = new Map(); // installationId -> { storeName, username, lastSeenAt, requestCount }

function recordDeviceSeen(installationId, meta = {}) {
    if (!installationId) return;
    const existing = seenDevices.get(installationId) || { requestCount: 0 };
    seenDevices.set(installationId, {
        storeName: meta.storeName || existing.storeName || null,
        username: meta.username || existing.username || null,
        lastSeenAt: Date.now(),
        requestCount: existing.requestCount + 1
    });
}

function requireAllowedDevice(req, res, next) {
    const { installationId, storeName, username } = req.body;
    recordDeviceSeen(installationId, { storeName, username }); // laging i-log, kahit tanggihan pagkatapos
    if (allowedDevices.size === 0) return next(); // walang allowlist naka-configure = walang restriction
    if (!installationId || !allowedDevices.has(installationId)) {
        return res.status(403).json({
            success: false,
            message: 'Hindi authorized ang device na ito para gumamit ng relay.'
        });
    }
    next();
}

// --------------------------------------------------------------
// ADMIN PANEL — /relay/admin (protektado ng sarili niyang password,
// HIWALAY sa RELAY_API_KEY). Dito mo makikita ang listahan ng mga
// device na kailanman humiling ng unlock, at pwede mo silang
// paganahin/tanggalin sa allowlist nang isang click na lang.
// --------------------------------------------------------------
const ADMIN_KEY = process.env.RELAY_ADMIN_KEY || null;
if (!ADMIN_KEY) {
    console.warn('⚠️  Walang RELAY_ADMIN_KEY na naka-set — hindi magagamit ang /relay/admin panel hangga\'t hindi ito nalagyan.');
}

function requireAdminKey(req, res, next) {
    const provided = req.headers['x-relay-admin-key'] || req.query.key;
    if (!ADMIN_KEY || provided !== ADMIN_KEY) {
        return res.status(403).json({ success: false, message: 'Invalid o walang admin key.' });
    }
    next();
}

app.use('/relay/admin', express.static(path.join(__dirname, 'public', 'admin')));

app.get('/relay/admin/api/devices', requireAdminKey, (req, res) => {
    const seen = [...seenDevices.entries()].map(([installationId, meta]) => ({
        installationId,
        ...meta,
        allowed: allowedDevices.has(installationId)
    })).sort((a, b) => b.lastSeenAt - a.lastSeenAt);

    res.json({
        success: true,
        seenDevices: seen,
        allowedDevices: [...allowedDevices],
        restrictionActive: allowedDevices.size > 0
    });
});

app.post('/relay/admin/api/devices/allow', requireAdminKey, (req, res) => {
    const { installationId } = req.body;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }
    allowedDevices.add(installationId);
    saveAllowedDevices(allowedDevices);
    res.json({ success: true, allowedDevices: [...allowedDevices] });
});

app.post('/relay/admin/api/devices/revoke', requireAdminKey, (req, res) => {
    const { installationId } = req.body;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }
    allowedDevices.delete(installationId);
    saveAllowedDevices(allowedDevices);
    res.json({ success: true, allowedDevices: [...allowedDevices] });
});

// Hiwalay na secret ito sa RELAY_API_KEY — ang RELAY_API_KEY ay hawak ng
// bawat CLIENT SERVER (maraming kliyente, posibleng mas mahina ang
// proteksyon sa kanilang deployment). Ang DEV_VIEWER_KEY ay hawak MO
// LANG, sa dev-viewer page na ikaw lang gagamit — kailangan ito bago
// makapasok bilang "viewer" sa kahit anong live screen-share session.
const DEV_VIEWER_KEY = process.env.RELAY_DEV_VIEWER_KEY || null;
if (!DEV_VIEWER_KEY) {
    console.warn('⚠️  Walang RELAY_DEV_VIEWER_KEY na naka-set — hindi magagamit ang Watch-My-Screen dev viewer hangga\'t hindi ito nalagyan.');
}

if (!MAIL_USER || !MAIL_PASS || !RECIPIENT_EMAIL) {
    console.error('❌ Kulang ang env vars: RELAY_MAIL_USER, RELAY_MAIL_PASS, RELAY_RECIPIENT_EMAIL. Tingnan ang .env.example.');
    process.exit(1);
}

// --------------------------------------------------------------
// SIGNING KEY — binabasa mula sa file na ginawa ng generate-keys.js.
// Kailangan itong pumasok bilang environment variable kung naka-deploy
// sa Render (walang persistent disk sa free tier) — RELAY_PRIVATE_KEY_PEM.
// Kung may sarili kang VPS na may disk, pwede ring basahin mula sa file.
// --------------------------------------------------------------
let privateKeyPem = process.env.RELAY_PRIVATE_KEY_PEM;
if (!privateKeyPem) {
    const keyPath = path.join(__dirname, 'relay-private-key.pem');
    if (fs.existsSync(keyPath)) {
        privateKeyPem = fs.readFileSync(keyPath, 'utf8');
    }
}
if (!privateKeyPem) {
    console.error('❌ Walang nakitang private key. Patakbuhin muna ang "npm run generate-keys", o i-set ang RELAY_PRIVATE_KEY_PEM env var.');
    process.exit(1);
}
const privateKey = crypto.createPrivateKey(privateKeyPem);

// --------------------------------------------------------------
// STORAGE — simpleng in-memory Map lang para sa mga PENDING OTP.
// Hindi kailangan ng persistent database dito dahil ang mga OTP ay
// panandalian lang (10-minute TTL) — kung mag-restart ang relay
// habang may pending OTP, kailangan na lang humingi ulit ng bago.
// Ang "totoong" resulta (ang naka-sign na token) ay ang TANGING
// kailangang mabuhay nang matagal, at yun ay naka-save na sa panig
// ng CLIENT (ang bawat kliyente ang nag-iingat ng sarili nilang token).
// --------------------------------------------------------------
const pendingOtps = new Map(); // key: `${installationId}:${featureId}` -> { code, expiresAt, requestedBy, featureName, price }

function requireApiKey(req, res, next) {
    if (!RELAY_API_KEY) return next(); // walang na-configure na key = walang gate (hindi rekomendado, pero valid config)
    const provided = req.headers['x-relay-key'];
    if (provided !== RELAY_API_KEY) {
        return res.status(403).json({ success: false, message: 'Invalid o walang API key.' });
    }
    next();
}

// Napaka-simpleng in-memory rate limiter (per key sa Map, hindi kailangan
// ng Redis o external store dahil isang maliit na relay lang ito).
const rateBuckets = new Map();
function rateLimit(bucketName, max, windowMs) {
    return (req, res, next) => {
        const key = `${bucketName}:${req.ip}`;
        const now = Date.now();
        const bucket = rateBuckets.get(key) || [];
        const recent = bucket.filter(ts => now - ts < windowMs);
        if (recent.length >= max) {
            return res.status(429).json({ success: false, message: 'Sobra sa pinapayagang bilang ng requests. Subukan mamaya.' });
        }
        recent.push(now);
        rateBuckets.set(key, recent);
        next();
    };
}

// --------------------------------------------------------------
// POST /relay/request-unlock
// Tinatawag ito ng CLIENT server (hindi diretso ng browser ng cashier)
// tuwing may humihiling mag-unlock ng isang Pro theme.
// --------------------------------------------------------------
app.post('/relay/request-unlock', requireApiKey, requireAllowedDevice, rateLimit('request-unlock', 5, 10 * 60 * 1000), async (req, res) => {
    const { installationId, featureId, featureName, price, username, storeName } = req.body;

    if (!installationId || !featureId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId o featureId.' });
    }

    const otpCode = String(Math.floor(100000 + Math.random() * 900000));
    const key = `${installationId}:${featureId}`;
    pendingOtps.set(key, {
        code: otpCode,
        expiresAt: Date.now() + OTP_TTL_MS,
        requestedBy: username || 'Unknown',
        featureName: featureName || featureId,
        price: price || null
    });

    try {
        const transporter = nodemailer.createTransport({
            service: 'gmail',
            auth: { user: MAIL_USER, pass: MAIL_PASS }
        });

        await transporter.sendMail({
            from: `"OmniPOS Unlock Relay" <${MAIL_USER}>`,
            to: RECIPIENT_EMAIL,
            subject: `🎨 Unlock Request — ${featureName || featureId}${price ? ` (₱${price})` : ''}`,
            text: `May humiling na i-unlock ang isang Pro theme.\n\n` +
                  `Store: ${storeName || 'Hindi tiyak'}\n` +
                  `Installation ID: ${installationId}\n` +
                  `Feature: ${featureName || featureId}\n` +
                  (price ? `Presyo: ₱${price}\n` : '') +
                  `Hiniling ni: ${username || 'Unknown'}\n` +
                  `OTP Code: ${otpCode}\n` +
                  `Mag-e-expire ito sa loob ng 10 minuto.\n\n` +
                  `I-verify muna ang bayad bago ibigay ang OTP na ito sa kliyente.`
        });

        res.json({ success: true, message: 'Naipadala ang OTP request.' });
    } catch (err) {
        console.error('Relay mail send failure:', err);
        pendingOtps.delete(key);
        res.status(500).json({ success: false, message: `Nabigo ang pagpapadala ng OTP: ${err.message}` });
    }
});

// --------------------------------------------------------------
// POST /relay/confirm-unlock
// Kapag TAMA ang OTP, gagawa ito ng isang SIGNED TOKEN gamit ang
// private key. Ang token na ito ang ibabalik sa client — ito na ang
// magiging "resibo" ng pagka-unlock, at maaaring i-verify kahit
// offline (walang internet) gamit lang ang public key.
// --------------------------------------------------------------
app.post('/relay/confirm-unlock', requireApiKey, requireAllowedDevice, rateLimit('confirm-unlock', 10, 10 * 60 * 1000), (req, res) => {
    const { installationId, featureId, otp } = req.body;

    if (!installationId || !featureId || !otp) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId, featureId, o otp.' });
    }

    const key = `${installationId}:${featureId}`;
    const pending = pendingOtps.get(key);

    if (!pending) {
        return res.status(400).json({ success: false, message: 'Walang aktibong unlock request para dito. Humingi muna ng OTP.' });
    }
    if (Date.now() > pending.expiresAt) {
        pendingOtps.delete(key);
        return res.status(400).json({ success: false, message: 'Expired na ang OTP code. Humingi ng bago.' });
    }
    if (String(otp).trim() !== pending.code) {
        return res.status(400).json({ success: false, message: 'Maling OTP code.' });
    }

    // Tama ang OTP — gumawa ng naka-sign na token. Ang payload ay
    // nagta-tali ng token na ito SA SPESIPIKONG installationId+featureId,
    // kaya hindi ito magagamit sa ibang installation o ibang theme.
    const payload = {
        installationId,
        featureId,
        issuedAt: Date.now()
    };
    const payloadString = JSON.stringify(payload);
    const signature = crypto.sign(null, Buffer.from(payloadString), privateKey).toString('base64');

    pendingOtps.delete(key);

    res.json({
        success: true,
        message: `Na-unlock ang ${pending.featureName}!`,
        token: { payload, signature }
    });
});

// --------------------------------------------------------------
// DEMO MODE — pansamantalang bubuksan ang LAHAT ng features (walang
// paywall) para sa isang installation, pero:
//   1) kailangan pa ring humingi/mag-verify ng OTP (parang unlock din,
//      kaya kontrolado pa rin ng developer kung sino/ilang beses ito
//      maibibigay), at
//   2) may EXPIRY na naka-bake sa mismong signed token (RELAY_DEMO_
//      DURATION_HOURS, default 24 oras) — kaya kahit i-save ng
//      kliyente ang token, mag-e-expire pa rin ito nang mag-isa sa
//      panig ng client server nang hindi na kailangang mag-check pa
//      ulit dito sa relay.
// --------------------------------------------------------------
const DEMO_FEATURE_ID = '__demo__';
const DEMO_DURATION_MS = (Number(process.env.RELAY_DEMO_DURATION_HOURS) || 24) * 60 * 60 * 1000;

app.post('/relay/request-demo', requireApiKey, requireAllowedDevice, rateLimit('request-demo', 5, 10 * 60 * 1000), async (req, res) => {
    const { installationId, username, storeName } = req.body;

    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }

    const otpCode = String(Math.floor(100000 + Math.random() * 900000));
    const key = `${installationId}:${DEMO_FEATURE_ID}`;
    pendingOtps.set(key, {
        code: otpCode,
        expiresAt: Date.now() + OTP_TTL_MS,
        requestedBy: username || 'Unknown',
        featureName: 'Full Demo Mode',
        price: null
    });

    try {
        const transporter = nodemailer.createTransport({
            service: 'gmail',
            auth: { user: MAIL_USER, pass: MAIL_PASS }
        });

        await transporter.sendMail({
            from: `"OmniPOS Unlock Relay" <${MAIL_USER}>`,
            to: RECIPIENT_EMAIL,
            subject: `🕒 Demo Mode Request — ${storeName || 'Hindi tiyak'}`,
            text: `May humiling ng FULL DEMO MODE (lahat ng features, ${Math.round(DEMO_DURATION_MS / 3600000)} oras lang bago mag-expire).\n\n` +
                  `Store: ${storeName || 'Hindi tiyak'}\n` +
                  `Installation ID: ${installationId}\n` +
                  `Hiniling ni: ${username || 'Unknown'}\n` +
                  `OTP Code: ${otpCode}\n` +
                  `Mag-e-expire ang OTP code na ito sa loob ng 10 minuto.\n\n` +
                  `Ibigay lang ito kung gusto mo talagang bigyan sila ng full trial.`
        });

        res.json({ success: true, message: 'Naipadala ang demo OTP request.' });
    } catch (err) {
        console.error('Relay mail send failure (demo):', err);
        pendingOtps.delete(key);
        res.status(500).json({ success: false, message: `Nabigo ang pagpapadala ng OTP: ${err.message}` });
    }
});

app.post('/relay/confirm-demo', requireApiKey, requireAllowedDevice, rateLimit('confirm-demo', 10, 10 * 60 * 1000), (req, res) => {
    const { installationId, otp } = req.body;

    if (!installationId || !otp) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId o otp.' });
    }

    const key = `${installationId}:${DEMO_FEATURE_ID}`;
    const pending = pendingOtps.get(key);

    if (!pending) {
        return res.status(400).json({ success: false, message: 'Walang aktibong demo request para dito. Humingi muna ng OTP.' });
    }
    if (Date.now() > pending.expiresAt) {
        pendingOtps.delete(key);
        return res.status(400).json({ success: false, message: 'Expired na ang OTP code. Humingi ng bago.' });
    }
    if (String(otp).trim() !== pending.code) {
        return res.status(400).json({ success: false, message: 'Maling OTP code.' });
    }

    const now = Date.now();
    // MAHALAGA: kasama na ang `expiresAt` sa payload (hindi tulad ng
    // regular na paid-feature tokens) — ito ang gumagawang "time-limited"
    // sa demo. Ang parehong key order na ito ay dapat itugma nang eksakto
    // sa verifyUnlockToken() sa panig ng OMNIPOS client server.
    const payload = {
        installationId,
        featureId: DEMO_FEATURE_ID,
        issuedAt: now,
        expiresAt: now + DEMO_DURATION_MS
    };
    const payloadString = JSON.stringify(payload);
    const signature = crypto.sign(null, Buffer.from(payloadString), privateKey).toString('base64');

    pendingOtps.delete(key);

    res.json({
        success: true,
        message: `Buksan na ang Demo Mode sa loob ng ${Math.round(DEMO_DURATION_MS / 3600000)} oras!`,
        token: { payload, signature }
    });
});

// --------------------------------------------------------------
// BULK/BUNDLE UNLOCK — parang /relay/request-unlock + /relay/confirm-
// unlock sa itaas, pero ISANG OTP na lang ang ginagawa para sa
// MARAMING featureIds nang sabay (isang tier o custom na à la carte
// selection mula sa upgrade modal). Sa /confirm-unlock-bulk, GUMAGAWA
// pa rin ng HIWALAY na signed token PER featureId (parehong eksaktong
// format ng single-feature token) — kaya walang epekto ito sa
// verifyUnlockToken() sa panig ng client, pang-convenience lang ito sa
// itaas ng parehong mekanismo.
// --------------------------------------------------------------
app.post('/relay/request-unlock-bulk', requireApiKey, requireAllowedDevice, rateLimit('request-unlock-bulk', 5, 10 * 60 * 1000), async (req, res) => {
    const { installationId, featureIds, featureNames, totalPrice, username, storeName } = req.body;

    if (!installationId || !Array.isArray(featureIds) || featureIds.length === 0) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId o featureIds.' });
    }

    const otpCode = String(Math.floor(100000 + Math.random() * 900000));
    const key = `${installationId}:__bulk__:${featureIds.slice().sort().join(',')}`;
    pendingOtps.set(key, {
        code: otpCode,
        expiresAt: Date.now() + OTP_TTL_MS,
        requestedBy: username || 'Unknown',
        featureIds,
        featureNames: featureNames || featureIds,
        price: totalPrice || null
    });

    try {
        const transporter = nodemailer.createTransport({
            service: 'gmail',
            auth: { user: MAIL_USER, pass: MAIL_PASS }
        });

        await transporter.sendMail({
            from: `"OmniPOS Unlock Relay" <${MAIL_USER}>`,
            to: RECIPIENT_EMAIL,
            subject: `📦 Bundle Unlock Request (${featureIds.length} items)${totalPrice ? ` — ₱${totalPrice}` : ''}`,
            text: `May humiling na i-unlock ang isang BUNDLE ng ${featureIds.length} feature(s).\n\n` +
                  `Store: ${storeName || 'Hindi tiyak'}\n` +
                  `Installation ID: ${installationId}\n` +
                  `Features: ${(featureNames || featureIds).join(', ')}\n` +
                  (totalPrice ? `Total Presyo: ₱${totalPrice}\n` : '') +
                  `Hiniling ni: ${username || 'Unknown'}\n` +
                  `OTP Code: ${otpCode}\n` +
                  `Mag-e-expire ito sa loob ng 10 minuto.\n\n` +
                  `I-verify muna ang bayad bago ibigay ang OTP na ito sa kliyente.`
        });

        res.json({ success: true, message: 'Naipadala ang bundle OTP request.' });
    } catch (err) {
        console.error('Relay mail send failure (bulk):', err);
        pendingOtps.delete(key);
        res.status(500).json({ success: false, message: `Nabigo ang pagpapadala ng OTP: ${err.message}` });
    }
});

app.post('/relay/confirm-unlock-bulk', requireApiKey, requireAllowedDevice, rateLimit('confirm-unlock-bulk', 10, 10 * 60 * 1000), (req, res) => {
    const { installationId, featureIds, otp } = req.body;

    if (!installationId || !Array.isArray(featureIds) || featureIds.length === 0 || !otp) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId, featureIds, o otp.' });
    }

    const key = `${installationId}:__bulk__:${featureIds.slice().sort().join(',')}`;
    const pending = pendingOtps.get(key);

    if (!pending) {
        return res.status(400).json({ success: false, message: 'Walang aktibong bundle unlock request para dito. Humingi muna ng OTP.' });
    }
    if (Date.now() > pending.expiresAt) {
        pendingOtps.delete(key);
        return res.status(400).json({ success: false, message: 'Expired na ang OTP code. Humingi ng bago.' });
    }
    if (String(otp).trim() !== pending.code) {
        return res.status(400).json({ success: false, message: 'Maling OTP code.' });
    }

    const now = Date.now();
    const tokens = {};
    for (const featureId of featureIds) {
        const payload = { installationId, featureId, issuedAt: now };
        const payloadString = JSON.stringify(payload);
        const signature = crypto.sign(null, Buffer.from(payloadString), privateKey).toString('base64');
        tokens[featureId] = { payload, signature };
    }

    pendingOtps.delete(key);

    res.json({ success: true, message: `Na-unlock ang ${featureIds.length} feature(s)!`, tokens });
});

app.get('/relay/health', (req, res) => res.json({ success: true, status: 'ok' }));

// ====================================================================
// "WATCH MY SCREEN" — live support screen-share (view-only, consent
// gated sa panig ng POS terminal, hindi awtomatiko).
//
// MAHALAGA sa disenyo: HINDI dumadaan dito ang aktwal na video. Ang
// RELAY na ito ay isang SIGNALING channel lang — tinutulungan lang
// nitong "magkakilala" ang dalawang panig (POS terminal = "host", dev
// viewer = "viewer") sa pamamagitan ng palitan ng maliliit na WebRTC
// control messages (offer/answer/ICE candidates). Kapag nagkonekt na
// sila, DIREKTANG dumadaan (peer-to-peer, naka-encrypt na sa DTLS-SRTP,
// built-in sa WebRTC) ang video — hindi na ito bumabalik dito sa relay.
//
// Ang "host" lang (ang POS terminal, matapos ang consent prompt) ang
// nagsisimula ng session — ang relay mismo ay hindi kailanman
// nagsisimula o humihiling ng screen share sa sinuman.
// ====================================================================

// key: sessionCode -> { hostToken, hostWs, viewerWs, createdAt, meta }
const screenShareSessions = new Map();

// Kinakalimutan lang ang mga session na hindi pa nagkaka-WS-connect ang
// host sa loob ng ilang minuto (hal. tinatawag ang /create pero
// hinahard-refresh o naisara ang tab bago pa makonekta).
const SCREEN_SHARE_PENDING_TTL_MS = 5 * 60 * 1000;
setInterval(() => {
    const now = Date.now();
    for (const [code, session] of screenShareSessions.entries()) {
        if (!session.hostWs && now - session.createdAt > SCREEN_SHARE_PENDING_TTL_MS) {
            screenShareSessions.delete(code);
        }
    }
}, 60 * 1000);

function generateSessionCode() {
    let code;
    do {
        code = String(Math.floor(100000 + Math.random() * 900000));
    } while (screenShareSessions.has(code));
    return code;
}

// --------------------------------------------------------------
// POST /relay/screen-share/create
// Tinatawag ito ng CLIENT SERVER (hindi diretso ng browser) — ibig
// sabihin, ang aktwal na "Simulan ang Watch My Screen" click sa POS ay
// dumaan MUNA sa sariling backend ng kliyente (na may hawak ng
// RELAY_API_KEY), bago mabuksan ang WebSocket diretso sa browser.
// Ito ang naglilihim ng RELAY_API_KEY mula sa browser habang pinapayagan
// pa ring diretso (peer-to-peer signaling, walang extra hop) ang WS.
// --------------------------------------------------------------
app.post('/relay/screen-share/create', requireApiKey, rateLimit('screen-share-create', 10, 10 * 60 * 1000), (req, res) => {
    const { installationId, storeName, username } = req.body;

    const sessionCode = generateSessionCode();
    const hostToken = crypto.randomBytes(24).toString('hex');

    screenShareSessions.set(sessionCode, {
        hostToken,
        hostWs: null,
        viewerWs: null,
        createdAt: Date.now(),
        meta: {
            installationId: installationId || null,
            storeName: storeName || null,
            username: username || 'Unknown'
        }
    });

    res.json({ success: true, sessionCode, hostToken });
});

// --------------------------------------------------------------
// WEBSOCKET SIGNALING — /relay/screen-share/ws
// Dalawang uri ng koneksyon ang pumapasok dito:
//   - HOST  (POS terminal browser): { type:'host-join', sessionCode, hostToken }
//   - VIEWER (dev viewer page):     { type:'viewer-join', sessionCode, devKey }
// Pagkatapos maging "paired" ang host+viewer sa isang sessionCode,
// basta ipinapasa lang ({type:'signal', payload}) papunta sa KABILANG
// panig ang lahat ng iba pang mensahe — hindi binabasa o binabago ang
// laman ng payload (opaque na WebRTC offer/answer/ICE data lang ito).
// --------------------------------------------------------------
const httpServer = http.createServer(app);
const wss = new WebSocketServer({ server: httpServer, path: '/relay/screen-share/ws' });

function safeSend(ws, data) {
    if (ws && ws.readyState === ws.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

function endScreenShareSession(sessionCode, reason) {
    const session = screenShareSessions.get(sessionCode);
    if (!session) return;
    safeSend(session.hostWs, { type: 'session-ended', reason });
    safeSend(session.viewerWs, { type: 'session-ended', reason });
    if (session.hostWs) session.hostWs.close();
    if (session.viewerWs) session.viewerWs.close();
    screenShareSessions.delete(sessionCode);
}

wss.on('connection', (ws) => {
    ws.role = null;
    ws.sessionCode = null;

    ws.on('message', (raw) => {
        let msg;
        try {
            msg = JSON.parse(raw);
        } catch {
            return;
        }

        if (msg.type === 'host-join') {
            const session = screenShareSessions.get(msg.sessionCode);
            if (!session || session.hostToken !== msg.hostToken) {
                return safeSend(ws, { type: 'error', message: 'Invalid session or host token.' });
            }
            session.hostWs = ws;
            ws.role = 'host';
            ws.sessionCode = msg.sessionCode;
            safeSend(ws, { type: 'host-ready', sessionCode: msg.sessionCode });
            return;
        }

        if (msg.type === 'viewer-join') {
            if (!DEV_VIEWER_KEY || msg.devKey !== DEV_VIEWER_KEY) {
                return safeSend(ws, { type: 'error', message: 'Invalid developer key.' });
            }
            const session = screenShareSessions.get(msg.sessionCode);
            if (!session) {
                return safeSend(ws, { type: 'error', message: 'No active session with that code.' });
            }
            session.viewerWs = ws;
            ws.role = 'viewer';
            ws.sessionCode = msg.sessionCode;
            safeSend(ws, { type: 'viewer-ready', meta: session.meta });
            // Sabihan ang host na puwede na siyang gumawa ng WebRTC offer.
            safeSend(session.hostWs, { type: 'viewer-connected' });
            return;
        }

        // Palitan lang ng offer/answer/ICE sa pagitan ng dalawang panig —
        // hindi binabasa ang laman, ipinapasa lang papunta sa kabila.
        if (msg.type === 'signal' && ws.sessionCode) {
            const session = screenShareSessions.get(ws.sessionCode);
            if (!session) return;
            const target = ws.role === 'host' ? session.viewerWs : session.hostWs;
            safeSend(target, { type: 'signal', payload: msg.payload });
            return;
        }

        // Ang taong naka-login sa POS ang nag-click ng Stop — tapusin
        // ang session agad para sa dalawang panig.
        if (msg.type === 'end-session' && ws.sessionCode) {
            endScreenShareSession(ws.sessionCode, 'ended_by_host');
            return;
        }
    });

    ws.on('close', () => {
        if (!ws.sessionCode) return;
        const session = screenShareSessions.get(ws.sessionCode);
        if (!session) return;
        if (ws.role === 'host') {
            // Nawala ang host (nasarado ang tab, network drop, atbp.) —
            // tapusin agad ang session, hindi natin gagawin ang
            // pag-viewing kung wala nang kontrol ang taong nasa terminal.
            endScreenShareSession(ws.sessionCode, 'host_disconnected');
        } else if (ws.role === 'viewer') {
            session.viewerWs = null;
            safeSend(session.hostWs, { type: 'viewer-disconnected' });
        }
    });
});

httpServer.listen(PORT, () => {
    console.log(`OmniPOS Unlock Relay running sa port ${PORT}`);
});
