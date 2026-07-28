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

// --------------------------------------------------------------
// SECURITY HEADERS — mabilis na dagdag na proteksyon nang hindi na
// kailangan pang mag-install ng bagong dependency (helmet, atbp.).
// - X-Frame-Options / frame-ancestors: pumipigil sa "clickjacking"
//   (hindi puwedeng i-embed ang admin panel sa loob ng <iframe> ng
//   ibang site para linlangin kang mag-click ng Allow/Revoke).
// - X-Content-Type-Options: pumipigil sa MIME-sniffing.
// - Referrer-Policy: hindi na-leleak ang buong URL (posibleng may key
//   sa query string) papunta sa ibang site sa pamamagitan ng Referer header.
// --------------------------------------------------------------
app.use((req, res, next) => {
    res.setHeader('X-Frame-Options', 'DENY');
    res.setHeader('Content-Security-Policy', "frame-ancestors 'none'");
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Referrer-Policy', 'no-referrer');
    next();
});

// FIX: default na 100kb lang ang limit ng express.json() — madalas sobra dito
// ang mga request na may kasamang requestor photo (base64 JPEG mula sa
// captureQuickPhoto() sa OMNIPOS client), kaya minsan na-REJECT ng RELAY ang
// buong request (413) bago pa man ito maka-abot sa /relay/request-unlock
// route — ibig sabihin, minsan hindi lang yung photo ang nawawala, buong
// unlock/demo/bundle request mismo ang nabibigo dahil dito. 2mb na ngayon,
// katumbas ng limit na ginagamit na rin ng OMNIPOS client server mismo.
app.use(express.json({ limit: '2mb' }));

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

// Opsyonal: default na bilang ng araw bago mag-expire ang isang BAGONG
// paid unlock/license (hindi demo), kung walang tahasang durationDays na
// pinasa ang admin sa Approve/Activate. NULL/wala = permanente (dating
// behavior, walang expiry) — kaya ligtas itong iwanang blangko kung ayaw
// mo pang gawing time-based ang lahat ng lisensya.
const RELAY_DEFAULT_LICENSE_DAYS = process.env.RELAY_DEFAULT_LICENSE_DAYS
    ? Number(process.env.RELAY_DEFAULT_LICENSE_DAYS)
    : null;

// --------------------------------------------------------------
// EXTRA NOTIFICATION CHANNELS (opsyonal) — Slack at/o Telegram, dagdag
// sa email na required pa rin. Kapag naka-set ang alinman dito, ipapadala
// din agad ang parehong mensahe (unlock/demo/bundle request) sa channel
// na iyon — best-effort lang ito, hindi ito dapat makasira ng buong
// request kung ito lang ang bumagsak (email pa rin ang "source of truth"
// kung na-notify ka; tingnan ang notifyUnlockRequest() sa ibaba).
// --------------------------------------------------------------
const SLACK_WEBHOOK_URL = process.env.RELAY_SLACK_WEBHOOK_URL || null;
const TELEGRAM_BOT_TOKEN = process.env.RELAY_TELEGRAM_BOT_TOKEN || null;
const TELEGRAM_CHAT_ID = process.env.RELAY_TELEGRAM_CHAT_ID || null;

// Opsyonal na PAGPAPABILIS: kung naka-set ang RESEND_API_KEY (env var),
// gagamitin ang Resend (https://resend.com) HTTPS API sa halip na Gmail
// SMTP. Bakit mas mabilis/maaasahan ito sa Render: (1) isang simpleng
// HTTPS POST call lang ito (walang TCP/TLS SMTP handshake+greeting na
// paulit-ulit), (2) hindi ito naka-block/na-throttle gaya minsan ng
// outbound SMTP ports sa ilang PaaS/free-tier networks, (3) may sarili
// itong timeout na madaling i-abort. Kung WALA namang RESEND_API_KEY na
// naka-set, awtomatikong babalik ito sa dating Gmail/nodemailer path sa
// ibaba — hindi kailangang palitan agad, opsyonal na upgrade lang ito.
const RESEND_API_KEY = process.env.RESEND_API_KEY || null;
const RESEND_FROM_EMAIL = process.env.RESEND_FROM_EMAIL || 'OmniPOS Unlock Relay <onboarding@resend.dev>';

// ====================================================================
// SHARED, POOLED NODEMAILER TRANSPORTER (fallback path, fix para sa
// dating OTP timeout kapag Gmail SMTP pa rin ang gamit)
// ====================================================================
// Dati, GUMAGAWA ng BAGONG koneksyon sa Gmail (bagong TCP/TLS handshake)
// sa BAWAT OTP request (request-unlock, demo-request, bundle-unlock).
// Sa Render, kapag na-spin-down/natulog ang serbisyo dahil sa free
// tier (walang traffic sa loob ng ~15 min), ang UNANG request pagkatapos
// ay kailangan munang gisingin ang container BAGO pa man ito magsimula
// gumawa ng bagong SMTP handshake papunta sa Gmail — kaya madaling
// lumagpas sa timeout ng platform/client. Dito, IISANG pooled
// transporter na lang ang ginagawa (buhay habang tumatakbo ang
// process), gamit muli sa lahat ng OTP endpoints, at may EXPLICIT na
// connection/greeting/socket timeouts para bumagsak na lang agad
// nang malinaw (at ma-retry) kaysa sa manatiling nakabitin.
const mailTransporter = (MAIL_USER && MAIL_PASS) ? nodemailer.createTransport({
    service: 'gmail',
    pool: true,
    maxConnections: 3,
    auth: { user: MAIL_USER, pass: MAIL_PASS },
    connectionTimeout: 8000,
    greetingTimeout: 8000,
    socketTimeout: 15000
}) : null;

// ====================================================================
// UNIFIED "sendOtpMail" HELPER
// ====================================================================
// Ginagamit ito ng LAHAT ng 3 OTP endpoint (request-unlock, demo-request,
// bundle-unlock) sa halip na direktang tumawag sa nodemailer/Resend.
// Susubukan munang gamitin ang Resend HTTPS API kung naka-configure ito
// (mas mabilis); kung hindi, babalik sa pooled Gmail transporter sa itaas.
async function sendOtpMail({ subject, text }) {
    if (RESEND_API_KEY) {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 8000);
        try {
            const resp = await fetch('https://api.resend.com/emails', {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${RESEND_API_KEY}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    from: RESEND_FROM_EMAIL,
                    to: [RECIPIENT_EMAIL],
                    subject,
                    text
                }),
                signal: controller.signal
            });
            if (!resp.ok) {
                const errBody = await resp.text().catch(() => '');
                throw new Error(`Resend API error (${resp.status}): ${errBody}`);
            }
            return;
        } finally {
            clearTimeout(timeout);
        }
    }

    if (!mailTransporter) {
        throw new Error('Walang RESEND_API_KEY o RELAY_MAIL_USER/RELAY_MAIL_PASS na naka-configure.');
    }
    await mailTransporter.sendMail({
        from: `"OmniPOS Unlock Relay" <${MAIL_USER}>`,
        to: RECIPIENT_EMAIL,
        subject,
        text
    });
}

// ====================================================================
// SLACK / TELEGRAM (opsyonal, best-effort) — hindi nire-required, at
// hindi dapat mag-throw papunta sa caller (sinasalo dito mismo ang
// error, sine-console.error na lang para may bakas sa Render logs).
// ====================================================================
async function sendSlackNotification(text) {
    if (!SLACK_WEBHOOK_URL) return;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);
    try {
        const resp = await fetch(SLACK_WEBHOOK_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ text }),
            signal: controller.signal
        });
        if (!resp.ok) {
            const errBody = await resp.text().catch(() => '');
            throw new Error(`Slack webhook error (${resp.status}): ${errBody}`);
        }
    } finally {
        clearTimeout(timeout);
    }
}

async function sendTelegramNotification(text) {
    if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);
    try {
        const resp = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text }),
            signal: controller.signal
        });
        if (!resp.ok) {
            const errBody = await resp.text().catch(() => '');
            throw new Error(`Telegram API error (${resp.status}): ${errBody}`);
        }
    } finally {
        clearTimeout(timeout);
    }
}

// --------------------------------------------------------------
// UNIFIED NOTIFICATION HELPER — ginagamit ito ng LAHAT ng OTP request
// endpoints (request-unlock, request-demo, request-unlock-bulk) sa
// halip na direktang tumawag sa sendOtpMail(). Email pa rin ang
// REQUIRED na channel (kapareho ng dati — kung mabigo ito, mabibigo pa
// rin ang buong request, kasi doon pa rin dinideliver ang OTP code).
// Ang Slack/Telegram ay dagdag lang na "heads up" (mas mabilis makita
// sa phone kaysa email) — best-effort, hindi ito hahadlang o
// magpapabagsak sa request kahit mabigo.
// --------------------------------------------------------------
async function notifyUnlockRequest({ subject, text }) {
    await sendOtpMail({ subject, text });

    const extraText = `*${subject}*\n${text}`;
    Promise.allSettled([
        sendSlackNotification(extraText),
        sendTelegramNotification(extraText)
    ]).then((results) => {
        results.forEach((r) => {
            if (r.status === 'rejected') {
                console.error('Extra notification channel failed:', r.reason);
            }
        });
    });
}

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

// --------------------------------------------------------------
// DEVICE LABELS — pangalan/tatak na MANU-MANONG inilalagay ng admin
// (hal. "Aling Nena — Sari-sari Store, Cubao") para sa isang
// installationId, HIWALAY sa self-reported storeName/username na galing
// mismo sa device (madaling ma-blangko o hindi kilala kung bagong
// request pa lang). Layunin: mas madaling makilala/matandaan kung SINO
// ang customer na "gumagawa ng request", kahit pa hindi pa ito
// naka-Allow. Naka-imbak sa hiwalay na JSON file (persistent, gaya ng
// allowed-devices.json) — pero tandaan din: mawawala din ito sa
// susunod na REDEPLOY dahil walang persistent disk sa Render free
// tier (parehong paalala gaya ng nasa itaas).
// --------------------------------------------------------------
const DEVICE_LABELS_PATH = path.join(__dirname, 'device-labels.json');

function loadDeviceLabels() {
    try {
        const raw = fs.readFileSync(DEVICE_LABELS_PATH, 'utf8');
        return new Map(Object.entries(JSON.parse(raw)));
    } catch (err) {
        return new Map();
    }
}

function saveDeviceLabels(map) {
    try {
        fs.writeFileSync(DEVICE_LABELS_PATH, JSON.stringify(Object.fromEntries(map), null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang device-labels.json:', err);
    }
}

let deviceLabels = loadDeviceLabels();

// --------------------------------------------------------------
// DEVICE FINGERPRINT BINDING — ito ang PANGUNAHING proteksyon laban sa
// "pag-clone": kada installationId, itinatago dito ang UNANG hardware
// fingerprint na na-verify online para dito. Kung sa susunod na
// verify-login request ay IBA na ang fingerprint na dumating PARA SA
// PAREHONG installationId — malinaw na senyales ito na ang buong data
// folder ay kinopya/inilipat papunta sa ibang pisikal na device — at
// dito ito ma-flag bilang "clone_suspected" hangga't hindi ito
// ni-review/ni-reset ng developer/store owner sa admin panel.
// PAALALA: mawawala din ito sa susunod na redeploy kung walang
// persistent disk (parehong caveat gaya ng allowed-devices.json).
// --------------------------------------------------------------
const DEVICE_FINGERPRINTS_PATH = path.join(__dirname, 'device-fingerprints.json');

function loadDeviceFingerprints() {
    try {
        const raw = fs.readFileSync(DEVICE_FINGERPRINTS_PATH, 'utf8');
        return new Map(Object.entries(JSON.parse(raw)));
    } catch (err) {
        return new Map();
    }
}

function saveDeviceFingerprints(map) {
    try {
        fs.writeFileSync(DEVICE_FINGERPRINTS_PATH, JSON.stringify(Object.fromEntries(map), null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang device-fingerprints.json:', err);
    }
}

let deviceFingerprints = loadDeviceFingerprints(); // installationId -> { fingerprint, firstVerifiedAt, lastVerifiedAt, verifyCount, flagged, flaggedFingerprint, flaggedAt }

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
            // Hiwalay na flag (hindi lang basta message string) para ma-detect
            // ito nang maaasahan ng OMNIPOS server/app — ginagamit ito para
            // ipakita ang isang "naghihintay pa ng authorization" na estado sa
            // requestor sa halip na basta-basta error, dahil normal at
            // inaasahang pangyayari ito sa UNANG request ng isang bagong
            // device (bago pa ito ma-Allow ng admin sa Relay admin panel).
            deviceNotAllowed: true,
            message: 'Hindi pa authorized ang device na ito para gumamit ng relay. Naka-log na ang device — maghintay ng authorization mula sa developer/store owner.'
        });
    }
    next();
}

// --------------------------------------------------------------
// FEATURE CATALOG MIRROR — para lang sa ADMIN PANEL (display + direct
// "Activate" button). Ito ay KOPYA ng FEATURE_CATALOG/UPGRADE_TIERS na
// nasa OMNIPOS/server.js — dapat i-sync manually kapag nagbago ang
// presyo/pangalan doon. Hindi umaasa ang OMNIPOS client dito; ginagamit
// lang ito ng admin panel para malaman kung anong mga package ang
// "locked pa" sa isang device (dahil hindi ito naka-imbak ng RELAY sa
// sarili nito), at para bigyan ng tamang featureName/price ang mga
// direct-activate na token na ginagawa mula sa admin panel.
// --------------------------------------------------------------
const FEATURE_CATALOG = {
    ocean: { name: 'Ocean Pro', price: 149, category: 'theme' },
    emerald: { name: 'Emerald Pro', price: 149, category: 'theme' },
    sunset: { name: 'Sunset Pro', price: 149, category: 'theme' },
    rosegold: { name: 'Rose Gold Pro', price: 149, category: 'theme' },
    cyber: { name: 'Cyber Neon Pro', price: 149, category: 'theme' },
    noir: { name: 'Coffee Noir Pro', price: 149, category: 'theme' },
    mintfrost: { name: 'Mint Frost Pro', price: 149, category: 'theme' },
    purchase_orders: { name: 'Purchase Orders Module', price: 999, category: 'module' },
    customer_crm: { name: 'Customer Profiles & Loyalty', price: 799, category: 'module' },
    promo_codes: { name: 'Promo Codes Module', price: 499, category: 'module' },
    advanced_reports: { name: 'Sales Analytics & Advanced Reports', price: 799, category: 'module' },
    shift_management: { name: 'Multi-Cashier Shift Oversight & Z-Reading Reports', price: 699, category: 'module' },
    rbac_management: { name: 'Roles & Permissions (RBAC) Management', price: 999, category: 'module' }
};

const UPGRADE_TIERS = [
    { id: 'basic', name: 'Basic Upgrade', featureIds: ['advanced_reports', 'promo_codes'], bundlePrice: 999 },
    { id: 'standard', name: 'Standard Upgrade', featureIds: ['advanced_reports', 'promo_codes', 'customer_crm', 'shift_management'], bundlePrice: 1999 },
    { id: 'pro', name: 'Pro Upgrade (Complete)', featureIds: Object.keys(FEATURE_CATALOG), bundlePrice: 4499 }
];

// --------------------------------------------------------------
// ISSUED UNLOCKS — persistent na "memory" ng RELAY kung anong mga
// token na talaga niyang na-isyu na sa bawat installationId. Dati,
// walang ganito — nagagawa lang ang token, ibinibigay sa client, at
// nakakalimutan agad ng RELAY. Kailangan ito para sa: (1) Device Detail
// admin page (makita kung ano na ang naka-unlock/locked pa), at (2) ang
// bagong /relay/restore-tokens endpoint (auto-restore pagkatapos ng
// hard reset sa OMNIPOS client, hindi na kailangang mag-OTP ulit).
//
// Estruktura: { [installationId]: { [featureId]: { featureName, price,
// issuedAt, expiresAt?, payload, signature, source, note? } } }
//
// PAALALA: kapareho ng allowed-devices.json, MAWAWALA ito sa susunod na
// REDEPLOY kung walang persistent disk (Render free tier) — hindi ito
// mawawala sa ordinaryong spin down/up.
// --------------------------------------------------------------
const ISSUED_UNLOCKS_PATH = path.join(__dirname, 'issued-unlocks.json');

function loadIssuedUnlocks() {
    try {
        return JSON.parse(fs.readFileSync(ISSUED_UNLOCKS_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}

function saveIssuedUnlocks(obj) {
    try {
        fs.writeFileSync(ISSUED_UNLOCKS_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang issued-unlocks.json:', err);
    }
}

let issuedUnlocks = loadIssuedUnlocks();

function recordIssuedUnlock(installationId, featureId, token, meta = {}) {
    if (!issuedUnlocks[installationId]) issuedUnlocks[installationId] = {};
    issuedUnlocks[installationId][featureId] = {
        featureName: meta.featureName || (FEATURE_CATALOG[featureId] && FEATURE_CATALOG[featureId].name) || featureId,
        price: typeof meta.price === 'number' ? meta.price : (FEATURE_CATALOG[featureId] && FEATURE_CATALOG[featureId].price) || null,
        issuedAt: token.payload.issuedAt,
        expiresAt: typeof token.payload.expiresAt === 'number' ? token.payload.expiresAt : null,
        payload: token.payload,
        signature: token.signature,
        source: meta.source || 'otp', // 'otp' | 'admin-direct'
        note: meta.note || null
    };
    saveIssuedUnlocks(issuedUnlocks);
}

// --------------------------------------------------------------
// ACTIVITY LOG — simpleng history (huling 500 entries) ng lahat ng
// mahalagang pangyayari kada device: hiningi ng OTP, na-approve,
// na-isyu ang token, in-allow/revoke, at "restore check-in" (ibig
// sabihin, nag-check-in ulit ang isang device na posibleng
// nag-hard-reset). Ipinapakita ito sa History timeline ng Device
// Detail admin page.
// --------------------------------------------------------------
const ACTIVITY_LOG_PATH = path.join(__dirname, 'activity-log.json');
const ACTIVITY_LOG_MAX = 500;

function loadActivityLog() {
    try {
        return JSON.parse(fs.readFileSync(ACTIVITY_LOG_PATH, 'utf8'));
    } catch (err) {
        return [];
    }
}

function saveActivityLog(arr) {
    try {
        fs.writeFileSync(ACTIVITY_LOG_PATH, JSON.stringify(arr, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang activity-log.json:', err);
    }
}

let activityLog = loadActivityLog();

function logActivity(installationId, type, details = {}) {
    activityLog.unshift({
        installationId: installationId || null,
        type, // 'otp_requested' | 'admin_approved' | 'unlock_issued' | 'device_allowed' | 'device_revoked' | 'restore_checkin'
        details,
        at: Date.now()
    });
    if (activityLog.length > ACTIVITY_LOG_MAX) activityLog.length = ACTIVITY_LOG_MAX;
    saveActivityLog(activityLog);
}

// --------------------------------------------------------------
// BACKUP CHECK-INS — "auto backup" na tinatawag ng OMNIPOS client
// (server.js doon, hindi ang browser) tuwing matagumpay itong
// nag-mirror ng sarili niyang database papunta sa Download/RELAY_BACKUP
// nito (iisang overwritten file, tingnan ang db.js/server.js ng
// OMNIPOS). Ginagamit ito para sa DALAWANG bagay:
//   1. Pinapakita sa admin panel kung KAILAN huling successful na-sync
//      ang bawat device (at ang pinaka-huli sa LAHAT, para sa
//      notification/dot sa itaas ng "Allowed devices").
//   2. (Opsyonal, naka-toggle) AWTOMATIKONG idinaragdag ang device sa
//      allowlist sa tuwing may matagumpay na check-in — "trust on
//      first successful backup" na modelo, para hindi na kailangang
//      i-Allow nang manual ang bawat bagong verified na device.
//      I-set ang RELAY_AUTOALLOW_ON_BACKUP=false sa .env kung ayaw mo
//      nito (manual Allow pa rin sa admin panel ang gagamitin).
//
// PAALALA: kapareho ng iba pang JSON stores dito, MAWAWALA ito sa
// susunod na REDEPLOY kung walang persistent disk (Render free tier).
// --------------------------------------------------------------
const BACKUP_CHECKINS_PATH = path.join(__dirname, 'backup-checkins.json');

function loadBackupCheckins() {
    try {
        return JSON.parse(fs.readFileSync(BACKUP_CHECKINS_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}

function saveBackupCheckins(obj) {
    try {
        fs.writeFileSync(BACKUP_CHECKINS_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang backup-checkins.json:', err);
    }
}

let backupCheckins = loadBackupCheckins(); // installationId -> { lastBackupAt, storeName, fileSizeBytes, checkinCount }

// Default na NAKA-ON ang auto-allow-on-backup (mas kaunting manual na
// hakbang para sa developer) — i-set ang env var na ito sa 'false' kung
// gusto mo pa ring manual na i-Allow bawat device sa admin panel kahit
// successful na ang backup check-in nito.
const AUTOALLOW_ON_BACKUP = String(process.env.RELAY_AUTOALLOW_ON_BACKUP || 'true').trim().toLowerCase() !== 'false';

function mostRecentBackupCheckinAt() {
    const values = Object.values(backupCheckins).map((c) => c.lastBackupAt).filter(Boolean);
    return values.length ? Math.max(...values) : null;
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

// Timing-safe string comparison — pumipigil sa "timing attack" kung saan
// puwedeng hulaan ng attacker ang key nang paunti-unti (character by
// character) batay sa kung gaano kabilis tumugon ang server sa bawat
// maling guess. Gumagamit ng crypto.timingSafeEqual, pero pareho munang
// pinapantayan ang haba ng dalawang string (kailangan ito ng function na
// iyon) nang hindi nagpapakita kung alin ang mas maikli/mahaba.
function safeCompare(a, b) {
    if (typeof a !== 'string' || typeof b !== 'string') return false;
    const bufA = Buffer.from(a);
    const bufB = Buffer.from(b);
    if (bufA.length !== bufB.length) {
        // Ipadaan pa rin sa timingSafeEqual gamit ang parehong haba (bufA
        // laban sa sarili nito) para hindi bumagsak agad sa maikling-circuit
        // na maaaring gamiting "oracle" ng attacker.
        crypto.timingSafeEqual(bufA, bufA);
        return false;
    }
    return crypto.timingSafeEqual(bufA, bufB);
}

// Bantay laban sa brute-force: limitado ang bilang ng MALING admin-key
// attempts bawat IP bago pansamantalang harangan (kahit tama na ang key
// pagkatapos) — hindi ito nakakaapekto sa normal na 8-second auto-refresh
// ng admin panel dahil MALING attempts lang ang binibilang, hindi lahat
// ng request.
const ADMIN_LOGIN_MAX_FAILURES = 10;
const ADMIN_LOGIN_WINDOW_MS = 10 * 60 * 1000;
const adminLoginFailures = new Map(); // ip -> [timestamps]

function isAdminLoginLocked(ip) {
    const attempts = (adminLoginFailures.get(ip) || []).filter(
        ts => Date.now() - ts < ADMIN_LOGIN_WINDOW_MS
    );
    adminLoginFailures.set(ip, attempts);
    return attempts.length >= ADMIN_LOGIN_MAX_FAILURES;
}

function recordAdminLoginFailure(ip) {
    const attempts = adminLoginFailures.get(ip) || [];
    attempts.push(Date.now());
    adminLoginFailures.set(ip, attempts);
}

function requireAdminKey(req, res, next) {
    if (isAdminLoginLocked(req.ip)) {
        return res.status(429).json({
            success: false,
            message: 'Sobra na sa maling pagtatangka. Subukan ulit mamaya.'
        });
    }
    const provided = req.headers['x-relay-admin-key'] || req.query.key;
    if (!ADMIN_KEY || !safeCompare(String(provided || ''), ADMIN_KEY)) {
        recordAdminLoginFailure(req.ip);
        return res.status(403).json({ success: false, message: 'Invalid o walang admin key.' });
    }
    next();
}

app.use('/relay/admin', express.static(path.join(__dirname, 'public', 'admin')));

// Body: { key, durationDays? }. `durationDays`:
//   - positibong numero -> ganoong dami ng araw bago mag-expire ang
//     lisensyang ito (AUTO-EXPIRING, hindi lang device-based).
//   - 0 (tahasang pinasa) -> PERMANENTE, kahit may RELAY_DEFAULT_LICENSE_DAYS.
//   - wala/undefined -> babalik sa RELAY_DEFAULT_LICENSE_DAYS (kung meron),
//     o permanente kung wala ring env default (backward-compatible).
app.post('/relay/admin/api/pending-otps/approve', requireAdminKey, (req, res) => {
    const { key, durationDays } = req.body;
    const pending = pendingOtps.get(key);
    if (!pending) {
        return res.status(404).json({ success: false, message: "Wala nang aktibong request na iyan (baka na-expire na o na-claim na)." });
    }
    pending.approved = true;
    if (typeof durationDays === 'number' && durationDays > 0) {
        pending.durationDays = durationDays;
    } else if (durationDays === 0) {
        pending.durationDays = null; // tahasang "Permanente" na pinili ng admin
    } else {
        pending.durationDays = RELAY_DEFAULT_LICENSE_DAYS;
    }
    logActivity(pending.installationId, 'admin_approved', {
        featureId: pending.featureId,
        featureName: pending.featureName,
        durationDays: pending.durationDays || null
    });
    res.json({ success: true, message: 'Naaprubahan. Pwede na ulit i-click ng kliyente ang unlock button nila.' });
});

// --------------------------------------------------------------
// AUTO-CLEANUP — kada 30 segundo, tinatanggal ang kahit anong pending
// OTP na LUMAMPAS na sa 10-minutong expiry nito nang hindi na-Allow.
// Ganito nakakamit ang hiling na "kung hindi ko na-Run/Allow bago
// matapos ang oras, mababaliwala at mabubura ang OTP" — hindi na
// kailangang balikan pa ng kliyente/kliyenteng humihiling.
// --------------------------------------------------------------
setInterval(() => {
    const now = Date.now();
    for (const [key, pending] of pendingOtps.entries()) {
        if (now > pending.expiresAt) {
            pendingOtps.delete(key);
        }
    }
}, 30 * 1000);

app.get('/relay/admin/api/pending-otps', requireAdminKey, (req, res) => {
    const now = Date.now();
    const pending = [...pendingOtps.entries()]
        .map(([key, data]) => ({ key, ...data }))
        .filter(entry => entry.expiresAt > now) // huwag ipakita yung na-expire na
        .sort((a, b) => b.expiresAt - a.expiresAt);

    res.json({ success: true, pendingOtps: pending });
});

// Helper: ilista lang ang mga featureId na may VALID (hindi pa expired)
// na naka-record na token para sa isang installationId.
function getActiveUnlockedFeatureIds(installationId) {
    const record = issuedUnlocks[installationId] || {};
    const now = Date.now();
    return Object.keys(record).filter(featureId => {
        const entry = record[featureId];
        return !(typeof entry.expiresAt === 'number' && now > entry.expiresAt);
    });
}

app.get('/relay/admin/api/devices', requireAdminKey, (req, res) => {
    const seen = [...seenDevices.entries()].map(([installationId, meta]) => {
        const unlockedIds = getActiveUnlockedFeatureIds(installationId).filter(id => id !== DEMO_FEATURE_ID);
        const activations = Object.values(issuedUnlocks[installationId] || {});
        const lastActivationAt = activations.length ? Math.max(...activations.map(a => a.issuedAt)) : null;
        const backupCheckin = backupCheckins[installationId] || null;
        return {
            installationId,
            ...meta,
            label: deviceLabels.get(installationId) || null,
            allowed: allowedDevices.has(installationId),
            unlockedCount: unlockedIds.length,
            totalCatalogCount: Object.keys(FEATURE_CATALOG).length,
            demoActive: getActiveUnlockedFeatureIds(installationId).includes(DEMO_FEATURE_ID),
            lastActivationAt,
            lastBackupAt: backupCheckin ? backupCheckin.lastBackupAt : null,
            backupCheckinCount: backupCheckin ? backupCheckin.checkinCount : 0
        };
    }).sort((a, b) => b.lastSeenAt - a.lastSeenAt);

    res.json({
        success: true,
        seenDevices: seen,
        allowedDevices: [...allowedDevices],
        restrictionActive: allowedDevices.size > 0,
        // Para sa notification/dot blinker sa itaas ng "Allowed devices":
        // huling successful backup check-in mula SA KAHIT ANONG device,
        // at kung naka-ON ang auto-allow-on-backup na behavior.
        lastBackupSyncAt: mostRecentBackupCheckinAt(),
        backupAutoAllowEnabled: AUTOALLOW_ON_BACKUP
    });
});

app.post('/relay/admin/api/devices/allow', requireAdminKey, (req, res) => {
    const { installationId } = req.body;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }
    allowedDevices.add(installationId);
    saveAllowedDevices(allowedDevices);
    logActivity(installationId, 'device_allowed', {});
    res.json({ success: true, allowedDevices: [...allowedDevices] });
});

// --------------------------------------------------------------
// POST /relay/admin/api/devices/:installationId/label
// Nagtatakda (o nagbabawas, kung blangko ang label) ng developer-given
// na palayaw para sa isang device — gawa ito PARA MA-GAMIT KAHIT HINDI
// PA NA-ALLOW ang device (i.e. sa "Recently Seen" list pa lang), para
// madaling makilala/matandaan kung sinong customer ito bago mo pa
// pindutin ang "Allow". Body: { label }.
// --------------------------------------------------------------
app.post('/relay/admin/api/devices/:installationId/label', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    const label = typeof req.body.label === 'string' ? req.body.label.trim().slice(0, 120) : '';

    if (label) {
        deviceLabels.set(installationId, label);
    } else {
        deviceLabels.delete(installationId);
    }
    saveDeviceLabels(deviceLabels);
    logActivity(installationId, 'device_labeled', { label: label || null });
    res.json({ success: true, label: label || null });
});

app.post('/relay/admin/api/devices/revoke', requireAdminKey, (req, res) => {
    const { installationId } = req.body;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }
    allowedDevices.delete(installationId);
    saveAllowedDevices(allowedDevices);
    logActivity(installationId, 'device_revoked', {});
    res.json({ success: true, allowedDevices: [...allowedDevices] });
});

// --------------------------------------------------------------
// GET /relay/admin/api/catalog
// Ibinabalik ang FEATURE_CATALOG + UPGRADE_TIERS mirror — ginagamit ng
// admin panel JS para malaman ang lahat ng posibleng package/presyo
// (kasama ang mga hindi pa na-request kailanman ng device), para sa
// "Locked pa" list at sa mga tier bulk-activate button.
// --------------------------------------------------------------
app.get('/relay/admin/api/catalog', requireAdminKey, (req, res) => {
    res.json({ success: true, catalog: FEATURE_CATALOG, tiers: UPGRADE_TIERS });
});

// --------------------------------------------------------------
// GET /relay/admin/api/analytics
// USAGE ANALYTICS DASHBOARD — buod ng "kalusugan" ng buong relay: ilang
// device ang aktibo/naka-allow, ilang pending approval, ilang lisensyang
// naka-issue (active vs all-time), tinatayang kita, pinaka-paborito na
// features, at kung ANO-ANONG lisensya ang MALAPIT NG MAG-EXPIRE (para
// hindi ka mahuhuli sa pag-follow-up sa customer bago pa mag-expire).
// Kinukuwenta lahat mula sa data na NASA MEMORY/DISK NA (walang bagong
// storage na kailangan) — mabilis, walang bagong dependency.
// --------------------------------------------------------------
app.get('/relay/admin/api/analytics', requireAdminKey, (req, res) => {
    const now = Date.now();
    const SOON_MS = 7 * 24 * 60 * 60 * 1000; // "malapit ng mag-expire" = sa loob ng 7 araw

    let activeUnlocksCount = 0;
    let allTimeUnlocksCount = 0;
    let activeRevenue = 0;
    let allTimeRevenue = 0;
    let demoActiveCount = 0;
    const featureCounts = {};
    const expiringSoon = [];

    for (const [installationId, record] of Object.entries(issuedUnlocks)) {
        for (const [featureId, entry] of Object.entries(record)) {
            const isExpired = typeof entry.expiresAt === 'number' && now > entry.expiresAt;

            if (featureId === DEMO_FEATURE_ID) {
                if (!isExpired) demoActiveCount++;
                continue;
            }

            allTimeUnlocksCount++;
            allTimeRevenue += entry.price || 0;

            if (!isExpired) {
                activeUnlocksCount++;
                activeRevenue += entry.price || 0;
                featureCounts[featureId] = (featureCounts[featureId] || 0) + 1;

                if (typeof entry.expiresAt === 'number' && entry.expiresAt - now <= SOON_MS) {
                    const meta = seenDevices.get(installationId);
                    expiringSoon.push({
                        installationId,
                        label: deviceLabels.get(installationId) || (meta && meta.storeName) || null,
                        featureId,
                        featureName: entry.featureName || featureId,
                        expiresAt: entry.expiresAt
                    });
                }
            }
        }
    }
    expiringSoon.sort((a, b) => a.expiresAt - b.expiresAt);

    const topFeatures = Object.entries(featureCounts)
        .map(([featureId, count]) => ({
            featureId,
            featureName: (FEATURE_CATALOG[featureId] && FEATURE_CATALOG[featureId].name) || featureId,
            count
        }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 8);

    const sevenDaysAgo = now - 7 * 24 * 60 * 60 * 1000;
    const thirtyDaysAgo = now - 30 * 24 * 60 * 60 * 1000;
    const unlocksLast7d = activityLog.filter(e => e.type === 'unlock_issued' && e.at >= sevenDaysAgo).length;
    const unlocksLast30d = activityLog.filter(e => e.type === 'unlock_issued' && e.at >= thirtyDaysAgo).length;

    const pendingCount = [...pendingOtps.values()].filter(p => p.expiresAt > now).length;

    res.json({
        success: true,
        analytics: {
            allowedCount: allowedDevices.size,
            seenCount: seenDevices.size,
            pendingCount,
            activeUnlocksCount,
            allTimeUnlocksCount,
            activeRevenue,
            allTimeRevenue,
            demoActiveCount,
            unlocksLast7d,
            unlocksLast30d,
            topFeatures,
            expiringSoon: expiringSoon.slice(0, 20)
        }
    });
});

// --------------------------------------------------------------
// GET /relay/admin/api/devices/:installationId/detail
// Ang buong detalye ng isang device: naka-unlock na, locked pa, demo
// status, at history ng lahat ng pangyayari — ito ang pina-pakita sa
// bagong "Device Detail" page.
// --------------------------------------------------------------
app.get('/relay/admin/api/devices/:installationId/detail', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    const meta = seenDevices.get(installationId) || null;
    const record = issuedUnlocks[installationId] || {};
    const now = Date.now();

    const unlocked = Object.entries(record)
        .filter(([featureId]) => featureId !== DEMO_FEATURE_ID)
        .filter(([, entry]) => !(typeof entry.expiresAt === 'number' && now > entry.expiresAt))
        .map(([featureId, entry]) => ({ featureId, ...entry }))
        .sort((a, b) => b.issuedAt - a.issuedAt);

    const unlockedIds = new Set(unlocked.map(u => u.featureId));
    const locked = Object.entries(FEATURE_CATALOG)
        .filter(([featureId]) => !unlockedIds.has(featureId))
        .map(([featureId, info]) => ({ featureId, ...info }));

    const demoEntry = record[DEMO_FEATURE_ID];
    const demoActive = !!demoEntry && !(typeof demoEntry.expiresAt === 'number' && now > demoEntry.expiresAt);

    const history = activityLog
        .filter(entry => entry.installationId === installationId)
        .slice(0, 100);

    res.json({
        success: true,
        installationId,
        meta,
        label: deviceLabels.get(installationId) || null,
        allowed: allowedDevices.has(installationId),
        unlocked,
        locked,
        demo: { active: demoActive, expiresAt: demoEntry ? demoEntry.expiresAt : null },
        tiers: UPGRADE_TIERS,
        history
    });
});

// --------------------------------------------------------------
// POST /relay/admin/api/devices/:installationId/activate
// Direktang gumagawa ng signed token(s) PARA SA installationId na ito
// — WALANG OTP kailangan. Gamit ito kung MAY REFERENCE KA NA (dati nang
// nabayaran/na-unlock na ang package na ito, gaya ng pagkatapos ng
// emergency hard reset ng customer) at gusto mo lang i-restore/i-issue
// ulit agad. Body: { featureId } o { featureIds: [...] } o { tierId }.
// Opsyonal na `note` para sa audit trail (hal. "Restore matapos ang
// hard reset, ref: <invoice #>"). Opsyonal ding `durationDays` — kung
// pinasa (positibong numero), MAY EXPIRY ang mga token na ito (parehong
// petsa ng expiry para sa lahat ng na-activate dito); kung wala, gagamit
// ito ng RELAY_DEFAULT_LICENSE_DAYS (kung meron), o permanente kung wala
// ring env default. Ginagamit din ito para sa "Renew/Extend" ng isang
// device — i-activate lang ulit ang parehong featureId na may bagong
// durationDays, ma-o-overwrite nito ang dating entry (bagong expiresAt).
// --------------------------------------------------------------
app.post('/relay/admin/api/devices/:installationId/activate', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    const { featureId, featureIds, tierId, note, durationDays } = req.body;

    let idsToActivate = [];
    if (tierId) {
        const tier = UPGRADE_TIERS.find(t => t.id === tierId);
        if (!tier) return res.status(400).json({ success: false, message: 'Hindi kilalang tierId.' });
        idsToActivate = tier.featureIds;
    } else if (Array.isArray(featureIds) && featureIds.length) {
        idsToActivate = featureIds;
    } else if (featureId) {
        idsToActivate = [featureId];
    } else {
        return res.status(400).json({ success: false, message: 'Kulang ang featureId, featureIds, o tierId.' });
    }

    const unknown = idsToActivate.filter(id => !FEATURE_CATALOG[id]);
    if (unknown.length) {
        return res.status(400).json({ success: false, message: `Hindi kilalang feature(s): ${unknown.join(', ')}` });
    }

    const resolvedDurationDays = (typeof durationDays === 'number' && durationDays > 0)
        ? durationDays
        : (durationDays === 0 ? null : RELAY_DEFAULT_LICENSE_DAYS);
    const durationMs = (typeof resolvedDurationDays === 'number' && resolvedDurationDays > 0)
        ? resolvedDurationDays * 24 * 60 * 60 * 1000
        : null;

    const tokens = {};
    for (const id of idsToActivate) {
        const token = issueSignedToken(installationId, id, durationMs);
        tokens[id] = token;
        recordIssuedUnlock(installationId, id, token, {
            featureName: FEATURE_CATALOG[id].name,
            price: FEATURE_CATALOG[id].price,
            source: 'admin-direct',
            note: note || null
        });
        logActivity(installationId, 'unlock_issued', { featureId: id, featureName: FEATURE_CATALOG[id].name, source: 'admin-direct', note: note || null, durationDays: resolvedDurationDays || null });
    }

    res.json({ success: true, message: `Na-activate ang ${idsToActivate.length} feature(s).`, tokens });
});

// --------------------------------------------------------------
// POST /relay/admin/api/devices/:installationId/deactivate
// Tinatanggal ang IISANG naka-unlock na feature sa installationId na
// ito — hindi ito nag-i-issue ng bagong invalidation token, "lokal" lang
// itong pag-alis sa panig ng RELAY: sa susunod na mag-check-in/mag-sync
// ang OMNIPOS client, wala na itong makikitang unlock record para sa
// feature na ito kaya babalik itong naka-lock. Hindi ito nire-refund at
// hindi rin binabago ang billing — audit trail lang ang ginagawa nito
// dito, ang aktwal na bayad ay hiwalay na usapin. Body: { featureId }.
// --------------------------------------------------------------
app.post('/relay/admin/api/devices/:installationId/deactivate', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    const { featureId } = req.body;

    if (!featureId) {
        return res.status(400).json({ success: false, message: 'Kulang ang featureId.' });
    }

    const record = issuedUnlocks[installationId];
    if (!record || !record[featureId]) {
        return res.status(404).json({ success: false, message: 'Walang ganitong naka-unlock na feature para sa device na ito.' });
    }

    const featureName = record[featureId].featureName || (FEATURE_CATALOG[featureId] && FEATURE_CATALOG[featureId].name) || featureId;
    delete record[featureId];
    saveIssuedUnlocks(issuedUnlocks);
    logActivity(installationId, 'feature_deactivated', { featureId, featureName });

    res.json({ success: true, message: `Na-deactivate ang ${featureName}.` });
});

// --------------------------------------------------------------
// POST /relay/admin/api/devices/:installationId/deactivate-all
// "Factory reset" ng feature unlocks ng device na ito — tinatanggal
// LAHAT (pati ang demo entry, kung meron) para bumalik ito sa default
// state na walang naka-unlock. Gamitin ito bago ibenta o ilipat ang
// physical na unit/device sa ibang customer, para hindi ma-carry-over
// ang mga dating binayarang feature ng dating may-ari.
// --------------------------------------------------------------
app.post('/relay/admin/api/devices/:installationId/deactivate-all', requireAdminKey, (req, res) => {
    const { installationId } = req.params;

    const record = issuedUnlocks[installationId];
    const deactivatedCount = record ? Object.keys(record).length : 0;
    if (deactivatedCount === 0) {
        return res.json({ success: true, message: 'Wala nang naka-unlock na feature dito.' });
    }

    delete issuedUnlocks[installationId];
    saveIssuedUnlocks(issuedUnlocks);
    logActivity(installationId, 'device_reset', { deactivatedCount });

    res.json({ success: true, message: `Na-reset ang device — ${deactivatedCount} feature(s) na tinanggal.` });
});

// --------------------------------------------------------------
// GET /relay/admin/api/backup
// Buong "export" ng lahat ng data na naka-store lang sa disk ng
// container na ito (allowed devices, issued unlocks, activity log) —
// WALANG persistent disk ang Render free tier kaya ito ang tanging
// paraan para hindi mawala ang lahat kapag na-redeploy (git push) o
// na-delete ang service/domain. I-download ito paminsan-minsan (lalo
// na pagkatapos ng bagong unlock/customer) at itago sa ligtas na lugar
// (Google Drive, laptop, atbp.) — gamitin ang /restore para ibalik.
// --------------------------------------------------------------
app.get('/relay/admin/api/backup', requireAdminKey, (req, res) => {
    res.json({
        success: true,
        backupVersion: 2, // v2: dinagdagan ng deviceLabels (dati v1, walang labels)
        exportedAt: Date.now(),
        allowedDevices: [...allowedDevices],
        deviceLabels: Object.fromEntries(deviceLabels),
        issuedUnlocks,
        activityLog
    });
});

// --------------------------------------------------------------
// POST /relay/admin/api/restore
// Ibinabalik ang datos mula sa isang backup file na ginawa ng
// /relay/admin/api/backup sa itaas. PINAPALITAN (hindi dinadagdag/
// merge) ang kasalukuyang allowed devices, issued unlocks, at activity
// log ng laman ng backup — sinusulat din agad sa disk (JSON files) at
// sa in-memory state, para agad itong lumabas sa admin panel. Body:
// yung buong JSON object na nakuha mula sa /backup (o mula sa
// na-download na backup file).
// --------------------------------------------------------------
app.post('/relay/admin/api/restore', requireAdminKey, (req, res) => {
    const { allowedDevices: backupAllowed, deviceLabels: backupLabels, issuedUnlocks: backupUnlocks, activityLog: backupLog } = req.body;

    if (!Array.isArray(backupAllowed) || typeof backupUnlocks !== 'object' || backupUnlocks === null || !Array.isArray(backupLog)) {
        return res.status(400).json({ success: false, message: 'Hindi kilalang format ng backup file — siguraduhing yung na-download galing sa /backup ang ini-restore.' });
    }
    // `deviceLabels` ay OPTIONAL — mga LUMANG (v1) backup na ginawa bago
    // idinagdag ang label feature ay walang field na ito. Sa ganung
    // kaso, iiwan na lang natin ang mga kasalukuyang label (huwag
    // burahin), sa halip na basta i-treat bilang "walang labels".
    const hasLabels = backupLabels && typeof backupLabels === 'object' && !Array.isArray(backupLabels);

    allowedDevices = new Set(backupAllowed);
    saveAllowedDevices(allowedDevices);

    if (hasLabels) {
        deviceLabels = new Map(Object.entries(backupLabels));
        saveDeviceLabels(deviceLabels);
    }

    issuedUnlocks = backupUnlocks;
    saveIssuedUnlocks(issuedUnlocks);

    activityLog = backupLog;
    saveActivityLog(activityLog);

    res.json({
        success: true,
        message: `Na-restore: ${allowedDevices.size} allowed device(s), ${deviceLabels.size} label(s)${hasLabels ? '' : ' (hindi binago — lumang backup na walang labels)'}, ${Object.keys(issuedUnlocks).length} device(s) may naka-unlock, ${activityLog.length} history entry(ies).`
    });
});

// --------------------------------------------------------------
// POST /relay/restore-tokens
// Tinatawag ito ng OMNIPOS CLIENT (hindi ng browser diretso) kapag
// nag-check-in ulit ang isang installationId na wala/kulang ang
// featureUnlocks nito sa panig ng client (hal. matapos ang emergency
// hard reset). Ibinabalik ang LAHAT ng dating na-isyu na (VALID pa
// rin, hindi pa expired) na tokens para sa installationId na ito —
// walang bagong OTP/bayad kailangan, dahil dati na itong nabayaran.
// --------------------------------------------------------------
app.post('/relay/restore-tokens', requireApiKey, requireAllowedDevice, rateLimit('restore-tokens', 30, 10 * 60 * 1000), (req, res) => {
    const { installationId } = req.body;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }

    const record = issuedUnlocks[installationId] || {};
    const now = Date.now();
    const tokens = {};
    for (const [featureId, entry] of Object.entries(record)) {
        if (typeof entry.expiresAt === 'number' && now > entry.expiresAt) continue; // expired na demo, huwag ibalik
        tokens[featureId] = { payload: entry.payload, signature: entry.signature };
    }

    logActivity(installationId, 'restore_checkin', { restoredCount: Object.keys(tokens).length });

    res.json({ success: true, tokens });
});

// --------------------------------------------------------------
// POST /relay/check-feature-status
// Tinatawag ito ng OMNIPOS CLIENT tuwing pinipindot ng user ang manual
// na "Sync sa Relay Ngayon" (Settings) — layunin: TUKUYIN kung alin sa
// mga feature/theme na NASA LOCAL na ng client (may token na siya doon,
// ibig sabihin dati itong na-unlock) ang HINDI NA kinikilala ng RELAY
// ngayon, dahil:
//   (a) na-deactivate mismo ng developer/store owner sa admin panel
//       (tinanggal ang record sa issuedUnlocks — tingnan ang
//       /relay/admin/api/devices/:installationId/deactivate sa itaas), o
//   (b) nag-expire na ang time-based na lisensya nito.
// Hindi ito nagbabalik ng bagong VALID token (/relay/restore-tokens ang
// gagawa niyan) — ito lang ang sagot sa tanong na "totoo pa ba ito?"
// bawat featureId na ipinasa, para agad ma-lock ng client ang mga ito
// nang hindi na kailangang maghintay ng susunod na server restart.
// Body: { installationId, featureIds: string[] }
// --------------------------------------------------------------
app.post('/relay/check-feature-status', requireApiKey, requireAllowedDevice, rateLimit('check-feature-status', 30, 10 * 60 * 1000), (req, res) => {
    const { installationId, featureIds } = req.body;
    if (!installationId || !Array.isArray(featureIds)) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId o featureIds.' });
    }

    const record = issuedUnlocks[installationId] || {};
    const now = Date.now();
    const statuses = {};

    for (const featureId of featureIds) {
        const entry = record[featureId];
        const catalogEntry = FEATURE_CATALOG[featureId];
        const featureName = (entry && entry.featureName) || (catalogEntry && catalogEntry.name) || featureId;
        const category = (catalogEntry && catalogEntry.category) || 'module';

        if (!entry) {
            // Walang record dito para dito — dahil ang client lang ang
            // tumatawag para sa mga featureId na MAY LOCAL TOKEN na ito
            // (dati na itong na-unlock), ang kawalang ito ay nangangahulugan
            // na na-deactivate ito mismo ng developer/store owner.
            statuses[featureId] = { status: 'deactivated', reason: 'deactivated', featureName, category };
            continue;
        }
        if (typeof entry.expiresAt === 'number' && now > entry.expiresAt) {
            statuses[featureId] = { status: 'expired', reason: 'expired', featureName, category, expiresAt: entry.expiresAt };
            continue;
        }
        statuses[featureId] = {
            status: 'active',
            reason: 'active',
            featureName,
            category,
            expiresAt: typeof entry.expiresAt === 'number' ? entry.expiresAt : null
        };
    }

    logActivity(installationId, 'feature_status_checked', { featureIds });

    res.json({ success: true, statuses });
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
// issueSignedToken — IISANG lugar na lang para gumawa ng naka-sign na
// unlock token, ginagamit ng LAHAT ng token-issuing routes (admin
// activate, confirm-unlock, confirm-demo, confirm-unlock-bulk).
//
// Kung binigyan ng `durationMs` (positibong numero), MAY EXPIRY ang
// token — kasama ang `expiresAt` sa payload (AUTO-EXPIRING LICENSE,
// hindi lang basta naka-tali sa device). Kung wala/null, permanente
// ang token (dating behavior — walang binabagong wire format).
//
// MAHALAGA: ang eksaktong key order ng payload object ({ installationId,
// featureId, issuedAt[, expiresAt] }) ay dapat ITUGMA nang eksakto sa
// verifyUnlockToken() sa panig ng OMNIPOS client server, dahil
// JSON.stringify() mismo (hindi ang parsed na object) ang sini-sign at
// ve-verify.
// --------------------------------------------------------------
function issueSignedToken(installationId, featureId, durationMs) {
    const now = Date.now();
    const payload = (typeof durationMs === 'number' && durationMs > 0)
        ? { installationId, featureId, issuedAt: now, expiresAt: now + durationMs }
        : { installationId, featureId, issuedAt: now };
    const payloadString = JSON.stringify(payload);
    const signature = crypto.sign(null, Buffer.from(payloadString), privateKey).toString('base64');
    return { payload, signature };
}

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
    if (!safeCompare(String(provided || ''), RELAY_API_KEY)) {
        return res.status(403).json({ success: false, message: 'Invalid o walang API key.' });
    }
    next();
}

// Napaka-simpleng in-memory rate limiter (per key sa Map, hindi kailangan
// ng Redis o external store dahil isang maliit na relay lang ito).
// `keyFn` (optional): function(req) -> extra string na idadagdag sa IP
// para bumuo ng mas specific na bucket. Ginagamit ito sa mga OTP
// endpoints (installationId bilang extra key) — kung hindi, DALAWANG
// magkaibang store/device na nagkataong parehong public IP (hal.
// parehong ISP/NAT o corporate network) ay COLLECTIVELY na-rate-limit
// sa isa't isa, kahit magkaibang installationId sila.
const rateBuckets = new Map();
function rateLimit(bucketName, max, windowMs, keyFn) {
    return (req, res, next) => {
        let key = `${bucketName}:${req.ip}`;
        if (typeof keyFn === 'function') {
            try {
                const extra = (keyFn(req) || '').toString().trim();
                if (extra) key += `:${extra}`;
            } catch (err) {
                // Kung mabigo ang keyFn, bumalik na lang sa dating IP-only key.
            }
        }
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
// POST /relay/backup-checkin
// Tinatawag ito ng OMNIPOS CLIENT SERVER (hindi ng browser) sa TUWING
// matagumpay itong nakapag-mirror ng sarili niyang database papunta sa
// Download/RELAY_BACKUP nito (iisang overwritten file). Layunin:
//
//   1. I-record ang "huling successful backup" ng device na ito, para
//      makita sa admin panel (Device Detail, at ang notification/dot
//      sa itaas ng "Allowed devices").
//   2. Kung naka-ON ang AUTOALLOW_ON_BACKUP (default), AWTOMATIKONG
//      idadagdag ang device na ito sa allowlist — hindi na kailangang
//      balikan pa ang admin panel para mag-Allow nang manual sa bawat
//      bagong device na regular nang gumagawa ng backup.
//
// SADYANG WALANG requireAllowedDevice dito (hindi tulad ng ibang
// /relay/* endpoints) — kailangan itong tawagin KAHIT HINDI PA
// naka-Allow ang device, dahil ito mismo ang paraan para maging
// naka-Allow ito. Nananatili pa ring protektado ito ng requireApiKey
// (shared secret) at rate limit, kaya hindi basta kahit sinong random
// tao (na walang API key) ang makaka-trigger nito.
// --------------------------------------------------------------
app.post('/relay/backup-checkin', requireApiKey, rateLimit('backup-checkin', 20, 60 * 60 * 1000, (req) => req.body?.installationId), (req, res) => {
    const { installationId, storeName, username, fileSizeBytes, backupAt } = req.body;

    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }

    recordDeviceSeen(installationId, { storeName, username });

    const existing = backupCheckins[installationId] || { checkinCount: 0 };
    backupCheckins[installationId] = {
        lastBackupAt: typeof backupAt === 'number' ? backupAt : Date.now(),
        storeName: storeName || existing.storeName || null,
        fileSizeBytes: typeof fileSizeBytes === 'number' ? fileSizeBytes : (existing.fileSizeBytes || null),
        checkinCount: existing.checkinCount + 1
    };
    saveBackupCheckins(backupCheckins);

    let newlyAllowed = false;
    if (AUTOALLOW_ON_BACKUP && !allowedDevices.has(installationId)) {
        allowedDevices.add(installationId);
        saveAllowedDevices(allowedDevices);
        newlyAllowed = true;
        logActivity(installationId, 'device_allowed', { source: 'auto_backup_checkin' });
    }

    logActivity(installationId, 'backup_checkin', {
        fileSizeBytes: backupCheckins[installationId].fileSizeBytes,
        newlyAllowed
    });

    res.json({
        success: true,
        allowed: allowedDevices.has(installationId),
        newlyAllowed,
        message: newlyAllowed
            ? 'Successful ang backup check-in — awtomatikong na-allow ang device na ito.'
            : 'Successful ang backup check-in.'
    });
});

// --------------------------------------------------------------
// POST /relay/verify-login  (ANTI-CLONE)
// Tinatawag ito ng OMNIPOS CLIENT SERVER bago pumayag ng login sa
// isang cashier/admin — (a) sa UNANG beses na kailanman gagawin ito
// sa isang installationId, o (b) sa tuwing nakita ng OMNIPOS client na
// nagbago na ang live hardware fingerprint nito kumpara sa huling
// naka-imbak na "verified" fingerprint (senyales ng pag-clone/paglipat
// sa ibang pisikal na device).
//
// Lohika:
//   - Kung bagong installationId (hindi pa dating naka-verify) —
//     itinatago ang fingerprint na ito bilang "binding" nito, at
//     pinapayagan.
//   - Kung dating naka-verify na PAREHONG fingerprint — pinapayagan.
//   - Kung dating naka-verify na sa IBANG fingerprint — ma-flag bilang
//     clone_suspected, TATANGGIHAN, hangga't hindi ito ni-reset ng
//     developer/admin sa admin panel (clear ang binding, tapos
//     papayagan uli sa susunod na verify).
// SADYANG WALANG requireAllowedDevice dito (tulad ng backup-checkin) —
// kailangan itong tumakbo KAHIT HINDI PA naka-Allow ang device, dahil
// paraan din ito para makapasok sa unahan bago pa man mag-request ng
// unlock. Protektado pa rin ito ng requireApiKey at rate limit.
// --------------------------------------------------------------
app.post('/relay/verify-login', requireApiKey, rateLimit('verify-login', 30, 10 * 60 * 1000, (req) => req.body?.installationId), (req, res) => {
    const { installationId, hardwareFingerprint, storeName, username } = req.body;

    if (!installationId || !hardwareFingerprint) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId o hardwareFingerprint.' });
    }

    recordDeviceSeen(installationId, { storeName, username });

    const existing = deviceFingerprints.get(installationId);

    if (!existing) {
        // Unang beses — itinatali ang fingerprint na ito sa installationId.
        deviceFingerprints.set(installationId, {
            fingerprint: hardwareFingerprint,
            firstVerifiedAt: Date.now(),
            lastVerifiedAt: Date.now(),
            verifyCount: 1,
            flagged: false
        });
        saveDeviceFingerprints(deviceFingerprints);
        logActivity(installationId, 'device_first_verified', { storeName: storeName || null, username: username || null });
        return res.json({
            success: true,
            allowed: allowedDevices.has(installationId),
            firstTime: true,
            message: 'Unang beses na na-verify online ang device na ito.'
        });
    }

    if (existing.flagged) {
        // Naka-flag na dati — kailangan munang i-clear ng admin sa panel
        // bago ito payagan ulit, kahit pareho na ulit ang fingerprint.
        return res.status(403).json({
            success: false,
            cloneSuspected: true,
            message: 'Naka-flag ang device na ito bilang posibleng clone/duplicate. Kontakin ang developer/store owner para i-review at i-reset.'
        });
    }

    if (existing.fingerprint !== hardwareFingerprint) {
        // Parehong installationId pero IBANG fingerprint — malamang
        // kinopya/inilipat ang buong data folder papunta sa ibang device.
        existing.flagged = true;
        existing.flaggedFingerprint = hardwareFingerprint;
        existing.flaggedAt = Date.now();
        saveDeviceFingerprints(deviceFingerprints);
        logActivity(installationId, 'clone_suspected', {
            storeName: storeName || null,
            username: username || null,
            originalFingerprint: existing.fingerprint,
            newFingerprint: hardwareFingerprint
        });
        return res.status(403).json({
            success: false,
            cloneSuspected: true,
            message: 'Ibang pisikal na device ang gumagamit ng installationId na ito kumpara sa dating na-verify. Na-flag ang device — kailangan ng manual na review ng developer/store owner bago ito payagan ulit.'
        });
    }

    existing.lastVerifiedAt = Date.now();
    existing.verifyCount = (existing.verifyCount || 0) + 1;
    saveDeviceFingerprints(deviceFingerprints);
    logActivity(installationId, 'device_reverified', {});

    res.json({
        success: true,
        allowed: allowedDevices.has(installationId),
        firstTime: false,
        message: 'Verified.'
    });
});

// --------------------------------------------------------------
// GET /relay/admin/api/devices/:installationId/fingerprint
// Para makita ng admin panel kung naka-flag ba bilang clone ang isang
// device, at para bigyan ng option na i-reset ang binding.
// --------------------------------------------------------------
app.get('/relay/admin/api/devices/:installationId/fingerprint', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    const record = deviceFingerprints.get(installationId) || null;
    res.json({ success: true, record });
});

// --------------------------------------------------------------
// POST /relay/admin/api/devices/:installationId/fingerprint/reset
// I-clear ang naka-bind na fingerprint (at ang flagged state) para sa
// isang installationId — gamitin ito kapag na-verify na ng
// developer/store owner na LEGIT na paglipat ito sa bagong device
// (hal. pinalitan ang unit/telepono ng customer), o kung false-positive
// ang naunang clone flag.
// --------------------------------------------------------------
app.post('/relay/admin/api/devices/:installationId/fingerprint/reset', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    deviceFingerprints.delete(installationId);
    saveDeviceFingerprints(deviceFingerprints);
    logActivity(installationId, 'device_fingerprint_reset', {});
    res.json({ success: true, message: 'Na-clear ang fingerprint binding — kailangan na namang mag-verify online sa susunod na login.' });
});

// --------------------------------------------------------------
// POST /relay/request-unlock
// Tinatawag ito ng CLIENT server (hindi diretso ng browser ng cashier)
// tuwing may humihiling mag-unlock ng isang Pro theme.
// --------------------------------------------------------------
app.post('/relay/request-unlock', requireApiKey, requireAllowedDevice, rateLimit('request-unlock', 5, 10 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
    const { installationId, featureId, featureName, price, username, storeName, photo } = req.body;

    if (!installationId || !featureId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId o featureId.' });
    }

    const otpCode = String(Math.floor(100000 + Math.random() * 900000));
    const key = `${installationId}:${featureId}`;
    pendingOtps.set(key, {
        code: otpCode,
        expiresAt: Date.now() + OTP_TTL_MS,
        requestedBy: username || 'Unknown',
        storeName: storeName || null,
        photo: photo || null,
        approved: false,
        otpVerified: false,
        installationId,
        featureId,
        featureName: featureName || featureId,
        price: price || null
    });

    try {
        // Ground-truth mula sa SARILING FEATURE_CATALOG ng relay — hindi
        // basta client-supplied na featureName/price ang isasalig, dahil
        // ang mga iyon ay galing lang sa request body (pwedeng palitan).
        // Kung hindi tugma, tahasang i-flag sa email para alertuhan ang
        // admin bago pa mag-Approve.
        const catalogEntry = FEATURE_CATALOG[featureId] || null;
        const priceMismatch = catalogEntry && typeof price === 'number' && price !== catalogEntry.price;
        const nameMismatch = catalogEntry && featureName && featureName !== catalogEntry.name;

        await notifyUnlockRequest({
            subject: `🎨 Unlock Request — ${featureName || featureId}${price ? ` (₱${price})` : ''}`,
            text: `May humiling na i-unlock ang isang Pro theme.\n\n` +
                  `Store: ${storeName || 'Hindi tiyak'}\n` +
                  `Installation ID: ${installationId}\n` +
                  `Feature: ${featureName || featureId}\n` +
                  (price ? `Presyo (sinabi ng client): ₱${price}\n` : '') +
                  (catalogEntry ? `Presyo ayon sa price list namin: ₱${catalogEntry.price} (${catalogEntry.name})\n` : `⚠️ Hindi nakita sa price list namin ang featureId na "${featureId}" — mag-ingat.\n`) +
                  ((priceMismatch || nameMismatch) ? `⚠️⚠️ MAY DISKREPANSIYA sa presyo/pangalan — hindi tugma sa opisyal na price list. HUWAG mag-Approve hangga't hindi ito na-verify.\n` : '') +
                  `Hiniling ni: ${username || 'Unknown'}\n` +
                  `OTP Code: ${otpCode}\n` +
                  `Mag-e-expire ito sa loob ng 10 minuto.\n\n` +
                  `I-verify muna ang bayad bago ibigay ang OTP na ito sa kliyente.`
        });

        logActivity(installationId, 'otp_requested', { featureId, featureName: featureName || featureId });
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
// --------------------------------------------------------------
// APPROVAL GATE — bago ito, kapag TAMA na ang OTP, agad na nabibigyan
// ng signed token ang kliyente. Ngayon, dagdag pang kondisyon: kailangan
// mo (ang may-ari, sa admin panel) na pindutin ang "Allow/Run" button
// BAGO talaga maisyu ang token — kahit pa tama na ang OTP na inilagay
// ng kliyente. Ito ay para masigurado mong nakabayad na talaga sila
// bago mo bigyan ng access, hindi lang basta tamang OTP.
// --------------------------------------------------------------
function checkApprovalGate(pending) {
    if (!pending.approved) {
        pending.otpVerified = true;
        pending.otpVerifiedAt = Date.now();
        return false;
    }
    return true;
}

app.post('/relay/confirm-unlock', requireApiKey, requireAllowedDevice, rateLimit('confirm-unlock', 120, 10 * 60 * 1000, (req) => req.body?.installationId), (req, res) => {
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
    if (!safeCompare(String(otp).trim(), pending.code)) {
        return res.status(400).json({ success: false, message: 'Maling OTP code.' });
    }

    if (!checkApprovalGate(pending)) {
        return res.json({
            success: false,
            pending: true,
            message: `Tama ang code para sa ${pending.featureName}! Naghihintay na lang ng approval mula sa may-ari. Pakisubukan ulit paglipas ng ilang segundo.`
        });
    }

    // Tama ang OTP — gumawa ng naka-sign na token. Ang payload ay
    // nagta-tali ng token na ito SA SPESIPIKONG installationId+featureId,
    // kaya hindi ito magagamit sa ibang installation o ibang theme. Kung
    // may durationDays na naitakda ng admin sa Approve, MAY EXPIRY ito
    // (auto-expiring license) — kung wala, permanente (dating behavior).
    const durationMs = typeof pending.durationDays === 'number' && pending.durationDays > 0
        ? pending.durationDays * 24 * 60 * 60 * 1000
        : null;
    const token = issueSignedToken(installationId, featureId, durationMs);

    recordIssuedUnlock(installationId, featureId, token, {
        featureName: pending.featureName,
        price: pending.price,
        source: 'otp'
    });
    logActivity(installationId, 'unlock_issued', { featureId, featureName: pending.featureName, source: 'otp' });

    pendingOtps.delete(key);

    res.json({
        success: true,
        message: `Na-unlock ang ${pending.featureName}!`,
        token
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
// Fallback/default lang ito ngayon — ang aktwal na tagal ng bawat demo ay
// PINIPILI NA NG ADMIN kada request (per-request, admin-configurable) sa
// Approve step sa admin panel, katulad ng ibang time-limited na
// features/subscriptions. Ginagamit lang ito kapag walang tahasang
// durationDays na naitakda (tingnan ang /relay/confirm-demo).
const DEMO_DURATION_MS = (Number(process.env.RELAY_DEMO_DURATION_HOURS) || 24) * 60 * 60 * 1000;

function formatDemoDurationLabel(durationMs) {
    const hours = durationMs / 3600000;
    if (hours < 48) {
        const rounded = Math.round(hours * 10) / 10;
        return `${rounded} oras`;
    }
    const days = Math.round((hours / 24) * 10) / 10;
    return `${days} araw`;
}

app.post('/relay/request-demo', requireApiKey, requireAllowedDevice, rateLimit('request-demo', 5, 10 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
    const { installationId, username, storeName, photo } = req.body;

    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }

    const otpCode = String(Math.floor(100000 + Math.random() * 900000));
    const key = `${installationId}:${DEMO_FEATURE_ID}`;
    pendingOtps.set(key, {
        code: otpCode,
        expiresAt: Date.now() + OTP_TTL_MS,
        requestedBy: username || 'Unknown',
        storeName: storeName || null,
        photo: photo || null,
        approved: false,
        otpVerified: false,
        installationId,
        featureId: DEMO_FEATURE_ID,
        featureName: 'Full Demo Mode',
        price: null
    });

    try {
        await notifyUnlockRequest({
            subject: `🕒 Demo Mode Request — ${storeName || 'Hindi tiyak'}`,
            text: `May humiling ng FULL DEMO MODE (lahat ng features, pansamantala lang).\n\n` +
                  `Store: ${storeName || 'Hindi tiyak'}\n` +
                  `Installation ID: ${installationId}\n` +
                  `Hiniling ni: ${username || 'Unknown'}\n` +
                  `OTP Code: ${otpCode}\n` +
                  `Mag-e-expire ang OTP code na ito sa loob ng 10 minuto.\n\n` +
                  `Pipiliin mo ang tagal ng demo (hal. ${formatDemoDurationLabel(DEMO_DURATION_MS)} bilang default) sa Admin Panel kapag Ina-Allow/Approve mo ito.\n` +
                  `Ibigay lang ito kung gusto mo talagang bigyan sila ng full trial.`
        });

        logActivity(installationId, 'otp_requested', { featureId: DEMO_FEATURE_ID, featureName: 'Full Demo Mode' });
        res.json({ success: true, message: 'Naipadala ang demo OTP request.' });
    } catch (err) {
        console.error('Relay mail send failure (demo):', err);
        pendingOtps.delete(key);
        res.status(500).json({ success: false, message: `Nabigo ang pagpapadala ng OTP: ${err.message}` });
    }
});

app.post('/relay/confirm-demo', requireApiKey, requireAllowedDevice, rateLimit('confirm-demo', 120, 10 * 60 * 1000, (req) => req.body?.installationId), (req, res) => {
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
    if (!safeCompare(String(otp).trim(), pending.code)) {
        return res.status(400).json({ success: false, message: 'Maling OTP code.' });
    }

    if (!checkApprovalGate(pending)) {
        return res.json({
            success: false,
            pending: true,
            message: 'Tama ang code para sa Demo Mode! Naghihintay na lang ng approval mula sa may-ari. Pakisubukan ulit paglipas ng ilang segundo.'
        });
    }

    // Ang TAGAL ng Demo Mode ay PINIPILI NA NGAYON NG ADMIN sa mismong
    // Approve step (parang ibang time-limited na feature/subscription),
    // gamit ang parehong pending.durationDays na ginagamit na ng
    // /relay/confirm-unlock — ang admin panel (approveOtp) ay may
    // hiwalay na oras-based na duration picker para dito (tingnan ang
    // promptForDemoDuration() sa public/admin/index.html). Kung sa
    // kadahilanan man ay walang natukoy na durationDays (hal. direktang
    // API call na nag-skip sa admin panel), babalik sa dating
    // DEMO_DURATION_MS default (RELAY_DEMO_DURATION_HOURS) — hindi
    // kailanman "walang expiry" nang hindi tahasang pinili ng admin.
    const durationMs = typeof pending.durationDays === 'number' && pending.durationDays > 0
        ? Math.round(pending.durationDays * 24 * 60 * 60 * 1000)
        : (pending.durationDays === null ? null : DEMO_DURATION_MS);
    const token = issueSignedToken(installationId, DEMO_FEATURE_ID, durationMs);

    recordIssuedUnlock(installationId, DEMO_FEATURE_ID, token, {
        featureName: 'Full Demo Mode',
        price: null,
        source: 'otp'
    });
    logActivity(installationId, 'unlock_issued', {
        featureId: DEMO_FEATURE_ID,
        featureName: 'Full Demo Mode',
        source: 'otp',
        durationDays: pending.durationDays ?? null
    });

    pendingOtps.delete(key);

    res.json({
        success: true,
        message: durationMs
            ? `Buksan na ang Demo Mode sa loob ng ${formatDemoDurationLabel(durationMs)}!`
            : 'Buksan na ang Demo Mode — walang expiry (tahasang pinili ng admin).',
        token
    });
});

// --------------------------------------------------------------
// POST /relay/admin/api/devices/:installationId/activate-demo
// Direktang nagbibigay ng FULL DEMO MODE (lahat ng features, pansamantala
// lang) sa isang device — WALANG OTP kailangan, at HINDI na kailangang
// maghintay na ang customer/kliyente mismo ang humingi muna
// (/relay/request-demo) bago ito ma-Allow/Approve. Gamitin ito kung
// PROAKTIBO mong gustong bigyan ng trial ang isang device (hal. bagong
// prospect, demo booth, o follow-up sa naka-Locked pang tindahan) —
// katulad ng "May reference ka na? i-activate agad" na admin-direct na
// flow ng ibang FEATURE_CATALOG entries sa /activate sa itaas, pero para
// dito sa DEMO_FEATURE_ID (na sinasadyang HINDI kasama sa FEATURE_CATALOG
// kaya hindi dumadaan sa parehong route). Parehong duration convention
// ang ginamit dito gaya ng promptForDemoDuration() sa admin panel
// (fractional na bilang ng araw — 0.25 = 6 oras, 0 = tahasang permanente,
// wala/undefined = babalik sa DEMO_DURATION_MS default).
// Body: { durationDays }
// --------------------------------------------------------------
app.post('/relay/admin/api/devices/:installationId/activate-demo', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    const { durationDays } = req.body;

    const durationMs = typeof durationDays === 'number' && durationDays > 0
        ? Math.round(durationDays * 24 * 60 * 60 * 1000)
        : (durationDays === 0 ? null : DEMO_DURATION_MS);

    const token = issueSignedToken(installationId, DEMO_FEATURE_ID, durationMs);

    recordIssuedUnlock(installationId, DEMO_FEATURE_ID, token, {
        featureName: 'Full Demo Mode',
        price: null,
        source: 'admin-direct'
    });
    logActivity(installationId, 'unlock_issued', {
        featureId: DEMO_FEATURE_ID,
        featureName: 'Full Demo Mode',
        source: 'admin-direct',
        durationDays: typeof durationDays === 'number' ? durationDays : null
    });

    res.json({
        success: true,
        message: durationMs
            ? `Na-activate ang Demo Mode sa loob ng ${formatDemoDurationLabel(durationMs)}.`
            : 'Na-activate ang Demo Mode — walang expiry (tahasang pinili ng admin).',
        token
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
app.post('/relay/request-unlock-bulk', requireApiKey, requireAllowedDevice, rateLimit('request-unlock-bulk', 5, 10 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
    const { installationId, featureIds, featureNames, totalPrice, username, storeName, photo } = req.body;

    if (!installationId || !Array.isArray(featureIds) || featureIds.length === 0) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId o featureIds.' });
    }

    const otpCode = String(Math.floor(100000 + Math.random() * 900000));
    const key = `${installationId}:__bulk__:${featureIds.slice().sort().join(',')}`;
    pendingOtps.set(key, {
        code: otpCode,
        expiresAt: Date.now() + OTP_TTL_MS,
        requestedBy: username || 'Unknown',
        storeName: storeName || null,
        photo: photo || null,
        approved: false,
        otpVerified: false,
        installationId,
        featureIds,
        featureNames: featureNames || featureIds,
        price: totalPrice || null
    });

    try {
        // Ground-truth mula sa SARILING FEATURE_CATALOG ng relay — hindi
        // basta client-supplied na totalPrice ang isasalig. Hindi natin
        // dine-duplicate dito ang proportional bundle-discount math (nasa
        // OMNIPOS client server lang iyon), pero ipinapakita ang à la
        // carte sum bilang reference kasama ng anumang unknown featureId,
        // para may masangguni ang admin bago mag-Approve.
        const unknownIds = featureIds.filter(id => !FEATURE_CATALOG[id]);
        const alaCarteTotal = featureIds.reduce((sum, id) => sum + (FEATURE_CATALOG[id] ? FEATURE_CATALOG[id].price : 0), 0);

        await notifyUnlockRequest({
            subject: `📦 Bundle Unlock Request (${featureIds.length} items)${totalPrice ? ` — ₱${totalPrice}` : ''}`,
            text: `May humiling na i-unlock ang isang BUNDLE ng ${featureIds.length} feature(s).\n\n` +
                  `Store: ${storeName || 'Hindi tiyak'}\n` +
                  `Installation ID: ${installationId}\n` +
                  `Features: ${(featureNames || featureIds).join(', ')}\n` +
                  (totalPrice ? `Total Presyo (sinabi ng client, posibleng may bundle discount): ₱${totalPrice}\n` : '') +
                  `À la carte na kabuuan ayon sa price list namin (walang discount): ₱${alaCarteTotal}\n` +
                  (unknownIds.length ? `⚠️⚠️ Hindi nakita sa price list namin ang: ${unknownIds.join(', ')} — mag-ingat, HUWAG mag-Approve hangga't hindi na-verify.\n` : '') +
                  `Hiniling ni: ${username || 'Unknown'}\n` +
                  `OTP Code: ${otpCode}\n` +
                  `Mag-e-expire ito sa loob ng 10 minuto.\n\n` +
                  `I-verify muna ang bayad bago ibigay ang OTP na ito sa kliyente.`
        });

        logActivity(installationId, 'otp_requested', { featureIds, featureNames: featureNames || featureIds });
        res.json({ success: true, message: 'Naipadala ang bundle OTP request.' });
    } catch (err) {
        console.error('Relay mail send failure (bulk):', err);
        pendingOtps.delete(key);
        res.status(500).json({ success: false, message: `Nabigo ang pagpapadala ng OTP: ${err.message}` });
    }
});

app.post('/relay/confirm-unlock-bulk', requireApiKey, requireAllowedDevice, rateLimit('confirm-unlock-bulk', 120, 10 * 60 * 1000, (req) => req.body?.installationId), (req, res) => {
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
    if (!safeCompare(String(otp).trim(), pending.code)) {
        return res.status(400).json({ success: false, message: 'Maling OTP code.' });
    }

    if (!checkApprovalGate(pending)) {
        return res.json({
            success: false,
            pending: true,
            message: 'Tama ang code para sa bundle na ito! Naghihintay na lang ng approval mula sa may-ari. Pakisubukan ulit paglipas ng ilang segundo.'
        });
    }

    const tokens = {};
    const namesList = pending.featureNames || featureIds;
    // Isang durationDays lang para sa buong bundle na ito (itinakda ng
    // admin sa Approve) — parehong expiresAt (o wala) ang makukuha ng
    // lahat ng featureId sa bundle.
    const durationMs = typeof pending.durationDays === 'number' && pending.durationDays > 0
        ? pending.durationDays * 24 * 60 * 60 * 1000
        : null;
    for (let i = 0; i < featureIds.length; i++) {
        const featureId = featureIds[i];
        const token = issueSignedToken(installationId, featureId, durationMs);
        tokens[featureId] = token;

        const featureName = namesList[i] || featureId;
        recordIssuedUnlock(installationId, featureId, token, {
            featureName,
            price: FEATURE_CATALOG[featureId] ? FEATURE_CATALOG[featureId].price : null,
            source: 'otp-bulk'
        });
        logActivity(installationId, 'unlock_issued', { featureId, featureName, source: 'otp-bulk' });
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
            if (!DEV_VIEWER_KEY || !safeCompare(String(msg.devKey || ''), DEV_VIEWER_KEY)) {
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
