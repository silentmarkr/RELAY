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

app.post('/relay/admin/api/pending-otps/approve', requireAdminKey, (req, res) => {
    const { key } = req.body;
    const pending = pendingOtps.get(key);
    if (!pending) {
        return res.status(404).json({ success: false, message: "Wala nang aktibong request na iyan (baka na-expire na o na-claim na)." });
    }
    pending.approved = true;
    logActivity(pending.installationId, 'admin_approved', { featureId: pending.featureId, featureName: pending.featureName });
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
        return {
            installationId,
            ...meta,
            allowed: allowedDevices.has(installationId),
            unlockedCount: unlockedIds.length,
            totalCatalogCount: Object.keys(FEATURE_CATALOG).length,
            demoActive: getActiveUnlockedFeatureIds(installationId).includes(DEMO_FEATURE_ID),
            lastActivationAt
        };
    }).sort((a, b) => b.lastSeenAt - a.lastSeenAt);

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
    logActivity(installationId, 'device_allowed', {});
    res.json({ success: true, allowedDevices: [...allowedDevices] });
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
// hard reset, ref: <invoice #>").
// --------------------------------------------------------------
app.post('/relay/admin/api/devices/:installationId/activate', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    const { featureId, featureIds, tierId, note } = req.body;

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

    const now = Date.now();
    const tokens = {};
    for (const id of idsToActivate) {
        const payload = { installationId, featureId: id, issuedAt: now };
        const payloadString = JSON.stringify(payload);
        const signature = crypto.sign(null, Buffer.from(payloadString), privateKey).toString('base64');
        const token = { payload, signature };
        tokens[id] = token;
        recordIssuedUnlock(installationId, id, token, {
            featureName: FEATURE_CATALOG[id].name,
            price: FEATURE_CATALOG[id].price,
            source: 'admin-direct',
            note: note || null
        });
        logActivity(installationId, 'unlock_issued', { featureId: id, featureName: FEATURE_CATALOG[id].name, source: 'admin-direct', note: note || null });
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
    if (!safeCompare(String(provided || ''), RELAY_API_KEY)) {
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
        await sendOtpMail({
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

app.post('/relay/confirm-unlock', requireApiKey, requireAllowedDevice, rateLimit('confirm-unlock', 120, 10 * 60 * 1000), (req, res) => {
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

    if (!checkApprovalGate(pending)) {
        return res.json({
            success: false,
            pending: true,
            message: `Tama ang code para sa ${pending.featureName}! Naghihintay na lang ng approval mula sa may-ari. Pakisubukan ulit paglipas ng ilang segundo.`
        });
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
    const token = { payload, signature };

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
const DEMO_DURATION_MS = (Number(process.env.RELAY_DEMO_DURATION_HOURS) || 24) * 60 * 60 * 1000;

app.post('/relay/request-demo', requireApiKey, requireAllowedDevice, rateLimit('request-demo', 5, 10 * 60 * 1000), async (req, res) => {
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
        await sendOtpMail({
            subject: `🕒 Demo Mode Request — ${storeName || 'Hindi tiyak'}`,
            text: `May humiling ng FULL DEMO MODE (lahat ng features, ${Math.round(DEMO_DURATION_MS / 3600000)} oras lang bago mag-expire).\n\n` +
                  `Store: ${storeName || 'Hindi tiyak'}\n` +
                  `Installation ID: ${installationId}\n` +
                  `Hiniling ni: ${username || 'Unknown'}\n` +
                  `OTP Code: ${otpCode}\n` +
                  `Mag-e-expire ang OTP code na ito sa loob ng 10 minuto.\n\n` +
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

app.post('/relay/confirm-demo', requireApiKey, requireAllowedDevice, rateLimit('confirm-demo', 120, 10 * 60 * 1000), (req, res) => {
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

    if (!checkApprovalGate(pending)) {
        return res.json({
            success: false,
            pending: true,
            message: 'Tama ang code para sa Demo Mode! Naghihintay na lang ng approval mula sa may-ari. Pakisubukan ulit paglipas ng ilang segundo.'
        });
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
    const token = { payload, signature };

    recordIssuedUnlock(installationId, DEMO_FEATURE_ID, token, {
        featureName: 'Full Demo Mode',
        price: null,
        source: 'otp'
    });
    logActivity(installationId, 'unlock_issued', { featureId: DEMO_FEATURE_ID, featureName: 'Full Demo Mode', source: 'otp' });

    pendingOtps.delete(key);

    res.json({
        success: true,
        message: `Buksan na ang Demo Mode sa loob ng ${Math.round(DEMO_DURATION_MS / 3600000)} oras!`,
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
app.post('/relay/request-unlock-bulk', requireApiKey, requireAllowedDevice, rateLimit('request-unlock-bulk', 5, 10 * 60 * 1000), async (req, res) => {
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
        await sendOtpMail({
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

        logActivity(installationId, 'otp_requested', { featureIds, featureNames: featureNames || featureIds });
        res.json({ success: true, message: 'Naipadala ang bundle OTP request.' });
    } catch (err) {
        console.error('Relay mail send failure (bulk):', err);
        pendingOtps.delete(key);
        res.status(500).json({ success: false, message: `Nabigo ang pagpapadala ng OTP: ${err.message}` });
    }
});

app.post('/relay/confirm-unlock-bulk', requireApiKey, requireAllowedDevice, rateLimit('confirm-unlock-bulk', 120, 10 * 60 * 1000), (req, res) => {
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

    if (!checkApprovalGate(pending)) {
        return res.json({
            success: false,
            pending: true,
            message: 'Tama ang code para sa bundle na ito! Naghihintay na lang ng approval mula sa may-ari. Pakisubukan ulit paglipas ng ilang segundo.'
        });
    }

    const now = Date.now();
    const tokens = {};
    const namesList = pending.featureNames || featureIds;
    for (let i = 0; i < featureIds.length; i++) {
        const featureId = featureIds[i];
        const payload = { installationId, featureId, issuedAt: now };
        const payloadString = JSON.stringify(payload);
        const signature = crypto.sign(null, Buffer.from(payloadString), privateKey).toString('base64');
        const token = { payload, signature };
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
