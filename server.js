// PATH: OMNIPOS/server.js  <-- I-REPLACE ang luma mong server.js nito (root ng OMNIPOS project)

const net = require('net');
const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const ExcelJS = require('exceljs');
const multer = require('multer');
const { execSync } = require('child_process');
const { readData, writeData, runLocalDatabaseBackup, checkModuleBlobSizes, mirrorBackupToDownloads, getCloudBackupPayload, getBackupStatus } = require('./db');
const webauthn = require('./webauthn');

try {
    // Dati: process.loadEnvFile(); — pinalitan para sumuporta sa
    // naka-encrypt na .env sa release build (env-loader.js). Sa dev
    // (plain .env pa), babalik din ito sa normal na loader.
    require('./env-loader')();
} catch (err) {

}

// ====================================================================
// CRASH-SAFETY NET
// ====================================================================
// Bakit kailangan ito: kung walang handler dito, ang ISANG hindi-
// nahuling error (hal. sa loob ng isang async route na walang try/catch,
// o isang error na galing sa isang callback/library) ay pwedeng
// mag-crash sa BUONG Node.js process — ibig sabihin bababa ang serbisyo
// sa LAHAT ng terminal/device na kumakabit dito, hanggang sa i-restart
// ito nang manual.
// Sa pamamagitan ng mga handler na ito, ang error ay naka-log lang at
// hindi na pinapatay ang server — patuloy pa rin itong tatakbo at
// makakapaglingkod sa ibang requests/terminals.
// PAALALA: hindi ito kapalit ng maayos na try/catch sa mismong endpoint
// — huling proteksyon lang ito ("safety net") laban sa mga hindi
// inaasahang error na nakalusot.
process.on('uncaughtException', (err) => {
    console.error('🔥 [CRASH-SAFETY] Uncaught Exception (hindi pinatay ang server):', err);
});

process.on('unhandledRejection', (reason) => {
    console.error('🔥 [CRASH-SAFETY] Unhandled Promise Rejection (hindi pinatay ang server):', reason);
});

// ====================================================================
// GMAIL / MAIL SENDING
// ====================================================================
// AYOS (refactor): inilipat ang buong Gmail SMTP + OAuth/API fallback
// logic (nodemailer transporter, network-error detection, Gmail REST
// API fallback, sendMailSmart, verifyMailCredentialsSmart) papunta sa
// hiwalay na module na ./mailer.js — dalawang dahilan:
//   1. Kondisyon: kailangang NAKAHIWALAY ang Gmail connection
//      verification logic (hindi ito nakabaon sa loob ng server.js), at
//      kasama ito sa release package (omnipos-client.zip) bilang sarili
//      nitong file — see build-release.js (SERVER_TARGETS).
//   2. Pinapayagan nito ang isang standalone na CLI tool
//      (verify-gmail-connection.js, kasama rin sa release package) na
//      i-verify ang Gmail SMTP/OAuth connection nang hiwalay sa buong
//      POS server — hindi na kailangang buuin at patakbuhin muna ang
//      buong app bago malaman kung gumagana ang koneksyon.
// Ang sendMailSmart/verifyMailCredentialsSmart mismo ay sumusunod na sa
// updated na patakaran (kondisyon #3): laging SUSUBUKAN muna ang SMTP
// (kasama na sa Render — hindi na ito basta nilalaktawan), at bababa
// LANG sa Gmail API/OAuth fallback kung talagang nabigo ang SMTP dahil
// sa network-level na error (hal. naka-block na port sa Render).
const { sendMailSmart, verifyMailCredentialsSmart } = require('./mailer');

const app = express();

// Para tama ang req.ip kapag dumaan sa cloudflared tunnel o ibang reverse proxy
// (babasahin ang X-Forwarded-For / CF-Connecting-IP na header).
app.set('trust proxy', true);

// Kinukuha ang "totoong" IP ng kliyente — priyoridad: CF-Connecting-IP (Cloudflare
// tunnel/proxy) → unang IP sa X-Forwarded-For → req.ip (Express, gamit ang trust proxy)
// → raw socket address bilang huling fallback. Tinatanggal din ang "::ffff:" prefix
// na idinagdag ng Node para sa IPv4-mapped IPv6 addresses (hal. "::ffff:192.168.1.5").
function getClientIp(req) {
    const cfIp = req.headers['cf-connecting-ip'];
    if (cfIp) return cfIp.trim().replace(/^::ffff:/, '');

    const xff = req.headers['x-forwarded-for'];
    if (xff) {
        const first = xff.split(',')[0].trim();
        if (first) return first.replace(/^::ffff:/, '');
    }

    const raw = req.ip || req.connection?.remoteAddress || req.socket?.remoteAddress || 'unknown';
    return raw.replace(/^::ffff:/, '');
}

// Kinukuha ang lahat ng LAN/WiFi subnet na naka-assign sa DEVICE na nagpapatakbo ng
// server (hal. "192.168.1.0/24" mula sa wlan0). Ginagamit ito para malaman kung ang
// isang connecting IP ay nanggaling sa PAREHONG WiFi/LAN network ng server, o hindi
// (galing sa ibang network / mobile data / sa labas via cloudflared tunnel).
function getServerLanSubnets() {
    const subnets = [];
    const interfaces = os.networkInterfaces();
    for (const name of Object.keys(interfaces)) {
        for (const iface of interfaces[name]) {
            if (iface.family === 'IPv4' && !iface.internal) {
                subnets.push({ address: iface.address, netmask: iface.netmask });
            }
        }
    }
    return subnets;
}

function ipToLong(ip) {
    const parts = ip.split('.').map(Number);
    if (parts.length !== 4 || parts.some(p => Number.isNaN(p))) return null;
    return ((parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]) >>> 0;
}

// True kung ang `ip` ay nasa parehong subnet ng alinman sa LAN interfaces ng server
// (ibig sabihin, parehong WiFi/network sila). False kung galing sa ibang network,
// localhost, o hindi mabasa (hal. IPv6, "unknown").
function isSameLanAsServer(ip) {
    if (!ip || ip === 'unknown') return false;
    if (ip === '127.0.0.1' || ip === '::1') return true; // parehong device mismo
    const ipLong = ipToLong(ip);
    if (ipLong === null) return false;

    const subnets = getServerLanSubnets();
    for (const { address, netmask } of subnets) {
        const addrLong = ipToLong(address);
        const maskLong = ipToLong(netmask);
        if (addrLong === null || maskLong === null) continue;
        if ((ipLong & maskLong) === (addrLong & maskLong)) return true;
    }
    return false;
}

let helmet;
try {
    helmet = require('helmet');
} catch (err) {
    console.warn('⚠️ Hindi pa naka-install ang "helmet" package. Patakbuhin ang `npm install` para magamit ito. Gagamitin muna ang manual security headers bilang fallback.');
}

if (helmet) {
    app.use(helmet({

        contentSecurityPolicy: false,
        crossOriginEmbedderPolicy: false
    }));
} else {
    app.use((req, res, next) => {
        res.setHeader('X-Content-Type-Options','nosniff');
        res.setHeader('X-Frame-Options','DENY');
        res.setHeader('Referrer-Policy','same-origin');
        res.removeHeader('X-Powered-By');
        next();
    });
}

const RATE_LIMIT_BUCKETS = new Map();
function rateLimit(routeKey, maxAttempts, windowMs) {
    return (req, res, next) => {
        const ip = req.ip || req.connection?.remoteAddress ||'unknown';
        const key = `${routeKey}:${ip}`;
        const now = Date.now();
        let attempts = RATE_LIMIT_BUCKETS.get(key) || [];
        attempts = attempts.filter(ts => now - ts < windowMs);
        if (attempts.length >= maxAttempts) {
            const retryAfterSec = Math.ceil((windowMs - (now - attempts[0])) / 1000);
            res.setHeader('Retry-After', retryAfterSec);
            return res.status(429).json({
                success: false,
                message: `Sobra na sa allowed attempts. Subukan muli pagkatapos ng ${retryAfterSec} segundo.`
            });
        }
        attempts.push(now);
        RATE_LIMIT_BUCKETS.set(key, attempts);
        next();
    };
}

setInterval(() => {
    const now = Date.now();
    for (const [key, attempts] of RATE_LIMIT_BUCKETS.entries()) {
        const fresh = attempts.filter(ts => now - ts < 15 * 60 * 1000);
        if (fresh.length === 0) RATE_LIMIT_BUCKETS.delete(key);
        else RATE_LIMIT_BUCKETS.set(key, fresh);
    }
}, 5 * 60 * 1000).unref();

// Dating gawi: iisang bucket lang bawat IP ang gamit sa /api/auth/login, kaya
// kapag maraming maling attempt sa IISANG account (hal. cashier1), naka-lock
// na rin agad ang LAHAT ng ibang account na naka-login/nagta-try mag-login
// mula sa parehong terminal/IP (karaniwan sa POS na iisa lang ang IP ng
// lahat ng cashier). Hindi dapat ganito — dapat hiwalay ang bawat account.
//
// Ang loginRateLimit() na ito ay gumagamit ng DALAWANG hiwalay na bucket:
//   1. Per-ACCOUNT bucket (keyed sa username) — ito ang totoong proteksyon
//      laban sa brute-force sa isang partikular na account, at HINDI na
//      naka-apekto sa ibang account kahit magkaparehas ang IP/terminal.
//   2. Per-IP bucket na mas mataas ang threshold — pangkalahatang proteksyon
//      lamang laban sa pag-spam/scan ng maraming iba't ibang username mula
//      sa iisang terminal, hindi na dapat maabot ito sa normal na paggamit.
// FIX: dating iisang middleware ito na agad nagre-record ng attempt sa
// SANDALING dumating ang POST /api/auth/login — kahit pa i-block pa lang
// ito ng anti-clone/Relay device-check bago pa man masuri ang username/
// password (hal. dahil timeout/unreachable ang Relay). Ibig sabihin,
// habang matagal/unreachable ang Relay, kada retry ng user (o kada
// timeout ng frontend) ay nauubos na ang quota kahit walang totoong
// maling password na na-try — kaya "max attempt" agad kahit hindi pa
// nga nakaka-successful na login attempt.
//
// Ngayon: hinati sa DALAWANG hakbang — checkLoginRateLimit() (read-only,
// walang binabago) ay tinatawag muna bago ang device-check (para sumagot
// agad kung na-lock na talaga); recordLoginAttempt() (ito ang
// nagdadagdag sa bucket) ay tinatawag na lang PAGKATAPOS pumasa ang
// device-check — ibig sabihin, ang quota ay para lang sa mga totoong
// pagkuha ng username/password, hindi sa mga naka-block dahil lang sa
// Relay connectivity.
function checkLoginRateLimit(req, res, maxAttemptsPerAccount, maxAttemptsPerIp, windowMs) {
    const ip = req.ip || req.connection?.remoteAddress ||'unknown';
    const username = ((req.body && req.body.username) ||'').toString().trim().toLowerCase();
    const now = Date.now();

    const ipKey = `login-ip:${ip}`;
    const ipAttempts = (RATE_LIMIT_BUCKETS.get(ipKey) || []).filter(ts => now - ts < windowMs);
    if (ipAttempts.length >= maxAttemptsPerIp) {
        const retryAfterSec = Math.ceil((windowMs - (now - ipAttempts[0])) / 1000);
        res.setHeader('Retry-After', retryAfterSec);
        res.status(429).json({
            success: false,
            message: `Too many login attempts from this terminal. Please try again in ${retryAfterSec} seconds.`
        });
        return false;
    }

    if (username) {
        const acctKey = `login-account:${username}`;
        const acctAttempts = (RATE_LIMIT_BUCKETS.get(acctKey) || []).filter(ts => now - ts < windowMs);
        if (acctAttempts.length >= maxAttemptsPerAccount) {
            const retryAfterSec = Math.ceil((windowMs - (now - acctAttempts[0])) / 1000);
            res.setHeader('Retry-After', retryAfterSec);
            res.status(429).json({
                success: false,
                message: `Too many attempts for account '${username}'. Please try again in ${retryAfterSec} seconds. Other accounts are not affected.`
            });
            return false;
        }
    }

    return true;
}

function recordLoginAttempt(req, windowMs) {
    const ip = req.ip || req.connection?.remoteAddress ||'unknown';
    const username = ((req.body && req.body.username) ||'').toString().trim().toLowerCase();
    const now = Date.now();

    const ipKey = `login-ip:${ip}`;
    const ipAttempts = (RATE_LIMIT_BUCKETS.get(ipKey) || []).filter(ts => now - ts < windowMs);
    ipAttempts.push(now);
    RATE_LIMIT_BUCKETS.set(ipKey, ipAttempts);

    if (username) {
        const acctKey = `login-account:${username}`;
        const acctAttempts = (RATE_LIMIT_BUCKETS.get(acctKey) || []).filter(ts => now - ts < windowMs);
        acctAttempts.push(now);
        RATE_LIMIT_BUCKETS.set(acctKey, acctAttempts);
    }
}

const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS ||'')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);

app.use(cors(ALLOWED_ORIGINS.length > 0 ? {
    origin(origin, callback) {

        if (!origin || ALLOWED_ORIGINS.includes(origin)) return callback(null, true);
        return callback(new Error(`CORS: hindi pinapayagang origin — ${origin}`));
    }
} : undefined));

app.use(express.json({ limit:'2mb' }));

app.use(express.static(path.join(__dirname,'public'), {
    etag: true,
    lastModified: true,
    setHeaders: (res) => {
        res.setHeader('Cache-Control','no-cache');
    }
}));

const SESSIONS = new Map();
const SESSION_TTL_MS = 8 * 60 * 60 * 1000;

const SESSIONS_MODULE ='sessions';

function persistSessions() {
    const snapshot = {};
    for (const [token, session] of SESSIONS.entries()) snapshot[token] = session;
    writeData(SESSIONS_MODULE, snapshot);
}

function loadSessionsFromDisk() {
    const snapshot = readData(SESSIONS_MODULE, {});
    const now = Date.now();
    let restored = 0;
    for (const [token, session] of Object.entries(snapshot)) {
        if (session && session.expiresAt > now) {
            SESSIONS.set(token, session);
            restored++;
        }
    }
    if (restored > 0) console.log(`🔄 Naibalik ang ${restored} aktibong session mula sa huling pagkaka-save (bago pa mag-restart).`);
}
loadSessionsFromDisk();

function parseDeviceInfo(userAgent) {
    const ua = (userAgent ||'');

    let deviceType ='Desktop';
    if (/Tablet|iPad/i.test(ua) || (/Android/i.test(ua) && !/Mobile/i.test(ua))) {
        deviceType ='Tablet';
    } else if (/Mobi|Android|iPhone|iPod|Windows Phone/i.test(ua)) {
        deviceType ='Mobile';
    }

    let os ='Unknown OS';
    if (/Windows NT/i.test(ua)) os ='Windows';
    else if (/Mac OS X/i.test(ua) && !/iPhone|iPad|iPod/i.test(ua)) os ='macOS';
    else if (/Android/i.test(ua)) os ='Android';
    else if (/iPhone|iPad|iPod/i.test(ua)) os ='iOS';
    else if (/Linux/i.test(ua)) os ='Linux';

    let browser ='Unknown Browser';
    if (/Edg\//i.test(ua)) browser ='Edge';
    else if (/OPR\/|Opera/i.test(ua)) browser ='Opera';
    else if (/Chrome\//i.test(ua) && !/Chromium/i.test(ua)) browser ='Chrome';
    else if (/CriOS\//i.test(ua)) browser ='Chrome';
    else if (/Firefox\//i.test(ua)) browser ='Firefox';
    else if (/Safari\//i.test(ua) && !/Chrome\//i.test(ua)) browser ='Safari';

    return { deviceType, os, browser, label: `${deviceType} · ${os} · ${browser}` };
}

function createSession(username, role, userAgent, ip) {
    const token = crypto.randomBytes(32).toString('hex');

    SESSIONS.set(token, { username, role, loginAt: Date.now(), expiresAt: Date.now() + SESSION_TTL_MS, device: parseDeviceInfo(userAgent), ip: ip || 'unknown' });
    persistSessions();
    return token;
}

function getSession(token) {
    const session = SESSIONS.get(token);
    if (!session) return null;
    if (Date.now() > session.expiresAt) {
        SESSIONS.delete(token);
        return null;
    }
    session.expiresAt = Date.now() + SESSION_TTL_MS;
    return session;
}

function destroySession(token) {
    SESSIONS.delete(token);
    persistSessions();
}

function renameUsernameEverywhere(oldUsername, newUsername) {
    for (const session of SESSIONS.values()) {
        if (session.username.toLowerCase() === oldUsername.toLowerCase()) {
            session.username = newUsername;
        }
    }
    persistSessions();
    const cartsData = readData(FILE_CARTS, {});
    const cartKey = Object.keys(cartsData).find(k => k.toLowerCase() === oldUsername.toLowerCase());
    if (cartKey && cartKey !== newUsername) {
        cartsData[newUsername] = cartsData[cartKey];
        delete cartsData[cartKey];
        writeData(FILE_CARTS, cartsData);
    }
}

setInterval(() => {
    const now = Date.now();
    let removedAny = false;
    for (const [token, session] of SESSIONS.entries()) {
        if (now > session.expiresAt) {
            SESSIONS.delete(token);
            removedAny = true;
        }
    }
    if (removedAny) persistSessions();
}, 15 * 60 * 1000).unref();

setInterval(persistSessions, 2 * 60 * 1000).unref();

// --- AUTO-BACKUP TOGGLE ---
// Habang nasa TESTING pa ang system, i-set ang DISABLE_AUTO_BACKUP=true sa .env
// para hindi na mag-scheduled backup (walang bagong .db files na maiipon sa
// database/backups/). Pagbebentahan/deployment na: alisin lang ang env var
// (o gawing false) — awtomatikong babalik ang normal na 30s-startup +
// 24-hour na auto-backup schedule, walang kailangan pang baguhin dito sa code.
const AUTO_BACKUP_DISABLED = String(process.env.DISABLE_AUTO_BACKUP ||'').trim().toLowerCase() ==='true';

// HIWALAY na switch para sa FILE INTEGRITY MONITOR (real-time watcher,
// scheduled 55s/24h check-in, at ang mabilis na "Check Now" poll) —
// FIX: dati, kasabay ito ng DISABLE_AUTO_BACKUP (parehong
// "AUTO_BACKUP_DISABLED" flag), kaya ang sinong nag-set ng
// DISABLE_AUTO_BACKUP=true para lang ihinto ang DATABASE backup ay
// hindi sinasadyang na-o-off DIN ang buong tamper/file-change detector
// papunta sa RELAY — walang error/warning man lang na lumalabas, "may
// laman" pa rin ang dating status pero wala nang bagong check-in kaya
// hindi na nagre-red-flag kahit may binago pa. Ngayon, HIWALAY at
// DEFAULT na naka-ON ang integrity monitor (independiyente sa backup
// toggle) — kailangan pang EKSPLISITONG i-set ang
// DISABLE_INTEGRITY_MONITOR=true kung talagang gusto itong i-off.
const INTEGRITY_MONITOR_DISABLED = String(process.env.DISABLE_INTEGRITY_MONITOR || '').trim().toLowerCase() === 'true';

if (INTEGRITY_MONITOR_DISABLED) {
    console.log('⏸️  Naka-disable ang file integrity monitor (DISABLE_INTEGRITY_MONITOR=true sa .env). Alisin/i-false ang env var para i-enable ulit ito.');
}

if (AUTO_BACKUP_DISABLED) {
    console.log('⏸️  Naka-disable ang auto-backup (DISABLE_AUTO_BACKUP=true sa .env). Alisin/i-false ang env var para i-enable ulit ito.');
} else {
    setTimeout(() => runLocalDatabaseBackup(14), 30 * 1000);
    setInterval(() => runLocalDatabaseBackup(14), 24 * 60 * 60 * 1000).unref();
}

// Blob-size monitor: read-only lang, kaya tumatakbo ito REGARDLESS ng
// DISABLE_AUTO_BACKUP toggle sa itaas — hindi ito nagsusulat/nag-a-alter
// ng anumang datos, babala lang ito para sa future scaling awareness.
setTimeout(() => checkModuleBlobSizes(), 45 * 1000);
setInterval(() => checkModuleBlobSizes(), 24 * 60 * 60 * 1000).unref();

function extractToken(req) {
    const authHeader = req.headers['authorization'] ||'';
    if (authHeader.startsWith('Bearer ')) return authHeader.slice(7).trim();
    return req.headers['x-auth-token'] ||'';
}

const PUBLIC_API_PATHS = new Set(['/api/auth/login','/api/auth/login/verify-otp','/api/auth/webauthn/login-options','/api/auth/webauthn/login-verify']);

app.use((req, res, next) => {
    if (!req.path.startsWith('/api/')) return next();
    if (PUBLIC_API_PATHS.has(req.path)) return next();

    const token = extractToken(req);
    if (!token) {
        return res.status(401).json({ success: false, code:'NO_TOKEN', message:'Kailangan mag-login muna. Walang session token.' });
    }

    const session = getSession(token);
    if (!session) {
        return res.status(401).json({ success: false, code:'INVALID_TOKEN', message:'Expired o invalid na ang session. Mangyaring mag-login muli.' });
    }

    // DEVICE-REVOCATION GUARD: suriin ang huling naka-imbak na
    // relayAuthorized flag bago patuloy. LOCAL read lang ito (walang tawag
    // sa RELAY per-request), kaya mura lang.
    const currentDeviceData = readFeatureUnlocks();
    if (currentDeviceData.relayAuthorized === false) {
        destroySession(token);
        return res.status(401).json({
            success: false,
            code:'DEVICE_REVOKED',
            message:'Inalis ng developer/store owner ang device na ito sa listahan ng mga pinapayagang device. Awtomatikong na-logout ka. Kontakin ang developer/store owner.'
        });
    }

    // Traffic-driven na live recheck (throttled, fire-and-forget): sa
    // halip na hiwalay na standalone timer na tumatakbo kahit walang
    // ginagawa, dito lang ito magpapa-trigger — sa BAWAT tunay na
    // request ng isang naka-login na user, kada DEVICE_REVOCATION_RECHECK_MS
    // lang. Kaya kung idle ang device (walang request), 0 tawag sa RELAY
    // — pareho pa rin sa orihinal na "one-time/opportunistic" na disenyo.
    // Hindi ito naghihintay/nagpapabagal sa kasalukuyang request (async,
    // hindi awaited) — sa SUSUNOD na request lang ma-re-reflect ang
    // anumang bagong resulta nito.
    if (Date.now() - lastLiveRecheckAt > DEVICE_REVOCATION_RECHECK_MS) {
        lastLiveRecheckAt = Date.now();
        recheckDeviceAuthorizationLive();
    }

    req.authUser = { username: session.username, role: session.role };
    req.authToken = token;

    if (req.body && typeof req.body ==='object') {
        if (typeof req.body.username ==='string') req.body.username = req.authUser.username;
        if (typeof req.body.user ==='string') req.body.user = req.authUser.username;
        if (typeof req.body.requester ==='string') req.body.requester = req.authUser.username;
    }
    if (req.query && typeof req.query ==='object') {
        if ('requester' in req.query) req.query.requester = req.authUser.username;
    }

    next();
});

const FILE_USERS ='users';
const FILE_PRODUCTS ='products';
const FILE_TRANSACTIONS ='transactions';
const FILE_REFUNDS ='refunds';
const FILE_USERLOGS ='userlogs';
const FILE_REQUESTS ='requests';
const DEFAULT_CATEGORIES = ['Beverages','Dairy','Snacks','Bakery','Grains'];
const FILE_CATEGORIES ='categories';
const FILE_CARTS ='carts';
const FILE_CUSTOMERS ='customers';
const FILE_PROMOCODES ='promocodes';
const FILE_SHIFTS ='shifts';
const FILE_SHIFT_META ='shiftMeta';
const FILE_PURCHASE_ORDERS ='purchaseOrders';

const FILE_LOWSTOCK_TRACKING ='lowStockTracking';

const MENU_REGISTRY = [
    { key:'terminal',     label:'POS Terminal' },
    { key:'dashboard',    label:'Inventory Dashboard' },
    { key:'products',     label:'Products' },
    { key:'barcode',      label:'Barcode' },
    { key:'transactions', label:'Transactions' },

    { key:'transactions_view_all', label:'Transactions — View All Cashiers' },

    { key:'void_own_password', label:'Transactions — Void gamit ang Sariling Password (Hindi na kailangan ng Admin Password)' },

    { key:'refund', label:'Transactions — Pwedeng Mag-process ng Refund (Full o Partial)' },

    { key:'refund_own_password', label:'Transactions — Refund gamit ang Sariling Password (Hindi na kailangan ng Admin Password)' },

    { key:'manual_discount_own_password', label:'Transactions — Authorize Manual Discount With Own Password (Admin Password Not Required)' },

    { key:'reports',      label:'Sales Report' },
    { key:'users',        label:'Users' },
    { key:'logs',         label:'User Logs' },

    { key:'edit_user_profile', label:'Edit User Profile (Widget)' },
    { key:'customers', label:'Customers & Loyalty' },

    { key:'loyalty_card_issue', label:'Customers & Loyalty — Issue/Regenerate Loyalty Card or QR (Authorized Personnel Only, e.g. New Customer Enrollment or Lost Card Replacement)' },

    { key:'loyalty_redeem_own_password', label:'Transactions — Authorize MANUAL Loyalty Points Redemption With Own Password (No Card/QR Scan, Admin Password Not Required)' },

    { key:'shiftreport', label:'Shift / Z-Reading' },

    { key:'reorder', label:'Reorder Alerts / Purchase Orders' },

    { key:'shiftreport_view_all', label:'Shift / Z-Reading — View All Cashiers' },

    { key:'shiftreport_view_amounts', label:'Shift / Z-Reading — View Sales Amounts (Gross/Discount/Net)' },

    { key:'shift_close_control', label:'Shift / Z-Reading — Admin/Supervisor Control (Close Other Cashiers\' Shift)' },

    { key:'shift_close_own_password', label:'Shift / Z-Reading — Pwedeng Mag-authorize ng Close gamit ang Sariling Password (Hindi na kailangan ng Admin Password)' },

    { key:'restock_direct_apply', label:'Reorder Alerts — Quick Restock Direct Apply (No Approval Needed)' },

    { key:'products_direct_apply', label:'Products — Add/Update/Delete Direct Apply (No Approval Needed)' },

    { key:'users_manage', label:'Users — Users Management Tab (view/add accounts)' },

    { key:'pending_requests', label:'Users — Pending Requests Tab' },

    { key:'roles_permissions_view', label:'Users — Roles & Permissions Tab (opening/viewing the RBAC matrix)' },

    { key:'reset_restore', label:'Users — Reset/Restore Tab' },

    { key:'receipt_settings_view', label:'Users — Receipt Customization Tab (view/open access)' },

    { key:'receipt_settings_direct_apply', label:'Receipt Customization — Direct Apply (No Approval Needed)' },

    { key:'store_settings_view', label:'Users — Store & Sales Settings Tab (view/open access)' },

    { key:'store_settings_direct_apply', label:'Store & Sales Settings — Direct Apply (No Approval Needed)' },

    { key:'ux_settings_view', label:'Users — Appearance/UX Settings Tab (view/open access)' },

    { key:'ux_settings_direct_apply', label:'Appearance/UX Settings — Direct Apply (No Approval Needed)' },

    { key:'advanced_settings_view', label:'Users — Advanced Settings Tab (view/open access)' },

    { key:'advanced_settings_direct_apply', label:'Advanced Settings — Direct Apply (No Approval Needed)' },

    { key:'relay_unlock_request', label:'Features/Themes — Pwedeng Mag-send ng Unlock/Demo OTP Request sa Relay' },
];

const FILE_ROLES ='roles';

const DEFAULT_ROLES = [
    {
        name:'Admin',

        protected: true,
        permissions: MENU_REGISTRY.reduce((acc, m) => { acc[m.key] = true; return acc; }, {})
    },
    {
        name:'Staff',
        protected: false,
        permissions: { terminal: true, dashboard: true, products: true, barcode: true, transactions: true, transactions_view_all: false, void_own_password: false, refund: false, refund_own_password: false, reports: false, users: false, logs: false, edit_user_profile: false, customers: true, loyalty_card_issue: false, loyalty_redeem_own_password: false, shiftreport: true, shiftreport_view_amounts: true, shift_close_control: false, shift_close_own_password: false, restock_direct_apply: false, products_direct_apply: false, users_manage: false, pending_requests: false, roles_permissions_view: false, reset_restore: false, receipt_settings_view: false, receipt_settings_direct_apply: false, store_settings_view: false, store_settings_direct_apply: false, ux_settings_view: false, ux_settings_direct_apply: false, advanced_settings_view: false, advanced_settings_direct_apply: false, relay_unlock_request: false }
    },
    {
        name:'Cashier',
        protected: false,

        permissions: { terminal: true, dashboard: false, products: false, barcode: false, transactions: true, transactions_view_all: false, void_own_password: false, refund: false, refund_own_password: false, reports: false, users: false, logs: false, edit_user_profile: false, customers: true, loyalty_card_issue: false, loyalty_redeem_own_password: false, shiftreport: true, shiftreport_view_amounts: false, shift_close_control: false, shift_close_own_password: false, restock_direct_apply: false, products_direct_apply: false, users_manage: false, pending_requests: false, roles_permissions_view: false, reset_restore: false, receipt_settings_view: false, receipt_settings_direct_apply: false, store_settings_view: false, store_settings_direct_apply: false, ux_settings_view: false, ux_settings_direct_apply: false, advanced_settings_view: false, advanced_settings_direct_apply: false, relay_unlock_request: false }
    }
];

function getRoles() {
    let roles = readData(FILE_ROLES, DEFAULT_ROLES);

    let changed = false;
    roles.forEach(r => {
        if (!r.permissions) { r.permissions = {}; changed = true; }
        MENU_REGISTRY.forEach(m => {
            if (!(m.key in r.permissions)) {
                r.permissions[m.key] = !!r.protected;
                changed = true;
            }
        });
    });
    if (changed) writeData(FILE_ROLES, roles);
    return roles;
}

function getPermissionsForRole(roleName) {
    const roles = getRoles();
    const role = roles.find(r => r.name.toLowerCase() === (roleName ||'').toLowerCase());
    if (!role) {

        if ((roleName ||'').toLowerCase() ==='admin') {
            return MENU_REGISTRY.reduce((acc, m) => { acc[m.key] = true; return acc; }, {});
        }
        return {};
    }
    return role.permissions;
}

// Hinahanap kung kaninong account ang pumasok na password, at kung
// pinahihintulutan ang taong iyon (Admin, o may role na naka-check ang
// 'void_own_password' sa RBAC / Roles & Permissions matrix) na mag-
// authorize ng void. SADYANG hindi ito nakabase sa req.authUser (ang
// naka-login sa terminal, karaniwa'y ang cashier) — kasi ang totoong
// real-world flow ay: naka-login ang CASHIER sa terminal, at isang
// Supervisor/Manager ang lumalapit at nagta-type ng SARILING password
// nila para paunahan ang void. Kaya kailangang hanapin sa LAHAT ng
// accounts kung kaninong password ang na-type, hindi lang sa account
// ng kasalukuyang naka-login.
// GENERIC na bersyon: parehong logic gamit ng void at ng shift-close
// (Z-Reading) authorization — hinahanap sa LAHAT ng accounts kung
// kaninong password ang na-type (Admin, o kung sino man ang may
// `permissionKey` na permission), dahil hindi laging ang naka-login sa
// terminal ang Supervisor/Manager na nagta-type ng sariling password.
// PERFORMANCE/CONCURRENCY FIX: dating gumagamit ito ng bcrypt.compareSync()
// sa loob ng isang for-loop na dumadaan sa LAHAT ng user accounts hanggang
// may matugma. Ang bcryptjs (pure-JS na implementation, ginagamit dito sa
// halip na native bcrypt — tingnan ang paliwanag sa db.js kung bakit
// node:sqlite/pure-JS ang piniling approach para sa Termux/Android
// compatibility) ay SYNCHRONOUS at CPU-BOUND ang compareSync() nito —
// ibig sabihin NAKA-BLOCK ang BUONG Node.js event loop habang tumatakbo
// ito (hindi lang ang kasalukuyang request, kundi LAHAT ng ibang kasabay
// na request/terminal sa buong system, kabilang ang mga background
// polling call gaya ng GET /api/products). Sa isang store na may ilang
// user accounts, at doble pa ito tinatawag bawat Manual Discount/Loyalty
// Redeem checkout (una sa POST /api/auth/verify-void na client-side
// pre-check, tapos ULIT sa loob mismo ng POST /api/transactions bilang
// server-side re-verification), maaari itong mag-block ng buong ilang
// daang milliseconds hanggang ilang segundo sa mahihinang device — dito
// nagmumula ang pinaka-malaking pagkakaiba sa OBSERVED latency sa pagitan
// ng may-discount/redeem na transaksyon at ng normal na walang discount.
//
// AYOS: async na ngayon ito, gamit ang bcrypt.compare() (Promise-based).
// Sa bcryptjs partikular, HINDI ito nag-o-offload sa isang thread pool
// (hindi tulad ng native bcrypt) — sa halip, ginagawa nito ang trabaho sa
// maliliit na chunks gamit ang setImmediate() sa pagitan, kaya
// PANSAMANTALANG naibabalik ang control sa event loop sa pagitan ng mga
// chunk na iyon (hindi 100% non-blocking gaya ng thread-pool approach,
// pero HINDI na rin ito iisang malaking unbroken block gaya ng
// compareSync — nabibigyan ng pagkakataon ang ibang pending
// request/callback na maisingit sa pagitan). Lahat ng caller sa ibaba ay
// na-update na rin para mag-`await`.
async function findPasswordAuthorizer(users, password, permissionKey) {
    if (!password) return null;
    for (const u of users) {
        let match = false;
        try {
            match = await bcrypt.compare(password, u.password);
        } catch (e) {
            match = (password === u.password);
        }
        if (!match) continue;

        const role = (u.role ||'').toLowerCase();
        if (role ==='admin') return { user: u, isAdmin: true };
        if (permissionKey && !!getPermissionsForRole(u.role)[permissionKey]) return { user: u, isAdmin: false };

        // Tumugma ang password sa account na ito pero walang access ang
        // role nila para dito — huwag nang ituloy ang paghahanap, dahil
        // iisa lamang ang account na dapat tumugma sa isang password.
        return null;
    }
    return null;
}

function findVoidAuthorizer(users, password) {
    return findPasswordAuthorizer(users, password,'void_own_password');
}

// Para sa Refund (hiwalay sa Void): kailangan ng Admin password, o
// password ng account na may `refund_own_password` permission (hal.
// Supervisor/Manager role) — kagaya ng void, hindi basta-basta
// pinapayagan ang sinumang naka-login sa terminal na mag-refund nang
// walang pag-verify.
function findRefundAuthorizer(users, password) {
    return findPasswordAuthorizer(users, password,'refund_own_password');
}

// For Manual Discounts (item-level or cart-level "MANUAL" discount type):
// requires the Admin password, or the password of an account with the
// `manual_discount_own_password` permission (e.g. Supervisor/Manager) —
// a Cashier can no longer apply a discretionary discount (e.g. 100% off)
// without verification, since this is the most common "sweethearting"
// fraud vector (the price is lowered in the system, but the customer is
// still charged full price, and the cashier pockets the difference).
function findManualDiscountAuthorizer(users, password) {
    return findPasswordAuthorizer(users, password,'manual_discount_own_password');
}

// Para sa Shift Close / Z-Reading: kailangan ng Admin password, o
// password ng account na may `shift_close_own_password` permission
// (hal. Supervisor/Manager role) — hindi puwedeng basta i-close ng
// Cashier ang sariling shift nang walang pag-verify na ito, kahit
// naka-login na sila sa terminal.
function findShiftCloseAuthorizer(users, password) {
    return findPasswordAuthorizer(users, password,'shift_close_own_password');
}

// ------------------------------------------------------------------
// LOYALTY CARD / QR SECURITY
// ------------------------------------------------------------------
// Bawat customer ay maaaring bigyan ng isang physical loyalty CARD o
// isang QR code na SIYA LANG ang may hawak nito — ito ang "something
// you have" na security factor. Kapag na-scan ito sa terminal, DIRETSO
// nang naa-authorize ang points redemption (hindi na kailangan ng
// password), dahil ang pagkakaroon mismo ng tamang card/QR ang siyang
// nagpapatunay na ang customer na iyon (o kung sino man may hawak ng
// card) ang nag-a-authorize ng sariling redemption niya.
//
// Kung walang na-scan na card/QR (manual entry sa terminal), kailangan
// ng Admin password o password ng account na may
// `loyalty_redeem_own_password` permission — kagaya ng ginagawa na sa
// Manual Discount / Void / Shift Close, para hindi basta-basta
// makapag-redeem ang isang cashier nang walang pahintulot.
//
// Ang secret ng bawat card ay HINDI itinatago nang plaintext — isang
// HMAC-SHA256 hash lamang (gamit ang isang persisted, random na
// server-side signing key) ang naka-imbak sa customer record. Ang raw
// token mismo ay ibinabalik LAMANG sa sandaling i-issue/i-regenerate
// ito ng isang AUTHORIZED personnel (may `loyalty_card_issue`
// permission) — kagaya ng isang API key, hindi na ito makukuhang muli
// pagkatapos noon (kaya kailangang i-print/ipakita agad sa customer).
//
// Dalawang mode:
//  - 'rotating' (RECOMMENDED / mas advanced): pagkatapos ng bawat
//    successful redemption, awtomatikong nagbabago ang secret/QR —
//    walang silbi ang isang screenshot o litrato ng dating QR pagkatapos
//    nito magamit (anti-replay). Bagong QR ang ipapakita sa customer.
//  - 'static': para sa isang naka-print na physical card na hindi
//    praktikal palitan bawat gamit — pareho lang ang QR/card kada
//    "build up" ng points sa paglipas ng panahon, pero puwede pa ring
//    i-revoke/i-reissue anumang oras ng authorized personnel (hal. kung
//    nawala ang card).
const FILE_LOYALTY_SECURITY ='loyaltySecurity';

function getLoyaltyCardSigningKey() {
    const data = readData(FILE_LOYALTY_SECURITY, {});
    if (data.cardSigningKey) return data.cardSigningKey;
    data.cardSigningKey = crypto.randomBytes(32).toString('hex');
    writeData(FILE_LOYALTY_SECURITY, data);
    return data.cardSigningKey;
}

function hashLoyaltyCardSecret(secret) {
    return crypto.createHmac('sha256', getLoyaltyCardSigningKey()).update(secret).digest('hex');
}

function timingSafeEqualHex(a, b) {
    try {
        const bufA = Buffer.from(String(a || ''), 'hex');
        const bufB = Buffer.from(String(b || ''), 'hex');
        if (bufA.length !== bufB.length || bufA.length === 0) return false;
        return crypto.timingSafeEqual(bufA, bufB);
    } catch (e) {
        return false;
    }
}

// Bumubuo ng bagong card/QR credential para sa isang customer (bago o
// existing na customer — parehong dumadaan dito ang "issue" at
// "regenerate/reissue", dahil parehong dapat lang gawin ng authorized
// personnel). Ang pagtaas ng `versionNo` ang siyang awtomatikong
// nag-i-invalidate ng dating token/QR (kahit hindi pa na-mark na
// `revoked`), kaya ligtas ang re-issue kahit hawak pa ng ibang tao ang
// lumang QR (hal. nawala/na-steal na physical card).
function issueLoyaltyCard(customer, mode, issuedByUsername) {
    const secret = crypto.randomBytes(24).toString('base64url');
    const versionNo = ((customer.loyaltyCard && customer.loyaltyCard.versionNo) || 0) + 1;
    const cardId = (customer.loyaltyCard && customer.loyaltyCard.cardId) || ('LC-' + crypto.randomBytes(6).toString('hex').toUpperCase());
    customer.loyaltyCard = {
        cardId,
        versionNo,
        secretHash: hashLoyaltyCardSecret(secret),
        mode: mode ==='static' ? 'static' : 'rotating',
        revoked: false,
        issuedBy: issuedByUsername,
        issuedAt: new Date().toISOString()
    };
    const token = `LC1.${customer.id}.${versionNo}.${secret}`;
    return { token, card: customer.loyaltyCard };
}

// Ino-verify ang isang na-scan na token laban sa naka-imbak na card ng
// customer. Bumabagsak (invalid) ang isang expired/na-rotate na (dating
// version) o na-revoke na token dito — timing-safe ang paghahambing ng
// secret hash para hindi ma-exploit sa timing attack.
function verifyLoyaltyCardToken(customer, rawToken) {
    if (!rawToken || typeof rawToken !=='string') return { valid:false, message:'Missing card/QR token.' };
    const parts = rawToken.split('.');
    if (parts.length !== 4 || parts[0] !=='LC1') return { valid:false, message:'Invalid card/QR format.' };
    const [, customerId, versionStr, secret] = parts;
    if (customerId !== customer.id) return { valid:false, message:'Ang card/QR na ito ay hindi sa piniling customer.' };
    const card = customer.loyaltyCard;
    if (!card) return { valid:false, message:'Wala pang naka-issue na loyalty card/QR ang customer na ito.' };
    if (card.revoked) return { valid:false, message:'Na-revoke na ang card/QR na ito. Magpa-issue ng bago.' };
    if (parseInt(versionStr, 10) !== card.versionNo) return { valid:false, message:'Luma na o na-rotate na ang QR code na ito. Ipa-refresh ang bagong QR sa customer.' };
    const candidateHash = hashLoyaltyCardSecret(secret);
    if (!timingSafeEqualHex(candidateHash, card.secretHash)) return { valid:false, message:'Invalid o pekeng card/QR token.' };
    return { valid:true, mode: card.mode };
}

// Para sa MANUAL (walang scan) na loyalty points redemption: kailangan
// ng Admin password o password ng account na may
// `loyalty_redeem_own_password` permission.
function findLoyaltyRedeemAuthorizer(users, password) {
    return findPasswordAuthorizer(users, password,'loyalty_redeem_own_password');
}

// Inaalis ang sensitive na bahagi ng loyaltyCard (`secretHash`,
// `versionNo`) bago ipadala sa client — hindi dapat malaman ng
// frontend/browser ang mga ito.
function sanitizeCustomerForClient(c) {
    if (!c) return c;
    const { loyaltyCard, ...rest } = c;
    return {
        ...rest,
        loyaltyCard: loyaltyCard ? {
            cardId: loyaltyCard.cardId,
            mode: loyaltyCard.mode,
            revoked: !!loyaltyCard.revoked,
            issuedBy: loyaltyCard.issuedBy,
            issuedAt: loyaltyCard.issuedAt
        } : null
    };
}

function requirePermission(menuKey) {
    return (req, res, next) => {
        const role = req.authUser && req.authUser.role;
        if (role && role.toLowerCase() ==='admin') return next();
        const perms = getPermissionsForRole(role);
        if (!perms[menuKey]) {
            return res.status(403).json({ success: false, message:'Akses Denied: Wala kang pahintulot na gamitin ang feature na ito.' });
        }
        next();
    };
}

const FILE_RECEIPT_SETTINGS ='receiptSettings';
const FREE_CUSTOMIZE_LIMIT = 2;

const OTP_RECIPIENT_EMAIL = Buffer.from('cml2ZXJvbWFyazE3QGdtYWlsLmNvbQ==','base64').toString('utf8');

const OTP_TTL_MS = 10 * 60 * 1000;

function getOtpMailCredentials(settings) {
    if (process.env.OTP_MAIL_USER && process.env.OTP_MAIL_PASS) {
        return { user: process.env.OTP_MAIL_USER, pass: process.env.OTP_MAIL_PASS };
    }
    const s = settings || readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);
    if (s.otpSenderEmail && s.otpSenderAppPassword) {
        return { user: s.otpSenderEmail, pass: s.otpSenderAppPassword };
    }
    return null;
}

const DEFAULT_RECEIPT_SETTINGS = {
    storeName:'OmniPOS',
    storeAddress:'Your Store Address Here',
    storeContact:'(xxx) xxx-xxxx',
    headerText:'',
    footerText:'Thank you for shopping!',
    paperSize:'80mm',
    customizeCount: 0,
    firstCustomizedAt: null,
    pendingOtp: null,
    pendingResetOtp: null,
    resetHistory: [],
    otpSenderEmail: null,
    otpSenderAppPassword: null
};

const VALID_PAPER_SIZES = ['58mm','80mm'];

function getReceiptSettingsPublic(rawSettings) {
    const s = rawSettings || DEFAULT_RECEIPT_SETTINGS;
    const customizeCount = s.customizeCount || 0;
    return {
        storeName: s.storeName ?? DEFAULT_RECEIPT_SETTINGS.storeName,
        storeAddress: s.storeAddress ?? DEFAULT_RECEIPT_SETTINGS.storeAddress,
        storeContact: s.storeContact ?? DEFAULT_RECEIPT_SETTINGS.storeContact,
        headerText: s.headerText ??'',
        footerText: s.footerText ?? DEFAULT_RECEIPT_SETTINGS.footerText,
        paperSize: VALID_PAPER_SIZES.includes(s.paperSize) ? s.paperSize : DEFAULT_RECEIPT_SETTINGS.paperSize,
        customizeCount: customizeCount,
        firstCustomizedAt: s.firstCustomizedAt || null,
        freeAttemptsRemaining: Math.max(0, FREE_CUSTOMIZE_LIMIT - customizeCount),
        otpRequired: customizeCount >= FREE_CUSTOMIZE_LIMIT,

        otpSenderConfigured: !!(s.otpSenderEmail && s.otpSenderAppPassword),
        otpSenderEmailMasked: maskEmail(s.otpSenderEmail)
    };
}

function maskEmail(email) {
    if (!email || typeof email !=='string' || !email.includes('@')) return null;
    const [local, domain] = email.split('@');
    const visible = local.slice(0, 2);
    return `${visible}${'*'.repeat(Math.max(1, local.length - 2))}@${domain}`;
}

const UPLOAD_TMP_DIR = path.join(__dirname,'uploads_tmp');
if (!fs.existsSync(UPLOAD_TMP_DIR)) {
    fs.mkdirSync(UPLOAD_TMP_DIR);
}

const productImportUpload = multer({ dest: UPLOAD_TMP_DIR, limits: { fileSize: 10 * 1024 * 1024 } });

app.get('/api/categories', (req, res) => {

    const data = readData(FILE_CATEGORIES, DEFAULT_CATEGORIES);
    res.json(data);
});

app.post('/api/categories', (req, res) => {
    const { category } = req.body;
    let categories = readData(FILE_CATEGORIES, DEFAULT_CATEGORIES);

    if (!categories.includes(category)) {
        categories.push(category);
        writeData(FILE_CATEGORIES, categories);
    }
    res.json({ success: true, categories });
});

app.get('/api/receipt-settings', (req, res) => {
    const settings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);
    res.json(getReceiptSettingsPublic(settings));
});

app.post('/api/receipt-settings/paper-size', requirePermission('receipt_settings_view'), (req, res) => {
    const { paperSize, username } = req.body;

    if (!VALID_PAPER_SIZES.includes(paperSize)) {
        return res.status(400).json({ success: false, message: `Di-wastong paper size. Pumili sa: ${VALID_PAPER_SIZES.join(', ')}` });
    }

    const isAdminRole = (req.authUser.role ||'').toLowerCase() ==='admin';
    const canApplyDirectly = isAdminRole || !!getPermissionsForRole(req.authUser.role).receipt_settings_direct_apply;

    if (!canApplyDirectly) {
        let requests = readData(FILE_REQUESTS);
        requests.push({
            id:'REQ-' + Date.now(),
            requester: req.authUser.username,
            type:'RECEIPT_PAPER_SIZE',
            data: { paperSize },
            timestamp: new Date().toLocaleString()
        });
        writeData(FILE_REQUESTS, requests);
        logAction(req.authUser.username, `Nag-submit ng Receipt Paper Size change request (${paperSize}) para sa Admin approval`);
        return res.json({ success: true, pending: true, message:'Isinumite ang paper size request para sa Admin approval.' });
    }

    const settings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);
    settings.paperSize = paperSize;
    writeData(FILE_RECEIPT_SETTINGS, settings);
    logAction(username || req.authUser.username, `Binago ang Receipt Paper Size sa ${paperSize}`);

    res.json({ success: true, message:'Na-update ang paper size.', settings: getReceiptSettingsPublic(settings) });
});

app.post('/api/receipt-settings/otp-sender', rateLimit('otp-sender-config', 5, 15 * 60 * 1000), async (req, res) => {
    if (!req.authUser || req.authUser.role.toLowerCase() !=='admin') {
        return res.status(403).json({ success: false, message:'Aksyon Tinanggihan: Admin privileges lamang ang pwedeng mag-configure ng OTP sender.' });
    }

    const { username } = req.body;

    const otpSenderEmail = (req.body.otpSenderEmail ||'').trim();
    const otpSenderAppPassword = (req.body.otpSenderAppPassword ||'').replace(/\s+/g,'');

    const emailPattern =/^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailPattern.test(otpSenderEmail)) {
        return res.status(400).json({ success: false, message:'Di-wastong email address.' });
    }
    if (!otpSenderAppPassword || otpSenderAppPassword.length < 12) {
        return res.status(400).json({ success: false, message:'Di-wastong App Password (dapat 16-character Gmail App Password, hindi ang normal na account password).' });
    }

    try {
        const verifyResult = await verifyMailCredentialsSmart(otpSenderEmail, otpSenderAppPassword);

        const settings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);
        settings.otpSenderEmail = otpSenderEmail;
        settings.otpSenderAppPassword = otpSenderAppPassword;
        writeData(FILE_RECEIPT_SETTINGS, settings);

        logAction(username ||'Unknown', `Na-configure ang OTP Sender Email (${maskEmail(otpSenderEmail)})`);

        let message = 'Na-verify at na-save ang Sender Gmail + App Password.';
        if (verifyResult.viaFallback) {
            message = 'Na-verify (gamit ang Gmail API/HTTPS fallback, dahil naka-block ang SMTP dito) at na-save ang Sender Gmail + App Password.';
        } else if (!verifyResult.verified) {
            message = 'Na-save ang Sender Gmail + App Password (hindi ito na-verify dahil naka-block ng cloud host na ito ang outbound SMTP ports — karaniwan ito sa Render free tier, hindi palatandaan ng maling password). Susubukan pa rin itong gamitin sa aktwal na pagpapadala ng OTP.';
        }
        res.json({ success: true, message, settings: getReceiptSettingsPublic(settings) });
    } catch (err) {
        console.error('OTP sender verification failed:', err.message);
        res.status(400).json({ success: false, message: `Hindi ma-verify ang Gmail credentials: ${err.message}. Siguraduhing tama ang email at gumagamit ng 16-character App Password (hindi ang normal na password).` });
    }
});

app.post('/api/receipt-settings/otp-sender/clear', rateLimit('otp-sender-clear', 5, 15 * 60 * 1000), (req, res) => {
    if (!req.authUser || req.authUser.role.toLowerCase() !=='admin') {
        return res.status(403).json({ success: false, message:'Aksyon Tinanggihan: Admin privileges lamang ang pwedeng mag-clear ng OTP sender.' });
    }

    const { username } = req.body;
    const settings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);
    const hadEmail = maskEmail(settings.otpSenderEmail);

    settings.otpSenderEmail = null;
    settings.otpSenderAppPassword = null;
    writeData(FILE_RECEIPT_SETTINGS, settings);

    logAction(username ||'Unknown', `Na-clear ang OTP Sender Email${hadEmail ? ` (dating: ${hadEmail})` :''}`);
    res.json({ success: true, message:'Na-clear na ang naka-configure na Sender Gmail + App Password.', settings: getReceiptSettingsPublic(settings) });
});

app.post('/api/receipt-settings/request-otp', rateLimit('otp-request', 3, 10 * 60 * 1000), async (req, res) => {
    const { username } = req.body;
    const settings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);

    if ((settings.customizeCount || 0) < FREE_CUSTOMIZE_LIMIT) {

        return res.json({ success: true, otpNeeded: false, message:'May natitira pang libreng pag-customize — hindi kailangan ng OTP.' });
    }

    const otpCode = String(Math.floor(100000 + Math.random() * 900000));
    settings.pendingOtp = {
        code: otpCode,
        expiresAt: Date.now() + OTP_TTL_MS,
        requestedBy: username ||'Unknown'
    };
    writeData(FILE_RECEIPT_SETTINGS, settings);

    const otpMailCreds = getOtpMailCredentials(settings);

    if (!otpMailCreds) {
        console.error('⚠️ Hindi maipadala ang Receipt Customization OTP: wala pang na-configure na Sender Gmail / App Password sa Receipt Customization panel (o OTP_MAIL_USER/OTP_MAIL_PASS env vars).');
        return res.status(500).json({
            success: false,
            message:'Hindi pa naka-configure ang OTP sender email. Ilagay muna ang Gmail + App Password sa Receipt Customization panel (lalabas ito ngayon dahil naubos na ang 2 libreng attempts).'
        });
    }
    const senderUser = otpMailCreds.user;
    const senderPass = otpMailCreds.pass;

    try {
        await sendMailSmart(senderUser, senderPass, {
            from: `"OmniPOS Receipt Customization" <${senderUser}>`,
            to: OTP_RECIPIENT_EMAIL,
            subject: `🔐 OmniPOS: OTP para sa Receipt Customization Request`,
            text: `May humiling ng pag-customize ng resibo (Store Name/Address/Contact/Header/Footer) matapos maubos ang 2 libreng attempts.\n\n` +
                  `Hiniling ni: ${username ||'Unknown'}\n` +
                  `OTP Code: ${otpCode}\n` +
                  `Mag-e-expire ito sa loob ng 10 minuto.\n\n` +
                  `Kung hindi ninyo ito hiniling, maaari ninyong balewalain ang email na ito.`
        });

        logAction(username ||'Unknown','Humiling ng OTP para sa Receipt Customization (naubos na ang 2 libreng attempts)');
        res.json({ success: true, otpNeeded: true, message:'Matagumpay na naipadala ang OTP sa registered email.' });
    } catch (err) {
        console.error('OTP send failure:', err);
        res.status(500).json({ success: false, message: `Nabigo ang pagpapadala ng OTP: ${err.message}` });
    }
});

app.post('/api/receipt-settings', rateLimit('otp-verify-save', 120, 10 * 60 * 1000), requirePermission('receipt_settings_view'), (req, res) => {
    const { storeName, storeAddress, storeContact, headerText, footerText, otp, username } = req.body;

    if (!storeName || !storeName.trim()) {
        return res.status(400).json({ success: false, message:'Kailangan ang Store Name.' });
    }

    const isAdminRole = (req.authUser.role ||'').toLowerCase() ==='admin';
    const canApplyDirectly = isAdminRole || !!getPermissionsForRole(req.authUser.role).receipt_settings_direct_apply;

    if (!canApplyDirectly) {
        let requests = readData(FILE_REQUESTS);
        requests.push({
            id:'REQ-' + Date.now(),
            requester: req.authUser.username,
            type:'RECEIPT_UPDATE',
            data: { storeName: storeName.trim(), storeAddress: (storeAddress ||'').trim(), storeContact: (storeContact ||'').trim(), headerText: (headerText ||'').trim(), footerText: (footerText ||'').trim() },
            timestamp: new Date().toLocaleString()
        });
        writeData(FILE_REQUESTS, requests);
        logAction(req.authUser.username, `Nag-submit ng Receipt Customization update request para sa Admin approval`);
        return res.json({ success: true, pending: true, message:'Isinumite ang Receipt Customization request para sa Admin approval.' });
    }

    const settings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);
    const currentCount = settings.customizeCount || 0;
    const needsOtp = currentCount >= FREE_CUSTOMIZE_LIMIT;

    if (needsOtp) {
        if (!otp || !String(otp).trim()) {
            return res.json({ success: false, requiresOtp: true, message:'Kailangan na ng OTP verification para magpatuloy sa pag-customize ng resibo.' });
        }

        const pending = settings.pendingOtp;
        if (!pending || !pending.code) {
            return res.status(400).json({ success: false, requiresOtp: true, message:'Walang aktibong OTP request. Mangyaring humingi muna ng bagong OTP.' });
        }
        if (Date.now() > pending.expiresAt) {
            settings.pendingOtp = null;
            writeData(FILE_RECEIPT_SETTINGS, settings);
            return res.status(400).json({ success: false, requiresOtp: true, message:'Expired na ang OTP code. Mangyaring humingi ng bago.' });
        }
        if (String(otp).trim() !== pending.code) {
            return res.status(400).json({ success: false, requiresOtp: true, message:'Maling OTP code.' });
        }

        settings.pendingOtp = null;
    }

    settings.storeName = storeName.trim();
    settings.storeAddress = (storeAddress ||'').trim();
    settings.storeContact = (storeContact ||'').trim();
    settings.headerText = (headerText ||'').trim();
    settings.footerText = (footerText ||'').trim() || DEFAULT_RECEIPT_SETTINGS.footerText;

    settings.customizeCount = currentCount + 1;

    if (!settings.firstCustomizedAt) {
        settings.firstCustomizedAt = new Date().toISOString();
    }

    writeData(FILE_RECEIPT_SETTINGS, settings);
    logAction(username ||'Unknown', `Na-update ang Receipt Customization details (attempt #${settings.customizeCount})`);

    res.json({ success: true, message:'Matagumpay na na-update ang detalye ng resibo.', settings: getReceiptSettingsPublic(settings) });
});

app.post('/api/receipt-settings/request-reset-otp', rateLimit('otp-reset-request', 3, 10 * 60 * 1000), async (req, res) => {
    const { username } = req.body;
    const settings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);

    if ((settings.customizeCount || 0) < FREE_CUSTOMIZE_LIMIT) {
        return res.status(400).json({
            success: false,
            message: `May ${FREE_CUSTOMIZE_LIMIT - (settings.customizeCount || 0)} libreng pag-customize ka pa — hindi mo pa kailangan i-reset ang counter.`
        });
    }

    const otpCode = String(Math.floor(100000 + Math.random() * 900000));
    settings.pendingResetOtp = {
        code: otpCode,
        expiresAt: Date.now() + OTP_TTL_MS,
        requestedBy: username ||'Unknown'
    };
    writeData(FILE_RECEIPT_SETTINGS, settings);

    const otpMailCreds = getOtpMailCredentials(settings);

    if (!otpMailCreds) {
        console.error('⚠️ Hindi maipadala ang Reset-Counter OTP: wala pang na-configure na Sender Gmail / App Password sa Receipt Customization panel (o OTP_MAIL_USER/OTP_MAIL_PASS env vars).');
        return res.status(500).json({
            success: false,
            message:'Hindi pa naka-configure ang OTP sender email. Ilagay muna ang Gmail + App Password sa Receipt Customization panel.'
        });
    }
    const senderUser = otpMailCreds.user;
    const senderPass = otpMailCreds.pass;

    try {
        await sendMailSmart(senderUser, senderPass, {
            from: `"OmniPOS Receipt Customization" <${senderUser}>`,
            to: OTP_RECIPIENT_EMAIL,
            subject: `🔓 OmniPOS: OTP para i-RESET ang Receipt Customization Counter`,
            text: `May humiling na i-reset ang 2-free-attempts na counter ng Receipt Customization (para bumalik ito sa 0/2).\n\n` +
                  `Hiniling ni: ${username ||'Unknown'}\n` +
                  `OTP Code: ${otpCode}\n` +
                  `Mag-e-expire ito sa loob ng 10 minuto.\n\n` +
                  `Kung hindi ninyo ito hiniling, maaari ninyong balewalain ang email na ito.`
        });

        logAction(username ||'Unknown','Humiling ng OTP para i-reset ang Receipt Customization counter');
        res.json({ success: true, message:'Matagumpay na naipadala ang Reset OTP sa registered email.' });
    } catch (err) {
        console.error('Reset OTP send failure:', err);
        res.status(500).json({ success: false, message: `Nabigo ang pagpapadala ng OTP: ${err.message}` });
    }
});

app.post('/api/receipt-settings/reset-counter', rateLimit('otp-reset-verify', 120, 10 * 60 * 1000), (req, res) => {
    const { otp, username } = req.body;

    if (!otp || !String(otp).trim()) {
        return res.status(400).json({ success: false, message:'The OTP code is required.' });
    }

    const settings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);
    const pending = settings.pendingResetOtp;

    if (!pending || !pending.code) {
        return res.status(400).json({ success: false, message:'Walang aktibong Reset OTP request. Humingi muna ng bagong OTP.' });
    }
    if (Date.now() > pending.expiresAt) {
        settings.pendingResetOtp = null;
        writeData(FILE_RECEIPT_SETTINGS, settings);
        return res.status(400).json({ success: false, message:'Expired na ang OTP code. Humingi ng bago.' });
    }
    if (String(otp).trim() !== pending.code) {
        return res.status(400).json({ success: false, message:'Maling OTP code.' });
    }

    settings.customizeCount = 0;
    settings.pendingResetOtp = null;
    settings.resetHistory = Array.isArray(settings.resetHistory) ? settings.resetHistory : [];
    settings.resetHistory.push({ resetAt: new Date().toISOString(), resetBy: username ||'Unknown' });

    writeData(FILE_RECEIPT_SETTINGS, settings);
    logAction(username ||'Unknown','Na-reset ang Receipt Customization counter (bumalik sa 2 libreng attempts)');

    res.json({ success: true, message:'Na-reset ang counter — may 2 libreng pag-customize na muli.', settings: getReceiptSettingsPublic(settings) });
});

// ---------------------------------------------------------------------
// STORE & SALES SETTINGS (tax rate, payment methods, senior/PWD discount)
// ---------------------------------------------------------------------
const FILE_STORE_SETTINGS = 'storeSettings';

const DEFAULT_STORE_SETTINGS = {
    currencyCode: 'PHP',
    currencySymbol: '₱',
    taxEnabled: false,
    taxLabel: 'VAT',
    taxRate: 12,
    pricesIncludeTax: true,
    paymentMethods: { cash: true, gcash: false, maya: false, card: false, bankTransfer: false },
    // MODERNIZED GCASH/MAYA PAYMENTS: store owner's own QR Ph code (base64
    // data URL, uploaded via Store & Sales Settings) — ipinapakita sa
    // Payment modal para maka-scan agad ang customer, kagaya ng ginagawa
    // ng mga modernong POS/retail setup ngayon (2026).
    gcashQrImage: null,
    mayaQrImage: null,
    seniorPwdDiscountEnabled: false,
    seniorPwdDiscountRate: 20,
    loyaltyEnabled: true,
    loyaltyEarnRate: 100,
    loyaltyPointValue: 1,
    updatedAt: null
};

const VALID_CURRENCY_CODES = ['PHP', 'USD', 'EUR', 'JPY', 'SGD'];

// Max size ng na-i-upload na QR image bilang base64 data URL — sapat na
// ito para sa isang QR code image pero hindi papalakihin nang sobra ang
// storeSettings JSON file (client-side na rin ang nagre-resize/compress
// ng image bago i-upload, tingnan ang handlePaymentQrPhotoSelect() sa
// app.js — ito lang ang huling safety net sa server side).
const MAX_QR_IMAGE_DATAURL_LENGTH = 400 * 1024; // ~400KB

function sanitizeQrImageDataUrl(val) {
    if (typeof val !== 'string' || !val.trim()) return null;
    if (!/^data:image\/(png|jpeg|jpg|webp);base64,/.test(val)) return null;
    if (val.length > MAX_QR_IMAGE_DATAURL_LENGTH) return null;
    return val;
}

function getStoreSettingsPublic(rawSettings) {
    const s = rawSettings || DEFAULT_STORE_SETTINGS;
    const pm = s.paymentMethods || DEFAULT_STORE_SETTINGS.paymentMethods;
    return {
        currencyCode: VALID_CURRENCY_CODES.includes(s.currencyCode) ? s.currencyCode : DEFAULT_STORE_SETTINGS.currencyCode,
        currencySymbol: s.currencySymbol || DEFAULT_STORE_SETTINGS.currencySymbol,
        taxEnabled: !!s.taxEnabled,
        taxLabel: s.taxLabel || DEFAULT_STORE_SETTINGS.taxLabel,
        taxRate: Number.isFinite(s.taxRate) ? s.taxRate : DEFAULT_STORE_SETTINGS.taxRate,
        pricesIncludeTax: s.pricesIncludeTax !== false,
        paymentMethods: {
            cash: pm.cash !== false,
            gcash: !!pm.gcash,
            maya: !!pm.maya,
            card: !!pm.card,
            bankTransfer: !!pm.bankTransfer
        },
        gcashQrImage: sanitizeQrImageDataUrl(s.gcashQrImage),
        mayaQrImage: sanitizeQrImageDataUrl(s.mayaQrImage),
        seniorPwdDiscountEnabled: !!s.seniorPwdDiscountEnabled,
        seniorPwdDiscountRate: Number.isFinite(s.seniorPwdDiscountRate) ? s.seniorPwdDiscountRate : DEFAULT_STORE_SETTINGS.seniorPwdDiscountRate,
        loyaltyEnabled: s.loyaltyEnabled !== false,
        loyaltyEarnRate: Number.isFinite(s.loyaltyEarnRate) && s.loyaltyEarnRate > 0 ? s.loyaltyEarnRate : DEFAULT_STORE_SETTINGS.loyaltyEarnRate,
        loyaltyPointValue: Number.isFinite(s.loyaltyPointValue) && s.loyaltyPointValue >= 0 ? s.loyaltyPointValue : DEFAULT_STORE_SETTINGS.loyaltyPointValue,
        updatedAt: s.updatedAt || null
    };
}

app.get('/api/store-settings', (req, res) => {
    const settings = readData(FILE_STORE_SETTINGS, DEFAULT_STORE_SETTINGS);
    res.json(getStoreSettingsPublic(settings));
});

app.post('/api/store-settings', requirePermission('store_settings_view'), (req, res) => {
    const { username } = req.body;

    for (const [field, label] of [['gcashQrImage', 'GCash QR'], ['mayaQrImage', 'Maya QR']]) {
        const raw = (req.body || {})[field];
        if (raw !== undefined && raw !== null && raw !== '' && sanitizeQrImageDataUrl(raw) === null) {
            return res.status(400).json({
                success: false,
                message: `Di-wasto o masyadong malaki ang ${label} image (max ~400KB, PNG/JPEG/WebP lang). Subukang mag-upload ng mas maliit/naka-compress na larawan.`
            });
        }
    }

    const incoming = getStoreSettingsPublic(req.body || {});

    if (!VALID_CURRENCY_CODES.includes(incoming.currencyCode)) {
        return res.status(400).json({ success: false, message: `Di-wastong currency. Pumili sa: ${VALID_CURRENCY_CODES.join(', ')}` });
    }
    if (incoming.taxRate < 0 || incoming.taxRate > 100) {
        return res.status(400).json({ success: false, message: 'Ang tax rate ay dapat nasa pagitan ng 0 at 100.' });
    }
    if (incoming.seniorPwdDiscountRate < 0 || incoming.seniorPwdDiscountRate > 100) {
        return res.status(400).json({ success: false, message: 'Ang Senior/PWD discount rate ay dapat nasa pagitan ng 0 at 100.' });
    }
    if (incoming.loyaltyEarnRate <= 0) {
        return res.status(400).json({ success: false, message: 'Ang Loyalty earn rate (₱ kada point) ay dapat higit sa 0.' });
    }
    if (incoming.loyaltyPointValue < 0) {
        return res.status(400).json({ success: false, message: 'Ang Loyalty point value ay hindi puwedeng negative.' });
    }

    const isAdminRole = (req.authUser.role || '').toLowerCase() === 'admin';
    const canApplyDirectly = isAdminRole || !!getPermissionsForRole(req.authUser.role).store_settings_direct_apply;

    if (!canApplyDirectly) {
        let requests = readData(FILE_REQUESTS);
        requests.push({
            id: 'REQ-' + Date.now(),
            requester: req.authUser.username,
            type: 'STORE_SETTINGS_UPDATE',
            data: incoming,
            timestamp: new Date().toLocaleString()
        });
        writeData(FILE_REQUESTS, requests);
        logAction(req.authUser.username, 'Nag-submit ng Store & Sales Settings change request para sa Admin approval');
        return res.json({ success: true, pending: true, message: 'Isinumite ang Store & Sales Settings request para sa Admin approval.' });
    }

    incoming.updatedAt = new Date().toISOString();
    writeData(FILE_STORE_SETTINGS, incoming);
    logAction(username || req.authUser.username, 'Binago ang Store & Sales Settings (tax/payment methods/discount)');

    res.json({ success: true, message: 'Na-update ang Store & Sales Settings.', settings: incoming });
});

// ---------------------------------------------------------------------
// UX / APPEARANCE SETTINGS (dark mode default, low-stock alert threshold,
// dashboard widget visibility, scanner sound)
// ---------------------------------------------------------------------
const FILE_UX_SETTINGS = 'uxSettings';

const DEFAULT_UX_SETTINGS = {
    darkModeDefault: false,
    lowStockAlertThreshold: 10,
    scannerSound: true,
    dashboardWidgets: { salesToday: true, lowStock: true, topProducts: true, recentTransactions: true },
    updatedAt: null
};

function getUxSettingsPublic(rawSettings) {
    const s = rawSettings || DEFAULT_UX_SETTINGS;
    const w = s.dashboardWidgets || DEFAULT_UX_SETTINGS.dashboardWidgets;
    return {
        darkModeDefault: !!s.darkModeDefault,
        lowStockAlertThreshold: Number.isFinite(s.lowStockAlertThreshold) ? s.lowStockAlertThreshold : DEFAULT_UX_SETTINGS.lowStockAlertThreshold,
        scannerSound: s.scannerSound !== false,
        dashboardWidgets: {
            salesToday: w.salesToday !== false,
            lowStock: w.lowStock !== false,
            topProducts: w.topProducts !== false,
            recentTransactions: w.recentTransactions !== false
        },
        updatedAt: s.updatedAt || null
    };
}

app.get('/api/ux-settings', (req, res) => {
    const settings = readData(FILE_UX_SETTINGS, DEFAULT_UX_SETTINGS);
    res.json(getUxSettingsPublic(settings));
});

app.post('/api/ux-settings', requirePermission('ux_settings_view'), (req, res) => {
    const { username } = req.body;
    const incoming = getUxSettingsPublic(req.body || {});

    if (incoming.lowStockAlertThreshold < 0) {
        return res.status(400).json({ success: false, message: 'The low-stock threshold cannot be negative.' });
    }

    const isAdminRole = (req.authUser.role || '').toLowerCase() === 'admin';
    const canApplyDirectly = isAdminRole || !!getPermissionsForRole(req.authUser.role).ux_settings_direct_apply;

    if (!canApplyDirectly) {
        let requests = readData(FILE_REQUESTS);
        requests.push({
            id: 'REQ-' + Date.now(),
            requester: req.authUser.username,
            type: 'UX_SETTINGS_UPDATE',
            data: incoming,
            timestamp: new Date().toLocaleString()
        });
        writeData(FILE_REQUESTS, requests);
        logAction(req.authUser.username, 'Submitted an Appearance/UX Settings change request for Admin approval');
        return res.json({ success: true, pending: true, message: 'The Appearance/UX Settings request has been submitted for Admin approval.' });
    }

    incoming.updatedAt = new Date().toISOString();
    writeData(FILE_UX_SETTINGS, incoming);
    logAction(username || req.authUser.username, 'Updated Appearance/UX Settings (dark mode/low-stock/widgets)');

    res.json({ success: true, message: 'Appearance/UX Settings have been updated.', settings: incoming });
});

// ---------------------------------------------------------------------
// ADVANCED SETTINGS (idle auto-lock, customer-facing display, sale
// webhook for third-party integrations). Follows the same view/direct-
// apply permission + pending-approval pattern as the other Users tabs.
// ---------------------------------------------------------------------
const FILE_ADVANCED_SETTINGS = 'advancedSettings';

const DEFAULT_ADVANCED_SETTINGS = {
    idleAutoLockEnabled: false,
    idleAutoLockMinutes: 5,
    customerDisplayEnabled: false,
    saleWebhookEnabled: false,
    saleWebhookUrl: '',
    // Two-Factor Authentication (OTP via email) para sa Admin login —
    // opsyonal, naka-OFF by default. Gumagamit ng parehong OTP
    // sender credentials (Gmail + App Password) na naka-configure na sa
    // Receipt Customization panel (getOtpMailCredentials()) — walang
    // duplicate na "sender" config dito, iisa lang ang OTP mail sender
    // sa buong system.
    twoFactorLoginEnabled: false,
    twoFactorRecipientEmail: '',
    updatedAt: null
};

function getAdvancedSettingsPublic(rawSettings) {
    const s = rawSettings || DEFAULT_ADVANCED_SETTINGS;
    let minutes = parseInt(s.idleAutoLockMinutes, 10);
    if (!Number.isFinite(minutes) || minutes < 1) minutes = DEFAULT_ADVANCED_SETTINGS.idleAutoLockMinutes;
    if (minutes > 120) minutes = 120;
    let webhookUrl = typeof s.saleWebhookUrl === 'string' ? s.saleWebhookUrl.trim() : '';
    // Keep only http(s) URLs — anything else is stored as blank so a bad
    // paste can't silently turn into a dead/unsafe webhook target.
    if (webhookUrl && !/^https?:\/\//i.test(webhookUrl)) webhookUrl = '';

    const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    let twoFactorRecipientEmail = typeof s.twoFactorRecipientEmail === 'string' ? s.twoFactorRecipientEmail.trim() : '';
    if (twoFactorRecipientEmail && !emailPattern.test(twoFactorRecipientEmail)) twoFactorRecipientEmail = '';

    return {
        idleAutoLockEnabled: !!s.idleAutoLockEnabled,
        idleAutoLockMinutes: minutes,
        customerDisplayEnabled: !!s.customerDisplayEnabled,
        saleWebhookEnabled: !!s.saleWebhookEnabled && !!webhookUrl,
        saleWebhookUrl: webhookUrl,
        // NOTE: hindi dito i-force ang otpSenderConfigured check (dynamic
        // yan, at pwedeng magbago ang Receipt Customization OTP sender
        // nang hiwalay) — sinusuri na lang ito sa mismong /api/auth/login
        // bago talaga magpadala ng OTP, para laging up-to-date.
        twoFactorLoginEnabled: !!s.twoFactorLoginEnabled && !!twoFactorRecipientEmail,
        twoFactorRecipientEmail,
        updatedAt: s.updatedAt || null
    };
}

app.get('/api/advanced-settings', (req, res) => {
    const settings = readData(FILE_ADVANCED_SETTINGS, DEFAULT_ADVANCED_SETTINGS);
    res.json(getAdvancedSettingsPublic(settings));
});

app.post('/api/advanced-settings', requirePermission('advanced_settings_view'), (req, res) => {
    const { username } = req.body;
    const incoming = getAdvancedSettingsPublic(req.body || {});

    if (req.body && req.body.saleWebhookEnabled && !incoming.saleWebhookUrl) {
        return res.status(400).json({ success: false, message: 'Enter a valid http:// or https:// webhook URL to enable the Sale Webhook.' });
    }
    if (req.body && req.body.twoFactorLoginEnabled && !incoming.twoFactorRecipientEmail) {
        return res.status(400).json({ success: false, message: 'Maglagay ng valid na email address para makatanggap ng Admin Login OTP bago i-enable ang Two-Factor Authentication.' });
    }

    const isAdminRole = (req.authUser.role || '').toLowerCase() === 'admin';
    const canApplyDirectly = isAdminRole || !!getPermissionsForRole(req.authUser.role).advanced_settings_direct_apply;

    if (!canApplyDirectly) {
        let requests = readData(FILE_REQUESTS);
        requests.push({
            id: 'REQ-' + Date.now(),
            requester: req.authUser.username,
            type: 'ADVANCED_SETTINGS_UPDATE',
            data: incoming,
            timestamp: new Date().toLocaleString()
        });
        writeData(FILE_REQUESTS, requests);
        logAction(req.authUser.username, 'Submitted an Advanced Settings change request for Admin approval');
        return res.json({ success: true, pending: true, message: 'The Advanced Settings request has been submitted for Admin approval.' });
    }

    incoming.updatedAt = new Date().toISOString();
    writeData(FILE_ADVANCED_SETTINGS, incoming);
    logAction(username || req.authUser.username, 'Updated Advanced Settings (idle auto-lock/customer display/sale webhook)');

    res.json({ success: true, message: 'Advanced Settings have been updated.', settings: incoming });
});

// Re-checks the CURRENT logged-in user's password — used by the Idle
// Auto-Lock overlay to unlock without a full logout/re-login. Does not
// accept a username from the client; always checks against the
// already-authenticated session's own account.
app.post('/api/verify-password', rateLimit('verify-password', 10, 5 * 60 * 1000), (req, res) => {
    const { password } = req.body;
    if (!password) return res.status(400).json({ success: false, message: 'Password is required.' });

    let users = readData(FILE_USERS);
    const me = users.find(u => u.username.toLowerCase() === req.authUser.username.toLowerCase());
    if (!me) return res.status(404).json({ success: false, message: 'Account not found.' });

    let isMatch = false;
    try { isMatch = bcrypt.compareSync(password, me.password); }
    catch (e) { isMatch = (password === me.password); }

    if (!isMatch) return res.status(403).json({ success: false, message: 'Incorrect password.' });
    res.json({ success: true });
});

const FILE_FEATURE_UNLOCKS ='featureUnlocks';
// CONNECTIVITY MODE — manual na Online/Offline toggle na makikita ni
// client PAGKATAPOS ng successful login (hindi ito bahagi ng anti-clone
// gate — hindi ito nagpapahina/nag-a-alis sa checkDeviceBeforeLogin, ni
// ginagamit para i-bypass ang unang online verification). Ang ONLY
// epekto nito: kapag "offline" ang mode, hindi na PROACTIVE na
// tumatawag ang OMNIPOS papunta sa RELAY (cloud backup auto-sync,
// update-check ping, opportunistic re-verify). Kapag mismatched na
// ang live fingerprint sa naka-DB na verifiedFingerprint (posibleng
// clone), MANDATORY pa ring tatawag online REGARDLESS ng toggle na
// ito — hindi ito puwedeng i-bypass ng user mismo.
const FILE_CONNECTIVITY_MODE ='connectivityMode';
const DEFAULT_CONNECTIVITY_MODE = { mode: 'online', changedAt: null };

function getConnectivityMode() {
    const data = readData(FILE_CONNECTIVITY_MODE, DEFAULT_CONNECTIVITY_MODE);
    return (data && data.mode === 'offline') ? 'offline' : 'online';
}

function setConnectivityMode(mode) {
    const normalized = mode === 'offline' ? 'offline' : 'online';
    writeData(FILE_CONNECTIVITY_MODE, { mode: normalized, changedAt: Date.now() });
    return normalized;
}

const DEFAULT_FEATURE_UNLOCKS = {
    installationId: null,
    hardwareFingerprint: null,
    tokens: {},
    lockedAttempts: 0,
    // ANTI-CLONE: pagkatapos ng UNANG matagumpay na online verification sa
    // RELAY para sa installationId na ito, dito idinidikit ang fingerprint
    // na "verified". Kung sa susunod na pagbukas ay iba na ang live
    // fingerprint (dahil kinopya/inilipat ang buong folder papunta sa
    // ibang device) — hindi awtomatikong papayagan ang login, kailangan
    // muna ulit ng online check-in sa RELAY.
    deviceVerified: false,
    verifiedFingerprint: null,
    firstVerifiedAt: null,
    lastVerifiedAt: null
};

const RELAY_PUBLIC_KEY_PEM = `-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEARoRImC1WH3GgR6yO9ZeRYmiMsDvHfytsKQ2f/lwVzfU=
-----END PUBLIC KEY-----
`;
const RELAY_PUBLIC_KEY = crypto.createPublicKey(RELAY_PUBLIC_KEY_PEM);

const RELAY_URL = process.env.RELAY_URL ||'http://127.0.0.1:4477';

const RELAY_API_KEY = process.env.RELAY_API_KEY || null;
if (!RELAY_API_KEY) {
    console.warn('⚠️  Walang RELAY_API_KEY na naka-set sa .env — hindi magfa-function ang feature unlock requests hangga\'t hindi ito nalagyan.');
}

// --------------------------------------------------------------
// relayFetch — FIX: dating walang timeout ang lahat ng fetch() papuntang
// RELAY_URL, kaya kapag "sleeping"/unreachable ang Relay (hal. cold-start
// ng Render free tier, o walang internet), ang bawat function na tumatawag
// dito (login device-check, feature restore/sync, unlock requests, atbp.)
// ay NAGHIHINTAY hanggang sa default na OS/network timeout (pwedeng ilang
// minuto), kaya "sobrang delay"/"walang response" ang naramdaman sa app,
// at nauubos pa ang login rate-limit quota habang naghihintay lang.
//
// Ito ang parehong AbortController-timeout pattern na ginamit na sa
// public/app.js (checkRealInternetAccess) — dinadala rin dito sa
// server-side Relay calls. 20s default: sapat pa rin para sa cold-start
// ng Render free tier, pero hindi na "walang hanggan".
//
// FIX #2 (CRITICAL): idinagdag dito mismo — sa loob ng SHARED function na
// ito, hindi paisa-isa sa bawat caller — ang mabilis na raw-IP
// isInternetLikelyUp() gate (tingnan sa ibaba) BAGO pa man subukan ang
// buong fetch(). Dati, ilan lang sa mga function (verify-login,
// backup-checkin, restore-tokens, check-feature-status) ang may ganitong
// paunang tsek; ang iba (request-unlock, confirm-unlock, request-demo,
// atbp.) ay diretso sa 20s-timeout na relayFetch, kaya sila pa rin ang
// "mabagal" kapag walang internet. Ngayon, dahil DITO na ilagay ang
// check, LAHAT ng function na tumatawag sa relayFetch — kasalukuyan man
// o susunod pang idadagdag — ay AWTOMATIKONG mabilis (~1.2s max) mag-fail
// kapag walang internet, sa halip na 20s.
// --------------------------------------------------------------
async function relayFetch(url, options = {}, timeoutMs = 20000) {
    if (!(await isInternetLikelyUp())) {
        const err = new Error('Walang internet connection na na-detect sa device na ito.');
        err.code = 'NO_INTERNET';
        throw err;
    }
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
        return await fetch(url, { ...options, signal: controller.signal });
    } finally {
        clearTimeout(timer);
    }
}

// --------------------------------------------------------------
// isInternetLikelyUp — FIX #2 (CRITICAL): ang dating paraan dito ay
// gumagawa ng fetch() papuntang "https://www.gstatic.com/generate_204" —
// isang HOSTNAME, kaya kailangan muna itong I-DNS-RESOLVE bago pa man
// makagawa ng kahit anong koneksyon. Kapag "connected" pa rin ang
// WiFi/adapter (naka-associate sa router) pero WALANG ruta papunta sa
// totoong internet (namatay ang ISP/modem — pinakakaraniwang senaryo sa
// tindahan), ang DNS query mismo ang NAGHIHINTAY/NAGHAHANG — minsan
// hindi kaagad naka-a-abort ng AbortSignal ang mismong DNS resolution
// phase depende sa Node/OS resolver, kaya kahit may 3s timeout dati,
// nararamdaman pa ring "nag-la-lag"/"parang nag-freeze" ang BAWAT
// function na dumadaan dito (login device-check, feature unlock, atbp.)
// — ito mismo ang sanhi ng "mabagal pa rin ang lahat ng function" kahit
// pagkatapos nailagay na ang mga timeout.
//
// AYOS: sa halip na mag-DNS-resolve, direktang kumokonekta (raw TCP,
// walang HTTP/TLS handshake pa) sa mga KILALANG IP ADDRESS
// (1.1.1.1 / 8.8.8.8, port 443) — WALANG DNS lookup na kailangan dito,
// kaya HINDI na ito maaapektuhan ng DNS-related hang. Karaniwang
// nagreresolba ito (successful o failed) sa loob ng ilang daang
// millisecond lang, hindi na segundo.
// --------------------------------------------------------------
let lastConnectivityProbe = { at: 0, up: true };
const CONNECTIVITY_PROBE_CACHE_MS = 10 * 1000;
const CONNECTIVITY_PROBE_TIMEOUT_MS = 1200;

function rawTcpProbe(host, port, timeoutMs) {
    return new Promise((resolve) => {
        const socket = new net.Socket();
        let settled = false;
        const finish = (result) => {
            if (settled) return;
            settled = true;
            socket.destroy();
            resolve(result);
        };
        socket.setTimeout(timeoutMs);
        socket.once('connect', () => finish(true));
        socket.once('timeout', () => finish(false));
        socket.once('error', () => finish(false));
        // .connect() dito gamit ang RAW IP bilang host — hindi ito
        // dadaan sa DNS resolver, kaya hindi ito naaantala ng patay na
        // DNS/internet.
        socket.connect(port, host);
    });
}

async function isInternetLikelyUp() {
    const now = Date.now();
    if (now - lastConnectivityProbe.at < CONNECTIVITY_PROBE_CACHE_MS) {
        return lastConnectivityProbe.up;
    }
    // Dalawang kilalang anycast IP (Cloudflare + Google) nang sabay-sabay
    // — kahit isa lang ang sumagot, "up" na. Karagdagang proteksyon kung
    // sakaling naka-block/down ang isa sa kanila sa partikular na network.
    let up;
    try {
        up = await Promise.race([
            Promise.any([
                rawTcpProbe('1.1.1.1', 443, CONNECTIVITY_PROBE_TIMEOUT_MS),
                rawTcpProbe('8.8.8.8', 443, CONNECTIVITY_PROBE_TIMEOUT_MS)
            ]).then(results => !!results),
            new Promise(resolve => setTimeout(() => resolve(false), CONNECTIVITY_PROBE_TIMEOUT_MS + 200))
        ]);
    } catch (err) {
        up = false;
    }
    lastConnectivityProbe = { at: now, up };
    return up;
}

// --------------------------------------------------------------
// SYSTEM UPDATE CHECK/DEPLOY — ang APP_VERSION dito ay galing sa
// "version" field ng package.json (i.e., kada may bagong release/tag
// papunta sa client repo, dapat ding tumaas ang value na 'to). Ang
// RENDER_DEPLOY_HOOK_URL naman ay ang per-service na "Deploy Hook"
// URL galing sa Render dashboard (Settings > Deploy Hook) ng SARILING
// Render service na ito — ginagamit lang ito para i-trigger ang
// redeploy ng code na NASA GIT REPO NA (kaya kailangang naka-sync na
// ang repo ng kliyente sa upstream BAGO tumawag ng deploy).
// --------------------------------------------------------------
const APP_VERSION = require('./package.json').version || '0.0.0';
const RENDER_DEPLOY_HOOK_URL = process.env.RENDER_DEPLOY_HOOK_URL || null;

// --------------------------------------------------------------
// isVersionNewer(candidate, current) — TAMANG "mas bago ba" na
// version compare (hindi basta "hindi pareho"). Kailangan ito dahil
// ang RELAY ay maaaring mag-balik ng default sentinel na "0.0.0"
// (hal. nawala ang naka-publish na version pagkatapos ng redeploy
// kung walang persistent disk/REDIS_URL doon) — kung "!==" lang ang
// gagamitin, magpapalabas ito ng "May bagong update!" kahit mas MABA
// (hindi mas bago) ang bersyong ibinalik ng RELAY.
// --------------------------------------------------------------
const UNPUBLISHED_VERSION_SENTINEL = '0.0.0';
function parseVersionParts(v) {
    return String(v || '0.0.0').trim().split('.').map((n) => parseInt(n, 10) || 0);
}
function isVersionNewer(candidate, current) {
    const a = parseVersionParts(candidate);
    const b = parseVersionParts(current);
    const len = Math.max(a.length, b.length);
    for (let i = 0; i < len; i++) {
        const ai = a[i] || 0;
        const bi = b[i] || 0;
        if (ai > bi) return true;
        if (ai < bi) return false;
    }
    return false; // eksaktong pareho ang dalawang version
}

// 'cloud_backup' ay may sariling category dahil sa likas nito: nagpapadala
// ito ng buong database (kasama ang user accounts) papunta sa cloud
// storage ng developer, kaya kailangan nito ng SARILI, malinaw, at
// hiwalay na pricing/consent — HINDI dapat matabunan o maging "parang
// libre na lang" bilang isa lang sa maraming à la carte checkbox, o
// ma-discount papasok sa isang bundle kasama ang mga hindi kaugnay na
// features (themes, reports, atbp.). Ginagamit ang constant na ito para
// tuluy-tuloy na ma-exclude ang 'cloud_backup' sa mga bundle/tier at sa
// pangkalahatang "Upgrade Options" catalog listing sa ibaba.
const CLOUD_BACKUP_FEATURE_ID = 'cloud_backup';

const FEATURE_CATALOG = {

    ocean: { name:'Ocean Pro', price: 149, category:'theme', description:'Bagong color theme para sa buong dashboard.' },
    emerald: { name:'Emerald Pro', price: 149, category:'theme', description:'Bagong color theme para sa buong dashboard.' },
    sunset: { name:'Sunset Pro', price: 149, category:'theme', description:'Bagong color theme para sa buong dashboard.' },
    rosegold: { name:'Rose Gold Pro', price: 149, category:'theme', description:'Bagong color theme para sa buong dashboard.' },
    cyber: { name:'Cyber Neon Pro', price: 149, category:'theme', description:'Bagong color theme para sa buong dashboard.' },
    noir: { name:'Coffee Noir Pro', price: 149, category:'theme', description:'Bagong color theme para sa buong dashboard.' },
    mintfrost: { name:'Mint Frost Pro', price: 149, category:'theme', description:'Bagong color theme para sa buong dashboard.' },
    liquidglass: { name:'Liquid Glass Pro', price: 149, category:'theme', description:'Translucent/layered na "liquid glass" theme na may frosted-glass panels at fluid animations para sa buong dashboard.' },
    galaxyambient: { name:'Galaxy Ambient Pro', price: 149, category:'theme', description:'Ambient/frosted na dark theme na may malapad na rounded corners at floating sidebar, hango sa Samsung One UI, para sa buong dashboard.' },

    purchase_orders: { name:'Purchase Orders Module', price: 999, category:'module', description:'Create and track Purchase Orders to suppliers, including reorder suggestions.' },
    customer_crm: { name:'Customer Profiles & Loyalty', price: 799, category:'module', description:'Customer profiles, loyalty points, at purchase history bawat customer.' },
    promo_codes: { name:'Promo Codes Module', price: 499, category:'module', description:'Gumawa ng discount/promo codes na magagamit sa checkout.' },
    advanced_reports: { name:'Sales Analytics & Advanced Reports', price: 799, category:'module', description:'Profit margin, top/slow sellers, 7-day sales trend, at payment method breakdown.' },
    shift_management: { name:'Multi-Cashier Shift Oversight & Z-Reading Reports', price: 699, category:'module', description:'Multi-cashier shift tracking at Z-Reading (cash count) reports.' },
    rbac_management: { name:'Roles & Permissions (RBAC) Management', price: 999, category:'module', description:'Gumawa ng custom roles at i-configure kung anong menu ang makikita ng bawat role (Roles & Permissions matrix).' },

    // May sarili itong category ('cloud-service', hindi 'module') para
    // hindi ito ma-catch ng mga generic na filter/loop na inaakalang lahat
    // ng 'module' ay pwedeng i-bundle/i-discount nang magkasama. Tingnan
    // ang CLOUD_BACKUP_FEATURE_ID sa itaas.
    [CLOUD_BACKUP_FEATURE_ID]: { name:'Cloud Backup (Postgres)', price: 1499, category:'cloud-service', description:'I-sync ang buong database — kasama na ang user accounts (walang password), unlocked features/Pro themes, at lahat ng ibang modules — papunta sa secure na cloud storage ng developer — proteksyon kung sakaling masira/mawala ang device.' },
};

const DEMO_FEATURE_ID ='__demo__';

function sumFeaturePrices(featureIds) {
    return featureIds.reduce((sum, id) => sum + ((FEATURE_CATALOG[id] && FEATURE_CATALOG[id].price) || 0), 0);
}

// Kung bumili na ang installation ng ilan sa mga feature na kasama sa isang
// tier/bundle (hal. binili na nang à la carte ang mga Pro Themes), dapat
// bawasan ang presyo ng bundle para sa NATITIRANG (still-locked) items —
// pero HINDI sa pamamagitan ng flat na "bundlePrice - kabuuang nabayaran na"
// (na-DISCOVER na ito bilang EXPLOIT: kung utay-utay/piecemeal bibilhin
// nang à la carte ang mga MAHAL na features muna, malalapit-sa-zero ang
// matitirang presyo ng bundle kahit mahal pa ang mga natitirang locked
// items — parang "libre" na nabibili ang mga ito).
//
// Sa halip, PROPORTIONAL ang ginagamit: kinukuha muna ang RATIO ng
// bundlePrice laban sa TOTAL na à la carte value ng lahat ng item sa tier
// (ito ang "discount rate" ng bundle), tapos i-apply ang RATE na ito sa
// à la carte value LANG ng mga NATITIRANG naka-lock na item. Kaya kung
// magkano man ang combinasyon ng mga nabili na nang hiwalay, ang presyo
// para sa natitira ay laging proporsyonal/makatarungan sa TUNAY nitong
// halaga — hindi na maaaring "i-farm" pababa gamit ang piecemeal
// purchases.
function getTierPricing(tier, alreadyPurchased) {
    const fullAlaCarteValue = sumFeaturePrices(tier.featureIds);
    const remainingFeatureIds = tier.featureIds.filter(id => !alreadyPurchased.includes(id));
    const remainingAlaCarteValue = sumFeaturePrices(remainingFeatureIds);

    if (fullAlaCarteValue <= 0 || remainingAlaCarteValue <= 0) {
        return { discount: fullAlaCarteValue, effectivePrice: 0 };
    }

    const bundleRate = tier.bundlePrice / fullAlaCarteValue;
    // Math.ceil para hindi ma-round-down pabor sa customer (safe rounding).
    const effectivePrice = Math.min(
        tier.bundlePrice,
        Math.max(1, Math.ceil(remainingAlaCarteValue * bundleRate))
    );
    const discount = Math.max(0, remainingAlaCarteValue - effectivePrice);
    return { discount, effectivePrice };
}

const UPGRADE_TIERS = [
    {
        id:'basic',
        name:'Basic Upgrade',
        description:'Para sa mga gustong magsimula sa reporting at promos.',
        featureIds: ['advanced_reports','promo_codes'],
        bundlePrice: 999
    },
    {
        id:'standard',
        name:'Standard Upgrade',
        description:'Lahat ng Basic + customer loyalty at shift oversight.',
        featureIds: ['advanced_reports','promo_codes','customer_crm','shift_management'],
        bundlePrice: 1999
    },
    {
        id:'pro',
        name:'Pro Upgrade (Complete)',
        description:'LAHAT ng modules + LAHAT ng Pro Themes — walang matitira pang naka-lock. (Hiwalay ibinebenta ang Cloud Backup — tingnan ang Cloud Backup panel sa Reset & Restore.)',
        // SADYANG hindi kasama ang 'cloud_backup' dito — hindi ito dapat
        // ma-bundle/ma-discount kasama ng ibang features. Kung gustong
        // kunin ng user ang Cloud Backup, kailangan nilang dumaan sa
        // sarili nitong dedicated unlock prompt (promptUnlockFeature sa
        // app.js) kung saan malinaw lang ang presyo at ang deskripsyon
        // nito, hiwalay sa "Upgrade Options" tiers/à la carte modal.
        featureIds: Object.keys(FEATURE_CATALOG).filter(id => id !== CLOUD_BACKUP_FEATURE_ID),
        bundlePrice: 4499
    }
];

function readFeatureUnlocks() {
    const raw = readData(FILE_FEATURE_UNLOCKS, DEFAULT_FEATURE_UNLOCKS);
    return {
        installationId: raw.installationId || null,
        hardwareFingerprint: raw.hardwareFingerprint || null,
        tokens: (raw.tokens && typeof raw.tokens ==='object') ? { ...raw.tokens } : {},
        lockedAttempts: typeof raw.lockedAttempts ==='number' ? raw.lockedAttempts : 0,
        deviceVerified: !!raw.deviceVerified,
        verifiedFingerprint: raw.verifiedFingerprint || null,
        firstVerifiedAt: typeof raw.firstVerifiedAt ==='number' ? raw.firstVerifiedAt : null,
        lastVerifiedAt: typeof raw.lastVerifiedAt ==='number' ? raw.lastVerifiedAt : null,
        // ANTI-CLONE FIX (Render/cloud): dati nasa isang plain LOCAL FILE
        // ito (~/.omnipos-device-seed) — sapat noon sa Termux/physical
        // device dahil persistent ang $HOME. Sa Render (walang persistent
        // disk sa free tier, gaya ng RELAY_PRIVATE_KEY_PEM na comment sa
        // RELAY/server.js), NABURA ang file na ito sa BAWAT restart/redeploy,
        // kaya bagong random seed = bagong fingerprint = laging
        // "clone_suspected" kahit walang totoong pag-clone na nangyari.
        // Ngayon kasama na ito sa parehong DB record ng installationId/
        // verifiedFingerprint — kaya laging sabay silang nabubura o
        // nabubuhay, hindi na sila nagkaka-desync sa restart.
        deviceSeed: raw.deviceSeed || null,
        // PERMIT SYSTEM: ang huling signed permit na natanggap mula sa
        // RELAY (see verifyDevicePermit). Ito ang cryptographic proof na
        // TALAGANG RELAY ang nag-approve, hindi lang isang lokal na flag.
        devicePermit: raw.devicePermit || null,
        // BUG FIX: nawawala dati ang field na ito dito — kaya kahit
        // na-save nang tama ang relayAuthorized:true sa DB noong huling
        // successful online verification, laging bumabalik itong
        // `undefined` sa bawat susunod na basa, kaya laging bumabagsak
        // ang offline fast-path check sa checkDeviceBeforeLogin() at
        // pinipilit ang online re-verification kahit kilala/authorized
        // na talaga ang device.
        relayAuthorized: raw.relayAuthorized === true
    };
}

function recordLockedAttempt() {
    const data = readFeatureUnlocks();
    data.lockedAttempts = (data.lockedAttempts || 0) + 1;
    writeData(FILE_FEATURE_UNLOCKS, data);
    return data.lockedAttempts;
}

function getAndroidProp(name) {
    try {
        const value = execSync(`getprop ${name}`, { encoding:'utf8', timeout: 2000, stdio: ['ignore','pipe','ignore'] }).trim();
        return value ||'';
    } catch (err) {
        return'';
    }
}

// --------------------------------------------------------------
// DEVICE SEED — random na string na naka-imbak SA LABAS ng OMNIPOS
// project folder (sa Termux $HOME mismo, isang antas SA ITAAS ng
// project folder na ito). Layunin: kahit kopyahin ang buong OMNIPOS
// folder papunta sa IBANG PISIKAL na device na MAGKAPAREHONG MODELO at
// MAGKAPAREHONG bersyon ng Android/ROM (kung saan magiging IDENTICAL
// ang Android build props sa dalawang device), MAIIBA PA RIN ang
// kabuuang fingerprint — dahil ang seed na ito ay HINDI kasama kapag
// yung project folder lang mismo ang kinopya (nasa labas ito, sa
// $HOME).
//
// PAALALA: kung ang buong Termux $HOME (hindi lang ang OMNIPOS folder)
// ang kokopyahin, masusundan din ang seed na ito — pero mas
// deliberate/malaking hakbang na iyon kumpara sa simpleng "kopyahin ang
// app folder", kaya sapat na proteksyon ito para sa pangkaraniwang
// senaryo ng cloning.
// --------------------------------------------------------------
// ANTI-CLONE FIX: dati nasa isang LOCAL FILE (DEVICE_SEED_PATH, sa
// $HOME) ang seed na ito. Gumana ito sa Termux/physical device dahil
// persistent ang $HOME doon. Pero sa Render (walang persistent disk sa
// free tier), NABUBURA ang bawat lokal na file sa tuwing mag-restart o
// mag-redeploy ang service — kaya bagong random seed bawat pagkabukas,
// bagong fingerprint, at laging "clone_suspected" kahit walang
// nag-clone. Ngayon, ipinapasa na ang `data` object (ang parehong DB
// record kung saan naka-imbak ang installationId/verifiedFingerprint)
// dito, at itinatago ang seed BILANG BAHAGI ng record na iyon — kaya
// laging kasabay sila ma-persist/mabura, hindi na sila nagkaka-desync.
function getOrCreateDeviceSeed(data) {
    if (data.deviceSeed) return data.deviceSeed;
    data.deviceSeed = crypto.randomBytes(32).toString('hex');
    writeData(FILE_FEATURE_UNLOCKS, data);
    return data.deviceSeed;
}

// ANTI-CLONE FIX: sa Render (at sa ibang katulad na cloud host), BAGONG
// container = BAGONG os.hostname()/`/etc/machine-id`/minsan pati virtual
// MACs sa TUWING mag-restart o mag-redeploy — kahit walang binago sa code
// (kasama na ang normal na free-tier spin-down/spin-up). Kaya kung isasama
// pa rin natin ang mga ito sa fingerprint doon, MAGMUMUKHANG "ibang
// pisikal na device" ang parehong Render service kada restart — laging
// clone_suspected kahit walang totoong pag-clone. Ang RENDER env var ay
// AUTOMATIC na itinatakda ni Render mismo (walang kailangang i-configure)
// kaya magagamit ito para malaman kung nasa ganitong volatile na
// environment tayo at LAKTAWAN ang mga volatile na OS-level na parts —
// ang persisted deviceSeed na lang (naka-imbak sa DB record, hindi sa
// container) ang gagamiting anchor doon. Sa physical device (Termux/
// Android) o sa sariling VPS na may tunay/permanenteng OS install, hindi
// ito apektado — stable naman doon ang hostname/machine-id kaya tuloy pa
// rin ang paggamit sa mga iyon para sa mas matibay na anti-clone binding.
const IS_VOLATILE_CLOUD_HOST = process.env.RENDER === 'true' || !!process.env.RENDER_SERVICE_ID;

function getNonAndroidMachineParts() {
    // ANTI-CLONE FALLBACK: kapag Android props ang wala (hal. tumatakbo
    // sa Windows/Linux/VM na PC), kailangan pa rin ng ibang paraan para
    // makakuha ng identifier na TALAGANG naka-tali sa PISIKAL na makina —
    // kung hindi, ang bawat simpleng "kopya ng buong folder papunta sa
    // ibang PC" ay hindi na-detect bilang bagong device.
    if (IS_VOLATILE_CLOUD_HOST) {
        // Sadyang blangko: sa Render/katulad, ang bagong hostname/machine-id
        // kada restart ay HINDI senyales ng pag-clone — senyales lang ito
        // ng normal na container recycling. Ang deviceSeed (idinadagdag na
        // sa computeHardwareFingerprint sa ibaba) na lang ang gagamitin.
        return [];
    }
    const parts = [];
    try { parts.push(os.hostname()); } catch (e) {}
    try { parts.push(os.platform()); } catch (e) {}
    try { parts.push(os.arch()); } catch (e) {}
    try {
        const cpus = os.cpus();
        if (cpus && cpus[0] && cpus[0].model) parts.push(cpus[0].model);
    } catch (e) {}
    try {
        // Stable MAC addresses ng mga non-internal network interfaces —
        // hindi ito nagbabago kahit i-reinstall ang OS o i-clone ang app.
        const nets = os.networkInterfaces();
        const macs = Object.values(nets || {})
            .flat()
            .filter(n => n && !n.internal && n.mac && n.mac !== '00:00:00:00:00:00')
            .map(n => n.mac)
            .sort();
        if (macs.length) parts.push(macs.join(','));
    } catch (e) {}
    try {
        // Linux machine-id: natatangi bawat OS install, hindi kasama kapag
        // kinopya lang ang app folder (nasa /etc, hindi kasama sa clone).
        if (fs.existsSync('/etc/machine-id')) {
            parts.push(fs.readFileSync('/etc/machine-id', 'utf8').trim());
        }
    } catch (e) {}
    return parts.filter(Boolean);
}

function computeHardwareFingerprint(data) {
    const androidParts = [
        getAndroidProp('ro.product.model'),
        getAndroidProp('ro.product.device'),
        getAndroidProp('ro.product.board'),
        getAndroidProp('ro.build.fingerprint'),
        getAndroidProp('ro.serialno'),
        getAndroidProp('ro.boot.serialno'),
    ].filter(Boolean);

    // Isinasama na ang device seed sa LAHAT ng path (Android man o hindi)
    // — ito ang nagpapatunay na MAIIBA pa rin ang fingerprint kahit
    // magkaparehong modelo/build ang dalawang pisikal na device.
    // ANTI-CLONE FIX: kinukuha/nililikha na ang seed mula sa DB record
    // (`data`) sa halip na sa isang local file, para hindi ito mabura sa
    // restart/redeploy sa Render (walang persistent disk).
    const seed = getOrCreateDeviceSeed(data);

    if (androidParts.length > 0) {
        return crypto.createHash('sha256').update([...androidParts, seed].join('|')).digest('hex');
    }

    const machineParts = getNonAndroidMachineParts();
    const allParts = [...machineParts, seed].filter(Boolean);
    if (allParts.length === 0) return null;
    return crypto.createHash('sha256').update(allParts.join('|')).digest('hex');
}

// --------------------------------------------------------------
// ANTI-CLONE FIX: ang installationId ay dapat na STABLE at RANDOM —
// ginagawa ito NANG ISANG BESES LANG at nakatago sa featureUnlocks data
// (na kasama sa "identity" ng install na ito, kahit kopyahin ang buong
// folder). HINDI na ito hinahango/kino-compute mula sa hardware
// fingerprint — dating BUG ito: dahil derived dati ang installationId
// sa fingerprint, kada magbago ang fingerprint (hal. kinopya papunta sa
// ibang device), NAGBABAGO RIN ang installationId kasabay nito — kaya
// laging "first time" / bagong ID ang nakikita ng RELAY, at hindi na
// kailanman na-de-detect ang clone (dahil hindi na pareho ang ID na
// sinusuri para sa fingerprint mismatch).
//
// Ngayon: installationId = permanenteng random UUID na sumasama sa
// kinopyang data. hardwareFingerprint = hiwalay na LIVE signal lang,
// ginagamit para malaman kung nagbago ang pisikal na makina PARA SA
// PAREHONG installationId — ito mismo ang kailangan ng RELAY verify-login
// para gumana nang tama ang "same installationId, different fingerprint
// = clone_suspected" na lohika.
// --------------------------------------------------------------
function getOrCreateInstallationId(data) {
    if (data.installationId) return data.installationId;
    data.installationId = crypto.randomUUID();
    writeData(FILE_FEATURE_UNLOCKS, data);
    return data.installationId;
}

function verifyUnlockToken(token, expectedInstallationId, expectedFeatureId) {
    if (!token || !token.payload || !token.signature) return false;
    const { installationId, featureId, issuedAt, expiresAt } = token.payload;
    if (installationId !== expectedInstallationId) return false;
    if (featureId !== expectedFeatureId) return false;
    if (typeof issuedAt !=='number') return false;

    if (typeof expiresAt ==='number' && Date.now() > expiresAt) return false;

    const payloadString = typeof expiresAt ==='number'
        ? JSON.stringify({ installationId, featureId, issuedAt, expiresAt })
        : JSON.stringify({ installationId, featureId, issuedAt });

    try {
        return crypto.verify(null, Buffer.from(payloadString), RELAY_PUBLIC_KEY, Buffer.from(token.signature,'base64'));
    } catch (err) {
        return false;
    }
}

// --------------------------------------------------------------
// verifyDevicePermit — ANTI-CLONE, PERMIT SYSTEM (offline-capable)
// Sinusuri kung ang naka-imbak na "permit" (nakuha noong huling
// SUCCESSFUL online verify-login sa RELAY, tingnan ang
// checkDeviceBeforeLogin) ay TALAGANG pinirmahan ng RELAY private key
// PARA SA eksaktong (installationId, fingerprint) na ito. Purong lokal
// na signature check ito gamit ang RELAY_PUBLIC_KEY — WALANG internet
// na kailangan — kaya patuloy na gagana ang OFFLINE login habang hindi
// nagbabago ang fingerprint.
//
// Bakit kailangan ito bukod pa sa simpleng `deviceVerified` boolean:
// kung kokopyahin/i-restore ang database file at direktang i-edit ang
// row (o kung may access sa raw DB), MADALING gawing `true` ang isang
// boolean — pero HINDI kailanman mapeke ang isang valid na signature
// dahil wala silang private key ng RELAY. Kaya kahit ma-tamper ang
// lokal na flag, hindi ito magiging katanggap-tanggap na "permit"
// hangga't walang totoong signature na tumutugma.
// --------------------------------------------------------------
function verifyDevicePermit(permit, expectedInstallationId, expectedFingerprint) {
    if (!permit || !permit.payload || !permit.signature) return false;
    const { installationId, fingerprint, issuedAt } = permit.payload;
    if (installationId !== expectedInstallationId) return false;
    if (fingerprint !== expectedFingerprint) return false;
    if (typeof issuedAt !== 'number') return false;

    const payloadString = JSON.stringify({ installationId, fingerprint, issuedAt });
    try {
        return crypto.verify(null, Buffer.from(payloadString), RELAY_PUBLIC_KEY, Buffer.from(permit.signature, 'base64'));
    } catch (err) {
        return false;
    }
}

// ====================================================================
// ANTI-CLONE DEVICE VERIFICATION — kailangan bago makapag-login
// ====================================================================
// Layunin: pigilan ang isang taong basta nag-copy/nag-move ng BUONG app
// folder (kasama ang data/featureUnlocks) papunta sa IBANG pisikal na
// device mula sa pag-login nang parang walang nangyari.
//
// Paano gumagana:
//   1. Sa BAWAT login attempt, kino-compute muna ang LIVE hardware
//      fingerprint ng kasalukuyang makina (computeHardwareFingerprint()).
//   2. Kung ito ang UNANG beses (walang naka-store na verifiedFingerprint
//      pa dati) — o kung IBA na ang live fingerprint kumpara sa huling
//      verified fingerprint (ibig sabihin, ibang pisikal na makina na
//      ito ngayon, malamang kinopya/inilipat) — HINDI muna papayagan
//      ang login hangga't hindi ito successful na na-verify ONLINE sa
//      RELAY. Kung wala/hindi maabot ang internet/RELAY sa sitwasyong
//      ito, tatanggihan ang login nang may malinaw na mensahe.
//   3. Kapag successful ang online verify, naka-imbak na ang
//      "verifiedFingerprint" — pwede nang mag-login nang OFFLINE
//      pagkatapos, hangga't PAREHO pa rin ang fingerprint ng makina.
//   4. Kung sa RELAY mismo ay nakita nitong ang installationId na ito
//      ay dating naka-bind na sa IBANG fingerprint (ibig sabihin dalawang
//      magkaibang pisikal na device ang nag-claim ng iisang
//      installationId — senyales ng cloning), tatanggihan ng RELAY ang
//      verification at ma-flag ito bilang "clone_suspected" sa admin
//      panel, hangga't hindi ito ni-review/ni-reset ng developer/owner.
async function verifyDeviceWithRelay(installationId, hardwareFingerprint, { username, storeName } = {}) {
    if (!RELAY_API_KEY) {
        return { ok: false, reason: 'no_api_key', message: 'Walang RELAY_API_KEY na naka-configure sa server na ito.' };
    }
    // FIX: bago pa man subukan ang buong relayFetch() (hanggang 20s),
    // mabilisang tsek muna (max ~3s, cached) kung may aktwal na internet
    // access ang device — para kahit "Online" pa rin ang manual na toggle
    // pero talagang wala ngang koneksyon, agad na mag-fail-fast dito sa
    // halip na maghintay pa ng buong Relay timeout.
    if (!(await isInternetLikelyUp())) {
        return { ok: false, reason: 'unreachable', message: 'Walang internet connection na na-detect sa device na ito.' };
    }
    try {
        const relayRes = await relayFetch(`${RELAY_URL}/relay/verify-login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-relay-key': RELAY_API_KEY },
            body: JSON.stringify({ installationId, hardwareFingerprint, username, storeName })
        });
        const relayData = await relayRes.json().catch(() => ({}));
        if (!relayRes.ok || !relayData.success) {
            return {
                ok: false,
                reason: relayData.cloneSuspected ? 'clone_suspected' : 'rejected',
                message: relayData.message || 'Tinanggihan ng RELAY ang device verification.'
            };
        }
        return {
            ok: true,
            allowed: !!relayData.allowed,
            // Kung na-"split" ng admin ang device na ito sa RELAY (tingnan
            // ang /relay/admin/api/devices/:id/split-clone), ibabalik dito
            // ng RELAY ang BAGONG installationId na dapat nang gamitin ng
            // client na ito mula ngayon.
            reassignedInstallationId: relayData.reassignedInstallationId || null,
            // PERMIT SYSTEM: signed proof mula sa RELAY na PARA SA
            // (installationId, fingerprint) na ito, ito-store lokal para
            // magamit ulit OFFLINE (verifyDevicePermit) sa susunod.
            permit: relayData.permit || null
        };
    } catch (err) {
        const message = err.name === 'AbortError'
            ? 'Hindi maabot ang RELAY (nag-timeout habang naghihintay ng response).'
            : `Hindi maabot ang RELAY (${err.message}).`;
        return { ok: false, reason: 'unreachable', message };
    }
}

// Tinatawag ito bago payagan ang login. Nagbabalik ng { allowed, message }.
async function checkDeviceBeforeLogin({ username } = {}) {
    const data = readFeatureUnlocks();
    const installationId = getOrCreateInstallationId(data);
    // PAALALA: dapat isunod ito bago i-compute ang fingerprint, dahil
    // ang deviceSeed ay ilalikha/ilalagay dito sa `data` mismo (hindi na
    // sa isang local file) — kailangan nating tiyakin munang naka-load
    // ang pinaka-bagong record bago tumawag ng computeHardwareFingerprint.
    const liveFingerprint = computeHardwareFingerprint(data);

    // Kung walang paraang makakuha ng anumang fingerprint (sobrang bihira),
    // hindi na natin ito ma-eenforce nang maayos — huwag i-block, pero
    // i-log bilang babala.
    if (!liveFingerprint) {
        console.warn('⚠️ ANTI-CLONE: walang na-compute na hardware fingerprint — hindi ma-enforce ang device-binding check.');
        return { allowed: true };
    }

    const fingerprintUnchanged = data.deviceVerified && data.verifiedFingerprint === liveFingerprint;

    // PERMIT SYSTEM: hindi na sapat na basta "magkatugma" ang lokal na
    // deviceVerified/verifiedFingerprint flags — dahil kung sakaling
    // direktang ma-edit/ma-restore ang DB row (mismatch ng backup,
    // atbp.), MADALING i-fake ang mga flag na iyon nang manwal. Kaya
    // sinusuri ulit dito, PURONG LOKAL (walang internet), kung ang huling
    // naka-imbak na permit ay TALAGANG naka-sign ng RELAY PARA SA
    // eksaktong installationId+fingerprint na ito ngayon.
    //
    // BACKWARD-COMPATIBLE MIGRATION: kung WALA pang naka-imbak na permit
    // (lumang record bago idagdag ang permit system), huwag agad i-block
    // — payagan muna, at kukunin/ita-tago lang ang permit sa opportunistic
    // background recheck sa ibaba. Sa susunod na login, mayroon na itong
    // permit na masusuri.
    const permitOk = !data.devicePermit || verifyDevicePermit(data.devicePermit, installationId, liveFingerprint);

    // DEVELOPER-AUTHORIZATION GATE: hindi lang basta "kilalang device"
    // (fingerprint match / permit valid) ang kailangan para makapag-login
    // — kailangan ding EXPLICIT na "Allow" na ito ng developer/owner sa
    // RELAY admin panel (allowedDevices), kahit pa unang beses pa lang
    // itong device (walang mismatch, walang clone suspicion). Kaya kahit
    // pumasa ang fingerprint/permit check sa itaas, hindi pa rin dapat
    // payagan ang login hangga't hindi pa naka-set ang lokal na
    // "relayAuthorized" flag na ito (na TANGING mula sa RELAY manggagaling,
    // hindi ito basta pwedeng i-edit lokal at magamit — dahil kailangan
    // pa ring pumasa ang fingerprint/permit check bago pa man dito
    // umabot).
    if (fingerprintUnchanged && permitOk && data.relayAuthorized === true) {
        // Opportunistic background re-check lang (hindi hinihintay/hindi
        // nagba-block ng login) — kung sakaling mag-flag ang RELAY na
        // clone_suspected dahil may ibang device na gumamit na rin ng
        // parehong installationId, o kung sakaling i-Revoke ng developer
        // ang authorization na ito, malalaman agad sa admin panel — at
        // dito rin ita-tago ang bagong resulta (kasama ang pagbawi ng
        // relayAuthorized kung na-revoke) para sa susunod na login.
        //
        // Sinusunod nito ang manual na Online/Offline TOGGLE ng user
        // (tingnan ang FILE_CONNECTIVITY_MODE): kapag "offline" ang
        // pinili niya, hindi na ito PROACTIVE na tatawag sa RELAY — pero
        // hindi ito nakakaapekto sa seguridad, dahil sa isang totoong
        // fingerprint mismatch (posibleng clone), MANDATORY pa ring
        // tatawag online REGARDLESS ng toggle na ito (tingnan sa baba).
        if (getConnectivityMode() === 'online') {
            verifyDeviceWithRelay(installationId, liveFingerprint, { username })
                .then(r => {
                    if (r.ok) {
                        const latest = readFeatureUnlocks();
                        if (r.permit) latest.devicePermit = r.permit;
                        latest.relayAuthorized = !!r.allowed;
                        writeData(FILE_FEATURE_UNLOCKS, latest);
                    }
                })
                .catch(() => {});
        }
        return { allowed: true };
    }

    // Alinman sa: (a) unang beses pa lang, (b) nagbago ang fingerprint
    // (ibig sabihin naka-move/na-clone papunta sa ibang device), o (c)
    // hindi pa na-a-authorize ng developer (relayAuthorized !== true) —
    // KAILANGAN ng SUCCESSFUL online verification muna sa RELAY.
    const result = await verifyDeviceWithRelay(installationId, liveFingerprint, { username });

    if (!result.ok) {
        return {
            allowed: false,
            message: result.reason === 'clone_suspected'
                ? 'This device is not recognized. Please contact your administrator.'
                : 'Unable to verify this device right now. Please check your internet connection and try again.'
        };
    }

    const updated = readFeatureUnlocks();

    if (result.reassignedInstallationId && result.reassignedInstallationId !== installationId) {
        // Na-"split" ng admin ang device na ito sa RELAY bilang sarili at
        // hiwalay na installationId (dating clone_suspected). Kailangan
        // nating i-adopt ito dito lokal — kasama ang paglinis ng mga
        // dating tokens (naka-bind ang mga iyon sa LUMANG installationId,
        // kaya hindi na rin sila magiging valid dito) — magsisimula itong
        // device nang walang naka-unlock na feature hangga't hindi ito
        // manual na inaktibo ng admin/developer para sa BAGONG ID.
        console.log(`ℹ️ ANTI-CLONE: hiwalay na installationId ang ibinigay ng RELAY (${result.reassignedInstallationId}) — ina-adopt lokal.`);
        updated.installationId = result.reassignedInstallationId;
        updated.tokens = {};
    } else {
        updated.installationId = installationId;
    }

    // Naka-verify na ang fingerprint (kilala/hindi clone) — pero ITO PA
    // RIN ang tunay na desisyon kung PWEDE NA ba talagang makapag-login:
    // kailangan munang naka-Allow ng developer/owner ang installationId
    // na ito sa RELAY admin panel (result.allowed). Ang fingerprint
    // binding sa ibaba ay pinag-iimbak PA RIN kahit hindi pa authorized
    // — para gumana pa rin ang anti-clone tracking simula ngayon — pero
    // hindi ito ang nagpapahintulot ng login.
    updated.deviceVerified = true;
    updated.verifiedFingerprint = liveFingerprint;
    updated.devicePermit = result.permit || null;
    updated.relayAuthorized = !!result.allowed;
    updated.firstVerifiedAt = updated.firstVerifiedAt || Date.now();
    updated.lastVerifiedAt = Date.now();
    writeData(FILE_FEATURE_UNLOCKS, updated);

    if (!result.allowed) {
        return {
            allowed: false,
            message: 'This device has been logged with the developer/store owner. Please wait for authorization (Allow) before it can log in. Contact the developer/store owner.'
        };
    }

    return { allowed: true };
}

// ====================================================================
// LIVE DEVICE-REVOCATION CHECK — para awtomatikong ma-logout LAHAT ng
// naka-login sa isang device sa SANDALING alisin ito ng developer/owner
// sa "allowed devices" ng RELAY, kahit walang gumawa ng bagong
// login/logout. Dati, ang relayAuthorized ay opportunistic lang
// na-rerecheck sa checkDeviceBeforeLogin (bago mag-login) — ibig sabihin
// habang naka-login na ang isang cashier, hindi ito naaapektuhan agad
// kahit alisin na siya sa allowed list; matatanggal lang siya sa
// susunod niyang pag-login. Ngayon, may hiwalay na periodic check na
// tumatawag sa RELAY (kapag "online" ang connectivity mode) at, sa
// SANDALING mag-flip ang relayAuthorized papuntang false, agad
// dinedestroy ang LAHAT ng kasalukuyang session sa device na ito —
// walang paraan para makapagpatuloy ang isang naka-login na session sa
// isang device na binawian na ng authorization.
const DEVICE_REVOCATION_RECHECK_MS = 3 * 60 * 1000;

let lastLiveRecheckAt = 0;

async function recheckDeviceAuthorizationLive() {
    try {
        // Kung walang kahit isang naka-login na session, walang i-fo-force-
        // logout — huwag nang mag-abala pa sa RELAY. Pinapanatili nito ang
        // ORIHINAL na "minimal na network chatter" na disenyo: 0 session =
        // 0 background call sa RELAY, eksaktong tulad ng dati.
        if (SESSIONS.size === 0) return;
        if (getConnectivityMode() !== 'online') return; // sinusunod ang manual na toggle ng user
        const data = readFeatureUnlocks();
        const installationId = getOrCreateInstallationId(data);
        const liveFingerprint = computeHardwareFingerprint(data);
        if (!liveFingerprint) return;

        const wasAuthorized = data.relayAuthorized === true;
        const result = await verifyDeviceWithRelay(installationId, liveFingerprint, {});

        // Hindi ma-abot ang RELAY (network blip lang, halimbawa) — huwag
        // pagbatayan ng revocation; panatilihin ang huling kilalang estado.
        if (!result.ok && result.reason === 'unreachable') return;

        const nowAuthorized = result.ok && !!result.allowed;

        const latest = readFeatureUnlocks();
        latest.relayAuthorized = nowAuthorized;
        if (result.ok && result.permit) latest.devicePermit = result.permit;
        writeData(FILE_FEATURE_UNLOCKS, latest);

        if (wasAuthorized && !nowAuthorized) {
            const revokedCount = SESSIONS.size;
            SESSIONS.clear();
            persistSessions();
            console.log(`🚫 DEVICE REVOKED: inalis ng developer/owner ang device na ito sa allowed list ng RELAY — na-force-logout ang ${revokedCount} aktibong session.`);
        }
    } catch (err) {
        console.error('⚠️ Hindi na-finish ang live device-authorization recheck:', err.message);
    }
}

// TANDAAN: WALANG standalone setInterval/setTimeout dito — sadyang
// tinanggal, para hindi ito palaging tumatawag sa RELAY kahit walang
// ginagawa ang device. Sa halip, ito ay tina-trigger na lang mula sa
// totoong API traffic (tingnan ang throttled na tawag dito sa loob ng
// /api/* auth middleware sa itaas) — kaya kung idle ang device, 0 tawag
// sa RELAY, eksaktong gaya ng orihinal na "one-time/opportunistic" na
// disenyo. Kapag ginagamit naman (bawat click/transaksyon), doon lang
// ito magre-recheck, throttled sa bawat DEVICE_REVOCATION_RECHECK_MS.
// ====================================================================
// Bawat successful run: (1) kinokopya/ino-overwrite ang database papunta
// sa IISANG file sa Download/RELAY_BACKUP ng device (tingnan ang
// mirrorBackupToDownloads() sa db.js — hindi ito dumaragdag ng bagong
// file bawat run), tapos (2) tumatawag sa bagong RELAY endpoint
// (/relay/backup-checkin) para ipaalam na successful ang backup na ito
// — ito ang nagpapa-awtomatikong dagdag sa device na ito sa "Allowed
// devices" list sa Relay admin panel (tingnan ang relay_server.js),
// kaya hindi na kailangang bumalik pa sa admin panel para i-Allow nang
// manual tuwing may bagong verified/regular na gumagawa ng backup.
//
// "Dot" status para dito (orange = naghihintay/waiting o may problema,
// green = matagumpay na na-sync) ay in-memory lang dito
// (relayBackupStatus), naka-expose sa /api/relay-backup/status kung
// kakailanganin balang araw ng isang UI widget sa loob mismo ng
// OMNIPOS — ang PANGUNAHING dot/indicator na hiningi ay nasa Relay
// admin panel mismo (sa itaas ng "Allowed devices"), dahil doon
// naka-consolidate ang status ng LAHAT ng device, hindi lang isa.
const relayBackupStatus = {
    state: 'orange', // 'orange' = waiting/may problema, 'green' = matagumpay
    lastAttemptAt: null,
    lastSuccessAt: null,
    lastError: null,
    path: null
};

async function runRelayBackupSync() {
    if (getConnectivityMode() === 'offline') {
        relayBackupStatus.state = 'orange';
        relayBackupStatus.lastError = 'Naka-OFFLINE mode — sinadya munang hindi tumatawag sa RELAY.';
        return;
    }
    // FIX: sinusunod din ngayon ang aktwal na internet status (hindi lang
    // ang manual na toggle) — ito ay tumatakbo AWTOMATIKO 40s pagkatapos
    // mag-boot ang server (at every 24h), kaya kung walang internet sa
    // start pa lang ng system (Online pa rin ang toggle), dating
    // naghihintay ito ng buong relayFetch timeout bago mag-fail.
    if (!(await isInternetLikelyUp())) {
        relayBackupStatus.state = 'orange';
        relayBackupStatus.lastError = 'Walang internet connection na na-detect.';
        return;
    }
    relayBackupStatus.lastAttemptAt = Date.now();

    const mirrorResult = mirrorBackupToDownloads();
    if (!mirrorResult.success) {
        relayBackupStatus.state = 'orange';
        relayBackupStatus.lastError = mirrorResult.message;
        console.error('⚠️ RELAY_BACKUP: hindi na-mirror sa Download folder:', mirrorResult.message);
        return;
    }
    relayBackupStatus.path = mirrorResult.path;
    console.log(
        mirrorResult.existedBefore
            ? `🔁 RELAY_BACKUP: na-update ang existing na file sa ${mirrorResult.path} (${mirrorResult.sizeBytes} bytes)`
            : `🆕 RELAY_BACKUP: unang beses na nagawa ang file sa ${mirrorResult.path} (${mirrorResult.sizeBytes} bytes)`
    );

    if (!RELAY_API_KEY) {
        relayBackupStatus.state = 'orange';
        relayBackupStatus.lastError = 'Walang RELAY_API_KEY na naka-configure — hindi ma-checkin sa relay.';
        return;
    }

    try {
        const data = readFeatureUnlocks();
        const installationId = getOrCreateInstallationId(data);
        const receiptSettings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);

        const relayRes = await relayFetch(`${RELAY_URL}/relay/backup-checkin`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-relay-key': RELAY_API_KEY },
            body: JSON.stringify({
                installationId,
                storeName: (receiptSettings && receiptSettings.storeName) || null,
                fileSizeBytes: mirrorResult.sizeBytes,
                backupAt: Date.now()
            })
        });
        const relayData = await parseRelayResponse(relayRes);

        if (!relayData.success) {
            relayBackupStatus.state = 'orange';
            relayBackupStatus.lastError = relayData.message || 'Tinanggihan ng relay ang backup check-in.';
            return;
        }

        relayBackupStatus.state = 'green';
        relayBackupStatus.lastSuccessAt = Date.now();
        relayBackupStatus.lastError = null;
        console.log(`✅ RELAY_BACKUP: na-checkin sa relay (installationId: ${installationId}).`);
    } catch (err) {
        relayBackupStatus.state = 'orange';
        relayBackupStatus.lastError = err.message;
        console.error('⚠️ RELAY_BACKUP: hindi ma-abot ang relay para sa check-in:', err.message);
    }
}

// Sinasama ito sa parehong AUTO_BACKUP_DISABLED toggle na ginagamit ng
// runLocalDatabaseBackup — iisang switch lang (.env) para i-off/on ang
// LAHAT ng auto-backup behavior nang sabay. Konting delay (40s) mula sa
// startup para makasunod muna ito sa unang runLocalDatabaseBackup(30s).
if (!AUTO_BACKUP_DISABLED) {
    setTimeout(runRelayBackupSync, 40 * 1000);
    setInterval(runRelayBackupSync, 24 * 60 * 60 * 1000).unref();
}

// Read-only status endpoint (opsyonal na gamitin ng future UI widget sa
// loob ng OMNIPOS mismo) — protektado pa rin ng parehong session/token
// middleware gaya ng ibang /api/* routes.
app.get('/api/relay-backup/status', (req, res) => {
    res.json({ success: true, ...relayBackupStatus });
});

// ====================================================================
// FILE INTEGRITY CHECK-IN ("git status" papuntang RELAY) — pana-panahon
// na kinukuha ang sha256 hash ng BAWAT file sa sarili nitong install
// folder (maliban sa runtime/data na inaasahang iba-iba talaga bawat
// device — .env, .env.key, database/, node_modules/, uploads_tmp/,
// .git/, release/, *.log), at ipinapadala papunta sa RELAY
// (/relay/integrity-checkin) kasama ang APP_VERSION nito. Doon
// kino-compare ito sa baseline manifest ng version na iyon (kinuha
// mismo mula sa eksaktong release na binuo/ipinadala para dito), at
// nakikita sa RELAY admin panel (parang "git status") kung may na-edit
// o na-delete na file ang client — naka-red-flag doon.
//
// SADYANG GAMIT ang PAREHONG exclude list (SELF_UPDATE_PRESERVE, tingnan
// sa ibaba ng file na ito) para hindi mag-report ng maling "modified/
// deleted" para lang sa runtime data na normal namang iba-iba bawat
// device.
// --------------------------------------------------------------
const INTEGRITY_SCAN_EXCLUDE_NAMES = new Set([
    '.env', '.env.key', 'database', 'node_modules', 'uploads_tmp',
    '.git', 'release', 'cf.log', 'server.log'
]);
const INTEGRITY_SCAN_EXCLUDE_EXTENSIONS = new Set(['.log', '.patch']);

function computeInstallDirManifest() {
    const manifest = {};
    function walk(dir, relBase) {
        let entries;
        try {
            entries = fs.readdirSync(dir, { withFileTypes: true });
        } catch (err) {
            return;
        }
        for (const entry of entries) {
            if (INTEGRITY_SCAN_EXCLUDE_NAMES.has(entry.name)) continue;
            const full = path.join(dir, entry.name);
            const rel = relBase ? `${relBase}/${entry.name}` : entry.name;
            if (entry.isDirectory()) {
                walk(full, rel);
                continue;
            }
            if (INTEGRITY_SCAN_EXCLUDE_EXTENSIONS.has(path.extname(entry.name).toLowerCase())) continue;
            try {
                const hash = crypto.createHash('sha256');
                hash.update(fs.readFileSync(full));
                manifest[rel] = hash.digest('hex');
            } catch (err) {
                // Hindi mababasa — laktawan na lang, hindi dapat
                // pabagsakin ang buong check-in dahil dito.
            }
        }
    }
    walk(__dirname, '');
    return manifest;
}

// "Dot" status para dito, kaparehong pattern ng relayBackupStatus sa
// itaas — read-only lang, kung kailangan balang araw ng isang UI widget.
const relayIntegrityStatus = {
    state: 'orange', // 'orange' = waiting/hindi pa nagawa, 'green' = malinis, 'red' = may naka-flag
    lastAttemptAt: null,
    lastSuccessAt: null,
    lastError: null,
    flagged: false,
    modifiedCount: 0,
    deletedCount: 0,
    addedCount: 0
};

async function runRelayIntegrityCheckin() {
    if (getConnectivityMode() === 'offline') {
        relayIntegrityStatus.state = 'orange';
        relayIntegrityStatus.lastError = 'Naka-OFFLINE mode — sinadya munang hindi tumatawag sa RELAY.';
        return;
    }
    if (!(await isInternetLikelyUp())) {
        relayIntegrityStatus.state = 'orange';
        relayIntegrityStatus.lastError = 'Walang internet connection na na-detect.';
        return;
    }
    if (!RELAY_API_KEY) {
        relayIntegrityStatus.state = 'orange';
        relayIntegrityStatus.lastError = 'Walang RELAY_API_KEY na naka-configure — hindi ma-checkin sa relay.';
        return;
    }

    relayIntegrityStatus.lastAttemptAt = Date.now();

    try {
        const data = readFeatureUnlocks();
        const installationId = getOrCreateInstallationId(data);
        const files = computeInstallDirManifest();

        const relayRes = await relayFetch(`${RELAY_URL}/relay/integrity-checkin`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-relay-key': RELAY_API_KEY },
            body: JSON.stringify({
                installationId,
                version: APP_VERSION,
                files
            })
        });
        const relayData = await parseRelayResponse(relayRes);

        if (!relayData.success) {
            relayIntegrityStatus.state = 'orange';
            relayIntegrityStatus.lastError = relayData.message || 'Tinanggihan ng relay ang integrity check-in.';
            return;
        }

        relayIntegrityStatus.lastSuccessAt = Date.now();
        relayIntegrityStatus.lastError = null;
        relayIntegrityStatus.flagged = !!relayData.flagged;
        relayIntegrityStatus.modifiedCount = relayData.modifiedCount || 0;
        relayIntegrityStatus.deletedCount = relayData.deletedCount || 0;
        relayIntegrityStatus.addedCount = relayData.addedCount || 0;
        relayIntegrityStatus.state = relayData.flagged ? 'red' : 'green';
        // (Sinadyang tinanggal ang console.log dito — silent, walang lumalabas
        // sa Termux terminal. Nasa relayIntegrityStatus/API na lang ang resulta.)
    } catch (err) {
        relayIntegrityStatus.state = 'orange';
        relayIntegrityStatus.lastError = err.message;
        // (Sinadyang tinanggal ang console.error dito — silent din ang mga error.)
    }
}

// Konting delay (55s) mula sa startup para makasunod sa backup sync
// (40s) at hindi magsabay sa parehong segundo — every 24h din pagkatapos.
// Ito ang SAFETY-NET na scheduled check-in — nananatili ito kahit may
// real-time watcher na sa ibaba (tingnan REAL-TIME INTEGRITY WATCHER),
// para may bumubuo pa ring "clean" na check-in kahit walang file na
// nagbago (at sakaling na-miss ng watcher ang isang event, hal. habang
// naka-offline o bago pa nag-start ang app).
// FIX: ginamit na ang HIWALAY na INTEGRITY_MONITOR_DISABLED (hindi na
// AUTO_BACKUP_DISABLED) — para hindi na-o-off ito kapag DISABLE_AUTO_BACKUP
// lang talaga ang sinadyang i-set (backup ≠ integrity monitor).
if (!INTEGRITY_MONITOR_DISABLED) {
    setTimeout(runRelayIntegrityCheckin, 55 * 1000);
    setInterval(runRelayIntegrityCheckin, 24 * 60 * 60 * 1000).unref();
}

// ====================================================================
// REAL-TIME INTEGRITY WATCHER — sa halip na hintayin ang susunod na
// 24-oras na naka-iskedyul na check-in, dito ay DINE-DETECT AGAD
// (event-driven, HINDI polling kada segundo) kapag may na-edit/na-
// delete/na-add na file sa install folder gamit ang fs.watch() per
// directory. Kapag may nangyaring pagbabago:
//   1. DEBOUNCE (0.8s) — para sama-samang mahawakan ang maramihang
//      sunod-sunod na save (hal. galing sa isang editor/deploy).
//   2. THROTTLE (min 1.5s gap sa pagitan ng dalawang check-in) — proteksyon
//      pa rin laban sa sunod-sunod na tawag, pero mas mabilis na ngayon.
// Full manifest pa rin ang ipinapadala kada check-in (kailangan ito ng
// RELAY para makita rin ang mga DELETED file, hindi lang MODIFIED) —
// pero event-driven na ang trigger sa halip na "every 1 second", kaya
// halos instant makikita sa RELAY admin panel ang pagbabago nang hindi
// pinapabagal ang sistema. SILENT ito — walang console output na
// lumalabas sa Termux terminal; tahimik na tumatakbo sa localhost:3000.
// --------------------------------------------------------------
let integrityWatchDebounceTimer = null;
let integrityWatchLastRunAt = 0;
const INTEGRITY_WATCH_DEBOUNCE_MS = 800;
const INTEGRITY_WATCH_MIN_GAP_MS = 1500;
const integrityWatchers = new Map(); // dir path -> fs.FSWatcher

function scheduleIntegrityCheckinFromWatcher(changedPath) {
    if (integrityWatchDebounceTimer) clearTimeout(integrityWatchDebounceTimer);
    integrityWatchDebounceTimer = setTimeout(() => {
        integrityWatchDebounceTimer = null;
        const wait = Math.max(0, INTEGRITY_WATCH_MIN_GAP_MS - (Date.now() - integrityWatchLastRunAt));
        setTimeout(() => {
            integrityWatchLastRunAt = Date.now();
            runRelayIntegrityCheckin().catch(() => {});
        }, wait);
    }, INTEGRITY_WATCH_DEBOUNCE_MS);
}

function watchDirForIntegrity(dir) {
    if (integrityWatchers.has(dir)) return;
    try {
        const watcher = fs.watch(dir, { persistent: false }, (eventType, filename) => {
            scheduleIntegrityCheckinFromWatcher(filename ? path.join(dir, filename) : dir);
        });
        watcher.on('error', () => { integrityWatchers.delete(dir); });
        integrityWatchers.set(dir, watcher);
    } catch (err) {
        // Hindi na-watch (hal. permission issue sa Termux/Android) —
        // hindi dapat pabagsakin ang buong app dahil dito, mananatili
        // na lang ang 24h na scheduled fallback sa itaas.
    }
}

function refreshIntegrityWatchers() {
    // NOTE: fs.watch sa bawat directory nang hiwalay (hindi umaasa sa
    // { recursive: true }, na hindi consistent kumilos sa lahat ng
    // Linux/Android environment) — kaya paulit-ulit itong tinatawag
    // (kada 5 min) para masaklaw din ang mga BAGONG subdirectory na
    // nagawa pagkatapos mag-start.
    function walk(dir) {
        let entries;
        try {
            entries = fs.readdirSync(dir, { withFileTypes: true });
        } catch (err) {
            return;
        }
        watchDirForIntegrity(dir);
        for (const entry of entries) {
            if (INTEGRITY_SCAN_EXCLUDE_NAMES.has(entry.name)) continue;
            if (entry.isDirectory()) walk(path.join(dir, entry.name));
        }
    }
    walk(__dirname);
}

if (!INTEGRITY_MONITOR_DISABLED) {
    setTimeout(refreshIntegrityWatchers, 10 * 1000);
    setInterval(refreshIntegrityWatchers, 5 * 60 * 1000).unref();
}

// ====================================================================
// FAST "CHECK NOW" POLL — BAGONG dagdag. Dati, ang "🔄 I-check ngayon"
// (Check Now) na pindot ng admin sa RELAY admin panel ay hinihintay
// lang ng client sa SUSUNOD na normal na 30s na attemptRelayFeatureSync()
// tawag (tingnan sa ibaba) — ibig sabihin pwedeng umabot ng malapit
// 30 segundo bago talaga ma-trigger.
//
// Dito, hiwalay at mas madalas (kada ilang segundo lang) tinatawag ang
// BAGONG magaan na /relay/pending-integrity-check (isang Set lookup
// lang sa RELAY side, walang manifest/hashing) — kapag "pending: true"
// ang sagot, saka pa lang tatawagin ang buong runRelayIntegrityCheckin().
// SADYANG HIWALAY ito sa attemptRelayFeatureSync() (hindi ito pinapalitan
// o binabago ang 30s na cadence nito) — layunin lang dito ay pabilisin
// ang "Check Now" nang hindi ginagalaw ang ibang existing na behavior
// (feature lockdown/restore, backup sync, real-time file watcher, atbp.),
// para walang panganib ng bagong side effect sa OMNIPOS.
//
// Guards (parehong ginagamit ng ibang relay call dito):
//   - Kailangan naka-configure ang RELAY_API_KEY.
//   - Naka-"online" ang connectivity mode (sinusunod ang manual toggle).
//   - May internet (gamit ang parehong cached na isInternetLikelyUp() —
//     hindi nagdadagdag ng bagong network probe kada tawag).
//   - Skip kung may kasalukuyang tumatakbo pang integrity check-in
//     (isInProgress na flag) — proteksyon laban sa magkatabing tawag.
// --------------------------------------------------------------
let integrityCheckNowInProgress = false;
const INTEGRITY_CHECK_NOW_POLL_MS = Number(process.env.RELAY_INTEGRITY_CHECK_NOW_POLL_MS) || 3 * 1000; // default: 3s

async function pollPendingIntegrityCheckNow() {
    if (!RELAY_API_KEY || getConnectivityMode() === 'offline') return;
    if (integrityCheckNowInProgress) return;
    if (!(await isInternetLikelyUp())) return;

    try {
        const data = readFeatureUnlocks();
        const installationId = getOrCreateInstallationId(data);

        const relayRes = await relayFetch(`${RELAY_URL}/relay/pending-integrity-check`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-relay-key': RELAY_API_KEY },
            body: JSON.stringify({ installationId })
        }, 8000); // maikling timeout — magaan lang dapat ang tawag na ito
        const relayData = await parseRelayResponse(relayRes);

        if (relayData.success && relayData.pending) {
            integrityCheckNowInProgress = true;
            try {
                await runRelayIntegrityCheckin();
            } finally {
                integrityCheckNowInProgress = false;
            }
        }
    } catch (err) {
        // Tahimik na palampasin — ito ay bukod-tanging isang mabilis na
        // "meron ka bang hiling?" poll, hindi mahalaga kung minsan itong
        // mabigo (may safety-net pa ring 55s/24h scheduled check-in at
        // ang normal na 30s check-feature-status na daanan din nito).
    }
}

if (!INTEGRITY_MONITOR_DISABLED && RELAY_API_KEY) {
    setTimeout(pollPendingIntegrityCheckNow, 15 * 1000);
    setInterval(() => { pollPendingIntegrityCheckNow().catch(() => {}); }, INTEGRITY_CHECK_NOW_POLL_MS).unref();
}

app.get('/api/relay-integrity/status', (req, res) => {
    res.json({ success: true, ...relayIntegrityStatus });
});

// --------------------------------------------------------------
// GET/POST /api/connectivity-mode
// Ang manual na Online/Offline TOGGLE na makikita ni client sa UI
// pagkatapos ng successful login. Basahin ang malaking paalala sa
// FILE_CONNECTIVITY_MODE sa itaas — hindi ito bahagi ng anti-clone
// gate, kontrolado lang nito kung PROACTIVE bang tumatawag ang app sa
// RELAY (backup auto-sync, update-check, opportunistic re-verify).
// --------------------------------------------------------------
app.get('/api/connectivity-mode', (req, res) => {
    res.json({ success: true, mode: getConnectivityMode() });
});

app.post('/api/connectivity-mode', (req, res) => {
    const { mode } = req.body || {};
    if (mode !== 'online' && mode !== 'offline') {
        return res.status(400).json({ success: false, message: "Ang 'mode' ay dapat 'online' o 'offline'." });
    }
    const previousMode = getConnectivityMode();
    const saved = setConnectivityMode(mode);

    // FIX: dati, kapag may na-edit/na-delete/na-add na file HABANG naka-
    // OFFLINE ang connectivity mode, agad na "na-drop" lang ito nang
    // tahimik — tumatakbo pa rin ang fs.watch() debounce (tingnan
    // scheduleIntegrityCheckinFromWatcher/REAL-TIME INTEGRITY WATCHER
    // sa itaas) pero agad ring nag-e-early-return ang
    // runRelayIntegrityCheckin() dahil "offline" pa ang mode noong
    // tumakbo ito (state='orange', walang retry na naka-schedule).
    // Ibig sabihin: kapag nag-toggle pabalik ang user papuntang ONLINE,
    // hinihintay pa rin dati ang SUSUNOD na 24h na scheduled check-in
    // (o ang 3s na "Check Now" poll, na umaasa pa sa pag-click ng admin
    // sa RELAY admin panel) bago talaga ma-detect ang mga pagbabagong
    // iyon — hindi ito "agad" gaya ng inaasahan.
    //
    // Ngayon, sa mismong sandaling lumipat papuntang ONLINE (mula sa
    // ibang mode), agad (fire-and-forget — hindi hinihintay/hindi
    // bina-block ang response na ito) tinatawag ang buong
    // runRelayIntegrityCheckin(), para masaklaw kaagad ang ANUMANG
    // file na nabago/nadagdag/nabura habang naka-offline, sa halip na
    // maghintay pa ng ilang segundo/oras bago ito ma-detect sa RELAY.
    if (saved === 'online' && previousMode !== 'online' && !INTEGRITY_MONITOR_DISABLED) {
        runRelayIntegrityCheckin().catch(() => {});
    }

    res.json({ success: true, mode: saved });
});

// ====================================================================
// CLOUD BACKUP (Postgres via RELAY) — MANUAL na trigger lang (button sa
// Settings/Reset & Restore panel), hindi tulad ng RELAY_BACKUP auto-sync
// sa itaas (na .db file mirror lang papunta sa Download folder).
// ====================================================================
// Ito: (1) kumukuha ng BUONG database maliban sa user accounts (tingnan
// ang getCloudBackupPayload() sa db.js), (2) ipinapadala ito papunta sa
// RELAY (/relay/cloud-backup/upload) kasama ang installationId nito, at
// (3) ang RELAY mismo ang tumitingin kung UNLOCKED ba ang 'cloud_backup'
// feature para sa installationId na ito BAGO ito talagang i-save sa
// Postgres — kaya kahit directly tumawag ang isang client papunta sa
// RELAY (nilagpasan ang requireFeature dito sa ibaba), hindi pa rin ito
// talagang maisusulat sa Postgres hangga't hindi ito na-unlock doon.
// ====================================================================
const cloudBackupStatus = {
    state: 'idle', // 'idle' | 'syncing' | 'success' | 'error'
    lastAttemptAt: null,
    lastSuccessAt: null,
    lastError: null,
    lastTotalRecords: null
};

app.get('/api/cloud-backup/status', (req, res) => {
    res.json({ success: true, ...cloudBackupStatus });
});

app.post('/api/cloud-backup/sync', requireFeature('cloud_backup'), async (req, res) => {
    if (getConnectivityMode() === 'offline') {
        return res.status(400).json({ success: false, message: 'Naka-OFFLINE mode ka ngayon. I-tap muna ang Online toggle para makapag-backup sa cloud.' });
    }
    cloudBackupStatus.state = 'syncing';
    cloudBackupStatus.lastAttemptAt = Date.now();

    if (!RELAY_API_KEY) {
        cloudBackupStatus.state = 'error';
        cloudBackupStatus.lastError = 'Walang RELAY_API_KEY na naka-configure sa .env.';
        return res.status(500).json({ success: false, message: cloudBackupStatus.lastError });
    }

    try {
        const featureData = readFeatureUnlocks();
        const installationId = getOrCreateInstallationId(featureData);
        const receiptSettings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);
        const backupPayload = getCloudBackupPayload();

        const relayRes = await relayFetch(`${RELAY_URL}/relay/cloud-backup/upload`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-relay-key': RELAY_API_KEY },
            body: JSON.stringify({
                installationId,
                storeName: (receiptSettings && receiptSettings.storeName) || null,
                modules: backupPayload.modules,
                moduleNames: backupPayload.moduleNames,
                totalRecords: backupPayload.totalRecords,
                generatedAt: backupPayload.generatedAt
            })
        });
        const relayData = await parseRelayResponse(relayRes);

        if (relayRes.status === 402 || relayData.featureLocked) {
            cloudBackupStatus.state = 'error';
            cloudBackupStatus.lastError = relayData.message || 'Naka-lock pa ang Cloud Backup feature.';
            return res.status(402).json(relayData);
        }

        if (!relayData.success) {
            cloudBackupStatus.state = 'error';
            cloudBackupStatus.lastError = relayData.message || 'Tinanggihan ng RELAY ang cloud backup upload.';
            return res.status(502).json({ success: false, message: cloudBackupStatus.lastError });
        }

        cloudBackupStatus.state = 'success';
        cloudBackupStatus.lastSuccessAt = Date.now();
        cloudBackupStatus.lastError = null;
        cloudBackupStatus.lastTotalRecords = backupPayload.totalRecords;

        logAction((req.authUser && req.authUser.username) || 'Unknown', `Cloud Backup: matagumpay na na-sync ang buong database (kasama ang user accounts [walang password], unlocked features/themes) papunta sa Postgres (${backupPayload.totalRecords} records, ${backupPayload.moduleNames.length} modules).`);

        res.json({
            success: true,
            message: 'Matagumpay na na-sync ang database papunta sa cloud (Postgres).',
            totalRecords: backupPayload.totalRecords,
            moduleNames: backupPayload.moduleNames,
            excludedModules: backupPayload.excludedModules
        });
    } catch (err) {
        cloudBackupStatus.state = 'error';
        cloudBackupStatus.lastError = err.message;
        console.error('⚠️ CLOUD_BACKUP: hindi na-abot ang relay para sa upload:', err.message);
        res.status(502).json({ success: false, message: 'Hindi ma-abot ang RELAY para sa cloud backup upload.' });
    }
});

// ====================================================================
// CLOUD BACKUP — SELF-SERVICE RESTORE (Postgres via RELAY)
// ====================================================================
// Tinatawag ito ng "Restore from Cloud" button sa Reset & Restore
// panel. Kailangan ng Admin password (gaya ng /api/restore-backup)
// dahil mapanganib na aksyon ito — papatayin ang kasalukuyang laman ng
// bawat na-restore na module.
//
// MAHALAGANG PALIWANAG (users module): tinanggal ang "password" field
// bago umakyat ang "users" module papunta sa cloud (tingnan ang
// stripRedactedFields() sa db.js). Kaya kapag bumaba ito papunta rito,
// WALANG password ang bawat record. Kung direktang isusulat ito,
// mawawalan ng magagamit na password ang lahat ng account — hindi
// makaka-login ang kahit sino. Para maiwasan ito:
//   - Kung may kaparehong username sa KASALUKUYANG (bago pa i-restore)
//     listahan ng users, ipapasok ang KASALUKUYANG password hash nito
//     sa na-restore na record (ibig sabihin, hindi nagbabago ang
//     password ng mga existing account).
//   - Kung WALANG kaparehong username (bagong account mula sa backup,
//     hal. na-delete na sa kasalukuyan pero narestore mula sa cloud),
//     bibigyan ito ng RANDOM na temporary password at ida-DISABLE
//     (kung sino man ang gustong gumamit nito, kailangan munang i-reset
//     ng Admin ang password sa User Management).
// ====================================================================
function mergeRestoredUsers(restoredUsers) {
    const currentUsers = readData(FILE_USERS, []);
    const currentByUsername = new Map(
        currentUsers.map(u => [String(u.username || '').toLowerCase(), u])
    );
    const accountsNeedingPasswordReset = [];

    const merged = restoredUsers.map(record => {
        const clone = { ...record };
        const key = String(clone.username || '').toLowerCase();
        const existing = currentByUsername.get(key);
        if (existing && existing.password) {
            clone.password = existing.password; // panatilihin ang KASALUKUYANG password
        } else {
            // Walang kaparehong existing account — walang ligtas na password
            // na maipapasok, kaya random temporary password na lang, at
            // i-flag para malaman ng Admin na kailangan itong i-reset.
            clone.password = bcrypt.hashSync(crypto.randomBytes(16).toString('hex'), 10);
            accountsNeedingPasswordReset.push(clone.username);
        }
        return clone;
    });

    return { merged, accountsNeedingPasswordReset };
}

app.post('/api/cloud-backup/restore', requireFeature('cloud_backup'), rateLimit('cloud-backup-restore', 5, 15 * 60 * 1000), async (req, res) => {
    const { username, password } = req.body;

    // Parehong admin-auth pattern gaya ng /api/restore-backup — kailangan
    // ng Admin password dahil overwrite ito ng kasalukuyang data.
    const currentUsers = readData(FILE_USERS, []);
    const currentAdmin = currentUsers.find(u => u.username && username && u.username.toLowerCase() === username.toLowerCase() && u.role && u.role.toLowerCase() === 'admin');
    if (!currentAdmin || !bcrypt.compareSync(password || '', currentAdmin.password)) {
        return res.status(403).json({ success: false, code: 'WRONG_ADMIN_PASSWORD', message: 'Maling Admin password. Hindi pinahintulutan ang pag-restore.' });
    }

    if (getConnectivityMode() === 'offline') {
        return res.status(400).json({ success: false, message: 'Naka-OFFLINE mode ka ngayon. I-tap muna ang Online toggle para makapag-restore mula sa cloud.' });
    }
    if (!RELAY_API_KEY) {
        return res.status(500).json({ success: false, message: 'Walang RELAY_API_KEY na naka-configure sa .env.' });
    }

    try {
        const featureData = readFeatureUnlocks();
        const installationId = getOrCreateInstallationId(featureData);
        const hardwareFingerprint = computeHardwareFingerprint(featureData);

        const relayRes = await relayFetch(`${RELAY_URL}/relay/cloud-backup/restore`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-relay-key': RELAY_API_KEY },
            body: JSON.stringify({ installationId, hardwareFingerprint })
        });
        const relayData = await parseRelayResponse(relayRes);

        if (relayRes.status === 402 || relayData.featureLocked) {
            return res.status(402).json(relayData);
        }
        if (!relayData.success) {
            return res.status(relayRes.status || 502).json({ success: false, message: relayData.message || 'Tinanggihan ng RELAY ang cloud backup restore.' });
        }

        const modules = relayData.modules || {};
        let restoredCount = 0;
        const accountsNeedingPasswordReset = [];

        for (const [moduleName, data] of Object.entries(modules)) {
            if (moduleName === 'users' && Array.isArray(data)) {
                const { merged, accountsNeedingPasswordReset: needReset } = mergeRestoredUsers(data);
                writeData(moduleName, merged);
                accountsNeedingPasswordReset.push(...needReset);
                restoredCount++;
            } else if (Array.isArray(data) || (data && typeof data === 'object')) {
                writeData(moduleName, data);
                restoredCount++;
            }
        }

        logAction(username, `Nag-restore mula sa Cloud Backup (${restoredCount} modules, ${Object.keys(modules).length} kabuuan na-download mula sa RELAY).`);

        res.json({
            success: true,
            message: `Matagumpay na na-restore ang ${restoredCount} module(s) mula sa Cloud Backup.`,
            restoredCount,
            moduleNames: Object.keys(modules),
            accountsNeedingPasswordReset // ipaalam sa UI kung sinong accounts kailangang i-reset ang password
        });
    } catch (err) {
        console.error('⚠️ CLOUD_BACKUP: hindi na-abot ang relay para sa restore:', err.message);
        res.status(502).json({ success: false, message: 'Hindi ma-abot ang RELAY para sa cloud backup restore.' });
    }
});

function isDemoActive() {
    const data = readFeatureUnlocks();
    const installationId = getOrCreateInstallationId(data);
    const token = data.tokens[DEMO_FEATURE_ID];
    if (!token) return false;
    return verifyUnlockToken(token, installationId, DEMO_FEATURE_ID);
}

function getDemoExpiry() {
    const data = readFeatureUnlocks();
    const token = data.tokens[DEMO_FEATURE_ID];
    if (!token || !token.payload) return null;
    return typeof token.payload.expiresAt ==='number' ? token.payload.expiresAt : null;
}

function getPurchasedFeatureIds() {
    const data = readFeatureUnlocks();
    const installationId = getOrCreateInstallationId(data);
    return Object.keys(data.tokens)
        .filter(featureId => featureId !== DEMO_FEATURE_ID)
        .filter(featureId => verifyUnlockToken(data.tokens[featureId], installationId, featureId));
}

function getUnlockedFeatureIds() {
    const purchased = getPurchasedFeatureIds();

    if (isDemoActive()) {
        return Array.from(new Set([...Object.keys(FEATURE_CATALOG), ...purchased]));
    }
    return purchased;
}

// 'cloud_backup' ay SINASADYANG hindi kasama sa "Pro / fully unlocked"
// na konsepto — tingnan ang paliwanag sa itaas ng CLOUD_BACKUP_FEATURE_ID
// (malapit sa FEATURE_CATALOG) kung bakit ito hiwalay pinapresyuhan/
// ibinebenta sa upgrade options.
function isFullyProUnlocked() {
    const purchased = getPurchasedFeatureIds();
    const allIds = Object.keys(FEATURE_CATALOG).filter(id => id !== CLOUD_BACKUP_FEATURE_ID);
    return allIds.length > 0 && allIds.every(id => purchased.includes(id));
}

function requireFeature(featureId) {
    return (req, res, next) => {
        const unlockedIds = getUnlockedFeatureIds();
        if (unlockedIds.includes(featureId)) return next();
        const feature = FEATURE_CATALOG[featureId];
        const attemptCount = recordLockedAttempt();
        return res.status(402).json({
            success: false,
            featureLocked: true,
            featureId,
            featureName: feature ? feature.name : featureId,
            price: feature ? feature.price : null,
            description: feature ? feature.description : null,

            // 'cloud_backup' ay laging dapat gamitin ang SARILI/dedicated
            // niyang single-feature na unlock prompt (malinaw na presyo +
            // buong description bago mag-request), HINDI ang paminsan-
            // minsang bundled "Upgrade Options" tiers modal — dahil
            // ibang klase ang consent na kailangan dito (nagpapadala ito
            // ng buong database, kasama ang user accounts, papunta sa
            // cloud storage ng developer). Kaya laging false ang
            // showUpgradeTiers para dito, anuman ang attemptCount.
            showUpgradeTiers: featureId === CLOUD_BACKUP_FEATURE_ID
                ? false
                : (attemptCount > 0 && attemptCount % 2 === 0),
            message: `"${feature ? feature.name : featureId}" is a premium feature and is currently locked. Please unlock it (additional purchase required) to continue.`
        });
    };
}

function checkShiftManagementUnlocked() {
    const unlockedIds = getUnlockedFeatureIds();
    if (unlockedIds.includes('shift_management')) return { unlocked: true };
    const feature = FEATURE_CATALOG['shift_management'];
    const attemptCount = recordLockedAttempt();
    return {
        unlocked: false,
        body: {
            success: false,
            featureLocked: true,
            featureId:'shift_management',
            featureName: feature ? feature.name :'shift_management',
            price: feature ? feature.price : null,
            showUpgradeTiers: attemptCount > 0 && attemptCount % 2 === 0,
            message: `"${feature ? feature.name :'shift_management'}" is a premium feature and is currently locked. Please unlock it (additional purchase required) to continue.`
        }
    };
}

// --------------------------------------------------------------
// AUTO-RESTORE FROM RELAY — supports the "emergency hard reset" case
// (Users > Reset/Restore > System Hard Reset). featureUnlocks.tokens gets
// wiped on hard reset, BUT the installationId (derived from the hardware
// fingerprint) STAYS the same — so when this server checks in with
// RELAY again using the SAME installationId, RELAY can hand back the
// tokens it previously issued (no new OTP/payment required).
//
// This is called: (1) ONCE on server start (best-effort, not required
// for the app to start), and (2) on-demand via the
// /api/features/restore-check endpoint (the manual "Sync with Relay Now"
// button in Settings, for when the user doesn't want to restart the
// server).
// --------------------------------------------------------------
async function attemptRelayRestore() {
    if (!RELAY_API_KEY) return { attempted: false, restoredCount: 0, restoredFeatureIds: [] };
    // FIX: dating tumatawag pa rin ito sa RELAY kahit naka-Offline mode
    // ang user (sinusunod na ng ibang Relay functions ang toggle na ito,
    // pero hindi ito). Dahil tumatakbo ito every 30s (attemptRelayFeatureSync)
    // regardless, ito yung pinagmulan ng paulit-ulit na "Could not reach
    // Relay for auto-restore check" habang naka-Offline mode talaga.
    if (getConnectivityMode() === 'offline') {
        return { attempted: false, restoredCount: 0, restoredFeatureIds: [] };
    }
    // FIX: sinusunod din ngayon ang AKTWAL na internet status (hindi lang
    // ang manual na toggle) — kung "Online" pa rin ang toggle pero
    // talagang wala ngang internet ngayon, huwag nang subukan ang buong
    // relayFetch (20s) — mag-skip agad, tulad ng offline mode.
    if (!(await isInternetLikelyUp())) {
        return { attempted: false, restoredCount: 0, restoredFeatureIds: [] };
    }

    const data = readFeatureUnlocks();
    const installationId = getOrCreateInstallationId(data);

    try {
        const relayRes = await relayFetch(`${RELAY_URL}/relay/restore-tokens`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-relay-key': RELAY_API_KEY },
            body: JSON.stringify({ installationId })
        });
        const relayData = await parseRelayResponse(relayRes);

        if (!relayData.success || !relayData.tokens) {
            return { attempted: true, restoredCount: 0, restoredFeatureIds: [] };
        }

        let restoredCount = 0;
        const restoredFeatureIds = [];
        for (const [featureId, token] of Object.entries(relayData.tokens)) {
            // Skip anything already recorded in data.tokens (e.g. if it
            // wasn't actually reset, the restore has no effect on it).
            if (data.tokens[featureId] && verifyUnlockToken(data.tokens[featureId], installationId, featureId)) {
                continue;
            }
            if (!verifyUnlockToken(token, installationId, featureId)) continue; // safety: still verify the signature
            data.tokens[featureId] = token;
            restoredCount++;
            restoredFeatureIds.push(featureId);
        }

        if (restoredCount > 0) {
            writeData(FILE_FEATURE_UNLOCKS, data);
            logAction('System', `Automatically restored ${restoredCount} feature(s) from Relay (post-reset check-in).`);
        }

        return { attempted: true, restoredCount, restoredFeatureIds };
    } catch (err) {
        console.warn('⚠️  Could not reach Relay for auto-restore check:', err.message);
        return { attempted: true, restoredCount: 0, restoredFeatureIds: [], error: err.message };
    }
}

// Best-effort na tawag sa pag-start ng server — hindi nire-require na
// matagumpay ito bago tuloy-tuloy ang app (offline-friendly).
setTimeout(() => {
    attemptRelayRestore().catch(() => {});
}, 3000);

// --------------------------------------------------------------
// AUTO-LOCKDOWN FROM RELAY — part of the same manual "Sync with Relay
// Now" button, but the REVERSE of attemptRelayRestore() above: instead
// of ADDING back a previously unlocked feature, this DETERMINES which
// feature/theme that is currently LOCAL (we have a token for it here,
// meaning it was previously unlocked) is NO LONGER recognized by Relay
// right now — because it was deactivated by the developer/store owner
// in the admin panel, or its time-based license has expired.
//
// Why this needs to be checked separately from the local
// verifyUnlockToken(): an unlocked token here is a self-contained,
// signed proof (works even offline) — once it's deactivated on Relay,
// the local copy doesn't automatically become invalid, since its
// signature is still valid and it hasn't expired yet. It has to be
// EXPLICITLY checked against Relay (the only "source of truth" for
// whether an unlock is still genuine) to know whether it should be
// removed/locked here on the client as well.
// --------------------------------------------------------------
async function attemptRelayFeatureSync() {
    const restoreResult = await attemptRelayRestore();
    const restoredFeatureIds = restoreResult.restoredFeatureIds || [];

    if (!RELAY_API_KEY || getConnectivityMode() === 'offline' || !(await isInternetLikelyUp())) {
        return { attempted: restoreResult.attempted, restoredCount: restoreResult.restoredCount || 0, restoredFeatureIds, removedFeatures: [] };
    }

    // Re-read the latest contents of the file (attemptRelayRestore()
    // above may have added to it).
    const latestData = readFeatureUnlocks();
    const installationId = getOrCreateInstallationId(latestData);
    const localFeatureIds = Object.keys(latestData.tokens);

    const removedFeatures = [];

    try {
        const relayRes = await relayFetch(`${RELAY_URL}/relay/check-feature-status`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-relay-key': RELAY_API_KEY },
            // Ipinapasa pa rin ang call na ito kahit walang laman ang
            // localFeatureIds (dating skinip nang buo dati) — dahil dito
            // rin dumadaan ang "🔄 I-check ngayon" (forceIntegrityCheck)
            // na pindot ng admin sa integrity monitor, na dapat maabot
            // ng device kahit wala pa itong kahit isang naka-unlock na
            // feature.
            body: JSON.stringify({ installationId, featureIds: localFeatureIds })
        });
        const relayData = await parseRelayResponse(relayRes);

        if (relayData.success && relayData.forceIntegrityCheck) {
            // Hindi na hinihintay ang katapusan nito (fire-and-forget) —
            // ang layunin lang dito ay i-trigger AGAD ang integrity
            // check-in sa halip na hintayin pa ang normal na 55s/24h
            // schedule; ang resulta ay makikita pa rin sa admin panel sa
            // pamamagitan ng normal na /relay/integrity-checkin flow.
            runRelayIntegrityCheckin().catch(() => {});
        }

        if (relayData.success && relayData.statuses && typeof relayData.statuses === 'object') {
            for (const [featureId, info] of Object.entries(relayData.statuses)) {
                if (!info || info.status === 'active') continue;
                if (!latestData.tokens[featureId]) continue;

                delete latestData.tokens[featureId];
                removedFeatures.push({
                    featureId,
                    featureName: info.featureName || (FEATURE_CATALOG[featureId] && FEATURE_CATALOG[featureId].name) || featureId,
                    category: info.category || (FEATURE_CATALOG[featureId] && FEATURE_CATALOG[featureId].category) || 'module',
                    reason: info.reason === 'expired' ? 'expired' : 'deactivated'
                });
            }

            if (removedFeatures.length > 0) {
                writeData(FILE_FEATURE_UNLOCKS, latestData);
                const summary = removedFeatures.map(r => `${r.featureName} (${r.reason})`).join(', ');
                logAction('System', `Automatically locked ${removedFeatures.length} feature(s) after detecting they are no longer active on Relay (manual sync): ${summary}.`);
            }
        }
    } catch (err) {
        console.warn('⚠️  Could not reach Relay for the feature status check:', err.message);
    }

    return { attempted: true, restoredCount: restoreResult.restoredCount || 0, restoredFeatureIds, removedFeatures };
}

// --------------------------------------------------------------
// AUTO-SYNC INTERVAL (bagong dagdag) — dati, ang attemptRelayFeatureSync()
// sa itaas ay tumatakbo lang kapag: (1) pag-start ng server (restore-only,
// hindi pa lockdown check), o (2) pag-pindot ng user sa manual na
// "Sync with Relay Now" button. Ibig sabihin, kung may binago ang
// developer/store owner sa Relay admin panel (nag-deactivate ng feature,
// nag-expire ang isang license, o nag-restore), HINDI ito agad
// mapapansin ng OMNIPOS client hangga't hindi ito manually na-sync.
//
// Ngayon, tumatakbo na rin ito nang AWTOMATIKO paulit-ulit habang buhay/
// online ang OMNIPOS server, para halos real-time (ilang segundo lang ang
// delay) ang pag-react ng OMNIPOS sa anumang pagbabago sa Relay — hindi
// na kailangang hintayin pa ang manual sync. Kapag offline naman ang
// OMNIPOS (walang internet), tahimik lang itong nabibigo bawat tawag
// (naka-try/catch na sa loob mismo ng attemptRelayFeatureSync /
// attemptRelayRestore), kaya ligtas itong paulit-ulit na tinatawag.
// --------------------------------------------------------------
const RELAY_FEATURE_SYNC_INTERVAL_MS = Number(process.env.RELAY_FEATURE_SYNC_INTERVAL_MS) || 30 * 1000; // default: 30s
if (RELAY_API_KEY) {
    // Konting delay (10s) mula sa startup para makasunod muna sa unang
    // best-effort na attemptRelayRestore() sa itaas (3s mark).
    setTimeout(() => { attemptRelayFeatureSync().catch(() => {}); }, 10 * 1000);
    setInterval(() => { attemptRelayFeatureSync().catch(() => {}); }, RELAY_FEATURE_SYNC_INTERVAL_MS).unref();
}

app.post('/api/features/restore-check', rateLimit('feature-restore-check', 10, 10 * 60 * 1000), async (req, res) => {
    const result = await attemptRelayFeatureSync();

    // Split what actually came back from Relay into the demo session vs.
    // genuinely purchased features, so the client can show the correct
    // prompt instead of a generic "feature(s) restored" message.
    const restoredFeatureIds = result.restoredFeatureIds || [];
    const demoRestored = restoredFeatureIds.includes(DEMO_FEATURE_ID);
    const purchasedRestoredCount = restoredFeatureIds.filter(id => id !== DEMO_FEATURE_ID).length;

    const messageParts = [];
    if (purchasedRestoredCount > 0) {
        messageParts.push(`Restored ${purchasedRestoredCount} previously purchased feature(s).`);
    }
    if (demoRestored) {
        messageParts.push('Restored an active Demo Mode session for this device.');
    }
    if (result.removedFeatures.length > 0) {
        messageParts.push(`Locked ${result.removedFeatures.length} feature(s) that are no longer active on Relay (deactivated or expired).`);
    }
    if (messageParts.length === 0) {
        messageParts.push('Nothing newly restored — no configurations or previously unlocked features detected for this device.');
    }

    res.json({
        success: true,
        restoredCount: result.restoredCount,
        restoredFeatureIds,
        demoRestored,
        purchasedRestoredCount,
        removedFeatures: result.removedFeatures,
        message: messageParts.join(' '),
        unlockedFeatureIds: getUnlockedFeatureIds()
    });
});

app.get('/api/features/status', (req, res) => {
    res.json({
        success: true,
        unlockedFeatureIds: getUnlockedFeatureIds(),

        purchasedFeatureIds: getPurchasedFeatureIds(),
        fullyPurchased: isFullyProUnlocked()
    });
});

async function parseRelayResponse(relayRes) {
    const rawText = await relayRes.text();
    let parsed;
    try {
        parsed = rawText ? JSON.parse(rawText) : {};
    } catch (err) {
        throw new Error(
            `the relay returned an unexpected non-JSON response (HTTP ${relayRes.status}). ` +
            `This usually means the relay service isn't running, RELAY_URL is misconfigured, ` +
            `or the relay crashed and returned an error page instead of JSON.`
        );
    }
    if (!relayRes.ok && parsed.success === undefined) {
        throw new Error(parsed.message || `the relay responded with HTTP ${relayRes.status}.`);
    }
    return parsed;
}

// --------------------------------------------------------------
// Kapag tinanggihan ng Relay ang request dahil hindi pa naka-Allow ang
// device (unang beses palang gumawa ng request ang device na ito), HINDI
// natin ito dapat ituring na basta "error" — inaasahang pangyayari ito
// habang naghihintay pa lang ng authorization mula sa developer/store
// owner (nakikita na ito sa Relay admin panel bilang bagong "Recently
// Seen" device, madali na lang i-Allow doon). Kaya't gumagawa tayo ng
// hiwalay/mas magandang response shape (`pendingAuthorization: true`)
// para maipakita ng OMNIPOS client ang isang "wait for authorization"
// na prompt sa halip na error dialog.
function relayRejectionResponse(res, relayData, fallbackMessage) {
    if (relayData && relayData.deviceNotAllowed) {
        return res.status(403).json({
            success: false,
            pendingAuthorization: true,
            message: 'Naipadala na ang device na ito papuntang developer/store owner para sa authorization. Wala pang access ang device na ito — maghintay lang na ma-\"Allow\" ka sa Relay admin panel, at awtomatiko na itong susubukan ulit.'
        });
    }
    return res.status(502).json({ success: false, message: (relayData && relayData.message) || fallbackMessage });
}

app.post('/api/features/request-unlock', requirePermission('relay_unlock_request'), rateLimit('feature-unlock-request', 3, 10 * 60 * 1000), async (req, res) => {
    const { featureId, username, photo } = req.body;
    const feature = FEATURE_CATALOG[featureId];

    if (!feature) {
        return res.status(400).json({ success: false, message:'Unknown feature.' });
    }
    if (!RELAY_API_KEY) {
        return res.status(500).json({ success: false, message:'RELAY_API_KEY is not configured on this server. Please contact the developer.' });
    }

    const data = readFeatureUnlocks();
    const installationId = getOrCreateInstallationId(data);

    if (data.tokens[featureId] && verifyUnlockToken(data.tokens[featureId], installationId, featureId)) {
        return res.json({ success: true, alreadyUnlocked: true, message: `Naka-unlock na ang ${feature.name}.` });
    }

    try {
        const receiptSettings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);
        const relayRes = await relayFetch(`${RELAY_URL}/relay/request-unlock`, {
            method:'POST',
            headers: {'Content-Type':'application/json','x-relay-key': RELAY_API_KEY },
            body: JSON.stringify({
                installationId,
                featureId,
                featureName: feature.name,
                price: feature.price,
                username: username ||'Unknown',
                storeName: (receiptSettings && receiptSettings.storeName) || null,
                photo: photo || null
            })
        });
        const relayData = await parseRelayResponse(relayRes);

        if (!relayData.success) {
            return relayRejectionResponse(res, relayData, 'The unlock relay declined the request.');
        }

        logAction(username ||'Unknown', `Humiling ng OTP para i-unlock ang ${feature.name}`);
        res.json({ success: true, message:'The unlock request has been sent. Please wait for the confirmation code from the developer/owner.' });
    } catch (err) {
        console.error('Hindi ma-abot ang Unlock Relay:', err);
        res.status(502).json({ success: false, message: `Could not reach the unlock relay: ${err.message}. Please verify RELAY_URL and your internet connection.` });
    }
});

app.post('/api/features/confirm-unlock', rateLimit('feature-unlock-confirm', 120, 10 * 60 * 1000), async (req, res) => {
    const { featureId, otp, username } = req.body;
    const feature = FEATURE_CATALOG[featureId];

    if (!feature) {
        return res.status(400).json({ success: false, message:'Unknown feature.' });
    }
    if (!otp || !String(otp).trim()) {
        return res.status(400).json({ success: false, message:'The OTP code is required.' });
    }
    if (!RELAY_API_KEY) {
        return res.status(500).json({ success: false, message:'RELAY_API_KEY is not configured on this server. Please contact the developer.' });
    }

    const data = readFeatureUnlocks();
    const installationId = getOrCreateInstallationId(data);

    try {
        const relayRes = await relayFetch(`${RELAY_URL}/relay/confirm-unlock`, {
            method:'POST',
            headers: {'Content-Type':'application/json','x-relay-key': RELAY_API_KEY },
            body: JSON.stringify({ installationId, featureId, otp: String(otp).trim() })
        });
        const relayData = await parseRelayResponse(relayRes);

        if (relayData.pending) {
            return res.status(202).json({ success: false, pending: true, message: relayData.message || 'Tama ang code — naghihintay ng approval ng may-ari.' });
        }

        if (!relayData.success) {
            return res.status(400).json({ success: false, message: relayData.message ||'Failed to verify the code.' });
        }

        if (!verifyUnlockToken(relayData.token, installationId, featureId)) {
            console.error('⚠️ Natanggap ang isang token mula sa relay pero HINDI valid ang signature nito. Posibleng may problema sa RELAY_PUBLIC_KEY_PEM o kompromisado ang koneksyon.');
            return res.status(500).json({ success: false, message:'Hindi valid ang signature ng token na natanggap. Kontakin ang developer.' });
        }

        data.tokens[featureId] = relayData.token;
        writeData(FILE_FEATURE_UNLOCKS, data);
        logAction(username ||'Unknown', `Na-unlock ang feature: ${feature.name}`);

        res.json({ success: true, message: `${feature.name} has been unlocked!`, unlockedFeatureIds: getUnlockedFeatureIds() });
    } catch (err) {
        console.error('Hindi ma-abot ang Unlock Relay:', err);
        res.status(502).json({ success: false, message: `Could not reach the unlock relay: ${err.message}. Please verify RELAY_URL and your internet connection.` });
    }
});

app.post('/api/features/request-demo', requirePermission('relay_unlock_request'), rateLimit('feature-demo-request', 3, 10 * 60 * 1000), async (req, res) => {
    const { username, photo } = req.body;

    if (!RELAY_API_KEY) {
        return res.status(500).json({ success: false, message:'RELAY_API_KEY is not configured on this server. Please contact the developer.' });
    }

    const data = readFeatureUnlocks();
    const installationId = getOrCreateInstallationId(data);

    if (isDemoActive()) {
        return res.json({ success: true, alreadyActive: true, message:'Aktibo na ang Demo Mode.', demoExpiresAt: getDemoExpiry() });
    }

    try {
        const receiptSettings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);
        const relayRes = await relayFetch(`${RELAY_URL}/relay/request-demo`, {
            method:'POST',
            headers: {'Content-Type':'application/json','x-relay-key': RELAY_API_KEY },
            body: JSON.stringify({
                installationId,
                username: username ||'Unknown',
                storeName: (receiptSettings && receiptSettings.storeName) || null,
                photo: photo || null
            })
        });
        const relayData = await parseRelayResponse(relayRes);

        if (!relayData.success) {
            return relayRejectionResponse(res, relayData, 'The unlock relay declined the request.');
        }

        logAction(username ||'Unknown','Humiling ng OTP para sa Demo Mode');
        res.json({ success: true, message:'The demo request has been sent. Please wait for the confirmation code from the developer/owner.' });
    } catch (err) {
        console.error('Hindi ma-abot ang Unlock Relay (demo):', err);
        res.status(502).json({ success: false, message: `Could not reach the unlock relay: ${err.message}. Please verify RELAY_URL and your internet connection.` });
    }
});

app.post('/api/features/confirm-demo', rateLimit('feature-demo-confirm', 120, 10 * 60 * 1000), async (req, res) => {
    const { otp, username } = req.body;

    if (!otp || !String(otp).trim()) {
        return res.status(400).json({ success: false, message:'The OTP code is required.' });
    }
    if (!RELAY_API_KEY) {
        return res.status(500).json({ success: false, message:'RELAY_API_KEY is not configured on this server. Please contact the developer.' });
    }

    const data = readFeatureUnlocks();
    const installationId = getOrCreateInstallationId(data);

    try {
        const relayRes = await relayFetch(`${RELAY_URL}/relay/confirm-demo`, {
            method:'POST',
            headers: {'Content-Type':'application/json','x-relay-key': RELAY_API_KEY },
            body: JSON.stringify({ installationId, otp: String(otp).trim() })
        });
        const relayData = await parseRelayResponse(relayRes);

        if (relayData.pending) {
            return res.status(202).json({ success: false, pending: true, message: relayData.message || 'Tama ang code — naghihintay ng approval ng may-ari.' });
        }

        if (!relayData.success) {
            return res.status(400).json({ success: false, message: relayData.message ||'Failed to verify the code.' });
        }

        if (!verifyUnlockToken(relayData.token, installationId, DEMO_FEATURE_ID)) {
            console.error('⚠️ Natanggap ang isang demo token mula sa relay pero HINDI valid ang signature/expiry nito.');
            return res.status(500).json({ success: false, message:'Hindi valid ang signature ng token na natanggap. Kontakin ang developer.' });
        }

        data.tokens[DEMO_FEATURE_ID] = relayData.token;
        writeData(FILE_FEATURE_UNLOCKS, data);
        logAction(username ||'Unknown','Na-activate ang Demo Mode');

        res.json({
            success: true,
            message:'Demo Mode has been activated!',
            demoExpiresAt: getDemoExpiry(),
            unlockedFeatureIds: getUnlockedFeatureIds()
        });
    } catch (err) {
        console.error('Hindi ma-abot ang Unlock Relay (demo confirm):', err);
        res.status(502).json({ success: false, message: `Could not reach the unlock relay: ${err.message}. Please verify RELAY_URL and your internet connection.` });
    }
});

app.get('/api/features/demo-status', (req, res) => {
    const active = isDemoActive();
    res.json({
        success: true,
        demoActive: active,
        demoExpiresAt: active ? getDemoExpiry() : null,
        fullyPurchased: isFullyProUnlocked()
    });
});

app.post('/api/features/end-demo', async (req, res) => {
    if (!req.authUser || req.authUser.role.toLowerCase() !=='admin') {
        return res.status(403).json({ success: false, message:'Aksyon Tinanggihan: Admin privileges lamang ang pwedeng magtapos ng Demo Mode nang maaga.' });
    }
    const data = readFeatureUnlocks();
    if (!data.tokens[DEMO_FEATURE_ID]) {
        return res.json({ success: true, alreadyInactive: true, message:'Wala namang aktibong Demo Mode.' });
    }
    delete data.tokens[DEMO_FEATURE_ID];
    writeData(FILE_FEATURE_UNLOCKS, data);
    logAction(req.authUser.username,'Manual na tinapos ang Demo Mode bago pa man mag-expire.');

    // Sabihin din sa RELAY na tapos na ang demo na ito — kung LOKAL lang
    // ito (sa featureUnlocks.json ng OMNIPOS na ito) ang tatanggalin, may
    // NATITIRA pa ring record ng issued demo sa RELAY (issuedUnlocks)
    // hanggang sa mismong expiry nito. Kung mag-hard-reset o mag-restore
    // mula sa RELAY ang device bago mag-expire ang orihinal na demo,
    // maaaring "bumalik" pa ito kahit tinapos na ito nang maaga dito.
    // Kaya tinatawagan ang /relay/end-demo (self-service, hindi
    // admin-key) para TULUYANG matanggal ang demo entry sa RELAY mismo —
    // hindi na ito maibabalik pa kahit anong restore/check-in pa mangyari.
    // Hindi ito dapat harangin ang response papunta sa user kahit mabigo
    // ang RELAY call (offline man ang device, o down ang RELAY) — lokal
    // na tapos na ang demo, at "best-effort" lang ang RELAY-side cleanup.
    if (RELAY_API_KEY) {
        try {
            const installationId = getOrCreateInstallationId(data);
            const relayRes = await relayFetch(`${RELAY_URL}/relay/end-demo`, {
                method:'POST',
                headers: {'Content-Type':'application/json','x-relay-key': RELAY_API_KEY },
                body: JSON.stringify({ installationId })
            });
            const relayData = await relayRes.json().catch(() => null);
            if (!relayRes.ok || !relayData || !relayData.success) {
                console.warn('⚠️ END_DEMO: hindi na-confirm ng RELAY ang pagtatapos ng demo (lokal na tapos na ito pero maaaring "bumalik" pa mula sa RELAY sa susunod na restore):', relayData && relayData.message);
            }
        } catch (e) {
            console.warn('⚠️ END_DEMO: hindi na-abot ang RELAY para tuluyang tapusin ang demo doon:', e.message);
        }
    }

    res.json({
        success: true,
        message:'Demo Mode has been closed.',
        unlockedFeatureIds: getUnlockedFeatureIds(),
        fullyPurchased: isFullyProUnlocked()
    });
});

app.get('/api/features/upgrade-catalog', (req, res) => {
    const alreadyPurchased = getPurchasedFeatureIds();
    // 'cloud_backup' ay laging tinatanggal dito — hindi ito dapat lumabas
    // bilang isa pang à la carte checkbox sa pangkalahatang "Upgrade
    // Options" modal. Ang presyo/pag-unlock nito ay dapat laging dumaan
    // sa sarili nitong dedicated prompt (tingnan ang requireFeature() at
    // ang showUpgradeTiers override para sa CLOUD_BACKUP_FEATURE_ID sa
    // itaas ng file na ito).
    const features = Object.keys(FEATURE_CATALOG)
        .filter(id => id !== CLOUD_BACKUP_FEATURE_ID)
        .map(id => ({ id, ...FEATURE_CATALOG[id] }));
    const tiers = UPGRADE_TIERS.map(tier => {
        const { discount, effectivePrice } = getTierPricing(tier, alreadyPurchased);
        const remainingFeatureIds = tier.featureIds.filter(id => !alreadyPurchased.includes(id));
        return {
            id: tier.id,
            name: tier.name,
            description: tier.description,
            featureIds: tier.featureIds,
            alaCartePrice: sumFeaturePrices(remainingFeatureIds),
            bundlePrice: tier.bundlePrice,
            bundleSavings: discount,
            effectiveBundlePrice: effectivePrice
        };
    });
    res.json({ success: true, features, tiers });
});

app.post('/api/features/request-unlock-bulk', requirePermission('relay_unlock_request'), rateLimit('feature-unlock-bulk-request', 3, 10 * 60 * 1000), async (req, res) => {
    const { featureIds, tierId, username, photo } = req.body;

    if (!Array.isArray(featureIds) || featureIds.length === 0) {
        return res.status(400).json({ success: false, message:'featureIds must be a non-empty array.' });
    }
    const unknown = featureIds.filter(id => !FEATURE_CATALOG[id]);
    if (unknown.length) {
        return res.status(400).json({ success: false, message: `Unknown feature(s): ${unknown.join(', ')}` });
    }
    // Server-side enforcement (hindi lang UI-level): kahit ma-craft man
    // ng client ang request nito nang direkta, hindi dapat makadaan ang
    // 'cloud_backup' sa bulk/bundle na landas. Kailangan itong i-request
    // nang mag-isa sa pamamagitan ng /api/features/request-unlock (tingnan
    // ang promptUnlockFeature() sa app.js).
    if (featureIds.includes(CLOUD_BACKUP_FEATURE_ID)) {
        return res.status(400).json({ success: false, message:'Ang Cloud Backup ay hiwalay na pinoproseso — gamitin ang sarili nitong "Get Cloud Backup" na prompt, hindi ang bundle/tier na unlock.' });
    }
    if (!RELAY_API_KEY) {
        return res.status(500).json({ success: false, message:'RELAY_API_KEY is not configured on this server. Please contact the developer.' });
    }

    const data = readFeatureUnlocks();
    const installationId = getOrCreateInstallationId(data);

    const alreadyPurchased = getPurchasedFeatureIds();
    const stillLocked = featureIds.filter(id => !alreadyPurchased.includes(id));
    if (stillLocked.length === 0) {
        return res.json({ success: true, alreadyUnlocked: true, message:'Naka-unlock na ang lahat ng napili.' });
    }

    // Kung pumili ang client ng isang PACKAGE/TIER (hindi à la carte), gamitin
    // ang PROPORTIONAL na presyo ng tier para sa mga natitirang naka-lock na
    // item (tingnan ang getTierPricing() sa itaas) — huwag basta i-sum ang
    // mga à la carte na presyo, dahil doon nanggagaling ang dating bug kung
    // saan hindi tugma ang presyong lumalabas sa OTP email sa presyong
    // nakita ng client sa Upgrade modal. Ang discounted na presyo ay VALID
    // lang kung eksaktong tumutugma ang (still-locked) na featureIds sa
    // (still-locked) na featureIds ng tier na sinasabing pinili — kung hindi
    // tugma (i.e. pinalitan/dinagdagan ng request ang listahan), bumalik sa
    // à la carte sum bilang ligtas na default.
    let totalPrice = sumFeaturePrices(stillLocked);
    if (tierId) {
        const tier = UPGRADE_TIERS.find(t => t.id === tierId);
        if (tier) {
            const tierStillLocked = tier.featureIds.filter(id => !alreadyPurchased.includes(id));
            const sameSet = tierStillLocked.length === stillLocked.length &&
                tierStillLocked.every(id => stillLocked.includes(id));
            if (sameSet) {
                totalPrice = getTierPricing(tier, alreadyPurchased).effectivePrice;
            }
        }
    }

    try {
        const receiptSettings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);
        const relayRes = await relayFetch(`${RELAY_URL}/relay/request-unlock-bulk`, {
            method:'POST',
            headers: {'Content-Type':'application/json','x-relay-key': RELAY_API_KEY },
            body: JSON.stringify({
                installationId,
                featureIds: stillLocked,
                featureNames: stillLocked.map(id => FEATURE_CATALOG[id].name),
                totalPrice,
                username: username ||'Unknown',
                storeName: (receiptSettings && receiptSettings.storeName) || null,
                photo: photo || null
            })
        });
        const relayData = await parseRelayResponse(relayRes);

        if (!relayData.success) {
            return relayRejectionResponse(res, relayData, 'The unlock relay declined the request.');
        }

        logAction(username ||'Unknown', `Humiling ng OTP para i-unlock ang ${stillLocked.length} feature(s) (₱${totalPrice})`);
        res.json({ success: true, message:'The bundle unlock request has been sent. Please wait for the confirmation code.', totalPrice, featureIds: stillLocked });
    } catch (err) {
        console.error('Hindi ma-abot ang Unlock Relay (bulk):', err);
        res.status(502).json({ success: false, message: `Could not reach the unlock relay: ${err.message}. Please verify RELAY_URL and your internet connection.` });
    }
});

app.post('/api/features/confirm-unlock-bulk', rateLimit('feature-unlock-bulk-confirm', 120, 10 * 60 * 1000), async (req, res) => {
    const { featureIds, otp, username } = req.body;

    if (!Array.isArray(featureIds) || featureIds.length === 0) {
        return res.status(400).json({ success: false, message:'featureIds must be a non-empty array.' });
    }
    if (!otp || !String(otp).trim()) {
        return res.status(400).json({ success: false, message:'The OTP code is required.' });
    }
    if (!RELAY_API_KEY) {
        return res.status(500).json({ success: false, message:'RELAY_API_KEY is not configured on this server. Please contact the developer.' });
    }

    const data = readFeatureUnlocks();
    const installationId = getOrCreateInstallationId(data);

    try {
        const relayRes = await relayFetch(`${RELAY_URL}/relay/confirm-unlock-bulk`, {
            method:'POST',
            headers: {'Content-Type':'application/json','x-relay-key': RELAY_API_KEY },
            body: JSON.stringify({ installationId, featureIds, otp: String(otp).trim() })
        });
        const relayData = await parseRelayResponse(relayRes);

        if (relayData.pending) {
            return res.status(202).json({ success: false, pending: true, message: relayData.message || 'Tama ang code — naghihintay ng approval ng may-ari.' });
        }

        if (!relayData.success) {
            return res.status(400).json({ success: false, message: relayData.message ||'Failed to verify the code.' });
        }

        const tokens = relayData.tokens || {};
        for (const featureId of featureIds) {
            const token = tokens[featureId];
            if (!token || !verifyUnlockToken(token, installationId, featureId)) {
                console.error(`⚠️ Invalid/missing token mula sa relay para sa ${featureId} (bulk confirm).`);
                return res.status(500).json({ success: false, message: `Hindi valid ang token na natanggap para sa ${featureId}. Kontakin ang developer.` });
            }
        }
        featureIds.forEach(featureId => { data.tokens[featureId] = tokens[featureId]; });
        writeData(FILE_FEATURE_UNLOCKS, data);
        logAction(username ||'Unknown', `Na-unlock ang ${featureIds.length} feature(s) via bundle`);

        res.json({ success: true, message:'Bundle unlocked!', unlockedFeatureIds: getUnlockedFeatureIds() });
    } catch (err) {
        console.error('Hindi ma-abot ang Unlock Relay (bulk confirm):', err);
        res.status(502).json({ success: false, message: `Could not reach the unlock relay: ${err.message}. Please verify RELAY_URL and your internet connection.` });
    }
});

app.get('/api/themes/status', (req, res) => {
    const unlockedFeatureIds = getUnlockedFeatureIds();
    const unlockedThemeIds = unlockedFeatureIds.filter(id => FEATURE_CATALOG[id] && FEATURE_CATALOG[id].category ==='theme');
    res.json({ success: true, unlockedThemeIds });
});

app.post('/api/themes/request-unlock', requirePermission('relay_unlock_request'), rateLimit('theme-unlock-request', 3, 10 * 60 * 1000), async (req, res) => {
    const { themeId, username, photo } = req.body;
    const theme = FEATURE_CATALOG[themeId];

    if (!theme || theme.category !=='theme') {
        return res.status(400).json({ success: false, message:'Unknown Pro theme.' });
    }
    if (!RELAY_API_KEY) {
        return res.status(500).json({ success: false, message:'RELAY_API_KEY is not configured on this server. Please contact the developer.' });
    }

    const data = readFeatureUnlocks();
    const installationId = getOrCreateInstallationId(data);

    if (data.tokens[themeId] && verifyUnlockToken(data.tokens[themeId], installationId, themeId)) {
        return res.json({ success: true, alreadyUnlocked: true, message: `Naka-unlock na ang ${theme.name}.` });
    }

    try {
        const receiptSettings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);
        const relayRes = await relayFetch(`${RELAY_URL}/relay/request-unlock`, {
            method:'POST',
            headers: {'Content-Type':'application/json','x-relay-key': RELAY_API_KEY },
            body: JSON.stringify({
                installationId,
                featureId: themeId,
                featureName: theme.name,
                price: theme.price,
                username: username ||'Unknown',
                storeName: (receiptSettings && receiptSettings.storeName) || null,
                photo: photo || null
            })
        });
        const relayData = await parseRelayResponse(relayRes);

        if (!relayData.success) {
            return relayRejectionResponse(res, relayData, 'The unlock relay declined the request.');
        }

        logAction(username ||'Unknown', `Humiling ng OTP para i-unlock ang ${theme.name}`);
        res.json({ success: true, message:'The unlock request has been sent. Please wait for the confirmation code from the developer/owner.' });
    } catch (err) {
        console.error('Hindi ma-abot ang Unlock Relay:', err);
        res.status(502).json({ success: false, message: `Could not reach the unlock relay: ${err.message}. Please verify RELAY_URL and your internet connection.` });
    }
});

app.post('/api/themes/confirm-unlock', rateLimit('theme-unlock-confirm', 120, 10 * 60 * 1000), async (req, res) => {
    const { themeId, otp, username } = req.body;
    const theme = FEATURE_CATALOG[themeId];
    if (!theme || theme.category !=='theme') {
        return res.status(400).json({ success: false, message:'Unknown Pro theme.' });
    }
    if (!otp || !String(otp).trim()) {
        return res.status(400).json({ success: false, message:'The OTP code is required.' });
    }
    if (!RELAY_API_KEY) {
        return res.status(500).json({ success: false, message:'RELAY_API_KEY is not configured on this server. Please contact the developer.' });
    }

    const data = readFeatureUnlocks();
    const installationId = getOrCreateInstallationId(data);

    try {
        const relayRes = await relayFetch(`${RELAY_URL}/relay/confirm-unlock`, {
            method:'POST',
            headers: {'Content-Type':'application/json','x-relay-key': RELAY_API_KEY },
            body: JSON.stringify({ installationId, featureId: themeId, otp: String(otp).trim() })
        });
        const relayData = await parseRelayResponse(relayRes);

        if (relayData.pending) {
            return res.status(202).json({ success: false, pending: true, message: relayData.message || 'Tama ang code — naghihintay ng approval ng may-ari.' });
        }

        if (!relayData.success) {
            return res.status(400).json({ success: false, message: relayData.message ||'Failed to verify the code.' });
        }
        if (!verifyUnlockToken(relayData.token, installationId, themeId)) {
            console.error('⚠️ Natanggap ang isang token mula sa relay pero HINDI valid ang signature nito.');
            return res.status(500).json({ success: false, message:'Hindi valid ang signature ng token na natanggap. Kontakin ang developer.' });
        }

        data.tokens[themeId] = relayData.token;
        writeData(FILE_FEATURE_UNLOCKS, data);
        logAction(username ||'Unknown', `Na-unlock ang Pro theme: ${theme.name}`);

        const unlockedThemeIds = getUnlockedFeatureIds().filter(id => FEATURE_CATALOG[id] && FEATURE_CATALOG[id].category ==='theme');
        res.json({ success: true, message: `${theme.name} has been unlocked!`, unlockedThemeIds });
    } catch (err) {
        console.error('Hindi ma-abot ang Unlock Relay:', err);
        res.status(502).json({ success: false, message: `Could not reach the unlock relay: ${err.message}. Please verify RELAY_URL and your internet connection.` });
    }
});

function getPHTime() {
    return new Date().toLocaleString("sv-SE", { timeZone:"Asia/Manila" });
}

const defaultUsers = [
    { username:'admin', password:'admin', role:'Admin', created:'2026-07-07 04:32:09' },
    { username:'cashier1', password:'cashier123', role:'Cashier', created:'2026-07-07 18:30:20' },
    { username:'staff', password:'staff123', role:'Staff', created:'2026-07-07 16:34:41' }
];

// Tinutukoy dito ang petsa ng expiry RELATIVE sa mismong sandali ng
// unang pag-launch ng bagong client (hindi naka-hardcode na absolute
// date) — para kahit kailan pa i-extract/i-deploy ito ng bagong
// kliyente, laging "makatotohanan" (hindi agad EXPIRED) ang mga sample
// na petsa, at para makita rin nila agad ang "Expiring Soon" na feature
// sa dashboard gamit ang Fresh Milk (default: 5 araw na lang bago
// mag-expire, loob ng 0–7 araw na saklaw ng "Expiring Soon" sa UI).
function daysFromNow(days) {
    const d = new Date();
    d.setDate(d.getDate() + days);
    return d.toISOString().split('T')[0]; // YYYY-MM-DD — tugma sa <input type="date">
}

const defaultProducts = [
    { code:'PRDT20250001', name:'Bottled Water 500ml', category:'Beverages', price: 25.00, stock: 8, cost: 15.00, supplier:'Absolute Distribution', expiryDate: daysFromNow(180), lowStockThreshold: 5 },
    { code:'PRDT20250002', name:'Coca-Cola 1L', category:'Beverages', price: 55.00, stock: 9, cost: 42.00, supplier:'Coca-Cola Beverages Philippines, Inc.', expiryDate: daysFromNow(150), lowStockThreshold: 5 },
    { code:'PRDT20250003', name:'Fresh Milk 1L', category:'Dairy', price: 85.00, stock: 10, cost: 65.00, supplier:'Local Dairy Supplier', expiryDate: daysFromNow(5), lowStockThreshold: 4 },
    { code:'PRDT20250004', name:'Nova Multigrain', category:'Snacks', price: 30.00, stock: 10, cost: 22.00, supplier:'Universal Robina Corporation', expiryDate: daysFromNow(90), lowStockThreshold: 5 },
    { code:'PRDT20250005', name:'Piattos Cheese', category:'Snacks', price: 35.00, stock: 10, cost: 26.00, supplier:'Universal Robina Corporation', expiryDate: daysFromNow(90), lowStockThreshold: 5 }
];

if (readData(FILE_USERS).length === 0) {
    const secureDefaultUsers = defaultUsers.map(u => ({
        ...u,
        password: bcrypt.hashSync(u.password, 10)
    }));
    writeData(FILE_USERS, secureDefaultUsers);
}
if (readData(FILE_PRODUCTS).length === 0) writeData(FILE_PRODUCTS, defaultProducts);

function verifyAdmin(req, res, next) {

    const { username, adminPassword } = req.body;

    if (!username) {
        return res.status(400).json({ success: false, message:'May kulang na impormasyon (Username required).' });
    }

    const users = readData(FILE_USERS);
    const activeUser = users.find(u => u.username.toLowerCase() === username.toLowerCase());

    if (!activeUser || activeUser.role.toLowerCase() !=='admin') {
        return res.status(403).json({
            success: false,
            message:'Akses Denied: Ang account na ito ay walang sapat na pribilehiyo bilang Admin!'
        });
    }

    if (!adminPassword) {
        return res.status(400).json({ success: false, message:'Kailangan ng Admin password para sa aksyong ito.' });
    }

    let isPasswordCorrect = false;
    try {
        isPasswordCorrect = bcrypt.compareSync(adminPassword, activeUser.password);
    } catch (e) {
        isPasswordCorrect = (adminPassword === activeUser.password);
    }

    if (!isPasswordCorrect) {

        return res.status(403).json({ success: false, code:'WRONG_ADMIN_PASSWORD', message:'Maling Admin Password. Hindi pinahintulutan ang aksyong ito.' });
    }

    next();
}

app.get('/api/roles', (req, res) => {
    res.json({ success: true, roles: getRoles(), menuRegistry: MENU_REGISTRY });
});

app.post('/api/roles', requireFeature('rbac_management'), verifyAdmin, (req, res) => {
    const { roleName, permissions } = req.body;
    if (!roleName || typeof roleName !=='string' || !roleName.trim()) {
        return res.status(400).json({ success: false, message:'Kailangan ng pangalan ng role.' });
    }
    const name = roleName.trim();
    if (name.toLowerCase() ==='admin') {
        return res.status(403).json({ success: false, message:'Nakareserba ang pangalang "Admin" bilang huling super-admin ng system.' });
    }
    if (!permissions || typeof permissions !=='object') {
        return res.status(400).json({ success: false, message:'Kulang ang permissions data.' });
    }

    let roles = getRoles();

    const normalizedPerms = {};
    MENU_REGISTRY.forEach(m => { normalizedPerms[m.key] = !!permissions[m.key]; });

    const existingIdx = roles.findIndex(r => r.name.toLowerCase() === name.toLowerCase());
    if (existingIdx !== -1) {
        if (roles[existingIdx].protected) {
            return res.status(403).json({ success: false, message: `Hindi maaaring baguhin ang "${roles[existingIdx].name}" role.` });
        }
        roles[existingIdx].permissions = normalizedPerms;
    } else {
        roles.push({ name, protected: false, permissions: normalizedPerms });
    }
    writeData(FILE_ROLES, roles);
    logAction(req.authUser.username, `Na-update ang permissions ng role: ${name}`);
    res.json({ success: true, roles });
});

app.post('/api/roles/reorder', requireFeature('rbac_management'), verifyAdmin, (req, res) => {
    const { orderedRoleNames } = req.body;
    if (!Array.isArray(orderedRoleNames) || !orderedRoleNames.length) {
        return res.status(400).json({ success: false, message:'Kailangan ng listahan ng roles sa bagong pagkakasunod-sunod.' });
    }

    let roles = getRoles();

    const currentNamesLower = roles.map(r => r.name.toLowerCase()).sort();
    const requestedNamesLower = orderedRoleNames.map(n => (n ||'').toLowerCase()).sort();
    const sameSet = currentNamesLower.length === requestedNamesLower.length &&
        currentNamesLower.every((n, i) => n === requestedNamesLower[i]);

    if (!sameSet) {
        return res.status(400).json({ success: false, message:'Hindi tugma ang listahan ng roles — baka may role na nadagdag/nabura habang nagre-reorder.' });
    }

    const roleByLowerName = new Map(roles.map(r => [r.name.toLowerCase(), r]));
    const reordered = orderedRoleNames.map(n => roleByLowerName.get(n.toLowerCase()));

    writeData(FILE_ROLES, reordered);
    logAction(req.body.username ||'Unknown', `Binago ang pagkakasunod-sunod ng Role columns sa Permission Matrix: ${orderedRoleNames.join(' → ')}`);
    res.json({ success: true, roles: reordered });
});

app.post('/api/roles/delete', requireFeature('rbac_management'), verifyAdmin, (req, res) => {
    const { roleName } = req.body;
    let roles = getRoles();
    const role = roles.find(r => r.name.toLowerCase() === (roleName ||'').toLowerCase());
    if (!role) return res.status(404).json({ success: false, message:'Role not found.' });
    if (role.protected) return res.status(403).json({ success: false, message:'Hindi maaaring burahin ang Admin role.' });

    const users = readData(FILE_USERS);
    const inUse = users.some(u => u.role.toLowerCase() === role.name.toLowerCase());
    if (inUse) {
        return res.status(409).json({ success: false, message: `May mga user pa na naka-assign sa role na "${role.name}". I-reassign muna sila bago ito burahin.` });
    }

    roles = roles.filter(r => r.name.toLowerCase() !== role.name.toLowerCase());
    writeData(FILE_ROLES, roles);
    logAction(req.authUser.username, `Binura ang role: ${role.name}`);
    res.json({ success: true, roles });
});

const LOGIN_RATE_LIMIT_WINDOW_MS = 10 * 60 * 1000;

// ====================================================================
// TWO-FACTOR AUTHENTICATION (2FA) — OTP via email, Admin login lamang
// ====================================================================
// Kapag naka-ON ang "twoFactorLoginEnabled" sa Advanced Settings, ang mga
// account na may role na "Admin" ay dadaan muna sa isang OTP step bago
// tuluyang makapasok, matapos tumugma ang username/password. In-memory
// lang ang pending challenges (parang WEBAUTHN_LOGIN_CHALLENGES sa ibaba)
// — hindi na kailangang i-persist ito sa DB dahil maikli lang ang buhay
// nito (OTP_TTL_MS = 10 minuto) at isang beses lang gagamitin.
const LOGIN_OTP_TTL_MS = 10 * 60 * 1000;
const LOGIN_OTP_CHALLENGES = new Map(); // loginToken -> { username, role, code, expiresAt, userAgent, ip }

setInterval(() => {
    const now = Date.now();
    for (const [k, v] of LOGIN_OTP_CHALLENGES.entries()) if (now > v.expiresAt) LOGIN_OTP_CHALLENGES.delete(k);
}, 60 * 1000).unref();

function generateLoginToken() {
    return 'LOGIN-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 10);
}

app.post('/api/auth/login', async (req, res) => {
    const { username, password } = req.body;

    if (!username || !password) {
        return res.status(400).json({
            success: false,
            message:'Please enter your username and password.'
        });
    }

    // Tingnan lang muna kung na-lock na (walang binabago) — para sumagot
    // agad kung sobra na talaga ang attempts.
    if (!checkLoginRateLimit(req, res, 5, 30, LOGIN_RATE_LIMIT_WINDOW_MS)) {
        return; // 429 na ang naipadala na ni checkLoginRateLimit
    }

    // ANTI-CLONE GATE: kailangan munang maka-verify online sa RELAY (unang
    // beses, o kapag nagbago ang hardware fingerprint) bago tuluyang
    // suriin ang username/password.
    //
    // FIX: HINDI na natin nire-record dito ang attempt (tingnan sa ibaba)
    // — kung ito ay bumagsak dahil lang timeout/unreachable ang Relay
    // (hal. cold-start ng Render, walang internet), hindi ito dapat
    // kumonsumo ng quota, dahil hindi pa nga natin nasusuri ang
    // username/password.
    const deviceCheck = await checkDeviceBeforeLogin({ username });
    if (!deviceCheck.allowed) {
        return res.status(403).json({
            success: false,
            deviceBlocked: true,
            message: deviceCheck.message
        });
    }

    // Pumasa na sa device-check — dito lang natin ire-record ang attempt,
    // dahil dito na talaga tayo susuri ng totoong username/password.
    recordLoginAttempt(req, LOGIN_RATE_LIMIT_WINDOW_MS);

    let users = readData(FILE_USERS);

    let userIndex = users.findIndex(u => u.username.trim().toLowerCase() === username.trim().toLowerCase());

    if (userIndex !== -1) {
        const user = users[userIndex];
        let isMatch = false;
        let needsUpgrade = false;

        try {

            isMatch = bcrypt.compareSync(password, user.password);
        } catch (error) {

            isMatch = (password === user.password);
            if (isMatch) needsUpgrade = true;
        }

        if (isMatch) {

            if (needsUpgrade) {
                user.password = bcrypt.hashSync(password, 10);
                writeData(FILE_USERS, users);
            }

            // 2FA GATE: kung naka-enable ang Two-Factor Authentication sa
            // Advanced Settings AT "Admin" ang role ng account na ito,
            // huwag munang bigyan ng session token — magpadala muna ng
            // OTP sa naka-configure na recipient email, at hintayin ang
            // /api/auth/login/verify-otp bago tuluyang mag-issue ng token.
            const advSettingsForLogin = getAdvancedSettingsPublic(readData(FILE_ADVANCED_SETTINGS, DEFAULT_ADVANCED_SETTINGS));
            const isAdminRole = (user.role || '').toLowerCase() === 'admin';

            if (advSettingsForLogin.twoFactorLoginEnabled && isAdminRole) {
                const otpMailCreds = getOtpMailCredentials();
                if (!otpMailCreds) {
                    // Walang OTP sender na naka-configure (Receipt Customization
                    // panel) — huwag i-block ang Admin sa sarili nilang system,
                    // sa halip ay tuluyan na lang paloginin at i-warn na lang.
                    console.warn('⚠️ Naka-enable ang 2FA Admin Login pero walang naka-configure na OTP sender email — nag-proceed nang walang OTP.');
                } else {
                    const otpCode = String(Math.floor(100000 + Math.random() * 900000));
                    const loginToken = generateLoginToken();
                    LOGIN_OTP_CHALLENGES.set(loginToken, {
                        username: user.username,
                        role: user.role,
                        code: otpCode,
                        expiresAt: Date.now() + LOGIN_OTP_TTL_MS,
                        userAgent: req.headers['user-agent'],
                        ip: getClientIp(req)
                    });

                    try {
                        await sendMailSmart(otpMailCreds.user, otpMailCreds.pass, {
                            from: `"OmniPOS Security" <${otpMailCreds.user}>`,
                            to: advSettingsForLogin.twoFactorRecipientEmail,
                            subject: `🔐 OmniPOS: Admin Login OTP (${user.username})`,
                            text: `May sumusubok mag-login sa OmniPOS Admin account "${user.username}".\n\n` +
                                  `OTP Code: ${otpCode}\n` +
                                  `Mag-e-expire ito sa loob ng 10 minuto.\n\n` +
                                  `Kung hindi ninyo ito hiniling, huwag ibigay ang code na ito at i-check ang inyong password.`
                        });
                    } catch (mailErr) {
                        console.error('2FA login OTP send failure:', mailErr.message);
                        return res.status(500).json({ success: false, message: `Nabigo ang pagpapadala ng 2FA OTP: ${mailErr.message}` });
                    }

                    return res.json({
                        success: true,
                        requiresOtp: true,
                        loginToken,
                        message: `Naipadala ang 6-digit na OTP sa naka-configure na email. Ilagay ito para makumpleto ang login.`
                    });
                }
            }

            logAction(user.username, `Logged into the system`);

            const token = createSession(user.username, user.role, req.headers['user-agent'], getClientIp(req));

            const permissions = getPermissionsForRole(user.role);
            return res.json({ success: true, user: { username: user.username, role: user.role, avatar: user.avatar || null }, token, permissions, menuRegistry: MENU_REGISTRY });
        }
    }

    res.status(401).json({ success: false, message:'Incorrect username or password.' });
});

// Ikalawang hakbang ng 2FA login: i-verify ang OTP code na ipinadala sa
// email, at kapag tama, saka lang mag-iissue ng totoong session token.
app.post('/api/auth/login/verify-otp', rateLimit('login-verify-otp', 8, 10 * 60 * 1000), (req, res) => {
    const { loginToken, otp } = req.body;

    if (!loginToken || !otp) {
        return res.status(400).json({ success: false, message: 'Kailangan ang loginToken at OTP code.' });
    }

    const pending = LOGIN_OTP_CHALLENGES.get(loginToken);
    if (!pending) {
        return res.status(400).json({ success: false, code: 'OTP_EXPIRED', message: 'Expired na o walang aktibong login OTP request. Mag-login ulit para makahingi ng bagong OTP.' });
    }
    if (Date.now() > pending.expiresAt) {
        LOGIN_OTP_CHALLENGES.delete(loginToken);
        return res.status(400).json({ success: false, code: 'OTP_EXPIRED', message: 'Expired na ang OTP code. Mag-login ulit para makahingi ng bago.' });
    }
    if (String(otp).trim() !== pending.code) {
        return res.status(403).json({ success: false, code: 'WRONG_OTP', message: 'Maling OTP code.' });
    }

    LOGIN_OTP_CHALLENGES.delete(loginToken);

    const users = readData(FILE_USERS);
    const user = users.find(u => u.username.toLowerCase() === pending.username.toLowerCase());
    if (!user) {
        return res.status(404).json({ success: false, message: 'Hindi na umiiral ang account na ito.' });
    }

    logAction(user.username, `Logged into the system (2FA-verified)`);

    const token = createSession(user.username, user.role, pending.userAgent, pending.ip);
    const permissions = getPermissionsForRole(user.role);

    res.json({ success: true, user: { username: user.username, role: user.role, avatar: user.avatar || null }, token, permissions, menuRegistry: MENU_REGISTRY });
});

// ====================================================================
// FINGERPRINT / BIOMETRIC LOGIN (WebAuthn — platform authenticator)
// ====================================================================
// MAHALAGA: HINDI ito nagbabasa/naka-imbak ng anumang aktwal na
// fingerprint scan. Ang biometric scan mismo (fingerprint/Face ID) ay
// ginagawa at pinananatili ng OS ng mobile phone — dito lang tayo
// naka-imbak ng isang PUBLIC KEY na ni-release ng phone matapos
// matagumpay na ma-verify ang may-ari nito (parang "digital na
// susi" na naka-lock sa loob ng device, hindi ang fingerprint mismo).
//
// Dahil ang navigator.credentials API ay kailangan ng "secure context"
// (HTTPS, o eksaktong "localhost"), gagana lang ang feature na ito
// kapag ang OmniPOS ay binuksan mismo sa parehong telepono na
// pinaghohostan nito (http://localhost:3000) o sa cloud (HTTPS/Render).
// Kaya nga "sa mobile device lang" makikita ang setting — tama ito.
//
// Dalawang flow:
//   1. REGISTRATION (habang naka-login na) — nagpapa-enroll ng
//      fingerprint bilang alternatibong paraan ng pag-login SA DEVICE
//      NA ITO. /register-options -> /register-verify
//   2. LOGIN (bago pa naka-login) — /login-options -> /login-verify
// ====================================================================

const WEBAUTHN_CHALLENGE_TTL_MS = 2 * 60 * 1000;
const WEBAUTHN_REGISTER_CHALLENGES = new Map(); // username(lowercase) -> { challenge, expiresAt }
const WEBAUTHN_LOGIN_CHALLENGES = new Map();    // challenge -> { username, expiresAt }

setInterval(() => {
    const now = Date.now();
    for (const [k, v] of WEBAUTHN_REGISTER_CHALLENGES.entries()) if (now > v.expiresAt) WEBAUTHN_REGISTER_CHALLENGES.delete(k);
    for (const [k, v] of WEBAUTHN_LOGIN_CHALLENGES.entries()) if (now > v.expiresAt) WEBAUTHN_LOGIN_CHALLENGES.delete(k);
}, 60 * 1000).unref();

function webauthnRpId(req) {
    return req.hostname;
}
function webauthnExpectedOrigin(req) {
    return `${req.protocol}://${req.get('host')}`;
}

// ---- 1a. Register options: bubuo ng challenge para sa PAG-ENROLL ----
//
// ROOT CAUSE FIX ("hindi gumana ang fingerprint registration sa fresh
// release build / laging may nade-detect na 'naka-rehistro na'"):
// Dati, ang WebAuthn `user.id` (userHandle) ay DETERMINISTIC — direktang
// hinango sa base64url(username) (hal. "admin"). Labag ito sa WebAuthn
// spec (ang user.id ay dapat RANDOM/opaque, HINDI dapat derivable mula
// sa PII gaya ng username), at nagdudulot ng totoong bug dito: dahil
// PAREHONG-PAREHO ang default na username ("admin") sa BAWAT bagong
// customer install, at kadalasang PAREHO rin ang rpId habang nagte-test
// (hal. parehong localhost, parehong staging/demo domain, parehong
// telepono ang ginagamit sa pag-demo), ang (rpId, userHandle) pair ay
// NAGIGING PARE-PAREHO sa MARAMING magkaibang "fresh" na package/install
// — kaya ang platform authenticator MISMO (ang OS-level passkey manager
// ng telepono — Android Credential Manager/iOS Keychain), hindi ang
// server na ito, ang nag-aakalang MAY NAKA-REHISTRO NA itong resident
// credential dito, kahit walang laman/fresh ang bagong database ng
// install. Ito ang dahilan kung bakit "laging may nade-detect na
// naka-rehistro na" kahit fresh ang bawat release build.
//
// FIX: gumagawa/gumagamit na tayo ng RANDOM, OPAQUE, PER-ACCOUNT na
// userHandle (32 random bytes) na naka-imbak sa DB record mismo ng user
// (`webauthnUserHandle`), sa halip na hinango sa username — kaya laging
// natatangi ito kahit parehong-pareho ang username/domain sa maraming
// install. Ginagawa lang ito minsan bawat account (lazy, on first
// register-options call) at nagpapatuloy pagkatapos, kaya hindi
// nagbabago ang binding ng mga credential na na-enroll na dati.
app.post('/api/auth/webauthn/register-options', (req, res) => {
    const username = req.authUser.username;
    const users = readData(FILE_USERS);
    const userIndex = users.findIndex(u => u.username.toLowerCase() === username.toLowerCase());
    if (userIndex === -1) return res.status(404).json({ success: false, message:'Hindi mahanap ang account.' });
    const user = users[userIndex];

    const challenge = webauthn.randomChallenge();
    WEBAUTHN_REGISTER_CHALLENGES.set(username.toLowerCase(), { challenge, expiresAt: Date.now() + WEBAUTHN_CHALLENGE_TTL_MS });

    const existingCredentials = (user.webauthnCredentials || []).map(c => ({ id: c.id, type:'public-key' }));

    if (!user.webauthnUserHandle) {
        user.webauthnUserHandle = crypto.randomBytes(32).toString('base64url');
        writeData(FILE_USERS, users);
    }

    res.json({
        success: true,
        options: {
            challenge,
            rp: { name:'OmniPOS', id: webauthnRpId(req) },
            user: {
                id: user.webauthnUserHandle,
                name: user.username,
                displayName: user.username
            },
            pubKeyCredParams: [
                { alg: -7, type:'public-key' },   // ES256
                { alg: -257, type:'public-key' }  // RS256
            ],
            authenticatorSelection: {
                authenticatorAttachment:'platform',
                // 'required' (hindi 'preferred'): dapat DISCOVERABLE ang
                // credential (naka-imbak ang buong reference sa loob mismo
                // ng authenticator/phone) — ito ang nagpapagana ng
                // USERNAMELESS login sa ibaba: kayang makilala ng OS kung
                // sino ang naka-enroll na account BASE LANG SA FINGERPRINT,
                // hindi na kailangang i-type muna ang username.
                residentKey:'required',
                userVerification:'required'
            },
            attestation:'none',
            timeout: 60000,
            excludeCredentials: existingCredentials
        }
    });
});

// ---- 1b. Register verify: i-che-check ang WebAuthn credential na
//          ibinalik ng browser (navigator.credentials.create()) at
//          ise-save ang PUBLIC KEY lamang sa account ng user. ----
app.post('/api/auth/webauthn/register-verify', (req, res) => {
    try {
        const username = req.authUser.username;
        const { credentialId, clientDataJSON, attestationObject, deviceLabel } = req.body || {};
        if (!credentialId || !clientDataJSON || !attestationObject) {
            return res.status(400).json({ success: false, message:'Missing data from the authenticator.' });
        }

        const stored = WEBAUTHN_REGISTER_CHALLENGES.get(username.toLowerCase());
        if (!stored || Date.now() > stored.expiresAt) {
            return res.status(400).json({ success: false, message:'Nag-expire na ang enrollment request. Subukan muli.' });
        }

        const clientData = JSON.parse(Buffer.from(clientDataJSON,'base64url').toString('utf8'));
        if (clientData.type !== 'webauthn.create') {
            return res.status(400).json({ success: false, message:'Invalid WebAuthn response type.' });
        }
        if (clientData.challenge !== stored.challenge) {
            return res.status(400).json({ success: false, message:'Hindi tugma ang challenge (posibleng expired o replayed na request).' });
        }
        if (clientData.origin !== webauthnExpectedOrigin(req)) {
            return res.status(400).json({ success: false, message:'The WebAuthn response origin does not match.' });
        }

        const attestationBuf = Buffer.from(attestationObject,'base64url');
        const { value: attObj } = webauthn.decodeCbor(attestationBuf, 0);
        const authDataBuf = Buffer.from(attObj.get('authData'));
        const parsed = webauthn.parseAuthenticatorData(authDataBuf);

        const expectedRpIdHash = webauthn.sha256(Buffer.from(webauthnRpId(req),'utf8'));
        if (Buffer.compare(parsed.rpIdHash, expectedRpIdHash) !== 0) {
            return res.status(400).json({ success: false, message:'The credential RP ID (site) does not match.' });
        }
        if (!parsed.flags.userPresent || !parsed.flags.userVerified) {
            return res.status(400).json({ success: false, message:'Hindi kumpirmadong biometric verification (kailangan tunay na fingerprint/Face ID, hindi lang pag-tap).' });
        }
        if (!parsed.credentialPublicKey) {
            return res.status(400).json({ success: false, message:'Walang natanggap na public key mula sa authenticator.' });
        }

        const { keyObject } = webauthn.coseKeyToPublicKeyObject(parsed.credentialPublicKey);
        const publicKeyJwk = keyObject.export({ format:'jwk' });
        const credIdB64 = Buffer.from(parsed.credentialId).toString('base64url');

        const users = readData(FILE_USERS);
        // Siguraduhing hindi na dating naka-rehistro kahit saang account ang
        // eksaktong credential ID na ito (dapat kaisa-isa).
        const alreadyUsed = users.some(u => (u.webauthnCredentials || []).some(c => c.id === credIdB64));
        if (alreadyUsed) {
            return res.status(409).json({ success: false, message:'Naka-rehistro na ang fingerprint na ito.' });
        }

        const userIndex = users.findIndex(u => u.username.toLowerCase() === username.toLowerCase());
        if (userIndex === -1) return res.status(404).json({ success: false, message:'Hindi mahanap ang account.' });

        if (!Array.isArray(users[userIndex].webauthnCredentials)) users[userIndex].webauthnCredentials = [];
        users[userIndex].webauthnCredentials.push({
            id: credIdB64,
            publicKeyJwk,
            counter: parsed.counter,
            deviceLabel: (deviceLabel || parseDeviceInfo(req.headers['user-agent']).label || 'Mobile device').slice(0, 80),
            createdAt: new Date().toLocaleString('en-US', { timeZone:'Asia/Manila' })
        });
        writeData(FILE_USERS, users);
        WEBAUTHN_REGISTER_CHALLENGES.delete(username.toLowerCase());

        logAction(username,`Nag-enable ng Fingerprint/Biometric Login (${users[userIndex].webauthnCredentials.at(-1).deviceLabel})`);
        res.json({ success: true, message:'Na-enable ang Fingerprint Login sa device na ito.' });
    } catch (err) {
        console.error('webauthn register-verify error:', err);
        res.status(400).json({ success: false, message:'Hindi ma-verify ang fingerprint enrollment. Subukan muli.' });
    }
});

// ---- 1c. List / remove enrolled biometric credentials ----
app.get('/api/auth/webauthn/credentials', (req, res) => {
    const users = readData(FILE_USERS);
    const user = users.find(u => u.username.toLowerCase() === req.authUser.username.toLowerCase());
    const creds = ((user && user.webauthnCredentials) || []).map(c => ({ id: c.id, deviceLabel: c.deviceLabel, createdAt: c.createdAt }));
    res.json({ success: true, credentials: creds });
});

app.delete('/api/auth/webauthn/credentials/:id', (req, res) => {
    const users = readData(FILE_USERS);
    const userIndex = users.findIndex(u => u.username.toLowerCase() === req.authUser.username.toLowerCase());
    if (userIndex === -1) return res.status(404).json({ success: false, message:'Hindi mahanap ang account.' });

    const before = (users[userIndex].webauthnCredentials || []).length;
    users[userIndex].webauthnCredentials = (users[userIndex].webauthnCredentials || []).filter(c => c.id !== req.params.id);
    if (users[userIndex].webauthnCredentials.length === before) {
        return res.status(404).json({ success: false, message:'Hindi mahanap ang fingerprint credential na iyon.' });
    }
    writeData(FILE_USERS, users);
    logAction(req.authUser.username,'Inalis ang isang Fingerprint/Biometric Login credential');
    res.json({ success: true, message:'Naalis na ang fingerprint credential.' });
});

// ---- 2a. Login options: bubuo ng challenge para makapag-LOGIN gamit
//          ang fingerprint (bago pa man magkaroon ng session). Kung
//          walang ibinigay na username, USERNAMELESS mode ito — ang
//          OS/browser mismo ng phone ang magpapakita ng listahan ng
//          naka-enroll na account dito (o direktang gagamitin kung
//          iisa lang), batay sa NAKA-SAVE na resident credential sa
//          device — hindi na kailangang i-type ang username. ----
app.post('/api/auth/webauthn/login-options', rateLimit('webauthn-login-options', 20, 10 * 60 * 1000), (req, res) => {
    const username = ((req.body && req.body.username) || '').toString().trim();
    const challenge = webauthn.randomChallenge();

    if (!username) {
        // USERNAMELESS: hindi natin alam kung sino pa lang ito — ang
        // credentialId/userHandle na ibabalik ng authenticator mismo
        // (pagkatapos ng fingerprint scan) ang siyang gagamitin sa
        // /login-verify para tukuyin kung sinong account ito.
        WEBAUTHN_LOGIN_CHALLENGES.set(challenge, { username: null, expiresAt: Date.now() + WEBAUTHN_CHALLENGE_TTL_MS });
        return res.json({
            success: true,
            challenge,
            rpId: webauthnRpId(req),
            userVerification:'required',
            timeout: 60000,
            allowCredentials: [] // sadyang blangko — nagpapagana ng discoverable/usernameless picker sa OS
        });
    }

    const users = readData(FILE_USERS);
    const user = users.find(u => u.username.toLowerCase() === username.toLowerCase());
    const credentials = (user && user.webauthnCredentials) || [];
    if (!user || credentials.length === 0) {
        return res.status(404).json({ success: false, message:'Fingerprint Login is not enabled for this account/device.' });
    }

    WEBAUTHN_LOGIN_CHALLENGES.set(challenge, { username: user.username, expiresAt: Date.now() + WEBAUTHN_CHALLENGE_TTL_MS });

    res.json({
        success: true,
        challenge,
        rpId: webauthnRpId(req),
        userVerification:'required',
        timeout: 60000,
        allowCredentials: credentials.map(c => ({ id: c.id, type:'public-key' }))
    });
});

// ---- 2b. Login verify: i-che-check ang assertion (navigator.credentials.get())
//          gamit ang naka-imbak na public key, tapos gagawa ng session
//          — pareho ang resulta sa /api/auth/login kapag successful.
//          Kung USERNAMELESS (walang naka-bind na username sa
//          challenge), ang credentialId/userHandle na dala ng assertion
//          ang gagamitin para tukuyin kung sinong account ito. ----
app.post('/api/auth/webauthn/login-verify', async (req, res) => {
    const { credentialId, clientDataJSON, authenticatorData, signature, userHandle } = req.body || {};
    if (!credentialId || !clientDataJSON || !authenticatorData || !signature) {
        return res.status(400).json({ success: false, message:'Missing data from the authenticator.' });
    }

    let clientData;
    try {
        clientData = JSON.parse(Buffer.from(clientDataJSON,'base64url').toString('utf8'));
    } catch {
        return res.status(400).json({ success: false, message:'Corrupted WebAuthn response.' });
    }

    const stored = WEBAUTHN_LOGIN_CHALLENGES.get(clientData.challenge);
    if (!stored || Date.now() > stored.expiresAt) {
        return res.status(400).json({ success: false, message:'The login request has expired. Please try again.' });
    }

    // Tukuyin kung sinong account ito BAGO pa man tumakbo ang rate-limit/
    // device-check gates (kailangan nila ng username). Kung USERNAMELESS
    // (walang naka-bind na username mula sa /login-options), hahanapin
    // ang may-ari base lang sa credentialId na dala ng authenticator
    // mismo — ito mismo ang "hindi na kailangang i-type ang username"
    // na bahagi ng feature.
    const users = readData(FILE_USERS);
    let userIndex = -1;
    let credIndex = -1;

    if (stored.username) {
        userIndex = users.findIndex(u => u.username.toLowerCase() === stored.username.toLowerCase());
        if (userIndex !== -1) {
            credIndex = (users[userIndex].webauthnCredentials || []).findIndex(c => c.id === credentialId);
        }
    } else {
        for (let i = 0; i < users.length; i++) {
            const idx = (users[i].webauthnCredentials || []).findIndex(c => c.id === credentialId);
            if (idx !== -1) { userIndex = i; credIndex = idx; break; }
        }
    }

    if (userIndex === -1 || credIndex === -1) {
        return res.status(401).json({ success: false, message:'This fingerprint is no longer registered. Please re-enroll it in Profile settings.' });
    }

    const username = users[userIndex].username;

    // Sanity check lang (hindi kritikal): kung may userHandle na dala ang
    // assertion, dapat tumutugma ito sa random/opaque na webauthnUserHandle
    // na naka-imbak sa DB record ng account na ito (itinakda noong
    // pag-enroll — see /register-options). HINDI na ito hinahango mula sa
    // username (dating bug — tingnan ang komento sa /register-options).
    // BACKWARD-COMPAT: kung walang naka-imbak na webauthnUserHandle ang
    // account (naka-enroll ito BAGO ang fix na ito), laktawan na lang ang
    // sanity check na ito — ang credentialId lookup + signature
    // verification sa ibaba na ang sapat/mapagkakatiwalaang pagkakakilanlan,
    // kaya hindi na kailangang pilitin ang legacy account na mag-re-enroll.
    if (userHandle) {
        const expectedHandle = users[userIndex].webauthnUserHandle;
        if (expectedHandle && userHandle !== expectedHandle) {
            return res.status(401).json({ success: false, message:'This fingerprint does not match the account.' });
        }
    }

    // Parehong anti-brute-force at anti-clone gates gaya ng /api/auth/login,
    // para hindi maging bypass ang fingerprint login sa mga proteksyong iyon.
    req.body.username = username;
    if (!checkLoginRateLimit(req, res, 5, 30, LOGIN_RATE_LIMIT_WINDOW_MS)) return;

    const deviceCheck = await checkDeviceBeforeLogin({ username });
    if (!deviceCheck.allowed) {
        return res.status(403).json({ success: false, deviceBlocked: true, message: deviceCheck.message });
    }
    recordLoginAttempt(req, LOGIN_RATE_LIMIT_WINDOW_MS);

    try {
        if (clientData.type !== 'webauthn.get') {
            return res.status(400).json({ success: false, message:'Invalid WebAuthn response type.' });
        }
        if (clientData.origin !== webauthnExpectedOrigin(req)) {
            return res.status(400).json({ success: false, message:'The WebAuthn response origin does not match.' });
        }

        const cred = users[userIndex].webauthnCredentials[credIndex];
        const authDataBuf = Buffer.from(authenticatorData,'base64url');
        const parsed = webauthn.parseAuthenticatorData(authDataBuf);

        const expectedRpIdHash = webauthn.sha256(Buffer.from(webauthnRpId(req),'utf8'));
        if (Buffer.compare(parsed.rpIdHash, expectedRpIdHash) !== 0) {
            return res.status(401).json({ success: false, message:'The credential RP ID (site) does not match.' });
        }
        if (!parsed.flags.userPresent || !parsed.flags.userVerified) {
            return res.status(401).json({ success: false, message:'Biometric verification was not confirmed.' });
        }

        const clientDataHash = webauthn.sha256(Buffer.from(clientDataJSON,'base64url'));
        const signedData = Buffer.concat([authDataBuf, clientDataHash]);
        const keyObject = crypto.createPublicKey({ key: cred.publicKeyJwk, format:'jwk' });
        const sigOk = webauthn.verifySignature(keyObject, signedData, Buffer.from(signature,'base64url'));
        if (!sigOk) {
            return res.status(401).json({ success: false, message:'Unable to verify the fingerprint signature.' });
        }

        // Anti-clone/anti-replay counter check — kung parehong may
        // counter support ang authenticator (hindi laging 0), dapat
        // laging TUMATAAS ang bagong counter kaysa sa huling na-save.
        if (!(parsed.counter === 0 && cred.counter === 0) && parsed.counter <= cred.counter) {
            return res.status(401).json({ success: false, message:'Suspicious repeated fingerprint signature (possible cloned authenticator). Please log in with your password.' });
        }
        cred.counter = parsed.counter;
        writeData(FILE_USERS, users);
        WEBAUTHN_LOGIN_CHALLENGES.delete(clientData.challenge);

        const user = users[userIndex];
        logAction(user.username,`Naka-login gamit ang Fingerprint/Biometric Login (${cred.deviceLabel || 'device'})`);

        const token = createSession(user.username, user.role, req.headers['user-agent'], getClientIp(req));
        const permissions = getPermissionsForRole(user.role);
        return res.json({ success: true, user: { username: user.username, role: user.role, avatar: user.avatar || null }, token, permissions, menuRegistry: MENU_REGISTRY });
    } catch (err) {
        console.error('webauthn login-verify error:', err);
        return res.status(400).json({ success: false, message:'Unable to verify Fingerprint Login. Please try again or use your password.' });
    }
});

app.post('/api/auth/logout', (req, res) => {
    if (req.authToken) {
        destroySession(req.authToken);
    }
    res.json({ success: true, message:'Logged out.' });
});

app.get('/api/auth/active-sessions', (req, res) => {
    const now = Date.now();

    const allUsers = readData(FILE_USERS);

    const requesterRole = req.authUser && req.authUser.role;
    const canSeeSensitiveDetails = (requesterRole ||'').toLowerCase() ==='admin' || !!getPermissionsForRole(requesterRole).users;

    const latestSessionByUsername = new Map();
    Array.from(SESSIONS.values())
        .filter(s => now <= s.expiresAt)
        .forEach(s => {
            const existing = latestSessionByUsername.get(s.username);
            if (!existing || (s.loginAt || 0) > (existing.loginAt || 0)) {
                latestSessionByUsername.set(s.username, s);
            }
        });

    const sessions = Array.from(latestSessionByUsername.values())
        .map(s => {
            const userRecord = allUsers.find(u => u.username === s.username);
            return {
                username: s.username,
                role: s.role,
                avatar: (userRecord && userRecord.avatar) || null,
                loginAt: s.loginAt || null,
                minutesActive: s.loginAt ? Math.max(0, Math.floor((now - s.loginAt) / 60000)) : null,
                isCurrentSession: req.authToken ? SESSIONS.get(req.authToken) === s : false,

                // Device/IP ay sensitibong impormasyon — itinatago sa mga
                // walang 'users' management access (o hindi Admin), pero
                // hindi natin binablock ang buong widget dahil ginagamit
                // ito ng lahat ng role bilang simpleng "who's online" view.
                device: canSeeSensitiveDetails ? (s.device || parseDeviceInfo('')) : null,
                ip: canSeeSensitiveDetails ? (s.ip ||'unknown') : null,
                sameWifi: canSeeSensitiveDetails ? isSameLanAsServer(s.ip) : false
            };
        })
        .sort((a, b) => (a.loginAt || 0) - (b.loginAt || 0));

    res.json({ success: true, activeUsers: sessions, count: sessions.length });
});

// SECURITY FIX: tinanggal ang function body ng dating '/api/products/checkout'.
// Legacy/unused na endpoint ito (hindi na tinatawag ng frontend/public/app.js)
// na diretsong nagbabawas ng stock BATAY LANG SA SINABI NG CLIENT — walang
// pag-verify ng presyo, walang paglikha ng aktwal na transaction/resibo, at
// walang audit log. Kung natira ito nang gumagana, may paraan ang isang
// Cashier (o kahit sino na naka-login) na direktang tumawag dito (hal. sa
// Postman/devtools) para "ayusin"/ibaba ang stock ng isang item — halimbawa
// pagkatapos nilang kunin/ibigay ang paninda nang hindi ito na-ring sa
// checkout — nang hindi ito lumalabas sa Transactions o Logs. Iniwan ang
// route pero pinatay ang function nito (410 Gone) sa halip na basta alisin,
// para malinaw ang dahilan kung sakaling may lumang client pa ring tatawag
// dito, at para may makuhang forensic trail kung sino ang sumusubok pa rin.
app.post('/api/products/checkout', requirePermission('terminal'), (req, res) => {
    try {
        logAction(req.authUser && req.authUser.username, `Blocked call to removed endpoint /api/products/checkout (security fix — direct stock manipulation na walang transaction record). Payload: ${JSON.stringify(req.body || {}).slice(0, 300)}`);
    } catch (e) {
        console.error('Failed to log blocked /api/products/checkout attempt:', e);
    }
    res.status(410).json({
        success: false,
        code:'ENDPOINT_REMOVED',
        message:'Tinanggal na ang endpoint na ito dahil sa security review — pwede itong dating gamitin para baguhin ang stock nang walang naitatalang benta at walang audit trail. Gamitin ang /api/transactions para sa checkout/sale.'
    });
});

app.get('/api/products', (req, res) => {
    // ADAPTIVE POLLING SUPPORT: idinagdag itong header (hindi sinira ang
    // dating plain-array na response body, dahil sa client, in-array pa
    // rin ang inaasahan dito) para malaman ng client (Terminal/Inventory
    // silent stock-poll) kung ilan ang kasalukuyang aktibong session
    // (terminal) — walang dagdag na network call, "piggyback" lang sa
    // response na ito na palagi namang tinatawag.
    res.set('X-Active-Terminals', String(SESSIONS.size));
    res.json(readData(FILE_PRODUCTS));
});

app.get('/api/products/export', requireFeature('advanced_reports'), (req, res) => {
    try {
        const products = readData(FILE_PRODUCTS);
        const escapeCsv = (val) => {
            const s = (val === undefined || val === null) ?'' : val.toString();
            return/[",\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
        };
        const headers = ['Code','Product Name','Category','Price','Stock','Supplier','Expiry Date','Low Stock Threshold','Cost Price'];
        const lines = [headers.join(',')];
        products.forEach(p => {
            lines.push([
                escapeCsv(p.code), escapeCsv(p.name), escapeCsv(p.category),
                escapeCsv(p.price), escapeCsv(p.stock), escapeCsv(p.supplier),
                escapeCsv(p.expiryDate), escapeCsv(p.lowStockThreshold), escapeCsv(p.cost)
            ].join(','));
        });
        const csvContent ='\uFEFF' + lines.join('\r\n');

        res.setHeader('Content-Type','text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename="inventory_export_${Date.now()}.csv"`);
        res.send(csvContent);
    } catch (err) {
        console.error('Export error:', err);
        res.status(500).json({ success: false, message:'Hindi ma-export ang inventory.' });
    }
});

app.get('/api/products/template', async (req, res) => {
    try {
        const categories = readData(FILE_CATEGORIES, DEFAULT_CATEGORIES);
        const workbook = new ExcelJS.Workbook();
        workbook.creator ='OmniPOS System';

        const catSheet = workbook.addWorksheet('CategoriesList');
        categories.forEach((cat, i) => {
            catSheet.getCell(`A${i + 1}`).value = cat;
        });
        catSheet.state ='veryHidden';

        const sheet = workbook.addWorksheet('New Products');
        sheet.columns = [
            { header:'Code', key:'code', width: 22 },
            { header:'Product Name', key:'name', width: 32 },
            { header:'Category', key:'category', width: 24 },
            { header:'Price', key:'price', width: 14 },
            { header:'Stock', key:'stock', width: 12 },
            { header:'Supplier', key:'supplier', width: 24 },
            { header:'Expiry Date', key:'expiry', width: 16 },
            { header:'Low Stock Threshold', key:'threshold', width: 18 },
            { header:'Cost Price', key:'cost', width: 14 }
        ];
        const headerRow = sheet.getRow(1);
        headerRow.eachCell(cell => {
            cell.font = { bold: true, color: { argb:'FFFFFFFF' } };
            cell.fill = { type:'pattern', pattern:'solid', fgColor: { argb:'FF2563EB' } };
            cell.alignment = { vertical:'middle', horizontal:'center' };
        });

        const catRange = `CategoriesList!$A$1:$A$${Math.max(categories.length, 1)}`;
        for (let row = 2; row <= 501; row++) {
            sheet.getCell(`C${row}`).dataValidation = {
                type:'list',
                allowBlank: true,
                formulae: [catRange],
                showErrorMessage: false,
                promptTitle:'Category',
                prompt:'Pumili sa dropdown, o mag-type ng bagong pangalan ng category para awtomatikong madagdag ito sa system.'
            };
            sheet.getCell(`D${row}`).numFmt ='#,##0.00';
            sheet.getCell(`E${row}`).numFmt ='#,##0';
        }

        const infoSheet = workbook.addWorksheet('Paano Gamitin');
        infoSheet.getColumn(1).width = 95;
        const instructions = [
'PAANO GAMITIN ANG TEMPLATE NA ITO:',
'',
'1. Pumunta sa sheet na "New Products".',
'2. Punan ang bawat hilera: Code, Product Name, Category, Price, Stock.',
'3. Sa column na "Category" (column C), pindutin ang dropdown arrow para pumili ng existing category.',
'4. Kung gusto mag-add ng BAGONG category, i-type lang ito diretso sa cell — awtomatiko itong madadagdag sa system pagka-import.',
'5. Huwag baguhin ang mga pangalan sa Row 1 (headers) at huwag magdagdag ng bagong column.',
'6. Isave ang file (.xlsx), pagkatapos i-upload gamit ang "Import Excel/CSV" button sa Product Inventory page.',
'7. Ang mga Product Code na dati nang ginagamit ay ise-skip habang nag-i-import — MALIBAN kung pinili mong "Update Existing" bago mag-upload.',
'8. Optional na columns: Supplier, Expiry Date (YYYY-MM-DD), Low Stock Threshold, at Cost Price — pwedeng iwanang blangko.',
'9. Sa "Update Existing" mode, ang mga blangkong cell ay HINDI nagbabago sa laman ng existing product — mananatili ang dati nitong value.'
        ];
        instructions.forEach((line, i) => { infoSheet.getCell(`A${i + 1}`).value = line; });
        infoSheet.getCell('A1').font = { bold: true, size: 13 };

        res.setHeader('Content-Type','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.setHeader('Content-Disposition','attachment; filename="product_import_template.xlsx"');
        await workbook.xlsx.write(res);
        res.end();
    } catch (err) {
        console.error('Template generation error:', err);
        res.status(500).json({ success: false, message:'Hindi magawa ang Excel template.' });
    }
});

function parseCsvLine(line) {
    const result = [];
    let cur ='';
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (inQuotes) {
            if (ch ==='"') {
                if (line[i + 1] ==='"') {
                    cur +='"';
                    i++;
                } else {
                    inQuotes = false;
                }
            } else {
                cur += ch;
            }
        } else {
            if (ch ==='"') {
                inQuotes = true;
            } else if (ch ===',') {
                result.push(cur);
                cur ='';
            } else {
                cur += ch;
            }
        }
    }
    result.push(cur);
    return result;
}

function parseMoney(raw) {
    const s = (raw ||'').toString().trim();
    if (s ==='') return NaN;
    const cleaned = s.replace(/^(₱|PHP)\s*/i,'').replace(/,/g,'');
    if (!/^\d+(\.\d+)?$/.test(cleaned)) return NaN;
    return parseFloat(cleaned);
}

function parseWholeNumber(raw) {
    const s = (raw ||'').toString().trim().replace(/,/g,'');
    if (!/^\d+$/.test(s)) return NaN;
    return parseInt(s, 10);
}

app.post('/api/products/import', rateLimit('product-import', 20, 10 * 60 * 1000), productImportUpload.single('file'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ success: false, message:'Walang na-attach na file.' });
    }

    const cleanupTmpFile = () => fs.unlink(req.file.path, () => {});

    const { username } = req.body;
    const users = readData(FILE_USERS);
    const activeUser = users.find(u => u.username.toLowerCase() === (username ||'').toLowerCase());
    if (!activeUser || activeUser.role.toLowerCase() !=='admin') {
        cleanupTmpFile();
        return res.status(403).json({ success: false, message:'Admin lang ang pwedeng mag-import ng products.' });
    }

    try {
        const ext = path.extname(req.file.originalname).toLowerCase();
        let rows = [];

        if (ext ==='.csv') {
            let content = fs.readFileSync(req.file.path,'utf8');
            content = content.replace(/^\uFEFF/,'');
            const lines = content.split(/\r?\n/).filter(l => l.trim() !=='');
            if (lines.length > 1) {
                const headers = parseCsvLine(lines[0]).map(h => h.trim().toLowerCase());
                for (let i = 1; i < lines.length; i++) {
                    const cols = parseCsvLine(lines[i]);
                    const obj = {};
                    headers.forEach((h, idx) => { obj[h] = (cols[idx] ||'').trim(); });
                    rows.push(obj);
                }
            }
        } else {
            const workbook = new ExcelJS.Workbook();
            await workbook.xlsx.readFile(req.file.path);
            const sheet = workbook.getWorksheet('New Products') || workbook.worksheets[0];
            const headerValues = (sheet.getRow(1).values || []).map(v => (v ||'').toString().trim().toLowerCase());

            sheet.eachRow((row, rowNumber) => {
                if (rowNumber === 1) return;
                const rowValues = row.values;
                const obj = {};
                headerValues.forEach((h, idx) => {
                    if (!h) return;
                    const cellVal = rowValues[idx];
                    obj[h] = (cellVal !== undefined && cellVal !== null) ? cellVal.toString().trim() :'';
                });
                if (Object.values(obj).some(v => v !=='')) rows.push(obj);
            });
        }

        const mode = (req.body.mode ||'skip').toLowerCase() ==='update' ?'update' :'skip';

        let products = readData(FILE_PRODUCTS);
        let categories = readData(FILE_CATEGORIES, DEFAULT_CATEGORIES);
        const codeIndex = new Map(products.map((p, i) => [p.code.trim().toLowerCase(), i]));
        const newCategoriesFound = new Set();
        let added = 0, updated = 0, skipped = 0;
        const errors = [];

        rows.forEach((r, idx) => {
            const rowNum = idx + 2;
            const code = (r.code ||'').toString().trim();
            const name = (r['product name'] || r.name ||'').toString().trim();
            const categoryRaw = (r.category ||'').toString().trim();
            const supplier = (r.supplier ||'').toString().trim();
            const expiry = (r['expiry date'] || r.expiry || r.expirydate ||'').toString().trim();
            const thresholdRaw = (r['low stock threshold'] || r.threshold || r.lowstockthreshold ||'').toString().trim();
            const costRaw = (r['cost price'] || r.cost || r.costprice ||'').toString().trim();

            const priceRaw = (r.price ||'').toString().trim();
            const stockRaw = (r.stock ||'').toString().trim();
            const price = parseMoney(priceRaw);
            const stock = parseWholeNumber(stockRaw);
            const existingIdx = codeIndex.get(code.toLowerCase());

            if (!code || !name) {
                errors.push(`Row ${rowNum}: Kulang ang Code o Product Name — na-skip.`);
                skipped++;
                return;
            }

            if (existingIdx !== undefined) {
                if (mode !=='update') {
                    errors.push(`Row ${rowNum}: Ginagamit na ang Code "${code}" — na-skip.`);
                    skipped++;
                    return;
                }

                if (priceRaw !=='' && isNaN(price)) {
                    errors.push(`Row ${rowNum}: Hindi valid ang Price para sa Code "${code}" — hindi na-update ang price.`);
                } else if (priceRaw !=='') {
                    products[existingIdx].price = price;
                }
                if (stockRaw !=='' && isNaN(stock)) {
                    errors.push(`Row ${rowNum}: Hindi valid ang Stock para sa Code "${code}" — hindi na-update ang stock.`);
                } else if (stockRaw !=='') {
                    products[existingIdx].stock = stock;
                }
                if (name) products[existingIdx].name = name;
                if (categoryRaw) products[existingIdx].category = categoryRaw;
                if (supplier) products[existingIdx].supplier = supplier;
                if (expiry) products[existingIdx].expiryDate = expiry;
                if (thresholdRaw !=='') {
                    const th = parseWholeNumber(thresholdRaw);
                    if (!isNaN(th)) products[existingIdx].lowStockThreshold = th;
                }
                if (costRaw !=='') {
                    const costVal = parseMoney(costRaw);
                    if (!isNaN(costVal)) products[existingIdx].cost = costVal;
                }
                if (categoryRaw && !categories.includes(categoryRaw)) {
                    categories.push(categoryRaw);
                    newCategoriesFound.add(categoryRaw);
                }
                updated++;
                return;
            }

            const category = categoryRaw ||'Others';
            if (isNaN(price) || isNaN(stock)) {
                errors.push(`Row ${rowNum}: Hindi valid ang Price o Stock — na-skip.`);
                skipped++;
                return;
            }

            const newProduct = { code, name, category, price, stock };
            if (supplier) newProduct.supplier = supplier;
            if (expiry) newProduct.expiryDate = expiry;
            if (thresholdRaw !=='') {
                const th = parseWholeNumber(thresholdRaw);
                if (!isNaN(th)) newProduct.lowStockThreshold = th;
            }
            if (costRaw !=='') {
                const costVal = parseMoney(costRaw);
                if (!isNaN(costVal)) newProduct.cost = costVal;
            }

            products.push(newProduct);
            codeIndex.set(code.toLowerCase(), products.length - 1);

            if (!categories.includes(category)) {
                categories.push(category);
                newCategoriesFound.add(category);
            }
            added++;
        });

        writeData(FILE_PRODUCTS, products);
        writeData(FILE_CATEGORIES, categories);
        logAction(username, `Bulk-imported products via Excel/CSV: ${added} added, ${updated} updated, ${skipped} skipped.`);
        cleanupTmpFile();

        res.json({
            success: true,
            added,
            updated,
            skipped,
            errors,
            newCategories: [...newCategoriesFound],
            categories,
            products
        });
    } catch (err) {
        console.error('Import error:', err);
        cleanupTmpFile();
        res.status(500).json({ success: false, message:'Hindi mabasa ang file. Siguraduhing wastong .xlsx o .csv format ang ginamit (gamitin ang Download Template button).' });
    }
});

app.post('/api/products', (req, res) => {
    const { product } = req.body;
    const username = req.authUser.username;
    let products = readData(FILE_PRODUCTS);

    const codeExists = products.some(p => p.code.trim().toLowerCase() === product.code.trim().toLowerCase());
    if (codeExists) {
        return res.status(400).json({ success: false, message: `❌ Ang Product Code [${product.code}] ay ginagamit na!` });
    }

    const isAdminRole = (req.authUser.role ||'').toLowerCase() ==='admin';
    const canApplyDirectly = isAdminRole || !!getPermissionsForRole(req.authUser.role).products_direct_apply;

    if (canApplyDirectly) {
        products.push(product);
        writeData(FILE_PRODUCTS, products);
        logAction(username, `Added new product: ${product.name}`);
        return res.json({ success: true, message:'Product added successfully' });
    } else {
            let requests = readData(FILE_REQUESTS);
    requests.push({
        id:'REQ-' + Date.now(),
        requester: username,
        type:'ADD',
        data: product,
        timestamp: new Date().toLocaleString()
    });
    writeData(FILE_REQUESTS, requests);
    return res.json({ success: true, message:'Request sent to Admin for approval.'
                    });
}

});

// SECURITY FIX: tinanggal ang function body ng dating '/api/products/deduct'.
// Ito ang PINAKA-MALALANG bug — walang requirePermission(...) kaya KAHIT
// ANONG naka-login na role (kahit pinaka-mababa) ay pwedeng tumawag dito
// direkta, walang pag-verify ng presyo, walang paglikha ng transaction
// record, at walang logAction (walang audit trail). Legacy/unused na rin
// ito sa frontend (public/app.js). Katulad ng '/api/products/checkout',
// iniwan ang route pero pinatay ang function nito (410 Gone) at nilagyan
// ng logging kung sino man ang susubok pa ring tumawag dito.
app.post('/api/products/deduct', (req, res) => {
    try {
        logAction(req.authUser && req.authUser.username, `Blocked call to removed endpoint /api/products/deduct (security fix — walang permission check dati, direct stock manipulation na walang transaction record). Payload: ${JSON.stringify(req.body || {}).slice(0, 300)}`);
    } catch (e) {
        console.error('Failed to log blocked /api/products/deduct attempt:', e);
    }
    res.status(410).json({
        success: false,
        code:'ENDPOINT_REMOVED',
        message:'Tinanggal na ang endpoint na ito dahil sa security review — pwede itong dating gamitin ng kahit sinong naka-login para baguhin ang stock nang walang permission check at walang audit trail. Gamitin ang /api/transactions para sa checkout/sale.'
    });
});

app.put('/api/products/:code', (req, res) => {
    const { code } = req.params;
    const { updatedData } = req.body;
    const username = req.authUser.username;
    let products = readData(FILE_PRODUCTS);

    const isAdminRole = (req.authUser.role ||'').toLowerCase() ==='admin';
    const canApplyDirectly = isAdminRole || !!getPermissionsForRole(req.authUser.role).products_direct_apply;

    if (canApplyDirectly) {
        products = products.map(p => p.code.trim().toLowerCase() === code.trim().toLowerCase() ? { ...p, ...updatedData } : p);
        writeData(FILE_PRODUCTS, products);
        logAction(username, `Updated product code: ${code}`);
        return res.json({ success: true, message:'Product updated successfully' });
    } else {
        let requests = readData(FILE_REQUESTS);
        requests.push({ id: Date.now(), type:'UPDATE', targetCode: code, requester: username, data: updatedData, timestamp: new Date().toLocaleString() });
        writeData(FILE_REQUESTS, requests);
        logAction(username, `Submitted an UPDATE request for code: ${code}`);
        return res.json({ success: true, message:'Update request submitted for Admin approval' });
    }
});

app.delete('/api/products/:code', (req, res) => {
    const { code } = req.params;
    const username = req.authUser.username;
    let products = readData(FILE_PRODUCTS);

    const isAdminRole = (req.authUser.role ||'').toLowerCase() ==='admin';
    const canApplyDirectly = isAdminRole || !!getPermissionsForRole(req.authUser.role).products_direct_apply;

    if (canApplyDirectly) {
        products = products.filter(p => p.code.trim().toLowerCase() !== code.trim().toLowerCase());
        writeData(FILE_PRODUCTS, products);
        logAction(username, `Deleted product code: ${code}`);
        return res.json({ success: true, message:'Product deleted successfully' });
    } else {
        let requests = readData(FILE_REQUESTS);
        requests.push({ id: Date.now(), type:'DELETE', targetCode: code, requester: username, timestamp: new Date().toLocaleString() });
        writeData(FILE_REQUESTS, requests);
        logAction(username, `Submitted a DELETE request for code: ${code}`);
        return res.json({ success: true, message:'Delete request submitted for Admin approval' });
    }
});

app.get('/api/requests', requirePermission('pending_requests'), (req, res) => {
    res.json(readData(FILE_REQUESTS));
});

app.post('/api/requests/:id/resolve', rateLimit('admin-resolve-request', 15, 10 * 60 * 1000), verifyAdmin, (req, res) => {
    const { id } = req.params;
    const { action, username } = req.body;

    let requests = readData(FILE_REQUESTS);
    const reqIndex = requests.findIndex(r => r.id.toString() === id.toString());

    if (reqIndex === -1) {
        return res.status(404).json({ success: false, message:'Request Reference ID Not Found.' });
    }

    const targetReq = requests[reqIndex];
    const normalizedAction = action ? action.toLowerCase() :'';

    if (normalizedAction ==='approve' || normalizedAction ==='approved') {
        if (targetReq.type ==='PROFILE_UPDATE') {

            const result = applyProfileChanges(targetReq.targetUser, targetReq.data || {});
            if (!result.ok) {
                return res.status(400).json({ success: false, message: `Hindi ma-apply ang Edit Profile request: ${result.error}` });
            }
            logAction(username, result.renamedFrom
                ? `APPROVED Edit Profile request para kay "${targetReq.targetUser}" — pinalitan ang username sa "${result.user.username}"`
                : `APPROVED Edit Profile request para kay "${targetReq.targetUser}"`);
        } else if (targetReq.type ==='RECEIPT_UPDATE') {
            const settings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);
            const d = targetReq.data || {};
            settings.storeName = (d.storeName ||'').trim() || settings.storeName;
            settings.storeAddress = (d.storeAddress ||'').trim();
            settings.storeContact = (d.storeContact ||'').trim();
            settings.headerText = (d.headerText ||'').trim();
            settings.footerText = (d.footerText ||'').trim() || DEFAULT_RECEIPT_SETTINGS.footerText;
            settings.customizeCount = (settings.customizeCount || 0) + 1;
            if (!settings.firstCustomizedAt) settings.firstCustomizedAt = new Date().toISOString();
            writeData(FILE_RECEIPT_SETTINGS, settings);
            logAction(username, `APPROVED Receipt Customization request mula kay "${targetReq.requester}"`);
        } else if (targetReq.type ==='RECEIPT_PAPER_SIZE') {
            const settings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);
            const paperSize = targetReq.data && targetReq.data.paperSize;
            if (VALID_PAPER_SIZES.includes(paperSize)) {
                settings.paperSize = paperSize;
                writeData(FILE_RECEIPT_SETTINGS, settings);
                logAction(username, `APPROVED Receipt Paper Size request (${paperSize}) mula kay "${targetReq.requester}"`);
            }
        } else {
            let products = readData(FILE_PRODUCTS);

            if (targetReq.type ==='ADD') {
                products.push(targetReq.data);
                logAction(username, `APPROVED ADD Request for product: ${targetReq.data?.name}`);
            }
            else if (targetReq.type ==='UPDATE') {
                products = products.map(p => p.code.trim().toLowerCase() === targetReq.targetCode.trim().toLowerCase() ? { ...p, ...targetReq.data } : p);
                logAction(username, `APPROVED UPDATE Request for code: ${targetReq.targetCode}`);
            }
            else if (targetReq.type ==='DELETE') {
                products = products.filter(p => p.code.trim().toLowerCase() !== targetReq.targetCode.trim().toLowerCase());
                logAction(username, `APPROVED DELETE Request for code: ${targetReq.targetCode}`);
            }
            else if (targetReq.type ==='RESTOCK') {
                const qtyToAdd = parseInt(targetReq.data?.qtyToAdd) || 0;
                products = products.map(p => {
                    if (p.code.trim().toLowerCase() === targetReq.targetCode.trim().toLowerCase()) {
                        return { ...p, stock: (parseInt(p.stock) || 0) + qtyToAdd };
                    }
                    return p;
                });
                logAction(username, `APPROVED RESTOCK Request for code: ${targetReq.targetCode} (+${qtyToAdd})`);
            }

            writeData(FILE_PRODUCTS, products);
        }
    } else {
        if (targetReq.type ==='PROFILE_UPDATE') {
            logAction(username, `REJECTED Edit Profile request para kay "${targetReq.targetUser}"`);
        } else {
            logAction(username, `REJECTED ${targetReq.type} request for item ID/Code: ${targetReq.targetCode || targetReq.data?.code}`);
        }
    }

    requests = requests.filter(r => r.id.toString() !== id.toString());
    writeData(FILE_REQUESTS, requests);

    res.json({ success: true, message: `Request processed and removed successfully.` });
});

app.post('/api/transactions', requirePermission('terminal'), async (req, res) => {
    const { transaction, username } = req.body;

    if (!transaction || typeof transaction !== 'object' || !Array.isArray(transaction.items) || transaction.items.length === 0) {
        return res.status(400).json({ success: false, message: 'Walang laman o hindi valid ang transaction items.' });
    }

    transaction.cashier = req.authUser.username;

    let transactions = readData(FILE_TRANSACTIONS);
    let products = readData(FILE_PRODUCTS);
    let customers = readData(FILE_CUSTOMERS, []);

    const storeSettings = getStoreSettingsPublic(readData(FILE_STORE_SETTINGS, DEFAULT_STORE_SETTINGS));

    // Payment method validation — tinatanggihan ang benta kung gumagamit
    // ng payment method na hindi naka-enable sa Store & Sales Settings
    // (hal. na-disable na ang GCash pero pinilit pa rin gamitin sa
    // client). Sinusuri pareho ang single-method at split-payment flow.
    const enabledMethodKeys = Object.entries(storeSettings.paymentMethods)
        .filter(([, enabled]) => enabled)
        .map(([key]) => key.toLowerCase());
    const methodAliasMap = { cash:'cash', gcash:'gcash', maya:'maya', paymaya:'maya', card:'card', banktransfer:'banktransfer', bank_transfer:'banktransfer' };
    function isMethodEnabled(rawMethod) {
        const norm = String(rawMethod || 'cash').toLowerCase().replace(/[\s_-]/g, '');
        const key = methodAliasMap[norm] || norm;
        return enabledMethodKeys.includes(key);
    }
    const paymentMethodsUsed = Array.isArray(transaction.payments) && transaction.payments.length > 0
        ? transaction.payments.map(p => p.method)
        : [transaction.method || transaction.payment_method || 'cash'];
    const disabledMethodUsed = paymentMethodsUsed.find(m => !isMethodEnabled(m));
    if (disabledMethodUsed) {
        return res.status(400).json({
            success: false,
            message: `Ang payment method na "${disabledMethodUsed}" ay hindi naka-enable sa Store & Sales Settings. Puntahan ang Users > Store & Sales para i-enable.`
        });
    }

    // MODERNIZED GCASH/MAYA: kagaya ng ginagawa ng mga aktwal na e-wallet
    // acceptance ngayon, kailangan ng reference/transaction number para sa
    // bawat GCash/Maya na bayad — ito ang gagamitin sa reconciliation
    // laban sa aktwal na GCash/Maya business app/statement ng tindahan.
    // Sinusuri dito (server-side), hindi lang sa client, kaya hindi ito
    // maiiwasan sa pamamagitan ng direktang API call.
    const eWalletMethods = new Set(['gcash', 'maya', 'paymaya']);
    function normalizeMethodKey(rawMethod) {
        return String(rawMethod || '').toLowerCase().replace(/[\s_-]/g, '');
    }
    if (Array.isArray(transaction.payments) && transaction.payments.length > 0) {
        const missingRef = transaction.payments.find(p =>
            eWalletMethods.has(normalizeMethodKey(p.method)) && !String(p.reference || '').trim());
        if (missingRef) {
            return res.status(400).json({
                success: false,
                message: `Kailangan ang reference/transaction number para sa ${missingRef.method} na bayad.`
            });
        }
    } else {
        const singleMethod = normalizeMethodKey(transaction.method || transaction.payment_method);
        if (eWalletMethods.has(singleMethod) && !String(transaction.paymentReference || '').trim()) {
            return res.status(400).json({
                success: false,
                message: `Kailangan ang reference/transaction number para sa ${transaction.method || transaction.payment_method} na bayad.`
            });
        }
    }

    // ------------------------------------------------------------------
    // SECURITY FIX: dati, ang quantity/price/discount/total ng transaction
    // ay basta TINITIWALAAN mula sa client (kayang i-manipulate sa
    // devtools/direct API call para sa negative-quantity "stock
    // injection", o price/total tampering — "skimming" fraud). Ngayon,
    // bawat item ay:
    //   1. kinukumpirma laban sa TALAGANG naitalang produkto sa database
    //      (code muna, name bilang fallback) — tinatanggihan ang buong
    //      transaksyon kung may item na hindi nahanap.
    //   2. kailangang POSITIVE INTEGER ang quantity (hindi puwedeng zero,
    //      negative, o decimal — dati'y walang validation dito, kaya
    //      kayang gamitin para MAGDAGDAG ng stock gamit ang negative
    //      quantity).
    //   3. ang presyo (`item.price`) ay PINIPWERSA na tumugma sa presyo
    //      ng produkto sa database — hindi na ang presyo na ipinasa ng
    //      client ang ginagamit. Ang legit na discount workflows (per-
    //      item discount, Senior/PWD 20%, promo code, manual discount)
    //      ay sinusuportahan pa rin, pero kinukwenta/kinukumpirma ULIT
    //      dito, hindi basta tinitiwalaan.
    // Ang FINAL na `transaction.total` ay laging kinukwenta ULIT sa
    // server base sa totoong presyo ng produkto + validated discounts.
    // ------------------------------------------------------------------
    const resolvedItems = [];
    const stockIssues = [];
    const rejectedItems = [];

    for (const item of (transaction.items || [])) {
        let prod = products.find(p => p.code === item.code);
        if (!prod) prod = products.find(p => p.name === item.name);

        if (!prod) {
            rejectedItems.push(item && (item.code || item.name) || '(unknown item)');
            continue;
        }

        const qty = parseInt(item.quantity, 10);
        if (!Number.isInteger(qty) || qty <= 0 || String(item.quantity).trim() === '') {
            rejectedItems.push(`${prod.name} (invalid quantity: ${item.quantity})`);
            continue;
        }

        const catalogPrice = parseFloat(prod.price) || 0;
        const lineSubtotal = Math.round(catalogPrice * qty * 100) / 100;
        const itemDiscount = Math.min(Math.max(0, parseFloat(item.itemDiscount) || 0), lineSubtotal);

        const availableStock = parseInt(prod.stock) || 0;
        if (qty > availableStock) {
            stockIssues.push(`${prod.name} (natitira: ${availableStock}, hiniling: ${qty})`);
        }

        resolvedItems.push({
            code: prod.code,
            name: prod.name,
            price: catalogPrice,
            quantity: qty,
            itemDiscount,
            cost: parseFloat(prod.cost) || 0
        });
    }

    if (rejectedItems.length > 0) {
        return res.status(400).json({
            success: false,
            message: `Hindi ma-proseso ang benta — invalid o hindi nahanap ang item(s): ${rejectedItems.join(', ')}.`
        });
    }
    if (stockIssues.length > 0) {
        return res.status(409).json({
            success: false,
            outOfStock: true,
            message: `Hindi ma-proceed ang benta — naubos/kulang na ang stock: ${stockIssues.join(', ')}. Malamang na-benta na ito sa ibang terminal/device. I-refresh ang product list.`
        });
    }

    const grossSubtotal = Math.round(resolvedItems.reduce((sum, it) => sum + (it.price * it.quantity), 0) * 100) / 100;
    const itemDiscountTotal = Math.round(resolvedItems.reduce((sum, it) => sum + it.itemDiscount, 0) * 100) / 100;
    const netAfterItemDiscounts = Math.max(0, Math.round((grossSubtotal - itemDiscountTotal) * 100) / 100);

    // Cart-level discount: kinukwenta/kinukumpirma ULIT ayon sa
    // discountType — hindi basta ang halagang ipinasa ng client (maliban
    // sa MANUAL, na sadyang discretion ng cashier, pero clamped pa rin
    // para hindi lumagpas sa net subtotal).
    let cartDiscount = 0;
    const discountType = transaction.discountType || 'NONE';

    if (discountType === 'SENIOR_PWD') {
        if (!transaction.seniorPwdId || !String(transaction.seniorPwdId).trim()) {
            return res.status(400).json({ success: false, message: 'Kailangan ng Senior/PWD ID Number para sa discount na ito.' });
        }
        if (!storeSettings.seniorPwdDiscountEnabled) {
            return res.status(400).json({ success: false, message: 'Naka-disable ang Senior/PWD Discount. Puntahan ang Users > Store & Sales para i-enable.' });
        }
        const seniorPwdRate = Math.min(Math.max(0, storeSettings.seniorPwdDiscountRate), 100) / 100;
        cartDiscount = Math.round(netAfterItemDiscounts * seniorPwdRate * 100) / 100;
    } else if (discountType === 'PROMO') {
        const promoCode = String(transaction.promoCode || '').toUpperCase();
        const promos = readData(FILE_PROMOCODES, []);
        const promo = promos.find(p => p.code === promoCode);
        if (!promo || !promo.active || (promo.expiresAt && new Date(promo.expiresAt).getTime() < Date.now())) {
            return res.status(400).json({ success: false, message: 'Hindi valid o na-expire na ang promo code na ito.' });
        }
        if (promo.minSpend && netAfterItemDiscounts < promo.minSpend) {
            return res.status(400).json({ success: false, message: `Kailangan ng minimum na ₱${promo.minSpend.toFixed(2)} para magamit ang promo na ito.` });
        }
        const promoDiscount = promo.type === 'percent' ? (netAfterItemDiscounts * promo.value / 100) : promo.value;
        cartDiscount = Math.round(Math.min(Math.max(promoDiscount, 0), netAfterItemDiscounts) * 100) / 100;
    } else if (discountType === 'MANUAL') {
        cartDiscount = Math.round(Math.min(Math.max(0, parseFloat(transaction.discount) || 0), netAfterItemDiscounts) * 100) / 100;
    } else if (discountType === 'LOYALTY') {
        // Loyalty points redemption — hindi basta tinitiwalaan ang bilang
        // ng points o ang halaga ng discount na ipinasa ng client. Ang
        // TALAGANG points balance ng customer sa database ang batayan,
        // at ang pinal na redeemed count ay ino-overwrite dito
        // (transaction.loyaltyPointsRedeemed) para ito rin ang magiging
        // basehan pagbawas ng points sa customer record sa ibaba.
        if (!storeSettings.loyaltyEnabled) {
            return res.status(400).json({ success: false, message: 'Naka-disable ang Loyalty Points redemption. Puntahan ang Users > Store & Sales para i-enable.' });
        }
        if (!transaction.customerId) {
            return res.status(400).json({ success: false, message: 'Pumili muna ng customer para makagamit ng loyalty points.' });
        }
        const redeemingCustomer = customers.find(c => c.id === transaction.customerId);
        if (!redeemingCustomer) {
            return res.status(400).json({ success: false, message: 'Customer not found.' });
        }
        const requestedPoints = Math.max(0, parseInt(transaction.loyaltyPointsRedeemed) || 0);
        const availablePoints = Math.max(0, redeemingCustomer.points || 0);
        const pointValue = Math.max(0, parseFloat(storeSettings.loyaltyPointValue) || 0);
        let pointsToRedeem = Math.min(requestedPoints, availablePoints);
        cartDiscount = Math.round(Math.min(pointsToRedeem * pointValue, netAfterItemDiscounts) * 100) / 100;
        // Kung na-cap ang discount dahil sa net subtotal, ibalik din ang
        // redeemed points sa proporsyonal na halaga (hindi dapat mas
        // marami ang points na babawasin kaysa sa aktwal na na-apply na discount).
        pointsToRedeem = pointValue > 0 ? Math.floor(cartDiscount / pointValue) : 0;
        cartDiscount = Math.round(pointsToRedeem * pointValue * 100) / 100;
        if (pointsToRedeem <= 0) {
            return res.status(400).json({ success: false, message: 'Walang sapat na loyalty points para gamitin.' });
        }
        transaction.loyaltyPointsRedeemed = pointsToRedeem;

        // --------------------------------------------------------------
        // SECURITY: bago pahintulutan ang aktwal na redemption, kailangan
        // ng isa sa dalawa:
        //   1) VALID na na-scan na Loyalty Card/QR ng customer mismo
        //      (`transaction.loyaltyCardToken`) — awtomatikong authorized
        //      dahil ang paghawak lang ng tamang card/QR ang nagpapatunay
        //      na ito, o
        //   2) Isang `transaction.loyaltyAuthPassword` na tumutugma sa
        //      Admin o sa account na may `loyalty_redeem_own_password`
        //      permission — para sa MANUAL na redemption (walang scan).
        // Kagaya ito ng ginagawang pattern sa Manual Discount / Void /
        // Shift Close sa ibaba.
        // --------------------------------------------------------------
        const cardToken = transaction.loyaltyCardToken;
        if (cardToken) {
            const cardCheck = verifyLoyaltyCardToken(redeemingCustomer, cardToken);
            if (!cardCheck.valid) {
                return res.status(403).json({ success: false, code:'LOYALTY_CARD_INVALID', message: cardCheck.message });
            }
            transaction.loyaltyAuthorizedBy = 'Customer Loyalty Card/QR Scan';
            transaction._loyaltyCardRotate = cardCheck.mode ==='rotating';
        } else {
            const loyaltyAuthPassword = transaction.loyaltyAuthPassword;
            if (!loyaltyAuthPassword) {
                return res.status(400).json({
                    success: false,
                    code:'LOYALTY_AUTH_REQUIRED',
                    message:'I-scan ang Loyalty Card/QR ng customer, o maglagay ng Admin/Supervisor password para sa manual na pag-redeem.'
                });
            }
            const loyaltyAuthUsers = readData(FILE_USERS);
            const loyaltyAuthResult = await findLoyaltyRedeemAuthorizer(loyaltyAuthUsers, loyaltyAuthPassword);
            if (!loyaltyAuthResult) {
                return res.status(403).json({ success: false, code:'WRONG_ADMIN_PASSWORD', message:'Incorrect password. Loyalty points redemption was not authorized.' });
            }
            transaction.loyaltyAuthorizedBy = loyaltyAuthResult.isAdmin
                ? `${loyaltyAuthResult.user.username} (Admin, manual)`
                : `${loyaltyAuthResult.user.username} (RBAC, manual)`;
        }
    }

    // ------------------------------------------------------------------
    // SECURITY FIX: previously, manual discounts (item-level
    // `itemDiscount` and cart-level `discountType: MANUAL`) were simply
    // accepted from whoever was logged into the terminal (even a
    // Cashier) — no password or approval was required, unlike
    // void/shift-close. This was the easiest "sweethearting" fraud
    // vector: lower the price in the system while still charging the
    // customer full price, then pocket the difference. Now, whenever
    // there's a manual discount (item or cart) GREATER THAN ZERO, a
    // `transaction.discountAuthPassword` is required that matches the
    // Admin or an account with the `manual_discount_own_password`
    // permission — checked against ALL accounts (same as void), since
    // it's usually a Supervisor typing their own password, not the
    // Cashier logged into the terminal.
    // ------------------------------------------------------------------
    const manualDiscountTotal = Math.round((itemDiscountTotal + (discountType ==='MANUAL' ? cartDiscount : 0)) * 100) / 100;
    let discountAuthorizedBy = null;

    if (manualDiscountTotal > 0) {
        const discountAuthPassword = transaction.discountAuthPassword;
        if (!discountAuthPassword) {
            return res.status(400).json({
                success: false,
                code:'DISCOUNT_AUTH_REQUIRED',
                message:'An Admin/Supervisor password is required to authorize this manual discount.'
            });
        }
        const authUsers = readData(FILE_USERS);
        const discountAuthResult = await findManualDiscountAuthorizer(authUsers, discountAuthPassword);
        if (!discountAuthResult) {
            return res.status(403).json({
                success: false,
                code:'WRONG_ADMIN_PASSWORD',
                message:'Incorrect password. Manual discount was not authorized.'
            });
        }
        discountAuthorizedBy = discountAuthResult.isAdmin
            ? `${discountAuthResult.user.username} (Admin)`
            : `${discountAuthResult.user.username} (RBAC)`;
    }

    const verifiedTotal = Math.max(0, Math.round((netAfterItemDiscounts - cartDiscount) * 100) / 100);

    // Tax computation (base sa Store & Sales Settings). Kapag "prices
    // include tax" ang setup, HINDI na dinadagdag ang tax sa total —
    // in-eextract lang ito para sa reporting (ang presyo mismo ng produkto
    // ay itinuturing na tax-inclusive na). Kapag hindi kasama ang tax sa
    // presyo, idinadagdag ito sa verifiedTotal para makuha ang GRAND total
    // na dapat bayaran.
    let taxAmount = 0;
    const taxRatePct = Math.min(Math.max(0, storeSettings.taxRate), 100);
    if (storeSettings.taxEnabled && taxRatePct > 0) {
        if (storeSettings.pricesIncludeTax) {
            taxAmount = Math.round((verifiedTotal - (verifiedTotal / (1 + taxRatePct / 100))) * 100) / 100;
        } else {
            taxAmount = Math.round(verifiedTotal * (taxRatePct / 100) * 100) / 100;
        }
    }
    const grandTotal = (storeSettings.taxEnabled && !storeSettings.pricesIncludeTax)
        ? Math.round((verifiedTotal + taxAmount) * 100) / 100
        : verifiedTotal;

    // Kumpirmahin na ang binayad (single payment o split payments) ay
    // sapat para sa VERIFIED total (kasama na ang tax kung applicable) —
    // dati'y hindi ito kinukumpirma laban sa recomputed na halaga.
    const tendered = Array.isArray(transaction.payments) && transaction.payments.length > 0
        ? transaction.payments.reduce((s, p) => s + (parseFloat(p.amount) || 0), 0)
        : (parseFloat(transaction.received ?? transaction.amount_paid) || 0);

    if (Math.round(tendered * 100) / 100 < grandTotal - 0.01) {
        return res.status(400).json({
            success: false,
            message: `Hindi tama ang binayad — kulang ito (₱${tendered.toFixed(2)}) kumpara sa tamang total (₱${grandTotal.toFixed(2)}).`
        });
    }

    // I-overwrite ang mga field na pinagmumulan ng fraud gamit ang
    // SERVER-VERIFIED na values — hindi na ito galing direkta sa client.
    transaction.items = resolvedItems;
    transaction.discount = cartDiscount;
    transaction.subtotalBeforeTax = verifiedTotal;
    transaction.taxRate = storeSettings.taxEnabled ? taxRatePct : 0;
    transaction.taxAmount = taxAmount;
    transaction.taxInclusive = !!storeSettings.pricesIncludeTax;
    transaction.total = grandTotal;
    transaction.change = Math.round((tendered - grandTotal) * 100) / 100;
    transaction.discountAuthorizedBy = discountAuthorizedBy;
    delete transaction.discountAuthPassword;
    // Sensitive: hindi dapat i-persist ang plaintext password/raw card
    // token sa saved transaction record — na-verify na ito sa itaas,
    // ang `loyaltyAuthorizedBy` label na lang ang kailangan sa audit trail.
    delete transaction.loyaltyAuthPassword;
    delete transaction.loyaltyCardToken;

    transaction.items.forEach(item => {
        const prod = products.find(p => p.code === item.code);
        if (prod) {
            prod.stock = Math.max(0, prod.stock - item.quantity);
        }
    });

    let newLoyaltyCardToken = null;
    if (transaction.customerId) {
        const cust = customers.find(c => c.id === transaction.customerId);
        if (cust) {
            // Ang `redeem` dito ay galing na sa server-validated
            // `transaction.loyaltyPointsRedeemed` (na-overwrite sa itaas sa
            // LOYALTY discount branch, hindi mula sa raw client input),
            // kaya siguradong hindi ito lalagpas sa totoong points balance.
            const redeem = discountType === 'LOYALTY' ? Math.max(0, parseInt(transaction.loyaltyPointsRedeemed) || 0) : 0;
            if (redeem > 0) {
                cust.points = Math.max(0, (cust.points || 0) - redeem);

                // ANTI-REPLAY: kung 'rotating' ang mode ng card/QR na ginamit
                // sa redemption na ito, awtomatikong bumubuo ng BAGONG
                // secret/QR ngayon din — ang dating QR (kahit may litrato o
                // screenshot nito ang sinuman) ay wala nang silbi simula
                // ngayon. Ibabalik ang bagong token sa response para
                // maipakita/ma-refresh agad sa customer.
                if (transaction._loyaltyCardRotate && cust.loyaltyCard && !cust.loyaltyCard.revoked) {
                    const rotated = issueLoyaltyCard(cust, 'rotating', transaction.loyaltyAuthorizedBy || 'system');
                    newLoyaltyCardToken = rotated.token;
                }
            } else {
                transaction.loyaltyPointsRedeemed = 0;
            }
            delete transaction._loyaltyCardRotate;

            const earnRate = storeSettings.loyaltyEnabled ? (parseFloat(storeSettings.loyaltyEarnRate) || 100) : 0;
            const earned = earnRate > 0 ? Math.floor((parseFloat(transaction.total) || 0) / earnRate) : 0;
            cust.points = (cust.points || 0) + earned;
            cust.totalSpent = Math.round(((cust.totalSpent || 0) + (parseFloat(transaction.total) || 0)) * 100) / 100;
            cust.visits = (cust.visits || 0) + 1;
            cust.lastVisit = new Date().toISOString();

            transaction.customerName = cust.name;
            transaction.customerEmail = cust.email ||'';
            transaction.loyaltyPointsEarned = earned;
            transaction.loyaltyPointsBalance = cust.points;

            writeData(FILE_CUSTOMERS, customers);
        }
    }

    transactions.unshift(transaction);
    writeData(FILE_TRANSACTIONS, transactions);
    writeData(FILE_PRODUCTS, products);

    logAction(username, `Processed sale transaction: ${transaction.id}`
        + (discountAuthorizedBy ? ` (Manual discount ₱${manualDiscountTotal.toFixed(2)} authorized by: ${discountAuthorizedBy})` : '')
        + (transaction.loyaltyAuthorizedBy ? ` (Loyalty redemption authorized by: ${transaction.loyaltyAuthorizedBy})` : ''));

    // Advanced Settings: Sale Webhook — fire-and-forget POST to the
    // configured integration URL (Zapier/Make/accounting/etc.). Never
    // blocks or fails the sale itself if the webhook is slow/down.
    try {
        const advSettings = getAdvancedSettingsPublic(readData(FILE_ADVANCED_SETTINGS, DEFAULT_ADVANCED_SETTINGS));
        if (advSettings.saleWebhookEnabled && advSettings.saleWebhookUrl && typeof fetch === 'function') {
            fetch(advSettings.saleWebhookUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    event: 'sale.completed',
                    transactionId: transaction.id,
                    total: transaction.total,
                    cashier: transaction.cashier,
                    itemCount: (transaction.items || []).length,
                    timestamp: transaction.isoDate || new Date().toISOString()
                })
            }).catch(err => console.error('Sale webhook delivery failed:', err.message));
        }
    } catch (webhookErr) {
        console.error('Sale webhook error:', webhookErr.message);
    }

    // `newLoyaltyCardToken` ay ipapakita/ipi-print LAMANG sa sandaling ito
    // (kagaya ng pag-issue) — hindi ito naka-imbak kahit saan bilang
    // plaintext, at hindi rin bahagi ng saved transaction record.
    res.json({ success: true, currentTransaction: transaction, newLoyaltyCardToken });

});

app.get('/api/cart/:username', (req, res) => {
    const username = req.params.username;

    const isOwner = req.authUser.username.toLowerCase() === username.toLowerCase();
    const isAdmin = req.authUser.role.toLowerCase() ==='admin';
    if (!isOwner && !isAdmin) {
        return res.status(403).json({ success: false, message:'Akses Denied: Hindi mo pwedeng tingnan ang cart ng ibang user.' });
    }

    const cartsData = readData(FILE_CARTS, {});

    res.json({ success: true, cart: cartsData[username] || [] });
});

app.post('/api/cart', (req, res) => {
    const { username, cart } = req.body;
    if (!username) {
        return res.status(400).json({ success: false, message:'Missing username' });
    }

    const cartsData = readData(FILE_CARTS, {});

    cartsData[username] = cart;

    writeData(FILE_CARTS, cartsData);
    res.json({ success: true, message:'Cart saved to database successfully.' });
});

app.get('/api/transactions', (req, res) => {
    const { requester } = req.query;
    const allTransactions = readData(FILE_TRANSACTIONS);

    if (!requester) {
        return res.json(allTransactions);
    }

    const users = readData(FILE_USERS);
    const activeUser = users.find(u => u.username.toLowerCase() === requester.toLowerCase());

    const activeRole = activeUser && activeUser.role;
    const isAdminRole = (activeRole ||'').toLowerCase() ==='admin';
    const canViewAll = isAdminRole || !!getPermissionsForRole(activeRole).transactions_view_all;
    if (canViewAll) {
        return res.json(allTransactions);
    }

    const ownTransactions = allTransactions.filter(
        tx => (tx.cashier ||'').toLowerCase() === requester.toLowerCase()
    );
    res.json(ownTransactions);
});

app.post('/api/transactions/:transactionId/email-receipt', rateLimit('email-receipt', 20, 15 * 60 * 1000), async (req, res) => {
    const { transactionId } = req.params;
    const { toEmail, transaction: clientTx, receiptImage } = req.body;

    const emailPattern =/^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!toEmail || !emailPattern.test(toEmail)) {
        return res.status(400).json({ success: false, message:'Di-wastong email address.' });
    }

    const transactions = readData(FILE_TRANSACTIONS, []);
    const tx = transactions.find(t => t.id === transactionId) || clientTx;
    if (!tx) {
        return res.status(404).json({ success: false, message:'Hindi mahanap ang transaction record na ito.' });
    }

    const settings = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);
    const mailCreds = getOtpMailCredentials(settings);
    if (!mailCreds) {
        return res.status(400).json({
            success: false,
            message:'Wala pang naka-configure na Sender Gmail. I-setup muna ito sa Users > Receipt Customization > OTP Sender Email.'
        });
    }

    try {
        const itemLines = (tx.items || []).map(i => {
            const itemDiscount = Math.max(0, parseFloat(i.itemDiscount) || 0);
            const lineTotal = ((parseFloat(i.price) || 0) * (parseInt(i.quantity) || 0)) - itemDiscount;
            return `  ${i.name} x${i.quantity} .......... ₱${lineTotal.toFixed(2)}`;
        }).join('\n');

        const paymentLine = (tx.payments && Array.isArray(tx.payments) && tx.payments.length > 1)
            ? tx.payments.map(p => `${p.method} ₱${parseFloat(p.amount).toFixed(2)}`).join(' + ')
            : (tx.method || tx.payment_method ||'CASH');

        const storeName = settings.storeName ||'OmniPOS';
        const textBody = `${storeName}\n${settings.storeAddress ||''}\n\nReceipt: ${tx.id}\nDate: ${tx.timestamp ||''}\nCashier: ${tx.cashier ||''}\n\n${itemLines}\n\nTOTAL: ₱${parseFloat(tx.total || 0).toFixed(2)}\nPayment (${paymentLine})\n\n${settings.footerText ||'Thank you for shopping!'}`;

        const mailOptions = {
            from: `"${storeName}" <${mailCreds.user}>`,
            to: toEmail,
            subject: `Receipt ${tx.id} - ${storeName}`,
            text: textBody
        };

        if (typeof receiptImage ==='string' && receiptImage.startsWith('data:image/')) {
            try {
                const match = receiptImage.match(/^data:image\/(png|jpeg|jpg);base64,(.+)$/);
                if (match) {
                    const ext = match[1] ==='jpg' ?'jpeg' : match[1];
                    const base64Data = match[2];

                    if (base64Data.length < 2_800_000) {
                        mailOptions.attachments = [{
                            filename: `receipt-${tx.id}.${ext ==='jpeg' ?'jpg' :'png'}`,
                            content: base64Data,
                            encoding:'base64'
                        }];
                    }
                }
            } catch (imgErr) {
                console.warn('Hindi na-attach ang receipt image:', imgErr.message);
            }
        }

        await sendMailSmart(mailCreds.user, mailCreds.pass, mailOptions);

        logAction(req.authUser ? req.authUser.username :'Unknown', `Naipadala ang resibo ${tx.id} sa email (${maskEmail(toEmail)})`);
        res.json({ success: true, message:'Naipadala ang resibo.' });
    } catch (err) {
        console.error('Email receipt failed:', err.message);
        res.status(500).json({ success: false, message: `Hindi naipadala ang resibo: ${err.message}` });
    }
});

app.get('/api/logs', requirePermission('logs'), (req, res) => {
    try {
        const logs = readData(FILE_USERLOGS, []);
        res.json({ success: true, data: logs });
    } catch (error) {
        console.error("Error reading logs:", error);
        res.status(500).json({ success: false, message:'Hindi makuha ang system logs.' });
    }
});

app.get('/api/users', requirePermission('users'), (req, res) => {
    const users = readData(FILE_USERS);

    const safeUsers = users.map(({ password, webauthnCredentials, ...rest }) => rest);
    res.json(safeUsers);
});

app.get('/api/users/self', (req, res) => {
    const users = readData(FILE_USERS);
    const me = users.find(u => u.username.toLowerCase() === req.authUser.username.toLowerCase());
    if (!me) return res.status(404).json({ success: false, message:'Account not found.' });
    res.json({ success: true, username: me.username, role: me.role, avatar: me.avatar || null, created: me.created || null });
});

function applyProfileChanges(currentUsername, { avatar, username: newUsernameRaw }) {
    let users = readData(FILE_USERS);
    const userIndex = users.findIndex(u => u.username.toLowerCase() === currentUsername.toLowerCase());
    if (userIndex === -1) {
        return { ok: false, error:'Account not found.' };
    }

    const newUsername = typeof newUsernameRaw ==='string' ? newUsernameRaw.trim() :'';
    const isRenaming = newUsername && newUsername.toLowerCase() !== currentUsername.toLowerCase();

    if (isRenaming) {
        if (!/^[a-zA-Z0-9_.\-]{3,32}$/.test(newUsername)) {
            return { ok: false, error:'Invalid na username. 3-32 characters lang, walang space (pwede lang letra, numero, "_", "." at "-").' };
        }
        const taken = users.some((u, i) => i !== userIndex && u.username.toLowerCase() === newUsername.toLowerCase());
        if (taken) {
            return { ok: false, error:'Kinuha na ng ibang account ang username na iyan.' };
        }
    }

    if (typeof avatar !=='undefined') {
        users[userIndex].avatar = avatar || null;
    }
    const finalUsername = isRenaming ? newUsername : users[userIndex].username;
    if (isRenaming) {
        users[userIndex].username = finalUsername;
    }
    writeData(FILE_USERS, users);

    if (isRenaming) {
        renameUsernameEverywhere(currentUsername, finalUsername);
    }

    return { ok: true, user: users[userIndex], renamedFrom: isRenaming ? currentUsername : null };
}

app.put('/api/users/self/profile', rateLimit('self-edit-profile', 15, 10 * 60 * 1000), (req, res) => {
    const { avatar, username: newUsername } = req.body;
    const actingUsername = req.authUser.username;
    const isAdminRole = (req.authUser.role ||'').toLowerCase() ==='admin';
    const canApplyDirectly = isAdminRole || !!getPermissionsForRole(req.authUser.role).edit_user_profile;

    if (canApplyDirectly) {
        const result = applyProfileChanges(actingUsername, { avatar, username: newUsername });
        if (!result.ok) {
            return res.status(400).json({ success: false, message: result.error });
        }
        if (result.renamedFrom) {
            logAction(result.user.username, `Changed own username from "${result.renamedFrom}" to "${result.user.username}"`);
        } else {
            logAction(actingUsername, `Updated own profile (Edit Profile widget)`);
        }
        return res.json({
            success: true,
            pending: false,
            message:'Na-update na ang profile mo.',
            username: result.user.username,
            avatar: result.user.avatar || null,
            usernameChanged: !!result.renamedFrom
        });
    }

    const trimmedNewUsername = typeof newUsername ==='string' ? newUsername.trim() :'';
    if (trimmedNewUsername && trimmedNewUsername.toLowerCase() !== actingUsername.toLowerCase()) {
        if (!/^[a-zA-Z0-9_.\-]{3,32}$/.test(trimmedNewUsername)) {
            return res.status(400).json({ success: false, message:'Invalid na username. 3-32 characters lang, walang space (pwede lang letra, numero, "_", "." at "-").' });
        }
        const users = readData(FILE_USERS);
        const taken = users.some(u => u.username.toLowerCase() === trimmedNewUsername.toLowerCase());
        if (taken) {
            return res.status(400).json({ success: false, message:'Kinuha na ng ibang account ang username na iyan.' });
        }
    }

    let requests = readData(FILE_REQUESTS);
    requests.push({
        id: Date.now(),
        type:'PROFILE_UPDATE',
        targetUser: actingUsername,
        requester: actingUsername,
        data: { avatar: typeof avatar ==='undefined' ? undefined : (avatar || null), username: trimmedNewUsername || undefined },
        timestamp: new Date().toLocaleString()
    });
    writeData(FILE_REQUESTS, requests);
    logAction(actingUsername, `Submitted an Edit Profile request for Admin approval`);
    res.json({ success: true, pending: true, message:'Naisumite ang iyong Edit Profile request. Hihintayin ang pag-approve ng Admin.' });
});

app.put('/api/users/:targetUser/avatar', rateLimit('admin-set-avatar', 20, 10 * 60 * 1000), verifyAdmin, (req, res) => {
    const { targetUser } = req.params;
    const { avatar, username } = req.body;

    let users = readData(FILE_USERS);
    const userIndex = users.findIndex(u => u.username.toLowerCase() === targetUser.toLowerCase());
    if (userIndex === -1) {
        return res.status(404).json({ success: false, message:'User account not found.' });
    }

    users[userIndex].avatar = avatar || null;
    writeData(FILE_USERS, users);
    logAction(username, `Updated profile picture for account: ${targetUser}`);
    res.json({ success: true, message: `Profile picture for ${targetUser} has been updated.`, avatar: users[userIndex].avatar });
});

app.post('/api/users/self/change-password', rateLimit('self-change-pw', 8, 10 * 60 * 1000), (req, res) => {
    const { currentPassword, newPassword } = req.body;

    if (!currentPassword || !newPassword) {
        return res.status(400).json({ success: false, message:'Kailangan ang kasalukuyan at bagong password.' });
    }
    if (String(newPassword).length < 4) {
        return res.status(400).json({ success: false, message:'Masyadong maikli ang bagong password.' });
    }

    let users = readData(FILE_USERS);
    const userIndex = users.findIndex(u => u.username.toLowerCase() === req.authUser.username.toLowerCase());
    if (userIndex === -1) {
        return res.status(404).json({ success: false, message:'Account not found.' });
    }

    const me = users[userIndex];
    let isMatch = false;
    try {
        isMatch = bcrypt.compareSync(currentPassword, me.password);
    } catch (e) {
        isMatch = (currentPassword === me.password);
    }
    if (!isMatch) {
        return res.status(403).json({ success: false, code:'WRONG_CURRENT_PASSWORD', message:'Mali ang kasalukuyang password.' });
    }

    users[userIndex].password = bcrypt.hashSync(newPassword, 10);
    writeData(FILE_USERS, users);
    logAction(me.username, `Changed own account password`);
    res.json({ success: true, message:'Na-update na ang password mo.' });
});

app.post('/api/users', rateLimit('admin-add-user', 8, 10 * 60 * 1000), verifyAdmin, (req, res) => {
    const { user, username } = req.body;

    let users = readData(FILE_USERS);
    if (users.some(u => u.username.toLowerCase() === user.username.toLowerCase())) {
        return res.status(400).json({ success: false, message:'Username is already taken.' });
    }

    user.password = bcrypt.hashSync(user.password, 10);
    user.created = new Date().toISOString().replace('T',' ').substring(0, 19);
    users.push(user);
    writeData(FILE_USERS, users);
    logAction(username, `Created new POS account: ${user.username}`);
    res.json({ success: true, message:'User created successfully.' });
});

app.put('/api/users/:targetUser/reset-password', rateLimit('admin-reset-pw', 8, 10 * 60 * 1000), verifyAdmin, (req, res) => {
    const { targetUser } = req.params;
    const { newPassword, username } = req.body;

    let users = readData(FILE_USERS);
    const userIndex = users.findIndex(u => u.username.toLowerCase() === targetUser.toLowerCase());

    if (userIndex === -1) {
        return res.status(404).json({ success: false, message:'User account not found.' });
    }

    users[userIndex].password = bcrypt.hashSync(newPassword, 10);
    writeData(FILE_USERS, users);

    logAction(username, `Force reset password for account: ${targetUser}`);
    res.json({ success: true, message: `Password for ${targetUser} has been updated successfully.` });
});

app.post('/api/users/delete-account', rateLimit('admin-delete-user', 8, 10 * 60 * 1000), verifyAdmin, (req, res) => {
    const { targetUser, username } = req.body;

    if (!targetUser) {
        return res.status(400).json({ success: false, message:'Kulang ang target user na buburahin.' });
    }

    if (targetUser.toLowerCase() === username.toLowerCase()) {
        return res.status(400).json({ success: false, message:'Bawal mong burahin ang sarili mong account habang naka-login!' });
    }

    let users = readData(FILE_USERS);
    const filteredUsers = users.filter(u => u.username.toLowerCase() !== targetUser.toLowerCase());

    if (users.length === filteredUsers.length) {
        return res.status(404).json({ success: false, message:'Account to delete not found.' });
    }

    writeData(FILE_USERS, filteredUsers);

    for (const [token, session] of SESSIONS.entries()) {
        if (session.username.toLowerCase() === targetUser.toLowerCase()) {
            SESSIONS.delete(token);
        }
    }
    persistSessions();

    logAction(username, `Deleted user account: ${targetUser}`);
    res.json({ success: true, message: `Account ${targetUser} has been completely removed.` });
});

function logAction(username, action) {
    let logs = readData(FILE_USERLOGS);
    logs.unshift({
        id: Date.now(),
        username: username,
        action: action,
        timestamp: new Date().toLocaleString('en-US', { timeZone:'Asia/Manila' })
    });
    writeData(FILE_USERLOGS, logs);
}

function logVoidAction(username, transactionId, voidedAmount, authMethodLabel) {
    let logs = readData(FILE_USERLOGS);
    logs.unshift({
        id: Date.now(),
        username: username,
        action: `VOIDED Transaction ID: ${transactionId} (${authMethodLabel})`,
        timestamp: new Date().toLocaleString('en-US', { timeZone:'Asia/Manila' }),
        voidedAmount: Math.round((parseFloat(voidedAmount) || 0) * 100) / 100,
        voidedTransactionId: transactionId
    });
    writeData(FILE_USERLOGS, logs);
}

function logRefundAction(username, transactionId, refundAmount, itemsLabel, reason, authMethodLabel) {
    let logs = readData(FILE_USERLOGS);
    logs.unshift({
        id: Date.now(),
        username: username,
        action: `REFUNDED ₱${refundAmount.toFixed(2)} sa Transaction ID: ${transactionId} — Items: ${itemsLabel}. Dahilan: ${reason || '(walang isinulat)'} (${authMethodLabel})`,
        timestamp: new Date().toLocaleString('en-US', { timeZone:'Asia/Manila' }),
        refundedAmount: Math.round((parseFloat(refundAmount) || 0) * 100) / 100,
        refundedTransactionId: transactionId
    });
    writeData(FILE_USERLOGS, logs);
}

// --------------------------------------------------------------
// GET /api/system/update-check
// Tinatawag ito ng "Check for Updates" button sa Settings. Tumatawag
// ito papunta sa RELAY (developer-hosted, tingnan ang /relay/latest-version)
// para malaman kung may bagong na-publish na version, tapos ikino-
// compare ito sa sariling APP_VERSION (mula sa package.json) ng
// INSTANCE na ito. Admin-only — hindi na kailangang ipakita ito sa
// mga cashier/staff.
// --------------------------------------------------------------
// --------------------------------------------------------------
// BACKUP STATUS — para sa isang warning banner sa Admin Dashboard kapag
// paulit-ulit nang nabibigo ang scheduled local database backup (hal.
// puno na ang storage, walang write permission). Dating tahimik lang
// ito nabibigo sa likod (console.error na lang) — ngayon, kahit hindi
// titingnan ng Admin ang server logs, may makikita silang alerto sa UI.
// Admin-only — hindi na kailangang ipakita ito sa mga cashier/staff.
// --------------------------------------------------------------
app.get('/api/system/backup-status', (req, res) => {
    if (!req.authUser || req.authUser.role.toLowerCase() !=='admin') {
        return res.status(403).json({ success: false, message:'Admin privileges lamang ang makakakita ng backup status.' });
    }
    const status = getBackupStatus();
    res.json({ success: true, status });
});

app.get('/api/system/update-check', rateLimit('system-update-check', 10, 10 * 60 * 1000), async (req, res) => {
    if (!req.authUser || req.authUser.role.toLowerCase() !=='admin') {
        return res.status(403).json({ success: false, message:'Admin privileges lamang ang makakagamit ng Check for Updates.' });
    }
    if (!RELAY_API_KEY) {
        return res.status(400).json({ success: false, message:'Walang RELAY_API_KEY na naka-configure sa server na ito.' });
    }
    if (getConnectivityMode() === 'offline') {
        return res.status(400).json({ success: false, message: 'Naka-OFFLINE mode ka ngayon. I-switch muna sa Online para makapag-check ng updates.' });
    }
    try {
        // Ipinapasa ang sariling installationId dito para masuri ng
        // RELAY kung may TARGETED release na naka-set PARA SA DEVICE
        // NA ITO lang (tingnan ang targetedReleases sa RELAY server.js)
        // — kung wala, babalik lang ito sa dating gawi (global version).
        const installationId = getOrCreateInstallationId(readFeatureUnlocks());
        const relayRes = await relayFetch(`${RELAY_URL}/relay/latest-version?installationId=${encodeURIComponent(installationId)}`, {
            headers: {'x-relay-key': RELAY_API_KEY }
        });
        const relayData = await parseRelayResponse(relayRes);
        if (!relayData.success) {
            return res.status(502).json({ success: false, message: relayData.message ||'Tinanggihan ng RELAY ang version check.' });
        }
        const publishedVersion = String(relayData.latestVersion || UNPUBLISHED_VERSION_SENTINEL).trim();
        // Kung sentinel/unpublished ang laman ng RELAY (walang na-publish
        // pa, o nawala ito dahil sa redeploy na walang persistent storage),
        // huwag itong ituring na "bagong update" kahit hindi pareho sa
        // APP_VERSION. Kailangan din talagang MAS BAGO (hindi basta
        // "iba") bago i-flag bilang available.
        const updateAvailable = publishedVersion !== UNPUBLISHED_VERSION_SENTINEL
            && isVersionNewer(publishedVersion, APP_VERSION);
        res.json({
            success: true,
            currentVersion: APP_VERSION,
            latestVersion: publishedVersion,
            changelog: relayData.changelog ||'',
            updateAvailable
        });
    } catch (err) {
        res.status(502).json({ success: false, message: `Hindi ma-check ang RELAY para sa bagong version: ${err.message}` });
    }
});

// --------------------------------------------------------------
// POST /api/system/deploy-update
// Ito ang "Check & Deploy Update" button. Dalawang paraan, depende sa
// environment ng instance na ito:
//
//   1. RENDER MODE (dating gawi): kung naka-configure ang
//      RENDER_DEPLOY_HOOK_URL env var, i-POST lang papunta rito — ito
//      ang nagre-redeploy sa Render mula sa ANUMANG NASA REPO NA (dapat
//      naka-sync/git-merge na muna mula sa upstream bago gamitin ito).
//
//   2. SELF-UPDATE MODE (bago — para sa Termux/lokal na kliyente na
//      WALANG Render, kung saan hindi gumagana ang deploy hook dahil
//      walang Render service talaga): direktang kinukuha ang bagong
//      release package MISMO mula sa RELAY (GET /relay/release
//      -package), ie-extract ito sa isang HIWALAY na staging folder
//      muna, saka lang ikino-copy paibabaw sa install folder — HINDI
//      kasama ang .env at database/ (ligtas ang mga ito) — tapos
//      awtomatikong nire-restart ang sariling Node process.
// --------------------------------------------------------------
app.post('/api/system/deploy-update', rateLimit('system-deploy-update', 3, 30 * 60 * 1000), async (req, res) => {
    if (!req.authUser || req.authUser.role.toLowerCase() !=='admin') {
        return res.status(403).json({ success: false, message:'Admin privileges lamang ang makakapag-trigger ng deploy.' });
    }

    if (RENDER_DEPLOY_HOOK_URL) {
        try {
            const hookRes = await fetch(RENDER_DEPLOY_HOOK_URL, { method:'POST' });
            if (!hookRes.ok) {
                return res.status(502).json({ success: false, message: `Tinanggihan ng Render ang deploy hook (HTTP ${hookRes.status}).` });
            }
            logAction(req.authUser.username ||'Unknown','Nag-trigger ng System Update Deploy sa Render.');
            return res.json({ success: true, message:'Na-trigger na ang bagong deploy sa Render. Aabutin ito ng ilang minuto — mag-a-auto-refresh ang system pagkatapos.' });
        } catch (err) {
            return res.status(502).json({ success: false, message: `Hindi ma-abot ang Render deploy hook: ${err.message}` });
        }
    }

    // Walang RENDER_DEPLOY_HOOK_URL na naka-configure — ibig sabihin
    // hindi ito naka-deploy sa Render (hal. Termux). Gamitin ang
    // self-update mode sa halip.
    return runSelfUpdateFromRelay(req, res);
});

// --------------------------------------------------------------
// SELF-UPDATE MODE (Termux-friendly, walang Render deploy hook)
// --------------------------------------------------------------
const SELF_UPDATE_PRESERVE = new Set([
   '.env','.env.key','database','node_modules','uploads_tmp','.git','release',
   'cf.log','server.log'
]);

function copyRecursivePreserving(srcDir, destDir, preserveNames) {
    for (const entry of fs.readdirSync(srcDir, { withFileTypes: true })) {
        if (preserveNames.has(entry.name)) continue; // huwag galawin — panatilihing buo
        const srcPath = path.join(srcDir, entry.name);
        const destPath = path.join(destDir, entry.name);
        if (entry.isDirectory()) {
            fs.mkdirSync(destPath, { recursive: true });
            copyRecursivePreserving(srcPath, destPath, new Set());
        } else {
            fs.mkdirSync(path.dirname(destPath), { recursive: true });
            fs.copyFileSync(srcPath, destPath);
        }
    }
}

// --------------------------------------------------------------
// PAALALA (Termux): dati, dito mismo nag-so-spawn ng sarili niyang
// "detached" child process si Node para i-restart ang sarili. Hindi
// ito maaasahan sa Termux/Android — kapag na-minimize o na-close ng
// customer ang Termux app, pinapatay ng Android ang BUONG session
// (kasama na ang mga "detached" child), kaya paulit-ulit na
// nade-deactivate ang server at kailangan pa ring i-run manually.
//
// Sa halip, dapat pinapatakbo na ang OMNIPOS via "start.sh" (isang
// supervisor loop na paulit-ulit na nagpapatakbo ng "node server.js").
// Dito, sapat na lang na lumabas (exit) ang kasalukuyang Node process
// — ang start.sh loop mismo (hindi si Node) ang bahalang mag-restart
// nito kaagad, kahit anong dahilan ng pagkawala (self-update, crash,
// atbp.), habang bukas pa ang Termux session.
// --------------------------------------------------------------
function scheduleSelfRestart(installRoot) {
    // Maikling delay lang — para may sapat na oras ang HTTP response
    // sa itaas na maka-abot muna sa client/browser bago pa lumabas
    // ang kasalukuyang process.
    setTimeout(() => process.exit(0), 500);
}

async function runSelfUpdateFromRelay(req, res) {
    if (!RELAY_API_KEY) {
        return res.status(400).json({ success: false, message:'Walang RELAY_API_KEY na naka-configure — kailangan ito para makakuha ng release package mula sa RELAY.' });
    }
    if (getConnectivityMode() === 'offline') {
        return res.status(400).json({ success: false, message:'Naka-OFFLINE mode ka ngayon. I-switch muna sa Online para makapag-self-update.' });
    }

    const installRoot = __dirname;
    const tmpRoot = path.join(os.tmpdir(), `omnipos-selfupdate-${Date.now()}`);
    const zipPath = path.join(tmpRoot,'omnipos-client.zip');
    const extractDir = path.join(tmpRoot,'extracted');

    try {
        fs.mkdirSync(tmpRoot, { recursive: true });

        // 1. i-download ang bagong release package mula sa RELAY
        const relayRes = await relayFetch(`${RELAY_URL}/relay/release-package`, {
            headers: {'x-relay-key': RELAY_API_KEY }
        });
        if (!relayRes.ok) {
            let detail ='';
            try { detail = (await relayRes.json()).message ||''; } catch (_e) {}
            throw new Error(`Tinanggihan ng RELAY ang release package (HTTP ${relayRes.status}). ${detail}`.trim());
        }
        const arrayBuffer = await relayRes.arrayBuffer();
        fs.writeFileSync(zipPath, Buffer.from(arrayBuffer));

        // 2. i-extract sa isang HIWALAY na staging folder muna (hindi
        // direkta sa install root) — kung sakaling masira ang download
        // o extract, hindi pa naaapektuhan ang kasalukuyang gumaganang
        // install.
        fs.mkdirSync(extractDir, { recursive: true });
        try {
            execSync(`unzip -o "${zipPath}" -d "${extractDir}"`, { stdio:'pipe' });
        } catch (unzipErr) {
            throw new Error(`Hindi ma-extract ang release package. Siguraduhing naka-install ang "unzip" sa Termux ("pkg install unzip -y"). Detalye: ${unzipErr.message}`);
        }

        // 3. i-copy ang laman ng extractDir PAIBABAW sa install root,
        // PERO LAKTAWAN ang mga bagay na dapat manatiling BUO sa
        // kasalukuyang instance: .env (secrets/keys), database/
        // (aktwal na datos ng tindahan), node_modules, at mga runtime
        // log/upload folder.
        copyRecursivePreserving(extractDir, installRoot, SELF_UPDATE_PRESERVE);

        logAction(req.authUser.username ||'Unknown','Nag-self-update ng OMNIPOS mula sa RELAY release package (Termux/non-Render mode).');

        res.json({
            success: true,
            message:'Na-download at na-apply na ang bagong update. Nag-re-restart na ang system ngayon — muling mag-lo-load ang page sa loob ng ilang segundo.'
        });

        // 4. i-restart ang sariling Node process (bagong process, exit
        // ang luma) para maka-load na ang bagong code.
        scheduleSelfRestart(installRoot);
    } catch (err) {
        console.error('❌ Self-update error:', err.message);
        if (!res.headersSent) {
            res.status(502).json({ success: false, message: `Hindi na-apply ang self-update: ${err.message}` });
        }
    } finally {
        fs.rmSync(tmpRoot, { recursive: true, force: true });
    }
}

app.post('/api/system/reset', rateLimit('system-reset', 3, 30 * 60 * 1000), async (req, res) => {

    if (!req.authUser || req.authUser.role.toLowerCase() !=='admin') {
        return res.status(403).json({ success: false, message:'Aksyon Tinanggihan: Admin privileges lamang ang pwedeng mag-factory reset.' });
    }

    const { additionalEmail } = req.body;
    const secondaryEmail = (additionalEmail ||'').trim();

    const receiptSettingsForReset = readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS);
    const otpMailCreds = getOtpMailCredentials(receiptSettingsForReset);

    if (!otpMailCreds) {
        return res.status(400).json({
            success: false,
            message:'Wala pang na-verify na Google App. I-setup at i-verify muna ito sa Users > Receipt Customization > Google App Verification bago magsagawa ng System Hard Reset.'
        });
    }

    const emailPattern =/^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!secondaryEmail || !emailPattern.test(secondaryEmail)) {
        return res.status(400).json({
            success: false,
            message:'Kailangan ang isang wastong Secondary Backup Email — dito ipapadala ang backup file.'
        });
    }

    const backupPayload = {
        timestamp: new Date().toISOString(),
        users: readData(FILE_USERS, []),
        products: readData(FILE_PRODUCTS, []),
        transactions: readData(FILE_TRANSACTIONS, []),
        userlogs: readData(FILE_USERLOGS, []),
        requests: readData(FILE_REQUESTS, []),
        categories: readData(FILE_CATEGORIES, ['Beverages','Dairy','Snacks','Bakery','Grains']),
        carts: readData(FILE_CARTS, {}),
        receiptSettings: readData(FILE_RECEIPT_SETTINGS, DEFAULT_RECEIPT_SETTINGS),
        customers: readData(FILE_CUSTOMERS, []),
        shifts: readData(FILE_SHIFTS, []),
        shiftMeta: readData(FILE_SHIFT_META, {})
    };

    try {
        let recipients = [secondaryEmail];

        const petsa_ng_ayon = new Date().toLocaleDateString('en-PH');
        const mailOptions = {
            from: `"OmniPOS Core System" <${otpMailCreds.user}>`,
            to: recipients.join(', '),
            subject: `💻 OmniPOS: Full System Reset & Synchronized Backup - ${petsa_ng_ayon}`,
            text: `Magandang araw,\n\nAng system database ay sumailalim sa isang Hard Factory Reset.\n\nKasama sa email na ito ang naka-attach na 'omnipos_full_backup.json' na naglalaman ng lahat ng synchronized tables (kasama na ang customers at shift/Z-Reading records) bago isagawa ang pagbura.`,
            attachments: [
                {
                    filename: `omnipos_full_backup_${Date.now()}.json`,
                    content: JSON.stringify(backupPayload, null, 4),
                    contentType:'application/json'
                }
            ]
        };

        await sendMailSmart(otpMailCreds.user, otpMailCreds.pass, mailOptions);

        const secureDefaultUsers = defaultUsers.map(u => ({
            ...u,
            password: bcrypt.hashSync(u.password, 10),
            created: getPHTime()
        }));
        writeData(FILE_USERS, secureDefaultUsers);

        SESSIONS.clear();
        persistSessions();

        writeData(FILE_PRODUCTS, []);

        writeData(FILE_TRANSACTIONS, []);
        writeData(FILE_REQUESTS, []);
        writeData(FILE_CARTS, {});

        writeData(FILE_CUSTOMERS, []);
        writeData(FILE_SHIFTS, []);
        writeData(FILE_SHIFT_META, {});

        const initialCategories = ['Beverages','Dairy','Snacks','Bakery','Grains'];
        writeData(FILE_CATEGORIES, initialCategories);

        // ANTI-CLONE FIX: dating pinapalitan ng DEFAULT_FEATURE_UNLOCKS
        // (na may `installationId: null`) ang BUONG file — dahil dito,
        // pagkatapos ng anti-clone patch (na gumawa ng persisted UUID na
        // nakatago mismo sa file na ito, hindi na sa hardware fingerprint),
        // kasama na ring nawiwipe ang installationId sa bawat hard reset.
        // Resulta: gumagawa ng BAGONG installationId ang susunod na request
        // kahit same physical device/hindi clone — kaya hindi na makikilala
        // ng RELAY ang dating na-unlock na features nito para ma-auto-
        // restore. AYOS: panatilihin ang identity fields (installationId,
        // hardwareFingerprint, verifiedFingerprint, deviceVerified,
        // firstVerifiedAt) — ang tokens/lockedAttempts/lastVerifiedAt lang
        // ang talagang kailangang i-reset dito.
        const preResetIdentity = readFeatureUnlocks();
        writeData(FILE_FEATURE_UNLOCKS, {
            ...DEFAULT_FEATURE_UNLOCKS,
            installationId: preResetIdentity.installationId,
            hardwareFingerprint: preResetIdentity.hardwareFingerprint,
            verifiedFingerprint: preResetIdentity.verifiedFingerprint,
            deviceVerified: preResetIdentity.deviceVerified,
            firstVerifiedAt: preResetIdentity.firstVerifiedAt
        });

        writeData(FILE_USERLOGS, []);

        // AUTO-RESTORE: kapag naka-configure ang RELAY_API_KEY, subukan
        // agad na kunin muli sa RELAY ang mga dating na-unlock na feature
        // para sa installationId na ito (nananatili ito ngayon dahil
        // sinadyang pinreserve sa itaas — tingnan ang "ANTI-CLONE FIX"
        // comment sa itaas). Best-effort lang — hindi ito nagpapabagsak ng
        // reset kung offline o hindi maabot ang relay.
        let restoredCount = 0;
        try {
            const restoreResult = await attemptRelayRestore();
            restoredCount = restoreResult.restoredCount || 0;
        } catch (err) {
            console.warn('⚠️  Auto-restore matapos ang hard reset: hindi na-check ang Relay.', err.message);
        }

        res.json({
            success: true,
            message: `Ang backup ay matagumpay na naipasa sa (${recipients.length}) email address at ang system ay tuluyan nang nalinis.` +
                (restoredCount > 0 ? ` Awtomatikong naibalik ang ${restoredCount} dating na-unlock na feature.` : ''),
            restoredFeatureCount: restoredCount
        });

    } catch (err) {
        console.error("Mail Reset Failure Context:", err);
        res.status(500).json({
            success: false,
            message: `Hindi itinuloy ang reset dahil nabigo ang email verification. Tiyakin na TAMA ang iyong Gmail at 16-character App Password. Error: ${err.message}`
        });
    }
});

app.post('/api/restore-backup', rateLimit('restore-backup', 5, 15 * 60 * 1000), (req, res) => {
    const { username, password, backupData } = req.body;

    const currentUsers = readData(FILE_USERS, []);
    if (currentUsers.length === 0) {
        return res.status(400).json({ success: false, message:"Walang mahanap na records ng mga user sa system." });
    }

    const currentAdmin = currentUsers.find(u => u.username.toLowerCase() === username.toLowerCase() && u.role.toLowerCase() ==='admin');

    if (!currentAdmin) {
        return res.status(403).json({ success: false, message:"Aksyon Tinanggihan: Walang pribilehiyong pang-administrator." });
    }

    if (!bcrypt.compareSync(password, currentAdmin.password)) {

        return res.status(403).json({ success: false, code:'WRONG_ADMIN_PASSWORD', message:"Maling Admin password. Hindi pinahintulutan ang pag-restore." });
    }

    if (!backupData || typeof backupData !=='object') {
        return res.status(400).json({ success: false, message:"May depekto o maling format ang ipinadalang backup file." });
    }

    try {

        let restoredCount = 0;
        if (backupData.users && Array.isArray(backupData.users)) { writeData(FILE_USERS, backupData.users); restoredCount++; }
        if (backupData.products && Array.isArray(backupData.products)) { writeData(FILE_PRODUCTS, backupData.products); restoredCount++; }
        if (backupData.transactions && Array.isArray(backupData.transactions)) { writeData(FILE_TRANSACTIONS, backupData.transactions); restoredCount++; }
        if (backupData.userlogs && Array.isArray(backupData.userlogs)) { writeData(FILE_USERLOGS, backupData.userlogs); restoredCount++; }
        if (backupData.requests && Array.isArray(backupData.requests)) { writeData(FILE_REQUESTS, backupData.requests); restoredCount++; }
        if (backupData.categories && Array.isArray(backupData.categories)) { writeData(FILE_CATEGORIES, backupData.categories); restoredCount++; }
        if (backupData.carts && typeof backupData.carts ==='object') { writeData(FILE_CARTS, backupData.carts); restoredCount++; }

        logAction(username, `Nag-restore mula sa backup file (${restoredCount} modules na-restore).`);
        res.json({ success: true, message: `Successfully restored and fully synchronized ${restoredCount} data module(s) from your backup file!` });
    } catch (e) {
        res.status(500).json({ success: false, message: `An error occurred while writing the extracted data: ${e.message}` });
    }
});

app.post('/api/transactions/:transactionId/void', rateLimit('void-transaction', 8, 10 * 60 * 1000), async (req, res) => {
    const { transactionId } = req.params;
    const { requester, adminPassword } = req.body;

    if (!adminPassword) {
        return res.status(400).json({ success: false, message:'Kailangan ng password para mag-void.' });
    }

    const users = readData(FILE_USERS);
    const authResult = await findVoidAuthorizer(users, adminPassword);

    if (!authResult) {
        return res.status(403).json({
            success: false,
            code:'WRONG_ADMIN_PASSWORD',
            message:'Maling password. Hindi pinahintulutan ang void.'
        });
    }

    let transactions = readData(FILE_TRANSACTIONS);
    let products = readData(FILE_PRODUCTS);

    const txIndex = transactions.findIndex(t => t.id === transactionId);
    if (txIndex === -1) {
        return res.status(404).json({ success: false, message:'Hindi nahanap ang Transaksyon ID.' });
    }

    const targetTx = transactions[txIndex];
    const voidedAmount = parseFloat(targetTx.total) || 0;

    targetTx.items.forEach(item => {
        let prod = products.find(p => p.code === item.code || p.name === item.name);
        if (prod) {
            prod.stock = (parseInt(prod.stock) || 0) + parseInt(item.quantity);
        }
    });

    // Reverse any customer stats (points, total spent, visits) that were applied
    // when this transaction was originally processed, so voiding a sale doesn't
    // leave the customer's record permanently inflated.
    if (targetTx.customerId) {
        const customers = readData(FILE_CUSTOMERS, []);
        const cust = customers.find(c => c.id === targetTx.customerId);
        if (cust) {
            const earned = Math.max(0, parseInt(targetTx.loyaltyPointsEarned) || 0);
            const redeemed = Math.max(0, parseInt(targetTx.loyaltyPointsRedeemed) || 0);

            // Undo the points that were earned from this sale, and give back
            // any points the customer redeemed on it.
            cust.points = Math.max(0, (cust.points || 0) - earned) + redeemed;
            cust.totalSpent = Math.round((((cust.totalSpent || 0) - voidedAmount)) * 100) / 100;
            if (cust.totalSpent < 0) cust.totalSpent = 0;
            cust.visits = Math.max(0, (cust.visits || 0) - 1);

            writeData(FILE_CUSTOMERS, customers);
        }
    }

    transactions = transactions.filter(t => t.id !== transactionId);

    writeData(FILE_TRANSACTIONS, transactions);
    writeData(FILE_PRODUCTS, products);

    logVoidAction(requester, transactionId, voidedAmount, authResult.isAdmin ?'Authorized by Admin' : `Authorized via Own Password (${authResult.user.username}, RBAC)`);

    res.json({ success: true, message: `Matagumpay na na-void ang transaksyon ${transactionId} at naibalik ang mga stock!` });
});

// ====================================================================
// REFUND (hiwalay sa Void)
// ====================================================================
// Pagkakaiba sa Void: hindi buburahin ang transaksyon sa records (nananatili
// ito sa Sales Report/Transactions para tama pa rin ang audit trail), at
// pwedeng PARTIAL lang (piling item/quantity) o FULL refund. Bawat item ay
// may sariling "refundable balance" (orihinal na quantity minus dati nang
// na-refund) para hindi ma-double-refund. Nagre-restock ito ng stock at
// nag-iiwan ng record sa `refunds` table (hiwalay sa userlogs) bilang
// dedicated audit trail — kaya makikita kung ilang beses at ano ang mga
// na-refund sa isang transaksyon.
app.post('/api/transactions/:transactionId/refund', rateLimit('refund-transaction', 12, 10 * 60 * 1000), async (req, res) => {
    const { transactionId } = req.params;
    const { requester, adminPassword, reason } = req.body;
    const requestedItems = Array.isArray(req.body.items) ? req.body.items : null;

    if (!adminPassword) {
        return res.status(400).json({ success: false, message: 'Kailangan ng password para mag-refund.' });
    }

    const users = readData(FILE_USERS);
    const authResult = await findRefundAuthorizer(users, adminPassword);

    if (!authResult) {
        return res.status(403).json({
            success: false,
            code: 'WRONG_ADMIN_PASSWORD',
            message: 'Maling password. Hindi pinahintulutan ang refund.'
        });
    }

    let transactions = readData(FILE_TRANSACTIONS);
    let products = readData(FILE_PRODUCTS);

    const txIndex = transactions.findIndex(t => t.id === transactionId);
    if (txIndex === -1) {
        return res.status(404).json({ success: false, message: 'Hindi nahanap ang Transaksyon ID.' });
    }

    const targetTx = transactions[txIndex];
    const grandTotal = parseFloat(targetTx.total) || 0;
    const alreadyRefunded = Math.min(grandTotal, parseFloat(targetTx.totalRefunded) || 0);
    const refundedQtyMap = targetTx.refundedQty && typeof targetTx.refundedQty === 'object' ? { ...targetTx.refundedQty } : {};

    if (alreadyRefunded >= grandTotal - 0.01) {
        return res.status(400).json({ success: false, message: 'Naka-full refund na ang transaksyong ito — wala nang matitirang matirang halaga na pwedeng i-refund.' });
    }

    // Ang refund amount ay kinukwenta base sa "gross" halaga bawat linya
    // (price x quantity, minus item-level discount) — pagkatapos ay
    // pino-prorate ang buong `grandTotal` ng transaksyon (na naglalaman
    // na ng cart-level discount at tax) ayon sa proportion ng refund sa
    // kabuuang gross. Ito ang pinaka-praktikal/patas na paraan kahit may
    // Senior/PWD, manual discount, o tax na naka-apply sa buong cart.
    const lineGross = (item) => {
        const qty = parseInt(item.quantity, 10) || 0;
        if (qty <= 0) return 0;
        return Math.max(0, (parseFloat(item.price) || 0) * qty - (parseFloat(item.itemDiscount) || 0));
    };

    const sumAllLinesGross = (targetTx.items || []).reduce((s, it) => s + lineGross(it), 0);

    const refundLines = [];
    const rejectedRefundItems = [];

    for (const item of (targetTx.items || [])) {
        const alreadyQty = parseInt(refundedQtyMap[item.code], 10) || 0;
        const maxRefundableQty = Math.max(0, (parseInt(item.quantity, 10) || 0) - alreadyQty);

        let qtyToRefund;
        if (requestedItems) {
            const requested = requestedItems.find(ri => ri.code === item.code);
            if (!requested) continue; // hindi kasama ang item na ito sa partial refund request
            qtyToRefund = parseInt(requested.quantity, 10) || 0;
            if (qtyToRefund <= 0) continue;
            if (qtyToRefund > maxRefundableQty) {
                rejectedRefundItems.push(`${item.name} (hiniling: ${qtyToRefund}, natitirang pwedeng i-refund: ${maxRefundableQty})`);
                continue;
            }
        } else {
            // Full refund ng buong natitirang balance ng bawat item.
            qtyToRefund = maxRefundableQty;
            if (qtyToRefund <= 0) continue;
        }

        refundLines.push({
            code: item.code,
            name: item.name,
            quantity: qtyToRefund,
            unitPrice: parseFloat(item.price) || 0
        });
    }

    if (rejectedRefundItems.length > 0) {
        return res.status(400).json({
            success: false,
            message: `Hindi maaaring i-refund ang mga sumusunod: ${rejectedRefundItems.join('; ')}`
        });
    }

    if (refundLines.length === 0) {
        return res.status(400).json({ success: false, message: 'Walang napiling item na may natitirang balanseng pwedeng i-refund.' });
    }

    let sumRefundGross = 0;
    for (const line of refundLines) {
        const originalItem = targetTx.items.find(it => it.code === line.code);
        const perUnitGross = originalItem.quantity > 0 ? lineGross(originalItem) / originalItem.quantity : 0;
        sumRefundGross += perUnitGross * line.quantity;
    }

    const refundRatio = sumAllLinesGross > 0 ? Math.min(1, sumRefundGross / sumAllLinesGross) : 0;
    let refundAmount = Math.round(grandTotal * refundRatio * 100) / 100;

    // I-clamp para hindi kailanman lumagpas ang KABUUANG na-refund (kasama
    // na ang mga naunang partial refund) sa aktwal na grand total.
    const remainingRefundable = Math.round((grandTotal - alreadyRefunded) * 100) / 100;
    if (refundAmount > remainingRefundable) refundAmount = remainingRefundable;
    if (refundAmount <= 0) {
        return res.status(400).json({ success: false, message: 'Zero ang na-compute na refund amount — walang matitirang halagang pwedeng i-refund.' });
    }

    // Restock: ibalik sa inventory ang mga na-refund na quantity.
    refundLines.forEach(line => {
        const prod = products.find(p => p.code === line.code);
        if (prod) {
            prod.stock = (parseInt(prod.stock) || 0) + line.quantity;
        }
        refundedQtyMap[line.code] = (parseInt(refundedQtyMap[line.code], 10) || 0) + line.quantity;
    });

    const newTotalRefunded = Math.round((alreadyRefunded + refundAmount) * 100) / 100;
    targetTx.refundedQty = refundedQtyMap;
    targetTx.totalRefunded = newTotalRefunded;
    targetTx.refundStatus = newTotalRefunded >= grandTotal - 0.01 ? 'full' : 'partial';

    // Proportional na pag-reverse ng customer stats (totalSpent at earned
    // points) — kagaya ng ginagawa ng Void, pero proportional lang dito
    // dahil pwedeng partial ang refund. Hindi na kinukuha pabalik ang mga
    // REDEEMED points (ginamit na ito ng customer bilang discount noon).
    if (targetTx.customerId) {
        const customers = readData(FILE_CUSTOMERS, []);
        const cust = customers.find(c => c.id === targetTx.customerId);
        if (cust) {
            const earnedOriginally = Math.max(0, parseInt(targetTx.loyaltyPointsEarned) || 0);
            const pointsToReverse = Math.floor(earnedOriginally * refundRatio);
            cust.points = Math.max(0, (cust.points || 0) - pointsToReverse);
            cust.totalSpent = Math.round(((cust.totalSpent || 0) - refundAmount) * 100) / 100;
            if (cust.totalSpent < 0) cust.totalSpent = 0;
            writeData(FILE_CUSTOMERS, customers);
        }
    }

    transactions[txIndex] = targetTx;
    writeData(FILE_TRANSACTIONS, transactions);
    writeData(FILE_PRODUCTS, products);

    const refundRecord = {
        id: 'RFD-' + Date.now(),
        transactionId,
        items: refundLines,
        reason: (reason || '').trim(),
        refundAmount,
        refundedBy: requester || 'Unknown',
        authorizedBy: authResult.isAdmin ? 'Admin' : `${authResult.user.username} (RBAC — refund_own_password)`,
        isFullRefund: targetTx.refundStatus === 'full',
        timestamp: new Date().toLocaleString('en-US', { timeZone: 'Asia/Manila' })
    };
    let refunds = readData(FILE_REFUNDS, []);
    refunds.unshift(refundRecord);
    writeData(FILE_REFUNDS, refunds);

    const itemsLabel = refundLines.map(l => `${l.name} x${l.quantity}`).join(', ');
    logRefundAction(
        requester || 'Unknown',
        transactionId,
        refundAmount,
        itemsLabel,
        reason,
        authResult.isAdmin ? 'Authorized by Admin' : `Authorized via Own Password (${authResult.user.username}, RBAC)`
    );

    res.json({
        success: true,
        message: `Matagumpay na na-refund ang ₱${refundAmount.toFixed(2)} (${itemsLabel}) at naibalik ang mga stock!`,
        refund: refundRecord,
        transaction: targetTx
    });
});

// Audit trail: lahat ng refund na na-process laban sa isang partikular na
// transaksyon (para makita sa Transaction Details kung ilang beses/ano
// ang na-refund dito).
app.get('/api/transactions/:transactionId/refunds', (req, res) => {
    const { transactionId } = req.params;
    const refunds = readData(FILE_REFUNDS, []).filter(r => r.transactionId === transactionId);
    res.json(refunds);
});

// Refunds Report: lahat ng refund sa buong system, may parehong
// view-all/own-only visibility rule gaya ng /api/transactions.
app.get('/api/refunds', (req, res) => {
    const { requester } = req.query;
    const allRefunds = readData(FILE_REFUNDS, []);

    if (!requester) return res.json(allRefunds);

    const users = readData(FILE_USERS);
    const activeUser = users.find(u => u.username.toLowerCase() === requester.toLowerCase());
    const activeRole = activeUser && activeUser.role;
    const isAdminRole = (activeRole || '').toLowerCase() === 'admin';
    const canViewAll = isAdminRole || !!getPermissionsForRole(activeRole).transactions_view_all;
    if (canViewAll) return res.json(allRefunds);

    res.json(allRefunds.filter(r => (r.refundedBy || '').toLowerCase() === requester.toLowerCase()));
});

app.post('/api/auth/verify-void', rateLimit('verify-void', 8, 10 * 60 * 1000), async (req, res) => {
    const { adminPassword, purpose } = req.body;

    if (!adminPassword) {
        return res.status(400).json({ success: false, message:'Kailangan ng password.' });
    }

    const users = readData(FILE_USERS);

    // Ang endpoint na ito ay ginagamit din ng ibang bahagi ng system (hal.
    // barcode print authorization) na hindi void-related, kaya ang
    // "kahit kaninong qualified na account" na paghahanap ay nakalimita
    // lamang sa mga request na explicit na nagsasabing purpose:'void'.
    // Para dito, HINDI ito naka-base sa session/req.authUser dahil ang
    // taong nagta-type ng password (hal. Supervisor) ay kadalasang HINDI
    // ang naka-login sa terminal (hal. Cashier).
    if (purpose ==='void') {
        const authResult = await findVoidAuthorizer(users, adminPassword);
        if (authResult) {
            return res.json({ success: true, message:'Authorized' });
        }
        return res.status(403).json({ success: false, code:'WRONG_ADMIN_PASSWORD', message:'Maling password!' });
    }

    if (purpose ==='manual_discount') {
        const authResult = await findManualDiscountAuthorizer(users, adminPassword);
        if (authResult) {
            return res.json({ success: true, message:'Authorized' });
        }
        return res.status(403).json({ success: false, code:'WRONG_ADMIN_PASSWORD', message:'Maling password!' });
    }

    if (purpose ==='refund') {
        const authResult = await findRefundAuthorizer(users, adminPassword);
        if (authResult) {
            return res.json({ success: true, message:'Authorized' });
        }
        return res.status(403).json({ success: false, code:'WRONG_ADMIN_PASSWORD', message:'Maling password!' });
    }

    if (purpose ==='loyalty_redeem') {
        const authResult = await findLoyaltyRedeemAuthorizer(users, adminPassword);
        if (authResult) {
            return res.json({ success: true, message:'Authorized' });
        }
        return res.status(403).json({ success: false, code:'WRONG_ADMIN_PASSWORD', message:'Maling password!' });
    }

    const adminUser = users.find(u => u.role.toLowerCase() ==='admin');
    if (!adminUser) {
        return res.status(404).json({ success: false, message:'Walang nahanap na Admin account sa system.' });
    }

    const isMatch = await bcrypt.compare(adminPassword, adminUser.password);

    if (isMatch) {
        return res.json({ success: true, message:'Authorized' });
    } else {

        return res.status(403).json({ success: false, code:'WRONG_ADMIN_PASSWORD', message:'Maling Admin Password!' });
    }
});

app.post('/api/logs', (req, res) => {
    const { user, action, authMethod, details } = req.body;
    let formattedAction = `[${action}]`;

    if (action ==="VOID_CART") {
        formattedAction += ` Voided cart: ${details.itemsCount} items, Total: ₱${details.totalAmount.toFixed(2)} (${authMethod}).`;
    }
    else if (action ==="MODIFY_MATRIX_QTY") {
        formattedAction += ` ${details.itemName}: Reduced by ${details.reducedQty}, Remaining: ${details.newQty} (${authMethod}).`;
    }
    else if (action ==="LOGOUT") {
        formattedAction += ` Logged out via ${authMethod}. Reason: ${details.message}`;
    }
    else {
        formattedAction += ` ${details.message ||'Executed non-standard action.'}`;
    }

    try {
        logAction(user, formattedAction);
        res.json({ success: true, message:'Log saved.' });
    } catch (error) {
        console.error("Logging error:", error);
        res.status(500).json({ success: false, message:'Server logging failed.' });
    }
});

function computeLowStockItems() {
    const products = readData(FILE_PRODUCTS);
    const purchaseOrders = readData(FILE_PURCHASE_ORDERS, []);
    let tracking = readData(FILE_LOWSTOCK_TRACKING, {});
    const nowIso = new Date().toISOString();

    const openPoQtyByCode = {};
    purchaseOrders.forEach(po => {
        if (po.status ==='ordered') {
            (po.items || []).forEach(it => {
                const key = (it.code ||'').trim().toLowerCase();
                openPoQtyByCode[key] = (openPoQtyByCode[key] || 0) + (parseInt(it.qty) || 0);
            });
        }
    });

    const uxSettingsForThreshold = readData(FILE_UX_SETTINGS, DEFAULT_UX_SETTINGS);
    const defaultLowStockThreshold = Number.isFinite(uxSettingsForThreshold.lowStockAlertThreshold)
        ? uxSettingsForThreshold.lowStockAlertThreshold : DEFAULT_UX_SETTINGS.lowStockAlertThreshold;

    const items = products
        .map(p => {
            const threshold = (p.lowStockThreshold !== undefined && p.lowStockThreshold !== null && p.lowStockThreshold !=='')
                ? parseInt(p.lowStockThreshold) : defaultLowStockThreshold;
            const stock = parseInt(p.stock || 0);
            const suggestedReorderQty = p.reorderQty ? parseInt(p.reorderQty) : Math.max((threshold * 2) - stock, threshold, 1);
            const key = (p.code ||'').trim().toLowerCase();
            return {
                code: p.code, name: p.name, category: p.category, supplier: p.supplier ||'',
                stock, threshold, suggestedReorderQty,
                status: stock <= 0 ?'OUT_OF_STOCK' :'LOW_STOCK',
                openOrderedQty: openPoQtyByCode[key] || 0,
                _key: key
            };
        })
        .filter(p => p.stock <= p.threshold);

    const stillLowKeys = new Set(items.map(i => i._key));
    let trackingChanged = false;
    items.forEach(i => {
        if (!tracking[i._key]) { tracking[i._key] = nowIso; trackingChanged = true; }
    });
    Object.keys(tracking).forEach(k => {
        if (!stillLowKeys.has(k)) { delete tracking[k]; trackingChanged = true; }
    });
    if (trackingChanged) writeData(FILE_LOWSTOCK_TRACKING, tracking);

    items.forEach(i => {
        const since = tracking[i._key] ? new Date(tracking[i._key]) : new Date();
        i.daysLow = Math.max(0, Math.floor((Date.now() - since.getTime()) / (1000 * 60 * 60 * 24)));
        i.lowSince = tracking[i._key] || nowIso;
        delete i._key;
    });

    items.sort((a, b) => a.stock - b.stock);
    return items;
}

app.get('/api/products/low-stock', (req, res) => {
    const items = computeLowStockItems();
    res.json({ success: true, count: items.length, items });
});

// ====================================================================
// ADVANCED SALES ANALYTICS — gated ng requirePermission('reports')
// (role-based: sino ang pwedeng makakita ng reports) AT
// requireFeature('advanced_reports') (paywall: binili ba ng store owner
// ang ₱799 na module). Dating client-side lang ang computation nito
// (basta kinukuha lahat ng /api/transactions, na FREE/ungated dahil
// ginagamit din ito ng ibang legit na views) — kaya kahit naka-hide sa
// UI ang Sales Analytics, kaya pa ring i-compute ng kahit sino ang
// profit/margin data sa pamamagitan lang ng DevTools. Dito na ngayon
// ginagawa ang buong aggregation SA SERVER, kaya totoong naka-enforce
// na ang paywall, hindi lang cosmetic/UI-level.
app.get('/api/reports/sales-analytics', requirePermission('reports'), requireFeature('advanced_reports'), (req, res) => {
    try {
        const rangeParam = (req.query.range || 'all').toString();
        const transactions = readData(FILE_TRANSACTIONS);

        const now = Date.now();
        const RANGE_MS = { today: 24 * 60 * 60 * 1000, '7d': 7 * 24 * 60 * 60 * 1000, '30d': 30 * 24 * 60 * 60 * 1000 };
        const cutoffMs = RANGE_MS[rangeParam] || null;

        const txs = cutoffMs
            ? transactions.filter(t => {
                const ts = t.isoDate ? Date.parse(t.isoDate) : NaN;
                return !isNaN(ts) && (now - ts) <= cutoffMs;
            })
            : transactions;

        let gross = 0;
        let totalRevenue = 0;
        let totalCost = 0;
        let anyCostRecorded = false;
        const rankingMap = {};
        const profitByProduct = {};
        const paymentBreakdown = {};
        const dailyTrendMap = {};

        txs.forEach(t => {
            const total = parseFloat(t.total) || 0;
            gross += total;

            const method = (t.method || t.payment_method || 'OTHER').toString().toUpperCase();
            paymentBreakdown[method] = (paymentBreakdown[method] || 0) + total;

            const dayKey = (t.isoDate ? t.isoDate.slice(0, 10) : (t.timestamp || '').slice(0, 10)) || 'unknown';
            dailyTrendMap[dayKey] = (dailyTrendMap[dayKey] || 0) + total;

            (t.items || []).forEach(i => {
                const qty = parseInt(i.quantity) || 0;
                rankingMap[i.name] = (rankingMap[i.name] || 0) + qty;

                const itemDiscount = Math.max(0, parseFloat(i.itemDiscount) || 0);
                const revenue = ((parseFloat(i.price) || 0) * qty) - itemDiscount;
                const cost = (parseFloat(i.cost) || 0) * qty;
                if (parseFloat(i.cost) > 0) anyCostRecorded = true;

                totalRevenue += revenue;
                totalCost += cost;

                if (!profitByProduct[i.name]) profitByProduct[i.name] = { revenue: 0, cost: 0, qty: 0 };
                profitByProduct[i.name].revenue += revenue;
                profitByProduct[i.name].cost += cost;
                profitByProduct[i.name].qty += qty;
            });
        });

        const estimatedProfit = totalRevenue - totalCost;
        const marginPct = totalRevenue > 0 ? (estimatedProfit / totalRevenue) * 100 : 0;

        const sortedByQty = Object.keys(rankingMap).sort((a, b) => rankingMap[b] - rankingMap[a]);
        const topProducts = sortedByQty.slice(0, 5).map(name => ({ name, qty: rankingMap[name] }));
        const slowProducts = [...sortedByQty].reverse().slice(0, 5).map(name => ({ name, qty: rankingMap[name] }));

        const profitEntries = Object.entries(profitByProduct)
            .map(([name, d]) => ({ name, profit: Math.round((d.revenue - d.cost) * 100) / 100, qty: d.qty }))
            .sort((a, b) => b.profit - a.profit)
            .slice(0, 5);

        // Last 7 days trend (kahit walang benta sa isang araw, kasama pa rin
        // ito bilang ₱0 sa chart, para consistent ang bilang ng bars).
        const dailyTrend = [];
        for (let d = 6; d >= 0; d--) {
            const dt = new Date(now - d * 24 * 60 * 60 * 1000);
            const key = dt.toISOString().slice(0, 10);
            dailyTrend.push({
                date: key,
                label: dt.toLocaleDateString('en-PH', { weekday: 'short' }),
                total: Math.round((dailyTrendMap[key] || 0) * 100) / 100
            });
        }

        res.json({
            success: true,
            range: rangeParam,
            gross: Math.round(gross * 100) / 100,
            transactionCount: txs.length,
            estimatedProfit: Math.round(estimatedProfit * 100) / 100,
            marginPct: Math.round(marginPct * 10) / 10,
            hasCostData: anyCostRecorded,
            topProducts,
            slowProducts,
            profitByProduct: profitEntries,
            paymentBreakdown,
            dailyTrend
        });
    } catch (err) {
        console.error('Sales analytics error:', err);
        res.status(500).json({ success: false, message: 'Hindi makuha ang sales analytics data.' });
    }
});

app.get('/api/products/low-stock/export', requirePermission('reorder'), requireFeature('advanced_reports'), (req, res) => {
    try {
        const items = computeLowStockItems();
        const escapeCsv = (val) => {
            const s = (val === undefined || val === null) ?'' : val.toString();
            return/[",\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
        };
        const headers = ['Code','Product Name','Category','Supplier','Current Stock','Threshold','Suggested Reorder Qty','Status','Days Low'];
        const lines = [headers.join(',')];
        items.forEach(p => {
            lines.push([
                escapeCsv(p.code), escapeCsv(p.name), escapeCsv(p.category), escapeCsv(p.supplier),
                escapeCsv(p.stock), escapeCsv(p.threshold), escapeCsv(p.suggestedReorderQty),
                escapeCsv(p.status), escapeCsv(p.daysLow)
            ].join(','));
        });
        const csvContent ='\uFEFF' + lines.join('\r\n');
        res.setHeader('Content-Type','text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename="reorder_alerts_${Date.now()}.csv"`);
        res.send(csvContent);
    } catch (err) {
        console.error('Reorder export error:', err);
        res.status(500).json({ success: false, message:'Hindi ma-export ang reorder list.' });
    }
});

app.post('/api/products/:code/quick-restock', requirePermission('reorder'), rateLimit('quick-restock', 60, 10 * 60 * 1000), (req, res) => {
    const { code } = req.params;
    const qty = parseInt(req.body.qty);
    const username = req.authUser.username;
    const isAdminRole = (req.authUser.role ||'').toLowerCase() ==='admin';
    const canApplyDirectly = isAdminRole || !!getPermissionsForRole(req.authUser.role).restock_direct_apply;

    if (!qty || qty <= 0) {
        return res.status(400).json({ success: false, message:'Mangyaring maglagay ng valid na quantity.' });
    }

    let products = readData(FILE_PRODUCTS);
    const target = products.find(p => p.code.trim().toLowerCase() === code.trim().toLowerCase());
    if (!target) return res.status(404).json({ success: false, message:'Product not found.' });

    if (canApplyDirectly) {
        target.stock = (parseInt(target.stock) || 0) + qty;
        writeData(FILE_PRODUCTS, products);
        logAction(username, `Quick-restocked "${target.name}" (+${qty}, bagong stock: ${target.stock})`);
        return res.json({ success: true, message: `+${qty} na-restock sa "${target.name}".`, newStock: target.stock });
    } else {
        let requests = readData(FILE_REQUESTS);
        requests.push({ id: Date.now(), type:'RESTOCK', targetCode: code, requester: username, data: { qtyToAdd: qty, productName: target.name }, timestamp: new Date().toLocaleString() });
        writeData(FILE_REQUESTS, requests);
        logAction(username, `Submitted a RESTOCK request for "${target.name}" (+${qty})`);
        return res.json({ success: true, pending: true, message:'Restock request submitted for Admin approval.' });
    }
});

app.get('/api/purchase-orders', requirePermission('reorder'), requireFeature('purchase_orders'), (req, res) => {
    const orders = readData(FILE_PURCHASE_ORDERS, []).sort((a, b) => (b.createdAt ||'').localeCompare(a.createdAt ||''));
    res.json({ success: true, orders });
});

app.post('/api/purchase-orders', requirePermission('reorder'), requireFeature('purchase_orders'), (req, res) => {
    const { supplier, items, notes } = req.body;
    const username = req.authUser.username;

    if (!Array.isArray(items) || items.length === 0) {
        return res.status(400).json({ success: false, message:'Walang napiling item para sa Purchase Order.' });
    }

    const cleanItems = items
        .map(it => ({ code: (it.code ||'').toString(), name: (it.name ||'').toString(), qty: parseInt(it.qty) || 0 }))
        .filter(it => it.code && it.qty > 0);

    if (cleanItems.length === 0) {
        return res.status(400).json({ success: false, message:'Walang valid na item/quantity sa Purchase Order.' });
    }

    const orders = readData(FILE_PURCHASE_ORDERS, []);
    const newPO = {
        id: Date.now(),
        supplier: (supplier ||'Walang Tinukoy na Supplier').toString(),
        items: cleanItems,
        notes: (notes ||'').toString(),
        status:'ordered',
        createdBy: username,
        createdAt: new Date().toISOString(),
    };
    orders.push(newPO);
    writeData(FILE_PURCHASE_ORDERS, orders);
    logAction(username, `Gumawa ng Purchase Order #${newPO.id} para kay "${newPO.supplier}" (${cleanItems.length} item/s)`);
    res.json({ success: true, message:'Nagawa ang Purchase Order.', po: newPO });
});

app.post('/api/purchase-orders/:id/receive', requirePermission('reorder'), requireFeature('purchase_orders'), (req, res) => {
    const { id } = req.params;
    const username = req.authUser.username;
    let orders = readData(FILE_PURCHASE_ORDERS, []);
    const po = orders.find(o => o.id.toString() === id.toString());
    if (!po) return res.status(404).json({ success: false, message:'Purchase Order not found.' });
    if (po.status !=='ordered') return res.status(400).json({ success: false, message: `Hindi na-a-apply — status na ito ay "${po.status}".` });

    let products = readData(FILE_PRODUCTS);
    po.items.forEach(it => {
        const prod = products.find(p => p.code.trim().toLowerCase() === it.code.trim().toLowerCase());
        if (prod) prod.stock = (parseInt(prod.stock) || 0) + (parseInt(it.qty) || 0);
    });
    writeData(FILE_PRODUCTS, products);

    po.status ='received';
    po.receivedBy = username;
    po.receivedAt = new Date().toISOString();
    writeData(FILE_PURCHASE_ORDERS, orders);
    logAction(username, `Na-receive ang Purchase Order #${po.id} (${po.supplier}) — idinagdag sa stock ang ${po.items.length} item/s`);
    res.json({ success: true, message:'Na-receive ang Purchase Order at na-update ang stock.', po });
});

app.post('/api/purchase-orders/:id/cancel', requirePermission('reorder'), requireFeature('purchase_orders'), (req, res) => {
    const { id } = req.params;
    const username = req.authUser.username;
    let orders = readData(FILE_PURCHASE_ORDERS, []);
    const po = orders.find(o => o.id.toString() === id.toString());
    if (!po) return res.status(404).json({ success: false, message:'Purchase Order not found.' });
    if (po.status !=='ordered') return res.status(400).json({ success: false, message: `Hindi na-a-apply — status na ito ay "${po.status}".` });

    po.status ='cancelled';
    po.cancelledBy = username;
    po.cancelledAt = new Date().toISOString();
    writeData(FILE_PURCHASE_ORDERS, orders);
    logAction(username, `Kinansela ang Purchase Order #${po.id} (${po.supplier})`);
    res.json({ success: true, message:'Kinansela ang Purchase Order.', po });
});

app.get('/api/customers', requirePermission('customers'), requireFeature('customer_crm'), (req, res) => {
    res.json(readData(FILE_CUSTOMERS, []).map(sanitizeCustomerForClient));
});

app.get('/api/customers/for-terminal', requirePermission('terminal'), (req, res) => {
    const customers = readData(FILE_CUSTOMERS, []);
    const minimal = customers.map(c => ({
        id: c.id,
        name: c.name,
        phone: c.phone ||'',
        email: c.email ||'',
        points: c.points || 0,
        hasLoyaltyCard: !!(c.loyaltyCard && !c.loyaltyCard.revoked)
    }));
    res.json(minimal);
});

app.get('/api/customers/search', requirePermission('customers'), requireFeature('customer_crm'), (req, res) => {
    const q = (req.query.q ||'').toLowerCase().trim();
    const customers = readData(FILE_CUSTOMERS, []);
    if (!q) return res.json(customers.slice(0, 25).map(sanitizeCustomerForClient));
    const results = customers.filter(c =>
        (c.name ||'').toLowerCase().includes(q) || (c.phone ||'').includes(q)
    ).slice(0, 25);
    res.json(results.map(sanitizeCustomerForClient));
});

app.post('/api/customers', requirePermission('customers'), requireFeature('customer_crm'), (req, res) => {
    const { name, phone, email, notes } = req.body;
    if (!name || !name.trim()) {
        return res.status(400).json({ success: false, message:'Kailangan ng pangalan ng customer.' });
    }
    const customers = readData(FILE_CUSTOMERS, []);
    if (phone && customers.some(c => c.phone && c.phone === phone)) {
        return res.status(400).json({ success: false, message:'May existing customer na gumagamit na ng phone number na ito.' });
    }
    const customer = {
        id:'CUST-' + Date.now(),
        name: name.trim(),
        phone: phone ||'',
        email: email ||'',
        notes: notes ||'',
        points: 0,
        totalSpent: 0,
        visits: 0,
        createdAt: new Date().toISOString(),
        lastVisit: null,
        loyaltyCard: null
    };
    customers.unshift(customer);
    writeData(FILE_CUSTOMERS, customers);
    logAction(req.authUser.username, `Added new customer: ${customer.name}`);
    res.json({ success: true, customer: sanitizeCustomerForClient(customer) });
});

app.put('/api/customers/:id', requirePermission('customers'), requireFeature('customer_crm'), (req, res) => {
    const customers = readData(FILE_CUSTOMERS, []);
    const idx = customers.findIndex(c => c.id === req.params.id);
    if (idx === -1) return res.status(404).json({ success: false, message:'Customer not found.' });
    const { name, phone, email, notes } = req.body;
    if (name !== undefined && name.trim()) customers[idx].name = name.trim();
    if (phone !== undefined) customers[idx].phone = phone;
    if (email !== undefined) customers[idx].email = email;
    if (notes !== undefined) customers[idx].notes = notes;
    writeData(FILE_CUSTOMERS, customers);
    res.json({ success: true, customer: sanitizeCustomerForClient(customers[idx]) });
});

app.delete('/api/customers/:id', requirePermission('customers'), requireFeature('customer_crm'), (req, res) => {
    let customers = readData(FILE_CUSTOMERS, []);
    if (!customers.some(c => c.id === req.params.id)) {
        return res.status(404).json({ success: false, message:'Customer not found.' });
    }
    customers = customers.filter(c => c.id !== req.params.id);
    writeData(FILE_CUSTOMERS, customers);
    logAction(req.authUser.username, `Deleted customer ID: ${req.params.id}`);
    res.json({ success: true });
});

// ------------------------------------------------------------------
// LOYALTY CARD / QR — issue, regenerate, revoke, at terminal lookup.
// Tingnan ang malaking paliwanag malapit sa `verifyLoyaltyCardToken`
// (itaas ng file na ito) kung paano gumagana ang buong security model.
// ------------------------------------------------------------------

// Nag-i-issue ng BAGONG card/QR (para sa bagong customer) o
// nagre-regenerate ng existing card (hal. nawala ang dating card) —
// AUTHORIZED PERSONNEL LAMANG (may `loyalty_card_issue` permission).
// Ang buong raw token ay ibinabalik LAMANG dito, minsan lang — kailangan
// itong i-print/i-display agad bilang QR para sa customer.
app.post('/api/customers/:id/loyalty-card', requirePermission('loyalty_card_issue'), requireFeature('customer_crm'), rateLimit('loyalty-card-issue', 20, 10 * 60 * 1000), (req, res) => {
    const customers = readData(FILE_CUSTOMERS, []);
    const customer = customers.find(c => c.id === req.params.id);
    if (!customer) return res.status(404).json({ success: false, message:'Customer not found.' });
    const mode = (req.body && req.body.mode ==='static') ? 'static' : 'rotating';
    const wasReissue = !!(customer.loyaltyCard && !customer.loyaltyCard.revoked);
    const { token, card } = issueLoyaltyCard(customer, mode, req.authUser.username);
    writeData(FILE_CUSTOMERS, customers);
    logAction(req.authUser.username, `${wasReissue ? 'Regenerated' : 'Issued'} loyalty ${mode ==='static' ? 'card' : 'QR'} for customer: ${customer.name} (${customer.id})`);
    res.json({
        success: true,
        message: wasReissue
            ? 'Na-regenerate ang loyalty card/QR. Awtomatiko nang na-invalidate ang dating QR — ito na lang ang gagana.'
            : 'Naka-issue na ang bagong loyalty card/QR.',
        token,
        card: { cardId: card.cardId, mode: card.mode, issuedAt: card.issuedAt }
    });
});

// Ibinabalewala/dini-disable ang kasalukuyang card/QR ng customer nang
// hindi agad nagbibigay ng kapalit (hal. nawala ang card, habang
// hinihintay ang bagong isyu) — AUTHORIZED PERSONNEL LAMANG.
app.post('/api/customers/:id/loyalty-card/revoke', requirePermission('loyalty_card_issue'), requireFeature('customer_crm'), (req, res) => {
    const customers = readData(FILE_CUSTOMERS, []);
    const customer = customers.find(c => c.id === req.params.id);
    if (!customer) return res.status(404).json({ success: false, message:'Customer not found.' });
    if (!customer.loyaltyCard) return res.status(400).json({ success: false, message:'Walang naka-issue na card/QR ang customer na ito.' });
    customer.loyaltyCard.revoked = true;
    writeData(FILE_CUSTOMERS, customers);
    logAction(req.authUser.username, `Revoked loyalty card/QR of customer: ${customer.name} (${customer.id})`);
    res.json({ success: true, message:'Na-revoke na ang loyalty card/QR na ito.' });
});

// Ginagamit ng POS terminal kapag ni-scan ang QR/card ng customer sa
// panahon ng checkout — nire-resolve kung sinong customer ito (para
// awtomatikong mapili sa cart) at kinukumpirma agad kung VALID ang
// card. Ang totoong points deduction/redemption ay sa /api/transactions
// pa rin nangyayari (gamit ang PAREHONG scanned token), dahil doon lang
// FINAL/fully-verified ang halaga ng points na ire-redeem.
app.post('/api/customers/lookup-by-card', requirePermission('terminal'), rateLimit('loyalty-card-scan', 60, 10 * 60 * 1000), (req, res) => {
    const rawToken = req.body && req.body.token;
    if (!rawToken || typeof rawToken !=='string') {
        return res.status(400).json({ success: false, message:'Missing QR/card token.' });
    }
    const parts = rawToken.split('.');
    if (parts.length !== 4 || parts[0] !=='LC1') {
        return res.status(400).json({ success: false, message:'Hindi ito valid na Loyalty Card/QR.' });
    }
    const customers = readData(FILE_CUSTOMERS, []);
    const customer = customers.find(c => c.id === parts[1]);
    if (!customer) return res.status(404).json({ success: false, message:'Walang customer na nahanap para sa card/QR na ito.' });
    const check = verifyLoyaltyCardToken(customer, rawToken);
    if (!check.valid) return res.status(403).json({ success: false, code:'LOYALTY_CARD_INVALID', message: check.message });
    res.json({
        success: true,
        customer: { id: customer.id, name: customer.name, phone: customer.phone ||'', email: customer.email ||'', points: customer.points || 0 },
        cardMode: check.mode
    });
});

app.get('/api/promocodes', requirePermission('products'), requireFeature('promo_codes'), (req, res) => {
    res.json(readData(FILE_PROMOCODES, []));
});

app.post('/api/promocodes', requirePermission('products'), requireFeature('promo_codes'), (req, res) => {
    let { code, type, value, description, expiresAt, minSpend } = req.body;
    code = (code ||'').trim().toUpperCase();
    if (!code) return res.status(400).json({ success: false, message:'Kailangan ng promo code.' });
    if (!['percent','fixed'].includes(type)) return res.status(400).json({ success: false, message:'Invalid discount type (percent o fixed lang).' });
    value = parseFloat(value);
    if (isNaN(value) || value <= 0) return res.status(400).json({ success: false, message:'Invalid discount value.' });
    if (type ==='percent' && value > 100) return res.status(400).json({ success: false, message:'Hindi pwedeng lumagpas sa 100% ang percent discount.' });

    const promos = readData(FILE_PROMOCODES, []);
    if (promos.some(p => p.code === code)) {
        return res.status(400).json({ success: false, message:'Existing na ang promo code na ito.' });
    }
    const promo = {
        code, type, value,
        description: description ||'',
        active: true,
        expiresAt: expiresAt || null,
        minSpend: parseFloat(minSpend) || 0,
        createdAt: new Date().toISOString()
    };
    promos.unshift(promo);
    writeData(FILE_PROMOCODES, promos);
    logAction(req.authUser.username, `Added promo code: ${code}`);
    res.json({ success: true, promo });
});

app.put('/api/promocodes/:code', requirePermission('products'), requireFeature('promo_codes'), (req, res) => {
    const codeParam = req.params.code.toUpperCase();
    const promos = readData(FILE_PROMOCODES, []);
    const idx = promos.findIndex(p => p.code === codeParam);
    if (idx === -1) return res.status(404).json({ success: false, message:'Promo code not found.' });
    const { type, value, description, active, expiresAt, minSpend } = req.body;
    if (type !== undefined) promos[idx].type = type;
    if (value !== undefined) promos[idx].value = parseFloat(value);
    if (description !== undefined) promos[idx].description = description;
    if (active !== undefined) promos[idx].active = !!active;
    if (expiresAt !== undefined) promos[idx].expiresAt = expiresAt;
    if (minSpend !== undefined) promos[idx].minSpend = parseFloat(minSpend) || 0;
    writeData(FILE_PROMOCODES, promos);
    res.json({ success: true, promo: promos[idx] });
});

app.delete('/api/promocodes/:code', requirePermission('products'), requireFeature('promo_codes'), (req, res) => {
    const codeParam = req.params.code.toUpperCase();
    let promos = readData(FILE_PROMOCODES, []);
    if (!promos.some(p => p.code === codeParam)) {
        return res.status(404).json({ success: false, message:'Promo code not found.' });
    }
    promos = promos.filter(p => p.code !== codeParam);
    writeData(FILE_PROMOCODES, promos);
    logAction(req.authUser.username, `Deleted promo code: ${codeParam}`);
    res.json({ success: true });
});

app.get('/api/promocodes/:code/validate', requireFeature('promo_codes'), (req, res) => {

    const codeParam = req.params.code.toUpperCase();
    const subtotal = parseFloat(req.query.subtotal) || 0;
    const promos = readData(FILE_PROMOCODES, []);
    const promo = promos.find(p => p.code === codeParam);
    if (!promo) return res.json({ success: false, message:'Hindi valid ang promo code na ito.' });
    if (!promo.active) return res.json({ success: false, message:'Naka-disable na ang promo code na ito.' });
    if (promo.expiresAt && new Date(promo.expiresAt).getTime() < Date.now()) {
        return res.json({ success: false, message:'Na-expire na ang promo code na ito.' });
    }
    if (promo.minSpend && subtotal < promo.minSpend) {
        return res.json({ success: false, message: `Kailangan ng minimum na ₱${promo.minSpend.toFixed(2)} para magamit ang promo na ito.` });
    }
    let discountAmount = promo.type ==='percent' ? (subtotal * promo.value / 100) : promo.value;
    discountAmount = Math.min(Math.max(discountAmount, 0), subtotal);
    res.json({ success: true, promo, discountAmount: Math.round(discountAmount * 100) / 100 });
});

function computeShiftSummary(periodStartIso, periodEndIso, cashierFilter) {
    const allTx = readData(FILE_TRANSACTIONS);
    const start = new Date(periodStartIso).getTime();
    const end = new Date(periodEndIso).getTime();
    const cashierKey = cashierFilter ? String(cashierFilter).toLowerCase() : null;

    const txs = allTx.filter(t => {
        const ts = new Date(t.isoDate || t.timestamp || 0).getTime();
        if (isNaN(ts) || ts <= start || ts > end) return false;
        if (cashierKey && (t.cashier ||'').toLowerCase() !== cashierKey) return false;
        return true;
    });

    const paymentBreakdown = {};
    let grossSales = 0, totalDiscount = 0, netSales = 0;
    txs.forEach(t => {
        const disc = parseFloat(t.discount || 0) || 0;
        const net = parseFloat(t.total || 0) || 0;
        totalDiscount += disc;
        netSales += net;
        grossSales += net + disc;

        if (Array.isArray(t.payments) && t.payments.length > 0) {
            const totalTendered = t.payments.reduce((s, p) => s + (parseFloat(p.amount) || 0), 0) || 1;
            t.payments.forEach(p => {
                const method = (p.method ||'CASH').toUpperCase();
                const share = Math.round((net * ((parseFloat(p.amount) || 0) / totalTendered)) * 100) / 100;
                if (!paymentBreakdown[method]) paymentBreakdown[method] = { count: 0, total: 0 };
                paymentBreakdown[method].total = Math.round((paymentBreakdown[method].total + share) * 100) / 100;
            });

            const primaryMethod = (t.payments[0].method ||'CASH').toUpperCase();
            paymentBreakdown[primaryMethod].count += 1;
        } else {
            const method = (t.payment_method || t.method ||'CASH').toUpperCase();
            if (!paymentBreakdown[method]) paymentBreakdown[method] = { count: 0, total: 0 };
            paymentBreakdown[method].count += 1;
            paymentBreakdown[method].total = Math.round((paymentBreakdown[method].total + net) * 100) / 100;
        }
    });

    const logs = readData(FILE_USERLOGS);
    const voidLogs = logs.filter(l => l.action && l.action.indexOf('VOIDED Transaction') === 0 && l.id > start && l.id <= end
        && (!cashierKey || (l.username ||'').toLowerCase() === cashierKey));
    const voidCount = voidLogs.length;
    const voidedAmount = Math.round(voidLogs.reduce((sum, l) => sum + (parseFloat(l.voidedAmount) || 0), 0) * 100) / 100;

    return {
        periodStart: new Date(start).toISOString(),
        periodEnd: new Date(end).toISOString(),
        transactionCount: txs.length,
        grossSales: Math.round(grossSales * 100) / 100,
        totalDiscount: Math.round(totalDiscount * 100) / 100,
        netSales: Math.round(netSales * 100) / 100,
        paymentBreakdown,
        voidCount,
        voidedAmount
    };
}

function readShiftMetaStore() {
    const raw = readData(FILE_SHIFT_META, {});
    if (raw && !raw.cashiers && (raw.beginningCash !== undefined || raw.lastCloseAt !== undefined)) {
        return { cashiers: {}, legacyLastCloseAt: raw.lastCloseAt || null };
    }
    if (!raw.cashiers) raw.cashiers = {};
    return raw;
}

function writeShiftMetaStore(store) {
    writeData(FILE_SHIFT_META, store);
}

function getCashierShiftMeta(store, username) {
    const existingKey = Object.keys(store.cashiers).find(k => k.toLowerCase() === (username ||'').toLowerCase());
    if (existingKey) return store.cashiers[existingKey];
    store.cashiers[username] = { lastCloseAt: store.legacyLastCloseAt || null };
    return store.cashiers[username];
}

app.get('/api/shift/current', (req, res) => {
    const role = req.authUser && req.authUser.role;
    const isAdminRole = (role ||'').toLowerCase() ==='admin';
    const canControlOthers = isAdminRole || !!getPermissionsForRole(role).shift_close_control;

    const requestedCashier = (req.query.cashier ||'').toString().trim();
    let targetCashier = req.authUser.username;
    if (requestedCashier && requestedCashier.toLowerCase() !== targetCashier.toLowerCase()) {
        if (!canControlOthers) {
            return res.status(403).json({ success: false, message:'Akses Denied: Wala kang pahintulot na tingnan/kontrolin ang shift ng ibang cashier.' });
        }
        targetCashier = requestedCashier;
    }

    const store = readShiftMetaStore();
    const targetHasOpenShift = (() => {
        const m = getCashierShiftMeta(store, targetCashier);
        return m.beginningCash !== undefined && m.beginningCash !== null;
    })();
    if (!targetHasOpenShift) {
        const gateResult = checkShiftManagementUnlocked();
        if (!gateResult.unlocked) return res.status(402).json(gateResult.body);
    }
    const meta = getCashierShiftMeta(store, targetCashier);
    const periodStart = meta.lastCloseAt || new Date(0).toISOString();
    const summary = computeShiftSummary(periodStart, new Date().toISOString(), targetCashier);

    const canViewAmounts = isAdminRole || !!getPermissionsForRole(role).shiftreport_view_amounts;
    if (!canViewAmounts) {
        delete summary.grossSales;
        delete summary.totalDiscount;
        delete summary.netSales;
        delete summary.voidedAmount;
        delete summary.paymentBreakdown;
    }

    res.json({
        success: true,
        summary,
        cashier: targetCashier,
        viewingOtherCashier: targetCashier.toLowerCase() !== req.authUser.username.toLowerCase(),
        canControlOtherShifts: canControlOthers,
        beginningCash: (meta.beginningCash !== undefined && meta.beginningCash !== null) ? meta.beginningCash : null,
        beginningCashLocked: meta.beginningCash !== undefined && meta.beginningCash !== null,
        beginningCashSetBy: meta.beginningCashSetBy || null
    });
});

app.get('/api/shift/open-list', (req, res) => {
    const role = req.authUser && req.authUser.role;
    const isAdminRole = (role ||'').toLowerCase() ==='admin';
    const canControlOthers = isAdminRole || !!getPermissionsForRole(role).shift_close_control;
    if (!canControlOthers) {
        return res.status(403).json({ success: false, message:'Akses Denied: Wala kang pahintulot na kontrolin ang shift close ng ibang cashier.' });
    }
    const store = readShiftMetaStore();
    const openShifts = Object.keys(store.cashiers)
        .map(username => ({ username, meta: store.cashiers[username] }))
        .filter(c => c.meta && c.meta.beginningCash !== undefined && c.meta.beginningCash !== null)
        .map(c => ({
            username: c.username,
            beginningCash: c.meta.beginningCash,
            beginningCashSetAt: c.meta.beginningCashSetAt || null
        }));
    res.json({ success: true, openShifts });
});

app.post('/api/shift/open-cash', requirePermission('terminal'), requireFeature('shift_management'), rateLimit('shift-open-cash', 20, 10 * 60 * 1000), (req, res) => {
    const store = readShiftMetaStore();
    const meta = getCashierShiftMeta(store, req.authUser.username);
    if (meta.beginningCash !== undefined && meta.beginningCash !== null) {
        return res.status(409).json({
            success: false,
            message:'The Beginning Cash for your current shift has already been set and locked.',
            beginningCash: meta.beginningCash
        });
    }
    const amount = parseFloat(req.body.beginningCash);
    if (isNaN(amount) || amount < 0) {
        return res.status(400).json({ success: false, message:'Invalid Beginning Cash amount.' });
    }
    meta.beginningCash = Math.round(amount * 100) / 100;
    meta.beginningCashSetBy = req.authUser.username;
    meta.beginningCashSetAt = new Date().toISOString();
    writeShiftMetaStore(store);
    logAction(req.authUser.username, `Set the Beginning Cash Float for a new shift: ₱${meta.beginningCash.toFixed(2)}`);
    res.json({ success: true, beginningCash: meta.beginningCash });
});

app.post('/api/shift/close', rateLimit('shift-close', 20, 10 * 60 * 1000), async (req, res) => {
    const role = req.authUser && req.authUser.role;
    const isAdminRole = (role ||'').toLowerCase() ==='admin';
    const canControlOthers = isAdminRole || !!getPermissionsForRole(role).shift_close_control;

    // SECURITY FIX: dati'y basta ika-close agad ang shift/Z-Reading ng
    // isang naka-login na Cashier — walang paraan para ma-verify ng
    // Admin/Manager/Supervisor na sila mismo ang nag-approve ng
    // pag-close (hal. bago mag-alis ng till/cash drawer). Kaya kayang
    // gawin ng isang Cashier na kupitan ang cash count nang walang
    // ibang tao na nakakaalam. Ngayon, kailangan muna ng Admin password,
    // o password ng account na may `shift_close_own_password`
    // permission (hal. Supervisor/Manager role) — pareho ang pattern
    // dito sa ginagamit na sa Void Transaction authorization.
    const { adminPassword } = req.body;
    if (!adminPassword) {
        return res.status(400).json({ success: false, message:'Kailangan ng Admin/Manager/Supervisor password para isara ang shift / Z-Reading.' });
    }
    const usersForShiftClose = readData(FILE_USERS);
    const shiftCloseAuth = await findShiftCloseAuthorizer(usersForShiftClose, adminPassword);
    if (!shiftCloseAuth) {
        return res.status(403).json({
            success: false,
            code:'WRONG_ADMIN_PASSWORD',
            message:'Maling password. Hindi pinahintulutan ang pag-close ng shift / Z-Reading.'
        });
    }

    const requestedTarget = (req.body.targetCashier ||'').toString().trim();
    let targetCashier = req.authUser.username;
    let closedOnBehalf = false;
    if (requestedTarget && requestedTarget.toLowerCase() !== targetCashier.toLowerCase()) {
        if (!canControlOthers) {
            return res.status(403).json({ success: false, message:'Akses Denied: Wala kang pahintulot na isara ang shift ng ibang cashier. Kailangan ng Admin/Supervisor control (shift_close_control) para dito.' });
        }
        targetCashier = requestedTarget;
        closedOnBehalf = true;
    }

    const store = readShiftMetaStore();
    const meta = getCashierShiftMeta(store, targetCashier);
    const targetHasOpenShift = meta.beginningCash !== undefined && meta.beginningCash !== null;
    if (!targetHasOpenShift) {

        const gateResult = checkShiftManagementUnlocked();
        if (!gateResult.unlocked) return res.status(402).json(gateResult.body);
    }

    const beginningCash = (meta.beginningCash !== undefined && meta.beginningCash !== null)
        ? meta.beginningCash
        : req.body.beginningCash;
    const { endingCashCounted, notes } = req.body;
    const periodStart = meta.lastCloseAt || new Date(0).toISOString();
    const periodEnd = new Date().toISOString();
    const summary = computeShiftSummary(periodStart, periodEnd, targetCashier);

    const isZeroActivityClose = summary.transactionCount === 0 && summary.voidCount === 0;

    // Payagan ang pag-close kahit walang transaksyon/void, BASTA may bukas
    // na shift talaga (may naka-set na beginning cash) — kasi valid na
    // use-case ito: papalitan lang ng bagong cashier at ipinapasa ang
    // hindi nagalaw na beginning cash. Kung walang bukas na shift AT walang
    // aktibidad, wala talagang dapat i-close kaya nananatiling naka-block.
    if (isZeroActivityClose && !targetHasOpenShift) {
        return res.json({ success: false, message: `Walang bukas na shift at walang bagong transaksyon o void para kay ${closedOnBehalf ? targetCashier :'sa iyo'}. Wala pang kailangang i-close.` });
    }

    const shifts = readData(FILE_SHIFTS, []);

    const cashSales = (summary.paymentBreakdown['CASH'] && summary.paymentBreakdown['CASH'].total) || 0;
    const beginCashNum = parseFloat(beginningCash) || 0;
    const expectedCash = Math.round((beginCashNum + cashSales) * 100) / 100;
    const hasCount = endingCashCounted !== undefined && endingCashCounted !=='' && endingCashCounted !== null;
    const endCashNum = hasCount ? (parseFloat(endingCashCounted) || 0) : null;
    const cashVariance = hasCount ? Math.round((endCashNum - expectedCash) * 100) / 100 : null;

    const record = {
        id:'Z-' + Date.now(),
        closedBy: targetCashier,

        closedOnBehalfBy: closedOnBehalf ? req.authUser.username : null,
        beginningCash: beginCashNum,
        endingCashCounted: endCashNum,
        cashSales,
        expectedCash,
        cashVariance,
        notes: notes ||'',
        noSalesShift: isZeroActivityClose,
        ...summary
    };
    shifts.unshift(record);
    writeData(FILE_SHIFTS, shifts);

    store.cashiers[targetCashier] = { lastCloseAt: periodEnd };
    writeShiftMetaStore(store);

    const varianceLog = cashVariance === null
        ?''
        : (cashVariance < 0
            ? `, Cash SHORT ₱${Math.abs(cashVariance).toFixed(2)}`
            : (cashVariance > 0 ? `, Cash OVER ₱${cashVariance.toFixed(2)}` :', Cash Exact'));
    const noSalesLog = isZeroActivityClose ?' (Walang Transaksyon - Shift Handover Lang)' :'';
    const authorizedByLog = shiftCloseAuth.isAdmin
        ?'Authorized by Admin'
        : `Authorized via Own Password (${shiftCloseAuth.user.username}, RBAC)`;
    const actionLog = closedOnBehalf
        ? `Closed shift / Z-Reading ${record.id} ng cashier '${targetCashier}' (Admin/Supervisor Control): ${summary.transactionCount} tx, Net Sales ₱${summary.netSales}${varianceLog}${noSalesLog} — ${authorizedByLog}`
        : `Closed shift / Z-Reading ${record.id}: ${summary.transactionCount} tx, Net Sales ₱${summary.netSales}${varianceLog}${noSalesLog} — ${authorizedByLog}`;
    logAction(req.authUser.username, actionLog);
    res.json({ success: true, shift: record });
});

app.get('/api/shifts', requirePermission('shiftreport'), requireFeature('shift_management'), (req, res) => {
    const allShifts = readData(FILE_SHIFTS, []);

    const requester = req.authUser && req.authUser.username;
    const activeRole = req.authUser && req.authUser.role;
    const isAdminRole = (activeRole ||'').toLowerCase() ==='admin';

    const canViewAll = isAdminRole || !!getPermissionsForRole(activeRole).shiftreport_view_all;
    if (canViewAll) {
        return res.json(allShifts);
    }

    const ownShifts = allShifts.filter(
        s => (s.closedBy ||'').toLowerCase() === (requester ||'').toLowerCase()
    );
    res.json(ownShifts);
});


const isProduction = process.env.NODE_ENV ==='production';
const HOST = isProduction ?'0.0.0.0' :'localhost';
const PORT = process.env.PORT || 3000;

app.listen(PORT, HOST, () => {
    console.log(`Server running at http://${HOST}:${PORT}`);
    if (isProduction) {
        console.log("MODE: Production (Public/Online Access Enabled)");
    } else {
        console.log("MODE: Development (Localhost Access Only)");
    }
});
