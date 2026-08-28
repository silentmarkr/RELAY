// PATH: RELAY/server.js  <-- I-REPLACE ang luma mong server.js NITO SA RELAY project (hindi sa OMNIPOS) — root ng RELAY project

// ====================================================================
// OmniPOS Unlock Relay — HIWALAY na maliit na service, hosted lang ng
// developer/owner (HINDI ito kasama sa client package na binebenta/
// dinideploy sa mga kliyente). 
//
// LAYUNIN: dating nangyayari LAHAT (OTP generation, storage,
// verification) sa loob ng server ng bawat kliyente — kaya kahit
// technical lang ang isang kliyente, kayang basahin ang sariling
// database nila at makita ang OTP code mismo. Dito, LUMILIPAT ang
// buong desisyon kungs "totoo ba ang unlock na ito" papunta rito — sa
// makinang HAWAK lang ng developer. Ang client server ay tumatawag
// lang dito sa network, at nagve-verify ng SIGNATURE gamit ang isang
// PUBLIC key (ligtas ipamahagi) — hindi nito kayang gumawa ng sarili
// niyang balidong approval kahit basahin niya lahat ng sariling code.
// ====================================================================

const express = require('express');
const crypto = require('crypto');
const nodemailer = require('nodemailer');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { execSync, execFileSync, spawn } = require('child_process');
const { Worker } = require('worker_threads');
const archiver = require('archiver');
const Redis = require('ioredis');
const JavaScriptObfuscator = require('javascript-obfuscator');

// --------------------------------------------------------------
// PERSISTENT STORAGE (Render Key Value / Redis) — dating JSON files lang
// sa loob ng service folder ang ginagamit dito (allowed-devices.json,
// issued-unlocks.json, atbp.), pero MAWAWALA ang mga iyon sa bawat
// restart/redeploy/spin-down kung ephemeral ang filesystem (hal. Render
// free/free-tier web service, walang naka-attach na persistent Disk).
//
// Kung naka-set ang REDIS_URL (hal. mula sa isang Render Key Value
// instance), gagamitin ito bilang TUNAY na persistent na imbakan — hindi
// na aasa sa lokal na disk. Kung WALA namang REDIS_URL (hal. sarili
// mong VPS na may sariling disk), babalik ito sa dating file-based na
// pamamaraan — walang kailangang baguhin doon.
// --------------------------------------------------------------
const REDIS_URL = process.env.REDIS_URL || null;
const redisClient = REDIS_URL
    ? new Redis(REDIS_URL, { maxRetriesPerRequest: 3, lazyConnect: false })
    : null;

if (redisClient) {
    redisClient.on('error', (err) => {
        console.error('⚠️  Redis connection error (persistent storage):', err.message);
    });
    redisClient.on('connect', () => {
        console.log('✅ Nakakonekta sa Redis/Render Key Value — gagamitin ito bilang persistent storage.');
    });
} else {
    console.warn('⚠️  Walang REDIS_URL na naka-set — babalik sa file-based na storage (mawawala ito sa ephemeral filesystem, hal. Render free web service, kada restart/redeploy).');
}

const REDIS_KEY_PREFIX = 'omnipos-relay:';

// --------------------------------------------------------------
// CLOUD BACKUP STORAGE (Postgres) — ito ang "malayong disk" na
// pinag-iimbakan ng BUONG na-sync na database (maliban sa user
// accounts) ng BAWAT OMNIPOS installation na naka-unlock ang
// 'cloud_backup' feature nito. IISANG Postgres instance lang ang
// ginagamit dito (env DATABASE_URL — pwedeng Render Postgres, Neon,
// Supabase, sariling VPS, atbp., kahit saan available), pero
// ISOLATED PER installationId ang bawat row (walang installationId
// ang makakabasa/makakapag-overwrite ng datos ng IBANG installationId
// — tingnan ang mga endpoints sa ibaba).
//
// Ang "bagong account" na ibinibigay ng developer sa isang customer
// (per instructions) ay ang PAG-UNLOCK mismo ng 'cloud_backup' feature
// sa kanilang installationId (parehong OTP/admin-activate na flow gaya
// ng ibang FEATURE_CATALOG entries) — hindi kailangan ng hiwalay na
// Postgres user/role bawat kliyente, dahil ang RELAY (na HAWAK LANG ng
// developer) ang tanging bagay na dumidiretso sa Postgres na ito.
// --------------------------------------------------------------
const { Pool } = require('pg');
const DATABASE_URL = process.env.DATABASE_URL || null;
// BUG FIX: "Connection terminated unexpectedly" mula sa `pg` (nakikita
// sa cloud-backup upload, lalo na sa malalaking store data) — karaniwang
// dahilan nito ay may intermediate NAT/load-balancer/firewall (karaniwan
// sa mga hosted platform tulad ng Render, at sa maraming managed Postgres
// provider din tulad ng Supabase/Neon sa likod ng isang pooler) na
// tahimik na pumuputol ng TCP connection na WALANG traffic sa loob ng
// ilang segundo/minuto — at kayang mangyari ito sa GITNA ng isang mabigat
// na INSERT/transaction (hal. malaking JSONB module) kahit tumatakbo pa
// ang Postgres mismo sa likod. Dinagdagan ito ng:
//  - keepAlive — nagpapadala ng TCP keepalive packets para hindi ito
//    ituring na "idle" ng mga intermediate na proxy/NAT sa itaas.
//  - statement_timeout: 0 / query_timeout: 0 — walang server/client-side
//    query timeout na maaaring pumutol nang maaga sa isang mabigat na
//    write (may sarili nang ceiling ang buong request sa pamamagitan ng
//    Express/Node request timeout, hindi na kailangan ng dagdag na
//    maikling limitasyon dito sa query level).
//  - connectionTimeoutMillis — 15s na sapat lang para gumawa ng bagong
//    koneksyon sa pool (hiwalay ito sa haba ng query mismo).
const pgPool = DATABASE_URL
    ? new Pool({
        connectionString: DATABASE_URL,
        ssl: process.env.PGSSL === 'false' ? false : { rejectUnauthorized: false },
        keepAlive: true,
        keepAliveInitialDelayMillis: 10000,
        statement_timeout: 0,
        query_timeout: 0,
        idle_in_transaction_session_timeout: 0,
        connectionTimeoutMillis: 15000
    })
    : null;

if (pgPool) {
    pgPool.on('error', (err) => {
        console.error('⚠️  Postgres pool error (cloud backup storage):', err.message);
    });
} else {
    console.warn('⚠️  Walang DATABASE_URL na naka-set — hindi gagana ang Cloud Backup (Postgres) feature hangga\'t hindi ito nalagyan.');
}

// --------------------------------------------------------------
// isTransientPgConnectionError — tinutukoy kung ang isang error mula sa
// `pg` ay dahil lang sa isang biglaang/pansamantalang pagkaputol ng
// connection (hal. "Connection terminated unexpectedly", ECONNRESET,
// admin_shutdown/crash_shutdown mula sa Postgres mismo) sa halip na isang
// TUNAY na problema sa datos/query (hal. syntax error, constraint
// violation) — ang mga una lang ang dapat awtomatikong ULITIN
// (retryable), dahil safe namang i-retry ang buong cloud-backup upload
// transaction (idempotent ito — UPSERT/ON CONFLICT DO UPDATE lahat ng
// writes dito).
// --------------------------------------------------------------
function isTransientPgConnectionError(err) {
    if (!err) return false;
    const msg = String(err.message || '');
    const code = err.code || '';
    return (
        msg.includes('Connection terminated unexpectedly') ||
        msg.includes('Connection terminated') ||
        code === 'ECONNRESET' ||
        code === 'EPIPE' ||
        code === '57P01' || // admin_shutdown
        code === '57P02' || // crash_shutdown
        code === '57P03'    // cannot_connect_now
    );
}

// --------------------------------------------------------------
// runCloudBackupWrite — isinasagawa ang buong BEGIN…COMMIT ng cloud
// backup upload gamit ang isang FRESH na connection mula sa pool, at
// AWTOMATIKONG ULINIT (hanggang 2 pang pagsubok, 3 total) kapag
// TRANSIENT connection error lang ang dahilan ng pagkabigo (tingnan sa
// itaas) — kung sa dahilang iba naman (hal. mismong mali ang datos),
// hindi ito ULIT-ULITIN, agad na itinapon ang error papunta sa caller.
// --------------------------------------------------------------
async function runCloudBackupWrite(writeFn, { maxAttempts = 3 } = {}) {
    let lastErr = null;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        const client = await pgPool.connect();
        try {
            await client.query('BEGIN');
            const result = await writeFn(client);
            await client.query('COMMIT');
            return result;
        } catch (err) {
            try { await client.query('ROLLBACK'); } catch (rollbackErr) { /* connection likely already gone — ignore */ }
            lastErr = err;
            if (!isTransientPgConnectionError(err) || attempt === maxAttempts) {
                throw err;
            }
            console.warn(`⚠️  CLOUD_BACKUP: transient Postgres connection error on attempt ${attempt}/${maxAttempts} (${err.message}) — retrying with a fresh connection…`);
        } finally {
            client.release();
        }
    }
    throw lastErr;
}

// --------------------------------------------------------------
// queryWithRetry — kagaya ng retry logic ng runCloudBackupWrite sa itaas
// (hanggang 3 attempts, fresh connection bawat retry, TRANSIENT lang na
// connection errors ang ino-otomatikong ulit-ulitin) pero para sa mga
// SIMPLENG READ (SELECT) query sa halip na isang buong BEGIN…COMMIT
// write transaction.
//
// BUG FIX: dati, ang runCloudBackupWrite lang (ginagamit ng UPLOAD/write
// path) ang may retry protection laban sa "Connection terminated
// unexpectedly" — lahat ng READ endpoints (cloud-backup/restore,
// cloud-backup/usage quota check, admin panel listing/detail/download)
// ay direktang tumatawag ng pgPool.query() nang walang retry. Ibig
// sabihin, kahit PAREHONG uri ng transient na Postgres/proxy connection
// blip (tingnan ang isTransientPgConnectionError sa itaas) ang dahilan,
// ang UPLOAD ay awtomatikong nakaka-recover na, pero ang RESTORE ay
// hindi — kaya mas madalas mag-"Failed: Could not reach RELAY" ang
// "Restore from Cloud" kesa sa "Cloud Backup Now" sa eksaktong parehong
// klase ng problema. Ginagamit na ngayon ang helper na ito sa lahat ng
// dating direktang pgPool.query() na read call para pantay ang proteksyon.
// --------------------------------------------------------------
async function queryWithRetry(text, params, { maxAttempts = 3 } = {}) {
    let lastErr = null;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            return await pgPool.query(text, params);
        } catch (err) {
            lastErr = err;
            if (!isTransientPgConnectionError(err) || attempt === maxAttempts) {
                throw err;
            }
            console.warn(`⚠️  CLOUD_BACKUP: transient Postgres connection error on read attempt ${attempt}/${maxAttempts} (${err.message}) — retrying with a fresh connection…`);
        }
    }
    throw lastErr;
}

async function ensureCloudBackupSchema() {
    if (!pgPool) return;
    await pgPool.query(`
        CREATE TABLE IF NOT EXISTS cloud_backup_modules (
            installation_id TEXT NOT NULL,
            module          TEXT NOT NULL,
            data            JSONB NOT NULL,
            record_count    INTEGER NOT NULL DEFAULT 0,
            size_bytes      BIGINT NOT NULL DEFAULT 0,
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (installation_id, module)
        );
    `);
    await pgPool.query(`
        CREATE TABLE IF NOT EXISTS cloud_backup_meta (
            installation_id   TEXT PRIMARY KEY,
            store_name        TEXT,
            total_records     INTEGER,
            module_count      INTEGER,
            size_bytes        BIGINT NOT NULL DEFAULT 0,
            last_sync_at      TIMESTAMPTZ,
            sync_count        INTEGER NOT NULL DEFAULT 0
        );
    `);
    // MIGRATION: kung dati nang existing ang mga table na ito (bago
    // idinagdag ang size_bytes column sa itaas), i-add pa rin ito nang
    // hindi nawawala ang existing data. Walang epekto ito sa mga bagong
    // installation dahil kasama na ang column sa CREATE TABLE sa itaas.
    await pgPool.query(`ALTER TABLE cloud_backup_modules ADD COLUMN IF NOT EXISTS size_bytes BIGINT NOT NULL DEFAULT 0;`);
    await pgPool.query(`ALTER TABLE cloud_backup_meta ADD COLUMN IF NOT EXISTS size_bytes BIGINT NOT NULL DEFAULT 0;`);
    console.log('✅ Cloud backup Postgres schema ready (cloud_backup_modules, cloud_backup_meta).');
}
ensureCloudBackupSchema().catch((err) => {
    console.error('⚠️  Hindi na-prepare ang Postgres schema para sa cloud backup:', err.message);
});

async function redisGetJSON(key, fallback) {
    if (!redisClient) return fallback;
    try {
        const raw = await redisClient.get(REDIS_KEY_PREFIX + key);
        if (raw === null) return fallback;
        return JSON.parse(raw);
    } catch (err) {
        console.error(`⚠️  Hindi mabasa sa Redis ang key "${key}":`, err.message);
        return fallback;
    }
}

function redisSetJSON(key, value) {
    if (!redisClient) return;
    // Fire-and-forget: hindi na kailangang i-await sa mga call site (na
    // sync ang existing na code), pero naka-catch pa rin ang errors.
    redisClient.set(REDIS_KEY_PREFIX + key, JSON.stringify(value)).catch((err) => {
        console.error(`⚠️  Hindi ma-save sa Redis ang key "${key}":`, err.message);
    });
}

// I-load ang .env file papunta sa process.env — gamit ang BUILT-IN na
// loader ng Node (available sa Node 20.12+/22+, kapareho ng bersyon na
// kailangan para sa node:sqlite sa client server). Kung wala/hindi
// mabasa ang .env (hal. sa Render, kung saan sa dashboard mismo inilagay
// ang env vars), ituloy pa rin nang tahimik — babagsak na lang sa
// ibaba ang malinaw na "Kulang ang env vars" na check kung talagang
// wala pa ring nakuhang value.
try {
    // ROOT CAUSE FIX: process.loadEnvFile() na WALANG argumento ay
    // humahanap ng ".env" relative sa process.cwd() (kung saan/paano
    // pinatakbo ang process — hal. `node /opt/relay/server.js` mula sa
    // "/"), HINDI relative sa __dirname (kung saan talaga nakatira ang
    // file na ito). Ito ang TUNAY na dahilan kung bakit "blangko" pa
    // rin minsan ang IMAGE_SEARCH_* (o anumang env var) kahit tama na
    // ang laman ng .env — maling direktoryo ang hinahanap, hindi
    // staleness ng cache. Lahat ng ibang path sa file na ito (tingnan
    // ang DEVICE_STORE_PATH, RELEASE_PACKAGE_PATH, atbp.) ay gumagamit
    // ng path.join(__dirname, ...) — dapat ganito rin dito.
    process.loadEnvFile(path.join(__dirname, '.env'));
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
//
// BUG FIX: itong 2mb na limit ay ginagamit din (dating walang exception)
// ng /relay/cloud-backup/upload — kung saan naka-attach ang BUONG
// database ng store (lahat ng modules bilang JSON). Kapag lumaki na ang
// data ng isang store (maraming transactions/products/customers), sobra
// agad ito sa 2mb at basta na-REJECT ng body-parser BAGO pa man umabot
// sa route handler — kaya "hindi basta maka-backup" kapag mataas na ang
// data. Dinagdagan ng sarili nitong mas malaking limit (1gb) ang
// cloud-backup upload route lang — hindi ito ginagawang global default
// (para hindi buksan ang ibang routes sa parehong panganib), gamit ang
// path-based na pagpili sa pagitan ng dalawang parser sa ibaba.
const CLOUD_BACKUP_UPLOAD_PATH = '/relay/cloud-backup/upload';
// BUG FIX: dating eksaktong '1024mb' (1 GiB) ang limit — kung sakaling
// aabot ang aktwal na store data sa eksaktong 1GB, may posibilidad itong
// ma-reject dahil sa JSON structural overhead (mga key names, quotes,
// brackets/commas ng array/object wrapping bawat module) sa ibabaw pa ng
// mismong laki ng mga naka-embed na base64 image. Dinagdagan ng ~15%
// headroom (1200mb) para hindi ito ang dahilan ng pagkabigo kapag
// eksaktong "1GB" mismo ang laki ng datos ng store.
const CLOUD_BACKUP_JSON_LIMIT = '1200mb';
const defaultJsonParser = express.json({ limit: '2mb' });
const cloudBackupJsonParser = express.json({ limit: CLOUD_BACKUP_JSON_LIMIT });

app.use((req, res, next) => {
    if (req.path === CLOUD_BACKUP_UPLOAD_PATH) {
        return cloudBackupJsonParser(req, res, next);
    }
    return defaultJsonParser(req, res, next);
});

// BUG FIX: wala pang dedicated error handler para sa PayloadTooLargeError
// (mula sa express.json() body-parser) dati — kaya kapag na-reject ang
// isang request dahil sobra sa limit, ang sagot ay isang generic
// HTML/plain-text error page ng Express (hindi JSON). Sa OMNIPOS client
// (parseRelayResponse sa server.js doon), hindi na-JSON.parse ang sagot
// na ito, kaya lumalabas ang misleading na error message na parang
// "hindi gumagana/naka-configure nang mali ang RELAY" — kahit ang totoong
// dahilan ay masyadong malaki lang ang datos. Dito, hinuhuli na ito
// nang partikular at binibigyan ng malinaw, user-facing na JSON message.
app.use((err, req, res, next) => {
    if (err && err.type === 'entity.too.large') {
        const limitLabel = req.path === CLOUD_BACKUP_UPLOAD_PATH ? CLOUD_BACKUP_JSON_LIMIT : '2mb';
        return res.status(413).json({
            success: false,
            payloadTooLarge: true,
            message: req.path === CLOUD_BACKUP_UPLOAD_PATH
                ? `Masyadong malaki na ang datos ng store para ma-backup sa isang pagkakataon (lumagpas sa ${limitLabel} limit). Mag-archive/burahin muna ng lumang transactions/records, o makipag-ugnayan sa developer para itaas pa ang limit.`
                : `Masyadong malaki ang request (lumagpas sa ${limitLabel} limit).`
        });
    }
    return next(err);
});

// --------------------------------------------------------------
// CONFIG — lahat ito ay dapat manggaling sa environment variables ng
// hosting mo (Render/VPS/atbp.), HINDI hardcoded dito sa source code.
// Tingnan ang .env.example para sa listahan.
// --------------------------------------------------------------
const PORT = process.env.PORT || 4477;
const RELAY_API_KEY = process.env.RELAY_API_KEY || null; // shared secret — pumipigil sa random tao (hindi mo kliyente) na mag-spam sa relay mo

// SerpAPI (Product Image Search) na dapat awtomatikong isama sa .env ng
// bawat bagong omnipos-client.zip na binubuo dito sa RELAY (build-release),
// kapareho ng key na ginagamit na sa panig ng OMNIPOS .env mismo — kaya
// AGAD gumagana ang "Search Image" button sa Add/Edit Product form ng
// bagong kliyente, walang kailangan pang i-configure nang manual. Kasama
// ito dito bilang ENV VAR lang (hindi hardcoded value) — tugma sa
// convention ng buong file na ito: manggagaling dapat sa environment
// variables ng hosting mo (Render/VPS/atbp.) ang mga secret, HINDI
// isusulat diretso sa source code. I-set ang IMAGE_SEARCH_API_KEY dito
// gamit ang PAREHONG value na nasa IMAGE_SEARCH_API_KEY ng OMNIPOS .env mo.
//
// BUG FIX (history): dating ini-cache ito dito bilang mga module-level
// constant (binabasa lang ISANG BESES, nang mag-boot ang RELAY process).
// Nag-attempt na dati ng fix na gawing direktang process.env.IMAGE_SEARCH_*
// na lang ang binabasa sa loob ng performBuildRelease sa halip na cached
// constants — pero HINDI pa rin nito nalulutas ang totoong ugat ng
// problema: si process.env mismo ay stale, dahil ang process.loadEnvFile()
// (tingnan sa itaas ng file) ay tumatakbo lang NANG ISANG BESES sa pag-boot.
// Kaya kahit "direkta" na ang pagbasa sa process.env sa loob ng
// performBuildRelease, kung stale/wala pa rin ang process.env mismo
// (hal. kadarating lang idagdag ang IMAGE_SEARCH_API_KEY sa .env HABANG
// tumatakbo na ang RELAY), stale/wala pa rin ang mababasa. Ang TUNAY na fix
// ay nasa simula ng performBuildRelease(): muling tinatawag doon ang
// process.loadEnvFile() sa mismong oras ng build/publish, kaya nare-refresh
// ang process.env ng anumang BAGONG idinagdag na key sa .env nang hindi na
// kailangang i-restart ang buong RELAY process.
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

async function loadAllowedDevices() {
    const fromRedis = await redisGetJSON('allowed-devices', null);
    if (fromRedis !== null) return new Set(fromRedis);
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
    if (redisClient) {
        redisSetJSON('allowed-devices', [...set]);
        return;
    }
    try {
        fs.writeFileSync(DEVICE_STORE_PATH, JSON.stringify([...set], null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang allowed-devices.json:', err);
    }
}

let allowedDevices = new Set(); // pupunuin sa bootstrapStores() bago tumakbo ang httpServer.listen()

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

async function loadDeviceLabels() {
    const fromRedis = await redisGetJSON('device-labels', null);
    if (fromRedis !== null) return new Map(Object.entries(fromRedis));
    try {
        const raw = fs.readFileSync(DEVICE_LABELS_PATH, 'utf8');
        return new Map(Object.entries(JSON.parse(raw)));
    } catch (err) {
        return new Map();
    }
}

function saveDeviceLabels(map) {
    if (redisClient) {
        redisSetJSON('device-labels', Object.fromEntries(map));
        return;
    }
    try {
        fs.writeFileSync(DEVICE_LABELS_PATH, JSON.stringify(Object.fromEntries(map), null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang device-labels.json:', err);
    }
}

let deviceLabels = new Map(); // pupunuin sa bootstrapStores()

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

async function loadDeviceFingerprints() {
    const fromRedis = await redisGetJSON('device-fingerprints', null);
    if (fromRedis !== null) return new Map(Object.entries(fromRedis));
    try {
        const raw = fs.readFileSync(DEVICE_FINGERPRINTS_PATH, 'utf8');
        return new Map(Object.entries(JSON.parse(raw)));
    } catch (err) {
        return new Map();
    }
}

function saveDeviceFingerprints(map) {
    if (redisClient) {
        redisSetJSON('device-fingerprints', Object.fromEntries(map));
        return;
    }
    try {
        fs.writeFileSync(DEVICE_FINGERPRINTS_PATH, JSON.stringify(Object.fromEntries(map), null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang device-fingerprints.json:', err);
    }
}

let deviceFingerprints = new Map(); // installationId -> { fingerprint, firstVerifiedAt, lastVerifiedAt, verifyCount, flagged, flaggedFingerprint, flaggedAt } — pupunuin sa bootstrapStores()

// --------------------------------------------------------------
// CLONE SPLIT MAP — para sa mga na-flag na clone na GUSTONG PATULOY na
// PAGANAHIN bilang SARILI at HIWALAY na device (hal. dating tester unit
// mo, ibinenta mo na sa customer, at gusto mong tuloy-tuloy pa rin
// gumana ang DALAWA — yung luma mong unit AT yung binenta mo — bilang
// dalawang magkaibang installationId, sa halip na "musical chairs" lang
// na iisang ID na palit-palit ng may-ari).
//
// Key: `${originalInstallationId}::${flaggedFingerprint}`
// Value: { newInstallationId, splitAt }
//
// Ginagamit ito ng /relay/verify-login: kapag may fingerprint mismatch
// na TUMUTUGMA sa isang naka-split na na entry dati, sa halip na
// i-flag ulit bilang clone_suspected, ipapaalam sa CLIENT (sa pamamagitan
// ng `reassignedInstallationId` sa response) na dapat lumipat na ito
// sa BAGONG installationId — mula noon, magpapadala na ang client na
// iyon ng bagong ID sa lahat ng susunod na request, kaya ganap na silang
// dalawang hiwalay/independent na "device" na sa mata ng RELAY.
// --------------------------------------------------------------
const CLONE_SPLITS_PATH = path.join(__dirname, 'clone-splits.json');

async function loadCloneSplits() {
    const fromRedis = await redisGetJSON('clone-splits', null);
    if (fromRedis !== null) return new Map(Object.entries(fromRedis));
    try {
        const raw = fs.readFileSync(CLONE_SPLITS_PATH, 'utf8');
        return new Map(Object.entries(JSON.parse(raw)));
    } catch (err) {
        return new Map();
    }
}

function saveCloneSplits(map) {
    if (redisClient) {
        redisSetJSON('clone-splits', Object.fromEntries(map));
        return;
    }
    try {
        fs.writeFileSync(CLONE_SPLITS_PATH, JSON.stringify(Object.fromEntries(map), null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang clone-splits.json:', err);
    }
}

let cloneSplits = new Map(); // pupunuin sa bootstrapStores()
function cloneSplitKey(installationId, fingerprint) {
    return `${installationId}::${fingerprint}`;
}

// In-memory lang, para lang sa "recently seen" view sa admin panel.
const seenDevices = new Map(); // installationId -> { storeName, username, lastSeenAt, requestCount }

// --------------------------------------------------------------
// ONLINE STATUS (TTL-based, via Redis) — dati, "online" ang isang
// device kung meron lang siyang lastSeenAt (kahit ilang ORAS na ang
// nakalipas), dahil walang expiry ang seenDevices Map sa itaas. Bunga:
// isang clone device na tumigil na (na-block, na-stop ang process,
// o na-logout) ay LAGING lalabas na "online" hangga't hindi
// nire-restart ang RELAY server (nawawala lang ang in-memory Map).
//
// Dito, gumagawa tayo ng HIWALAY na "heartbeat" key per device na
// may TTL sa Redis (ire-refresh ito kada request, tulad ng ginagawa
// ng OMNIPOS client kada 30s sa /relay/check-feature-status). Kapag
// umere-expire ang key (ibig sabihin, hindi na nag-request ang
// device sa loob ng ONLINE_WINDOW_MS), awtomatiko itong nawawala sa
// Redis — walang kailangang i-cron/i-clean pa manually.
//
// ONLINE_WINDOW_MS = 90s: bahagyang mas mahaba sa default na 30s na
// sync interval ng OMNIPOS client (RELAY_FEATURE_SYNC_INTERVAL_MS),
// para may tolerance sa isang naka-miss na beat (hal. dahil sa slow
// network) nang hindi agad nagpapakita ng false "offline".
//
// Kung WALANG naka-configure na REDIS_URL, babalik ito sa dating
// in-memory na pagtantiya gamit ang lastSeenAt (approximate lang,
// mawawala rin ito sa restart tulad ng dati) — ligtas pa ring
// tumatakbo ang RELAY kahit walang Redis.
// --------------------------------------------------------------
const ONLINE_WINDOW_MS = 90 * 1000;
const ONLINE_KEY_PREFIX = REDIS_KEY_PREFIX + 'online:';

function markDeviceOnline(installationId) {
    if (!installationId || !redisClient) return;
    redisClient.set(ONLINE_KEY_PREFIX + installationId, '1', 'PX', ONLINE_WINDOW_MS).catch((err) => {
        console.error(`⚠️  Hindi ma-set ang online heartbeat key para sa "${installationId}":`, err.message);
    });
}

// Batch check — mas efficient kaysa isa-isahang GET kada device sa
// listahan (gamit ang Redis pipeline, iisang round-trip lang).
async function getOnlineStatusMap(installationIds) {
    const now = Date.now();
    if (!redisClient) {
        // Fallback na walang Redis: itinuturing na "online" kung
        // may request sa loob ng ONLINE_WINDOW_MS ayon sa in-memory
        // na seenDevices (approximate lang, walang cross-restart
        // survival, pero mas tama pa rin kaysa "laging online").
        const map = {};
        for (const id of installationIds) {
            const meta = seenDevices.get(id);
            map[id] = !!(meta && (now - meta.lastSeenAt) < ONLINE_WINDOW_MS);
        }
        return map;
    }

    if (installationIds.length === 0) return {};
    const pipeline = redisClient.pipeline();
    installationIds.forEach((id) => pipeline.exists(ONLINE_KEY_PREFIX + id));
    const results = await pipeline.exec();
    const map = {};
    installationIds.forEach((id, i) => {
        const [err, exists] = results[i] || [null, 0];
        map[id] = !err && exists === 1;
    });
    return map;
}

function recordDeviceSeen(installationId, meta = {}) {
    if (!installationId) return;
    const existing = seenDevices.get(installationId) || { requestCount: 0 };
    seenDevices.set(installationId, {
        storeName: meta.storeName || existing.storeName || null,
        username: meta.username || existing.username || null,
        lastSeenAt: Date.now(),
        requestCount: existing.requestCount + 1
    });
    markDeviceOnline(installationId);
}

function requireAllowedDevice(req, res, next) {
    // FIX: dating req.body lang ang tinitignan dito — gumana lang ito
    // para sa POST endpoints. Para sa GET /relay/branch-summary (walang
    // JSON body sa isang GET request — hindi ito pinapayagan ng Fetch
    // spec), kailangang tanggapin din ang installationId mula sa query
    // string bilang fallback.
    const installationId = (req.body && req.body.installationId) || req.query.installationId;
    const storeName = (req.body && req.body.storeName) || undefined;
    const username = (req.body && req.body.username) || undefined;
    recordDeviceSeen(installationId, { storeName, username }); // laging i-log, kahit tanggihan pagkatapos
    // STRICT BY DEFAULT: dati, kapag WALA pang laman ang allowedDevices
    // (bagong deploy, o pagkatapos mag-clone-reset), basta-basta
    // pinapayagan ang LAHAT ng device na dumaan dito nang walang
    // restriction — ibig sabihin, kayang mag-request-unlock/demo/backup
    // ang KAHIT SINONG bagong kliyente hangga't wala pang unang device na
    // manual na na-Allow ng developer. Tinanggal na ito — ngayon,
    // KAILANGAN palaging EXPLICIT na "Allow" mula sa developer/store
    // owner bago payagan ang KAHIT ANONG installationId, kahit pa unang
    // device pa lang ito o kahit walang laman ang listahan.
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
const FEATURE_CATALOG_BASE = {
    ocean: { name: 'Ocean Pro', price: 149, category: 'theme' },
    emerald: { name: 'Emerald Pro', price: 149, category: 'theme' },
    sunset: { name: 'Sunset Pro', price: 149, category: 'theme' },
    rosegold: { name: 'Rose Gold Pro', price: 149, category: 'theme' },
    cyber: { name: 'Cyber Neon Pro', price: 149, category: 'theme' },
    noir: { name: 'Coffee Noir Pro', price: 149, category: 'theme' },
    mintfrost: { name: 'Mint Frost Pro', price: 149, category: 'theme' },
    liquidglass: { name: 'Liquid Glass Pro', price: 149, category: 'theme' },
    galaxyambient: { name: 'Galaxy Ambient Pro', price: 149, category: 'theme' },
    purchase_orders: { name: 'Purchase Orders Module', price: 999, category: 'module' },
    customer_crm: { name: 'Customer Profiles, Loyalty & Debtors', price: 799, category: 'module' }, // naka-sync sa OMNIPOS/server.js FEATURE_CATALOG — pinalitan ang pangalan para makasama ang Debtors ledger
    promo_codes: { name: 'Promo Codes Module', price: 499, category: 'module' },
    advanced_reports: { name: 'Sales Analytics & Advanced Reports', price: 799, category: 'module' },
    shift_management: { name: 'Multi-Cashier Shift Oversight & Z-Reading Reports', price: 699, category: 'module' },
    rbac_management: { name: 'Roles & Permissions (RBAC) Management', price: 999, category: 'module' },
    // BUG FIX: 'multi_branch' was missing from this mirror entirely. Since
    // this file is a MANUALLY-synced copy of OMNIPOS/server.js's
    // FEATURE_CATALOG, and the 'pro' tier below is built from
    // Object.keys(FEATURE_CATALOG), a missing entry here means
    // 'multi_branch' silently drops out of the 'pro' tier's featureIds —
    // so the admin panel's "Activate Pro" bulk action never issues a
    // token for it, and it stays locked even after the rest of Pro is
    // activated (and, since it was never actually activated in the first
    // place, "Deactivate Pro"/"Deactivate All" has nothing to remove for
    // it either — it just remains locked either way).
    multi_branch: { name: 'Multi-Branch Dashboard', price: 999, category: 'module' },
    // 'cloud_backup': no longer has a single flat `price` here — it's a
    // subscription now (Basic/Standard/Pro, monthly/yearly). See
    // CLOUD_BACKUP_PLANS below, which is now the GROUND TRUTH for pricing
    // (must be mirrored exactly in OMNIPOS/server.js).
    cloud_backup: { name: 'Cloud Backup (Postgres)', price: null, category: 'module', isSubscription: true }
};

// ============================================================
// CLOUD BACKUP — SUBSCRIPTION PLANS (must match OMNIPOS/server.js
// CLOUD_BACKUP_PLANS exactly — this IS the ground truth for pricing
// since this is RELAY, not just whatever the client claims).
// ============================================================
const CLOUD_BACKUP_PLANS = {
    basic: { id: 'basic', name: 'Cloud Backup — Basic', price: { monthly: 129, yearly: 1290 }, storageQuotaMB: 250 },
    standard: { id: 'standard', name: 'Cloud Backup — Standard', price: { monthly: 249, yearly: 2490 }, storageQuotaMB: 1024 },
    pro: { id: 'pro', name: 'Cloud Backup — Pro', price: { monthly: 399, yearly: 3990 }, storageQuotaMB: 5120 }
};
const CLOUD_BACKUP_BILLING_DAYS = { monthly: 30, yearly: 365 };

function getCloudBackupPlanPrice(tier, billingCycle) {
    const plan = CLOUD_BACKUP_PLANS[tier];
    if (!plan || !CLOUD_BACKUP_BILLING_DAYS[billingCycle]) return null;
    return typeof plan.price[billingCycle] === 'number' ? plan.price[billingCycle] : null;
}

// --------------------------------------------------------------
// AUTO-LEARNED FEATURES — kapag may dumaang unlock request papunta sa
// RELAY na ang featureId ay HINDI pa kilala dito sa FEATURE_CATALOG_BASE
// (hal. bagong theme/module na idinagdag sa OMNIPOS pero nakalimutang
// i-mirror dito), awtomatikong idinadagdag ito sa "overrides" store na
// ito sa SANDALING ma-isyu na ang unlock nito (tingnan ang
// registerFeatureIfUnknown() at recordIssuedUnlock() sa ibaba) — hindi
// agad sa pagdating pa lang ng /relay/request-unlock (dahil client-
// supplied pa lang ang featureName/price doon; ang totoong "ground
// truth" ay ang sandaling aktwal nang na-isyu ang token, pagkatapos ng
// OTP/admin approval). Kaya awtomatiko nang naka-sync ang FEATURE_CATALOG
// mirror na ito sa OMNIPOS sa paglipas ng panahon, kahit walang manual
// pag-edit dito — pero PANSAMANTALA lang ito bilang "narinig na namin
// ito" na entry; puwede pa ring i-refine ang pangalan/presyo/category
// nito dito sa FEATURE_CATALOG_BASE paglipas ng panahon kung gusto.
// --------------------------------------------------------------
const FEATURE_CATALOG_OVERRIDES_PATH = path.join(__dirname, 'feature-catalog-overrides.json');

async function loadFeatureCatalogOverrides() {
    const fromRedis = await redisGetJSON('feature-catalog-overrides', null);
    if (fromRedis !== null) return fromRedis;
    try {
        return JSON.parse(fs.readFileSync(FEATURE_CATALOG_OVERRIDES_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}

function saveFeatureCatalogOverrides(obj) {
    if (redisClient) {
        redisSetJSON('feature-catalog-overrides', obj);
        return;
    }
    try {
        fs.writeFileSync(FEATURE_CATALOG_OVERRIDES_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang feature-catalog-overrides.json:', err);
    }
}

let featureCatalogOverrides = {}; // pupunuin sa bootstrapStores()

// Ang mismong FEATURE_CATALOG na ginagamit ng buong file (admin panel,
// pricing, atbp.) ay ang BASE + anumang auto-learned na overrides.
// `let` (hindi `const`) dahil dinadagdagan ito sa runtime ng
// registerFeatureIfUnknown() sa sandaling may bagong featureId na
// ma-detect.
let FEATURE_CATALOG = { ...FEATURE_CATALOG_BASE };

const UPGRADE_TIERS = [
    { id: 'basic', name: 'Basic Upgrade', featureIds: ['advanced_reports', 'promo_codes'], bundlePrice: 999 },
    { id: 'standard', name: 'Standard Upgrade', featureIds: ['advanced_reports', 'promo_codes', 'customer_crm', 'shift_management'], bundlePrice: 1999 },
    // 'pro' ay laging LAHAT ng laman ng FEATURE_CATALOG sa oras na ito i-
    // resolve (tingnan ang recomputeProTierFeatureIds(), tinatawag sa
    // bootstrapStores() at muli sa tuwing may auto-add) — kaya kasama na
    // rito agad ang anumang bagong theme/module, manual man o auto-learned.
    // bundlePrice: naka-sync sa OMNIPOS/server.js UPGRADE_TIERS 'pro'
    // (6499, now including Multi-Branch Dashboard in this price).
    // NOTE: 'cloud_backup' is no longer included here — it's a
    // subscription now (Basic/Standard/Pro monthly/yearly), so it's
    // purchased SEPARATELY from the one-time Pro bundle, not part of this
    // bundlePrice.
    { id: 'pro', name: 'Pro Upgrade (Complete)', featureIds: Object.keys(FEATURE_CATALOG).filter(id => id !== 'cloud_backup'), bundlePrice: 6499 }
];

function recomputeProTierFeatureIds() {
    const proTier = UPGRADE_TIERS.find(t => t.id === 'pro');
    if (proTier) proTier.featureIds = Object.keys(FEATURE_CATALOG).filter(id => id !== 'cloud_backup');
}

// Tinatawag sa sandaling AKTWAL nang na-isyu ang isang unlock token (hindi
// sa simpleng pagdating pa lang ng request) para sa featureId na wala pa
// sa FEATURE_CATALOG — ibig sabihin, may bagong locked feature sa
// OMNIPOS client na hindi pa naka-mirror dito. Gumagamit ng
// meta.featureName/meta.price na sinama na ng OMNIPOS client sa unlock
// request bilang panimulang detalye ng bagong entry.
function registerFeatureIfUnknown(featureId, meta = {}, installationId = null) {
    if (!featureId || FEATURE_CATALOG[featureId]) return false;
    if (!meta.featureName) return false; // walang sapat na detalye para awtomatikong idagdag
    // Ang DEMO_FEATURE_ID ('__demo__') ay SADYANG HINDI kasama sa
    // FEATURE_CATALOG (hindi ito isang binibiling feature/theme/module,
    // kundi ang pansamantalang "buksan lahat" demo mode) — huwag itong
    // idagdag dito o sa 'pro' bundle kahit pa dumaan ito sa
    // recordIssuedUnlock().
    if (typeof DEMO_FEATURE_ID !== 'undefined' && featureId === DEMO_FEATURE_ID) return false;

    const entry = {
        name: meta.featureName,
        price: typeof meta.price === 'number' ? meta.price : null,
        category: meta.category || 'module',
        autoAdded: true,
        learnedAt: Date.now(),
        learnedFrom: meta.source || null
    };

    featureCatalogOverrides[featureId] = entry;
    FEATURE_CATALOG = { ...FEATURE_CATALOG, [featureId]: entry };
    recomputeProTierFeatureIds();
    saveFeatureCatalogOverrides(featureCatalogOverrides);

    console.log(`🆕 Bagong feature na na-detect at awtomatikong idinagdag sa RELAY catalog mirror: ${featureId} (${entry.name}, ${entry.price !== null ? '₱' + entry.price : 'walang presyo'}).`);
    try {
        logActivity(installationId, 'feature_auto_registered', { featureId, featureName: entry.name, price: entry.price, source: entry.learnedFrom });
    } catch (err) {
        // logActivity/activityLog ay maaaring wala pa sa unang pagkakataong
        // ito ay tawagin habang naglo-load pa lang (di malamang, pero safe
        // guard lang) — hindi dapat ma-block ang auto-registration dahil dito.
    }
    return true;
}

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

async function loadIssuedUnlocks() {
    const fromRedis = await redisGetJSON('issued-unlocks', null);
    if (fromRedis !== null) return fromRedis;
    try {
        return JSON.parse(fs.readFileSync(ISSUED_UNLOCKS_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}

function saveIssuedUnlocks(obj) {
    if (redisClient) {
        redisSetJSON('issued-unlocks', obj);
        return;
    }
    try {
        fs.writeFileSync(ISSUED_UNLOCKS_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang issued-unlocks.json:', err);
    }
}

let issuedUnlocks = {}; // pupunuin sa bootstrapStores()

function recordIssuedUnlock(installationId, featureId, token, meta = {}) {
    // Bago i-record, tingnan muna kung ito ay isang featureId na wala pa
    // sa FEATURE_CATALOG mirror ng RELAY — kung gayon, awtomatiko itong
    // idadagdag dito gamit ang featureName/price na ipinasa ng OMNIPOS
    // client (tingnan ang registerFeatureIfUnknown() sa itaas).
    registerFeatureIfUnknown(featureId, { featureName: meta.featureName, price: meta.price, source: meta.source }, installationId);

    if (!issuedUnlocks[installationId]) issuedUnlocks[installationId] = {};
    issuedUnlocks[installationId][featureId] = {
        featureName: meta.featureName || (FEATURE_CATALOG[featureId] && FEATURE_CATALOG[featureId].name) || featureId,
        price: typeof meta.price === 'number' ? meta.price : (FEATURE_CATALOG[featureId] && FEATURE_CATALOG[featureId].price) || null,
        issuedAt: token.payload.issuedAt,
        expiresAt: typeof token.payload.expiresAt === 'number' ? token.payload.expiresAt : null,
        payload: token.payload,
        signature: token.signature,
        source: meta.source || 'otp', // 'otp' | 'admin-direct'
        note: meta.note || null,
        // Cloud Backup subscription metadata only (Basic/Standard/Pro,
        // Monthly/Yearly) — so the admin panel/analytics can show which
        // plan was activated; the expiresAt above (from the signed token)
        // is still what actually enforces access.
        tier: meta.tier || null,
        billingCycle: meta.billingCycle || null
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

async function loadActivityLog() {
    const fromRedis = await redisGetJSON('activity-log', null);
    if (fromRedis !== null) return fromRedis;
    try {
        return JSON.parse(fs.readFileSync(ACTIVITY_LOG_PATH, 'utf8'));
    } catch (err) {
        return [];
    }
}

function saveActivityLog(arr) {
    if (redisClient) {
        redisSetJSON('activity-log', arr);
        return;
    }
    try {
        fs.writeFileSync(ACTIVITY_LOG_PATH, JSON.stringify(arr, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang activity-log.json:', err);
    }
}

let activityLog = []; // pupunuin sa bootstrapStores()

// --------------------------------------------------------------
// Mga uri ng activity na "ROUTINE"/inaasahang paulit-ulit (feature
// activate/renew/deactivate, auto-backup sync, status check, relabel,
// atbp.) — sa halip na dumagdag ng BAGONG ROW kada pangyayari,
// ii-UPDATE na lang ang PETSA/ORAS (at bilang ng beses) ng ISANG
// existing na row para sa parehong (device + uri + feature), para
// hindi kumakapal nang sobra ang History timeline sa mga paulit-ulit
// na aksyon (hal. maraming "Renew" sa iisang feature).
//
// SADYANG HINDI kasama dito ang mga SECURITY-sensitive na event (hal.
// clone_suspected, integrity_alert, cloud_backup_restore_blocked,
// device_first_verified, clone_split_*, demo_ended_early, atbp.) —
// dapat MANATILING MAGKAKAHIWALAY ang bawat isa sa mga iyon, dahil
// mahalaga ang bawat pagkakataon para sa fraud/abuse detection (hal.
// paulit-ulit na clone attempt sa loob ng maikling panahon ay dapat
// makita nang buo, hindi natatago sa likod ng isang "Nx" na bilang).
// --------------------------------------------------------------
const MERGEABLE_ACTIVITY_TYPES = new Set([
    'unlock_issued', 'admin_approved', 'feature_deactivated', 'otp_requested',
    'device_allowed', 'device_revoked', 'device_labeled', 'device_fingerprint_reset',
    'device_reverified', 'restore_checkin', 'feature_status_checked', 'backup_checkin',
    'device_history_cleared', 'device_bulk_reset', 'device_reset',
    'integrity_check_requested', 'integrity_alert_cleared'
]);

function logActivity(installationId, type, details = {}) {
    const now = Date.now();

    if (MERGEABLE_ACTIVITY_TYPES.has(type)) {
        // Kung may featureId ang event (hal. unlock_issued para sa isang
        // partikular na module), hiwalay pa rin ang row PER FEATURE —
        // gusto pa rin nating makita ang "huling ginalaw" ng BAWAT
        // feature, hindi lang ng device sa kabuuan.
        const featureId = (details && details.featureId) ? details.featureId : '';
        const mergeKey = `${installationId || ''}|${type}|${featureId}`;
        const existingIndex = activityLog.findIndex(e => e._mergeKey === mergeKey);

        if (existingIndex !== -1) {
            const existing = activityLog[existingIndex];
            existing.details = details; // pinaka-bagong detalye (hal. bagong price/note)
            existing.at = now;
            existing.count = (existing.count || 1) + 1;
            // Ilipat sa unahan ng listahan — parang "bumped": ang
            // pinaka-huling ginalaw na row ang unang makikita sa timeline.
            activityLog.splice(existingIndex, 1);
            activityLog.unshift(existing);
            saveActivityLog(activityLog);
            return;
        }

        activityLog.unshift({
            installationId: installationId || null,
            type,
            details,
            at: now,
            firstAt: now,
            count: 1,
            _mergeKey: mergeKey
        });
        if (activityLog.length > ACTIVITY_LOG_MAX) activityLog.length = ACTIVITY_LOG_MAX;
        saveActivityLog(activityLog);
        return;
    }

    // Hindi mergeable (security-sensitive/one-off) — dating gawi: laging
    // BAGONG row, para buo ang record ng bawat pagkakataon.
    activityLog.unshift({
        installationId: installationId || null,
        type,
        details,
        at: now
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

async function loadBackupCheckins() {
    const fromRedis = await redisGetJSON('backup-checkins', null);
    if (fromRedis !== null) return fromRedis;
    try {
        return JSON.parse(fs.readFileSync(BACKUP_CHECKINS_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}

function saveBackupCheckins(obj) {
    if (redisClient) {
        redisSetJSON('backup-checkins', obj);
        return;
    }
    try {
        fs.writeFileSync(BACKUP_CHECKINS_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang backup-checkins.json:', err);
    }
}

let backupCheckins = {}; // installationId -> { lastBackupAt, storeName, fileSizeBytes, checkinCount } — pupunuin sa bootstrapStores()

// --------------------------------------------------------------
// MULTI-BRANCH SUMMARY STORAGE
// Layunin: pinapayagan ang isang merchant na may 2+ magkahiwalay na
// OMNIPOS install (magkaibang LAN/lokasyon, hal. iba't ibang branch ng
// parehong tindahan) na makita ang COMBINED na sales/stock snapshot ng
// lahat ng branch nila sa isang lugar — kahit walang direktang network
// connection ang mga branch sa isa't isa (RELAY na ang tanging bagay na
// nakikipag-usap sa LAHAT ng OMNIPOS install, kaya ito ang natural na
// "meeting point" para dito, hindi kailangan pa ng bagong infra).
//
// PAANO NAG-GGROUP: pinipili ng merchant ang sarili nilang "Business
// Group Code" (malayang text, hal. pangalan ng negosyo) sa loob ng
// Store & Sales Settings ng BAWAT branch device — dapat PAREHONG value
// ang ilagay nila sa lahat ng device na gusto nilang pagsamahin. HINDI
// ipinapadala ang hilaw/raw na code na ito papunta dito — ang OMNIPOS
// client mismo ang kumukuha ng SHA-256 hash nito bago ipadala
// (`branchGroupKeyHash`), kaya kahit ang RELAY (o sinumang makakabasa
// ng request papunta rito) ay hindi malalaman ang aktwal na code, tanging
// ang hash lang. Ito ay parang isang shared "room code" — hindi ito
// pangangalagaan gaya ng password (walang salt/bcrypt), pero sapat na
// ito para maiwasan ang random na pagkakataon (accidental collision) at
// hindi basta nire-reveal ang plain code sa logs/storage ng RELAY.
//
// Estraktura: { [branchGroupKeyHash]: { [installationId]: { branchName,
// summary: {...}, updatedAt } } }
// --------------------------------------------------------------
const BRANCH_SUMMARIES_PATH = path.join(__dirname, 'branch-summaries.json');

async function loadBranchSummaries() {
    const fromRedis = await redisGetJSON('branch-summaries', null);
    if (fromRedis !== null) return fromRedis;
    try {
        return JSON.parse(fs.readFileSync(BRANCH_SUMMARIES_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}

function saveBranchSummaries(obj) {
    if (redisClient) {
        redisSetJSON('branch-summaries', obj);
        return;
    }
    try {
        fs.writeFileSync(BRANCH_SUMMARIES_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang branch-summaries.json:', err);
    }
}

let branchSummaries = {}; // pupunuin sa bootstrapStores()

const BRANCH_GROUP_HASH_RE = /^[a-f0-9]{64}$/; // dapat lowercase hex SHA-256 (64 chars)
const BRANCH_NAME_MAX_LEN = 60;
// Whitelist ng mga numeric field na tinatanggap sa loob ng "summary" —
// kahit anong ipasa ang client na wala rito ay basta na lang tatanggalin,
// para hindi maging bukas na channel ito para sa arbitrary data storage.
const BRANCH_SUMMARY_NUMERIC_FIELDS = [
    'grossSalesToday', 'netSalesToday', 'transactionCountToday',
    'lowStockCount', 'activeShiftCount'
];

function sanitizeBranchSummaryPayload(raw) {
    const out = {};
    const src = (raw && typeof raw === 'object') ? raw : {};
    for (const field of BRANCH_SUMMARY_NUMERIC_FIELDS) {
        const n = Number(src[field]);
        out[field] = Number.isFinite(n) ? Math.max(0, n) : 0;
    }
    return out;
}

// Default na NAKA-ON ang auto-allow-on-backup (mas kaunting manual na
// hakbang para sa developer) — i-set ang env var na ito sa 'false' kung
// gusto mo pa ring manual na i-Allow bawat device sa admin panel kahit
// successful na ang backup check-in nito.
const AUTOALLOW_ON_BACKUP = String(process.env.RELAY_AUTOALLOW_ON_BACKUP || 'true').trim().toLowerCase() !== 'false';

function mostRecentBackupCheckinAt() {
    const values = Object.values(backupCheckins).map((c) => c.lastBackupAt).filter(Boolean);
    return values.length ? Math.max(...values) : null;
}

// ====================================================================
// FILE INTEGRITY MONITORING ("git status" para sa deployed client)
// --------------------------------------------------------------------
// LAYUNIN: malaman ng developer/owner (sa RELAY admin panel) kung may
// na-edit o na-delete na FILE ang isang client sa loob ng sarili
// nilang OMNIPOS install — hal. binuksan/binago ang server.js para
// tanggalin ang isang license/anti-clone check, o binura ang isang
// file na kasama dapat sa release. Dalawang bahagi ito:
//
//   1. BASELINE MANIFEST — kada matagumpay na build (performBuildRelease,
//      tingnan sa ibaba), kinukuha ang sha256 hash ng BAWAT file na
//      talagang isinama sa release zip (mula mismo sa tmpDir bago pa
//      i-zip — ibig sabihin eksaktong kapareho ito ng ibinigay sa
//      customer), at itinatago bilang "baseline" PARA SA VERSION na
//      iyon (systemVersionInfo.version ng build na iyon). Maraming
//      version ang naka-imbak nang sabay (hindi lang "pinaka-huli"),
//      dahil hindi laging naka-update agad ang lahat ng client.
//   2. CLIENT CHECK-IN — ang OMNIPOS client mismo (tingnan ang bagong
//      runRelayIntegrityCheckin() sa OMNIPOS/server.js) ay pana-panahong
//      kinukuha rin ang sha256 hash ng bawat file sa sarili nitong
//      install folder (gamit ang PAREHONG exclude list — .env,
//      .env.key, database/, node_modules/, uploads_tmp/, .git/,
//      release/, *.log — dahil normal/inaasahan lang na magkaiba ang
//      mga ito bawat device), at ipinapadala ito dito
//      (POST /relay/integrity-checkin) kasama ang sariling APP_VERSION
//      nito. Dito, kino-compare ito sa naka-imbak na baseline PARA SA
//      VERSION NA IYON — kung walang tugmang baseline (hal. sobrang
//      luma/bago pa lang i-publish), hindi ito basta-basta finaflag.
//
// Resulta ng paghahambing (tatlong klase, gaya ng "git status"):
//   - modified: nasa parehong panig, pero IBA ang hash (binago)
//   - deleted:  nasa baseline, WALA sa client (binura)
//   - added:    WALA sa baseline, NASA client (bagong idinagdag na file
//               na hindi bahagi ng orihinal na release — hal. isang
//               ipinasok na backdoor/patch file)
// Kahit alin sa tatlo ay nagre-red-flag sa device sa admin panel.
// ====================================================================

// Exclude set na dapat MAG-TUGMA (parehong pangalan) sa ginagamit ng
// OMNIPOS client mismo (SELF_UPDATE_PRESERVE sa OMNIPOS/server.js) at
// sa build pipeline dito (BUILD_EXCLUDE_NAMES/EXTENSIONS sa ibaba) —
// kung hindi tugma ang mga ito, magkakaroon ng maling positibo
// (false "modified/deleted") na hindi naman talagang tampering, kundi
// runtime data lang na iba-iba talaga bawat device.
const INTEGRITY_EXCLUDE_NAMES = new Set([
    // BUG FIX: idinagdag ang '.self-update-backup' para tumugma sa
    // EXCLUDE set ng build-release.js (OMNIPOS) — kung natirang
    // hindi nalinis ang backup folder na ito pagkatapos ng isang
    // self-update, huwag itong isali sa integrity comparison (hindi
    // ito bahagi ng aktwal na release).
    '.env', '.env.key', 'database', 'node_modules', 'uploads_tmp',
    '.git', 'release', 'cf.log', 'server.log', '.start.sh.lock',
    '.self-update-backup', 'package-lock.json', 'certs'
]);
const INTEGRITY_EXCLUDE_EXTENSIONS = new Set(['.log', '.patch']);

function sha256File(filePath) {
    const hash = crypto.createHash('sha256');
    hash.update(fs.readFileSync(filePath));
    return hash.digest('hex');
}

// Naglalakad sa isang direktoryo (recursive) at nagbabalik ng plain
// object { "relative/path.js": "<sha256 hex>", ... }, gamit ang
// forward-slash PALAGI sa relative path (kahit sa Windows) para
// tumugma ito sa parehong ginawa ng OMNIPOS client (mas madaling
// i-compare nang direkta, walang path-separator mismatch).
function buildFileManifest(rootDir) {
    const manifest = {};
    function walk(dir, relBase) {
        let entries;
        try {
            entries = fs.readdirSync(dir, { withFileTypes: true });
        } catch (err) {
            return;
        }
        for (const entry of entries) {
            if (INTEGRITY_EXCLUDE_NAMES.has(entry.name)) continue;
            const full = path.join(dir, entry.name);
            const rel = relBase ? `${relBase}/${entry.name}` : entry.name;
            if (entry.isDirectory()) {
                walk(full, rel);
                continue;
            }
            if (INTEGRITY_EXCLUDE_EXTENSIONS.has(path.extname(entry.name).toLowerCase())) continue;
            try {
                manifest[rel] = sha256File(full);
            } catch (err) {
                // Hindi mababasa (hal. broken symlink) — laktawan na
                // lang, hindi dapat pabagsakin ang buong build dahil dito.
            }
        }
    }
    walk(rootDir, '');
    return manifest;
}

// baseline manifests, keyed by VERSION string (hindi installationId) —
// maraming version ang naka-imbak nang sabay dahil hindi laging
// naka-update agad ang lahat ng client sa parehong version.
const RELEASE_BASELINES_PATH = path.join(__dirname, 'release-baselines.json');

async function loadReleaseBaselines() {
    const fromRedis = await redisGetJSON('release-baselines', null);
    if (fromRedis !== null) return new Map(Object.entries(fromRedis));
    try {
        return new Map(Object.entries(JSON.parse(fs.readFileSync(RELEASE_BASELINES_PATH, 'utf8'))));
    } catch (err) {
        return new Map();
    }
}

function saveReleaseBaselines(map) {
    const obj = Object.fromEntries(map);
    if (redisClient) {
        redisSetJSON('release-baselines', obj);
        return;
    }
    try {
        fs.writeFileSync(RELEASE_BASELINES_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang release-baselines.json:', err);
    }
}

let releaseBaselines = new Map(); // version -> { builtAt, fileCount, files: {relPath: sha256} } — pupunuin sa bootstrapStores()

// integrity status, keyed by installationId (resulta ng huling
// check-in ng bawat device).
const INTEGRITY_STATUS_PATH = path.join(__dirname, 'integrity-status.json');

async function loadIntegrityStatus() {
    const fromRedis = await redisGetJSON('integrity-status', null);
    if (fromRedis !== null) return fromRedis;
    try {
        return JSON.parse(fs.readFileSync(INTEGRITY_STATUS_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}

function saveIntegrityStatus(obj) {
    if (redisClient) {
        redisSetJSON('integrity-status', obj);
        return;
    }
    try {
        fs.writeFileSync(INTEGRITY_STATUS_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang integrity-status.json:', err);
    }
}

// installationId -> { checkedAt, baselineVersion, hasBaseline,
//   flagged, modified: [...], deleted: [...], added: [...],
//   modifiedCount, deletedCount, addedCount, clearedAt, clearedNote }
let integrityStatus = {}; // pupunuin sa bootstrapStores()

// Mga installationId na hiniling ng ADMIN (pindot sa "🔄 I-check
// ngayon" button) na kailangang mag-check-in AGAD, hindi na maghintay
// pa ng normal na 55s/24h schedule nito. In-memory lang ito (hindi
// kailangang mag-persist across restart — isang beses lang gagamitin,
// sasagutin ng client sa susunod na tawag nito sa
// /relay/check-feature-status, na tumatakbo na kada ~30s), at
// awtomatikong na-a-"consume"/tinatanggal pagkatapos masagot minsan.
const pendingIntegrityChecks = new Set();

// --------------------------------------------------------------
// SYSTEM VERSION — pinapatunayan ng developer/owner dito ang
// "pinakabagong" version ng OMNIPOS client app (hal. pagkatapos
// mag-merge ng bagong upgrade papunta sa upstream/main). Ang bawat
// OMNIPOS client instance (kahit saan naka-deploy — Render, Termux,
// atbp.) ay tumatawag dito (GET /relay/latest-version) para malaman
// kung may bagong version na available, at ipapakita ito bilang
// "Check for Updates" sa Settings nila.
//
// PAALALA: gaya ng ibang stores dito, mawawala ito sa susunod na
// REDEPLOY kung walang persistent disk/REDIS_URL — i-publish lang
// ulit ito sa admin panel pagkatapos.
// --------------------------------------------------------------
const SYSTEM_VERSION_PATH = path.join(__dirname, 'system-version.json');
const DEFAULT_SYSTEM_VERSION_INFO = { version: '0.0.0', changelog: '', publishedAt: null };

async function loadSystemVersionInfo() {
    const fromRedis = await redisGetJSON('system-version', null);
    if (fromRedis !== null) return fromRedis;
    try {
        return JSON.parse(fs.readFileSync(SYSTEM_VERSION_PATH, 'utf8'));
    } catch (err) {
        return { ...DEFAULT_SYSTEM_VERSION_INFO };
    }
}

function saveSystemVersionInfo(obj) {
    if (redisClient) {
        redisSetJSON('system-version', obj);
        return;
    }
    try {
        fs.writeFileSync(SYSTEM_VERSION_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang system-version.json:', err);
    }
}

let systemVersionInfo = { ...DEFAULT_SYSTEM_VERSION_INFO }; // pupunuin sa bootstrapStores()

// --------------------------------------------------------------
// TARGETED RELEASES — opsyonal na PER-DEVICE override sa itaas ng
// global na SYSTEM VERSION sa itaas. Kapag may naka-store na targeted
// entry PARA SA isang installationId, ITO ang isasagot ng
// /relay/latest-version PARA SA installationId na iyon lang — hindi
// makikita ng ibang device na "may bagong update" kahit tumaas na ang
// global version. Kung walang targeted entry, babalik sa dating gawi:
// ang global systemVersionInfo ang sasagutin sa lahat.
//
// GAMIT: staged/limited rollout (hal. isang beta customer muna bago
// i-broadcast sa lahat), o talagang isang partikular na customer/device
// lang ang dapat makatanggap ng isang partikular na patch/build.
//
// PAALALA (mahalaga): ang aktwal na zip file mismo
// (release/omnipos-client.zip, sinesserve ng /relay/release-package)
// ay IISA lang at SHARED sa lahat ng device — ang targeting dito ay
// TUNGKOL LANG SA "sinong nakikita ang 'may bagong update' notice."
// Ang x-relay-key lang (shared sa lahat ng client) ang gate ng
// /relay/release-package, HINDI installationId — kaya kung sino man
// ang mag-trigger ng self-update, ma-a-apply pa rin sa kanila ang
// parehong zip. Kung kailangan talagang i-block ang PAG-DOWNLOAD mismo
// (hindi lang ang notice), kailangan pang dagdagan ang
// /relay/release-package ng parehong installationId check — hindi pa
// ito kasama dito.
// --------------------------------------------------------------
const TARGETED_RELEASES_PATH = path.join(__dirname, 'targeted-releases.json');

async function loadTargetedReleases() {
    const fromRedis = await redisGetJSON('targeted-releases', null);
    if (fromRedis !== null) return new Map(Object.entries(fromRedis));
    try {
        const raw = fs.readFileSync(TARGETED_RELEASES_PATH, 'utf8');
        return new Map(Object.entries(JSON.parse(raw)));
    } catch (err) {
        return new Map();
    }
}

function saveTargetedReleases(map) {
    if (redisClient) {
        redisSetJSON('targeted-releases', Object.fromEntries(map));
        return;
    }
    try {
        fs.writeFileSync(TARGETED_RELEASES_PATH, JSON.stringify(Object.fromEntries(map), null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang targeted-releases.json:', err);
    }
}

let targetedReleases = new Map(); // installationId -> { version, changelog, publishedAt } — pupunuin sa bootstrapStores()

// --------------------------------------------------------------
// DOWNLOAD CODES — para sa "one-time online setup, offline pagkatapos"
// na hiling: ito ang paraan para makapag-download ang isang BAGONG
// client ng OMNIPOS package (zip) nang HINDI pinapublic ang link —
// developer/admin lang ang gumagawa ng code (may bilang ng uses at
// expiry), ipapadala sa kliyente (SMS/email/chat), tapos gagamitin
// nila ito ISANG BESES (o kung ilang beses ipinapayagan) para
// makuha ang release zip mula sa RELAY.
//
// Structure: code -> { label, usesRemaining, maxUses, createdAt,
//                        expiresAt, lastUsedAt, downloadCount }
// --------------------------------------------------------------
const DOWNLOAD_CODES_PATH = path.join(__dirname, 'download-codes.json');

async function loadDownloadCodes() {
    const fromRedis = await redisGetJSON('download-codes', null);
    if (fromRedis !== null) return new Map(Object.entries(fromRedis));
    try {
        const raw = fs.readFileSync(DOWNLOAD_CODES_PATH, 'utf8');
        return new Map(Object.entries(JSON.parse(raw)));
    } catch (err) {
        return new Map();
    }
}

function saveDownloadCodes(map) {
    if (redisClient) {
        redisSetJSON('download-codes', Object.fromEntries(map));
        return;
    }
    try {
        fs.writeFileSync(DOWNLOAD_CODES_PATH, JSON.stringify(Object.fromEntries(map), null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang download-codes.json:', err);
    }
}

let downloadCodes = new Map(); // pupunuin sa bootstrapStores()

// Kung saan naka-store ang aktwal na zip na ipapadala — i-place ito ng
// developer bawat may bagong release (tingnan ang build-release.js sa
// OMNIPOS repo). HINDI kasama sa git ang zip mismo.
const RELEASE_PACKAGE_PATH = path.join(__dirname, 'release', 'omnipos-client.zip');

// BUG FIX (corrupted self-update downloads): dating direkta sa
// RELEASE_PACKAGE_PATH mismo isinusulat ng build ang bagong zip gamit
// ang fs.createWriteStream — pero WALANG lock sa pagitan ng build
// (POST /relay/admin/api/build-release, isang write stream) at ng
// download (GET /relay/release-package o /relay/download/:code, na
// gumagamit ng res.download → fs.createReadStream sa PAREHONG path).
// Kung may kasabay na build habang may aktibong download, na-oo-overwrite
// ang file sa disk HABANG binabasa pa ito — resulta: halong lumang at
// bagong bytes sa nakuhang zip ng client (mismatched na
// central-directory offsets vs. aktwal na posisyon ng local file
// headers — "bad zipfile offset (local header sig)" sa unzip).
//
// Fix: isulat muna ang bagong build sa ISANG HIWALAY na temp filename
// sa loob mismo ng release/ folder (parehong filesystem/volume, kaya
// atomic ang susunod na rename), at saka lang i-fs.renameSync() PAPUNTA
// sa RELEASE_PACKAGE_PATH pagkatapos LANG na kumpleto/successful ang
// buong archive.finalize(). Atomic ang rename() sa parehong
// filesystem — hindi na kailanman makikita ng isang kasabay na
// GET /relay/release-package o /relay/download/:code ang "half-written"
// na file; laging BUO — luma man o bago — ang mababasa nito.
const RELEASE_PACKAGE_TMP_PATH = path.join(__dirname, 'release', '.omnipos-client.zip.building');

// --------------------------------------------------------------
// BUILD HISTORY — log lang ng mga na-build/na-publish na
// omnipos-client.zip. MAHALAGA: HINDI nito binabago ang filename ng
// aktwal na na-build na package — laging "omnipos-client.zip" pa rin
// ito sa RELEASE_PACKAGE_PATH (ino-overwrite lang kada build). Ang
// history na ito ay HIWALAY na listahan lang (caption/label na inilagay
// ng admin, ang naka-publish na version number nung oras na iyon, at
// timestamp) — copy ng "kailan/ano" nangyari, hindi ng mismong file.
// Pinaka-bago munang entry ang nasa unahan; pinuputol sa
// BUILD_HISTORY_MAX_ENTRIES para hindi lumaki nang wala sa kontrol.
// --------------------------------------------------------------
const BUILD_HISTORY_PATH = path.join(__dirname, 'build-history.json');
const BUILD_HISTORY_MAX_ENTRIES = 100;

async function loadBuildHistory() {
    const fromRedis = await redisGetJSON('build-history', null);
    if (fromRedis !== null) return Array.isArray(fromRedis) ? fromRedis : [];
    try {
        const raw = fs.readFileSync(BUILD_HISTORY_PATH, 'utf8');
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed : [];
    } catch (err) {
        return [];
    }
}

function saveBuildHistory(list) {
    if (redisClient) {
        redisSetJSON('build-history', list);
        return;
    }
    try {
        fs.writeFileSync(BUILD_HISTORY_PATH, JSON.stringify(list, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang build-history.json:', err);
    }
}

function recordBuildHistoryEntry(entry) {
    buildHistory.unshift(entry); // pinaka-bago sa unahan
    if (buildHistory.length > BUILD_HISTORY_MAX_ENTRIES) {
        buildHistory.length = BUILD_HISTORY_MAX_ENTRIES;
    }
    saveBuildHistory(buildHistory);
}

let buildHistory = []; // pupunuin sa bootstrapStores()

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
    } else if (pending.featureId === 'cloud_backup' && pending.billingCycle && CLOUD_BACKUP_BILLING_DAYS[pending.billingCycle]) {
        // Cloud Backup subscription: if the admin didn't pass an explicit
        // durationDays, use the correct number of days based on the
        // billing cycle the customer requested (30 for monthly, 365 for
        // yearly) — NOT the generic RELAY_DEFAULT_LICENSE_DAYS, since that
        // default means something different (for other features).
        pending.durationDays = CLOUD_BACKUP_BILLING_DAYS[pending.billingCycle];
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

app.get('/relay/admin/api/devices', requireAdminKey, async (req, res) => {
    const ids = [...seenDevices.keys()];
    const onlineMap = await getOnlineStatusMap(ids);

    const seen = ids.map((installationId) => {
        const meta = seenDevices.get(installationId);
        const unlockedIds = getActiveUnlockedFeatureIds(installationId).filter(id => id !== DEMO_FEATURE_ID);
        const activations = Object.values(issuedUnlocks[installationId] || {});
        const lastActivationAt = activations.length ? Math.max(...activations.map(a => a.issuedAt)) : null;
        const backupCheckin = backupCheckins[installationId] || null;
        const fingerprintRecord = deviceFingerprints.get(installationId) || null;
        const integrityRecord = integrityStatus[installationId] || null;
        return {
            installationId,
            ...meta,
            // Totoong "online ngayon" (heartbeat sa loob ng ONLINE_WINDOW_MS),
            // hiwalay sa lastSeenAt na text lang (na "huling nakita X ago"
            // kahit matagal na — lastSeenAt ay HINDI na dapat ituring na
            // "online" nang basta-basta sa admin UI).
            online: !!onlineMap[installationId],
            label: deviceLabels.get(installationId) || null,
            allowed: allowedDevices.has(installationId),
            unlockedCount: unlockedIds.length,
            totalCatalogCount: Object.keys(FEATURE_CATALOG).length,
            demoActive: getActiveUnlockedFeatureIds(installationId).includes(DEMO_FEATURE_ID),
            lastActivationAt,
            lastBackupAt: backupCheckin ? backupCheckin.lastBackupAt : null,
            backupCheckinCount: backupCheckin ? backupCheckin.checkinCount : 0,
            // ANTI-CLONE: para makita agad sa listahan kung may device na
            // naka-flag bilang posibleng clone (dalawang magkaibang
            // pisikal na makina na nag-claim ng iisang installationId).
            cloneFlagged: !!(fingerprintRecord && fingerprintRecord.flagged),
            fingerprintVerifyCount: fingerprintRecord ? fingerprintRecord.verifyCount : 0,
            // FILE INTEGRITY: red-flag view — kung may modified/deleted/
            // added file ang huling check-in ng device na ito, at hindi
            // pa ito na-clear/na-acknowledge ng admin.
            integrityFlagged: !!(integrityRecord && integrityRecord.flagged && !integrityRecord.clearedAt),
            integrityModifiedCount: integrityRecord ? integrityRecord.modifiedCount : 0,
            integrityDeletedCount: integrityRecord ? integrityRecord.deletedCount : 0,
            integrityAddedCount: integrityRecord ? integrityRecord.addedCount : 0,
            integrityCheckedAt: integrityRecord ? integrityRecord.checkedAt : null,
            // BAGONG dagdag — FIX: dati, kapag WALANG naka-publish na
            // baseline para sa version na iniulat ng device (hal.
            // bagong version pero hindi pa na-"Build Release" dito, o
            // nawala ang baseline dahil sa redeploy na walang persistent
            // disk/REDIS_URL), ang tanging paraan para malaman ito ay
            // pumunta sa "View Changed Files" modal — pero LALABAS LANG
            // ang button na iyon kapag integrityFlagged na (na hindi
            // mangyayari kailanman kung walang baseline). Ibig sabihin,
            // walang KAHIT ANONG makikita sa admin — mukhang "gumagana"
            // pero silent lang ito. Ngayon, isinasama na dito ang
            // hasBaseline/baselineVersion para may makita agad na
            // babala ang admin sa MAIN LIST mismo, hindi na kailangang
            // hintayin munang ma-flag.
            integrityHasBaseline: integrityRecord ? !!integrityRecord.hasBaseline : null,
            integrityBaselineVersion: integrityRecord ? integrityRecord.baselineVersion : null,
            // BAGONG dagdag — DIAGNOSTIC: true/false kung aktibo ba
            // talaga ang real-time fs.watch() sa device na ito noong
            // huli itong nag-check-in (null = lumang OMNIPOS build,
            // wala pang field na ito). Kung false, event-driven/instant
            // detection ang HINDI gumagana dito — 24h scheduled
            // check-in/"Check Now" na lang ang natitirang paraan.
            integrityWatcherActive: integrityRecord ? (integrityRecord.watcherActive ?? null) : null
        };
    }).sort((a, b) => b.lastSeenAt - a.lastSeenAt);

    res.json({
        success: true,
        seenDevices: seen,
        allowedDevices: [...allowedDevices],
        // BAGONG dagdag — FIX: hiwalay na deviceLabels map (installationId ->
        // label), direkta mula sa PERSISTENT na deviceLabels store (Redis/
        // device-labels.json), hindi mula sa in-memory seenDevices Map.
        // Dati, ang tanging pinagmumulan ng "label" para sa isang allowed
        // device sa admin UI ay ang kani-kanilang entry sa seenDevices — pero
        // ang seenDevices ay in-memory lang at nawawala tuwing mag-restart
        // ang server (o simpleng wala pang entry doon dahil hindi pa
        // nakaka-request ang device kailanman mula nang mag-restart). Kaya
        // ang label ng isang naka-Allow na device ay "nawawala" (o lumalabas
        // lang) tuwing offline/hindi pa online ulit ang device — kahit na
        // buo pa rin talaga ang label sa persistent storage. Sa
        // pagpapadala ng buong deviceLabels dito, laging makikita sa admin
        // ang label ng isang allowed device kahit offline ito.
        deviceLabels: Object.fromEntries(deviceLabels),
        restrictionActive: true, // laging ON simula ngayon — tinanggal na ang dating "walang laman = walang restriction" na bypass
        // Para sa notification/dot blinker sa itaas ng "Allowed devices":
        // huling successful backup check-in mula SA KAHIT ANONG device,
        // at kung naka-ON ang auto-allow-on-backup na behavior.
        lastBackupSyncAt: mostRecentBackupCheckinAt(),
        backupAutoAllowEnabled: AUTOALLOW_ON_BACKUP,
        // Bilang ng mga device na kasalukuyang naka-flag bilang clone —
        // para sa isang mabilis na "may reklamo ka bang tignan" na counter
        // sa itaas ng admin panel.
        cloneFlaggedCount: [...deviceFingerprints.values()].filter(r => r.flagged).length,
        // Bilang ng mga device na kasalukuyang naka-red-flag dahil sa
        // may nabago/nabura silang file (hindi pa na-clear ng admin).
        integrityFlaggedCount: Object.values(integrityStatus).filter(r => r.flagged && !r.clearedAt).length,
        // BAGONG dagdag — bilang ng mga device na may CHECK-IN NA pero
        // WALANG naka-imbak na baseline para sa iniulat nilang version
        // (hindi pa na-"Build Release"/na-publish, o nawala dahil sa
        // redeploy na walang persistent disk/REDIS_URL). Zero flags man
        // ang mga ito, kailangan pa ring makita ng admin dahil ang ibig
        // sabihin talaga nito ay "HINDI pa aktwal na na-che-check" ang
        // device, kahit successful ang tawag nito papunta dito.
        integrityNoBaselineCount: Object.values(integrityStatus).filter(r => r.hasBaseline === false).length
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

    // FIX: i-clear din ang naka-imbak na integrity record ng device na
    // ito sa sandaling ma-revoke ito. Kung hindi ito ginawa, maaaring
    // manatili sa listahan ang isang lumang "⚠️ NOT ACTUALLY CHECKED
    // (no baseline...)" o kahit "🛠️ MODIFIED FILES" na badge para sa
    // device na ito kahit na naalis na ito sa allowed list — mukhang
    // may kailangan pang aksyunan ang admin (o mukhang may laman pa
    // itong data) gayong hindi na naman ito active/allowed device.
    if (integrityStatus[installationId]) {
        delete integrityStatus[installationId];
        saveIntegrityStatus(integrityStatus);
    }

    logActivity(installationId, 'device_revoked', {});
    res.json({ success: true, allowedDevices: [...allowedDevices] });
});

// --------------------------------------------------------------
// POST /relay/admin/api/devices/clone-reset  (ANTI-CLONE)
// Body-based na bersyon (tugma sa parehong pattern ng /allow at /revoke
// sa itaas, para madaling tawagin ng admin panel JS). I-clear ang
// naka-bind na fingerprint (kasama ang flagged state) para sa isang
// installationId — gamitin kapag na-verify na ng developer/store owner
// na LEGIT na paglipat ito sa bagong device (hal. pinalitan ang
// unit/telepono ng customer), o kung false-positive ang clone flag.
// --------------------------------------------------------------
app.post('/relay/admin/api/devices/clone-reset', requireAdminKey, (req, res) => {
    const { installationId } = req.body;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }
    deviceFingerprints.delete(installationId);
    saveDeviceFingerprints(deviceFingerprints);
    logActivity(installationId, 'device_fingerprint_reset', {});
    res.json({ success: true, message: 'Na-clear ang fingerprint binding — kailangan na namang mag-verify online sa susunod na login.' });
});

// --------------------------------------------------------------
// POST /relay/admin/api/devices/bulk-reset  (SELECTIVE RESET)
// Katulad ng /clone-reset, pero:
//   1. Pwedeng MARAMI ang installationId sa isang tawag (checkbox-based
//      selection sa admin panel).
//   2. Bukod sa fingerprint binding, kasama na rin dito ang device
//      LABEL, ang CLONE-SPLIT record, at ang ONLINE/heartbeat status.
// HINDI NAAAPEKTUHAN: allowedDevices (hindi na-a-un-allow ang device —
// nananatili itong "allowed", linis lang ang labels/fingerprint/
// clone-split/online nito), issuedUnlocks, activityLog, backupCheckins.
// --------------------------------------------------------------
app.post('/relay/admin/api/devices/bulk-reset', requireAdminKey, async (req, res) => {
    const { installationIds } = req.body;
    if (!Array.isArray(installationIds) || installationIds.length === 0) {
        return res.status(400).json({ success: false, message: 'Kulang o mali ang installationIds (dapat non-empty array).' });
    }

    for (const id of installationIds) {
        deviceLabels.delete(id);
        deviceFingerprints.delete(id);
        cloneSplits.delete(id);
        seenDevices.delete(id); // "recently seen" entry (self-reported storeName/username, lastSeenAt, requestCount)
        logActivity(id, 'device_bulk_reset', {});
    }
    saveDeviceLabels(deviceLabels);
    saveDeviceFingerprints(deviceFingerprints);
    saveCloneSplits(cloneSplits);

    if (redisClient) {
        try {
            const onlineKeys = installationIds.map((id) => ONLINE_KEY_PREFIX + id);
            await redisClient.del(...onlineKeys);
        } catch (err) {
            console.error('⚠️  Hindi na-clear ang online heartbeat keys (bulk-reset):', err.message);
        }
    }

    res.json({
        success: true,
        resetCount: installationIds.length,
        message: `Na-reset ang ${installationIds.length} device(s): tinanggal ang label, fingerprint binding, clone-split record, at online status. Nananatili silang naka-Allow.`
    });
});

// --------------------------------------------------------------
// POST /relay/admin/api/devices/reset-all  (FULL RESET)
// I-CLEAR ang LAHAT ng device-related na estado (Redis + in-memory)
// para ibalik ang RELAY sa parang bagong-deploy — walang naka-Allow,
// walang naka-label, walang naka-bind na fingerprint, walang
// clone-split record, walang "seen"/online device, at (opsyonal, kung
// wipeUnlocksToo=true sa request body) wala rin ANUMANG naka-issue nang
// unlock/activity-log/backup-checkin.
//
// HINDI nito ginagalaw ang: FEATURE_CATALOG/UPGRADE_TIERS (config, hindi
// data), system-version, targeted-releases, download-codes, build-history
// — hindi ito "device" data, at kadalasang ayaw mo itong mabura kapag
// nag-reset ka lang ng device list.
//
// Kailangan ng ?confirm=RESET sa query string (o { confirm: "RESET" } sa
// body) bilang simpleng safety check laban sa aksidenteng pag-tawag dito.
// --------------------------------------------------------------
app.post('/relay/admin/api/devices/reset-all', requireAdminKey, async (req, res) => {
    const confirm = req.query.confirm || req.body?.confirm;
    if (confirm !== 'RESET') {
        return res.status(400).json({
            success: false,
            message: 'Safety check: kailangan ng ?confirm=RESET (o "confirm":"RESET" sa JSON body) para tuluyang i-reset ang LAHAT ng device data. Hindi na ito mababawi.'
        });
    }

    const wipeUnlocksToo = req.body?.wipeUnlocksToo === true;

    // 1. Device allowlist, labels, fingerprints, clone-splits
    allowedDevices = new Set();
    deviceLabels = new Map();
    deviceFingerprints = new Map();
    cloneSplits = new Map();
    saveAllowedDevices(allowedDevices);
    saveDeviceLabels(deviceLabels);
    saveDeviceFingerprints(deviceFingerprints);
    saveCloneSplits(cloneSplits);

    // 2. In-memory-only "seen devices" tracker (walang Redis key nito,
    //    per-installation lang ang ONLINE_KEY_PREFIX heartbeat keys).
    seenDevices.clear();

    // 3. Online heartbeat keys sa Redis (omnipos-relay:online:*)
    if (redisClient) {
        try {
            const onlineKeys = await redisClient.keys(ONLINE_KEY_PREFIX + '*');
            if (onlineKeys.length > 0) {
                await redisClient.del(...onlineKeys);
            }
        } catch (err) {
            console.error('⚠️  Hindi na-clear ang online heartbeat keys:', err.message);
        }
    }

    // 4. (Opsyonal) issued unlocks / activity log / backup check-ins —
    //    ibig sabihin, TOTAL wipe, kasama na ang lahat ng nabenta/
    //    na-unlock na feature history. Default: HINDI ginagalaw, dahil
    //    naiiba ito sa "reset device list" — pero available kung talagang
    //    gusto ng buong simula-ulit.
    if (wipeUnlocksToo) {
        issuedUnlocks = {};
        activityLog = [];
        backupCheckins = {};
        saveIssuedUnlocks(issuedUnlocks);
        saveActivityLog(activityLog);
        saveBackupCheckins(backupCheckins);
    }

    res.json({
        success: true,
        message: wipeUnlocksToo
            ? 'Buong reset: wala nang naka-Allow/naka-label/naka-fingerprint na device, wala nang online status, at wala nang issued unlocks/activity log/backup check-ins.'
            : 'Na-reset ang device list: wala nang naka-Allow/naka-label/naka-fingerprint na device at wala nang online status. Hindi ginalaw ang issued unlocks/activity log/backup check-ins (pasa "wipeUnlocksToo": true kung gusto mo ring buraan iyon).'
    });
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
        // Ang "ACTIVE" na mga bilang (active licenses, active revenue, top
        // features, expiring soon) ay dapat laging TUGMA sa "Allowed
        // devices" — ibig sabihin, isang device na (1) may naka-issue na
        // hindi pa expired na unlock, AT (2) kasalukuyang naka-Allow pa rin.
        // Kung na-revoke/na-tanggal na ang isang device sa allowlist
        // (hal. dahil sa "reset device list"), ang mga naiwan nitong
        // issuedUnlocks record ay itinuturing na ORPHAN/STALE — hindi na
        // dapat mabilang sa "active" — kahit hindi pa sila tinatanggal sa
        // ALL-TIME na kasaysayan (allTime* pa rin ang naghahawak ng
        // buong history, kasama na ang mga orphan, para sa audit trail).
        const isAllowedDevice = allowedDevices.has(installationId);

        for (const [featureId, entry] of Object.entries(record)) {
            const isExpired = typeof entry.expiresAt === 'number' && now > entry.expiresAt;

            if (featureId === DEMO_FEATURE_ID) {
                if (!isExpired && isAllowedDevice) demoActiveCount++;
                continue;
            }

            allTimeUnlocksCount++;
            allTimeRevenue += entry.price || 0;

            if (!isExpired && isAllowedDevice) {
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
// POST /relay/admin/api/analytics/reset
// "Reset Analytics" — dalawang bagay LANG ang ginagalaw nito:
//
// 1. Binubura ang buong activityLog (ang pinagmumulan ng "Activity" na
//    7d/30d na bilang ng unlocks, at ng "History" timeline sa Device
//    Detail ng BAWAT device).
//
// 2. Binubura ang mga ORPHAN/STALE na entry sa issuedUnlocks — ibig
//    sabihin, mga naka-record na "unlock" para sa isang installationId na
//    HINDI NA kasalukuyang naka-Allow (hal. dating na-unlock na device na
//    tinanggal na sa allowlist noong nag-reset ng device list, pero
//    naiwan pa rin ang lumang unlock record nito). ANG MGA ITO ANG
//    SANHI kung bakit hindi tugma ang "Active Licenses"/"Active Revenue"
//    sa aktwal na bilang ng "Allowed devices" — kaya ito ang binubura
//    dito para talagang mag-zero/mag-tama ang mga bilang.
//
// MAHALAGA: hindi kailanman ginagalaw nito ang mga unlock record ng mga
// device na KASALUKUYANG naka-Allow pa rin — ibig sabihin, ligtas ang
// mga totoong aktibong customer/naka-unlock na features nila. Gayundin,
// HINDI ginagalaw:
//   - allowedDevices / deviceLabels / deviceFingerprints (device+customer list)
//   - backupCheckins   (backup metadata ng customer)
//   - seenDevices, pendingOtps (live status)
// Kailangan ng ?confirm=RESET (o {confirm:"RESET"} sa body) bilang
// safety check, dahil hindi na ito mababawi.
// --------------------------------------------------------------
app.post('/relay/admin/api/analytics/reset', requireAdminKey, (req, res) => {
    const confirm = req.query.confirm || req.body?.confirm;
    if (confirm !== 'RESET') {
        return res.status(400).json({
            success: false,
            message: 'Safety check: kailangan ng ?confirm=RESET (o "confirm":"RESET" sa JSON body) para i-reset ang Analytics. Hindi na ito mababawi.'
        });
    }

    // 1. Activity log — buong "Activity" 7d/30d at Device History timeline.
    const clearedActivityCount = activityLog.length;
    activityLog = [];
    saveActivityLog(activityLog);

    // 2. Orphan/stale na issuedUnlocks — mga record para sa device na
    //    HINDI NA naka-Allow. Ang mga record ng KASALUKUYANG naka-Allow na
    //    device ay hindi ginagalaw (kaya buo pa rin ang totoong customer
    //    data/naka-unlock na features).
    let clearedOrphanDeviceCount = 0;
    let clearedOrphanFeatureCount = 0;
    for (const installationId of Object.keys(issuedUnlocks)) {
        if (!allowedDevices.has(installationId)) {
            clearedOrphanFeatureCount += Object.keys(issuedUnlocks[installationId]).length;
            delete issuedUnlocks[installationId];
            clearedOrphanDeviceCount++;
        }
    }
    if (clearedOrphanDeviceCount > 0) {
        saveIssuedUnlocks(issuedUnlocks);
    }

    // Bagong unang linya ng buong (bagong-simula) activity log — audit
    // trail na may nagbura ng analytics.
    logActivity(null, 'analytics_reset', {
        clearedActivityCount,
        clearedOrphanDeviceCount,
        clearedOrphanFeatureCount
    });

    res.json({
        success: true,
        clearedActivityCount,
        clearedOrphanDeviceCount,
        clearedOrphanFeatureCount,
        message: `Na-reset ang Analytics: nabura ang ${clearedActivityCount} activity-log entries, at ${clearedOrphanFeatureCount} orphan/stale na license record mula sa ${clearedOrphanDeviceCount} device na hindi na naka-Allow. Hindi ginalaw ang unlocks ng mga device na kasalukuyang naka-Allow, at hindi rin ginalaw ang device/customer database (allowed devices, labels, backup check-ins).`
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
// POST /relay/admin/api/devices/:installationId/clear-history
// Binubura LANG ang mga activity-log entries NG ISANG partikular na
// device (hal. "otp_requested", "unlock_issued", "restore_checkin",
// atbp. — ang pina-pakita sa "History" timeline ng Device Detail
// page). LOG LANG ito, kapareho ng /build-history/clear — WALANG
// epekto sa aktwal na estado ng device (allowed/unlocked/fingerprint/
// label/atbp. ay nananatiling buo, hindi ito "reset" ng device).
// Ang mga entry ng IBANG device sa activityLog ay hindi nagagalaw.
// --------------------------------------------------------------
app.post('/relay/admin/api/devices/:installationId/clear-history', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }

    const before = activityLog.length;
    activityLog = activityLog.filter(entry => entry.installationId !== installationId);
    const clearedCount = before - activityLog.length;
    saveActivityLog(activityLog);

    // Itong entry mismo ang magiging bagong unang linya ng history ng
    // device na ito — parang "audit trail" na may nagbura dito.
    logActivity(installationId, 'device_history_cleared', { clearedCount });

    res.json({ success: true, clearedCount });
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

    // Kung tierId ang ginamit, gamitin ang bundlePrice ng tier (na may
    // discount) sa halip na ang buong à la carte na presyo ng bawat
    // feature — kaya tumutugma rin dito ang na-record na revenue sa
    // aktwal na presyo ng bundle/tier na ito (parehong logic ng
    // ginagamit sa /relay/confirm-unlock-bulk para sa customer-facing
    // na bundle purchases). Kung featureId/featureIds (à la carte,
    // hindi tier) ang ginamit, mananatili ang dating gawi — ang buong
    // à la carte price ng bawat isa.
    let perFeaturePrice = {};
    if (tierId) {
        const tier = UPGRADE_TIERS.find(t => t.id === tierId);
        const alaCartePrices = idsToActivate.map(id => (FEATURE_CATALOG[id] && FEATURE_CATALOG[id].price) || 0);
        const alaCarteTotal = alaCartePrices.reduce((s, p) => s + p, 0);

        if (alaCarteTotal > 0) {
            let allocated = 0;
            idsToActivate.forEach((id, i) => {
                const share = Math.floor((tier.bundlePrice * alaCartePrices[i]) / alaCarteTotal);
                perFeaturePrice[id] = share;
                allocated += share;
            });
            const remainder = Math.round(tier.bundlePrice) - allocated;
            if (remainder !== 0 && idsToActivate.length > 0) {
                const priciestIdx = alaCartePrices.indexOf(Math.max(...alaCartePrices));
                perFeaturePrice[idsToActivate[priciestIdx]] += remainder;
            }
        }
    }

    for (const id of idsToActivate) {
        const token = issueSignedToken(installationId, id, durationMs);
        tokens[id] = token;
        const priceForThisFeature = Object.prototype.hasOwnProperty.call(perFeaturePrice, id)
            ? perFeaturePrice[id]
            : FEATURE_CATALOG[id].price;
        recordIssuedUnlock(installationId, id, token, {
            featureName: FEATURE_CATALOG[id].name,
            price: priceForThisFeature,
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

    // Kung may pending na "🔄 I-check ngayon" request ang admin para sa
    // device na ito (integrity check-now), isama ito dito bilang
    // forceIntegrityCheck:true — dito rin ito "kina-consume"/tinatanggal
    // (isang beses lang) para hindi na paulit-ulit i-force sa susunod
    // pang mga check-feature-status call.
    let forceIntegrityCheck = false;
    if (pendingIntegrityChecks.has(installationId)) {
        forceIntegrityCheck = true;
        pendingIntegrityChecks.delete(installationId);
    }

    res.json({ success: true, statuses, forceIntegrityCheck });
});

// --------------------------------------------------------------
// POST /relay/pending-integrity-check
// BAGONG dagdag — LAYUNIN: pabilisin ang "🔄 I-check ngayon" (Check
// Now) na pindot ng admin sa integrity monitor. Dati, ang tanging
// paraan para maabot ng OMNIPOS client ang forceIntegrityCheck flag
// ay ang /relay/check-feature-status, na tumatakbo lang kada ~30s
// (RELAY_FEATURE_SYNC_INTERVAL_MS sa client) — ibig sabihin, pwedeng
// umabot ng malapit 30 segundo bago talaga ma-trigger ang check-in
// pagkatapos pindutin ng admin ang "Check Now".
//
// Ito ay ISANG MABILIS, MAGAAN na endpoint na LAMANG — isang Set
// lookup (walang manifest, walang file hashing, walang
// logActivity spam) — para SEPARADO itong ma-poll ng client sa MAS
// MADALAS na pagitan (ilang segundo lang) nang hindi kinakailangang
// baguhin ang normal na 30s na cadence ng check-feature-status
// (na siyang humahawak pa rin ng feature lockdown/restore, hindi ito
// pinapalitan). Kapag "may pending" (true) ang sagot dito, agad na
// tinatawag ng client ang buong runRelayIntegrityCheckin() (kasama
// ang buong manifest) — doon pa rin talaga nangyayari ang aktwal na
// paghahambing sa baseline.
// Body: { installationId }
// --------------------------------------------------------------
app.post('/relay/pending-integrity-check', requireApiKey, requireAllowedDevice, rateLimit('pending-integrity-check', 150, 5 * 60 * 1000, (req) => req.body?.installationId), (req, res) => {
    const { installationId } = req.body;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }
    let pending = false;
    if (pendingIntegrityChecks.has(installationId)) {
        pending = true;
        pendingIntegrityChecks.delete(installationId);
    }
    res.json({ success: true, pending });
});

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
// issueDevicePermit — ANTI-CLONE, PERMIT SYSTEM
// Kapareho ng ideya ng issueSignedToken (parehong PRIVATE KEY na
// RELAY lang ang may hawak), pero para dito: pinapatunayan nito na
// "TALAGANG si RELAY (ang developer) ang nag-approve na dumapo ang
// installationId na ito sa fingerprint na ito" — hindi lang basta
// isang lokal na boolean flag (deviceVerified=true) na naka-imbak sa
// DB ng OMNIPOS client, na kung sakaling direktang i-edit ng isang
// user ang database row (o i-restore mula sa kinopyang backup), MADALI
// lang i-fake ang isang boolean pero HINDI kailanman mapeke ang
// signature na ito dahil wala silang private key ng RELAY.
//
// Ito ang "permit" na sinusuri ng OMNIPOS client sa BAWAT startup/login
// gamit lang ang RELAY_PUBLIC_KEY nito (kaya gumagana ito OFFLINE) —
// online lang kailangan kapag kailangan ng BAGONG permit (unang beses,
// o nagbago ang fingerprint/hardware).
// --------------------------------------------------------------
function issueDevicePermit(installationId, fingerprint) {
    const payload = { installationId, fingerprint, issuedAt: Date.now() };
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

// IMPROVEMENT: dati, hindi na-clean-up ang mga entry ng rateBuckets
// kapag naubos na ang lahat ng timestamps nito (na-filter na lahat ng
// "recent" pero nananatili pa rin ang EMPTY array sa Map) — kaya kada
// bagong unique key (bagong IP, bagong installationId, atbp.) ay
// PERMANENTENG entry sa memory kahit hindi na ito ginagamit. Sa
// mahabang uptime na may maraming iba't ibang devices/IPs, unbounded
// growth ito. Nililinis ito paminsan-minsan (tuwing 30 min) — tinatanggal
// ang mga key na walang laman/luma nang timestamps.
const RATE_BUCKET_CLEANUP_INTERVAL_MS = 30 * 60 * 1000;
const RATE_BUCKET_MAX_AGE_MS = 60 * 60 * 1000; // kahit anong window, 1 oras nang hindi ginagalaw = pwede nang tanggalin
setInterval(() => {
    const now = Date.now();
    for (const [key, timestamps] of rateBuckets) {
        const recent = timestamps.filter((ts) => now - ts < RATE_BUCKET_MAX_AGE_MS);
        if (recent.length === 0) {
            rateBuckets.delete(key);
        } else if (recent.length !== timestamps.length) {
            rateBuckets.set(key, recent);
        }
    }
}, RATE_BUCKET_CLEANUP_INTERVAL_MS).unref();

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
// POST /relay/integrity-checkin  (FILE INTEGRITY MONITORING)
// Tinatawag ito ng OMNIPOS CLIENT SERVER (pana-panahon, awtomatiko —
// tingnan ang runRelayIntegrityCheckin() sa OMNIPOS/server.js) kasama
// ang: installationId, version (APP_VERSION nito ngayon), at files
// (isang { "relative/path.js": "<sha256>" } manifest ng SARILI nitong
// install folder). Dito, kino-compare ito sa naka-imbak na baseline
// PARA SA VERSION NA IYON (galing sa huling matagumpay na build para
// doon) — kung wala pang baseline (hal. hindi pa naka-publish/naka-
// build dito ang version na iyon), hindi ito ma-flag, sasabihing
// hasBaseline:false na lang sa resulta.
// --------------------------------------------------------------
// LIMIT: 12/hour dati (sapat noon dahil once-a-day lang ang tawag).
// Ngayon may real-time watcher na sa OMNIPOS client (event-driven, min
// 30s gap sa pagitan ng checkin — tingnan ang REAL-TIME INTEGRITY
// WATCHER sa OMNIPOS/server.js), kaya dinagdagan dito ang limit
// (90/hour = 1 kada ~40s pinaka-mabilis) para hindi ma-throttle ang
// mga lehitimong agad na check-in kapag may nabagong file, pero
// nananatiling may proteksyon pa rin laban sa abuse/spam.
// LIMIT: dati 12/hour (once-a-day lang ang tawag), tapos 90/hour (30s
// client throttle). Ngayon 5s na ang client-side throttle (tingnan ang
// OMNIPOS/server.js), kaya dinagdagan pa ito (300/hour = 1 kada ~12s
// average) — sapat na headroom para sa totoong burst ng magkakasunod
// na file event, pero may bound pa rin laban sa abuse.
app.post('/relay/integrity-checkin', requireApiKey, requireAllowedDevice, rateLimit('integrity-checkin', 300, 60 * 60 * 1000, (req) => req.body?.installationId), (req, res) => {
    const { installationId, version, files, watcherActive } = req.body || {};
    // DIAGNOSTIC: true/false galing mismo sa client kung aktibo talaga
    // ang real-time fs.watch() nito ngayong check-in na ito — undefined
    // kung mula pa sa lumang OMNIPOS build (bago idagdag ang field na
    // ito), kaya hindi natin ito basta-basta ituturing na "false".
    const watcherActiveFlag = typeof watcherActive === 'boolean' ? watcherActive : null;

    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }
    if (!files || typeof files !== 'object' || Array.isArray(files)) {
        return res.status(400).json({ success: false, message: 'Kulang o mali ang format ng "files" (dapat object na { relPath: sha256 }).' });
    }

    recordDeviceSeen(installationId, {});

    const baselineVersion = String(version || '').trim() || null;
    const baseline = baselineVersion ? releaseBaselines.get(baselineVersion) : null;

    if (!baseline) {
        // Walang baseline na maihahambing (bago pang i-publish/i-build
        // ang version na ito dito, o hindi kilalang version) — itago pa
        // rin ang huling nakita, pero HUWAG i-flag (iiwasan ang maling
        // positibo).
        integrityStatus[installationId] = {
            checkedAt: Date.now(),
            baselineVersion,
            hasBaseline: false,
            flagged: false,
            modified: [],
            deleted: [],
            added: [],
            modifiedCount: 0,
            deletedCount: 0,
            addedCount: 0,
            watcherActive: watcherActiveFlag,
            clearedAt: integrityStatus[installationId] ? integrityStatus[installationId].clearedAt || null : null,
            clearedNote: integrityStatus[installationId] ? integrityStatus[installationId].clearedNote || null : null
        };
        saveIntegrityStatus(integrityStatus);
        return res.json({ success: true, hasBaseline: false, flagged: false, message: `Walang naka-imbak na baseline para sa version "${baselineVersion}" — hindi muna ito na-compare.` });
    }

    const baselineFiles = baseline.files || {};
    const modified = [];
    const deleted = [];
    const added = [];

    for (const [relPath, baseHash] of Object.entries(baselineFiles)) {
        const clientHash = files[relPath];
        if (clientHash === undefined) {
            deleted.push(relPath);
        } else if (clientHash !== baseHash) {
            modified.push(relPath);
        }
    }
    for (const relPath of Object.keys(files)) {
        if (!(relPath in baselineFiles)) added.push(relPath);
    }

    modified.sort();
    deleted.sort();
    added.sort();

    const flagged = modified.length > 0 || deleted.length > 0 || added.length > 0;

    integrityStatus[installationId] = {
        checkedAt: Date.now(),
        baselineVersion,
        hasBaseline: true,
        flagged,
        modified,
        deleted,
        added,
        modifiedCount: modified.length,
        deletedCount: deleted.length,
        addedCount: added.length,
        watcherActive: watcherActiveFlag,
        // Nananatili ang dating clearedAt/clearedNote (kung meron)
        // hangga't hindi bago-bagong-flag ito — sa susunod na
        // matagumpay/malinis na check-in, dito rin ito ire-reset sa
        // ibaba (bagong "clean" state).
        clearedAt: flagged ? (integrityStatus[installationId] ? integrityStatus[installationId].clearedAt || null : null) : null,
        clearedNote: flagged ? (integrityStatus[installationId] ? integrityStatus[installationId].clearedNote || null : null) : null
    };
    saveIntegrityStatus(integrityStatus);

    if (flagged) {
        logActivity(installationId, 'integrity_alert', {
            baselineVersion,
            modifiedCount: modified.length,
            deletedCount: deleted.length,
            addedCount: added.length
        });
    }

    res.json({
        success: true,
        hasBaseline: true,
        flagged,
        modifiedCount: modified.length,
        deletedCount: deleted.length,
        addedCount: added.length
    });
});

// --------------------------------------------------------------
// POST /relay/branch-checkin
// Tinatawag ito ng OMNIPOS CLIENT SERVER (pana-panahon, tingnan ang
// runRelayBranchCheckin() sa OMNIPOS/server.js) — PERO lang kung
// naka-configure ang isang "Business Group Code" sa Store & Sales
// Settings ng device na iyon (kung wala, hindi ito tinatawag). Ini-
// imbak lang dito ang PINAKA-BAGONG snapshot (overwrite, hindi
// history/log) ng bawat installationId sa ilalim ng groupKeyHash nito.
//
// Kasama rin dito ang paglilinis: kung dating naka-imbak na ang
// installationId na ito sa ILANG IBANG groupKeyHash (hal. binago ng
// merchant ang Business Group Code), tinatanggal muna ito doon bago
// isulat sa bago — para hindi na magpakita ang "ghost" branch entry sa
// lumang group.
// --------------------------------------------------------------
app.post('/relay/branch-checkin', requireApiKey, requireAllowedDevice, rateLimit('branch-checkin', 40, 60 * 60 * 1000, (req) => req.body?.installationId), (req, res) => {
    const { installationId, branchGroupKeyHash, branchName, summary } = req.body || {};

    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }
    if (!branchGroupKeyHash || !BRANCH_GROUP_HASH_RE.test(String(branchGroupKeyHash))) {
        return res.status(400).json({ success: false, message: 'Kulang o mali ang format ng branchGroupKeyHash (dapat SHA-256 hex).' });
    }

    const cleanName = String(branchName || '').trim().slice(0, BRANCH_NAME_MAX_LEN) || 'Unnamed Branch';
    const cleanSummary = sanitizeBranchSummaryPayload(summary);

    // Alisin muna ang installationId na ito sa ANUMANG ibang group (baka
    // nagpalit ng code ang merchant) bago isulat sa bago.
    let changed = false;
    for (const hash of Object.keys(branchSummaries)) {
        if (hash === branchGroupKeyHash) continue;
        if (branchSummaries[hash] && branchSummaries[hash][installationId]) {
            delete branchSummaries[hash][installationId];
            if (Object.keys(branchSummaries[hash]).length === 0) delete branchSummaries[hash];
            changed = true;
        }
    }

    if (!branchSummaries[branchGroupKeyHash]) branchSummaries[branchGroupKeyHash] = {};
    branchSummaries[branchGroupKeyHash][installationId] = {
        branchName: cleanName,
        summary: cleanSummary,
        updatedAt: Date.now()
    };
    saveBranchSummaries(branchSummaries);
    if (changed) { /* naka-save na sa itaas kasama ng bagong entry */ }

    res.json({ success: true, message: 'Branch check-in recorded.' });
});

// --------------------------------------------------------------
// GET /relay/branch-summary?groupKeyHash=<sha256 hex>
// Tinatawag ito ng OMNIPOS CLIENT SERVER (proxy lang — ang browser ng
// merchant ay hindi direktang tumatawag dito) kapag binubuksan ang
// "All Branches" na widget. Ibinabalik ang LAHAT ng branch entry sa
// ilalim ng groupKeyHash na ibinigay — kaya SINUMANG makakakuha ng
// tamang hash (nangangahulugang alam nila ang plain Business Group
// Code, o na-guess/na-brute-force nila ang hash) ay makikita ang
// combined data ng group na iyon. Dalawang layer pa rin ng proteksyon
// bago maabot ito: (1) requireApiKey — shared secret na tanging mga
// legit na OMNIPOS client build lang ang may hawak, (2)
// requireAllowedDevice — dapat naka-Allow na sa Relay ang installationId
// mismo ng humihiling. Kaya HINDI ito basta bukas sa publiko sa
// internet — pero hindi rin ito full end-to-end secret matching kaya
// hindi dapat gamitin ang parehong code na ginagamit din bilang totoong
// password kahit saan.
// --------------------------------------------------------------
app.get('/relay/branch-summary', requireApiKey, requireAllowedDevice, rateLimit('branch-summary', 120, 60 * 60 * 1000, (req) => req.query?.installationId), (req, res) => {
    const groupKeyHash = String(req.query.groupKeyHash || '');
    if (!BRANCH_GROUP_HASH_RE.test(groupKeyHash)) {
        return res.status(400).json({ success: false, message: 'Kulang o mali ang format ng groupKeyHash query param.' });
    }

    const group = branchSummaries[groupKeyHash] || {};
    const branches = Object.entries(group).map(([installationId, entry]) => ({
        installationId,
        branchName: entry.branchName,
        summary: entry.summary,
        updatedAt: entry.updatedAt
    })).sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));

    const combined = branches.reduce((acc, b) => {
        for (const field of BRANCH_SUMMARY_NUMERIC_FIELDS) {
            acc[field] = (acc[field] || 0) + (Number(b.summary && b.summary[field]) || 0);
        }
        return acc;
    }, {});

    res.json({ success: true, branchCount: branches.length, branches, combined });
});

// --------------------------------------------------------------
// isFeatureCurrentlyUnlocked(installationId, featureId) — GROUND-TRUTH
// na pagsusuri (kaparehong lohika ng /relay/check-feature-status sa
// itaas) kung talagang naka-unlock ang isang feature PARA sa
// installationId na ito NGAYON (na isinasaalang-alang ang expiry).
// Ginagamit ito ng cloud-backup upload endpoint sa ibaba bilang
// SERVER-SIDE gate — hindi ito basta umaasa sa sinasabi ng client.
// --------------------------------------------------------------
function isFeatureCurrentlyUnlocked(installationId, featureId) {
    const entry = (issuedUnlocks[installationId] || {})[featureId];
    if (!entry) return false;
    if (typeof entry.expiresAt === 'number' && Date.now() > entry.expiresAt) return false;
    return true;
}

// --------------------------------------------------------------
// POST /relay/cloud-backup/upload
// Tinatawag ito ng OMNIPOS CLIENT SERVER (manual — pinindot ng customer
// ang "Cloud Backup" button) para i-sync ang BUONG database nito
// (maliban sa user accounts — hinihigpitan din ito DITO, hindi lang
// umaasa sa client) papunta sa Postgres. TINATANGGIHAN ito (402) kung
// HINDI pa naka-unlock ang 'cloud_backup' feature para sa
// installationId na ito — kahit anong ipadala ng client, walang
// maisusulat sa Postgres hangga't hindi ito na-verify dito.
//
// UPDATE: dating buong-module ang laging tinatanggihan dito ("users",
// "featureUnlocks") — ngayon, sadyang GUSTO NA ring i-backup ang mga
// ito (user accounts, unlocked features/themes). Ang "users" module
// mismo ay dapat nang dumating dito na WALANG "password" field —
// ginagawa ito ng OMNIPOS client bago pa ito ipadala (tingnan ang
// stripRedactedFields()/REDACTED_FIELDS_BY_MODULE sa db.js doon). Dito,
// defense-in-depth pa rin: kahit sumingit ang isang password field sa
// bawat record ng "users" (hal. luma/binagong client), tinatanggal pa
// rin ito dito bago isulat sa Postgres.
// --------------------------------------------------------------
const CLOUD_BACKUP_REDACTED_FIELDS_BY_MODULE = { users: ['password'] };

function stripCloudBackupRedactedFields(moduleName, data) {
    const redactedFields = CLOUD_BACKUP_REDACTED_FIELDS_BY_MODULE[moduleName];
    if (!redactedFields || !Array.isArray(data)) return data;
    return data.map((record) => {
        if (!record || typeof record !== 'object') return record;
        const clone = { ...record };
        redactedFields.forEach((field) => { delete clone[field]; });
        return clone;
    });
}

app.post('/relay/cloud-backup/upload', requireApiKey, requireAllowedDevice, rateLimit('cloud-backup-upload', 12, 60 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
    const { installationId, storeName, modules, moduleNames, totalRecords } = req.body;

    if (!installationId || !modules || typeof modules !== 'object') {
        return res.status(400).json({ success: false, message: 'Missing or invalid installationId/modules.' });
    }

    if (!isFeatureCurrentlyUnlocked(installationId, 'cloud_backup')) {
        logActivity(installationId, 'cloud_backup_blocked', { reason: 'feature_not_unlocked' });
        return res.status(402).json({
            success: false,
            featureLocked: true,
            featureId: 'cloud_backup',
            featureName: FEATURE_CATALOG.cloud_backup.name,
            isSubscription: true,
            plans: CLOUD_BACKUP_PLANS,
            message: 'There is no active/it has expired for the Cloud Backup subscription for this installation. You must subscribe (or renew) first before using Cloud Backup.'
        });
    }

    if (!pgPool) {
        return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured yet on RELAY. Tell the developer to set it.' });
    }

    // Alamin muna ang tier/quota ng installation na ito BAGO isulat
    // kahit ano sa Postgres — kailangan ito para malaman kung sapat pa
    // ang natitirang allowance bago tanggapin ang buong upload.
    const cloudBackupUnlockForQuota = (issuedUnlocks[installationId] || {})['cloud_backup'];
    const tierForQuota = (cloudBackupUnlockForQuota && cloudBackupUnlockForQuota.tier && CLOUD_BACKUP_PLANS[cloudBackupUnlockForQuota.tier]) ? cloudBackupUnlockForQuota.tier : 'basic';
    const quotaMBForQuota = CLOUD_BACKUP_PLANS[tierForQuota].storageQuotaMB;

    // I-serialize muna ang LAHAT ng module (hindi pa isinusulat) para
    // masukat ang TOTAL na byte size bago pa magsimula ng anumang
    // Postgres write — kaya kung lalagpas sa quota, WALANG NAISULAT na
    // kahit ano (hindi partial write), tanggihan lang agad ito nang
    // buo.
    const serializedModules = {};
    let projectedSizeBytes = 0;
    for (const [moduleName, rawData] of Object.entries(modules)) {
        const data = stripCloudBackupRedactedFields(moduleName, rawData);
        const serialized = JSON.stringify(data);
        serializedModules[moduleName] = { data, serialized, recordCount: Array.isArray(data) ? data.length : 0 };
        projectedSizeBytes += Buffer.byteLength(serialized, 'utf8');
    }
    const projectedSizeMB = Math.round((projectedSizeBytes / (1024 * 1024)) * 100) / 100;

    if (projectedSizeMB > quotaMBForQuota) {
        logActivity(installationId, 'cloud_backup_blocked', { reason: 'storage_quota_exceeded', tier: tierForQuota, quotaMB: quotaMBForQuota, sizeMB: projectedSizeMB });
        return res.status(413).json({
            success: false,
            storageQuotaExceeded: true,
            tier: tierForQuota,
            quotaMB: quotaMBForQuota,
            sizeMB: projectedSizeMB,
            overageMB: Math.round((projectedSizeMB - quotaMBForQuota) * 100) / 100,
            message: `Cloud backup exceeds your ${CLOUD_BACKUP_PLANS[tierForQuota].name} storage allowance (${projectedSizeMB} MB used, ${quotaMBForQuota} MB limit). Upgrade your Cloud Backup plan or free up space (e.g., trim old transaction/userlog history) before syncing.`
        });
    }

    try {
        let totalSizeBytes = 0;
        let moduleCount = 0;

        await runCloudBackupWrite(async (client) => {
            // I-reset ang mga counter na ito sa SIMULA ng bawat pagsubok
            // (attempt) — kung ito ay isang RETRY (dahil sa transient
            // connection error), gusto nating simulan ulit mula sa zero
            // ang totalSizeBytes/moduleCount ng bagong attempt sa halip
            // na dagdagan pa ang natitirang value mula sa nabigong
            // attempt bago rito.
            totalSizeBytes = 0;
            moduleCount = 0;
            for (const [moduleName, entry] of Object.entries(serializedModules)) {
                // "Na-consume" na storage: sinusukat sa aktwal na byte size
                // ng na-store na JSON bawat module (Buffer.byteLength dahil
                // multi-byte ang UTF-8 characters gaya ng ₱/ñ), tapos
                // sinusuma ito para sa TOTAL ng buong installation sa ibaba.
                const sizeBytes = Buffer.byteLength(entry.serialized, 'utf8');
                totalSizeBytes += sizeBytes;
                await client.query(
                    `INSERT INTO cloud_backup_modules (installation_id, module, data, record_count, size_bytes, updated_at)
                     VALUES ($1, $2, $3, $4, $5, now())
                     ON CONFLICT (installation_id, module) DO UPDATE SET
                        data = excluded.data, record_count = excluded.record_count, size_bytes = excluded.size_bytes, updated_at = excluded.updated_at`,
                    [installationId, moduleName, entry.serialized, entry.recordCount, sizeBytes]
                );
                moduleCount++;
            }

            await client.query(
                `INSERT INTO cloud_backup_meta (installation_id, store_name, total_records, module_count, size_bytes, last_sync_at, sync_count)
                 VALUES ($1, $2, $3, $4, $5, now(), 1)
                 ON CONFLICT (installation_id) DO UPDATE SET
                    store_name = excluded.store_name,
                    total_records = excluded.total_records,
                    module_count = excluded.module_count,
                    size_bytes = excluded.size_bytes,
                    last_sync_at = now(),
                    sync_count = cloud_backup_meta.sync_count + 1`,
                [installationId, storeName || null, typeof totalRecords === 'number' ? totalRecords : null, moduleCount, totalSizeBytes]
            );
        });

        logActivity(installationId, 'cloud_backup_sync', { moduleCount: Object.keys(modules).length, totalRecords: totalRecords || null, storeName: storeName || null, sizeBytes: totalSizeBytes });

        const tierForResponse = tierForQuota;
        const quotaMBForResponse = quotaMBForQuota;
        const sizeMBForResponse = Math.round((totalSizeBytes / (1024 * 1024)) * 100) / 100;
        const percentUsedForResponse = quotaMBForResponse > 0 ? Math.round(Math.min(100, (sizeMBForResponse / quotaMBForResponse) * 100) * 10) / 10 : 0;
        // Malapit na sa quota (>=90%) pero HINDI pa lumagpas — pinapayagan
        // pa rin ang sync (hindi ito ang 413 na block sa itaas), pero
        // idinadagdag itong warning flag para maka-heads-up na ang client
        // BAGO pa lumagpas at ma-block sa susunod na sync.
        const nearQuota = percentUsedForResponse >= 90;

        res.json({
            success: true,
            message: 'Cloud backup successfully saved to Postgres.',
            moduleNames: moduleNames || Object.keys(modules),
            sizeBytes: totalSizeBytes,
            sizeMB: sizeMBForResponse,
            tier: tierForResponse,
            quotaMB: quotaMBForResponse,
            percentUsed: percentUsedForResponse,
            nearQuota
        });
    } catch (err) {
        console.error('⚠️ CLOUD_BACKUP: hindi na-save sa Postgres:', err.message);
        const friendlyMessage = isTransientPgConnectionError(err)
            ? 'Lost connection to the database while saving the cloud backup (this can happen with very large uploads). Please try syncing again — no partial data was saved.'
            : ('An error occurred while saving to Postgres: ' + err.message);
        res.status(500).json({ success: false, message: friendlyMessage });
    }
});

// --------------------------------------------------------------
// GET /relay/cloud-backup/usage
// Tinatawag ito ng OMNIPOS CLIENT SERVER (tuwing binubuksan/ini-refresh
// ang Cloud Backup section sa My Store) para malaman kung ilang MB/GB
// na ang NA-CONSUME nitong installation sa Postgres storage — hindi
// lang pagkatapos ng sync (`/relay/cloud-backup/upload` na response
// ay mayroon na ring sizeBytes/sizeMB, pero hindi tinatawag yun sa
// tuwing binubuksan lang ang page, kaya ito ang mabilis/read-only na
// bersyon para doon).
// --------------------------------------------------------------
app.get('/relay/cloud-backup/usage', requireApiKey, requireAllowedDevice, rateLimit('cloud-backup-usage', 60, 60 * 60 * 1000, (req) => req.query?.installationId), async (req, res) => {
    const installationId = String(req.query.installationId || '').trim();
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }
    if (!pgPool) {
        return res.status(503).json({ success: false, message: 'Hindi pa naka-configure ang Postgres (DATABASE_URL).' });
    }

    // Tignan kung anong tier ang aktibo para malaman ang storageQuotaMB
    // na dapat ipalabas kasama ng usage — kung walang naka-record na
    // tier (hal. legacy one-time buyer bago pa idagdag ang tier
    // metadata), i-default sa Basic bilang pinaka-ligtas na palagay.
    const cloudBackupUnlock = (issuedUnlocks[installationId] || {})['cloud_backup'];
    const tier = (cloudBackupUnlock && cloudBackupUnlock.tier && CLOUD_BACKUP_PLANS[cloudBackupUnlock.tier]) ? cloudBackupUnlock.tier : 'basic';
    const quotaMB = CLOUD_BACKUP_PLANS[tier].storageQuotaMB;

    try {
        const result = await queryWithRetry(
            'SELECT total_records, module_count, size_bytes, last_sync_at, sync_count FROM cloud_backup_meta WHERE installation_id = $1',
            [installationId]
        );
        if (!result.rows[0]) {
            return res.json({ success: true, hasBackup: false, sizeBytes: 0, sizeMB: 0, tier, quotaMB, percentUsed: 0 });
        }
        const row = result.rows[0];
        const sizeBytes = Number(row.size_bytes) || 0;
        const sizeMB = Math.round((sizeBytes / (1024 * 1024)) * 100) / 100;
        const percentUsed = quotaMB > 0 ? Math.round(Math.min(100, (sizeMB / quotaMB) * 100) * 10) / 10 : 0;
        res.json({
            success: true,
            hasBackup: true,
            sizeBytes,
            sizeMB,
            sizeGB: Math.round((sizeBytes / (1024 * 1024 * 1024)) * 1000) / 1000,
            tier,
            quotaMB,
            percentUsed,
            nearQuota: percentUsed >= 90,
            totalRecords: row.total_records,
            moduleCount: row.module_count,
            lastSyncAt: row.last_sync_at,
            syncCount: row.sync_count
        });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// --------------------------------------------------------------
// GET /relay/admin/api/cloud-backup — listahan ng LAHAT ng
// installations na may cloud backup data (para sa admin panel table).
// GET /relay/admin/api/cloud-backup/:installationId — buong laman
// (lahat ng modules) ng cloud backup ng isang partikular na
// installationId — ito ang "sa storage nila ito babasahin" na binanggit
// sa instructions (developer/admin lang ang may access dito, protektado
// ng requireAdminKey).
// --------------------------------------------------------------
app.get('/relay/admin/api/cloud-backup', requireAdminKey, async (req, res) => {
    if (!pgPool) return res.status(503).json({ success: false, message: 'Hindi pa naka-configure ang Postgres (DATABASE_URL).' });
    try {
        const result = await queryWithRetry('SELECT installation_id, store_name, total_records, module_count, size_bytes, last_sync_at, sync_count FROM cloud_backup_meta ORDER BY last_sync_at DESC NULLS LAST');
        const backups = result.rows.map((r) => ({
            ...r,
            size_mb: Math.round((Number(r.size_bytes || 0) / (1024 * 1024)) * 100) / 100
        }));
        res.json({ success: true, backups });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

app.get('/relay/admin/api/cloud-backup/:installationId', requireAdminKey, async (req, res) => {
    if (!pgPool) return res.status(503).json({ success: false, message: 'Hindi pa naka-configure ang Postgres (DATABASE_URL).' });
    try {
        const { installationId } = req.params;
        const metaResult = await queryWithRetry('SELECT * FROM cloud_backup_meta WHERE installation_id = $1', [installationId]);
        const modulesResult = await queryWithRetry('SELECT module, data, record_count, size_bytes, updated_at FROM cloud_backup_modules WHERE installation_id = $1 ORDER BY module', [installationId]);
        if (!metaResult.rows[0]) {
            return res.status(404).json({ success: false, message: 'Walang cloud backup na nakita para sa installationId na ito.' });
        }
        const sizeBytesForMeta = Number(metaResult.rows[0].size_bytes || 0);
        const sizeMBForMeta = Math.round((sizeBytesForMeta / (1024 * 1024)) * 100) / 100;

        // Same tier lookup logic as /relay/cloud-backup/usage — kailangan
        // dito ang tier para malaman ang quotaMB na ipapakita sa admin
        // panel na progress bar (Basic/Standard/Pro), default sa Basic
        // kung walang naka-record na tier.
        const cloudBackupUnlockForAdmin = (issuedUnlocks[installationId] || {})['cloud_backup'];
        const tierForAdmin = (cloudBackupUnlockForAdmin && cloudBackupUnlockForAdmin.tier && CLOUD_BACKUP_PLANS[cloudBackupUnlockForAdmin.tier]) ? cloudBackupUnlockForAdmin.tier : 'basic';
        const quotaMBForAdmin = CLOUD_BACKUP_PLANS[tierForAdmin].storageQuotaMB;

        const meta = {
            ...metaResult.rows[0],
            size_mb: sizeMBForMeta,
            tier: tierForAdmin,
            quota_mb: quotaMBForAdmin,
            percent_used: quotaMBForAdmin > 0 ? Math.round(Math.min(100, (sizeMBForMeta / quotaMBForAdmin) * 100) * 10) / 10 : 0
        };
        const modules = modulesResult.rows.map((m) => ({
            ...m,
            size_mb: Math.round((Number(m.size_bytes || 0) / (1024 * 1024)) * 100) / 100
        }));
        res.json({ success: true, meta, modules });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

app.get('/relay/admin/api/cloud-backup/:installationId/download', requireAdminKey, async (req, res) => {
    if (!pgPool) return res.status(503).json({ success: false, message: 'Hindi pa naka-configure ang Postgres (DATABASE_URL).' });
    try {
        const { installationId } = req.params;
        const modulesResult = await queryWithRetry('SELECT module, data FROM cloud_backup_modules WHERE installation_id = $1', [installationId]);
        if (modulesResult.rows.length === 0) {
            return res.status(404).json({ success: false, message: 'Walang cloud backup na nakita para sa installationId na ito.' });
        }
        const payload = {};
        modulesResult.rows.forEach((r) => { payload[r.module] = r.data; });
        res.setHeader('Content-Disposition', `attachment; filename="cloud-backup-${installationId}.json"`);
        res.setHeader('Content-Type', 'application/json');
        res.send(JSON.stringify(payload, null, 2));
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// --------------------------------------------------------------
// POST /relay/cloud-backup/restore  (SELF-SERVICE, tinatawag mismo ng
// OMNIPOS CLIENT SERVER — hindi ng admin panel)
//
// MAHALAGANG PAALALA: ang requireApiKey ay SHARED SECRET — PAREHONG
// key ang ginagamit ng LAHAT ng kliyente/installation (naka-bake sa
// bawat client .env). Kaya HINDI ito sapat na proof-of-ownership —
// kahit sinong may hawak ng leaked/nakitang installationId ng IBANG
// tindahan ay kayang gumawa ng request papunta rito gamit lang ang
// parehong shared key. Ang TUNAY na naghihiwalay dito ay ang
// hardwareFingerprint check sa ibaba: dapat itong TUMUGMA sa huling
// verified fingerprint na naka-bind sa installationId na ito (mula sa
// /relay/verify-login flow) — ibig sabihin, dapat mismong ang PARE-
// PAREHONG pisikal na device na huling nag-verify-login ang humihiling
// ng restore, hindi lang basta may alam na installationId.
//
// Body: { installationId, hardwareFingerprint }
// --------------------------------------------------------------
app.post('/relay/cloud-backup/restore', requireApiKey, requireAllowedDevice, rateLimit('cloud-backup-restore', 10, 60 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
    const { installationId, hardwareFingerprint } = req.body;

    if (!installationId || !hardwareFingerprint) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId o hardwareFingerprint.' });
    }

    if (!isFeatureCurrentlyUnlocked(installationId, 'cloud_backup')) {
        logActivity(installationId, 'cloud_backup_restore_blocked', { reason: 'feature_not_unlocked' });
        return res.status(402).json({
            success: false,
            featureLocked: true,
            featureId: 'cloud_backup',
            featureName: FEATURE_CATALOG.cloud_backup.name,
            isSubscription: true,
            plans: CLOUD_BACKUP_PLANS,
            message: 'There is no active Cloud Backup subscription for this installation, or it has expired.'
        });
    }

    // --- Proof-of-ownership check (HINDI lang basta yung shared API key) ---
    const fpRecord = deviceFingerprints.get(installationId);
    if (!fpRecord) {
        logActivity(installationId, 'cloud_backup_restore_blocked', { reason: 'no_verified_fingerprint' });
        return res.status(403).json({
            success: false,
            message: 'Wala pang na-verify na device fingerprint para sa installation na ito. Mag-login muna online (verify-login) bago mag-restore.'
        });
    }
    if (fpRecord.flagged) {
        logActivity(installationId, 'cloud_backup_restore_blocked', { reason: 'clone_flagged' });
        return res.status(403).json({
            success: false,
            cloneSuspected: true,
            message: 'Naka-flag ang device na ito bilang posibleng clone/duplicate. Kontakin ang developer/store owner para i-review at i-reset bago payagan ang restore.'
        });
    }
    if (fpRecord.fingerprint !== hardwareFingerprint) {
        logActivity(installationId, 'cloud_backup_restore_blocked', { reason: 'fingerprint_mismatch' });
        return res.status(403).json({
            success: false,
            message: 'Hindi tumutugma ang device na ito sa huling na-verify na device para sa installation na ito. Kontakin ang developer/store owner kung totoong ikaw ang may-ari nito.'
        });
    }

    if (!pgPool) {
        return res.status(503).json({ success: false, message: 'Hindi pa naka-configure ang Postgres (DATABASE_URL).' });
    }

    try {
        const metaResult = await queryWithRetry('SELECT * FROM cloud_backup_meta WHERE installation_id = $1', [installationId]);
        if (!metaResult.rows[0]) {
            return res.status(404).json({ success: false, message: 'Walang cloud backup na nakita para sa installation na ito.' });
        }
        const modulesResult = await queryWithRetry('SELECT module, data, record_count FROM cloud_backup_modules WHERE installation_id = $1', [installationId]);

        const modules = {};
        modulesResult.rows.forEach((r) => { modules[r.module] = r.data; });

        logActivity(installationId, 'cloud_backup_restored', {
            moduleCount: modulesResult.rows.length,
            lastSyncAt: metaResult.rows[0].last_sync_at
        });

        res.json({
            success: true,
            message: 'Nakuha ang cloud backup para sa installation na ito.',
            meta: {
                storeName: metaResult.rows[0].store_name,
                totalRecords: metaResult.rows[0].total_records,
                moduleCount: metaResult.rows[0].module_count,
                lastSyncAt: metaResult.rows[0].last_sync_at
            },
            modules,
            // Alam ng client kung aling fields ang hindi kasama (redacted)
            // dito, para malinaw sa kanya na kailangan pa ring i-reset ang
            // password ng mga na-restore na user account.
            redactedFieldsByModule: CLOUD_BACKUP_REDACTED_FIELDS_BY_MODULE
        });
    } catch (err) {
        console.error('⚠️ CLOUD_BACKUP: hindi na-kuha mula sa Postgres:', err.message);
        res.status(500).json({ success: false, message: 'May error habang kinukuha mula sa Postgres: ' + err.message });
    }
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
            permit: issueDevicePermit(installationId, hardwareFingerprint),
            message: 'Unang beses na na-verify online ang device na ito.'
        });
    }

    // Bago suriin ang flagged/mismatch state: baka dati nang "split" ng
    // admin ang EKSAKTONG fingerprint na ito papunta sa sarili nitong
    // bagong installationId (tingnan ang /split-clone admin endpoint) —
    // ibig sabihin sinadya na, hiwalay na dapat itong device mula ngayon,
    // kahit naka-flag pa rin ang ORIGINAL na installationId. Kung ganoon,
    // huwag nang i-block — sabihin lang sa client na dapat lumipat na
    // ito sa bagong ID mula ngayon.
    const splitEntry = cloneSplits.get(cloneSplitKey(installationId, hardwareFingerprint));
    if (splitEntry && splitEntry.newInstallationId) {
        const newId = splitEntry.newInstallationId;
        const splitFp = deviceFingerprints.get(newId);
        if (splitFp) {
            splitFp.lastVerifiedAt = Date.now();
            splitFp.verifyCount = (splitFp.verifyCount || 0) + 1;
            saveDeviceFingerprints(deviceFingerprints);
        }
        recordDeviceSeen(newId, { storeName, username });
        logActivity(newId, 'device_reverified_after_split', { splitFromInstallationId: installationId });
        return res.json({
            success: true,
            allowed: allowedDevices.has(newId),
            firstTime: false,
            reassignedInstallationId: newId,
            permit: issueDevicePermit(newId, hardwareFingerprint),
            message: 'Na-verify — ang device na ito ay hiwalay na (na-split mula sa isang naunang na-flag na clone). Ida-adopt ng client ang bagong installationId mula ngayon.'
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
        permit: issueDevicePermit(installationId, hardwareFingerprint),
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
// GET /relay/admin/api/devices/:installationId/integrity  (FILE INTEGRITY)
// Detalyadong resulta ng huling integrity check-in ng isang device —
// listahan ng modified/deleted/added files, parang "git status" na
// view sa admin panel.
// --------------------------------------------------------------
app.get('/relay/admin/api/devices/:installationId/integrity', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    const record = integrityStatus[installationId] || null;
    res.json({ success: true, record });
});

// --------------------------------------------------------------
// POST /relay/admin/api/devices/:installationId/integrity/clear
// I-a-acknowledge/i-clear ang red flag ng isang device (hal. na-review
// na ng developer ang mga binagong file at LEGIT na pala ito — hal.
// sarili nilang customization). HINDI nito binabago ang baseline —
// sa susunod na check-in, kung parehong "modified" pa rin ang file na
// iyon, ma-flag ulit ito (sinasadya: hindi ito "puwede ka nang
// magbago paulit-ulit", isang beses lang na acknowledge ng KASALUKUYAN
// na state).
// --------------------------------------------------------------
app.post('/relay/admin/api/devices/:installationId/integrity/clear', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    const { note } = req.body || {};
    const record = integrityStatus[installationId];
    if (!record) {
        return res.status(404).json({ success: false, message: 'Walang naitalang integrity check-in para sa device na ito.' });
    }
    record.clearedAt = Date.now();
    record.clearedNote = String(note || '').trim() || null;
    integrityStatus[installationId] = record;
    saveIntegrityStatus(integrityStatus);
    logActivity(installationId, 'integrity_alert_cleared', { note: record.clearedNote });
    res.json({ success: true, message: 'Na-clear ang integrity flag.' });
});

// --------------------------------------------------------------
// POST /relay/admin/api/devices/:installationId/integrity/check-now
// "🔄 I-check ngayon" — hindi direktang tumatawag ang RELAY papunta sa
// device (walang ganoong push channel sa architecture na ito, laging
// ang client ang nagsisimula ng connection). Sa halip, dito lang
// itinatakda ang isang pending flag; ang OMNIPOS client ang bibisita
// dito (sa loob ng existing na attemptRelayFeatureSync() nito, na
// tumatakbo na kada ~30s), makikita ang flag, at doon lang mismo agad
// tatakbo ang integrity check-in nito — kaya ilang segundo hanggang
// ~30s (hindi instant) ang praktikal na abot ng button na ito, hindi
// dahil sa delay dito kundi dahil sa polling interval ng client.
// --------------------------------------------------------------
app.post('/relay/admin/api/devices/:installationId/integrity/check-now', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    pendingIntegrityChecks.add(installationId);
    logActivity(installationId, 'integrity_check_requested', {});
    res.json({ success: true, message: 'Hihintayin ang susunod na online check-in ng device na ito (karaniwan ay ilang segundo hanggang ~30s).' });
});

// --------------------------------------------------------------
// POST /relay/admin/api/devices/:installationId/split-clone  (ANTI-CLONE)
// Gamitin ito kapag TALAGANG dalawang HIWALAY na device ang gusto mong
// PATULOY na paganahin nang sabay — hal. luma mong tester unit AT yung
// unit na ibinenta mo sa customer, base sa parehong lumang data folder.
// Sa halip na i-share ng dalawa ang IISANG installationId (na parang
// "musical chairs" — isa lang sa kanila ang puwedeng maging "allowed"
// nang sabay), ginagawan ito ng SARILI at BAGONG installationId + sarili
// nitong fingerprint binding — kaya independent na sila mula ngayon.
//
// Lohika:
//   1. Kunin ang naka-flag na record ng ORIGINAL installationId
//      (dapat mayroon itong flaggedFingerprint mula sa isang mismatch).
//   2. Gumawa ng BAGONG installationId (random UUID).
//   3. I-bind ang flaggedFingerprint sa BAGONG installationId (sarili
//      na nitong deviceFingerprints entry).
//   4. I-unflag ang ORIGINAL — babalik ito sa dati nitong fingerprint,
//      parang walang nangyari, walang kailangang hiwalay na "reset".
//   5. Itago sa cloneSplits kung aling fingerprint ang naka-bind na sa
//      bagong ID — para sa susunod na verify-login mula sa clone device,
//      awtomatiko na itong ma-reassign (tingnan ang /relay/verify-login).
//   6. Kung ALLOWED na ang ORIGINAL, gagawin ding ALLOWED ang bagong ID
//      (dahil legit namang ginagamit na ito bago pa man i-split) — pero
//      WALANG kasamang mga naka-unlock na feature — sinasadyang blangko
//      ito, dapat manual na i-activate ng admin kung ano lang ang
//      totoong binayaran ng bagong may-ari (tingnan ang /activate at
//      /deactivate endpoints).
// --------------------------------------------------------------
app.post('/relay/admin/api/devices/:installationId/split-clone', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    const record = deviceFingerprints.get(installationId);

    if (!record || !record.flagged || !record.flaggedFingerprint) {
        return res.status(400).json({
            success: false,
            message: 'Walang naka-flag na clone fingerprint ang installationId na ito — wala nang i-sa-split.'
        });
    }

    const flaggedFingerprint = record.flaggedFingerprint;
    const newInstallationId = crypto.randomUUID();

    // 2-3. Bagong installationId, sariling fingerprint binding.
    deviceFingerprints.set(newInstallationId, {
        fingerprint: flaggedFingerprint,
        firstVerifiedAt: Date.now(),
        lastVerifiedAt: Date.now(),
        verifyCount: 1,
        flagged: false
    });

    // 4. I-unflag ang ORIGINAL, ibalik sa dati nitong (unflagged) na estado.
    record.flagged = false;
    delete record.flaggedFingerprint;
    delete record.flaggedAt;
    saveDeviceFingerprints(deviceFingerprints);

    // 5. Itala ang split mapping.
    cloneSplits.set(cloneSplitKey(installationId, flaggedFingerprint), {
        newInstallationId,
        splitAt: Date.now()
    });
    saveCloneSplits(cloneSplits);

    // 6. Kung allowed ang orig, gawin ding allowed ang bago (walang features).
    if (allowedDevices.has(installationId)) {
        allowedDevices.add(newInstallationId);
        saveAllowedDevices(allowedDevices);
    }

    // I-carry over ang label (may markang "hiwalay/split") para malinaw sa listahan.
    const originalLabel = deviceLabels.get(installationId);
    if (originalLabel) {
        deviceLabels.set(newInstallationId, `${originalLabel} (hiwalay/split)`);
        saveDeviceLabels(deviceLabels);
    }

    logActivity(installationId, 'clone_split_from', { newInstallationId });
    logActivity(newInstallationId, 'clone_split_created', { splitFromInstallationId: installationId });

    res.json({
        success: true,
        newInstallationId,
        allowed: allowedDevices.has(newInstallationId),
        message: `Nagawa na ang hiwalay na installationId (${newInstallationId}). Awtomatiko itong ia-adopt ng clone device sa susunod nitong pag-verify. Huwag kalimutang i-activate manually ang mga totoong binayaran nitong features.`
    });
});

// --------------------------------------------------------------
// POST /relay/request-unlock
// Tinatawag ito ng CLIENT server (hindi diretso ng browser ng cashier)
// tuwing may humihiling mag-unlock ng isang Pro theme.
// --------------------------------------------------------------
app.post('/relay/request-unlock', requireApiKey, requireAllowedDevice, rateLimit('request-unlock', 5, 10 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
    const { installationId, featureId, featureName, price, username, storeName, photo, tier, billingCycle } = req.body;

    if (!installationId || !featureId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId o featureId.' });
    }

    const isCloudBackup = featureId === 'cloud_backup';
    let groundTruthPrice = null;

    if (isCloudBackup) {
        // Subscription — needs a valid tier + billingCycle, and the PRICE
        // does NOT come from the client (`price` in the body) but from
        // RELAY's own CLOUD_BACKUP_PLANS (ground truth).
        groundTruthPrice = getCloudBackupPlanPrice(tier, billingCycle);
        if (groundTruthPrice === null) {
            return res.status(400).json({ success: false, message: 'Invalid Cloud Backup tier/billingCycle.' });
        }
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
        featureName: isCloudBackup ? CLOUD_BACKUP_PLANS[tier].name : (featureName || featureId),
        price: isCloudBackup ? groundTruthPrice : (price || null),
        tier: isCloudBackup ? tier : null,
        billingCycle: isCloudBackup ? billingCycle : null,
        // durationDays is pre-set based on the chosen billing cycle — the
        // admin no longer needs to guess/type this manually on each
        // approval (see also /relay/admin/api/pending-otps/approve).
        durationDays: isCloudBackup ? CLOUD_BACKUP_BILLING_DAYS[billingCycle] : undefined
    });

    try {
        // Ground-truth from RELAY's OWN FEATURE_CATALOG/CLOUD_BACKUP_PLANS —
        // we don't rely on the client-supplied featureName/price, since
        // those just come from the request body (which can be altered).
        // If there's a mismatch, explicitly flag it in the email to alert
        // the admin before they Approve.
        const catalogEntry = FEATURE_CATALOG[featureId] || null;
        const displayPrice = isCloudBackup ? groundTruthPrice : price;
        const displayName = isCloudBackup ? CLOUD_BACKUP_PLANS[tier].name : (featureName || featureId);
        const priceMismatch = !isCloudBackup && catalogEntry && typeof price === 'number' && price !== catalogEntry.price;
        const nameMismatch = !isCloudBackup && catalogEntry && featureName && featureName !== catalogEntry.name;

        await notifyUnlockRequest({
            subject: `🎨 Unlock Request — ${displayName}${displayPrice ? ` (₱${displayPrice})` : ''}`,
            text: `Someone requested to ${isCloudBackup ? 'subscribe/renew Cloud Backup' : 'unlock a Pro theme'}.\n\n` +
                  `Store: ${storeName || 'Not specified'}\n` +
                  `Installation ID: ${installationId}\n` +
                  `Feature: ${displayName}\n` +
                  (isCloudBackup ? `Plan: ${tier} (${billingCycle}) — ₱${groundTruthPrice}\n` : '') +
                  (isCloudBackup ? `Access to be granted: ${CLOUD_BACKUP_BILLING_DAYS[billingCycle]} days from approval\n` : '') +
                  (!isCloudBackup && price ? `Price (stated by client): ₱${price}\n` : '') +
                  (!isCloudBackup && catalogEntry ? `Price per our price list: ₱${catalogEntry.price} (${catalogEntry.name})\n` : (!isCloudBackup ? `⚠️ featureId "${featureId}" was not found in our price list — be careful.\n` : '')) +
                  ((priceMismatch || nameMismatch) ? `⚠️⚠️ THERE IS A DISCREPANCY in price/name — it does not match the official price list. DO NOT Approve until this is verified.\n` : '') +
                  `Requested by: ${username || 'Unknown'}\n` +
                  `OTP Code: ${otpCode}\n` +
                  `This will expire within 10 minutes.\n\n` +
                  `Verify payment first before giving this OTP to the client.`
        });

        logActivity(installationId, 'otp_requested', { featureId, featureName: displayName, tier: tier || null, billingCycle: billingCycle || null });
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
        source: 'otp',
        tier: pending.tier || null,
        billingCycle: pending.billingCycle || null
    });
    logActivity(installationId, 'unlock_issued', { featureId, featureName: pending.featureName, source: 'otp', tier: pending.tier || null, billingCycle: pending.billingCycle || null });

    pendingOtps.delete(key);

    res.json({
        success: true,
        message: `Na-unlock ang ${pending.featureName}!`,
        token,
        tier: pending.tier || undefined,
        billingCycle: pending.billingCycle || undefined
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
// POST /relay/end-demo — SELF-SERVICE, client-facing (requireApiKey lang,
// HINDI requireAdminKey) na endpoint na tinatawag ng OMNIPOS mismo
// (POST /api/features/end-demo sa server.js nito) sa sandaling manual na
// tinapos ng admin ng tindahan ang Demo Mode nito nang maaga.
//
// BAKIT KAILANGAN ITO: dati, ang "End Demo" sa OMNIPOS ay LOKAL lang —
// tinatanggal lang nito ang demo token sa sarili nitong featureUnlocks.json,
// pero HINDI naaalis ang record dito sa RELAY (issuedUnlocks). Kaya kung
// may NATITIRA pang oras ang demo bago talaga ito mag-expire (hal. RELAY_
// DEMO_DURATION_HOURS pa rin sa hinaharap), at nag-hard-reset o nag-restore
// mula sa RELAY ang device (tingnan ang /relay/restore-check-in), maaari
// pa ring "bumalik" ang parehong demo session — ibinabalik lang kasi ng
// restore ang LAHAT ng entries na naka-record dito na hindi pa expired.
//
// Kaya sa endpoint na ito, tinatanggal na rin DIRETSO dito sa RELAY ang
// issued demo entry ng installationId na ito (DEMO_FEATURE_ID lang — hindi
// ito ginagamit para mag-alis ng kahit anong BINAYARANG feature), kaya
// TULUYAN na itong hindi na maibabalik pa kahit anong restore/check-in pa
// ang mangyari — kailangan na ng bagong OTP request kung gugustuhin pang
// muling buksan ang demo sa hinaharap.
// --------------------------------------------------------------
app.post('/relay/end-demo', requireApiKey, requireAllowedDevice, rateLimit('end-demo', 20, 10 * 60 * 1000, (req) => req.body?.installationId), (req, res) => {
    const { installationId } = req.body;

    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }

    const record = issuedUnlocks[installationId];
    if (!record || !record[DEMO_FEATURE_ID]) {
        return res.json({ success: true, alreadyInactive: true, message: 'Wala namang naka-record na aktibong Demo Mode dito sa RELAY para sa device na ito.' });
    }

    delete record[DEMO_FEATURE_ID];
    saveIssuedUnlocks(issuedUnlocks);
    logActivity(installationId, 'demo_ended_early', {
        featureId: DEMO_FEATURE_ID,
        featureName: 'Full Demo Mode',
        reason: 'client_self_service'
    });

    res.json({ success: true, message: 'Tuluyan nang tinapos ang Demo Mode dito sa RELAY — hindi na ito maibabalik kahit pa may natitirang oras dati.' });
});

// --------------------------------------------------------------
// POST /relay/cancel-otp
// Lets the CLIENT (OMNIPOS, on behalf of whoever is looking at the
// verification modal) proactively expire a pending OTP BEFORE its
// normal 10-minute TTL, instead of waiting for cleanupExpiredOtps()
// to eventually sweep it. This is used for two cases:
//   1) The client closed/cancelled the "Verification Required" modal
//      without entering a code.
//   2) The client ran out of verification attempts (wrong code 3
//      times) and the modal auto-closed on its own.
// In both cases the OTP must not remain usable — even though time
// is technically still left on the clock — so we delete it from
// pendingOtps right away. This mirrors the SAME key formats used by
// /relay/request-unlock, /relay/request-unlock-bulk, and
// /relay/request-demo above, so pass one of: featureId (single
// feature/theme/cloud-backup), featureIds (bundle), or demo: true.
// Body: { installationId, featureId? , featureIds?, demo? }
// --------------------------------------------------------------
app.post('/relay/cancel-otp', requireApiKey, requireAllowedDevice, rateLimit('cancel-otp', 30, 10 * 60 * 1000, (req) => req.body?.installationId), (req, res) => {
    const { installationId, featureId, featureIds, demo } = req.body;

    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }

    let key;
    if (demo) {
        key = `${installationId}:${DEMO_FEATURE_ID}`;
    } else if (Array.isArray(featureIds) && featureIds.length > 0) {
        key = `${installationId}:__bulk__:${featureIds.slice().sort().join(',')}`;
    } else if (featureId) {
        key = `${installationId}:${featureId}`;
    } else {
        return res.status(400).json({ success: false, message: 'Kulang ang featureId, featureIds, o demo flag.' });
    }

    const pending = pendingOtps.get(key);
    if (!pending) {
        // Already gone (expired, already confirmed, or never existed) —
        // still a success from the client's point of view, since the
        // end state (no usable pending OTP) is what it wanted anyway.
        return res.json({ success: true, alreadyGone: true, message: 'Wala nang pending OTP dito.' });
    }

    pendingOtps.delete(key);
    logActivity(installationId, 'otp_cancelled', {
        featureId: featureId || null,
        featureIds: featureIds || null,
        demo: !!demo,
        featureName: pending.featureName || pending.featureNames || null
    });

    res.json({ success: true, message: 'Na-cancel ang pending OTP.' });
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

    // --------------------------------------------------------------
    // PROPORTIONAL BUNDLE-DISCOUNT PRICING — dati, bawat featureId sa
    // bundle ay naire-record gamit ang SARILING à la carte na presyo
    // mula sa FEATURE_CATALOG (walang discount), kahit na ang aktwal
    // na binayaran ng customer (pending.price, mula sa totalPrice na
    // ipinasa ng OMNIPOS client noong /request-unlock-bulk) ay MAS
    // MABABA dahil sa bundle/tier discount. Ibig sabihin, kada mag-
    // bundle ang isang customer, "bumubulak" (overstated) ang
    // Active Revenue sa Analytics kumpara sa TOTOONG binayaran.
    //
    // Para tumugma ang revenue sa Relay sa aktwal na binayaran (kasama
    // ang discount), ipinapamahagi na natin ang pending.price nang
    // PROPORTIONAL sa à la carte na presyo ng bawat feature (kaya ang
    // mas mahal na feature ay mas malaking bahagi ng discount ang
    // natatanggap), sa halip na gamitin ang buong à la carte price ng
    // bawat isa. Kung walang pending.price (hal. direktang na-activate
    // ng admin nang walang totalPrice), babalik ito sa dating gawi —
    // gamitin na lang ang à la carte price ng bawat feature.
    // --------------------------------------------------------------
    let perFeaturePrice = {};
    if (typeof pending.price === 'number' && pending.price >= 0) {
        const alaCartePrices = featureIds.map(id => (FEATURE_CATALOG[id] && FEATURE_CATALOG[id].price) || 0);
        const alaCarteTotal = alaCartePrices.reduce((s, p) => s + p, 0);

        if (alaCarteTotal > 0) {
            let allocated = 0;
            featureIds.forEach((id, i) => {
                const share = Math.floor((pending.price * alaCartePrices[i]) / alaCarteTotal);
                perFeaturePrice[id] = share;
                allocated += share;
            });
            // Ilagay ang natirang piso (dahil sa pag-round-down) sa
            // pinakamahal na feature, para eksaktong tumutugma pa rin
            // ang KABUUAN ng mga naka-record na presyo sa pending.price
            // (ang aktwal na binayaran).
            let remainder = Math.round(pending.price) - allocated;
            if (remainder !== 0 && featureIds.length > 0) {
                const priciestIdx = alaCartePrices.indexOf(Math.max(...alaCartePrices));
                perFeaturePrice[featureIds[priciestIdx]] += remainder;
            }
        } else {
            // Walang à la carte reference (hal. lahat unknown/₱0 sa
            // catalog) — hatiin na lang nang pantay-pantay.
            const evenShare = Math.floor(pending.price / featureIds.length);
            let allocated = 0;
            featureIds.forEach((id, i) => {
                perFeaturePrice[id] = evenShare;
                allocated += evenShare;
            });
            perFeaturePrice[featureIds[featureIds.length - 1]] += Math.round(pending.price) - allocated;
        }
    }

    for (let i = 0; i < featureIds.length; i++) {
        const featureId = featureIds[i];
        const token = issueSignedToken(installationId, featureId, durationMs);
        tokens[featureId] = token;

        const featureName = namesList[i] || featureId;
        const priceForThisFeature = Object.prototype.hasOwnProperty.call(perFeaturePrice, featureId)
            ? perFeaturePrice[featureId]
            : (FEATURE_CATALOG[featureId] ? FEATURE_CATALOG[featureId].price : null);
        recordIssuedUnlock(installationId, featureId, token, {
            featureName,
            price: priceForThisFeature,
            source: 'otp-bulk'
        });
        logActivity(installationId, 'unlock_issued', { featureId, featureName, source: 'otp-bulk' });
    }

    pendingOtps.delete(key);

    res.json({ success: true, message: `Na-unlock ang ${featureIds.length} feature(s)!`, tokens });
});

// --------------------------------------------------------------
// Admin password reset (self-service, developer-approved).
// REWRITTEN VERSION — pinalitan ang lumang implementasyon ng bagong
// bersyon na gumagamit ng parehong URL paths, request body fields, at
// response shapes (para hindi kailangang baguhin ang OMNIPOS client:
// tingnan ang /api/admin/request-password-reset at
// /api/admin/confirm-password-reset doon) pero may mga dagdag na
// proteksyon:
//
//   1. AUTO-CLEANUP — dati, walang setInterval na naglilinis ng
//      pendingAdminResets Map (kaiba sa pendingOtps na may cleanup
//      taliwas dito). Ibig sabihin, kada humiling ng reset ang isang
//      bagong installationId, PERMANENTENG naiiwan ang entry sa
//      memory kahit na-expire/na-consume na — unbounded growth sa
//      matagal na uptime. Idinagdag na ang parehong 30-segundong
//      cleanup pattern na ginagamit ng pendingOtps.
//   2. FAILED-ATTEMPT LOCKOUT — dati, walang bilang ng maling OTP
//      guesses bukod sa generic na rate limit (30 tries/15 min bawat
//      installationId). Dahil ito ay FULL ADMIN ACCOUNT TAKEOVER kapag
//      na-guess, dagdag pang proteksyon: pagkatapos ng
//      MAX_FAILED_OTP_ATTEMPTS na maling tangka, AWTOMATIKONG
//      binubura ang pending request — kailangan nang humiling ulit ng
//      bagong OTP (na kailangan pang aprubahan ulit ng developer),
//      hindi lang basta maghintay ng rate-limit window.
//   3. Ginagamit na ang parehong checkApprovalGate() helper (tulad ng
//      confirm-unlock) sa halip na duplicate na inline na logic, para
//      consistent ang approval-gate behavior sa buong relay.
//
// LAYUNIN (hindi nagbago): Self-service ang pag-request, PERO ang
// developer/owner pa rin (dito, sa RELAY) ang huling humahawak ng
// desisyon — kailangan pareho ng (a) tamang OTP at (b) manual Approve
// sa admin panel bago ma-issue ang reset ticket. Parang unlock flow,
// pero:
//   - Ang OTP ay pumupunta sa DEVELOPER (hindi sa customer) dahil
//     ito ay pag-reset ng Admin account mismo (walang ibang paraan
//     para i-verify na TALAGANG ang may-ari ng store ang humihiling).
//   - Ang na-issue na ticket ay MAIKLI lang mabuhay (5 minuto) at
//     may `purpose:'admin-password-reset'` field para hindi ito
//     magamit sa ibang bagay kahit paano.
// --------------------------------------------------------------
const ADMIN_RESET_OTP_TTL_MS = 10 * 60 * 1000; // 10 minuto, tugma sa OTP_TTL_MS
const ADMIN_RESET_TICKET_TTL_MS = 5 * 60 * 1000; // 5 minuto lang ang reset ticket
const MAX_FAILED_OTP_ATTEMPTS = 5; // pagkatapos nito, kailangan nang humiling ng bagong OTP

// key: installationId -> { code, expiresAt, approved, otpVerified, failedAttempts,
//                           storeName, hintUsername, requestedAt }
const pendingAdminResets = new Map();

function generateAdminResetOtp() {
    return String(Math.floor(100000 + Math.random() * 900000));
}

// AUTO-CLEANUP — kada 30 segundo, tinatanggal ang kahit anong pending
// admin-reset request na LUMAMPAS na sa expiry nito, kapareho ng
// ginagawang cleanup para sa pendingOtps (tingnan sa itaas). Kung
// walang ganito, hindi na-Run/Approve na requests ay MANANATILI sa
// memory magpakailanman kahit hindi na kailanman magagamit muli.
setInterval(() => {
    const now = Date.now();
    for (const [installationId, pending] of pendingAdminResets.entries()) {
        if (now > pending.expiresAt) {
            pendingAdminResets.delete(installationId);
        }
    }
}, 30 * 1000).unref();

app.post('/relay/request-admin-reset',
    requireApiKey,
    requireAllowedDevice,
    rateLimit('request-admin-reset', 3, 15 * 60 * 1000, (req) => req.body?.installationId),
    async (req, res) => {
        const { installationId, storeName, hintUsername } = req.body;

        if (!installationId) {
            return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
        }

        const otpCode = generateAdminResetOtp();
        pendingAdminResets.set(installationId, {
            code: otpCode,
            expiresAt: Date.now() + ADMIN_RESET_OTP_TTL_MS,
            approved: false,
            otpVerified: false,
            failedAttempts: 0,
            storeName: storeName || null,
            hintUsername: hintUsername || null,
            requestedAt: Date.now()
        });

        try {
            await notifyUnlockRequest({
                subject: `🔑 Admin Password Reset Request — ${storeName || installationId}`,
                text: `May humiling na i-reset ang ADMIN password ng isang OMNIPOS installation.\n\n` +
                      `Store: ${storeName || 'Hindi tiyak'}\n` +
                      `Installation ID: ${installationId}\n` +
                      `Posibleng account (sinabi ng client): ${hintUsername || 'Hindi tiyak'}\n` +
                      `OTP Code: ${otpCode}\n` +
                      `Mag-e-expire ito sa loob ng 10 minuto.\n\n` +
                      `⚠️ TIYAKIN muna na TALAGANG ang may-ari/kilalang contact ng store na ito ang ` +
                      `humihiling (tumawag/mag-text kung kinakailangan) BAGO mag-Approve at ibigay ang ` +
                      `OTP na ito — ang sinumang naka-access sa terminal ang pwedeng nag-trigger nito.`
            });

            logActivity(installationId, 'admin_reset_requested', { storeName: storeName || null });
            res.json({ success: true, message: 'Naipadala ang reset request. Kontakin ang developer para sa OTP.' });
        } catch (err) {
            console.error('Relay mail send failure (admin-reset):', err);
            pendingAdminResets.delete(installationId);
            res.status(500).json({ success: false, message: `Nabigo ang pagpapadala ng OTP: ${err.message}` });
        }
    }
);

// Reuses the same approval-gate idea as unlocks — pinapayagan ang
// "tama na ang OTP pero naghihintay pa ng Approve" na estado.
app.get('/relay/admin/api/pending-admin-resets', requireAdminKey, (req, res) => {
    const list = [];
    for (const [installationId, pending] of pendingAdminResets) {
        if (Date.now() > pending.expiresAt) continue;
        list.push({
            installationId,
            storeName: pending.storeName,
            hintUsername: pending.hintUsername,
            approved: pending.approved,
            otpVerified: pending.otpVerified,
            requestedAt: pending.requestedAt,
            expiresAt: pending.expiresAt,
            code: pending.code
        });
    }
    res.json({ success: true, pending: list });
});

app.post('/relay/admin/api/pending-admin-resets/approve', requireAdminKey, (req, res) => {
    const { installationId } = req.body;
    const pending = pendingAdminResets.get(installationId);
    if (!pending) {
        return res.status(404).json({ success: false, message: 'Walang pending admin-reset request para dito.' });
    }
    pending.approved = true;
    logActivity(installationId, 'admin_reset_approved', {});
    res.json({ success: true, message: 'Naaprubahan. Puwede nang gamitin ng client ang OTP.' });
});

app.post('/relay/confirm-admin-reset',
    requireApiKey,
    requireAllowedDevice,
    // FIX: ang client (OMNIPOS) ay AWTOMATIKONG nag-po-poll dito kada 6
    // segundo (pollUntilApproved) habang naghihintay ng "Allow/Run" mula
    // sa developer, kapareho ng ginagawa ng confirm-unlock. Ang dating
    // limitasyong 30/15-min ay nauubos sa loob lang ng ~3 minuto ng
    // polling — kung matagal pa bago ma-Approve ng developer (karaniwan,
    // dahil kailangan pa niyang tumawag/mag-text para i-verify ang
    // requester), basta na lang na-bo-block ng 429 ang flow kahit tama na
    // ang OTP. Ginawa itong 120/10-min — kapareho ng limitasyon sa
    // confirm-unlock — para sapat ito para sa buong 10-minutong OTP TTL.
    rateLimit('confirm-admin-reset', 120, 10 * 60 * 1000, (req) => req.body?.installationId),
    (req, res) => {
        const { installationId, otp } = req.body;

        if (!installationId || !otp) {
            return res.status(400).json({ success: false, message: 'Kulang ang installationId o otp.' });
        }

        const pending = pendingAdminResets.get(installationId);
        if (!pending) {
            return res.status(400).json({ success: false, message: 'Walang aktibong reset request. Humingi muna ng OTP.' });
        }
        if (Date.now() > pending.expiresAt) {
            pendingAdminResets.delete(installationId);
            return res.status(400).json({ success: false, message: 'Expired na ang OTP. Humingi ng bago.' });
        }
        if (!safeCompare(String(otp).trim(), pending.code)) {
            // FAILED-ATTEMPT LOCKOUT: bilangin ang maling tangka. Pagkatapos
            // ng MAX_FAILED_OTP_ATTEMPTS, buburahin na ang buong pending
            // request kahit hindi pa na-expire — kailangan nang humiling ng
            // bagong OTP (bagong developer approval) sa halip na patuloy na
            // maka-guess hangga't hindi pa naaabot ang generic rate limit.
            pending.failedAttempts = (pending.failedAttempts || 0) + 1;
            if (pending.failedAttempts >= MAX_FAILED_OTP_ATTEMPTS) {
                pendingAdminResets.delete(installationId);
                logActivity(installationId, 'admin_reset_locked_out', { failedAttempts: pending.failedAttempts });
                return res.status(400).json({
                    success: false,
                    message: 'Sobra na sa pinapayagang maling tangka. Nakansela ang request na ito — humiling ng bagong reset request at OTP.'
                });
            }
            return res.status(400).json({ success: false, message: 'Maling OTP code.' });
        }

        if (!checkApprovalGate(pending)) {
            return res.json({
                success: false,
                pending: true,
                message: 'Tama ang OTP! Naghihintay pa lang ng approval mula sa developer. Subukan ulit paglipas ng ilang segundo.'
            });
        }

        // Tama ang OTP AT naaprubahan — gumawa ng SHORT-LIVED signed
        // ticket. `purpose` field ang nagbubukod dito sa mga
        // feature-unlock tokens kahit parehong private key ang gamit.
        const now = Date.now();
        const payload = {
            installationId,
            purpose: 'admin-password-reset',
            issuedAt: now,
            expiresAt: now + ADMIN_RESET_TICKET_TTL_MS
        };
        const payloadString = JSON.stringify(payload);
        const signature = crypto.sign(null, Buffer.from(payloadString), privateKey).toString('base64');

        logActivity(installationId, 'admin_reset_ticket_issued', {});
        pendingAdminResets.delete(installationId);

        res.json({
            success: true,
            message: 'Na-verify. Puwede ka nang mag-set ng bagong Admin password.',
            ticket: { payload, signature }
        });
    }
);

// --------------------------------------------------------------
// RECEIPT CUSTOMIZATION COUNTER RESET (self-service, RELAY-verified)
// Pareho ito sa admin-password-reset flow sa itaas — LUMIPAT dito
// mula sa dating implementasyon (OMNIPOS mismo ang nagpapadala ng OTP
// via sarili nitong Gmail sender sa Receipt Customization panel).
// Ngayon, kagaya na ng feature-unlock at admin-password-reset,
// dito na RIN dumadaan ang OTP para sa pag-reset ng 2-free-attempts
// na counter ng Receipt Customization — pumupunta ito sa DEVELOPER
// (hindi sa store), at kailangan pa rin ng manual Approve dito bago
// ma-issue ang reset ticket. Kapareho rin ng admin-reset:
//   - AUTO-CLEANUP kada 30s ng mga expired na pending request.
//   - FAILED-ATTEMPT LOCKOUT pagkatapos ng MAX_FAILED_OTP_ATTEMPTS.
//   - Ginagamit ang parehong checkApprovalGate() helper.
//   - Naka-sign na ticket lang (purpose:'receipt-customization-reset')
//     ang ibinabalik — WALANG ibang datos na binabago dito sa RELAY;
//     ang OMNIPOS mismo ang nag-a-apply ng reset gamit ang ticket.
// --------------------------------------------------------------
const RECEIPT_RESET_OTP_TTL_MS = 10 * 60 * 1000; // 10 minuto, tugma sa OTP_TTL_MS
const RECEIPT_RESET_TICKET_TTL_MS = 5 * 60 * 1000; // 5 minuto lang ang reset ticket

// key: installationId -> { code, expiresAt, approved, otpVerified, failedAttempts,
//                           storeName, requestedBy, requestedAt }
const pendingReceiptResets = new Map();

function generateReceiptResetOtp() {
    return String(Math.floor(100000 + Math.random() * 900000));
}

// AUTO-CLEANUP — kada 30 segundo, tinatanggal ang kahit anong pending
// receipt-reset request na LUMAMPAS na sa expiry nito.
setInterval(() => {
    const now = Date.now();
    for (const [installationId, pending] of pendingReceiptResets.entries()) {
        if (now > pending.expiresAt) {
            pendingReceiptResets.delete(installationId);
        }
    }
}, 30 * 1000).unref();

app.post('/relay/request-receipt-reset',
    requireApiKey,
    requireAllowedDevice,
    rateLimit('request-receipt-reset', 3, 15 * 60 * 1000, (req) => req.body?.installationId),
    async (req, res) => {
        const { installationId, storeName, requestedBy } = req.body;

        if (!installationId) {
            return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
        }

        const otpCode = generateReceiptResetOtp();
        pendingReceiptResets.set(installationId, {
            code: otpCode,
            expiresAt: Date.now() + RECEIPT_RESET_OTP_TTL_MS,
            approved: false,
            otpVerified: false,
            failedAttempts: 0,
            storeName: storeName || null,
            requestedBy: requestedBy || null,
            requestedAt: Date.now()
        });

        try {
            await notifyUnlockRequest({
                subject: `🧾 Receipt Customization Reset Request — ${storeName || installationId}`,
                text: `May humiling na i-reset ang 2-free-attempts na Receipt Customization counter ng isang OMNIPOS installation.\n\n` +
                      `Store: ${storeName || 'Hindi tiyak'}\n` +
                      `Installation ID: ${installationId}\n` +
                      `Hiniling ni: ${requestedBy || 'Hindi tiyak'}\n` +
                      `OTP Code: ${otpCode}\n` +
                      `Mag-e-expire ito sa loob ng 10 minuto.\n\n` +
                      `I-verify muna kung kinakailangan bago mag-Approve at ibigay ang OTP na ito sa kliyente.`
            });

            logActivity(installationId, 'receipt_reset_requested', { storeName: storeName || null });
            res.json({ success: true, message: 'Naipadala ang reset request. Kontakin ang developer para sa OTP.' });
        } catch (err) {
            console.error('Relay mail send failure (receipt-reset):', err);
            pendingReceiptResets.delete(installationId);
            res.status(500).json({ success: false, message: `Nabigo ang pagpapadala ng OTP: ${err.message}` });
        }
    }
);

app.get('/relay/admin/api/pending-receipt-resets', requireAdminKey, (req, res) => {
    const list = [];
    for (const [installationId, pending] of pendingReceiptResets) {
        if (Date.now() > pending.expiresAt) continue;
        list.push({
            installationId,
            storeName: pending.storeName,
            requestedBy: pending.requestedBy,
            approved: pending.approved,
            otpVerified: pending.otpVerified,
            requestedAt: pending.requestedAt,
            expiresAt: pending.expiresAt,
            code: pending.code
        });
    }
    res.json({ success: true, pending: list });
});

app.post('/relay/admin/api/pending-receipt-resets/approve', requireAdminKey, (req, res) => {
    const { installationId } = req.body;
    const pending = pendingReceiptResets.get(installationId);
    if (!pending) {
        return res.status(404).json({ success: false, message: 'Walang pending receipt-reset request para dito.' });
    }
    pending.approved = true;
    logActivity(installationId, 'receipt_reset_approved', {});
    res.json({ success: true, message: 'Naaprubahan. Puwede nang gamitin ng client ang OTP.' });
});

app.post('/relay/confirm-receipt-reset',
    requireApiKey,
    requireAllowedDevice,
    // Kagaya ng confirm-admin-reset — AWTOMATIKONG nag-po-poll dito ang
    // client kada 6 segundo (pollUntilApproved) habang naghihintay ng
    // Allow/Run mula sa developer, kaya ginawang 120/10-min ang limitasyon.
    rateLimit('confirm-receipt-reset', 120, 10 * 60 * 1000, (req) => req.body?.installationId),
    (req, res) => {
        const { installationId, otp } = req.body;

        if (!installationId || !otp) {
            return res.status(400).json({ success: false, message: 'Kulang ang installationId o otp.' });
        }

        const pending = pendingReceiptResets.get(installationId);
        if (!pending) {
            return res.status(400).json({ success: false, message: 'Walang aktibong reset request. Humingi muna ng OTP.' });
        }
        if (Date.now() > pending.expiresAt) {
            pendingReceiptResets.delete(installationId);
            return res.status(400).json({ success: false, message: 'Expired na ang OTP. Humingi ng bago.' });
        }
        if (!safeCompare(String(otp).trim(), pending.code)) {
            pending.failedAttempts = (pending.failedAttempts || 0) + 1;
            if (pending.failedAttempts >= MAX_FAILED_OTP_ATTEMPTS) {
                pendingReceiptResets.delete(installationId);
                logActivity(installationId, 'receipt_reset_locked_out', { failedAttempts: pending.failedAttempts });
                return res.status(400).json({
                    success: false,
                    message: 'Sobra na sa pinapayagang maling tangka. Nakansela ang request na ito — humiling ng bagong reset request at OTP.'
                });
            }
            return res.status(400).json({ success: false, message: 'Maling OTP code.' });
        }

        if (!checkApprovalGate(pending)) {
            return res.json({
                success: false,
                pending: true,
                message: 'Tama ang OTP! Naghihintay pa lang ng approval mula sa developer. Subukan ulit paglipas ng ilang segundo.'
            });
        }

        // Tama ang OTP AT naaprubahan — gumawa ng SHORT-LIVED signed
        // ticket. `purpose` field ang nagbubukod dito sa ibang klase ng
        // tickets (admin-password-reset, feature unlocks) kahit parehong
        // private key ang gamit.
        const now = Date.now();
        const payload = {
            installationId,
            purpose: 'receipt-customization-reset',
            issuedAt: now,
            expiresAt: now + RECEIPT_RESET_TICKET_TTL_MS
        };
        const payloadString = JSON.stringify(payload);
        const signature = crypto.sign(null, Buffer.from(payloadString), privateKey).toString('base64');

        logActivity(installationId, 'receipt_reset_ticket_issued', {});
        pendingReceiptResets.delete(installationId);

        res.json({
            success: true,
            message: 'Na-verify. Puwede nang i-reset ang counter.',
            ticket: { payload, signature }
        });
    }
);

// --------------------------------------------------------------
// GET /relay/latest-version
// Tinatawag ito ng OMNIPOS CLIENT SERVER (hindi ng browser mismo) sa
// "Check for Updates" ng Settings nito. Basic API key lang ang
// kailangan dito (walang requireAllowedDevice) — publicly-readable
// info lang naman ito (bersyon + changelog), hindi kailangang naka-
// Allow muna ang device para lang malaman kung may bagong update.
// --------------------------------------------------------------
app.get('/relay/latest-version', requireApiKey, rateLimit('latest-version', 60, 10 * 60 * 1000), (req, res) => {
    // TARGETED RELEASES: kung nagpasa ng ?installationId=... ang client
    // (bagong bersyon ng OMNIPOS client — tingnan ang /api/system
    // /update-check doon) AT may targeted entry PARA DITO, ito ang
    // isasagot sa halip na ang global systemVersionInfo — kaya ibang
    // device lang (kahit hindi target) ang makakakita ng "may bagong
    // update" na notice.
    const installationId = String(req.query.installationId || '').trim();
    const targeted = installationId ? targetedReleases.get(installationId) : null;
    const info = targeted || systemVersionInfo;
    res.json({
        success: true,
        latestVersion: info.version || '0.0.0',
        changelog: info.changelog || '',
        publishedAt: info.publishedAt || null,
        targeted: !!targeted
    });
});

// --------------------------------------------------------------
// POST /relay/admin/api/system/publish-version
// Ito ang tinatawag ng developer/owner (manual, hal. gamit ang curl o
// isang admin panel form) tuwing may na-merge/na-deploy na bagong
// upgrade papunta sa mga client repo. Dito lang dapat isulat ang
// bagong version — HINDI ito awtomatikong nade-derive mula sa git,
// dahil sadyang hiwalay ang RELAY (developer-hosted lang) sa git repo
// ng bawat kliyente.
// --------------------------------------------------------------
// --------------------------------------------------------------
// GET /relay/release-package
// BAGO: para sa self-update ng isang KLIYENTENG NAKA-INSTALL NA
// (may sarili nang RELAY_API_KEY) — kaiba ito sa /relay/download/:code
// (na para sa UNANG pag-download bago pa man ma-install ang client).
// Ito ang tinatawag ng OMNIPOS instance mismo (POST /api/system/deploy
// -update sa panig nito, sa "self-update mode" kapag walang Render
// deploy hook na naka-configure, hal. Termux) para kunin ang
// pinaka-bagong omnipos-client.zip at i-apply ito nang lokal.
// Gate lang ito ng x-relay-key (parehong pattern ng /relay/latest
// -version) — hindi kailangan ng requireAllowedDevice dahil parehong
// developer-issued secret naman ang RELAY_API_KEY sa lahat ng
// kliyente, at ang release package mismo ay hindi naman
// client-specific na datos.
// --------------------------------------------------------------
app.get('/relay/release-package', requireApiKey, rateLimit('release-package', 10, 60 * 60 * 1000), (req, res) => {
    if (!fs.existsSync(RELEASE_PACKAGE_PATH)) {
        return res.status(503).json({ success: false, message: 'Walang naka-publish na release package sa RELAY pa.' });
    }
    logActivity(null, 'release_package_self_update_fetch', { ip: req.ip });
    res.download(RELEASE_PACKAGE_PATH, 'omnipos-client.zip');
});

app.post('/relay/admin/api/system/publish-version', requireAdminKey, (req, res) => {
    const { version, changelog, installationId } = req.body || {};
    const trimmedVersion = String(version || '').trim();
    if (!trimmedVersion) {
        return res.status(400).json({ success: false, message: 'Kailangan ang "version" (hal. "1.3.0").' });
    }
    const entry = {
        version: trimmedVersion,
        changelog: String(changelog || '').trim(),
        publishedAt: Date.now()
    };
    // TARGETED PUBLISH: kung may binigay na installationId, dito lang
    // ito ita-tago (targetedReleases) — HINDI apektado ang global
    // systemVersionInfo, kaya ang ibang device lang ang walang
    // makikitang bagong update.
    const targetId = String(installationId || '').trim();
    if (targetId) {
        targetedReleases.set(targetId, entry);
        saveTargetedReleases(targetedReleases);
        return res.json({ success: true, targeted: true, installationId: targetId, release: entry });
    }
    systemVersionInfo = entry;
    saveSystemVersionInfo(systemVersionInfo);
    res.json({ success: true, targeted: false, systemVersionInfo });
});

app.get('/relay/admin/api/system/version', requireAdminKey, (req, res) => {
    res.json({ success: true, systemVersionInfo });
});

// --------------------------------------------------------------
// POST /relay/admin/api/build-release
// AWTOMATIKONG gumagawa ng bagong omnipos-client.zip DIREKTA SA RELAY
// (Render) mismo — WALANG kailangang Termux o kahit anong lokal na
// machine. Ito ang sagot sa "hindi ba pwede sa RELAY nalang gawin
// online yun para ipapasa nalang ang zip sa client":
//
//   1. Git-clone ang OMNIPOS repo mo (kailangang naka-push na muna ang
//      pinaka-bagong bersyon dito — gamit mo lang ang normal na
//      "git push" na dati mong gawi).
//   2. Tanggalin ang mga bagay na HINDI dapat isama (.git, .env,
//      database/, node_modules, logs, patches).
//   3. Gumawa ng BAGONG client .env (RELAY_URL, RELAY_API_KEY, PORT)
//      at ilagay ITO sa loob ng tmpDir bago mag-zip — kaya kapag
//      dina-download na ng bagong kliyente ang zip (sa pamamagitan
//      ng /relay/download/:code), READY NA AGAD ITO — hindi na
//      kailangang gumawa/mag-upload pa ng sariling .env ang kliyente.
//   4. I-zip gamit ang "archiver" (purong Node.js — walang external
//      zip CLI/Termux na kailangan).
//   5. I-save bilang release/omnipos-client.zip — ito na ang
//      awtomatikong maiipasa sa /relay/download/:code mula ngayon.
//
// Body: { repoUrl?, ref?, relayUrl?, relayApiKey?, port? }
//   - repoUrl/ref: kung wala, gagamit ng OMNIPOS_REPO_URL env var
//     (dapat naka-set sa Render dashboard). Kung PRIVATE ang repo,
//     isama ang access token DIREKTA sa URL, hal.:
//       https://<TOKEN>@github.com/iyong-username/OMNIPOS.git
//   - relayUrl: URL na ilalagay sa RELAY_URL ng client .env. Kung
//     wala, gagamit ng RELAY_PUBLIC_URL env var kung naka-set, kung
//     wala rin ay awtomatikong kukunin mula sa kasalukuyang request
//     (req.protocol + req.get('host')).
//   - relayApiKey: kung wala, gagamit ng RELAY_API_KEY na naka-set na
//     dito mismo sa RELAY .env (ito rin ang parehong key na
//     ginagamit ng lahat ng kliyente).
//   - port: default 3000 kung wala.
// --------------------------------------------------------------
// BUG FIX: idinagdag ang 'uploads_tmp' — dating wala ito dito kahit
// kasama na ito sa EXCLUDE set ng OMNIPOS/build-release.js (lokal na
// build script). Hindi tugma ang dalawa: kung sakaling ma-commit ang
// uploads_tmp/ (temp uploaded files — resibo/proof-of-payment photos)
// sa git repo, ma-i-ship ito sa customer release zip dahil hindi ito
// na-filter dati ng remote build endpoint na ito.
// '.start.sh.lock' idinagdag — runtime lock file lang ito na ginagawa
// (at binubura) ng OMNIPOS start.sh/stop.sh habang tumatakbo ang server
// sa device ng kliyente. Hindi ito bahagi ng orihinal na release, kaya
// dapat laging tanggalin/hindi isama sa bawat bagong build-release —
// baka aksidenteng ma-commit/ma-push ito papasok sa package.
// BUG FIX: idinagdag ang '.self-update-backup' — itinugma sa EXCLUDE
// set ng OMNIPOS/build-release.js (parehong dahilan doon: runtime
// backup dir lang ito na ginagawa ng OMNIPOS server.js bago mag-apply
// ng self-update, hindi dapat kasama sa customer release zip).
const BUILD_EXCLUDE_NAMES = new Set([
    '.git', 'node_modules', 'database', 'release', 'uploads_tmp',
    '.start.sh.lock', '.self-update-backup', 'package-lock.json',
    // BUG FIX: itinugma sa EXCLUDE set ng OMNIPOS/build-release.js — dev/
    // build-only tooling na nasasama pala sa omnipos-client.zip dahil wala
    // silang exclude entry dito (Method B remote build): build-release.js /
    // obfuscate-worker.js (ang build script mismo at ang worker nito —
    // nangangailangan ng javascript-obfuscator na hindi naman naka-install
    // sa client, at naglalantad ng obfuscation config/EXCLUDE list),
    // start.sh.bak (stray backup, walang gamit sa client), at vacuum-now.js
    // (developer-only diagnostic CLI — redundant dahil may built-in
    // auto-vacuum na ang server.js pagkatapos ng hard reset).
    'build-release.js', 'obfuscate-worker.js', 'start.sh.bak', 'vacuum-now.js',
]);
const BUILD_EXCLUDE_EXTENSIONS = new Set(['.log', '.patch']);

function removeExcludedRecursive(dir) {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        const fullPath = path.join(dir, entry.name);
        if (entry.name === '.env' || BUILD_EXCLUDE_NAMES.has(entry.name)) {
            fs.rmSync(fullPath, { recursive: true, force: true });
            continue;
        }
        if (entry.isFile() && BUILD_EXCLUDE_EXTENSIONS.has(path.extname(entry.name))) {
            fs.rmSync(fullPath, { force: true });
            continue;
        }
        if (entry.isDirectory()) removeExcludedRecursive(fullPath);
    }
}

// --------------------------------------------------------------
// OBFUSCATION NG RELEASE PACKAGE (gagana lang sa loob ng tmpDir —
// HINDI kailanman ginagalaw ang orihinal na OMNIPOS git repo/main
// branch mo. Isa lang itong "papel" ng temporary na cloned copy na
// buburahin din pagkatapos i-zip.)
//
// Parehong config ito sa build-release.js na hiwalay na binigay para
// sa OMNIPOS repo mismo (para consistent ang behavior/quality), pero
// dito ito tinatawag AWTOMATIKO bawat build-release DITO SA RELAY —
// kaya hindi na kailangan pang mano-manong tumakbo ng `npm run
// build:release` sa panig mo bago mag-deploy.
//
// SYNC FIX: dating hindi kasama dito ang 'mailer.js' at
// 'verify-gmail-connection.js' — dalawang BAGONG first-party server
// file (isolated Gmail SMTP + OAuth/API fallback module, at ang
// standalone CLI verification tool nito, ayon sa hiling na "nakahiwalay
// ang gmail connection Verification nito at kasama sa package") na
// naidagdag na sa OMNIPOS/build-release.js's SERVER_TARGETS pero HINDI
// pa dati dito. Bunga: kapag ang remote/progress-bar na build DITO SA
// RELAY ang ginamit (sa halip na `npm run build:release` nang lokal),
// hindi na-o-obfuscate/naisasama ang dalawang file na iyon sa
// resulting omnipos-client.zip — hindi tugma ang dalawang listahan
// kahit sinasabi ng komentong ito na "dapat laging magkasabay". Ngayon,
// pareho na sila.
// --------------------------------------------------------------
// AUDIT FIX: idinagdag ang 'webauthn.js' (CBOR decode/passkey credential
// verification — security-critical) at 'cloud-snapshot.js' (Postgres
// backup/restore ng buong SQLite DB) — dalawang first-party server module
// na aktwal na ginagamit ng server.js (require('./webauthn'),
// require('./cloud-snapshot')) pero hindi kasama dati dito, kaya naka-
// PLAINTEXT pa rin sila sa bawat na-build na omnipos-client.zip kahit
// obfuscated na ang lahat ng ibang server file. Na-verify: walang
// self-referential/eval na code sa dalawang ito, parehong klase lang ng
// code gaya ng db.js/mailer.js na matagal nang gumagana nang naka-obfuscate.
const RELEASE_SERVER_TARGETS = new Set([
    'server.js',
    'db.js',
    'migrate-to-sqlite.js',
    '_fix_project.js',
    'mailer.js',
    'verify-gmail-connection.js',
    'webauthn.js',
    'cloud-snapshot.js',
]);
// NOTE: public/service-worker.js ay SADYANG HINDI kasama dito. Service
// workers ay may mas mahigpit na execution context kaysa normal na page
// scripts, at ang selfDefending/controlFlowFlattening output ng
// javascript-obfuscator ay may known compatibility issues doon
// (tamper-check code na umaasa sa Function.prototype.toString()
// self-comparisons). Dagdag pa rito, ang string-array shuffling ay
// gumagawa ng byte-different na output KADA BUILD — at ang browser ay
// gumagawa ng byte-for-byte diff ng service-worker.js para malaman kung
// kailangan mag-install ng bagong SW version, kaya laging nagiging
// forced update cycle ang bawat redeploy. Ito ang ugat ng PWA
// install/offline breakage. Maliit lang ang service-worker.js at wala
// namang business logic na kailangang itago, kaya verbatim/plain na
// lang ito kokopyahin (parehong fix gaya ng nasa build-release.js sa
// OMNIPOS repo mismo — dapat laging magkasabay ang dalawang listahan).
const RELEASE_CLIENT_TARGETS = new Set([
    path.join('public', 'app.js'),
    path.join('public', 'bt-printer.js'),
    path.join('public', 'faq-engine.js'),
    path.join('public', 'faq-knowledge.js'),
]);
const RELEASE_ENV_LOADER_FILENAME = 'env-loader.js';
const RELEASE_ENV_KEY_FILENAME = '.env.key';

// public/index.html — dito ilalagay ang comment-stripping AT
// pag-obfuscate ng anumang inline <script> na laman nito (hiwalay
// sa mga hiwalay na .js file na SERVER/CLIENT_TARGETS).
// AUDIT FIX: idinagdag ang 'public/customer-display.html' — may sarili
// itong ~200+ linyang inline <script> (loyalty display, idle rotation,
// pagpoproseso ng data mula sa BroadcastChannel) na hindi kasama dati
// dito, kaya kumpletong readable/hindi obfuscated pa rin ito sa bawat
// na-build na zip kahit na-obfuscate na ang index.html. Na-verify: walang
// onclick/inline HTML attribute na umaasa sa pangalan ng function (lahat
// addEventListener-based sa loob mismo ng script), at walang
// server-side templating dito (static file lang) — ligtas itong
// idagdag gamit ang parehong obfuscation path ng index.html.
const RELEASE_HTML_TARGETS = new Set([
    path.join('public', 'index.html'),
    path.join('public', 'customer-display.html'),
]);

// Third-party CSS na dapat HUWAG galawin — kasama ang mga license/
// attribution header comment nito (hal. "/*! Font Awesome Free ...
// License ...") na kinakailangan panatilihin ayon sa lisensya nila.
const THIRD_PARTY_CSS = new Set([
    path.join('public', 'fontawesome.min.css'),
    path.join('public', 'css', 'all.css'),
    path.join('public', 'css', 'all.min.css'),
    path.join('public', 'css', 'brands.css'),
    path.join('public', 'css', 'brands.min.css'),
    path.join('public', 'css', 'fontawesome.css'),
    path.join('public', 'css', 'fontawesome.min.css'),
    path.join('public', 'css', 'regular.css'),
    path.join('public', 'css', 'regular.min.css'),
    path.join('public', 'css', 'solid.css'),
    path.join('public', 'css', 'solid.min.css'),
    path.join('public', 'css', 'svg-with-js.css'),
    path.join('public', 'css', 'svg-with-js.min.css'),
    path.join('public', 'css', 'v4-font-face.css'),
    path.join('public', 'css', 'v4-font-face.min.css'),
    path.join('public', 'css', 'v4-shims.css'),
    path.join('public', 'css', 'v4-shims.min.css'),
    path.join('public', 'css', 'v5-font-face.css'),
    path.join('public', 'css', 'v5-font-face.min.css'),
]);

const releaseServerObfOptions = {
    compact: true,
    target: 'node',
    controlFlowFlattening: true,
    controlFlowFlatteningThreshold: 0.4,
    deadCodeInjection: true,
    deadCodeInjectionThreshold: 0.15,
    debugProtection: false,
    disableConsoleOutput: false,
    identifierNamesGenerator: 'hexadecimal',
    numbersToExpressions: true,
    renameGlobals: false,
    selfDefending: true,
    simplify: true,
    splitStrings: true,
    splitStringsChunkLength: 12,
    stringArray: true,
    stringArrayEncoding: ['base64'],
    stringArrayThreshold: 0.75,
    transformObjectKeys: true,
    unicodeEscapeSequence: false,
};
const releaseClientObfOptions = {
    ...releaseServerObfOptions,
    target: 'browser',
    controlFlowFlatteningThreshold: 0.3,
    deadCodeInjectionThreshold: 0.1,
};

// PERFORMANCE FIX: control-flow flattening and dead-code injection cost
// grow much faster than linearly with file size. server.js and
// public/app.js are each several hundred KB, and running them through
// the same thresholds as small files is the main reason build-release
// used to take a very long time (and, on a CPU-constrained host like a
// Render free instance, block the process long enough to look "stuck").
// Files at or above this size get scaled-down thresholds instead —
// still fully obfuscated (renaming, string array encoding, dead code,
// self-defending all stay on), just without the worst-case blow-up.
const LARGE_FILE_BYTES = 100 * 1024; // 100KB
const largeServerObfOptions = {
    ...releaseServerObfOptions,
    controlFlowFlatteningThreshold: 0.1,
    deadCodeInjectionThreshold: 0.04,
};
const largeClientObfOptions = {
    ...releaseClientObfOptions,
    controlFlowFlatteningThreshold: 0.08,
    deadCodeInjectionThreshold: 0.03,
};

function pickReleaseObfOptions(fullPath, isClient) {
    const isLarge = fs.statSync(fullPath).size >= LARGE_FILE_BYTES;
    if (isClient) return isLarge ? largeClientObfOptions : releaseClientObfOptions;
    return isLarge ? largeServerObfOptions : releaseServerObfOptions;
}

// ROOT CAUSE FIX (progress bar "freezes"/stops updating mid-build):
// JavaScriptObfuscator.obfuscate() is a heavy, fully SYNCHRONOUS, CPU-bound
// call — for a large file (server.js is 400+KB, OMNIPOS's public/app.js is
// even bigger) with controlFlowFlattening/deadCodeInjection/selfDefending
// all turned on, a single call can take many seconds. Because it runs on
// RELAY's one and only main thread, it blocks the ENTIRE Node.js event
// loop for that whole stretch — so GET /relay/admin/api/build-progress
// (the request the admin panel is polling every 700ms) can't even be
// answered until the big file finishes, which is exactly what makes the
// progress bar look "stuck"/frozen instead of just moving slowly. The
// existing `await yieldToEventLoop()` between files (setImmediate) did
// NOT help with this, because it only yields BETWEEN files — it can't
// yield in the middle of one single obfuscate() call.
//
// FIX: run the actual obfuscation inside a worker_threads Worker (same
// pattern already used by the local/offline OMNIPOS/build-release.js +
// OMNIPOS/obfuscate-worker.js for this exact reason) so the CPU-heavy work
// happens on a separate thread — RELAY's main thread (and therefore the
// build-progress polling route) stays free and responsive the whole time,
// even while obfuscating the biggest files.
function obfuscateInWorker({ code, srcPath, destPath, options }) {
    return new Promise((resolve, reject) => {
        const worker = new Worker(path.join(__dirname, 'obfuscate-worker.js'), {
            workerData: { code, srcPath, destPath, options }
        });
        worker.on('message', (msg) => {
            if (msg && msg.ok) resolve(msg.obfuscatedCode);
            else reject(new Error((msg && msg.error) || 'Nabigo ang obfuscate worker.'));
        });
        worker.on('error', reject);
        worker.on('exit', (exitCode) => {
            if (exitCode !== 0) reject(new Error(`Ang obfuscate worker para sa ${srcPath || '(inline script)'} ay lumabas nang code ${exitCode}.`));
        });
    });
}

// Drop-in async replacement para sa dating obfuscateFileInPlace() —
// pareho pa rin ang ginagawa (basahin ang file, obfuscate, isulat pabalik
// sa parehong path), pero sa loob ng worker thread na tumatakbo (tingnan
// ang komento sa obfuscateInWorker() sa itaas).
async function obfuscateFileInPlace(fullPath, options) {
    await obfuscateInWorker({ srcPath: fullPath, destPath: fullPath, options });
}

// Yields control back to the event loop. Used between obfuscation
// passes so a long build doesn't fully block RELAY from answering other
// requests (health checks, status polling, other terminals' API calls)
// for its entire duration.
function yieldToEventLoop() {
    return new Promise((resolve) => setImmediate(resolve));
}

// Tinatanggal ang lahat ng <!-- ... --> na comment sa isang HTML file.
function stripHtmlComments(html) {
    return html.replace(/<!--[\s\S]*?-->/g, '');
}

// Tinatanggal ang lahat ng /* ... */ na comment sa isang CSS file.
function stripCssComments(css) {
    return css.replace(/\/\*[\s\S]*?\*\//g, '');
}

// Hinahanap at ino-obfuscate ang laman ng anumang INLINE <script> (walang
// src="...") sa isang HTML file — dito rin daanan ang comments/logic na
// direktang nakasulat sa index.html mismo, hindi lang sa hiwalay na .js
// files. Ang mga <script src="..."> (external references) ay HINDI
// hinahawakan dito.
// NOTE: dating gumagamit ito ng String.replace() na may SYNCHRONOUS
// callback (JavaScriptObfuscator.obfuscate() diretso sa main thread) —
// kasama ito sa root cause ng "nag-fi-freeze ang progress bar" (tingnan
// ang malaking komento sa obfuscateInWorker() sa itaas). String.replace()
// mismo ay hindi kayang mag-await ng async callback, kaya dito, unang
// kinokolekta ang lahat ng match (regex.exec loop) bago i-obfuscate ang
// bawat isa nang paisa-isa sa loob ng worker thread (await sa bawat isa),
// saka lang muling pinagsasama-sama ang buong HTML string.
async function obfuscateHtmlInlineScripts(html, options) {
    const re = /<script(\s[^>]*)?>([\s\S]*?)<\/script>/gi;
    const matches = [];
    let m;
    while ((m = re.exec(html)) !== null) {
        const attrStr = m[1] || '';
        const content = m[2];
        const isExternal = /\bsrc\s*=/i.test(attrStr);
        const isEmpty = !content.trim();
        matches.push({ index: m.index, fullMatch: m[0], attrStr, content, skip: isExternal || isEmpty });
    }

    let result = '';
    let cursor = 0;
    for (const match of matches) {
        result += html.slice(cursor, match.index);
        if (match.skip) {
            result += match.fullMatch;
        } else {
            const obfuscated = await obfuscateInWorker({ code: match.content, options });
            result += `<script${match.attrStr}>${obfuscated}</script>`;
        }
        cursor = match.index + match.fullMatch.length;
    }
    result += html.slice(cursor);
    return result;
}

// Collects every file that needs obfuscating/stripping first (cheap,
// synchronous directory walk), then processes that flat list with an
// event-loop yield between each entry. Splitting "plan" from "execute"
// like this is what lets us yield between files below.
function planReleaseTree(tmpDir) {
    const items = [];

    function walk(dir, baseRel) {
        for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
            const rel = path.join(baseRel, entry.name);
            const full = path.join(dir, entry.name);
            if (entry.isDirectory()) {
                walk(full, rel);
                continue;
            }
            // env-loader.js is already handled by encryptClientEnvAndPatchLoader().
            if (rel === RELEASE_ENV_LOADER_FILENAME) continue;

            if (RELEASE_SERVER_TARGETS.has(rel)) {
                items.push({ type: 'server', full });
            } else if (RELEASE_CLIENT_TARGETS.has(rel)) {
                items.push({ type: 'client', full });
            } else if (RELEASE_HTML_TARGETS.has(rel)) {
                items.push({ type: 'html', full });
            } else if (rel.toLowerCase().endsWith('.css') && !THIRD_PARTY_CSS.has(rel)) {
                items.push({ type: 'css', full });
            }
        }
    }

    walk(tmpDir, '');
    return items;
}

// shouldObfuscate (default true — dating default/pre-existing na
// behavior kung walang ipinasa/reqBody.obfuscate ay undefined, para
// walang masirang existing na build/publish flow o API caller na hindi
// pa alam sa bagong option na ito): kapag false, LILIPAT LANG ang mga
// server/client JS at inline <script> nang hindi dumadaan sa
// JavaScriptObfuscator (walang rename/string-encode/dead-code) — pero
// tuloy pa rin ang comment-stripping ng CSS/HTML (cosmetic lang,
// hindi nakakaapekto sa functionality) para pareho pa rin ang laki/
// package structure ng resulta.
async function obfuscateReleaseTree(tmpDir, onProgress, shouldObfuscate = true) {
    const items = planReleaseTree(tmpDir);
    const totalItems = items.length;
    let obfuscatedCount = 0;

    for (const { type, full } of items) {
        if (type === 'server') {
            if (shouldObfuscate) await obfuscateFileInPlace(full, pickReleaseObfOptions(full, false));
        } else if (type === 'client') {
            if (shouldObfuscate) await obfuscateFileInPlace(full, pickReleaseObfOptions(full, true));
        } else if (type === 'html') {
            let html = fs.readFileSync(full, 'utf8');
            // FIX: inline-script obfuscation used to run BEFORE comment
            // stripping — so an HTML comment that merely mentioned the
            // literal text "<script>" in its prose (e.g. a comment
            // explaining something about script tags) would confuse the
            // simple regex-based scanner below (obfuscateHtmlInlineScripts):
            // it would mistake that for a real opening tag and treat
            // everything from there up to the NEXT real </script> —
            // including the rest of the comment text — as "JS content",
            // then try to parse that as JavaScript (the source of the
            // "Unexpected token" error). Stripping comments before
            // scanning for script tags removes any comment text that
            // could confuse the scanner.
            html = stripHtmlComments(html);
            if (shouldObfuscate) html = await obfuscateHtmlInlineScripts(html, pickReleaseObfOptions(full, true));
            fs.writeFileSync(full, html, 'utf8');
        } else if (type === 'css') {
            const css = fs.readFileSync(full, 'utf8');
            fs.writeFileSync(full, stripCssComments(css), 'utf8');
        }
        // Bilang ng na-PROCESS (hindi lang "na-obfuscate") na file —
        // ginagamit pa rin ito para sa progress bar % kahit naka-off
        // ang obfuscation, dahil dumadaan pa rin ang bawat file dito.
        obfuscatedCount += 1;
        if (typeof onProgress === 'function') {
            try { onProgress(obfuscatedCount, totalItems); } catch (_) { /* huwag hayaang masira ng UI callback ang build */ }
        }
        // Give RELAY a chance to answer other requests (status polling,
        // health checks, other terminals) between each file instead of
        // holding the event loop for the whole build in one unbroken
        // stretch.
        await yieldToEventLoop();
    }

    return obfuscatedCount;
}

// Ini-encrypt ang client .env (AES-256-GCM) at isusulat ang key sa
// sarili niyang ".env.key" file (KATABI ng .env) — HINDI na ito
// ibinabake diretso sa code ng env-loader.js. FIX ito para sa
// "device revoked after self-update" bug: dati, bawat build ay may
// BAGONG random key na naka-embed sa loader mismo, pero self-update
// ay PRESERVED ang .env ng client (hindi ito nagbabago) habang
// PINAPALITAN ang env-loader.js (may BAGONG di-tugmang key) — kaya
// laging nabibigo ang decrypt pagkatapos ng unang self-update. Ngayon,
// preserved din ang .env.key kasabay ng .env sa self-update (tingnan
// ang SELF_UPDATE_PRESERVE sa OMNIPOS/server.js), kaya magkatugma pa
// rin sila kahit ilang beses pang mag-rebuild ang loader code.
// Kung walang env-loader.js sa cloned repo (hal. hindi mo pa na-commit),
// babalik lang ito sa dating plaintext .env — walang masisira, pero
// mananatiling readable ang .env sa ganitong kaso.
async function encryptClientEnvAndPatchLoader(tmpDir, envContent) {
    const loaderPath = path.join(tmpDir, RELEASE_ENV_LOADER_FILENAME);

    if (!fs.existsSync(loaderPath)) {
        fs.writeFileSync(path.join(tmpDir, '.env'), envContent);
        return { encrypted: false };
    }

    const key = crypto.randomBytes(32);
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
    const encrypted = Buffer.concat([cipher.update(envContent, 'utf8'), cipher.final()]);
    const tag = cipher.getAuthTag();

    const payload = {
        iv: iv.toString('hex'),
        tag: tag.toString('hex'),
        data: encrypted.toString('base64'),
    };
    fs.writeFileSync(path.join(tmpDir, '.env'), JSON.stringify(payload));
    fs.writeFileSync(path.join(tmpDir, RELEASE_ENV_KEY_FILENAME), key.toString('hex'), 'utf8');

    await obfuscateFileInPlace(loaderPath, releaseServerObfOptions);

    return { encrypted: true };
}

// ----------------------------------------------------------------
// LIVE BUILD PROGRESS (visual %) — walang idinagdag na disk/network
// I/O, isang plain in-memory object lang ito na ina-update sa bawat
// hakbang ng build (clone → filter → env-encrypt → obfuscate → zip →
// tapos). Ang frontend ay pina-poll lang ito paminsan-minsan
// (GET, mabilis, walang ginagawang mabigat na trabaho) habang
// naka-disable ang "I-build ang Release" button — hindi ito
// nagpapabagal at hindi nakakaapekto sa aktwal na proseso ng build
// mismo, isang beses lang bawat build ang tatakbo (single global
// state ang sapat dahil isang admin session/build lang ang
// inaasahang aktibo sa isang pagkakataon).
// AYOS (hiling): "hatiin ang 100% base sa bilang ng sunod-sunod na
// function/hakbang" ng build, sa halip na basta-basta lang na mga
// percent kada hakbang. Dito, tinutukoy ang pagkakasunod-sunod ng mga
// hakbang ng "I-build ang Release" (BUILD_STEPS): unang hakbang (clone)
// ay bigyan ng FIXED na 10% (gaya ng eksaktong hiniling — kaagad itong
// tumatalon sa 10% pagkatapos matapos ang clone), at ang NATITIRANG 90%
// ay pantay-pantay na hinahati sa bilang ng mga SUMUSUNOD na hakbang
// (filter, env, obfuscate, zip = 4 hakbang → 22.5% bawat isa). Kung sa
// hinaharap ay may idagdag/tanggal na hakbang sa BUILD_STEPS, awtomatiko
// na lang muling maghahati ang formula na ito — hindi na kailangang
// i-adjust ang mga percent nang manu-mano bawat pagbabago.
// BUG FIX: idinagdag ang 'verify' bilang huling hakbang — ang
// integrity-check (unzip -tq) at atomic rename() na idinagdag para sa
// corrupted-download fix (tingnan ang RELEASE_PACKAGE_TMP_PATH sa
// itaas) ay may sariling oras na kinukuha (lalo na ang unzip -tq sa
// isang zip na ilang MB), pero dati ay wala itong sariling stage/%
// window — natatago lang ito sa pagitan ng 'zip' pagtapos (na
// tumatama na sa 100%) at ng 'done'. Ibig sabihin, kung sakaling
// tumagal ang verify+rename step, mukhang "nakatigil sa 100%" ang
// progress bar sa mata ng admin bago pa talaga tapos — hindi tugma sa
// totoong estado. Ngayon, may sarili nang % allocation ang hakbang na
// ito, kaya tumpak ang display sa buong proseso.
const BUILD_STEPS = ['clone', 'filter', 'env', 'obfuscate', 'zip', 'verify'];
const BUILD_FIRST_STEP_PERCENT = 10;
const BUILD_STEP_END_PERCENT = (() => {
    const map = {};
    const remainingSteps = BUILD_STEPS.length - 1;
    const perRemainingStep = remainingSteps > 0 ? (100 - BUILD_FIRST_STEP_PERCENT) / remainingSteps : 0;
    let cumulative = 0;
    BUILD_STEPS.forEach((step, idx) => {
        cumulative = idx === 0 ? BUILD_FIRST_STEP_PERCENT : cumulative + perRemainingStep;
        map[step] = Math.round(cumulative * 10) / 10; // 1 decimal na lang para malinis ang display
    });
    return map;
})();

function buildStepStartPercent(step) {
    const idx = BUILD_STEPS.indexOf(step);
    return idx <= 0 ? 0 : BUILD_STEP_END_PERCENT[BUILD_STEPS[idx - 1]];
}

let buildProgressState = {
    buildId: null,
    stage: 'idle',       // idle | clone | filter | env | obfuscate | zip | done | error
    percent: 0,
    message: '',
    startedAt: null,
    updatedAt: null,
    done: true,
    error: null,
    etaMs: null
};

// --------------------------------------------------------------
// ETA (time remaining) ESTIMATE — hiling: gusto makita ng admin ang
// "may time remaining" habang umuusad ang build, hindi lang bare
// percent. In-memory lang ito (nare-reset sa bawat restart ng RELAY —
// tama lang, cosmetic estimate lang ito, hindi kailangang i-persist),
// isang rolling average ng huling 5 MATAGUMPAY na FULL build duration.
// Bago pa magkaroon ng history (o pagkatapos lang ng restart), gumagamit
// muna ng DEFAULT_ESTIMATED_BUILD_MS bilang unang tantiya.
// --------------------------------------------------------------
let recentBuildDurationsMs = [];
const DEFAULT_ESTIMATED_BUILD_MS = 45 * 1000;

function recordBuildDurationForEta(ms) {
    if (typeof ms === 'number' && ms > 0) {
        recentBuildDurationsMs.push(ms);
        if (recentBuildDurationsMs.length > 5) recentBuildDurationsMs.shift();
    }
}

function estimatedTotalBuildMs() {
    if (recentBuildDurationsMs.length === 0) return DEFAULT_ESTIMATED_BUILD_MS;
    const sum = recentBuildDurationsMs.reduce((a, b) => a + b, 0);
    return Math.round(sum / recentBuildDurationsMs.length);
}

function estimateRemainingMs(percent, startedAt) {
    if (!startedAt) return null;
    const elapsed = Date.now() - startedAt;
    const pct = Math.max(0, Math.min(100, percent || 0));
    // Sa simula pa lang (napakababa ang percent), hindi pa maaasahan ang
    // "elapsed * (100-pct)/pct" na formula (halos hatiin-by-zero) — gamitin
    // muna ang rolling average ng nakaraang buo-build bilang unang tantiya.
    if (pct < 3) return Math.max(0, estimatedTotalBuildMs() - elapsed);
    const projectedTotal = elapsed * (100 / pct);
    // Panatilihing makatwiran: huwag hayaang mas maikli pa sa aktwal na
    // elapsed na, at i-blend nang bahagya sa historical average para hindi
    // masyadong "jumpy" ang display kada tick.
    const blended = recentBuildDurationsMs.length > 0
        ? (projectedTotal * 0.7) + (estimatedTotalBuildMs() * 0.3)
        : projectedTotal;
    return Math.max(0, Math.round(blended - elapsed));
}

function setBuildProgress(patch) {
    buildProgressState = { ...buildProgressState, ...patch, updatedAt: Date.now() };
    buildProgressState.etaMs = buildProgressState.done
        ? 0
        : estimateRemainingMs(buildProgressState.percent, buildProgressState.startedAt);
}

app.get('/relay/admin/api/build-progress', requireAdminKey, (req, res) => {
    res.json({ success: true, progress: buildProgressState });
});

// --------------------------------------------------------------
// performBuildRelease(reqBody, req) — ang aktwal na build logic,
// hiwalay na function para magamit ng DALAWANG route: (1) yung
// dating '/relay/admin/api/build-release' (build lang, walang
// publish), at (2) yung bagong '/relay/admin/api/system/publish-release'
// (build + publish-version sa IISANG request/tawag — para hindi na
// kailangan pang dalawahin ang curl/HTTP Shortcut sa Termux).
// Nagba-throw ito ng Error sa sinumang caller kapag nabigo — ang
// route handlers na ang bahalang mag-catch at mag-format ng sagot.
// --------------------------------------------------------------
async function performBuildRelease(reqBody, req, publishOverride) {
    // ROOT CAUSE FIX (SerpAPI key laging blangko sa bagong build kahit
    // "naka-set" na ito sa .env): si process.loadEnvFile() sa itaas ng file
    // ay TUMATAKBO LANG NANG ISANG BESES — sa mismong pag-boot ng RELAY
    // process. Ibig sabihin, kapag nag-edit ka ng .env (hal. nagdagdag ng
    // IMAGE_SEARCH_PROVIDER/IMAGE_SEARCH_API_KEY) HABANG tumatakbo na ang
    // RELAY, hindi talaga nag-uupdate ang process.env — nananatili itong
    // "blangko"/wala hangga't hindi mo ni-restart ang buong RELAY process.
    // Ang naunang "BUG FIX" sa IMAGE_SEARCH_* sa ibaba (na direktang
    // process.env.IMAGE_SEARCH_* na ang binabasa sa halip na cached
    // constants) ay HINDI pa rin nagre-resolve nito, dahil parehong
    // process.env ang pinagmumulan ng dalawa — kung stale/wala ang
    // process.env, mananatiling stale/wala rin ang binabasa nito kahit
    // "direkta" na ang pagbasa. Dito, muling tinatawag ang
    // process.loadEnvFile() sa MISMONG ORAS ng build/publish — hindi
    // ino-overwrite nito ang mga existing na key sa process.env (per Node
    // docs), dagdag lang ito ng mga BAGONG idinagdag na key mula sa .env
    // file, kaya kahit hindi pa na-restart ang RELAY, agad nasusundan ng
    // susunod na build/publish ang bagong SerpAPI key (o anumang bagong env
    // var) na kadarating lang idagdag sa .env.
    try {
        // Tingnan ang paliwanag sa unang process.loadEnvFile() call malapit
        // sa itaas ng file na ito: kailangan ng EXPLICIT na path
        // (path.join(__dirname, '.env')), hindi ang default/no-argument na
        // bersyon — dahil ang default ay humahanap batay sa process.cwd()
        // (posibleng ibang direktoryo kaysa dito ito pinatakbo), hindi sa
        // aktwal na lokasyon ng file na ito. Ito talaga ang dahilan kaya
        // "blangko" pa rin ang nakukuhang IMAGE_SEARCH_* (o anumang bagong
        // idinagdag na env var) sa bagong build kahit tama na ang laman ng
        // .env at kahit muling tinawag na ang loadEnvFile() dito.
        process.loadEnvFile(path.join(__dirname, '.env'));
    } catch (err) {
        // Walang nakitang .env file o hindi supported ng Node version na ito
        // ang loadEnvFile() — okay lang, babalik na lang sa mga value na
        // nasa process.env na mula sa pag-boot (o sa env vars ng hosting
        // dashboard).
    }

    const repoUrl = (reqBody && reqBody.repoUrl) || process.env.OMNIPOS_REPO_URL;
    const ref = (reqBody && reqBody.ref) || 'main';
    // Optional na maikling paglalarawan ng build na ito (hal. "hotfix
    // barcode scan bug"). Puro history/log lang ang gamit nito — hindi
    // ito nakakaapekto sa aktwal na filename ng na-build na zip.
    const caption = String((reqBody && reqBody.caption) || '').trim();
    // OBFUSCATE TOGGLE (bago): checkbox sa "Build Release" at "Publish a
    // New Update" cards sa admin UI — default TRUE (dating behavior)
    // kapag walang ipinasang reqBody.obfuscate, para walang masirang
    // luma nang caller/flow. Explicit lang na false (mula sa unchecked
    // na checkbox) ang mag-o-off nito.
    const shouldObfuscate = !(reqBody && (reqBody.obfuscate === false || reqBody.obfuscate === 'false' || reqBody.obfuscate === 0 || reqBody.obfuscate === '0'));

    if (!repoUrl) {
        const err = new Error('Walang repoUrl na ibinigay at walang OMNIPOS_REPO_URL env var na naka-set.');
        err.statusCode = 400;
        throw err;
    }

    const tmpDir = path.join(os.tmpdir(), `omnipos-build-${Date.now()}`);
    const buildId = crypto.randomBytes(6).toString('hex');
    setBuildProgress({ buildId, stage: 'clone', percent: 0, message: 'Kino-clone ang repo...', startedAt: Date.now(), done: false, error: null });

    try {
        // LIVE CLONE PROGRESS (hiling): dati, execFileSync (SYNCHRONOUS —
        // hinaharang nito ang BUONG event loop ng RELAY habang nagkoclo-clone,
        // kaya wala ring ibang request — kasama ang integrity-checkin ng
        // real-time watcher ng mga OMNIPOS client — ang naseserve habang
        // tumatagal ang clone), at walang paraan para malaman ang % habang
        // tumatakbo ito — kaya nakikita LANG ang biglaang tumalon mula 0%
        // papuntang 10% pagkatapos lang matapos ang buong clone.
        //
        // Ngayon, ginagamit ang spawn() (NON-blocking — hindi na nire-freeze
        // ang server habang tumatagal ang clone) kasabay ng --progress flag
        // ng git mismo, na nagpapadala ng TUNAY (hindi peke/pasadyang
        // pagtaas) na percentage papunta sa stderr habang tumatakbo ito
        // (hal. "Receiving objects: 45% (450/1000)"). Ino-parse dito ang
        // mga linyang iyon at isinasalin sa loob ng saklaw ng "clone" step
        // (0% hanggang BUILD_FIRST_STEP_PERCENT), kaya makikita ang aktwal
        // na pag-usad bawat segundo sa halip na nakatigil/naka-freeze.
        //
        // WALANG epekto ito sa aktwal na resulta ng clone — parehong git
        // clone --depth 1 --branch <ref> pa rin ito, ARGUMENT ARRAY pa rin
        // (hindi shell string) para ligtas pa rin sa shell-injection.
        await new Promise((resolve, reject) => {
            const gitArgs = ['clone', '--depth', '1', '--progress', '--branch', ref, repoUrl, tmpDir];
            const child = spawn('git', gitArgs, { stdio: ['ignore', 'ignore', 'pipe'] });
            let stderrTail = '';
            child.stderr.on('data', (chunk) => {
                stderrTail = (stderrTail + chunk.toString()).slice(-4000);
                // Git prints progress lines separated by \r (carriage return,
                // hindi \n) habang umuusad — hatiin sa parehong dalawa para
                // makuha ang pinakabagong linya.
                const lines = stderrTail.split(/[\r\n]+/).filter(Boolean);
                const lastLine = lines[lines.length - 1] || '';
                const match = lastLine.match(/(\d{1,3})%/);
                if (match) {
                    const rawPct = Math.max(0, Math.min(100, Number(match[1])));
                    const scaled = Math.round((rawPct / 100) * BUILD_FIRST_STEP_PERCENT * 10) / 10;
                    setBuildProgress({ stage: 'clone', percent: scaled, message: `Kino-clone ang repo... (${lastLine.trim()})` });
                }
            });
            child.on('error', reject);
            child.on('close', (code) => {
                if (code === 0) resolve();
                else reject(new Error(`git clone exited with code ${code}`));
            });
        });

        setBuildProgress({ stage: 'filter', percent: BUILD_STEP_END_PERCENT.clone, message: 'Tinatanggal ang mga excluded file...' });
        removeExcludedRecursive(tmpDir);

        // BUG FIX (root cause ng "Waiting for the New Version..." na
        // nakatigil habang paikot-ikot ang poll): ang version na ipina-
        // publish (systemVersionInfo.version / publishOverride.version,
        // sinasagot ng /relay/latest-version) at ang aktwal na
        // package.json "version" na naka-bake sa loob ng ipinapadalang
        // zip (binabasa ng client bilang APP_VERSION pagkatapos ng
        // self-update) ay DALAWANG HIWALAY na bagay dati — ang isa ay
        // manu-manong tinatype ng admin sa publish form, ang isa naman
        // ay kung anuman ang laman ng package.json sa cloned repo/ref.
        // Walang code na nagsi-sync sa dalawa. Kapag hindi tugma ang mga
        // ito (typo, o hindi na-bump ang package.json bago mag-push),
        // matagumpay pa rin ang self-update sa likod ng eksena, pero ang
        // pollForDeployCompletion() sa app.js ay walang katapusang
        // maghihintay dahil ang bagong result.currentVersion (mula sa
        // bagong package.json) ay hindi na kailanman magiging pareho sa
        // targetVersion (ang dating-ipina-publish na string) — kaya
        // "nakatigil" ang modal kahit tapos na talaga ang deploy.
        // AYOS: dito mismo i-patch ang package.json sa loob ng tmpDir
        // (staging copy lang — hindi nagagalaw ang orihinal na git repo)
        // para eksaktong tumugma ang "version" field nito sa version na
        // talagang ipapa-publish ng build na ito — parehong pinagmulan
        // (single source of truth) na ang dalawa mula ngayon.
        const resolvedVersion = (publishOverride && publishOverride.version) || systemVersionInfo.version || '0.0.0';
        try {
            const pkgPath = path.join(tmpDir, 'package.json');
            if (fs.existsSync(pkgPath)) {
                const pkgJson = JSON.parse(fs.readFileSync(pkgPath, 'utf8'));
                if (pkgJson.version !== resolvedVersion) {
                    console.log(`ℹ️  Pinapatch ang package.json version mula "${pkgJson.version}" papuntang "${resolvedVersion}" (para tumugma sa ipapa-publish na version).`);
                }
                pkgJson.version = resolvedVersion;
                fs.writeFileSync(pkgPath, JSON.stringify(pkgJson, null, 2) + '\n', 'utf8');
            } else {
                console.warn('⚠️  Walang package.json sa cloned repo — hindi na-patch ang version (magiging mismatch ang client APP_VERSION kung may laman itong default).');
            }
        } catch (pkgErr) {
            console.warn(`⚠️  Hindi ma-patch ang package.json version: ${pkgErr.message}`);
        }

        // Gumawa ng client .env DIREKTA sa loob ng tmpDir bago mag-zip,
        // para READY NA AGAD ang zip pagka-download ng bagong kliyente
        // (walang kailangan pang gawin/i-upload na .env sa panig nila).
        const relayUrl = (reqBody && reqBody.relayUrl)
            || process.env.RELAY_PUBLIC_URL
            || `${req.protocol}://${req.get('host')}`;
        const relayApiKey = (reqBody && reqBody.relayApiKey) || process.env.RELAY_API_KEY;
        const clientPort = (reqBody && reqBody.port) || 3000;

        if (!relayApiKey) {
            throw new Error('Walang RELAY_API_KEY na naka-set (ni sa request body ni sa RELAY .env) — hindi makakagawa ng client .env.');
        }

        // SerpAPI (Product Image Search) — isinasama rin AGAD sa bagong
        // client .env, gamit ang parehong key na naka-set na sa RELAY
        // (IMAGE_SEARCH_API_KEY env var), pwede ring i-override per-build
        // gamit ang reqBody — kapareho ng pattern ng relayUrl/relayApiKey/
        // port sa itaas. Kung wala namang naka-configure kahit saan
        // (reqBody o RELAY .env), hinahayaan lang itong blangko sa client
        // .env — normal namang gumagana pa rin ang OMNIPOS nang walang ito
        // (walang "Search Image" button lang, tingnan ang OMNIPOS .env
        // comments), kaya hindi ito dapat mag-fail ng buong build.
        //
        // BUG FIX: direktang process.env.IMAGE_SEARCH_* na ang binabasa
        // dito (hindi na ang dating cached DEFAULT_IMAGE_SEARCH_* na
        // constant sa itaas), kapareho ng ginagawa na ng relayApiKey sa
        // itaas — para laging pinakabagong value ang nasusunod, kahit
        // idinagdag/binago mo lang ang key HABANG tumatakbo na ang RELAY
        // process (walang kailangan pang i-restart ang RELAY bago ito
        // mapulot ng susunod na build/publish).
        const imageSearchProvider = (reqBody && reqBody.imageSearchProvider) || process.env.IMAGE_SEARCH_PROVIDER || '';
        const imageSearchApiKey = (reqBody && reqBody.imageSearchApiKey) || process.env.IMAGE_SEARCH_API_KEY || '';
        const imageSearchCx = (reqBody && reqBody.imageSearchCx) || process.env.IMAGE_SEARCH_CX || '';

        const clientEnvContent = [
            `RELAY_URL=${relayUrl}`,
            `RELAY_API_KEY=${relayApiKey}`,
            `PORT=${clientPort}`,
            `IMAGE_SEARCH_PROVIDER=${imageSearchProvider}`,
            `IMAGE_SEARCH_API_KEY=${imageSearchApiKey}`,
            `IMAGE_SEARCH_CX=${imageSearchCx}`,
            ''
        ].join('\n');
        setBuildProgress({ stage: 'env', percent: BUILD_STEP_END_PERCENT.filter, message: 'Ini-encrypt ang client .env...' });
        const envResult = await encryptClientEnvAndPatchLoader(tmpDir, clientEnvContent);

        // Obfuscate OMNIPOS's own server-side/client-side JS here, inside
        // tmpDir only (a staging copy) — this never touches your original
        // git repo/main branch. Ipinapasa ang isang progress callback para
        // ma-update ang % LIVE bawat file (ito karaniwan ang pinakamatagal
        // na hakbang ng buong build), gamit ang existing per-file loop —
        // walang dagdag na I/O, text-lang na update sa in-memory object.
        setBuildProgress({ stage: 'obfuscate', percent: BUILD_STEP_END_PERCENT.env, message: shouldObfuscate ? 'Ino-obfuscate ang mga file...' : 'Inihahanda ang mga file (obfuscation OFF)...' });
        const obfuscateStart = BUILD_STEP_END_PERCENT.env;
        const obfuscateEnd = BUILD_STEP_END_PERCENT.obfuscate;
        const obfuscatedCount = await obfuscateReleaseTree(tmpDir, (done, total) => {
            const pct = total > 0 ? obfuscateStart + ((done / total) * (obfuscateEnd - obfuscateStart)) : obfuscateStart;
            const msg = shouldObfuscate ? `Ino-obfuscate ang mga file... (${done}/${total})` : `Inihahanda ang mga file (obfuscation OFF)... (${done}/${total})`;
            setBuildProgress({ stage: 'obfuscate', percent: Math.round(pct * 10) / 10, message: msg });
        }, shouldObfuscate);

        setBuildProgress({ stage: 'zip', percent: BUILD_STEP_END_PERCENT.obfuscate, message: 'Ginagawa ang zip package...' });
        const releaseDir = path.dirname(RELEASE_PACKAGE_PATH);
        if (!fs.existsSync(releaseDir)) fs.mkdirSync(releaseDir, { recursive: true });

        const zipStepStart = BUILD_STEP_END_PERCENT.obfuscate;
        // BUG FIX: ang 'zip' ay HINDI na ang huling hakbang sa
        // BUILD_STEPS ngayon (may dagdag na 'verify' step pagkatapos)
        // — kaya dito na lang hanggang sa nakalaang % ng 'zip' step
        // mismo umaabot ang progreso, hindi na diretso hanggang 100.
        const zipStepEnd = BUILD_STEP_END_PERCENT.zip;
        // BUG FIX: isulat muna sa hiwalay/temp na filename (HINDI direkta sa
        // RELEASE_PACKAGE_PATH) — tingnan ang komento sa RELEASE_PACKAGE_TMP_PATH
        // sa itaas kung bakit. Anumang lumang natitirang temp file (hal.
        // dahil sa nag-crash na build) ay linisin muna bago simulan.
        if (fs.existsSync(RELEASE_PACKAGE_TMP_PATH)) {
            fs.rmSync(RELEASE_PACKAGE_TMP_PATH, { force: true });
        }
        await new Promise((resolve, reject) => {
            const output = fs.createWriteStream(RELEASE_PACKAGE_TMP_PATH);
            // PERFORMANCE FIX: zlib level 9 (max compression) costs
            // significantly more CPU time than level 6 for only a small
            // reduction in zip size — not a good trade-off on a
            // CPU-constrained host. Level 6 is zlib's own default and a
            // much better time/size balance for a package this size.
            const archive = archiver('zip', { zlib: { level: 6 } });
            output.on('close', resolve);
            archive.on('error', reject);
            // LIVE ZIP PROGRESS (hiling): ang archiver package mismo ay may
            // sariling built-in 'progress' event (entries processed/total)
            // habang isinusulat ang zip — dati ay hindi ito ginagamit, kaya
            // nakatigil ang % sa buong pag-archive (walang event handler na
            // nag-uupdate) hanggang sa "close" na lang bigla ito tumalon
            // papuntang 100%. Ngayon, kada progress event, isinasalin ito
            // sa loob ng saklaw ng zip step para makita ang totoong pag-usad
            // (bilang ng na-archive na file / kabuuan) bawat segundo.
            archive.on('progress', (data) => {
                const total = data && data.entries && data.entries.total;
                const processed = data && data.entries && data.entries.processed;
                if (total) {
                    const pct = zipStepStart + ((processed / total) * (zipStepEnd - zipStepStart));
                    setBuildProgress({ stage: 'zip', percent: Math.round(pct * 10) / 10, message: `Ginagawa ang zip package... (${processed}/${total} file)` });
                }
            });
            archive.pipe(output);
            archive.directory(tmpDir, false);
            archive.finalize();
        });

        // BUG FIX: bago i-expose sa mga kliyente (rename papunta sa
        // RELEASE_PACKAGE_PATH), i-verify muna na hindi sira ang na-build
        // na zip — kung paano man ito nasira (bug sa archiver, puno ang
        // disk, atbp.), mas mabuting mahuli ito DITO, bago pa maka-download
        // ang kahit isang client, kaysa palitan lang ang lumang gumaganang
        // release ng sira.
        setBuildProgress({ stage: 'verify', percent: zipStepEnd, message: 'Sinusuri ang integridad ng bagong zip...' });
        try {
            execSync(`unzip -tq "${RELEASE_PACKAGE_TMP_PATH}"`, { stdio: 'pipe' });
        } catch (zipCheckErr) {
            fs.rmSync(RELEASE_PACKAGE_TMP_PATH, { force: true });
            throw new Error(`Nabuo ang zip pero HINDI ito pumasa sa integrity check (unzip -t) — hindi ito ipapalit sa kasalukuyang release. Detalye: ${zipCheckErr.message}`);
        }
        setBuildProgress({ stage: 'verify', percent: BUILD_STEP_END_PERCENT.verify, message: 'Pumasa sa integrity check — inilalapat na ang bagong release...' });

        // BUG FIX (corrupted self-update downloads — root cause): atomic
        // ang rename() sa loob ng parehong filesystem/volume, kaya kahit
        // may kasabay na GET /relay/release-package o /relay/download/:code
        // na nagbabasa ng RELEASE_PACKAGE_PATH sa eksaktong sandaling ito,
        // makikita nila ALINMAN sa buong lumang file O buong bagong file —
        // hindi na kailanman ang halo ng dalawa.
        fs.renameSync(RELEASE_PACKAGE_TMP_PATH, RELEASE_PACKAGE_PATH);

        const stats = fs.statSync(RELEASE_PACKAGE_PATH);
        const builtAt = Date.now();
        logActivity(null, 'release_package_built', {
            ref,
            sizeBytes: stats.size,
            obfuscatedFiles: obfuscatedCount,
            obfuscated: shouldObfuscate,
            envEncrypted: envResult.encrypted
        });

        // INTEGRITY BASELINE: kunin ang sha256 ng BAWAT file na eksaktong
        // isinama sa release na ito (mula sa tmpDir bago pa i-zip — ito
        // mismo ang binubuksan ng customer), at itago bilang "baseline"
        // para sa VERSION na ipina-publish/itinatakda ng build na ito.
        // Dito lang ito ginagawa (hindi sa /relay/admin/api/build-release
        // route wrapper) para sabay ito laging tumatakbo kada
        // matagumpay na build, kahit anong entry point ang tumawag dito
        // (build-only o publish-release).
        const baselineVersion = resolvedVersion;
        const baselineFiles = buildFileManifest(tmpDir);
        releaseBaselines.set(baselineVersion, {
            builtAt,
            fileCount: Object.keys(baselineFiles).length,
            files: baselineFiles
        });
        saveReleaseBaselines(releaseBaselines);
        console.log(`🔐 Integrity baseline saved para sa version ${baselineVersion} (${Object.keys(baselineFiles).length} file(s)).`);

        // HISTORY ENTRY: caption (kung binigay), ang naka-publish na
        // version number nung mismong oras ng build na ito (systemVersionInfo
        // — HINDI kinukuha mula sa git ref dahil ang version number ay
        // hiwalay na "opisyal" na ipinapahayag lang via publish-version) —
        // MALIBAN kung tumawag ito mula sa publish-release (may
        // publishOverride), kung saan ang ITINATAKDANG bagong version
        // (at target installationId kung meron) ang gagamitin sa halip,
        // dahil ito na mismo ang aktwal na version na kasabay ina-publish
        // ng build na ito. Ang filename ng zip mismo ay HINDI nagbabago
        // — palaging "omnipos-client.zip" pa rin ito.
        recordBuildHistoryEntry({
            id: crypto.randomBytes(6).toString('hex'),
            caption: caption || null,
            version: resolvedVersion,
            targetInstallationId: (publishOverride && publishOverride.targetInstallationId) || null,
            ref,
            builtAt,
            sizeBytes: stats.size,
            obfuscatedFiles: obfuscatedCount,
            obfuscated: shouldObfuscate,
            envEncrypted: envResult.encrypted,
            fileName: 'omnipos-client.zip'
        });

        if (buildProgressState.startedAt) {
            recordBuildDurationForEta(Date.now() - buildProgressState.startedAt);
        }
        setBuildProgress({ stage: 'done', percent: 100, message: 'Tapos na ang build.', done: true });

        const obfMsgPart = shouldObfuscate
            ? `Na-obfuscate ang ${obfuscatedCount} file(s)`
            : `Nagawa nang HINDI obfuscated (${obfuscatedCount} file(s) na-process)`;
        return {
            message: envResult.encrypted
                ? `Nagawa ang bagong release package. ${obfMsgPart}, naka-encrypt na ang .env.`
                : `Nagawa ang bagong release package. ${obfMsgPart}. PAALALA: walang env-loader.js sa repo mo — plaintext pa rin ang .env.`,
            sizeBytes: stats.size,
            obfuscatedFiles: obfuscatedCount,
            obfuscated: shouldObfuscate,
            envEncrypted: envResult.encrypted,
            builtAt
        };
    } catch (err) {
        console.error('❌ Build-release error:', err.message);
        setBuildProgress({ stage: 'error', percent: buildProgressState.percent, message: err.message, done: true, error: err.message });
        throw err;
    } finally {
        fs.rmSync(tmpDir, { recursive: true, force: true });
    }
}

// --------------------------------------------------------------
// POST /relay/admin/api/build-release  (route wrapper)
// Ito pa rin ang dating "build lang, walang publish" na endpoint —
// gumagamit lang ngayon ng shared performBuildRelease() sa itaas.
// --------------------------------------------------------------
app.post('/relay/admin/api/build-release', requireAdminKey, async (req, res) => {
    try {
        const result = await performBuildRelease(req.body, req);
        res.json({ success: true, ...result });
    } catch (err) {
        res.status(err.statusCode || 500).json({ success: false, message: err.statusCode ? err.message : `Hindi na-build ang release: ${err.message}` });
    }
});

// --------------------------------------------------------------
// POST /relay/admin/api/system/publish-release  (BAGO)
// "Isang tawag lang" na version: pinagsasama nito ang build-release
// (gumawa ng bagong omnipos-client.zip) AT publish-version (i-set ang
// bagong version/changelog na titignan ng existing clients) sa IISANG
// request — para hindi na kailangan pang dalawahing i-curl/Termux/HTTP
// Shortcut kada bagong update. Ito ang direktang sagot sa "paano para
// RELAY lang ang gagamitin, makikita na agad ng customer ang update."
//
// Body: { version (required), changelog?, repoUrl?, ref?, relayUrl?,
//         relayApiKey?, port?, caption? }
//
// Pagkakasunod: (1) build muna — kung mabigo ito (hal. sirang repoUrl,
// walang bagong commit, atbp.), HINDI na ita-tuloy ang publish-version,
// para hindi maka-publish ng version na wala pang katumbas na zip.
// (2) Kapag successful ang build, saka lang isasagawa ang publish.
// --------------------------------------------------------------
app.post('/relay/admin/api/system/publish-release', requireAdminKey, async (req, res) => {
    const version = String((req.body && req.body.version) || '').trim();
    if (!version) {
        return res.status(400).json({ success: false, message: 'Kailangan ang "version" (hal. "1.3.0").' });
    }
    // TARGETED PUBLISH (opsyonal): kung may binigay na installationId,
    // ang build pa rin ay tumatakbo nang normal (iisa lang naman ang
    // zip para sa lahat), pero ang PAG-ANNOUNCE ng version/changelog na
    // ito ay sa TARGETED store lang mapupunta — ang global
    // systemVersionInfo ay hindi magbabago, kaya ang ibang device lang
    // ang walang makikitang "may bagong update."
    const targetId = String((req.body && req.body.installationId) || '').trim();

    let buildResult;
    try {
        buildResult = await performBuildRelease(req.body, req, { version, targetInstallationId: targetId || null });
    } catch (err) {
        return res.status(err.statusCode || 500).json({
            success: false,
            stage: 'build',
            message: `Hindi na-build ang release — HINDI isinagawa ang publish-version: ${err.message}`
        });
    }

    const changelog = String((req.body && req.body.changelog) || '').trim();
    const entry = { version, changelog, publishedAt: Date.now() };

    if (targetId) {
        targetedReleases.set(targetId, entry);
        saveTargetedReleases(targetedReleases);
    } else {
        systemVersionInfo = entry;
        saveSystemVersionInfo(systemVersionInfo);
    }
    logActivity(targetId || null, 'release_built_and_published', {
        version, changelog, sizeBytes: buildResult.sizeBytes, targeted: !!targetId
    });

    res.json({
        success: true,
        message: targetId
            ? `Nabuo ang bersyon ${version} — na-target lang ito sa installationId ${targetId}. Ibang device, hindi ito makikita.`
            : `Nabuo at na-publish na ang bersyon ${version}. Makikita na ito ng LAHAT ng kliyente sa susunod na update-check nila.`,
        build: buildResult,
        targeted: !!targetId,
        installationId: targetId || null,
        systemVersionInfo: targetId ? undefined : systemVersionInfo,
        targetedRelease: targetId ? entry : undefined
    });
});

// --------------------------------------------------------------
// GET /relay/admin/api/system/targeted-releases
// Listahan ng lahat ng kasalukuyang naka-target na installationId →
// {version, changelog, publishedAt}. Para makita mo agad kung sino-sino
// pa ang may "pending" na targeted release na hindi pa naiaapply/
// na-clear.
// --------------------------------------------------------------
app.get('/relay/admin/api/system/targeted-releases', requireAdminKey, (req, res) => {
    res.json({ success: true, targeted: Object.fromEntries(targetedReleases) });
});

// --------------------------------------------------------------
// POST /relay/admin/api/system/targeted-releases/:installationId/clear
// Tinatanggal ang targeted override ng isang partikular na
// installationId — babalik ito sa pagtingin sa GLOBAL systemVersionInfo
// na lang, tulad ng ibang device.
// --------------------------------------------------------------
app.post('/relay/admin/api/system/targeted-releases/:installationId/clear', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    const existed = targetedReleases.delete(installationId);
    if (existed) saveTargetedReleases(targetedReleases);
    res.json({ success: true, cleared: existed });
});

// --------------------------------------------------------------
// GET /relay/admin/api/build-history
// Listahan lang ng mga naitala nang build (caption, version noong
// build, timestamp, sizeBytes, atbp.) — pinaka-bago munang entry.
// Hindi ito ang zip mismo, log lang ito ng "kailan/ano" ang bawat
// build na ginawa gamit ang "I-build ang Release" sa itaas.
// --------------------------------------------------------------
app.get('/relay/admin/api/build-history', requireAdminKey, (req, res) => {
    res.json({ success: true, history: buildHistory });
});

// --------------------------------------------------------------
// POST /relay/admin/api/build-history/clear
// Binubura ang BUONG log ng build history (caption/version/petsa per
// entry) — LOG LANG ito, hindi ito nagbubura/humihipo sa aktwal na
// omnipos-client.zip package o sa systemVersionInfo/targeted-releases,
// kaya ligtas itong i-clear anumang oras; parang "clear activity log"
// lang, walang epekto sa kung ano ang makikita/ma-download ng mga
// kliyente.
// --------------------------------------------------------------
app.post('/relay/admin/api/build-history/clear', requireAdminKey, (req, res) => {
    const clearedCount = buildHistory.length;
    buildHistory = [];
    saveBuildHistory(buildHistory);
    logActivity(null, 'build_history_cleared', { clearedCount });
    res.json({ success: true, clearedCount });
});

// --------------------------------------------------------------
// POST /relay/admin/api/download-codes/generate
// Gumagawa ang developer/admin nito ng isang BAGONG code para sa isang
// bagong kliyente — walang publicly-listed link, kaya kontrolado kung
// sino-sino talaga ang nagkakaroon ng access sa release package.
// Body: { label?, maxUses?, expiresInHours? }
// --------------------------------------------------------------
app.post('/relay/admin/api/download-codes/generate', requireAdminKey, (req, res) => {
    const { label, maxUses, expiresInHours } = req.body || {};
    const code = crypto.randomBytes(9).toString('base64url'); // ~12 chars, URL-safe
    const now = Date.now();
    downloadCodes.set(code, {
        label: label || null,
        maxUses: typeof maxUses === 'number' && maxUses > 0 ? maxUses : 1,
        usesRemaining: typeof maxUses === 'number' && maxUses > 0 ? maxUses : 1,
        createdAt: now,
        expiresAt: typeof expiresInHours === 'number' && expiresInHours > 0 ? now + expiresInHours * 60 * 60 * 1000 : null,
        lastUsedAt: null,
        downloadCount: 0
    });
    saveDownloadCodes(downloadCodes);
    logActivity(null, 'download_code_generated', { code, label: label || null });
    res.json({
        success: true,
        code,
        downloadUrl: `${req.protocol}://${req.get('host')}/relay/download/${code}`
    });
});

app.get('/relay/admin/api/download-codes', requireAdminKey, (req, res) => {
    const list = [...downloadCodes.entries()].map(([code, meta]) => ({ code, ...meta }));
    res.json({ success: true, codes: list });
});

app.post('/relay/admin/api/download-codes/:code/revoke', requireAdminKey, (req, res) => {
    downloadCodes.delete(req.params.code);
    saveDownloadCodes(downloadCodes);
    res.json({ success: true });
});

// --------------------------------------------------------------
// GET /relay/download/:code
// TINATAWAG NG BROWSER/CURL NG KLIYENTE MISMO (hindi ng OMNIPOS server)
// — ito ang aktwal na "i-download ang mga files para sa offline usage".
// Walang requireApiKey dito dahil hindi pa nga naka-install ang
// OMNIPOS client sa yugtong ito — ang code mismo (random, one-time,
// may bilang ng uses/expiry) ang proteksyon.
// --------------------------------------------------------------
app.get('/relay/download/:code', (req, res) => {
    const { code } = req.params;
    const meta = downloadCodes.get(code);

    if (!meta) {
        return res.status(404).send('Invalid o expired na download code. Kontakin ang developer para sa bagong link.');
    }
    if (meta.expiresAt && Date.now() > meta.expiresAt) {
        downloadCodes.delete(code);
        saveDownloadCodes(downloadCodes);
        return res.status(410).send('Expired na ang download code na ito. Kontakin ang developer para sa bagong link.');
    }
    if (meta.usesRemaining <= 0) {
        return res.status(410).send('Naubos na ang bilang ng pwedeng gamitin sa code na ito. Kontakin ang developer para sa bagong link.');
    }
    if (!fs.existsSync(RELEASE_PACKAGE_PATH)) {
        return res.status(503).send('Walang naka-publish na release package sa server pa. Kontakin ang developer.');
    }

    meta.usesRemaining -= 1;
    meta.lastUsedAt = Date.now();
    meta.downloadCount = (meta.downloadCount || 0) + 1;
    saveDownloadCodes(downloadCodes);
    logActivity(null, 'client_package_downloaded', { code, label: meta.label || null, ip: req.ip });

    res.download(RELEASE_PACKAGE_PATH, 'omnipos-client.zip');
});

app.get('/relay/health', (req, res) => res.json({ success: true, status: 'ok' }));

// --------------------------------------------------------------
// BOOTSTRAP — kailangan munang ma-load ang lahat ng persisted na
// estado (mula sa Redis kung naka-configure ang REDIS_URL, o mula sa
// lokal na JSON files kung wala) BAGO tumanggap ng kahit anong request
// ang server.
// --------------------------------------------------------------
async function bootstrapStores() {
    [
        allowedDevices,
        deviceLabels,
        deviceFingerprints,
        cloneSplits,
        issuedUnlocks,
        activityLog,
        backupCheckins,
        systemVersionInfo,
        targetedReleases,
        downloadCodes,
        buildHistory,
        releaseBaselines,
        integrityStatus,
        featureCatalogOverrides,
        branchSummaries
    ] = await Promise.all([
        loadAllowedDevices(),
        loadDeviceLabels(),
        loadDeviceFingerprints(),
        loadCloneSplits(),
        loadIssuedUnlocks(),
        loadActivityLog(),
        loadBackupCheckins(),
        loadSystemVersionInfo(),
        loadTargetedReleases(),
        loadDownloadCodes(),
        loadBuildHistory(),
        loadReleaseBaselines(),
        loadIntegrityStatus(),
        loadFeatureCatalogOverrides(),
        loadBranchSummaries()
    ]);

    // I-merge ang anumang dating naka-auto-learn na features papunta sa
    // FEATURE_CATALOG mirror (BASE + overrides), tapos i-sync ang 'pro'
    // tier para makasama agad ang mga ito.
    FEATURE_CATALOG = { ...FEATURE_CATALOG_BASE, ...featureCatalogOverrides };
    recomputeProTierFeatureIds();
    if (Object.keys(featureCatalogOverrides).length > 0) {
        console.log(`🆕 Na-load ang ${Object.keys(featureCatalogOverrides).length} dating auto-learned na feature(s) papunta sa catalog mirror: ${Object.keys(featureCatalogOverrides).join(', ')}.`);
    }

    console.log(
        redisClient
            ? `✅ Na-load mula sa Redis: ${allowedDevices.size} allowed device(s), ${Object.keys(issuedUnlocks).length} device(s) may naka-unlock.`
            : `ℹ️  Na-load mula sa lokal na JSON files: ${allowedDevices.size} allowed device(s), ${Object.keys(issuedUnlocks).length} device(s) may naka-unlock.`
    );
}

bootstrapStores()
    .then(() => {
        const server = app.listen(PORT, () => {
            console.log(`OmniPOS Unlock Relay running sa port ${PORT}`);
        });

        // BUG FIX: default sa Node.js ang `requestTimeout` ng http.Server ay
        // 300000ms (5 minuto) lamang — ito ang oras na binibigay para
        // TAPUSIN matanggap ang BUONG request (kasama na ang buong body) mula
        // sa client. Sa isang mabigat na /relay/cloud-backup/upload (hanggang
        // ~1GB, see CLOUD_BACKUP_JSON_LIMIT sa itaas) na dumadaan sa mabagal
        // na upload speed ng isang tindahan, kayang lumagpas ng 5 minuto ang
        // PAG-UPLOAD PALANG bago pa man ito maka-abot sa route handler — kaya
        // basta pinuputol ito ng Node mismo (bago pa man ma-apply ang
        // AbortController timeout na nasa relayFetch ng OMNIPOS client),
        // lumalabas na "network error"/dropped connection kahit walang
        // problema sa route handler o sa quota/size-limit logic nito.
        // 60 minuto na ngayon dito — mas mataas pa sa 40-minutong client-side
        // timeout (relayFetch sa OMNIPOS server.js) — para ang OMNIPOS client
        // ang laging unang mag-a-abort nang may malinaw na error, sa halip na
        // ang RELAY na basta mag-drop ng koneksyon nang tahimik.
        server.requestTimeout = 60 * 60 * 1000;
        // headersTimeout (default 60000ms) ay para lang sa pagtanggap ng mga
        // HTTP headers — maliit lang ito kahit malaking upload, kaya default
        // na lang ang iniiwan dito (kailangan lang siguraduhing mas mababa
        // ito kaysa requestTimeout sa itaas, na siyang totoo pa rin).
    })
    .catch((err) => {
        console.error('❌ Hindi ma-bootstrap ang persistent storage — hindi tumakbo ang server:', err);
        process.exit(1);
    });
