
const express = require('express');
const crypto = require('crypto');
const nodemailer = require('nodemailer');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { execSync, execFileSync, spawn } = require('child_process');
process.on('uncaughtException', (err) => {
    console.error('🔥 UNCAUGHT EXCEPTION (RELAY stayed up — this should be investigated):', err);
});
process.on('unhandledRejection', (reason) => {
    console.error('🔥 UNHANDLED PROMISE REJECTION (RELAY stayed up — this should be investigated):', reason);
});
const { Worker } = require('worker_threads');
const archiver = require('archiver');
const Redis = require('ioredis');
const JavaScriptObfuscator = require('javascript-obfuscator');
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
const { Pool } = require('pg');
// AYOS: dating IISANG DATABASE_URL/pgPool lang ang ginagamit PAREHO ng
// (1) Cloud Backup (cloud_backup_modules/cloud_backup_meta) AT (2) Device/
// license data (relay_devices/relay_device_fingerprints/relay_clone_splits),
// kaya iisa lang ang Neon database na tumatanggap ng dalawang klase ng data.
// Ngayon, hiwalay na ang dalawang Postgres connection:
//   - DATABASE_URL                -> Cloud Backup pool (pgPool)
//   - RELAY_DEVICES_DATABASE_URL  -> Device/license pool (pgPoolDevices)
// Kung walang naka-set na RELAY_DEVICES_DATABASE_URL, babalik muna ito sa
// DATABASE_URL (backward-compatible, hindi masisira ang existing deployments
// na iisa pa lang ang naka-configure na URL) — pero para TUNAY na mahiwalay
// ang dalawang database gaya ng gusto, kailangang lagyan ng SARILI at
// IBANG Neon connection string ang RELAY_DEVICES_DATABASE_URL.
const DATABASE_URL = process.env.DATABASE_URL || null;
const DEVICES_DATABASE_URL = process.env.RELAY_DEVICES_DATABASE_URL || DATABASE_URL || null;
const DEVICES_DB_IS_SEPARATE = !!(process.env.RELAY_DEVICES_DATABASE_URL && process.env.RELAY_DEVICES_DATABASE_URL !== DATABASE_URL);

function makePgPool(connectionString) {
    if (!connectionString) return null;
    return new Pool({
        connectionString,
        ssl: process.env.PGSSL === 'false' ? false : { rejectUnauthorized: false },
        keepAlive: true,
        keepAliveInitialDelayMillis: 10000,
        statement_timeout: 0,
        query_timeout: 0,
        idle_in_transaction_session_timeout: 0,
        connectionTimeoutMillis: 15000
    });
}

const pgPool = makePgPool(DATABASE_URL);
if (pgPool) {
    pgPool.on('error', (err) => {
        console.error('⚠️  Postgres pool error (cloud backup storage):', err.message);
    });
} else {
    console.warn('⚠️  Walang DATABASE_URL na naka-set — hindi gagana ang Cloud Backup (Postgres) feature hangga\'t hindi ito nalagyan.');
}

// Kung magkapareho ang connection string, muling gamitin ang parehong Pool
// instance (huwag gumawa ng dobleng koneksyon papunta sa iisang database).
const pgPoolDevices = DEVICES_DB_IS_SEPARATE ? makePgPool(DEVICES_DATABASE_URL) : pgPool;
if (DEVICES_DB_IS_SEPARATE && pgPoolDevices) {
    pgPoolDevices.on('error', (err) => {
        console.error('⚠️  Postgres pool error (device/license storage):', err.message);
    });
    console.log('✅ Hiwalay na Neon database ang ginagamit para sa Device/License data (RELAY_DEVICES_DATABASE_URL) mula sa Cloud Backup (DATABASE_URL).');
} else if (!pgPoolDevices) {
    console.warn('⚠️  Walang DATABASE_URL/RELAY_DEVICES_DATABASE_URL na naka-set — hindi gagana ang Device/License (Postgres) feature hangga\'t hindi ito nalagyan.');
} else {
    console.warn('ℹ️  Walang hiwalay na RELAY_DEVICES_DATABASE_URL na naka-set — GINAGAMIT PA RIN ang parehong DATABASE_URL para sa Device/License data at Cloud Backup. Para tunay na mahiwalay, magtakda ng ibang Neon connection string sa RELAY_DEVICES_DATABASE_URL.');
}
function isTransientPgConnectionError(err) {
    if (!err) return false;
    const msg = String(err.message || '');
    const code = err.code || '';
    return (
        msg.includes('Connection terminated unexpectedly') ||
        msg.includes('Connection terminated') ||
        code === 'ECONNRESET' ||
        code === 'EPIPE' ||
        code === 'ENOTFOUND' ||   
        code === 'ECONNREFUSED' || 
        code === '57P01' || 
        code === '57P02' || 
        code === '57P03'    
    );
}
const PG_WRITE_RETRY_DELAYS_MS = [2000, 5000, 10000, 20000];
// NOTE: tumatanggap na ito ng `pool` bilang unang argumento (pgPool para sa
// Cloud Backup, pgPoolDevices para sa device/license data) — dating pgPool
// lang ang direktang ginagamit dito, kaya nagsasalo ang dalawang klase ng
// data sa parehong connection.
async function runPgWriteTx(pool, writeFn, { maxAttempts = 5 } = {}) {
    let lastErr = null;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        const client = await pool.connect();
        client.on('error', (err) => {
            console.warn(`⚠️  PG_WRITE_TX: checked-out Postgres client emitted an error while idle between queries (${err.message}) — this attempt will fail and retry below instead of crashing the server.`);
        });
        try {
            await client.query('BEGIN');
            const result = await writeFn(client);
            await client.query('COMMIT');
            return result;
        } catch (err) {
            try { await client.query('ROLLBACK'); } catch (rollbackErr) {   }
            lastErr = err;
            if (!isTransientPgConnectionError(err) || attempt === maxAttempts) {
                throw err;
            }
            const delayMs = PG_WRITE_RETRY_DELAYS_MS[attempt - 1] || PG_WRITE_RETRY_DELAYS_MS[PG_WRITE_RETRY_DELAYS_MS.length - 1];
            console.warn(`⚠️  PG_WRITE_TX: transient Postgres connection error on attempt ${attempt}/${maxAttempts} (${err.message}) — waiting ${delayMs}ms then retrying with a fresh connection (database may be waking up from idle)…`);
        } finally {
            client.release();
        }
        const delayMs = PG_WRITE_RETRY_DELAYS_MS[attempt - 1] || PG_WRITE_RETRY_DELAYS_MS[PG_WRITE_RETRY_DELAYS_MS.length - 1];
        await new Promise(r => setTimeout(r, delayMs));
    }
    throw lastErr;
}
const READ_RETRY_DELAYS_MS = [2000, 5000];
async function queryWithRetry(pool, text, params, { maxAttempts = 3 } = {}) {
    let lastErr = null;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            return await pool.query(text, params);
        } catch (err) {
            lastErr = err;
            if (!isTransientPgConnectionError(err) || attempt === maxAttempts) {
                throw err;
            }
            const delayMs = READ_RETRY_DELAYS_MS[attempt - 1] || READ_RETRY_DELAYS_MS[READ_RETRY_DELAYS_MS.length - 1];
            console.warn(`⚠️  PG_READ: transient Postgres connection error on read attempt ${attempt}/${maxAttempts} (${err.message}) — waiting ${delayMs}ms then retrying with a fresh connection…`);
            await new Promise(r => setTimeout(r, delayMs));
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
    await pgPool.query(`ALTER TABLE cloud_backup_modules ADD COLUMN IF NOT EXISTS size_bytes BIGINT NOT NULL DEFAULT 0;`);
    await pgPool.query(`ALTER TABLE cloud_backup_meta ADD COLUMN IF NOT EXISTS size_bytes BIGINT NOT NULL DEFAULT 0;`);
    console.log('✅ Cloud backup Postgres schema ready (cloud_backup_modules, cloud_backup_meta).');
}
async function ensureDeviceLicenseSchema() {
    if (!pgPoolDevices) return;
    await pgPoolDevices.query(`
        CREATE TABLE IF NOT EXISTS relay_devices (
            installation_id TEXT PRIMARY KEY,
            allowed         BOOLEAN NOT NULL DEFAULT true,
            label           TEXT,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    `);
    await pgPoolDevices.query(`
        CREATE TABLE IF NOT EXISTS relay_device_fingerprints (
            installation_id     TEXT PRIMARY KEY,
            fingerprint         TEXT,
            flagged             BOOLEAN NOT NULL DEFAULT false,
            flagged_fingerprint TEXT,
            flagged_at          TIMESTAMPTZ,
            first_verified_at   TIMESTAMPTZ,
            last_verified_at    TIMESTAMPTZ,
            verify_count        INTEGER NOT NULL DEFAULT 0,
            updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    `);
    await pgPoolDevices.query(`
        CREATE TABLE IF NOT EXISTS relay_clone_splits (
            split_key           TEXT PRIMARY KEY,
            installation_id     TEXT NOT NULL,
            fingerprint         TEXT NOT NULL,
            new_installation_id TEXT NOT NULL,
            split_at            TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    `);
    console.log('✅ Device/license Postgres schema ready (relay_devices, relay_device_fingerprints, relay_clone_splits).');
}
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
    redisClient.set(REDIS_KEY_PREFIX + key, JSON.stringify(value)).catch((err) => {
        console.error(`⚠️  Hindi ma-save sa Redis ang key "${key}":`, err.message);
    });
}
try {
    process.loadEnvFile(path.join(__dirname, '.env'));
} catch (err) {
}
const app = express();
app.use((req, res, next) => {
    res.setHeader('X-Frame-Options', 'DENY');
    res.setHeader('Content-Security-Policy', "frame-ancestors 'none'");
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Referrer-Policy', 'no-referrer');
    next();
});
const CLOUD_BACKUP_UPLOAD_CHUNK_PATH = '/relay/cloud-backup/upload/chunk';
const defaultJsonParser = express.json({ limit: '2mb' });
const cloudBackupChunkRawParser = express.raw({ type: '*/*', limit: '6mb' });
app.use((req, res, next) => {
    if (req.path === CLOUD_BACKUP_UPLOAD_CHUNK_PATH) {
        return cloudBackupChunkRawParser(req, res, next);
    }
    return defaultJsonParser(req, res, next);
});
app.use((err, req, res, next) => {
    if (err && err.type === 'entity.too.large') {
        return res.status(413).json({
            success: false,
            payloadTooLarge: true,
            message: req.path === CLOUD_BACKUP_UPLOAD_CHUNK_PATH
                ? 'A cloud backup chunk was larger than expected — this should not normally happen. Please try syncing again.'
                : 'Masyadong malaki ang request (lumagpas sa 2mb limit).'
        });
    }
    return next(err);
});
const PORT = process.env.PORT || 4477;
const RELAY_API_KEY = process.env.RELAY_API_KEY || null; 
const MAIL_USER = process.env.RELAY_MAIL_USER;
const MAIL_PASS = process.env.RELAY_MAIL_PASS;
const RECIPIENT_EMAIL = process.env.RELAY_RECIPIENT_EMAIL; 
const OTP_TTL_MS = 10 * 60 * 1000; 
const RELAY_DEFAULT_LICENSE_DAYS = process.env.RELAY_DEFAULT_LICENSE_DAYS
    ? Number(process.env.RELAY_DEFAULT_LICENSE_DAYS)
    : null;
const SLACK_WEBHOOK_URL = process.env.RELAY_SLACK_WEBHOOK_URL || null;
const TELEGRAM_BOT_TOKEN = process.env.RELAY_TELEGRAM_BOT_TOKEN || null;
const TELEGRAM_CHAT_ID = process.env.RELAY_TELEGRAM_CHAT_ID || null;
const RESEND_API_KEY = process.env.RESEND_API_KEY || null;
const RESEND_FROM_EMAIL = process.env.RESEND_FROM_EMAIL || 'OmniPOS Unlock Relay <onboarding@resend.dev>';
const mailTransporter = (MAIL_USER && MAIL_PASS) ? nodemailer.createTransport({
    service: 'gmail',
    pool: true,
    maxConnections: 3,
    auth: { user: MAIL_USER, pass: MAIL_PASS },
    connectionTimeout: 8000,
    greetingTimeout: 8000,
    socketTimeout: 15000
}) : null;
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
const DEVICE_STORE_PATH = path.join(__dirname, 'allowed-devices.json');
async function loadAllowedDevices() {
    if (pgPoolDevices) {
        try {
            const result = await queryWithRetry(pgPoolDevices, 'SELECT installation_id FROM relay_devices WHERE allowed = true', []);
            return new Set(result.rows.map(r => r.installation_id));
        } catch (err) {
            console.error('⚠️  Hindi mabasa sa Postgres ang allowed-devices, babalik sa lokal na file:', err.message);
        }
    }
    try {
        const raw = fs.readFileSync(DEVICE_STORE_PATH, 'utf8');
        return new Set(JSON.parse(raw));
    } catch (err) {
        const seed = (process.env.RELAY_ALLOWED_DEVICES || '')
            .split(',')
            .map(id => id.trim())
            .filter(Boolean);
        return new Set(seed);
    }
}
function saveAllowedDevices(set) {
    if (pgPoolDevices) {
        const ids = [...set];
        runPgWriteTx(pgPoolDevices, async (client) => {
            await client.query('UPDATE relay_devices SET allowed = false, updated_at = now() WHERE allowed = true');
            for (const id of ids) {
                await client.query(
                    `INSERT INTO relay_devices (installation_id, allowed, updated_at)
                     VALUES ($1, true, now())
                     ON CONFLICT (installation_id) DO UPDATE SET allowed = true, updated_at = now()`,
                    [id]
                );
            }
        }).catch((err) => {
            console.error('⚠️  Hindi ma-save sa Postgres ang allowed-devices:', err.message);
        });
        return;
    }
    try {
        fs.writeFileSync(DEVICE_STORE_PATH, JSON.stringify([...set], null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang allowed-devices.json:', err);
    }
}
let allowedDevices = new Set(); 
const DEVICE_LABELS_PATH = path.join(__dirname, 'device-labels.json');
async function loadDeviceLabels() {
    if (pgPoolDevices) {
        try {
            const result = await queryWithRetry(pgPoolDevices, 'SELECT installation_id, label FROM relay_devices WHERE label IS NOT NULL', []);
            return new Map(result.rows.map(r => [r.installation_id, r.label]));
        } catch (err) {
            console.error('⚠️  Hindi mabasa sa Postgres ang device-labels, babalik sa lokal na file:', err.message);
        }
    }
    try {
        const raw = fs.readFileSync(DEVICE_LABELS_PATH, 'utf8');
        return new Map(Object.entries(JSON.parse(raw)));
    } catch (err) {
        return new Map();
    }
}
function saveDeviceLabels(map) {
    if (pgPoolDevices) {
        const entries = [...map.entries()];
        runPgWriteTx(pgPoolDevices, async (client) => {
            await client.query('UPDATE relay_devices SET label = NULL, updated_at = now() WHERE label IS NOT NULL');
            for (const [id, label] of entries) {
                await client.query(
                    `INSERT INTO relay_devices (installation_id, allowed, label, updated_at)
                     VALUES ($1, false, $2, now())
                     ON CONFLICT (installation_id) DO UPDATE SET label = $2, updated_at = now()`,
                    [id, label]
                );
            }
        }).catch((err) => {
            console.error('⚠️  Hindi ma-save sa Postgres ang device-labels:', err.message);
        });
        return;
    }
    try {
        fs.writeFileSync(DEVICE_LABELS_PATH, JSON.stringify(Object.fromEntries(map), null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang device-labels.json:', err);
    }
}
let deviceLabels = new Map(); 
const DEVICE_FINGERPRINTS_PATH = path.join(__dirname, 'device-fingerprints.json');
function fingerprintRowToRecord(r) {
    return {
        fingerprint: r.fingerprint,
        flagged: r.flagged,
        flaggedFingerprint: r.flagged_fingerprint || undefined,
        flaggedAt: r.flagged_at ? new Date(r.flagged_at).getTime() : undefined,
        firstVerifiedAt: r.first_verified_at ? new Date(r.first_verified_at).getTime() : undefined,
        lastVerifiedAt: r.last_verified_at ? new Date(r.last_verified_at).getTime() : undefined,
        verifyCount: r.verify_count || 0
    };
}
async function loadDeviceFingerprints() {
    if (pgPoolDevices) {
        try {
            const result = await queryWithRetry(pgPoolDevices, 'SELECT * FROM relay_device_fingerprints', []);
            return new Map(result.rows.map(r => [r.installation_id, fingerprintRowToRecord(r)]));
        } catch (err) {
            console.error('⚠️  Hindi mabasa sa Postgres ang device-fingerprints, babalik sa lokal na file:', err.message);
        }
    }
    try {
        const raw = fs.readFileSync(DEVICE_FINGERPRINTS_PATH, 'utf8');
        return new Map(Object.entries(JSON.parse(raw)));
    } catch (err) {
        return new Map();
    }
}
function saveDeviceFingerprints(map) {
    if (pgPoolDevices) {
        const entries = [...map.entries()];
        runPgWriteTx(pgPoolDevices, async (client) => {
            await client.query('DELETE FROM relay_device_fingerprints WHERE installation_id != ALL($1::text[])', [entries.map(([id]) => id)]);
            for (const [id, rec] of entries) {
                await client.query(
                    `INSERT INTO relay_device_fingerprints
                        (installation_id, fingerprint, flagged, flagged_fingerprint, flagged_at, first_verified_at, last_verified_at, verify_count, updated_at)
                     VALUES ($1, $2, $3, $4, to_timestamp($5::double precision / 1000.0), to_timestamp($6::double precision / 1000.0), to_timestamp($7::double precision / 1000.0), $8, now())
                     ON CONFLICT (installation_id) DO UPDATE SET
                        fingerprint = $2, flagged = $3, flagged_fingerprint = $4,
                        flagged_at = to_timestamp($5::double precision / 1000.0),
                        first_verified_at = to_timestamp($6::double precision / 1000.0),
                        last_verified_at = to_timestamp($7::double precision / 1000.0),
                        verify_count = $8, updated_at = now()`,
                    [id, rec.fingerprint || null, !!rec.flagged, rec.flaggedFingerprint || null,
                     rec.flaggedAt || null, rec.firstVerifiedAt || null, rec.lastVerifiedAt || null, rec.verifyCount || 0]
                );
            }
        }).catch((err) => {
            console.error('⚠️  Hindi ma-save sa Postgres ang device-fingerprints:', err.message);
        });
        return;
    }
    try {
        fs.writeFileSync(DEVICE_FINGERPRINTS_PATH, JSON.stringify(Object.fromEntries(map), null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang device-fingerprints.json:', err);
    }
}
let deviceFingerprints = new Map(); 
const CLONE_SPLITS_PATH = path.join(__dirname, 'clone-splits.json');
async function loadCloneSplits() {
    if (pgPoolDevices) {
        try {
            const result = await queryWithRetry(pgPoolDevices, 'SELECT * FROM relay_clone_splits', []);
            return new Map(result.rows.map(r => [r.split_key, {
                newInstallationId: r.new_installation_id,
                splitAt: r.split_at ? new Date(r.split_at).getTime() : undefined
            }]));
        } catch (err) {
            console.error('⚠️  Hindi mabasa sa Postgres ang clone-splits, babalik sa lokal na file:', err.message);
        }
    }
    try {
        const raw = fs.readFileSync(CLONE_SPLITS_PATH, 'utf8');
        return new Map(Object.entries(JSON.parse(raw)));
    } catch (err) {
        return new Map();
    }
}
function saveCloneSplits(map) {
    if (pgPoolDevices) {
        const entries = [...map.entries()];
        runPgWriteTx(pgPoolDevices, async (client) => {
            await client.query('DELETE FROM relay_clone_splits WHERE split_key != ALL($1::text[])', [entries.map(([key]) => key)]);
            for (const [key, rec] of entries) {
                const [installationId, fingerprint] = key.split('::');
                await client.query(
                    `INSERT INTO relay_clone_splits (split_key, installation_id, fingerprint, new_installation_id, split_at)
                     VALUES ($1, $2, $3, $4, to_timestamp($5::double precision / 1000.0))
                     ON CONFLICT (split_key) DO UPDATE SET new_installation_id = $4, split_at = to_timestamp($5::double precision / 1000.0)`,
                    [key, installationId, fingerprint, rec.newInstallationId, rec.splitAt || Date.now()]
                );
            }
        }).catch((err) => {
            console.error('⚠️  Hindi ma-save sa Postgres ang clone-splits:', err.message);
        });
        return;
    }
    try {
        fs.writeFileSync(CLONE_SPLITS_PATH, JSON.stringify(Object.fromEntries(map), null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang clone-splits.json:', err);
    }
}
let cloneSplits = new Map(); 
function cloneSplitKey(installationId, fingerprint) {
    return `${installationId}::${fingerprint}`;
}
const seenDevices = new Map(); 
const ONLINE_WINDOW_MS = 90 * 1000;
const ONLINE_KEY_PREFIX = REDIS_KEY_PREFIX + 'online:';
function markDeviceOnline(installationId) {
    if (!installationId || !redisClient) return;
    redisClient.set(ONLINE_KEY_PREFIX + installationId, '1', 'PX', ONLINE_WINDOW_MS).catch((err) => {
        console.error(`⚠️  Hindi ma-set ang online heartbeat key para sa "${installationId}":`, err.message);
    });
}
async function getOnlineStatusMap(installationIds) {
    const now = Date.now();
    if (!redisClient) {
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
    const installationId = (req.body && req.body.installationId) || req.query.installationId;
    const storeName = (req.body && req.body.storeName) || undefined;
    const username = (req.body && req.body.username) || undefined;
    recordDeviceSeen(installationId, { storeName, username }); 
    if (!installationId || !allowedDevices.has(installationId)) {
        return res.status(403).json({
            success: false,
            deviceNotAllowed: true,
            message: 'Hindi pa authorized ang device na ito para gumamit ng relay. Naka-log na ang device — maghintay ng authorization mula sa developer/store owner.'
        });
    }
    next();
}
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
    customer_crm: { name: 'Customer Profiles, Loyalty & Debtors', price: 799, category: 'module' }, 
    promo_codes: { name: 'Promo Codes Module', price: 499, category: 'module' },
    advanced_reports: { name: 'Sales Analytics & Advanced Reports', price: 799, category: 'module' },
    shift_management: { name: 'Multi-Cashier Shift Oversight & Z-Reading Reports', price: 699, category: 'module' },
    rbac_management: { name: 'Roles & Permissions (RBAC) Management', price: null, category: 'module', isSubscription: true },
    multi_branch: { name: 'Multi-Branch Dashboard', price: null, category: 'module', isSubscription: true },
    cloud_backup: { name: 'Cloud Backup (Postgres)', price: null, category: 'module', isSubscription: true }
};
const CLOUD_BACKUP_PLANS_BASE = {
    basic: { id: 'basic', name: 'Cloud Backup — Basic', price: { monthly: 129, yearly: 1290 }, storageQuotaMB: 250 },
    standard: { id: 'standard', name: 'Cloud Backup — Standard', price: { monthly: 249, yearly: 2490 }, storageQuotaMB: 1024 },
    pro: { id: 'pro', name: 'Cloud Backup — Pro', price: { monthly: 399, yearly: 3990 }, storageQuotaMB: 5120 }
};
const CLOUD_BACKUP_BILLING_DAYS = { monthly: 30, yearly: 365 };
const CLOUD_BACKUP_PLAN_OVERRIDES_PATH = path.join(__dirname, 'cloud-backup-plan-overrides.json');
async function loadCloudBackupPlanOverrides() {
    const fromRedis = await redisGetJSON('cloud-backup-plan-overrides', null);
    if (fromRedis !== null) return fromRedis;
    try {
        return JSON.parse(fs.readFileSync(CLOUD_BACKUP_PLAN_OVERRIDES_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveCloudBackupPlanOverrides(obj) {
    if (redisClient) {
        redisSetJSON('cloud-backup-plan-overrides', obj);
        return;
    }
    try {
        fs.writeFileSync(CLOUD_BACKUP_PLAN_OVERRIDES_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang cloud-backup-plan-overrides.json:', err);
    }
}
let cloudBackupPlanOverrides = {}; 
let CLOUD_BACKUP_PLANS = { ...CLOUD_BACKUP_PLANS_BASE };
function recomputeCloudBackupPlans() {
    const merged = {};
    for (const tier of Object.keys(CLOUD_BACKUP_PLANS_BASE)) {
        const base = CLOUD_BACKUP_PLANS_BASE[tier];
        const override = cloudBackupPlanOverrides[tier] || {};
        merged[tier] = {
            ...base,
            ...override,
            price: { ...base.price, ...(override.price || {}) }
        };
    }
    CLOUD_BACKUP_PLANS = merged;
}
function getCloudBackupPlanPrice(tier, billingCycle) {
    const plan = CLOUD_BACKUP_PLANS[tier];
    if (!plan || !CLOUD_BACKUP_BILLING_DAYS[billingCycle]) return null;
    return typeof plan.price[billingCycle] === 'number' ? plan.price[billingCycle] : null;
}
const MODULE_SUBSCRIPTION_FEATURE_IDS = ['rbac_management', 'multi_branch'];
function isModuleSubscriptionFeature(featureId) {
    return MODULE_SUBSCRIPTION_FEATURE_IDS.includes(featureId);
}
function isSubscriptionOnlyFeature(featureId) {
    return featureId === 'cloud_backup' || isModuleSubscriptionFeature(featureId);
}
const MODULE_SUBSCRIPTION_PLANS_BASE = {
    rbac_management: {
        id: 'rbac_management',
        name: 'Roles & Permissions (RBAC) Management',
        price: { monthly: 149, yearly: 1490 } 
    },
    multi_branch: {
        id: 'multi_branch',
        name: 'Multi-Branch Dashboard',
        price: { monthly: 199, yearly: 1990 }
    }
};
const MODULE_SUBSCRIPTION_BILLING_DAYS = { monthly: 30, yearly: 365 };
const MODULE_SUBSCRIPTION_GRACE_PERIOD_DAYS = 7;
const MODULE_SUBSCRIPTION_GRACE_PERIOD_MS = MODULE_SUBSCRIPTION_GRACE_PERIOD_DAYS * 24 * 60 * 60 * 1000;
const MODULE_SUBSCRIPTION_OVERRIDES_PATH = path.join(__dirname, 'module-subscription-overrides.json');
async function loadModuleSubscriptionOverrides() {
    const fromRedis = await redisGetJSON('module-subscription-overrides', null);
    if (fromRedis !== null) return fromRedis;
    try {
        return JSON.parse(fs.readFileSync(MODULE_SUBSCRIPTION_OVERRIDES_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveModuleSubscriptionOverrides(obj) {
    if (redisClient) {
        redisSetJSON('module-subscription-overrides', obj);
        return;
    }
    try {
        fs.writeFileSync(MODULE_SUBSCRIPTION_OVERRIDES_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Could not save module-subscription-overrides.json:', err);
    }
}
let moduleSubscriptionOverrides = {}; 
let MODULE_SUBSCRIPTION_PLANS = { ...MODULE_SUBSCRIPTION_PLANS_BASE };
function recomputeModuleSubscriptionPlans() {
    const merged = {};
    for (const featureId of MODULE_SUBSCRIPTION_FEATURE_IDS) {
        const base = MODULE_SUBSCRIPTION_PLANS_BASE[featureId];
        const override = moduleSubscriptionOverrides[featureId] || {};
        merged[featureId] = {
            ...base,
            ...override,
            price: { ...base.price, ...(override.price || {}) }
        };
    }
    MODULE_SUBSCRIPTION_PLANS = merged;
}
function getModuleSubscriptionPrice(featureId, billingCycle) {
    const plan = MODULE_SUBSCRIPTION_PLANS[featureId];
    if (!plan || !MODULE_SUBSCRIPTION_BILLING_DAYS[billingCycle]) return null;
    return typeof plan.price[billingCycle] === 'number' ? plan.price[billingCycle] : null;
}
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
let featureCatalogOverrides = {}; 
const FEATURE_PRICING_OVERRIDES_PATH = path.join(__dirname, 'feature-pricing-overrides.json');
async function loadFeaturePricingOverrides() {
    const fromRedis = await redisGetJSON('feature-pricing-overrides', null);
    if (fromRedis !== null) return fromRedis;
    try {
        return JSON.parse(fs.readFileSync(FEATURE_PRICING_OVERRIDES_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveFeaturePricingOverrides(obj) {
    if (redisClient) {
        redisSetJSON('feature-pricing-overrides', obj);
        return;
    }
    try {
        fs.writeFileSync(FEATURE_PRICING_OVERRIDES_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang feature-pricing-overrides.json:', err);
    }
}
const UPGRADE_TIER_BUNDLE_PRICE_BASE = { basic: 999, standard: 1999, pro: 3599 };
const UPGRADE_TIER_PRICING_OVERRIDES_PATH = path.join(__dirname, 'upgrade-tier-pricing-overrides.json');
async function loadUpgradeTierPricingOverrides() {
    const fromRedis = await redisGetJSON('upgrade-tier-pricing-overrides', null);
    if (fromRedis !== null) return fromRedis;
    try {
        return JSON.parse(fs.readFileSync(UPGRADE_TIER_PRICING_OVERRIDES_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveUpgradeTierPricingOverrides(obj) {
    if (redisClient) {
        redisSetJSON('upgrade-tier-pricing-overrides', obj);
        return;
    }
    try {
        fs.writeFileSync(UPGRADE_TIER_PRICING_OVERRIDES_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang upgrade-tier-pricing-overrides.json:', err);
    }
}
let upgradeTierPricingOverrides = {}; 
let featurePricingOverrides = {}; 
let FEATURE_CATALOG = { ...FEATURE_CATALOG_BASE };
function recomputeFeatureCatalog() {
    const merged = { ...FEATURE_CATALOG_BASE, ...featureCatalogOverrides };
    for (const featureId of Object.keys(featurePricingOverrides)) {
        if (!merged[featureId] || isSubscriptionOnlyFeature(featureId)) continue;
        merged[featureId] = { ...merged[featureId], ...featurePricingOverrides[featureId] };
    }
    FEATURE_CATALOG = merged;
    recomputeProTierFeatureIds();
}
const UPGRADE_TIER_FEATURE_IDS_BASE = {
    basic: ['advanced_reports', 'promo_codes'],
    standard: ['advanced_reports', 'promo_codes', 'customer_crm', 'shift_management']
};
let UPGRADE_TIERS = [
    { id: 'basic', name: 'Basic Upgrade', featureIds: [...UPGRADE_TIER_FEATURE_IDS_BASE.basic], bundlePrice: UPGRADE_TIER_BUNDLE_PRICE_BASE.basic },
    { id: 'standard', name: 'Standard Upgrade', featureIds: [...UPGRADE_TIER_FEATURE_IDS_BASE.standard], bundlePrice: UPGRADE_TIER_BUNDLE_PRICE_BASE.standard },
    { id: 'pro', name: 'Pro Upgrade (Complete)', featureIds: Object.keys(FEATURE_CATALOG).filter(id => !isSubscriptionOnlyFeature(id)), bundlePrice: UPGRADE_TIER_BUNDLE_PRICE_BASE.pro }
];
function recomputeProTierFeatureIds() {
    const proTier = UPGRADE_TIERS.find(t => t.id === 'pro');
    if (!proTier) return;
    const override = upgradeTierPricingOverrides && upgradeTierPricingOverrides.pro;
    if (override && Array.isArray(override.featureIds) && override.featureIds.length) {
        proTier.featureIds = override.featureIds.filter(id => FEATURE_CATALOG[id] && !isSubscriptionOnlyFeature(id));
    } else {
        proTier.featureIds = Object.keys(FEATURE_CATALOG).filter(id => !isSubscriptionOnlyFeature(id));
    }
}
function recomputeUpgradeTierPricing() {
    for (const tier of UPGRADE_TIERS) {
        const base = UPGRADE_TIER_BUNDLE_PRICE_BASE[tier.id];
        const override = upgradeTierPricingOverrides[tier.id];
        tier.bundlePrice = (override && typeof override.bundlePrice === 'number') ? override.bundlePrice : base;
        if (override && typeof override.name === 'string' && override.name.trim()) {
            tier.name = override.name.trim();
        }
        if (tier.id === 'pro') continue;
        if (override && Array.isArray(override.featureIds) && override.featureIds.length) {
            tier.featureIds = override.featureIds.filter(id => FEATURE_CATALOG[id] && !isSubscriptionOnlyFeature(id));
        } else {
            tier.featureIds = [...(UPGRADE_TIER_FEATURE_IDS_BASE[tier.id] || [])];
        }
    }
    recomputeProTierFeatureIds();
}
function registerFeatureIfUnknown(featureId, meta = {}, installationId = null) {
    if (!featureId || FEATURE_CATALOG[featureId]) return false;
    if (!meta.featureName) return false; 
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
    recomputeFeatureCatalog(); 
    saveFeatureCatalogOverrides(featureCatalogOverrides);
    console.log(`🆕 Bagong feature na na-detect at awtomatikong idinagdag sa RELAY catalog mirror: ${featureId} (${entry.name}, ${entry.price !== null ? '₱' + entry.price : 'walang presyo'}).`);
    try {
        logActivity(installationId, 'feature_auto_registered', { featureId, featureName: entry.name, price: entry.price, source: entry.learnedFrom });
    } catch (err) {
    }
    return true;
}
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
let issuedUnlocks = {}; 
function recordIssuedUnlock(installationId, featureId, token, meta = {}) {
    registerFeatureIfUnknown(featureId, { featureName: meta.featureName, price: meta.price, source: meta.source }, installationId);
    if (!issuedUnlocks[installationId]) issuedUnlocks[installationId] = {};
    issuedUnlocks[installationId][featureId] = {
        featureName: meta.featureName || (FEATURE_CATALOG[featureId] && FEATURE_CATALOG[featureId].name) || featureId,
        price: typeof meta.price === 'number' ? meta.price : (FEATURE_CATALOG[featureId] && FEATURE_CATALOG[featureId].price) || null,
        issuedAt: token.payload.issuedAt,
        expiresAt: typeof token.payload.expiresAt === 'number' ? token.payload.expiresAt : null,
        payload: token.payload,
        signature: token.signature,
        source: meta.source || 'otp', 
        note: meta.note || null,
        tier: meta.tier || null,
        billingCycle: meta.billingCycle || null
    };
    saveIssuedUnlocks(issuedUnlocks);
}
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
let activityLog = []; 
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
        const featureId = (details && details.featureId) ? details.featureId : '';
        const mergeKey = `${installationId || ''}|${type}|${featureId}`;
        const existingIndex = activityLog.findIndex(e => e._mergeKey === mergeKey);
        if (existingIndex !== -1) {
            const existing = activityLog[existingIndex];
            existing.details = details; 
            existing.at = now;
            existing.count = (existing.count || 1) + 1;
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
    activityLog.unshift({
        installationId: installationId || null,
        type,
        details,
        at: now
    });
    if (activityLog.length > ACTIVITY_LOG_MAX) activityLog.length = ACTIVITY_LOG_MAX;
    saveActivityLog(activityLog);
}
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
let backupCheckins = {}; 
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
let branchSummaries = {}; 
const BRANCH_GROUP_HASH_RE = /^[a-f0-9]{64}$/; 
const BRANCH_NAME_MAX_LEN = 60;
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
const AUTOALLOW_ON_BACKUP = String(process.env.RELAY_AUTOALLOW_ON_BACKUP || 'true').trim().toLowerCase() !== 'false';
function mostRecentBackupCheckinAt() {
    const values = Object.values(backupCheckins).map((c) => c.lastBackupAt).filter(Boolean);
    return values.length ? Math.max(...values) : null;
}
const INTEGRITY_EXCLUDE_NAMES = new Set([
    '.env', '.env.key', 'database', 'node_modules', 'uploads_tmp',
    '.git', 'release', 'cf.log', 'server.log', '.start.sh.lock',
    '.self-update-backup', 'package-lock.json', 'certs',
    'cloud-backup-pricing-cache.json'
]);
const INTEGRITY_EXCLUDE_EXTENSIONS = new Set(['.log', '.patch']);
function sha256File(filePath) {
    const hash = crypto.createHash('sha256');
    hash.update(fs.readFileSync(filePath));
    return hash.digest('hex');
}
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
            }
        }
    }
    walk(rootDir, '');
    return manifest;
}
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
let releaseBaselines = new Map(); 
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
let integrityStatus = {}; 
const pendingIntegrityChecks = new Set();
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
let systemVersionInfo = { ...DEFAULT_SYSTEM_VERSION_INFO }; 
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
let targetedReleases = new Map(); 
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
let downloadCodes = new Map(); 
const RELEASE_PACKAGE_PATH = path.join(__dirname, 'release', 'omnipos-client.zip');
const RELEASE_PACKAGE_TMP_PATH = path.join(__dirname, 'release', '.omnipos-client.zip.building');
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
    buildHistory.unshift(entry); 
    if (buildHistory.length > BUILD_HISTORY_MAX_ENTRIES) {
        buildHistory.length = BUILD_HISTORY_MAX_ENTRIES;
    }
    saveBuildHistory(buildHistory);
}
let buildHistory = []; 
const ADMIN_KEY = process.env.RELAY_ADMIN_KEY || null;
if (!ADMIN_KEY) {
    console.warn('⚠️  Walang RELAY_ADMIN_KEY na naka-set — hindi magagamit ang /relay/admin panel hangga\'t hindi ito nalagyan.');
}
function safeCompare(a, b) {
    if (typeof a !== 'string' || typeof b !== 'string') return false;
    const bufA = Buffer.from(a);
    const bufB = Buffer.from(b);
    if (bufA.length !== bufB.length) {
        crypto.timingSafeEqual(bufA, bufA);
        return false;
    }
    return crypto.timingSafeEqual(bufA, bufB);
}
const ADMIN_LOGIN_MAX_FAILURES = 10;
const ADMIN_LOGIN_WINDOW_MS = 10 * 60 * 1000;
const adminLoginFailures = new Map(); 
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
    const { key, durationDays } = req.body;
    const pending = pendingOtps.get(key);
    if (!pending) {
        return res.status(404).json({ success: false, message: "Wala nang aktibong request na iyan (baka na-expire na o na-claim na)." });
    }
    pending.approved = true;
    if (typeof durationDays === 'number' && durationDays > 0) {
        pending.durationDays = durationDays;
    } else if (durationDays === 0) {
        pending.durationDays = null; 
    } else if (pending.featureId === 'cloud_backup' && pending.billingCycle && CLOUD_BACKUP_BILLING_DAYS[pending.billingCycle]) {
        pending.durationDays = CLOUD_BACKUP_BILLING_DAYS[pending.billingCycle];
    } else if (isModuleSubscriptionFeature(pending.featureId) && pending.billingCycle && MODULE_SUBSCRIPTION_BILLING_DAYS[pending.billingCycle]) {
        pending.durationDays = MODULE_SUBSCRIPTION_BILLING_DAYS[pending.billingCycle];
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
        .filter(entry => entry.expiresAt > now) 
        .sort((a, b) => b.expiresAt - a.expiresAt);
    res.json({ success: true, pendingOtps: pending });
});
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
            online: !!onlineMap[installationId],
            label: deviceLabels.get(installationId) || null,
            allowed: allowedDevices.has(installationId),
            unlockedCount: unlockedIds.length,
            totalCatalogCount: Object.keys(FEATURE_CATALOG).length,
            demoActive: getActiveUnlockedFeatureIds(installationId).includes(DEMO_FEATURE_ID),
            lastActivationAt,
            lastBackupAt: backupCheckin ? backupCheckin.lastBackupAt : null,
            backupCheckinCount: backupCheckin ? backupCheckin.checkinCount : 0,
            cloneFlagged: !!(fingerprintRecord && fingerprintRecord.flagged),
            fingerprintVerifyCount: fingerprintRecord ? fingerprintRecord.verifyCount : 0,
            integrityFlagged: !!(integrityRecord && integrityRecord.flagged && !integrityRecord.clearedAt),
            integrityModifiedCount: integrityRecord ? integrityRecord.modifiedCount : 0,
            integrityDeletedCount: integrityRecord ? integrityRecord.deletedCount : 0,
            integrityAddedCount: integrityRecord ? integrityRecord.addedCount : 0,
            integrityCheckedAt: integrityRecord ? integrityRecord.checkedAt : null,
            integrityHasBaseline: integrityRecord ? !!integrityRecord.hasBaseline : null,
            integrityBaselineVersion: integrityRecord ? integrityRecord.baselineVersion : null,
            integrityWatcherActive: integrityRecord ? (integrityRecord.watcherActive ?? null) : null
        };
    }).sort((a, b) => b.lastSeenAt - a.lastSeenAt);
    res.json({
        success: true,
        seenDevices: seen,
        allowedDevices: [...allowedDevices],
        deviceLabels: Object.fromEntries(deviceLabels),
        restrictionActive: true, 
        lastBackupSyncAt: mostRecentBackupCheckinAt(),
        backupAutoAllowEnabled: AUTOALLOW_ON_BACKUP,
        cloneFlaggedCount: [...deviceFingerprints.values()].filter(r => r.flagged).length,
        integrityFlaggedCount: Object.values(integrityStatus).filter(r => r.flagged && !r.clearedAt).length,
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
    if (integrityStatus[installationId]) {
        delete integrityStatus[installationId];
        saveIntegrityStatus(integrityStatus);
    }
    logActivity(installationId, 'device_revoked', {});
    res.json({ success: true, allowedDevices: [...allowedDevices] });
});
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
app.post('/relay/admin/api/devices/bulk-reset', requireAdminKey, async (req, res) => {
    const { installationIds } = req.body;
    if (!Array.isArray(installationIds) || installationIds.length === 0) {
        return res.status(400).json({ success: false, message: 'Kulang o mali ang installationIds (dapat non-empty array).' });
    }
    for (const id of installationIds) {
        deviceLabels.delete(id);
        deviceFingerprints.delete(id);
        cloneSplits.delete(id);
        seenDevices.delete(id); 
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
app.post('/relay/admin/api/devices/reset-all', requireAdminKey, async (req, res) => {
    const confirm = req.query.confirm || req.body?.confirm;
    if (confirm !== 'RESET') {
        return res.status(400).json({
            success: false,
            message: 'Safety check: kailangan ng ?confirm=RESET (o "confirm":"RESET" sa JSON body) para tuluyang i-reset ang LAHAT ng device data. Hindi na ito mababawi.'
        });
    }
    const wipeUnlocksToo = req.body?.wipeUnlocksToo === true;
    allowedDevices = new Set();
    deviceLabels = new Map();
    deviceFingerprints = new Map();
    cloneSplits = new Map();
    saveAllowedDevices(allowedDevices);
    saveDeviceLabels(deviceLabels);
    saveDeviceFingerprints(deviceFingerprints);
    saveCloneSplits(cloneSplits);
    seenDevices.clear();
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
app.get('/relay/admin/api/catalog', requireAdminKey, (req, res) => {
    res.json({ success: true, catalog: FEATURE_CATALOG, tiers: UPGRADE_TIERS });
});
app.get('/relay/admin/api/pricing', requireAdminKey, (req, res) => {
    res.json({
        success: true,
        cloudBackupPlans: CLOUD_BACKUP_PLANS,
        cloudBackupPlansBase: CLOUD_BACKUP_PLANS_BASE,
        cloudBackupPlanOverrides,
        billingDays: CLOUD_BACKUP_BILLING_DAYS,
        moduleSubscriptionPlans: MODULE_SUBSCRIPTION_PLANS,
        moduleSubscriptionPlansBase: MODULE_SUBSCRIPTION_PLANS_BASE,
        moduleSubscriptionOverrides,
        moduleSubscriptionBillingDays: MODULE_SUBSCRIPTION_BILLING_DAYS,
        moduleSubscriptionGracePeriodDays: MODULE_SUBSCRIPTION_GRACE_PERIOD_DAYS
    });
});
app.post('/relay/admin/api/pricing/cloud-backup', requireAdminKey, (req, res) => {
    const { tier, name, monthly, yearly, storageQuotaMB } = req.body || {};
    if (!tier || !CLOUD_BACKUP_PLANS_BASE[tier]) {
        return res.status(400).json({ success: false, message: 'Invalid o walang tier (basic/standard/pro).' });
    }
    if (monthly !== undefined && (typeof monthly !== 'number' || !isFinite(monthly) || monthly < 0)) {
        return res.status(400).json({ success: false, message: 'Invalid monthly price.' });
    }
    if (yearly !== undefined && (typeof yearly !== 'number' || !isFinite(yearly) || yearly < 0)) {
        return res.status(400).json({ success: false, message: 'Invalid yearly price.' });
    }
    if (storageQuotaMB !== undefined && (typeof storageQuotaMB !== 'number' || !isFinite(storageQuotaMB) || storageQuotaMB <= 0)) {
        return res.status(400).json({ success: false, message: 'Invalid storageQuotaMB.' });
    }
    const existing = cloudBackupPlanOverrides[tier] || {};
    const updated = { ...existing };
    if (typeof name === 'string' && name.trim()) updated.name = name.trim();
    if (typeof storageQuotaMB === 'number') updated.storageQuotaMB = storageQuotaMB;
    if (typeof monthly === 'number' || typeof yearly === 'number') {
        const price = { ...(existing.price || {}) };
        if (typeof monthly === 'number') price.monthly = monthly;
        if (typeof yearly === 'number') price.yearly = yearly;
        updated.price = price;
    }
    cloudBackupPlanOverrides[tier] = updated;
    saveCloudBackupPlanOverrides(cloudBackupPlanOverrides);
    recomputeCloudBackupPlans();
    console.log(`💳 Na-update ang Cloud Backup pricing override para sa "${tier}" via admin panel.`);
    res.json({ success: true, plan: CLOUD_BACKUP_PLANS[tier] });
});
app.post('/relay/admin/api/pricing/cloud-backup/reset', requireAdminKey, (req, res) => {
    const { tier } = req.body || {};
    if (!tier || !CLOUD_BACKUP_PLANS_BASE[tier]) {
        return res.status(400).json({ success: false, message: 'Invalid o walang tier (basic/standard/pro).' });
    }
    delete cloudBackupPlanOverrides[tier];
    saveCloudBackupPlanOverrides(cloudBackupPlanOverrides);
    recomputeCloudBackupPlans();
    console.log(`💳 Na-reset sa default ang Cloud Backup pricing ng "${tier}" via admin panel.`);
    res.json({ success: true, plan: CLOUD_BACKUP_PLANS[tier] });
});
app.get('/relay/admin/api/pricing/module-subscriptions', requireAdminKey, (req, res) => {
    res.json({
        success: true,
        moduleSubscriptionPlans: MODULE_SUBSCRIPTION_PLANS,
        moduleSubscriptionPlansBase: MODULE_SUBSCRIPTION_PLANS_BASE,
        moduleSubscriptionOverrides,
        billingDays: MODULE_SUBSCRIPTION_BILLING_DAYS,
        gracePeriodDays: MODULE_SUBSCRIPTION_GRACE_PERIOD_DAYS
    });
});
app.post('/relay/admin/api/pricing/module-subscriptions', requireAdminKey, (req, res) => {
    const { featureId, name, monthly, yearly } = req.body || {};
    if (!featureId || !MODULE_SUBSCRIPTION_PLANS_BASE[featureId]) {
        return res.status(400).json({ success: false, message: 'Invalid or missing featureId (rbac_management/multi_branch).' });
    }
    if (monthly !== undefined && (typeof monthly !== 'number' || !isFinite(monthly) || monthly < 0)) {
        return res.status(400).json({ success: false, message: 'Invalid monthly price.' });
    }
    if (yearly !== undefined && (typeof yearly !== 'number' || !isFinite(yearly) || yearly < 0)) {
        return res.status(400).json({ success: false, message: 'Invalid yearly price.' });
    }
    if (name !== undefined && (typeof name !== 'string' || !name.trim())) {
        return res.status(400).json({ success: false, message: 'Invalid name.' });
    }
    const existing = moduleSubscriptionOverrides[featureId] || {};
    const updated = { ...existing };
    if (typeof name === 'string' && name.trim()) updated.name = name.trim();
    if (typeof monthly === 'number' || typeof yearly === 'number') {
        const price = { ...(existing.price || {}) };
        if (typeof monthly === 'number') price.monthly = monthly;
        if (typeof yearly === 'number') price.yearly = yearly;
        updated.price = price;
    }
    moduleSubscriptionOverrides[featureId] = updated;
    saveModuleSubscriptionOverrides(moduleSubscriptionOverrides);
    recomputeModuleSubscriptionPlans();
    console.log(`💳 Updated module subscription pricing override for "${featureId}" via admin panel.`);
    res.json({ success: true, plan: MODULE_SUBSCRIPTION_PLANS[featureId] });
});
app.post('/relay/admin/api/pricing/module-subscriptions/reset', requireAdminKey, (req, res) => {
    const { featureId } = req.body || {};
    if (!featureId || !MODULE_SUBSCRIPTION_PLANS_BASE[featureId]) {
        return res.status(400).json({ success: false, message: 'Invalid or missing featureId (rbac_management/multi_branch).' });
    }
    delete moduleSubscriptionOverrides[featureId];
    saveModuleSubscriptionOverrides(moduleSubscriptionOverrides);
    recomputeModuleSubscriptionPlans();
    console.log(`💳 Reset module subscription pricing for "${featureId}" to default via admin panel.`);
    res.json({ success: true, plan: MODULE_SUBSCRIPTION_PLANS[featureId] });
});
app.get('/relay/admin/api/pricing/features', requireAdminKey, (req, res) => {
    const editable = {};
    for (const [featureId, entry] of Object.entries(FEATURE_CATALOG)) {
        if (isSubscriptionOnlyFeature(featureId)) continue; 
        editable[featureId] = entry;
    }
    const editableBase = {};
    for (const [featureId, entry] of Object.entries(FEATURE_CATALOG_BASE)) {
        if (isSubscriptionOnlyFeature(featureId)) continue;
        editableBase[featureId] = entry;
    }
    res.json({
        success: true,
        featureCatalog: editable,
        featureCatalogBase: editableBase,
        featurePricingOverrides
    });
});
app.post('/relay/admin/api/pricing/features', requireAdminKey, (req, res) => {
    const { featureId, name, price } = req.body || {};
    if (!featureId || isSubscriptionOnlyFeature(featureId) || !FEATURE_CATALOG[featureId]) {
        return res.status(400).json({ success: false, message: 'Invalid or unknown featureId (or it is a subscription feature — cloud_backup/rbac_management/multi_branch — which has its own dedicated pricing editor).' });
    }
    if (price !== undefined && (typeof price !== 'number' || !isFinite(price) || price < 0)) {
        return res.status(400).json({ success: false, message: 'Invalid price.' });
    }
    if (name !== undefined && (typeof name !== 'string' || !name.trim())) {
        return res.status(400).json({ success: false, message: 'Invalid name.' });
    }
    const existing = featurePricingOverrides[featureId] || {};
    const updated = { ...existing };
    if (typeof name === 'string' && name.trim()) updated.name = name.trim();
    if (typeof price === 'number') updated.price = price;
    featurePricingOverrides[featureId] = updated;
    saveFeaturePricingOverrides(featurePricingOverrides);
    recomputeFeatureCatalog();
    console.log(`💳 Na-update ang pricing override ng feature "${featureId}" via admin panel.`);
    res.json({ success: true, feature: { featureId, ...FEATURE_CATALOG[featureId] } });
});
app.post('/relay/admin/api/pricing/features/reset', requireAdminKey, (req, res) => {
    const { featureId } = req.body || {};
    if (!featureId || isSubscriptionOnlyFeature(featureId) || !FEATURE_CATALOG[featureId]) {
        return res.status(400).json({ success: false, message: 'Invalid or unknown featureId.' });
    }
    delete featurePricingOverrides[featureId];
    saveFeaturePricingOverrides(featurePricingOverrides);
    recomputeFeatureCatalog();
    console.log(`💳 Na-reset sa default ang pricing ng feature "${featureId}" via admin panel.`);
    res.json({ success: true, feature: { featureId, ...FEATURE_CATALOG[featureId] } });
});
let SUGGESTED_DISCOUNT_PERCENT = 30;
const SUGGESTED_DISCOUNT_PERCENT_PATH = path.join(__dirname, 'suggested-discount-percent.json');
async function loadSuggestedDiscountPercent() {
    const fromRedis = await redisGetJSON('suggested-discount-percent', null);
    if (fromRedis !== null && typeof fromRedis === 'number') return fromRedis;
    try {
        const parsed = JSON.parse(fs.readFileSync(SUGGESTED_DISCOUNT_PERCENT_PATH, 'utf8'));
        if (typeof parsed === 'number') return parsed;
    } catch (err) {   }
    return 30;
}
function saveSuggestedDiscountPercent(value) {
    if (redisClient) {
        redisSetJSON('suggested-discount-percent', value);
        return;
    }
    try {
        fs.writeFileSync(SUGGESTED_DISCOUNT_PERCENT_PATH, JSON.stringify(value));
    } catch (err) {
        console.error('Hindi ma-save ang suggested-discount-percent.json:', err);
    }
}
function computeTierPricingSuggestion(tier) {
    const alaCarteTotal = tier.featureIds.reduce((sum, id) => sum + ((FEATURE_CATALOG[id] && FEATURE_CATALOG[id].price) || 0), 0);
    const discountPercent = alaCarteTotal > 0
        ? Math.round(((alaCarteTotal - tier.bundlePrice) / alaCarteTotal) * 100)
        : 0;
    const rawSuggested = alaCarteTotal * (1 - SUGGESTED_DISCOUNT_PERCENT / 100);
    const suggestedBundlePrice = alaCarteTotal > 0 ? Math.max(1, Math.round(rawSuggested / 50) * 50) : tier.bundlePrice;
    return { alaCarteTotal, discountPercent, suggestedBundlePrice };
}
app.get('/relay/admin/api/pricing/tiers', requireAdminKey, (req, res) => {
    const effective = {};
    for (const tier of UPGRADE_TIERS) {
        const suggestion = computeTierPricingSuggestion(tier);
        effective[tier.id] = {
            id: tier.id,
            name: tier.name,
            bundlePrice: tier.bundlePrice,
            featureIds: tier.featureIds,
            alaCarteTotal: suggestion.alaCarteTotal,
            discountPercent: suggestion.discountPercent,
            suggestedBundlePrice: suggestion.suggestedBundlePrice
        };
    }
    const tierOrder = ['basic', 'standard', 'pro'];
    const invertedDiscountWarnings = [];
    for (let i = 1; i < tierOrder.length; i++) {
        const bigger = effective[tierOrder[i]];
        const smaller = effective[tierOrder[i - 1]];
        if (bigger && smaller && bigger.discountPercent < smaller.discountPercent) {
            invertedDiscountWarnings.push(`"${bigger.name}" (${bigger.discountPercent}% off) has a SMALLER discount than "${smaller.name}" (${smaller.discountPercent}% off) — consider lowering its bundle price.`);
        }
    }
    const base = {};
    for (const tierId of Object.keys(UPGRADE_TIER_BUNDLE_PRICE_BASE)) {
        base[tierId] = {
            bundlePrice: UPGRADE_TIER_BUNDLE_PRICE_BASE[tierId],
            featureIds: UPGRADE_TIER_FEATURE_IDS_BASE[tierId] || undefined
        };
    }
    const selectableFeatureIds = Object.keys(FEATURE_CATALOG).filter(id => !isSubscriptionOnlyFeature(id));
    res.json({
        success: true,
        upgradeTiers: effective,
        upgradeTiersBase: base,
        upgradeTierPricingOverrides,
        selectableFeatureIds,
        invertedDiscountWarnings
    });
});
app.post('/relay/admin/api/pricing/tiers', requireAdminKey, (req, res) => {
    const { tierId, name, bundlePrice, featureIds } = req.body || {};
    if (!tierId || !UPGRADE_TIER_BUNDLE_PRICE_BASE[tierId]) {
        return res.status(400).json({ success: false, message: 'Invalid o hindi kilalang tierId (basic/standard/pro).' });
    }
    if (bundlePrice !== undefined && (typeof bundlePrice !== 'number' || !isFinite(bundlePrice) || bundlePrice < 0)) {
        return res.status(400).json({ success: false, message: 'Invalid bundlePrice.' });
    }
    if (name !== undefined && (typeof name !== 'string' || !name.trim())) {
        return res.status(400).json({ success: false, message: 'Invalid name.' });
    }
    if (featureIds !== undefined) {
        if (!Array.isArray(featureIds) || featureIds.length === 0) {
            return res.status(400).json({ success: false, message: 'featureIds must be a non-empty array of feature IDs.' });
        }
        const invalid = featureIds.filter(id => !FEATURE_CATALOG[id] || isSubscriptionOnlyFeature(id));
        if (invalid.length) {
            return res.status(400).json({ success: false, message: `These feature IDs cannot be placed in a bundle (unknown, or a subscription-only feature that is always purchased separately): ${invalid.join(', ')}` });
        }
    }
    const existing = upgradeTierPricingOverrides[tierId] || {};
    const updated = { ...existing };
    if (typeof name === 'string' && name.trim()) updated.name = name.trim();
    if (typeof bundlePrice === 'number') updated.bundlePrice = bundlePrice;
    if (Array.isArray(featureIds)) updated.featureIds = featureIds;
    upgradeTierPricingOverrides[tierId] = updated;
    saveUpgradeTierPricingOverrides(upgradeTierPricingOverrides);
    recomputeUpgradeTierPricing();
    console.log(`💳 Na-update ang bundle pricing/selection override ng tier "${tierId}" via admin panel.`);
    const tier = UPGRADE_TIERS.find(t => t.id === tierId);
    res.json({ success: true, tier });
});
app.post('/relay/admin/api/pricing/tiers/reset', requireAdminKey, (req, res) => {
    const { tierId } = req.body || {};
    if (!tierId || !UPGRADE_TIER_BUNDLE_PRICE_BASE[tierId]) {
        return res.status(400).json({ success: false, message: 'Invalid o hindi kilalang tierId (basic/standard/pro).' });
    }
    delete upgradeTierPricingOverrides[tierId];
    saveUpgradeTierPricingOverrides(upgradeTierPricingOverrides);
    recomputeUpgradeTierPricing();
    console.log(`💳 Na-reset sa default ang bundle pricing ng tier "${tierId}" via admin panel.`);
    const tier = UPGRADE_TIERS.find(t => t.id === tierId);
    res.json({ success: true, tier });
});
app.get('/relay/admin/api/pricing/suggested-discount-percent', requireAdminKey, (req, res) => {
    res.json({ success: true, suggestedDiscountPercent: SUGGESTED_DISCOUNT_PERCENT });
});
app.post('/relay/admin/api/pricing/suggested-discount-percent', requireAdminKey, (req, res) => {
    const { percent } = req.body || {};
    if (typeof percent !== 'number' || !Number.isFinite(percent) || percent < 0 || percent > 90) {
        return res.status(400).json({ success: false, message: 'Ang percent ay dapat isang numero sa pagitan ng 0 at 90.' });
    }
    SUGGESTED_DISCOUNT_PERCENT = percent;
    saveSuggestedDiscountPercent(percent);
    console.log(`💳 Na-update ang default suggested discount % sa ${percent}% via admin panel.`);
    res.json({ success: true, suggestedDiscountPercent: SUGGESTED_DISCOUNT_PERCENT });
});
app.get('/relay/admin/api/analytics', requireAdminKey, (req, res) => {
    const now = Date.now();
    const SOON_MS = 7 * 24 * 60 * 60 * 1000; 
    let activeUnlocksCount = 0;
    let allTimeUnlocksCount = 0;
    let activeRevenue = 0;
    let allTimeRevenue = 0;
    let demoActiveCount = 0;
    const featureCounts = {};
    const featureRevenue = {};
    const cloudBackupTierCounts = {};
    const expiringSoon = [];
    for (const [installationId, record] of Object.entries(issuedUnlocks)) {
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
                featureRevenue[featureId] = (featureRevenue[featureId] || 0) + (entry.price || 0);
                if (featureId === 'cloud_backup' && entry.tier) {
                    cloudBackupTierCounts[entry.tier] = (cloudBackupTierCounts[entry.tier] || 0) + 1;
                }
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
            count,
            revenue: featureRevenue[featureId] || 0
        }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 8);
    const cloudBackupTierPopularity = Object.entries(cloudBackupTierCounts)
        .map(([tier, count]) => ({ tier, name: (CLOUD_BACKUP_PLANS[tier] && CLOUD_BACKUP_PLANS[tier].name) || tier, count }))
        .sort((a, b) => b.count - a.count);
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
            cloudBackupTierPopularity,
            expiringSoon: expiringSoon.slice(0, 20)
        }
    });
});
app.post('/relay/admin/api/analytics/reset', requireAdminKey, (req, res) => {
    const confirm = req.query.confirm || req.body?.confirm;
    if (confirm !== 'RESET') {
        return res.status(400).json({
            success: false,
            message: 'Safety check: kailangan ng ?confirm=RESET (o "confirm":"RESET" sa JSON body) para i-reset ang Analytics. Hindi na ito mababawi.'
        });
    }
    const clearedActivityCount = activityLog.length;
    activityLog = [];
    saveActivityLog(activityLog);
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
app.post('/relay/admin/api/devices/:installationId/clear-history', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }
    const before = activityLog.length;
    activityLog = activityLog.filter(entry => entry.installationId !== installationId);
    const clearedCount = before - activityLog.length;
    saveActivityLog(activityLog);
    logActivity(installationId, 'device_history_cleared', { clearedCount });
    res.json({ success: true, clearedCount });
});
app.post('/relay/admin/api/devices/:installationId/activate', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    const { featureId, featureIds, tierId, note, durationDays, tier, billingCycle } = req.body;
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
    const includesCloudBackup = idsToActivate.includes('cloud_backup');
    let cloudBackupPrice = null;
    if (includesCloudBackup) {
        if (!tier || !CLOUD_BACKUP_PLANS_BASE[tier]) {
            return res.status(400).json({ success: false, message: 'Kulang o invalid ang tier (basic/standard/pro) para sa Cloud Backup.' });
        }
        if (!billingCycle || !CLOUD_BACKUP_BILLING_DAYS[billingCycle]) {
            return res.status(400).json({ success: false, message: 'Kulang o invalid ang billingCycle (monthly/yearly) para sa Cloud Backup.' });
        }
        cloudBackupPrice = getCloudBackupPlanPrice(tier, billingCycle);
    }
    const includesModuleSubscription = idsToActivate.some(id => isModuleSubscriptionFeature(id));
    if (includesModuleSubscription) {
        if (!billingCycle || !MODULE_SUBSCRIPTION_BILLING_DAYS[billingCycle]) {
            return res.status(400).json({ success: false, message: 'Missing or invalid billingCycle (monthly/yearly) for the module subscription feature(s).' });
        }
    }
    const resolvedDurationDays = (typeof durationDays === 'number' && durationDays > 0)
        ? durationDays
        : (durationDays === 0 ? null : RELAY_DEFAULT_LICENSE_DAYS);
    const durationMs = (typeof resolvedDurationDays === 'number' && resolvedDurationDays > 0)
        ? resolvedDurationDays * 24 * 60 * 60 * 1000
        : null;
    const cloudBackupDurationMs = includesCloudBackup
        ? ((typeof durationDays === 'number' && durationDays > 0) ? durationMs : CLOUD_BACKUP_BILLING_DAYS[billingCycle] * 24 * 60 * 60 * 1000)
        : null;
    const cloudBackupDurationDays = includesCloudBackup
        ? ((typeof durationDays === 'number' && durationDays > 0) ? resolvedDurationDays : CLOUD_BACKUP_BILLING_DAYS[billingCycle])
        : null;
    const moduleSubscriptionDurationMs = includesModuleSubscription
        ? ((typeof durationDays === 'number' && durationDays > 0) ? durationMs : MODULE_SUBSCRIPTION_BILLING_DAYS[billingCycle] * 24 * 60 * 60 * 1000)
        : null;
    const moduleSubscriptionDurationDays = includesModuleSubscription
        ? ((typeof durationDays === 'number' && durationDays > 0) ? resolvedDurationDays : MODULE_SUBSCRIPTION_BILLING_DAYS[billingCycle])
        : null;
    const tokens = {};
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
        const isThisCloudBackup = id === 'cloud_backup';
        const isThisModuleSubscription = isModuleSubscriptionFeature(id);
        const tokenDurationMs = isThisCloudBackup ? cloudBackupDurationMs : (isThisModuleSubscription ? moduleSubscriptionDurationMs : durationMs);
        const token = issueSignedToken(installationId, id, tokenDurationMs);
        tokens[id] = token;
        const priceForThisFeature = isThisCloudBackup
            ? cloudBackupPrice
            : isThisModuleSubscription
                ? getModuleSubscriptionPrice(id, billingCycle)
                : (Object.prototype.hasOwnProperty.call(perFeaturePrice, id) ? perFeaturePrice[id] : FEATURE_CATALOG[id].price);
        const featureNameForThisFeature = isThisCloudBackup
            ? CLOUD_BACKUP_PLANS[tier].name
            : isThisModuleSubscription
                ? MODULE_SUBSCRIPTION_PLANS[id].name
                : FEATURE_CATALOG[id].name;
        recordIssuedUnlock(installationId, id, token, {
            featureName: featureNameForThisFeature,
            price: priceForThisFeature,
            source: 'admin-direct',
            note: note || null,
            tier: isThisCloudBackup ? tier : null,
            billingCycle: (isThisCloudBackup || isThisModuleSubscription) ? billingCycle : null
        });
        logActivity(installationId, 'unlock_issued', {
            featureId: id,
            featureName: featureNameForThisFeature,
            source: 'admin-direct',
            note: note || null,
            durationDays: isThisCloudBackup ? cloudBackupDurationDays : (isThisModuleSubscription ? moduleSubscriptionDurationDays : (resolvedDurationDays || null)),
            tier: isThisCloudBackup ? tier : null,
            billingCycle: (isThisCloudBackup || isThisModuleSubscription) ? billingCycle : null
        });
    }
    res.json({ success: true, message: `Na-activate ang ${idsToActivate.length} feature(s).`, tokens });
});
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
app.get('/relay/admin/api/backup', requireAdminKey, (req, res) => {
    res.json({
        success: true,
        backupVersion: 2, 
        exportedAt: Date.now(),
        allowedDevices: [...allowedDevices],
        deviceLabels: Object.fromEntries(deviceLabels),
        issuedUnlocks,
        activityLog
    });
});
app.post('/relay/admin/api/restore', requireAdminKey, (req, res) => {
    const { allowedDevices: backupAllowed, deviceLabels: backupLabels, issuedUnlocks: backupUnlocks, activityLog: backupLog } = req.body;
    if (!Array.isArray(backupAllowed) || typeof backupUnlocks !== 'object' || backupUnlocks === null || !Array.isArray(backupLog)) {
        return res.status(400).json({ success: false, message: 'Hindi kilalang format ng backup file — siguraduhing yung na-download galing sa /backup ang ini-restore.' });
    }
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
app.post('/relay/restore-tokens', requireApiKey, requireAllowedDevice, rateLimit('restore-tokens', 30, 10 * 60 * 1000), (req, res) => {
    const { installationId } = req.body;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }
    const record = issuedUnlocks[installationId] || {};
    const now = Date.now();
    const tokens = {};
    for (const [featureId, entry] of Object.entries(record)) {
        if (typeof entry.expiresAt === 'number' && now > entry.expiresAt) continue; 
        tokens[featureId] = { payload: entry.payload, signature: entry.signature };
    }
    logActivity(installationId, 'restore_checkin', { restoredCount: Object.keys(tokens).length });
    res.json({ success: true, tokens });
});
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
    let forceIntegrityCheck = false;
    if (pendingIntegrityChecks.has(installationId)) {
        forceIntegrityCheck = true;
        pendingIntegrityChecks.delete(installationId);
    }
    res.json({ success: true, statuses, forceIntegrityCheck });
});
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
function issueSignedToken(installationId, featureId, durationMs) {
    const now = Date.now();
    const payload = (typeof durationMs === 'number' && durationMs > 0)
        ? { installationId, featureId, issuedAt: now, expiresAt: now + durationMs }
        : { installationId, featureId, issuedAt: now };
    const payloadString = JSON.stringify(payload);
    const signature = crypto.sign(null, Buffer.from(payloadString), privateKey).toString('base64');
    return { payload, signature };
}
function issueDevicePermit(installationId, fingerprint) {
    const payload = { installationId, fingerprint, issuedAt: Date.now() };
    const payloadString = JSON.stringify(payload);
    const signature = crypto.sign(null, Buffer.from(payloadString), privateKey).toString('base64');
    return { payload, signature };
}
const pendingOtps = new Map(); 
function requireApiKey(req, res, next) {
    if (!RELAY_API_KEY) return next(); 
    const provided = req.headers['x-relay-key'];
    if (!safeCompare(String(provided || ''), RELAY_API_KEY)) {
        return res.status(403).json({ success: false, message: 'Invalid o walang API key.' });
    }
    next();
}
const rateBuckets = new Map();
const RATE_BUCKET_CLEANUP_INTERVAL_MS = 30 * 60 * 1000;
const RATE_BUCKET_MAX_AGE_MS = 60 * 60 * 1000; 
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
            }
        }
        const now = Date.now();
        const bucket = rateBuckets.get(key) || [];
        const recent = bucket.filter(ts => now - ts < windowMs);
        if (recent.length >= max) {
            const earliestTs = recent[0];
            const retryAfterSec = Math.max(1, Math.ceil((earliestTs + windowMs - now) / 1000));
            res.set('Retry-After', String(retryAfterSec));
            return res.status(429).json({
                success: false,
                message: 'Sobra sa pinapayagang bilang ng requests. Subukan mamaya.',
                retryAfterSec
            });
        }
        recent.push(now);
        rateBuckets.set(key, recent);
        next();
    };
}
app.get('/relay/pricing', requireApiKey, rateLimit('pricing', 120, 60 * 60 * 1000), (req, res) => {
    const featureCatalog = {};
    for (const [featureId, entry] of Object.entries(FEATURE_CATALOG)) {
        if (isSubscriptionOnlyFeature(featureId)) continue;
        featureCatalog[featureId] = { name: entry.name, price: entry.price, category: entry.category };
    }
    const upgradeTiers = {};
    for (const tier of UPGRADE_TIERS) {
        upgradeTiers[tier.id] = { name: tier.name, bundlePrice: tier.bundlePrice, featureIds: tier.featureIds };
    }
    res.json({
        success: true,
        cloudBackupPlans: CLOUD_BACKUP_PLANS,
        billingDays: CLOUD_BACKUP_BILLING_DAYS,
        featureCatalog,
        upgradeTiers,
        moduleSubscriptions: MODULE_SUBSCRIPTION_PLANS,
        moduleSubscriptionBillingDays: MODULE_SUBSCRIPTION_BILLING_DAYS,
        moduleSubscriptionGracePeriodDays: MODULE_SUBSCRIPTION_GRACE_PERIOD_DAYS,
        fetchedAt: new Date().toISOString()
    });
});
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
app.post('/relay/integrity-checkin', requireApiKey, requireAllowedDevice, rateLimit('integrity-checkin', 300, 60 * 60 * 1000, (req) => req.body?.installationId), (req, res) => {
    const { installationId, version, files, watcherActive } = req.body || {};
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
    if (changed) {   }
    res.json({ success: true, message: 'Branch check-in recorded.' });
});
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
const MULTI_TERMINAL_DISCOUNT_TIERS_BASE = [
    { minDevices: 7, percent: 15 },
    { minDevices: 4, percent: 10 },
    { minDevices: 2, percent: 5 }
]; 
function getGroupDeviceCount(installationId) {
    for (const hash of Object.keys(branchSummaries)) {
        const group = branchSummaries[hash];
        if (group && Object.prototype.hasOwnProperty.call(group, installationId)) {
            return Object.keys(group).length;
        }
    }
    return 1; 
}
function getMultiTerminalDiscountPercent(deviceCount) {
    for (const t of MULTI_TERMINAL_DISCOUNT_TIERS_BASE) {
        if (deviceCount >= t.minDevices) return t.percent;
    }
    return 0;
}
app.get('/relay/pricing/group-discount', requireApiKey, requireAllowedDevice, rateLimit('group-discount', 120, 60 * 60 * 1000, (req) => req.query?.installationId), (req, res) => {
    const installationId = String(req.query.installationId || '');
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId query param.' });
    }
    const deviceCount = getGroupDeviceCount(installationId);
    const discountPercent = getMultiTerminalDiscountPercent(deviceCount);
    res.json({ success: true, deviceCount, discountPercent });
});
function isFeatureCurrentlyUnlocked(installationId, featureId) {
    const entry = (issuedUnlocks[installationId] || {})[featureId];
    if (!entry) return false;
    if (typeof entry.expiresAt === 'number' && Date.now() > entry.expiresAt) return false;
    return true;
}
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
const CLOUD_BACKUP_MAX_CHUNK_SIZE_BYTES = 5 * 1024 * 1024;
const CLOUD_BACKUP_CHUNK_SIZE_BYTES = CLOUD_BACKUP_MAX_CHUNK_SIZE_BYTES;
const CLOUD_BACKUP_CHUNK_UPLOAD_SESSIONS = new Map();
const CLOUD_BACKUP_UPLOAD_LOCKS = new Map();
const CLOUD_BACKUP_CHUNK_SESSION_IDLE_TIMEOUT_MS = 10 * 60 * 1000;
function cleanupCloudBackupUploadSession(uploadId) {
    const session = CLOUD_BACKUP_CHUNK_UPLOAD_SESSIONS.get(uploadId);
    if (!session) return;
    CLOUD_BACKUP_CHUNK_UPLOAD_SESSIONS.delete(uploadId);
    if (CLOUD_BACKUP_UPLOAD_LOCKS.get(session.installationId) === uploadId) {
        CLOUD_BACKUP_UPLOAD_LOCKS.delete(session.installationId);
    }
    if (session.idleTimer) clearTimeout(session.idleTimer);
}
function armCloudBackupSessionIdleTimer(uploadId) {
    const session = CLOUD_BACKUP_CHUNK_UPLOAD_SESSIONS.get(uploadId);
    if (!session) return;
    if (session.idleTimer) clearTimeout(session.idleTimer);
    session.idleTimer = setTimeout(() => {
        console.warn(`⚠️ CLOUD_BACKUP: inabandona ang upload session ${uploadId} (walang bagong chunk sa loob ng ${CLOUD_BACKUP_CHUNK_SESSION_IDLE_TIMEOUT_MS / 60000} min) — nililinis, pinapalaya ang lock.`);
        cleanupCloudBackupUploadSession(uploadId);
    }, CLOUD_BACKUP_CHUNK_SESSION_IDLE_TIMEOUT_MS);
    if (typeof session.idleTimer.unref === 'function') session.idleTimer.unref();
}
app.post('/relay/cloud-backup/upload/start', requireApiKey, requireAllowedDevice, rateLimit('cloud-backup-upload-start', 30, 60 * 60 * 1000, (req) => req.body?.installationId), (req, res) => {
    const { installationId, totalBytes } = req.body || {};
    if (!installationId || typeof totalBytes !== 'number' || totalBytes <= 0) {
        return res.status(400).json({ success: false, message: 'Missing or invalid installationId/totalBytes.' });
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
    const existingUploadId = CLOUD_BACKUP_UPLOAD_LOCKS.get(installationId);
    if (existingUploadId && CLOUD_BACKUP_CHUNK_UPLOAD_SESSIONS.has(existingUploadId)) {
        return res.status(409).json({
            success: false,
            uploadInProgress: true,
            message: 'A cloud backup upload for this installation is already in progress. Please wait for it to finish before starting another.'
        });
    }
    const cloudBackupUnlockForQuota = (issuedUnlocks[installationId] || {})['cloud_backup'];
    const tier = (cloudBackupUnlockForQuota && cloudBackupUnlockForQuota.tier && CLOUD_BACKUP_PLANS[cloudBackupUnlockForQuota.tier]) ? cloudBackupUnlockForQuota.tier : 'basic';
    const quotaMB = CLOUD_BACKUP_PLANS[tier].storageQuotaMB;
    const totalMB = Math.round((totalBytes / (1024 * 1024)) * 100) / 100;
    if (totalMB > quotaMB) {
        logActivity(installationId, 'cloud_backup_blocked', { reason: 'storage_quota_exceeded', tier, quotaMB, sizeMB: totalMB });
        return res.status(413).json({
            success: false,
            storageQuotaExceeded: true,
            tier,
            quotaMB,
            sizeMB: totalMB,
            overageMB: Math.round((totalMB - quotaMB) * 100) / 100,
            message: `Cloud backup exceeds your ${CLOUD_BACKUP_PLANS[tier].name} storage allowance (${totalMB} MB used, ${quotaMB} MB limit). Upgrade your Cloud Backup plan or free up space (e.g., trim old transaction/userlog history) before syncing.`
        });
    }
    const uploadId = crypto.randomUUID();
    CLOUD_BACKUP_CHUNK_UPLOAD_SESSIONS.set(uploadId, {
        installationId,
        totalBytes,
        receivedBytes: 0,
        chunks: [],
        tier,
        quotaMB,
        createdAt: Date.now(),
        idleTimer: null
    });
    CLOUD_BACKUP_UPLOAD_LOCKS.set(installationId, uploadId);
    armCloudBackupSessionIdleTimer(uploadId);
    res.json({ success: true, uploadId, chunkSizeBytes: CLOUD_BACKUP_CHUNK_SIZE_BYTES });
});
app.post('/relay/cloud-backup/upload/chunk', requireApiKey, requireAllowedDevice, rateLimit('cloud-backup-upload-chunk', 5000, 60 * 60 * 1000, (req) => req.query?.installationId), (req, res) => {
    const uploadId = String(req.query.uploadId || '');
    const installationId = String(req.query.installationId || '');
    const session = uploadId && CLOUD_BACKUP_CHUNK_UPLOAD_SESSIONS.get(uploadId);
    if (!session || session.installationId !== installationId) {
        return res.status(404).json({ success: false, message: 'Unknown or expired upload session. Start a new cloud backup upload.' });
    }
    const chunk = req.body;
    if (!Buffer.isBuffer(chunk) || chunk.length === 0) {
        return res.status(400).json({ success: false, message: 'Empty or invalid chunk.' });
    }
    if (session.receivedBytes + chunk.length > session.totalBytes) {
        cleanupCloudBackupUploadSession(uploadId);
        return res.status(400).json({ success: false, message: 'Received more bytes than declared at upload start — aborting session. Please start a new sync.' });
    }
    session.chunks.push(chunk);
    session.receivedBytes += chunk.length;
    armCloudBackupSessionIdleTimer(uploadId);
    res.json({ success: true, receivedBytes: session.receivedBytes, totalBytes: session.totalBytes });
});
app.post('/relay/cloud-backup/upload/finish', requireApiKey, requireAllowedDevice, rateLimit('cloud-backup-upload-finish', 30, 60 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
    const { uploadId, installationId } = req.body || {};
    const session = uploadId && CLOUD_BACKUP_CHUNK_UPLOAD_SESSIONS.get(String(uploadId));
    if (!session || session.installationId !== installationId) {
        return res.status(404).json({ success: false, message: 'Unknown or expired upload session. Start a new cloud backup upload.' });
    }
    if (session.receivedBytes !== session.totalBytes) {
        return res.status(400).json({
            success: false,
            message: `Incomplete upload — received ${session.receivedBytes} of ${session.totalBytes} declared bytes. No partial backup was saved; keep sending the remaining chunks or start a new sync.`
        });
    }
    let parsedBody;
    try {
        const fullBuffer = Buffer.concat(session.chunks, session.receivedBytes);
        parsedBody = JSON.parse(fullBuffer.toString('utf8'));
    } catch (err) {
        cleanupCloudBackupUploadSession(String(uploadId));
        return res.status(400).json({ success: false, message: 'Could not parse the assembled backup data as JSON — the upload may have been corrupted in transit. Please try syncing again.' });
    }
    const { storeName, modules, moduleNames, totalRecords } = parsedBody || {};
    if (!modules || typeof modules !== 'object') {
        cleanupCloudBackupUploadSession(String(uploadId));
        return res.status(400).json({ success: false, message: 'Missing or invalid modules in the assembled backup data.' });
    }
    if (!isFeatureCurrentlyUnlocked(installationId, 'cloud_backup')) {
        cleanupCloudBackupUploadSession(String(uploadId));
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
        cleanupCloudBackupUploadSession(String(uploadId));
        return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured yet on RELAY. Tell the developer to set it.' });
    }
    const serializedModules = {};
    let projectedSizeBytes = 0;
    for (const [moduleName, rawData] of Object.entries(modules)) {
        const data = stripCloudBackupRedactedFields(moduleName, rawData);
        const serialized = JSON.stringify(data);
        serializedModules[moduleName] = { data, serialized, recordCount: Array.isArray(data) ? data.length : 0 };
        projectedSizeBytes += Buffer.byteLength(serialized, 'utf8');
    }
    const projectedSizeMB = Math.round((projectedSizeBytes / (1024 * 1024)) * 100) / 100;
    const { tier, quotaMB } = session;
    if (projectedSizeMB > quotaMB) {
        cleanupCloudBackupUploadSession(String(uploadId));
        logActivity(installationId, 'cloud_backup_blocked', { reason: 'storage_quota_exceeded', tier, quotaMB, sizeMB: projectedSizeMB });
        return res.status(413).json({
            success: false,
            storageQuotaExceeded: true,
            tier,
            quotaMB,
            sizeMB: projectedSizeMB,
            overageMB: Math.round((projectedSizeMB - quotaMB) * 100) / 100,
            message: `Cloud backup exceeds your ${CLOUD_BACKUP_PLANS[tier].name} storage allowance (${projectedSizeMB} MB used, ${quotaMB} MB limit). Upgrade your Cloud Backup plan or free up space (e.g., trim old transaction/userlog history) before syncing.`
        });
    }
    try {
        let totalSizeBytes = 0;
        let moduleCount = 0;
        const failedModules = [];
        for (const [moduleName, entry] of Object.entries(serializedModules)) {
            const sizeBytes = Buffer.byteLength(entry.serialized, 'utf8');
            try {
                await runPgWriteTx(pgPool, async (client) => {
                    await client.query(
                        `INSERT INTO cloud_backup_modules (installation_id, module, data, record_count, size_bytes, updated_at)
                         VALUES ($1, $2, $3, $4, $5, now())
                         ON CONFLICT (installation_id, module) DO UPDATE SET
                            data = excluded.data, record_count = excluded.record_count, size_bytes = excluded.size_bytes, updated_at = excluded.updated_at`,
                        [installationId, moduleName, entry.serialized, entry.recordCount, sizeBytes]
                    );
                });
                totalSizeBytes += sizeBytes;
                moduleCount++;
            } catch (moduleErr) {
                failedModules.push({
                    module: moduleName,
                    message: moduleErr.message,
                    transient: isTransientPgConnectionError(moduleErr),
                    sizeMB: Math.round((sizeBytes / (1024 * 1024)) * 100) / 100
                });
            }
        }
        if (failedModules.length > 0) {
            const failedList = failedModules.map(f => `${f.module} (${f.sizeMB} MB)`).join(', ');
            console.error(`⚠️ CLOUD_BACKUP: hindi na-save ang ${failedModules.length} module(s) para sa ${installationId}: ${failedList}`);
            const anyTransient = failedModules.some(f => f.transient);
            const friendlyMessage = anyTransient
                ? `Lost connection to the database while saving ${failedList} (this can happen with very large modules, e.g. many high-resolution product photos). The other module(s) were already saved successfully — please try syncing again; only the module(s) above still need to go through.`
                : `An error occurred while saving ${failedList} to Postgres: ${failedModules[0].message}`;
            return res.status(500).json({ success: false, message: friendlyMessage, failedModules: failedModules.map(f => f.module) });
        }
        await runPgWriteTx(pgPool, async (client) => {
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
        const tierForResponse = tier;
        const quotaMBForResponse = quotaMB;
        const sizeMBForResponse = Math.round((totalSizeBytes / (1024 * 1024)) * 100) / 100;
        const percentUsedForResponse = quotaMBForResponse > 0 ? Math.round(Math.min(100, (sizeMBForResponse / quotaMBForResponse) * 100) * 10) / 10 : 0;
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
    } finally {
        cleanupCloudBackupUploadSession(String(uploadId));
    }
});
app.get('/relay/cloud-backup/usage', requireApiKey, requireAllowedDevice, rateLimit('cloud-backup-usage', 60, 60 * 60 * 1000, (req) => req.query?.installationId), async (req, res) => {
    const installationId = String(req.query.installationId || '').trim();
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId.' });
    }
    if (!pgPool) {
        return res.status(503).json({ success: false, message: 'Hindi pa naka-configure ang Postgres (DATABASE_URL).' });
    }
    const cloudBackupUnlock = (issuedUnlocks[installationId] || {})['cloud_backup'];
    const tier = (cloudBackupUnlock && cloudBackupUnlock.tier && CLOUD_BACKUP_PLANS[cloudBackupUnlock.tier]) ? cloudBackupUnlock.tier : 'basic';
    const quotaMB = CLOUD_BACKUP_PLANS[tier].storageQuotaMB;
    try {
        const result = await queryWithRetry(
            pgPool,
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
app.get('/relay/admin/api/cloud-backup', requireAdminKey, async (req, res) => {
    if (!pgPool) return res.status(503).json({ success: false, message: 'Hindi pa naka-configure ang Postgres (DATABASE_URL).' });
    try {
        const result = await queryWithRetry(pgPool, 'SELECT installation_id, store_name, total_records, module_count, size_bytes, last_sync_at, sync_count FROM cloud_backup_meta ORDER BY last_sync_at DESC NULLS LAST');
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
        const metaResult = await queryWithRetry(pgPool, 'SELECT * FROM cloud_backup_meta WHERE installation_id = $1', [installationId]);
        const modulesResult = await queryWithRetry(pgPool, 'SELECT module, data, record_count, size_bytes, updated_at FROM cloud_backup_modules WHERE installation_id = $1 ORDER BY module', [installationId]);
        if (!metaResult.rows[0]) {
            return res.status(404).json({ success: false, message: 'Walang cloud backup na nakita para sa installationId na ito.' });
        }
        const sizeBytesForMeta = Number(metaResult.rows[0].size_bytes || 0);
        const sizeMBForMeta = Math.round((sizeBytesForMeta / (1024 * 1024)) * 100) / 100;
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
        const modulesResult = await queryWithRetry(pgPool, 'SELECT module, data FROM cloud_backup_modules WHERE installation_id = $1', [installationId]);
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
        const metaResult = await queryWithRetry(pgPool, 'SELECT * FROM cloud_backup_meta WHERE installation_id = $1', [installationId]);
        if (!metaResult.rows[0]) {
            return res.status(404).json({ success: false, message: 'Walang cloud backup na nakita para sa installation na ito.' });
        }
        const modulesResult = await queryWithRetry(pgPool, 'SELECT module, data, record_count FROM cloud_backup_modules WHERE installation_id = $1', [installationId]);
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
            redactedFieldsByModule: CLOUD_BACKUP_REDACTED_FIELDS_BY_MODULE
        });
    } catch (err) {
        console.error('⚠️ CLOUD_BACKUP: hindi na-kuha mula sa Postgres:', err.message);
        res.status(500).json({ success: false, message: 'May error habang kinukuha mula sa Postgres: ' + err.message });
    }
});
app.post('/relay/verify-login', requireApiKey, rateLimit('verify-login', 30, 10 * 60 * 1000, (req) => req.body?.installationId), (req, res) => {
    const { installationId, hardwareFingerprint, storeName, username } = req.body;
    if (!installationId || !hardwareFingerprint) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId o hardwareFingerprint.' });
    }
    recordDeviceSeen(installationId, { storeName, username });
    const existing = deviceFingerprints.get(installationId);
    if (!existing) {
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
        return res.status(403).json({
            success: false,
            cloneSuspected: true,
            message: 'Naka-flag ang device na ito bilang posibleng clone/duplicate. Kontakin ang developer/store owner para i-review at i-reset.'
        });
    }
    if (existing.fingerprint !== hardwareFingerprint) {
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
app.get('/relay/admin/api/devices/:installationId/fingerprint', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    const record = deviceFingerprints.get(installationId) || null;
    res.json({ success: true, record });
});
app.post('/relay/admin/api/devices/:installationId/fingerprint/reset', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    deviceFingerprints.delete(installationId);
    saveDeviceFingerprints(deviceFingerprints);
    logActivity(installationId, 'device_fingerprint_reset', {});
    res.json({ success: true, message: 'Na-clear ang fingerprint binding — kailangan na namang mag-verify online sa susunod na login.' });
});
app.get('/relay/admin/api/devices/:installationId/integrity', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    const record = integrityStatus[installationId] || null;
    res.json({ success: true, record });
});
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
app.post('/relay/admin/api/devices/:installationId/integrity/check-now', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    pendingIntegrityChecks.add(installationId);
    logActivity(installationId, 'integrity_check_requested', {});
    res.json({ success: true, message: 'Hihintayin ang susunod na online check-in ng device na ito (karaniwan ay ilang segundo hanggang ~30s).' });
});
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
    deviceFingerprints.set(newInstallationId, {
        fingerprint: flaggedFingerprint,
        firstVerifiedAt: Date.now(),
        lastVerifiedAt: Date.now(),
        verifyCount: 1,
        flagged: false
    });
    record.flagged = false;
    delete record.flaggedFingerprint;
    delete record.flaggedAt;
    saveDeviceFingerprints(deviceFingerprints);
    cloneSplits.set(cloneSplitKey(installationId, flaggedFingerprint), {
        newInstallationId,
        splitAt: Date.now()
    });
    saveCloneSplits(cloneSplits);
    if (allowedDevices.has(installationId)) {
        allowedDevices.add(newInstallationId);
        saveAllowedDevices(allowedDevices);
    }
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
app.post('/relay/request-unlock', requireApiKey, requireAllowedDevice, rateLimit('request-unlock', 5, 10 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
    const { installationId, featureId, featureName, price, username, storeName, photo, tier, billingCycle } = req.body;
    if (!installationId || !featureId) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId o featureId.' });
    }
    const isCloudBackup = featureId === 'cloud_backup';
    const isModuleSubscription = isModuleSubscriptionFeature(featureId);
    let groundTruthPrice = null;
    if (isCloudBackup) {
        groundTruthPrice = getCloudBackupPlanPrice(tier, billingCycle);
        if (groundTruthPrice === null) {
            return res.status(400).json({ success: false, message: 'Invalid Cloud Backup tier/billingCycle.' });
        }
    } else if (isModuleSubscription) {
        groundTruthPrice = getModuleSubscriptionPrice(featureId, billingCycle);
        if (groundTruthPrice === null) {
            return res.status(400).json({ success: false, message: 'Invalid billing cycle (monthly/yearly) for this subscription module.' });
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
        featureName: isCloudBackup ? CLOUD_BACKUP_PLANS[tier].name : isModuleSubscription ? MODULE_SUBSCRIPTION_PLANS[featureId].name : (featureName || featureId),
        price: (isCloudBackup || isModuleSubscription) ? groundTruthPrice : (price || null),
        tier: isCloudBackup ? tier : null,
        billingCycle: (isCloudBackup || isModuleSubscription) ? billingCycle : null,
        durationDays: isCloudBackup ? CLOUD_BACKUP_BILLING_DAYS[billingCycle] : isModuleSubscription ? MODULE_SUBSCRIPTION_BILLING_DAYS[billingCycle] : undefined
    });
    try {
        const catalogEntry = FEATURE_CATALOG[featureId] || null;
        const isAnySubscription = isCloudBackup || isModuleSubscription;
        const displayPrice = isAnySubscription ? groundTruthPrice : price;
        const displayName = isCloudBackup ? CLOUD_BACKUP_PLANS[tier].name : isModuleSubscription ? MODULE_SUBSCRIPTION_PLANS[featureId].name : (featureName || featureId);
        const priceMismatch = !isAnySubscription && catalogEntry && typeof price === 'number' && price !== catalogEntry.price;
        const nameMismatch = !isAnySubscription && catalogEntry && featureName && featureName !== catalogEntry.name;
        await notifyUnlockRequest({
            subject: `🎨 Unlock Request — ${displayName}${displayPrice ? ` (₱${displayPrice})` : ''}`,
            text: `Someone requested to ${isCloudBackup ? 'subscribe/renew Cloud Backup' : isModuleSubscription ? `subscribe/renew ${displayName}` : 'unlock a Pro theme'}.\n\n` +
                  `Store: ${storeName || 'Not specified'}\n` +
                  `Installation ID: ${installationId}\n` +
                  `Feature: ${displayName}\n` +
                  (isCloudBackup ? `Plan: ${tier} (${billingCycle}) — ₱${groundTruthPrice}\n` : '') +
                  (isModuleSubscription ? `Plan: ${billingCycle} — ₱${groundTruthPrice}\n` : '') +
                  (isAnySubscription ? `Access to be granted: ${isCloudBackup ? CLOUD_BACKUP_BILLING_DAYS[billingCycle] : MODULE_SUBSCRIPTION_BILLING_DAYS[billingCycle]} days from approval\n` : '') +
                  (!isAnySubscription && price ? `Price (stated by client): ₱${price}\n` : '') +
                  (!isAnySubscription && catalogEntry ? `Price per our price list: ₱${catalogEntry.price} (${catalogEntry.name})\n` : (!isAnySubscription ? `⚠️ featureId "${featureId}" was not found in our price list — be careful.\n` : '')) +
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
const DEMO_FEATURE_ID = '__demo__';
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
app.post('/relay/request-unlock-bulk', requireApiKey, requireAllowedDevice, rateLimit('request-unlock-bulk', 5, 10 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
    const { installationId, featureIds, featureNames, totalPrice, username, storeName, photo } = req.body;
    if (!installationId || !Array.isArray(featureIds) || featureIds.length === 0) {
        return res.status(400).json({ success: false, message: 'Kulang ang installationId o featureIds.' });
    }
    const subscriptionIdsInBulk = featureIds.filter(id => isSubscriptionOnlyFeature(id));
    if (subscriptionIdsInBulk.length) {
        return res.status(400).json({ success: false, message: `These are subscription features and cannot be bundled into a one-time bulk unlock: ${subscriptionIdsInBulk.join(', ')}. Please use the subscribe/renew flow for each of them instead.` });
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
    const durationMs = typeof pending.durationDays === 'number' && pending.durationDays > 0
        ? pending.durationDays * 24 * 60 * 60 * 1000
        : null;
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
            let remainder = Math.round(pending.price) - allocated;
            if (remainder !== 0 && featureIds.length > 0) {
                const priciestIdx = alaCartePrices.indexOf(Math.max(...alaCartePrices));
                perFeaturePrice[featureIds[priciestIdx]] += remainder;
            }
        } else {
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
const ADMIN_RESET_OTP_TTL_MS = 10 * 60 * 1000; 
const ADMIN_RESET_TICKET_TTL_MS = 5 * 60 * 1000; 
const MAX_FAILED_OTP_ATTEMPTS = 5; 
const pendingAdminResets = new Map();
function generateAdminResetOtp() {
    return String(Math.floor(100000 + Math.random() * 900000));
}
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
const RECEIPT_RESET_OTP_TTL_MS = 10 * 60 * 1000; 
const RECEIPT_RESET_TICKET_TTL_MS = 5 * 60 * 1000; 
const pendingReceiptResets = new Map();
function generateReceiptResetOtp() {
    return String(Math.floor(100000 + Math.random() * 900000));
}
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
app.get('/relay/latest-version', requireApiKey, rateLimit('latest-version', 60, 10 * 60 * 1000), (req, res) => {
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
const BUILD_EXCLUDE_NAMES = new Set([
    '.git', 'node_modules', 'database', 'release', 'uploads_tmp',
    '.start.sh.lock', '.self-update-backup', 'package-lock.json',
    'build-release.js', 'obfuscate-worker.js', 'start.sh.bak', 'vacuum-now.js',
    'cloud-backup-pricing-cache.json',
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
const RELEASE_CLIENT_TARGETS = new Set([
    path.join('public', 'app.js'),
    path.join('public', 'bt-printer.js'),
    path.join('public', 'faq-engine.js'),
    path.join('public', 'faq-knowledge.js'),
]);
const RELEASE_ENV_LOADER_FILENAME = 'env-loader.js';
const RELEASE_ENV_KEY_FILENAME = '.env.key';
const RELEASE_HTML_TARGETS = new Set([
    path.join('public', 'index.html'),
    path.join('public', 'customer-display.html'),
]);
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
const LARGE_FILE_BYTES = 100 * 1024; 
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
async function obfuscateFileInPlace(fullPath, options) {
    await obfuscateInWorker({ srcPath: fullPath, destPath: fullPath, options });
}
function yieldToEventLoop() {
    return new Promise((resolve) => setImmediate(resolve));
}
function stripHtmlComments(html) {
    return html.replace(/<!--[\s\S]*?-->/g, '');
}
function stripCssComments(css) {
    return css.replace(/\/\*[\s\S]*?\*\//g, '');
}
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
            html = stripHtmlComments(html);
            if (shouldObfuscate) html = await obfuscateHtmlInlineScripts(html, pickReleaseObfOptions(full, true));
            fs.writeFileSync(full, html, 'utf8');
        } else if (type === 'css') {
            const css = fs.readFileSync(full, 'utf8');
            fs.writeFileSync(full, stripCssComments(css), 'utf8');
        }
        obfuscatedCount += 1;
        if (typeof onProgress === 'function') {
            try { onProgress(obfuscatedCount, totalItems); } catch (_) {   }
        }
        await yieldToEventLoop();
    }
    return obfuscatedCount;
}
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
const BUILD_STEPS = ['clone', 'filter', 'env', 'obfuscate', 'zip', 'verify'];
const BUILD_FIRST_STEP_PERCENT = 10;
const BUILD_STEP_END_PERCENT = (() => {
    const map = {};
    const remainingSteps = BUILD_STEPS.length - 1;
    const perRemainingStep = remainingSteps > 0 ? (100 - BUILD_FIRST_STEP_PERCENT) / remainingSteps : 0;
    let cumulative = 0;
    BUILD_STEPS.forEach((step, idx) => {
        cumulative = idx === 0 ? BUILD_FIRST_STEP_PERCENT : cumulative + perRemainingStep;
        map[step] = Math.round(cumulative * 10) / 10; 
    });
    return map;
})();
function buildStepStartPercent(step) {
    const idx = BUILD_STEPS.indexOf(step);
    return idx <= 0 ? 0 : BUILD_STEP_END_PERCENT[BUILD_STEPS[idx - 1]];
}
let buildProgressState = {
    buildId: null,
    stage: 'idle',       
    percent: 0,
    message: '',
    startedAt: null,
    updatedAt: null,
    done: true,
    error: null,
    etaMs: null
};
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
    if (pct < 3) return Math.max(0, estimatedTotalBuildMs() - elapsed);
    const projectedTotal = elapsed * (100 / pct);
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
async function performBuildRelease(reqBody, req, publishOverride) {
    try {
        process.loadEnvFile(path.join(__dirname, '.env'));
    } catch (err) {
    }
    const repoUrl = (reqBody && reqBody.repoUrl) || process.env.OMNIPOS_REPO_URL;
    const ref = (reqBody && reqBody.ref) || 'main';
    const caption = String((reqBody && reqBody.caption) || '').trim();
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
        await new Promise((resolve, reject) => {
            const gitArgs = ['clone', '--depth', '1', '--progress', '--branch', ref, repoUrl, tmpDir];
            const child = spawn('git', gitArgs, { stdio: ['ignore', 'ignore', 'pipe'] });
            let stderrTail = '';
            child.stderr.on('data', (chunk) => {
                stderrTail = (stderrTail + chunk.toString()).slice(-4000);
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
        const relayUrl = (reqBody && reqBody.relayUrl)
            || process.env.RELAY_PUBLIC_URL
            || `${req.protocol}://${req.get('host')}`;
        const relayApiKey = (reqBody && reqBody.relayApiKey) || process.env.RELAY_API_KEY;
        const clientPort = (reqBody && reqBody.port) || 3000;
        if (!relayApiKey) {
            throw new Error('Walang RELAY_API_KEY na naka-set (ni sa request body ni sa RELAY .env) — hindi makakagawa ng client .env.');
        }
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
        const zipStepEnd = BUILD_STEP_END_PERCENT.zip;
        if (fs.existsSync(RELEASE_PACKAGE_TMP_PATH)) {
            fs.rmSync(RELEASE_PACKAGE_TMP_PATH, { force: true });
        }
        await new Promise((resolve, reject) => {
            const output = fs.createWriteStream(RELEASE_PACKAGE_TMP_PATH);
            const archive = archiver('zip', { zlib: { level: 6 } });
            output.on('close', resolve);
            archive.on('error', reject);
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
        setBuildProgress({ stage: 'verify', percent: zipStepEnd, message: 'Sinusuri ang integridad ng bagong zip...' });
        try {
            execSync(`unzip -tq "${RELEASE_PACKAGE_TMP_PATH}"`, { stdio: 'pipe' });
        } catch (zipCheckErr) {
            fs.rmSync(RELEASE_PACKAGE_TMP_PATH, { force: true });
            throw new Error(`Nabuo ang zip pero HINDI ito pumasa sa integrity check (unzip -t) — hindi ito ipapalit sa kasalukuyang release. Detalye: ${zipCheckErr.message}`);
        }
        setBuildProgress({ stage: 'verify', percent: BUILD_STEP_END_PERCENT.verify, message: 'Pumasa sa integrity check — inilalapat na ang bagong release...' });
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
        const baselineVersion = resolvedVersion;
        const baselineFiles = buildFileManifest(tmpDir);
        releaseBaselines.set(baselineVersion, {
            builtAt,
            fileCount: Object.keys(baselineFiles).length,
            files: baselineFiles
        });
        saveReleaseBaselines(releaseBaselines);
        console.log(`🔐 Integrity baseline saved para sa version ${baselineVersion} (${Object.keys(baselineFiles).length} file(s)).`);
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
app.post('/relay/admin/api/build-release', requireAdminKey, async (req, res) => {
    try {
        const result = await performBuildRelease(req.body, req);
        res.json({ success: true, ...result });
    } catch (err) {
        res.status(err.statusCode || 500).json({ success: false, message: err.statusCode ? err.message : `Hindi na-build ang release: ${err.message}` });
    }
});
app.post('/relay/admin/api/system/publish-release', requireAdminKey, async (req, res) => {
    const version = String((req.body && req.body.version) || '').trim();
    if (!version) {
        return res.status(400).json({ success: false, message: 'Kailangan ang "version" (hal. "1.3.0").' });
    }
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
app.get('/relay/admin/api/system/targeted-releases', requireAdminKey, (req, res) => {
    res.json({ success: true, targeted: Object.fromEntries(targetedReleases) });
});
app.post('/relay/admin/api/system/targeted-releases/:installationId/clear', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    const existed = targetedReleases.delete(installationId);
    if (existed) saveTargetedReleases(targetedReleases);
    res.json({ success: true, cleared: existed });
});
app.get('/relay/admin/api/build-history', requireAdminKey, (req, res) => {
    res.json({ success: true, history: buildHistory });
});
app.post('/relay/admin/api/build-history/clear', requireAdminKey, (req, res) => {
    const clearedCount = buildHistory.length;
    buildHistory = [];
    saveBuildHistory(buildHistory);
    logActivity(null, 'build_history_cleared', { clearedCount });
    res.json({ success: true, clearedCount });
});
app.post('/relay/admin/api/download-codes/generate', requireAdminKey, (req, res) => {
    const { label, maxUses, expiresInHours } = req.body || {};
    const code = crypto.randomBytes(9).toString('base64url'); 
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
async function bootstrapStores() {
    // Siguraduhing tapos na ang CREATE TABLE bago mag-SELECT — iwas race condition sa Neon cold-start
    await Promise.all([
        ensureCloudBackupSchema(),
        ensureDeviceLicenseSchema()
    ]);

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
        branchSummaries,
        cloudBackupPlanOverrides,
        featurePricingOverrides,
        upgradeTierPricingOverrides,
        moduleSubscriptionOverrides,
        SUGGESTED_DISCOUNT_PERCENT
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
        loadBranchSummaries(),
        loadCloudBackupPlanOverrides(),
        loadFeaturePricingOverrides(),
        loadUpgradeTierPricingOverrides(),
        loadModuleSubscriptionOverrides(),
        loadSuggestedDiscountPercent()
    ]);
    recomputeFeatureCatalog();
    if (Object.keys(featureCatalogOverrides).length > 0) {
        console.log(`🆕 Na-load ang ${Object.keys(featureCatalogOverrides).length} dating auto-learned na feature(s) papunta sa catalog mirror: ${Object.keys(featureCatalogOverrides).join(', ')}.`);
    }
    if (Object.keys(featurePricingOverrides).length > 0) {
        console.log(`💳 Na-load ang custom na Feature pricing override para sa: ${Object.keys(featurePricingOverrides).join(', ')}.`);
    }
    recomputeCloudBackupPlans();
    if (Object.keys(cloudBackupPlanOverrides).length > 0) {
        console.log(`💳 Na-load ang custom na Cloud Backup pricing override para sa: ${Object.keys(cloudBackupPlanOverrides).join(', ')}.`);
    }
    recomputeModuleSubscriptionPlans();
    if (Object.keys(moduleSubscriptionOverrides).length > 0) {
        console.log(`💳 Loaded custom Module Subscription pricing override for: ${Object.keys(moduleSubscriptionOverrides).join(', ')}.`);
    }
    recomputeUpgradeTierPricing();
    if (Object.keys(upgradeTierPricingOverrides).length > 0) {
        console.log(`💳 Na-load ang custom na Upgrade Tier bundle pricing override para sa: ${Object.keys(upgradeTierPricingOverrides).join(', ')}.`);
    }
    console.log(
        pgPoolDevices
            ? `✅ Na-load mula sa Postgres (Neon): ${allowedDevices.size} allowed device(s), ${Object.keys(issuedUnlocks).length} device(s) may naka-unlock.`
            : `ℹ️  Na-load mula sa lokal na JSON files: ${allowedDevices.size} allowed device(s), ${Object.keys(issuedUnlocks).length} device(s) may naka-unlock.`
    );
}
bootstrapStores()
    .then(() => {
        const server = app.listen(PORT, () => {
            console.log(`OmniPOS Unlock Relay running sa port ${PORT}`);
        });
        server.requestTimeout = 60 * 60 * 1000;
    })
    .catch((err) => {
        console.error('❌ Hindi ma-bootstrap ang persistent storage — hindi tumakbo ang server:', err);
        process.exit(1);
    });
