
// AYOS/BAGO: itinaas ang laki ng libuv thread pool (default ay 4 lang)
// BAGO pa man mag-require ng anumang module. Ito ang thread pool na
// ginagamit ng async zlib.gzip/gunzip (tingnan sa ibaba) — kung
// sabay-sabay na nag-sync/restore ang maraming stores nang malalaking
// backup, posibleng mapuno ang 4 default threads at maghintayan na lang
// ang mga sumunod na gzip/gunzip job (hindi na naman babalik sa
// pag-block ng buong event loop — ang epekto lang ay konting pila sa
// pagitan ng ibang gzip/gunzip operations, hindi sa LAHAT ng requests).
// Dapat itakda ito nang mas maaga bago pa gumamit ang proseso ng thread
// pool (kaya nasa pinaka-unang linya ito) — walang epekto kung
// naka-set na ito sa environment variables mismo (Render dashboard,
// atbp.), doon pa rin susunod ang proseso.
if (!process.env.UV_THREADPOOL_SIZE) process.env.UV_THREADPOOL_SIZE = '8';
// FIX: RELAY never actually loaded .env into process.env (walang
// dotenv, walang custom loader tulad ng OMNIPOS/env-loader.js) — kaya
// ang CF_ACCOUNT_ID/CF_AI_API_TOKEN na nakalagay sa RELAY/.env ay
// hindi talaga nagagamit maliban na lang kung manual mong ini-export
// sa mismong shell bago mo pinatakbo ang `node server.js`. Ito ang
// dahilan kung bakit "not configured" pa rin ang vision AI kahit
// naka-set na ang .env at naka-agree na sa Cloudflare model terms.
(function loadDotEnvFile() {
    const fs = require('fs');
    const path = require('path');
    const envPath = path.join(__dirname, '.env');
    if (!fs.existsSync(envPath)) return;
    const lines = fs.readFileSync(envPath, 'utf8').split('\n');
    for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) continue;
        const idx = trimmed.indexOf('=');
        if (idx === -1) continue;
        const key = trimmed.slice(0, idx).trim();
        let val = trimmed.slice(idx + 1).trim();
        if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
            val = val.slice(1, -1);
        }
        if (!(key in process.env)) process.env[key] = val;
    }
})();
const express = require('express');
const crypto = require('crypto');
const zlib = require('zlib');
const { promisify } = require('util');
// AYOS/BAGO: async (thread-pool) na bersyon ng gzip/gunzip sa halip na
// ang *Sync variants. Ang zlib.gzipSync/gunzipSync ay tumatakbo sa main
// thread mismo — kaya habang malaki ang binu-buo/dini-decompress na
// payload (hal. isang buong store backup na may maraming records),
// naka-block ang buong Node.js event loop ng RELAY sa loob ng ilang
// millisecond hanggang segundo, na maaaring magpaantala ng LAHAT ng
// ibang kasabay na request (kasama ang para sa ibang stores). Ang
// promisify(zlib.gzip/gunzip) naman ay gumagamit ng libuv thread pool,
// kaya hindi na-b-block ang main thread habang nagpo-proseso. Walang
// binago sa format/logic — parehong gzip output/input pa rin.
const gzipAsync = promisify(zlib.gzip);
const gunzipAsync = promisify(zlib.gunzip);
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
// ===================================================================
// BUILD & PUSH DATABASE (hiwalay na Neon project/database) — dito
// lang naka-save ang download codes, build history, targeted releases,
// system version info, at release integrity/baseline data. SADYANG
// HIWALAY ito sa DATABASE_URL (Cloud Backup) at RELAY_DEVICES_DATABASE_URL
// (allowed devices/license) — kahit magkasabay silang ma-configure sa
// parehong Neon account, magkaiba dapat ang project/database para
// walang paghahalo ng data. Kung walang RELAY_BUILD_DATABASE_URL na
// naka-set, HINDI ito babalik sa pgPool/pgPoolDevices (iyon mismo ang
// gustong iwasan) — babalik na lang sa lokal na file (na alam nating
// ephemeral sa Render, pero mas mabuti pa rin kesa ihalo sa ibang DB).
const BUILD_DATABASE_URL = process.env.RELAY_BUILD_DATABASE_URL || null;
const pgPoolBuild = makePgPool(BUILD_DATABASE_URL);
if (pgPoolBuild) {
    pgPoolBuild.on('error', (err) => {
        console.error('⚠️  Postgres pool error (build/push storage):', err.message);
    });
    console.log('✅ Hiwalay na Neon database ang ginagamit para sa Build/Push data (RELAY_BUILD_DATABASE_URL) — hiwalay ito sa Cloud Backup at Devices/License.');
} else {
    console.warn('⚠️  Walang RELAY_BUILD_DATABASE_URL na naka-set — babalik sa lokal na file (ephemeral sa Render) ang download codes/build history/atbp., SADYANG hindi ito ibinabalik sa Cloud Backup o Devices database. Gumawa ng bagong Neon database at itakda ang connection string dito para persistent.');
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
    // AYOS/BAGO: tracking-only columns (walang kinalaman sa charging) — para
    // may visibility bago magdesisyon kung mag-charge/hihigpitan pa. Kasama
    // sa Client Cost Allocation report para makita agad kung sinong client
    // ang sobrang dalas mag-restore (posibleng abuser).
    await pgPool.query(`ALTER TABLE cloud_backup_meta ADD COLUMN IF NOT EXISTS restore_count INTEGER NOT NULL DEFAULT 0;`);
    await pgPool.query(`ALTER TABLE cloud_backup_meta ADD COLUMN IF NOT EXISTS last_restore_at TIMESTAMPTZ;`);
    // ===================================================================
    // AYOS/BAGO: mga column para sa paparating na "storage holding fee"
    // scheduled job (#2 sa safety-margin discussion) — hiwalay na bayad
    // para sa datos na TULOY-TULOY na nakaupo sa Neon storage, HINDI na
    // naka-depende sa bilang ng syncs (kung 0 syncs ang customer sa isang
    // buwan, 0 din dapat ang revenue base sa syncs, pero patuloy pa ring
    // binabayaran ang Neon storage — kaya kailangan ng SARILING billing
    // cycle ang storage fee, hiwalay sa per-sync charge). Ang mga column na
    // ito ay TRACKING/ACCRUAL LANG dito sa migration na ito — ang aktwal na
    // cron/scheduled job na gagamit sa mga ito ay hiwalay pang gagawin:
    //   - storage_fee_last_billed_at: huling sandali kung kailan na-charge
    //     (o unang na-set, para sa bagong installation) ang storage holding
    //     fee — ito ang pinagbabatayan ng "elapsed time" sa susunod na run
    //     ng scheduled job (elapsed = now() - storage_fee_last_billed_at).
    //   - storage_fee_fraction_accrued: fractional (hindi pa buong token)
    //     na naipon na storage fee — parehong pattern ng
    //     sync_fraction_accrued sa cloud_token_wallets sa ibaba, para hindi
    //     kailanman ma-undercharge dahil sa paulit-ulit na rounding.
    // ===================================================================
    await pgPool.query(`ALTER TABLE cloud_backup_meta ADD COLUMN IF NOT EXISTS storage_fee_last_billed_at TIMESTAMPTZ;`);
    await pgPool.query(`ALTER TABLE cloud_backup_meta ADD COLUMN IF NOT EXISTS storage_fee_fraction_accrued NUMERIC NOT NULL DEFAULT 0;`);
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
    await pgPoolDevices.query(`
        CREATE TABLE IF NOT EXISTS relay_ai_credit_usage (
            installation_id TEXT NOT NULL,
            month_key       TEXT NOT NULL,
            used_credits    INTEGER NOT NULL DEFAULT 0 CHECK (used_credits >= 0),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (installation_id, month_key)
        );
    `);
    await pgPoolDevices.query(`
        CREATE TABLE IF NOT EXISTS relay_ai_credit_requests (
            installation_id TEXT NOT NULL,
            month_key       TEXT NOT NULL,
            request_id      TEXT NOT NULL,
            credit_cost     INTEGER NOT NULL CHECK (credit_cost > 0),
            status          TEXT NOT NULL DEFAULT 'reserved',
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (installation_id, month_key, request_id)
        );
    `);
    await pgPoolDevices.query(`
        CREATE TABLE IF NOT EXISTS relay_ai_credit_settings (
            installation_id TEXT PRIMARY KEY,
            monthly_credits INTEGER NOT NULL CHECK (monthly_credits >= 1),
            text_cost       INTEGER NOT NULL CHECK (text_cost >= 1),
            file_cost       INTEGER NOT NULL CHECK (file_cost >= 1),
            image_cost      INTEGER NOT NULL CHECK (image_cost >= 1),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    `);
    await pgPoolDevices.query(`
        CREATE INDEX IF NOT EXISTS idx_relay_ai_credit_usage_month
        ON relay_ai_credit_usage (month_key);
    `);
    const defaultMonthly = Math.max(1, parseInt(process.env.RELAY_AI_MONTHLY_CREDITS, 10) || 300);
    const defaultText = Math.max(1, parseInt(process.env.RELAY_AI_TEXT_CREDIT_COST, 10) || 1);
    const defaultFile = Math.max(defaultText, parseInt(process.env.RELAY_AI_FILE_CREDIT_COST, 10) || 2);
    const defaultImage = Math.max(defaultFile, parseInt(process.env.RELAY_AI_IMAGE_CREDIT_COST, 10) || 3);
    await pgPoolDevices.query(
        `INSERT INTO relay_ai_credit_settings (installation_id, monthly_credits, text_cost, file_cost, image_cost)
         VALUES ('__default__', $1, $2, $3, $4)
         ON CONFLICT (installation_id) DO NOTHING`,
        [defaultMonthly, defaultText, defaultFile, defaultImage]
    );
    console.log('✅ AI credit schema ready (usage + requests + editable settings) — RELAY ang authoritative source.');
}
// ===================================================================
// GENERIC PERSISTENT KEY-VALUE STORE (Neon Postgres) — dito na-save
// ang LAHAT ng admin settings/lists na dating Redis-or-local-file lang
// (download codes, pricing overrides, device unlocks, activity log,
// atbp. — bawat load*()/save*() function pair sa buong file na
// dumadaan sa getPersistentJSON/setPersistentJSON sa ibaba). Ginamit ang
// parehong Neon Postgres na ginagamit na rin ng Cloud Backup/Devices
// data — walang extra service (Redis/Upstash) na kailangan pang i-set
// up, at HINDI ito mawawala kapag nag-restart/natulog ang Render web
// service (hindi tulad ng lokal na file, na napapawi sa ephemeral
// filesystem — tingnan ang paalala sa REDIS_URL warning sa itaas).
// Redis pa rin ang unang susubukan KUNG naka-configure ito (para hindi
// biglang mawala ang datos ng mga umiiral nang gumagamit ng Redis),
// pero Postgres na ang PANGUNAHING target ng LAHAT ng bagong save mula
// ngayon — tingnan ang getPersistentJSON/setPersistentJSON sa ibaba.
async function ensureKvSchema() {
    const pool = pgPoolDevices || pgPool;
    if (!pool) return;
    await pool.query(`
        CREATE TABLE IF NOT EXISTS relay_kv_store (
            key        TEXT PRIMARY KEY,
            value      JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    `);
    console.log('✅ Persistent settings Postgres schema ready (relay_kv_store) — dito na-save ang download codes at lahat ng admin settings, hindi na sa ephemeral file.');
}
// Hiwalay na schema/table sa HIWALAY na database (pgPoolBuild) — tingnan
// ang paalala sa itaas kung bakit sinadyang hiwalay ito sa relay_kv_store.
async function ensureBuildKvSchema() {
    if (!pgPoolBuild) return;
    await pgPoolBuild.query(`
        CREATE TABLE IF NOT EXISTS relay_build_kv_store (
            key        TEXT PRIMARY KEY,
            value      JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    `);
    // AYOS: dating ang release zip mismo (RELEASE_PACKAGE_PATH) ay lokal na file
    // LANG sa ephemeral disk ng Render — nawawala ito kapag nag-restart/na-redeploy
    // ang service, kahit na-publish na ang bersyon (ang metadata lang gaya ng
    // download codes/build history ang naka-Neon dati, hindi ang binary zip mismo).
    // Kaya "Walang naka-publish na release package sa server pa" pa rin kahit
    // may na-generate nang download code. Dito na rin ito naka-save (bytea) sa
    // parehong HIWALAY na Build database, para kahit mabura ang lokal na disk,
    // mai-restore pa rin ang aktwal na zip mula sa Neon.
    await pgPoolBuild.query(`
        CREATE TABLE IF NOT EXISTS relay_build_package_blob (
            id         TEXT PRIMARY KEY,
            file_name  TEXT NOT NULL,
            data       BYTEA NOT NULL,
            size_bytes BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    `);
    console.log('✅ Build/Push Postgres schema ready (relay_build_kv_store + relay_build_package_blob) sa HIWALAY na database mula sa allowed devices.');
}
const RELEASE_PACKAGE_BLOB_ID = 'omnipos-client';
// Marker ng KASALUKUYANG laman ng RELEASE_PACKAGE_PATH sa DISK NG INSTANCE NA
// ITO — ginagamit para malaman kung "stale" na ba ito kumpara sa pinakabagong
// naka-save sa Neon build DB (hal. binuo ng ANOTHER Render instance kung
// naka-multiple instances/autoscale). Kung existence-check lang ang gagamitin,
// posibleng manatiling naka-serve ng LUMANG zip ang isang instance kahit na
// meron nang mas bago sa Neon (dahil "meron na naman" ang lokal na file nito).
let releasePackageDiskMeta = { sizeBytes: null, updatedAtMs: null };
// I-save ang aktwal na release zip (binary) papuntang Neon build DB para
// hindi na umasa lang sa lokal/ephemeral disk. Tinatawag ito pagkatapos
// magtagumpay ang build (pagkatapos ng fs.renameSync papuntang RELEASE_PACKAGE_PATH).
async function saveReleasePackageToBuildDb(filePath) {
    if (!pgPoolBuild) return false;
    try {
        const buf = fs.readFileSync(filePath);
        const result = await queryWithRetry(pgPoolBuild, `
            INSERT INTO relay_build_package_blob (id, file_name, data, size_bytes, updated_at)
            VALUES ($1, $2, $3, $4, now())
            ON CONFLICT (id) DO UPDATE SET file_name = EXCLUDED.file_name, data = EXCLUDED.data,
                size_bytes = EXCLUDED.size_bytes, updated_at = now()
            RETURNING size_bytes, updated_at
        `, [RELEASE_PACKAGE_BLOB_ID, 'omnipos-client.zip', buf, buf.length]);
        const row = result.rows[0];
        releasePackageDiskMeta = { sizeBytes: Number(row.size_bytes), updatedAtMs: new Date(row.updated_at).getTime() };
        console.log(`✅ Na-save ang release package (${buf.length} bytes) sa Neon build DB — hindi na ito mawawala kahit ma-restart/ma-redeploy ang server.`);
        return true;
    } catch (err) {
        console.error('⚠️  Hindi ma-save ang release package sa Neon build DB (bytea):', err.message);
        return false;
    }
}
// Tinitiyak na "fresh" (pinakabagong bersyon) ang laman ng RELEASE_PACKAGE_PATH
// sa DISK NG INSTANCE NA ITO kumpara sa naka-save sa Neon build DB. Isang
// mabilisang metadata-only query lang ang ginagawa (walang binary transfer)
// maliban kung talagang naiiba ang laki/petsa — doon lang hihilahin ang buong
// blob. Ginagamit ito sa halip na basta existence-check lang, dahil kung
// naka-multiple Render instances (autoscale/zero-downtime deploy), posibleng
// may ibang instance na nakapag-build ng mas bagong version at ang instance na
// ito ay mananatiling naka-serve ng LUMANG zip kung existence-check lang.
async function ensureReleasePackageFreshOnDisk() {
    if (!pgPoolBuild) {
        // Walang Neon build DB na naka-configure — wala tayong paraan para
        // malaman kung stale ang lokal na file, existence na lang ang masasabi.
        return fs.existsSync(RELEASE_PACKAGE_PATH);
    }
    try {
        const result = await queryWithRetry(pgPoolBuild, 'SELECT size_bytes, updated_at FROM relay_build_package_blob WHERE id = $1', [RELEASE_PACKAGE_BLOB_ID]);
        if (result.rows.length === 0) {
            // Wala pang na-publish kahit sa Neon — gamitin na lang kung
            // meron man dating naiwan sa lokal na disk (hal. bago pa ang
            // migration na ito).
            return fs.existsSync(RELEASE_PACKAGE_PATH);
        }
        const dbSize = Number(result.rows[0].size_bytes);
        const dbUpdatedAtMs = new Date(result.rows[0].updated_at).getTime();
        const localMatches = fs.existsSync(RELEASE_PACKAGE_PATH)
            && releasePackageDiskMeta.sizeBytes === dbSize
            && releasePackageDiskMeta.updatedAtMs === dbUpdatedAtMs;
        if (localMatches) return true;
        // Naiiba (o wala pa sa disk) — hilahin ang buong blob mula Neon.
        const blobResult = await queryWithRetry(pgPoolBuild, 'SELECT data FROM relay_build_package_blob WHERE id = $1', [RELEASE_PACKAGE_BLOB_ID]);
        if (blobResult.rows.length === 0) return fs.existsSync(RELEASE_PACKAGE_PATH);
        const releaseDir = path.dirname(RELEASE_PACKAGE_PATH);
        if (!fs.existsSync(releaseDir)) fs.mkdirSync(releaseDir, { recursive: true });
        // Isulat muna sa ibang temp filename sa PAREHONG folder tapos i-rename
        // (atomic sa parehong filesystem) — para kung may kasabay na
        // res.download() na kasalukuyang nagba-basa/nagsa-stream ng
        // RELEASE_PACKAGE_PATH, hindi ito maabutan ng bahagyang-nasulat pa
        // lang (partial write) na file.
        const tmpRefreshPath = `${RELEASE_PACKAGE_PATH}.refresh-${process.pid}-${Date.now()}.tmp`;
        fs.writeFileSync(tmpRefreshPath, blobResult.rows[0].data);
        fs.renameSync(tmpRefreshPath, RELEASE_PACKAGE_PATH);
        releasePackageDiskMeta = { sizeBytes: dbSize, updatedAtMs: dbUpdatedAtMs };
        console.log('✅ Na-refresh ang release package sa lokal na disk ng instance na ito mula sa Neon build DB (bagong bersyon o unang restore).');
        return true;
    } catch (err) {
        console.error('⚠️  Hindi ma-verify/ma-refresh ang release package mula sa Neon build DB, babalik sa existence-check na lang:', err.message);
        return fs.existsSync(RELEASE_PACKAGE_PATH);
    }
}
// Parehong pattern ng getPersistentJSON/setPersistentJSON sa itaas, pero NAKATUON
// lang sa pgPoolBuild (hiwalay na Neon database) — walang Redis fallback
// dito dahil sadyang isolated na dapat itong storage, at walang
// pag-fallback sa pgPool/pgPoolDevices (iyon mismo ang iniiwasan).
async function buildKvGetJSON(key, fallback) {
    if (pgPoolBuild) {
        try {
            const result = await queryWithRetry(pgPoolBuild, 'SELECT value FROM relay_build_kv_store WHERE key = $1', [key]);
            if (result.rows.length > 0) return result.rows[0].value; 
        } catch (err) {
            console.error(`⚠️  Hindi mabasa sa Build Postgres store ang key "${key}":`, err.message);
        }
        return fallback;
    }
    return fallback;
}
// AYOS: dating "fire-and-forget" ito (walang return, walang paraan para
// malaman ng caller kung nagtagumpay o hindi ang pag-save) — parehong uri ng
// gap na nagdulot ng orihinal na "release package" bug. Ngayon, ibinabalik
// na nito ang Promise<boolean> para magamit ng mga tumatawag (lalo na sa
// build/publish/download-code endpoints) na i-await ito at ipaalam sa admin
// kung mabigo ang pag-save sa Neon, sa halip na tahimik lang na-log.
function buildKvSetJSON(key, value) {
    if (!pgPoolBuild) return Promise.resolve(false);
    return pgPoolBuild.query(
        `INSERT INTO relay_build_kv_store (key, value, updated_at) VALUES ($1, $2::jsonb, now())
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
        [key, JSON.stringify(value)]
    ).then(() => true).catch((err) => {
        console.error(`⚠️  Hindi ma-save sa Build Postgres store ang key "${key}":`, err.message);
        return false;
    });
}
async function getPersistentJSON(key, fallback) {
    // 1) Neon Postgres — pangunahing storage ngayon, laging persistent.
    const pool = pgPoolDevices || pgPool;
    if (pool) {
        try {
            const result = await queryWithRetry(pool, 'SELECT value FROM relay_kv_store WHERE key = $1', [REDIS_KEY_PREFIX + key]);
            if (result.rows.length > 0) return result.rows[0].value; 
        } catch (err) {
            console.error(`⚠️  Hindi mabasa sa Postgres KV store ang key "${key}":`, err.message);
        }
    }
    // 2) Redis — fallback lang ngayon (kung meron pang lumang datos dito
    // mula bago ang Postgres migration na ito). Kapag nakita dito,
    // isinusulat din agad papuntang Postgres para sa susunod na basa.
    if (redisClient) {
        try {
            const raw = await redisClient.get(REDIS_KEY_PREFIX + key);
            if (raw !== null) {
                const parsed = JSON.parse(raw);
                if (pool) setPersistentJSON(key, parsed); 
                return parsed;
            }
        } catch (err) {
            console.error(`⚠️  Hindi mabasa sa Redis ang key "${key}":`, err.message);
        }
    }
    // 3) Wala talaga — babalik sa fallback (kadalasan ay lokal na file,
    // hawak-hawak na ito ng bawat load*() function sa ibaba).
    return fallback;
}
function setPersistentJSON(key, value) {
    const pool = pgPoolDevices || pgPool;
    if (pool) {
        pool.query(
            `INSERT INTO relay_kv_store (key, value, updated_at) VALUES ($1, $2::jsonb, now())
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
            [REDIS_KEY_PREFIX + key, JSON.stringify(value)]
        ).catch((err) => {
            console.error(`⚠️  Hindi ma-save sa Postgres KV store ang key "${key}":`, err.message);
        });
    }
    if (redisClient) {
        redisClient.set(REDIS_KEY_PREFIX + key, JSON.stringify(value)).catch((err) => {
            console.error(`⚠️  Hindi ma-save sa Redis ang key "${key}":`, err.message);
        });
    }
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
// AYOS: kailangan ng PayMongo webhook ang RAW (unparsed) request body
// para ma-verify ang "Paymongo-Signature" header (HMAC laban sa eksaktong
// bytes na natanggap) — kaya excluded din ito dito, kagaya ng cloud
// backup chunk upload sa itaas. Ang mismong express.raw() para dito ay
// nasa route definition mismo ng /relay/webhooks/paymongo sa ibaba.
const PAYMONGO_WEBHOOK_PATH = '/relay/webhooks/paymongo';
// AYOS: kailangan din ng Stripe webhook ang RAW (unparsed) request body
// para ma-verify ang "Stripe-Signature" header, kagaya mismo ng PayMongo
// sa itaas — kaya excluded din ito dito. (Xendit at PayPal webhooks ay
// hindi nangangailangan ng raw bytes para sa kanilang verification, kaya
// default JSON parser lang ang gamit doon.)
const STRIPE_WEBHOOK_PATH = '/relay/webhooks/stripe';
// Dragonpay Postback ay pina-POST bilang normal na
// application/x-www-form-urlencoded (HINDI JSON, at hindi rin
// nangangailangan ng raw bytes tulad ng PayMongo/Stripe sa itaas —
// SHA1 lang ang laban sa mismong POSTED FIELDS, hindi sa raw body) —
// kaya excluded din ito dito, at gagamit ng express.urlencoded() sa
// route definition mismo ng /relay/webhooks/dragonpay sa ibaba.
const DRAGONPAY_WEBHOOK_PATH = '/relay/webhooks/dragonpay';
// BAGO: ang AI Assistant proxy endpoint ay puwedeng magdala ng isang
// base64-encoded screenshot sa loob mismo ng JSON body (hanggang ~6MB,
// tugma sa limitasyon na sinusunod na rin ng OMNIPOS client bago pa
// ito ipadala) — masyadong maliit para dito ang default 2mb JSON
// limit sa ibaba, kaya may sarili itong mas malaking parser.
const AI_ASSISTANT_PROXY_PATH = '/relay/ai-assistant/complete';
const defaultJsonParser = express.json({ limit: '2mb' });
const aiAssistantJsonParser = express.json({ limit: '8mb' });
const cloudBackupChunkRawParser = express.raw({ type: '*/*', limit: '6mb' });
app.use((req, res, next) => {
    if (req.path === CLOUD_BACKUP_UPLOAD_CHUNK_PATH) {
        return cloudBackupChunkRawParser(req, res, next);
    }
    if (req.path === AI_ASSISTANT_PROXY_PATH) {
        return aiAssistantJsonParser(req, res, next);
    }
    if (req.path === PAYMONGO_WEBHOOK_PATH || req.path === STRIPE_WEBHOOK_PATH || req.path === DRAGONPAY_WEBHOOK_PATH) {
        return next();
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
// BAGO: ang OmniPOS AI Assistant ay dating dumideretso sa Cloudflare
// Workers AI GAMIT ANG CREDENTIALS NA NAKA-EMBED SA BAWAT client build
// (encrypted man, kasama pa rin ang decryption key sa parehong package —
// kaya madaling ma-access ng end customer ang token). Dito na lang sila
// nakatira ngayon (RELAY, server ng developer lang) — tinatawag na lang
// ng bawat OMNIPOS client ang isang proxy endpoint dito (tingnan sa
// ibaba: /relay/ai-assistant/complete) sa halip na direktang tumawag sa
// Cloudflare gamit ang sariling naka-embed na token.
const CF_ACCOUNT_ID = process.env.CF_ACCOUNT_ID || null;
const CF_AI_API_TOKEN = process.env.CF_AI_API_TOKEN || null;
const CF_AI_MODEL = process.env.CF_AI_MODEL || '@cf/meta/llama-3.3-70b-instruct-fp8-fast';
const CF_AI_VISION_MODEL = process.env.CF_AI_VISION_MODEL || '@cf/meta/llama-3.2-11b-vision-instruct';
function isCfAiConfigured() {
    return !!(CF_ACCOUNT_ID && CF_AI_API_TOKEN);
}
async function callCloudflareWorkersAI(messages, vision) {
    if (vision) return callCloudflareVisionAI(messages);
    const model = CF_AI_MODEL;
    const url = `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/ai/v1/chat/completions`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 20000);
    try {
        const cfRes = await fetch(url, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${CF_AI_API_TOKEN}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ model, messages, max_tokens: 650, temperature: 0.3 }),
            signal: controller.signal
        });
        const raw = await cfRes.text();
        let data;
        try { data = JSON.parse(raw); } catch (e) { data = null; }
        if (!cfRes.ok || !data) {
            const errMsg = (data && data.errors && data.errors[0] && data.errors[0].message) || `Cloudflare AI request failed (HTTP ${cfRes.status}).`;
            return { success: false, message: errMsg };
        }
        const answer = data.choices && data.choices[0] && data.choices[0].message && data.choices[0].message.content;
        if (!answer || !answer.trim()) {
            return { success: false, message: 'Empty response from AI provider.' };
        }
        return { success: true, answer: answer.trim() };
    } catch (err) {
        return { success: false, message: err.name === 'AbortError' ? 'AI request timed out.' : (err.message || 'AI request failed.') };
    } finally {
        clearTimeout(timeout);
    }
}
// FIX: ang /ai/v1/chat/completions (OpenAI-compatible) endpoint ay may
// bug/limitation para sa @cf/meta/llama-3.2-11b-vision-instruct kapag
// naka-embed ang larawan sa loob ng "content" array (OpenAI multimodal
// style: [{type:'text',...},{type:'image_url',...}]) — sa panloob na
// conversion papunta sa sariling native format ni Cloudflare, minsan
// "nawawala" ang buong text message, kaya lumalabas ang:
//   "AiError: Unable to add image when there are no user-supplied nor
//    system-supplied messages."
// Ang ayos: gamitin ang NATIVE na /ai/run/{model} endpoint sa halip,
// kung saan HIWALAY na field ang larawan ("image": raw byte array —
// hindi base64 string/data URL) sa "messages" (plain text content
// lang, walang image_url sa loob).
async function callCloudflareVisionAI(messages) {
    const url = `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/ai/run/${CF_AI_VISION_MODEL}`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30000);
    try {
        // Hanapin ang message na may array content (kung saan naka-embed
        // ang image_url) — kunin ang larawan (data URL) at ang text
        // portion, at gawing plain-string content ang LAHAT ng messages
        // (kailangan ito ng native endpoint — hindi array).
        let imageDataUrl = null;
        const plainMessages = [];
        for (const m of messages) {
            if (Array.isArray(m.content)) {
                let textPart = '';
                for (const part of m.content) {
                    if (part && part.type === 'text') textPart += part.text || '';
                    else if (part && part.type === 'image_url' && part.image_url && part.image_url.url) {
                        imageDataUrl = part.image_url.url;
                    }
                }
                plainMessages.push({ role: m.role, content: textPart });
            } else {
                plainMessages.push({ role: m.role, content: m.content });
            }
        }
        if (!imageDataUrl) {
            return { success: false, message: 'No image found in vision request messages.' };
        }
        const commaIdx = imageDataUrl.indexOf(',');
        const base64Payload = commaIdx !== -1 ? imageDataUrl.slice(commaIdx + 1) : imageDataUrl;
        let imageBuf;
        try {
            imageBuf = Buffer.from(base64Payload, 'base64');
        } catch (e) {
            return { success: false, message: 'Could not decode the attached image.' };
        }
        const imageBytes = Array.from(imageBuf);
        const cfRes = await fetch(url, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${CF_AI_API_TOKEN}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ image: imageBytes, messages: plainMessages, max_tokens: 650 }),
            signal: controller.signal
        });
        const raw = await cfRes.text();
        let data;
        try { data = JSON.parse(raw); } catch (e) { data = null; }
        if (!cfRes.ok || !data) {
            const errMsg = (data && data.errors && data.errors[0] && data.errors[0].message) || `Cloudflare Vision AI request failed (HTTP ${cfRes.status}).`;
            return { success: false, message: errMsg };
        }
        if (data.success === false) {
            const errMsg = (data.errors && data.errors[0] && data.errors[0].message) || 'Cloudflare Vision AI request failed.';
            return { success: false, message: errMsg };
        }
        const answer = data.result && data.result.response;
        if (!answer || !answer.trim()) {
            return { success: false, message: 'Empty response from vision AI provider.' };
        }
        return { success: true, answer: answer.trim() };
    } catch (err) {
        return { success: false, message: err.name === 'AbortError' ? 'AI image analysis timed out.' : (err.message || 'AI image analysis failed.') };
    } finally {
        clearTimeout(timeout);
    }
}
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
    ai_assistant: { name: 'OmniPOS AI Assistant', price: null, category: 'module', isSubscription: true },
    cloud_backup: { name: 'Cloud Backup (Postgres)', price: null, category: 'module', isSubscription: true }
};
const CLOUD_BACKUP_PLANS_BASE = {
    // AYOS: idinagdag ang autoBackupIntervalMs bilang bahagi ng plan config
    // mismo dito sa RELAY (dating naka-hardcode lang sa OMNIPOS client),
    // para admin-editable na ito via /relay/admin/api/pricing/cloud-backup
    // (kasabay ng price/storageQuotaMB) at awtomatikong nasusundan ng
    // OMNIPOS sa susunod na pricing refresh nito (tingnan ang
    // applyCloudBackupPricingOverlay sa OMNIPOS server.js).
    basic: { id: 'basic', name: 'Cloud Backup — Basic', price: { monthly: 129, yearly: 1290 }, storageQuotaMB: 250, autoBackupIntervalMs: 24 * 60 * 60 * 1000 },
    standard: { id: 'standard', name: 'Cloud Backup — Standard', price: { monthly: 249, yearly: 2490 }, storageQuotaMB: 1024, autoBackupIntervalMs: 6 * 60 * 60 * 1000 },
    pro: { id: 'pro', name: 'Cloud Backup — Pro', price: { monthly: 399, yearly: 3990 }, storageQuotaMB: 5120, autoBackupIntervalMs: 60 * 60 * 1000 }
};
// Sanity bounds para sa autoBackupIntervalMs override — mas mababa pa sa
// heartbeat ng OMNIPOS client (15 min) ay walang epekto (mag-a-update pa
// rin ito kada heartbeat lang), at mas mataas sa 30 araw ay malamang mali
// nang pagkaka-type.
const CLOUD_BACKUP_MIN_AUTO_BACKUP_INTERVAL_MS = 15 * 60 * 1000;
const CLOUD_BACKUP_MAX_AUTO_BACKUP_INTERVAL_MS = 30 * 24 * 60 * 60 * 1000;
const CLOUD_BACKUP_BILLING_DAYS = { monthly: 30, yearly: 365 };
const CLOUD_BACKUP_PLAN_OVERRIDES_PATH = path.join(__dirname, 'cloud-backup-plan-overrides.json');
async function loadCloudBackupPlanOverrides() {
    const fromStore = await getPersistentJSON('cloud-backup-plan-overrides', null);
    if (fromStore !== null) return fromStore;
    try {
        return JSON.parse(fs.readFileSync(CLOUD_BACKUP_PLAN_OVERRIDES_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveCloudBackupPlanOverrides(obj) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('cloud-backup-plan-overrides', obj);
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
// ===================================================================
// CLOUD BACKUP TOKENS ("diamonds") — bagong currency na ginagamit ng
// isang OMNIPOS installation para panatilihing ACTIVE ang cloud backup
// sync nito (auto o manual). 1 token = ₱1. Binibili ito gamit ang
// GCash/Maya/Online Banking (PayMongo), tapos "ginagastos" (consumed)
// isang beses kada successful sync — iyon ang dahilan kung bakit may
// separate na "Auto-Sync" toggle sa OMNIPOS admin panel: kapag na-off
// ito, hindi na sumusunod ang auto-schedule (nakakatipid ng tokens),
// pero pwede pa ring mag-manual backup/restore basta may sapat na
// balance. Kapag naubos na ang tokens (mas mababa sa halagang
// kailangan kada sync ng kasalukuyang tier), awtomatikong nagiging
// "insufficient" ang wallet — dito hihinto ang auto-sync AT ang
// manual backup/restore buttons sa OMNIPOS hanggang sa bumili ulit ng
// tokens.
// ===================================================================
const CLOUD_TOKEN_PACKAGES = ['basic', 'standard', 'pro'];
// Ang bilang ng tokens na ibinibigay kada package ay eksaktong katumbas
// (1:1, ₱1 = 1 token) ng buwanang presyo ng kaukulang Cloud Backup tier
// sa CLOUD_BACKUP_PLANS sa itaas — kaya awtomatiko itong sumusunod
// kapag binago ang pricing dito (walang duplicate na numero).
function getCloudTokenPackages() {
    const packages = {};
    for (const tier of CLOUD_TOKEN_PACKAGES) {
        const plan = CLOUD_BACKUP_PLANS[tier];
        if (!plan) continue;
        const amountPHP = plan.price.monthly;
        // AYOS/BAGO: dating "Sapat na para sa ~1 buwan ng auto-sync" ang
        // tagline dito — MISLEADING ito dahil ang halagang ito (amountPHP)
        // ay EKSAKTONG katumbas lang ng buwanang MAINTENANCE/ACTIVATION FEE
        // ng tier (ang agad na babawasin sa /relay/cloud-tokens/activate-
        // cloud-backup pagka-subscribe/renew) — HINDI pa kasama ang hiwalay
        // na tokens na kakailanganin para sa aktwal na pag-auto-sync sa
        // buong buwan. At dahil sa disenyo ng cost-per-sync formula
        // (monthlyPrice / expectedSyncsPerMonth, tapos ibinubuod muli sa
        // buong buwan), ang TOTOONG kakailanganing tokens para lang sa
        // pag-sync sa normal na dalas ay humigit-kumulang KATUMBAS din
        // ng buwanang presyo. Kaya kung 129 lang ang binili at in-activate
        // agad ang Basic, maaagaw agad ng maintenance fee ang lahat — zero
        // na ang matitira para sa pag-sync, kahit sabi ng tagline "sapat
        // na". Ginawa nang tapat ang tagline dito + idinagdag ang
        // breakdown fields (maintenanceFeeTokens/estSyncTokensPerMonth/
        // estTotalMonthlyTokens) para magamit ito ng OMNIPOS UI bilang
        // malinaw na paliwanag bago bumili/mag-activate.
        const maintenanceFeeTokens = amountPHP;
        const estSyncTokensPerMonth = amountPHP; // by design, tumutugma sa monthly price kapag normal na dalas
        const estTotalMonthlyTokens = maintenanceFeeTokens + estSyncTokensPerMonth;
        const shortName = plan.name.replace('Cloud Backup — ', '');
        packages[tier] = {
            tier,
            name: plan.name,
            tokens: amountPHP,
            amountPHP,
            maintenanceFeeTokens,
            estSyncTokensPerMonth,
            estTotalMonthlyTokens,
            tagline: `Sakop lang nito ang buwanang maintenance fee ng ${shortName} — hiwalay pa ang tokens para sa aktwal na auto-sync. Tingnan sa ibaba ang buong breakdown.`
        };
    }
    // GAWA/BAGO: "Starter Bundle" — opsyonal na IISANG-bili na package kada
    // tier na sumasakop na sa PAREHONG (a) buwanang maintenance/activation
    // fee AT (b) tinatayang isang buong buwan ng auto-sync sa normal na
    // dalas — para sa mga customer na ayaw nang mag-isip ng breakdown at
    // gusto lang tapos na agad ang buong buwan sa isang bili. Hindi
    // pinapalitan o binabago ang halaga ng maintenance fee/allotment sa
    // itaas (mananatili ang mga iyon nang eksakto) — dagdag na CHOICE lang
    // ito sa tabi ng mga ito.
    for (const tier of CLOUD_TOKEN_PACKAGES) {
        const plan = CLOUD_BACKUP_PLANS[tier];
        if (!plan) continue;
        const monthlyPrice = plan.price.monthly;
        const bundleTokens = monthlyPrice * 2;
        const shortName = plan.name.replace('Cloud Backup — ', '');
        packages[`${tier}_bundle`] = {
            tier: `${tier}_bundle`,
            baseTier: tier,
            isBundle: true,
            name: `${shortName} — Starter Bundle`,
            tokens: bundleTokens,
            amountPHP: bundleTokens,
            maintenanceFeeTokens: monthlyPrice,
            estSyncTokensPerMonth: monthlyPrice,
            estTotalMonthlyTokens: bundleTokens,
            tagline: `All-in: kasama na ang maintenance fee + tinatayang 1 buwan ng auto-sync. Isang bili lang, sakop na ang buong buwan.`
        };
    }
    return packages;
}
// AYOS/BUGFIX: dating ang presyo (sa tokens) kada isang sync ay derived
// sa MAINTENANCE FEE ng tier (plan.price.monthly / inaasahang bilang ng
// auto-syncs kada buwan) — kaya kapag binago ang maintenance fee dito sa
// RELAY pricing admin, KASABAY na ring nagbabago ang "Est. sync cost"/
// "Cost per sync" kahit hindi naman talaga nagbago ang laki ng data ng
// customer. Ngayon, HIWALAY na ito sa maintenance fee: batay na lang sa
// AKTWAL na laki (bytes) ng datos na sini-sync at sa TOTOONG Neon storage
// rate (₱/GB/buwan, mula sa Neon plan na naka-configure para sa Cloud
// Backup — tingnan ang NEON_PRICING/neonConfiguredPlans sa itaas), pinag-
// prorate lang ayon sa dalas ng auto-sync ng tier (autoBackupIntervalMs,
// hal. isang upload bawat araw sa Basic) para makuha ang presyo kada
// ISANG sync. Ang tier ay ginagamit LANG dito para sa dalas ng pag-sync
// (hindi na para sa presyo) — kaya ang pagbabago ng maintenance fee sa
// pricing admin ay hindi na makakaapekto rito kailanman.
// ===================================================================
// AYOS/BAGO (Universal Safety Margin): idinagdag ang isang flat markup
// multiplier sa likod mismo ng bawat "real cost" formula (sync + restore),
// bago pa man ma-convert papuntang PHP/tokens. Layunin: kahit magkamali
// ang isa sa mga assumption sa itaas (maling naka-configure na tier
// dropdown, medyo mababa ang seed compute assumption, biglang bumagsak
// ang piso bago ma-refresh ang FX rate, ...), may 30% na cushion na
// bago pa man pumasok sa loss ang developer/negosyo — hindi na
// kailangang i-predict nang eksakto ang bawat variable, ang buffer na
// mismo ang proteksyon. Baguhin lang ang multiplier na ito kung
// kailangang i-adjust ang laki ng cushion (hal. 1.30 = +30%).
// ===================================================================
const CLOUD_BACKUP_COST_SAFETY_MARGIN_MULTIPLIER = 1.30;
function computeRealCloudBackupSyncCostPHP(sizeBytes, tier, usdToPhpRate) {
    const plan = CLOUD_BACKUP_PLANS[tier] || CLOUD_BACKUP_PLANS.basic;
    const sizeBytesSafe = Math.max(0, Number(sizeBytes) || 0);
    const sizeGB = sizeBytesSafe / (1024 * 1024 * 1024);
    const sizeMB = sizeBytesSafe / (1024 * 1024);
    const neonPlanId = neonConfiguredPlans.cloudBackup || 'free';
    let neonTier = NEON_PRICING[neonPlanId];
    // Ang Free tier ay walang bayad na storage/compute rate ($0) — hindi
    // ito makatotohanang batayan kung TALAGANG may bayad na Neon account
    // (mali lang ang naka-configure na dropdown dito sa admin panel).
    // AYOS/BAGO: dating "Launch" ang fallback dito — pinalitan papuntang
    // "Scale" (ang PINAKAMATAAS/pinaka-conservative na paid-tier rate),
    // dahil kung hindi tama o hindi malinaw ang dropdown, mas ligtas na
    // MALING paraan ang bahagyang mag-overcharge kaysa sa dating
    // undercharge (safe-by-default fallback).
    if (!neonTier || !neonTier.storageRatePerGBMonthUSD) neonTier = NEON_PRICING.scale;
    // STORAGE component
    const monthlyStorageCostUSD = sizeGB * neonTier.storageRatePerGBMonthUSD;
    const expectedSyncsPerMonth = Math.max(1, Math.round((30 * 24 * 60 * 60 * 1000) / plan.autoBackupIntervalMs));
    const perSyncStorageCostUSD = monthlyStorageCostUSD / expectedSyncsPerMonth;
    // AYOS/BAGO: COMPUTE component — bukod sa storage, may bayad din ang
    // Neon sa COMPUTE (CU-hours). Gamit ang admin-configurable na
    // "cloudBackupSyncCompute" assumption (tingnan ang NEON_PRICING_BASE
    // sa itaas para sa buong paliwanag), kinukwenta dito ang conservative
    // na estimate ng compute time na "nagagamit" ng ISANG sync — base
    // overhead (segundo) + karagdagang oras batay sa laki ng na-upload na
    // datos (mas malaki ang datos = mas matagal ang assumed na compute
    // time). Ang computeRatePerCUHourUSD ng Free tier ay $0 rin — parehong
    // fallback sa Scale rate (pinaka-mataas/pinaka-conservative) ang
    // ginagamit dito, kaayon ng safe-by-default fallback sa itaas.
    const computeRateUSD = (typeof neonTier.computeRatePerCUHourUSD === 'number' && neonTier.computeRatePerCUHourUSD > 0)
        ? neonTier.computeRatePerCUHourUSD
        : NEON_PRICING.scale.computeRatePerCUHourUSD;
    const computeAssumption = NEON_PRICING.cloudBackupSyncCompute || NEON_PRICING_BASE.cloudBackupSyncCompute;
    const assumedComputeSeconds = computeAssumption.assumedBaseSeconds + (sizeMB * computeAssumption.assumedSecondsPerMB);
    const assumedComputeCUHours = computeAssumption.assumedCU * (assumedComputeSeconds / 3600);
    const perSyncComputeCostUSD = assumedComputeCUHours * computeRateUSD;
    const perSyncCostUSD = perSyncStorageCostUSD + perSyncComputeCostUSD;
    const rate = usdToPhpRate || EXCHANGE_RATE_FALLBACK_USD_TO_PHP;
    // Universal Safety Margin — tingnan ang paliwanag sa itaas.
    return perSyncCostUSD * rate * CLOUD_BACKUP_COST_SAFETY_MARGIN_MULTIPLIER;
}
// "Ilustratibong" sample size lang (hal. sa packages catalog, kung saan
// wala pang partikular na installation/aktwal na laki ng datos na
// mapagbabatayan) — 5MB, katulad ng halimbawang ginamit sa paghingi ng
// bugfix na ito.
const CLOUD_BACKUP_SAMPLE_SIZE_BYTES_FOR_CATALOG = 5 * 1024 * 1024;
// EKSAKTONG (fractional, walang rounding) na presyo kada isang sync — ito
// na ang totoong batayan ng pag-charge. `sizeBytes` ang AKTWAL na laki ng
// datos na sini-sync (o pinakahuling kilalang laki, kung estimate/display
// lang ang layunin) — HINDI na ang buwanang presyo ng tier ang ginagamit.
async function getCloudTokenCostPerSyncExact(sizeBytes, tier) {
    const { rate } = await getUsdToPhpRate();
    return computeRealCloudBackupSyncCostPHP(sizeBytes, tier, rate);
}
// Presyo (sa tokens) kada ISANG successful sync, pinapalago pataas
// (Math.ceil) para hindi kailanman ma-undercharge, minimum 1 token kada
// sync. NOTE: ESTIMATE/DISPLAY LANG ito (ipinapakita sa OMNIPOS admin
// panel bago pa man mag-sync, at ginagamit bilang paunang "gate" kung
// sapat kaya ang balance) — HINDI na ito ang aktwal na ginagamit sa
// pag-charge. Ang totoong pag-charge (exact, hindi pataas ang rounding)
// ay nasa getCloudTokenCostPerSyncExact() sa itaas + consumeCloudTokensForSyncExact()
// sa ibaba.
async function getCloudTokenCostPerSync(sizeBytes, tier) {
    const exact = await getCloudTokenCostPerSyncExact(sizeBytes, tier);
    return Math.max(1, Math.ceil(exact));
}
// ===================================================================
// RESTORE COST MODEL — same PHP-from-real-Neon-cost approach as
// computeRealCloudBackupSyncCostPHP() above, but for a RESTORE instead of
// a sync. A restore is NOT prorated across "expected operations per
// month" the way a sync is (there is no sane "expected restores per
// month" — a legitimate restore is rare/unscheduled, disaster-recovery
// style, unlike auto-sync which runs on a fixed interval). Instead it is
// priced as ONE full read of the entire backup:
//   - STORAGE side: uses Neon's own "Instant Restore" rate
//     (instantRestoreRatePerGBMonthUSD, already defined above under
//     NEON_PRICING/NEON_PRICING_BASE) — this is the real Neon rate for
//     point-in-time/restore-style storage, more accurate here than
//     reusing the regular ongoing storageRatePerGBMonthUSD.
//   - COMPUTE side: reuses the SAME cloudBackupSyncCompute assumption as
//     sync — reading X MB back out of Postgres costs roughly the same
//     compute as writing X MB into it, so there's no need for a separate
//     self-calibrating assumption just for restores.
// The goal (per request): give restoring a small real cost to the
// customer, discourage repeated/abusive restores, and make sure the
// actual Neon egress/compute cost of a restore is covered rather than
// eaten by the developer.
// ===================================================================
function computeRealCloudBackupRestoreCostPHP(sizeBytes, tier, usdToPhpRate) {
    const sizeBytesSafe = Math.max(0, Number(sizeBytes) || 0);
    const sizeGB = sizeBytesSafe / (1024 * 1024 * 1024);
    const sizeMB = sizeBytesSafe / (1024 * 1024);
    const restoreStorageRateUSD = (typeof NEON_PRICING.instantRestoreRatePerGBMonthUSD === 'number')
        ? NEON_PRICING.instantRestoreRatePerGBMonthUSD
        : NEON_PRICING_BASE.instantRestoreRatePerGBMonthUSD;
    const restoreStorageCostUSD = sizeGB * restoreStorageRateUSD;
    const neonPlanId = neonConfiguredPlans.cloudBackup || 'free';
    let neonTier = NEON_PRICING[neonPlanId];
    // Same Free-tier fallback reasoning as computeRealCloudBackupSyncCostPHP()
    // above — $0 Free-tier rates aren't a realistic basis if there's
    // actually a paid Neon account behind this. AYOS/BAGO: safe-by-default
    // fallback papuntang "Scale" (pinakamataas na rate) sa halip na
    // "Launch" — pareho sa ginawang fix sa sync cost function sa itaas.
    if (!neonTier || !neonTier.storageRatePerGBMonthUSD) neonTier = NEON_PRICING.scale;
    const computeRateUSD = (typeof neonTier.computeRatePerCUHourUSD === 'number' && neonTier.computeRatePerCUHourUSD > 0)
        ? neonTier.computeRatePerCUHourUSD
        : NEON_PRICING.scale.computeRatePerCUHourUSD;
    const computeAssumption = NEON_PRICING.cloudBackupSyncCompute || NEON_PRICING_BASE.cloudBackupSyncCompute;
    const assumedComputeSeconds = computeAssumption.assumedBaseSeconds + (sizeMB * computeAssumption.assumedSecondsPerMB);
    const assumedComputeCUHours = computeAssumption.assumedCU * (assumedComputeSeconds / 3600);
    const restoreComputeCostUSD = assumedComputeCUHours * computeRateUSD;
    const perRestoreCostUSD = restoreStorageCostUSD + restoreComputeCostUSD;
    const rate = usdToPhpRate || EXCHANGE_RATE_FALLBACK_USD_TO_PHP;
    // Universal Safety Margin — tingnan ang paliwanag sa computeRealCloudBackupSyncCostPHP() sa itaas.
    return perRestoreCostUSD * rate * CLOUD_BACKUP_COST_SAFETY_MARGIN_MULTIPLIER;
}
// Exact (fractional, no rounding) price for one restore — this is the true
// basis used for charging. `sizeBytes` should be the installation's actual
// known backup size (cloud_backup_meta.size_bytes).
async function getCloudTokenCostPerRestoreExact(sizeBytes, tier) {
    const { rate } = await getUsdToPhpRate();
    return computeRealCloudBackupRestoreCostPHP(sizeBytes, tier, rate);
}
// Rounded-up (minimum 1 token) price for one restore — used for
// display/gating and for the actual whole-token deduction (unlike sync,
// restores are rare enough that we don't bother accruing sub-1-token
// fractions — every restore charges at least 1 whole token).
async function getCloudTokenCostPerRestore(sizeBytes, tier) {
    const exact = await getCloudTokenCostPerRestoreExact(sizeBytes, tier);
    return Math.max(1, Math.ceil(exact));
}
// AYOS/BUGFIX: atomic, EKSAKTONG pag-charge kada sync (pinapalitan ang
// dating "ipasa na lang mula OMNIPOS ang Math.ceil na tokens" na paraan).
// Sa bawat tawag: idinadagdag ang eksaktong (fractional) na presyo — batay
// na ngayon sa AKTWAL na laki ng datos na sini-sync (`sizeBytes`), HINDI
// na sa maintenance fee ng tier — sa naipong "sync_fraction_accrued" ng
// wallet, tapos isang WHOLE token lang ang aktwal na binabawas sa balance
// kapag umabot na sa 1.0 pataas ang bagong kabuuan — ang labi
// (fraction < 1.0) ay naka-imbak lang, hihintayin ng susunod na sync.
// AYOS/BUGFIX: dahil ang batayan ng presyo ngayon ay ang AKTWAL na laki ng
// datos (natural na nagbabago-bago nang kaunti sa bawat sync) at hindi na
// isang static na "presyo ng tier", tinanggal na ang dating "i-reset kapag
// nagbago ang tier" na lohika — hindi na ito naaangkop, dahil wala nang
// discrete na "presyo ng tier" na pwedeng magbago sa unang lugar. Ang
// natitirang fraction ay palaging pinagsasama, anuman ang tier.
async function consumeCloudTokensForSyncExact(installationId, sizeBytes, tier, note, trigger = 'manual') {
    const costFraction = await getCloudTokenCostPerSyncExact(sizeBytes, tier);
    return runPgWriteTx(pgPool, async (client) => {
        await client.query(
            `INSERT INTO cloud_token_wallets (installation_id, balance_tokens, auto_sync_enabled) VALUES ($1, 0, true)
             ON CONFLICT (installation_id) DO NOTHING`,
            [installationId]
        );
        const walletRes = await client.query(
            `SELECT balance_tokens, sync_fraction_accrued, sync_fraction_tier FROM cloud_token_wallets WHERE installation_id = $1 FOR UPDATE`,
            [installationId]
        );
        const row = walletRes.rows[0];
        const baseFraction = Number(row.sync_fraction_accrued) || 0;
        const newFraction = baseFraction + costFraction;
        const wholeTokens = Math.floor(newFraction);
        const remainder = newFraction - wholeTokens;
        const currentBalance = Number(row.balance_tokens);
        if (wholeTokens > 0 && currentBalance < wholeTokens) {
            // Sapat pa ang balance kaysa ma-charge, pero hindi pa kailanman
            // na-touch ang wallet — walang binabago, para pareho pa rin ang
            // susunod na pagsubok (walang na-lose na accrued fraction).
            return { ok: false, insufficient: true, balanceTokens: currentBalance };
        }
        const newBalance = currentBalance - wholeTokens;
        await client.query(
            `UPDATE cloud_token_wallets SET balance_tokens = $2, sync_fraction_accrued = $3, sync_fraction_tier = $4, updated_at = now() WHERE installation_id = $1`,
            [installationId, newBalance, remainder, tier]
        );
        if (wholeTokens > 0) {
            await client.query(
                `INSERT INTO cloud_token_ledger (installation_id, type, tokens, balance_after, note, category, trigger_type) VALUES ($1, 'consume', $2, $3, $4, 'SYNC_CHARGE', $5)`,
                [installationId, -wholeTokens, newBalance, note || null, trigger]
            );
        } else {
            // AYOS/BAGO: kahit walang WHOLE token na na-deduct (fraction pa
            // lang), itinatala pa rin ito sa lightweight na
            // cloud_sync_activity table — para makita ng customer sa
            // Transaction History ang BAWAT auto-sync, kahit ilang
            // sentimos-katumbas lang (hal. 0.010 token) ang presyo, sa
            // halip na maghintay munang umabot sa 1 buong token bago
            // may makikitang anumang entry.
            await client.query(
                `INSERT INTO cloud_sync_activity (installation_id, trigger_type, cost_tokens, accrued_after, size_bytes) VALUES ($1, $2, $3, $4, $5)`,
                [installationId, trigger, costFraction, remainder, Number(sizeBytes) || null]
            );
        }
        return { ok: true, balanceTokens: newBalance, tokensCharged: wholeTokens };
    });
}
// AYOS/BAGO: atomic charge for a RESTORE (see computeRealCloudBackupRestoreCostPHP()
// above for the pricing rationale). Simpler than consumeCloudTokensForSyncExact() —
// no fractional accrual bucket, since restores are rare (capped at 5/day)
// and always charge at least 1 whole token immediately. Fails closed
// (insufficient: true) without touching the balance if the wallet doesn't
// have enough.
async function consumeCloudTokensForRestore(installationId, sizeBytes, tier, note) {
    const costTokens = await getCloudTokenCostPerRestore(sizeBytes, tier);
    return runPgWriteTx(pgPool, async (client) => {
        await client.query(
            `INSERT INTO cloud_token_wallets (installation_id, balance_tokens, auto_sync_enabled) VALUES ($1, 0, true)
             ON CONFLICT (installation_id) DO NOTHING`,
            [installationId]
        );
        const walletRes = await client.query(
            `SELECT balance_tokens FROM cloud_token_wallets WHERE installation_id = $1 FOR UPDATE`,
            [installationId]
        );
        const currentBalance = Number(walletRes.rows[0].balance_tokens);
        if (currentBalance < costTokens) {
            return { ok: false, insufficient: true, balanceTokens: currentBalance, costTokens };
        }
        const newBalance = currentBalance - costTokens;
        await client.query(
            `UPDATE cloud_token_wallets SET balance_tokens = $2, updated_at = now() WHERE installation_id = $1`,
            [installationId, newBalance]
        );
        await client.query(
            `INSERT INTO cloud_token_ledger (installation_id, type, tokens, balance_after, note, category, trigger_type) VALUES ($1, 'consume', $2, $3, $4, 'RESTORE_CHARGE', 'manual')`,
            [installationId, -costTokens, newBalance, note || null]
        );
        return { ok: true, balanceTokens: newBalance, tokensCharged: costTokens };
    });
}
// ===================================================================
// PAYMENT PROVIDERS — dating PayMongo lang ang suportado dito. Ngayon,
// isang "registry" ng maraming posibleng online payment/banking
// provider (PayMongo, Xendit, Stripe, PayPal) — ANG BAWAT ISA AY
// AWTOMATIKONG "AVAILABLE" LANG kapag kumpleto ang env var(s) na
// kailangan nito (nakalagay sa Render environment). Kung wala/hindi
// nakalagay ang env var ng isang provider, hindi ito lalabas sa listahan
// ng GET /relay/cloud-tokens/packages — kaya ang OMNIPOS lang ay
// magpapakita/magpapapili ng mga paraan ng bayad na TALAGANG naka-configure
// dito sa relay. Walang kailangang baguhin sa OMNIPOS kapag nagdagdag o
// nag-alis ng provider/env var dito — sumusunod na lang ito.
//
// Env vars na kinikilala (lahat optional — piliin lang ang gusto mong
// i-enable, i-set sa Render > Environment):
//   PAYMONGO_ENV (test|live), PAYMONGO_TEST_SECRET_KEY / PAYMONGO_LIVE_SECRET_KEY,
//   PAYMONGO_TEST_WEBHOOK_SECRET / PAYMONGO_LIVE_WEBHOOK_SECRET
//     -> GCash/Maya/Online Banking (PayMongo) — HIWALAY na key/secret ang
//        test at live mode (ito mismo ang dahilan kung bakit MALI ang
//        ilagay ang dalawa sa IISANG PAYMONGO_SECRET_KEY, hal.
//        "sk_test_xxx/sk_live_xxx" — hindi ito kikilalanin ni PayMongo
//        bilang valid na key). Ang PAYMONGO_ENV ang siyang pumipili kung
//        alin sa dalawang pares ang GAGAMITIN sa ngayon.
//   XENDIT_SECRET_KEY,   XENDIT_WEBHOOK_TOKEN           -> GCash/Maya/GrabPay/Bank Transfer/Card (Xendit Invoice)
//   STRIPE_SECRET_KEY,   STRIPE_WEBHOOK_SECRET          -> Credit/Debit Card (Stripe Checkout)
//   PAYPAL_CLIENT_ID,    PAYPAL_CLIENT_SECRET, PAYPAL_WEBHOOK_ID, PAYPAL_ENV (sandbox|live) -> PayPal
//   DRAGONPAY_MERCHANT_ID, DRAGONPAY_SECRET_KEY, DRAGONPAY_ENV (test|live)
//     -> GCash / Bank Transfer via InstaPay & PESONet / Over-the-Counter
//        (7-Eleven, Cebuana Lhuillier, LBC, atbp.) sa pamamagitan ng
//        Dragonpay's hosted checkout (maraming channel sa iisang provider,
//        kagaya ng Xendit sa itaas).
//
// PAALALA tungkol sa "InstaPay" at "Pisonet":
//   - Ang InstaPay (at PESONet) ay hindi isang payment GATEWAY na
//     maaaring direktang i-integrate gamit ang sarili nitong API key —
//     ito ay isang real-time interbank transfer RAIL na pinapatakbo ng
//     BSP/PhilPaSS, at napapasukan lang sa pamamagitan ng bangko, e-money
//     issuer, o isang aggregator/PSP tulad ng Dragonpay o Xendit. Kaya
//     idinagdag dito ang Dragonpay (sa itaas) — awtomatiko nang kasama
//     doon ang InstaPay/PESONet bilang isa sa mga channel sa checkout
//     page nito, hindi na kailangan ng hiwalay na "InstaPay provider".
//   - Ang "Pisonet" naman ay hindi isang online payment provider — ito ay
//     tawag sa coin-operated na internet café kiosks/vending machines sa
//     Pilipinas, walang kinalaman sa pagtanggap ng online payment. Kung
//     ibang provider ang tinutukoy (hal. Bux.ph, Coins.ph, DirectPay,
//     Adyen), sabihin lang ang eksaktong pangalan para maidagdag nang
//     tama at ligtas (kailangan ng eksaktong opisyal na API docs nito
//     para hindi magkamali ang signature/digest verification).
// ===================================================================

// ---- PayMongo (GCash / Maya / Online Banking via Direct Online Banking source) ----
// AYOS: dating IISANG PAYMONGO_SECRET_KEY / PAYMONGO_WEBHOOK_SECRET lang
// (kung saan kadalasang MALING nilalagay ng dalawang key — test AT live —
// sa iisang env var, hal. "sk_test_xxx/sk_live_xxx", na nagreresulta sa
// "API key ... does not exist" error mula sa PayMongo). Ngayon, HIWALAY
// na env var ang test at live mode, at ang PAYMONGO_ENV (test|live, kagaya
// ng DRAGONPAY_ENV/PAYPAL_ENV sa itaas) ang pumipili kung alin ang
// gagamitin — hindi na kailangang mag-edit ng code, env var lang.
const PAYMONGO_ENV = process.env.PAYMONGO_ENV === 'live' ? 'live' : 'test';
const PAYMONGO_SECRET_KEY = (PAYMONGO_ENV === 'live' ? process.env.PAYMONGO_LIVE_SECRET_KEY : process.env.PAYMONGO_TEST_SECRET_KEY) || null;
const PAYMONGO_WEBHOOK_SECRET = (PAYMONGO_ENV === 'live' ? process.env.PAYMONGO_LIVE_WEBHOOK_SECRET : process.env.PAYMONGO_TEST_WEBHOOK_SECRET) || null;
const PAYMONGO_API_BASE = 'https://api.paymongo.com/v1';
function paymongoAuthHeader() {
    return 'Basic ' + Buffer.from(`${PAYMONGO_SECRET_KEY}:`).toString('base64');
}
const PAYMONGO_METHOD_TO_SOURCE_TYPE = { gcash: 'gcash', maya: 'paymaya', online_banking: 'dob' };
async function paymongoCreateSource({ amountPHP, method, redirectSuccessUrl, redirectFailedUrl, description }) {
    if (!PAYMONGO_SECRET_KEY) {
        const err = new Error(`PAYMONGO_${PAYMONGO_ENV.toUpperCase()}_SECRET_KEY is not configured on the relay yet (PAYMONGO_ENV=${PAYMONGO_ENV}) — cannot accept GCash/Maya/Online Banking payments right now.`);
        err.code = 'PAYMONGO_NOT_CONFIGURED';
        throw err;
    }
    const sourceType = PAYMONGO_METHOD_TO_SOURCE_TYPE[method];
    if (!sourceType) throw new Error(`Unsupported payment method: ${method}`);
    const resp = await fetch(`${PAYMONGO_API_BASE}/sources`, {
        method: 'POST',
        headers: { Authorization: paymongoAuthHeader(), 'Content-Type': 'application/json' },
        body: JSON.stringify({
            data: {
                attributes: {
                    amount: Math.round(amountPHP * 100),
                    currency: 'PHP',
                    type: sourceType,
                    redirect: { success: redirectSuccessUrl, failed: redirectFailedUrl },
                    description: description || 'OmniPOS Cloud Backup Tokens'
                }
            }
        })
    });
    const data = await resp.json().catch(() => null);
    if (!resp.ok || !data || !data.data) {
        const message = (data && data.errors && data.errors[0] && data.errors[0].detail) || `PayMongo error (HTTP ${resp.status})`;
        throw new Error(message);
    }
    return data.data; 
}
async function paymongoCreatePayment({ sourceId, amountPHP, description }) {
    const resp = await fetch(`${PAYMONGO_API_BASE}/payments`, {
        method: 'POST',
        headers: { Authorization: paymongoAuthHeader(), 'Content-Type': 'application/json' },
        body: JSON.stringify({
            data: {
                attributes: {
                    amount: Math.round(amountPHP * 100),
                    currency: 'PHP',
                    source: { id: sourceId, type: 'source' },
                    description: description || 'OmniPOS Cloud Backup Tokens'
                }
            }
        })
    });
    const data = await resp.json().catch(() => null);
    if (!resp.ok || !data || !data.data) {
        const message = (data && data.errors && data.errors[0] && data.errors[0].detail) || `PayMongo payment error (HTTP ${resp.status})`;
        throw new Error(message);
    }
    return data.data;
}
// ---- PayMongo QR Ph — HIWALAY na flow ito kumpara sa Sources API sa
// itaas (gcash/paymaya/dob), dahil ang QR Ph ay hindi "source" kundi isang
// Payment Intent + Payment Method (type: "qrph"). Walang redirect/
// checkout_url dito — sa halip, nagbabalik ito ng larawan ng QR code
// (next_action.code.image_url) na ipapakita sa OMNIPOS admin panel, at
// sino mang bank/e-wallet app na sumusuporta sa QR Ph (GCash, Maya,
// karamihan sa mga banking app) ay puwedeng mag-scan dito para magbayad.
// Parehong PAYMONGO_LIVE_SECRET_KEY/PAYMONGO_TEST_SECRET_KEY (depende sa
// PAYMONGO_ENV) at PAYMONGO_*_WEBHOOK_SECRET sa itaas ang ginagamit dito
// — walang bagong env var na kailangan idagdag para dito.
const PAYMONGO_QRPH_MIN_AMOUNT_PHP = 1; // BUGFIX: dating ₱100 ang nilagay dito (maling akala) — dokumentado ng PayMongo na ₱1.00 lang ang minimum para sa QR Ph, kaya tama ang mababang custom amount (hal. ₱50 na minimum ng OMNIPOS mismo).
async function paymongoCreateQrPhIntent({ amountPHP, description }) {
    if (!PAYMONGO_SECRET_KEY) {
        const err = new Error(`PAYMONGO_${PAYMONGO_ENV.toUpperCase()}_SECRET_KEY is not configured on the relay yet (PAYMONGO_ENV=${PAYMONGO_ENV}) — cannot accept QR Ph payments right now.`);
        err.code = 'PAYMONGO_NOT_CONFIGURED';
        throw err;
    }
    if (!(amountPHP >= PAYMONGO_QRPH_MIN_AMOUNT_PHP)) {
        throw new Error(`Minimum na halaga para sa QR Ph ay ₱${PAYMONGO_QRPH_MIN_AMOUNT_PHP}.`);
    }
    const amountCentavos = Math.round(amountPHP * 100);
    // 1) Gumawa ng Payment Intent na "qrph" lang ang allowed na paraan.
    const intentResp = await fetch(`${PAYMONGO_API_BASE}/payment_intents`, {
        method: 'POST',
        headers: { Authorization: paymongoAuthHeader(), 'Content-Type': 'application/json' },
        body: JSON.stringify({
            data: {
                attributes: {
                    amount: amountCentavos,
                    currency: 'PHP',
                    payment_method_allowed: ['qrph'],
                    capture_type: 'automatic',
                    description: description || 'OmniPOS Cloud Backup Tokens'
                }
            }
        })
    });
    const intentData = await intentResp.json().catch(() => null);
    if (!intentResp.ok || !intentData || !intentData.data) {
        const message = (intentData && intentData.errors && intentData.errors[0] && intentData.errors[0].detail) || `PayMongo error (HTTP ${intentResp.status})`;
        throw new Error(message);
    }
    const intent = intentData.data;
    const clientKey = intent.attributes.client_key;
    // 2) Gumawa ng Payment Method na type "qrph". BUGFIX: dating umaasa
    // ang code dito sa isang "next_action.code.expires_at" field mula sa
    // PayMongo response para malaman kung kailan mag-e-expire ang QR —
    // WALANG GANOONG FIELD sa opisyal na PayMongo docs (laging null ito
    // dati, kaya walang expiry na naipapakita). Sa halip, dito na natin
    // eksplisitong itinatakda ang expiry_seconds (60–9000 segundo ang
    // allowed range ng PayMongo, default ay 1800/30 minuto) — at doon na
    // lang natin kino-compute ang expiresAt sa server side mismo.
    const QRPH_EXPIRY_SECONDS = 900; // 15 minuto — angkop sa admin-panel na pagbili ng tokens
    const pmResp = await fetch(`${PAYMONGO_API_BASE}/payment_methods`, {
        method: 'POST',
        headers: { Authorization: paymongoAuthHeader(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ data: { attributes: { type: 'qrph', expiry_seconds: QRPH_EXPIRY_SECONDS } } })
    });
    const pmData = await pmResp.json().catch(() => null);
    if (!pmResp.ok || !pmData || !pmData.data) {
        const message = (pmData && pmData.errors && pmData.errors[0] && pmData.errors[0].detail) || `PayMongo error (HTTP ${pmResp.status})`;
        throw new Error(message);
    }
    const paymentMethodId = pmData.data.id;
    // 3) I-attach ang Payment Method sa Payment Intent — dito na dapat
    // dumating ang next_action.code.image_url (ang QR code na ipapakita).
    const attachResp = await fetch(`${PAYMONGO_API_BASE}/payment_intents/${intent.id}/attach`, {
        method: 'POST',
        headers: { Authorization: paymongoAuthHeader(), 'Content-Type': 'application/json' },
        body: JSON.stringify({
            data: {
                attributes: {
                    payment_method: paymentMethodId,
                    client_key: clientKey
                }
            }
        })
    });
    const attachData = await attachResp.json().catch(() => null);
    if (!attachResp.ok || !attachData || !attachData.data) {
        const message = (attachData && attachData.errors && attachData.errors[0] && attachData.errors[0].detail) || `PayMongo QR Ph error (HTTP ${attachResp.status})`;
        throw new Error(message);
    }
    const attached = attachData.data;
    const nextAction = attached.attributes.next_action;
    const qrImageUrl = nextAction && nextAction.code && nextAction.code.image_url;
    if (!qrImageUrl) {
        throw new Error(`Hindi nakabalik ng QR code image ang PayMongo (status: ${attached.attributes.status || 'unknown'}).`);
    }
    return {
        id: attached.id, // ito ang Payment Intent ID (pi_xxx) — ito ang itatago natin bilang source_id/providerRefId
        qrCodeImageUrl: qrImageUrl,
        // BUGFIX: server-side na compute base sa expiry_seconds na eksplisito
        // nating itinakda sa itaas (di na umaasa sa di-umiiral na field).
        expiresAt: new Date(Date.now() + QRPH_EXPIRY_SECONDS * 1000).toISOString(),
        status: attached.attributes.status
    };
}
// Ginagamit ito ng webhook sa ibaba para i-credit ang tokens kapag
// nag-"paid" na ang isang QR Ph Payment Intent — hiwalay na function dahil
// may ilang posibleng event type/shape na maaaring dumating mula kay
// PayMongo (payment.paid na may payment_intent_id, o payment_intent.succeeded
// na direktang ang Payment Intent mismo ang resource) pero pareho lang ang
// gagawin: hanapin ang pending purchase gamit ang Payment Intent ID
// (naka-imbak sa source_id column) at i-credit kapag matagumpay na na-claim.
async function creditPaidPaymentIntent(paymentIntentId, { failed = false } = {}) {
    if (!paymentIntentId) return;
    if (failed) {
        await queryWithRetry(pgPool, `UPDATE cloud_token_purchases SET status = 'failed', updated_at = now() WHERE source_id = $1 AND status = 'pending'`, [paymentIntentId]);
        return;
    }
    const purchaseResult = await queryWithRetry(
        pgPool,
        `SELECT * FROM cloud_token_purchases WHERE source_id = $1 AND status = 'pending'`,
        [paymentIntentId]
    );
    const purchase = purchaseResult.rows[0];
    if (!purchase) return;
    const claimResult = await queryWithRetry(pgPool, `UPDATE cloud_token_purchases SET status = 'paid', updated_at = now() WHERE purchase_id = $1 AND status = 'pending' RETURNING *`, [purchase.purchase_id]);
    if (!claimResult.rows[0]) return; // naunahan na ng ibang concurrent/duplicate webhook delivery
    const newBalance = await creditCloudTokens(purchase.installation_id, Number(purchase.tokens), `Purchase ${purchase.purchase_id} (${purchase.method})`);
    logActivity(purchase.installation_id, 'cloud_token_purchase_paid', { purchaseId: purchase.purchase_id, tokens: Number(purchase.tokens), newBalance });
    sendTelegramNotification(`💎 Omni Tokens bought: ${purchase.tokens} tokens (₱${purchase.amount_php}) via ${purchase.method} — installation ${purchase.installation_id.slice(0, 12)}... New balance: ${newBalance}`).catch(() => {});
}
// Kina-verify ang "Paymongo-Signature" header (t=<timestamp>,te=<hmac>,li=<hmac live>)
// laban sa raw request body, gamit ang PAYMONGO_WEBHOOK_SECRET — pinipigilan nito
// ang kahit sinong mag-fake ng "successful payment" papunta sa webhook endpoint.
function verifyPaymongoSignature(rawBody, signatureHeader) {
    if (!PAYMONGO_WEBHOOK_SECRET || !signatureHeader) return false;
    const parts = Object.fromEntries(
        String(signatureHeader).split(',').map((kv) => {
            const [k, v] = kv.split('=');
            return [k, v];
        })
    );
    const timestamp = parts.t;
    const expectedHmac = parts.te || parts.li;
    if (!timestamp || !expectedHmac) return false;
    const signedPayload = `${timestamp}.${rawBody}`;
    const computedHmac = crypto.createHmac('sha256', PAYMONGO_WEBHOOK_SECRET).update(signedPayload).digest('hex');
    return safeCompare(computedHmac, expectedHmac);
}

// ---- Xendit (Invoice API — iisang hosted checkout na sumusuporta na sa
// GCash/Maya/GrabPay/ShopeePay/Bank Transfer/Cards nang walang kailangan
// pang piliin ang eksaktong channel dito sa relay) ----
const XENDIT_SECRET_KEY = process.env.XENDIT_SECRET_KEY || null;
const XENDIT_WEBHOOK_TOKEN = process.env.XENDIT_WEBHOOK_TOKEN || null;
const XENDIT_API_BASE = 'https://api.xendit.co';
function xenditAuthHeader() {
    return 'Basic ' + Buffer.from(`${XENDIT_SECRET_KEY}:`).toString('base64');
}
async function xenditCreateInvoice({ amountPHP, purchaseId, redirectSuccessUrl, redirectFailedUrl, description }) {
    if (!XENDIT_SECRET_KEY) {
        const err = new Error('XENDIT_SECRET_KEY is not configured on the relay yet.');
        err.code = 'XENDIT_NOT_CONFIGURED';
        throw err;
    }
    const resp = await fetch(`${XENDIT_API_BASE}/v2/invoices`, {
        method: 'POST',
        headers: { Authorization: xenditAuthHeader(), 'Content-Type': 'application/json' },
        body: JSON.stringify({
            external_id: purchaseId,
            amount: amountPHP,
            currency: 'PHP',
            description: description || 'OmniPOS Cloud Backup Tokens',
            success_redirect_url: redirectSuccessUrl,
            failure_redirect_url: redirectFailedUrl
        })
    });
    const data = await resp.json().catch(() => null);
    if (!resp.ok || !data || !data.id) {
        throw new Error((data && data.message) || `Xendit error (HTTP ${resp.status})`);
    }
    return data; 
}
// Simpleng token-comparison lang ang webhook verification ng Xendit
// (header na "x-callback-token" laban sa XENDIT_WEBHOOK_TOKEN na naka-set
// sa Xendit dashboard) — hindi HMAC ng raw body kaya OK lang ang default
// JSON body parser dito.
function verifyXenditWebhookToken(headerToken) {
    if (!XENDIT_WEBHOOK_TOKEN || !headerToken) return false;
    return safeCompare(String(headerToken), XENDIT_WEBHOOK_TOKEN);
}

// ---- Stripe (Checkout Session — internasyonal na credit/debit card) ----
const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY || null;
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || null;
const STRIPE_API_BASE = 'https://api.stripe.com/v1';
async function stripeCreateCheckoutSession({ amountPHP, purchaseId, redirectSuccessUrl, redirectFailedUrl, description }) {
    if (!STRIPE_SECRET_KEY) {
        const err = new Error('STRIPE_SECRET_KEY is not configured on the relay yet.');
        err.code = 'STRIPE_NOT_CONFIGURED';
        throw err;
    }
    const body = new URLSearchParams();
    body.append('mode', 'payment');
    body.append('success_url', redirectSuccessUrl);
    body.append('cancel_url', redirectFailedUrl);
    body.append('client_reference_id', purchaseId);
    body.append('line_items[0][quantity]', '1');
    body.append('line_items[0][price_data][currency]', 'php');
    body.append('line_items[0][price_data][unit_amount]', String(Math.round(amountPHP * 100)));
    body.append('line_items[0][price_data][product_data][name]', description || 'OmniPOS Cloud Backup Tokens');
    const resp = await fetch(`${STRIPE_API_BASE}/checkout/sessions`, {
        method: 'POST',
        headers: { Authorization: 'Bearer ' + STRIPE_SECRET_KEY, 'Content-Type': 'application/x-www-form-urlencoded' },
        body
    });
    const data = await resp.json().catch(() => null);
    if (!resp.ok || !data || !data.id) {
        const message = (data && data.error && data.error.message) || `Stripe error (HTTP ${resp.status})`;
        throw new Error(message);
    }
    return data; 
}
// Kina-verify ang "Stripe-Signature" header (t=<timestamp>,v1=<hmac>) laban
// sa raw request body, gamit ang STRIPE_WEBHOOK_SECRET — kaparehong pattern
// ng PayMongo sa itaas.
function verifyStripeSignature(rawBody, signatureHeader) {
    if (!STRIPE_WEBHOOK_SECRET || !signatureHeader) return false;
    const parts = Object.fromEntries(
        String(signatureHeader).split(',').map((kv) => {
            const [k, v] = kv.split('=');
            return [k, v];
        })
    );
    const timestamp = parts.t;
    const expectedHmac = parts.v1;
    if (!timestamp || !expectedHmac) return false;
    const signedPayload = `${timestamp}.${rawBody}`;
    const computedHmac = crypto.createHmac('sha256', STRIPE_WEBHOOK_SECRET).update(signedPayload).digest('hex');
    return safeCompare(computedHmac, expectedHmac);
}

// ---- PayPal (Orders v2 API) ----
const PAYPAL_CLIENT_ID = process.env.PAYPAL_CLIENT_ID || null;
const PAYPAL_CLIENT_SECRET = process.env.PAYPAL_CLIENT_SECRET || null;
const PAYPAL_WEBHOOK_ID = process.env.PAYPAL_WEBHOOK_ID || null;
const PAYPAL_API_BASE = process.env.PAYPAL_ENV === 'live' ? 'https://api-m.paypal.com' : 'https://api-m.sandbox.paypal.com';
async function paypalAccessToken() {
    const resp = await fetch(`${PAYPAL_API_BASE}/v1/oauth2/token`, {
        method: 'POST',
        headers: {
            Authorization: 'Basic ' + Buffer.from(`${PAYPAL_CLIENT_ID}:${PAYPAL_CLIENT_SECRET}`).toString('base64'),
            'Content-Type': 'application/x-www-form-urlencoded'
        },
        body: 'grant_type=client_credentials'
    });
    const data = await resp.json().catch(() => null);
    if (!resp.ok || !data || !data.access_token) throw new Error('Could not authenticate with PayPal.');
    return data.access_token;
}
async function paypalCreateOrder({ amountPHP, purchaseId, redirectSuccessUrl, redirectFailedUrl, description }) {
    if (!PAYPAL_CLIENT_ID || !PAYPAL_CLIENT_SECRET) {
        const err = new Error('PAYPAL_CLIENT_ID / PAYPAL_CLIENT_SECRET is not configured on the relay yet.');
        err.code = 'PAYPAL_NOT_CONFIGURED';
        throw err;
    }
    const token = await paypalAccessToken();
    const resp = await fetch(`${PAYPAL_API_BASE}/v2/checkout/orders`, {
        method: 'POST',
        headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
        body: JSON.stringify({
            intent: 'CAPTURE',
            purchase_units: [{
                reference_id: purchaseId,
                description: description || 'OmniPOS Cloud Backup Tokens',
                amount: { currency_code: 'PHP', value: amountPHP.toFixed(2) }
            }],
            application_context: {
                return_url: redirectSuccessUrl,
                cancel_url: redirectFailedUrl,
                brand_name: 'OmniPOS',
                user_action: 'PAY_NOW'
            }
        })
    });
    const data = await resp.json().catch(() => null);
    if (!resp.ok || !data || !data.id) {
        throw new Error((data && data.message) || `PayPal error (HTTP ${resp.status})`);
    }
    const approveLink = (data.links || []).find((l) => l.rel === 'approve');
    return { id: data.id, approveUrl: approveLink && approveLink.href };
}
async function paypalCaptureOrder(orderId) {
    const token = await paypalAccessToken();
    const resp = await fetch(`${PAYPAL_API_BASE}/v2/checkout/orders/${orderId}/capture`, {
        method: 'POST',
        headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' }
    });
    const data = await resp.json().catch(() => null);
    if (!resp.ok || !data) throw new Error('Could not capture the PayPal order.');
    return data;
}
// PayPal webhook signatures ay hindi simpleng HMAC — kailangang tawagan
// ang "verify-webhook-signature" endpoint mismo ng PayPal, na binibigyan
// ng mga "Paypal-*" headers galing sa orihinal na webhook request kasama
// ng naka-parse (JSON) na event body.
async function verifyPaypalWebhook(headers, parsedBody) {
    if (!PAYPAL_WEBHOOK_ID) return false;
    try {
        const token = await paypalAccessToken();
        const resp = await fetch(`${PAYPAL_API_BASE}/v1/notifications/verify-webhook-signature`, {
            method: 'POST',
            headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
            body: JSON.stringify({
                auth_algo: headers['paypal-auth-algo'],
                cert_url: headers['paypal-cert-url'],
                transmission_id: headers['paypal-transmission-id'],
                transmission_sig: headers['paypal-transmission-sig'],
                transmission_time: headers['paypal-transmission-time'],
                webhook_id: PAYPAL_WEBHOOK_ID,
                webhook_event: parsedBody
            })
        });
        const data = await resp.json().catch(() => null);
        return !!(data && data.verification_status === 'SUCCESS');
    } catch (err) {
        console.error('⚠️  PayPal webhook verification failed:', err.message);
        return false;
    }
}

// ---- Dragonpay (hosted checkout — GCash / InstaPay & PESONet bank
// transfer / Over-the-Counter sa 7-Eleven, Cebuana Lhuillier, LBC, atbp.,
// lahat sa IISANG "Request Payment" redirect, kaparehong konsepto ng
// Xendit Invoice sa itaas) ----
const DRAGONPAY_MERCHANT_ID = process.env.DRAGONPAY_MERCHANT_ID || null;
const DRAGONPAY_SECRET_KEY = process.env.DRAGONPAY_SECRET_KEY || null;
// "test" (default, gamit ang test.dragonpay.ph sandbox) o "live".
const DRAGONPAY_ENV = process.env.DRAGONPAY_ENV === 'live' ? 'live' : 'test';
const DRAGONPAY_API_BASE = DRAGONPAY_ENV === 'live' ? 'https://gw.dragonpay.ph' : 'https://test.dragonpay.ph';
// Ginagamit sa pareho ng (a) pagbuo ng Request Payment digest — bago
// mag-redirect ang customer papunta sa Dragonpay — at (b) pag-verify ng
// Postback digest na pinapadala PABALIK ni Dragonpay pagkatapos magbayad.
// Colon(":")-joined ang mga input bago i-SHA1, base sa opisyal na
// Dragonpay Merchant Integration Guide — I-DOBLE-CHECK LANG ito laban
// sa pinakabagong guide na ibinigay sa iyo ni Dragonpay pagka-sign-up,
// dahil kritikal na tama ang eksaktong pagkakasunod-sunod ng fields
// para gumana ang signature.
function dragonpaySha1(input) {
    return crypto.createHash('sha1').update(input).digest('hex');
}
async function dragonpayCreateTransaction({ amountPHP, purchaseId, redirectSuccessUrl, description, email }) {
    if (!DRAGONPAY_MERCHANT_ID || !DRAGONPAY_SECRET_KEY) {
        const err = new Error('DRAGONPAY_MERCHANT_ID / DRAGONPAY_SECRET_KEY is not configured on the relay yet.');
        err.code = 'DRAGONPAY_NOT_CONFIGURED';
        throw err;
    }
    const amount = Number(amountPHP).toFixed(2);
    const ccy = 'PHP';
    const desc = String(description || 'OmniPOS Cloud Backup Tokens').slice(0, 100);
    const payerEmail = email || 'customer@omnipos.local';
    const digest = dragonpaySha1(`${DRAGONPAY_MERCHANT_ID}:${purchaseId}:${amount}:${ccy}:${desc}:${payerEmail}:${DRAGONPAY_SECRET_KEY}`);
    const params = new URLSearchParams({
        merchantid: DRAGONPAY_MERCHANT_ID,
        txnid: purchaseId,
        amount,
        ccy,
        description: desc,
        email: payerEmail,
        digest,
        // Balikan ang parehong "return" landing page na ginagamit na ng
        // lahat ng ibang provider sa itaas — ang totoong pag-credit ng
        // tokens ay nasa Postback pa rin (server-to-server), hindi dito.
        redirecturl: redirectSuccessUrl
    });
    return { id: purchaseId, checkoutUrl: `${DRAGONPAY_API_BASE}/Pay.aspx?${params.toString()}` };
}
// Postback digest (server-to-server, mula Dragonpay papunta sa
// DRAGONPAY_WEBHOOK_PATH sa ibaba): SHA1(txnid:refno:status:message:secretkey).
// Status codes ng Dragonpay: S=Success, F=Failure, P=Pending, U=Unknown,
// R=Refund, K=Chargeback, V=Void, A=Authorized (pre-auth lang).
function verifyDragonpayPostbackDigest({ txnid, refno, status, message, digest }) {
    if (!DRAGONPAY_SECRET_KEY || !digest) return false;
    const expected = dragonpaySha1(`${txnid}:${refno}:${status}:${message}:${DRAGONPAY_SECRET_KEY}`);
    return safeCompare(String(digest).toLowerCase(), expected);
}

// ---- Payment method catalog + registry ----
// Ito ang TANGING lugar na kailangang baguhin kapag may idadagdag pang
// bagong provider/method — awtomatiko nang susunod dito ang lahat ng
// endpoint (packages list, purchase/create, availability check).
const PAYMENT_METHOD_CATALOG = [
    { id: 'gcash', provider: 'paymongo', label: 'GCash' },
    { id: 'maya', provider: 'paymongo', label: 'Maya' },
    { id: 'online_banking', provider: 'paymongo', label: 'Online Banking (PayMongo)' },
    { id: 'qrph', provider: 'paymongo', label: 'QR Ph (PayMongo)' },
    { id: 'xendit_checkout', provider: 'xendit', label: 'GCash / Maya / Bank Transfer / Card (Xendit)' },
    { id: 'card', provider: 'stripe', label: 'Credit/Debit Card (Stripe)' },
    { id: 'paypal', provider: 'paypal', label: 'PayPal' },
    { id: 'dragonpay_checkout', provider: 'dragonpay', label: 'GCash / InstaPay / PESONet / Over-the-Counter (Dragonpay)' }
];
const PAYMENT_PROVIDERS_CONFIGURED = {
    paymongo: () => !!PAYMONGO_SECRET_KEY,
    xendit: () => !!XENDIT_SECRET_KEY,
    stripe: () => !!STRIPE_SECRET_KEY,
    paypal: () => !!(PAYPAL_CLIENT_ID && PAYPAL_CLIENT_SECRET),
    dragonpay: () => !!(DRAGONPAY_MERCHANT_ID && DRAGONPAY_SECRET_KEY)
};
function findPaymentMethod(methodId) {
    return PAYMENT_METHOD_CATALOG.find((m) => m.id === methodId) || null;
}
function isPaymentMethodAvailable(methodId) {
    const m = findPaymentMethod(methodId);
    if (!m) return false;
    const check = PAYMENT_PROVIDERS_CONFIGURED[m.provider];
    return !!(check && check());
}
// Ibinabalik lang ang mga method na TALAGANG naka-configure ang env
// var(s) nito sa Render ngayon — ito mismo ang isasagawa ng OMNIPOS
// bilang listahan sa dropdown/select ng "paraan ng bayad".
function getAvailablePaymentMethods() {
    return PAYMENT_METHOD_CATALOG.filter((m) => isPaymentMethodAvailable(m.id)).map((m) => ({ id: m.id, label: m.label, provider: m.provider }));
}
async function createPaymentCheckout({ method, amountPHP, purchaseId, base, description }) {
    const m = findPaymentMethod(method);
    if (!m) {
        const err = new Error('Invalid payment method.');
        err.code = 'UNSUPPORTED_METHOD';
        throw err;
    }
    if (!isPaymentMethodAvailable(method)) {
        const err = new Error(`${m.label} is not configured on the relay right now.`);
        err.code = 'PROVIDER_NOT_CONFIGURED';
        throw err;
    }
    const redirectSuccessUrl = `${base}/relay/cloud-tokens/return?purchaseId=${encodeURIComponent(purchaseId)}&result=success`;
    const redirectFailedUrl = `${base}/relay/cloud-tokens/return?purchaseId=${encodeURIComponent(purchaseId)}&result=failed`;
    if (m.provider === 'paymongo') {
        // AYOS: QR Ph ay hindi Sources-based (walang redirect/checkout_url)
        // — Payment Intent + QR code image ang ibinabalik nito sa halip,
        // kaya hiwalay itong branch (tingnan ang paymongoCreateQrPhIntent
        // sa itaas para sa detalye).
        if (method === 'qrph') {
            const intent = await paymongoCreateQrPhIntent({ amountPHP, description });
            return { provider: 'paymongo', providerRefId: intent.id, checkoutUrl: null, qrCodeImageUrl: intent.qrCodeImageUrl, expiresAt: intent.expiresAt };
        }
        const source = await paymongoCreateSource({ amountPHP, method, redirectSuccessUrl, redirectFailedUrl, description });
        return { provider: 'paymongo', providerRefId: source.id, checkoutUrl: source.attributes.redirect.checkout_url };
    }
    if (m.provider === 'xendit') {
        const invoice = await xenditCreateInvoice({ amountPHP, purchaseId, redirectSuccessUrl, redirectFailedUrl, description });
        return { provider: 'xendit', providerRefId: invoice.id, checkoutUrl: invoice.invoice_url };
    }
    if (m.provider === 'stripe') {
        const session = await stripeCreateCheckoutSession({ amountPHP, purchaseId, redirectSuccessUrl, redirectFailedUrl, description });
        return { provider: 'stripe', providerRefId: session.id, checkoutUrl: session.url };
    }
    if (m.provider === 'paypal') {
        const order = await paypalCreateOrder({ amountPHP, purchaseId, redirectSuccessUrl, redirectFailedUrl, description });
        if (!order.approveUrl) throw new Error('PayPal did not return an approval link.');
        return { provider: 'paypal', providerRefId: order.id, checkoutUrl: order.approveUrl };
    }
    if (m.provider === 'dragonpay') {
        const txn = await dragonpayCreateTransaction({ amountPHP, purchaseId, redirectSuccessUrl, description });
        return { provider: 'dragonpay', providerRefId: txn.id, checkoutUrl: txn.checkoutUrl };
    }
    throw new Error('Unhandled payment provider.');
}
async function ensureCloudTokenSchema() {
    if (!pgPool) return;
    await pgPool.query(`
        CREATE TABLE IF NOT EXISTS cloud_token_wallets (
            installation_id TEXT PRIMARY KEY,
            balance_tokens  NUMERIC NOT NULL DEFAULT 0,
            auto_sync_enabled BOOLEAN NOT NULL DEFAULT true,
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    `);
    await pgPool.query(`
        CREATE TABLE IF NOT EXISTS cloud_token_ledger (
            id              BIGSERIAL PRIMARY KEY,
            installation_id TEXT NOT NULL,
            type            TEXT NOT NULL, 
            tokens          NUMERIC NOT NULL,
            balance_after   NUMERIC,
            note            TEXT,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    `);
    await pgPool.query(`
        CREATE TABLE IF NOT EXISTS cloud_token_purchases (
            purchase_id     TEXT PRIMARY KEY,
            installation_id TEXT NOT NULL,
            package_id      TEXT,
            tokens          NUMERIC NOT NULL,
            amount_php      NUMERIC NOT NULL,
            method          TEXT NOT NULL,
            source_id       TEXT,
            status          TEXT NOT NULL DEFAULT 'pending', 
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    `);
    // AYOS: dating PayMongo lang, kaya walang "provider" column pa dati —
    // idinagdag ito para malaman ng webhook kung aling provider (PayMongo,
    // Xendit, Stripe, PayPal, ...) ang gumawa ng source_id/providerRefId na
    // naka-imbak sa row na ito.
    await pgPool.query(`ALTER TABLE cloud_token_purchases ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 'paymongo';`);
    // AYOS/BUGFIX: dating Math.ceil ang laging ginagamit para sa presyo
    // (tokens) kada isang sync — kaya lagi at lagi itong LUMALAGO kumpara
    // sa advertised monthly price ng tier (hal. Pro tier: dapat ₱399/buwan
    // pero sa Math.ceil, aabot ng ~720 tokens/buwan kung eksaktong
    // sinundan ang schedule nito — +80%). Ang dalawang column na ito ang
    // nagbibigay-daan sa EKSAKTONG (fractional) na pag-charge sa halip:
    // sa bawat sync, idinadagdag lang ang eksaktong bahagi (hindi
    // pinapalago) sa "sync_fraction_accrued", at isang WHOLE token lamang
    // ang aktwal na binabawas sa balance kapag umabot na sa 1.0 pataas ang
    // naipong fraction — tingnan ang consumeCloudTokensForSyncExact().
    await pgPool.query(`ALTER TABLE cloud_token_wallets ADD COLUMN IF NOT EXISTS sync_fraction_accrued NUMERIC NOT NULL DEFAULT 0;`);
    await pgPool.query(`ALTER TABLE cloud_token_wallets ADD COLUMN IF NOT EXISTS sync_fraction_tier TEXT;`);
    // ===================================================================
    // AYOS/BAGO: idinagdag ang `category` column sa cloud_token_ledger —
    // dating ang paraan lang ng pag-identify kung anong klaseng entry ito
    // ay ang MANUAL na pag-parse ng `note` text (hal. maghahanap ng
    // "Cloud Backup activation" sa string) — mahina ito at madaling
    // magkamali/mag-overlap (hal. "Purchase" ay ginagamit PAREHO para sa
    // pagbili ng TOKENS mismo, at para sa pagbili ng isang FEATURE gamit
    // tokens — magkaibang bagay pero parehong salita). Ngayon, ONE FIXED
    // KEYWORD lang bawat kategorya (tingnan ang CLOUD_TOKEN_LEDGER_CATEGORIES
    // sa ibaba) — exact-match filtering, hindi na basta paghahanap sa text.
    // Ang `trigger_type` naman ay para lang sa mga sync-related na
    // category (SYNC_CHARGE) — 'manual' o 'automatic'.
    await pgPool.query(`ALTER TABLE cloud_token_ledger ADD COLUMN IF NOT EXISTS category TEXT;`);
    await pgPool.query(`ALTER TABLE cloud_token_ledger ADD COLUMN IF NOT EXISTS trigger_type TEXT;`);
    // AYOS/BAGO: bagong LIGHTWEIGHT na table — para sa mga auto/manual sync
    // na TOTOONG may presyo (fraction ng token, hal. 0.010) pero HINDI pa
    // umaabot sa 1 buong token kaya WALANG binabago sa totoong balance.
    // Sinasadyang HIWALAY ito sa cloud_token_ledger (na siyang OFFICIAL na
    // audit trail ng TOTOONG pagbabago ng balance) — para hindi bumigat ang
    // pangunahing ledger ng maraming rows na walang totoong balance impact,
    // habang nananatiling makikita pa rin ng customer ang BAWAT sync sa
    // Transaction History (per kahilingan). May automatic na pag-prune ito
    // (tingnan ang pruneCloudSyncActivity() sa ibaba) para hindi lumaki
    // nang walang hanggan.
    await pgPool.query(`
        CREATE TABLE IF NOT EXISTS cloud_sync_activity (
            id              BIGSERIAL PRIMARY KEY,
            installation_id TEXT NOT NULL,
            trigger_type    TEXT NOT NULL,
            cost_tokens     NUMERIC NOT NULL,
            accrued_after   NUMERIC NOT NULL,
            size_bytes      BIGINT,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    `);
    await pgPool.query(`CREATE INDEX IF NOT EXISTS idx_cloud_sync_activity_install ON cloud_sync_activity (installation_id, created_at DESC);`);
    console.log('✅ Omni Token wallet Postgres schema ready (cloud_token_wallets, cloud_token_ledger, cloud_token_purchases, cloud_sync_activity).');
}
// ===================================================================
// AYOS/BAGO: pinagkasunduang FIXED na listahan ng categories — ito ang
// EXACT keyword na naka-store sa `category` column, at ito rin ang
// pinagbabatayan ng filter dropdown sa Transaction History modal (OMNIPOS
// frontend). Bawat isa ay may sariling malinaw na kahulugan, walang
// pagkakapareho ng salita sa pagitan ng magkaibang klase ng transaksyon.
// ===================================================================
const CLOUD_TOKEN_LEDGER_CATEGORIES = {
    TOKEN_PURCHASE: 'Token Purchase',       // bumili ng Omni Tokens gamit totoong pera (GCash/atbp.)
    FEATURE_UNLOCK: 'Feature Activation',   // nag-activate ng bagong paid plan/feature (hal. Cloud Backup) gamit tokens
    ADDON_PURCHASE: 'Add-on Purchase',      // bumili ng karagdagang module/feature (à la carte) gamit tokens
    SYNC_CHARGE: 'Cloud Sync Charge',       // WHOLE token na na-deduct mula sa isang sync (manual o auto)
    SYNC_FRACTION: 'Auto-Sync Activity',    // fractional cost lang, WALANG na-deduct na buong token pa
    RESTORE_CHARGE: 'Cloud Restore Charge', // WHOLE token na na-deduct mula sa isang cloud restore (auto-charge or manual admin charge)
    STORAGE_HOLDING_FEE: 'Storage Holding Fee', // AYOS/BAGO: scheduled (hindi naka-depende sa bilang ng syncs) na bayad para sa datos na TULOY-TULOY na nakaupo sa Neon storage — naniningil kahit walang sync na naganap, dahil tuloy-tuloy din ang bayad ng developer kay Neon para sa storage na iyon
    REFUND: 'Refund'                        // ibinalik na tokens dahil sa failed/incomplete sync
};
// AYOS (cost-optimization): i-cache sa memory ang buong /relay/cloud-tokens/wallet
// response (balance + last-20 ledger + pending purchases) — 3 Neon queries kada
// tawag dati, at hanggang 120x/oras kada device pwedeng tawagin. Ang cache ay
// ini-invalidate agad sa mismong sandaling magbago ang balance (check-and-consume,
// consumeCloudTokensForSyncExact, o successful na payment webhook), kaya hindi ito
// magiging stale sa totoong paggamit — TTL lang ito bilang huling safety net.
const WALLET_CACHE_TTL_MS = 45 * 1000; // 45 segundo
const walletResponseCache = new Map(); // installationId -> { data, expiresAt }
function getWalletCache(installationId) {
    const entry = walletResponseCache.get(installationId);
    if (!entry) return null;
    if (Date.now() >= entry.expiresAt) {
        walletResponseCache.delete(installationId);
        return null;
    }
    return entry.data;
}
function setWalletCache(installationId, data) {
    walletResponseCache.set(installationId, { data, expiresAt: Date.now() + WALLET_CACHE_TTL_MS });
}
function invalidateWalletCache(installationId) {
    walletResponseCache.delete(installationId);
}
// ===================================================================
// AYOS/BAGO: retention policy para sa cloud_sync_activity (fractional
// auto-sync visibility entries). Bakit 30 ARAW: ito ang tumutugma sa
// isang buong billing cycle (ang "Est. Total Required Monthly" na
// makikita sa Omni Tokens page) — sapat na para makita ng customer ang
// BUONG huling buwan ng sync activity kahit anong dalas ng auto-sync
// niya, habang pinapanatili ang table na maliit (karaniwang ~30 row
// bawat installation kung araw-araw ang auto-sync — mabilis i-query,
// hindi tumataas nang walang hanggan). Time-based (hindi bilang-based)
// ang retention dahil mas makatarungan ito — pareho ang "1 buwan" na
// makikita ng lahat, kahit iba-iba ang dalas ng sync ng bawat customer.
// Hindi ito nakakaapekto sa cloud_token_ledger (ang OFFICIAL audit
// trail ng totoong balance changes) — permanente pa rin ang mga entries
// doon.
// ===================================================================
const CLOUD_SYNC_ACTIVITY_RETENTION_DAYS = 30;
async function pruneCloudSyncActivity() {
    if (!pgPool) return;
    try {
        const result = await pgPool.query(
            `DELETE FROM cloud_sync_activity WHERE created_at < now() - interval '${CLOUD_SYNC_ACTIVITY_RETENTION_DAYS} days'`
        );
        if (result.rowCount > 0) {
            console.log(`🧹 CLOUD_SYNC_ACTIVITY: na-prune ang ${result.rowCount} sync-fraction entry(ies) na mas matanda sa ${CLOUD_SYNC_ACTIVITY_RETENTION_DAYS} araw.`);
        }
    } catch (err) {
        console.error('⚠️ CLOUD_SYNC_ACTIVITY: hindi na-prune:', err.message);
    }
}
setInterval(pruneCloudSyncActivity, 24 * 60 * 60 * 1000);
async function getOrCreateCloudTokenWallet(installationId) {
    const result = await queryWithRetry(pgPool, 'SELECT installation_id, balance_tokens, auto_sync_enabled FROM cloud_token_wallets WHERE installation_id = $1', [installationId]);
    if (result.rows[0]) return result.rows[0];
    const inserted = await queryWithRetry(
        pgPool,
        `INSERT INTO cloud_token_wallets (installation_id, balance_tokens, auto_sync_enabled) VALUES ($1, 0, true)
         ON CONFLICT (installation_id) DO UPDATE SET installation_id = EXCLUDED.installation_id
         RETURNING installation_id, balance_tokens, auto_sync_enabled`,
        [installationId]
    );
    return inserted.rows[0];
}
async function creditCloudTokens(installationId, tokens, note, category = 'TOKEN_PURCHASE') {
    await getOrCreateCloudTokenWallet(installationId);
    const result = await queryWithRetry(
        pgPool,
        `UPDATE cloud_token_wallets SET balance_tokens = balance_tokens + $2, updated_at = now()
         WHERE installation_id = $1 RETURNING balance_tokens`,
        [installationId, tokens]
    );
    const balanceAfter = result.rows[0] ? Number(result.rows[0].balance_tokens) : null;
    await queryWithRetry(
        pgPool,
        `INSERT INTO cloud_token_ledger (installation_id, type, tokens, balance_after, note, category) VALUES ($1, 'purchase', $2, $3, $4, $5)`,
        [installationId, tokens, balanceAfter, note || null, category]
    );
    invalidateWalletCache(installationId);
    return balanceAfter;
}
const MODULE_SUBSCRIPTION_FEATURE_IDS = ['rbac_management', 'multi_branch', 'ai_assistant'];
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
    },
    ai_assistant: {
        id: 'ai_assistant',
        name: 'OmniPOS AI Assistant',
        price: { monthly: 179, yearly: 1790 }
    }
};
const MODULE_SUBSCRIPTION_BILLING_DAYS = { monthly: 30, yearly: 365 };
const MODULE_SUBSCRIPTION_GRACE_PERIOD_DAYS = 7;
const MODULE_SUBSCRIPTION_GRACE_PERIOD_MS = MODULE_SUBSCRIPTION_GRACE_PERIOD_DAYS * 24 * 60 * 60 * 1000;
const MODULE_SUBSCRIPTION_OVERRIDES_PATH = path.join(__dirname, 'module-subscription-overrides.json');
async function loadModuleSubscriptionOverrides() {
    const fromStore = await getPersistentJSON('module-subscription-overrides', null);
    if (fromStore !== null) return fromStore;
    try {
        return JSON.parse(fs.readFileSync(MODULE_SUBSCRIPTION_OVERRIDES_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveModuleSubscriptionOverrides(obj) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('module-subscription-overrides', obj);
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
// ===================================================================
// NEON POSTGRES PRICING (para sa "Database Health" tab ng admin panel)
// ===================================================================
// Hindi ito galing sa isang live/official Neon pricing API — walang
// pampublikong API si Neon para dito, kaya ang mga rates sa ibaba ay
// manual na kinopya mula sa https://neon.com/docs/introduction/plans
// (verified 2026-08). Kapag nagbago ang pricing ni Neon, i-update ang
// NEON_PRICING_BASE sa ibaba (o gamitin ang admin panel override —
// tingnan ang /relay/admin/api/pricing/neon sa baba, parehong pattern
// ng cloudBackupPlanOverrides) — awtomatikong susundan ito ng
// /relay/admin/api/db-health sa susunod na request, walang redeploy
// na kailangan.
const NEON_PRICING_VERIFIED_AT = '2026-08';
// GAWA/BAGO: staleness check — kung matagal nang hindi na-verify ang
// NEON_PRICING_VERIFIED_AT (>6 buwan), posibleng luma na ang mga rate
// dito kumpara sa aktwal na kasalukuyang presyo ni Neon. Ginagamit sa
// db-health/pricing-neon payload para may makitang warning sa UI.
function getNeonPricingStaleness() {
    const [y, m] = NEON_PRICING_VERIFIED_AT.split('-').map(Number);
    const verifiedAtMs = Date.UTC(y, m - 1, 1);
    const monthsSince = (Date.now() - verifiedAtMs) / (30.44 * 24 * 60 * 60 * 1000);
    const STALE_THRESHOLD_MONTHS = 6;
    return {
        verifiedAt: NEON_PRICING_VERIFIED_AT,
        monthsSinceVerified: Math.round(monthsSince * 10) / 10,
        isStale: monthsSince >= STALE_THRESHOLD_MONTHS,
        staleThresholdMonths: STALE_THRESHOLD_MONTHS
    };
}
const NEON_PRICING_SOURCE_URL = 'https://neon.com/docs/introduction/plans';
const NEON_PRICING_BASE = {
    free: {
        id: 'free', name: 'Free', monthlyBaseUSD: 0,
        computeRatePerCUHourUSD: 0, storageRatePerGBMonthUSD: 0,
        includedStorageGB: 0.5, includedComputeHours: 100, includedProjects: 100,
        includedBranchesPerProject: 10, includedEgressGB: 5,
        maxAutoscaleCU: 2, hasHardCap: true,
        notes: 'Walang bayad. May hard cap: kapag naubos ang 0.5 GB storage o 100 CU-hours/project, tumitigil ang compute hanggang susunod na billing cycle o mag-upgrade.'
    },
    launch: {
        id: 'launch', name: 'Launch', monthlyBaseUSD: 0,
        computeRatePerCUHourUSD: 0.106, storageRatePerGBMonthUSD: 0.35,
        includedBranchesPerProject: 10, extraBranchRatePerMonthUSD: 1.50,
        includedEgressGB: 100, egressOverageRatePerGBUSD: 0.10,
        maxAutoscaleCU: 16, hasHardCap: false,
        notes: 'Bayad ayon lang sa aktwal na gamit, walang minimum na bayad bawat buwan. Walang hard cap — pero tuloy-tuloy ang bayad sa storage kahit naka-suspend ang compute.'
    },
    scale: {
        id: 'scale', name: 'Scale', monthlyBaseUSD: 0,
        computeRatePerCUHourUSD: 0.222, storageRatePerGBMonthUSD: 0.35,
        includedBranchesPerProject: 10, extraBranchRatePerMonthUSD: 1.50,
        includedEgressGB: 100, egressOverageRatePerGBUSD: 0.10,
        maxAutoscaleCU: 16, hasHardCap: false,
        notes: 'Kaparehong storage rate ng Launch pero mas mataas ang compute rate — kapalit nito ang SOC2/HIPAA, SLA, at read replicas para sa production-grade na workload.'
    },
    enterprise: {
        id: 'enterprise', name: 'Enterprise', monthlyBaseUSD: null,
        computeRatePerCUHourUSD: null, storageRatePerGBMonthUSD: null,
        includedBranchesPerProject: null, extraBranchRatePerMonthUSD: null,
        includedEgressGB: null, egressOverageRatePerGBUSD: null,
        maxAutoscaleCU: null, hasHardCap: false, customPricing: true,
        notes: 'Custom quote lang — kailangang makipag-ugnayan sa Neon sales para dito.'
    },
    // Mga karagdagang rate na hiwalay sa itaas (parehas ito sa Launch at Scale)
    instantRestoreRatePerGBMonthUSD: 0.20,
    snapshotRatePerGBMonthUSD: 0.09,
    // AYOS/BAGO: idinagdag ang COMPUTE cost assumption para sa Cloud Backup
    // per-sync charge (tingnan ang computeRealCloudBackupSyncCostPHP() sa
    // ibaba). Bukod sa storage, may bayad din ang Neon sa COMPUTE
    // (CU-hours) — kaya kailangan itong isama sa presyo, hindi puwedeng
    // basta tanggalin (kung tatanggalin ito, mas mababa ang icha-charge sa
    // customer kaysa sa TALAGANG binabayaran sa Neon — ang developer/negosyo
    // na lang ang magpapasan ng pagkakaiba).
    //
    // AYOS/SELF-CALIBRATING: `assumedCU` ay direktang kopya ng OFFICIAL
    // minimum compute size ni Neon (hindi assumption, totoong published
    // spec). Pero ang `assumedBaseSeconds`/`assumedSecondsPerMB` sa ibaba ay
    // mga COLD-START SEED lang ngayon — panandaliang gamit habang wala pang
    // sapat na TOTOONG na-measure na datos. Sa bawat successful sync,
    // sinusukat na ng RELAY (sa /relay/cloud-backup/upload/finish) ang
    // AKTWAL na tagal ng pagsulat sa Neon Postgres kumpara sa laking data —
    // tingnan ang recordCloudBackupSyncTiming() sa ibaba. Kapag umabot na sa
    // CLOUD_BACKUP_TIMING_MIN_SAMPLES na totoong sample, awtomatiko nang
    // pinapalitan ng least-squares regression (batay sa totoong measured
    // duration) ang dalawang value na ito sa neonPricingOverrides —
    // AGAD itong nagagamit sa SUSUNOD na sync (hindi lang naka-log,
    // direktang ginagamit na sa pag-charge sa customer). Ang mga seed value
    // sa ibaba ay ginagamit lang bago pa umabot sa minimum sample count.
    // AYOS/BAGO: itinaas ang seed values papuntang mas malapit sa "worst
    // case" (hindi na "best case") habang wala pang sapat na totoong
    // measured data — 3s -> 5s na assumedBaseSeconds, 0.5 -> 0.7 s/MB na
    // assumedSecondsPerMB. Kasabay ito ng Universal Safety Margin sa itaas
    // bilang karagdagang proteksyon, pero mas mabuting hindi rin masyadong
    // optimistiko ang panimulang tantiya mismo — awtomatiko naman itong
    // mapapalitan ng TOTOONG measured value (recordCloudBackupSyncTiming())
    // pagkatapos ng ilang totoong sync.
    cloudBackupSyncCompute: {
        assumedCU: 0.25,          // Neon's smallest/minimum compute size (official, hindi assumption)
        assumedBaseSeconds: 5,    // SEED LANG (mas konserbatibo) — papalitan ng measured value pagkatapos ng ilang totoong sync
        assumedSecondsPerMB: 0.7  // SEED LANG (mas konserbatibo) — papalitan ng measured value pagkatapos ng ilang totoong sync
    }
};
const NEON_PRICING_OVERRIDES_PATH = path.join(__dirname, 'neon-pricing-overrides.json');
async function loadNeonPricingOverrides() {
    const fromStore = await getPersistentJSON('neon-pricing-overrides', null);
    if (fromStore !== null) return fromStore;
    try {
        return JSON.parse(fs.readFileSync(NEON_PRICING_OVERRIDES_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveNeonPricingOverrides(obj) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('neon-pricing-overrides', obj);
        return;
    }
    try {
        fs.writeFileSync(NEON_PRICING_OVERRIDES_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang neon-pricing-overrides.json:', err);
    }
}
let neonPricingOverrides = {}; 
let NEON_PRICING = JSON.parse(JSON.stringify(NEON_PRICING_BASE));
const NEON_PRICING_TIER_IDS = ['free', 'launch', 'scale', 'enterprise'];
// ===================================================================
// AYOS/BAGO: SELF-CALIBRATING na measurement ng aktwal na compute time
// kada Cloud Backup sync — pinapalitan ang hardcoded na
// assumedBaseSeconds/assumedSecondsPerMB (dating pure guess) ng TOTOONG
// na-measure na tagal ng pagsulat sa Neon Postgres, batay sa laki ng
// datos. Ginagamit ang simpleng linear regression (least squares) sa
// mga sample: durationSeconds ≈ baseSeconds + (secondsPerMB × sizeMB).
//
// Bakit hindi lang "logging": ang bawat successful sync ay nagdaragdag
// ng bagong (sizeMB, durationSeconds) sample sa persisted running sums
// (neonPricingOverrides.cloudBackupSyncCompute) — pagkatapos, kaagad
// tinatawag ang recomputeNeonPricing(), kaya ang NA-UPDATE na
// assumedBaseSeconds/assumedSecondsPerMB ay AGAD na epektibo sa
// SUSUNOD na sync (na siyang gagamitin ni computeRealCloudBackupSyncCostPHP()
// para sa presyo na icha-charge sa customer) — hindi na kailangan hintayin
// ng developer/admin na i-review ang logs at i-adjust nang manual.
// ===================================================================
const CLOUD_BACKUP_TIMING_MIN_SAMPLES = 5; // gaano karaming totoong sample bago gamitin ang regression (baguhin/i-tune kung gusto)
const CLOUD_BACKUP_TIMING_MAX_SAMPLES = 5000; // takip para hindi ma-overflow ang running sums kahit matagal nang tumatakbo
function recordCloudBackupSyncTiming(sizeMB, durationSeconds) {
    try {
        const sizeMBSafe = Math.max(0, Number(sizeMB) || 0);
        const durationSafe = Math.max(0, Number(durationSeconds) || 0);
        if (!isFinite(sizeMBSafe) || !isFinite(durationSafe)) return;
        const existing = neonPricingOverrides.cloudBackupSyncCompute || {};
        let n = Number(existing.measuredSampleCount) || 0;
        let sumX = Number(existing.measuredSumSizeMB) || 0;
        let sumY = Number(existing.measuredSumDurationSec) || 0;
        let sumXY = Number(existing.measuredSumSizeMBxDurationSec) || 0;
        let sumXX = Number(existing.measuredSumSizeMBSquared) || 0;
        n += 1;
        sumX += sizeMBSafe;
        sumY += durationSafe;
        sumXY += sizeMBSafe * durationSafe;
        sumXX += sizeMBSafe * sizeMBSafe;
        // AYOS/BUGFIX: dating basta Math.min(n, MAX) lang ang ginagawa —
        // na-cap si `n` pero PATULOY na tumataas nang walang hanggan ang mga
        // sums (sumX/sumY/sumXY/sumXX). Kapag na-cap na si `n`, hindi na
        // ito tumutugma sa TOTOONG dami ng datos na kinakatawan ng mga sums
        // — sisira nito nang unti-unti ang regression (slope/intercept)
        // sa mahabang panahon. AYOS: kapag lumagpas sa MAX_SAMPLES, i-scale
        // pababa ang `n` KASAMA ang lahat ng 4 sums gamit ang parehong
        // proportion — pinapanatili nito nang EKSAKTO ang parehong
        // slope/intercept (napapatunayan sa least-squares algebra: kapag
        // magkakatulad ang scale factor sa n, Σx, Σy, Σxy, at Σx², walang
        // nababago sa resulta), habang unti-unting binibigyang mas mababang
        // timbang ang lumang datos — parang "forgetting factor" — sa halip
        // na basta pabayaang mag-drift ang matematika.
        if (n > CLOUD_BACKUP_TIMING_MAX_SAMPLES) {
            const decayFactor = CLOUD_BACKUP_TIMING_MAX_SAMPLES / n;
            n = CLOUD_BACKUP_TIMING_MAX_SAMPLES;
            sumX *= decayFactor;
            sumY *= decayFactor;
            sumXY *= decayFactor;
            sumXX *= decayFactor;
        }
        const updated = { ...existing, measuredSampleCount: n, measuredSumSizeMB: sumX, measuredSumDurationSec: sumY, measuredSumSizeMBxDurationSec: sumXY, measuredSumSizeMBSquared: sumXX };
        if (n >= CLOUD_BACKUP_TIMING_MIN_SAMPLES) {
            const denom = (n * sumXX) - (sumX * sumX);
            let measuredSecondsPerMB = NEON_PRICING_BASE.cloudBackupSyncCompute.assumedSecondsPerMB;
            let measuredBaseSeconds = NEON_PRICING_BASE.cloudBackupSyncCompute.assumedBaseSeconds;
            if (Math.abs(denom) > 1e-9) {
                const slope = ((n * sumXY) - (sumX * sumY)) / denom;
                const intercept = (sumY - (slope * sumX)) / n;
                // I-clamp sa >= 0 — hindi dapat negatibo ang tagal ng compute,
                // kung negatibo ang lumabas sa regression (madalas dahil sa
                // ingay/kaunti pang sample), gamitin na lang ang seed default
                // para hindi ma-undercharge ang mga susunod na sync.
                measuredSecondsPerMB = (isFinite(slope) && slope >= 0) ? slope : NEON_PRICING_BASE.cloudBackupSyncCompute.assumedSecondsPerMB;
                measuredBaseSeconds = (isFinite(intercept) && intercept >= 0) ? intercept : NEON_PRICING_BASE.cloudBackupSyncCompute.assumedBaseSeconds;
            }
            updated.assumedBaseSeconds = measuredBaseSeconds;
            updated.assumedSecondsPerMB = measuredSecondsPerMB;
            updated.calibrated = true;
        } else {
            updated.calibrated = false;
        }
        neonPricingOverrides.cloudBackupSyncCompute = updated;
        saveNeonPricingOverrides(neonPricingOverrides);
        recomputeNeonPricing();
        if (updated.calibrated) {
            console.log(`📏 CLOUD_BACKUP_TIMING: na-recalibrate gamit ang ${n} totoong sample — assumedBaseSeconds=${updated.assumedBaseSeconds.toFixed(4)}s, assumedSecondsPerMB=${updated.assumedSecondsPerMB.toFixed(4)}s/MB (agad gagamitin sa susunod na sync).`);
        } else {
            console.log(`📏 CLOUD_BACKUP_TIMING: naitala ang sample #${n} (${sizeMBSafe.toFixed(2)}MB, ${durationSafe.toFixed(3)}s) — gagamitin pa muna ang seed values hanggang umabot sa ${CLOUD_BACKUP_TIMING_MIN_SAMPLES} sample.`);
        }
    } catch (err) {
        console.error('⚠️ CLOUD_BACKUP_TIMING: hindi na-record ang timing sample:', err.message);
    }
}
function recomputeNeonPricing() {
    const merged = { ...NEON_PRICING_BASE };
    for (const tier of NEON_PRICING_TIER_IDS) {
        merged[tier] = { ...NEON_PRICING_BASE[tier], ...(neonPricingOverrides[tier] || {}) };
    }
    merged.instantRestoreRatePerGBMonthUSD = (typeof neonPricingOverrides.instantRestoreRatePerGBMonthUSD === 'number')
        ? neonPricingOverrides.instantRestoreRatePerGBMonthUSD : NEON_PRICING_BASE.instantRestoreRatePerGBMonthUSD;
    merged.snapshotRatePerGBMonthUSD = (typeof neonPricingOverrides.snapshotRatePerGBMonthUSD === 'number')
        ? neonPricingOverrides.snapshotRatePerGBMonthUSD : NEON_PRICING_BASE.snapshotRatePerGBMonthUSD;
    const computeOverride = neonPricingOverrides.cloudBackupSyncCompute || {};
    merged.cloudBackupSyncCompute = {
        assumedCU: (typeof computeOverride.assumedCU === 'number') ? computeOverride.assumedCU : NEON_PRICING_BASE.cloudBackupSyncCompute.assumedCU,
        assumedBaseSeconds: (typeof computeOverride.assumedBaseSeconds === 'number') ? computeOverride.assumedBaseSeconds : NEON_PRICING_BASE.cloudBackupSyncCompute.assumedBaseSeconds,
        assumedSecondsPerMB: (typeof computeOverride.assumedSecondsPerMB === 'number') ? computeOverride.assumedSecondsPerMB : NEON_PRICING_BASE.cloudBackupSyncCompute.assumedSecondsPerMB
    };
    NEON_PRICING = merged;
}
// Anong Neon plan ang aktwal na ginagamit ngayon para sa bawat database
// (dalawang hiwalay na Neon project/database ang RELAY — Cloud Backup at
// Devices/License — kaya posibleng magkaiba ang plan ng bawat isa).
// Admin-configurable dahil hindi ito automatic na nalalaman ng RELAY
// (walang Neon account API key na naka-configure) — ito lang ang
// paraan para malaman ng /relay/admin/api/db-health kung anong hard cap
// (kung free) o rate (kung paid) ang dapat gamitin sa computation.
const NEON_CONFIGURED_PLAN_PATH = path.join(__dirname, 'neon-configured-plans.json');
async function loadNeonConfiguredPlans() {
    const fromStore = await getPersistentJSON('neon-configured-plans', null);
    if (fromStore !== null) return fromStore;
    try {
        return JSON.parse(fs.readFileSync(NEON_CONFIGURED_PLAN_PATH, 'utf8'));
    } catch (err) {
        return { cloudBackup: 'free', devices: 'free', build: 'free' };
    }
}
// AYOS/BAGO: dating "Redis OR file" lang ang save logic dito — ibig
// sabihin kapag naka-Postgres/Redis ka (karaniwan sa production), HINDI
// KAILANMAN nasusulat ang lokal na JSON file bilang backup. Kung ma-lose
// ang Postgres KV row (hal. maling migration, na-clear ang table) o wala
// pang Postgres/Redis configured, babalik ito sa hardcoded default
// (["free","free","free"]) NANG TAHIMIK sa susunod na restart — ito
// mismo ang inilarawan nating "butas" kanina (tingnan ang usapan
// tungkol sa computeNeonRealCost). Ngayon, laging sinusulat sa PAREHONG
// Postgres/Redis (setPersistentJSON — pangunahing storage talaga ngayon ay
// Postgres, "Redis" na lang ang pangalan) AT sa lokal na backup file,
// kaya may pagbabalikan kung ma-lose man ang isa.
function saveNeonConfiguredPlans(obj) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('neon-configured-plans', obj);
    }
    try {
        fs.writeFileSync(NEON_CONFIGURED_PLAN_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang neon-configured-plans.json:', err);
    }
}
let neonConfiguredPlans = { cloudBackup: 'free', devices: 'free', build: 'free' };
// ===================================================================
// PER-CLIENT COST ALLOCATION (Cloud Backup Neon project lang)
// ===================================================================
// Iisang Neon project ang Cloud Backup para sa LAHAT ng client (bawat
// installationId), kaya iisang bill lang ang lumalabas kay Neon —
// pinaghahalu-halo ang storage/compute ng lahat. Ang "maintenance fee"
// dito ay ang FLAT na dagdag bayad bawat client (para sa monitoring/
// pagpapanatili, HINDI Neon consumption) na idinadagdag PAGKATAPOS
// hatiin ang aktwal na Neon cost ayon sa proporsyon ng gamit ng bawat
// client — global default + pwedeng i-override bawat installationId.
const CLIENT_MAINTENANCE_FEE_PATH = path.join(__dirname, 'client-maintenance-fee.json');
const CLIENT_MAINTENANCE_FEE_DEFAULT = { defaultFeePHP: 150, perClientOverridePHP: {} };
async function loadClientMaintenanceFeeConfig() {
    const fromStore = await getPersistentJSON('client-maintenance-fee', null);
    if (fromStore !== null) return fromStore;
    try {
        return JSON.parse(fs.readFileSync(CLIENT_MAINTENANCE_FEE_PATH, 'utf8'));
    } catch (err) {
        return JSON.parse(JSON.stringify(CLIENT_MAINTENANCE_FEE_DEFAULT));
    }
}
function saveClientMaintenanceFeeConfig(obj) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('client-maintenance-fee', obj);
        return;
    }
    try {
        fs.writeFileSync(CLIENT_MAINTENANCE_FEE_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang client-maintenance-fee.json:', err);
    }
}
let clientMaintenanceFeeConfig = JSON.parse(JSON.stringify(CLIENT_MAINTENANCE_FEE_DEFAULT));
function getMaintenanceFeeForClient(installationId) {
    const override = clientMaintenanceFeeConfig.perClientOverridePHP || {};
    const v = override[installationId];
    return typeof v === 'number' && isFinite(v) ? v : (clientMaintenanceFeeConfig.defaultFeePHP || 0);
}
// AYOS: sa unang successful na Cloud Backup subscribe/renew (confirm-unlock,
// tingnan sa /relay/confirm-unlock), naka-"paid" na ang maintenance fee
// hanggang sa mismong petsa ng pag-expire ng subscription period na iyon
// (parehong petsa ng subscription token mismo) — kaya HINDI na ito
// idinadagdag sa "total" na babayaran habang aktibo pa ang subscription.
// Pag-expire (walang na-renew), awtomatiko itong "babalik" bilang bahagi ng
// dapat bayaran sa susunod na total — walang extra na "reset" step, oras
// lang mismo (Date.now() vs paidUntil) ang sinusunod.
const CLIENT_MAINTENANCE_FEE_PAID_UNTIL_PATH = path.join(__dirname, 'client-maintenance-fee-paid-until.json');
async function loadClientMaintenanceFeePaidUntil() {
    const fromStore = await getPersistentJSON('client-maintenance-fee-paid-until', null);
    if (fromStore !== null) return fromStore;
    try {
        return JSON.parse(fs.readFileSync(CLIENT_MAINTENANCE_FEE_PAID_UNTIL_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveClientMaintenanceFeePaidUntil(obj) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('client-maintenance-fee-paid-until', obj);
        return;
    }
    try {
        fs.writeFileSync(CLIENT_MAINTENANCE_FEE_PAID_UNTIL_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang client-maintenance-fee-paid-until.json:', err);
    }
}
let clientMaintenanceFeePaidUntil = {}; 
function markMaintenanceFeePaidUntil(installationId, paidUntilMs) {
    if (!installationId || typeof paidUntilMs !== 'number' || !isFinite(paidUntilMs)) return;
    clientMaintenanceFeePaidUntil[installationId] = paidUntilMs;
    saveClientMaintenanceFeePaidUntil(clientMaintenanceFeePaidUntil);
}
function isMaintenanceFeePaidForClient(installationId) {
    const paidUntil = clientMaintenanceFeePaidUntil[installationId];
    return typeof paidUntil === 'number' && Date.now() < paidUntil;
}
// ===================================================================
// GAWA/BAGO: TIER-based na "maintenance fee" — kapalit ng flat/admin-
// configurable na halaga sa itaas (getMaintenanceFeeForClient/
// clientMaintenanceFeeConfig). Hiniling ito: dapat ang MONTHLY at YEARLY
// presyo ng aktwal na Cloud Backup tier (basic/standard/pro — mula sa
// Cloud Backup Pricing / pricing.html, CLOUD_BACKUP_PLANS) na kinuha o
// sinubscribe ng client ang lumalabas bilang "maintenance fee" sa
// cost-share widget, tier modal, at Client Cost Allocation admin — HINDI
// na isang hiwalay/independent na halagang naka-configure lang dito sa
// itaas. Ang mga lumang function/storage sa itaas (getMaintenanceFeeForClient,
// clientMaintenanceFeeConfig, atbp.) ay iniwan lang bilang legacy data
// (kasama pa rin sa admin backup/restore snapshot para hindi masira ang
// import/export) pero HINDI na ito ginagamit para sa aktwal na
// pagkukwenta ng bayarin ng kliyente.
function getCloudBackupSubscriptionForClient(installationId) {
    const entry = issuedUnlocks[installationId] && issuedUnlocks[installationId]['cloud_backup'];
    if (!entry) return { tier: null, billingCycle: null, active: false, expiresAt: null, isLifetime: false };
    const expiresAt = typeof entry.expiresAt === 'number' ? entry.expiresAt : null;
    const active = expiresAt === null ? true : Date.now() < expiresAt;
    // Legacy lifetime unlocks (mula bago naging subscription ang Cloud
    // Backup) ay walang expiresAt AT walang naka-record na tier — hindi na
    // dapat mag-apply ng buwanang/taunang plan fee sa mga ito kailanman,
    // dahil bayad na ito nang buo noon pa (one-time).
    const isLifetime = active && expiresAt === null && !entry.tier;
    return {
        tier: entry.tier || (active && !isLifetime ? 'basic' : null),
        billingCycle: entry.billingCycle || null,
        active,
        expiresAt,
        isLifetime
    };
}
function getCloudBackupTierPricePHP(tier) {
    const plan = tier && CLOUD_BACKUP_PLANS[tier];
    return plan ? { monthly: plan.price.monthly, yearly: plan.price.yearly, name: plan.name } : { monthly: 0, yearly: 0, name: null };
}

// ===================================================================
// USD -> PHP exchange rate (live, may cache + fallback)
// ===================================================================
// Gumagamit ng open.er-api.com (walang API key na kailangan). May
// in-memory cache (6 oras) para hindi paulit-ulit tinatawagan sa bawat
// request, at may hardcoded fallback rate kung mabigo ang fetch (hal.
// walang internet, nag-expire ang free API, atbp.) — laging ipapakita
// sa UI kung "live" o "fallback" ang rate na ginamit, kasama ang oras
// noong huling successful fetch.
const EXCHANGE_RATE_FALLBACK_USD_TO_PHP = 58.7;
const EXCHANGE_RATE_FALLBACK_NOTE_DATE = '2026-01';
const EXCHANGE_RATE_CACHE_MS = 6 * 60 * 60 * 1000;
let exchangeRateCache = { at: 0, rate: null, source: null, fetchedAt: null };
async function getUsdToPhpRate() {
    const now = Date.now();
    if (exchangeRateCache.rate && (now - exchangeRateCache.at) < EXCHANGE_RATE_CACHE_MS) {
        return exchangeRateCache;
    }
    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 6000);
        const resp = await fetch('https://open.er-api.com/v6/latest/USD', { signal: controller.signal });
        clearTimeout(timeout);
        const data = await resp.json();
        const rate = data && data.rates && typeof data.rates.PHP === 'number' ? data.rates.PHP : null;
        if (rate) {
            exchangeRateCache = { at: now, rate, source: 'open.er-api.com (live)', fetchedAt: now };
            return exchangeRateCache;
        }
    } catch (err) {
        console.warn('⚠️  Hindi ma-fetch ang live USD→PHP rate, gagamit ng fallback:', err.message);
    }
    // Fallback: gamitin ang huling successful live rate kung meron (kahit
    // lumagpas na sa cache window), kung wala talaga, gamitin ang hardcoded.
    if (exchangeRateCache.rate) {
        return { ...exchangeRateCache, source: exchangeRateCache.source + ' — stale, hindi na-refresh' };
    }
    return {
        at: now, rate: EXCHANGE_RATE_FALLBACK_USD_TO_PHP,
        source: `fallback (hardcoded, huling ni-verify noong ${EXCHANGE_RATE_FALLBACK_NOTE_DATE} — hindi live)`,
        fetchedAt: null
    };
}
// ===================================================================
// AYOS/BAGO: STORAGE HOLDING FEE — scheduled job (#2 sa safety-margin
// discussion). Bakit kailangan ito bukod sa per-sync charge sa itaas:
// ang per-sync cost ay naka-attach lang sa BILANG ng syncs — kung 0 ang
// syncs ng isang customer sa isang buwan, 0 din ang revenue mula sa
// customer na iyon, PERO tuloy-tuloy pa ring binabayaran ang Neon
// storage rate para sa datos niyang nakaupo doon (ang Neon ay
// naniningil base sa GB-buwan na naka-store, HINDI base sa bilang ng
// beses na na-access/na-sync ang datos). Ang storage holding fee na ito
// ay HIWALAY sa per-sync compute charge — ito ay isang bayad na
// naka-schedule (hindi naka-depende sa auto-sync interval ng tier),
// batay lang sa (a) ELAPSED TIME mula sa huling pagsingil at (b) ang
// AKTWAL na laki (size_bytes) ng backup — kaya kahit hindi kailanman
// mag-sync ang customer sa buong buwan, patuloy pa rin siyang naba-bill
// para sa storage na hawak niya, kagaya mismo ng paraan ng pagbabayad
// ng developer kay Neon.
//
// Tulad ng ibang "real cost" formula sa itaas, gumagamit din ito ng
// safe-by-default fallback (Scale tier kapag mali/malabo ang naka-
// configure na Neon plan dropdown) AT ng Universal Safety Margin
// (CLOUD_BACKUP_COST_SAFETY_MARGIN_MULTIPLIER) bago ma-convert sa
// PHP/tokens.
//
// TALA: sinasadyang pinapayagan nitong maging NEGATIVE ang balance_tokens
// (walang "insufficient" gate dito, di tulad ng consumeCloudTokensForSyncExact
// sa itaas) — dahil ito ay isang HINDI maiiwasang gastos na ginagastos
// pa rin ng developer kay Neon anuman ang balance ng customer; ang
// pag-block ng auto-sync/restore kapag naubos na ang balance ay
// nananatili sa mga existing gate sa ibang function — dito lang, sa
// "rent" mismo, walang paglaktaw.
// ===================================================================
const STORAGE_HOLDING_FEE_MIN_INTERVAL_MS = 20 * 60 * 60 * 1000; // ~20 oras — takip para hindi ma-double-charge kung mas madalas tumakbo ang sweep kaysa sa layunin nitong 24-oras na cadence
const STORAGE_HOLDING_FEE_MONTH_MS = 30 * 24 * 60 * 60 * 1000; // batayan ng "isang buwan" para sa prorating (kaayon ng ginamit na sa computeRealCloudBackupSyncCostPHP)
function computeStorageHoldingFeeCostPHP(sizeBytes, elapsedMs, usdToPhpRate) {
    const sizeBytesSafe = Math.max(0, Number(sizeBytes) || 0);
    const sizeGB = sizeBytesSafe / (1024 * 1024 * 1024);
    const elapsedMsSafe = Math.max(0, Number(elapsedMs) || 0);
    const neonPlanId = neonConfiguredPlans.cloudBackup || 'free';
    let neonTier = NEON_PRICING[neonPlanId];
    // Safe-by-default fallback — pareho ng ginawang fix sa sync/restore cost
    // functions sa itaas: Scale (pinakamataas na rate) sa halip na Launch.
    if (!neonTier || !neonTier.storageRatePerGBMonthUSD) neonTier = NEON_PRICING.scale;
    const monthlyStorageCostUSD = sizeGB * neonTier.storageRatePerGBMonthUSD;
    const fractionOfMonth = elapsedMsSafe / STORAGE_HOLDING_FEE_MONTH_MS;
    const proRatedCostUSD = monthlyStorageCostUSD * fractionOfMonth;
    const rate = usdToPhpRate || EXCHANGE_RATE_FALLBACK_USD_TO_PHP;
    // Universal Safety Margin — tingnan ang paliwanag sa computeRealCloudBackupSyncCostPHP() sa itaas.
    return proRatedCostUSD * rate * CLOUD_BACKUP_COST_SAFETY_MARGIN_MULTIPLIER;
}
// Isang buong pass sa lahat ng installation na may cloud backup data
// (size_bytes > 0) — kinukwenta at ide-deduct (₱1 = 1 token, kaayon ng
// existing pattern) ang storage holding fee na naipon mula noong huling
// pagkakataong na-bill ang bawat isa. Tumatakbo bilang naka-schedule na
// background job (tingnan ang setInterval sa ibaba) — hindi kailanman
// dapat i-trigger mula sa isang customer-facing request.
async function runStorageHoldingFeeSweep() {
    if (!pgPool) return;
    try {
        const { rate } = await getUsdToPhpRate();
        const metaRows = await pgPool.query(
            `SELECT installation_id FROM cloud_backup_meta WHERE size_bytes > 0`
        );
        for (const { installation_id: installationId } of metaRows.rows) {
            try {
                await runPgWriteTx(pgPool, async (client) => {
                    await client.query(
                        `INSERT INTO cloud_token_wallets (installation_id, balance_tokens, auto_sync_enabled) VALUES ($1, 0, true)
                         ON CONFLICT (installation_id) DO NOTHING`,
                        [installationId]
                    );
                    const metaRes = await client.query(
                        `SELECT size_bytes, storage_fee_last_billed_at, storage_fee_fraction_accrued FROM cloud_backup_meta WHERE installation_id = $1 FOR UPDATE`,
                        [installationId]
                    );
                    const metaRow = metaRes.rows[0];
                    if (!metaRow) return;
                    // Unang beses lang: itakda ang baseline, huwag maniningil
                    // para sa "hindi kilalang" nakaraang panahon (mas ligtas
                    // kaysa basta bigla na lang maniningil ng malaking halaga
                    // batay sa isang haka-hakang "simula").
                    if (!metaRow.storage_fee_last_billed_at) {
                        await client.query(
                            `UPDATE cloud_backup_meta SET storage_fee_last_billed_at = now() WHERE installation_id = $1`,
                            [installationId]
                        );
                        return;
                    }
                    const elapsedMs = Date.now() - new Date(metaRow.storage_fee_last_billed_at).getTime();
                    if (elapsedMs < STORAGE_HOLDING_FEE_MIN_INTERVAL_MS) return;
                    const walletRes = await client.query(
                        `SELECT balance_tokens FROM cloud_token_wallets WHERE installation_id = $1 FOR UPDATE`,
                        [installationId]
                    );
                    const currentBalance = Number(walletRes.rows[0].balance_tokens);
                    const costFraction = computeStorageHoldingFeeCostPHP(metaRow.size_bytes, elapsedMs, rate);
                    const baseFraction = Number(metaRow.storage_fee_fraction_accrued) || 0;
                    const newFraction = baseFraction + costFraction;
                    const wholeTokens = Math.floor(newFraction);
                    const remainder = newFraction - wholeTokens;
                    // Sinasadyang WALANG "insufficient" gate dito — tingnan
                    // ang paliwanag sa itaas kung bakit dapat itong tuloy-
                    // tuloy magsingil, kahit umabot pa sa negatibong balance.
                    const newBalance = currentBalance - wholeTokens;
                    await client.query(
                        `UPDATE cloud_backup_meta SET storage_fee_last_billed_at = now(), storage_fee_fraction_accrued = $2 WHERE installation_id = $1`,
                        [installationId, remainder]
                    );
                    if (wholeTokens > 0) {
                        await client.query(
                            `UPDATE cloud_token_wallets SET balance_tokens = $2, updated_at = now() WHERE installation_id = $1`,
                            [installationId, newBalance]
                        );
                        // AYOS/BUGFIX: 'automatic' ang ginamit dito (hindi 'scheduled')
                        // dahil ang OMNIPOS Transaction History UI (CT_CATEGORY_META /
                        // triggerLabel sa app.js) ay ang eksaktong string na 'automatic'
                        // lang ang kinikilala bilang "(Auto)" — kahit anong ibang value
                        // dito ay maling lalabas na "(Manual)", kahit na WALANG
                        // taong nag-trigger ng bayad na ito (fully automatic/system-
                        // scheduled job ito, walang manual path).
                        await client.query(
                            `INSERT INTO cloud_token_ledger (installation_id, type, tokens, balance_after, note, category, trigger_type) VALUES ($1, 'consume', $2, $3, $4, 'STORAGE_HOLDING_FEE', 'automatic')`,
                            [installationId, -wholeTokens, newBalance, `Storage holding fee — ${formatDuration(elapsedMs)}`]
                        );
                    }
                });
                invalidateWalletCache(installationId);
            } catch (err) {
                console.error(`⚠️ STORAGE_HOLDING_FEE: hindi na-process ang installation ${installationId}:`, err.message);
            }
        }
    } catch (err) {
        console.error('⚠️ runStorageHoldingFeeSweep error:', err.message);
    }
}
setInterval(runStorageHoldingFeeSweep, 24 * 60 * 60 * 1000);
setTimeout(runStorageHoldingFeeSweep, 90 * 1000); // unang check, 90 segundo pagkatapos mag-boot
// ===================================================================
// NEON ACCOUNT API — kunin ang TUNAY na compute/storage usage ng
// kasalukuyang billing period direkta mula sa Neon account (GET
// /projects/{project_id}), sa halip na yung "illustrative" na compute
// estimate lang. Gumagana ito kahit sa Free plan (walang bayad ang
// endpoint na ito), kaya ito ang ginamit dito imbes na yung
// consumption_history endpoint na Scale-plan-and-up lang.
// Reference: https://neon.com/docs/introduction/usage-calculations
// ===================================================================
const NEON_API_KEY = process.env.NEON_API_KEY || '';
const NEON_CLOUD_BACKUP_PROJECT_ID = process.env.NEON_CLOUD_BACKUP_PROJECT_ID || '';
const NEON_DEVICES_PROJECT_ID = process.env.NEON_DEVICES_PROJECT_ID || NEON_CLOUD_BACKUP_PROJECT_ID;
// BUILD/PUSH DATABASE Neon project ID — HIWALAY na project ito (pgPoolBuild),
// kaya SINASADYANG walang fallback sa NEON_CLOUD_BACKUP_PROJECT_ID/
// NEON_DEVICES_PROJECT_ID dito (di tulad ng devices sa itaas) — kung mali
// ang fallback, magiging maling project ang mala-attribute ng usage/cost.
// Kung blangko ito, magpapakita pa rin ang Database Health card ng laki
// ng build database (via pg_database_size), pero walang real Neon
// usage/cost card para dito hangga't hindi ito nalagyan.
const NEON_BUILD_PROJECT_ID = process.env.NEON_BUILD_PROJECT_ID || '';
const NEON_API_CONFIGURED = !!(NEON_API_KEY && (NEON_CLOUD_BACKUP_PROJECT_ID || NEON_DEVICES_PROJECT_ID || NEON_BUILD_PROJECT_ID));
const NEON_USAGE_CACHE_MS = 5 * 60 * 1000; // 5 min — huwag masyadong tawagin, kahit safe naman ang endpoint na ito (hindi ito gumigising ng suspended compute).
let neonUsageCache = {}; // keyed by projectId -> { at, data }
// Kung magkaparehong projectId ang cloud backup at devices (walang hiwalay
// na NEON_DEVICES_PROJECT_ID na naka-set), pareho silang tatawag dito
// nang sabay-sabay via Promise.all BAGO pa man mag-populate ang cache —
// kaya dini-dedupe dito gamit ang isang shared in-flight promise, para
// isa lang ang aktwal na HTTP request sa Neon sa parehong sandali.
let neonUsageInFlight = {}; // keyed by projectId -> Promise
async function getNeonProjectUsage(projectId) {
    if (!NEON_API_KEY || !projectId) return null;
    const cached = neonUsageCache[projectId];
    if (cached && (Date.now() - cached.at) < NEON_USAGE_CACHE_MS) return cached.data;
    if (neonUsageInFlight[projectId]) return neonUsageInFlight[projectId];
    const fetchPromise = (async () => {
        try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 8000);
            const resp = await fetch(`https://console.neon.tech/api/v2/projects/${projectId}`, {
                headers: { 'Authorization': `Bearer ${NEON_API_KEY}`, 'Accept': 'application/json' },
                signal: controller.signal
            });
            clearTimeout(timeout);
            if (!resp.ok) {
                console.warn(`⚠️  Neon API error (HTTP ${resp.status}) para sa project "${projectId}" — baka mali ang NEON_API_KEY o project ID.`);
                return cached ? cached.data : null;
            }
            const body = await resp.json();
            const p = (body && body.project) || {};
            const usage = {
                projectId,
                // MAHALAGA: WALANG top-level "plan_id" field sa GET
                // /projects/{project_id} response ng Neon (kaya laging
                // null/"—" ang lumalabas dati). Ang totoong lokasyon nito
                // ay nested sa project.owner.subscription_type (hal.
                // "free_v2", "scale_v3" — may version suffix, kaya
                // kailangan pa rin ng normalizeNeonPlanId sa ibaba).
                // Verified laban sa opisyal na schema (2026-09):
                // https://neon.com/docs/reference/api/projects/get-project
                planId: (p.owner && p.owner.subscription_type) || p.plan_id || p.pending_plan_id || null,
                consumptionPeriodStart: p.consumption_period_start || null,
                computeTimeSeconds: typeof p.compute_time_seconds === 'number' ? p.compute_time_seconds : null,
                activeTimeSeconds: typeof p.active_time_seconds === 'number' ? p.active_time_seconds : null,
                dataStorageBytesHour: typeof p.data_storage_bytes_hour === 'number' ? p.data_storage_bytes_hour : null,
                dataTransferBytes: typeof p.data_transfer_bytes === 'number' ? p.data_transfer_bytes : null
            };
            neonUsageCache[projectId] = { at: Date.now(), data: usage };
            return usage;
        } catch (err) {
            console.warn(`⚠️  Hindi ma-fetch ang Neon usage ng project "${projectId}":`, err.message);
            return cached ? cached.data : null;
        } finally {
            delete neonUsageInFlight[projectId];
        }
    })();
    neonUsageInFlight[projectId] = fetchPromise;
    return fetchPromise;
}
// I-tugma ang raw plan_id na ibinabalik ng Neon API (hal. "free_v2",
// "launch_v2", "scale_v2") papunta sa mga tier id na ginagamit ng
// NEON_PRICING sa itaas ("free"/"launch"/"scale"/"enterprise"). Nagbabalik
// ng null kung walang tumugma, para malinaw na hindi ito basta-basta
// ipipilit na "free" (na siyang magiging SANHI ng maling $0 na cost).
function normalizeNeonPlanId(rawPlanId) {
    if (!rawPlanId) return null;
    const cleaned = String(rawPlanId).toLowerCase().replace(/_v\d+$/, '').trim();
    return NEON_PRICING_TIER_IDS.includes(cleaned) ? cleaned : null;
}
// I-convert ang raw Neon API usage papuntang totoong cost (USD). MAHALAGA:
// ang RATES na ginagamit dito ay dapat batay sa AKTWAL na plan na sinasabi
// ng Neon API mismo (usage.planId, na-normalize) — HINDI sa "configured
// plan" dropdown ng admin panel (na para lang sa illustrative tier
// comparison table). Kung mali/luma ang dropdown value (hal. naka-"Free"
// pa rin kahit Launch/Scale na pala ang totoong account), pero ginamit pa
// rin ito bilang batayan ng rate, LALABAS na $0 ang cost kahit may
// totoong gastos na — ito mismo ang dating bug dito. Ang dropdown ay
// ginagamit na lang bilang FALLBACK kapag hindi na-detect/na-recognize
// ang plan_id na ibinalik ng Neon (hal. bagong plan name sa hinaharap).
function computeNeonRealCost(usage, fallbackConfiguredPlan) {
    if (!usage) return null;
    const detectedTierId = normalizeNeonPlanId(usage.planId);
    const tierId = detectedTierId || fallbackConfiguredPlan;
    const usedFallback = !detectedTierId;
    const tier = NEON_PRICING[tierId];
    if (!tier || tier.customPricing) return null;
    const cuHours = usage.computeTimeSeconds !== null ? usage.computeTimeSeconds / 3600 : null;
    const storageGBMonths = usage.dataStorageBytesHour !== null ? usage.dataStorageBytesHour / 744 / 1e9 : null;
    const avgComputeCUs = (usage.computeTimeSeconds !== null && usage.activeTimeSeconds) ? usage.computeTimeSeconds / usage.activeTimeSeconds : null;
    const computeCostUSD = (cuHours !== null && typeof tier.computeRatePerCUHourUSD === 'number') ? cuHours * tier.computeRatePerCUHourUSD : null;
    const storageCostUSD = (storageGBMonths !== null && typeof tier.storageRatePerGBMonthUSD === 'number') ? storageGBMonths * tier.storageRatePerGBMonthUSD : null;
    const totalUSD = (computeCostUSD !== null && storageCostUSD !== null) ? computeCostUSD + storageCostUSD : null;
    return {
        cuHours: cuHours !== null ? Math.round(cuHours * 100) / 100 : null,
        avgComputeCUs: avgComputeCUs !== null ? Math.round(avgComputeCUs * 1000) / 1000 : null,
        storageGBMonths: storageGBMonths !== null ? Math.round(storageGBMonths * 10000) / 10000 : null,
        dataTransferGB: usage.dataTransferBytes !== null ? Math.round((usage.dataTransferBytes / 1e9) * 1000) / 1000 : null,
        computeCostUSD: computeCostUSD !== null ? Math.round(computeCostUSD * 100) / 100 : null,
        storageCostUSD: storageCostUSD !== null ? Math.round(storageCostUSD * 100) / 100 : null,
        totalMonthlyCostUSD: totalUSD !== null ? Math.round(totalUSD * 100) / 100 : null,
        tierUsedForRates: tierId,
        usedFallbackPlan: usedFallback,
        rawPlanId: usage.planId,
        consumptionPeriodStart: usage.consumptionPeriodStart
    };
}
// I-project ang TUNAY na average compute usage (mula sa Neon API) papuntang
// buong 744-oras (31-araw) na billing period ni Neon, para magamit ito sa
// tier comparison table sa halip na yung generic na "0.25 CU × 8 oras/araw
// × 30 araw" na canned example. Halimbawa: kung 2.57 CU-hr na ang nagamit
// sa loob lang ng 10 oras mula nag-reset ang period, ang average rate na
// iyon (~0.257 CU-hr bawat oras) ang ipoproject papuntang 744 oras.
// Sinasadyang ibinabalik na null kung wala pang isang oras na lumipas sa
// period (masyadong maaga pa para maging stable/makatotohanan ang
// projection — isang spike lang sa unang minuto ay pwedeng lumabas na
// parang libu-libong pesos bawat buwan kung i-extrapolate agad).
const NEON_BILLING_PERIOD_HOURS = 744; // parehong constant na ginagamit ni Neon mismo — 31 araw x 24 oras
const PROJECTION_MIN_ELAPSED_HOURS = 1;
function projectFullPeriodCUHours(usage) {
    if (!usage || usage.computeTimeSeconds === null || !usage.consumptionPeriodStart) return null;
    const periodStartMs = new Date(usage.consumptionPeriodStart).getTime();
    if (!Number.isFinite(periodStartMs)) return null;
    const elapsedHours = (Date.now() - periodStartMs) / (60 * 60 * 1000);
    if (elapsedHours < PROJECTION_MIN_ELAPSED_HOURS) return null;
    const cuHoursSoFar = usage.computeTimeSeconds / 3600;
    const projectedCUHoursFullPeriod = cuHoursSoFar * (NEON_BILLING_PERIOD_HOURS / elapsedHours);
    return {
        elapsedHours: Math.round(elapsedHours * 100) / 100,
        cuHoursSoFar: Math.round(cuHoursSoFar * 100) / 100,
        projectedCUHoursFullPeriod: Math.round(projectedCUHoursFullPeriod * 100) / 100,
        // Kung bago pa lang ang datos (konting oras pa lang), mahina ang
        // batayan ng projection — ipinapaalam ito sa UI para malinaw na
        // magbabago pa ito habang tumatagal ang billing period.
        lowConfidence: elapsedHours < 24
    };
}
// ===================================================================
// Hatiin ang TOTAL na Neon cost ng Cloud Backup project sa bawat client
// (installationId), ayon sa ACTUAL na proporsyon ng ginamit ng bawat isa
// — HINDI pantay-pantay na hati (hal. total/10), dahil magkakaiba ang
// laki ng database at dalas ng backup ng bawat client:
//   - storage cost  -> hinahati ayon sa share ng size_bytes bawat client
//     (mas malaking naka-store na data = mas malaking share ng storage cost)
//   - compute cost   -> hinahati ayon sa share ng sync_count bawat client
//     (mas madalas mag-backup/mag-poke sa admin panel = mas malaking share
//     ng compute cost, dahil bawat sync/health-check ay gumagamit ng
//     compute time)
// Ang kabuuan ng lahat ng client na baseCostPHP ay dapat === sa totoong
// Neon cost (walang extra, walang kulang) — sinisiguro ito sa pamamagitan
// ng pag-assign ng anumang centavo na natitira (rounding remainder) sa
// client na may pinakamalaking share, imbes na basta i-drop.
// Kada client, dinadagdag pa ang maintenanceFeePHP (flat, HINDI Neon
// consumption — bayad para sa monitoring/pagpapanatili) para sa
// finalPricePHP na ipapakita sa OMNIPOS admin panel ng client na iyon.
async function computeClientCostAllocation() {
    if (!pgPool) {
        return { success: false, message: 'Hindi pa naka-configure ang Postgres (DATABASE_URL) para sa Cloud Backup.' };
    }
    const [exchangeRate, neonUsage] = await Promise.all([
        getUsdToPhpRate(),
        getNeonProjectUsage(NEON_CLOUD_BACKUP_PROJECT_ID)
    ]);
    if (!neonUsage) {
        return {
            success: false,
            message: 'Walang Neon account API na naka-configure (NEON_API_KEY/NEON_CLOUD_BACKUP_PROJECT_ID) — hindi makukuha ang totoong consumption para hatiin sa mga client.'
        };
    }
    const realCost = computeNeonRealCost(neonUsage, neonConfiguredPlans.cloudBackup || 'free');
    const projection = projectFullPeriodCUHours(neonUsage);
    // Piliin ang batayan ng total cost na hahatiin: gamitin ang PROJECTED
    // full-period compute (mas makatotohanan bilang "monthly figure") kung
    // may sapat nang datos (hindi lowConfidence); kung wala, gamitin na
    // lang ang aktwal na cost-so-far ng elapsed period at ilagay ang
    // ⚠️ warning na "unstable, sample pa lang" — kaparehong pattern ng
    // /relay/admin/api/db-health.
    const tier = NEON_PRICING[normalizeNeonPlanId(neonUsage.planId) || (neonConfiguredPlans.cloudBackup || 'free')];
    let totalComputeCostUSD = realCost ? realCost.computeCostUSD : null;
    let totalStorageCostUSD = realCost ? realCost.storageCostUSD : null;
    const usingProjection = !!(projection && !projection.lowConfidence && tier && typeof tier.computeRatePerCUHourUSD === 'number');
    if (usingProjection) {
        totalComputeCostUSD = Math.round(projection.projectedCUHoursFullPeriod * tier.computeRatePerCUHourUSD * 100) / 100;
        // storage cost hindi na-project pa dahil kadalasan mas stable na
        // ito (hindi biglang tumataas/bumaba tulad ng compute), gagamitin
        // pa rin ang aktwal na kasalukuyang storage cost.
    }
    if (totalComputeCostUSD === null || totalStorageCostUSD === null) {
        return { success: false, message: 'Hindi makuha ang cost breakdown (baka hindi pa naka-detect/naka-configure ang tamang Neon pricing tier).' };
    }
    const rate = exchangeRate.rate || EXCHANGE_RATE_FALLBACK_USD_TO_PHP;
    const totalComputeCostPHP = totalComputeCostUSD * rate;
    const totalStorageCostPHP = totalStorageCostUSD * rate;
    const totalCostPHP = totalComputeCostPHP + totalStorageCostPHP;

    const { rows } = await queryWithRetry(
        pgPool,
        'SELECT installation_id, store_name, size_bytes, sync_count, total_records, last_sync_at, restore_count, last_restore_at FROM cloud_backup_meta ORDER BY size_bytes DESC',
        []
    );
    // BUGFIX: restore_count sa cloud_backup_meta ay LIFETIME cumulative
    // counter (hindi na-reset kailanman) — kaya kung ito lang ang basehan
    // ng "⚠️ flagged" na abuse indicator, isang lehitimong client na
    // 3 beses lang nag-restore sa loob ng ilang TAON (hal. disaster
    // recovery, bagong device kada matagal-tagal) ay MAGIGING PERMANENTENG
    // naka-flag magpakailanman, kahit malayo-layo ang mga pagkakataon at
    // hindi naman talaga abuse. Kinukuha rito ang bilang ng restore CHARGES
    // (RESTORE_CHARGE, trigger_type='manual' — galing sa awtomatikong
    // per-restore charge, hindi kasama ang manual admin charge) sa loob NG
    // HULING 30 ARAW LANG mula sa cloud_token_ledger (may created_at) —
    // rolling window, hindi lifetime — ito na ang tamang basehan ng "⚠️
    // flagged" sa client-cost-allocation.html. Ang restore_count column mula
    // sa cloud_backup_meta ay itinatago pa rin bilang "all-time total" para
    // sa konteksto, pero HINDI na ito ginagamit para sa pag-flag.
    // NOTE: trigger_type = 'manual' dito ay ang ginagamit ng
    // consumeCloudTokensForRestore() para sa AWTOMATIKONG per-restore
    // charge (ang totoong restore EVENT) — HINDI kapareho ng
    // trigger_type = 'manual_admin', na isang hiwalay/punitive na
    // deduction lang mula sa ⚡ Charge button (walang kaakibat na aktwal
    // na restore), kaya sinasadyang HINDI kasama rito.
    const restoreRecentResult = await queryWithRetry(
        pgPool,
        `SELECT installation_id, COUNT(*)::int AS recent_count
         FROM cloud_token_ledger
         WHERE category = 'RESTORE_CHARGE' AND trigger_type = 'manual' AND created_at > now() - interval '30 days'
         GROUP BY installation_id`,
        []
    );
    const restoreCountRecentByClient = new Map(restoreRecentResult.rows.map(r => [r.installation_id, Number(r.recent_count) || 0]));
    const totalSizeBytes = rows.reduce((sum, r) => sum + (Number(r.size_bytes) || 0), 0);
    const totalSyncCount = rows.reduce((sum, r) => sum + (Number(r.sync_count) || 0), 0);
    const n = rows.length;

    let clients = rows.map((r) => {
        const sizeBytes = Number(r.size_bytes) || 0;
        const syncCount = Number(r.sync_count) || 0;
        const storageShare = totalSizeBytes > 0 ? sizeBytes / totalSizeBytes : (n > 0 ? 1 / n : 0);
        const computeShare = totalSyncCount > 0 ? syncCount / totalSyncCount : (n > 0 ? 1 / n : 0);
        const storageCostPHP = totalStorageCostPHP * storageShare;
        const computeCostPHP = totalComputeCostPHP * computeShare;
        const baseCostPHP = storageCostPHP + computeCostPHP;
        // AYOS/BAGO: ang "maintenance fee" ngayon ay ang MONTHLY/YEARLY
        // presyo mismo ng Cloud Backup tier (basic/standard/pro) na
        // kinuha/sinubscribe ng client na ito (tingnan ang
        // getCloudBackupSubscriptionForClient() sa itaas) — HINDI na flat/
        // admin-configurable na halaga. Kung "active" pa ang subscription
        // niya ngayong billing period (o lifetime), HINDI muna idinadagdag
        // sa total ang fee (naibayad na ito nang direkta sa pamamagitan ng
        // Cloud Backup subscription flow mismo). Kung lapsed/wala pang
        // na-renew, ipinapakita pa rin ang huling kilalang plan (monthly/
        // yearly) bilang "dapat bayaran", gamit ang monthly rate nito para
        // sa idinadagdag sa finalPricePHP.
        const cbSubscription = getCloudBackupSubscriptionForClient(r.installation_id);
        const cbTierPrice = getCloudBackupTierPricePHP(cbSubscription.tier);
        const maintenanceFeeMonthlyPHP = cbTierPrice.monthly;
        const maintenanceFeeYearlyPHP = cbTierPrice.yearly;
        const maintenanceFeePaid = cbSubscription.active || cbSubscription.isLifetime;
        const maintenanceFeePHP = maintenanceFeePaid ? 0 : maintenanceFeeMonthlyPHP;
        return {
            installationId: r.installation_id,
            // AYOS: idinagdag ang label (mula sa Devices page, `deviceLabels`
            // — ito ang hiniling: makita rin sa Client Cost Allocation kung
            // ano ang naka-label sa Allowed Devices, hindi lang basta
            // installation ID) at storeName (ang self-reported na pangalan ng
            // tindahan mula mismo sa cloud_backup_meta) bilang fallback kapag
            // wala pang manual label na naka-set.
            label: deviceLabels.get(r.installation_id) || null,
            storeName: r.store_name || null,
            sizeBytes,
            sizeMB: Math.round((sizeBytes / (1024 * 1024)) * 100) / 100,
            syncCount,
            totalRecords: r.total_records,
            lastSyncAt: r.last_sync_at,
            // AYOS/BAGO: tracking-only fields (hindi kasama sa charging
            // computation sa itaas) — para makita kaagad kung sinong client
            // ang sobrang dalas mag-restore (posibleng abuser) bago
            // magdesisyon kung mag-charge pa/hihigpitan pa.
            restoreCount: Number(r.restore_count) || 0,
            // BUGFIX: idinagdag ang rolling 30-day count — ITO na ang
            // dapat gamitin ng UI para sa "⚠️ flagged" (tingnan ang
            // paliwanag sa itaas, malapit sa restoreRecentResult query).
            // Ang restoreCount naman sa itaas ay nananatiling lifetime
            // total, para lang sa konteksto/reference.
            restoreCountRecent30d: restoreCountRecentByClient.get(r.installation_id) || 0,
            lastRestoreAt: r.last_restore_at || null,
            storageSharePercent: Math.round(storageShare * 10000) / 100,
            computeSharePercent: Math.round(computeShare * 10000) / 100,
            storageCostPHP,
            computeCostPHP,
            baseCostPHP,
            // Bagong tier-aware na fields (ito na ang ginagamit ng widget/
            // tier modal/allocation admin — tingnan ang paliwanag sa itaas).
            cloudBackupTier: cbSubscription.tier,
            cloudBackupTierName: cbTierPrice.name,
            cloudBackupBillingCycle: cbSubscription.billingCycle,
            cloudBackupIsLifetime: cbSubscription.isLifetime,
            maintenanceFeePHP,
            maintenanceFeeMonthlyPHP,
            maintenanceFeeYearlyPHP,
            maintenanceFeePaid,
            maintenanceFeePaidUntil: cbSubscription.expiresAt,
            finalPricePHP: baseCostPHP + maintenanceFeePHP
        };
    });
    // I-round PAGKATAPOS mag-compute (hindi bago), at ibigay ang anumang
    // centavo na "nawala" sa rounding papunta sa client na may
    // pinakamalaking baseCostPHP, para ang SUM ng lahat ng naka-round na
    // baseCostPHP ay TAMANG-TAMA na katumbas ng naka-round na totalCostPHP
    // — walang sobra, walang kulang.
    const roundedTotal = Math.round(totalCostPHP * 100) / 100;
    clients = clients.map(c => ({ ...c, baseCostPHP: Math.round(c.baseCostPHP * 100) / 100, storageCostPHP: Math.round(c.storageCostPHP * 100) / 100, computeCostPHP: Math.round(c.computeCostPHP * 100) / 100 }));
    if (clients.length > 0) {
        const sumRounded = clients.reduce((s, c) => s + c.baseCostPHP, 0);
        const remainder = Math.round((roundedTotal - sumRounded) * 100) / 100;
        if (Math.abs(remainder) >= 0.01) {
            const biggest = clients.reduce((a, b) => (b.baseCostPHP > a.baseCostPHP ? b : a), clients[0]);
            biggest.baseCostPHP = Math.round((biggest.baseCostPHP + remainder) * 100) / 100;
        }
    }
    clients = clients.map(c => ({ ...c, finalPricePHP: Math.round((c.baseCostPHP + c.maintenanceFeePHP) * 100) / 100 }));

    return {
        success: true,
        checkedAt: Date.now(),
        exchangeRate: { usdToPhp: rate, source: exchangeRate.source, fetchedAt: exchangeRate.fetchedAt },
        costBasis: usingProjection ? 'projected-full-period' : 'elapsed-period-actual',
        warning: usingProjection
            ? null
            : 'Preliminary data — this billing period started less than 24 hours ago, so these figures are still an early sample and may shift before the monthly average stabilizes. Please don\'t treat this as the final basis for pricing yet.',
        totalComputeCostPHP: Math.round(totalComputeCostPHP * 100) / 100,
        totalStorageCostPHP: Math.round(totalStorageCostPHP * 100) / 100,
        totalCostPHP: roundedTotal,
        clientCount: clients.length,
        clients: clients.sort((a, b) => b.finalPricePHP - a.finalPricePHP)
    };
}
const FEATURE_CATALOG_OVERRIDES_PATH = path.join(__dirname, 'feature-catalog-overrides.json');
async function loadFeatureCatalogOverrides() {
    const fromStore = await getPersistentJSON('feature-catalog-overrides', null);
    if (fromStore !== null) return fromStore;
    try {
        return JSON.parse(fs.readFileSync(FEATURE_CATALOG_OVERRIDES_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveFeatureCatalogOverrides(obj) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('feature-catalog-overrides', obj);
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
    const fromStore = await getPersistentJSON('feature-pricing-overrides', null);
    if (fromStore !== null) return fromStore;
    try {
        return JSON.parse(fs.readFileSync(FEATURE_PRICING_OVERRIDES_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveFeaturePricingOverrides(obj) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('feature-pricing-overrides', obj);
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
    const fromStore = await getPersistentJSON('upgrade-tier-pricing-overrides', null);
    if (fromStore !== null) return fromStore;
    try {
        return JSON.parse(fs.readFileSync(UPGRADE_TIER_PRICING_OVERRIDES_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveUpgradeTierPricingOverrides(obj) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('upgrade-tier-pricing-overrides', obj);
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
    const fromStore = await getPersistentJSON('issued-unlocks', null);
    if (fromStore !== null) return fromStore;
    try {
        return JSON.parse(fs.readFileSync(ISSUED_UNLOCKS_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveIssuedUnlocks(obj) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('issued-unlocks', obj);
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
    const fromStore = await getPersistentJSON('activity-log', null);
    if (fromStore !== null) return fromStore;
    try {
        return JSON.parse(fs.readFileSync(ACTIVITY_LOG_PATH, 'utf8'));
    } catch (err) {
        return [];
    }
}
function saveActivityLog(arr) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('activity-log', arr);
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
    const fromStore = await getPersistentJSON('backup-checkins', null);
    if (fromStore !== null) return fromStore;
    try {
        return JSON.parse(fs.readFileSync(BACKUP_CHECKINS_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveBackupCheckins(obj) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('backup-checkins', obj);
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
    const fromStore = await getPersistentJSON('branch-summaries', null);
    if (fromStore !== null) return fromStore;
    try {
        return JSON.parse(fs.readFileSync(BRANCH_SUMMARIES_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveBranchSummaries(obj) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('branch-summaries', obj);
        return;
    }
    try {
        fs.writeFileSync(BRANCH_SUMMARIES_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Could not save branch-summaries.json:', err);
    }
}
let branchSummaries = {}; 
// FIX (cost-optimization): don't persist branch-summaries to Neon on every
// checkin if nothing actually changed in that installationId's summary.
// Still does a periodic safety flush every BRANCH_PERSIST_MIN_INTERVAL_MS.
const BRANCH_PERSIST_MIN_INTERVAL_MS = 15 * 60 * 1000; // 15 minutes
const branchLastPersisted = new Map(); // `${groupHash}|${installationId}` -> { signature, at }
function branchEntrySignature(branchName, summary) {
    return `${branchName}|${JSON.stringify(summary)}`;
}
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
// === Multi-Branch: hourly trend history (PRO) ===
const BRANCH_HISTORY_PATH = path.join(__dirname, 'branch-history.json');
async function loadBranchHistory() {
    const fromStore = await getPersistentJSON('branch-history', null);
    if (fromStore !== null) return fromStore;
    try {
        return JSON.parse(fs.readFileSync(BRANCH_HISTORY_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveBranchHistory(obj) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('branch-history', obj);
        return;
    }
    try {
        fs.writeFileSync(BRANCH_HISTORY_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Could not save branch-history.json:', err);
    }
}
let branchHistory = {};
const BRANCH_HISTORY_MAX_POINTS = 48; // ~2 days at 1x/hour resolution
const BRANCH_HISTORY_MIN_GAP_MS = 50 * 60 * 1000; // skip appending if a point was just recorded
function appendBranchHistoryPoint(groupKeyHash, installationId, summary) {
    const key = `${groupKeyHash}|${installationId}`;
    if (!Array.isArray(branchHistory[key])) branchHistory[key] = [];
    const arr = branchHistory[key];
    const last = arr[arr.length - 1];
    if (last && (Date.now() - last.ts) < BRANCH_HISTORY_MIN_GAP_MS) return;
    arr.push({ ts: Date.now(), ...summary });
    if (arr.length > BRANCH_HISTORY_MAX_POINTS) arr.splice(0, arr.length - BRANCH_HISTORY_MAX_POINTS);
    saveBranchHistory(branchHistory);
}
// === Multi-Branch: stock transfer requests (PRO) ===
const BRANCH_TRANSFERS_PATH = path.join(__dirname, 'branch-transfers.json');
async function loadBranchTransfers() {
    const fromStore = await getPersistentJSON('branch-transfers', null);
    if (fromStore !== null) return fromStore;
    try {
        return JSON.parse(fs.readFileSync(BRANCH_TRANSFERS_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveBranchTransfers(obj) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('branch-transfers', obj);
        return;
    }
    try {
        fs.writeFileSync(BRANCH_TRANSFERS_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Could not save branch-transfers.json:', err);
    }
}
let branchTransfers = {}; // groupKeyHash -> array of transfer objects
const BRANCH_TRANSFERS_MAX_PER_GROUP = 300;
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
    const fromBuildDb = await buildKvGetJSON('release-baselines', null);
    if (fromBuildDb !== null) return new Map(Object.entries(fromBuildDb));
    try {
        return new Map(Object.entries(JSON.parse(fs.readFileSync(RELEASE_BASELINES_PATH, 'utf8'))));
    } catch (err) {
        return new Map();
    }
}
async function saveReleaseBaselines(map) {
    const obj = Object.fromEntries(map);
    if (pgPoolBuild) {
        return await buildKvSetJSON('release-baselines', obj);
    }
    try {
        fs.writeFileSync(RELEASE_BASELINES_PATH, JSON.stringify(obj, null, 2));
        return true;
    } catch (err) {
        console.error('Hindi ma-save ang release-baselines.json:', err);
        return false;
    }
}
let releaseBaselines = new Map(); 
const INTEGRITY_STATUS_PATH = path.join(__dirname, 'integrity-status.json');
async function loadIntegrityStatus() {
    const fromBuildDb = await buildKvGetJSON('integrity-status', null);
    if (fromBuildDb !== null) return fromBuildDb;
    try {
        return JSON.parse(fs.readFileSync(INTEGRITY_STATUS_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
function saveIntegrityStatus(obj) {
    if (pgPoolBuild) {
        buildKvSetJSON('integrity-status', obj);
        return;
    }
    try {
        fs.writeFileSync(INTEGRITY_STATUS_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang integrity-status.json:', err);
    }
}
let integrityStatus = {}; 
// AYOS (cost-optimization): huwag i-persist sa Neon ang integrity-status kada
// checkin (up to 300x/oras kada device) — buong blob ng LAHAT ng devices kasi
// ang nire-rewrite ni saveIntegrityStatus(). Isulat lang sa Neon kapag
// talagang nagbago ang resulta (flagged/counts/watcher) o kapag lumipas na
// ang INTEGRITY_PERSIST_MIN_INTERVAL_MS mula huling save (periodic safety
// flush). Ang in-memory `integrityStatus` ay laging updated agad — walang
// epekto sa live API responses, apektado lang ang dalas ng Neon writes.
const INTEGRITY_PERSIST_MIN_INTERVAL_MS = 15 * 60 * 1000; // 15 minuto
const integrityLastPersisted = new Map(); // installationId -> { signature, at }
function integrityEntrySignature(entry) {
    if (!entry) return '';
    return [
        entry.hasBaseline, entry.flagged, entry.baselineVersion,
        entry.modifiedCount, entry.deletedCount, entry.addedCount,
        entry.watcherActive, entry.clearedAt, entry.clearedNote
    ].join('|');
}
function maybePersistIntegrityStatus(installationId, entry) {
    const sig = integrityEntrySignature(entry);
    const prev = integrityLastPersisted.get(installationId);
    const now = Date.now();
    const changed = !prev || prev.signature !== sig;
    const dueForFlush = !prev || (now - prev.at) >= INTEGRITY_PERSIST_MIN_INTERVAL_MS;
    if (changed || dueForFlush) {
        saveIntegrityStatus(integrityStatus);
        integrityLastPersisted.set(installationId, { signature: sig, at: now });
    }
}
const pendingIntegrityChecks = new Set();
const SYSTEM_VERSION_PATH = path.join(__dirname, 'system-version.json');
const DEFAULT_SYSTEM_VERSION_INFO = { version: '0.0.0', changelog: '', publishedAt: null };
async function loadSystemVersionInfo() {
    const fromBuildDb = await buildKvGetJSON('system-version', null);
    if (fromBuildDb !== null) return fromBuildDb;
    try {
        return JSON.parse(fs.readFileSync(SYSTEM_VERSION_PATH, 'utf8'));
    } catch (err) {
        return { ...DEFAULT_SYSTEM_VERSION_INFO };
    }
}
function saveSystemVersionInfo(obj) {
    if (pgPoolBuild) {
        buildKvSetJSON('system-version', obj);
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
    const fromBuildDb = await buildKvGetJSON('targeted-releases', null);
    if (fromBuildDb !== null) return new Map(Object.entries(fromBuildDb));
    try {
        const raw = fs.readFileSync(TARGETED_RELEASES_PATH, 'utf8');
        return new Map(Object.entries(JSON.parse(raw)));
    } catch (err) {
        return new Map();
    }
}
async function saveTargetedReleases(map) {
    if (pgPoolBuild) {
        return await buildKvSetJSON('targeted-releases', Object.fromEntries(map));
    }
    try {
        fs.writeFileSync(TARGETED_RELEASES_PATH, JSON.stringify(Object.fromEntries(map), null, 2));
        return true;
    } catch (err) {
        console.error('Hindi ma-save ang targeted-releases.json:', err);
        return false;
    }
}
let targetedReleases = new Map(); 
const DOWNLOAD_CODES_PATH = path.join(__dirname, 'download-codes.json');
async function loadDownloadCodes() {
    const fromBuildDb = await buildKvGetJSON('download-codes', null);
    if (fromBuildDb !== null) return new Map(Object.entries(fromBuildDb));
    try {
        const raw = fs.readFileSync(DOWNLOAD_CODES_PATH, 'utf8');
        return new Map(Object.entries(JSON.parse(raw)));
    } catch (err) {
        return new Map();
    }
}
async function saveDownloadCodes(map) {
    if (pgPoolBuild) {
        return await buildKvSetJSON('download-codes', Object.fromEntries(map));
    }
    try {
        fs.writeFileSync(DOWNLOAD_CODES_PATH, JSON.stringify(Object.fromEntries(map), null, 2));
        return true;
    } catch (err) {
        console.error('Hindi ma-save ang download-codes.json:', err);
        return false;
    }
}
let downloadCodes = new Map(); 
const RELEASE_PACKAGE_PATH = path.join(__dirname, 'release', 'omnipos-client.zip');
const RELEASE_PACKAGE_TMP_PATH = path.join(__dirname, 'release', '.omnipos-client.zip.building');
const BUILD_HISTORY_PATH = path.join(__dirname, 'build-history.json');
const BUILD_HISTORY_MAX_ENTRIES = 100;
async function loadBuildHistory() {
    const fromBuildDb = await buildKvGetJSON('build-history', null);
    if (fromBuildDb !== null) return Array.isArray(fromBuildDb) ? fromBuildDb : [];
    try {
        const raw = fs.readFileSync(BUILD_HISTORY_PATH, 'utf8');
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed : [];
    } catch (err) {
        return [];
    }
}
async function saveBuildHistory(list) {
    if (pgPoolBuild) {
        return await buildKvSetJSON('build-history', list);
    }
    try {
        fs.writeFileSync(BUILD_HISTORY_PATH, JSON.stringify(list, null, 2));
        return true;
    } catch (err) {
        console.error('Hindi ma-save ang build-history.json:', err);
        return false;
    }
}
async function recordBuildHistoryEntry(entry) {
    buildHistory.unshift(entry); 
    if (buildHistory.length > BUILD_HISTORY_MAX_ENTRIES) {
        buildHistory.length = BUILD_HISTORY_MAX_ENTRIES;
    }
    return await saveBuildHistory(buildHistory);
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
// BAGO: dating kailangan pang buksan isa-isa ang device detail para
// makita kung sino ang malapit nang mag-expire sa Cloud Backup, RBAC, o
// Multi-Branch subscriptions. Isang endpoint na ito na nagsasama-sama ng
// LAHAT ng active na subscription sa 3 feature na ito (isSubscriptionOnlyFeature),
// sorted soonest-expiring-first, para agad makita sa isang tingin.
app.get('/relay/admin/api/subscriptions', requireAdminKey, (req, res) => {
    const now = Date.now();
    const SOON_MS = 7 * 24 * 60 * 60 * 1000;
    const rows = [];
    for (const [installationId, record] of Object.entries(issuedUnlocks)) {
        // Kaparehong convention ng /relay/admin/api/analytics: "active"
        // subscription lang ang binibilang kung naka-Allow pa rin ang
        // device (hindi lang basta hindi pa expired ang token).
        if (!allowedDevices.has(installationId)) continue;
        for (const [featureId, entry] of Object.entries(record)) {
            if (!isSubscriptionOnlyFeature(featureId)) continue;
            const isExpired = typeof entry.expiresAt === 'number' && now > entry.expiresAt;
            // RBAC/Multi-Branch subscriptions get a 7-day grace period after
            // expiresAt (see MODULE_SUBSCRIPTION_GRACE_PERIOD_MS, ginagamit din
            // ito sa OMNIPOS client para malaman kung "still active but about
            // to lose access"). Cloud Backup has no such grace period.
            // I-uuwi pa rin dito ang mga naka-grace-period (may
            // inGracePeriod: true) para makita ng admin — hindi lang basta
            // itapon tulad ng mga tunay nang expired/walang grace period.
            const inGracePeriod = isExpired
                && isModuleSubscriptionFeature(featureId)
                && (now - entry.expiresAt) <= MODULE_SUBSCRIPTION_GRACE_PERIOD_MS;
            if (isExpired && !inGracePeriod) continue;
            const meta = seenDevices.get(installationId);
            const daysLeft = typeof entry.expiresAt === 'number'
                ? Math.ceil((entry.expiresAt - now) / (24 * 60 * 60 * 1000))
                : null; // null = walang expiry (hal. legacy lifetime Cloud Backup)
            rows.push({
                installationId,
                label: deviceLabels.get(installationId) || (meta && meta.storeName) || null,
                featureId,
                featureName: entry.featureName || (FEATURE_CATALOG[featureId] && FEATURE_CATALOG[featureId].name) || featureId,
                tier: entry.tier || null,
                billingCycle: entry.billingCycle || null,
                issuedAt: entry.issuedAt,
                expiresAt: entry.expiresAt,
                isLifetime: entry.expiresAt === null,
                inGracePeriod,
                daysLeft,
                expiringSoon: typeof daysLeft === 'number' && daysLeft <= SOON_MS / (24 * 60 * 60 * 1000)
            });
        }
    }
    // Soonest-expiring muna; ang mga walang expiry (lifetime) ay nasa
    // dulo dahil wala namang dapat asikasuhing renewal doon.
    rows.sort((a, b) => {
        if (a.daysLeft === null && b.daysLeft === null) return 0;
        if (a.daysLeft === null) return 1;
        if (b.daysLeft === null) return -1;
        return a.daysLeft - b.daysLeft;
    });
    const countsByFeature = {};
    for (const row of rows) {
        countsByFeature[row.featureId] = (countsByFeature[row.featureId] || 0) + 1;
    }
    res.json({
        success: true,
        subscriptions: rows,
        countsByFeature,
        expiringSoonCount: rows.filter(r => r.expiringSoon).length
    });
});
app.post('/relay/admin/api/devices/allow', requireAdminKey, (req, res) => {
    const { installationId } = req.body;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
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
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
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
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
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
        purgeCloneSplitsForInstallation(id);
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
// Tinatanggal ang LAHAT ng clone-split record ng isang device — kapwa ang
// mga rekord kung saan ito ang ORIHINAL (split_key na "installationId::fp")
// AT ang mga rekord kung saan ito pala ang RESULTA ng isang split (ibig
// sabihin, ito ang naka-set bilang new_installation_id ng ibang device).
// Composite key ang `cloneSplits` Map ("installationId::fingerprint"), kaya
// hindi tama/sapat ang simpleng `cloneSplits.delete(installationId)` (ito
// ang dating bug sa /bulk-reset sa itaas — hindi na-clear ang tunay na
// clone-split record dahil hindi tumutugma ang key).
function purgeCloneSplitsForInstallation(installationId) {
    let removed = 0;
    const prefix = `${installationId}::`;
    for (const [key, rec] of [...cloneSplits.entries()]) {
        if (key === installationId || key.startsWith(prefix) || rec.newInstallationId === installationId) {
            cloneSplits.delete(key);
            removed++;
        }
    }
    if (removed > 0) saveCloneSplits(cloneSplits);
    return removed;
}
// TULUYANG PAGBURA NG DEVICE — ginagamit ito kapag, hal., na-uninstall at
// muling na-install ang OMNIPOS sa parehong pisikal na device kaya nagkaroon
// ito ng BAGONG installation ID, at gusto nang tuluyang alisin ang LUMANG
// installation ID sa lahat ng lugar: allow-list, label, fingerprint binding,
// clone-split history, issued unlocks (ala-carte/subscription), backup at
// integrity check-in history, per-client maintenance-fee override, online
// status, AT ang Cloud Backup data mismo (cloud_backup_modules/meta) sa Neon
// — kaya awtomatiko rin itong mawawala sa "client cost allocation" list
// dahil derived lang iyon mula sa cloud_backup_meta.
//
// SAFETY: kailangan ng ?confirm=DELETE (o "confirm":"DELETE" sa JSON body)
// dahil hindi na ito mababawi — hindi tulad ng revoke (na puwede pang i-allow
// ulit), permanenteng bura na ito ng lahat ng datos ng device.
app.post('/relay/admin/api/devices/:installationId/purge', requireAdminKey, async (req, res) => {
    const { installationId } = req.params;
    const confirm = req.query.confirm || req.body?.confirm;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
    }
    if (confirm !== 'DELETE') {
        return res.status(400).json({
            success: false,
            message: 'Safety check: kailangan ng ?confirm=DELETE (o "confirm":"DELETE" sa JSON body) para tuluyang burahin ang device na ito. Hindi na ito mababawi — kabilang na ang Cloud Backup data nito sa Neon.'
        });
    }
    const summary = { installationId };
    try {
        summary.wasAllowed = allowedDevices.has(installationId);
        allowedDevices.delete(installationId);
        saveAllowedDevices(allowedDevices);

        summary.hadLabel = deviceLabels.has(installationId);
        deviceLabels.delete(installationId);
        saveDeviceLabels(deviceLabels);

        summary.hadFingerprint = deviceFingerprints.has(installationId);
        deviceFingerprints.delete(installationId);
        saveDeviceFingerprints(deviceFingerprints);

        summary.removedCloneSplits = purgeCloneSplitsForInstallation(installationId);

        seenDevices.delete(installationId);

        let removedPendingOtps = 0;
        for (const key of [...pendingOtps.keys()]) {
            if (key.startsWith(`${installationId}:`)) {
                pendingOtps.delete(key);
                removedPendingOtps++;
            }
        }
        summary.removedPendingOtps = removedPendingOtps;

        summary.hadIssuedUnlocks = !!issuedUnlocks[installationId];
        if (summary.hadIssuedUnlocks) {
            delete issuedUnlocks[installationId];
            saveIssuedUnlocks(issuedUnlocks);
        }

        summary.hadBackupCheckin = !!backupCheckins[installationId];
        if (summary.hadBackupCheckin) {
            delete backupCheckins[installationId];
            saveBackupCheckins(backupCheckins);
        }

        summary.hadIntegrityStatus = !!integrityStatus[installationId];
        if (summary.hadIntegrityStatus) {
            delete integrityStatus[installationId];
            saveIntegrityStatus(integrityStatus);
        }

        const hasFeeOverride = !!(clientMaintenanceFeeConfig.perClientOverridePHP &&
            clientMaintenanceFeeConfig.perClientOverridePHP[installationId] !== undefined);
        summary.hadMaintenanceFeeOverride = hasFeeOverride;
        if (hasFeeOverride) {
            delete clientMaintenanceFeeConfig.perClientOverridePHP[installationId];
            saveClientMaintenanceFeeConfig(clientMaintenanceFeeConfig);
        }

        if (redisClient) {
            try {
                await redisClient.del(ONLINE_KEY_PREFIX + installationId);
            } catch (err) {
                console.error('⚠️  Hindi na-clear ang online heartbeat key (purge):', err.message);
            }
        }

        if (pgPool) {
            try {
                await runPgWriteTx(pgPool, async (client) => {
                    const modulesResult = await client.query('DELETE FROM cloud_backup_modules WHERE installation_id = $1', [installationId]);
                    const metaResult = await client.query('DELETE FROM cloud_backup_meta WHERE installation_id = $1', [installationId]);
                    summary.deletedCloudBackupModuleRows = modulesResult.rowCount;
                    summary.deletedCloudBackupMetaRows = metaResult.rowCount;
                });
            } catch (err) {
                console.error('⚠️  Hindi na-delete ang Cloud Backup data ng device (purge):', err.message);
                summary.cloudBackupDeleteError = err.message;
            }
        } else {
            summary.cloudBackupDeleteError = 'Hindi naka-configure ang Postgres (DATABASE_URL) para sa Cloud Backup.';
        }

        // AYOS: si saveAllowedDevices()/saveDeviceLabels() ay hindi talaga
        // nagde-DELETE ng row sa `relay_devices` — nagse-set lamang ito ng
        // allowed=false at label=NULL (soft update, dahil "buong listahan"
        // ang isinusulat nila kada save, hindi single-row delete). Kaya kahit
        // walang epekto ang naiiwan na row (hindi ito lalabas kahit saan
        // dahil laging naka-filter ang mga query sa allowed=true/label IS NOT
        // NULL), tuluyan pa rin nating tatanggalin dito ang buong row para
        // talagang wala nang bakas ang lumang installation ID sa database.
        if (pgPoolDevices) {
            try {
                const devicesResult = await queryWithRetry(pgPoolDevices, 'DELETE FROM relay_devices WHERE installation_id = $1', [installationId]);
                summary.deletedRelayDevicesRows = devicesResult.rowCount;
            } catch (err) {
                console.error('⚠️  Hindi na-delete ang relay_devices row (purge):', err.message);
                summary.relayDevicesDeleteError = err.message;
            }
        }

        logActivity(installationId, 'device_purged', summary);
        res.json({
            success: true,
            message: `Tuluyang binura ang device ${installationId}: allow-list, label, fingerprint binding, clone-split record, issued unlocks, backup/integrity history, maintenance-fee override, online status, at Cloud Backup data (Neon). Awtomatiko rin itong mawawala sa client cost allocation list.`,
            summary
        });
    } catch (err) {
        console.error('⚠️  /relay/admin/api/devices/:installationId/purge error:', err.message);
        res.status(500).json({ success: false, message: 'Hindi na-buo ang pag-purge ng device — posibleng may parte itong tapos na (tingnan ang summary sa logs).', summary });
    }
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
        // AYOS: kasama na rin dito ang maintenance/monitoring fee (default),
        // dahil kahit na nakatago pa rin ito sa clientMaintenanceFeeConfig
        // (client-maintenance-fee.json — hindi ito ginalaw para hindi masira
        // ang existing per-client overrides), dito na ito sa Cloud Backup
        // Pricing page (pricing.html) mismo ie-edit ng admin, hindi na sa
        // Client Cost Allocation page — doon na lang mananatili ang
        // per-client override list, pero ang DEFAULT ay dito na sa pricing.
        defaultMaintenanceFeePHP: clientMaintenanceFeeConfig.defaultFeePHP || 0,
        billingDays: CLOUD_BACKUP_BILLING_DAYS,
        moduleSubscriptionPlans: MODULE_SUBSCRIPTION_PLANS,
        moduleSubscriptionPlansBase: MODULE_SUBSCRIPTION_PLANS_BASE,
        moduleSubscriptionOverrides,
        moduleSubscriptionBillingDays: MODULE_SUBSCRIPTION_BILLING_DAYS,
        moduleSubscriptionGracePeriodDays: MODULE_SUBSCRIPTION_GRACE_PERIOD_DAYS
    });
});
app.post('/relay/admin/api/pricing/cloud-backup', requireAdminKey, (req, res) => {
    const { tier, name, monthly, yearly, storageQuotaMB, autoBackupIntervalHours } = req.body || {};
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
    let autoBackupIntervalMs;
    if (autoBackupIntervalHours !== undefined) {
        const hours = Number(autoBackupIntervalHours);
        if (!isFinite(hours) || hours <= 0) {
            return res.status(400).json({ success: false, message: 'Invalid autoBackupIntervalHours.' });
        }
        autoBackupIntervalMs = Math.round(hours * 60 * 60 * 1000);
        if (autoBackupIntervalMs < CLOUD_BACKUP_MIN_AUTO_BACKUP_INTERVAL_MS || autoBackupIntervalMs > CLOUD_BACKUP_MAX_AUTO_BACKUP_INTERVAL_MS) {
            return res.status(400).json({
                success: false,
                message: `Ang autoBackupIntervalHours ay dapat nasa pagitan ng ${CLOUD_BACKUP_MIN_AUTO_BACKUP_INTERVAL_MS / (60 * 60 * 1000)} oras at ${CLOUD_BACKUP_MAX_AUTO_BACKUP_INTERVAL_MS / (60 * 60 * 1000)} oras (30 araw).`
            });
        }
    }
    const existing = cloudBackupPlanOverrides[tier] || {};
    const updated = { ...existing };
    if (typeof name === 'string' && name.trim()) updated.name = name.trim();
    if (typeof storageQuotaMB === 'number') updated.storageQuotaMB = storageQuotaMB;
    if (typeof autoBackupIntervalMs === 'number') updated.autoBackupIntervalMs = autoBackupIntervalMs;
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
        return res.status(400).json({ success: false, message: 'Invalid or missing featureId (rbac_management/multi_branch/ai_assistant).' });
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
        return res.status(400).json({ success: false, message: 'Invalid or missing featureId (rbac_management/multi_branch/ai_assistant).' });
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
        return res.status(400).json({ success: false, message: 'Invalid or unknown featureId (or it is a subscription feature — cloud_backup/rbac_management/multi_branch/ai_assistant — which has its own dedicated pricing editor).' });
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
    const fromStore = await getPersistentJSON('suggested-discount-percent', null);
    if (fromStore !== null && typeof fromStore === 'number') return fromStore;
    try {
        const parsed = JSON.parse(fs.readFileSync(SUGGESTED_DISCOUNT_PERCENT_PATH, 'utf8'));
        if (typeof parsed === 'number') return parsed;
    } catch (err) {   }
    return 30;
}
function saveSuggestedDiscountPercent(value) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('suggested-discount-percent', value);
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
// BAGO: pang-emergency na "kill switch" para sa developer — kung
// hindi available ang developer para mag-manual approve ng "Send
// Request" (OTP) unlock requests, pwede muna itong i-disable
// pansamantala (mananatiling bukas ang "Activate via Omni Tokens" kung
// gusto). Kabaligtaran naman kung may maintenance/problema sa Omni
// Token activation (Postgres wallet, etc.) — pwede iyon namang i-disable
// at "Send Request" na lang muna ang bukas. Pareho itong naka-toggle
// mula sa Home tab ng admin dashboard, at pareho ring pinapatupad dito
// mismo sa RELAY (hindi lang sa OMNIPOS client) para tiyak na hindi
// ito ma-bypass kahit anong gawin sa client.
let ACTIVATION_FLAGS = { otpRequestsEnabled: true, omniTokenActivationEnabled: true };
const ACTIVATION_FLAGS_PATH = path.join(__dirname, 'activation-flags.json');
async function loadActivationFlags() {
    const fromStore = await getPersistentJSON('activation-flags', null);
    if (fromStore && typeof fromStore === 'object') {
        return {
            otpRequestsEnabled: fromStore.otpRequestsEnabled !== false,
            omniTokenActivationEnabled: fromStore.omniTokenActivationEnabled !== false
        };
    }
    try {
        const parsed = JSON.parse(fs.readFileSync(ACTIVATION_FLAGS_PATH, 'utf8'));
        if (parsed && typeof parsed === 'object') {
            return {
                otpRequestsEnabled: parsed.otpRequestsEnabled !== false,
                omniTokenActivationEnabled: parsed.omniTokenActivationEnabled !== false
            };
        }
    } catch (err) {   }
    return { otpRequestsEnabled: true, omniTokenActivationEnabled: true };
}
function saveActivationFlags(value) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('activation-flags', value);
        return;
    }
    try {
        fs.writeFileSync(ACTIVATION_FLAGS_PATH, JSON.stringify(value));
    } catch (err) {
        console.error('Hindi ma-save ang activation-flags.json:', err);
    }
}
app.get('/relay/admin/api/activation-flags', requireAdminKey, (req, res) => {
    res.json({ success: true, activationFlags: ACTIVATION_FLAGS });
});
app.post('/relay/admin/api/activation-flags', requireAdminKey, (req, res) => {
    const { otpRequestsEnabled, omniTokenActivationEnabled } = req.body || {};
    if (otpRequestsEnabled !== undefined && typeof otpRequestsEnabled !== 'boolean') {
        return res.status(400).json({ success: false, message: 'otpRequestsEnabled must be true or false.' });
    }
    if (omniTokenActivationEnabled !== undefined && typeof omniTokenActivationEnabled !== 'boolean') {
        return res.status(400).json({ success: false, message: 'omniTokenActivationEnabled must be true or false.' });
    }
    if (typeof otpRequestsEnabled === 'boolean') ACTIVATION_FLAGS.otpRequestsEnabled = otpRequestsEnabled;
    if (typeof omniTokenActivationEnabled === 'boolean') ACTIVATION_FLAGS.omniTokenActivationEnabled = omniTokenActivationEnabled;
    saveActivationFlags(ACTIVATION_FLAGS);
    console.log(`⚙️  Na-update ang activation flags via admin panel: otpRequestsEnabled=${ACTIVATION_FLAGS.otpRequestsEnabled}, omniTokenActivationEnabled=${ACTIVATION_FLAGS.omniTokenActivationEnabled}.`);
    res.json({ success: true, activationFlags: ACTIVATION_FLAGS });
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
// ===================================================================
// BAGO: "Installation Data" admin page — isang listahan ng LAHAT ng
// installation (per store) na may buod ng lahat ng nakasave nilang
// datos: ilang records/modules, gaano kalaki (bytes), kailan huling
// nag-sync, existing pa ba sa allowed device list, at fingerprint/
// clone-flag status. Ginagamit ang parehong in-memory state
// (allowedDevices/deviceLabels/deviceFingerprints/seenDevices) na
// ginagamit na rin ng /relay/admin/api/devices, dagdag na lang ang
// SQL query sa cloud_backup_meta (Neon-only, walang in-memory cache
// nito) para sa mga column na size_bytes/last_sync_at/atbp.
app.get('/relay/admin/api/installations', requireAdminKey, async (req, res) => {
    try {
        let metaByInstallation = new Map();
        if (pgPool) {
            const { rows } = await queryWithRetry(
                pgPool,
                `SELECT installation_id, store_name, total_records, module_count, size_bytes,
                        last_sync_at, sync_count, restore_count, last_restore_at
                 FROM cloud_backup_meta`,
                []
            );
            metaByInstallation = new Map(rows.map(r => [r.installation_id, r]));
        }
        // Kunin ang UNION ng lahat ng kilalang installation ID mula sa bawat
        // pinagmumulan — hindi lahat ng device ay may Cloud Backup data pa
        // (hal. hindi pa naka-subscribe), at hindi lahat ng may Cloud Backup
        // row ay kasalukuyang naka-seen/naka-allow (hal. na-uninstall na
        // ang app pero naiwan pa ang datos sa Neon) — dapat pareho itong
        // makita dito, kasi parehong may "data na pwedeng ikalugi" kapag
        // hindi na-manage.
        const allIds = new Set([
            ...metaByInstallation.keys(),
            ...seenDevices.keys(),
            ...allowedDevices
        ]);
        const ids = [...allIds];
        const onlineMap = await getOnlineStatusMap(ids);
        const installations = ids.map((installationId) => {
            const meta = metaByInstallation.get(installationId) || null;
            const seen = seenDevices.get(installationId) || null;
            const fingerprintRecord = deviceFingerprints.get(installationId) || null;
            return {
                installationId,
                storeName: (meta && meta.store_name) || (seen && seen.storeName) || null,
                label: deviceLabels.get(installationId) || null,
                allowed: allowedDevices.has(installationId),
                online: !!onlineMap[installationId],
                lastSeenAt: seen ? seen.lastSeenAt : null,
                // === Cloud Backup data footprint (Neon) ===
                hasCloudBackupData: !!meta,
                totalRecords: meta ? Number(meta.total_records) || 0 : 0,
                moduleCount: meta ? Number(meta.module_count) || 0 : 0,
                sizeBytes: meta ? Number(meta.size_bytes) || 0 : 0,
                lastSyncAt: meta ? meta.last_sync_at : null,
                syncCount: meta ? Number(meta.sync_count) || 0 : 0,
                restoreCount: meta ? Number(meta.restore_count) || 0 : 0,
                lastRestoreAt: meta ? meta.last_restore_at : null,
                // === Device/clone integrity ===
                fingerprintVerified: !!(fingerprintRecord && fingerprintRecord.fingerprint),
                fingerprintFlagged: !!(fingerprintRecord && fingerprintRecord.flagged),
                fingerprintVerifyCount: fingerprintRecord ? fingerprintRecord.verifyCount : 0,
                lastVerifiedAt: fingerprintRecord ? fingerprintRecord.lastVerifiedAt : null
            };
        }).sort((a, b) => (b.sizeBytes || 0) - (a.sizeBytes || 0));
        res.json({
            success: true,
            checkedAt: Date.now(),
            cloudBackupConfigured: !!pgPool,
            count: installations.length,
            installations
        });
    } catch (err) {
        console.error('⚠️  /relay/admin/api/installations error:', err.message);
        res.status(500).json({ success: false, message: 'Could not load the installations list.' });
    }
});
// BAGO: i-export ang buong Cloud Backup data (lahat ng module, hilaw
// na JSONB) ng IISANG installation bilang isang downloadable JSON file
// — GAMIT ANG PAREHONG SHAPE ({ meta, modules, redactedFieldsByModule })
// na ibinabalik ng /relay/cloud-backup/restore sa OMNIPOS client mismo,
// para kung sakaling kailanganin pa ito, direktang magagamit/mai-restore
// ito pabalik sa parehong format na kilala na ng sistema. Walang token
// charge dito (admin-initiated na export/manual backup, hindi client
// restore), at hindi ito naka-rate-limit tulad ng client-facing restore
// dahil ang tumatawag dito ay ang developer/admin lang (requireAdminKey).
app.get('/relay/admin/api/installations/:installationId/export', requireAdminKey, async (req, res) => {
    const { installationId } = req.params;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
    }
    if (!pgPool) {
        return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured — walang Cloud Backup data na makukuha.' });
    }
    try {
        const metaResult = await queryWithRetry(pgPool, 'SELECT * FROM cloud_backup_meta WHERE installation_id = $1', [installationId]);
        if (!metaResult.rows[0]) {
            return res.status(404).json({ success: false, message: 'Walang Cloud Backup data na nakita para sa installation na ito sa Neon.' });
        }
        const modulesResult = await queryWithRetry(pgPool, 'SELECT module, data, record_count, size_bytes, updated_at FROM cloud_backup_modules WHERE installation_id = $1', [installationId]);
        const modules = {};
        modulesResult.rows.forEach((r) => { modules[r.module] = r.data; });
        const exportBody = {
            exportedAt: new Date().toISOString(),
            exportedBy: 'relay-admin-installations-page',
            installationId,
            meta: {
                storeName: metaResult.rows[0].store_name,
                totalRecords: metaResult.rows[0].total_records,
                moduleCount: metaResult.rows[0].module_count,
                sizeBytes: Number(metaResult.rows[0].size_bytes) || 0,
                lastSyncAt: metaResult.rows[0].last_sync_at,
                syncCount: metaResult.rows[0].sync_count,
                restoreCount: metaResult.rows[0].restore_count,
                lastRestoreAt: metaResult.rows[0].last_restore_at
            },
            modules,
            redactedFieldsByModule: CLOUD_BACKUP_REDACTED_FIELDS_BY_MODULE
        };
        logActivity(installationId, 'admin_data_exported', {
            moduleCount: modulesResult.rows.length,
            sizeBytes: Number(metaResult.rows[0].size_bytes) || 0
        });
        const fileSafeId = String(installationId).replace(/[^a-zA-Z0-9_-]/g, '_');
        res.set('Content-Type', 'application/json; charset=utf-8');
        res.set('Content-Disposition', `attachment; filename="relay-backup-${fileSafeId}-${Date.now()}.json"`);
        res.send(JSON.stringify(exportBody, null, 2));
    } catch (err) {
        console.error('⚠️  /relay/admin/api/installations/:installationId/export error:', err.message);
        res.status(500).json({ success: false, message: 'May error habang ine-export ang data: ' + err.message });
    }
});
// BAGO: i-export ang Cloud Backup data ng MARAMING installation nang
// sabay-sabay bilang ISANG JSON file (array ng bawat installation's
// export object) — para sa "select multiple, backup all" flow sa
// installations.html, iisa lang ang na-do-download na file (hindi
// paulit-ulit na browser download prompt kada row).
app.post('/relay/admin/api/installations/bulk-export', requireAdminKey, async (req, res) => {
    const { installationIds } = req.body || {};
    if (!Array.isArray(installationIds) || installationIds.length === 0) {
        return res.status(400).json({ success: false, message: 'Kulang o walang laman ang installationIds array.' });
    }
    if (!pgPool) {
        return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured.' });
    }
    try {
        const exports = [];
        const skipped = [];
        for (const installationId of installationIds) {
            const metaResult = await queryWithRetry(pgPool, 'SELECT * FROM cloud_backup_meta WHERE installation_id = $1', [installationId]);
            if (!metaResult.rows[0]) { skipped.push(installationId); continue; }
            const modulesResult = await queryWithRetry(pgPool, 'SELECT module, data FROM cloud_backup_modules WHERE installation_id = $1', [installationId]);
            const modules = {};
            modulesResult.rows.forEach((r) => { modules[r.module] = r.data; });
            exports.push({
                installationId,
                meta: {
                    storeName: metaResult.rows[0].store_name,
                    totalRecords: metaResult.rows[0].total_records,
                    moduleCount: metaResult.rows[0].module_count,
                    sizeBytes: Number(metaResult.rows[0].size_bytes) || 0,
                    lastSyncAt: metaResult.rows[0].last_sync_at
                },
                modules,
                redactedFieldsByModule: CLOUD_BACKUP_REDACTED_FIELDS_BY_MODULE
            });
            logActivity(installationId, 'admin_data_exported', { via: 'bulk', moduleCount: modulesResult.rows.length });
        }
        res.set('Content-Type', 'application/json; charset=utf-8');
        res.set('Content-Disposition', `attachment; filename="relay-bulk-backup-${Date.now()}.json"`);
        res.send(JSON.stringify({ exportedAt: new Date().toISOString(), count: exports.length, skipped, exports }, null, 2));
    } catch (err) {
        console.error('⚠️  /relay/admin/api/installations/bulk-export error:', err.message);
        res.status(500).json({ success: false, message: 'May error habang ine-export ang bulk backup: ' + err.message });
    }
});
app.post('/relay/admin/api/devices/:installationId/clear-history', requireAdminKey, (req, res) => {
    const { installationId } = req.params;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
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
        // AYOS: parehong "paid" marking gaya ng sa OTP flow (tingnan
        // /relay/confirm-unlock) — kapag Cloud Backup ang manually
        // ina-activate ng admin dito, dapat din itong mag-mark na "paid"
        // ang maintenance fee hanggang sa expiry ng ibinigay na duration.
        if (isThisCloudBackup && token.payload.expiresAt) {
            markMaintenanceFeePaidUntil(installationId, token.payload.expiresAt);
        }
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
        backupVersion: 3, 
        exportedAt: Date.now(),
        // === Devices/License (dating v2 fields) ===
        allowedDevices: [...allowedDevices],
        deviceLabels: Object.fromEntries(deviceLabels),
        issuedUnlocks,
        activityLog,
        // === BAGO sa v3: dating Postgres-only o file-only, wala pang backup ===
        deviceFingerprints: Object.fromEntries(deviceFingerprints),
        cloneSplits: Object.fromEntries(cloneSplits),
        backupCheckins,
        branchSummaries,
        // Build/Push
        systemVersionInfo,
        targetedReleases: Object.fromEntries(targetedReleases),
        downloadCodes: Object.fromEntries(downloadCodes),
        buildHistory,
        releaseBaselines: Object.fromEntries(releaseBaselines),
        integrityStatus,
        // Pricing/feature config overrides
        featureCatalogOverrides,
        featurePricingOverrides,
        upgradeTierPricingOverrides,
        cloudBackupPlanOverrides,
        moduleSubscriptionOverrides,
        suggestedDiscountPercent: SUGGESTED_DISCOUNT_PERCENT,
        neonPricingOverrides,
        neonConfiguredPlans,
        clientMaintenanceFeeConfig
    });
});
app.post('/relay/admin/api/restore', requireAdminKey, (req, res) => {
    const body = req.body || {};
    const { allowedDevices: backupAllowed, deviceLabels: backupLabels, issuedUnlocks: backupUnlocks, activityLog: backupLog } = body;
    if (!Array.isArray(backupAllowed) || typeof backupUnlocks !== 'object' || backupUnlocks === null || !Array.isArray(backupLog)) {
        return res.status(400).json({ success: false, message: 'Hindi kilalang format ng backup file — siguraduhing yung na-download galing sa /backup ang ini-restore.' });
    }
    const restoredParts = [];
    const hasLabels = backupLabels && typeof backupLabels === 'object' && !Array.isArray(backupLabels);
    allowedDevices = new Set(backupAllowed);
    saveAllowedDevices(allowedDevices);
    restoredParts.push(`${allowedDevices.size} allowed device(s)`);
    if (hasLabels) {
        deviceLabels = new Map(Object.entries(backupLabels));
        saveDeviceLabels(deviceLabels);
        restoredParts.push(`${deviceLabels.size} label(s)`);
    }
    issuedUnlocks = backupUnlocks;
    saveIssuedUnlocks(issuedUnlocks);
    restoredParts.push(`${Object.keys(issuedUnlocks).length} device(s) may naka-unlock`);
    activityLog = backupLog;
    saveActivityLog(activityLog);
    restoredParts.push(`${activityLog.length} history entry(ies)`);
    // === v3 fields — LAHAT ay OPTIONAL (backward-compatible sa lumang v2
    // backup files na 4 fields lang ang laman). Kung wala sa backup ang
    // isang field, hindi ito ginagalaw — nananatili ang kasalukuyang datos. ===
    const isPlainObj = (v) => v && typeof v === 'object' && !Array.isArray(v);
    if (isPlainObj(body.deviceFingerprints)) {
        deviceFingerprints = new Map(Object.entries(body.deviceFingerprints));
        saveDeviceFingerprints(deviceFingerprints);
        restoredParts.push(`${deviceFingerprints.size} fingerprint(s)`);
    }
    if (isPlainObj(body.cloneSplits)) {
        cloneSplits = new Map(Object.entries(body.cloneSplits));
        saveCloneSplits(cloneSplits);
        restoredParts.push(`${cloneSplits.size} clone-split(s)`);
    }
    if (isPlainObj(body.backupCheckins)) {
        backupCheckins = body.backupCheckins;
        saveBackupCheckins(backupCheckins);
    }
    if (isPlainObj(body.branchSummaries)) {
        branchSummaries = body.branchSummaries;
        saveBranchSummaries(branchSummaries);
    }
    if (isPlainObj(body.systemVersionInfo)) {
        systemVersionInfo = body.systemVersionInfo;
        saveSystemVersionInfo(systemVersionInfo);
    }
    if (isPlainObj(body.targetedReleases)) {
        targetedReleases = new Map(Object.entries(body.targetedReleases));
        saveTargetedReleases(targetedReleases);
    }
    if (isPlainObj(body.downloadCodes)) {
        downloadCodes = new Map(Object.entries(body.downloadCodes));
        saveDownloadCodes(downloadCodes);
        restoredParts.push(`${downloadCodes.size} download code(s)`);
    }
    if (Array.isArray(body.buildHistory)) {
        buildHistory = body.buildHistory;
        saveBuildHistory(buildHistory);
    }
    if (isPlainObj(body.releaseBaselines)) {
        releaseBaselines = new Map(Object.entries(body.releaseBaselines));
        saveReleaseBaselines(releaseBaselines);
    }
    if (isPlainObj(body.integrityStatus)) {
        integrityStatus = body.integrityStatus;
        saveIntegrityStatus(integrityStatus);
    }
    if (isPlainObj(body.featureCatalogOverrides)) {
        featureCatalogOverrides = body.featureCatalogOverrides;
        saveFeatureCatalogOverrides(featureCatalogOverrides);
    }
    if (isPlainObj(body.featurePricingOverrides)) {
        featurePricingOverrides = body.featurePricingOverrides;
        saveFeaturePricingOverrides(featurePricingOverrides);
    }
    if (isPlainObj(body.upgradeTierPricingOverrides)) {
        upgradeTierPricingOverrides = body.upgradeTierPricingOverrides;
        saveUpgradeTierPricingOverrides(upgradeTierPricingOverrides);
    }
    // AYOS: umaasa ang recomputeFeatureCatalog() sa PAREHONG featureCatalogOverrides
    // AT featurePricingOverrides, at ito rin ang tumatawag sa recomputeProTierFeatureIds()
    // (na umaasa naman sa upgradeTierPricingOverrides) — kaya laging tinatawag ito nang
    // isang beses dito sa dulo, hindi lang kondisyonal sa isa sa tatlong field, para hindi
    // mag-stale ang FEATURE_CATALOG/UPGRADE_TIERS kung isa lang sa tatlo ang nasa backup.
    recomputeFeatureCatalog();
    if (isPlainObj(body.cloudBackupPlanOverrides)) {
        cloudBackupPlanOverrides = body.cloudBackupPlanOverrides;
        saveCloudBackupPlanOverrides(cloudBackupPlanOverrides);
        recomputeCloudBackupPlans();
    }
    if (isPlainObj(body.moduleSubscriptionOverrides)) {
        moduleSubscriptionOverrides = body.moduleSubscriptionOverrides;
        saveModuleSubscriptionOverrides(moduleSubscriptionOverrides);
        recomputeModuleSubscriptionPlans();
    }
    if (typeof body.suggestedDiscountPercent === 'number') {
        SUGGESTED_DISCOUNT_PERCENT = body.suggestedDiscountPercent;
        saveSuggestedDiscountPercent(SUGGESTED_DISCOUNT_PERCENT);
    }
    if (isPlainObj(body.neonPricingOverrides)) {
        neonPricingOverrides = body.neonPricingOverrides;
        saveNeonPricingOverrides(neonPricingOverrides);
        recomputeNeonPricing();
    }
    if (isPlainObj(body.neonConfiguredPlans)) {
        neonConfiguredPlans = body.neonConfiguredPlans;
        saveNeonConfiguredPlans(neonConfiguredPlans);
    }
    if (isPlainObj(body.clientMaintenanceFeeConfig)) {
        clientMaintenanceFeeConfig = body.clientMaintenanceFeeConfig;
        saveClientMaintenanceFeeConfig(clientMaintenanceFeeConfig);
    }
    if (!hasLabels) restoredParts.push('(lumang backup na walang labels — hindi binago ang labels)');
    res.json({
        success: true,
        message: `Na-restore: ${restoredParts.join(', ')}.`
    });
});
app.post('/relay/restore-tokens', requireApiKey, requireAllowedDevice, rateLimit('restore-tokens', 30, 10 * 60 * 1000), (req, res) => {
    const { installationId } = req.body;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
    }
    const record = issuedUnlocks[installationId] || {};
    const now = Date.now();
    const tokens = {};
    // AYOS/BUGFIX: bukod sa token (payload+signature), isinasama na rin
    // dito ang KASALUKUYANG tier/billingCycle ng bawat subscription
    // feature (Cloud Backup Basic/Standard/Pro, o module subscription
    // monthly/yearly) — dati, dito lang sa restore-tokens nire-refresh
    // ng OMNIPOS ang lokal nitong token pagkatapos mag-expire ang luma,
    // pero WALANG paraan itong malaman ang bagong tier/billingCycle
    // dahil hindi kasama sa token payload ang mga iyon. Resulta: pag
    // nag-renew ang admin (hal. Basic -> Pro) dito sa RELAY, nagbabago
    // lang ang petsa ng expiry sa OMNIPOS pero nananatiling "Basic" (o
    // kung ano mang dating tier) ang naka-display/naka-cache doon. Ang
    // subscriptionMeta na ito ang gagamitin ng OMNIPOS para i-sync ang
    // sarili nitong lokal na cache (cloudBackupPlan / moduleSubscriptions).
    const subscriptionMeta = {};
    for (const [featureId, entry] of Object.entries(record)) {
        if (typeof entry.expiresAt === 'number' && now > entry.expiresAt) continue; 
        tokens[featureId] = { payload: entry.payload, signature: entry.signature };
        if (entry.tier || entry.billingCycle) {
            subscriptionMeta[featureId] = {
                tier: entry.tier || null,
                billingCycle: entry.billingCycle || null
            };
        }
    }
    logActivity(installationId, 'restore_checkin', { restoredCount: Object.keys(tokens).length });
    res.json({ success: true, tokens, subscriptionMeta });
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
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
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
// AYOS/SECURITY FIX: dati, kapag WALANG naka-configure na RELAY_API_KEY
// (halimbawa, di-sinasadyang nakalimutang i-set ito sa production .env),
// basta `return next()` agad ang ginagawa nito — ibig sabihin, LAHAT ng
// endpoint na pinoprotektahan ng requireApiKey (halos lahat maliban sa
// /relay/admin/*) ay NAGIGING BUKAS SA LAHAT, walang kailangang key.
// Ito ay "fail-open" sa isang money-handling backend — mapanganib kung
// magkamali ang deployment config. Ang requireAdminKey ay tama namang
// fail-CLOSED (tinatanggihan kung walang ADMIN_KEY) — ginawa na rin
// itong ganoon dito, tulad ng dapat.
function requireApiKey(req, res, next) {
    if (!RELAY_API_KEY) {
        console.error('⚠️  RELAY_API_KEY ay hindi naka-configure sa server na ito — tinatanggihan (fail-closed) ang request sa halip na tanggapin ito nang walang verification.');
        return res.status(503).json({ success: false, message: 'RELAY_API_KEY is not configured on this server yet. Please set it before using the relay.' });
    }
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
// ===================================================================
// RELAY-AUTHORITATIVE AI CREDITS
// ===================================================================
// Ang OMNIPOS .env/local storage ay UI/client configuration lamang.
// Hindi ito ginagamit bilang security gate. Ang RELAY + Neon ang source
// of truth para sa subscription at monthly AI credit consumption.
const RELAY_AI_DEFAULT_MONTHLY_CREDITS = Math.max(1, parseInt(process.env.RELAY_AI_MONTHLY_CREDITS, 10) || 300);
const RELAY_AI_DEFAULT_TEXT_CREDIT_COST = Math.max(1, parseInt(process.env.RELAY_AI_TEXT_CREDIT_COST, 10) || 1);
const RELAY_AI_DEFAULT_FILE_CREDIT_COST = Math.max(RELAY_AI_DEFAULT_TEXT_CREDIT_COST, parseInt(process.env.RELAY_AI_FILE_CREDIT_COST, 10) || 2);
const RELAY_AI_DEFAULT_IMAGE_CREDIT_COST = Math.max(RELAY_AI_DEFAULT_FILE_CREDIT_COST, parseInt(process.env.RELAY_AI_IMAGE_CREDIT_COST, 10) || 3);
const RELAY_AI_DEFAULT_SETTINGS_ID = '__default__';

function relayAiMonthKey() {
    const d = new Date();
    return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
}
function getRelayAiCost(body, config) {
    const messages = Array.isArray(body?.messages) ? body.messages : [];
    const hasImagePayload = !!body?.vision || messages.some(m => Array.isArray(m?.content) && m.content.some(c => c && c.type === 'image_url'));
    if (hasImagePayload) return config.imageCost;
    if (body?.attachmentType === 'file') return config.fileCost;
    return config.textCost;
}
function hasActiveRelayAiSubscription(installationId) {
    const entry = (issuedUnlocks[installationId] || {}).ai_assistant;
    if (!entry) return false;
    const expiresAt = Number(entry.expiresAt || (entry.payload && entry.payload.expiresAt) || 0);
    return !expiresAt || expiresAt > Date.now();
}
function normalizeRelayAiSettingNumber(value, fallback, min = 1, max = 1000000000) {
    const n = Number(value);
    if (!Number.isInteger(n) || n < min || n > max) return fallback;
    return n;
}
function normalizeRelayAiSettings(body, base = {}) {
    const monthlyCredits = normalizeRelayAiSettingNumber(body?.monthlyCredits, base.monthlyCredits || RELAY_AI_DEFAULT_MONTHLY_CREDITS, 1);
    const textCost = normalizeRelayAiSettingNumber(body?.textCost, base.textCost || RELAY_AI_DEFAULT_TEXT_CREDIT_COST, 1);
    const fileCost = normalizeRelayAiSettingNumber(body?.fileCost, base.fileCost || RELAY_AI_DEFAULT_FILE_CREDIT_COST, textCost);
    const imageCost = normalizeRelayAiSettingNumber(body?.imageCost, base.imageCost || RELAY_AI_DEFAULT_IMAGE_CREDIT_COST, fileCost);
    return { monthlyCredits, textCost, fileCost, imageCost };
}
async function getRelayAiDefaultSettings(clientOrPool = pgPoolDevices) {
    if (!clientOrPool) throw new Error('AI credit database is not configured on the RELAY.');
    const result = await clientOrPool.query(
        `SELECT monthly_credits, text_cost, file_cost, image_cost, updated_at\n         FROM relay_ai_credit_settings WHERE installation_id = $1`,
        [RELAY_AI_DEFAULT_SETTINGS_ID]
    );
    if (!result.rows[0]) {
        return {
            monthlyCredits: RELAY_AI_DEFAULT_MONTHLY_CREDITS,
            textCost: RELAY_AI_DEFAULT_TEXT_CREDIT_COST,
            fileCost: RELAY_AI_DEFAULT_FILE_CREDIT_COST,
            imageCost: RELAY_AI_DEFAULT_IMAGE_CREDIT_COST,
            updatedAt: null,
            source: 'env-default'
        };
    }
    const row = result.rows[0];
    return {
        monthlyCredits: Number(row.monthly_credits),
        textCost: Number(row.text_cost),
        fileCost: Number(row.file_cost),
        imageCost: Number(row.image_cost),
        updatedAt: row.updated_at,
        source: 'relay-default'
    };
}
async function getRelayAiSettings(installationId, clientOrPool = pgPoolDevices) {
    if (!clientOrPool) throw new Error('AI credit database is not configured on the RELAY.');
    const defaults = await getRelayAiDefaultSettings(clientOrPool);
    if (!installationId || installationId === RELAY_AI_DEFAULT_SETTINGS_ID) return defaults;
    const result = await clientOrPool.query(
        `SELECT monthly_credits, text_cost, file_cost, image_cost, updated_at\n         FROM relay_ai_credit_settings WHERE installation_id = $1`,
        [installationId]
    );
    if (!result.rows[0]) return { ...defaults, source: 'relay-default', override: false };
    const row = result.rows[0];
    return {
        monthlyCredits: Number(row.monthly_credits),
        textCost: Number(row.text_cost),
        fileCost: Number(row.file_cost),
        imageCost: Number(row.image_cost),
        updatedAt: row.updated_at,
        source: 'installation-override',
        override: true
    };
}
async function getRelayAiCreditStatus(installationId, clientOrPool = pgPoolDevices) {
    if (!clientOrPool) throw new Error('AI credit database is not configured on the RELAY.');
    const monthKey = relayAiMonthKey();
    const settings = await getRelayAiSettings(installationId, clientOrPool);
    const result = await clientOrPool.query(
        `SELECT used_credits FROM relay_ai_credit_usage WHERE installation_id = $1 AND month_key = $2`,
        [installationId, monthKey]
    );
    const used = result.rows[0] ? Number(result.rows[0].used_credits) || 0 : 0;
    return {
        month: monthKey,
        used,
        limit: settings.monthlyCredits,
        remaining: Math.max(0, settings.monthlyCredits - used),
        settings
    };
}
async function reserveRelayAiCredits(installationId, requestId, body) {
    if (!pgPoolDevices) return { ok: false, reason: 'AI credit database is not configured on the RELAY.' };
    const monthKey = relayAiMonthKey();
    const client = await pgPoolDevices.connect();
    try {
        await client.query('BEGIN');
        // Lock the effective settings for this transaction so an admin change
        // cannot create a half-old/half-new credit reservation.
        const settingsRows = await client.query(
            `SELECT installation_id, monthly_credits, text_cost, file_cost, image_cost\n             FROM relay_ai_credit_settings\n             WHERE installation_id IN ($1, $2)\n             ORDER BY CASE WHEN installation_id = $1 THEN 0 ELSE 1 END\n             FOR UPDATE`,
            [installationId, RELAY_AI_DEFAULT_SETTINGS_ID]
        );
        const defaultRow = settingsRows.rows.find(r => r.installation_id === RELAY_AI_DEFAULT_SETTINGS_ID);
        const overrideRow = settingsRows.rows.find(r => r.installation_id === installationId);
        const settings = overrideRow ? {
            monthlyCredits: Number(overrideRow.monthly_credits), textCost: Number(overrideRow.text_cost),
            fileCost: Number(overrideRow.file_cost), imageCost: Number(overrideRow.image_cost),
            updatedAt: null, source: 'installation-override', override: true
        } : {
            monthlyCredits: Number(defaultRow?.monthly_credits || RELAY_AI_DEFAULT_MONTHLY_CREDITS),
            textCost: Number(defaultRow?.text_cost || RELAY_AI_DEFAULT_TEXT_CREDIT_COST),
            fileCost: Number(defaultRow?.file_cost || RELAY_AI_DEFAULT_FILE_CREDIT_COST),
            imageCost: Number(defaultRow?.image_cost || RELAY_AI_DEFAULT_IMAGE_CREDIT_COST),
            updatedAt: null, source: 'relay-default', override: false
        };
        const cost = getRelayAiCost(body, settings);
        const existing = await client.query(
            `SELECT credit_cost, status FROM relay_ai_credit_requests\n             WHERE installation_id = $1 AND month_key = $2 AND request_id = $3 FOR UPDATE`,
            [installationId, monthKey, requestId]
        );
        if (existing.rows[0]) {
            const row = existing.rows[0];
            const originalCost = Number(row.credit_cost);
            if (row.status === 'completed' || row.status === 'retrying' || row.status === 'reserved') {
                await client.query('ROLLBACK');
                return { ok: false, reason: 'request_reuse' };
            }
            // A failed logical request may retry once with the SAME requestId.
            // Reuse the original charged cost even if an admin changed pricing
            // between attempts. This prevents a mid-request config change from
            // causing a false mismatch/double charge.
            if (row.status === 'failed') {
                const usage = await client.query(
                    `SELECT used_credits FROM relay_ai_credit_usage WHERE installation_id = $1 AND month_key = $2`,
                    [installationId, monthKey]
                );
                const used = usage.rows[0] ? Number(usage.rows[0].used_credits) || 0 : 0;
                await client.query('COMMIT');
                return { ok: true, reused: true, cost: originalCost, status: {
                    month: monthKey, used, limit: settings.monthlyCredits,
                    remaining: Math.max(0, settings.monthlyCredits - used), settings
                } };
            }
            await client.query('ROLLBACK');
            return { ok: false, reason: 'request_reuse' };
        }
        const upsert = await client.query(
            `INSERT INTO relay_ai_credit_usage (installation_id, month_key, used_credits)\n             VALUES ($1, $2, $3)\n             ON CONFLICT (installation_id, month_key) DO UPDATE\n             SET used_credits = relay_ai_credit_usage.used_credits + EXCLUDED.used_credits, updated_at = now()\n             WHERE relay_ai_credit_usage.used_credits + EXCLUDED.used_credits <= $4\n             RETURNING used_credits`,
            [installationId, monthKey, cost, settings.monthlyCredits]
        );
        if (!upsert.rows[0]) {
            const usage = await client.query(
                `SELECT used_credits FROM relay_ai_credit_usage WHERE installation_id = $1 AND month_key = $2`,
                [installationId, monthKey]
            );
            const used = usage.rows[0] ? Number(usage.rows[0].used_credits) || 0 : 0;
            await client.query('ROLLBACK');
            return { ok: false, reason: 'exhausted', status: {
                month: monthKey, used, limit: settings.monthlyCredits,
                remaining: Math.max(0, settings.monthlyCredits - used), settings
            }, cost };
        }
        await client.query(
            `INSERT INTO relay_ai_credit_requests (installation_id, month_key, request_id, credit_cost, status)\n             VALUES ($1, $2, $3, $4, 'reserved')`,
            [installationId, monthKey, requestId, cost]
        );
        const used = Number(upsert.rows[0].used_credits) || 0;
        await client.query('COMMIT');
        return { ok: true, reused: false, cost, status: {
            month: monthKey, used, limit: settings.monthlyCredits,
            remaining: Math.max(0, settings.monthlyCredits - used), settings
        } };
    } catch (err) {
        try { await client.query('ROLLBACK'); } catch (_) {}
        throw err;
    } finally {
        client.release();
    }
}
async function markRelayAiRequestStatus(installationId, requestId, status) {
    if (!pgPoolDevices || !requestId) return;
    const monthKey = relayAiMonthKey();
    await queryWithRetry(pgPoolDevices,
        `UPDATE relay_ai_credit_requests SET status = $4, updated_at = now() WHERE installation_id = $1 AND month_key = $2 AND request_id = $3`,
        [installationId, monthKey, requestId, status]);
}

app.post('/relay/ai-assistant/complete', requireApiKey, requireAllowedDevice, rateLimit('ai-assistant-complete', 40, 5 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
    const installationId = String(req.body?.installationId || '').trim();
    const requestId = String(req.body?.requestId || '').trim();
    if (!installationId) return res.status(400).json({ success: false, message: 'Missing installationId.' });
    if (!requestId || requestId.length > 120) return res.status(400).json({ success: false, message: 'Missing or invalid requestId.' });
    if (!hasActiveRelayAiSubscription(installationId)) {
        return res.status(403).json({ success: false, subscriptionRequired: true, message: 'Walang active OmniPOS AI Assistant subscription para sa device na ito.' });
    }
    if (!isCfAiConfigured()) {
        return res.status(503).json({ success: false, message: 'AI Assistant is not configured on the relay server (missing CF_ACCOUNT_ID/CF_AI_API_TOKEN in RELAY .env). Contact the developer.' });
    }
    const messages = Array.isArray(req.body?.messages) ? req.body.messages : null;
    if (!messages || !messages.length) return res.status(400).json({ success: false, message: 'Missing messages.' });
    try {
        const reservation = await reserveRelayAiCredits(installationId, requestId, req.body);
        if (!reservation.ok) {
            if (reservation.reason === 'exhausted') {
                return res.status(402).json({ success: false, creditsExhausted: true, creditCost: reservation.cost, ...reservation.status, message: `Naubos na ang buwanang AI credits (${reservation.status.used}/${reservation.status.limit}). Mare-reset ito sa susunod na buwan.` });
            }
            if (reservation.reason === 'request_mismatch' || reservation.reason === 'request_reuse') return res.status(409).json({ success: false, message: 'Invalid AI request reuse.' });
            return res.status(503).json({ success: false, message: reservation.reason });
        }
        // The same requestId is deliberately reusable once for the OMNIPOS
        // vision->text fallback. It prevents a failed vision attempt from
        // charging the same user question twice.
        if (reservation.reused) {
            await markRelayAiRequestStatus(installationId, requestId, 'retrying');
        }
        const result = await callCloudflareWorkersAI(messages, !!req.body?.vision);
        if (!result.success) {
            // Keep the single reservation for this logical user request.
            // OMNIPOS may use the same requestId once for its vision->text
            // fallback without charging a second time. A later unrelated
            // request always receives a fresh requestId and is charged normally.
            await markRelayAiRequestStatus(installationId, requestId, 'failed');
            return res.status(502).json({ ...result, creditCost: reservation.cost, credits: reservation.status });
        }
        await markRelayAiRequestStatus(installationId, requestId, 'completed');
        res.json({ ...result, creditCost: reservation.cost, credits: reservation.status });
    } catch (err) {
        console.error('⚠️ RELAY AI credit/request error:', err.message);
        return res.status(503).json({ success: false, message: 'AI credit service temporarily unavailable.' });
    }
});
app.get('/relay/ai-assistant/usage', requireApiKey, requireAllowedDevice, rateLimit('ai-assistant-usage', 120, 60 * 60 * 1000, (req) => req.query?.installationId), async (req, res) => {
    const installationId = String(req.query?.installationId || '').trim();
    if (!installationId) return res.status(400).json({ success: false, message: 'Missing installationId.' });
    if (!hasActiveRelayAiSubscription(installationId)) return res.status(403).json({ success: false, subscriptionRequired: true, message: 'Walang active OmniPOS AI Assistant subscription para sa device na ito.' });
    try {
        const credits = await getRelayAiCreditStatus(installationId);
        return res.json({ success: true, ...credits });
    } catch (err) {
        return res.status(503).json({ success: false, message: err.message || 'AI credit service unavailable.' });
    }
});
// ===================================================================
// ADMIN: RELAY AI CREDIT SETTINGS
// ===================================================================
// Ito ang editable source of truth. Ang customer/OMNIPOS .env ay hindi
// makakapagpalit ng monthly limit o per-request cost dito.
app.get('/relay/admin/api/ai-credits/settings', requireAdminKey, async (req, res) => {
    try {
        if (!pgPoolDevices) return res.status(503).json({ success: false, message: 'Devices/License database is not configured.' });
        const defaults = await getRelayAiDefaultSettings();
        const monthKey = relayAiMonthKey();
        const rows = await pgPoolDevices.query(
            `SELECT s.installation_id, s.monthly_credits, s.text_cost, s.file_cost, s.image_cost, s.updated_at,\n                    COALESCE(u.used_credits, 0) AS used_credits\n             FROM relay_ai_credit_settings s\n             LEFT JOIN relay_ai_credit_usage u\n               ON u.installation_id = s.installation_id AND u.month_key = $1\n             WHERE s.installation_id <> $2\n             ORDER BY s.updated_at DESC`,
            [monthKey, RELAY_AI_DEFAULT_SETTINGS_ID]
        );
        const overrides = rows.rows.map(r => ({
            installationId: r.installation_id,
            monthlyCredits: Number(r.monthly_credits),
            textCost: Number(r.text_cost),
            fileCost: Number(r.file_cost),
            imageCost: Number(r.image_cost),
            used: Number(r.used_credits) || 0,
            remaining: Math.max(0, Number(r.monthly_credits) - (Number(r.used_credits) || 0)),
            updatedAt: r.updated_at
        }));
        return res.json({ success: true, month: monthKey, defaults, overrides });
    } catch (err) {
        console.error('AI credit settings GET error:', err.message);
        return res.status(503).json({ success: false, message: 'Unable to load AI credit settings.' });
    }
});
app.post('/relay/admin/api/ai-credits/default', requireAdminKey, async (req, res) => {
    try {
        if (!pgPoolDevices) return res.status(503).json({ success: false, message: 'Devices/License database is not configured.' });
        const current = await getRelayAiDefaultSettings();
        const settings = normalizeRelayAiSettings(req.body, current);
        await pgPoolDevices.query(
            `INSERT INTO relay_ai_credit_settings (installation_id, monthly_credits, text_cost, file_cost, image_cost, updated_at)\n             VALUES ($1, $2, $3, $4, $5, now())\n             ON CONFLICT (installation_id) DO UPDATE SET\n               monthly_credits = EXCLUDED.monthly_credits, text_cost = EXCLUDED.text_cost,\n               file_cost = EXCLUDED.file_cost, image_cost = EXCLUDED.image_cost, updated_at = now()`,
            [RELAY_AI_DEFAULT_SETTINGS_ID, settings.monthlyCredits, settings.textCost, settings.fileCost, settings.imageCost]
        );
        const saved = await getRelayAiDefaultSettings();
        console.log(`🤖 AI credit defaults updated via admin: ${saved.monthlyCredits} monthly / text ${saved.textCost} / file ${saved.fileCost} / image ${saved.imageCost}`);
        return res.json({ success: true, settings: saved });
    } catch (err) {
        console.error('AI credit default update error:', err.message);
        return res.status(503).json({ success: false, message: 'Unable to save AI credit defaults.' });
    }
});
app.post('/relay/admin/api/ai-credits/installation', requireAdminKey, async (req, res) => {
    try {
        if (!pgPoolDevices) return res.status(503).json({ success: false, message: 'Devices/License database is not configured.' });
        const installationId = String(req.body?.installationId || '').trim();
        if (!installationId || installationId === RELAY_AI_DEFAULT_SETTINGS_ID || installationId.length > 200) {
            return res.status(400).json({ success: false, message: 'Valid installationId is required.' });
        }
        const current = await getRelayAiSettings(installationId);
        const settings = normalizeRelayAiSettings(req.body, current);
        await pgPoolDevices.query(
            `INSERT INTO relay_ai_credit_settings (installation_id, monthly_credits, text_cost, file_cost, image_cost, updated_at)\n             VALUES ($1, $2, $3, $4, $5, now())\n             ON CONFLICT (installation_id) DO UPDATE SET\n               monthly_credits = EXCLUDED.monthly_credits, text_cost = EXCLUDED.text_cost,\n               file_cost = EXCLUDED.file_cost, image_cost = EXCLUDED.image_cost, updated_at = now()`,
            [installationId, settings.monthlyCredits, settings.textCost, settings.fileCost, settings.imageCost]
        );
        const saved = await getRelayAiSettings(installationId);
        console.log(`🤖 AI credit override updated for ${installationId}: ${saved.monthlyCredits} monthly`);
        return res.json({ success: true, installationId, settings: saved });
    } catch (err) {
        console.error('AI credit installation update error:', err.message);
        return res.status(503).json({ success: false, message: 'Unable to save installation AI credit override.' });
    }
});
app.post('/relay/admin/api/ai-credits/installation/reset', requireAdminKey, async (req, res) => {
    try {
        if (!pgPoolDevices) return res.status(503).json({ success: false, message: 'Devices/License database is not configured.' });
        const installationId = String(req.body?.installationId || '').trim();
        if (!installationId || installationId === RELAY_AI_DEFAULT_SETTINGS_ID) {
            return res.status(400).json({ success: false, message: 'Valid installationId is required.' });
        }
        await pgPoolDevices.query(`DELETE FROM relay_ai_credit_settings WHERE installation_id = $1`, [installationId]);
        const effective = await getRelayAiSettings(installationId);
        console.log(`🤖 AI credit override reset for ${installationId}; default is active again.`);
        return res.json({ success: true, installationId, settings: effective });
    } catch (err) {
        console.error('AI credit installation reset error:', err.message);
        return res.status(503).json({ success: false, message: 'Unable to reset installation AI credit override.' });
    }
});
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
    // AYOS/BAGO: ang "maintenance fee" na ibinabalik dito ngayon ay
    // hango na sa MONTHLY/YEARLY presyo mismo ng Cloud Backup tier
    // (basic/standard/pro — CLOUD_BACKUP_PLANS) na kinuha/sinubscribe ng
    // client, HINDI na sa dating flat/admin-configurable na
    // clientMaintenanceFeeConfig — tingnan ang getCloudBackupSubscriptionForClient()/
    // getCloudBackupTierPricePHP() sa itaas (parehong ginagamit na rin ng
    // computeClientCostAllocation(), para magkatugma ang tier modal, ang
    // cost-share widget, at ang Client Cost Allocation admin).
    // FIX/hardening: bago ibalik ang per-client fields (tier + paid
    // status), i-check muna kung nasa allowedDevices ang installationId na
    // ito (parehong gate na ginagamit ng requireAllowedDevice sa ibang mga
    // per-client na endpoint tulad ng /relay/cloud-backup/cost-allocation).
    // Hindi natin ginawang buong requireAllowedDevice ang buong route dahil
    // dapat pa ring makakuha ng generic catalog/plans ang mga bagong device
    // na hindi pa "allowed" (kailangan nila makita ang tier modal bago pa
    // sila ma-approve) — ang ipinagbabawal lang dito ay ang pagbunyag ng
    // per-client tier/paid data ng isang installationId na hindi pa naman
    // kilalang device.
    const installationId = String(req.query.installationId || '').trim();
    const perClientFee = {};
    if (installationId && allowedDevices.has(installationId)) {
        const cbSubscription = getCloudBackupSubscriptionForClient(installationId);
        const cbTierPrice = getCloudBackupTierPricePHP(cbSubscription.tier);
        const paid = cbSubscription.active || cbSubscription.isLifetime;
        perClientFee.cloudBackupTier = cbSubscription.tier;
        perClientFee.cloudBackupBillingCycle = cbSubscription.billingCycle;
        perClientFee.cloudBackupIsLifetime = cbSubscription.isLifetime;
        perClientFee.maintenanceFeeMonthlyPHP = cbTierPrice.monthly;
        perClientFee.maintenanceFeeYearlyPHP = cbTierPrice.yearly;
        perClientFee.maintenanceFeePHP = paid ? 0 : cbTierPrice.monthly;
        perClientFee.maintenanceFeePaid = paid;
        perClientFee.maintenanceFeePaidUntil = cbSubscription.expiresAt;
    }
    res.json({
        success: true,
        cloudBackupPlans: CLOUD_BACKUP_PLANS,
        ...perClientFee,
        billingDays: CLOUD_BACKUP_BILLING_DAYS,
        featureCatalog,
        upgradeTiers,
        moduleSubscriptions: MODULE_SUBSCRIPTION_PLANS,
        moduleSubscriptionBillingDays: MODULE_SUBSCRIPTION_BILLING_DAYS,
        moduleSubscriptionGracePeriodDays: MODULE_SUBSCRIPTION_GRACE_PERIOD_DAYS,
        activationFlags: ACTIVATION_FLAGS,
        fetchedAt: new Date().toISOString()
    });
});
app.post('/relay/backup-checkin', requireApiKey, rateLimit('backup-checkin', 20, 60 * 60 * 1000, (req) => req.body?.installationId), (req, res) => {
    const { installationId, storeName, username, fileSizeBytes, backupAt } = req.body;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
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
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
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
        maybePersistIntegrityStatus(installationId, integrityStatus[installationId]);
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
    maybePersistIntegrityStatus(installationId, integrityStatus[installationId]);
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
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
    }
    if (!branchGroupKeyHash || !BRANCH_GROUP_HASH_RE.test(String(branchGroupKeyHash))) {
        return res.status(400).json({ success: false, message: 'Missing or invalid branchGroupKeyHash format (must be SHA-256 hex).' });
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
    appendBranchHistoryPoint(branchGroupKeyHash, installationId, cleanSummary);
    const branchPersistKey = `${branchGroupKeyHash}|${installationId}`;
    const branchSig = branchEntrySignature(cleanName, cleanSummary);
    const branchPrev = branchLastPersisted.get(branchPersistKey);
    const branchDue = !branchPrev || (Date.now() - branchPrev.at) >= BRANCH_PERSIST_MIN_INTERVAL_MS;
    if (changed || branchDue || !branchPrev || branchPrev.signature !== branchSig) {
        saveBranchSummaries(branchSummaries);
        branchLastPersisted.set(branchPersistKey, { signature: branchSig, at: Date.now() });
    }
    if (changed) {   }
    res.json({ success: true, message: 'Branch check-in recorded.' });
});
app.get('/relay/branch-summary', requireApiKey, requireAllowedDevice, rateLimit('branch-summary', 120, 60 * 60 * 1000, (req) => req.query?.installationId), (req, res) => {
    const groupKeyHash = String(req.query.groupKeyHash || '');
    if (!BRANCH_GROUP_HASH_RE.test(groupKeyHash)) {
        return res.status(400).json({ success: false, message: 'Missing or invalid groupKeyHash query param format.' });
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
app.get('/relay/branch-trend', requireApiKey, requireAllowedDevice, rateLimit('branch-trend', 120, 60 * 60 * 1000, (req) => req.query?.installationId), (req, res) => {
    const groupKeyHash = String(req.query.groupKeyHash || '');
    if (!BRANCH_GROUP_HASH_RE.test(groupKeyHash)) {
        return res.status(400).json({ success: false, message: 'Missing or invalid groupKeyHash query param format.' });
    }
    const group = branchSummaries[groupKeyHash] || {};
    const branches = Object.entries(group).map(([installationId, entry]) => ({
        installationId,
        branchName: entry.branchName,
        history: branchHistory[`${groupKeyHash}|${installationId}`] || []
    }));
    // Combined trend: bucket all points from all branches into the nearest hour
    // (rounded to the hour) and sum each branch's most recent point's grossSalesToday
    // before that bucket — a way to derive the "total sales curve" across the day.
    const bucketMap = new Map();
    for (const b of branches) {
        for (const point of b.history) {
            const bucketTs = Math.floor(point.ts / (60 * 60 * 1000)) * (60 * 60 * 1000);
            if (!bucketMap.has(bucketTs)) bucketMap.set(bucketTs, {});
            bucketMap.get(bucketTs)[b.installationId] = point;
        }
    }
    const sortedBuckets = Array.from(bucketMap.keys()).sort((a, b) => a - b);
    const lastKnown = {};
    const combinedHistory = sortedBuckets.map((ts) => {
        const atBucket = bucketMap.get(ts);
        for (const [id, point] of Object.entries(atBucket)) lastKnown[id] = point;
        const acc = { ts };
        for (const field of BRANCH_SUMMARY_NUMERIC_FIELDS) {
            acc[field] = Object.values(lastKnown).reduce((sum, p) => sum + (Number(p[field]) || 0), 0);
        }
        return acc;
    });
    res.json({ success: true, branches, combinedHistory });
});
app.post('/relay/branch-transfer-request', requireApiKey, requireAllowedDevice, rateLimit('branch-transfer-request', 60, 60 * 60 * 1000, (req) => req.body?.installationId), (req, res) => {
    const { installationId, branchGroupKeyHash, fromBranchName, toInstallationId, toBranchName, itemName, sku, qty, note } = req.body || {};
    if (!installationId || !toInstallationId) {
        return res.status(400).json({ success: false, message: 'Missing installationId or toInstallationId.' });
    }
    if (!branchGroupKeyHash || !BRANCH_GROUP_HASH_RE.test(String(branchGroupKeyHash))) {
        return res.status(400).json({ success: false, message: 'Missing or invalid branchGroupKeyHash format.' });
    }
    if (installationId === toInstallationId) {
        return res.status(400).json({ success: false, message: 'The transfer destination cannot be the same branch as the source.' });
    }
    const cleanItemName = String(itemName || '').trim().slice(0, 120);
    if (!cleanItemName) {
        return res.status(400).json({ success: false, message: 'Missing item name.' });
    }
    const cleanQty = Math.max(1, Math.floor(Number(qty) || 0));
    if (!Number.isFinite(cleanQty) || cleanQty < 1) {
        return res.status(400).json({ success: false, message: 'Invalid quantity.' });
    }
    const transfer = {
        id: crypto.randomBytes(8).toString('hex'),
        fromInstallationId: installationId,
        fromBranchName: String(fromBranchName || '').trim().slice(0, BRANCH_NAME_MAX_LEN) || 'Unnamed Branch',
        toInstallationId,
        toBranchName: String(toBranchName || '').trim().slice(0, BRANCH_NAME_MAX_LEN) || 'Unnamed Branch',
        itemName: cleanItemName,
        sku: String(sku || '').trim().slice(0, 60),
        qty: cleanQty,
        note: String(note || '').trim().slice(0, 300),
        status: 'pending',
        createdAt: Date.now(),
        updatedAt: Date.now()
    };
    if (!Array.isArray(branchTransfers[branchGroupKeyHash])) branchTransfers[branchGroupKeyHash] = [];
    branchTransfers[branchGroupKeyHash].unshift(transfer);
    if (branchTransfers[branchGroupKeyHash].length > BRANCH_TRANSFERS_MAX_PER_GROUP) {
        branchTransfers[branchGroupKeyHash].length = BRANCH_TRANSFERS_MAX_PER_GROUP;
    }
    saveBranchTransfers(branchTransfers);
    res.json({ success: true, transfer });
});
app.get('/relay/branch-transfers', requireApiKey, requireAllowedDevice, rateLimit('branch-transfers', 120, 60 * 60 * 1000, (req) => req.query?.installationId), (req, res) => {
    const groupKeyHash = String(req.query.groupKeyHash || '');
    if (!BRANCH_GROUP_HASH_RE.test(groupKeyHash)) {
        return res.status(400).json({ success: false, message: 'Missing or invalid groupKeyHash query param format.' });
    }
    const list = branchTransfers[groupKeyHash] || [];
    res.json({ success: true, transfers: list });
});
// AYOS/BUGFIX (two-sided stock movement): dati, ang tanging ginagawa dito ay
// palitan ang `status` field (pending -> accepted/rejected/cancelled) — walang
// kahit anong epekto sa totoong stock ng alinmang branch, kaya effectively
// "request/coordination tracker" lang ito. Dinagdagan ngayon ng dalawang bagong
// action/status para maging tunay na two-sided na paglipat ng stock:
//   pending -> accepted -> in_transit ("Mark as Sent", source branch) -> completed ("Confirm Received", destination branch)
// Mahalaga: ang RELAY na ito ay walang access sa totoong Products/Inventory ng
// alinmang branch (magkahiwalay na database bawat branch — RELAY lang ang
// tagapag-ugnay/coordinator). Kaya ang totoong pagbawas ng stock sa source at
// pagdagdag ng stock sa destination ay ginagawa ng bawat OMNIPOS instance sa
// sarili nitong server (tingnan ang /api/branches/transfer-respond sa OMNIPOS),
// BAGO tumawag dito para i-update ang shared status. Dito lang pinipilit ang
// tamang pagkakasunod-sunod (state machine) at kung sinong branch ang
// pwedeng gumawa ng bawat hakbang.
const BRANCH_TRANSFER_ACTIONS = ['accept', 'reject', 'cancel', 'send', 'receive'];
const BRANCH_TRANSFER_NEXT_STATUS = {
    accept: 'accepted',
    reject: 'rejected',
    cancel: 'cancelled',
    send: 'in_transit',
    receive: 'completed'
};
// Aling status kailangan bago payagan ang bawat action, at sinong panig
// (source/destination installationId) lang ang pwedeng gumawa nito.
const BRANCH_TRANSFER_RULES = {
    accept: { requiredStatus: 'pending', actorField: 'toInstallationId', errorMessage: 'Only the destination branch can accept or reject this transfer.' },
    reject: { requiredStatus: 'pending', actorField: 'toInstallationId', errorMessage: 'Only the destination branch can accept or reject this transfer.' },
    cancel: { requiredStatus: 'pending', actorField: 'fromInstallationId', errorMessage: 'Only the requesting branch can cancel this transfer.' },
    send: { requiredStatus: 'accepted', actorField: 'fromInstallationId', errorMessage: 'Only the source branch can mark this transfer as sent.' },
    receive: { requiredStatus: 'in_transit', actorField: 'toInstallationId', errorMessage: 'Only the destination branch can confirm receipt of this transfer.' }
};
app.post('/relay/branch-transfer-respond', requireApiKey, requireAllowedDevice, rateLimit('branch-transfer-respond', 60, 60 * 60 * 1000, (req) => req.body?.installationId), (req, res) => {
    const { installationId, branchGroupKeyHash, transferId, action } = req.body || {};
    if (!installationId || !transferId || !BRANCH_TRANSFER_ACTIONS.includes(action)) {
        return res.status(400).json({ success: false, message: 'Missing or invalid request.' });
    }
    if (!branchGroupKeyHash || !BRANCH_GROUP_HASH_RE.test(String(branchGroupKeyHash))) {
        return res.status(400).json({ success: false, message: 'Missing or invalid branchGroupKeyHash format.' });
    }
    const list = branchTransfers[branchGroupKeyHash] || [];
    const transfer = list.find((t) => t.id === transferId);
    if (!transfer) {
        return res.status(404).json({ success: false, message: 'Transfer request not found.' });
    }
    const rule = BRANCH_TRANSFER_RULES[action];
    if (transfer.status !== rule.requiredStatus) {
        return res.status(409).json({ success: false, message: `This transfer must be "${rule.requiredStatus}" for that action (current status: ${transfer.status}).` });
    }
    if (installationId !== transfer[rule.actorField]) {
        return res.status(403).json({ success: false, message: rule.errorMessage });
    }
    transfer.status = BRANCH_TRANSFER_NEXT_STATUS[action];
    transfer.updatedAt = Date.now();
    if (action === 'send') transfer.sentAt = Date.now();
    if (action === 'receive') transfer.receivedAt = Date.now();
    saveBranchTransfers(branchTransfers);
    res.json({ success: true, transfer });
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
app.post('/relay/cloud-backup/upload/start', requireApiKey, requireAllowedDevice, rateLimit('cloud-backup-upload-start', 30, 60 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
    const { installationId, totalBytes } = req.body || {};
    if (!installationId || typeof totalBytes !== 'number' || totalBytes <= 0) {
        return res.status(400).json({ success: false, message: 'Missing or invalid installationId/totalBytes.' });
    }
    // AYOS/BAGO: kung gzip-compressed ang papadalang buong backup (bagong
    // OMNIPOS client), `totalBytes` dito ay ang COMPRESSED na laki (ito
    // ang aktwal na bytes na ipapasa sa /upload/chunk). Kung ibinigay ng
    // client ang `uncompressedSizeBytes` (ang tunay na decompressed na
    // laki), gamitin natin ITO para sa mga pre-check sa ibaba (token
    // balance estimate, storage quota) — mas tumpak ito kaysa sa
    // compressed size. Kung wala namang naipasa (mas lumang client na
    // hindi pa naka-gzip), babalik lang ito sa totalBytes gaya ng dati —
    // walang pagbabago sa behavior nila.
    const compressed = req.body && req.body.compressed === true;
    const uncompressedSizeBytesRaw = req.body ? req.body.uncompressedSizeBytes : undefined;
    const sizeBytesForPrecheck = (typeof uncompressedSizeBytesRaw === 'number' && uncompressedSizeBytesRaw > 0)
        ? uncompressedSizeBytesRaw
        : totalBytes;
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
    // AYOS/SECURITY FIX: fail-fast na Omni Token check BAGO pa man
    // tanggapin ang unang chunk. Hindi ito ang aktwal/atomic na gate
    // (nasa ibaba iyon, sa upload/finish, kaagad bago ang totoong
    // pagsulat sa Postgres) — ito ay para lang hindi na mag-aksaya ng
    // bandwidth/memory sa pagtanggap ng buong backup kung alam na
    // agad na kulang ang balance. Kung sakaling mabilis na nagbago ang
    // balance sa pagitan nito at ng finish (hal. auto-sync at manual
    // sync na sabay-sabay), ang finish gate pa rin ang huling
    // magpapasya — hindi ito papalitan.
    try {
        const walletRow = await getOrCreateCloudTokenWallet(installationId);
        const cloudBackupUnlockForPrecheck = (issuedUnlocks[installationId] || {})['cloud_backup'];
        const tierForPrecheck = (cloudBackupUnlockForPrecheck && cloudBackupUnlockForPrecheck.tier && CLOUD_BACKUP_PLANS[cloudBackupUnlockForPrecheck.tier]) ? cloudBackupUnlockForPrecheck.tier : 'basic';
        // AYOS/BUGFIX: `totalBytes` na ang AKTWAL na deklaradong laki ng
        // upload na ito (hindi na basta tier-derived/maintenance-fee-based
        // na estimate) — tingnan ang comment sa getCloudTokenCostPerSyncExact().
        const minCostForPrecheck = await getCloudTokenCostPerSyncExact(sizeBytesForPrecheck, tierForPrecheck);
        if (Number(walletRow.balance_tokens) < minCostForPrecheck) {
            logActivity(installationId, 'cloud_backup_blocked', { reason: 'insufficient_tokens', balanceTokens: Number(walletRow.balance_tokens) });
            return res.status(402).json({
                success: false,
                insufficientTokens: true,
                balanceTokens: Number(walletRow.balance_tokens),
                message: 'Insufficient Cloud Backup (Omni Tokens) balance. Please buy more tokens to keep syncing.'
            });
        }
    } catch (walletErr) {
        // AYOS: fail-CLOSED dito (hindi tulad ng ibang "balance check" sa
        // OMNIPOS na fail-open kapag hindi ma-verify) — dahil ang RELAY
        // mismo ang authoritative source ng balance ngayon, kung sandaling
        // nabigo ang query (hal. transient Postgres blip), mas ligtas na
        // sabihin sa client na mag-retry na lang, kaysa tuluyang tanggapin
        // ang upload nang hindi na-verify ang balance.
        console.error('⚠️ CLOUD_BACKUP: hindi ma-verify ang Omni Token balance bago mag-start ng upload:', walletErr.message);
        return res.status(503).json({ success: false, message: 'Could not verify the Omni Tokens balance right now — please try syncing again in a moment.' });
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
    const totalMB = Math.round((sizeBytesForPrecheck / (1024 * 1024)) * 100) / 100;
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
        compressed,
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
    // AYOS/BAGO: para malaman kung MANUAL o AUTOMATIC (scheduled) na sync
    // ito — para sa pag-tag ng SYNC_CHARGE/SYNC_FRACTION entries sa
    // Transaction History. Ang OMNIPOS mismo ang nagpapasa nito
    // (performCloudBackupUpload trigger param) — sanitized dito, 'manual'
    // ang default kung hindi valid/wala.
    const trigger = (req.body && req.body.trigger === 'automatic') ? 'automatic' : 'manual';
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
        // AYOS/BAGO: kung na-flag bilang gzip-compressed ang upload session
        // na ito (mula sa upload/start — tingnan ang `compressed` doon),
        // i-decompress muna bago i-JSON.parse. Kung hindi naman naka-flag
        // (mas lumang client, walang gzip), plain JSON pa rin ang inaasahan
        // gaya ng dati — walang pagbabago sa kanilang behavior. Anumang
        // error dito (sirang gzip, corrupted JSON) ay nahuhuli pa rin ng
        // parehong catch block sa ibaba — walang bagong failure mode.
        //
        // AYOS/SECURITY FIX: `maxOutputLength` bilang proteksyon laban sa
        // "gzip/zip bomb" — isang maliit na compressed payload (pumasa sa
        // 5MB-per-chunk limit) na dinisenyo para mag-inflate sa sobrang
        // laki (GBs) kapag na-decompress, na maaaring maubos ang RAM ng
        // buong RELAY process at mag-crash ito PARA SA LAHAT ng stores —
        // mas malala pa ito kaysa sa dating event-loop-blocking na isyu,
        // dahil hindi na lang delay ang epekto kundi total outage. Ang
        // session.quotaMB (mula sa upload/start, batay sa NA-SUBSCRIBE-ANG
        // tier ng installation na ito — hindi client-controlled) ang
        // ginamit na batayan, na may 20% margin + 5MB buffer para sa JSON
        // structure overhead (keys, storeName, moduleNames, atbp.) — kaya
        // walang epekto ito sa mga lehitimong backup sa loob ng quota
        // nila. Kapag na-exceed, ERR_BUFFER_TOO_LARGE ang itatapon ng
        // Node zlib — hinuhuli ito sa ibaba at binibigyan ng malinaw na
        // 413 (kaysa sa generic na "corrupted" message).
        const maxDecompressedBytes = Math.ceil((Number(session.quotaMB) + 5) * 1.2 * 1024 * 1024);
        const rawBuffer = session.compressed ? await gunzipAsync(fullBuffer, { maxOutputLength: maxDecompressedBytes }) : fullBuffer;
        parsedBody = JSON.parse(rawBuffer.toString('utf8'));
    } catch (err) {
        cleanupCloudBackupUploadSession(String(uploadId));
        // AYOS/BAGO: hiwalay/mas malinaw na message kapag ang dahilan ng
        // pagkabigo ay ang bagong maxOutputLength guard sa itaas (tunay na
        // masyadong malaki ang na-decompress na laki kumpara sa quota ng
        // installation na ito), kaysa sa generic na "corrupted" message —
        // mas madali itong ma-diagnose kung sakaling matawag ang customer
        // support tungkol dito.
        if (err && err.code === 'ERR_BUFFER_TOO_LARGE') {
            logActivity(installationId, 'cloud_backup_blocked', { reason: 'decompressed_size_exceeded_quota', tier: session.tier, quotaMB: session.quotaMB });
            return res.status(413).json({
                success: false,
                storageQuotaExceeded: true,
                tier: session.tier,
                quotaMB: session.quotaMB,
                message: `The uploaded backup decompressed to more data than your ${CLOUD_BACKUP_PLANS[session.tier] ? CLOUD_BACKUP_PLANS[session.tier].name : session.tier} storage allowance (${session.quotaMB} MB limit) allows. Upgrade your Cloud Backup plan or free up space before syncing.`
            });
        }
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
    // ===================================================================
    // AYOS/SECURITY FIX: ito na ang TANGING tunay na gate ng Omni Tokens
    // para sa cloud backup sync. Dating ang pag-charge (check-and-consume)
    // ay ginagawa ng OMNIPOS (client-controlled, self-hosted) PAGKATAPOS
    // lang ng successful upload — ibig sabihin, kahit alisin/i-edit ng
    // isang client ang balance-check code sa sarili nilang OMNIPOS
    // server.js (o tawagin nila mismo, direkta, itong RELAY endpoint gamit
    // ang RELAY_API_KEY na makukuha nila sa sarili nilang .env), TALAGANG
    // masusulat pa rin ang data nila sa Neon nang walang bayad — walang
    // technical enforcement, "trust-based" lang.
    //
    // Ngayon, DITO MISMO — bago pa man magsimula ang totoong pagsulat sa
    // Postgres (cloud_backup_modules) sa ibaba — atomic na chine-check AT
    // sabay ding kina-consume ang Omni Token balance ng installation na
    // ito. RELAY (developer-owned, HINDI naa-access/hindi na-eedit ng
    // kliyente) ang tanging pinagmumulan ng katotohanan nito, kaya
    // WALANG paraan para makatakas ang sinumang client — anuman ang
    // dumaan (tunay na OMNIPOS, na-edit na OMNIPOS, o direktang HTTP call)
    // ay dadaan sa gate na ito bago ma-save ang kanilang backup.
    // ===================================================================
    let tokenConsumeResult;
    try {
        // AYOS/BUGFIX: `projectedSizeBytes` (ang AKTWAL na laki ng datos na
        // kasasama lang i-serialize sa itaas) ang ipinapasa ngayon bilang
        // batayan ng presyo — hindi na ang tier/maintenance fee.
        tokenConsumeResult = await consumeCloudTokensForSyncExact(installationId, projectedSizeBytes, tier, 'Cloud backup sync', trigger);
        invalidateWalletCache(installationId);
    } catch (consumeErr) {
        // AYOS: fail-CLOSED — kung nabigo ang atomic charge mismo (hal.
        // transient Postgres error), HUWAG ituloy ang pagsulat ng backup
        // (hindi natin alam kung na-charge nga o hindi). I-cleanup ang
        // session at sabihin sa client na mag-retry na lang.
        cleanupCloudBackupUploadSession(String(uploadId));
        console.error('⚠️ CLOUD_BACKUP: hindi ma-verify/ma-charge ang Omni Token balance sa upload/finish:', consumeErr.message);
        return res.status(503).json({ success: false, message: 'Could not verify the Omni Tokens balance right now — no data was written. Please try syncing again in a moment.' });
    }
    if (!tokenConsumeResult.ok) {
        cleanupCloudBackupUploadSession(String(uploadId));
        logActivity(installationId, 'cloud_backup_blocked', { reason: 'insufficient_tokens', balanceTokens: tokenConsumeResult.balanceTokens });
        return res.status(402).json({
            success: false,
            insufficientTokens: true,
            balanceTokens: tokenConsumeResult.balanceTokens,
            message: 'Insufficient Cloud Backup (Omni Tokens) balance. Nothing was written to the cloud — please buy more tokens to keep syncing.'
        });
    }
    // AYOS/BAGO: itatala ang timestamp bago magsimula ang aktwal na
    // pagsulat sa Neon Postgres, para masukat kalaunan (kapag successful)
    // kung gaano talaga katagal ang isang sync na may ganitong laki ng
    // datos — tingnan ang recordCloudBackupSyncTiming() sa itaas.
    const writeStartedAtMs = Date.now();
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
            // Kung may na-charge na WHOLE token para sa sync na ito pero
            // nabigo pala ang pagsulat, ibalik/i-refund — patuloy na dapat
            // TANGING successful sync lang ang binabayaran, kahit ngayong
            // atomic na ang gate.
            if (tokenConsumeResult.tokensCharged > 0) {
                await creditCloudTokens(installationId, tokenConsumeResult.tokensCharged, `Refund — failed sync (${failedList})`, 'REFUND').catch((refundErr) => {
                    console.error('⚠️ CLOUD_TOKENS: failed to refund after a failed sync write:', refundErr.message);
                });
            }
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
        // AYOS/BAGO: TAPOS na ang buong pagsulat sa Postgres (successful,
        // hindi kasama ang mga naunang gate/checks) — dito lang natin
        // masusukat ang AKTWAL na tagal ng compute na ito. Idinaragdag ito
        // bilang bagong totoong sample sa self-calibrating na estimate;
        // kapag umabot na sa minimum sample count, agad na epektibo ang
        // na-recalibrate na assumedBaseSeconds/assumedSecondsPerMB sa
        // SUSUNOD na sync na magbabayad ng tokens (hindi na kailangan ng
        // manual na admin review).
        const writeDurationSeconds = Math.max(0, (Date.now() - writeStartedAtMs) / 1000);
        recordCloudBackupSyncTiming(projectedSizeMB, writeDurationSeconds);
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
            nearQuota,
            tokensCharged: tokenConsumeResult.tokensCharged,
            balanceTokens: tokenConsumeResult.balanceTokens
        });
    } catch (err) {
        console.error('⚠️ CLOUD_BACKUP: hindi na-save sa Postgres:', err.message);
        const friendlyMessage = isTransientPgConnectionError(err)
            ? 'Lost connection to the database while saving the cloud backup (this can happen with very large uploads). Please try syncing again — no partial data was saved.'
            : ('An error occurred while saving to Postgres: ' + err.message);
        if (tokenConsumeResult.tokensCharged > 0) {
            await creditCloudTokens(installationId, tokenConsumeResult.tokensCharged, `Refund — failed sync (${err.message})`, 'REFUND').catch((refundErr) => {
                console.error('⚠️ CLOUD_TOKENS: failed to refund after a failed sync write:', refundErr.message);
            });
        }
        res.status(500).json({ success: false, message: friendlyMessage });
    } finally {
        cleanupCloudBackupUploadSession(String(uploadId));
    }
});
app.get('/relay/cloud-backup/usage', requireApiKey, requireAllowedDevice, rateLimit('cloud-backup-usage', 60, 60 * 60 * 1000, (req) => req.query?.installationId), async (req, res) => {
    const installationId = String(req.query.installationId || '').trim();
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
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
// ===================================================================
// CLOUD BACKUP TOKENS — mga endpoint na ginagamit ng bagong
// "Omni Tokens" admin-only page sa OMNIPOS (Google App Verification +
// token/diamond wallet). Lahat ng balance/ledger ay nakatira DITO sa
// RELAY (hindi sa OMNIPOS/kliyente) para hindi ito ma-tamper — ang
// OMNIPOS lang ang basta magta-trigger ng purchase/check-and-consume.
// ===================================================================
app.get('/relay/cloud-tokens/packages', requireApiKey, rateLimit('cloud-tokens-packages', 120, 60 * 60 * 1000), async (req, res) => {
    const packages = getCloudTokenPackages();
    const tokenCostPerSync = {};
    // AYOS: bukod sa dating rounded-up (ceil) na estimate, ibinabalik na rin
    // dito ang EKSAKTONG (fractional) na presyo kada tier — mula sa parehong
    // getCloudTokenCostPerSyncExact() na siya ring ginagamit sa totoong
    // pag-charge (consumeCloudTokensForSyncExact). Ito ang dapat ipakita sa
    // customer bilang "totoong" presyo kada sync (average), hindi na yung
    // paitaas-palaging ceil na estimate lang.
    // AYOS/BUGFIX: ito ay isang GENERIC na catalog view (walang partikular
    // na installationId dito, kaya walang aktwal na laki ng datos na
    // mapagbabatayan) — gumagamit na lang ito ng CLOUD_BACKUP_SAMPLE_SIZE_BYTES_FOR_CATALOG
    // (5MB, ilustratibong halimbawa lang) bilang batayan, HINDI na ang
    // maintenance fee ng tier. Ang totoong/eksaktong presyo kada sync ng
    // isang partikular na installation (batay sa aktwal nilang laki ng
    // datos) ay nasa /relay/cloud-tokens/wallet.
    const tokenCostPerSyncExact = {};
    // AYOS/BAGO: kaparehong sample-size-based na catalog view, pero para sa
    // RESTORE (tingnan ang computeRealCloudBackupRestoreCostPHP() sa itaas)
    // — para makita ng customer, kahit bago pa lang mag-subscribe, kung
    // gaano karaming extra tokens ang dapat panatilihing "reserve" kung
    // sakaling kailanganin nilang mag-restore (rare/disaster-recovery lang,
    // kaya hindi ito pinaparami kada buwan/taon tulad ng sync — isang
    // beses lang ang tinatantya, tingnan ang estRestoreTokensPerRestore sa
    // ibaba).
    const tokenCostPerRestore = {};
    const tokenCostPerRestoreExact = {};
    for (const tier of Object.keys(CLOUD_BACKUP_PLANS)) {
        tokenCostPerSync[tier] = await getCloudTokenCostPerSync(CLOUD_BACKUP_SAMPLE_SIZE_BYTES_FOR_CATALOG, tier);
        tokenCostPerSyncExact[tier] = await getCloudTokenCostPerSyncExact(CLOUD_BACKUP_SAMPLE_SIZE_BYTES_FOR_CATALOG, tier);
        tokenCostPerRestore[tier] = await getCloudTokenCostPerRestore(CLOUD_BACKUP_SAMPLE_SIZE_BYTES_FOR_CATALOG, tier);
        tokenCostPerRestoreExact[tier] = await getCloudTokenCostPerRestoreExact(CLOUD_BACKUP_SAMPLE_SIZE_BYTES_FOR_CATALOG, tier);
    }
    // UPDATE: the package cards used to show "Est. sync cost (1 mo.)" as a
    // plain copy of the tier's monthly price (maintenanceFeeTokens ===
    // estSyncTokensPerMonth by design), so it moved in lockstep with the
    // maintenance fee any time the price was edited in the pricing admin.
    // It is now decoupled: recomputed here from the same real,
    // data-size + Neon-rate based formula used by the wallet endpoint
    // (getCloudTokenCostPerSyncExact), scaled by how many auto-syncs the
    // tier is expected to run in a month. This is still an illustrative
    // estimate (it uses CLOUD_BACKUP_SAMPLE_SIZE_BYTES_FOR_CATALOG as a
    // stand-in size, since there is no real installation here yet), but
    // it no longer tracks the maintenance fee — only the tier's sync
    // frequency and the configured Neon rate affect it now. A yearly
    // estimate (12 renewals + 12 months of syncs) is also added so the
    // customer can see roughly how much extra balance to keep on top of
    // the maintenance fee for a full month or a full year.
    for (const tier of Object.keys(packages)) {
        const pkg = packages[tier];
        const planTier = pkg.baseTier || pkg.tier;
        const plan = CLOUD_BACKUP_PLANS[planTier];
        if (!plan || typeof pkg.maintenanceFeeTokens !== 'number') continue;
        const expectedSyncsPerMonth = Math.max(1, Math.round((30 * 24 * 60 * 60 * 1000) / plan.autoBackupIntervalMs));
        const perSyncCostExact = tokenCostPerSyncExact[planTier];
        const estSyncTokensPerMonth = Math.max(1, Math.ceil(perSyncCostExact * expectedSyncsPerMonth));
        const estSyncTokensPerYear = Math.max(1, Math.ceil(perSyncCostExact * expectedSyncsPerMonth * 12));
        pkg.estSyncTokensPerMonth = estSyncTokensPerMonth;
        pkg.estSyncTokensPerYear = estSyncTokensPerYear;
        pkg.estTotalMonthlyTokens = pkg.maintenanceFeeTokens + estSyncTokensPerMonth;
        // FIX: the yearly maintenance fee is NOT 12 monthly renewals —
        // plan.price.yearly is the tier's actual configured yearly price in
        // the RELAY pricing admin, which is already discounted (e.g. ~2
        // months off vs. paying monthly 12 times). Using
        // maintenanceFeeTokens * 12 here overstated the yearly maintenance
        // cost and ignored that discount. Falls back to *12 only if a
        // tier somehow has no yearly price configured at all.
        const maintenanceFeeTokensYearly = (typeof plan.price.yearly === 'number') ? plan.price.yearly : pkg.maintenanceFeeTokens * 12;
        pkg.maintenanceFeeTokensYearly = maintenanceFeeTokensYearly;
        pkg.estTotalYearlyTokens = maintenanceFeeTokensYearly + estSyncTokensPerYear;
        // "Recommended extra balance" = the sync-cost portion only (the
        // maintenance fee itself is already covered by whichever package
        // is purchased) — this is the number customers actually need to
        // keep as spare balance for auto-sync/manual backup to keep
        // working across the period.
        pkg.recommendedExtraBalanceMonthly = estSyncTokensPerMonth;
        pkg.recommendedExtraBalanceYearly = estSyncTokensPerYear;
        // AYOS/BAGO: restore cost reference (per-request, hindi bahagi ng
        // sinusundan/recurring na "recommended extra balance" sa itaas — ang
        // restore ay hindi nangyayari on a schedule, kaya walang tamang
        // "per month" o "per year" na bilang para dito). Isang beses lang
        // ang tinatantya (estRestoreTokensPerRestore) gamit ang parehong
        // CLOUD_BACKUP_SAMPLE_SIZE_BYTES_FOR_CATALOG na sample size sa
        // itaas. Idinagdag din ang "kung sakaling kailanganin ng isang
        // restore" na variant ng monthly/yearly total (...WithOneRestore)
        // para makita agad ng customer, sa parehong monthly/yearly
        // breakdown, kung magkano ang dapat panatilihing reserve kung sa
        // palagay nila malamang kakailanganin nila ng restore sa loob ng
        // period na iyon — HINDI ito otomatikong idinadagdag sa
        // estTotalMonthlyTokens/estTotalYearlyTokens mismo (hindi
        // guaranteed na mangyayari ang isang restore), purely reference lang.
        pkg.estRestoreTokensPerRestore = tokenCostPerRestore[planTier];
        pkg.estRestoreTokensPerRestoreExact = Math.round(tokenCostPerRestoreExact[planTier] * 1000) / 1000;
        pkg.estTotalMonthlyTokensWithOneRestore = pkg.estTotalMonthlyTokens + pkg.estRestoreTokensPerRestore;
        pkg.estTotalYearlyTokensWithOneRestore = pkg.estTotalYearlyTokens + pkg.estRestoreTokensPerRestore;
    }
    // AYOS: dating hard-coded (GCash/Maya/Online Banking) ang select sa
    // OMNIPOS. Ngayon, ibinabalik dito ang paymentMethods — LISTAHAN NG MGA
    // PARAAN NG BAYAD NA TALAGANG NAKA-CONFIGURE (env vars) sa relay
    // ngayon — ito mismo ang gagamitin ng OMNIPOS para buuin ang dropdown,
    // kaya kung ano lang ang naka-set sa Render env, iyon lang ang
    // lalabas/mapipili.
    const paymentMethods = getAvailablePaymentMethods();
    res.json({
        success: true,
        packages,
        tokenCostPerSync,
        tokenCostPerSyncExact,
        tokenCostPerRestore,
        tokenCostPerRestoreExact,
        sampleSizeBytesUsed: CLOUD_BACKUP_SAMPLE_SIZE_BYTES_FOR_CATALOG,
        tokensPerPeso: 1,
        paymentMethods,
        // NOTE: estSyncTokensPerMonth/Year, estTotalMonthlyTokens/YearlyTokens,
        // recommendedExtraBalanceMonthly/Yearly, estRestoreTokensPerRestore(Exact),
        // and estTotalMonthly/YearlyTokensWithOneRestore on each package are
        // approximate estimates based on a sample backup size — not an
        // exact forecast. Actual monthly/yearly usage will vary with the
        // real backup size and the account's real sync frequency. The
        // restore figures are a reference only (restores are rare/unscheduled
        // events, not a recurring monthly/yearly cost) — they are NOT
        // included in estTotalMonthlyTokens/estTotalYearlyTokens by default.
        estimateDisclaimer: 'Sync-cost, monthly, and yearly figures are approximate estimates based on a sample backup size — not a guaranteed final cost. Restore-cost figures are a reference only, in case a restore is needed; restores are not scheduled/recurring, so they are shown separately from — and not included in — the monthly/yearly totals.'
    });
});
// ===================================================================
// AYOS/BAGO: reusable helper — pinagsasama (UNION) ang cloud_token_ledger
// (totoong balance movements: TOKEN_PURCHASE, FEATURE_UNLOCK,
// ADDON_PURCHASE, SYNC_CHARGE, REFUND) at ang cloud_sync_activity
// (SYNC_FRACTION — fractional-cost auto/manual syncs na wala pang
// buong token na na-deduct) papunta sa IISANG chronological na listahan.
// Ginagamit ito PAREHO ng (1) ang maliit na "Recent Activity" widget sa
// Omni Tokens page (default 5, walang filter) at (2) ang bagong
// Transaction History modal (may category + date-range filter +
// pagination).
// ===================================================================
async function fetchMergedTransactionHistory(installationId, { category = null, dateFrom = null, dateTo = null, limit = 5, offset = 0 } = {}) {
    const params = [installationId, category || null, dateFrom || null, dateTo || null, limit, offset];
    const sql = `
        SELECT * FROM (
            SELECT id,
                   -- BUGFIX: dating "category" column lang ang basta ginagamit
                   -- dito, pero ang column na ito ay hindi pa umiiral noon
                   -- (ADD COLUMN IF NOT EXISTS category — tingnan sa taas) —
                   -- kaya lahat ng row na na-insert BAGO idinagdag ang
                   -- category tracking ay NULL ang category sa DB. Dahil
                   -- "category = $2" ay HINDI kailanman nagta-TRUE laban sa
                   -- NULL sa SQL, dati itong mga LUMANG entry ay TULULOY
                   -- nakikita sa "All categories" (walang filter) pero
                   -- NAWAWALA/blangko kapag pumili ng KAHIT ANONG specific
                   -- na category — kahit pa malinaw namang kabilang sila
                   -- doon (hal. isang "Cloud Backup activation" entry na
                   -- dapat makita sa "Feature Activation" filter). Ang
                   -- COALESCE dito ay nag-iinfer ng category PARA LANG SA
                   -- mga NULL na lumang row (walang epekto sa mga bagong row
                   -- na may tamang category na), gamit ang parehong
                   -- type/note na signal na ginagamit din ng frontend bilang
                   -- fallback (tingnan ang CT_CATEGORY_META sa app.js) —
                   -- REFUND muna bago ang generic type='purchase' dahil
                   -- pareho itong 'purchase' ang type sa creditCloudTokens().
                   COALESCE(
                       category,
                       CASE
                           WHEN type = 'purchase' AND note ILIKE 'Refund%' THEN 'REFUND'
                           WHEN type = 'purchase' THEN 'TOKEN_PURCHASE'
                           WHEN note ILIKE 'Cloud Backup activation%' THEN 'FEATURE_UNLOCK'
                           WHEN note ILIKE 'Purchase —%' THEN 'ADDON_PURCHASE'
                           ELSE NULL
                       END
                   ) AS category,
                   type, tokens, balance_after, note, trigger_type, created_at
            FROM cloud_token_ledger
            WHERE installation_id = $1
            UNION ALL
            SELECT id, 'SYNC_FRACTION' AS category, 'consume' AS type, (-cost_tokens) AS tokens,
                   NULL AS balance_after,
                   ('Auto-sync activity — cost ~' || ROUND(cost_tokens::numeric, 3) || ' token, accrued so far: ' || ROUND(accrued_after::numeric, 3) || '/1.0 token') AS note,
                   trigger_type, created_at
            FROM cloud_sync_activity
            WHERE installation_id = $1
        ) combined
        WHERE ($2::text IS NULL OR category = $2)
          AND ($3::timestamptz IS NULL OR created_at >= $3)
          AND ($4::timestamptz IS NULL OR created_at <= $4)
        ORDER BY created_at DESC
        LIMIT $5 OFFSET $6
    `;
    const result = await queryWithRetry(pgPool, sql, params);
    return result.rows;
}
app.get('/relay/cloud-tokens/wallet', requireApiKey, requireAllowedDevice, rateLimit('cloud-tokens-wallet', 120, 60 * 60 * 1000, (req) => req.query?.installationId), async (req, res) => {
    const installationId = String(req.query.installationId || '').trim();
    if (!installationId) return res.status(400).json({ success: false, message: 'Missing installationId.' });
    if (!pgPool) return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured.' });
    const cached = getWalletCache(installationId);
    if (cached) return res.json(cached);
    try {
        const wallet = await getOrCreateCloudTokenWallet(installationId);
        // AYOS/BUGFIX: 5 na lang ang default (dating 20) — mas mabilis at
        // mas magaan sa data, dahil ang customer ay LATEST activity lang
        // talaga ang gustong makita dito. Ang buong history (kasama ang
        // fractional na auto-sync entries) ay makikita sa Transaction
        // History modal (/relay/cloud-tokens/transaction-history sa
        // ibaba), na may filter at pagination.
        const ledgerResult = { rows: await fetchMergedTransactionHistory(installationId, { limit: 5 }) };
        const pendingResult = await queryWithRetry(
            pgPool,
            `SELECT purchase_id, package_id, tokens, amount_php, method, status, created_at FROM cloud_token_purchases
             WHERE installation_id = $1 AND status = 'pending' ORDER BY created_at DESC LIMIT 5`,
            [installationId]
        );
        // AYOS/BUGFIX: idinagdag ang TOTOONG (data-size-based, hindi na
        // maintenance-fee-based) na presyo kada sync PARA SA PARTIKULAR NA
        // INSTALLATION NA ITO — batay sa kanyang huling kilalang aktwal na
        // laki ng backup (cloud_backup_meta.size_bytes). Kung wala pa itong
        // naitatalang backup (bagong subscriber), gamitin muna ang 5MB na
        // ilustratibong sample bilang panandaliang estimate — mapapalitan
        // na ito ng totoong laki sa unang successful sync.
        const metaSizeResult = await queryWithRetry(
            pgPool,
            'SELECT size_bytes FROM cloud_backup_meta WHERE installation_id = $1',
            [installationId]
        );
        const knownSizeBytes = metaSizeResult.rows[0] ? Number(metaSizeResult.rows[0].size_bytes) || 0 : 0;
        const sizeBytesForRealCost = knownSizeBytes > 0 ? knownSizeBytes : CLOUD_BACKUP_SAMPLE_SIZE_BYTES_FOR_CATALOG;
        const cloudBackupUnlockForWallet = (issuedUnlocks[installationId] || {})['cloud_backup'];
        const tierForWallet = (cloudBackupUnlockForWallet && cloudBackupUnlockForWallet.tier && CLOUD_BACKUP_PLANS[cloudBackupUnlockForWallet.tier]) ? cloudBackupUnlockForWallet.tier : 'basic';
        const realSyncCostTokensExact = Math.round((await getCloudTokenCostPerSyncExact(sizeBytesForRealCost, tierForWallet)) * 1000) / 1000;
        const realSyncCostTokens = await getCloudTokenCostPerSync(sizeBytesForRealCost, tierForWallet);
        // AYOS/BAGO: kaparehong estimate pero para sa RESTORE (tingnan ang
        // computeRealCloudBackupRestoreCostPHP() sa itaas) — magkaibang
        // pricing model kaysa sync (Instant Restore rate, hindi prorated sa
        // "expected operations per month"), kaya hiwalay na field ito.
        // Ginagamit ito ng OMNIPOS bilang pre-check estimate bago pa man
        // subukan ang aktwal na restore (na siyang TALAGANG nagcha-charge,
        // sa RELAY /relay/cloud-backup/restore).
        const realRestoreCostTokensExact = Math.round((await getCloudTokenCostPerRestoreExact(sizeBytesForRealCost, tierForWallet)) * 1000) / 1000;
        const realRestoreCostTokens = await getCloudTokenCostPerRestore(sizeBytesForRealCost, tierForWallet);
        const responseBody = {
            success: true,
            balanceTokens: Number(wallet.balance_tokens),
            ledger: ledgerResult.rows,
            pendingPurchases: pendingResult.rows,
            realSyncCostTokens,
            realSyncCostTokensExact,
            realSyncCostBasedOnKnownSize: knownSizeBytes > 0,
            realSyncCostSizeBytesUsed: sizeBytesForRealCost,
            realRestoreCostTokens,
            realRestoreCostTokensExact,
            realRestoreCostBasedOnKnownSize: knownSizeBytes > 0,
            realRestoreCostSizeBytesUsed: sizeBytesForRealCost
        };
        setWalletCache(installationId, responseBody);
        res.json(responseBody);
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});
// AYOS/BAGO: single source of truth ng category list — ginagamit ito ng
// OMNIPOS frontend para i-populate ang filter dropdown sa Transaction
// History modal, para kung magdagdag/magbago man ng category dito sa
// RELAY, hindi na kailangang i-edit ulit nang hiwalay sa OMNIPOS.
app.get('/relay/cloud-tokens/transaction-categories', requireApiKey, (req, res) => {
    res.json({
        success: true,
        categories: Object.entries(CLOUD_TOKEN_LEDGER_CATEGORIES).map(([value, label]) => ({ value, label }))
    });
});
// AYOS/BAGO: buong Transaction History — may filter (category, date
// range) at pagination (limit/offset), hindi tulad ng maliit na
// "Recent Activity" widget (5 lang, walang filter) sa /wallet endpoint
// sa itaas. Parehong galing ito sa fetchMergedTransactionHistory() —
// pinagsama ang totoong balance movements (cloud_token_ledger) at ang
// fractional na sync visibility entries (cloud_sync_activity).
app.get('/relay/cloud-tokens/transaction-history', requireApiKey, requireAllowedDevice, rateLimit('cloud-tokens-transaction-history', 120, 60 * 60 * 1000, (req) => req.query?.installationId), async (req, res) => {
    const installationId = String(req.query.installationId || '').trim();
    if (!installationId) return res.status(400).json({ success: false, message: 'Missing installationId.' });
    if (!pgPool) return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured.' });
    const rawCategory = String(req.query.category || '').trim().toUpperCase();
    const category = (rawCategory && rawCategory !== 'ALL' && CLOUD_TOKEN_LEDGER_CATEGORIES[rawCategory]) ? rawCategory : null;
    // dateFrom/dateTo: inaasahang ISO date/datetime strings (hal.
    // "2026-08-01" o "2026-08-01T00:00:00Z") — pinapasa lang ito nang
    // direkta sa Postgres timestamptz cast, kaya kahit anong valid ISO
    // format ay tatanggapin.
    const dateFrom = req.query.dateFrom ? String(req.query.dateFrom).trim() : null;
    const dateTo = req.query.dateTo ? String(req.query.dateTo).trim() : null;
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit, 10) || 20));
    const offset = Math.max(0, parseInt(req.query.offset, 10) || 0);
    try {
        const rows = await fetchMergedTransactionHistory(installationId, { category, dateFrom, dateTo, limit, offset });
        res.json({ success: true, transactions: rows, limit, offset, hasMore: rows.length === limit });
    } catch (err) {
        res.status(400).json({ success: false, message: `Invalid filter or query error: ${err.message}` });
    }
});
app.post('/relay/cloud-tokens/purchase/create', requireApiKey, requireAllowedDevice, rateLimit('cloud-tokens-purchase-create', 20, 60 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
    const { installationId, packageId, customTokens, method, returnBaseUrl } = req.body;
    if (!installationId) return res.status(400).json({ success: false, message: 'Missing installationId.' });
    if (!pgPool) return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured.' });
    // AYOS: dating PayMongo lang ang tinatanggap dito. Ngayon, tinitignan
    // sa PAYMENT_METHOD_CATALOG/PAYMENT_PROVIDERS_CONFIGURED (env-based)
    // kung valid AT available ang hiniling na method — anumang provider
    // ang nasa likod nito (PayMongo, Xendit, Stripe, PayPal, ...).
    if (!isPaymentMethodAvailable(method)) {
        const available = getAvailablePaymentMethods().map((m) => m.label).join(', ') || 'no payment method configured yet';
        return res.status(400).json({ success: false, message: `This payment method is invalid or unavailable. Currently available: ${available}.` });
    }
    let tokens;
    let resolvedPackageId = null;
    if (packageId && getCloudTokenPackages()[packageId]) {
        const pkg = getCloudTokenPackages()[packageId];
        tokens = pkg.tokens;
        resolvedPackageId = packageId;
    } else if (typeof customTokens === 'number' && customTokens >= 50) {
        tokens = Math.round(customTokens);
    } else {
        return res.status(400).json({ success: false, message: 'Please choose a Basic/Standard/Pro package, or enter a custom amount (minimum 50 tokens/₱50).' });
    }
    const amountPHP = tokens; 
    const purchaseId = `TKN-${installationId.slice(0, 8)}-${Date.now()}-${crypto.randomBytes(3).toString('hex')}`;
    const base = (returnBaseUrl && /^https?:\/\//.test(returnBaseUrl)) ? returnBaseUrl.replace(/\/$/, '') : `${req.protocol}://${req.get('host')}`;
    try {
        const checkout = await createPaymentCheckout({
            method,
            amountPHP,
            purchaseId,
            base,
            description: `OmniPOS Cloud Backup Tokens (${tokens} token/s)`
        });
        await queryWithRetry(
            pgPool,
            `INSERT INTO cloud_token_purchases (purchase_id, installation_id, package_id, tokens, amount_php, method, source_id, provider, status)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending')`,
            [purchaseId, installationId, resolvedPackageId, tokens, amountPHP, method, checkout.providerRefId, checkout.provider]
        );
        logActivity(installationId, 'cloud_token_purchase_created', { purchaseId, tokens, amountPHP, method, provider: checkout.provider });
        // AYOS: idinagdag ang qrCodeImageUrl/expiresAt para sa QR Ph (walang
        // checkoutUrl ito — larawan ng QR ang ipapakita sa OMNIPOS sa halip
        // na mag-open ng bagong tab). Null lang ito sa ibang method/provider.
        res.json({ success: true, purchaseId, checkoutUrl: checkout.checkoutUrl || null, qrCodeImageUrl: checkout.qrCodeImageUrl || null, expiresAt: checkout.expiresAt || null, tokens, amountPHP });
    } catch (err) {
        const isConfigErr = err.code === 'PAYMONGO_NOT_CONFIGURED' || err.code === 'XENDIT_NOT_CONFIGURED' || err.code === 'STRIPE_NOT_CONFIGURED' || err.code === 'PAYPAL_NOT_CONFIGURED' || err.code === 'DRAGONPAY_NOT_CONFIGURED' || err.code === 'PROVIDER_NOT_CONFIGURED';
        res.status(isConfigErr ? 503 : 502).json({ success: false, message: err.message });
    }
});
// Simpleng "processing" page na binabalikan ng checkout ng kahit anong
// naka-configure na provider (PayMongo/Xendit/Stripe/PayPal) pagkatapos
// magbayad ang customer — ang TUNAY na pagcredit ng tokens ay nasa
// webhook lang (mas mapagkakatiwalaan, hindi client-side redirect). Dito
// lang ito nagpapakita ng malinaw na mensahe at nagsasabing balikan na
// lang ang OMNIPOS admin panel.
app.get('/relay/cloud-tokens/return', (req, res) => {
    const result = req.query.result === 'success' ? 'success' : 'failed';
    const title = result === 'success' ? 'Payment Received' : 'Payment Not Completed';
    const message = result === 'success'
        ? 'Salamat! Kinukumpirma pa ng system ang bayad na ito — makikita ang updated token balance sa OmniPOS admin panel (Omni Tokens page) sa loob ng ilang segundo hanggang isang minuto.'
        : 'Hindi natapos o kinansela ang bayad na ito. Wala pang na-deduct/na-charge. Pwede mo ulit subukan sa OmniPOS admin panel (Omni Tokens page).';
    res.set('Content-Type', 'text/html; charset=utf-8').send(`<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>${title}</title>
<style>body{font-family:system-ui,-apple-system,sans-serif;background:#0f172a;color:#e2e8f0;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;padding:24px;text-align:center;}
.card{max-width:420px;background:#1e293b;border-radius:16px;padding:32px 24px;box-shadow:0 10px 30px rgba(0,0,0,.3);}
h1{font-size:1.25rem;margin:0 0 12px;color:${result === 'success' ? '#4ade80' : '#f87171'};}
p{line-height:1.5;color:#cbd5e1;}</style></head>
<body><div class="card"><h1>${result === 'success' ? '✅' : '⚠️'} ${title}</h1><p>${message}</p></div></body></html>`);
});
app.post('/relay/webhooks/paymongo', express.raw({ type: '*/*', limit: '1mb' }), async (req, res) => {
    const rawBody = req.body ? req.body.toString('utf8') : '';
    const signatureHeader = req.headers['paymongo-signature'];
    if (!verifyPaymongoSignature(rawBody, signatureHeader)) {
        console.error('⚠️  PayMongo webhook: invalid/missing signature — tinanggihan.');
        return res.status(400).json({ success: false, message: 'Invalid webhook signature.' });
    }
    let event;
    try {
        event = JSON.parse(rawBody);
    } catch (err) {
        return res.status(400).json({ success: false, message: 'Invalid JSON.' });
    }
    const eventType = event && event.data && event.data.attributes && event.data.attributes.type;
    const resource = event && event.data && event.data.attributes && event.data.attributes.data;
    try {
        if (eventType === 'source.chargeable' && resource && resource.id) {
            const sourceId = resource.id;
            const purchaseResult = await queryWithRetry(
                pgPool,
                `SELECT * FROM cloud_token_purchases WHERE source_id = $1 AND status = 'pending'`,
                [sourceId]
            );
            const purchase = purchaseResult.rows[0];
            if (!purchase) {
                return res.json({ success: true, ignored: true, message: 'No matching pending purchase for this source.' });
            }
            const payment = await paymongoCreatePayment({
                sourceId,
                amountPHP: Number(purchase.amount_php),
                description: `OmniPOS Cloud Backup Tokens (${purchase.tokens} token/s)`
            });
            const paymentStatus = payment && payment.attributes && payment.attributes.status;
            if (paymentStatus === 'paid') {
                // BUGFIX: dating SELECT ('pending') -> UPDATE ('paid') -> creditCloudTokens
                // ang pagkakasunod-sunod dito, na HINDI atomic — kung dalawang magkatabing
                // pagtawag sa webhook na ito (duplicate delivery/retry ng PayMongo, o
                // dalawang sabay na request) ay parehong nakabasa ng status='pending' BAGO
                // pa man matapos ang UNANG UPDATE, doble ang pagcredit ng tokens. Ginagawa
                // muna dito ang atomic na "claim" (UPDATE ... WHERE status = 'pending') at
                // credit lang kapag TALAGANG tayo ang nakaclaim (may row na nabago).
                const claimResult = await queryWithRetry(pgPool, `UPDATE cloud_token_purchases SET status = 'paid', updated_at = now() WHERE purchase_id = $1 AND status = 'pending' RETURNING *`, [purchase.purchase_id]);
                if (!claimResult.rows[0]) {
                    // Naunahan na tayo ng ibang concurrent/duplicate delivery — wala nang
                    // dapat gawin dito, huwag nang mag-double-credit.
                    return res.json({ success: true, alreadyProcessed: true });
                }
                const newBalance = await creditCloudTokens(purchase.installation_id, Number(purchase.tokens), `Purchase ${purchase.purchase_id} (${purchase.method})`);
                logActivity(purchase.installation_id, 'cloud_token_purchase_paid', { purchaseId: purchase.purchase_id, tokens: Number(purchase.tokens), newBalance });
                sendTelegramNotification(`💎 Omni Tokens bought: ${purchase.tokens} tokens (₱${purchase.amount_php}) via ${purchase.method} — installation ${purchase.installation_id.slice(0, 12)}... New balance: ${newBalance}`).catch(() => {});
            } else {
                await queryWithRetry(pgPool, `UPDATE cloud_token_purchases SET status = 'failed', updated_at = now() WHERE purchase_id = $1 AND status = 'pending'`, [purchase.purchase_id]);
            }
        } else if (eventType === 'payment.paid' && resource && resource.attributes && resource.attributes.source && resource.attributes.source.type === 'qrph') {
            // BUGFIX: dating ang guard dito ay "!(source present)" — MALI ito.
            // Na-verify sa opisyal na PayMongo webhook payload samples na ang
            // payment.paid event PARA MISMO SA QR PH ay MAY "source" object
            // (type: "qrph", id: "qrph_xxx") — kaya kabaligtaran ang tamang
            // guard: dapat "source.type === 'qrph'" (hindi absence of source).
            // Dahil dito, HINDI TALAGA GUMAGANA ang naunang bersyon ng branch
            // na ito — walang QR Ph payment na na-credit sana kahit successful
            // na ang bayad sa PayMongo. Ang GCash/Maya/DOB (Sources flow) ay
            // hindi naaapektuhan nito dahil iba ang source.type nila
            // ('gcash'/'paymaya'/'dob'), at may sarili na silang
            // "source.chargeable" branch sa itaas.
            await creditPaidPaymentIntent(resource.attributes.payment_intent_id, { failed: false });
        } else if (eventType === 'payment_intent.succeeded' && resource && resource.id) {
            // Opsyonal na redundant na daan patungo sa parehong resulta sa
            // itaas — sa ilang PayMongo webhook subscription, ang
            // "payment_intent.succeeded" (ang Payment Intent mismo ang
            // resource) ang dumarating sa halip na/kasabay ng "payment.paid".
            // Ligtas itong tawagin nang dalawang beses (idempotent — atomic
            // "claim" na lang ang gagana sa unang tumama, tingnan
            // creditPaidPaymentIntent sa itaas).
            await creditPaidPaymentIntent(resource.id, { failed: false });
        } else if (eventType === 'payment.failed' && resource && resource.attributes) {
            // BUGFIX: para sa QR Ph, ang resource.attributes.source.id (kung
            // meron man) ay "qrph_xxx" — HINDI ang Payment Intent ID ("pi_xxx")
            // na siya namang naka-imbak bilang source_id column ng purchase
            // na ito. Kaya kailangang tignan muna kung QR Ph ba ito
            // (source.type === 'qrph') bago piliin kung alin ang gagamiting ID
            // pang-match — dating palaging source.id muna ang priority, na
            // sasablay para dito.
            const isQrPh = resource.attributes.source && resource.attributes.source.type === 'qrph';
            const sourceId = isQrPh
                ? resource.attributes.payment_intent_id
                : ((resource.attributes.source && resource.attributes.source.id) || resource.attributes.payment_intent_id);
            if (sourceId) {
                await queryWithRetry(pgPool, `UPDATE cloud_token_purchases SET status = 'failed', updated_at = now() WHERE source_id = $1 AND status = 'pending'`, [sourceId]);
            }
        } else if (eventType === 'payment_intent.awaiting_payment_method' && resource && resource.id) {
            // BUGFIX: dating "payment_intent.payment_failed" ang pinakikinggan
            // dito — HINDI ito totoong event type ng PayMongo (Stripe naming
            // convention ito, na-guess lang, hindi na-verify). Ang opisyal at
            // dokumentadong event na ibinabalik ni PayMongo kapag nag-expire o
            // nabigo ang isang QR Ph code (nagbabalik ang Payment Intent sa
            // "awaiting_payment_method") ay "payment_intent.awaiting_payment_method"
            // — kaya pinalitan ito para talagang ma-mark na 'failed' ang
            // purchase at makapag-retry ang customer sa OMNIPOS.
            //
            // KARAGDAGANG SAFEGUARD: "awaiting_payment_method" din mismo ang
            // PAUNANG status ng bawat BAGONG Payment Intent bago pa man
            // i-attach (kumpirmado sa opisyal na PayMongo docs) — kaya kung
            // sakaling ipinapadala rin pala ng PayMongo ang event na ito sa
            // mismong paglikha (hindi lang sa pag-"return" dahil sa
            // failed/expired na attempt), mali agad na ma-mamarkahang
            // "failed" ang BAWAT QR Ph purchase pagkatapos lang itong gawin.
            // Kaya dito, tinitignan muna kung may "last_payment_error" —
            // ang mismong field na sinasabi ng opisyal na PayMongo
            // troubleshooting docs bilang palatandaan ng TUNAY na
            // pagkabigo/pag-expire — bago mag-mark ng "failed". Kung wala
            // pang last_payment_error, ligtas lang na huwag munang gawin
            // ang UPDATE (mananatiling 'pending' ang purchase, tatapusin
            // na lang ito ng normal na paid/failed webhook sa itaas).
            if (resource.attributes && resource.attributes.last_payment_error) {
                await creditPaidPaymentIntent(resource.id, { failed: true });
            }
        }
        res.json({ success: true });
    } catch (err) {
        console.error('⚠️  PayMongo webhook processing error:', err.message);
        res.status(500).json({ success: false, message: err.message });
    }
});
// ---- Xendit webhook (invoice.paid) ----
app.post('/relay/webhooks/xendit', async (req, res) => {
    const headerToken = req.headers['x-callback-token'];
    if (!verifyXenditWebhookToken(headerToken)) {
        console.error('⚠️  Xendit webhook: invalid/missing callback token — tinanggihan.');
        return res.status(400).json({ success: false, message: 'Invalid webhook token.' });
    }
    const event = req.body || {};
    const invoiceId = event.id;
    const status = event.status; 
    try {
        if (!invoiceId) return res.json({ success: true, ignored: true });
        const purchaseResult = await queryWithRetry(
            pgPool,
            `SELECT * FROM cloud_token_purchases WHERE source_id = $1 AND provider = 'xendit' AND status = 'pending'`,
            [invoiceId]
        );
        const purchase = purchaseResult.rows[0];
        if (!purchase) return res.json({ success: true, ignored: true, message: 'No matching pending purchase for this invoice.' });
        if (status === 'PAID' || status === 'SETTLED') {
            // BUGFIX: same double-credit race as the PayMongo webhook above — claim
            // atomically (pending -> paid) BEFORE crediting, so a duplicate/concurrent
            // Xendit delivery can never credit the same purchase twice.
            const claimResult = await queryWithRetry(pgPool, `UPDATE cloud_token_purchases SET status = 'paid', updated_at = now() WHERE purchase_id = $1 AND status = 'pending' RETURNING *`, [purchase.purchase_id]);
            if (!claimResult.rows[0]) {
                return res.json({ success: true, alreadyProcessed: true });
            }
            const newBalance = await creditCloudTokens(purchase.installation_id, Number(purchase.tokens), `Purchase ${purchase.purchase_id} (${purchase.method})`);
            logActivity(purchase.installation_id, 'cloud_token_purchase_paid', { purchaseId: purchase.purchase_id, tokens: Number(purchase.tokens), newBalance });
            sendTelegramNotification(`💎 Omni Tokens bought: ${purchase.tokens} tokens (₱${purchase.amount_php}) via Xendit (${purchase.method}) — installation ${purchase.installation_id.slice(0, 12)}... New balance: ${newBalance}`).catch(() => {});
        } else if (status === 'EXPIRED') {
            await queryWithRetry(pgPool, `UPDATE cloud_token_purchases SET status = 'failed', updated_at = now() WHERE purchase_id = $1 AND status = 'pending'`, [purchase.purchase_id]);
        }
        res.json({ success: true });
    } catch (err) {
        console.error('⚠️  Xendit webhook processing error:', err.message);
        res.status(500).json({ success: false, message: err.message });
    }
});
// ---- Stripe webhook (checkout.session.completed) ----
app.post('/relay/webhooks/stripe', express.raw({ type: '*/*', limit: '1mb' }), async (req, res) => {
    const rawBody = req.body ? req.body.toString('utf8') : '';
    const signatureHeader = req.headers['stripe-signature'];
    if (!verifyStripeSignature(rawBody, signatureHeader)) {
        console.error('⚠️  Stripe webhook: invalid/missing signature — tinanggihan.');
        return res.status(400).json({ success: false, message: 'Invalid webhook signature.' });
    }
    let event;
    try {
        event = JSON.parse(rawBody);
    } catch (err) {
        return res.status(400).json({ success: false, message: 'Invalid JSON.' });
    }
    try {
        const session = event && event.data && event.data.object;
        if (event.type === 'checkout.session.completed' && session && session.id) {
            const purchaseResult = await queryWithRetry(
                pgPool,
                `SELECT * FROM cloud_token_purchases WHERE source_id = $1 AND provider = 'stripe' AND status = 'pending'`,
                [session.id]
            );
            const purchase = purchaseResult.rows[0];
            if (!purchase) return res.json({ success: true, ignored: true, message: 'No matching pending purchase for this session.' });
            if (session.payment_status === 'paid') {
                // BUGFIX: same double-credit race as the other provider webhooks — claim
                // atomically (pending -> paid) BEFORE crediting, so a duplicate/concurrent
                // Stripe delivery can never credit the same purchase twice.
                const claimResult = await queryWithRetry(pgPool, `UPDATE cloud_token_purchases SET status = 'paid', updated_at = now() WHERE purchase_id = $1 AND status = 'pending' RETURNING *`, [purchase.purchase_id]);
                if (!claimResult.rows[0]) {
                    return res.json({ success: true, alreadyProcessed: true });
                }
                const newBalance = await creditCloudTokens(purchase.installation_id, Number(purchase.tokens), `Purchase ${purchase.purchase_id} (${purchase.method})`);
                logActivity(purchase.installation_id, 'cloud_token_purchase_paid', { purchaseId: purchase.purchase_id, tokens: Number(purchase.tokens), newBalance });
                sendTelegramNotification(`💎 Omni Tokens bought: ${purchase.tokens} tokens (₱${purchase.amount_php}) via Stripe (card) — installation ${purchase.installation_id.slice(0, 12)}... New balance: ${newBalance}`).catch(() => {});
            }
        } else if (event.type === 'checkout.session.expired' && session && session.id) {
            await queryWithRetry(pgPool, `UPDATE cloud_token_purchases SET status = 'failed', updated_at = now() WHERE source_id = $1 AND provider = 'stripe' AND status = 'pending'`, [session.id]);
        }
        res.json({ success: true });
    } catch (err) {
        console.error('⚠️  Stripe webhook processing error:', err.message);
        res.status(500).json({ success: false, message: err.message });
    }
});
// ---- PayPal webhook (PAYMENT.CAPTURE.COMPLETED / CHECKOUT.ORDER.APPROVED) ----
app.post('/relay/webhooks/paypal', async (req, res) => {
    const event = req.body || {};
    try {
        const verified = await verifyPaypalWebhook(req.headers, event);
        if (!verified) {
            console.error('⚠️  PayPal webhook: invalid/unverifiable signature — tinanggihan.');
            return res.status(400).json({ success: false, message: 'Invalid webhook signature.' });
        }
        const eventType = event.event_type;
        const orderId = event.resource && (event.resource.id || (event.resource.supplementary_data && event.resource.supplementary_data.related_ids && event.resource.supplementary_data.related_ids.order_id));
        if (eventType === 'CHECKOUT.ORDER.APPROVED' && orderId) {
            const purchaseResult = await queryWithRetry(
                pgPool,
                `SELECT * FROM cloud_token_purchases WHERE source_id = $1 AND provider = 'paypal' AND status = 'pending'`,
                [orderId]
            );
            const purchase = purchaseResult.rows[0];
            if (!purchase) return res.json({ success: true, ignored: true, message: 'No matching pending purchase for this order.' });
            const capture = await paypalCaptureOrder(orderId);
            const captureStatus = capture && capture.status;
            if (captureStatus === 'COMPLETED') {
                // BUGFIX: same double-credit race as the other provider webhooks — claim
                // atomically (pending -> paid) BEFORE crediting, so a duplicate/concurrent
                // PayPal delivery can never credit the same purchase twice.
                const claimResult = await queryWithRetry(pgPool, `UPDATE cloud_token_purchases SET status = 'paid', updated_at = now() WHERE purchase_id = $1 AND status = 'pending' RETURNING *`, [purchase.purchase_id]);
                if (!claimResult.rows[0]) {
                    return res.json({ success: true, alreadyProcessed: true });
                }
                const newBalance = await creditCloudTokens(purchase.installation_id, Number(purchase.tokens), `Purchase ${purchase.purchase_id} (${purchase.method})`);
                logActivity(purchase.installation_id, 'cloud_token_purchase_paid', { purchaseId: purchase.purchase_id, tokens: Number(purchase.tokens), newBalance });
                sendTelegramNotification(`💎 Omni Tokens bought: ${purchase.tokens} tokens (₱${purchase.amount_php}) via PayPal — installation ${purchase.installation_id.slice(0, 12)}... New balance: ${newBalance}`).catch(() => {});
            } else {
                await queryWithRetry(pgPool, `UPDATE cloud_token_purchases SET status = 'failed', updated_at = now() WHERE purchase_id = $1 AND status = 'pending'`, [purchase.purchase_id]);
            }
        }
        res.json({ success: true });
    } catch (err) {
        console.error('⚠️  PayPal webhook processing error:', err.message);
        res.status(500).json({ success: false, message: err.message });
    }
});
// ---- Dragonpay Postback (server-to-server, itinakda sa Dragonpay
// merchant back-office bilang "Postback URL") ----
// AYOS: Dragonpay ay NAGPAPADALA rin ng katulad na request papunta sa
// "Return URL"/browser redirect (na ginamit na nating
// /relay/cloud-tokens/return sa itaas, kagaya ng ibang provider) —
// pero HINDI dapat doon nakabatay ang pag-credit ng tokens (madaling
// i-fake ng kliyente ang isang plain browser redirect). Ang Postback
// dito lang, na naka-verify gamit ang SHA1 digest, ang PINAGKAKATIWALAAN
// na batayan.
app.post(DRAGONPAY_WEBHOOK_PATH, express.urlencoded({ extended: false, limit: '256kb' }), async (req, res) => {
    const { txnid, refno, status, message, digest } = req.body || {};
    if (!verifyDragonpayPostbackDigest({ txnid, refno, status, message, digest })) {
        console.error('⚠️  Dragonpay postback: invalid/missing digest — tinanggihan.');
        // Text lang (hindi JSON) ang inaasahan ni Dragonpay bilang response.
        return res.status(400).type('text/plain').send('result=INVALID');
    }
    try {
        if (!txnid) return res.type('text/plain').send('result=OK');
        const purchaseResult = await queryWithRetry(
            pgPool,
            `SELECT * FROM cloud_token_purchases WHERE source_id = $1 AND provider = 'dragonpay' AND status = 'pending'`,
            [txnid]
        );
        const purchase = purchaseResult.rows[0];
        if (!purchase) return res.type('text/plain').send('result=OK');
        if (status === 'S') {
            // BUGFIX: same double-credit race as the other provider webhooks — claim
            // atomically (pending -> paid) BEFORE crediting. Dragonpay in particular is
            // known to resend the Postback until it gets an explicit 'result=OK', so
            // without this guard a slow first response could easily trigger a second,
            // fully duplicate credit.
            const claimResult = await queryWithRetry(pgPool, `UPDATE cloud_token_purchases SET status = 'paid', updated_at = now() WHERE purchase_id = $1 AND status = 'pending' RETURNING *`, [purchase.purchase_id]);
            if (!claimResult.rows[0]) {
                return res.type('text/plain').send('result=OK');
            }
            const newBalance = await creditCloudTokens(purchase.installation_id, Number(purchase.tokens), `Purchase ${purchase.purchase_id} (${purchase.method})`);
            logActivity(purchase.installation_id, 'cloud_token_purchase_paid', { purchaseId: purchase.purchase_id, tokens: Number(purchase.tokens), newBalance });
            sendTelegramNotification(`💎 Omni Tokens bought: ${purchase.tokens} tokens (₱${purchase.amount_php}) via Dragonpay (refno ${refno || 'n/a'}) — installation ${purchase.installation_id.slice(0, 12)}... New balance: ${newBalance}`).catch(() => {});
        } else if (status === 'F' || status === 'V' || status === 'K') {
            await queryWithRetry(pgPool, `UPDATE cloud_token_purchases SET status = 'failed', updated_at = now() WHERE purchase_id = $1 AND status = 'pending'`, [purchase.purchase_id]);
        }
        // Kung P (Pending), U (Unknown), o A (Authorized) — hindi muna
        // ginagalaw, hihintayin na lang ang susunod na Postback ng
        // Dragonpay pagbago ng status.
        res.type('text/plain').send('result=OK');
    } catch (err) {
        console.error('⚠️  Dragonpay webhook processing error:', err.message);
        res.status(500).type('text/plain').send('result=ERROR');
    }
});
// AYOS/BUGFIX: dating tumatanggap ito ng isang PRE-COMPUTED na `tokens`
// (integer, Math.ceil na) mula sa OMNIPOS — kaya (a) palaging sobra ang
// aktwal na na-cconsume kada buwan kumpara sa advertised monthly price
// (tingnan ang comment sa getCloudTokenCostPerSync/Exact sa itaas), at
// (b) kung sandaling hindi pa naka-refresh ang lokal na cache ng presyo
// sa OMNIPOS (tingnan ang applyCloudBackupPricingOverlay doon), maaaring
// magkaiba pa ang presyong ginamit sa pag-charge dito kumpara sa TALAGANG
// kasalukuyang presyo dito mismo sa RELAY. Ngayon, `tier` na lang
// (hindi na `tokens`) ang tinatanggap — dito mismo sa RELAY (ang
// tunay na "may-ari" ng presyo) EKSAKTONG kinukwenta at ina-accrue ang
// fractional cost kada sync, kaya laging TAMA/EKSAKTO ito kahit
// magpalit ng presyo ng tier anumang oras, at laging magkatugma ang
// OMNIPOS at RELAY (iisang pinagmumulan ng presyo na lang).
app.post('/relay/cloud-tokens/check-and-consume', requireApiKey, requireAllowedDevice, rateLimit('cloud-tokens-consume', 300, 60 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
    const { installationId, tier, note } = req.body;
    if (!installationId) return res.status(400).json({ success: false, message: 'Missing installationId.' });
    if (!tier || !CLOUD_BACKUP_PLANS[tier]) return res.status(400).json({ success: false, message: 'Invalid o missing na Cloud Backup tier.' });
    if (!pgPool) return res.status(503).json({ success: false, message: 'Hindi pa naka-configure ang Postgres (DATABASE_URL).' });
    try {
        // AYOS/BUGFIX: DEPRECATED na endpoint ito (hindi na tinatawag ng
        // live na flow — tingnan ang comment sa OMNIPOS consumeCloudTokensForSync()).
        // Wala itong sizeBytes sa request body, kaya gamitin muna ang huling
        // kilalang aktwal na laki ng backup ng installation na ito (o ang
        // 5MB na ilustratibong sample kung wala pa) — hindi na ang tier.
        const legacySizeResult = await queryWithRetry(pgPool, 'SELECT size_bytes FROM cloud_backup_meta WHERE installation_id = $1', [installationId]);
        const legacyKnownSizeBytes = legacySizeResult.rows[0] ? Number(legacySizeResult.rows[0].size_bytes) || 0 : 0;
        const legacySizeBytes = legacyKnownSizeBytes > 0 ? legacyKnownSizeBytes : CLOUD_BACKUP_SAMPLE_SIZE_BYTES_FOR_CATALOG;
        const result = await consumeCloudTokensForSyncExact(installationId, legacySizeBytes, tier, note);
        invalidateWalletCache(installationId);
        if (!result.ok) {
            return res.status(402).json({
                success: false,
                insufficient: true,
                balanceTokens: result.balanceTokens,
                message: 'Insufficient Cloud Backup tokens. Please buy more tokens to keep syncing.'
            });
        }
        res.json({ success: true, balanceTokens: result.balanceTokens, tokensCharged: result.tokensCharged });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});
// ===================================================================
// CLOUD BACKUP — TOKEN-FUNDED SELF-SERVE ACTIVATION
//
// Advanced Pro flow: pays for a Cloud Backup plan directly out of the
// installation's own Omni Token wallet, instead of the old manual
// "developer approves, then hands the OTP to the client" process used
// by /relay/request-unlock + /relay/confirm-unlock.
//
// Division of responsibility (by design):
//   - OMNIPOS owns IDENTITY verification: it generates the OTP, emails
//     it straight to the requestor's own Gmail using the store's own
//     verified Sender Gmail App Password, and verifies the code
//     LOCALLY. The raw OTP never has to travel to/through RELAY.
//   - RELAY owns MONEY: it is the sole source of truth for the Cloud
//     Token wallet/ledger, so it is the one that performs the actual
//     atomic check-and-deduct once OMNIPOS confirms the requestor
//     proved ownership of the Gmail address.
//
// Flow:
//   1) OMNIPOS checks the wallet balance first via the existing
//      GET /relay/cloud-tokens/wallet (no new endpoint needed for
//      this step) — if insufficient, OMNIPOS sends the requestor to
//      the Omni Token purchase page and nothing is touched here.
//   2) If sufficient, OMNIPOS sends the OTP straight to the requestor
//      Gmail and verifies it locally. Only once that succeeds does it
//      call POST /relay/cloud-tokens/activate-cloud-backup below.
//   3) This endpoint deducts the tokens and issues the signed
//      activation token in ONE atomic UPDATE — so if the balance
//      changed in the meantime (e.g. spent by auto-sync between step
//      1 and step 3), it fails safely with insufficient:true and
//      issues NOTHING, rather than allowing a negative balance.
//      Nothing is ever deducted for an abandoned/failed/expired
//      request — only a successful call to this endpoint spends
//      tokens.
// ===================================================================
// ===================================================================
// NOTE/idempotency safeguard for this endpoint: if the response to
// OMNIPOS times out or gets lost AFTER RELAY has already successfully
// processed the activation (rare, but possible on network blips),
// OMNIPOS may retry the same confirm request. Without dedupe, tokens
// would be deducted twice (and the subscription extended twice). The
// OMNIPOS confirm handler must pass a stable `clientRequestId`
// (generated back when the request-OTP step ran, not regenerated on
// each retry) — if the same id was already seen here before, the exact
// same result is simply replayed, with no new deduction.
const cloudBackupActivationDedupe = new Map(); // `${installationId}:${clientRequestId}` -> { status, body, expiresAt }
const CLOUD_BACKUP_ACTIVATION_DEDUPE_TTL_MS = 15 * 60 * 1000;
setInterval(() => {
    const now = Date.now();
    for (const [key, entry] of cloudBackupActivationDedupe) {
        if (now > entry.expiresAt) cloudBackupActivationDedupe.delete(key);
    }
}, 5 * 60 * 1000).unref();
app.post('/relay/cloud-tokens/activate-cloud-backup', requireApiKey, requireAllowedDevice, rateLimit('cloud-tokens-activate-cloud-backup', 30, 10 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
    if (!ACTIVATION_FLAGS.omniTokenActivationEnabled) {
        return res.status(503).json({ success: false, message: '"Activate via Omni Tokens" is temporarily disabled (maintenance/upgrade). Please try "Send Request" instead, or try again later.' });
    }
    const { installationId, tier, billingCycle, requestorEmail, clientRequestId } = req.body;
    if (!installationId || !tier || !billingCycle) {
        return res.status(400).json({ success: false, message: 'Missing installationId, tier, or billingCycle.' });
    }
    if (!pgPool) {
        return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured — the Omni Token wallet needs it.' });
    }
    const requiredTokens = getCloudBackupPlanPrice(tier, billingCycle);
    if (requiredTokens === null) {
        return res.status(400).json({ success: false, message: 'Invalid Cloud Backup tier/billingCycle.' });
    }
    const dedupeKey = clientRequestId ? `${installationId}:${clientRequestId}` : null;
    if (dedupeKey && cloudBackupActivationDedupe.has(dedupeKey)) {
        const cached = cloudBackupActivationDedupe.get(dedupeKey);
        return res.status(cached.status).json(cached.body);
    }
    try {
        // NOTE: this is a single atomic UPDATE (WHERE balance_tokens >=
        // requiredTokens), so exactly one of two things can happen: (a)
        // the tokens are successfully deducted AND the activation token
        // is issued, or (b) the balance is left untouched and no token
        // is issued. No partial state is possible.
        await getOrCreateCloudTokenWallet(installationId);
        const deductResult = await queryWithRetry(
            pgPool,
            `UPDATE cloud_token_wallets SET balance_tokens = balance_tokens - $2, updated_at = now()
             WHERE installation_id = $1 AND balance_tokens >= $2 RETURNING balance_tokens`,
            [installationId, requiredTokens]
        );
        if (!deductResult.rows[0]) {
            const currentResult = await queryWithRetry(pgPool, 'SELECT balance_tokens FROM cloud_token_wallets WHERE installation_id = $1', [installationId]);
            const insufficientBody = {
                success: false,
                insufficient: true,
                balanceTokens: currentResult.rows[0] ? Number(currentResult.rows[0].balance_tokens) : 0,
                requiredTokens,
                message: `Insufficient Omni Tokens. ${requiredTokens} token(s) are needed for ${CLOUD_BACKUP_PLANS[tier].name} (${billingCycle}). Please buy more Omni Tokens first, then try again.`
            };
            // NOTE: an "insufficient" result is NOT cached as an
            // idempotent result — this way, if the requestor buys more
            // tokens and retries with the same clientRequestId, a fresh
            // attempt is still made instead of forever replaying the
            // old "insufficient" response.
            return res.status(402).json(insufficientBody);
        }
        const balanceAfter = Number(deductResult.rows[0].balance_tokens);
        invalidateWalletCache(installationId);
        await queryWithRetry(
            pgPool,
            `INSERT INTO cloud_token_ledger (installation_id, type, tokens, balance_after, note, category) VALUES ($1, 'consume', $2, $3, $4, 'FEATURE_UNLOCK')`,
            [installationId, -requiredTokens, balanceAfter, `Cloud Backup activation — ${CLOUD_BACKUP_PLANS[tier].name} (${billingCycle}), verified via ${requestorEmail || 'requestor Gmail'}`]
        );
        const durationDays = CLOUD_BACKUP_BILLING_DAYS[billingCycle];
        const durationMs = typeof durationDays === 'number' && durationDays > 0 ? durationDays * 24 * 60 * 60 * 1000 : null;
        const token = issueSignedToken(installationId, 'cloud_backup', durationMs);
        recordIssuedUnlock(installationId, 'cloud_backup', token, {
            featureName: CLOUD_BACKUP_PLANS[tier].name,
            price: requiredTokens,
            source: 'cloud_token_selfserve',
            tier,
            billingCycle
        });
        if (token.payload.expiresAt) {
            markMaintenanceFeePaidUntil(installationId, token.payload.expiresAt);
        }
        logActivity(installationId, 'unlock_issued', {
            featureId: 'cloud_backup',
            featureName: CLOUD_BACKUP_PLANS[tier].name,
            source: 'cloud_token_selfserve',
            tier,
            billingCycle,
            tokensSpent: requiredTokens
        });
        const successBody = {
            success: true,
            message: `${CLOUD_BACKUP_PLANS[tier].name} has been activated!`,
            token,
            tier,
            billingCycle,
            balanceTokens: balanceAfter,
            tokensSpent: requiredTokens
        };
        if (dedupeKey) {
            cloudBackupActivationDedupe.set(dedupeKey, { status: 200, body: successBody, expiresAt: Date.now() + CLOUD_BACKUP_ACTIVATION_DEDUPE_TTL_MS });
        }
        res.json(successBody);
    } catch (err) {
        console.error('cloud-tokens/activate-cloud-backup error:', err);
        res.status(500).json({ success: false, message: err.message });
    }
});
// ===================================================================
// GENERIC TOKEN-FUNDED SELF-SERVE ACTIVATION — for everything that is
// NOT Cloud Backup (which has its own dedicated endpoint above, since
// it alone carries a tier/maintenance-fee side effect). Covers:
//   - a single Module Subscription (RBAC Management / Multi-Branch) —
//     pass featureIds: [oneModuleSubscriptionId] + billingCycle
//   - one or more one-time Pro Themes / à la carte features — pass
//     featureIds: [...] (no billingCycle). Optional totalPrice lets a
//     bundle/tier discount be honored (split proportionally across the
//     items exactly like /relay/confirm-unlock-bulk already does),
//     otherwise the official à la carte FEATURE_CATALOG price per item
//     is used.
// Same atomic-deduct-then-issue guarantee and clientRequestId dedupe as
// the Cloud Backup endpoint — nothing is ever spent for an
// abandoned/expired/failed request, only for a confirmed one.
// ===================================================================
const featurePurchaseActivationDedupe = new Map(); // `${installationId}:${clientRequestId}` -> { status, body, expiresAt }
const FEATURE_PURCHASE_ACTIVATION_DEDUPE_TTL_MS = 15 * 60 * 1000;
setInterval(() => {
    const now = Date.now();
    for (const [key, entry] of featurePurchaseActivationDedupe) {
        if (now > entry.expiresAt) featurePurchaseActivationDedupe.delete(key);
    }
}, 5 * 60 * 1000).unref();
// ===================================================================
// AYOS/SECURITY FIX (totalPrice trust bug): dati, ang à la carte na
// sanga ng /relay/cloud-tokens/activate-purchase sa ibaba ay tinatanggap
// LANG ang `totalPrice` mula sa CLIENT (kinukwenta sa browser JS ng
// OMNIPOS) basta `>= 0` — walang server-side na pag-verify laban sa
// FEATURE_CATALOG o sa totoong bundle-discount rules. Dahil ATOMIC at
// AUTOMATIC (walang developer/admin approval) ang endpoint na ito,
// kahit sinong nag-modify ng request (devtools, proxy, patched client)
// ay pwedeng magpadala ng totalPrice: 0 o 1 para sa isang mamahaling
// Pro theme/feature, at agad-agad na mabigyan ng valid, properly-
// signed unlock token — halos libre. Ang mga function dito ay
// kumukwenta ng PINAKAMABABANG lehitimong presyo sa RELAY mismo
// (ang sole source of truth para sa pera), gamit ang eksaktong
// parehong bundle-discount formula ng OMNIPOS getTierPricing() at ang
// totoong multi-terminal discount ng installation na ito — hindi na
// basta tinitiwalaan ang bilang mula sa client kung mas mababa ito.
function getTierBundleEffectivePrice(tier, alreadyPurchased, multiTerminalDiscountPercent = 0) {
    const fullAlaCarteValue = tier.featureIds.reduce((sum, id) => sum + ((FEATURE_CATALOG[id] && FEATURE_CATALOG[id].price) || 0), 0);
    const remainingFeatureIds = tier.featureIds.filter(id => !alreadyPurchased.includes(id));
    const remainingAlaCarteValue = remainingFeatureIds.reduce((sum, id) => sum + ((FEATURE_CATALOG[id] && FEATURE_CATALOG[id].price) || 0), 0);
    if (fullAlaCarteValue <= 0 || remainingAlaCarteValue <= 0) return 0;
    const bundleRate = tier.bundlePrice / fullAlaCarteValue;
    let effectivePrice = Math.min(
        tier.bundlePrice,
        Math.max(1, Math.ceil(remainingAlaCarteValue * bundleRate))
    );
    if (multiTerminalDiscountPercent > 0) {
        effectivePrice = Math.max(1, Math.round(effectivePrice * (1 - multiTerminalDiscountPercent / 100)));
    }
    return effectivePrice;
}
// Kinukwenta ang pinakamababang presyo na maaaring lehitimong asahan ng
// client para sa hiniling na `featureIds`. Ang alaCarteTotal (walang
// discount) ang default floor; kung LAHAT ng hiniling na featureIds ay
// bahagi ng isang kilalang UPGRADE_TIER (at ang natitirang bahagi ng
// tier na iyon ay totoong-totoo nang naka-unlock na sa installation na
// ito), pinapayagan ang marginal/bundle-rate na presyo ng tier na iyon
// bilang mas mababang floor — kopya ito ng eksaktong parehong senaryo
// na ginagamit ng OMNIPOS getTierPricing() (request-unlock-bulk), hindi
// basta-basta arbitrary discount.
function computeMinimumLegitimatePrice(installationId, featureIds) {
    const alaCarteTotal = featureIds.reduce((sum, id) => sum + ((FEATURE_CATALOG[id] && FEATURE_CATALOG[id].price) || 0), 0);
    const deviceCount = getGroupDeviceCount(installationId);
    const multiTerminalDiscountPercent = getMultiTerminalDiscountPercent(deviceCount);
    let bestPrice = alaCarteTotal;
    for (const tier of UPGRADE_TIERS) {
        const requestedInTier = featureIds.filter(id => tier.featureIds.includes(id));
        if (requestedInTier.length !== featureIds.length) continue; // hindi lahat ng hiniling ay bahagi ng tier na ito
        const alreadyPurchased = tier.featureIds.filter(id => !featureIds.includes(id) && isFeatureCurrentlyUnlocked(installationId, id));
        const tierPrice = getTierBundleEffectivePrice(tier, alreadyPurchased, multiTerminalDiscountPercent);
        if (tierPrice < bestPrice) bestPrice = tierPrice;
    }
    return bestPrice;
}
app.post('/relay/cloud-tokens/activate-purchase', requireApiKey, requireAllowedDevice, rateLimit('cloud-tokens-activate-purchase', 30, 10 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
    if (!ACTIVATION_FLAGS.omniTokenActivationEnabled) {
        return res.status(503).json({ success: false, message: '"Activate via Omni Tokens" is temporarily disabled (maintenance/upgrade). Please try "Send Request" instead, or try again later.' });
    }
    const { installationId, featureIds, billingCycle, totalPrice, clientRequestId } = req.body;
    if (!installationId || !Array.isArray(featureIds) || featureIds.length === 0) {
        return res.status(400).json({ success: false, message: 'Missing installationId or featureIds.' });
    }
    if (featureIds.includes('cloud_backup')) {
        return res.status(400).json({ success: false, message: 'Cloud Backup has its own activation endpoint (activate-cloud-backup) — it cannot be combined with other items here.' });
    }
    const moduleSubIds = featureIds.filter(id => isModuleSubscriptionFeature(id));
    if (moduleSubIds.length > 1 || (moduleSubIds.length === 1 && featureIds.length > 1)) {
        return res.status(400).json({ success: false, message: 'A subscription module must be activated on its own, separate from other items.' });
    }
    const isModuleSubscriptionPurchase = moduleSubIds.length === 1;
    if (!pgPool) {
        return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured — the Omni Token wallet needs it.' });
    }
    let requiredTokens;
    let durationMs = null;
    if (isModuleSubscriptionPurchase) {
        if (!MODULE_SUBSCRIPTION_BILLING_DAYS[billingCycle]) {
            return res.status(400).json({ success: false, message: 'Please choose a valid billing cycle (monthly/yearly).' });
        }
        requiredTokens = getModuleSubscriptionPrice(moduleSubIds[0], billingCycle);
        if (requiredTokens === null) {
            return res.status(400).json({ success: false, message: 'Invalid subscription module.' });
        }
        durationMs = MODULE_SUBSCRIPTION_BILLING_DAYS[billingCycle] * 24 * 60 * 60 * 1000;
    } else {
        // AYOS/SECURITY FIX: hindi na basta tinitiwalaan ang client-supplied
        // `totalPrice`. Kinukwenta muna dito sa RELAY (server-side, authoritative)
        // ang pinakamababang lehitimong presyo — kung mas mababa dito ang
        // ipinasa ng client (o wala/invalid), gagamitin ang floor na ito
        // sa halip, at ilalagay sa activity log para malaman ng developer
        // kung may nag-attempt ng price tampering.
        const minimumLegitimatePrice = computeMinimumLegitimatePrice(installationId, featureIds);
        if (typeof totalPrice === 'number' && isFinite(totalPrice) && totalPrice >= minimumLegitimatePrice) {
            requiredTokens = Math.round(totalPrice);
        } else {
            if (typeof totalPrice === 'number' && isFinite(totalPrice) && totalPrice < minimumLegitimatePrice) {
                console.warn(`⚠️ PRICE TAMPERING SUSPECTED: installation ${installationId} sent totalPrice=${totalPrice} for [${featureIds.join(', ')}] pero ang pinakamababang lehitimong presyo ay ${minimumLegitimatePrice}. Ginamit ang floor sa halip.`);
                logActivity(installationId, 'price_tampering_suspected', { featureIds, clientTotalPrice: totalPrice, enforcedPrice: minimumLegitimatePrice });
            }
            requiredTokens = minimumLegitimatePrice;
        }
    }
    if (typeof requiredTokens !== 'number' || requiredTokens < 0 || !isFinite(requiredTokens)) {
        return res.status(400).json({ success: false, message: 'Could not determine a valid price for this purchase.' });
    }
    const dedupeKey = clientRequestId ? `${installationId}:${clientRequestId}` : null;
    if (dedupeKey && featurePurchaseActivationDedupe.has(dedupeKey)) {
        const cached = featurePurchaseActivationDedupe.get(dedupeKey);
        return res.status(cached.status).json(cached.body);
    }
    try {
        await getOrCreateCloudTokenWallet(installationId);
        // requiredTokens could be 0 for a free/₱0 item — an UPDATE with
        // "balance_tokens >= 0" always matches (even at 0 balance), which
        // is the correct behavior (nothing to actually charge).
        const deductResult = await queryWithRetry(
            pgPool,
            `UPDATE cloud_token_wallets SET balance_tokens = balance_tokens - $2, updated_at = now()
             WHERE installation_id = $1 AND balance_tokens >= $2 RETURNING balance_tokens`,
            [installationId, requiredTokens]
        );
        if (!deductResult.rows[0]) {
            const currentResult = await queryWithRetry(pgPool, 'SELECT balance_tokens FROM cloud_token_wallets WHERE installation_id = $1', [installationId]);
            const insufficientBody = {
                success: false,
                insufficient: true,
                balanceTokens: currentResult.rows[0] ? Number(currentResult.rows[0].balance_tokens) : 0,
                requiredTokens,
                message: `Insufficient Omni Tokens. ${requiredTokens} token(s) are needed for this purchase. Please buy more Omni Tokens first, then try again.`
            };
            return res.status(402).json(insufficientBody);
        }
        const balanceAfter = Number(deductResult.rows[0].balance_tokens);
        invalidateWalletCache(installationId);
        const featureNames = featureIds.map(id => (FEATURE_CATALOG[id] && FEATURE_CATALOG[id].name) || (isModuleSubscriptionPurchase && MODULE_SUBSCRIPTION_PLANS[id] && MODULE_SUBSCRIPTION_PLANS[id].name) || id);
        await queryWithRetry(
            pgPool,
            `INSERT INTO cloud_token_ledger (installation_id, type, tokens, balance_after, note, category) VALUES ($1, 'consume', $2, $3, $4, 'ADDON_PURCHASE')`,
            [installationId, -requiredTokens, balanceAfter, `Purchase — ${featureNames.join(', ')}`]
        );
        // Split requiredTokens across items proportionally to their à la
        // carte price (same allocation approach as /relay/confirm-unlock-bulk),
        // so per-item records/reporting stay meaningful even when a
        // bundle discount made requiredTokens less than the à la carte sum.
        let perFeaturePrice = {};
        if (isModuleSubscriptionPurchase) {
            perFeaturePrice[moduleSubIds[0]] = requiredTokens;
        } else {
            const alaCartePrices = featureIds.map(id => (FEATURE_CATALOG[id] && FEATURE_CATALOG[id].price) || 0);
            const alaCarteTotal = alaCartePrices.reduce((s, p) => s + p, 0);
            if (alaCarteTotal > 0) {
                let allocated = 0;
                featureIds.forEach((id, i) => {
                    const share = Math.floor((requiredTokens * alaCartePrices[i]) / alaCarteTotal);
                    perFeaturePrice[id] = share;
                    allocated += share;
                });
                const remainder = requiredTokens - allocated;
                if (remainder !== 0 && featureIds.length > 0) {
                    const priciestIdx = alaCartePrices.indexOf(Math.max(...alaCartePrices));
                    perFeaturePrice[featureIds[priciestIdx]] += remainder;
                }
            } else {
                const evenShare = Math.floor(requiredTokens / featureIds.length);
                let allocated = 0;
                featureIds.forEach((id) => { perFeaturePrice[id] = evenShare; allocated += evenShare; });
                perFeaturePrice[featureIds[featureIds.length - 1]] += requiredTokens - allocated;
            }
        }
        const tokens = {};
        featureIds.forEach((featureId, i) => {
            const token = issueSignedToken(installationId, featureId, durationMs);
            tokens[featureId] = token;
            recordIssuedUnlock(installationId, featureId, token, {
                featureName: featureNames[i],
                price: perFeaturePrice[featureId],
                source: 'cloud_token_selfserve',
                billingCycle: isModuleSubscriptionPurchase ? billingCycle : null
            });
            logActivity(installationId, 'unlock_issued', {
                featureId,
                featureName: featureNames[i],
                source: 'cloud_token_selfserve',
                billingCycle: isModuleSubscriptionPurchase ? billingCycle : null,
                tokensSpent: perFeaturePrice[featureId]
            });
        });
        const successBody = {
            success: true,
            message: `${featureNames.join(', ')} ${featureNames.length > 1 ? 'have' : 'has'} been activated!`,
            tokens,
            billingCycle: isModuleSubscriptionPurchase ? billingCycle : undefined,
            balanceTokens: balanceAfter,
            tokensSpent: requiredTokens
        };
        if (dedupeKey) {
            featurePurchaseActivationDedupe.set(dedupeKey, { status: 200, body: successBody, expiresAt: Date.now() + FEATURE_PURCHASE_ACTIVATION_DEDUPE_TTL_MS });
        }
        res.json(successBody);
    } catch (err) {
        console.error('cloud-tokens/activate-purchase error:', err);
        res.status(500).json({ success: false, message: err.message });
    }
});
// Ang bahaging ito lang ang makikita ng isang OMNIPOS installation sa
// sarili niyang admin panel: kanya-kanyang share sa TOTAL na Neon cost
// (base sa proporsyon ng laki ng data at dalas ng backup), dagdag ang
// maintenance fee, para malaman ng may-ari kung magkano ang dapat niyang
// bayaran ngayong buwan — hindi niya makikita ang breakdown ng ibang client.
app.get('/relay/cloud-backup/cost-allocation', requireApiKey, requireAllowedDevice, rateLimit('cloud-backup-cost-allocation', 30, 60 * 60 * 1000, (req) => req.query?.installationId), async (req, res) => {
    const installationId = String(req.query.installationId || '').trim();
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
    }
    try {
        const allocation = await computeClientCostAllocation();
        if (!allocation.success) {
            return res.status(503).json(allocation);
        }
        const mine = allocation.clients.find(c => c.installationId === installationId);
        if (!mine) {
            return res.json({
                success: true,
                hasUsage: false,
                message: 'No Cloud Backup usage recorded yet for this installation in the current billing period.',
                costBasis: allocation.costBasis,
                warning: allocation.warning
            });
        }
        res.json({
            success: true,
            hasUsage: true,
            checkedAt: allocation.checkedAt,
            costBasis: allocation.costBasis,
            warning: allocation.warning,
            clientCount: allocation.clientCount,
            totalCostPHP: allocation.totalCostPHP,
            yourShare: mine
        });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});
app.get('/relay/admin/api/client-cost-allocation', requireAdminKey, async (req, res) => {
    try {
        const allocation = await computeClientCostAllocation();
        res.json(allocation);
    } catch (err) {
        console.error('⚠️  /relay/admin/api/client-cost-allocation error:', err.message);
        res.status(500).json({ success: false, message: 'Could not compute client cost allocation.' });
    }
});
// ===================================================================
// GAWA/BAGO: RECONCILIATION REPORT — sinasagot nito ang "lugi ba ako?"
// gamit ang TOTOONG pera, hindi estimate/projection. Kumukuha ng
// KABUUANG na-charge sa mga customer para sa Cloud Backup (mula sa
// cloud_token_ledger mismo — SYNC_CHARGE + RESTORE_CHARGE +
// FEATURE_UNLOCK + STORAGE_HOLDING_FEE, bawas ang anumang REFUND) sa
// loob ng isang buwan, at
// pinagkukumpara ito sa TOTOONG binayaran mo kay Neon (na mano-manong
// ilalagay mo dito bawat buwan mula sa aktwal na Neon invoice — walang
// public API si Neon para sa historical invoice amount, kaya hindi ito
// awtomatikong makukuha). Dahil ₱1 = 1 Omni Token (tingnan ang
// getCloudTokenPackages() sa itaas), direktang PHP na ang mga token
// amount sa ledger — walang kailangang i-convert pa.
// ===================================================================
const CLOUD_BACKUP_ACTUAL_BILLS_PATH = path.join(__dirname, 'cloud-backup-actual-bills.json');
async function loadCloudBackupActualBills() {
    const fromStore = await getPersistentJSON('cloud-backup-actual-bills', null);
    if (fromStore !== null) return fromStore;
    try {
        return JSON.parse(fs.readFileSync(CLOUD_BACKUP_ACTUAL_BILLS_PATH, 'utf8'));
    } catch (err) {
        return {};
    }
}
// AYOS/BAGO: pareho ng bug na nahanap sa saveNeonConfiguredPlans() sa
// itaas — dating "return" agad pagkatapos ng redis/Postgres save, kaya
// walang lokal na file backup. Delikado ito dahil ITO MISMO ang totoong
// Neon invoice data na ginagamit ng Reconciliation report para sagutin
// ang "lugi ba ako?" — kaya dapat protektado rin ito ng dual-write.
function saveCloudBackupActualBills(obj) {
    if (pgPoolDevices || pgPool || redisClient) {
        setPersistentJSON('cloud-backup-actual-bills', obj);
    }
    try {
        fs.writeFileSync(CLOUD_BACKUP_ACTUAL_BILLS_PATH, JSON.stringify(obj, null, 2));
    } catch (err) {
        console.error('Hindi ma-save ang cloud-backup-actual-bills.json:', err);
    }
}
let cloudBackupActualBills = {}; // keyed by 'YYYY-MM' -> { actualBillUSD, exchangeRateUsed, actualBillPHP, note, enteredAt }
function monthRangeUtc(monthStr) {
    // monthStr = 'YYYY-MM'. Ibinabalik ang [startMs, endMs) sa UTC.
    const [y, m] = monthStr.split('-').map(Number);
    if (!y || !m || m < 1 || m > 12) return null;
    const start = Date.UTC(y, m - 1, 1);
    const end = Date.UTC(m === 12 ? y + 1 : y, m === 12 ? 0 : m, 1);
    return { start, end };
}
function currentMonthStr() {
    const d = new Date();
    return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
}
// I-save/i-update ang TOTOONG Neon invoice amount (USD) para sa isang
// buwan — mano-mano itong ilalagay ng developer/admin base sa aktwal na
// billing statement ng Neon. Ang PHP equivalent ay LOCKED IN gamit ang
// exchange rate NGAYONG oras ng pag-e-enter (hindi na babagu-bago kahit
// magbago pa ang live rate sa susunod na tingin dito).
app.post('/relay/admin/api/cloud-backup/actual-bill', requireAdminKey, async (req, res) => {
    const { month, actualBillUSD, note } = req.body || {};
    const range = monthRangeUtc(String(month || ''));
    if (!range) return res.status(400).json({ success: false, message: 'Invalid month format — dapat "YYYY-MM" (hal. "2026-09").' });
    const billUSD = Number(actualBillUSD);
    if (!isFinite(billUSD) || billUSD < 0) return res.status(400).json({ success: false, message: 'Invalid actualBillUSD.' });
    const { rate } = await getUsdToPhpRate();
    cloudBackupActualBills[month] = {
        actualBillUSD: billUSD,
        exchangeRateUsed: rate,
        actualBillPHP: Math.round(billUSD * rate * 100) / 100,
        note: note || null,
        enteredAt: Date.now()
    };
    saveCloudBackupActualBills(cloudBackupActualBills);
    res.json({ success: true, month, entry: cloudBackupActualBills[month] });
});
// Kabuuang reconciliation para sa isang buwan (default: kasalukuyang
// buwan) — totoong charged-to-customers vs totoong Neon bill.
app.get('/relay/admin/api/cloud-backup/reconciliation', requireAdminKey, async (req, res) => {
    if (!pgPool) return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured.' });
    const month = String(req.query.month || currentMonthStr());
    const range = monthRangeUtc(month);
    if (!range) return res.status(400).json({ success: false, message: 'Invalid month format — dapat "YYYY-MM".' });
    try {
        const { rows } = await queryWithRetry(
            pgPool,
            `SELECT
                COALESCE(SUM(CASE WHEN type = 'consume' AND category IN ('SYNC_CHARGE','RESTORE_CHARGE','FEATURE_UNLOCK','STORAGE_HOLDING_FEE') THEN -tokens ELSE 0 END), 0) AS gross_charged_php,
                COALESCE(SUM(CASE WHEN type = 'purchase' AND category = 'REFUND' THEN tokens ELSE 0 END), 0) AS refunded_php,
                COUNT(*) FILTER (WHERE category = 'SYNC_CHARGE') AS sync_charge_count,
                COUNT(*) FILTER (WHERE category = 'RESTORE_CHARGE') AS restore_charge_count,
                COUNT(*) FILTER (WHERE category = 'FEATURE_UNLOCK') AS activation_count,
                COUNT(*) FILTER (WHERE category = 'STORAGE_HOLDING_FEE') AS storage_fee_charge_count
             FROM cloud_token_ledger
             WHERE created_at >= to_timestamp($1 / 1000.0) AND created_at < to_timestamp($2 / 1000.0)`,
            [range.start, range.end]
        );
        const r = rows[0] || {};
        const grossChargedPHP = Math.round(Number(r.gross_charged_php || 0) * 100) / 100;
        const refundedPHP = Math.round(Number(r.refunded_php || 0) * 100) / 100;
        const netChargedPHP = Math.round((grossChargedPHP - refundedPHP) * 100) / 100;
        const actualBillEntry = cloudBackupActualBills[month] || null;
        const actualBillPHP = actualBillEntry ? actualBillEntry.actualBillPHP : null;
        const profitLossPHP = actualBillPHP !== null ? Math.round((netChargedPHP - actualBillPHP) * 100) / 100 : null;
        res.json({
            success: true,
            month,
            netChargedPHP,
            grossChargedPHP,
            refundedPHP,
            syncChargeCount: Number(r.sync_charge_count || 0),
            restoreChargeCount: Number(r.restore_charge_count || 0),
            activationCount: Number(r.activation_count || 0),
            storageFeeChargeCount: Number(r.storage_fee_charge_count || 0),
            actualBill: actualBillEntry,
            profitLossPHP,
            status: profitLossPHP === null ? 'unknown' : (profitLossPHP >= 0 ? 'kumikita' : 'lugi'),
            allMonthsWithActualBill: Object.keys(cloudBackupActualBills).sort().reverse()
        });
    } catch (err) {
        console.error('⚠️  /relay/admin/api/cloud-backup/reconciliation error:', err.message);
        res.status(500).json({ success: false, message: 'Could not compute reconciliation.' });
    }
});
// ===================================================================
// GAWA/BAGO: RETENTION REVIEW — hinahanap ang mga installation na may
// naka-store pa ring Cloud Backup data sa Neon (patuloy na gumagastos
// ng storage) pero HINDI na active/bayad ang subscription nila nang
// matagal na (>= graceDays, default 90 araw). Kailanman ay hindi
// kasama ang Lifetime purchases (bayad na nang buo, dapat manatili
// habang buhay). Read-only lang ang endpoint na ito — HINDI awtomatikong
// nagde-delete; ang aktwal na pagbura ay hiwalay na endpoint
// (retention-purge, sa ibaba) na kailangan pang i-confirm nang mano-mano
// per-installation — sinasadya ito, dahil delikado/hindi na-uundo ang
// pagbura ng customer data kung basta na lang awtomatiko.
// ===================================================================
const CLOUD_BACKUP_RETENTION_DEFAULT_GRACE_DAYS = 90;
app.get('/relay/admin/api/cloud-backup/retention-review', requireAdminKey, async (req, res) => {
    if (!pgPool) return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured.' });
    const graceDays = Number(req.query.graceDays) > 0 ? Number(req.query.graceDays) : CLOUD_BACKUP_RETENTION_DEFAULT_GRACE_DAYS;
    try {
        const { rows } = await queryWithRetry(
            pgPool,
            'SELECT installation_id, store_name, size_bytes, last_sync_at, module_count FROM cloud_backup_meta ORDER BY size_bytes DESC',
            []
        );
        const { rate } = await getUsdToPhpRate();
        const neonPlanId = neonConfiguredPlans.cloudBackup || 'free';
        let neonTier = NEON_PRICING[neonPlanId];
        if (!neonTier || !neonTier.storageRatePerGBMonthUSD) neonTier = NEON_PRICING.launch;
        const now = Date.now();
        const candidates = [];
        for (const r of rows) {
            const sub = getCloudBackupSubscriptionForClient(r.installation_id);
            if (sub.isLifetime || sub.active) continue; // huwag kailanman i-touch ang Lifetime o currently-active
            const expiryKnown = sub.expiresAt !== null;
            const referenceMs = expiryKnown ? sub.expiresAt : new Date(r.last_sync_at).getTime();
            if (!isFinite(referenceMs)) continue;
            const daysSince = Math.floor((now - referenceMs) / (24 * 60 * 60 * 1000));
            if (daysSince < graceDays) continue;
            const sizeBytes = Number(r.size_bytes) || 0;
            const sizeGB = sizeBytes / (1024 * 1024 * 1024);
            const estMonthlyStorageCostUSD = sizeGB * neonTier.storageRatePerGBMonthUSD;
            candidates.push({
                installationId: r.installation_id,
                label: deviceLabels.get(r.installation_id) || null,
                storeName: r.store_name || null,
                sizeMB: Math.round((sizeBytes / (1024 * 1024)) * 100) / 100,
                moduleCount: r.module_count,
                lastSyncAt: r.last_sync_at,
                expiryKnown,
                referenceDate: new Date(referenceMs).toISOString(),
                daysSinceExpiry: daysSince,
                estMonthlyStorageCostUSD: Math.round(estMonthlyStorageCostUSD * 1000) / 1000,
                estMonthlyStorageCostPHP: Math.round(estMonthlyStorageCostUSD * rate * 100) / 100
            });
        }
        candidates.sort((a, b) => b.daysSinceExpiry - a.daysSinceExpiry);
        res.json({
            success: true,
            checkedAt: now,
            graceDays,
            candidateCount: candidates.length,
            totalEstMonthlyWasteMB: Math.round(candidates.reduce((s, c) => s + c.sizeMB, 0) * 100) / 100,
            totalEstMonthlyWastePHP: Math.round(candidates.reduce((s, c) => s + c.estMonthlyStorageCostPHP, 0) * 100) / 100,
            candidates
        });
    } catch (err) {
        console.error('⚠️  /relay/admin/api/cloud-backup/retention-review error:', err.message);
        res.status(500).json({ success: false, message: 'Could not compute retention review.' });
    }
});
// Aktwal na pagbura — kailangan pa ring i-confirm nang mano-mano
// per-installation (tingnan ang paalala sa itaas). Binubura ang
// cloud_backup_modules AT cloud_backup_meta — hindi na-touch ang
// cloud_token_wallets/ledger (transaction history mananatili).
app.post('/relay/admin/api/cloud-backup/retention-purge', requireAdminKey, async (req, res) => {
    if (!pgPool) return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured.' });
    const { installationId, confirm } = req.body || {};
    if (!installationId) return res.status(400).json({ success: false, message: 'Missing installationId.' });
    if (confirm !== true) return res.status(400).json({ success: false, message: 'Kailangan ng confirm:true — hindi na-uundo ang pagbura.' });
    const sub = getCloudBackupSubscriptionForClient(installationId);
    if (sub.isLifetime || sub.active) {
        return res.status(400).json({ success: false, message: 'Ligtas na hinarang ang pagbura — Lifetime o kasalukuyang active pa ang subscription na ito.' });
    }
    try {
        await runPgWriteTx(pgPool, async (client) => {
            await client.query('DELETE FROM cloud_backup_modules WHERE installation_id = $1', [installationId]);
            await client.query('DELETE FROM cloud_backup_meta WHERE installation_id = $1', [installationId]);
        });
        logActivity(installationId, 'cloud_backup_retention_purge', { purgedBy: 'admin', graceReason: 'expired subscription retention sweep' });
        res.json({ success: true, message: `Nabura na ang Cloud Backup data ng installation ${installationId}.` });
    } catch (err) {
        console.error('⚠️  /relay/admin/api/cloud-backup/retention-purge error:', err.message);
        res.status(500).json({ success: false, message: 'Could not purge this installation\'s cloud backup data.' });
    }
});
// ===================================================================
// GAWA/BAGO: PROACTIVE ALERTS — dati, kailangan mo pang buksan mismo
// ang admin panel para malaman kung UNREACHABLE ang NEON_API_KEY o
// kung naka-fallback (hindi na-detect ang totoong plan) ang isa sa 3
// database. Ngayon, may background sweep (tuwing 15 minuto) na
// awtomatikong magpapadala ng Telegram message at/o generic webhook
// kapag:
//   1. UNREACHABLE ang Neon Account API nang >= 30 minuto (tuloy-tuloy)
//   2. Naka-fallback ang plan detection ng isa sa 3 Neon database
//      nang >= 2 oras (tuloy-tuloy) — ibig sabihin posibleng maling
//      rate/₱0 ang "REAL na usage" report na iyon sa loob ng mahabang
//      panahon nang walang nakakaalam
// May "resolved" na follow-up alert din kapag bumalik sa normal, at
// paulit-ulit na reminder (hindi lang minsan) habang patuloy pa ring
// down, para hindi ito ma-miss/malimutan.
//
// SETUP (opsyonal — pareho o isa lang sa dalawa, wala ring epekto sa
// normal na operation kung wala man):
//   RELAY_ALERT_TELEGRAM_BOT_TOKEN + RELAY_ALERT_TELEGRAM_CHAT_ID
//     — gumawa ng bot via @BotFather sa Telegram, kunin ang token, at
//       ang chat ID mo (hal. via @userinfobot o /getUpdates).
//   RELAY_ALERT_WEBHOOK_URL
//     — kahit anong URL na tumatanggap ng POST JSON {text: "..."},
//       gumagana rin ito sa Slack/Discord incoming webhooks.
// Kung wala man itong dalawa naka-set, laging naka-console.warn/error
// pa rin ang mga alert na ito sa server logs bilang huling fallback.
// ===================================================================
const ALERT_NEON_API_KEY_DOWN_THRESHOLD_MS = 30 * 60 * 1000;      // 30 minuto
const ALERT_NEON_API_KEY_REMINDER_MS = 6 * 60 * 60 * 1000;        // 6 oras
const ALERT_FALLBACK_PLAN_THRESHOLD_MS = 2 * 60 * 60 * 1000;      // 2 oras
const ALERT_FALLBACK_PLAN_REMINDER_MS = 12 * 60 * 60 * 1000;      // 12 oras
let pricingStalenessAlerted = false;
const costAlertState = {
    neonApiKey: { downSince: null, lastAlertAt: null },
    fallbackPlan: {
        cloudBackup: { downSince: null, lastAlertAt: null },
        devices: { downSince: null, lastAlertAt: null },
        build: { downSince: null, lastAlertAt: null }
    }
};
async function sendCostAlert(title, message) {
    const fullText = `⚠️ RELAY Cost Alert — ${title}\n\n${message}`;
    console.warn(`\n${'='.repeat(60)}\n${fullText}\n${'='.repeat(60)}\n`);
    const tasks = [];
    if (process.env.RELAY_ALERT_TELEGRAM_BOT_TOKEN && process.env.RELAY_ALERT_TELEGRAM_CHAT_ID) {
        tasks.push(
            fetch(`https://api.telegram.org/bot${process.env.RELAY_ALERT_TELEGRAM_BOT_TOKEN}/sendMessage`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ chat_id: process.env.RELAY_ALERT_TELEGRAM_CHAT_ID, text: fullText })
            }).catch(err => console.error('⚠️ Hindi napadala ang Telegram alert:', err.message))
        );
    }
    if (process.env.RELAY_ALERT_WEBHOOK_URL) {
        tasks.push(
            fetch(process.env.RELAY_ALERT_WEBHOOK_URL, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ text: fullText, title, message })
            }).catch(err => console.error('⚠️ Hindi napadala ang webhook alert:', err.message))
        );
    }
    if (tasks.length > 0) await Promise.allSettled(tasks);
}
// Generic helper para sa "down since X, alert kapag lumagpas sa
// threshold, paulit-ulit na reminder, at resolved-alert pag bumalik" —
// ginagamit ng parehong Neon API key check at fallback-plan check sa
// ibaba, para hindi paulit-ulit ang parehong logic.
async function evaluateAlertCondition(state, isDown, thresholdMs, reminderMs, onAlert, onResolved) {
    const now = Date.now();
    if (isDown) {
        if (state.downSince === null) {
            state.downSince = now; // unang beses na na-detect ang problema
            return;
        }
        const downForMs = now - state.downSince;
        if (downForMs < thresholdMs) return; // hindi pa umaabot sa threshold, huwag pa mag-alert
        const dueForReminder = state.lastAlertAt === null || (now - state.lastAlertAt) >= reminderMs;
        if (dueForReminder) {
            await onAlert(downForMs);
            state.lastAlertAt = now;
        }
    } else if (state.downSince !== null) {
        // bumalik sa normal matapos ang isang problema — isang beses na
        // "resolved" alert, tapos i-reset ang state
        const wasDownForMs = now - state.downSince;
        if (state.lastAlertAt !== null) await onResolved(wasDownForMs); // alert lang na "resolved" kung may naipadalang alert dati
        state.downSince = null;
        state.lastAlertAt = null;
    }
}
function formatDuration(ms) {
    const hours = ms / (60 * 60 * 1000);
    if (hours < 1) return `${Math.round(ms / 60000)} minuto`;
    if (hours < 24) return `${hours.toFixed(1)} oras`;
    return `${(hours / 24).toFixed(1)} araw`;
}
async function runCostAlertSweep() {
    try {
        // 0) Pricing staleness — isang beses lang mag-a-alert kapag
        // biglang naging stale (hindi paulit-ulit, dahil buwan-buwan lang
        // ito nagbabago at hindi urgent gaya ng downtime).
        const staleness = getNeonPricingStaleness();
        if (staleness.isStale && !pricingStalenessAlerted) {
            await sendCostAlert(
                'Neon Pricing — Posibleng Luma Na',
                `Huling na-verify ang Neon pricing rates noong ${staleness.verifiedAt} (${staleness.monthsSinceVerified} buwan na ang nakalipas). ` +
                `Posibleng nagbago na ang totoong presyo ni Neon. I-check ang ${NEON_PRICING_SOURCE_URL} at i-update kung kinakailangan (Pricing admin panel).`
            );
            pricingStalenessAlerted = true;
        } else if (!staleness.isStale) {
            pricingStalenessAlerted = false;
        }
        // 1) Neon Account API key reachability
        const keyCheck = await checkNeonApiKeyReachable();
        const keyIsDown = keyCheck.configured && !keyCheck.reachable;
        await evaluateAlertCondition(
            costAlertState.neonApiKey,
            keyIsDown,
            ALERT_NEON_API_KEY_DOWN_THRESHOLD_MS,
            ALERT_NEON_API_KEY_REMINDER_MS,
            async (downForMs) => sendCostAlert(
                'Neon Account API UNREACHABLE',
                `Hindi na-reach ang Neon Account API (NEON_API_KEY) sa loob ng ${formatDuration(downForMs)}.\n` +
                `Error: ${keyCheck.error || 'unknown'}\n\n` +
                `Habang ganito, TAHIMIK gagamitin ang fallback/dropdown plan (hindi ang totoong plan) sa "REAL na usage" breakdown — posibleng maling ₱0 o maling rate ang lalabas. Puntahan ang Database Health sa admin panel para i-verify.`
            ),
            async (wasDownForMs) => sendCostAlert(
                'Neon Account API — RESOLVED',
                `Bumalik na sa REACHABLE ang Neon Account API matapos ang ${formatDuration(wasDownForMs)}.`
            )
        );
        // 2) Fallback-plan detection, per configured Neon database
        const projectIds = {
            cloudBackup: NEON_CLOUD_BACKUP_PROJECT_ID,
            devices: NEON_DEVICES_PROJECT_ID,
            build: NEON_BUILD_PROJECT_ID
        };
        const labels = { cloudBackup: 'Cloud Backup DB', devices: 'Devices/License DB', build: 'Build/Push DB' };
        for (const key of Object.keys(projectIds)) {
            if (!projectIds[key]) continue; // hindi naka-configure ang project na ito, laktawan
            // AYOS/BUGFIX: kapag DOWN na mismo ang buong Neon API key (#1 sa itaas),
            // laktawan ang per-database check na ito — dahil parehong root cause lang
            // ito (ang key mismo), hindi dapat mag-alert nang hiwalay ang bawat isa
            // sa 3 databases (Cloud Backup/Devices/Build) na magiging usageMissing=true
            // lahat. Dati, apat na magkakahiwalay na alert (1 key-down + 3 per-db) ang
            // pumapasok kahit iisa lang talaga ang dahilan. Ipagpapatuloy ang per-db
            // check na ito sa susunod na sweep kapag bumalik na REACHABLE ang key.
            if (keyIsDown) continue;
            // AYOS/BAGO: dati, "usedFallback" lang (galing sa computeNeonRealCost)
            // ang sinusuri — pero kung mali/hindi tugma ang SPECIFIC na project
            // ID ng database na ito habang tama pa rin ang NEON_API_KEY mismo
            // (hal. na-typo, o na-delete/na-transfer ang project sa Neon), null
            // lang ang ibabalik ng getNeonProjectUsage() — hindi "fallback"
            // technically, pero pareho rin ang epekto: blangko/maling REAL na
            // usage report para dito. Sinama na rin ito sa parehong alert.
            let usageMissing = false;
            let usedFallback = false;
            try {
                const usage = await getNeonProjectUsage(projectIds[key]);
                usageMissing = !usage;
                const realCost = usage ? computeNeonRealCost(usage, neonConfiguredPlans[key] || 'free') : null;
                usedFallback = !!(realCost && realCost.usedFallbackPlan);
            } catch (err) {
                usageMissing = true;
            }
            const isDown = usedFallback || usageMissing;
            await evaluateAlertCondition(
                costAlertState.fallbackPlan[key],
                isDown,
                ALERT_FALLBACK_PLAN_THRESHOLD_MS,
                ALERT_FALLBACK_PLAN_REMINDER_MS,
                async (downForMs) => sendCostAlert(
                    `${labels[key]} — ${usageMissing ? 'Walang Real Usage Data' : 'Fallback Plan'} sa loob ng ${formatDuration(downForMs)}`,
                    usageMissing
                        ? `Walang naibabalik na usage data mula sa Neon para sa ${labels[key]} sa loob ng ${formatDuration(downForMs)}, kahit tila REACHABLE ang Neon Account API key sa pangkalahatan. Posibleng mali/na-delete/na-transfer ang project ID nito — i-verify sa Database Health.`
                        : `Hindi na-detect ang totoong Neon plan para sa ${labels[key]} sa loob ng ${formatDuration(downForMs)} — gamit na lang ang admin dropdown ("${neonConfiguredPlans[key] || 'free'}") bilang batayan.\n\nKung mali ang naka-set na dropdown na 'yan (hal. "Free" pero may bayad ka na talaga), maling numero ang lumalabas sa "REAL na usage" report. I-verify sa Database Health.`
                ),
                async (wasDownForMs) => sendCostAlert(
                    `${labels[key]} — Plan Detection RESOLVED`,
                    `Bumalik na sa normal ang plan detection para sa ${labels[key]} matapos ang ${formatDuration(wasDownForMs)}.`
                )
            );
        }
    } catch (err) {
        console.error('⚠️ runCostAlertSweep error:', err.message);
    }
}
setInterval(runCostAlertSweep, 15 * 60 * 1000);
setTimeout(runCostAlertSweep, 60 * 1000); // unang check, 1 minuto pagkatapos mag-boot
// Manual na test button sa admin panel — para ma-verify agad kung
// tama ang Telegram/webhook setup, hindi na kailangan hintayin pa ang
// susunod na aktwal na problema.
app.post('/relay/admin/api/alerts/test', requireAdminKey, async (req, res) => {
    const configured = !!((process.env.RELAY_ALERT_TELEGRAM_BOT_TOKEN && process.env.RELAY_ALERT_TELEGRAM_CHAT_ID) || process.env.RELAY_ALERT_WEBHOOK_URL);
    await sendCostAlert('Test Alert', 'Ito ay test message lang — kung nakita mo ito, gumagana ang alert setup mo.');
    res.json({ success: true, configured, message: configured ? 'Naipadala ang test alert.' : 'Walang naka-configure na Telegram/webhook — na-log lang sa server console (tingnan sa terminal/logs).' });
});
app.get('/relay/admin/api/alerts/config', requireAdminKey, (req, res) => {
    res.json({
        success: true,
        telegramConfigured: !!(process.env.RELAY_ALERT_TELEGRAM_BOT_TOKEN && process.env.RELAY_ALERT_TELEGRAM_CHAT_ID),
        webhookConfigured: !!process.env.RELAY_ALERT_WEBHOOK_URL,
        currentState: costAlertState
    });
});
// ===================================================================
// GAWA/BAGO: NEAR-QUOTA UPSELL — mga PAYING (active o lifetime) na
// subscriber na malapit na o nasa quota na ng kasalukuyang tier nila.
// Hindi ito tungkol sa "lugi" mismo (may-bayad naman sila), kundi
// revenue opportunity: bago pa sila mag-storage-exceeded error,
// puwede mo na silang i-suggest na mag-upgrade — mas maganda ito
// kaysa first time nilang malaman pag nag-error na ang sync nila.
// ===================================================================
app.get('/relay/admin/api/cloud-backup/near-quota-review', requireAdminKey, async (req, res) => {
    if (!pgPool) return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured.' });
    const thresholdPercent = Number(req.query.thresholdPercent) > 0 ? Number(req.query.thresholdPercent) : 80;
    const CLOUD_BACKUP_TIER_ORDER = ['basic', 'standard', 'pro'];
    try {
        const { rows } = await queryWithRetry(
            pgPool,
            'SELECT installation_id, store_name, size_bytes, last_sync_at FROM cloud_backup_meta ORDER BY size_bytes DESC',
            []
        );
        const candidates = [];
        for (const r of rows) {
            const sub = getCloudBackupSubscriptionForClient(r.installation_id);
            if (!sub.active && !sub.isLifetime) continue; // hindi active/bayad — hindi upsell target, tingnan na lang sa retention review
            const tier = sub.tier || 'basic';
            const plan = CLOUD_BACKUP_PLANS[tier];
            if (!plan || !plan.storageQuotaMB) continue;
            const sizeMB = (Number(r.size_bytes) || 0) / (1024 * 1024);
            const percentUsed = (sizeMB / plan.storageQuotaMB) * 100;
            if (percentUsed < thresholdPercent) continue;
            const tierIdx = CLOUD_BACKUP_TIER_ORDER.indexOf(tier);
            const nextTier = tierIdx >= 0 && tierIdx < CLOUD_BACKUP_TIER_ORDER.length - 1 ? CLOUD_BACKUP_TIER_ORDER[tierIdx + 1] : null;
            const nextPlan = nextTier ? CLOUD_BACKUP_PLANS[nextTier] : null;
            candidates.push({
                installationId: r.installation_id,
                label: deviceLabels.get(r.installation_id) || null,
                storeName: r.store_name || null,
                currentTier: tier,
                currentTierName: plan.name,
                sizeMB: Math.round(sizeMB * 100) / 100,
                quotaMB: plan.storageQuotaMB,
                percentUsed: Math.round(percentUsed * 10) / 10,
                lastSyncAt: r.last_sync_at,
                atMaxTier: !nextPlan,
                suggestedTier: nextPlan ? nextPlan.id : null,
                suggestedTierName: nextPlan ? nextPlan.name : null,
                suggestedTierMonthlyPHP: nextPlan ? nextPlan.price.monthly : null
            });
        }
        candidates.sort((a, b) => b.percentUsed - a.percentUsed);
        res.json({
            success: true,
            checkedAt: Date.now(),
            thresholdPercent,
            candidateCount: candidates.length,
            candidates
        });
    } catch (err) {
        console.error('⚠️  /relay/admin/api/cloud-backup/near-quota-review error:', err.message);
        res.status(500).json({ success: false, message: 'Could not compute near-quota review.' });
    }
});

// tinatawag sa loob ng /relay/cloud-backup/restore) — ginagamit ito para sa
// karagdagang/punitive na charge na direktang desisyon ng developer, halimbawa
// kung sa tingin niya hindi sapat ang standard na per-restore charge para sa
// isang paulit-ulit na abuser.
app.post('/relay/admin/api/devices/:installationId/charge-restore', requireAdminKey, async (req, res) => {
    if (!pgPool) return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured.' });
    const { installationId } = req.params;
    const tokens = Number(req.body?.tokens);
    const note = (req.body?.note || '').toString().trim();
    if (!installationId) return res.status(400).json({ success: false, message: 'Missing installationId.' });
    if (!isFinite(tokens) || tokens <= 0) {
        return res.status(400).json({ success: false, message: 'Provide a positive number of tokens to charge.' });
    }
    try {
        const result = await runPgWriteTx(pgPool, async (client) => {
            await client.query(
                `INSERT INTO cloud_token_wallets (installation_id, balance_tokens, auto_sync_enabled) VALUES ($1, 0, true)
                 ON CONFLICT (installation_id) DO NOTHING`,
                [installationId]
            );
            const walletRes = await client.query(
                `SELECT balance_tokens FROM cloud_token_wallets WHERE installation_id = $1 FOR UPDATE`,
                [installationId]
            );
            const currentBalance = Number(walletRes.rows[0].balance_tokens);
            if (currentBalance < tokens) {
                return { ok: false, insufficient: true, balanceTokens: currentBalance };
            }
            const newBalance = currentBalance - tokens;
            await client.query(
                `UPDATE cloud_token_wallets SET balance_tokens = $2, updated_at = now() WHERE installation_id = $1`,
                [installationId, newBalance]
            );
            await client.query(
                `INSERT INTO cloud_token_ledger (installation_id, type, tokens, balance_after, note, category, trigger_type) VALUES ($1, 'consume', $2, $3, $4, 'RESTORE_CHARGE', 'manual_admin')`,
                [installationId, -tokens, newBalance, note ? `Manual restore-abuse charge (admin): ${note}` : 'Manual restore-abuse charge (admin)']
            );
            return { ok: true, balanceTokens: newBalance };
        });
        if (!result.ok) {
            return res.status(402).json({
                success: false,
                insufficientTokens: true,
                balanceTokens: result.balanceTokens,
                message: `This client's balance (${result.balanceTokens}) is lower than the ${tokens} token(s) you tried to charge.`
            });
        }
        invalidateWalletCache(installationId);
        logActivity(installationId, 'cloud_backup_restore_manual_charge', { tokens, note: note || null, balanceAfter: result.balanceTokens });
        res.json({ success: true, tokensCharged: tokens, balanceTokens: result.balanceTokens });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});
app.post('/relay/admin/api/client-cost-allocation/maintenance-fee', requireAdminKey, (req, res) => {
    const { defaultFeePHP, installationId, feePHP } = req.body || {};
    if (typeof defaultFeePHP === 'number' && isFinite(defaultFeePHP) && defaultFeePHP >= 0) {
        clientMaintenanceFeeConfig.defaultFeePHP = defaultFeePHP;
    }
    if (installationId) {
        if (feePHP === null) {
            delete clientMaintenanceFeeConfig.perClientOverridePHP[installationId];
        } else if (typeof feePHP === 'number' && isFinite(feePHP) && feePHP >= 0) {
            clientMaintenanceFeeConfig.perClientOverridePHP[installationId] = feePHP;
        } else {
            return res.status(400).json({ success: false, message: 'Invalid feePHP — must be a non-negative number, or null to clear the override.' });
        }
    }
    saveClientMaintenanceFeeConfig(clientMaintenanceFeeConfig);
    res.json({ success: true, clientMaintenanceFeeConfig });
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
// TIGHTENED CAP: a legitimate restore (new device setup, disaster
// recovery) is a rare, one-off event for a real customer — it should never
// need to repeat many times in a single hour. Lowered from 10/hour to
// 5/day per installationId, which still comfortably covers any real
// recovery scenario while making repeated/scripted restore abuse far
// harder to pull off.
app.post('/relay/cloud-backup/restore', requireApiKey, requireAllowedDevice, rateLimit('cloud-backup-restore', 5, 24 * 60 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
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
        // AYOS/BAGO: proportional-to-size token charge for the restore
        // itself (same PHP-from-real-Neon-cost model as sync — see
        // computeRealCloudBackupRestoreCostPHP() above). This gives the
        // restore a small real cost to the customer, discourages
        // repeated/abusive restores, and covers the actual Neon
        // compute/egress cost of pulling the whole backup back out.
        const subscriptionForRestoreCharge = getCloudBackupSubscriptionForClient(installationId);
        const tierForRestoreCharge = subscriptionForRestoreCharge.tier || 'basic';
        const sizeBytesForRestoreCharge = Number(metaResult.rows[0].size_bytes) || 0;
        const chargeResult = await consumeCloudTokensForRestore(installationId, sizeBytesForRestoreCharge, tierForRestoreCharge, 'Cloud Backup restore');
        if (!chargeResult.ok) {
            logActivity(installationId, 'cloud_backup_restore_blocked', { reason: 'insufficient_tokens', balanceTokens: chargeResult.balanceTokens });
            const tokenCostPerRestoreExact = Math.round((await getCloudTokenCostPerRestoreExact(sizeBytesForRestoreCharge, tierForRestoreCharge)) * 1000) / 1000;
            return res.status(402).json({
                success: false,
                insufficientTokens: true,
                balanceTokens: chargeResult.balanceTokens,
                tokenCostPerRestore: chargeResult.costTokens,
                tokenCostPerRestoreExact,
                message: `Insufficient Cloud Backup tokens (balance: ${chargeResult.balanceTokens}, needed: ~${tokenCostPerRestoreExact} for this restore). Please buy more Omni Tokens on the Omni Tokens page.`
            });
        }
        const modulesResult = await queryWithRetry(pgPool, 'SELECT module, data, record_count FROM cloud_backup_modules WHERE installation_id = $1', [installationId]);
        const modules = {};
        modulesResult.rows.forEach((r) => { modules[r.module] = r.data; });
        // AYOS/BAGO: tracking-only (walang kinalaman sa charging sa itaas) —
        // para may visibility kung sino ang sobrang dalas mag-restore, bago
        // magdesisyon kung mas hihigpitan pa/mas taasan ang charge. Makikita
        // ito sa Client Cost Allocation report (computeClientCostAllocation()).
        await queryWithRetry(
            pgPool,
            'UPDATE cloud_backup_meta SET restore_count = restore_count + 1, last_restore_at = now() WHERE installation_id = $1',
            [installationId]
        );
        logActivity(installationId, 'cloud_backup_restored', {
            moduleCount: modulesResult.rows.length,
            lastSyncAt: metaResult.rows[0].last_sync_at,
            tokensCharged: chargeResult.tokensCharged,
            balanceAfter: chargeResult.balanceTokens
        });
        const restoreResponseBody = {
            success: true,
            message: 'Nakuha ang cloud backup para sa installation na ito.',
            meta: {
                storeName: metaResult.rows[0].store_name,
                totalRecords: metaResult.rows[0].total_records,
                moduleCount: metaResult.rows[0].module_count,
                lastSyncAt: metaResult.rows[0].last_sync_at
            },
            modules,
            redactedFieldsByModule: CLOUD_BACKUP_REDACTED_FIELDS_BY_MODULE,
            tokensCharged: chargeResult.tokensCharged,
            balanceTokens: chargeResult.balanceTokens
        };
        // AYOS/BAGO: i-gzip ang buong restore payload (ito ang pinakamalaking
        // response sa buong RELAY — kasing-laki ng buong backup) gamit ang
        // STANDARD na HTTP `Content-Encoding: gzip` header, hindi custom na
        // format. Anumang normal na HTTP client (kasama na ang built-in
        // fetch() ng Node na ginagamit ng OMNIPOS) ay awtomatikong
        // nagde-decode nito bago pa man umabot sa JSON.parse — kaya walang
        // kailangang baguhin sa client side. Kung sakaling may consumer sa
        // hinaharap na hindi marunong mag-decode ng gzip (hal. curl na
        // walang --compressed flag), makikita nila ito bilang binary
        // garbage sa halip na JSON — pero ang TANGING kilalang consumer ng
        // endpoint na ito ngayon ay ang OMNIPOS server mismo, kaya ligtas
        // ito. Kung may error/rejection response naman (402/403/404/atbp.),
        // hindi ito dinadaanan — plain/uncompressed JSON pa rin ang mga
        // 'yon gaya ng dati.
        const restoreResponseJson = JSON.stringify(restoreResponseBody);
        const restoreResponseGzipped = await gzipAsync(restoreResponseJson);
        res.set('Content-Type', 'application/json; charset=utf-8');
        res.set('Content-Encoding', 'gzip');
        res.send(restoreResponseGzipped);
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
    if (!ACTIVATION_FLAGS.otpRequestsEnabled) {
        return res.status(503).json({ success: false, message: 'Manual unlock requests ("Send Request") are temporarily disabled by the developer. Please try "Activate via Omni Tokens" instead, or try again later.' });
    }
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
    // AYOS: sa unang successful na subscribe/renew ng Cloud Backup, i-mark
    // na "paid" ang maintenance fee hanggang sa pag-expire ng subscription
    // period na ito mismo (parehong petsa ng token.payload.expiresAt) —
    // tingnan ang computeClientCostAllocation() para sa paggamit nito sa
    // pag-exclude ng maintenance fee sa "total" habang aktibo pa ito.
    if (featureId === 'cloud_backup' && token.payload.expiresAt) {
        markMaintenanceFeePaidUntil(installationId, token.payload.expiresAt);
    }
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
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
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
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
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
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
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
    if (!ACTIVATION_FLAGS.otpRequestsEnabled) {
        return res.status(503).json({ success: false, message: 'Manual unlock requests ("Send Request") are temporarily disabled by the developer. Please try "Activate via Omni Tokens" instead, or try again later.' });
    }
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
            return res.status(400).json({ success: false, message: 'Missing installationId.' });
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
            return res.status(400).json({ success: false, message: 'Missing installationId.' });
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
        } catch (err) {
            // NOTE: email/Slack/Telegram are best-effort notification channels only.
            // The request itself was already saved to pendingReceiptResets above, so it
            // still shows up (with its OTP code) under "Pending Receipt Customization
            // Resets" in the RELAY admin panel even if every notification channel is
            // down/misconfigured — the admin dashboard is the source of truth, not email.
            console.error('Relay notification failure (receipt-reset) — request was still saved and is visible in the admin panel:', err);
        }
        logActivity(installationId, 'receipt_reset_requested', { storeName: storeName || null });
        res.json({ success: true, message: 'Naipadala ang reset request. Tignan ng developer ang RELAY admin panel (Pending Receipt Customization Resets) para sa OTP.' });
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
const RECEIPT_CREDIT_OTP_TTL_MS = 10 * 60 * 1000;
const RECEIPT_CREDIT_TICKET_TTL_MS = 5 * 60 * 1000;
const RECEIPT_CREDIT_UNITS_PER_PURCHASE = 1;
const RECEIPT_CREDIT_PRICING_KEY = 'receipt-credit-pricing';
const RECEIPT_CREDIT_PRICING_DEFAULT = {
    pricePHP: 59,
    discountMinQuantity: 2,
    discountPercent: 5,
    maxQuantity: 100
};
let receiptCreditPricing = { ...RECEIPT_CREDIT_PRICING_DEFAULT };
async function loadReceiptCreditPricing() {
    const stored = await getPersistentJSON(RECEIPT_CREDIT_PRICING_KEY, null);
    if (stored && typeof stored === 'object') receiptCreditPricing = { ...RECEIPT_CREDIT_PRICING_DEFAULT, ...stored };
    return receiptCreditPricing;
}
function saveReceiptCreditPricing() {
    if (pgPoolDevices || pgPool || redisClient) return setPersistentJSON(RECEIPT_CREDIT_PRICING_KEY, receiptCreditPricing);
    return null;
}
function calculateReceiptCreditQuote(quantity) {
    const maxQuantity = Math.max(1, Math.min(1000, Math.floor(Number(receiptCreditPricing.maxQuantity) || 100)));
    const qty = Math.max(1, Math.min(maxQuantity, Math.floor(Number(quantity) || 1)));
    const unitPrice = Math.max(0, Number(receiptCreditPricing.pricePHP) || 0);
    const minQty = Math.max(2, Math.floor(Number(receiptCreditPricing.discountMinQuantity) || 2));
    const discountPercent = Math.max(0, Math.min(90, Number(receiptCreditPricing.discountPercent) || 0));
    const subtotal = unitPrice * qty;
    const appliedDiscountPercent = qty >= minQty ? discountPercent : 0;
    const discount = Math.round(subtotal * appliedDiscountPercent) / 100;
    const totalPHP = Math.max(0, Math.round((subtotal - discount) * 100) / 100);
    const totalTokens = Math.ceil(totalPHP);
    return { quantity: qty, unitPrice, subtotal, discountMinQuantity: minQty, discountPercent: appliedDiscountPercent, discount, totalPHP, totalTokens, maxQuantity };
}

const pendingReceiptCreditPurchases = new Map();
function generateReceiptCreditOtp() {
    return String(Math.floor(100000 + Math.random() * 900000));
}
setInterval(() => {
    const now = Date.now();
    for (const [installationId, pending] of pendingReceiptCreditPurchases.entries()) {
        if (now > pending.expiresAt) {
            pendingReceiptCreditPurchases.delete(installationId);
        }
    }
}, 30 * 1000).unref();
app.post('/relay/request-receipt-credit-purchase',
    requireApiKey,
    requireAllowedDevice,
    rateLimit('request-receipt-credit-purchase', 5, 15 * 60 * 1000, (req) => req.body?.installationId),
    async (req, res) => {
        if (!ACTIVATION_FLAGS.otpRequestsEnabled) {
            return res.status(503).json({ success: false, message: 'Manual unlock requests ("Send Request") are temporarily disabled by the developer. Please try "Activate via Omni Tokens" instead, or try again later.' });
        }
        const { installationId, storeName, requestedBy, quantity } = req.body;
        const quote = calculateReceiptCreditQuote(quantity);
        if (!installationId) {
            return res.status(400).json({ success: false, message: 'Missing installationId.' });
        }
        const otpCode = generateReceiptCreditOtp();
        pendingReceiptCreditPurchases.set(installationId, {
            code: otpCode,
            expiresAt: Date.now() + RECEIPT_CREDIT_OTP_TTL_MS,
            approved: false,
            otpVerified: false,
            failedAttempts: 0,
            storeName: storeName || null,
            requestedBy: requestedBy || null,
            pricePHP: quote.totalPHP,
            unitPricePHP: quote.unitPrice,
            quote,
            credits: quote.quantity,
            requestedAt: Date.now()
        });
        try {
            await notifyUnlockRequest({
                subject: `💰 Receipt Customization Credit Purchase Request — ${storeName || installationId}`,
                text: `May humiling bumili ng Receipt Customization credit (para sa customization beyond sa 2 free attempts).\n\n` +
                      `Store: ${storeName || 'Hindi tiyak'}\n` +
                      `Installation ID: ${installationId}\n` +
                      `Hiniling ni: ${requestedBy || 'Hindi tiyak'}\n` +
                      `Presyo: ₱${Number(quote.totalPHP) || '?'}${quote.quantity > 1 ? ` (${quote.quantity} credits × ₱${quote.unitPrice})` : ''}\n` +
                      `OTP Code: ${otpCode}\n` +
                      `Mag-e-expire ito sa loob ng 10 minuto.\n\n` +
                      `I-VERIFY MUNA na natanggap ang bayad (GCash/Maya/Cash/etc.) bago mag-Approve at ibigay ang OTP na ito sa kliyente.`
            });
        } catch (err) {
            // NOTE: email/Slack/Telegram are best-effort notification channels only.
            // The request itself was already saved to pendingReceiptCreditPurchases above,
            // so it still shows up (with its OTP code) under "Pending Receipt Customization
            // Credit Purchases" in the RELAY admin panel even if every notification channel
            // is down/misconfigured — the admin dashboard is the source of truth, not email.
            console.error('Relay notification failure (receipt-credit-purchase) — request was still saved and is visible in the admin panel:', err);
        }
        logActivity(installationId, 'receipt_credit_purchase_requested', { storeName: storeName || null, pricePHP: quote.totalPHP, quantity: quote.quantity });
        res.json({ success: true, message: 'Naipadala ang purchase request. Tignan ng developer ang RELAY admin panel (Pending Receipt Customization Credit Purchases) para sa confirmation code, matapos ang bayad.', quote });
    }
);
app.get('/relay/receipt-credit-pricing', requireApiKey, requireAllowedDevice, (req, res) => {
    const pricing = { ...receiptCreditPricing };
    const quote = calculateReceiptCreditQuote(req.query.quantity || 1);
    res.json({ success: true, pricing, quote });
});
app.get('/relay/admin/api/pricing/receipt-credits', requireAdminKey, (req, res) => {
    res.json({ success: true, pricing: { ...receiptCreditPricing } });
});
app.post('/relay/admin/api/pricing/receipt-credits', requireAdminKey, async (req, res) => {
    const body = req.body || {};
    const pricePHP = Number(body.pricePHP);
    const discountMinQuantity = Number(body.discountMinQuantity);
    const discountPercent = Number(body.discountPercent);
    const maxQuantity = Number(body.maxQuantity);
    if (!Number.isFinite(pricePHP) || pricePHP < 0) return res.status(400).json({ success: false, message: 'Invalid Receipt Credit price.' });
    if (!Number.isInteger(discountMinQuantity) || discountMinQuantity < 2 || discountMinQuantity > 1000) return res.status(400).json({ success: false, message: 'Discount start quantity must be a whole number from 2 to 1000.' });
    if (!Number.isFinite(discountPercent) || discountPercent < 0 || discountPercent > 90) return res.status(400).json({ success: false, message: 'Discount must be between 0% and 90%.' });
    if (!Number.isInteger(maxQuantity) || maxQuantity < 1 || maxQuantity > 1000) return res.status(400).json({ success: false, message: 'Maximum quantity must be a whole number from 1 to 1000.' });
    receiptCreditPricing = { pricePHP: Math.round(pricePHP * 100) / 100, discountMinQuantity, discountPercent: Math.round(discountPercent * 100) / 100, maxQuantity };
    await saveReceiptCreditPricing();
    logActivity('SYSTEM', 'receipt_credit_pricing_updated', receiptCreditPricing);
    res.json({ success: true, pricing: { ...receiptCreditPricing } });
});
app.get('/relay/admin/api/pending-receipt-credit-purchases', requireAdminKey, (req, res) => {
    const list = [];
    for (const [installationId, pending] of pendingReceiptCreditPurchases) {
        if (Date.now() > pending.expiresAt) continue;
        list.push({
            installationId,
            storeName: pending.storeName,
            requestedBy: pending.requestedBy,
            pricePHP: pending.pricePHP,
            unitPricePHP: pending.unitPricePHP,
            quote: pending.quote,
            credits: pending.credits,
            approved: pending.approved,
            otpVerified: pending.otpVerified,
            requestedAt: pending.requestedAt,
            expiresAt: pending.expiresAt,
            code: pending.code
        });
    }
    res.json({ success: true, pending: list });
});
app.post('/relay/admin/api/pending-receipt-credit-purchases/approve', requireAdminKey, (req, res) => {
    const { installationId } = req.body;
    const pending = pendingReceiptCreditPurchases.get(installationId);
    if (!pending) {
        return res.status(404).json({ success: false, message: 'Walang pending credit-purchase request para dito.' });
    }
    pending.approved = true;
    logActivity(installationId, 'receipt_credit_purchase_approved', { pricePHP: pending.pricePHP });
    res.json({ success: true, message: 'Naaprubahan. Puwede nang gamitin ng client ang code.' });
});
app.post('/relay/confirm-receipt-credit-purchase',
    requireApiKey,
    requireAllowedDevice,
    rateLimit('confirm-receipt-credit-purchase', 120, 10 * 60 * 1000, (req) => req.body?.installationId),
    (req, res) => {
        const { installationId, otp } = req.body;
        if (!installationId || !otp) {
            return res.status(400).json({ success: false, message: 'Kulang ang installationId o otp.' });
        }
        const pending = pendingReceiptCreditPurchases.get(installationId);
        if (!pending) {
            return res.status(400).json({ success: false, message: 'Walang aktibong purchase request. Humingi muna ng code.' });
        }
        if (Date.now() > pending.expiresAt) {
            pendingReceiptCreditPurchases.delete(installationId);
            return res.status(400).json({ success: false, message: 'Expired na ang code. Humingi ng bago.' });
        }
        if (!safeCompare(String(otp).trim(), pending.code)) {
            pending.failedAttempts = (pending.failedAttempts || 0) + 1;
            if (pending.failedAttempts >= MAX_FAILED_OTP_ATTEMPTS) {
                pendingReceiptCreditPurchases.delete(installationId);
                logActivity(installationId, 'receipt_credit_purchase_locked_out', { failedAttempts: pending.failedAttempts });
                return res.status(400).json({
                    success: false,
                    message: 'Sobra na sa pinapayagang maling tangka. Nakansela ang request na ito — humiling ng bagong purchase request at code.'
                });
            }
            return res.status(400).json({ success: false, message: 'Maling code.' });
        }
        if (!checkApprovalGate(pending)) {
            return res.json({
                success: false,
                pending: true,
                message: 'Tama ang code! Naghihintay pa lang ng approval mula sa developer (i.e. kumpirmasyon ng bayad). Subukan ulit paglipas ng ilang segundo.'
            });
        }
        const now = Date.now();
        const payload = {
            installationId,
            purpose: 'receipt-customization-credit-purchase',
            credits: pending.credits || RECEIPT_CREDIT_UNITS_PER_PURCHASE,
            pricePHP: pending.pricePHP || 0,
            issuedAt: now,
            expiresAt: now + RECEIPT_CREDIT_TICKET_TTL_MS
        };
        const payloadString = JSON.stringify(payload);
        const signature = crypto.sign(null, Buffer.from(payloadString), privateKey).toString('base64');
        logActivity(installationId, 'receipt_credit_purchase_ticket_issued', { credits: payload.credits, pricePHP: pending.pricePHP });
        pendingReceiptCreditPurchases.delete(installationId);
        res.json({
            success: true,
            message: 'Na-verify ang bayad. Naidagdag na ang credit.',
            ticket: { payload, signature }
        });
    }
);
// ===================================================================
// RECEIPT CUSTOMIZATION CREDIT — TOKEN-FUNDED SELF-SERVE ACTIVATION
// Mirrors /relay/cloud-tokens/activate-cloud-backup: pays for a credit
// directly out of the installation's own Omni Token wallet instead of
// the manual "developer approves, then hands over a code" process
// used by /relay/request-receipt-credit-purchase + /relay/confirm-receipt-credit-purchase.
// Same atomic-deduct-then-issue guarantee and clientRequestId dedupe.
// Keep RECEIPT_CREDIT_PRICE_TOKENS in sync with CUSTOMIZE_CREDIT_PRICE_PHP
// in OMNIPOS/server.js (1 token = ₱1, same as the rest of the à la carte catalog).
// ===================================================================
const RECEIPT_CREDIT_PRICE_TOKENS = 59; // Legacy fallback; calculateReceiptCreditQuote() is authoritative.
const receiptCreditActivationDedupe = new Map(); // `${installationId}:${clientRequestId}` -> { status, body, expiresAt }
const RECEIPT_CREDIT_ACTIVATION_DEDUPE_TTL_MS = 15 * 60 * 1000;
setInterval(() => {
    const now = Date.now();
    for (const [key, entry] of receiptCreditActivationDedupe) {
        if (now > entry.expiresAt) receiptCreditActivationDedupe.delete(key);
    }
}, 5 * 60 * 1000).unref();
app.post('/relay/cloud-tokens/activate-receipt-credit', requireApiKey, requireAllowedDevice, rateLimit('cloud-tokens-activate-receipt-credit', 30, 10 * 60 * 1000, (req) => req.body?.installationId), async (req, res) => {
    if (!ACTIVATION_FLAGS.omniTokenActivationEnabled) {
        return res.status(503).json({ success: false, message: '"Activate via Omni Tokens" is temporarily disabled (maintenance/upgrade). Please try "Send Request" instead, or try again later.' });
    }
    const { installationId, clientRequestId } = req.body;
    if (!installationId) {
        return res.status(400).json({ success: false, message: 'Missing installationId.' });
    }
    if (!pgPool) {
        return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured — the Omni Token wallet needs it.' });
    }
    const requestedQuantity = req.body?.quantity || 1;
    const quote = calculateReceiptCreditQuote(requestedQuantity);
    const requiredTokens = quote.totalTokens;
    const dedupeKey = clientRequestId ? `${installationId}:${clientRequestId}` : null;
    if (dedupeKey && receiptCreditActivationDedupe.has(dedupeKey)) {
        const cached = receiptCreditActivationDedupe.get(dedupeKey);
        return res.status(cached.status).json(cached.body);
    }
    try {
        await getOrCreateCloudTokenWallet(installationId);
        const deductResult = await queryWithRetry(
            pgPool,
            `UPDATE cloud_token_wallets SET balance_tokens = balance_tokens - $2, updated_at = now()
             WHERE installation_id = $1 AND balance_tokens >= $2 RETURNING balance_tokens`,
            [installationId, requiredTokens]
        );
        if (!deductResult.rows[0]) {
            const currentResult = await queryWithRetry(pgPool, 'SELECT balance_tokens FROM cloud_token_wallets WHERE installation_id = $1', [installationId]);
            const insufficientBody = {
                success: false,
                insufficient: true,
                balanceTokens: currentResult.rows[0] ? Number(currentResult.rows[0].balance_tokens) : 0,
                requiredTokens,
                message: `Insufficient Omni Tokens. ${requiredTokens} token(s) are needed for a Receipt Customization credit. Please buy more Omni Tokens first, then try again.`
            };
            // Not cached as an idempotent result, same reasoning as activate-purchase:
            // a retry after topping up should make a fresh attempt, not replay "insufficient".
            return res.status(402).json(insufficientBody);
        }
        const balanceAfter = Number(deductResult.rows[0].balance_tokens);
        invalidateWalletCache(installationId);
        await queryWithRetry(
            pgPool,
            `INSERT INTO cloud_token_ledger (installation_id, type, tokens, balance_after, note, category) VALUES ($1, 'consume', $2, $3, $4, 'ADDON_PURCHASE')`,
            [installationId, -requiredTokens, balanceAfter, `Receipt Customization credit purchase (${quote.quantity} credit(s), self-serve via Omni Tokens; ₱${quote.totalPHP.toFixed(2)})`]
        );
        const now = Date.now();
        const payload = {
            installationId,
            purpose: 'receipt-customization-credit-purchase',
            credits: quote.quantity,
            pricePHP: quote.totalPHP || 0,
            issuedAt: now,
            expiresAt: now + RECEIPT_CREDIT_TICKET_TTL_MS
        };
        const payloadString = JSON.stringify(payload);
        const signature = crypto.sign(null, Buffer.from(payloadString), privateKey).toString('base64');
        logActivity(installationId, 'receipt_credit_purchase_ticket_issued', { credits: payload.credits, source: 'cloud_token_selfserve', tokensSpent: requiredTokens });
        const successBody = {
            success: true,
            message: `Payment confirmed via Omni Tokens — ${quote.quantity} customization credit(s) added.`,
            ticket: { payload, signature },
            balanceTokens: balanceAfter,
            tokensSpent: requiredTokens
        };
        if (dedupeKey) {
            receiptCreditActivationDedupe.set(dedupeKey, { status: 200, body: successBody, expiresAt: Date.now() + RECEIPT_CREDIT_ACTIVATION_DEDUPE_TTL_MS });
        }
        res.json(successBody);
    } catch (err) {
        console.error('cloud-tokens/activate-receipt-credit error:', err);
        res.status(500).json({ success: false, message: err.message });
    }
});
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
app.get('/relay/release-package', requireApiKey, rateLimit('release-package', 10, 60 * 60 * 1000), async (req, res) => {
    await ensureReleasePackageFreshOnDisk();
    if (!fs.existsSync(RELEASE_PACKAGE_PATH)) {
        return res.status(503).json({ success: false, message: 'Walang naka-publish na release package sa RELAY pa.' });
    }
    logActivity(null, 'release_package_self_update_fetch', { ip: req.ip });
    res.download(RELEASE_PACKAGE_PATH, 'omnipos-client.zip');
});
app.post('/relay/admin/api/system/publish-version', requireAdminKey, async (req, res) => {
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
        const persisted = await saveTargetedReleases(targetedReleases);
        return res.json({
            success: true, targeted: true, installationId: targetId, release: entry,
            warning: (!persisted && pgPoolBuild) ? 'PAALALA: Hindi na-save sa Neon build DB ang targeted release na ito — lokal na memory lang muna ito ng instance na ito.' : undefined
        });
    }
    systemVersionInfo = entry;
    saveSystemVersionInfo(systemVersionInfo);
    res.json({ success: true, targeted: false, systemVersionInfo });
});
app.get('/relay/admin/api/system/version', requireAdminKey, (req, res) => {
    res.json({ success: true, systemVersionInfo });
});
// Ligtas at read-only lang ito — hindi nito binabago ang anumang data, hindi
// rin nito inilalantad ang buong connection string (username/password), para
// ma-verify lang sa admin dashboard kung talagang hiwalay na ang Cloud Backup
// database sa Device/License database.
function maskDatabaseUrlForDisplay(urlStr) {
    if (!urlStr) return null;
    try {
        const parsed = new URL(urlStr);
        return {
            host: parsed.hostname || null,
            database: parsed.pathname ? parsed.pathname.replace(/^\//, '') || null : null
        };
    } catch (err) {
        return { host: null, database: null };
    }
}
// AYOS: idinagdag ang latencyMs (Date.now() bago/pagkatapos ng query, para
// makita kung "reachable pero mabagal" — hindi lang plain yes/no) pati na
// rin ang live pool stats mula mismo sa pg library (totalCount/idleCount/
// waitingCount, walang extra query, laging tinatrack na ito ng `pg`)
// para makita agad kung papalapit sa pagka-exhaust ang connection pool.
async function checkPgPoolReachable(pool, timeoutMs = 5000) {
    if (!pool) return { configured: false, reachable: false, error: null, latencyMs: null, pool: null };
    const startedAt = Date.now();
    const poolStats = () => ({
        totalCount: pool.totalCount,
        idleCount: pool.idleCount,
        waitingCount: pool.waitingCount
    });
    try {
        await Promise.race([
            pool.query('SELECT 1'),
            new Promise((_, reject) => setTimeout(() => reject(new Error('Timed out while checking this database.')), timeoutMs))
        ]);
        return { configured: true, reachable: true, error: null, latencyMs: Date.now() - startedAt, pool: poolStats() };
    } catch (err) {
        return { configured: true, reachable: false, error: err.message, latencyMs: Date.now() - startedAt, pool: poolStats() };
    }
}
// ===================================================================
// AYOS/BAGO: hiwalay na health check para sa NEON ACCOUNT API mismo
// (NEON_API_KEY) — hindi ito katulad ng 3 check sa itaas, na Postgres
// CONNECTION lang (DATABASE_URL, atbp.) ang sinusuri. Ang NEON_API_KEY
// ay ibang credential — ginagamit lang para kunin ang REAL na plan/
// usage mismo mula sa Neon account (tingnan ang getNeonProjectUsage() /
// computeNeonRealCost() sa itaas).
//
// Bago ito: kung invalid/expired/na-revoke ang key na ito, TAHIMIK
// itong babagsak sa "fallback" na dropdown plan sa loob ng bawat
// database card ng Storage Usage & Neon Pricing (may maliit na warning
// text doon, pero madaling ma-miss). Ngayon, may MALINAW at HIWALAY
// nang REACHABLE/UNREACHABLE/NOT CONFIGURED na badge dito sa Database
// Health mismo — kaya isang tingin lang sa itaas, alam na kaagad kung
// may problema sa key bago pa man tignan isa-isa ang bawat card sa
// baba.
//
// Tumatawag sa GET /api/v2/projects?limit=1 (pinaka-magaan na endpoint,
// hindi umaasa sa isang partikular na project ID) — walang epekto sa
// datos, read-only lang, kaparehong pattern ng SELECT 1 checks sa itaas.
// ===================================================================
async function checkNeonApiKeyReachable(timeoutMs = 8000) {
    if (!NEON_API_KEY) return { configured: false, reachable: false, error: null, latencyMs: null };
    const startedAt = Date.now();
    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeoutMs);
        const resp = await fetch('https://console.neon.tech/api/v2/projects?limit=1', {
            headers: { 'Authorization': `Bearer ${NEON_API_KEY}`, 'Accept': 'application/json' },
            signal: controller.signal
        });
        clearTimeout(timeout);
        const latencyMs = Date.now() - startedAt;
        if (!resp.ok) {
            let detail = `HTTP ${resp.status}`;
            if (resp.status === 401 || resp.status === 403) detail += ' — mali/expired/walang access ang NEON_API_KEY (i-check sa Neon console kung buhay pa ito)';
            else if (resp.status === 429) detail += ' — na-rate-limit ng Neon API (subukan ulit mamaya)';
            return { configured: true, reachable: false, error: detail, latencyMs };
        }
        return { configured: true, reachable: true, error: null, latencyMs };
    } catch (err) {
        const isTimeout = err.name === 'AbortError';
        return { configured: true, reachable: false, error: isTimeout ? 'Timed out while checking the Neon Account API.' : err.message, latencyMs: Date.now() - startedAt };
    }
}
// Maikling in-memory cache lang (hindi persisted, hindi shared sa ibang
// process) para kung ipapa-poll ito ng dashboard kada ilang segundo,
// hindi paulit-ulit na tinatamaan ng live SELECT 1 ang parehong dalawang
// database sa bawat request.
let dbStatusCache = { at: 0, payload: null };
const DB_STATUS_CACHE_MS = 4000;
app.get('/relay/admin/api/db-status', requireAdminKey, async (req, res) => {
    try {
        if (dbStatusCache.payload && (Date.now() - dbStatusCache.at) < DB_STATUS_CACHE_MS) {
            return res.json({ ...dbStatusCache.payload, cached: true });
        }
        const [cloudBackupCheck, devicesCheck, buildCheck, neonApiKeyCheck] = await Promise.all([
            checkPgPoolReachable(pgPool),
            checkPgPoolReachable(pgPoolDevices),
            checkPgPoolReachable(pgPoolBuild),
            checkNeonApiKeyReachable()
        ]);
        const payload = {
            success: true,
            checkedAt: Date.now(),
            isSeparateDatabase: DEVICES_DB_IS_SEPARATE,
            cloudBackup: {
                ...cloudBackupCheck,
                envVar: 'DATABASE_URL',
                connection: maskDatabaseUrlForDisplay(DATABASE_URL)
            },
            devices: {
                ...devicesCheck,
                envVar: process.env.RELAY_DEVICES_DATABASE_URL ? 'RELAY_DEVICES_DATABASE_URL' : 'DATABASE_URL (fallback — RELAY_DEVICES_DATABASE_URL not set)',
                connection: maskDatabaseUrlForDisplay(DEVICES_DATABASE_URL)
            },
            build: {
                ...buildCheck,
                envVar: 'RELAY_BUILD_DATABASE_URL',
                connection: maskDatabaseUrlForDisplay(BUILD_DATABASE_URL)
            },
            // AYOS/BAGO: hiwalay na check para sa NEON_API_KEY mismo (tingnan
            // ang checkNeonApiKeyReachable() sa itaas para sa buong paliwanag)
            // — ibang credential ito kumpara sa 3 DATABASE_URL check sa itaas.
            neonApiKey: {
                ...neonApiKeyCheck,
                envVar: 'NEON_API_KEY',
                projectIdsConfigured: {
                    cloudBackup: !!NEON_CLOUD_BACKUP_PROJECT_ID,
                    devices: !!NEON_DEVICES_PROJECT_ID,
                    build: !!NEON_BUILD_PROJECT_ID
                }
            }
        };
        dbStatusCache = { at: Date.now(), payload };
        res.json({ ...payload, cached: false });
    } catch (err) {
        console.error('⚠️  /relay/admin/api/db-status error:', err.message);
        res.status(500).json({ success: false, message: 'Could not determine database status.' });
    }
});
// ===================================================================
// DATABASE HEALTH — real na storage usage mula sa Neon (pg_database_size),
// kasama ang natitirang allowance (kung Free plan) at ang buong Neon
// pricing/tier comparison (USD + PHP) para makatulong sa desisyon ng
// developer kung kailan/anong tier dapat mag-upgrade. Tingnan ang
// NEON_PRICING_BASE sa itaas — manual na pinapanatiling updated ito
// laban sa https://neon.com/docs/introduction/plans.
// ===================================================================
async function getPgDatabaseSizeBytes(pool) {
    if (!pool) return null;
    try {
        const result = await pool.query('SELECT pg_database_size(current_database()) AS bytes');
        const bytes = result.rows && result.rows[0] ? Number(result.rows[0].bytes) : null;
        return Number.isFinite(bytes) ? bytes : null;
    } catch (err) {
        console.warn('⚠️  Hindi makuha ang pg_database_size:', err.message);
        return null;
    }
}
async function getTopTableSizes(pool, limit = 8) {
    if (!pool) return [];
    try {
        const result = await pool.query(`
            SELECT relname AS table_name, pg_total_relation_size(relid) AS bytes
            FROM pg_catalog.pg_statio_user_tables
            ORDER BY pg_total_relation_size(relid) DESC
            LIMIT $1
        `, [limit]);
        return result.rows.map(r => ({ table: r.table_name, bytes: Number(r.bytes) }));
    } catch (err) {
        console.warn('⚠️  Hindi makuha ang per-table sizes:', err.message);
        return [];
    }
}
function bytesToGB(bytes) {
    return typeof bytes === 'number' ? bytes / (1024 * 1024 * 1024) : null;
}
// Tinatantya lang ang buong Neon bill base sa CURRENT storage usage
// (walang paraan ang RELAY na malaman ang aktwal na compute-hours na
// nagamit nang hindi kumonekta sa Neon's own API/account) — kaya ang
// "computeCostUSD" dito ay isang ILLUSTRATIVE example lang gamit ang
// buong buwan (720 oras) ng average compute size, HINDI aktwal na
// bill. Malinaw itong nakalagay sa response bilang paalala.
function computeNeonTierEstimate(tierId, storageGB, exampleComputeCUHours) {
    const tier = NEON_PRICING[tierId];
    if (!tier) return null;
    if (tier.customPricing) {
        return { tier: tierId, name: tier.name, customPricing: true, notes: tier.notes };
    }
    if (tierId === 'free') {
        const overStorage = storageGB > tier.includedStorageGB;
        return {
            tier: tierId, name: tier.name, monthlyCostUSD: 0,
            fitsInFreeStorage: !overStorage,
            storageUsedGB: storageGB, storageIncludedGB: tier.includedStorageGB,
            storageRemainingGB: Math.max(0, tier.includedStorageGB - storageGB),
            storagePercentUsed: tier.includedStorageGB > 0 ? Math.min(999, (storageGB / tier.includedStorageGB) * 100) : null,
            computeHoursIncluded: tier.includedComputeHours,
            notes: overStorage ? '⚠️ Lumampas na sa 0.5 GB free storage cap — kailangan nang mag-upgrade.' : tier.notes
        };
    }
    const storageCostUSD = storageGB * tier.storageRatePerGBMonthUSD;
    const computeCostUSD = typeof exampleComputeCUHours === 'number' ? exampleComputeCUHours * tier.computeRatePerCUHourUSD : null;
    return {
        tier: tierId, name: tier.name,
        storageCostUSD: Math.round(storageCostUSD * 100) / 100,
        exampleComputeCostUSD: computeCostUSD !== null ? Math.round(computeCostUSD * 100) / 100 : null,
        exampleComputeCUHoursAssumed: exampleComputeCUHours,
        estimatedMonthlyTotalUSD: computeCostUSD !== null ? Math.round((storageCostUSD + computeCostUSD) * 100) / 100 : null,
        computeRatePerCUHourUSD: tier.computeRatePerCUHourUSD,
        storageRatePerGBMonthUSD: tier.storageRatePerGBMonthUSD,
        notes: tier.notes
    };
}
let dbHealthCache = { at: 0, payload: null };
const DB_HEALTH_CACHE_MS = 15000;
app.get('/relay/admin/api/db-health', requireAdminKey, async (req, res) => {
    try {
        if (dbHealthCache.payload && (Date.now() - dbHealthCache.at) < DB_HEALTH_CACHE_MS && req.query.force !== '1') {
            return res.json({ ...dbHealthCache.payload, cached: true });
        }
        const [cloudBackupBytes, devicesBytes, buildBytes, cloudBackupTables, devicesTables, buildTables, exchangeRate, cloudBackupNeonUsage, devicesNeonUsage, buildNeonUsage] = await Promise.all([
            getPgDatabaseSizeBytes(pgPool),
            getPgDatabaseSizeBytes(pgPoolDevices),
            getPgDatabaseSizeBytes(pgPoolBuild),
            getTopTableSizes(pgPool),
            getTopTableSizes(pgPoolDevices),
            getTopTableSizes(pgPoolBuild),
            getUsdToPhpRate(),
            getNeonProjectUsage(NEON_CLOUD_BACKUP_PROJECT_ID),
            getNeonProjectUsage(NEON_DEVICES_PROJECT_ID),
            getNeonProjectUsage(NEON_BUILD_PROJECT_ID)
        ]);
        // Halimbawang compute assumption — ito na lang ang FALLBACK kapag
        // wala pang totoong Neon usage data (o wala pang isang oras na
        // lumipas sa billing period, kaya hindi pa stable ang projection).
        // Katumbas ng 0.25 CU na tumatakbo nang 8 oras/araw sa loob ng 30 araw.
        const EXAMPLE_COMPUTE_CU_HOURS = 0.25 * 8 * 30;
        function buildDbEntry(label, bytes, tables, configuredPlan, neonUsage) {
            const gb = bytesToGB(bytes);
            // Kung may totoong Neon usage na, i-project ang TUNAY na average
            // papuntang buong billing period — ito ang gagamitin sa tier
            // comparison table imbes na yung generic na canned example, para
            // consistent ang numero dito sa REAL usage box sa itaas.
            const projection = neonUsage ? projectFullPeriodCUHours(neonUsage) : null;
            const computeCUHoursForComparison = projection ? projection.projectedCUHoursFullPeriod : EXAMPLE_COMPUTE_CU_HOURS;
            const tierEstimates = NEON_PRICING_TIER_IDS.map(t => computeNeonTierEstimate(t, gb === null ? 0 : gb, computeCUHoursForComparison));
            return {
                label,
                configuredPlan,
                bytes,
                mb: bytes !== null ? Math.round((bytes / (1024 * 1024)) * 100) / 100 : null,
                gb: gb !== null ? Math.round(gb * 10000) / 10000 : null,
                topTables: tables,
                currentTierEstimate: tierEstimates.find(t => t.tier === configuredPlan) || null,
                tierComparison: tierEstimates,
                // Ipinapakita kung saan galing ang compute assumption na ginamit
                // sa tierComparison sa itaas — para malinaw sa UI (at sa
                // sinumang gumagamit ng API na ito) kung TUNAY na projection
                // ba ito o canned example lang pa rin.
                tierComparisonBasis: projection ? 'real-projection' : 'illustrative-example',
                usageProjection: projection,
                // REAL na usage/cost mula sa Neon account API (kung naka-configure
                // ang NEON_API_KEY + project ID) — null kung wala pang naka-set.
                realUsage: neonUsage,
                realCost: neonUsage ? computeNeonRealCost(neonUsage, configuredPlan) : null
            };
        }
        const payload = {
            success: true,
            checkedAt: Date.now(),
            exchangeRate: {
                usdToPhp: exchangeRate.rate,
                source: exchangeRate.source,
                fetchedAt: exchangeRate.fetchedAt
            },
            pricingMeta: {
                verifiedAt: NEON_PRICING_VERIFIED_AT,
                staleness: getNeonPricingStaleness(),
                sourceUrl: NEON_PRICING_SOURCE_URL,
                hasOverrides: Object.keys(neonPricingOverrides).length > 0,
                exampleComputeCUHoursAssumed: EXAMPLE_COMPUTE_CU_HOURS,
                hasNeonApiKey: NEON_API_CONFIGURED,
                exampleComputeAssumptionNote: NEON_API_CONFIGURED
                    ? 'May naka-configure nang Neon account API. Ang "realUsage"/"realCost" bawat database ay TUNAY na compute/storage sa kasalukuyang billing period. Sa tier comparison table sa ibaba, kung may sapat nang datos (>1 oras mula nag-reset ang period), ang "Est. total" ay PROJECTION na mula sa totoong average usage niyo (tingnan ang "tierComparisonBasis": "real-projection" bawat database) — hindi na canned example. Kung bagong-bago pa lang ang period, pansamantalang canned example muna ("illustrative-example") habang wala pang sapat na datos.'
                    : 'Ang exampleComputeCostUSD/estimatedMonthlyTotalUSD ay HALIMBAWA LANG (0.25 CU × 8 oras/araw × 30 araw) — hindi ito aktwal na compute usage niyo. Para makita ang TUNAY na usage, i-set ang NEON_API_KEY + NEON_CLOUD_BACKUP_PROJECT_ID (at NEON_DEVICES_PROJECT_ID kung hiwalay) sa .env. Ang storageCostUSD lang ang base sa TUNAY na kasalukuyang laki ng database sa ngayon.'
            },
            databases: {
                cloudBackup: buildDbEntry('Cloud Backup (DATABASE_URL)', cloudBackupBytes, cloudBackupTables, neonConfiguredPlans.cloudBackup || 'free', cloudBackupNeonUsage),
                devices: buildDbEntry('Devices / License' + (DEVICES_DB_IS_SEPARATE ? ' (RELAY_DEVICES_DATABASE_URL)' : ' (shared sa DATABASE_URL)'), devicesBytes, devicesTables, neonConfiguredPlans.devices || 'free', devicesNeonUsage),
                build: buildDbEntry('Build / Push' + (pgPoolBuild ? ' (RELAY_BUILD_DATABASE_URL)' : ' (not configured — walang RELAY_BUILD_DATABASE_URL)'), buildBytes, buildTables, neonConfiguredPlans.build || 'free', buildNeonUsage)
            },
            neonPricing: NEON_PRICING
        };
        dbHealthCache = { at: Date.now(), payload };
        res.json({ ...payload, cached: false });
    } catch (err) {
        console.error('⚠️  /relay/admin/api/db-health error:', err.message);
        res.status(500).json({ success: false, message: 'Could not compute database health.' });
    }
});
app.post('/relay/admin/api/db-health/plan', requireAdminKey, (req, res) => {
    const { database, tier } = req.body || {};
    if (!['cloudBackup', 'devices', 'build'].includes(database)) {
        return res.status(400).json({ success: false, message: 'Invalid database (cloudBackup/devices/build).' });
    }
    if (!NEON_PRICING_TIER_IDS.includes(tier)) {
        return res.status(400).json({ success: false, message: `Invalid tier. Options: ${NEON_PRICING_TIER_IDS.join(', ')}.` });
    }
    neonConfiguredPlans = { ...neonConfiguredPlans, [database]: tier };
    saveNeonConfiguredPlans(neonConfiguredPlans);
    dbHealthCache = { at: 0, payload: null };
    console.log(`🗄️  Na-set ang configured Neon plan ng "${database}" tungong "${tier}" via admin panel.`);
    res.json({ success: true, neonConfiguredPlans });
});
app.get('/relay/admin/api/pricing/neon', requireAdminKey, (req, res) => {
    res.json({
        success: true,
        neonPricing: NEON_PRICING,
        neonPricingBase: NEON_PRICING_BASE,
        neonPricingOverrides,
        verifiedAt: NEON_PRICING_VERIFIED_AT,
        staleness: getNeonPricingStaleness(),
        sourceUrl: NEON_PRICING_SOURCE_URL
    });
});
app.post('/relay/admin/api/pricing/neon', requireAdminKey, (req, res) => {
    const { tier, computeRatePerCUHourUSD, storageRatePerGBMonthUSD, includedStorageGB, includedComputeHours } = req.body || {};
    if (!tier || !NEON_PRICING_BASE[tier] || NEON_PRICING_BASE[tier].customPricing) {
        return res.status(400).json({ success: false, message: `Invalid tier. Options: free, launch, scale.` });
    }
    const numericFields = { computeRatePerCUHourUSD, storageRatePerGBMonthUSD, includedStorageGB, includedComputeHours };
    for (const [key, val] of Object.entries(numericFields)) {
        if (val !== undefined && (typeof val !== 'number' || !isFinite(val) || val < 0)) {
            return res.status(400).json({ success: false, message: `Invalid ${key}.` });
        }
    }
    const existing = neonPricingOverrides[tier] || {};
    const updated = { ...existing };
    if (typeof computeRatePerCUHourUSD === 'number') updated.computeRatePerCUHourUSD = computeRatePerCUHourUSD;
    if (typeof storageRatePerGBMonthUSD === 'number') updated.storageRatePerGBMonthUSD = storageRatePerGBMonthUSD;
    if (typeof includedStorageGB === 'number') updated.includedStorageGB = includedStorageGB;
    if (typeof includedComputeHours === 'number') updated.includedComputeHours = includedComputeHours;
    neonPricingOverrides[tier] = updated;
    saveNeonPricingOverrides(neonPricingOverrides);
    recomputeNeonPricing();
    dbHealthCache = { at: 0, payload: null };
    console.log(`🗄️  Na-update ang Neon pricing override para sa "${tier}" via admin panel (Neon updated their pricing).`);
    res.json({ success: true, neonPricing: NEON_PRICING[tier] });
});
app.post('/relay/admin/api/pricing/neon/reset', requireAdminKey, (req, res) => {
    const { tier } = req.body || {};
    if (!tier || !NEON_PRICING_BASE[tier]) {
        return res.status(400).json({ success: false, message: 'Invalid tier.' });
    }
    delete neonPricingOverrides[tier];
    saveNeonPricingOverrides(neonPricingOverrides);
    recomputeNeonPricing();
    dbHealthCache = { at: 0, payload: null };
    res.json({ success: true, neonPricing: NEON_PRICING[tier] });
});
// AYOS/BAGO: admin endpoint para tingnan/i-adjust ang CONSERVATIVE na
// compute-cost assumption na ginagamit sa REAL-TIME na Cloud Backup
// per-sync charge (tingnan ang computeRealCloudBackupSyncCostPHP() sa
// itaas at ang cloudBackupSyncCompute sa NEON_PRICING_BASE para sa buong
// paliwanag). Gamitin ang Client Cost Allocation report (na kumukuha ng
// AKTWAL na Neon compute usage mula sa Neon account API) bilang batayan
// kung dapat itaas ang mga value dito — kung mas mataas ang totoong
// compute bill kumpara sa nako-kolekta sa mga customer, itaas ang
// assumedCU/assumedBaseSeconds/assumedSecondsPerMB para mas ligtas
// (mas malaki ang kolektahin bawat sync).
app.get('/relay/admin/api/pricing/cloud-backup-sync-compute', requireAdminKey, (req, res) => {
    const override = neonPricingOverrides.cloudBackupSyncCompute || {};
    res.json({
        success: true,
        cloudBackupSyncCompute: NEON_PRICING.cloudBackupSyncCompute,
        cloudBackupSyncComputeBase: NEON_PRICING_BASE.cloudBackupSyncCompute,
        cloudBackupSyncComputeOverride: neonPricingOverrides.cloudBackupSyncCompute || null,
        // AYOS/BAGO: transparency para sa self-calibrating na measurement —
        // makikita ng admin kung ilang TOTOONG sync sample na ang na-record,
        // at kung na-recalibrate na (gamit ang totoong measured duration) o
        // seed default pa rin ang ginagamit.
        calibration: {
            calibrated: !!override.calibrated,
            sampleCount: Number(override.measuredSampleCount) || 0,
            minSamplesNeeded: CLOUD_BACKUP_TIMING_MIN_SAMPLES
        }
    });
});
app.post('/relay/admin/api/pricing/cloud-backup-sync-compute', requireAdminKey, (req, res) => {
    const { assumedCU, assumedBaseSeconds, assumedSecondsPerMB } = req.body || {};
    const numericFields = { assumedCU, assumedBaseSeconds, assumedSecondsPerMB };
    for (const [key, val] of Object.entries(numericFields)) {
        if (val !== undefined && (typeof val !== 'number' || !isFinite(val) || val < 0)) {
            return res.status(400).json({ success: false, message: `Invalid ${key}.` });
        }
    }
    const existing = neonPricingOverrides.cloudBackupSyncCompute || {};
    const updated = { ...existing };
    if (typeof assumedCU === 'number') updated.assumedCU = assumedCU;
    if (typeof assumedBaseSeconds === 'number') updated.assumedBaseSeconds = assumedBaseSeconds;
    if (typeof assumedSecondsPerMB === 'number') updated.assumedSecondsPerMB = assumedSecondsPerMB;
    neonPricingOverrides.cloudBackupSyncCompute = updated;
    saveNeonPricingOverrides(neonPricingOverrides);
    recomputeNeonPricing();
    console.log('🗄️  Na-update ang Cloud Backup per-sync compute-cost assumption via admin panel.');
    res.json({ success: true, cloudBackupSyncCompute: NEON_PRICING.cloudBackupSyncCompute });
});
app.post('/relay/admin/api/pricing/cloud-backup-sync-compute/reset', requireAdminKey, (req, res) => {
    delete neonPricingOverrides.cloudBackupSyncCompute;
    saveNeonPricingOverrides(neonPricingOverrides);
    recomputeNeonPricing();
    res.json({ success: true, cloudBackupSyncCompute: NEON_PRICING.cloudBackupSyncCompute });
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
    // Printer Device Settings (Bluetooth brand presets + WiFi/LAN network
    // printing) — proprietary code, must ship/obfuscate alongside bt-printer.js.
    path.join('public', 'printer-manager.js'),
    path.join('public', 'faq-engine.js'),
    path.join('public', 'faq-knowledge.js'),
    path.join('public', 'faq-knowledge.en.js'),
    path.join('public', 'faq-lang.js'),
    // BUG FIX: nakaligtaan — proprietary code rin ito (cache-shell list,
    // PWA logic), hindi third-party lib, pero wala sa dating list kaya
    // plain/readable pa rin ito sa loob ng omnipos-client.zip kahit
    // obfuscated na ang lahat ng kapatid nitong public/*.js files.
    path.join('public', 'service-worker.js'),
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
// Simpleng in-process lock (per Render instance) para maiwasan ang dalawang
// magkasabay na build (hal. double-click sa admin panel, o /build-release at
// /system/publish-release na parehong tinawag halos sabay) na maglaro sa
// parehong RELEASE_PACKAGE_TMP_PATH/tmpDir at magresulta sa sirang zip o
// nagkakasalungat na build-progress state.
let buildInProgress = false;
// AYOS/BUGFIX (natitigil/hindi gumagalaw ang "~Ns natitira" sa panahon ng
// obfuscation): dati, iisang beses lang ina-update/nire-recompute ang
// buildProgressState.etaMs — sa bawat pagtapos ng isang FILE (onProgress
// callback ng obfuscateReleaseTree, tingnan sa ibaba). Kung malaki ang
// isang SOLONG file (hal. server.js, public/app.js — daan-daang KB),
// puwedeng ilang segundo itong tumagal sa loob ng worker thread nang
// walang kahit anong progress event sa buong panahong iyon — kaya
// "nakafreeze" ang etaMs (at ang ipinapakitang countdown sa admin panel)
// hanggang matapos ang buong file, kahit patuloy namang tumatakbo ang
// totoong oras. Itong heartbeat na ito ay tumatakbo bawat segundo habang
// may aktibong build, tumatawag ng setBuildProgress({}) (walang binabago
// sa percent/stage/message) — sapat na ito para patuloy na ma-recompute
// ang etaMs batay sa totoong elapsed time (Date.now() - startedAt), kaya
// palaging "buhay"/tumatakbo ang ipinapakitang numero anuman kalaki ang
// kasalukuyang file.
let buildProgressHeartbeatTimer = null;
function startBuildProgressHeartbeat() {
    stopBuildProgressHeartbeat();
    buildProgressHeartbeatTimer = setInterval(() => {
        if (buildProgressState.done) { stopBuildProgressHeartbeat(); return; }
        setBuildProgress({});
    }, 1000);
}
function stopBuildProgressHeartbeat() {
    if (buildProgressHeartbeatTimer) { clearInterval(buildProgressHeartbeatTimer); buildProgressHeartbeatTimer = null; }
}
async function performBuildRelease(reqBody, req, publishOverride) {
    if (buildInProgress) {
        const err = new Error('May kasalukuyang build pa rin na tumatakbo — hintayin munang matapos ito bago mag-request ng bago.');
        err.statusCode = 409;
        throw err;
    }
    buildInProgress = true;
    startBuildProgressHeartbeat();
    try {
        return await performBuildReleaseInner(reqBody, req, publishOverride);
    } finally {
        buildInProgress = false;
        stopBuildProgressHeartbeat();
    }
}
async function performBuildReleaseInner(reqBody, req, publishOverride) {
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
        // AYOS: dating 3 segundo ang default na poll interval ng client para sa
        // /relay/pending-integrity-check (tingnan ang INTEGRITY_CHECK_NOW_POLL_MS
        // sa OMNIPOS client server.js) — masyadong mataas ang bandwidth/request
        // volume nito kapag maraming client na (hal. ~860MB/buwan/client sa 3 sec,
        // laban sa 5GB/buwan na free-tier bandwidth cap ng Render ngayon).
        // Ginagawa itong 5 minuto (300000ms) dito para awtomatikong ma-apply sa
        // bawat BAGONG client build/push, nang hindi kailangang baguhin ang
        // OMNIPOS source code mismo. Pwede itong i-override kada build sa
        // pamamagitan ng `reqBody.integrityCheckNowPollMs` o ng
        // RELAY_INTEGRITY_CHECK_NOW_POLL_MS env var dito sa RELAY.
        const integrityCheckNowPollMs = (reqBody && reqBody.integrityCheckNowPollMs)
            || process.env.RELAY_INTEGRITY_CHECK_NOW_POLL_MS
            || 300000;
        // AYOS DIN: si RELAY_FEATURE_SYNC_INTERVAL_MS (default 30 seg sa OMNIPOS
        // client) ay nagpapatakbo ng DALAWANG relay request kada cycle
        // (restore-tokens + check-feature-status) — mas malaki pa ang epekto nito
        // sa bandwidth kaysa sa integrity poll dahil mas malaki ang payload
        // (may listahan ng feature IDs). Ginawa ring 5 minuto (300000ms) ito.
        // Ligtas itong bagalan dahil may hiwalay na ON-DEMAND na endpoint na
        // pa rin sa client (/api/features/restore-check) na tumatawag agad sa
        // parehong function — kaya kahit pabagalin ang passive/background
        // interval, hindi apektado ang mga sitwasyong kailangan ng
        // agad-agarang sync (hal. pagkatapos bumili/mag-restore).
        const featureSyncIntervalMs = (reqBody && reqBody.featureSyncIntervalMs)
            || process.env.RELAY_FEATURE_SYNC_INTERVAL_MS
            || 300000;
        const clientEnvContent = [
            `RELAY_URL=${relayUrl}`,
            `RELAY_API_KEY=${relayApiKey}`,
            `PORT=${clientPort}`,
            `IMAGE_SEARCH_PROVIDER=${imageSearchProvider}`,
            `IMAGE_SEARCH_API_KEY=${imageSearchApiKey}`,
            `IMAGE_SEARCH_CX=${imageSearchCx}`,
            `RELAY_INTEGRITY_CHECK_NOW_POLL_MS=${integrityCheckNowPollMs}`,
            `RELAY_FEATURE_SYNC_INTERVAL_MS=${featureSyncIntervalMs}`,
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
        setBuildProgress({ stage: 'verify', percent: BUILD_STEP_END_PERCENT.verify, message: 'Sine-save ang release package sa Neon build DB...' });
        const persistedToNeon = await saveReleasePackageToBuildDb(RELEASE_PACKAGE_PATH);
        if (!persistedToNeon && pgPoolBuild) {
            // Nabuo ang zip at nasa lokal na disk NG INSTANCE NA ITO, pero
            // HINDI ito na-save sa Neon build DB (hal. temporary connectivity
            // issue). Kung ma-restart/ma-redeploy bago ito ma-retry, mababalik
            // ito sa dating (mas lumang) na-publish na version. Ipinapaalam
            // ito sa admin sa halip na tahimik lang na-log sa server console.
            console.warn('⚠️  Nabuo ang release pero HINDI ito na-persist sa Neon build DB — lokal na disk lang muna ito ng instance na ito.');
        }
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
        const baselinePersisted = await saveReleaseBaselines(releaseBaselines);
        console.log(`🔐 Integrity baseline saved para sa version ${baselineVersion} (${Object.keys(baselineFiles).length} file(s)).`);
        const historyPersisted = await recordBuildHistoryEntry({
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
        // Kolektahin ang lahat ng "hindi na-persist sa Neon" na warning (zip
        // mismo, integrity baseline, build history) sa IISANG warning string —
        // para hindi kailangang hanapin ng admin sa maraming fields, buong
        // larawan agad ng kung ano ang tunay na naka-Neon vs. lokal na disk lang.
        const persistWarnings = [];
        if (!persistedToNeon && pgPoolBuild) persistWarnings.push('release zip');
        if (!baselinePersisted && pgPoolBuild) persistWarnings.push('integrity baseline');
        if (!historyPersisted && pgPoolBuild) persistWarnings.push('build history entry');
        let warning;
        if (persistWarnings.length > 0) {
            warning = `PAALALA: Nabuo ang release pero HINDI na-save sa Neon build DB ang: ${persistWarnings.join(', ')}. Kung ma-restart/ma-redeploy ang RELAY bago ito ma-retry, posibleng mawala/mababalik sa luma ang mga ito. I-retry ang build o suriin ang RELAY_BUILD_DATABASE_URL connection.`;
        } else if (!pgPoolBuild) {
            warning = 'PAALALA: Walang RELAY_BUILD_DATABASE_URL na naka-configure — lokal na ephemeral disk lang ang release package/build metadata, mawawala ito kapag nag-restart/na-redeploy.';
        }
        return {
            message: envResult.encrypted
                ? `Nagawa ang bagong release package. ${obfMsgPart}, naka-encrypt na ang .env.`
                : `Nagawa ang bagong release package. ${obfMsgPart}. PAALALA: walang env-loader.js sa repo mo — plaintext pa rin ang .env.`,
            sizeBytes: stats.size,
            obfuscatedFiles: obfuscatedCount,
            obfuscated: shouldObfuscate,
            envEncrypted: envResult.encrypted,
            builtAt,
            persistedToNeon,
            warning
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
app.post('/relay/admin/api/system/targeted-releases/:installationId/clear', requireAdminKey, async (req, res) => {
    const { installationId } = req.params;
    const existed = targetedReleases.delete(installationId);
    let persisted = true;
    if (existed) persisted = await saveTargetedReleases(targetedReleases);
    res.json({
        success: true, cleared: existed,
        warning: (existed && !persisted && pgPoolBuild) ? 'PAALALA: Hindi na-save sa Neon build DB ang pag-clear na ito — posibleng bumalik ito kapag nag-restart ang server.' : undefined
    });
});
app.get('/relay/admin/api/build-history', requireAdminKey, (req, res) => {
    res.json({ success: true, history: buildHistory });
});
app.post('/relay/admin/api/build-history/clear', requireAdminKey, async (req, res) => {
    const clearedCount = buildHistory.length;
    buildHistory = [];
    const persisted = await saveBuildHistory(buildHistory);
    logActivity(null, 'build_history_cleared', { clearedCount });
    res.json({
        success: true, clearedCount,
        warning: (!persisted && pgPoolBuild) ? 'PAALALA: Hindi na-save sa Neon build DB ang pag-clear na ito — posibleng bumalik ang lumang history kapag nag-restart ang server.' : undefined
    });
});
app.post('/relay/admin/api/download-codes/generate', requireAdminKey, async (req, res) => {
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
    const persisted = await saveDownloadCodes(downloadCodes);
    logActivity(null, 'download_code_generated', { code, label: label || null });
    res.json({
        success: true,
        code,
        downloadUrl: `${req.protocol}://${req.get('host')}/relay/download/${code}`,
        warning: (!persisted && pgPoolBuild) ? 'PAALALA: Hindi na-save sa Neon build DB ang download code na ito — kung ma-restart/ma-redeploy ang server bago ito ma-retry, hindi na gagana ang link na ito.' : undefined
    });
});
app.get('/relay/admin/api/download-codes', requireAdminKey, (req, res) => {
    const list = [...downloadCodes.entries()].map(([code, meta]) => ({ code, ...meta }));
    res.json({ success: true, codes: list });
});
app.post('/relay/admin/api/download-codes/:code/revoke', requireAdminKey, async (req, res) => {
    downloadCodes.delete(req.params.code);
    const persisted = await saveDownloadCodes(downloadCodes);
    res.json({
        success: true,
        warning: (!persisted && pgPoolBuild) ? 'PAALALA: Hindi na-save sa Neon build DB ang pag-revoke na ito — posibleng gumana pa rin ang code kapag nag-restart ang server bago ito ma-retry.' : undefined
    });
});
app.get('/relay/download/:code', async (req, res) => {
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
    await ensureReleasePackageFreshOnDisk();
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
        ensureDeviceLicenseSchema(),
        ensureKvSchema(),
        ensureBuildKvSchema(),
        ensureCloudTokenSchema()
    ]);
    // Ibalik agad sa disk ang release zip (kung meron na naka-save sa Neon build
    // DB) bago pa man tumanggap ng unang download request — para hindi na
    // mag-503 ang unang user na mag-do-download pagkatapos ng redeploy/restart.
    await ensureReleasePackageFreshOnDisk();

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
        branchHistory,
        branchTransfers,
        cloudBackupPlanOverrides,
        featurePricingOverrides,
        upgradeTierPricingOverrides,
        moduleSubscriptionOverrides,
        SUGGESTED_DISCOUNT_PERCENT,
        neonPricingOverrides,
        neonConfiguredPlans,
        clientMaintenanceFeeConfig,
        clientMaintenanceFeePaidUntil,
        ACTIVATION_FLAGS,
        cloudBackupActualBills
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
        loadBranchHistory(),
        loadBranchTransfers(),
        loadCloudBackupPlanOverrides(),
        loadFeaturePricingOverrides(),
        loadUpgradeTierPricingOverrides(),
        loadModuleSubscriptionOverrides(),
        loadSuggestedDiscountPercent(),
        loadNeonPricingOverrides(),
        loadNeonConfiguredPlans(),
        loadClientMaintenanceFeeConfig(),
        loadClientMaintenanceFeePaidUntil(),
        loadActivationFlags(),
        loadCloudBackupActualBills()
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
    recomputeNeonPricing();
    if (Object.keys(neonPricingOverrides).length > 0) {
        console.log(`🗄️  Na-load ang custom na Neon pricing override para sa: ${Object.keys(neonPricingOverrides).join(', ')}.`);
    }
    pruneCloudSyncActivity();
    if (!neonConfiguredPlans || typeof neonConfiguredPlans !== 'object') neonConfiguredPlans = { cloudBackup: 'free', devices: 'free', build: 'free' };
    // AYOS/BAGO: paalala sa bawat pag-restart kung "free" pa rin ang
    // configured plan habang may naka-configure nang totoong Neon API key
    // — hindi ito palaging mali (baka totoo ngang Free pa kayo), pero
    // sulit na paalala ito kung sakaling default lang ito dahil na-reset
    // ang Redis (tingnan ang saveNeonConfiguredPlans() sa itaas).
    if (NEON_API_CONFIGURED) {
        const stillFreeKeys = Object.entries(neonConfiguredPlans).filter(([, v]) => v === 'free').map(([k]) => k);
        if (stillFreeKeys.length > 0) {
            console.log(`ℹ️  Paalala: "free" pa rin ang configured Neon plan para sa: ${stillFreeKeys.join(', ')}. Kung may bayad ka na talaga dito, i-set ito nang tama sa Database Health admin panel (ligtas naman ang aktwal na sinisingil sa customer dahil may fallback-to-launch protection, pero para tumpak ang "REAL na usage" report).`);
        }
    }
    if (!cloudBackupActualBills || typeof cloudBackupActualBills !== 'object') cloudBackupActualBills = {};
    if (!clientMaintenanceFeeConfig || typeof clientMaintenanceFeeConfig !== 'object') clientMaintenanceFeeConfig = JSON.parse(JSON.stringify(CLIENT_MAINTENANCE_FEE_DEFAULT));
    if (!clientMaintenanceFeeConfig.perClientOverridePHP || typeof clientMaintenanceFeeConfig.perClientOverridePHP !== 'object') clientMaintenanceFeeConfig.perClientOverridePHP = {};
    console.log(
        pgPoolDevices
            ? `✅ Na-load mula sa Postgres (Neon): ${allowedDevices.size} allowed device(s), ${Object.keys(issuedUnlocks).length} device(s) may naka-unlock.`
            : `ℹ️  Na-load mula sa lokal na JSON files: ${allowedDevices.size} allowed device(s), ${Object.keys(issuedUnlocks).length} device(s) may naka-unlock.`
    );
}
bootstrapStores()
    .then(async () => {
        await loadReceiptCreditPricing();
        const server = app.listen(PORT, () => {
            console.log(`OmniPOS Unlock Relay running sa port ${PORT}`);
        });
        server.requestTimeout = 60 * 60 * 1000;
    })
    .catch((err) => {
        console.error('❌ Hindi ma-bootstrap ang persistent storage — hindi tumakbo ang server:', err);
        process.exit(1);
    });
