// migrate-redis-to-postgres.js
//
// Isang beses lang patakbuhin ito para kopyahin ang kasalukuyang device/license
// data mula sa Redis (allowed-devices, device-labels, device-fingerprints,
// clone-splits) papunta sa bagong Postgres (Neon) tables.
//
// PAANO GAMITIN:
//   1. Siguraduhing naka-set ang REDIS_URL (lumang data) AT
//      RELAY_DEVICES_DATABASE_URL (Neon — ang HIWALAY na database ng
//      Device/License data, hindi ang Cloud Backup DATABASE_URL) sa
//      parehong .env o environment kung saan mo ito papatakbuhin.
//      Kung wala pang RELAY_DEVICES_DATABASE_URL na naka-set, babalik ito
//      sa DATABASE_URL (backward-compatible sa mga hindi pa naghihiwalay).
//   2. node migrate-redis-to-postgres.js
//   3. Kapag "Migration complete" na, i-deploy mo na ang bagong server.js.
//      Pwede mo nang tanggalin ang REDIS_URL kung ayaw mo nang gamitin pa
//      ang Redis kahit para sa online-heartbeat (opsyonal na hybrid piece).
//
// Ligtas itong patakbuhin nang paulit-ulit — UPSERT ang lahat ng writes, kaya
// hindi ito magda-duplicate o magsisira kung sakaling ma-interrupt at ulitin.

require('dotenv').config();
const Redis = require('ioredis');
const { Pool } = require('pg');

const REDIS_URL = process.env.REDIS_URL;
const DATABASE_URL = process.env.RELAY_DEVICES_DATABASE_URL || process.env.DATABASE_URL;

if (!REDIS_URL) {
    console.error('❌ Walang REDIS_URL na naka-set. Kailangan ito para mabasa ang lumang data.');
    process.exit(1);
}
if (!DATABASE_URL) {
    console.error('❌ Walang RELAY_DEVICES_DATABASE_URL/DATABASE_URL na naka-set. Kailangan ito para sa Neon Postgres connection string ng Device/License database.');
    process.exit(1);
}

const redis = new Redis(REDIS_URL);
const pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: process.env.PGSSL === 'false' ? false : { rejectUnauthorized: false }
});
const REDIS_KEY_PREFIX = 'omnipos-relay:';

async function ensureSchema() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS relay_devices (
            installation_id TEXT PRIMARY KEY,
            allowed         BOOLEAN NOT NULL DEFAULT true,
            label           TEXT,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    `);
    await pool.query(`
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
    await pool.query(`
        CREATE TABLE IF NOT EXISTS relay_clone_splits (
            split_key           TEXT PRIMARY KEY,
            installation_id     TEXT NOT NULL,
            fingerprint          TEXT NOT NULL,
            new_installation_id TEXT NOT NULL,
            split_at            TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    `);
    console.log('✅ Schema ready sa Neon.');
}

async function migrateAllowedDevicesAndLabels() {
    const rawDevices = await redis.get(REDIS_KEY_PREFIX + 'allowed-devices');
    const rawLabels = await redis.get(REDIS_KEY_PREFIX + 'device-labels');
    const allowedIds = rawDevices ? JSON.parse(rawDevices) : [];
    const labels = rawLabels ? JSON.parse(rawLabels) : {};

    const allIds = new Set([...allowedIds, ...Object.keys(labels)]);
    for (const id of allIds) {
        await pool.query(
            `INSERT INTO relay_devices (installation_id, allowed, label, updated_at)
             VALUES ($1, $2, $3, now())
             ON CONFLICT (installation_id) DO UPDATE SET allowed = $2, label = $3, updated_at = now()`,
            [id, allowedIds.includes(id), labels[id] || null]
        );
    }
    console.log(`✅ Na-migrate ang ${allIds.size} device record(s) (allowed + labels).`);
}

async function migrateFingerprints() {
    const raw = await redis.get(REDIS_KEY_PREFIX + 'device-fingerprints');
    const fingerprints = raw ? JSON.parse(raw) : {};
    const entries = Object.entries(fingerprints);
    for (const [id, rec] of entries) {
        await pool.query(
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
    console.log(`✅ Na-migrate ang ${entries.length} device fingerprint record(s).`);
}

async function migrateCloneSplits() {
    const raw = await redis.get(REDIS_KEY_PREFIX + 'clone-splits');
    const splits = raw ? JSON.parse(raw) : {};
    const entries = Object.entries(splits);
    for (const [key, rec] of entries) {
        const [installationId, fingerprint] = key.split('::');
        await pool.query(
            `INSERT INTO relay_clone_splits (split_key, installation_id, fingerprint, new_installation_id, split_at)
             VALUES ($1, $2, $3, $4, to_timestamp($5::double precision / 1000.0))
             ON CONFLICT (split_key) DO UPDATE SET new_installation_id = $4, split_at = to_timestamp($5::double precision / 1000.0)`,
            [key, installationId, fingerprint, rec.newInstallationId, rec.splitAt || Date.now()]
        );
    }
    console.log(`✅ Na-migrate ang ${entries.length} clone-split record(s).`);
}

(async () => {
    try {
        await ensureSchema();
        await migrateAllowedDevicesAndLabels();
        await migrateFingerprints();
        await migrateCloneSplits();
        console.log('🎉 Migration complete. I-verify muna sa Neon dashboard/psql bago i-deploy ang bagong server.js.');
    } catch (err) {
        console.error('❌ May error habang nagmi-migrate:', err);
        process.exitCode = 1;
    } finally {
        await pool.end();
        redis.disconnect();
    }
})();
