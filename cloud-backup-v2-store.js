'use strict';
// ===================================================================
// CLOUD BACKUP v2 — STORAGE LAYER (zero-copy incremental)
// ===================================================================
// Ang laman (gzip BYTEA) ng bawat segment ay nakatira sa `cloud_backup_blobs`. Ang `cloud_backup_segments` ay mapa lang ng
// (installation, generation, module, part) -> blob_id. Kaya ang hindi nagbagong segment ay NAGTUTURO LANG sa parehong blob ng
// bagong generation — walang kopya ng data, walang dagdag na storage (mahalaga sa Neon, na sinisingil sa storage).
//
// Lifecycle ng blob: ang database trigger na `cloud_backup_blob_gc` ang nagbubura ng blob kapag wala nang segment row na
// tumutukoy dito (kapag binura ang lumang generation, pinalitan ang segment, binura ang installation, atbp.). Kaya hindi na kailangang
// baguhin ang bawat lugar sa server.js na nagbubura ng segments.
//
// Lahat ng function dito ay tumatanggap ng `q(text, params)` — pwedeng pool.query, query ng transaction client, o queryWithRetry.
const crypto = require('crypto');

const DUPLICATE_CODES = new Set(['42710', '42P07', '42701', '23505', '42P16']); // duplicate_object/table/column, race ng dalawang instance sa startup

async function ddl(q, text) {
    try {
        await q(text, []);
    } catch (err) {
        if (err && DUPLICATE_CODES.has(err.code)) return;   // ginawa na ng ibang instance
        throw err;
    }
}

async function ensureSchema(q) {
    await ddl(q, `
        CREATE TABLE IF NOT EXISTS cloud_backup_blobs (
            installation_id TEXT NOT NULL,
            blob_id         TEXT NOT NULL,
            data            BYTEA NOT NULL,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (installation_id, blob_id)
        )`);
    await ddl(q, 'ALTER TABLE cloud_backup_segments ADD COLUMN IF NOT EXISTS client_sha256 TEXT');
    await ddl(q, 'ALTER TABLE cloud_backup_segments ADD COLUMN IF NOT EXISTS blob_id TEXT');
    await ddl(q, 'ALTER TABLE cloud_backup_segments ALTER COLUMN data DROP NOT NULL');
    await ddl(q, `
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'cloud_backup_segments_has_payload') THEN
                ALTER TABLE cloud_backup_segments ADD CONSTRAINT cloud_backup_segments_has_payload CHECK (data IS NOT NULL OR blob_id IS NOT NULL) NOT VALID;
            END IF;
        END $$`);
    await ddl(q, `
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'cloud_backup_segments_blob_fk') THEN
                ALTER TABLE cloud_backup_segments ADD CONSTRAINT cloud_backup_segments_blob_fk
                    FOREIGN KEY (installation_id, blob_id) REFERENCES cloud_backup_blobs (installation_id, blob_id);
            END IF;
        END $$`);
    await ddl(q, 'CREATE INDEX IF NOT EXISTS idx_cloud_backup_segments_client_sha ON cloud_backup_segments (installation_id, generation, module, client_sha256)');
    await ddl(q, 'CREATE INDEX IF NOT EXISTS idx_cloud_backup_segments_blob ON cloud_backup_segments (installation_id, blob_id)');
    await ddl(q, `
        CREATE OR REPLACE FUNCTION cloud_backup_blob_gc() RETURNS trigger LANGUAGE plpgsql AS $fn$
        BEGIN
            IF OLD.blob_id IS NOT NULL THEN
                DELETE FROM cloud_backup_blobs b
                 WHERE b.installation_id = OLD.installation_id AND b.blob_id = OLD.blob_id
                   AND NOT EXISTS (SELECT 1 FROM cloud_backup_segments s WHERE s.installation_id = OLD.installation_id AND s.blob_id = OLD.blob_id);
            END IF;
            RETURN NULL;
        END
        $fn$`);
    await ddl(q, `
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'cloud_backup_segments_blob_gc_del' AND NOT tgisinternal) THEN
                CREATE TRIGGER cloud_backup_segments_blob_gc_del AFTER DELETE ON cloud_backup_segments
                    FOR EACH ROW EXECUTE PROCEDURE cloud_backup_blob_gc();
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'cloud_backup_segments_blob_gc_upd' AND NOT tgisinternal) THEN
                CREATE TRIGGER cloud_backup_segments_blob_gc_upd AFTER UPDATE OF blob_id ON cloud_backup_segments
                    FOR EACH ROW WHEN (OLD.blob_id IS DISTINCT FROM NEW.blob_id) EXECUTE PROCEDURE cloud_backup_blob_gc();
            END IF;
        END $$`);
}

// Dapat tumakbo sa loob ng transaction (BEGIN/COMMIT): ang blob at ang segment row ay magkasamang nasusulat o wala.
async function saveSegment(q, p) {
    const blobId = crypto.randomUUID();
    await q('INSERT INTO cloud_backup_blobs (installation_id, blob_id, data) VALUES ($1, $2, $3)', [p.installationId, blobId, p.storedGz]);
    await q(
        `INSERT INTO cloud_backup_segments (installation_id, generation, module, part, kind, record_count, raw_bytes, data, sha256, client_sha256, blob_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8, $9, $10)
         ON CONFLICT (installation_id, generation, module, part) DO UPDATE SET
            kind = excluded.kind, record_count = excluded.record_count, raw_bytes = excluded.raw_bytes, data = NULL,
            sha256 = excluded.sha256, client_sha256 = excluded.client_sha256, blob_id = excluded.blob_id`,
        [p.installationId, p.generation, p.moduleName, p.part, p.kind, p.recordCount, p.rawBytes, p.storedSha, p.clientSha, blobId]
    );
    return blobId;
}

// ZERO-COPY: gagawa lang ng bagong segment row na tumuturo sa parehong blob ng segment sa baselineGeneration. Nagbabalik ng null kung
// walang tugmang (module, client_sha256) sa baseline (hal. nabura na) — dapat i-upload nang normal ng client.
async function reuseSegment(q, p) {
    const r = await q(
        `INSERT INTO cloud_backup_segments (installation_id, generation, module, part, kind, record_count, raw_bytes, data, sha256, client_sha256, blob_id)
         SELECT installation_id, $2::text, module, $4::integer, kind, record_count, raw_bytes, NULL::bytea, sha256, client_sha256, blob_id
           FROM cloud_backup_segments
          WHERE installation_id = $1 AND generation = $5 AND module = $3 AND client_sha256 = $6 AND blob_id IS NOT NULL
          ORDER BY part
          LIMIT 1
         ON CONFLICT (installation_id, generation, module, part) DO UPDATE SET
            kind = excluded.kind, record_count = excluded.record_count, raw_bytes = excluded.raw_bytes, data = NULL,
            sha256 = excluded.sha256, client_sha256 = excluded.client_sha256, blob_id = excluded.blob_id
         RETURNING kind, record_count, raw_bytes`,
        [p.installationId, p.generation, p.moduleName, p.part, p.baselineGeneration, p.clientSha]
    );
    const row = r.rows && r.rows[0];
    if (!row) return null;
    return { kind: row.kind, recordCount: Number(row.record_count) || 0, rawBytes: Number(row.raw_bytes) || 0 };
}

// Binabasa ang segment para sa restore: luma/inline (data) o bago (blob).
async function readSegment(q, p) {
    const r = await q(
        `SELECT COALESCE(s.data, b.data) AS data, s.sha256 AS sha256
           FROM cloud_backup_segments s
           LEFT JOIN cloud_backup_blobs b ON b.installation_id = s.installation_id AND b.blob_id = s.blob_id
          WHERE s.installation_id = $1 AND s.generation = $2 AND s.module = $3 AND s.part = $4`,
        [p.installationId, p.generation, p.moduleName, p.part]
    );
    const row = r.rows && r.rows[0];
    if (!row || !row.data) return null;
    return { data: row.data, sha256: row.sha256 || null };
}

// Listahan ng (module, client sha) ng AKTIBONG generation na pwedeng gamitin ulit.
async function loadBaseline(q, installationId) {
    const m = await q('SELECT v2_generation FROM cloud_backup_meta WHERE installation_id = $1', [installationId]);
    const generation = (m.rows[0] && m.rows[0].v2_generation) || null;
    if (!generation) return { generation: null, items: [] };
    const r = await q(
        `SELECT DISTINCT module, client_sha256 FROM cloud_backup_segments
          WHERE installation_id = $1 AND generation = $2 AND client_sha256 IS NOT NULL AND blob_id IS NOT NULL`,
        [installationId, generation]
    );
    return { generation, items: r.rows.map((x) => ({ module: x.module, sha256: x.client_sha256 })) };
}

module.exports = { ensureSchema, saveSegment, reuseSegment, readSegment, loadBaseline };
