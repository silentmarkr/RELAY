'use strict';
// ===================================================================
// DATABASE MIGRATION (Neon / any Postgres) — move one RELAY database to a NEW connection URL
// ===================================================================
// RELAY talks to up to three separate Postgres databases:
//   cloud   -> DATABASE_URL                  (Cloud Backup, Omni Tokens, wallets)
//   devices -> RELAY_DEVICES_DATABASE_URL    (devices, licences, AI credits, settings)
//   build   -> RELAY_BUILD_DATABASE_URL      (download codes, build history, release package)
//
// This module gives the admin a "Migrate" button for each of them:
//   1. The NEW database must be EMPTY. Nothing on it is ever dropped or overwritten.
//   2. The old database is only READ (inside a REPEATABLE READ snapshot). It is never modified, except for one tiny
//      "redirect marker" table that remembers where the data moved to.
//   3. Everything (extensions, sequences, tables, data, constraints, indexes, foreign keys, trigger functions,
//      triggers, sequence values) is created on the new database inside ONE transaction. Row counts (and a content
//      hash for small tables) are verified BEFORE the commit, so any mismatch rolls the new database back to empty.
//   4. While the copy runs, all RELAY queries for THAT database are held (not failed) by SwitchablePool, so no write can
//      be lost. When the copy is verified, the pool is switched to the new URL and the held queries continue on it.
//   5. The new URL is persisted twice (local override file + redirect marker in the old database) so a restart/redeploy
//      does not silently fall back to the stale old database while the env var still has the old URL. If it cannot be
//      persisted anywhere, the switch is NOT done (the verified copy stays on the new database, nothing changes).
const crypto = require('crypto');

const REDIRECT_TABLE = 'relay_db_redirect';
// Tables that are never copied (they describe the old location itself).
const SKIPPED_TABLES = new Set([REDIRECT_TABLE]);
const HASH_VERIFY_MAX_BYTES = 32 * 1024 * 1024; // content-hash verification only for tables up to this size
const TARGET_BATCH_BYTES = 4 * 1024 * 1024;     // aim for ~4 MB of data per fetch/insert round trip

// ---------- small helpers ----------
function qi(name) { return '"' + String(name).replace(/"/g, '""') + '"'; }          // quote identifier
function qt(name) { return `${qi('public')}.${qi(name)}`; }                          // quoted public.<table>
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
function urlHash(url) { return crypto.createHash('sha256').update(String(url || '')).digest('hex'); }

// Validate a Postgres connection string. Returns { ok, message, host, database } — never includes the password.
function parsePgUrl(url) {
    const raw = String(url || '').trim();
    if (!raw) return { ok: false, message: 'The connection URL is empty.' };
    let u;
    try { u = new URL(raw); } catch (_e) { return { ok: false, message: 'That is not a valid URL. Expected: postgresql://user:password@host/dbname' }; }
    if (u.protocol !== 'postgres:' && u.protocol !== 'postgresql:') {
        return { ok: false, message: 'The URL must start with postgresql:// (or postgres://).' };
    }
    if (!u.hostname) return { ok: false, message: 'The URL has no host name.' };
    const database = decodeURIComponent((u.pathname || '').replace(/^\//, ''));
    if (!database) return { ok: false, message: 'The URL has no database name (the part after the host, e.g. /neondb).' };
    return { ok: true, host: u.hostname, database, url: raw };
}
// Host + database only, for display and logs.
function maskUrl(url) {
    const p = parsePgUrl(url);
    return p.ok ? `${p.host}/${p.database}` : '(invalid URL)';
}
// Remove secrets (the full URL and the password) from any text that may reach logs or the admin page.
function scrubSecrets(text, urls) {
    let out = String(text == null ? '' : text);
    for (const url of urls || []) {
        if (!url) continue;
        out = out.split(String(url)).join('[url]');
        try {
            const pw = decodeURIComponent(new URL(String(url)).password || '');
            if (pw && pw.length >= 4) out = out.split(pw).join('***');
        } catch (_e) { /* not a URL */ }
    }
    return out;
}

// ===================================================================
// SwitchablePool — drop-in replacement for pg.Pool (query / connect / on) whose target database can be swapped at
// runtime, and which can briefly HOLD new queries (pause) so a migration sees a quiet database.
// ===================================================================
class SwitchablePool {
    constructor({ label, url, createPool, holdUntilReady = false, startupFailsafeMs = 90000 }) {
        this.label = label;
        this._url = url;
        this._envUrl = url;      // the URL given by the env var; never changes (anchor for redirect chains)
        this._createPool = createPool;
        this._pool = createPool(url);
        this._handlers = [];     // [event, fn] pairs, re-attached to the new pool after a switch
        this._active = 0;        // queries / checked-out clients currently running on the pool
        this._gate = null;       // while set, new queries and connects wait here
        this._failsafe = null;
        this.source = 'env';     // where the current URL came from: env | override-file | old-db-marker | migration
        if (holdUntilReady) this._closeGate(startupFailsafeMs);
    }
    get url() { return this._url; }
    get envUrl() { return this._envUrl; }
    get rawPool() { return this._pool; }   // bypasses the gate — only for the migration engine and read-only status checks
    get isPaused() { return !!this._gate; }
    createRawPool(url) { return this._createPool(url); }

    on(event, fn) {
        this._handlers.push([event, fn]);
        this._pool.on(event, fn);
        return this;
    }
    async end() { return this._pool.end(); }

    async query(...args) {
        await this._waitGate();
        const pool = this._pool;
        this._active++;                   // counted before the first await so a pause always sees this query
        try { return await pool.query(...args); }
        finally { this._active--; }
    }
    async connect() {
        await this._waitGate();
        const pool = this._pool;
        this._active++;                   // counted while still connecting, so a pause cannot miss it
        let client;
        try { client = await pool.connect(); }
        catch (err) { this._active--; throw err; }
        let released = false;
        const origRelease = client.release.bind(client);
        client.release = (arg) => {
            if (released) return undefined;
            released = true;
            this._active--;
            return origRelease(arg);
        };
        return client;
    }

    async _waitGate() {
        while (this._gate) await this._gate.promise;
    }
    _closeGate(failsafeMs) {
        if (this._gate) return false;
        let open;
        const promise = new Promise((resolve) => { open = resolve; });
        this._gate = { promise, open };
        // Failsafe: a bug or crash in the migration must never leave RELAY frozen forever.
        this._failsafe = setTimeout(() => {
            if (this._gate) console.error(`⚠️  Database gate for "${this.label}" was open-forced by the failsafe timer.`);
            this.resume();
        }, Math.max(1000, failsafeMs));
        if (this._failsafe.unref) this._failsafe.unref();
        return true;
    }
    // Hold new queries, then wait for running ones to finish. Throws (and resumes) if they do not finish in time.
    async pause({ drainTimeoutMs = 20000, failsafeMs = 20 * 60 * 1000 } = {}) {
        if (!this._closeGate(failsafeMs)) throw new Error(`The "${this.label}" database is already paused by another operation.`);
        const started = Date.now();
        while (this._active > 0) {
            if (Date.now() - started > drainTimeoutMs) {
                this.resume();
                throw new Error(`The "${this.label}" database is busy (${this._active} running operation(s) did not finish within ${Math.round(drainTimeoutMs / 1000)}s). Nothing was changed — try again in a moment.`);
            }
            await sleep(100);
        }
    }
    resume() {
        if (this._failsafe) { clearTimeout(this._failsafe); this._failsafe = null; }
        if (this._gate) { const g = this._gate; this._gate = null; g.open(); }
    }
    markReady() { this.resume(); }

    // Point this pool at another (already connected and verified) pg.Pool. Held queries continue on it after resume().
    switchTo(newPool, newUrl, source) {
        const oldPool = this._pool;
        for (const [event, fn] of this._handlers) newPool.on(event, fn);
        this._pool = newPool;
        this._url = newUrl;
        this.source = source || 'migration';
        // Close the old pool a little later; a no-op error handler prevents a late socket error from crashing the process.
        try { oldPool.on('error', () => {}); } catch (_e) { /* ignore */ }
        const t = setTimeout(() => { Promise.resolve(oldPool.end()).catch(() => {}); }, 30000);
        if (t.unref) t.unref();
    }
}

// ===================================================================
// Redirect persistence: local file + marker table in the OLD database
// ===================================================================
async function ensureRedirectTable(q) {
    await q(`CREATE TABLE IF NOT EXISTS ${REDIRECT_TABLE} (
        slot       TEXT PRIMARY KEY,
        new_url    TEXT NOT NULL,
        from_host  TEXT,
        to_host    TEXT,
        moved_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    )`, []);
}
async function writeRedirectMarker(pool, slot, newUrl, fromUrl) {
    await ensureRedirectTable((t, p) => pool.query(t, p));
    await pool.query(
        `INSERT INTO ${REDIRECT_TABLE} (slot, new_url, from_host, to_host, moved_at) VALUES ($1,$2,$3,$4, now())
         ON CONFLICT (slot) DO UPDATE SET new_url = EXCLUDED.new_url, from_host = EXCLUDED.from_host, to_host = EXCLUDED.to_host, moved_at = now()`,
        [slot, newUrl, maskUrl(fromUrl), maskUrl(newUrl)]
    );
}
async function readRedirectMarker(pool, slot) {
    try {
        const r = await pool.query(`SELECT new_url, from_host, to_host, moved_at FROM ${REDIRECT_TABLE} WHERE slot = $1`, [slot]);
        const row = r.rows && r.rows[0];
        return row ? { url: row.new_url, fromHost: row.from_host, toHost: row.to_host, movedAt: row.moved_at } : null;
    } catch (err) {
        if (err && err.code === '42P01') return null; // table does not exist: never migrated
        throw err;
    }
}
async function clearRedirectMarker(pool, slot) {
    try { await pool.query(`DELETE FROM ${REDIRECT_TABLE} WHERE slot = $1`, [slot]); }
    catch (err) { if (!(err && err.code === '42P01')) throw err; }
}

// Small JSON file { slot: { url, fromUrlHash, fromHost, toHost, movedAt } }. File I/O is injected from server.js
// (atomic writer) so this module stays testable on its own.
function createOverrideStore({ filePath, readText, writeText }) {
    function load() {
        try {
            const parsed = JSON.parse(readText(filePath));
            return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
        } catch (_e) { return {}; }
    }
    return {
        get(slot) { const e = load()[slot]; return e && typeof e.url === 'string' ? e : null; },
        set(slot, entry) { const all = load(); all[slot] = entry; writeText(filePath, JSON.stringify(all, null, 2)); },
        remove(slot) { const all = load(); if (all[slot]) { delete all[slot]; writeText(filePath, JSON.stringify(all, null, 2)); } }
    };
}

function withTimeout(promise, ms, what) {
    let t;
    const timeout = new Promise((_r, reject) => { t = setTimeout(() => reject(new Error(`${what} timed out after ${ms}ms`)), ms); });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(t));
}

// Runs once at startup (before the schema checks). If a migration happened earlier and the env var still holds the OLD
// url, switch to the new one — otherwise RELAY would quietly go back to a stale copy of the data. Redirects are followed
// as a chain (A -> B -> C) so a second migration works even if the env var was never updated.
const MAX_REDIRECT_HOPS = 5;
async function resolveStartupRedirects({ pools, store, log = console.log, warn = console.warn }) {
    // pools: [{ slot, pool }] — one entry per DISTINCT SwitchablePool
    for (const { slot, pool } of pools) {
        let finalPool = null;
        try {
            const envUrl = pool.envUrl;
            const visited = new Set([envUrl]);
            let currentRaw = pool.rawPool;      // pool used to read the next marker
            let finalUrl = null;
            let source = null;
            let firstHop = true;
            for (let hop = 0; hop < MAX_REDIRECT_HOPS; hop++) {
                let next = null;
                let hopSource = 'old-db-marker';
                if (firstHop) {
                    const entry = store.get(slot);
                    if (entry) {
                        if (entry.fromUrlHash === urlHash(envUrl)) { next = entry.url; hopSource = 'override-file'; }
                        else {
                            // The env var was changed after the migration (normally to the new URL): the env var wins.
                            try { store.remove(slot); } catch (_e) { /* ignore */ }
                            log(`ℹ️  DB redirect for "${slot}" dropped: the env var no longer holds the URL that was migrated away from.`);
                        }
                    }
                }
                firstHop = false;
                if (!next) {
                    let marker = null;
                    try { marker = await withTimeout(readRedirectMarker(currentRaw, slot), 8000, 'redirect marker check'); }
                    catch (err) { warn(`⚠️  Could not check the redirect marker for "${slot}": ${err.message}`); }
                    if (marker && marker.url) next = marker.url;
                }
                if (!next || visited.has(next)) break;
                const test = pool.createRawPool(next);
                try {
                    await withTimeout(test.query('SELECT 1'), 20000, 'new database connection');
                } catch (err) {
                    try { await test.end(); } catch (_e) { /* ignore */ }
                    warn(`❌ "${slot}" database was migrated to ${maskUrl(next)} but that database is NOT reachable now (${scrubSecrets(err.message, [next])}). RELAY keeps using ${maskUrl(finalUrl || envUrl)}, which may hold OLD data — fix this immediately.`);
                    break;
                }
                if (finalPool) { try { await finalPool.end(); } catch (_e) { /* ignore */ } }   // drop the intermediate hop
                visited.add(next);
                finalPool = test; currentRaw = test; finalUrl = next; source = hopSource;
            }
            if (finalPool) {
                pool.switchTo(finalPool, finalUrl, source);
                finalPool = null;
                log(`🔁 "${slot}" database follows the earlier migration → ${maskUrl(pool.url)} (${source}). Please update the env var to the new URL.`);
            }
        } catch (err) {
            if (finalPool) { try { await finalPool.end(); } catch (_e) { /* ignore */ } }
            warn(`⚠️  Redirect check failed for "${slot}": ${err && err.message}`);
        }
    }
}

// Remove the saved redirect (file entry is removed by the caller) and every marker along the chain that starts at startUrl.
async function clearRedirectChain({ createPool, startUrl, slot }) {
    let url = startUrl;
    const visited = new Set();
    let cleared = 0;
    for (let hop = 0; url && !visited.has(url) && hop < MAX_REDIRECT_HOPS; hop++) {
        visited.add(url);
        const pool = createPool(url);
        pool.on('error', () => {});
        let next = null;
        try {
            const marker = await readRedirectMarker(pool, slot);
            if (marker) { next = marker.url; await clearRedirectMarker(pool, slot); cleared++; }
        } finally { try { await pool.end(); } catch (_e) { /* ignore */ } }
        url = next;
    }
    return cleared;
}

// ===================================================================
// Schema + data inspection
// ===================================================================
async function inspectSchema(q) {
    const tablesRes = await q(`
        SELECT c.oid::text AS oid, c.relname AS name, c.relkind::text AS kind, c.relispartition AS is_partition,
               EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid OR i.inhparent = c.oid) AS has_inheritance,
               pg_total_relation_size(c.oid)::text AS bytes
          FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'public' AND c.relkind IN ('r','p','v','m','f')
           AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e')
         ORDER BY c.relname`, []);
    const typesRes = await q(`
        SELECT t.typname AS name, t.typtype::text AS kind
          FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
         WHERE n.nspname = 'public' AND t.typtype IN ('e','d','r','m')
           AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = t.oid AND d.deptype = 'e')`, []);
    const unsupported = [];
    const tables = [];
    for (const t of tablesRes.rows) {
        if (SKIPPED_TABLES.has(t.name)) continue;
        if (t.kind !== 'r') unsupported.push(`${t.name} (${({ p: 'partitioned table', v: 'view', m: 'materialized view', f: 'foreign table' })[t.kind] || t.kind})`);
        else if (t.is_partition || t.has_inheritance) unsupported.push(`${t.name} (partition/inheritance)`);
        else tables.push({ oid: t.oid, name: t.name, bytes: Number(t.bytes) || 0 });
    }
    for (const t of typesRes.rows) unsupported.push(`${t.name} (custom ${({ e: 'enum', d: 'domain', r: 'range', m: 'multirange' })[t.kind] || 'type'})`);

    for (const t of tables) {
        const cols = await q(`
            SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type, a.attnotnull AS not_null,
                   pg_get_expr(d.adbin, d.adrelid) AS default_expr, a.attidentity::text AS identity,
                   a.attgenerated::text AS generated, (a.atttypid = 'bytea'::regtype) AS is_bytea
              FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
             WHERE a.attrelid = $1::oid AND a.attnum > 0 AND NOT a.attisdropped
             ORDER BY a.attnum`, [t.oid]);
        t.columns = cols.rows.map((c) => ({
            name: c.name, type: c.type, notNull: c.not_null, defaultExpr: c.default_expr,
            identity: c.identity || '', generated: c.generated || '', isBytea: !!c.is_bytea
        }));
    }
    const seqs = await q(`
        SELECT s.sequencename AS name, s.data_type::text AS data_type, s.start_value::text AS start_value, s.min_value::text AS min_value,
               s.max_value::text AS max_value, s.increment_by::text AS increment_by, s.cycle AS cycle, s.cache_size::text AS cache_size,
               s.last_value::text AS last_value
          FROM pg_sequences s WHERE s.schemaname = 'public' ORDER BY s.sequencename`, []);
    const seqDeps = await q(`
        SELECT sq.relname AS seq, d.deptype::text AS deptype, cl.relname AS tbl, a.attname AS col
          FROM pg_depend d
          JOIN pg_class sq ON sq.oid = d.objid AND sq.relkind = 'S'
          JOIN pg_namespace sn ON sn.oid = sq.relnamespace AND sn.nspname = 'public'
          JOIN pg_class cl ON cl.oid = d.refobjid
          JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
         WHERE d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a','i')`, []);
    const identitySeqs = new Set(seqDeps.rows.filter((r) => r.deptype === 'i').map((r) => r.seq));
    const sequences = seqs.rows.map((s) => {
        const owner = seqDeps.rows.find((r) => r.seq === s.name && r.deptype === 'a');
        return {
            name: s.name, dataType: s.data_type, start: s.start_value, min: s.min_value, max: s.max_value,
            increment: s.increment_by, cycle: !!s.cycle, cache: s.cache_size, lastValue: s.last_value,
            isIdentity: identitySeqs.has(s.name), ownerTable: owner ? owner.tbl : null, ownerColumn: owner ? owner.col : null
        };
    });
    const cons = await q(`
        SELECT con.conname AS name, c.relname AS tbl, con.contype::text AS kind, pg_get_constraintdef(con.oid) AS def
          FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'public' AND con.contype IN ('p','u','c','x','f') AND con.conparentid = 0
         ORDER BY CASE con.contype WHEN 'p' THEN 1 WHEN 'u' THEN 2 WHEN 'x' THEN 3 WHEN 'c' THEN 4 ELSE 5 END, c.relname, con.conname`, []);
    const idx = await q(`
        SELECT i.indexname AS name, i.tablename AS tbl, i.indexdef AS def FROM pg_indexes i
         WHERE i.schemaname = 'public'
           AND NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class ic ON ic.oid = con.conindid
                            WHERE ic.relname = i.indexname AND con.connamespace = 'public'::regnamespace)
         ORDER BY i.tablename, i.indexname`, []);
    const funcs = await q(`
        SELECT p.proname AS name, pg_get_functiondef(p.oid) AS def
          FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = 'public' AND p.prokind IN ('f','p')
           AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e')
         ORDER BY p.proname`, []);
    const trigs = await q(`
        SELECT t.tgname AS name, c.relname AS tbl, pg_get_triggerdef(t.oid) AS def
          FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'public' AND NOT t.tgisinternal
         ORDER BY c.relname, t.tgname`, []);
    const exts = await q(`SELECT extname AS name FROM pg_extension WHERE extname <> 'plpgsql' ORDER BY extname`, []);
    const tableNames = new Set(tables.map((t) => t.name));
    return {
        tables, unsupported, sequences,
        constraints: cons.rows.filter((c) => tableNames.has(c.tbl)),
        indexes: idx.rows.filter((i) => tableNames.has(i.tbl)),
        functions: funcs.rows,
        triggers: trigs.rows.filter((t) => tableNames.has(t.tbl)),
        extensions: exts.rows.map((e) => e.name)
    };
}

// Objects that already exist in the target's public schema (so it is not "empty").
async function listTargetExistingObjects(q) {
    const r = await q(`
        SELECT c.relname AS name, c.relkind::text AS kind
          FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'public' AND c.relkind IN ('r','p','v','m','f','S')
           AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype IN ('e','i','a'))
         ORDER BY c.relname`, []);
    const f = await q(`
        SELECT p.proname AS name FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = 'public' AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e')`, []);
    return r.rows.map((x) => x.name).concat(f.rows.map((x) => x.name + '()'));
}

// ===================================================================
// DDL builders
// ===================================================================
function buildCreateSequence(s) {
    return `CREATE SEQUENCE ${qt(s.name)} AS ${s.dataType} INCREMENT BY ${s.increment} MINVALUE ${s.min} MAXVALUE ${s.max} START WITH ${s.start} CACHE ${s.cache}${s.cycle ? ' CYCLE' : ' NO CYCLE'}`;
}
function buildCreateTable(t) {
    const cols = t.columns.map((c) => {
        let def = `${qi(c.name)} ${c.type}`;
        if (c.generated === 's') {
            def += ` GENERATED ALWAYS AS (${c.defaultExpr}) STORED`;
        } else if (c.identity === 'a' || c.identity === 'd') {
            def += ` GENERATED ${c.identity === 'a' ? 'ALWAYS' : 'BY DEFAULT'} AS IDENTITY`;
        } else if (c.defaultExpr != null) {
            def += ` DEFAULT ${c.defaultExpr}`;
        }
        if (c.notNull) def += ' NOT NULL';
        return def;
    });
    return `CREATE TABLE ${qt(t.name)} (\n  ${cols.join(',\n  ')}\n)`;
}
function dataColumns(t) { return t.columns.filter((c) => c.generated !== 's'); }

// ===================================================================
// Preflight (read-only on both sides)
// ===================================================================
async function preflight({ sourcePool, targetUrl, createPool }) {
    const parsed = parsePgUrl(targetUrl);
    if (!parsed.ok) return { ok: false, blocking: [parsed.message] };
    const result = { ok: false, blocking: [], warnings: [], target: { host: parsed.host, database: parsed.database }, source: null };
    let targetPool = null;
    try {
        targetPool = createPool(targetUrl);
        const tq = (text, params) => withTimeout(targetPool.query(text, params), 30000, 'target query');
        const info = await tq('SELECT version() AS version, current_database() AS db, current_user AS usr', []);
        result.target.version = String(info.rows[0].version || '').split(' ').slice(0, 2).join(' ');
        const existing = await listTargetExistingObjects(tq);
        result.target.existingObjects = existing.slice(0, 20);
        if (existing.length) result.blocking.push(`The new database is not empty (it already has: ${existing.slice(0, 8).join(', ')}${existing.length > 8 ? ', …' : ''}). Use a brand-new, empty database — nothing is ever overwritten.`);
        // Can we create objects there?
        const canCreate = await tq(`SELECT has_schema_privilege(current_user, 'public', 'CREATE') AS ok`, []);
        if (!canCreate.rows[0].ok) result.blocking.push('The database user has no permission to create tables in the public schema of the new database.');
    } catch (err) {
        result.blocking.push(`Cannot use the new database: ${scrubSecrets(err.message, [targetUrl])}`);
    } finally {
        if (targetPool) { try { await targetPool.end(); } catch (_e) { /* ignore */ } }
    }
    try {
        const sq = (text, params) => withTimeout(sourcePool.query(text, params), 30000, 'source query');
        const schema = await inspectSchema(sq);
        if (schema.unsupported.length) result.blocking.push(`The current database has objects this tool cannot copy: ${schema.unsupported.join(', ')}.`);
        const tables = [];
        for (const t of schema.tables) {
            let rows = null;
            try { rows = Number((await sq(`SELECT count(*)::text AS n FROM ${qt(t.name)}`, [])).rows[0].n); } catch (_e) { /* leave null */ }
            tables.push({ name: t.name, rows, bytes: t.bytes });
        }
        const totalBytes = tables.reduce((s, t) => s + t.bytes, 0);
        // Rough guess (about 3 MB/s end to end, at least 5 s) of how long RELAY will hold requests for this database.
        result.source = { tables, totalBytes, totalRows: tables.reduce((s, t) => s + (t.rows || 0), 0), estimateSeconds: Math.max(5, Math.ceil(totalBytes / (3 * 1024 * 1024))) };
        if (!tables.length) result.warnings.push('The current database has no tables to copy.');
    } catch (err) {
        result.blocking.push(`Cannot read the current database: ${scrubSecrets(err.message, [targetUrl])}`);
    }
    result.ok = result.blocking.length === 0;
    return result;
}

// ===================================================================
// The migration itself
// ===================================================================
class MigrationCancelled extends Error {}

// job: mutable object updated for the admin page (phase, tables[], rowsDone, ...).
async function runMigration({ slot, wrapper, targetUrl, store, job, maxPauseMs = 15 * 60 * 1000, drainTimeoutMs = 20000, log = console.log, warn = console.warn }) {
    const sourceUrl = wrapper.url;
    const secrets = [targetUrl, sourceUrl];
    const deadline = () => { if (Date.now() > job.deadlineAt) throw new Error(`The copy took longer than the allowed ${Math.round(maxPauseMs / 60000)} minutes, so it was cancelled to keep RELAY responsive. Nothing was changed. Run it again at a quieter time (env DB_MIGRATE_MAX_PAUSE_MS raises the limit).`); };
    const checkpoint = () => { if (job.cancelRequested) throw new MigrationCancelled('Cancelled by the admin.'); deadline(); };
    const setPhase = (phase, message) => { job.phase = phase; job.message = message || ''; log(`🗄️  DB migration [${slot}] ${phase}${message ? ' — ' + message : ''}`); };

    job.deadlineAt = Infinity; // the time limit only starts once RELAY queries are being held (see below)
    let targetPool = null;
    let srcClient = null;
    let dstClient = null;
    let paused = false;
    let committed = false;
    try {
        // ---- 1. connect + preflight (no pause yet) ----
        setPhase('preflight', 'Checking both databases');
        const pf = await preflight({ sourcePool: wrapper.rawPool, targetUrl, createPool: (u) => wrapper.createRawPool(u) });
        if (!pf.ok) throw new Error(pf.blocking.join(' '));
        job.rowsTotal = pf.source.totalRows;
        job.bytesTotal = pf.source.totalBytes;
        job.tables = pf.source.tables.map((t) => ({ name: t.name, rows: t.rows, done: 0, state: 'waiting' }));
        checkpoint();

        targetPool = wrapper.createRawPool(targetUrl);
        targetPool.on('error', () => {});
        // ---- 2. hold RELAY traffic for this database ----
        job.deadlineAt = Date.now() + maxPauseMs;
        setPhase('pausing', 'Holding new queries for a moment');
        await wrapper.pause({ drainTimeoutMs, failsafeMs: maxPauseMs + 60000 });
        paused = true;

        // ---- 3. consistent snapshot of the old database ----
        srcClient = await wrapper.rawPool.connect();
        srcClient.on('error', () => {});
        await srcClient.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
        await srcClient.query(`SET LOCAL TIME ZONE 'UTC'`);
        await srcClient.query(`SET LOCAL DateStyle = 'ISO, MDY'`);
        const sq = (text, params) => srcClient.query(text, params);
        const schema = await inspectSchema(sq);
        if (schema.unsupported.length) throw new Error(`The current database has objects this tool cannot copy: ${schema.unsupported.join(', ')}.`);
        const byName = new Map(schema.tables.map((t) => [t.name, t]));
        // Exact row counts inside the snapshot (the paused database is quiet, the snapshot makes it airtight).
        for (const jt of job.tables) {
            const n = Number((await sq(`SELECT count(*)::text AS n FROM ${qt(jt.name)}`, [])).rows[0].n);
            jt.rows = n;
            const st = byName.get(jt.name);
            if (st) st.rows = n;
        }
        job.rowsTotal = schema.tables.reduce((s, t) => s + (t.rows || 0), 0);

        // ---- 4. build everything on the new database, in ONE transaction ----
        dstClient = await targetPool.connect();
        dstClient.on('error', () => {});
        const dq = (text, params) => dstClient.query(text, params);
        await dq('BEGIN', []);
        await dq(`SET LOCAL statement_timeout = 0`, []);
        await dq(`SET LOCAL check_function_bodies = off`, []);
        await dq(`SET LOCAL TIME ZONE 'UTC'`, []);
        await dq(`SET LOCAL DateStyle = 'ISO, MDY'`, []);

        setPhase('schema', 'Creating tables on the new database');
        const warnings = [];
        for (const ext of schema.extensions) {
            await dq('SAVEPOINT ext', []);
            try { await dq(`CREATE EXTENSION IF NOT EXISTS ${qi(ext)}`, []); await dq('RELEASE SAVEPOINT ext', []); }
            catch (err) { await dq('ROLLBACK TO SAVEPOINT ext', []); warnings.push(`Extension "${ext}" could not be enabled on the new database (${scrubSecrets(err.message, secrets)}).`); }
        }
        for (const s of schema.sequences.filter((x) => !x.isIdentity)) await dq(buildCreateSequence(s), []);
        for (const f of schema.functions) await dq(f.def, []);
        for (const t of schema.tables) await dq(buildCreateTable(t), []);
        for (const s of schema.sequences.filter((x) => !x.isIdentity && x.ownerTable && x.ownerColumn)) {
            await dq(`ALTER SEQUENCE ${qt(s.name)} OWNED BY ${qt(s.ownerTable)}.${qi(s.ownerColumn)}`, []);
        }
        checkpoint();

        // ---- 5. copy the data ----
        setPhase('data', 'Copying rows');
        for (const t of schema.tables) {
            const jt = job.tables.find((x) => x.name === t.name);
            jt.state = 'copying';
            await copyTable({ sq, dq, t, jt, job, checkpoint });
            jt.state = 'copied';
        }

        // ---- 6. constraints, indexes, foreign keys, triggers (after the data: faster, and triggers never see the bulk load) ----
        setPhase('constraints', 'Building keys, indexes and triggers');
        for (const c of schema.constraints.filter((x) => x.kind !== 'f')) await dq(`ALTER TABLE ${qt(c.tbl)} ADD CONSTRAINT ${qi(c.name)} ${c.def}`, []);
        for (const i of schema.indexes) await dq(i.def, []);
        for (const c of schema.constraints.filter((x) => x.kind === 'f')) await dq(`ALTER TABLE ${qt(c.tbl)} ADD CONSTRAINT ${qi(c.name)} ${c.def}`, []);
        for (const tr of schema.triggers) await dq(tr.def, []);
        checkpoint();

        // ---- 7. sequence positions ----
        for (const s of schema.sequences) {
            if (s.lastValue == null) continue;               // never used on the old database
            let targetSeq = s.name;
            if (s.isIdentity) {
                targetSeq = await identitySequenceName(sq, dq, s);
                if (!targetSeq) { warnings.push(`Could not match identity sequence "${s.name}" on the new database.`); continue; }
            }
            await dq(`SELECT setval($1::regclass, $2::bigint, true)`, [`${qi('public')}.${qi(targetSeq)}`, s.lastValue]);
        }

        // ---- 8. verify BEFORE committing ----
        setPhase('verify', 'Comparing both databases');
        for (const t of schema.tables) {
            checkpoint();
            const jt = job.tables.find((x) => x.name === t.name);
            const a = await tableFingerprint(sq, t);
            const b = await tableFingerprint(dq, t);
            if (a.count !== b.count) throw new Error(`Verification failed for "${t.name}": ${a.count} rows on the old database but ${b.count} on the new one. The new database was rolled back to empty.`);
            if (a.extra !== b.extra) throw new Error(`Verification failed for "${t.name}": the data differs between the old and new database. The new database was rolled back to empty.`);
            jt.state = 'verified';
            jt.verifiedBy = a.hashed ? 'rows + content hash' : 'rows + sizes';
        }
        await dq('COMMIT', []);
        committed = true;
        try { await srcClient.query('ROLLBACK'); } catch (_e) { /* read-only snapshot */ }

        // ---- 9. remember the new location, then switch ----
        setPhase('switching', 'Saving the new location and switching RELAY over');
        const persisted = { marker: false, file: false };
        try { await writeRedirectMarker(wrapper.rawPool, slot, targetUrl, sourceUrl); persisted.marker = true; }
        catch (err) { warn(`⚠️  Could not write the redirect marker in the old database: ${scrubSecrets(err.message, secrets)}`); }
        try {
            store.set(slot, { url: targetUrl, fromUrlHash: urlHash(wrapper.envUrl), fromHost: maskUrl(sourceUrl), toHost: maskUrl(targetUrl), movedAt: new Date().toISOString() });
            persisted.file = true;
        } catch (err) { warn(`⚠️  Could not write the local override file: ${scrubSecrets(err.message, secrets)}`); }
        if (!persisted.marker && !persisted.file) {
            throw Object.assign(new Error('The data was copied and verified, but the new location could not be saved anywhere, so RELAY did NOT switch (a restart would otherwise go back to old data). Nothing changed for RELAY. The new database now holds a complete copy: set the env var to the new URL and redeploy, or fix the problem and migrate again to another empty database.'), { keepCopy: true });
        }
        wrapper.switchTo(targetPool, targetUrl, 'migration');
        targetPool = null; // now owned by the wrapper
        job.result = {
            fromHost: maskUrl(sourceUrl), toHost: maskUrl(targetUrl), persisted, warnings,
            tables: job.tables.length, rows: job.rowsTotal
        };
        job.state = 'done';
        setPhase('done', 'Migration finished');
        return job.result;
    } catch (err) {
        if (dstClient && !committed) { try { await dstClient.query('ROLLBACK'); } catch (_e) { /* connection may already be gone */ } }
        if (err instanceof MigrationCancelled) { job.state = 'cancelled'; job.error = null; job.message = 'Cancelled. Nothing was changed.'; }
        else {
            job.state = 'failed';
            job.error = scrubSecrets(err && err.message ? err.message : String(err), secrets);
            warn(`❌ DB migration [${slot}] failed: ${job.error}`);
        }
        throw err;
    } finally {
        if (srcClient) { try { await srcClient.query('ROLLBACK'); } catch (_e) { /* ignore */ } try { srcClient.release(); } catch (_e) { /* ignore */ } }
        if (dstClient) { try { dstClient.release(); } catch (_e) { /* ignore */ } }
        if (targetPool) { try { await targetPool.end(); } catch (_e) { /* ignore */ } }
        if (paused) wrapper.resume();
    }
}

async function identitySequenceName(sq, dq, seq) {
    // Find "<table>.<column>" that owns the identity sequence on the source, then ask the target for its own sequence name.
    const own = await sq(`
        SELECT cl.relname AS tbl, a.attname AS col FROM pg_depend d
          JOIN pg_class sq ON sq.oid = d.objid AND sq.relkind = 'S' AND sq.relname = $1
          JOIN pg_class cl ON cl.oid = d.refobjid JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
         WHERE d.deptype = 'i' LIMIT 1`, [seq.name]);
    if (!own.rows[0]) return null;
    const r = await dq(`SELECT pg_get_serial_sequence($1, $2) AS s`, [`${qi('public')}.${qi(own.rows[0].tbl)}`, own.rows[0].col]);
    const full = r.rows[0] && r.rows[0].s;
    return full ? full.replace(/^"?public"?\./, '').replace(/^"|"$/g, '') : null;
}

async function copyTable({ sq, dq, t, jt, job, checkpoint }) {
    const cols = dataColumns(t);
    if (!cols.length || !jt.rows) return;
    const selectList = cols.map((c) => (c.isBytea ? qi(c.name) : `${qi(c.name)}::text AS ${qi(c.name)}`)).join(', ');
    const avgRow = Math.max(1, Math.ceil((t.bytes || 0) / Math.max(1, jt.rows)));
    const fetchRows = Math.max(1, Math.min(2000, Math.floor(TARGET_BATCH_BYTES / avgRow)));
    const maxRowsPerInsert = Math.max(1, Math.floor(60000 / cols.length));   // Postgres allows 65535 parameters per statement
    const hasIdentity = t.columns.some((c) => c.identity);
    const insertHead = `INSERT INTO ${qt(t.name)} (${cols.map((c) => qi(c.name)).join(', ')})${hasIdentity ? ' OVERRIDING SYSTEM VALUE' : ''} VALUES `;
    const cursor = `mig_${crypto.randomBytes(4).toString('hex')}`;
    await sq(`DECLARE ${cursor} NO SCROLL CURSOR FOR SELECT ${selectList} FROM ${qt(t.name)}`, []);
    try {
        for (;;) {
            checkpoint();
            const page = await sq(`FETCH ${fetchRows} FROM ${cursor}`, []);
            const rows = page.rows;
            if (!rows.length) break;
            for (let i = 0; i < rows.length; i += maxRowsPerInsert) {
                const slice = rows.slice(i, i + maxRowsPerInsert);
                const values = [];
                const groups = slice.map((row) => {
                    const ph = cols.map((c) => { values.push(row[c.name]); return `$${values.length}::${c.type}`; });
                    return `(${ph.join(', ')})`;
                });
                await dq(insertHead + groups.join(', '), values);
            }
            jt.done += rows.length;
            job.rowsDone += rows.length;
        }
    } finally {
        try { await sq(`CLOSE ${cursor}`, []); } catch (_e) { /* ignore */ }
    }
}

// Cheap-but-meaningful comparison: row count always; small tables also get an order-independent content hash,
// big tables get the total size of every bytea column.
async function tableFingerprint(q, t) {
    const n = Number((await q(`SELECT count(*)::text AS n FROM ${qt(t.name)}`, [])).rows[0].n);
    if (t.bytes <= HASH_VERIFY_MAX_BYTES) {
        const r = await q(`SELECT md5(coalesce(string_agg(h, '' ORDER BY h), '')) AS x FROM (SELECT md5(r::text) AS h FROM ${qt(t.name)} r) s`, []);
        return { count: n, extra: r.rows[0].x, hashed: true };
    }
    const byteaCols = t.columns.filter((c) => c.isBytea);
    if (!byteaCols.length) return { count: n, extra: '', hashed: false };
    const sums = byteaCols.map((c) => `coalesce(sum(octet_length(${qi(c.name)})),0)::text`).join(` || ':' || `);
    const r = await q(`SELECT ${sums} AS x FROM ${qt(t.name)}`, []);
    return { count: n, extra: r.rows[0].x, hashed: false };
}

module.exports = {
    SwitchablePool, parsePgUrl, maskUrl, scrubSecrets, urlHash,
    createOverrideStore, resolveStartupRedirects, clearRedirectChain, readRedirectMarker, clearRedirectMarker,
    preflight, runMigration, MigrationCancelled, REDIRECT_TABLE
};
