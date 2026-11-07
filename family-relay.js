'use strict';
/**
 * Family Remittance & Expense Tracker module ng RELAY. Mount: tingnan ang apply-family-patch.js.
 * Lahat sa ilalim ng /relay/family. Gumagamit ng umiiral na Postgres pool ng RELAY (durable, hindi memory).
 *
 * Auth: "Authorization: Bearer <token>". Token = 32 random bytes; ang SHA-256 hash lang ang nasa DB.
 * Roles (galing sa SERVER, hindi sa app):  admin = view-only sa pera;  client = nagsusulat ng transaksyon.
 * Tenant isolation: ang workspace ay laging galing sa token ng device, HINDI sa body/query ng app.
 *
 * Endpoints:
 *   GET  /health                         (public) DB check
 *   POST /admin/register   [x-relay-admin-key]  gumawa ng family + admin account (gamit ang owner key mo, minsan lang, via curl)
 *   POST /admin/login                    email+password -> token (role admin)
 *   POST /binding-codes    [admin]       one-time code (hash lang ang naka-save, 10 min default)
 *   POST /join                           code -> client token (single-use, atomic)
 *   GET  /devices          [admin]       listahan ng client devices
 *   POST /devices/:id/revoke [admin]
 *   GET  /relay/admin/api/family  [x-relay-admin-key] owner view: families + devices (online/recent/idle); page: /relay/admin/family.html
 *   POST /sign-out         [any]
 *   POST /sync/push        [client]      idempotent na batch ng ops
 *   GET  /sync/pull?since= [admin|client] mga record ng sariling workspace (seq > since)
 * Env (opsyonal): FAMILY_CODE_TTL_MIN=10, FAMILY_ADMIN_TOKEN_DAYS=30, FAMILY_CLIENT_TOKEN_DAYS=365.
 * Walang password/token/code/halaga na isinusulat sa log.
 */
const crypto = require('crypto');

const ALPHA = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const CODE_TTL_MIN = Math.max(1, parseInt(process.env.FAMILY_CODE_TTL_MIN || '10', 10) || 10);
const ADMIN_TOKEN_DAYS = Math.max(1, parseInt(process.env.FAMILY_ADMIN_TOKEN_DAYS || '30', 10) || 30);
const CLIENT_TOKEN_DAYS = Math.max(1, parseInt(process.env.FAMILY_CLIENT_TOKEN_DAYS || '365', 10) || 365);
const MAX_OPS = 100;
const MAX_MINOR = 99999999999;
const ENTITIES = {
    remittance: ['id', 'workspaceId', 'senderId', 'receiverId', 'currency', 'sentMinor', 'receivedMinor', 'sentAt', 'receivedAt', 'confirmedAt', 'reference', 'notes', 'status', 'createdAt', 'updatedAt', 'voided'],
    expense: ['id', 'workspaceId', 'category', 'description', 'amountMinor', 'currency', 'occurredAt', 'paymentMethod', 'receiptRef', 'notes', 'createdBy', 'createdAt', 'updatedAt', 'voided'],
    savings: ['id', 'workspaceId', 'type', 'amountMinor', 'currency', 'occurredAt', 'goalId', 'notes', 'createdBy', 'createdAt', 'updatedAt', 'voided'],
    goal: ['id', 'workspaceId', 'name', 'targetMinor', 'currency', 'createdAt', 'updatedAt', 'voided'],
    category: ['name', 'createdAt']
};
const MONEY_FIELDS = ['sentMinor', 'receivedMinor', 'amountMinor', 'targetMinor'];

const sha = (s) => crypto.createHash('sha256').update(String(s)).digest('hex');
const clamp = (v, n) => String(v === undefined || v === null ? '' : v).trim().slice(0, n);
const rnd = (len) => { const b = crypto.randomBytes(len); let s = ''; for (let i = 0; i < len; i++) s += ALPHA[b[i] % ALPHA.length]; return s; };
const normCode = (s) => String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
const newToken = () => crypto.randomBytes(32).toString('base64url');
const uid = (p) => p + '_' + crypto.randomBytes(9).toString('hex');
const scrypt = (pw, salt) => new Promise((ok, no) => crypto.scrypt(pw, salt, 64, { N: 16384 }, (e, k) => (e ? no(e) : ok(k))));

module.exports = function mountFamily(d) {
    const { app } = d;
    const q = (text, params) => d.queryWithRetry(d.pool(), text, params);

    // ------------------------------------------------------------------ schema
    let schemaPromise = null;
    function ensureSchema() {
        if (!schemaPromise) {
            schemaPromise = (async () => {
                const p = d.pool();
                if (!p) throw new Error('Postgres is not configured.');
                await p.query(`CREATE TABLE IF NOT EXISTS family_groups (workspace_id TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now())`);
                await p.query(`CREATE TABLE IF NOT EXISTS family_users (user_id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL REFERENCES family_groups(workspace_id),
                    email TEXT NOT NULL, pw_salt TEXT NOT NULL, pw_hash TEXT NOT NULL, display_name TEXT NOT NULL DEFAULT '',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now())`);
                await p.query(`CREATE UNIQUE INDEX IF NOT EXISTS uq_family_user_email ON family_users (lower(email))`);
                await p.query(`CREATE TABLE IF NOT EXISTS family_devices (device_id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, role TEXT NOT NULL,
                    user_id TEXT, name TEXT NOT NULL DEFAULT '', token_hash TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    expires_at TIMESTAMPTZ NOT NULL, revoked_at TIMESTAMPTZ, last_seen TIMESTAMPTZ)`);
                await p.query(`CREATE UNIQUE INDEX IF NOT EXISTS uq_family_device_token ON family_devices (token_hash)`);
                await p.query(`CREATE INDEX IF NOT EXISTS idx_family_devices_ws ON family_devices (workspace_id, role)`);
                await p.query(`CREATE TABLE IF NOT EXISTS family_binding_codes (code_hash TEXT PRIMARY KEY, workspace_id TEXT NOT NULL,
                    created_by TEXT, expires_at TIMESTAMPTZ NOT NULL, used_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now())`);
                await p.query(`CREATE SEQUENCE IF NOT EXISTS family_record_seq`);
                await p.query(`CREATE TABLE IF NOT EXISTS family_records (workspace_id TEXT NOT NULL, entity TEXT NOT NULL, entity_id TEXT NOT NULL,
                    payload JSONB NOT NULL, voided BOOLEAN NOT NULL DEFAULT FALSE, client_updated_at BIGINT NOT NULL DEFAULT 0,
                    server_version INT NOT NULL DEFAULT 1, seq BIGINT NOT NULL, device_id TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (workspace_id, entity, entity_id))`);
                await p.query(`CREATE INDEX IF NOT EXISTS idx_family_records_seq ON family_records (workspace_id, seq)`);
                await p.query(`CREATE TABLE IF NOT EXISTS family_ops (workspace_id TEXT NOT NULL, op_id TEXT NOT NULL, result TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (workspace_id, op_id))`);
                await p.query(`CREATE TABLE IF NOT EXISTS family_audit (id BIGSERIAL PRIMARY KEY, workspace_id TEXT, actor TEXT, action TEXT NOT NULL,
                    detail TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now())`);
            })().catch((e) => { schemaPromise = null; throw e; });
        }
        return schemaPromise;
    }
    const wrap = (fn) => async (req, res, next) => {
        try { await ensureSchema(); await fn(req, res, next); }
        catch (err) {
            console.error('Family module error:', err && err.message); // message lang; walang body/token
            if (!res.headersSent) res.status(503).json({ success: false, code: 'UNAVAILABLE', message: 'Family service temporarily unavailable.' });
        }
    };
    const audit = (ws, actor, action, detail) => q('INSERT INTO family_audit (workspace_id, actor, action, detail) VALUES ($1,$2,$3,$4)', [ws, actor, action, clamp(detail, 200)]).catch(() => {});
    const fail = (res, status, code, message) => {
        if (status === 401) res.set('WWW-Authenticate', 'Bearer realm="family"'); // Android HttpURLConnection needs this on 401
        return res.status(status).json({ success: false, code, message });
    };

    // -------------------------------------------------------------------- auth
    const authFails = new Map(); // ip -> {count, first}
    function tooManyFails(ip) { const f = authFails.get(ip); return !!(f && f.count >= 15 && Date.now() - f.first < 10 * 60000); }
    function noteFail(ip) { const f = authFails.get(ip); if (!f || Date.now() - f.first > 10 * 60000) authFails.set(ip, { count: 1, first: Date.now() }); else f.count++; }

    function bearer(req) { const m = /^Bearer ([A-Za-z0-9_-]{20,80})$/.exec(String(req.headers.authorization || '')); return m ? m[1] : null; }

    /** need: 'admin' | 'client' | null (any valid device). Sets req.fam = {deviceId, workspaceId, role, userId}. */
    const auth = (need) => async (req, res, next) => {
        try {
            await ensureSchema();
            if (tooManyFails(req.ip)) return fail(res, 429, 'RATE_LIMITED', 'Too many attempts.');
            const t = bearer(req);
            const r = t ? await q('SELECT device_id, workspace_id, role, user_id, expires_at, revoked_at FROM family_devices WHERE token_hash = $1', [sha(t)]) : { rows: [] };
            const row = r.rows[0];
            if (!row || row.revoked_at || new Date(row.expires_at).getTime() < Date.now()) { noteFail(req.ip); return fail(res, 401, 'UNAUTHORIZED', 'Session is not valid.'); }
            if (need && row.role !== need) return fail(res, 403, 'FORBIDDEN', 'Not allowed for this role.'); // server-side role check
            req.fam = { deviceId: row.device_id, workspaceId: row.workspace_id, role: row.role, userId: row.user_id };
            q('UPDATE family_devices SET last_seen = now() WHERE device_id = $1', [row.device_id]).catch(() => {});
            next();
        } catch (err) {
            console.error('Family auth error:', err && err.message);
            fail(res, 503, 'UNAVAILABLE', 'Family service temporarily unavailable.');
        }
    };

    async function issueDevice(workspaceId, role, userId, name, days) {
        const token = newToken(), deviceId = uid('dev');
        await q('INSERT INTO family_devices (device_id, workspace_id, role, user_id, name, token_hash, expires_at) VALUES ($1,$2,$3,$4,$5,$6, now() + ($7 || \' days\')::interval)',
            [deviceId, workspaceId, role, userId, clamp(name, 60) || (role === 'admin' ? 'Admin phone' : 'Family phone'), sha(token), String(days)]);
        return { token, deviceId };
    }

    // ------------------------------------------------------------------ health
    app.get('/relay/family/health', d.rateLimit('family-health', 120, 60 * 1000), wrap(async (req, res) => {
        await q('SELECT 1');
        res.json({ success: true, status: 'ok', serverTime: Date.now() });
    }));

    // ------------------------------------------- admin provisioning (owner only)
    app.post('/relay/family/admin/register', d.requireAdminKey, d.rateLimit('family-register', 10, 60 * 60 * 1000), wrap(async (req, res) => {
        const email = clamp(req.body && req.body.email, 120).toLowerCase();
        const password = String((req.body && req.body.password) || '');
        const familyName = clamp(req.body && req.body.familyName, 80) || 'My Family';
        const displayName = clamp(req.body && req.body.displayName, 60) || 'Admin';
        if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) return fail(res, 400, 'BAD_REQUEST', 'Invalid email.');
        if (password.length < 10 || password.length > 200) return fail(res, 400, 'BAD_REQUEST', 'Password must be 10-200 characters.');
        const salt = crypto.randomBytes(16).toString('hex');
        const hash = (await scrypt(password, salt)).toString('hex');
        const ws = uid('fam'), userId = uid('usr');
        const dup = await q('SELECT 1 FROM family_users WHERE lower(email) = $1', [email]);
        if (dup.rows.length) return fail(res, 409, 'EXISTS', 'Email already registered.');
        await q('INSERT INTO family_groups (workspace_id, name) VALUES ($1,$2)', [ws, familyName]);
        await q('INSERT INTO family_users (user_id, workspace_id, email, pw_salt, pw_hash, display_name) VALUES ($1,$2,$3,$4,$5,$6)', [userId, ws, email, salt, hash, displayName]);
        audit(ws, userId, 'admin_registered', '');
        res.json({ success: true, workspaceId: ws });
    }));

    app.post('/relay/family/admin/login', d.rateLimit('family-login', 20, 10 * 60 * 1000), wrap(async (req, res) => {
        if (tooManyFails(req.ip)) return fail(res, 429, 'RATE_LIMITED', 'Too many attempts.');
        const email = clamp(req.body && req.body.email, 120).toLowerCase();
        const password = String((req.body && req.body.password) || '').slice(0, 200);
        const r = await q('SELECT user_id, workspace_id, pw_salt, pw_hash, display_name FROM family_users WHERE lower(email) = $1', [email]);
        const u = r.rows[0];
        // Always run scrypt (even for unknown email) so timing does not reveal which emails exist.
        const calc = (await scrypt(password, u ? u.pw_salt : 'x'.repeat(32))).toString('hex');
        if (!u || !d.safeCompare(calc, u.pw_hash)) { noteFail(req.ip); return fail(res, 401, 'BAD_CREDENTIALS', 'Wrong email or password.'); }
        const dev = await issueDevice(u.workspace_id, 'admin', u.user_id, req.body && req.body.deviceName, ADMIN_TOKEN_DAYS);
        audit(u.workspace_id, u.user_id, 'admin_login', '');
        res.json({ success: true, role: 'admin', token: dev.token, workspaceId: u.workspace_id, deviceId: dev.deviceId, displayName: u.display_name });
    }));

    // ----------------------------------------------------------- pairing codes
    app.post('/relay/family/binding-codes', auth('admin'), d.rateLimit('family-code', 20, 60 * 60 * 1000), wrap(async (req, res) => {
        const code = 'FTR-' + rnd(4) + '-' + rnd(4);
        const expires = Date.now() + CODE_TTL_MIN * 60000;
        await q('INSERT INTO family_binding_codes (code_hash, workspace_id, created_by, expires_at) VALUES ($1,$2,$3, to_timestamp($4::double precision / 1000.0))',
            [sha(normCode(code)), req.fam.workspaceId, req.fam.deviceId, expires]);
        audit(req.fam.workspaceId, req.fam.deviceId, 'code_created', '');
        res.json({ success: true, code, expiresAt: expires });
    }));

    app.post('/relay/family/join', d.rateLimit('family-join', 15, 10 * 60 * 1000), wrap(async (req, res) => {
        if (tooManyFails(req.ip)) return fail(res, 429, 'RATE_LIMITED', 'Too many attempts.');
        const code = normCode(req.body && req.body.code);
        if (code.length !== 11) { noteFail(req.ip); return fail(res, 400, 'INVALID_CODE', 'Invalid code.'); } // FTR + 8
        // Atomic single use: only one request can flip used_at from NULL.
        const r = await q('UPDATE family_binding_codes SET used_at = now() WHERE code_hash = $1 AND used_at IS NULL AND expires_at > now() RETURNING workspace_id', [sha(code)]);
        if (!r.rows[0]) {
            noteFail(req.ip);
            const e = await q('SELECT used_at, expires_at FROM family_binding_codes WHERE code_hash = $1', [sha(code)]);
            const row = e.rows[0];
            if (row && !row.used_at && new Date(row.expires_at).getTime() <= Date.now()) return fail(res, 410, 'EXPIRED_CODE', 'Code expired.');
            return fail(res, 400, 'INVALID_CODE', 'Invalid or already used code.');
        }
        const ws = r.rows[0].workspace_id;
        const dev = await issueDevice(ws, 'client', null, req.body && req.body.deviceName, CLIENT_TOKEN_DAYS);
        audit(ws, dev.deviceId, 'device_joined', '');
        res.json({ success: true, role: 'client', token: dev.token, workspaceId: ws, deviceId: dev.deviceId, displayName: clamp(req.body && req.body.deviceName, 60) || 'Family phone' });
    }));

    // ------------------------------------------------------------ device admin
    app.get('/relay/family/devices', auth('admin'), wrap(async (req, res) => {
        const r = await q("SELECT device_id, name, created_at, revoked_at FROM family_devices WHERE workspace_id = $1 AND role = 'client' ORDER BY created_at DESC LIMIT 100", [req.fam.workspaceId]);
        res.json({ success: true, devices: r.rows.map((x) => ({ id: x.device_id, name: x.name, pairedAt: new Date(x.created_at).getTime(), revoked: !!x.revoked_at })) });
    }));

    app.post('/relay/family/devices/:id/revoke', auth('admin'), d.rateLimit('family-revoke', 30, 60 * 60 * 1000), wrap(async (req, res) => {
        const r = await q("UPDATE family_devices SET revoked_at = now() WHERE device_id = $1 AND workspace_id = $2 AND role = 'client' AND revoked_at IS NULL RETURNING device_id", [clamp(req.params.id, 60), req.fam.workspaceId]);
        if (!r.rows[0]) return fail(res, 404, 'NOT_FOUND', 'Device not found.');
        audit(req.fam.workspaceId, req.fam.deviceId, 'device_revoked', r.rows[0].device_id);
        res.json({ success: true });
    }));

    app.post('/relay/family/sign-out', auth(null), wrap(async (req, res) => {
        await q('UPDATE family_devices SET revoked_at = now() WHERE device_id = $1', [req.fam.deviceId]);
        res.json({ success: true });
    }));

    // -------------------------------------------------------------------- sync
    function cleanRecord(entity, p) {
        if (!p || typeof p !== 'object') return null;
        const out = {};
        for (const k of ENTITIES[entity]) if (p[k] !== undefined) out[k] = p[k];
        for (const k of MONEY_FIELDS) {
            if (out[k] === undefined || out[k] === null) continue;
            if (!Number.isInteger(out[k]) || out[k] < 0 || out[k] > MAX_MINOR) return null;
            if (k !== 'sentMinor' && out[k] === 0) return null; // sent may be 0 (unknown); other amounts must be > 0
        }
        if (entity === 'category') { out.name = clamp(out.name, 40); return out.name ? out : null; }
        if (out.currency !== undefined && !/^[A-Z]{3}$/.test(out.currency)) return null;
        if (!Number.isFinite(out.updatedAt)) return null;
        for (const k of ['description', 'notes', 'reference', 'category', 'name']) if (out[k] !== undefined) out[k] = clamp(out[k], k === 'notes' ? 500 : 120);
        return out;
    }

    app.post('/relay/family/sync/push', auth('client'), d.rateLimit('family-push', 120, 10 * 60 * 1000), wrap(async (req, res) => {
        const ops = req.body && Array.isArray(req.body.ops) ? req.body.ops : null;
        if (!ops || ops.length === 0 || ops.length > MAX_OPS) return fail(res, 400, 'BAD_REQUEST', 'ops must be 1-' + MAX_OPS + ' items.');
        const ws = req.fam.workspaceId; // NEVER taken from the body
        const results = [];
        for (const op of ops) {
            const opId = clamp(op && op.opId, 80), entity = clamp(op && op.entity, 20), entityId = clamp(op && op.entityId, 120);
            if (!opId || !ENTITIES[entity] || !entityId) { results.push({ opId, status: 'rejected', reason: 'bad_op' }); continue; }
            const rec = cleanRecord(entity, op.payload);
            if (!rec) { results.push({ opId, status: 'rejected', reason: 'invalid_payload' }); continue; }
            const prior = await q('SELECT result FROM family_ops WHERE workspace_id = $1 AND op_id = $2', [ws, opId]);
            if (prior.rows[0]) { results.push({ opId, status: 'duplicate' }); continue; } // idempotency key
            rec.workspaceId = ws;
            const cu = Number.isFinite(rec.updatedAt) ? rec.updatedAt : Date.now();
            const up = await q(`INSERT INTO family_records (workspace_id, entity, entity_id, payload, voided, client_updated_at, seq, device_id)
                VALUES ($1,$2,$3,$4::jsonb,$5,$6,nextval('family_record_seq'),$7)
                ON CONFLICT (workspace_id, entity, entity_id) DO UPDATE SET payload = EXCLUDED.payload, voided = EXCLUDED.voided,
                    client_updated_at = EXCLUDED.client_updated_at, server_version = family_records.server_version + 1,
                    seq = EXCLUDED.seq, device_id = EXCLUDED.device_id, updated_at = now()
                WHERE family_records.client_updated_at <= EXCLUDED.client_updated_at
                RETURNING server_version`, [ws, entity, entityId, JSON.stringify(rec), !!rec.voided, cu, req.fam.deviceId]);
            const status = up.rows[0] ? 'ok' : 'conflict_ignored'; // older edit than what the server already has
            await q('INSERT INTO family_ops (workspace_id, op_id, result) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING', [ws, opId, status]);
            results.push({ opId, status, serverVersion: up.rows[0] ? up.rows[0].server_version : undefined });
        }
        audit(ws, req.fam.deviceId, 'push', 'ops=' + ops.length); // count only, never amounts
        res.json({ success: true, results, serverTime: Date.now() });
    }));

    app.get('/relay/family/sync/pull', auth(null), d.rateLimit('family-pull', 240, 10 * 60 * 1000), wrap(async (req, res) => {
        const since = Math.max(0, parseInt(req.query.since, 10) || 0);
        const r = await q('SELECT entity, entity_id, payload, seq, server_version FROM family_records WHERE workspace_id = $1 AND seq > $2 ORDER BY seq ASC LIMIT 500', [req.fam.workspaceId, since]);
        const next = r.rows.length ? Number(r.rows[r.rows.length - 1].seq) : since;
        res.json({ success: true, records: r.rows.map((x) => ({ entity: x.entity, entityId: x.entity_id, payload: x.payload, serverVersion: x.server_version, seq: Number(x.seq) })),
            nextSince: next, more: r.rows.length === 500, serverTime: Date.now() });
    }));

    // ------------------------------------------------ OWNER VIEW (admin key): who is active
    // Page: /relay/admin/family.html . Shows families and their devices, NEVER amounts or transaction contents.
    const ONLINE_MS = 5 * 60 * 1000;   // seen within 5 min  = online now (the Admin app polls every 60 s while open)
    const RECENT_MS = 24 * 3600 * 1000;
    app.get('/relay/admin/api/family', d.requireAdminKey, d.rateLimit('family-owner-list', 60, 10 * 60 * 1000), wrap(async (req, res) => {
        const g = await q(`SELECT g.workspace_id, g.name, g.created_at,
                (SELECT email FROM family_users u WHERE u.workspace_id = g.workspace_id ORDER BY created_at LIMIT 1) AS admin_email,
                (SELECT count(*) FROM family_records r WHERE r.workspace_id = g.workspace_id) AS records,
                (SELECT max(updated_at) FROM family_records r WHERE r.workspace_id = g.workspace_id) AS last_record,
                (SELECT count(*) FROM family_binding_codes c WHERE c.workspace_id = g.workspace_id AND c.used_at IS NULL AND c.expires_at > now()) AS open_codes
            FROM family_groups g ORDER BY g.created_at DESC LIMIT 200`);
        const ids = g.rows.map((x) => x.workspace_id);
        const dv = ids.length ? await q(`SELECT device_id, workspace_id, role, name, created_at, last_seen, revoked_at, expires_at
            FROM family_devices WHERE workspace_id = ANY($1) ORDER BY last_seen DESC NULLS LAST, created_at DESC LIMIT 3000`, [ids]) : { rows: [] };
        const now = Date.now();
        const byWs = new Map();
        for (const x of dv.rows) {
            const seen = x.last_seen ? new Date(x.last_seen).getTime() : 0;
            const revoked = !!x.revoked_at, expired = new Date(x.expires_at).getTime() < now;
            const state = revoked ? 'revoked' : expired ? 'expired' : (seen && now - seen < ONLINE_MS) ? 'online' : (seen && now - seen < RECENT_MS) ? 'recent' : 'idle';
            const list = byWs.get(x.workspace_id) || [];
            list.push({ id: x.device_id, role: x.role, name: x.name, pairedAt: new Date(x.created_at).getTime(), lastSeen: seen || null, state });
            byWs.set(x.workspace_id, list);
        }
        res.json({ success: true, serverTime: now, families: g.rows.map((x) => ({
            workspaceId: x.workspace_id, name: x.name, createdAt: new Date(x.created_at).getTime(), adminEmail: x.admin_email,
            records: Number(x.records), lastRecordAt: x.last_record ? new Date(x.last_record).getTime() : null, openCodes: Number(x.open_codes),
            devices: byWs.get(x.workspace_id) || [] })) });
    }));

    app.post('/relay/admin/api/family/devices/:id/revoke', d.requireAdminKey, d.rateLimit('family-owner-revoke', 30, 60 * 60 * 1000), wrap(async (req, res) => {
        const r = await q('UPDATE family_devices SET revoked_at = now() WHERE device_id = $1 AND revoked_at IS NULL RETURNING workspace_id', [clamp(req.params.id, 60)]);
        if (!r.rows[0]) return fail(res, 404, 'NOT_FOUND', 'Device not found.');
        audit(r.rows[0].workspace_id, 'owner', 'device_revoked_by_owner', clamp(req.params.id, 60));
        res.json({ success: true });
    }));
};
