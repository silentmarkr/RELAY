'use strict';
/**
 * Family Remittance & Expense Tracker module ng RELAY. Lahat sa ilalim ng /relay/family. Gumagamit ng umiiral na Postgres pool ng RELAY.
 *
 * ACTIVATION (kagaya ng School): ang app mismo ang gumagawa ng pamilya -> Family ID (FAM-XXXXXXXX) + Recovery Key (16 char).
 * Walang email/password at walang one-time code. Ang ibang phone ay sumasali gamit ang Family ID + Recovery Key.
 * Ang Recovery Key ay SHA-256 hash lang ang naka-save sa server; ipinapakita lang ito nang buo kapag ginawa (o ni-reset).
 *
 * Auth ng device: "Authorization: Bearer <token>". Token = 32 random bytes; hash lang ang nasa DB (kaya may revoke).
 * Roles (galing sa SERVER):  admin = view-only sa pera;  client = nagsusulat ng transaksyon.
 * Tenant isolation: ang workspace ay laging galing sa token ng device, HINDI sa body/query ng app.
 *
 * Endpoints:
 *   GET  /health                         (public) DB check
 *   POST /register                       gumawa ng pamilya -> {familyId, recoveryKey, token (admin device)}
 *   POST /join                           {familyId, recoveryKey, deviceName, role: client|admin} -> token
 *   POST /recovery-key/rotate [admin]    bagong Recovery Key (ipinapakita minsan); ang dating key ay hindi na gagana
 *   GET  /devices          [admin]       lahat ng activated device ng pamilya (client + admin), may last seen
 *   POST /devices/:id/revoke [admin]
 *   POST /sign-out         [any]
 *   POST /sync/push        [client]      idempotent na batch ng ops
 *   GET  /sync/pull?since= [admin|client] mga record ng sariling workspace (seq > since)
 * OWNER (x-relay-admin-key), page: /relay/admin/family.html
 *   GET  /relay/admin/api/family                       pamilya + activated devices + buod ng pera
 *   GET  /relay/admin/api/family/:id/data              buong data mula sa relay (remittance, gastos, ipon, goal)
 *   POST /relay/admin/api/family/:id/block {blocked}   i-block / i-unblock
 *   POST /relay/admin/api/family/:id/reset-key         bagong Recovery Key kapag nawala
 *   DELETE /relay/admin/api/family/:id                 burahin ang pamilya at lahat ng data
 *   POST /relay/admin/api/family/devices/:id/revoke
 * Env (opsyonal): FAMILY_ADMIN_TOKEN_DAYS=365, FAMILY_CLIENT_TOKEN_DAYS=365.
 * Walang key/token/halaga na isinusulat sa log.
 */
const crypto = require('crypto');

const ALPHA = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const ADMIN_TOKEN_DAYS = Math.max(1, parseInt(process.env.FAMILY_ADMIN_TOKEN_DAYS || '365', 10) || 365);
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
const normSecret = (s) => String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
const newToken = () => crypto.randomBytes(32).toString('base64url');
const uid = (p) => p + '_' + crypto.randomBytes(9).toString('hex');

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
                await p.query(`ALTER TABLE family_groups ADD COLUMN IF NOT EXISTS secret_hash TEXT NOT NULL DEFAULT ''`);
                await p.query(`ALTER TABLE family_groups ADD COLUMN IF NOT EXISTS blocked BOOLEAN NOT NULL DEFAULT FALSE`);
                await p.query(`CREATE TABLE IF NOT EXISTS family_devices (device_id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, role TEXT NOT NULL,
                    user_id TEXT, name TEXT NOT NULL DEFAULT '', token_hash TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    expires_at TIMESTAMPTZ NOT NULL, revoked_at TIMESTAMPTZ, last_seen TIMESTAMPTZ)`);
                await p.query(`CREATE UNIQUE INDEX IF NOT EXISTS uq_family_device_token ON family_devices (token_hash)`);
                await p.query(`CREATE INDEX IF NOT EXISTS idx_family_devices_ws ON family_devices (workspace_id, role)`);
                await p.query(`ALTER TABLE family_devices ADD COLUMN IF NOT EXISTS app_version TEXT`);
                await p.query(`ALTER TABLE family_devices ADD COLUMN IF NOT EXISTS ip TEXT`);
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
            const r = t ? await q('SELECT d.device_id, d.workspace_id, d.role, d.user_id, d.expires_at, d.revoked_at, g.blocked FROM family_devices d JOIN family_groups g ON g.workspace_id = d.workspace_id WHERE d.token_hash = $1', [sha(t)]) : { rows: [] };
            const row = r.rows[0];
            if (!row || row.revoked_at || new Date(row.expires_at).getTime() < Date.now()) { noteFail(req.ip); return fail(res, 401, 'UNAUTHORIZED', 'Session is not valid.'); }
            if (row.blocked) return fail(res, 403, 'FAMILY_BLOCKED', 'This family is blocked. Contact the developer.');
            if (need && row.role !== need) return fail(res, 403, 'FORBIDDEN', 'Not allowed for this role.'); // server-side role check
            req.fam = { deviceId: row.device_id, workspaceId: row.workspace_id, role: row.role, userId: row.user_id };
            touchDevice(row.device_id, req);
            next();
        } catch (err) {
            console.error('Family auth error:', err && err.message);
            fail(res, 503, 'UNAVAILABLE', 'Family service temporarily unavailable.');
        }
    };

    const lastTouch = new Map(); // deviceId -> ms (huwag mag-write sa DB sa bawat request)
    function touchDevice(deviceId, req) {
        const now = Date.now();
        if (now - (lastTouch.get(deviceId) || 0) < 30000) return;
        lastTouch.set(deviceId, now);
        q('UPDATE family_devices SET last_seen = now(), ip = $2, app_version = COALESCE(NULLIF($3::text, \'\'), app_version) WHERE device_id = $1',
            [deviceId, req.ip, clamp(req.headers['x-app-version'], 30)]).catch(() => {});
    }

    async function issueDevice(workspaceId, role, name, days, req) {
        const token = newToken(), deviceId = uid('dev');
        await q('INSERT INTO family_devices (device_id, workspace_id, role, name, token_hash, expires_at, last_seen, ip, app_version) VALUES ($1,$2,$3,$4,$5, now() + ($6 || \' days\')::interval, now(), $7, $8)',
            [deviceId, workspaceId, role, clamp(name, 60) || (role === 'admin' ? 'Admin phone' : 'Family phone'), sha(token), String(days), req.ip, clamp(req.headers['x-app-version'] || (req.body && req.body.appVersion), 30)]);
        return { token, deviceId };
    }

    // ------------------------------------------------------------------ health
    app.get('/relay/family/health', d.rateLimit('family-health', 120, 60 * 1000), wrap(async (req, res) => {
        await q('SELECT 1');
        res.json({ success: true, status: 'ok', serverTime: Date.now() });
    }));

    // ------------------------------------------------ activation (kagaya ng School)
    const joinFails = new Map(); // familyId -> {count, first}: proteksyon laban sa paghula ng Recovery Key
    const joinLocked = (id) => { const f = joinFails.get(id); return !!(f && f.count >= 20 && Date.now() - f.first < 10 * 60000); };
    const joinFail = (id) => { const f = joinFails.get(id); if (!f || Date.now() - f.first > 10 * 60000) joinFails.set(id, { count: 1, first: Date.now() }); else f.count++; };

    app.post('/relay/family/register', d.rateLimit('family-register', 10, 60 * 60 * 1000), wrap(async (req, res) => {
        const familyName = clamp(req.body && req.body.familyName, 80) || 'My Family';
        const familyId = 'FAM-' + rnd(8);
        const secret = rnd(16);
        await q('INSERT INTO family_groups (workspace_id, name, secret_hash) VALUES ($1,$2,$3)', [familyId, familyName, sha(secret)]);
        const dev = await issueDevice(familyId, 'admin', req.body && req.body.deviceName, ADMIN_TOKEN_DAYS, req);
        audit(familyId, dev.deviceId, 'family_registered', '');
        try { d.logActivity && d.logActivity(familyId, 'family_registered', { name: familyName }); } catch (_) {}
        try { d.sendTelegramNotification && Promise.resolve(d.sendTelegramNotification('\uD83D\uDC68\u200D\uD83D\uDC69\u200D\uD83D\uDC67 Bagong pamilya ang nag-register\n' + familyName + '\nID: ' + familyId)).catch(() => {}); } catch (_) {}
        res.json({ success: true, role: 'admin', familyId, recoveryKey: secret, token: dev.token, workspaceId: familyId, deviceId: dev.deviceId, displayName: familyName });
    }));

    app.post('/relay/family/join', d.rateLimit('family-join', 20, 10 * 60 * 1000), wrap(async (req, res) => {
        if (tooManyFails(req.ip)) return fail(res, 429, 'RATE_LIMITED', 'Too many attempts.');
        const familyId = clamp(req.body && req.body.familyId, 40).toUpperCase();
        const secret = normSecret(req.body && req.body.recoveryKey);
        const role = String((req.body && req.body.role) || 'client') === 'admin' ? 'admin' : 'client';
        if (!familyId || secret.length !== 16) { noteFail(req.ip); return fail(res, 400, 'INVALID_CREDENTIAL', 'Wrong Family ID or Recovery Key.'); }
        if (joinLocked(familyId)) return fail(res, 429, 'RATE_LIMITED', 'Too many attempts.');
        const r = await q('SELECT workspace_id, name, secret_hash, blocked FROM family_groups WHERE workspace_id = $1', [familyId]);
        const g = r.rows[0];
        // Parehong sagot kapag walang ganoong pamilya o mali ang key (hindi nagbubunyag kung alin).
        if (!g || !g.secret_hash || !d.safeCompare(sha(secret), g.secret_hash)) { noteFail(req.ip); joinFail(familyId); return fail(res, 401, 'INVALID_CREDENTIAL', 'Wrong Family ID or Recovery Key.'); }
        if (g.blocked) return fail(res, 403, 'FAMILY_BLOCKED', 'This family is blocked. Contact the developer.');
        const dev = await issueDevice(g.workspace_id, role, req.body && req.body.deviceName, role === 'admin' ? ADMIN_TOKEN_DAYS : CLIENT_TOKEN_DAYS, req);
        audit(g.workspace_id, dev.deviceId, 'device_joined', role);
        res.json({ success: true, role, token: dev.token, familyId: g.workspace_id, workspaceId: g.workspace_id, deviceId: dev.deviceId, displayName: g.name });
    }));

    app.post('/relay/family/recovery-key/rotate', auth('admin'), d.rateLimit('family-rotate', 10, 60 * 60 * 1000), wrap(async (req, res) => {
        const secret = rnd(16);
        await q('UPDATE family_groups SET secret_hash = $2 WHERE workspace_id = $1', [req.fam.workspaceId, sha(secret)]);
        audit(req.fam.workspaceId, req.fam.deviceId, 'key_rotated', '');
        res.json({ success: true, familyId: req.fam.workspaceId, recoveryKey: secret });
    }));

    // ------------------------------------------------------------ device admin
    app.get('/relay/family/devices', auth('admin'), wrap(async (req, res) => {
        const r = await q("SELECT device_id, role, name, created_at, last_seen, revoked_at FROM family_devices WHERE workspace_id = $1 ORDER BY created_at DESC LIMIT 100", [req.fam.workspaceId]);
        res.json({ success: true, devices: r.rows.map((x) => ({ id: x.device_id, role: x.role, name: x.name, pairedAt: new Date(x.created_at).getTime(),
            lastSeen: x.last_seen ? new Date(x.last_seen).getTime() : 0, revoked: !!x.revoked_at, self: x.device_id === req.fam.deviceId })) });
    }));

    app.post('/relay/family/devices/:id/revoke', auth('admin'), d.rateLimit('family-revoke', 30, 60 * 60 * 1000), wrap(async (req, res) => {
        const id = clamp(req.params.id, 60);
        if (id === req.fam.deviceId) return fail(res, 400, 'BAD_REQUEST', 'Use sign out for this device.');
        const r = await q("UPDATE family_devices SET revoked_at = now() WHERE device_id = $1 AND workspace_id = $2 AND revoked_at IS NULL RETURNING device_id", [id, req.fam.workspaceId]);
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

    // ------------------------------------------------ OWNER VIEW (admin key)
    // Page: /relay/admin/family.html . Lahat ng activated na device at ang data nila na galing sa relay.
    const ONLINE_MS = 5 * 60 * 1000;   // seen within 5 min  = online now
    const RECENT_MS = 24 * 3600 * 1000;
    const deviceState = (x, now) => {
        const seen = x.last_seen ? new Date(x.last_seen).getTime() : 0;
        const revoked = !!x.revoked_at, expired = new Date(x.expires_at).getTime() < now;
        return { seen, state: revoked ? 'revoked' : expired ? 'expired' : (seen && now - seen < ONLINE_MS) ? 'online' : (seen && now - seen < RECENT_MS) ? 'recent' : 'idle' };
    };
    const devView = (x, now) => { const s = deviceState(x, now);
        return { id: x.device_id, role: x.role, name: x.name, pairedAt: new Date(x.created_at).getTime(), lastSeen: s.seen || null, state: s.state, appVersion: x.app_version || '', ip: x.ip || '' }; };

    /** Buod ng pera kada currency, kapareho ng patakaran ng app: confirmed lang ang remittance, hindi binibilang ang voided. */
    function summarize(rows) {
        const t = {};
        const cur = (c) => (t[c] || (t[c] = { received: 0, expenses: 0, savingsIn: 0, savingsOut: 0 }));
        for (const r of rows) {
            if (r.voided) continue;
            const p = r.payload || {}, c = p.currency || 'PHP';
            if (r.entity === 'remittance' && p.status === 'CONFIRMED' && Number.isInteger(p.receivedMinor) && p.confirmedAt) cur(c).received += p.receivedMinor;
            else if (r.entity === 'expense' && Number.isInteger(p.amountMinor)) cur(c).expenses += p.amountMinor;
            else if (r.entity === 'savings' && Number.isInteger(p.amountMinor)) { if (p.type === 'WITHDRAWAL') cur(c).savingsOut += p.amountMinor; else cur(c).savingsIn += p.amountMinor; }
        }
        for (const c of Object.keys(t)) { const x = t[c]; x.savingsBalance = x.savingsIn - x.savingsOut; x.available = x.received - x.expenses - x.savingsBalance; }
        return t;
    }

    app.get('/relay/admin/api/family', d.requireAdminKey, d.rateLimit('family-owner-list', 60, 10 * 60 * 1000), wrap(async (req, res) => {
        const g = await q(`SELECT g.workspace_id, g.name, g.created_at, g.blocked,
                (SELECT count(*) FROM family_records r WHERE r.workspace_id = g.workspace_id) AS records,
                (SELECT max(updated_at) FROM family_records r WHERE r.workspace_id = g.workspace_id) AS last_record
            FROM family_groups g ORDER BY g.created_at DESC LIMIT 200`);
        const ids = g.rows.map((x) => x.workspace_id);
        const dv = ids.length ? await q(`SELECT device_id, workspace_id, role, name, created_at, last_seen, revoked_at, expires_at, app_version, ip
            FROM family_devices WHERE workspace_id = ANY($1) ORDER BY last_seen DESC NULLS LAST, created_at DESC LIMIT 3000`, [ids]) : { rows: [] };
        const rec = ids.length ? await q(`SELECT workspace_id, entity, payload, voided FROM family_records WHERE workspace_id = ANY($1) AND entity IN ('remittance','expense','savings')`, [ids]) : { rows: [] };
        const now = Date.now();
        const byWs = new Map(), recByWs = new Map();
        for (const x of dv.rows) { const l = byWs.get(x.workspace_id) || []; l.push(devView(x, now)); byWs.set(x.workspace_id, l); }
        for (const x of rec.rows) { const l = recByWs.get(x.workspace_id) || []; l.push(x); recByWs.set(x.workspace_id, l); }
        res.json({ success: true, serverTime: now, families: g.rows.map((x) => ({
            familyId: x.workspace_id, workspaceId: x.workspace_id, name: x.name, createdAt: new Date(x.created_at).getTime(), blocked: !!x.blocked,
            records: Number(x.records), lastRecordAt: x.last_record ? new Date(x.last_record).getTime() : null,
            totals: summarize(recByWs.get(x.workspace_id) || []), devices: byWs.get(x.workspace_id) || [] })) });
    }));

    app.get('/relay/admin/api/family/:id/data', d.requireAdminKey, d.rateLimit('family-owner-data', 120, 10 * 60 * 1000), wrap(async (req, res) => {
        const ws = clamp(req.params.id, 40);
        const g = await q('SELECT workspace_id, name, created_at, blocked FROM family_groups WHERE workspace_id = $1', [ws]);
        if (!g.rows[0]) return fail(res, 404, 'NOT_FOUND', 'Family not found.');
        const now = Date.now();
        const dv = await q(`SELECT device_id, workspace_id, role, name, created_at, last_seen, revoked_at, expires_at, app_version, ip FROM family_devices WHERE workspace_id = $1 ORDER BY created_at`, [ws]);
        const names = new Map(dv.rows.map((x) => [x.device_id, x.name]));
        const r = await q(`SELECT entity, entity_id, payload, voided, device_id, updated_at, server_version FROM family_records WHERE workspace_id = $1 ORDER BY updated_at DESC LIMIT 5000`, [ws]);
        const out = { remittance: [], expense: [], savings: [], goal: [], category: [] };
        for (const x of r.rows) {
            const list = out[x.entity];
            if (list && list.length < 500) list.push({ id: x.entity_id, voided: !!x.voided, device: names.get(x.device_id) || '', updatedAt: new Date(x.updated_at).getTime(), version: x.server_version, data: x.payload });
        }
        res.json({ success: true, serverTime: now, family: { familyId: g.rows[0].workspace_id, name: g.rows[0].name, createdAt: new Date(g.rows[0].created_at).getTime(), blocked: !!g.rows[0].blocked },
            devices: dv.rows.map((x) => devView(x, now)), totals: summarize(r.rows), records: out, truncated: r.rows.length >= 5000 });
    }));

    app.post('/relay/admin/api/family/devices/:id/revoke', d.requireAdminKey, d.rateLimit('family-owner-revoke', 30, 60 * 60 * 1000), wrap(async (req, res) => {
        const r = await q('UPDATE family_devices SET revoked_at = now() WHERE device_id = $1 AND revoked_at IS NULL RETURNING workspace_id', [clamp(req.params.id, 60)]);
        if (!r.rows[0]) return fail(res, 404, 'NOT_FOUND', 'Device not found.');
        audit(r.rows[0].workspace_id, 'owner', 'device_revoked_by_owner', clamp(req.params.id, 60));
        res.json({ success: true });
    }));

    app.post('/relay/admin/api/family/:id/block', d.requireAdminKey, d.rateLimit('family-owner-block', 60, 60 * 60 * 1000), wrap(async (req, res) => {
        const blocked = !!(req.body && req.body.blocked);
        const r = await q('UPDATE family_groups SET blocked = $2 WHERE workspace_id = $1 RETURNING workspace_id', [clamp(req.params.id, 40), blocked]);
        if (!r.rows[0]) return fail(res, 404, 'NOT_FOUND', 'Family not found.');
        audit(r.rows[0].workspace_id, 'owner', blocked ? 'family_blocked' : 'family_unblocked', '');
        res.json({ success: true, blocked });
    }));

    app.post('/relay/admin/api/family/:id/reset-key', d.requireAdminKey, d.rateLimit('family-owner-resetkey', 20, 60 * 60 * 1000), wrap(async (req, res) => {
        const secret = rnd(16);
        const r = await q('UPDATE family_groups SET secret_hash = $2 WHERE workspace_id = $1 RETURNING workspace_id', [clamp(req.params.id, 40), sha(secret)]);
        if (!r.rows[0]) return fail(res, 404, 'NOT_FOUND', 'Family not found.');
        audit(r.rows[0].workspace_id, 'owner', 'key_reset_by_owner', '');
        res.json({ success: true, familyId: r.rows[0].workspace_id, recoveryKey: secret });
    }));

    app.delete('/relay/admin/api/family/:id', d.requireAdminKey, d.rateLimit('family-owner-delete', 20, 60 * 60 * 1000), wrap(async (req, res) => {
        const ws = clamp(req.params.id, 40);
        const ex = await q('SELECT 1 FROM family_groups WHERE workspace_id = $1', [ws]);
        if (!ex.rows[0]) return fail(res, 404, 'NOT_FOUND', 'Family not found.');
        await q('DELETE FROM family_users WHERE workspace_id = $1', [ws]).catch(() => {}); // lumang email accounts, kung meron pa
        for (const t of ['family_records', 'family_ops', 'family_devices', 'family_groups']) await q('DELETE FROM ' + t + ' WHERE workspace_id = $1', [ws]);
        audit(ws, 'owner', 'family_deleted', '');
        res.json({ success: true });
    }));
};
