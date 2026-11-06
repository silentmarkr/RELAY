'use strict';
/**
 * School Attendance module ng RELAY (School Gate + Parent Watch apps).
 *
 * Mga endpoint (lahat sa ilalim ng /relay/school):
 *   POST /register                  -> gumawa ng schoolId + recovery key (kapalit ng "activation" ng OmniPOS; libre)
 *   POST /sync            (school)  -> itulak ang estudyante/events, kunin ang bago galing sa ibang scanner phone
 *   GET  /info                      -> support desk open?, paraan ng sponsor
 *   POST /support-tickets (school)  -> ticket para sa developer (lalabas sa admin/tickets.html, installation id = schoolId)
 *   GET  /support-tickets/status (school)
 *   POST /sponsor/create  (school)  -> donasyon online (PayMongo/atbp., gamit ang parehong checkout ng OmniPOS)
 *   GET  /sponsor/status  (school)
 *   POST /parent/sponsor/create     -> donasyon mula sa Parent Watch (deviceId lang, walang school login)
 *   GET  /parent/sponsor/status
 *   GET  /parent/events             -> para sa Parent Watch app at sa web viewer (parent code lang)
 * Admin (x-relay-admin-key): GET /relay/admin/api/schools, POST .../:id/block, DELETE .../:id
 * Web viewer ng magulang: /s  at  /s/<CODE>   (public/school-viewer.html)
 */
const crypto = require('crypto');
const path = require('path');

const ALPHA = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const TZ_OFFSET_MIN = parseInt(process.env.SCHOOL_TZ_OFFSET_MIN || '480', 10); // Pilipinas
const KEEP_DAYS = Math.max(14, parseInt(process.env.SCHOOL_KEEP_DAYS || '120', 10));
const VALID_TYPES = ['GATE_IN', 'ROOM_IN', 'BREAK_OUT', 'BREAK_IN', 'LUNCH_OUT', 'LUNCH_IN', 'DISMISS'];
const PROXY_KEY = process.env.SCHOOL_PROXY_KEY || ''; // sikreto ng Cloudflare Worker ng share.omniposph.com (optional)

function rnd(len) {
    let s = '';
    const buf = crypto.randomBytes(len);
    for (let i = 0; i < len; i++) s += ALPHA[buf[i] % ALPHA.length];
    return s;
}
const sha = (s) => crypto.createHash('sha256').update(String(s)).digest('hex');
const clamp = (v, n) => String(v === undefined || v === null ? '' : v).trim().slice(0, n);
const normSecret = (s) => String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
const TZ_SQL = Number.isFinite(TZ_OFFSET_MIN) ? TZ_OFFSET_MIN : 480;
const dayOf = (ts) => new Date(ts + TZ_OFFSET_MIN * 60000).toISOString().slice(0, 10);

module.exports = function mountSchool(d) {
    const { app, express } = d;
    const dbPool = () => d.pool();
    const q = (text, params) => d.queryWithRetry(dbPool(), text, params);

    // ------------------------------------------------------------------ schema
    let schemaPromise = null;
    function ensureSchema() {
        if (!schemaPromise) {
            schemaPromise = (async () => {
                const p = dbPool();
                if (!p) throw new Error('Postgres is not configured.');
                await p.query(`CREATE TABLE IF NOT EXISTS relay_schools (
                    school_id TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '', secret_hash TEXT NOT NULL,
                    blocked BOOLEAN NOT NULL DEFAULT FALSE, device_name TEXT, app_version TEXT, ip TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_seen TIMESTAMPTZ)`);
                await p.query(`ALTER TABLE relay_schools ADD COLUMN IF NOT EXISTS absent_after_min INT NOT NULL DEFAULT 600`);
                await p.query(`CREATE TABLE IF NOT EXISTS relay_school_students (
                    school_id TEXT NOT NULL, student_id TEXT NOT NULL, name TEXT NOT NULL, section TEXT NOT NULL DEFAULT '',
                    guardian TEXT NOT NULL DEFAULT '', parent_code TEXT NOT NULL, updated_at BIGINT NOT NULL DEFAULT 0,
                    srv_ts BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (school_id, student_id))`);
                await p.query(`CREATE UNIQUE INDEX IF NOT EXISTS uq_school_parent_code ON relay_school_students (parent_code)`);
                await p.query(`CREATE INDEX IF NOT EXISTS idx_school_students_srv ON relay_school_students (school_id, srv_ts)`);
                await p.query(`CREATE TABLE IF NOT EXISTS relay_school_events (
                    seq BIGSERIAL, school_id TEXT NOT NULL, event_id TEXT NOT NULL, student_id TEXT NOT NULL, type TEXT NOT NULL,
                    ts BIGINT NOT NULL, late BOOLEAN NOT NULL DEFAULT FALSE, station TEXT NOT NULL DEFAULT '',
                    received_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (school_id, event_id))`);
                await p.query(`CREATE INDEX IF NOT EXISTS idx_school_events_student ON relay_school_events (school_id, student_id, ts)`);
                await p.query(`CREATE INDEX IF NOT EXISTS idx_school_events_seq ON relay_school_events (school_id, seq)`);
                // Larawan ng estudyante (320x320 JPEG). Para lang sa mga scanner phone ng school (school ID + secret); hindi ibinibigay sa parent endpoint.
                await p.query(`CREATE TABLE IF NOT EXISTS relay_school_photos (
                    school_id TEXT NOT NULL, student_id TEXT NOT NULL, data BYTEA, deleted BOOLEAN NOT NULL DEFAULT FALSE,
                    updated_at BIGINT NOT NULL DEFAULT 0, srv_ts BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (school_id, student_id))`);
                await p.query(`CREATE INDEX IF NOT EXISTS idx_school_photos_srv ON relay_school_photos (school_id, srv_ts)`);
                // Isang event kada estudyante, kada uri ng scan, kada araw (oras sa Pilipinas). Ang pinakaunang scan ang nananatili.
                await p.query(`ALTER TABLE relay_school_events ADD COLUMN IF NOT EXISTS day TEXT`);
                await p.query(`UPDATE relay_school_events SET day = to_char(to_timestamp((ts + ${TZ_SQL}::bigint * 60000)::double precision / 1000.0) AT TIME ZONE 'UTC', 'YYYY-MM-DD') WHERE day IS NULL`);
                await p.query(`DELETE FROM relay_school_events a USING relay_school_events b
                    WHERE a.school_id = b.school_id AND a.student_id = b.student_id AND a.type = b.type AND a.day = b.day
                      AND (a.ts > b.ts OR (a.ts = b.ts AND a.event_id > b.event_id))`);
                await p.query(`CREATE UNIQUE INDEX IF NOT EXISTS uq_school_event_day ON relay_school_events (school_id, student_id, type, day)`);
            })().catch((e) => { schemaPromise = null; throw e; });
        }
        return schemaPromise;
    }
    const withSchema = (fn) => async (req, res, next) => {
        try { await ensureSchema(); await fn(req, res, next); }
        catch (err) {
            console.error('⚠️ School module error:', err && err.message);
            if (!res.headersSent) res.status(503).json({ success: false, message: 'School service temporarily unavailable.' });
        }
    };

    // ------------------------------------------------------------------ helpers
    function clientIp(req) {
        if (PROXY_KEY && d.safeCompare(String(req.headers['x-share-proxy-key'] || ''), PROXY_KEY)) {
            const h = String(req.headers['x-client-ip'] || '').trim();
            if (h) return h;
        }
        return req.ip;
    }
    const schoolCache = new Map(); // schoolId -> { row, at }
    async function loadSchool(id) {
        const hit = schoolCache.get(id);
        if (hit && Date.now() - hit.at < 20000) return hit.row;
        const r = await q('SELECT school_id, name, secret_hash, blocked FROM relay_schools WHERE school_id = $1', [id]);
        const row = r.rows[0] || null;
        schoolCache.set(id, { row, at: Date.now() });
        return row;
    }
    const lastSeenAt = new Map();
    function touch(id, req, extra) {
        const now = Date.now();
        if (now - (lastSeenAt.get(id) || 0) < 60000 && !(extra && extra.force)) return;
        lastSeenAt.set(id, now);
        q('UPDATE relay_schools SET last_seen = now(), ip = $2, device_name = COALESCE($3::text, device_name), app_version = COALESCE($4::text, app_version), name = COALESCE(NULLIF($5::text, \'\'), name) WHERE school_id = $1',
            [id, clientIp(req), extra && extra.device ? clamp(extra.device, 60) : null, extra && extra.version ? clamp(extra.version, 30) : null, extra && extra.name ? clamp(extra.name, 120) : '']).catch(() => {});
    }
    const authFails = new Map();
    async function schoolAuth(req, res, next) {
        try {
            await ensureSchema();
            const ip = clientIp(req);
            const f = authFails.get(ip);
            if (f && f.count >= 20 && Date.now() - f.first < 10 * 60000) return res.status(429).json({ success: false, message: 'Too many attempts.' });
            const id = clamp(req.headers['x-school-id'], 40);
            const secret = normSecret(req.headers['x-school-secret']);
            const row = id ? await loadSchool(id) : null;
            if (!row || !secret || !d.safeCompare(sha(secret), row.secret_hash)) {
                const cur = authFails.get(ip);
                if (!cur || Date.now() - cur.first > 10 * 60000) authFails.set(ip, { count: 1, first: Date.now() }); else cur.count++;
                return res.status(401).json({ success: false, code: 'BAD_SCHOOL_CREDENTIAL', message: 'Maling School ID o Recovery Key.' });
            }
            if (row.blocked) return res.status(403).json({ success: false, code: 'SCHOOL_BLOCKED', message: 'Naka-block ang school na ito. Makipag-ugnayan sa developer.' });
            req.school = row;
            return next();
        } catch (err) {
            console.error('⚠️ School auth error:', err && err.message);
            return res.status(503).json({ success: false, message: 'School service temporarily unavailable.' });
        }
    }

    // ------------------------------------------------------------------ register
    app.post('/relay/school/register', d.rateLimit('school-register', 10, 60 * 60 * 1000), withSchema(async (req, res) => {
        const name = clamp(req.body && req.body.name, 120) || 'Aking Paaralan';
        const schoolId = 'SCH-' + rnd(8);
        const secret = rnd(16);
        await q('INSERT INTO relay_schools (school_id, name, secret_hash, device_name, app_version, ip, last_seen) VALUES ($1,$2,$3,$4,$5,$6, now())',
            [schoolId, name, sha(secret), clamp(req.body && req.body.device, 60), clamp(req.body && req.body.appVersion, 30), clientIp(req)]);
        try { d.logActivity(schoolId, 'school_registered', { name }); } catch (_) {}
        try { Promise.resolve(d.sendTelegramNotification(`🏫 Bagong school ang nag-register\n${name}\nID: ${schoolId}`)).catch(() => {}); } catch (_) {}
        res.json({ success: true, schoolId, secret });
    }));

    // ------------------------------------------------------------------ info (public)
    app.get('/relay/school/info', d.rateLimit('school-info', 120, 10 * 60 * 1000), async (req, res) => {
        const desk = d.getSupportDesk() || {};
        let methods = [];
        try { await d.refreshPaymongoCapabilities(false); methods = d.getAvailablePaymentMethods().map((m) => ({ id: m.id, label: m.label })); } catch (_) {}
        res.json({
            success: true, serverTime: Date.now(),
            supportOpen: !!desk.open, supportMessage: desk.message || '',
            sponsor: { methods, minPHP: d.SPONSOR_MIN_PHP, onlineMinPHP: d.SPONSOR_ONLINE_MIN_PHP, maxPHP: d.SPONSOR_MAX_PHP }
        });
    });

    // ------------------------------------------------------------------ sync
    app.post('/relay/school/sync', schoolAuth, d.rateLimit('school-sync', 240, 10 * 60 * 1000, (req) => req.headers['x-school-id']), withSchema(async (req, res) => {
        const sid = req.school.school_id;
        const body = req.body || {};
        const now = Date.now();
        touch(sid, req, { device: body.device, version: body.appVersion, name: body.name });
        // Oras (minuto mula hatinggabi) pagkatapos nito, ang walang scan ay ituturing na ABSENT sa magulang.
        const aam = Number(body.absentAfterMin);
        if (Number.isInteger(aam) && aam >= 0 && aam < 1440) {
            q('UPDATE relay_schools SET absent_after_min = $2 WHERE school_id = $1 AND absent_after_min <> $2', [sid, aam]).catch(() => {});
        }

        // 1) estudyante (upsert; mas bago lang ang papalit)
        const students = Array.isArray(body.students) ? body.students.slice(0, 1000) : [];
        let conflicts = 0;
        const upsertOne = async (s) => {
            await q(`INSERT INTO relay_school_students (school_id, student_id, name, section, guardian, parent_code, updated_at, srv_ts)
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                     ON CONFLICT (school_id, student_id) DO UPDATE SET name = EXCLUDED.name, section = EXCLUDED.section,
                       guardian = EXCLUDED.guardian, parent_code = EXCLUDED.parent_code, updated_at = EXCLUDED.updated_at, srv_ts = EXCLUDED.srv_ts
                     WHERE relay_school_students.updated_at <= EXCLUDED.updated_at`,
                [sid, s.id, s.name, s.section, s.guardian, s.parentCode, s.updatedAt, now]);
        };
        for (const raw of students) {
            const s = {
                id: clamp(raw && raw.id, 40), name: clamp(raw && raw.name, 120), section: clamp(raw && raw.section, 60),
                guardian: clamp(raw && raw.guardian, 120), parentCode: clamp(raw && raw.parentCode, 20).toUpperCase(),
                updatedAt: Number(raw && raw.updatedAt) || now
            };
            if (!s.id || !s.name || s.parentCode.length < 8) continue;
            try { await upsertOne(s); } catch (e) { if (e && e.code === '23505') conflicts++; else throw e; }
        }
        // 2) binura
        const del = Array.isArray(body.deletedStudents) ? body.deletedStudents.slice(0, 1000).map((x) => clamp(x, 40)).filter(Boolean) : [];
        if (del.length) {
            await q('DELETE FROM relay_school_events WHERE school_id = $1 AND student_id = ANY($2::text[])', [sid, del]);
            await q('DELETE FROM relay_school_photos WHERE school_id = $1 AND student_id = ANY($2::text[])', [sid, del]);
            await q('DELETE FROM relay_school_students WHERE school_id = $1 AND student_id = ANY($2::text[])', [sid, del]);
        }
        // 3) events (idempotent; para lang sa estudyanteng nasa school na ito)
        const evs = (Array.isArray(body.events) ? body.events.slice(0, 500) : []).filter((e) => e && e.id && e.studentId && VALID_TYPES.includes(e.type));
        let accepted = 0;
        let duplicates = [];
        if (evs.length) {
            evs.sort((a, b) => (Number(a.ts) || 0) - (Number(b.ts) || 0)); // pinakaunang scan ang unang papasok
            const r = await q(`INSERT INTO relay_school_events (school_id, event_id, student_id, type, ts, late, station, day)
                SELECT $1::text, t.id, t.sid, t.type, t.ts, t.late, t.station,
                       to_char(to_timestamp((t.ts + $8::bigint * 60000)::double precision / 1000.0) AT TIME ZONE 'UTC', 'YYYY-MM-DD')
                FROM unnest($2::text[], $3::text[], $4::text[], $5::bigint[], $6::boolean[], $7::text[]) AS t(id, sid, type, ts, late, station)
                WHERE EXISTS (SELECT 1 FROM relay_school_students s WHERE s.school_id = $1 AND s.student_id = t.sid)
                ON CONFLICT DO NOTHING`,
                [sid, evs.map((e) => clamp(e.id, 80)), evs.map((e) => clamp(e.studentId, 40)), evs.map((e) => e.type),
                    evs.map((e) => Number(e.ts) || now), evs.map((e) => !!e.late), evs.map((e) => clamp(e.station, 60)), TZ_SQL]);
            accepted = r.rowCount || 0;
            if (accepted < evs.length) {
                // Alin ang tinanggihan dahil may naka-tala na ngayong araw (ibang phone)? Ibabalik para mabura sa phone.
                const ids = evs.map((e) => clamp(e.id, 80));
                const have = new Set((await q('SELECT event_id FROM relay_school_events WHERE school_id = $1 AND event_id = ANY($2::text[])', [sid, ids])).rows.map((x) => x.event_id));
                const known = new Set((await q('SELECT student_id FROM relay_school_students WHERE school_id = $1 AND student_id = ANY($2::text[])', [sid, evs.map((e) => clamp(e.studentId, 40))])).rows.map((x) => x.student_id));
                duplicates = evs.filter((e) => !have.has(clamp(e.id, 80)) && known.has(clamp(e.studentId, 40))).map((e) => clamp(e.id, 80));
            }
        }
        // 4) para sa phone: bagong estudyante at events galing sa ibang scanner phone
        const cursor = Math.max(0, Number(body.cursor) || 0);
        const evSeq = Math.max(0, Number(body.eventSeq) || 0);
        const days = Math.min(KEEP_DAYS, Math.max(1, Number(body.eventDays) || 2));
        const sr = await q('SELECT student_id, name, section, guardian, parent_code, updated_at, srv_ts FROM relay_school_students WHERE school_id = $1 AND srv_ts > $2 ORDER BY srv_ts LIMIT 2000', [sid, cursor]);
        const er = await q('SELECT seq, event_id, student_id, type, ts, late, station FROM relay_school_events WHERE school_id = $1 AND seq > $2 AND ts > $3 ORDER BY seq LIMIT 3000',
            [sid, evSeq, now - days * 86400000]);
        const lastSeq = er.rows.length ? Number(er.rows[er.rows.length - 1].seq) : evSeq;
        const newCursor = sr.rows.length ? Number(sr.rows[sr.rows.length - 1].srv_ts) : cursor;
        // larawan: ano ang bago/nabago mula sa huling photoCursor ng phone (metadata lang; hiwalay ang pag-download)
        const pcur = Math.max(0, Number(body.photoCursor) || 0);
        const pr = await q('SELECT student_id, deleted, updated_at, srv_ts FROM relay_school_photos WHERE school_id = $1 AND srv_ts > $2 ORDER BY srv_ts LIMIT 100', [sid, pcur]);
        const photoCursor = pr.rows.length ? Number(pr.rows[pr.rows.length - 1].srv_ts) : pcur;
        // paminsan-minsan: linisin ang lumang events
        if (Math.random() < 0.02) q('DELETE FROM relay_school_events WHERE school_id = $1 AND ts < $2', [sid, now - KEEP_DAYS * 86400000]).catch(() => {});
        res.json({
            success: true, accepted, duplicates, conflicts, serverTime: now, schoolName: req.school.name, cursor: newCursor, eventSeq: lastSeq,
            hasMoreEvents: er.rows.length >= 3000, photoCursor, hasMorePhotos: pr.rows.length >= 100,
            photos: pr.rows.map((r) => ({ studentId: r.student_id, deleted: !!r.deleted, updatedAt: Number(r.updated_at) })), hasMoreStudents: sr.rows.length >= 2000,
            students: sr.rows.map((r) => ({ id: r.student_id, name: r.name, section: r.section, guardian: r.guardian, parentCode: r.parent_code, updatedAt: Number(r.updated_at) })),
            events: er.rows.map((r) => ({ id: r.event_id, studentId: r.student_id, type: r.type, ts: Number(r.ts), late: r.late, station: r.station }))
        });
    }));

    // ------------------------------------------------------------------ photos (school auth lang)
    const photoLimiter = (name) => d.rateLimit(name, 900, 60 * 60 * 1000, (req) => req.headers['x-school-id']);
    app.post('/relay/school/photo', schoolAuth, photoLimiter('school-photo-up'), withSchema(async (req, res) => {
        const sid = req.school.school_id;
        const b = req.body || {};
        const studentId = clamp(b.studentId, 40);
        if (!studentId) return res.status(400).json({ success: false, message: 'Walang studentId.' });
        const ex = await q('SELECT 1 FROM relay_school_students WHERE school_id = $1 AND student_id = $2', [sid, studentId]);
        if (!ex.rows[0]) return res.status(404).json({ success: false, message: 'Hindi nahanap ang estudyante.' });
        const now = Date.now();
        const updatedAt = Math.min(now + 86400000, Math.max(1, Number(b.updatedAt) || now));
        let data = null;
        const del = b.delete === true;
        if (!del) {
            if (typeof b.photo !== 'string' || b.photo.length < 100 || b.photo.length > 400000) return res.status(400).json({ success: false, message: 'Hindi tanggap ang larawan.' });
            data = Buffer.from(b.photo, 'base64');
            if (data.length > 200 * 1024 || data.length < 100 || data[0] !== 0xFF || data[1] !== 0xD8) return res.status(400).json({ success: false, message: 'JPEG lang, hanggang 200 KB.' });
        }
        await q(`INSERT INTO relay_school_photos (school_id, student_id, data, deleted, updated_at, srv_ts) VALUES ($1,$2,$3,$4,$5,$6)
                 ON CONFLICT (school_id, student_id) DO UPDATE SET data = EXCLUDED.data, deleted = EXCLUDED.deleted, updated_at = EXCLUDED.updated_at, srv_ts = EXCLUDED.srv_ts
                 WHERE relay_school_photos.updated_at <= EXCLUDED.updated_at`, [sid, studentId, data, del, updatedAt, now]);
        res.json({ success: true });
    }));
    app.get('/relay/school/photo', schoolAuth, photoLimiter('school-photo-down'), withSchema(async (req, res) => {
        const r = await q('SELECT data, updated_at FROM relay_school_photos WHERE school_id = $1 AND student_id = $2 AND deleted = FALSE', [req.school.school_id, clamp(req.query.studentId, 40)]);
        if (!r.rows[0] || !r.rows[0].data) return res.status(404).json({ success: false, message: 'Walang larawan.' });
        res.json({ success: true, photo: Buffer.from(r.rows[0].data).toString('base64'), updatedAt: Number(r.rows[0].updated_at) });
    }));

    // ------------------------------------------------------------------ support tickets
    app.post('/relay/school/support-tickets', schoolAuth, d.rateLimit('school-ticket', 30, 60 * 60 * 1000, (req) => req.headers['x-school-id']), withSchema(async (req, res) => {
        const desk = d.getSupportDesk() || {};
        if (!desk.open) return res.status(503).json({ success: false, code: 'SUPPORT_DESK_CLOSED', message: desk.message || 'Sarado muna ang support. Naka-save ang ticket mo at ipapadala pagbukas.' });
        const tp = d.ticketPool();
        if (!tp) return res.status(503).json({ success: false, message: 'Devices/License database is not configured.' });
        const t = (req.body && typeof req.body.ticket === 'object' && req.body.ticket) || null;
        const localId = clamp(t && t.id, 80);
        const message = clamp(t && t.message, 4000);
        if (!t || !localId || !message) return res.status(400).json({ success: false, message: 'Kulang ang ticket.' });
        const subject = '[School] ' + (clamp(t.subject, 140) || 'Support request');
        let diag = null;
        if (t.diagnostics && typeof t.diagnostics === 'object') {
            diag = {};
            Object.keys(t.diagnostics).slice(0, 40).forEach((k) => { diag[clamp(k, 40)] = clamp(t.diagnostics[k], 200); });
        }
        let createdAt = new Date(Number(t.createdAt) || Date.now());
        if (Number.isNaN(createdAt.getTime())) createdAt = new Date();
        const sid = req.school.school_id;
        const storeName = '🏫 ' + clamp(req.school.name, 140);
        const ins = await d.queryWithRetry(tp,
            `INSERT INTO relay_support_tickets (installation_id, local_ticket_id, store_name, username, subject, message, transcript, diagnostics, created_at)
             VALUES ($1,$2,$3,$4,$5,$6,'[]'::jsonb,$7::jsonb,$8) ON CONFLICT (installation_id, local_ticket_id) DO NOTHING RETURNING id`,
            [sid, localId, storeName, clamp(t.contact, 120) || null, subject, message, diag ? JSON.stringify(diag) : null, createdAt.toISOString()]);
        const created = ins.rows.length > 0;
        if (created) {
            try { d.logActivity(sid, 'support_ticket_received', { subject }); } catch (_) {}
            const text = `🎫 Bagong School support ticket\nSchool: ${req.school.name} (${sid})\nMula kay: ${clamp(t.contact, 120) || 'Hindi binanggit'}\nSubject: ${subject}`;
            try { Promise.resolve(d.sendSlackNotification(text)).catch(() => {}); } catch (_) {}
            try { Promise.resolve(d.sendTelegramNotification(text)).catch(() => {}); } catch (_) {}
        }
        const cur = await d.queryWithRetry(tp, 'SELECT status, admin_note, updated_at FROM relay_support_tickets WHERE installation_id = $1 AND local_ticket_id = $2', [sid, localId]);
        const row = cur.rows[0] || { status: 'open', admin_note: '', updated_at: new Date() };
        res.json({ success: true, created, status: row.status, adminNote: row.admin_note || '', updatedAt: row.updated_at });
    }));
    app.get('/relay/school/support-tickets/status', schoolAuth, d.rateLimit('school-ticket-status', 120, 60 * 60 * 1000, (req) => req.headers['x-school-id']), withSchema(async (req, res) => {
        const tp = d.ticketPool();
        if (!tp) return res.status(503).json({ success: false, message: 'Devices/License database is not configured.' });
        const r = await d.queryWithRetry(tp, 'SELECT local_ticket_id, status, admin_note, updated_at FROM relay_support_tickets WHERE installation_id = $1 ORDER BY received_at DESC, id DESC LIMIT 50', [req.school.school_id]);
        const desk = d.getSupportDesk() || {};
        res.json({ success: true, supportOpen: !!desk.open, tickets: r.rows.map((x) => ({ localTicketId: x.local_ticket_id, status: x.status, adminNote: x.admin_note || '', updatedAt: x.updated_at })) });
    }));

    // ------------------------------------------------------------------ sponsor
    app.post('/relay/school/sponsor/create', schoolAuth, d.rateLimit('school-sponsor-create', 20, 60 * 60 * 1000, (req) => req.headers['x-school-id']), withSchema(async (req, res) => {
        const sid = req.school.school_id;
        const { method, returnBaseUrl } = req.body || {};
        const amount = d.parseSponsorAmount(req.body && req.body.amount);
        if (amount === null) return res.status(400).json({ success: false, message: `Maglagay ng buong halaga mula ₱${d.SPONSOR_MIN_PHP} hanggang ₱${d.SPONSOR_MAX_PHP}.` });
        if (amount < d.SPONSOR_ONLINE_MIN_PHP) return res.status(400).json({ success: false, message: `Ang online payment ay mula ₱${d.SPONSOR_ONLINE_MIN_PHP} pataas.` });
        const pp = d.purchasePool();
        if (!pp) return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured.' });
        await d.refreshPaymongoCapabilities(false);
        if (!d.isPaymentMethodAvailable(method)) {
            const available = d.getAvailablePaymentMethods().map((m) => m.label).join(', ') || 'wala pang naka-configure';
            return res.status(400).json({ success: false, message: `Hindi available ang paraan ng bayad na ito. Available ngayon: ${available}.` });
        }
        const purchaseId = `DON-${sid.slice(0, 8)}-${Date.now()}-${crypto.randomBytes(3).toString('hex')}`;
        const base = (returnBaseUrl && /^https?:\/\//.test(returnBaseUrl)) ? String(returnBaseUrl).replace(/\/$/, '') : `${req.protocol}://${req.get('host')}`;
        try {
            const checkout = await d.createPaymentCheckout({ method, amountPHP: amount, purchaseId, base, description: `School Attendance Sponsor Donation (PHP ${amount})` });
            await d.queryWithRetry(pp,
                `INSERT INTO cloud_token_purchases (purchase_id, installation_id, package_id, tokens, amount_php, method, source_id, provider, status)
                 VALUES ($1, $2, 'DONATION', 0, $3, $4, $5, $6, 'pending')`,
                [purchaseId, sid, amount, method, checkout.providerRefId, checkout.provider]);
            try { d.logActivity(sid, 'sponsor_donation_created', { purchaseId, amountPHP: amount, method, provider: checkout.provider }); } catch (_) {}
            res.json({ success: true, purchaseId, checkoutUrl: checkout.checkoutUrl || null, qrCodeImageUrl: checkout.qrCodeImageUrl || null, expiresAt: checkout.expiresAt || null, amountPHP: amount });
        } catch (err) {
            const cfg = /_NOT_CONFIGURED$/.test(String(err.code || '')) || err.code === 'PROVIDER_NOT_CONFIGURED';
            res.status(cfg ? 503 : 502).json({ success: false, message: err.message });
        }
    }));
    app.get('/relay/school/sponsor/status', schoolAuth, d.rateLimit('school-sponsor-status', 300, 60 * 60 * 1000, (req) => req.headers['x-school-id']), withSchema(async (req, res) => {
        const purchaseId = clamp(req.query.purchaseId, 120);
        if (!purchaseId.startsWith('DON-')) return res.status(400).json({ success: false, message: 'Invalid purchaseId.' });
        const pp = d.purchasePool();
        if (!pp) return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured.' });
        const r = await d.queryWithRetry(pp, 'SELECT status, amount_php, method FROM cloud_token_purchases WHERE purchase_id = $1 AND installation_id = $2', [purchaseId, req.school.school_id]);
        if (!r.rows[0]) return res.status(404).json({ success: false, message: 'Hindi nahanap ang donasyon.' });
        if (r.rows[0].status === 'paid') { try { await d.backfillPaidOnlineDonations(); } catch (_) {} }
        res.json({ success: true, status: r.rows[0].status, amountPHP: Number(r.rows[0].amount_php), method: r.rows[0].method });
    }));

    // ------------------------------------------------------------------ sponsor (Parent Watch)
    // Walang school login ang magulang, kaya ang pagkakakilanlan ay random na "deviceId" ng app (PARENT-<id>).
    // Parehong checkout at talaan ng donasyon ng School Gate (DON-... purchase_id), kaya lalabas din ito sa sponsor donations ng admin.
    const parentInst = (raw) => {
        const id = String(raw || '').replace(/[^A-Za-z0-9]/g, '').slice(0, 32);
        return id.length >= 12 ? 'PARENT-' + id : '';
    };
    app.post('/relay/school/parent/sponsor/create', d.rateLimit('parent-sponsor-create', 20, 60 * 60 * 1000, (req) => clientIp(req)), withSchema(async (req, res) => {
        const inst = parentInst(req.body && req.body.deviceId);
        if (!inst) return res.status(400).json({ success: false, message: 'Invalid device.' });
        const { method, returnBaseUrl } = req.body || {};
        const amount = d.parseSponsorAmount(req.body && req.body.amount);
        if (amount === null) return res.status(400).json({ success: false, message: `Maglagay ng buong halaga mula ₱${d.SPONSOR_MIN_PHP} hanggang ₱${d.SPONSOR_MAX_PHP}.` });
        if (amount < d.SPONSOR_ONLINE_MIN_PHP) return res.status(400).json({ success: false, message: `Ang online payment ay mula ₱${d.SPONSOR_ONLINE_MIN_PHP} pataas.` });
        const pp = d.purchasePool();
        if (!pp) return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured.' });
        await d.refreshPaymongoCapabilities(false);
        if (!d.isPaymentMethodAvailable(method)) {
            const available = d.getAvailablePaymentMethods().map((m) => m.label).join(', ') || 'wala pang naka-configure';
            return res.status(400).json({ success: false, message: `Hindi available ang paraan ng bayad na ito. Available ngayon: ${available}.` });
        }
        const purchaseId = `DON-${inst.slice(7, 15)}-${Date.now()}-${crypto.randomBytes(3).toString('hex')}`;
        const base = (returnBaseUrl && /^https?:\/\//.test(returnBaseUrl)) ? String(returnBaseUrl).replace(/\/$/, '') : `${req.protocol}://${req.get('host')}`;
        try {
            const checkout = await d.createPaymentCheckout({ method, amountPHP: amount, purchaseId, base, description: `Parent Watch Sponsor Donation (PHP ${amount})` });
            await d.queryWithRetry(pp,
                `INSERT INTO cloud_token_purchases (purchase_id, installation_id, package_id, tokens, amount_php, method, source_id, provider, status)
                 VALUES ($1, $2, 'DONATION', 0, $3, $4, $5, $6, 'pending')`,
                [purchaseId, inst, amount, method, checkout.providerRefId, checkout.provider]);
            try { d.logActivity(inst, 'sponsor_donation_created', { purchaseId, amountPHP: amount, method, provider: checkout.provider, app: 'parent-watch' }); } catch (_) {}
            res.json({ success: true, purchaseId, checkoutUrl: checkout.checkoutUrl || null, qrCodeImageUrl: checkout.qrCodeImageUrl || null, expiresAt: checkout.expiresAt || null, amountPHP: amount });
        } catch (err) {
            const cfg = /_NOT_CONFIGURED$/.test(String(err.code || '')) || err.code === 'PROVIDER_NOT_CONFIGURED';
            res.status(cfg ? 503 : 502).json({ success: false, message: err.message });
        }
    }));
    app.get('/relay/school/parent/sponsor/status', d.rateLimit('parent-sponsor-status', 300, 60 * 60 * 1000, (req) => clientIp(req)), withSchema(async (req, res) => {
        const inst = parentInst(req.query.deviceId);
        const purchaseId = clamp(req.query.purchaseId, 120);
        if (!inst || !purchaseId.startsWith('DON-')) return res.status(400).json({ success: false, message: 'Invalid request.' });
        const pp = d.purchasePool();
        if (!pp) return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured.' });
        const r = await d.queryWithRetry(pp, 'SELECT status, amount_php, method FROM cloud_token_purchases WHERE purchase_id = $1 AND installation_id = $2', [purchaseId, inst]);
        if (!r.rows[0]) return res.status(404).json({ success: false, message: 'Hindi nahanap ang donasyon.' });
        if (r.rows[0].status === 'paid') { try { await d.backfillPaidOnlineDonations(); } catch (_) {} }
        res.json({ success: true, status: r.rows[0].status, amountPHP: Number(r.rows[0].amount_php), method: r.rows[0].method });
    }));

    // ------------------------------------------------------------------ parent (app + web viewer)
    const parentFails = new Map();
    app.get('/relay/school/parent/events', d.rateLimit('school-parent', 900, 10 * 60 * 1000, (req) => clientIp(req)), withSchema(async (req, res) => {
        const ip = clientIp(req);
        const f = parentFails.get(ip);
        if (f && f.count >= 30 && Date.now() - f.first < 10 * 60000) return res.status(429).json({ success: false, message: 'Masyadong maraming maling subok. Subukan ulit mamaya.' });
        const code = clamp(req.query.code, 20).toUpperCase().replace(/[^A-Z0-9]/g, '');
        const sr = code.length >= 8 ? await q(`SELECT s.school_id, s.student_id, s.name, s.section, c.name AS school_name, c.blocked, c.absent_after_min
                                               FROM relay_school_students s JOIN relay_schools c ON c.school_id = s.school_id WHERE s.parent_code = $1`, [code]) : { rows: [] };
        const st = sr.rows[0];
        if (!st || st.blocked) {
            const cur = parentFails.get(ip);
            if (!cur || Date.now() - cur.first > 10 * 60000) parentFails.set(ip, { count: 1, first: Date.now() }); else cur.count++;
            return res.status(404).json({ success: false, message: 'Hindi nahanap ang code.' });
        }
        const today = dayOf(Date.now());
        const date = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.date || '')) ? String(req.query.date) : today;
        const start = Date.parse(date + 'T00:00:00Z') - TZ_OFFSET_MIN * 60000;
        const er = await q('SELECT event_id, type, ts, late, station FROM relay_school_events WHERE school_id = $1 AND student_id = $2 AND ts >= $3 AND ts < $4 ORDER BY ts',
            [st.school_id, st.student_id, start, start + 86400000]);
        // ABSENT: ngayon lang, walang scan ang bata, lampas na sa oras ng absent, at may nag-scan na ibang estudyante
        // sa school ngayong araw (para hindi magkamali tuwing Sabado/Linggo o walang klase).
        let absent = false;
        const absentAfter = st.absent_after_min != null ? Number(st.absent_after_min) : 600;
        const nowMin = Math.floor((((Date.now() + TZ_SQL * 60000) % 86400000) + 86400000) % 86400000 / 60000);
        if (date === today && er.rows.length === 0 && nowMin >= absentAfter) {
            const act = await q('SELECT 1 FROM relay_school_events WHERE school_id = $1 AND ts >= $2 AND ts < $3 LIMIT 1', [st.school_id, start, start + 86400000]);
            absent = act.rows.length > 0;
        }
        res.json({
            success: true, student: { name: st.name, section: st.section }, schoolName: st.school_name,
            date, today, now: Date.now(), absent, absentAfterMin: absentAfter,
            events: er.rows.map((r) => ({ id: r.event_id, type: r.type, ts: Number(r.ts), late: r.late, station: r.station }))
        });
    }));

    // web viewer
    const viewer = (req, res) => { res.set('Cache-Control', 'no-cache'); res.sendFile(path.join(__dirname, 'public', 'school-viewer.html')); };
    app.get('/s', viewer);
    app.get('/s/:code', viewer);

    // ------------------------------------------------------------------ admin
    app.get('/relay/admin/api/schools', d.requireAdminKey, withSchema(async (req, res) => {
        const r = await q(`SELECT c.school_id, c.name, c.blocked, c.device_name, c.app_version, c.created_at, c.last_seen,
              (SELECT COUNT(*)::int FROM relay_school_students s WHERE s.school_id = c.school_id) AS students,
              (SELECT COUNT(*)::int FROM relay_school_events e WHERE e.school_id = c.school_id AND e.ts > $1) AS events_today
            FROM relay_schools c ORDER BY c.last_seen DESC NULLS LAST LIMIT 500`, [Date.parse(dayOf(Date.now()) + 'T00:00:00Z') - TZ_OFFSET_MIN * 60000]);
        res.json({ success: true, schools: r.rows.map((x) => ({ schoolId: x.school_id, name: x.name, blocked: x.blocked, device: x.device_name, appVersion: x.app_version, createdAt: x.created_at, lastSeen: x.last_seen, students: x.students, eventsToday: x.events_today })) });
    }));
    app.post('/relay/admin/api/schools/:id/block', d.requireAdminKey, withSchema(async (req, res) => {
        const blocked = !!(req.body && req.body.blocked);
        const r = await q('UPDATE relay_schools SET blocked = $2 WHERE school_id = $1', [clamp(req.params.id, 40), blocked]);
        schoolCache.clear();
        res.json({ success: r.rowCount > 0, blocked });
    }));
    app.delete('/relay/admin/api/schools/:id', d.requireAdminKey, withSchema(async (req, res) => {
        const id = clamp(req.params.id, 40);
        await q('DELETE FROM relay_school_events WHERE school_id = $1', [id]);
        await q('DELETE FROM relay_school_photos WHERE school_id = $1', [id]);
        await q('DELETE FROM relay_school_students WHERE school_id = $1', [id]);
        const r = await q('DELETE FROM relay_schools WHERE school_id = $1', [id]);
        schoolCache.clear();
        res.json({ success: r.rowCount > 0 });
    }));
    console.log('🏫 School attendance module loaded (/relay/school/*, /s).');
};
