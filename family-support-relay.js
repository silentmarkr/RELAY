'use strict';
/**
 * Developer Support + Sponsor for the Family Finance Tracker app (RELAY add-on).
 *
 * Mount it NEXT TO your existing family-relay.js, with the same dependency object `d` that school-relay.js gets
 * and the same auth middleware that family-relay.js already uses for per-device Bearer tokens:
 *
 *     require('./family-support-relay')(d, {
 *         familyAuth,                       // your existing Express middleware: 401 on a bad token, else sets req.familyIdentity (see below)
 *         identityOf: (req) => ({ familyId: req.family.id, familyName: req.family.name, deviceId: req.device.id })   // adapt to your names
 *     });
 *
 * Endpoints (all under /relay/family, same JSON shapes the app already speaks):
 *   GET  /info                       public  -> { supportOpen, supportMessage, sponsor: { methods, minPHP, onlineMinPHP, maxPHP } }
 *   POST /support-tickets            auth    -> { ticket: { id, subject, message, contact, createdAt, diagnostics } }
 *   GET  /support-tickets/status     auth    -> { supportOpen, tickets: [{ localTicketId, status, adminNote, updatedAt }] }
 *   POST /sponsor/create             auth    -> { amount, method } -> { purchaseId, checkoutUrl, qrCodeImageUrl, expiresAt }
 *   GET  /sponsor/status?purchaseId  auth    -> { status, amountPHP, method }
 *
 * It reuses what school-relay.js already uses (no new tables, no new npm packages):
 *   relay_support_tickets (shows up in admin/tickets.html with a [Family] mark),
 *   cloud_token_purchases (shows up in the sponsor donations list), and d.createPaymentCheckout (PayMongo etc.).
 * Required members of `d` (all of them already exist because school-relay.js uses them):
 *   app, rateLimit, queryWithRetry, ticketPool, purchasePool, getSupportDesk, refreshPaymongoCapabilities,
 *   getAvailablePaymentMethods, isPaymentMethodAvailable, createPaymentCheckout, parseSponsorAmount,
 *   backfillPaidOnlineDonations, SPONSOR_MIN_PHP, SPONSOR_ONLINE_MIN_PHP, SPONSOR_MAX_PHP,
 *   and optionally logActivity, sendSlackNotification, sendTelegramNotification.
 */
const crypto = require('crypto');

const clamp = (v, n) => String(v === undefined || v === null ? '' : v).trim().slice(0, n);
const MAX_DIAG_KEYS = 40;

module.exports = function mountFamilySupport(d, opts) {
    const { app } = d;
    const familyAuth = opts && opts.familyAuth;
    const identityOf = opts && opts.identityOf;
    if (typeof familyAuth !== 'function' || typeof identityOf !== 'function') {
        throw new Error('family-support-relay: pass { familyAuth, identityOf } (see the comment at the top of this file).');
    }
    const P = '/relay/family';
    const idKey = (req) => clamp((identityOf(req) || {}).familyId, 60) || req.ip; // rate-limit bucket per family

    /** Installation id used by the tickets table: the family id, prefixed so it can never clash with a school id. */
    const installationId = (req) => 'FAM-' + clamp((identityOf(req) || {}).familyId, 60);

    const safe = (fn) => async (req, res) => {
        try { await fn(req, res); }
        catch (err) {
            console.error('⚠️ Family support error:', err && err.message);
            if (!res.headersSent) res.status(503).json({ success: false, message: 'Family support temporarily unavailable.' });
        }
    };

    // ---------------------------------------------------------------- info (public)
    app.get(P + '/info', d.rateLimit('family-info', 120, 10 * 60 * 1000), async (req, res) => {
        const desk = d.getSupportDesk() || {};
        let methods = [];
        try {
            await d.refreshPaymongoCapabilities(false);
            methods = d.getAvailablePaymentMethods().map((m) => ({ id: m.id, label: m.label }));
        } catch (_) { /* no payment provider configured: the app shows "no payment methods" */ }
        res.json({
            success: true, serverTime: Date.now(),
            supportOpen: !!desk.open, supportMessage: desk.message || '',
            sponsor: { methods, minPHP: d.SPONSOR_MIN_PHP, onlineMinPHP: d.SPONSOR_ONLINE_MIN_PHP, maxPHP: d.SPONSOR_MAX_PHP }
        });
    });

    // ---------------------------------------------------------------- support tickets
    app.post(P + '/support-tickets', familyAuth, d.rateLimit('family-ticket', 30, 60 * 60 * 1000, idKey), safe(async (req, res) => {
        const desk = d.getSupportDesk() || {};
        if (!desk.open) {
            return res.status(503).json({ success: false, code: 'SUPPORT_DESK_CLOSED', message: desk.message || 'Support is closed. The ticket stays on the phone and is sent when support opens.' });
        }
        const tp = d.ticketPool();
        if (!tp) return res.status(503).json({ success: false, message: 'Devices/License database is not configured.' });
        const t = (req.body && typeof req.body.ticket === 'object' && req.body.ticket) || null;
        const localId = clamp(t && t.id, 80);
        const message = clamp(t && t.message, 4000);
        if (!t || !localId || !message) return res.status(400).json({ success: false, message: 'The ticket is incomplete.' });
        const subject = '[Family] ' + (clamp(t.subject, 140) || 'Support request');
        let diag = null;
        if (t.diagnostics && typeof t.diagnostics === 'object') {
            diag = {};
            Object.keys(t.diagnostics).slice(0, MAX_DIAG_KEYS).forEach((k) => { diag[clamp(k, 40)] = clamp(t.diagnostics[k], 200); });
        }
        let createdAt = new Date(Number(t.createdAt) || Date.now());
        if (Number.isNaN(createdAt.getTime())) createdAt = new Date();
        const ident = identityOf(req) || {};
        const inst = installationId(req);
        const storeName = '👨‍👩‍👧 ' + (clamp(ident.familyName, 140) || clamp(ident.familyId, 60));
        const ins = await d.queryWithRetry(tp,
            `INSERT INTO relay_support_tickets (installation_id, local_ticket_id, store_name, username, subject, message, transcript, diagnostics, created_at)
             VALUES ($1,$2,$3,$4,$5,$6,'[]'::jsonb,$7::jsonb,$8) ON CONFLICT (installation_id, local_ticket_id) DO NOTHING RETURNING id`,
            [inst, localId, storeName, clamp(t.contact, 120) || null, subject, message, diag ? JSON.stringify(diag) : null, createdAt.toISOString()]);
        const created = ins.rows.length > 0;
        if (created) {
            try { if (d.logActivity) d.logActivity(inst, 'support_ticket_received', { subject }); } catch (_) {}
            const text = `🎫 New Family support ticket\nFamily: ${clamp(ident.familyName, 80)} (${clamp(ident.familyId, 60)})\nFrom: ${clamp(t.contact, 120) || 'not given'}\nSubject: ${subject}`;
            try { if (d.sendSlackNotification) Promise.resolve(d.sendSlackNotification(text)).catch(() => {}); } catch (_) {}
            try { if (d.sendTelegramNotification) Promise.resolve(d.sendTelegramNotification(text)).catch(() => {}); } catch (_) {}
        }
        const cur = await d.queryWithRetry(tp, 'SELECT status, admin_note, updated_at FROM relay_support_tickets WHERE installation_id = $1 AND local_ticket_id = $2', [inst, localId]);
        const row = cur.rows[0] || { status: 'open', admin_note: '', updated_at: new Date() };
        res.json({ success: true, created, status: row.status, adminNote: row.admin_note || '', updatedAt: row.updated_at });
    }));

    app.get(P + '/support-tickets/status', familyAuth, d.rateLimit('family-ticket-status', 120, 60 * 60 * 1000, idKey), safe(async (req, res) => {
        const tp = d.ticketPool();
        if (!tp) return res.status(503).json({ success: false, message: 'Devices/License database is not configured.' });
        const r = await d.queryWithRetry(tp,
            'SELECT local_ticket_id, status, admin_note, updated_at FROM relay_support_tickets WHERE installation_id = $1 ORDER BY received_at DESC, id DESC LIMIT 50',
            [installationId(req)]);
        const desk = d.getSupportDesk() || {};
        res.json({ success: true, supportOpen: !!desk.open, tickets: r.rows.map((x) => ({ localTicketId: x.local_ticket_id, status: x.status, adminNote: x.admin_note || '', updatedAt: x.updated_at })) });
    }));

    // ---------------------------------------------------------------- sponsor
    app.post(P + '/sponsor/create', familyAuth, d.rateLimit('family-sponsor-create', 20, 60 * 60 * 1000, idKey), safe(async (req, res) => {
        const inst = installationId(req);
        const { method, returnBaseUrl } = req.body || {};
        const amount = d.parseSponsorAmount(req.body && req.body.amount);
        if (amount === null) return res.status(400).json({ success: false, message: `Enter a whole amount from ₱${d.SPONSOR_MIN_PHP} to ₱${d.SPONSOR_MAX_PHP}.` });
        if (amount < d.SPONSOR_ONLINE_MIN_PHP) return res.status(400).json({ success: false, message: `Online payment starts at ₱${d.SPONSOR_ONLINE_MIN_PHP}.` });
        const pp = d.purchasePool();
        if (!pp) return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured.' });
        await d.refreshPaymongoCapabilities(false);
        if (!d.isPaymentMethodAvailable(method)) {
            const available = d.getAvailablePaymentMethods().map((m) => m.label).join(', ') || 'none configured yet';
            return res.status(400).json({ success: false, message: `That payment method is not available. Available now: ${available}.` });
        }
        const purchaseId = `DON-${inst.slice(0, 12)}-${Date.now()}-${crypto.randomBytes(3).toString('hex')}`;
        const base = (returnBaseUrl && /^https?:\/\//.test(returnBaseUrl)) ? String(returnBaseUrl).replace(/\/$/, '') : `${req.protocol}://${req.get('host')}`;
        try {
            const checkout = await d.createPaymentCheckout({ method, amountPHP: amount, purchaseId, base, description: `Family Finance Tracker Sponsor Donation (PHP ${amount})` });
            await d.queryWithRetry(pp,
                `INSERT INTO cloud_token_purchases (purchase_id, installation_id, package_id, tokens, amount_php, method, source_id, provider, status)
                 VALUES ($1, $2, 'DONATION', 0, $3, $4, $5, $6, 'pending')`,
                [purchaseId, inst, amount, method, checkout.providerRefId, checkout.provider]);
            try { if (d.logActivity) d.logActivity(inst, 'sponsor_donation_created', { purchaseId, amountPHP: amount, method, provider: checkout.provider, app: 'family-finance' }); } catch (_) {}
            res.json({ success: true, purchaseId, checkoutUrl: checkout.checkoutUrl || null, qrCodeImageUrl: checkout.qrCodeImageUrl || null, expiresAt: checkout.expiresAt || null, amountPHP: amount });
        } catch (err) {
            const cfg = /_NOT_CONFIGURED$/.test(String(err.code || '')) || err.code === 'PROVIDER_NOT_CONFIGURED';
            res.status(cfg ? 503 : 502).json({ success: false, message: err.message });
        }
    }));

    app.get(P + '/sponsor/status', familyAuth, d.rateLimit('family-sponsor-status', 300, 60 * 60 * 1000, idKey), safe(async (req, res) => {
        const purchaseId = clamp(req.query.purchaseId, 120);
        if (!purchaseId.startsWith('DON-')) return res.status(400).json({ success: false, message: 'Invalid purchaseId.' });
        const pp = d.purchasePool();
        if (!pp) return res.status(503).json({ success: false, message: 'Postgres (DATABASE_URL) is not configured.' });
        // Only this family's own donations can be read: the installation id is part of the query.
        const r = await d.queryWithRetry(pp, 'SELECT status, amount_php, method FROM cloud_token_purchases WHERE purchase_id = $1 AND installation_id = $2', [purchaseId, installationId(req)]);
        if (!r.rows[0]) return res.status(404).json({ success: false, message: 'Donation not found.' });
        if (r.rows[0].status === 'paid') { try { await d.backfillPaidOnlineDonations(); } catch (_) {} }
        res.json({ success: true, status: r.rows[0].status, amountPHP: Number(r.rows[0].amount_php), method: r.rows[0].method });
    }));
};
