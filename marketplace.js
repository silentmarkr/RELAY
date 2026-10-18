// PATH: RELAY/marketplace.js
//
// Marketplace Hub (Shopee / Lazada / TikTok Shop) — RELAY side.
//
// BAKIT SA RELAY: ang partner_key / app_secret ng bawat platform ay HINDI dapat
// mapunta sa client (OMNIPOS) package. Dito rin dumadaan ang OAuth redirect
// (kailangan ng public URL) at dito naka-store (encrypted) ang access/refresh
// tokens. Ang OMNIPOS ay tumatawag lang sa /relay/marketplace/* gamit ang
// x-relay-key + installationId, at BAWAT tawag ay dumadaan sa subscription gate
// ('marketplace_hub').
//
// Env (RELAY):
//   SHOPEE_PARTNER_ID, SHOPEE_PARTNER_KEY, SHOPEE_ENV=live|test (default live)
//   LAZADA_APP_KEY, LAZADA_APP_SECRET
//   TIKTOK_APP_KEY, TIKTOK_APP_SECRET, TIKTOK_SERVICE_ID (optional)
//   MARKETPLACE_TOKEN_KEY   (encrypts stored tokens; REQUIRED in production)
//   MARKETPLACE_STATE_SECRET (optional; defaults to RELAY_API_KEY)
//   RELAY_PUBLIC_URL        (public https URL of this RELAY — used for redirect URIs)
// Test overrides: SHOPEE_BASE_URL, LAZADA_BASE_URL, LAZADA_AUTH_URL,
//   LAZADA_AUTH_REST_URL, TIKTOK_BASE_URL, TIKTOK_AUTH_API_URL
//
// NOTE: Ang mga endpoint path/field names sa ibaba ay ayon sa public docs ng
// bawat platform at DAPAT i-verify sa sandbox/test shop ng developer account
// bago i-release (tingnan ang MARKETPLACE_NOTES.md).

'use strict';
const crypto = require('crypto');

const PLATFORMS = ['shopee', 'lazada', 'tiktok'];
const FEATURE_ID = 'marketplace_hub';

// ----------------------------------------------------------------- utilities
const hmacHex = (key, str) => crypto.createHmac('sha256', key).update(str).digest('hex');
const b64url = (buf) => Buffer.from(buf).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
const fromB64url = (s) => Buffer.from(String(s).replace(/-/g, '+').replace(/_/g, '/'), 'base64');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const num = (v, d = 0) => { const n = Number(v); return Number.isFinite(n) ? n : d; };
const escHtml = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const escXml = escHtml;

class MarketplaceError extends Error {
    constructor(message, opts = {}) {
        super(message);
        this.name = 'MarketplaceError';
        this.code = opts.code || 'MARKETPLACE_ERROR';
        this.authExpired = !!opts.authExpired;   // access token rejected -> refresh then retry once
        this.reauthRequired = !!opts.reauthRequired; // refresh token dead -> seller must reconnect
        this.status = opts.status || 502;
    }
}

async function http(url, { method = 'GET', headers = {}, body, timeoutMs = 25000, raw = false } = {}) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
        const res = await fetch(url, { method, headers, body, signal: ctrl.signal });
        if (raw) return { status: res.status, headers: res.headers, buffer: Buffer.from(await res.arrayBuffer()) };
        const text = await res.text();
        let json = null;
        try { json = JSON.parse(text); } catch (_) { /* non-JSON */ }
        return { status: res.status, json, text, headers: res.headers };
    } catch (err) {
        throw new MarketplaceError(err.name === 'AbortError' ? 'The marketplace did not respond in time.' : `Could not reach the marketplace: ${err.message}`, { code: 'NETWORK' });
    } finally {
        clearTimeout(timer);
    }
}

// Only fetch label files from https URLs handed back by the platform APIs.
async function fetchHttpsFile(url) {
    let u;
    try { u = new URL(url); } catch (_) { throw new MarketplaceError('The marketplace returned an invalid label URL.'); }
    if (u.protocol !== 'https:') throw new MarketplaceError('The marketplace returned a non-HTTPS label URL.');
    const r = await http(u.toString(), { raw: true, timeoutMs: 40000 });
    if (r.status >= 400) throw new MarketplaceError(`Label download failed (HTTP ${r.status}).`);
    return r.buffer;
}

function sniffMime(buf) {
    const head = buf.slice(0, 8).toString('latin1');
    if (head.startsWith('%PDF')) return 'application/pdf';
    if (head.startsWith('PK')) return 'application/zip';
    return 'text/html';
}

// ------------------------------------------------------------ token encryption
function keyFromSecret(secret) {
    return crypto.createHash('sha256').update(String(secret)).digest();
}
// SECURITY FIX: dati ay may hardcoded na fallback na 'omnipos-dev-only-key' — kapag nakalimutang i-set ang env sa production,
// lahat ng seller token ay naka-encrypt gamit ang key na nasa source code. Ngayon, sa production ay REQUIRED ang
// MARKETPLACE_TOKEN_KEY (o RELAY_API_KEY bilang huling pagpipilian, may babala); ang dev key ay para lang sa non-production.
// KEY ROTATION: ang MARKETPLACE_TOKEN_KEY_PREVIOUS (comma-separated) ay sinusubukan lang sa pag-decrypt, para hindi mawala ang lahat
// ng naka-save na token kapag nagpalit ng key o ng RELAY_API_KEY.
let tokenKeyWarned = false;
// "Production-like": NODE_ENV=production, O may DATABASE_URL (ibig sabihin totoong deployment ito at hindi dev laptop). Kung hindi
// naka-set ang NODE_ENV sa host, nawawalan ng silbi ang production check — kaya idinagdag ang DATABASE_URL signal.
// MARKETPLACE_ALLOW_DEV_KEY=1 lang ang paraan para pilitin ang dev key sa ganitong deployment (huwag gamitin sa live).
function isProductionLike() {
    if (String(process.env.NODE_ENV || '').toLowerCase() === 'production') return true;
    return !!process.env.DATABASE_URL && String(process.env.MARKETPLACE_ALLOW_DEV_KEY || '') !== '1';
}
const LEGACY_DEV_TOKEN_SECRET = 'omnipos-dev-only-key';
function tokenKeys() {
    const primary = process.env.MARKETPLACE_TOKEN_KEY || process.env.RELAY_API_KEY;
    if (!primary) {
        // Malinaw na 503 (hindi generic na 500) para alam ng admin kung ano ang kulang.
        if (isProductionLike()) throw new MarketplaceError('Marketplace token encryption is not configured on the server (set MARKETPLACE_TOKEN_KEY).', { code: 'NOT_CONFIGURED', status: 503 });
        if (!tokenKeyWarned) { tokenKeyWarned = true; console.warn('⚠️ Marketplace: walang MARKETPLACE_TOKEN_KEY — dev-only key ang gamit (HUWAG sa production).'); }
    } else if (!process.env.MARKETPLACE_TOKEN_KEY && !tokenKeyWarned) {
        tokenKeyWarned = true;
        console.warn('⚠️ Marketplace: walang MARKETPLACE_TOKEN_KEY — RELAY_API_KEY ang gamit na pang-encrypt. Mawawala ang access sa mga token kapag nagpalit ng RELAY_API_KEY. Mag-set ng hiwalay na MARKETPLACE_TOKEN_KEY.');
    }
    const secrets = [primary || LEGACY_DEV_TOKEN_SECRET];
    String(process.env.MARKETPLACE_TOKEN_KEY_PREVIOUS || '').split(',').map((x) => x.trim()).filter(Boolean).forEach((x) => secrets.push(x));
    if (process.env.MARKETPLACE_TOKEN_KEY && process.env.RELAY_API_KEY) secrets.push(process.env.RELAY_API_KEY);
    // MIGRATION: ang mga token na na-encrypt ng lumang bersyon na walang naka-set na key ay gumamit ng 'omnipos-dev-only-key'. Para hindi
    // sila mawala (at kailanganing mag-reconnect ang lahat ng seller) kapag nag-set na ng tamang key, sinusubukan din ito PERO PARA SA
    // PAG-DECRYPT LAMANG — hindi ito kailanman ginagamit na pang-encrypt, at ang token ay awtomatikong ire-re-encrypt gamit ang bagong key
    // pagkatapos ng unang matagumpay na paggamit. I-disable gamit ang MARKETPLACE_DISABLE_LEGACY_DEV_KEY=1 kapag wala nang lumang token.
    if (String(process.env.MARKETPLACE_DISABLE_LEGACY_DEV_KEY || '') !== '1') secrets.push(LEGACY_DEV_TOKEN_SECRET);
    const seen = new Set();
    const keys = [];
    for (const secret of secrets) {
        const key = keyFromSecret(secret);
        const id = key.toString('hex');
        if (seen.has(id)) continue;
        seen.add(id);
        keys.push(key);
    }
    return keys;
}
function encryptSecret(plain) {
    if (!plain) return null;
    const iv = crypto.randomBytes(12);
    const c = crypto.createCipheriv('aes-256-gcm', tokenKeys()[0], iv);
    const ct = Buffer.concat([c.update(String(plain), 'utf8'), c.final()]);
    return Buffer.concat([iv, c.getAuthTag(), ct]).toString('base64');
}
// keyIndex 0 = pangunahing key; > 0 = lumang key (kailangan i-re-encrypt gamit ang bagong key).
function decryptSecretMeta(blob) {
    if (!blob) return { value: null, keyIndex: 0 };
    const raw = Buffer.from(String(blob), 'base64');
    let lastErr = null;
    const keys = tokenKeys();
    for (let i = 0; i < keys.length; i++) {
        try {
            const d = crypto.createDecipheriv('aes-256-gcm', keys[i], raw.slice(0, 12));
            d.setAuthTag(raw.slice(12, 28));
            return { value: Buffer.concat([d.update(raw.slice(28)), d.final()]).toString('utf8'), keyIndex: i };
        } catch (e) { lastErr = e; }
    }
    throw lastErr || new Error('Cannot decrypt stored marketplace secret.');
}
function decryptSecret(blob) {
    return decryptSecretMeta(blob).value;
}

// --------------------------------------------------------------- status maps
// Normalized statuses: unpaid | to_ship | shipped | completed | cancelled | returned
const SHOPEE_STATUS = {
    UNPAID: 'unpaid', PENDING: 'to_ship', READY_TO_SHIP: 'to_ship', PROCESSED: 'to_ship', IN_CANCEL: 'to_ship',
    SHIPPED: 'shipped', TO_CONFIRM_RECEIVE: 'shipped', COMPLETED: 'completed',
    CANCELLED: 'cancelled', TO_RETURN: 'returned'
};
const LAZADA_STATUS = {
    unpaid: 'unpaid', pending: 'to_ship', topack: 'to_ship', packed: 'to_ship', ready_to_ship: 'to_ship', toship: 'to_ship',
    shipped: 'shipped', shipping: 'shipped', delivered: 'completed',
    canceled: 'cancelled', cancelled: 'cancelled', failed: 'cancelled', returned: 'returned', lost: 'returned'
};
const TIKTOK_STATUS = {
    UNPAID: 'unpaid', ON_HOLD: 'to_ship', AWAITING_SHIPMENT: 'to_ship', PARTIALLY_SHIPPING: 'to_ship',
    AWAITING_COLLECTION: 'shipped', IN_TRANSIT: 'shipped', DELIVERED: 'completed', COMPLETED: 'completed', CANCELLED: 'cancelled'
};

// ============================================================ SHOPEE ADAPTER
const shopee = {
    id: 'shopee',
    // BUGFIX: dating `label: 'Shopee'` — nabubura ito ng `async label(...)` (shipping label) sa parehong object, kaya function source ang lumalabas sa mga mensahe.
    title: 'Shopee',
    cfg() {
        return {
            partnerId: process.env.SHOPEE_PARTNER_ID || '',
            partnerKey: process.env.SHOPEE_PARTNER_KEY || '',
            base: process.env.SHOPEE_BASE_URL || (process.env.SHOPEE_ENV === 'test' ? 'https://partner.test-stable.shopeemobile.com' : 'https://partner.shopeemobile.com')
        };
    },
    configured() { const c = this.cfg(); return !!(c.partnerId && c.partnerKey); },
    sign(path, ts, accessToken = '', shopId = '') {
        const c = this.cfg();
        return hmacHex(c.partnerKey, `${c.partnerId}${path}${ts}${accessToken}${shopId}`);
    },
    authUrl({ redirectUri, state }) {
        const c = this.cfg();
        const path = '/api/v2/shop/auth_partner';
        const ts = Math.floor(Date.now() / 1000);
        // Shopee does not echo a state param, so it rides inside the redirect URL itself.
        const redirect = `${redirectUri}?state=${encodeURIComponent(state)}`;
        const qs = new URLSearchParams({ partner_id: c.partnerId, timestamp: String(ts), sign: this.sign(path, ts), redirect });
        return `${c.base}${path}?${qs}`;
    },
    async tokenCall(path, body) {
        const c = this.cfg();
        const ts = Math.floor(Date.now() / 1000);
        const qs = new URLSearchParams({ partner_id: c.partnerId, timestamp: String(ts), sign: this.sign(path, ts) });
        const r = await http(`${c.base}${path}?${qs}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...body, partner_id: num(c.partnerId) }) });
        const j = r.json || {};
        if (j.error || !j.access_token) {
            throw new MarketplaceError(`Shopee auth failed: ${j.message || j.error || `HTTP ${r.status}`}`, { code: 'AUTH', reauthRequired: true, status: 401 });
        }
        return j;
    },
    toConn(j, shopId) {
        const now = Date.now();
        return {
            shopId: String(shopId), accessToken: j.access_token, refreshToken: j.refresh_token,
            accessExpiresAt: now + num(j.expire_in, 14400) * 1000, refreshExpiresAt: now + 30 * 24 * 3600 * 1000, extra: {}
        };
    },
    async exchange({ query }) {
        const shopId = query.shop_id || query.main_account_id;
        if (!query.code || !shopId) throw new MarketplaceError('Shopee did not return a code/shop_id.', { code: 'AUTH', status: 400 });
        const j = await this.tokenCall('/api/v2/auth/token/get', { code: String(query.code), shop_id: num(shopId) });
        return this.toConn(j, shopId);
    },
    async refresh(conn) {
        const j = await this.tokenCall('/api/v2/auth/access_token/get', { refresh_token: conn.refreshToken, shop_id: num(conn.shopId) });
        return this.toConn(j, conn.shopId);
    },
    async call(conn, method, path, { query = {}, body, raw = false } = {}) {
        const c = this.cfg();
        const ts = Math.floor(Date.now() / 1000);
        const qs = new URLSearchParams({
            partner_id: c.partnerId, timestamp: String(ts), access_token: conn.accessToken, shop_id: conn.shopId,
            sign: this.sign(path, ts, conn.accessToken, conn.shopId)
        });
        Object.entries(query).forEach(([k, v]) => { if (v !== undefined && v !== null && v !== '') qs.set(k, String(v)); });
        const r = await http(`${c.base}${path}?${qs}`, {
            method, raw,
            headers: body ? { 'Content-Type': 'application/json' } : {},
            body: body ? JSON.stringify(body) : undefined
        });
        if (raw) {
            if (sniffMime(r.buffer) === 'text/html' && r.buffer.slice(0, 1).toString() === '{') {
                let j = null; try { j = JSON.parse(r.buffer.toString('utf8')); } catch (_) { /* ignore */ }
                if (j && j.error) throw new MarketplaceError(`Shopee: ${j.message || j.error}`);
            }
            return r.buffer;
        }
        const j = r.json || {};
        if (j.error) {
            const authExpired = /access_token|auth|permission/i.test(`${j.error} ${j.message}`) && /expire|invalid|token/i.test(`${j.error} ${j.message}`);
            throw new MarketplaceError(`Shopee: ${j.message || j.error}`, { code: j.error, authExpired });
        }
        return j.response !== undefined ? j.response : j;
    },
    async describe(conn) {
        const r = await this.call(conn, 'GET', '/api/v2/shop/get_shop_info');
        return { shopName: r.shop_name || `Shopee ${conn.shopId}`, extra: { region: r.region || 'PH' } };
    },
    normOrder(conn, o) {
        const items = (o.item_list || []).map((it) => ({
            itemId: String(it.item_id), modelId: String(it.model_id || 0),
            sku: String(it.model_sku || it.item_sku || ''), name: it.item_name || '', variation: it.model_name || '',
            qty: num(it.model_quantity_purchased, 1), unitPrice: num(it.model_discounted_price || it.model_original_price)
        }));
        const pkg = (o.package_list || [])[0] || {};
        return {
            platform: 'shopee', shopId: conn.shopId, orderId: String(o.order_sn), orderNo: String(o.order_sn),
            status: SHOPEE_STATUS[o.order_status] || 'to_ship', rawStatus: o.order_status,
            cancelRequested: o.order_status === 'IN_CANCEL',
            createdAt: num(o.create_time) * 1000, updatedAt: num(o.update_time) * 1000,
            buyer: o.buyer_username || '', total: num(o.total_amount), shippingFee: num(o.actual_shipping_fee), currency: o.currency || 'PHP',
            paymentMethod: o.payment_method || '', carrier: pkg.shipping_carrier || o.shipping_carrier || '', trackingNumber: o.tracking_no || '',
            items, meta: { packageNumber: pkg.package_number || '' }
        };
    },
    async listOrders(conn, { sinceMs }) {
        const now = Math.floor(Date.now() / 1000);
        const from = Math.max(Math.floor((sinceMs || 0) / 1000), now - 14 * 24 * 3600);
        const sns = [];
        let cursor = '';
        for (let page = 0; page < 20; page++) {
            const r = await this.call(conn, 'GET', '/api/v2/order/get_order_list', {
                query: { time_range_field: 'update_time', time_from: from, time_to: now, page_size: 100, cursor }
            });
            (r.order_list || []).forEach((o) => sns.push(o.order_sn));
            if (!r.more || !r.next_cursor) break;
            cursor = r.next_cursor;
        }
        const orders = [];
        for (let i = 0; i < sns.length; i += 50) {
            const r = await this.call(conn, 'GET', '/api/v2/order/get_order_detail', {
                query: {
                    order_sn_list: sns.slice(i, i + 50).join(','),
                    response_optional_fields: 'item_list,total_amount,payment_method,package_list,buyer_username,actual_shipping_fee,shipping_carrier'
                }
            });
            (r.order_list || []).forEach((o) => orders.push(this.normOrder(conn, o)));
        }
        return orders;
    },
    async listProducts(conn, { cursor }) {
        const offset = num(cursor, 0);
        const r = await this.call(conn, 'GET', '/api/v2/product/get_item_list', { query: { offset, page_size: 50, item_status: 'NORMAL' } });
        const ids = (r.item || []).map((i) => i.item_id);
        const rows = [];
        if (ids.length) {
            const base = await this.call(conn, 'GET', '/api/v2/product/get_item_base_info', { query: { item_id_list: ids.join(',') } });
            for (const it of (base.item_list || [])) {
                if (it.has_model) {
                    const m = await this.call(conn, 'GET', '/api/v2/product/get_model_list', { query: { item_id: it.item_id } });
                    for (const mod of (m.model || [])) {
                        rows.push({
                            platform: 'shopee', shopId: conn.shopId, itemId: String(it.item_id), modelId: String(mod.model_id),
                            sku: mod.model_sku || it.item_sku || '', name: it.item_name, variation: mod.model_name || '',
                            stock: num(mod.stock_info_v2 && mod.stock_info_v2.summary_info && mod.stock_info_v2.summary_info.total_available_stock),
                            price: num(mod.price_info && mod.price_info[0] && (mod.price_info[0].original_price || mod.price_info[0].current_price)), meta: {}
                        });
                    }
                } else {
                    rows.push({
                        platform: 'shopee', shopId: conn.shopId, itemId: String(it.item_id), modelId: '0',
                        sku: it.item_sku || '', name: it.item_name, variation: '',
                        stock: num(it.stock_info_v2 && it.stock_info_v2.summary_info && it.stock_info_v2.summary_info.total_available_stock),
                        price: num(it.price_info && it.price_info[0] && (it.price_info[0].original_price || it.price_info[0].current_price)), meta: {}
                    });
                }
            }
        }
        return { items: rows, nextCursor: r.has_next_page ? String(r.next_offset) : null };
    },
    async pushStock(conn, updates) {
        const results = [];
        for (const u of updates) {
            try {
                await this.call(conn, 'POST', '/api/v2/product/update_stock', {
                    body: { item_id: num(u.itemId), stock_list: [{ model_id: num(u.modelId, 0), seller_stock: [{ stock: Math.max(0, Math.floor(num(u.stock))) }] }] }
                });
                results.push({ itemId: u.itemId, modelId: u.modelId, ok: true });
            } catch (e) { if (e.authExpired) throw e; results.push({ itemId: u.itemId, modelId: u.modelId, ok: false, error: e.message }); }
        }
        return results;
    },
    async pushPrice(conn, updates) {
        const results = [];
        for (const u of updates) {
            try {
                await this.call(conn, 'POST', '/api/v2/product/update_price', {
                    body: { item_id: num(u.itemId), price_list: [{ model_id: num(u.modelId, 0), original_price: Math.round(num(u.price) * 100) / 100 }] }
                });
                results.push({ itemId: u.itemId, modelId: u.modelId, ok: true });
            } catch (e) { if (e.authExpired) throw e; results.push({ itemId: u.itemId, modelId: u.modelId, ok: false, error: e.message }); }
        }
        return results;
    },
    async label(conn, { orderId, arrange, meta = {} }) {
        let packageNumber = meta.packageNumber || '';
        if (!packageNumber) {
            const d = await this.call(conn, 'GET', '/api/v2/order/get_order_detail', { query: { order_sn_list: orderId, response_optional_fields: 'package_list' } });
            packageNumber = (((d.order_list || [])[0] || {}).package_list || [])[0]?.package_number || '';
        }
        if (arrange) {
            const p = await this.call(conn, 'GET', '/api/v2/logistics/get_shipping_parameter', { query: { order_sn: orderId } });
            const needed = p.info_needed || {};
            let body;
            if (needed.dropoff) body = { order_sn: orderId, dropoff: {} };
            else if (needed.pickup) {
                const addr = ((p.pickup || {}).address_list || [])[0] || {};
                const slot = (addr.time_slot_list || [])[0] || {};
                body = { order_sn: orderId, pickup: { address_id: addr.address_id, pickup_time_id: slot.pickup_time_id } };
            } else body = { order_sn: orderId };
            if (packageNumber) body.package_number = packageNumber;
            await this.call(conn, 'POST', '/api/v2/logistics/ship_order', { body });
        }
        const tq = { order_sn: orderId };
        if (packageNumber) tq.package_number = packageNumber;
        const t = await this.call(conn, 'GET', '/api/v2/logistics/get_tracking_number', { query: tq });
        const tracking = t.tracking_number || '';
        const ref = { order_sn: orderId };
        if (packageNumber) ref.package_number = packageNumber;
        const pr = await this.call(conn, 'POST', '/api/v2/logistics/get_shipping_document_parameter', { body: { order_list: [ref] } });
        const docType = (((pr.result_list || [])[0] || {}).suggest_shipping_document_type) || 'NORMAL_AIR_WAYBILL';
        const entry = { ...ref, shipping_document_type: docType };
        if (tracking) entry.tracking_number = tracking;
        await this.call(conn, 'POST', '/api/v2/logistics/create_shipping_document', { body: { order_list: [entry] } });
        let ready = false;
        for (let i = 0; i < 10 && !ready; i++) {
            const s = await this.call(conn, 'POST', '/api/v2/logistics/get_shipping_document_result', { body: { order_list: [{ ...ref, shipping_document_type: docType }] } });
            const st = (((s.result_list || [])[0]) || {}).status;
            if (st === 'READY') ready = true;
            else if (st === 'FAILED') throw new MarketplaceError('Shopee could not generate the shipping document yet.');
            else await sleep(1500);
        }
        if (!ready) throw new MarketplaceError('The Shopee shipping document is still processing. Please try again in a moment.', { code: 'LABEL_PENDING', status: 202 });
        const buf = await this.call(conn, 'POST', '/api/v2/logistics/download_shipping_document', {
            body: { shipping_document_type: docType, order_list: [ref] }, raw: true
        });
        return { buffer: buf, mimeType: sniffMime(buf), trackingNumber: tracking, carrier: '' };
    }
};

// ============================================================ LAZADA ADAPTER
const lazada = {
    id: 'lazada',
    // BUGFIX: dating `label: 'Lazada'` — nabubura ito ng `async label(...)` (shipping label) sa parehong object, kaya function source ang lumalabas sa mga mensahe.
    title: 'Lazada',
    cfg() {
        return {
            appKey: process.env.LAZADA_APP_KEY || '',
            appSecret: process.env.LAZADA_APP_SECRET || '',
            base: process.env.LAZADA_BASE_URL || 'https://api.lazada.com.ph/rest',
            authUrl: process.env.LAZADA_AUTH_URL || 'https://auth.lazada.com/oauth/authorize',
            authRest: process.env.LAZADA_AUTH_REST_URL || 'https://auth.lazada.com/rest'
        };
    },
    configured() { const c = this.cfg(); return !!(c.appKey && c.appSecret); },
    sign(apiPath, params) {
        const c = this.cfg();
        const base = apiPath + Object.keys(params).filter((k) => k !== 'sign' && params[k] !== undefined && params[k] !== null).sort().map((k) => k + params[k]).join('');
        return hmacHex(c.appSecret, base).toUpperCase();
    },
    authUrl({ redirectUri, state }) {
        const c = this.cfg();
        const qs = new URLSearchParams({ response_type: 'code', force_auth: 'true', redirect_uri: redirectUri, client_id: c.appKey, state });
        return `${c.authUrl}?${qs}`;
    },
    async request(host, apiPath, params, { method = 'GET' } = {}) {
        const c = this.cfg();
        const p = { app_key: c.appKey, timestamp: String(Date.now()), sign_method: 'sha256', ...params };
        p.sign = this.sign(apiPath, p);
        const qs = new URLSearchParams(p).toString();
        const r = method === 'GET'
            ? await http(`${host}${apiPath}?${qs}`)
            : await http(`${host}${apiPath}`, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8' }, body: qs });
        return r.json || {};
    },
    toConn(j) {
        const now = Date.now();
        const info = (j.country_user_info || []).find((x) => String(x.country).toLowerCase() === 'ph') || (j.country_user_info || [])[0] || {};
        return {
            shopId: String(info.seller_id || j.account_id || j.account || ''), accessToken: j.access_token, refreshToken: j.refresh_token,
            accessExpiresAt: now + num(j.expires_in, 604800) * 1000, refreshExpiresAt: now + num(j.refresh_expires_in, 2592000) * 1000,
            extra: { account: j.account || '', country: info.country || 'PH' }
        };
    },
    async exchange({ query }) {
        if (!query.code) throw new MarketplaceError('Lazada did not return an authorization code.', { code: 'AUTH', status: 400 });
        const c = this.cfg();
        const j = await this.request(c.authRest, '/auth/token/create', { code: String(query.code) });
        if (!j.access_token) throw new MarketplaceError(`Lazada auth failed: ${j.message || j.code || 'no token returned'}`, { code: 'AUTH', reauthRequired: true, status: 401 });
        return this.toConn(j);
    },
    async refresh(conn) {
        const c = this.cfg();
        const j = await this.request(c.authRest, '/auth/token/refresh', { refresh_token: conn.refreshToken });
        if (!j.access_token) throw new MarketplaceError(`Lazada token refresh failed: ${j.message || j.code || 'no token returned'}`, { code: 'AUTH', reauthRequired: true, status: 401 });
        const next = this.toConn(j);
        if (!next.shopId) next.shopId = conn.shopId;
        return next;
    },
    async call(conn, apiPath, params = {}, method = 'GET') {
        const c = this.cfg();
        const j = await this.request(c.base, apiPath, { access_token: conn.accessToken, ...params }, { method });
        if (j.code && String(j.code) !== '0') {
            const authExpired = /^(IllegalAccessToken|AccessTokenExpired|ACCESS_TOKEN)/i.test(String(j.code)) || /access.?token/i.test(String(j.message || ''));
            throw new MarketplaceError(`Lazada: ${j.message || j.code}`, { code: String(j.code), authExpired });
        }
        return j;
    },
    async describe(conn) {
        try {
            const j = await this.call(conn, '/seller/get');
            const d = j.data || {};
            return { shopName: d.name || d.name_company || conn.extra.account || `Lazada ${conn.shopId}`, extra: {} };
        } catch (_) { return { shopName: conn.extra.account || `Lazada ${conn.shopId}`, extra: {} }; }
    },
    deriveStatus(statuses) {
        const list = (statuses || []).map((s) => LAZADA_STATUS[String(s).toLowerCase()] || null).filter(Boolean);
        if (!list.length) return 'to_ship';
        const rank = ['returned', 'unpaid', 'to_ship', 'shipped', 'completed', 'cancelled'];
        // an order with ANY live (non-cancelled) line takes the earliest live stage
        const live = list.filter((s) => s !== 'cancelled');
        if (!live.length) return 'cancelled';
        return live.sort((a, b) => rank.indexOf(a) - rank.indexOf(b))[0];
    },
    async listOrders(conn, { sinceMs }) {
        const after = new Date(Math.max(sinceMs || 0, Date.now() - 30 * 24 * 3600 * 1000) + 8 * 3600 * 1000).toISOString().replace(/\.\d{3}Z$/, '+08:00'); // PH time, walang milliseconds
        const heads = [];
        for (let offset = 0; offset < 2000; offset += 100) {
            const j = await this.call(conn, '/orders/get', { update_after: after, sort_by: 'updated_at', sort_direction: 'ASC', offset: String(offset), limit: '100' });
            const list = (j.data && j.data.orders) || [];
            heads.push(...list);
            if (list.length < 100) break;
        }
        const orders = [];
        for (let i = 0; i < heads.length; i += 50) {
            const chunk = heads.slice(i, i + 50);
            const j = await this.call(conn, '/orders/items/get', { order_ids: JSON.stringify(chunk.map((h) => Number(h.order_id))) });
            const byId = {};
            (j.data || []).forEach((row) => { byId[String(row.order_id)] = row.order_items || []; });
            for (const h of chunk) {
                const lines = byId[String(h.order_id)] || [];
                const live = lines.filter((l) => !/^(canceled|cancelled|failed)$/i.test(String(l.status || '')));
                const grouped = {};
                live.forEach((l) => {
                    const key = `${l.product_id}:${l.sku_id}:${l.sku}`;
                    if (!grouped[key]) grouped[key] = { itemId: String(l.product_id || ''), modelId: String(l.sku_id || ''), sku: String(l.sku || l.shop_sku || ''), name: l.name || '', variation: l.variation || '', qty: 0, unitPrice: num(l.paid_price || l.item_price), orderItemIds: [] };
                    grouped[key].qty += 1;
                    grouped[key].orderItemIds.push(String(l.order_item_id));
                });
                const statuses = [].concat(h.statuses || []).concat(lines.map((l) => l.status));
                const first = lines[0] || {};
                orders.push({
                    platform: 'lazada', shopId: conn.shopId, orderId: String(h.order_id), orderNo: String(h.order_number || h.order_id),
                    status: live.length ? this.deriveStatus(live.map((l) => l.status).concat(h.statuses || [])) : 'cancelled', rawStatus: [].concat(h.statuses || []).join(','),
                    cancelRequested: false,
                    createdAt: Date.parse(h.created_at) || 0, updatedAt: Date.parse(h.updated_at) || 0,
                    buyer: [h.customer_first_name, h.customer_last_name].filter(Boolean).join(' ').trim(),
                    total: num(h.price), shippingFee: num(h.shipping_fee), currency: 'PHP', paymentMethod: h.payment_method || '',
                    carrier: first.shipment_provider || '', trackingNumber: first.tracking_code || '',
                    items: Object.values(grouped).map(({ orderItemIds, ...rest }) => ({ ...rest, meta: { orderItemIds } })),
                    meta: { orderItemIds: live.map((l) => String(l.order_item_id)), statuses }
                });
            }
        }
        return orders;
    },
    async listProducts(conn, { cursor }) {
        const offset = num(cursor, 0);
        const j = await this.call(conn, '/products/get', { filter: 'all', limit: '50', offset: String(offset) });
        const d = j.data || {};
        const rows = [];
        for (const p of (d.products || [])) {
            const name = (p.attributes && p.attributes.name) || '';
            for (const s of (p.skus || [])) {
                rows.push({
                    platform: 'lazada', shopId: conn.shopId, itemId: String(p.item_id), modelId: String(s.SkuId || ''),
                    sku: s.SellerSku || '', name, variation: '', stock: num(s.quantity), price: num(s.price),
                    meta: { shopSku: s.ShopSku || '' }
                });
            }
        }
        const total = num(d.total_products);
        const next = offset + 50;
        return { items: rows, nextCursor: next < total ? String(next) : null };
    },
    skuXml(u, withStock, withPrice) {
        let x = `<Sku><ItemId>${escXml(u.itemId)}</ItemId><SkuId>${escXml(u.modelId)}</SkuId><SellerSku>${escXml(u.sku || '')}</SellerSku>`;
        if (withStock) x += `<Quantity>${Math.max(0, Math.floor(num(u.stock)))}</Quantity>`;
        if (withPrice) x += `<Price>${(Math.round(num(u.price) * 100) / 100).toFixed(2)}</Price>`;
        return x + '</Sku>';
    },
    async pushBatch(conn, updates, withStock, withPrice) {
        const results = [];
        for (let i = 0; i < updates.length; i += 20) {
            const chunk = updates.slice(i, i + 20);
            const payload = `<Request><Product><Skus>${chunk.map((u) => this.skuXml(u, withStock, withPrice)).join('')}</Skus></Product></Request>`;
            try {
                await this.call(conn, '/product/price_quantity/update', { payload }, 'POST');
                chunk.forEach((u) => results.push({ itemId: u.itemId, modelId: u.modelId, ok: true }));
            } catch (e) {
                if (e.authExpired) throw e;
                chunk.forEach((u) => results.push({ itemId: u.itemId, modelId: u.modelId, ok: false, error: e.message }));
            }
        }
        return results;
    },
    pushStock(conn, updates) { return this.pushBatch(conn, updates, true, false); },
    pushPrice(conn, updates) { return this.pushBatch(conn, updates, false, true); },
    async label(conn, { orderId, arrange, meta = {} }) {
        let ids = (meta.orderItemIds || []).map(String);
        if (!ids.length) {
            const j = await this.call(conn, '/order/items/get', { order_id: String(orderId) });
            ids = (j.data || []).filter((l) => !/^(canceled|cancelled|failed)$/i.test(String(l.status))).map((l) => String(l.order_item_id));
        }
        if (!ids.length) throw new MarketplaceError('This Lazada order has no active items to print a label for.');
        if (arrange) {
            const orderItemIds = JSON.stringify(ids.map(Number));
            const pv = await this.call(conn, '/order/shipment/providers/get', { orders: JSON.stringify([{ order_id: Number(orderId), order_item_ids: ids.map(Number) }]) });
            const providers = (pv.result && pv.result.data && pv.result.data.shipment_providers) || (pv.data && pv.data.shipment_providers) || [];
            const pick = providers.find((p) => p.is_default || p.default) || providers[0] || {};
            const providerName = pick.provider_name || pick.name || pick.provider_code || '';
            await this.call(conn, '/order/pack', { shipping_provider: providerName, delivery_type: 'dropship', order_item_ids: orderItemIds }, 'POST');
            await this.call(conn, '/order/rts', { delivery_type: 'dropship', order_item_ids: orderItemIds, shipment_provider: providerName, tracking_number: '' }, 'POST');
        }
        const doc = await this.call(conn, '/order/document/get', { doc_type: 'shippingLabel', order_item_ids: JSON.stringify(ids.map(Number)) });
        const d = (doc.data && (doc.data.document || doc.data)) || {};
        let buffer;
        if (d.file) buffer = Buffer.from(String(d.file), 'base64');
        else if (d.pdf_url) buffer = await fetchHttpsFile(d.pdf_url);
        else throw new MarketplaceError('Lazada did not return a shipping label. The order may need to be packed/ready-to-ship first.', { code: 'LABEL_UNAVAILABLE', status: 409 });
        return { buffer, mimeType: sniffMime(buffer), trackingNumber: '', carrier: '' };
    }
};

// ======================================================== TIKTOK SHOP ADAPTER
const tiktok = {
    id: 'tiktok',
    // BUGFIX: dating `label: 'TikTok Shop'` — nabubura ito ng `async label(...)` (shipping label) sa parehong object, kaya function source ang lumalabas sa mga mensahe.
    title: 'TikTok Shop',
    cfg() {
        return {
            appKey: process.env.TIKTOK_APP_KEY || '',
            appSecret: process.env.TIKTOK_APP_SECRET || '',
            serviceId: process.env.TIKTOK_SERVICE_ID || '',
            base: process.env.TIKTOK_BASE_URL || 'https://open-api.tiktokglobalshop.com',
            authApi: process.env.TIKTOK_AUTH_API_URL || 'https://auth.tiktok-shops.com'
        };
    },
    configured() { const c = this.cfg(); return !!(c.appKey && c.appSecret); },
    // sign = HMAC-SHA256(secret, secret + path + sorted(k+v of query minus sign/access_token) + body + secret)
    sign(path, query, bodyStr) {
        const c = this.cfg();
        const s = Object.keys(query).filter((k) => k !== 'sign' && k !== 'access_token').sort().map((k) => k + query[k]).join('');
        return hmacHex(c.appSecret, `${c.appSecret}${path}${s}${bodyStr || ''}${c.appSecret}`);
    },
    authUrl({ state }) {
        const c = this.cfg();
        if (c.serviceId) return `https://services.tiktokshop.com/open/authorize?${new URLSearchParams({ service_id: c.serviceId, state })}`;
        return `${c.authApi}/oauth/authorize?${new URLSearchParams({ app_key: c.appKey, state })}`;
    },
    toConn(d) {
        const now = Date.now();
        const exp = (v, fallback) => { const n = num(v); return n > 1e9 ? (n < 1e12 ? n * 1000 : n) : (n ? now + n * 1000 : fallback); };
        return {
            shopId: '', accessToken: d.access_token, refreshToken: d.refresh_token,
            accessExpiresAt: exp(d.access_token_expire_in, now + 3600 * 1000), refreshExpiresAt: exp(d.refresh_token_expire_in, now + 30 * 24 * 3600 * 1000),
            extra: { openId: d.open_id || '', sellerName: d.seller_name || '' }
        };
    },
    async tokenGet(params) {
        const c = this.cfg();
        const r = await http(`${c.authApi}/api/v2/token/${params.grant_type === 'refresh_token' ? 'refresh' : 'get'}?${new URLSearchParams({ app_key: c.appKey, app_secret: c.appSecret, ...params })}`);
        const j = r.json || {};
        if (num(j.code) !== 0 || !j.data || !j.data.access_token) {
            throw new MarketplaceError(`TikTok Shop auth failed: ${j.message || `HTTP ${r.status}`}`, { code: 'AUTH', reauthRequired: true, status: 401 });
        }
        return j.data;
    },
    async exchange({ query }) {
        const code = query.code || query.auth_code;
        if (!code) throw new MarketplaceError('TikTok Shop did not return an authorization code.', { code: 'AUTH', status: 400 });
        const conn = this.toConn(await this.tokenGet({ auth_code: String(code), grant_type: 'authorized_code' }));
        const shops = await this.listShops(conn);
        const shop = shops.find((s) => String(s.region || '').toUpperCase() === 'PH') || shops[0];
        if (!shop) throw new MarketplaceError('No TikTok Shop is linked to this authorization.', { code: 'AUTH', status: 400 });
        conn.shopId = String(shop.id);
        conn.extra = { ...conn.extra, shopCipher: shop.cipher, region: shop.region || '', shopName: shop.name || '' };
        return conn;
    },
    async refresh(conn) {
        const next = this.toConn(await this.tokenGet({ refresh_token: conn.refreshToken, grant_type: 'refresh_token' }));
        next.shopId = conn.shopId;
        next.extra = { ...conn.extra, ...next.extra, shopCipher: conn.extra.shopCipher, region: conn.extra.region, shopName: conn.extra.shopName };
        return next;
    },
    async call(conn, method, path, { query = {}, body, withCipher = true } = {}) {
        const c = this.cfg();
        const q = { app_key: c.appKey, timestamp: String(Math.floor(Date.now() / 1000)), ...query };
        if (withCipher && conn.extra && conn.extra.shopCipher) q.shop_cipher = conn.extra.shopCipher;
        const bodyStr = body ? JSON.stringify(body) : '';
        q.sign = this.sign(path, q, bodyStr);
        const r = await http(`${c.base}${path}?${new URLSearchParams(q)}`, {
            method,
            headers: { 'Content-Type': 'application/json', 'x-tts-access-token': conn.accessToken },
            body: body ? bodyStr : undefined
        });
        const j = r.json || {};
        if (num(j.code) !== 0) {
            const authExpired = [105002, 105001, 36004004].includes(num(j.code)) || /access.?token|expired/i.test(String(j.message || ''));
            throw new MarketplaceError(`TikTok Shop: ${j.message || `error ${j.code}`}`, { code: String(j.code), authExpired });
        }
        return j.data || {};
    },
    async listShops(conn) {
        const d = await this.call(conn, 'GET', '/authorization/202309/shops', { withCipher: false });
        return d.shops || [];
    },
    async describe(conn) {
        return { shopName: (conn.extra && (conn.extra.shopName || conn.extra.sellerName)) || `TikTok Shop ${conn.shopId}`, extra: {} };
    },
    normOrder(conn, o) {
        const grouped = {};
        (o.line_items || []).filter((l) => !/CANCEL/i.test(String(l.display_status || ''))).forEach((l) => {
            const key = `${l.product_id}:${l.sku_id}`;
            if (!grouped[key]) grouped[key] = { itemId: String(l.product_id || ''), modelId: String(l.sku_id || ''), sku: String(l.seller_sku || ''), name: l.product_name || '', variation: l.sku_name || '', qty: 0, unitPrice: num(l.sale_price), meta: {} };
            grouped[key].qty += 1;
        });
        const pay = o.payment || {};
        const pkg = (o.packages || [])[0] || {};
        return {
            platform: 'tiktok', shopId: conn.shopId, orderId: String(o.id), orderNo: String(o.id),
            status: TIKTOK_STATUS[o.status] || 'to_ship', rawStatus: o.status, cancelRequested: false,
            createdAt: num(o.create_time) * 1000, updatedAt: num(o.update_time) * 1000,
            buyer: (o.recipient_address && o.recipient_address.name) || '', total: num(pay.total_amount), shippingFee: num(pay.shipping_fee), currency: pay.currency || 'PHP',
            paymentMethod: o.payment_method_name || '', carrier: o.shipping_provider || '', trackingNumber: o.tracking_number || '',
            items: Object.values(grouped), meta: { packageId: pkg.id || '' }
        };
    },
    async listOrders(conn, { sinceMs }) {
        const geSec = Math.floor(Math.max(sinceMs || 0, Date.now() - 30 * 24 * 3600 * 1000) / 1000);
        const ltSec = Math.floor(Date.now() / 1000) + 60;
        const orders = [];
        let token = '';
        for (let page = 0; page < 40; page++) {
            const d = await this.call(conn, 'POST', '/order/202309/orders/search', {
                query: { page_size: '50', sort_field: 'update_time', sort_order: 'ASC', ...(token ? { page_token: token } : {}) },
                body: { update_time_ge: geSec, update_time_lt: ltSec }
            });
            (d.orders || []).forEach((o) => orders.push(this.normOrder(conn, o)));
            if (!d.next_page_token) break;
            token = d.next_page_token;
        }
        return orders;
    },
    async listProducts(conn, { cursor }) {
        const d = await this.call(conn, 'POST', '/product/202309/products/search', { query: { page_size: '20', ...(cursor ? { page_token: cursor } : {}) }, body: { status: 'ALL' } });
        const rows = [];
        for (const p of (d.products || [])) {
            let detail = p;
            if (!Array.isArray(p.skus) || !p.skus.length) detail = await this.call(conn, 'GET', `/product/202309/products/${encodeURIComponent(p.id)}`);
            for (const s of (detail.skus || [])) {
                const inv = (s.inventory || [])[0] || {};
                rows.push({
                    platform: 'tiktok', shopId: conn.shopId, itemId: String(p.id), modelId: String(s.id),
                    sku: s.seller_sku || '', name: detail.title || p.title || '', variation: '',
                    stock: (s.inventory || []).reduce((sum, i) => sum + num(i.quantity), 0), price: num(s.price && (s.price.sale_price || s.price.tax_exclusive_price)),
                    meta: { warehouseId: inv.warehouse_id || '' }
                });
            }
        }
        return { items: rows, nextCursor: d.next_page_token || null };
    },
    async pushStock(conn, updates) {
        const results = [];
        for (const u of updates) {
            try {
                await this.call(conn, 'POST', `/product/202309/products/${encodeURIComponent(u.itemId)}/inventory/update`, {
                    body: { skus: [{ id: u.modelId, inventory: [{ warehouse_id: (u.meta && u.meta.warehouseId) || '', quantity: Math.max(0, Math.floor(num(u.stock))) }] }] }
                });
                results.push({ itemId: u.itemId, modelId: u.modelId, ok: true });
            } catch (e) { if (e.authExpired) throw e; results.push({ itemId: u.itemId, modelId: u.modelId, ok: false, error: e.message }); }
        }
        return results;
    },
    async pushPrice(conn, updates) {
        const results = [];
        for (const u of updates) {
            try {
                await this.call(conn, 'POST', `/product/202309/products/${encodeURIComponent(u.itemId)}/prices/update`, {
                    body: { skus: [{ id: u.modelId, price: { amount: (Math.round(num(u.price) * 100) / 100).toFixed(2), currency: 'PHP' } }] }
                });
                results.push({ itemId: u.itemId, modelId: u.modelId, ok: true });
            } catch (e) { if (e.authExpired) throw e; results.push({ itemId: u.itemId, modelId: u.modelId, ok: false, error: e.message }); }
        }
        return results;
    },
    async label(conn, { orderId, arrange, meta = {} }) {
        let packageId = meta.packageId || '';
        if (!packageId) {
            const d = await this.call(conn, 'GET', '/order/202309/orders', { query: { ids: String(orderId) } });
            packageId = ((((d.orders || [])[0] || {}).packages || [])[0] || {}).id || '';
        }
        if (!packageId) throw new MarketplaceError('This TikTok Shop order has no package yet (it may still be unpaid or on hold).', { code: 'LABEL_UNAVAILABLE', status: 409 });
        if (arrange) {
            await this.call(conn, 'POST', `/fulfillment/202309/packages/${encodeURIComponent(packageId)}/ship`, { body: { handover_method: 'PICKUP' } });
        }
        const d = await this.call(conn, 'GET', `/fulfillment/202309/packages/${encodeURIComponent(packageId)}/shipping_documents`, { query: { document_type: 'SHIPPING_LABEL', document_size: 'A6' } });
        if (!d.doc_url) throw new MarketplaceError('TikTok Shop did not return a shipping label yet.', { code: 'LABEL_UNAVAILABLE', status: 409 });
        const buffer = await fetchHttpsFile(d.doc_url);
        return { buffer, mimeType: sniffMime(buffer), trackingNumber: d.tracking_number || '', carrier: '' };
    }
};

const ADAPTERS = { shopee, lazada, tiktok };

// =============================================================== ROUTE LAYER
function registerMarketplaceRoutes(app, deps) {
    const {
        requireApiKey, requireAllowedDevice, rateLimit, isFeatureCurrentlyUnlocked, logActivity,
        getPersistentJSON, setPersistentJSON, getSubscriptionPrice, featureName
    } = deps;

    let store = null; // { [installationId]: { ["platform:shopId"]: conn } }
    let storeLoading = null;
    async function loadStore() {
        if (store) return store;
        if (!storeLoading) storeLoading = Promise.resolve(getPersistentJSON('marketplace-connections', null)).then((v) => { store = (v && typeof v === 'object') ? v : {}; return store; });
        return storeLoading;
    }
    function saveStore() { setPersistentJSON('marketplace-connections', store); }

    const locks = new Map();
    async function withLock(key, fn) {
        const prev = locks.get(key) || Promise.resolve();
        let release;
        const gate = new Promise((r) => { release = r; });
        const mine = prev.then(() => gate);
        locks.set(key, mine);
        await prev;
        try { return await fn(); } finally { release(); if (locks.get(key) === mine) locks.delete(key); }
    }

    // SECURITY FIX: dati may hardcoded na 'omnipos-dev-state' kapag walang env — sa production, mapepeke ng sinuman ang OAuth `state`
    // (na nagbibigkis ng token ng seller sa isang installation). Ngayon, sa production ay kailangan ng secret; 503 ang sagot kung wala.
    function stateSecret() {
        const secret = process.env.MARKETPLACE_STATE_SECRET || process.env.RELAY_API_KEY;
        if (secret) return secret;
        if (isProductionLike()) throw new MarketplaceError('Marketplace OAuth signing secret is not configured on the server (set MARKETPLACE_STATE_SECRET).', { code: 'NOT_CONFIGURED', status: 503 });
        return 'omnipos-dev-state';
    }
    const usedNonces = new Map();
    function signState(payload) {
        const body = b64url(JSON.stringify(payload));
        return `${body}.${b64url(crypto.createHmac('sha256', stateSecret()).update(body).digest())}`;
    }
    function verifyState(token) {
        const [body, sig] = String(token || '').split('.');
        if (!body || !sig) return null;
        const expect = b64url(crypto.createHmac('sha256', stateSecret()).update(body).digest());
        if (sig.length !== expect.length || !crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(expect))) return null;
        let p; try { p = JSON.parse(fromB64url(body).toString('utf8')); } catch (_) { return null; }
        if (!p || p.e < Date.now() || usedNonces.has(p.n)) return null;
        for (const [n, exp] of usedNonces) if (exp < Date.now()) usedNonces.delete(n);
        usedNonces.set(p.n, p.e);
        return p;
    }

    const publicBase = (req) => (process.env.RELAY_PUBLIC_URL || `${req.protocol}://${req.get('host')}`).replace(/\/+$/, '');
    const connKey = (platform, shopId) => `${platform}:${shopId}`;
    const toPublic = (c) => ({
        platform: c.platform, shopId: c.shopId, shopName: c.shopName, connectedAt: c.connectedAt, status: c.status,
        accessExpiresAt: c.accessExpiresAt, lastError: c.lastError || null, region: (c.extra && c.extra.region) || ''
    });
    const unpack = (c) => {
        const at = decryptSecretMeta(c.accessTokenEnc);
        const rt = decryptSecretMeta(c.refreshTokenEnc);
        return { ...c, accessToken: at.value, refreshToken: rt.value, extra: c.extra || {}, _needsKeyRotation: at.keyIndex > 0 || rt.keyIndex > 0 };
    };
    function pack(platform, plain, prev = {}) {
        return {
            platform, shopId: String(plain.shopId), shopName: plain.shopName || prev.shopName || '',
            accessTokenEnc: encryptSecret(plain.accessToken), refreshTokenEnc: encryptSecret(plain.refreshToken),
            accessExpiresAt: plain.accessExpiresAt, refreshExpiresAt: plain.refreshExpiresAt,
            extra: plain.extra || prev.extra || {}, connectedAt: prev.connectedAt || Date.now(), status: 'active', lastError: null
        };
    }

    function gate(req, res) {
        const installationId = req.body && req.body.installationId;
        if (!installationId) { res.status(400).json({ success: false, message: 'Missing installationId.' }); return null; }
        if (!isFeatureCurrentlyUnlocked(installationId, FEATURE_ID)) {
            res.status(402).json({
                success: false, featureLocked: true, featureId: FEATURE_ID, featureName: featureName(), isSubscription: true,
                subscriptionPrice: getSubscriptionPrice(),
                message: 'Marketplace Hub (Shopee / Lazada / TikTok Shop) requires an active subscription.'
            });
            return null;
        }
        return installationId;
    }

    // Loads + refreshes (if needed) a connection; runs fn(adapter, plainConn); retries once on auth expiry.
    async function withConnection(installationId, platform, shopId, fn) {
        const adapter = ADAPTERS[platform];
        if (!adapter) throw new MarketplaceError('Unknown marketplace.', { status: 400 });
        if (!adapter.configured()) throw new MarketplaceError(`${adapter.title} is not configured on the server yet.`, { code: 'NOT_CONFIGURED', status: 503 });
        await loadStore();
        const key = connKey(platform, shopId);
        return withLock(`${installationId}|${key}`, async () => {
            const rec = (store[installationId] || {})[key];
            if (!rec) throw new MarketplaceError(`${adapter.title} is not connected.`, { code: 'NOT_CONNECTED', status: 404 });
            if (rec.status === 'reauth_required') throw new MarketplaceError(`${adapter.title} needs to be reconnected.`, { code: 'REAUTH', status: 401, reauthRequired: true });
            let plain;
            try {
                plain = unpack(rec);
            } catch (decryptErr) {
                if (decryptErr instanceof MarketplaceError) throw decryptErr;
                // HINDI minamarkahang 'reauth_required' (pwedeng pansamantalang maling env lang ito): kapag naayos ang key, gagana ulit lahat nang
                // hindi na kailangang mag-reconnect ang mga seller.
                console.error(`🔴 Marketplace: cannot decrypt the stored ${adapter.title} credentials for ${installationId} — check MARKETPLACE_TOKEN_KEY / MARKETPLACE_TOKEN_KEY_PREVIOUS.`);
                throw new MarketplaceError(`${adapter.title} credentials cannot be read with the server's current encryption key. This is a server configuration problem — the shop does NOT need to be reconnected yet.`, { code: 'KEY_MISMATCH', status: 503 });
            }
            const needsKeyRotation = !!plain._needsKeyRotation;
            delete plain._needsKeyRotation;
            if (needsKeyRotation) {
                // Lumang key ang nakabasa: i-save agad gamit ang pangunahing key para puwede nang tanggalin ang lumang key balang-araw.
                try {
                    const savedShopName = rec.shopName;
                    store[installationId][key] = pack(platform, plain, rec);
                    store[installationId][key].shopName = savedShopName;
                    saveStore();
                } catch (rotateErr) { console.error('Marketplace key re-encryption skipped:', rotateErr && rotateErr.message); }
            }
            const doRefresh = async () => {
                try {
                    const next = await adapter.refresh(plain);
                    plain = { ...plain, ...next, shopName: plain.shopName };
                    store[installationId][key] = pack(platform, plain, rec);
                    store[installationId][key].shopName = rec.shopName;
                    saveStore();
                } catch (e) {
                    if (e.reauthRequired) { const cur = (store[installationId] || {})[key] || rec; cur.status = 'reauth_required'; cur.lastError = e.message; saveStore(); }
                    throw e;
                }
            };
            if (plain.accessExpiresAt && plain.accessExpiresAt - Date.now() < 5 * 60 * 1000) await doRefresh();
            try {
                return await fn(adapter, plain);
            } catch (e) {
                if (e.authExpired) { await doRefresh(); return fn(adapter, plain); }
                throw e;
            }
        });
    }

    const fail = (res, e) => {
        const status = e instanceof MarketplaceError ? e.status : 500;
        if (!(e instanceof MarketplaceError)) console.error('marketplace route error:', e);
        res.status(status).json({ success: false, code: e.code || 'ERROR', reauthRequired: !!e.reauthRequired, message: e instanceof MarketplaceError ? e.message : 'Unexpected marketplace error.' });
    };
    const baseGuards = [requireApiKey, requireAllowedDevice];
    const rl = (name, max, ms) => rateLimit(name, max, ms, (req) => req.body && req.body.installationId);

    app.post('/relay/marketplace/status', ...baseGuards, rl('mp-status', 120, 10 * 60 * 1000), async (req, res) => {
        const id = gate(req, res); if (!id) return;
        await loadStore();
        res.json({
            success: true,
            platforms: PLATFORMS.map((p) => ({ id: p, label: ADAPTERS[p].title, configured: ADAPTERS[p].configured() })),
            connections: Object.values(store[id] || {}).map(toPublic)
        });
    });

    app.post('/relay/marketplace/connect-url', ...baseGuards, rl('mp-connect', 20, 10 * 60 * 1000), async (req, res) => {
        const id = gate(req, res); if (!id) return;
        const platform = String(req.body.platform || '');
        const adapter = ADAPTERS[platform];
        if (!adapter) return res.status(400).json({ success: false, message: 'Unknown marketplace.' });
        if (!adapter.configured()) return res.status(503).json({ success: false, code: 'NOT_CONFIGURED', message: `${adapter.title} is not configured on the server yet. The developer must add the ${adapter.title} app credentials to RELAY first.` });
        // Ang signState() ay pwedeng magtapon ng MarketplaceError (503) kung walang signing secret sa production. Dapat may try/catch: ang
        // async handler na walang catch ay nagiging unhandled rejection (nakabitin ang request, at pwedeng bumagsak ang proseso sa Node 22).
        try {
            const state = signState({ i: id, p: platform, n: crypto.randomBytes(12).toString('hex'), e: Date.now() + 15 * 60 * 1000 });
            const redirectUri = `${publicBase(req)}/relay/marketplace/callback/${platform}`;
            res.json({ success: true, url: adapter.authUrl({ redirectUri, state }), redirectUri });
        } catch (e) {
            fail(res, e);
        }
    });

    app.get('/relay/marketplace/callback/:platform', rateLimit('mp-callback', 30, 10 * 60 * 1000), async (req, res) => {
        const page = (ok, msg) => res.status(ok ? 200 : 400).type('html').send(
            `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>OmniPOS Marketplace</title>` +
            `<body style="font-family:system-ui,sans-serif;display:flex;min-height:100vh;align-items:center;justify-content:center;background:#f4f6f8;margin:0">` +
            `<div style="background:#fff;padding:32px;border-radius:14px;max-width:420px;text-align:center;box-shadow:0 6px 24px rgba(0,0,0,.08)">` +
            `<div style="font-size:42px">${ok ? '✅' : '⚠️'}</div><h2 style="margin:8px 0">${ok ? 'Shop connected' : 'Connection failed'}</h2>` +
            `<p style="color:#555">${escHtml(msg)}</p></div></body>`);
        try {
            const platform = String(req.params.platform || '');
            const adapter = ADAPTERS[platform];
            if (!adapter || !adapter.configured()) return page(false, 'This marketplace is not available.');
            const st = verifyState(req.query.state);
            if (!st || st.p !== platform) return page(false, 'This link has expired or is invalid. Please start the connection again from OmniPOS.');
            if (!isFeatureCurrentlyUnlocked(st.i, FEATURE_ID)) return page(false, 'The Marketplace Hub subscription is not active for this device.');
            const conn = await adapter.exchange({ query: req.query });
            let info; try { info = await adapter.describe(conn); } catch (_) { info = { shopName: `${adapter.title} ${conn.shopId}`, extra: {} }; }
            conn.shopName = info.shopName;
            conn.extra = { ...(conn.extra || {}), ...(info.extra || {}) };
            await loadStore();
            store[st.i] = store[st.i] || {};
            const key = connKey(platform, conn.shopId);
            store[st.i][key] = pack(platform, conn, store[st.i][key] || {});
            saveStore();
            logActivity(st.i, 'marketplace_connected', { featureId: FEATURE_ID, platform, shopId: conn.shopId });
            page(true, `${info.shopName} is now linked. You can close this window and go back to OmniPOS.`);
        } catch (e) {
            console.error('marketplace callback error:', e.message);
            page(false, e instanceof MarketplaceError ? e.message : 'Something went wrong while connecting. Please try again.');
        }
    });

    app.post('/relay/marketplace/disconnect', ...baseGuards, rl('mp-disconnect', 20, 10 * 60 * 1000), async (req, res) => {
        const id = gate(req, res); if (!id) return;
        await loadStore();
        const key = connKey(String(req.body.platform || ''), String(req.body.shopId || ''));
        if (store[id] && store[id][key]) {
            delete store[id][key]; saveStore();
            logActivity(id, 'marketplace_disconnected', { featureId: FEATURE_ID, key });
        }
        res.json({ success: true, message: 'Disconnected. To fully revoke access, also remove the app from your seller center.' });
    });

    const connRoute = (path, bucket, max, handler) => app.post(path, ...baseGuards, rl(bucket, max, 10 * 60 * 1000), async (req, res) => {
        const id = gate(req, res); if (!id) return;
        const platform = String(req.body.platform || ''); const shopId = String(req.body.shopId || '');
        if (!platform || !shopId) return res.status(400).json({ success: false, message: 'platform and shopId are required.' });
        try { res.json({ success: true, ...(await withConnection(id, platform, shopId, (a, c) => handler(a, c, req.body, id))) }); }
        catch (e) { fail(res, e); }
    });

    connRoute('/relay/marketplace/orders', 'mp-orders', 60, async (a, c, body) => ({ orders: await a.listOrders(c, { sinceMs: num(body.sinceMs, 0) }) }));
    connRoute('/relay/marketplace/products', 'mp-products', 120, async (a, c, body) => a.listProducts(c, { cursor: body.cursor || '' }));
    connRoute('/relay/marketplace/push-stock', 'mp-push-stock', 60, async (a, c, body) => {
        const updates = (Array.isArray(body.updates) ? body.updates : []).slice(0, 200);
        return { results: await a.pushStock(c, updates) };
    });
    connRoute('/relay/marketplace/push-price', 'mp-push-price', 30, async (a, c, body) => {
        const updates = (Array.isArray(body.updates) ? body.updates : []).slice(0, 200);
        return { results: await a.pushPrice(c, updates) };
    });
    connRoute('/relay/marketplace/label', 'mp-label', 40, async (a, c, body, id) => {
        const out = await a.label(c, { orderId: String(body.orderId || ''), arrange: !!body.arrange, meta: body.meta || {} });
        logActivity(id, 'marketplace_label', { featureId: FEATURE_ID, platform: a.id });
        return { mimeType: out.mimeType, dataBase64: out.buffer.toString('base64'), trackingNumber: out.trackingNumber || '', carrier: out.carrier || '' };
    });
}

module.exports = {
    FEATURE_ID, PLATFORMS, ADAPTERS, MarketplaceError, registerMarketplaceRoutes,
    // exported for tests
    _internal: { encryptSecret, decryptSecret, decryptSecretMeta, tokenKeys, hmacHex, SHOPEE_STATUS, LAZADA_STATUS, TIKTOK_STATUS }
};
