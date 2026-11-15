// support-qr.js (RELAY)
// -----------------------------------------------------------------------------
// Extra "Support the Developer" payment options: static QR images uploaded by the developer in the RELAY admin
// panel (admin/support-qr.html), one per online-banking / e-wallet provider (GCash, Maya, BDO, ...).
//
// A client's OMNIPOS only sees a provider in its Support Developer payment dropdown when a QR image is attached AND
// enabled here. Providers without a QR never appear. These options sit next to the existing live checkout methods
// (including the PayMongo QR Ph option).
//
// This module is pure (no Express, no DB): server.js passes in load/save callbacks. That keeps it unit-testable.
// -----------------------------------------------------------------------------
'use strict';
const crypto = require('crypto');

const MAX_ITEMS = 40;
const MAX_IMAGE_BYTES = 300 * 1024;      // decoded image size cap per QR
const MAX_NOTE_LEN = 80;
const MAX_CUSTOM_LABEL_LEN = 40;
const STORE_KEY = 'support-qr-methods';

// Catalog of Philippine online banking apps / e-wallets / digital banks. Order = display order inside each group.
// `id` is stable and stored with the QR; never rename an id once released.
const CATALOG = [
    // ---- E-wallets & payment apps ----
    { id: 'gcash', label: 'GCash', group: 'E-wallets & payment apps' },
    { id: 'maya', label: 'Maya', group: 'E-wallets & payment apps' },
    { id: 'grabpay', label: 'GrabPay', group: 'E-wallets & payment apps' },
    { id: 'shopeepay', label: 'ShopeePay', group: 'E-wallets & payment apps' },
    { id: 'coinsph', label: 'Coins.ph', group: 'E-wallets & payment apps' },
    { id: 'paypal', label: 'PayPal', group: 'E-wallets & payment apps' },
    { id: 'alipay', label: 'Alipay', group: 'E-wallets & payment apps' },
    { id: 'wechatpay', label: 'WeChat Pay', group: 'E-wallets & payment apps' },
    { id: 'palawanpay', label: 'Palawan Pay', group: 'E-wallets & payment apps' },
    { id: 'starpay', label: 'StarPay', group: 'E-wallets & payment apps' },
    { id: 'qrph_static', label: 'QR Ph (my own bank QR)', group: 'E-wallets & payment apps' },
    // ---- Digital banks ----
    { id: 'gotyme', label: 'GoTyme Bank', group: 'Digital banks' },
    { id: 'seabank', label: 'SeaBank', group: 'Digital banks' },
    { id: 'maribank', label: 'MariBank', group: 'Digital banks' },
    { id: 'tonik', label: 'Tonik Digital Bank', group: 'Digital banks' },
    { id: 'uno_digital', label: 'UNO Digital Bank', group: 'Digital banks' },
    { id: 'uniondigital', label: 'UnionDigital Bank', group: 'Digital banks' },
    { id: 'ownbank', label: 'OwnBank', group: 'Digital banks' },
    { id: 'diskartech', label: 'DiskarTech (Rizal)', group: 'Digital banks' },
    { id: 'netbank', label: 'Netbank', group: 'Digital banks' },
    { id: 'cimb_ph', label: 'CIMB Bank PH', group: 'Digital banks' },
    { id: 'ing_ph', label: 'ING Bank PH', group: 'Digital banks' },
    { id: 'komo', label: 'KOMO (EastWest)', group: 'Digital banks' },
    // ---- Banks ----
    { id: 'bdo', label: 'BDO Unibank', group: 'Banks' },
    { id: 'bpi', label: 'BPI (Bank of the Philippine Islands)', group: 'Banks' },
    { id: 'metrobank', label: 'Metrobank', group: 'Banks' },
    { id: 'landbank', label: 'Landbank of the Philippines', group: 'Banks' },
    { id: 'pnb', label: 'PNB (Philippine National Bank)', group: 'Banks' },
    { id: 'securitybank', label: 'Security Bank', group: 'Banks' },
    { id: 'unionbank', label: 'UnionBank of the Philippines', group: 'Banks' },
    { id: 'rcbc', label: 'RCBC', group: 'Banks' },
    { id: 'chinabank', label: 'China Bank', group: 'Banks' },
    { id: 'eastwest', label: 'EastWest Bank', group: 'Banks' },
    { id: 'psbank', label: 'PSBank', group: 'Banks' },
    { id: 'dbp', label: 'DBP (Development Bank of the Philippines)', group: 'Banks' },
    { id: 'aub', label: 'AUB (Asia United Bank)', group: 'Banks' },
    { id: 'robinsonsbank', label: 'Robinsons Bank', group: 'Banks' },
    { id: 'bankcom', label: 'Bank of Commerce', group: 'Banks' },
    { id: 'pbcom', label: 'PBCom', group: 'Banks' },
    { id: 'maybank_ph', label: 'Maybank Philippines', group: 'Banks' },
    { id: 'ctbc', label: 'CTBC Bank (Philippines)', group: 'Banks' },
    { id: 'citibank_ph', label: 'Citibank Philippines', group: 'Banks' },
    { id: 'hsbc_ph', label: 'HSBC Philippines', group: 'Banks' },
    { id: 'sc_ph', label: 'Standard Chartered PH', group: 'Banks' },
    { id: 'philtrust', label: 'Philippine Trust Company (Philtrust)', group: 'Banks' },
    { id: 'sterling', label: 'Sterling Bank of Asia', group: 'Banks' },
    { id: 'chinabank_savings', label: 'China Bank Savings', group: 'Banks' },
    { id: 'bdo_network', label: 'BDO Network Bank', group: 'Banks' },
    { id: 'producers', label: 'Producers Bank', group: 'Banks' },
    { id: 'dungganon', label: 'Dungganon Bank', group: 'Banks' },
    { id: 'cebuana', label: 'Cebuana Lhuillier (Bank)', group: 'Banks' },
    { id: 'bangko_kabayan', label: 'Bangko Kabayan', group: 'Banks' }
];
const CATALOG_BY_ID = new Map(CATALOG.map((c) => [c.id, c]));

// A catalog id, or "custom_<hex>" for a provider that is not in the list (developer types the name).
const CUSTOM_ID_RE = /^custom_[a-f0-9]{8}$/;
const ID_RE = /^[a-z0-9_]{1,40}$/;

function isValidId(id) {
    return typeof id === 'string' && ID_RE.test(id) && (CATALOG_BY_ID.has(id) || CUSTOM_ID_RE.test(id));
}

function cleanText(v, maxLen) {
    // Remove control characters and angle brackets (labels are rendered by clients; keep them plain text) and collapse spaces.
    return String(v == null ? '' : v)
        .replace(/[\u0000-\u001f\u007f<>]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim()
        .slice(0, maxLen);
}

// Accepts only base64 PNG / JPEG / WEBP data URLs, verifies the real file signature (not just the declared type),
// and enforces the size cap. Returns { ok, dataUrl, mime, bytes } or { ok:false, message }.
function validateImageDataUrl(raw) {
    const s = typeof raw === 'string' ? raw.trim() : '';
    const m = /^data:(image\/(?:png|jpeg|webp));base64,([A-Za-z0-9+/]+={0,2})$/.exec(s);
    if (!m) return { ok: false, message: 'The QR image must be a PNG, JPG or WEBP file.' };
    const mime = m[1];
    let buf;
    try { buf = Buffer.from(m[2], 'base64'); } catch (e) { return { ok: false, message: 'The QR image could not be read.' }; }
    if (!buf.length) return { ok: false, message: 'The QR image is empty.' };
    if (buf.length > MAX_IMAGE_BYTES) {
        return { ok: false, message: `The QR image is too large (${Math.ceil(buf.length / 1024)} KB). The maximum is ${Math.floor(MAX_IMAGE_BYTES / 1024)} KB.` };
    }
    const isPng = buf.length > 8 && buf[0] === 0x89 && buf[1] === 0x50 && buf[2] === 0x4e && buf[3] === 0x47;
    const isJpg = buf.length > 3 && buf[0] === 0xff && buf[1] === 0xd8 && buf[2] === 0xff;
    const isWebp = buf.length > 12 && buf.toString('ascii', 0, 4) === 'RIFF' && buf.toString('ascii', 8, 12) === 'WEBP';
    const sigOk = (mime === 'image/png' && isPng) || (mime === 'image/jpeg' && isJpg) || (mime === 'image/webp' && isWebp);
    if (!sigOk) return { ok: false, message: 'The file content does not match an image of that type.' };
    return { ok: true, dataUrl: `data:${mime};base64,${m[2]}`, mime, bytes: buf.length };
}

function normalizeStoredItems(raw) {
    const list = raw && Array.isArray(raw.items) ? raw.items : [];
    const out = [];
    const seen = new Set();
    for (const it of list) {
        if (!it || !isValidId(it.id) || seen.has(it.id)) continue;
        const img = validateImageDataUrl(it.image);
        if (!img.ok) continue; // drop corrupt entries instead of serving a broken image to clients
        const catalogEntry = CATALOG_BY_ID.get(it.id);
        const label = catalogEntry ? catalogEntry.label : cleanText(it.label, MAX_CUSTOM_LABEL_LEN);
        if (!label) continue;
        seen.add(it.id);
        out.push({
            id: it.id,
            label,
            custom: !catalogEntry,
            note: cleanText(it.note, MAX_NOTE_LEN),
            image: img.dataUrl,
            bytes: img.bytes,
            enabled: it.enabled !== false,
            updatedAt: Number.isFinite(it.updatedAt) ? it.updatedAt : 0
        });
    }
    return out;
}

// load(): async () => stored object or null (must THROW if the storage could not be read, never return null for an error).
// save(obj): synchronous/async function that persists the object.
function createStore({ load, save, now }) {
    const clock = typeof now === 'function' ? now : () => Date.now();
    let items = null;
    let loading = null;
    let chain = Promise.resolve(); // serializes mutations so two admin clicks cannot overwrite each other

    async function ensureLoaded() {
        if (items) return items;
        if (!loading) {
            loading = (async () => {
                const stored = await load();
                items = normalizeStoredItems(stored);
                return items;
            })().finally(() => { loading = null; });
        }
        return loading;
    }
    function persist() {
        return save({ items: items.map((i) => ({ id: i.id, label: i.label, note: i.note, image: i.image, enabled: i.enabled, updatedAt: i.updatedAt })) });
    }
    function mutate(fn) {
        const run = chain.then(async () => {
            await ensureLoaded();
            const before = items;
            try {
                const result = await fn();
                await persist();
                return result;
            } catch (err) {
                items = before; // roll back the in-memory copy when validation or saving failed
                throw err;
            }
        });
        chain = run.catch(() => {});
        return run;
    }
    function fail(code, message) { const e = new Error(message); e.code = code; return e; }

    return {
        async listAdmin() { return (await ensureLoaded()).map((i) => ({ ...i })); },
        // Client-facing: enabled providers only, WITHOUT image data (the image is fetched when the client picks one).
        async listPublic() {
            return (await ensureLoaded()).filter((i) => i.enabled).map((i) => ({ id: i.id, label: i.label, updatedAt: i.updatedAt }));
        },
        async getPublicImage(id) {
            if (!isValidId(id)) return null;
            const found = (await ensureLoaded()).find((i) => i.id === id && i.enabled);
            return found ? { id: found.id, label: found.label, note: found.note, image: found.image, updatedAt: found.updatedAt } : null;
        },
        // bankId: catalog id; or omit and give customLabel for a provider that is not in the catalog.
        async upsert({ bankId, customLabel, image, note }) {
            const img = validateImageDataUrl(image);
            if (!img.ok) throw fail('BAD_IMAGE', img.message);
            return mutate(async () => {
                let id = typeof bankId === 'string' ? bankId.trim() : '';
                let label;
                if (id) {
                    if (!isValidId(id)) throw fail('BAD_PROVIDER', 'Unknown payment provider.');
                    const c = CATALOG_BY_ID.get(id);
                    if (c) label = c.label;
                    else {
                        const existing = items.find((i) => i.id === id);
                        if (!existing) throw fail('BAD_PROVIDER', 'Unknown payment provider.');
                        label = existing.label;
                    }
                } else {
                    label = cleanText(customLabel, MAX_CUSTOM_LABEL_LEN);
                    if (!label) throw fail('BAD_PROVIDER', 'Choose a provider from the list, or type the name of your bank / e-wallet.');
                    const dup = items.find((i) => i.custom && i.label.toLowerCase() === label.toLowerCase());
                    id = dup ? dup.id : 'custom_' + crypto.randomBytes(4).toString('hex');
                }
                const idx = items.findIndex((i) => i.id === id);
                if (idx < 0 && items.length >= MAX_ITEMS) throw fail('LIMIT', `You can attach at most ${MAX_ITEMS} QR codes. Delete one first.`);
                const entry = {
                    id,
                    label,
                    custom: !CATALOG_BY_ID.has(id),
                    note: cleanText(note, MAX_NOTE_LEN),
                    image: img.dataUrl,
                    bytes: img.bytes,
                    enabled: idx >= 0 ? items[idx].enabled : true,
                    updatedAt: clock()
                };
                items = idx >= 0 ? items.map((i, n) => (n === idx ? entry : i)) : items.concat(entry);
                return { ...entry };
            });
        },
        async setEnabled(id, enabled) {
            if (!isValidId(id)) throw fail('BAD_PROVIDER', 'Unknown payment provider.');
            return mutate(async () => {
                const idx = items.findIndex((i) => i.id === id);
                if (idx < 0) throw fail('NOT_FOUND', 'That QR code was not found.');
                items = items.map((i, n) => (n === idx ? { ...i, enabled: !!enabled, updatedAt: clock() } : i));
                return { id, enabled: !!enabled };
            });
        },
        async remove(id) {
            if (!isValidId(id)) throw fail('BAD_PROVIDER', 'Unknown payment provider.');
            return mutate(async () => {
                if (!items.some((i) => i.id === id)) throw fail('NOT_FOUND', 'That QR code was not found.');
                items = items.filter((i) => i.id !== id);
                return { id };
            });
        }
    };
}

module.exports = { CATALOG, MAX_ITEMS, MAX_IMAGE_BYTES, MAX_NOTE_LEN, MAX_CUSTOM_LABEL_LEN, STORE_KEY, isValidId, cleanText, validateImageDataUrl, normalizeStoredItems, createStore };
