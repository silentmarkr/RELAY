// env-vault-tools.js (RELAY)
// -----------------------------------------------------------------------------
// Helpers for the ENV page (env.html):
//   1. parseEnvText()  – reads Render's "Add from .env" format (KEY=value lines; also `export KEY=...`, quoted and
//                        multi-line quoted values, `# comments`) and a plain JSON object {"KEY":"value"}.
//   2. formatEnv()     – writes the same format back, so an export can be pasted straight into Render.
//   3. runChecks()     – tests the env values that are currently in effect (host ENV + vault) against the real
//                        services (database, Cloudflare, Google AI, e-mail, Telegram, payments ...) and also does
//                        format/consistency checks. It never returns a secret value; every message is scrubbed.
// Pure logic, no Express. The routes live in server.js.
// -----------------------------------------------------------------------------
'use strict';
const NAME_RE = /^[A-Z][A-Z0-9_]{1,79}$/;

// ---------------------------------------------------------------------------
// 1. PARSE
// ---------------------------------------------------------------------------
function unescapeDouble(s) {
    return s.replace(/\\(n|r|t|"|\\)/g, (_m, c) => (c === 'n' ? '\n' : c === 'r' ? '\r' : c === 't' ? '\t' : c));
}

/**
 * @returns {{ entries: {name:string,value:string,line:number}[], errors: {line:number,message:string}[], duplicates: string[] }}
 */
function parseEnvText(input) {
    const text = String(input == null ? '' : input).replace(/^﻿/, '').replace(/\r\n?/g, '\n');
    const errors = [];
    const byName = new Map();
    const duplicates = new Set();
    const put = (name, value, line) => {
        if (byName.has(name)) duplicates.add(name);
        byName.set(name, { name, value, line });
    };
    const trimmed = text.trim();
    // JSON object: {"KEY":"value", ...}
    if (trimmed.startsWith('{')) {
        try {
            const obj = JSON.parse(trimmed);
            if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
                for (const [k, v] of Object.entries(obj)) {
                    if (v === null || v === undefined) continue;
                    put(String(k).trim(), typeof v === 'string' ? v : String(v), 0);
                }
                return { entries: [...byName.values()], errors, duplicates: [...duplicates] };
            }
        } catch (_e) { /* not JSON: parse as .env below */ }
    }
    const lines = text.split('\n');
    for (let i = 0; i < lines.length; i++) {
        const lineNo = i + 1;
        let line = lines[i];
        if (!line.trim() || line.trim().startsWith('#')) continue;
        line = line.replace(/^\s*export\s+/, '');
        const eq = line.indexOf('=');
        if (eq < 1) { errors.push({ line: lineNo, message: 'Expected NAME=value' }); continue; }
        const name = line.slice(0, eq).trim();
        let rest = line.slice(eq + 1).replace(/^[ \t]+/, '');
        if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) { errors.push({ line: lineNo, message: `Invalid name: ${name.slice(0, 40)}` }); continue; }
        const q = rest[0];
        if (q === '"' || q === "'") {
            // Quoted value: may continue on the following lines until the closing quote.
            let acc = rest.slice(1);
            let closed = -1;
            let j = i;
            for (;;) {
                closed = -1;
                for (let k = 0; k < acc.length; k++) {
                    if (q === '"' && acc[k] === '\\') { k++; continue; }
                    if (acc[k] === q) { closed = k; break; }
                }
                if (closed >= 0) break;
                j++;
                if (j >= lines.length) break;
                acc += '\n' + lines[j];
            }
            if (closed < 0) { errors.push({ line: lineNo, message: 'Missing closing quote' }); i = j; continue; }
            const raw = acc.slice(0, closed);
            const after = acc.slice(closed + 1).trim();
            if (after && !after.startsWith('#')) errors.push({ line: lineNo, message: 'Unexpected text after the closing quote (ignored)' });
            put(name, q === '"' ? unescapeDouble(raw) : raw, lineNo);
            i = j;
        } else {
            // Unquoted: an inline comment starts at " #".
            const hash = rest.search(/\s#/);
            if (hash >= 0) rest = rest.slice(0, hash);
            put(name, rest.trim(), lineNo);
        }
    }
    return { entries: [...byName.values()], errors, duplicates: [...duplicates] };
}

// ---------------------------------------------------------------------------
// 2. FORMAT
// ---------------------------------------------------------------------------
function formatValue(v) {
    const s = String(v == null ? '' : v);
    if (s === '') return '';
    if (/^[A-Za-z0-9_.\/:@%+=,~\-]+$/.test(s)) return s;
    return '"' + s.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\r/g, '\\r').replace(/\n/g, '\\n').replace(/\t/g, '\\t') + '"';
}
function formatEnv(map, opts) {
    const o = opts || {};
    const names = Object.keys(map).sort();
    const header = o.header ? o.header.split('\n').map((l) => '# ' + l).join('\n') + '\n\n' : '';
    return header + names.map((n) => `${n}=${formatValue(map[n])}`).join('\n') + (names.length ? '\n' : '');
}

// ---------------------------------------------------------------------------
// 3. CHECKS
// ---------------------------------------------------------------------------
const NUMERIC_RE = /_(DAYS|HOURS|MS|MINUTES|CAP|COST|MAX|CLIENTS|CREDITS|RATIO|OFFSET_HOURS|OFFSET_MIN|SIZE|HOPS|PER_M|PER_DAY|TTL_HOURS|KEEP_DAYS|RECORDS|TIMEOUT_MS)$/;
const NOT_NUMERIC = new Set(['RELAY_AI_DAY_BOOST_OPTIONS', 'RELAY_AI_DAY_BOOST_ENABLED', 'RELAY_ALLOWED_DEVICES', 'RELAY_AUTOALLOW_ON_BACKUP', 'RELAY_ALERT_TELEGRAM_CHAT_ID']);
const ENUMS = {
    PAYMONGO_ENV: ['test', 'live'], PAYPAL_ENV: ['sandbox', 'live'], DRAGONPAY_ENV: ['test', 'sandbox', 'live', 'production'],
    SHOPEE_ENV: ['test', 'sandbox', 'live', 'production'], PGSSL: ['true', 'false', 'require', 'disable']
};
// Names that must be set together. [label, [names...]]
const PAIRS = [
    ['Gmail sender', ['RELAY_MAIL_USER', 'RELAY_MAIL_PASS']],
    ['Cloudflare AI', ['CF_ACCOUNT_ID', 'CF_AI_API_TOKEN']],
    ['Telegram (OTP/alerts)', ['RELAY_TELEGRAM_BOT_TOKEN', 'RELAY_TELEGRAM_CHAT_ID']],
    ['Telegram (system alerts)', ['RELAY_ALERT_TELEGRAM_BOT_TOKEN', 'RELAY_ALERT_TELEGRAM_CHAT_ID']],
    ['PayPal', ['PAYPAL_CLIENT_ID', 'PAYPAL_CLIENT_SECRET']],
    ['Dragonpay', ['DRAGONPAY_MERCHANT_ID', 'DRAGONPAY_SECRET_KEY']],
    ['Lazada', ['LAZADA_APP_KEY', 'LAZADA_APP_SECRET']],
    ['Shopee', ['SHOPEE_PARTNER_ID', 'SHOPEE_PARTNER_KEY']],
    ['TikTok Shop', ['TIKTOK_APP_KEY', 'TIKTOK_APP_SECRET']],
    ['Image search', ['IMAGE_SEARCH_API_KEY', 'IMAGE_SEARCH_CX']],
    ['PayMongo test', ['PAYMONGO_TEST_SECRET_KEY', 'PAYMONGO_TEST_WEBHOOK_SECRET']],
    ['PayMongo live', ['PAYMONGO_LIVE_SECRET_KEY', 'PAYMONGO_LIVE_WEBHOOK_SECRET']],
    ['Neon API', ['NEON_API_KEY']]
];
const URL_VARS = ['RELAY_PUBLIC_URL', 'RENDER_EXTERNAL_URL', 'OMNIPOS_REPO_URL', 'GOOGLE_PRICING_URL', 'RELAY_ALERT_WEBHOOK_URL',
    'RELAY_SLACK_WEBHOOK_URL', 'BOOTSTRAP_CLIENT_ZIP_URL', 'BOOTSTRAP_TERMUX_ARM32', 'BOOTSTRAP_TERMUX_ARM64', 'BOOTSTRAP_TERMUX_UNIVERSAL',
    'BOOTSTRAP_TERMUX_X86_64', 'LAZADA_BASE_URL', 'LAZADA_AUTH_URL', 'LAZADA_AUTH_REST_URL', 'SHOPEE_BASE_URL', 'TIKTOK_BASE_URL', 'TIKTOK_AUTH_API_URL'];
const PREFIXES = [
    ['PAYMONGO_TEST_SECRET_KEY', /^sk_test_/, 'should start with sk_test_'],
    ['PAYMONGO_LIVE_SECRET_KEY', /^sk_live_/, 'should start with sk_live_'],
    ['PAYMONGO_TEST_WEBHOOK_SECRET', /^whsk_/, 'should start with whsk_'],
    ['PAYMONGO_LIVE_WEBHOOK_SECRET', /^whsk_/, 'should start with whsk_'],
    ['STRIPE_SECRET_KEY', /^(sk|rk)_(live|test)_/, 'should start with sk_live_ / sk_test_'],
    ['STRIPE_WEBHOOK_SECRET', /^whsec_/, 'should start with whsec_'],
    ['RESEND_API_KEY', /^re_/, 'should start with re_'],
    ['GOOGLE_AI_API_KEY', /^AIza/, 'should start with AIza'],
    ['RELAY_TELEGRAM_BOT_TOKEN', /^\d{5,}:[\w-]{20,}$/, 'should look like 123456789:AA...'],
    ['RELAY_ALERT_TELEGRAM_BOT_TOKEN', /^\d{5,}:[\w-]{20,}$/, 'should look like 123456789:AA...'],
    ['CF_ACCOUNT_ID', /^[0-9a-f]{32}$/i, 'should be 32 hex characters'],
    ['DATABASE_URL', /^postgres(ql)?:\/\//, 'should start with postgresql://'],
    ['RELAY_DEVICES_DATABASE_URL', /^postgres(ql)?:\/\//, 'should start with postgresql://'],
    ['RELAY_BUILD_DATABASE_URL', /^postgres(ql)?:\/\//, 'should start with postgresql://'],
    ['REDIS_URL', /^rediss?:\/\//, 'should start with redis:// or rediss://']
];

function has(env, n) { return env[n] !== undefined && env[n] !== null && String(env[n]).trim() !== ''; }
function isHttpUrl(v) { try { const u = new URL(String(v)); return u.protocol === 'https:' || u.protocol === 'http:'; } catch (_e) { return false; } }
// Block obviously private targets so the check can never be used to probe the internal network.
function isPublicHttpUrl(v) {
    if (!isHttpUrl(v)) return false;
    const h = new URL(String(v)).hostname.toLowerCase();
    if (h === 'localhost' || h.endsWith('.local') || h.endsWith('.internal')) return false;
    if (/^(127\.|10\.|0\.|169\.254\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)/.test(h)) return false;
    if (h === '::1' || h.startsWith('fc') || h.startsWith('fd') || h.startsWith('fe80')) return false;
    return true;
}

function makeScrubber(env, names) {
    const secrets = [];
    for (const n of names) {
        const v = env[n];
        if (v && String(v).length >= 6) {
            secrets.push(String(v));
            try { const u = new URL(String(v)); if (u.password) secrets.push(decodeURIComponent(u.password)); if (u.username && u.username.length >= 6) secrets.push(u.username); } catch (_e) { /* not a URL */ }
        }
    }
    secrets.sort((a, b) => b.length - a.length);
    return (msg) => {
        let out = String(msg == null ? '' : msg);
        for (const s of secrets) out = out.split(s).join('***');
        return out.length > 400 ? out.slice(0, 400) + '…' : out;
    };
}

async function timedFetch(url, opts, ms) {
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), ms || 8000);
    try { return await fetch(url, Object.assign({}, opts || {}, { signal: ac.signal })); } finally { clearTimeout(t); }
}
async function reachable(url) {
    let r = await timedFetch(url, { method: 'HEAD', redirect: 'follow' }, 8000).catch((e) => ({ _err: e }));
    if (r && r._err || (r && (r.status === 405 || r.status === 403 || r.status === 501))) {
        r = await timedFetch(url, { method: 'GET', redirect: 'follow', headers: { Range: 'bytes=0-0' } }, 8000);
        try { if (r.body && r.body.cancel) await r.body.cancel(); } catch (_e) { /* ok */ }
    }
    return r;
}

/**
 * @param {object} ctx  { env, deps:{ pg, Redis, nodemailer, maskUrl }, live?:boolean }
 * @returns {Promise<{ results: object[], summary: object }>}
 */
async function runChecks(ctx) {
    const env = ctx.env;
    const live = ctx.live !== false;
    const deps = ctx.deps || {};
    const allNames = Object.keys(env).filter((n) => NAME_RE.test(n));
    const scrub = makeScrubber(env, allNames);
    const results = [];
    const jobs = [];
    const add = (group, id, title, vars, fn, opts) => {
        const o = opts || {};
        jobs.push((async () => {
            const t0 = Date.now();
            let r;
            try { r = await fn(); } catch (e) { r = { status: 'fail', detail: scrub(e && e.name === 'AbortError' ? 'Timed out' : (e && e.message) || e) }; }
            if (!r) r = { status: 'skip', detail: 'Not configured' };
            results.push({ group, id, title, vars: vars || [], status: r.status, detail: scrub(r.detail || ''), ms: o.local ? 0 : Date.now() - t0, live: !!o.live });
        })());
    };
    const ok = (detail) => ({ status: 'ok', detail });
    const warn = (detail) => ({ status: 'warn', detail });
    const fail = (detail) => ({ status: 'fail', detail });
    const skip = (detail) => ({ status: 'skip', detail: detail || 'Not configured (optional)' });
    const addLive = (group, id, title, vars, fn) => add(group, id, title, vars, async () => (live ? fn() : skip('Skipped (live tests are off)')), { live: true });

    // ----- Core / format checks (local, instant) -----
    add('Core', 'core-admin-key', 'RELAY admin key', ['RELAY_ADMIN_KEY'], async () => {
        if (!has(env, 'RELAY_ADMIN_KEY')) return fail('RELAY_ADMIN_KEY is not set: the admin panel cannot be opened.');
        return String(env.RELAY_ADMIN_KEY).length < 16 ? warn('Set, but shorter than 16 characters. Use a longer key.') : ok('Set, length is fine.');
    }, { local: true });
    add('Core', 'core-api-key', 'RELAY API key (clients)', ['RELAY_API_KEY'], async () => (has(env, 'RELAY_API_KEY') ? (String(env.RELAY_API_KEY).length < 16 ? warn('Set, but short. Use at least 16 characters.') : ok('Set.')) : fail('RELAY_API_KEY is not set: OmniPOS clients cannot talk to RELAY.')), { local: true });
    add('Core', 'core-private-key', 'RELAY private key (signing)', ['RELAY_PRIVATE_KEY_PEM'], async () => {
        const crypto = require('crypto');
        let pem = has(env, 'RELAY_PRIVATE_KEY_PEM') ? String(env.RELAY_PRIVATE_KEY_PEM).replace(/\\n/g, '\n') : null;
        let src = 'RELAY_PRIVATE_KEY_PEM';
        if (!pem) {
            try { pem = require('fs').readFileSync(require('path').join(__dirname, 'relay-private-key.pem'), 'utf8'); src = 'relay-private-key.pem file'; } catch (_e) { /* none */ }
        }
        if (!pem) return fail('No private key found (RELAY_PRIVATE_KEY_PEM or relay-private-key.pem). Licenses and unlock commands cannot be signed.');
        try { const k = crypto.createPrivateKey(pem); return ok(`Valid ${k.asymmetricKeyType.toUpperCase()} private key (from ${src}).`); } catch (_e) { return fail(`The private key from ${src} cannot be read. If it was pasted with line breaks lost, use \\n for new lines.`); }
    }, { local: true });
    add('Core', 'core-urls', 'Public URL', ['RELAY_PUBLIC_URL', 'RENDER_EXTERNAL_URL'], async () => {
        if (!has(env, 'RELAY_PUBLIC_URL') && !has(env, 'RENDER_EXTERNAL_URL')) return warn('Neither RELAY_PUBLIC_URL nor RENDER_EXTERNAL_URL is set: links sent to clients may be wrong.');
        for (const n of ['RELAY_PUBLIC_URL', 'RENDER_EXTERNAL_URL']) if (has(env, n) && !isHttpUrl(env[n])) return fail(`${n} is not a valid http(s) URL.`);
        if (has(env, 'RELAY_PUBLIC_URL') && !/^https:/i.test(env.RELAY_PUBLIC_URL) && !/localhost|127\./.test(env.RELAY_PUBLIC_URL)) return warn('RELAY_PUBLIC_URL is not https.');
        if (has(env, 'RELAY_PUBLIC_URL') && has(env, 'RENDER_EXTERNAL_URL')) {
            const a = new URL(env.RELAY_PUBLIC_URL).host, b = new URL(env.RENDER_EXTERNAL_URL).host;
            if (a !== b) return warn(`RELAY_PUBLIC_URL (${a}) is different from the Render URL (${b}). Fine if you use a custom domain; otherwise update it.`);
        }
        return ok('Valid.');
    }, { local: true });

    // Numeric / enum / url format scan over every variable that is set.
    add('Format', 'fmt-numbers', 'Number values', [], async () => {
        const bad = allNames.filter((n) => has(env, n) && NUMERIC_RE.test(n) && !NOT_NUMERIC.has(n) && !/^-?\d+(\.\d+)?$/.test(String(env[n]).trim()));
        return bad.length ? fail(`Not a number: ${bad.join(', ')}`) : ok('All numeric settings are numbers.');
    }, { local: true });
    add('Format', 'fmt-enums', 'Mode values (test / live ...)', Object.keys(ENUMS), async () => {
        const bad = Object.keys(ENUMS).filter((n) => has(env, n) && !ENUMS[n].includes(String(env[n]).trim().toLowerCase()));
        return bad.length ? fail(bad.map((n) => `${n} must be one of: ${ENUMS[n].join(', ')}`).join('; ')) : ok('All mode values are valid.');
    }, { local: true });
    add('Format', 'fmt-urls', 'URL values', URL_VARS, async () => {
        const bad = URL_VARS.filter((n) => has(env, n) && !isHttpUrl(env[n]));
        return bad.length ? fail(`Not a valid http(s) URL: ${bad.join(', ')}`) : ok('All URLs are valid.');
    }, { local: true });
    add('Format', 'fmt-prefix', 'Key formats', PREFIXES.map((p) => p[0]), async () => {
        const bad = PREFIXES.filter((p) => has(env, p[0]) && !p[1].test(String(env[p[0]]).trim())).map((p) => `${p[0]} ${p[2]}`);
        return bad.length ? warn(bad.join('; ')) : ok('All keys look correctly formatted.');
    }, { local: true });
    add('Format', 'fmt-whitespace', 'Hidden spaces / quotes', [], async () => {
        const bad = allNames.filter((n) => has(env, n) && n !== 'RELAY_PRIVATE_KEY_PEM' && (String(env[n]) !== String(env[n]).trim() || /^["'].*["']$/s.test(String(env[n]).trim()) && !/\n/.test(String(env[n]))));
        return bad.length ? warn(`Leading/trailing space or wrapping quotes in: ${bad.join(', ')} (often a copy-paste mistake).`) : ok('None found.');
    }, { local: true });
    add('Format', 'fmt-pairs', 'Variables that must be set together', PAIRS.flatMap((p) => p[1]), async () => {
        const probs = [];
        for (const [label, names] of PAIRS) {
            if (names.length < 2) continue;
            const set = names.filter((n) => has(env, n));
            if (set.length && set.length < names.length) probs.push(`${label}: missing ${names.filter((n) => !has(env, n)).join(', ')}`);
        }
        if (has(env, 'IMAGE_SEARCH_API_KEY') && !has(env, 'IMAGE_SEARCH_PROVIDER')) probs.push('Image search: IMAGE_SEARCH_PROVIDER is not set');
        if (String(env.PAYMONGO_ENV || '').toLowerCase() === 'live' && !has(env, 'PAYMONGO_LIVE_SECRET_KEY')) probs.push('PAYMONGO_ENV=live but PAYMONGO_LIVE_SECRET_KEY is missing');
        if (String(env.PAYMONGO_ENV || '').toLowerCase() === 'test' && !has(env, 'PAYMONGO_TEST_SECRET_KEY')) probs.push('PAYMONGO_ENV=test but PAYMONGO_TEST_SECRET_KEY is missing');
        return probs.length ? fail(probs.join('; ')) : ok('No half-filled groups.');
    }, { local: true });

    // ----- Databases -----
    const dbVars = [['DATABASE_URL', 'Cloud backup database'], ['RELAY_DEVICES_DATABASE_URL', 'Devices database'], ['RELAY_BUILD_DATABASE_URL', 'Build database']];
    for (const [name, label] of dbVars) {
        addLive('Database', 'db-' + name, `${label} (${name})`, [name, 'PGSSL'], async () => {
            if (!has(env, name)) return skip(name === 'DATABASE_URL' && !has(env, 'RELAY_DEVICES_DATABASE_URL') ? 'Not set: RELAY would fall back to Redis/file storage.' : 'Not set (optional)');
            if (!deps.pg) return skip('pg driver unavailable');
            const client = new deps.pg.Client({ connectionString: env[name], ssl: env.PGSSL === 'false' ? false : { rejectUnauthorized: false }, connectionTimeoutMillis: 7000, statement_timeout: 7000 });
            client.on('error', () => {});
            try {
                await client.connect();
                const r = await client.query('select current_database() as db, version() as v');
                return ok(`Connected to ${deps.maskUrl ? deps.maskUrl(env[name]) : r.rows[0].db} — ${String(r.rows[0].v).split(' ').slice(0, 2).join(' ')}.`);
            } finally { try { await client.end(); } catch (_e) { /* ok */ } }
        });
    }
    addLive('Database', 'db-redis', 'Redis (REDIS_URL)', ['REDIS_URL'], async () => {
        if (!has(env, 'REDIS_URL')) return skip();
        if (!deps.Redis) return skip('ioredis unavailable');
        const r = new deps.Redis(env.REDIS_URL, { lazyConnect: true, connectTimeout: 6000, maxRetriesPerRequest: 1, retryStrategy: () => null });
        r.on('error', () => {});
        try { await r.connect(); const pong = await r.ping(); return ok(`Connected (${pong}).`); } finally { try { r.disconnect(); } catch (_e) { /* ok */ } }
    });
    addLive('Database', 'neon-api', 'Neon API key + project IDs', ['NEON_API_KEY', 'NEON_DEVICES_PROJECT_ID', 'NEON_CLOUD_BACKUP_PROJECT_ID', 'NEON_BUILD_PROJECT_ID'], async () => {
        if (!has(env, 'NEON_API_KEY')) return has(env, 'NEON_DEVICES_PROJECT_ID') || has(env, 'NEON_CLOUD_BACKUP_PROJECT_ID') || has(env, 'NEON_BUILD_PROJECT_ID') ? fail('Project IDs are set but NEON_API_KEY is missing.') : skip();
        const h = { Authorization: 'Bearer ' + env.NEON_API_KEY, Accept: 'application/json' };
        const r = await timedFetch('https://console.neon.tech/api/v2/projects?limit=1', { headers: h }, 8000);
        if (r.status === 401 || r.status === 403) return fail('Neon rejected the API key.');
        if (!r.ok) return fail(`Neon API answered HTTP ${r.status}.`);
        const bad = [];
        for (const n of ['NEON_DEVICES_PROJECT_ID', 'NEON_CLOUD_BACKUP_PROJECT_ID', 'NEON_BUILD_PROJECT_ID']) {
            if (!has(env, n)) continue;
            const pr = await timedFetch('https://console.neon.tech/api/v2/projects/' + encodeURIComponent(env[n]), { headers: h }, 8000);
            if (!pr.ok) bad.push(`${n} (HTTP ${pr.status})`);
        }
        return bad.length ? fail(`API key works, but these project IDs were not found: ${bad.join(', ')}`) : ok('API key works and every project ID was found.');
    });

    // ----- AI -----
    addLive('AI', 'cf-ai', 'Cloudflare Workers AI token', ['CF_ACCOUNT_ID', 'CF_AI_API_TOKEN'], async () => {
        if (!has(env, 'CF_ACCOUNT_ID') && !has(env, 'CF_AI_API_TOKEN')) return skip();
        if (!has(env, 'CF_ACCOUNT_ID') || !has(env, 'CF_AI_API_TOKEN')) return fail('Both CF_ACCOUNT_ID and CF_AI_API_TOKEN are needed.');
        const h = { Authorization: 'Bearer ' + env.CF_AI_API_TOKEN };
        const acct = encodeURIComponent(env.CF_ACCOUNT_ID);
        let v = await timedFetch(`https://api.cloudflare.com/client/v4/accounts/${acct}/tokens/verify`, { headers: h }, 8000);
        let j = await v.json().catch(() => ({}));
        if (!(j && j.success)) {
            v = await timedFetch('https://api.cloudflare.com/client/v4/user/tokens/verify', { headers: h }, 8000);
            j = await v.json().catch(() => ({}));
        }
        if (!(j && j.success && j.result && j.result.status === 'active')) return fail('Cloudflare did not accept this token (invalid, expired or wrong account).');
        const m = await timedFetch(`https://api.cloudflare.com/client/v4/accounts/${acct}/ai/models/search?per_page=1`, { headers: h }, 8000);
        if (m.status === 401 || m.status === 403) return warn('Token is valid but has no Workers AI permission for this account ID.');
        if (!m.ok) return warn(`Token is valid; the Workers AI check answered HTTP ${m.status}.`);
        return ok('Token is active and can use Workers AI for this account.');
    });
    const googleModels = [['GOOGLE_AI_MODEL_FLASH', 'Flash'], ['GOOGLE_AI_MODEL_FLASH_LITE', 'Flash-Lite']];
    addLive('AI', 'google-key', 'Google AI (Gemini) API key', ['GOOGLE_AI_API_KEY'], async () => {
        if (!has(env, 'GOOGLE_AI_API_KEY')) return skip();
        const r = await timedFetch('https://generativelanguage.googleapis.com/v1beta/models?pageSize=1', { headers: { 'x-goog-api-key': env.GOOGLE_AI_API_KEY } }, 8000);
        if (r.status === 400 || r.status === 401 || r.status === 403) return fail('Google rejected the API key.');
        if (r.status === 429) return warn('Key is accepted but Google says the rate limit is reached right now.');
        return r.ok ? ok('API key accepted.') : fail(`Google answered HTTP ${r.status}.`);
    });
    for (const [name, label] of googleModels) {
        addLive('AI', 'google-' + name, `Google AI model (${label})`, [name, 'GOOGLE_AI_API_KEY'], async () => {
            if (!has(env, name)) return skip('Not set: the built-in default model is used.');
            if (!has(env, 'GOOGLE_AI_API_KEY')) return skip('Needs GOOGLE_AI_API_KEY');
            const id = String(env[name]).replace(/^models\//, '');
            const r = await timedFetch('https://generativelanguage.googleapis.com/v1beta/models/' + encodeURIComponent(id), { headers: { 'x-goog-api-key': env.GOOGLE_AI_API_KEY } }, 8000);
            if (r.status === 404) return fail(`Model "${id}" does not exist (renamed or retired). Pick another model.`);
            if (r.status === 400 || r.status === 401 || r.status === 403) return fail('Google rejected the API key.');
            return r.ok ? ok(`Model "${id}" exists.`) : warn(`Google answered HTTP ${r.status}.`);
        });
    }

    // ----- Email -----
    addLive('Email', 'mail-gmail', 'Gmail sender (SMTP login)', ['RELAY_MAIL_USER', 'RELAY_MAIL_PASS', 'RELAY_RECIPIENT_EMAIL'], async () => {
        if (!has(env, 'RELAY_MAIL_USER') && !has(env, 'RELAY_MAIL_PASS')) return skip(has(env, 'RESEND_API_KEY') ? 'Not used (Resend is configured).' : 'Not set.');
        if (!has(env, 'RELAY_MAIL_USER') || !has(env, 'RELAY_MAIL_PASS')) return fail('Both RELAY_MAIL_USER and RELAY_MAIL_PASS are needed.');
        if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(env.RELAY_MAIL_USER)) return fail('RELAY_MAIL_USER is not an e-mail address.');
        if (has(env, 'RELAY_RECIPIENT_EMAIL') && !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(String(env.RELAY_RECIPIENT_EMAIL).split(',')[0].trim())) return fail('RELAY_RECIPIENT_EMAIL is not an e-mail address.');
        if (!deps.nodemailer) return skip('nodemailer unavailable');
        const t = deps.nodemailer.createTransport({ service: 'gmail', auth: { user: env.RELAY_MAIL_USER, pass: env.RELAY_MAIL_PASS }, connectionTimeout: 8000, greetingTimeout: 8000, socketTimeout: 10000 });
        try { await t.verify(); return ok('Gmail accepted the login (no e-mail was sent).'); } catch (e) {
            if (/535|Username and Password not accepted|Invalid login/i.test(String(e && e.message))) return fail('Gmail rejected the login. Use a Google App Password (16 characters), not the normal password.');
            throw e;
        } finally { try { t.close(); } catch (_e) { /* ok */ } }
    });
    addLive('Email', 'mail-resend', 'Resend API key', ['RESEND_API_KEY', 'RESEND_FROM_EMAIL'], async () => {
        if (!has(env, 'RESEND_API_KEY')) return skip();
        if (has(env, 'RESEND_FROM_EMAIL') && !/^(.+<)?[^@\s<>]+@[^@\s<>]+\.[^@\s<>]+>?$/.test(String(env.RESEND_FROM_EMAIL).trim())) return fail('RESEND_FROM_EMAIL is not a valid sender (Name <a@b.com> or a@b.com).');
        const r = await timedFetch('https://api.resend.com/domains', { headers: { Authorization: 'Bearer ' + env.RESEND_API_KEY } }, 8000);
        const j = await r.json().catch(() => ({}));
        if (r.ok) return ok('API key accepted.');
        if (r.status === 401 && /restricted/i.test(JSON.stringify(j))) return ok('Key accepted (sending-only key).');
        if (r.status === 401 || r.status === 403) return fail('Resend rejected the API key.');
        return warn(`Resend answered HTTP ${r.status}.`);
    });

    // ----- Alerts -----
    const tg = (id, label, tokenVar, chatVar) => addLive('Alerts', id, label, [tokenVar, chatVar], async () => {
        if (!has(env, tokenVar) && !has(env, chatVar)) return skip();
        if (!has(env, tokenVar) || !has(env, chatVar)) return fail(`Both ${tokenVar} and ${chatVar} are needed.`);
        const base = 'https://api.telegram.org/bot' + env[tokenVar];
        const me = await timedFetch(base + '/getMe', {}, 8000);
        const mj = await me.json().catch(() => ({}));
        if (!mj.ok) return fail('Telegram rejected the bot token.');
        const chat = await timedFetch(base + '/getChat?chat_id=' + encodeURIComponent(env[chatVar]), {}, 8000);
        const cj = await chat.json().catch(() => ({}));
        if (!cj.ok) return fail(`Bot @${mj.result && mj.result.username} is valid, but it cannot see chat ${env[chatVar]}. Open the bot in Telegram and press Start (or add it to the group).`);
        return ok(`Bot @${mj.result && mj.result.username} is valid and can see the chat. (No message was sent.)`);
    });
    tg('alert-tg', 'Telegram bot (OTP / alerts)', 'RELAY_TELEGRAM_BOT_TOKEN', 'RELAY_TELEGRAM_CHAT_ID');
    tg('alert-tg2', 'Telegram bot (system alerts)', 'RELAY_ALERT_TELEGRAM_BOT_TOKEN', 'RELAY_ALERT_TELEGRAM_CHAT_ID');
    add('Alerts', 'alert-webhooks', 'Alert webhooks (Slack / generic)', ['RELAY_SLACK_WEBHOOK_URL', 'RELAY_ALERT_WEBHOOK_URL'], async () => {
        const set = ['RELAY_SLACK_WEBHOOK_URL', 'RELAY_ALERT_WEBHOOK_URL'].filter((n) => has(env, n));
        if (!set.length) return skip();
        const bad = set.filter((n) => !/^https:/i.test(env[n]));
        return bad.length ? fail(`${bad.join(', ')} must be an https URL.`) : ok('Format is valid. (Not called, to avoid sending a test message to your channel.)');
    }, { local: true });

    // ----- Payments -----
    const basic = (k) => 'Basic ' + Buffer.from(k + ':').toString('base64');
    for (const [mode, name] of [['test', 'PAYMONGO_TEST_SECRET_KEY'], ['live', 'PAYMONGO_LIVE_SECRET_KEY']]) {
        addLive('Payments', 'pay-paymongo-' + mode, `PayMongo ${mode} secret key`, [name], async () => {
            if (!has(env, name)) return skip();
            const r = await timedFetch('https://api.paymongo.com/v1/webhooks', { headers: { Authorization: basic(env[name]) } }, 8000);
            if (r.status === 401 || r.status === 403) return fail('PayMongo rejected the key.');
            return r.ok ? ok('Key accepted.') : warn(`PayMongo answered HTTP ${r.status}.`);
        });
    }
    addLive('Payments', 'pay-stripe', 'Stripe secret key', ['STRIPE_SECRET_KEY'], async () => {
        if (!has(env, 'STRIPE_SECRET_KEY')) return skip();
        const r = await timedFetch('https://api.stripe.com/v1/balance', { headers: { Authorization: 'Bearer ' + env.STRIPE_SECRET_KEY } }, 8000);
        if (r.status === 401) return fail('Stripe rejected the key.');
        return r.ok ? ok('Key accepted.') : warn(`Stripe answered HTTP ${r.status} (a restricted key may not read the balance).`);
    });
    addLive('Payments', 'pay-xendit', 'Xendit secret key', ['XENDIT_SECRET_KEY'], async () => {
        if (!has(env, 'XENDIT_SECRET_KEY')) return skip();
        const r = await timedFetch('https://api.xendit.co/balance', { headers: { Authorization: basic(env.XENDIT_SECRET_KEY) } }, 8000);
        if (r.status === 401) return fail('Xendit rejected the key.');
        return r.ok ? ok('Key accepted.') : warn(`Xendit answered HTTP ${r.status} (the key may not have balance permission).`);
    });
    addLive('Payments', 'pay-paypal', 'PayPal client ID + secret', ['PAYPAL_CLIENT_ID', 'PAYPAL_CLIENT_SECRET', 'PAYPAL_ENV'], async () => {
        if (!has(env, 'PAYPAL_CLIENT_ID') && !has(env, 'PAYPAL_CLIENT_SECRET')) return skip();
        if (!has(env, 'PAYPAL_CLIENT_ID') || !has(env, 'PAYPAL_CLIENT_SECRET')) return fail('Both PAYPAL_CLIENT_ID and PAYPAL_CLIENT_SECRET are needed.');
        const host = String(env.PAYPAL_ENV || 'sandbox').toLowerCase() === 'live' ? 'api-m.paypal.com' : 'api-m.sandbox.paypal.com';
        const r = await timedFetch(`https://${host}/v1/oauth2/token`, { method: 'POST', headers: { Authorization: 'Basic ' + Buffer.from(env.PAYPAL_CLIENT_ID + ':' + env.PAYPAL_CLIENT_SECRET).toString('base64'), 'Content-Type': 'application/x-www-form-urlencoded' }, body: 'grant_type=client_credentials' }, 9000);
        if (r.status === 401) return fail(`PayPal rejected the credentials for ${host}. Check PAYPAL_ENV (sandbox vs live) too.`);
        return r.ok ? ok(`Credentials accepted by ${host}.`) : warn(`PayPal answered HTTP ${r.status}.`);
    });
    add('Payments', 'pay-webhook-secrets', 'Payment webhook secrets', ['PAYMONGO_LIVE_WEBHOOK_SECRET', 'PAYMONGO_TEST_WEBHOOK_SECRET', 'STRIPE_WEBHOOK_SECRET', 'XENDIT_WEBHOOK_TOKEN', 'PAYPAL_WEBHOOK_ID'], async () => {
        const names = ['PAYMONGO_LIVE_WEBHOOK_SECRET', 'PAYMONGO_TEST_WEBHOOK_SECRET', 'STRIPE_WEBHOOK_SECRET', 'XENDIT_WEBHOOK_TOKEN', 'PAYPAL_WEBHOOK_ID'].filter((n) => has(env, n));
        return names.length ? ok(`Set: ${names.join(', ')}. (The provider only confirms these when it sends a real webhook.)`) : skip();
    }, { local: true });

    // ----- Downloads / links -----
    const links = [['OMNIPOS_REPO_URL', 'OmniPOS repository URL'], ['GOOGLE_PRICING_URL', 'Google pricing page'], ['BOOTSTRAP_CLIENT_ZIP_URL', 'Client ZIP download'],
        ['BOOTSTRAP_TERMUX_ARM64', 'Termux download (arm64)'], ['BOOTSTRAP_TERMUX_ARM32', 'Termux download (arm32)'], ['BOOTSTRAP_TERMUX_UNIVERSAL', 'Termux download (universal)'], ['BOOTSTRAP_TERMUX_X86_64', 'Termux download (x86_64)']];
    for (const [name, label] of links) {
        addLive('Links', 'link-' + name, `${label}`, [name], async () => {
            if (!has(env, name)) return skip();
            if (!isPublicHttpUrl(env[name])) return fail('Not a public http(s) URL.');
            const r = await reachable(env[name]);
            if (r.status >= 200 && r.status < 400) return ok(`Reachable (HTTP ${r.status}).`);
            if (r.status === 404) return fail('The link returns 404 (file moved or deleted).');
            return warn(`The link answered HTTP ${r.status}.`);
        });
    }
    addLive('Links', 'link-self', 'RELAY answers on its public URL', ['RELAY_PUBLIC_URL'], async () => {
        const base = has(env, 'RELAY_PUBLIC_URL') ? env.RELAY_PUBLIC_URL : (has(env, 'RENDER_EXTERNAL_URL') ? env.RENDER_EXTERNAL_URL : '');
        if (!base) return skip('No public URL set.');
        if (!isHttpUrl(base)) return fail('Not a valid URL.');
        const r = await timedFetch(String(base).replace(/\/+$/, '') + '/relay/health', {}, 8000);
        const j = await r.json().catch(() => ({}));
        return r.ok && j && j.status === 'ok' ? ok('RELAY answers at its public URL.') : fail(`The public URL did not return RELAY's health check (HTTP ${r.status}). Clients will not reach RELAY.`);
    });

    // ----- Marketplace (format only: these providers have no harmless test call) -----
    add('Marketplace', 'mkt-config', 'Lazada / Shopee / TikTok settings', ['LAZADA_APP_KEY', 'SHOPEE_PARTNER_ID', 'TIKTOK_APP_KEY', 'MARKETPLACE_STATE_SECRET', 'MARKETPLACE_TOKEN_KEY'], async () => {
        const used = ['LAZADA_APP_KEY', 'SHOPEE_PARTNER_ID', 'TIKTOK_APP_KEY'].filter((n) => has(env, n));
        if (!used.length) return skip();
        const probs = [];
        if (!has(env, 'MARKETPLACE_STATE_SECRET')) probs.push('MARKETPLACE_STATE_SECRET is missing');
        if (!has(env, 'MARKETPLACE_TOKEN_KEY')) probs.push('MARKETPLACE_TOKEN_KEY is missing (stored shop tokens cannot be encrypted)');
        else if (String(env.MARKETPLACE_TOKEN_KEY).length < 16) probs.push('MARKETPLACE_TOKEN_KEY is shorter than 16 characters');
        return probs.length ? fail(probs.join('; ')) : ok('Marketplace secrets are present. (App keys are only verified when a shop connects.)');
    }, { local: true });

    await Promise.all(jobs);
    const order = { fail: 0, warn: 1, ok: 2, skip: 3 };
    results.sort((a, b) => a.group.localeCompare(b.group) || (order[a.status] - order[b.status]) || a.title.localeCompare(b.title));
    const summary = { ok: 0, warn: 0, fail: 0, skip: 0, total: results.length };
    for (const r of results) summary[r.status] = (summary[r.status] || 0) + 1;
    return { results, summary };
}

module.exports = { parseEnvText, formatEnv, formatValue, runChecks, isPublicHttpUrl, NAME_RE };
