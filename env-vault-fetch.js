// env-vault-fetch.js — tinatawag ng env-vault.js (sync, sa boot) para kunin ang encrypted vault sa Postgres.
// Nagpi-print ng JSON (encrypted blob) o "null". Hindi nagpi-print ng anumang secret.
'use strict';
const urls = [process.env.RELAY_DEVICES_DATABASE_URL, process.env.DATABASE_URL].filter(Boolean);
(async () => {
    const { Client } = require('pg');
    for (const url of [...new Set(urls)]) {
        const c = new Client({ connectionString: url, ssl: process.env.PGSSL === 'false' ? false : { rejectUnauthorized: false }, connectionTimeoutMillis: 8000, query_timeout: 8000 });
        try {
            await c.connect();
            const r = await c.query('SELECT value FROM relay_kv_store WHERE key = $1', ['omnipos-relay:env-vault']);
            if (r.rows.length) { process.stdout.write(JSON.stringify(r.rows[0].value)); await c.end().catch(() => {}); return; }
        } catch (_e) { /* subukan ang susunod */ }
        try { await c.end(); } catch (_e) { /* */ }
    }
    process.stdout.write('null');
})().then(() => process.exit(0), () => { process.stdout.write('null'); process.exit(0); });
