'use strict';
// ===================================================================
// DATABASE PROVIDER CATALOG for Admin -> Database Migration
// ===================================================================
// RELAY stores its data in PostgreSQL (JSONB, BYTEA, PL/pgSQL triggers, sequences), so a database can be migrated to
// ANY real PostgreSQL service: serverless, developer platforms, the big clouds, specialty hosts, or your own server.
// MySQL / MongoDB / SQLite / SQL Server / Oracle are different database engines and cannot hold RELAY's data.
//
// Prices are APPROXIMATE list prices in USD per month as published in October 2026 (sources below). Providers change
// prices and free tiers often — always confirm on the provider's own pricing page (pricingUrl) before you buy.
// Entries are ordered from the cheapest paid entry price to the most expensive; every provider also lists its
// "pre-tier" (free tier / trial / credits) first when it has one.
//
// Sources: bytebase.com/blog/postgres-hosting-options-pricing-comparison (Oct 12 2026), nearbase.dev/blog/best-postgres-hosting,
// selfhost.dev/blog/managed-postgresql-comparison-2026 — cross-checked against each other; where they differ the note says so.
const CATALOG_CHECKED_AT = '2026-10-31';

const PROVIDERS = [
    {
        id: 'northflank', name: 'Northflank', group: 'Developer platform', sortUSD: 2.7,
        hosts: [], pricingUrl: 'https://northflank.com/pricing',
        free: { available: true, summary: 'Sandbox tier with one always-on database' },
        tiers: [
            { name: 'Sandbox (pre-tier)', usd: 0, specs: '1 managed database', note: 'Free sandbox' },
            { name: 'nf-compute-10', usd: 2.7, specs: 'Entry compute', note: 'Storage billed separately (~$0.15/GB-month); egress ~$0.06/GB' }
        ],
        urlHelp: 'Database page → Connection details → copy the PostgreSQL connection string (external/public address).',
        caveats: ['Only one managed region in Asia (Asia East).']
    },
    {
        id: 'heroku', name: 'Heroku Postgres', group: 'Developer platform', sortUSD: 5, sslNoVerify: true,
        hosts: ['\\.compute-1\\.amazonaws\\.com$', '\\.compute\\.amazonaws\\.com$'], pricingUrl: 'https://www.heroku.com/postgres',
        free: { available: false, summary: 'No free tier' },
        tiers: [
            { name: 'Essential-0', usd: 5, specs: '1 GB storage', note: 'US/EU only' },
            { name: 'Standard', usd: 50, specs: 'Production tier (starts at $50)', note: '' }
        ],
        urlHelp: 'Heroku CLI: heroku pg:credentials:url -a <app> (or the DATABASE_URL config var).',
        caveats: ['Uses a private certificate: RELAY switches sslmode to no-verify automatically.']
    },
    {
        id: 'planetscale', name: 'PlanetScale (Postgres)', group: 'Serverless / branching', sortUSD: 5,
        hosts: ['\\.psdb\\.cloud$', 'planetscale'], pricingUrl: 'https://planetscale.com/pricing',
        free: { available: false, summary: 'No free tier' },
        tiers: [
            { name: 'PS-5 single node', usd: 5, specs: '1/16 vCPU, 512 MB RAM, 10 GB storage', note: 'Storage past 10 GB ~$0.125/GB' },
            { name: 'PS-5 HA', usd: 15, specs: 'High availability', note: '' },
            { name: 'PS-40', usd: 28, specs: 'Up to ~$83 depending on setup', note: '' },
            { name: 'PS-2560 HA', usd: 4529, specs: 'Largest published size', note: '' }
        ],
        urlHelp: 'Choose the PostgreSQL database type (not the MySQL/Vitess one) and copy its connection string.',
        caveats: ['PlanetScale MySQL is a different engine and is NOT supported. Per-branch billing; PgBouncer costs extra.']
    },
    {
        id: 'aiven', name: 'Aiven for PostgreSQL', group: 'Managed (multi-cloud)', sortUSD: 5, sslNoVerify: true,
        hosts: ['\\.aivencloud\\.com$'], pricingUrl: 'https://aiven.io/pricing',
        free: { available: true, summary: 'Free plan: 1 CPU, 1 GB RAM, small SSD; may power off when inactive', storageMB: 1024 },
        tiers: [
            { name: 'Free (pre-tier)', usd: 0, specs: '1 CPU, 1 GB RAM, ~1–5 GB SSD', note: 'No cloud/region choice; powers off when idle' },
            { name: 'Developer', usd: 5, specs: '8 GB storage cap', note: 'No connection pooling' },
            { name: 'Hobbyist', usd: 12, specs: '', note: 'Sources disagree on exact limits' },
            { name: 'Startup', usd: 75, specs: '99.99% SLA, region choice', note: '' },
            { name: 'Professional / Business', usd: 250, specs: 'Single node ~$250, HA ~$295 (one source)', note: 'Pricing varies by cloud and region' }
        ],
        urlHelp: 'Service overview → Service URI (use the one ending with ?sslmode=require).',
        caveats: ['Uses a private CA: RELAY switches sslmode to no-verify automatically.']
    },
    {
        id: 'render', name: 'Render Postgres', group: 'Developer platform', sortUSD: 6,
        hosts: ['\\.render\\.com$', '^dpg-[a-z0-9]+-a(\\.|$)'], pricingUrl: 'https://render.com/pricing',
        free: { available: true, summary: 'Free instance, 256 MB; deleted after about 30 days of inactivity', storageMB: 256 },
        tiers: [
            { name: 'Free (pre-tier)', usd: 0, specs: '256 MB', note: 'Expires if unused (~30 days) — not for production' },
            { name: 'Basic', usd: 6, specs: '256 MB RAM entry plan', note: 'Higher Basic/Pro/Accelerated plans cost more; HA only on Pro and Accelerated' }
        ],
        urlHelp: 'Database → Connections → External Database URL (use the external one, not the internal one).',
        caveats: ['Read replicas bill at full primary price.']
    },
    {
        id: 'nearbase', name: 'Nearbase', group: 'Specialty', sortUSD: 6,
        hosts: ['nearbase'], pricingUrl: 'https://nearbase.dev',
        free: { available: false, summary: 'Not listed' },
        tiers: [{ name: 'Instance', usd: 6, specs: '1 vCPU, 2 GB (about $13 with the 20 GB minimum storage)', note: 'No published backup-retention or SLA details' }],
        urlHelp: 'Copy the PostgreSQL connection string from the instance page.', caveats: []
    },
    {
        id: 'crunchy', name: 'Crunchy Bridge', group: 'Managed (production-focused)', sortUSD: 9,
        hosts: ['\\.db\\.postgresbridge\\.com$', 'postgresbridge'], pricingUrl: 'https://www.crunchydata.com/pricing',
        free: { available: false, summary: 'No free tier; no scale-to-zero' },
        tiers: [
            { name: 'Hobby-0', usd: 9, specs: 'Non-production', note: '$9 minimum even when idle' },
            { name: 'Standard-4', usd: 70, specs: 'Production tier', note: '' }
        ],
        urlHelp: 'Cluster → Connection → copy the connection URL for the application role (create an app role first).', caveats: []
    },
    {
        id: 'prisma', name: 'Prisma Postgres', group: 'Serverless / per-operation', sortUSD: 10,
        hosts: ['db\\.prisma\\.io$', 'prisma-data\\.net$'], pricingUrl: 'https://www.prisma.io/pricing',
        free: { available: true, summary: 'Free: 200k operations, ~1 GB storage', storageMB: 1024 },
        tiers: [
            { name: 'Free (pre-tier)', usd: 0, specs: '200k operations, 1.01 GB', note: '' },
            { name: 'Starter', usd: 10, specs: '1M operations, 10 GB', note: '' },
            { name: 'Pro', usd: 49, specs: '10M operations, 50 GB', note: '' },
            { name: 'Business', usd: 129, specs: '50M operations, 100 GB', note: '' }
        ],
        urlHelp: 'Use the direct TCP connection string (postgres://…@db.prisma.io:5432/…), not the Accelerate / prisma+postgres:// URL.',
        caveats: ['Billed per operation: a full migration copy counts as many operations.']
    },
    {
        id: 'gcp', name: 'Google Cloud SQL', group: 'Big cloud', sortUSD: 11.32,
        hosts: ['\\.cloudsql\\.', 'googleusercontent'], pricingUrl: 'https://cloud.google.com/sql/pricing',
        free: { available: true, summary: '$300 credit valid 90 days (new accounts)' },
        tiers: [
            { name: 'Credits (pre-tier)', usd: 0, specs: '$300 / 90 days', note: 'New accounts only' },
            { name: 'db-f1-micro', usd: 11.32, specs: '0.2 vCPU, 0.6 GiB', note: '' },
            { name: 'db-n1-standard-2', usd: 122.49, specs: '2 vCPU, 7.5 GiB', note: '' },
            { name: 'db-n1-standard-16', usd: 980.03, specs: '16 vCPU, 60 GiB', note: 'HA roughly doubles cost' }
        ],
        urlHelp: 'Use the instance public IP; add RELAY\'s outbound IP to "Authorized networks" first.',
        caveats: ['Needs an IP allow-list entry for the host that runs RELAY.']
    },
    {
        id: 'azure', name: 'Azure Database for PostgreSQL', group: 'Big cloud', sortUSD: 12,
        hosts: ['\\.postgres\\.database\\.azure\\.com$'], pricingUrl: 'https://azure.microsoft.com/pricing/details/postgresql/flexible-server/',
        free: { available: true, summary: '12 months free tier (new accounts)' },
        tiers: [
            { name: '12-month free (pre-tier)', usd: 0, specs: 'New accounts', note: '' },
            { name: 'Burstable B1ms', usd: 14.6, specs: '1 vCPU, 2 GiB', note: 'Another source lists ~$12; burstable is not for production' },
            { name: 'General Purpose 4 vCPU', usd: 99.28, specs: '4 vCPU, 16 GiB', note: '' },
            { name: 'General Purpose 32 vCPU', usd: 794.24, specs: '32 vCPU, 128 GiB', note: 'Zone-redundant HA doubles cost' }
        ],
        urlHelp: 'Server → Connection strings. The user name may need the form user@servername. Allow RELAY\'s IP in Networking.',
        caveats: ['Storage, HA and bandwidth are billed separately.']
    },
    {
        id: 'rds', name: 'Amazon RDS for PostgreSQL', group: 'Big cloud', sortUSD: 12.41, sslNoVerify: true,
        hosts: ['\\.rds\\.amazonaws\\.com$'], pricingUrl: 'https://aws.amazon.com/rds/postgresql/pricing/',
        free: { available: true, summary: '12-month free tier (750 h micro) and/or $200 credits for new accounts' },
        tiers: [
            { name: 'Free tier / credits (pre-tier)', usd: 0, specs: '750 h micro or $200 credits', note: 'New accounts only' },
            { name: 'db.t4g.micro', usd: 12.41, specs: '2 vCPU, 1 GiB', note: 'About $18 in some regions; compute only' },
            { name: 'db.m6g.large', usd: 99.28, specs: '2 vCPU, 8 GiB', note: '' },
            { name: 'db.m6g.4xlarge', usd: 794.24, specs: '16 vCPU, 64 GiB', note: 'Multi-AZ doubles instance cost' }
        ],
        urlHelp: 'RDS console → instance → Endpoint + port. Make it publicly accessible and allow RELAY\'s IP in the security group.',
        caveats: ['Storage, IOPS and transfer are billed on top. RELAY switches sslmode to no-verify (RDS CA).']
    },
    {
        id: 'digitalocean', name: 'DigitalOcean Managed Databases', group: 'Managed (VPS provider)', sortUSD: 15.15, sslNoVerify: true,
        hosts: ['\\.db\\.ondigitalocean\\.com$'], pricingUrl: 'https://www.digitalocean.com/pricing/managed-databases',
        free: { available: false, summary: 'No free tier' },
        tiers: [
            { name: 'Basic', usd: 15.15, specs: '1 vCPU, 1 GB RAM, 10 GB disk', note: '' },
            { name: 'Growth', usd: 60.9, specs: '2 vCPU, 4 GB RAM, 60 GB disk', note: '' },
            { name: 'Production', usd: 244.35, specs: '6 vCPU, 16 GB RAM, 290 GB disk', note: 'A standby roughly doubles the bill' }
        ],
        urlHelp: 'Cluster → Connection details → Public network → connection string. Add RELAY\'s IP under Trusted Sources.',
        caveats: ['Disks can grow but not shrink. RELAY switches sslmode to no-verify (private CA).']
    },
    {
        id: 'neon', name: 'Neon', group: 'Serverless Postgres', sortUSD: 15,
        hosts: ['\\.neon\\.tech$', '\\.neon\\.build$'], pricingUrl: 'https://neon.com/pricing',
        free: { available: true, summary: 'Always free: 0.5 GB per project, 100 CU-hours/month', storageMB: 512 },
        tiers: [
            { name: 'Free (pre-tier)', usd: 0, specs: '0.5 GB per project, 100 CU-hours/month', note: 'Scales to zero after 5 minutes idle' },
            { name: 'Launch', usd: 15, specs: 'Usage-based: $0.106/CU-hour + $0.35/GB-month', note: '~$15/month typical for light workloads' },
            { name: 'Scale', usd: 80, specs: 'Usage-based: $0.222/CU-hour', note: '1 CU running 24/7 is roughly $80; adds SOC 2/HIPAA' }
        ],
        urlHelp: 'Neon console → Connect → copy the connection string (the "pooled" one is fine; the direct one is slightly faster for migration).',
        caveats: ['Storage past 0.5 GB on Free is not available — upgrade first if your data is bigger.']
    },
    {
        id: 'railway', name: 'Railway', group: 'Developer platform', sortUSD: 5,
        hosts: ['\\.rlwy\\.net$', '\\.railway\\.app$', '\\.railway\\.internal$'], pricingUrl: 'https://railway.com/pricing',
        free: { available: true, summary: '30-day trial with $5 credit, then paid' },
        tiers: [
            { name: 'Trial (pre-tier)', usd: 0, specs: '$5 credit', note: '' },
            { name: 'Hobby', usd: 5, specs: 'Usage-based, $5 minimum', note: 'Always-on Postgres often reported $30–$80/month' },
            { name: 'Pro', usd: 20, specs: 'Usage-based, $20 minimum', note: '' }
        ],
        urlHelp: 'Postgres service → Variables → DATABASE_PUBLIC_URL (not the .internal one).',
        caveats: ['Single-node by default; no published backup guarantee.']
    },
    {
        id: 'tigerdata', name: 'TigerData (Timescale)', group: 'Specialty (time-series)', sortUSD: 14.6,
        hosts: ['\\.tsdb\\.cloud$', '\\.timescaledb\\.io$'], pricingUrl: 'https://www.tigerdata.com/pricing',
        free: { available: true, summary: 'Free: 750 MB storage (read-only past the cap)', storageMB: 750 },
        tiers: [
            { name: 'Free (pre-tier)', usd: 0, specs: '750 MB, forks, pgvector', note: '' },
            { name: 'Smallest paid compute', usd: 14.6, specs: 'About $0.02/hour plus storage used', note: 'HA optional, extra cost' }
        ],
        urlHelp: 'Service → Connection info → copy the service URL.', caveats: []
    },
    {
        id: 'supabase', name: 'Supabase', group: 'Serverless / BaaS', sortUSD: 25,
        hosts: ['\\.supabase\\.co$', '\\.supabase\\.com$', 'pooler\\.supabase\\.com$'], pricingUrl: 'https://supabase.com/pricing',
        free: { available: true, summary: 'Free: 500 MB, project pauses after a week of inactivity', storageMB: 500 },
        tiers: [
            { name: 'Free (pre-tier)', usd: 0, specs: '500 MB, shared compute', note: 'Pauses after ~1 week of inactivity' },
            { name: 'Pro', usd: 25, specs: 'Includes Micro compute, 8 GB disk', note: 'Usage-based overages (egress, compute, disk)' },
            { name: 'Pro + Medium compute', usd: 75, specs: '', note: '' },
            { name: 'Team', usd: 599, specs: 'Governance features', note: 'Enterprise: custom' }
        ],
        urlHelp: 'Project → Connect → use the "Session pooler" string (port 5432) — the direct host is IPv6-only on many networks. Do not use the transaction pooler (port 6543) for migration.',
        caveats: ['Free projects pause when inactive — RELAY would stop working.']
    },
    {
        id: 'aurora', name: 'Amazon Aurora PostgreSQL', group: 'Big cloud', sortUSD: 59.86, sslNoVerify: true,
        hosts: ['\\.cluster-[a-z0-9]+\\.[a-z0-9-]+\\.rds\\.amazonaws\\.com$'], pricingUrl: 'https://aws.amazon.com/rds/aurora/pricing/',
        free: { available: true, summary: 'Free Plan: serverless up to 4 ACU / 1 GiB per cluster (≈6 months or until credits end) + $200 credits' },
        tiers: [
            { name: 'Free plan / credits (pre-tier)', usd: 0, specs: 'Serverless up to 4 ACU, 1 GiB', note: 'New accounts' },
            { name: 'Serverless v2', usd: null, specs: '$0.12/ACU-hour + $0.10/GB-month storage', note: 'Usage-based; idle costs only storage' },
            { name: 'db.t4g.medium', usd: 59.86, specs: '2 vCPU, 4 GiB', note: '' },
            { name: 'db.r6g.large', usd: 211.7, specs: '2 vCPU, 16 GiB', note: '' },
            { name: 'db.r6g.4xlarge', usd: 1693.6, specs: '16 vCPU, 128 GiB', note: 'I/O charges can rival compute' }
        ],
        urlHelp: 'Use the cluster (writer) endpoint; make it publicly accessible and allow RELAY\'s IP in the security group.',
        caveats: ['Overkill for small apps; six billing dimensions.']
    },
    {
        id: 'alloydb', name: 'Google AlloyDB', group: 'Big cloud', sortUSD: null,
        hosts: ['alloydb'], pricingUrl: 'https://cloud.google.com/alloydb/pricing',
        free: { available: true, summary: '30-day trial plus $300 credits' },
        tiers: [
            { name: 'Trial (pre-tier)', usd: 0, specs: '30 days / $300 credits', note: '' },
            { name: 'Usage-based', usd: null, specs: '~$0.066/vCPU-hour + ~$0.0112/GiB-hour', note: 'No pause option: delete the cluster to stop billing' }
        ],
        urlHelp: 'Use the instance IP (public IP enabled) and allow RELAY\'s IP.', caveats: []
    },
    {
        id: 'selfhosted', name: 'Your own PostgreSQL server (VPS, Docker, on-prem)', group: 'Self-hosted', sortUSD: null,
        hosts: [], pricingUrl: '',
        free: { available: true, summary: 'Software is free; you pay only for the server and its backups' },
        tiers: [{ name: 'Self-managed', usd: null, specs: 'PostgreSQL 12 or newer', note: 'You handle backups, updates, security and uptime' }],
        urlHelp: 'postgresql://user:password@your-host:5432/dbname?sslmode=require — the host must be reachable from where RELAY runs.',
        caveats: ['Make sure the server accepts TLS connections and the firewall allows RELAY\'s IP.']
    }
];

// Engines that look like databases but are NOT PostgreSQL — shown to the admin as "cannot be used".
const UNSUPPORTED_ENGINES = [
    { name: 'MySQL / MariaDB (incl. PlanetScale MySQL, Aurora MySQL)', schemes: ['mysql:', 'mysql2:', 'mariadb:'] },
    { name: 'MongoDB / DocumentDB', schemes: ['mongodb:', 'mongodb+srv:'] },
    { name: 'SQL Server / Azure SQL', schemes: ['sqlserver:', 'mssql:'] },
    { name: 'SQLite / Turso (libSQL)', schemes: ['sqlite:', 'libsql:', 'file:'] },
    { name: 'Redis / Valkey', schemes: ['redis:', 'rediss:'] },
    { name: 'Oracle', schemes: ['oracle:'] }
];
// Speak PostgreSQL on the wire but lack the system catalogs RELAY's migration reads (so they cannot be used).
const PARTIAL_POSTGRES_PATTERNS = [/cockroach/i, /yugabyte/i, /questdb/i, /materialize/i];

function hostOf(url) {
    try { return new URL(String(url)).hostname.toLowerCase(); } catch (_e) { return ''; }
}
// Provider guessed from the host name; null when unknown (any other PostgreSQL still works).
function detectProvider(url) {
    const host = hostOf(url);
    if (!host) return null;
    // Most specific patterns first (Aurora before RDS; Supabase pooler hosts).
    const order = ['aurora', 'rds'].concat(PROVIDERS.map((p) => p.id).filter((id) => id !== 'aurora' && id !== 'rds'));
    for (const id of order) {
        const p = PROVIDERS.find((x) => x.id === id);
        if (!p) continue;
        for (const pattern of p.hosts) {
            try { if (new RegExp(pattern, 'i').test(host)) return p; } catch (_e) { /* bad pattern */ }
        }
    }
    return null;
}
function unsupportedEngineForUrl(url) {
    let protocol = '';
    try { protocol = new URL(String(url).trim()).protocol.toLowerCase(); } catch (_e) { return null; }
    return UNSUPPORTED_ENGINES.find((e) => e.schemes.includes(protocol)) || null;
}

// Some providers sign their certificates with a private CA that Node does not trust. RELAY already accepts such
// certificates for its own env URLs (encrypted but not identity-verified); for the new URL we do the same by
// switching sslmode to no-verify, so the admin does not have to download CA files. Returns { url, notes }.
function prepareTargetUrl(url) {
    const raw = String(url || '').trim();
    const notes = [];
    const provider = detectProvider(raw);
    let out = raw;
    try {
        const u = new URL(raw);
        const mode = (u.searchParams.get('sslmode') || '').toLowerCase();
        if (provider && provider.sslNoVerify && ['', 'require', 'prefer', 'verify-ca', 'verify-full'].includes(mode)) {
            u.searchParams.set('sslmode', 'no-verify');
            out = u.toString();
            notes.push(`${provider.name} uses a private certificate, so sslmode was set to no-verify (the connection is still encrypted).`);
        }
    } catch (_e) { /* parsePgUrl reports invalid URLs */ }
    return { url: out, provider, notes };
}

// Turn a low-level connection error into a plain next step.
function explainConnectError(err, provider) {
    const msg = String((err && err.message) || err || '');
    const code = (err && err.code) || '';
    const hint = (t) => ` Hint: ${t}`;
    if (code === 'ENOTFOUND' || /getaddrinfo/i.test(msg)) return hint('the host name was not found — re-copy the connection string from the provider (no extra spaces).');
    if (code === 'ECONNREFUSED') return hint('the server refused the connection — check the port and that the database is running (not paused).');
    if (code === 'ETIMEDOUT' || /timeout|timed out/i.test(msg)) return hint('no answer from the server — it may be paused, waking up, or blocking RELAY\'s IP' + (provider && /rds|aurora|azure|gcp|digitalocean|aiven/.test(provider.id) ? ' (add RELAY\'s outbound IP to the allow-list / security group / trusted sources)' : '') + '.');
    if (code === '28P01' || /password authentication failed/i.test(msg)) return hint('wrong user name or password.');
    if (code === '28000' || /no pg_hba.conf entry/i.test(msg)) return hint('the server does not allow this IP or does not accept non-SSL connections — allow RELAY\'s IP and keep sslmode=require.');
    if (code === '3D000' || /database .* does not exist/i.test(msg)) return hint('that database name does not exist on the server — create it first or fix the name at the end of the URL.');
    if (/self.signed|unable to verify|certificate/i.test(msg)) return hint('certificate problem — add ?sslmode=no-verify to the URL.');
    if (/SSL.*(required|off)|does not support SSL/i.test(msg)) return hint('this server has TLS turned off or required — adjust ?sslmode= in the URL.');
    if (/too many connections|remaining connection slots/i.test(msg)) return hint('the new database has no free connections — close other clients or use the pooled URL.');
    return '';
}

// Compare the data size with the provider's smallest free plan so the admin is warned BEFORE the copy fails or the free
// plan becomes read-only. Returns a warning string or null.
function storageFitWarning(provider, totalBytes) {
    if (!provider || !provider.free || !provider.free.storageMB || !totalBytes) return null;
    const limit = provider.free.storageMB * 1024 * 1024;
    // Indexes and system overhead make the new database larger than the raw data: keep a 15% margin.
    if (totalBytes * 1.15 > limit) {
        const mb = (n) => `${Math.round(n / 1024 / 1024)} MB`;
        return `${provider.name}'s free plan allows about ${provider.free.storageMB} MB, but your data is about ${mb(totalBytes)}. Pick a paid plan on the new database first, or the copy can fail or the database may become read-only.`;
    }
    return null;
}

// Catalog for the admin page: cheapest paid entry first, entries without a fixed price (usage-based/self-hosted) last.
function listProviders() {
    const rows = PROVIDERS.map((p) => ({
        id: p.id, name: p.name, group: p.group, sortUSD: p.sortUSD, pricingUrl: p.pricingUrl,
        free: p.free, tiers: p.tiers, urlHelp: p.urlHelp, caveats: p.caveats || [],
        sslNoVerify: !!p.sslNoVerify, freeStorageMB: (p.free && p.free.storageMB) || null
    }));
    rows.sort((a, b) => {
        const av = a.sortUSD == null ? Infinity : a.sortUSD;
        const bv = b.sortUSD == null ? Infinity : b.sortUSD;
        return av === bv ? a.name.localeCompare(b.name) : av - bv;
    });
    return {
        checkedAt: CATALOG_CHECKED_AT,
        disclaimer: 'Approximate list prices in USD per month, collected October 2026. Providers change prices and free tiers often — confirm on the provider\'s pricing page before buying.',
        providers: rows,
        unsupported: UNSUPPORTED_ENGINES.map((e) => e.name).concat(['CockroachDB, YugabyteDB and other "PostgreSQL-compatible" engines (they lack the system catalogs RELAY needs)'])
    };
}

module.exports = {
    PROVIDERS, UNSUPPORTED_ENGINES, PARTIAL_POSTGRES_PATTERNS, CATALOG_CHECKED_AT,
    detectProvider, unsupportedEngineForUrl, prepareTargetUrl, explainConnectError, storageFitWarning, listProviders
};
