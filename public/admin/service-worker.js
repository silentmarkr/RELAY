// OmniPOS Relay — Admin Panel Service Worker
//
// LAYUNIN: bigyan lang ng "installable" na app shell (para gumana ang
// "Install App" sa Android/desktop Chrome/Edge) — HINDI para mag-offline
// cache ng data. Lahat ng /relay/admin/api/ calls (pending OTPs, device
// list, allow/revoke/approve) ay dapat LAGING FRESH mula sa network —
// mapanganib mag-serve ng lumang OTP code o lumang device list mula sa
// cache, kaya hindi natin ito ginagalaw dito.

// BUMPED: v3 -> v4 (bagong Backup/Restore card sa itaas ng admin panel —
// nabago ulit ang index.html).
const CACHE_VERSION = 'relay-admin-shell-v4';

// Mga static shell file lang — walang laman na dynamic/sensitive data.
const SHELL_FILES = [
  '/relay/admin/',
  '/relay/admin/index.html',
  '/relay/admin/manifest.json'
];

const ICON_ASSETS = [
  '/relay/admin/icons/icon-192.png',
  '/relay/admin/icons/icon-512.png',
  '/relay/admin/icons/icon-maskable-512.png',
  '/relay/admin/icons/apple-touch-icon.png'
];

const SHELL_ASSETS = [...SHELL_FILES, ...ICON_ASSETS];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_VERSION)
      .then((cache) => cache.addAll(SHELL_ASSETS))
      .catch((err) => console.warn('[Relay Admin SW] Pre-cache warning:', err))
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys.filter((key) => key !== CACHE_VERSION).map((key) => caches.delete(key))
      )
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  const url = new URL(req.url);

  // MAHALAGA: huwag kailanman i-cache o i-intercept ang mga API call —
  // dapat laging dumaan ito nang direkta sa network (kasama ang admin
  // key sa header, OTPs, device allow/revoke, atbp.).
  if (url.pathname.startsWith('/relay/admin/api/')) {
    return;
  }

  // Non-GET requests: bypass service worker entirely.
  if (req.method !== 'GET') {
    return;
  }

  // Ang shell lang (index.html, manifest) ang NETWORK-FIRST, offline
  // fallback na lang ang cache — para laging fresh ang bersyon ng UI
  // kapag may connection, pero puwede pa ring bumukas kung offline.
  if (SHELL_FILES.includes(url.pathname) || req.mode === 'navigate') {
    event.respondWith(
      fetch(req)
        .then((res) => {
          if (res && res.status === 200) {
            const resClone = res.clone();
            caches.open(CACHE_VERSION).then((cache) => cache.put(req, resClone));
          }
          return res;
        })
        .catch(() =>
          caches.match(req).then((cached) => cached || caches.match('/relay/admin/index.html'))
        )
    );
    return;
  }

  // Icons: cache-first (hindi naman ito nagbabago), fallback sa network.
  event.respondWith(
    caches.match(req).then((cached) => {
      const networkFetch = fetch(req)
        .then((res) => {
          if (res && res.status === 200) {
            const resClone = res.clone();
            caches.open(CACHE_VERSION).then((cache) => cache.put(req, resClone));
          }
          return res;
        })
        .catch(() => cached);
      return cached || networkFetch;
    })
  );
});
