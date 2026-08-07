// The version below is substituted by the /sw.js route from the styles.css
// fingerprint. Editing the stylesheet therefore changes this file, which is what
// makes the browser reinstall the worker and drop the previous cache.
const VERSION = '__VERSION__';
const CACHE = 'slskdsync-' + VERSION;
const OFFLINE = '/static/offline.html';
const STATIC = [
  '/static/styles.css?v=' + VERSION,
  OFFLINE,
  '/manifest.json',
  // Precached so the typeface survives offline and slow first paints.
  '/static/fonts/hanken-grotesk-400.woff2',
  '/static/fonts/hanken-grotesk-500.woff2',
  '/static/fonts/hanken-grotesk-600.woff2',
  '/static/fonts/hanken-grotesk-700.woff2',
  '/static/fonts/hanken-grotesk-800.woff2',
  '/static/fonts/ibm-plex-mono-400.woff2',
  '/static/fonts/ibm-plex-mono-500.woff2',
  '/static/fonts/ibm-plex-mono-600.woff2',
];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(STATIC)).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);
  // Network-first for API and page routes
  if (url.pathname.startsWith('/api/') || e.request.method !== 'GET') {
    return;
  }
  // Cache-first for static assets
  if (url.pathname.startsWith('/static/')) {
    e.respondWith(
      caches.match(e.request).then(cached => cached || fetch(e.request))
    );
    return;
  }
  // Network-first for pages, falling back to the cache and then to the offline
  // page. Without that last step an uncached page resolved to undefined, and
  // respondWith turns that into the browser's own network-error screen.
  e.respondWith(
    fetch(e.request).catch(() =>
      caches.match(e.request).then(cached => cached || caches.match(OFFLINE))
    )
  );
});
