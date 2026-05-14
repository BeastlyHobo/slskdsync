const CACHE = 'slskdsync-v1';
const STATIC = [
  '/static/styles.css',
  '/manifest.json',
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
  // Network-first for everything else (pages)
  e.respondWith(
    fetch(e.request).catch(() => caches.match(e.request))
  );
});
