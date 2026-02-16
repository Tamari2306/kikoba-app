const CACHE_NAME = "kikoba-v1";
const urlsToCache = [
  "/",
  "/dashboard",
  "/static/icons/favicon.ico",
  "/static/icons/apple-touch-icon.png",
  "/static/css/style.css"
];


self.addEventListener("install", event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(urlsToCache))
  );
});

self.addEventListener("fetch", event => {
  event.respondWith(
    caches.match(event.request)
      .then(response => response || fetch(event.request))
  );
});
