/**
 * coi-serviceworker — enables SharedArrayBuffer (required for DuckDB WASM)
 * on hosts that don't set COOP/COEP headers, like GitHub Pages.
 * Adapted from https://github.com/gzuidhof/coi-serviceworker
 */
self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

async function handleFetch(request) {
  if (request.cache === "only-if-cached" && request.mode !== "same-origin") {
    return;
  }
  const response = await fetch(request).catch((e) => { throw e; });
  if (!response || !response.ok) return response;

  const newHeaders = new Headers(response.headers);
  newHeaders.set("Cross-Origin-Opener-Policy", "same-origin");
  newHeaders.set("Cross-Origin-Embedder-Policy", "credentialless");

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers: newHeaders,
  });
}

self.addEventListener("fetch", (event) => {
  event.respondWith(handleFetch(event.request));
});
