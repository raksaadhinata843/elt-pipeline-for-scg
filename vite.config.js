import { defineConfig } from 'vite';

export default defineConfig({
  server: {
    port: 5000,
    host: '0.0.0.0',
    allowedHosts: true,
    headers: {
      // Required for SharedArrayBuffer (DuckDB WASM)
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'credentialless',
    },
  },
  define: {
    // Expose server-side secret to the WASM client at build/dev time
    'import.meta.env.VITE_MOTHERDUCK_TOKEN': JSON.stringify(
      process.env.MOTHERDUCK_TOKEN || ''
    ),
  },
  optimizeDeps: {
    // Skip pre-bundling — the WASM package must load as-is
    exclude: ['@motherduck/motherduck-duckdb-wasm'],
  },
});
