import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Node's environment, without pulling in @types/node for one variable.
declare const process: { env: Record<string, string | undefined> }

// `just dev-ui` runs this against a local vlpds (VLPDS_URL, default :2620).
const target = process.env.VLPDS_URL ?? 'http://127.0.0.1:2620'
const proxy = Object.fromEntries(
  ['/xrpc', '/oauth', '/metrics', '/internal', '/.well-known'].map((p) => [p, { target, changeOrigin: false }]),
)

export default defineConfig({
  plugins: [react()],
  base: '/',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    // no inline polyfill script: the page CSP is script-src 'self'
    modulePreload: { polyfill: false },
    assetsInlineLimit: 0,
    chunkSizeWarningLimit: 800,
  },
  server: { port: 5620, proxy },
})
