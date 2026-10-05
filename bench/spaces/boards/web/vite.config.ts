import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// `npm run dev` against a running `just spaces-boards-ui` (the appview on :2888).
// Sign-in comes back to 127.0.0.1:2888/oauth/callback, so OAuth sign-ins land on the built UI there.
const target = 'http://127.0.0.1:2888'
const proxy = Object.fromEntries(['/api', '/xrpc', '/oauth'].map((p) => [p, { target, changeOrigin: false }]))

export default defineConfig({
  plugins: [react()],
  base: '/',
  build: { outDir: 'dist', emptyOutDir: true, modulePreload: { polyfill: false } },
  server: { port: 5688, proxy },
})
