// Ports, URLs and test-only secrets of the harness stack (docker-compose.yml,
// run.sh). Every URL is http://localhost:<port>, the same string inside the
// reference PDS containers (see the socat sidecars) and on the host.
import { mkdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

const int = (k, d) => Number(process.env[k] ?? d)

export const HERE = fileURLToPath(new URL('..', import.meta.url))
export const OUT = process.env.OUT ?? `${HERE}out/`
mkdirSync(OUT, { recursive: true })

export const PORTS = {
  plc: int('PLC_PORT', 2860),
  refA: int('REF_A_PORT', 2861),
  refB: int('REF_B_PORT', 2862),
  vlpds: int('VLPDS_PORT', 2863), // single node, or the cluster's load balancer
  vlpdsNodes: [2864, 2865, 2866], // cluster mode
  vlpdsPeers: [2884, 2885, 2886],
  minio: int('MINIO_PORT', 2868),
  syncer: 2870, // notify receivers: 2870..2873
  syncerProxy: 2874, // fault proxies in front of them: 2874..2877
  hostProxy: { 'ref-a': 2880, 'ref-b': 2881, vlpds: 2882 }, // fault proxies in front of space hosts
}

// STORE=r2 puts vlpds on a real S3-compatible bucket (README.md "Real R2") under
// R2_PREFIX; the endpoint, bucket and keys come from the env file run.sh loads.
export const STORE = process.env.STORE ?? 'minio'
export const R2 = STORE === 'r2'
  ? {
      endpoint: process.env.VLPDS_BENCH_ENDPOINT,
      bucket: process.env.VLPDS_BENCH_BUCKET,
      prefix: process.env.R2_PREFIX,
      // vlpds_object_store_requests_total across every node and restart; past this the driver kills vlpds and exits 4
      opsLimit: int('R2_OPS_LIMIT', 45_000),
    }
  : null

export const URLS = {
  plc: `http://127.0.0.1:${PORTS.plc}`,
  refA: `http://localhost:${PORTS.refA}`,
  refB: `http://localhost:${PORTS.refB}`,
  vlpds: `http://localhost:${PORTS.vlpds}`,
  minio: `http://127.0.0.1:${PORTS.minio}`,
}

// Test-only values, generated for this stack; nothing here is a real secret.
export const REF_ADMIN_PASSWORD = 'spaces-e2e-admin'
export const REF_ROTATION_KEYS = {
  'ref-a': '5c4e2b1a09f8e7d6c5b4a39281706f5e4d3c2b1a0918f7e6d5c4b3a291807f6e',
  'ref-b': '6d5f3c2b1a09f8e7d6c5b4a39281706f5e4d3c2b1a0918f7e6d5c4b3a291807f',
}
export const VLPDS_ROTATION_KEY = 'c068fa83769c250f272fd0c393fb68717f2731dc7003c5d6afdd51728da32440'
// vlpds's --dev-mode default admin token (src/main.rs); only accepted in dev mode
export const VLPDS_ADMIN_TOKEN = process.env.VLPDS_ADMIN_TOKEN ?? 'dev-admin-token'

export const HOSTS = {
  'ref-a': { key: 'ref-a', kind: 'ref', url: URLS.refA, handleDomain: 'test' },
  'ref-b': { key: 'ref-b', kind: 'ref', url: URLS.refB, handleDomain: 'test' },
  vlpds: { key: 'vlpds', kind: 'vlpds', url: URLS.vlpds, handleDomain: 'vlpds.test' },
}

export const SPACE_TYPE = 'com.example.group'
export const COLL = 'com.example.spaceRecord'
export const COLL_ALT = 'com.example.spaceNote'

export const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a)
