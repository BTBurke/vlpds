// The vlpds under test, run by the driver so the fault sim can kill it: one
// node on MinIO (or --memory, or a real R2 bucket with STORE=r2), or a 3-node
// cluster behind a small round-robin HTTP balancer on the public port, as in
// tests/E2E.md. Logs go to out/vlpds-*.log. Dev mode only, with the harness's
// test keys.
import { spawn } from 'node:child_process'
import { createWriteStream, mkdirSync, writeFileSync } from 'node:fs'
import http from 'node:http'
import { OUT, PORTS, R2, URLS, VLPDS_ROTATION_KEY, log } from './env.mjs'
import { hostUrl, sleep } from './http.mjs'

const BIN = process.env.VLPDS_BIN

function cleanEnv(extra = {}) {
  const env = {}
  for (const [k, v] of Object.entries(process.env)) if ((!k.startsWith('VLPDS_') || k === 'VLPDS_BIN') && !k.startsWith('AWS_')) env[k] = v
  // R2 keys go in by env, never argv: the log's start line prints the args
  if (R2) Object.assign(env, { VLPDS_S3_ACCESS_KEY: process.env.AWS_ACCESS_KEY_ID, VLPDS_S3_SECRET_KEY: process.env.AWS_SECRET_ACCESS_KEY })
  return { ...env, RUST_LOG: process.env.VLPDS_RUST_LOG ?? 'info', ...extra }
}

/** vlpds_object_store_requests_total by op from one node's /metrics text. */
function storeOpsOf(txt) {
  const m = new Map()
  for (const line of txt.split('\n')) {
    if (!line.startsWith('vlpds_object_store_requests_total{')) continue
    const op = /op="([^"]+)"/.exec(line)?.[1] ?? '?'
    m.set(op, (m.get(op) ?? 0) + Number(line.slice(line.lastIndexOf(' ') + 1)))
  }
  return m
}

class Node {
  constructor(i, opts) {
    this.i = i
    this.opts = opts
    this.port = opts.cluster ? PORTS.vlpdsNodes[i] : PORTS.vlpds
    this.url = `http://127.0.0.1:${this.port}`
    this.proc = null
    this.starts = 0
    // bucket ops of earlier processes (a restart zeroes the counters), and the last sample of this one
    this.carried = new Map()
    this.lastOps = new Map()
  }

  args() {
    const o = this.opts
    const a = [
      '--dev-mode',
      '--spaces',
      '--no-rate-limits',
      '--listen', `127.0.0.1:${this.port}`,
      '--public-url', URLS.vlpds,
      '--handle-domain', 'vlpds.test',
      '--service-did', 'did:web:vlpds.test',
      '--plc-url', URLS.plc,
      '--plc-mode', 'directory',
      '--plc-rotation-key', VLPDS_ROTATION_KEY,
      '--memory-budget-mb', process.env.VLPDS_MEMORY_MB ?? '6144',
    ]
    if (o.memory) a.push('--memory')
    else if (R2) {
      a.push('--s3-endpoint', R2.endpoint, '--s3-bucket', R2.bucket, '--s3-region', 'auto', '--prefix', o.prefix)
      // fewer shards than the default 64: each one polls and checkpoints on its own, and idle requests are billed
      if (!o.cluster) a.push('--shards', process.env.R2_SHARDS ?? '16')
    } else a.push('--s3-endpoint', URLS.minio, '--s3-bucket', 'vlpds', '--prefix', o.prefix)
    if (o.cluster) {
      a.push(
        '--node-id', `n${this.i}`,
        '--peer-listen', `127.0.0.1:${PORTS.vlpdsPeers[this.i]}`,
        '--advertise-url', `https://127.0.0.1:${PORTS.vlpdsPeers[this.i]}`,
        '--peer-tls-dir', `${OUT}peer-tls`,
        // 3 s fails over fast on MinIO; on R2 the 3 s control-plane deadline it implies (min(TTL, 5 s)) is too tight for R2's tail
        '--lease-ttl-ms', String(o.leaseTtlMs ?? (R2 ? 10_000 : 3000)),
        '--shards', '16',
        '--workers', '2',
      )
    }
    return [...a, ...(o.extra ?? [])]
  }

  async start() {
    this.starts++
    for (const [k, v] of this.lastOps) this.carried.set(k, (this.carried.get(k) ?? 0) + v)
    this.lastOps = new Map()
    mkdirSync(OUT, { recursive: true })
    const logf = createWriteStream(`${OUT}vlpds-n${this.i}.log`, { flags: 'a' })
    logf.write(`\n==== start ${this.starts} ${new Date().toISOString()} ${BIN} ${this.args().join(' ')}\n`)
    this.proc = spawn(BIN, this.args(), { env: cleanEnv(), stdio: ['ignore', 'pipe', 'pipe'] })
    this.proc.stdout.pipe(logf)
    this.proc.stderr.pipe(logf)
    this.exited = new Promise((r) => this.proc.once('exit', (code, sig) => r({ code, sig })))
    await this.waitHealthy()
  }

  async waitHealthy(ms = 60_000) {
    const t0 = Date.now()
    while (Date.now() - t0 < ms) {
      if (this.proc.exitCode !== null) throw new Error(`vlpds n${this.i} exited (${this.proc.exitCode}); see ${OUT}vlpds-n${this.i}.log`)
      try {
        const r = await fetch(`${this.url}/xrpc/_health`)
        if (r.ok) return
      } catch {}
      await sleep(150)
    }
    throw new Error(`vlpds n${this.i} not healthy after ${ms} ms`)
  }

  alive() {
    return this.proc && this.proc.exitCode === null && this.proc.signalCode === null
  }

  async kill(signal = 'SIGKILL') {
    if (!this.alive()) return
    this.proc.kill(signal)
    await Promise.race([this.exited, sleep(30_000)])
  }
}

export class Vlpds {
  constructor({ cluster = false, memory = false, prefix = R2 ? R2.prefix : `spaces-${Date.now().toString(36)}`, extra = [] } = {}) {
    if (R2 && !(R2.endpoint && R2.bucket && R2.prefix && process.env.AWS_ACCESS_KEY_ID)) throw new Error('STORE=r2 needs VLPDS_BENCH_ENDPOINT, VLPDS_BENCH_BUCKET, R2_PREFIX and the AWS_* keys (run.sh loads them)')
    this.cluster = cluster
    this.prefix = prefix
    const opts = { cluster, memory, prefix, extra }
    this.nodes = cluster ? [0, 1, 2].map((i) => new Node(i, opts)) : [new Node(0, opts)]
    this.rr = 0
  }

  async start() {
    if (!BIN) throw new Error('VLPDS_BIN is not set (run.sh builds it)')
    // a driver that dies (a node that never got healthy, say) must not leave the others running
    if (!this.exitHook) process.once('exit', (this.exitHook = () => this.nodes.forEach((n) => n.alive() && n.proc.kill('SIGKILL'))))
    if (this.cluster) {
      await this.startBalancer()
      for (const n of this.nodes) await n.start()
      await this.waitCluster()
    } else {
      await this.nodes[0].start()
    }
    log(`vlpds up: ${this.cluster ? '3-node cluster behind ' : ''}${URLS.vlpds} (${this.nodes[0].opts.memory ? 'memory' : `${R2 ? 'R2' : 'MinIO'} prefix ${this.prefix}`})`)
    if (R2) this.watchOps()
    return this
  }

  /**
   * STORE=r2: samples every node's bucket request counters every 2 s into
   * out/r2-ops.json, and kills everything past R2.opsLimit. A kill -9 loses
   * at most the 2 s since a node's last sample.
   */
  watchOps() {
    this.opsTimer = setInterval(() => this.sampleOps().catch(() => {}), 2000)
  }

  async sampleOps() {
    for (const n of this.nodes) {
      if (!n.alive()) continue
      const txt = await fetch(`${n.url}/metrics`).then((r) => (r.ok ? r.text() : null)).catch(() => null)
      if (txt) n.lastOps = storeOpsOf(txt)
    }
    const by = {}
    for (const n of this.nodes) for (const m of [n.carried, n.lastOps]) for (const [k, v] of m) by[k] = (by[k] ?? 0) + v
    const total = Object.values(by).reduce((a, b) => a + b, 0)
    this.ops = { prefix: this.prefix, total, by_op: by, at: new Date().toISOString() }
    writeFileSync(`${OUT}r2-ops.json`, JSON.stringify(this.ops, null, 1))
    if (total > R2.opsLimit && !this.overBudget) {
      this.overBudget = true
      log(`R2 ops ${total} > limit ${R2.opsLimit}: killing vlpds and exiting`)
      this.nodes.forEach((n) => n.alive() && n.proc.kill('SIGKILL'))
      process.exit(4)
    }
    return this.ops
  }

  /** Every shard of the layout owned by a live node, and every live node owning some (vlpds_owned_partitions). */
  async waitCluster(ms = 90_000) {
    const t0 = Date.now()
    let last = ''
    while (Date.now() - t0 < ms) {
      let layout = 0
      let owned = 0
      let idle = 0
      for (const n of this.nodes.filter((x) => x.alive())) {
        const txt = await fetch(`${n.url}/metrics`).then((r) => r.text()).catch(() => '')
        const get = (name) => Number(new RegExp(`^${name}(?:\\{[^}]*\\})? (\\S+)`, 'm').exec(txt)?.[1] ?? 0)
        layout = Math.max(layout, get('vlpds_shard_layout_shards'))
        const o = get('vlpds_owned_partitions')
        owned += o
        if (!o) idle++
      }
      last = `layout ${layout} owned ${owned} idle nodes ${idle}`
      if (layout > 0 && owned >= layout && idle === 0) return
      await sleep(500)
    }
    log(`cluster not settled after ${ms} ms (${last}); carrying on`)
  }

  async startBalancer() {
    this.lb = http.createServer(async (req, res) => {
      const chunks = []
      for await (const c of req) chunks.push(c)
      const body = Buffer.concat(chunks)
      const live = this.nodes.filter((n) => n.alive())
      for (let tries = 0; tries < Math.max(1, live.length); tries++) {
        const n = live[this.rr++ % live.length]
        if (!n) break
        try {
          const headers = { ...req.headers }
          delete headers['content-length']
          const up = await fetch(`${n.url}${req.url}`, {
            method: req.method,
            headers,
            body: ['GET', 'HEAD'].includes(req.method) ? undefined : body,
            redirect: 'manual',
          })
          const h = {}
          up.headers.forEach((v, k) => {
            if (!['content-encoding', 'transfer-encoding', 'connection', 'content-length'].includes(k)) h[k] = v
          })
          const setCookies = up.headers.getSetCookie?.() ?? []
          if (setCookies.length) h['set-cookie'] = setCookies
          res.writeHead(up.status, h)
          res.end(Buffer.from(await up.arrayBuffer()))
          return
        } catch {}
      }
      res.writeHead(503, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ error: 'Unavailable', message: 'no live vlpds node' }))
    })
    await new Promise((r) => this.lb.listen(PORTS.vlpds, '127.0.0.1', r))
  }

  /** The node URLs (for per-node firehose taps and metrics). */
  urls() {
    return this.nodes.map((n) => n.url)
  }

  async killNode(i, signal = 'SIGKILL') {
    log(`vlpds n${i}: ${signal}`)
    await this.nodes[i].kill(signal)
  }

  async restartNode(i) {
    await this.nodes[i].start()
    if (this.cluster) await this.waitCluster()
  }

  async stop() {
    if (this.opsTimer) {
      clearInterval(this.opsTimer)
      const o = await this.sampleOps()
      log(`R2 bucket ops this run: ${o.total} ${JSON.stringify(o.by_op)}`)
    }
    for (const n of this.nodes) await n.kill('SIGTERM')
    this.lb?.closeAllConnections?.()
    await new Promise((r) => (this.lb ? this.lb.close(r) : r()))
  }

  /** Prometheus text from every node, summed by series. */
  async metrics() {
    const sums = new Map()
    for (const n of this.nodes) {
      if (!n.alive()) continue
      const r = await fetch(`${n.url}/metrics`).catch(() => null)
      if (!r?.ok) continue
      for (const line of (await r.text()).split('\n')) {
        if (!line || line.startsWith('#')) continue
        const i = line.lastIndexOf(' ')
        const v = Number(line.slice(i + 1))
        if (Number.isFinite(v)) sums.set(line.slice(0, i), (sums.get(line.slice(0, i)) ?? 0) + v)
      }
    }
    return sums
  }
}
