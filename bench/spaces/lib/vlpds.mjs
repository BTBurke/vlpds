// The vlpds under test, run by the driver so the fault sim can kill it: one
// node on MinIO (or --memory), or a 3-node cluster on MinIO behind a small
// round-robin HTTP balancer on the public port, as in tests/E2E.md. Logs go
// to out/vlpds-*.log. Dev mode only, with the harness's test keys.
import { spawn } from 'node:child_process'
import { createWriteStream, mkdirSync } from 'node:fs'
import http from 'node:http'
import { OUT, PORTS, URLS, VLPDS_ROTATION_KEY, log } from './env.mjs'
import { hostUrl, sleep } from './http.mjs'

const BIN = process.env.VLPDS_BIN

function cleanEnv(extra = {}) {
  const env = {}
  for (const [k, v] of Object.entries(process.env)) if (!k.startsWith('VLPDS_') || k === 'VLPDS_BIN') env[k] = v
  return { ...env, RUST_LOG: process.env.VLPDS_RUST_LOG ?? 'info', ...extra }
}

class Node {
  constructor(i, opts) {
    this.i = i
    this.opts = opts
    this.port = opts.cluster ? PORTS.vlpdsNodes[i] : PORTS.vlpds
    this.url = `http://127.0.0.1:${this.port}`
    this.proc = null
    this.starts = 0
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
    else a.push('--s3-endpoint', URLS.minio, '--s3-bucket', 'vlpds', '--prefix', o.prefix)
    if (o.cluster) {
      a.push(
        '--node-id', `n${this.i}`,
        '--peer-listen', `127.0.0.1:${PORTS.vlpdsPeers[this.i]}`,
        '--advertise-url', `https://127.0.0.1:${PORTS.vlpdsPeers[this.i]}`,
        '--peer-tls-dir', `${OUT}peer-tls`,
        '--lease-ttl-ms', String(o.leaseTtlMs ?? 3000),
        '--shards', '16',
        '--workers', '2',
      )
    }
    return [...a, ...(o.extra ?? [])]
  }

  async start() {
    this.starts++
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
  constructor({ cluster = false, memory = false, prefix = `spaces-${Date.now().toString(36)}`, extra = [] } = {}) {
    this.cluster = cluster
    this.prefix = prefix
    const opts = { cluster, memory, prefix, extra }
    this.nodes = cluster ? [0, 1, 2].map((i) => new Node(i, opts)) : [new Node(0, opts)]
    this.rr = 0
  }

  async start() {
    if (!BIN) throw new Error('VLPDS_BIN is not set (run.sh builds it)')
    if (this.cluster) {
      await this.startBalancer()
      for (const n of this.nodes) await n.start()
      await this.waitCluster()
    } else {
      await this.nodes[0].start()
    }
    log(`vlpds up: ${this.cluster ? '3-node cluster behind ' : ''}${URLS.vlpds} (${this.nodes[0].opts.memory ? 'memory' : `MinIO prefix ${this.prefix}`})`)
    return this
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
