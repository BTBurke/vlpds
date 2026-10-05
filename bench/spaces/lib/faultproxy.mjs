// A small HTTP fault proxy for notification hops: forwards to `target`, and
// for requests whose path matches `match` can drop them (answering 503, as a
// lost or failed delivery looks to the sender), delay them, or deliver them
// twice. Rules change at runtime (`proxy.rules = {...}`).
import http from 'node:http'
import { hostUrl } from './http.mjs'

export class FaultProxy {
  constructor(port, target, { match = /notify/ } = {}) {
    this.port = port
    this.target = target
    this.match = match
    this.rules = { drop: 0, dup: 0, delayMs: [0, 0] }
    this.stats = { forwarded: 0, dropped: 0, duplicated: 0, delayed: 0 }
    this.rand = Math.random
  }

  async start() {
    this.server = http.createServer((req, res) => this.handle(req, res))
    await new Promise((r) => this.server.listen(this.port, '127.0.0.1', r))
    return this
  }

  async stop() {
    this.server?.closeAllConnections?.()
    await new Promise((r) => this.server?.close(r) ?? r())
  }

  async handle(req, res) {
    const chunks = []
    for await (const c of req) chunks.push(c)
    const body = Buffer.concat(chunks)
    const faulty = this.match.test(req.url)
    const r = this.rules
    if (faulty && this.rand() < r.drop) {
      this.stats.dropped++
      res.writeHead(503, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ error: 'InjectedDrop', message: 'dropped by the fault proxy' }))
      return
    }
    if (faulty && r.delayMs[1] > 0) {
      const ms = r.delayMs[0] + this.rand() * (r.delayMs[1] - r.delayMs[0])
      this.stats.delayed++
      await new Promise((ok) => setTimeout(ok, ms))
    }
    const send = () => {
      const headers = { ...req.headers }
      delete headers.host
      delete headers['content-length']
      return fetch(hostUrl(`${this.target}${req.url}`), {
        method: req.method,
        headers,
        body: req.method === 'GET' || req.method === 'HEAD' ? undefined : body,
        redirect: 'manual',
      })
    }
    try {
      const up = await send()
      this.stats.forwarded++
      const buf = Buffer.from(await up.arrayBuffer())
      const h = {}
      up.headers.forEach((v, k) => {
        if (!['content-encoding', 'transfer-encoding', 'connection', 'content-length'].includes(k)) h[k] = v
      })
      res.writeHead(up.status, h)
      res.end(buf)
      if (faulty && this.rand() < r.dup) {
        this.stats.duplicated++
        send().catch(() => {})
      }
    } catch (e) {
      res.writeHead(502, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ error: 'BadGateway', message: String(e) }))
    }
  }
}
