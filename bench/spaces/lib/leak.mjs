// The privacy leak check. Every space write the harness makes carries the
// run's sentinel (in record values, rkeys and the space's skey). A host's
// public surfaces must never show it: the firehose (subscribeRepos live from
// before the first write, and a cursor-0 backfill at the end), and each
// author's sync.getRepo and sync.listBlobs. Space-only blobs must answer
// BlobNotFound on sync.getBlob and stay out of sync.listBlobs.
import { randomBytes } from 'node:crypto'
import { RUN } from './actor.mjs'
import { hostUrl, rawXrpc, sleep } from './http.mjs'

// lowercase alphanumerics, so it fits a record key, an skey and a string
export const SENTINEL = `sntl${RUN}${randomBytes(4).toString('hex')}`
const NEEDLE = Buffer.from(SENTINEL)

export const containsSentinel = (bytes) => Buffer.from(bytes).includes(NEEDLE)

export class FirehoseTap {
  constructor(base, label) {
    this.base = base
    this.label = label
    this.frames = 0
    this.bytes = 0
    this.hits = []
    this.errors = []
  }

  open(cursor) {
    const u = new URL('/xrpc/com.atproto.sync.subscribeRepos', hostUrl(this.base))
    u.protocol = 'ws:'
    if (cursor !== undefined) u.searchParams.set('cursor', String(cursor))
    const ws = new WebSocket(u)
    ws.binaryType = 'arraybuffer'
    this.lastFrameAt = Date.now()
    ws.onmessage = (ev) => {
      const b = Buffer.from(ev.data)
      this.frames++
      this.bytes += b.length
      this.lastFrameAt = Date.now()
      if (b.includes(NEEDLE)) this.hits.push(`frame ${this.frames} (${b.length} B) has the sentinel at byte ${b.indexOf(NEEDLE)}`)
    }
    ws.onerror = (e) => this.errors.push(String(e?.message ?? e))
    this.ws = ws
    return new Promise((ok, err) => {
      ws.onopen = () => ok(this)
      setTimeout(() => err(new Error(`${this.label}: subscribeRepos did not open`)), 5000)
    })
  }

  /** Wait until the stream has been quiet for `quietMs` (a backfill is drained). */
  async drain(quietMs = 1500, maxMs = 60_000) {
    const t0 = Date.now()
    while (Date.now() - this.lastFrameAt < quietMs && Date.now() - t0 < maxMs) await sleep(200)
  }

  close() {
    try {
      this.ws?.close()
    } catch {}
  }
}

/**
 * Check one author's public sync surfaces on its host. Returns a list of
 * leak descriptions (empty when clean).
 */
export async function checkAuthor(base, did, spaceBlobCids = []) {
  const leaks = []
  const repo = await rawXrpc(base, 'com.atproto.sync.getRepo', { params: { did } })
  if (repo.ok && containsSentinel(repo.bytes)) leaks.push(`sync.getRepo(${did}) contains the sentinel`)
  if (!repo.ok && repo.status !== 400) leaks.push(`sync.getRepo(${did}) answered ${repo.status}`)
  let cursor
  const listed = new Set()
  do {
    const r = await rawXrpc(base, 'com.atproto.sync.listBlobs', { params: { did, cursor, limit: 1000 } })
    if (!r.ok) break
    for (const c of r.json.cids ?? []) listed.add(c)
    cursor = r.json.cursor
  } while (cursor)
  for (const cid of spaceBlobCids) {
    if (listed.has(cid)) leaks.push(`sync.listBlobs(${did}) lists space blob ${cid}`)
    const b = await rawXrpc(base, 'com.atproto.sync.getBlob', { params: { did, cid } })
    if (b.ok) leaks.push(`sync.getBlob(${did}, ${cid}) serves a space-only blob`)
  }
  return leaks
}
