// The boards appview: indexes boards it's a read-only member of and serves
// them over an XRPC-style API.
//
// Inbound, it's a registered space syncer (lib/notifysvc.mjs: a did:plc whose
// #atproto_space_syncer entry points at it, checking each call's service
// auth). Per board it runs the harness Syncer: listRepos from a spaceRev
// checkpoint, listRepoOps per repo with a running LtHash checked against the
// signed commit, getRepo (CAR-verified) on a mismatch. Forwarded notifyWrites
// drive it; a slow poll catches what nothing pushes (a record takedown changes
// the signed view at the same rev). It holds nothing it didn't get that way.
//
// Outbound, every query needs a space credential the board's owner issued,
// signed for this appview's DID (Authorization: Atproto-Space + an RFC 9421
// signature with Atproto-Space-Audience). Revocations the owner sends here
// (notifyCredentialRevoked) are honoured until the credential would expire.
import http from 'node:http'
import { existsSync, readFileSync } from 'node:fs'
import { extname, join } from 'node:path'
import { verifySpaceSignature, verifySpaceToken } from '@atproto/space'
import { attempt } from '../lib/http.mjs'
import { signingKey } from '../lib/identity.mjs'
import { Syncer } from '../lib/syncer.mjs'
import { authorityOf, boardOf, boardView, karmaOf, materialize, postView, sortPosts, thread } from './model.mjs'

const NOTIFY_WRITE = 'com.atproto.space.notifyWrite'
const NOTIFY_DELETED = 'com.atproto.space.notifySpaceDeleted'
const NOTIFY_REVOKED = 'com.atproto.space.notifyCredentialRevoked'

class XErr extends Error {
  constructor(status, error, message = error) {
    super(message)
    this.status = status
    this.error = error
  }
}

const keyCache = new Map()
async function cachedKey(did, force) {
  const hit = keyCache.get(did)
  if (hit && !force && Date.now() - hit.at < 60_000) return hit.key
  const key = await signingKey(did)
  keyCache.set(did, { key, at: Date.now() })
  return key
}

export class Appview {
  /**
   * `account`: the appview's own member account (harness Actor) whose
   * delegation its credentials come from. `svc`: a started NotifyService,
   * whose DID is the appview's identity and the credentials' audience.
   */
  constructor({ port, account, svc, pollMs = 3000, webDir, extraRoutes } = {}) {
    this.port = port
    this.account = account
    this.svc = svc
    this.pollMs = pollMs
    this.paused = false
    this.webDir = webDir
    this.extraRoutes = extraRoutes
    this.boards = new Map() // board -> { syncer, version, cache, deleted, regExpires, lastNotify, changedAt }
    this.revoked = new Map() // board -> Map(jti -> until ms)
    this.log = []
  }

  get did() {
    return this.svc.did
  }

  async start() {
    this.unsub = this.svc.on((call) => this.onCall(call))
    this.server = http.createServer((req, res) => this.handle(req, res))
    await new Promise((r) => this.server.listen(this.port, '127.0.0.1', r))
    this.timer = setInterval(() => this.poll().catch(() => {}), this.pollMs)
    return this
  }

  async stop() {
    clearInterval(this.timer)
    for (const b of this.boards.values()) clearTimeout(b.renew)
    this.unsub?.()
    this.server?.closeAllConnections?.()
    await new Promise((r) => this.server?.close(r) ?? r())
  }

  // indexing

  async indexBoard(board) {
    let b = this.boards.get(board)
    if (b && !b.deleted) return b
    const syncer = new Syncer(`appview/${board.split('/')[5]}`, board, this.account)
    b = { board, syncer, version: 0, cache: null, deleted: false, lastNotify: null, changedAt: Date.now() }
    try {
      await syncer.credential()
    } catch (e) {
      throw new XErr(400, 'NotAMember', `the appview's account can't read this board: ${e.error ?? e.message}`)
    }
    this.boards.set(board, b)
    await this.register(b)
    await syncer.sync()
    if (syncer.lastError) throw syncer.lastError
    this.bump(b)
    return b
  }

  async register(b) {
    const cred = await b.syncer.credential()
    const r = await (await cred.hostClient()).com.atproto.space.registerNotify({ space: b.board, service: this.svc.serviceRef })
    b.regExpires = Date.parse(r.data.expiresAt)
    clearTimeout(b.renew)
    const renewIn = Math.max(10_000, (b.regExpires - Date.now()) / 2)
    b.renew = setTimeout(() => this.register(b).catch((e) => this.note(`renew ${b.board}: ${e.message}`)), renewIn)
    b.renew.unref?.()
  }

  note(s) {
    this.log.push(`${new Date().toISOString()} ${s}`)
  }

  bump(b) {
    b.version++
    b.cache = null
    b.changedAt = Date.now()
  }

  drop(b, why) {
    if (b.deleted) return
    b.deleted = why
    clearTimeout(b.renew)
    b.syncer.repos.clear()
    b.cache = null
    this.note(`dropped ${b.board}: ${why}`)
  }

  onCall(call) {
    const body = call.body ?? {}
    const b = this.boards.get(body.space)
    if (call.lxm === NOTIFY_REVOKED) {
      const m = this.revoked.get(body.space) ?? new Map()
      for (const jti of body.credentials ?? []) m.set(jti, Date.now() + 3600_000)
      this.revoked.set(body.space, m)
      return
    }
    if (!b || b.deleted) return
    if (call.lxm === NOTIFY_DELETED) return this.drop(b, 'notifySpaceDeleted')
    if (call.lxm === NOTIFY_WRITE) {
      b.lastNotify = { at: Date.now(), repo: body.repo, repoRev: body.repoRev, spaceRev: body.spaceRev }
      b.syncer.notify(body).then(
        () => this.bump(b),
        (e) => this.syncFailed(b, e),
      )
    }
  }

  syncFailed(b, e) {
    if (/SpaceNotFound|SpaceDeleted/.test(`${e?.error} ${e?.message}`)) this.drop(b, e.error ?? e.message)
    else this.note(`sync ${b.board}: ${e?.error ?? ''} ${e?.message}`)
  }

  /** Catch up from the checkpoint, then re-pull every repo (cheap when nothing moved). */
  async poll() {
    if (this.paused) return
    for (const b of this.boards.values()) {
      if (b.deleted) continue
      await b.syncer
        .enqueue(async () => {
          await b.syncer.catchUp()
          for (const did of [...b.syncer.repos.keys()]) await b.syncer.pull(did)
        })
        .then(
          () => this.bump(b),
          (e) => this.syncFailed(b, e),
        )
    }
  }

  view(b) {
    if (!b.cache || b.cache.version !== b.version) {
      const repos = new Map([...b.syncer.repos].map(([did, r]) => [did, r.records]))
      b.cache = { version: b.version, view: materialize(b.board, repos) }
    }
    return b.cache.view
  }

  // auth

  /** The viewer's space credential: issued by the board's owner, for this board, signed for this appview. */
  async authorize(req, board) {
    const auth = req.headers.authorization ?? ''
    if (!auth.startsWith('Atproto-Space ')) throw new XErr(401, 'AuthRequired', 'a space credential for the board is required')
    const jwt = auth.slice('Atproto-Space '.length)
    let tok
    try {
      tok = await verifySpaceToken('credential', jwt, { getSigningKey: (iss, _kid, force) => cachedKey(iss, force), sub: board })
    } catch (e) {
      throw new XErr(401, 'AuthRequired', `bad credential: ${e.message}`)
    }
    if (tok.payload.iss !== authorityOf(board)) throw new XErr(401, 'AuthRequired', 'credential not issued by the board owner')
    if (req.headers['atproto-space-audience'] !== this.did) throw new XErr(401, 'AuthRequired', `audience must be ${this.did}`)
    try {
      await verifySpaceSignature(req.headers, tok.payload.cnf?.kid)
    } catch (e) {
      throw new XErr(401, 'AuthRequired', e.message)
    }
    if (this.revoked.get(board)?.has(tok.payload.jti)) throw new XErr(401, 'CredentialRevoked', 'credential revoked')
    return tok.payload
  }

  live(board) {
    const b = this.boards.get(board)
    if (!b || b.deleted) throw new XErr(400, 'BoardNotFound', b?.deleted ? `board deleted (${b.deleted})` : 'not indexed here')
    return b
  }

  // HTTP

  async handle(req, res) {
    const url = new URL(req.url, 'http://localhost')
    const send = (status, obj, headers = {}) => {
      res.writeHead(status, { 'content-type': 'application/json', ...headers })
      res.end(JSON.stringify(obj))
    }
    try {
      if (this.extraRoutes && (await this.extraRoutes(req, res, url))) return
      if (!url.pathname.startsWith('/xrpc/')) return this.serveStatic(url, res)
      const nsid = url.pathname.slice(6)
      const q = Object.fromEntries(url.searchParams)
      let body = {}
      if (req.method === 'POST') {
        const chunks = []
        for await (const c of req) chunks.push(c)
        body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : {}
      }
      if (nsid === '_health') return send(200, { ok: true, did: this.did })
      const out = await this.route(nsid, q, body, req, res)
      if (out !== undefined) send(200, out)
    } catch (e) {
      if (e instanceof XErr) return send(e.status, { error: e.error, message: e.message })
      send(500, { error: 'InternalServerError', message: String(e?.message ?? e) })
    }
  }

  async route(nsid, q, body, req, res) {
    const need = (k, v) => {
      if (!v) throw new XErr(400, 'InvalidRequest', `${k} is required`)
      return v
    }
    switch (nsid) {
      case 'dev.example.boards.indexBoard': {
        const board = need('board', body.board)
        await this.authorize(req, board)
        const b = await this.indexBoard(board)
        return { board, spaceRev: b.syncer.checkpoint }
      }
      case 'dev.example.boards.getBoard': {
        const board = need('board', q.board)
        await this.authorize(req, board)
        return boardView(this.view(this.live(board)))
      }
      case 'dev.example.boards.getPosts': {
        const board = need('board', q.board)
        await this.authorize(req, board)
        const sort = q.sort ?? 'hot'
        if (!['hot', 'new', 'top'].includes(sort)) throw new XErr(400, 'InvalidRequest', 'sort is hot, new or top')
        const limit = Math.min(Math.max(Number(q.limit ?? 25) || 25, 1), 1000)
        const all = sortPosts(this.view(this.live(board)), sort)
        const start = Number(q.cursor ?? 0) || 0
        const page = all.slice(start, start + limit).map((p) => postView(p, this.imageUrl(board)))
        return { posts: page, cursor: start + limit < all.length ? String(start + limit) : undefined }
      }
      case 'dev.example.boards.getPostThread': {
        const uri = need('uri', q.uri)
        const board = boardOf(uri)
        await this.authorize(req, board)
        const t = thread(this.view(this.live(board)), uri)
        if (!t) throw new XErr(400, 'PostNotFound', 'no such post')
        return { post: postView(t.post, this.imageUrl(board)), replies: t.replies }
      }
      case 'dev.example.boards.getKarma': {
        const board = need('board', q.board)
        await this.authorize(req, board)
        return karmaOf(this.view(this.live(board)), need('actor', q.actor))
      }
      case 'dev.example.boards.getImage': {
        const board = need('board', q.board)
        await this.authorize(req, board)
        const b = this.live(board)
        const view = this.view(b)
        const post = [...view.posts.values()].find((p) => p.author === q.did && p.image?.cid === q.cid)
        if (!post) throw new XErr(400, 'BlobNotFound', 'no live post on this board has that image')
        const r = await b.syncer.withCred(async (cred) => attempt(async () => (await cred.repoClient(q.did)).com.atproto.space.getBlob({ space: board, repo: q.did, cid: q.cid })))
        if (!r.ok) throw new XErr(400, r.error ?? 'BlobNotFound', r.message)
        res.writeHead(200, { 'content-type': post.image.mimeType ?? 'application/octet-stream', 'cache-control': 'private, max-age=60' })
        res.end(Buffer.from(r.data))
        return undefined
      }
      case 'dev.example.boards.getIndexState': {
        const board = need('board', q.board)
        await this.authorize(req, board)
        return this.indexState(this.live(board))
      }
      default:
        throw new XErr(501, 'MethodNotImplemented', `${nsid} is not a boards method`)
    }
  }

  imageUrl(board) {
    return (p) => `/xrpc/dev.example.boards.getImage?board=${encodeURIComponent(board)}&did=${encodeURIComponent(p.author)}&cid=${p.image.cid}`
  }

  /** What the Spaces debug drawer shows: the authority, each synced repo's rev and digest, the last notify. */
  indexState(b) {
    const s = b.syncer
    return {
      board: b.board,
      authority: authorityOf(b.board),
      appview: this.did,
      checkpoint: s.checkpoint,
      lastSpaceRev: s.lastSpaceRev,
      registrationExpires: b.regExpires ? new Date(b.regExpires).toISOString() : undefined,
      credentialExpires: s.cred ? new Date(s.cred.claims.payload.exp * 1000).toISOString() : undefined,
      lastNotify: b.lastNotify ? { ...b.lastNotify, at: new Date(b.lastNotify.at).toISOString() } : undefined,
      repos: [...s.repos].map(([did, r]) => ({ did, rev: r.rev, records: r.records.size, digest: Buffer.from(r.hash.digest()).toString('hex').slice(0, 32) })),
      stats: s.stats,
      violations: s.violations.slice(-20),
      version: b.version,
    }
  }

  serveStatic(url, res) {
    if (!this.webDir) {
      res.writeHead(404).end()
      return
    }
    let p = join(this.webDir, url.pathname.replace(/\.\.+/g, ''))
    if (!existsSync(p) || url.pathname === '/') p = join(this.webDir, 'index.html')
    const types = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.png': 'image/png', '.json': 'application/json' }
    res.writeHead(200, { 'content-type': types[extname(p)] ?? 'application/octet-stream' })
    res.end(readFileSync(p))
  }
}

