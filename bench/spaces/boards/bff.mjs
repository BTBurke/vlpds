// The web UI's backend: sign-in and sessions for the boards UI, mounted on
// the appview's HTTP server. vlpds accounts sign in with real atproto OAuth
// (a loopback client: PAR, PKCE, DPoP; the browser goes to vlpds's own
// sign-in and consent pages and comes back to /oauth/callback). Reference
// PDS accounts sign in with a password session, since the harness's ref PDSes
// serve OAuth only over https. Every action then goes through the boards
// client library (as the signed-in user) and the appview's API (with the
// user's space credential).
import { createHash, randomBytes } from 'node:crypto'
import { HOSTS } from '../lib/env.mjs'
import { hfetch, makeClient, rawXrpc } from '../lib/http.mjs'
import { pdsEndpoint, resolveDid } from '../lib/identity.mjs'
import { DpopKey, OAuthSession } from '../lib/oauth.mjs'
import { BoardsClient, SCOPES } from './client.mjs'
import { BOARD_TYPE } from './model.mjs'

const b64u = (b) => Buffer.from(b).toString('base64url')

export async function asPost(base, key, path, form) {
  const htu = `${base}${path}`
  for (let i = 0; i < 3; i++) {
    const res = await hfetch(htu, {
      method: 'POST',
      headers: { 'content-type': 'application/x-www-form-urlencoded', dpop: key.proof('POST', htu) },
      body: new URLSearchParams(form),
    })
    key.absorb(res)
    const j = await res.json().catch(() => ({}))
    if (j.error === 'use_dpop_nonce') continue
    return { status: res.status, body: j }
  }
  throw new Error(`${path}: no DPoP nonce accepted`)
}

function hostKeyOf(url) {
  const port = new URL(url).port
  return Object.values(HOSTS).find((h) => new URL(h.url).port === port)?.key ?? url
}

/** A password session on a reference PDS, refreshed when it expires. */
function passwordSigner(state) {
  const s = ({ headers }) => headers.set('authorization', `Bearer ${state.accessJwt}`)
  s.retry = async (res) => {
    if (res.status !== 400 && res.status !== 401) return false
    const j = await res.clone().json().catch(() => ({}))
    if (j.error !== 'ExpiredToken') return false
    const r = await rawXrpc(state.base, 'com.atproto.server.refreshSession', { method: 'POST', headers: { authorization: `Bearer ${state.refreshJwt}` } })
    if (!r.ok) return false
    Object.assign(state, { accessJwt: r.json.accessJwt, refreshJwt: r.json.refreshJwt })
    return true
  }
  return s
}

export class Bff {
  constructor({ appview, publicUrl, vlpdsUrl }) {
    this.appview = appview
    this.publicUrl = publicUrl // where the browser reaches us (http://127.0.0.1:<port>)
    this.vlpdsUrl = vlpdsUrl
    this.sessions = new Map() // sid -> { did, handle, host, client: BoardsClient }
    this.pending = new Map() // state -> { key, verifier, base, clientId, handle }
    this.handles = new Map()
  }

  get redirectUri() {
    return `${this.publicUrl}/oauth/callback`
  }

  clientId(scope) {
    return `http://localhost?scope=${encodeURIComponent(scope)}&redirect_uri=${encodeURIComponent(this.redirectUri)}`
  }

  /** The routes: true when handled. */
  routes() {
    return async (req, res, url) => {
      if (url.pathname === '/oauth/callback') return this.callback(req, res, url), true
      if (!url.pathname.startsWith('/api/')) return false
      const send = (status, obj) => {
        res.writeHead(status, { 'content-type': 'application/json' })
        res.end(JSON.stringify(obj))
      }
      try {
        let body = {}
        if (req.method === 'POST') {
          const chunks = []
          for await (const c of req) chunks.push(c)
          body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : {}
        }
        const q = Object.fromEntries(url.searchParams)
        const out = await this.api(url.pathname.slice(5), q, body, req, res)
        if (out !== undefined) send(200, out)
      } catch (e) {
        send(e.status && e.status < 600 ? e.status : 500, { error: e.error ?? 'Error', message: e.message })
      }
      return true
    }
  }

  session(req) {
    const sid = /(?:^|;\s*)boards_sid=([^;]+)/.exec(req.headers.cookie ?? '')?.[1]
    return sid ? this.sessions.get(sid) : undefined
  }

  newSession(res, s) {
    const sid = b64u(randomBytes(18))
    this.sessions.set(sid, s)
    res.setHeader('set-cookie', `boards_sid=${sid}; Path=/; HttpOnly; SameSite=Lax`)
  }

  async resolveHandle(handle) {
    for (const h of Object.values(HOSTS)) {
      const r = await rawXrpc(h.url, 'com.atproto.identity.resolveHandle', { params: { handle } }).catch(() => null)
      if (r?.ok) {
        const base = await pdsEndpoint(r.json.did)
        return { did: r.json.did, base, host: hostKeyOf(base) }
      }
    }
    throw Object.assign(new Error(`no local PDS knows ${handle}`), { status: 400, error: 'HandleNotFound' })
  }

  /** { handle, host } for a DID, from its PLC document. */
  async profile(did) {
    if (!this.handles.has(did)) {
      const doc = await resolveDid(did).catch(() => null)
      const pds = doc?.service?.find((x) => x.id.endsWith('#atproto_pds'))?.serviceEndpoint
      this.handles.set(did, { handle: doc?.alsoKnownAs?.[0]?.replace('at://', '') ?? did, host: pds ? hostKeyOf(pds) : '?' })
    }
    return this.handles.get(did)
  }

  async startOAuth(handle, did, base) {
    // any user may create boards in the UI, so everyone gets the owner grant
    const scope = SCOPES.owner
    const key = new DpopKey()
    const clientId = this.clientId(scope)
    const verifier = b64u(randomBytes(32))
    const state = b64u(randomBytes(12))
    const par = await asPost(base, key, '/oauth/par', {
      client_id: clientId,
      response_type: 'code',
      redirect_uri: this.redirectUri,
      scope,
      state,
      code_challenge: b64u(createHash('sha256').update(verifier).digest()),
      code_challenge_method: 'S256',
      login_hint: handle,
    })
    if (par.status !== 201) throw Object.assign(new Error(`PAR ${par.status} ${JSON.stringify(par.body)}`), { status: 502 })
    this.pending.set(state, { key, verifier, base, clientId, handle, did })
    return `${base}/oauth/authorize?client_id=${encodeURIComponent(clientId)}&request_uri=${encodeURIComponent(par.body.request_uri)}`
  }

  async callback(req, res, url) {
    const p = this.pending.get(url.searchParams.get('state') ?? '')
    const fail = (msg) => {
      res.writeHead(302, { location: `/?error=${encodeURIComponent(msg)}` })
      res.end()
    }
    if (!p) return fail('unknown sign-in attempt')
    this.pending.delete(url.searchParams.get('state'))
    if (url.searchParams.get('error')) return fail(`${url.searchParams.get('error')}: ${url.searchParams.get('error_description') ?? ''}`)
    const tok = await asPost(p.base, p.key, '/oauth/token', {
      grant_type: 'authorization_code',
      client_id: p.clientId,
      code: url.searchParams.get('code'),
      redirect_uri: this.redirectUri,
      code_verifier: p.verifier,
    })
    if (tok.status !== 200) return fail(`token ${tok.status} ${tok.body.error ?? ''}`)
    const oauth = new OAuthSession(p.base, p.key, p.clientId, tok.body)
    const actor = { did: tok.body.sub, handle: p.handle, host: hostKeyOf(p.base), base: p.base, oauth, client: makeClient(p.base, oauth.signer()) }
    this.newSession(res, { ...actor, auth: 'oauth', scope: tok.body.scope, bc: new BoardsClient(actor) })
    res.writeHead(302, { location: '/' })
    res.end()
  }

  need(s) {
    if (!s) throw Object.assign(new Error('sign in first'), { status: 401, error: 'AuthRequired' })
    return s
  }

  av(s) {
    return s.bc.appview(`http://127.0.0.1:${this.appview.port}`, this.appview.did)
  }

  async api(path, q, body, req, res) {
    const s = this.session(req)
    switch (path) {
      case 'config':
        return { appview: this.appview.did, vlpds: this.vlpdsUrl, console: `${this.vlpdsUrl}/admin`, hosts: Object.fromEntries(Object.values(HOSTS).map((h) => [h.key, h.url])) }
      case 'login': {
        const handle = String(body.handle ?? '').trim().replace(/^@/, '')
        const who = await this.resolveHandle(handle)
        if (HOSTS[who.host]?.kind === 'vlpds') return { redirect: await this.startOAuth(handle, who.did, who.base) }
        if (!body.password) return { needPassword: true, host: who.host }
        const r = await rawXrpc(who.base, 'com.atproto.server.createSession', { method: 'POST', body: { identifier: handle, password: body.password } })
        if (!r.ok) throw Object.assign(new Error(r.message ?? 'sign-in failed'), { status: 401, error: r.error })
        const state = { base: who.base, accessJwt: r.json.accessJwt, refreshJwt: r.json.refreshJwt }
        const actor = { did: r.json.did, handle, host: who.host, base: who.base, client: makeClient(who.base, passwordSigner(state)) }
        this.newSession(res, { ...actor, auth: 'password', bc: new BoardsClient(actor) })
        return { ok: true }
      }
      case 'logout':
        res.setHeader('set-cookie', 'boards_sid=; Path=/; Max-Age=0')
        return { ok: true }
      case 'me':
        return s ? { did: s.did, handle: s.handle, host: s.host, auth: s.auth, scope: s.scope } : { signedIn: false }
      case 'profiles': {
        const dids = String(q.dids ?? '').split(',').filter(Boolean)
        return Object.fromEntries(await Promise.all(dids.map(async (d) => [d, await this.profile(d)])))
      }
    }
    this.need(s)
    const bc = s.bc
    const av = this.av(s)
    switch (path) {
      case 'boards': {
        const out = []
        let cursor
        do {
          const r = await s.client.com.atproto.space.listSpaces({ spaceType: BOARD_TYPE, cursor, limit: 100 })
          for (const sp of r.data.spaces) out.push(sp.uri)
          cursor = r.data.cursor
        } while (cursor)
        const boards = []
        for (const uri of out) {
          const r = await av.call('dev.example.boards.getBoard', uri, { params: { board: uri } }).catch((e) => ({ ok: false, error: e.error ?? e.message }))
          boards.push(r.ok ? { ...r.data, indexed: true } : { uri, name: uri.split('/')[5], owner: uri.split('/')[2], indexed: false, error: r.error })
        }
        return { boards }
      }
      case 'createBoard': {
        const slug = String(body.name ?? '').toLowerCase().replace(/[^a-z0-9]+/g, '').slice(0, 24) || 'board'
        const board = await bc.createBoard(`${slug}${randomBytes(3).toString('hex')}`, {
          name: body.name,
          description: body.description || undefined,
          flairs: (body.flairs ?? []).filter(Boolean),
        })
        await bc.addMember(board, this.appview.account.did, { write: false })
        await av.indexBoard(board)
        return { board }
      }
      case 'board':
        return av.getBoard(q.board)
      case 'members': {
        const ms = await bc.listMembers(q.board)
        return { members: await Promise.all(ms.map(async (m) => ({ ...m, ...(await this.profile(m.did)), appview: m.did === this.appview.account.did }))) }
      }
      case 'invite': {
        const who = await this.resolveHandle(String(body.handle).replace(/^@/, ''))
        await bc.addMember(body.board, who.did, { write: body.role !== 'lurker' })
        return { did: who.did, host: who.host }
      }
      case 'removeMember':
        await bc.removeMember(body.board, body.did)
        return { ok: true }
      case 'deleteBoard':
        await bc.deleteBoard(body.board)
        return { ok: true }
      case 'posts':
        return av.getPosts(q.board, q.sort ?? 'hot', 100)
      case 'thread':
        return av.getPostThread(q.uri)
      case 'karma':
        return av.getKarma(q.board, q.actor)
      case 'myVotes': {
        const out = {}
        let cursor
        do {
          const r = await s.client.com.atproto.space.listRecords({ space: q.board, repo: s.did, collection: 'dev.example.boards.vote', cursor, limit: 100 })
          for (const v of r.data.records) out[v.value.subject] = v.value.direction
          cursor = r.data.cursor
        } while (cursor)
        return out
      }
      case 'post': {
        const image = body.image?.data ? { bytes: Buffer.from(body.image.data, 'base64'), mimeType: body.image.mimeType, alt: body.image.alt || undefined } : undefined
        return bc.post(body.board, { title: body.title, body: body.body || undefined, flair: body.flair || undefined, image })
      }
      case 'comment':
        return bc.comment(body.board, body.post, body.body, { parent: body.parent || undefined })
      case 'vote':
        return body.direction ? bc.vote(body.board, body.subject, body.direction) : bc.unvote(body.board, body.subject)
      case 'editPost':
        return bc.editPost(body.uri, { title: body.title, body: body.body })
      case 'delete':
        await bc.deleteRecord(body.uri)
        return { ok: true }
      case 'image': {
        const r = await av.getImage(q.board, q.did, q.cid)
        res.writeHead(r.ok ? 200 : r.status, { 'content-type': r.ok ? r.type : 'application/json', 'cache-control': 'private, max-age=300' })
        res.end(r.bytes)
        return undefined
      }
      case 'debug': {
        const st = await av.getIndexState(q.board)
        const cred = await bc.credential(q.board)
        return { ...st, viewerCredential: { jti: cred.jti, expires: new Date(cred.claims.payload.exp * 1000).toISOString(), key: cred.claims.payload.cnf?.kid } }
      }
      default:
        throw Object.assign(new Error(`no such route ${path}`), { status: 404, error: 'NotFound' })
    }
  }
}
