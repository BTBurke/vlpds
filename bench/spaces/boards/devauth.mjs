// Sign-in for the boards UI on the harness stack (the `auth` the BFF in
// packages/boards/src/bff.mjs takes). vlpds accounts sign in with real atproto
// OAuth (a loopback client: PAR, PKCE, DPoP; the browser goes to vlpds's own
// sign-in and consent pages and comes back to /oauth/callback). Reference PDS
// accounts sign in with a password session, since the harness's ref PDSes
// serve OAuth only over https. Sessions live in memory.
import { createHash, randomBytes } from 'node:crypto'
import { SCOPES } from '../../../../boards/src/client.mjs'
import { HOSTS } from '../lib/env.mjs'
import { makeClient, rawXrpc } from '../lib/http.mjs'
import { pdsEndpoint, resolveDid } from '../lib/identity.mjs'
import { DpopKey, OAuthSession, asPost } from '../lib/oauth.mjs'
import { BoardsClient } from './harness.mjs'

const b64u = (b) => Buffer.from(b).toString('base64url')
const fail = (status, error, message) => Object.assign(new Error(message), { status, error })

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

export class DevAuth {
  constructor({ publicUrl, vlpdsUrl }) {
    this.publicUrl = publicUrl // where the browser reaches us (http://127.0.0.1:<port>)
    this.vlpdsUrl = vlpdsUrl
    this.sessions = new Map() // sid -> { did, handle, host, client, bc }
    this.pending = new Map() // state -> { key, verifier, base, clientId, handle }
    this.handles = new Map()
  }

  get redirectUri() {
    return `${this.publicUrl}/oauth/callback`
  }

  clientId(scope) {
    return `http://localhost?scope=${encodeURIComponent(scope)}&redirect_uri=${encodeURIComponent(this.redirectUri)}`
  }

  config() {
    return { mode: 'dev', vlpds: this.vlpdsUrl, console: `${this.vlpdsUrl}/admin`, hosts: Object.fromEntries(Object.values(HOSTS).map((h) => [h.key, h.url])) }
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
    throw fail(400, 'HandleNotFound', `no local PDS knows ${handle}`)
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
    if (par.status !== 201) throw fail(502, 'ParFailed', `PAR ${par.status} ${JSON.stringify(par.body)}`)
    this.pending.set(state, { key, verifier, base, clientId, handle, did })
    return `${base}/oauth/authorize?client_id=${encodeURIComponent(clientId)}&request_uri=${encodeURIComponent(par.body.request_uri)}`
  }

  async callback(req, res, url) {
    const p = this.pending.get(url.searchParams.get('state') ?? '')
    const back = (msg) => {
      res.writeHead(302, { location: `/?error=${encodeURIComponent(msg)}` })
      res.end()
    }
    if (!p) return back('unknown sign-in attempt')
    this.pending.delete(url.searchParams.get('state'))
    if (url.searchParams.get('error')) return back(`${url.searchParams.get('error')}: ${url.searchParams.get('error_description') ?? ''}`)
    const tok = await asPost(p.base, p.key, '/oauth/token', {
      grant_type: 'authorization_code',
      client_id: p.clientId,
      code: url.searchParams.get('code'),
      redirect_uri: this.redirectUri,
      code_verifier: p.verifier,
    })
    if (tok.status !== 200) return back(`token ${tok.status} ${tok.body.error ?? ''}`)
    const oauth = new OAuthSession(p.base, p.key, p.clientId, tok.body)
    const actor = { did: tok.body.sub, handle: p.handle, host: hostKeyOf(p.base), base: p.base, oauth, client: makeClient(p.base, oauth.signer()) }
    this.newSession(res, { ...actor, auth: 'oauth', scope: tok.body.scope, bc: new BoardsClient(actor) })
    res.writeHead(302, { location: '/' })
    res.end()
  }

  async login(body, req, res) {
    const handle = String(body.handle ?? '').trim().replace(/^@/, '')
    const who = await this.resolveHandle(handle)
    if (HOSTS[who.host]?.kind === 'vlpds') return { redirect: await this.startOAuth(handle, who.did, who.base) }
    if (!body.password) return { needPassword: true, host: who.host }
    const r = await rawXrpc(who.base, 'com.atproto.server.createSession', { method: 'POST', body: { identifier: handle, password: body.password } })
    if (!r.ok) throw fail(401, r.error, r.message ?? 'sign-in failed')
    const state = { base: who.base, accessJwt: r.json.accessJwt, refreshJwt: r.json.refreshJwt }
    const actor = { did: r.json.did, handle, host: who.host, base: who.base, client: makeClient(who.base, passwordSigner(state)) }
    this.newSession(res, { ...actor, auth: 'password', bc: new BoardsClient(actor) })
    return { ok: true }
  }

  async logout(req, res) {
    res.setHeader('set-cookie', 'boards_sid=; Path=/; Max-Age=0')
  }
}
