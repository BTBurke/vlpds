// A headless OAuth client for vlpds: a loopback client (PAR + PKCE + DPoP),
// the authorization page driven as plain form posts (sign-in, consent), the
// code exchange, and DPoP-bound resource requests. The same flow as
// tests/all/oauth.rs; the account's password is generated per run.
import { createHash, generateKeyPairSync, randomBytes, sign } from 'node:crypto'
import { hfetch } from './http.mjs'

const b64u = (b) => Buffer.from(b).toString('base64url')
const REDIRECT = 'http://127.0.0.1/cb'

export class DpopKey {
  constructor() {
    this.key = generateKeyPairSync('ec', { namedCurve: 'P-256' })
    const { kty, crv, x, y } = this.key.publicKey.export({ format: 'jwk' })
    this.jwk = { kty, crv, x, y }
    this.nonce = undefined
  }
  proof(htm, htu, ath) {
    const header = { typ: 'dpop+jwt', alg: 'ES256', jwk: this.jwk }
    const payload = {
      jti: b64u(randomBytes(12)),
      htm,
      htu,
      iat: Math.floor(Date.now() / 1000),
      ...(this.nonce ? { nonce: this.nonce } : {}),
      ...(ath ? { ath } : {}),
    }
    const input = `${b64u(JSON.stringify(header))}.${b64u(JSON.stringify(payload))}`
    const sig = sign('sha256', Buffer.from(input), { key: this.key.privateKey, dsaEncoding: 'ieee-p1363' })
    return `${input}.${b64u(sig)}`
  }
  absorb(res) {
    const n = res.headers.get('dpop-nonce')
    if (n) this.nonce = n
  }
}

/** POST a form to the authorization server with DPoP, retrying once for a nonce. */
async function asPost(base, key, path, form) {
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

const hidden = (html, name) => {
  const m = new RegExp(`name="${name}" value="([^"]*)"`).exec(html)
  return m ? m[1].replace(/&amp;/g, '&') : undefined
}

class Browser {
  cookie = undefined
  async go(url, init = {}) {
    const headers = { ...(init.headers ?? {}), ...(this.cookie ? { cookie: this.cookie } : {}) }
    const res = await hfetch(url, { ...init, headers })
    for (const sc of res.headers.getSetCookie?.() ?? []) {
      const c = sc.split(';')[0]
      if (c.startsWith('vlpds-device=')) this.cookie = c
    }
    return { status: res.status, headers: res.headers, html: await res.text() }
  }
  post(base, path, pairs) {
    return this.go(`${base}${path}`, {
      method: 'POST',
      headers: { 'content-type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams(pairs),
    })
  }
}

/**
 * Authorize `scope` for an account (handle + password) on `base`; returns a
 * session with the DPoP key, tokens and granted scope. Throws with the
 * server's words when a step refuses.
 */
export async function oauthLogin(base, { handle, did, password, scope }) {
  const key = new DpopKey()
  const clientId = `http://localhost?scope=${encodeURIComponent(scope)}&redirect_uri=${encodeURIComponent(REDIRECT)}`
  const verifier = b64u(randomBytes(32))
  const challenge = b64u(createHash('sha256').update(verifier).digest())
  const state = b64u(randomBytes(8))
  const par = await asPost(base, key, '/oauth/par', {
    client_id: clientId,
    response_type: 'code',
    redirect_uri: REDIRECT,
    scope,
    state,
    code_challenge: challenge,
    code_challenge_method: 'S256',
    login_hint: handle,
  })
  if (par.status !== 201) throw new Error(`PAR ${par.status} ${JSON.stringify(par.body)}`)
  const ru = par.body.request_uri
  const b = new Browser()
  let page = await b.go(`${base}/oauth/authorize?client_id=${encodeURIComponent(clientId)}&request_uri=${encodeURIComponent(ru)}`)
  if (page.status !== 200) throw new Error(`authorize page ${page.status}: ${page.html.slice(0, 300)}`)
  if (page.html.includes('Choose an account')) {
    page = await b.post(base, '/oauth/authorize/select', { request_uri: ru, csrf: hidden(page.html, 'csrf'), did: '' })
  }
  if (page.html.includes('name="password"')) {
    page = await b.post(base, '/oauth/authorize/sign-in', {
      request_uri: ru,
      csrf: hidden(page.html, 'csrf'),
      identifier: handle,
      password,
      action: 'sign-in',
    })
  }
  if (!page.html.includes('Authorize access')) {
    throw new Error(`expected the consent page, got ${page.status}: ${page.html.replace(/\s+/g, ' ').slice(0, 400)}`)
  }
  const consent = await b.post(base, '/oauth/authorize/consent', {
    request_uri: ru,
    csrf: hidden(page.html, 'csrf'),
    did,
    action: 'allow',
  })
  const loc = consent.headers.get('location')
  if (consent.status !== 303 || !loc) throw new Error(`consent ${consent.status}: ${consent.html.slice(0, 300)}`)
  const q = new URL(loc.replace('#', '?')).searchParams
  if (q.get('error')) throw new Error(`consent error ${q.get('error')} ${q.get('error_description')}`)
  const tok = await asPost(base, key, '/oauth/token', {
    grant_type: 'authorization_code',
    client_id: clientId,
    code: q.get('code'),
    redirect_uri: REDIRECT,
    code_verifier: verifier,
  })
  if (tok.status !== 200) throw new Error(`token ${tok.status} ${JSON.stringify(tok.body)}`)
  return new OAuthSession(base, key, clientId, tok.body)
}

export class OAuthSession {
  constructor(base, key, clientId, tok) {
    this.base = base
    this.key = key
    this.clientId = clientId
    this.set(tok)
  }
  set(tok) {
    this.access = tok.access_token
    this.refreshToken = tok.refresh_token
    this.scope = tok.scope
    this.sub = tok.sub
    this.expiresAt = Date.now() + (tok.expires_in ?? 300) * 1000
  }
  async refresh() {
    const r = await asPost(this.base, this.key, '/oauth/token', {
      grant_type: 'refresh_token',
      client_id: this.clientId,
      refresh_token: this.refreshToken,
    })
    if (r.status !== 200) throw new Error(`refresh ${r.status} ${JSON.stringify(r.body)}`)
    this.set(r.body)
  }
  /** The auth layer for {@link makeClient}: DPoP-bound access token, nonce retries, refresh near expiry. */
  signer() {
    const s = async ({ method, url, headers }) => {
      if (Date.now() > this.expiresAt - 30_000) await this.refresh()
      const htu = url.split('?')[0]
      const ath = b64u(createHash('sha256').update(this.access).digest())
      headers.set('authorization', `DPoP ${this.access}`)
      headers.set('dpop', this.key.proof(method, htu, ath))
    }
    s.retry = async (res) => {
      this.key.absorb(res)
      if (res.status === 401 && /use_dpop_nonce/.test(res.headers.get('www-authenticate') ?? '')) return true
      return false
    }
    return s
  }
}
