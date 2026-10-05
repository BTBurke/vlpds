// A headless OAuth client for vlpds (the approach of bench/spaces/lib/oauth.mjs):
// a loopback client with PAR, PKCE and DPoP, the authorization pages posted
// as plain forms (sign-in, consent), the code exchange, then DPoP-bound
// XRPC. The seed writes space data on a vlpds with it (space access there is
// OAuth-only), and the verify step reads the moved repos back.
import { createHash, generateKeyPairSync, randomBytes, sign } from 'node:crypto'

const b64u = (b) => Buffer.from(b).toString('base64url')
const REDIRECT = 'http://127.0.0.1/cb'
// node resolves localhost to ::1 first; every server here listens on 127.0.0.1
export const hostUrl = (u) => String(u).replace('//localhost:', '//127.0.0.1:')

class DpopKey {
  constructor() {
    this.key = generateKeyPairSync('ec', { namedCurve: 'P-256' })
    const { kty, crv, x, y } = this.key.publicKey.export({ format: 'jwk' })
    this.jwk = { kty, crv, x, y }
  }
  proof(htm, htu, ath) {
    const header = { typ: 'dpop+jwt', alg: 'ES256', jwk: this.jwk }
    const payload = { jti: b64u(randomBytes(12)), htm, htu, iat: Math.floor(Date.now() / 1000), ...(this.nonce ? { nonce: this.nonce } : {}), ...(ath ? { ath } : {}) }
    const input = `${b64u(JSON.stringify(header))}.${b64u(JSON.stringify(payload))}`
    return `${input}.${b64u(sign('sha256', Buffer.from(input), { key: this.key.privateKey, dsaEncoding: 'ieee-p1363' }))}`
  }
  absorb(res) {
    const n = res.headers.get('dpop-nonce')
    if (n) this.nonce = n
  }
}

async function asPost(base, key, path, form) {
  const htu = `${base}${path}`
  for (let i = 0; i < 3; i++) {
    const res = await fetch(hostUrl(htu), {
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
  async go(url, init = {}) {
    const headers = { ...(init.headers ?? {}), ...(this.cookie ? { cookie: this.cookie } : {}) }
    const res = await fetch(hostUrl(url), { ...init, headers, redirect: 'manual' })
    for (const sc of res.headers.getSetCookie?.() ?? []) {
      const c = sc.split(';')[0]
      if (c.startsWith('vlpds-device=')) this.cookie = c
    }
    return { status: res.status, headers: res.headers, html: await res.text() }
  }
  post(base, path, pairs) {
    return this.go(`${base}${path}`, { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams(pairs) })
  }
}

/** Authorizes `scope` for an account (handle + password) on the vlpds at `base`. */
export async function oauthLogin(base, { handle, did, password, scope }) {
  const key = new DpopKey()
  const clientId = `http://localhost?scope=${encodeURIComponent(scope)}&redirect_uri=${encodeURIComponent(REDIRECT)}`
  const verifier = b64u(randomBytes(32))
  const state = b64u(randomBytes(8))
  const par = await asPost(base, key, '/oauth/par', {
    client_id: clientId,
    response_type: 'code',
    redirect_uri: REDIRECT,
    scope,
    state,
    code_challenge: b64u(createHash('sha256').update(verifier).digest()),
    code_challenge_method: 'S256',
    login_hint: handle,
  })
  if (par.status !== 201) throw new Error(`PAR ${par.status} ${JSON.stringify(par.body)}`)
  const ru = par.body.request_uri
  const b = new Browser()
  let page = await b.go(`${base}/oauth/authorize?client_id=${encodeURIComponent(clientId)}&request_uri=${encodeURIComponent(ru)}`)
  if (page.html.includes('Choose an account')) page = await b.post(base, '/oauth/authorize/select', { request_uri: ru, csrf: hidden(page.html, 'csrf'), did: '' })
  if (page.html.includes('name="password"')) {
    page = await b.post(base, '/oauth/authorize/sign-in', { request_uri: ru, csrf: hidden(page.html, 'csrf'), identifier: handle, password, action: 'sign-in' })
  }
  if (!page.html.includes('Authorize access')) throw new Error(`expected the consent page, got ${page.status}: ${page.html.replace(/\s+/g, ' ').slice(0, 400)}`)
  const consent = await b.post(base, '/oauth/authorize/consent', { request_uri: ru, csrf: hidden(page.html, 'csrf'), did, action: 'allow' })
  const loc = consent.headers.get('location')
  if (consent.status !== 303 || !loc) throw new Error(`consent ${consent.status}: ${consent.html.slice(0, 300)}`)
  const q = new URL(loc.replace('#', '?')).searchParams
  if (q.get('error')) throw new Error(`consent error ${q.get('error')} ${q.get('error_description')}`)
  const tok = await asPost(base, key, '/oauth/token', { grant_type: 'authorization_code', client_id: clientId, code: q.get('code'), redirect_uri: REDIRECT, code_verifier: verifier })
  if (tok.status !== 200) throw new Error(`token ${tok.status} ${JSON.stringify(tok.body)}`)
  return new OAuthSession(base, key, tok.body)
}

export class OAuthSession {
  constructor(base, key, tok) {
    this.base = base
    this.key = key
    this.access = tok.access_token
    this.scope = tok.scope
  }
  /** XRPC with the DPoP-bound token: JSON in and out, or raw bytes (`bytes`/`type`, `raw`). */
  async xrpc(nsid, { params, body, bytes, type, raw } = {}) {
    const q = params ? `?${new URLSearchParams(Object.entries(params).filter(([, v]) => v !== undefined))}` : ''
    const htu = `${this.base}/xrpc/${nsid}`
    const method = body !== undefined || bytes ? 'POST' : 'GET'
    for (let i = 0; ; i++) {
      const headers = {
        authorization: `DPoP ${this.access}`,
        dpop: this.key.proof(method, htu, b64u(createHash('sha256').update(this.access).digest())),
      }
      if (bytes) headers['content-type'] = type
      else if (body !== undefined) headers['content-type'] = 'application/json'
      const res = await fetch(hostUrl(`${htu}${q}`), { method, headers, body: bytes ?? (body !== undefined ? JSON.stringify(body) : undefined) })
      this.key.absorb(res)
      if (res.status === 401 && i < 2 && /use_dpop_nonce/.test(res.headers.get('www-authenticate') ?? '')) continue
      const buf = Buffer.from(await res.arrayBuffer())
      if (!res.ok) {
        let j = {}
        try {
          j = JSON.parse(buf.toString())
        } catch {}
        const e = new Error(`${nsid} ${res.status} ${j.error ?? ''} ${j.message ?? buf.toString().slice(0, 200)}`)
        e.status = res.status
        e.error = j.error
        throw e
      }
      return raw ? buf : JSON.parse(buf.toString() || '{}')
    }
  }
}
