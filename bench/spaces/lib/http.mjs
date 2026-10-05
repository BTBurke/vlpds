// HTTP plumbing: host-side fetch, the reference XRPC client over a pluggable
// auth layer, and the errors scenarios classify on.
import { AtpBaseClient } from '@atproto/api'

/** A method the server answered 501 for (or doesn't route): a scenario using it is "not implemented". */
export class NotImplemented extends Error {
  constructor(nsid, detail = '') {
    super(`${nsid} not implemented ${detail}`.trim())
    this.nsid = nsid
  }
}

/** The NotImplemented behind an error, if any (the XRPC client wraps what the fetch handler throws). */
export function notImplemented(e) {
  for (let x = e, i = 0; x && i < 5; x = x.cause, i++) if (x instanceof NotImplemented) return x
  const m = /(\S+) not implemented \((\d+)\)/.exec(e?.message ?? '')
  return m ? new NotImplemented(m[1], `(${m[2]})`) : null
}

// "localhost" can resolve to ::1 first, where nothing listens on the host
export const hostUrl = (u) => String(u).replace('//localhost:', '//127.0.0.1:')

/** Per-request timings of the driver, by NSID, for the cost report. */
export const timings = new Map()
export function recordTiming(nsid, ms) {
  let a = timings.get(nsid)
  if (!a) timings.set(nsid, (a = []))
  a.push(ms)
}

export async function hfetch(url, init = {}) {
  const t0 = performance.now()
  const res = await fetch(hostUrl(url), { ...init, redirect: 'manual' })
  const nsid = /\/xrpc\/([^?]+)/.exec(String(url))?.[1]
  if (nsid) recordTiming(nsid, performance.now() - t0)
  return res
}

function nsidOf(path) {
  return /\/xrpc\/([^?]+)/.exec(path)?.[1] ?? path
}

/**
 * The reference XRPC client (@atproto/api at the alpha) against `base`, with
 * `sign(request)` adding auth headers to each request (and retrying it when it
 * returns a new Request, for DPoP nonces). 501 becomes {@link NotImplemented}.
 */
export function makeClient(base, sign = null) {
  const handler = async (path, init) => {
    const url = new URL(path, base).toString()
    const nsid = nsidOf(path)
    let headers = new Headers(init.headers)
    let res
    for (let attempt = 0; attempt < 3; attempt++) {
      const h = new Headers(headers)
      if (sign) await sign({ method: (init.method ?? 'GET').toUpperCase(), url, headers: h, body: init.body, attempt })
      res = await hfetch(url, { ...init, headers: h })
      if (sign?.retry && (await sign.retry(res, { method: (init.method ?? 'GET').toUpperCase(), url }))) continue
      break
    }
    if (res.status === 501) throw new NotImplemented(nsid, `(${res.status})`)
    if (res.status === 404) {
      const body = await res.clone().text()
      if (/MethodNotImplemented|XRPCNotSupported|Method Not Implemented/i.test(body)) throw new NotImplemented(nsid, '(404)')
    }
    return res
  }
  return new AtpBaseClient(handler)
}

/** Call and return `{ok, status, error, message, data}` instead of throwing XRPC errors. */
export async function attempt(fn) {
  try {
    const r = await fn()
    return { ok: true, status: 200, data: r.data, headers: r.headers }
  } catch (e) {
    const ni = notImplemented(e)
    if (ni) throw ni
    if (e && typeof e.status === 'number') return { ok: false, status: e.status, error: e.error, message: e.message }
    throw e
  }
}

/** Raw XRPC request (for wire tests the typed client can't express). */
export async function rawXrpc(base, nsid, { method = 'GET', params, body, headers = {}, contentType } = {}) {
  const u = new URL(`/xrpc/${nsid}`, base)
  for (const [k, v] of Object.entries(params ?? {})) {
    if (Array.isArray(v)) v.forEach((x) => u.searchParams.append(k, x))
    else if (v !== undefined) u.searchParams.set(k, String(v))
  }
  const init = { method, headers: { ...headers } }
  if (body !== undefined) {
    if (body instanceof Uint8Array) {
      init.body = body
      init.headers['content-type'] = contentType ?? 'application/octet-stream'
    } else {
      init.body = JSON.stringify(body)
      init.headers['content-type'] = 'application/json'
    }
  }
  const res = await hfetch(u, init)
  if (res.status === 501) throw new NotImplemented(nsid, '(501)')
  const buf = new Uint8Array(await res.arrayBuffer())
  let json
  try {
    json = JSON.parse(new TextDecoder().decode(buf))
  } catch {}
  if (res.status === 404 && /MethodNotImplemented|XRPCNotSupported/.test(json?.error ?? '')) throw new NotImplemented(nsid, '(404)')
  return { status: res.status, ok: res.ok, headers: res.headers, bytes: buf, json, error: json?.error, message: json?.message }
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

export async function waitFor(what, fn, { timeoutMs = 15000, intervalMs = 100 } = {}) {
  const t0 = Date.now()
  let last
  while (Date.now() - t0 < timeoutMs) {
    try {
      last = await fn()
      if (last) return last
    } catch (e) {
      const ni = notImplemented(e)
      if (ni) throw ni
      last = e
    }
    await sleep(intervalMs)
  }
  throw new Error(`timed out waiting for ${what}${last instanceof Error ? `: ${last.message}` : ''}`)
}
