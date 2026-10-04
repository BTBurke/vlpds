// Fetching from a PDS that rate-limits: one 429 pauses every worker until
// the window resets. Shared by the migration copy and the account backup.

import { XrpcError } from './xrpc'

export type RateLimited = XrpcError & { retryAfter?: number }

/** Seconds a 429 asks us to wait: Retry-After, else RateLimit-Reset (epoch seconds). */
function waitSecs(get: (h: string) => string | null): number | undefined {
  const ra = Number(get('retry-after'))
  if (ra > 0) return ra
  const reset = Number(get('ratelimit-reset'))
  if (reset > 0) return Math.max(1, reset - Date.now() / 1000)
  return undefined
}

export function httpError(status: number, body: any, text: string, get: (h: string) => string | null): XrpcError {
  const e = new XrpcError(status, body?.error ?? `HTTP ${status}`, body?.message ?? text)
  if (status === 429) (e as RateLimited).retryAfter = waitSecs(get)
  return e
}

export async function fetchOk(url: string, init?: RequestInit): Promise<Response> {
  let r: Response
  try {
    r = await fetch(url, init)
  } catch (e) {
    if (init?.signal?.aborted) throw e
    throw new Error(`Couldn't reach ${new URL(url, location.href).host}. Check your connection and retry.`)
  }
  if (!r.ok) {
    let body: any = {}
    try {
      body = await r.json()
    } catch {
      /* not JSON */
    }
    throw httpError(r.status, body, '', (h) => r.headers.get(h))
  }
  return r
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

let pausedUntil = 0

export async function retry<T>(fn: () => Promise<T>, onPause: (until: number) => void, tries = 4, signal?: AbortSignal): Promise<T> {
  let failures = 0
  for (let limited = 0; ; ) {
    const wait = pausedUntil - Date.now()
    if (wait > 0) await sleep(wait)
    signal?.throwIfAborted()
    try {
      return await fn()
    } catch (e) {
      if (signal?.aborted) throw e
      if (e instanceof XrpcError && e.status === 429 && ++limited < 50) {
        const secs = Math.min(Math.max((e as RateLimited).retryAfter ?? 60, 2), 900)
        pausedUntil = Math.max(pausedUntil, Date.now() + secs * 1000)
        onPause(pausedUntil)
        continue
      }
      // a definite "no" doesn't change on retry
      if (e instanceof XrpcError && e.status >= 400 && e.status < 500 && e.status !== 408) throw e
      if (++failures >= tries) throw e
      await sleep(500 * 2 ** failures)
    }
  }
}
