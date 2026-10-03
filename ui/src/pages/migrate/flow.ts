// The migration's plumbing: a client per PDS (sessions refresh themselves),
// identity lookup, the copy jobs, and what survives a reload. Progress (no
// secrets) is kept in localStorage so a closed tab can pick up where it left
// off; session tokens and a signed PLC operation only in sessionStorage;
// passwords never leave memory.

import { call, XrpcError, type CallOpts, type Session } from '../../lib/xrpc'

export type Tokens = { accessJwt: string; refreshJwt: string }

// ---------------------------------------------------------------- persistence

export type StepId = 'find' | 'signin' | 'check' | 'handle' | 'create' | 'copy' | 'identity' | 'finish' | 'done'
export const STEPS: { id: StepId; label: string }[] = [
  { id: 'find', label: 'Find your account' },
  { id: 'signin', label: 'Sign in where you are now' },
  { id: 'check', label: 'Pre-flight checks' },
  { id: 'handle', label: 'Choose your handle' },
  { id: 'create', label: 'Create your account here' },
  { id: 'copy', label: 'Copy your data' },
  { id: 'identity', label: 'Move your identity' },
  { id: 'finish', label: 'Switch over' },
]

export type Saved = {
  did: string
  /** The old PDS's origin. */
  oldPds: string
  /** Where the old account signs in (differs for an entryway like bsky.social). */
  oldAuth: string
  oldHandle: string
  oldDomains: string[]
  checkedAt?: number
  newHandle?: string
  email?: string
  invite?: string
  created?: boolean
  repoDone?: boolean
  blobsDone?: boolean
  prefsDone?: boolean
  plcRequestedAt?: number
  identityDone?: boolean
  activated?: boolean
  oldDeactivated?: boolean
  /** Blobs the old server couldn't produce; reported, not retried forever. */
  unavailableBlobs?: string[]
  startedAt: number
  finishedAt?: number
}

const LKEY = 'vlpds.migrate'
const SKEY = 'vlpds.migrate.secrets'

export function loadSaved(): Saved | null {
  try {
    const s = localStorage.getItem(LKEY)
    return s ? JSON.parse(s) : null
  } catch {
    return null
  }
}

export function storeSaved(s: Saved | null) {
  try {
    if (s) localStorage.setItem(LKEY, JSON.stringify(s))
    else localStorage.removeItem(LKEY)
  } catch {
    /* storage blocked: this tab only */
  }
}

type Secrets = { did?: string; old?: Tokens; new?: Tokens; op?: unknown }

function loadSecrets(): Secrets {
  try {
    return JSON.parse(sessionStorage.getItem(SKEY) ?? '{}')
  } catch {
    return {}
  }
}

function storeSecrets(s: Secrets) {
  try {
    sessionStorage.setItem(SKEY, JSON.stringify(s))
  } catch {
    /* memory only */
  }
}

export function clearSecrets() {
  try {
    sessionStorage.removeItem(SKEY)
  } catch {
    /* nothing kept */
  }
}

/** A PLC operation signed by the old server but not yet accepted here. */
export function signedOp(did: string): unknown | undefined {
  const s = loadSecrets()
  return s.did === did ? s.op : undefined
}

export function keepSignedOp(did: string, op: unknown | undefined) {
  const s = loadSecrets()
  storeSecrets({ ...s, did, op })
}

// ---------------------------------------------------------------- clients

function jwtPayload(jwt: string): Record<string, any> {
  try {
    const b = jwt.split('.')[1].replace(/-/g, '+').replace(/_/g, '/')
    return JSON.parse(atob(b + '='.repeat((4 - (b.length % 4)) % 4)))
  } catch {
    return {}
  }
}

const expiredJwt = (jwt: string) => {
  const exp = jwtPayload(jwt).exp
  return typeof exp === 'number' && exp * 1000 < Date.now() + 10_000
}

/** A signed-in account on one PDS. `base` '' is this server. */
export class Pds {
  tokens?: Tokens
  constructor(
    readonly side: 'old' | 'new',
    readonly base: string,
    readonly did: string,
    /** createSession / refreshSession go here (an entryway may hold the password). */
    readonly authBase: string = base,
  ) {
    const s = loadSecrets()
    if (s.did === did && s[side]) this.tokens = s[side]
  }

  private keep(t: Tokens | undefined) {
    this.tokens = t
    const s = loadSecrets()
    storeSecrets({ ...(s.did === this.did ? s : {}), did: this.did, [this.side]: t })
  }

  forget() {
    this.keep(undefined)
  }

  /** Throws XrpcError AuthFactorTokenRequired when the account wants an emailed code. */
  async login(password: string, authFactorToken?: string): Promise<Session & Record<string, any>> {
    const out = await call('com.atproto.server.createSession', {
      base: this.authBase,
      body: { identifier: this.did, password, authFactorToken: authFactorToken || undefined },
    })
    if (out.did !== this.did) throw new Error(`Signed in as ${out.did}, not ${this.did}. Check the account you entered.`)
    const scope = jwtPayload(out.accessJwt).scope
    if (scope && scope !== 'com.atproto.access') {
      throw new XrpcError(
        400,
        'AppPassword',
        'That is an app password. Moving an account needs your main password: the one you use to sign in at your current server.',
      )
    }
    this.keep({ accessJwt: out.accessJwt, refreshJwt: out.refreshJwt })
    return out
  }

  /** Adopts a session another call created (createAccount). */
  adopt(t: Tokens) {
    this.keep({ accessJwt: t.accessJwt, refreshJwt: t.refreshJwt })
  }

  private async refresh() {
    const t = this.tokens
    if (!t) throw new XrpcError(401, 'AuthenticationRequired', 'Sign in again')
    try {
      const out = await call('com.atproto.server.refreshSession', { base: this.authBase, method: 'POST', auth: `Bearer ${t.refreshJwt}` })
      this.keep({ accessJwt: out.accessJwt, refreshJwt: out.refreshJwt })
    } catch (e) {
      if (e instanceof XrpcError && e.status < 500) this.forget()
      throw e
    }
  }

  /** The access token, refreshed first if it is about to expire (long uploads). */
  async auth(): Promise<string> {
    if (!this.tokens) throw new XrpcError(401, 'AuthenticationRequired', 'Sign in again')
    if (expiredJwt(this.tokens.accessJwt)) await this.refresh()
    return `Bearer ${this.tokens!.accessJwt}`
  }

  async call<T = any>(nsid: string, o: CallOpts = {}): Promise<T> {
    const go = async () => call<T>(nsid, { ...o, base: o.base ?? this.base, auth: await this.auth() })
    try {
      return await go()
    } catch (e) {
      // ExpiredToken is also what a stale emailed code gets ("Token is
      // expired"): refresh only when the session itself ran out
      const session = e instanceof XrpcError && e.error === 'ExpiredToken' && (/token has expired/i.test(e.message) || expiredJwt(this.tokens?.accessJwt ?? ''))
      if (!session) throw e
      await this.refresh()
      return go()
    }
  }
}

// ---------------------------------------------------------------- identity

export type DidDoc = {
  id: string
  alsoKnownAs?: string[]
  verificationMethod?: { id: string; type: string; publicKeyMultibase?: string }[]
  service?: { id: string; type: string; serviceEndpoint: string }[]
}

export function pdsOf(doc: DidDoc): string | undefined {
  const s = doc.service?.find((s) => s.id.endsWith('#atproto_pds') && s.type === 'AtprotoPersonalDataServer')
  return s?.serviceEndpoint?.replace(/\/+$/, '')
}

export function signingKeyOf(doc: DidDoc): string | undefined {
  const m = doc.verificationMethod?.find((v) => v.id.endsWith('#atproto'))
  return m?.publicKeyMultibase ? `did:key:${m.publicKeyMultibase}` : undefined
}

export function handleOf(doc: DidDoc): string | undefined {
  return doc.alsoKnownAs?.find((a) => a.startsWith('at://'))?.slice(5)
}

/** bsky.social accounts live on *.host.bsky.network PDSes but sign in, and
 * sign PLC operations, at the entryway. */
export function authHostFor(pds: string): string {
  try {
    if (new URL(pds).hostname.endsWith('.host.bsky.network')) return 'https://bsky.social'
  } catch {
    /* not a URL: left to fail on use */
  }
  return pds
}

export function normalizeHost(input: string): string {
  let s = input.trim().replace(/\/+$/, '')
  if (!/^https?:\/\//i.test(s)) s = `https://${s}`
  return new URL(s).origin
}

export type Found = { did: string; handle: string; doc: DidDoc; pds: string }

/** Looks the account up through this server (DNS, HTTPS and PLC are server-side lookups). */
export async function findAccount(identifier: string, hostHint?: string): Promise<Found> {
  const id = identifier.trim().replace(/^@/, '').toLowerCase()
  let did = id
  if (!id.startsWith('did:')) {
    if (hostHint) {
      const r = await call('com.atproto.identity.resolveHandle', { base: normalizeHost(hostHint), params: { handle: id } })
      did = r.did
    } else {
      try {
        const r = await call('com.atproto.identity.resolveIdentity', { params: { identifier: id } })
        return found(r.did, r.didDoc, id)
      } catch (e) {
        if (e instanceof XrpcError && e.status < 500) throw new HandleUnresolved(id)
        throw e
      }
    }
  }
  if (did.startsWith('did:web:')) throw new DidWeb(did)
  const r = await call('com.atproto.identity.resolveIdentity', { params: { identifier: did } })
  return found(r.did, r.didDoc, id.startsWith('did:') ? undefined : id)
}

function found(did: string, doc: DidDoc, typed?: string): Found {
  if (did.startsWith('did:web:')) throw new DidWeb(did)
  if (!did.startsWith('did:plc:')) throw new Error(`${did} is not a did:plc; only did:plc accounts can move with this page.`)
  const pds = pdsOf(doc)
  if (!pds) throw new Error('Your DID document names no PDS, so there is no account to move.')
  return { did, doc, pds, handle: handleOf(doc) ?? typed ?? did }
}

export class HandleUnresolved extends Error {
  constructor(readonly handle: string) {
    super(`We couldn't look up @${handle}.`)
  }
}

export class DidWeb extends Error {
  constructor(readonly did: string) {
    super(`${did} is a did:web.`)
  }
}

// ---------------------------------------------------------------- copy jobs

export type Progress = { done: number; total?: number; note?: string }

/** Reads a fetch body, reporting bytes as they arrive. */
async function readAll(r: Response, onBytes: (n: number) => void): Promise<Blob> {
  if (!r.body) return r.blob()
  const reader = r.body.getReader()
  const parts: BlobPart[] = []
  let n = 0
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    parts.push(value)
    n += value.byteLength
    onBytes(n)
  }
  return new Blob(parts, { type: 'application/vnd.ipld.car' })
}

/** POSTs a body here with upload progress (fetch has none). */
function upload(nsid: string, body: Blob, contentType: string, auth: string, onBytes: (n: number) => void): Promise<any> {
  return new Promise((resolve, reject) => {
    const x = new XMLHttpRequest()
    x.open('POST', `/xrpc/${nsid}`)
    x.setRequestHeader('Authorization', auth)
    x.setRequestHeader('Content-Type', contentType)
    x.upload.onprogress = (e) => onBytes(e.loaded)
    x.onerror = () => reject(new Error('The upload was interrupted (network error). Retry to send it again.'))
    x.onload = () => {
      let j: any = undefined
      try {
        j = x.responseText ? JSON.parse(x.responseText) : undefined
      } catch {
        /* not JSON */
      }
      if (x.status >= 200 && x.status < 300) resolve(j)
      else reject(new XrpcError(x.status, j?.error ?? `HTTP ${x.status}`, j?.message ?? x.responseText))
    }
    x.send(body)
  })
}

export async function fetchOk(url: string): Promise<Response> {
  let r: Response
  try {
    r = await fetch(url)
  } catch {
    throw new Error(`Couldn't reach ${new URL(url).host}. Check your connection and retry.`)
  }
  if (!r.ok) {
    let body: any = {}
    try {
      body = await r.json()
    } catch {
      /* not JSON */
    }
    throw new XrpcError(r.status, body.error ?? `HTTP ${r.status}`, body.message ?? '')
  }
  return r
}

/** getRepo from the old server, importRepo here. Re-running replaces the copy. */
export async function copyRepo(oldPds: Pds, newPds: Pds, onProgress: (phase: 'download' | 'upload', bytes: number, total?: number) => void) {
  const r = await fetchOk(`${oldPds.base}/xrpc/com.atproto.sync.getRepo?did=${encodeURIComponent(oldPds.did)}`)
  const len = Number(r.headers.get('content-length')) || undefined
  const car = await readAll(r, (n) => onProgress('download', n, len))
  onProgress('upload', 0, car.size)
  await upload('com.atproto.repo.importRepo', car, 'application/vnd.ipld.car', await newPds.auth(), (n) => onProgress('upload', n, car.size))
  return car.size
}

export type BlobResult = { copied: number; failed: { cid: string; reason: string }[]; remaining: number }

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

async function retry<T>(fn: () => Promise<T>, tries = 4): Promise<T> {
  let last: unknown
  for (let i = 0; i < tries; i++) {
    try {
      return await fn()
    } catch (e) {
      last = e
      // a definite "no" doesn't change on retry
      if (e instanceof XrpcError && e.status >= 400 && e.status < 500 && e.status !== 408 && e.status !== 429) throw e
      await sleep(500 * 2 ** i)
    }
  }
  throw last
}

/** One pass over listMissingBlobs: copy each blob the new server lacks.
 * Safe to stop and re-run at any point; the list shrinks as blobs land. */
export async function copyBlobs(
  oldPds: Pds,
  newPds: Pds,
  skip: Set<string>,
  onCopied: (cid: string, bytes: number) => void,
  shouldStop: () => boolean,
  concurrency = 4,
): Promise<BlobResult> {
  const failed: { cid: string; reason: string }[] = []
  let copied = 0
  let cursor: string | undefined
  const queue: string[] = []
  for (;;) {
    const page = await newPds.call('com.atproto.repo.listMissingBlobs', { params: { limit: 500, cursor } })
    for (const b of page.blobs as { cid: string }[]) if (!skip.has(b.cid)) queue.push(b.cid)
    cursor = page.cursor
    if (!cursor || !page.blobs.length) break
  }
  const one = async (cid: string) => {
    try {
      await retry(async () => {
        const r = await fetchOk(`${oldPds.base}/xrpc/com.atproto.sync.getBlob?did=${encodeURIComponent(oldPds.did)}&cid=${encodeURIComponent(cid)}`)
        const body = await r.blob()
        const type = r.headers.get('content-type') || 'application/octet-stream'
        const out = await upload('com.atproto.repo.uploadBlob', body, type, await newPds.auth(), () => {})
        const got = out?.blob?.ref?.$link
        if (got && got !== cid) throw new Error(`the copy hashed to ${got}, not ${cid}`)
        copied++
        onCopied(cid, body.size)
      })
    } catch (e) {
      failed.push({ cid, reason: e instanceof Error ? e.message : String(e) })
    }
  }
  const workers = Array.from({ length: concurrency }, async () => {
    while (queue.length && !shouldStop()) await one(queue.shift()!)
  })
  await Promise.all(workers)
  return { copied, failed, remaining: queue.length }
}

export type AccountStatus = {
  activated: boolean
  validDid: boolean
  repoCommit: string
  repoBlocks: number
  indexedRecords: number
  expectedBlobs: number
  importedBlobs: number
}

export const shortKey = (k?: string) => (k ? (k.length > 28 ? `${k.slice(0, 16)}…${k.slice(-8)}` : k) : '—')
