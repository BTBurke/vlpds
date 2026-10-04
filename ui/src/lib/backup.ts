// A whole-account backup as one ZIP, built in the browser: the repo CAR,
// every blob (checked against its CID), preferences, the DID document and
// PLC audit log, and a README on restoring. client-zip writes it as a
// stream (stored, not deflated: blobs are already compressed media), so
// with the File System Access API it goes straight to disk; elsewhere it is
// collected in memory and saved as a blob: download.

import { makeZip } from 'client-zip'
import { fetchOk, retry } from './backoff'
import { call, type CallOpts } from './xrpc'

/** The server the backup is read from, signed in as the account. */
export type BackupSource = {
  /** Origin; '' is this server. */
  base: string
  did: string
  handle: string
  call: <T = any>(nsid: string, o?: CallOpts) => Promise<T>
}

export type BackupProgress = {
  phase: 'prepare' | 'repo' | 'blobs' | 'finish'
  bytes: number
  blobsDone: number
  blobsTotal?: number
  missing: number
  pausedUntil?: number
}

export type BackupSummary = { name: string; bytes: number; blobs: number; missing: { cid: string; reason: string }[]; notes: string[]; streamed: boolean }

export type BackupOptions = {
  source: BackupSource
  /** Generated in this browser session and opted into: the user's own PLC key. */
  recoveryKey?: { privateHex: string; didKey: string }
  /** Extra JSON files (path -> value), read when the backup starts. */
  extras?: () => Promise<Record<string, unknown>>
  signal: AbortSignal
  onProgress: (p: BackupProgress) => void
}

type SaveTarget = { createWritable(): Promise<WritableStream<Uint8Array>> }

export const canStreamToDisk = () => typeof (window as any).showSaveFilePicker === 'function'

export function backupName(handle: string) {
  return `${handle.replace(/[^a-zA-Z0-9.-]/g, '_')}-backup-${new Date().toISOString().slice(0, 10)}.zip`
}

/** Asks where to save. Call it before anything else awaits in the click
 * handler: the picker needs the click's user activation. null: no picker
 * here (build in memory instead). Throws AbortError when the user cancels. */
export async function pickBackupFile(name: string): Promise<SaveTarget | null> {
  if (!canStreamToDisk()) return null
  return (window as any).showSaveFilePicker({
    suggestedName: name,
    types: [{ description: 'ZIP archive', accept: { 'application/zip': ['.zip'] } }],
  })
}

/** Rough size before downloading anything, for browsers that hold the whole
 * ZIP in memory: blob sizes aren't listed, so this guesses per photo. */
export function estimateBytes(st: { repoBlocks?: number; expectedBlobs?: number }) {
  return (st.repoBlocks ?? 0) * 300 + (st.expectedBlobs ?? 0) * 400_000
}

export async function writeBackup(o: BackupOptions, target: SaveTarget | null, name: string): Promise<BackupSummary> {
  const sum: BackupSummary = { name, bytes: 0, blobs: 0, missing: [], notes: [], streamed: !!target }
  const p: BackupProgress = { phase: 'prepare', bytes: 0, blobsDone: 0, missing: 0 }
  const report = (patch: Partial<BackupProgress> = {}) => {
    Object.assign(p, patch)
    o.onProgress({ ...p })
  }
  report()
  const zip = makeZip(entries(o, sum, report), { buffersAreUTF8: true }).pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      transform(c, ctl) {
        sum.bytes += c.byteLength
        report({ bytes: sum.bytes })
        ctl.enqueue(c)
      },
    }),
  )
  if (target) {
    // an aborted pipe aborts the writable, which discards the partial file
    await zip.pipeTo(await target.createWritable(), { signal: o.signal })
  } else {
    const parts: BlobPart[] = []
    await zip.pipeTo(new WritableStream({ write: (c) => void parts.push(c as BlobPart) }), { signal: o.signal })
    saveBlobAs(new Blob(parts, { type: 'application/zip' }), name)
  }
  return sum
}

function saveBlobAs(blob: Blob, name: string) {
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = name
  document.body.appendChild(a)
  a.click()
  a.remove()
  // the download has to start reading before the URL goes
  setTimeout(() => URL.revokeObjectURL(url), 60_000)
}

const enc = new TextEncoder()
const json = (v: unknown) => enc.encode(`${JSON.stringify(v, null, 2)}\n`)

const PREFETCH = 4

async function* entries(o: BackupOptions, sum: BackupSummary, report: (p?: Partial<BackupProgress>) => void) {
  const { source: src, signal } = o
  const lastModified = new Date()
  const did = src.did
  const here = src.base === ''
  const origin = here ? location.origin : src.base
  const onPause = (until: number) => report({ pausedUntil: until })
  const files: string[] = []
  const file = (name: string, input: Uint8Array | ReadableStream<Uint8Array>) => {
    files.push(name)
    return { name, input, lastModified }
  }
  const opt = async <T>(what: string, f: () => Promise<T>): Promise<T | undefined> => {
    try {
      return await retry(f, onPause, 3, signal)
    } catch (e) {
      if (signal.aborted) throw e
      sum.notes.push(`${what}: not included (${e instanceof Error ? e.message : String(e)})`)
      return undefined
    }
  }

  const session = await retry(() => src.call('com.atproto.server.getSession', { signal }), onPause, 3, signal)
  const status = await opt('account status', () => src.call('com.atproto.server.checkAccountStatus', { signal }))
  const prefs = await opt('preferences.json', () => src.call('app.bsky.actor.getPreferences', { signal }))
  // the DID document and PLC log come through this server either way: it
  // resolves for any account it holds, and the page may only reach it
  const doc = await opt('identity/did.json', () => call('com.atproto.identity.resolveDid', { params: { did }, signal }).then((r) => r.didDoc))
  const audit = did.startsWith('did:plc:')
    ? await opt('identity/plc-audit-log.json', () => call('vlpds.identity.getPlcAuditLog', { params: { did }, signal }).then((r) => r.log))
    : undefined
  const extras = o.extras ? await opt('server extras', o.extras) : undefined
  const head = await opt('latest commit', () => src.call('com.atproto.sync.getLatestCommit', { params: { did }, signal }))

  yield file('README.txt', enc.encode(readme(src, session.handle ?? src.handle, origin, lastModified, !!o.recoveryKey, Object.keys(extras ?? {}))))
  if (prefs) yield file('preferences.json', json(prefs))
  if (doc) yield file('identity/did.json', json(doc))
  if (audit) yield file('identity/plc-audit-log.json', json(audit))

  report({ phase: 'repo' })
  const repo = await retry(() => src.call<Response>('com.atproto.sync.getRepo', { params: { did }, raw: true, signal }), onPause, 4, signal)
  let carBytes = 0
  yield file(
    'repo.car',
    repo.body!.pipeThrough(
      new TransformStream<Uint8Array, Uint8Array>({
        transform(c, ctl) {
          carBytes += c.byteLength
          ctl.enqueue(c)
        },
      }),
    ),
  )

  report({ phase: 'blobs' })
  const cids: string[] = []
  for (let cursor: string | undefined; ; ) {
    const page = await retry(
      () =>
        here
          ? src.call('com.atproto.sync.listBlobs', { params: { did, limit: 1000, cursor }, signal })
          : fetchOk(`${src.base}/xrpc/com.atproto.sync.listBlobs?did=${encodeURIComponent(did)}&limit=1000${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`, { signal }).then((r) => r.json()),
      onPause,
      4,
      signal,
    )
    cids.push(...(page.cids as string[]))
    cursor = page.cursor
    if (!cursor || !page.cids.length) break
  }
  report({ blobsTotal: cids.length })

  // a cross-origin request with Authorization is preflighted, per blob URL;
  // the old server serves an active account's blobs without it anyway
  const getBlob = (cid: string): Promise<Response> =>
    here
      ? src.call<Response>('com.atproto.sync.getBlob', { params: { did, cid }, raw: true, signal })
      : fetchOk(`${src.base}/xrpc/com.atproto.sync.getBlob?did=${encodeURIComponent(did)}&cid=${encodeURIComponent(cid)}`, { signal })
  const fetchOne = async (cid: string): Promise<{ cid: string; bytes?: Uint8Array; reason?: string }> => {
    try {
      const bytes = await retry(async () => new Uint8Array(await (await getBlob(cid)).arrayBuffer()), onPause, 4, signal)
      const want = cidSha256(cid)
      if (!want) return { cid, bytes }
      const got = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))
      return equal(got, want) ? { cid, bytes } : { cid, reason: 'downloaded bytes do not match the CID (sha-256)' }
    } catch (e) {
      if (signal.aborted) throw e
      return { cid, reason: e instanceof Error ? e.message : String(e) }
    }
  }
  const inflight: Promise<Awaited<ReturnType<typeof fetchOne>>>[] = []
  let next = 0
  const fill = () => {
    while (inflight.length < PREFETCH && next < cids.length) {
      const f = fetchOne(cids[next++])
      f.catch(() => {}) // an abort rejects the ones still queued; the awaited one reports it
      inflight.push(f)
    }
  }
  fill()
  let done = 0
  while (inflight.length) {
    const r = await inflight.shift()!
    fill()
    done++
    if (r.bytes) {
      sum.blobs++
      report({ blobsDone: done, pausedUntil: undefined })
      yield file(`blobs/${r.cid}`, r.bytes)
    } else {
      sum.missing.push({ cid: r.cid, reason: r.reason ?? 'unknown' })
      report({ blobsDone: done, missing: sum.missing.length })
    }
  }

  report({ phase: 'finish' })
  if (sum.missing.length) {
    const lines = sum.missing.map((m) => `${m.cid}\t${m.reason.replace(/\s+/g, ' ')}`)
    yield file('missing-blobs.txt', enc.encode(`# Blobs the server listed but could not provide (CID, reason). Not in blobs/.\n${lines.join('\n')}\n`))
  }
  for (const [path, v] of Object.entries(extras ?? {})) yield file(path, json(v))
  if (o.recoveryKey) {
    yield file(
      'keys/recovery-key.txt',
      enc.encode(
        `Your PLC recovery (rotation) key, made in your browser. Keep this file secret and offline.\n\nprivate key (secp256k1, hex): ${o.recoveryKey.privateHex}\npublic key: ${o.recoveryKey.didKey}\n`,
      ),
    )
  }
  const created = Array.isArray(audit) ? audit[0]?.createdAt : undefined
  yield file(
    'account.json',
    json({
      did,
      handle: session.handle ?? src.handle,
      email: session.email ?? null,
      emailConfirmed: session.emailConfirmed ?? null,
      server: origin,
      identityCreatedAt: created ?? null,
      exportedAt: lastModified.toISOString(),
      latestCommit: head ? { cid: head.cid, rev: head.rev } : null,
      counts: {
        records: status?.indexedRecords ?? null,
        repoBlocks: status?.repoBlocks ?? null,
        repoCarBytes: carBytes,
        blobsListed: cids.length,
        blobsIncluded: sum.blobs,
        blobsMissing: sum.missing.length,
        preferences: Array.isArray(prefs?.preferences) ? prefs.preferences.length : null,
      },
      notes: sum.notes,
      files: [...files, 'account.json'].filter((f) => !f.startsWith('blobs/')),
    }),
  )
}

// ---------------------------------------------------------------- CIDs

const B32 = 'abcdefghijklmnopqrstuvwxyz234567'

function base32(s: string): Uint8Array | null {
  const out: number[] = []
  let bits = 0
  let acc = 0
  for (const c of s) {
    const v = B32.indexOf(c)
    if (v < 0) return null
    acc = (acc << 5) | v
    bits += 5
    if (bits >= 8) {
      bits -= 8
      out.push((acc >> bits) & 0xff)
    }
  }
  return new Uint8Array(out)
}

function varint(b: Uint8Array, at: number): [number, number] {
  let n = 0
  for (let shift = 1; at < b.length; shift *= 128) {
    const x = b[at++]
    n += (x & 0x7f) * shift
    if (x < 0x80) return [n, at]
  }
  return [-1, at]
}

/** The sha-256 digest a CIDv1 (base32) names; undefined for any other hash. */
export function cidSha256(cid: string): Uint8Array | undefined {
  if (!cid.startsWith('b')) return undefined
  const b = base32(cid.slice(1))
  if (!b) return undefined
  const [version, p1] = varint(b, 0)
  if (version !== 1) return undefined
  const [, p2] = varint(b, p1)
  if (b[p2] !== 0x12 || b[p2 + 1] !== 0x20 || b.length !== p2 + 34) return undefined
  return b.subarray(p2 + 2)
}

const equal = (a: Uint8Array, b: Uint8Array) => a.length === b.length && a.every((x, i) => x === b[i])

// ---------------------------------------------------------------- README

const EXTRAS: Record<string, string> = {
  'vlpds/app-passwords.json': 'App password names and dates (no secrets).',
  'vlpds/connected-apps.json': 'Apps signed in with OAuth: client and scope.',
  'vlpds/rotation-keys.json': 'Your PLC rotation keys (public did:keys).',
}

function readme(src: BackupSource, handle: string, origin: string, at: Date, withKey: boolean, extras: string[]) {
  const host = new URL(origin).host
  return `Backup of @${handle} (${src.did})
Taken from ${host} on ${at.toISOString()}.

This archive holds everything needed to rebuild the account on any atproto
server (PDS). Passwords, app passwords and sign-in sessions are never
included.

What's inside
-------------
repo.car                     Your repository: every post, like, follow, block,
                             list and profile record, signed, in the standard
                             CAR format (com.atproto.sync.getRepo).
blobs/<cid>                  Every image and video the repository references,
                             one file per blob, named by its CID. Each was
                             checked against its CID's sha-256 when saved.
missing-blobs.txt            (only if needed) Blobs the server listed but
                             could not provide, with the reason. They are not
                             in blobs/.
preferences.json             Private app settings: saved feeds, muted words,
                             content filters (app.bsky.actor.getPreferences).
identity/did.json            Your DID document as of the backup: handle,
                             signing key and hosting server.
identity/plc-audit-log.json  (did:plc only) The PLC directory's full history
                             of your identity.
account.json                 Handle, email, server, dates and counts.
${extras.map((e) => `${e.padEnd(29)}${EXTRAS[e] ?? 'Details only this server keeps.'}\n`).join('')}${withKey ? 'keys/recovery-key.txt        The recovery (rotation) private key you made in\n                             your browser. Anyone with it can take over your\n                             identity: keep this archive offline.\n' : ''}
Restoring on a new server
-------------------------
1. Create the account on the new server with your existing DID
   (com.atproto.server.createAccount with "did"; the new server may need a
   service-auth token from your old server, or an invite).
2. Import the repository: POST repo.car to com.atproto.repo.importRepo
   (Content-Type: application/vnd.ipld.car).
3. Upload every file in blobs/ with com.atproto.repo.uploadBlob; the server
   recomputes each CID, which should equal the file name.
   com.atproto.repo.listMissingBlobs tells you what is still missing.
4. Restore settings: POST the contents of preferences.json to
   app.bsky.actor.putPreferences.
5. Point your identity at the new server. For a did:plc, the server that
   holds your rotation keys signs a PLC operation naming the new PDS and its
   signing key (com.atproto.identity.signPlcOperation, then
   submitPlcOperation on the new server). If that server is gone, a
   rotation key you hold yourself (a recovery key) can sign the operation
   directly against the PLC directory. identity/plc-audit-log.json shows
   which keys are current.
6. Activate the account on the new server (com.atproto.server.activateAccount).

Keys
----
The repository signing key stays on the server that hosts you and is not
exportable: a new server signs with a key of its own once your identity
points to it. Recovering an identity whose server is unreachable relies on a
recovery key you hold${withKey ? ' (see keys/recovery-key.txt)' : ''} or on that server's operator.
`
}
