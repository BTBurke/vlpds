// The boards client: what a boards app does for a signed-in member, on top
// of the space methods. A board is a simplespace space of type
// dev.example.boards.board whose authority is the owner's DID. Every write goes
// to the member's own space repo; reads of other members go through a space
// credential, and the appview's API takes the same credential.
import { createHash } from 'node:crypto'
import { attempt, hfetch, rawXrpc } from '../lib/http.mjs'
import { POLICY, credentialFor } from '../lib/space.mjs'
import { BOARD_TYPE, C, parseRecordUri, recordUri } from './model.mjs'

const ACTIONS = 'action=read&action=create&action=update&action=delete'
const MEMBER_COLLS = [C.post, C.comment, C.vote].map((c) => `collection=${c}`).join('&')

// Explicit collections: vlpds resolves a bare grant's collections from the
// type's declaration over DNS (_lexicon.boards.example.com), which this stack
// doesn't have.
export const SCOPES = {
  member: ['atproto', `space:${BOARD_TYPE}?authority=*&${MEMBER_COLLS}&${ACTIONS}`, 'blob:image/*'].join(' '),
  owner: [
    'atproto',
    `space:${BOARD_TYPE}?authority=*&collection=${C.settings}&${MEMBER_COLLS}&${ACTIONS}&manage=create&manage=update&manage=delete`,
    'blob:image/*',
    'rpc:com.atproto.space.notifyCredentialRevoked?aud=*',
  ].join(' '),
  // the appview's own account only ever reads
  reader: ['atproto', `space:${BOARD_TYPE}?authority=*&action=read`].join(' '),
  // what a boards app asks for once the declaration resolves (C6)
  bare: ['atproto', `space:${BOARD_TYPE}?authority=*&${ACTIONS}`].join(' '),
}

/** A vote's rkey: one per (voter, subject), so changing a vote is a put. */
export const voteRkey = (subject) => `v${createHash('sha256').update(subject).digest('hex').slice(0, 24)}`

let tidSeq = 0
/** A TID-shaped rkey (microseconds, base32-sortable) for the record key type 'tid'. */
export function tid(ms = Date.now()) {
  const S32 = '234567abcdefghijklmnopqrstuvwxyz'
  let n = BigInt(ms) * 1000n + BigInt(tidSeq++ % 1000)
  let out = ''
  for (let i = 0; i < 11; i++) {
    out = S32[Number(n & 31n)] + out
    n >>= 5n
  }
  const clock = S32[Math.floor(Math.random() * 32)] + S32[Math.floor(Math.random() * 32)]
  return `${out}${clock}`
}

export class BoardsClient {
  /**
   * `actor`: a harness Actor (OAuth on vlpds, a password session on a
   * reference PDS). `onWrite(board, did, path, cid|null, value)` sees every acked
   * write (the runner's ground truth).
   */
  constructor(actor, { onWrite } = {}) {
    this.actor = actor
    this.did = actor.did
    this.onWrite = onWrite ?? (() => {})
    this.creds = new Map()
  }

  get space() {
    return this.actor.client.com.atproto.space
  }

  get simple() {
    return this.actor.client.com.atproto.simplespace
  }

  // board lifecycle (owner)

  async createBoard(skey, { name, description, flairs = [] }) {
    const r = await this.simple.createSpace({
      spaceType: BOARD_TYPE,
      skey,
      readPolicy: POLICY.members,
      writePolicy: POLICY.members,
      appAccess: POLICY.open,
    })
    const board = r.data.uri
    await this.putSettings(board, { name, description, flairs, pinned: [], removed: [] })
    return board
  }

  async getSettings(board) {
    const r = await attempt(() => this.space.getRecord({ space: board, repo: this.did, collection: C.settings, rkey: 'self' }))
    return r.ok ? r.data.value : null
  }

  async putSettings(board, patch) {
    const cur = (await this.getSettings(board)) ?? { createdAt: new Date().toISOString() }
    const value = { ...cur, ...patch, $type: C.settings }
    for (const k of Object.keys(value)) if (value[k] === undefined) delete value[k]
    const r = await this.space.putRecord({ space: board, repo: this.did, collection: C.settings, rkey: 'self', record: value })
    this.onWrite(board, this.did, `${C.settings}/self`, String(r.data.cid), value)
    return r.data
  }

  /** Invite a member: `write: false` makes a lurker (reads only; its repo isn't tracked). */
  async addMember(board, did, { write = true } = {}) {
    await this.simple.putMember({ space: board, did, read: true, write })
    const s = await this.getSettings(board)
    if (s?.removed?.includes(did)) await this.putSettings(board, { removed: s.removed.filter((d) => d !== did) })
  }

  /** Remove a member, and hide what they wrote (settings.removed). */
  async removeMember(board, did) {
    await this.simple.removeMember({ space: board, did })
    const s = await this.getSettings(board)
    await this.putSettings(board, { removed: [...new Set([...(s?.removed ?? []), did])] })
  }

  async listMembers(board) {
    const out = []
    let cursor
    do {
      const r = await this.simple.listMembers({ space: board, cursor, limit: 100 })
      out.push(...r.data.members)
      cursor = r.data.cursor
    } while (cursor)
    return out
  }

  async pin(board, postUri, on = true) {
    const s = await this.getSettings(board)
    const pinned = (s?.pinned ?? []).filter((u) => u !== postUri)
    if (on) pinned.push(postUri)
    await this.putSettings(board, { pinned })
  }

  async deleteBoard(board) {
    await this.simple.deleteSpace({ space: board })
  }

  // writing (any writer)

  async create(board, collection, value, rkey) {
    const r = await this.space.createRecord({ space: board, repo: this.did, collection, rkey, record: value })
    const path = `${collection}/${r.data.uri.split('/').pop()}`
    this.onWrite(board, this.did, path, String(r.data.cid), value)
    return { uri: r.data.uri, cid: String(r.data.cid) }
  }

  async put(board, collection, rkey, value) {
    const r = await this.space.putRecord({ space: board, repo: this.did, collection, rkey, record: value })
    this.onWrite(board, this.did, `${collection}/${rkey}`, String(r.data.cid), value)
    return { uri: r.data.uri ?? recordUri(board, this.did, collection, rkey), cid: String(r.data.cid) }
  }

  async del(board, collection, rkey) {
    await this.space.deleteRecord({ space: board, repo: this.did, collection, rkey })
    this.onWrite(board, this.did, `${collection}/${rkey}`, null, null)
  }

  async uploadImage(bytes, mimeType = 'image/png') {
    const r = await this.actor.client.com.atproto.repo.uploadBlob(bytes, { encoding: mimeType })
    return r.data.blob
  }

  async post(board, { title, body, image, flair, createdAt = new Date().toISOString(), rkey }) {
    const value = { $type: C.post, title, createdAt }
    if (body !== undefined) value.body = body
    if (flair) value.flair = flair
    if (image) value.image = { blob: image.blob ?? (await this.uploadImage(image.bytes, image.mimeType)), alt: image.alt }
    if (value.image && value.image.alt === undefined) delete value.image.alt
    return this.create(board, C.post, value, rkey)
  }

  async comment(board, postUri, body, { parent, createdAt = new Date().toISOString(), rkey } = {}) {
    const value = { $type: C.comment, subject: postUri, body, createdAt }
    if (parent) value.parent = parent
    return this.create(board, C.comment, value, rkey)
  }

  async vote(board, subject, direction = 'up', { createdAt = new Date().toISOString() } = {}) {
    return this.put(board, C.vote, voteRkey(subject), { $type: C.vote, subject, direction, createdAt })
  }

  async unvote(board, subject) {
    return this.del(board, C.vote, voteRkey(subject))
  }

  async editPost(postUri, patch) {
    const u = parseRecordUri(postUri)
    if (u.did !== this.did) throw new Error('only the author edits a post')
    const cur = await this.space.getRecord({ space: u.board, repo: this.did, collection: C.post, rkey: u.rkey })
    const value = { ...cur.data.value, ...patch, editedAt: new Date().toISOString() }
    return this.put(u.board, C.post, u.rkey, value)
  }

  async deleteRecord(uri) {
    const u = parseRecordUri(uri)
    if (u.did !== this.did) throw new Error('only the author deletes a record')
    return this.del(u.board, u.collection, u.rkey)
  }

  // reading

  /** A space credential for the board (delegation on the member's PDS, exchange at the owner's host), reused until near expiry. */
  async credential(board) {
    let c = this.creds.get(board)
    if (!c || c.expiresInMs() < 60_000) {
      c = await credentialFor(this.actor, board)
      this.creds.set(board, c)
    }
    return c
  }

  /** A record straight from its author's host, with this member's credential. */
  async getRecord(uri) {
    const u = parseRecordUri(uri)
    const cred = await this.credential(u.board)
    const cl = await cred.repoClient(u.did)
    return cl.com.atproto.space.getRecord({ space: u.board, repo: u.did, collection: u.collection, rkey: u.rkey })
  }

  appview(base, appviewDid) {
    return new AppviewClient(base, appviewDid, (board) => this.credential(board))
  }
}

/** The appview's XRPC-style API, authenticated with a space credential for the board. */
export class AppviewClient {
  constructor(base, appviewDid, credFor) {
    this.base = base
    this.did = appviewDid
    this.credFor = credFor
  }

  async call(nsid, board, { params = {}, body, cred } = {}) {
    const headers = {}
    const c = cred === undefined ? (board ? await this.credFor(board) : null) : cred
    if (c) Object.assign(headers, await c.headersFor(this.did))
    if (body) headers['content-type'] = 'application/json'
    const u = new URL(`/xrpc/${nsid}`, this.base)
    for (const [k, v] of Object.entries(params)) if (v !== undefined) u.searchParams.set(k, String(v))
    const res = await hfetch(u, { method: body ? 'POST' : 'GET', headers, body: body ? JSON.stringify(body) : undefined })
    const buf = Buffer.from(await res.arrayBuffer())
    const type = res.headers.get('content-type') ?? ''
    const json = type.includes('json') ? JSON.parse(buf.toString() || '{}') : undefined
    return { ok: res.ok, status: res.status, error: json?.error, message: json?.message, data: json, bytes: buf, type }
  }

  async must(nsid, board, opts) {
    const r = await this.call(nsid, board, opts)
    if (!r.ok) throw Object.assign(new Error(`${nsid}: ${r.status} ${r.error} ${r.message}`), { status: r.status, error: r.error })
    return r.data ?? r.bytes
  }

  indexBoard(board) {
    return this.must('dev.example.boards.indexBoard', board, { body: { board } })
  }
  getBoard(board) {
    return this.must('dev.example.boards.getBoard', board, { params: { board } })
  }
  getPosts(board, sort = 'hot', limit = 1000, cursor) {
    return this.must('dev.example.boards.getPosts', board, { params: { board, sort, limit, cursor } })
  }
  getPostThread(uri) {
    return this.must('dev.example.boards.getPostThread', parseRecordUri(uri).board, { params: { uri } })
  }
  getKarma(board, actor) {
    return this.must('dev.example.boards.getKarma', board, { params: { board, actor } })
  }
  getImage(board, did, cid) {
    return this.call('dev.example.boards.getImage', board, { params: { board, did, cid } })
  }
  getIndexState(board) {
    return this.must('dev.example.boards.getIndexState', board, { params: { board } })
  }
}

/** The authority tells a host that credentials were revoked (service auth from the owner's PDS). */
export async function revokeCredentials(owner, board, jtis, { aud, url }) {
  const sa = await owner.client.com.atproto.server.getServiceAuth({ aud, lxm: 'com.atproto.space.notifyCredentialRevoked' })
  return rawXrpc(url, 'com.atproto.space.notifyCredentialRevoked', {
    method: 'POST',
    body: { space: board, credentials: jtis },
    headers: { authorization: `Bearer ${sa.data.token}` },
  })
}

