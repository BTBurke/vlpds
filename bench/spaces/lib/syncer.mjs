// A space syncer built on the reference library (@atproto/space): catch up
// through the authority's listRepos from a spaceRev checkpoint, pull each repo
// from its own host with listRepoOps (inline values; the page that reaches the
// head carries the signed commit), keep a running LtHash per repo and compare
// it to the verified commit, falling back to a full getRepo (CAR verified with
// verifyRepoCarFull) on a mismatch or when it has nothing yet. Forwarded
// notifyWrites drive it between catch-ups; a prevSpaceRev it hasn't processed
// means a gap, which sends it back to listRepos.
import { LtHash, verifyCommit, verifyRepoCarFull } from '@atproto/space'
import { attempt } from './http.mjs'
import { signingKey } from './identity.mjs'
import { credentialFor } from './space.mjs'

const keys = new Map()
async function keyOf(did) {
  if (!keys.has(did)) keys.set(did, await signingKey(did))
  return keys.get(did)
}
export const forgetKeys = () => keys.clear()

const cidStr = (c) => (c == null ? null : typeof c === 'string' ? c : c.toString())

export class Syncer {
  /** `member`: the account whose delegation the syncer's credentials come from (a reader). */
  constructor(name, space, member, { snapshot } = {}) {
    this.name = name
    this.space = space
    this.member = member
    this.cred = null
    this.checkpoint = undefined // the last spaceRev whose repos are fully processed
    this.repos = new Map() // did -> { rev, hash: LtHash, records: Map(path -> { cid, value }) }
    this.violations = []
    this.stats = { listRepos: 0, opsPulls: 0, fullPulls: 0, mismatchFallbacks: 0, gaps: 0, notifies: 0, dupNotifies: 0, credentials: 0 }
    this.lastSpaceRev = undefined // highest spaceRev seen (listRepos or notify)
    this.chains = new Map() // did -> promise, to serialize per-repo pulls
    this.lastError = null
    this.queue = Promise.resolve()
    if (snapshot) this.restore(snapshot)
  }

  /** Notifies and catch-ups run one at a time, in arrival order. */
  enqueue(fn) {
    const p = this.queue.then(fn)
    this.queue = p.catch((e) => {
      this.lastError = e
    })
    return p
  }

  sync() {
    return this.enqueue(() => this.catchUp())
  }

  notify(body) {
    return this.enqueue(() => this.onNotify(body))
  }

  idle() {
    return this.queue
  }

  violation(what) {
    this.violations.push(`${new Date().toISOString()} ${this.name}: ${what}`)
  }

  async credential() {
    if (!this.cred || this.cred.expiresInMs() < 60_000) {
      this.cred = await credentialFor(this.member, this.space)
      this.stats.credentials++
    }
    return this.cred
  }

  /** Repeat a read with a fresh credential once when the host says it expired or was revoked. */
  async withCred(fn) {
    let cred = await this.credential()
    try {
      return await fn(cred)
    } catch (e) {
      if (e?.status === 401 || /Expired|Revoked/i.test(e?.error ?? '')) {
        this.cred = null
        cred = await this.credential()
        return fn(cred)
      }
      throw e
    }
  }

  /** listRepos from the checkpoint to an empty page; pulls repos whose repoRev moved. */
  async catchUp() {
    let cursor = this.checkpoint
    for (;;) {
      this.stats.listRepos++
      const page = await this.withCred(async (cred) =>
        (await cred.hostClient()).com.atproto.space.listRepos({ space: this.space, cursor, limit: 100 }),
      )
      const repos = page.data.repos
      if (!repos.length) {
        if (page.data.cursor !== undefined) this.violation(`empty listRepos page carries cursor ${page.data.cursor}`)
        break
      }
      let prev = cursor
      for (const r of repos) {
        if (prev !== undefined && !(r.spaceRev > prev)) this.violation(`listRepos spaceRev not ascending: ${r.spaceRev} after ${prev}`)
        prev = r.spaceRev
        const local = this.repos.get(r.did)
        if (!local || local.rev < r.repoRev || !hashEq(local.hash.digest(), r.hash)) await this.pull(r.did)
        this.seeSpaceRev(r.spaceRev)
      }
      if (page.data.cursor !== repos.at(-1).spaceRev) this.violation(`listRepos cursor ${page.data.cursor} != last spaceRev ${repos.at(-1).spaceRev}`)
      cursor = page.data.cursor
      this.checkpoint = cursor
    }
  }

  seeSpaceRev(rev) {
    if (this.lastSpaceRev === undefined || rev > this.lastSpaceRev) this.lastSpaceRev = rev
  }

  /** A forwarded notifyWrite (body: space, repo, repoRev, hash, spaceRev, prevSpaceRev?). */
  async onNotify(body) {
    if (body.space !== this.space) return
    this.stats.notifies++
    if (this.lastSpaceRev !== undefined && body.spaceRev <= this.lastSpaceRev) {
      this.stats.dupNotifies++ // duplicate or out of order: never move the checkpoint back
      const local = this.repos.get(body.repo)
      if (!local || local.rev < body.repoRev) await this.pull(body.repo)
      return
    }
    if (body.prevSpaceRev !== this.checkpoint) {
      this.stats.gaps++
      await this.catchUp()
      return
    }
    await this.pull(body.repo)
    this.seeSpaceRev(body.spaceRev)
    this.checkpoint = body.spaceRev
  }

  pull(did) {
    const prev = this.chains.get(did) ?? Promise.resolve()
    const next = prev.catch(() => {}).then(() => this.pullNow(did))
    this.chains.set(did, next)
    return next
  }

  async pullNow(did) {
    const local = this.repos.get(did)
    if (!local) return this.full(did)
    this.stats.opsPulls++
    const work = { rev: local.rev, hash: new LtHash(local.hash.state()), records: new Map(local.records) }
    let cursor
    let commit
    for (;;) {
      const page = await this.withCred(async (cred) =>
        (await cred.repoClient(did)).com.atproto.space.listRepoOps({ space: this.space, repo: did, since: local.rev, cursor, limit: 100 }),
      )
      for (const op of page.data.ops) {
        const path = `${op.collection}/${op.rkey}`
        const prevCid = cidStr(op.prev)
        const cid = cidStr(op.cid)
        if (prevCid) work.hash.remove(`${path}/${prevCid}`)
        if (cid) work.hash.add(`${path}/${cid}`)
        if (cid) work.records.set(path, { cid, value: op.value })
        else work.records.delete(path)
      }
      commit = page.data.commit
      if (commit || !page.data.cursor) break
      cursor = page.data.cursor
    }
    if (!commit) {
      this.violation(`listRepoOps for ${did} ended without a commit`)
      return this.full(did)
    }
    if (!(await verifyCommit(commit, { space: this.space, author: did, rev: commit.rev }, await keyOf(did)))) {
      this.violation(`listRepoOps commit for ${did} failed verification`)
      return this.full(did)
    }
    if (!hashEq(work.hash.digest(), commit.hash)) {
      this.stats.mismatchFallbacks++
      return this.full(did)
    }
    if (local.rev && commit.rev < local.rev) this.violation(`repo ${did} rev went back: ${commit.rev} < ${local.rev}`)
    work.rev = commit.rev
    this.repos.set(did, work)
  }

  async full(did) {
    this.stats.fullPulls++
    const res = await this.withCred(async (cred) => attempt(async () => (await cred.repoClient(did)).com.atproto.space.getRepo({ space: this.space, repo: did })))
    if (!res.ok) {
      if (res.error === 'RepoNotFound') {
        this.repos.set(did, { rev: '', hash: new LtHash(), records: new Map() })
        return
      }
      throw new Error(`getRepo ${did}: ${res.status} ${res.error} ${res.message}`)
    }
    const verified = await verifyRepoCarFull([res.data], { space: this.space, author: did, didKey: await keyOf(did) })
    const records = new Map()
    for (const r of verified.records) records.set(`${r.collection}/${r.rkey}`, { cid: r.cid.toString(), value: r.record })
    const prev = this.repos.get(did)
    if (prev?.rev && verified.commit.rev < prev.rev) this.violation(`repo ${did} rev went back on getRepo: ${verified.commit.rev} < ${prev.rev}`)
    this.repos.set(did, { rev: verified.commit.rev, hash: verified.repo.setHash, records })
  }

  /** Paths and CIDs of a repo, for comparing against ground truth. */
  view(did) {
    const r = this.repos.get(did)
    if (!r) return null
    return new Map([...r.records].map(([p, v]) => [p, v.cid]))
  }

  snapshot() {
    return {
      checkpoint: this.checkpoint,
      lastSpaceRev: this.lastSpaceRev,
      repos: [...this.repos].map(([did, r]) => [did, { rev: r.rev, state: Buffer.from(r.hash.state()).toString('base64'), records: [...r.records] }]),
    }
  }

  restore(s) {
    this.checkpoint = s.checkpoint
    this.lastSpaceRev = s.lastSpaceRev
    this.repos = new Map(s.repos.map(([did, r]) => [did, { rev: r.rev, hash: new LtHash(Buffer.from(r.state, 'base64')), records: new Map(r.records) }]))
  }
}

export function hashEq(a, b) {
  if (!a || !b || a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false
  return true
}
