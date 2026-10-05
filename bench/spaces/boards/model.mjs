// What a board looks like, computed from its members' space repos. Pure: the
// appview runs it over what it synced, and the scenario runner runs it over
// the writes it saw acked, so the two can be compared view for view.
//
// Rules (README.md):
// - settings: only the owner's dev.example.boards.settings/self counts
// - members in settings.removed are hidden: their posts, comments and votes
// - a comment needs a live post; a reply whose parent is gone hangs under a
//   `deleted` placeholder; comments of a missing post aren't shown
// - one vote per (voter, subject), the newest createdAt winning; score = ups - downs
// - karma = the scores of a member's live posts and comments
// - hot: pinned first, then Reddit's hot (log10 of the score plus age / 45000 s)

export const NS = 'dev.example.boards'
export const BOARD_TYPE = `${NS}.board`
export const C = {
  settings: `${NS}.settings`,
  post: `${NS}.post`,
  comment: `${NS}.comment`,
  vote: `${NS}.vote`,
}

/** A space record URI: the space URI, then the repo, collection and rkey. */
export const recordUri = (board, did, collection, rkey) => `${board}/${did}/${collection}/${rkey}`

export function parseRecordUri(uri) {
  const p = String(uri).split('/')
  if (p.length !== 9 || p[0] !== 'at:' || p[3] !== 'space') return null
  return { board: p.slice(0, 6).join('/'), authority: p[2], type: p[4], skey: p[5], did: p[6], collection: p[7], rkey: p[8] }
}

export const boardOf = (uri) => String(uri).split('/').slice(0, 6).join('/')
export const authorityOf = (board) => String(board).split('/')[2]

/** A blob's CID from lex JSON ({$link}), a BlobRef or decoded CBOR (a CID). */
export function blobCid(b) {
  const ref = b?.ref
  if (!ref) return undefined
  return ref.$link ?? ref.toString()
}

const HOT_EPOCH = 1134028003
export function hotScore(score, createdAt) {
  const order = Math.log10(Math.max(Math.abs(score), 1))
  const sign = score > 0 ? 1 : score < 0 ? -1 : 0
  const secs = Date.parse(createdAt) / 1000 - HOT_EPOCH
  return sign * order + secs / 45000
}

const str = (x, max = 100_000) => (typeof x === 'string' && x.length <= max ? x : undefined)

/**
 * `repos`: Map did -> Map `${collection}/${rkey}` -> { cid, value }, the
 * repos a board's space host tracks. Returns the materialized board.
 */
export function materialize(board, repos) {
  const owner = authorityOf(board)
  const own = repos.get(owner)?.get(`${C.settings}/self`)?.value
  const settings = {
    name: str(own?.name) ?? board.split('/')[5],
    description: str(own?.description),
    flairs: Array.isArray(own?.flairs) ? own.flairs.filter((f) => typeof f === 'string') : [],
    pinned: Array.isArray(own?.pinned) ? own.pinned.filter((u) => typeof u === 'string') : [],
    removed: new Set(Array.isArray(own?.removed) ? own.removed : []),
  }
  settings.removed.delete(owner)

  const posts = new Map()
  const comments = new Map()
  const votes = new Map() // `${voter} ${subject}` -> { direction, createdAt, rkey }
  const members = []
  for (const [did, recs] of repos) {
    if (settings.removed.has(did)) continue
    members.push(did)
    for (const [path, { cid, value }] of recs) {
      const [collection, rkey] = path.split('/')
      const uri = recordUri(board, did, collection, rkey)
      if (collection === C.post) {
        if (!str(value?.title) || !str(value?.createdAt)) continue
        const img = value.image?.blob ? { cid: blobCid(value.image.blob), mimeType: value.image.blob.mimeType, alt: str(value.image.alt) } : undefined
        posts.set(uri, {
          uri,
          cid: String(cid),
          author: did,
          title: value.title,
          body: str(value.body),
          flair: settings.flairs.includes(value.flair) ? value.flair : undefined,
          image: img?.cid ? img : undefined,
          createdAt: value.createdAt,
          editedAt: str(value.editedAt),
        })
      } else if (collection === C.comment) {
        if (!str(value?.subject) || typeof value?.body !== 'string' || !str(value?.createdAt)) continue
        comments.set(uri, { uri, cid: String(cid), author: did, subject: value.subject, parent: str(value.parent), body: value.body, createdAt: value.createdAt })
      } else if (collection === C.vote) {
        if (!str(value?.subject) || !['up', 'down'].includes(value?.direction)) continue
        const k = `${did} ${value.subject}`
        const prev = votes.get(k)
        const cur = { voter: did, subject: value.subject, direction: value.direction, createdAt: str(value.createdAt) ?? '', rkey }
        if (!prev || cur.createdAt > prev.createdAt || (cur.createdAt === prev.createdAt && cur.rkey > prev.rkey)) votes.set(k, cur)
      }
    }
  }
  for (const [uri, c] of comments) if (!posts.has(c.subject) || boardOf(c.subject) !== board) comments.delete(uri)

  const tally = new Map() // subject -> { ups, downs }
  for (const v of votes.values()) {
    if (!posts.has(v.subject) && !comments.has(v.subject)) continue
    const t = tally.get(v.subject) ?? { ups: 0, downs: 0 }
    if (v.direction === 'up') t.ups++
    else t.downs++
    tally.set(v.subject, t)
  }
  const scoreOf = (uri) => {
    const t = tally.get(uri) ?? { ups: 0, downs: 0 }
    return { ups: t.ups, downs: t.downs, score: t.ups - t.downs }
  }

  const pinned = settings.pinned.filter((u) => posts.has(u))
  const commentCount = new Map()
  for (const c of comments.values()) commentCount.set(c.subject, (commentCount.get(c.subject) ?? 0) + 1)
  const postViews = new Map()
  for (const p of posts.values()) {
    postViews.set(p.uri, { ...p, ...scoreOf(p.uri), comments: commentCount.get(p.uri) ?? 0, pinned: pinned.includes(p.uri) })
  }

  const karma = new Map()
  const addKarma = (did, kind, n) => {
    const k = karma.get(did) ?? { post: 0, comment: 0, total: 0 }
    k[kind] += n
    k.total += n
    karma.set(did, k)
  }
  for (const p of postViews.values()) addKarma(p.author, 'post', p.score)
  for (const c of comments.values()) addKarma(c.author, 'comment', scoreOf(c.uri).score)

  return {
    board,
    owner,
    settings: { ...settings, removed: [...settings.removed], pinned },
    members,
    posts: postViews,
    comments,
    scoreOf,
    karma,
  }
}

const byNew = (a, b) => (a.createdAt === b.createdAt ? (a.uri < b.uri ? 1 : -1) : a.createdAt < b.createdAt ? 1 : -1)

export function sortPosts(view, sort = 'hot') {
  const all = [...view.posts.values()]
  if (sort === 'new') return all.sort(byNew)
  if (sort === 'top') return all.sort((a, b) => b.score - a.score || byNew(a, b))
  const pins = view.settings.pinned.map((u) => view.posts.get(u))
  const rest = all.filter((p) => !p.pinned)
  rest.sort((a, b) => hotScore(b.score, b.createdAt) - hotScore(a.score, a.createdAt) || byNew(a, b))
  return [...pins, ...rest]
}

/** A post's comment tree, best first (score, then oldest). */
export function thread(view, postUri) {
  const post = view.posts.get(postUri)
  if (!post) return null
  const nodes = new Map()
  const node = (c) => ({ uri: c.uri, cid: c.cid, author: c.author, body: c.body, createdAt: c.createdAt, score: view.scoreOf(c.uri).score, replies: [] })
  const mine = [...view.comments.values()].filter((c) => c.subject === postUri)
  for (const c of mine) nodes.set(c.uri, node(c))
  const top = []
  for (const c of mine) {
    const n = nodes.get(c.uri)
    if (!c.parent || c.parent === postUri) {
      top.push(n)
      continue
    }
    let parent = nodes.get(c.parent)
    if (!parent) {
      parent = { uri: c.parent, deleted: true, score: 0, replies: [] }
      nodes.set(c.parent, parent)
      top.push(parent)
    }
    parent.replies.push(n)
  }
  const best = (a, b) => b.score - a.score || (a.createdAt ?? '').localeCompare(b.createdAt ?? '') || (a.uri < b.uri ? -1 : 1)
  const sortRec = (list) => {
    list.sort(best)
    for (const n of list) sortRec(n.replies)
    return list
  }
  return { post, replies: sortRec(top) }
}

export function boardView(view) {
  return {
    uri: view.board,
    owner: view.owner,
    name: view.settings.name,
    description: view.settings.description,
    flairs: view.settings.flairs,
    pinned: view.settings.pinned,
    members: view.members.length,
    posts: view.posts.size,
    comments: view.comments.size,
  }
}

export const karmaOf = (view, did) => view.karma.get(did) ?? { post: 0, comment: 0, total: 0 }

/** A post as the API shows it (no internal fields, undefined dropped). */
export function postView(p, imageUrl) {
  const out = { ...p }
  if (out.image && imageUrl) out.image = { ...out.image, url: imageUrl(p) }
  for (const k of Object.keys(out)) if (out[k] === undefined) delete out[k]
  return out
}
