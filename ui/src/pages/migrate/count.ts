// Records per collection in a repo CAR, from the MST keys alone: walking the
// tree (rather than counting record blocks) stays exact when identical
// records share one block, and only tree nodes need decoding.

import { decode, type TagDecoder } from 'cborg'

export type RecordCounts = { posts: number; likes: number; follows: number; reposts: number; total: number }

const COLLECTIONS: Record<string, keyof Omit<RecordCounts, 'total'>> = {
  'app.bsky.feed.post': 'posts',
  'app.bsky.feed.like': 'likes',
  'app.bsky.graph.follow': 'follows',
  'app.bsky.feed.repost': 'reposts',
}

function varint(b: Uint8Array, at: number): [number, number] {
  let n = 0
  for (let shift = 1; ; shift *= 128) {
    if (at >= b.length) throw new Error('truncated varint')
    const x = b[at++]
    n += (x & 0x7f) * shift
    if (x < 0x80) return [n, at]
  }
}

const hex = (b: Uint8Array) => {
  let s = ''
  for (let i = 0; i < b.length; i++) s += b[i].toString(16).padStart(2, '0')
  return s
}

// DAG-CBOR links are tag 42 over the CID bytes behind a 0x00 multibase prefix
const tags: Record<number, TagDecoder> = { 42: (inner) => hex((inner() as Uint8Array).subarray(1)) }

export function countRecords(car: Uint8Array): RecordCounts {
  const [hlen, h0] = varint(car, 0)
  const header = decode(car.subarray(h0, h0 + hlen), { tags }) as { roots?: string[] }
  const root = header.roots?.[0]
  if (!root) throw new Error('CAR has no root')

  const blocks = new Map<string, Uint8Array>()
  for (let at = h0 + hlen; at < car.length; ) {
    const [len, start] = varint(car, at)
    const end = start + len
    if (end > car.length) throw new Error('truncated block')
    // CIDv1: version, codec, then the multihash (code, digest length, digest)
    let p = start
    ;[, p] = varint(car, p)
    ;[, p] = varint(car, p)
    ;[, p] = varint(car, p)
    const [dlen, d0] = varint(car, p)
    const cidEnd = d0 + dlen
    blocks.set(hex(car.subarray(start, cidEnd)), car.subarray(cidEnd, end))
    at = end
  }
  const get = (cid: string) => {
    const b = blocks.get(cid)
    if (!b) throw new Error(`block ${cid} missing`)
    return decode(b, { tags })
  }

  const commit = get(root) as { data?: string }
  if (typeof commit.data !== 'string') throw new Error('root is not a commit')
  const out: RecordCounts = { posts: 0, likes: 0, follows: 0, reposts: 0, total: 0 }
  const dec = new TextDecoder()
  const stack = [commit.data]
  while (stack.length) {
    const node = get(stack.pop()!) as { l?: string | null; e?: { p: number; k: Uint8Array; t?: string | null }[] }
    if (node.l) stack.push(node.l)
    let prev = new Uint8Array(0)
    for (const e of node.e ?? []) {
      const key = new Uint8Array(e.p + e.k.length)
      key.set(prev.subarray(0, e.p))
      key.set(e.k, e.p)
      prev = key
      const name = COLLECTIONS[dec.decode(key).split('/')[0]]
      if (name) out[name]++
      out.total++
      if (e.t) stack.push(e.t)
    }
  }
  return out
}
