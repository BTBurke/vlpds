// Generates export.car and export.json: a space repo as the reference PDS's
// com.atproto.space.getRepo serves it (packages/pds/src/api/com/atproto/space/
// getRepo.ts: buildSignedCommit = RepoCommit.fromState(..).sign, then
// serializeRepo), for vlpds's importRepo tests (tests/all/spaces_side/import_repo.rs).
// Run from packages/space of a built bluesky-social/atproto checkout at the
// commit in SOURCE: cp gen-export.mjs <atproto>/packages/space/ && cd there &&
// node gen-export.mjs <out dir>
// The key is fixed; the commit's ikm is random, so a re-run changes the commit
// block (and only it). The CAR is checked with verifyRepoCarFull before it is
// written.
import { writeFileSync } from 'node:fs'
import { Secp256k1Keypair, sha256 } from '@atproto/crypto'
import { cidForRawBytes } from '@atproto/lex-data'
import {
  RepoCommit,
  serializeRecord,
  serializeRepo,
  verifyRepoCarFull,
} from './dist/index.js'

const out = process.argv[2] ?? '.'
const hex = (b) => Buffer.from(b).toString('hex')
const seed = async (label) => sha256(new TextEncoder().encode(label))

// s32 TID: 53 bits of microseconds, 10 bits of clock id
const S32 = '234567abcdefghijklmnopqrstuvwxyz'
const tid = (micros, clock) => {
  let n = (BigInt(micros) << 10n) | BigInt(clock)
  let s = ''
  for (let i = 0; i < 13; i++) {
    s = S32[Number(n & 31n)] + s
    n >>= 5n
  }
  return s
}

const author = await Secp256k1Keypair.import(await seed('vlpds spaces phase 3 export author k256'), {
  exportable: true,
})
// A did:plc in syntax only: the tests serve its document from a stub directory.
const did = 'did:plc:vlpdsspacesrefexport2345'
const space = `at://${did}/space/com.example.group/refexport`
const rev = tid(1790000000000000, 7)

const blobBytes = new TextEncoder().encode('\x89PNG\r\n\x1a\n vlpds spaces reference export blob')
const blobCid = await cidForRawBytes(blobBytes)

const C1 = 'com.example.spaceRecord'
const C2 = 'com.example.spaceNote'
const recs = [
  [C1, '3lzzzzzzzzz22', { $type: C1, text: 'first', createdAt: '2026-09-21T12:00:00.000Z' }],
  [C1, 'a', { $type: C1, text: 'short rkey', n: 1 }],
  [C1, 'self', { $type: C1, text: 'with a blob', image: { $type: 'blob', ref: blobCid, mimeType: 'image/png', size: blobBytes.length } }],
  [C2, '3lzzzzzzzzz23', { $type: C2, text: 'a note', tags: ['x', 'y'], nested: { deep: true } }],
  [C2, 'longer-record-key-than-the-others', { $type: C2, text: 'canonical order is length first' }],
]
const serialized = []
for (const [c, r, v] of recs) serialized.push(await serializeRecord(c, r, v))
const repo = RepoCommit.fromRecords(serialized)
const commit = await repo.sign({ space, author: did, rev }, author)

const chunks = []
for await (const c of serializeRepo(commit, serialized)) chunks.push(c)
const car = Buffer.concat(chunks)

const verified = await verifyRepoCarFull([car], { space, author: did, didKey: author.did() })
if (verified.records.length !== recs.length) throw new Error('verifyRepoCarFull: record count')

writeFileSync(`${out}/export.car`, car)
writeFileSync(
  `${out}/export.json`,
  `${JSON.stringify(
    {
      generatedBy: 'gen-export.mjs: @atproto/space serializeRepo + RepoCommit.sign at the commit in SOURCE',
      did,
      space,
      rev,
      didKey: author.did(),
      privateKeyHex: hex(await author.export()),
      hash: hex(commit.hash),
      blob: { cid: blobCid.toString(), bytesHex: hex(blobBytes), mimeType: 'image/png' },
      records: serialized.map((s) => ({ collection: s.collection, rkey: s.rkey, cid: s.cid.toString() })),
    },
    null,
    2,
  )}\n`,
)
