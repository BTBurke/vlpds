// Generates vectors.json from the reference implementation (@atproto/space).
// Run from packages/space of a built bluesky-social/atproto checkout at the
// commit in SOURCE: cp gen-vectors.mjs <atproto>/packages/space/ && cd there &&
// node gen-vectors.mjs > vectors.json
// Keys are fixed and both curves sign with RFC 6979, so the output is
// deterministic; Date.now is pinned for the tokens (jti stays random).
import { P256Keypair, Secp256k1Keypair, sha256 } from '@atproto/crypto'
import { parseCid } from '@atproto/lex-data'
import {
  LtHash,
  RepoCommit,
  createSpaceSigHeaders,
  createSpaceToken,
  encodeCommitCtx,
  formatSetHashElement,
  verifyCommit,
  verifySpaceSignature,
  verifySpaceToken,
} from './dist/index.js'
import { hkdfSha256, hmacSha256 } from '@atproto/crypto'

const hex = (b) => Buffer.from(b).toString('hex')
const seed = async (label) => sha256(new TextEncoder().encode(label))

const NOW_SEC = 1790000000
Date.now = () => NOW_SEC * 1000

const out = { now: NOW_SEC }

// LtHash
const lthash = []
const seq = (ops) => {
  const h = new LtHash()
  for (const [op, el] of ops) op === '+' ? h.add(el) : h.remove(el)
  return h
}
for (const ops of [
  [],
  [['+', 'one'], ['+', 'two']],
  [['+', 'two'], ['+', 'one']],
  [['+', 'a']],
  [['-', 'a']],
  [['+', 'a'], ['+', 'a']],
  [['+', 'a'], ['+', 'b'], ['-', 'a']],
  [['+', ''], ['+', 'é/ü/☃']],
  [['+', 'app.bsky.feed.post/3kbcq3p7ad400/bafyreidefdycgbfy3oglcb6ism3eqhyp5llsrpzxjsuac2gsy4mtrtx244']],
]) {
  const h = seq(ops)
  lthash.push({ ops, state: hex(h.state()), digest: hex(h.digest()), empty: h.isEmpty() })
}
out.lthash = lthash

// Commit: element, ctx, mac, sig (k256), verify
const CID_A = parseCid('bafyreidefdycgbfy3oglcb6ism3eqhyp5llsrpzxjsuac2gsy4mtrtx244')
const CID_B = parseCid('bafyreidpw4cbv6gr4ukh33z23pvvrpr3wi4gnpmi4doamlsl3sa4rgri2a')
const author = await Secp256k1Keypair.import(await seed('vlpds spaces alpha author k256'))
const authorP256 = await P256Keypair.import(await seed('vlpds spaces alpha author p256'))
const commits = []
for (const [kp, ctx, records] of [
  [
    author,
    { space: 'at://did:example:space/space/app.bsky.group/test', author: 'did:example:alice', rev: '3kbcq3p7ad400' },
    [['app.bsky.feed.post', '1', CID_A]],
  ],
  [
    author,
    { space: 'at://did:plc:asdf123/space/com.example.group/default', author: 'did:plc:user1', rev: '3lzzzzzzzzz22' },
    [['n.c.a', '1', CID_A], ['n.c.b', '2', CID_B]],
  ],
  [authorP256, { space: 'at://did:plc:x/space/com.example.group/self', author: 'did:plc:x', rev: '3kbcq3p7ad401' }, []],
]) {
  const repo = new RepoCommit()
  for (const [c, r, cid] of records) repo.add(c, r, cid)
  const ikm = await seed(`ikm ${ctx.rev}`)
  const ctxBytes = encodeCommitCtx(ctx, ikm)
  const hash = repo.setHash.digest()
  const commit = {
    ver: 1,
    hash,
    ikm,
    mac: hmacSha256(hkdfSha256(ikm, ctxBytes), hash),
    sig: await kp.sign(ctxBytes),
    rev: ctx.rev,
  }
  if (!(await verifyCommit(commit, ctx, kp.did()))) throw new Error('commit vector does not verify')
  commits.push({
    ctx,
    didKey: kp.did(),
    records: records.map(([c, r, cid]) => ({ collection: c, rkey: r, cid: cid.toString(), element: formatSetHashElement(c, r, cid) })),
    ikm: hex(ikm),
    ctxBytes: hex(ctxBytes),
    hash: hex(hash),
    mac: hex(commit.mac),
    sig: hex(commit.sig),
  })
}
out.commits = commits

// HTTP message signatures (P-256 did:key)
const sigKey = await P256Keypair.import(await seed('vlpds spaces alpha http sig p256'))
const httpsig = []
for (const [authorization, audience] of [
  ['Atproto-Space credential', 'did:example:repo'],
  ['Bearer delegation', undefined],
  ['Atproto-Space eyJhbGciOiJFUzI1NksifQ.e30.c2ln', 'did:plc:ewvi7nxzyoun6zhxrhs64oiz'],
]) {
  const headers = await createSpaceSigHeaders(sigKey, { authorization, audience })
  const keyId = audience === undefined ? undefined : sigKey.did()
  const got = await verifySpaceSignature(headers, keyId)
  if (got !== sigKey.did()) throw new Error('httpsig vector does not verify')
  httpsig.push({ keyDid: sigKey.did(), credentialKey: keyId ?? null, headers })
}
out.httpsig = httpsig

// Tokens: ES256K issuers (PDS account keys), and one ES256 credential
const user = await Secp256k1Keypair.import(await seed('vlpds spaces alpha user k256'))
const authority = await Secp256k1Keypair.import(await seed('vlpds spaces alpha authority k256'))
const authorityP256 = await P256Keypair.import(await seed('vlpds spaces alpha authority p256'))
const SPACE = 'at://did:example:space/space/app.bsky.group/test'
const tokens = []
const mk = async (type, opts, kp, verifyOpts) => {
  const jwt = await createSpaceToken(type, opts, kp)
  await verifySpaceToken(type, jwt, { getSigningKey: () => kp.did(), ...verifyOpts })
  tokens.push({ type, jwt, signingKey: kp.did(), opts })
}
await mk('delegation', { iss: 'did:example:alice', sub: SPACE, aud: 'did:example:space#atproto_space_host' }, user, {
  aud: 'did:example:space#atproto_space_host',
  sub: SPACE,
})
await mk('credential', { iss: 'did:example:space', sub: SPACE, keyId: sigKey.did() }, authority, { sub: SPACE })
await mk('credential', { iss: 'did:example:space', sub: SPACE, keyId: sigKey.did(), expiresInSec: 3600 }, authority, {})
await mk(
  'credential',
  { iss: 'did:example:space', sub: SPACE, keyId: sigKey.did(), kid: '#atproto_space' },
  authorityP256,
  {},
)
out.tokens = tokens

console.log(JSON.stringify(out, null, 1))
