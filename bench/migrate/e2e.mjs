// Migration e2e (README.md): seeds accounts on the local reference PDS,
// drives /migrate on a local vlpds headlessly through the whole wizard
// (plus failure and resume paths), then checks both sides and the PLC.
//
//   node e2e.mjs            seed + drive + verify
//   node e2e.mjs seed|drive|verify   one phase (state in out/state.json)
//
// Every URL is local; nothing here reaches the public network.

import { chromium } from 'playwright'
import { createECDH, createHash, randomBytes } from 'node:crypto'
import { deflateSync } from 'node:zlib'
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { execFileSync } from 'node:child_process'

const REF = process.env.REF_PDS ?? 'http://localhost:2783'
const VLPDS = process.env.VLPDS ?? 'http://127.0.0.1:2784'
const PLC = process.env.PLC ?? 'http://127.0.0.1:2782'
const MAIL = process.env.MAIL ?? 'http://127.0.0.1:2785'
const ADMIN = process.env.VLPDS_ADMIN ?? 'dev-admin-token'
const OUT = new URL('./out/', import.meta.url).pathname
const SHOTS = `${OUT}shots/`
const HEADED = !!process.env.HEADED

const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
let failures = 0
function check(ok, what, detail = '') {
  if (ok) log(`  ok   ${what}`)
  else {
    failures++
    log(`  FAIL ${what} ${detail}`)
  }
}

// ---------------------------------------------------------------- xrpc

async function xrpc(base, nsid, { params, body, auth, bytes, type, method } = {}) {
  const q = params ? `?${new URLSearchParams(Object.entries(params).filter(([, v]) => v !== undefined))}` : ''
  const headers = {}
  if (auth) headers.authorization = auth.startsWith('Basic') || auth.startsWith('Bearer') ? auth : `Bearer ${auth}`
  let payload
  if (bytes) {
    payload = bytes
    headers['content-type'] = type
  } else if (body !== undefined) {
    payload = JSON.stringify(body)
    headers['content-type'] = 'application/json'
  }
  // node resolves localhost to ::1 first; the containers listen on 127.0.0.1
  const r = await fetch(`${base.replace('//localhost:', '//127.0.0.1:')}/xrpc/${nsid}${q}`, { method: method ?? (payload !== undefined ? 'POST' : 'GET'), headers, body: payload })
  const buf = Buffer.from(await r.arrayBuffer())
  if (!r.ok) {
    let j = {}
    try {
      j = JSON.parse(buf.toString())
    } catch {}
    const e = new Error(`${nsid} ${r.status} ${j.error ?? ''} ${j.message ?? buf.toString().slice(0, 200)}`)
    e.status = r.status
    e.error = j.error
    throw e
  }
  const ct = r.headers.get('content-type') ?? ''
  return ct.includes('json') ? JSON.parse(buf.toString() || '{}') : buf
}

const basic = `Basic ${Buffer.from(`admin:${ADMIN}`).toString('base64')}`

// ---------------------------------------------------------------- mail (mailpit)

async function mailToken(to, since, subject) {
  for (let i = 0; i < 60; i++) {
    const r = await fetch(`${MAIL}/api/v1/search?query=${encodeURIComponent(`to:${to}`)}&limit=20`).then((r) => r.json())
    const m = (r.messages ?? []).find((m) => Date.parse(m.Created) >= since - 2000 && (!subject || subject.test(m.Subject)))
    if (m) {
      const full = await fetch(`${MAIL}/api/v1/message/${m.ID}`).then((r) => r.json())
      const tok = (full.Text ?? '').match(/\b([A-Za-z0-9]{5}-[A-Za-z0-9]{5})\b/)
      if (tok) return tok[1]
    }
    await sleep(500)
  }
  throw new Error(`no mail to ${to} since ${new Date(since).toISOString()}`)
}

// ---------------------------------------------------------------- images

const crcTable = Array.from({ length: 256 }, (_, n) => {
  let c = n
  for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1
  return c >>> 0
})
function crc32(buf) {
  let c = 0xffffffff
  for (const b of buf) c = crcTable[(c ^ b) & 0xff] ^ (c >>> 8)
  return (c ^ 0xffffffff) >>> 0
}
function chunk(type, data) {
  const len = Buffer.alloc(4)
  len.writeUInt32BE(data.length)
  const td = Buffer.concat([Buffer.from(type), data])
  const crc = Buffer.alloc(4)
  crc.writeUInt32BE(crc32(td))
  return Buffer.concat([len, td, crc])
}
/** A real, unique PNG: a w×h gradient seeded by `seed`, padded with an ancillary chunk to about `pad` bytes. */
function png(seed, w = 64, h = 64, pad = 0) {
  const raw = Buffer.alloc((w * 3 + 1) * h)
  const s = createHash('sha256').update(String(seed)).digest()
  for (let y = 0; y < h; y++) {
    raw[y * (w * 3 + 1)] = 0
    for (let x = 0; x < w; x++) {
      const o = y * (w * 3 + 1) + 1 + x * 3
      raw[o] = (s[0] + x * 3) & 0xff
      raw[o + 1] = (s[1] + y * 3) & 0xff
      raw[o + 2] = (s[2] + x + y) & 0xff
    }
  }
  const ihdr = Buffer.alloc(13)
  ihdr.writeUInt32BE(w, 0)
  ihdr.writeUInt32BE(h, 4)
  ihdr[8] = 8
  ihdr[9] = 2
  const parts = [Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]), chunk('IHDR', ihdr)]
  if (pad) parts.push(chunk('zzZz', randomBytes(pad)))
  parts.push(chunk('IDAT', deflateSync(raw)), chunk('IEND', Buffer.alloc(0)))
  return Buffer.concat(parts)
}
const sha = (b) => createHash('sha256').update(b).digest('hex')

// ---------------------------------------------------------------- did:key (secp256k1)

const B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
function b58(bytes) {
  let n = BigInt(`0x${Buffer.from(bytes).toString('hex') || '0'}`)
  let out = ''
  while (n > 0n) {
    out = B58[Number(n % 58n)] + out
    n /= 58n
  }
  for (const b of bytes) {
    if (b !== 0) break
    out = `1${out}`
  }
  return out
}
/** The did:key of a secp256k1 private key given as hex. */
function didKeyOfPrivate(hex) {
  const e = createECDH('secp256k1')
  e.setPrivateKey(Buffer.from(hex, 'hex'))
  return `did:key:z${b58(Buffer.concat([Buffer.from([0xe7, 0x01]), e.getPublicKey(null, 'compressed')]))}`
}
const newDidKey = () => didKeyOfPrivate(randomBytes(32).toString('hex'))

// ---------------------------------------------------------------- seed

const ACCOUNTS = [
  // alice: found by handle + server address, an app password tried first, a
  // one-click invite link, the same password
  { name: 'alice', posts: 40, images: 6 },
  // bob: email 2FA on the old server, invite typed, createAccount answer lost
  // (re-run), a new password, a wrong PLC code first
  { name: 'bob', posts: 30, images: 4, twofa: true, newPassword: 'bob-new-password' },
  // carol: bigger; a reload mid-blob-copy, then a new tab (resume with fresh sign-ins)
  { name: 'carol', posts: 400, images: 40, big: true },
  // dave: keeps his handle (the old server is made to look like it doesn't
  // own .test, so the page offers to keep it as a custom domain)
  { name: 'dave', posts: 10, images: 2 },
]

const PREFS = (name) => [
  { $type: 'app.bsky.actor.defs#adultContentPref', enabled: name !== 'bob' },
  { $type: 'app.bsky.actor.defs#contentLabelPref', label: 'gore', visibility: 'hide' },
  {
    $type: 'app.bsky.actor.defs#savedFeedsPrefV2',
    items: [{ type: 'timeline', value: 'following', pinned: true, id: '3kxyzkqgaq22a' }],
  },
  { $type: 'app.bsky.actor.defs#mutedWordsPref', items: [{ value: `spoilers-${name}`, targets: ['content', 'tag'], actorTarget: 'all' }] },
  { $type: 'app.bsky.actor.defs#threadViewPref', sort: 'oldest' },
]

const now = () => new Date().toISOString()

async function seed() {
  log('seed: accounts on the reference PDS', REF)
  const state = { accounts: {} }
  const made = {}
  for (const a of ACCOUNTS) {
    const handle = `${a.name}${Date.now().toString(36).slice(-4)}.test`
    const email = `${a.name}-${Date.now().toString(36)}@ref.test`
    const password = `${a.name}-old-password`
    const s = await xrpc(REF, 'com.atproto.server.createAccount', { body: { handle, email, password } })
    made[a.name] = { ...a, did: s.did, handle, email, password, jwt: s.accessJwt, blobs: {} }
    log(`  ${handle} ${s.did}`)
  }
  for (const a of Object.values(made)) {
    const up = async (bytes) => {
      const r = await xrpc(REF, 'com.atproto.repo.uploadBlob', { bytes, type: 'image/png', auth: a.jwt })
      a.blobs[r.blob.ref.$link] = sha(bytes)
      return r.blob
    }
    const avatar = await up(png(`${a.did}-avatar`, 96, 96))
    const banner = await up(png(`${a.did}-banner`, 300, 100))
    await xrpc(REF, 'com.atproto.repo.putRecord', {
      auth: a.jwt,
      body: {
        repo: a.did,
        collection: 'app.bsky.actor.profile',
        rkey: 'self',
        record: { $type: 'app.bsky.actor.profile', displayName: `${a.name} (moving)`, description: 'Seeded for the vlpds migration e2e', avatar, banner },
      },
    })
    const images = []
    for (let i = 0; i < a.images; i++) images.push(await up(png(`${a.did}-${i}`, 128, 96, a.big && i % 10 === 0 ? 1_500_000 : 20_000)))
    let writes = []
    for (let i = 0; i < a.posts; i++) {
      const record = { $type: 'app.bsky.feed.post', text: `post ${i} from ${a.name}`, createdAt: now(), langs: ['en'] }
      if (i < images.length) record.embed = { $type: 'app.bsky.embed.images', images: [{ alt: `picture ${i}`, image: images[i] }] }
      writes.push({ $type: 'com.atproto.repo.applyWrites#create', collection: 'app.bsky.feed.post', value: record })
      if (writes.length === 100 || i === a.posts - 1) {
        await xrpc(REF, 'com.atproto.repo.applyWrites', { auth: a.jwt, body: { repo: a.did, writes } })
        writes = []
      }
    }
    await xrpc(REF, 'app.bsky.actor.putPreferences', { auth: a.jwt, body: { preferences: PREFS(a.name) } })
  }
  // follows and likes across the accounts
  const all = Object.values(made)
  for (const a of all) {
    for (const b of all) {
      if (a === b) continue
      await xrpc(REF, 'com.atproto.repo.createRecord', {
        auth: a.jwt,
        body: { repo: a.did, collection: 'app.bsky.graph.follow', record: { $type: 'app.bsky.graph.follow', subject: b.did, createdAt: now() } },
      })
      const posts = await xrpc(REF, 'com.atproto.repo.listRecords', { params: { repo: b.did, collection: 'app.bsky.feed.post', limit: 5 } })
      for (const p of posts.records) {
        await xrpc(REF, 'com.atproto.repo.createRecord', {
          auth: a.jwt,
          body: { repo: a.did, collection: 'app.bsky.feed.like', record: { $type: 'app.bsky.feed.like', subject: { uri: p.uri, cid: p.cid }, createdAt: now() } },
        })
      }
    }
  }
  // bob: confirm his email, then turn on email 2FA
  const bob = made.bob
  let t = Date.now()
  await xrpc(REF, 'com.atproto.server.requestEmailConfirmation', { auth: bob.jwt, method: 'POST' })
  await xrpc(REF, 'com.atproto.server.confirmEmail', { auth: bob.jwt, body: { email: bob.email, token: await mailToken(bob.email, t) } })
  t = Date.now()
  const upd = await xrpc(REF, 'com.atproto.server.requestEmailUpdate', { auth: bob.jwt, method: 'POST' })
  const token = upd.tokenRequired ? await mailToken(bob.email, t) : undefined
  await xrpc(REF, 'com.atproto.server.updateEmail', { auth: bob.jwt, body: { email: bob.email, emailAuthFactor: true, token } })
  try {
    await xrpc(REF, 'com.atproto.server.createSession', { body: { identifier: bob.handle, password: bob.password } })
    throw new Error('bob should need a 2FA code')
  } catch (e) {
    if (e.error !== 'AuthFactorTokenRequired') throw e
  }

  // the source of truth for verify: every record's CID, every blob's hash, the prefs
  for (const a of all) {
    const desc = await xrpc(REF, 'com.atproto.repo.describeRepo', { params: { repo: a.did } })
    const records = {}
    for (const c of desc.collections) {
      let cursor
      do {
        const r = await xrpc(REF, 'com.atproto.repo.listRecords', { params: { repo: a.did, collection: c, limit: 100, cursor } })
        for (const x of r.records) records[x.uri] = x.cid
        cursor = r.records.length ? r.cursor : undefined
      } while (cursor)
    }
    const st = await xrpc(REF, 'com.atproto.server.checkAccountStatus', { auth: a.jwt })
    const prefs = await xrpc(REF, 'app.bsky.actor.getPreferences', { auth: a.jwt })
    const { jwt, ...rest } = a
    state.accounts[a.name] = { ...rest, records, prefs: prefs.preferences, oldStatus: st }
    log(`  ${a.handle}: ${Object.keys(records).length} records, ${Object.keys(a.blobs).length} blobs, ${st.repoBlocks} blocks`)
  }
  // invites on vlpds (it runs with --invite-required)
  const inv = await xrpc(VLPDS, 'com.atproto.server.createInviteCodes', { auth: basic, body: { codeCount: ACCOUNTS.length, useCount: 1 } })
  state.invites = inv.codes[0].codes
  save(state)
  return state
}

function save(s) {
  mkdirSync(OUT, { recursive: true })
  writeFileSync(`${OUT}state.json`, JSON.stringify(s, null, 2))
}
const load = () => JSON.parse(readFileSync(`${OUT}state.json`, 'utf8'))

// ---------------------------------------------------------------- drive

let shotN = 0
/** Set while driving an account in simple mode: every /migrate screen is checked for protocol jargon. */
let simpleMode = false
const JARGON = /\b(PLC|DIDs?|did:\w*|repo|repository|CAR|blobs?|rotation keys?|service auth|PDS)\b/
async function shot(page, name) {
  mkdirSync(SHOTS, { recursive: true })
  if (simpleMode && page.url().includes('/migrate')) {
    const text = await page.locator('.mig-main').innerText()
    const m = text.match(JARGON)
    check(!m, `simple mode, ${name}: no protocol jargon on screen`, m ? `found "${m[0]}" in: ${text.slice(Math.max(0, m.index - 60), m.index + 60)}` : '')
  }
  await page.screenshot({ path: `${SHOTS}${String(++shotN).padStart(2, '0')}-${name}.png`, fullPage: true })
}

/** localStorage and sessionStorage of the page's origin, as one string. */
const storageOf = (page) => page.evaluate(() => JSON.stringify({ ...localStorage }) + JSON.stringify({ ...sessionStorage }))

/** The newest email token vlpds (dev mode) delivered to `email`, once it differs from `before`. */
async function devMailToken(email, before) {
  for (let i = 0; i < 60; i++) {
    const r = await xrpc(VLPDS, 'vlpds.admin.getDevMail', { auth: basic, params: { email } })
    if (r.token && r.token !== before) return r.token
    await sleep(250)
  }
  throw new Error(`no new dev mail token for ${email}`)
}
const lastDevMailToken = (email) => xrpc(VLPDS, 'vlpds.admin.getDevMail', { auth: basic, params: { email } }).then((r) => r.token)

const plcData = (did) => fetch(`${PLC}/${did}/data`).then((r) => r.json())

const heading = (page) => page.locator('.mig-card h1')

/** What simple mode should say about `a`, from the seeded records and blobs. */
function expectCounts(a) {
  const by = {}
  for (const uri of Object.keys(a.records)) {
    const c = uri.split('/')[3]
    by[c] = (by[c] ?? 0) + 1
  }
  const n = (k, one, many) => `${k.toLocaleString('en-US')} ${k === 1 ? one : many}`
  const posts = n(by['app.bsky.feed.post'] ?? 0, 'post', 'posts')
  const likes = n(by['app.bsky.feed.like'] ?? 0, 'like', 'likes')
  const follows = n(by['app.bsky.graph.follow'] ?? 0, 'follow', 'follows')
  const media = n(Object.keys(a.blobs).length, 'photo or video', 'photos & videos')
  return { by, posts, likes, follows, media, nBlobs: Object.keys(a.blobs).length }
}

const flat = (s) => s.replace(/\s+/g, ' ').trim()

/** Polls the card until its text matches `re` (the page's CSP rules out waitForFunction). */
async function waitCardText(page, re, timeout = 60_000) {
  for (const end = Date.now() + timeout; ; ) {
    const t = flat(await page.locator('.mig-card').innerText().catch(() => ''))
    if (re.test(t)) return t
    if (Date.now() > end) throw new Error(`card never matched ${re}: ${t.slice(0, 300)}`)
    await sleep(100)
  }
}

const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

async function waitHeading(page, re, timeout = 60_000) {
  await page.locator('.mig-card h1', { hasText: re }).waitFor({ timeout })
}

async function driveOne(ctx, a, state, opts) {
  log(`drive: ${a.name} (${a.handle}), ${opts.advanced ? 'advanced' : 'simple'} mode`)
  simpleMode = !opts.advanced
  let page = await ctx.newPage()
  page.on('pageerror', (e) => log(`  [pageerror] ${e.message}`))
  page.on('console', (m) => m.type() === 'error' && log(`  [console] ${m.text()}`))
  const invite = opts.inviteInUrl ? state.invites.shift() : undefined
  if (opts.keepHandle) {
    await page.route(`${REF}/xrpc/com.atproto.server.describeServer`, (r) =>
      r.fulfill({ json: { did: 'did:web:localhost', availableUserDomains: ['.elsewhere.invalid'], inviteCodeRequired: false } }),
    )
  }
  if (opts.fakeProfile) {
    // the harness has no AppView; answer getProfile (proxied by the old server) with the seeded numbers
    const w = expectCounts(a)
    await page.route(`${REF}/xrpc/app.bsky.actor.getProfile*`, (r) =>
      r.fulfill({
        json: { did: a.did, handle: a.handle, postsCount: w.by['app.bsky.feed.post'], followsCount: w.by['app.bsky.graph.follow'], followersCount: 3 },
      }),
    )
  }
  if (opts.fromLanding) {
    await page.goto(`${VLPDS}/`)
    await shot(page, 'landing')
    await page.click('a.move-here')
  } else {
    await page.goto(`${VLPDS}/migrate${invite ? `?invite=${invite}` : ''}`)
  }
  await waitHeading(page, /Move your account here/)
  if (opts.advanced) {
    await page.check('input[name=advanced]')
    await page.getByText('Your handle or DID').waitFor()
  }
  await shot(page, `${a.name}-find`)

  // 1. find: a .test handle can't be looked up by a dev-mode vlpds, so the
  // page asks for the current server (alice); bob and carol give their DID
  if (opts.byHandle) {
    await page.fill('input[name=identifier]', a.handle)
    await page.click('button:has-text("Find my account")')
    await page.locator('input[name=host]').waitFor()
    await shot(page, `${a.name}-find-host`)
    await page.fill('input[name=host]', REF)
  } else {
    await page.fill('input[name=identifier]', a.did)
  }
  await page.click('button:has-text("Find my account")')

  // 2. sign in at the old PDS
  await waitHeading(page, /Sign in to localhost/)
  if (opts.appPasswordFirst) {
    const ap = await xrpc(REF, 'com.atproto.server.createSession', { body: { identifier: a.handle, password: a.password } }).then((s) =>
      xrpc(REF, 'com.atproto.server.createAppPassword', { auth: s.accessJwt, body: { name: 'e2e' } }),
    )
    await page.fill('input[name=password]', ap.password)
    await page.click('button:has-text("Sign in")')
    await page.getByText('That is an app password').waitFor()
    check(true, 'an app password is refused with an explanation')
    await shot(page, `${a.name}-app-password`)
  }
  const t2fa = Date.now()
  await page.fill('input[name=password]', a.password)
  await page.click('button:has-text("Sign in")')
  if (a.twofa) {
    await page.locator('input[name=code]').waitFor()
    await shot(page, `${a.name}-2fa`)
    await page.fill('input[name=code]', await mailToken(a.email, t2fa))
    await page.click('button:has-text("Sign in")')
    check(true, 'email 2FA code asked for and accepted')
  }

  // 3. checks
  await waitHeading(page, opts.advanced ? /Pre-flight checks/ : /Checking your account/)
  for (let i = 0; (await page.locator('.mig-check .spinner').count()) > 0; i++) {
    if (i > 150) throw new Error('preflight checks never finished')
    await sleep(200)
  }
  await shot(page, `${a.name}-checks`)
  const checksText = await page.locator('.mig-checks').innerText()
  const want = expectCounts(a)
  if (opts.advanced) check(checksText.includes(`${a.oldStatus.indexedRecords} records`), 'preflight shows the record count', checksText)
  else {
    check(!checksText.includes('records'), 'simple preflight shows no record count', checksText)
    const line = flat(await page.locator('.mig-counts').innerText().catch(() => ''))
    const exp = opts.fakeProfile
      ? `About ${want.posts} and ${want.follows}, plus ${want.media}.`
      : `${want.media}, plus your posts, likes and follows.`
    check(line === exp, `simple preflight counts: "${exp}"${opts.fakeProfile ? ' (AppView profile counts)' : ' (no AppView: photos only)'}`, `got "${line}"`)
  }
  await page.click('button:has-text("Continue")')

  // 4. handle
  await waitHeading(page, /Choose your handle/)
  if (opts.keepHandle) {
    await page.getByText(`Keep @${a.handle}`).waitFor()
    // a dev-mode vlpds resolves no outside handles, so it can't confirm it
    await page.getByText(/couldn't confirm it resolves/).waitFor()
    await shot(page, `${a.name}-handle-keep`)
    await page.click('button:has-text("Continue")')
    a.newHandle = a.handle
    check(true, 'a custom-domain handle can be kept')
  } else {
    const name = `${a.name}${Math.floor(Math.random() * 1e4)}`
    await page.fill('input[name=handle]', name)
    await page.getByText(/is available/).waitFor()
    await shot(page, `${a.name}-handle`)
    await page.click('button:has-text("Continue")')
    a.newHandle = `${name}.vlpds.test`
  }

  // 5. create
  await waitHeading(page, /Create @/)
  if (a.newPassword) {
    await page.uncheck('input[name=same-password]')
    await page.fill('input[name=new-password]', a.newPassword)
  }
  if (!invite) await page.fill('input[name=invite]', state.invites.shift())
  await shot(page, `${a.name}-create`)
  if (opts.loseCreateAnswer) {
    // the server creates the account but the answer never arrives
    let first = true
    await page.route('**/xrpc/com.atproto.server.createAccount', async (route) => {
      if (!first) return route.continue()
      first = false
      await route.fetch()
      await route.abort('connectionreset')
    })
    await page.click('button:has-text("Create my account here")')
    await page.locator('.notice.err').waitFor()
    await shot(page, `${a.name}-create-lost`)
    check(true, 'a lost createAccount answer shows an error and stays on the step')
    await page.click('button:has-text("Create my account here")')
  } else {
    await page.click('button:has-text("Create my account here")')
  }

  // 6. copy
  const repoLine = `All ${want.posts}, ${want.likes} and ${want.follows} copied.`
  // blobs, then settings, wait until each screen has been read, so the step can't move on first
  const gate = () => {
    let open
    const p = new Promise((r) => (open = r))
    return { p, open }
  }
  const blobGate = gate()
  const prefsGate = gate()
  if (opts.holdBlobs) {
    await page.route(`${REF}/xrpc/com.atproto.sync.getBlob*`, async (r) => (await blobGate.p, r.continue()))
    await page.route(`${REF}/xrpc/app.bsky.actor.getPreferences*`, async (r) => (await prefsGate.p, r.continue()))
  }
  await waitHeading(page, /Copying your data/)
  if (opts.holdBlobs) {
    await waitCardText(page, new RegExp(`${esc(repoLine)}.*0 of ${want.nBlobs} photos & videos copied`))
    check(true, `simple copy step: "${repoLine}" and "0 of ${want.media} copied"`)
    await shot(page, `${a.name}-copy-counts`)
    blobGate.open()
    await waitCardText(page, new RegExp(esc(`All ${want.media} copied.`)))
    check(true, `simple copy step: "All ${want.media} copied."`)
    await shot(page, `${a.name}-copy-done`)
    prefsGate.open()
  }
  if (opts.reloadMidBlobs) {
    // slow the old server's blobs so the reload lands mid-copy
    await page.route(`${REF}/xrpc/com.atproto.sync.getBlob*`, async (r) => {
      await sleep(250)
      await r.continue()
    })
    // (polled from here: the page's CSP forbids the eval waitForFunction needs)
    for (let i = 0; !/\b([5-9]|\d\d+) of \d+ (photos & videos )?copied/.test(await page.locator('.mig-card').innerText()); i++) {
      if (i > 600) throw new Error('blob copy never got going')
      await sleep(200)
    }
    await shot(page, `${a.name}-copy-midway`)
    const before = flat(await page.locator('.mig-card').innerText())
    if (!opts.advanced) {
      check(before.includes(repoLine), `simple copy step: "${repoLine}"`, before)
      check(new RegExp(`\\d+ of ${want.nBlobs} photos & videos copied`).test(before), `simple copy step: "N of ${want.media}" while copying`, before)
    }
    let imports = 0
    page.on('request', (r) => r.url().includes('com.atproto.repo.importRepo') && imports++)
    await page.reload()
    await waitHeading(page, /Copying your data|Save a copy of your account|Back up before the switch/)
    check(true, `reloaded mid-copy (${before.match(/\d+ of \d+ (photos & videos )?copied/)?.[0]}) and landed back on the copy step`)
    if (!opts.advanced) {
      const after = await waitCardText(page, new RegExp(`${esc(repoLine)}|Save a copy of your account`))
      check(after.includes(repoLine), 'after the reload, the repo counts are still shown (kept with the progress)', after)
    }
    await waitHeading(page, BACKUP_HEADING, 300_000)
    check(imports === 0, 'the resumed copy did not import the repository again', `imports=${imports}`)
  } else {
    await waitHeading(page, BACKUP_HEADING, 300_000)
  }

  // 7. the optional backup, from the old server
  await shot(page, `${a.name}-backup`)
  if (opts.backupAtStep) {
    const zip = await downloadBackup(page, opts.backupAtStep, () => page.click('button[name=backup]'))
    await page.locator('.mig-card .notice', { hasText: 'Saved' }).waitFor()
    await shot(page, `${a.name}-backup-saved`)
    await verifyBackup(zip, a, { base: REF, label: 'step', password: [a.password, a.newPassword] })
    await page.click('.mig-card button:has-text("Continue")')
  } else {
    await page.click('button[name=skip-backup]')
  }
  await waitHeading(page, /Move your identity|Confirm the move/)
  if (!opts.backupAtStep) {
    await page.reload()
    await waitHeading(page, /Move your identity|Confirm the move|Continue moving/)
    if (/Continue moving/.test(await heading(page).innerText())) await page.click('button:has-text("Continue")')
    await waitHeading(page, /Move your identity|Confirm the move/)
    check(true, 'a skipped backup is not offered again')
  }

  // 8. identity
  if (opts.advanced) await page.locator('.mig-diff').waitFor()
  else {
    await page.getByText('Your account will be hosted at').first().waitFor()
    check((await page.locator('.mig-diff').count()) === 0, 'simple mode hides the key table')
  }
  await shot(page, `${a.name}-identity-review`)
  let tPlc = Date.now()
  await page.click('button:has-text("Email me a confirmation code")')
  await page.locator('input[name=plc-token]').waitFor()
  if (opts.newTabBeforePlc) {
    // a new tab: progress is in localStorage, sessions are not
    await page.close()
    page = await ctx.newPage()
    await page.goto(`${VLPDS}/migrate`)
    await waitHeading(page, /Continue moving/)
    await shot(page, `${a.name}-resume`)
    await page.click('button:has-text("Continue")')
    await waitHeading(page, /Sign in to localhost/)
    await page.fill('input[name=password]', a.password)
    await page.click('button:has-text("Sign in")')
    await waitHeading(page, /Sign in to your new account/)
    await page.fill('input[name=password]', a.newPassword ?? a.password)
    await page.click('button:has-text("Sign in")')
    await waitHeading(page, /Move your identity|Confirm the move/)
    await page.locator('input[name=plc-token]').waitFor()
    check(true, 'a new tab resumed at the identity step after signing in to both servers')
  }
  const token = await mailToken(a.email, tPlc, /PLC|Update/i)
  const moveBtn = page.getByRole('button', { name: /^Move my (identity|account)$/ })
  if (!opts.advanced) check((await page.locator('summary', { hasText: 'recovery key' }).count()) === 0, 'simple mode offers no recovery key')
  if (opts.ownKey) await addOwnKey(page, a, opts.ownKey, moveBtn, token)
  if (opts.wrongPlcToken) {
    await page.fill('input[name=plc-token]', 'AAAAA-BBBBB')
    await page.check('input[name=understood]')
    await moveBtn.click()
    await page.getByText(/That code isn't right|code has expired/).waitFor()
    await shot(page, `${a.name}-wrong-code`)
    check(true, 'a wrong PLC code is refused and the step stays put')
    await page.fill('input[name=plc-token]', token)
  } else {
    await page.fill('input[name=plc-token]', token)
    await page.check('input[name=understood]')
  }
  await shot(page, `${a.name}-identity-code`)
  await moveBtn.click()

  // 9. finish
  await waitHeading(page, /Welcome to your new home/, 120_000)
  await page.locator('.tiles').waitFor()
  if (!opts.advanced) {
    const tiles = flat(await page.locator('.tiles.mig-counts').innerText().catch(() => ''))
    const exp = `${want.posts} ${want.likes} ${want.follows} ${want.media}`
    check(tiles.startsWith(exp), `welcome screen: ${exp}`, `got "${tiles}"`)
  } else check((await page.locator('.tiles.mig-counts').count()) === 0, 'advanced welcome screen keeps the records tile')
  await shot(page, `${a.name}-done`)
  if (opts.backupAtWelcome) {
    // the secondary entry point, from vlpds, with the key made in this tab
    await page.click('summary:has-text("Download a backup from here")')
    const box = page.locator('.mig-backup')
    if (a.ownPrivate) {
      await box.locator('input[name=backup-recovery-key]').check()
      await box.getByText('Anyone with this file can take over your identity').waitFor()
    }
    await shot(page, `${a.name}-done-backup`)
    const zip = await downloadBackup(page, opts.backupAtWelcome, () => box.locator('button[name=backup]').click())
    await box.locator('.notice', { hasText: 'Saved' }).waitFor()
    await shot(page, `${a.name}-done-backup-saved`)
    await verifyBackup(zip, a, { base: VLPDS, label: 'welcome', password: [a.password, a.newPassword], key: a.ownPrivate })
  }
  await page.click('button:has-text("Open your account")')
  await page.waitForURL(/\/account/)
  await page.getByText(a.newHandle).first().waitFor({ timeout: 20_000 })
  await shot(page, `${a.name}-account`)
  check(true, 'the account page opens signed in to the moved account')
  if (a.ownPrivate) {
    const stored = await storageOf(page)
    check(!stored.includes(a.ownPrivate), 'the generated private key is in neither localStorage nor sessionStorage')
    delete a.ownPrivate
  }
  if (opts.accountKey) await accountRecoveryKey(page, a)
  if (opts.backupOnAccount) {
    await page.click('aside.sidenav a:has-text("Export")')
    await page.getByText('Download my data').first().waitFor()
    await shot(page, `${a.name}-account-export`)
    const zip = await downloadBackup(page, opts.backupOnAccount, () => page.click('button[name=backup]'))
    await page.locator('.notice', { hasText: 'Saved' }).first().waitFor()
    await shot(page, `${a.name}-account-export-saved`)
    await verifyBackup(zip, a, { base: VLPDS, label: 'account', password: [a.password, a.newPassword], extras: true })
  }
  await page.close()
}

/** /migrate, advanced: add the user's own recovery key ahead of vlpds's keys. */
async function addOwnKey(page, a, how, moveBtn, token) {
  await page.click('summary:has-text("Advanced: add your own recovery key")')
  if (how === 'generate') {
    await page.click('button:has-text("Generate a recovery key")')
    const priv = (await page.locator('.rk-private .mono').innerText()).trim()
    const pub = (await page.locator('.rk-public .mono').innerText()).trim()
    check(/^[0-9a-f]{64}$/.test(priv) && didKeyOfPrivate(priv) === pub, 'the shown private key (hex) derives the shown did:key', `${priv} ${pub}`)
    await page.fill('input[name=plc-token]', token)
    await page.check('input[name=understood]')
    check(await moveBtn.isDisabled(), 'moving waits until "I saved it" is ticked')
    check(!(await storageOf(page)).includes(priv), 'the private key is not stored in the browser')
    await shot(page, `${a.name}-own-key-generated`)
    await page.check('input[name=saved-key]')
    a.ownPrivate = priv
    a.userKey = pub
  } else {
    await page.check('input[name=rk-mode][value=paste]')
    await page.fill('input[name=recovery-did-key]', 'did:key:zQ3shokFTS3brHcDQrn82RUDfCZESWL1ZdCEJwekUDPQiYBmf')
    await page.locator('.rk-bad').waitFor()
    await page.fill('input[name=plc-token]', token)
    await page.check('input[name=understood]')
    check(await moveBtn.isDisabled(), 'an invalid did:key is refused before anything is signed')
    await shot(page, `${a.name}-own-key-invalid`)
    a.userKey = newDidKey()
    await page.fill('input[name=recovery-did-key]', a.userKey)
  }
  await page.locator('.mig-yours').waitFor()
  check(!(await moveBtn.isDisabled()), 'with the key ready, the move can go ahead')
  await shot(page, `${a.name}-own-key-ready`)
}

/** The account page: add a recovery key with an emailed code, then remove it. */
async function accountRecoveryKey(page, a) {
  await page.click('aside.sidenav a:has-text("Security")')
  const panel = page.locator('#recovery-key')
  await panel.locator('.rk-list li').first().waitFor({ timeout: 20_000 })
  const rec = (await plcData(a.did)).rotationKeys
  await shot(page, `${a.name}-account-keys`)
  await panel.locator('button:has-text("Add a recovery key")').click()
  await panel.locator('input[name=rk-mode][value=paste]').check()
  const key = newDidKey()
  await panel.locator('input[name=recovery-did-key]').fill(key)
  let before = await lastDevMailToken(a.email)
  await panel.locator('button:has-text("Continue")').click()
  await panel.locator('input[name=plc-token]').fill(await devMailToken(a.email, before))
  await panel.locator('button:has-text("Add recovery key")').click()
  await panel.getByText('Your recovery key is added').waitFor()
  let data = await plcData(a.did)
  check(JSON.stringify(data.rotationKeys) === JSON.stringify([key, ...rec]), 'account page: the added key is first in PLC rotationKeys', JSON.stringify(data.rotationKeys))
  await shot(page, `${a.name}-account-key-added`)
  before = await lastDevMailToken(a.email)
  await panel.locator('li', { hasText: key }).locator('button:has-text("Remove")').click()
  await panel.locator('input[name=plc-token]').fill(await devMailToken(a.email, before))
  await panel.locator('button:has-text("Remove key")').click()
  await panel.getByText('That key is removed').waitFor()
  data = await plcData(a.did)
  check(JSON.stringify(data.rotationKeys) === JSON.stringify(rec), 'account page: removing the key restores the server keys', JSON.stringify(data.rotationKeys))
}

async function drive(state) {
  const browser = await chromium.launch({ headless: !HEADED })
  try {
    // alice and dave in advanced mode with their own recovery keys; bob and carol in simple mode
    const plans = {
      // backups: bob saves one at the step (streamed to a picked file), the
      // others skip it; alice also takes one from the welcome screen (with
      // her new recovery key) and dave from the account page (in memory)
      alice: { byHandle: true, inviteInUrl: true, appPasswordFirst: true, advanced: true, ownKey: 'generate', backupAtWelcome: 'memory' },
      bob: { loseCreateAnswer: true, wrongPlcToken: true, fromLanding: true, accountKey: true, fakeProfile: true, holdBlobs: true, backupAtStep: 'stream' },
      carol: { reloadMidBlobs: true, newTabBeforePlc: true },
      dave: { keepHandle: true, advanced: true, ownKey: 'paste', backupOnAccount: 'memory' },
    }
    for (const [name, opts] of Object.entries(plans)) {
      const ctx = await browser.newContext({ viewport: { width: 1180, height: 900 }, acceptDownloads: true })
      await ctx.addInitScript(SAVE_PICKER)
      try {
        await driveOne(ctx, state.accounts[name], state, opts)
      } catch (e) {
        for (const p of ctx.pages()) await shot(p, `${name}-error`).catch(() => {})
        throw e
      }
      await ctx.close()
    }
    simpleMode = false
    // the page refuses an account that already lives here
    const ctx = await browser.newContext()
    const page = await ctx.newPage()
    await page.goto(`${VLPDS}/migrate`)
    await page.fill('input[name=identifier]', state.accounts.alice.did)
    await page.click('button:has-text("Find my account")')
    await page.getByText(/already lives on/).waitFor()
    await shot(page, 'already-moved')
    check(true, 'an account that already moved here is recognised up front')
    await page.fill('input[name=identifier]', 'did:web:pds.example.com')
    await page.click('button:has-text("Find my account")')
    await page.getByText(/managed on its own website/).waitFor()
    await shot(page, 'did-web-simple')
    check(true, 'simple mode: a did:web is refused in plain words')
    await page.check('input[name=advanced]')
    await page.getByText(/is a did:web/).waitFor()
    await shot(page, 'did-web')
    check(true, 'advanced mode: a did:web is refused with the manual steps')
    await ctx.close()
  } finally {
    await browser.close()
  }
  save(state)
}

// ---------------------------------------------------------------- backups

const BACKUP_HEADING = /Save a copy of your account|Back up before the switch/

/** Installed in every page before its scripts. Which save path the page
 * takes is set per download through window.__backupMode: 'stream' stands in
 * for showSaveFilePicker (headless Chromium has no file dialog) with a
 * writable that keeps the bytes; 'memory' hides it, so the page builds the
 * ZIP in memory and downloads a blob: URL. */
const SAVE_PICKER = () => {
  const real = window.showSaveFilePicker
  Object.defineProperty(window, 'showSaveFilePicker', {
    configurable: true,
    get() {
      if (window.__backupMode === 'memory') return undefined
      if (window.__backupMode !== 'stream') return real
      return async (opts) => {
        window.__backupName = opts?.suggestedName
        return {
          createWritable: async () => {
            const chunks = []
            window.__backupBytes = undefined
            return new WritableStream({
              write: (c) => void chunks.push(c),
              close: () => {
                window.__backupBytes = new Blob(chunks)
              },
            })
          },
        }
      }
    },
  })
}

/** Clicks the backup button and returns the ZIP's bytes. */
async function downloadBackup(page, mode, click) {
  await page.evaluate((m) => (window.__backupMode = m), mode)
  if (mode === 'memory') {
    const [dl] = await Promise.all([page.waitForEvent('download', { timeout: 120_000 }), click()])
    check(/-backup-\d{4}-\d\d-\d\d\.zip$/.test(dl.suggestedFilename()), `in-memory backup downloaded as ${dl.suggestedFilename()}`)
    return readFileSync(await dl.path())
  }
  await click()
  for (let i = 0; ; i++) {
    const b64 = await page.evaluate(async () => {
      const b = window.__backupBytes
      if (!b) return null
      const bytes = new Uint8Array(await b.arrayBuffer())
      let s = ''
      for (let i = 0; i < bytes.length; i += 0x8000) s += String.fromCharCode(...bytes.subarray(i, i + 0x8000))
      return btoa(s)
    })
    if (b64) {
      check(true, `backup streamed through the save picker (${await page.evaluate(() => window.__backupName)})`)
      return Buffer.from(b64, 'base64')
    }
    if (i > 600) throw new Error('the streamed backup never finished')
    await sleep(200)
  }
}

const canon = (v) => (Array.isArray(v) ? v.map(canon) : v && typeof v === 'object' ? Object.fromEntries(Object.keys(v).sort().map((k) => [k, canon(v[k])])) : v)
const norm = (p) => JSON.stringify(canon(p.filter((x) => !x.$type.endsWith('#declaredAgePref')).sort((x, y) => x.$type.localeCompare(y.$type))))

const B32 = 'abcdefghijklmnopqrstuvwxyz234567'
function b32(bytes) {
  let out = 'b'
  let acc = 0
  let bits = 0
  for (const x of bytes) {
    acc = ((acc << 8) | x) & 0xffff
    bits += 8
    while (bits >= 5) {
      bits -= 5
      out += B32[(acc >> bits) & 31]
    }
  }
  if (bits) out += B32[(acc << (5 - bits)) & 31]
  return out
}

function uvarint(b, at) {
  let n = 0
  for (let shift = 1; ; shift *= 128) {
    const x = b[at++]
    n += (x & 0x7f) * shift
    if (x < 0x80) return [n, at]
  }
}

/** Just enough DAG-CBOR for a CAR header: maps, arrays, strings, bytes, ints, tag 42. */
function cbor(b, at = 0) {
  const ib = b[at++]
  const major = ib >> 5
  let n = ib & 31
  if (n === 24) n = b[at++]
  else if (n === 25) (n = b.readUInt16BE(at)), (at += 2)
  else if (n === 26) (n = b.readUInt32BE(at)), (at += 4)
  else if (n === 27) (n = Number(b.readBigUInt64BE(at))), (at += 8)
  if (major === 0) return [n, at]
  if (major === 2) return [b.subarray(at, at + n), at + n]
  if (major === 3) return [b.subarray(at, at + n).toString(), at + n]
  if (major === 4 || major === 5) {
    const out = major === 4 ? [] : {}
    for (let i = 0; i < n; i++) {
      let k, v
      if (major === 5) [k, at] = cbor(b, at)
      ;[v, at] = cbor(b, at)
      if (major === 4) out.push(v)
      else out[k] = v
    }
    return [out, at]
  }
  if (major === 6 && n === 42) {
    const [v, end] = cbor(b, at)
    return [b32(v.subarray(1)), end]
  }
  throw new Error(`unexpected CBOR major type ${major} at ${at - 1}`)
}

/** The CAR's header, and how many blocks don't hash to their CID. */
function parseCar(car) {
  const [hlen, h0] = uvarint(car, 0)
  const [header] = cbor(car.subarray(h0, h0 + hlen))
  let blocks = 0
  let bad = 0
  for (let at = h0 + hlen; at < car.length; ) {
    const [len, start] = uvarint(car, at)
    let p = start
    ;[, p] = uvarint(car, p)
    ;[, p] = uvarint(car, p)
    const [code, p2] = uvarint(car, p)
    const [dlen, d0] = uvarint(car, p2)
    const digest = car.subarray(d0, d0 + dlen)
    const data = car.subarray(d0 + dlen, start + len)
    if (code !== 0x12 || createHash('sha256').update(data).digest().compare(digest) !== 0) bad++
    blocks++
    at = start + len
  }
  return { root: header.roots?.[0], version: header.version, blocks, bad }
}

/** Unzips a backup and checks it against the seeded account and the server it came from. */
async function verifyBackup(zip, a, { base, label, password = [], key, extras }) {
  const name = `${OUT}backup-${a.name}-${label}`
  const dir = `${name}/`
  rmSync(dir, { recursive: true, force: true })
  mkdirSync(dir, { recursive: true })
  writeFileSync(`${name}.zip`, zip)
  execFileSync('unzip', ['-tq', `${name}.zip`])
  execFileSync('unzip', ['-q', `${name}.zip`, '-d', dir])
  label = `backup (${label})`
  check(true, `${label}: the ZIP (${zip.length} bytes) passes unzip -t`)
  const read = (f) => readFileSync(`${dir}${f}`)
  const json = (f) => JSON.parse(read(f).toString())

  const car = parseCar(read('repo.car'))
  const head = await xrpc(base, 'com.atproto.sync.getLatestCommit', { params: { did: a.did } })
  check(car.version === 1 && car.bad === 0 && car.blocks > 10, `${label}: repo.car parses, all ${car.blocks} blocks hash to their CIDs`, JSON.stringify(car))
  check(car.root === head.cid, `${label}: repo.car's root is getLatestCommit's ${head.cid}`, car.root)

  const files = existsSync(`${dir}blobs`) ? readdirSync(`${dir}blobs`).sort() : []
  const want = Object.keys(a.blobs).sort()
  const badHash = files.filter((c) => sha(read(`blobs/${c}`)) !== a.blobs[c])
  check(JSON.stringify(files) === JSON.stringify(want), `${label}: blobs/ holds all ${want.length} blobs`, `${files.length} files`)
  check(badHash.length === 0, `${label}: every blob's sha-256 matches the seeded bytes`, badHash.join(' '))
  check(!existsSync(`${dir}missing-blobs.txt`), `${label}: no missing-blobs.txt`)

  check(norm(json('preferences.json').preferences) === norm(a.prefs), `${label}: preferences.json equals the seeded preferences`)
  const doc = await fetch(`${PLC}/${a.did}`).then((r) => r.json())
  const pick = (d) => JSON.stringify(canon({ id: d.id, alsoKnownAs: d.alsoKnownAs, verificationMethod: d.verificationMethod, service: d.service }))
  check(pick(json('identity/did.json')) === pick(doc), `${label}: identity/did.json matches the PLC directory's document`, `${pick(json('identity/did.json'))} vs ${pick(doc)}`)
  const audit = await fetch(`${PLC}/${a.did}/log/audit`).then((r) => r.json())
  const ops = json('identity/plc-audit-log.json')
  check(JSON.stringify(ops.map((e) => e.cid)) === JSON.stringify(audit.map((e) => e.cid)), `${label}: identity/plc-audit-log.json has the directory's ${audit.length} ops`)

  const acct = json('account.json')
  check(
    acct.did === a.did && acct.latestCommit?.cid === car.root && acct.counts.blobsIncluded === want.length && acct.counts.blobsMissing === 0,
    `${label}: account.json (did, commit, counts)`,
    JSON.stringify(acct),
  )
  const readme = read('README.txt').toString()
  check(readme.includes('importRepo') && /not\s+exportable/.test(readme), `${label}: README.txt explains restoring, and that the signing key stays on the server`)

  const all = execFileSync('find', [dir, '-type', 'f']).toString().trim().split('\n')
  const text = all.filter((f) => !f.includes('/blobs/')).map((f) => readFileSync(f, 'latin1')).join('\n')
  const leaked = password.filter((p) => p && text.includes(p))
  check(leaked.length === 0 && !/accessJwt|refreshJwt|eyJ[A-Za-z0-9_-]{20,}\./.test(text), `${label}: no password, JWT or session token in the backup`)
  if (key) check(read('keys/recovery-key.txt').toString().includes(key), `${label}: keys/recovery-key.txt holds the recovery key ticked in`)
  else check(!existsSync(`${dir}keys`), `${label}: no keys/ without the opt-in`)
  if (extras) {
    const rk = json('vlpds/rotation-keys.json')
    const data = await plcData(a.did)
    check(JSON.stringify(rk.rotationKeys) === JSON.stringify(data.rotationKeys), `${label}: vlpds/rotation-keys.json lists the PLC rotation keys`)
    check(Array.isArray(json('vlpds/app-passwords.json')) && Array.isArray(json('vlpds/connected-apps.json')), `${label}: app password names and connected apps`)
  }
}

// ---------------------------------------------------------------- verify

async function verify(state) {
  for (const a of Object.values(state.accounts)) {
    log(`verify: ${a.name} ${a.did} -> @${a.newHandle}`)
    const s = await xrpc(VLPDS, 'com.atproto.server.createSession', { body: { identifier: a.newHandle, password: a.newPassword ?? a.password } })
    check(s.did === a.did && s.active !== false, 'login on vlpds works and the account is active')
    const jwt = s.accessJwt

    // records: every URI with the same CID
    const desc = await xrpc(VLPDS, 'com.atproto.repo.describeRepo', { params: { repo: a.did } })
    const got = {}
    for (const c of desc.collections) {
      let cursor
      do {
        const r = await xrpc(VLPDS, 'com.atproto.repo.listRecords', { params: { repo: a.did, collection: c, limit: 100, cursor } })
        for (const x of r.records) got[x.uri] = x.cid
        cursor = r.records.length ? r.cursor : undefined
      } while (cursor)
    }
    const want = a.records
    const diff = Object.keys(want).filter((u) => got[u] !== want[u]).concat(Object.keys(got).filter((u) => !(u in want)))
    check(diff.length === 0, `all ${Object.keys(want).length} records present with the same CIDs`, diff.slice(0, 3).join(' '))

    // blobs: nothing missing, bytes identical
    const missing = await xrpc(VLPDS, 'com.atproto.repo.listMissingBlobs', { auth: jwt, params: { limit: 1000 } })
    check(missing.blobs.length === 0, 'listMissingBlobs is empty', JSON.stringify(missing.blobs.slice(0, 2)))
    let bad = 0
    for (const [cid, hash] of Object.entries(a.blobs)) {
      const b = await xrpc(VLPDS, 'com.atproto.sync.getBlob', { params: { did: a.did, cid } })
      if (sha(b) !== hash) bad++
    }
    check(bad === 0, `all ${Object.keys(a.blobs).length} blobs served with identical bytes`, `${bad} differ`)
    const st = await xrpc(VLPDS, 'com.atproto.server.checkAccountStatus', { auth: jwt })
    check(
      st.activated && st.validDid && st.indexedRecords === a.oldStatus.indexedRecords && st.importedBlobs === st.expectedBlobs,
      `checkAccountStatus: active, valid DID, ${st.indexedRecords} records, ${st.importedBlobs}/${st.expectedBlobs} blobs`,
      JSON.stringify(st),
    )

    // preferences (vlpds adds the derived declared-age pref, as the reference does)
    const prefs = await xrpc(VLPDS, 'app.bsky.actor.getPreferences', { auth: jwt })
    check(norm(prefs.preferences) === norm(a.prefs), 'preferences equal', `${norm(prefs.preferences)} vs ${norm(a.prefs)}`)

    // the PLC directory points here
    const data = await fetch(`${PLC}/${a.did}/data`).then((r) => r.json())
    const rec = await xrpc(VLPDS, 'com.atproto.identity.getRecommendedDidCredentials', { auth: jwt })
    check(data.services?.atproto_pds?.endpoint === VLPDS, `PLC: PDS endpoint is ${VLPDS}`, JSON.stringify(data.services))
    check(data.verificationMethods?.atproto === rec.verificationMethods.atproto, 'PLC: signing key is the vlpds one')
    const wantKeys = [...(a.userKey ? [a.userKey] : []), ...rec.rotationKeys]
    check(
      JSON.stringify(data.rotationKeys) === JSON.stringify(wantKeys),
      a.userKey ? "PLC: rotation keys are the user's own key first, then vlpds's" : "PLC: rotation keys are vlpds's",
      JSON.stringify(data.rotationKeys),
    )
    check(data.alsoKnownAs?.[0] === `at://${a.newHandle}`, `PLC: handle at://${a.newHandle}`)

    // both sides' status
    const newRs = await xrpc(VLPDS, 'com.atproto.sync.getRepoStatus', { params: { did: a.did } })
    check(newRs.active === true, 'vlpds getRepoStatus: active')
    const oldRs = await xrpc(REF, 'com.atproto.sync.getRepoStatus', { params: { did: a.did } })
    check(oldRs.active === false && oldRs.status === 'deactivated', 'reference getRepoStatus: deactivated', JSON.stringify(oldRs))

    // the repo is servable and the identity resolves here
    const car = await xrpc(VLPDS, 'com.atproto.sync.getRepo', { params: { did: a.did } })
    check(car.length > 1000 && car[0] > 0, `getRepo serves a CAR (${car.length} bytes)`)
    const head = await xrpc(VLPDS, 'com.atproto.sync.getLatestCommit', { params: { did: a.did } })
    check(!!head.cid && !!head.rev, 'getLatestCommit answers')
    const id = await xrpc(VLPDS, 'com.atproto.identity.resolveHandle', { params: { handle: a.newHandle } })
    check(id.did === a.did, 'the new handle resolves to the DID')
    // and it takes writes
    await xrpc(VLPDS, 'com.atproto.repo.createRecord', {
      auth: jwt,
      body: { repo: a.did, collection: 'app.bsky.feed.post', record: { $type: 'app.bsky.feed.post', text: 'hello from vlpds', createdAt: now() } },
    })
    check(true, 'a new post is accepted on vlpds')
  }
}

// ---------------------------------------------------------------- main

const phase = process.argv[2] ?? 'all'
try {
  let state = phase === 'all' || phase === 'seed' ? await seed() : load()
  if (phase === 'all' || phase === 'drive') await drive(state)
  if (phase === 'all' || phase === 'verify') await verify(load())
} catch (e) {
  failures++
  log('ERROR', e.stack ?? e)
}
log(failures ? `RESULT: FAIL (${failures})` : 'RESULT: PASS')
process.exit(failures ? 1 : 0)
