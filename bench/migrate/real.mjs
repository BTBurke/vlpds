// Real migrations (README.md "Real run"): seeds test accounts on a live
// reference PDS, drives the deployed /migrate for some of them, verifies
// against the live PLC directory. Unlike e2e.mjs this writes to the public
// network, so it paces itself: at most ~1.8 writes/s per account with jitter,
// under 150 records per repo, and it only interacts with its own accounts.
//
//   node real.mjs seed     accounts + content on REF_PDS (state in out/real.json)
//   node real.mjs drive    migrate the accounts listed in MIGRATE (comma names)
//   node real.mjs verify   check the migrated ones
//
// Env: REF_PDS, VLPDS, VLPDS_ADMIN_URL (where vlpds.admin/createInviteCodes is
// reachable), VLPDS_ADMIN (token), REF_SSH (user@host of the old PDS, for
// invite codes and the PLC confirmation token in /pds/account.sqlite).

import { chromium } from 'playwright'
import { createHash, randomBytes } from 'node:crypto'
import { execFileSync } from 'node:child_process'
import { deflateSync } from 'node:zlib'
import { chmodSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'

const REF = process.env.REF_PDS ?? 'https://old-pds.example.com'
const VLPDS = process.env.VLPDS ?? 'https://pds.example.com'
const ADMIN_URL = process.env.VLPDS_ADMIN_URL ?? VLPDS
const ADMIN = process.env.VLPDS_ADMIN
const REF_SSH = process.env.REF_SSH
const PLC = process.env.PLC ?? 'https://plc.directory'
const NAMES = (process.env.NAMES ?? 'vlmig1,vlmig2,vlmig3,jazmig').split(',')
const OUT = new URL('./out/', import.meta.url).pathname
const STATE = `${OUT}real.json`
const SHOTS = `${OUT}real-shots/`

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
  for (let attempt = 0; ; attempt++) {
    const r = await fetch(`${base}/xrpc/${nsid}${q}`, { method: method ?? (payload !== undefined ? 'POST' : 'GET'), headers, body: payload })
    const buf = Buffer.from(await r.arrayBuffer())
    if (r.status === 429 && attempt < 5) {
      const wait = Number(r.headers.get('retry-after') ?? 30)
      log(`  429 on ${nsid}, waiting ${wait}s`)
      await sleep(wait * 1000)
      continue
    }
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
}

// One write at a time per account, 0.55-1.5 s apart, with an occasional
// longer pause, so no account ever exceeds ~1.8 writes/s.
async function paced(fn) {
  const r = await fn()
  await sleep(550 + Math.random() * 950 + (Math.random() < 0.08 ? 2000 + Math.random() * 4000 : 0))
  return r
}

function ssh(cmd) {
  if (!REF_SSH) throw new Error('REF_SSH is not set')
  return execFileSync('ssh', [REF_SSH, cmd], { encoding: 'utf8' }).trim()
}
const sqlq = (s) => `'${s.replace(/'/g, "''")}'`
function plcToken(did, since) {
  const out = ssh(`sudo sqlite3 /pds/account.sqlite "select token, requestedAt from email_token where purpose='plc_operation' and did=${sqlq(did).replace(/"/g, '\\"')}"`)
  const [token, at] = out.split('|')
  if (!token || Date.parse(at) < since - 5000) return undefined
  return token
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
function png(seed, w, h) {
  const raw = Buffer.alloc((w * 3 + 1) * h)
  const s = createHash('sha256').update(String(seed)).digest()
  for (let y = 0; y < h; y++) {
    raw[y * (w * 3 + 1)] = 0
    for (let x = 0; x < w; x++) {
      const o = y * (w * 3 + 1) + 1 + x * 3
      raw[o] = (s[0] + x * s[3] / 64) & 0xff
      raw[o + 1] = (s[1] + y * s[4] / 64) & 0xff
      raw[o + 2] = (s[2] + ((x + y) * s[5]) / 128) & 0xff
    }
  }
  const ihdr = Buffer.alloc(13)
  ihdr.writeUInt32BE(w, 0)
  ihdr.writeUInt32BE(h, 4)
  ihdr[8] = 8
  ihdr[9] = 2
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    chunk('IHDR', ihdr),
    chunk('IDAT', deflateSync(raw)),
    chunk('IEND', Buffer.alloc(0)),
  ])
}
const sha = (b) => createHash('sha256').update(b).digest('hex')

// ---------------------------------------------------------------- state

function save(s) {
  mkdirSync(OUT, { recursive: true })
  writeFileSync(STATE, JSON.stringify(s, null, 2))
  chmodSync(STATE, 0o600)
}
const load = () => (existsSync(STATE) ? JSON.parse(readFileSync(STATE, 'utf8')) : { accounts: {} })
const now = () => new Date().toISOString()

const PREFS = (name, i) => [
  { $type: 'app.bsky.actor.defs#adultContentPref', enabled: i % 2 === 0 },
  { $type: 'app.bsky.actor.defs#contentLabelPref', label: 'gore', visibility: 'hide' },
  { $type: 'app.bsky.actor.defs#contentLabelPref', label: 'nudity', visibility: i % 2 ? 'warn' : 'show' },
  {
    $type: 'app.bsky.actor.defs#savedFeedsPrefV2',
    items: [
      { type: 'timeline', value: 'following', pinned: true, id: '3kxyzkqgaq22a' },
      { type: 'feed', value: 'at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/whats-hot', pinned: i % 2 === 0, id: '3kxyzkqgaq22b' },
    ],
  },
  { $type: 'app.bsky.actor.defs#mutedWordsPref', items: [{ value: `spoilers-${name}`, targets: ['content', 'tag'], actorTarget: 'all' }] },
  { $type: 'app.bsky.actor.defs#threadViewPref', sort: i % 2 ? 'oldest' : 'newest' },
  { $type: 'app.bsky.actor.defs#feedViewPref', feed: 'home', hideReplies: false, hideReposts: i % 2 === 1 },
]

const TEXTS = [
  'testing a PDS migration, ignore me',
  'this account exists to prove vlpds can take a whole repo from a reference PDS',
  'records, blobs and preferences should all arrive intact',
  'hello from a migration test account',
  'checking that images survive the move',
  'another test post, nothing to see here',
]

// ---------------------------------------------------------------- seed

async function seed() {
  const state = load()
  log(`seed on ${REF}: ${NAMES.join(', ')}`)
  for (const [i, name] of NAMES.entries()) {
    if (state.accounts[name]?.seeded) continue
    const handle = `${name}.${new URL(REF).hostname}`
    const email = `${name}@example.com`
    const password = randomBytes(15).toString('base64url')
    const invite = ssh('sudo pdsadmin create-invite-code').split('\n').pop().trim()
    const s = await xrpc(REF, 'com.atproto.server.createAccount', { body: { handle, email, password, inviteCode: invite } })
    state.accounts[name] = { name, i, did: s.did, handle, email, password, blobs: {} }
    save(state)
    log(`  ${handle} ${s.did}`)
  }
  const sessions = {}
  for (const name of NAMES) {
    const a = state.accounts[name]
    sessions[name] = (await xrpc(REF, 'com.atproto.server.createSession', { body: { identifier: a.did, password: a.password } })).accessJwt
  }
  // each account's content, interleaved across accounts so the PDS sees a mix
  await Promise.all(
    NAMES.map(async (name) => {
      const a = state.accounts[name]
      if (a.seeded) return
      const jwt = sessions[name]
      const up = async (bytes) => {
        const r = await paced(() => xrpc(REF, 'com.atproto.repo.uploadBlob', { bytes, type: 'image/png', auth: jwt }))
        a.blobs[r.blob.ref.$link] = sha(bytes)
        return r.blob
      }
      const avatar = await up(png(`${a.did}-avatar`, 400, 400))
      const banner = await up(png(`${a.did}-banner`, 1500, 500))
      await paced(() =>
        xrpc(REF, 'com.atproto.repo.putRecord', {
          auth: jwt,
          body: {
            repo: a.did,
            collection: 'app.bsky.actor.profile',
            rkey: 'self',
            record: {
              $type: 'app.bsky.actor.profile',
              displayName: `${name} (migration test)`,
              description: 'A test account for moving between PDSes (vlpds). Not a real person.',
              avatar,
              banner,
              createdAt: now(),
            },
          },
        }),
      )
      const posts = 70 + a.i * 5
      for (let p = 0; p < posts; p++) {
        const record = { $type: 'app.bsky.feed.post', text: `${TEXTS[p % TEXTS.length]} (#${p + 1})`, createdAt: now(), langs: ['en'] }
        if (p % 6 === 0) {
          const n = 1 + (p % 4)
          const images = []
          for (let k = 0; k < n; k++) images.push({ alt: `test image ${p}.${k}`, image: await up(png(`${a.did}-${p}-${k}`, 800 + k * 40, 600)), aspectRatio: { width: 800 + k * 40, height: 600 } })
          record.embed = { $type: 'app.bsky.embed.images', images }
        }
        await paced(() => xrpc(REF, 'com.atproto.repo.createRecord', { auth: jwt, body: { repo: a.did, collection: 'app.bsky.feed.post', record } }))
      }
      await paced(() => xrpc(REF, 'app.bsky.actor.putPreferences', { auth: jwt, body: { preferences: PREFS(name, a.i) } }))
      log(`  ${name}: profile, ${posts} posts, ${Object.keys(a.blobs).length} blobs`)
    }),
  )
  // follows, likes and reposts between the test accounts only
  await Promise.all(
    NAMES.map(async (name) => {
      const a = state.accounts[name]
      if (a.seeded) return
      const jwt = sessions[name]
      for (const other of NAMES) {
        if (other === name) continue
        const b = state.accounts[other]
        await paced(() =>
          xrpc(REF, 'com.atproto.repo.createRecord', {
            auth: jwt,
            body: { repo: a.did, collection: 'app.bsky.graph.follow', record: { $type: 'app.bsky.graph.follow', subject: b.did, createdAt: now() } },
          }),
        )
        const theirs = await xrpc(REF, 'com.atproto.repo.listRecords', { params: { repo: b.did, collection: 'app.bsky.feed.post', limit: 12 } })
        for (const [k, p] of theirs.records.entries()) {
          const collection = k % 6 === 5 ? 'app.bsky.feed.repost' : 'app.bsky.feed.like'
          await paced(() =>
            xrpc(REF, 'com.atproto.repo.createRecord', {
              auth: jwt,
              body: { repo: a.did, collection, record: { $type: collection, subject: { uri: p.uri, cid: p.cid }, createdAt: now() } },
            }),
          )
        }
      }
      a.seeded = true
      save(state)
    }),
  )
  for (const name of NAMES) await snapshot(state, name, sessions[name])
  save(state)
}

// The source of truth for verify: every record's CID, the prefs, the counts.
async function snapshot(state, name, jwt) {
  const a = state.accounts[name]
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
  a.records = records
  a.oldStatus = await xrpc(REF, 'com.atproto.server.checkAccountStatus', { auth: jwt })
  a.prefs = (await xrpc(REF, 'app.bsky.actor.getPreferences', { auth: jwt })).preferences
  log(`  ${name}: ${Object.keys(records).length} records, ${Object.keys(a.blobs).length} blobs, ${a.oldStatus.repoBlocks} blocks`)
}

// ---------------------------------------------------------------- drive

let shotN = 0
async function shot(page, name) {
  mkdirSync(SHOTS, { recursive: true })
  await page.screenshot({ path: `${SHOTS}${String(++shotN).padStart(2, '0')}-${name}.png`, fullPage: true })
}
const waitHeading = (page, re, timeout = 60_000) => page.locator('.mig-card h1', { hasText: re }).waitFor({ timeout })

async function vlpdsInvite() {
  const basic = `Basic ${Buffer.from(`admin:${ADMIN}`).toString('base64')}`
  const r = await xrpc(ADMIN_URL, 'com.atproto.server.createInviteCodes', { auth: basic, body: { codeCount: 1, useCount: 1 } })
  return r.codes[0].codes[0]
}

async function driveOne(browser, state, name, opts) {
  const a = state.accounts[name]
  log(`drive: ${name} (${a.handle})`)
  const ctx = await browser.newContext({ viewport: { width: 1180, height: 900 } })
  const page = await ctx.newPage()
  page.on('pageerror', (e) => log(`  [pageerror] ${e.message}`))
  page.on('console', (m) => m.type() === 'error' && log(`  [console] ${m.text()}`))
  const invite = await vlpdsInvite()
  await page.goto(`${VLPDS}/migrate${opts.inviteInUrl ? `?invite=${invite}` : ''}`)
  await waitHeading(page, /Move your account here/)
  await page.fill('input[name=identifier]', opts.byDid ? a.did : a.handle)
  await page.click('button:has-text("Find my account")')
  await waitHeading(page, /Sign in to/)
  await shot(page, `${name}-signin`)
  await page.fill('input[name=password]', a.password)
  await page.click('button:has-text("Sign in")')

  await waitHeading(page, /Pre-flight checks/)
  for (let i = 0; (await page.locator('.mig-check .spinner').count()) > 0; i++) {
    if (i > 300) throw new Error('preflight checks never finished')
    await sleep(200)
  }
  await shot(page, `${name}-checks`)
  const checksText = await page.locator('.mig-checks').innerText()
  check(checksText.includes(`${a.oldStatus.indexedRecords} records`), 'preflight shows the record count', checksText)
  await page.click('button:has-text("Continue")')

  await waitHeading(page, /Choose your handle/)
  await page.fill('input[name=handle]', name)
  await page.getByText(/is available/).waitFor()
  await shot(page, `${name}-handle`)
  await page.click('button:has-text("Continue")')
  a.newHandle = `${name}.${new URL(VLPDS).hostname}`

  await waitHeading(page, /Create @/)
  if (opts.newPassword) {
    a.newPassword = randomBytes(15).toString('base64url')
    save(state)
    await page.uncheck('input[name=same-password]')
    await page.fill('input[name=new-password]', a.newPassword)
  }
  if (!opts.inviteInUrl) await page.fill('input[name=invite]', invite)
  await shot(page, `${name}-create`)
  await page.click('button:has-text("Create my account here")')

  await waitHeading(page, /Copying your data/)
  if (opts.reloadMidBlobs) {
    for (let i = 0; !/\b([1-9]\d*) of \d+ copied/.test(await page.locator('.mig-card').innerText()); i++) {
      if (i > 600) break
      await sleep(100)
    }
    await shot(page, `${name}-copy-midway`)
    let imports = 0
    page.on('request', (r) => r.url().includes('com.atproto.repo.importRepo') && imports++)
    await page.reload()
    await waitHeading(page, /Copying your data|Move your identity/)
    await waitHeading(page, /Move your identity/, 600_000)
    check(imports === 0, 'a reload mid-copy resumed without importing the repo again', `imports=${imports}`)
  } else {
    await waitHeading(page, /Move your identity/, 600_000)
  }

  await page.locator('.mig-diff').waitFor()
  await shot(page, `${name}-identity-review`)
  const tPlc = Date.now()
  await page.click('button:has-text("Email me a confirmation code")')
  await page.locator('input[name=plc-token]').waitFor()
  let token
  for (let i = 0; !(token = plcToken(a.did, tPlc)); i++) {
    if (i > 30) throw new Error('no PLC token in the old PDS database')
    await sleep(1000)
  }
  if (opts.wrongPlcToken) {
    await page.fill('input[name=plc-token]', 'AAAAA-BBBBB')
    await page.check('input[name=understood]')
    await page.click('button:has-text("Move my identity")')
    await page.getByText(/That code isn't right|code has expired/).waitFor()
    await shot(page, `${name}-wrong-code`)
    check(true, 'a wrong PLC code is refused and the step stays put')
    await page.fill('input[name=plc-token]', token)
  } else {
    await page.fill('input[name=plc-token]', token)
    await page.check('input[name=understood]')
  }
  await page.click('button:has-text("Move my identity")')

  await waitHeading(page, /Welcome to your new home/, 180_000)
  await page.locator('.tiles').waitFor()
  await shot(page, `${name}-done`)
  a.migrated = true
  save(state)
  await ctx.close()
}

async function drive(state) {
  const plans = {
    vlmig1: { inviteInUrl: true },
    vlmig2: { byDid: true, newPassword: true, wrongPlcToken: true },
    vlmig3: { reloadMidBlobs: true },
  }
  const only = (process.env.MIGRATE ?? Object.keys(plans).join(',')).split(',')
  const browser = await chromium.launch({ headless: !process.env.HEADED })
  try {
    for (const name of only) if (!state.accounts[name].migrated) await driveOne(browser, state, name, plans[name] ?? {})
  } finally {
    await browser.close()
  }
}

// ---------------------------------------------------------------- verify

async function verify(state) {
  for (const a of Object.values(state.accounts).filter((a) => a.migrated)) {
    log(`verify: ${a.name} ${a.did} -> @${a.newHandle}`)
    const s = await xrpc(VLPDS, 'com.atproto.server.createSession', { body: { identifier: a.newHandle, password: a.newPassword ?? a.password } })
    check(s.did === a.did && s.active !== false, 'login on vlpds works and the account is active')
    const jwt = s.accessJwt
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
    const prefs = await xrpc(VLPDS, 'app.bsky.actor.getPreferences', { auth: jwt })
    const canon = (v) =>
      Array.isArray(v) ? v.map(canon) : v && typeof v === 'object' ? Object.fromEntries(Object.keys(v).sort().map((k) => [k, canon(v[k])])) : v
    const norm = (p) => JSON.stringify(canon(p.filter((x) => !x.$type.endsWith('#declaredAgePref')).sort((x, y) => x.$type.localeCompare(y.$type))))
    check(norm(prefs.preferences) === norm(a.prefs), 'preferences equal', `${norm(prefs.preferences)} vs ${norm(a.prefs)}`)
    const data = await fetch(`${PLC}/${a.did}/data`).then((r) => r.json())
    const rec = await xrpc(VLPDS, 'com.atproto.identity.getRecommendedDidCredentials', { auth: jwt })
    check(data.services?.atproto_pds?.endpoint === VLPDS, `PLC: PDS endpoint is ${VLPDS}`, JSON.stringify(data.services))
    check(data.verificationMethods?.atproto === rec.verificationMethods.atproto, 'PLC: signing key is the vlpds one')
    check(JSON.stringify(data.rotationKeys) === JSON.stringify(rec.rotationKeys), 'PLC: rotation keys are the vlpds ones')
    check(data.alsoKnownAs?.[0] === `at://${a.newHandle}`, `PLC: handle at://${a.newHandle}`)
    const newRs = await xrpc(VLPDS, 'com.atproto.sync.getRepoStatus', { params: { did: a.did } })
    check(newRs.active === true, 'vlpds getRepoStatus: active')
    const oldRs = await xrpc(REF, 'com.atproto.sync.getRepoStatus', { params: { did: a.did } })
    check(oldRs.active === false && oldRs.status === 'deactivated', 'old PDS getRepoStatus: deactivated', JSON.stringify(oldRs))
    const id = await xrpc(VLPDS, 'com.atproto.identity.resolveHandle', { params: { handle: a.newHandle } })
    check(id.did === a.did, 'the new handle resolves to the DID')
    const wk = await fetch(`https://${a.newHandle}/.well-known/atproto-did`).then((r) => r.text())
    check(wk.trim() === a.did, 'https://<handle>/.well-known/atproto-did serves the DID')
  }
}

const phase = process.argv[2]
try {
  if (phase === 'seed') await seed()
  else if (phase === 'drive') await drive(load())
  else if (phase === 'verify') await verify(load())
  else throw new Error('usage: node real.mjs seed|drive|verify')
} catch (e) {
  failures++
  log('ERROR', e.stack ?? e)
}
log(failures ? `RESULT: FAIL (${failures})` : 'RESULT: PASS')
process.exit(failures ? 1 : 0)
