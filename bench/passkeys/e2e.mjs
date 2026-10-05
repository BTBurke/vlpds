// Passkeys in a real browser (README.md): headless Chromium with a CDP
// virtual authenticator (ctap2, internal, resident keys, user verification)
// against a local vlpds on http://localhost. Registers a passkey on the
// Security page, uses it as the OAuth page's second step, signs in without
// a password on the OAuth page and the account page, and fails on any CSP
// violation in the console.
//
//   node e2e.mjs      (VLPDS=http://localhost:2790, SHOTS=<dir for screenshots>)

import { chromium } from 'playwright'
import { createHash, generateKeyPairSync, randomBytes, sign } from 'node:crypto'
import { mkdirSync } from 'node:fs'

const VLPDS = process.env.VLPDS ?? 'http://localhost:2790'
const SHOTS = process.env.SHOTS ?? new URL('./out/shots/', import.meta.url).pathname
const HEADED = !!process.env.HEADED
mkdirSync(SHOTS, { recursive: true })

const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a)
let failures = 0
function check(ok, what, detail = '') {
  if (ok) log(`  ok   ${what}`)
  else {
    failures++
    log(`  FAIL ${what} ${detail}`)
  }
}

const b64u = (b) => Buffer.from(b).toString('base64url')

async function xrpc(nsid, body, auth, base = VLPDS) {
  const r = await fetch(`${base.replace('//localhost:', '//127.0.0.1:')}/xrpc/${nsid}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(auth ? { authorization: `Bearer ${auth}` } : {}) },
    body: JSON.stringify(body),
  })
  const j = await r.json().catch(() => ({}))
  if (!r.ok) throw new Error(`${nsid} ${r.status} ${JSON.stringify(j)}`)
  return j
}

// ---------------------------------------------------------------- a minimal OAuth client (PAR + DPoP)

const dpopKey = generateKeyPairSync('ec', { namedCurve: 'P-256' })
const jwk = (() => {
  const { kty, crv, x, y } = dpopKey.publicKey.export({ format: 'jwk' })
  return { kty, crv, x, y }
})()

function dpopProof(htm, htu, nonce) {
  const header = { typ: 'dpop+jwt', alg: 'ES256', jwk }
  const payload = { jti: b64u(randomBytes(12)), htm, htu, iat: Math.floor(Date.now() / 1000), ...(nonce ? { nonce } : {}) }
  const input = `${b64u(JSON.stringify(header))}.${b64u(JSON.stringify(payload))}`
  const sig = sign('sha256', Buffer.from(input), { key: dpopKey.privateKey, dsaEncoding: 'ieee-p1363' })
  return `${input}.${b64u(sig)}`
}

const REDIRECT = 'http://127.0.0.1/cb'
const CLIENT_ID = `http://localhost?scope=${encodeURIComponent('atproto')}&redirect_uri=${encodeURIComponent(REDIRECT)}`

/** A pushed authorization request; the browser URL that opens it. */
async function authorizeUrl(base = VLPDS) {
  const verifier = b64u(randomBytes(32))
  const challenge = b64u(createHash('sha256').update(verifier).digest())
  const form = new URLSearchParams({
    client_id: CLIENT_ID,
    response_type: 'code',
    redirect_uri: REDIRECT,
    scope: 'atproto',
    state: b64u(randomBytes(8)),
    code_challenge: challenge,
    code_challenge_method: 'S256',
  })
  const htu = `${base}/oauth/par`
  let nonce
  for (let i = 0; i < 2; i++) {
    const r = await fetch(htu.replace('//localhost:', '//127.0.0.1:'), {
      method: 'POST',
      headers: { 'content-type': 'application/x-www-form-urlencoded', dpop: dpopProof('POST', htu, nonce) },
      body: form,
    })
    const j = await r.json()
    if (j.error === 'use_dpop_nonce') {
      nonce = r.headers.get('dpop-nonce')
      continue
    }
    if (!r.ok) throw new Error(`PAR ${r.status} ${JSON.stringify(j)}`)
    return `${base}/oauth/authorize?client_id=${encodeURIComponent(CLIENT_ID)}&request_uri=${encodeURIComponent(j.request_uri)}`
  }
  throw new Error('PAR: no nonce')
}

// ---------------------------------------------------------------- the run

const handle = `pk${Date.now().toString(36)}.vlpds.test`
const password = 'correct horse passkey staple'
const acct = await xrpc('com.atproto.server.createAccount', { handle, password, email: `${handle.replace(/\./g, '-')}@example.com` })
log('account', acct.did, handle)

const browser = await chromium.launch({ headless: !HEADED })
const context = await browser.newContext({ viewport: { width: 1180, height: 900 } })
const page = await context.newPage()
const cspErrors = []
page.on('console', (m) => {
  if (/Content Security Policy|Refused to (execute|load|apply)/i.test(m.text())) cspErrors.push(m.text())
})
page.on('pageerror', (e) => cspErrors.push(`pageerror: ${e.message}`))

const cdp = await context.newCDPSession(page)
await cdp.send('WebAuthn.enable')
const { authenticatorId } = await cdp.send('WebAuthn.addVirtualAuthenticator', {
  options: {
    protocol: 'ctap2',
    transport: 'internal',
    hasResidentKey: true,
    hasUserVerification: true,
    isUserVerified: true,
    automaticPresenceSimulation: true,
  },
})

async function shot(name, locator) {
  const path = `${SHOTS}${name}.png`
  if (locator) await locator.screenshot({ path })
  else await page.screenshot({ path, fullPage: true })
  log(`  shot ${path}`)
}

process.on('unhandledRejection', async (e) => {
  console.error(e)
  await page.screenshot({ path: `${SHOTS}failure.png`, fullPage: true }).catch(() => {})
  process.exit(1)
})

// 1. the account page: sign in with the password, add a passkey
log('register on the Security page')
await page.goto(`${VLPDS}/account`)
await page.getByLabel('Handle, DID or email').fill(handle)
await page.getByLabel('Password', { exact: true }).fill(password)
await page.getByRole('button', { name: 'Sign in', exact: true }).click()
await page.getByRole('link', { name: 'Security' }).click()
await page.getByRole('button', { name: 'Add a passkey' }).click()
await page.locator('#passkeys').getByLabel('Name').fill('Virtual authenticator')
await page.locator('#passkeys').getByLabel('Password').fill(password)
await page.getByRole('button', { name: 'Continue' }).click()
await page.getByText('Save your recovery codes').waitFor()
await shot('security-recovery-codes')
await page.getByRole('button', { name: "I've saved them" }).click()
await page.getByRole('cell', { name: /Virtual authenticator/ }).waitFor()
const creds = await cdp.send('WebAuthn.getCredentials', { authenticatorId })
check(creds.credentials.length === 1 && creds.credentials[0].isResidentCredential, 'a discoverable credential was created')
check(creds.credentials[0]?.rpId === 'localhost', 'its RP ID is the public URL host', creds.credentials[0]?.rpId)
check(Buffer.from(creds.credentials[0]?.userHandle ?? '', 'base64').toString() === acct.did, 'its user handle is the DID')
await shot('security-passkeys', page.locator('#passkeys'))
await shot('security-page')

// 2. the OAuth page: the password, then the passkey as the second step
log('OAuth second step')
// the page's autofill request (conditional UI) would answer at once under
// simulated presence: hold presence until the passkey is wanted
const presence = (enabled) => cdp.send('WebAuthn.setAutomaticPresenceSimulation', { authenticatorId, enabled })
await presence(false)
await context.clearCookies()
await page.goto(await authorizeUrl())
await page.getByLabel('Handle or DID').fill(handle)
await page.getByLabel('Password', { exact: true }).fill(password)
await page.getByRole('button', { name: 'Sign in', exact: true }).click()
await page.getByRole('button', { name: 'Use your passkey' }).waitFor()
check(!(await page.content()).includes('Sign-in code from your email'), 'no emailed code in place of the passkey')
await shot('oauth-2fa')
await presence(true)
await page.getByRole('button', { name: 'Use your passkey' }).click()
await page.getByRole('heading', { name: 'Authorize access' }).waitFor()
check(true, 'the passkey passed the second step')
await shot('oauth-consent-after-2fa')

// 3. the OAuth page without a password
log('OAuth passwordless')
await presence(false)
await context.clearCookies()
await page.goto(await authorizeUrl())
const pwless = page.getByRole('button', { name: 'Sign in with a passkey' })
await pwless.waitFor()
check((await page.getByLabel('Handle or DID').getAttribute('autocomplete')) === 'username webauthn', 'autofill is offered')
await shot('oauth-passwordless')
await presence(true)
await pwless.click()
await page.getByRole('heading', { name: 'Authorize access' }).waitFor()
check((await page.content()).includes(handle), 'signed in as the account, no password typed')
await shot('oauth-consent-after-passkey')

// 4. the account page without a password
log('account page passwordless')
await presence(false)
await context.clearCookies()
await page.evaluate(() => sessionStorage.clear())
await page.goto(`${VLPDS}/account`)
await page.getByRole('button', { name: 'Sign in with a passkey' }).waitFor()
await shot('account-sign-in')
await presence(true)
await page.getByRole('button', { name: 'Sign in with a passkey' }).click()
await page.getByRole('link', { name: 'Security' }).waitFor()
check((await page.content()).includes(acct.did), 'the account page is signed in')
await shot('account-passwordless')

// 5. Enter in the password field signs in like the button, with autofill's
// passkey request pending (presence held, so it stays pending)
log('Enter in the password field')
const VLPDS_IP = process.env.VLPDS_IP ?? 'http://127.0.0.1:2791'
const plain = `pe${Date.now().toString(36)}.vlpds.test`
const email = (h) => `${h.replace(/\./g, '-')}@example.com`
await xrpc('com.atproto.server.createAccount', { handle: plain, password, email: email(plain) })
await xrpc('com.atproto.server.createAccount', { handle: plain, password, email: email(plain) }, undefined, VLPDS_IP)
await presence(false)
const errBanner = () => page.locator('.notice.err, p.err:visible').count()

/** Fills the account page's sign-in and presses Enter; createSession's answer ('ok' or the error), if one was sent. */
async function enterOnAccountPage(base, who, autofill) {
  await context.clearCookies()
  await page.evaluate(() => sessionStorage.clear()).catch(() => {})
  const opts = autofill && page.waitForResponse((r) => r.url().includes('vlpds.server.startPasskeySignIn'))
  await page.goto(`${base}/account`)
  await page.getByLabel('Handle, DID or email').waitFor()
  if (opts) {
    await opts
    await page.waitForTimeout(300)
  }
  await page.getByLabel('Handle, DID or email').fill(who)
  const pw = page.getByLabel('Password', { exact: true })
  await pw.fill(password)
  const session = page.waitForResponse((r) => r.url().includes('com.atproto.server.createSession'), { timeout: 5000 }).catch(() => undefined)
  await pw.press('Enter')
  const r = await session
  return r && (r.ok() ? 'ok' : (await r.json().catch(() => ({}))).error)
}

check((await enterOnAccountPage(VLPDS, plain, true)) === 'ok', 'localhost: Enter sends createSession')
await page.getByRole('link', { name: 'Security' }).waitFor({ timeout: 5000 }).catch(() => {})
check((await page.getByRole('link', { name: 'Security' }).count()) === 1 && (await errBanner()) === 0, 'localhost: Enter signs in, no error banner')

check((await enterOnAccountPage(VLPDS, handle, true)) === 'PasskeyRequired', 'localhost, passkey account: Enter sends createSession (PasskeyRequired)')
await page.getByRole('heading', { name: 'Two-factor check' }).waitFor()
check((await page.getByLabel('Handle, DID or email').count()) === 0 && (await errBanner()) === 0, 'it moves to the second step, no error banner')
await presence(true)
await page.getByRole('button', { name: 'Use your passkey' }).click()
await page.getByRole('link', { name: 'Security' }).waitFor()
check((await page.content()).includes(acct.did), 'the passkey button passes the second step after Enter')
await presence(false)

check((await enterOnAccountPage(VLPDS_IP, plain, false)) === 'ok', '127.0.0.1: Enter sends createSession')
await page.getByRole('link', { name: 'Security' }).waitFor({ timeout: 5000 }).catch(() => {})
check((await page.getByRole('link', { name: 'Security' }).count()) === 1 && (await errBanner()) === 0, '127.0.0.1: Enter signs in, no error banner')
await context.clearCookies()
await page.evaluate(() => sessionStorage.clear())
await page.goto(`${VLPDS_IP}/account`)
await page.getByText(/Passkeys are off on 127\.0\.0\.1/).waitFor()
check((await page.getByRole('button', { name: 'Sign in with a passkey' }).count()) === 0 && (await errBanner()) === 0, '127.0.0.1: no passkey button, a note instead, no error banner')
await shot('account-sign-in-ip')

/** The OAuth sign-in page: fill it in, Enter, and the consent screen follows (not Cancel's denial). */
async function enterOnOAuthPage(base, who) {
  await context.clearCookies()
  await page.goto(await authorizeUrl(base))
  await page.getByLabel('Handle or DID').fill(who)
  await page.waitForTimeout(300)
  await page.getByLabel('Password', { exact: true }).fill(password)
  await page.getByLabel('Password', { exact: true }).press('Enter')
  await page.getByRole('heading', { name: 'Authorize access' }).waitFor({ timeout: 5000 }).catch(() => {})
  return page.getByRole('heading', { name: 'Authorize access' }).count()
}
check((await enterOnOAuthPage(VLPDS, plain)) === 1, 'OAuth, localhost: Enter signs in')
await context.clearCookies()
await page.goto(await authorizeUrl(VLPDS_IP))
await page.getByLabel('Handle or DID').waitFor()
await page.waitForTimeout(300)
check(await page.locator('#pk-go').isHidden(), 'OAuth, 127.0.0.1: no passkey button')
check((await errBanner()) === 0, 'OAuth, 127.0.0.1: no error')
check((await enterOnOAuthPage(VLPDS_IP, plain)) === 1, 'OAuth, 127.0.0.1: Enter signs in')
await context.clearCookies()
await page.goto(`${VLPDS_IP}/oauth/account`)
await page.getByLabel('Handle or DID').fill(plain)
await page.getByLabel('Password', { exact: true }).fill(password)
await page.getByLabel('Password', { exact: true }).press('Enter')
await page.getByRole('button', { name: 'Sign out' }).waitFor({ timeout: 5000 }).catch(() => {})
check((await page.getByRole('button', { name: 'Sign out' }).count()) === 1 && (await errBanner()) === 0, '/oauth/account, 127.0.0.1: Enter signs in')

// the log names the passkey sign-ins
const session = await xrpc('com.atproto.server.createSession', { identifier: handle, password }).catch((e) => e)
check(String(session).includes('PasskeyRequired'), 'other apps get PasskeyRequired for the password alone')

check(cspErrors.length === 0, 'no CSP violations or page errors', cspErrors.join(' | '))
await browser.close()
if (failures) {
  log(`${failures} check(s) failed`)
  process.exit(1)
}
log('all passkey browser checks passed')
