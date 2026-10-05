// The boards UI in headless Chromium against a running `just spaces-boards-ui`
// (run.sh boards-ui with UI_E2E=1 does both). Two people in two browsers:
// alice (vlpds, real OAuth through vlpds's sign-in and consent pages) and
// carol (a reference PDS, password). Alice posts with an image, carol sees
// it, comments and votes, alice sees that and replies, carol sees the reply.
// Screenshots go to bench/spaces/out/boards-ui/.
import { mkdirSync, readFileSync } from 'node:fs'
import { deflateSync, crc32 } from 'node:zlib'
import { fileURLToPath } from 'node:url'
import { chromium } from 'playwright'

const here = fileURLToPath(new URL('.', import.meta.url))
const seed = JSON.parse(readFileSync(`${here}../.local/seed-accounts.json`, 'utf8'))
const UI = seed.ui
const SHOTS = process.env.SHOTS ?? `${here}../../out/boards-ui/`
mkdirSync(SHOTS, { recursive: true })
const who = (n) => seed.accounts.find((a) => a.name === n)
const alice = who('alice')
const carol = who('carol')
const stamp = Date.now().toString(36)
const checks = []
const check = (ok, what) => {
  checks.push({ ok: !!ok, what })
  console.log(`${ok ? 'ok  ' : 'FAIL'} ${what}`)
}

function png(w, h, rgb) {
  const chunk = (type, data) => {
    const len = Buffer.alloc(4)
    len.writeUInt32BE(data.length)
    const td = Buffer.concat([Buffer.from(type), data])
    const crc = Buffer.alloc(4)
    crc.writeUInt32BE(crc32(td))
    return Buffer.concat([len, td, crc])
  }
  const ihdr = Buffer.alloc(13)
  ihdr.writeUInt32BE(w, 0)
  ihdr.writeUInt32BE(h, 4)
  ihdr[8] = 8
  ihdr[9] = 2
  const rows = []
  for (let y = 0; y < h; y++) {
    rows.push(Buffer.from([0]))
    for (let x = 0; x < w; x++) rows.push(Buffer.from((x + y) % 2 ? rgb : [255, 255, 255]))
  }
  return Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', ihdr), chunk('IDAT', deflateSync(Buffer.concat(rows))), chunk('IEND', Buffer.alloc(0))])
}

async function signInOAuth(page, a) {
  await page.goto(UI)
  await page.fill('input[name=handle]', a.handle)
  await page.click('button:has-text("Continue")')
  await page.waitForURL(/\/oauth\/authorize/)
  for (let i = 0; i < 4 && !page.url().startsWith(UI); i++) {
    await page.waitForLoadState()
    if (await page.locator('button[value=allow]').count()) {
      await page.screenshot({ path: `${SHOTS}consent.png` })
      await page.click('button[value=allow]')
    } else if (await page.locator('input[name=password]').count()) {
      const pw = page.locator('input[name=password]')
      if (!(await pw.isVisible())) await page.locator('details summary').first().click()
      if (await page.locator('input[name=identifier]').isVisible()) await page.fill('input[name=identifier]', a.handle)
      await pw.fill(a.password)
      await page.locator('button[name=action][value=sign-in]:visible').first().click()
    } else if (await page.locator('button:has-text("Continue")').count()) {
      await page.click('button:has-text("Continue")')
    }
    await page.waitForURL((u) => u.toString().startsWith(UI) || /consent|authorize/.test(u.toString()), { timeout: 10_000 }).catch(() => {})
  }
  await page.waitForURL((u) => u.toString().startsWith(UI))
}

async function signInPassword(page, a) {
  await page.goto(UI)
  await page.fill('input[name=handle]', a.handle)
  await page.click('button:has-text("Continue")')
  await page.fill('input[name=password]', a.password)
  await page.click('button:has-text("Sign in")')
}

const boardHash = `#/b/${encodeURIComponent(seed.board)}`

async function main() {
  const browser = await chromium.launch({ headless: !process.env.HEADED })
  const A = await (await browser.newContext({ viewport: { width: 1180, height: 860 } })).newPage()
  const Cr = await (await browser.newContext({ viewport: { width: 1180, height: 860 }, colorScheme: 'dark' })).newPage()
  try {
    await signInOAuth(A, alice)
    await A.waitForSelector(`text=@${alice.handle}`)
    check(await A.locator('.top .who').innerText().then((t) => t.includes('vlpds') && t.includes('oauth')), 'alice signed in with OAuth on vlpds')
    await A.waitForSelector('text=rustaceans')
    await A.screenshot({ path: `${SHOTS}home.png` })

    await A.goto(`${UI}/${boardHash}`)
    await A.waitForSelector('[data-testid=post]')
    await A.waitForSelector('img.thumb')
    await A.screenshot({ path: `${SHOTS}board.png`, fullPage: true })

    const title = `posted from the UI ${stamp}`
    await A.click('button:has-text("New post")')
    await A.fill('form input[name=title]', title)
    await A.fill('form textarea[name=body]', 'An image, through uploadBlob and a space record.')
    await A.selectOption('select[name=flair]', 'show-and-tell')
    await A.setInputFiles('input[name=image]', { name: 'crab.png', mimeType: 'image/png', buffer: png(12, 12, [192, 74, 46]) })
    await A.click('button:has-text("Post")')
    await A.waitForSelector(`text=${title}`)
    check(true, 'alice posts with an image')

    await signInPassword(Cr, carol)
    await Cr.waitForSelector(`text=@${carol.handle}`)
    check(await Cr.locator('.top .who').innerText().then((t) => t.includes('ref-a') && t.includes('password')), 'carol signed in with a password on ref-a')
    await Cr.goto(`${UI}/${boardHash}`)
    await Cr.click('button:has-text("new")')
    await Cr.waitForSelector(`text=${title}`, { timeout: 15_000 })
    check(true, "carol (ref-a) sees alice's new post")
    await Cr.click(`a:has-text("${title}")`)
    await Cr.waitForSelector('textarea[name=comment]')
    const cText = `carol was here ${stamp}`
    await Cr.fill('textarea[name=comment]', cText)
    await Cr.click('button.primary:has-text("Comment")')
    await Cr.waitForSelector(`text=${cText}`)
    await Cr.locator('[data-testid=post] button[aria-label=upvote]').first().click()
    await Cr.waitForFunction(() => document.querySelector('[data-testid=post] [data-testid=score]')?.textContent === '1')
    check(true, 'carol comments and upvotes')

    await A.goto(`${UI}/#/p/${encodeURIComponent(await Cr.evaluate(() => decodeURIComponent(location.hash.slice(4))))}`)
    await A.waitForSelector(`text=${cText}`, { timeout: 15_000 })
    await A.waitForFunction(() => document.querySelector('[data-testid=post] [data-testid=score]')?.textContent === '1', null, { timeout: 15_000 })
    check(true, "alice sees carol's comment and vote")
    const comment = A.locator('[data-testid=comment]').filter({ hasText: cText }).first()
    await comment.locator('button:has-text("reply")').click()
    const rText = `thanks carol ${stamp}`
    await comment.locator('textarea[name=reply]').fill(rText)
    await comment.locator('button.primary:has-text("Reply")').click()
    await A.waitForSelector(`text=${rText}`)
    await comment.locator('button[aria-label=upvote]').first().click()
    await A.screenshot({ path: `${SHOTS}post.png`, fullPage: true })

    await Cr.waitForSelector(`text=${rText}`, { timeout: 15_000 })
    check(true, "carol sees alice's reply after notify and sync")
    await Cr.waitForFunction((t) => [...document.querySelectorAll('[data-testid=comment]')].some((c) => c.textContent.includes(t) && c.querySelector('[data-testid=score]')?.textContent === '1'), cText, { timeout: 15_000 })
    check(true, "carol sees alice's upvote on her comment")
    await Cr.screenshot({ path: `${SHOTS}post-dark.png`, fullPage: true })

    await A.goto(`${UI}/${boardHash}`)
    await A.click('button:has-text("Spaces debug")')
    await A.waitForSelector('table.repos tbody tr')
    const repos = await A.locator('table.repos tbody tr').count()
    check(repos >= 3, `the debug drawer lists the synced member repos (${repos})`)
    await A.screenshot({ path: `${SHOTS}debug.png` })
    await A.click('button:has-text("Close")')
    check(await A.locator('text=Members').count(), 'the owner sees the member list')

    await A.click('a:has-text("Your karma here")')
    await A.waitForSelector('[data-testid=karma]')
    await A.screenshot({ path: `${SHOTS}user.png` })

    await A.setViewportSize({ width: 390, height: 844 })
    await A.goto(`${UI}/${boardHash}`)
    await A.waitForSelector('[data-testid=post]')
    const overflow = await A.evaluate(() => document.documentElement.scrollWidth - window.innerWidth)
    check(overflow <= 0, `no horizontal scroll at 390 px (${overflow})`)
    await A.screenshot({ path: `${SHOTS}board-phone.png`, fullPage: true })
  } catch (e) {
    check(false, `error: ${e.message}`)
    await A.screenshot({ path: `${SHOTS}error-alice.png` }).catch(() => {})
    await Cr.screenshot({ path: `${SHOTS}error-carol.png` }).catch(() => {})
  } finally {
    await browser.close()
  }
  const bad = checks.filter((c) => !c.ok)
  console.log(`\n${checks.length - bad.length}/${checks.length} ok; screenshots in ${SHOTS}`)
  process.exit(bad.length ? 1 : 0)
}

main()
