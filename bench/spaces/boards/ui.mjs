// `just spaces-boards-ui`: vlpds (--spaces --dev-mode) on the harness stack,
// the boards appview with the web UI and its sign-in layer on
// http://127.0.0.1:2888, and a seeded demo board. Runs until Ctrl-C.
//
// Seeded accounts get generated passwords, written to boards/.local/ (git
// ignored) and printed as handles only.
import { mkdirSync, writeFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { Actor } from '../lib/actor.mjs'
import { URLS, log } from '../lib/env.mjs'
import { NotifyService } from '../lib/notifysvc.mjs'
import { Vlpds } from '../lib/vlpds.mjs'
import { Appview } from './appview.mjs'
import { Bff } from './bff.mjs'
import { BoardsClient, SCOPES } from './client.mjs'
import { png } from './scenarios.mjs'

const here = fileURLToPath(new URL('.', import.meta.url))
const PORT = Number(process.env.BOARDS_PORT ?? 2888)
const PUBLIC = `http://127.0.0.1:${PORT}`

async function seed(appview) {
  const people = [
    ['alice', 'vlpds', SCOPES.owner],
    ['bob', 'vlpds', SCOPES.owner],
    ['carol', 'ref-a', null],
    ['dave', 'ref-b', null],
  ]
  const acct = {}
  for (const [name, host, scope] of people) acct[name] = await Actor.create(host, name, { scope: scope ?? SCOPES.member })
  const c = Object.fromEntries(Object.entries(acct).map(([k, a]) => [k, new BoardsClient(a)]))
  const board = await c.alice.createBoard(`rustaceans${Date.now().toString(36)}`, {
    name: 'rustaceans',
    description: 'Crabs, lifetimes and the borrow checker. Members only.',
    flairs: ['question', 'show-and-tell', 'news'],
  })
  await c.alice.addMember(board, acct.bob.did)
  await c.alice.addMember(board, acct.carol.did)
  await c.alice.addMember(board, acct.dave.did, { write: false })
  await c.alice.addMember(board, appview.account.did, { write: false })
  await appview.indexBoard(board)
  const p1 = await c.bob.post(board, { title: 'Ferris, rendered in one pixel', body: 'Took me all weekend.', flair: 'show-and-tell', image: { bytes: png('ferris'), mimeType: 'image/png', alt: 'a very small crab' } })
  const p2 = await c.carol.post(board, { title: 'Why does this need a lifetime?', body: "fn first(a: &str, b: &str) -> &str won't compile and I don't see why.", flair: 'question' })
  await c.alice.post(board, { title: 'Welcome to rustaceans', body: 'Be kind. Posts here are private to members: they live in your own space repo, not on the firehose.' })
  const c1 = await c.alice.comment(board, p2.uri, 'The compiler can\'t tell which input the output borrows from. Add <\'a> to both.')
  await c.carol.comment(board, p2.uri, 'Oh. That makes sense now, thanks.', { parent: c1.uri })
  await c.carol.comment(board, p1.uri, 'Majestic.')
  await c.alice.vote(board, p1.uri, 'up')
  await c.carol.vote(board, p1.uri, 'up')
  await c.bob.vote(board, p2.uri, 'up')
  await c.bob.vote(board, c1.uri, 'up')
  await c.alice.pin(board, (await c.alice.post(board, { title: 'Board rules', body: '1. No crypto. 2. Show your code.' })).uri)
  const accounts = Object.entries(acct).map(([name, a]) => ({ name, handle: a.handle, host: a.host, did: a.did, password: a.password }))
  return { board, accounts }
}

let vlpds
async function main() {
  vlpds = await new Vlpds({ memory: !!process.env.MEMORY }).start()
  const svc = await new NotifyService(2871).start()
  const bot = await Actor.create('vlpds', 'appview', { scope: SCOPES.reader })
  const appview = new Appview({ port: PORT, account: bot, svc, pollMs: 2000, webDir: `${here}web/dist` })
  const bff = new Bff({ appview, publicUrl: PUBLIC, vlpdsUrl: URLS.vlpds })
  appview.extraRoutes = bff.routes()
  await appview.start()
  const { board, accounts } = await seed(appview)
  mkdirSync(`${here}.local`, { recursive: true })
  const file = `${here}.local/seed-accounts.json`
  writeFileSync(file, JSON.stringify({ board, ui: PUBLIC, accounts }, null, 2))
  log(`boards UI: ${PUBLIC}`)
  log(`seeded board ${board}`)
  for (const a of accounts) log(`  @${a.handle} (${a.host})`)
  log(`passwords: ${file}`)
  log(`vlpds console: ${URLS.vlpds}/admin`)
  const stop = async () => {
    log('stopping')
    await appview.stop()
    await svc.stop()
    await vlpds.stop()
    process.exit(0)
  }
  process.on('SIGINT', stop)
  process.on('SIGTERM', stop)
}

main().catch(async (e) => {
  console.error(e)
  await vlpds?.stop()
  process.exit(2)
})
