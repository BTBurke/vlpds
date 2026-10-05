import { useEffect, useRef, useState, type ReactNode } from 'react'
import { CopyText, CopyValue, Empty, ErrorNotice, Field, JsonView, Loading, Notice, PageHead, Panel, PartialNotice, partialOf, Spinner, Status, type PartialResult } from '../../components/ui'
import { fmtNum, fmtTime, relTime, short } from '../../lib/format'
import { useAction, useLoad } from '../../lib/hooks'
import { parseProm, sum, type Scrape } from '../../lib/prom'
import { Link, navigate, useSearch } from '../../lib/router'
import { admin, call } from '../../lib/xrpc'
import { ModerateButton, type AuditEntry } from './Moderation'

// The console's Spaces pages. Metadata only: who's in a space, revs, counts
// and times. A record's value is only ever shown after an audited read with a
// reason (vlpds.admin.getSpaceRecord / listSpaceRecords).

type Rev = { rev: string; at?: string } | null
export type SpaceRow = {
  uri: string
  authority: string
  handle?: string
  spaceType: string
  skey: string
  readPolicy: string
  writePolicy: string
  appAccess: string
  createdAt: string
  deletedAt?: string
  takendown: boolean
  members: number
  writers: number
  repos: number
  records: number
  lastSpaceRev?: string
  lastActivityUs: number
}
type Totals = { spaces: number; deletedSpaces: number; takendown: number; members: number; writers: number; spaceRepos: number; records: number; foreignSpaces: number }
type ListOut = PartialResult & { totals: Totals; count: number; spaces: SpaceRow[]; cursor?: string; truncated?: boolean; countCap: number }
type Writer = { did: string; handle?: string | null; repoRev: Rev; spaceRev: Rev; hash: string; local: boolean; records?: number; headRev?: Rev; takendownRecords?: number }
type Registration = { service: string; host?: string | null; expiresAt?: string; expired: boolean }
type SpaceInfo = {
  space: Omit<SpaceRow, 'members' | 'writers' | 'repos' | 'records' | 'lastSpaceRev' | 'lastActivityUs'> & { deletedAt?: string | null; handle?: string | null }
  members: { did: string; handle?: string | null; read: boolean; write: boolean }[]
  moreMembers: boolean
  writers: Writer[]
  moreWriters: boolean
  activity: { spaceRev: string; at?: string; writer: string }[]
  registrations: Registration[]
  takendownRecords: { uri: string; did: string }[]
}
type NodeStatus = {
  node?: string | null
  outbox: { rows: number; max: number }
  fanout: { pending: number; queueMax: number }
  revocations: { entries: number; softCap: number; hardCap: number; loaded: boolean; fresh: boolean; refreshEverySecs: number; staleAfterSecs: number }
  credentialCache: { entries: number; max: number }
}

export const spaceUrl = (uri: string) => `/admin/spaces/space?uri=${encodeURIComponent(uri)}`
const lookupUrl = (q: string) => `/admin/moderation?q=${encodeURIComponent(q)}`
const accountUrl = (did: string) => `/admin/accounts/${encodeURIComponent(did)}`

/** at://{authority}/space/{type}/{skey}[/{author}/{collection}/{rkey}] */
function parseSpaceUri(s: string) {
  const m = /^at:\/\/([^/]+)\/space\/([^/]+)\/([^/]+)(?:\/([^/]+)\/([^/]+)\/([^/]+))?\/?$/.exec(s.trim())
  if (!m) return null
  return { space: `at://${m[1]}/space/${m[2]}/${m[3]}`, authority: m[1], record: m[4] ? { author: m[4], collection: m[5], rkey: m[6] } : undefined }
}

const spaceLabel = (s: { spaceType: string; skey: string }) => `${s.spaceType} / ${s.skey}`

/** Whether this server runs Spaces (describeServer's `vlpds.spaces`). */
export function useSpacesOn() {
  return useLoad<boolean>(async () => {
    const d = await call('com.atproto.server.describeServer')
    return !!d?.vlpds?.spaces
  }, [])
}

function SpacesOff() {
  return (
    <Notice kind="info">
      <p>
        <b>Spaces is off on this server.</b> Start it with <span className="mono">--spaces</span> (the Ansible role's <span className="mono">vlpds_spaces: true</span>) to host spaces and space repos. It's an alpha that
        changes upstream every week, so leave it off unless you're testing against it.
      </p>
    </Notice>
  )
}

function Count({ n, cap }: { n: number; cap?: number }) {
  return <>{cap !== undefined && n > cap ? `${fmtNum(cap)}+` : fmtNum(n)}</>
}

function When({ at }: { at?: string | number | null }) {
  if (!at) return <span className="muted">—</span>
  const ms = typeof at === 'number' ? at : Date.parse(at)
  return <span title={fmtTime(ms)}>{relTime(ms)}</span>
}

function SpaceState({ s }: { s: { takendown: boolean; deletedAt?: string | null } }) {
  if (s.takendown) return <Status kind="bad">Taken down</Status>
  if (s.deletedAt) return <Status kind="idle">Deleted</Status>
  return <Status kind="ok">Live</Status>
}

function Who({ did, handle }: { did: string; handle?: string | null }) {
  return handle ? (
    <span className="spc-who">
      <Link to={accountUrl(did)}>@{handle}</Link> <CopyValue text={did} display={short(did, 10)} title={did} label={`Copy DID ${did}`} className="muted small" />
    </span>
  ) : (
    <CopyValue text={did} label={`Copy DID ${did}`} className="small" />
  )
}

// ---------------------------------------------------------------- overview

const SORTS = [
  { id: 'activity', label: 'Recent activity' },
  { id: 'members', label: 'Members' },
  { id: 'writers', label: 'Writers' },
  { id: 'records', label: 'Records' },
  { id: 'created', label: 'Newest' },
] as const
const PAGE = 50

export function Spaces() {
  const on = useSpacesOn()
  return (
    <>
      <div className="console-head">
        <h1>Spaces</h1>
        {on.data && <OpenSpace />}
      </div>
      <ErrorNotice error={on.error} />
      {on.data === undefined ? !on.error && <Loading /> : !on.data ? <SpacesOff /> : <SpacesOverview />}
    </>
  )
}

function OpenSpace() {
  const [v, setV] = useState('')
  const [bad, setBad] = useState(false)
  return (
    <form
      className="toolbar spc-open"
      onSubmit={(e) => {
        e.preventDefault()
        const p = parseSpaceUri(v)
        if (!p) return setBad(true)
        // a record goes to the lookup, whose record card has the audited "Read record…"
        navigate(p.record ? lookupUrl(v.trim()) : spaceUrl(p.space))
      }}
    >
      <input
        type="search"
        value={v}
        onChange={(e) => {
          setV(e.target.value)
          setBad(false)
        }}
        placeholder="at://… space or space record URI"
        aria-label="Open a space or space record by URI"
        aria-invalid={bad || undefined}
        spellCheck={false}
        autoCapitalize="none"
      />
      <button className="btn">Open</button>
    </form>
  )
}

function SpacesOverview() {
  const [sort, setSort] = useState<(typeof SORTS)[number]['id']>('activity')
  const [cursor, setCursor] = useState<string>()
  const l = useLoad<ListOut>(() => admin('vlpds.admin.listSpaces', { params: { sort, limit: PAGE, cursor } }), [sort, cursor], 30_000)
  const d = l.data
  const offset = Number(cursor ?? 0)
  return (
    <>
      <ErrorNotice error={l.error} />
      {d && <PartialNotice partial={partialOf(d)} />}
      {d?.truncated && <Notice kind="warn">More spaces than one page of the scan holds: totals and the list cover the first 2,000 spaces per node.</Notice>}
      <div className="tiles">
        <Tile k="Spaces hosted here" v={d ? fmtNum(d.totals.spaces) : '—'} sub={d && d.totals.takendown ? `${fmtNum(d.totals.takendown)} taken down` : undefined} />
        <Tile k="Space repos stored here" v={d ? fmtNum(d.totals.spaceRepos) : '—'} sub={d && d.totals.foreignSpaces ? `in ${fmtNum(d.totals.foreignSpaces)} spaces hosted elsewhere` : undefined} />
        <Tile k="Members" v={d ? fmtNum(d.totals.members) : '—'} />
        <Tile k="Writers" v={d ? fmtNum(d.totals.writers) : '—'} />
        <Tile k="Space records stored here" v={d ? fmtNum(d.totals.records) : '—'} />
      </div>
      <Health />
      <Panel
        flush
        title="Spaces hosted here"
        desc="Spaces whose authority is an account on this PDS. Records only counts space repos stored here, since a writer on another PDS keeps its own."
        actions={
          <>
            <div className="seg" role="group" aria-label="Sort by">
              {SORTS.map((s) => (
                <button
                  key={s.id}
                  type="button"
                  aria-pressed={sort === s.id}
                  onClick={() => {
                    setSort(s.id)
                    setCursor(undefined)
                  }}
                >
                  {s.label}
                </button>
              ))}
            </div>
            <Link className="btn sm" to="/admin/moderation?tab=audit&scope=spaces">
              Audit log
            </Link>
          </>
        }
      >
        {!d ? (
          !l.error && <Loading />
        ) : d.spaces.length === 0 ? (
          <Empty title="No spaces yet">A space shows up here once an account on this PDS creates one.</Empty>
        ) : (
          <div className="table-wrap">
            <table className="data compact">
              <thead>
                <tr>
                  <th>Space</th>
                  <th>Authority</th>
                  <th>Read / write</th>
                  <th className="num">Members</th>
                  <th className="num">Writers</th>
                  <th className="num">Records</th>
                  <th>Last write</th>
                  <th>Created</th>
                  <th>State</th>
                </tr>
              </thead>
              <tbody>
                {d.spaces.map((s) => (
                  <tr key={s.uri} className="link" onClick={() => navigate(spaceUrl(s.uri))}>
                    <td className="spc-name">
                      <Link to={spaceUrl(s.uri)} onClick={(e) => e.stopPropagation()} title={s.uri}>
                        <span className="muted">{s.spaceType} /</span> <b>{s.skey}</b>
                      </Link>
                    </td>
                    <td>{s.handle ? <span title={s.authority}>@{s.handle}</span> : <span className="mono small">{short(s.authority, 10)}</span>}</td>
                    <td className="small">
                      {s.readPolicy} / {s.writePolicy}
                    </td>
                    <td className="num">
                      <Count n={s.members} cap={d.countCap} />
                    </td>
                    <td className="num">
                      <Count n={s.writers} cap={d.countCap} />
                    </td>
                    <td className="num">{fmtNum(s.records)}</td>
                    <td>
                      <When at={s.lastActivityUs ? s.lastActivityUs / 1000 : null} />
                    </td>
                    <td>
                      <When at={s.createdAt} />
                    </td>
                    <td>
                      <SpaceState s={s} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
      {d && (d.cursor || offset > 0) && (
        <div className="row end">
          <span className="small muted">
            {offset + 1}–{offset + d.spaces.length} of {fmtNum(d.count)}
          </span>
          <button className="btn" disabled={offset === 0 || l.loading} onClick={() => setCursor(offset - PAGE > 0 ? String(offset - PAGE) : undefined)}>
            Previous
          </button>
          <button className="btn" disabled={!d.cursor || l.loading} onClick={() => setCursor(d.cursor)}>
            {l.loading && <Spinner />}
            Next
          </button>
        </div>
      )}
    </>
  )
}

function Tile({ k, v, sub }: { k: string; v: ReactNode; sub?: ReactNode }) {
  return (
    <div className="tile">
      <div className="v">{v}</div>
      <div className="k">{k}</div>
      {sub && <div className="k small">{sub}</div>}
    </div>
  )
}

// ---------------------------------------------------------------- health

type Kind = 'ok' | 'warn' | 'bad' | 'idle'
type Card = { k: string; v: ReactNode; kind: Kind; note: ReactNode }
const SCRAPE_MS = 5000

const notify = (s: Scrape, hop: string, f: (r: string) => boolean) => sum(s, 'vlpds_space_notify_total', (l) => l.hop === hop && f(l.result)) ?? 0
const perSec = (a: number, b: number, ms: number) => (ms > 0 ? Math.max(0, b - a) / (ms / 1000) : 0)
const pct = (n: number) => `${(n * 100).toFixed(n >= 0.995 || n < 0.1 ? 0 : 1)}%`

/** Health of this node's Spaces, from /metrics and getSpacesStatus, with the VlpdsSpace* alert thresholds. */
function Health() {
  const [cur, setCur] = useState<Scrape>()
  const [prev, setPrev] = useState<Scrape>()
  const [error, setError] = useState<unknown>()
  const last = useRef<Scrape | undefined>(undefined)
  const st = useLoad<NodeStatus>(() => admin('vlpds.admin.getSpacesStatus'), [], SCRAPE_MS)
  useEffect(() => {
    let live = true
    const tick = async () => {
      try {
        const r = await fetch('/metrics')
        if (!r.ok) throw new Error(`/metrics returned ${r.status}`)
        const s = parseProm(await r.text())
        if (!live) return
        setError(undefined)
        setPrev(last.current)
        setCur(s)
        last.current = s
      } catch (e) {
        if (live) setError(e)
      }
    }
    tick()
    const id = setInterval(tick, SCRAPE_MS)
    return () => {
      live = false
      clearInterval(id)
    }
  }, [])
  const cards = cur ? healthCards(cur, prev, st.data) : []
  const node = st.data?.node
  return (
    <Panel
      title="Health"
      desc={
        <>
          This node{node ? <> (<span className="mono">{node}</span>)</> : ''}, live from <span className="mono">/metrics</span> every {SCRAPE_MS / 1000} s. Rates are over the last scrape, and the alerts in{' '}
          <span className="mono">ops/alerts.yml</span> look over 10 to 30 min. Other nodes: the <span className="mono">vlpds internals</span> dashboard's Spaces row.
        </>
      }
    >
      <ErrorNotice error={error || st.error} />
      {!cur ? (
        !error && <Loading />
      ) : (
        <div className="spc-health">
          {cards.map((c) => (
            <div key={c.k} className={`spc-card ${c.kind}`}>
              <div className="row between">
                <span className="k">{c.k}</span>
                <Status kind={c.kind}>{c.kind === 'ok' ? 'OK' : c.kind === 'warn' ? 'Watch' : c.kind === 'bad' ? 'Alert' : 'Idle'}</Status>
              </div>
              <div className="v">{c.v}</div>
              <div className="small muted">{c.note}</div>
            </div>
          ))}
        </div>
      )}
    </Panel>
  )
}

function healthCards(s: Scrape, p: Scrape | undefined, st?: NodeStatus): Card[] {
  const ms = p ? s.t - p.t : 0
  const g = (n: string, f?: (l: Record<string, string>) => boolean) => sum(s, n, f) ?? 0
  const gp = (n: string, f?: (l: Record<string, string>) => boolean) => (p ? (sum(p, n, f) ?? 0) : g(n, f))
  const rateOf = (n: string, f?: (l: Record<string, string>) => boolean) => perSec(gp(n, f), g(n, f), ms)
  const out: Card[] = []

  const rows = g('vlpds_space_outbox_rows')
  const oldest = g('vlpds_space_outbox_oldest_seconds')
  out.push({
    k: 'notifyWrite outbox',
    v: (
      <>
        {fmtNum(rows)} <small>rows</small>
      </>
    ),
    kind: oldest > 3600 ? 'bad' : oldest > 300 ? 'warn' : rows === 0 ? 'idle' : 'ok',
    note: rows ? `Oldest ${fmtAge(oldest)}. Alert past 1 h (VlpdsSpaceOutboxBacklog).` : 'Nothing owed to an authority.',
  })

  const fanoutAll = (r: Scrape) => notify(r, 'fanout', () => true)
  const fanoutBad = (r: Scrape) => notify(r, 'fanout', (x) => x !== 'ok')
  const fRate = p ? perSec(fanoutAll(p), fanoutAll(s), ms) : 0
  const fBad = p ? perSec(fanoutBad(p), fanoutBad(s), ms) : 0
  const dropped = g('vlpds_space_fanout_dropped_total')
  const dRate = rateOf('vlpds_space_fanout_dropped_total')
  const depth = g('vlpds_space_fanout_queue_depth')
  out.push({
    k: 'Fan-out to syncers',
    v: (
      <>
        {fmtNum(depth)} <small>queued</small>
        {st && (
          <>
            {' '}
            · {fmtNum(st.fanout.pending)} <small>in lanes</small>
          </>
        )}
      </>
    ),
    kind: fRate > 0 && fBad / fRate > 0.5 && fBad > 0.1 ? 'bad' : dRate > 0 || fBad > 0 ? 'warn' : fRate === 0 && depth === 0 ? 'idle' : 'ok',
    note: (
      <>
        {fRate > 0 ? `${pct(fBad / fRate)} of ${fRate.toFixed(2)}/s failing. ` : ''}
        {fmtNum(dropped)} dropped since start{dRate > 0 ? ` (${dRate.toFixed(2)}/s now)` : ''}. Alert past 50% failing (VlpdsSpaceNotifyFanoutFailing).
      </>
    ),
  })

  const outFail = notify(s, 'out', (r) => r !== 'ok')
  const inFail = notify(s, 'in', (r) => !['ok', 'noop'].includes(r) && !r.startsWith('same_rev'))
  const failRate = p ? perSec(notify(p, 'out', (r) => r !== 'ok') + notify(p, 'in', (r) => !['ok', 'noop'].includes(r) && !r.startsWith('same_rev')), outFail + inFail, ms) : 0
  out.push({
    k: 'Notify failures',
    v: (
      <>
        {fmtNum(outFail)} <small>out</small> · {fmtNum(inFail)} <small>in</small>
      </>
    ),
    kind: failRate > 0 ? 'warn' : 'ok',
    note: failRate > 0 ? `${failRate.toFixed(2)}/s now. Out retries until its authority answers.` : 'Since start. Healthy is about 0 both ways.',
  })

  const capped = notify(s, 'in', (r) => r === 'same_rev_capped')
  const unverified = notify(s, 'in', (r) => r === 'same_rev_unverified')
  out.push({
    k: 'Same-rev notifies',
    v: (
      <>
        {fmtNum(capped)} <small>capped</small> · {fmtNum(unverified)} <small>unverified</small>
      </>
    ),
    kind: capped + unverified > 0 ? 'warn' : 'ok',
    note: 'Only a record takedown at a writer’s host sends one. Healthy is 0.',
  })

  const rv = st?.revocations
  const fill = rv ? rv.entries / rv.hardCap : 0
  out.push({
    k: 'Revocations',
    v: rv ? (
      <>
        {fmtNum(rv.entries)} <small>of {fmtNum(rv.hardCap)}</small>
      </>
    ) : (
      fmtNum(g('vlpds_space_revocations'))
    ),
    kind: !rv ? 'idle' : !rv.loaded || !rv.fresh ? 'bad' : fill >= 0.9 ? 'bad' : fill >= 0.5 ? 'warn' : 'ok',
    note: !rv
      ? 'Loading.'
      : !rv.loaded
        ? 'Not read yet: credential reads wait for it.'
        : !rv.fresh
          ? `Not re-read for over ${fmtAge(rv.staleAfterSecs)}: check the bucket.`
          : `${pct(fill)} full, re-read every ${fmtAge(rv.refreshEverySecs)}. Spaces nobody here has a stake in share ${fmtNum(rv.softCap)}.`,
  })

  const hit = g('vlpds_space_credential_cache_total', (l) => l.result === 'hit')
  const miss = g('vlpds_space_credential_cache_total', (l) => l.result === 'miss')
  out.push({
    k: 'Credential cache',
    v: hit + miss ? pct(hit / (hit + miss)) : '—',
    kind: hit + miss === 0 ? 'idle' : hit / (hit + miss) < 0.5 && hit + miss > 100 ? 'warn' : 'ok',
    note: `Hit rate since start, ${fmtNum(hit + miss)} checks${st ? `, ${fmtNum(st.credentialCache.entries)} cached` : ''}. High under steady polling.`,
  })

  const all = (r: Scrape) => sum(r, 'vlpds_space_credential_checks_total') ?? 0
  const rejected = (r: Scrape) => sum(r, 'vlpds_space_credential_checks_total', (l) => l.result !== 'ok' && l.result !== 'expired') ?? 0
  const cRate = p ? perSec(all(p), all(s), ms) : 0
  const rRate = p ? perSec(rejected(p), rejected(s), ms) : 0
  out.push({
    k: 'Credential rejects',
    v: fmtNum(rejected(s)),
    kind: cRate > 0 && rRate / cRate > 0.25 && rRate > 0.5 ? 'bad' : rRate > 0 ? 'warn' : 'ok',
    note: 'Refused credential reads since start, expired ones left out. Alert past 25% (VlpdsSpaceCredentialRejectsHigh).',
  })

  const mismatch = g('vlpds_space_digest_mismatch_total')
  out.push({
    k: 'Digest mismatches',
    v: fmtNum(mismatch),
    kind: mismatch > 0 ? 'bad' : 'ok',
    note: 'Counted by check-space. Anything above 0 is an integrity bug (VlpdsSpaceDigestMismatch).',
  })

  const throttled = g('vlpds_object_store_throttled_total')
  const tRate = rateOf('vlpds_object_store_throttled_total')
  out.push({
    k: 'Object-store throttles',
    v: fmtNum(throttled),
    kind: tRate > 0 ? 'warn' : 'ok',
    note: '429s and SlowDowns since start, all of the node. The odd lease 429 on R2 is normal.',
  })
  return out
}

function fmtAge(s: number) {
  if (s < 90) return `${Math.round(s)} s`
  if (s < 5400) return `${Math.round(s / 60)} min`
  return `${(s / 3600).toFixed(1)} h`
}

// ---------------------------------------------------------------- one space

export function SpaceDetail() {
  const uri = useSearch().get('uri') ?? ''
  const on = useSpacesOn()
  const p = parseSpaceUri(uri)
  const crumbs = [{ to: '/admin/spaces', label: 'Spaces' }]
  if (on.data === false)
    return (
      <>
        <PageHead title="Space" crumbs={crumbs} />
        <SpacesOff />
      </>
    )
  if (!p || p.record)
    return (
      <>
        <PageHead title="Space" crumbs={crumbs} />
        <Notice kind="warn">
          <span className="mono break">{uri || '(no uri)'}</span> isn't a space URI (at://authority/space/type/key).
        </Notice>
      </>
    )
  return <SpaceInfoPage uri={p.space} authority={p.authority} />
}

function SpaceInfoPage({ uri, authority }: { uri: string; authority: string }) {
  const l = useLoad<SpaceInfo>(() => admin('vlpds.admin.getSpaceInfo', { params: { did: authority, uri } }), [uri])
  const d = l.data
  const s = d?.space
  const [browse, setBrowse] = useState<string>()
  const audit = useLoad<{ entries: AuditEntry[] }>(() => admin('vlpds.admin.getAuditLog', { params: { space: uri, limit: 50 } }), [uri])
  const reload = () => {
    l.reload()
    audit.reload()
  }
  return (
    <>
      <PageHead title={s ? spaceLabel(s) : uri} crumbs={[{ to: '/admin/spaces', label: 'Spaces' }]} />
      <ErrorNotice error={l.error} />
      {!d || !s ? (
        !l.error && <Loading />
      ) : (
        <>
          {s.takendown && (
            <Notice kind="err">
              <b>Taken down.</b> No one gets a credential to read it, syncers can't list its writers or register for its notifications, and members' notifies are dropped.
            </Notice>
          )}
          {s.deletedAt && <Notice kind="warn">Deleted by its owner {fmtTime(s.deletedAt)}. The row stays so credential requests get SpaceDeleted.</Notice>}
          <div className="mod-grid">
            <div className="stack">
              <Panel title="Space" actions={<ModerateButton subject={{ kind: 'space', did: s.authority, uri: s.uri }} applied={s.takendown} onDone={reload} />}>
                <dl className="dl">
                  <dt>URI</dt>
                  <dd>
                    <CopyText text={s.uri} display={<span className="break">{s.uri}</span>} />
                  </dd>
                  <dt>Type</dt>
                  <dd className="mono small">{s.spaceType}</dd>
                  <dt>Key</dt>
                  <dd className="mono small">{s.skey}</dd>
                  <dt>Authority</dt>
                  <dd>
                    <Who did={s.authority} handle={s.handle} />
                  </dd>
                  <dt>Read policy</dt>
                  <dd>{s.readPolicy}</dd>
                  <dt>Write policy</dt>
                  <dd>{s.writePolicy}</dd>
                  <dt>App access</dt>
                  <dd>{s.appAccess}</dd>
                  <dt>Created</dt>
                  <dd>{fmtTime(s.createdAt)}</dd>
                  <dt>State</dt>
                  <dd>
                    <SpaceState s={s} />
                  </dd>
                </dl>
              </Panel>
              <Panel flush title={`Writers (${d.writers.length}${d.moreWriters ? '+' : ''})`} desc="As their repo hosts last reported them. Records and taken-down counts show for writers stored here.">
                {d.writers.length === 0 ? (
                  <Empty title="No writes yet" />
                ) : (
                  <div className="table-wrap">
                    <table className="data compact">
                      <thead>
                        <tr>
                          <th>Writer</th>
                          <th className="num">Records</th>
                          <th>Last write</th>
                          <th>repoRev</th>
                          <th>spaceRev</th>
                          <th>Hash</th>
                          <th />
                        </tr>
                      </thead>
                      <tbody>
                        {d.writers.map((w) => (
                          <tr key={w.did}>
                            <td>
                              <Who did={w.did} handle={w.handle} />
                            </td>
                            <td className="num">
                              {w.local ? fmtNum(w.records) : <span className="muted" title="Stored on another PDS">—</span>}
                              {!!w.takendownRecords && <span className="pill danger spc-pill">{w.takendownRecords} down</span>}
                            </td>
                            <td>
                              <When at={w.repoRev?.at} />
                            </td>
                            <td className="mono small">{w.repoRev?.rev ?? '—'}</td>
                            <td className="mono small">{w.spaceRev?.rev ?? '—'}</td>
                            <td className="mono small" title="First 8 bytes of the set hash">
                              {w.hash}
                            </td>
                            <td>
                              {w.local && (
                                <button type="button" className="btn sm" onClick={() => setBrowse(w.did)}>
                                  Records…
                                </button>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </Panel>
              {browse && <RecordBrowser key={browse} space={s.uri} repo={browse} handle={d.writers.find((w) => w.did === browse)?.handle} onClose={() => setBrowse(undefined)} onDone={reload} />}
              <Panel flush title={`Members (${d.members.length}${d.moreMembers ? '+' : ''})`} desc="The member list this space's policy reads. Writers on a managing-app or public policy needn't be on it.">
                {d.members.length === 0 ? (
                  <Empty title="No members" />
                ) : (
                  <div className="table-wrap">
                    <table className="data compact">
                      <tbody>
                        {d.members.map((m) => (
                          <tr key={m.did}>
                            <td>
                              <Who did={m.did} handle={m.handle} />
                            </td>
                            <td>
                              {m.read && <span className="pill">read</span>} {m.write && <span className="pill accent">write</span>}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </Panel>
            </div>
            <div className="stack">
              <Panel flush title="Recent activity" desc="The newest spaceRevs, one per writer: when each writer was last sequenced here.">
                {d.activity.length === 0 ? (
                  <Empty title="Nothing sequenced yet" />
                ) : (
                  <div className="table-wrap">
                    <table className="data compact">
                      <tbody>
                        {d.activity.map((a) => (
                          <tr key={a.spaceRev}>
                            <td>
                              <When at={a.at} />
                            </td>
                            <td>
                              <CopyValue text={a.writer} display={short(a.writer, 10)} title={a.writer} className="small" />
                            </td>
                            <td className="mono small">{a.spaceRev}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </Panel>
              <Registrations space={s.uri} authority={s.authority} regs={d.registrations} onDone={reload} />
              <Panel flush title="Taken-down records" desc="Records of this space taken down on this PDS. Their authors still hold them.">
                {d.takendownRecords.length === 0 ? (
                  <Empty title="None" />
                ) : (
                  <div className="table-wrap">
                    <table className="data compact">
                      <tbody>
                        {d.takendownRecords.map((r) => (
                          <tr key={r.uri}>
                            <td className="spc-uri">
                              <Link to={lookupUrl(r.uri)} title={r.uri} className="mono small">
                                {r.uri.slice(s.uri.length + 1)}
                              </Link>
                            </td>
                            <td>
                              <ModerateButton subject={{ kind: 'record', did: r.did, uri: r.uri }} applied onDone={reload} />
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </Panel>
              <Panel flush title="Audit log" desc="Takedowns, restores, record reads and registration removals for this space." actions={<button className="btn sm" onClick={audit.reload}>Refresh</button>}>
                <ErrorNotice error={audit.error} />
                {!audit.data ? <Loading /> : audit.data.entries.length === 0 ? <Empty title="Nothing yet" /> : <SpaceAudit space={s.uri} entries={audit.data.entries} />}
              </Panel>
            </div>
          </div>
        </>
      )}
    </>
  )
}

/** One line per entry: what it touched within the space, by whom, and why. */
function SpaceAudit({ space, entries }: { space: string; entries: AuditEntry[] }) {
  const what = (e: AuditEntry) => {
    const sub = e.subject
    if (!sub) return '—'
    if (sub.kind === 'space') return e.detail?.service ? `registration ${e.detail.service}` : 'the space'
    if (sub.kind === 'spaceRepo') return `repo of ${sub.did}`
    return sub.uri?.startsWith(`${space}/`) ? sub.uri.slice(space.length + 1) : (sub.uri ?? sub.did)
  }
  return (
    <div className="table-wrap">
      <table className="data compact">
        <tbody>
          {entries.map((e) => (
            <tr key={e.id}>
              <td>
                <When at={e.at} />
              </td>
              <td>
                <span className={`pill${e.action === 'takedown' || e.action.endsWith('.remove') ? ' danger' : ''}`}>{e.detail?.method ?? e.action}</span>
              </td>
              <td className="spc-uri">
                <span className="mono small spc-trunc" title={what(e)}>
                  {what(e)}
                </span>
              </td>
              <td className="small" title={e.ip}>
                {e.actor}
              </td>
              <td className="small spc-reason">
                <span className="spc-trunc" title={e.reason}>
                  {e.reason ?? '—'}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function Registrations({ space, authority, regs, onDone }: { space: string; authority: string; regs: Registration[]; onDone: () => void }) {
  const [removing, setRemoving] = useState<string>()
  return (
    <Panel flush title="Notify registrations" desc="Services told about each write. The endpoint's host only; registrations last a day unless renewed.">
      {regs.length === 0 ? (
        <Empty title="None" />
      ) : (
        <div className="table-wrap">
          <table className="data compact">
            <tbody>
              {regs.map((r) => (
                <tr key={r.service}>
                  <td>
                    <CopyValue text={r.service} display={short(r.service, 14)} title={r.service} className="small" />
                  </td>
                  <td className="mono small">{r.host ?? '—'}</td>
                  <td>{r.expired ? <Status kind="idle">Expired</Status> : <span title={fmtTime(r.expiresAt)}>until {relTime(Date.parse(r.expiresAt!)).replace(/^in /, '')}</span>}</td>
                  <td>
                    <button type="button" className="btn sm danger" onClick={() => setRemoving(r.service)}>
                      Remove…
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {removing && (
        <ReasonDialog
          title="Remove this registration?"
          what={removing}
          note="The service stops getting this space's notifies at once. It can register again with a credential for the space, so take the space down to keep it out."
          action="Remove"
          danger
          onClose={() => setRemoving(undefined)}
          onConfirm={async (reason) => {
            await admin('vlpds.admin.removeSpaceRegistration', { body: { did: authority, space, service: removing, reason } })
            setRemoving(undefined)
            onDone()
          }}
        />
      )}
    </Panel>
  )
}

function ReasonDialog({
  title,
  what,
  note,
  action,
  danger,
  onClose,
  onConfirm,
}: {
  title: string
  what: string
  note: ReactNode
  action: string
  danger?: boolean
  onClose: () => void
  onConfirm: (reason: string) => Promise<void>
}) {
  const ref = useRef<HTMLDialogElement>(null)
  const [reason, setReason] = useState('')
  const act = useAction(onConfirm)
  useEffect(() => {
    ref.current?.showModal()
  }, [])
  return (
    <dialog ref={ref} className="modal mod-dialog" onClose={onClose} aria-labelledby="reason-title">
      <form
        className="inner"
        onSubmit={(e) => {
          e.preventDefault()
          if (reason.trim()) act.run(reason.trim())
        }}
      >
        <h2 id="reason-title">{title}</h2>
        <p className="mono small break">{what}</p>
        <p className="muted small">{note}</p>
        <ErrorNotice error={act.error} />
        <Field label="Reason (required, kept in the audit log)">
          <textarea value={reason} onChange={(e) => setReason(e.target.value)} rows={3} required autoFocus maxLength={2000} />
        </Field>
        <div className="row end">
          <button type="button" className="btn" onClick={() => ref.current?.close()}>
            Cancel
          </button>
          <button type="submit" className={`btn ${danger ? 'danger solid' : 'primary'}`} disabled={!reason.trim() || act.busy}>
            {act.busy && <Spinner />}
            {action}
          </button>
        </div>
      </form>
    </dialog>
  )
}

type SpaceRecord = { uri: string; cid: string; value: unknown; takendown: boolean }

/** A writer's records in the space: an audited read (listSpaceRecords), one audit entry per page. */
function RecordBrowser({ space, repo, handle, onClose, onDone }: { space: string; repo: string; handle?: string | null; onClose: () => void; onDone: () => void }) {
  const [reason, setReason] = useState('')
  const [asked, setAsked] = useState(false)
  const [rows, setRows] = useState<SpaceRecord[]>([])
  const [cursor, setCursor] = useState<string>()
  const [open, setOpen] = useState<Set<string>>(new Set())
  const page = useAction(async (cur?: string) => {
    const r: { records: SpaceRecord[]; cursor?: string } = await admin('vlpds.admin.listSpaceRecords', { params: { space, repo, reason: reason.trim(), cursor: cur, limit: 50 } })
    setRows((x) => (cur ? [...x, ...r.records] : r.records))
    setCursor(r.cursor)
    setAsked(true)
  })
  const toggle = (u: string) =>
    setOpen((s) => {
      const n = new Set(s)
      if (n.has(u)) n.delete(u)
      else n.add(u)
      return n
    })
  return (
    <Panel
      title={`Records of ${handle ? `@${handle}` : short(repo, 10)}`}
      desc="Space records are private to the space's members. Listing them reads their values, so each page is written to the audit log with your reason."
      actions={
        <button type="button" className="btn sm" onClick={onClose}>
          Close
        </button>
      }
      flush={asked}
    >
      {!asked ? (
        <form
          onSubmit={(e) => {
            e.preventDefault()
            if (reason.trim()) page.run(undefined)
          }}
        >
          <ErrorNotice error={page.error} />
          <Field label="Reason (required, kept in the audit log)">
            <textarea value={reason} onChange={(e) => setReason(e.target.value)} rows={2} required autoFocus maxLength={2000} />
          </Field>
          <div className="row end">
            <button className="btn primary" disabled={!reason.trim() || page.busy}>
              {page.busy && <Spinner />}
              Read records
            </button>
          </div>
        </form>
      ) : rows.length === 0 ? (
        <Empty title="No records" />
      ) : (
        <>
          <div className="table-wrap">
            <table className="data compact">
              <tbody>
                {rows.map((r) => {
                  const path = r.uri.slice(`${space}/${repo}/`.length)
                  return (
                    <RecordRow key={r.uri} path={path} r={r} open={open.has(r.uri)} onToggle={() => toggle(r.uri)}>
                      <ModerateButton
                        subject={{ kind: 'record', did: repo, uri: r.uri }}
                        applied={r.takendown}
                        onDone={() => {
                          setRows((x) => x.map((y) => (y.uri === r.uri ? { ...y, takendown: !y.takendown } : y)))
                          onDone()
                        }}
                      />
                    </RecordRow>
                  )
                })}
              </tbody>
            </table>
          </div>
          <ErrorNotice error={page.error} />
          {cursor && (
            <div className="row end spc-more">
              <button className="btn sm" disabled={page.busy} onClick={() => page.run(cursor)}>
                {page.busy && <Spinner />}
                Next page (another audited read)
              </button>
            </div>
          )}
        </>
      )}
    </Panel>
  )
}

function RecordRow({ path, r, open, onToggle, children }: { path: string; r: SpaceRecord; open: boolean; onToggle: () => void; children: ReactNode }) {
  return (
    <>
      <tr>
        <td className="spc-uri">
          <Link to={lookupUrl(r.uri)} title={r.uri} className="mono small">
            {path}
          </Link>
        </td>
        <td>{r.takendown ? <Status kind="bad">Taken down</Status> : <Status kind="ok">Visible</Status>}</td>
        <td className="row end">
          <button type="button" className="btn sm" onClick={onToggle} aria-expanded={open}>
            {open ? 'Hide' : 'Show'}
          </button>
          {children}
        </td>
      </tr>
      {open && (
        <tr>
          <td colSpan={3} className="spc-json">
            <JsonView value={r.value} />
          </td>
        </tr>
      )}
    </>
  )
}

// ---------------------------------------------------------------- an account's spaces

type AccountSpacesOut = {
  repos: { space: string; authority: string; records: number; rev: Rev; createdAt?: string | null; takendownRecords: number }[]
  governs: { uri: string; createdAt: string; deletedAt?: string | null; takendown: boolean }[]
  more: boolean
}

const label = (uri: string) => {
  const p = uri.replace(/^at:\/\//, '').split('/')
  return `${p[2]} / ${p[3]}`
}

/** The account page's Spaces panel: spaces it writes in and spaces it governs. Metadata only. */
export function AccountSpaces({ did }: { did: string }) {
  const on = useSpacesOn()
  const l = useLoad<AccountSpacesOut | null>(() => (on.data ? admin('vlpds.admin.getAccountSpaces', { params: { did } }) : Promise.resolve(null)), [did, on.data])
  if (!on.data) return null
  const d = l.data
  return (
    <Panel flush title="Spaces" desc="Spaces this account writes in and spaces it governs. Counts and revs only; its records are read from the space's page, with a reason.">
      <ErrorNotice error={l.error} />
      {!d ? (
        !l.error && <Loading />
      ) : d.repos.length + d.governs.length === 0 ? (
        <Empty title="No spaces" />
      ) : (
        <div className="table-wrap">
          <table className="data compact">
            <thead>
              <tr>
                <th>Space</th>
                <th>Role</th>
                <th className="num">Records</th>
                <th>Last write</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {d.governs.map((g) => (
                <tr key={`g${g.uri}`} className="link" onClick={() => navigate(spaceUrl(g.uri))}>
                  <td className="spc-name">
                    <Link to={spaceUrl(g.uri)} title={g.uri} onClick={(e) => e.stopPropagation()}>
                      {label(g.uri)}
                    </Link>
                  </td>
                  <td>
                    <span className="pill accent">authority</span>
                  </td>
                  <td className="num muted">—</td>
                  <td className="muted">—</td>
                  <td>
                    <SpaceState s={g} />
                  </td>
                </tr>
              ))}
              {d.repos.map((r) => (
                <tr key={`r${r.space}`} className="link" onClick={() => navigate(spaceUrl(r.space))}>
                  <td className="spc-name">
                    <Link to={spaceUrl(r.space)} title={r.space} onClick={(e) => e.stopPropagation()}>
                      {label(r.space)}
                    </Link>
                  </td>
                  <td>
                    <span className="pill">writer</span>
                  </td>
                  <td className="num">
                    {fmtNum(r.records)}
                    {r.takendownRecords > 0 && <span className="pill danger spc-pill">{r.takendownRecords} down</span>}
                  </td>
                  <td>
                    <When at={r.rev?.at} />
                  </td>
                  <td className="mono small muted">{r.rev?.rev}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {d?.more && <p className="small muted spc-more">Showing the first 1,000.</p>}
    </Panel>
  )
}
