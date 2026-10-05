import { useEffect, useMemo, useRef, useState, type KeyboardEvent, type ReactNode } from 'react'
import { Chart, type Series } from '../../components/Chart'
import { ErrorNotice, Loading, Notice, Panel, PartialNotice, partialOf, Spinner, Status } from '../../components/ui'
import { fmtNum, fmtSi, fmtTime, relTime } from '../../lib/format'
import { useAction, useLoad } from '../../lib/hooks'
import { admin } from '../../lib/xrpc'
import { useLive } from './Cluster'

// ---------------------------------------------------------------- server shapes (src/xrpc/ratelimits.rs)

type KeyKind = 'ip' | 'identifier-ip' | 'did' | 'node' | 'cluster'

type Limiter = {
  name: string
  key: KeyKind
  scope: string
  windowSecs: number
  points: number
  enabled: boolean
  custom: boolean
  default: { windowSecs: number; points: number } | null
}

type ConfigError = { version: number | null; message: string; atMs: number }

type NodeRow = {
  node: string
  self: boolean
  reachable: boolean
  enabledByFlag?: boolean
  configVersion?: number
  configError?: ConfigError | null
  loadedAtMs?: number | null
  checkedAtMs?: number | null
  liveWindows?: number
}

type Consumer = { key: string; used: number; maxNodeUsed: number; limit: number | null; resetMs: number; nodes: string[] }
type Rejection = { limiter: string; route: string; last1m: number; last5m: number; last15m: number; total: number }

type LimiterCfg = { enabled?: boolean; points?: number; windowSecs?: number }
type RouteCfg = { nsid: string; points: number; windowSecs: number; enabled?: boolean }
type OverrideCfg = { ip?: string; did?: string; limiters?: string[]; exempt?: boolean; points?: number; note?: string }
type Audit = { version: number; at: string; by: string; ip?: string; node: string; note?: string; changes: string[] }

type Doc = {
  version: number
  enabled?: boolean
  limiters?: Record<string, LimiterCfg>
  routes?: RouteCfg[]
  overrides?: OverrideCfg[]
  updatedAt?: string
  updatedBy?: string
  note?: string
  history?: Audit[]
}

type RateLimits = {
  node: string
  enabledByFlag: boolean
  enabled: boolean
  configVersion: number
  config: Doc | null
  configError: ConfigError | null
  refreshSecs: number
  limiters: Limiter[]
  nodes: NodeRow[]
  top: Record<string, Consumer[]>
  rejections: Rejection[]
  unreachableNodes?: string[]
  time: number
}

type Loaded = RateLimits & { fetchedAt: number }

const POLL = 5000
const KEEP = 72 // 6 minutes at 5 s
const TOP = 10
const SLOTS = ['c1', 'c2', 'c3', 'c4', 'c5']

// ---------------------------------------------------------------- helpers

function fmtWindow(s: number): string {
  if (s % 86400 === 0) return s === 86400 ? '1 day' : `${s / 86400} days`
  if (s % 3600 === 0) return `${s / 3600} h`
  if (s % 60 === 0) return `${s / 60} min`
  return `${s} s`
}

const KEY_LABEL: Record<KeyKind, string> = { ip: 'client IP (IPv6: /64)', 'identifier-ip': 'identifier + IP', did: 'DID', node: 'whole node', cluster: 'whole cluster (counted in the bucket)' }
const KEY_SHORT: Record<KeyKind, string> = { ip: 'IP', 'identifier-ip': 'ID+IP', did: 'DID', node: 'node', cluster: 'cluster' }

/** Bucket names without the com.atproto. prefix every method bucket carries. */
const shortName = (n: string) => n.replace(/^(route:)?com\.atproto\./, '$1')

/** What a bucket covers beyond the method its name already says ("" when nothing). */
function scopeExtra(l: Limiter): string {
  const base = shortName(l.name).replace(/-\d+$/, '')
  if (l.scope === base) return ''
  if (l.scope.startsWith(`${base}; `)) return `also ${l.scope.slice(base.length + 2)}`
  return l.scope
}

/** The editable part of the config: every built-in bucket's values (defaults filled in), routes and overrides. */
type Draft = {
  enabled: boolean
  limiters: Record<string, { points: number; windowSecs: number; enabled: boolean }>
  routes: RouteCfg[]
  overrides: OverrideCfg[]
}

function draftOf(d: RateLimits): Draft {
  const doc = d.config
  const limiters: Draft['limiters'] = {}
  for (const l of d.limiters) {
    if (l.custom || !l.default) continue
    const c = doc?.limiters?.[l.name] ?? {}
    limiters[l.name] = { points: c.points ?? l.default.points, windowSecs: c.windowSecs ?? l.default.windowSecs, enabled: c.enabled ?? true }
  }
  return {
    enabled: doc?.enabled ?? true,
    limiters,
    routes: (doc?.routes ?? []).map((r) => ({ ...r, enabled: r.enabled ?? true })),
    overrides: doc?.overrides ?? [],
  }
}

/** The config object to save: only differences from the defaults. */
function docOf(draft: Draft, d: RateLimits): Omit<Doc, 'version'> {
  const limiters: Record<string, LimiterCfg> = {}
  for (const l of d.limiters) {
    if (l.custom || !l.default) continue
    const v = draft.limiters[l.name]
    if (!v) continue
    const c: LimiterCfg = {}
    if (v.points !== l.default.points) c.points = v.points
    if (v.windowSecs !== l.default.windowSecs) c.windowSecs = v.windowSecs
    if (!v.enabled) c.enabled = false
    if (Object.keys(c).length) limiters[l.name] = c
  }
  return {
    enabled: draft.enabled,
    limiters,
    routes: draft.routes.map((r) => (r.enabled === false ? r : { nsid: r.nsid, points: r.points, windowSecs: r.windowSecs })),
    overrides: draft.overrides,
  }
}

const same = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b)

/** The stored version an edit is made against (an object a node rejected still counts). */
function baseVersion(d: RateLimits): number {
  let v = d.config?.version ?? 0
  for (const n of d.nodes) if (n.configError?.version != null) v = Math.max(v, n.configError.version)
  return v
}

const routeDirty = (r: RouteCfg, origin: Draft) => {
  const o = origin.routes.find((x) => x.nsid === r.nsid)
  return !o || !same(o, r)
}

/** Edited rows: the enforce switch, each bucket, each added/removed/changed method bucket and override. */
function countChanges(a: Draft, o: Draft): number {
  let n = a.enabled !== o.enabled ? 1 : 0
  for (const k of Object.keys(a.limiters)) if (!same(a.limiters[k], o.limiters[k])) n++
  for (const r of a.routes) if (routeDirty(r, o)) n++
  for (const r of o.routes) if (!a.routes.some((x) => x.nsid === r.nsid)) n++
  const ao = a.overrides.map((x) => JSON.stringify(x))
  const oo = o.overrides.map((x) => JSON.stringify(x))
  n += ao.filter((x) => !oo.includes(x)).length + oo.filter((x) => !ao.includes(x)).length
  return n
}

/** The key closest to its limit (the limit applies to what one node counted). */
function busiest(rows?: Consumer[]): Consumer | undefined {
  let best: Consumer | undefined
  let bf = -1
  for (const r of rows ?? []) {
    const f = r.limit ? r.maxNodeUsed / r.limit : -0.5
    if (f > bf) {
      best = r
      bf = f
    }
  }
  return best
}

const level = (frac: number) => (frac >= 1 ? 'over' : frac >= 0.8 ? 'near' : '')

// ---------------------------------------------------------------- page

export function RateLimits() {
  const c = useLoad<Loaded>(async () => ({ ...(await admin('vlpds.admin.getRateLimits', { params: { top: TOP } })), fetchedAt: Date.now() }), [], POLL)
  const d = c.data
  const live = useLive(d?.fetchedAt) && !c.error
  const history = useRejectionHistory(d)

  if (!d)
    return (
      <>
        <ErrorNotice error={c.error} />
        {!c.error && <Loading />}
      </>
    )
  return <Page d={d} live={live} error={c.error} history={history} reload={c.reload} />
}

function Page({ d, live, error, history, reload }: { d: Loaded; live: boolean; error: unknown; history: History; reload: () => void }) {
  const ed = useEditor(d, reload)
  const [sel, setSel] = useState<string>()

  const rej = useMemo(() => {
    const m = new Map<string, { m1: number; m15: number }>()
    for (const r of d.rejections) {
      const x = m.get(r.limiter) ?? { m1: 0, m15: 0 }
      x.m1 += r.last1m
      x.m15 += r.last15m
      m.set(r.limiter, x)
    }
    return m
  }, [d])

  // default selection: the bucket with the most recent 429s, else the first one counting anything
  const fallback = useMemo(() => {
    let best: string | undefined
    let n = 0
    for (const [k, v] of rej) if (v.m15 > n) [best, n] = [k, v.m15]
    return best ?? d.limiters.find((l) => d.top[l.name]?.length)?.name ?? d.limiters[0]?.name
  }, [rej, d])
  const selected = sel ?? fallback

  const reachable = d.nodes.filter((n) => n.reachable)
  const onVersion = reachable.filter((n) => n.configVersion === d.configVersion).length
  const versions = [...new Set(reachable.map((n) => n.configVersion))]
  const errored = d.nodes.filter((n) => n.configError)
  const flagOff = d.nodes.filter((n) => n.reachable && n.enabledByFlag === false)
  const last1m = d.rejections.reduce((s, r) => s + r.last1m, 0)
  const last15m = d.rejections.reduce((s, r) => s + r.last15m, 0)
  const overrides = d.config?.overrides?.length ?? 0
  const { result, changed } = ed

  return (
    <div className={ed.changed ? 'rl-page editing' : 'rl-page'}>
      <div className="console-head">
        <h1>Rate limits</h1>
        <span className={`live${live ? '' : ' stale'}`} aria-live="polite">
          <i aria-hidden="true" />
          {live ? `Live, every ${POLL / 1000} s, from every node` : 'Not updating'}
        </span>
      </div>
      <ErrorNotice error={error} />
      <PartialNotice partial={partialOf(d)} />
      {errored.map((n) => (
        <Notice kind="err" key={n.node}>
          <p>
            <b className="mono">{n.node}</b> rejected config{n.configError!.version != null && <> version {n.configError!.version}</>} {relTime(n.configError!.atMs)} and
            keeps running version {n.configVersion}: <span className="mono small">{n.configError!.message}</span>
          </p>
        </Notice>
      ))}
      {versions.length > 1 && (
        <Notice kind="warn">
          Nodes disagree on the config version ({reachable.map((n) => `${n.node}: v${n.configVersion}`).join(', ')}). Peers are nudged on every change and re-read the
          object every {d.refreshSecs} s.
        </Notice>
      )}
      {flagOff.length > 0 && (
        <Notice kind="warn">
          {flagOff.map((n) => n.node).join(', ')} {flagOff.length === 1 ? 'runs' : 'run'} with <span className="mono">--no-rate-limits</span>: the config is kept there but
          nothing is counted or limited.
        </Notice>
      )}
      {result && !changed && result.version >= ed.base && (
        <Notice kind={result.nodes.every((n) => n.ok && n.configVersion === result.version) ? 'ok' : 'warn'}>
          Saved version {result.version}.{' '}
          {result.nodes.map((n) => (
            <span key={n.node} className="nowrap">
              <span className="mono">{n.node}</span> {n.ok ? `runs v${n.configVersion}` : `not reached (${n.error ?? 'error'}); it re-reads within ${d.refreshSecs} s`}.{' '}
            </span>
          ))}
        </Notice>
      )}

      <div className="tiles rl-tiles">
        <div className="tile" title={d.config?.updatedAt ? `Saved ${fmtTime(d.config.updatedAt)} by ${d.config.updatedBy}` : 'No config object: built-in defaults'}>
          <div className="v">{d.configVersion ? `v${d.configVersion}` : 'Defaults'}</div>
          <div className="k">Config in force</div>
        </div>
        <div className="tile">
          <div className="v">
            {onVersion}
            <small>/ {d.nodes.length}</small>
          </div>
          <div className="k">Nodes on this version</div>
        </div>
        <div className="tile">
          <div className="v">{d.enabled ? <Status kind="ok">Enforced</Status> : <Status kind="warn">Off</Status>}</div>
          <div className="k">Rate limiting</div>
        </div>
        <div className="tile">
          <div className={`v${last1m ? ' hot' : ''}`}>{fmtNum(last1m)}</div>
          <div className="k">429s this minute</div>
        </div>
        <div className="tile">
          <div className="v">{fmtNum(last15m)}</div>
          <div className="k">429s, last 15 min</div>
        </div>
        <div className="tile">
          <div className="v">{overrides}</div>
          <div className="k">Overrides</div>
        </div>
      </div>

      <div className="rl-grid">
        <div className="rl-main">
          <BucketsPanel d={d} ed={ed} rej={rej} history={history} selected={selected} onSelect={setSel} />
          <OverridesPanel d={d} ed={ed} />
        </div>
        <aside className="rl-side">
          <RatePanel history={history} />
          <TopKeysPanel d={d} bucket={selected} onSelect={setSel} />
          <RecentRejections d={d} selected={selected} onSelect={setSel} />
          <NodesPanel d={d} />
          <HistoryPanel d={d} />
        </aside>
      </div>
      <SaveBar ed={ed} />
    </div>
  )
}

// ---------------------------------------------------------------- live 429 rates (from the change in each bucket's total between polls)

type Sample = { t: number; totals: Map<string, number> }
type History = { series: Series[]; data: (number | null)[][]; spark: Map<string, number[]>; samples: number; any: boolean }

function useRejectionHistory(d?: Loaded): History {
  const [samples, setSamples] = useState<Sample[]>([])
  const last = useRef<number | undefined>(undefined)
  useEffect(() => {
    if (!d || last.current === d.fetchedAt) return
    last.current = d.fetchedAt
    const totals = new Map<string, number>()
    for (const r of d.rejections) totals.set(r.limiter, (totals.get(r.limiter) ?? 0) + r.total)
    setSamples((xs) => [...xs, { t: d.fetchedAt / 1000, totals }].slice(-(KEEP + 1)))
  }, [d])
  return useMemo(() => {
    const rates: { t: number; by: Map<string, number> }[] = []
    for (let i = 1; i < samples.length; i++) {
      const a = samples[i - 1]
      const b = samples[i]
      const dt = b.t - a.t
      const by = new Map<string, number>()
      for (const [k, v] of b.totals) by.set(k, dt > 0 ? Math.max(0, v - (a.totals.get(k) ?? 0)) / dt : 0)
      rates.push({ t: b.t, by })
    }
    const tot = new Map<string, number>()
    for (const r of rates) for (const [k, v] of r.by) tot.set(k, (tot.get(k) ?? 0) + v)
    const spark = new Map<string, number[]>()
    for (const k of tot.keys()) spark.set(k, rates.map((r) => r.by.get(k) ?? 0))
    const top = [...tot.entries()]
      .filter(([, v]) => v > 0)
      .sort((a, b) => b[1] - a[1])
      .slice(0, SLOTS.length)
      .map(([k]) => k)
      .sort()
    const series: Series[] = top.map((k, i) => ({ label: shortName(k), color: SLOTS[i] }))
    const data: (number | null)[][] = [rates.map((r) => r.t), ...top.map((k) => rates.map((r) => r.by.get(k) ?? 0))]
    return { series, data, spark, samples: rates.length, any: top.length > 0 }
  }, [samples])
}

function Spark({ values }: { values?: number[] }) {
  const W = 64
  const H = 18
  const v = values ?? []
  const max = Math.max(0, ...v)
  if (v.length < 2 || max === 0)
    return (
      <svg className="rl-spark" width={W} height={H} aria-hidden="true">
        <line x1={0} x2={W} y1={H - 1.5} y2={H - 1.5} className="base" />
      </svg>
    )
  const step = W / (v.length - 1)
  const pts = v.map((x, i) => `${(i * step).toFixed(1)},${(H - 1.5 - (x / max) * (H - 4)).toFixed(1)}`).join(' ')
  return (
    <svg className="rl-spark hot" width={W} height={H} role="img" aria-label={`Up to ${fmtSi(max)} 429s per second`}>
      <title>{`Peak ${fmtSi(max)}/s over the last ${Math.round((v.length * POLL) / 60000)} min`}</title>
      <polygon points={`0,${H} ${pts} ${W},${H}`} className="area" />
      <polyline points={pts} className="line" />
    </svg>
  )
}

// ---------------------------------------------------------------- editing state

const ACTOR_KEY = 'vlpds.admin.actor'

type SaveResult = { version: number; nodes: { node: string; ok: boolean; configVersion?: number; error?: string }[] }
type Editor = ReturnType<typeof useEditor>

function useEditor(d: RateLimits, onSaved: () => void) {
  const server = useMemo(() => draftOf(d), [d])
  const base = baseVersion(d)
  // what an edit started from: the server's draft and version at the time
  const [origin, setOrigin] = useState({ draft: server, version: base })
  const [draft, setDraft] = useState<Draft>(server)
  const changed = !same(draft, origin.draft)
  // follow the server while nothing is edited
  useEffect(() => {
    if (!changed) {
      setOrigin({ draft: server, version: base })
      setDraft(server)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [server, base])
  const movedOn = changed && origin.version !== base

  const [actor, setActor] = useState(() => {
    try {
      return sessionStorage.getItem(ACTOR_KEY) ?? ''
    } catch {
      return ''
    }
  })
  const [note, setNote] = useState('')
  const [result, setResult] = useState<SaveResult>()
  const save = useAction(async () => {
    try {
      sessionStorage.setItem(ACTOR_KEY, actor)
    } catch {
      /* per-tab only */
    }
    const r = await admin('vlpds.admin.updateRateLimits', {
      body: { config: docOf(draft, d), ifVersion: origin.version, actor: actor.trim() || undefined, note: note.trim() || undefined },
    })
    setResult(r)
    setOrigin({ draft, version: r.version })
    setNote('')
    onSaved()
  })
  const discard = () => {
    setOrigin({ draft: server, version: base })
    setDraft(server)
    save.setError(undefined)
  }
  const set = (f: (x: Draft) => void) =>
    setDraft((x) => {
      const n: Draft = structuredClone(x)
      f(n)
      return n
    })
  return { draft, origin, base, changed, movedOn, set, discard, save, result, actor, setActor, note, setNote, count: changed ? Math.max(1, countChanges(draft, origin.draft)) : 0 }
}

const toInt = (s: string) => Math.max(0, Math.floor(Number(s)))

function LimitInputs({ name, points, windowSecs, onPoints, onWindow, dirty }: { name: string; points: number; windowSecs: number; onPoints: (n: number) => void; onWindow: (n: number) => void; dirty?: { p: boolean; w: boolean } }) {
  return (
    <span className="rl-limit">
      <input type="number" min={1} aria-label={`${name} points`} className={dirty?.p ? 'mod' : ''} value={points} onChange={(e) => onPoints(toInt(e.target.value))} />
      <span className="sep">/</span>
      <input type="number" min={1} max={604800} aria-label={`${name} window in seconds`} className={`win${dirty?.w ? ' mod' : ''}`} value={windowSecs} onChange={(e) => onWindow(toInt(e.target.value))} />
      <span className="unit">s</span>
      <span className="hw">{windowSecs >= 60 ? fmtWindow(windowSecs) : ''}</span>
    </span>
  )
}

// The whole (truncated) key is the click target: keys are long DIDs and IPs.
function CopyKey({ text, title }: { text: string; title: string }) {
  const [done, setDone] = useState(false)
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text)
      setDone(true)
      setTimeout(() => setDone(false), 1400)
    } catch {
      /* clipboard blocked */
    }
  }
  return (
    <button type="button" className="rl-copykey" onClick={copy} title={done ? 'Copied' : `${title}\nClick to copy`} aria-label={done ? 'Copied' : `Copy ${text}`}>
      {done ? 'Copied' : text}
    </button>
  )
}

function Busiest({ rows }: { rows?: Consumer[] }) {
  const r = busiest(rows)
  if (!r) return <span className="muted small">—</span>
  const frac = r.limit ? r.maxNodeUsed / r.limit : 0
  return (
    <span className="rl-busy" title={`${r.key}: ${r.limit == null ? `${fmtNum(r.used)} used, exempt` : `${fmtNum(r.maxNodeUsed)} of ${fmtNum(r.limit)} on the busiest node`} (${rows!.length} key${rows!.length === 1 ? '' : 's'} listed)`}>
      {r.limit == null ? (
        <span className="pill accent">exempt</span>
      ) : (
        <>
          <span className="rl-mini">
            <i className={level(frac)} style={{ width: `${Math.max(3, Math.min(1, frac) * 100)}%` }} />
          </span>
          <span className={`pct ${level(frac)}`}>{Math.round(frac * 100)}%</span>
        </>
      )}
      <span className="mono key">{r.key}</span>
      {rows!.length > 1 && <span className="more">+{rows!.length - 1}</span>}
    </span>
  )
}

const Count = ({ n }: { n?: number }) => (n ? <b className="rl-hot">{fmtNum(n)}</b> : <span className="rl-zero">0</span>)

// ---------------------------------------------------------------- buckets (config + live state)

function BucketsPanel({
  d,
  ed,
  rej,
  history,
  selected,
  onSelect,
}: {
  d: RateLimits
  ed: Editor
  rej: Map<string, { m1: number; m15: number }>
  history: History
  selected?: string
  onSelect: (n: string) => void
}) {
  const { draft, origin, set } = ed
  const [filter, setFilter] = useState('')
  const q = filter.trim().toLowerCase()
  const match = (...xs: string[]) => !q || xs.some((x) => x.toLowerCase().includes(q))
  const builtins = d.limiters.filter((l) => !l.custom && l.default && match(l.name, l.scope))
  const routes = draft.routes.map((r, i) => [r, i] as const).filter(([r]) => match(`route:${r.nsid}`))
  const enabledDirty = draft.enabled !== origin.draft.enabled
  const COLS = 9

  return (
    <Panel
      title="Buckets"
      id="rl-edit"
      desc="Each node counts on its own, except cluster buckets (one count in the bucket, windows aligned to the epoch: a day is the UTC day). A new points value keeps each key's live window; a new window length starts fresh windows. Click a bucket to see its top keys."
      flush
      actions={
        <>
          <input type="search" className="rl-filter" placeholder="Filter buckets" aria-label="Filter buckets" value={filter} onChange={(e) => setFilter(e.target.value)} />
          <label
            className={`rl-enforce${draft.enabled ? '' : ' off'}${enabledDirty ? ' dirty' : ''}`}
            title="Off: nothing is counted or limited on any node (the RateLimit headers go away too)."
          >
            <input type="checkbox" checked={draft.enabled} onChange={(e) => set((x) => void (x.enabled = e.target.checked))} />
            <span>Enforce rate limits</span>
          </label>
        </>
      }
    >
      {!draft.enabled && (
        <div className="rl-banner">Enforcement is off{enabledDirty ? ' in your edit' : ''}: nothing is counted or limited on any node, and the RateLimit headers go away.</div>
      )}
      <div className="table-wrap">
        <table className={`data compact rl-table${draft.enabled ? '' : ' disabled'}`}>
          <thead>
            <tr>
              <th>Bucket</th>
              <th title="What a bucket is keyed by">Key</th>
              <th>Limit (points / window)</th>
              <th title="The key closest to its limit in the current window, on the busiest node">Busiest key</th>
              <th className="num" title="429s this minute, cluster-wide">1m</th>
              <th className="num" title="429s in the last 15 minutes, cluster-wide">15m</th>
              <th className="rl-trend" title="429s per second since this page opened">429/s</th>
              <th>On</th>
              <th className="rl-act-h" />
            </tr>
          </thead>
          <tbody>
            {builtins.map((l) => {
              const v = draft.limiters[l.name]
              if (!v) return null
              const o = origin.draft.limiters[l.name]
              const def = l.default!
              const modified = v.points !== def.points || v.windowSecs !== def.windowSecs || !v.enabled
              const dirty = !same(v, o)
              return (
                <Row
                  key={l.name}
                  name={l.name}
                  extra={scopeExtra(l)}
                  title={`${l.name}\n${l.scope}\nDefault: ${def.points} per ${fmtWindow(def.windowSecs)}`}
                  keyKind={l.key}
                  enabled={v.enabled}
                  dirty={dirty}
                  selected={selected === l.name}
                  onSelect={onSelect}
                  limit={
                    <LimitInputs
                      name={l.name}
                      points={v.points}
                      windowSecs={v.windowSecs}
                      dirty={{ p: v.points !== o?.points, w: v.windowSecs !== o?.windowSecs }}
                      onPoints={(n) => set((x) => void (x.limiters[l.name].points = n))}
                      onWindow={(n) => set((x) => void (x.limiters[l.name].windowSecs = n))}
                    />
                  }
                  live={d.top[l.name]}
                  rej={rej.get(l.name)}
                  spark={history.spark.get(l.name)}
                  onToggle={(on) => set((x) => void (x.limiters[l.name].enabled = on))}
                  action={
                    modified && (
                      <button
                        type="button"
                        className="btn quiet sm rl-icon"
                        title={`Reset to the default: ${def.points} per ${fmtWindow(def.windowSecs)}, on`}
                        aria-label={`Reset ${l.name} to the default`}
                        onClick={() => set((x) => void (x.limiters[l.name] = { points: def.points, windowSecs: def.windowSecs, enabled: true }))}
                      >
                        ↺
                      </button>
                    )
                  }
                />
              )
            })}
            {builtins.length === 0 && routes.length === 0 && (
              <tr>
                <td colSpan={COLS} className="muted small">
                  No bucket matches “{filter}”.
                </td>
              </tr>
            )}
          </tbody>
          <tbody className="rl-routes">
            <tr className="rl-group">
              <td colSpan={COLS}>
                Per-method buckets <span className="muted">· keyed by client IP, counted on top of global-ip</span>
              </td>
            </tr>
            {routes.map(([r, i]) => {
              const name = `route:${r.nsid}`
              const o = origin.draft.routes.find((x) => x.nsid === r.nsid)
              return (
                <Row
                  key={name}
                  name={name}
                  extra={o ? '' : 'new'}
                  title={`${name}\nAdded bucket for ${r.nsid}`}
                  keyKind="ip"
                  enabled={r.enabled !== false}
                  dirty={routeDirty(r, origin.draft)}
                  selected={selected === name}
                  onSelect={onSelect}
                  limit={
                    <LimitInputs
                      name={r.nsid}
                      points={r.points}
                      windowSecs={r.windowSecs}
                      dirty={{ p: r.points !== o?.points, w: r.windowSecs !== o?.windowSecs }}
                      onPoints={(n) => set((x) => void (x.routes[i].points = n))}
                      onWindow={(n) => set((x) => void (x.routes[i].windowSecs = n))}
                    />
                  }
                  live={d.top[name]}
                  rej={rej.get(name)}
                  spark={history.spark.get(name)}
                  onToggle={(on) => set((x) => void (x.routes[i].enabled = on))}
                  action={
                    <button type="button" className="btn quiet sm rl-icon" title="Remove this bucket" aria-label={`Remove ${name}`} onClick={() => set((x) => void x.routes.splice(i, 1))}>
                      ✕
                    </button>
                  }
                />
              )
            })}
            <AddRoute cols={COLS} onAdd={(r) => set((x) => void x.routes.push(r))} taken={draft.routes.map((r) => r.nsid)} />
          </tbody>
        </table>
      </div>
    </Panel>
  )
}

function Row({
  name,
  extra,
  title,
  keyKind,
  enabled,
  dirty,
  selected,
  onSelect,
  limit,
  live,
  rej,
  spark,
  onToggle,
  action,
}: {
  name: string
  extra: string
  title: string
  keyKind: KeyKind
  enabled: boolean
  dirty: boolean
  selected: boolean
  onSelect: (n: string) => void
  limit: ReactNode
  live?: Consumer[]
  rej?: { m1: number; m15: number }
  spark?: number[]
  onToggle: (on: boolean) => void
  action: ReactNode
}) {
  const cls = [enabled ? '' : 'off', dirty ? 'dirty' : '', selected ? 'sel' : ''].filter(Boolean).join(' ')
  return (
    <tr className={cls} onClick={() => onSelect(name)} aria-selected={selected}>
      <td className="rl-name" title={title}>
        <div>
          <button type="button" className="mono" onClick={() => onSelect(name)} aria-pressed={selected}>
            {shortName(name)}
          </button>
          {extra && <span className="extra">{extra}</span>}
        </div>
      </td>
      <td className="small muted" title={KEY_LABEL[keyKind]}>
        {KEY_SHORT[keyKind]}
      </td>
      <td>{limit}</td>
      <td className="rl-live">
        <Busiest rows={live} />
      </td>
      <td className="num rl-live">
        <Count n={rej?.m1} />
      </td>
      <td className="num rl-live">
        <Count n={rej?.m15} />
      </td>
      <td className="rl-live rl-trend">
        <Spark values={spark} />
      </td>
      <td>
        <input type="checkbox" aria-label={`${name} enabled`} checked={enabled} onChange={(e) => onToggle(e.target.checked)} onClick={(e) => e.stopPropagation()} />
      </td>
      <td className="rl-act">{action}</td>
    </tr>
  )
}

function AddRoute({ cols, onAdd, taken }: { cols: number; onAdd: (r: RouteCfg) => void; taken: string[] }) {
  const [nsid, setNsid] = useState('')
  const [points, setPoints] = useState(300)
  const [win, setWin] = useState(300)
  const n = nsid.trim()
  const dup = taken.includes(n)
  const add = () => {
    if (!n || dup) return
    onAdd({ nsid: n, points, windowSecs: win, enabled: true })
    setNsid('')
  }
  return (
    <tr className="rl-addrow">
      <td className="rl-name">
        <input
          type="text"
          value={nsid}
          onChange={(e) => setNsid(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') {
              e.preventDefault()
              add()
            }
          }}
          placeholder="Add a method: app.bsky.feed.getTimeline"
          aria-label="Add a bucket for an XRPC method"
          aria-invalid={dup || undefined}
          spellCheck={false}
        />
      </td>
      <td className="small muted">IP</td>
      <td>
        <LimitInputs name="New bucket" points={points} windowSecs={win} onPoints={setPoints} onWindow={setWin} />
      </td>
      <td colSpan={cols - 3} className="rl-act">
        <button type="button" className="btn sm" disabled={!n || dup} onClick={add}>
          Add bucket
        </button>
        {dup && <span className="rl-err">That method already has one.</span>}
      </td>
    </tr>
  )
}

// ---------------------------------------------------------------- overrides

function OverridesPanel({ d, ed }: { d: RateLimits; ed: Editor }) {
  const { draft, origin, set } = ed
  const names = d.limiters.map((l) => l.name).concat(draft.routes.map((r) => `route:${r.nsid}`).filter((n) => !d.limiters.some((l) => l.name === n)))
  const orig = origin.draft.overrides.map((x) => JSON.stringify(x))
  const removed = origin.draft.overrides.filter((x) => !draft.overrides.some((y) => same(x, y))).length
  return (
    <Panel
      title="Overrides"
      desc="An IP or CIDR override applies to every request from a matching client IP; a DID override to the DID-keyed buckets (repo writes, handle and email flows, OAuth sign-in) of that account. Exempt beats a custom limit; between custom limits the larger wins."
      flush
    >
      <div className="table-wrap">
        <table className="data compact rl-table rl-over">
          <thead>
            <tr>
              <th>Match</th>
              <th>Buckets</th>
              <th>Action</th>
              <th>Note</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {draft.overrides.map((o, i) => (
              <tr key={i} className={orig.includes(JSON.stringify(o)) ? '' : 'dirty'}>
                <td className="mono small">
                  <span className="muted">{o.ip ? 'IP ' : 'DID '}</span>
                  {o.ip ?? o.did}
                </td>
                <td className="mono small">{o.limiters?.length ? o.limiters.map(shortName).join(', ') : <span className="muted">all</span>}</td>
                <td>{o.exempt ? <span className="pill accent">exempt</span> : <span className="pill">{fmtNum(o.points)} points</span>}</td>
                <td className="small rl-note" title={o.note}>
                  {o.note}
                </td>
                <td className="rl-act">
                  <button type="button" className="btn quiet sm rl-icon" title="Remove this override" aria-label={`Remove override for ${o.ip ?? o.did}`} onClick={() => set((x) => void x.overrides.splice(i, 1))}>
                    ✕
                  </button>
                </td>
              </tr>
            ))}
            {draft.overrides.length === 0 && (
              <tr>
                <td colSpan={5} className="small muted">
                  No overrides{removed ? ` (${removed} removed in your edit)` : ''}: every client gets the bucket limits.
                </td>
              </tr>
            )}
            {draft.overrides.length > 0 && removed > 0 && (
              <tr>
                <td colSpan={5} className="small muted">
                  {removed} removed in your edit.
                </td>
              </tr>
            )}
            <AddOverride names={names} onAdd={(o) => set((x) => void x.overrides.push(o))} />
          </tbody>
        </table>
      </div>
    </Panel>
  )
}

function AddOverride({ names, onAdd }: { names: string[]; onAdd: (o: OverrideCfg) => void }) {
  const [kind, setKind] = useState<'ip' | 'did'>('ip')
  const [who, setWho] = useState('')
  const [buckets, setBuckets] = useState('')
  const [action, setAction] = useState<'exempt' | 'points'>('exempt')
  const [points, setPoints] = useState(10000)
  const [note, setNote] = useState('')
  const list = buckets
    .split(/[\s,]+/)
    .map((s) => s.trim())
    .filter(Boolean)
  const unknown = list.filter((b) => !names.includes(b))
  const ok = who.trim() && !unknown.length
  const add = () => {
    if (!ok) return
    const o: OverrideCfg = kind === 'ip' ? { ip: who.trim() } : { did: who.trim() }
    if (list.length) o.limiters = list
    if (action === 'exempt') o.exempt = true
    else o.points = points
    if (note.trim()) o.note = note.trim()
    onAdd(o)
    setWho('')
    setNote('')
  }
  const enter = (e: KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      add()
    }
  }
  return (
    <tr className="rl-addrow">
      <td>
        <span className="rl-pair">
          <select aria-label="Match on" value={kind} onChange={(e) => setKind(e.target.value as 'ip' | 'did')}>
            <option value="ip">IP/CIDR</option>
            <option value="did">DID</option>
          </select>
          <input
            type="text"
            aria-label={kind === 'ip' ? 'IP or CIDR block' : 'DID'}
            value={who}
            onChange={(e) => setWho(e.target.value)}
            onKeyDown={enter}
            placeholder={kind === 'ip' ? '203.0.113.0/24' : 'did:plc:…'}
            spellCheck={false}
          />
        </span>
      </td>
      <td>
        <input
          type="text"
          list="rl-bucket-names"
          aria-label="Buckets (blank: all)"
          aria-invalid={unknown.length > 0 || undefined}
          title={unknown.length ? `Unknown: ${unknown.join(', ')}` : 'Comma-separated bucket names; blank for all'}
          value={buckets}
          onChange={(e) => setBuckets(e.target.value)}
          onKeyDown={enter}
          placeholder="All buckets"
          spellCheck={false}
        />
        <datalist id="rl-bucket-names">
          {names.map((n) => (
            <option key={n} value={n} />
          ))}
        </datalist>
        {unknown.length > 0 && <div className="rl-err">Unknown: {unknown.join(', ')}</div>}
      </td>
      <td>
        <span className="rl-pair">
          <select aria-label="Action" value={action} onChange={(e) => setAction(e.target.value as 'exempt' | 'points')}>
            <option value="exempt">Exempt</option>
            <option value="points">Custom limit</option>
          </select>
          {action === 'points' && <input type="number" min={1} aria-label="Points" className="pts" value={points} onChange={(e) => setPoints(Math.floor(Number(e.target.value)))} />}
        </span>
      </td>
      <td>
        <input type="text" aria-label="Note" value={note} onChange={(e) => setNote(e.target.value)} onKeyDown={enter} placeholder="Relay, trusted service…" maxLength={280} />
      </td>
      <td className="rl-act">
        <button type="button" className="btn sm" disabled={!ok} onClick={add}>
          Add override
        </button>
      </td>
    </tr>
  )
}

// ---------------------------------------------------------------- save bar

function SaveBar({ ed }: { ed: Editor }) {
  const { changed, save, movedOn, base, origin, count, actor, setActor, note, setNote, discard } = ed
  if (!changed && !save.busy) return null
  return (
    <form
      className="rl-savebar"
      aria-label="Unsaved rate-limit changes"
      onSubmit={(e) => {
        e.preventDefault()
        save.run()
      }}
    >
      <ErrorNotice error={save.error} />
      {movedOn && !save.error && (
        <Notice kind="warn">
          Version {base} was saved elsewhere since you started editing v{origin.version}. Saving will be refused; discard your edits to load it.
        </Notice>
      )}
      <div className="rl-savebar-row">
        <b className="count">
          {count} unsaved {count === 1 ? 'change' : 'changes'}
        </b>
        <input type="text" className="actor" value={actor} onChange={(e) => setActor(e.target.value)} placeholder="Your name" aria-label="Your name" title="Recorded in the change history." maxLength={64} />
        <input type="text" className="note" value={note} onChange={(e) => setNote(e.target.value)} maxLength={280} placeholder="Note (optional): why this change" aria-label="Note (optional)" />
        <button type="button" className="btn sm" disabled={save.busy} onClick={discard}>
          Discard
        </button>
        <button className="btn sm primary" disabled={!changed || save.busy}>
          {save.busy && <Spinner />}
          Save as v{origin.version + 1}
        </button>
      </div>
    </form>
  )
}

// ---------------------------------------------------------------- sidebar

function RatePanel({ history }: { history: History }) {
  return (
    <Panel title="429s per second" flush actions={<span className="small muted">cluster-wide</span>}>
      {history.any ? (
        <Chart title="" series={history.series} data={history.data} fmt={fmtSi} height={130} />
      ) : (
        <div className="rl-quiet">{history.samples < 1 ? 'Collecting samples…' : `No 429s in the last ${Math.max(1, Math.round((history.samples * POLL) / 60000))} min.`}</div>
      )}
    </Panel>
  )
}

function TopKeysPanel({ d, bucket, onSelect }: { d: RateLimits; bucket?: string; onSelect: (n: string) => void }) {
  const counted = d.limiters.filter((l) => d.top[l.name]?.length).map((l) => l.name)
  const options = bucket && !counted.includes(bucket) ? [bucket, ...counted] : counted
  const rows = bucket ? d.top[bucket] ?? [] : []
  const spec = d.limiters.find((l) => l.name === bucket)
  const multi = d.nodes.length > 1
  return (
    <Panel
      title="Top keys"
      desc={spec ? `Current ${fmtWindow(spec.windowSecs)} window, by ${KEY_LABEL[spec.key]}` : 'Most points used in the current window'}
      flush
      actions={
        options.length > 0 && (
          <select aria-label="Bucket" className="rl-select" value={bucket} onChange={(e) => onSelect(e.target.value)}>
            {options.map((b) => (
              <option key={b} value={b}>
                {shortName(b)}
              </option>
            ))}
          </select>
        )
      }
    >
      {rows.length === 0 ? (
        <div className="rl-quiet">{bucket ? 'Nothing counted in this bucket right now.' : 'Keys appear here once requests consume a bucket.'}</div>
      ) : (
        <div className="table-wrap">
          <table className="data compact rl-side-table">
            <thead>
              <tr>
                <th>Key</th>
                <th>Used / limit</th>
                <th className="num">Resets</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                // per-node counters: the limit applies to what one node counted
                const frac = r.limit ? Math.min(1, r.maxNodeUsed / r.limit) : 0
                return (
                  <tr key={r.key}>
                    <td className="mono small rl-trunc">
                      <CopyKey text={r.key} title={`${r.key}${multi ? `\nCounted on ${r.nodes.join(', ')}` : ''}`} />
                      {multi && r.nodes.length > 1 && <span className="muted"> ×{r.nodes.length}</span>}
                    </td>
                    <td>
                      {r.limit == null ? (
                        <span className="pill accent">exempt · {fmtNum(r.used)}</span>
                      ) : (
                        <span className="rl-meter" title={`${fmtNum(r.used)} used cluster-wide; ${fmtNum(r.maxNodeUsed)} of ${fmtNum(r.limit)} on the busiest node`}>
                          <span className={level(frac)} style={{ width: `${Math.max(2, frac * 100)}%` }} />
                          <b>
                            {fmtNum(r.used)} / {fmtNum(r.limit)}
                          </b>
                        </span>
                      )}
                    </td>
                    <td className="num small muted">{relTime(r.resetMs)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  )
}

function RecentRejections({ d, selected, onSelect }: { d: RateLimits; selected?: string; onSelect: (n: string) => void }) {
  const rows = d.rejections.filter((r) => r.last15m > 0).slice(0, 15)
  const quiet = d.rejections.length - rows.length
  return (
    <Panel title="Recent 429s" desc="By bucket and route, cluster-wide." flush>
      {rows.length === 0 ? (
        <div className="rl-quiet">
          No 429s in the last 15 minutes{quiet > 0 && ` (${quiet} older series since the nodes started)`}.
        </div>
      ) : (
        <div className="table-wrap">
          <table className="data compact rl-side-table">
            <thead>
              <tr>
                <th>Bucket · route</th>
                <th className="num">1m</th>
                <th className="num">5m</th>
                <th className="num">15m</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                const b = shortName(r.limiter)
                const route = r.route.replace(/^com\.atproto\./, '')
                return (
                  <tr key={`${r.limiter} ${r.route}`} className={`link${r.limiter === selected ? ' sel' : ''}`} onClick={() => onSelect(r.limiter)} title={`${r.limiter}\n${r.route}`}>
                    <td className="small rl-trunc">
                      <span className="mono">{b}</span>
                      {!b.startsWith(route) && <span className="muted mono"> · {route}</span>}
                    </td>
                    <td className="num">
                      <Count n={r.last1m} />
                    </td>
                    <td className="num">{fmtNum(r.last5m)}</td>
                    <td className="num">{fmtNum(r.last15m)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  )
}

function NodesPanel({ d }: { d: RateLimits }) {
  return (
    <Panel title="Nodes" desc={`Each re-reads the config every ${d.refreshSecs} s and at once when the saving node nudges it.`} flush>
      <div className="table-wrap">
        <table className="data compact rl-side-table">
          <thead>
            <tr>
              <th>Node</th>
              <th>Config</th>
              <th title="Last change applied">Applied</th>
              <th title="Last checked">Checked</th>
              <th className="num" title="Live windows">Windows</th>
            </tr>
          </thead>
          <tbody>
            {d.nodes.map((n) => (
              <tr key={n.node}>
                <td>
                  <b className="mono">{n.node}</b> {n.self && <span className="pill accent">this</span>}
                </td>
                <td>
                  {!n.reachable ? (
                    <Status kind="bad">Unreachable</Status>
                  ) : n.configError ? (
                    <Status kind="bad">v{n.configVersion}, rejected newer</Status>
                  ) : n.configVersion === d.configVersion ? (
                    <Status kind="ok">v{n.configVersion}</Status>
                  ) : (
                    <Status kind="warn">v{n.configVersion}</Status>
                  )}{' '}
                  {n.enabledByFlag === false && <span className="pill amber">--no-rate-limits</span>}
                </td>
                <td className="small muted">{n.loadedAtMs ? relTime(n.loadedAtMs) : '—'}</td>
                <td className="small muted">{n.checkedAtMs ? relTime(n.checkedAtMs) : '—'}</td>
                <td className="num">{fmtNum(n.liveWindows)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Panel>
  )
}

function HistoryPanel({ d }: { d: RateLimits }) {
  const h = [...(d.config?.history ?? [])].reverse()
  const latest = h[0]
  return (
    <section className="panel rl-history">
      <details>
        <summary>
          <h2>Change history</h2>
          <span className="small muted">{latest ? `${h.length} saved · latest v${latest.version} by ${latest.by}, ${relTime(new Date(latest.at).getTime())}` : 'No changes: built-in defaults'}</span>
        </summary>
        <p className="small muted rl-history-desc">The last 50 saves, newest first. Each is also an audit log line (target vlpds::audit) on the node that saved it.</p>
        {h.length === 0 ? (
          <div className="rl-quiet">No changes yet. The built-in defaults are in force.</div>
        ) : (
          <ol className="rl-log">
            {h.map((a) => (
              <li key={a.version}>
                <div className="head">
                  <b className="mono">v{a.version}</b>
                  <span>{a.by}</span>
                  <span className="muted" title={fmtTime(a.at)}>
                    {relTime(new Date(a.at).getTime())}
                  </span>
                  <span className="muted mono small">
                    {a.node}
                    {a.ip && ` · ${a.ip}`}
                  </span>
                </div>
                {a.changes.map((x, i) => (
                  <div key={i} className="mono small change">
                    {x}
                  </div>
                ))}
                {a.note && <div className="small muted">“{a.note}”</div>}
              </li>
            ))}
          </ol>
        )}
      </details>
    </section>
  )
}
