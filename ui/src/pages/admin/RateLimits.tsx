import { useEffect, useMemo, useRef, useState } from 'react'
import { Chart, type Series } from '../../components/Chart'
import { Empty, ErrorNotice, Field, Loading, Notice, Panel, PartialNotice, partialOf, Spinner, Status } from '../../components/ui'
import { fmtNum, fmtSi, fmtTime, relTime } from '../../lib/format'
import { useAction, useLoad } from '../../lib/hooks'
import { admin } from '../../lib/xrpc'
import { useLive } from './Cluster'

// ---------------------------------------------------------------- server shapes (src/xrpc/ratelimits.rs)

type KeyKind = 'ip' | 'identifier-ip' | 'did'

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

const KEY_LABEL: Record<KeyKind, string> = { ip: 'client IP', 'identifier-ip': 'identifier + IP', did: 'DID' }

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

// ---------------------------------------------------------------- page

export function RateLimits() {
  const c = useLoad<RateLimits & { fetchedAt: number }>(
    async () => ({ ...(await admin('vlpds.admin.getRateLimits', { params: { top: TOP } })), fetchedAt: Date.now() }),
    [],
    POLL,
  )
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
  const reachable = d.nodes.filter((n) => n.reachable)
  const onVersion = reachable.filter((n) => n.configVersion === d.configVersion).length
  const versions = [...new Set(reachable.map((n) => n.configVersion))]
  const errored = d.nodes.filter((n) => n.configError)
  const flagOff = d.nodes.filter((n) => n.reachable && n.enabledByFlag === false)
  const last1m = d.rejections.reduce((s, r) => s + r.last1m, 0)
  const last15m = d.rejections.reduce((s, r) => s + r.last15m, 0)
  const overrides = d.config?.overrides?.length ?? 0

  return (
    <>
      <div className="console-head">
        <h1>Rate limits</h1>
        <span className={`live${live ? '' : ' stale'}`} aria-live="polite">
          <i aria-hidden="true" />
          {live ? `Live, every ${POLL / 1000} s, from every node` : 'Not updating'}
        </span>
      </div>
      <ErrorNotice error={c.error} />
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
      <div className="tiles">
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
          <div className="v">{fmtNum(last1m)}</div>
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
      <Panel flush>
        <Chart title="429s per second by bucket" sub="Cluster-wide, from the change in each bucket's 429 total between polls." series={history.series} data={history.data} fmt={fmtSi} />
      </Panel>
      <div className="grid2">
        <TopConsumers d={d} />
        <RecentRejections d={d} />
      </div>
      <Editor d={d} onSaved={c.reload} />
      <NodesPanel d={d} />
      <HistoryPanel d={d} />
    </>
  )
}

// ---------------------------------------------------------------- live 429 chart

type Sample = { t: number; totals: Map<string, number> }

function useRejectionHistory(d?: RateLimits & { fetchedAt: number }) {
  const [samples, setSamples] = useState<Sample[]>([])
  const last = useRef<number>()
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
    const top = [...tot.entries()]
      .filter(([, v]) => v > 0)
      .sort((a, b) => b[1] - a[1])
      .slice(0, SLOTS.length)
      .map(([k]) => k)
      .sort()
    const series: Series[] = top.map((k, i) => ({ label: k, color: SLOTS[i] }))
    const data: (number | null)[][] = [rates.map((r) => r.t), ...top.map((k) => rates.map((r) => r.by.get(k) ?? 0))]
    if (!top.length) {
      series.push({ label: 'all buckets', color: 'c6' })
      data.push(rates.map((r) => [...r.by.values()].reduce((s, v) => s + v, 0)))
    }
    return { series, data }
  }, [samples])
}

// ---------------------------------------------------------------- observability panels

function TopConsumers({ d }: { d: RateLimits }) {
  const buckets = d.limiters.filter((l) => d.top[l.name]?.length).map((l) => l.name)
  const [sel, setSel] = useState<string>()
  const bucket = sel && buckets.includes(sel) ? sel : buckets[0]
  const rows = bucket ? d.top[bucket] ?? [] : []
  const spec = d.limiters.find((l) => l.name === bucket)
  const multi = d.nodes.length > 1
  return (
    <Panel
      title="Top consumers"
      desc={spec ? `Most points used in the current ${fmtWindow(spec.windowSecs)} window, keyed by ${KEY_LABEL[spec.key]}.` : 'Most points used in the current window.'}
      flush
      actions={
        buckets.length > 0 && (
          <select aria-label="Bucket" value={bucket} onChange={(e) => setSel(e.target.value)} style={{ width: 'auto', minHeight: 32, padding: '4px 8px', fontSize: 13 }}>
            {buckets.map((b) => (
              <option key={b}>{b}</option>
            ))}
          </select>
        )
      }
    >
      {!bucket ? (
        <Empty title="Nothing counted yet">Keys appear here once requests consume a bucket.</Empty>
      ) : (
        <div className="table-wrap">
          <table className="data compact">
            <thead>
              <tr>
                <th>Key</th>
                <th className="num">Used</th>
                <th>Of limit</th>
                <th className="num">Resets</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                // per-node counters: the limit applies to what one node counted
                const frac = r.limit ? Math.min(1, r.maxNodeUsed / r.limit) : 0
                return (
                  <tr key={r.key}>
                    <td className="mono small" title={multi ? `Counted on ${r.nodes.join(', ')}` : undefined}>
                      {r.key}
                      {multi && r.nodes.length > 1 && <span className="muted"> ×{r.nodes.length}</span>}
                    </td>
                    <td className="num">{fmtNum(r.used)}</td>
                    <td>
                      {r.limit == null ? (
                        <span className="pill accent">exempt</span>
                      ) : (
                        <span className="rl-meter" title={`${fmtNum(r.maxNodeUsed)} of ${fmtNum(r.limit)} on the busiest node`}>
                          <span className={frac >= 1 ? 'over' : frac >= 0.8 ? 'near' : ''} style={{ width: `${Math.max(2, frac * 100)}%` }} />
                          <b>{fmtNum(r.limit)}</b>
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

function RecentRejections({ d }: { d: RateLimits }) {
  const rows = d.rejections.filter((r) => r.last15m > 0).slice(0, 15)
  const quiet = d.rejections.length - rows.length
  return (
    <Panel title="Recent 429s" desc="By bucket and route, cluster-wide. Minute buckets: this minute plus the previous ones." flush>
      {rows.length === 0 ? (
        <Empty title="No 429s in the last 15 minutes">{quiet > 0 && `${quiet} older series since the nodes started.`}</Empty>
      ) : (
        <div className="table-wrap">
          <table className="data compact">
            <thead>
              <tr>
                <th>Bucket</th>
                <th>Route</th>
                <th className="num">1 min</th>
                <th className="num">5 min</th>
                <th className="num">15 min</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={`${r.limiter} ${r.route}`}>
                  <td className="mono small">{r.limiter}</td>
                  <td className="mono small muted">{r.route.replace(/^com\.atproto\./, '')}</td>
                  <td className="num">{fmtNum(r.last1m)}</td>
                  <td className="num">{fmtNum(r.last5m)}</td>
                  <td className="num">{fmtNum(r.last15m)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  )
}

function NodesPanel({ d }: { d: RateLimits }) {
  return (
    <Panel title="Nodes" desc={`Each node re-reads the config every ${d.refreshSecs} s (a 304 when unchanged) and at once when the saving node nudges it.`} flush>
      <div className="table-wrap">
        <table className="data compact">
          <thead>
            <tr>
              <th>Node</th>
              <th>Config</th>
              <th>Last change applied</th>
              <th>Last checked</th>
              <th className="num">Live windows</th>
            </tr>
          </thead>
          <tbody>
            {d.nodes.map((n) => (
              <tr key={n.node}>
                <td>
                  <b className="mono">{n.node}</b> {n.self && <span className="pill accent">this node</span>}
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
  return (
    <Panel title="Change history" desc="The last 50 saves, newest first. Each is also an audit log line (target vlpds::audit) on the node that saved it." flush>
      {h.length === 0 ? (
        <Empty title="No changes yet">The built-in defaults are in force.</Empty>
      ) : (
        <div className="table-wrap">
          <table className="data">
            <thead>
              <tr>
                <th className="num">Version</th>
                <th>When</th>
                <th>Who</th>
                <th>Changes</th>
              </tr>
            </thead>
            <tbody>
              {h.map((a) => (
                <tr key={a.version}>
                  <td className="num mono">v{a.version}</td>
                  <td className="nowrap small">{fmtTime(a.at)}</td>
                  <td className="small">
                    {a.by}
                    <div className="muted mono">
                      {a.node}
                      {a.ip && ` · ${a.ip}`}
                    </div>
                  </td>
                  <td className="small">
                    {a.changes.map((x, i) => (
                      <div key={i} className="mono">
                        {x}
                      </div>
                    ))}
                    {a.note && <div className="muted">“{a.note}”</div>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  )
}

// ---------------------------------------------------------------- editor

const ACTOR_KEY = 'vlpds.admin.actor'

function Editor({ d, onSaved }: { d: RateLimits; onSaved: () => void }) {
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
  const [result, setResult] = useState<{ version: number; nodes: { node: string; ok: boolean; configVersion?: number; error?: string }[] }>()
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
  const builtins = d.limiters.filter((l) => !l.custom && l.default)
  const names = d.limiters.map((l) => l.name).concat(draft.routes.map((r) => `route:${r.nsid}`).filter((n) => !d.limiters.some((l) => l.name === n)))

  return (
    <Panel
      title="Edit limits"
      id="rl-edit"
      desc={
        <>
          Saved as a new version of the cluster's config object; every node applies it within seconds, without a restart. A new points value keeps each key's live window; a new
          window length starts fresh windows.
        </>
      }
    >
      <label className="check">
        <input type="checkbox" checked={draft.enabled} onChange={(e) => set((x) => void (x.enabled = e.target.checked))} />
        <span>
          <b>Enforce rate limits</b> <span className="muted">— off: nothing is counted or limited on any node (the RateLimit headers go away too).</span>
        </span>
      </label>

      <h3 className="rl-h">Buckets</h3>
      <div className="table-wrap">
        <table className="data compact rl-edit">
          <thead>
            <tr>
              <th>Bucket</th>
              <th>Key</th>
              <th className="num">Points</th>
              <th className="num">Window (s)</th>
              <th>On</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {builtins.map((l) => {
              const v = draft.limiters[l.name]
              if (!v) return null
              const def = l.default!
              const modified = v.points !== def.points || v.windowSecs !== def.windowSecs || !v.enabled
              return (
                <tr key={l.name} className={v.enabled ? '' : 'off'}>
                  <td>
                    <span className="mono">{l.name}</span>
                    <div className="small muted">{l.scope}</div>
                  </td>
                  <td className="small">{KEY_LABEL[l.key]}</td>
                  <td className="num">
                    <input
                      type="number"
                      min={1}
                      aria-label={`${l.name} points`}
                      value={v.points}
                      onChange={(e) => set((x) => void (x.limiters[l.name].points = Math.max(0, Math.floor(Number(e.target.value)))))}
                    />
                  </td>
                  <td className="num">
                    <input
                      type="number"
                      min={1}
                      max={604800}
                      aria-label={`${l.name} window in seconds`}
                      value={v.windowSecs}
                      onChange={(e) => set((x) => void (x.limiters[l.name].windowSecs = Math.max(0, Math.floor(Number(e.target.value)))))}
                    />
                    <div className="small muted">{v.windowSecs > 0 && fmtWindow(v.windowSecs)}</div>
                  </td>
                  <td>
                    <input type="checkbox" aria-label={`${l.name} enabled`} checked={v.enabled} onChange={(e) => set((x) => void (x.limiters[l.name].enabled = e.target.checked))} />
                  </td>
                  <td className="nowrap">
                    {modified && (
                      <button
                        type="button"
                        className="btn quiet sm"
                        title={`Default: ${def.points} per ${fmtWindow(def.windowSecs)}`}
                        onClick={() => set((x) => void (x.limiters[l.name] = { points: def.points, windowSecs: def.windowSecs, enabled: true }))}
                      >
                        Default
                      </button>
                    )}
                  </td>
                </tr>
              )
            })}
            {draft.routes.map((r, i) => (
              <tr key={`route-${i}`} className={r.enabled === false ? 'off' : ''}>
                <td>
                  <span className="mono">route:{r.nsid}</span>
                  <div className="small muted">added bucket for {r.nsid}</div>
                </td>
                <td className="small">client IP</td>
                <td className="num">
                  <input type="number" min={1} aria-label={`${r.nsid} points`} value={r.points} onChange={(e) => set((x) => void (x.routes[i].points = Math.max(0, Math.floor(Number(e.target.value)))))} />
                </td>
                <td className="num">
                  <input
                    type="number"
                    min={1}
                    max={604800}
                    aria-label={`${r.nsid} window in seconds`}
                    value={r.windowSecs}
                    onChange={(e) => set((x) => void (x.routes[i].windowSecs = Math.max(0, Math.floor(Number(e.target.value)))))}
                  />
                  <div className="small muted">{r.windowSecs > 0 && fmtWindow(r.windowSecs)}</div>
                </td>
                <td>
                  <input type="checkbox" aria-label={`${r.nsid} enabled`} checked={r.enabled !== false} onChange={(e) => set((x) => void (x.routes[i].enabled = e.target.checked))} />
                </td>
                <td>
                  <button type="button" className="btn quiet sm" onClick={() => set((x) => void x.routes.splice(i, 1))}>
                    Remove
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <AddRoute onAdd={(r) => set((x) => void x.routes.push(r))} taken={draft.routes.map((r) => r.nsid)} />

      <h3 className="rl-h">Overrides</h3>
      <p className="small muted rl-p">
        An IP or CIDR override applies to every request from a matching client IP; a DID override to the DID-keyed buckets (repo writes, handle and email flows, OAuth sign-in) of
        that account. Exempt beats a custom limit; between custom limits the larger wins.
      </p>
      {draft.overrides.length > 0 && (
        <div className="table-wrap">
          <table className="data compact">
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
                <tr key={i}>
                  <td className="mono small">
                    <span className="muted">{o.ip ? 'IP ' : 'DID '}</span>
                    {o.ip ?? o.did}
                  </td>
                  <td className="mono small">{o.limiters?.length ? o.limiters.join(', ') : <span className="muted">all</span>}</td>
                  <td>{o.exempt ? <span className="pill accent">exempt</span> : <span className="pill">{fmtNum(o.points)} points</span>}</td>
                  <td className="small">{o.note}</td>
                  <td>
                    <button type="button" className="btn quiet sm" onClick={() => set((x) => void x.overrides.splice(i, 1))}>
                      Remove
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <AddOverride names={names} onAdd={(o) => set((x) => void x.overrides.push(o))} />

      <div className="rl-save">
        <ErrorNotice error={save.error} />
        {movedOn && !save.error && (
          <Notice kind="warn">
            Version {base} was saved elsewhere since you started editing v{origin.version}. Saving will be refused; discard your edits to load it.
          </Notice>
        )}
        {result && !changed && (
          <Notice kind={result.nodes.every((n) => n.ok && n.configVersion === result.version) ? 'ok' : 'warn'}>
            Saved version {result.version}.{' '}
            {result.nodes.map((n) => (
              <span key={n.node} className="nowrap">
                <span className="mono">{n.node}</span> {n.ok ? `runs v${n.configVersion}` : `not reached (${n.error ?? 'error'}); it re-reads within ${d.refreshSecs} s`}.{' '}
              </span>
            ))}
          </Notice>
        )}
        <form
          className="inline-form"
          onSubmit={(e) => {
            e.preventDefault()
            save.run()
          }}
        >
          <Field label="Your name" hint="Recorded in the change history.">
            <input type="text" value={actor} onChange={(e) => setActor(e.target.value)} placeholder="admin" maxLength={64} />
          </Field>
          <Field label="Note (optional)">
            <input type="text" value={note} onChange={(e) => setNote(e.target.value)} maxLength={280} placeholder="Why this change" />
          </Field>
          <div className="row">
            <button type="button" className="btn" disabled={!changed || save.busy} onClick={discard}>
              Discard edits
            </button>
            <button className="btn primary" disabled={!changed || save.busy}>
              {save.busy && <Spinner />}
              Save as v{origin.version + 1}
            </button>
          </div>
        </form>
      </div>
    </Panel>
  )
}

function AddRoute({ onAdd, taken }: { onAdd: (r: RouteCfg) => void; taken: string[] }) {
  const [nsid, setNsid] = useState('')
  const [points, setPoints] = useState(300)
  const [win, setWin] = useState(300)
  const n = nsid.trim()
  const dup = taken.includes(n)
  return (
    <form
      className="inline-form rl-add"
      onSubmit={(e) => {
        e.preventDefault()
        if (!n || dup) return
        onAdd({ nsid: n, points, windowSecs: win, enabled: true })
        setNsid('')
      }}
    >
      <Field label="Add a bucket for an XRPC method" hint={dup ? 'That method already has one.' : 'Keyed by client IP, on top of global-ip.'}>
        <input type="text" value={nsid} onChange={(e) => setNsid(e.target.value)} placeholder="app.bsky.feed.getTimeline" spellCheck={false} />
      </Field>
      <Field label="Points">
        <input type="number" min={1} value={points} onChange={(e) => setPoints(Math.floor(Number(e.target.value)))} />
      </Field>
      <Field label="Window (s)">
        <input type="number" min={1} max={604800} value={win} onChange={(e) => setWin(Math.floor(Number(e.target.value)))} />
      </Field>
      <button className="btn" disabled={!n || dup}>
        Add bucket
      </button>
    </form>
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
  return (
    <form
      className="inline-form rl-add"
      onSubmit={(e) => {
        e.preventDefault()
        if (!who.trim() || unknown.length) return
        const o: OverrideCfg = kind === 'ip' ? { ip: who.trim() } : { did: who.trim() }
        if (list.length) o.limiters = list
        if (action === 'exempt') o.exempt = true
        else o.points = points
        if (note.trim()) o.note = note.trim()
        onAdd(o)
        setWho('')
        setNote('')
      }}
    >
      <Field label="Match">
        <select value={kind} onChange={(e) => setKind(e.target.value as 'ip' | 'did')}>
          <option value="ip">IP or CIDR</option>
          <option value="did">DID</option>
        </select>
      </Field>
      <Field label={kind === 'ip' ? 'IP or CIDR block' : 'DID'}>
        <input type="text" value={who} onChange={(e) => setWho(e.target.value)} placeholder={kind === 'ip' ? '203.0.113.0/24' : 'did:plc:…'} spellCheck={false} />
      </Field>
      <Field label="Buckets (blank: all)" hint={unknown.length ? `Unknown: ${unknown.join(', ')}` : undefined}>
        <input type="text" list="rl-bucket-names" value={buckets} onChange={(e) => setBuckets(e.target.value)} placeholder="global-ip, repo-write-hour" spellCheck={false} />
        <datalist id="rl-bucket-names">
          {names.map((n) => (
            <option key={n} value={n} />
          ))}
        </datalist>
      </Field>
      <Field label="Action">
        <select value={action} onChange={(e) => setAction(e.target.value as 'exempt' | 'points')}>
          <option value="exempt">Exempt</option>
          <option value="points">Custom limit</option>
        </select>
      </Field>
      {action === 'points' && (
        <Field label="Points">
          <input type="number" min={1} value={points} onChange={(e) => setPoints(Math.floor(Number(e.target.value)))} />
        </Field>
      )}
      <Field label="Note">
        <input type="text" value={note} onChange={(e) => setNote(e.target.value)} placeholder="Relay, trusted service…" maxLength={280} />
      </Field>
      <button className="btn" disabled={!who.trim() || unknown.length > 0}>
        Add override
      </button>
    </form>
  )
}
