import { useEffect, useMemo, useRef, useState } from 'react'
import { Chart, type Series } from '../../components/Chart'
import { ErrorNotice, Panel, saveBlob } from '../../components/ui'
import { fmtBytes, fmtNum, fmtSecs, fmtSi } from '../../lib/format'
import { useAction } from '../../lib/hooks'
import { parseProm, quantile, rate, rateBy, sum, type Scrape } from '../../lib/prom'
import { admin } from '../../lib/xrpc'

const INTERVAL = 2000
const KEEP = 180 // 6 minutes at 2 s
const TOP_METHODS = 5

type Point = {
  t: number
  commits?: number
  ops?: number
  http: Map<string, number>
  putP50?: number
  putP99?: number
  durP50?: number
  durP99?: number
  loads?: number
  loadP99?: number
  fhEvents?: number
  fhSent?: number
  forwarded?: number
  shed?: number
  limited?: number
  allocated?: number
  resident?: number
}

function derive(a: Scrape, b: Scrape): Point {
  const r = (n: string, f?: (l: Record<string, string>) => boolean) => rate(a, b, n, f)
  const q = (n: string, x: number) => quantile(a, b, n, x)
  return {
    t: b.t / 1000,
    commits: r('vlpds_commits_total'),
    ops: r('vlpds_ops_total'),
    http: rateBy(a, b, 'vlpds_http_requests_total', 'method'),
    putP50: q('vlpds_segment_put_seconds', 0.5),
    putP99: q('vlpds_segment_put_seconds', 0.99),
    durP50: q('vlpds_commit_durable_seconds', 0.5),
    durP99: q('vlpds_commit_durable_seconds', 0.99),
    loads: r('vlpds_repo_loads_total'),
    loadP99: q('vlpds_repo_load_seconds', 0.99),
    fhEvents: r('vlpds_firehose_events_total'),
    fhSent: r('vlpds_firehose_frames_sent_total'),
    forwarded: r('vlpds_requests_forwarded_total'),
    shed: r('vlpds_writes_shed_total'),
    limited: r('vlpds_rate_limited_total'),
    allocated: sum(b, 'vlpds_jemalloc_bytes', (l) => l.stat === 'allocated'),
    resident: sum(b, 'vlpds_jemalloc_bytes', (l) => l.stat === 'resident'),
  }
}

// Method → color slot, kept while the method stays in the top set, so a
// method never changes color because another one got busier.
const methodSlots = new Map<string, string>()
const SLOTS = ['c1', 'c2', 'c3', 'c4', 'c5']

function pickMethods(points: Point[]): string[] {
  const tot = new Map<string, number>()
  for (const p of points.slice(-30)) for (const [k, v] of p.http) tot.set(k, (tot.get(k) ?? 0) + v)
  const top = [...tot.entries()]
    .filter(([, v]) => v > 0)
    .sort((a, b) => b[1] - a[1])
    .slice(0, TOP_METHODS)
    .map(([k]) => k)
  for (const [k] of methodSlots) if (!top.includes(k)) methodSlots.delete(k)
  for (const k of top) {
    if (methodSlots.has(k)) continue
    const used = new Set(methodSlots.values())
    methodSlots.set(k, SLOTS.find((s) => !used.has(s)) ?? 'c6')
  }
  return top.sort()
}

// the server labels every non-XRPC route (UI, OAuth, /metrics) "other"
const methodLabel = (m: string) => (m === 'other' ? 'non-XRPC routes' : m.replace(/^com\.atproto\./, ''))

const col = (points: Point[], f: (p: Point) => number | undefined) => points.map((p) => f(p) ?? null)

export function Metrics() {
  const [points, setPoints] = useState<Point[]>([])
  const [gauges, setGauges] = useState<Scrape>()
  const [error, setError] = useState<unknown>()
  const last = useRef<Scrape>()

  useEffect(() => {
    let live = true
    const tick = async () => {
      try {
        const r = await fetch('/metrics')
        if (!r.ok) throw new Error(`/metrics returned ${r.status}`)
        const s = parseProm(await r.text())
        if (!live) return
        setError(undefined)
        setGauges(s)
        if (last.current) {
          const p = derive(last.current, s)
          setPoints((xs) => [...xs, p].slice(-KEEP))
        }
        last.current = s
      } catch (e) {
        if (live) setError(e)
      }
    }
    tick()
    const id = setInterval(tick, INTERVAL)
    return () => {
      live = false
      clearInterval(id)
    }
  }, [])

  const xs = useMemo(() => points.map((p) => p.t), [points])
  const methods = useMemo(() => pickMethods(points), [points])
  const latest = points[points.length - 1]
  const g = (n: string, f?: (l: Record<string, string>) => boolean) => (gauges ? sum(gauges, n, f) : undefined)
  const wmLagUs = g('vlpds_watermark_lag_microseconds')

  const httpSeries: Series[] = methods.map((m) => ({ label: methodLabel(m), color: methodSlots.get(m) ?? 'c6' }))
  const httpOther = points.map((p) => {
    let o = 0
    for (const [k, v] of p.http) if (!methods.includes(k)) o += v
    return o
  })
  const http = [xs, ...methods.map((m) => col(points, (p) => p.http.get(m) ?? 0)), httpOther]
  httpSeries.push({ label: 'all other methods', color: 'c6', dash: true })

  return (
    <>
      <div className="console-head">
        <h1>Live metrics</h1>
        <span className={`live${error ? ' stale' : ''}`}>
          <i aria-hidden="true" />
          {error ? 'Not updating' : `Scraping /metrics every ${INTERVAL / 1000} s, last ${KEEP * (INTERVAL / 1000) / 60} min`}
        </span>
      </div>
      <ErrorNotice error={error} />
      <div className="tiles">
        <Tile k="Commits per second" v={latest?.commits !== undefined ? fmtSi(latest.commits) : '—'} />
        <Tile k="HTTP requests in flight" v={fmtNum(g('vlpds_http_requests_inflight'))} />
        <Tile k="Firehose subscribers" v={fmtNum(g('vlpds_firehose_subscribers'))} />
        <Tile k="Repos in memory" v={fmtNum(g('vlpds_cached_repos'))} />
        <Tile k="Watermark lag" v={wmLagUs !== undefined ? fmtSecs(wmLagUs / 1e6) : '—'} />
        <Tile k="Memory allocated" v={latest?.allocated !== undefined ? fmtBytes(latest.allocated) : '—'} />
      </div>
      <div className="grid3">
        <Panel flush>
          <Chart
            title="Commits and record ops"
            sub="Per second. Ops above commits means writes are being coalesced."
            series={[
              { label: 'commits', color: 'c1' },
              { label: 'ops', color: 'c2' },
            ]}
            data={[xs, col(points, (p) => p.commits), col(points, (p) => p.ops)]}
            fmt={fmtSi}
          />
        </Panel>
        <Panel flush>
          <Chart title="HTTP requests by method" sub={`Per second, top ${TOP_METHODS} methods.`} series={httpSeries} data={http} fmt={fmtSi} />
        </Panel>
        <Panel flush>
          <Chart
            title="Commit to durable"
            sub="Enqueue until the segment is durable, applied and acked."
            series={[
              { label: 'p50', color: 'c1' },
              { label: 'p99', color: 'c3' },
            ]}
            data={[xs, col(points, (p) => p.durP50), col(points, (p) => p.durP99)]}
            fmt={fmtSecs}
          />
        </Panel>
        <Panel flush>
          <Chart
            title="Segment PUT latency"
            sub="Until durable, including hedges and retries."
            series={[
              { label: 'p50', color: 'c1' },
              { label: 'p99', color: 'c3' },
            ]}
            data={[xs, col(points, (p) => p.putP50), col(points, (p) => p.putP99)]}
            fmt={fmtSecs}
          />
        </Panel>
        <Panel flush>
          <Chart
            title="Firehose"
            sub="Events merged and frames sent to subscribers, per second."
            series={[
              { label: 'events', color: 'c1' },
              { label: 'frames sent', color: 'c2' },
            ]}
            data={[xs, col(points, (p) => p.fhEvents), col(points, (p) => p.fhSent)]}
            fmt={fmtSi}
          />
        </Panel>
        <Panel flush>
          <Chart
            title="Cold repo loads"
            sub={latest?.loadP99 !== undefined ? `Per second. p99 load time ${fmtSecs(latest.loadP99)}.` : 'Per second: repos rebuilt from state.'}
            series={[{ label: 'loads', color: 'c4' }]}
            data={[xs, col(points, (p) => p.loads)]}
            fmt={fmtSi}
          />
        </Panel>
        <Panel flush>
          <Chart
            title="Forwarded requests"
            sub="Per second, proxied to the shard's owner node."
            series={[{ label: 'forwarded', color: 'c2' }]}
            data={[xs, col(points, (p) => p.forwarded)]}
            fmt={fmtSi}
          />
        </Panel>
        <Panel flush>
          <Chart
            title="Rejected requests"
            sub="Per second: writes shed by admission control (503) and rate-limited requests (429)."
            series={[
              { label: 'shed', color: 'c5' },
              { label: 'rate-limited', color: 'c3' },
            ]}
            data={[xs, col(points, (p) => p.shed), col(points, (p) => p.limited)]}
            fmt={fmtSi}
          />
        </Panel>
        <Panel flush>
          <Chart
            title="Memory (jemalloc)"
            sub="Bytes allocated by the process and resident in RAM."
            series={[
              { label: 'allocated', color: 'c1' },
              { label: 'resident', color: 'c2' },
            ]}
            data={[xs, col(points, (p) => p.allocated), col(points, (p) => p.resident)]}
            fmt={fmtBytes}
          />
        </Panel>
      </div>
      <GrafanaDashboards />
    </>
  )
}

/** The dashboards `vlpds dashboards` prints, for Grafana's Import dialog. */
function GrafanaDashboards() {
  const dl = useAction(async (name: string, file: string) => {
    const r: Response = await admin('vlpds.admin.getGrafanaDashboard', { params: { name }, raw: true })
    saveBlob(await r.blob(), file)
  })
  return (
    <div className="muted small">
      History and alerts: import the Grafana dashboards (Dashboards → New → Import, then pick your Prometheus):{' '}
      <button className="btn" disabled={dl.busy} onClick={() => dl.run('vlpds', 'vlpds.json')}>
        vlpds.json
      </button>{' '}
      <button className="btn" disabled={dl.busy} onClick={() => dl.run('internals', 'vlpds-internals.json')}>
        vlpds-internals.json
      </button>
      <ErrorNotice error={dl.error} />
    </div>
  )
}

function Tile({ k, v }: { k: string; v: string }) {
  return (
    <div className="tile">
      <div className="v">{v}</div>
      <div className="k">{k}</div>
    </div>
  )
}
