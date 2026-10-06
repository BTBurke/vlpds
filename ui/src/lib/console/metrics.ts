import { useSyncExternalStore } from 'react'
import { parseProm, quantile, rate, sum, type Sample, type Scrape } from '../prom'
import { nodeMetrics } from './adminAdapter'
import { getLive } from './live'

// Prometheus scrapes, every 2 s while a console page reads them, turned into rates and
// quantiles client-side (lib/prom.ts). Per node when the server fans /metrics out to its peers
// (adminAdapter.nodeMetrics), else this node's own /metrics; with neither (production keeps
// /metrics off the app port unless --metrics-listen app) the source is "none" and tiles say so.

export const INTERVAL = 2000
export const KEEP = 90 // 3 minutes

export type Point = {
  t: number
  commits?: number
  ops?: number
  http?: number
  http5xx?: number
  durP50?: number
  durP99?: number
  putP50?: number
  putP99?: number
  emitP99?: number
  loads?: number
  fhEvents?: number
  fhBytes?: number
  objA?: number
  objB?: number
  objErr?: number
  limited?: number
  hedges?: number
  /** Percent of the process's cores. */
  cpu?: number
  leaseRatioP99?: number
}

export type Gauges = {
  cachedRepos?: number
  mailQueue?: number
  mailBudgetRemaining?: number
  mailBudgetLimit?: number
  resident?: number
  memLimit?: number
  subscribers?: number
  inflight?: number
}

export type MetricsState = {
  source: 'pending' | 'fanout' | 'local' | 'none'
  /** Merged over every node scraped. */
  cluster: Point[]
  byNode: Record<string, Point[]>
  gauges: Record<string, Gauges>
  /** Node the local scrape belongs to (vlpds_build_info node_id), when known. */
  localNode?: string
  error?: string
  version: number
}

const CLASS_A = /^(put|list|copy|mpu)/
const CLASS_B = /^(get|head)/

function derive(a: Scrape, b: Scrape): Point {
  const r = (n: string, f?: (l: Record<string, string>) => boolean) => rate(a, b, n, f)
  const q = (n: string, x: number) => quantile(a, b, n, x)
  const cpuSecs = r('vlpds_process_cpu_seconds_total')
  const cores = sum(b, 'vlpds_cpu_cores')
  return {
    t: b.t,
    commits: r('vlpds_commits_total'),
    ops: r('vlpds_ops_total'),
    http: r('vlpds_http_requests_total'),
    http5xx: r('vlpds_http_requests_total', (l) => (l.status ?? '').startsWith('5')),
    durP50: q('vlpds_commit_durable_seconds', 0.5),
    durP99: q('vlpds_commit_durable_seconds', 0.99),
    putP50: q('vlpds_segment_put_seconds', 0.5),
    putP99: q('vlpds_segment_put_seconds', 0.99),
    emitP99: q('vlpds_firehose_emit_delay_seconds', 0.99),
    loads: r('vlpds_repo_loads_total'),
    fhEvents: r('vlpds_firehose_events_total'),
    fhBytes: r('vlpds_firehose_bytes_sent_total'),
    objA: r('vlpds_object_store_requests_total', (l) => CLASS_A.test(l.op ?? '')),
    objB: r('vlpds_object_store_requests_total', (l) => CLASS_B.test(l.op ?? '')),
    objErr: r('vlpds_object_store_requests_total', (l) => l.result === 'timeout' || l.result === 'error'),
    limited: r('vlpds_rate_limited_total'),
    hedges: r('vlpds_segment_put_hedges_total'),
    cpu: cpuSecs !== undefined && cores ? (cpuSecs / cores) * 100 : undefined,
    leaseRatioP99: q('vlpds_lease_renew_ttl_ratio', 0.99),
  }
}

function gaugesOf(s: Scrape): Gauges {
  const g = (n: string, f?: (l: Record<string, string>) => boolean) => sum(s, n, f)
  return {
    cachedRepos: g('vlpds_cached_repos'),
    mailQueue: g('vlpds_mail_queue_depth'),
    mailBudgetRemaining: g('vlpds_mail_budget_remaining'),
    mailBudgetLimit: g('vlpds_mail_budget_limit'),
    resident: g('vlpds_process_resident_bytes'),
    memLimit: g('vlpds_memory_limit_bytes'),
    subscribers: g('vlpds_firehose_subscribers'),
    inflight: g('vlpds_http_requests_inflight'),
  }
}

let st: MetricsState = { source: 'pending', cluster: [], byNode: {}, gauges: {}, version: 0 }
const prev = new Map<string, Scrape>()
const subs = new Set<() => void>()
let timer: ReturnType<typeof setInterval> | undefined
let busy = false
let fanout: boolean | undefined

const push = (arr: Point[] | undefined, p: Point) => [...(arr ?? []), p].slice(-KEEP)

async function scrapeAll(): Promise<{ node: string; s: Scrape }[] | null> {
  if (fanout !== false) {
    const r = await nodeMetrics()
    if (r.supported) {
      fanout = true
      const t = Date.now()
      return r.data.filter((n) => n.text).map((n) => ({ node: n.node, s: parseProm(n.text!, t) }))
    }
    fanout = false
  }
  const res = await fetch('/metrics')
  if (res.status === 404) return null
  if (!res.ok) throw new Error(`/metrics answered ${res.status}`)
  const s = parseProm(await res.text())
  const node = s.samples.find((x) => x.name === 'vlpds_build_info')?.labels.node_id || 'local'
  return [{ node, s }]
}

async function tick() {
  if (busy || getLive().paused) return
  busy = true
  try {
    const got = await scrapeAll()
    if (!got) {
      st = { ...st, source: 'none', version: st.version + 1 }
      return
    }
    const byNode = { ...st.byNode }
    const gauges: Record<string, Gauges> = {}
    const mergedA: Sample[] = []
    const mergedB: Sample[] = []
    let ta = 0
    let tb = 0
    for (const { node, s } of got) {
      gauges[node] = gaugesOf(s)
      const p = prev.get(node)
      if (p) {
        byNode[node] = push(byNode[node], derive(p, s))
        mergedA.push(...p.samples)
        mergedB.push(...s.samples)
        ta = Math.max(ta, p.t)
        tb = Math.max(tb, s.t)
      }
      prev.set(node, s)
    }
    const cluster = mergedB.length ? push(st.cluster, derive({ t: ta, samples: mergedA }, { t: tb, samples: mergedB })) : st.cluster
    st = {
      source: fanout ? 'fanout' : 'local',
      cluster,
      byNode,
      gauges,
      localNode: fanout ? undefined : got[0]?.node,
      version: st.version + 1,
    }
  } catch (e) {
    st = { ...st, error: e instanceof Error ? e.message : String(e), version: st.version + 1 }
  } finally {
    busy = false
    subs.forEach((l) => l())
  }
}

function subscribe(l: () => void) {
  subs.add(l)
  if (subs.size === 1) {
    tick()
    timer = setInterval(tick, INTERVAL)
  }
  return () => {
    subs.delete(l)
    if (!subs.size && timer) {
      clearInterval(timer)
      timer = undefined
    }
  }
}

export const useMetrics = () => useSyncExternalStore(subscribe, () => st)

/** One field of a series, nulls where it was undefined (a gap, not a zero). */
export const col = (points: Point[], k: keyof Point) => points.map((p) => (p[k] as number | undefined) ?? null)
export const last = (points: Point[] | undefined, k: keyof Point): number | undefined => {
  if (!points) return undefined
  for (let i = points.length - 1; i >= 0; i--) {
    const v = points[i][k]
    if (v !== undefined) return v as number
  }
  return undefined
}

/** Points for the node with this id: the local scrape answers for the console's own node. */
export function nodePoints(m: MetricsState, node: string, self?: boolean): Point[] | undefined {
  return m.byNode[node] ?? (self && m.source === 'local' ? Object.values(m.byNode)[0] : undefined)
}
export function nodeGauges(m: MetricsState, node: string, self?: boolean): Gauges | undefined {
  return m.gauges[node] ?? (self && m.source === 'local' ? Object.values(m.gauges)[0] : undefined)
}
