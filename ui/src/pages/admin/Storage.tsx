import { useState } from 'react'
import { DataTable } from '../../components/console/DataTable'
import { Banners, Chip, ErrorState, Glyph, KV, Loading, Meter, Mini, Minis, NeedsVersion, PageHead, Panel, PanelBody, Seg, Spark, Src, Swatch, Tiles, type BannerSpec } from '../../components/console/kit'
import { registerPalette } from '../../components/console/Palette'
import { useClusterView } from '../../lib/console/cluster'
import { fmtMs, fmtNum, fmtSi } from '../../lib/console/fmt'
import { configPoll, maxLatest, nodeSeries, sumLatest, sumMean, sumSeries, useNodeMetrics, worstSeries, type NodeSeries } from '../../lib/console/sys'
import { navigate } from '../../lib/router'

// Object store: billable request rates (class A: writes, lists, CAS; class B: reads) and what
// they cost, by key component and by node, segment PUT latency, and the same traffic priced at
// R2, S3 and GCS. Bucket prefix sizes would mean listing the bucket, so they aren't here.

export type Provider = 'R2' | 'S3' | 'GCS'
/** USD list prices: per million requests, per GB-month; free tiers per month. */
export const PRICE: Record<Provider, { name: string; a: number; b: number; gb: number; freeA: number; freeB: number; freeGB: number; egress: string }> = {
  R2: { name: 'Cloudflare R2', a: 4.5, b: 0.36, gb: 0.015, freeA: 1, freeB: 10, freeGB: 10, egress: 'free' },
  S3: { name: 'AWS S3 Standard', a: 5, b: 0.4, gb: 0.023, freeA: 0, freeB: 0, freeGB: 0, egress: '$0.09/GB out of AWS' },
  GCS: { name: 'Google Cloud Storage', a: 5, b: 0.4, gb: 0.02, freeA: 0, freeB: 0, freeGB: 0, egress: '$0.12/GB out of GCP' },
}

/** Monthly figures at a steady rate: millions of requests, and dollars after the free tier. */
export function monthCost(p: Provider, aPerSec: number, bPerSec: number, gb?: number, days = 30) {
  const P = PRICE[p]
  const mA = (aPerSec * 86400 * days) / 1e6
  const mB = (bPerSec * 86400 * days) / 1e6
  const ca = Math.max(0, mA - P.freeA) * P.a
  const cb = Math.max(0, mB - P.freeB) * P.b
  const cs = gb === undefined ? undefined : Math.max(0, gb - P.freeGB) * P.gb
  return { mA, mB, ca, cb, cs, total: ca + cb + (cs ?? 0) }
}

/** One component's requests over a month at list price, before the free tier (which is per account, not per component). */
export const componentUsd = (p: Provider, aPerSec: number, bPerSec: number, days = 30) => ((aPerSec * PRICE[p].a + bPerSec * PRICE[p].b) * 86400 * days) / 1e6

/** The provider an endpoint belongs to; MinIO and the rest are "S3-compatible". */
export function providerOf(endpoint?: string): { p?: Provider; label: string } {
  const e = endpoint ?? ''
  if (/r2\.cloudflarestorage\.com/.test(e)) return { p: 'R2', label: 'Cloudflare R2' }
  if (/amazonaws\.com/.test(e)) return { p: 'S3', label: 'AWS S3' }
  if (/storage\.googleapis\.com/.test(e)) return { p: 'GCS', label: 'Google Cloud Storage' }
  return { label: 'S3-compatible' }
}

export const COMPONENTS: Record<string, { name: string; what: string }> = {
  log_segment: { name: 'Log segments', what: 'a PUT per batch of commits; GETs for replay and cursor backfill' },
  retention_report: { name: 'Log retention', what: 'retention passes and their reports' },
  ctl_lease: { name: 'Node leases', what: 'each node renews its lease by CAS every TTL/5' },
  ctl_assign: { name: 'Shard assignments', what: 'owners and the slot layout, by CAS' },
  ctl_writer: { name: 'Writer claims', what: 'the writer byte each log signs seqs with' },
  ctl_version: { name: 'Feature level', what: 'the cluster version object' },
  account_index: { name: 'Handle and email index', what: 'uniqueness claims for handles and emails' },
  blob: { name: 'Blobs', what: 'uploads (multipart when large), reads, GC' },
  state_manifest: { name: 'SlateDB manifests', what: 'every shard re-reads its manifest (--slatedb-manifest-poll)' },
  state_sst: { name: 'SlateDB SSTs', what: 'memtable flushes, compaction, reads that miss the caches' },
  state_wal: { name: 'SlateDB WAL', what: 'write-ahead log objects of the shard DBs' },
  state_compactions: { name: 'SlateDB compactions', what: 'compaction state each compactor polls' },
  state_gc_boundary: { name: 'SlateDB GC boundary', what: 'read with every latest-manifest and compactions read' },
  state_other: { name: 'SlateDB, other', what: 'checkpoints and the rest of a shard DB' },
  other: { name: 'Everything else', what: 'config, mail budget, moderation, spaces' },
}
export const componentName = (c: string) => COMPONENTS[c]?.name ?? c

export type ComponentRow = { component: string; a: number; b: number; byNode: { node: string; a: number; b: number }[] }
/** storeComponents summed over the nodes that answered. */
export function componentRows(nodes: NodeSeries[]): ComponentRow[] {
  const m = new Map<string, ComponentRow>()
  for (const n of nodes) {
    for (const c of n.raw.storeComponents ?? []) {
      const r = m.get(c.component) ?? { component: c.component, a: 0, b: 0, byNode: [] }
      r.a += c.classAPerSec
      r.b += c.classBPerSec
      r.byNode.push({ node: n.node, a: c.classAPerSec, b: c.classBPerSec })
      m.set(c.component, r)
    }
  }
  return [...m.values()]
}

const usd = (v?: number) => (v === undefined ? '—' : v < 10 ? `$${v.toFixed(2)}` : `$${Math.round(v).toLocaleString()}`)
const GB_KEY = 'vlpds.console.storedGb'
const readGb = () => {
  try {
    const v = localStorage.getItem(GB_KEY)
    return v ? Number(v) : undefined
  } catch {
    return undefined
  }
}

/** Days into the UTC month, and its length. */
function monthProgress(now = Date.now()) {
  const d = new Date(now)
  const start = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1)
  const end = Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1)
  return { elapsed: (now - start) / 86400_000, length: (end - start) / 86400_000 }
}

registerPalette({
  items: () => [
    { group: 'Go to', title: 'Object store spend', desc: 'class A and B, projected', glyph: '$', run: () => navigate('/admin/storage') },
    ...Object.entries(COMPONENTS).map(([k, v]) => ({ group: 'Go to', title: `Object store: ${v.name}`, desc: v.what, hay: k, glyph: '◫', run: () => navigate(`/admin/storage?open=storecomp:${k}`) })),
  ],
})

export function Storage() {
  const { view } = useClusterView()
  const m = useNodeMetrics()
  const cfg = configPoll.use()
  const [gb, setGb] = useState<number | undefined>(readGb)
  const self = cfg.data?.find((x) => x.config && x.self)?.config ?? cfg.data?.find((x) => x.config)?.config
  const setting = (f: string) => self?.settings.find((s) => s.flag === f)?.value
  const endpoint = setting('--s3-endpoint')
  const prov = providerOf(endpoint)
  const [hl, setHl] = useState<Provider | undefined>()
  const priceAt: Provider = hl ?? prov.p ?? 'R2'
  const color = (node: string) => view?.nodes.find((n) => n.node === node)?.color

  if (m.status === 'unsupported')
    return (
      <>
        <PageHead title="Object store" />
        <Panel>
          <NeedsVersion what="Object-store request rates" nsid="vlpds.admin.getNodeMetrics" />
        </Panel>
      </>
    )
  if (m.status === 'pending') return <Loading label="Asking every node…" />
  if (m.status === 'error' && !m.nodes.length) return <ErrorState error={m.error} />

  const nodes = m.nodes.filter((n) => n.reachable)
  const aNow = sumLatest(nodes, 'classAPerSec')
  const bNow = sumLatest(nodes, 'classBPerSec')
  const aAvg = sumMean(nodes, 'classAPerSec') ?? 0
  const bAvg = sumMean(nodes, 'classBPerSec') ?? 0
  const errNow = sumLatest(nodes, 'storeErrorsPerSec') ?? 0
  const month = monthProgress()
  const cur = monthCost(priceAt, aAvg, bAvg, gb, month.length)
  const mtd = monthCost(priceAt, aAvg, bAvg, gb === undefined ? undefined : gb * (month.elapsed / month.length), month.elapsed)
  const comps = componentRows(nodes).sort((x, y) => y.a * PRICE[priceAt].a + y.b * PRICE[priceAt].b - (x.a * PRICE[priceAt].a + x.b * PRICE[priceAt].b))
  const spendOf = (r: { a: number; b: number }) => r.a * PRICE[priceAt].a + r.b * PRICE[priceAt].b
  const spendAll = comps.reduce((t, r) => t + spendOf(r), 0) || 1
  const windowMin = Math.round((nodes[0]?.raw.storeWindowMs ?? 0) / 60000)
  const putP99 = maxLatest(nodes, 'putP99Ms')

  const banners: BannerSpec[] = []
  if (m.unreachable.length) banners.push({ id: 'unreach', tone: 'warn', title: `${m.unreachable.join(', ')} didn't answer`, desc: 'Rates and spend leave out what those nodes send.' })
  if (errNow > 0) banners.push({ id: 'err', tone: 'err', title: `${fmtSi(errNow)} object-store errors or timeouts a second`, desc: 'Timeouts and errors after the client’s retries, all nodes, last 10 s.' })

  const byNode = nodes.map((n) => ({
    n,
    a: n.latest?.classAPerSec ?? 0,
    b: n.latest?.classBPerSec ?? 0,
    err: n.latest?.storeErrorsPerSec ?? 0,
    put: n.latest?.putP99Ms,
  }))

  return (
    <>
      <PageHead
        title="Object store"
        sub={
          <>
            <span>{prov.label}</span>
            {self && (
              <span className="mono">
                {setting('--s3-bucket')}/{setting('--prefix') ?? ''}
                {setting('--prefix') ? '/' : ''}
              </span>
            )}
            {endpoint && <span className="mono muted">{endpoint.replace(/^https?:\/\//, '')}</span>}
          </>
        }
        actions={
          <Seg
            label="Price at"
            value={priceAt}
            onChange={(v) => setHl(v)}
            options={(['R2', 'S3', 'GCS'] as Provider[]).map((p) => ({ v: p, label: p }))}
          />
        }
      />
      <Banners items={banners} />
      <Tiles
        boxed
        tiles={[
          { label: 'Class A requests / s', right: 'PUT · LIST · CAS', value: aNow === undefined ? '—' : fmtSi(aNow), spark: <Spark data={sumSeries(nodes, 'classAPerSec')} color="c3" /> },
          { label: 'Class B requests / s', right: 'GET · HEAD', value: bNow === undefined ? '—' : fmtSi(bNow), spark: <Spark data={sumSeries(nodes, 'classBPerSec')} color="c6" /> },
          {
            label: 'Spent this month',
            right: `${priceAt} · day ${Math.ceil(month.elapsed)} of ${month.length}`,
            value: usd(mtd.total),
            sec: `of ${usd(cur.total)}`,
            title: `Estimated: the last ${windowMin || 3} minutes' rate over the whole month so far, after ${priceAt}'s free tier`,
          },
          { label: 'Projected, this month', right: `${fmtNum(cur.mA, 1)} M A · ${fmtNum(cur.mB, 1)} M B`, value: usd(cur.total), sec: gb === undefined ? 'requests only' : `incl. ${fmtNum(gb, 1)} GB` },
          { label: 'Errors · timeouts / s', right: 'after retries', value: errNow ? fmtSi(errNow) : '0', spark: <Spark data={sumSeries(nodes, 'storeErrorsPerSec')} color="warn" /> },
          { label: 'Segment PUT p99', right: 'worst node · p50 dashed', value: fmtMs(putP99), spark: <Spark data={worstSeries(nodes, 'putP99Ms')} l2={worstSeries(nodes, 'putP50Ms')} color="amber" /> },
        ]}
      />
      <div className="cx-grid2 cx-mt">
        <div className="cx-stack">
          <Panel
            title="Requests by component"
            src={<Src>getNodeMetrics · storeComponents</Src>}
            right={<span className="muted sm">last {windowMin || 3} min · all nodes</span>}
            foot="Monthly figures here are before the free tier. Spend follows nodes and shards more than traffic: every shard polls its SlateDB manifest and every node renews its lease, idle or not."
          >
            <DataTable
              compact
              rows={comps}
              rowKey={(r) => r.component}
              open={(r) => ({ type: 'storecomp', id: r.component })}
              empty={<div className="cx-empty">No requests counted yet.</div>}
              cols={[
                {
                  id: 'c',
                  label: 'Component',
                  render: (r) => (
                    <span title={COMPONENTS[r.component]?.what}>
                      <b>{componentName(r.component)}</b> <span className="muted mono sm">{r.component}</span>
                    </span>
                  ),
                },
                { id: 'a', label: 'A/s', r: true, sort: (x, y) => x.a - y.a, render: (r) => <span className="mono">{fmtNum(r.a, 2)}</span> },
                { id: 'b', label: 'B/s', r: true, sort: (x, y) => x.b - y.b, render: (r) => <span className="mono">{fmtNum(r.b, 2)}</span> },
                {
                  id: 'share',
                  label: 'Share of spend',
                  render: (r) => (
                    <span className="cx-cellid">
                      <Meter v={spendOf(r)} max={spendAll} k="info" />
                      <span className="mono sm">{Math.round((spendOf(r) / spendAll) * 100)}%</span>
                    </span>
                  ),
                },
                { id: 'usd', label: '$/month', r: true, sort: (x, y) => spendOf(x) - spendOf(y), render: (r) => <span className="mono">{usd(componentUsd(priceAt, r.a, r.b, month.length))}</span> },
              ]}
            />
          </Panel>
          <Panel title="Latency" src={<Src>getNodeMetrics · putP50Ms, putP99Ms</Src>} foot="Segment PUTs run until durable, hedges and retries included. A commit is acked only once its segment is.">
            <Minis n={Math.min(3, Math.max(1, nodes.length))}>
              {nodes.map((n) => (
                <Mini
                  key={n.node}
                  label={
                    <>
                      <Swatch color={color(n.node)} /> {n.node} PUT p99
                    </>
                  }
                  value={fmtMs(n.latest?.putP99Ms)}
                >
                  <Spark data={nodeSeries(n, 'putP99Ms')} l2={nodeSeries(n, 'putP50Ms')} color="amber" />
                </Mini>
              ))}
            </Minis>
          </Panel>
        </div>
        <div className="cx-stack">
          <Panel
            title="What this traffic would cost elsewhere"
            src={<Src>list prices · USD</Src>}
            foot={
              <>
                At the last {windowMin || 3} minutes' average: {fmtNum(cur.mA, 1)} M class A and {fmtNum(cur.mB, 1)} M class B this month. R2's free tier (1 M A, 10 M B, 10 GB) is shared by the
                whole Cloudflare account.{' '}
                <label className="nowrap">
                  GB stored{' '}
                  <input
                    className="cx-inp mono"
                    style={{ width: 80, height: 24, display: 'inline-block' }}
                    inputMode="decimal"
                    placeholder="—"
                    aria-label="GB stored"
                    value={gb ?? ''}
                    onChange={(e) => {
                      const v = e.target.value === '' ? undefined : Number(e.target.value)
                      setGb(v)
                      try {
                        if (v === undefined) localStorage.removeItem(GB_KEY)
                        else localStorage.setItem(GB_KEY, String(v))
                      } catch {
                        /* per-tab */
                      }
                    }}
                  />
                </label>{' '}
                <span className="muted">(the console can't list the bucket to measure it)</span>
              </>
            }
          >
            <DataTable
              compact
              rows={(['R2', 'S3', 'GCS'] as Provider[]).map((p) => ({ p, c: monthCost(p, aAvg, bAvg, gb, month.length) }))}
              rowKey={(r) => r.p}
              onRow={(r) => setHl(r.p)}
              dim={(r) => r.p !== priceAt}
              cols={[
                {
                  id: 'p',
                  label: 'Provider',
                  render: (r) => (
                    <span className="cx-cellid">
                      <b>{PRICE[r.p].name}</b>
                      {r.p === prov.p && <Chip k="acc">current</Chip>}
                    </span>
                  ),
                },
                { id: 'a', label: 'Class A', r: true, render: (r) => <span className="mono">{usd(r.c.ca)}</span> },
                { id: 'b', label: 'Class B', r: true, render: (r) => <span className="mono">{usd(r.c.cb)}</span> },
                { id: 's', label: 'Storage', r: true, render: (r) => <span className="mono">{r.c.cs === undefined ? <span className="muted">${PRICE[r.p].gb}/GB</span> : usd(r.c.cs)}</span> },
                { id: 'e', label: 'Egress', render: (r) => <span className="t2 sm">{PRICE[r.p].egress}</span> },
                { id: 't', label: 'Month', r: true, render: (r) => <b className="mono">{usd(r.c.total)}</b> },
              ]}
            />
          </Panel>
          <Panel title="By node" src={<Src>getNodeMetrics · latest 10 s</Src>}>
            <DataTable
              compact
              rows={byNode}
              rowKey={(r) => r.n.node}
              open={(r) => ({ type: 'node', id: r.n.node })}
              cols={[
                {
                  id: 'n',
                  label: 'Node',
                  render: (r) => (
                    <span className="cx-cellid">
                      <Swatch color={color(r.n.node)} />
                      <span className="mono">{r.n.node}</span>
                    </span>
                  ),
                },
                { id: 'a', label: 'A/s', r: true, sort: (x, y) => x.a - y.a, render: (r) => <span className="mono">{fmtNum(r.a, 2)}</span> },
                { id: 'b', label: 'B/s', r: true, sort: (x, y) => x.b - y.b, render: (r) => <span className="mono">{fmtNum(r.b, 2)}</span> },
                {
                  id: 'e',
                  label: 'Errors/s',
                  r: true,
                  render: (r) =>
                    r.err > 0 ? (
                      <span className="s-err">
                        <Glyph k="err" /> {fmtSi(r.err)}
                      </span>
                    ) : (
                      <span className="muted">0</span>
                    ),
                },
                { id: 'p', label: 'PUT p99', r: true, render: (r) => <span className="mono">{fmtMs(r.put)}</span> },
              ]}
            />
          </Panel>
          <Panel title="Bucket">
            <PanelBody>
              <KV
                rows={[
                  ['Provider', prov.label],
                  ['Endpoint', endpoint ? <span className="mono sm">{endpoint}</span> : '—'],
                  ['Bucket', <span className="mono">{setting('--s3-bucket') ?? '—'}</span>],
                  ['Prefix', <span className="mono">{setting('--prefix') ?? '(none)'}</span>],
                  ['Region', <span className="mono">{setting('--s3-region') ?? '—'}</span>],
                  ['Log retention', <span className="mono">{setting('--log-retention') ?? '—'}</span>],
                  ['Hedge after', <span className="mono">{setting('--hedge-after-ms') ? `${setting('--hedge-after-ms')} ms` : '—'}</span>],
                ]}
              />
              <p className="muted sm" style={{ margin: '10px 0 0' }}>
                Object counts and sizes by prefix would mean listing the bucket, so the console doesn't show them. Never edit or delete objects by hand: <span className="mono">assign/</span> and{' '}
                <span className="mono">nodes/</span> are how nodes agree on ownership.
              </p>
            </PanelBody>
          </Panel>
        </div>
      </div>
    </>
  )
}
