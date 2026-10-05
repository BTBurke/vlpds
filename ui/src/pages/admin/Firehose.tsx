import { useRef } from 'react'
import { Empty, ErrorNotice, Loading, Notice, Panel, Status } from '../../components/ui'
import { fmtBytes, fmtNum, fmtSi, relTime } from '../../lib/format'
import { useLoad } from '../../lib/hooks'
import { admin } from '../../lib/xrpc'

type Subscriber = {
  node: string
  conn: string
  /** False past the per-node labelled cap: its metrics count under conn="other". */
  labelled: boolean
  ip: string | null
  userAgent: string
  relay: string | null
  connectedAt: number
  cursor: number | null
  shard: string | null
  state: 'live' | 'backfilling'
  lastSeq: number
  events: number
  bytes: number
  lagBytes: number | null
  lagMs: number | null
  lagEvents: number | null
  disconnectedAt?: number
  reason?: string
}

type NodeRow = {
  node: string
  self: boolean
  reachable: boolean
  subscribers?: number
  backfilling?: number
  eventsEmitted?: number
  bytesSent?: number
}

type SubscriberList = {
  node: string
  total: number
  live: number
  backfilling: number
  subscribers: Subscriber[]
  recentDisconnects: Subscriber[]
  nodes: NodeRow[]
  unreachableNodes?: string[]
  time: number
}

type Sample = { t: number; events: number; bytes: number }

const REFRESH_MS = 5000

const REASONS: Record<string, string> = {
  client_gone: 'Connection dropped',
  client_closed: 'Client closed it',
  too_slow: 'Too slow (fell behind)',
  write_stalled: 'Stopped reading',
  future_cursor: 'Cursor in the future',
  backfill_failed: 'Backfill failed',
  kicked: 'Disconnected by the server',
  shutdown: 'Server shut down',
}

function dur(ms: number): string {
  const s = Math.max(0, Math.round(ms / 1000))
  if (s < 60) return `${s}s`
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`
  if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`
  return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`
}

function lag(s: Subscriber): string {
  if (s.lagMs != null) return s.lagMs < 1000 ? 'caught up' : `${dur(s.lagMs)} behind`
  if (s.lagEvents != null) return s.lagEvents === 0 ? 'caught up' : `${fmtNum(s.lagEvents)} events behind`
  return '—'
}

const key = (s: Subscriber) => `${s.node}/${s.conn}`

/** Per-second rates from the previous poll's counters (undefined on the first poll or after a reset). */
function useRates(d?: SubscriberList) {
  const prev = useRef<{ subs: Map<string, Sample>; nodes: Map<string, Sample> }>({ subs: new Map(), nodes: new Map() })
  const out = useRef<{ at?: number; subs: Map<string, number>; pds?: number; sendBytes?: number }>({ subs: new Map() })
  if (!d || out.current.at === d.time) return out.current
  const rate = (p: Sample | undefined, now: Sample, f: (s: Sample) => number) =>
    p && now.t > p.t && f(now) >= f(p) ? ((f(now) - f(p)) * 1000) / (now.t - p.t) : undefined
  const subs = new Map<string, Sample>()
  const subRates = new Map<string, number>()
  for (const s of d.subscribers) {
    const now = { t: d.time, events: s.events, bytes: s.bytes }
    subs.set(key(s), now)
    const r = rate(prev.current.subs.get(key(s)), now, (x) => x.events)
    if (r !== undefined) subRates.set(key(s), r)
  }
  const nodes = new Map<string, Sample>()
  let pds: number | undefined
  let sendBytes: number | undefined
  for (const n of d.nodes) {
    if (!n.reachable || n.eventsEmitted == null || n.bytesSent == null) continue
    const now = { t: d.time, events: n.eventsEmitted, bytes: n.bytesSent }
    nodes.set(n.node, now)
    const p = prev.current.nodes.get(n.node)
    // every node emits the whole merged stream: the PDS's rate is any one node's
    const e = rate(p, now, (x) => x.events)
    if (e !== undefined) pds = Math.max(pds ?? 0, e)
    const b = rate(p, now, (x) => x.bytes)
    if (b !== undefined) sendBytes = (sendBytes ?? 0) + b
  }
  prev.current = { subs, nodes }
  out.current = { at: d.time, subs: subRates, pds, sendBytes }
  return out.current
}

export function Firehose() {
  const c = useLoad<SubscriberList & { fetchedAt: number }>(
    async () => ({ ...(await admin('vlpds.admin.listFirehoseSubscribers')), fetchedAt: Date.now() }),
    [],
    REFRESH_MS,
  )
  const d = c.data
  const rates = useRates(d)
  const live = !!d && Date.now() - d.fetchedAt < REFRESH_MS * 2 && !c.error
  if (!d)
    return (
      <>
        <ErrorNotice error={c.error} />
        {!c.error && <Loading />}
      </>
    )
  const multi = d.nodes.length > 1
  return (
    <>
      <div className="console-head">
        <h1>Firehose subscribers</h1>
        <span className={`live${live ? '' : ' stale'}`} aria-live="polite">
          <i aria-hidden="true" />
          {live ? 'Live, every 5 s' : 'Not updating'}
        </span>
      </div>
      <ErrorNotice error={c.error} />
      {d.unreachableNodes && (
        <Notice kind="warn">
          Not listed: the subscribers of {d.unreachableNodes.join(', ')}, which didn't answer.
        </Notice>
      )}
      <div className="tiles">
        <div className="tile">
          <div className="v">{fmtNum(d.total)}</div>
          <div className="k">Subscribers{multi ? `, ${d.nodes.length} nodes` : ''}</div>
        </div>
        <div className="tile">
          <div className="v">
            {fmtNum(d.live)}
            <small>/ {fmtNum(d.backfilling)}</small>
          </div>
          <div className="k">Live / backfilling</div>
        </div>
        <div className="tile" title="Events this PDS adds to its firehose: a caught-up subscriber gets the same rate">
          <div className="v">
            {rates.pds !== undefined ? fmtSi(rates.pds) : '—'}
            <small>events/s</small>
          </div>
          <div className="k">This PDS's firehose</div>
        </div>
        <div className="tile" title="Websocket bytes written to every subscriber, all nodes">
          <div className="v">{rates.sendBytes !== undefined ? `${fmtBytes(rates.sendBytes)}/s` : '—'}</div>
          <div className="k">Sent to subscribers</div>
        </div>
      </div>

      <Panel
        title="Connected"
        desc={
          <>
            Oldest first{d.total > d.subscribers.length ? `, the first ${fmtNum(d.subscribers.length)} of ${fmtNum(d.total)}` : ''}. <b>Relay</b> names a
            configured relay whose hostname resolves to the client's address or appears in its user agent. <b>#conn</b> is the{' '}
            <span className="mono">conn</span> label on <span className="mono">vlpds_firehose_subscriber_events_total</span>.
          </>
        }
        flush
      >
        {d.subscribers.length === 0 ? (
          <Empty title="No subscribers">Nobody is connected to subscribeRepos{multi ? ' on any node' : ''} right now.</Empty>
        ) : (
          <div className="table-wrap">
            <table className="data compact firehose-subs">
              <thead>
                <tr>
                  <th>Conn</th>
                  <th>Client</th>
                  <th>User agent</th>
                  <th className="num">Connected</th>
                  <th>Cursor</th>
                  <th>State</th>
                  <th className="num">Lag</th>
                  <th className="num" title="Events sent per second, against this PDS's firehose rate">Events/s</th>
                  <th className="num">Events</th>
                  <th className="num">Bytes</th>
                </tr>
              </thead>
              <tbody>
                {d.subscribers.map((s) => {
                  const r = rates.subs.get(key(s))
                  const slow = s.state === 'live' && r !== undefined && rates.pds !== undefined && rates.pds > 1 && r < rates.pds * 0.9
                  return (
                    <tr key={key(s)}>
                      <td>
                        <span className="mono">#{s.conn}</span>
                        {!s.labelled && (
                          <span className="pill" title="Past the per-node cap: counted under conn=&quot;other&quot;">
                            other
                          </span>
                        )}
                        {multi && <div className="muted mono small">{s.node}</div>}
                      </td>
                      <td>
                        <span className="mono">{s.ip ?? 'unknown'}</span>
                        {s.relay && (
                          <div>
                            <span className="pill accent">{s.relay}</span>
                          </div>
                        )}
                      </td>
                      <td className="ua" title={s.userAgent}>
                        {s.userAgent || <span className="muted">none</span>}
                      </td>
                      <td className="num" title={new Date(s.connectedAt).toLocaleString()}>
                        {dur(d.time - s.connectedAt)}
                      </td>
                      <td className="mono">
                        {s.cursor != null ? s.cursor : <span className="muted">live</span>}
                        {s.shard && <div className="muted small">shard {s.shard}</div>}
                      </td>
                      <td>{s.state === 'live' ? <Status kind="ok">Live</Status> : <Status kind="warn">Backfilling</Status>}</td>
                      <td className="num">
                        {lag(s)}
                        {s.lagBytes ? <div className="muted small">{fmtBytes(s.lagBytes)} unsent</div> : null}
                      </td>
                      <td className="num">
                        {r !== undefined ? <span className={slow ? 'slow' : undefined}>{fmtSi(r)}</span> : '—'}
                        {rates.pds !== undefined && <div className="muted small">of {fmtSi(rates.pds)}</div>}
                      </td>
                      <td className="num">{fmtNum(s.events)}</td>
                      <td className="num">{fmtBytes(s.bytes)}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </Panel>

      <Panel title="Recently disconnected" desc={`The last ${multi ? '50 per node' : '50'}, newest first, with why they went.`} flush>
        {d.recentDisconnects.length === 0 ? (
          <Empty title="None yet">No subscriber has disconnected since the {multi ? 'nodes' : 'node'} started.</Empty>
        ) : (
          <div className="table-wrap">
            <table className="data compact firehose-subs">
              <thead>
                <tr>
                  <th>Conn</th>
                  <th>Client</th>
                  <th>User agent</th>
                  <th>Reason</th>
                  <th className="num">Stayed</th>
                  <th className="num">Events</th>
                  <th className="num">Left</th>
                </tr>
              </thead>
              <tbody>
                {d.recentDisconnects.map((s) => (
                  <tr key={key(s)}>
                    <td>
                      <span className="mono">#{s.conn}</span>
                      {multi && <div className="muted mono small">{s.node}</div>}
                    </td>
                    <td>
                      <span className="mono">{s.ip ?? 'unknown'}</span>
                      {s.relay && (
                        <div>
                          <span className="pill accent">{s.relay}</span>
                        </div>
                      )}
                    </td>
                    <td className="ua" title={s.userAgent}>
                      {s.userAgent || <span className="muted">none</span>}
                    </td>
                    <td>
                      {s.reason === 'too_slow' || s.reason === 'write_stalled' || s.reason === 'backfill_failed' ? (
                        <Status kind="bad">{REASONS[s.reason]}</Status>
                      ) : (
                        <Status kind="idle">{REASONS[s.reason ?? ''] ?? s.reason}</Status>
                      )}
                    </td>
                    <td className="num">{dur((s.disconnectedAt ?? d.time) - s.connectedAt)}</td>
                    <td className="num">{fmtNum(s.events)}</td>
                    <td className="num">{s.disconnectedAt ? relTime(s.disconnectedAt) : '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
    </>
  )
}
