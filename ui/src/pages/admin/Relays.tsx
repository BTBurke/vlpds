import { useEffect, useState } from 'react'
import { AddRow, ConsolePage, CopyValue, Empty, ErrorNotice, Loading, Notice, Panel, Spinner, Status } from '../../components/ui'
import { fmtTime, relTime } from '../../lib/format'
import { useAction, useLoad } from '../../lib/hooks'
import { admin } from '../../lib/xrpc'

type RelayStatus = { lastAttemptMs: number; lastSuccessMs?: number; ok: boolean; httpStatus?: number; error?: string; node: string }
type Relay = { relay: string; url: string; status?: RelayStatus }
type Crawlers = {
  hostname: string
  relays: Relay[]
  intervalSecs: number
  relaysSource: 'stored' | 'flags'
  intervalSource: 'stored' | 'flags'
  flagRelays: string[]
  flagIntervalSecs: number
  updatedAt?: string
  node: string
  sender: boolean
}
type CrawlResult = { relay: string; ok: boolean; status?: number; error?: string }

function every(secs: number): string {
  if (secs % 3600 === 0) return secs === 3600 ? 'hour' : `${secs / 3600} hours`
  if (secs % 60 === 0) return secs === 60 ? 'minute' : `${secs / 60} minutes`
  return secs === 1 ? 'second' : `${secs} seconds`
}

function Result({ s }: { s?: RelayStatus }) {
  if (!s) return <Status kind="idle">Not asked yet</Status>
  if (s.ok) return <Status kind="ok">Accepted{s.httpStatus ? ` (${s.httpStatus})` : ''}</Status>
  return <Status kind="bad">{s.httpStatus ? `Rejected (${s.httpStatus})` : 'Unreachable'}</Status>
}

export function Relays() {
  const c = useLoad<Crawlers>(() => admin('vlpds.admin.getCrawlers'), [], 5000)
  const [added, setAdded] = useState('')
  const [minutes, setMinutes] = useState('')
  const [results, setResults] = useState<CrawlResult[]>()
  const d = c.data
  useEffect(() => {
    if (d && minutes === '') setMinutes(String(d.intervalSecs / 60))
  }, [d, minutes])

  const [adding, setAdding] = useState(false)
  const save = useAction(async (body: { relays?: string[] | null; intervalSecs?: number | null }, isAdd: boolean = false) => {
    setAdding(isAdd)
    await admin('vlpds.admin.setCrawlers', { body })
    c.reload()
    return true
  })
  const crawl = useAction(async (relays: string[]) => {
    const r = await admin('vlpds.admin.requestCrawl', { body: { relays } })
    setResults(r.results)
    c.reload()
  })

  if (!d)
    return (
      <ConsolePage title="Relays">
        <ErrorNotice error={c.error} />
        {!c.error && <Loading />}
      </ConsolePage>
    )
  const list = d.relays.map((r) => r.relay)
  const intervalSecs = Math.round(Number(minutes) * 60)
  const intervalValid = Number.isFinite(intervalSecs) && intervalSecs >= 1 && intervalSecs <= 7 * 24 * 3600
  const busy = save.busy || crawl.busy
  return (
    <ConsolePage
      title="Relays"
      intro={
        <>
          Relays are asked to crawl <span className="mono">{d.hostname}</span> at startup and after new activity, at most once every {every(d.intervalSecs)}{' '}
          per relay.
        </>
      }
      setupLabel="How requests are sent"
      setup={
        <p>
          Each request is a <span className="mono">com.atproto.sync.requestCrawl</span>. One node sends for the whole cluster, the owner of slot 0; that is{' '}
          {d.sender ? 'this node' : 'not this node'} (<span className="mono">{d.node}</span>). The relay list and interval are stored in the bucket once
          changed here, and override the nodes’ <span className="mono">--crawlers</span> and <span className="mono">--crawl-interval-secs</span>.
        </p>
      }
    >
      <ErrorNotice error={c.error || crawl.error || (!adding && save.error)} />
      {results && (
        <Notice kind={results.every((r) => r.ok) ? 'ok' : 'warn'}>
          {results.map((r) => (
            <div key={r.relay}>
              <span className="mono">{r.relay}</span>: {r.ok ? 'accepted' : `failed ${r.status ?? ''} ${r.error ?? ''}`}
            </div>
          ))}
        </Notice>
      )}
      <Panel
        title="Relays"
        desc={d.relaysSource === 'stored' ? 'Set in this console; overrides --crawlers.' : 'From --crawlers. Changing the list here stores it for the cluster.'}
        flush
        actions={
          <>
            {d.relaysSource === 'stored' && (
              <button className="btn sm" disabled={busy} onClick={() => save.run({ relays: null })} title={`--crawlers: ${d.flagRelays.join(', ') || 'none'}`}>
                Use --crawlers
              </button>
            )}
            <button className="btn sm primary" disabled={busy || list.length === 0} onClick={() => crawl.run([])}>
              {crawl.busy && <Spinner />}
              Crawl all now
            </button>
          </>
        }
      >
        {list.length === 0 ? (
          <Empty title="No relays">Nothing is told about this PDS: relays won’t crawl it until asked.</Empty>
        ) : (
          <div className="table-wrap">
            <table className="data fit">
              <thead>
                <tr>
                  <th>Relay</th>
                  <th>Last result</th>
                  <th>Asked</th>
                  <th>Accepted</th>
                  <th className="slack">
                    <span className="sr-only">Actions</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {d.relays.map((r) => (
                  <tr key={r.relay}>
                    <td>
                      <CopyValue text={r.relay} label={`Copy relay ${r.relay}`} title={r.url === r.relay ? undefined : `${r.url} (click to copy ${r.relay})`} />
                    </td>
                    <td>
                      <Result s={r.status} />
                      {r.status?.error && (
                        <span className="sub" title={r.status.error}>
                          {r.status.error}
                        </span>
                      )}
                    </td>
                    <td className="small" title={r.status ? `${fmtTime(r.status.lastAttemptMs)} by ${r.status.node}` : undefined}>
                      {r.status ? relTime(r.status.lastAttemptMs) : <span className="faint">—</span>}
                    </td>
                    <td className="small" title={r.status?.lastSuccessMs ? fmtTime(r.status.lastSuccessMs) : undefined}>
                      {r.status?.lastSuccessMs ? relTime(r.status.lastSuccessMs) : <span className="faint">—</span>}
                    </td>
                    <td className="slack">
                      <div className="row">
                        <button className="btn sm" disabled={busy} onClick={() => crawl.run([r.relay])}>
                          Crawl now
                        </button>
                        <button className="btn sm danger" disabled={busy} onClick={() => save.run({ relays: list.filter((x) => x !== r.relay) })}>
                          Remove
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        <div className="panel-foot roomy">
          <AddRow
            label="Add a relay"
            hint="A hostname (asked over https) or an http(s):// origin."
            error={adding && save.error}
            onSubmit={async () => {
              const v = added.trim()
              if (!v) return
              if (await save.run({ relays: [...list, v] }, true)) setAdded('')
            }}
            submit={
              <button className="btn primary" disabled={busy || !added.trim()}>
                Add relay
              </button>
            }
          >
            <input type="text" value={added} onChange={(e) => setAdded(e.target.value)} placeholder="bsky.network" autoCapitalize="none" spellCheck={false} required />
          </AddRow>
        </div>
      </Panel>
      <Panel title="Crawl interval">
        <AddRow
          label="Minimum interval (minutes)"
          hint={
            d.intervalSource === 'stored'
              ? `Set in this console; --crawl-interval-secs is ${d.flagIntervalSecs}.`
              : 'From --crawl-interval-secs. The reference PDS uses 20 minutes.'
          }
          onSubmit={() => intervalValid && save.run({ intervalSecs })}
          submit={
            <button className="btn primary" disabled={busy || !intervalValid || intervalSecs === d.intervalSecs}>
              {save.busy && <Spinner />}
              Save interval
            </button>
          }
          extra={
            d.intervalSource === 'stored' && (
              <button
                type="button"
                className="btn"
                disabled={busy}
                onClick={async () => {
                  await save.run({ intervalSecs: null })
                  setMinutes('')
                }}
              >
                Use flag
              </button>
            )
          }
        >
          <input type="number" min={1 / 60} step="any" value={minutes} onChange={(e) => setMinutes(e.target.value)} required />
        </AddRow>
      </Panel>
      {d.updatedAt && <p className="small faint">Last changed {fmtTime(d.updatedAt)}.</p>}
    </ConsolePage>
  )
}
