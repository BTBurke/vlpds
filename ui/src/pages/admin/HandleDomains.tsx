import { useState } from 'react'
import { ErrorNotice, Field, Loading, Notice, Panel, Spinner, Status } from '../../components/ui'
import { fmtTime } from '../../lib/format'
import { useAction, useLoad } from '../../lib/hooks'
import { admin } from '../../lib/xrpc'

type Domain = {
  domain: string
  source: 'config' | 'managed'
  state: 'active' | 'retiring'
  addedAt?: string
  retiringSince?: string
  removableAfter?: string
}
type Domains = { domains: Domain[]; retireGraceSecs: number }

function State({ d }: { d: Domain }) {
  if (d.state === 'active') return <Status kind="ok">In service</Status>
  return (
    <>
      <Status kind="warn">Retiring</Status>
      {d.removableAfter && <div className="small muted">Removable after {fmtTime(d.removableAfter)}</div>}
    </>
  )
}

export function HandleDomains() {
  const l = useLoad<Domains>(() => admin('vlpds.admin.getHandleDomains'), [], 10000)
  const [added, setAdded] = useState('')
  const [note, setNote] = useState<string>()
  const act = useAction(async (nsid: string, domain: string) => {
    setNote(undefined)
    const r = await admin(nsid, { body: { domain } })
    if (r.state === 'retiring')
      setNote(
        `${r.domain} gives out no new handles now. ${r.blockingAccounts ?? 'Some'} account(s) still hold one; remove it after ${fmtTime(r.removableAfter)} once none do.`,
      )
    else if (r.state === 'removed') setNote(`${r.domain} was removed.`)
    l.reload()
    return true
  })
  const d = l.data
  if (!d)
    return (
      <>
        <ErrorNotice error={l.error} />
        {!l.error && <Loading />}
      </>
    )
  const now = Date.now()
  return (
    <>
      <div className="console-head">
        <h1>Handle domains</h1>
      </div>
      <p className="muted">
        Handles are given out as <span className="mono">name.&lt;domain&gt;</span>. Domains from <span className="mono">--handle-domains</span> are fixed;
        the ones added here are stored in the bucket and reach every node within seconds. Each needs DNS for{' '}
        <span className="mono">*.&lt;domain&gt;</span> pointing at the PDS, like the first.
      </p>
      <ErrorNotice error={l.error || act.error} />
      {note && <Notice kind="info">{note}</Notice>}
      <Panel title="Domains" flush>
        <div className="table-wrap">
          <table className="data">
            <thead>
              <tr>
                <th>Domain</th>
                <th>Source</th>
                <th>State</th>
                <th>Added</th>
                <th>
                  <span className="sr-only">Actions</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {d.domains.map((x) => {
                const due = !x.removableAfter || Date.parse(x.removableAfter) <= now
                return (
                  <tr key={x.domain}>
                    <td className="mono">{x.domain}</td>
                    <td className="small">{x.source === 'config' ? '--handle-domains' : 'Added here'}</td>
                    <td className="nowrap" title={x.retiringSince ? `Retiring since ${fmtTime(x.retiringSince)}` : undefined}>
                      <State d={x} />
                    </td>
                    <td className="nowrap small">{x.addedAt ? fmtTime(x.addedAt) : <span className="muted">—</span>}</td>
                    <td className="nowrap">
                      {x.source === 'managed' && (
                        <div className="row end">
                          {x.state === 'active' ? (
                            <button className="btn sm danger" disabled={act.busy} onClick={() => act.run('vlpds.admin.removeHandleDomain', x.domain)}>
                              Retire
                            </button>
                          ) : (
                            <>
                              <button className="btn sm" disabled={act.busy} onClick={() => act.run('vlpds.admin.addHandleDomain', x.domain)}>
                                Put back in service
                              </button>
                              <button
                                className="btn sm danger"
                                disabled={act.busy || !due}
                                title={due ? undefined : `After ${fmtTime(x.removableAfter!)}`}
                                onClick={() => act.run('vlpds.admin.removeHandleDomain', x.domain)}
                              >
                                Remove
                              </button>
                            </>
                          )}
                        </div>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </Panel>
      <Panel title="Add a domain">
        <form
          className="inline-form"
          onSubmit={async (e) => {
            e.preventDefault()
            const v = added.trim()
            if (!v) return
            if (await act.run('vlpds.admin.addHandleDomain', v)) setAdded('')
          }}
        >
          <Field label="Domain" hint="Refused while an account's handle would come under it, or a node can't be asked.">
            <input type="text" value={added} onChange={(e) => setAdded(e.target.value)} placeholder="example.org" spellCheck={false} required />
          </Field>
          <button className="btn primary" disabled={act.busy || !added.trim()}>
            {act.busy && <Spinner />}
            Add domain
          </button>
        </form>
        <p className="small muted">
          Removing takes two steps: retiring stops new handles under it at once; after {Math.round(d.retireGraceSecs / 60)} minute(s) it can be removed,
          once no account holds a handle under it.
        </p>
      </Panel>
    </>
  )
}
