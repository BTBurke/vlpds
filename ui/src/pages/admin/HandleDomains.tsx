import { Fragment, useState } from 'react'
import { AddRow, ConsolePage, CopyValue, ErrorNotice, Loading, Notice, Panel, PartialNotice, partialOf, Spinner, type PartialResult } from '../../components/ui'
import { fmtNum, fmtTime, relTime } from '../../lib/format'
import { useAction, useLoad } from '../../lib/hooks'
import { admin, errText, XrpcError } from '../../lib/xrpc'

type Domain = { domain: string; primary: boolean; accounts: number | null; addedAt?: string; addedBy?: string }
type Listing = PartialResult & { primary: string; domains: Domain[]; updatedAt?: string; refreshSecs: number; countsPartial?: boolean }

function Added({ x }: { x: Domain }) {
  if (x.primary) return <span className="faint">from --handle-domain</span>
  const ms = x.addedAt ? Date.parse(x.addedAt) : NaN
  if (isNaN(ms)) return <span className="faint">—</span>
  return (
    <span title={fmtTime(x.addedAt)}>
      {relTime(ms)}
      {x.addedBy && <span className="faint"> by {x.addedBy}</span>}
    </span>
  )
}

const POLL = 5000

export function HandleDomains() {
  const l = useLoad<Listing>(() => admin('vlpds.admin.listHandleDomains'), [], POLL)
  const [added, setAdded] = useState('')
  const [notice, setNotice] = useState<string>()
  const [inUse, setInUse] = useState<{ domain: string; message: string }>()
  const d = l.data

  const add = useAction(async (domain: string) => {
    await admin('vlpds.admin.addHandleDomain', { body: { domain } })
    setAdded('')
    setNotice(`Added ${domain}. Every node serves it within a few seconds.`)
    l.reload()
  })
  const remove = useAction(async (domain: string, force: boolean) => {
    setInUse(undefined)
    setNotice(undefined)
    try {
      const r = await admin('vlpds.admin.removeHandleDomain', { body: { domain, force: force || undefined } })
      setNotice(
        r.accounts
          ? `Removed ${domain}. ${r.accounts === 1 ? 'One account still has' : `${fmtNum(r.accounts)} accounts still have`} a handle under it.`
          : `Removed ${domain}.`,
      )
    } catch (e) {
      if (e instanceof XrpcError && e.error === 'DomainInUse') return setInUse({ domain, message: errText(e).replace(/; pass force to remove it anyway$/, '.') })
      throw e
    } finally {
      l.reload()
    }
  })
  const forceRemove = (domain: string) => {
    const ok = window.confirm(
      `Remove ${domain} anyway?\n\n` +
        'Accounts under it keep their handle, but it stops verifying (/.well-known/atproto-did answers 404), so other services no longer resolve it, and no new TLS certificates are issued for it. ' +
        'They should pick a new handle; you can rename them with updateAccountHandle.',
    )
    if (ok) remove.run(domain, true)
  }

  if (!d)
    return (
      <ConsolePage title="Handle domains">
        <ErrorNotice error={l.error} />
        {!l.error && <Loading />}
      </ConsolePage>
    )
  const busy = add.busy || remove.busy
  const typed = added.trim().toLowerCase().replace(/^\.+/, '')
  return (
    <ConsolePage
      title="Handle domains"
      intro={
        <>
          Accounts can take a handle under any of these domains. Every node picks up a change within seconds (at most {d.refreshSecs} s).
        </>
      }
      setup={
        <>
          <p>
            Each domain needs a wildcard DNS record (<span className="mono">*.example.org</span>) pointing at the PDS, and a certificate. On-demand TLS covers
            it; a wildcard certificate needs the DNS-01 token to control the zone.
          </p>
          <p>
            The primary, <span className="mono">{d.primary}</span>, comes from <span className="mono">--handle-domain</span> and is always served. Domains added
            here are stored in the bucket.
          </p>
        </>
      }
    >
      <ErrorNotice error={l.error || remove.error} />
      {notice && <Notice kind="ok">{notice}</Notice>}
      {d.countsPartial && (
        <Notice kind="warn">
          Some nodes or shards didn’t answer or are still loading their totals, so the account counts may be low. Removing a domain needs “Remove anyway” until
          they do.
        </Notice>
      )}
      <PartialNotice partial={partialOf(d)} />
      <Panel title="Domains" flush>
        <div className="table-wrap">
          <table className="data fit">
            <thead>
              <tr>
                <th>Domain</th>
                <th className="num" title="Active accounts with a handle under the domain">
                  Accounts
                </th>
                <th>Added</th>
                <th className="slack">
                  <span className="sr-only">Actions</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {d.domains.map((x) => {
                const alert = inUse?.domain === x.domain
                return (
                  <Fragment key={x.domain}>
                    <tr className={alert ? 'has-alert' : undefined}>
                      <td>
                        <span className="cell-main">
                          <CopyValue text={x.domain} label={`Copy domain ${x.domain}`} />
                          {x.primary && <span className="pill accent">Primary</span>}
                        </span>
                      </td>
                      <td className="num">{x.accounts === null ? <span className="faint" title="Couldn't be counted">—</span> : fmtNum(x.accounts)}</td>
                      <td className="small">
                        <Added x={x} />
                      </td>
                      <td className="slack">
                        {x.primary ? (
                          <span className="small faint" title="The primary comes from --handle-domain and can't be removed here">
                            Always served
                          </span>
                        ) : (
                          <button type="button" className="btn sm danger" disabled={busy} onClick={() => remove.run(x.domain, false)}>
                            Remove
                          </button>
                        )}
                      </td>
                    </tr>
                    {alert && (
                      <tr className="row-alert">
                        <td colSpan={4}>
                          <Notice kind="warn">
                            <p>
                              <b>Still in use.</b> {inUse.message}
                            </p>
                            <div className="row">
                              <button type="button" className="btn sm danger" disabled={busy} onClick={() => forceRemove(inUse.domain)}>
                                Remove anyway
                              </button>
                              <button type="button" className="btn sm quiet" onClick={() => setInUse(undefined)}>
                                Keep it
                              </button>
                            </div>
                          </Notice>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                )
              })}
            </tbody>
          </table>
        </div>
        <div className="panel-foot roomy">
          <AddRow
            label="Add a domain"
            hint={
              <>
                Handles look like <span className="mono">alice.{typed || 'example.org'}</span>. Set up its DNS first.
              </>
            }
            error={add.error}
            onSubmit={() => typed && add.run(typed)}
            submit={
              <button className="btn primary" disabled={busy || !typed}>
                {add.busy && <Spinner />}
                Add domain
              </button>
            }
          >
            <input
              type="text"
              value={added}
              onChange={(e) => {
                setAdded(e.target.value)
                if (add.error) add.setError(undefined)
              }}
              placeholder="at.example.org"
              autoCapitalize="none"
              spellCheck={false}
              required
            />
          </AddRow>
        </div>
      </Panel>
      {d.updatedAt && <p className="small faint">Last changed {fmtTime(d.updatedAt)}.</p>}
    </ConsolePage>
  )
}
