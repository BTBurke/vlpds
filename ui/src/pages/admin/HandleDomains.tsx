import { useState } from 'react'
import { CopyValue, ErrorNotice, Field, Loading, Notice, Panel, PartialNotice, partialOf, Spinner, type PartialResult } from '../../components/ui'
import { fmtNum, fmtTime } from '../../lib/format'
import { useAction, useLoad } from '../../lib/hooks'
import { admin, errText, XrpcError } from '../../lib/xrpc'

type Domain = { domain: string; primary: boolean; accounts: number | null; addedAt?: string; addedBy?: string }
type Listing = PartialResult & { primary: string; domains: Domain[]; updatedAt?: string; refreshSecs: number; countsPartial?: boolean }

export function HandleDomains() {
  // not polled: each load reads every account row in the cluster to count them
  const l = useLoad<Listing>(() => admin('vlpds.admin.listHandleDomains'), [])
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
      if (e instanceof XrpcError && e.error === 'DomainInUse') return setInUse({ domain, message: errText(e) })
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
      <>
        <ErrorNotice error={l.error} />
        {!l.error && <Loading />}
      </>
    )
  const busy = add.busy || remove.busy
  const typed = added.trim().toLowerCase().replace(/^\.+/, '')
  return (
    <>
      <div className="console-head">
        <h1>Handle domains</h1>
      </div>
      <p className="muted">
        Accounts can take a handle under any of these domains. The primary, <span className="mono">{d.primary}</span>, comes from{' '}
        <span className="mono">--handle-domain</span> and is always served. Domains added here are stored in the bucket for the whole cluster. Each one needs DNS: a
        wildcard record pointing at the PDS, and a certificate (on-demand TLS covers it; a wildcard certificate needs the DNS-01 token to control the zone).
      </p>
      <ErrorNotice error={l.error || add.error || remove.error} />
      {notice && <Notice kind="ok">{notice}</Notice>}
      {inUse && (
        <Notice kind="warn">
          <p>{inUse.message}</p>
          <div className="row">
            <button type="button" className="btn sm danger" disabled={busy} onClick={() => forceRemove(inUse.domain)}>
              Remove anyway
            </button>
            <button type="button" className="btn sm quiet" onClick={() => setInUse(undefined)}>
              Keep it
            </button>
          </div>
        </Notice>
      )}
      {d.countsPartial && (
        <Notice kind="warn">Some nodes or shards didn’t answer, so the account counts may be low. Removing a domain needs “Remove anyway” until they do.</Notice>
      )}
      <PartialNotice partial={partialOf(d)} />
      <Panel title="Domains" desc={`Primary first. Nodes pick up changes within seconds (at most ${d.refreshSecs} s).`} flush>
        <div className="table-wrap">
          <table className="data">
            <thead>
              <tr>
                <th>Domain</th>
                <th className="num">Active accounts</th>
                <th>Added</th>
                <th>
                  <span className="sr-only">Actions</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {d.domains.map((x) => (
                <tr key={x.domain}>
                  <td>
                    <CopyValue text={x.domain} label={`Copy domain ${x.domain}`} /> {x.primary && <span className="pill accent">Primary</span>}
                  </td>
                  <td className="num">{x.accounts === null ? <span className="muted" title="Couldn't be counted">—</span> : fmtNum(x.accounts)}</td>
                  <td className="nowrap small">
                    {x.primary ? (
                      <span className="muted">--handle-domain</span>
                    ) : (
                      <>
                        {fmtTime(x.addedAt)}
                        {x.addedBy && <span className="muted"> by {x.addedBy}</span>}
                      </>
                    )}
                  </td>
                  <td className="nowrap">
                    <div className="row end">
                      <button
                        type="button"
                        className="btn sm danger"
                        disabled={busy || x.primary}
                        title={x.primary ? 'The primary comes from --handle-domain' : undefined}
                        onClick={() => remove.run(x.domain, false)}
                      >
                        Remove
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
      <Panel title="Add a domain">
        <form
          className="inline-form"
          onSubmit={(e) => {
            e.preventDefault()
            if (typed) add.run(typed)
          }}
        >
          <Field label="Domain" hint="Handles look like alice.<domain>. Set up its DNS first.">
            <input type="text" value={added} onChange={(e) => setAdded(e.target.value)} placeholder="at.example.org" autoCapitalize="none" spellCheck={false} required />
          </Field>
          <button className="btn primary" disabled={busy || !typed}>
            {add.busy && <Spinner />}
            Add domain
          </button>
        </form>
        {d.updatedAt && <p className="small muted">Last changed {fmtTime(d.updatedAt)}.</p>}
      </Panel>
    </>
  )
}
