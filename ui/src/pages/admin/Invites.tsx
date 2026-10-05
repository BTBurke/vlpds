import { Fragment, useEffect, useState } from 'react'
import { CopyText, CopyValue, Empty, ErrorNotice, Field, Loading, Notice, Panel, PartialNotice, partialOf, Spinner, Status, type PartialResult } from '../../components/ui'
import { fmtTime } from '../../lib/format'
import { useAction } from '../../lib/hooks'
import { Link } from '../../lib/router'
import { admin } from '../../lib/xrpc'

/** `available` is the code's total uses, as the reference PDS reports it; what's left is `available - uses.length`. */
type Code = { code: string; available: number; disabled: boolean; forAccount: string; createdBy: string; createdAt: string; uses: { usedBy: string; usedAt: string }[] }

const left = (c: Code) => Math.max(0, c.available - c.uses.length)

export function Invites() {
  const [codes, setCodes] = useState<Code[]>([])
  const [cursor, setCursor] = useState<string>()
  const [loaded, setLoaded] = useState(false)
  const [sel, setSel] = useState<Set<string>>(new Set())
  const [open, setOpen] = useState<Set<string>>(new Set())
  const [partial, setPartial] = useState<PartialResult>()
  const page = useAction(async (cur?: string) => {
    const r = await admin('com.atproto.admin.getInviteCodes', { params: { sort: 'recent', limit: 100, cursor: cur } })
    setPartial(partialOf(r))
    setCodes((x) => (cur ? [...x, ...r.codes] : r.codes))
    setCursor(r.cursor)
    setLoaded(true)
  })
  useEffect(() => {
    page.run(undefined)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const [count, setCount] = useState(1)
  const [uses, setUses] = useState(1)
  const [forAccount, setFor] = useState('')
  const [created, setCreated] = useState<string[]>()
  const create = useAction(async () => {
    const r = await admin('com.atproto.server.createInviteCodes', {
      body: { codeCount: count, useCount: uses, forAccounts: forAccount.trim() ? [forAccount.trim()] : undefined },
    })
    setCreated(r.codes.flatMap((x: any) => x.codes))
    page.run(undefined)
  })
  const disable = useAction(async (codes: string[]) => {
    await admin('com.atproto.admin.disableInviteCodes', { body: { codes } })
    setSel((s) => new Set([...s].filter((c) => !codes.includes(c))))
    page.run(undefined)
  })
  const flip = (set: (f: (s: Set<string>) => Set<string>) => void, c: string) =>
    set((s) => {
      const n = new Set(s)
      if (n.has(c)) n.delete(c)
      else n.add(c)
      return n
    })

  return (
    <>
      <div className="console-head">
        <h1>Invite codes</h1>
      </div>
      <Panel title="Create codes">
        <ErrorNotice error={create.error} />
        {created && (
          <Notice kind="ok">
            <p>Created {created.length === 1 ? 'one code' : `${created.length} codes`}:</p>
            {created.map((c) => (
              <div key={c} className="row">
                <CopyText text={c} />
                <InviteLinks code={c} />
              </div>
            ))}
          </Notice>
        )}
        <form
          className="inline-form"
          onSubmit={(e) => {
            e.preventDefault()
            create.run()
          }}
        >
          <Field label="How many codes">
            <input type="number" min={1} max={100} value={count} onChange={(e) => setCount(Number(e.target.value))} required />
          </Field>
          <Field label="Uses per code">
            <input type="number" min={1} max={1000} value={uses} onChange={(e) => setUses(Number(e.target.value))} required />
          </Field>
          <Field label="For account (DID, optional)">
            <input type="text" value={forAccount} onChange={(e) => setFor(e.target.value)} placeholder="admin" spellCheck={false} />
          </Field>
          <button className="btn primary" disabled={create.busy}>
            {create.busy && <Spinner />}
            Create codes
          </button>
        </form>
      </Panel>
      <ErrorNotice error={page.error || disable.error} />
      <PartialNotice partial={partial} />
      <Panel
        title="All codes"
        desc="Newest first."
        flush
        actions={
          <button className="btn danger sm" disabled={!sel.size || disable.busy} onClick={() => disable.run([...sel])}>
            Disable {sel.size || ''} selected
          </button>
        }
      >
        {!loaded ? (
          <Loading />
        ) : codes.length === 0 ? (
          <Empty title="No invite codes yet" />
        ) : (
          <div className="table-wrap">
            <table className="data compact invites">
              <thead>
                <tr>
                  <th>
                    <span className="sr-only">Select</span>
                  </th>
                  <th>Code</th>
                  <th className="num" title="Uses left out of the code's total">
                    Left
                  </th>
                  <th>Created</th>
                  <th>Created by</th>
                  <th>Used by</th>
                  <th>Status</th>
                  <th>Links</th>
                  <th>
                    <span className="sr-only">Actions</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {codes.map((c) => {
                  const usable = !c.disabled && left(c) > 0
                  const expanded = open.has(c.code)
                  return (
                    <Fragment key={c.code}>
                      <tr>
                        <td>
                          <input type="checkbox" aria-label={`Select ${c.code}`} checked={sel.has(c.code)} disabled={c.disabled} onChange={() => flip(setSel, c.code)} />
                        </td>
                        <td>
                          <CopyValue text={c.code} label={`Copy invite code ${c.code}`} />
                        </td>
                        <td className="num">
                          {left(c)}
                          <span className="muted"> / {c.available}</span>
                        </td>
                        <td title={fmtTime(c.createdAt)}>{fmtTime(c.createdAt)}</td>
                        <td>
                          <Owner c={c} />
                        </td>
                        <td>
                          {c.uses.length === 0 ? (
                            <span className="muted">—</span>
                          ) : c.uses.length === 1 ? (
                            <AccountLink did={c.uses[0].usedBy} />
                          ) : (
                            <button type="button" className="btn sm" aria-expanded={expanded} onClick={() => flip(setOpen, c.code)}>
                              {c.uses.length} accounts <span aria-hidden="true">{expanded ? '▴' : '▾'}</span>
                            </button>
                          )}
                        </td>
                        <td>{c.disabled ? <Status kind="bad">Disabled</Status> : usable ? <Status kind="ok">Active</Status> : <Status kind="idle">Used up</Status>}</td>
                        <td>{usable ? <InviteLinks code={c.code} /> : <span className="muted">—</span>}</td>
                        <td>
                          {!c.disabled && (
                            <button type="button" className="btn sm danger" disabled={disable.busy} onClick={() => disable.run([c.code])}>
                              Disable
                            </button>
                          )}
                        </td>
                      </tr>
                      {expanded && (
                        <tr className="invite-uses">
                          <td />
                          <td colSpan={8}>
                            <span className="muted small">Used by </span>
                            {c.uses.map((u) => (
                              <span key={u.usedBy} title={`Used ${fmtTime(u.usedAt)}`}>
                                <AccountLink did={u.usedBy} />
                              </span>
                            ))}
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
      {cursor && (
        <div className="row end">
          <button className="btn" onClick={() => page.run(cursor)} disabled={page.busy}>
            Load more
          </button>
        </div>
      )}
    </>
  )
}

function AccountLink({ did }: { did: string }) {
  return (
    <Link to={`/admin/accounts/${encodeURIComponent(did)}`} className="mono did" title={did}>
      {did}
    </Link>
  )
}

/** "admin", an account (earned codes are created by the account itself), or both for a code the admin gave an account. */
function Owner({ c }: { c: Code }) {
  const who = (x: string) => (x.startsWith('did:') ? <AccountLink did={x} /> : <span>{x}</span>)
  if (c.createdBy === c.forAccount) return who(c.forAccount)
  return (
    <span className="owner">
      {who(c.createdBy)}
      <span className="muted"> for </span>
      {who(c.forAccount)}
    </span>
  )
}

function InviteLinks({ code }: { code: string }) {
  const q = `?invite=${encodeURIComponent(code)}`
  return (
    <span className="invite-links small">
      <CopyText text={`${location.origin}/migrate${q}`} display="Migrate" label="Copy migrate link" mono={false} />
      <CopyText text={`${location.origin}/account/signup${q}`} display="Sign-up" label="Copy sign-up link" mono={false} />
    </span>
  )
}
