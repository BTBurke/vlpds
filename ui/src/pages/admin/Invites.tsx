import { useEffect, useState } from 'react'
import { CopyText, Empty, ErrorNotice, Field, Loading, Notice, Panel, PartialNotice, partialOf, Spinner, type PartialResult } from '../../components/ui'
import { fmtTime } from '../../lib/format'
import { useAction } from '../../lib/hooks'
import { Link } from '../../lib/router'
import { admin } from '../../lib/xrpc'

type Code = { code: string; available: number; disabled: boolean; forAccount: string; createdBy: string; createdAt: string; uses: { usedBy: string; usedAt: string }[] }

export function Invites() {
  const [codes, setCodes] = useState<Code[]>([])
  const [cursor, setCursor] = useState<string>()
  const [loaded, setLoaded] = useState(false)
  const [sel, setSel] = useState<Set<string>>(new Set())
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
  const disable = useAction(async () => {
    await admin('com.atproto.admin.disableInviteCodes', { body: { codes: [...sel] } })
    setSel(new Set())
    page.run(undefined)
  })
  const toggle = (c: string) =>
    setSel((s) => {
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
              <div key={c}>
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
          <button className="btn danger sm" disabled={!sel.size || disable.busy} onClick={() => disable.run()}>
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
            <table className="data">
              <thead>
                <tr>
                  <th>
                    <span className="sr-only">Select</span>
                  </th>
                  <th>Code</th>
                  <th className="num">Remaining</th>
                  <th>Used by</th>
                  <th>For</th>
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>
                {codes.map((c) => (
                  <tr key={c.code}>
                    <td>
                      <input type="checkbox" aria-label={`Select ${c.code}`} checked={sel.has(c.code)} disabled={c.disabled} onChange={() => toggle(c.code)} />
                    </td>
                    <td className="nowrap">
                      <CopyText text={c.code} /> {c.disabled && <span className="pill danger">disabled</span>}
                      {!c.disabled && c.available > 0 && <InviteLinks code={c.code} />}
                    </td>
                    <td className="num">{c.available}</td>
                    <td className="small">
                      {c.uses.length === 0 ? (
                        <span className="muted">—</span>
                      ) : (
                        c.uses.map((u) => (
                          <div key={u.usedBy}>
                            <Link to={`/admin/accounts/${encodeURIComponent(u.usedBy)}`} className="mono">
                              {u.usedBy}
                            </Link>
                          </div>
                        ))
                      )}
                    </td>
                    <td className="mono small">{c.forAccount}</td>
                    <td className="nowrap">{fmtTime(c.createdAt)}</td>
                  </tr>
                ))}
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

function InviteLinks({ code }: { code: string }) {
  const q = `?invite=${encodeURIComponent(code)}`
  return (
    <div className="invite-links small">
      <CopyText text={`${location.origin}/migrate${q}`} display="Migrate link" label="Copy migrate link" mono={false} />
      <CopyText text={`${location.origin}/account/signup${q}`} display="Signup link" label="Copy signup link" mono={false} />
    </div>
  )
}
