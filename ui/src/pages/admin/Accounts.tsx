import { useEffect, useState } from 'react'
import { Confirm, CopyText, Empty, ErrorNotice, Field, JsonView, Loading, Notice, PageHead, Panel, PartialNotice, partialOf, Spinner, Status, type PartialResult } from '../../components/ui'
import { fmtTime } from '../../lib/format'
import { useAction, useLoad } from '../../lib/hooks'
import { Link, navigate } from '../../lib/router'
import { admin, call } from '../../lib/xrpc'

type InviteCode = { code: string; available: number; disabled: boolean; forAccount: string; createdBy: string; createdAt: string; uses: { usedBy: string; usedAt: string }[] }

export type AccountView = {
  did: string
  handle: string
  email?: string
  indexedAt: string
  emailConfirmedAt?: string
  deactivatedAt?: string
  deletionScheduledAt?: string
  invitesDisabled?: boolean
  invites?: InviteCode[]
  invitedBy?: InviteCode
}

export function Accounts() {
  const [q, setQ] = useState('')
  const [applied, setApplied] = useState('')
  const [rows, setRows] = useState<AccountView[]>([])
  const [cursor, setCursor] = useState<string>()
  const [loaded, setLoaded] = useState(false)
  const [partial, setPartial] = useState<PartialResult>()
  const page = useAction(async (email: string, cur?: string) => {
    const r = await admin('com.atproto.admin.searchAccounts', { params: { email, cursor: cur, limit: 50 } })
    setPartial(partialOf(r))
    setRows((x) => (cur ? [...x, ...r.accounts] : r.accounts))
    setCursor(r.accounts.length === 50 ? r.cursor : undefined)
    setLoaded(true)
  })
  useEffect(() => {
    page.run('', undefined)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])
  const [jump, setJump] = useState('')
  const lookup = useAction(async () => {
    const v = jump.trim().replace(/^@/, '')
    const did = v.startsWith('did:') ? v : (await call('com.atproto.identity.resolveHandle', { params: { handle: v } })).did
    navigate(`/admin/accounts/${encodeURIComponent(did)}`)
  })
  return (
    <>
      <div className="console-head">
        <h1>Accounts</h1>
      </div>
      <div className="grid2">
        <form
          className="toolbar"
          onSubmit={(e) => {
            e.preventDefault()
            setApplied(q)
            page.run(q.trim(), undefined)
          }}
        >
          <input type="search" placeholder="Email starts with…" value={q} onChange={(e) => setQ(e.target.value)} aria-label="Search accounts by email prefix" />
          <button className="btn">Search</button>
        </form>
        <form
          className="toolbar"
          onSubmit={(e) => {
            e.preventDefault()
            lookup.run()
          }}
        >
          <input type="search" placeholder="Handle or DID" value={jump} onChange={(e) => setJump(e.target.value)} aria-label="Open account by handle or DID" />
          <button className="btn" disabled={lookup.busy}>
            Open account
          </button>
        </form>
      </div>
      <ErrorNotice error={page.error || lookup.error} />
      <PartialNotice partial={partial} />
      <Panel flush>
        {!loaded ? (
          <Loading />
        ) : rows.length === 0 ? (
          <Empty title={applied ? `No accounts with an email starting “${applied}”` : 'No accounts yet'} />
        ) : (
          <div className="table-wrap">
            <table className="data">
              <thead>
                <tr>
                  <th>Handle</th>
                  <th>DID</th>
                  <th>Email</th>
                  <th>Created</th>
                  <th>State</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((a) => {
                  const to = `/admin/accounts/${encodeURIComponent(a.did)}`
                  return (
                    <tr key={a.did} className="link" onClick={() => navigate(to)}>
                      <td>
                        <Link to={to} onClick={(e) => e.stopPropagation()}>
                          <b>@{a.handle}</b>
                        </Link>
                      </td>
                      <td className="mono small">{a.did}</td>
                      <td className="break">
                        {a.email ?? <span className="muted">—</span>}
                        {a.email && !a.emailConfirmedAt && <span className="muted small"> (unconfirmed)</span>}
                      </td>
                      <td className="nowrap">{fmtTime(a.indexedAt)}</td>
                      <td>{a.deactivatedAt ? <Status kind="warn">Deactivated</Status> : <Status kind="ok">Active</Status>}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
      {cursor && (
        <div className="row end">
          <button className="btn" disabled={page.busy} onClick={() => page.run(applied.trim(), cursor)}>
            {page.busy && <Spinner />}
            Load more
          </button>
        </div>
      )}
    </>
  )
}

type SubjectStatus = { subject: any; takedown?: { applied: boolean; ref?: string }; deactivated?: { applied: boolean } }

export function AccountDetail({ did }: { did: string }) {
  const info = useLoad<AccountView>(() => admin('com.atproto.admin.getAccountInfo', { params: { did } }), [did])
  const subj = useLoad<SubjectStatus>(() => admin('com.atproto.admin.getSubjectStatus', { params: { did } }), [did])
  const a = info.data
  const reload = () => {
    info.reload()
    subj.reload()
  }
  return (
    <>
      <PageHead title={a ? `@${a.handle}` : did} crumbs={[{ to: '/admin/accounts', label: 'Accounts' }]} />
      <ErrorNotice error={info.error} />
      {!a ? (
        !info.error && <Loading />
      ) : (
        <div className="grid2">
          <div>
            <Panel title="Account">
              <dl className="dl">
                <dt>DID</dt>
                <dd>
                  <CopyText text={a.did} />
                </dd>
                <dt>Handle</dt>
                <dd>@{a.handle}</dd>
                <dt>Email</dt>
                <dd>
                  {a.email ?? '—'} {a.email && (a.emailConfirmedAt ? <Status kind="ok">Confirmed</Status> : <Status kind="warn">Unconfirmed</Status>)}
                </dd>
                <dt>Created</dt>
                <dd>{fmtTime(a.indexedAt)}</dd>
                <dt>Status</dt>
                <dd>
                  {subj.data?.takedown?.applied ? (
                    <Status kind="bad">Taken down</Status>
                  ) : a.deactivatedAt ? (
                    <>
                      <Status kind="warn">Deactivated {fmtTime(a.deactivatedAt)}</Status>{' '}
                      {a.deletionScheduledAt && <Status kind="bad">Scheduled for deletion on {fmtTime(a.deletionScheduledAt)}</Status>}
                    </>
                  ) : (
                    <Status kind="ok">Active</Status>
                  )}
                </dd>
                <dt>Invited with</dt>
                <dd className="mono small">{a.invitedBy?.code ?? '—'}</dd>
              </dl>
            </Panel>
            <Takedown did={did} status={subj.data} onDone={reload} />
            <Invites did={did} a={a} onDone={reload} />
          </div>
          <div>
            <Updates did={did} a={a} onDone={reload} />
            <SecondFactors did={did} handle={a.handle} />
            {a.email && <DevMail email={a.email} />}
            <DeleteAccount did={did} handle={a.handle} />
          </div>
        </div>
      )}
    </>
  )
}

function Takedown({ did, status, onDone }: { did: string; status?: SubjectStatus; onDone: () => void }) {
  const [ref, setRef] = useState('')
  const [confirm, setConfirm] = useState(false)
  const applied = !!status?.takedown?.applied
  const act = useAction(async (apply: boolean) => {
    await admin('com.atproto.admin.updateSubjectStatus', {
      body: { subject: { $type: 'com.atproto.admin.defs#repoRef', did }, takedown: { applied: apply, ref: apply ? ref.trim() || undefined : undefined } },
    })
    setConfirm(false)
    setRef('')
    onDone()
  })
  return (
    <Panel title="Takedown" danger={!applied} desc={applied ? `In effect${status?.takedown?.ref ? ` (ref ${status.takedown.ref})` : ''}. The repo is hidden and sessions are revoked.` : 'Hides the repo from the network and signs the account out everywhere.'}>
      <ErrorNotice error={act.error} />
      {applied ? (
        <button className="btn primary" onClick={() => act.run(false)} disabled={act.busy}>
          {act.busy && <Spinner />}
          Reverse takedown
        </button>
      ) : (
        <form
          onSubmit={(e) => {
            e.preventDefault()
            setConfirm(true)
          }}
        >
          <Field label="Reference (optional)" hint="A ticket or report id, kept with the takedown." action={<button className="btn danger">Take down</button>}>
            <input type="text" value={ref} onChange={(e) => setRef(e.target.value)} />
          </Field>
        </form>
      )}
      <Confirm open={confirm} title="Take down this account?" action="Take down" danger busy={act.busy} onConfirm={() => act.run(true)} onClose={() => setConfirm(false)}>
        The account's repo stops being served, a #account event goes out on the firehose, and all of its sessions are revoked.
      </Confirm>
    </Panel>
  )
}

function Updates({ did, a, onDone }: { did: string; a: AccountView; onDone: () => void }) {
  const [handle, setHandle] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [ok, setOk] = useState<string>()
  const act = useAction(async (what: 'handle' | 'email' | 'password') => {
    if (what === 'handle') await admin('com.atproto.admin.updateAccountHandle', { body: { did, handle: handle.trim() } })
    if (what === 'email') await admin('com.atproto.admin.updateAccountEmail', { body: { account: did, email: email.trim() } })
    if (what === 'password') await admin('com.atproto.admin.updateAccountPassword', { body: { did, password } })
    setOk(what === 'password' ? 'Password set. The account was signed out everywhere.' : `${what[0].toUpperCase()}${what.slice(1)} updated.`)
    setHandle('')
    setEmail('')
    setPassword('')
    onDone()
  })
  const form = (what: 'handle' | 'email' | 'password', label: string, input: JSX.Element, btn: string) => (
    <form
      onSubmit={(e) => {
        e.preventDefault()
        setOk(undefined)
        act.run(what)
      }}
    >
      <Field
        label={label}
        action={
          <button className="btn" disabled={act.busy}>
            {btn}
          </button>
        }
      >
        {input}
      </Field>
    </form>
  )
  return (
    <Panel title="Change details">
      {ok && <Notice kind="ok">{ok}</Notice>}
      <ErrorNotice error={act.error} />
      {form('handle', 'Handle', <input type="text" value={handle} onChange={(e) => setHandle(e.target.value)} placeholder={a.handle} autoCapitalize="none" spellCheck={false} required />, 'Set handle')}
      {form('email', 'Email', <input type="email" value={email} onChange={(e) => setEmail(e.target.value)} placeholder={a.email} required />, 'Set email')}
      {form('password', 'Password', <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="new-password" required />, 'Set password')}
    </Panel>
  )
}

function Invites({ did, a, onDone }: { did: string; a: AccountView; onDone: () => void }) {
  const act = useAction(async (enable: boolean) => {
    await admin(enable ? 'com.atproto.admin.enableAccountInvites' : 'com.atproto.admin.disableAccountInvites', { body: { account: did } })
    onDone()
  })
  return (
    <Panel
      title="Invites"
      desc={a.invitesDisabled ? 'This account cannot create invite codes.' : 'This account may create invite codes.'}
      actions={
        <button className="btn sm" onClick={() => act.run(!!a.invitesDisabled)} disabled={act.busy}>
          {a.invitesDisabled ? 'Allow invites' : 'Block invites'}
        </button>
      }
      flush={!!a.invites?.length}
    >
      <ErrorNotice error={act.error} />
      {!a.invites?.length ? (
        <p className="muted small" style={{ margin: 0 }}>
          No codes created for this account.
        </p>
      ) : (
        <table className="data">
          <tbody>
            {a.invites.map((c) => (
              <tr key={c.code}>
                <td className="mono small">{c.code}</td>
                <td className="num">
                  {c.uses.length}/{c.available + c.uses.length} used
                </td>
                <td>{c.disabled && <span className="pill danger">disabled</span>}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  )
}

/** For a user who lost every factor. Audited, and the user is mailed. */
function SecondFactors({ did, handle }: { did: string; handle: string }) {
  const [reason, setReason] = useState('')
  const [open, setOpen] = useState(false)
  const [done, setDone] = useState<{ passkeys: number; totp: boolean; trustedBrowsers: number }>()
  const act = useAction(async () => {
    const r = await admin('vlpds.admin.resetSecondFactors', { body: { did, reason: reason.trim() } })
    setDone(r.result)
    setOpen(false)
    setReason('')
  })
  return (
    <Panel
      title="Two-factor sign-in"
      desc="For someone who lost every passkey, their authenticator app and their recovery codes. Removes all of them and their trusted browsers; their password and email sign-in codes stay. Check who's asking first: it's recorded in the audit log and the user is emailed."
    >
      {done && (
        <Notice kind="ok">
          Reset: {done.passkeys} passkey{done.passkeys === 1 ? '' : 's'}, {done.totp ? 'the authenticator app, ' : ''}
          {done.trustedBrowsers} trusted browser{done.trustedBrowsers === 1 ? '' : 's'}.
        </Notice>
      )}
      <ErrorNotice error={act.error} />
      <form
        onSubmit={(e) => {
          e.preventDefault()
          setOpen(true)
        }}
      >
        <Field label="Reason" hint="Kept in the audit log, e.g. how you checked it was them." action={<button className="btn danger">Reset</button>}>
          <input type="text" value={reason} onChange={(e) => setReason(e.target.value)} maxLength={2000} required />
        </Field>
      </form>
      <Confirm open={open} title={`Reset two-factor sign-in for @${handle}?`} action="Reset" danger busy={act.busy} onConfirm={() => act.run()} onClose={() => setOpen(false)}>
        Their passkeys, authenticator app, recovery codes and trusted browsers are removed, and anything their passkeys signed in to is signed out. Until they set up two-factor again, they sign in with the password (and an emailed code, if they turned that on).
      </Confirm>
    </Panel>
  )
}

function DevMail({ email }: { email: string }) {
  const m = useLoad(() => admin('vlpds.admin.getDevMail', { params: { email } }), [email])
  if (m.error) return null // not in dev mode
  const msgs: any[] = m.data?.messages ?? []
  return (
    <Panel title="Dev mailbox" desc="Dev mode only: mail this server would have sent to the account." actions={<button className="btn sm" onClick={m.reload}>Refresh</button>}>
      {!m.data ? (
        <Loading />
      ) : msgs.length === 0 ? (
        <p className="muted small" style={{ margin: 0 }}>
          Nothing sent to {email} yet.
        </p>
      ) : (
        <>
          {m.data.token && (
            <p>
              Latest code: <CopyText text={m.data.token} />
            </p>
          )}
          <JsonView value={msgs.slice(-3).reverse()} />
        </>
      )}
    </Panel>
  )
}

function DeleteAccount({ did, handle }: { did: string; handle: string }) {
  const [open, setOpen] = useState(false)
  const act = useAction(async () => {
    await admin('com.atproto.admin.deleteAccount', { body: { did } })
    navigate('/admin/accounts')
  })
  return (
    <Panel title="Delete account" danger desc="Erases the repo, blobs and account record. There is no undo.">
      <ErrorNotice error={act.error} />
      <button className="btn danger" onClick={() => setOpen(true)}>
        Delete account
      </button>
      <Confirm open={open} title={`Delete @${handle}?`} action="Delete account" danger confirmText={handle} busy={act.busy} onConfirm={() => act.run()} onClose={() => setOpen(false)}>
        A #account event marks it deleted on the firehose.
      </Confirm>
    </Panel>
  )
}
