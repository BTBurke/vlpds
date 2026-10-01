import { useState } from 'react'
import { ErrorNotice, Field, Loading, Notice, PageHead, Panel, Spinner, Status } from '../../components/ui'
import { useAction, useLoad } from '../../lib/hooks'
import { acall, call } from '../../lib/xrpc'
import { loadSession } from './Overview'

export function Identity() {
  const info = useLoad(loadSession, [])
  const desc = useLoad(() => call('com.atproto.server.describeServer'), [])
  const d = info.data
  return (
    <>
      <PageHead title="Handle and email" desc="Your handle is how people find you; your email is used for sign-in recovery." />
      <ErrorNotice error={info.error} />
      {!d ? (
        <Loading />
      ) : (
        <>
          <HandleForm current={d.handle} domains={desc.data?.availableUserDomains ?? []} onDone={info.reload} />
          <EmailPanel email={d.email} confirmed={!!d.emailConfirmed} onDone={info.reload} />
        </>
      )}
    </>
  )
}

function HandleForm({ current, domains, onDone }: { current: string; domains: string[]; onDone: () => void }) {
  const [handle, setHandle] = useState('')
  const [done, setDone] = useState<string>()
  const domain = domains[0] ?? ''
  const full = handle.includes('.') ? handle.trim().replace(/^@/, '') : handle.trim() ? `${handle.trim()}${domain}` : ''
  const act = useAction(async () => {
    await acall('com.atproto.identity.updateHandle', { body: { handle: full } })
    setDone(full)
    setHandle('')
    onDone()
  })
  return (
    <Panel title="Handle" desc={<>Currently <b>@{current}</b>.</>} id="handle">
      {done && <Notice kind="ok">Your handle is now @{done}. Apps may take a few minutes to show it.</Notice>}
      <ErrorNotice error={act.error} />
      <form
        onSubmit={(e) => {
          e.preventDefault()
          act.run()
        }}
      >
        <Field
          label="New handle"
          action={
            <button className="btn primary" disabled={act.busy || !full}>
              {act.busy && <Spinner />}
              Change handle
            </button>
          }
          hint={
            <>
              A name ending in <span className="mono">{domain || '.your-domain'}</span>, or a domain you own with a matching DNS record.
              {full && full !== handle.trim() && (
                <>
                  {' '}
                  You'll be <b>@{full}</b>.
                </>
              )}
            </>
          }
        >
          <input type="text" value={handle} onChange={(e) => setHandle(e.target.value)} autoCapitalize="none" spellCheck={false} placeholder="alice" required />
        </Field>
      </form>
    </Panel>
  )
}

function EmailPanel({ email, confirmed, onDone }: { email?: string; confirmed: boolean; onDone: () => void }) {
  // confirmation
  const [codeSent, setCodeSent] = useState(false)
  const [code, setCode] = useState('')
  const send = useAction(async () => {
    await acall('com.atproto.server.requestEmailConfirmation', { method: 'POST' })
    setCodeSent(true)
  })
  const confirm = useAction(async () => {
    await acall('com.atproto.server.confirmEmail', { body: { email, token: code.trim() } })
    setCodeSent(false)
    setCode('')
    onDone()
  })
  // update
  const [stage, setStage] = useState<'idle' | 'form'>('idle')
  const [tokenRequired, setTokenRequired] = useState(false)
  const [newEmail, setNewEmail] = useState('')
  const [token, setToken] = useState('')
  const [updated, setUpdated] = useState<string>()
  const start = useAction(async () => {
    const r = await acall('com.atproto.server.requestEmailUpdate', { method: 'POST' })
    setTokenRequired(!!r.tokenRequired)
    setStage('form')
  })
  const update = useAction(async () => {
    await acall('com.atproto.server.updateEmail', { body: { email: newEmail.trim(), token: tokenRequired ? token.trim() : undefined } })
    setUpdated(newEmail.trim())
    setStage('idle')
    setNewEmail('')
    setToken('')
    onDone()
  })

  return (
    <Panel title="Email" id="email">
      <dl className="dl" style={{ marginBottom: 14 }}>
        <dt>Address</dt>
        <dd>{email ?? <span className="muted">None on file</span>}</dd>
        <dt>Status</dt>
        <dd>{confirmed ? <Status kind="ok">Confirmed</Status> : <Status kind="warn">Not confirmed</Status>}</dd>
      </dl>
      {updated && <Notice kind="ok">Email changed to {updated}. Confirm the new address to use it for recovery.</Notice>}

      {email && !confirmed && (
        <div className="stack" style={{ marginBottom: 18 }}>
          <h3>Confirm your address</h3>
          <ErrorNotice error={send.error || confirm.error} />
          {!codeSent ? (
            <div className="row">
              <button className="btn" onClick={() => send.run()} disabled={send.busy}>
                {send.busy && <Spinner />}
                Email me a confirmation code
              </button>
            </div>
          ) : (
            <form
              onSubmit={(e) => {
                e.preventDefault()
                confirm.run()
              }}
            >
              <Field
                label="Confirmation code"
                hint={`Sent to ${email}.`}
                action={
                  <button className="btn primary" disabled={confirm.busy}>
                    {confirm.busy && <Spinner />}
                    Confirm email
                  </button>
                }
              >
                <input type="text" value={code} onChange={(e) => setCode(e.target.value)} autoComplete="one-time-code" spellCheck={false} required autoFocus />
              </Field>
            </form>
          )}
        </div>
      )}

      <h3 style={{ marginBottom: 10 }}>Change address</h3>
      <ErrorNotice error={start.error || update.error} />
      {stage === 'idle' ? (
        <button className="btn" onClick={() => start.run()} disabled={start.busy}>
          {start.busy && <Spinner />}
          Change email
        </button>
      ) : (
        <form
          onSubmit={(e) => {
            e.preventDefault()
            update.run()
          }}
        >
          {tokenRequired && <Notice>We emailed a code to {email}. Enter it below to approve the change.</Notice>}
          <Field label="New email">
            <input type="email" value={newEmail} onChange={(e) => setNewEmail(e.target.value)} autoComplete="email" required autoFocus />
          </Field>
          {tokenRequired && (
            <Field label="Code from your current address">
              <input type="text" value={token} onChange={(e) => setToken(e.target.value)} autoComplete="one-time-code" spellCheck={false} required />
            </Field>
          )}
          <div className="row">
            <button type="button" className="btn" onClick={() => setStage('idle')}>
              Cancel
            </button>
            <button className="btn primary" disabled={update.busy}>
              {update.busy && <Spinner />}
              Save new email
            </button>
          </div>
        </form>
      )}
    </Panel>
  )
}
