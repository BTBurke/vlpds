import { useState } from 'react'
import { ErrorNotice, Field, Loading, Notice, PageHead, Panel, Spinner, Status } from '../../components/ui'
import { useAction, useLoad } from '../../lib/hooks'
import { acall } from '../../lib/xrpc'
import { HandleChange } from './HandleChange'
import { loadSession } from './Overview'

export function Identity() {
  const info = useLoad(loadSession, [])
  const d = info.data
  // the domain this account may take a name under (several are possible)
  const home = useLoad<{ domain: string | null }>(() => acall('vlpds.identity.getHandleDomain'), [d?.handle])
  return (
    <>
      <PageHead title="Handle and email" desc="Your handle is how people find you; your email is used for sign-in recovery." />
      <ErrorNotice error={info.error} />
      {!d ? (
        <Loading />
      ) : (
        <>
          {home.data ? (
            <HandleChange current={d.handle} did={d.did} domain={home.data.domain ?? ''} onDone={info.reload} />
          ) : home.error ? (
            <ErrorNotice error={home.error} />
          ) : (
            <Loading />
          )}
          <EmailPanel email={d.email} confirmed={!!d.emailConfirmed} onDone={info.reload} />
        </>
      )}
    </>
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
