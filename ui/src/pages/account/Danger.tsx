import { useState } from 'react'
import { Confirm, ErrorNotice, Field, Loading, Notice, PageHead, Panel, Spinner } from '../../components/ui'
import { useAction, useLoad, useSession } from '../../lib/hooks'
import { navigate } from '../../lib/router'
import { acall, call, setSession } from '../../lib/xrpc'
import { accountStatus, loadSession } from './Overview'

export function Danger() {
  const s = useSession()!
  const info = useLoad(loadSession, [])
  const [confirmDeactivate, setConfirmDeactivate] = useState(false)
  const deactivate = useAction(async () => {
    await acall('com.atproto.server.deactivateAccount', { body: {} })
    setConfirmDeactivate(false)
    info.reload()
  })
  const activate = useAction(async () => {
    await acall('com.atproto.server.activateAccount', { method: 'POST' })
    info.reload()
  })

  const [requested, setRequested] = useState(false)
  const [token, setToken] = useState('')
  const [password, setPassword] = useState('')
  const [confirmDelete, setConfirmDelete] = useState(false)
  const request = useAction(async () => {
    await acall('com.atproto.server.requestAccountDelete', { method: 'POST' })
    setRequested(true)
  })
  const del = useAction(async () => {
    await call('com.atproto.server.deleteAccount', { body: { did: s.did, password, token: token.trim() } })
    setSession(null)
    navigate('/')
  })

  const d = info.data
  return (
    <>
      <PageHead title="Deactivate or delete" desc="Take your account offline for a while, or remove it from this server for good." />
      <ErrorNotice error={info.error} />
      {!d ? (
        <Loading />
      ) : (
        <>
          <Panel
            title={d.active ? 'Deactivate account' : 'Reactivate account'}
            desc={
              d.active
                ? 'Your profile and posts disappear from apps until you reactivate. Nothing is deleted.'
                : 'Your account is offline. Reactivating publishes it to the network again.'
            }
          >
            <div className="row" style={{ marginBottom: 12 }}>
              <span className="small muted">Current status</span> {accountStatus(d)}
            </div>
            <ErrorNotice error={deactivate.error || activate.error} />
            {d.active ? (
              <button className="btn danger" onClick={() => setConfirmDeactivate(true)}>
                Deactivate account
              </button>
            ) : (
              <button className="btn primary" onClick={() => activate.run()} disabled={activate.busy}>
                {activate.busy && <Spinner />}
                Reactivate account
              </button>
            )}
          </Panel>

          <Panel title="Delete account" danger desc="Permanently removes your repository, media and identity from this server. This can't be undone.">
            <ErrorNotice error={request.error || del.error} />
            {!requested ? (
              <button className="btn danger" onClick={() => request.run()} disabled={request.busy}>
                {request.busy && <Spinner />}
                Email me a deletion code
              </button>
            ) : (
              <form
                onSubmit={(e) => {
                  e.preventDefault()
                  setConfirmDelete(true)
                }}
              >
                <Notice kind="warn">We emailed a deletion code to the address on @{s.handle}. Export your repository first if you want a copy.</Notice>
                <Field label="Deletion code">
                  <input type="text" value={token} onChange={(e) => setToken(e.target.value)} autoComplete="one-time-code" spellCheck={false} required autoFocus />
                </Field>
                <Field label="Password">
                  <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="current-password" required />
                </Field>
                <div className="row">
                  <button type="button" className="btn" onClick={() => setRequested(false)}>
                    Cancel
                  </button>
                  <button className="btn danger solid">Delete my account</button>
                </div>
              </form>
            )}
          </Panel>
        </>
      )}
      <Confirm
        open={confirmDeactivate}
        title="Deactivate your account?"
        action="Deactivate"
        danger
        busy={deactivate.busy}
        onConfirm={() => deactivate.run()}
        onClose={() => setConfirmDeactivate(false)}
      >
        Apps will show your account as unavailable. You can reactivate any time by signing in here.
      </Confirm>
      <Confirm
        open={confirmDelete}
        title={`Delete @${s.handle} permanently?`}
        action="Delete account forever"
        danger
        confirmText={s.handle}
        busy={del.busy}
        onConfirm={() => del.run()}
        onClose={() => setConfirmDelete(false)}
      >
        Your records, media and identity on this server are erased.
      </Confirm>
    </>
  )
}
