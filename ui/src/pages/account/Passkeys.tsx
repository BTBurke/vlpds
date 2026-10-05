import { useEffect, useRef, useState, type ReactNode } from 'react'
import { CopyText, Empty, ErrorNotice, Field, Loading, Notice, Panel, Spinner, Status, saveBlob } from '../../components/ui'
import { Download } from '../../components/icons'
import { fmtTime } from '../../lib/format'
import { useAction, useLoad, useSession } from '../../lib/hooks'
import { acall } from '../../lib/xrpc'
import { cancelled, createPasskey, passkeysHere } from '../../lib/webauthn'

type Passkey = {
  id: string
  name: string
  createdAt: string
  lastUsedAt?: string
  synced: boolean
  backedUp: boolean
  passwordless: boolean
  suspectAt?: string
}
type List = { passkeys: Passkey[]; max: number; passwordlessAvailable: boolean; rpId: string; origin: string }

/** Passkeys on the Security page: add (password, then the browser's prompt), rename, remove. `onChange`: the second factor may have changed. */
export function Passkeys({ onChange, totpOn }: { onChange: () => void; totpOn?: boolean }) {
  const list = useLoad<List>(() => acall('vlpds.server.listPasskeys'), [])
  const [adding, setAdding] = useState(false)
  const [pw, setPw] = useState('')
  const [name, setName] = useState('')
  const [added, setAdded] = useState<string>()
  const [renaming, setRenaming] = useState<Passkey>()
  const [newName, setNewName] = useState('')
  const [removing, setRemoving] = useState<Passkey>()
  const [rpw, setRpw] = useState('')
  const [everywhere, setEverywhere] = useState(false)
  const [codes, setCodes] = useState<string[]>()
  const s = useSession()!

  const add = useAction(async () => {
    const options = await acall('vlpds.server.startPasskeyRegistration', { body: { password: pw } })
    let credential
    try {
      credential = await createPasskey(options)
    } catch (e) {
      if (cancelled(e)) throw new Error('The passkey prompt was closed before a passkey was saved.')
      throw e
    }
    const r = await acall('vlpds.server.finishPasskeyRegistration', { body: { name: name.trim() || defaultName(), credential } })
    setAdded(r.name)
    if (r.recoveryCodes?.length) setCodes(r.recoveryCodes)
    setAdding(false)
    setPw('')
    setName('')
    list.reload()
    onChange()
  })
  const rename = useAction(async () => {
    await acall('vlpds.server.renamePasskey', { body: { id: renaming!.id, name: newName.trim() } })
    setRenaming(undefined)
    list.reload()
  })
  const remove = useAction(async () => {
    await acall('vlpds.server.removePasskey', { body: { id: removing!.id, password: rpw, signOutEverywhere: everywhere } })
    setRemoving(undefined)
    setRpw('')
    setEverywhere(false)
    list.reload()
    onChange()
  })

  const d = list.data
  const here = passkeysHere(d?.origin)
  const keys = d?.passkeys ?? []
  if (codes) return <SavedCodes codes={codes} handle={s.handle} onDone={() => setCodes(undefined)} />
  return (
    <Panel
      title="Passkeys"
      desc="Sign in with your fingerprint, face or screen lock, or a security key, instead of a code. A passkey only works on this server's own sign-in pages, so a fake site can't trick you into handing it over."
      id="passkeys"
      flush={keys.length > 0 && !adding}
    >
      <ErrorNotice error={list.error} />
      {added && (
        <div style={{ padding: keys.length ? '12px 18px 0' : 0 }}>
          <Notice kind="ok">
            <b>{added}</b> added. You'll be asked for a passkey or a code when you sign in with your password.
          </Notice>
        </div>
      )}
      {!d ? (
        !list.error && <Loading />
      ) : (
        <>
          {keys.length === 0 ? (
            !adding && <Empty title="No passkeys">Adding one turns on two-factor sign-in for your password.</Empty>
          ) : (
            !adding && (
              <div className="table-wrap">
                <table className="data">
                  <thead>
                    <tr>
                      <th>Passkey</th>
                      <th className="hide-sm">Added</th>
                      <th className="hide-sm">Last used</th>
                      <th />
                    </tr>
                  </thead>
                  <tbody>
                    {keys.map((k) => (
                      <tr key={k.id}>
                        <td>
                          <b className="break">{k.name}</b>{' '}
                          {k.suspectAt && <Status kind="bad">Refused: looks copied</Status>}
                          <div className="small muted">
                            {k.synced ? 'Synced passkey' : 'Security key or single device'}
                            {k.passwordless ? ' · can sign in without your password' : ' · second step only'}
                          </div>
                        </td>
                        <td className="nowrap hide-sm">{fmtTime(k.createdAt)}</td>
                        <td className="nowrap hide-sm">{k.lastUsedAt ? fmtTime(k.lastUsedAt) : 'never'}</td>
                        <td className="num nowrap">
                          <button
                            className="btn sm"
                            onClick={() => {
                              setRenaming(k)
                              setNewName(k.name)
                            }}
                          >
                            Rename
                          </button>{' '}
                          <button className="btn danger sm" onClick={() => setRemoving(k)}>
                            Remove
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )
          )}
          <div style={{ padding: keys.length && !adding ? '12px 18px 16px' : 0 }}>
            {keys.some((k) => k.suspectAt) && (
              <Notice kind="warn">A passkey reported a signature count older than one we'd seen, which means it may have been copied. It's refused until you remove it.</Notice>
            )}
            {!d.passwordlessAvailable && <Notice>Your DID is too long to be stored in a passkey, so passkeys here are a second step after your password, not a replacement for it.</Notice>}
            {totpOn && keys.length >= 2 && (
              <p className="small muted">
                A fake site can still trick you into typing an authenticator code. With two passkeys set up, you could turn the authenticator app off.
              </p>
            )}
            {!here ? (
              <p className="small muted" style={{ margin: 0 }}>
                {typeof window.PublicKeyCredential !== 'function'
                  ? "This browser doesn't support passkeys."
                  : `Passkeys only work at ${d.origin}. Open the account page there to add one.`}
              </p>
            ) : adding ? (
              <form
                onSubmit={(e) => {
                  e.preventDefault()
                  add.run()
                }}
              >
                <ErrorNotice error={add.error} />
                <Field label="Name" hint="So you can tell your passkeys apart, e.g. “MacBook” or “YubiKey”.">
                  <input type="text" value={name} onChange={(e) => setName(e.target.value)} maxLength={64} placeholder={defaultName()} />
                </Field>
                <Field label="Password" hint="Your password confirms it's you before a new way to sign in is added.">
                  <input type="password" value={pw} onChange={(e) => setPw(e.target.value)} autoComplete="current-password" required autoFocus />
                </Field>
                <div className="row">
                  <button type="button" className="btn" onClick={() => setAdding(false)}>
                    Cancel
                  </button>
                  <button className="btn primary" disabled={add.busy}>
                    {add.busy && <Spinner />}
                    Continue
                  </button>
                </div>
              </form>
            ) : (
              <div className="row">
                <span className="small muted" style={{ flex: 1 }}>
                  Passkeys stay on this server. If you move your account to another server, set up new ones there.
                </span>
                <button
                  className="btn primary"
                  disabled={keys.length >= d.max}
                  onClick={() => {
                    setAdded(undefined)
                    setAdding(true)
                  }}
                >
                  Add a passkey
                </button>
              </div>
            )}
          </div>
        </>
      )}
      {renaming && (
        <Dialog title="Rename passkey" onClose={() => setRenaming(undefined)}>
          <form
            onSubmit={(e) => {
              e.preventDefault()
              rename.run()
            }}
          >
            <ErrorNotice error={rename.error} />
            <Field label="Name">
              <input type="text" value={newName} onChange={(e) => setNewName(e.target.value)} maxLength={64} required autoFocus />
            </Field>
            <div className="row end">
              <button type="button" className="btn" onClick={() => setRenaming(undefined)}>
                Cancel
              </button>
              <button className="btn primary" disabled={rename.busy}>
                Save
              </button>
            </div>
          </form>
        </Dialog>
      )}
      {removing && (
        <Dialog title={`Remove “${removing.name}”?`} onClose={() => setRemoving(undefined)}>
          <form
            onSubmit={(e) => {
              e.preventDefault()
              remove.run()
            }}
          >
            <p>Anything this passkey signed in to is signed out. You can't use it on this account again unless you add it back.</p>
            <ErrorNotice error={remove.error} />
            <Field label="Password">
              <input type="password" value={rpw} onChange={(e) => setRpw(e.target.value)} autoComplete="current-password" required autoFocus />
            </Field>
            <label className="check">
              <input type="checkbox" checked={everywhere} onChange={(e) => setEverywhere(e.target.checked)} />
              <span>
                Sign out everywhere
                <span className="small muted" style={{ display: 'block' }}>
                  Every session, app and trusted browser on this account, including this page. Do this if the passkey was lost or stolen.
                </span>
              </span>
            </label>
            <div className="row end">
              <button type="button" className="btn" onClick={() => setRemoving(undefined)}>
                Cancel
              </button>
              <button className="btn danger solid" disabled={remove.busy}>
                {remove.busy && <Spinner />}
                Remove passkey
              </button>
            </div>
          </form>
        </Dialog>
      )}
    </Panel>
  )
}

/** Shown once, when a set is issued (the first passkey or authenticator app) or regenerated. */
export function SavedCodes({ codes, handle, onDone }: { codes: string[]; handle: string; onDone: () => void }) {
  return (
    <Panel title="Save your recovery codes" desc="Each code works once, in place of a passkey or an authenticator code, if you lose them. This is the only time they are shown.">
      <Notice kind="ok">Two-factor sign-in is on.</Notice>
      <ol className="codes">
        {codes.map((c) => (
          <li key={c}>{c}</li>
        ))}
      </ol>
      <div className="row">
        <button className="btn" onClick={() => saveBlob(new Blob([codes.join('\n') + '\n'], { type: 'text/plain' }), `${handle}-recovery-codes.txt`)}>
          <Download />
          Download as text
        </button>
        <CopyText text={codes.join('\n')} display="Copy all" mono={false} />
        <div style={{ flex: 1 }} />
        <button className="btn primary" onClick={onDone}>
          I've saved them
        </button>
      </div>
    </Panel>
  )
}

/** How many recovery codes are left, and a fresh set behind the password. `ver`: reload when the factors change. */
export function RecoveryCodes({ ver }: { ver: number }) {
  const st = useLoad<{ recoveryCodesRemaining: number }>(() => acall('vlpds.server.listPasskeys'), [ver])
  const s = useSession()!
  const [asking, setAsking] = useState(false)
  const [pw, setPw] = useState('')
  const [codes, setCodes] = useState<string[]>()
  const regen = useAction(async () => {
    const r = await acall('vlpds.server.regenerateRecoveryCodes', { body: { password: pw } })
    setCodes(r.recoveryCodes)
    setAsking(false)
    setPw('')
    st.reload()
  })
  if (codes) return <SavedCodes codes={codes} handle={s.handle} onDone={() => setCodes(undefined)} />
  const left = st.data?.recoveryCodesRemaining ?? 0
  if (!st.data || (left === 0 && !asking)) return null
  return (
    <Panel title="Recovery codes" desc="One set of codes for your passkeys and authenticator app. Each works once if you lose them." id="recovery-codes">
      <ErrorNotice error={regen.error} />
      {!asking ? (
        <div className="row">
          <span style={{ flex: 1 }}>
            <b>{left}</b> left{left < 3 && <span className="muted"> — get a new set before you run out</span>}
          </span>
          <button className="btn" onClick={() => setAsking(true)}>
            Get new codes
          </button>
        </div>
      ) : (
        <form
          onSubmit={(e) => {
            e.preventDefault()
            regen.run()
          }}
        >
          <Field label="Password" hint="The codes you have now stop working.">
            <input type="password" value={pw} onChange={(e) => setPw(e.target.value)} autoComplete="current-password" required autoFocus />
          </Field>
          <div className="row">
            <button type="button" className="btn" onClick={() => setAsking(false)}>
              Cancel
            </button>
            <button className="btn primary" disabled={regen.busy}>
              {regen.busy && <Spinner />}
              Get new codes
            </button>
          </div>
        </form>
      )}
    </Panel>
  )
}

function defaultName() {
  const ua = navigator.userAgent
  if (/iPhone/.test(ua)) return 'iPhone'
  if (/iPad/.test(ua)) return 'iPad'
  if (/Android/.test(ua)) return 'Android'
  if (/Macintosh/.test(ua)) return 'Mac'
  if (/Windows/.test(ua)) return 'Windows'
  return 'Passkey'
}

function Dialog({ title, onClose, children }: { title: string; onClose: () => void; children: ReactNode }) {
  const ref = useRef<HTMLDialogElement>(null)
  useEffect(() => {
    const d = ref.current
    if (d && !d.open) d.showModal()
  }, [])
  return (
    <dialog ref={ref} className="modal" onClose={onClose} aria-labelledby="pk-dialog-title">
      <div className="inner">
        <h2 id="pk-dialog-title">{title}</h2>
        {children}
      </div>
    </dialog>
  )
}
