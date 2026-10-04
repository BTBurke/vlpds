import { useState, type FormEvent } from 'react'
import { ErrorNotice, Field, Notice, Spinner, Topbar } from '../../components/ui'
import { useSession } from '../../lib/hooks'
import { Link, match, navigate, useSearch } from '../../lib/router'
import { call, setSession, signOut, XrpcError, errText } from '../../lib/xrpc'
import { Overview } from './Overview'
import { Identity } from './Identity'
import { Security } from './Security'
import { Collections, Records, RecordView } from './Repo'
import { Blobs } from './Blobs'
import { Export, Preferences } from './Data'
import { Danger } from './Danger'
import { SignUp } from './SignUp'

const NAV = [
  { to: '/account', label: 'Overview' },
  { to: '/account/identity', label: 'Handle and email' },
  { to: '/account/security', label: 'Security' },
  { to: '/account/repo', label: 'Repository' },
  { to: '/account/blobs', label: 'Media' },
  { to: '/account/export', label: 'Export' },
  { to: '/account/preferences', label: 'Preferences' },
]

export function AccountApp({ path }: { path: string }) {
  const s = useSession()
  const p = path.replace(/\/+$/, '') || '/account'
  if (p === '/account/reset') return <PasswordReset />
  if (p === '/account/signup' && !s) return <SignUp />
  if (!s) return <SignIn />

  let page: JSX.Element
  let m: Record<string, string> | null
  if (p === '/account' || p === '/account/signup') page = <Overview />
  else if (p === '/account/identity') page = <Identity />
  else if (p === '/account/security') page = <Security />
  else if (p === '/account/repo') page = <Collections />
  else if ((m = match('/account/repo/:collection', p))) page = <Records collection={m.collection} />
  else if ((m = match('/account/repo/:collection/:rkey', p))) page = <RecordView collection={m.collection} rkey={m.rkey} />
  else if (p === '/account/blobs') page = <Blobs />
  else if (p === '/account/export') page = <Export />
  else if (p === '/account/preferences') page = <Preferences />
  else if (p === '/account/danger') page = <Danger />
  else page = <Notice kind="warn">There is no page at {p}.</Notice>

  const current = (to: string) => (to === '/account' ? p === '/account' : p === to || p.startsWith(`${to}/`))
  return (
    <>
      <Topbar where="Account">
        <button type="button" className="btn sm" onClick={() => signOut().then(() => navigate('/account'))}>
          Sign out
        </button>
      </Topbar>
      <div className="app">
        <aside className="sidenav">
          <div className="who">
            <strong>@{s.handle}</strong>
            <span className="mono">{s.did}</span>
          </div>
          <nav aria-label="Account">
            {NAV.map((n) => (
              <Link key={n.to} to={n.to} aria-current={current(n.to) ? 'page' : undefined}>
                {n.label}
              </Link>
            ))}
            <Link to="/account/danger" className="danger-link" aria-current={current('/account/danger') ? 'page' : undefined}>
              Deactivate or delete
            </Link>
          </nav>
        </aside>
        <main className="content" id="main">
          {page}
        </main>
      </div>
    </>
  )
}

function SignIn() {
  const [identifier, setIdentifier] = useState('')
  const [password, setPassword] = useState('')
  const [code, setCode] = useState('')
  const [needCode, setNeedCode] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<unknown>()

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    setBusy(true)
    setError(undefined)
    try {
      const out = await call('com.atproto.server.createSession', {
        body: { identifier: identifier.trim().replace(/^@/, ''), password, authFactorToken: needCode ? code.trim() : undefined },
      })
      setSession(out)
    } catch (err) {
      if (err instanceof XrpcError && err.error === 'AuthFactorTokenRequired') {
        if (needCode) setError(err)
        setNeedCode(true)
      } else setError(err)
    } finally {
      setBusy(false)
    }
  }

  return (
    <>
      <Topbar where="Account" />
      <main className="signin">
        <div className="card">
          <div className="inner">
            <form onSubmit={submit}>
              {!needCode ? (
                <>
                  <h1>Sign in</h1>
                  <p className="sub">Manage your handle, security settings and data on {location.hostname}.</p>
                  <ErrorNotice error={error} />
                  <Field label="Handle, DID or email">
                    <input
                      type="text"
                      value={identifier}
                      onChange={(e) => setIdentifier(e.target.value)}
                      autoComplete="username"
                      autoCapitalize="none"
                      spellCheck={false}
                      required
                      autoFocus
                    />
                  </Field>
                  <Field label="Password">
                    <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="current-password" required />
                  </Field>
                </>
              ) : (
                <>
                  <h1>Two-factor check</h1>
                  <p className="sub">
                    Enter the 6-digit code from your authenticator app for <b>{identifier}</b>, or one of your recovery codes.
                  </p>
                  {!!error && <Notice kind="err">{errText(error)}</Notice>}
                  {/* password managers pick the account's one-time code by the username beside it */}
                  <input type="text" className="sr-only" autoComplete="username" value={identifier} readOnly tabIndex={-1} aria-hidden="true" />
                  <Field label="Authentication code">
                    <input
                      type="text"
                      id="totp"
                      name="totp"
                      className="code"
                      value={code}
                      onChange={(e) => setCode(e.target.value)}
                      inputMode="numeric"
                      autoComplete="one-time-code"
                      required
                      autoFocus
                    />
                  </Field>
                </>
              )}
              <div className="row between">
                {needCode ? (
                  <button
                    type="button"
                    className="btn quiet"
                    onClick={() => {
                      setNeedCode(false)
                      setCode('')
                      setError(undefined)
                    }}
                  >
                    Use a different account
                  </button>
                ) : (
                  <Link to="/account/reset" className="small">
                    Forgot your password?
                  </Link>
                )}
                <button type="submit" className="btn primary" disabled={busy}>
                  {busy && <Spinner />}
                  {needCode ? 'Verify and sign in' : 'Sign in'}
                </button>
              </div>
            </form>
            {!needCode && (
              <p className="alt">
                New here? <Link to="/account/signup">Create an account</Link>
              </p>
            )}
          </div>
        </div>
      </main>
    </>
  )
}

/** Password reset: email a code, then set a new password with it. Also used to change a password. */
export function PasswordReset() {
  const q = useSearch()
  const [email, setEmail] = useState(q.get('email') ?? '')
  const [sent, setSent] = useState(false)
  const [token, setToken] = useState('')
  const [password, setPassword] = useState('')
  const [done, setDone] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<unknown>()

  const run = async (fn: () => Promise<void>) => {
    setBusy(true)
    setError(undefined)
    try {
      await fn()
    } catch (e) {
      setError(e)
    } finally {
      setBusy(false)
    }
  }

  return (
    <>
      <Topbar where="Account" />
      <main className="signin">
        <div className="card">
          <div className="inner">
            <h1>Reset your password</h1>
            {done ? (
              <>
                <Notice kind="ok">Password changed. Every session on this account has been signed out.</Notice>
                <div className="row end">
                  <Link to="/account" className="btn primary">
                    Sign in
                  </Link>
                </div>
              </>
            ) : !sent ? (
              <form
                onSubmit={(e) => {
                  e.preventDefault()
                  run(async () => {
                    await call('com.atproto.server.requestPasswordReset', { body: { email: email.trim() } })
                    setSent(true)
                  })
                }}
              >
                <p className="sub">We'll email a reset code to the address on your account.</p>
                <ErrorNotice error={error} />
                <Field label="Email">
                  <input type="email" value={email} onChange={(e) => setEmail(e.target.value)} autoComplete="email" required autoFocus />
                </Field>
                <div className="row between">
                  <Link to="/account" className="small">
                    Back to sign in
                  </Link>
                  <button className="btn primary" disabled={busy}>
                    {busy && <Spinner />}
                    Email me a code
                  </button>
                </div>
              </form>
            ) : (
              <form
                onSubmit={(e) => {
                  e.preventDefault()
                  run(async () => {
                    await call('com.atproto.server.resetPassword', { body: { token: token.trim(), password } })
                    setSession(null)
                    setDone(true)
                  })
                }}
              >
                <p className="sub">
                  If <b>{email}</b> belongs to an account here, a code is on its way. Codes look like <span className="mono">ABCDE-12345</span>.
                </p>
                <ErrorNotice error={error} />
                <Field label="Reset code">
                  <input type="text" value={token} onChange={(e) => setToken(e.target.value)} autoComplete="one-time-code" spellCheck={false} required autoFocus />
                </Field>
                <Field label="New password" hint="Changing your password signs out every session, including app passwords' sessions.">
                  <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="new-password" minLength={8} required />
                </Field>
                <div className="row between">
                  <button type="button" className="btn quiet" onClick={() => setSent(false)}>
                    Send a new code
                  </button>
                  <button className="btn primary" disabled={busy}>
                    {busy && <Spinner />}
                    Change password
                  </button>
                </div>
              </form>
            )}
          </div>
        </div>
      </main>
    </>
  )
}
