import { useState } from 'react'
import { ErrorNotice, Field, Notice, Spinner, Topbar } from '../../components/ui'
import { useAdminToken } from '../../lib/hooks'
import { Link, match } from '../../lib/router'
import { basic, call, setAdminToken } from '../../lib/xrpc'
import { Cluster } from './Cluster'
import { Metrics } from './Metrics'
import { AccountDetail, Accounts } from './Accounts'
import { Invites } from './Invites'
import { RateLimits } from './RateLimits'
import { Relays } from './Relays'

const TABS = [
  { to: '/admin', label: 'Cluster' },
  { to: '/admin/metrics', label: 'Live metrics' },
  { to: '/admin/accounts', label: 'Accounts' },
  { to: '/admin/invites', label: 'Invite codes' },
  { to: '/admin/ratelimits', label: 'Rate limits' },
  { to: '/admin/relays', label: 'Relays' },
]

export function AdminApp({ path }: { path: string }) {
  const token = useAdminToken()
  const p = path.replace(/\/+$/, '') || '/admin'
  if (!token) return <AdminLogin />
  let page: JSX.Element
  let m: Record<string, string> | null
  if (p === '/admin') page = <Cluster />
  else if (p === '/admin/metrics') page = <Metrics />
  else if (p === '/admin/accounts') page = <Accounts />
  else if ((m = match('/admin/accounts/:did', p))) page = <AccountDetail did={m.did} />
  else if (p === '/admin/invites') page = <Invites />
  else if (p === '/admin/ratelimits') page = <RateLimits />
  else if (p === '/admin/relays') page = <Relays />
  else page = <Notice kind="warn">There is no console page at {p}.</Notice>
  const current = (to: string) => (to === '/admin' ? p === '/admin' : p === to || p.startsWith(`${to}/`))
  return (
    <>
      <Topbar where="Operator console">
        <button type="button" className="btn sm" onClick={() => setAdminToken(null)}>
          Lock console
        </button>
      </Topbar>
      <main className="console">
        <nav className="tabs" aria-label="Console">
          {TABS.map((t) => (
            <Link key={t.to} to={t.to} aria-current={current(t.to) ? 'page' : undefined}>
              {t.label}
            </Link>
          ))}
        </nav>
        {page}
      </main>
    </>
  )
}

function AdminLogin() {
  const [token, setToken] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<unknown>()
  return (
    <>
      <Topbar where="Operator console" />
      <main className="signin">
        <div className="card">
          <div className="inner">
            <form
              onSubmit={async (e) => {
                e.preventDefault()
                setBusy(true)
                setError(undefined)
                try {
                  await call('vlpds.admin.getClusterStatus', { auth: basic(token.trim()) })
                  setAdminToken(token.trim())
                } catch (err) {
                  setError(err)
                } finally {
                  setBusy(false)
                }
              }}
            >
              <h1>Operator console</h1>
              <p className="sub">Cluster health, live metrics and account administration. The token stays in this tab only.</p>
              <ErrorNotice error={error} />
              <Field label="Admin token" hint="The server's --admin-token (VLPDS_ADMIN_TOKEN).">
                <input type="password" value={token} onChange={(e) => setToken(e.target.value)} autoComplete="off" required autoFocus />
              </Field>
              <div className="row end">
                <button className="btn primary" disabled={busy}>
                  {busy && <Spinner />}
                  Unlock console
                </button>
              </div>
            </form>
          </div>
        </div>
      </main>
    </>
  )
}
