import { useState } from 'react'
import { Confirm, Empty, ErrorNotice, Loading, Notice, Panel, Spinner } from '../../components/ui'
import { fmtTime } from '../../lib/format'
import { useAction, useLoad } from '../../lib/hooks'
import { acall } from '../../lib/xrpc'

type Browser = { id: string; device: string; ip?: string; createdAt: string; lastUsedAt: string; expiresAt: string; current: boolean }
type SignInEntry = {
  at: string
  method: 'password' | 'app_password' | 'oauth'
  appPassword?: string
  clientId?: string
  device: string
  ip?: string
  factor?: 'totp' | 'email' | 'trusted'
  newDevice: boolean
  alerted: boolean
}
type State = {
  oauthOnly: boolean
  blockAppPasswords: boolean
  alerts: { password: boolean; appPassword: boolean }
  alertsPerDay: number
  secondFactor: boolean
  trustDays: number
  trustedBrowsers: Browser[]
  recentSignIns: SignInEntry[]
}

function host(id: string) {
  try {
    return new URL(id).hostname
  } catch {
    return id
  }
}

function how(e: SignInEntry) {
  if (e.method === 'app_password') return `App password “${e.appPassword ?? ''}”`
  if (e.method === 'oauth') return e.clientId ? `Password, to connect ${host(e.clientId)}` : 'Password, on the sign-in page'
  return 'Password'
}

const FACTOR: Record<string, string> = { totp: 'authenticator code', email: 'emailed code', trusted: 'trusted browser' }

/** Recent sign-ins, trusted browsers, sign-in emails and the OAuth-only switch. `ver` reloads it (two-factor changed). */
export function SignInSecurity({ ver }: { ver: number }) {
  const st = useLoad<State>(() => acall('vlpds.server.getSignInSecurity'), [ver])
  if (!st.data)
    return (
      <Panel title="Recent sign-ins" id="sign-ins">
        <ErrorNotice error={st.error} />
        {!st.error && <Loading />}
      </Panel>
    )
  return (
    <>
      <RecentSignIns list={st.data.recentSignIns} />
      <TrustedBrowsers s={st.data} reload={st.reload} />
      <SignInRules s={st.data} reload={st.reload} />
    </>
  )
}

function RecentSignIns({ list }: { list: SignInEntry[] }) {
  return (
    <Panel
      title="Recent sign-ins"
      desc="The last 50 times someone signed in to this account in the past 30 days. If you don't recognize one, change your password."
      id="sign-ins"
      flush={list.length > 0}
    >
      {list.length === 0 ? (
        <Empty title="No sign-ins yet">Sign-ins show up here from now on.</Empty>
      ) : (
        <div className="table-wrap">
          <table className="data">
            <thead>
              <tr>
                <th>When</th>
                <th>Device</th>
                <th className="hide-sm">How</th>
              </tr>
            </thead>
            <tbody>
              {list.map((e, i) => (
                <tr key={i}>
                  <td className="nowrap">{fmtTime(e.at)}</td>
                  <td>
                    <b className="break">{e.device}</b>
                    {e.newDevice && <span className="pill amber" style={{ marginLeft: 6 }}>New</span>}
                    <div className="mono small muted break">{e.ip ?? 'unknown address'}</div>
                    <div className="small muted show-sm">{how(e)}</div>
                  </td>
                  <td className="hide-sm">
                    {how(e)}
                    {e.factor && <div className="small muted">with {FACTOR[e.factor] ?? e.factor}</div>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  )
}

function TrustedBrowsers({ s, reload }: { s: State; reload: () => void }) {
  const [revoking, setRevoking] = useState<Browser | 'all'>()
  const revoke = useAction(async (b: Browser | 'all') => {
    await acall('vlpds.server.revokeTrustedBrowser', { body: b === 'all' ? { all: true } : { id: b.id } })
    setRevoking(undefined)
    reload()
  })
  const list = s.trustedBrowsers
  if (!s.secondFactor && list.length === 0) return null
  return (
    <Panel
      title="Trusted browsers"
      desc={`Browsers where you ticked “Trust this browser” after entering a code. They skip the code for ${s.trustDays} days. Changing your password or your two-factor settings removes them all.`}
      id="trusted"
      flush={list.length > 0}
      actions={
        list.length > 1 && (
          <button className="btn danger sm" onClick={() => setRevoking('all')}>
            Remove all
          </button>
        )
      }
    >
      <ErrorNotice error={revoke.error} />
      {list.length === 0 ? (
        <Empty title="No trusted browsers">Every sign-in asks for your code.</Empty>
      ) : (
        <div className="table-wrap">
          <table className="data">
            <thead>
              <tr>
                <th>Browser</th>
                <th className="hide-sm">Last used</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {list.map((b) => (
                <tr key={b.id}>
                  <td>
                    <b className="break">{b.device}</b>
                    {b.current && <span className="pill accent" style={{ marginLeft: 6 }}>This browser</span>}
                    <div className="mono small muted break">{b.ip ?? 'unknown address'}</div>
                    <div className="small muted">
                      Trusted {fmtTime(b.createdAt)}, until {fmtTime(b.expiresAt)}
                    </div>
                  </td>
                  <td className="nowrap hide-sm">{fmtTime(b.lastUsedAt)}</td>
                  <td className="num">
                    <button className="btn danger sm" onClick={() => setRevoking(b)}>
                      Remove
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <Confirm
        open={!!revoking}
        title={revoking === 'all' ? 'Remove every trusted browser?' : `Remove ${revoking?.device ?? ''}?`}
        action="Remove"
        danger
        busy={revoke.busy}
        onConfirm={() => revoking && revoke.run(revoking)}
        onClose={() => setRevoking(undefined)}
      >
        {revoking === 'all' ? 'Every browser' : 'That browser'} will be asked for a code at its next sign-in. Anyone already signed in stays signed in.
      </Confirm>
    </Panel>
  )
}

function SignInRules({ s, reload }: { s: State; reload: () => void }) {
  const save = useAction(async (body: Record<string, unknown>) => {
    await acall('vlpds.server.updateSignInSecurity', { body })
    reload()
  })
  return (
    <Panel title="Sign-in protection" desc="Emails about new sign-ins, and which kinds of sign-in this account accepts." id="sign-in-rules">
      <ErrorNotice error={save.error} />
      <h3 className="sub-h">Emails</h3>
      <label className="check">
        <input type="checkbox" checked={s.alerts.password} disabled={save.busy} onChange={(e) => save.run({ alerts: { password: e.target.checked } })} />
        <span>Email me when my password is used on a new device or browser</span>
      </label>
      <label className="check">
        <input
          type="checkbox"
          checked={s.alerts.appPassword}
          disabled={save.busy}
          onChange={(e) => save.run({ alerts: { appPassword: e.target.checked } })}
        />
        <span>Email me when an app password is used on a new device</span>
      </label>
      <p className="small muted">At most {s.alertsPerDay} of these a day. A sign-in with a code we just emailed you doesn't send one.</p>

      <h3 className="sub-h">Only sign in through OAuth</h3>
      {!s.secondFactor && !s.oauthOnly ? (
        <Notice>
          Turn on two-factor sign-in first. This setting makes sure nobody can use your password without your second factor, so it needs one to
          protect.
        </Notice>
      ) : (
        <>
          <p className="small">
            Some apps ask for your handle and password directly. With this on, they can't use your main password: apps have to send you to this
            server's sign-in page, which always asks for your second factor. This account page still works. App passwords keep working unless you
            block them below.
          </p>
          <label className="check">
            <input type="checkbox" checked={s.oauthOnly} disabled={save.busy} onChange={(e) => save.run({ oauthOnly: e.target.checked })} />
            <span>Don't let apps use my main password</span>
          </label>
          <label className="check">
            <input
              type="checkbox"
              checked={s.blockAppPasswords}
              disabled={save.busy || (!s.oauthOnly && !s.blockAppPasswords)}
              onChange={(e) => save.run({ blockAppPasswords: e.target.checked })}
            />
            <span>Block app passwords too</span>
          </label>
          {s.blockAppPasswords && <p className="small muted">Apps signed in with an app password stay signed in until you revoke the password.</p>}
        </>
      )}
      {save.busy && <Spinner />}
    </Panel>
  )
}
