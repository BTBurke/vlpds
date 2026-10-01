import { CopyText, ErrorNotice, JsonView, Loading, PageHead, Panel, Status } from '../../components/ui'
import { fmtNum } from '../../lib/format'
import { useLoad, useSession } from '../../lib/hooks'
import { Link } from '../../lib/router'
import { acall, getSession, setSession } from '../../lib/xrpc'

export type SessionInfo = {
  did: string
  handle: string
  email?: string
  emailConfirmed?: boolean
  active: boolean
  status?: string
  didDoc?: any
}

/** getSession, also refreshing the stored handle/email copy. */
export async function loadSession(): Promise<SessionInfo> {
  const info = await acall<SessionInfo>('com.atproto.server.getSession')
  const s = getSession()
  if (s && (s.handle !== info.handle || s.email !== info.email || s.emailConfirmed !== info.emailConfirmed || s.status !== info.status))
    setSession({ ...s, handle: info.handle, email: info.email, emailConfirmed: info.emailConfirmed, active: info.active, status: info.status })
  return info
}

export function accountStatus(info: { active: boolean; status?: string }) {
  if (info.active) return <Status kind="ok">Active</Status>
  if (info.status === 'deactivated') return <Status kind="warn">Deactivated</Status>
  return <Status kind="bad">{info.status ? info.status[0].toUpperCase() + info.status.slice(1) : 'Inactive'}</Status>
}

export function Overview() {
  const s = useSession()!
  const info = useLoad(loadSession, [])
  const st = useLoad(() => acall('com.atproto.server.checkAccountStatus'), [])
  const totp = useLoad(() => acall('vlpds.server.getTotpStatus'), [])
  const d = info.data
  const pds = d?.didDoc?.service?.find?.((x: any) => x.type === 'AtprotoPersonalDataServer')?.serviceEndpoint

  return (
    <>
      <PageHead title={`@${d?.handle ?? s.handle}`} desc="Your identity on this server and the state of your repository." />
      <ErrorNotice error={info.error} />
      {!d ? (
        <Loading />
      ) : (
        <>
          <Panel title="Identity">
            <dl className="dl">
              <dt>Handle</dt>
              <dd>
                <span className="row">
                  <CopyText text={d.handle} mono={false} display={<b>@{d.handle}</b>} />
                  <Link to="/account/identity" className="small">
                    Change
                  </Link>
                </span>
              </dd>
              <dt>DID</dt>
              <dd>
                <CopyText text={d.did} />
              </dd>
              <dt>Email</dt>
              <dd>
                {d.email ? (
                  <span className="row">
                    <span className="break">{d.email}</span>
                    {d.emailConfirmed ? <Status kind="ok">Confirmed</Status> : <Status kind="warn">Not confirmed</Status>}
                    {!d.emailConfirmed && (
                      <Link to="/account/identity" className="small">
                        Confirm
                      </Link>
                    )}
                  </span>
                ) : (
                  <span className="muted">None on file</span>
                )}
              </dd>
              <dt>Account</dt>
              <dd>{accountStatus(d)}</dd>
              <dt>Two-factor</dt>
              <dd>
                {totp.data ? (
                  totp.data.enabled ? (
                    <Status kind="ok">Authenticator app on</Status>
                  ) : (
                    <span className="row">
                      <Status kind="idle">Off</Status>
                      <Link to="/account/security" className="small">
                        Set up
                      </Link>
                    </span>
                  )
                ) : (
                  '…'
                )}
              </dd>
              {pds && (
                <>
                  <dt>Hosted at</dt>
                  <dd className="mono">{pds}</dd>
                </>
              )}
            </dl>
          </Panel>
          <Panel title="Repository" desc="The signed data structure holding your records." actions={<Link to="/account/repo">Browse records</Link>}>
            <ErrorNotice error={st.error} />
            {st.data ? (
              <dl className="dl">
                <dt>Records</dt>
                <dd>{fmtNum(st.data.indexedRecords)}</dd>
                <dt>Latest revision</dt>
                <dd className="mono">{st.data.repoRev}</dd>
                <dt>Head commit</dt>
                <dd>
                  <CopyText text={st.data.repoCommit} />
                </dd>
                <dt>Media</dt>
                <dd>
                  {fmtNum(st.data.importedBlobs)} of {fmtNum(st.data.expectedBlobs)} referenced files stored
                </dd>
              </dl>
            ) : (
              !st.error && <Loading />
            )}
          </Panel>
          {d.didDoc && (
            <Panel title="DID document" desc="What the network resolves your DID to: your handle, signing key and this server.">
              <JsonView value={d.didDoc} />
            </Panel>
          )}
        </>
      )}
    </>
  )
}
