import { useState, type ReactNode } from 'react'
import { registerDetail, type DetailMode } from '../../components/console/Drawer'
import { Banners, Chip, Copy, ErrorState, Json, KV, Loading, Meter, NeedsVersion, RRow, Sec, Spinner, Src, Strip, type BannerSpec } from '../../components/console/kit'
import { openPanel } from '../../components/console/nav'
import { registerPalette, type PalItem } from '../../components/console/Palette'
import { toast } from '../../components/console/toast'
import * as api from '../../lib/adminApi'
import type { AccountRow, AccountSecurity, RepoOpsResult, Session } from '../../lib/adminApi'
import { withAdmin } from '../../lib/console/adminAdapter'
import { useClusterView } from '../../lib/console/cluster'
import { ago, factorName, fmtBytes, fmtNum, plural, shortDid } from '../../lib/console/fmt'
import { isUnsupported } from '../../lib/console/live'
import { useLoad } from '../../lib/hooks'
import { Link } from '../../lib/router'
import { admin, call, errText } from '../../lib/xrpc'
import * as act from './accountActions'
import { useAccountsVersion, type Quota, type Who } from './accountActions'
import { accountState, TwoFactor } from './Accounts'
import { NodeTag } from './clusterUi'

// The account detail, in the slide-over or as a full page: identity, placement, sign-in and
// second factors, sessions, recent ops, blobs and quota, invites, spaces, moderation, and the
// actions (accountActions.tsx). The row comes from listAccounts (an exact DID is one lookup on
// the owner); the rest loads per section.

type InviteCode = { code: string; available: number; disabled: boolean; forAccount: string; createdBy: string; createdAt: string; uses: { usedBy: string; usedAt: string }[] }
type AccountInfo = {
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
type SubjectStatus = { takedown?: { applied: boolean; ref?: string }; deactivated?: { applied: boolean } }
type SubjectDetail = { quota: Quota }

const when = (ms?: number | null) => (ms ? ago(ms) : '—')
const iso = (s?: string | null) => (s ? ago(new Date(s).getTime()) : '—')
const date = (s?: string | number | null) => (s ? new Date(s).toISOString().slice(0, 10) : '—')

/** Opens another detail from inside this one. */
function Open({ type, id, children }: { type: string; id: string; children: ReactNode }) {
  return (
    <button type="button" className="cx-linklike" onClick={() => openPanel(type, id)}>
      {children}
    </button>
  )
}

function Act({ t, d, children }: { t: string; d: string; children: ReactNode }) {
  return (
    <div className="cx-act">
      <div className="ad">
        <b>{t}</b>
        {d}
      </div>
      {children}
    </div>
  )
}

const btn = (label: string, run: () => unknown, danger?: boolean) => (
  <button type="button" className={`cx-btn sm${danger ? ' danger' : ''}`} onClick={run}>
    {label}
  </button>
)

// ---------------------------------------------------------------- sections

function Security({ a, sec, mode }: { a: Who; sec: ReturnType<typeof useLoad<AccountSecurity>>; mode: DetailMode }) {
  const s = sec.data
  const factors = s ? [s.passkeys.length ? plural(s.passkeys.length, 'passkey') : '', s.totp.enabled ? 'authenticator app' : '', s.emailCode.enabled ? 'email code' : ''].filter(Boolean) : []
  return (
    <Sec title="Sign-in & two-factor" digest={!s ? '…' : factors.length ? factors.join(' · ') : s.oauthOnly ? 'OAuth only' : 'password only'} open right={<Src>getAccountSecurity</Src>}>
      {sec.error ? (
        isUnsupported(sec.error) ? <NeedsVersion what="Sign-in details" nsid="vlpds.admin.getAccountSecurity" /> : <ErrorState error={sec.error} retry={sec.reload} />
      ) : !s ? (
        <Loading />
      ) : (
        <>
          <KV
            rows={[
              ['Password', s.oauthOnly ? <Chip k="info">OAuth only</Chip> : s.passwordSet ? 'set' : <span className="muted">not set</span>],
              ['Second factor', s.secondFactorRequired ? <Chip k="ok">required at sign-in</Chip> : <span className="muted">not required</span>],
              [
                'Passkeys',
                s.passkeys.length ? (
                  <span className="cx-acc-chips">
                    {s.passkeys.map((p) => (
                      <Chip key={p.name + p.createdAt} k={p.suspect ? 'warn' : 'plain'} title={`added ${date(p.createdAt)}${p.lastUsedAt ? `, used ${ago(p.lastUsedAt)}` : ''}${p.backedUp ? ', synced' : ''}`}>
                        {p.name}
                        {p.suspect ? ' · copied?' : ''}
                      </Chip>
                    ))}
                  </span>
                ) : (
                  <span className="muted">none</span>
                ),
              ],
              ['Authenticator', s.totp.enabled ? <Chip k="ok">on{s.totp.enabledAt ? ` since ${date(s.totp.enabledAt)}` : ''}</Chip> : <span className="muted">off</span>],
              ['Email code', s.emailCode.enabled ? <Chip k="ok">on</Chip> : <span className="muted">off</span>],
              ['Recovery codes', s.recoveryCodes.issuedAt ? `${s.recoveryCodes.remaining} of ${s.recoveryCodes.total} left` : <span className="muted">none issued</span>],
              ['Trusted browsers', fmtNum(s.trustedBrowsers.length)],
              ['App passwords', s.blockAppPasswords ? <Chip k="info">blocked by the owner</Chip> : fmtNum(s.appPasswords.length)],
              ...s.lockouts
                .filter((l) => l.failures > 0 || l.lockedUntil)
                .map((l): [ReactNode, ReactNode] => [
                  factorName(l.factor),
                  l.lockedUntil && l.lockedUntil > Date.now() ? <Chip k="err">locked, {plural(l.failures, 'wrong code')}</Chip> : `${plural(l.failures, 'wrong code')}`,
                ]),
            ]}
          />
          <div className="cx-form-row" style={{ marginTop: 10, flexWrap: 'wrap' }}>
            {btn('Reset two-factor…', () => act.resetSecondFactors(a), true)}
            {!s.oauthOnly && btn('Set password…', () => act.setPassword(a))}
          </div>
          {s.recentSignIns.length > 0 && (
            <Sec title="Recent sign-ins" digest={`${s.recentSignIns.length} in 30 days · last ${ago(s.recentSignIns[0].at)}`} open={mode === 'page'} flush>
              <div className="cx-tw">
                <table className="cx-t compact">
                  <tbody>
                    {s.recentSignIns.slice(0, mode === 'page' ? 50 : 10).map((x, i) => (
                      <tr key={i}>
                        <td>{ago(x.at)}</td>
                        <td>
                          <span className="mono sm">{x.method === 'app_password' ? `app password ${x.appPassword ?? ''}` : x.method === 'oauth' ? (x.clientId ?? 'oauth') : x.method}</span>
                          {x.newDevice && <> <Chip k="info">new device</Chip></>}
                        </td>
                        <td className="t2 sm">{x.device}</td>
                        <td className="mono sm">{x.ip ?? ''}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Sec>
          )}
        </>
      )}
    </Sec>
  )
}

function sessionLabel(s: Session): string {
  if (s.kind === 'oauth') return s.clientId
  if (s.kind === 'appPassword') return `app password “${s.appPassword ?? ''}”`
  return 'password session'
}

function Sessions({ a, mode }: { a: Who; mode: DetailMode }) {
  const v = useAccountsVersion()
  const l = useLoad(() => withAdmin((c) => api.listSessions(c, a.did)), [a.did, v])
  const ss = l.data?.sessions ?? []
  const oauth = ss.filter((s) => s.kind === 'oauth').length
  return (
    <Sec title="Sessions" digest={l.data ? `${ss.length} signed in · ${oauth} OAuth` : '…'} open flush right={<Src>listSessions</Src>}>
      {l.error ? (
        isUnsupported(l.error) ? <NeedsVersion what="Sessions" nsid="vlpds.admin.listSessions" /> : <ErrorState error={l.error} retry={l.reload} />
      ) : !l.data ? (
        <Loading />
      ) : !ss.length ? (
        <div className="cx-empty">No live sessions.</div>
      ) : (
        <>
          <div className="cx-tw">
            <table className="cx-t compact">
              <thead>
                <tr>
                  <th>App</th>
                  <th>Device</th>
                  <th>IP</th>
                  <th className="r">Signed in</th>
                  <th className="r">Last refresh</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {ss.slice(0, mode === 'page' ? 200 : 20).map((s) => (
                  <tr key={s.id} title={s.kind === 'oauth' ? s.scope : undefined}>
                    <td>
                      {s.kind === 'oauth' ? <span className="mono sm">{s.clientId}</span> : <Chip k="plain">{s.kind === 'appPassword' ? `app pw · ${s.appPassword ?? ''}` : 'password'}</Chip>}
                      {s.kind !== 'oauth' && s.privileged && <> <Chip k="warn">privileged</Chip></>}
                      {s.passkey && <> <Chip k="acc">passkey</Chip></>}
                    </td>
                    <td className="t2 sm">{s.kind === 'oauth' ? (s.device ?? '—') : '—'}</td>
                    <td className="mono sm" title={s.signedInIp ? `signed in from ${s.signedInIp}` : undefined}>
                      {s.ip ?? '—'}
                    </td>
                    <td className="r">{when(s.signedInAt)}</td>
                    <td className="r">{when(s.refreshedAt)}</td>
                    <td className="r">
                      <button type="button" className="cx-btn sm quiet" onClick={() => act.revokeSession(a, s.id, sessionLabel(s))}>
                        Revoke
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="cx-pn-f">{btn('Sign out everywhere…', () => act.signOutEverywhere(a, ss.length), true)}</div>
        </>
      )}
    </Sec>
  )
}

function AppPasswords({ a, sec, mode }: { a: Who; sec?: AccountSecurity; mode: DetailMode }) {
  const pw = sec?.appPasswords ?? []
  return (
    <Sec title="App passwords" digest={!sec ? '…' : pw.length ? pw.map((p) => p.name).join(', ') : 'none'} flush open={mode === 'page' && pw.length > 0}>
      {!pw.length ? (
        <div className="cx-empty">{sec ? 'This account has no app passwords.' : '…'}</div>
      ) : (
        <div className="cx-tw">
          <table className="cx-t compact">
            <tbody>
              {pw.map((p) => (
                <tr key={p.name}>
                  <td>
                    <b>{p.name}</b> {p.privileged && <Chip k="warn">privileged</Chip>}
                  </td>
                  <td className="t2">created {iso(p.createdAt)}</td>
                  <td className="r">
                    <button type="button" className="cx-btn sm quiet" onClick={() => act.revokeAppPassword(a, p.name)}>
                      Revoke
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Sec>
  )
}

function CheckRepo({ did }: { did: string }) {
  const [busy, setBusy] = useState(false)
  const [r, setR] = useState<{ ok: boolean; problems?: string[]; records?: { count: number } }>()
  return (
    <>
      <button
        type="button"
        className="cx-btn sm"
        disabled={busy}
        onClick={async () => {
          setBusy(true)
          try {
            setR(await admin('vlpds.admin.checkRepo', { params: { did } }))
          } catch (e) {
            toast(errText(e), { err: true })
          } finally {
            setBusy(false)
          }
        }}
      >
        {busy && <Spinner />}
        Check repo
      </button>
      {r && (
        <div className="cx-acc-check">
          {r.ok ? <Chip k="ok">ok</Chip> : <Chip k="err">{plural(r.problems?.length ?? 0, 'problem')}</Chip>} head signature, records, MST and indexes
          {!!r.problems?.length && (
            <ul>
              {r.problems.map((p, i) => (
                <li key={i}>{p}</li>
              ))}
            </ul>
          )}
        </div>
      )}
    </>
  )
}

/** The DID document's keys and the directory's rotation keys: one directory request, so only on a click. */
function AccountKeys({ did }: { did: string }) {
  const [busy, setBusy] = useState(false)
  const [k, setK] = useState<api.AccountKeys>()
  const load = async (refresh: boolean) => {
    setBusy(true)
    try {
      setK(await withAdmin((c) => api.getAccountKeys(c, did, refresh)))
    } catch (e) {
      toast(errText(e), { err: true })
    } finally {
      setBusy(false)
    }
  }
  if (!k)
    return (
      <button type="button" className="cx-btn sm" disabled={busy} onClick={() => load(false)}>
        {busy && <Spinner />}
        Show keys
      </button>
    )
  return (
    <div className="cx-acc-keys">
      {(k.verificationMethods ?? []).map((m) => (
        <div key={m.id} title={m.type}>
          <span className="muted sm">{m.id.replace(did, '')}</span> <Copy text={m.publicKeyMultibase ?? ''} />{' '}
          {m.matchesAccount ? <Chip k="ok">matches</Chip> : <Chip k="err">not the account’s key</Chip>}
        </div>
      ))}
      {k.didDocError && <div className="t2 sm">DID document: {k.didDocError}</div>}
      {k.pendingSigningKey && (
        <div>
          <span className="muted sm">pending</span> <Copy text={k.pendingSigningKey} />
        </div>
      )}
      {(k.rotationKeys ?? []).map((r, i) => (
        <div key={r.didKey}>
          <span className="muted sm">rotation {i + 1}</span> <Copy text={r.didKey} />{' '}
          <Chip k={r.role === 'other' ? 'plain' : 'acc'}>{r.role === 'server' ? 'this PDS' : r.role === 'operator_recovery' ? 'operator recovery' : 'other'}</Chip>
        </div>
      ))}
      {k.rotationKeysError && <div className="t2 sm">Rotation keys: {k.rotationKeysError}</div>}
      <button type="button" className="cx-btn sm quiet" disabled={busy} onClick={() => load(true)}>
        {busy && <Spinner />}
        Refetch the DID document
      </button>
    </div>
  )
}

function Placement({ row, mode }: { row?: AccountRow; mode: DetailMode }) {
  const { view } = useClusterView()
  if (!row) return null
  const owner = view?.nodes.find((n) => n.node === row.node)
  const pos = view?.raw.layout ? view.raw.layout.shards.findIndex((s) => s.id === row.shard) : row.shard
  return (
    <Sec title="Placement" digest={`shard ${row.shard} on ${row.node}`} open={mode === 'page'}>
      <KV
        rows={[
          ['Shard', <span className="mono">{String(row.shard).padStart(10, '0')} {pos !== undefined && pos >= 0 && <span className="muted">· <Open type="shard" id={String(pos)}>open shard</Open></span>}</span>],
          ['Owner', owner ? <Open type="node" id={owner.node}><NodeTag n={owner} /></Open> : <span className="mono">{row.node}</span>],
          ['Repo rev', <span className="mono">{row.rev ?? '—'}</span>],
          ['Last commit', when(row.lastCommitAt)],
          ['Records', row.records === undefined ? '—' : fmtNum(row.records)],
          ['MST nodes', row.mstNodes === undefined ? '—' : fmtNum(row.mstNodes)],
          [
            'Repo size',
            <span title="Record blocks + MST node blocks, kept close by each commit; a recount makes it exact">
              {row.repoBytes === undefined ? '—' : `${fmtBytes(row.repoBytes)} (${fmtBytes(row.recordBytes ?? 0)} records · ${fmtBytes(row.mstBytes ?? 0)} MST)`}{' '}
              {btn('Recount…', () => act.recountRepo({ did: row.did, handle: row.handle, node: row.node }))}
            </span>,
          ],
          ['Checks', <CheckRepo did={row.did} />],
        ]}
      />
    </Sec>
  )
}

function opRows(r: RepoOpsResult) {
  const out: { key: string; at?: string | null; kind: string; cls: string; path: string; seq: string }[] = []
  for (const e of r.events) {
    if (e.kind === 'commit') e.ops.forEach((o, i) => out.push({ key: `${e.seq}/${i}`, at: e.time, kind: o.action, cls: o.action === 'create' ? 'op-c' : o.action === 'update' ? 'op-u' : 'op-d', path: o.path, seq: e.seq }))
    else out.push({ key: e.seq, at: e.time, kind: `#${e.kind}`, cls: '', path: e.kind === 'identity' ? (e.handle ?? '') : e.kind === 'account' ? (e.active ? 'active' : (e.status ?? 'inactive')) : `rev ${e.rev}`, seq: e.seq })
  }
  return out
}

function Ops({ did, mode }: { did: string; mode: DetailMode }) {
  const v = useAccountsVersion()
  const n = mode === 'page' ? 40 : 12
  const l = useLoad(() => withAdmin((c) => api.listRepoOps(c, did, n)), [did, n, v])
  const rows = l.data ? opRows(l.data) : []
  const first = rows[0]
  return (
    <Sec title="Recent operations" digest={!l.data ? '…' : first ? `${first.kind} ${first.path.split('/')[0]} ${iso(first.at)}` : 'none in memory'} flush open={mode === 'page'} right={<Src>listRepoOps · firehose ring</Src>}>
      {l.error ? (
        isUnsupported(l.error) ? <NeedsVersion what="Recent operations" nsid="vlpds.admin.listRepoOps" /> : <ErrorState error={l.error} retry={l.reload} />
      ) : !l.data ? (
        <Loading />
      ) : !rows.length ? (
        <div className="cx-empty">Nothing for this account in the firehose ring{l.data.reachesBackToTime ? ` (back to ${ago(l.data.reachesBackToTime)})` : ''}.</div>
      ) : (
        <>
          <div className="cx-tw">
            <table className="cx-t compact">
              <tbody>
                {rows.map((o) => (
                  <tr key={o.key} data-open={`event:${o.seq}`} onClick={() => openPanel('event', o.seq)} style={{ cursor: 'pointer' }}>
                    <td>{iso(o.at)}</td>
                    <td className={`mono sm ${o.cls}`}>{o.kind}</td>
                    <td className="mono sm trunc" style={{ maxWidth: 280 }}>
                      {o.path}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="cx-pn-f">
            {l.data.ringExhausted ? 'Everything in memory' : `Back to seq ${l.data.reachesBackTo ?? '—'}`}
            {l.data.reachesBackToTime ? ` · ${ago(l.data.reachesBackToTime)}` : ''}
          </div>
        </>
      )}
    </Sec>
  )
}

function Blobs({ a, row, mode }: { a: Who; row?: AccountRow; mode: DetailMode }) {
  const v = useAccountsVersion()
  const l = useLoad<SubjectDetail>(() => admin('vlpds.admin.getSubject', { params: { did: a.did } }), [a.did, v])
  const q = l.data?.quota
  const pct = q && q.limitBytes ? (q.bytes / q.limitBytes) * 100 : undefined
  return (
    <Sec title="Blobs & quota" digest={q ? `${fmtBytes(q.bytes)}${q.limitBytes ? ` of ${fmtBytes(q.limitBytes)}` : ''}` : row ? fmtBytes(row.blobBytes) : '…'} open={mode === 'page'} right={<Src>getSubject · setBlobQuota</Src>}>
      {l.error ? (
        <ErrorState error={l.error} retry={l.reload} />
      ) : !q ? (
        <Loading />
      ) : (
        <>
          {q.limitBytes > 0 && (
            <div className="cx-mini" style={{ marginBottom: 8 }}>
              <div className="ml">
                <span>quota</span>
                <b>{pct!.toFixed(0)}%</b>
              </div>
              <Meter v={q.bytes} max={q.limitBytes} k={q.over ? 'err' : undefined} wide />
            </div>
          )}
          <KV
            rows={[
              ['Blobs', row?.blobs !== undefined ? fmtNum(row.blobs) : '—'],
              ['Stored', `${fmtBytes(q.bytes)}${q.limitBytes ? ` of ${fmtBytes(q.limitBytes)}` : ', no limit'}`],
              ['Uploads today', `${fmtNum(q.uploadsToday)}${q.limitUploadsPerDay ? ` of ${fmtNum(q.limitUploadsPerDay)}` : ''}`],
              ['Limits', q.override.bytes !== undefined || q.override.uploadsPerDay !== undefined ? <Chip k="info">custom</Chip> : 'server defaults'],
            ]}
          />
          <div className="cx-form-row" style={{ marginTop: 10 }}>
            {btn('Change quota…', () => act.setQuota(a, q))}
          </div>
        </>
      )}
    </Sec>
  )
}

function Invites({ a, info }: { a: Who; info?: AccountInfo }) {
  const codes = info?.invites ?? []
  return (
    <Sec title="Invites" digest={!info ? '…' : info.invitesDisabled ? 'blocked from creating codes' : plural(codes.length, 'code')} right={<Src>getAccountInfo</Src>}>
      <div className="cx-form-row" style={{ justifyContent: 'space-between', marginBottom: codes.length ? 8 : 0 }}>
        <span className="t2">{info?.invitesDisabled ? 'This account cannot create invite codes.' : 'This account may create invite codes.'}</span>
        {info && btn(info.invitesDisabled ? 'Allow invites…' : 'Block invites…', () => act.setInvites(a, !!info.invitesDisabled))}
      </div>
      {codes.map((c) => (
        <RRow key={c.code} x={`${c.uses.length}/${c.available} used${c.disabled ? ' · disabled' : ''}`}>
          <span className="mono sm">{c.code}</span>
        </RRow>
      ))}
      <KV style={{ marginTop: 8 }} rows={[['Invited with', <span className="mono sm">{info?.invitedBy?.code ?? '—'}</span>]]} />
    </Sec>
  )
}

type AccountSpaces = {
  repos: { space: string; records: number; rev: { rev: string; at?: string } | null; takendownRecords: number }[]
  governs: { uri: string; createdAt: string; deletedAt?: string | null; takendown: boolean }[]
  more: boolean
}
const spaceLabel = (uri: string) => {
  const p = uri.replace(/^at:\/\//, '').split('/')
  return `${p[2]} / ${p[3]}`
}
const spacePath = (uri: string) => `/admin/spaces/space?uri=${encodeURIComponent(uri)}`

function Spaces({ did }: { did: string }) {
  const l = useLoad<AccountSpaces | null>(async () => {
    const d = await call('com.atproto.server.describeServer')
    if (!d?.vlpds?.spaces) return null
    return admin('vlpds.admin.getAccountSpaces', { params: { did } })
  }, [did])
  if (l.data === null) return null
  const d = l.data
  return (
    <Sec title="Spaces" digest={!d ? '…' : d.governs.length ? `authority of ${plural(d.governs.length, 'space')}` : d.repos.length ? `writes in ${plural(d.repos.length, 'space')}` : 'none'} right={<Src>getAccountSpaces</Src>}>
      {l.error ? (
        <ErrorState error={l.error} retry={l.reload} />
      ) : !d ? (
        <Loading />
      ) : d.governs.length + d.repos.length === 0 ? (
        <div className="t2">In no spaces.</div>
      ) : (
        <>
          {d.governs.map((g) => (
            <RRow key={`g${g.uri}`} to={spacePath(g.uri)} x={g.takendown ? 'taken down' : g.deletedAt ? 'deleted' : 'authority'}>
              <b className="nm">{spaceLabel(g.uri)}</b>
            </RRow>
          ))}
          {d.repos.map((r) => (
            <RRow key={`r${r.space}`} to={spacePath(r.space)} x={`${plural(r.records, 'record')}${r.takendownRecords ? ` · ${r.takendownRecords} down` : ''}`}>
              <span className="nm">{spaceLabel(r.space)}</span>
            </RRow>
          ))}
          {d.more && <div className="t2 sm">Showing the first 1,000.</div>}
        </>
      )}
    </Sec>
  )
}

type Case = { id: string; createdAt: string; status: string; source: string; subjects: { kind: string; did: string }[] }
type Audit = { id: string; at: string; actor: string; action: string; reason?: string; caseId?: string }

function Moderation({ did, status, mode }: { did: string; status?: SubjectStatus; mode: DetailMode }) {
  const v = useAccountsVersion()
  const cases = useLoad(async () => (await admin<{ cases: Case[] }>('vlpds.admin.listCases', { params: { did } })).cases, [did, v])
  const audit = useLoad(async () => (await admin<{ entries: Audit[] }>('vlpds.admin.getAuditLog', { params: { did, limit: 10 } })).entries, [did, v])
  const cs = cases.data ?? []
  const open = cs.filter((c) => c.status === 'open').length
  return (
    <Sec
      title="Moderation"
      digest={`${status?.takedown?.applied ? 'taken down · ' : ''}${cases.data ? (cs.length ? `${plural(cs.length, 'case')}${open ? `, ${open} open` : ''}` : 'no cases') : '…'}`}
      open={mode === 'page' && (cs.length > 0 || !!status?.takedown?.applied)}
      right={<Src>getSubjectStatus · listCases · getAuditLog</Src>}
    >
      <KV
        rows={[
          ['Takedown', status?.takedown?.applied ? <Chip k="err">in effect{status.takedown.ref ? ` · ${status.takedown.ref}` : ''}</Chip> : 'none'],
          ['Deactivated', status?.deactivated?.applied ? <Chip k="warn">yes</Chip> : 'no'],
        ]}
      />
      {!!cases.error && <ErrorState error={cases.error} retry={cases.reload} />}
      {cs.map((c) => (
        <RRow key={c.id} to={`/admin/moderation/cases/${encodeURIComponent(c.id)}`} x={c.status}>
          <span className="mono sm">{c.id}</span>
          <span className="nm t2">{c.source}</span>
        </RRow>
      ))}
      {!!audit.data?.length && (
        <>
          <div className="cx-eyebrow" style={{ marginTop: 10 }}>
            Audit log
          </div>
          {audit.data.map((e) => (
            <RRow key={e.id} x={iso(e.at)} title={e.reason}>
              <span className="mono sm">{e.action}</span>
              <span className="nm t2">{e.actor}</span>
            </RRow>
          ))}
        </>
      )}
      <div className="cx-form-row" style={{ marginTop: 8 }}>
        <Link className="cx-btn sm" to={`/admin/moderation?q=${encodeURIComponent(did)}`}>
          Look up in Moderation ›
        </Link>
      </div>
    </Sec>
  )
}

function DevMail({ email }: { email: string }) {
  const m = useLoad<{ messages?: unknown[]; token?: string }>(() => admin('vlpds.admin.getDevMail', { params: { email } }), [email])
  if (m.error || !m.data) return null
  const msgs = m.data.messages ?? []
  return (
    <Sec title="Dev mailbox" digest={plural(msgs.length, 'message')}>
      {m.data.token && (
        <KV rows={[['Latest code', <Copy text={m.data.token} />]]} />
      )}
      {msgs.length ? <Json value={msgs.slice(-3).reverse()} /> : <div className="t2">Nothing sent yet.</div>}
    </Sec>
  )
}

function Danger({ a, row, status, mode }: { a: Who; row?: AccountRow; status?: SubjectStatus; mode: DetailMode }) {
  const taken = !!status?.takedown?.applied || row?.status === 'takendown'
  const deact = !!status?.deactivated?.applied || row?.status === 'deactivated'
  return (
    <Sec title="Danger zone" digest="each asks you to type the handle" danger flush open={mode === 'page'}>
      <div className="cx-acts">
        {taken ? (
          <Act t="Reverse takedown" d="Serve the repo again and send an #account event.">
            {btn('Reverse…', () => act.reverseTakedown(a))}
          </Act>
        ) : (
          <Act t="Take down" d="Hide the repo from the network and revoke every session.">
            {btn('Take down…', () => act.takeDown(a), true)}
          </Act>
        )}
        {deact ? (
          <Act t="Reactivate" d="Serve the repo again and cancel a scheduled deletion.">
            {btn('Reactivate…', () => act.reactivate(a))}
          </Act>
        ) : (
          <Act t="Deactivate" d="Stop serving the repo until it's reactivated.">
            {btn('Deactivate…', () => act.deactivate(a), true)}
          </Act>
        )}
        <Act t="Rotate signing key" d="New repo key, PLC update, re-signed head.">
          {btn('Rotate…', () => act.rotateKey(a), true)}
        </Act>
        <Act t="Rebuild repo" d="Re-derive from records under a new signed commit and a #sync.">
          {btn('Rebuild…', () => act.rebuildRepo(a).catch((e) => toast(errText(e), { err: true })), true)}
        </Act>
        <Act t="Delete account" d="Erase the repo, blobs and account record. No undo.">
          {btn('Delete…', () => act.deleteAccount(a, { records: row?.records, blobs: row?.blobs }), true)}
        </Act>
      </div>
    </Sec>
  )
}

// ---------------------------------------------------------------- the detail

function useAccount(did: string) {
  const v = useAccountsVersion()
  const row = useLoad<AccountRow | undefined>(async () => {
    try {
      const r = await withAdmin((c) => api.listAccounts(c, { q: did, limit: 1 }))
      return r.accounts.find((x) => x.did === did)
    } catch (e) {
      if (isUnsupported(e)) return undefined
      throw e
    }
  }, [did, v])
  const info = useLoad<AccountInfo>(() => admin('com.atproto.admin.getAccountInfo', { params: { did } }), [did, v])
  const status = useLoad<SubjectStatus>(() => admin('com.atproto.admin.getSubjectStatus', { params: { did } }), [did, v])
  const sec = useLoad<AccountSecurity>(() => withAdmin((c) => api.getAccountSecurity(c, did)), [did, v])
  return { row, info, status, sec }
}

function banners(a: Who, row: AccountRow | undefined, info: AccountInfo | undefined, status: SubjectStatus | undefined, sec: AccountSecurity | undefined): BannerSpec[] {
  const out: BannerSpec[] = []
  const locks = (sec?.lockouts ?? []).filter((l) => l.lockedUntil && l.lockedUntil > Date.now())
  if (locks.length)
    out.push({
      id: 'locked',
      tone: 'warn',
      title: 'Sign-in codes locked',
      desc: `${locks.map((l) => factorName(l.factor)).join(' and ')} after wrong codes · clears ${ago(Math.max(...locks.map((l) => l.lockedUntil!)))} · live sessions keep working`,
      right: btn('Unlock…', () => act.clearLockout(a)),
    })
  if (status?.takedown?.applied) out.push({ id: 'td', tone: 'err', title: 'Taken down', desc: `${status.takedown.ref ? `${status.takedown.ref} · ` : ''}repo hidden, sessions revoked`, right: btn('Reverse…', () => act.reverseTakedown(a)) })
  if (info?.deletionScheduledAt) out.push({ id: 'del', tone: 'err', title: 'Deactivated, deletion scheduled', desc: `deleted ${ago(new Date(info.deletionScheduledAt).getTime())} unless the owner reactivates` })
  else if (info?.deactivatedAt && !status?.takedown?.applied) out.push({ id: 'deact', tone: 'warn', title: 'Deactivated', desc: `since ${date(info.deactivatedAt)} · the repo isn't served`, right: btn('Reactivate…', () => act.reactivate(a)) })
  if (row?.overQuota) out.push({ id: 'quota', tone: 'warn', title: 'Over its blob quota', desc: `${fmtBytes(row.blobBytes)} · new uploads refused` })
  if (info?.email && !info.emailConfirmedAt) out.push({ id: 'email', tone: 'warn', title: 'Email not confirmed', desc: 'actions that need a confirmed email are refused' })
  return out
}

registerDetail('account', {
  kind: 'Account',
  section: 'accounts',
  use: (did, mode) => {
    const { row, info, status, sec } = useAccount(did)
    const r = row.data
    const i = info.data
    const handle = i?.handle ?? r?.handle
    // a deleted account keeps its last data in the load, so check the error first
    const gone = !!info.error && ((info.error as { status?: number }).status === 404 || /not found/i.test(errText(info.error)))
    if (gone) return { title: <span className="mono">{shortDid(did)}</span>, body: null, missing: 'No account with this DID on this PDS.' }
    if (info.error && !i) return { title: <span className="mono">{shortDid(did)}</span>, body: null, missing: <ErrorState error={info.error} retry={info.reload} /> }
    if (!handle) return { title: <span className="mono">{shortDid(did)}</span>, body: null, loading: true }
    const a: Who = { did, handle, node: r?.node }
    const [k, t] = accountState(r ?? { status: status.data?.takedown?.applied ? 'takendown' : i?.deactivatedAt ? 'deactivated' : 'active', deleteAfter: i?.deletionScheduledAt })
    const s = sec.data

    const strip = (
      <Strip
        items={[
          ['records', r?.records === undefined ? '—' : fmtNum(r.records)],
          ['repo', r?.repoBytes === undefined ? '—' : fmtBytes(r.repoBytes)],
          [`blobs · ${r ? fmtBytes(r.blobBytes) : '—'}`, r?.blobs === undefined ? '—' : fmtNum(r.blobs)],
          ['MST nodes', r?.mstNodes === undefined ? '—' : fmtNum(r.mstNodes)],
          ['app passwords', s ? fmtNum(s.appPasswords.length) : '—'],
          ['last commit', when(r?.lastCommitAt)],
        ]}
      />
    )
    const identity = (
      <Sec title="Identity" digest={did} open right={<Src>getAccountInfo</Src>}>
        <KV
          rows={[
            ['DID', <Copy text={did} />],
            ['Handle', <>@{handle} {btn('Change…', () => act.setHandle(a))}</>],
            ['Email', i?.email ? <><Copy text={i.email} mono={false} /> {i.emailConfirmedAt ? <Chip k="ok">confirmed</Chip> : <Chip k="warn">unconfirmed</Chip>} {btn('Change…', () => act.setEmail(a, i.email))}</> : <span className="muted">none</span>],
            ['Created', i ? <>{date(i.indexedAt)} <span className="muted">({iso(i.indexedAt)})</span></> : '—'],
            ['2FA', r ? <TwoFactor f={r.secondFactors} /> : '—'],
            ['PLC', did.startsWith('did:plc:') ? <a href={`https://plc.directory/${did}/log/audit`} target="_blank" rel="noreferrer">audit log ↗</a> : <span className="mono sm">{did.split(':').slice(0, 2).join(':')}</span>],
            ['Keys', <AccountKeys did={did} />],
            ['Identity', btn('Publish #identity…', () => act.publishIdentity(a))],
          ]}
        />
      </Sec>
    )
    const placement = <Placement row={r} mode={mode} />
    const security = <Security a={a} sec={sec} mode={mode} />
    const sessions = <Sessions a={a} mode={mode} />
    const apppw = <AppPasswords a={a} sec={s} mode={mode} />
    const ops = <Ops did={did} mode={mode} />
    const blobs = <Blobs a={a} row={r} mode={mode} />
    const invites = <Invites a={a} info={i} />
    const spaces = <Spaces did={did} />
    const moder = <Moderation did={did} status={status.data} mode={mode} />
    const dev = i?.email ? <DevMail email={i.email} /> : null
    const danger = <Danger a={a} row={r} status={status.data} mode={mode} />
    const top = (
      <>
        <Banners items={banners(a, r, i, status.data, s)} />
        {strip}
      </>
    )
    return {
      title: `@${handle}`,
      chip: <Chip k={k}>{t}</Chip>,
      foot: (
        <>
          <Src>listAccounts · getAccountInfo · getSubjectStatus</Src> {r ? `on ${r.node}, shard ${r.shard}` : ''}
        </>
      ),
      body:
        mode === 'page' ? (
          <>
            {top}
            <div className="cols">
              <div>
                {identity}
                {placement}
                {ops}
                {blobs}
                {spaces}
                {moder}
              </div>
              <div>
                {security}
                {sessions}
                {apppw}
                {invites}
                {dev}
                {danger}
              </div>
            </div>
          </>
        ) : (
          <>
            {top}
            {identity}
            {security}
            {sessions}
            {placement}
            {apppw}
            {ops}
            {blobs}
            {invites}
            {spaces}
            {moder}
            {dev}
            {danger}
          </>
        ),
    }
  },
})

// ---------------------------------------------------------------- ⌘K

const VERBS: { re: RegExp; label: string; run: (a: Who, r: AccountRow) => unknown }[] = [
  { re: /^take ?down$/, label: 'Take down', run: (a) => act.takeDown(a) },
  { re: /^(reverse|undo)( takedown)?$/, label: 'Reverse the takedown of', run: (a) => act.reverseTakedown(a) },
  { re: /^deactivate$/, label: 'Deactivate', run: (a) => act.deactivate(a) },
  { re: /^reactivate$/, label: 'Reactivate', run: (a) => act.reactivate(a) },
  { re: /^(reset 2fa|reset)$/, label: 'Reset two-factor for', run: (a) => act.resetSecondFactors(a) },
  { re: /^sign ?out$/, label: 'Sign out everywhere:', run: (a) => act.signOutEverywhere(a) },
  { re: /^rotate( key)?$/, label: 'Rotate the signing key of', run: (a) => act.rotateKey(a) },
  { re: /^rebuild( repo)?$/, label: 'Rebuild the repo of', run: (a) => act.rebuildRepo(a).catch((e) => toast(errText(e), { err: true })) },
  { re: /^unlock$/, label: 'Unlock sign-in codes for', run: (a) => act.clearLockout(a) },
  { re: /^delete$/, label: 'Delete', run: (a, r) => act.deleteAccount(a, { records: r.records, blobs: r.blobs }) },
]
const VERB_RE = /^(take ?down|reverse takedown|reverse|undo|deactivate|reactivate|reset 2fa|reset|sign ?out|rotate key|rotate|rebuild repo|rebuild|unlock|delete)\s+@?(\S+)$/i

// "take down @handle", "reset 2fa alice", "delete did:plc:…": one item per matching account
registerPalette({
  items: () => [],
  async: async (q, signal): Promise<PalItem[]> => {
    const m = VERB_RE.exec(q.trim())
    if (!m) return []
    const verb = VERBS.find((v) => v.re.test(m[1].toLowerCase()))
    if (!verb) return []
    const r = await withAdmin((c) => api.listAccounts(c, { q: m[2], limit: 8 }, signal))
    return r.accounts.map((x) => {
      const [k] = accountState(x)
      return {
        group: 'Actions',
        glyph: <span className={`cx-g s-${k === 'acc' || k === 'plain' || k === 'violet' || k === 'stale' ? 'idle' : k}`}>■</span>,
        title: `${verb.label} @${x.handle}…`,
        desc: 'typed confirm',
        hay: x.did,
        run: () => {
          openPanel('account', x.did)
          verb.run({ did: x.did, handle: x.handle, node: x.node }, x)
        },
      }
    })
  },
})
