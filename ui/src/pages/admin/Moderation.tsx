import { useEffect, useRef, useState, type ReactNode } from 'react'
import { CopyText, Empty, ErrorNotice, Field, JsonView, Loading, Notice, Panel, Spinner, Status } from '../../components/ui'
import { fmtBytes, fmtTime } from '../../lib/format'
import { useAction, useLoad } from '../../lib/hooks'
import { Link, navigate, useSearch } from '../../lib/router'
import { admin } from '../../lib/xrpc'

type Kind = 'account' | 'record' | 'blob'
type SubjectRef = { kind: Kind; did: string; uri?: string; cid?: string }
type Resolved = { kind: Kind; did: string; handle: string; uri?: string; cid?: string }
type BlobView = {
  cid: string
  takendown: boolean
  stored: boolean
  quarantined: boolean
  mimeType?: string
  size?: number
  purgeAfterMs?: number
  takedown?: { ref?: string; quarantinedAt?: number; purgedAtMs?: number }
}
type Quota = {
  bytes: number
  uploadsToday: number
  limitBytes: number
  limitUploadsPerDay: number
  override: { bytes?: number; uploadsPerDay?: number }
  defaults: { bytes: number; uploadsPerDay: number }
  over: boolean
}
type SubjectDetail = {
  account: { did: string; handle: string; email?: string; createdAt: string; status?: string; takedown: { applied: boolean; ref?: string } }
  quota: Quota
  record?: { uri: string; exists: boolean; takendown: boolean; cid?: string; value?: unknown; blobs?: BlobView[] }
  blob?: BlobView
}
type CaseAction = { at: string; auditId: string; action: string; subject: SubjectRef; reason?: string; actor: string }
type Case = {
  id: string
  createdAt: string
  updatedAt: string
  status: 'open' | 'actioned' | 'dismissed' | 'restored'
  source: string
  subjects: SubjectRef[]
  notes: { at: string; actor: string; ip?: string; text: string }[]
  actions: CaseAction[]
}
type AuditEntry = { id: string; at: string; actor: string; ip?: string; node: string; action: string; subject?: SubjectRef; reason?: string; caseId?: string; detail?: any }
type TakedownEntry = {
  subject: SubjectRef
  reason?: string
  ref?: string
  caseId?: string
  at: string
  actor: string
  quarantined?: boolean
  purgeAfterMs?: number
  purgedAtMs?: number
}

const TABS = [
  { id: 'lookup', label: 'Look up' },
  { id: 'cases', label: 'Cases' },
  { id: 'takedowns', label: 'Active takedowns' },
  { id: 'audit', label: 'Audit log' },
  { id: 'quotas', label: 'Quotas' },
] as const

const SEMANTICS: Record<Kind, string> = {
  account:
    'The account is taken down: its repo stops being served, a #account event tells relays and AppViews, and its sessions are revoked. Restoring reverses all of it except the revoked sessions.',
  record:
    "Hidden from this PDS's record reads (getRecord, listRecords). It is still in the signed repo and visible to relays and AppViews until the user deletes it; ask Bluesky Trust & Safety to act on their copy.",
  blob: 'Stops being served at once, cannot be re-uploaded or referenced, and its bytes move to quarantine. They are deleted after the quarantine period unless restored. Copies already in AppView/CDN caches are not purged by this.',
}

/** Decimal units, as --blob-quota-gb. */
const fmtGB = (n: number) => (n >= 1e9 ? `${(n / 1e9).toFixed(2)} GB` : n >= 1e6 ? `${(n / 1e6).toFixed(1)} MB` : `${(n / 1e3).toFixed(1)} kB`)
const lookupUrl = (q: string, caseId?: string) => `/admin/moderation?q=${encodeURIComponent(q)}${caseId ? `&case=${encodeURIComponent(caseId)}` : ''}`
const subjectQuery = (s: SubjectRef) => (s.kind === 'record' ? s.uri! : s.kind === 'blob' ? `${s.did} ${s.cid}` : s.did)

function SubjectText({ s }: { s: SubjectRef }) {
  return (
    <Link to={lookupUrl(subjectQuery(s))} className="mono small break">
      {s.kind === 'record' ? s.uri : s.kind === 'blob' ? `${s.did} · ${s.cid}` : s.did}
    </Link>
  )
}

export function Moderation() {
  const search = useSearch()
  const tab = search.get('tab') ?? 'lookup'
  const q = search.get('q') ?? ''
  return (
    <>
      <div className="console-head">
        <h1>Moderation</h1>
        <div className="seg" role="group" aria-label="Moderation views">
          {TABS.map((t) => (
            <button key={t.id} type="button" aria-pressed={tab === t.id} onClick={() => navigate(t.id === 'lookup' ? lookupUrl(q) : `/admin/moderation?tab=${t.id}`, { replace: true })}>
              {t.label}
            </button>
          ))}
        </div>
      </div>
      {tab === 'lookup' && <Lookup q={q} />}
      {tab === 'cases' && <Cases />}
      {tab === 'takedowns' && <Takedowns />}
      {tab === 'audit' && <AuditLog />}
      {tab === 'quotas' && <OverQuota />}
    </>
  )
}

// ---------------------------------------------------------------- lookup

function Lookup({ q }: { q: string }) {
  const caseId = useSearch().get('case') ?? undefined
  const [input, setInput] = useState(q)
  useEffect(() => setInput(q), [q])
  const res = useLoad<Resolved | null>(() => (q ? admin('vlpds.admin.resolveSubject', { params: { q } }) : Promise.resolve(null)), [q])
  const r = res.data
  const detail = useLoad<SubjectDetail | null>(
    () => (r ? admin('vlpds.admin.getSubject', { params: { did: r.did, uri: r.uri, cid: r.cid } }) : Promise.resolve(null)),
    [r?.did, r?.uri, r?.cid],
  )
  const d = detail.data
  const reload = () => detail.reload()
  return (
    <>
      <form
        className="toolbar mod-lookup"
        onSubmit={(e) => {
          e.preventDefault()
          navigate(lookupUrl(input.trim(), caseId))
        }}
      >
        <input
          type="search"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="bsky.app URL, at:// URI, @handle, DID, or DID + blob CID"
          aria-label="Subject to look up"
          spellCheck={false}
          autoCapitalize="none"
        />
        <button className="btn primary">Look up</button>
      </form>
      <p className="muted small">
        Only content stored on this PDS can be acted on here. Previews load only when you ask, and images stay blurred until you reveal them.
      </p>
      {caseId && (
        <Notice kind="info">
          Actions taken here are filed under <Link to={`/admin/moderation/cases/${caseId}`}>case {caseId}</Link>.
        </Notice>
      )}
      {!q ? (
        <Empty title="Paste a link from a report">A profile or post URL, an at:// URI, a handle or DID, a CDN image URL, or a DID and blob CID.</Empty>
      ) : res.error ? (
        <ErrorNotice error={res.error} />
      ) : !r || (!d && !detail.error) ? (
        <Loading />
      ) : (
        <>
          <ErrorNotice error={detail.error} />
          {d && (
            <div className={d.record || d.blob ? 'mod-grid' : 'grid2'}>
              <div className="stack">
                {d.record && <RecordCard did={r.did} rec={d.record} onDone={reload} />}
                {d.blob && <BlobCard did={r.did} b={d.blob} onDone={reload} />}
              </div>
              <div className="stack">
                <AccountCard d={d} onDone={reload} />
                <QuotaCard did={r.did} q={d.quota} onDone={reload} />
              </div>
            </div>
          )}
        </>
      )}
    </>
  )
}

function TakedownBadge({ on, children }: { on: boolean; children?: ReactNode }) {
  return on ? <Status kind="bad">{children ?? 'Taken down'}</Status> : <Status kind="ok">Visible</Status>
}

function AccountCard({ d, onDone }: { d: SubjectDetail; onDone: () => void }) {
  const a = d.account
  return (
    <Panel
      title={`@${a.handle}`}
      desc="Account"
      actions={<ModerateButton subject={{ kind: 'account', did: a.did }} applied={a.takedown.applied} onDone={onDone} />}
    >
      <dl className="dl">
        <dt>DID</dt>
        <dd>
          <CopyText text={a.did} />
        </dd>
        <dt>Status</dt>
        <dd>
          <TakedownBadge on={a.takedown.applied} />
          {a.status && a.status !== 'takendown' && <span className="muted small"> ({a.status})</span>}
          {a.takedown.ref && <span className="muted small"> ref {a.takedown.ref}</span>}
        </dd>
        <dt>Created</dt>
        <dd>{fmtTime(a.createdAt)}</dd>
        <dt>Email</dt>
        <dd className="break">{a.email ?? '—'}</dd>
        <dt>Admin</dt>
        <dd>
          <Link to={`/admin/accounts/${encodeURIComponent(a.did)}`}>Account page</Link>
        </dd>
      </dl>
    </Panel>
  )
}

function RecordCard({ did, rec, onDone }: { did: string; rec: NonNullable<SubjectDetail['record']>; onDone: () => void }) {
  const [show, setShow] = useState(false)
  return (
    <Panel
      title="Record"
      desc={<span className="mono small break">{rec.uri}</span>}
      actions={rec.exists || rec.takendown ? <ModerateButton subject={{ kind: 'record', did, uri: rec.uri }} applied={rec.takendown} onDone={onDone} /> : undefined}
    >
      {!rec.exists ? (
        <Notice kind="warn">No such record in this repo (deleted, or never existed).</Notice>
      ) : (
        <>
          <div className="row between">
            <TakedownBadge on={rec.takendown}>Hidden from record reads</TakedownBadge>
            <button type="button" className="btn sm" onClick={() => setShow((s) => !s)} aria-expanded={show}>
              {show ? 'Hide record JSON' : 'Show record JSON'}
            </button>
          </div>
          {show && <JsonView value={rec.value} />}
          {!!rec.blobs?.length && (
            <>
              <h3 className="mod-sub">Blobs in this record</h3>
              <div className="mod-blobs">
                {rec.blobs.map((b) => (
                  <BlobCard key={b.cid} did={did} b={b} onDone={onDone} compact />
                ))}
              </div>
            </>
          )}
        </>
      )}
    </Panel>
  )
}

function BlobCard({ did, b, onDone, compact }: { did: string; b: BlobView; onDone: () => void; compact?: boolean }) {
  const purged = !!b.takedown?.purgedAtMs
  const body = (
    <div className="mod-blob">
      <SafePreview did={did} b={b} />
      <div className="mod-blob-meta">
        <div className="mono small break">{b.cid}</div>
        <div className="small muted">
          {b.mimeType ?? 'unknown type'}
          {b.size !== undefined && ` · ${fmtBytes(b.size)}`}
        </div>
        <div className="row">
          <TakedownBadge on={b.takendown} />
          {b.quarantined && <span className="pill">quarantined</span>}
          {purged && <span className="pill danger">bytes purged</span>}
          {!b.stored && !b.quarantined && !purged && <span className="pill">not stored</span>}
        </div>
        {b.takendown && b.purgeAfterMs && !purged && <div className="small muted">Deleted after {fmtTime(b.purgeAfterMs)} unless restored.</div>}
        <ModerateButton subject={{ kind: 'blob', did, cid: b.cid }} applied={b.takendown} onDone={onDone} />
      </div>
    </div>
  )
  return compact ? body : <Panel title="Blob">{body}</Panel>
}

/** Fetches nothing until asked; images blurred until revealed; video never autoplays. */
function SafePreview({ did, b }: { did: string; b: BlobView }) {
  const [url, setUrl] = useState<string>()
  const [reveal, setReveal] = useState(false)
  const urlRef = useRef<string>()
  useEffect(
    () => () => {
      if (urlRef.current) URL.revokeObjectURL(urlRef.current)
    },
    [],
  )
  const load = useAction(async () => {
    const r: Response = await admin('com.atproto.sync.getBlob', { params: { did, cid: b.cid }, raw: true })
    const u = URL.createObjectURL(await r.blob())
    urlRef.current = u
    setUrl(u)
  })
  const mime = b.mimeType ?? ''
  const image = mime.startsWith('image/')
  const video = mime.startsWith('video/')
  if (!b.stored && !b.quarantined) return <div className="mod-preview empty">No bytes</div>
  if (!url)
    return (
      <div className="mod-preview empty">
        <span className="small muted">{image ? 'Image' : video ? 'Video' : 'File'} not loaded</span>
        <button type="button" className="btn sm" onClick={() => load.run()} disabled={load.busy}>
          {load.busy && <Spinner />}
          {image ? 'Load blurred preview' : video ? 'Load video (paused)' : 'Load'}
        </button>
        <ErrorNotice error={load.error} />
      </div>
    )
  if (image)
    return (
      <button type="button" className={`mod-preview${reveal ? '' : ' blurred'}`} onClick={() => setReveal((v) => !v)} aria-label={reveal ? 'Blur image' : 'Reveal image'}>
        <img src={url} alt="" />
        {!reveal && <span className="mod-reveal">Click to reveal</span>}
      </button>
    )
  if (video)
    return (
      <div className={`mod-preview${reveal ? '' : ' blurred'}`}>
        <video src={url} controls={reveal} preload="metadata" playsInline muted />
        {!reveal && (
          <button type="button" className="mod-reveal" onClick={() => setReveal(true)}>
            Click to reveal
          </button>
        )}
      </div>
    )
  return (
    <div className="mod-preview empty">
      <a className="btn sm" href={url} download={b.cid}>
        Download
      </a>
    </div>
  )
}

function QuotaCard({ did, q, onDone }: { did: string; q: Quota; onDone: () => void }) {
  const [edit, setEdit] = useState(false)
  const [gb, setGb] = useState('')
  const [perDay, setPerDay] = useState('')
  const [reason, setReason] = useState('')
  const save = useAction(async (reset: boolean) => {
    const body: any = { did, reason: reason.trim() || undefined }
    if (!reset) {
      if (gb.trim() !== '') body.bytes = Math.round(Number(gb) * 1e9)
      if (perDay.trim() !== '') body.uploadsPerDay = Math.round(Number(perDay))
    }
    await admin('vlpds.admin.setBlobQuota', { body })
    setEdit(false)
    onDone()
  })
  const pct = q.limitBytes ? Math.min(100, (q.bytes / q.limitBytes) * 100) : 0
  const custom = q.override.bytes !== undefined || q.override.uploadsPerDay !== undefined
  return (
    <Panel
      title="Blob quota"
      desc={custom ? 'Custom for this account.' : 'Server defaults.'}
      actions={
        <button type="button" className="btn sm" onClick={() => setEdit((e) => !e)}>
          {edit ? 'Cancel' : 'Edit'}
        </button>
      }
    >
      {q.over && <Notice kind="warn">Over its byte quota (a migration brought more than it allows). New uploads are refused until it is under.</Notice>}
      <dl className="dl">
        <dt>Stored</dt>
        <dd>
          {fmtGB(q.bytes)} of {q.limitBytes ? fmtGB(q.limitBytes) : 'unlimited'}
          {q.limitBytes > 0 && (
            <div className="mod-meter" aria-hidden="true">
              <div style={{ width: `${pct}%` }} className={q.over ? 'over' : ''} />
            </div>
          )}
        </dd>
        <dt>Uploads today</dt>
        <dd>
          {q.uploadsToday} of {q.limitUploadsPerDay || 'unlimited'}
        </dd>
      </dl>
      {edit && (
        <form
          onSubmit={(e) => {
            e.preventDefault()
            save.run(false)
          }}
        >
          <ErrorNotice error={save.error} />
          <div className="grid2">
            <Field label="Bytes (GB)" hint={`Blank: default (${fmtGB(q.defaults.bytes)}). 0: unlimited.`}>
              <input type="number" min="0" step="any" value={gb} onChange={(e) => setGb(e.target.value)} placeholder={q.override.bytes !== undefined ? String(q.override.bytes / 1e9) : ''} />
            </Field>
            <Field label="Uploads per day" hint={`Blank: default (${q.defaults.uploadsPerDay}). 0: unlimited.`}>
              <input type="number" min="0" step="1" value={perDay} onChange={(e) => setPerDay(e.target.value)} placeholder={q.override.uploadsPerDay !== undefined ? String(q.override.uploadsPerDay) : ''} />
            </Field>
          </div>
          <Field label="Reason (audit log)">
            <input type="text" value={reason} onChange={(e) => setReason(e.target.value)} />
          </Field>
          <div className="row end">
            {custom && (
              <button type="button" className="btn" onClick={() => save.run(true)} disabled={save.busy}>
                Back to defaults
              </button>
            )}
            <button className="btn primary" disabled={save.busy}>
              {save.busy && <Spinner />}
              Save quota
            </button>
          </div>
        </form>
      )}
    </Panel>
  )
}

// ---------------------------------------------------------------- takedown / restore

/** Cases an action can still be filed under (not dismissed). */
function useOpenCases() {
  return useLoad<{ cases: Case[] }>(async () => {
    const r: { cases: Case[] } = await admin('vlpds.admin.listCases')
    return { cases: r.cases.filter((c) => c.status !== 'dismissed') }
  }, [])
}

function ModerateButton({ subject, applied, onDone, caseId }: { subject: SubjectRef; applied: boolean; onDone: () => void; caseId?: string }) {
  const [open, setOpen] = useState(false)
  const fromUrl = useSearch().get('case') ?? undefined
  caseId ??= fromUrl
  return (
    <>
      <button type="button" className={`btn sm ${applied ? '' : 'danger'}`} onClick={() => setOpen(true)}>
        {applied ? 'Restore…' : 'Take down…'}
      </button>
      {open && (
        <ModerateDialog
          subject={subject}
          restore={applied}
          caseId={caseId}
          onClose={() => setOpen(false)}
          onDone={() => {
            setOpen(false)
            onDone()
          }}
        />
      )}
    </>
  )
}

function ModerateDialog({ subject, restore, caseId, onClose, onDone }: { subject: SubjectRef; restore: boolean; caseId?: string; onClose: () => void; onDone: () => void }) {
  const ref = useRef<HTMLDialogElement>(null)
  const [reason, setReason] = useState('')
  const [cid, setCid] = useState(caseId ?? '')
  const cases = useOpenCases()
  useEffect(() => {
    ref.current?.showModal()
  }, [])
  const act = useAction(async () => {
    await admin('vlpds.admin.moderate', {
      body: { did: subject.did, kind: subject.kind, uri: subject.uri, cid: subject.cid, action: restore ? 'restore' : 'takedown', reason: reason.trim(), caseId: cid || undefined },
    })
    onDone()
  })
  const what = subject.kind === 'account' ? 'account' : subject.kind === 'record' ? 'record' : 'blob'
  return (
    <dialog ref={ref} className="modal mod-dialog" onClose={onClose} aria-labelledby="mod-title">
      <form
        className="inner"
        onSubmit={(e) => {
          e.preventDefault()
          if (reason.trim()) act.run()
        }}
      >
        <h2 id="mod-title">{restore ? `Restore this ${what}?` : `Take down this ${what}?`}</h2>
        <p className="mono small break">{subjectQuery(subject)}</p>
        <p className="muted small">{restore ? 'Lifts the takedown and puts any quarantined bytes back.' : SEMANTICS[subject.kind]}</p>
        <ErrorNotice error={act.error} />
        <Field label="Reason (required, kept in the audit log)">
          <textarea value={reason} onChange={(e) => setReason(e.target.value)} rows={3} required autoFocus maxLength={2000} />
        </Field>
        <Field label="Case" hint="Links this action to a case (optional).">
          <select value={cid} onChange={(e) => setCid(e.target.value)}>
            <option value="">No case</option>
            {caseId && !cases.data?.cases.some((c) => c.id === caseId) && <option value={caseId}>{caseId}</option>}
            {cases.data?.cases.map((c) => (
              <option key={c.id} value={c.id}>
                {c.source.slice(0, 60)} ({c.id})
              </option>
            ))}
          </select>
        </Field>
        <div className="row end">
          <button type="button" className="btn" onClick={() => ref.current?.close()}>
            Cancel
          </button>
          <button type="submit" className={`btn ${restore ? 'primary' : 'danger solid'}`} disabled={!reason.trim() || act.busy}>
            {act.busy && <Spinner />}
            {restore ? 'Restore' : 'Take down'}
          </button>
        </div>
      </form>
    </dialog>
  )
}

// ---------------------------------------------------------------- lists

function Takedowns() {
  const [kind, setKind] = useState<'' | Kind>('')
  const l = useLoad<{ takedowns: TakedownEntry[] }>(() => admin('vlpds.admin.listTakedowns', { params: { kind } }), [kind])
  const rows = l.data?.takedowns ?? []
  return (
    <>
      <div className="row between">
        <div className="seg" role="group" aria-label="Kind">
          {(['', 'account', 'record', 'blob'] as const).map((k) => (
            <button key={k} type="button" aria-pressed={kind === k} onClick={() => setKind(k)}>
              {k ? `${k[0].toUpperCase()}${k.slice(1)}s` : 'All'}
            </button>
          ))}
        </div>
        <button className="btn sm" onClick={l.reload}>
          Refresh
        </button>
      </div>
      <ErrorNotice error={l.error} />
      <Panel flush>
        {!l.data ? (
          <Loading />
        ) : rows.length === 0 ? (
          <Empty title="No active takedowns">{kind ? `No ${kind} takedowns.` : 'Takedowns made from here or by a moderation service show up here.'}</Empty>
        ) : (
          <div className="table-wrap">
            <table className="data">
              <thead>
                <tr>
                  <th>Kind</th>
                  <th>Subject</th>
                  <th>Reason</th>
                  <th>When</th>
                  <th>Bytes</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {rows.map((t) => (
                  <tr key={`${t.subject.kind}:${subjectQuery(t.subject)}`}>
                    <td>
                      <span className="pill">{t.subject.kind}</span>
                    </td>
                    <td>
                      <SubjectText s={t.subject} />
                    </td>
                    <td className="break">
                      {t.reason ?? <span className="muted">{t.ref ? `ref ${t.ref}` : '—'}</span>}
                      {t.caseId && (
                        <div className="small">
                          <Link to={`/admin/moderation/cases/${t.caseId}`}>case {t.caseId}</Link>
                        </div>
                      )}
                    </td>
                    <td className="nowrap">
                      {fmtTime(t.at)}
                      <div className="small muted">{t.actor}</div>
                    </td>
                    <td className="small">
                      {t.subject.kind !== 'blob' ? '—' : t.purgedAtMs ? 'purged' : t.quarantined ? `quarantined until ${fmtTime(t.purgeAfterMs)}` : 'none stored'}
                    </td>
                    <td>
                      <ModerateButton subject={t.subject} applied onDone={l.reload} caseId={t.caseId} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
    </>
  )
}

function AuditLog() {
  const l = useLoad<{ entries: AuditEntry[] }>(() => admin('vlpds.admin.getAuditLog', { params: { limit: 200 } }), [])
  return (
    <>
      <ErrorNotice error={l.error} />
      <Panel flush title="Audit log" desc="Every takedown, restore, purge, case and quota change: who, from where, and why. Newest first." actions={<button className="btn sm" onClick={l.reload}>Refresh</button>}>
        {!l.data ? <Loading /> : l.data.entries.length === 0 ? <Empty title="Nothing yet" /> : <AuditTable entries={l.data.entries} />}
      </Panel>
    </>
  )
}

function AuditTable({ entries }: { entries: AuditEntry[] }) {
  return (
    <div className="table-wrap">
      <table className="data">
        <thead>
          <tr>
            <th>When</th>
            <th>Who</th>
            <th>Action</th>
            <th>Subject</th>
            <th>Reason</th>
          </tr>
        </thead>
        <tbody>
          {entries.map((e) => (
            <tr key={e.id}>
              <td className="nowrap">{fmtTime(e.at)}</td>
              <td className="nowrap">
                {e.actor}
                <div className="small muted mono">{e.ip ?? '—'}</div>
              </td>
              <td>
                <span className={`pill${e.action === 'takedown' || e.action === 'blob.purge' ? ' danger' : ''}`}>{e.action}</span>
              </td>
              <td>{e.subject ? <SubjectText s={e.subject} /> : <span className="muted">—</span>}</td>
              <td className="break">
                {e.reason ?? <span className="muted">—</span>}
                {e.caseId && (
                  <div className="small">
                    <Link to={`/admin/moderation/cases/${e.caseId}`}>case {e.caseId}</Link>
                  </div>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function OverQuota() {
  const l = useLoad<{ accounts: { did: string; bytes: number; limit: number; at: string }[] }>(() => admin('vlpds.admin.listOverQuota'), [])
  return (
    <Panel flush title="Accounts over their byte quota" desc="Only a migration can put an account here: blobs of a repo moving in are never refused. Raise the account's quota or ask it to delete media.">
      <ErrorNotice error={l.error} />
      {!l.data ? (
        <Loading />
      ) : l.data.accounts.length === 0 ? (
        <Empty title="No account is over its quota" />
      ) : (
        <table className="data">
          <tbody>
            {l.data.accounts.map((a) => (
              <tr key={a.did}>
                <td>
                  <Link to={lookupUrl(a.did)} className="mono small">
                    {a.did}
                  </Link>
                </td>
                <td className="num">
                  {fmtGB(a.bytes)} / {fmtGB(a.limit)}
                </td>
                <td className="nowrap small muted">since {fmtTime(a.at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  )
}

// ---------------------------------------------------------------- cases

const STATUS_KIND: Record<Case['status'], 'ok' | 'warn' | 'bad' | 'idle'> = { open: 'warn', actioned: 'bad', dismissed: 'idle', restored: 'ok' }

function Cases() {
  const [status, setStatus] = useState('')
  const l = useLoad<{ cases: Case[] }>(() => admin('vlpds.admin.listCases', { params: { status } }), [status])
  const [source, setSource] = useState('')
  const [note, setNote] = useState('')
  const create = useAction(async () => {
    const c: Case = await admin('vlpds.admin.createCase', { body: { source: source.trim(), note: note.trim() || undefined } })
    navigate(`/admin/moderation/cases/${c.id}`)
  })
  return (
    <div className="mod-grid">
      <div className="stack">
        <div className="seg" role="group" aria-label="Status">
          {['', 'open', 'actioned', 'restored', 'dismissed'].map((s) => (
            <button key={s} type="button" aria-pressed={status === s} onClick={() => setStatus(s)}>
              {s ? `${s[0].toUpperCase()}${s.slice(1)}` : 'All'}
            </button>
          ))}
        </div>
        <ErrorNotice error={l.error} />
        <Panel flush>
          {!l.data ? (
            <Loading />
          ) : l.data.cases.length === 0 ? (
            <Empty title="No cases">Log a notice from email (DMCA, abuse report, law enforcement) to track it here.</Empty>
          ) : (
            <table className="data">
              <thead>
                <tr>
                  <th>Opened</th>
                  <th>Source</th>
                  <th>Subjects</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {l.data.cases.map((c) => {
                  const to = `/admin/moderation/cases/${c.id}`
                  return (
                    <tr key={c.id} className="link" onClick={() => navigate(to)}>
                      <td className="nowrap">{fmtTime(c.createdAt)}</td>
                      <td className="break">
                        <Link to={to} onClick={(e) => e.stopPropagation()}>
                          {c.source}
                        </Link>
                      </td>
                      <td className="num">{c.subjects.length}</td>
                      <td>
                        <Status kind={STATUS_KIND[c.status]}>{c.status}</Status>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          )}
        </Panel>
      </div>
      <Panel title="New case" desc="One per notice or report: who sent it and what it claims.">
        <form
          onSubmit={(e) => {
            e.preventDefault()
            create.run()
          }}
        >
          <ErrorNotice error={create.error} />
          <Field label="Source" hint="e.g. DMCA notice from Example Studios, received by email">
            <input type="text" value={source} onChange={(e) => setSource(e.target.value)} required />
          </Field>
          <Field label="First note (optional)">
            <textarea value={note} onChange={(e) => setNote(e.target.value)} rows={3} />
          </Field>
          <div className="row end">
            <button className="btn primary" disabled={create.busy || !source.trim()}>
              {create.busy && <Spinner />}
              Open case
            </button>
          </div>
        </form>
      </Panel>
    </div>
  )
}

export function CaseDetail({ id }: { id: string }) {
  const l = useLoad<Case>(() => admin('vlpds.admin.getCase', { params: { id } }), [id])
  const [note, setNote] = useState('')
  const [add, setAdd] = useState('')
  const upd = useAction(async (body: Record<string, unknown>) => {
    await admin('vlpds.admin.updateCase', { body: { id, ...body } })
    l.reload()
  })
  const addSubject = useAction(async () => {
    const r: Resolved = await admin('vlpds.admin.resolveSubject', { params: { q: add.trim() } })
    await admin('vlpds.admin.updateCase', { body: { id, addSubject: { kind: r.kind, did: r.did, uri: r.uri, cid: r.cid } } })
    setAdd('')
    l.reload()
  })
  const c = l.data
  return (
    <>
      <div className="pagehead">
        <nav className="crumbs" aria-label="Breadcrumb">
          <Link to="/admin/moderation?tab=cases">Moderation cases</Link>
        </nav>
        <h1 className="break">{c ? c.source : id}</h1>
      </div>
      <ErrorNotice error={l.error || upd.error} />
      {!c ? (
        !l.error && <Loading />
      ) : (
        <div className="mod-grid">
          <div className="stack">
            <Panel title="Subjects" desc="Open one to review it and act; actions taken from here are linked to this case.">
              {c.subjects.length === 0 ? (
                <p className="muted small">None yet.</p>
              ) : (
                <ul className="mod-list">
                  {c.subjects.map((s) => (
                    <li key={`${s.kind}:${subjectQuery(s)}`}>
                      <span className="pill">{s.kind}</span> <SubjectText s={s} />{' '}
                      <Link className="btn sm" to={lookupUrl(subjectQuery(s), c.id)}>
                        Review
                      </Link>{' '}
                      <button type="button" className="btn sm" onClick={() => upd.run({ removeSubject: s })}>
                        Remove
                      </button>
                    </li>
                  ))}
                </ul>
              )}
              <form
                className="toolbar"
                onSubmit={(e) => {
                  e.preventDefault()
                  addSubject.run()
                }}
              >
                <input type="search" value={add} onChange={(e) => setAdd(e.target.value)} placeholder="Add a subject: URL, at:// URI, handle, DID + CID" aria-label="Add a subject" />
                <button className="btn" disabled={addSubject.busy || !add.trim()}>
                  Add
                </button>
              </form>
              <ErrorNotice error={addSubject.error} />
            </Panel>
            <Panel title="Actions" flush>
              {c.actions.length === 0 ? (
                <Empty title="No takedowns or restores yet" />
              ) : (
                <table className="data">
                  <tbody>
                    {c.actions.map((a) => (
                      <tr key={a.auditId}>
                        <td className="nowrap">{fmtTime(a.at)}</td>
                        <td>
                          <span className={`pill${a.action === 'takedown' ? ' danger' : ''}`}>{a.action}</span>
                        </td>
                        <td>
                          <SubjectText s={a.subject} />
                        </td>
                        <td className="break">{a.reason}</td>
                        <td className="small muted">{a.actor}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </Panel>
          </div>
          <div className="stack">
            <Panel title="Status">
              <div className="row">
                <Status kind={STATUS_KIND[c.status]}>{c.status}</Status>
                <span className="small muted">
                  opened {fmtTime(c.createdAt)} · updated {fmtTime(c.updatedAt)} · id <CopyText text={c.id} />
                </span>
              </div>
              <div className="seg" role="group" aria-label="Set status" style={{ marginTop: 12 }}>
                {(['open', 'actioned', 'restored', 'dismissed'] as const).map((s) => (
                  <button key={s} type="button" aria-pressed={c.status === s} onClick={() => c.status !== s && upd.run({ status: s })}>
                    {s}
                  </button>
                ))}
              </div>
            </Panel>
            <Panel title="Notes">
              <ol className="mod-notes">
                {c.notes.map((n, i) => (
                  <li key={i}>
                    <div className="small muted">
                      {fmtTime(n.at)} · {n.actor}
                      {n.ip && ` (${n.ip})`}
                    </div>
                    <div className="break mod-note">{n.text}</div>
                  </li>
                ))}
              </ol>
              <form
                onSubmit={(e) => {
                  e.preventDefault()
                  if (note.trim()) upd.run({ note: note.trim() }).then(() => setNote(''))
                }}
              >
                <Field label="Add a note" hint="e.g. counter-notice received; restore window ends 2026-10-17">
                  <textarea value={note} onChange={(e) => setNote(e.target.value)} rows={3} />
                </Field>
                <div className="row end">
                  <button className="btn" disabled={upd.busy || !note.trim()}>
                    Add note
                  </button>
                </div>
              </form>
            </Panel>
          </div>
        </div>
      )}
    </>
  )
}
