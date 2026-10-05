import { useEffect, useRef, useState, type ReactNode } from 'react'
import { Notice, Spinner } from './ui'
import { Download } from './icons'
import { fmtBytes, fmtNum } from '../lib/format'
import {
  backupName,
  canStreamToDisk,
  estimateBytes,
  pickBackupFile,
  writeBackup,
  type BackupOptions,
  type BackupProgress,
  type BackupSource,
  type BackupSummary,
} from '../lib/backup'
import { errText } from '../lib/xrpc'

/** Above this, an in-memory ZIP risks the tab running out of memory. */
const MEMORY_WARN = 1.5 * 2 ** 30

type State =
  | { s: 'idle' }
  | { s: 'running'; p: BackupProgress }
  | { s: 'done'; sum: BackupSummary }
  | { s: 'cancelled' }
  | { s: 'error'; error: unknown }

/** "Download a backup": one ZIP of the account, streamed to disk where the
 * browser allows. `simple` words it without protocol terms. `leading` and
 * `trailing` flank the button (the migration's "Skip" and "Continue"). */
export function BackupBox({
  source,
  simple,
  extras,
  recoveryKey,
  onSaved,
  leading,
  trailing,
  primary = true,
}: {
  source: BackupSource
  simple?: boolean
  extras?: BackupOptions['extras']
  recoveryKey?: { privateHex: string; didKey: string }
  onSaved?: (sum: BackupSummary) => void
  leading?: ReactNode
  trailing?: ReactNode
  primary?: boolean
}) {
  const [st, setSt] = useState<State>({ s: 'idle' })
  const [withKey, setWithKey] = useState(false)
  const [estimate, setEstimate] = useState<number>()
  const ac = useRef<AbortController | undefined>(undefined)
  const streams = canStreamToDisk()
  const host = source.base ? new URL(source.base).host : location.host

  useEffect(() => {
    if (streams) return
    let live = true
    source
      .call('com.atproto.server.checkAccountStatus')
      .then((x) => live && setEstimate(estimateBytes(x)))
      .catch(() => {})
    return () => {
      live = false
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [source.did, source.base])
  useEffect(() => () => ac.current?.abort(), [])

  const start = async () => {
    const name = backupName(source.handle)
    let target: Awaited<ReturnType<typeof pickBackupFile>> = null
    try {
      target = await pickBackupFile(name)
    } catch (e) {
      if (e instanceof DOMException && e.name === 'AbortError') return
      // refused (a sandboxed frame, a policy): build it in memory instead
    }
    const c = new AbortController()
    ac.current = c
    setSt({ s: 'running', p: { phase: 'prepare', bytes: 0, blobsDone: 0, missing: 0 } })
    try {
      const sum = await writeBackup(
        {
          source,
          recoveryKey: recoveryKey && withKey ? recoveryKey : undefined,
          extras,
          signal: c.signal,
          onProgress: (p) => !c.signal.aborted && setSt({ s: 'running', p }),
        },
        target,
        name,
      )
      setSt({ s: 'done', sum })
      onSaved?.(sum)
    } catch (error) {
      setSt(c.signal.aborted ? { s: 'cancelled' } : { s: 'error', error })
    }
  }

  const running = st.s === 'running'
  return (
    <div className="bk">
      {!streams && estimate !== undefined && st.s === 'idle' && (
        <Notice kind={estimate > MEMORY_WARN ? 'warn' : 'info'}>
          <p>
            This browser puts the whole file together in memory before saving it{estimate > 50e6 ? <> (roughly {fmtBytes(estimate)} here)</> : null}.
            {estimate > MEMORY_WARN && (
              <>
                {' '}
                That may be more than it can hold. On a computer, Chrome or Edge save it straight to disk instead.
              </>
            )}
          </p>
        </Notice>
      )}
      {recoveryKey && (
        <div className="bk-key">
          <label className="check">
            <input type="checkbox" name="backup-recovery-key" checked={withKey} disabled={running} onChange={(e) => setWithKey(e.target.checked)} />
            <span>Include my recovery private key in the backup</span>
          </label>
          {withKey && (
            <Notice kind="warn">
              <p>
                <b>Anyone with this file can take over your identity.</b> Only include the key if you will keep the backup offline and private (an
                encrypted drive, not a shared folder or email).
              </p>
            </Notice>
          )}
        </div>
      )}
      {st.s === 'running' && <Running p={st.p} simple={simple} host={host} />}
      {st.s === 'done' && <Done sum={st.sum} simple={simple} host={host} />}
      {st.s === 'cancelled' && <Notice>Stopped. Nothing was saved.</Notice>}
      {st.s === 'error' && (
        <Notice kind="err">
          <p>
            {simple ? "The copy couldn't be finished. Try again in a moment." : "The backup couldn't be finished."} <span className="small">({errText(st.error)})</span>
          </p>
        </Notice>
      )}
      <div className={leading || trailing ? 'row between' : 'row'}>
        {leading ?? (trailing ? <span /> : null)}
        <div className="row">
          {running ? (
            <button type="button" className="btn" onClick={() => ac.current?.abort()}>
              Cancel
            </button>
          ) : (
            <button type="button" className={primary ? 'btn primary' : 'btn'} onClick={start} name="backup">
              <Download />
              {st.s === 'done' ? (simple ? 'Save another copy' : 'Download again') : simple ? 'Save a copy (recommended)' : 'Download a backup (.zip)'}
            </button>
          )}
          {trailing}
        </div>
      </div>
    </div>
  )
}

function Running({ p, simple, host }: { p: BackupProgress; simple?: boolean; host: string }) {
  const media = (n: number) => (n === 1 ? 'photo or video' : 'photos & videos')
  const line =
    p.phase === 'prepare'
      ? simple
        ? 'Getting ready…'
        : 'Reading the session, preferences, DID document and PLC log…'
      : p.phase === 'repo'
        ? simple
          ? 'Saving your posts, follows and likes…'
          : 'Writing repo.car…'
        : p.phase === 'blobs'
          ? simple
            ? `${fmtNum(p.blobsDone)}${p.blobsTotal !== undefined ? ` of ${fmtNum(p.blobsTotal)}` : ''} ${media(p.blobsTotal ?? p.blobsDone)} saved`
            : `Blobs: ${fmtNum(p.blobsDone)}${p.blobsTotal !== undefined ? ` of ${fmtNum(p.blobsTotal)}` : ''}${p.missing ? ` (${fmtNum(p.missing)} missing)` : ''}`
          : 'Finishing…'
  return (
    <div className="bk-progress" aria-live="polite">
      <div className="row between small">
        <span>
          <Spinner /> {line}
        </span>
        <span className="muted">{fmtBytes(p.bytes)} written</span>
      </div>
      <div className="mig-bar" role="progressbar" aria-label="Backup">
        {p.phase === 'blobs' && p.blobsTotal ? (
          <div style={{ width: `${Math.min(100, (p.blobsDone / p.blobsTotal) * 100)}%` }} />
        ) : (
          <div className={p.phase === 'blobs' && p.blobsTotal === 0 ? '' : 'indeterminate'} style={{ width: '100%' }} />
        )}
      </div>
      {p.pausedUntil && p.pausedUntil > Date.now() && (
        <p className="small muted">
          {host} asked us to slow down. Resuming by {new Date(p.pausedUntil).toLocaleTimeString()}; keep this tab open.
        </p>
      )}
    </div>
  )
}

function Done({ sum, simple, host }: { sum: BackupSummary; simple?: boolean; host: string }) {
  const where = sum.streamed ? 'where you chose' : 'to your downloads'
  return (
    <Notice kind={sum.missing.length ? 'warn' : 'ok'}>
      <p>
        Saved <span className="mono">{sum.name}</span> ({fmtBytes(sum.bytes)}) {where}.
        {simple ? ' Keep it somewhere safe.' : ` ${fmtNum(sum.blobs)} blobs included.`}
      </p>
      {sum.missing.length > 0 && (
        <p>
          {simple
            ? `${fmtNum(sum.missing.length)} ${sum.missing.length === 1 ? "photo or video couldn't" : "photos or videos couldn't"} be downloaded from ${host}; the file lists which.`
            : `${fmtNum(sum.missing.length)} blobs ${host} couldn't provide are listed in missing-blobs.txt (first: ${sum.missing[0].cid}: ${sum.missing[0].reason}).`}
        </p>
      )}
      {!simple && sum.notes.length > 0 && (
        <ul className="small">
          {sum.notes.map((n) => (
            <li key={n}>{n}</li>
          ))}
        </ul>
      )}
    </Notice>
  )
}
