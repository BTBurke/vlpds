import { useCallback, useEffect, useMemo, useRef, useState, type FormEvent, type ReactNode } from 'react'
import { CopyText, ErrorNotice, Field, Notice, Spinner, Status, Topbar } from '../../components/ui'
import * as I from '../../components/icons'
import { fmtBytes, fmtNum } from '../../lib/format'
import { useLoad } from '../../lib/hooks'
import { Link, navigate, useSearch } from '../../lib/router'
import { call, errText, setSession, XrpcError } from '../../lib/xrpc'
import {
  authHostFor,
  clearSecrets,
  copyBlobs,
  copyRepo,
  DidWeb,
  findAccount,
  HandleUnresolved,
  keepSignedOp,
  loadSaved,
  Pds,
  pdsOf,
  shortKey,
  signedOp,
  signingKeyOf,
  STEPS,
  storeSaved,
  type AccountStatus,
  type DidDoc,
  type Found,
  type Saved,
  type StepId,
} from './flow'

type Describe = { did: string; availableUserDomains: string[]; inviteCodeRequired?: boolean }

const here = location.host

/** Moves an account from another PDS (the reference implementation, or
 * anything speaking the same XRPC) to this one, in the browser: the old
 * server is called directly (the reference PDS allows CORS from anywhere). */
export function Migrate() {
  const q = useSearch()
  const describe = useLoad<Describe>(() => call('com.atproto.server.describeServer'), [])
  const [saved, setSavedRaw] = useState<Saved | null>(loadSaved)
  // only for "use the same password here"; never stored
  const [oldPassword, setOldPassword] = useState('')
  const [resumeAsked, setResumeAsked] = useState(false)
  const [, bump] = useState(0)

  const update = useCallback((patch: Partial<Saved>) => {
    setSavedRaw((s) => {
      if (!s) return s
      const n = { ...s, ...patch }
      storeSaved(n)
      return n
    })
  }, [])
  const begin = (s: Saved) => {
    clearSecrets()
    storeSaved(s)
    setSavedRaw(s)
  }
  const reset = () => {
    storeSaved(null)
    clearSecrets()
    setSavedRaw(null)
    setOldPassword('')
  }

  const oldPds = useMemo(() => (saved ? new Pds('old', saved.oldPds, saved.did, saved.oldAuth) : null), [saved?.did, saved?.oldPds])
  const newPds = useMemo(() => (saved ? new Pds('new', '', saved.did) : null), [saved?.did])
  const refresh = () => bump((n) => n + 1)

  const invite = q.get('invite') ?? ''
  const step = currentStep(saved)
  const needOld = !!saved && step !== 'find' && step !== 'done' && !(step === 'finish' && saved.oldDeactivated) && !oldPds?.tokens
  const needNew = !!saved?.created && step !== 'done' && !newPds?.tokens
  // a reload in the same tab still has the sessions and goes on; a new tab asks first
  const needResume = !!saved && step !== 'done' && !resumeAsked && (needOld || needNew) && !!saved.checkedAt
  const railStep: StepId = needOld && !saved?.checkedAt ? 'signin' : step

  let body: ReactNode
  if (!describe.data) {
    body = describe.error ? <Notice kind="err">This server's info is unavailable ({errText(describe.error)}). Reload to try again.</Notice> : <Spinner />
  } else if (!saved) {
    body = <FindStep onFound={begin} invite={invite} />
  } else if (needResume) {
    body = <ResumeStep saved={saved} onContinue={() => setResumeAsked(true)} onReset={reset} />
  } else if (needOld) {
    body = <SignInStep side="old" saved={saved} pds={oldPds!} onDone={(pw) => (setOldPassword(pw), refresh())} onReset={reset} />
  } else if (needNew) {
    body = (
      <SignInStep
        side="new"
        saved={saved}
        pds={newPds!}
        onDone={(_pw, out) => {
          update({ newHandle: out.handle })
          refresh()
        }}
        onReset={reset}
      />
    )
  } else if (step === 'check') {
    body = <CheckStep saved={saved} describe={describe.data} oldPds={oldPds!} update={update} onReset={reset} />
  } else if (step === 'handle') {
    body = <HandleStep saved={saved} describe={describe.data} update={update} />
  } else if (step === 'create') {
    body = (
      <CreateStep saved={saved} describe={describe.data} oldPds={oldPds!} newPds={newPds!} oldPassword={oldPassword} invite={invite} update={update} />
    )
  } else if (step === 'copy') {
    body = <CopyStep saved={saved} oldPds={oldPds!} newPds={newPds!} update={update} />
  } else if (step === 'identity') {
    body = <IdentityStep saved={saved} oldPds={oldPds!} newPds={newPds!} update={update} />
  } else if (step === 'finish') {
    body = <FinishStep saved={saved} oldPds={oldPds!} newPds={newPds!} update={update} />
  } else {
    body = <DoneStep saved={saved} newPds={newPds!} onReset={reset} />
  }

  return (
    <>
      <Topbar where="Move here" />
      <main className="mig">
        <aside className="mig-rail" aria-label="Steps">
          <ol>
            {STEPS.map((s, i) => {
              const at = STEPS.findIndex((x) => x.id === railStep)
              const state = railStep === 'done' || i < at ? 'done' : i === at ? 'now' : 'todo'
              return (
                <li key={s.id} className={state} aria-current={state === 'now' ? 'step' : undefined}>
                  <span className="n">{state === 'done' ? <I.Check /> : i + 1}</span>
                  {s.label}
                </li>
              )
            })}
          </ol>
          {saved && <Standing saved={saved} />}
        </aside>
        <section className="mig-main">{body}</section>
      </main>
    </>
  )
}

function currentStep(s: Saved | null): StepId {
  if (!s) return 'find'
  if (s.finishedAt) return 'done'
  if (!s.checkedAt) return 'check'
  if (!s.newHandle) return 'handle'
  if (!s.created) return 'create'
  if (!(s.repoDone && s.blobsDone && s.prefsDone)) return 'copy'
  if (!s.identityDone) return 'identity'
  if (!(s.activated && s.oldDeactivated)) return 'finish'
  return 'done'
}

// ---------------------------------------------------------------- layout bits

function Card({ title, sub, children }: { title: string; sub?: ReactNode; children: ReactNode }) {
  return (
    <div className="mig-card">
      <h1>{title}</h1>
      {sub && <p className="sub">{sub}</p>}
      {children}
    </div>
  )
}

/** Both sides at a glance, so a stop at any point leaves no doubt where the account is. */
function Standing({ saved: s }: { saved: Saved }) {
  const oldHost = hostOf(s.oldPds)
  return (
    <div className="mig-standing" aria-label="Where things stand">
      <h2>Where things stand</h2>
      <dl>
        <dt>{oldHost}</dt>
        <dd>
          {s.oldDeactivated ? <Status kind="idle">Deactivated, kept as a fallback</Status> : <Status kind="ok">Active: your account today</Status>}
        </dd>
        <dt>{here}</dt>
        <dd>
          {s.activated ? (
            <Status kind="ok">Active</Status>
          ) : s.created ? (
            <Status kind="warn">Created, not live yet</Status>
          ) : (
            <Status kind="idle">Nothing yet</Status>
          )}
        </dd>
        <dt>Identity points to</dt>
        <dd className="mono">{s.identityDone ? here : oldHost}</dd>
      </dl>
      {!s.identityDone && <p>Until your identity moves, nothing changes for anyone else. You can stop here and keep using {oldHost}.</p>}
    </div>
  )
}

const hostOf = (u: string) => {
  try {
    return new URL(u).host
  } catch {
    return u
  }
}

function Checkline({ state, title, children }: { state: 'ok' | 'warn' | 'bad' | 'wait'; title: string; children?: ReactNode }) {
  return (
    <li className={`mig-check ${state}`}>
      <span className="icon">{state === 'wait' ? <Spinner /> : state === 'ok' ? <I.Check /> : <I.Alert />}</span>
      <div>
        <strong>{title}</strong>
        {children && <div className="small muted">{children}</div>}
      </div>
    </li>
  )
}

function Bar({ value, total, label }: { value: number; total?: number; label: string }) {
  const pct = total ? Math.min(100, (value / total) * 100) : undefined
  return (
    <div className="mig-bar" role="progressbar" aria-label={label} aria-valuenow={pct !== undefined ? Math.round(pct) : undefined} aria-valuemin={0} aria-valuemax={100}>
      <div style={{ width: pct !== undefined ? `${pct}%` : '100%' }} className={pct === undefined ? 'indeterminate' : ''} />
    </div>
  )
}

/** Runs `fn` once per key even when React mounts twice (StrictMode) or the
 * component remounts mid-run: the second caller gets the same promise. */
const jobs = new Map<string, Promise<unknown>>()
function once<T>(key: string, fn: () => Promise<T>): Promise<T> {
  let p = jobs.get(key) as Promise<T> | undefined
  if (!p) {
    p = fn().finally(() => jobs.delete(key))
    jobs.set(key, p)
  }
  return p
}

// ---------------------------------------------------------------- 1. find

function FindStep({ onFound, invite }: { onFound: (s: Saved) => void; invite: string }) {
  const [ident, setIdent] = useState('')
  const [host, setHost] = useState('')
  const [needHost, setNeedHost] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<unknown>()
  const [already, setAlready] = useState<Found>()

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    setBusy(true)
    setError(undefined)
    setAlready(undefined)
    try {
      const f = await findAccount(ident, needHost ? host : undefined)
      if (f.pds === location.origin) {
        setAlready(f)
        return
      }
      let domains: string[] = []
      try {
        domains = (await call('com.atproto.server.describeServer', { base: f.pds })).availableUserDomains ?? []
      } catch (err) {
        throw new Error(`Your account's server, ${hostOf(f.pds)}, isn't answering (${errText(err)}). Check that it is online and try again.`)
      }
      onFound({
        did: f.did,
        oldPds: f.pds,
        oldAuth: authHostFor(f.pds),
        oldHandle: f.handle,
        oldDomains: domains,
        invite: invite || undefined,
        startedAt: Date.now(),
      })
    } catch (err) {
      if (err instanceof HandleUnresolved) setNeedHost(true)
      setError(err)
    } finally {
      setBusy(false)
    }
  }

  return (
    <Card
      title="Move your account here"
      sub={
        <>
          Bring your Bluesky / atproto account to {here}. Your posts, follows, likes, images and settings come along, and your followers keep
          following you. It takes a few minutes, and nothing changes for anyone else until the last step.
        </>
      }
    >
      <div className="mig-need">
        <strong>You'll need</strong>
        <ul>
          <li>Your account's <b>main password</b> (not an app password).</li>
          <li>Access to the <b>email</b> on your account: your current server emails you a confirmation code.</li>
          <li>To keep this tab open while your data copies.</li>
        </ul>
      </div>
      <form onSubmit={submit}>
        {error instanceof DidWeb ? (
          <Notice kind="err">
            <p>
              <b>{error.did}</b> is a did:web. Its DID document lives on a web server you control, so this page can't move it for you. Copy your data
              with the steps below, then update the <span className="mono">#atproto_pds</span> service and <span className="mono">#atproto</span> key in
              your <span className="mono">did.json</span> yourself.
            </p>
          </Notice>
        ) : error instanceof HandleUnresolved ? (
          <Notice kind="warn">We couldn't look up @{error.handle} from here. Enter your DID instead, or the address of the server your account is on now.</Notice>
        ) : (
          <ErrorNotice error={error} />
        )}
        {already && (
          <Notice kind="ok">
            <p>
              @{already.handle} already lives on {here}. <Link to="/account">Sign in to manage it.</Link>
            </p>
          </Notice>
        )}
        <Field label="Your handle or DID" hint="For example alice.bsky.social, your own domain, or did:plc:…">
          <input
            type="text"
            name="identifier"
            value={ident}
            onChange={(e) => setIdent(e.target.value)}
            autoCapitalize="none"
            autoComplete="username"
            spellCheck={false}
            required
            autoFocus
          />
        </Field>
        {needHost && (
          <Field label="Your current server" hint="Where you sign in today, e.g. bsky.social or pds.example.com.">
            <input type="text" name="host" value={host} onChange={(e) => setHost(e.target.value)} autoCapitalize="none" spellCheck={false} />
          </Field>
        )}
        <div className="row end">
          <button type="submit" className="btn primary" disabled={busy}>
            {busy && <Spinner />}
            Find my account
          </button>
        </div>
      </form>
    </Card>
  )
}

// ---------------------------------------------------------------- resume / sign in

function ResumeStep({ saved, onContinue, onReset }: { saved: Saved; onContinue: () => void; onReset: () => void }) {
  const [sure, setSure] = useState(false)
  return (
    <Card title={`Continue moving @${saved.oldHandle}?`} sub={<>You started moving this account on {new Date(saved.startedAt).toLocaleString()}. Pick up where you left off.</>}>
      {saved.created && !saved.identityDone && (
        <Notice kind="info">An inactive copy of your account already exists on {here}. Continuing reuses it.</Notice>
      )}
      {saved.identityDone && (
        <Notice kind="warn">Your identity already points to {here}. Continue to finish switching over; starting over isn't possible past this point.</Notice>
      )}
      <div className="row between">
        {!saved.identityDone ? (
          sure ? (
            <button type="button" className="btn danger" onClick={onReset}>
              Yes, forget this move
            </button>
          ) : (
            <button type="button" className="btn quiet" onClick={() => setSure(true)}>
              Start over with another account
            </button>
          )
        ) : (
          <span />
        )}
        <button type="button" className="btn primary" onClick={onContinue} autoFocus>
          Continue
        </button>
      </div>
    </Card>
  )
}

function SignInStep({
  side,
  saved,
  pds,
  onDone,
  onReset,
}: {
  side: 'old' | 'new'
  saved: Saved
  pds: Pds
  onDone: (password: string, session: { handle: string }) => void
  onReset: () => void
}) {
  const [password, setPassword] = useState('')
  const [code, setCode] = useState('')
  const [needCode, setNeedCode] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<unknown>()
  const host = side === 'old' ? hostOf(saved.oldAuth) : here

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    setBusy(true)
    setError(undefined)
    try {
      const out = await pds.login(password, needCode ? code.trim() : undefined)
      onDone(password, out)
    } catch (err) {
      if (err instanceof XrpcError && err.error === 'AuthFactorTokenRequired') {
        setNeedCode(true)
        setError(undefined)
      } else setError(err)
    } finally {
      setBusy(false)
    }
  }

  return (
    <Card
      title={side === 'old' ? `Sign in to ${host}` : `Sign in to your new account`}
      sub={
        side === 'old' ? (
          <>
            Signing in as <b>@{saved.oldHandle}</b> <span className="mono small">({saved.did})</span>. Your password goes only to {host}; this page keeps it in
            memory for the move and never stores it.
          </>
        ) : (
          <>The account you created here for @{saved.newHandle ?? saved.oldHandle} is waiting. Enter the password you chose for it.</>
        )
      }
    >
      <form onSubmit={submit}>
        {error instanceof XrpcError && error.error === 'AuthenticationRequired' ? (
          <Notice kind="err">
            Wrong password.{' '}
            {side === 'old'
              ? 'Use your main password. App passwords (xxxx-xxxx-xxxx-xxxx) cannot move an account.'
              : 'Use the password you set when creating the account here.'}
          </Notice>
        ) : (
          <ErrorNotice error={error} />
        )}
        {needCode && (
          <Notice kind="info">
            {host} emailed you a sign-in code because your account has email two-factor turned on. Enter it below.
          </Notice>
        )}
        <Field label={side === 'old' ? 'Main password' : 'Password'}>
          <input type="password" name="password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="current-password" required autoFocus={!needCode} />
        </Field>
        {needCode && (
          <Field label="Sign-in code from your email" hint="It looks like ABCDE-12345.">
            <input type="text" name="code" value={code} onChange={(e) => setCode(e.target.value)} autoComplete="one-time-code" spellCheck={false} required autoFocus />
          </Field>
        )}
        <div className="row between">
          {!saved.created ? (
            <button type="button" className="btn quiet" onClick={onReset}>
              Use a different account
            </button>
          ) : (
            <span />
          )}
          <button type="submit" className="btn primary" disabled={busy}>
            {busy && <Spinner />}
            Sign in
          </button>
        </div>
      </form>
    </Card>
  )
}

// ---------------------------------------------------------------- 3. checks

type CheckResult = { state: 'ok' | 'warn' | 'bad' | 'wait'; title: string; detail?: ReactNode }

function CheckStep({
  saved,
  describe,
  oldPds,
  update,
  onReset,
}: {
  saved: Saved
  describe: Describe
  oldPds: Pds
  update: (p: Partial<Saved>) => void
  onReset: () => void
}) {
  const [checks, setChecks] = useState<Record<string, CheckResult>>({})
  const [tick, setTick] = useState(0)
  const [existing, setExisting] = useState<'none' | 'deactivated' | 'active'>()
  const put = (k: string, c: CheckResult) => setChecks((x) => ({ ...x, [k]: c }))

  useEffect(() => {
    setChecks({})
    const did = saved.did
    put('server', { state: 'ok', title: `${here} is ready`, detail: describe.inviteCodeRequired ? 'New accounts here need an invite code.' : 'Open to new accounts.' })
    put('did', { state: 'ok', title: 'Your identity can move', detail: <>A did:plc: the PLC directory records which server hosts it.</> })
    put('here', { state: 'wait', title: 'Looking for an earlier copy here' })
    call('com.atproto.sync.getRepoStatus', { params: { did } })
      .then((r) => {
        if (r.active) {
          setExisting('active')
          put('here', { state: 'bad', title: 'This account already lives here', detail: <>It's active on {here}. Sign in at <Link to="/account">your account page</Link>.</> })
        } else {
          setExisting('deactivated')
          put('here', { state: 'ok', title: 'An earlier, unfinished copy is here', detail: "We'll reuse it: you'll sign in to it with the password you chose then." })
        }
      })
      .catch((e) => {
        if (e instanceof XrpcError && e.status === 400) {
          setExisting('none')
          put('here', { state: 'ok', title: 'Not on this server yet' })
        } else put('here', { state: 'bad', title: "Couldn't check this server", detail: errText(e) })
      })
    put('old', { state: 'wait', title: `Checking your account on ${hostOf(saved.oldPds)}` })
    oldPds
      .call<AccountStatus>('com.atproto.server.checkAccountStatus')
      .then((st) => {
        if (!st.activated) put('old', { state: 'warn', title: `Your account on ${hostOf(saved.oldPds)} is deactivated`, detail: 'It can still be moved, but it is not live there now.' })
        else put('old', { state: 'ok', title: `Your account on ${hostOf(saved.oldPds)} is active` })
        const big = st.indexedRecords > 500_000 || st.expectedBlobs > 20_000
        put('size', {
          state: big ? 'warn' : 'ok',
          title: `${fmtNum(st.indexedRecords)} records, ${fmtNum(st.expectedBlobs)} images and videos`,
          detail: big
            ? 'A large account: copying can take a long time. Keep this tab open; if it stops, reload and it picks up where it was.'
            : 'About a minute or two to copy.',
        })
      })
      .catch((e) => put('old', { state: 'bad', title: `Couldn't read your account on ${hostOf(saved.oldPds)}`, detail: errText(e) }))
    put('email', { state: 'wait', title: 'Checking your email' })
    oldPds
      .call('com.atproto.server.getSession')
      .then((s) => {
        if (!s.email) put('email', { state: 'bad', title: 'No email on your account', detail: `Moving needs a code emailed by ${hostOf(saved.oldAuth)}. Add an email there first.` })
        else {
          update({ email: saved.email ?? s.email })
          put('email', {
            state: s.emailConfirmed === false ? 'warn' : 'ok',
            title: `Codes will go to ${maskEmail(s.email)}`,
            detail: s.emailConfirmed === false ? 'This address is not confirmed; make sure you can receive mail there.' : undefined,
          })
        }
      })
      .catch((e) => put('email', { state: 'bad', title: "Couldn't read your account's email", detail: errText(e) }))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tick])

  const list = Object.values(checks)
  const pending = list.some((c) => c.state === 'wait')
  const blocked = list.some((c) => c.state === 'bad')
  return (
    <Card title="Pre-flight checks" sub="Nothing has been changed anywhere yet. These checks make sure the move can finish.">
      <ul className="mig-checks">
        {list.map((c) => (
          <Checkline key={c.title} state={c.state} title={c.title}>
            {c.detail}
          </Checkline>
        ))}
      </ul>
      <div className="row between">
        <button type="button" className="btn quiet" onClick={onReset}>
          Cancel
        </button>
        <div className="row">
          {blocked && (
            <button type="button" className="btn" onClick={() => setTick((t) => t + 1)}>
              Check again
            </button>
          )}
          <button
            type="button"
            className="btn primary"
            disabled={pending || blocked}
            onClick={() => update({ checkedAt: Date.now(), ...(existing === 'deactivated' ? { created: true, newHandle: saved.newHandle ?? saved.oldHandle } : {}) })}
          >
            Continue
          </button>
        </div>
      </div>
    </Card>
  )
}

function maskEmail(e: string) {
  const [u, d] = e.split('@')
  return d ? `${u.slice(0, 1)}•••@${d}` : e
}

// ---------------------------------------------------------------- 4. handle

function HandleStep({ saved, describe, update }: { saved: Saved; describe: Describe; update: (p: Partial<Saved>) => void }) {
  const domain = describe.availableUserDomains[0] ?? ''
  const provided = saved.oldDomains.some((d) => saved.oldHandle.endsWith(d))
  const custom = !provided && !saved.oldHandle.startsWith('did:') && saved.oldHandle !== 'handle.invalid'
  const [mode, setMode] = useState<'keep' | 'new'>(custom ? 'keep' : 'new')
  const [name, setName] = useState(() => (saved.oldHandle.split('.')[0] || '').replace(/[^a-z0-9-]/gi, '').slice(0, 18).toLowerCase())
  const full = `${name.trim().toLowerCase()}${domain}`
  const [avail, setAvail] = useState<{ handle: string; ok: boolean; suggestions: string[] } | { error: unknown }>()
  const keepCheck = useLoad(
    () =>
      custom
        ? call('com.atproto.identity.resolveHandle', { params: { handle: saved.oldHandle } })
            .then((r) => r.did === saved.did)
            .catch(() => false)
        : Promise.resolve(false),
    [],
  )

  useEffect(() => {
    if (mode !== 'new' || name.trim().length < 3) {
      setAvail(undefined)
      return
    }
    let live = true
    const t = setTimeout(() => {
      call('com.atproto.temp.checkHandleAvailability', { params: { handle: full } })
        .then((r) => {
          if (!live) return
          const ok = String(r.result?.$type ?? '').endsWith('#resultAvailable')
          setAvail({ handle: full, ok, suggestions: (r.result?.suggestions ?? []).map((s: any) => s.handle) })
        })
        .catch((error) => live && setAvail({ error }))
    }, 350)
    return () => {
      live = false
      clearTimeout(t)
    }
  }, [mode, full, name])

  const ready = mode === 'keep' || (avail && 'ok' in avail && avail.ok && avail.handle === full)
  return (
    <Card title="Choose your handle" sub="Your handle is the name people see and mention. Your followers and DID stay the same whichever you pick.">
      {provided && (
        <Notice kind="info">
          <b>@{saved.oldHandle}</b> belongs to {hostOf(saved.oldPds)}, so it stops working when you leave. Pick a new one here; you can switch to your own
          domain any time later.
        </Notice>
      )}
      {custom && (
        <label className="mig-choice">
          <input type="radio" name="handle-mode" checked={mode === 'keep'} onChange={() => setMode('keep')} />
          <div>
            <strong>Keep @{saved.oldHandle}</strong>
            {keepCheck.loading ? (
              <span className="small muted"> Checking it…</span>
            ) : keepCheck.data ? (
              <div className="small muted">
                It points to your account through DNS or your own website, not through {hostOf(saved.oldPds)}, so it keeps working. Nothing to change.
              </div>
            ) : (
              <div className="small">
                <Status kind="warn">We couldn't confirm it resolves.</Status> Check the <span className="mono">_atproto.{saved.oldHandle}</span> TXT record
                (<span className="mono">did={saved.did}</span>) or <span className="mono">https://{saved.oldHandle}/.well-known/atproto-did</span>. If neither
                is set up, pick a handle here instead.
              </div>
            )}
          </div>
        </label>
      )}
      <label className={custom ? 'mig-choice' : 'mig-choice solo'}>
        {custom && <input type="radio" name="handle-mode" checked={mode === 'new'} onChange={() => setMode('new')} />}
        <div style={{ flex: 1 }}>
          {custom && <strong>Use a handle on {here}</strong>}
          <Field label={custom ? '' : 'New handle'} hint="3 to 18 letters, digits or hyphens.">
            <span className="affix">
              <input
                type="text"
                name="handle"
                value={name}
                onChange={(e) => {
                  setName(e.target.value)
                  setMode('new')
                }}
                autoCapitalize="none"
                spellCheck={false}
                minLength={3}
                maxLength={18}
              />
              <span className="mono">{domain}</span>
            </span>
          </Field>
          {mode === 'new' && avail && 'error' in avail && <ErrorNotice error={avail.error} />}
          {mode === 'new' && avail && 'ok' in avail && avail.handle === full && (
            <div className="small">
              {avail.ok ? (
                <Status kind="ok">@{full} is available</Status>
              ) : (
                <>
                  <Status kind="bad">@{full} is taken.</Status>
                  {avail.suggestions.length > 0 && (
                    <>
                      {' '}
                      Try{' '}
                      {avail.suggestions.map((s) => (
                        <button key={s} type="button" className="btn quiet mig-sugg" onClick={() => setName(s.slice(0, -domain.length))}>
                          {s}
                        </button>
                      ))}
                    </>
                  )}
                </>
              )}
            </div>
          )}
        </div>
      </label>
      <div className="row end">
        <button type="button" className="btn primary" disabled={!ready} onClick={() => update({ newHandle: mode === 'keep' ? saved.oldHandle : full })}>
          Continue
        </button>
      </div>
    </Card>
  )
}

// ---------------------------------------------------------------- 5. create

function CreateStep({
  saved,
  describe,
  oldPds,
  newPds,
  oldPassword,
  invite: urlInvite,
  update,
}: {
  saved: Saved
  describe: Describe
  oldPds: Pds
  newPds: Pds
  oldPassword: string
  invite: string
  update: (p: Partial<Saved>) => void
}) {
  const [email, setEmail] = useState(saved.email ?? '')
  const [same, setSame] = useState(!!oldPassword)
  const [password, setPassword] = useState('')
  const [invite, setInvite] = useState(saved.invite ?? urlInvite)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<unknown>()
  const [existsAsk, setExistsAsk] = useState(false)

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    setBusy(true)
    setError(undefined)
    const pw = same ? oldPassword : password
    try {
      // fresh each try: service tokens live about a minute
      const { token } = await oldPds.call('com.atproto.server.getServiceAuth', {
        params: { aud: describe.did, lxm: 'com.atproto.server.createAccount' },
      })
      try {
        const out = await call('com.atproto.server.createAccount', {
          auth: `Bearer ${token}`,
          body: { did: saved.did, handle: saved.newHandle, email: email.trim(), password: pw, inviteCode: invite.trim() || undefined },
        })
        newPds.adopt(out)
      } catch (err) {
        // made by an earlier try whose answer never arrived: sign in to it
        if (!(err instanceof XrpcError && /already exists/i.test(err.message))) throw err
        try {
          const out = await newPds.login(pw)
          update({ created: true, newHandle: out.handle, email: email.trim(), invite: undefined })
          return
        } catch (e2) {
          if (e2 instanceof XrpcError && e2.error === 'AuthenticationRequired') {
            setExistsAsk(true)
            return
          }
          throw e2
        }
      }
      update({ created: true, email: email.trim(), invite: undefined })
    } catch (err) {
      setError(err)
    } finally {
      setBusy(false)
    }
  }

  if (existsAsk) {
    return (
      <Card title="Your account here already exists" sub="An earlier attempt created it. Sign in to it with the password you chose then to continue.">
        <SignInInline pds={newPds} onDone={(handle) => update({ created: true, newHandle: handle, email: email.trim(), invite: undefined })} />
      </Card>
    )
  }

  const friendly = createError(error)
  return (
    <Card
      title={`Create @${saved.newHandle} on ${here}`}
      sub={<>This makes an inactive account here with your same DID. Nothing changes on {hostOf(saved.oldPds)} yet.</>}
    >
      <form onSubmit={submit}>
        {friendly ? <Notice kind="err">{friendly}</Notice> : <ErrorNotice error={error} />}
        <Field label="Email" hint="For password resets and security codes on this server.">
          <input type="email" name="email" value={email} onChange={(e) => setEmail(e.target.value)} autoComplete="email" required />
        </Field>
        {oldPassword && (
          <label className="check">
            <input type="checkbox" name="same-password" checked={same} onChange={(e) => setSame(e.target.checked)} />
            <span>Use the same password as on {hostOf(saved.oldAuth)}</span>
          </label>
        )}
        {!same && (
          <Field label="Password for this server" hint="At least 8 characters.">
            <input
              type="password"
              name="new-password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              autoComplete="new-password"
              minLength={8}
              maxLength={256}
              required
            />
          </Field>
        )}
        {(describe.inviteCodeRequired || invite) && (
          <Field label="Invite code" hint={describe.inviteCodeRequired ? 'This server only takes new accounts with an invite.' : undefined}>
            <input
              type="text"
              name="invite"
              value={invite}
              onChange={(e) => setInvite(e.target.value)}
              autoCapitalize="none"
              spellCheck={false}
              required={describe.inviteCodeRequired}
            />
          </Field>
        )}
        <div className="row end">
          <button type="submit" className="btn primary" disabled={busy}>
            {busy && <Spinner />}
            Create my account here
          </button>
        </div>
      </form>
    </Card>
  )
}

function createError(e: unknown): ReactNode {
  if (!(e instanceof XrpcError)) return null
  if (e.error === 'InvalidInviteCode') return 'That invite code is not valid or has been used up. Ask the operator of this server for another.'
  if (e.error === 'HandleNotAvailable') return 'That handle was just taken. Go back and pick another (reload the page, then change it).'
  if (/^Email already taken/.test(e.message)) return 'Another account here uses that email. Use a different address.'
  if (e.error === 'BadJwtSignature' || e.error === 'BadJwtAudience' || e.error === 'BadJwtLexiconMethod' || e.error === 'JwtExpired')
    return `Your current server's authorization didn't check out (${e.error}). Try again.`
  return null
}

function SignInInline({ pds, onDone }: { pds: Pds; onDone: (handle: string) => void }) {
  const [password, setPassword] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<unknown>()
  return (
    <form
      onSubmit={async (e) => {
        e.preventDefault()
        setBusy(true)
        setError(undefined)
        try {
          const out = await pds.login(password)
          onDone(out.handle)
        } catch (err) {
          setError(err)
        } finally {
          setBusy(false)
        }
      }}
    >
      <ErrorNotice error={error} />
      <Field label="Password">
        <input type="password" name="password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="current-password" required autoFocus />
      </Field>
      <div className="row end">
        <button className="btn primary" disabled={busy}>
          {busy && <Spinner />}
          Sign in
        </button>
      </div>
    </form>
  )
}

// ---------------------------------------------------------------- 6. copy

type CopyView = {
  repo: { phase?: 'download' | 'upload' | 'verify'; bytes: number; total?: number; records?: [number, number] }
  blobs: { done: number; total?: number; bytes: number; failed: { cid: string; reason: string }[] }
  prefs?: number
  error?: unknown
  mismatch?: string
  running: boolean
}

function CopyStep({ saved, oldPds, newPds, update }: { saved: Saved; oldPds: Pds; newPds: Pds; update: (p: Partial<Saved>) => void }) {
  const [v, setV] = useState<CopyView>({ repo: { bytes: 0 }, blobs: { done: 0, bytes: 0, failed: [] }, running: false })
  const set = (f: (v: CopyView) => CopyView) => setV((x) => f(x))
  const savedRef = useRef(saved)
  savedRef.current = saved
  const stop = useRef(false)

  const run = useCallback(
    (opts: { acceptMismatch?: boolean; skipFailed?: boolean } = {}) =>
      once(`copy:${saved.did}`, async () => {
        stop.current = false
        set((x) => ({ ...x, running: true, error: undefined, mismatch: undefined }))
        try {
          const s = () => savedRef.current
          if (!s().repoDone) {
            await copyRepo(oldPds, newPds, (phase, bytes, total) => set((x) => ({ ...x, repo: { ...x.repo, phase, bytes, total } })))
            set((x) => ({ ...x, repo: { ...x.repo, phase: 'verify' } }))
            const [a, b] = await Promise.all([
              oldPds.call<AccountStatus>('com.atproto.server.checkAccountStatus'),
              newPds.call<AccountStatus>('com.atproto.server.checkAccountStatus'),
            ])
            set((x) => ({ ...x, repo: { ...x.repo, records: [a.indexedRecords, b.indexedRecords] } }))
            if (a.indexedRecords !== b.indexedRecords && !opts.acceptMismatch) {
              set((x) => ({ ...x, mismatch: `${hostOf(saved.oldPds)} reports ${fmtNum(a.indexedRecords)} records; the copy here has ${fmtNum(b.indexedRecords)}.` }))
              return
            }
            update({ repoDone: true })
          }
          if (!s().blobsDone) {
            const skip = new Set(s().unavailableBlobs ?? [])
            let failed: { cid: string; reason: string }[] = []
            for (let pass = 0; pass < 5 && !stop.current; pass++) {
              const st = await newPds.call<AccountStatus>('com.atproto.server.checkAccountStatus')
              set((x) => ({ ...x, blobs: { ...x.blobs, done: st.importedBlobs, total: st.expectedBlobs } }))
              const r = await copyBlobs(
                oldPds,
                newPds,
                skip,
                (_cid, bytes) => set((x) => ({ ...x, blobs: { ...x.blobs, done: x.blobs.done + 1, bytes: x.blobs.bytes + bytes } })),
                () => stop.current,
              )
              failed = r.failed
              if (r.copied === 0) break
            }
            if (stop.current) return
            set((x) => ({ ...x, blobs: { ...x.blobs, failed } }))
            if (failed.length && !opts.skipFailed) return
            update({ blobsDone: true, unavailableBlobs: failed.length ? [...skip, ...failed.map((f) => f.cid)] : s().unavailableBlobs })
          }
          if (!s().prefsDone) {
            const p = await oldPds.call('app.bsky.actor.getPreferences')
            await newPds.call('app.bsky.actor.putPreferences', { body: { preferences: p.preferences ?? [] } })
            set((x) => ({ ...x, prefs: (p.preferences ?? []).length }))
            update({ prefsDone: true })
          }
        } catch (e) {
          set((x) => ({ ...x, error: e }))
        } finally {
          set((x) => ({ ...x, running: false }))
        }
      }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [saved.did],
  )

  useEffect(() => {
    stop.current = false
    run()
    return () => {
      stop.current = true
    }
  }, [run])

  const r = v.repo
  return (
    <Card title="Copying your data" sub={<>From {hostOf(saved.oldPds)} to {here}. Your account there keeps working meanwhile. If anything stops, reload: it picks up where it was.</>}>
      <ul className="mig-checks">
        <Checkline
          state={saved.repoDone ? 'ok' : v.mismatch ? 'warn' : v.error && !saved.repoDone ? 'bad' : 'wait'}
          title="Posts, follows, likes and profile (your repository)"
        >
          {saved.repoDone ? (
            r.records ? `${fmtNum(r.records[1])} records copied and checked.` : 'Copied.'
          ) : r.phase === 'download' ? (
            <>
              Downloading from {hostOf(saved.oldPds)}: {fmtBytes(r.bytes)}
              {r.total ? ` of ${fmtBytes(r.total)}` : ''}
              <Bar value={r.bytes} total={r.total} label="Repository download" />
            </>
          ) : r.phase === 'upload' ? (
            <>
              Uploading here: {fmtBytes(r.bytes)} of {fmtBytes(r.total ?? 0)}
              <Bar value={r.bytes} total={r.total} label="Repository upload" />
            </>
          ) : r.phase === 'verify' ? (
            'Checking the copy…'
          ) : (
            'Starting…'
          )}
        </Checkline>
        <Checkline
          state={saved.blobsDone ? 'ok' : v.blobs.failed.length && !v.running ? 'warn' : saved.repoDone ? 'wait' : 'wait'}
          title="Images and videos"
        >
          {saved.blobsDone ? (
            saved.unavailableBlobs?.length ? (
              `Copied, except ${saved.unavailableBlobs.length} that ${hostOf(saved.oldPds)} couldn't provide.`
            ) : (
              'All copied.'
            )
          ) : !saved.repoDone ? (
            'After the repository.'
          ) : (
            <>
              {fmtNum(v.blobs.done)}
              {v.blobs.total !== undefined ? ` of ${fmtNum(v.blobs.total)}` : ''} copied{v.blobs.bytes ? ` (${fmtBytes(v.blobs.bytes)} this session)` : ''}
              <Bar value={v.blobs.done} total={v.blobs.total || undefined} label="Images and videos" />
            </>
          )}
        </Checkline>
        <Checkline state={saved.prefsDone ? 'ok' : 'wait'} title="App settings (feeds, muted words, content filters)">
          {saved.prefsDone ? (v.prefs !== undefined ? `${v.prefs} settings copied.` : 'Copied.') : saved.blobsDone ? 'Copying…' : 'Last.'}
        </Checkline>
      </ul>
      {v.mismatch && (
        <Notice kind="warn">
          <p>
            <b>The record counts don't match.</b> {v.mismatch} Retrying usually fixes a copy cut short.
          </p>
          <div className="row">
            <button type="button" className="btn" onClick={() => run()}>
              Copy again
            </button>
            <button type="button" className="btn quiet" onClick={() => run({ acceptMismatch: true })}>
              Continue anyway
            </button>
          </div>
        </Notice>
      )}
      {!v.running && v.blobs.failed.length > 0 && !saved.blobsDone && (
        <Notice kind="warn">
          <p>
            <b>{v.blobs.failed.length} files couldn't be copied.</b> The first: <span className="mono">{v.blobs.failed[0].cid}</span> ({v.blobs.failed[0].reason}
            ). If {hostOf(saved.oldPds)} no longer has them, they'd be missing here too.
          </p>
          <div className="row">
            <button type="button" className="btn" onClick={() => run()}>
              Retry
            </button>
            <button type="button" className="btn quiet" onClick={() => run({ skipFailed: true })}>
              Continue without them
            </button>
          </div>
        </Notice>
      )}
      {!!v.error && !v.running && (
        <>
          <ErrorNotice error={v.error} />
          <div className="row end">
            <button type="button" className="btn primary" onClick={() => run()}>
              Retry
            </button>
          </div>
        </>
      )}
    </Card>
  )
}

// ---------------------------------------------------------------- 7. identity

type Recommended = {
  alsoKnownAs: string[]
  verificationMethods: { atproto: string }
  rotationKeys: string[]
  services: { atproto_pds: { type: string; endpoint: string } }
}

function IdentityStep({ saved, oldPds, newPds, update }: { saved: Saved; oldPds: Pds; newPds: Pds; update: (p: Partial<Saved>) => void }) {
  const rec = useLoad<Recommended>(() => newPds.call('com.atproto.identity.getRecommendedDidCredentials'), [])
  const doc = useLoad<DidDoc>(() => call('com.atproto.identity.resolveDid', { params: { did: saved.did } }).then((r) => r.didDoc), [])
  const [token, setToken] = useState('')
  const [understood, setUnderstood] = useState(false)
  const [busy, setBusy] = useState<'' | 'request' | 'move'>('')
  const [error, setError] = useState<unknown>()
  const [sentNow, setSentNow] = useState(false)
  const pending = signedOp(saved.did)
  const requested = !!saved.plcRequestedAt
  useEffect(() => {
    // the move may have landed in a tab that closed before it could note it
    newPds
      .call<AccountStatus>('com.atproto.server.checkAccountStatus')
      .then((st) => st.validDid && update({ identityDone: true }))
      .catch(() => {})
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const request = async () => {
    setBusy('request')
    setError(undefined)
    try {
      await oldPds.call('com.atproto.identity.requestPlcOperationSignature', { method: 'POST' })
      update({ plcRequestedAt: Date.now() })
      setSentNow(true)
    } catch (e) {
      setError(e)
    } finally {
      setBusy('')
    }
  }

  const move = async (e?: FormEvent) => {
    e?.preventDefault()
    setBusy('move')
    setError(undefined)
    try {
      let op = signedOp(saved.did)
      if (!op) {
        const r = rec.data!
        const out = await oldPds.call('com.atproto.identity.signPlcOperation', {
          body: { token: token.trim(), rotationKeys: r.rotationKeys, alsoKnownAs: r.alsoKnownAs, verificationMethods: r.verificationMethods, services: r.services },
        })
        op = out.operation
        keepSignedOp(saved.did, op)
      }
      await newPds.call('com.atproto.identity.submitPlcOperation', { body: { operation: op } })
      keepSignedOp(saved.did, undefined)
      update({ identityDone: true })
    } catch (err) {
      setError(err)
    } finally {
      setBusy('')
    }
  }

  const r = rec.data
  const d = doc.data
  const tokenError = error instanceof XrpcError && (error.error === 'InvalidToken' || error.error === 'ExpiredToken')
  return (
    <Card
      title="Move your identity"
      sub={
        <>
          Your data is all here. The last step tells the PLC directory that <b>{here}</b> now hosts <span className="mono">{saved.did}</span>. Apps and
          relays follow that record, so this is the switch-over moment.
        </>
      }
    >
      <ErrorNotice error={rec.error || doc.error} />
      {r && (
        <table className="data mig-diff">
          <thead>
            <tr>
              <th />
              <th>Now</th>
              <th>After</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <th>Hosting server</th>
              <td className="mono">{d ? pdsOf(d) : hostOf(saved.oldPds)}</td>
              <td className="mono">{r.services.atproto_pds.endpoint}</td>
            </tr>
            <tr>
              <th>Handle</th>
              <td className="mono">{d?.alsoKnownAs?.join(', ') ?? `at://${saved.oldHandle}`}</td>
              <td className="mono">{r.alsoKnownAs.join(', ')}</td>
            </tr>
            <tr>
              <th>Signing key</th>
              <td className="mono" title={d && signingKeyOf(d)}>
                {shortKey(d && signingKeyOf(d))}
              </td>
              <td className="mono" title={r.verificationMethods.atproto}>
                {shortKey(r.verificationMethods.atproto)}
              </td>
            </tr>
            <tr>
              <th>Rotation keys</th>
              <td className="muted">{hostOf(saved.oldAuth)}'s key{d ? '' : ''}</td>
              <td className="mono">
                {r.rotationKeys.map((k) => (
                  <div key={k} title={k}>
                    {shortKey(k)}
                  </div>
                ))}
              </td>
            </tr>
          </tbody>
        </table>
      )}
      <Notice kind="warn">
        <p>
          <b>This is the point of no return for this page.</b> Once the directory accepts the change, {here} controls your identity: {hostOf(saved.oldAuth)}{' '}
          can't move it back for you. Rotation keys you added yourself are replaced by the ones above (add them again later from here). Going back
          later means moving again, from {here}.
        </p>
      </Notice>
      {pending ? (
        <>
          <Notice kind="info">Your current server already signed the change. It just needs to reach the directory.</Notice>
          <ErrorNotice error={error} />
          <div className="row end">
            <button type="button" className="btn primary" disabled={!!busy} onClick={() => move()}>
              {busy === 'move' && <Spinner />}
              Finish moving my identity
            </button>
          </div>
        </>
      ) : !requested ? (
        <>
          <p>
            {hostOf(saved.oldAuth)} has to approve the change: it emails a confirmation code to {saved.email ? maskEmail(saved.email) : 'your account email'}.
          </p>
          <ErrorNotice error={error} />
          <div className="row end">
            <button type="button" className="btn primary" disabled={!!busy || !r} onClick={request}>
              {busy === 'request' && <Spinner />}
              Email me a confirmation code
            </button>
          </div>
        </>
      ) : (
        <form onSubmit={move}>
          {sentNow && <Notice kind="ok">Code sent. Check your inbox (and spam) for mail from {hostOf(saved.oldAuth)}.</Notice>}
          {tokenError ? (
            <Notice kind="err">
              {(error as XrpcError).error === 'ExpiredToken' ? 'That code has expired.' : "That code isn't right."} Check it, or send a new code.
            </Notice>
          ) : (
            <ErrorNotice error={error} />
          )}
          <Field label="Confirmation code" hint="From the email titled something like “PLC Update Operation Requested”. It looks like ABCDE-12345.">
            <input type="text" name="plc-token" value={token} onChange={(e) => setToken(e.target.value)} autoComplete="one-time-code" spellCheck={false} required />
          </Field>
          <label className="check">
            <input type="checkbox" name="understood" checked={understood} onChange={(e) => setUnderstood(e.target.checked)} />
            <span>I understand: after this, {here} hosts my account and my identity.</span>
          </label>
          <div className="row between">
            <button type="button" className="btn quiet" disabled={!!busy} onClick={request}>
              {busy === 'request' && <Spinner />}
              Send a new code
            </button>
            <button type="submit" className="btn primary" disabled={!!busy || !understood || !r}>
              {busy === 'move' && <Spinner />}
              Move my identity
            </button>
          </div>
        </form>
      )}
    </Card>
  )
}

// ---------------------------------------------------------------- 8. finish

function FinishStep({ saved, oldPds, newPds, update }: { saved: Saved; oldPds: Pds; newPds: Pds; update: (p: Partial<Saved>) => void }) {
  const [error, setError] = useState<unknown>()
  const [running, setRunning] = useState(false)
  const savedRef = useRef(saved)
  savedRef.current = saved

  const run = useCallback(
    () =>
      once(`finish:${saved.did}`, async () => {
        setRunning(true)
        setError(undefined)
        try {
          if (!savedRef.current.activated) {
            await newPds.call('com.atproto.server.activateAccount', { method: 'POST' })
            update({ activated: true })
          }
          if (!savedRef.current.oldDeactivated) {
            await oldPds.call('com.atproto.server.deactivateAccount', { body: {} })
            update({ oldDeactivated: true, finishedAt: Date.now() })
          }
        } catch (e) {
          setError(e)
        } finally {
          setRunning(false)
        }
      }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [saved.did],
  )
  useEffect(() => {
    run()
  }, [run])

  return (
    <Card title="Switching over" sub={<>Turning your account on here, then off at {hostOf(saved.oldPds)}.</>}>
      <ul className="mig-checks">
        <Checkline state={saved.activated ? 'ok' : error ? 'bad' : 'wait'} title={`Activate your account on ${here}`}>
          {saved.activated ? 'Live: the network now reads your posts from here.' : undefined}
        </Checkline>
        <Checkline state={saved.oldDeactivated ? 'ok' : error && saved.activated ? 'bad' : 'wait'} title={`Deactivate your old account on ${hostOf(saved.oldPds)}`}>
          Deactivated, not deleted: it stays there, offline, as a fallback.
        </Checkline>
      </ul>
      {!!error && !running && (
        <>
          <ErrorNotice error={error} />
          {saved.activated && (
            <Notice kind="info">
              Your account is already live here. The old copy is still switched on at {hostOf(saved.oldPds)}; retry, or deactivate it later from there.
            </Notice>
          )}
          <div className="row end">
            {saved.activated && (
              <button type="button" className="btn quiet" onClick={() => update({ oldDeactivated: true, finishedAt: Date.now() })}>
                Skip, I'll do it later
              </button>
            )}
            <button type="button" className="btn primary" onClick={() => run()}>
              Retry
            </button>
          </div>
        </>
      )}
    </Card>
  )
}

// ---------------------------------------------------------------- done

function DoneStep({ saved, newPds, onReset }: { saved: Saved; newPds: Pds; onReset: () => void }) {
  const st = useLoad<AccountStatus | null>(() => (newPds.tokens ? newPds.call('com.atproto.server.checkAccountStatus') : Promise.resolve(null)), [])
  const open = () => {
    if (newPds.tokens) setSession({ did: saved.did, handle: saved.newHandle ?? '', ...newPds.tokens })
    onReset()
    navigate('/account')
  }
  return (
    <Card title="Welcome to your new home" sub={<>@{saved.newHandle} now lives on {here}. Your followers, posts and DID came with you.</>}>
      {st.data && (
        <div className="tiles">
          <div className="tile">
            <div className="v">{fmtNum(st.data.indexedRecords)}</div>
            <div className="k">records</div>
          </div>
          <div className="tile">
            <div className="v">
              {fmtNum(st.data.importedBlobs)}
              <small>/ {fmtNum(st.data.expectedBlobs)}</small>
            </div>
            <div className="k">images and videos</div>
          </div>
          <div className="tile">
            <div className="v">{st.data.activated && st.data.validDid ? <Status kind="ok">Live</Status> : <Status kind="warn">Check</Status>}</div>
            <div className="k">status</div>
          </div>
        </div>
      )}
      <ol className="mig-next">
        <li>
          <strong>Sign out of the Bluesky app, then sign back in.</strong>
          On the sign-in screen choose <b>Hosting provider</b> → <b>Custom</b>, and enter <CopyText text={location.origin} />. Use @{saved.newHandle} and the
          password you set here.
        </li>
        <li>
          <strong>Check that it worked.</strong>
          Post something; it should show up for your followers within a minute. Apps can take a few minutes to notice the move.
        </li>
        <li>
          <strong>Keep your old account for now.</strong>
          It is deactivated at {hostOf(saved.oldPds)}, not deleted. Once you're happy here, you can delete it there.
        </li>
      </ol>
      <div className="row between">
        <button type="button" className="btn quiet" onClick={onReset}>
          Move another account
        </button>
        <button type="button" className="btn primary" onClick={open}>
          Open your account
        </button>
      </div>
    </Card>
  )
}
