import { useEffect, useRef, useState, type ReactNode } from 'react'
import { CopyText, Field, Notice, Panel, Spinner, Status } from '../../components/ui'
import * as I from '../../components/icons'
import { useAction } from '../../lib/hooks'
import { acall, errText, XrpcError } from '../../lib/xrpc'
import './handle.css'

/** vlpds.identity.checkHandle's answer. */
type Check = {
  handle: string
  kind: 'service' | 'external'
  status: 'available' | 'current' | 'taken' | 'reserved' | 'invalid' | 'verified' | 'unverified'
  message?: string | null
  proofRequired: boolean
  method?: 'dns' | 'http' | null
  dns?: { result: 'match' | 'other' | 'several' | 'none'; did?: string | null }
  http?: { result: 'match' | 'other' | 'none' | 'refused'; did?: string | null; detail?: string | null }
}

const checkHandle = (name: string, signal?: AbortSignal) => acall<Check>('vlpds.identity.checkHandle', { params: { name }, signal })

const RECHECK_MS = 15_000
const RECHECK_FOR_MS = 30 * 60_000

// The server's own words, for the cases a browser can tell before asking it.
export function serverNameProblem(label: string): string | undefined {
  if (!label) return undefined
  if (label.includes('.')) return 'A name on this server can\'t contain dots. To use a domain you own, choose "Your own domain".'
  if (!/^[a-z0-9-]+$/.test(label)) return 'Use only letters, numbers and hyphens.'
  if (label.startsWith('-') || label.endsWith('-')) return "A name can't start or end with a hyphen."
  if (label.length < 3) return "That's too short. Use at least 3 characters."
  if (label.length > 18) return "That's too long. Use at most 18 characters."
  return undefined
}

/** What people paste: a URL, an @handle, a trailing dot. */
export function normalizeDomain(input: string): string {
  return input
    .trim()
    .toLowerCase()
    .replace(/^@/, '')
    .replace(/^[a-z]+:\/\//, '')
    .replace(/[/?#].*$/, '')
    .replace(/\.$/, '')
}

export function domainProblem(d: string, serviceDomain: string): string | undefined {
  if (!d) return undefined
  if (serviceDomain && d.endsWith(serviceDomain)) return 'That\'s a name on this server. Choose "A name on this server" for that.'
  if (!d.includes('.')) return 'Enter the whole domain, like alice.com.'
  const labels = d.split('.')
  if (d.length > 253 || labels.some((l) => !/^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/.test(l)) || !/^[a-z]/.test(labels[labels.length - 1]))
    return "That doesn't look like a domain. It should look like alice.com or me.alice.com."
  return undefined
}

/** updateHandle's errors, for people. */
export function handleErrText(e: unknown): string {
  if (!(e instanceof XrpcError)) return errText(e)
  const m = e.message
  switch (e.error) {
    case 'HandleNotAvailable':
      return /reserved/i.test(m) ? 'That name is reserved on this server. Try another.' : 'Someone else already has that handle. Try another.'
    case 'InvalidHandle':
      if (/inappropriate/i.test(m)) return "That name isn't allowed. Try another."
      if (/too short/i.test(m)) return "That's too short. Use at least 3 characters."
      if (/too long/i.test(m)) return "That's too long. Use at most 18 characters."
      if (/TLD/.test(m)) return "Domains with that ending can't be used as handles."
      return "That isn't a valid handle."
    case 'UnsupportedDomain':
      return "That domain can't be used for handles here."
    case 'RateLimitExceeded':
      return "You've changed your handle a lot in a short time. Wait a few minutes and try again (the limit is 10 changes in 5 minutes and 50 a day)."
    case 'AccountTakedown':
      return "This account is suspended, so its handle can't be changed."
  }
  if (/did not resolve to DID/i.test(m)) return "We couldn't confirm that the domain points to your account. Check the record and try again."
  if (/changed concurrently/i.test(m)) return 'Your handle was changed somewhere else at the same time. Reload the page and try again.'
  if (/not properly configured/i.test(m)) return 'Your DID document has to list the new handle before you can switch to it.'
  if (e.status >= 500)
    return "We couldn't update your identity record in the PLC directory, so nothing changed. Try again in a minute."
  return m || e.error
}

function checkErrText(e: unknown): string {
  if (e instanceof XrpcError && e.error === 'RateLimitExceeded') return "We've checked a lot in a short time. Wait a few minutes, then check again."
  return errText(e)
}

type Done = { from: string; to: string }

/** The Handle panel: pick a name on this server or set up your own domain, then switch. */
export function HandleChange({ current, did, domain, onDone }: { current: string; did: string; domain: string; onDone: () => void }) {
  const [mode, setMode] = useState<'server' | 'domain'>()
  const [done, setDone] = useState<Done>()
  const [prefill, setPrefill] = useState('')
  const host = domain.replace(/^\./, '')

  const pick = (m: 'server' | 'domain') => {
    setDone(undefined)
    setMode(m)
  }

  const switched = (to: string) => {
    setDone({ from: current, to })
    setMode(undefined)
    onDone()
  }

  return (
    <Panel title="Handle" desc={<>Your handle is <b>@{current}</b>.</>} id="handle">
      {done && (
        <AfterSwitch
          done={done}
          domain={domain}
          onSwitchBack={() => {
            const label = done.from.endsWith(domain) ? done.from.slice(0, -domain.length) : ''
            setPrefill(label)
            setDone(undefined)
            setMode('server')
          }}
          onDismiss={() => setDone(undefined)}
        />
      )}
      <fieldset className="hc-choice">
        <legend>Change it to</legend>
        {domain && (
          <label className="hc-option">
            <input type="radio" name="hc-mode" checked={mode === 'server'} onChange={() => pick('server')} />
            <span>
              <strong>A name on this server</strong>
              <span className="small muted">
                Like <span className="mono">@alice{domain}</span>. Ready right away.
              </span>
            </span>
          </label>
        )}
        <label className="hc-option">
          <input type="radio" name="hc-mode" checked={mode === 'domain'} onChange={() => pick('domain')} />
          <span>
            <strong>Your own domain</strong>
            <span className="small muted">
              Like <span className="mono">@alice.com</span>. You add one setting where you manage the domain.
            </span>
          </span>
        </label>
      </fieldset>
      {mode === 'server' && <ServerName key={prefill} domain={domain} host={host} current={current} initial={prefill} onSwitched={switched} />}
      {mode === 'domain' && <OwnDomain domain={domain} did={did} current={current} onSwitched={switched} />}
    </Panel>
  )
}

// ---------------------------------------------------------------- a name here

function ServerName({
  domain,
  host,
  current,
  initial,
  onSwitched,
}: {
  domain: string
  host: string
  current: string
  initial: string
  onSwitched: (to: string) => void
}) {
  const [name, setName] = useState(initial)
  const label = name.trim().replace(/^@/, '').toLowerCase().replace(new RegExp(`${domain.replace(/\./g, '\\.')}$`), '')
  const full = label ? `${label}${domain}` : ''
  const local = serverNameProblem(label)
  const [state, setState] = useState<{ handle: string; check?: Check; error?: unknown }>()

  useEffect(() => {
    setState(undefined)
    if (!label || local) return
    const ctl = new AbortController()
    const t = setTimeout(() => {
      checkHandle(full, ctl.signal)
        .then((check) => setState({ handle: full, check }))
        .catch((error) => !ctl.signal.aborted && setState({ handle: full, error }))
    }, 400)
    return () => {
      clearTimeout(t)
      ctl.abort()
    }
  }, [full, label, local])

  const check = state?.handle === full ? state.check : undefined
  const available = check?.status === 'available'
  const act = useAction(async () => {
    await acall('com.atproto.identity.updateHandle', { body: { handle: full } })
    onSwitched(full)
  })

  let verdict: ReactNode = null
  if (local) verdict = <Status kind="bad">{local}</Status>
  else if (!label) verdict = <span className="muted">3 to 18 letters, numbers or hyphens.</span>
  else if (state?.error) verdict = <Status kind="warn">{checkErrText(state.error)}</Status>
  else if (!check)
    verdict = (
      <span className="muted">
        <Spinner label="Checking" /> Checking @{full}…
      </span>
    )
  else if (available) verdict = <Status kind="ok">@{full} is available.</Status>
  else if (check.status === 'current') verdict = <Status kind="idle">That's already your handle.</Status>
  else verdict = <Status kind="bad">{check.message ?? 'That handle is not available.'}</Status>

  return (
    <form
      className="hc-flow"
      onSubmit={(e) => {
        e.preventDefault()
        if (available) act.run()
      }}
    >
      <Field label="New handle" hint={<span aria-live="polite">{verdict}</span>}>
        <span className="affix">
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            autoCapitalize="none"
            autoComplete="off"
            spellCheck={false}
            maxLength={64}
            placeholder="alice"
            aria-invalid={!!local || (!!check && !available)}
            autoFocus
          />
          <span className="mono">{domain || host}</span>
        </span>
      </Field>
      {act.error ? <Notice kind="err">{handleErrText(act.error)}</Notice> : null}
      <div className="row">
        <button type="submit" className="btn primary" disabled={!available || act.busy}>
          {act.busy && <Spinner />}
          {available ? `Switch to @${full}` : 'Switch handle'}
        </button>
        <span className="small muted">@{current} stops working as soon as you switch.</span>
      </div>
    </form>
  )
}

// ---------------------------------------------------------------- your own domain

const STEPS = ['Your domain', 'Add the record', 'Check', 'Switch'] as const

function OwnDomain({ domain, did, current, onSwitched }: { domain: string; did: string; current: string; onSwitched: (to: string) => void }) {
  const [step, setStep] = useState(0)
  const [input, setInput] = useState('')
  const [method, setMethod] = useState<'dns' | 'http'>('dns')
  const [verified, setVerified] = useState<Check>()
  const d = normalizeDomain(input)
  const headRef = useRef<HTMLHeadingElement>(null)
  const first = useRef(true)

  useEffect(() => {
    if (first.current) {
      first.current = false
      return
    }
    headRef.current?.focus()
  }, [step])

  const go = (n: number) => setStep(n)

  return (
    <div className="hc-flow">
      <ol className="hc-steps" aria-label="Steps">
        {STEPS.map((s, i) => (
          <li key={s} aria-current={i === step ? 'step' : undefined} className={i < step ? 'done' : undefined}>
            <span className="n" aria-hidden="true">
              {i < step ? <I.Check /> : i + 1}
            </span>
            <span className="t">{s}</span>
          </li>
        ))}
      </ol>
      <h3 ref={headRef} tabIndex={-1} className="hc-step-title">
        Step {step + 1} of {STEPS.length}: {STEPS[step]}
      </h3>
      {step === 0 && <DomainStep input={input} setInput={setInput} d={d} domain={domain} current={current} onNext={(c) => (c.status === 'verified' ? (setVerified(c), go(3)) : go(1))} />}
      {step === 1 && <RecordStep d={d} did={did} method={method} setMethod={setMethod} onBack={() => go(0)} onNext={() => go(2)} />}
      {step === 2 && (
        <CheckStep
          d={d}
          method={method}
          onBack={() => go(1)}
          onVerified={(c) => {
            setVerified(c)
            go(3)
          }}
        />
      )}
      {step === 3 && <ConfirmStep d={d} current={current} verified={verified} onBack={() => go(verified?.status === 'verified' ? 1 : 2)} onSwitched={onSwitched} />}
    </div>
  )
}

function DomainStep({
  input,
  setInput,
  d,
  domain,
  current,
  onNext,
}: {
  input: string
  setInput: (s: string) => void
  d: string
  domain: string
  current: string
  onNext: (c: Check) => void
}) {
  const [touched, setTouched] = useState(false)
  const local = domainProblem(d, domain)
  const [problem, setProblem] = useState<string>()
  const act = useAction(async () => {
    const c = await checkHandle(d)
    if (c.status === 'verified' || c.status === 'unverified') onNext(c)
    else setProblem(c.message ?? "That domain can't be used.")
  })
  const shown = (touched && local) || problem
  return (
    <form
      onSubmit={(e) => {
        e.preventDefault()
        setTouched(true)
        if (!local && d) act.run()
      }}
    >
      <p className="hc-lede">
        Enter a domain you own. It can be the whole domain (<span className="mono">alice.com</span>) or a subdomain of it (
        <span className="mono">me.alice.com</span>). You'll need to be able to change its DNS settings or put a file on its website.
      </p>
      <Field label="Your domain" hint={shown ? <Status kind="bad">{shown}</Status> : d && d !== input.trim() ? <>We'll use <b className="mono">{d}</b>.</> : undefined}>
        <input
          type="text"
          value={input}
          onChange={(e) => {
            setInput(e.target.value)
            setProblem(undefined)
          }}
          onBlur={() => setTouched(true)}
          autoCapitalize="none"
          autoComplete="off"
          spellCheck={false}
          inputMode="url"
          placeholder="alice.com"
          aria-invalid={!!shown}
          autoFocus
          required
        />
      </Field>
      {d === current && <Notice>That's already your handle.</Notice>}
      {act.error ? <Notice kind="err">{checkErrText(act.error)}</Notice> : null}
      <div className="row end">
        <button type="submit" className="btn primary" disabled={act.busy || !d || !!local || d === current}>
          {act.busy && <Spinner />}
          Next
        </button>
      </div>
    </form>
  )
}

function RecordStep({
  d,
  did,
  method,
  setMethod,
  onBack,
  onNext,
}: {
  d: string
  did: string
  method: 'dns' | 'http'
  setMethod: (m: 'dns' | 'http') => void
  onBack: () => void
  onNext: () => void
}) {
  return (
    <div>
      <p className="hc-lede">
        To prove <b>{d}</b> is yours, add one of these. You only need one.
      </p>
      <div className="hc-methods" role="radiogroup" aria-label="How to prove it">
        <MethodTab on={method === 'dns'} onClick={() => setMethod('dns')} title="DNS record" tag="Recommended" sub="Works for any domain." />
        <MethodTab on={method === 'http'} onClick={() => setMethod('http')} title="File on your website" sub="If you run a website on this domain." />
      </div>
      {method === 'dns' ? (
        <div className="hc-howto">
          <ol>
            <li>Sign in where you manage your domain (usually where you bought it) and open its DNS settings.</li>
            <li>
              Add a new record with these values:
              <dl className="hc-record">
                <dt>Type</dt>
                <dd>
                  <span className="mono">TXT</span>
                </dd>
                <dt>Name</dt>
                <dd>
                  <CopyText text={`_atproto.${d}`} label="Copy the record name" />
                  <span className="small muted">Some providers add your domain to the end for you. If yours does, type only the part before it.</span>
                </dd>
                <dt>Value</dt>
                <dd>
                  <CopyText text={`did=${did}`} label="Copy the record value" />
                </dd>
                <dt>TTL</dt>
                <dd className="small muted">Leave the default.</dd>
              </dl>
            </li>
            <li>Save it. If there's already a TXT record with that name starting with "did=", change it instead of adding a second one.</li>
          </ol>
        </div>
      ) : (
        <div className="hc-howto">
          <ol>
            <li>
              Make this address work:
              <div className="hc-copyline">
                <CopyText text={`https://${d}/.well-known/atproto-did`} label="Copy the address" />
              </div>
            </li>
            <li>
              Make it return just this text, as a plain-text file:
              <div className="hc-copyline">
                <CopyText text={did} label="Copy your DID" />
              </div>
            </li>
            <li className="small muted">It has to be HTTPS with a valid certificate, and it can't redirect somewhere else.</li>
          </ol>
        </div>
      )}
      <div className="row between">
        <button type="button" className="btn" onClick={onBack}>
          Back
        </button>
        <button type="button" className="btn primary" onClick={onNext}>
          I've added it, check now
        </button>
      </div>
    </div>
  )
}

function MethodTab({ on, onClick, title, sub, tag }: { on: boolean; onClick: () => void; title: string; sub: string; tag?: string }) {
  return (
    <button type="button" role="radio" aria-checked={on} className="hc-method" onClick={onClick}>
      <strong>
        {title}
        {tag && <span className="pill accent">{tag}</span>}
      </strong>
      <span className="small muted">{sub}</span>
    </button>
  )
}

function CheckStep({ d, method, onBack, onVerified }: { d: string; method: 'dns' | 'http'; onBack: () => void; onVerified: (c: Check) => void }) {
  const [check, setCheck] = useState<Check>()
  const [error, setError] = useState<unknown>()
  const [busy, setBusy] = useState(false)
  const [next, setNext] = useState<number>()
  const [now, setNow] = useState(Date.now())
  const [skipOk, setSkipOk] = useState(false)
  const until = useRef(Date.now() + RECHECK_FOR_MS)
  const live = useRef(true)

  const run = async () => {
    setBusy(true)
    setNext(undefined)
    try {
      const c = await checkHandle(d)
      if (!live.current) return
      setCheck(c)
      setError(undefined)
      if (c.status === 'verified') return onVerified(c)
      if (c.status !== 'unverified') return
      if (Date.now() < until.current) setNext(Date.now() + RECHECK_MS)
    } catch (e) {
      if (!live.current) return
      setError(e)
      // a rate limit stops the automatic checks; anything else retries
      if (!(e instanceof XrpcError && e.error === 'RateLimitExceeded') && Date.now() < until.current) setNext(Date.now() + RECHECK_MS)
    } finally {
      if (live.current) setBusy(false)
    }
  }

  useEffect(() => {
    live.current = true
    run()
    return () => {
      live.current = false
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [d])

  useEffect(() => {
    if (!next) return
    const id = setInterval(() => {
      setNow(Date.now())
      if (Date.now() >= next) {
        clearInterval(id)
        run()
      }
    }, 1000)
    return () => clearInterval(id)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [next])

  const secs = next ? Math.max(0, Math.ceil((next - now) / 1000)) : undefined
  const stopped = !busy && !next && check?.status === 'unverified'
  const unusable = check && check.status !== 'verified' && check.status !== 'unverified'

  return (
    <div>
      <div className="hc-checks" aria-live="polite">
        <CheckRow label="DNS record" busy={busy && !check} result={check?.dns?.result} other={check?.dns?.did} preferred={method === 'dns'} />
        <CheckRow label="File on your website" busy={busy && !check} result={check?.http?.result} other={check?.http?.did} detail={check?.http?.detail} preferred={method === 'http'} />
      </div>
      {unusable ? (
        <Notice kind="err">{check?.message ?? "That domain can't be used."}</Notice>
      ) : error ? (
        <Notice kind="warn">{checkErrText(error)}</Notice>
      ) : check?.dns?.result === 'other' ? (
        <Notice kind="warn">
          There's a DNS record for {d}, but it points to a different account. Apps check DNS first, so change its value to the one from the last step (or
          delete it if you're using the file).
        </Notice>
      ) : check?.dns?.result === 'several' ? (
        <Notice kind="warn">There's more than one "did=" record for {d}. Keep only the one with your DID and delete the others.</Notice>
      ) : check?.status === 'unverified' ? (
        <Notice kind="info">
          {method === 'dns' ? (
            <>We can't see the record yet. DNS changes can take a few minutes to an hour. </>
          ) : (
            <>We can't see the file yet. </>
          )}
          {stopped ? <>We've stopped checking for now. Check again when you're ready.</> : <>We'll keep checking.</>}
        </Notice>
      ) : null}
      {check && !check.proofRequired && check.status === 'unverified' && (
        <Notice kind="warn">
          This server is in dev mode, so it doesn't need the proof.{' '}
          <button type="button" className="btn sm" onClick={() => (setSkipOk(true), onVerified(check))} disabled={skipOk}>
            Continue without it
          </button>
        </Notice>
      )}
      <div className="row between">
        <button type="button" className="btn" onClick={onBack}>
          Back to the instructions
        </button>
        <div className="row">
          <span className="small muted" aria-live="off">
            {busy ? 'Checking…' : secs !== undefined ? `Checking again in ${secs} s` : null}
          </span>
          <button type="button" className="btn" onClick={run} disabled={busy}>
            {busy ? <Spinner label="Checking" /> : <I.Refresh />}
            Check now
          </button>
        </div>
      </div>
    </div>
  )
}

function CheckRow({
  label,
  busy,
  result,
  other,
  detail,
  preferred,
}: {
  label: string
  busy: boolean
  result?: string
  other?: string | null
  detail?: string | null
  preferred: boolean
}) {
  let s: ReactNode
  if (busy || !result) s = <span className="muted"><Spinner label="Checking" /> Looking…</span>
  else if (result === 'match') s = <Status kind="ok">Found. It points to your account.</Status>
  else if (result === 'other') s = <Status kind="bad">Found, but it points to a different account{other ? ` (${other})` : ''}.</Status>
  else if (result === 'several') s = <Status kind="bad">Found more than one.</Status>
  else if (result === 'refused') s = <Status kind="bad">{detail ?? "Can't be checked."}</Status>
  else s = <Status kind={preferred ? 'warn' : 'idle'}>Not found yet.</Status>
  return (
    <div className={`hc-check${preferred ? ' preferred' : ''}`}>
      <span className="k">{label}</span>
      <span className="v">
        {s}
        {result === 'none' && detail && preferred && <span className="small muted"> {detail}</span>}
      </span>
    </div>
  )
}

function ConfirmStep({
  d,
  current,
  verified,
  onBack,
  onSwitched,
}: {
  d: string
  current: string
  verified?: Check
  onBack: () => void
  onSwitched: (to: string) => void
}) {
  const act = useAction(async () => {
    await acall('com.atproto.identity.updateHandle', { body: { handle: d } })
    onSwitched(d)
  })
  const how = verified?.method === 'dns' ? 'your DNS record' : verified?.method === 'http' ? 'the file on your website' : undefined
  return (
    <div>
      {how ? (
        <Notice kind="ok">
          {d} points to your account (we found {how}).
        </Notice>
      ) : (
        <Notice kind="warn">{d} isn't verified, but this dev server will take it anyway.</Notice>
      )}
      <ul className="hc-facts">
        <li>Your followers, posts and everything else stay with you. Only the name changes.</li>
        <li>
          <b>@{current}</b> stops working right away. Mentions and links that use it won't find you anymore.
        </li>
        <li>Keep the {verified?.method === 'http' ? 'file' : 'record'} in place for as long as you use this handle.</li>
      </ul>
      {act.error ? <Notice kind="err">{handleErrText(act.error)}</Notice> : null}
      <div className="row between">
        <button type="button" className="btn" onClick={onBack}>
          Back
        </button>
        <button type="button" className="btn primary" onClick={() => act.run()} disabled={act.busy}>
          {act.busy && <Spinner />}
          Switch to @{d}
        </button>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------- after

function AfterSwitch({ done, domain, onSwitchBack, onDismiss }: { done: Done; domain: string; onSwitchBack: () => void; onDismiss: () => void }) {
  const ref = useRef<HTMLDivElement>(null)
  useEffect(() => ref.current?.focus(), [])
  const own = !done.to.endsWith(domain)
  return (
    <div className="hc-after" ref={ref} tabIndex={-1}>
      <Notice kind="ok">
        <p>
          <b>You're now @{done.to}.</b>
        </p>
        <ul className="hc-facts">
          <li>Your followers and posts stay with you.</li>
          <li>@{done.from} doesn't point to you anymore.</li>
          <li>Apps may take a few minutes to show your new name.</li>
          {own && <li>Leave the DNS record or file in place. If it goes away, apps will show your handle as invalid.</li>}
        </ul>
        <div className="row">
          {own && (
            <button type="button" className="btn sm" onClick={onSwitchBack}>
              Switch back to a name on this server
            </button>
          )}
          <button type="button" className="btn sm quiet" onClick={onDismiss}>
            Done
          </button>
        </div>
      </Notice>
    </div>
  )
}
