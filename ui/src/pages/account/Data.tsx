import { useEffect, useState } from 'react'
import { ErrorNotice, Loading, Notice, PageHead, Panel, Spinner, saveBlob } from '../../components/ui'
import { Download } from '../../components/icons'
import { fmtBytes } from '../../lib/format'
import { useAction, useLoad, useSession } from '../../lib/hooks'
import { acall } from '../../lib/xrpc'

export function Export() {
  const s = useSession()!
  const [result, setResult] = useState<{ name: string; size: number }>()
  const [progress, setProgress] = useState(0)
  const dl = useAction(async () => {
    setResult(undefined)
    setProgress(0)
    const r: Response = await acall('com.atproto.sync.getRepo', { params: { did: s.did }, raw: true })
    const rev = r.headers.get('atproto-repo-rev') ?? new Date().toISOString().slice(0, 10)
    // stream so large repos show progress
    const reader = r.body!.getReader()
    const parts: BlobPart[] = []
    let n = 0
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      parts.push(value)
      n += value.length
      setProgress(n)
    }
    const name = `${s.handle}-${rev}.car`
    saveBlob(new Blob(parts, { type: 'application/vnd.ipld.car' }), name)
    setResult({ name, size: n })
  })
  return (
    <>
      <PageHead title="Export" desc="Download your whole repository: every record, signed and verifiable, in the standard CAR format." />
      <Panel title="Repository archive" desc="Use it as a backup or to move your account to another server. Media files are not included; they're listed in Media.">
        <ErrorNotice error={dl.error} />
        {result && (
          <Notice kind="ok">
            Saved <span className="mono">{result.name}</span> ({fmtBytes(result.size)}).
          </Notice>
        )}
        <div className="row">
          <button className="btn primary" onClick={() => dl.run()} disabled={dl.busy}>
            {dl.busy ? <Spinner /> : <Download />}
            {dl.busy ? `Downloading… ${fmtBytes(progress)}` : 'Download repository (.car)'}
          </button>
        </div>
      </Panel>
    </>
  )
}

export function Preferences() {
  const p = useLoad<{ preferences: unknown[] }>(() => acall('app.bsky.actor.getPreferences'), [])
  const [text, setText] = useState('')
  const [parseErr, setParseErr] = useState<string>()
  const [saved, setSaved] = useState(false)
  useEffect(() => {
    if (p.data) setText(JSON.stringify(p.data.preferences, null, 2))
  }, [p.data])
  const save = useAction(async () => {
    const v = JSON.parse(text)
    await acall('app.bsky.actor.putPreferences', { body: { preferences: v } })
    setSaved(true)
    p.reload()
  })
  const onChange = (v: string) => {
    setText(v)
    setSaved(false)
    try {
      const x = JSON.parse(v)
      setParseErr(Array.isArray(x) ? undefined : 'Preferences must be a JSON array.')
    } catch (e) {
      setParseErr((e as Error).message)
    }
  }
  const dirty = p.data && text !== JSON.stringify(p.data.preferences, null, 2)
  return (
    <>
      <PageHead title="Preferences" desc="App settings stored privately on this server, such as feeds, muted words and content filters. Apps manage these for you." />
      <Panel
        title="Stored preferences"
        desc="Edit with care: apps expect each entry to match its lexicon $type."
        actions={
          <>
            <button className="btn" onClick={() => p.data && onChange(JSON.stringify(p.data.preferences, null, 2))} disabled={!dirty}>
              Revert
            </button>
            <button className="btn primary" onClick={() => save.run()} disabled={!dirty || !!parseErr || save.busy}>
              {save.busy && <Spinner />}
              Save preferences
            </button>
          </>
        }
      >
        <ErrorNotice error={p.error || save.error} />
        {saved && <Notice kind="ok">Preferences saved.</Notice>}
        {parseErr && <Notice kind="warn">Not valid yet: {parseErr}</Notice>}
        {!p.data ? (
          !p.error && <Loading />
        ) : (
          <textarea
            aria-label="Preferences JSON"
            value={text}
            onChange={(e) => onChange(e.target.value)}
            rows={Math.min(30, Math.max(8, text.split('\n').length + 1))}
            spellCheck={false}
          />
        )}
      </Panel>
    </>
  )
}
