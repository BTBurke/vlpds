import { useEffect, useState } from 'react'
import { CopyText, Field, Notice, saveBlob } from './ui'
import { Download } from './icons'
import { checkDidKey, generateRotationKey } from '../lib/didkey'

export type RecoveryKeyChoice = { status: 'empty' } | { status: 'incomplete' } | { status: 'ready'; didKey: string }

/** What a user-held recovery key does, in a sentence or three (inline: the caller picks the block). */
export function RecoveryKeyExplainer({ server }: { server: string }) {
  return (
    <>
      A recovery key is a key only you hold. It goes first in your account's list of rotation keys, ahead of {server}'s, so for 72 hours after any change to
      your identity it can undo that change, even one made by {server}. <b>Losing it is harmless</b>: nothing depends on it and {server} keeps working as
      before. <b>Leaking it is not</b>: whoever has it can take over your identity.
    </>
  )
}

/** Paste a did:key, or make one here. A generated private key lives only in
 * this component's state: shown once, never stored. */
export function RecoveryKeyPicker({ onChange }: { onChange: (c: RecoveryKeyChoice) => void }) {
  const [mode, setMode] = useState<'generate' | 'paste'>('generate')
  const [pasted, setPasted] = useState('')
  const [made, setMade] = useState<{ privateHex: string; didKey: string }>()
  const [saved, setSaved] = useState(false)
  const check = pasted.trim() ? checkDidKey(pasted) : undefined

  useEffect(() => {
    if (mode === 'paste') {
      onChange(!pasted.trim() ? { status: 'empty' } : check?.ok ? { status: 'ready', didKey: pasted.trim() } : { status: 'incomplete' })
    } else {
      onChange(!made ? { status: 'empty' } : saved ? { status: 'ready', didKey: made.didKey } : { status: 'incomplete' })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, pasted, made, saved])

  const generate = () => {
    setMade(generateRotationKey())
    setSaved(false)
  }
  const download = () => {
    if (!made) return
    const text = `PLC recovery (rotation) key. Keep this file secret and offline.\n\nprivate key (secp256k1, hex): ${made.privateHex}\npublic key: ${made.didKey}\n`
    saveBlob(new Blob([text], { type: 'text/plain' }), 'recovery-key.txt')
  }

  return (
    <div className="rk">
      <div className="rk-modes" role="radiogroup" aria-label="How to add a recovery key">
        <label className="check">
          <input type="radio" name="rk-mode" value="generate" checked={mode === 'generate'} onChange={() => setMode('generate')} />
          <span>Make a new key in this browser</span>
        </label>
        <label className="check">
          <input type="radio" name="rk-mode" value="paste" checked={mode === 'paste'} onChange={() => setMode('paste')} />
          <span>Use a key I already have (paste its did:key)</span>
        </label>
      </div>
      {mode === 'paste' ? (
        <Field
          label="Public key (did:key)"
          hint={
            check && !check.ok ? (
              <span className="rk-bad">{check.reason}</span>
            ) : check?.ok ? (
              `A valid ${check.curve} key.`
            ) : (
              'secp256k1 (did:key:zQ3s…) or P-256 (did:key:zDn…). Only the public key: never paste a private key into a web page.'
            )
          }
        >
          <input
            type="text"
            name="recovery-did-key"
            value={pasted}
            onChange={(e) => setPasted(e.target.value)}
            autoCapitalize="none"
            autoComplete="off"
            spellCheck={false}
            placeholder="did:key:zQ3s…"
          />
        </Field>
      ) : !made ? (
        <div className="row">
          <button type="button" className="btn" onClick={generate}>
            Generate a recovery key
          </button>
        </div>
      ) : (
        <div className="rk-made">
          <Notice kind="warn">
            <p>
              <b>Save the private key now.</b> This is the only time it is shown. It is not stored anywhere: not on this server, not in this browser.
            </p>
          </Notice>
          <dl className="dl">
            <dt>Private key (secret)</dt>
            <dd className="rk-private">
              <CopyText text={made.privateHex} />
            </dd>
            <dt>Public key</dt>
            <dd className="rk-public">
              <CopyText text={made.didKey} />
            </dd>
          </dl>
          <div className="row">
            <button type="button" className="btn sm" onClick={download}>
              <Download />
              Download as a file
            </button>
            <button type="button" className="btn sm quiet" onClick={generate}>
              Make a different one
            </button>
          </div>
          <label className="check">
            <input type="checkbox" name="saved-key" checked={saved} onChange={(e) => setSaved(e.target.checked)} />
            <span>I saved the private key somewhere safe (a password manager, or on paper).</span>
          </label>
        </div>
      )}
    </div>
  )
}
