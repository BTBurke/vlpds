import { useState } from 'react'
import { RecoveryKeyExplainer, RecoveryKeyPicker, type RecoveryKeyChoice } from '../../components/RecoveryKey'
import { ErrorNotice, Field, Loading, Notice, Panel, Spinner } from '../../components/ui'
import { useAction, useLoad } from '../../lib/hooks'
import { acall, XrpcError } from '../../lib/xrpc'

type PlcData = {
  did: string
  rotationKeys: string[]
  serverKeys: string[]
  recoveryKey: string | null
  recommendedRotationKeys: string[]
}

type Change = { kind: 'add' | 'remove'; key: string; rotationKeys: string[] }

const here = location.host

/** The account's PLC rotation keys, and adding or removing keys the user
 * holds: request an emailed code, sign the new list here, submit it. */
export function RecoveryKeys() {
  const data = useLoad<PlcData>(() => acall('vlpds.identity.getPlcData'), [])
  const [adding, setAdding] = useState(false)
  const [choice, setChoice] = useState<RecoveryKeyChoice>({ status: 'empty' })
  const [change, setChange] = useState<Change>()
  const [code, setCode] = useState('')
  const [done, setDone] = useState<string>()

  const d = data.data
  const kindOf = (k: string) => (d?.serverKeys.includes(k) ? 'server' : k === d?.recoveryKey ? 'operator' : 'yours')
  const yours = d?.rotationKeys.filter((k) => kindOf(k) === 'yours') ?? []

  const request = useAction(async (c: Change) => {
    await acall('com.atproto.identity.requestPlcOperationSignature', { method: 'POST' })
    setChange(c)
    setCode('')
  })
  const apply = useAction(async () => {
    if (!change) return
    const { operation } = await acall('com.atproto.identity.signPlcOperation', { body: { token: code.trim(), rotationKeys: change.rotationKeys } })
    await acall('com.atproto.identity.submitPlcOperation', { body: { operation } })
    setDone(change.kind === 'add' ? 'Your recovery key is added. It is now first in line.' : 'That key is removed.')
    setChange(undefined)
    setAdding(false)
    setCode('')
    data.reload()
  })

  const startAdd = () => {
    if (!d || choice.status !== 'ready') return
    const key = choice.didKey
    request.run({ kind: 'add', key, rotationKeys: [key, ...d.rotationKeys.filter((k) => k !== key)] })
  }
  const startRemove = (key: string) => {
    if (!d) return
    request.run({ kind: 'remove', key, rotationKeys: d.rotationKeys.filter((k) => k !== key) })
  }
  const cancel = () => {
    setChange(undefined)
    setAdding(false)
    setCode('')
  }

  const off = data.error instanceof XrpcError && (data.error.status === 501 || data.error.error === 'InvalidRequest')
  return (
    <Panel title="Recovery key" desc={<RecoveryKeyExplainer server={here} />} id="recovery-key">
      {off ? (
        <Notice>This account's identity isn't registered in the PLC directory by this server, so there are no rotation keys to manage here.</Notice>
      ) : (
        <ErrorNotice error={data.error} />
      )}
      {!d && !data.error && <Loading />}
      {d && (
        <>
          <ol className="rk-list" aria-label="Rotation keys, highest priority first">
            {d.rotationKeys.map((k) => {
              const kind = kindOf(k)
              return (
                <li key={k}>
                  <span className="mono">{k}</span>
                  <span className="row">
                    <span className="who">{kind === 'yours' ? 'Your key' : kind === 'operator' ? `${here}'s operator recovery key` : here}</span>
                    {kind === 'yours' && !change && (
                      <button type="button" className="btn sm quiet" disabled={request.busy} onClick={() => startRemove(k)}>
                        Remove
                      </button>
                    )}
                  </span>
                </li>
              )
            })}
          </ol>
          {done && <Notice kind="ok">{done}</Notice>}
          <ErrorNotice error={request.error} />
          {change ? (
            <form
              onSubmit={(e) => {
                e.preventDefault()
                apply.run()
              }}
            >
              <Notice>
                {change.kind === 'add' ? 'To add the key' : 'To remove the key'} <span className="mono small">{change.key}</span>, enter the code we just
                emailed you.
              </Notice>
              <ErrorNotice error={apply.error} />
              <Field label="Code from your email" hint="It looks like ABCDE-12345.">
                <input type="text" name="plc-token" value={code} onChange={(e) => setCode(e.target.value)} autoComplete="one-time-code" spellCheck={false} required autoFocus />
              </Field>
              <div className="row">
                <button type="button" className="btn" onClick={cancel}>
                  Cancel
                </button>
                <button className="btn primary" disabled={apply.busy}>
                  {apply.busy && <Spinner />}
                  {change.kind === 'add' ? 'Add recovery key' : 'Remove key'}
                </button>
              </div>
            </form>
          ) : adding ? (
            <>
              <RecoveryKeyPicker onChange={setChoice} />
              <div className="row">
                <button type="button" className="btn" onClick={cancel}>
                  Cancel
                </button>
                <button type="button" className="btn primary" disabled={choice.status !== 'ready' || request.busy} onClick={startAdd}>
                  {request.busy && <Spinner />}
                  Continue
                </button>
              </div>
            </>
          ) : (
            <button
              type="button"
              className="btn"
              onClick={() => {
                setDone(undefined)
                setAdding(true)
              }}
            >
              {yours.length ? 'Add another recovery key' : 'Add a recovery key'}
            </button>
          )}
        </>
      )}
    </Panel>
  )
}
