import { useEffect, useState } from 'react'
import { Confirm, CopyText, Empty, ErrorNotice, JsonView, Loading, PageHead, Panel, Spinner } from '../../components/ui'
import { External } from '../../components/icons'
import { fmtTime, short } from '../../lib/format'
import { useAction, useLoad, useSession } from '../../lib/hooks'
import { Link, navigate } from '../../lib/router'
import { acall } from '../../lib/xrpc'

const base = '/account/repo'

export function Nsid({ nsid }: { nsid: string }) {
  const i = nsid.lastIndexOf('.')
  return (
    <span className="nsid">
      <span>{nsid.slice(0, i + 1)}</span>
      <b>{nsid.slice(i + 1)}</b>
    </span>
  )
}

/** at://did/collection/rkey in this repo → its page here. */
export function useRecordLink() {
  const s = useSession()!
  return (v: string) => {
    const m = /^at:\/\/([^/]+)\/([^/]+)\/([^/]+)$/.exec(v)
    if (m && (m[1] === s.did || m[1] === s.handle)) return `${base}/${m[2]}/${m[3]}`
    return undefined
  }
}

export function Collections() {
  const s = useSession()!
  const d = useLoad(() => acall('com.atproto.repo.describeRepo', { params: { repo: s.did } }), [s.did])
  const cols: string[] = d.data?.collections ?? []
  return (
    <>
      <PageHead title="Repository" desc="Every record you've published, grouped by collection. Records are public: anyone can read them." />
      <ErrorNotice error={d.error} />
      <Panel title="Collections" desc={d.data ? `${cols.length} ${cols.length === 1 ? 'collection' : 'collections'}` : undefined} flush>
        {!d.data ? (
          !d.error && <Loading />
        ) : cols.length === 0 ? (
          <Empty title="No records yet">Posts, likes and follows you create in an app appear here.</Empty>
        ) : (
          <ul className="collections">
            {cols.map((c) => (
              <li key={c}>
                <Link to={`${base}/${c}`}>
                  <Nsid nsid={c} />
                  <span className="muted small" aria-hidden="true">
                    Browse
                  </span>
                </Link>
              </li>
            ))}
          </ul>
        )}
      </Panel>
    </>
  )
}

type Rec = { uri: string; cid: string; value: any }

export function preview(v: any): string {
  if (!v || typeof v !== 'object') return ''
  if (typeof v.text === 'string' && v.text) return v.text
  if (typeof v.displayName === 'string') return v.displayName
  if (typeof v.name === 'string') return v.name
  if (v.subject) return typeof v.subject === 'string' ? v.subject : v.subject.uri ?? ''
  return ''
}

const PAGE = 50

export function Records({ collection }: { collection: string }) {
  const s = useSession()!
  const [recs, setRecs] = useState<Rec[]>([])
  const [cursor, setCursor] = useState<string | undefined>()
  const [done, setDone] = useState(false)
  const [first, setFirst] = useState(true)
  const page = useAction(async (cur?: string) => {
    const r = await acall('com.atproto.repo.listRecords', { params: { repo: s.did, collection, limit: PAGE, cursor: cur } })
    setRecs((x) => (cur ? [...x, ...r.records] : r.records))
    setCursor(r.cursor)
    setDone(!r.cursor || r.records.length < PAGE)
    setFirst(false)
  })
  useEffect(() => {
    setRecs([])
    setFirst(true)
    page.run(undefined)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [collection, s.did])

  return (
    <>
      <PageHead title={<Nsid nsid={collection} />} crumbs={[{ to: base, label: 'Repository' }]} desc="Newest first." />
      <ErrorNotice error={page.error} />
      <Panel flush>
        {first && page.busy ? (
          <Loading />
        ) : recs.length === 0 ? (
          <Empty title="This collection is empty" />
        ) : (
          <div className="table-wrap">
            <table className="data">
              <thead>
                <tr>
                  <th>Record key</th>
                  <th>Content</th>
                  <th className="hide-sm">Created</th>
                  <th className="hide-sm">CID</th>
                </tr>
              </thead>
              <tbody>
                {recs.map((r) => {
                  const rkey = r.uri.split('/').pop()!
                  const to = `${base}/${collection}/${rkey}`
                  return (
                    <tr key={r.uri} className="link" onClick={() => navigate(to)}>
                      <td className="mono nowrap">
                        <Link to={to} onClick={(e) => e.stopPropagation()}>
                          {rkey}
                        </Link>
                      </td>
                      <td>
                        <span className="preview">{preview(r.value) || <span className="muted">{r.value?.$type}</span>}</span>
                      </td>
                      <td className="nowrap hide-sm">{fmtTime(r.value?.createdAt)}</td>
                      <td className="mono small muted hide-sm">{short(r.cid, 6)}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
      {recs.length > 0 && (
        <div className="row between">
          <span className="small muted">
            {recs.length} {recs.length === 1 ? 'record' : 'records'} shown
          </span>
          {!done && (
            <button className="btn" onClick={() => page.run(cursor)} disabled={page.busy}>
              {page.busy && <Spinner />}
              Load {PAGE} more
            </button>
          )}
        </div>
      )}
    </>
  )
}

export function RecordView({ collection, rkey }: { collection: string; rkey: string }) {
  const s = useSession()!
  const linkFor = useRecordLink()
  const r = useLoad<Rec>(() => acall('com.atproto.repo.getRecord', { params: { repo: s.did, collection, rkey } }), [collection, rkey, s.did])
  const [confirm, setConfirm] = useState(false)
  const del = useAction(async () => {
    await acall('com.atproto.repo.deleteRecord', { body: { repo: s.did, collection, rkey } })
    setConfirm(false)
    navigate(`${base}/${collection}`)
  })
  return (
    <>
      <PageHead
        title={<span className="mono">{rkey}</span>}
        crumbs={[
          { to: base, label: 'Repository' },
          { to: `${base}/${collection}`, label: collection },
        ]}
      />
      <ErrorNotice error={r.error || del.error} />
      {!r.data ? (
        !r.error && <Loading />
      ) : (
        <>
          <Panel>
            <dl className="dl">
              <dt>URI</dt>
              <dd>
                <span className="row" style={{ gap: 8 }}>
                  <CopyText text={r.data.uri} />
                  <a href={r.data.uri} className="small" title="Open with an app that handles at:// links">
                    <External style={{ width: 14, height: 14, verticalAlign: -2 }} /> Open
                  </a>
                </span>
              </dd>
              <dt>CID</dt>
              <dd>
                <CopyText text={r.data.cid} />
              </dd>
              <dt>Type</dt>
              <dd className="mono">{r.data.value?.$type}</dd>
              {r.data.value?.createdAt && (
                <>
                  <dt>Created</dt>
                  <dd>{fmtTime(r.data.value.createdAt)}</dd>
                </>
              )}
            </dl>
          </Panel>
          <Panel title="Record" actions={<CopyText text={JSON.stringify(r.data.value, null, 2)} display="Copy JSON" mono={false} />}>
            <JsonView value={r.data.value} linkFor={linkFor} />
          </Panel>
          <Panel title="Delete this record" danger desc="Deleting is published to the network as a signed commit. Apps will stop showing it.">
            <button className="btn danger" onClick={() => setConfirm(true)}>
              Delete record
            </button>
          </Panel>
          <Confirm
            open={confirm}
            title="Delete this record?"
            action="Delete record"
            danger
            busy={del.busy}
            onConfirm={() => del.run()}
            onClose={() => setConfirm(false)}
          >
            <span className="mono">
              {collection}/{rkey}
            </span>{' '}
            will be removed from your repository. This can't be undone.
          </Confirm>
        </>
      )}
    </>
  )
}
