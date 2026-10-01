import { useCallback, useEffect, useRef, useState } from 'react'
import { CopyText, Empty, ErrorNotice, Loading, PageHead, Panel, Spinner, saveBlob } from '../../components/ui'
import { useAction, useSession } from '../../lib/hooks'
import { fmtBytes } from '../../lib/format'
import { acall } from '../../lib/xrpc'

const PAGE = 120

const blobUrl = (did: string, cid: string) =>
  `/xrpc/com.atproto.sync.getBlob?did=${encodeURIComponent(did)}&cid=${encodeURIComponent(cid)}`

/** Grid tile: image thumbnail, or a video's first frame, or a file badge. */
function Tile({ did, cid, onOpen }: { did: string; cid: string; onOpen: () => void }) {
  const [kind, setKind] = useState<'img' | 'video' | 'file'>('img')
  const url = blobUrl(did, cid)
  return (
    <button type="button" className="blob" onClick={onOpen} title={cid} aria-label={`Preview ${cid}`}>
      <div className="thumb">
        {kind === 'img' && <img src={url} alt="" loading="lazy" decoding="async" onError={() => setKind('video')} />}
        {kind === 'video' && (
          <video src={`${url}#t=0.1`} muted playsInline preload="metadata" onError={() => setKind('file')} />
        )}
        {kind === 'file' && <span>File</span>}
        {kind === 'video' && <span className="badge">video</span>}
      </div>
      <div className="cid">{cid}</div>
    </button>
  )
}

type Loaded = { url: string; type: string; size: number; blob: Blob }

/** Modal preview with prev/next navigation; fetches the blob once and shows it inline. */
function Lightbox({
  did,
  cids,
  index,
  onIndex,
  onClose,
}: {
  did: string
  cids: string[]
  index: number
  onIndex: (i: number) => void
  onClose: () => void
}) {
  const ref = useRef<HTMLDialogElement>(null)
  const cid = cids[index]
  const [loaded, setLoaded] = useState<Loaded | null>(null)
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    const d = ref.current
    if (d && !d.open) d.showModal()
  }, [])

  useEffect(() => {
    let alive = true
    let objectUrl: string | null = null
    setLoaded(null)
    setErr(null)
    fetch(blobUrl(did, cid))
      .then(async (r) => {
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`)
        const blob = await r.blob()
        const type = r.headers.get('content-type') || blob.type || 'application/octet-stream'
        objectUrl = URL.createObjectURL(blob)
        if (alive) setLoaded({ url: objectUrl, type, size: blob.size, blob })
      })
      .catch((e) => alive && setErr(String(e.message || e)))
    return () => {
      alive = false
      if (objectUrl) URL.revokeObjectURL(objectUrl)
    }
  }, [did, cid])

  const go = useCallback(
    (delta: number) => {
      const next = index + delta
      if (next >= 0 && next < cids.length) onIndex(next)
    },
    [index, cids.length, onIndex],
  )

  const onKey = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowRight') go(1)
    if (e.key === 'ArrowLeft') go(-1)
  }

  const kind = loaded?.type.split('/')[0]
  return (
    <dialog ref={ref} className="modal lightbox" onClose={onClose} onKeyDown={onKey} aria-label="Media preview">
      <div className="inner">
        <div className="lb-stage">
          {err ? (
            <div className="lb-msg">Couldn't load this blob: {err}</div>
          ) : !loaded ? (
            <div className="lb-msg">
              <Spinner /> Loading…
            </div>
          ) : kind === 'image' ? (
            <img src={loaded.url} alt="" />
          ) : kind === 'video' ? (
            <video src={loaded.url} controls autoPlay muted playsInline />
          ) : kind === 'audio' ? (
            <audio src={loaded.url} controls />
          ) : (
            <div className="lb-msg">No preview for {loaded.type}.</div>
          )}
        </div>
        <div className="lb-meta">
          <div className="lb-facts">
            <span className="mono">{loaded?.type ?? '…'}</span>
            <span>{loaded ? fmtBytes(loaded.size) : ''}</span>
            <span className="muted">
              {index + 1} of {cids.length}
            </span>
          </div>
          <CopyText text={cid} />
          <div className="row end lb-actions">
            <button className="btn" onClick={() => go(-1)} disabled={index === 0} aria-label="Previous">
              ← Prev
            </button>
            <button className="btn" onClick={() => go(1)} disabled={index === cids.length - 1} aria-label="Next">
              Next →
            </button>
            <a className="btn" href={blobUrl(did, cid)} target="_blank" rel="noreferrer">
              Open raw
            </a>
            <button className="btn" disabled={!loaded} onClick={() => loaded && saveBlob(loaded.blob, cid)}>
              Download
            </button>
            <button className="btn primary" onClick={() => ref.current?.close()}>
              Close
            </button>
          </div>
        </div>
      </div>
    </dialog>
  )
}

export function Blobs() {
  const s = useSession()!
  const [cids, setCids] = useState<string[]>([])
  const [cursor, setCursor] = useState<string>()
  const [first, setFirst] = useState(true)
  const [open, setOpen] = useState<number | null>(null)
  const page = useAction(async (cur?: string) => {
    const r = await acall('com.atproto.sync.listBlobs', { params: { did: s.did, limit: PAGE, cursor: cur } })
    setCids((x) => (cur ? [...x, ...r.cids] : r.cids))
    setCursor(r.cids.length === PAGE ? r.cursor : undefined)
    setFirst(false)
  })
  useEffect(() => {
    page.run(undefined)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [s.did])
  return (
    <>
      <PageHead title="Media" desc="Images, video and other files your records reference. Click one to preview it." />
      <ErrorNotice error={page.error} />
      <Panel>
        {first ? (
          <Loading />
        ) : cids.length === 0 ? (
          <Empty title="No media yet">Images you attach to posts are stored here.</Empty>
        ) : (
          <div className="blobs">
            {cids.map((c, i) => (
              <Tile key={c} did={s.did} cid={c} onOpen={() => setOpen(i)} />
            ))}
          </div>
        )}
      </Panel>
      {cursor && (
        <div className="row end">
          <button className="btn" onClick={() => page.run(cursor)} disabled={page.busy}>
            {page.busy && <Spinner />}
            Load more
          </button>
        </div>
      )}
      {open !== null && (
        <Lightbox did={s.did} cids={cids} index={open} onIndex={setOpen} onClose={() => setOpen(null)} />
      )}
    </>
  )
}
