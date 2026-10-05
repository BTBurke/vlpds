import { useCallback, useEffect, useState, type FormEvent, type ReactNode } from 'react'
import { api, ApiError, authorOf, boardOf, imageUrl, type Comment, type Debug, type Me, type Post, type Profile } from './api'

// Hash routes: #/ (my boards), #/b/<board>, #/p/<post>, #/u/<board>/<did>
type Route = { page: 'home' } | { page: 'board'; board: string } | { page: 'post'; uri: string } | { page: 'user'; board: string; did: string }

function parseRoute(): Route {
  const [, kind, a, b] = location.hash.split('/')
  if (kind === 'b' && a) return { page: 'board', board: decodeURIComponent(a) }
  if (kind === 'p' && a) return { page: 'post', uri: decodeURIComponent(a) }
  if (kind === 'u' && a && b) return { page: 'user', board: decodeURIComponent(a), did: decodeURIComponent(b) }
  return { page: 'home' }
}
const href = {
  board: (b: string) => `#/b/${encodeURIComponent(b)}`,
  post: (u: string) => `#/p/${encodeURIComponent(u)}`,
  user: (b: string, d: string) => `#/u/${encodeURIComponent(b)}/${encodeURIComponent(d)}`,
}

function useRoute() {
  const [r, setR] = useState(parseRoute)
  useEffect(() => {
    const on = () => setR(parseRoute())
    addEventListener('hashchange', on)
    return () => removeEventListener('hashchange', on)
  }, [])
  return r
}

// handles and hosts, fetched once per DID
const profileCache = new Map<string, Profile>()
function useProfiles(dids: string[]) {
  const [, bump] = useState(0)
  const key = [...new Set(dids)].sort().join(',')
  useEffect(() => {
    const missing = key.split(',').filter((d) => d && !profileCache.has(d))
    if (!missing.length) return
    api.profiles(missing).then((p) => {
      for (const [d, v] of Object.entries(p)) profileCache.set(d, v)
      bump((n) => n + 1)
    })
  }, [key])
  return (did: string) => profileCache.get(did)
}

function Who({ did, board, profile }: { did: string; board: string; profile?: Profile }) {
  return (
    <span>
      <a href={href.user(board, did)}>@{profile?.handle ?? did.slice(0, 16)}</a> {profile && <span className="chip host">{profile.host}</span>}
    </span>
  )
}

const ago = (iso?: string) => {
  if (!iso) return ''
  const s = (Date.now() - Date.parse(iso)) / 1000
  if (s < 60) return 'just now'
  if (s < 3600) return `${Math.floor(s / 60)}m ago`
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`
  return `${Math.floor(s / 86400)}d ago`
}

function useAsync<T>(fn: () => Promise<T>, deps: unknown[]): [T | undefined, string | undefined, () => void] {
  const [v, setV] = useState<T>()
  const [err, setErr] = useState<string>()
  const [n, setN] = useState(0)
  useEffect(() => {
    let live = true
    fn().then(
      (x) => live && (setV(x), setErr(undefined)),
      (e) => live && setErr(e instanceof ApiError ? `${e.error}: ${e.message}` : String(e)),
    )
    return () => {
      live = false
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, n])
  return [v, err, () => setN((x) => x + 1)]
}

/** Re-run `fn` every `ms` while the page is visible, so other members' activity shows up. */
function useTick(ms: number) {
  const [t, setT] = useState(0)
  useEffect(() => {
    const id = setInterval(() => document.visibilityState === 'visible' && setT((x) => x + 1), ms)
    return () => clearInterval(id)
  }, [ms])
  return t
}

function Theme() {
  const [theme, setTheme] = useState(() => {
    try {
      return localStorage.getItem('boards-theme') ?? ''
    } catch {
      return ''
    }
  })
  useEffect(() => {
    if (theme) document.documentElement.dataset.theme = theme
    else delete document.documentElement.dataset.theme
    try {
      localStorage.setItem('boards-theme', theme)
    } catch {}
  }, [theme])
  const next = theme === '' ? 'dark' : theme === 'dark' ? 'light' : ''
  return (
    <button className="link" onClick={() => setTheme(next)} title="theme">
      {theme || 'auto'}
    </button>
  )
}

export function App() {
  const route = useRoute()
  const [me, meErr, reloadMe] = useAsync(() => api.me(), [])
  const [cfg] = useAsync(() => api.config(), [])
  const err = new URLSearchParams(location.search).get('error')
  const signedIn = me && me.signedIn !== false
  return (
    <>
      <header className="top">
        <a className="brand" href="#/">
          boards
        </a>
        <span className="faint small">private boards on atproto Spaces</span>
        <span className="spacer" />
        {cfg && (
          <a className="small" href={cfg.console} target="_blank" rel="noreferrer">
            vlpds console
          </a>
        )}
        <Theme />
        {signedIn && (
          <>
            <span className="who small">
              @{me.handle} <span className="chip host">{me.host}</span> <span className="faint">({me.auth})</span>
            </span>
            <button className="link" onClick={() => api.logout().then(() => (location.href = '/'))}>
              sign out
            </button>
          </>
        )}
      </header>
      <main className="wrap">
        {err && <div className="err">{err}</div>}
        {meErr && <div className="err">{meErr}</div>}
        {!me ? null : !signedIn ? (
          <SignIn onDone={reloadMe} />
        ) : route.page === 'home' ? (
          <Home me={me} />
        ) : route.page === 'board' ? (
          <BoardPage me={me} board={route.board} />
        ) : route.page === 'post' ? (
          <PostPage me={me} uri={route.uri} />
        ) : (
          <UserPage board={route.board} did={route.did} />
        )}
      </main>
    </>
  )
}

function SignIn({ onDone }: { onDone: () => void }) {
  const [handle, setHandle] = useState('')
  const [password, setPassword] = useState('')
  const [host, setHost] = useState<string>()
  const [err, setErr] = useState<string>()
  const [busy, setBusy] = useState(false)
  const submit = async (e: FormEvent) => {
    e.preventDefault()
    setBusy(true)
    setErr(undefined)
    try {
      const r = await api.login(handle, host ? password : undefined)
      if (r.redirect) location.href = r.redirect
      else if (r.needPassword) setHost(r.host)
      else onDone()
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }
  return (
    <div className="card" style={{ maxWidth: 420, margin: '40px auto' }}>
      <h1>Sign in</h1>
      <p className="muted small">
        vlpds accounts sign in with OAuth on vlpds. Accounts on the reference PDSes use their password (the local ref PDSes don't serve OAuth over http).
      </p>
      <form onSubmit={submit} className="stack">
        <label>
          Handle
          <input name="handle" value={handle} onChange={(e) => setHandle(e.target.value)} placeholder="alice….vlpds.test" autoFocus autoComplete="username" />
        </label>
        {host && (
          <label>
            Password <span className="chip host">{host}</span>
            <input name="password" type="password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="current-password" autoFocus />
          </label>
        )}
        {err && <div className="err">{err}</div>}
        <button className="primary" disabled={busy || !handle}>
          {host ? 'Sign in' : 'Continue'}
        </button>
      </form>
    </div>
  )
}

function Home({ me }: { me: Me }) {
  const [boards, err, reload] = useAsync(() => api.boards(), [])
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [flairs, setFlairs] = useState('question, news')
  const [busy, setBusy] = useState(false)
  const [cErr, setCErr] = useState<string>()
  const create = async (e: FormEvent) => {
    e.preventDefault()
    setBusy(true)
    setCErr(undefined)
    try {
      const r = await api.createBoard(name, description, flairs.split(',').map((f) => f.trim()))
      location.hash = href.board(r.board)
    } catch (e) {
      setCErr(e instanceof ApiError ? e.message : String(e))
    } finally {
      setBusy(false)
      reload()
    }
  }
  return (
    <div className="cols">
      <section>
        <h1 style={{ marginBottom: 12 }}>Your boards</h1>
        {err && <div className="err">{err}</div>}
        {boards?.boards.length === 0 && <p className="muted">You're not in any boards yet. Make one, or ask a board's owner to invite @{me.handle}.</p>}
        {boards?.boards.map((b) => (
          <a key={b.uri} href={href.board(b.uri)} className="card" style={{ display: 'block', textDecoration: 'none', color: 'inherit' }}>
            <h2>{b.name}</h2>
            {b.description && <p className="muted">{b.description}</p>}
            <div className="meta">
              {b.indexed ? `${b.posts} posts, ${b.comments} comments, ${b.members} members` : `not indexed (${b.error})`}
              {b.owner === me.did && <> · you own it</>}
            </div>
          </a>
        ))}
      </section>
      <aside className="card">
        <h2>New board</h2>
        <form onSubmit={create} className="stack">
          <label>
            Name
            <input name="name" value={name} onChange={(e) => setName(e.target.value)} maxLength={64} />
          </label>
          <label>
            Description
            <textarea name="description" value={description} onChange={(e) => setDescription(e.target.value)} />
          </label>
          <label>
            Flairs (comma separated)
            <input name="flairs" value={flairs} onChange={(e) => setFlairs(e.target.value)} />
          </label>
          {cErr && <div className="err">{cErr}</div>}
          <button className="primary" disabled={busy || !name}>
            Create board
          </button>
          <p className="faint small">A board is a Spaces space you're the authority of. The appview joins it as a read-only member to index it.</p>
        </form>
      </aside>
    </div>
  )
}

function VoteBox({ board, subject, score, mine, onVoted }: { board: string; subject: string; score: number; mine?: 'up' | 'down'; onVoted: () => void }) {
  const [busy, setBusy] = useState(false)
  const vote = async (dir: 'up' | 'down') => {
    setBusy(true)
    try {
      await api.vote(board, subject, mine === dir ? null : dir)
    } finally {
      setBusy(false)
      onVoted()
    }
  }
  return (
    <div className="votes">
      <button aria-label="upvote" className={`up ${mine === 'up' ? 'on' : ''}`} disabled={busy} onClick={() => vote('up')}>
        ▲
      </button>
      <span className="score" data-testid="score">
        {score}
      </span>
      <button aria-label="downvote" className={`down ${mine === 'down' ? 'on' : ''}`} disabled={busy} onClick={() => vote('down')}>
        ▼
      </button>
    </div>
  )
}

function PostRow({ board, p, votes, profile, onVoted, full }: { board: string; p: Post; votes?: Record<string, 'up' | 'down'>; profile?: Profile; onVoted: () => void; full?: boolean }) {
  return (
    <article className="card post" data-testid="post">
      <VoteBox board={board} subject={p.uri} score={p.score} mine={votes?.[p.uri]} onVoted={onVoted} />
      <div className="main stack">
        <div className="row">
          {p.pinned && <span className="chip pin">pinned</span>}
          {p.flair && <span className="chip flair">{p.flair}</span>}
          {full ? <h1 className="break">{p.title}</h1> : <a className="title break" href={href.post(p.uri)}>{p.title}</a>}
        </div>
        <div className="meta">
          <Who did={p.author} board={board} profile={profile} /> · {ago(p.createdAt)}
          {p.editedAt && <> · edited</>} · {p.ups} up, {p.downs} down
        </div>
        {(full || !p.image) && p.body && <p className="break" style={{ whiteSpace: 'pre-wrap' }}>{p.body}</p>}
        {p.image && <img className="thumb" src={imageUrl(board, p)} alt={p.image.alt ?? ''} />}
        {!full && (
          <div className="meta">
            <a href={href.post(p.uri)}>
              {p.comments} {p.comments === 1 ? 'comment' : 'comments'}
            </a>
          </div>
        )}
      </div>
    </article>
  )
}

function readFile(f: File): Promise<string> {
  return new Promise((ok, err) => {
    const r = new FileReader()
    r.onload = () => ok(String(r.result).split(',')[1] ?? '')
    r.onerror = () => err(r.error)
    r.readAsDataURL(f)
  })
}

function Composer({ board, flairs, onPosted }: { board: string; flairs: string[]; onPosted: () => void }) {
  const [open, setOpen] = useState(false)
  const [title, setTitle] = useState('')
  const [body, setBody] = useState('')
  const [flair, setFlair] = useState('')
  const [file, setFile] = useState<File | null>(null)
  const [alt, setAlt] = useState('')
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string>()
  if (!open)
    return (
      <button className="primary" onClick={() => setOpen(true)} style={{ marginBottom: 12 }}>
        New post
      </button>
    )
  const submit = async (e: FormEvent) => {
    e.preventDefault()
    setBusy(true)
    setErr(undefined)
    try {
      const image = file ? { data: await readFile(file), mimeType: file.type || 'image/png', alt } : undefined
      await api.post(board, { title, body, flair, image })
      setTitle('')
      setBody('')
      setFile(null)
      setOpen(false)
      onPosted()
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }
  return (
    <form className="card stack" onSubmit={submit}>
      <label>
        Title
        <input name="title" value={title} onChange={(e) => setTitle(e.target.value)} autoFocus />
      </label>
      <label>
        Body
        <textarea name="body" value={body} onChange={(e) => setBody(e.target.value)} />
      </label>
      <div className="row">
        <label style={{ flex: 1 }}>
          Flair
          <select name="flair" value={flair} onChange={(e) => setFlair(e.target.value)}>
            <option value="">none</option>
            {flairs.map((f) => (
              <option key={f}>{f}</option>
            ))}
          </select>
        </label>
        <label style={{ flex: 2 }}>
          Image
          <input name="image" type="file" accept="image/*" onChange={(e) => setFile(e.target.files?.[0] ?? null)} />
        </label>
      </div>
      {file && (
        <label>
          Alt text
          <input value={alt} onChange={(e) => setAlt(e.target.value)} />
        </label>
      )}
      {err && <div className="err">{err}</div>}
      <div className="row">
        <button className="primary" disabled={busy || !title}>
          Post
        </button>
        <button type="button" onClick={() => setOpen(false)}>
          Cancel
        </button>
      </div>
    </form>
  )
}

function Members({ board, me, onChange }: { board: string; me: Me; onChange: () => void }) {
  const [m, err, reload] = useAsync(() => api.members(board), [board])
  const [handle, setHandle] = useState('')
  const [role, setRole] = useState<'writer' | 'lurker'>('writer')
  const [iErr, setIErr] = useState<string>()
  const invite = async (e: FormEvent) => {
    e.preventDefault()
    setIErr(undefined)
    try {
      await api.invite(board, handle, role)
      setHandle('')
      reload()
      onChange()
    } catch (e) {
      setIErr(e instanceof ApiError ? e.message : String(e))
    }
  }
  return (
    <div className="card stack">
      <h2>Members</h2>
      {err && <div className="err">{err}</div>}
      <ul style={{ listStyle: 'none', padding: 0, margin: 0 }} className="stack small">
        {m?.members.map((x) => (
          <li key={x.did} className="row" style={{ justifyContent: 'space-between', flexWrap: 'nowrap' }}>
            <span className="break">
              @{x.handle} <span className="chip host">{x.host}</span> <span className="faint">{x.appview ? 'appview' : x.write ? 'writer' : 'lurker'}</span>
            </span>
            {x.did !== me.did && !x.appview && (
              <button className="link danger" onClick={() => api.removeMember(board, x.did).then(() => (reload(), onChange()))}>
                remove
              </button>
            )}
          </li>
        ))}
      </ul>
      <form onSubmit={invite} className="stack">
        <label>
          Invite by handle
          <input name="invite" value={handle} onChange={(e) => setHandle(e.target.value)} placeholder="bob….vlpds.test" />
        </label>
        <div className="row">
          <select name="role" value={role} onChange={(e) => setRole(e.target.value as 'writer' | 'lurker')} style={{ flex: 1 }}>
            <option value="writer">writer</option>
            <option value="lurker">lurker (read only)</option>
          </select>
          <button disabled={!handle}>Invite</button>
        </div>
        {iErr && <div className="err">{iErr}</div>}
      </form>
    </div>
  )
}

function DebugDrawer({ board, onClose }: { board: string; onClose: () => void }) {
  const tick = useTick(2000)
  const [d, err] = useAsync(() => api.debug(board), [board, tick])
  const prof = useProfiles(d?.repos.map((r) => r.did) ?? [])
  return (
    <aside className="drawer" aria-label="Spaces debug">
      <div className="row" style={{ justifyContent: 'space-between', marginBottom: 12 }}>
        <h2>Spaces debug</h2>
        <button onClick={onClose}>Close</button>
      </div>
      {err && <div className="err">{err}</div>}
      {d && <DebugBody d={d} prof={prof} />}
    </aside>
  )
}

function DebugBody({ d, prof }: { d: Debug; prof: (did: string) => Profile | undefined }) {
  const kv: [string, ReactNode][] = [
    ['space', d.board],
    ['authority', d.authority],
    ['appview', d.appview],
    ['checkpoint', d.checkpoint ?? '–'],
    ['last spaceRev', d.lastSpaceRev ?? '–'],
    ['last notify', d.lastNotify ? `${ago(d.lastNotify.at)} · ${prof(d.lastNotify.repo)?.handle ?? d.lastNotify.repo} @ ${d.lastNotify.repoRev}` : 'none yet'],
    ['notify registration', d.registrationExpires ? `expires ${d.registrationExpires}` : '–'],
    ['appview credential', d.credentialExpires ? `expires ${d.credentialExpires}` : '–'],
    ['your credential', `expires ${d.viewerCredential.expires}`],
    ['your jti', d.viewerCredential.jti],
  ]
  return (
    <div className="stack">
      <dl className="kv">
        {kv.map(([k, v]) => (
          <div key={k} style={{ display: 'contents' }}>
            <dt>{k}</dt>
            <dd>{v}</dd>
          </div>
        ))}
      </dl>
      <h3>Member repos the appview syncs</h3>
      <table className="repos">
        <thead>
          <tr>
            <th>repo</th>
            <th>rev</th>
            <th>n</th>
            <th>LtHash</th>
          </tr>
        </thead>
        <tbody>
          {d.repos.map((r) => (
            <tr key={r.did}>
              <td className="break">
                @{prof(r.did)?.handle ?? r.did} <span className="chip host">{prof(r.did)?.host}</span>
              </td>
              <td className="mono">{r.rev}</td>
              <td>{r.records}</td>
              <td className="mono">{r.digest.slice(0, 16)}…</td>
            </tr>
          ))}
        </tbody>
      </table>
      <h3>Sync counters</h3>
      <p className="mono small">{Object.entries(d.stats).map(([k, v]) => `${k} ${v}`).join(' · ')}</p>
      {d.violations.length > 0 && <div className="err">{d.violations.join('\n')}</div>}
    </div>
  )
}

function BoardPage({ me, board }: { me: Me; board: string }) {
  const [sort, setSort] = useState<'hot' | 'new' | 'top'>('hot')
  const tick = useTick(3000)
  const [info, infoErr, reloadInfo] = useAsync(() => api.board(board), [board, tick])
  const [posts, err, reload] = useAsync(() => api.posts(board, sort), [board, sort, tick])
  const [votes, , reloadVotes] = useAsync(() => api.myVotes(board), [board])
  const [debug, setDebug] = useState(false)
  const prof = useProfiles(posts?.posts.map((p) => p.author) ?? [])
  const owner = info?.owner === me.did
  const refresh = useCallback(() => {
    reload()
    reloadVotes()
    reloadInfo()
  }, [reload, reloadVotes, reloadInfo])
  if (infoErr) return <div className="err">{infoErr}</div>
  return (
    <div className="cols">
      <section>
        <div className="row" style={{ justifyContent: 'space-between', marginBottom: 12 }}>
          <h1>{info?.name ?? '…'}</h1>
          <button onClick={() => setDebug(true)}>Spaces debug</button>
        </div>
        <div className="tabs" role="group" aria-label="sort">
          {(['hot', 'new', 'top'] as const).map((s) => (
            <button key={s} aria-pressed={sort === s} onClick={() => setSort(s)}>
              {s}
            </button>
          ))}
        </div>
        <Composer board={board} flairs={info?.flairs ?? []} onPosted={refresh} />
        {err && <div className="err">{err}</div>}
        {posts?.posts.length === 0 && <p className="muted">No posts yet.</p>}
        {posts?.posts.map((p) => (
          <PostRow key={p.uri} board={board} p={p} votes={votes} profile={prof(p.author)} onVoted={refresh} />
        ))}
      </section>
      <aside>
        <div className="card stack small">
          {info?.description && <p>{info.description}</p>}
          <div className="meta">
            {info?.posts} posts · {info?.comments} comments · {info?.members} member repos
          </div>
          <div className="meta">
            owner <Who did={board.split('/')[2]} board={board} profile={useProfiles([board.split('/')[2]])(board.split('/')[2])} />
          </div>
          <a href={href.user(board, me.did)}>Your karma here</a>
        </div>
        {owner && <Members board={board} me={me} onChange={refresh} />}
        {owner && (
          <div className="card">
            <button
              className="danger"
              onClick={() => {
                if (confirm('Delete this board? Members keep their own records; the board goes away.')) api.deleteBoard(board).then(() => (location.hash = '#/'))
              }}
            >
              Delete board
            </button>
          </div>
        )}
      </aside>
      {debug && <DebugDrawer board={board} onClose={() => setDebug(false)} />}
    </div>
  )
}

function CommentNode({ c, board, post, votes, prof, me, onChange }: { c: Comment; board: string; post: string; votes?: Record<string, 'up' | 'down'>; prof: (d: string) => Profile | undefined; me: Me; onChange: () => void }) {
  const [replying, setReplying] = useState(false)
  const [text, setText] = useState('')
  const reply = async (e: FormEvent) => {
    e.preventDefault()
    await api.comment(board, post, text, c.uri)
    setText('')
    setReplying(false)
    onChange()
  }
  return (
    <div className="comment" data-testid="comment">
      {c.deleted ? (
        <div className="head faint">[removed]</div>
      ) : (
        <div className="post">
          <VoteBox board={board} subject={c.uri} score={c.score} mine={votes?.[c.uri]} onVoted={onChange} />
          <div className="main">
            <div className="head">
              <Who did={c.author!} board={board} profile={prof(c.author!)} /> · {ago(c.createdAt)}
            </div>
            <p className="break" style={{ whiteSpace: 'pre-wrap' }}>
              {c.body}
            </p>
            <div className="row">
              <button className="link" onClick={() => setReplying(!replying)}>
                reply
              </button>
              {c.author === me.did && (
                <button className="link danger" onClick={() => api.remove(c.uri).then(onChange)}>
                  delete
                </button>
              )}
            </div>
            {replying && (
              <form onSubmit={reply} className="stack" style={{ marginTop: 6 }}>
                <textarea name="reply" value={text} onChange={(e) => setText(e.target.value)} autoFocus />
                <div className="row">
                  <button className="primary" disabled={!text}>
                    Reply
                  </button>
                </div>
              </form>
            )}
          </div>
        </div>
      )}
      {c.replies.map((r) => (
        <CommentNode key={r.uri} c={r} board={board} post={post} votes={votes} prof={prof} me={me} onChange={onChange} />
      ))}
    </div>
  )
}

const authorsOf = (cs: Comment[]): string[] => cs.flatMap((c) => [...(c.author ? [c.author] : []), ...authorsOf(c.replies)])

function PostPage({ me, uri }: { me: Me; uri: string }) {
  const board = boardOf(uri)
  const tick = useTick(3000)
  const [t, err, reload] = useAsync(() => api.thread(uri), [uri, tick])
  const [votes, , reloadVotes] = useAsync(() => api.myVotes(board), [board])
  const prof = useProfiles([...(t ? [t.post.author] : []), ...authorsOf(t?.replies ?? [])])
  const [text, setText] = useState('')
  const [editing, setEditing] = useState(false)
  const [title, setTitle] = useState('')
  const [body, setBody] = useState('')
  const refresh = () => {
    reload()
    reloadVotes()
  }
  if (err) return <div className="err">{err}</div>
  if (!t) return null
  const mine = authorOf(uri) === me.did
  const comment = async (e: FormEvent) => {
    e.preventDefault()
    await api.comment(board, uri, text)
    setText('')
    refresh()
  }
  return (
    <div>
      <p>
        <a href={href.board(board)}>← back to the board</a>
      </p>
      {editing ? (
        <form
          className="card stack"
          onSubmit={async (e) => {
            e.preventDefault()
            await api.editPost(uri, title, body)
            setEditing(false)
            refresh()
          }}
        >
          <input name="title" value={title} onChange={(e) => setTitle(e.target.value)} />
          <textarea name="body" value={body} onChange={(e) => setBody(e.target.value)} />
          <div className="row">
            <button className="primary">Save</button>
            <button type="button" onClick={() => setEditing(false)}>
              Cancel
            </button>
          </div>
        </form>
      ) : (
        <PostRow board={board} p={t.post} votes={votes} profile={prof(t.post.author)} onVoted={refresh} full />
      )}
      {mine && !editing && (
        <div className="row" style={{ marginBottom: 12 }}>
          <button
            onClick={() => {
              setTitle(t.post.title)
              setBody(t.post.body ?? '')
              setEditing(true)
            }}
          >
            Edit
          </button>
          <button className="danger" onClick={() => api.remove(uri).then(() => (location.hash = href.board(board)))}>
            Delete
          </button>
        </div>
      )}
      <form className="card stack" onSubmit={comment}>
        <label>
          Comment
          <textarea name="comment" value={text} onChange={(e) => setText(e.target.value)} />
        </label>
        <div className="row">
          <button className="primary" disabled={!text}>
            Comment
          </button>
        </div>
      </form>
      <section>
        {t.replies.length === 0 && <p className="muted">No comments yet.</p>}
        {t.replies.map((c) => (
          <CommentNode key={c.uri} c={c} board={board} post={uri} votes={votes} prof={prof} me={me} onChange={refresh} />
        ))}
      </section>
    </div>
  )
}

function UserPage({ board, did }: { board: string; did: string }) {
  const [k, err] = useAsync(() => api.karma(board, did), [board, did])
  const [posts] = useAsync(() => api.posts(board, 'new'), [board])
  const prof = useProfiles([did])
  const theirs = posts?.posts.filter((p) => p.author === did) ?? []
  return (
    <div>
      <p>
        <a href={href.board(board)}>← back to the board</a>
      </p>
      <div className="card">
        <h1>
          @{prof(did)?.handle ?? did} {prof(did) && <span className="chip host">{prof(did)?.host}</span>}
        </h1>
        <p className="mono faint small break">{did}</p>
        {err && <div className="err">{err}</div>}
        {k && (
          <p data-testid="karma">
            <strong>{k.total}</strong> karma here ({k.post} from posts, {k.comment} from comments)
          </p>
        )}
      </div>
      <h2 style={{ margin: '12px 0' }}>Posts</h2>
      {theirs.length === 0 && <p className="muted">None on this board.</p>}
      {theirs.map((p) => (
        <PostRow key={p.uri} board={board} p={p} profile={prof(did)} onVoted={() => {}} />
      ))}
    </div>
  )
}
