// The UI's backend (boards/bff.mjs): same origin, session cookie.

export type Me = { did: string; handle: string; host: string; auth: 'oauth' | 'password'; scope?: string; signedIn?: false }
export type Board = {
  uri: string
  owner: string
  name: string
  description?: string
  flairs?: string[]
  pinned?: string[]
  members: number
  posts: number
  comments: number
  indexed?: boolean
  error?: string
}
export type Image = { cid: string; mimeType?: string; alt?: string; url?: string }
export type Post = {
  uri: string
  cid: string
  author: string
  title: string
  body?: string
  flair?: string
  image?: Image
  pinned: boolean
  score: number
  ups: number
  downs: number
  comments: number
  createdAt: string
  editedAt?: string
}
export type Comment = { uri: string; cid?: string; author?: string; body?: string; deleted?: boolean; score: number; createdAt?: string; replies: Comment[] }
export type Member = { did: string; read: boolean; write: boolean; handle: string; host: string; appview: boolean }
export type Profile = { handle: string; host: string }
export type Debug = {
  board: string
  authority: string
  appview: string
  checkpoint?: string
  lastSpaceRev?: string
  registrationExpires?: string
  credentialExpires?: string
  lastNotify?: { at: string; repo: string; repoRev: string; spaceRev: string }
  repos: { did: string; rev: string; records: number; digest: string }[]
  stats: Record<string, number>
  violations: string[]
  viewerCredential: { jti: string; expires: string; key?: string }
}

export class ApiError extends Error {
  constructor(
    public status: number,
    public error: string,
    message: string,
  ) {
    super(message)
  }
}

async function call<T>(path: string, init?: { body?: unknown; query?: Record<string, string | undefined> }): Promise<T> {
  const u = new URL(`/api/${path}`, location.origin)
  for (const [k, v] of Object.entries(init?.query ?? {})) if (v !== undefined) u.searchParams.set(k, v)
  const res = await fetch(u, init?.body !== undefined ? { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(init.body) } : {})
  const j = await res.json().catch(() => ({}))
  if (!res.ok) throw new ApiError(res.status, j.error ?? 'Error', j.message ?? res.statusText)
  return j as T
}

export const api = {
  config: () => call<{ appview: string; vlpds: string; console: string; hosts: Record<string, string> }>('config'),
  me: () => call<Me>('me'),
  login: (handle: string, password?: string) => call<{ redirect?: string; needPassword?: boolean; host?: string; ok?: boolean }>('login', { body: { handle, password } }),
  logout: () => call('logout', { body: {} }),
  profiles: (dids: string[]) => call<Record<string, Profile>>('profiles', { query: { dids: dids.join(',') } }),
  boards: () => call<{ boards: Board[] }>('boards'),
  createBoard: (name: string, description: string, flairs: string[]) => call<{ board: string }>('createBoard', { body: { name, description, flairs } }),
  board: (board: string) => call<Board>('board', { query: { board } }),
  members: (board: string) => call<{ members: Member[] }>('members', { query: { board } }),
  invite: (board: string, handle: string, role: 'writer' | 'lurker') => call<{ did: string; host: string }>('invite', { body: { board, handle, role } }),
  removeMember: (board: string, did: string) => call('removeMember', { body: { board, did } }),
  deleteBoard: (board: string) => call('deleteBoard', { body: { board } }),
  posts: (board: string, sort: string) => call<{ posts: Post[] }>('posts', { query: { board, sort } }),
  thread: (uri: string) => call<{ post: Post; replies: Comment[] }>('thread', { query: { uri } }),
  karma: (board: string, actor: string) => call<{ post: number; comment: number; total: number }>('karma', { query: { board, actor } }),
  myVotes: (board: string) => call<Record<string, 'up' | 'down'>>('myVotes', { query: { board } }),
  post: (board: string, p: { title: string; body?: string; flair?: string; image?: { data: string; mimeType: string; alt?: string } }) => call<{ uri: string }>('post', { body: { board, ...p } }),
  comment: (board: string, post: string, body: string, parent?: string) => call<{ uri: string }>('comment', { body: { board, post, body, parent } }),
  vote: (board: string, subject: string, direction: 'up' | 'down' | null) => call('vote', { body: { board, subject, direction } }),
  editPost: (uri: string, title: string, body: string) => call('editPost', { body: { uri, title, body } }),
  remove: (uri: string) => call('delete', { body: { uri } }),
  debug: (board: string) => call<Debug>('debug', { query: { board } }),
}

export const imageUrl = (board: string, p: Post) => (p.image ? `/api/image?board=${encodeURIComponent(board)}&did=${encodeURIComponent(p.author)}&cid=${p.image.cid}` : '')
export const boardOf = (uri: string) => uri.split('/').slice(0, 6).join('/')
export const authorOf = (uri: string) => uri.split('/')[6]
