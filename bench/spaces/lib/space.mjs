// The space credential chain, as an app runs it with the reference library:
// a delegation token from the user's own PDS, exchanged at the authority's
// space host for a credential bound to a fresh P-256 key, then reads anywhere
// with `Authorization: Atproto-Space`, an audience and an RFC 9421 signature
// (@atproto/space createSpaceSigHeaders).
import { P256Keypair } from '@atproto/crypto'
import { createSpaceSigHeaders } from '@atproto/space'
import { SPACE_TYPE } from './env.mjs'
import { makeClient } from './http.mjs'
import { pdsEndpoint, spaceHostEndpoint } from './identity.mjs'

export const spaceUri = (authorityDid, skey, type = SPACE_TYPE) => `at://${authorityDid}/space/${type}/${skey}`
export const spaceDidOf = (uri) => uri.split('/')[2]

export const POLICY = {
  public: { $type: 'com.atproto.simplespace.defs#publicPolicy' },
  members: { $type: 'com.atproto.simplespace.defs#memberListPolicy' },
  app: (managingApp) => ({ $type: 'com.atproto.simplespace.defs#managingAppPolicy', managingApp }),
  open: { $type: 'com.atproto.simplespace.defs#open' },
}

const endpoints = new Map()
/** A repo's host (its #atproto_pds), cached: the harness never moves accounts. */
export async function repoHost(did) {
  if (!endpoints.has(did)) endpoints.set(did, await pdsEndpoint(did))
  return endpoints.get(did)
}

export function decodeJwt(jwt) {
  const [h, p] = jwt.split('.')
  return {
    header: JSON.parse(Buffer.from(h, 'base64url').toString()),
    payload: JSON.parse(Buffer.from(p, 'base64url').toString()),
  }
}

export async function createSpace(authority, skey, opts = {}) {
  const res = await authority.client.com.atproto.simplespace.createSpace({
    spaceType: opts.spaceType ?? SPACE_TYPE,
    skey,
    readPolicy: opts.readPolicy ?? POLICY.members,
    writePolicy: opts.writePolicy ?? POLICY.members,
    appAccess: opts.appAccess ?? POLICY.open,
  })
  return res.data.uri
}

export async function putMember(authority, space, member, read = true, write = true) {
  await authority.client.com.atproto.simplespace.putMember({ space, did: member.did ?? member, read, write })
}

/** The delegation token: minted by the user's own PDS for the space. */
export async function delegationToken(actor, space) {
  const r = await actor.client.com.atproto.space.getDelegationToken({ space })
  return r.data.token
}

/** Exchange a delegation token at the authority's space host. */
export async function exchange(space, token, { key, clientAttestation, headers } = {}) {
  key ??= await P256Keypair.create()
  const host = await spaceHostEndpoint(spaceDidOf(space))
  const sigHeaders = headers ?? (await createSpaceSigHeaders(key, { authorization: `Bearer ${token}` }))
  const client = makeClient(host, ({ headers: h }) => {
    for (const [k, v] of Object.entries(sigHeaders)) h.set(k, v)
  })
  const r = await client.com.atproto.space.getSpaceCredential({ space, clientAttestation })
  return new SpaceCred(space, r.data.credential, key)
}

/** A fresh credential for `actor` (delegation on their PDS, exchange at the authority). */
export async function credentialFor(actor, space, opts = {}) {
  return exchange(space, await delegationToken(actor, space), opts)
}

export class SpaceCred {
  constructor(space, credential, key) {
    this.space = space
    this.credential = credential
    this.key = key
    this.claims = decodeJwt(credential)
    this.sigs = new Map() // audience -> headers; a signature is reusable per (credential, audience)
  }

  get jti() {
    return this.claims.payload.jti
  }

  expiresInMs() {
    return this.claims.payload.exp * 1000 - Date.now()
  }

  async headersFor(audience) {
    let h = this.sigs.get(audience)
    if (!h) {
      h = await createSpaceSigHeaders(this.key, { authorization: `Atproto-Space ${this.credential}`, audience })
      this.sigs.set(audience, h)
    }
    return h
  }

  /**
   * The auth layer for {@link makeClient}: the audience is the request's
   * `repo` (a repo-host method) or else the space's authority (a host method).
   */
  signer(overrides = {}) {
    return async ({ url, headers, body }) => {
      let params = Object.fromEntries(new URL(url).searchParams)
      if (body && typeof body === 'string') {
        try {
          params = { ...params, ...JSON.parse(body) }
        } catch {}
      }
      const audience = overrides.audience ?? params.repo ?? spaceDidOf(params.space ?? this.space)
      for (const [k, v] of Object.entries(await this.headersFor(audience))) headers.set(k, v)
      for (const [k, v] of Object.entries(overrides.headers ?? {})) headers.set(k, v)
    }
  }

  client(base, overrides) {
    return makeClient(base, this.signer(overrides))
  }

  /** A client for `did`'s repo host. */
  async repoClient(did) {
    return this.client(await repoHost(did))
  }

  async hostClient() {
    return this.client(await spaceHostEndpoint(spaceDidOf(this.space)))
  }
}
