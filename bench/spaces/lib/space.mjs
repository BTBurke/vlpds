// The space credential chain, as an app runs it with the reference library:
// a delegation token from the user's own PDS, exchanged at the authority's
// space host for a credential bound to a fresh P-256 key, then reads anywhere
// with `Authorization: Atproto-Space`, an audience and an RFC 9421 signature
// (@atproto/space createSpaceSigHeaders).
// The chain itself lives in the boards app (packages/boards/src/spaces); this
// wires it to the harness's PLC and its timed XRPC client.
import { SpaceCred as BaseCred, Spaces } from '../../../../boards/src/spaces/cred.mjs'
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

export { decodeJwt } from '../../../../boards/src/spaces/xrpc.mjs'

export const spaces = new Spaces({ repoHost, spaceHost: spaceHostEndpoint, makeClient })

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
  return spaces.delegationToken(actor.client, space)
}

/** Exchange a delegation token at the authority's space host. */
export async function exchange(space, token, opts = {}) {
  const c = await spaces.exchange(space, token, opts)
  return new SpaceCred(space, c.credential, c.key)
}

/** A fresh credential for `actor` (delegation on their PDS, exchange at the authority). */
export async function credentialFor(actor, space, opts = {}) {
  return exchange(space, await delegationToken(actor, space), opts)
}

export class SpaceCred extends BaseCred {
  constructor(space, credential, key) {
    super(space, credential, key, spaces)
  }
}
