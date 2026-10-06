// A space syncer built on the reference library: the boards app's
// SpaceSyncer (packages/boards/src/spaces/syncer.mjs), with credentials from a
// harness member account's delegation and keys from the local PLC.
import { SpaceSyncer, hashEq } from '../../../../boards/src/spaces/syncer.mjs'
import { attempt } from './http.mjs'
import { signingKey } from './identity.mjs'
import { credentialFor } from './space.mjs'

export { hashEq }

const keys = new Map()
async function keyOf(did) {
  if (!keys.has(did)) keys.set(did, await signingKey(did))
  return keys.get(did)
}
export const forgetKeys = () => keys.clear()

export class Syncer extends SpaceSyncer {
  /** `member`: the account whose delegation the syncer's credentials come from (a reader). */
  constructor(name, space, member, { snapshot } = {}) {
    super(name, space, { mint: () => credentialFor(member, space), keyOf, attempt, snapshot })
    this.member = member
  }
}
