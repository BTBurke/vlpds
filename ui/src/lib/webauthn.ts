// WebAuthn for the account page: the server sends options as JSON with
// base64url binary fields, and takes the browser's answer back the same way.

export function b64uToBytes(s: string): Uint8Array<ArrayBuffer> {
  const b = atob(s.replace(/-/g, '+').replace(/_/g, '/'))
  const out = new Uint8Array(new ArrayBuffer(b.length))
  for (let i = 0; i < b.length; i++) out[i] = b.charCodeAt(i)
  return out
}

export function bytesToB64u(buf: ArrayBuffer | Uint8Array): string {
  const u = buf instanceof Uint8Array ? buf : new Uint8Array(buf)
  let s = ''
  for (let i = 0; i < u.length; i++) s += String.fromCharCode(u[i])
  return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

/** Passkeys only work on the public origin (the relying party's); the operator's tailnet address can't use them. */
export const passkeysHere = (origin?: string) => typeof window.PublicKeyCredential === 'function' && (!origin || origin === location.origin)

type Desc = { type: 'public-key'; id: string; transports?: string[] }

const descs = (l?: Desc[]) => (l ?? []).map((d) => ({ type: d.type, id: b64uToBytes(d.id), transports: d.transports as AuthenticatorTransport[] | undefined }))

/** Runs navigator.credentials.create with the server's options; the registration as the server wants it. */
export async function createPasskey(o: any) {
  const cred = (await navigator.credentials.create({
    publicKey: {
      ...o,
      challenge: b64uToBytes(o.challenge),
      user: { ...o.user, id: b64uToBytes(o.user.id) },
      excludeCredentials: descs(o.excludeCredentials),
    },
  })) as PublicKeyCredential | null
  if (!cred) throw new Error('No passkey was created')
  const r = cred.response as AuthenticatorAttestationResponse
  return {
    id: cred.id,
    rawId: bytesToB64u(cred.rawId),
    type: cred.type,
    response: {
      clientDataJSON: bytesToB64u(r.clientDataJSON),
      attestationObject: bytesToB64u(r.attestationObject),
      transports: typeof r.getTransports === 'function' ? r.getTransports() : [],
    },
    clientExtensionResults: cred.getClientExtensionResults(),
  }
}

export type AssertionJson = { id: string; clientDataJSON: string; authenticatorData: string; signature: string; userHandle?: string }

/** navigator.credentials.get; `mediation: 'conditional'` for the username field's autofill. */
export async function getPasskey(o: { challenge: string; rpId: string; timeout?: number; userVerification?: UserVerificationRequirement; allowCredentials?: Desc[] }, mediation?: CredentialMediationRequirement, signal?: AbortSignal): Promise<AssertionJson> {
  const cred = (await navigator.credentials.get({
    mediation,
    signal,
    publicKey: {
      challenge: b64uToBytes(o.challenge),
      rpId: o.rpId,
      timeout: o.timeout,
      userVerification: o.userVerification,
      allowCredentials: descs(o.allowCredentials),
    },
  })) as PublicKeyCredential | null
  if (!cred) throw new Error('No passkey was used')
  const r = cred.response as AuthenticatorAssertionResponse
  return {
    id: bytesToB64u(cred.rawId),
    clientDataJSON: bytesToB64u(r.clientDataJSON),
    authenticatorData: bytesToB64u(r.authenticatorData),
    signature: bytesToB64u(r.signature),
    userHandle: r.userHandle ? bytesToB64u(r.userHandle) : undefined,
  }
}

/** The DID a passkey's user handle names (vlpds sets it to the DID). */
export function didOfUserHandle(h?: string): string | undefined {
  if (!h) return undefined
  const s = new TextDecoder().decode(b64uToBytes(h))
  return s.startsWith('did:') ? s : undefined
}

export async function conditionalAvailable(): Promise<boolean> {
  const P = window.PublicKeyCredential as any
  return typeof P?.isConditionalMediationAvailable === 'function' && (await P.isConditionalMediationAvailable())
}

/** A cancelled or timed-out prompt, which isn't worth an error message. */
export const cancelled = (e: unknown) => e instanceof DOMException && (e.name === 'NotAllowedError' || e.name === 'AbortError')
