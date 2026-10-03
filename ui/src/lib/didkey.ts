// did:key for PLC rotation keys: compressed secp256k1 or P-256 public keys,
// multicodec-prefixed, base58btc multibase ('z').

import { secp256k1 } from '@noble/curves/secp256k1.js'
import { p256 } from '@noble/curves/nist.js'

const B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'

// little-endian digit arrays; leading zero bytes <-> leading '1's
function b58encode(bytes: Uint8Array): string {
  const digits: number[] = []
  for (const b of bytes) {
    let carry = b
    for (let i = 0; i < digits.length; i++) {
      carry += digits[i] << 8
      digits[i] = carry % 58
      carry = (carry / 58) | 0
    }
    while (carry) {
      digits.push(carry % 58)
      carry = (carry / 58) | 0
    }
  }
  let out = ''
  for (let i = 0; i < bytes.length && bytes[i] === 0; i++) out += '1'
  for (let i = digits.length - 1; i >= 0; i--) out += B58[digits[i]]
  return out
}

function b58decode(s: string): Uint8Array | null {
  const bytes: number[] = []
  for (const c of s) {
    const v = B58.indexOf(c)
    if (v < 0) return null
    let carry = v
    for (let i = 0; i < bytes.length; i++) {
      carry += bytes[i] * 58
      bytes[i] = carry & 0xff
      carry >>= 8
    }
    while (carry) {
      bytes.push(carry & 0xff)
      carry >>= 8
    }
  }
  for (let i = 0; i < s.length && s[i] === '1'; i++) bytes.push(0)
  return new Uint8Array(bytes.reverse())
}

const SECP256K1 = [0xe7, 0x01]
const P256 = [0x80, 0x24]

export type KeyCheck = { ok: true; curve: 'secp256k1' | 'P-256' } | { ok: false; reason: string }

/** What the PLC directory accepts as a rotation key. */
export function checkDidKey(input: string): KeyCheck {
  const s = input.trim()
  if (!s.startsWith('did:key:')) return { ok: false, reason: 'It should start with did:key:' }
  if (!s.startsWith('did:key:z')) return { ok: false, reason: 'Only base58 did:keys (did:key:z…) are supported.' }
  const raw = b58decode(s.slice('did:key:z'.length))
  if (!raw) return { ok: false, reason: 'It contains characters that are not base58.' }
  const curve = raw[0] === SECP256K1[0] && raw[1] === SECP256K1[1] ? 'secp256k1' : raw[0] === P256[0] && raw[1] === P256[1] ? 'P-256' : null
  if (!curve) return { ok: false, reason: 'Only secp256k1 (zQ3s…) and P-256 (zDn…) keys can be rotation keys.' }
  const pub = raw.slice(2)
  if (pub.length !== 33) return { ok: false, reason: 'Rotation keys must be compressed public keys (33 bytes).' }
  try {
    ;(curve === 'secp256k1' ? secp256k1 : p256).Point.fromBytes(pub)
  } catch {
    return { ok: false, reason: 'That is not a valid point on the curve. Check for a copy-paste mistake.' }
  }
  return { ok: true, curve }
}

const hex = (b: Uint8Array) => Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('')

export function didKeyOf(compressedSecp256k1: Uint8Array): string {
  const m = new Uint8Array(2 + compressedSecp256k1.length)
  m.set(SECP256K1)
  m.set(compressedSecp256k1, 2)
  return `did:key:z${b58encode(m)}`
}

/** A fresh secp256k1 key pair. The private key exists only in the returned object. */
export function generateRotationKey(): { privateHex: string; didKey: string } {
  const sk = secp256k1.utils.randomSecretKey()
  const didKey = didKeyOf(secp256k1.getPublicKey(sk, true))
  const privateHex = hex(sk)
  sk.fill(0)
  return { privateHex, didKey }
}
