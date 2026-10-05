//! The WebAuthn relying party (docs/oauth-2fa.md "Passkeys"): the checks a
//! registration or an assertion has to pass, on `ring`, with no attestation
//! verification (we ask for `attestation: "none"`).
//!
//! Pure functions only: storage, challenges' single use and the counter's
//! consequences are `xrpc::passkeys`. Every input is size-capped before it
//! is parsed, and nothing here logs its inputs.
//!
//! The CBOR reader is its own, strict and small: `crate::cbor` is DAG-CBOR,
//! whose maps only have string keys, and COSE keys use integer ones. It
//! takes definite lengths only, no tags or floats, no duplicate map keys,
//! and bounds nesting and item counts.

use crate::auth::ct_eq;
use crate::oauth::util::{b64u_decode, hmac_sha256};
use sha2::{Digest, Sha256};

pub const ALG_ES256: i64 = -7;
pub const ALG_EDDSA: i64 = -8;
pub const ALG_RS256: i64 = -257;
/// `pubKeyCredParams`, in our order of preference.
pub const ALGS: [i64; 3] = [ALG_ES256, ALG_EDDSA, ALG_RS256];

pub const MAX_CLIENT_DATA: usize = 4096;
pub const MAX_ATTESTATION_OBJECT: usize = 16 * 1024;
pub const MAX_AUTHENTICATOR_DATA: usize = 4096;
/// An RS256 signature under a 4096-bit key is 512 bytes.
pub const MAX_SIGNATURE: usize = 1024;
/// The spec's limit.
pub const MAX_CREDENTIAL_ID: usize = 1023;
/// The spec's limit; the DID's bytes are the user handle, so a longer DID
/// can't sign in without its password.
pub const MAX_USER_HANDLE: usize = 64;
const MIN_RSA_BITS: usize = 2048;
const MAX_RSA_BITS: usize = 4096;

pub const FLAG_UP: u8 = 0x01;
pub const FLAG_UV: u8 = 0x04;
pub const FLAG_BE: u8 = 0x08;
pub const FLAG_BS: u8 = 0x10;
pub const FLAG_AT: u8 = 0x40;
pub const FLAG_ED: u8 = 0x80;

/// Why a ceremony was refused: one bounded metric label each.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fail {
    Malformed,
    TooLarge,
    Type,
    Challenge,
    Origin,
    CrossOrigin,
    RpId,
    UserPresent,
    UserVerified,
    Algorithm,
    Key,
    Signature,
    CredentialId,
    Counter,
    UnknownCredential,
    Replay,
}

impl Fail {
    pub const ALL: [Fail; 16] = [
        Fail::Malformed,
        Fail::TooLarge,
        Fail::Type,
        Fail::Challenge,
        Fail::Origin,
        Fail::CrossOrigin,
        Fail::RpId,
        Fail::UserPresent,
        Fail::UserVerified,
        Fail::Algorithm,
        Fail::Key,
        Fail::Signature,
        Fail::CredentialId,
        Fail::Counter,
        Fail::UnknownCredential,
        Fail::Replay,
    ];

    pub fn reason(self) -> &'static str {
        match self {
            Fail::Malformed => "malformed",
            Fail::TooLarge => "too_large",
            Fail::Type => "type",
            Fail::Challenge => "challenge",
            Fail::Origin => "origin",
            Fail::CrossOrigin => "cross_origin",
            Fail::RpId => "rp_id",
            Fail::UserPresent => "user_present",
            Fail::UserVerified => "user_verified",
            Fail::Algorithm => "algorithm",
            Fail::Key => "key",
            Fail::Signature => "signature",
            Fail::CredentialId => "credential_id",
            Fail::Counter => "counter",
            Fail::UnknownCredential => "unknown_credential",
            Fail::Replay => "replay",
        }
    }
}

impl std::fmt::Display for Fail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.reason())
    }
}

// ---------------------------------------------------------------- CBOR

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cbor {
    Uint(u64),
    /// -1 - n
    Nint(u64),
    Bytes(Vec<u8>),
    Text(String),
    Array(Vec<Cbor>),
    Map(Vec<(Cbor, Cbor)>),
    Bool(bool),
    Null,
}

const MAX_DEPTH: usize = 8;
/// Entries in one array or map. A packed attestation's `x5c` is a handful
/// of certificates; nothing we read comes near it.
const MAX_ITEMS: u64 = 64;

impl Cbor {
    pub fn int(&self) -> Option<i64> {
        match *self {
            Cbor::Uint(n) => i64::try_from(n).ok(),
            Cbor::Nint(n) => i64::try_from(n).ok().map(|n| -1 - n),
            _ => None,
        }
    }

    /// A map's value under integer `k`.
    pub fn get_int(&self, k: i64) -> Option<&Cbor> {
        match self {
            Cbor::Map(m) => m.iter().find(|(key, _)| key.int() == Some(k)).map(|(_, v)| v),
            _ => None,
        }
    }

    /// A map's value under text `k`.
    pub fn get_text(&self, k: &str) -> Option<&Cbor> {
        match self {
            Cbor::Map(m) => m.iter().find(|(key, _)| matches!(key, Cbor::Text(t) if t == k)).map(|(_, v)| v),
            _ => None,
        }
    }

    fn bytes(&self) -> Option<&[u8]> {
        match self {
            Cbor::Bytes(b) => Some(b),
            _ => None,
        }
    }
}

/// One item from the front of `b`, and what follows it.
pub fn cbor_read(b: &[u8]) -> Result<(Cbor, &[u8]), Fail> {
    let mut rest = b;
    let v = item(&mut rest, 0)?;
    Ok((v, rest))
}

/// Exactly one item: trailing bytes are an error.
pub fn cbor_read_exact(b: &[u8]) -> Result<Cbor, Fail> {
    let (v, rest) = cbor_read(b)?;
    if !rest.is_empty() {
        return Err(Fail::Malformed);
    }
    Ok(v)
}

fn take<'a>(b: &mut &'a [u8], n: usize) -> Result<&'a [u8], Fail> {
    if b.len() < n {
        return Err(Fail::Malformed);
    }
    let (h, t) = b.split_at(n);
    *b = t;
    Ok(h)
}

fn arg(b: &mut &[u8], info: u8) -> Result<u64, Fail> {
    Ok(match info {
        0..=23 => info as u64,
        24 => take(b, 1)?[0] as u64,
        25 => u16::from_be_bytes(take(b, 2)?.try_into().unwrap()) as u64,
        26 => u32::from_be_bytes(take(b, 4)?.try_into().unwrap()) as u64,
        27 => u64::from_be_bytes(take(b, 8)?.try_into().unwrap()),
        // reserved, or an indefinite length
        _ => return Err(Fail::Malformed),
    })
}

fn item(b: &mut &[u8], depth: usize) -> Result<Cbor, Fail> {
    if depth > MAX_DEPTH {
        return Err(Fail::Malformed);
    }
    let head = take(b, 1)?[0];
    let (major, info) = (head >> 5, head & 0x1f);
    let n = arg(b, info)?;
    Ok(match major {
        0 => Cbor::Uint(n),
        1 => Cbor::Nint(n),
        2 => Cbor::Bytes(take(b, usize::try_from(n).map_err(|_| Fail::Malformed)?)?.to_vec()),
        3 => {
            let raw = take(b, usize::try_from(n).map_err(|_| Fail::Malformed)?)?;
            Cbor::Text(String::from_utf8(raw.to_vec()).map_err(|_| Fail::Malformed)?)
        }
        4 => {
            // every item is at least a byte: a count past what's left is junk
            if n > MAX_ITEMS || n > b.len() as u64 {
                return Err(Fail::Malformed);
            }
            let mut v = Vec::with_capacity(n as usize);
            for _ in 0..n {
                v.push(item(b, depth + 1)?);
            }
            Cbor::Array(v)
        }
        5 => {
            if n > MAX_ITEMS || n.saturating_mul(2) > b.len() as u64 {
                return Err(Fail::Malformed);
            }
            let mut m: Vec<(Cbor, Cbor)> = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let k = item(b, depth + 1)?;
                if !matches!(k, Cbor::Uint(_) | Cbor::Nint(_) | Cbor::Text(_)) || m.iter().any(|(x, _)| *x == k) {
                    return Err(Fail::Malformed);
                }
                let v = item(b, depth + 1)?;
                m.push((k, v));
            }
            Cbor::Map(m)
        }
        7 => match info {
            20 => Cbor::Bool(false),
            21 => Cbor::Bool(true),
            22 => Cbor::Null,
            _ => return Err(Fail::Malformed),
        },
        // tags
        _ => return Err(Fail::Malformed),
    })
}

// ---------------------------------------------------------------- keys

/// A credential public key we can verify with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublicKey {
    /// SEC1 uncompressed point, checked on the curve.
    Es256([u8; 65]),
    Ed25519([u8; 32]),
    /// Big-endian, no leading zeros.
    Rs256 {
        n: Vec<u8>,
        e: Vec<u8>,
    },
}

/// An Ed25519 key that isn't a usable one: a point of small order (the
/// identity verifies any message for some implementations), in either sign,
/// or a y coordinate that isn't reduced mod p. libsodium's blocklist.
fn weak_ed25519(x: &[u8; 32]) -> bool {
    let mut c = *x;
    c[31] &= 0x7f;
    let ff = |lo: u8| {
        let mut v = [0xffu8; 32];
        v[0] = lo;
        v[31] = 0x7f;
        v
    };
    let mut one = [0u8; 32];
    one[0] = 1;
    const ORDER8_A: [u8; 32] = [
        0x26, 0xe8, 0x95, 0x8f, 0xc2, 0xb2, 0x27, 0xb0, 0x45, 0xc3, 0xf4, 0x89, 0xf2, 0xef, 0x98, 0xf0, 0xd5, 0xdf,
        0xac, 0x05, 0xd3, 0xc6, 0x33, 0x39, 0xb1, 0x38, 0x02, 0x88, 0x6d, 0x53, 0xfc, 0x05,
    ];
    const ORDER8_B: [u8; 32] = [
        0xc7, 0x17, 0x6a, 0x70, 0x3d, 0x4d, 0xd8, 0x4f, 0xba, 0x3c, 0x0b, 0x76, 0x0d, 0x10, 0x67, 0x0f, 0x2a, 0x20,
        0x53, 0xfa, 0x2c, 0x39, 0xcc, 0xc6, 0x4e, 0xc7, 0xfd, 0x77, 0x92, 0xac, 0x03, 0x7a,
    ];
    // y >= p = 2^255 - 19: not canonical (this also covers p and p + 1)
    let unreduced = c[31] == 0x7f && c[1..31].iter().all(|b| *b == 0xff) && c[0] >= 0xed;
    unreduced || [[0u8; 32], one, ORDER8_A, ORDER8_B, ff(0xec)].contains(&c)
}

/// Base64url as WebAuthn JSON carries it: no padding.
pub fn b64u_strict(s: &str) -> Option<Vec<u8>> {
    if s.contains('=') {
        return None;
    }
    b64u_decode(s)
}

fn strip_zeros(b: &[u8]) -> Vec<u8> {
    let i = b.iter().position(|x| *x != 0).unwrap_or(b.len());
    b[i..].to_vec()
}

impl PublicKey {
    /// From the COSE_Key bytes stored at registration.
    pub fn from_cose(b: &[u8]) -> Result<PublicKey, Fail> {
        Self::from_cose_item(&cbor_read_exact(b)?)
    }

    fn from_cose_item(k: &Cbor) -> Result<PublicKey, Fail> {
        if !matches!(k, Cbor::Map(_)) {
            return Err(Fail::Key);
        }
        let int = |label| k.get_int(label).and_then(Cbor::int);
        let bytes = |label| k.get_int(label).and_then(Cbor::bytes);
        let (kty, alg) = (int(1).ok_or(Fail::Key)?, int(3).ok_or(Fail::Key)?);
        match (kty, alg) {
            (2, ALG_ES256) => {
                let (x, y) = (bytes(-2).ok_or(Fail::Key)?, bytes(-3).ok_or(Fail::Key)?);
                if int(-1) != Some(1) || x.len() != 32 || y.len() != 32 {
                    return Err(Fail::Key);
                }
                let mut point = [0u8; 65];
                point[0] = 4;
                point[1..33].copy_from_slice(x);
                point[33..].copy_from_slice(y);
                p256::PublicKey::from_sec1_bytes(&point).map_err(|_| Fail::Key)?;
                Ok(PublicKey::Es256(point))
            }
            (1, ALG_EDDSA) => {
                let x = bytes(-2).ok_or(Fail::Key)?;
                if int(-1) != Some(6) || x.len() != 32 {
                    return Err(Fail::Key);
                }
                let x: [u8; 32] = x.try_into().unwrap();
                if weak_ed25519(&x) {
                    return Err(Fail::Key);
                }
                Ok(PublicKey::Ed25519(x))
            }
            (3, ALG_RS256) => {
                let (n, e) = (strip_zeros(bytes(-1).ok_or(Fail::Key)?), strip_zeros(bytes(-2).ok_or(Fail::Key)?));
                let (Some(first), Some(last)) = (n.first(), n.last()) else { return Err(Fail::Key) };
                let bits = n.len() * 8 - first.leading_zeros() as usize;
                // a modulus is odd, and so is a usable exponent (at least 3)
                let e_val = e.iter().fold(0u64, |a, b| (a << 8) | *b as u64);
                if !(MIN_RSA_BITS..=MAX_RSA_BITS).contains(&bits)
                    || last & 1 == 0
                    || e.is_empty()
                    || e.len() > 4
                    || e_val < 3
                    || e_val & 1 == 0
                {
                    return Err(Fail::Key);
                }
                Ok(PublicKey::Rs256 { n, e })
            }
            _ => Err(Fail::Algorithm),
        }
    }

    pub fn alg(&self) -> i64 {
        match self {
            PublicKey::Es256(_) => ALG_ES256,
            PublicKey::Ed25519(_) => ALG_EDDSA,
            PublicKey::Rs256 { .. } => ALG_RS256,
        }
    }

    pub fn verify(&self, msg: &[u8], sig: &[u8]) -> Result<(), Fail> {
        use ring::signature as s;
        let r = match self {
            // WebAuthn ES256 signatures are ASN.1 DER
            PublicKey::Es256(p) => s::UnparsedPublicKey::new(&s::ECDSA_P256_SHA256_ASN1, p).verify(msg, sig),
            PublicKey::Ed25519(x) => s::UnparsedPublicKey::new(&s::ED25519, x).verify(msg, sig),
            PublicKey::Rs256 { n, e } => {
                s::RsaPublicKeyComponents { n, e }.verify(&s::RSA_PKCS1_2048_8192_SHA256, msg, sig)
            }
        };
        r.map_err(|_| Fail::Signature)
    }
}

// ---------------------------------------------------------------- authenticator data

#[derive(Clone, Debug)]
pub struct Attested {
    pub aaguid: [u8; 16],
    pub credential_id: Vec<u8>,
    /// The COSE_Key exactly as the authenticator encoded it.
    pub cose_key: Vec<u8>,
    pub key: PublicKey,
}

#[derive(Clone, Debug)]
pub struct AuthData {
    pub rp_id_hash: [u8; 32],
    pub flags: u8,
    pub sign_count: u32,
    pub attested: Option<Attested>,
}

impl AuthData {
    pub fn has(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }
}

pub fn parse_auth_data(b: &[u8]) -> Result<AuthData, Fail> {
    let mut rest = b;
    let rp_id_hash: [u8; 32] = take(&mut rest, 32)?.try_into().unwrap();
    let flags = take(&mut rest, 1)?[0];
    let sign_count = u32::from_be_bytes(take(&mut rest, 4)?.try_into().unwrap());
    // backed up but not backup eligible can't happen
    if flags & FLAG_BS != 0 && flags & FLAG_BE == 0 {
        return Err(Fail::Malformed);
    }
    let attested = if flags & FLAG_AT != 0 {
        let aaguid: [u8; 16] = take(&mut rest, 16)?.try_into().unwrap();
        let len = u16::from_be_bytes(take(&mut rest, 2)?.try_into().unwrap()) as usize;
        if len == 0 || len > MAX_CREDENTIAL_ID {
            return Err(Fail::CredentialId);
        }
        let credential_id = take(&mut rest, len)?.to_vec();
        let (k, after) = cbor_read(rest)?;
        let cose_key = rest[..rest.len() - after.len()].to_vec();
        rest = after;
        Some(Attested { aaguid, credential_id, cose_key, key: PublicKey::from_cose_item(&k)? })
    } else {
        None
    };
    if flags & FLAG_ED != 0 {
        let (ext, after) = cbor_read(rest)?;
        if !matches!(ext, Cbor::Map(_)) {
            return Err(Fail::Malformed);
        }
        rest = after;
    }
    if !rest.is_empty() {
        return Err(Fail::Malformed);
    }
    Ok(AuthData { rp_id_hash, flags, sign_count, attested })
}

// ---------------------------------------------------------------- client data

/// The relying party: this server's public URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rp {
    /// The public URL's host.
    pub id: String,
    /// `scheme://host[:port]`, compared exactly with `clientDataJSON.origin`.
    pub origin: String,
}

impl Rp {
    pub fn from_public_url(url: &str) -> Option<Rp> {
        let u = reqwest::Url::parse(url).ok()?;
        let host = u.host_str()?.to_string();
        if !matches!(u.scheme(), "https" | "http") {
            return None;
        }
        Some(Rp { id: host, origin: u.origin().ascii_serialization() })
    }

    fn id_hash(&self) -> [u8; 32] {
        Sha256::digest(self.id.as_bytes()).into()
    }
}

#[derive(serde::Deserialize)]
struct ClientData {
    #[serde(rename = "type")]
    ty: String,
    challenge: String,
    origin: String,
    #[serde(rename = "crossOrigin", default)]
    cross_origin: Option<bool>,
    #[serde(rename = "topOrigin", default)]
    top_origin: Option<String>,
}

fn client_data(b: &[u8]) -> Result<ClientData, Fail> {
    if b.len() > MAX_CLIENT_DATA {
        return Err(Fail::TooLarge);
    }
    // serde would also take a struct as a JSON array of its fields
    if b.iter().find(|c| !c.is_ascii_whitespace()) != Some(&b'{') {
        return Err(Fail::Malformed);
    }
    serde_json::from_slice(b).map_err(|_| Fail::Malformed)
}

/// The challenge a `clientDataJSON` answers, so the caller can find what
/// it was minted for before checking the rest.
pub fn client_data_challenge(b: &[u8]) -> Result<Vec<u8>, Fail> {
    b64u_strict(&client_data(b)?.challenge).ok_or(Fail::Challenge)
}

fn check_client_data(b: &[u8], ty: &str, challenge: &[u8], rp: &Rp) -> Result<(), Fail> {
    let c = client_data(b)?;
    if c.ty != ty {
        return Err(Fail::Type);
    }
    if !b64u_strict(&c.challenge).is_some_and(|got| ct_eq(&got, challenge)) {
        return Err(Fail::Challenge);
    }
    if c.origin != rp.origin {
        return Err(Fail::Origin);
    }
    // our pages are never framed (frame-ancestors 'none')
    if c.cross_origin == Some(true) || c.top_origin.is_some() {
        return Err(Fail::CrossOrigin);
    }
    Ok(())
}

fn check_flags(a: &AuthData, rp: &Rp, require_uv: bool) -> Result<(), Fail> {
    if !ct_eq(&a.rp_id_hash, &rp.id_hash()) {
        return Err(Fail::RpId);
    }
    if !a.has(FLAG_UP) {
        return Err(Fail::UserPresent);
    }
    if require_uv && !a.has(FLAG_UV) {
        return Err(Fail::UserVerified);
    }
    Ok(())
}

// ---------------------------------------------------------------- ceremonies

/// A registration that passed every check.
#[derive(Clone, Debug)]
pub struct Registration {
    pub credential_id: Vec<u8>,
    pub cose_key: Vec<u8>,
    pub alg: i64,
    pub sign_count: u32,
    pub aaguid: [u8; 16],
    pub uv: bool,
    pub backup_eligible: bool,
    pub backed_up: bool,
}

/// `raw_id` is the credential's `rawId`, which has to name the same
/// credential as the attested data.
pub fn verify_registration(
    rp: &Rp,
    challenge: &[u8],
    raw_id: &[u8],
    client_data_json: &[u8],
    attestation_object: &[u8],
    require_uv: bool,
) -> Result<Registration, Fail> {
    if attestation_object.len() > MAX_ATTESTATION_OBJECT || raw_id.len() > MAX_CREDENTIAL_ID {
        return Err(Fail::TooLarge);
    }
    check_client_data(client_data_json, "webauthn.create", challenge, rp)?;
    let obj = cbor_read_exact(attestation_object)?;
    let (Some(Cbor::Text(fmt)), Some(stmt @ Cbor::Map(_)), Some(Cbor::Bytes(auth_data))) =
        (obj.get_text("fmt"), obj.get_text("attStmt"), obj.get_text("authData"))
    else {
        return Err(Fail::Malformed);
    };
    // we don't verify attestation: any format's statement is ignored, but
    // "none" has to be the empty statement it claims to be
    if fmt == "none" && *stmt != Cbor::Map(Vec::new()) {
        return Err(Fail::Malformed);
    }
    let a = parse_auth_data(auth_data)?;
    check_flags(&a, rp, require_uv)?;
    let att = a.attested.clone().ok_or(Fail::Malformed)?;
    if att.credential_id != raw_id {
        return Err(Fail::CredentialId);
    }
    Ok(Registration {
        credential_id: att.credential_id,
        alg: att.key.alg(),
        cose_key: att.cose_key,
        sign_count: a.sign_count,
        aaguid: att.aaguid,
        uv: a.has(FLAG_UV),
        backup_eligible: a.has(FLAG_BE),
        backed_up: a.has(FLAG_BS),
    })
}

/// An assertion that passed every check but the counter, which needs the
/// stored state ([`counter_regressed`]).
#[derive(Clone, Copy, Debug)]
pub struct Assertion {
    pub sign_count: u32,
    pub uv: bool,
    pub backup_eligible: bool,
    pub backed_up: bool,
}

pub fn verify_assertion(
    rp: &Rp,
    challenge: &[u8],
    key: &PublicKey,
    client_data_json: &[u8],
    authenticator_data: &[u8],
    signature: &[u8],
    require_uv: bool,
) -> Result<Assertion, Fail> {
    if authenticator_data.len() > MAX_AUTHENTICATOR_DATA || signature.len() > MAX_SIGNATURE {
        return Err(Fail::TooLarge);
    }
    check_client_data(client_data_json, "webauthn.get", challenge, rp)?;
    let a = parse_auth_data(authenticator_data)?;
    if a.attested.is_some() {
        return Err(Fail::Malformed);
    }
    check_flags(&a, rp, require_uv)?;
    let mut msg = Vec::with_capacity(authenticator_data.len() + 32);
    msg.extend_from_slice(authenticator_data);
    msg.extend_from_slice(&Sha256::digest(client_data_json));
    key.verify(&msg, signature)?;
    Ok(Assertion {
        sign_count: a.sign_count,
        uv: a.has(FLAG_UV),
        backup_eligible: a.has(FLAG_BE),
        backed_up: a.has(FLAG_BS),
    })
}

/// Synced passkeys stay at 0 forever; anything else has to go up.
pub fn counter_regressed(stored: u32, new: u32) -> bool {
    !(new > stored || (stored == 0 && new == 0))
}

// ---------------------------------------------------------------- challenges

/// The same as a PAR request's and a pending second factor's lifetime.
pub const CHALLENGE_TTL: u64 = 300;
const CHALLENGE_VERSION: u8 = 1;
const CHALLENGE_LEN: usize = 1 + 16 + 8 + 16;

/// What a checked challenge was: its nonce is claimed once at the
/// account's owner, until `exp`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Challenge {
    pub nonce: [u8; 16],
    pub exp: u64,
}

impl Challenge {
    /// The cluster-wide single-use claim's key.
    pub fn replay_key(&self) -> String {
        format!("wa:{}", hex::encode(self.nonce))
    }
}

fn challenge_mac(key: &[u8; 32], purpose: &str, binding: &str, nonce: &[u8], exp: u64) -> [u8; 32] {
    hmac_sha256(key, &[b"vlpds-webauthn-challenge", purpose.as_bytes(), binding.as_bytes(), nonce, &exp.to_be_bytes()])
}

/// A stateless challenge for one `purpose` and `binding` (the flow, browser
/// and account it may finish), valid [`CHALLENGE_TTL`].
pub fn mint_challenge(key: &[u8; 32], purpose: &str, binding: &str, now: u64) -> Vec<u8> {
    let nonce: [u8; 16] = rand::random();
    let exp = now + CHALLENGE_TTL;
    let mut out = Vec::with_capacity(CHALLENGE_LEN);
    out.push(CHALLENGE_VERSION);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&exp.to_be_bytes());
    out.extend_from_slice(&challenge_mac(key, purpose, binding, &nonce, exp)[..16]);
    out
}

pub fn open_challenge(key: &[u8; 32], purpose: &str, binding: &str, b: &[u8], now: u64) -> Result<Challenge, Fail> {
    if b.len() != CHALLENGE_LEN || b[0] != CHALLENGE_VERSION {
        return Err(Fail::Challenge);
    }
    let nonce: [u8; 16] = b[1..17].try_into().unwrap();
    let exp = u64::from_be_bytes(b[17..25].try_into().unwrap());
    if !ct_eq(&b[25..], &challenge_mac(key, purpose, binding, &nonce, exp)[..16]) {
        return Err(Fail::Challenge);
    }
    if now > exp || exp > now + CHALLENGE_TTL + 60 {
        return Err(Fail::Challenge);
    }
    Ok(Challenge { nonce, exp })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oauth::util::b64u;
    use p256::ecdsa::signature::Signer;

    fn d(s: &str) -> Vec<u8> {
        b64u_decode(s).unwrap()
    }

    fn localhost() -> Rp {
        Rp { id: "localhost".into(), origin: "http://localhost:5000".into() }
    }

    // Recorded browser output: py_webauthn's tests (duo-labs/py_webauthn,
    // tests/test_verify_authentication_response.py and
    // test_verify_registration_response.py).

    #[test]
    fn recorded_es256_assertion() {
        let key = PublicKey::from_cose(&d(
            "pQECAyYgASFYIIeDTe-gN8A-zQclHoRnGFWN8ehM1b7yAsa8I8KIvmplIlgg4nFGT5px8o6gpPZZhO01wdy9crDSA_Ngtkx0vGpvPHI",
        ))
        .unwrap();
        assert_eq!(key.alg(), ALG_ES256);
        let a = verify_assertion(
            &localhost(),
            &d("xi30GPGAFYRxVDpY1sM10DaLzVQG66nv-_7RUazH0vI2YvG8LYgDEnvN5fZZNVuvEDuMi9te3VLqb42N0fkLGA"),
            &key,
            &d("eyJjaGFsbGVuZ2UiOiJ4aTMwR1BHQUZZUnhWRHBZMXNNMTBEYUx6VlFHNjZudi1fN1JVYXpIMHZJMll2RzhMWWdERW52TjVmWlpOVnV2RUR1TWk5dGUzVkxxYjQyTjBma0xHQSIsImNsaWVudEV4dGVuc2lvbnMiOnt9LCJoYXNoQWxnb3JpdGhtIjoiU0hBLTI1NiIsIm9yaWdpbiI6Imh0dHA6Ly9sb2NhbGhvc3Q6NTAwMCIsInR5cGUiOiJ3ZWJhdXRobi5nZXQifQ"),
            &d("SZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2MBAAAATg"),
            &d("MEUCIGisVZOBapCWbnJJvjelIzwpixxIwkjCCb5aCHafQu68AiEA88v-2pJNNApPFwAKFiNuf82-2hBxYW5kGwVweeoxCwo"),
            false,
        )
        .unwrap();
        assert_eq!((a.sign_count, a.uv, a.backup_eligible, a.backed_up), (78, false, false, false));
        assert!(!counter_regressed(77, a.sign_count));
    }

    const RS256_KEY: &str = "pAEDAzkBACBZAQDfV20epzvQP-HtcdDpX-cGzdOxy73WQEvsU7Dnr9UWJophEfpngouvgnRLXaEUn_d8HGkp_HIx8rrpkx4BVs6X_B6ZjhLlezjIdJbLbVeb92BaEsmNn1HW2N9Xj2QM8cH-yx28_vCjf82ahQ9gyAr552Bn96G22n8jqFRQKdVpO-f-bvpvaP3IQ9F5LCX7CUaxptgbog1SFO6FI6ob5SlVVB00lVXsaYg8cIDZxCkkENkGiFPgwEaZ7995SCbiyCpUJbMqToLMgojPkAhWeyktu7TlK6UBWdJMHc3FPAIs0lH_2_2hKS-mGI1uZAFVAfW1X-mzKL0czUm2P1UlUox7IUMBAAE";
    const RS256_CDJ: &str = "eyJ0eXBlIjoid2ViYXV0aG4uZ2V0IiwiY2hhbGxlbmdlIjoiaVBtQWkxUHAxWEw2b0FncTNQV1p0WlBuWmExekZVRG9HYmFRMF9LdlZHMWxGMnMzUnRfM280dVN6Y2N5MHRtY1RJcFRUVDRCVTFULUk0bWFhdm5kalEiLCJvcmlnaW4iOiJodHRwOi8vbG9jYWxob3N0OjUwMDAiLCJjcm9zc09yaWdpbiI6ZmFsc2V9";
    const RS256_SIG: &str = "iOHKX3erU5_OYP_r_9HLZ-CexCE4bQRrxM8WmuoKTDdhAnZSeTP0sjECjvjfeS8MJzN1ArmvV0H0C3yy_FdRFfcpUPZzdZ7bBcmPh1XPdxRwY747OrIzcTLTFQUPdn1U-izCZtP_78VGw9pCpdMsv4CUzZdJbEcRtQuRS03qUjqDaovoJhOqEBmxJn9Wu8tBi_Qx7A33RbYjlfyLm_EDqimzDZhyietyop6XUcpKarKqVH0M6mMrM5zTjp8xf3W7odFCadXEJg-ERZqFM0-9Uup6kJNLbr6C5J4NDYmSm3HCSA6lp2iEiMPKU8Ii7QZ61kybXLxsX4w4Dm3fOLjmDw";

    #[test]
    fn recorded_rs256_assertion_windows_hello() {
        let key = PublicKey::from_cose(&d(RS256_KEY)).unwrap();
        assert_eq!(key.alg(), ALG_RS256);
        let ch = d("iPmAi1Pp1XL6oAgq3PWZtZPnZa1zFUDoGbaQ0_KvVG1lF2s3Rt_3o4uSzccy0tmcTIpTTT4BU1T-I4maavndjQ");
        let ad = d("SZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2MFAAAAAQ");
        let a = verify_assertion(&localhost(), &ch, &key, &d(RS256_CDJ), &ad, &d(RS256_SIG), true).unwrap();
        assert_eq!((a.sign_count, a.uv), (1, true));
        let mut bad = d(RS256_SIG);
        bad[100] ^= 1;
        assert_eq!(
            verify_assertion(&localhost(), &ch, &key, &d(RS256_CDJ), &ad, &bad, true).unwrap_err(),
            Fail::Signature
        );
    }

    #[test]
    fn recorded_eddsa_assertion() {
        let key = PublicKey::from_cose(&d("pAEBAycgBiFYIMz6_SUFLiDid2Yhlq0YboyJ-CDrIrNpkPUGmJp4D3Dp")).unwrap();
        assert_eq!(key.alg(), ALG_EDDSA);
        let a = verify_assertion(
            &localhost(),
            &d("eZ4eeA3O4jy5FIzqDaSJ6JDNGu0bBc5zI1DjQ_kLso1WNqkG6k5mCYf1dtQhVUiBWZWlZkzR5MFeeWCpJRUNXw"),
            &key,
            &d("eyJ0eXBlIjoid2ViYXV0aG4uZ2V0IiwiY2hhbGxlbmdlIjoiZVo0ZWVBM080ank1Rkl6cURhU0o2SkROR3UwYkJjNXpJMURqUV9rTHNvMVdOcWtHNms1bUNZZjFkdFFoVlVpQldaV2xaa3pSNU1GZWVXQ3BKUlVOWHciLCJvcmlnaW4iOiJodHRwOi8vbG9jYWxob3N0OjUwMDAiLCJjcm9zc09yaWdpbiI6ZmFsc2V9"),
            &d("SZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2MBAAAABw"),
            &d("RRWV8mYDRvK7YdQgdtZD4pJ2dh1D_IWZ_D6jsZo6FHJBoenbj0CVT5nA20vUzlRhN4R6dOEUHmUwP1F8eRBhBg"),
            false,
        )
        .unwrap();
        assert_eq!(a.sign_count, 7);
    }

    const REG_ID: &str = "9y1xA8Tmg1FEmT-c7_fvWZ_uoTuoih3OvR45_oAK-cwHWhAbXrl2q62iLVTjiyEZ7O7n-CROOY494k7Q3xrs_w";
    const REG_ATT: &str = "o2NmbXRkbm9uZWdhdHRTdG10oGhhdXRoRGF0YVjESZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2NFAAAAFwAAAAAAAAAAAAAAAAAAAAAAQPctcQPE5oNRRJk_nO_371mf7qE7qIodzr0eOf6ACvnMB1oQG165dqutoi1U44shGezu5_gkTjmOPeJO0N8a7P-lAQIDJiABIVggSFbUJF-42Ug3pdM8rDRFu_N5oiVEysPDB6n66r_7dZAiWCDUVnB39FlGypL-qAoIO9xWHtJygo2jfDmHl-_eKFRLDA";
    const REG_CDJ: &str = "eyJ0eXBlIjoid2ViYXV0aG4uY3JlYXRlIiwiY2hhbGxlbmdlIjoiVHdON240V1R5R0tMYzRaWS1xR3NGcUtuSE00bmdscXN5VjBJQ0psTjJUTzlYaVJ5RnRya2FEd1V2c3FsLWdrTEpYUDZmbkYxTWxyWjUzTW00UjdDdnciLCJvcmlnaW4iOiJodHRwOi8vbG9jYWxob3N0OjUwMDAiLCJjcm9zc09yaWdpbiI6ZmFsc2V9";
    const REG_CH: &str = "TwN7n4WTyGKLc4ZY-qGsFqKnHM4nglqsyV0ICJlN2TO9XiRyFtrkaDwUvsql-gkLJXP6fnF1MlrZ53Mm4R7Cvw";

    #[test]
    fn recorded_none_registration() {
        let r = verify_registration(&localhost(), &d(REG_CH), &d(REG_ID), &d(REG_CDJ), &d(REG_ATT), false).unwrap();
        assert_eq!(r.credential_id, d(REG_ID));
        assert_eq!(
            b64u(&r.cose_key),
            "pQECAyYgASFYIEhW1CRfuNlIN6XTPKw0RbvzeaIlRMrDwwep-uq_-3WQIlgg1FZwd_RZRsqS_qgKCDvcVh7ScoKNo3w5h5fv3ihUSww"
        );
        assert_eq!((r.alg, r.sign_count, r.aaguid, r.backup_eligible), (ALG_ES256, 23, [0; 16], false));
        // its authenticator did user verification
        assert!(r.uv);
        // rawId has to be the attested credential
        assert_eq!(
            verify_registration(&localhost(), &d(REG_CH), b"other", &d(REG_CDJ), &d(REG_ATT), false).unwrap_err(),
            Fail::CredentialId
        );
        // truncations of the attestation object never parse
        let att = d(REG_ATT);
        for n in 0..att.len() {
            assert!(verify_registration(&localhost(), &d(REG_CH), &d(REG_ID), &d(REG_CDJ), &att[..n], false).is_err());
        }
        // and a trailing byte is refused
        let mut long = att.clone();
        long.push(0);
        assert_eq!(
            verify_registration(&localhost(), &d(REG_CH), &d(REG_ID), &d(REG_CDJ), &long, false).unwrap_err(),
            Fail::Malformed
        );
    }

    // ---- a software authenticator for the one-check-each failures

    struct Auth {
        sk: p256::ecdsa::SigningKey,
        id: Vec<u8>,
    }

    fn cose_es256(sk: &p256::ecdsa::SigningKey) -> Vec<u8> {
        let p = sk.verifying_key().to_encoded_point(false);
        let mut out = vec![0xa5, 0x01, 0x02, 0x03, 0x26, 0x20, 0x01, 0x21, 0x58, 0x20];
        out.extend_from_slice(p.x().unwrap());
        out.extend_from_slice(&[0x22, 0x58, 0x20]);
        out.extend_from_slice(p.y().unwrap());
        out
    }

    fn cdj(ty: &str, challenge: &[u8], origin: &str, extra: &str) -> Vec<u8> {
        format!(r#"{{"type":"{ty}","challenge":"{}","origin":"{origin}"{extra}}}"#, b64u(challenge)).into_bytes()
    }

    fn auth_data(rp_id: &str, flags: u8, count: u32, attested: Option<(&[u8], &[u8])>) -> Vec<u8> {
        let mut out = Sha256::digest(rp_id.as_bytes()).to_vec();
        out.push(flags | if attested.is_some() { FLAG_AT } else { 0 });
        out.extend_from_slice(&count.to_be_bytes());
        if let Some((id, key)) = attested {
            out.extend_from_slice(&[7; 16]);
            out.extend_from_slice(&(id.len() as u16).to_be_bytes());
            out.extend_from_slice(id);
            out.extend_from_slice(key);
        }
        out
    }

    fn att_obj(auth_data: &[u8]) -> Vec<u8> {
        let mut out = vec![0xa3, 0x63];
        out.extend_from_slice(b"fmt");
        out.extend_from_slice(&[0x64]);
        out.extend_from_slice(b"none");
        out.extend_from_slice(&[0x67]);
        out.extend_from_slice(b"attStmt");
        out.push(0xa0);
        out.extend_from_slice(&[0x68]);
        out.extend_from_slice(b"authData");
        out.extend_from_slice(&[0x59]);
        out.extend_from_slice(&(auth_data.len() as u16).to_be_bytes());
        out.extend_from_slice(auth_data);
        out
    }

    impl Auth {
        fn new() -> Auth {
            Auth {
                sk: p256::ecdsa::SigningKey::random(&mut rand::rngs::OsRng),
                id: rand::random::<[u8; 16]>().to_vec(),
            }
        }

        fn key(&self) -> PublicKey {
            PublicKey::from_cose(&cose_es256(&self.sk)).unwrap()
        }

        fn sign(&self, ad: &[u8], cdj: &[u8]) -> Vec<u8> {
            let mut msg = ad.to_vec();
            msg.extend_from_slice(&Sha256::digest(cdj));
            let sig: p256::ecdsa::Signature = self.sk.sign(&msg);
            sig.to_der().as_bytes().to_vec()
        }
    }

    struct Get {
        ty: &'static str,
        origin: &'static str,
        extra: &'static str,
        rp_id: &'static str,
        flags: u8,
        count: u32,
    }

    impl Default for Get {
        fn default() -> Get {
            Get {
                ty: "webauthn.get",
                origin: "http://localhost:5000",
                extra: "",
                rp_id: "localhost",
                flags: FLAG_UP | FLAG_UV,
                count: 5,
            }
        }
    }

    fn get(a: &Auth, g: Get, require_uv: bool) -> Result<Assertion, Fail> {
        let ch = b"the challenge".to_vec();
        let c = cdj(g.ty, &ch, g.origin, g.extra);
        let ad = auth_data(g.rp_id, g.flags, g.count, None);
        let sig = a.sign(&ad, &c);
        verify_assertion(&localhost(), &ch, &a.key(), &c, &ad, &sig, require_uv)
    }

    #[test]
    fn one_failure_per_check() {
        let a = Auth::new();
        assert!(get(&a, Get::default(), true).is_ok());
        let fails = [
            (Get { origin: "http://localhost:5001", ..Default::default() }, Fail::Origin),
            (Get { origin: "https://localhost:5000", ..Default::default() }, Fail::Origin),
            (Get { origin: "http://evil.localhost:5000", ..Default::default() }, Fail::Origin),
            (Get { extra: r#","crossOrigin":true"#, ..Default::default() }, Fail::CrossOrigin),
            (Get { extra: r#","topOrigin":"http://localhost:5000""#, ..Default::default() }, Fail::CrossOrigin),
            (Get { ty: "webauthn.create", ..Default::default() }, Fail::Type),
            (Get { rp_id: "example.com", ..Default::default() }, Fail::RpId),
            (Get { flags: FLAG_UV, ..Default::default() }, Fail::UserPresent),
            (Get { flags: FLAG_UP, ..Default::default() }, Fail::UserVerified),
            (Get { flags: FLAG_UP | FLAG_UV | FLAG_BS, ..Default::default() }, Fail::Malformed),
        ];
        for (g, want) in fails {
            assert_eq!(get(&a, g, true).unwrap_err(), want);
        }
        // crossOrigin false is fine; UV only when asked for
        assert!(get(&a, Get { extra: r#","crossOrigin":false"#, ..Default::default() }, true).is_ok());
        assert!(get(&a, Get { flags: FLAG_UP, ..Default::default() }, false).is_ok());

        let ch = b"the challenge".to_vec();
        let c = cdj("webauthn.get", &ch, "http://localhost:5000", "");
        let ad = auth_data("localhost", FLAG_UP, 1, None);
        let sig = a.sign(&ad, &c);
        let rp = localhost();
        // a different challenge
        assert_eq!(verify_assertion(&rp, b"other", &a.key(), &c, &ad, &sig, false).unwrap_err(), Fail::Challenge);
        // another key, a tampered signature, signed data that isn't what was sent
        assert_eq!(verify_assertion(&rp, &ch, &Auth::new().key(), &c, &ad, &sig, false).unwrap_err(), Fail::Signature);
        let mut ad2 = ad.clone();
        ad2[36] = 2;
        assert_eq!(verify_assertion(&rp, &ch, &a.key(), &c, &ad2, &sig, false).unwrap_err(), Fail::Signature);
        // trailing bytes after the authenticator data
        let mut ad3 = ad.clone();
        ad3.push(0);
        let sig3 = a.sign(&ad3, &c);
        assert_eq!(verify_assertion(&rp, &ch, &a.key(), &c, &ad3, &sig3, false).unwrap_err(), Fail::Malformed);
        // attested data has no place in an assertion
        let ad4 = auth_data("localhost", FLAG_UP, 1, Some((&a.id, &cose_es256(&a.sk))));
        let sig4 = a.sign(&ad4, &c);
        assert_eq!(verify_assertion(&rp, &ch, &a.key(), &c, &ad4, &sig4, false).unwrap_err(), Fail::Malformed);
        // oversized inputs are refused before parsing
        let big = vec![b' '; MAX_CLIENT_DATA + 1];
        assert_eq!(verify_assertion(&rp, &ch, &a.key(), &big, &ad, &sig, false).unwrap_err(), Fail::TooLarge);
        let big = vec![0; MAX_SIGNATURE + 1];
        assert_eq!(verify_assertion(&rp, &ch, &a.key(), &c, &ad, &big, false).unwrap_err(), Fail::TooLarge);
        let big = vec![0; MAX_AUTHENTICATOR_DATA + 1];
        assert_eq!(verify_assertion(&rp, &ch, &a.key(), &c, &big, &sig, false).unwrap_err(), Fail::TooLarge);
        // not JSON, duplicate fields
        assert_eq!(verify_assertion(&rp, &ch, &a.key(), b"{", &ad, &sig, false).unwrap_err(), Fail::Malformed);
        let dup = format!(
            r#"{{"type":"webauthn.get","type":"webauthn.get","challenge":"{}","origin":"http://localhost:5000"}}"#,
            b64u(&ch)
        );
        assert_eq!(
            verify_assertion(&rp, &ch, &a.key(), dup.as_bytes(), &ad, &sig, false).unwrap_err(),
            Fail::Malformed
        );
    }

    #[test]
    fn registration_checks() {
        let a = Auth::new();
        let rp = localhost();
        let ch = b"reg".to_vec();
        let key = cose_es256(&a.sk);
        let reg = |ty: &str, flags: u8, key: &[u8], id: &[u8]| {
            let c = cdj(ty, &ch, "http://localhost:5000", "");
            let obj = att_obj(&auth_data("localhost", flags, 0, Some((&a.id, key))));
            verify_registration(&rp, &ch, id, &c, &obj, true)
        };
        let ok = reg("webauthn.create", FLAG_UP | FLAG_UV | FLAG_BE | FLAG_BS, &key, &a.id).unwrap();
        assert_eq!((ok.backup_eligible, ok.backed_up, ok.uv), (true, true, true));
        assert_eq!(ok.cose_key, key);
        assert_eq!(reg("webauthn.get", FLAG_UP | FLAG_UV, &key, &a.id).unwrap_err(), Fail::Type);
        assert_eq!(reg("webauthn.create", FLAG_UP, &key, &a.id).unwrap_err(), Fail::UserVerified);
        assert_eq!(reg("webauthn.create", FLAG_UV, &key, &a.id).unwrap_err(), Fail::UserPresent);
        // an unknown algorithm: ES384 (-35)
        let mut es384 = key.clone();
        es384[4] = 0x38;
        es384.insert(5, 0x22);
        assert_eq!(reg("webauthn.create", FLAG_UP | FLAG_UV, &es384, &a.id).unwrap_err(), Fail::Algorithm);
        // a point off the curve
        let mut off = key.clone();
        *off.last_mut().unwrap() ^= 1;
        assert_eq!(reg("webauthn.create", FLAG_UP | FLAG_UV, &off, &a.id).unwrap_err(), Fail::Key);
        // no attested data
        let c = cdj("webauthn.create", &ch, "http://localhost:5000", "");
        let obj = att_obj(&auth_data("localhost", FLAG_UP | FLAG_UV, 0, None));
        assert_eq!(verify_registration(&rp, &ch, &a.id, &c, &obj, true).unwrap_err(), Fail::Malformed);
        // an oversized attestation object
        let big = vec![0; MAX_ATTESTATION_OBJECT + 1];
        assert_eq!(verify_registration(&rp, &ch, &a.id, &c, &big, true).unwrap_err(), Fail::TooLarge);
        // a credential id over the spec's limit
        let long_id = vec![1; MAX_CREDENTIAL_ID + 1];
        let mut ad = Sha256::digest(b"localhost").to_vec();
        ad.push(FLAG_UP | FLAG_UV | FLAG_AT);
        ad.extend_from_slice(&[0, 0, 0, 0]);
        ad.extend_from_slice(&[0; 16]);
        ad.extend_from_slice(&(long_id.len() as u16).to_be_bytes());
        ad.extend_from_slice(&long_id);
        ad.extend_from_slice(&key);
        assert_eq!(verify_registration(&rp, &ch, &a.id, &c, &att_obj(&ad), true).unwrap_err(), Fail::CredentialId);
    }

    #[test]
    fn cose_keys() {
        // RSA under 2048 bits is refused
        let k = cbor_read_exact(&d(RS256_KEY)).unwrap();
        let Cbor::Map(mut m) = k else { panic!() };
        for (key, v) in m.iter_mut() {
            if key.int() == Some(-1) {
                *v = Cbor::Bytes(vec![0xff; 128]);
            }
        }
        assert_eq!(PublicKey::from_cose_item(&Cbor::Map(m)).unwrap_err(), Fail::Key);
        // EdDSA with the wrong curve, a key with no alg
        let mut x25519 = vec![0xa4, 0x01, 0x01, 0x03, 0x27, 0x20, 0x04, 0x21, 0x58, 0x20];
        x25519.extend_from_slice(&[9; 32]);
        assert_eq!(PublicKey::from_cose(&x25519).unwrap_err(), Fail::Key);
        assert_eq!(PublicKey::from_cose(&[0xa1, 0x01, 0x02]).unwrap_err(), Fail::Key);
        // kty and alg that don't go together
        assert_eq!(PublicKey::from_cose(&[0xa2, 0x01, 0x02, 0x03, 0x27]).unwrap_err(), Fail::Algorithm);
    }

    #[test]
    fn rsa_key_with_an_empty_modulus_is_refused() {
        // n = 00 (empty once its zeros are stripped), e = 65537
        let k = [0xa4, 0x01, 0x03, 0x03, 0x39, 0x01, 0x00, 0x20, 0x40, 0x21, 0x43, 0x01, 0x00, 0x01];
        assert_eq!(PublicKey::from_cose(&k).unwrap_err(), Fail::Key);
        let k = [0xa4, 0x01, 0x03, 0x03, 0x39, 0x01, 0x00, 0x20, 0x41, 0x00, 0x21, 0x43, 0x01, 0x00, 0x01];
        assert_eq!(PublicKey::from_cose(&k).unwrap_err(), Fail::Key);
    }

    fn rsa_cose(n: &[u8], e: &[u8]) -> Cbor {
        Cbor::Map(vec![
            (Cbor::Uint(1), Cbor::Uint(3)),
            (Cbor::Uint(3), Cbor::Nint(256)),
            (Cbor::Nint(0), Cbor::Bytes(n.to_vec())),
            (Cbor::Nint(1), Cbor::Bytes(e.to_vec())),
        ])
    }

    #[test]
    fn rsa_keys_must_be_usable() {
        let Cbor::Map(m) = cbor_read_exact(&d(RS256_KEY)).unwrap() else { panic!() };
        let n = m.iter().find(|(k, _)| k.int() == Some(-1)).unwrap().1.bytes().unwrap().to_vec();
        assert!(PublicKey::from_cose_item(&rsa_cose(&n, &[1, 0, 1])).is_ok());
        let mut even = n.clone();
        *even.last_mut().unwrap() &= 0xfe;
        assert_eq!(PublicKey::from_cose_item(&rsa_cose(&even, &[1, 0, 1])).unwrap_err(), Fail::Key, "even modulus");
        for e in [&[1u8][..], &[2], &[1, 0, 0], &[]] {
            assert_eq!(PublicKey::from_cose_item(&rsa_cose(&n, e)).unwrap_err(), Fail::Key, "e = {e:?}");
        }
    }

    #[test]
    fn small_order_ed25519_keys_are_refused() {
        let key = |x: [u8; 32]| {
            let mut c = vec![0xa4, 0x01, 0x01, 0x03, 0x27, 0x20, 0x06, 0x21, 0x58, 0x20];
            c.extend_from_slice(&x);
            PublicKey::from_cose(&c)
        };
        let mut identity = [0u8; 32];
        identity[0] = 1;
        let mut p_minus_1 = [0xffu8; 32];
        p_minus_1[0] = 0xec;
        p_minus_1[31] = 0x7f;
        let mut p = p_minus_1;
        p[0] = 0xed;
        let mut big = [0xffu8; 32];
        big[31] = 0x7f;
        let mut signed_identity = identity;
        signed_identity[31] = 0x80;
        for x in [identity, signed_identity, [0; 32], p_minus_1, p, big] {
            assert_eq!(key(x).unwrap_err(), Fail::Key, "{x:x?}");
        }
        // the recorded key is fine
        assert!(PublicKey::from_cose(&d("pAEBAycgBiFYIMz6_SUFLiDid2Yhlq0YboyJ-CDrIrNpkPUGmJp4D3Dp")).is_ok());
    }

    #[test]
    fn client_data_must_be_an_object_and_unpadded() {
        let rp = localhost();
        let ch = b"the challenge".to_vec();
        let arr = format!(r#"["webauthn.get","{}","http://localhost:5000"]"#, b64u(&ch));
        assert_eq!(check_client_data(arr.as_bytes(), "webauthn.get", &ch, &rp).unwrap_err(), Fail::Malformed);
        let ok = format!(r#" {{"type":"webauthn.get","challenge":"{}","origin":"http://localhost:5000"}}"#, b64u(&ch));
        assert!(check_client_data(ok.as_bytes(), "webauthn.get", &ch, &rp).is_ok());
        let padded = format!(
            r#"{{"type":"webauthn.get","challenge":"{}=","origin":"http://localhost:5000"}}"#,
            b64u(b"the challeng")
        );
        assert_eq!(
            check_client_data(padded.as_bytes(), "webauthn.get", b"the challeng", &rp).unwrap_err(),
            Fail::Challenge
        );
        assert_eq!(b64u_strict("AA=="), None);
        assert_eq!(b64u_strict("AA"), Some(vec![0]));
    }

    #[test]
    fn cbor_reader_is_strict() {
        assert_eq!(cbor_read_exact(&[0xa1, 0x01, 0x02]).unwrap(), Cbor::Map(vec![(Cbor::Uint(1), Cbor::Uint(2))]));
        assert_eq!(cbor_read_exact(&[0x20]).unwrap().int(), Some(-1));
        assert_eq!(cbor_read_exact(&[0x39, 0x01, 0x00]).unwrap().int(), Some(-257));
        // indefinite lengths, tags, floats, reserved info, duplicate keys,
        // non-scalar keys, bad UTF-8, trailing bytes
        for bad in [
            &[0x9f, 0x01, 0xff][..],
            &[0x5f, 0x41, 0x00, 0xff],
            &[0xc0, 0x01],
            &[0xf9, 0x3c, 0x00],
            &[0x1c],
            &[0xa2, 0x01, 0x02, 0x01, 0x03],
            &[0xa1, 0x80, 0x01],
            &[0x62, 0xc3, 0x28],
            &[0x01, 0x01],
        ] {
            assert_eq!(cbor_read_exact(bad).unwrap_err(), Fail::Malformed, "{bad:x?}");
        }
        // nesting is bounded
        let mut deep = vec![0x81; MAX_DEPTH + 1];
        deep.push(0x01);
        assert_eq!(cbor_read_exact(&deep).unwrap_err(), Fail::Malformed);
        let mut ok = vec![0x81; MAX_DEPTH];
        ok.push(0x01);
        assert!(cbor_read_exact(&ok).is_ok());
        // a huge declared length doesn't allocate
        assert_eq!(
            cbor_read_exact(&[0x9b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]).unwrap_err(),
            Fail::Malformed
        );
        assert_eq!(cbor_read_exact(&[0x5b, 0, 0, 0, 1, 0, 0, 0, 0]).unwrap_err(), Fail::Malformed);
    }

    #[test]
    fn cbor_truncation_and_flips_never_panic() {
        let samples = [d(REG_ATT), d(RS256_KEY)];
        for s in &samples {
            for n in 0..s.len() {
                assert!(cbor_read_exact(&s[..n]).is_err(), "prefix {n} parsed");
            }
            for _ in 0..2000 {
                let mut m = s.clone();
                let i = rand::random::<usize>() % m.len();
                m[i] ^= 1 << (rand::random::<u8>() % 8);
                let _ = cbor_read_exact(&m);
                let _ = parse_auth_data(&m);
            }
        }
        for _ in 0..5000 {
            let len = rand::random::<usize>() % 64;
            let junk: Vec<u8> = (0..len).map(|_| rand::random()).collect();
            let _ = cbor_read_exact(&junk);
            let _ = parse_auth_data(&junk);
            let _ = PublicKey::from_cose(&junk);
        }
    }

    #[test]
    fn counter_rules() {
        assert!(!counter_regressed(0, 0), "synced passkeys stay at 0");
        assert!(!counter_regressed(5, 6));
        assert!(!counter_regressed(0, 1));
        assert!(counter_regressed(5, 5), "a repeat is a regression");
        assert!(counter_regressed(5, 4));
        assert!(counter_regressed(5, 0), "back to 0 after counting");
    }

    #[test]
    fn challenges_are_bound_and_expire() {
        let k = [3u8; 32];
        let now = 1_800_000_000;
        let c = mint_challenge(&k, "signin", "dev-a\0req-b", now);
        let opened = open_challenge(&k, "signin", "dev-a\0req-b", &c, now + 10).unwrap();
        assert_eq!(opened.exp, now + CHALLENGE_TTL);
        assert_eq!(opened.replay_key().len(), 3 + 32);
        // another purpose, binding, key, or past its life
        assert_eq!(open_challenge(&k, "2fa", "dev-a\0req-b", &c, now).unwrap_err(), Fail::Challenge);
        assert_eq!(open_challenge(&k, "signin", "dev-a\0req-c", &c, now).unwrap_err(), Fail::Challenge);
        assert_eq!(open_challenge(&[4; 32], "signin", "dev-a\0req-b", &c, now).unwrap_err(), Fail::Challenge);
        assert_eq!(
            open_challenge(&k, "signin", "dev-a\0req-b", &c, now + CHALLENGE_TTL + 1).unwrap_err(),
            Fail::Challenge
        );
        // a flipped bit anywhere
        for i in 0..c.len() {
            let mut m = c.clone();
            m[i] ^= 0x40;
            assert!(open_challenge(&k, "signin", "dev-a\0req-b", &m, now).is_err());
        }
        assert!(open_challenge(&k, "signin", "dev-a\0req-b", &c[..40], now).is_err());
        // two mints differ
        assert_ne!(c, mint_challenge(&k, "signin", "dev-a\0req-b", now));
    }

    #[test]
    fn rp_from_public_url() {
        assert_eq!(
            Rp::from_public_url("https://pds.example.com/").unwrap(),
            Rp { id: "pds.example.com".into(), origin: "https://pds.example.com".into() }
        );
        assert_eq!(
            Rp::from_public_url("http://localhost:2784").unwrap(),
            Rp { id: "localhost".into(), origin: "http://localhost:2784".into() }
        );
        assert_eq!(Rp::from_public_url("https://pds.example.com:443").unwrap().origin, "https://pds.example.com");
    }
}
