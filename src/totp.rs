//! TOTP second factor (RFC 6238: HMAC-SHA-1, 6 digits, 30 s steps, ±1 step
//! of clock skew) plus one-time recovery codes.
//!
//! State lives in private account state (`p/{did}\0totp`) and is written
//! through the partition log like every other durable change. Accepted codes
//! advance `last_step`, so a code can't be replayed inside its window.
//!
//! Used by `com.atproto.server.createSession` (password logins only; app
//! passwords bypass it) and by the OAuth login page via
//! [`check_second_factor`].

use crate::state::{self, Account};
use crate::xrpc::{App, XrpcError};
use axum::http::StatusCode;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use sha1::Sha1;
use sha2::{Digest, Sha256};

pub const STEP_SECS: u64 = 30;
pub const DIGITS: u32 = 6;
/// Accepted clock skew, in steps, on either side of now.
pub const SKEW: u64 = 1;
pub const RECOVERY_CODES: usize = 10;
/// Private-state name under the account's DID.
pub const PRIVATE_NAME: &str = "totp";

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct TotpState {
    /// Base32 secret of the enabled factor (None = TOTP disabled).
    #[serde(default)]
    pub secret: Option<String>,
    /// Base32 secret from setupTotp awaiting confirmTotp.
    #[serde(default)]
    pub pending: Option<String>,
    /// sha256 hex of each unused recovery code (normalized).
    #[serde(default)]
    pub recovery: Vec<String>,
    /// Highest time step accepted so far; codes for steps <= this are replays.
    #[serde(default)]
    pub last_step: u64,
    #[serde(default)]
    pub enabled_at: Option<String>,
}

impl TotpState {
    pub fn enabled(&self) -> bool {
        self.secret.is_some()
    }
}

/// HOTP (RFC 4226) value for `counter`, truncated to `DIGITS` digits.
pub fn hotp(secret: &[u8], counter: u64) -> u32 {
    let mut mac = Hmac::<Sha1>::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(&counter.to_be_bytes());
    let h = mac.finalize().into_bytes();
    let off = (h[h.len() - 1] & 0x0f) as usize;
    let bin = ((h[off] as u32 & 0x7f) << 24)
        | ((h[off + 1] as u32) << 16)
        | ((h[off + 2] as u32) << 8)
        | h[off + 3] as u32;
    bin % 10u32.pow(DIGITS)
}

pub fn step_at(unix_secs: u64) -> u64 {
    unix_secs / STEP_SECS
}

pub fn code_for_step(secret: &[u8], step: u64) -> String {
    format!("{:0width$}", hotp(secret, step), width = DIGITS as usize)
}

pub fn now_secs() -> u64 {
    crate::tid::now_micros() / 1_000_000
}

/// Returns the matched step if `code` is valid for a step within ±SKEW of
/// `now` that is strictly after `after_step`.
pub fn verify_code(secret: &[u8], code: &str, now: u64, after_step: u64) -> Option<u64> {
    let code = code.trim();
    if code.len() != DIGITS as usize || !code.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let now_step = step_at(now);
    let mut found = None;
    for step in now_step.saturating_sub(SKEW)..=now_step + SKEW {
        // compare every candidate (no early exit) in constant time
        if ct_eq(code_for_step(secret, step).as_bytes(), code.as_bytes())
            && step > after_step
            && found.is_none()
        {
            found = Some(step);
        }
    }
    found
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub fn generate_secret() -> Vec<u8> {
    rand::random::<[u8; 20]>().to_vec()
}

/// RFC 4648 base32, uppercase, no padding (what authenticator apps expect).
pub fn base32_encode(b: &[u8]) -> String {
    crate::cid::base32_encode(b).to_ascii_uppercase()
}

pub fn base32_decode(s: &str) -> Option<Vec<u8>> {
    let norm: String = s
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '=')
        .collect::<String>()
        .to_ascii_lowercase();
    crate::cid::base32_decode(&norm)
}

fn uri_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// otpauth:// provisioning URI (Key Uri Format).
pub fn otpauth_uri(secret_b32: &str, issuer: &str, account: &str) -> String {
    format!(
        "otpauth://totp/{}:{}?secret={}&issuer={}&algorithm=SHA1&digits={}&period={}",
        uri_encode(issuer),
        uri_encode(account),
        secret_b32,
        uri_encode(issuer),
        DIGITS,
        STEP_SECS
    )
}

/// Ten random recovery codes formatted `xxxxx-xxxxx` (base32).
pub fn generate_recovery_codes() -> Vec<String> {
    (0..RECOVERY_CODES)
        .map(|_| {
            let s = crate::cid::base32_encode(&rand::random::<[u8; 7]>());
            format!("{}-{}", &s[..5], &s[5..10])
        })
        .collect()
}

pub fn hash_recovery_code(code: &str) -> String {
    let norm: String = code
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase();
    hex::encode(Sha256::digest(
        format!("vlpds-totp-recovery:{norm}").as_bytes(),
    ))
}

pub async fn load(app: &App, did: &str) -> Result<TotpState, XrpcError> {
    match app.get_private(did, PRIVATE_NAME).await? {
        Some(v) => serde_json::from_slice(&v).map_err(XrpcError::from_err),
        None => Ok(TotpState::default()),
    }
}

pub async fn save(app: &App, did: &str, st: &TotpState) -> Result<(), XrpcError> {
    let val = if st.secret.is_none() && st.pending.is_none() {
        None
    } else {
        Some(Bytes::from(
            serde_json::to_vec(st).map_err(XrpcError::from_err)?,
        ))
    };
    app.put_private(
        did,
        vec![crate::segment::Mutation {
            key: state::private_key(did, PRIVATE_NAME).into(),
            val,
        }],
    )
    .await
}

/// Serializes read-modify-write of one account's TOTP state on this node.
static LOCKS: [tokio::sync::Mutex<()>; 32] = [const { tokio::sync::Mutex::const_new(()) }; 32];

pub async fn lock(did: &str) -> tokio::sync::MutexGuard<'static, ()> {
    LOCKS[(state::did_hash(did) % LOCKS.len() as u64) as usize]
        .lock()
        .await
}

fn factor_required() -> XrpcError {
    XrpcError {
        status: StatusCode::UNAUTHORIZED,
        error: "AuthFactorTokenRequired".into(),
        message: "A two-factor authentication code is required".into(),
    }
}

fn invalid_code() -> XrpcError {
    XrpcError::bad("InvalidToken", "Token is invalid")
}

/// Consumes `code` (a current TOTP code or an unused recovery code) against
/// the given state, updating it in memory. Caller persists.
pub fn consume(st: &mut TotpState, code: &str) -> Result<(), XrpcError> {
    let secret = st
        .secret
        .as_deref()
        .and_then(base32_decode)
        .ok_or_else(|| XrpcError::internal("corrupt TOTP secret"))?;
    let trimmed = code.trim();
    if trimmed.len() == DIGITS as usize && trimmed.bytes().all(|b| b.is_ascii_digit()) {
        let step =
            verify_code(&secret, trimmed, now_secs(), st.last_step).ok_or_else(invalid_code)?;
        st.last_step = step;
        return Ok(());
    }
    let h = hash_recovery_code(trimmed);
    let pos = st
        .recovery
        .iter()
        .position(|r| ct_eq(r.as_bytes(), h.as_bytes()))
        .ok_or_else(invalid_code)?;
    st.recovery.remove(pos);
    Ok(())
}

/// Second-factor check for a password login. Ok when the account has no TOTP
/// enabled; otherwise `code` must be a valid, unused TOTP code or recovery
/// code (401 AuthFactorTokenRequired when missing, 400 InvalidToken when wrong).
pub async fn check_second_factor(
    app: &App,
    account: &Account,
    code: Option<&str>,
) -> Result<(), XrpcError> {
    // cheap path: most accounts never enable TOTP
    if account.extra.get("totpEnabled").and_then(|v| v.as_bool()) == Some(false) {
        return Ok(());
    }
    let _g = lock(&account.did).await;
    let mut st = load(app, &account.did).await?;
    if !st.enabled() {
        return Ok(());
    }
    let code = code
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .ok_or_else(factor_required)?;
    consume(&mut st, code)?;
    save(app, &account.did, &st).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc6238_vectors() {
        // RFC 6238 appendix B (SHA-1 seed), truncated to 6 digits.
        let k = b"12345678901234567890";
        assert_eq!(code_for_step(k, step_at(59)), "287082");
        assert_eq!(code_for_step(k, step_at(1111111109)), "081804");
        assert_eq!(code_for_step(k, step_at(1234567890)), "005924");
        assert_eq!(code_for_step(k, step_at(2000000000)), "279037");
    }

    #[test]
    fn verify_window_and_replay() {
        let k = b"12345678901234567890";
        let now = 1111111109;
        let s = step_at(now);
        assert_eq!(verify_code(k, &code_for_step(k, s), now, 0), Some(s));
        assert_eq!(
            verify_code(k, &code_for_step(k, s - 1), now, 0),
            Some(s - 1)
        );
        assert_eq!(
            verify_code(k, &code_for_step(k, s + 1), now, 0),
            Some(s + 1)
        );
        assert_eq!(verify_code(k, &code_for_step(k, s + 2), now, 0), None);
        // replay: already accepted step s
        assert_eq!(verify_code(k, &code_for_step(k, s), now, s), None);
        assert_eq!(verify_code(k, "12345", now, 0), None);
    }

    #[test]
    fn base32_roundtrip_and_recovery() {
        let sec = generate_secret();
        let enc = base32_encode(&sec);
        assert_eq!(enc.len(), 32);
        assert_eq!(base32_decode(&enc).unwrap(), sec);
        let codes = generate_recovery_codes();
        assert_eq!(codes.len(), 10);
        let mut st = TotpState {
            secret: Some(enc),
            recovery: codes.iter().map(|c| hash_recovery_code(c)).collect(),
            ..Default::default()
        };
        assert!(consume(&mut st, &codes[3].to_uppercase()).is_ok());
        assert_eq!(st.recovery.len(), 9);
        assert!(consume(&mut st, &codes[3]).is_err());
    }
}
