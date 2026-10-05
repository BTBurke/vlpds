//! The few primitives `src/webauthn.rs` needs from the rest of the crate,
//! in one file so `passkey-differential/` compiles the same code (it
//! includes this file) and its copy can't drift. Re-exported where the
//! crate always had them: `auth::ct_eq`, `oauth::util::{b64u,
//! b64u_decode, hmac_sha256}`.

use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Constant time for equal lengths.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}

pub fn b64u(b: impl AsRef<[u8]>) -> String {
    B64.encode(b)
}

pub fn b64u_decode(s: &str) -> Option<Vec<u8>> {
    B64.decode(s.trim_end_matches('=')).ok()
}

/// Each part is length-prefixed, so no two part lists MAC alike.
pub fn hmac_sha256(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut m = Hmac::<Sha256>::new_from_slice(key).expect("hmac key");
    for p in parts {
        m.update(&(p.len() as u64).to_be_bytes());
        m.update(p);
    }
    m.finalize().into_bytes().into()
}
