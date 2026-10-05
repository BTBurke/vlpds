//! vlpds's WebAuthn relying party, compiled on its own: `src/webauthn.rs`
//! verbatim, with the two crate helpers it uses (constant-time compare,
//! base64url and the length-prefixed HMAC) copied from `src/auth.rs` and
//! `src/oauth/util.rs`. The tests drive it with passkey-rs.

#[path = "../../src/webauthn.rs"]
pub mod webauthn;

pub mod auth {
    /// `src/auth.rs`
    pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
        a.len() == b.len() && a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
    }
}

pub mod oauth {
    pub mod util {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
        use base64::Engine;
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        /// `src/oauth/util.rs`
        pub fn b64u(b: impl AsRef<[u8]>) -> String {
            B64.encode(b)
        }

        pub fn b64u_decode(s: &str) -> Option<Vec<u8>> {
            B64.decode(s.trim_end_matches('=')).ok()
        }

        pub fn hmac_sha256(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
            let mut m = Hmac::<Sha256>::new_from_slice(key).expect("hmac key");
            for p in parts {
                m.update(&(p.len() as u64).to_be_bytes());
                m.update(p);
            }
            m.finalize().into_bytes().into()
        }
    }
}
