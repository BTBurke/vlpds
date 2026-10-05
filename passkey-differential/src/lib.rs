//! vlpds's WebAuthn relying party, compiled on its own: `src/webauthn.rs`
//! and the primitives it uses (`src/prims.rs`), both verbatim, at the paths
//! the crate has them. The tests drive it with passkey-rs.

#[path = "../../src/prims.rs"]
pub mod prims;
#[path = "../../src/webauthn.rs"]
pub mod webauthn;

pub mod auth {
    pub use crate::prims::ct_eq;
}

pub mod oauth {
    pub mod util {
        pub use crate::prims::{b64u, b64u_decode, hmac_sha256};
    }
}
