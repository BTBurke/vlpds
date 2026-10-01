//! Small helpers shared by the OAuth modules: base64url, random ids, URL
//! component encoding (matching JS `encodeURIComponent` / `URLSearchParams`),
//! form parsing, HTML escaping and server-secret key derivation.

use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

pub fn now_secs() -> i64 {
    (crate::tid::now_micros() / 1_000_000) as i64
}

pub fn b64u(b: impl AsRef<[u8]>) -> String {
    B64.encode(b)
}

pub fn b64u_decode(s: &str) -> Option<Vec<u8>> {
    B64.decode(s.trim_end_matches('=')).ok()
}

pub fn sha256(b: impl AsRef<[u8]>) -> [u8; 32] {
    Sha256::digest(b.as_ref()).into()
}

pub fn sha256_b64u(b: impl AsRef<[u8]>) -> String {
    b64u(sha256(b))
}

/// `{prefix}{base64url(n random bytes)}`
pub fn random_id(prefix: &str, n: usize) -> String {
    let mut b = vec![0u8; n];
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut b);
    format!("{prefix}{}", b64u(b))
}

pub fn hmac_sha256(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut m = Hmac::<Sha256>::new_from_slice(key).expect("hmac key");
    for p in parts {
        m.update(&(p.len() as u64).to_be_bytes());
        m.update(p);
    }
    m.finalize().into_bytes().into()
}

/// Constant-time equality for secrets.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle_eq::ConstantTimeEq;
    a.len() == b.len() && a.ct_eq(b)
}

mod subtle_eq {
    pub trait ConstantTimeEq {
        fn ct_eq(&self, other: &Self) -> bool;
    }
    impl ConstantTimeEq for [u8] {
        fn ct_eq(&self, other: &[u8]) -> bool {
            let mut d = 0u8;
            for (x, y) in self.iter().zip(other) {
                d |= x ^ y;
            }
            d == 0
        }
    }
}

/// Derives a purpose-specific 32-byte secret from the server's configured
/// secret. Every node of a deployment shares `jwt_secret`, so derived keys
/// (access-token signing key, DPoP nonce secret, CSRF key, refresh-token MAC
/// key) agree across nodes without any stored state.
pub fn derive_secret(server_secret: &str, label: &str) -> [u8; 32] {
    hmac_sha256(
        server_secret.as_bytes(),
        &[b"vlpds-oauth-v1", label.as_bytes()],
    )
}

// ---------- URL component encoding ----------

fn hex_upper(b: u8) -> [u8; 3] {
    const H: &[u8; 16] = b"0123456789ABCDEF";
    [b'%', H[(b >> 4) as usize], H[(b & 15) as usize]]
}

/// JS `encodeURIComponent`.
pub fn encode_uri_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(std::str::from_utf8(&hex_upper(b)).unwrap());
        }
    }
    out
}

/// application/x-www-form-urlencoded serialization of one component (as
/// `URLSearchParams.toString()` does: space -> '+', `*-._` and alnum kept).
pub fn form_encode_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || b"*-._".contains(&b) {
            out.push(b as char);
        } else if b == b' ' {
            out.push('+');
        } else {
            out.push_str(std::str::from_utf8(&hex_upper(b)).unwrap());
        }
    }
    out
}

pub fn form_encode(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", form_encode_component(k), form_encode_component(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Strict percent-decoding (JS `decodeURIComponent`); None on malformed input.
pub fn percent_decode_strict(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let h = std::str::from_utf8(b.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(h, 16).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Lenient form-component decoding ('+' -> space, bad escapes kept verbatim),
/// like the WHATWG urlencoded parser.
pub fn form_decode_component(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 3 <= b.len() => {
                match std::str::from_utf8(&b[i + 1..i + 3])
                    .ok()
                    .and_then(|h| u8::from_str_radix(h, 16).ok())
                {
                    Some(v) => {
                        out.push(v);
                        i += 3;
                    }
                    None => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Parses an urlencoded string into ordered pairs (WHATWG semantics).
pub fn parse_form(s: &str) -> Vec<(String, String)> {
    s.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (form_decode_component(k), form_decode_component(v)),
            None => (form_decode_component(p), String::new()),
        })
        .collect()
}

pub fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

/// A single-use value (DPoP proof / client assertion / request object
/// `jti`) to claim until `until` (unix secs). The claim is made at the owner
/// of `routing`'s partition (`xrpc::internal::claim_replay_anywhere`), so
/// every node checks a given key against the same set (HA notes in mod.rs).
#[derive(Clone, Debug)]
pub struct Replay {
    pub routing: String,
    pub key: String,
    pub until: i64,
}

/// Routing key of a client's single-use values (assertion / JAR `jti`s).
pub fn client_routing(client_id: &str) -> String {
    format!("oauth:client:{}", sha256_b64u(client_id))
}

/// Routing key of a DPoP key's proofs at the authorization server.
pub fn jkt_routing(jkt: &str) -> String {
    format!("oauth:jkt:{jkt}")
}

/// Per-node OAuth state: the replay set of the routing keys this node owns
/// and the locks for single-use read-modify-writes. Per `App` (not per
/// process), so in-process test clusters behave like separate machines.
pub(crate) struct NodeState {
    replays: ReplayCache,
    pub(crate) locks: Vec<std::sync::Arc<tokio::sync::Mutex<()>>>,
}

static NODES: parking_lot::RwLock<Vec<(usize, std::sync::Arc<NodeState>)>> =
    parking_lot::RwLock::new(Vec::new());

pub(crate) fn node_state(app: &crate::xrpc::App) -> std::sync::Arc<NodeState> {
    let id = app as *const crate::xrpc::App as usize;
    if let Some((_, n)) = NODES.read().iter().find(|(k, _)| *k == id) {
        return n.clone();
    }
    let mut w = NODES.write();
    if let Some((_, n)) = w.iter().find(|(k, _)| *k == id) {
        return n.clone();
    }
    let n = std::sync::Arc::new(NodeState {
        replays: ReplayCache::new(2_000_000),
        locks: (0..256).map(|_| std::sync::Arc::new(tokio::sync::Mutex::new(()))).collect(),
    });
    w.push((id, n.clone()));
    n
}

/// Private-row name prefix of persisted single-use claims
/// (`p/{routing}\0oauth/replay/{sha256(key)}` -> `until`, JSON).
pub const REPLAY_ROW: &str = "oauth/replay/";

fn replay_row(key: &str) -> String {
    format!("{REPLAY_ROW}{}", sha256_b64u(key))
}

/// Claims `key` (single use until `until`, unix secs) at this node, which
/// owns `routing`'s partition. False = already claimed (a replay).
///
/// The in-memory set is the fast path and settles concurrent claims here.
/// A `durable` claim is also written to the partition (and awaited) before
/// it counts, and a claim missing from memory is checked against the
/// partition first, so a new owner after a failover (empty set) still sees
/// the claims its predecessor accepted. Expired rows are removed by the
/// OAuth GC (`gc.rs`). Transient claims (a guard released right after,
/// with a durable record of its own) skip both.
pub async fn claim_replay_owned(
    app: &crate::xrpc::App,
    routing: &str,
    key: &str,
    until: i64,
    durable: bool,
) -> Result<bool, crate::xrpc::XrpcError> {
    if !node_state(app).replays.insert_unique(key, until) {
        return Ok(false);
    }
    if !durable {
        return Ok(true);
    }
    let name = replay_row(key);
    if let Some(v) = app.get_private(routing, &name).await? {
        let prev: i64 = serde_json::from_slice(&v).unwrap_or(i64::MAX);
        if prev > now_secs() {
            return Ok(false);
        }
    }
    let m = crate::segment::Mutation {
        key: crate::state::private_key(routing, &name).into(),
        val: Some(serde_json::to_vec(&until).unwrap().into()),
    };
    app.put_private(routing, vec![m]).await?;
    Ok(true)
}

/// Forgets the in-memory claims of this node (tests: what a node that just
/// took over a partition starts with).
pub fn forget_replays(app: &crate::xrpc::App) {
    node_state(app).replays.inner.lock().0.clear();
}

/// Releases a transient claim made with [`claim_replay_owned`] (a guard
/// whose durable record is now written).
pub fn release_replay_local(app: &crate::xrpc::App, key: &str) {
    node_state(app).replays.inner.lock().0.remove(key);
}

/// Drops expired replay keys (OAuth GC task). Returns how many were removed.
pub fn sweep_replays(app: &crate::xrpc::App) -> usize {
    node_state(app).replays.sweep()
}

/// Simple TTL set used for replay detection (DPoP proof `jti`s, client
/// assertion `jti`s): [`claim_replay_owned`].
pub struct ReplayCache {
    inner: parking_lot::Mutex<(std::collections::HashMap<String, i64>, i64)>,
    max: usize,
}

impl ReplayCache {
    pub fn new(max: usize) -> ReplayCache {
        ReplayCache {
            inner: parking_lot::Mutex::new((std::collections::HashMap::new(), 0)),
            max,
        }
    }

    /// Drops expired entries (also done lazily by `insert_unique`; the OAuth
    /// GC task calls this so an idle cache does not hold its peak size).
    /// Returns how many were removed.
    pub fn sweep(&self) -> usize {
        let now = now_secs();
        let mut g = self.inner.lock();
        let (map, last_sweep) = &mut *g;
        let before = map.len();
        map.retain(|_, exp| *exp > now);
        *last_sweep = now;
        before - map.len()
    }

    /// Records `key` until `expires_at` (unix secs). Returns false if it was
    /// already present (a replay).
    pub fn insert_unique(&self, key: &str, expires_at: i64) -> bool {
        let now = now_secs();
        let mut g = self.inner.lock();
        let (map, last_sweep) = &mut *g;
        if now - *last_sweep > 30 || map.len() >= self.max {
            map.retain(|_, exp| *exp > now);
            *last_sweep = now;
            if map.len() >= self.max {
                // Over capacity even after expiry: refuse rather than forget
                // entries (forgetting would re-open the replay window).
                return false;
            }
        }
        match map.get(key) {
            Some(exp) if *exp > now => false,
            _ => {
                map.insert(key.to_string(), expires_at);
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding() {
        assert_eq!(encode_uri_component("a b/c:d?e"), "a%20b%2Fc%3Ad%3Fe");
        assert_eq!(form_encode_component("a b/c*"), "a+b%2Fc*");
        assert_eq!(percent_decode_strict("a%2Fb"), Some("a/b".into()));
        assert_eq!(percent_decode_strict("a%2"), None);
        assert_eq!(
            parse_form("a=1&b=x+y&a=%2F&c"),
            vec![
                ("a".into(), "1".into()),
                ("b".into(), "x y".into()),
                ("a".into(), "/".into()),
                ("c".into(), "".into())
            ]
        );
    }

    #[test]
    fn replay() {
        let c = ReplayCache::new(10);
        let exp = now_secs() + 60;
        assert!(c.insert_unique("x", exp));
        assert!(!c.insert_unique("x", exp));
        assert!(c.insert_unique("y", exp));
    }
}
