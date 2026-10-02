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

/// Longest a single-use claim is kept (unix secs from now). Every claim's
/// own window is shorter (DPoP proofs: `iat` within 10 s + 180 s skew
/// either way; client assertions: `iat` + 60 s + 10 s; request objects:
/// `iat` + 59 s + 10 s), so this only bounds what a caller (or a peer, over
/// the internal endpoint) passes: no claim, in memory or persisted, can
/// outlive it, whatever `exp` a client put in its JWT.
pub const MAX_CLAIM_TTL: i64 = 600;

/// What a single-use claim is: each kind has its own replay cache, so a
/// flood of one (resource-request proofs) can't evict another's
/// (authorization-server proofs, client assertions, request objects).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimKind {
    /// DPoP proofs of resource requests (memory only).
    ResourceProof = 0,
    /// DPoP proofs at the authorization server (PAR, token).
    AsProof = 1,
    /// Client assertion `jti`s.
    Assertion = 2,
    /// Request object (JAR) `jti`s.
    RequestObject = 3,
    /// Short guards released right after (PKCE code_challenge claims).
    Guard = 4,
}

impl ClaimKind {
    const ALL: [ClaimKind; 5] =
        [ClaimKind::ResourceProof, ClaimKind::AsProof, ClaimKind::Assertion, ClaimKind::RequestObject, ClaimKind::Guard];

    /// The kind of `key` (by the prefix its maker gives it: jose.rs,
    /// client.rs, store.rs); `durable`: persisted (AS) or memory-only.
    pub fn of(key: &str, durable: bool) -> ClaimKind {
        if key.starts_with("dpop:") {
            if durable {
                ClaimKind::AsProof
            } else {
                ClaimKind::ResourceProof
            }
        } else if key.starts_with("assert:") {
            ClaimKind::Assertion
        } else if key.starts_with("jar:") {
            ClaimKind::RequestObject
        } else {
            ClaimKind::Guard
        }
    }

    /// (entries, entries per routing key) of this kind's cache.
    fn caps(self) -> (usize, usize) {
        match self {
            ClaimKind::ResourceProof => (2_000_000, 50_000),
            ClaimKind::AsProof | ClaimKind::Assertion | ClaimKind::RequestObject => (500_000, 50_000),
            ClaimKind::Guard => (100_000, 1_000),
        }
    }
}

/// Per-node OAuth state: the replay sets of the routing keys this node owns
/// and the locks for single-use read-modify-writes. Per `App` (not per
/// process), so in-process test clusters behave like separate machines.
pub(crate) struct NodeState {
    replays: [ReplayCache; 5],
    pub(crate) locks: Vec<std::sync::Arc<tokio::sync::Mutex<()>>>,
}

impl NodeState {
    fn replays(&self, kind: ClaimKind) -> &ReplayCache {
        &self.replays[kind as usize]
    }
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
        replays: ClaimKind::ALL.map(|k| {
            let (max, per_group) = k.caps();
            ReplayCache::new(max, per_group)
        }),
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

/// Claims `key` (single use until `until`, unix secs, capped at
/// [`MAX_CLAIM_TTL`] from now) at this node, which owns `routing`'s
/// partition. False = already claimed (a replay).
///
/// The in-memory set is the fast path and settles concurrent claims here.
/// A `durable` claim is also written to the partition (and awaited) before
/// it counts, and a claim missing from memory is checked against the
/// partition first, so a new owner after a failover (empty set), or this
/// node after evicting it from a full cache, still sees the claims accepted
/// before. Expired rows are removed by the OAuth GC (`gc.rs`). Transient
/// claims (a guard released right after, with a durable record of its own)
/// skip both.
pub async fn claim_replay_owned(
    app: &crate::xrpc::App,
    routing: &str,
    key: &str,
    until: i64,
    durable: bool,
) -> Result<bool, crate::xrpc::XrpcError> {
    let until = until.min(now_secs() + MAX_CLAIM_TTL);
    if !node_state(app).replays(ClaimKind::of(key, durable)).insert_unique(routing, key, until) {
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
    for c in &node_state(app).replays {
        c.clear();
    }
}

/// Releases a transient claim made with [`claim_replay_owned`] (a guard
/// whose durable record is now written).
pub fn release_replay_local(app: &crate::xrpc::App, key: &str) {
    node_state(app).replays(ClaimKind::of(key, false)).remove(key);
}

/// Drops expired replay keys (OAuth GC task). Returns how many were removed.
pub fn sweep_replays(app: &crate::xrpc::App) -> usize {
    node_state(app).replays.iter().map(|c| c.sweep()).sum()
}

/// Entries in this node's replay cache of `kind` (tests, metrics).
pub fn replay_entries(app: &crate::xrpc::App, kind: ClaimKind) -> usize {
    node_state(app).replays(kind).len()
}

/// TTL set used for replay detection ([`claim_replay_owned`]), bounded in
/// total and per routing key (a DID, a client, a DPoP key).
///
/// Full, it evicts the entry closest to expiry (of the routing key over its
/// cap, else of the whole set) instead of refusing every new claim, which
/// would let one client flooding it lock every other client out. Evicting
/// is safe for persisted claims (the partition row still refuses a replay)
/// and, for memory-only resource-request proofs, only affects a routing key
/// that exceeded its own cap (or a set full across many keys): a proof
/// evicted then could be replayed for what is left of its short window,
/// and only with the access token it is bound to.
pub struct ReplayCache {
    inner: parking_lot::Mutex<ReplayInner>,
    max: usize,
    max_per_group: usize,
}

#[derive(Default)]
struct ReplayInner {
    /// key -> (until, seq, group)
    map: std::collections::HashMap<String, (i64, u64, std::sync::Arc<str>)>,
    /// (until, seq) -> key: expiry order
    order: std::collections::BTreeMap<(i64, u64), String>,
    /// group -> its entries' (until, seq)
    groups: std::collections::HashMap<std::sync::Arc<str>, std::collections::BTreeSet<(i64, u64)>>,
    seq: u64,
}

impl ReplayInner {
    fn remove_at(&mut self, at: (i64, u64)) {
        if let Some(key) = self.order.remove(&at) {
            if let Some((_, _, g)) = self.map.remove(&key) {
                if let Some(set) = self.groups.get_mut(&g) {
                    set.remove(&at);
                    if set.is_empty() {
                        self.groups.remove(&g);
                    }
                }
            }
        }
    }

    /// Drops every entry expired at `now`; returns how many.
    fn expire(&mut self, now: i64) -> usize {
        let mut n = 0;
        while let Some((&at, _)) = self.order.first_key_value() {
            if at.0 > now {
                break;
            }
            self.remove_at(at);
            n += 1;
        }
        n
    }
}

impl ReplayCache {
    pub fn new(max: usize, max_per_group: usize) -> ReplayCache {
        ReplayCache { inner: parking_lot::Mutex::new(ReplayInner::default()), max: max.max(1), max_per_group: max_per_group.max(1) }
    }

    /// Drops expired entries (also done on every insert; the OAuth GC task
    /// calls this so an idle cache does not hold its peak size). Returns
    /// how many were removed.
    pub fn sweep(&self) -> usize {
        self.inner.lock().expire(now_secs())
    }

    pub fn len(&self) -> usize {
        self.inner.lock().map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn clear(&self) {
        *self.inner.lock() = ReplayInner::default();
    }

    fn remove(&self, key: &str) {
        let mut g = self.inner.lock();
        if let Some(&(until, seq, _)) = g.map.get(key) {
            g.remove_at((until, seq));
        }
    }

    /// Records `key` of routing key `group` until `expires_at` (unix secs).
    /// Returns false if it was already present (a replay). Never refuses a
    /// new key: a full set evicts (type docs).
    pub fn insert_unique(&self, group: &str, key: &str, expires_at: i64) -> bool {
        let now = now_secs();
        let mut g = self.inner.lock();
        g.expire(now);
        if g.map.contains_key(key) {
            return false;
        }
        if expires_at <= now {
            // nothing to remember: it can't be presented again in time
            return true;
        }
        g.seq += 1;
        let at = (expires_at, g.seq);
        let group: std::sync::Arc<str> = match g.groups.get_key_value(group) {
            Some((k, _)) => k.clone(),
            None => group.into(),
        };
        g.map.insert(key.to_string(), (expires_at, at.1, group.clone()));
        g.order.insert(at, key.to_string());
        let over = {
            let set = g.groups.entry(group).or_default();
            set.insert(at);
            (set.len() > self.max_per_group).then(|| *set.first().expect("non-empty"))
        };
        if let Some(oldest) = over {
            g.remove_at(oldest);
        }
        while g.map.len() > self.max {
            let oldest = *g.order.first_key_value().expect("non-empty").0;
            g.remove_at(oldest);
        }
        true
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
        let c = ReplayCache::new(10, 10);
        let exp = now_secs() + 60;
        assert!(c.insert_unique("g", "x", exp));
        assert!(!c.insert_unique("g", "x", exp));
        assert!(c.insert_unique("g", "y", exp));
        c.remove("x");
        assert!(c.insert_unique("g", "x", exp), "released");
        // already expired: accepted, not kept
        assert!(c.insert_unique("g", "old", now_secs() - 1));
        assert_eq!(c.len(), 2);
    }

    /// A full cache evicts (the entry closest to expiry) instead of refusing
    /// new claims; one routing key over its cap evicts only its own entries.
    #[test]
    fn replay_cache_full_evicts() {
        let now = now_secs();
        let c = ReplayCache::new(100, 10);
        // another client's claims, expiring late
        for i in 0..5 {
            assert!(c.insert_unique("victim", &format!("v{i}"), now + 300));
        }
        // a flood from one routing key: never refused, capped at 10 of its own
        for i in 0..1_000 {
            assert!(c.insert_unique("flood", &format!("f{i}"), now + 60 + i), "claim {i} refused");
        }
        assert_eq!(c.len(), 15);
        for i in 0..5 {
            assert!(!c.insert_unique("victim", &format!("v{i}"), now + 300), "victim claim {i} evicted by another key's flood");
        }
        // the newest of the flood are still claimed
        assert!(!c.insert_unique("flood", "f999", now + 60));
        // a flood across many keys fills the whole set: still no refusal,
        // the soonest-expiring entries go first
        for i in 0..1_000 {
            assert!(c.insert_unique(&format!("k{i}"), &format!("m{i}"), now + 400 + i));
        }
        assert_eq!(c.len(), 100);
        assert!(!c.insert_unique("k999", "m999", now + 400));
    }
}
