//! Verified space credentials, by sha256 of the token, until they expire:
//! a cached credential costs a hash lookup instead of a key resolution and
//! an ES256K verify. A hit is still checked against the revocation set,
//! and the request's own HTTP signature (by the cached `cnf.kid`, over its
//! own audience) is verified every time.
//!
//! A credential stays cached until `exp` (an hour at most) even if its
//! authority rotates its key meanwhile. The reference resolves the key per
//! request; the cache trades that for the hot path (DESIGN.md "Spaces").

use sha2::{Digest, Sha256};
use std::num::NonZeroUsize;
use std::sync::Arc;

pub const DEFAULT_ENTRIES: usize = 50_000;

/// What a verified credential says, minus the request it came with.
#[derive(Debug)]
pub struct Verified {
    pub space: String,
    pub iss: String,
    pub jti: String,
    pub exp: f64,
    pub cnf_kid: String,
}

pub type Key = [u8; 32];

pub fn key(token: &str) -> Key {
    Sha256::digest(token.as_bytes()).into()
}

pub struct CredCache {
    map: parking_lot::Mutex<lru::LruCache<Key, Arc<Verified>>>,
}

impl CredCache {
    pub fn new(entries: usize) -> CredCache {
        let cap = NonZeroUsize::new(entries.max(1)).expect("non-zero");
        CredCache { map: parking_lot::Mutex::new(lru::LruCache::new(cap)) }
    }

    /// The entry, unless expired as `token::SpaceToken::check` counts it
    /// (an expired one is dropped).
    pub fn get(&self, k: &Key, now: i64) -> Option<Arc<Verified>> {
        let mut g = self.map.lock();
        let v = g.get(k)?.clone();
        if (now - super::token::CLOCK_SKEW_SECS) as f64 >= v.exp {
            g.pop(k);
            return None;
        }
        Some(v)
    }

    pub fn insert(&self, k: Key, v: Arc<Verified>) {
        self.map.lock().put(k, v);
    }

    /// Drops the entries of revoked credentials. Rare (a revocation), so a
    /// scan rather than a second index.
    pub fn invalidate(&self, revoked: &[(String, String)]) {
        if revoked.is_empty() {
            return;
        }
        let mut g = self.map.lock();
        let gone: Vec<Key> = g
            .iter()
            .filter(|(_, v)| revoked.iter().any(|(s, j)| *s == v.space && *j == v.jti))
            .map(|(k, _)| *k)
            .collect();
        for k in gone {
            g.pop(&k);
        }
    }

    pub fn len(&self) -> usize {
        self.map.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(space: &str, jti: &str, exp: f64) -> Arc<Verified> {
        Arc::new(Verified { space: space.into(), iss: "did:a".into(), jti: jti.into(), exp, cnf_kid: "k".into() })
    }

    #[test]
    fn expiry_bound_and_invalidation() {
        let c = CredCache::new(2);
        c.insert(key("a"), v("s", "1", 100.0));
        assert!(c.get(&key("a"), 90).is_some());
        // expired as the token check counts it (5 s skew): dropped
        assert!(c.get(&key("a"), 105).is_none());
        assert!(c.is_empty());
        c.insert(key("a"), v("s", "1", 100.0));
        c.insert(key("b"), v("s", "2", 100.0));
        c.insert(key("c"), v("t", "1", 100.0));
        assert_eq!(c.len(), 2, "bounded");
        assert!(c.get(&key("a"), 0).is_none(), "least recently used goes");
        c.invalidate(&[("s".into(), "1".into()), ("s".into(), "2".into())]);
        assert!(c.get(&key("b"), 0).is_none());
        assert!(c.get(&key("c"), 0).is_some(), "another space's jti stays");
    }
}
