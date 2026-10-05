//! Revoked space credentials, by (space, jti), until a time past which the
//! credential has expired anyway (the reference keeps them 3610 s). Every
//! credential check consults this set.

use std::collections::HashMap;

#[derive(Default)]
pub struct Revocations {
    /// (space, jti) -> until (Unix seconds).
    set: parking_lot::RwLock<HashMap<(String, String), i64>>,
}

impl Revocations {
    pub fn is_revoked(&self, space: &str, jti: &str, now: i64) -> bool {
        let g = self.set.read();
        if g.is_empty() {
            return false;
        }
        g.get(&(space.to_string(), jti.to_string())).is_some_and(|until| *until > now)
    }

    pub fn revoke(&self, space: &str, jti: &str, until: i64) {
        let mut g = self.set.write();
        let e = g.entry((space.to_string(), jti.to_string())).or_insert(until);
        *e = (*e).max(until);
    }

    pub fn prune(&self, now: i64) {
        self.set.write().retain(|_, until| *until > now);
    }

    pub fn len(&self) -> usize {
        self.set.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revoked_until() {
        let r = Revocations::default();
        assert!(!r.is_revoked("s", "j", 0));
        r.revoke("s", "j", 100);
        assert!(r.is_revoked("s", "j", 99));
        assert!(!r.is_revoked("s", "j", 100));
        assert!(!r.is_revoked("t", "j", 0));
        r.prune(100);
        assert!(r.is_empty());
    }
}
