//! Revoked space credentials, by (space, jti), until a time past which the
//! credential has expired anyway (the reference keeps them 3610 s: the
//! longest lifetime plus skew at both ends). Every credential check
//! consults this set.
//!
//! Cluster-wide, since a credential can read any repo hosted here: one
//! control object, `{prefix}/spaces/revocations.json`, appended with CAS on
//! its ETag and pruned at `until` as it is rewritten. It is written only
//! when something is revoked. Every node loads it before it serves a
//! credential, re-reads it every [`REFRESH_EVERY`] (a conditional GET),
//! and when the node that revoked nudges it.

use crate::store::Store;
use object_store::{GetOptions, ObjectStore, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// `SPACE_CREDENTIAL_MAX_AGE_SEC + 2 * CLOCK_SKEW_SEC` (reference
/// addRevokedSpaceCredentials).
pub const KEEP_SECS: i64 = super::token::CREDENTIAL_MAX_AGE_SECS + 2 * super::token::CLOCK_SKEW_SECS;
/// Peers are nudged on every revocation, so this only bounds staleness
/// after a lost nudge.
pub const REFRESH_EVERY: Duration = Duration::from_secs(300);
/// Until the first load succeeds (credential reads answer 503 meanwhile),
/// and after a failed re-read.
pub const RETRY_EVERY: Duration = Duration::from_secs(5);
const CALL_DEADLINE: Duration = Duration::from_secs(5);
const CAS_RETRIES: usize = 16;

/// The control object.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Doc {
    pub revoked: Vec<Entry>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub space: String,
    pub jti: String,
    /// Unix seconds.
    pub until: i64,
}

impl Doc {
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("serializable")
    }

    pub fn decode(b: &[u8]) -> anyhow::Result<Doc> {
        Ok(serde_json::from_slice(b)?)
    }

    fn prune(&mut self, now: i64) {
        self.revoked.retain(|e| e.until > now);
    }

    /// Adds or extends each (space, jti); false if nothing changed.
    fn upsert(&mut self, space: &str, jtis: &[String], until: i64) -> bool {
        let mut changed = false;
        for jti in jtis {
            match self.revoked.iter_mut().find(|e| e.space == space && e.jti == *jti) {
                Some(e) if e.until >= until => {}
                Some(e) => {
                    e.until = until;
                    changed = true;
                }
                None => {
                    self.revoked.push(Entry { space: space.into(), jti: jti.clone(), until });
                    changed = true;
                }
            }
        }
        changed
    }
}

pub fn path(store: &Store) -> object_store::path::Path {
    object_store::path::Path::from(format!("{}/spaces/revocations.json", store.prefix))
}

async fn bounded<T>(f: impl std::future::Future<Output = object_store::Result<T>>) -> object_store::Result<T> {
    match tokio::time::timeout(CALL_DEADLINE, f).await {
        Ok(r) => r,
        Err(_) => Err(object_store::Error::Generic { store: "revocations", source: "call timed out".into() }),
    }
}

/// (doc, etag); None when there is no object. With `etag`, NotModified if
/// the object is still that one.
async fn fetch(store: &Store, etag: Option<String>) -> object_store::Result<Option<(Doc, Option<String>)>> {
    let got = bounded(async {
        let r = store.raw.get_opts(&path(store), GetOptions { if_none_match: etag, ..Default::default() }).await?;
        let e = r.meta.e_tag.clone();
        Ok((r.bytes().await?, e))
    })
    .await;
    match got {
        Ok((b, e)) => {
            let doc = Doc::decode(&b)
                .map_err(|err| object_store::Error::Generic { store: "revocations", source: err.into() })?;
            Ok(Some((doc, e)))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e),
    }
}

#[derive(Default)]
pub struct Revocations {
    /// space -> jti -> until (Unix seconds).
    set: parking_lot::RwLock<HashMap<String, HashMap<String, i64>>>,
    /// Of the object installed last.
    etag: parking_lot::Mutex<Option<String>>,
    loaded: AtomicBool,
    /// Loads and appends on this node, in order: a slow load never installs
    /// an older object over a newer one.
    io: tokio::sync::Mutex<()>,
    pub(super) wake: std::sync::Arc<tokio::sync::Notify>,
    pub(super) started: AtomicBool,
}

impl Revocations {
    pub fn is_revoked(&self, space: &str, jti: &str, now: i64) -> bool {
        let g = self.set.read();
        if g.is_empty() {
            return false;
        }
        g.get(space).and_then(|j| j.get(jti)).is_some_and(|until| *until > now)
    }

    /// Whether the control object has been read since this node started.
    pub fn loaded(&self) -> bool {
        self.loaded.load(Ordering::Acquire)
    }

    pub fn len(&self) -> usize {
        self.set.read().values().map(HashMap::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Asks the background re-read to run now.
    pub fn wake(&self) {
        self.wake.notify_one();
    }

    /// Replaces the set with `doc`'s live entries; returns those that are
    /// new to this node.
    fn install(&self, doc: &Doc, etag: Option<String>, now: i64) -> Vec<(String, String)> {
        let mut next: HashMap<String, HashMap<String, i64>> = HashMap::new();
        for e in doc.revoked.iter().filter(|e| e.until > now) {
            let u = next.entry(e.space.clone()).or_default().entry(e.jti.clone()).or_insert(e.until);
            *u = (*u).max(e.until);
        }
        let mut g = self.set.write();
        let added = next
            .iter()
            .flat_map(|(s, js)| js.keys().map(move |j| (s, j)))
            .filter(|(s, j)| !g.get(*s).is_some_and(|m| m.contains_key(*j)))
            .map(|(s, j)| (s.clone(), j.clone()))
            .collect();
        *g = next;
        let n = g.values().map(HashMap::len).sum::<usize>();
        drop(g);
        *self.etag.lock() = etag;
        self.loaded.store(true, Ordering::Release);
        crate::metrics::space_revocations(n);
        added
    }

    /// Re-reads the object (conditional on the last ETag). Returns the
    /// revocations new to this node.
    pub async fn refresh(&self, store: &Store, now: i64) -> anyhow::Result<Vec<(String, String)>> {
        let _io = self.io.lock().await;
        let seen = if self.loaded() { self.etag.lock().clone() } else { None };
        match fetch(store, seen).await {
            Ok(Some((doc, etag))) => Ok(self.install(&doc, etag, now)),
            Ok(None) => Ok(self.install(&Doc::default(), None, now)),
            Err(object_store::Error::NotModified { .. }) => {
                // entries past `until` leave even when nothing was written
                self.set.write().retain(|_, js| {
                    js.retain(|_, until| *until > now);
                    !js.is_empty()
                });
                crate::metrics::space_revocations(self.len());
                Ok(Vec::new())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Appends `jtis` of `space` to the object (CAS), pruning expired
    /// entries as it goes, and installs the result here. Durable when this
    /// returns. Returns the revocations new to this node.
    pub async fn revoke(
        &self,
        store: &Store,
        space: &str,
        jtis: &[String],
        now: i64,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let _io = self.io.lock().await;
        let until = now + KEEP_SECS;
        for _ in 0..CAS_RETRIES {
            let (mut doc, etag) = fetch(store, None).await?.unwrap_or_default();
            let before = doc.revoked.len();
            doc.prune(now);
            if !doc.upsert(space, jtis, until) && doc.revoked.len() == before {
                return Ok(self.install(&doc, etag, now));
            }
            let mode = match &etag {
                Some(e) => crate::cluster::if_match(Some(e.clone())),
                None => PutMode::Create,
            };
            let opts = PutOptions { mode, ..Default::default() };
            match bounded(store.raw.put_opts(&path(store), PutPayload::from(doc.encode()), opts)).await {
                Ok(r) => return Ok(self.install(&doc, r.e_tag, now)),
                // another node appended first (S3 answers an If-Match PUT
                // of a key deleted meanwhile 404)
                Err(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. }
                    | object_store::Error::NotFound { .. },
                ) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        anyhow::bail!("revocations: too much contention, try again")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn store() -> Store {
        Store { raw: Arc::new(object_store::memory::InMemory::new()), prefix: "t".into(), latency: None }
    }

    #[tokio::test]
    async fn append_reload_and_prune() {
        let s = store();
        let (a, b) = (Revocations::default(), Revocations::default());
        assert!(!a.loaded());
        assert!(a.refresh(&s, 0).await.unwrap().is_empty());
        assert!(a.loaded());
        let added = a.revoke(&s, "sp", &["1".into(), "2".into()], 100).await.unwrap();
        assert_eq!(added.len(), 2);
        assert!(a.is_revoked("sp", "1", 100));
        assert!(!a.is_revoked("other", "1", 100), "scoped to its space");
        // idempotent: nothing new, no write
        assert!(a.revoke(&s, "sp", &["1".into()], 100).await.unwrap().is_empty());
        // another node: a concurrent append isn't lost
        b.revoke(&s, "sp", &["3".into()], 100).await.unwrap();
        let added = a.refresh(&s, 100).await.unwrap();
        assert_eq!(added, vec![("sp".to_string(), "3".to_string())]);
        assert_eq!(a.len(), 3);
        // unchanged object: a conditional re-read
        assert!(a.refresh(&s, 100).await.unwrap().is_empty());
        // past `until`: gone from the set, and pruned at the next append
        let later = 100 + KEEP_SECS;
        assert!(!a.is_revoked("sp", "1", later));
        a.refresh(&s, later).await.unwrap();
        assert!(a.is_empty());
        a.revoke(&s, "sp", &["4".into()], later).await.unwrap();
        let (doc, _) = fetch(&s, None).await.unwrap().unwrap();
        assert_eq!(doc.revoked.iter().map(|e| e.jti.as_str()).collect::<Vec<_>>(), ["4"]);
    }
}
