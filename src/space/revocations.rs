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
/// Past this since the last good read (a re-read failing, a nudge lost and
/// the next re-read failing too), the set may lack a revocation: credential
/// reads answer 503 until a read succeeds.
pub const STALE_AFTER: Duration = Duration::from_secs(REFRESH_EVERY.as_secs() + 60);
const CALL_DEADLINE: Duration = Duration::from_secs(5);
const CAS_RETRIES: usize = 16;
/// The reference's jtis are 32 hex characters; a credential with a longer
/// one is refused, so every credential accepted here can be revoked.
pub const MAX_JTI_LEN: usize = 128;
/// Live entries any notifier may add up to. The object is read whole by
/// every node, so it stays small: past this, only revocations of a space
/// with a stake here (an account here holds a repo in it, or governs it)
/// are taken, up to [`HARD_CAP`].
pub const SOFT_CAP: usize = 10_000;
pub const HARD_CAP: usize = 50_000;
/// Live entries of one authority's spaces.
pub const PER_AUTHORITY: usize = 2_000;
/// Spaces whose revocation couldn't be stored, refused on this node.
const MAX_BLOCKED: usize = 10_000;

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

    /// Adds each (space, jti) not held yet (one already held outlives every
    /// credential it can name); false if nothing was added.
    fn add(&mut self, space: &str, jtis: &[String], until: i64) -> bool {
        let held: std::collections::HashSet<&str> =
            self.revoked.iter().filter(|e| e.space == space).map(|e| e.jti.as_str()).collect();
        let new: Vec<Entry> = jtis
            .iter()
            .filter(|j| !held.contains(j.as_str()))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(|j| Entry { space: space.into(), jti: j.clone(), until })
            .collect();
        drop(held);
        let added = !new.is_empty();
        self.revoked.extend(new);
        added
    }

    fn of_authority(&self, authority: &str) -> usize {
        self.revoked.iter().filter(|e| authority_of(&e.space) == Some(authority)).count()
    }
}

/// A space URI's authority.
pub fn authority_of(space: &str) -> Option<&str> {
    space.strip_prefix("at://")?.split('/').next()
}

/// A jti a revocation may name: 1 to [`MAX_JTI_LEN`] printable ASCII
/// characters.
pub fn valid_jti(jti: &str) -> bool {
    (1..=MAX_JTI_LEN).contains(&jti.len()) && jti.bytes().all(|b| b.is_ascii_graphic())
}

/// Why a revocation wasn't stored. Either way the space is refused on this
/// node and its peers until it would have expired ([`Revocations::block`]).
#[derive(Debug, PartialEq, Eq)]
pub enum Refused {
    /// The authority has [`PER_AUTHORITY`] live entries.
    Authority,
    /// The object is at its cap for this kind of revocation.
    Full,
}

#[derive(Debug, Default)]
pub struct Revoked {
    /// Revocations new to this node.
    pub added: Vec<(String, String)>,
    /// The object was written (peers have something to re-read).
    pub wrote: bool,
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

/// [`SOFT_CAP`], [`HARD_CAP`] and [`PER_AUTHORITY`]; tests lower them.
struct Caps {
    soft: std::sync::atomic::AtomicUsize,
    hard: std::sync::atomic::AtomicUsize,
    authority: std::sync::atomic::AtomicUsize,
}

impl Default for Caps {
    fn default() -> Caps {
        Caps { soft: SOFT_CAP.into(), hard: HARD_CAP.into(), authority: PER_AUTHORITY.into() }
    }
}

#[derive(Default)]
pub struct Revocations {
    /// space -> jti -> until (Unix seconds).
    set: parking_lot::RwLock<HashMap<String, HashMap<String, i64>>>,
    /// Of the object installed last.
    etag: parking_lot::Mutex<Option<String>>,
    loaded: AtomicBool,
    /// When a read of the object last succeeded (unix µs).
    last_ok: std::sync::atomic::AtomicU64,
    /// space -> until (Unix seconds): spaces whose revocation couldn't be
    /// stored. Fails closed: their credentials are refused here meanwhile.
    blocked: parking_lot::RwLock<HashMap<String, i64>>,
    caps: Caps,
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

    /// Loaded, and read within [`STALE_AFTER`].
    pub fn fresh(&self) -> bool {
        let age = crate::tid::now_micros().saturating_sub(self.last_ok.load(Ordering::Acquire));
        self.loaded() && age < STALE_AFTER.as_micros() as u64
    }

    #[doc(hidden)]
    pub fn set_caps(&self, soft: usize, hard: usize, authority: usize) {
        self.caps.soft.store(soft, Ordering::Relaxed);
        self.caps.hard.store(hard, Ordering::Relaxed);
        self.caps.authority.store(authority, Ordering::Relaxed);
    }

    /// Makes the last good read `by` older, as a run of failed re-reads would.
    #[doc(hidden)]
    pub fn age_last_read(&self, by: Duration) {
        self.last_ok.fetch_sub(by.as_micros() as u64, Ordering::AcqRel);
    }

    fn read_ok(&self) {
        self.last_ok.store(crate::tid::now_micros(), Ordering::Release);
    }

    pub fn is_blocked(&self, space: &str, now: i64) -> bool {
        let g = self.blocked.read();
        !g.is_empty() && g.get(space).is_some_and(|until| *until > now)
    }

    /// Refuses `space`'s credentials here for as long as a revocation of it
    /// would have lasted.
    pub fn block(&self, space: &str, now: i64) {
        let mut g = self.blocked.write();
        g.retain(|_, until| *until > now);
        if g.len() < MAX_BLOCKED || g.contains_key(space) {
            g.insert(space.to_string(), now + KEEP_SECS);
        } else {
            tracing::warn!("space revocation blocks full; not blocking another space");
        }
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
        self.read_ok();
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
                self.read_ok();
                crate::metrics::space_revocations(self.len());
                Ok(Vec::new())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Appends `jtis` of `space` to the object (CAS), pruning expired
    /// entries as it goes, and installs the result here. Durable when this
    /// returns Ok(Ok). `staked`: an account here holds a repo in the space
    /// or governs it (see [`SOFT_CAP`]).
    pub async fn revoke(
        &self,
        store: &Store,
        space: &str,
        jtis: &[String],
        staked: bool,
        now: i64,
    ) -> anyhow::Result<Result<Revoked, Refused>> {
        let _io = self.io.lock().await;
        let until = now + KEEP_SECS;
        let authority = authority_of(space).unwrap_or_default();
        for _ in 0..CAS_RETRIES {
            let (mut doc, etag) = fetch(store, None).await?.unwrap_or_default();
            let before = doc.revoked.len();
            doc.prune(now);
            let live = doc.revoked.len();
            let mine = doc.of_authority(authority);
            if !doc.add(space, jtis, until) {
                if live == before {
                    return Ok(Ok(Revoked { added: self.install(&doc, etag, now), wrote: false }));
                }
            } else {
                let n = doc.revoked.len();
                let cap = |c: &std::sync::atomic::AtomicUsize| c.load(Ordering::Relaxed);
                if mine + (n - live) > cap(&self.caps.authority) {
                    return Ok(Err(Refused::Authority));
                }
                if n > cap(&self.caps.hard) || (n > cap(&self.caps.soft) && !staked) {
                    return Ok(Err(Refused::Full));
                }
            }
            let mode = match &etag {
                Some(e) => crate::cluster::if_match(Some(e.clone())),
                None => PutMode::Create,
            };
            let opts = PutOptions { mode, ..Default::default() };
            match bounded(store.raw.put_opts(&path(store), PutPayload::from(doc.encode()), opts)).await {
                Ok(r) => return Ok(Ok(Revoked { added: self.install(&doc, r.e_tag, now), wrote: true })),
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
        assert!(!a.loaded() && !a.fresh());
        assert!(a.refresh(&s, 0).await.unwrap().is_empty());
        assert!(a.loaded() && a.fresh());
        let r = a.revoke(&s, "sp", &["1".into(), "2".into()], true, 100).await.unwrap().unwrap();
        assert_eq!((r.added.len(), r.wrote), (2, true));
        assert!(a.is_revoked("sp", "1", 100));
        assert!(!a.is_revoked("other", "1", 100), "scoped to its space");
        // idempotent: nothing new, no write
        let r = a.revoke(&s, "sp", &["1".into()], true, 100).await.unwrap().unwrap();
        assert!(r.added.is_empty() && !r.wrote);
        // another node: a concurrent append isn't lost
        b.revoke(&s, "sp", &["3".into()], true, 100).await.unwrap().unwrap();
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
        a.revoke(&s, "sp", &["4".into()], true, later).await.unwrap().unwrap();
        let (doc, _) = fetch(&s, None).await.unwrap().unwrap();
        assert_eq!(doc.revoked.iter().map(|e| e.jti.as_str()).collect::<Vec<_>>(), ["4"]);
    }

    #[test]
    fn jtis() {
        assert!(valid_jti(&new_jti_like()));
        assert!(valid_jti(&"a".repeat(MAX_JTI_LEN)));
        for bad in ["", "a b", "\u{e9}", "a\n"] {
            assert!(!valid_jti(bad), "{bad:?}");
        }
        assert!(!valid_jti(&"a".repeat(MAX_JTI_LEN + 1)));
    }

    fn new_jti_like() -> String {
        crate::space::token::new_jti()
    }

    /// The object stays small whoever notifies: an authority's own cap, a
    /// soft cap past which only spaces with a stake here are taken, and a
    /// hard cap. Nothing past a cap is written.
    #[tokio::test]
    async fn bounded() {
        let s = store();
        let r = Revocations::default();
        let jtis = |from: usize, n: usize| (from..from + n).map(|i| format!("{i:032x}")).collect::<Vec<_>>();
        let space = |a: usize| format!("at://did:web:a{a}.example/space/t.t/k");
        // one authority, past its cap
        for i in 0..PER_AUTHORITY / 100 {
            r.revoke(&s, &space(0), &jtis(i * 100, 100), false, 1).await.unwrap().unwrap();
        }
        let e = r.revoke(&s, &space(0), &jtis(PER_AUTHORITY, 1), false, 1).await.unwrap();
        assert_eq!(e.unwrap_err(), Refused::Authority);
        // many authorities: unstaked up to the soft cap, staked up to the hard
        let mut a = 1;
        while r.len() + 100 <= SOFT_CAP {
            r.revoke(&s, &space(a), &jtis(0, 100), false, 1).await.unwrap().unwrap();
            a += 1;
        }
        let e = r.revoke(&s, &space(a), &jtis(0, 100), false, 1).await.unwrap();
        assert_eq!(e.unwrap_err(), Refused::Full);
        while r.len() + 100 <= HARD_CAP {
            a += 1;
            r.revoke(&s, &space(a), &jtis(0, 100), true, 1).await.unwrap().unwrap();
        }
        let e = r.revoke(&s, &space(a + 1), &jtis(0, 100), true, 1).await.unwrap();
        assert_eq!(e.unwrap_err(), Refused::Full);
        let (doc, _) = fetch(&s, None).await.unwrap().unwrap();
        assert_eq!(doc.revoked.len(), r.len());
        assert!(doc.encode().len() < 16 << 20);
        // already held: no cap applies, nothing is written
        let held = r.revoke(&s, &space(a), &jtis(0, 1), true, 1).await.unwrap().unwrap();
        assert!(!held.wrote);
        // past `until` the room comes back
        r.revoke(&s, &space(a + 1), &jtis(0, 1), true, 1 + KEEP_SECS).await.unwrap().unwrap();
    }

    #[test]
    fn blocks_expire() {
        let r = Revocations::default();
        r.block("sp", 10);
        assert!(r.is_blocked("sp", 10) && !r.is_blocked("other", 10));
        assert!(!r.is_blocked("sp", 10 + KEEP_SECS));
    }

    #[tokio::test]
    async fn stale_after_a_failed_read() {
        let s = store();
        let r = Revocations::default();
        r.refresh(&s, 0).await.unwrap();
        assert!(r.fresh());
        r.age_last_read(STALE_AFTER);
        assert!(r.loaded() && !r.fresh(), "a set not read for too long is refused");
        r.refresh(&s, 0).await.unwrap();
        assert!(r.fresh(), "a conditional re-read counts");
    }
}
