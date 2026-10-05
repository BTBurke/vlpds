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
//!
//! Only revocations with a stake here are stored (the caller drops the
//! rest: no credential for such a space reads anything here), each capped
//! per authority, space and audience account. One that can't be stored
//! blocks its space instead, in the object too, so the block outlives a
//! restart and reaches every node. It never fails open: with the blocks
//! full, every space credential is refused until they drain.

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
/// Live entries in all. The object is read whole by every node, so it
/// stays small (~7 MB here).
pub const HARD_CAP: usize = 50_000;
/// Live entries of one authority's spaces.
pub const PER_AUTHORITY: usize = 2_000;
/// Live entries of one space.
pub const PER_SPACE: usize = 1_000;
/// Live entries one account here gave the stake for: one account and many
/// authorities can't fill the object.
pub const PER_AUD: usize = 5_000;
/// Spaces blocked at once; past this, every space is.
const MAX_BLOCKED: usize = 10_000;
/// Revokes waiting on this node's writes: past this they're refused at
/// once (the space blocked) rather than queued behind a flood.
pub const MAX_QUEUED: usize = 8;

/// The control object.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Doc {
    pub revoked: Vec<Entry>,
    // The new fields are left out while unset, so an object without them
    // is written as level 1 wrote it.
    /// Spaces whose revocation couldn't be stored.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked: Vec<Block>,
    /// Every space, once `blocked` is full (Unix seconds).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub all_blocked_until: i64,
    /// Bumped by every write: a node never installs an older object over a
    /// newer one, whichever read finishes last.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub gen: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub space: String,
    pub jti: String,
    /// Unix seconds.
    pub until: i64,
    /// The account here that gave the stake ([`PER_AUD`]).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub aud: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Block {
    pub space: String,
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

    /// Whether anything was past `until`.
    fn prune(&mut self, now: i64) -> bool {
        let n = (self.revoked.len(), self.blocked.len());
        self.revoked.retain(|e| e.until > now);
        self.blocked.retain(|b| b.until > now);
        n != (self.revoked.len(), self.blocked.len())
    }

    /// Each (space, jti) not held yet (one already held outlives every
    /// credential it can name).
    fn new_entries(&self, space: &str, aud: &str, jtis: &[String], until: i64) -> Vec<Entry> {
        let held: std::collections::HashSet<&str> =
            self.revoked.iter().filter(|e| e.space == space).map(|e| e.jti.as_str()).collect();
        jtis.iter()
            .filter(|j| !held.contains(j.as_str()))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(|j| Entry { space: space.into(), jti: j.clone(), until, aud: aud.into() })
            .collect()
    }

    fn count(&self, f: impl Fn(&Entry) -> bool) -> usize {
        self.revoked.iter().filter(|e| f(e)).count()
    }

    /// Blocks `space` until `until`; every space, once [`MAX_BLOCKED`] are.
    fn block(&mut self, space: &str, until: i64) {
        if let Some(b) = self.blocked.iter_mut().find(|b| b.space == space) {
            b.until = b.until.max(until);
        } else if self.blocked.len() < MAX_BLOCKED {
            self.blocked.push(Block { space: space.into(), until });
        } else {
            self.all_blocked_until = self.all_blocked_until.max(until);
        }
    }
}

fn is_zero<T: Default + PartialEq>(n: &T) -> bool {
    *n == T::default()
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
    /// The space has [`PER_SPACE`].
    Space,
    /// The audience account gave the stake for [`PER_AUD`].
    Aud,
    /// The object has [`HARD_CAP`].
    Full,
    /// [`MAX_QUEUED`] revokes were waiting on this node.
    Busy,
}

#[derive(Debug, Default)]
pub struct Revoked {
    /// Refused, the space blocked instead (in the object when `wrote`).
    pub refused: Option<Refused>,
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

/// [`HARD_CAP`], [`PER_AUTHORITY`], [`PER_SPACE`] and [`PER_AUD`]; tests
/// lower them.
struct Caps {
    hard: std::sync::atomic::AtomicUsize,
    authority: std::sync::atomic::AtomicUsize,
    space: std::sync::atomic::AtomicUsize,
    aud: std::sync::atomic::AtomicUsize,
}

impl Default for Caps {
    fn default() -> Caps {
        Caps { hard: HARD_CAP.into(), authority: PER_AUTHORITY.into(), space: PER_SPACE.into(), aud: PER_AUD.into() }
    }
}

/// What a node enforces, from the object and its own blocks.
#[derive(Default)]
struct Installed {
    /// space -> jti -> until (Unix seconds).
    set: HashMap<String, HashMap<String, i64>>,
    /// The object's blocks, space -> until.
    blocked: HashMap<String, i64>,
    all_blocked_until: i64,
    gen: u64,
}

#[derive(Default)]
pub struct Revocations {
    st: parking_lot::RwLock<Installed>,
    /// Of the object installed last.
    etag: parking_lot::Mutex<Option<String>>,
    loaded: AtomicBool,
    /// When a read of the object last succeeded (unix µs).
    last_ok: std::sync::atomic::AtomicU64,
    /// space -> until (Unix seconds): spaces blocked here alone (a peer's
    /// nudge, or a block the object couldn't take), and every space once
    /// [`MAX_BLOCKED`] are. Fails closed.
    local_blocked: parking_lot::RwLock<(HashMap<String, i64>, i64)>,
    caps: Caps,
    /// Appends on this node, one at a time (fewer CAS conflicts). Re-reads
    /// take their own lock, so a queue of appends never holds them up past
    /// [`STALE_AFTER`]; `Installed::gen` orders what either installs.
    writes: tokio::sync::Mutex<()>,
    reads: tokio::sync::Mutex<()>,
    queued: std::sync::atomic::AtomicUsize,
    pub(super) wake: std::sync::Arc<tokio::sync::Notify>,
    pub(super) started: AtomicBool,
}

impl Revocations {
    pub fn is_revoked(&self, space: &str, jti: &str, now: i64) -> bool {
        let g = self.st.read();
        if g.set.is_empty() {
            return false;
        }
        g.set.get(space).and_then(|j| j.get(jti)).is_some_and(|until| *until > now)
    }

    /// The live revocations of `space`: (jti, until).
    pub fn of_space(&self, space: &str, now: i64) -> Vec<(String, i64)> {
        let g = self.st.read();
        let mut out: Vec<(String, i64)> =
            g.set.get(space).into_iter().flatten().filter(|(_, u)| **u > now).map(|(j, u)| (j.clone(), *u)).collect();
        out.sort();
        out
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
    pub fn set_caps(&self, hard: usize, authority: usize, space: usize, aud: usize) {
        self.caps.hard.store(hard, Ordering::Relaxed);
        self.caps.authority.store(authority, Ordering::Relaxed);
        self.caps.space.store(space, Ordering::Relaxed);
        self.caps.aud.store(aud, Ordering::Relaxed);
    }

    /// Makes the last good read `by` older, as a run of failed re-reads would.
    #[doc(hidden)]
    pub fn age_last_read(&self, by: Duration) {
        self.last_ok.fetch_sub(by.as_micros() as u64, Ordering::AcqRel);
    }

    fn read_ok(&self) {
        self.last_ok.store(crate::tid::now_micros(), Ordering::Release);
    }

    /// Until when `space`'s credentials are refused, if they are.
    pub fn blocked_until(&self, space: &str, now: i64) -> Option<i64> {
        let live = |u: i64| (u > now).then_some(u);
        let (local, all_local) = {
            let g = self.local_blocked.read();
            (g.0.get(space).copied().and_then(live), live(g.1))
        };
        let g = self.st.read();
        [local, all_local, g.blocked.get(space).copied().and_then(live), live(g.all_blocked_until)]
            .into_iter()
            .flatten()
            .max()
    }

    pub fn is_blocked(&self, space: &str, now: i64) -> bool {
        self.blocked_until(space, now).is_some()
    }

    /// Refuses `space`'s credentials here for as long as a revocation of it
    /// would have lasted; every space's once [`MAX_BLOCKED`] are blocked.
    pub fn block(&self, space: &str, now: i64) {
        let mut g = self.local_blocked.write();
        g.0.retain(|_, until| *until > now);
        if g.0.len() < MAX_BLOCKED || g.0.contains_key(space) {
            g.0.insert(space.to_string(), now + KEEP_SECS);
        } else {
            tracing::error!("space revocation blocks full: refusing every space credential until they drain");
            g.1 = g.1.max(now + KEEP_SECS);
        }
    }

    pub fn len(&self) -> usize {
        self.st.read().set.values().map(HashMap::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Asks the background re-read to run now.
    pub fn wake(&self) {
        self.wake.notify_one();
    }

    /// Replaces the set with `doc`'s live entries, unless a newer object is
    /// installed already; returns those that are new to this node.
    fn install(&self, doc: &Doc, etag: Option<String>, now: i64) -> Vec<(String, String)> {
        let mut next: HashMap<String, HashMap<String, i64>> = HashMap::new();
        for e in doc.revoked.iter().filter(|e| e.until > now) {
            let u = next.entry(e.space.clone()).or_default().entry(e.jti.clone()).or_insert(e.until);
            *u = (*u).max(e.until);
        }
        let blocked = doc.blocked.iter().filter(|b| b.until > now).map(|b| (b.space.clone(), b.until)).collect();
        let mut g = self.st.write();
        if self.loaded() && doc.gen < g.gen {
            drop(g);
            self.read_ok();
            return Vec::new();
        }
        let added = next
            .iter()
            .flat_map(|(s, js)| js.keys().map(move |j| (s, j)))
            .filter(|(s, j)| !g.set.get(*s).is_some_and(|m| m.contains_key(*j)))
            .map(|(s, j)| (s.clone(), j.clone()))
            .collect();
        *g = Installed { set: next, blocked, all_blocked_until: doc.all_blocked_until, gen: doc.gen };
        let n = g.set.values().map(HashMap::len).sum::<usize>();
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
        let _reads = self.reads.lock().await;
        let seen = if self.loaded() { self.etag.lock().clone() } else { None };
        match fetch(store, seen).await {
            Ok(Some((doc, etag))) => Ok(self.install(&doc, etag, now)),
            Ok(None) => Ok(self.install(&Doc::default(), None, now)),
            Err(object_store::Error::NotModified { .. }) => {
                // entries past `until` leave even when nothing was written
                let mut g = self.st.write();
                g.set.retain(|_, js| {
                    js.retain(|_, until| *until > now);
                    !js.is_empty()
                });
                g.blocked.retain(|_, until| *until > now);
                drop(g);
                self.read_ok();
                crate::metrics::space_revocations(self.len());
                Ok(Vec::new())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Appends `jtis` of `space` to the object (CAS), pruning expired
    /// entries as it goes, and installs the result here. `aud`: the account
    /// here whose stake let it in. Durable when this returns Ok with no
    /// `refused`; a refused one blocks the space in the object instead
    /// (`wrote`) or, with the object unwritable, Err (the caller blocks it
    /// here).
    pub async fn revoke(
        &self,
        store: &Store,
        space: &str,
        aud: &str,
        jtis: &[String],
        now: i64,
    ) -> anyhow::Result<Revoked> {
        struct Queued<'a>(&'a std::sync::atomic::AtomicUsize);
        impl Drop for Queued<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
        let _q = Queued(&self.queued);
        if self.queued.fetch_add(1, Ordering::AcqRel) >= MAX_QUEUED {
            return Ok(Revoked { refused: Some(Refused::Busy), ..Default::default() });
        }
        let _writes = self.writes.lock().await;
        let until = now + KEEP_SECS;
        let authority = authority_of(space).unwrap_or_default();
        let cap = |c: &std::sync::atomic::AtomicUsize| c.load(Ordering::Relaxed);
        for _ in 0..CAS_RETRIES {
            let (mut doc, etag) = fetch(store, None).await?.unwrap_or_default();
            let pruned = doc.prune(now);
            let new = doc.new_entries(space, aud, jtis, until);
            let refused = if new.is_empty() {
                if !pruned {
                    return Ok(Revoked { added: self.install(&doc, etag, now), ..Default::default() });
                }
                None
            } else {
                let k = new.len();
                if doc.count(|e| authority_of(&e.space) == Some(authority)) + k > cap(&self.caps.authority) {
                    Some(Refused::Authority)
                } else if doc.count(|e| e.space == space) + k > cap(&self.caps.space) {
                    Some(Refused::Space)
                } else if doc.count(|e| e.aud == aud) + k > cap(&self.caps.aud) {
                    Some(Refused::Aud)
                } else if doc.revoked.len() + k > cap(&self.caps.hard) {
                    Some(Refused::Full)
                } else {
                    None
                }
            };
            match refused {
                Some(_) => doc.block(space, until),
                None => doc.revoked.extend(new),
            }
            doc.gen += 1;
            let mode = match &etag {
                Some(e) => crate::cluster::if_match(Some(e.clone())),
                None => PutMode::Create,
            };
            let opts = PutOptions { mode, ..Default::default() };
            match bounded(store.raw.put_opts(&path(store), PutPayload::from(doc.encode()), opts)).await {
                Ok(r) => return Ok(Revoked { added: self.install(&doc, r.e_tag, now), wrote: true, refused }),
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

    async fn revoke(r: &Revocations, s: &Store, space: &str, aud: &str, jtis: &[String], now: i64) -> Revoked {
        r.revoke(s, space, aud, jtis, now).await.unwrap()
    }

    #[tokio::test]
    async fn append_reload_and_prune() {
        let s = store();
        let (a, b) = (Revocations::default(), Revocations::default());
        assert!(!a.loaded() && !a.fresh());
        assert!(a.refresh(&s, 0).await.unwrap().is_empty());
        assert!(a.loaded() && a.fresh());
        let r = revoke(&a, &s, "sp", "did:aud", &["1".into(), "2".into()], 100).await;
        assert_eq!((r.added.len(), r.wrote, r.refused), (2, true, None));
        assert!(a.is_revoked("sp", "1", 100));
        assert!(!a.is_revoked("other", "1", 100), "scoped to its space");
        assert_eq!(a.of_space("sp", 100), [("1".to_string(), 100 + KEEP_SECS), ("2".to_string(), 100 + KEEP_SECS)]);
        // idempotent: nothing new, no write
        let r = revoke(&a, &s, "sp", "did:aud", &["1".into()], 100).await;
        assert!(r.added.is_empty() && !r.wrote);
        // another node: a concurrent append isn't lost
        revoke(&b, &s, "sp", "did:aud", &["3".into()], 100).await;
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
        revoke(&a, &s, "sp", "did:aud", &["4".into()], later).await;
        let (doc, _) = fetch(&s, None).await.unwrap().unwrap();
        assert_eq!(doc.revoked.iter().map(|e| e.jti.as_str()).collect::<Vec<_>>(), ["4"]);
    }

    /// An object written before this version (no gen, no blocks, no aud)
    /// still reads.
    #[test]
    fn older_objects_decode() {
        let d = Doc::decode(br#"{"revoked":[{"space":"s","jti":"j","until":5}]}"#).unwrap();
        assert_eq!((d.gen, d.blocked.len(), d.revoked[0].aud.as_str()), (0, 0, ""));
    }

    #[test]
    fn valid_jtis() {
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

    fn jtis(from: usize, n: usize) -> Vec<String> {
        (from..from + n).map(|i| format!("{i:032x}")).collect()
    }

    fn space(a: usize, k: usize) -> String {
        format!("at://did:web:a{a}.example/space/t.t/k{k}")
    }

    /// The object stays small whoever notifies: caps per authority, space
    /// and audience account, and in all. A refusal stores a block of the
    /// space instead, which every node reads, before and after a restart.
    #[tokio::test]
    async fn bounded_and_blocked_in_the_object() {
        let s = store();
        let r = Revocations::default();
        r.set_caps(400, 200, 100, 300);
        // one space, past its cap
        revoke(&r, &s, &space(0, 0), "did:x", &jtis(0, 100), 1).await;
        let x = revoke(&r, &s, &space(0, 0), "did:y", &jtis(100, 1), 1).await;
        assert_eq!((x.refused, x.wrote), (Some(Refused::Space), true));
        assert!(r.is_blocked(&space(0, 0), 1) && !r.is_blocked(&space(0, 1), 1));
        // one authority, past its cap
        revoke(&r, &s, &space(0, 1), "did:y", &jtis(0, 100), 1).await;
        let x = revoke(&r, &s, &space(0, 2), "did:z", &jtis(0, 1), 1).await;
        assert_eq!(x.refused, Some(Refused::Authority));
        // one audience account, past its cap (x has 100, y 100)
        for a in 1..=2 {
            revoke(&r, &s, &space(a, 0), "did:x", &jtis(0, 100), 1).await;
        }
        let x = revoke(&r, &s, &space(3, 0), "did:x", &jtis(0, 1), 1).await;
        assert_eq!(x.refused, Some(Refused::Aud));
        // in all
        let x = revoke(&r, &s, &space(4, 0), "did:w", &jtis(0, 100), 1).await;
        assert_eq!(x.refused, Some(Refused::Full), "{}", r.len());
        assert_eq!(r.len(), 400);
        let (doc, _) = fetch(&s, None).await.unwrap().unwrap();
        assert_eq!(doc.revoked.len(), 400);
        assert_eq!(doc.blocked.len(), 4);
        // already held: no cap applies, nothing is written
        let held = revoke(&r, &s, &space(1, 0), "did:x", &jtis(0, 1), 1).await;
        assert!(!held.wrote && held.refused.is_none());
        // another node (or this one restarted) reads the blocks
        let fresh = Revocations::default();
        fresh.refresh(&s, 1).await.unwrap();
        for b in [space(0, 0), space(0, 2), space(3, 0), space(4, 0)] {
            assert!(fresh.is_blocked(&b, 1), "{b}");
            assert!(!fresh.is_blocked(&b, 1 + KEEP_SECS), "{b}");
        }
        assert!(!fresh.is_blocked(&space(1, 0), 1));
        // past `until` the room comes back
        let x = revoke(&r, &s, &space(4, 0), "did:w", &jtis(0, 1), 1 + KEEP_SECS).await;
        assert_eq!(x.refused, None);
    }

    /// Never fails open: with the blocks full, every space is refused.
    #[test]
    fn full_blocks_refuse_every_space() {
        let mut d = Doc::default();
        for i in 0..MAX_BLOCKED {
            d.block(&space(i, 0), 10);
        }
        assert_eq!(d.all_blocked_until, 0);
        d.block(&space(0, 0), 20);
        assert_eq!(d.all_blocked_until, 0, "a space already blocked is extended");
        d.block("at://did:web:new/space/t.t/k", 30);
        assert_eq!(d.all_blocked_until, 30);
        let r = Revocations::default();
        r.install(&d, None, 0);
        assert!(r.is_blocked("at://did:web:other/space/t.t/k", 0));
        assert!(!r.is_blocked("at://did:web:other/space/t.t/k", 30));

        let r = Revocations::default();
        for i in 0..MAX_BLOCKED {
            r.block(&space(i, 0), 10);
        }
        assert!(!r.is_blocked("at://did:web:other/space/t.t/k", 10));
        r.block("at://did:web:new/space/t.t/k", 10);
        assert!(r.is_blocked("at://did:web:other/space/t.t/k", 10));
    }

    #[test]
    fn blocks_expire() {
        let r = Revocations::default();
        r.block("sp", 10);
        assert!(r.is_blocked("sp", 10) && !r.is_blocked("other", 10));
        assert!(!r.is_blocked("sp", 10 + KEEP_SECS));
    }

    /// Appends queued behind a slow one don't hold the re-read up (it has
    /// its own lock), and past [`MAX_QUEUED`] they're refused at once.
    #[tokio::test]
    async fn a_queue_of_appends_never_starves_the_reread() {
        let s = store();
        let r = Arc::new(Revocations::default());
        r.refresh(&s, 0).await.unwrap();
        let slow = r.writes.lock().await;
        let waiting: Vec<_> = (0..MAX_QUEUED)
            .map(|i| {
                let (r, s) = (r.clone(), s.clone());
                tokio::spawn(async move { r.revoke(&s, &space(i, 0), "did:a", &jtis(0, 1), 0).await.unwrap() })
            })
            .collect();
        while r.queued.load(Ordering::Acquire) < MAX_QUEUED {
            tokio::task::yield_now().await;
        }
        let t = std::time::Instant::now();
        let busy = r.revoke(&s, &space(99, 0), "did:a", &jtis(0, 1), 0).await.unwrap();
        assert_eq!(busy.refused, Some(Refused::Busy));
        r.age_last_read(STALE_AFTER);
        assert!(!r.fresh());
        tokio::time::timeout(Duration::from_secs(1), r.refresh(&s, 0)).await.expect("re-read not starved").unwrap();
        assert!(r.fresh() && t.elapsed() < Duration::from_secs(1));
        drop(slow);
        for w in waiting {
            assert_eq!(w.await.unwrap().refused, None);
        }
        assert_eq!(r.len(), MAX_QUEUED);
    }

    /// A slow re-read that read an older object doesn't install it over a
    /// newer one an append installed meanwhile.
    #[tokio::test]
    async fn an_older_object_never_replaces_a_newer_one() {
        let s = store();
        let r = Revocations::default();
        revoke(&r, &s, "sp", "did:a", &["1".into()], 0).await;
        let (old, e) = fetch(&s, None).await.unwrap().unwrap();
        revoke(&r, &s, "sp", "did:a", &["2".into()], 0).await;
        r.install(&old, e, 0);
        assert!(r.is_revoked("sp", "2", 0));
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
