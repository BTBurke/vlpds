//! The notifyWrite outbox (repo-host side). A space write logs an `sP` row
//! with its rev and hash in the write's own entry, so the notify survives
//! a crash or takeover after the ack; this is the in-memory side that
//! sends it.
//!
//! One row per (repo, space), single-flight: writes acked while a send is
//! in flight only move the row's rev, and the next send carries the newest
//! one as soon as the first returns. Retries back off from 1 min, doubling
//! to 1 h with 50-100% jitter, until 24 h after the rev was written; a
//! permanent refusal drops the row. Retry state lives here only, never in
//! the bucket. A delivered row's `sP` delete rides the author's next space
//! write (see `take_delivered`), so delivery costs no extra PUT; a row left
//! behind is resent once by the next owner, which the authority ignores as
//! not newer.
//!
//! Bounded: at most [`MAX_ROWS`] rows are held (a row past that stays in
//! the bucket, and the owned shards' `sP` rows are scanned again once the
//! outbox has drained to half) and [`MAX_SENDS`] sends are in flight.

use crate::state::SpaceId;
use crate::tid::Tid;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

pub const RETRY_BASE: Duration = Duration::from_secs(60);
pub const RETRY_MAX: Duration = Duration::from_secs(3600);
pub const DEADLINE: Duration = Duration::from_secs(24 * 3600);
pub const MAX_ROWS: usize = 1 << 18;
pub const MAX_SENDS: usize = 256;

/// What a send came to.
#[derive(Debug)]
pub enum Outcome {
    Delivered,
    /// Refused for good (a 4xx the reference doesn't retry): dropped.
    Refused(String),
    Retry(String),
    /// The writer's account is inactive: tried again later, not counted.
    Wait,
    /// No longer this node's to send (the shard moved, the account is gone).
    Gone,
}

type Key = (Arc<str>, SpaceId);

struct Row {
    uri: Arc<str>,
    repo_rev: Tid,
    hash: [u8; 32],
    in_flight: bool,
    attempts: u32,
    next_at: Instant,
    /// When the write this row first held was acked (or the row was found
    /// on open): the outbox age gauge.
    since: Instant,
    /// When the write of `repo_rev` was acked (None: found on open).
    acked: Option<Instant>,
}

/// A send to make: the row as it was when taken.
pub struct Pending {
    pub did: Arc<str>,
    pub sid: SpaceId,
    pub uri: Arc<str>,
    pub repo_rev: Tid,
    pub hash: [u8; 32],
}

pub struct Outbox {
    rows: parking_lot::Mutex<HashMap<Key, Row>>,
    /// Delivered revs whose `sP` rows are still in the bucket, by author.
    delivered: parking_lot::Mutex<HashMap<Arc<str>, Vec<(SpaceId, Tid)>>>,
    wake: tokio::sync::Notify,
    started: AtomicBool,
    /// A row was left in the bucket for want of room.
    overflowed: AtomicBool,
    in_flight: std::sync::atomic::AtomicUsize,
    max_rows: usize,
}

impl Default for Outbox {
    fn default() -> Outbox {
        Outbox::new(MAX_ROWS)
    }
}

impl Outbox {
    pub fn new(max_rows: usize) -> Outbox {
        Outbox {
            rows: Default::default(),
            delivered: Default::default(),
            wake: Default::default(),
            started: AtomicBool::new(false),
            overflowed: AtomicBool::new(false),
            in_flight: Default::default(),
            max_rows,
        }
    }

    /// A write of (did, space) at `repo_rev` is durable; `acked`: it was
    /// just acked (not a row found on open).
    pub fn enqueue(&self, did: &str, sid: SpaceId, uri: &str, repo_rev: Tid, hash: [u8; 32], acked: bool) {
        let now = Instant::now();
        {
            let mut d = self.delivered.lock();
            if let Some(v) = d.get_mut(did) {
                v.retain(|(s, _)| *s != sid);
                if v.is_empty() {
                    d.remove(did);
                }
            }
        }
        let mut rows = self.rows.lock();
        let full = rows.len() >= self.max_rows;
        match rows.get_mut(&(did.into(), sid)) {
            Some(r) if r.repo_rev >= repo_rev => return,
            Some(r) => {
                r.repo_rev = repo_rev;
                r.hash = hash;
                r.attempts = 0;
                r.next_at = now;
                r.acked = acked.then_some(now);
            }
            None if full => {
                self.overflowed.store(true, Ordering::Release);
                crate::metrics::space_outbox_overflow();
                return;
            }
            None => {
                rows.insert(
                    (did.into(), sid),
                    Row {
                        uri: uri.into(),
                        repo_rev,
                        hash,
                        in_flight: false,
                        attempts: 0,
                        next_at: now,
                        since: now,
                        acked: acked.then_some(now),
                    },
                );
            }
        }
        drop(rows);
        self.wake.notify_one();
    }

    /// Delivered revs of `did`'s spaces whose `sP` rows the caller (the
    /// author's worker, which orders every `sP` write of the author) may
    /// delete, if it has written nothing newer.
    pub fn take_delivered(&self, did: &str) -> Vec<(SpaceId, Tid)> {
        let mut d = self.delivered.lock();
        if d.is_empty() {
            return Vec::new();
        }
        d.remove(did).unwrap_or_default()
    }

    /// `did`'s account is active again: its waiting rows send now.
    pub fn resume(&self, did: &str) {
        let now = Instant::now();
        let mut any = false;
        for ((d, _), r) in self.rows.lock().iter_mut() {
            if &**d == did && !r.in_flight {
                r.next_at = now;
                any = true;
            }
        }
        if any {
            self.wake.notify_one();
        }
    }

    /// `did`'s account is gone with its rows.
    pub fn drop_did(&self, did: &str) {
        self.rows.lock().retain(|(d, _), _| &**d != did);
        self.delivered.lock().remove(did);
    }

    pub fn len(&self) -> usize {
        self.rows.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the rows left in the bucket should be scanned for again:
    /// some were, and there's room for them now.
    fn take_overflow(&self) -> bool {
        self.overflowed.load(Ordering::Acquire)
            && self.rows.lock().len() <= self.max_rows / 2
            && self.overflowed.swap(false, Ordering::AcqRel)
    }

    /// Rows due now (as many as may be in flight), marked in flight.
    fn due(&self, now: Instant) -> (Vec<Pending>, Option<Instant>) {
        let mut rows = self.rows.lock();
        let mut out = Vec::new();
        let mut next: Option<Instant> = None;
        let room = MAX_SENDS.saturating_sub(self.in_flight.load(Ordering::Acquire));
        for ((did, sid), r) in rows.iter_mut() {
            if r.in_flight {
                continue;
            }
            if r.next_at <= now {
                // a row past the room waits for a send to finish
                if out.len() == room {
                    continue;
                }
                r.in_flight = true;
                out.push(Pending {
                    did: did.clone(),
                    sid: *sid,
                    uri: r.uri.clone(),
                    repo_rev: r.repo_rev,
                    hash: r.hash,
                });
            } else {
                next = Some(next.map_or(r.next_at, |n| n.min(r.next_at)));
            }
        }
        self.in_flight.fetch_add(out.len(), Ordering::AcqRel);
        let oldest = rows.values().map(|r| r.since).min();
        crate::metrics::space_outbox_gauges(rows.len(), oldest.map_or(0.0, |s| now.duration_since(s).as_secs_f64()));
        (out, next)
    }

    fn finish(&self, s: &Pending, outcome: &Outcome) {
        if self.in_flight.fetch_sub(1, Ordering::AcqRel) >= MAX_SENDS {
            self.wake.notify_one();
        }
        let key: Key = (s.did.clone(), s.sid);
        let now = Instant::now();
        let mut rows = self.rows.lock();
        let Some(r) = rows.get_mut(&key) else { return };
        r.in_flight = false;
        let newer = r.repo_rev > s.repo_rev;
        let expired = Duration::from_micros(crate::tid::now_micros().saturating_sub(r.repo_rev.micros())) > DEADLINE;
        let (result, drop_row) = match outcome {
            Outcome::Delivered => {
                if let Some(t) = r.acked.filter(|_| !newer) {
                    crate::metrics::space_notify_ack(now.duration_since(t));
                }
                ("ok", !newer)
            }
            Outcome::Refused(_) => ("refused", !newer),
            Outcome::Gone => ("gone", true),
            Outcome::Retry(_) if expired => ("expired", true),
            Outcome::Retry(_) => ("retry", false),
            Outcome::Wait => ("wait", false),
        };
        crate::metrics::space_notify("out", result);
        if newer {
            r.attempts = 0;
            r.next_at = now;
        } else if !drop_row {
            if !matches!(outcome, Outcome::Wait) {
                r.attempts += 1;
            }
            r.next_at = now + backoff(r.attempts.max(1));
        }
        if drop_row {
            let r = rows.remove(&key).expect("present");
            drop(rows);
            if !matches!(outcome, Outcome::Gone) {
                self.delivered.lock().entry(key.0).or_default().push((key.1, r.repo_rev));
            }
        } else {
            drop(rows);
        }
        if newer {
            self.wake.notify_one();
        }
        if let Outcome::Refused(why) | Outcome::Retry(why) = outcome {
            tracing::info!(did = %s.did, space = %s.uri, rev = %s.repo_rev, "space notifyWrite {result}: {why}");
        }
    }

    /// Starts the sender (once). `app` is held weakly: the sender ends
    /// with the server.
    pub fn start(self: &Arc<Self>, app: Weak<crate::xrpc::App>) {
        if self.started.swap(true, Ordering::AcqRel) {
            return;
        }
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                let (sends, next) = me.due(Instant::now());
                for s in sends {
                    let Some(app) = app.upgrade() else { return };
                    let me = me.clone();
                    tokio::spawn(async move {
                        let outcome = crate::xrpc::space::deliver(&app, &s).await;
                        me.finish(&s, &outcome);
                    });
                }
                if me.take_overflow() {
                    let Some(app) = app.upgrade() else { return };
                    if let Some(sp) = app.spaces.clone() {
                        let shards = app.partitions.owned().into_iter().map(|p| (p.id, p.db.clone())).collect();
                        sp.spawn_outbox_rescan(shards);
                    }
                }
                if app.strong_count() == 0 {
                    return;
                }
                // the gauges stay fresh while rows wait
                let wait = next.map_or(Duration::from_secs(1), |n| n.saturating_duration_since(Instant::now()));
                let _ = tokio::time::timeout(wait.min(Duration::from_secs(1)), me.wake.notified()).await;
            }
        });
    }
}

/// 1 min doubling to 1 h, then 50-100% of that.
pub fn backoff(attempts: u32) -> Duration {
    let base = RETRY_BASE.saturating_mul(1 << attempts.saturating_sub(1).min(6)).min(RETRY_MAX);
    base.mul_f64(0.5 + rand::random::<f64>() / 2.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn send(o: &Outbox) -> Vec<Pending> {
        o.due(Instant::now()).0
    }

    #[test]
    fn single_flight_coalesces() {
        let o = Outbox::default();
        let sid = [1; 16];
        o.enqueue("did:a", sid, "at://s", Tid(1), [0; 32], true);
        let s1 = send(&o);
        assert_eq!(s1.len(), 1);
        // in flight: newer writes only move the row
        o.enqueue("did:a", sid, "at://s", Tid(2), [0; 32], true);
        o.enqueue("did:a", sid, "at://s", Tid(3), [0; 32], true);
        assert!(send(&o).is_empty());
        o.finish(&s1[0], &Outcome::Delivered);
        let s2 = send(&o);
        assert_eq!(s2.iter().map(|s| s.repo_rev).collect::<Vec<_>>(), vec![Tid(3)]);
        assert!(o.take_delivered("did:a").is_empty(), "the newer rev is still owed");
        o.finish(&s2[0], &Outcome::Delivered);
        assert!(o.is_empty());
        assert_eq!(o.take_delivered("did:a"), vec![(sid, Tid(3))]);
        // an older rev never replaces a newer one
        o.enqueue("did:a", sid, "at://s", Tid(5), [0; 32], false);
        o.enqueue("did:a", sid, "at://s", Tid(4), [0; 32], false);
        assert_eq!(send(&o)[0].repo_rev, Tid(5));
    }

    #[test]
    fn bounded() {
        let o = Outbox::new(2);
        for i in 0..3u8 {
            o.enqueue("did:a", [i; 16], "at://s", Tid(1), [0; 32], true);
        }
        assert_eq!(o.len(), 2);
        assert!(!o.take_overflow(), "no room yet");
        let s = send(&o);
        for p in &s {
            o.finish(p, &Outcome::Delivered);
        }
        assert!(o.take_overflow(), "drained: rescan");
        assert!(!o.take_overflow(), "once");
        // sends in flight are capped
        let o = Outbox::default();
        for i in 0..(MAX_SENDS + 10) {
            let mut sid = [0; 16];
            sid[..8].copy_from_slice(&(i as u64).to_be_bytes());
            o.enqueue("did:a", sid, "at://s", Tid(1), [0; 32], true);
        }
        let first = send(&o);
        assert_eq!(first.len(), MAX_SENDS);
        assert!(send(&o).is_empty());
        o.finish(&first[0], &Outcome::Delivered);
        assert_eq!(send(&o).len(), 1);
    }

    #[test]
    fn retries_back_off() {
        let o = Outbox::default();
        let sid = [1; 16];
        o.enqueue("did:a", sid, "at://s", crate::tid::Tid::from_parts(crate::tid::now_micros(), 0), [0; 32], true);
        let s = send(&o);
        o.finish(&s[0], &Outcome::Retry("503".into()));
        assert!(send(&o).is_empty(), "backed off");
        assert_eq!(o.len(), 1);
        // a newer write sends at once
        o.enqueue("did:a", sid, "at://s", crate::tid::Tid::from_parts(crate::tid::now_micros() + 1, 0), [0; 32], true);
        let s = send(&o);
        assert_eq!(s.len(), 1);
        o.finish(&s[0], &Outcome::Refused("400".into()));
        assert!(o.is_empty());
        // past the deadline a retryable failure drops the row
        o.enqueue("did:b", sid, "at://s", Tid::from_parts(1_000_000, 0), [0; 32], false);
        let s = send(&o);
        o.finish(&s[0], &Outcome::Retry("503".into()));
        assert!(o.is_empty());
        for a in 1..10 {
            let d = backoff(a);
            assert!(d >= RETRY_BASE / 2 && d <= RETRY_MAX, "{a}: {d:?}");
        }
        assert!(backoff(1) <= RETRY_BASE);
    }
}
