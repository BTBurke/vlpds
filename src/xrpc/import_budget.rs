//! importRepo admission by estimated working set (DESIGN.md "Import
//! admission"). Each import reserves what its repo's size says it will hold
//! (from Content-Length, else a small start that grows as the body
//! arrives) from the node's import budget, one FIFO byte gate. Imports
//! whose estimate passes [`SMALL_SHARE`] also take the excess from a large
//! share (half the budget), so large imports can't crowd out small ones;
//! a large one blocked only by that share lets smaller ones pass, and one
//! blocked by the budget itself holds the line until enough frees, so
//! small ones can't starve it either.

use super::*;
use crate::metrics;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

const KIB: u64 = 1 << 10;
const MIB: u64 = 1 << 20;

/// An import's smallest and largest reservation.
pub const MIN_WORKING_SET: u64 = 512 * KIB;
pub const MAX_WORKING_SET: u64 = 80 * MIB;
/// What an import may hold outside the large share.
pub const SMALL_SHARE: u64 = 8 * MIB;
/// The import budget, when not given: a share of the node's memory budget,
/// within these bounds.
pub const BUDGET_FRACTION: u64 = 16;
pub const MIN_BUDGET: u64 = 192 * MIB;
pub const MAX_BUDGET: u64 = 1 << 30;

/// CAR bytes assumed before any arrive, without a Content-Length.
const UNKNOWN_LENGTH: u64 = 64 * KIB;
pub const ADMIT_WAIT: Duration = Duration::from_secs(30);
const GROW_WAIT: Duration = Duration::from_secs(10);

/// An import's own state outside its batches: tasks, channels, the parse's
/// right spine of open MST nodes, the staging maps.
const FIXED: u64 = 64 * KIB;
/// Batches alive at once: parsed ahead, being built, being staged, in flight.
const LIVE_BATCHES: u64 = (super::import_stream::ITEMS_AHEAD + super::staged_import::IN_FLIGHT + 2) as u64;
/// A record's heap in a live batch besides its bytes: path, CID, blob refs,
/// and its rows' keys.
const RECORD_OVERHEAD: u64 = 400;
/// CAR bytes per record of a real-shaped repo: the record block, its MST
/// entry, node overhead (tests/all/import_burst.rs measures 326).
const CAR_PER_RECORD: u64 = 330;
/// Fewest CAR bytes per record when counting records from a CAR's size.
const MIN_CAR_PER_RECORD: u64 = 200;
/// A repo's heap whole, as batches, per CAR byte: its record bytes twice
/// (as parsed, as row values) plus each record's overhead.
const REPO_FACTOR: u64 = 3;
/// The buffered parse per CAR byte: the body, its block map, the tree it
/// loads and the rebuilt one, the record list.
const BUFFERED_FACTOR: u64 = 4;
pub(super) const BLOOM_BITS_PER_LINK: u64 = 10;
pub(super) const MAX_BLOOM_WORDS: usize = 1 << 19;

/// What an import of a `car`-byte repo holds, and the batch and bloom sizes
/// that keep it there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sizing {
    pub batch_bytes: usize,
    pub batch_records: usize,
    pub bloom_words: usize,
    pub working_set: u64,
}

/// Small repos fit in one batch; batch bytes scale with the repo up to 4
/// MiB; the record count per batch stays 4,096 (a repo's batches are log
/// round trips: fewer, larger ones keep big imports fast).
pub fn sizing(car: u64) -> Sizing {
    let batch_bytes = (car / 4).clamp(256 * KIB, 4 * MIB);
    let batch_records = super::import_stream::BATCH_RECORDS as u64;
    let records = car / MIN_CAR_PER_RECORD + 1;
    let bloom_words = (records * BLOOM_BITS_PER_LINK).div_ceil(64).next_power_of_two().clamp(16, MAX_BLOOM_WORDS as u64);
    let window = LIVE_BATCHES * (2 * batch_bytes + RECORD_OVERHEAD * batch_records);
    let batches = window.min(REPO_FACTOR * car);
    let body = car.min(super::import_stream::SPILL_AFTER as u64);
    let working_set = (FIXED + body + batches + bloom_words * 8).clamp(MIN_WORKING_SET, MAX_WORKING_SET);
    Sizing { batch_bytes: batch_bytes as usize, batch_records: batch_records as usize, bloom_words: bloom_words as usize, working_set }
}

/// The buffered fallback holds the whole body and its parse.
pub fn buffered_working_set(car: u64) -> u64 {
    (FIXED + BUFFERED_FACTOR * car).max(sizing(car).working_set)
}

/// The import budget a node of `budget` bytes reserves.
pub fn budget_share(budget: u64) -> u64 {
    (budget / BUDGET_FRACTION).clamp(MIN_BUDGET, MAX_BUDGET)
}

/// The import budget in `--memory-plan`: what one import reserves at the
/// real distribution's percentiles, and how many fit.
pub fn plan_json(budget: u64) -> serde_json::Value {
    let at = |q: f64| sizing(crate::real_dist::quantile(q) as u64 * CAR_PER_RECORD + 400).working_set;
    serde_json::json!({
        "budget_mb": budget >> 20,
        "large_share_mb": (budget / 2) >> 20,
        "per_import_kb": {"min": MIN_WORKING_SET >> 10, "p90": at(0.9) >> 10, "p99": at(0.99) >> 10, "p99.9": at(0.999) >> 10, "max": MAX_WORKING_SET >> 10},
        "p90_imports_beside_a_p99.9": budget.saturating_sub(at(0.999)) / at(0.9),
    })
}

/// The node's import budget: a FIFO gate over bytes, with the large share.
pub struct ImportBudget {
    total: u64,
    large: u64,
    wait: Duration,
    st: parking_lot::Mutex<Gate>,
    /// The buffered fallback runs one at a time.
    pub(super) buffered: tokio::sync::Semaphore,
}

#[derive(Default)]
struct Gate {
    free: u64,
    free_large: u64,
    /// Growth first (in order), then admissions (in order).
    queue: VecDeque<Waiter>,
    next: u64,
}

struct Waiter {
    id: u64,
    need: u64,
    need_large: u64,
    grow: bool,
    tx: oneshot::Sender<()>,
}

impl Gate {
    /// Grants waiters in order. One the budget can't hold stops the line;
    /// one only the large share can't hold lets those after it that need
    /// none of it pass (larger ones wait behind it).
    fn grant(&mut self) {
        let mut i = 0;
        let mut large_blocked = false;
        while i < self.queue.len() {
            let w = &self.queue[i];
            let fits = w.need <= self.free;
            let fits_large = w.need_large <= self.free_large && !(large_blocked && w.need_large > 0);
            if fits && fits_large {
                let w = self.queue.remove(i).expect("in range");
                // charged even if the waiter is gone: its guard hands it back
                self.free -= w.need;
                self.free_large -= w.need_large;
                let _ = w.tx.send(());
                continue;
            }
            if !fits {
                return;
            }
            large_blocked = true;
            i += 1;
        }
    }
}

impl ImportBudget {
    /// `wait`: how long an import waits to be admitted.
    pub fn new(total: u64, wait: Duration) -> Arc<ImportBudget> {
        let total = total.max(MIN_WORKING_SET);
        let large = total / 2;
        metrics::IMPORT_BUDGET_BYTES.set(total as i64);
        Arc::new(ImportBudget {
            total,
            large,
            wait,
            st: parking_lot::Mutex::new(Gate { free: total, free_large: large, ..Default::default() }),
            buffered: tokio::sync::Semaphore::new(1),
        })
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    /// Bytes reserved by imports now.
    pub fn reserved(&self) -> u64 {
        self.total - self.st.lock().free
    }

    /// (from the budget, from the large share) for a working set: an import
    /// larger than the budget takes all of it.
    fn split(&self, bytes: u64) -> (u64, u64) {
        let need = bytes.min(self.total);
        (need, need.saturating_sub(SMALL_SHARE).min(self.large))
    }

    /// Waits up to `wait` for `need` bytes (`need_large` of them from the
    /// large share). Growth goes ahead of admissions: an import that already
    /// runs holds memory others wait for.
    async fn acquire(&self, need: u64, need_large: u64, grow: bool, wait: Duration) -> bool {
        if need == 0 && need_large == 0 {
            return true;
        }
        let (tx, mut rx) = oneshot::channel();
        let id = {
            let mut g = self.st.lock();
            let id = g.next;
            g.next += 1;
            let at = if grow { g.queue.iter().take_while(|w| w.grow).count() } else { g.queue.len() };
            g.queue.insert(at, Waiter { id, need, need_large, grow, tx });
            g.grant();
            id
        };
        if rx.try_recv().is_ok() {
            return true;
        }
        // Dropped (the request went away) while queued: leave the queue, or
        // hand back what was granted meanwhile.
        struct Queued<'a> {
            b: &'a ImportBudget,
            id: u64,
            need: u64,
            need_large: u64,
            armed: bool,
        }
        impl Drop for Queued<'_> {
            fn drop(&mut self) {
                if self.armed && !self.b.leave(self.id) {
                    self.b.release(self.need, self.need_large);
                }
            }
        }
        let mut q = Queued { b: self, id, need, need_large, armed: true };
        let granted = match tokio::time::timeout(wait, rx).await {
            Ok(r) => r.is_ok(),
            Err(_) => !self.leave(id),
        };
        q.armed = false;
        granted
    }

    /// Takes a waiter out of the queue; false if it was granted already.
    fn leave(&self, id: u64) -> bool {
        let mut g = self.st.lock();
        let Some(i) = g.queue.iter().position(|w| w.id == id) else { return false };
        g.queue.remove(i);
        // the line may have waited on this one
        g.grant();
        true
    }

    fn release(&self, n: u64, n_large: u64) {
        if n == 0 && n_large == 0 {
            return;
        }
        let mut g = self.st.lock();
        g.free += n;
        g.free_large += n_large;
        g.grant();
    }

    /// Admits an import declaring `declared` bytes (None: unknown), waiting
    /// its turn (30 s by default); then a retryable 503.
    pub async fn admit(self: &Arc<Self>, declared: Option<u64>) -> XResult<Arc<Reservation>> {
        let car = declared.unwrap_or(UNKNOWN_LENGTH);
        let (need, need_large) = self.split(sizing(car).working_set);
        let t = Instant::now();
        let waiting = metrics::IMPORTS.with_label_values(&["waiting"]);
        waiting.inc();
        let ok = self.acquire(need, need_large, false, self.wait).await;
        waiting.dec();
        let waited = t.elapsed();
        if waited > Duration::from_millis(1) {
            metrics::IMPORT_WAIT_SECONDS.with_label_values(&["admit"]).observe(waited.as_secs_f64());
        }
        if !ok {
            metrics::IMPORT_ADMISSIONS.with_label_values(&["rejected"]).inc();
            return Err(overloaded());
        }
        metrics::IMPORT_ADMISSIONS.with_label_values(&[if waited > Duration::from_millis(1) { "waited" } else { "admitted" }]).inc();
        metrics::IMPORTS.with_label_values(&["running"]).inc();
        metrics::IMPORT_RESERVED_BYTES.add(need as i64);
        Ok(Arc::new(Reservation {
            budget: self.clone(),
            held: parking_lot::Mutex::new((need, need_large)),
            car: AtomicU64::new(car),
            buffered: AtomicBool::new(false),
            grow: tokio::sync::Mutex::new(()),
        }))
    }
}

fn overloaded() -> XrpcError {
    XrpcError::unavailable("Overloaded", "too many repo imports in progress; retry shortly")
}

/// One import's share of the budget, returned when the last holder (the
/// staging, the body pump, the parse thread) drops it.
pub struct Reservation {
    budget: Arc<ImportBudget>,
    /// (from the budget, from the large share).
    held: parking_lot::Mutex<(u64, u64)>,
    /// The CAR bytes the reservation covers.
    car: AtomicU64,
    /// Sized for the buffered fallback.
    buffered: AtomicBool,
    grow: tokio::sync::Mutex<()>,
}

impl Reservation {
    pub fn car(&self) -> u64 {
        self.car.load(Ordering::Relaxed)
    }

    pub fn sizing(&self) -> Sizing {
        sizing(self.car())
    }

    pub(super) fn budget(&self) -> &ImportBudget {
        &self.budget
    }

    pub fn held(&self) -> u64 {
        self.held.lock().0
    }

    fn working_set(&self, car: u64) -> u64 {
        match self.buffered.load(Ordering::Relaxed) {
            true => buffered_working_set(car),
            false => sizing(car).working_set,
        }
    }

    /// Covers `received` body bytes: past what it covers (no Content-Length,
    /// or a wrong one), it grows to half again what arrived, waiting ahead
    /// of new imports up to 10 s; then the import fails with a retryable
    /// 503.
    pub async fn cover(&self, received: u64) -> XResult<()> {
        if received <= self.car() {
            return Ok(());
        }
        self.grow_to(received + received / 2).await
    }

    /// Sizes the reservation for the buffered fallback of a `car`-byte body
    /// (at least what it covers now).
    pub async fn buffered(&self, car: u64) -> XResult<()> {
        self.buffered.store(true, Ordering::Relaxed);
        self.grow_to(car.max(self.car())).await
    }

    async fn grow_to(&self, car: u64) -> XResult<()> {
        let _one = self.grow.lock().await;
        let (want, want_large) = self.budget.split(self.working_set(car));
        let (held, held_large) = *self.held.lock();
        let (n, n_large) = (want.saturating_sub(held), want_large.saturating_sub(held_large));
        let t = Instant::now();
        if !self.budget.acquire(n, n_large, true, GROW_WAIT.min(self.budget.wait)).await {
            metrics::IMPORT_GROWTHS.with_label_values(&["rejected"]).inc();
            return Err(overloaded());
        }
        let waited = t.elapsed();
        if n > 0 || n_large > 0 {
            if waited > Duration::from_millis(1) {
                metrics::IMPORT_WAIT_SECONDS.with_label_values(&["grow"]).observe(waited.as_secs_f64());
            }
            metrics::IMPORT_GROWTHS.with_label_values(&[if waited > Duration::from_millis(1) { "waited" } else { "granted" }]).inc();
            metrics::IMPORT_RESERVED_BYTES.add(n as i64);
            let mut h = self.held.lock();
            h.0 += n;
            h.1 += n_large;
        }
        self.car.fetch_max(car, Ordering::Relaxed);
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let (n, n_large) = *self.held.get_mut();
        self.budget.release(n, n_large);
        metrics::IMPORT_RESERVED_BYTES.sub(n as i64);
        metrics::IMPORTS.with_label_values(&["running"]).dec();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::real_dist::quantile;

    fn car(records: u32) -> u64 {
        records as u64 * CAR_PER_RECORD + 400
    }

    /// The sizing against the real distribution, and the default budgets'
    /// arithmetic: the smallest holds 64 imports at p90 and one at p99.9 at
    /// once, the large share included.
    #[test]
    fn sizing_follows_the_distribution() {
        let ws = |q: f64| sizing(car(quantile(q))).working_set;
        for q in [0.5, 0.9, 0.99, 0.999, 1.0] {
            let s = sizing(car(quantile(q)));
            println!("p{q}: {} records, CAR {} KiB -> {s:?} ({} KiB)", quantile(q), car(quantile(q)) >> 10, s.working_set >> 10);
        }
        assert_eq!(ws(0.5), MIN_WORKING_SET);
        assert!(ws(0.9) <= MIB, "{}", ws(0.9));
        assert!(ws(0.99) <= 16 * MIB, "{}", ws(0.99));
        assert!(ws(0.999) < MAX_WORKING_SET, "{}", ws(0.999));
        assert!(ws(1.0) > 70 * MIB && ws(1.0) <= MAX_WORKING_SET);
        // a repo smaller than a batch is one batch
        let s = sizing(car(300));
        assert!(s.batch_records >= 300 && s.batch_bytes as u64 >= car(300));
        // big repos keep today's batches and bloom
        let s = sizing(64 * MIB);
        assert_eq!((s.batch_bytes, s.batch_records), (4 << 20, 4096));
        assert_eq!(sizing(2 << 30).bloom_words, MAX_BLOOM_WORDS);
        let mut last = 0;
        for c in (0..40).map(|i| 1u64 << i) {
            let w = sizing(c).working_set;
            assert!(w >= last, "monotonic");
            last = w;
        }
        assert_eq!(budget_share(2560 * MIB), MIN_BUDGET);
        assert_eq!(budget_share(4 << 30), 256 * MIB);
        assert_eq!(budget_share(27 << 30), MAX_BUDGET);
        let need = 64 * ws(0.9) + ws(0.999);
        assert!(need <= MIN_BUDGET, "{} MiB", need >> 20);
        assert!(ws(0.999) - SMALL_SHARE <= MIN_BUDGET / 2);
        assert_eq!(buffered_working_set(100 * MIB), FIXED + 400 * MIB);
    }

    fn budget(total: u64) -> Arc<ImportBudget> {
        ImportBudget::new(total, Duration::from_millis(100))
    }

    #[tokio::test]
    async fn admits_in_order_and_returns_everything() {
        let b = budget(4 * MIB);
        let r1 = b.admit(Some(10 << 10)).await.ok().unwrap();
        assert_eq!(r1.held(), MIN_WORKING_SET);
        let rs: Vec<_> = futures::future::join_all((0..7).map(|_| b.admit(Some(1000)))).await;
        assert!(rs.iter().all(|r| r.is_ok()));
        assert_eq!(b.reserved(), 4 * MIB);
        // full: the next one waits its 100 ms, then 503
        let e = b.admit(None).await.err().unwrap();
        assert_eq!((e.status, e.error.as_str()), (StatusCode::SERVICE_UNAVAILABLE, "Overloaded"));
        drop(rs);
        drop(r1);
        assert_eq!(b.reserved(), 0);
        assert!(b.st.lock().queue.is_empty());
    }

    /// A large import blocked only by the large share lets small ones pass;
    /// one blocked by the budget holds the line until it fits.
    #[tokio::test]
    async fn large_imports_share_half_and_keep_their_place() {
        let b = budget(MIN_BUDGET);
        let a = b.admit(Some(40 * MIB)).await.ok().unwrap();
        assert!(a.held() > 64 * MIB && a.held() <= MAX_WORKING_SET, "{}", a.held() >> 20);
        // a second large one: over the large share, queued
        let b2 = tokio::spawn({
            let b = b.clone();
            async move { b.admit(Some(40 * MIB)).await.is_ok() }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(b.st.lock().queue.len(), 1);
        // small ones pass it
        let small = b.admit(Some(1000)).await.ok().unwrap();
        assert!(!b2.await.unwrap(), "timed out behind the first");
        drop(a);
        // a large one waiting on the budget itself: a small one behind it
        // waits too
        let fill: Vec<_> = futures::future::join_all((0..30).map(|_| b.admit(Some(MIB)))).await.into_iter().map(|r| r.ok().unwrap()).collect();
        assert!(b.total - b.reserved() < sizing(40 * MIB).working_set);
        let queued_big = tokio::spawn({
            let b = b.clone();
            async move { b.admit(Some(40 * MIB)).await.ok() }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        let queued_small = tokio::spawn({
            let b = b.clone();
            async move { b.admit(Some(1000)).await.ok() }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(b.st.lock().queue.len(), 2);
        drop(fill);
        let (qb, qs) = (queued_big.await.unwrap(), queued_small.await.unwrap());
        assert!(qb.is_some() && qs.is_some());
        drop((qb, qs, small));
        assert_eq!(b.reserved(), 0);
        assert_eq!(b.st.lock().free_large, b.large);
    }

    /// Growth goes ahead of queued admissions, and fails cleanly.
    #[tokio::test]
    async fn growth_goes_first_and_fails_cleanly() {
        let b = budget(4 * MIB);
        let r = b.admit(None).await.ok().unwrap();
        let mut fill: Vec<_> = futures::future::join_all((0..7).map(|_| b.admit(Some(1000)))).await.into_iter().map(|r| r.ok().unwrap()).collect();
        let waiter = tokio::spawn({
            let b = b.clone();
            async move { b.admit(Some(1000)).await.is_ok() }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        // a body past its estimate grows ahead of the waiter once there is room
        let grow = tokio::spawn({
            let r = r.clone();
            async move { r.cover(300 << 10).await.is_ok() }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        fill.truncate(4);
        assert!(grow.await.unwrap());
        assert!(r.held() > MIN_WORKING_SET && r.car() >= 300 << 10);
        assert!(!waiter.await.unwrap(), "the growth took the room");
        // past the budget: a clean 503, the reservation as it was
        let held = r.held();
        assert!(r.cover(64 << 20).await.is_err());
        assert_eq!(r.held(), held);
        drop((r, fill));
        assert_eq!(b.reserved(), 0);
        assert!(b.st.lock().queue.is_empty());
    }

    /// A request dropped while queued leaves no trace.
    #[tokio::test]
    async fn cancelled_waiters_leak_nothing() {
        let b = budget(2 * MIB);
        let held: Vec<_> = futures::future::join_all((0..4).map(|_| b.admit(Some(1000)))).await.into_iter().map(|r| r.ok().unwrap()).collect();
        let t = tokio::spawn({
            let b = b.clone();
            async move { b.admit(Some(1000)).await.is_ok() }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        t.abort();
        let _ = t.await;
        assert!(b.st.lock().queue.is_empty());
        drop(held);
        assert_eq!(b.reserved(), 0);
    }

    /// The buffered fallback counts its body against the budget: all of it
    /// past the budget's size.
    #[tokio::test]
    async fn buffered_fallback_counts_against_the_budget() {
        let b = budget(64 * MIB);
        let r = b.admit(Some(MIB)).await.ok().unwrap();
        r.buffered(4 * MIB).await.ok().unwrap();
        assert_eq!(r.held(), buffered_working_set(4 * MIB));
        r.buffered(1 << 30).await.ok().unwrap();
        assert_eq!(r.held(), 64 * MIB);
        assert!(b.admit(Some(1000)).await.is_err());
        drop(r);
        assert_eq!(b.reserved(), 0);
    }
}
