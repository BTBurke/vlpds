//! Account totals for the operator dashboard (DESIGN.md "Account totals"):
//! accounts by status, and repos by the UTC day of their latest commit,
//! kept exact under every change instead of counted by a periodic scan.
//!
//! Each slot's totals are one row, `0x01 ‖ slot ‖ T/`, so they split, merge
//! and move with the slot like every other key. A repo's worker knows its
//! account's status and head before and after each change, and puts the
//! [`Delta`] on the log entry. The sequencer (the one place a shard's
//! entries are ordered) folds it into the shard's in-memory totals and
//! appends the slot's new row to the entry's mutations, so the row lands in
//! the same batch as the change. Rows are absolute, not increments: replaying
//! an entry twice is harmless, as for every other mutation.
//!
//! A shard opens without reading the rows (one seek per slot through every
//! L0 and sorted run: seconds after a day of writes). Until the background
//! load is done, a change is written as a delta row keyed by its entry's
//! seq, `0x01 ‖ slot ‖ T/ ‖ seq`, which is just as idempotent; the load adds
//! a slot's delta rows to its row, and the slot's next row write deletes
//! them.

use crate::segment::Mutation;
use crate::state::{self, Account, Head};
use crate::tid::Tid;
use bytes::{BufMut, Bytes};
use std::collections::HashMap;

pub const STATUSES: [&str; 5] = ["active", "deactivated", "takendown", "suspended", "other"];

/// (label, days): a window counts the repos whose latest commit's UTC day
/// is at most `days` days before today's, so "1d" is yesterday and today.
pub const WINDOWS: [(&str, u32); 3] = [("1d", 1), ("7d", 7), ("30d", 30)];

/// Days a row keeps, past the widest window: a shard's next owner whose
/// clock is a little behind still finds every day of its windows.
const KEEP_DAYS: u32 = 32;

const DAY_MICROS: u64 = 86_400_000_000;

pub const FAMILY: &[u8] = b"T/";

pub fn key(slot: u16) -> Vec<u8> {
    state::slot_family(slot, FAMILY)
}

pub fn status_index(status: Option<&str>) -> u8 {
    match status {
        None => 0,
        Some(s) => STATUSES[1..4].iter().position(|k| *k == s).map_or(4, |i| i as u8 + 1),
    }
}

pub fn day_of(rev: Tid) -> u32 {
    (rev.micros() / DAY_MICROS) as u32
}

pub fn today() -> u32 {
    (crate::tid::now_micros() / DAY_MICROS) as u32
}

fn cutoff(today: u32) -> u32 {
    today.saturating_sub(KEEP_DAYS)
}

/// What one repo counts toward.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RepoKey {
    pub status: u8,
    pub day: u32,
}

impl RepoKey {
    /// None: the account is gone (deleted).
    pub fn of(account: &Account, head: &Head) -> Option<RepoKey> {
        (account.status.as_deref() != Some("deleted"))
            .then(|| RepoKey { status: status_index(account.status.as_deref()), day: day_of(head.rev) })
    }
}

/// One repo's change: before -> after (None = no account).
#[derive(Clone, Copy, Debug)]
pub struct Delta {
    pub slot: u16,
    pub before: Option<RepoKey>,
    pub after: Option<RepoKey>,
}

impl Delta {
    /// None when nothing it counts toward changed.
    pub fn new(did: &str, before: Option<RepoKey>, after: Option<RepoKey>) -> Option<Delta> {
        (before != after).then(|| Delta { slot: crate::slots::slot_of(did), before, after })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Totals {
    /// By [`STATUSES`] index.
    pub accounts: [i64; 5],
    /// (UTC day, repos whose latest commit is on it), ascending; days before
    /// the cutoff linger until the row's next write but are never counted.
    pub days: Vec<(u32, i64)>,
}

impl Totals {
    fn add(&mut self, k: RepoKey, n: i64, cutoff: u32) {
        self.accounts[k.status as usize] += n;
        if k.day < cutoff {
            return;
        }
        match self.days.binary_search_by_key(&k.day, |d| d.0) {
            Ok(i) => {
                self.days[i].1 += n;
                if self.days[i].1 == 0 {
                    self.days.remove(i);
                }
            }
            Err(i) => self.days.insert(i, (k.day, n)),
        }
    }

    fn prune(&mut self, cutoff: u32) {
        self.days.retain(|d| d.0 >= cutoff);
    }

    pub fn merge(&mut self, o: &Totals) {
        for (a, b) in self.accounts.iter_mut().zip(o.accounts) {
            *a += b;
        }
        for &(day, n) in &o.days {
            match self.days.binary_search_by_key(&day, |d| d.0) {
                Ok(i) => self.days[i].1 += n,
                Err(i) => self.days.insert(i, (day, n)),
            }
        }
        self.days.retain(|d| d.1 != 0);
    }

    /// Every account has a repo.
    pub fn repos(&self) -> i64 {
        self.accounts.iter().sum()
    }

    /// Repos whose latest commit is within `days` UTC days of `today` (or later).
    pub fn written_within(&self, days: u32, today: u32) -> i64 {
        let from = today.saturating_sub(days);
        self.days.iter().filter(|d| d.0 >= from).map(|d| d.1).sum()
    }

    /// Zigzag varints: the five status counts, the number of days, then
    /// per day its distance from the previous one (the first: absolute)
    /// and its count. ~100 bytes for a slot active on every day kept.
    pub fn encode(&self) -> Bytes {
        let mut b = Vec::with_capacity(16 + 6 * self.days.len());
        for a in self.accounts {
            put_varint(&mut b, zigzag(a));
        }
        put_varint(&mut b, self.days.len() as u64);
        let mut prev = 0u32;
        for &(day, n) in &self.days {
            put_varint(&mut b, (day - prev) as u64);
            put_varint(&mut b, zigzag(n));
            prev = day;
        }
        b.into()
    }

    pub fn decode(mut b: &[u8]) -> anyhow::Result<Totals> {
        let mut t = Totals::default();
        for a in &mut t.accounts {
            *a = unzigzag(get_varint(&mut b)?);
        }
        let n = get_varint(&mut b)? as usize;
        anyhow::ensure!(n <= 1 << 16, "totals row: {n} days");
        let mut day = 0u32;
        for _ in 0..n {
            day = day
                .checked_add(u32::try_from(get_varint(&mut b)?)?)
                .ok_or_else(|| anyhow::anyhow!("totals row: day overflow"))?;
            t.days.push((day, unzigzag(get_varint(&mut b)?)));
        }
        anyhow::ensure!(b.is_empty(), "totals row: {} trailing bytes", b.len());
        Ok(t)
    }
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn unzigzag(v: u64) -> i64 {
    (v >> 1) as i64 ^ -((v & 1) as i64)
}

fn put_varint(b: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        b.put_u8(v as u8 | 0x80);
        v >>= 7;
    }
    b.put_u8(v as u8);
}

fn get_varint(b: &mut &[u8]) -> anyhow::Result<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = b.split_first().ok_or_else(|| anyhow::anyhow!("totals row: truncated"))?;
        *b = rest;
        v |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(v);
        }
    }
    anyhow::bail!("totals row: varint too long")
}

/// A delta row's key: the slot's row key, then the seq of the entry that
/// wrote it (unique: seqs only grow across a shard's owners).
fn delta_key(slot: u16, seq: i64) -> Bytes {
    let mut k = key(slot);
    k.extend_from_slice(&seq.to_be_bytes());
    k.into()
}

/// A loaded slot: its totals, and the delta rows the DB still holds (or
/// will, once their entries apply), deleted with the next row write.
#[derive(Default)]
struct Slot {
    row: Totals,
    deltas: Vec<Bytes>,
}

/// A shard's totals as of the last entry the sequencer took. They load in
/// the background after the shard opens (`spawn_load`); until then a slot's
/// change is written as a delta row next to its row, and the load adds the
/// slot's delta rows (those in the DB and those taken since the open) to
/// its row.
pub struct ShardTotals {
    slots: HashMap<u16, Slot>,
    /// Delta rows taken for slots not loaded yet.
    pending: HashMap<u16, Vec<(Bytes, Totals)>>,
    /// Over the loaded slots; every slot once `loaded`.
    sum: Totals,
    loaded: bool,
}

impl Default for ShardTotals {
    /// Loaded, with no rows: a shard with nothing in it yet.
    fn default() -> ShardTotals {
        ShardTotals { slots: HashMap::new(), pending: HashMap::new(), sum: Totals::default(), loaded: true }
    }
}

impl ShardTotals {
    /// For a shard opened over existing state, before [`spawn_load`].
    pub fn unloaded() -> ShardTotals {
        ShardTotals { loaded: false, ..Default::default() }
    }

    /// Every slot's rows: (slot, its row, its delta rows). One family scan;
    /// delta rows sort right after their slot's row.
    pub async fn read<R: slatedb::DbReadOps + ?Sized>(
        db: &R,
    ) -> anyhow::Result<Vec<(u16, Option<Totals>, Vec<(Bytes, Totals)>)>> {
        let opts = slatedb::config::ScanOptions { read_ahead_bytes: 1 << 20, max_fetch_tasks: 2, ..Default::default() };
        let mut scan = state::FamilyScan::new(db, FAMILY, None, &opts).await?;
        let mut out: Vec<(u16, Option<Totals>, Vec<(Bytes, Totals)>)> = Vec::new();
        while let Some(kv) = scan.next().await? {
            let Some(slot) = state::key_slot(&kv.key) else { continue };
            let body = state::key_body(&kv.key);
            let row = Totals::decode(&kv.value).map_err(|e| e.context(format!("slot {slot}")))?;
            if out.last().is_none_or(|o| o.0 != slot) {
                out.push((slot, None, Vec::new()));
            }
            let o = out.last_mut().unwrap();
            match body.len() - FAMILY.len() {
                0 => o.1 = Some(row),
                8 => o.2.push((kv.key, row)),
                n => anyhow::bail!("slot {slot}: totals key with a {n}-byte suffix"),
            }
        }
        Ok(out)
    }

    /// Installs what [`ShardTotals::read`] found, read after the shard opened.
    /// A slot's row only changes once it is loaded, and until then its
    /// delta rows only accumulate, all of them in `pending` since the open:
    /// so the read plus the pending rows it missed is the slot's state.
    pub fn install(&mut self, rows: Vec<(u16, Option<Totals>, Vec<(Bytes, Totals)>)>, today: u32) {
        if self.loaded {
            return;
        }
        let cut = cutoff(today);
        for (slot, base, deltas) in rows {
            let pending = self.pending.remove(&slot).unwrap_or_default();
            self.install_slot(slot, base, deltas, pending, cut);
        }
        for (slot, pending) in std::mem::take(&mut self.pending) {
            self.install_slot(slot, None, Vec::new(), pending, cut);
        }
        self.sum.prune(cut);
        self.loaded = true;
    }

    fn install_slot(
        &mut self,
        slot: u16,
        base: Option<Totals>,
        deltas: Vec<(Bytes, Totals)>,
        pending: Vec<(Bytes, Totals)>,
        cut: u32,
    ) {
        let mut s = Slot { row: base.unwrap_or_default(), deltas: Vec::new() };
        for (k, d) in deltas.into_iter().chain(pending) {
            if !s.deltas.contains(&k) {
                s.row.merge(&d);
                s.deltas.push(k);
            }
        }
        s.row.prune(cut);
        self.sum.merge(&s.row);
        self.slots.insert(slot, s);
    }

    /// Appends the slot's new row (and the deletes of its delta rows) to
    /// `muts`, or a delta row while the shard's totals are loading. `seq`
    /// is the entry's.
    pub fn apply(&mut self, d: &Delta, today: u32, seq: i64, muts: &mut Vec<Mutation>) {
        let cut = cutoff(today);
        if !self.loaded {
            let mut delta = Totals::default();
            for (k, n) in [(d.before, -1), (d.after, 1)] {
                if let Some(k) = k {
                    delta.add(k, n, cut);
                }
            }
            let k = delta_key(d.slot, seq);
            muts.push(Mutation { key: k.clone(), val: Some(delta.encode()) });
            self.pending.entry(d.slot).or_default().push((k, delta));
            return;
        }
        let s = self.slots.entry(d.slot).or_default();
        for (k, n) in [(d.before, -1), (d.after, 1)] {
            if let Some(k) = k {
                s.row.add(k, n, cut);
                self.sum.add(k, n, cut);
            }
        }
        s.row.prune(cut);
        self.sum.prune(cut);
        muts.push(Mutation { key: key(d.slot).into(), val: Some(s.row.encode()) });
        muts.extend(s.deltas.drain(..).map(|key| Mutation { key, val: None }));
    }

    /// None while loading.
    pub fn sum(&self) -> Option<&Totals> {
        self.loaded.then_some(&self.sum)
    }
}

/// Nodes whose totals loads wait (tests: writes while loading).
static HELD: std::sync::LazyLock<parking_lot::Mutex<std::collections::HashSet<String>>> =
    std::sync::LazyLock::new(Default::default);

/// Until dropped, node `node_id`'s totals loads wait before reading.
#[doc(hidden)]
pub fn hold_loads(node_id: &str) -> HeldLoads {
    HELD.lock().insert(node_id.to_string());
    HeldLoads(node_id.to_string())
}

#[doc(hidden)]
pub struct HeldLoads(String);

impl Drop for HeldLoads {
    fn drop(&mut self) {
        HELD.lock().remove(&self.0);
    }
}

/// Loads `sink`'s totals in the background, retrying until they load or the
/// shard closes.
pub fn spawn_load(sink: &std::sync::Arc<crate::nodelog::ShardSink>, node_id: &str) {
    let weak = std::sync::Arc::downgrade(sink);
    let (id, db, node_id) = (sink.id, sink.db.clone(), node_id.to_string());
    tokio::spawn(async move {
        let started = std::time::Instant::now();
        let mut backoff = std::time::Duration::from_millis(200);
        while HELD.lock().contains(&node_id) {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        loop {
            match ShardTotals::read(db.as_ref()).await {
                Ok(rows) => {
                    let Some(sink) = weak.upgrade() else { return };
                    sink.totals.lock().install(rows, today());
                    crate::metrics::TOTALS_LOAD_SECONDS.observe(started.elapsed().as_secs_f64());
                    return;
                }
                Err(e) => {
                    if weak.upgrade().is_none_or(|s| s.barrier_taken()) {
                        return;
                    }
                    tracing::warn!(shard = id.0, "loading account totals: {e:#}");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(std::time::Duration::from_secs(10));
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses() {
        assert_eq!(status_index(None), 0);
        assert_eq!(status_index(Some("deactivated")), 1);
        assert_eq!(status_index(Some("takendown")), 2);
        assert_eq!(status_index(Some("suspended")), 3);
        assert_eq!(status_index(Some("deleted")), 4);
    }

    #[test]
    fn rows_round_trip() {
        for t in [
            Totals::default(),
            Totals { accounts: [1, 0, 7, -3, i64::MAX], days: vec![(20_000, 1), (20_001, -2), (20_031, 1 << 40)] },
        ] {
            assert_eq!(Totals::decode(&t.encode()).unwrap(), t);
        }
        let mut b = Totals { accounts: [1; 5], days: vec![(9, 9)] }.encode().to_vec();
        b.push(0);
        assert!(Totals::decode(&b).is_err());
        assert!(Totals::decode(&b[..3]).is_err());
    }

    fn k(status: u8, day: u32) -> Option<RepoKey> {
        Some(RepoKey { status, day })
    }

    fn apply(s: &mut ShardTotals, d: Delta, today: u32) -> Vec<Mutation> {
        let mut muts = Vec::new();
        s.apply(&d, today, 0, &mut muts);
        muts
    }

    /// Rows and their sum follow creates, writes on later days, status
    /// changes and deletes; days past the cutoff are dropped, and a repo
    /// whose last write is older than that leaves no trace in the days.
    #[test]
    fn deltas_windows_and_cutoff() {
        let mut s = ShardTotals::default();
        let d = |slot, before, after| Delta { slot, before, after };
        let today = 20_000;
        apply(&mut s, d(1, None, k(0, today - 40)), today);
        apply(&mut s, d(1, None, k(0, today - 3)), today);
        apply(&mut s, d(2, None, k(0, today)), today);
        apply(&mut s, d(2, k(0, today), k(1, today)), today);
        apply(&mut s, d(2, None, k(2, today - 1)), today);
        let m = apply(&mut s, d(1, k(0, today - 3), k(0, today)), today);
        assert_eq!(
            Totals::decode(m[0].val.as_ref().unwrap()).unwrap(),
            Totals { accounts: [2, 0, 0, 0, 0], days: vec![(today, 1)] }
        );
        let sum = s.sum().unwrap().clone();
        assert_eq!(sum.accounts, [2, 1, 1, 0, 0]);
        assert_eq!(sum.repos(), 4);
        assert_eq!(sum.written_within(0, today), 2);
        assert_eq!(sum.written_within(1, today), 3);
        assert_eq!(sum.written_within(30, today), 3);
        // the 40-day-old repo is deleted: only its status count moves
        apply(&mut s, d(1, k(0, today - 40), None), today);
        assert_eq!(s.sum().unwrap().accounts, [1, 1, 1, 0, 0]);
        assert_eq!(s.sum().unwrap().written_within(30, today), 3);
        // a month on, nothing is within 1d and the old days are gone
        let later = today + 33;
        apply(&mut s, d(2, k(1, today), k(3, today)), later);
        assert_eq!(s.sum().unwrap().written_within(30, later), 0);
        assert!(s.sum().unwrap().days.is_empty(), "{:?}", s.sum().unwrap().days);
        assert_eq!(s.sum().unwrap().accounts, [1, 0, 1, 1, 0]);
    }

    /// What [`ShardTotals::read`] returns, from a key-value map.
    fn read(db: &std::collections::BTreeMap<Bytes, Bytes>) -> Vec<(u16, Option<Totals>, Vec<(Bytes, Totals)>)> {
        let mut out: Vec<(u16, Option<Totals>, Vec<(Bytes, Totals)>)> = Vec::new();
        for (k, v) in db {
            let slot = state::key_slot(k).unwrap();
            if out.last().is_none_or(|o| o.0 != slot) {
                out.push((slot, None, Vec::new()));
            }
            let o = out.last_mut().unwrap();
            let row = Totals::decode(v).unwrap();
            match state::key_body(k).len() - FAMILY.len() {
                0 => o.1 = Some(row),
                _ => o.2.push((k.clone(), row)),
            }
        }
        out
    }

    /// Randomized deltas against the per-repo truth, through reopens of the
    /// shard whose totals load at a random later point (from a read taken
    /// before more deltas, as the background load's scan can be), and
    /// sometimes never before the next reopen.
    #[test]
    fn matches_truth_through_lazy_loads() {
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        let mut repos: HashMap<u32, RepoKey> = HashMap::new();
        let mut db = std::collections::BTreeMap::<Bytes, Bytes>::new();
        let mut s = ShardTotals::default();
        let mut snapshot: Option<Vec<_>> = None;
        let mut today = 20_000u32;
        let mut seq = 0i64;
        for step in 0..30_000u32 {
            if rng.gen_ratio(1, 500) {
                today += 1;
            }
            if rng.gen_ratio(1, 700) {
                s = ShardTotals::unloaded();
                snapshot = None;
            }
            if s.sum().is_none() {
                if snapshot.is_none() && rng.gen_ratio(1, 20) {
                    snapshot = Some(read(&db));
                } else if snapshot.is_some() && rng.gen_ratio(1, 20) {
                    s.install(snapshot.take().unwrap(), today);
                }
            }
            let id = rng.gen_range(0..300u32);
            let slot = (id % 17) as u16;
            let before = repos.get(&id).copied();
            let after = if rng.gen_ratio(1, 10) {
                None
            } else {
                Some(RepoKey { status: rng.gen_range(0..5), day: today - rng.gen_range(0..2) })
            };
            match after {
                Some(a) => repos.insert(id, a),
                None => repos.remove(&id),
            };
            if before != after {
                seq += 1;
                let mut muts = Vec::new();
                s.apply(&Delta { slot, before, after }, today, seq, &mut muts);
                for m in muts {
                    match m.val {
                        Some(v) => db.insert(m.key, v),
                        None => db.remove(&m.key),
                    };
                }
            }
            if step % 997 == 0 || step == 29_999 {
                if s.sum().is_none() {
                    s.install(read(&db), today);
                    snapshot = None;
                }
                let mut want = Totals::default();
                for r in repos.values() {
                    want.accounts[r.status as usize] += 1;
                }
                let sum = s.sum().unwrap();
                assert_eq!(sum.accounts, want.accounts, "step {step}");
                for (_, days) in WINDOWS {
                    let n = repos.values().filter(|r| r.day + days >= today).count() as i64;
                    assert_eq!(sum.written_within(days, today), n, "step {step}");
                }
                let mut reloaded = ShardTotals::unloaded();
                reloaded.install(read(&db), today);
                assert_eq!(reloaded.sum().unwrap().accounts, want.accounts, "step {step}");
                for (_, days) in WINDOWS {
                    assert_eq!(reloaded.sum().unwrap().written_within(days, today), sum.written_within(days, today));
                }
                assert!(
                    db.len()
                        <= 17
                            + s.pending.values().map(Vec::len).sum::<usize>()
                            + s.slots.values().map(|x| x.deltas.len()).sum::<usize>()
                );
            }
        }
    }
}
