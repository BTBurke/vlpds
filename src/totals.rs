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
        (account.status.as_deref() != Some("deleted")).then(|| RepoKey { status: status_index(account.status.as_deref()), day: day_of(head.rev) })
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
            day = day.checked_add(u32::try_from(get_varint(&mut b)?)?).ok_or_else(|| anyhow::anyhow!("totals row: day overflow"))?;
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

/// A shard's totals: its slots' rows as of the last entry the sequencer
/// took, and their sum. Loaded when the shard opens, after its replay.
#[derive(Default)]
pub struct ShardTotals {
    slots: HashMap<u16, Totals>,
    sum: Totals,
}

impl ShardTotals {
    pub async fn load<R: slatedb::DbReadOps + ?Sized>(db: &R) -> anyhow::Result<ShardTotals> {
        let opts = slatedb::config::ScanOptions { read_ahead_bytes: 1 << 20, max_fetch_tasks: 2, ..Default::default() };
        let mut scan = state::FamilyScan::new(db, FAMILY, None, &opts).await?;
        let mut t = ShardTotals::default();
        while let Some(kv) = scan.next().await? {
            let Some(slot) = state::key_slot(&kv.key) else { continue };
            if state::key_body(&kv.key) != FAMILY {
                continue;
            }
            let row = Totals::decode(&kv.value).map_err(|e| e.context(format!("slot {slot}")))?;
            t.sum.merge(&row);
            t.slots.insert(slot, row);
        }
        t.sum.prune(cutoff(today()));
        Ok(t)
    }

    /// The slot's new row.
    pub fn apply(&mut self, d: &Delta, today: u32) -> Mutation {
        let cut = cutoff(today);
        let row = self.slots.entry(d.slot).or_default();
        for (k, n) in [(d.before, -1), (d.after, 1)] {
            if let Some(k) = k {
                row.add(k, n, cut);
                self.sum.add(k, n, cut);
            }
        }
        row.prune(cut);
        self.sum.prune(cut);
        Mutation { key: key(d.slot).into(), val: Some(row.encode()) }
    }

    pub fn sum(&self) -> &Totals {
        &self.sum
    }
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

    /// Rows and their sum follow creates, writes on later days, status
    /// changes and deletes; days past the cutoff are dropped, and a repo
    /// whose last write is older than that leaves no trace in the days.
    #[test]
    fn deltas_windows_and_cutoff() {
        let mut s = ShardTotals::default();
        let d = |slot, before, after| Delta { slot, before, after };
        let today = 20_000;
        s.apply(&d(1, None, k(0, today - 40)), today);
        s.apply(&d(1, None, k(0, today - 3)), today);
        s.apply(&d(2, None, k(0, today)), today);
        s.apply(&d(2, k(0, today), k(1, today)), today);
        s.apply(&d(2, None, k(2, today - 1)), today);
        let m = s.apply(&d(1, k(0, today - 3), k(0, today)), today);
        assert_eq!(Totals::decode(m.val.as_ref().unwrap()).unwrap(), Totals { accounts: [2, 0, 0, 0, 0], days: vec![(today, 1)] });
        let sum = s.sum().clone();
        assert_eq!(sum.accounts, [2, 1, 1, 0, 0]);
        assert_eq!(sum.repos(), 4);
        assert_eq!(sum.written_within(0, today), 2);
        assert_eq!(sum.written_within(1, today), 3);
        assert_eq!(sum.written_within(30, today), 3);
        // the 40-day-old repo is deleted: only its status count moves
        s.apply(&d(1, k(0, today - 40), None), today);
        assert_eq!(s.sum().accounts, [1, 1, 1, 0, 0]);
        assert_eq!(s.sum().written_within(30, today), 3);
        // a month on, nothing is within 1d and the old days are gone
        let later = today + 33;
        s.apply(&d(2, k(1, today), k(3, today)), later);
        assert_eq!(s.sum().written_within(30, later), 0);
        assert!(s.sum().days.is_empty(), "{:?}", s.sum().days);
        assert_eq!(s.sum().accounts, [1, 0, 1, 1, 0]);
    }

    /// Randomized deltas against the per-repo truth, through a reload from
    /// the rows.
    #[test]
    fn matches_truth() {
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        let mut repos: HashMap<u32, (u16, RepoKey)> = HashMap::new();
        let mut s = ShardTotals::default();
        let mut rows: HashMap<u16, Bytes> = HashMap::new();
        let mut today = 20_000u32;
        for step in 0..20_000u32 {
            if rng.gen_ratio(1, 500) {
                today += 1;
            }
            let id = rng.gen_range(0..300u32);
            let slot = (id % 17) as u16;
            let before = repos.get(&id).map(|r| r.1);
            let after = if rng.gen_ratio(1, 10) { None } else { Some(RepoKey { status: rng.gen_range(0..5), day: today - rng.gen_range(0..2) }) };
            match after {
                Some(a) => repos.insert(id, (slot, a)),
                None => repos.remove(&id),
            };
            if before != after {
                let m = s.apply(&Delta { slot, before, after }, today);
                rows.insert(slot, m.val.unwrap());
            }
            if step % 997 == 0 || step == 19_999 {
                let mut want = Totals::default();
                for (_, r) in repos.values() {
                    want.accounts[r.status as usize] += 1;
                }
                for (_, days) in WINDOWS {
                    let n = repos.values().filter(|(_, r)| r.day + days >= today).count() as i64;
                    assert_eq!(s.sum().written_within(days, today), n, "step {step}");
                }
                assert_eq!(s.sum().accounts, want.accounts, "step {step}");
                let mut reloaded = Totals::default();
                for r in rows.values() {
                    reloaded.merge(&Totals::decode(r).unwrap());
                }
                assert_eq!(reloaded.accounts, want.accounts);
                for (_, days) in WINDOWS {
                    assert_eq!(reloaded.written_within(days, today), s.sum().written_within(days, today));
                }
            }
        }
    }
}
