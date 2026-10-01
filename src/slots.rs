//! Fixed hash-slot space. A DID's slot never changes (top 16 bits of
//! sha256(did)); shards own contiguous slot ranges, recorded in a versioned
//! [`Layout`], so shards split and merge online without rehashing any
//! account (DESIGN.md "Online shard split/merge").

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SLOTS: u32 = 65_536;

pub fn slot_of(did: &str) -> u16 {
    slot_of_bytes(did.as_bytes())
}

pub fn slot_of_bytes(did: &[u8]) -> u16 {
    let h = Sha256::digest(did);
    u16::from_be_bytes([h[0], h[1]])
}

/// Uniform layout: shard i owns slots [i*65536/n, (i+1)*65536/n).
pub fn shard_of_slot(slot: u16, shards: u16) -> u16 {
    (slot as u32 * shards as u32 / SLOTS) as u16
}

pub fn shard_of(did: &str, shards: u16) -> u16 {
    shard_of_slot(slot_of(did), shards)
}

/// One shard of a layout: slots [lo, hi).
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardRange {
    pub id: u16,
    pub lo: u32,
    pub hi: u32,
}

impl ShardRange {
    pub fn contains(&self, slot: u16) -> bool {
        (self.lo..self.hi).contains(&(slot as u32))
    }
}

/// A split or merge in flight (at most one per cluster).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Reshard {
    pub id: u64,
    /// Adjacent shards being replaced, in slot order (1 = split, 2 = merge).
    pub parents: Vec<u16>,
    /// Their replacement, covering the same slots.
    pub children: Vec<ShardRange>,
    /// Node completing it once every parent is frozen.
    pub driver: String,
}

impl Reshard {
    pub fn is_split(&self) -> bool {
        self.parents.len() == 1
    }
}

/// The shard map: contiguous slot ranges covering [0, 65536), each naming a
/// stable shard id (`assign/layout`, CAS on its ETag). `version` grows when
/// routing changes; `op` is a split/merge being prepared.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Layout {
    pub version: u64,
    /// In slot order.
    pub shards: Vec<ShardRange>,
    /// Next unused shard id (ids are never reused).
    pub next_id: u32,
    /// Ops planned so far (op ids).
    #[serde(default)]
    pub op_seq: u64,
    #[serde(default)]
    pub op: Option<Reshard>,
}

impl Layout {
    /// Version 1: `n` uniform ranges with ids 0..n (`shard_of_slot`).
    pub fn uniform(n: u16) -> Layout {
        let n = n.max(1);
        let shards = (0..n)
            .map(|k| {
                let r = SlotRange::new(k as u32, n as u32).expect("k < n");
                ShardRange { id: k, lo: r.lo, hi: r.hi }
            })
            .collect();
        Layout { version: 1, shards, next_id: n as u32, op_seq: 0, op: None }
    }

    /// Contiguous, covering, ids unique and below `next_id`.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.shards.is_empty(), "empty layout");
        anyhow::ensure!(self.shards[0].lo == 0 && self.shards.last().unwrap().hi == SLOTS, "layout must cover [0, 65536)");
        let mut ids = std::collections::HashSet::new();
        for w in self.shards.windows(2) {
            anyhow::ensure!(w[0].hi == w[1].lo, "layout ranges not contiguous at {}", w[0].hi);
        }
        for s in &self.shards {
            anyhow::ensure!(s.lo < s.hi, "empty range for shard {}", s.id);
            anyhow::ensure!((s.id as u32) < self.next_id, "shard id {} >= next_id", s.id);
            anyhow::ensure!(ids.insert(s.id), "duplicate shard id {}", s.id);
        }
        Ok(())
    }

    /// Index (in slot order) of the shard holding `slot`.
    pub fn index_of_slot(&self, slot: u16) -> usize {
        self.shards.partition_point(|r| r.hi <= slot as u32).min(self.shards.len() - 1)
    }

    pub fn shard_of_slot(&self, slot: u16) -> u16 {
        self.shards[self.index_of_slot(slot)].id
    }

    /// Shard owning a routing key (DID or private routing key).
    pub fn shard_of(&self, key: &str) -> u16 {
        self.shard_of_slot(slot_of(key))
    }

    pub fn range_of(&self, id: u16) -> Option<ShardRange> {
        self.shards.iter().find(|r| r.id == id).copied()
    }

    pub fn contains(&self, id: u16) -> bool {
        self.shards.iter().any(|r| r.id == id)
    }

    pub fn ids(&self) -> Vec<u16> {
        self.shards.iter().map(|r| r.id).collect()
    }

    /// The shard after `id` in slot order.
    pub fn next_after(&self, id: u16) -> Option<u16> {
        let i = self.shards.iter().position(|r| r.id == id)?;
        self.shards.get(i + 1).map(|r| r.id)
    }

    /// Plans splitting `id` at slot `at` (default: the midpoint).
    pub fn plan_split(&self, id: u16, at: Option<u32>, driver: &str) -> anyhow::Result<Reshard> {
        anyhow::ensure!(self.op.is_none(), "a reshard is already in progress");
        let r = self.range_of(id).ok_or_else(|| anyhow::anyhow!("no shard {id} in layout v{}", self.version))?;
        anyhow::ensure!(r.hi - r.lo >= 2, "shard {id} holds a single slot");
        let at = at.unwrap_or(r.lo + (r.hi - r.lo) / 2);
        anyhow::ensure!(r.lo < at && at < r.hi, "split point {at} outside ({}, {})", r.lo, r.hi);
        anyhow::ensure!(self.next_id + 2 <= SLOTS, "shard ids exhausted");
        let (a, b) = (self.next_id as u16, self.next_id as u16 + 1);
        Ok(Reshard {
            id: self.op_seq + 1,
            parents: vec![id],
            children: vec![ShardRange { id: a, lo: r.lo, hi: at }, ShardRange { id: b, lo: at, hi: r.hi }],
            driver: driver.to_string(),
        })
    }

    /// Plans merging adjacent shards `left` and `right` (slot order).
    pub fn plan_merge(&self, left: u16, right: u16, driver: &str) -> anyhow::Result<Reshard> {
        anyhow::ensure!(self.op.is_none(), "a reshard is already in progress");
        let i = self.shards.iter().position(|r| r.id == left).ok_or_else(|| anyhow::anyhow!("no shard {left} in layout v{}", self.version))?;
        let r = self.shards.get(i + 1).filter(|r| r.id == right).ok_or_else(|| anyhow::anyhow!("shard {right} does not follow {left}"))?;
        anyhow::ensure!(self.next_id < SLOTS, "shard ids exhausted");
        Ok(Reshard {
            id: self.op_seq + 1,
            parents: vec![left, right],
            children: vec![ShardRange { id: self.next_id as u16, lo: self.shards[i].lo, hi: r.hi }],
            driver: driver.to_string(),
        })
    }

    /// This layout with `op` planned. Its children's ids are used up now,
    /// whether it flips or is aborted: a child's state is cloned from its
    /// parents as frozen *for this op*, so a later op must never find (and
    /// reuse) an aborted op's clone under the same id.
    pub fn with_op(&self, op: Reshard) -> Layout {
        let next_id = op.children.iter().map(|c| c.id as u32 + 1).fold(self.next_id, u32::max);
        Layout { op_seq: op.id, op: Some(op), next_id, ..self.clone() }
    }

    /// The next version: `op`'s parents replaced by its children.
    pub fn flipped(&self, op: &Reshard) -> anyhow::Result<Layout> {
        let first = self.shards.iter().position(|r| r.id == op.parents[0]).ok_or_else(|| anyhow::anyhow!("parent {} not in layout", op.parents[0]))?;
        for (k, p) in op.parents.iter().enumerate() {
            anyhow::ensure!(self.shards.get(first + k).is_some_and(|r| r.id == *p), "parents not adjacent in layout");
        }
        let mut shards = self.shards.clone();
        shards.splice(first..first + op.parents.len(), op.children.iter().copied());
        let next_id = op.children.iter().map(|c| c.id as u32 + 1).fold(self.next_id, u32::max);
        let l = Layout { version: self.version + 1, shards, next_id, op_seq: self.op_seq, op: None };
        l.validate()?;
        Ok(l)
    }
}

/// Shard `k` of `n` of the slot space (`subscribeRepos?shard=k/n`): the
/// slots `s` with `s * n / 65536 == k`, i.e. what `shard_of_slot` puts in
/// shard k of an n-shard layout. For n dividing a cluster's shard count (or
/// vice versa) the boundaries line up with the cluster's shards.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotRange {
    pub k: u32,
    pub n: u32,
    /// slots [lo, hi)
    pub lo: u32,
    pub hi: u32,
}

impl SlotRange {
    /// k < n <= 65,536.
    pub fn new(k: u32, n: u32) -> Option<SlotRange> {
        if n == 0 || n > SLOTS || k >= n {
            return None;
        }
        // first slot s with s * n >= k * SLOTS
        let first = |k: u32| ((k as u64 * SLOTS as u64).div_ceil(n as u64)) as u32;
        Some(SlotRange { k, n, lo: first(k), hi: first(k + 1) })
    }

    /// "k/n" (decimal, no signs or spaces).
    pub fn parse(s: &str) -> Option<SlotRange> {
        let (k, n) = s.split_once('/')?;
        let num = |x: &str| x.bytes().all(|b| b.is_ascii_digit()).then(|| x.parse::<u32>().ok()).flatten();
        SlotRange::new(num(k)?, num(n)?)
    }

    pub fn contains(&self, slot: u16) -> bool {
        (self.lo..self.hi).contains(&(slot as u32))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn uniform_ranges() {
        assert_eq!(shard_of_slot(0, 256), 0);
        assert_eq!(shard_of_slot(255, 256), 0);
        assert_eq!(shard_of_slot(256, 256), 1);
        assert_eq!(shard_of_slot(65535, 256), 255);
        // a 2-way split of every shard keeps each slot inside its parent's range
        for s in [0u16, 1000, 40000, 65535] {
            assert_eq!(shard_of_slot(s, 512) / 2, shard_of_slot(s, 256));
        }
    }

    #[test]
    fn slot_ranges_partition_the_space() {
        for n in [1u32, 2, 3, 7, 16, 256, 1000, 65_535, 65_536] {
            let ranges: Vec<SlotRange> = (0..n).map(|k| SlotRange::new(k, n).unwrap()).collect();
            // contiguous and covering
            assert_eq!((ranges[0].lo, ranges[n as usize - 1].hi), (0, SLOTS));
            for w in ranges.windows(2) {
                assert_eq!(w[0].hi, w[1].lo);
            }
            // range k holds exactly the slots shard_of_slot puts in shard k
            for s in 0..=u16::MAX {
                let k = (s as u64 * n as u64 / SLOTS as u64) as usize;
                assert!(ranges[k].contains(s), "slot {s} of {n}");
                if n < SLOTS {
                    assert_eq!(k, shard_of_slot(s, n as u16) as usize);
                }
            }
        }
        assert_eq!(SlotRange::parse("3/16"), SlotRange::new(3, 16));
        // the uniform layout is shard_of_slot's
        let l = Layout::uniform(7);
        l.validate().unwrap();
        for s in [0u16, 9362, 9363, 30000, 65535] {
            assert_eq!(l.shard_of_slot(s), shard_of_slot(s, 7));
        }
        for bad in ["", "1", "1/", "/2", "2/2", "0/0", "-1/2", "+1/2", " 1/2", "1/65537", "1/2/3", "a/b"] {
            assert_eq!(SlotRange::parse(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn split_and_merge_regroup_slots() {
        let l = Layout::uniform(4);
        let op = l.plan_split(1, None, "n").unwrap();
        assert_eq!(op.children, vec![ShardRange { id: 4, lo: 16384, hi: 24576 }, ShardRange { id: 5, lo: 24576, hi: 32768 }]);
        let l2 = l.with_op(op.clone());
        assert!(l2.plan_split(0, None, "n").is_err(), "one op at a time");
        let l2 = l2.flipped(&op).unwrap();
        assert_eq!((l2.version, l2.ids(), l2.next_id, l2.op.clone()), (2, vec![0, 4, 5, 2, 3], 6, None));
        assert_eq!(l2.shard_of_slot(16384), 4);
        assert_eq!(l2.shard_of_slot(24575), 4);
        assert_eq!(l2.shard_of_slot(24576), 5);
        assert!(l2.plan_merge(4, 2, "n").is_err(), "not adjacent");
        let m = l2.plan_merge(5, 2, "n").unwrap();
        assert_eq!(m.children, vec![ShardRange { id: 6, lo: 24576, hi: 49152 }]);
        let l3 = l2.with_op(m.clone()).flipped(&m).unwrap();
        assert_eq!((l3.ids(), l3.next_id), (vec![0, 4, 6, 3], 7));
        // an aborted op's ids stay used: the next op gets fresh ones
        let aborted = Layout { op: None, ..l3.with_op(l3.plan_split(0, None, "n").unwrap()) };
        assert_eq!(aborted.plan_split(0, None, "n").unwrap().children[0].id, 9);
        assert!(l.plan_split(9, None, "n").is_err());
        assert!(l.plan_split(0, Some(0), "n").is_err());
        let one = Layout { shards: vec![ShardRange { id: 0, lo: 0, hi: 1 }, ShardRange { id: 1, lo: 1, hi: SLOTS }], next_id: 2, ..Layout::uniform(1) };
        one.validate().unwrap();
        assert!(one.plan_split(0, None, "n").is_err(), "a single slot can't split");
    }
}
