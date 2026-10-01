//! Fixed hash-slot space. A DID's slot never changes (top 16 bits of
//! sha256(did)); shards own contiguous slot ranges, so the shard count can
//! grow by splitting ranges without rehashing any account.

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

/// Shard `k` of `n` of the slot space (`subscribeRepos?shard=k/n`): the
/// slots `s` with `s * n / 65536 == k`, i.e. what `shard_of_slot` puts in
/// shard k of an n-shard layout. For n dividing a cluster's shard count (or
/// vice versa) the boundaries line up with the cluster's shards.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotRange {
    pub k: u32,
    pub n: u32,
    /// slots [lo, hi)
    lo: u32,
    hi: u32,
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
        for bad in ["", "1", "1/", "/2", "2/2", "0/0", "-1/2", "+1/2", " 1/2", "1/65537", "1/2/3", "a/b"] {
            assert_eq!(SlotRange::parse(bad), None, "{bad:?}");
        }
    }
}
