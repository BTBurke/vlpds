//! Fixed hash-slot space. A DID's slot never changes (top 16 bits of
//! sha256(did)); shards own contiguous slot ranges, so the shard count can
//! grow by splitting ranges without rehashing any account.

use sha2::{Digest, Sha256};

pub const SLOTS: u32 = 65_536;

pub fn slot_of(did: &str) -> u16 {
    let h = Sha256::digest(did.as_bytes());
    u16::from_be_bytes([h[0], h[1]])
}

/// Uniform layout: shard i owns slots [i*65536/n, (i+1)*65536/n).
pub fn shard_of_slot(slot: u16, shards: u16) -> u16 {
    (slot as u32 * shards as u32 / SLOTS) as u16
}

pub fn shard_of(did: &str, shards: u16) -> u16 {
    shard_of_slot(slot_of(did), shards)
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
}
