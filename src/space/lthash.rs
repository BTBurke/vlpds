//! LtHash over BLAKE3 (@atproto/space `lthash.ts`): a homomorphic multiset
//! hash. Each element expands, by BLAKE3 in XOF mode, to 1024 little-endian
//! u16 lanes, which are added to or subtracted from the state mod 2^16, so
//! the state depends only on the current multiset, not on insertion order.
//! The commit carries sha256 of the 2048-byte state.

use sha2::{Digest, Sha256};

const LANES: usize = 1024;
pub const STATE_BYTES: usize = LANES * 2;

#[derive(Clone, PartialEq, Eq)]
pub struct LtHash {
    lanes: [u16; LANES],
}

impl Default for LtHash {
    fn default() -> Self {
        LtHash { lanes: [0; LANES] }
    }
}

impl std::fmt::Debug for LtHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LtHash({})", hex::encode(self.digest()))
    }
}

impl LtHash {
    /// A persisted [`state`](Self::state); None unless exactly
    /// [`STATE_BYTES`] long.
    pub fn from_state(state: &[u8]) -> Option<LtHash> {
        if state.len() != STATE_BYTES {
            return None;
        }
        let mut h = LtHash::default();
        for (lane, b) in h.lanes.iter_mut().zip(state.as_chunks::<2>().0) {
            *lane = u16::from_le_bytes(*b);
        }
        Some(h)
    }

    pub fn add(&mut self, element: &str) -> &mut Self {
        let e = expand(element);
        for (l, x) in self.lanes.iter_mut().zip(e) {
            *l = l.wrapping_add(x);
        }
        self
    }

    pub fn remove(&mut self, element: &str) -> &mut Self {
        let e = expand(element);
        for (l, x) in self.lanes.iter_mut().zip(e) {
            *l = l.wrapping_sub(x);
        }
        self
    }

    pub fn state(&self) -> [u8; STATE_BYTES] {
        let mut out = [0u8; STATE_BYTES];
        for (b, l) in out.as_chunks_mut::<2>().0.iter_mut().zip(self.lanes) {
            b.copy_from_slice(&l.to_le_bytes());
        }
        out
    }

    pub fn digest(&self) -> [u8; 32] {
        Sha256::digest(self.state()).into()
    }

    /// The empty set's state (all zero lanes); also what any multiset whose
    /// elements cancel out returns to.
    pub fn is_empty(&self) -> bool {
        self.lanes.iter().all(|&l| l == 0)
    }
}

fn expand(element: &str) -> [u16; LANES] {
    let mut bytes = [0u8; STATE_BYTES];
    blake3::Hasher::new().update(element.as_bytes()).finalize_xof().fill(&mut bytes);
    let mut lanes = [0u16; LANES];
    for (l, b) in lanes.iter_mut().zip(bytes.as_chunks::<2>().0) {
        *l = u16::from_le_bytes(*b);
    }
    lanes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::space::vectors::VECTORS;

    fn digest_hex(h: &LtHash) -> String {
        hex::encode(h.digest())
    }

    /// The two vectors pinned by the reference's tests/lthash.test.ts.
    #[test]
    fn reference_snapshot_vectors() {
        assert_eq!(digest_hex(&LtHash::default()), "e5a00aa9991ac8a5ee3109844d84a55583bd20572ad3ffcd42792f3c36b183ad");
        let mut h = LtHash::default();
        h.add("one").add("two");
        assert_eq!(digest_hex(&h), "ae05cb6d224379d9710c290c8529945c5b0e0fde9ead30b9699057ce701c63e7");
    }

    /// testdata/spaces-alpha/vectors.json `lthash`: full states and digests.
    #[test]
    fn generated_vectors() {
        let cases = VECTORS["lthash"].as_array().unwrap();
        assert!(cases.len() >= 9);
        for c in cases {
            let mut h = LtHash::default();
            for op in c["ops"].as_array().unwrap() {
                let el = op[1].as_str().unwrap();
                match op[0].as_str().unwrap() {
                    "+" => h.add(el),
                    _ => h.remove(el),
                };
            }
            assert_eq!(hex::encode(h.state()), c["state"].as_str().unwrap(), "{c}");
            assert_eq!(digest_hex(&h), c["digest"].as_str().unwrap(), "{c}");
            assert_eq!(h.is_empty(), c["empty"].as_bool().unwrap(), "{c}");
            assert_eq!(LtHash::from_state(&h.state()).as_ref(), Some(&h));
        }
    }

    /// The reference's behavioural cases (tests/lthash.test.ts).
    #[test]
    fn set_semantics() {
        let mut h = LtHash::default();
        assert!(h.is_empty() && h.state() == [0; STATE_BYTES]);
        h.add("a");
        assert!(!h.is_empty());
        h.remove("a");
        assert!(h.is_empty());

        let (mut a, mut b) = (LtHash::default(), LtHash::default());
        a.add("a").add("b");
        b.add("b").add("a");
        assert_eq!(a, b);
        let (mut x, mut y) = (LtHash::default(), LtHash::default());
        x.add("a");
        y.add("b");
        assert_ne!(x, y);

        // a multiset: a double add doesn't cancel out
        let mut m = LtHash::default();
        m.add("a").add("a");
        assert!(!m.is_empty());
        m.remove("a");
        assert_eq!(m, x);

        // removing what was never added wraps rather than failing
        let mut w = LtHash::default();
        w.remove("a").add("a");
        assert!(w.is_empty());
    }

    #[test]
    fn state_round_trips() {
        let mut a = LtHash::default();
        a.add("a").add("b");
        assert_eq!(LtHash::from_state(&a.state()), Some(a.clone()));
        assert_eq!(LtHash::from_state(&[0; 32]), None);
        assert_eq!(LtHash::from_state(&[0; STATE_BYTES + 1]), None);
        let mut s = [0u8; STATE_BYTES];
        s[0] = 0xff;
        let h = LtHash::from_state(&s).unwrap();
        assert_eq!(h.state()[0], 0xff);
        assert_eq!(h.state()[1], 0);
    }
}
