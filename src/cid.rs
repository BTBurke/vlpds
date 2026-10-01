//! Compact CIDv1 with a sha2-256 multihash. Every CID this PDS produces or
//! accepts in a repo is one of these (dag-cbor records/nodes/commits, raw blobs),
//! so we store 33 bytes instead of a general-purpose CID.

use sha2::{Digest, Sha256};
use std::fmt;

pub const CODEC_DAG_CBOR: u8 = 0x71;
pub const CODEC_RAW: u8 = 0x55;

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Cid {
    pub codec: u8,
    pub digest: [u8; 32],
}

/// Binary CID length: version(1) + codec(1) + mh code(1) + mh len(1) + digest(32).
pub const CID_BYTES_LEN: usize = 36;

impl Cid {
    pub fn dag_cbor(data: &[u8]) -> Cid {
        Cid {
            codec: CODEC_DAG_CBOR,
            digest: Sha256::digest(data).into(),
        }
    }

    pub fn raw(data: &[u8]) -> Cid {
        Cid {
            codec: CODEC_RAW,
            digest: Sha256::digest(data).into(),
        }
    }

    pub fn to_bytes(&self) -> [u8; CID_BYTES_LEN] {
        let mut out = [0u8; CID_BYTES_LEN];
        out[0] = 0x01;
        out[1] = self.codec;
        out[2] = 0x12;
        out[3] = 0x20;
        out[4..].copy_from_slice(&self.digest);
        out
    }

    pub fn from_bytes(b: &[u8]) -> Result<Cid, CidError> {
        if b.len() != CID_BYTES_LEN {
            return Err(CidError::Unsupported);
        }
        if b[0] != 0x01
            || (b[1] != CODEC_DAG_CBOR && b[1] != CODEC_RAW)
            || b[2] != 0x12
            || b[3] != 0x20
        {
            return Err(CidError::Unsupported);
        }
        let mut digest = [0u8; 32];
        digest.copy_from_slice(&b[4..]);
        Ok(Cid {
            codec: b[1],
            digest,
        })
    }

    /// Reads a binary CID from the front of `b`, returning it and the bytes consumed.
    pub fn read_prefix(b: &[u8]) -> Result<(Cid, usize), CidError> {
        if b.len() < CID_BYTES_LEN {
            return Err(CidError::Unsupported);
        }
        Ok((Cid::from_bytes(&b[..CID_BYTES_LEN])?, CID_BYTES_LEN))
    }

    pub fn parse(s: &str) -> Result<Cid, CidError> {
        let rest = s.strip_prefix('b').ok_or(CidError::Unsupported)?;
        let bytes = base32_decode(rest).ok_or(CidError::Unsupported)?;
        Cid::from_bytes(&bytes)
    }
}

impl Cid {
    /// Appends the string form (as `Display`) without an intermediate String.
    pub fn write_string(&self, out: &mut Vec<u8>) {
        out.push(b'b');
        base32_encode_into(&self.to_bytes(), out);
    }
}

impl fmt::Display for Cid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("b")?;
        f.write_str(&base32_encode(&self.to_bytes()))
    }
}

impl fmt::Debug for Cid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CidError {
    #[error("unsupported or malformed CID")]
    Unsupported,
}

const B32: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

pub fn base32_encode(data: &[u8]) -> String {
    let mut out = Vec::with_capacity((data.len() * 8).div_ceil(5));
    base32_encode_into(data, &mut out);
    // the alphabet is ASCII
    String::from_utf8(out).unwrap()
}

pub fn base32_encode_into(data: &[u8], out: &mut Vec<u8>) {
    let mut buf: u32 = 0;
    let mut bits = 0;
    for &b in data {
        buf = (buf << 8) | b as u32;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(B32[((buf >> bits) & 31) as usize]);
        }
    }
    if bits > 0 {
        out.push(B32[((buf << (5 - bits)) & 31) as usize]);
    }
}

pub fn base32_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 5 / 8);
    let mut buf: u32 = 0;
    let mut bits = 0;
    for c in s.bytes() {
        let v = match c {
            b'a'..=b'z' => c - b'a',
            b'2'..=b'7' => c - b'2' + 26,
            _ => return None,
        } as u32;
        buf = (buf << 5) | v;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    // canonical only: the leftover bits are padding (fewer than one
    // character's worth) and must be zero, so each byte string has exactly
    // one encoding
    if bits >= 5 || buf & ((1 << bits) - 1) != 0 {
        return None;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_string() {
        // CID of the empty MST node, a well-known value in atproto.
        let c = Cid::parse("bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm").unwrap();
        assert_eq!(
            c.to_string(),
            "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"
        );
    }

    #[test]
    fn non_canonical_base32_rejected() {
        let s = "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm";
        // 36 bytes = 288 bits in 58 characters: the last character carries 3
        // data bits and 2 padding bits, which must be zero
        let last = s.as_bytes()[s.len() - 1];
        let v = B32.iter().position(|&c| c == last).unwrap();
        assert_eq!(v & 3, 0);
        for pad in 1..4 {
            let mut t = s[..s.len() - 1].to_string();
            t.push(B32[v | pad] as char);
            assert!(Cid::parse(&t).is_err(), "{t}");
        }
        // an extra character is padding too
        assert!(Cid::parse(&format!("{s}a")).is_err());
        assert!(base32_decode("a").is_none());
        // every byte string round-trips through its one encoding
        for n in 0..12u8 {
            let data: Vec<u8> = (0..n).map(|i| i.wrapping_mul(37) ^ 0xa5).collect();
            assert_eq!(base32_decode(&base32_encode(&data)).unwrap(), data);
        }
    }
}
