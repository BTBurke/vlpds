//! Log segment object format (one log per node incarnation).
//!
//! Segments carry the finished firehose frames (what subscribers receive) plus
//! the materialized-state mutations (SlateDB puts/deletes) used to apply and
//! replay them, each entry tagged with its shard and ownership epoch.

use bytes::{BufMut, Bytes};

#[derive(Clone, Debug)]
pub struct Mutation {
    pub key: Bytes,
    pub val: Option<Bytes>,
}

// ---------------------------------------------------------------------------
// Per-node logs. One log per node incarnation (`log_id`), entries tagged
// with the shard (and its ownership epoch) they belong to. A log is closed by
// a *fence* object written at its next ordinal (If-None-Match), after which
// the writer can never append again.
//
// "VLSEG02\n"
// header: log_id_len u16 | log_id | ordinal u64 | first_seq i64 | last_seq i64 | count u32
// entry:  seq i64 | shard u16 | epoch u64 | frame_len u32 | frame
//         | mut_count u32 | (key_len u16 | key | val_len u32 (MAX = delete) | val)*
//
// "VLFENCE\n" | fenced_by (utf8)
// ---------------------------------------------------------------------------

pub const MAGIC: &[u8; 8] = b"VLSEG02\n";
pub const FENCE_MAGIC: &[u8; 8] = b"VLFENCE\n";

#[derive(Clone, Debug)]
pub struct SegHeader {
    pub log_id: String,
    pub ordinal: u64,
    pub first_seq: i64,
    pub last_seq: i64,
    pub count: u32,
}

pub struct SegEntry {
    pub seq: i64,
    pub shard: u16,
    pub epoch: u64,
    pub frame: Bytes,
    pub muts: Vec<Mutation>,
}

pub enum LogObject {
    Segment(SegHeader, Vec<SegEntry>),
    Fence { by: String },
}

pub struct SegmentBuilder {
    pub body: Vec<u8>,
    pub first_seq: i64,
    pub last_seq: i64,
    pub count: u32,
}

impl Default for SegmentBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SegmentBuilder {
    pub fn new() -> Self {
        SegmentBuilder { body: Vec::with_capacity(1 << 20), first_seq: 0, last_seq: 0, count: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn len(&self) -> usize {
        self.body.len()
    }

    pub fn push(
        &mut self,
        seq: i64,
        shard: u16,
        epoch: u64,
        write_frame: impl FnOnce(&mut Vec<u8>),
        muts: &[Mutation],
    ) -> std::ops::Range<usize> {
        if self.count == 0 {
            self.first_seq = seq;
        }
        self.last_seq = seq;
        self.count += 1;
        self.body.put_i64(seq);
        self.body.put_u16(shard);
        self.body.put_u64(epoch);
        let len_at = self.body.len();
        self.body.put_u32(0);
        let start = self.body.len();
        write_frame(&mut self.body);
        let end = self.body.len();
        self.body[len_at..start].copy_from_slice(&((end - start) as u32).to_be_bytes());
        self.body.put_u32(muts.len() as u32);
        for m in muts {
            self.body.put_u16(m.key.len() as u16);
            self.body.put_slice(&m.key);
            match &m.val {
                Some(v) => {
                    self.body.put_u32(v.len() as u32);
                    self.body.put_slice(v);
                }
                None => self.body.put_u32(u32::MAX),
            }
        }
        start..end
    }

    /// Header bytes; entry ranges returned by `push` are relative to the body,
    /// so add the header length to address the full object.
    pub fn header(&self, log_id: &str, ordinal: u64) -> Vec<u8> {
        let mut h = Vec::with_capacity(40 + log_id.len());
        h.put_slice(MAGIC);
        h.put_u16(log_id.len() as u16);
        h.put_slice(log_id.as_bytes());
        h.put_u64(ordinal);
        h.put_i64(self.first_seq);
        h.put_i64(self.last_seq);
        h.put_u32(self.count);
        h
    }
}

pub fn fence_object(by: &str) -> Bytes {
    let mut b = Vec::with_capacity(8 + by.len());
    b.put_slice(FENCE_MAGIC);
    b.put_slice(by.as_bytes());
    b.into()
}

/// Parses a v2 log object (segment or fence). With `shard` set, only that
/// shard's entries are returned (handoff replay).
pub fn parse(data: Bytes, with_muts: bool, shard: Option<u16>) -> anyhow::Result<LogObject> {
    if data.len() >= 8 && &data[..8] == FENCE_MAGIC {
        return Ok(LogObject::Fence { by: String::from_utf8_lossy(&data[8..]).into_owned() });
    }
    anyhow::ensure!(data.len() >= 10 && &data[..8] == MAGIC, "bad v2 segment magic");
    let need = |pos: usize, n: usize| -> anyhow::Result<()> {
        anyhow::ensure!(pos + n <= data.len(), "truncated segment");
        Ok(())
    };
    let mut pos = 8;
    let idlen = u16::from_be_bytes(data[pos..pos + 2].try_into()?) as usize;
    pos += 2;
    need(pos, idlen + 28)?;
    let log_id = String::from_utf8(data[pos..pos + idlen].to_vec())?;
    pos += idlen;
    let rd8 = |p: usize| -> [u8; 8] { data[p..p + 8].try_into().unwrap() };
    let ordinal = u64::from_be_bytes(rd8(pos));
    let first_seq = i64::from_be_bytes(rd8(pos + 8));
    let last_seq = i64::from_be_bytes(rd8(pos + 16));
    let count = u32::from_be_bytes(data[pos + 24..pos + 28].try_into()?);
    pos += 28;
    let h = SegHeader { log_id, ordinal, first_seq, last_seq, count };
    let mut out = Vec::new();
    for _ in 0..count {
        need(pos, 22)?;
        let seq = i64::from_be_bytes(rd8(pos));
        let sh = u16::from_be_bytes(data[pos + 8..pos + 10].try_into()?);
        let epoch = u64::from_be_bytes(data[pos + 10..pos + 18].try_into()?);
        let flen = u32::from_be_bytes(data[pos + 18..pos + 22].try_into()?) as usize;
        pos += 22;
        need(pos, flen + 4)?;
        let frame = data.slice(pos..pos + flen);
        pos += flen;
        let nm = u32::from_be_bytes(data[pos..pos + 4].try_into()?) as usize;
        pos += 4;
        let keep = shard.is_none_or(|s| s == sh);
        let mut muts = Vec::new();
        for _ in 0..nm {
            need(pos, 2)?;
            let kl = u16::from_be_bytes(data[pos..pos + 2].try_into()?) as usize;
            pos += 2;
            need(pos, kl + 4)?;
            let key = data.slice(pos..pos + kl);
            pos += kl;
            let vl = u32::from_be_bytes(data[pos..pos + 4].try_into()?);
            pos += 4;
            let val = if vl == u32::MAX {
                None
            } else {
                need(pos, vl as usize)?;
                let v = data.slice(pos..pos + vl as usize);
                pos += vl as usize;
                Some(v)
            };
            if with_muts && keep {
                muts.push(Mutation { key, val });
            }
        }
        if keep {
            out.push(SegEntry { seq, shard: sh, epoch, frame, muts });
        }
    }
    Ok(LogObject::Segment(h, out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_filter() {
        let mut b = SegmentBuilder::new();
        let m = |k: &str, v: Option<&str>| Mutation { key: Bytes::from(k.to_string()), val: v.map(|v| Bytes::from(v.to_string())) };
        b.push(10, 3, 7, |o| o.extend_from_slice(b"frame-a"), &[m("k1", Some("v1"))]);
        b.push(11, 5, 1, |o| o.extend_from_slice(b"frame-b"), &[m("k2", None)]);
        b.push(12, 3, 7, |_| {}, &[m("k3", Some("v3"))]);
        let mut obj = b.header("node-a.1", 42);
        obj.extend_from_slice(&b.body);
        let LogObject::Segment(h, all) = parse(Bytes::from(obj.clone()), true, None).unwrap() else { panic!() };
        assert_eq!((h.log_id.as_str(), h.ordinal, h.first_seq, h.last_seq, h.count), ("node-a.1", 42, 10, 12, 3));
        assert_eq!(all.len(), 3);
        assert_eq!(&all[1].frame[..], b"frame-b");
        assert!(all[1].muts[0].val.is_none());
        let LogObject::Segment(_, only3) = parse(Bytes::from(obj), true, Some(3)).unwrap() else { panic!() };
        assert_eq!(only3.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![10, 12]);
        assert!(matches!(parse(fence_object("node-b"), false, None).unwrap(), LogObject::Fence { by } if by == "node-b"));
    }
}
