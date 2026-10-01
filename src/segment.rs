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
// "VLSEG05\n"
// header: log_id_len u16 | log_id | ordinal u64 | prefix_end u64
//         | first_seq i64 | last_seq i64 | count u32 | codec u8 | body_len u32
// body:   entry* (codec 0), or one zstd frame of them (codec 1)
// entry:  seq i64 | shard u16 | epoch u64 | frame_len u32 | frame
//         | mut_count u32 | (key_len u16 | key | val_len u32 (MAX = delete) | val)*
//
// The header is never compressed, so header-only reads (`parse_header` on
// a small range GET: prefix_end, seq ranges) work on either codec.
// `body_len` is the uncompressed body length. The writer keeps the
// uncompressed object in memory (codec 0: the live ring and the merger
// slice frames out of it) and stores `compress(obj)`; readers `decode` the
// stored object back to exactly those bytes (codec byte reset to 0), so
// entry offsets are the same in both. Real commits compress ~1.9-2.7x at
// zstd level 1 in segments of 256 KiB and up (DESIGN.md "Log compression").
//
// mut_count with its top bit set: bits 0-15 count the muts stored, bits
// 16-30 the muts *derived* from the #commit frame, which come first (see
// `derive_commit_muts`). A commit's record and head values repeat the record
// blocks and the signed commit its CAR already carries; storing them again
// cost ~20% of a single-record commit's segment bytes.
//
// "VLFENCE\n" | fenced_by (utf8)
//
// Up to K segment PUTs are in flight per log, so a crash can leave holes
// (ordinal n missing, n+1 present). `prefix_end` is the writer's promise when
// it sealed the segment: every ordinal below it was already durable. Holes
// can therefore only sit in [prefix_end, ordinal), at most K - 1 ordinals,
// which lets a reader prove a segment is inside the log's gap-free prefix
// with a bounded number of probes (see `nodelog::in_prefix`).
// ---------------------------------------------------------------------------

pub const MAGIC: &[u8; 8] = b"VLSEG05\n";

/// Body codecs (the header's codec byte).
pub const CODEC_NONE: u8 = 0;
pub const CODEC_ZSTD: u8 = 1;

/// zstd level for stored segment bodies (`--log-compression`); 0 = off.
static ZSTD_LEVEL: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(DEFAULT_ZSTD_LEVEL);
pub const DEFAULT_ZSTD_LEVEL: i32 = 1;

/// Sets the zstd level segments are stored with from now on (0 = store
/// them uncompressed). Readers handle either.
pub fn set_compression_level(level: i32) {
    ZSTD_LEVEL.store(level, std::sync::atomic::Ordering::Relaxed);
}

pub fn compression_level() -> i32 {
    ZSTD_LEVEL.load(std::sync::atomic::Ordering::Relaxed)
}

/// mut_count flag: derived muts precede the stored ones.
const DERIVED: u32 = 1 << 31;
pub const FENCE_MAGIC: &[u8; 8] = b"VLFENCE\n";

#[derive(Clone, Debug)]
pub struct SegHeader {
    pub log_id: String,
    pub ordinal: u64,
    /// Every ordinal below this was durable when this segment was sealed.
    pub prefix_end: u64,
    pub first_seq: i64,
    pub last_seq: i64,
    pub count: u32,
    /// Body codec ([`CODEC_NONE`] or [`CODEC_ZSTD`]).
    pub codec: u8,
    /// Uncompressed body length.
    pub body_len: u32,
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
    /// Bytes reserved at the start of `body` for the header (`for_log`).
    header_room: usize,
}

impl Default for SegmentBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SegmentBuilder {
    pub fn new() -> Self {
        SegmentBuilder { body: Vec::with_capacity(1 << 20), first_seq: 0, last_seq: 0, count: 0, header_room: 0 }
    }

    /// A builder whose body starts with room for `log_id`'s header, so
    /// `seal` writes it in place instead of copying the body behind it.
    /// Entry ranges from `push` are then offsets into the sealed object.
    pub fn for_log(log_id: &str) -> Self {
        let room = header_len(log_id);
        let mut body = Vec::with_capacity(1 << 20);
        body.resize(room, 0);
        SegmentBuilder { body, first_seq: 0, last_seq: 0, count: 0, header_room: room }
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
        self.push_derived(seq, shard, epoch, write_frame, muts, 0)
    }

    /// [`push`](Self::push) where the first `derived` muts are left out:
    /// replay rebuilds them from the #commit frame (`derive_commit_muts`).
    pub fn push_derived(
        &mut self,
        seq: i64,
        shard: u16,
        epoch: u64,
        write_frame: impl FnOnce(&mut Vec<u8>),
        muts: &[Mutation],
        derived: usize,
    ) -> std::ops::Range<usize> {
        let stored = &muts[derived.min(muts.len())..];
        let derived = if derived > 0 && derived <= muts.len() && derived < 1 << 15 && stored.len() < 1 << 16 { derived } else { 0 };
        let stored = &muts[derived..];
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
        if derived > 0 {
            self.body.put_u32(DERIVED | (derived as u32) << 16 | stored.len() as u32);
        } else {
            self.body.put_u32(stored.len() as u32);
        }
        for m in stored {
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

    /// Header bytes for a segment written after every earlier ordinal was
    /// durable (a dense log: `prefix_end` = `ordinal`).
    pub fn header(&self, log_id: &str, ordinal: u64) -> Vec<u8> {
        self.sealed_header(log_id, ordinal, ordinal)
    }

    /// The sealed (uncompressed) object: the header written into the room
    /// `for_log` left (no copy), or prepended. [`compress`] makes the stored form.
    pub fn seal(self, log_id: &str, ordinal: u64, prefix_end: u64) -> Vec<u8> {
        let h = self.sealed_header(log_id, ordinal, prefix_end);
        if self.header_room == h.len() {
            let mut body = self.body;
            body[..h.len()].copy_from_slice(&h);
            return body;
        }
        let mut obj = h;
        obj.extend_from_slice(&self.body[self.header_room..]);
        obj
    }

    /// Header bytes; entry ranges returned by `push` are relative to the body,
    /// so add the header length to address the full object.
    pub fn sealed_header(&self, log_id: &str, ordinal: u64, prefix_end: u64) -> Vec<u8> {
        debug_assert!(prefix_end <= ordinal);
        let mut h = Vec::with_capacity(header_len(log_id));
        h.put_slice(MAGIC);
        h.put_u16(log_id.len() as u16);
        h.put_slice(log_id.as_bytes());
        h.put_u64(ordinal);
        h.put_u64(prefix_end);
        h.put_i64(self.first_seq);
        h.put_i64(self.last_seq);
        h.put_u32(self.count);
        h.put_u8(CODEC_NONE);
        h.put_u32((self.body.len() - self.header_room) as u32);
        h
    }
}

/// Header bytes after the log id.
const HEADER_TAIL: usize = 41;

fn header_len(log_id: &str) -> usize {
    MAGIC.len() + 2 + log_id.len() + HEADER_TAIL
}

thread_local! {
    static ZCTX: std::cell::RefCell<Option<(i32, zstd::bulk::Compressor<'static>)>> = const { std::cell::RefCell::new(None) };
    static DCTX: std::cell::RefCell<Option<zstd::bulk::Decompressor<'static>>> = const { std::cell::RefCell::new(None) };
}

/// The stored form of a sealed, uncompressed segment `obj`: its body as one
/// zstd frame at `level` (header unchanged but for the codec byte). None if
/// `level` is 0 or compression doesn't make it smaller: store `obj` as is.
pub fn compress(obj: &[u8], level: i32) -> anyhow::Result<Option<Vec<u8>>> {
    if level == 0 {
        return Ok(None);
    }
    let Some((h, hl)) = parse_header(obj)? else { return Ok(None) };
    anyhow::ensure!(h.codec == CODEC_NONE && hl + h.body_len as usize == obj.len(), "compress: not a sealed uncompressed segment");
    let body = &obj[hl..];
    let mut out = Vec::with_capacity(hl + zstd::zstd_safe::compress_bound(body.len()));
    out.extend_from_slice(&obj[..hl]);
    out[hl - 5] = CODEC_ZSTD;
    out.resize(out.capacity(), 0);
    let n = ZCTX.with(|c| -> anyhow::Result<usize> {
        let mut c = c.borrow_mut();
        if c.as_ref().is_none_or(|(l, _)| *l != level) {
            *c = Some((level, zstd::bulk::Compressor::new(level)?));
        }
        Ok(c.as_mut().unwrap().1.compress_to_buffer(body, &mut out[hl..])?)
    })?;
    if n >= body.len() {
        return Ok(None);
    }
    out.truncate(hl + n);
    Ok(Some(out))
}

/// A stored log object in the form the writer sealed it: a compressed
/// segment's body decompressed (and its codec byte reset), so entry offsets
/// match the writer's in-memory object. Fences and uncompressed segments
/// come back as they are (no copy).
pub fn decode(data: Bytes) -> anyhow::Result<Bytes> {
    let Some((h, hl)) = parse_header(&data)? else { return Ok(data) };
    let body_len = h.body_len as usize;
    match h.codec {
        CODEC_NONE => {
            anyhow::ensure!(data.len() == hl + body_len, "segment {} body is {} bytes, header says {body_len}", h.ordinal, data.len() - hl);
            Ok(data)
        }
        CODEC_ZSTD => {
            let mut out = Vec::with_capacity(hl + body_len);
            out.extend_from_slice(&data[..hl]);
            out[hl - 5] = CODEC_NONE;
            out.resize(hl + body_len, 0);
            let n = DCTX.with(|d| -> anyhow::Result<usize> {
                let mut d = d.borrow_mut();
                if d.is_none() {
                    *d = Some(zstd::bulk::Decompressor::new()?);
                }
                Ok(d.as_mut().unwrap().decompress_to_buffer(&data[hl..], &mut out[hl..])?)
            })?;
            anyhow::ensure!(n == body_len, "segment {} decompressed to {n} bytes, header says {body_len}", h.ordinal);
            crate::metrics::SEGMENT_DECODES.inc();
            Ok(out.into())
        }
        c => anyhow::bail!("segment {} has unknown codec {c}", h.ordinal),
    }
}

pub fn fence_object(by: &str) -> Bytes {
    let mut b = Vec::with_capacity(8 + by.len());
    b.put_slice(FENCE_MAGIC);
    b.put_slice(by.as_bytes());
    b.into()
}

/// Parses just the header of a log object (a prefix of it is enough): None
/// for a fence. Returns the header and its length.
pub fn parse_header(data: &[u8]) -> anyhow::Result<Option<(SegHeader, usize)>> {
    if data.starts_with(FENCE_MAGIC) {
        return Ok(None);
    }
    anyhow::ensure!(data.len() >= 10 && data.starts_with(MAGIC), "bad segment magic");
    let idlen = u16::from_be_bytes(data[8..10].try_into()?) as usize;
    let pos = 10 + idlen;
    anyhow::ensure!(data.len() >= pos + HEADER_TAIL, "truncated segment header");
    let rd8 = |p: usize| -> [u8; 8] { data[p..p + 8].try_into().unwrap() };
    let h = SegHeader {
        log_id: String::from_utf8(data[10..pos].to_vec())?,
        ordinal: u64::from_be_bytes(rd8(pos)),
        prefix_end: u64::from_be_bytes(rd8(pos + 8)),
        first_seq: i64::from_be_bytes(rd8(pos + 16)),
        last_seq: i64::from_be_bytes(rd8(pos + 24)),
        count: u32::from_be_bytes(data[pos + 32..pos + 36].try_into()?),
        codec: data[pos + 36],
        body_len: u32::from_be_bytes(data[pos + 37..pos + 41].try_into()?),
    };
    anyhow::ensure!(h.prefix_end <= h.ordinal, "segment {} has prefix_end {} past it", h.ordinal, h.prefix_end);
    Ok(Some((h, pos + HEADER_TAIL)))
}

/// Parses a stored log object (segment or fence), decompressing it if
/// needed ([`decode`]): frames and values are slices of the uncompressed
/// object. With `shard` set, only that shard's entries are returned
/// (handoff replay).
pub fn parse(data: Bytes, with_muts: bool, shard: Option<u16>) -> anyhow::Result<LogObject> {
    let data = decode(data)?;
    let Some((h, mut pos)) = parse_header(&data)? else {
        return Ok(LogObject::Fence { by: String::from_utf8_lossy(&data[8..]).into_owned() });
    };
    let need = |pos: usize, n: usize| -> anyhow::Result<()> {
        anyhow::ensure!(pos + n <= data.len(), "truncated segment");
        Ok(())
    };
    let rd8 = |p: usize| -> [u8; 8] { data[p..p + 8].try_into().unwrap() };
    let count = h.count;
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
        let nm = u32::from_be_bytes(data[pos..pos + 4].try_into()?);
        pos += 4;
        let (derived, nm) = if nm & DERIVED != 0 { (((nm & !DERIVED) >> 16) as usize, (nm & 0xffff) as usize) } else { (0, nm as usize) };
        let keep = shard.is_none_or(|s| s == sh);
        let mut muts = Vec::new();
        if derived > 0 && with_muts && keep {
            muts = derive_commit_muts(&frame)?;
            anyhow::ensure!(muts.len() == derived, "segment entry {seq}: {} muts derived from its frame, {derived} expected", muts.len());
        }
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

/// The state mutations of a #commit, rebuilt from its frame: for each op,
/// the record CID index keys (delete the previous, put the new), the record
/// (`R/`: cid | rev | the block from the commit's CAR) or its delete; then
/// the head (`h/`). Exactly what the repo worker writes for a commit, in
/// the same order; the worker checks the two agree (debug builds).
pub fn derive_commit_muts(frame: &[u8]) -> anyhow::Result<Vec<Mutation>> {
    use crate::cbor::Value;
    use crate::cid::Cid;
    use crate::state;
    let (header, n) = Value::decode_prefix(frame)?;
    anyhow::ensure!(header.get("t").and_then(Value::as_str) == Some("#commit"), "not a #commit frame");
    let body = Value::decode(&frame[n..])?;
    let text = |k: &str| body.get(k).and_then(Value::as_str).ok_or_else(|| anyhow::anyhow!("#commit without {k}"));
    let link = |v: Option<&Value>| match v {
        Some(Value::Link(c)) => Some(*c),
        _ => None,
    };
    let did = text("repo")?;
    let rev = crate::tid::Tid::parse(text("rev")?).ok_or_else(|| anyhow::anyhow!("bad #commit rev"))?;
    let commit = link(body.get("commit")).ok_or_else(|| anyhow::anyhow!("#commit without commit"))?;
    let Some(Value::Bytes(car)) = body.get("blocks") else { anyhow::bail!("#commit without blocks") };
    let (_, blocks) = crate::car::read_car(car)?;
    let block = |c: &Cid| blocks.iter().find(|(b, _)| b == c).map(|(_, d)| *d).ok_or_else(|| anyhow::anyhow!("#commit CAR lacks block {c}"));
    let Some(Value::Array(ops)) = body.get("ops") else { anyhow::bail!("#commit without ops") };
    let mut muts = Vec::with_capacity(ops.len() * 3 + 1);
    for op in ops {
        let path = op.get("path").and_then(Value::as_str).ok_or_else(|| anyhow::anyhow!("op without path"))?;
        let (prev, new) = (link(op.get("prev")), link(op.get("cid")));
        if let Some(p) = &prev {
            muts.push(Mutation { key: state::record_cid_key(did, p, path).into(), val: None });
        }
        if let Some(c) = &new {
            muts.push(Mutation { key: state::record_cid_key(did, c, path).into(), val: Some(Bytes::new()) });
        }
        let key = Bytes::from(state::record_key(did, path));
        muts.push(match &new {
            Some(c) => Mutation { key, val: Some(state::record_value(c, rev.0, block(c)?)) },
            None => Mutation { key, val: None },
        });
    }
    let commit_block = block(&commit)?;
    let data = link(Value::decode(commit_block)?.get("data")).ok_or_else(|| anyhow::anyhow!("commit block without data"))?;
    let head = state::Head { commit, data, rev, commit_block: Bytes::copy_from_slice(commit_block) };
    muts.push(Mutation { key: state::head_key(did).into(), val: Some(head.encode()) });
    Ok(muts)
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
        let mut obj = b.sealed_header("node-a.1", 42, 39);
        obj.extend_from_slice(&b.body);
        let LogObject::Segment(h, all) = parse(Bytes::from(obj.clone()), true, None).unwrap() else { panic!() };
        assert_eq!((h.log_id.as_str(), h.ordinal, h.prefix_end, h.first_seq, h.last_seq, h.count), ("node-a.1", 42, 39, 10, 12, 3));
        let (hh, len) = parse_header(&obj[..60]).unwrap().unwrap();
        assert_eq!((hh.ordinal, hh.prefix_end, len), (42, 39, b.header("node-a.1", 42).len()));
        assert!(parse_header(&fence_object("node-b")).unwrap().is_none());
        assert_eq!(all.len(), 3);
        assert_eq!(&all[1].frame[..], b"frame-b");
        assert!(all[1].muts[0].val.is_none());
        let LogObject::Segment(_, only3) = parse(Bytes::from(obj), true, Some(3)).unwrap() else { panic!() };
        assert_eq!(only3.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![10, 12]);
        assert!(matches!(parse(fence_object("node-b"), false, None).unwrap(), LogObject::Fence { by } if by == "node-b"));
    }

    /// A builder made `for_log` seals in place (same bytes as prepending the
    /// header), and an entry's derived muts are left out of the segment and
    /// rebuilt from its #commit frame on parse.
    #[test]
    fn seal_in_place_and_derived_muts() {
        use crate::cid::Cid;
        let did = "did:plc:abc";
        let rec = crate::cid::Cid::dag_cbor(b"\xa1aa\x01");
        let mut rec_block = Vec::new();
        rec_block.extend_from_slice(b"\xa1aa\x01");
        let mut commit_block = Vec::new();
        crate::cbor::Value::Map(vec![("did".into(), crate::cbor::Value::Text(did.into())), ("data".into(), crate::cbor::Value::Link(rec))]).encode(&mut commit_block);
        let commit = Cid::dag_cbor(&commit_block);
        let mut car = Vec::new();
        crate::car::write_header(&mut car, &commit);
        crate::car::write_block(&mut car, &commit, &commit_block);
        crate::car::write_block(&mut car, &rec, &rec_block);
        let rev = crate::tid::Tid::parse("3l3qo2vutsw2b").unwrap();
        let ops = [crate::events::RepoOp { action: "update", path: "app.bsky.feed.post/1", cid: Some(rec), prev: Some(commit) }];
        let frame = crate::events::commit_frame(&crate::events::CommitFrame {
            repo: did,
            rev: &rev.to_string(),
            since: None,
            commit,
            prev_data: None,
            blocks: &car,
            ops: &ops,
            time: "2026-10-01T00:00:00.000Z",
        });
        let mut bytes = Vec::new();
        frame.finish(5, &mut bytes);
        let derived = derive_commit_muts(&bytes).unwrap();
        let keys: Vec<&[u8]> = derived.iter().map(|m| &crate::state::key_body(&m.key)[..2]).collect();
        assert_eq!(keys, vec![b"c/" as &[u8], b"c/", b"R/", b"h/"]);
        assert_eq!(derived[2].val.as_deref(), Some(&crate::state::record_value(&rec, rev.0, &rec_block)[..]));
        let head = crate::state::Head::decode(derived[3].val.as_ref().unwrap()).unwrap();
        assert_eq!((head.commit, head.data, head.rev.0, &head.commit_block[..]), (commit, rec, rev.0, &commit_block[..]));

        let extra = Mutation { key: Bytes::from_static(b"C/x"), val: Some(Bytes::new()) };
        let mut all = derived.clone();
        all.push(extra);
        for in_place in [false, true] {
            let mut b = if in_place { SegmentBuilder::for_log("L") } else { SegmentBuilder::new() };
            let r = b.push_derived(5, 1, 2, |o| frame.finish(5, o), &all, derived.len());
            b.push(6, 1, 2, |o| o.extend_from_slice(b"plain"), &all[..1]);
            let obj = b.seal("L", 9, 9);
            let off = if in_place { 0 } else { header_len("L") };
            assert_eq!(&obj[r.start + off..r.end + off], &bytes[..]);
            let LogObject::Segment(h, entries) = parse(Bytes::from(obj.clone()), true, None).unwrap() else { panic!() };
            assert_eq!((h.ordinal, h.count), (9, 2));
            assert_eq!(entries[0].muts.len(), all.len(), "derived + stored");
            for (a, b) in entries[0].muts.iter().zip(&all) {
                assert!(a.key == b.key && a.val == b.val);
            }
            assert_eq!(entries[1].muts.len(), 1);
            // the derived muts aren't stored: the record and commit blocks
            // appear once (in the frame's CAR)
            let n = obj.windows(commit_block.len()).filter(|w| *w == &commit_block[..]).count();
            assert_eq!(n, 1);
        }
    }


    /// A compressed segment keeps its header readable on its own, decodes
    /// to exactly the bytes the writer sealed (so frame ranges from `push`
    /// address both), and parses like the uncompressed one.
    #[test]
    fn compressed_roundtrip() {
        let mut b = SegmentBuilder::for_log("node-a.7");
        let mut ranges = Vec::new();
        for i in 0..200u32 {
            let m = Mutation { key: Bytes::from(format!("R/did:plc:aaaa{}\0app.bsky.feed.like/{i:08}", i % 7)), val: Some(Bytes::from(vec![b'v'; 40])) };
            ranges.push(b.push(1000 + i as i64, (i % 3) as u16, 2, |o| o.extend_from_slice(format!("frame {i} {}", "x".repeat(64)).as_bytes()), &[m]));
        }
        let sealed = b.seal("node-a.7", 5, 3);
        let stored = compress(&sealed, 1).unwrap().expect("compressible");
        assert!(stored.len() * 3 < sealed.len(), "{} -> {}", sealed.len(), stored.len());
        // header-only read of the stored object
        let (h, hl) = parse_header(&stored[..64]).unwrap().unwrap();
        assert_eq!((h.ordinal, h.prefix_end, h.first_seq, h.last_seq, h.count, h.codec), (5, 3, 1000, 1199, 200, CODEC_ZSTD));
        assert_eq!((h.body_len as usize, hl), (sealed.len() - hl, header_len("node-a.7")));
        let decoded = decode(Bytes::from(stored.clone())).unwrap();
        assert_eq!(&decoded[..], &sealed[..]);
        let LogObject::Segment(h2, entries) = parse(Bytes::from(stored), true, Some(1)).unwrap() else { panic!() };
        assert_eq!(h2.codec, CODEC_NONE);
        assert_eq!(entries.len(), 67);
        for e in &entries {
            let i = (e.seq - 1000) as usize;
            assert_eq!(&e.frame[..], &sealed[ranges[i].clone()]);
            assert_eq!(e.muts.len(), 1);
        }
        // level 0, fences and incompressible bodies are stored as they are
        assert!(compress(&sealed, 0).unwrap().is_none());
        assert!(compress(&fence_object("x"), 1).unwrap().is_none());
        let mut b = SegmentBuilder::new();
        let noise: Vec<u8> = (0..4096).map(|_| rand::random::<u8>()).collect();
        b.push(1, 0, 0, |o| o.extend_from_slice(&noise), &[]);
        assert!(compress(&b.seal("L", 0, 0), 1).unwrap().is_none());
        // a corrupt body is an error, not a short segment
        let mut bad = compress(&sealed, 1).unwrap().unwrap();
        let n = bad.len();
        bad.truncate(n - 8);
        assert!(decode(Bytes::from(bad)).is_err());
    }
}
