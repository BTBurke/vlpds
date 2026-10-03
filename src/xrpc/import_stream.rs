//! importRepo's body, parsed as it arrives when the CAR is in the
//! streamable block order (src/car_order.rs): one pass verifies the commit
//! block, every node's and record's CID, and key order, holding only the
//! nodes on the path from the root, and builds the tree as records arrive.
//! Anything else (another order, a malformed or refused CAR) falls back to
//! the buffered [`parse_import`] of the whole body. The fast path accepts
//! only CARs that one accepts, with the same records and tree, so the result
//! and every error are the buffered parse's.

use super::repo::{imported_record_blobs, parse_import, ImportedRecord};
use super::*;
use crate::car_order::{Next, Walk};
use crate::mst::{self, Tree};
use futures::StreamExt;
use tokio::sync::mpsc;

/// Chunks queued for the parser: hyper's are up to a few hundred KB.
const QUEUE: usize = 32;

fn too_large(max: usize) -> XrpcError {
    XrpcError {
        status: StatusCode::PAYLOAD_TOO_LARGE,
        error: "PayloadTooLarge".into(),
        message: format!("request entity too large (max {max} bytes)"),
    }
}

/// Reads the body (at most `max` bytes) and parses it off the async runtime.
pub(super) async fn read(body: Body, headers: &HeaderMap, max: usize) -> XResult<(Vec<ImportedRecord>, Tree)> {
    let declared = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if declared.is_some_and(|n| n > max as u64) {
        return Err(too_large(max));
    }
    // None marks the end of the body; a closed channel without it, an abort
    let (tx, rx) = mpsc::channel::<Option<Bytes>>(QUEUE);
    let parser = tokio::task::spawn_blocking(move || parse(rx));
    let mut stream = body.into_data_stream();
    let mut total = 0usize;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| XrpcError::bad("InvalidRequest", format!("error reading body: {e}")))?;
        total += chunk.len();
        if total > max {
            return Err(too_large(max));
        }
        if !chunk.is_empty() && tx.send(Some(chunk)).await.is_err() {
            break;
        }
    }
    let _ = tx.send(None).await;
    let (r, path) = parser.await.map_err(XrpcError::from_err)?;
    metrics::IMPORT_REPO_PARSES.with_label_values(&[path]).inc();
    r
}

/// The parse and which path took it (`stream` or `buffered`).
fn parse(rx: mpsc::Receiver<Option<Bytes>>) -> (XResult<(Vec<ImportedRecord>, Tree)>, &'static str) {
    let mut input = Input { rx, chunks: Vec::new(), at: 0, off: 0, avail: 0, end: End::Open };
    if let Some(r) = stream(&mut input) {
        return (Ok(r), "stream");
    }
    let r = match input.rest() {
        Some(body) => parse_import(&body),
        None => Err(XrpcError::bad("InvalidRequest", "request body aborted")),
    };
    (r, "buffered")
}

/// The single pass; None on any departure from the order, or anything the
/// buffered parse would refuse (it then reports why).
fn stream(input: &mut Input) -> Option<(Vec<ImportedRecord>, Tree)> {
    let hlen = input.varint()?;
    let header = input.take(usize::try_from(hlen).ok()?)?;
    let roots = car::read_header(&header).ok()?;
    let [root] = roots[..] else { return None };
    let (c, commit) = input.block()?;
    if c != root {
        return None;
    }
    let commit = Value::decode(&commit).ok()?;
    if !matches!(commit.get("version"), Some(Value::Int(2 | 3))) {
        return None;
    }
    let Some(Value::Link(data)) = commit.get("data") else { return None };
    let data = *data;
    let mut walk = Walk::new(data);
    let mut tree = Tree::new();
    let mut records: Vec<ImportedRecord> = Vec::new();
    let mut prev: Option<Arc<[u8]>> = None;
    loop {
        match walk.next() {
            Next::Node(want) => {
                let (c, b) = input.block()?;
                if c != want {
                    return None;
                }
                walk.enter(mst::decode_node(&b, c).ok()?).ok()?;
            }
            Next::Record { key, cid } => {
                if prev.as_ref().is_some_and(|p| key <= *p) {
                    return None;
                }
                let path = std::str::from_utf8(&key).ok()?;
                if !super::syntax::valid_record_path(path) {
                    return None;
                }
                let (c, b) = input.block()?;
                if c != cid {
                    return None;
                }
                let blobs = imported_record_blobs(path, &b).ok()?;
                tree.insert_no_proof(&key, cid).ok()?;
                records.push((path.to_string(), cid, b, blobs));
                prev = Some(key);
            }
            Next::Done => break,
        }
    }
    // The tree rebuilt from the records reproduces `data` only if every node
    // streamed was the canonical one: the same tree the buffered parse loads.
    if tree.root_cid().ok()? != data {
        return None;
    }
    // further blocks are only checked against their CIDs, as there
    while !input.at_end() {
        input.block()?;
    }
    Some((records, tree))
}

#[derive(PartialEq)]
enum End {
    Open,
    Done,
    Aborted,
}

/// The body as received so far, kept whole for a fallback; a block within
/// one chunk is a slice of it.
struct Input {
    rx: mpsc::Receiver<Option<Bytes>>,
    chunks: Vec<Bytes>,
    /// Read position: chunk index and offset in it.
    at: usize,
    off: usize,
    /// Bytes received past the read position.
    avail: usize,
    end: End,
}

impl Input {
    fn recv(&mut self) -> bool {
        if self.end != End::Open {
            return false;
        }
        match self.rx.blocking_recv() {
            Some(Some(c)) => {
                self.avail += c.len();
                self.chunks.push(c);
                true
            }
            Some(None) => {
                self.end = End::Done;
                false
            }
            None => {
                self.end = End::Aborted;
                false
            }
        }
    }

    fn fill(&mut self, n: usize) -> bool {
        while self.avail < n {
            if !self.recv() {
                return false;
            }
        }
        true
    }

    fn at_end(&mut self) -> bool {
        !self.fill(1)
    }

    /// The next `n` bytes, or None if the body ends first.
    fn take(&mut self, n: usize) -> Option<Bytes> {
        if !self.fill(n) {
            return None;
        }
        self.avail -= n;
        while self.at < self.chunks.len() && self.off == self.chunks[self.at].len() {
            self.at += 1;
            self.off = 0;
        }
        if n == 0 {
            return Some(Bytes::new());
        }
        let c = &self.chunks[self.at];
        if c.len() - self.off >= n {
            let b = c.slice(self.off..self.off + n);
            self.off += n;
            return Some(b);
        }
        let mut out = Vec::with_capacity(n);
        while out.len() < n {
            let c = &self.chunks[self.at];
            let k = (c.len() - self.off).min(n - out.len());
            out.extend_from_slice(&c[self.off..self.off + k]);
            self.off += k;
            if self.off == c.len() {
                self.at += 1;
                self.off = 0;
            }
        }
        Some(Bytes::from(out))
    }

    /// As [`car::read_varint`].
    fn varint(&mut self) -> Option<u64> {
        let mut n = 0u64;
        for i in 0..10 {
            let b = self.take(1)?[0];
            n |= ((b & 0x7f) as u64) << (7 * i);
            if b & 0x80 == 0 {
                return Some(n);
            }
        }
        None
    }

    /// A block whose bytes match its CID.
    fn block(&mut self) -> Option<(Cid, Bytes)> {
        let len = self.varint()?;
        let b = self.take(usize::try_from(len).ok()?)?;
        let (c, cl) = Cid::read_prefix(&b).ok()?;
        let data = b.slice(cl..);
        car::block_matches(&c, &data).then_some((c, data))
    }

    /// The whole body; None if it was aborted.
    fn rest(mut self) -> Option<Bytes> {
        while self.recv() {}
        if self.end == End::Aborted {
            return None;
        }
        if self.chunks.len() == 1 {
            return self.chunks.pop();
        }
        let total = self.chunks.iter().map(Bytes::len).sum();
        let mut out = Vec::with_capacity(total);
        // freed as copied, so the copy adds little
        for c in self.chunks.drain(..) {
            out.extend_from_slice(&c);
        }
        Some(Bytes::from(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cbor::key_cmp;
    use std::collections::HashMap;

    struct Repo {
        commit: (Cid, Vec<u8>),
        data: Cid,
        blocks: HashMap<Cid, Vec<u8>>,
    }

    fn record(path: &str) -> Vec<u8> {
        let mut m = vec![
            ("$type".to_string(), Value::Text(crate::worker::collection_of(path).into())),
            ("k".to_string(), Value::Text(path.into())),
        ];
        m.sort_by(|a, b| key_cmp(&a.0, &b.0));
        Value::Map(m).to_cbor()
    }

    fn commit(data: Cid) -> (Cid, Vec<u8>) {
        let mut f = vec![
            ("did".to_string(), Value::Text("did:plc:abc".into())),
            ("rev".to_string(), Value::Text("3l3qo2vuowo2b".into())),
            ("data".to_string(), Value::Link(data)),
            ("prev".to_string(), Value::Null),
            ("version".to_string(), Value::Int(3)),
            ("sig".to_string(), Value::Bytes(vec![0; 64])),
        ];
        f.sort_by(|a, b| key_cmp(&a.0, &b.0));
        let b = Value::Map(f).to_cbor();
        (Cid::dag_cbor(&b), b)
    }

    fn repo_of(records: &[(String, Vec<u8>)]) -> Repo {
        let mut tree = Tree::new();
        let mut blocks = Vec::new();
        for (p, r) in records {
            let c = Cid::dag_cbor(r);
            tree.insert_no_proof(p.as_bytes(), c).unwrap();
            blocks.push((c, r.clone()));
        }
        let data = tree.write_diff_blocks(&mut blocks).unwrap();
        Repo { commit: commit(data), data, blocks: blocks.into_iter().collect() }
    }

    fn repo(n: usize) -> Repo {
        let recs: Vec<(String, Vec<u8>)> = (0..n)
            .map(|i| {
                let p = format!("com.example.{}/{i:06}", ["a", "b", "c"][i % 3]);
                let r = record(&p);
                (p, r)
            })
            .collect();
        repo_of(&recs)
    }

    impl Repo {
        fn streamed(&self) -> Vec<u8> {
            crate::car_order::write_car((self.commit.0, &self.commit.1), self.data, &self.blocks).unwrap()
        }

        /// Commit, then the other blocks by CID.
        fn cid_ordered(&self) -> Vec<u8> {
            let mut out = Vec::new();
            car::write_header(&mut out, &self.commit.0);
            car::write_block(&mut out, &self.commit.0, &self.commit.1);
            let mut cs: Vec<&Cid> = self.blocks.keys().collect();
            cs.sort_by_key(|c| c.to_bytes());
            for c in cs {
                car::write_block(&mut out, c, &self.blocks[c]);
            }
            out
        }
    }

    /// Feeds `car` in chunks of `chunk` bytes, as a request body would.
    fn run(car: &[u8], chunk: usize) -> (XResult<(Vec<ImportedRecord>, Tree)>, &'static str) {
        let (tx, rx) = mpsc::channel(car.len() / chunk + 2);
        for c in car.chunks(chunk) {
            tx.try_send(Some(Bytes::copy_from_slice(c))).unwrap();
        }
        tx.try_send(None).unwrap();
        parse(rx)
    }

    fn summary(r: &XResult<(Vec<ImportedRecord>, Tree)>) -> Result<(Vec<ImportedRecord>, Cid), String> {
        match r {
            Ok((recs, t)) => Ok((recs.clone(), t.clone().root_cid().unwrap())),
            Err(e) => Err(format!("{} {}: {}", e.status, e.error, e.message)),
        }
    }

    fn buffered(car: &[u8]) -> Result<(Vec<ImportedRecord>, Cid), String> {
        summary(&parse_import(&Bytes::copy_from_slice(car)))
    }

    /// A streamed CAR takes the fast path at any chunking (blocks split
    /// across chunks included), with the buffered parse's result.
    #[test]
    fn streamed_car_takes_the_fast_path() {
        for n in [0, 1, 7, 500] {
            let r = repo(n);
            let car = r.streamed();
            let want = buffered(&car);
            assert!(want.is_ok());
            for chunk in [1, 3, 64, 4096, car.len()] {
                let (got, path) = run(&car, chunk);
                assert_eq!(path, "stream", "n={n} chunk={chunk}");
                assert_eq!(summary(&got), want, "n={n} chunk={chunk}");
            }
        }
    }

    /// Other orders fall back, with the same result.
    #[test]
    fn other_orders_fall_back() {
        let r = repo(300);
        let car = r.cid_ordered();
        let (got, path) = run(&car, 1000);
        assert_eq!(path, "buffered");
        assert_eq!(summary(&got), buffered(&r.streamed()));
        // a streamed CAR with two blocks swapped
        let streamed = r.streamed();
        let (_, blocks) = car::read_car(&streamed).unwrap();
        let mut blocks: Vec<(Cid, Vec<u8>)> = blocks.into_iter().map(|(c, b)| (c, b.to_vec())).collect();
        let k = blocks.len() / 2;
        blocks.swap(k, k + 1);
        let mut car = Vec::new();
        car::write_header(&mut car, &r.commit.0);
        for (c, b) in &blocks {
            car::write_block(&mut car, c, b);
        }
        let (got, path) = run(&car, 1000);
        assert_eq!(path, "buffered");
        assert_eq!(summary(&got), buffered(&r.streamed()));
    }

    /// Extra blocks after the tree are only hash-checked; a record under two
    /// keys is repeated at each, or (deduplicated) falls back.
    #[test]
    fn trailing_blocks_and_shared_records() {
        let r = repo(50);
        let mut car = r.streamed();
        car::write_block(&mut car, &Cid::raw(b"extra"), b"extra");
        let (got, path) = run(&car, 100);
        assert_eq!(path, "stream");
        assert_eq!(summary(&got), buffered(&car));
        let mut bad = r.streamed();
        car::write_block(&mut bad, &Cid::raw(b"extra"), b"other");
        let (got, path) = run(&bad, 100);
        assert_eq!(path, "buffered");
        assert!(summary(&got).unwrap_err().contains("block does not match its cid"));

        let same = record("com.example.a/x");
        let recs: Vec<(String, Vec<u8>)> =
            (0..40).map(|i| (format!("com.example.a/{i:03}"), if i % 10 == 0 { same.clone() } else { record(&format!("com.example.a/{i:03}")) })).collect();
        let r = repo_of(&recs);
        let car = r.streamed();
        let (got, path) = run(&car, 100);
        assert_eq!(path, "stream");
        assert_eq!(summary(&got), buffered(&car));
        assert_eq!(summary(&got).unwrap().0.len(), 40);
        let (_, blocks) = car::read_car(&car).unwrap();
        let mut seen = std::collections::HashSet::new();
        let mut dedup = Vec::new();
        car::write_header(&mut dedup, &r.commit.0);
        for (c, b) in blocks {
            if seen.insert(c) {
                car::write_block(&mut dedup, &c, b);
            }
        }
        let (got, path) = run(&dedup, 100);
        assert_eq!(path, "buffered");
        assert_eq!(summary(&got), buffered(&car));
    }

    /// Refused CARs fail with the buffered parse's error.
    #[test]
    fn refusals_match_the_buffered_parse() {
        let r = repo(200);
        let good = r.streamed();
        let (_, blocks) = car::read_car(&good).unwrap();
        let blocks: Vec<(Cid, Vec<u8>)> = blocks.into_iter().map(|(c, b)| (c, b.to_vec())).collect();
        let build = |bs: &[(Cid, Vec<u8>)]| {
            let mut car = Vec::new();
            car::write_header(&mut car, &r.commit.0);
            for (c, b) in bs {
                car::write_block(&mut car, c, b);
            }
            car
        };
        let rec_at = blocks.iter().position(|(c, b)| *c != r.commit.0 && mst::decode_node(b, *c).is_err()).unwrap();
        let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();
        // a record block replaced by another record (wrong CID for its leaf)
        let mut bs = blocks.clone();
        let other = record("com.example.zzz/x");
        bs[rec_at] = (Cid::dag_cbor(&other), other);
        cases.push(("wrong record cid", build(&bs)));
        // a record's bytes changed under its CID
        let mut bs = blocks.clone();
        bs[rec_at].1.push(0);
        cases.push(("bad record bytes", build(&bs)));
        // a missing record block, and a missing node
        let mut bs = blocks.clone();
        bs.remove(rec_at);
        cases.push(("missing record", build(&bs)));
        let mut bs = blocks.clone();
        let node_at = blocks.iter().rposition(|(c, b)| mst::decode_node(b, *c).is_ok()).unwrap();
        bs.remove(node_at);
        cases.push(("missing node", build(&bs)));
        // truncated mid-block, and at a block boundary
        cases.push(("truncated", good[..good.len() - 7].to_vec()));
        cases.push(("truncated at a block", build(&blocks[..blocks.len() - 1])));
        // a record too big, and not CBOR
        let mut tree = Tree::new();
        let big = {
            let m = vec![("$type".to_string(), Value::Text("com.example.a".into())), ("b".to_string(), Value::Bytes(vec![1; (2 << 20) + 1]))];
            Value::Map(m).to_cbor()
        };
        let junk = b"\xff\xff".to_vec();
        for (name, rec) in [("record too large", big), ("record not cbor", junk)] {
            let c = Cid::dag_cbor(&rec);
            tree.insert_no_proof(b"com.example.a/x", c).unwrap();
            let mut bs = vec![(c, rec)];
            let data = tree.write_diff_blocks(&mut bs).unwrap();
            let map: HashMap<Cid, Vec<u8>> = bs.into_iter().collect();
            let cm = commit(data);
            cases.push((name, crate::car_order::write_car((cm.0, &cm.1), data, &map).unwrap()));
        }
        // a header with two roots, and garbage
        let mut two = Vec::new();
        let mut h = Vec::new();
        crate::cbor::write_map_head(&mut h, 2);
        crate::cbor::write_text(&mut h, "roots");
        crate::cbor::write_array_head(&mut h, 2);
        crate::cbor::write_cid(&mut h, &r.commit.0);
        crate::cbor::write_cid(&mut h, &r.commit.0);
        crate::cbor::write_text(&mut h, "version");
        crate::cbor::write_uint(&mut h, 1);
        car::write_varint(&mut two, h.len() as u64);
        two.extend_from_slice(&h);
        for (c, b) in &blocks {
            car::write_block(&mut two, c, b);
        }
        cases.push(("two roots", two));
        cases.push(("garbage", vec![0xff; 20]));
        cases.push(("empty", Vec::new()));
        for (name, car) in cases {
            let want = buffered(&car);
            assert!(want.is_err(), "{name}: {want:?}");
            for chunk in [1, 100, car.len().max(1)] {
                let (got, path) = run(&car, chunk);
                assert_eq!(path, "buffered", "{name}");
                assert_eq!(summary(&got).map(|_| ()), want.clone().map(|_| ()), "{name}");
            }
        }
    }

    /// A node linking one child twice: the stream has to send the child again
    /// for the second link (work stays linear in the body), so one sent once
    /// is a departure, and the buffered parse refuses the DAG.
    #[test]
    fn dag_mst_falls_back_and_is_refused() {
        let k1 = (0..).map(|i| format!("com.example.a/{i}")).find(|k| mst::height_for_key(k.as_bytes()) == 1).unwrap();
        let rec = record("com.example.a/x");
        let rc = Cid::dag_cbor(&rec);
        let mut leaf = Vec::new();
        let n = mst::Node::clean(0, vec![mst::Entry::Value { key: Arc::from(&b"com.example.a/x"[..]), val: rc }], None);
        mst::encode_node(&n, &mut leaf).unwrap();
        let lc = Cid::dag_cbor(&leaf);
        let child = || mst::Entry::Child { node: None, cid: Some(lc) };
        let entries = vec![child(), mst::Entry::Value { key: Arc::from(k1.as_bytes()), val: rc }, child()];
        let mut parent = Vec::new();
        mst::encode_node(&mst::Node::clean(1, entries, None), &mut parent).unwrap();
        let pc = Cid::dag_cbor(&parent);
        let cm = commit(pc);
        let mut car = Vec::new();
        car::write_header(&mut car, &cm.0);
        for (c, b) in [(cm.0, &cm.1), (pc, &parent), (lc, &leaf), (rc, &rec), (rc, &rec)] {
            car::write_block(&mut car, &c, b);
        }
        let (got, path) = run(&car, 50);
        assert_eq!(path, "buffered");
        let want = buffered(&car);
        assert!(want.as_ref().unwrap_err().contains("could not load MST"), "{want:?}");
        assert_eq!(summary(&got).map(|_| ()), want.map(|_| ()));
    }

    /// A well-formed stream of a non-canonical tree (a key-less root over the
    /// one leaf) falls back and is refused.
    #[test]
    fn non_canonical_tree_falls_back_and_is_refused() {
        let leaf_rec = record("com.example.a/x");
        let rc = Cid::dag_cbor(&leaf_rec);
        let mut leaf = Vec::new();
        let n = mst::Node::clean(0, vec![mst::Entry::Value { key: Arc::from(&b"com.example.a/x"[..]), val: rc }], None);
        mst::encode_node(&n, &mut leaf).unwrap();
        let lc = Cid::dag_cbor(&leaf);
        let entries = vec![
            mst::Entry::Child { node: None, cid: Some(lc) },
        ];
        let mut parent = Vec::new();
        mst::encode_node(&mst::Node::clean(1, entries, None), &mut parent).unwrap();
        let pc = Cid::dag_cbor(&parent);
        let cm = commit(pc);
        let mut car = Vec::new();
        car::write_header(&mut car, &cm.0);
        for (c, b) in [(cm.0, &cm.1), (pc, &parent), (lc, &leaf), (rc, &leaf_rec)] {
            car::write_block(&mut car, &c, b);
        }
        let (got, path) = run(&car, 50);
        assert_eq!(path, "buffered");
        assert_eq!(summary(&got).map(|_| ()), buffered(&car).map(|_| ()));
        assert!(got.is_err());
    }
}
