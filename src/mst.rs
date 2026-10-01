//! Merkle Search Tree, ported from indigo's `atproto/repo/mst` so that tree
//! shapes and sync 1.1 proof block sets match the reference implementation.
//!
//! Nodes are `Arc`-shared and mutated copy-on-write (`Arc::make_mut`), so a
//! snapshot of the root (for exports, or inverting a commit) costs one refcount.
//! Keys are `Arc<[u8]>`, so copying a node on write bumps refcounts instead of
//! copying every key.
//!
//! `dirty` means "this node's block must be emitted in the next diff".
//! Mutations mark the nodes they rewrite (and drop their cached encoding);
//! `prove_mutation` also marks neighbouring nodes that a verifier needs to
//! invert the operation, whose cached encoding stays valid. Written internal
//! nodes keep their encoded block (`bytes`), so exports, proofs and getBlocks
//! copy those blocks instead of re-encoding them (leaves re-encode).

use crate::cbor;
use crate::cid::Cid;
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum MstError {
    #[error("MST is not complete")]
    Partial,
    #[error("invalid MST structure: {0}")]
    Invalid(&'static str),
    #[error("invalid MST key")]
    InvalidKey,
}

type Result<T> = std::result::Result<T, MstError>;

pub const MAX_KEY_BYTES: usize = 1024;

/// Deepest tree accepted from a block set (nodes on a root-to-leaf path). A
/// key's height counts its hash's leading zero 2-bit pairs, so 2^32 keys
/// give a tree ~16 levels deep, and a key of height 64 would take a 128-bit
/// zero hash prefix. The bound keeps every recursive walk off the end of the
/// stack: a chain of `{e: [], l: child}` nodes is otherwise unbounded.
pub const MAX_DEPTH: usize = 64;

#[derive(Clone, Debug)]
pub struct Node {
    pub height: i32,
    pub entries: Vec<Entry>,
    pub cid: Option<Cid>,
    pub dirty: bool,
    /// Placeholder for a node known only by CID (partial trees).
    pub stub: bool,
    /// The node's encoded block (hashing to `cid`), kept from its last write
    /// while its content is unchanged. Only internal nodes (height >= 1)
    /// keep one: leaves are ~3/4 of the nodes and bytes, and every proof
    /// path has just one.
    pub bytes: Option<Arc<[u8]>>,
}

#[derive(Clone, Debug)]
pub enum Entry {
    Value {
        key: Arc<[u8]>,
        val: Cid,
    },
    /// `node` is None in partial trees; `cid` is authoritative only then
    /// (or once `node` has been written).
    Child {
        node: Option<Arc<Node>>,
        cid: Option<Cid>,
    },
}

impl Entry {
    fn is_child(&self) -> bool {
        matches!(self, Entry::Child { .. })
    }
    fn key(&self) -> Option<&[u8]> {
        match self {
            Entry::Value { key, .. } => Some(key),
            _ => None,
        }
    }
    fn child(node: Arc<Node>) -> Entry {
        Entry::Child {
            node: Some(node),
            cid: None,
        }
    }
}

pub fn height_for_key(key: &[u8]) -> i32 {
    let hv = Sha256::digest(key);
    let mut height = 0;
    for &b in hv.iter() {
        if b & 0xC0 != 0 {
            break;
        }
        if b == 0 {
            height += 4;
            continue;
        }
        if b & 0xFC == 0 {
            height += 3;
        } else if b & 0xF0 == 0 {
            height += 2;
        } else {
            height += 1;
        }
        break;
    }
    height
}

fn valid_key(key: &[u8]) -> bool {
    !key.is_empty() && key.len() <= MAX_KEY_BYTES
}

impl Node {
    fn empty(height: i32) -> Node {
        Node {
            height,
            entries: Vec::new(),
            cid: None,
            dirty: true,
            stub: false,
            bytes: None,
        }
    }

    /// Marks a content change: re-encode and emit in the next diff.
    fn touch(&mut self) {
        self.dirty = true;
        self.bytes = None;
    }

    /// The node's block: the cached encoding, or a fresh one.
    fn block(&self) -> Result<std::borrow::Cow<'_, [u8]>> {
        if let Some(b) = &self.bytes {
            return Ok(std::borrow::Cow::Borrowed(b));
        }
        let mut buf = Vec::with_capacity(64 + self.entries.len() * 80);
        encode_node(self, &mut buf)?;
        Ok(std::borrow::Cow::Owned(buf))
    }

    /// The smallest key in this subtree (None for an empty or partial one).
    fn first_key(&self) -> Option<&Arc<[u8]>> {
        let mut n = self;
        loop {
            match n.entries.first()? {
                Entry::Value { key, .. } => return Some(key),
                Entry::Child { node, .. } => n = node.as_ref()?,
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn find_existing_entry(&self, key: &[u8]) -> Option<usize> {
        self.entries.iter().position(|e| e.key() == Some(key))
    }

    fn find_existing_child(&self, key: &[u8]) -> Option<usize> {
        let mut idx = None;
        for (i, e) in self.entries.iter().enumerate() {
            match e {
                Entry::Child { .. } => idx = Some(i),
                Entry::Value { key: k, .. } => {
                    if key <= &k[..] {
                        break;
                    }
                    idx = None;
                }
            }
        }
        idx
    }

    /// Returns (index, needs_split).
    fn find_insertion_index(&self, key: &[u8]) -> Result<(usize, bool)> {
        if self.stub {
            return Err(MstError::Partial);
        }
        for (i, e) in self.entries.iter().enumerate() {
            match e {
                Entry::Value { key: k, .. } => {
                    if key < &k[..] {
                        return Ok((i, false));
                    }
                }
                Entry::Child { node, .. } => {
                    if let Some(Entry::Value { key: nk, .. }) = self.entries.get(i + 1) {
                        if key > &nk[..] {
                            continue;
                        }
                    }
                    let child = node.as_ref().ok_or(MstError::Partial)?;
                    match child.compare_key(key)? {
                        Ordering::Less => return Ok((i, false)),
                        Ordering::Greater => continue,
                        Ordering::Equal => return Ok((i, true)),
                    }
                }
            }
        }
        Ok((self.entries.len(), false))
    }

    /// Where `key` falls relative to the key range of this subtree:
    /// Less = before all keys, Greater = after all, Equal = within.
    fn compare_key(&self, key: &[u8]) -> Result<Ordering> {
        if self.stub {
            return Err(MstError::Partial);
        }
        if self.is_empty() {
            return Err(MstError::Invalid("can't determine key range of empty node"));
        }
        if let Some(Entry::Value { key: k, .. }) = self.entries.first() {
            if key < &k[..] {
                return Ok(Ordering::Less);
            }
        }
        if let Some(Entry::Value { key: k, .. }) = self.entries.last() {
            if key > &k[..] {
                return Ok(Ordering::Greater);
            }
        }
        let n = self.entries.len();
        for (i, e) in self.entries.iter().enumerate() {
            match e {
                Entry::Value { key: k, .. } => {
                    if key < &k[..] {
                        return Ok(Ordering::Equal);
                    }
                }
                Entry::Child { node, .. } => {
                    if let Some(Entry::Value { key: nk, .. }) = self.entries.get(i + 1) {
                        if key > &nk[..] {
                            continue;
                        }
                    }
                    let child = node.as_ref().ok_or(MstError::Partial)?;
                    let order = child.compare_key(key)?;
                    if i == 0 && order == Ordering::Less {
                        return Ok(Ordering::Less);
                    }
                    if i == n - 1 && order == Ordering::Greater {
                        return Ok(Ordering::Greater);
                    }
                    return Ok(Ordering::Equal);
                }
            }
        }
        Ok(Ordering::Equal)
    }

    /// Same as `compare_key`, but marks every node it inspects dirty (proof).
    fn compare_key_mark(&mut self, key: &[u8]) -> Result<Ordering> {
        if self.stub {
            return Err(MstError::Partial);
        }
        if self.is_empty() {
            return Err(MstError::Invalid("can't determine key range of empty node"));
        }
        self.dirty = true;
        if let Some(Entry::Value { key: k, .. }) = self.entries.first() {
            if key < &k[..] {
                return Ok(Ordering::Less);
            }
        }
        if let Some(Entry::Value { key: k, .. }) = self.entries.last() {
            if key > &k[..] {
                return Ok(Ordering::Greater);
            }
        }
        let n = self.entries.len();
        for i in 0..n {
            if let Entry::Value { key: k, .. } = &self.entries[i] {
                if key < &k[..] {
                    return Ok(Ordering::Equal);
                }
                continue;
            }
            if let Some(Entry::Value { key: nk, .. }) = self.entries.get(i + 1) {
                if key > &nk[..] {
                    continue;
                }
            }
            let Entry::Child { node, .. } = &mut self.entries[i] else {
                unreachable!()
            };
            let child = Arc::make_mut(node.as_mut().ok_or(MstError::Partial)?);
            let order = child.compare_key_mark(key)?;
            if i == 0 && order == Ordering::Less {
                return Ok(Ordering::Less);
            }
            if i == n - 1 && order == Ordering::Greater {
                return Ok(Ordering::Greater);
            }
            return Ok(Ordering::Equal);
        }
        Ok(Ordering::Equal)
    }

    fn get(&self, key: &[u8], height: i32) -> Result<Option<Cid>> {
        if self.stub {
            return Err(MstError::Partial);
        }
        if height > self.height {
            return Ok(None);
        }
        if height < self.height {
            return match self.find_existing_child(key) {
                Some(idx) => match &self.entries[idx] {
                    Entry::Child { node: Some(c), .. } => c.get(key, height),
                    _ => Err(MstError::Partial),
                },
                None => Ok(None),
            };
        }
        Ok(self
            .find_existing_entry(key)
            .map(|i| match &self.entries[i] {
                Entry::Value { val, .. } => *val,
                _ => unreachable!(),
            }))
    }
}

/// Marks the nodes adjacent to `key` dirty so they are included as a
/// "covering proof" for the mutation at `key`.
fn prove_mutation(n: &mut Node, key: &[u8]) -> Result<()> {
    let len = n.entries.len();
    for i in 0..len {
        if let Entry::Value { key: k, .. } = &n.entries[i] {
            if key < &k[..] {
                return Ok(());
            }
            continue;
        }
        if let Some(Entry::Value { key: nk, .. }) = n.entries.get(i + 1) {
            if key > &nk[..] {
                continue;
            }
        }
        let Entry::Child { node, .. } = &mut n.entries[i] else {
            unreachable!()
        };
        let child = Arc::make_mut(node.as_mut().ok_or(MstError::Partial)?);
        match child.compare_key_mark(key)? {
            Ordering::Greater => continue,
            Ordering::Less => return Ok(()),
            Ordering::Equal => return prove_mutation(child, key),
        }
    }
    Ok(())
}

fn ignore_partial(r: Result<()>) -> Result<()> {
    match r {
        Err(MstError::Partial) => Ok(()),
        other => other,
    }
}

fn insert(
    mut n: Arc<Node>,
    key: &[u8],
    val: Cid,
    height: i32,
    prove: bool,
) -> Result<(Arc<Node>, Option<Cid>)> {
    if n.stub {
        return Err(MstError::Partial);
    }
    if height > n.height {
        return insert_parent(n, key, val, height, prove);
    }
    if height < n.height {
        return insert_child(n, key, val, height, prove);
    }
    if let Some(idx) = n.find_existing_entry(key) {
        if let Entry::Value { val: existing, .. } = &n.entries[idx] {
            if *existing == val {
                return Ok((n, Some(val)));
            }
        }
        let nm = Arc::make_mut(&mut n);
        let Entry::Value { val: existing, .. } = &mut nm.entries[idx] else {
            unreachable!()
        };
        let prev = *existing;
        *existing = val;
        nm.touch();
        return Ok((n, Some(prev)));
    }

    let (idx, split) = n.find_insertion_index(key)?;
    let nm = Arc::make_mut(&mut n);
    nm.touch();
    if prove {
        ignore_partial(prove_mutation(nm, key))?;
    }
    let new_entry = Entry::Value {
        key: key.into(),
        val,
    };
    if !split {
        nm.entries.insert(idx, new_entry);
        return Ok((n, None));
    }
    let child = match &nm.entries[idx] {
        Entry::Child { node: Some(c), .. } => c.clone(),
        _ => return Err(MstError::Partial),
    };
    let (left, right) = split_node(&child, key)?;
    nm.entries.splice(
        idx..idx + 1,
        [Entry::child(left), new_entry, Entry::child(right)],
    );
    Ok((n, None))
}

fn split_entries(n: &Node, idx: usize) -> Result<(Arc<Node>, Arc<Node>)> {
    if idx == 0 || idx >= n.entries.len() {
        return Err(MstError::Invalid("splitting at one end of entries"));
    }
    let left = Node {
        entries: n.entries[..idx].to_vec(),
        ..Node::empty(n.height)
    };
    let right = Node {
        entries: n.entries[idx..].to_vec(),
        ..Node::empty(n.height)
    };
    Ok((Arc::new(left), Arc::new(right)))
}

fn split_node(n: &Node, key: &[u8]) -> Result<(Arc<Node>, Arc<Node>)> {
    if n.is_empty() {
        return Err(MstError::Invalid("tried to split an empty node"));
    }
    let (idx, split) = n.find_insertion_index(key)?;
    if !split {
        return split_entries(n, idx);
    }
    let child = match &n.entries[idx] {
        Entry::Child { node: Some(c), .. } => c,
        _ => return Err(MstError::Partial),
    };
    let (lower_left, lower_right) = split_node(child, key)?;
    let mut le = n.entries[..idx].to_vec();
    le.push(Entry::child(lower_left));
    let mut re = vec![Entry::child(lower_right)];
    re.extend_from_slice(&n.entries[idx + 1..]);
    Ok((
        Arc::new(Node {
            entries: le,
            ..Node::empty(n.height)
        }),
        Arc::new(Node {
            entries: re,
            ..Node::empty(n.height)
        }),
    ))
}

fn insert_parent(
    n: Arc<Node>,
    key: &[u8],
    val: Cid,
    height: i32,
    prove: bool,
) -> Result<(Arc<Node>, Option<Cid>)> {
    let parent = if n.is_empty() {
        Node::empty(height)
    } else {
        let h = n.height + 1;
        Node {
            entries: vec![Entry::child(n)],
            ..Node::empty(h)
        }
    };
    insert(Arc::new(parent), key, val, height, prove)
}

fn insert_child(
    mut n: Arc<Node>,
    key: &[u8],
    val: Cid,
    height: i32,
    prove: bool,
) -> Result<(Arc<Node>, Option<Cid>)> {
    if let Some(idx) = n.find_existing_child(key) {
        let nm = Arc::make_mut(&mut n);
        let Entry::Child { node, .. } = &mut nm.entries[idx] else {
            unreachable!()
        };
        let child = node.take().ok_or(MstError::Partial)?;
        let (new_child, prev) = insert(child, key, val, height, prove)?;
        *node = Some(new_child);
        if prev == Some(val) {
            return Ok((n, Some(val)));
        }
        nm.touch();
        return Ok((n, prev));
    }
    let (idx, split) = n.find_insertion_index(key)?;
    if split {
        return Err(MstError::Invalid("unexpected split when inserting child"));
    }
    let nm = Arc::make_mut(&mut n);
    nm.touch();
    let (new_child, _) = insert(
        Arc::new(Node::empty(nm.height - 1)),
        key,
        val,
        height,
        prove,
    )?;
    nm.entries.insert(idx, Entry::child(new_child));
    Ok((n, None))
}

fn remove(
    mut n: Arc<Node>,
    key: &[u8],
    height: Option<i32>,
    prove: bool,
) -> Result<(Arc<Node>, Option<Cid>)> {
    if n.stub {
        return Err(MstError::Partial);
    }
    let top = height.is_none();
    let height = height.unwrap_or_else(|| height_for_key(key));
    if height > n.height {
        return Ok((n, None));
    }
    if height < n.height {
        return remove_child(n, key, height, prove);
    }
    let Some(idx) = n.find_existing_entry(key) else {
        return Ok((n, None));
    };
    let nm = Arc::make_mut(&mut n);
    nm.touch();
    let Entry::Value { val: prev, .. } = nm.entries[idx] else {
        unreachable!()
    };

    let len = nm.entries.len();
    if idx > 0 && idx + 1 < len && nm.entries[idx - 1].is_child() && nm.entries[idx + 1].is_child()
    {
        let (left, right) = match (&nm.entries[idx - 1], &nm.entries[idx + 1]) {
            (Entry::Child { node: Some(l), .. }, Entry::Child { node: Some(r), .. }) => {
                (l.clone(), r.clone())
            }
            _ => return Err(MstError::Partial),
        };
        let merged = merge_nodes(&left, &right)?;
        nm.entries.drain(idx..idx + 2);
        nm.entries[idx - 1] = Entry::child(merged);
    } else {
        nm.entries.remove(idx);
    }

    if prove {
        ignore_partial(prove_mutation(nm, key))?;
    }

    if top {
        loop {
            if n.entries.len() != 1 || !n.entries[0].is_child() {
                break;
            }
            let Entry::Child { node, cid } = &n.entries[0] else {
                unreachable!()
            };
            n = match (node, cid) {
                (Some(c), _) => c.clone(),
                (None, Some(c)) => Arc::new(Node {
                    height: n.height - 1,
                    entries: Vec::new(),
                    cid: Some(*c),
                    dirty: false,
                    stub: true,
                    bytes: None,
                }),
                (None, None) => return Err(MstError::Partial),
            };
        }
    }
    Ok((n, Some(prev)))
}

fn merge_nodes(left: &Node, right: &Node) -> Result<Arc<Node>> {
    let idx = left.entries.len();
    let mut entries = Vec::with_capacity(left.entries.len() + right.entries.len());
    entries.extend_from_slice(&left.entries);
    entries.extend_from_slice(&right.entries);
    if idx > 0 && idx < entries.len() && entries[idx - 1].is_child() && entries[idx].is_child() {
        let merged = match (&entries[idx - 1], &entries[idx]) {
            (Entry::Child { node: Some(l), .. }, Entry::Child { node: Some(r), .. }) => {
                merge_nodes(l, r)?
            }
            _ => return Err(MstError::Partial),
        };
        entries[idx - 1] = Entry::child(merged);
        entries.remove(idx);
    }
    Ok(Arc::new(Node {
        entries,
        ..Node::empty(left.height)
    }))
}

fn remove_child(
    mut n: Arc<Node>,
    key: &[u8],
    height: i32,
    prove: bool,
) -> Result<(Arc<Node>, Option<Cid>)> {
    // the key exists below (checked once by Tree::remove_inner, so a no-op
    // delete doesn't copy-on-write the path)
    let Some(idx) = n.find_existing_child(key) else {
        return Ok((n, None));
    };
    let nm = Arc::make_mut(&mut n);
    let Entry::Child { node, .. } = &mut nm.entries[idx] else {
        unreachable!()
    };
    let child = node.take().ok_or(MstError::Partial)?;
    let (new_child, prev) = remove(child, key, Some(height), prove)?;
    if !new_child.is_empty() {
        *node = Some(new_child);
    } else {
        nm.entries.remove(idx);
    }
    nm.touch();
    Ok((n, prev))
}

// ---------- encoding ----------

fn count_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

fn child_cid(e: &Entry) -> Option<Cid> {
    match e {
        Entry::Child { node: Some(n), cid } => n.cid.or(*cid),
        Entry::Child { node: None, cid } => *cid,
        _ => None,
    }
}

/// Encodes a node whose children all have CIDs computed.
pub fn encode_node(n: &Node, out: &mut Vec<u8>) -> Result<()> {
    let nvals = n.entries.iter().filter(|e| !e.is_child()).count();
    let mut left = None;
    let mut start = 0;
    if let Some(e @ Entry::Child { .. }) = n.entries.first() {
        left = Some(child_cid(e).ok_or(MstError::Invalid("child without cid"))?);
        start = 1;
    }
    cbor::write_map_head(out, 2);
    cbor::write_text(out, "e");
    cbor::write_array_head(out, nvals);
    let mut prev_key: &[u8] = &[];
    let mut i = start;
    while i < n.entries.len() {
        let Entry::Value { key, val } = &n.entries[i] else {
            return Err(MstError::Invalid("two adjacent child pointers"));
        };
        let right = match n.entries.get(i + 1) {
            Some(e @ Entry::Child { .. }) => {
                i += 1;
                Some(child_cid(e).ok_or(MstError::Invalid("child without cid"))?)
            }
            _ => None,
        };
        let p = count_prefix_len(prev_key, key);
        cbor::write_map_head(out, 4);
        cbor::write_text(out, "k");
        cbor::write_bytes(out, &key[p..]);
        cbor::write_text(out, "p");
        cbor::write_uint(out, p as u64);
        cbor::write_text(out, "t");
        cbor::write_opt_cid(out, right.as_ref());
        cbor::write_text(out, "v");
        cbor::write_cid(out, val);
        prev_key = key;
        i += 1;
    }
    cbor::write_text(out, "l");
    cbor::write_opt_cid(out, left.as_ref());
    Ok(())
}

/// Recomputes CIDs of dirty nodes, emitting their blocks into `out`. A node
/// marked only as proof (content and children's CIDs unchanged) emits its
/// cached block without re-encoding or re-hashing it.
fn write_blocks(
    n: &mut Arc<Node>,
    out: &mut Option<&mut Vec<(Cid, Vec<u8>)>>,
    refs: &mut Option<&mut Vec<(Cid, NodeRef)>>,
    depth: usize,
) -> Result<Cid> {
    if depth > MAX_DEPTH {
        return Err(MstError::Invalid("tree too deep"));
    }
    if n.stub {
        return Err(MstError::Invalid("nil tree node"));
    }
    if !n.dirty {
        if let Some(c) = n.cid {
            return Ok(c);
        }
    }
    let nm = Arc::make_mut(n);
    let mut children_changed = false;
    for e in nm.entries.iter_mut() {
        if let Entry::Child { node: Some(c), cid } = e {
            let new = if c.dirty || c.cid.is_none() {
                write_blocks(c, out, refs, depth + 1)?
            } else {
                c.cid.ok_or(MstError::Invalid("child without cid"))?
            };
            children_changed |= *cid != Some(new);
            *cid = Some(new);
        }
    }
    if children_changed {
        nm.bytes = None;
    }
    let c = match (nm.cid, &nm.bytes) {
        (Some(c), Some(b)) => {
            if let Some(out) = out.as_mut() {
                out.push((c, b.to_vec()));
            }
            c
        }
        _ => {
            let mut buf = Vec::with_capacity(64 + nm.entries.len() * 80);
            encode_node(nm, &mut buf)?;
            let c = Cid::dag_cbor(&buf);
            nm.cid = Some(c);
            // leaves (most nodes, most bytes) re-encode on demand
            if nm.height >= 1 {
                nm.bytes = Some(Arc::from(&buf[..]));
            }
            if let Some(out) = out.as_mut() {
                out.push((c, buf));
            }
            c
        }
    };
    nm.dirty = false;
    if let (Some(refs), Some(k)) = (refs.as_mut(), nm.first_key()) {
        refs.push((c, (k.clone(), nm.height)));
    }
    Ok(c)
}

// ---------- decoding (partial trees from a block set) ----------

/// Decodes one node block, checking that it is the canonical encoding of a
/// valid node: exactly the `e` and `l` fields (both required, `l` and each
/// `t` a link or null, as the reference's NodeData schema), entries with
/// exactly `k`/`p`/`t`/`v`, keys strictly ascending and all of one height,
/// maximal prefix lengths (the first entry's is 0), and no child pointers
/// in a height-0 node. Heights across nodes are checked by `load_from_blocks`.
pub fn decode_node(data: &[u8], c: Cid) -> std::result::Result<Node, MstError> {
    use cbor::Value;
    let v = Value::decode(data).map_err(|_| MstError::Invalid("bad node cbor"))?;
    let link = |v: Option<&Value>| match v {
        Some(Value::Link(l)) => Ok(Some(*l)),
        Some(Value::Null) => Ok(None),
        _ => Err(MstError::Invalid("bad link")),
    };
    let fields = |v: &Value| match v {
        Value::Map(m) => m.len(),
        _ => 0,
    };
    if fields(&v) != 2 {
        return Err(MstError::Invalid("node must have exactly e and l"));
    }
    let mut entries = Vec::new();
    if let Some(l) = link(v.get("l"))? {
        entries.push(Entry::Child {
            node: None,
            cid: Some(l),
        });
    }
    let Some(Value::Array(es)) = v.get("e") else {
        return Err(MstError::Invalid("bad e"));
    };
    let mut prev: Vec<u8> = Vec::new();
    let mut height = -1;
    for (i, e) in es.iter().enumerate() {
        if fields(e) != 4 {
            return Err(MstError::Invalid("entry must have exactly k, p, t and v"));
        }
        let p = match e.get("p") {
            Some(Value::Int(p)) if *p >= 0 && (*p as usize) <= prev.len() => *p as usize,
            _ => return Err(MstError::Invalid("bad prefix len")),
        };
        let Some(Value::Bytes(k)) = e.get("k") else {
            return Err(MstError::Invalid("bad k"));
        };
        let Some(Value::Link(val)) = e.get("v") else {
            return Err(MstError::Invalid("bad v"));
        };
        let t = link(e.get("t"))?;
        let mut key = prev[..p].to_vec();
        key.extend_from_slice(k);
        if !valid_key(&key) {
            return Err(MstError::InvalidKey);
        }
        if i > 0 && key <= prev {
            return Err(MstError::Invalid("keys not in ascending order"));
        }
        // canonical prefix compression; for the first entry prev is empty, so p == 0
        if count_prefix_len(&prev, &key) != p {
            return Err(MstError::Invalid("non-canonical prefix len"));
        }
        let h = height_for_key(&key);
        if height < 0 {
            height = h;
        } else if h != height {
            return Err(MstError::Invalid("keys of different heights in one node"));
        }
        entries.push(Entry::Value {
            key: key.clone().into(),
            val: *val,
        });
        prev = key;
        if let Some(t) = t {
            entries.push(Entry::Child {
                node: None,
                cid: Some(t),
            });
        }
    }
    if height == 0 && entries.iter().any(Entry::is_child) {
        return Err(MstError::Invalid("child of a height-0 node"));
    }
    Ok(Node {
        height,
        entries,
        cid: Some(c),
        dirty: false,
        stub: false,
        bytes: None,
    })
}

/// Loads the subtree at `c` (`depth` nodes below the root). Children missing
/// from `blocks` stay unloaded (partial tree). A node without keys takes its
/// height from its child; every loaded child must be exactly one level below
/// its parent.
fn load_from_blocks(
    blocks: &HashMap<Cid, Vec<u8>>,
    c: Cid,
    depth: usize,
) -> Result<Option<Arc<Node>>> {
    if depth >= MAX_DEPTH {
        return Err(MstError::Invalid("tree too deep"));
    }
    let Some(data) = blocks.get(&c) else {
        return Ok(None);
    };
    let mut n = decode_node(data, c)?;
    if depth > 0 && n.entries.is_empty() {
        return Err(MstError::Invalid("empty child node"));
    }
    for e in n.entries.iter_mut() {
        if let Entry::Child {
            node,
            cid: Some(cc),
        } = e
        {
            if let Some(child) = load_from_blocks(blocks, *cc, depth + 1)? {
                if child.height >= 0 {
                    if n.height < 0 {
                        n.height = child.height + 1;
                    } else if child.height != n.height - 1 {
                        return Err(MstError::Invalid("child height is not parent height - 1"));
                    }
                }
                *node = Some(child);
            }
        }
    }
    Ok(Some(Arc::new(n)))
}

/// Pushes known heights down into loaded nodes without keys (whose subtrees
/// had no keys either). Depth is bounded by `load_from_blocks`.
fn ensure_heights(n: &mut Arc<Node>, depth: usize) -> Result<()> {
    debug_assert!(depth <= MAX_DEPTH);
    if n.height < 0 {
        return Ok(());
    }
    if n.height == 0 {
        // a key-less node pushed down to height 0 cannot have children either
        return match n.entries.iter().any(Entry::is_child) {
            true => Err(MstError::Invalid("child of a height-0 node")),
            false => Ok(()),
        };
    }
    let h = n.height;
    let nm = Arc::make_mut(n);
    for e in nm.entries.iter_mut() {
        if let Entry::Child { node: Some(c), .. } = e {
            if c.height < 0 {
                Arc::make_mut(c).height = h - 1;
            }
            ensure_heights(c, depth + 1)?;
        }
    }
    Ok(())
}

// ---------- tree ----------

#[derive(Clone, Debug)]
pub struct Tree {
    pub root: Arc<Node>,
}

impl Default for Tree {
    fn default() -> Self {
        Tree::new()
    }
}

impl Tree {
    pub fn new() -> Tree {
        Tree {
            root: Arc::new(Node::empty(0)),
        }
    }

    /// Inserts or updates; returns the previous value. Marks proof nodes.
    pub fn insert(&mut self, key: &[u8], val: Cid) -> Result<Option<Cid>> {
        self.insert_inner(key, val, true)
    }

    /// Insert without proof marking (bulk loads).
    pub fn insert_no_proof(&mut self, key: &[u8], val: Cid) -> Result<Option<Cid>> {
        self.insert_inner(key, val, false)
    }

    fn insert_inner(&mut self, key: &[u8], val: Cid, prove: bool) -> Result<Option<Cid>> {
        if !valid_key(key) {
            return Err(MstError::InvalidKey);
        }
        let height = height_for_key(key);
        let root = std::mem::replace(&mut self.root, Arc::new(Node::empty(0)));
        // An emptied tree can be left with a non-zero height; an empty node has no
        // fixed height, so restart it at the key's height to keep the shape canonical.
        let root = if root.is_empty() && !root.stub && root.height != height {
            Arc::new(Node::empty(height))
        } else {
            root
        };
        match insert(root.clone(), key, val, height, prove) {
            Ok((r, prev)) => {
                self.root = r;
                Ok(prev)
            }
            Err(e) => {
                self.root = root;
                Err(e)
            }
        }
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<Option<Cid>> {
        self.remove_inner(key, true)
    }

    fn remove_inner(&mut self, key: &[u8], prove: bool) -> Result<Option<Cid>> {
        if !valid_key(key) {
            return Err(MstError::InvalidKey);
        }
        if self.root.get(key, height_for_key(key))?.is_none() {
            return Ok(None);
        }
        let root = self.root.clone();
        let (r, prev) = remove(root, key, None, prove)?;
        self.root = r;
        Ok(prev)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Cid>> {
        if !valid_key(key) {
            return Err(MstError::InvalidKey);
        }
        self.root.get(key, height_for_key(key))
    }

    pub fn is_empty(&self) -> bool {
        self.root.is_empty()
    }

    /// Computes the root CID, clearing dirty flags without collecting blocks.
    pub fn root_cid(&mut self) -> Result<Cid> {
        if self.root.stub && !self.root.dirty {
            if let Some(c) = self.root.cid {
                return Ok(c);
            }
        }
        write_blocks(&mut self.root, &mut None, &mut None, 0)
    }

    /// Computes the root CID and returns every dirty block (new nodes + proof nodes).
    pub fn write_diff_blocks(&mut self, out: &mut Vec<(Cid, Vec<u8>)>) -> Result<Cid> {
        write_blocks(&mut self.root, &mut Some(out), &mut None, 0)
    }

    /// [`Tree::write_diff_blocks`], also reporting where each emitted node
    /// sits (for [`NodeIndex::advance`]).
    pub fn write_diff_blocks_with_refs(
        &mut self,
        out: &mut Vec<(Cid, Vec<u8>)>,
        refs: &mut Vec<(Cid, NodeRef)>,
    ) -> Result<Cid> {
        write_blocks(&mut self.root, &mut Some(out), &mut Some(refs), 0)
    }

    /// Where every node of a fully written tree sits (cid -> first key,
    /// height); an empty root has no key and is left out.
    pub fn node_refs(&self, out: &mut HashMap<Cid, NodeRef>) -> Result<()> {
        fn rec(n: &Node, out: &mut HashMap<Cid, NodeRef>, depth: usize) -> Result<()> {
            if depth > MAX_DEPTH {
                return Err(MstError::Invalid("tree too deep"));
            }
            let c = n.cid.ok_or(MstError::Invalid("unwritten node"))?;
            if let Some(k) = n.first_key() {
                out.insert(c, (k.clone(), n.height));
            }
            for e in &n.entries {
                if let Entry::Child { node: Some(c), .. } = e {
                    rec(c, out, depth + 1)?;
                }
            }
            Ok(())
        }
        rec(&self.root, out, 0)
    }

    /// Loads a (possibly partial) tree from a block set.
    pub fn load_from_blocks(blocks: &HashMap<Cid, Vec<u8>>, root: Cid) -> Result<Tree> {
        let mut r = load_from_blocks(blocks, root, 0)?.ok_or(MstError::Partial)?;
        ensure_heights(&mut r, 0)?;
        Ok(Tree { root: r })
    }

    /// Visits every (key, value) in key order.
    pub fn walk(&self, f: &mut dyn FnMut(&[u8], Cid)) {
        fn rec(n: &Node, f: &mut dyn FnMut(&[u8], Cid), depth: usize) {
            // loaded trees are bounded by load_from_blocks, built ones by key heights
            debug_assert!(depth <= MAX_DEPTH);
            for e in &n.entries {
                match e {
                    Entry::Value { key, val } => f(key, *val),
                    Entry::Child { node: Some(c), .. } => rec(c, f, depth + 1),
                    _ => {}
                }
            }
        }
        rec(&self.root, f, 0)
    }

    /// Visits every node block (cid, encoded bytes). The tree must be fully
    /// written (no dirty nodes), e.g. right after `root_cid`.
    pub fn walk_blocks(&self, f: &mut dyn FnMut(Cid, &[u8])) -> Result<()> {
        fn rec(
            n: &Node,
            buf: &mut Vec<u8>,
            f: &mut dyn FnMut(Cid, &[u8]),
            depth: usize,
        ) -> Result<()> {
            if depth > MAX_DEPTH {
                return Err(MstError::Invalid("tree too deep"));
            }
            let c = n.cid.ok_or(MstError::Invalid("unwritten node"))?;
            match &n.bytes {
                Some(b) if !n.dirty => f(c, b),
                _ => {
                    buf.clear();
                    encode_node(n, buf)?;
                    f(c, buf);
                }
            }
            for e in &n.entries {
                if let Entry::Child { node: Some(c), .. } = e {
                    rec(c, buf, f, depth + 1)?;
                }
            }
            Ok(())
        }
        let mut buf = Vec::with_capacity(1024);
        rec(&self.root, &mut buf, f, 0)
    }

    /// The block of node `cid`, looked up in `index` (which must cover this
    /// tree's version for a None to be final).
    pub fn find_node(&self, cid: &Cid, index: &NodeIndex) -> Result<Option<Vec<u8>>> {
        if self.root.cid == Some(*cid) {
            return Ok(Some(self.root.block()?.into_owned()));
        }
        match index.get(cid) {
            Some((key, height)) => self.node_block(cid, key, *height),
            None => Ok(None),
        }
    }

    /// The block of the node at `height` on the path to `key`, if that
    /// node's CID is `cid` (the tree must be fully written).
    pub fn node_block(&self, cid: &Cid, key: &[u8], height: i32) -> Result<Option<Vec<u8>>> {
        let mut n: &Node = &self.root;
        loop {
            if n.height <= height {
                return match n.height == height && n.cid == Some(*cid) {
                    true => Ok(Some(n.block()?.into_owned())),
                    false => Ok(None),
                };
            }
            match n.find_existing_child(key).map(|i| &n.entries[i]) {
                Some(Entry::Child { node: Some(c), .. }) => n = c,
                _ => return Ok(None),
            }
        }
    }

    /// Node blocks on the path from the root to `key` (inclusion or exclusion proof).
    pub fn proof_blocks(&self, key: &[u8]) -> Result<Vec<(Cid, Vec<u8>)>> {
        let height = height_for_key(key);
        let mut out = Vec::new();
        let mut n: &Node = &self.root;
        loop {
            out.push((n.cid.ok_or(MstError::Invalid("unwritten node"))?, n.block()?.into_owned()));
            if height >= n.height {
                return Ok(out);
            }
            match n.find_existing_child(key) {
                Some(idx) => match &n.entries[idx] {
                    Entry::Child { node: Some(c), .. } => n = c,
                    _ => return Err(MstError::Partial),
                },
                None => return Ok(out),
            }
        }
    }
}

// ---------- node index (getBlocks) ----------

/// Where a node sits: a key in its subtree (the first) and its height. The
/// node is the one at that height on the path from the root to the key.
pub type NodeRef = (Arc<[u8]>, i32);

/// Node CID -> [`NodeRef`] for one repo, so getBlocks finds MST nodes by
/// CID in O(depth) instead of walking the tree. Built from a tree version
/// (one walk), then advanced by each commit's written nodes; it only grows,
/// so it covers every version in `from..=to` (revs) and a miss for one of
/// those is final. Entries of replaced nodes linger until a rebuild;
/// lookups check the CID against the tree they read.
pub struct NodeIndex {
    map: HashMap<Cid, NodeRef>,
    pub from: u64,
    pub to: u64,
    /// Nodes at the last build: rebuild once stale entries outnumber them.
    live: usize,
}

impl NodeIndex {
    pub fn build(tree: &Tree, rev: u64) -> Result<NodeIndex> {
        let mut map = HashMap::new();
        tree.node_refs(&mut map)?;
        let live = map.len();
        Ok(NodeIndex { map, from: rev, to: rev, live })
    }

    pub fn covers(&self, rev: u64) -> bool {
        (self.from..=self.to).contains(&rev)
    }

    pub fn get(&self, cid: &Cid) -> Option<&NodeRef> {
        self.map.get(cid)
    }

    /// Adds the nodes written by commit `prev -> rev`. False (unchanged) if
    /// the index doesn't end at `prev`.
    pub fn advance(&mut self, prev: u64, rev: u64, written: &[(Cid, NodeRef)]) -> bool {
        if self.to != prev || rev < prev {
            return false;
        }
        self.map.extend(written.iter().cloned());
        self.to = rev;
        true
    }

    /// Mostly stale entries: drop and rebuild on the next miss.
    pub fn bloated(&self) -> bool {
        self.map.len() > 2 * self.live + 4096
    }
}

/// A repo's node index, shared by the repo worker (which advances it per
/// commit once anyone has asked for it) and getBlocks (which builds it).
#[derive(Default)]
pub struct NodeIndexCell {
    pub index: Option<NodeIndex>,
    /// Set by the first getBlocks that needed node blocks: from then on the
    /// worker reports each commit's written nodes.
    pub wanted: bool,
    /// The latest commits' written nodes (prev rev, rev, refs) while there
    /// is no index to advance, so a build from an older view can catch up.
    pub recent: std::collections::VecDeque<(u64, u64, Vec<(Cid, NodeRef)>)>,
}

/// Commits kept in [`NodeIndexCell::recent`].
const RECENT_COMMITS: usize = 64;

impl NodeIndexCell {
    /// Worker side: records a commit's written nodes.
    pub fn commit(&mut self, prev: u64, rev: u64, written: Vec<(Cid, NodeRef)>) {
        if let Some(ix) = &mut self.index {
            if ix.advance(prev, rev, &written) && !ix.bloated() {
                return;
            }
            self.index = None;
        }
        if self.recent.len() >= RECENT_COMMITS {
            self.recent.pop_front();
        }
        self.recent.push_back((prev, rev, written));
    }

    /// getBlocks side: installs an index built from a view, caught up with
    /// the commits recorded since, unless the current one reaches further.
    pub fn install(&mut self, mut ix: NodeIndex) {
        for (prev, rev, written) in &self.recent {
            ix.advance(*prev, *rev, written);
        }
        if self.index.as_ref().is_none_or(|cur| cur.to < ix.to) {
            self.recent.clear();
            self.index = Some(ix);
        }
    }
}

pub type SharedNodeIndex = Arc<parking_lot::Mutex<NodeIndexCell>>;

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{seq::SliceRandom, Rng, SeedableRng};
    use std::collections::BTreeMap;

    fn leaf() -> Cid {
        Cid::parse("bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454").unwrap()
    }

    #[test]
    fn empty_tree_cid() {
        let mut t = Tree::new();
        assert_eq!(
            t.root_cid().unwrap().to_string(),
            "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"
        );
    }

    #[test]
    fn known_heights() {
        // from the atproto MST interop tests
        assert_eq!(height_for_key(b""), 0);
        assert_eq!(height_for_key(b"asdf"), 0);
        assert_eq!(height_for_key(b"blue"), 1);
        assert_eq!(height_for_key(b"2653ae71"), 0);
        assert_eq!(height_for_key(b"88bfafc7"), 2);
        assert_eq!(height_for_key(b"2a92d355"), 4);
        assert_eq!(height_for_key(b"884976f5"), 6);
        assert_eq!(height_for_key(b"app.bsky.feed.post/454397e440ec"), 4);
        assert_eq!(height_for_key(b"app.bsky.feed.post/9adeb165882c"), 8);
    }

    #[derive(serde::Deserialize)]
    struct Fixture {
        comment: String,
        #[serde(rename = "leafValue")]
        leaf_value: String,
        keys: Vec<String>,
        adds: Vec<String>,
        dels: Vec<String>,
        #[serde(rename = "rootBeforeCommit")]
        root_before: String,
        #[serde(rename = "rootAfterCommit")]
        root_after: String,
        #[serde(rename = "blocksInProof")]
        blocks_in_proof: Vec<String>,
    }

    /// The atproto commit-proof interop fixtures (copied from indigo).
    #[test]
    fn commit_proof_fixtures() {
        let raw = include_str!("../testdata/commit-proof-fixtures.json");
        let fixtures: Vec<Fixture> = serde_json::from_str(raw).unwrap();
        for f in fixtures {
            let v = Cid::parse(&f.leaf_value).unwrap();
            let mut t = Tree::new();
            for k in &f.keys {
                t.insert_no_proof(k.as_bytes(), v).unwrap();
            }
            assert_eq!(
                t.root_cid().unwrap().to_string(),
                f.root_before,
                "{}",
                f.comment
            );
            for k in &f.adds {
                t.insert(k.as_bytes(), v).unwrap();
            }
            for k in &f.dels {
                assert_eq!(t.remove(k.as_bytes()).unwrap(), Some(v));
            }
            let mut blocks = Vec::new();
            let root = t.write_diff_blocks(&mut blocks).unwrap();
            assert_eq!(root.to_string(), f.root_after, "{}", f.comment);
            let map: HashMap<Cid, Vec<u8>> = blocks.into_iter().collect();
            for b in &f.blocks_in_proof {
                assert!(
                    map.contains_key(&Cid::parse(b).unwrap()),
                    "{}: missing proof block {b}",
                    f.comment
                );
            }
            // invert using only the diff blocks
            let mut inv = Tree::load_from_blocks(&map, root).unwrap();
            for k in &f.adds {
                assert_eq!(inv.remove(k.as_bytes()).unwrap(), Some(v), "{}", f.comment);
            }
            for k in &f.dels {
                assert_eq!(inv.insert(k.as_bytes(), v).unwrap(), None, "{}", f.comment);
            }
            assert_eq!(
                inv.root_cid().unwrap().to_string(),
                f.root_before,
                "{}",
                f.comment
            );
        }
    }

    fn rand_key(rng: &mut impl Rng) -> String {
        let colls = [
            "app.bsky.feed.post",
            "app.bsky.feed.like",
            "app.bsky.graph.follow",
        ];
        format!(
            "{}/{}",
            colls[rng.gen_range(0..3)],
            crate::tid::Tid::from_parts(rng.gen::<u64>() >> 11, rng.gen_range(0..1024))
        )
    }

    fn rand_cid(rng: &mut impl Rng) -> Cid {
        Cid::dag_cbor(&rng.gen::<[u8; 16]>())
    }

    /// The MST is a function of its contents: any history must yield the same
    /// root as building the final map from scratch, and every commit's diff
    /// blocks alone must be enough to invert it (sync 1.1).
    #[test]
    fn random_history_canonical_and_invertible() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        for round in 0..40 {
            let mut t = Tree::new();
            let mut model: BTreeMap<String, Cid> = BTreeMap::new();
            let n0 = rng.gen_range(0..300);
            for _ in 0..n0 {
                let k = rand_key(&mut rng);
                let v = rand_cid(&mut rng);
                t.insert_no_proof(k.as_bytes(), v).unwrap();
                model.insert(k, v);
            }
            let mut prev_root = t.root_cid().unwrap();
            for _ in 0..30 {
                // a multi-op commit
                let nops = rng.gen_range(1..8);
                let mut ops: Vec<(String, Option<Cid>, Option<Cid>)> = Vec::new(); // path, new, prev
                for _ in 0..nops {
                    let roll = rng.gen_range(0..10);
                    let existing: Vec<String> = model.keys().cloned().collect();
                    let (k, newv) = if roll < 5 || existing.is_empty() {
                        (rand_key(&mut rng), Some(rand_cid(&mut rng)))
                    } else if roll < 7 {
                        (
                            existing.choose(&mut rng).unwrap().clone(),
                            Some(rand_cid(&mut rng)),
                        )
                    } else {
                        (existing.choose(&mut rng).unwrap().clone(), None)
                    };
                    if ops.iter().any(|o| o.0 == k) {
                        continue;
                    }
                    let prev = match newv {
                        Some(v) => {
                            model.insert(k.clone(), v);
                            t.insert(k.as_bytes(), v).unwrap()
                        }
                        None => {
                            model.remove(&k);
                            t.remove(k.as_bytes()).unwrap()
                        }
                    };
                    ops.push((k, newv, prev));
                }
                let mut blocks = Vec::new();
                let root = t.write_diff_blocks(&mut blocks).unwrap();
                let map: HashMap<Cid, Vec<u8>> = blocks.into_iter().collect();
                let mut inv = Tree::load_from_blocks(&map, root).unwrap();
                // invert in a different order than applied: deletes first, then by path
                let mut sorted = ops.clone();
                sorted.sort_by(|a, b| (a.1.is_some(), &a.0).cmp(&(b.1.is_some(), &b.0)));
                for (k, newv, prev) in &sorted {
                    match (newv, prev) {
                        (Some(_), None) => {
                            inv.remove(k.as_bytes()).unwrap();
                        }
                        (_, Some(p)) => {
                            inv.insert(k.as_bytes(), *p).unwrap();
                        }
                        (None, None) => unreachable!(),
                    }
                }
                assert_eq!(
                    inv.root_cid().unwrap(),
                    prev_root,
                    "round {round}: inversion failed"
                );
                prev_root = root;
            }
            let mut fresh = Tree::new();
            let mut entries: Vec<_> = model.iter().collect();
            entries.shuffle(&mut rng);
            for (k, v) in entries {
                fresh.insert_no_proof(k.as_bytes(), *v).unwrap();
            }
            assert_eq!(
                fresh.root_cid().unwrap(),
                prev_root,
                "round {round}: not canonical"
            );
            let mut walked = Vec::new();
            t.walk(&mut |k, v| walked.push((String::from_utf8(k.to_vec()).unwrap(), v)));
            assert_eq!(walked, model.into_iter().collect::<Vec<_>>());
        }
    }

    fn rss_mb() -> f64 {
        let out = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().parse::<f64>().unwrap_or(0.0) / 1024.0
    }

    /// Build, commit-shaped insert/delete (snapshot clone + insert + diff
    /// blocks per op, as the repo worker does), getRepo walk and proofs on
    /// an n-key tree.
    fn tree_bench(entries: &[(String, Cid)], fresh: &[(String, Cid)], probes: &[&(String, Cid)]) -> Tree {
        use std::time::Instant;
        let rss0 = rss_mb();
        let t = Instant::now();
        let mut tree = Tree::new();
        for (k, v) in entries {
            tree.insert_no_proof(k.as_bytes(), *v).unwrap();
        }
        let build = t.elapsed();
        let t = Instant::now();
        tree.root_cid().unwrap();
        let rc = t.elapsed();
        let rss = rss_mb() - rss0;
        let mut snap = tree.clone();
        let t = Instant::now();
        for (k, v) in fresh {
            tree.insert(k.as_bytes(), *v).unwrap();
            let mut out = Vec::new();
            tree.write_diff_blocks(&mut out).unwrap();
            snap = tree.clone();
        }
        let ins = t.elapsed();
        let t = Instant::now();
        for (k, _) in fresh {
            tree.remove(k.as_bytes()).unwrap();
            let mut out = Vec::new();
            tree.write_diff_blocks(&mut out).unwrap();
            snap = tree.clone();
        }
        let del = t.elapsed();
        drop(snap);
        let t = Instant::now();
        let mut bytes = 0usize;
        tree.walk_blocks(&mut |_, b| bytes += b.len()).unwrap();
        let walk = t.elapsed();
        let t = Instant::now();
        let mut pb = 0;
        for (k, _) in probes {
            pb += tree.proof_blocks(k.as_bytes()).unwrap().len();
        }
        let proof = t.elapsed().as_secs_f64() * 1e6 / probes.len() as f64;
        let ops = fresh.len() as f64;
        println!(
            "build {:.2}s + root {:?} (rss +{:.0} MB) | commit ops: insert {:.0}/s delete {:.0}/s | getRepo walk {:.1} MB {:?} | proof {:.2} us ({pb})",
            build.as_secs_f64(), rc, rss, ops / ins.as_secs_f64(), ops / del.as_secs_f64(),
            bytes as f64 / 1e6, walk, proof
        );
        tree
    }

    /// Plus node-index build and getBlocks node lookups. Run alone (RSS):
    /// `cargo test --profile dev-release --lib mst::tests::bench_mst -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn bench_mst() {
        use std::time::Instant;
        let n: usize = std::env::var("MST_BENCH_N").ok().and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
        let mut rng = rand::rngs::StdRng::seed_from_u64(11);
        let entries: Vec<(String, Cid)> = (0..n).map(|_| (rand_key(&mut rng), rand_cid(&mut rng))).collect();
        let fresh: Vec<(String, Cid)> = (0..20_000).map(|_| (rand_key(&mut rng), rand_cid(&mut rng))).collect();
        let probes: Vec<&(String, Cid)> = (0..10_000).map(|_| &entries[rng.gen_range(0..n)]).collect();
        let tree = tree_bench(&entries, &fresh, &probes);
        let t = Instant::now();
        let ix = NodeIndex::build(&tree, 1).unwrap();
        println!("node index build: {:?} ({} nodes)", t.elapsed(), ix.map.len());
        let mut node_cids = Vec::new();
        tree.walk_blocks(&mut |c, _| node_cids.push(c)).unwrap();
        let t = Instant::now();
        for i in 0..10_000 {
            let c = node_cids[(i * 7919) % node_cids.len()];
            assert!(tree.find_node(&c, &ix).unwrap().is_some());
        }
        println!("getBlocks node lookup: {:.2} us/cid", t.elapsed().as_secs_f64() * 1e6 / 10_000.0);
    }

    /// Every node's cached block is its fresh encoding and hashes to its
    /// CID, through random inserts/removes with snapshots held (copy on
    /// write) and proof marking (cached blocks reused).
    fn assert_blocks_fresh(t: &Tree) {
        fn rec(n: &Node) {
            let mut fresh = Vec::new();
            encode_node(n, &mut fresh).unwrap();
            assert_eq!(n.block().unwrap().as_ref(), &fresh[..]);
            assert_eq!(Some(Cid::dag_cbor(&fresh)), n.cid);
            for e in &n.entries {
                if let Entry::Child { node: Some(c), cid } = e {
                    assert_eq!(*cid, c.cid);
                    rec(c);
                }
            }
        }
        rec(&t.root);
    }

    /// The node index, advanced commit by commit, finds every node of every
    /// version it covers (and nothing else), in that version's tree.
    #[test]
    fn node_index_tracks_commits() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(21);
        let mut t = Tree::new();
        let mut model: Vec<String> = Vec::new();
        for _ in 0..500 {
            let k = rand_key(&mut rng);
            t.insert_no_proof(k.as_bytes(), rand_cid(&mut rng)).unwrap();
            model.push(k);
        }
        t.root_cid().unwrap();
        let mut ix = NodeIndex::build(&t, 1).unwrap();
        let mut versions: Vec<(u64, Tree)> = vec![(1, t.clone())];
        for rev in 2..120u64 {
            for _ in 0..rng.gen_range(1..6) {
                if rng.gen_bool(0.6) || model.is_empty() {
                    let k = rand_key(&mut rng);
                    t.insert(k.as_bytes(), rand_cid(&mut rng)).unwrap();
                    model.push(k);
                } else if rng.gen_bool(0.5) {
                    let k = model.swap_remove(rng.gen_range(0..model.len()));
                    t.remove(k.as_bytes()).unwrap();
                } else {
                    let k = &model[rng.gen_range(0..model.len())];
                    t.insert(k.as_bytes(), rand_cid(&mut rng)).unwrap();
                }
            }
            let (mut blocks, mut refs) = (Vec::new(), Vec::new());
            let root = t.write_diff_blocks_with_refs(&mut blocks, &mut refs).unwrap();
            assert_eq!(blocks.len(), refs.len() + t.root.first_key().is_none() as usize);
            for (c, b) in &blocks {
                assert_eq!(Cid::dag_cbor(b), *c);
            }
            assert!(ix.advance(rev - 1, rev, &refs));
            assert!(!ix.advance(rev - 1, rev, &refs), "advanced twice");
            assert_eq!(t.root.cid, Some(root));
            assert_blocks_fresh(&t);
            versions.push((rev, t.clone()));
        }
        for (rev, v) in versions.iter().step_by(7) {
            assert!(ix.covers(*rev));
            let mut nodes = Vec::new();
            v.walk_blocks(&mut |c, b| nodes.push((c, b.to_vec()))).unwrap();
            for (c, b) in &nodes {
                assert_eq!(v.find_node(c, &ix).unwrap().as_ref(), Some(b), "rev {rev}");
            }
            // another version's nodes and record CIDs are not this tree's
            let other = &versions[0].1;
            let mut theirs = Vec::new();
            other.walk_blocks(&mut |c, _| theirs.push(c)).unwrap();
            for c in theirs.iter().filter(|c| !nodes.iter().any(|(n, _)| n == *c)) {
                assert_eq!(v.find_node(c, &ix).unwrap(), None);
            }
            assert_eq!(v.find_node(&rand_cid(&mut rng), &ix).unwrap(), None);
        }
        // a cell catches a late build up through its recent commits
        let mut cell = NodeIndexCell { wanted: true, ..Default::default() };
        let base = versions[100].1.clone();
        let mut t2 = base.clone();
        for rev in 101..105u64 {
            t2.insert(rand_key(&mut rng).as_bytes(), rand_cid(&mut rng)).unwrap();
            let (mut blocks, mut refs) = (Vec::new(), Vec::new());
            t2.write_diff_blocks_with_refs(&mut blocks, &mut refs).unwrap();
            cell.commit(rev - 1, rev, refs);
        }
        cell.install(NodeIndex::build(&base, 100).unwrap());
        let ix = cell.index.as_ref().unwrap();
        assert!(ix.covers(100) && ix.covers(104) && !ix.covers(105));
        t2.walk_blocks(&mut |c, b| assert_eq!(t2.find_node(&c, ix).unwrap().as_deref(), Some(b))).unwrap();
        // a commit that doesn't chain drops it
        cell.commit(200, 201, Vec::new());
        assert!(cell.index.is_none());
    }

    #[test]
    fn delete_everything_then_reinsert() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(3);
        let mut t = Tree::new();
        let keys: Vec<String> = (0..200).map(|_| rand_key(&mut rng)).collect();
        for k in &keys {
            t.insert(k.as_bytes(), leaf()).unwrap();
        }
        t.root_cid().unwrap();
        for k in &keys {
            t.remove(k.as_bytes()).unwrap();
        }
        assert_eq!(t.root_cid().unwrap(), Tree::new().root_cid().unwrap());
        t.insert(b"app.bsky.feed.post/aaaa", leaf()).unwrap();
        let mut fresh = Tree::new();
        fresh.insert(b"app.bsky.feed.post/aaaa", leaf()).unwrap();
        assert_eq!(t.root_cid().unwrap(), fresh.root_cid().unwrap());
    }

    /// Raw node bytes: `l`, then entries of (key suffix, prefix len, t).
    fn raw_node(l: Option<Cid>, es: &[(&[u8], u64, Option<Cid>)]) -> Vec<u8> {
        let mut b = Vec::new();
        cbor::write_map_head(&mut b, 2);
        cbor::write_text(&mut b, "e");
        cbor::write_array_head(&mut b, es.len());
        for (k, p, t) in es {
            cbor::write_map_head(&mut b, 4);
            cbor::write_text(&mut b, "k");
            cbor::write_bytes(&mut b, k);
            cbor::write_text(&mut b, "p");
            cbor::write_uint(&mut b, *p);
            cbor::write_text(&mut b, "t");
            cbor::write_opt_cid(&mut b, t.as_ref());
            cbor::write_text(&mut b, "v");
            cbor::write_cid(&mut b, &leaf());
        }
        cbor::write_text(&mut b, "l");
        cbor::write_opt_cid(&mut b, l.as_ref());
        b
    }

    fn add(blocks: &mut HashMap<Cid, Vec<u8>>, b: Vec<u8>) -> Cid {
        let c = Cid::dag_cbor(&b);
        blocks.insert(c, b);
        c
    }

    fn load(blocks: &HashMap<Cid, Vec<u8>>, root: Cid) -> Result<Tree> {
        Tree::load_from_blocks(blocks, root)
    }

    /// A chain of `{e: [], l: child}` nodes used to overflow the stack (and
    /// abort the process) in load_from_blocks; it is an error now, on the
    /// 2 MiB stack of a tokio blocking thread.
    #[test]
    fn deep_chain_rejected_not_overflowed() {
        let mut blocks = HashMap::new();
        let mut c = add(&mut blocks, raw_node(None, &[(b"asdf", 0, None)]));
        let mut depth_ok = None;
        for d in 1..200_000 {
            c = add(&mut blocks, raw_node(Some(c), &[]));
            if d == MAX_DEPTH - 1 {
                depth_ok = Some(c);
            }
        }
        let r = std::thread::Builder::new()
            .stack_size(2 << 20)
            .spawn(move || {
                let ok = load(&blocks, depth_ok.unwrap()).map(|t| t.root.height);
                (ok, load(&blocks, c).err())
            })
            .unwrap()
            .join()
            .unwrap();
        // MAX_DEPTH levels load (heights inferred upward from the leaf)
        assert_eq!(r.0, Ok(MAX_DEPTH as i32 - 1));
        assert_eq!(r.1, Some(MstError::Invalid("tree too deep")));
    }

    #[test]
    fn heights_must_step_down_by_one() {
        // "blue" has height 1, "88bfafc7" height 2, "asdf"/"2653ae71" height 0
        let mut blocks = HashMap::new();
        let h0 = add(&mut blocks, raw_node(None, &[(b"asdf", 0, None)]));
        let h1 = add(&mut blocks, raw_node(None, &[(b"blue", 0, None)]));
        let good = add(&mut blocks, raw_node(Some(h0), &[(b"blue", 0, None)]));
        assert_eq!(
            load(&blocks, good).unwrap().get(b"asdf").unwrap(),
            Some(leaf())
        );
        // height 2 over height 0, height 1 over height 1
        let skip = add(&mut blocks, raw_node(Some(h0), &[(b"88bfafc7", 0, None)]));
        let same = add(&mut blocks, raw_node(None, &[(b"blue", 0, Some(h1))]));
        // a key-less node over height 0 is height 1; over that, height 2 is fine
        let mid = add(&mut blocks, raw_node(Some(h0), &[]));
        let ok2 = add(&mut blocks, raw_node(Some(mid), &[(b"88bfafc7", 0, None)]));
        let bad2 = add(&mut blocks, raw_node(Some(mid), &[(b"blue", 0, None)]));
        for c in [skip, same, bad2] {
            assert!(load(&blocks, c).is_err());
        }
        assert!(load(&blocks, ok2).is_ok());
        // key-less nodes pushed down to height 0 cannot have children: the
        // stub below `mid0` is missing, so its height comes from above
        let stub = add(&mut blocks, raw_node(Some(leaf()), &[]));
        let mid0 = add(&mut blocks, raw_node(Some(stub), &[]));
        let top = add(&mut blocks, raw_node(Some(mid0), &[(b"blue", 0, None)]));
        assert_eq!(
            load(&blocks, top).err(),
            Some(MstError::Invalid("child of a height-0 node"))
        );
        // empty non-root nodes don't exist in a canonical tree
        let empty = add(&mut blocks, raw_node(None, &[]));
        let over_empty = add(&mut blocks, raw_node(Some(empty), &[(b"blue", 0, None)]));
        assert!(load(&blocks, over_empty).is_err());
        assert!(load(&blocks, empty).unwrap().is_empty());
    }

    #[test]
    fn decode_node_structural_checks() {
        let c = leaf();
        let ok = |b: Vec<u8>| decode_node(&b, Cid::dag_cbor(&b));
        // "asdf" < "asdg", both height 0, sharing a 3-byte prefix
        assert!(ok(raw_node(None, &[(b"asdf", 0, None), (b"g", 3, None)])).is_ok());
        let bad: Vec<(&str, Vec<u8>)> = vec![
            (
                "unsorted",
                raw_node(None, &[(b"asdf", 0, None), (b"2653ae71", 0, None)]),
            ),
            (
                "duplicate",
                raw_node(None, &[(b"asdf", 0, None), (b"", 4, None)]),
            ),
            (
                "mixed heights",
                raw_node(None, &[(b"asdf", 0, None), (b"blue", 0, None)]),
            ),
            ("first p != 0", raw_node(None, &[(b"asdf", 1, None)])),
            (
                "prefix not maximal",
                raw_node(None, &[(b"asdf", 0, None), (b"asdg", 0, None)]),
            ),
            ("empty key", raw_node(None, &[(b"", 0, None)])),
            ("height-0 child", raw_node(Some(c), &[(b"asdf", 0, None)])),
            (
                "height-0 right child",
                raw_node(None, &[(b"asdf", 0, Some(c))]),
            ),
        ];
        for (what, b) in bad {
            assert!(ok(b).is_err(), "{what} accepted");
        }
        use cbor::Value as V;
        let entry = |extra: Option<(&str, V)>, t: Option<V>| {
            let mut m = vec![
                ("k".to_string(), V::Bytes(b"blue".to_vec())),
                ("p".to_string(), V::Int(0)),
                ("v".to_string(), V::Link(c)),
            ];
            if let Some(t) = t {
                m.push(("t".to_string(), t));
            }
            if let Some((k, v)) = extra {
                m.push((k.to_string(), v));
            }
            m.sort_by(|a, b| cbor::key_cmp(&a.0, &b.0));
            V::Map(m)
        };
        let node = |e: V, l: Option<V>, extra: Option<(&str, V)>| {
            let mut m = vec![("e".to_string(), V::Array(vec![e]))];
            if let Some(l) = l {
                m.push(("l".to_string(), l));
            }
            if let Some((k, v)) = extra {
                m.push((k.to_string(), v));
            }
            m.sort_by(|a, b| cbor::key_cmp(&a.0, &b.0));
            V::Map(m).to_cbor()
        };
        assert!(ok(node(entry(None, Some(V::Null)), Some(V::Null), None)).is_ok());
        assert!(ok(node(entry(None, Some(V::Link(c))), Some(V::Link(c)), None)).is_ok());
        let bad = [
            ("missing l", node(entry(None, Some(V::Null)), None, None)),
            ("missing t", node(entry(None, None), Some(V::Null), None)),
            (
                "unknown node field",
                node(
                    entry(None, Some(V::Null)),
                    Some(V::Null),
                    Some(("x", V::Null)),
                ),
            ),
            (
                "unknown entry field",
                node(
                    entry(Some(("x", V::Null)), Some(V::Null)),
                    Some(V::Null),
                    None,
                ),
            ),
            (
                "t not a link",
                node(
                    entry(None, Some(V::Bytes(c.to_bytes().to_vec()))),
                    Some(V::Null),
                    None,
                ),
            ),
            (
                "t int",
                node(entry(None, Some(V::Int(0))), Some(V::Null), None),
            ),
            (
                "l not a link",
                node(entry(None, Some(V::Null)), Some(V::Text("x".into())), None),
            ),
        ];
        for (what, b) in bad {
            assert!(ok(b).is_err(), "{what} accepted");
        }
    }
}
