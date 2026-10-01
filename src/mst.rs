//! Merkle Search Tree, ported from indigo's `atproto/repo/mst` so that tree
//! shapes and sync 1.1 proof block sets match the reference implementation.
//!
//! Nodes are `Arc`-shared and mutated copy-on-write (`Arc::make_mut`), so a
//! snapshot of the root (for exports, or inverting a commit) costs one refcount.
//!
//! `dirty` means "this node's CID must be (re)computed and its block emitted in
//! the next diff". Mutations mark the nodes they rewrite; `prove_mutation` also
//! marks neighbouring nodes that a verifier needs to invert the operation.

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

#[derive(Clone, Debug)]
pub struct Node {
    pub height: i32,
    pub entries: Vec<Entry>,
    pub cid: Option<Cid>,
    pub dirty: bool,
    /// Placeholder for a node known only by CID (partial trees).
    pub stub: bool,
}

#[derive(Clone, Debug)]
pub enum Entry {
    Value {
        key: Box<[u8]>,
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
        nm.dirty = true;
        return Ok((n, Some(prev)));
    }

    let (idx, split) = n.find_insertion_index(key)?;
    let nm = Arc::make_mut(&mut n);
    nm.dirty = true;
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
        nm.dirty = true;
        return Ok((n, prev));
    }
    let (idx, split) = n.find_insertion_index(key)?;
    if split {
        return Err(MstError::Invalid("unexpected split when inserting child"));
    }
    let nm = Arc::make_mut(&mut n);
    nm.dirty = true;
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
    nm.dirty = true;
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
    let Some(idx) = n.find_existing_child(key) else {
        return Ok((n, None));
    };
    match &n.entries[idx] {
        Entry::Child { node: Some(c), .. } => {
            // cheap pre-check so a no-op delete doesn't copy-on-write the path
            if c.get(key, height)?.is_none() {
                return Ok((n, None));
            }
        }
        _ => return Err(MstError::Partial),
    }
    let nm = Arc::make_mut(&mut n);
    let Entry::Child { node, .. } = &mut nm.entries[idx] else {
        unreachable!()
    };
    let child = node.take().ok_or(MstError::Partial)?;
    let (new_child, prev) = remove(child, key, Some(height), prove)?;
    nm.dirty = true;
    if !new_child.is_empty() {
        *node = Some(new_child);
    } else {
        nm.entries.remove(idx);
    }
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

/// Recomputes CIDs of dirty nodes, emitting their blocks into `out`.
fn write_blocks(n: &mut Arc<Node>, out: &mut Option<&mut Vec<(Cid, Vec<u8>)>>) -> Result<Cid> {
    if n.stub {
        return Err(MstError::Invalid("nil tree node"));
    }
    if !n.dirty {
        if let Some(c) = n.cid {
            return Ok(c);
        }
    }
    let nm = Arc::make_mut(n);
    for e in nm.entries.iter_mut() {
        if let Entry::Child { node: Some(c), cid } = e {
            if c.dirty || c.cid.is_none() {
                *cid = Some(write_blocks(c, out)?);
            } else {
                *cid = c.cid;
            }
        }
    }
    let mut buf = Vec::with_capacity(64 + nm.entries.len() * 80);
    encode_node(nm, &mut buf)?;
    let c = Cid::dag_cbor(&buf);
    nm.cid = Some(c);
    nm.dirty = false;
    if let Some(out) = out.as_mut() {
        out.push((c, buf));
    }
    Ok(c)
}

// ---------- decoding (partial trees from a block set) ----------

pub fn decode_node(data: &[u8], c: Cid) -> std::result::Result<Node, MstError> {
    use cbor::Value;
    let v = Value::decode(data).map_err(|_| MstError::Invalid("bad node cbor"))?;
    let mut entries = Vec::new();
    match v.get("l") {
        Some(Value::Link(l)) => entries.push(Entry::Child {
            node: None,
            cid: Some(*l),
        }),
        Some(Value::Null) | None => {}
        _ => return Err(MstError::Invalid("bad l")),
    }
    let Some(Value::Array(es)) = v.get("e") else {
        return Err(MstError::Invalid("bad e"));
    };
    let mut prev: Vec<u8> = Vec::new();
    let mut height = -1;
    for e in es {
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
        let mut key = prev[..p].to_vec();
        key.extend_from_slice(k);
        if height < 0 {
            height = height_for_key(&key);
        }
        entries.push(Entry::Value {
            key: key.clone().into(),
            val: *val,
        });
        prev = key;
        if let Some(Value::Link(t)) = e.get("t") {
            entries.push(Entry::Child {
                node: None,
                cid: Some(*t),
            });
        }
    }
    Ok(Node {
        height,
        entries,
        cid: Some(c),
        dirty: false,
        stub: false,
    })
}

fn load_from_blocks(blocks: &HashMap<Cid, Vec<u8>>, c: Cid) -> Result<Option<Arc<Node>>> {
    let Some(data) = blocks.get(&c) else {
        return Ok(None);
    };
    let mut n = decode_node(data, c)?;
    for e in n.entries.iter_mut() {
        if let Entry::Child {
            node,
            cid: Some(cc),
        } = e
        {
            if let Some(child) = load_from_blocks(blocks, *cc)? {
                if n.height == -1 && child.height >= 0 {
                    n.height = child.height + 1;
                }
                *node = Some(child);
            }
        }
    }
    Ok(Some(Arc::new(n)))
}

fn ensure_heights(n: &mut Arc<Node>) {
    if n.height <= 0 {
        return;
    }
    let h = n.height;
    let nm = Arc::make_mut(n);
    for e in nm.entries.iter_mut() {
        if let Entry::Child { node: Some(c), .. } = e {
            if c.height < 0 {
                Arc::make_mut(c).height = h - 1;
            }
            ensure_heights(c);
        }
    }
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
        write_blocks(&mut self.root, &mut None)
    }

    /// Computes the root CID and returns every dirty block (new nodes + proof nodes).
    pub fn write_diff_blocks(&mut self, out: &mut Vec<(Cid, Vec<u8>)>) -> Result<Cid> {
        write_blocks(&mut self.root, &mut Some(out))
    }

    /// Loads a (possibly partial) tree from a block set.
    pub fn load_from_blocks(blocks: &HashMap<Cid, Vec<u8>>, root: Cid) -> Result<Tree> {
        let mut r = load_from_blocks(blocks, root)?.ok_or(MstError::Partial)?;
        ensure_heights(&mut r);
        Ok(Tree { root: r })
    }

    /// Visits every (key, value) in key order.
    pub fn walk(&self, f: &mut dyn FnMut(&[u8], Cid)) {
        fn rec(n: &Node, f: &mut dyn FnMut(&[u8], Cid)) {
            for e in &n.entries {
                match e {
                    Entry::Value { key, val } => f(key, *val),
                    Entry::Child { node: Some(c), .. } => rec(c, f),
                    _ => {}
                }
            }
        }
        rec(&self.root, f)
    }

    /// Visits every node block (cid, encoded bytes). The tree must be fully
    /// written (no dirty nodes), e.g. right after `root_cid`.
    pub fn walk_blocks(&self, f: &mut dyn FnMut(Cid, &[u8])) -> Result<()> {
        fn rec(n: &Node, buf: &mut Vec<u8>, f: &mut dyn FnMut(Cid, &[u8])) -> Result<()> {
            buf.clear();
            encode_node(n, buf)?;
            let c = n.cid.ok_or(MstError::Invalid("unwritten node"))?;
            f(c, buf);
            for e in &n.entries {
                if let Entry::Child { node: Some(c), .. } = e {
                    rec(c, buf, f)?;
                }
            }
            Ok(())
        }
        let mut buf = Vec::with_capacity(1024);
        rec(&self.root, &mut buf, f)
    }

    /// Node blocks on the path from the root to `key` (inclusion or exclusion proof).
    pub fn proof_blocks(&self, key: &[u8]) -> Result<Vec<(Cid, Vec<u8>)>> {
        let height = height_for_key(key);
        let mut out = Vec::new();
        let mut n: &Node = &self.root;
        loop {
            let mut buf = Vec::new();
            encode_node(n, &mut buf)?;
            out.push((n.cid.ok_or(MstError::Invalid("unwritten node"))?, buf));
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
}
