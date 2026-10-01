//! The shards this node currently has open, and the layout (slot ranges ->
//! shard ids) it routes by. Every shard in single-node mode; changes as
//! shards are acquired/released and as the layout changes (split/merge).

use crate::partition::Partition;
use crate::slots::Layout;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct PartitionTable {
    layout: RwLock<Arc<Layout>>,
    open: RwLock<BTreeMap<u16, Arc<Partition>>>,
}

impl PartitionTable {
    /// Starts with the uniform layout of `n` shards (replaced by the
    /// cluster's layout as soon as it is read).
    pub fn new(n: u16) -> Arc<PartitionTable> {
        Arc::new(PartitionTable { layout: RwLock::new(Arc::new(Layout::uniform(n))), open: RwLock::default() })
    }

    pub fn layout(&self) -> Arc<Layout> {
        self.layout.read().clone()
    }

    /// Installs `l` whatever its version (the cluster's layout at startup).
    pub fn replace_layout(&self, l: Arc<Layout>) {
        *self.layout.write() = l;
    }

    /// Installs a newer layout (older versions are ignored).
    pub fn set_layout(&self, l: Arc<Layout>) {
        let mut cur = self.layout.write();
        if l.version > cur.version || (l.version == cur.version && l.op != cur.op) {
            *cur = l;
        }
    }

    /// Shard owning a routing key (DID or private routing key).
    pub fn shard_of(&self, key: &str) -> u16 {
        self.layout.read().shard_of(key)
    }

    /// The open shard owning `key`, if this node serves it.
    pub fn for_key(&self, key: &str) -> Option<Arc<Partition>> {
        self.get(self.shard_of(key))
    }

    /// Shards in the layout (owned or not).
    pub fn len(&self) -> usize {
        self.layout.read().shards.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The open shard with this id (any integer type; ids are u16).
    pub fn get<I: TryInto<u16>>(&self, id: I) -> Option<Arc<Partition>> {
        let id = id.try_into().ok()?;
        self.open.read().get(&id).cloned()
    }

    pub fn set(&self, id: u16, part: Option<Arc<Partition>>) {
        let mut open = self.open.write();
        match part {
            Some(p) => open.insert(id, p),
            None => open.remove(&id),
        };
    }

    /// Shards open on this node, by id.
    pub fn owned(&self) -> Vec<Arc<Partition>> {
        self.open.read().values().cloned().collect()
    }
}
