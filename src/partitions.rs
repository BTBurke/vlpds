//! The set of partitions this node currently owns. Static (all of them) in
//! single-node mode; changes as leases are acquired/released in cluster mode.

use crate::partition::Partition;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct PartitionTable {
    slots: RwLock<Vec<Option<Arc<Partition>>>>,
}

impl PartitionTable {
    pub fn new(n: u16) -> Arc<PartitionTable> {
        Arc::new(PartitionTable { slots: RwLock::new(vec![None; n as usize]) })
    }

    /// Total partitions in the keyspace (owned or not).
    pub fn len(&self) -> usize {
        self.slots.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, p: usize) -> Option<Arc<Partition>> {
        self.slots.read().get(p).cloned().flatten()
    }

    pub fn set(&self, p: u16, part: Option<Arc<Partition>>) {
        self.slots.write()[p as usize] = part;
    }

    /// Partitions owned by this node.
    pub fn owned(&self) -> Vec<Arc<Partition>> {
        self.slots.read().iter().flatten().cloned().collect()
    }

    /// Snapshot of all slots (None = owned elsewhere).
    pub fn slots(&self) -> Vec<Option<Arc<Partition>>> {
        self.slots.read().clone()
    }
}
