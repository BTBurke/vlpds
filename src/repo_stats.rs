//! The counts checkAccountStatus reports (`state::RepoStats`), from scratch:
//! what the repo worker's incremental `S/{did}` must equal. O(repo): tests,
//! `vlpds admin check-repo`, and a repo whose `S/` row is missing.

use crate::mst::{Entry, MstError, Node, Tree};
use crate::state::{self, RepoStats};
use std::collections::HashSet;

/// (records, nodes with entries) of a fully loaded tree.
pub fn count_tree(tree: &Tree) -> Result<(u64, u64), MstError> {
    fn rec(n: &Node, out: &mut (u64, u64)) -> Result<(), MstError> {
        if n.stub {
            return Err(MstError::Partial);
        }
        if !n.entries.is_empty() {
            out.1 += 1;
        }
        for e in &n.entries {
            match e {
                Entry::Value { .. } => out.0 += 1,
                Entry::Child { node: Some(c), .. } => rec(c, out)?,
                Entry::Child { node: None, .. } => return Err(MstError::Partial),
            }
        }
        Ok(())
    }
    let mut out = (0, 0);
    rec(&tree.root, &mut out)?;
    Ok(out)
}

/// Records and the tree rebuilt from them (`R/`), distinct blob CIDs (`b/`).
pub async fn walk<R: slatedb::DbReadOps + Sync + ?Sized>(db: &R, did: &str) -> anyhow::Result<RepoStats> {
    let prefix = state::record_prefix(did);
    let mut it = state::BatchedScan::new(db.scan(prefix.clone()..state::prefix_end(&prefix)).await?);
    let mut recs = Vec::new();
    while let Some(kv) = it.next().await? {
        let (cid, _) = state::record_value_parts(&kv.value)?;
        recs.push((crate::mst_lazy::Key::from(&kv.key[prefix.len()..]), cid));
    }
    let tree = tokio::task::spawn_blocking(move || crate::mst_lazy::build_tree(&recs)).await??;
    let (records, nodes) = count_tree(&tree)?;
    let bprefix = state::blob_ref_prefix(did);
    let mut it = db.scan(bprefix.clone()..state::prefix_end(&bprefix)).await?;
    let mut blobs = HashSet::new();
    while let Some(kv) = it.next().await? {
        let rest = &kv.key[bprefix.len()..];
        let cid = rest.split(|b| *b == 0).next().unwrap_or_default();
        blobs.insert(cid.to_vec());
    }
    Ok(RepoStats { records, nodes, blobs: blobs.len() as u64 })
}
