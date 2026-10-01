//! Firehose cursor backfill from S3 segments.
//!
//! A subscriber whose cursor is older than the in-memory ring is served by
//! reading the node logs straight from S3: for each log, find the first
//! segment past the cursor (exponential probe + binary search on segment
//! headers), stream its segments, and k-way merge every log by seq. The
//! merged order equals the live merger's order (both are "by seq"), so the
//! subscriber sees one continuous stream when it hands off to the ring.

use crate::nodelog::segment_path;
use crate::segment::{self, LogObject};
use crate::store::Store;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStoreExt};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use tokio::sync::mpsc;

/// Header of a segment (or None if missing / a fence), via a small range GET.
async fn seg_header(store: &Store, log_id: &str, ordinal: u64) -> anyhow::Result<Option<(i64, i64)>> {
    let opts = GetOptions { range: Some(GetRange::Bounded(0..4096)), ..Default::default() };
    let data = match store.raw.get_opts(&segment_path(store, log_id, ordinal), opts).await {
        Ok(r) => r.bytes().await?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if data.starts_with(segment::FENCE_MAGIC) {
        return Ok(None);
    }
    anyhow::ensure!(data.len() >= 10 && data.starts_with(segment::MAGIC), "bad segment header");
    let idlen = u16::from_be_bytes(data[8..10].try_into()?) as usize;
    let p = 10 + idlen + 8; // skip log id and ordinal
    anyhow::ensure!(data.len() >= p + 16, "short segment header");
    let first = i64::from_be_bytes(data[p..p + 8].try_into()?);
    let last = i64::from_be_bytes(data[p + 8..p + 16].try_into()?);
    Ok(Some((first, last)))
}

/// Every log id under `{prefix}/log/`.
pub async fn list_logs(store: &Store) -> anyhow::Result<Vec<String>> {
    let prefix = Path::from(format!("{}/log", store.prefix));
    let r = store.raw.list_with_delimiter(Some(&prefix)).await?;
    Ok(r.common_prefixes.iter().filter_map(|p| p.filename().map(String::from)).collect())
}

/// First ordinal of `log_id` whose segment has events with seq > `after`
/// (None if the log has nothing past it).
async fn first_ordinal_after(store: &Store, log_id: &str, after: i64) -> anyhow::Result<Option<u64>> {
    let o = seek(store, log_id, after).await?;
    Ok(seg_header(store, log_id, o).await?.map(|_| o))
}

/// First ordinal of `log_id` that is missing (not written yet, or the fence)
/// or whose segment has events with seq > `after`. Segments appear in ordinal
/// order and their seqs increase, so everything before it is <= `after`.
pub async fn seek(store: &Store, log_id: &str, after: i64) -> anyhow::Result<u64> {
    // exponential probe for an upper bound (first missing ordinal or a segment past `after`)
    match seg_header(store, log_id, 0).await? {
        Some((_, last0)) if last0 <= after => {}
        _ => return Ok(0),
    }
    let (mut lo, mut hi) = (0u64, 1u64); // invariant: seg(lo).last <= after
    loop {
        match seg_header(store, log_id, hi).await? {
            Some((_, last)) if last <= after => {
                lo = hi;
                hi *= 2;
            }
            _ => break,
        }
    }
    // binary search in (lo, hi]: first ordinal that is missing or has last > after
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        match seg_header(store, log_id, mid).await? {
            Some((_, last)) if last <= after => lo = mid,
            _ => hi = mid,
        }
    }
    Ok(hi)
}

struct LogCursor {
    log_id: String,
    next: u64,
    buf: VecDeque<(i64, Bytes)>,
    done: bool,
}

impl LogCursor {
    /// Ensures `buf` has at least one event unless the log is exhausted.
    async fn fill(&mut self, store: &Store, after: i64) -> anyhow::Result<()> {
        while self.buf.is_empty() && !self.done {
            let data = match store.raw.get(&segment_path(store, &self.log_id, self.next)).await {
                Ok(r) => r.bytes().await?,
                Err(object_store::Error::NotFound { .. }) => {
                    self.done = true;
                    break;
                }
                Err(e) => return Err(e.into()),
            };
            self.next += 1;
            match segment::parse(data, false, None)? {
                LogObject::Fence { .. } => self.done = true,
                LogObject::Segment(_, entries) => {
                    self.buf.extend(entries.into_iter().filter(|e| !e.frame.is_empty() && e.seq > after).map(|e| (e.seq, e.frame)));
                }
            }
        }
        Ok(())
    }
}

/// Sends every event with `after < seq <= until`, in seq order, across all
/// logs. Returns the last seq sent (or `after`).
pub async fn backfill(store: &Store, after: i64, until: i64, tx: &mpsc::Sender<(i64, Bytes)>) -> anyhow::Result<i64> {
    let mut cursors = Vec::new();
    for log_id in list_logs(store).await? {
        if let Some(ord) = first_ordinal_after(store, &log_id, after).await? {
            cursors.push(LogCursor { log_id, next: ord, buf: VecDeque::new(), done: false });
        }
    }
    let mut heap = BinaryHeap::new();
    for (i, c) in cursors.iter_mut().enumerate() {
        c.fill(store, after).await?;
        if let Some((seq, _)) = c.buf.front() {
            heap.push(Reverse((*seq, i)));
        }
    }
    let mut last = after;
    while let Some(Reverse((seq, i))) = heap.pop() {
        if seq > until {
            break;
        }
        let c = &mut cursors[i];
        let (s, frame) = c.buf.pop_front().expect("heap entry has an event");
        if tx.send((s, frame)).await.is_err() {
            return Ok(last); // subscriber went away
        }
        last = s;
        c.fill(store, after).await?;
        if let Some((next, _)) = c.buf.front() {
            heap.push(Reverse((*next, i)));
        }
    }
    Ok(last)
}
