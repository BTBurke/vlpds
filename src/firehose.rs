//! Firehose: merges durable node-log streams into one seq-ordered stream.
//!
//! Each log (this node's, each live peer's, and any dead node's log still
//! being drained from S3) delivers durable batches and a watermark W_l (every
//! event with seq <= W_l has been delivered). The merger emits events with
//! seq <= min_l W_l in seq order, so the merged order is total, stable and the
//! same on every node.
//!
//! The merged stream starts at a floor F (the clock at startup): every log
//! delivers its events with seq > F (followers catch up from S3, see
//! remote.rs), the merger drops anything <= F, and cursors at or below F are
//! backfilled from S3 (backfill.rs). A log followed later starts at the
//! merger's position at that moment, so nothing above it is skipped either.

use crate::events;
use crate::metrics;
use crate::nodelog::{LogBatch, Watermark};
use crate::segment::{self, LogObject};
use crate::stats::STATS;
use axum::extract::ws::{Message, WebSocket};
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

/// Where a log's watermark comes from: our own log, or a peer stream / S3
/// drain (the last watermark it reported).
#[derive(Clone)]
pub enum Source {
    Local(Arc<Watermark>),
    Remote(Arc<AtomicI64>),
}

impl Source {
    fn get(&self) -> i64 {
        match self {
            Source::Local(w) => w.get(),
            Source::Remote(a) => a.load(Ordering::Acquire),
        }
    }
}

pub struct MergedBatch {
    pub first: i64,
    pub last: i64,
    pub events: Vec<(i64, Bytes)>,
    pub bytes: usize,
}

pub struct Firehose {
    pub tx: broadcast::Sender<Arc<MergedBatch>>,
    ring: RwLock<VecDeque<Arc<MergedBatch>>>,
    ring_bytes: AtomicI64,
    max_ring_bytes: i64,
    pub last_emitted: AtomicI64,
    /// Watermark source per log id.
    pub sources: RwLock<HashMap<Arc<str>, Source>>,
    /// The ring holds every event with seq > ring_floor (the start floor
    /// until the ring evicts). Older cursors are backfilled from S3.
    ring_floor: AtomicI64,
    /// The merged stream's start floor F: events <= F are only served by the
    /// S3 backfill.
    start_floor: i64,
    /// Highest min watermark the merger has acted on: every event <= it (and
    /// > start_floor) of every followed log has been emitted.
    settled: AtomicI64,
    /// Object store for S3 backfill (set once the node log is known).
    pub store: RwLock<Option<crate::store::Store>>,
    max_queue_bytes: AtomicUsize,
    queued_bytes: AtomicUsize,
}

impl Firehose {
    pub fn new(max_ring_bytes: usize) -> Arc<Firehose> {
        let (tx, _) = broadcast::channel(4096);
        let floor = crate::nodelog::seq_floor(crate::tid::now_micros());
        Arc::new(Firehose {
            tx,
            ring: RwLock::new(VecDeque::new()),
            ring_bytes: AtomicI64::new(0),
            max_ring_bytes: max_ring_bytes as i64,
            last_emitted: AtomicI64::new(0),
            sources: RwLock::new(HashMap::new()),
            ring_floor: AtomicI64::new(floor),
            start_floor: floor,
            settled: AtomicI64::new(i64::MIN),
            store: RwLock::new(None),
            max_queue_bytes: AtomicUsize::new(DEFAULT_MERGE_QUEUE_BYTES),
            queued_bytes: AtomicUsize::new(0),
        })
    }

    pub fn min_watermark(&self) -> Option<i64> {
        self.sources.read().values().map(|s| s.get()).min()
    }

    /// Everything at or below this has been emitted (or is below the start
    /// floor): a log followed from now on only owes us its events above it.
    pub fn position(&self) -> i64 {
        self.start_floor.max(self.settled.load(Ordering::Acquire))
    }

    /// Registers a newly followed peer log: returns the floor its follower
    /// must deliver every event above, and its watermark (starting there).
    /// Taken under the sources lock, so no merger tick that ignored this log
    /// can settle past the floor afterwards.
    pub fn add_remote(&self, log_id: &str) -> (i64, Arc<AtomicI64>) {
        let mut s = self.sources.write();
        let floor = self.position();
        let wm = Arc::new(AtomicI64::new(floor));
        s.insert(log_id.into(), Source::Remote(wm.clone()));
        (floor, wm)
    }

    /// Adds (Some) or removes (None) a log's watermark source. Remove a log
    /// only after every one of its events has been handed to the merger.
    pub fn set_source(&self, log_id: &str, source: Option<Source>) {
        let mut s = self.sources.write();
        match source {
            Some(src) => {
                s.insert(log_id.into(), src);
            }
            None => {
                s.remove(log_id);
            }
        }
    }

    /// Byte budget of the merger's queues (events waiting for the min
    /// watermark). Over it, logs are spilled: see `spawn_merger`.
    pub fn set_max_queue_bytes(&self, n: usize) {
        self.max_queue_bytes.store(n, Ordering::Relaxed);
    }

    /// Frame bytes currently queued in the merger.
    pub fn queued_bytes(&self) -> usize {
        self.queued_bytes.load(Ordering::Relaxed)
    }

    /// Merges every followed log's batches by seq.
    ///
    /// While one log holds the min watermark back (a dead peer whose fence
    /// hasn't been drained yet), every other log queues here. Past the byte
    /// budget the merger *spills* a log instead of queueing it: it ignores
    /// that log's batches from then on and later reads them back from its S3
    /// segments, one chunk at a time as the watermark lets them out, until it
    /// meets the live stream again. Memory stays near the budget however long
    /// the stall lasts; the merged order is unchanged (events are durable in
    /// S3 before any producer hands them to us).
    pub fn spawn_merger(self: &Arc<Self>, mut rx: mpsc::UnboundedReceiver<LogBatch>) {
        let fh = self.clone();
        tokio::spawn(async move {
            let mut logs: HashMap<Arc<str>, LogQ> = HashMap::new();
            // Everything at or below this has been emitted (or is below the
            // start floor): later events at or below it are late.
            let mut emitted = fh.start_floor;
            let mut total = 0usize;
            let mut tick = tokio::time::interval(Duration::from_millis(2));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut behind = false;
            loop {
                if !behind {
                    tick.tick().await;
                }
                behind = false;
                // Read the watermark *before* draining: anything at or below it
                // was sent to us before the watermark was published. Settle it
                // under the sources lock (see add_remote): from here on the
                // merger owes exactly the events <= w of the logs it saw.
                let w = {
                    let s = fh.sources.read();
                    let Some(w) = s.values().map(|s| s.get()).min() else {
                        continue;
                    };
                    fh.settled.fetch_max(w, Ordering::AcqRel);
                    w
                };
                let max = fh.max_queue_bytes.load(Ordering::Relaxed);
                let store = fh.store.read().clone();
                let mut late = 0usize;
                loop {
                    let b = match rx.try_recv() {
                        Ok(b) => b,
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => return,
                    };
                    let lq = logs.entry(b.log_id.clone()).or_default();
                    match &lq.spill {
                        // already read back from S3, or will be
                        Some(sp) if b.ordinal != sp.next || sp.end || total >= max / 2 => continue,
                        Some(_) => {
                            // the read-back met the live stream: queue it again
                            tracing::info!(log_id = %b.log_id, ordinal = b.ordinal, "firehose merger: spilled log rejoined the live stream");
                            lq.spill = None;
                        }
                        None if total >= max && store.is_some() => {
                            tracing::warn!(log_id = %b.log_id, ordinal = b.ordinal, queued = total, "firehose merger: queue over budget, spilling log to S3 read-back");
                            metrics::FIREHOSE_SPILLS.inc();
                            lq.spill = Some(Spill { next: b.ordinal, loaded: lq.high, end: false });
                            continue;
                        }
                        None => {}
                    }
                    total += lq.accept(b.events, emitted, fh.start_floor, &mut late);
                }
                // Read spilled logs back, up to w, a chunk at a time: emit only
                // up to what every spilled log has loaded.
                let mut bound = w;
                if let Some(store) = &store {
                    let chunk = (max / 16).max(1);
                    for (log_id, lq) in logs.iter_mut() {
                        let Some(sp) = &mut lq.spill else { continue };
                        let mut caught_up = sp.end || sp.loaded >= w;
                        let mut failed = false;
                        while !caught_up && lq.bytes < chunk {
                            match read_segment(store, log_id, sp.next).await {
                                Ok(Some(LogObject::Segment(_, entries))) => {
                                    metrics::FIREHOSE_SPILL_SEGMENTS.inc();
                                    sp.next += 1;
                                    let last = entries.last().map(|e| e.seq);
                                    let events = entries.into_iter().filter(|e| !e.frame.is_empty()).map(|e| (e.seq, e.frame)).collect();
                                    let LogQ { q, bytes, high, .. } = lq;
                                    let n = accept_into(q, bytes, high, events, emitted, fh.start_floor, &mut late);
                                    total += n;
                                    if let Some(l) = last {
                                        sp.loaded = sp.loaded.max(l);
                                    }
                                    caught_up = sp.loaded >= w;
                                }
                                // the fence: the log is complete
                                Ok(Some(LogObject::Fence { .. })) => {
                                    sp.end = true;
                                    caught_up = true;
                                }
                                // not written: every event <= w of this log was
                                // PUT before w was published, so all are loaded
                                Ok(None) => caught_up = true,
                                Err(e) => {
                                    tracing::warn!(%log_id, ordinal = sp.next, "firehose merger: reading back a spilled log failed: {e:#}");
                                    failed = true;
                                    break;
                                }
                            }
                        }
                        if !caught_up {
                            bound = bound.min(sp.loaded);
                            // more to read once this round is emitted (errors
                            // wait for the next tick)
                            behind |= !failed;
                        }
                    }
                }
                if late > 0 {
                    tracing::warn!(late, emitted, "firehose merger: dropped late events below the emitted watermark");
                }
                let mut out = Vec::new();
                for lq in logs.values_mut() {
                    while let Some((seq, f)) = lq.q.front() {
                        if *seq > bound {
                            break;
                        }
                        lq.bytes -= f.len();
                        total -= f.len();
                        out.push(lq.q.pop_front().unwrap());
                    }
                }
                emitted = emitted.max(bound);
                // forget logs that are gone and fully emitted
                {
                    let s = fh.sources.read();
                    logs.retain(|id, lq| !lq.q.is_empty() || s.contains_key(id) || lq.spill.as_ref().is_some_and(|sp| !sp.end));
                }
                fh.queued_bytes.store(total, Ordering::Relaxed);
                metrics::FIREHOSE_MERGE_QUEUE_BYTES.set(total as i64);
                if out.is_empty() {
                    continue;
                }
                out.sort_unstable_by_key(|(s, _)| *s);
                let bytes = out.iter().map(|(_, b)| b.len()).sum();
                let batch = Arc::new(MergedBatch {
                    first: out[0].0,
                    last: out[out.len() - 1].0,
                    events: out,
                    bytes,
                });
                STATS
                    .firehose_events
                    .fetch_add(batch.events.len() as u64, Ordering::Relaxed);
                metrics::FIREHOSE_EVENTS.inc_by(batch.events.len() as u64);
                metrics::FIREHOSE_BATCH.observe(batch.events.len() as f64);
                fh.push(batch);
            }
        });
    }

    fn push(&self, batch: Arc<MergedBatch>) {
        {
            let mut ring = self.ring.write();
            self.ring_bytes
                .fetch_add(batch.bytes as i64, Ordering::Relaxed);
            ring.push_back(batch.clone());
            while self.ring_bytes.load(Ordering::Relaxed) > self.max_ring_bytes && ring.len() > 1 {
                let old = ring.pop_front().unwrap();
                self.ring_floor.fetch_max(old.last, Ordering::AcqRel);
                self.ring_bytes
                    .fetch_sub(old.bytes as i64, Ordering::Relaxed);
            }
        }
        metrics::FIREHOSE_RING_BYTES.set(self.ring_bytes.load(Ordering::Relaxed));
        self.last_emitted.store(batch.last, Ordering::Release);
        let _ = self.tx.send(batch);
    }

    /// Events with seq > `after` currently in the ring, and whether the ring
    /// still reaches back to `after` (false = some were already dropped).
    fn from_ring(&self, after: i64) -> (Vec<Arc<MergedBatch>>, bool) {
        let ring = self.ring.read();
        // seqs are gappy: completeness is about what was evicted, not adjacency.
        let complete = after >= self.ring_floor.load(Ordering::Acquire);
        let out = ring.iter().filter(|b| b.last > after).cloned().collect();
        (out, complete)
    }

    pub async fn serve(self: Arc<Self>, ws: WebSocket, cursor: Option<i64>) {
        metrics::FIREHOSE_SUBSCRIBERS.inc();
        let reason = self.serve_inner(ws, cursor).await;
        metrics::FIREHOSE_SUBSCRIBERS.dec();
        metrics::FIREHOSE_DISCONNECTS
            .with_label_values(&[reason])
            .inc();
    }

    async fn serve_inner(self: Arc<Self>, mut ws: WebSocket, cursor: Option<i64>) -> &'static str {
        let mut rx = self.tx.subscribe();
        let mut last = match cursor {
            Some(c) => c,
            None => self.last_emitted.load(Ordering::Acquire),
        };
        if let Some(c) = cursor {
            // seqs are time-based: a cursor beyond both the stream head and the
            // current clock can't have been issued by us
            let now = crate::nodelog::seq_floor(crate::tid::now_micros()) | 0xff;
            if c > self.last_emitted.load(Ordering::Acquire).max(now) {
                let _ = ws.send(Message::Binary(events::error_frame("FutureCursor", "cursor in the future").into())).await;
                let _ = ws.send(Message::Close(None)).await;
                return "future_cursor";
            }
        }
        if cursor.is_some() {
            // older than the ring: stream it from the S3 segments first
            let store = self.store.read().clone();
            if let Some(store) = store {
                loop {
                    // backfill up to the ring floor, once every log is durable
                    // up to it (right after startup the start floor can be
                    // ahead of a peer's watermark: its events <= F may not be
                    // in S3 yet)
                    let floor = self.ring_floor.load(Ordering::Acquire);
                    if last >= floor {
                        break;
                    }
                    if self.settled.load(Ordering::Acquire) < floor {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        continue;
                    }
                    let (tx, mut brx) = mpsc::channel(4096);
                    let (st, from) = (store.clone(), last);
                    let job = tokio::spawn(async move { crate::backfill::backfill(&st, from, floor, &tx).await });
                    while let Some((seq, frame)) = brx.recv().await {
                        if ws.send(Message::Binary(frame)).await.is_err() {
                            job.abort();
                            return "client_gone";
                        }
                        metrics::FIREHOSE_SENT.inc();
                        last = seq;
                    }
                    match job.await {
                        Ok(Ok(_)) => {}
                        _ => break,
                    }
                    last = last.max(floor); // everything <= floor that exists was sent
                }
            }
            let (batches, complete) = self.from_ring(last);
            if !complete {
                let _ = ws.send(Message::Binary(info_frame("OutdatedCursor", "cursor is older than the retained history; starting from the oldest available event").into())).await;
            }
            if send_batches(&mut ws, &batches, &mut last).await.is_err() {
                return "client_gone";
            }
        }
        loop {
            match rx.recv().await {
                Ok(b) => {
                    if send_batches(&mut ws, std::slice::from_ref(&b), &mut last)
                        .await
                        .is_err()
                    {
                        return "client_gone";
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    let (batches, complete) = self.from_ring(last);
                    if !complete {
                        let _ = ws
                            .send(Message::Binary(
                                events::error_frame(
                                    "ConsumerTooSlow",
                                    "fell behind the in-memory window",
                                )
                                .into(),
                            ))
                            .await;
                        return "too_slow";
                    }
                    if send_batches(&mut ws, &batches, &mut last).await.is_err() {
                        return "client_gone";
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return "shutdown",
            }
        }
    }
}

/// Default byte budget of the merger's queues (see `Firehose::spawn_merger`).
pub const DEFAULT_MERGE_QUEUE_BYTES: usize = 256 << 20;

/// One log's events waiting in the merger.
#[derive(Default)]
struct LogQ {
    q: VecDeque<(i64, Bytes)>,
    bytes: usize,
    /// Highest seq accepted: drops duplicates when a log's events arrive
    /// twice (S3 catch-up overlapping a live stream).
    high: i64,
    spill: Option<Spill>,
}

/// A log the merger stopped queueing: its batches are read back from S3.
struct Spill {
    /// next ordinal to read
    next: u64,
    /// every event of the log <= this is queued or emitted
    loaded: i64,
    /// read up to the log's fence
    end: bool,
}

impl LogQ {
    fn accept(&mut self, events: Vec<(i64, Bytes)>, emitted: i64, start_floor: i64, late: &mut usize) -> usize {
        accept_into(&mut self.q, &mut self.bytes, &mut self.high, events, emitted, start_floor, late)
    }
}

/// Queues a log's events in seq order; returns the bytes added.
fn accept_into(
    q: &mut VecDeque<(i64, Bytes)>,
    bytes: &mut usize,
    high: &mut i64,
    events: Vec<(i64, Bytes)>,
    emitted: i64,
    start_floor: i64,
    late: &mut usize,
) -> usize {
    let mut added = 0;
    for (seq, frame) in events {
        if seq <= *high {
            continue;
        }
        *high = seq;
        // At or below what we already emitted: the start of a follower's S3
        // catch-up (<= the start floor, backfill serves it), or a late event
        // (a log we weren't following yet, or a watermark that overpromised),
        // which live order can't take.
        if seq <= emitted {
            if seq > start_floor {
                *late += 1;
            }
            continue;
        }
        added += frame.len();
        q.push_back((seq, frame));
    }
    *bytes += added;
    added
}

/// A log object, checked against the path it was read from (None = missing).
async fn read_segment(store: &crate::store::Store, log_id: &str, ordinal: u64) -> anyhow::Result<Option<LogObject>> {
    use object_store::ObjectStoreExt;
    let data = match store.raw.get(&crate::nodelog::segment_path(store, log_id, ordinal)).await {
        Ok(r) => r.bytes().await?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let obj = segment::parse(data, false, None)?;
    if let LogObject::Segment(h, _) = &obj {
        anyhow::ensure!(h.log_id == log_id && h.ordinal == ordinal, "log object {log_id}/{ordinal} has header {}/{}", h.log_id, h.ordinal);
    }
    Ok(Some(obj))
}

async fn send_batches(
    ws: &mut WebSocket,
    batches: &[Arc<MergedBatch>],
    last: &mut i64,
) -> Result<(), axum::Error> {
    for b in batches {
        if b.last <= *last {
            continue;
        }
        for (seq, frame) in &b.events {
            if *seq <= *last {
                continue;
            }
            ws.send(Message::Binary(frame.clone())).await?;
            metrics::FIREHOSE_SENT.inc();
            *last = *seq;
        }
    }
    Ok(())
}

fn info_frame(name: &str, message: &str) -> Vec<u8> {
    use crate::cbor::*;
    let mut out = Vec::new();
    write_map_head(&mut out, 2);
    write_text(&mut out, "t");
    write_text(&mut out, "#info");
    write_text(&mut out, "op");
    write_uint(&mut out, 1);
    write_map_head(&mut out, 2);
    write_text(&mut out, "name");
    write_text(&mut out, name);
    write_text(&mut out, "message");
    write_text(&mut out, message);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::SegmentBuilder;
    use object_store::{ObjectStoreExt, PutPayload};

    async fn put_seg(store: &crate::store::Store, log: &str, ord: u64, seq: i64, frame_len: usize) -> LogBatch {
        let frame = Bytes::from(vec![ord as u8; frame_len]);
        let mut b = SegmentBuilder::new();
        b.push(seq, 0, 1, |o| o.extend_from_slice(&frame), &[]);
        let mut obj = b.header(log, ord);
        obj.extend_from_slice(&b.body);
        store.raw.put(&crate::nodelog::segment_path(store, log, ord), PutPayload::from(obj)).await.unwrap();
        LogBatch { log_id: log.into(), ordinal: ord, events: vec![(seq, frame)] }
    }

    /// A stalled log holds the min watermark back while another keeps
    /// writing: the merger's queue stays near its budget (the busy log is
    /// read back from S3 later) and the merged stream is still complete and
    /// in order once the stall ends.
    #[tokio::test]
    async fn merger_queue_is_bounded_while_a_log_stalls() {
        let store = crate::store::Store::memory(None);
        let fh = Firehose::new(64 << 20);
        fh.set_max_queue_bytes(2000);
        *fh.store.write() = Some(store.clone());
        let (_, wa) = fh.add_remote("A");
        let (_, wb) = fh.add_remote("B");
        let (tx, rx) = mpsc::unbounded_channel();
        let mut sub = fh.tx.subscribe();
        fh.spawn_merger(rx);
        let base = fh.position();
        let seq = |k: i64| base + k * 256 + 1;
        let n = 60;
        for k in 0..n {
            tx.send(put_seg(&store, "B", k as u64, seq(2 * k + 1), 200).await).unwrap();
            wb.store(seq(2 * k + 1), Ordering::Release);
            if k % 8 == 0 {
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
            assert!(fh.queued_bytes() <= 2000 + 200, "queued {}", fh.queued_bytes());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(fh.queued_bytes() <= 2000 + 200, "queued {}", fh.queued_bytes());
        // the stalled log delivers interleaved events, then catches up
        for k in 0..n {
            tx.send(put_seg(&store, "A", k as u64, seq(2 * k), 10).await).unwrap();
        }
        wa.store(seq(2 * n), Ordering::Release);
        let mut got = Vec::new();
        while got.len() < 2 * n as usize {
            let b = tokio::time::timeout(Duration::from_secs(5), sub.recv()).await.expect("merged stream stalled").unwrap();
            got.extend(b.events.iter().map(|(s, _)| *s));
        }
        assert_eq!(got, (0..2 * n).map(seq).collect::<Vec<_>>());
        // B rejoins the live stream after the read-back
        tx.send(put_seg(&store, "B", n as u64, seq(2 * n + 1), 200).await).unwrap();
        wb.store(seq(2 * n + 1), Ordering::Release);
        wa.store(seq(2 * n + 1), Ordering::Release);
        let b = tokio::time::timeout(Duration::from_secs(5), sub.recv()).await.unwrap().unwrap();
        assert_eq!(b.events[0].0, seq(2 * n + 1));
    }
}
