//! Firehose: merges durable node-log streams into one seq-ordered stream.
//!
//! Each log (this node's, each live peer's, and any dead node's log still
//! being drained from S3) delivers durable batches and a watermark W_l (every
//! event with seq <= W_l has been delivered). The merger emits events with
//! seq <= min_l W_l in seq order, so the merged order is total, stable and the
//! same on every node.

use crate::events;
use crate::metrics;
use crate::nodelog::{LogBatch, Watermark};
use crate::stats::STATS;
use axum::extract::ws::{Message, WebSocket};
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering};
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
    /// The ring holds every event with seq > ring_floor (i64::MAX until the
    /// first batch). Older cursors are backfilled from S3.
    ring_floor: AtomicI64,
    /// Object store for S3 backfill (set once the node log is known).
    pub store: RwLock<Option<crate::store::Store>>,
}

impl Firehose {
    pub fn new(max_ring_bytes: usize) -> Arc<Firehose> {
        let (tx, _) = broadcast::channel(4096);
        Arc::new(Firehose {
            tx,
            ring: RwLock::new(VecDeque::new()),
            ring_bytes: AtomicI64::new(0),
            max_ring_bytes: max_ring_bytes as i64,
            last_emitted: AtomicI64::new(0),
            sources: RwLock::new(HashMap::new()),
            ring_floor: AtomicI64::new(i64::MAX),
            store: RwLock::new(None),
        })
    }

    pub fn min_watermark(&self) -> Option<i64> {
        self.sources.read().values().map(|s| s.get()).min()
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

    pub fn spawn_merger(self: &Arc<Self>, mut rx: mpsc::UnboundedReceiver<LogBatch>) {
        let fh = self.clone();
        tokio::spawn(async move {
            let mut queues: HashMap<Arc<str>, VecDeque<(i64, Bytes)>> = HashMap::new();
            // Highest seq accepted per log: drops duplicates when a log's
            // events arrive twice (S3 catch-up overlapping a live stream).
            let mut high: HashMap<Arc<str>, i64> = HashMap::new();
            let mut tick = tokio::time::interval(Duration::from_millis(2));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                // Read the watermark *before* draining: anything at or below it
                // was sent to us before the watermark was published.
                let Some(w) = fh.min_watermark() else {
                    continue;
                };
                loop {
                    match rx.try_recv() {
                        Ok(b) => {
                            let h = high.entry(b.log_id.clone()).or_insert(i64::MIN);
                            let q = queues.entry(b.log_id.clone()).or_default();
                            for (seq, frame) in b.events {
                                if seq > *h {
                                    *h = seq;
                                    q.push_back((seq, frame));
                                }
                            }
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => return,
                    }
                }
                let mut out = Vec::new();
                for q in queues.values_mut() {
                    while let Some((seq, _)) = q.front() {
                        if *seq > w {
                            break;
                        }
                        out.push(q.pop_front().unwrap());
                    }
                }
                if out.is_empty() {
                    continue;
                }
                out.sort_unstable_by_key(|(s, _)| *s);
                // Diagnostic: an event at or below what we already emitted
                // arrived after the min watermark passed it (a log we weren't
                // following yet, or a watermark that overpromised). Live
                // subscribers skip it (seq <= their last), so it is lost to them
                // and the merged order differs from other nodes'.
                let prev = fh.last_emitted.load(Ordering::Acquire);
                let late = out.iter().take_while(|(s, _)| *s <= prev).count();
                if late > 0 {
                    tracing::warn!(late, first_late = out[0].0, last_emitted = prev, "firehose merger: late events below the emitted watermark");
                }
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
            if ring.is_empty() && self.ring_floor.load(Ordering::Acquire) == i64::MAX {
                self.ring_floor.store(batch.first - 1, Ordering::Release);
            }
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
        // An empty ring that never held anything is complete (backfill covers
        // anything older from S3).
        let floor = self.ring_floor.load(Ordering::Acquire);
        let complete = floor == i64::MAX || after >= floor;
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
                    // backfill up to the ring floor, or (empty ring) up to what
                    // the live stream has already emitted
                    let floor = match self.ring_floor.load(Ordering::Acquire) {
                        i64::MAX => self.last_emitted.load(Ordering::Acquire),
                        f => f,
                    };
                    if last >= floor {
                        break;
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
