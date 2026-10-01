//! Cross-node log streams (merged firehose).
//!
//! Each node serves its own log at `/internal/v1/log/stream`, a websocket of
//! binary messages:
//!   0x00 | ordinal u64 | count u32 | (seq i64 | len u32 | frame)*   durable batch
//!   0x01 | watermark i64                                            heartbeat
//! A watermark promises every event with seq <= it has been sent.
//!
//! Every node follows every peer's log. On (re)connect it first catches up
//! from S3 segments after the last ordinal it delivered. When the peer dies,
//! the follower drains the log from S3 up to its fence object and then
//! retires (the host removes its firehose source).

use crate::nodelog::{segment_path, LogBatch, NodeLog};
use crate::segment::{self, LogObject};
use crate::store::Store;
use axum::extract::ws::{Message, WebSocket};
use bytes::{Buf, BufMut, Bytes};
use futures::{SinkExt, StreamExt};
use object_store::ObjectStoreExt;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

const HEARTBEAT: Duration = Duration::from_millis(5);

pub fn encode_batch(b: &LogBatch) -> Bytes {
    let size: usize = b.events.iter().map(|(_, f)| f.len() + 12).sum();
    let mut out = Vec::with_capacity(13 + size);
    out.put_u8(0);
    out.put_u64(b.ordinal);
    out.put_u32(b.events.len() as u32);
    for (seq, f) in &b.events {
        out.put_i64(*seq);
        out.put_u32(f.len() as u32);
        out.put_slice(f);
    }
    out.into()
}

pub enum StreamMsg {
    Batch(LogBatch),
    Watermark(i64),
}

pub fn decode(log_id: &Arc<str>, data: Bytes) -> anyhow::Result<StreamMsg> {
    let mut r = data.clone();
    anyhow::ensure!(r.has_remaining(), "empty message");
    match r.get_u8() {
        0 => {
            anyhow::ensure!(r.remaining() >= 12, "short batch");
            let ordinal = r.get_u64();
            let n = r.get_u32() as usize;
            let mut events = Vec::with_capacity(n);
            for _ in 0..n {
                anyhow::ensure!(r.remaining() >= 12, "short event");
                let seq = r.get_i64();
                let len = r.get_u32() as usize;
                anyhow::ensure!(r.remaining() >= len, "short frame");
                let off = data.len() - r.remaining();
                events.push((seq, data.slice(off..off + len)));
                r.advance(len);
            }
            Ok(StreamMsg::Batch(LogBatch { log_id: log_id.clone(), ordinal, events }))
        }
        1 => {
            anyhow::ensure!(r.remaining() >= 8, "short watermark");
            Ok(StreamMsg::Watermark(r.get_i64()))
        }
        t => anyhow::bail!("unknown message type {t}"),
    }
}

/// Owner side: stream our log's durable batches and watermark heartbeats.
pub async fn serve_stream(mut ws: WebSocket, log: Arc<NodeLog>) {
    let mut rx = log.live.subscribe();
    let mut tick = tokio::time::interval(HEARTBEAT);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        // Read the watermark *before* draining: batches covered by it were
        // broadcast before it advanced, so they are already in our queue.
        let w = log.wm.get();
        loop {
            match rx.try_recv() {
                Ok(b) => {
                    if ws.send(Message::Binary(encode_batch(&b))).await.is_err() {
                        return;
                    }
                }
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    // the peer catches up from S3 when it reconnects
                    let _ = ws.close().await;
                    return;
                }
                Err(broadcast::error::TryRecvError::Closed) => return,
            }
        }
        let mut m = Vec::with_capacity(9);
        m.put_u8(1);
        m.put_i64(w);
        if ws.send(Message::Binary(m.into())).await.is_err() {
            return;
        }
    }
}

/// A follower of one peer log feeding our merger.
pub struct Follower {
    pub log_id: Arc<str>,
    pub watermark: Arc<AtomicI64>,
    pub stop: Arc<AtomicBool>,
    /// Set once a dead log has been drained up to its fence.
    pub done: Arc<AtomicBool>,
}

/// `addr` returns the peer's base URL while it is alive, None once it's dead.
pub fn follow_log(
    log_id: &str,
    store: Store,
    addr: Arc<dyn Fn() -> Option<String> + Send + Sync>,
    token: String,
    merger_tx: mpsc::UnboundedSender<LogBatch>,
) -> Follower {
    let log_id: Arc<str> = log_id.into();
    let f = Follower {
        log_id: log_id.clone(),
        watermark: Arc::new(AtomicI64::new(0)),
        stop: Arc::new(AtomicBool::new(false)),
        done: Arc::new(AtomicBool::new(false)),
    };
    let (wm, stop, done) = (f.watermark.clone(), f.stop.clone(), f.done.clone());
    tokio::spawn(async move {
        // last ordinal delivered to the merger
        let mut last: Option<u64> = None;
        while !stop.load(Ordering::Acquire) {
            match addr() {
                Some(base) => {
                    if let Err(e) = stream_live(&log_id, &store, &base, &token, &merger_tx, &wm, &mut last, &stop).await {
                        tracing::debug!(%log_id, "log stream from {base} ended: {e:#}");
                    }
                }
                // never streamed it while alive: its events predate our firehose
                None if last.is_none() => {
                    done.store(true, Ordering::Release);
                    return;
                }
                None => match drain_s3(&log_id, &store, &merger_tx, &wm, &mut last).await {
                    Ok(true) => {
                        done.store(true, Ordering::Release);
                        return;
                    }
                    Ok(false) => {} // not fenced yet: keep waiting
                    Err(e) => tracing::warn!(%log_id, "draining dead log: {e:#}"),
                },
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    f
}

/// Delivers segments after `last` from S3. Ok(true) = reached the fence.
async fn drain_s3(
    log_id: &Arc<str>,
    store: &Store,
    merger_tx: &mpsc::UnboundedSender<LogBatch>,
    wm: &AtomicI64,
    last: &mut Option<u64>,
) -> anyhow::Result<bool> {
    let mut next = last.map(|o| o + 1).unwrap_or(0);
    loop {
        let data = match store.raw.get(&segment_path(store, log_id, next)).await {
            Ok(r) => r.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        match segment::parse(data, false, None)? {
            LogObject::Fence { .. } => return Ok(true),
            LogObject::Segment(h, entries) => {
                let events: Vec<_> = entries.into_iter().filter(|e| !e.frame.is_empty()).map(|e| (e.seq, e.frame)).collect();
                let _ = merger_tx.send(LogBatch { log_id: log_id.clone(), ordinal: next, events });
                wm.fetch_max(h.last_seq, Ordering::AcqRel);
                *last = Some(next);
                next += 1;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn stream_live(
    log_id: &Arc<str>,
    store: &Store,
    base: &str,
    token: &str,
    merger_tx: &mpsc::UnboundedSender<LogBatch>,
    wm: &AtomicI64,
    last: &mut Option<u64>,
    stop: &AtomicBool,
) -> anyhow::Result<()> {
    let url = format!("{}/internal/v1/log/stream", base.replacen("http", "ws", 1));
    let mut req = tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(url.as_str())?;
    req.headers_mut().insert("x-vlpds-internal", token.parse()?);
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await?;
    // Connected (live batches now buffer in the socket): catch up from S3
    // after what we already delivered, then dedupe against the stream.
    if last.is_some() {
        drain_s3(log_id, store, merger_tx, wm, last).await?;
    }
    while let Some(msg) = ws.next().await {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        let tokio_tungstenite::tungstenite::Message::Binary(data) = msg? else { continue };
        match decode(log_id, data)? {
            StreamMsg::Batch(b) => {
                if let Some(ord) = *last {
                    if b.ordinal <= ord {
                        continue; // already delivered via S3 catch-up
                    }
                    anyhow::ensure!(b.ordinal == ord + 1, "log {log_id} stream gap: {} after {ord}", b.ordinal);
                }
                *last = Some(b.ordinal);
                let _ = merger_tx.send(b);
            }
            StreamMsg::Watermark(w) => {
                wm.fetch_max(w, Ordering::AcqRel);
            }
        }
    }
    let _ = ws.close(None).await;
    Ok(())
}
