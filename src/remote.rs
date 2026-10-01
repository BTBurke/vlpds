//! Cross-node log streams (merged firehose).
//!
//! Each node serves its own log at `/internal/v1/log/stream`, a websocket of
//! binary messages:
//!   0x00 | ordinal u64 | count u32 | (seq i64 | len u32 | frame)*   durable batch
//!   0x01 | watermark i64                                            heartbeat
//! A watermark promises every event with seq <= it has been sent.
//!
//! Every node follows every peer's log. A follower owes the merger every
//! event of the log above its floor (the merger's position when it started,
//! see firehose.rs), in ordinal order with no gaps. Its first ordinal is the
//! first segment in S3 past the floor. On every (re)connect, once the owner
//! has subscribed us, it catches up from S3 after the last ordinal it
//! delivered, then dedupes against the stream. When the peer dies, the
//! follower drains the log from S3 up to its fence object and then retires
//! (the host removes its firehose source). S3 reads are sequential and stop
//! at the first missing ordinal or the fence, so they only ever deliver the
//! log's gap-free durable prefix, never segments a crash left past a hole.

use crate::firehose::Firehose;
use crate::nodelog::{segment_path, LiveRecv, LogBatch, NodeLog};
use crate::segment::{self, LogObject};
use crate::store::Store;
use axum::extract::ws::{Message, WebSocket};
use bytes::{Buf, BufMut, Bytes};
use futures::{SinkExt, StreamExt};
use object_store::ObjectStoreExt;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

const HEARTBEAT: Duration = Duration::from_millis(5);
/// A live log stream (or its connect) silent this long is presumed dead.
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

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
                LiveRecv::Batch(b) => {
                    if ws.send(Message::Binary(encode_batch(&b))).await.is_err() {
                        return;
                    }
                }
                LiveRecv::Empty => break,
                LiveRecv::Lagged => {
                    // the peer catches up from S3 when it reconnects
                    crate::metrics::LOG_STREAM_LAGGED.inc();
                    tracing::warn!(log_id = %log.log_id, "peer fell behind our live ring: dropping its stream (it catches up from S3)");
                    let _ = ws.close().await;
                    return;
                }
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

/// Registers the log as a firehose source of `fh` and follows it. `addr`
/// returns the peer's base URL while it is alive, None once it's dead.
pub fn follow_log(
    log_id: &str,
    fh: &Firehose,
    store: Store,
    addr: Arc<dyn Fn() -> Option<String> + Send + Sync>,
    token: String,
    merger_tx: mpsc::UnboundedSender<LogBatch>,
) -> Follower {
    let log_id: Arc<str> = log_id.into();
    let (floor, watermark) = fh.add_remote(&log_id);
    let f = Follower {
        log_id: log_id.clone(),
        watermark,
        stop: Arc::new(AtomicBool::new(false)),
        done: Arc::new(AtomicBool::new(false)),
    };
    let (wm, stop, done) = (f.watermark.clone(), f.stop.clone(), f.done.clone());
    tokio::spawn(async move {
        // next ordinal to deliver to the merger (None until found in S3)
        let mut next: Option<u64> = None;
        while !stop.load(Ordering::Acquire) {
            match addr() {
                Some(base) => {
                    if let Err(e) = stream_live(&log_id, &store, floor, &base, &token, &merger_tx, &wm, &mut next, &stop).await {
                        tracing::debug!(%log_id, "log stream from {base} ended: {e:#}");
                    }
                }
                None => match catch_up(&log_id, &store, floor, &merger_tx, &wm, &mut next).await {
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

/// Delivers segments from `next` (first resolved as the first segment past
/// `floor`) from S3 until the first missing one. Ok(true) = reached the fence.
async fn catch_up(
    log_id: &Arc<str>,
    store: &Store,
    floor: i64,
    merger_tx: &mpsc::UnboundedSender<LogBatch>,
    wm: &AtomicI64,
    next: &mut Option<u64>,
) -> anyhow::Result<bool> {
    let next = match next {
        Some(n) => n,
        None => next.insert(crate::backfill::seek(store, log_id, floor).await?),
    };
    loop {
        let data = match store.raw.get(&segment_path(store, log_id, *next)).await {
            Ok(r) => r.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        match segment::parse(data, false, None)? {
            LogObject::Fence { .. } => return Ok(true),
            LogObject::Segment(h, entries) => {
                anyhow::ensure!(
                    *h.log_id == **log_id && h.ordinal == *next,
                    "log object {log_id}/{next} has header {}/{}",
                    h.log_id,
                    h.ordinal
                );
                let events: Vec<_> = entries.into_iter().filter(|e| !e.frame.is_empty()).map(|e| (e.seq, e.frame)).collect();
                let _ = merger_tx.send(LogBatch { log_id: log_id.clone(), ordinal: *next, events });
                wm.fetch_max(h.last_seq, Ordering::AcqRel);
                *next += 1;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn stream_live(
    log_id: &Arc<str>,
    store: &Store,
    floor: i64,
    base: &str,
    token: &str,
    merger_tx: &mpsc::UnboundedSender<LogBatch>,
    wm: &AtomicI64,
    next: &mut Option<u64>,
    stop: &AtomicBool,
) -> anyhow::Result<()> {
    let url = format!("{}/internal/v1/log/stream", base.replacen("http", "ws", 1));
    let mut req = tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(url.as_str())?;
    req.headers_mut().insert("x-vlpds-internal", token.parse()?);
    let (mut ws, _) = tokio::time::timeout(STREAM_IDLE_TIMEOUT, tokio_tungstenite::connect_async(req))
        .await
        .map_err(|_| anyhow::anyhow!("log stream connect to {base} timed out"))??;
    // The owner subscribes to its log only after the upgrade, so wait for its
    // first message before the S3 catch-up: every batch it won't send us was
    // broadcast, so PUT, before that. Catching up any earlier could miss a
    // batch landing in between, which the first heartbeat already covers.
    let Some(first) = next_msg(&mut ws, log_id, base).await? else { return Ok(()) };
    let mut first = Some(first);
    catch_up(log_id, store, floor, merger_tx, wm, next).await?;
    let n = next.as_mut().expect("resolved by catch_up");
    // HA fix: an idle timeout. The owner heartbeats every 5 ms, so silence
    // means a dead or partitioned peer. Without it, a half-open connection
    // (peer cut off by the network, then dead: no FIN/RST ever arrives) kept
    // us in ws.next() forever. We never noticed the peer's lease was gone, so
    // we never drained its log to the fence, and every survivor's merged
    // firehose stalled for good (bench/ha ctr-partition).
    loop {
        let msg = match first.take() {
            Some(m) => m,
            None => match next_msg(&mut ws, log_id, base).await? {
                Some(m) => m,
                None => break,
            },
        };
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        let Some(data) = msg else { continue };
        match decode(log_id, data)? {
            StreamMsg::Batch(b) => {
                if b.ordinal < *n {
                    continue; // already delivered via S3 catch-up
                }
                anyhow::ensure!(b.ordinal == *n, "log {log_id} stream gap: {} while expecting {n}", b.ordinal);
                *n += 1;
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

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// The next message of a log stream: None at its end, Some(None) for a
/// non-binary message.
async fn next_msg(ws: &mut Ws, log_id: &str, base: &str) -> anyhow::Result<Option<Option<Bytes>>> {
    match tokio::time::timeout(STREAM_IDLE_TIMEOUT, ws.next()).await {
        Ok(Some(m)) => Ok(Some(match m? {
            tokio_tungstenite::tungstenite::Message::Binary(data) => Some(data),
            _ => None,
        })),
        Ok(None) => Ok(None),
        Err(_) => anyhow::bail!("log {log_id} stream from {base} idle for {STREAM_IDLE_TIMEOUT:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::SegmentBuilder;
    use object_store::PutPayload;

    async fn put(store: &Store, ord: u64, prefix_end: u64) {
        let mut b = SegmentBuilder::new();
        b.push(1000 + ord as i64, 0, 1, |o| o.extend_from_slice(b"f"), &[]);
        let mut obj = b.sealed_header("A", ord, prefix_end);
        obj.extend_from_slice(&b.body);
        store.raw.put(&segment_path(store, "A", ord), PutPayload::from(obj)).await.unwrap();
    }

    /// Draining a dead log delivers its gap-free prefix up to the fence and
    /// nothing past it (segments a crash left beyond the hole).
    #[tokio::test]
    async fn catch_up_stops_at_the_fence_before_garbage() {
        let store = Store::memory(None);
        put(&store, 0, 0).await;
        put(&store, 1, 0).await;
        put(&store, 3, 2).await; // landed while 2 was in flight; 2 never did
        let log_id: Arc<str> = "A".into();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let wm = AtomicI64::new(0);
        let mut next = None;
        // not fenced yet: delivers 0, 1 and waits at the hole
        assert!(!catch_up(&log_id, &store, 0, &tx, &wm, &mut next).await.unwrap());
        assert_eq!(next, Some(2));
        store.raw.put(&segment_path(&store, "A", 2), PutPayload::from_bytes(segment::fence_object("B"))).await.unwrap();
        assert!(catch_up(&log_id, &store, 0, &tx, &wm, &mut next).await.unwrap());
        drop(tx);
        let mut ords = Vec::new();
        while let Some(b) = rx.recv().await {
            ords.push(b.ordinal);
        }
        assert_eq!(ords, vec![0, 1]);
        assert_eq!(wm.load(Ordering::Acquire), 1001);
        // a follower starting late seeks into the prefix, never past the fence
        let mut next = None;
        let (tx, _rx) = mpsc::unbounded_channel();
        assert!(catch_up(&log_id, &store, 1002, &tx, &wm, &mut next).await.unwrap());
        assert_eq!(next, Some(2));
    }
}
