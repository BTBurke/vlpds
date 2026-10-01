//! Cursor backfill beyond the in-memory firehose ring.
//!
//! DESIGN.md §3/§5: "Log = WAL = firehose ... Backfill: recent segments come
//! from memory; older ones are range-GETs from S3", with a retention window of
//! e.g. 72 h. So a cursor that is older than the in-memory ring, but within
//! retention, must be replayed in full from the segment log (the reference
//! PDS likewise serves its whole sequencer table window), byte-identical to
//! what live subscribers saw.
use crate::common::*;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backfill_beyond_ring_is_served_from_the_log() {
    let s = TestServer::spawn_with(|c| c.firehose_ring_bytes = 2048).await;
    let mut live = s.subscribe(Some(0)).await;
    let a = s.create_account("bf").await;
    let mut last = None;
    for i in 0..60 {
        last = Some(s.post(&a, &format!("backfill {i} {}", "x".repeat(64))).await);
    }
    let head = Cid::parse(last.unwrap().commit_cid.as_deref().unwrap()).unwrap();
    let live_frames = live
        .until(FH_TIMEOUT, |fs| fs.last().map(|f| matches!(f.body.get("commit"), Some(Value::Link(c)) if *c == head)).unwrap_or(false))
        .await;
    let live_seq: Vec<(i64, Vec<u8>)> = live_frames.iter().filter_map(|f| f.seq().map(|q| (q, f.raw.clone()))).collect();
    assert!(live_seq.len() >= 63, "expected #identity/#account/#sync + 60 commits, got {}", live_seq.len());

    // replay from cursor 0: must be complete, no OutdatedCursor
    let mut sub = s.subscribe(Some(0)).await;
    let frames = sub.drain(Duration::from_millis(800)).await;
    let infos: Vec<_> = frames.iter().filter(|f| f.kind() == "#info").map(|f| f.str("name").map(String::from)).collect();
    let got: Vec<(i64, Vec<u8>)> = frames.iter().filter_map(|f| f.seq().map(|q| (q, f.raw.clone()))).collect();
    assert!(
        infos.is_empty() && got == live_seq,
        "cursor=0 with a 2 KiB ring: got {} events (infos {infos:?}), want all {} live events byte-identical; \
         backfill older than the ring must come from the segment log",
        got.len(),
        live_seq.len()
    );

    // replay from a mid-stream cursor older than the ring: exactly the suffix
    let mid = live_seq[5].0;
    let mut sub = s.subscribe(Some(mid)).await;
    let frames = sub.drain(Duration::from_millis(800)).await;
    let got: Vec<i64> = frames.iter().filter_map(|f| f.seq()).collect();
    let want: Vec<i64> = live_seq.iter().map(|x| x.0).filter(|q| *q > mid).collect();
    assert_eq!(got, want, "cursor {mid} (older than the ring) must yield exactly the later events");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cursor_at_head_yields_only_new_events() {
    let s = TestServer::spawn().await;
    let a = s.create_account("hd").await;
    s.post(&a, "one").await;
    let head = s.current_seq().await;
    let mut sub = s.subscribe(Some(head)).await;
    assert!(sub.next(Duration::from_millis(400)).await.is_none(), "cursor at head replayed old events");
    let p = s.post(&a, "two").await;
    let f = sub.next(FH_TIMEOUT).await.expect("live event after cursor=head");
    let c = f.commit().expect("#commit");
    assert_eq!(Some(c.commit.to_string()), p.commit_cid);
    assert!(c.seq > head);
}
