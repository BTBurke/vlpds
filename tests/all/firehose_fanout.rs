//! subscribeRepos fan-out isolation: subscribers are served from the shared
//! ring on their own runtime with pre-built websocket messages; a stalled
//! one is cut off by its lag bound (ConsumerTooSlow) without delaying writes
//! or other subscribers.
use crate::common::*;
use futures::{SinkExt, StreamExt};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::Role;

/// A websocket client that does the handshake and then reads nothing: its
/// receive window fills and the server's writes to it block.
async fn stalled_client(s: &TestServer) -> tokio::net::TcpStream {
    let sock = tokio::net::TcpSocket::new_v4().unwrap();
    sock.set_recv_buffer_size(4096).unwrap();
    let mut c = sock.connect(s.addr).await.unwrap();
    let req = format!(
        "GET /xrpc/com.atproto.sync.subscribeRepos HTTP/1.1\r\nHost: {}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
         Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
        s.addr
    );
    c.write_all(req.as_bytes()).await.unwrap();
    // the response head, a byte at a time so no frame bytes are consumed
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(c.read_u8().await.unwrap());
    }
    let head = String::from_utf8(head).unwrap();
    assert!(head.starts_with("HTTP/1.1 101"), "{head}");
    assert!(head.contains("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="), "accept key: {head}");
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_subscriber_is_cut_off_without_delaying_writes_or_others() {
    let s = TestServer::spawn_with(|c| {
        c.firehose_max_lag_bytes = 256 << 10;
        c.firehose_ring_bytes = 64 << 20;
    })
    .await;
    let a = s.create_account("stall").await;
    let mut healthy = s.subscribe(None).await;
    s.sync_subs(&a, std::slice::from_mut(&mut healthy)).await;
    let stalled = stalled_client(&s).await;
    // the healthy one reads as fast as it can
    let (rev_tx, rev_rx) = tokio::sync::oneshot::channel::<String>();
    let healthy = tokio::spawn(async move {
        let mut frames = Vec::new();
        let mut rev_rx = rev_rx;
        let mut want: Option<String> = None;
        loop {
            if want.is_none() {
                want = rev_rx.try_recv().ok();
            }
            if let (Some(w), Some(f)) = (&want, frames.last()) {
                if Frame::str(f, "rev") == Some(w.as_str()) {
                    return frames;
                }
            }
            match healthy.next(Duration::from_millis(100)).await {
                Some(f) => frames.push(f),
                None => assert!(!healthy.closed, "healthy subscriber closed: {:?}", frames.last()),
            }
        }
    });

    // ~8 MB of commits: far past the stalled reader's socket buffers and lag bound
    let big = "x".repeat(40_000);
    let mut lat = Vec::new();
    let mut last = None;
    for i in 0..200 {
        let t = Instant::now();
        last = Some(s.create_record(&a, "com.example.big", json!({"$type": "com.example.big", "i": i, "data": big})).await);
        lat.push(t.elapsed());
    }
    lat.sort();
    let p99 = lat[lat.len() * 99 / 100];
    assert!(p99 < Duration::from_secs(2), "write p99 {p99:?} with a stalled subscriber");

    // the healthy subscriber got every commit, in order
    let rev = last.unwrap().rev.unwrap();
    rev_tx.send(rev.clone()).unwrap();
    let frames = tokio::time::timeout(FH_TIMEOUT, healthy).await.expect("healthy subscriber fell behind").unwrap();
    let commits: Vec<_> = frames.iter().filter(|f| f.kind() == "#commit" && f.did() == Some(a.did.as_str())).collect();
    assert!(commits.len() >= 200, "healthy subscriber got {} commits", commits.len());
    let seqs: Vec<i64> = frames.iter().filter_map(|f| f.seq()).collect();
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "out of order");

    // the stalled one, read now: what fit in its buffers, then
    // ConsumerTooSlow and a close
    let mut ws = tokio_tungstenite::WebSocketStream::from_raw_socket(stalled, Role::Client, None).await;
    let mut got = 0;
    let mut error = None;
    loop {
        match tokio::time::timeout(FH_TIMEOUT, ws.next()).await.expect("stalled subscriber never closed") {
            Some(Ok(Message::Binary(b))) => {
                let f = Frame::decode(&b).unwrap();
                if f.op == -1 {
                    error = f.str("error").map(String::from);
                } else {
                    assert!(error.is_none(), "event after the error frame");
                    got += 1;
                }
            }
            Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
            Some(Ok(_)) => {}
        }
    }
    assert_eq!(error.as_deref(), Some("ConsumerTooSlow"), "after {got} events");
    assert!(got < commits.len(), "the stalled subscriber still got all {got} events");

    // and it can resume from its cursor
    let mut again = s.subscribe(Some(seqs[0])).await;
    let rest = again.until(FH_TIMEOUT, |fs| fs.last().is_some_and(|f| f.str("rev") == Some(rev.as_str()))).await;
    assert_eq!(rest.iter().filter_map(|f| f.seq()).collect::<Vec<_>>(), seqs[1..].to_vec());
}

/// The hand-rolled server side speaks enough websocket: pings are answered,
/// a client close is echoed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_ping_and_close() {
    let s = TestServer::spawn().await;
    let (mut ws, _) = tokio_tungstenite::connect_async(s.ws_url(None)).await.unwrap();
    ws.send(Message::Ping(b"hi".to_vec().into())).await.unwrap();
    let m = tokio::time::timeout(FH_TIMEOUT, ws.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(m, Message::Pong(b"hi".to_vec().into()));
    ws.close(None).await.unwrap();
    let m = tokio::time::timeout(FH_TIMEOUT, ws.next()).await.unwrap();
    assert!(matches!(m, Some(Ok(Message::Close(_))) | None), "{m:?}");
    // not a websocket request
    let r = reqwest::get(format!("{}/xrpc/com.atproto.sync.subscribeRepos", s.url)).await.unwrap();
    assert_eq!(r.status(), 400);
}
