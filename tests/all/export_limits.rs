//! Bounded serving paths: getRepo exports whose readers stop reading end
//! (and free their slot, blocking thread and buffers) while commits keep
//! acking; identical record blocks come by each entry; subscribeRepos
//! connections are capped per client address, and a bad cursor is an XRPC
//! error.

use crate::common::*;
use std::time::{Duration, Instant};

const BIG: &str = "com.example.big";

/// A record of about `kb` KiB, distinct per `i`.
fn big(i: usize, kb: usize) -> J {
    json!({"$type": BIG, "i": i, "data": "x".repeat(kb << 10)})
}

type H2 = hyper::client::conn::http2::SendRequest<axum::body::Body>;

/// An h2 connection that never opens a stream window: the server can send
/// response heads but no body bytes (a client that stopped reading).
async fn zero_window_client(addr: std::net::SocketAddr) -> H2 {
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (send, conn) = hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
        .initial_stream_window_size(0)
        .handshake(hyper_util::rt::TokioIo::new(tcp))
        .await
        .unwrap();
    tokio::spawn(conn);
    send
}

fn ended(reason: &str) -> u64 {
    vlpds::metrics::SYNC_EXPORTS_ENDED.with_label_values(&[reason]).get()
}

/// Exports to clients that read nothing hold at most `max_exports` slots,
/// end after `export_stall`, and the next ones (queued for a slot) get
/// theirs; meanwhile writes keep committing, and afterwards a normal export
/// is served whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_export_readers_dont_block_commits() {
    let s = TestServer::spawn_with(|c| {
        c.max_exports = 2;
        c.export_stall = Duration::from_millis(400);
    })
    .await;
    let a = s.create_account("exp").await;
    // ~11 MB of records: more than an export queues for its body
    for i in 0..12 {
        s.create_record(&a, BIG, big(i, 900)).await;
    }
    let stalled_before = ended("stalled");
    let mut h2 = zero_window_client(s.addr).await;
    let url = format!("http://{}/xrpc/com.atproto.sync.getRepo?did={}", s.addr, a.did);
    let mut heads = Vec::new();
    for _ in 0..4 {
        h2.ready().await.unwrap();
        let req = axum::http::Request::get(&url).body(axum::body::Body::empty()).unwrap();
        heads.push(h2.send_request(req));
    }
    // while they stall: commits ack promptly
    let writer = async {
        let mut worst = Duration::ZERO;
        for i in 0..10 {
            let t = Instant::now();
            s.post(&a, &format!("during stalled exports {i}")).await;
            worst = worst.max(t.elapsed());
        }
        worst
    };
    let (responses, worst) =
        tokio::join!(async { tokio::time::timeout(Duration::from_secs(20), futures::future::join_all(heads)).await.expect("export heads") }, writer);
    assert!(worst < Duration::from_secs(2), "a commit took {worst:?} during stalled exports");
    let mut bodies = Vec::new();
    for r in responses {
        let r = r.expect("getRepo head");
        assert_eq!(r.status(), 200);
        bodies.push(r.into_body()); // held, never read
    }
    // all four stall out: two at a time, each queued one after a slot frees
    wait_until("all four stalled exports end", Duration::from_secs(15), || ended("stalled") >= stalled_before + 4).await;
    // a reading client gets the whole repo
    let repo = s.get_repo(&a.did).await;
    repo.check_block_hashes().unwrap();
    assert_eq!(repo.entries().len(), 22);
    drop(bodies);
}

/// Records with identical contents share a block: the streamable order puts
/// it by each entry naming it (a single-pass reader finds every record by
/// its node), so it comes once per entry and nothing else repeats.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_records_come_by_each_entry() {
    let s = TestServer::spawn().await;
    let a = s.create_account("dup").await;
    let same = json!({"$type": BIG, "same": true});
    let r1 = s.create_record(&a, BIG, same.clone()).await;
    let r2 = s.create_record(&a, BIG, same).await;
    assert_eq!(r1.cid, r2.cid);
    s.post(&a, "different").await;
    let repo = s.get_repo(&a.did).await;
    let cid: Cid = Cid::parse(&r1.cid).unwrap();
    assert_eq!(repo.order.iter().filter(|c| **c == cid).count(), 2);
    assert_eq!(repo.order.len(), repo.blocks.len() + 1, "no other block twice");
    assert_eq!(repo.entries().len(), 3);
}

/// subscribeRepos: at most `firehose_max_per_ip` connections per client
/// address (429 past it, a slot back once one closes); a cursor that isn't
/// an integer is 400 InvalidRequest (XRPC JSON).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_repos_per_ip_cap_and_bad_cursor() {
    let s = TestServer::spawn_with(|c| c.firehose_max_per_ip = 2).await;
    let first = Sub::connect(&s.ws_url(None)).await;
    let _second = Sub::connect(&s.ws_url(None)).await;
    let local: std::net::IpAddr = "127.0.0.1".parse().unwrap();
    wait_until("two subscribers counted", Duration::from_secs(5), || s.app.firehose.connections_from(local) >= 2).await;
    match tokio_tungstenite::connect_async(s.ws_url(None)).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(r)) => assert_eq!(r.status(), 429),
        Err(e) => panic!("{e}"),
        Ok(_) => panic!("third connection from one address accepted"),
    }
    drop(first);
    wait_until("the closed subscriber uncounted", Duration::from_secs(5), || s.app.firehose.connections_from(local) <= 1).await;
    let _third = Sub::connect(&s.ws_url(None)).await;

    let r = s.xrpc.get("com.atproto.sync.subscribeRepos", &[("cursor", "abc")], &Auth::None).await;
    assert_eq!(r.status, 400, "{r:?}");
    assert_eq!(r.json["error"], "InvalidRequest", "{r:?}");
}
