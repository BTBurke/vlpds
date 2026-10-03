//! importRepo admission by estimated working set (src/xrpc/import_budget.rs):
//! many small imports run at once beside a large one, a body without (or
//! with a wrong) Content-Length is covered or refused cleanly, and an
//! exhausted budget refuses with a retryable 503 and leaks nothing.
use crate::common::*;
use crate::import_burst::repo_car;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use vlpds::state::bulk_did;
use vlpds::xrpc::staged_import as si;

const MIB: u64 = 1 << 20;

async fn node(budget: Option<u64>, wait: Duration) -> TestServer {
    TestServer::spawn_with(move |c| {
        c.allow_bulk_create = true;
        c.import_memory_bytes = budget;
        c.import_wait = wait;
    })
    .await
}

/// `n` empty bulk accounts from `start`, and their access tokens.
async fn accounts(s: &TestServer, start: u64, n: u64) -> Vec<(String, String)> {
    let r = s.xrpc.post("vlpds.admin.bulkCreate", &json!({"start": start, "count": n, "records": 0}), &Auth::Bearer(ADMIN_TOKEN.into())).await;
    assert_eq!(r.status, 200, "{}", r.text());
    (start..start + n).map(|i| (bulk_did(i), s.app.jwt.access(&bulk_did(i)))).collect()
}

async fn import(s: &TestServer, token: &str, car: Vec<u8>) -> Resp {
    s.import_repo(&Auth::Bearer(token.into()), car).await
}

/// Holds the import of `did` once it has begun (admitted, generation
/// reserved) until `go`, counting it in `held`.
fn hold(did: &str, held: Arc<AtomicUsize>, go: Arc<AtomicBool>) {
    si::set_crash_hook(
        did,
        Some(Arc::new(move |p: &str| {
            if p == "begun" {
                held.fetch_add(1, Ordering::SeqCst);
                let t = Instant::now();
                while !go.load(Ordering::SeqCst) && t.elapsed() < Duration::from_secs(60) {
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
            false
        })),
    );
}

async fn reserved_drains(s: &TestServer) {
    let b = s.app.imports.clone();
    wait_until("import reservations returned", Duration::from_secs(10), move || b.reserved() == 0).await;
}

/// 100 small imports (real sizes up to p90) held at once, all admitted,
/// while a 20,000-record import runs to completion beside them; none is
/// refused. (Fixed slots ran 4 at a time.)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn many_small_imports_run_beside_a_large_one() {
    let s = node(Some(96 * MIB), Duration::from_secs(30)).await;
    let accts = accounts(&s, 8_100_000, 101).await;
    let (held, go) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicBool::new(false)));
    let mut small = Vec::new();
    for (i, (did, tok)) in accts[..100].iter().enumerate() {
        hold(did, held.clone(), go.clone());
        let n = vlpds::real_dist::draw((i as f64 + 0.5) / 100.0 * 0.9, 0.5) as usize;
        let car = repo_car(i as u64, n);
        let (xrpc, tok) = (s.xrpc.clone(), tok.clone());
        small.push(tokio::spawn(async move { xrpc.post_bytes("com.atproto.repo.importRepo", car, "application/vnd.ipld.car", &Auth::Bearer(tok)).await }));
    }
    wait_until("100 imports held at once", Duration::from_secs(30), || held.load(Ordering::SeqCst) == 100).await;
    let peak = s.app.imports.reserved();
    assert!(peak >= 100 * vlpds::xrpc::import_budget::MIN_WORKING_SET, "{peak}");
    let (did, tok) = &accts[100];
    let r = import(&s, tok, repo_car(7, 20_000)).await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(s.get_repo(did).await.entries().len(), 20_000);
    go.store(true, Ordering::SeqCst);
    for (t, (did, _)) in small.into_iter().zip(&accts) {
        let r = t.await.unwrap();
        assert_eq!(r.status, 200, "{did}: {}", r.text());
        si::set_crash_hook(did, None);
    }
    reserved_drains(&s).await;
}

/// A raw HTTP/1.1 importRepo: `head` adds headers (the framing among them),
/// `body` is sent as given, then the write side closed if `close` (hyper
/// drops a connection half-closed before it answers).
async fn raw(s: &TestServer, token: &str, head: &str, body: &[u8], close: bool) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut c = tokio::net::TcpStream::connect(s.addr).await.unwrap();
    let req = format!(
        "POST /xrpc/com.atproto.repo.importRepo HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/vnd.ipld.car\r\nConnection: close\r\n{head}\r\n",
        s.addr
    );
    c.write_all(req.as_bytes()).await.unwrap();
    let _ = c.write_all(body).await;
    if close {
        let _ = c.shutdown().await;
    }
    let mut out = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(60), c.read_to_end(&mut out)).await;
    let text = String::from_utf8_lossy(&out).into_owned();
    let status = text.split(' ').nth(1).and_then(|c| c.parse().ok()).unwrap_or(0);
    (status, text)
}

fn chunked(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for c in body.chunks(16 << 10) {
        out.extend_from_slice(format!("{:x}\r\n", c.len()).as_bytes());
        out.extend_from_slice(c);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"0\r\n\r\n");
    out
}

/// No Content-Length: the reservation grows as the body arrives and the
/// import completes. A Content-Length shorter than the CAR (the body ends
/// there: a truncated CAR) and one longer than what is sent (the body ends
/// early) fail with a 400. Nothing stays reserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_and_wrong_lengths() {
    let s = node(None, Duration::from_secs(30)).await;
    let accts = accounts(&s, 8_200_000, 3).await;
    let car = repo_car(11, 6_000);
    let (st, text) = raw(&s, &accts[0].1, "Transfer-Encoding: chunked\r\n", &chunked(&car), false).await;
    assert_eq!(st, 200, "{text}");
    assert_eq!(s.get_repo(&accts[0].0).await.entries().len(), 6_000);

    let (st, text) = raw(&s, &accts[1].1, "Content-Length: 1000\r\n", &car[..1000], false).await;
    assert_eq!(st, 400, "{text}");
    let (st, text) = raw(&s, &accts[2].1, &format!("Content-Length: {}\r\n", car.len() + 1000), &car, true).await;
    assert_eq!(st, 400, "{text}");
    reserved_drains(&s).await;
}

/// A full budget: a new import waits its turn, then gets a retryable 503;
/// a running one that can't grow (no Content-Length) gets one too. Every
/// held import then completes, and the reservations return to 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn exhausted_budget_refuses_cleanly() {
    let min = vlpds::xrpc::import_budget::MIN_WORKING_SET;
    let s = node(Some(4 * min), Duration::from_millis(300)).await;
    let accts = accounts(&s, 8_300_000, 6).await;
    let held = Arc::new(AtomicUsize::new(0));
    let gos: Vec<Arc<AtomicBool>> = (0..4).map(|_| Arc::new(AtomicBool::new(false))).collect();
    let mut running = Vec::new();
    for (i, (did, tok)) in accts[..4].iter().enumerate() {
        hold(did, held.clone(), gos[i].clone());
        let (xrpc, tok, car) = (s.xrpc.clone(), tok.clone(), repo_car(20 + i as u64, 50));
        running.push(tokio::spawn(async move { xrpc.post_bytes("com.atproto.repo.importRepo", car, "application/vnd.ipld.car", &Auth::Bearer(tok)).await }));
    }
    wait_until("4 imports held", Duration::from_secs(10), || held.load(Ordering::SeqCst) == 4).await;
    assert_eq!(s.app.imports.reserved(), 4 * min);

    let t = Instant::now();
    let r = import(&s, &accts[4].1, repo_car(30, 50)).await;
    r.err(503, "Overloaded");
    assert!(t.elapsed() >= Duration::from_millis(300));

    // one finishes; an import without a Content-Length takes its place and
    // can't grow past it
    gos[0].store(true, Ordering::SeqCst);
    let first = running.remove(0).await.unwrap();
    assert_eq!(first.status, 200, "{}", first.text());
    let b = s.app.imports.clone();
    wait_until("one returned", Duration::from_secs(10), move || b.reserved() == 3 * min).await;
    let (st, text) = raw(&s, &accts[5].1, "Transfer-Encoding: chunked\r\n", &chunked(&repo_car(31, 3_000)), false).await;
    assert_eq!(st, 503, "{text}");
    assert!(text.contains("Overloaded"), "{text}");
    let b = s.app.imports.clone();
    wait_until("the refused one returned", Duration::from_secs(10), move || b.reserved() == 3 * min).await;

    for g in &gos {
        g.store(true, Ordering::SeqCst);
    }
    for t in running {
        let r = t.await.unwrap();
        assert_eq!(r.status, 200, "{}", r.text());
    }
    for (did, _) in &accts[..4] {
        si::set_crash_hook(did, None);
    }
    reserved_drains(&s).await;
    // and the budget admits again
    let r = import(&s, &accts[4].1, repo_car(30, 50)).await;
    assert_eq!(r.status, 200, "{}", r.text());
    reserved_drains(&s).await;
}
