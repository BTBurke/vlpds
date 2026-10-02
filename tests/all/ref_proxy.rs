//! Ported from the reference PDS's tests/proxied/*: proxy-header,
//! proxy-catchall, decompression-bound and the encoding cases of
//! read-after-write (see tests/REFERENCE_COVERAGE.md). The upstream is a
//! did:web service on loopback (dev mode allows plain http there) that is
//! also configured as the AppView, so read-after-write applies.

use crate::common::*;
use axum::body::Body;
use axum::extract::Request;
use axum::response::Response;
use parking_lot::Mutex;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

const APPVIEW_DID: &str = "did:web:appview.test";
/// Over the proxy's 10 MiB response cap.
const BOMB_SIZE: usize = (10 << 20) + (1 << 20);

#[derive(Default)]
struct Upstream {
    /// did:web of this service.
    did: String,
    /// (path?query) of every xrpc request.
    seen: Mutex<Vec<String>>,
    /// getProfile: status, gzip?, atproto-repo-rev.
    bomb: Mutex<(u16, bool, Option<String>)>,
    /// getTimeline: atproto-repo-rev and body.
    timeline: Mutex<(Option<String>, J)>,
}

fn gzip(b: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(b).unwrap();
    e.finish().unwrap()
}

fn json_resp(status: u16) -> axum::http::response::Builder {
    Response::builder().status(status).header("content-type", "application/json")
}

async fn handle(up: Arc<Upstream>, base: String, req: Request) -> Response {
    let path = req.uri().path().to_string();
    if path == "/.well-known/did.json" {
        let doc = json!({
            "id": up.did,
            "service": [
                {"id": "#atproto_test", "type": "TestAtprotoService", "serviceEndpoint": base},
                {"id": "#dead", "type": "TestAtprotoService", "serviceEndpoint": "http://127.0.0.1:1"},
            ],
        });
        return Response::builder()
            .header("content-type", "application/json")
            .body(Body::from(doc.to_string()))
            .unwrap();
    }
    up.seen.lock().push(req.uri().to_string());
    let ok = |s: &'static str| json_resp(200).body(Body::from(s)).unwrap();
    match path.as_str() {
        "/xrpc/com.example.ok" => ok(r#"{"foo":"ok"}"#),
        "/xrpc/com.example.error" => json_resp(500)
            .body(Body::from(r#"{"error":"FooBar","message":"My message"}"#))
            .unwrap(),
        // headers after 50 ms, then the body a byte every 10 ms
        "/xrpc/com.example.slow" => {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let s = futures::stream::unfold(0usize, |i| async move {
                let body = br#"{"foo":"slow"}"#;
                if i == body.len() {
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                Some((Ok::<_, std::io::Error>(bytes::Bytes::copy_from_slice(&body[i..i + 1])), i + 1))
            });
            json_resp(200).body(Body::from_stream(s)).unwrap()
        }
        // part of a body, then the connection breaks
        "/xrpc/com.example.abort" => {
            let s = futures::stream::unfold(0, |i| async move {
                match i {
                    0 => Some((Ok(bytes::Bytes::from_static(br#"{"foo""#)), 1)),
                    1 => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Some((Err(std::io::Error::other("abort")), 2))
                    }
                    _ => None,
                }
            });
            json_resp(200).body(Body::from_stream(s)).unwrap()
        }
        // a manual pipethrough (the reference's getPreferences) that fails
        "/xrpc/app.bsky.actor.getPreferences" => Response::builder().status(501).body(Body::empty()).unwrap(),
        "/xrpc/app.bsky.actor.getProfile" => {
            let (status, gz, rev) = up.bomb.lock().clone();
            let raw = serde_json::to_vec(&json!({"error": "Bomb", "message": "A".repeat(BOMB_SIZE)})).unwrap();
            let mut b = json_resp(status);
            if let Some(rev) = rev {
                b = b.header("atproto-repo-rev", rev);
            }
            if gz {
                b.header("content-encoding", "gzip").body(Body::from(gzip(&raw))).unwrap()
            } else {
                b.body(Body::from(raw)).unwrap()
            }
        }
        "/xrpc/app.bsky.feed.getTimeline" => {
            let (rev, body) = up.timeline.lock().clone();
            let mut b = json_resp(200);
            if let Some(rev) = rev {
                b = b.header("atproto-repo-rev", rev);
            }
            b.body(Body::from(body.to_string())).unwrap()
        }
        _ => ok("{}"),
    }
}

/// An upstream service, plus a PDS that has it as its AppView.
async fn setup() -> (Arc<Upstream>, TestServer) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let base = format!("http://{addr}");
    let up = Arc::new(Upstream { did: format!("did:web:127.0.0.1%3A{}", addr.port()), ..Default::default() });
    let (u, b) = (up.clone(), base.clone());
    let router = axum::Router::new().fallback(move |req: Request| handle(u.clone(), b.clone(), req));
    tokio::spawn(async move { axum::serve(l, router).await.unwrap() });
    let s = TestServer::spawn_with(|c| c.appview = Some((base, APPVIEW_DID.into()))).await;
    (up, s)
}

impl Upstream {
    fn proxy(&self) -> String {
        format!("{}#atproto_test", self.did)
    }
    fn count(&self) -> usize {
        self.seen.lock().len()
    }
}

fn get(s: &TestServer, a: &TestAccount, nsid_and_query: &str) -> reqwest::RequestBuilder {
    s.xrpc
        .http
        .get(format!("{}/xrpc/{nsid_and_query}", s.url))
        .header("authorization", format!("Bearer {}", a.access))
}

async fn send(s: &TestServer, rb: reqwest::RequestBuilder) -> Resp {
    s.xrpc.send(rb).await
}

/// reference proxy-header.test.ts "parses proxy header", "fails on a
/// non-existant did", "fails when a service is not specified", "fails on a
/// non-existant service": the error messages, and nothing reaches the
/// upstream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_proxy_header_errors() {
    let (up, s) = setup().await;
    let a = s.create_account("rph").await;
    let did = up.did.clone();
    let cases = [
        ("#atproto_test".to_string(), "no did specified in proxy header"),
        (did.clone(), "no service id specified in proxy header"),
        (format!("{did}#"), "no service id specified in proxy header"),
        (format!("{did}#atproto_test#foo"), "invalid proxy header format"),
        // HTTP strips leading/trailing whitespace from header values, so the
        // reference's " did#svc" / "did#svc " (a direct parseProxyHeader call)
        // become inner spaces on the wire
        (format!("{did}#atproto test"), "proxy header cannot contain spaces"),
        (format!("{did} #atproto_test"), "proxy header cannot contain spaces"),
        ("did:plc:12345678123456781234578#atproto_test".to_string(), "could not resolve proxy did"),
        (format!("{did}#atproto_bad"), "could not resolve proxy did service url"),
        // The reference's DID resolver throws PoorlyFormattedDidError /
        // UnsupportedDidMethodError here, which xrpc-server turns into a 500
        // InternalServerError; vlpds answers 400 like any unresolvable DID.
        ("did:foo#bar".to_string(), "could not resolve proxy did"),
        ("did:foo:bar#baz".to_string(), "could not resolve proxy did"),
        ("foo#bar".to_string(), "could not resolve proxy did"),
    ];
    for (header, msg) in cases {
        let r = send(&s, get(&s, &a, &format!("app.bsky.actor.getProfile?actor={}", a.did)).header("atproto-proxy", &header)).await;
        assert_eq!((r.status, r.error_name(), r.json["message"].as_str()), (400, Some("InvalidRequest"), Some(msg)), "{header:?}");
    }
    assert_eq!(up.count(), 0, "no request reached the upstream");

    // reference proxy-header.test.ts "handles failing manual pipethroughs"
    let r = send(&s, get(&s, &a, "app.bsky.actor.getPreferences").header("atproto-proxy", up.proxy())).await;
    assert_eq!(r.status, 501, "manual pipethroughs relay the upstream failure: {r:?}");
    assert_eq!(up.count(), 1);
}

/// reference proxy-catchall.test.ts "rejects when upstream unavailable".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_proxy_upstream_unavailable() {
    let (up, s) = setup().await;
    let a = s.create_account("rpdead").await;
    let r = send(&s, get(&s, &a, "com.example.ok").header("atproto-proxy", format!("{}#dead", up.did))).await;
    assert_eq!(
        (r.status, r.error_name(), r.json["message"].as_str()),
        (502, Some("UpstreamFailure"), Some("Upstream service unreachable")),
        "{r:?}"
    );
}

/// reference proxy-catchall.test.ts "successfully proxies requests",
/// "handles failing upstream requests" (500 -> 502 with the upstream's error).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_proxy_catchall_ok_and_error() {
    let (up, s) = setup().await;
    let a = s.create_account("rpok").await;
    let r = send(&s, get(&s, &a, "com.example.ok").header("atproto-proxy", up.proxy())).await;
    assert_eq!(r.ok(), json!({"foo": "ok"}));
    let r = send(&s, get(&s, &a, "com.example.error").header("atproto-proxy", up.proxy())).await;
    assert_eq!((r.status, r.error_name(), r.json["message"].as_str()), (502, Some("FooBar"), Some("My message")));
}

/// reference proxy-catchall.test.ts "handles cancelled upstream requests":
/// an upstream that breaks off mid-body is not turned into a complete
/// response.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_proxy_cancelled_upstream() {
    let (up, s) = setup().await;
    let a = s.create_account("rpabort").await;
    let r = get(&s, &a, "com.example.abort").header("atproto-proxy", up.proxy()).send().await.unwrap();
    // the head was already relayed; the body must fail (or at least never
    // parse as the complete document)
    match r.bytes().await {
        Err(_) => {}
        Ok(b) => assert!(serde_json::from_slice::<J>(&b).is_err(), "truncated body delivered as complete: {b:?}"),
    }
}

/// reference proxy-catchall.test.ts "handles cancelled downstream requests":
/// a client that gives up mid-request doesn't break the next request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_proxy_cancelled_downstream() {
    let (up, s) = setup().await;
    let a = s.create_account("rpslow").await;
    let r = get(&s, &a, "com.example.slow").header("atproto-proxy", up.proxy()).timeout(Duration::from_millis(20)).send().await;
    assert!(r.is_err() || r.unwrap().bytes().await.is_err(), "the request should have timed out");
    let r = send(&s, get(&s, &a, "com.example.slow").header("atproto-proxy", up.proxy())).await;
    assert_eq!(r.ok(), json!({"foo": "slow"}));
}

/// A rev strictly between two local writes (decompression-bound.test.ts
/// `sinceRev`): read-after-write has a record to splice in.
async fn rev_between_posts(s: &TestServer, a: &TestAccount) -> String {
    s.post(a, "hello").await;
    let rev = s.latest_commit(&a.did).await.1;
    s.post(a, "world").await;
    rev
}

/// reference decompression-bound.test.ts "stops parsing an oversized decoded
/// error body": the upstream's error name in a body over the cap never
/// reaches the client (gzip bomb, and an uncompressed one).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_proxy_oversized_error_body_is_not_parsed() {
    let (up, s) = setup().await;
    let a = s.create_account("rpbomb").await;
    for gz in [true, false] {
        *up.bomb.lock() = (418, gz, None);
        let r = send(&s, get(&s, &a, "app.bsky.actor.getProfile?actor=x").header("atproto-proxy", up.proxy())).await;
        assert_eq!(r.status, 418, "gzip {gz}");
        assert_ne!(r.error_name(), Some("Bomb"), "gzip {gz}");
        assert!(r.body.len() < 1 << 20, "the bomb was not relayed");
    }
}

/// reference decompression-bound.test.ts "rejects an oversized decoded
/// read-after-write body" / "... that is not compressed".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_proxy_oversized_read_after_write_body() {
    let (up, s) = setup().await;
    let a = s.create_account("rprawbomb").await;
    let since = rev_between_posts(&s, &a).await;
    *up.bomb.lock() = (200, true, Some(since.clone()));
    let r = send(&s, get(&s, &a, "app.bsky.actor.getProfile?actor=x").header("atproto-proxy", up.proxy())).await;
    assert_eq!(r.status, 502, "{r:?}");
    assert_eq!(r.json["message"], "upstream response too large");
    *up.bomb.lock() = (200, false, Some(since));
    let r = send(&s, get(&s, &a, "app.bsky.actor.getProfile?actor=x").header("atproto-proxy", up.proxy())).await;
    assert_eq!(r.status, 502, "{}", r.status);
    assert!(r.body.len() < 1 << 20);
}

/// A timeline the AppView has indexed up to `since`, padded over the 1 KiB
/// compression threshold.
async fn timeline_setup(prefix: &str) -> (Arc<Upstream>, TestServer, TestAccount) {
    let (up, s) = setup().await;
    let a = s.create_account(prefix).await;
    let since = rev_between_posts(&s, &a).await;
    *up.timeline.lock() = (Some(since), json!({"feed": [], "cursor": "c2", "startCursor": "s1", "pad": "x".repeat(4000)}));
    (up, s, a)
}

/// reference read-after-write.test.ts "negotiates encoding", "defaults to
/// identity encoding", "falls back to identity encoding", "errors when
/// failing to negotiate encoding", "errors on invalid content-encoding
/// format".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_read_after_write_encoding_negotiation() {
    let (_up, s, a) = timeline_setup("rpenc").await;
    let tl = |ae: Option<&str>| {
        let mut rb = get(&s, &a, "app.bsky.feed.getTimeline");
        if let Some(ae) = ae {
            rb = rb.header("accept-encoding", ae);
        }
        send(&s, rb)
    };
    for ae in [Some("identity"), None, Some("invalid")] {
        let r = tl(ae).await;
        assert_eq!(r.status, 200, "{ae:?}: {r:?}");
        assert!(r.header("atproto-upstream-lag").is_some(), "munged");
        assert_eq!(r.header("content-encoding"), None, "{ae:?}");
    }
    let r = tl(Some("gzip, *;q=0")).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-encoding").as_deref(), Some("gzip"));

    let r = tl(Some("invalid, *;q=0")).await;
    assert_eq!(
        (r.status, r.json["message"].as_str()),
        (406, Some("this service does not support any of the requested encodings"))
    );
    let r = tl(Some(";q=1")).await;
    assert_eq!((r.status, r.json["message"].as_str()), (400, Some("Invalid accept-encoding: \";q=1\"")));
}

/// reference read-after-write.test.ts "passes the appview cursors through
/// the timeline munge", "forwards since to the appview through the timeline
/// munge".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_read_after_write_timeline_cursors_and_since() {
    let (up, s, a) = timeline_setup("rpcur").await;
    let r = send(&s, get(&s, &a, "app.bsky.feed.getTimeline?limit=2&since=s0")).await;
    let j = r.ok();
    assert!(r.header("atproto-upstream-lag").is_some(), "munged");
    assert_eq!(j["feed"][0]["post"]["record"]["text"], "world");
    assert_eq!((j["cursor"].as_str(), j["startCursor"].as_str()), (Some("c2"), Some("s1")));
    let last = up.seen.lock().last().cloned().unwrap();
    assert_eq!(last, "/xrpc/app.bsky.feed.getTimeline?limit=2&since=s0");
}
