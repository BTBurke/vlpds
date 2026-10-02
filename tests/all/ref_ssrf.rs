//! Port of the reference PDS's ssrf.test.ts: registerPush, unregisterPush
//! and an atproto-proxy'd createReport go to endpoints taken from a DID
//! document. Outside dev mode (vlpds's SSRF policy) a loopback endpoint is
//! refused before anything is sent; in dev mode (the differential control:
//! same DID, same calls) the stub receives them. The service DID is a
//! did:plc in the mock directory, which both modes resolve (the operator's
//! PLC URL is trusted), so the refusal can only come from the endpoint.

use crate::common::*;
use axum::extract::{Request, State};
use axum::response::IntoResponse;
use parking_lot::Mutex;
use std::sync::Arc;
use vlpds::plc::mock::MockPlc;

#[derive(Clone, Default)]
struct Upstream {
    seen: Arc<Mutex<Vec<String>>>,
}

async fn upstream_handler(State(u): State<Upstream>, req: Request) -> axum::response::Response {
    let path = req.uri().path().to_string();
    u.seen.lock().push(format!("{} {path}", req.method()));
    let body = if path.ends_with("createReport") {
        json!({
            "id": 1,
            "reasonType": "com.atproto.moderation.defs#reasonSpam",
            "subject": {"$type": "com.atproto.admin.defs#repoRef", "did": "did:plc:abcdefghijklmnopqrstuvwx"},
            "reportedBy": "did:plc:abcdefghijklmnopqrstuvwx",
            "createdAt": now_iso(),
        })
    } else {
        json!({})
    };
    axum::Json(body).into_response()
}

async fn spawn_upstream() -> (Upstream, String) {
    let u = Upstream::default();
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://localhost:{}", l.local_addr().unwrap().port());
    let router = axum::Router::new().fallback(upstream_handler).with_state(u.clone());
    tokio::spawn(async move { axum::serve(l, router).await.unwrap() });
    (u, endpoint)
}

/// Registers a did:plc whose `#bsky_notif` and `#atproto_labeler` services
/// point at `endpoint`.
async fn service_did(plc: &MockPlc, endpoint: &str) -> String {
    let key = vlpds::crypto::Keypair::generate();
    let op = json!({
        "type": "plc_operation",
        "rotationKeys": [key.did_key()],
        "verificationMethods": {"atproto": key.did_key(), "atproto_label": key.did_key()},
        "alsoKnownAs": ["at://notifsvc.example.com"],
        "services": {
            "bsky_notif": {"type": "BskyNotificationService", "endpoint": endpoint},
            "atproto_labeler": {"type": "AtprotoLabeler", "endpoint": endpoint},
        },
        "prev": null,
    });
    let op = vlpds::plc::sign(op, &key).unwrap();
    let did = vlpds::plc::did_for_genesis(&op).unwrap();
    let r = reqwest::Client::new().post(format!("{}/{did}", plc.url)).json(&op).send().await.unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    did
}

struct Ctx {
    s: TestServer,
    reporter: TestAccount,
    service: String,
}

async fn setup(ssrf_protection: bool, endpoint: &str) -> Ctx {
    let plc = MockPlc::start().await;
    let service = service_did(&plc, endpoint).await;
    let url = plc.url.clone();
    let s = TestServer::spawn_with(move |c| {
        // vlpds ties its SSRF policy to dev mode
        c.dev_mode = !ssrf_protection;
        c.plc_url = url;
        // push needs an AppView configured (unused here: never contacted)
        c.appview = Some(("http://127.0.0.1:1".into(), "did:web:appview.test".into()));
    })
    .await;
    let reporter = s.create_account("reporter").await;
    Ctx { s, reporter, service }
}

impl Ctx {
    async fn push(&self, nsid: &str) -> Resp {
        let body = json!({"serviceDid": self.service, "token": "tok1", "platform": "web", "appId": "app1"});
        self.s.xrpc.post(nsid, &body, &self.reporter.auth()).await
    }

    async fn create_report(&self) -> Resp {
        let body = json!({
            "reasonType": "com.atproto.moderation.defs#reasonSpam",
            "reason": "ssrf probe",
            "subject": {"$type": "com.atproto.admin.defs#repoRef", "did": self.reporter.did},
        });
        let rb = self
            .s
            .xrpc
            .http
            .post(format!("{}/xrpc/com.atproto.moderation.createReport", self.s.url))
            .bearer_auth(&self.reporter.access)
            .header("atproto-proxy", format!("{}#atproto_labeler", self.service))
            .json(&body);
        self.s.xrpc.send(rb).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_with_ssrf_protection_enabled_nothing_is_sent() {
    let (up, endpoint) = spawn_upstream().await;
    let ctx = setup(true, &endpoint).await;
    // refuses to send registerPush to a non-unicast endpoint
    let r = ctx.push("app.bsky.notification.registerPush").await;
    assert!(!r.is_ok(), "{}", r.text());
    // refuses to send unregisterPush to a non-unicast endpoint
    let r = ctx.push("app.bsky.notification.unregisterPush").await;
    assert!(!r.is_ok(), "{}", r.text());
    // refuses to send createReport to a non-unicast endpoint
    let r = ctx.create_report().await;
    assert!(!r.is_ok(), "{}", r.text());
    assert_eq!(*up.seen.lock(), Vec::<String>::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_with_ssrf_protection_disabled_calls_are_sent() {
    let (up, endpoint) = spawn_upstream().await;
    let ctx = setup(false, &endpoint).await;
    // sends registerPush to the endpoint
    ctx.push("app.bsky.notification.registerPush").await.ok();
    assert_eq!(std::mem::take(&mut *up.seen.lock()), ["POST /xrpc/app.bsky.notification.registerPush"]);
    // sends unregisterPush to the endpoint
    ctx.push("app.bsky.notification.unregisterPush").await.ok();
    assert_eq!(std::mem::take(&mut *up.seen.lock()), ["POST /xrpc/app.bsky.notification.unregisterPush"]);
    // sends createReport to the endpoint
    let r = ctx.create_report().await;
    assert!(r.is_ok(), "{}", r.text());
    assert_eq!(std::mem::take(&mut *up.seen.lock()), ["POST /xrpc/com.atproto.moderation.createReport"]);
}
