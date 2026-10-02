//! app.bsky.notification.{registerPush,unregisterPush} (reference
//! api/app/bsky/notification/*.ts): forwarded with a service-auth JWT
//! (iss = account, aud = serviceDid, lxm = method) to the configured AppView
//! when serviceDid is its DID, else to the `#bsky_notif` endpoint of
//! serviceDid's DID document. Stub services record what they receive.

use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use k256::ecdsa::signature::Verifier;
use parking_lot::Mutex;
use serde_json::{Value as J, json};
use std::sync::Arc;

const APPVIEW_DID: &str = "did:web:appview.test";
const REGISTER: &str = "app.bsky.notification.registerPush";
const UNREGISTER: &str = "app.bsky.notification.unregisterPush";

#[derive(Clone)]
struct Seen {
    uri: String,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Clone, Default)]
struct Stub {
    seen: Arc<Mutex<Vec<Seen>>>,
    did: Arc<Mutex<String>>,
    base: Arc<Mutex<String>>,
    /// `type` of the `#bsky_notif` service in this stub's DID document.
    notif_type: Arc<Mutex<String>>,
}

impl Stub {
    fn take(&self) -> Vec<Seen> {
        std::mem::take(&mut *self.seen.lock())
    }
    fn did(&self) -> String {
        self.did.lock().clone()
    }
}

async fn stub_handler(State(s): State<Stub>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let body = axum::body::to_bytes(body, 1 << 20).await.unwrap();
    if parts.uri.path() == "/.well-known/did.json" {
        return axum::Json(json!({
            "id": s.did(),
            "service": [{"id": "#bsky_notif", "type": s.notif_type.lock().clone(), "serviceEndpoint": s.base.lock().clone()}],
        }))
        .into_response();
    }
    s.seen.lock().push(Seen { uri: parts.uri.to_string(), headers: parts.headers, body });
    axum::http::StatusCode::OK.into_response()
}

async fn spawn_stub() -> Stub {
    let stub = Stub::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    *stub.base.lock() = format!("http://{addr}");
    *stub.did.lock() = format!("did:web:127.0.0.1%3A{}", addr.port());
    *stub.notif_type.lock() = "BskyNotificationService".into();
    let router = axum::Router::new().fallback(stub_handler).with_state(stub.clone());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    stub
}

struct Env {
    url: String,
    http: reqwest::Client,
    appview: Stub,
    push: Stub,
}

struct User {
    did: String,
    jwt: String,
}

async fn spawn_env(with_appview: bool) -> Env {
    let appview = spawn_stub().await;
    let push = spawn_stub().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = vlpds::server::Config {
        public_url: format!("http://{addr}"),
        appview: with_appview.then(|| (appview.base.lock().clone(), APPVIEW_DID.into())),
        // dev mode: did:web over http on loopback (the stub's DID document)
        dev_mode: true,
        plc_url: "http://127.0.0.1:1".into(),
        ..Default::default()
    };
    vlpds::server::spawn(cfg, listener, None).await.unwrap();
    Env { url: format!("http://{addr}"), http: reqwest::Client::new(), appview, push }
}

impl Env {
    async fn create_account(&self, name: &str) -> User {
        let v: J = self
            .http
            .post(format!("{}/xrpc/com.atproto.server.createAccount", self.url))
            .json(&json!({"handle": format!("{name}.vlpds.test"), "password": "hunter22", "email": format!("{name}@example.com")}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        User { did: v["did"].as_str().expect("did").into(), jwt: v["accessJwt"].as_str().unwrap().into() }
    }

    async fn call(&self, user: Option<&User>, nsid: &str, body: &J) -> (u16, J) {
        let mut rb = self.http.post(format!("{}/xrpc/{nsid}", self.url)).json(body);
        if let Some(u) = user {
            rb = rb.bearer_auth(&u.jwt);
        }
        let r = rb.send().await.unwrap();
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(J::Null))
    }

    async fn signing_key(&self, did: &str) -> k256::ecdsa::VerifyingKey {
        let v: J = self.http.get(format!("{}/xrpc/com.atproto.repo.describeRepo?repo={did}", self.url)).send().await.unwrap().json().await.unwrap();
        let mb = v["didDoc"]["verificationMethod"][0]["publicKeyMultibase"].as_str().unwrap();
        let raw = bs58::decode(mb.strip_prefix('z').unwrap()).into_vec().unwrap();
        k256::ecdsa::VerifyingKey::from_sec1_bytes(&raw[2..]).unwrap()
    }
}

/// Verifies the service-auth JWT against the account key; returns claims.
fn claims(headers: &HeaderMap, key: &k256::ecdsa::VerifyingKey) -> J {
    let auth = headers.get("authorization").expect("authorization").to_str().unwrap();
    let tok = auth.strip_prefix("Bearer ").expect("bearer");
    let (input, sig) = tok.rsplit_once('.').unwrap();
    let (h, p) = input.split_once('.').unwrap();
    let header: J = serde_json::from_slice(&B64.decode(h).unwrap()).unwrap();
    assert_eq!(header["alg"], "ES256K");
    let sig = k256::ecdsa::Signature::from_slice(&B64.decode(sig).unwrap()).unwrap();
    key.verify(input.as_bytes(), &sig).expect("JWT signed with the account's repo key");
    serde_json::from_slice(&B64.decode(p).unwrap()).unwrap()
}

fn input(service_did: &str) -> J {
    json!({"serviceDid": service_did, "token": "device-token-1", "platform": "ios", "appId": "xyz.blueskyweb.app"})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn appview_service_did_goes_to_the_configured_appview() {
    let env = spawn_env(true).await;
    let u = env.create_account("ana").await;
    let key = env.signing_key(&u.did).await;
    for lxm in [REGISTER, UNREGISTER] {
        let (st, body) = env.call(Some(&u), lxm, &input(APPVIEW_DID)).await;
        assert_eq!(st, 200, "{lxm}: {body}");
        let seen = env.appview.take();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].uri, format!("/xrpc/{lxm}"));
        assert_eq!(serde_json::from_slice::<J>(&seen[0].body).unwrap(), input(APPVIEW_DID));
        let c = claims(&seen[0].headers, &key);
        assert_eq!(c["iss"], json!(u.did));
        assert_eq!(c["aud"], json!(APPVIEW_DID));
        assert_eq!(c["lxm"], json!(lxm));
    }
    assert!(env.push.take().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn other_service_did_resolves_its_bsky_notif_endpoint() {
    let env = spawn_env(true).await;
    let u = env.create_account("ben").await;
    let key = env.signing_key(&u.did).await;
    let push_did = env.push.did();
    for lxm in [REGISTER, UNREGISTER] {
        let (st, body) = env.call(Some(&u), lxm, &input(&push_did)).await;
        assert_eq!(st, 200, "{lxm}: {body}");
        let seen = env.push.take();
        assert_eq!(seen.len(), 1, "{lxm}");
        assert_eq!(seen[0].uri, format!("/xrpc/{lxm}"));
        let c = claims(&seen[0].headers, &key);
        assert_eq!(c["iss"], json!(u.did));
        assert_eq!(c["aud"], json!(push_did));
        assert_eq!(c["lxm"], json!(lxm));
    }
    assert!(env.appview.take().is_empty());

    // a DID document without a BskyNotificationService is refused
    let other = spawn_stub().await;
    *other.notif_type.lock() = "SomethingElse".into();
    let (st, body) = env.call(Some(&u), REGISTER, &input(&other.did())).await;
    assert_eq!(st, 400, "{body}");
    assert_eq!(body["error"], "InvalidRequest");
    assert!(body["message"].as_str().unwrap().contains("invalid notification service details"), "{body}");
    assert!(other.take().is_empty());
    // an unresolvable DID
    let (st, body) = env.call(Some(&u), REGISTER, &input("did:web:127.0.0.1%3A1")).await;
    assert_eq!(st, 400, "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auth_and_input_are_checked() {
    let env = spawn_env(true).await;
    let u = env.create_account("cyd").await;
    let (st, _) = env.call(None, REGISTER, &input(APPVIEW_DID)).await;
    assert_eq!(st, 401);
    let mut bad = input(APPVIEW_DID);
    bad["platform"] = json!("windows");
    assert_eq!(env.call(Some(&u), REGISTER, &bad).await.0, 400);
    let mut bad = input(APPVIEW_DID);
    bad.as_object_mut().unwrap().remove("serviceDid");
    assert_eq!(env.call(Some(&u), REGISTER, &bad).await.0, 400);
    assert!(env.appview.take().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_an_appview_there_is_no_push_service() {
    let env = spawn_env(false).await;
    let u = env.create_account("dee").await;
    let (st, body) = env.call(Some(&u), REGISTER, &input(&env.push.did())).await;
    assert_eq!(st, 400, "{body}");
    assert!(env.push.take().is_empty());
}
