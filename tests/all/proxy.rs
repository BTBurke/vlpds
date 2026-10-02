//! Service proxying (atproto-proxy / default AppView), preferences and
//! createReport, against a fake AppView that verifies the service-auth JWTs.

use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use k256::ecdsa::signature::Verifier;
use parking_lot::Mutex;
use serde_json::{Value as J, json};
use std::sync::Arc;
use vlpds::server::{self, Config};

const APPVIEW_DID: &str = "did:web:appview.test";
const REPORT_DID: &str = "did:web:mod.test";

#[derive(Clone, Debug)]
struct Seen {
    method: String,
    uri: String,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Clone, Default)]
struct Fake {
    seen: Arc<Mutex<Vec<Seen>>>,
    /// did:web of this fake (served at /.well-known/did.json).
    did: Arc<Mutex<String>>,
    base: Arc<Mutex<String>>,
}

impl Fake {
    fn last(&self) -> Seen {
        self.seen.lock().last().cloned().expect("fake upstream saw no request")
    }
    fn count(&self) -> usize {
        self.seen.lock().len()
    }
}

async fn fake_handler(State(f): State<Fake>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let body = axum::body::to_bytes(body, 1 << 20).await.unwrap();
    let path = parts.uri.path().to_string();
    if path == "/.well-known/did.json" {
        let did = f.did.lock().clone();
        let base = f.base.lock().clone();
        return axum::Json(json!({
            "id": did,
            "service": [{"id": "#other_svc", "type": "Other", "serviceEndpoint": base}],
        }))
        .into_response();
    }
    f.seen.lock().push(Seen { method: parts.method.to_string(), uri: parts.uri.to_string(), headers: parts.headers.clone(), body: body.clone() });
    let json_err = |status: u16, v: J| (StatusCode::from_u16(status).unwrap(), axum::Json(v)).into_response();
    match path.as_str() {
        "/xrpc/app.bsky.test.err400" => {
            let mut r = json_err(400, json!({"error": "CustomErr", "message": "boom"}));
            r.headers_mut().insert("retry-after", "7".parse().unwrap());
            r.headers_mut().insert("x-internal", "nope".parse().unwrap());
            r
        }
        "/xrpc/app.bsky.test.err500" => json_err(500, json!({"error": "Oops", "message": "upstream broke"})),
        "/xrpc/app.bsky.test.err404plain" => (StatusCode::NOT_FOUND, "not here").into_response(),
        "/xrpc/app.bsky.test.err400gzip" => {
            use std::io::Write;
            let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            e.write_all(br#"{"error":"CustomGz","message":"zipped boom"}"#).unwrap();
            let gz = e.finish().unwrap();
            (StatusCode::BAD_REQUEST, [("content-type", "application/json"), ("content-encoding", "gzip")], gz).into_response()
        }
        "/xrpc/app.bsky.test.echo" => {
            let ct = parts.headers.get("content-type").cloned().unwrap_or("application/octet-stream".parse().unwrap());
            ([("content-type", ct)], body).into_response()
        }
        "/xrpc/com.atproto.moderation.createReport" => {
            let mut v: J = serde_json::from_slice(&body).unwrap();
            v["id"] = json!(42);
            v["reportedBy"] = json!("did:example:x");
            v["createdAt"] = json!("2026-01-01T00:00:00Z");
            axum::Json(v).into_response()
        }
        _ => {
            let mut r = axum::Json(json!({"feed": [], "path": path})).into_response();
            r.headers_mut().insert("atproto-repo-rev", "3abc".parse().unwrap());
            r.headers_mut().insert("content-language", "en".parse().unwrap());
            r.headers_mut().insert("x-secret", "leak".parse().unwrap());
            r.headers_mut().insert("set-cookie", "a=b".parse().unwrap());
            r
        }
    }
}

async fn spawn_fake() -> (Fake, String) {
    let fake = Fake::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    *fake.base.lock() = base.clone();
    *fake.did.lock() = format!("did:web:127.0.0.1%3A{}", addr.port());
    let router = axum::Router::new().fallback(fake_handler).with_state(fake.clone());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (fake, base)
}

struct Env {
    url: String,
    http: reqwest::Client,
    appview: Fake,
    reports: Fake,
}

async fn spawn_env(dev_mode: bool) -> Env {
    let (appview, av_url) = spawn_fake().await;
    let (reports, rep_url) = spawn_fake().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = Config {
        public_url: format!("http://{addr}"),
        appview: Some((av_url, APPVIEW_DID.into())),
        report_service: Some((rep_url, REPORT_DID.into())),
        dev_mode,
        plc_url: "http://127.0.0.1:1".into(),
        ..Default::default()
    };
    server::spawn(cfg, listener, None).await.unwrap();
    Env { url: format!("http://{addr}"), http: reqwest::Client::new(), appview, reports }
}

struct User {
    did: String,
    jwt: String,
}

impl Env {
    async fn create_account(&self, name: &str) -> User {
        let r = self
            .http
            .post(format!("{}/xrpc/com.atproto.server.createAccount", self.url))
            .json(&json!({"handle": format!("{name}.vlpds.test"), "password": "hunter22", "email": format!("{name}@example.com")}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "createAccount: {}", r.text().await.unwrap());
        let v: J = r.json().await.unwrap();
        User { did: v["did"].as_str().unwrap().into(), jwt: v["accessJwt"].as_str().unwrap().into() }
    }

    fn get(&self, user: Option<&User>, nsid_and_query: &str) -> reqwest::RequestBuilder {
        let rb = self.http.get(format!("{}/xrpc/{nsid_and_query}", self.url));
        match user {
            Some(u) => rb.bearer_auth(&u.jwt),
            None => rb,
        }
    }

    fn post(&self, user: &User, nsid: &str) -> reqwest::RequestBuilder {
        self.http.post(format!("{}/xrpc/{nsid}", self.url)).bearer_auth(&user.jwt)
    }

    /// The account's atproto verification key, from describeRepo's didDoc.
    async fn signing_key(&self, did: &str) -> k256::ecdsa::VerifyingKey {
        let v: J = self.get(None, &format!("com.atproto.repo.describeRepo?repo={did}")).send().await.unwrap().json().await.unwrap();
        let mb = v["didDoc"]["verificationMethod"][0]["publicKeyMultibase"].as_str().unwrap();
        let raw = bs58::decode(mb.strip_prefix('z').unwrap()).into_vec().unwrap();
        assert_eq!(&raw[..2], &[0xe7, 0x01], "secp256k1-pub multicodec");
        k256::ecdsa::VerifyingKey::from_sec1_bytes(&raw[2..]).unwrap()
    }
}

/// Verifies the forwarded service-auth JWT and returns its claims.
fn verify_service_jwt(headers: &HeaderMap, key: &k256::ecdsa::VerifyingKey) -> J {
    let auth = headers.get("authorization").expect("authorization forwarded").to_str().unwrap();
    let tok = auth.strip_prefix("Bearer ").expect("bearer token");
    let (signing_input, sig) = tok.rsplit_once('.').unwrap();
    let (h, p) = signing_input.split_once('.').unwrap();
    let header: J = serde_json::from_slice(&B64.decode(h).unwrap()).unwrap();
    assert_eq!(header["alg"], "ES256K");
    let sig = k256::ecdsa::Signature::from_slice(&B64.decode(sig).unwrap()).unwrap();
    assert!(sig.normalize_s().is_none(), "signature must be low-S");
    key.verify(signing_input.as_bytes(), &sig).expect("service JWT signature verifies with the repo key");
    let claims: J = serde_json::from_slice(&B64.decode(p).unwrap()).unwrap();
    let now = chrono::Utc::now().timestamp();
    let exp = claims["exp"].as_i64().unwrap();
    assert!(exp > now && exp <= now + 300, "short-lived token");
    claims
}

async fn err_of(r: reqwest::Response) -> (u16, J) {
    let status = r.status().as_u16();
    (status, r.json().await.unwrap_or(J::Null))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxies_to_default_appview_with_service_auth_and_header_rules() {
    let env = spawn_env(true).await;
    let alice = env.create_account("alice").await;
    let key = env.signing_key(&alice.did).await;

    let r = env
        .get(Some(&alice), "app.bsky.feed.getTimeline?limit=5&cursor=abc")
        .header("accept-language", "en, fr")
        .header("atproto-accept-labelers", "did:plc:labeler")
        .header("x-atproto-foo", "bar")
        .header("x-bsky-topics", "t1")
        .header("cookie", "session=secret")
        .header("x-random", "nope")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers().get("atproto-repo-rev").unwrap(), "3abc");
    assert_eq!(r.headers().get("content-language").unwrap(), "en");
    assert!(r.headers().get("content-type").unwrap().to_str().unwrap().starts_with("application/json"));
    assert!(r.headers().get("x-secret").is_none(), "non-allow-listed response header leaked");
    assert!(r.headers().get("set-cookie").is_none());
    let body: J = r.json().await.unwrap();
    assert_eq!(body["path"], "/xrpc/app.bsky.feed.getTimeline");

    let seen = env.appview.last();
    assert_eq!(seen.method, "GET");
    assert_eq!(seen.uri, "/xrpc/app.bsky.feed.getTimeline?limit=5&cursor=abc");
    let h = &seen.headers;
    assert_eq!(h.get("accept-language").unwrap(), "en, fr");
    assert_eq!(h.get("atproto-accept-labelers").unwrap(), "did:plc:labeler");
    assert_eq!(h.get("x-atproto-foo").unwrap(), "bar");
    assert_eq!(h.get("x-bsky-topics").unwrap(), "t1");
    assert_eq!(h.get("accept-encoding").unwrap(), "identity");
    assert!(h.get("cookie").is_none() && h.get("x-random").is_none());
    assert!(h.get("content-type").is_none(), "no content headers on GET");

    let claims = verify_service_jwt(h, &key);
    assert_eq!(claims["iss"], alice.did.as_str());
    assert_eq!(claims["aud"], APPVIEW_DID);
    assert_eq!(claims["lxm"], "app.bsky.feed.getTimeline");

    // Explicit header naming the configured AppView goes to the same place.
    let r = env.get(Some(&alice), "app.bsky.actor.getProfile?actor=x").header("atproto-proxy", format!("{APPVIEW_DID}#bsky_appview")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(env.appview.last().uri, "/xrpc/app.bsky.actor.getProfile?actor=x");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_post_bodies() {
    let env = spawn_env(true).await;
    let bob = env.create_account("bob").await;
    let payload = vec![7u8; 300_000];
    let r = env
        .post(&bob, "app.bsky.test.echo")
        .header("content-type", "application/x-test")
        .header("accept-encoding", "gzip")
        .body(payload.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers().get("content-type").unwrap(), "application/x-test");
    assert_eq!(r.bytes().await.unwrap().as_ref(), payload.as_slice());
    let seen = env.appview.last();
    assert_eq!(seen.method, "POST");
    assert_eq!(seen.headers.get("content-type").unwrap(), "application/x-test");
    assert_eq!(seen.headers.get("accept-encoding").unwrap(), "gzip");
    assert_eq!(seen.body.len(), payload.len());

    // encoded request bodies go upstream as the client sent them: not
    // decoded (that was unbounded), not refused for a coding this PDS
    // doesn't decode
    for coding in ["gzip", "zstd", "br"] {
        let raw: Vec<u8> = (0..5000u32).map(|i| (i * 13) as u8).collect();
        let r = env
            .post(&bob, "app.bsky.test.echo")
            .header("content-type", "application/x-test")
            .header("content-encoding", coding)
            .body(raw.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "{coding}");
        let seen = env.appview.last();
        assert_eq!(seen.headers.get("content-encoding").unwrap(), coding);
        assert_eq!(seen.body.as_ref(), raw.as_slice(), "{coding}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maps_upstream_errors() {
    let env = spawn_env(true).await;
    let u = env.create_account("carol").await;

    let r = env.get(Some(&u), "app.bsky.test.err400").send().await.unwrap();
    assert_eq!(r.headers().get("retry-after").unwrap(), "7");
    assert!(r.headers().get("x-internal").is_none());
    let (s, b) = err_of(r).await;
    assert_eq!((s, b["error"].as_str(), b["message"].as_str()), (400, Some("CustomErr"), Some("boom")));

    let (s, b) = err_of(env.get(Some(&u), "app.bsky.test.err500").send().await.unwrap()).await;
    assert_eq!((s, b["error"].as_str(), b["message"].as_str()), (502, Some("Oops"), Some("upstream broke")));

    // a compressed error body is decoded for its error name
    let (s, b) = err_of(env.get(Some(&u), "app.bsky.test.err400gzip").send().await.unwrap()).await;
    assert_eq!((s, b["error"].as_str(), b["message"].as_str()), (400, Some("CustomGz"), Some("zipped boom")));

    let (s, b) = err_of(env.get(Some(&u), "app.bsky.test.err404plain").send().await.unwrap()).await;
    assert_eq!((s, b["error"].as_str(), b["message"].as_str()), (404, Some("XRPCNotSupported"), Some("XRPC Not Supported")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unreachable_upstream_is_502() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // Port 1 on loopback: connection refused.
    let cfg = Config { appview: Some(("http://127.0.0.1:1".into(), APPVIEW_DID.into())), dev_mode: true, ..Default::default() };
    server::spawn(cfg, listener, None).await.unwrap();
    let http = reqwest::Client::new();
    let v: J = http
        .post(format!("http://{addr}/xrpc/com.atproto.server.createAccount"))
        .json(&json!({"handle": "dave.vlpds.test", "password": "pw", "email": "dave@example.com"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let r = http.get(format!("http://{addr}/xrpc/app.bsky.feed.getTimeline")).bearer_auth(v["accessJwt"].as_str().unwrap()).send().await.unwrap();
    let (s, b) = err_of(r).await;
    assert_eq!((s, b["error"].as_str()), (502, Some("UpstreamFailure")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn target_selection_and_rejections() {
    let env = spawn_env(true).await;
    let u = env.create_account("erin").await;

    // Unauthenticated.
    let (s, b) = err_of(env.get(None, "app.bsky.feed.getTimeline").send().await.unwrap()).await;
    assert_eq!((s, b["error"].as_str()), (401, Some("AuthenticationRequired")));
    // Not a proxied namespace.
    let (s, b) = err_of(env.get(Some(&u), "com.example.foo.bar").send().await.unwrap()).await;
    assert_eq!((s, b["error"].as_str()), (501, Some("MethodNotImplemented")));
    // Chat needs an explicit atproto-proxy header.
    let (s, b) = err_of(env.get(Some(&u), "chat.bsky.convo.listConvos").send().await.unwrap()).await;
    assert_eq!((s, b["error"].as_str()), (400, Some("InvalidRequest")));
    // Protected account-management methods are never proxied. They are
    // implemented locally, and like the reference the local route wins over
    // an atproto-proxy header: xrpc-server mounts `this.routes` before the
    // proxy catchall (packages/xrpc-server/src/server.ts,
    // `this.router.use(this.routes); this.router.use(this.catchall)`), and
    // pipethrough.ts's PROTECTED_METHODS 'Bad token method' check only runs
    // in that catchall. Clients that set atproto-proxy on every request
    // (e.g. getSession) keep working.
    let seen = env.appview.count();
    let r = env.get(Some(&u), "com.atproto.server.listAppPasswords").header("atproto-proxy", format!("{APPVIEW_DID}#bsky_appview")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let b: J = r.json().await.unwrap();
    assert!(b["passwords"].is_array(), "{b}");
    assert_eq!(env.appview.count(), seen, "protected method must not reach the upstream");
    // Malformed headers.
    for bad in ["did:web:x", "#svc", "did:web:x#", "did:web:x#a#b", "did:web:x #a"] {
        let (s, b) = err_of(env.get(Some(&u), "app.bsky.feed.getTimeline").header("atproto-proxy", bad).send().await.unwrap()).await;
        assert_eq!((s, b["error"].as_str()), (400, Some("InvalidRequest")), "{bad}");
    }
    // Unresolvable DID.
    let (s, b) =
        err_of(env.get(Some(&u), "app.bsky.feed.getTimeline").header("atproto-proxy", "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa#bsky_appview").send().await.unwrap())
            .await;
    assert_eq!((s, b["message"].as_str()), (400, Some("could not resolve proxy did")));
    assert_eq!(env.appview.count(), 0);

    // did:web target resolved via /.well-known/did.json (dev mode allows http + loopback).
    let key = env.signing_key(&u.did).await;
    let other_did = env.reports.did.lock().clone();
    let r = env.get(Some(&u), "chat.bsky.convo.listConvos?limit=1").header("atproto-proxy", format!("{other_did}#other_svc")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let seen = env.reports.last();
    assert_eq!(seen.uri, "/xrpc/chat.bsky.convo.listConvos?limit=1");
    let claims = verify_service_jwt(&seen.headers, &key);
    assert_eq!(claims["aud"], other_did.as_str(), "token aud is the bare DID");
    assert_eq!(claims["lxm"], "chat.bsky.convo.listConvos");
    // NSIDs are case-insensitive here as in the method lists: a chat
    // method in any case needs the header...
    let (s, b) = err_of(env.get(Some(&u), "Chat.Bsky.convo.listConvos").send().await.unwrap()).await;
    assert_eq!((s, b["error"].as_str()), (400, Some("InvalidRequest")), "{b}");
    // ...and a non-privileged app password can't reach any of them
    let pw = env
        .post(&u, "com.atproto.server.createAppPassword")
        .json(&json!({"name": "plain", "privileged": false}))
        .send()
        .await
        .unwrap()
        .json::<J>()
        .await
        .unwrap()["password"]
        .as_str()
        .unwrap()
        .to_string();
    let session: J = env
        .http
        .post(format!("{}/xrpc/com.atproto.server.createSession", env.url))
        .json(&json!({"identifier": u.did, "password": pw}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let app_pw = User { did: u.did.clone(), jwt: session["accessJwt"].as_str().unwrap().to_string() };
    let before = env.reports.seen.lock().len();
    for lxm in ["chat.bsky.convo.addReaction", "CHAT.bsky.convo.addReaction", "Chat.Bsky.Convo.AddReaction"] {
        let (s, _) = err_of(env.get(Some(&app_pw), lxm).header("atproto-proxy", format!("{other_did}#other_svc")).send().await.unwrap()).await;
        assert!((400..500).contains(&s), "{lxm}: {s}");
    }
    assert_eq!(env.reports.seen.lock().len(), before, "an app password reached a chat method");
    // Unknown service id in a resolvable document.
    let (s, b) = err_of(env.get(Some(&u), "chat.bsky.convo.listConvos").header("atproto-proxy", format!("{other_did}#nope")).send().await.unwrap()).await;
    assert_eq!((s, b["message"].as_str()), (400, Some("could not resolve proxy did service url")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ssrf_guard_outside_dev_mode() {
    let env = spawn_env(false).await;
    let u = env.create_account("frank").await;
    // A local account's #atproto_pds endpoint is plain http on loopback:
    // refused before connecting.
    let (s, b) = err_of(env.get(Some(&u), "app.bsky.feed.getTimeline").header("atproto-proxy", format!("{}#atproto_pds", u.did)).send().await.unwrap()).await;
    assert_eq!((s, b["error"].as_str()), (502, Some("UpstreamFailure")));
    // did:web on an IP literal is fetched over https (and loopback is refused).
    let other_did = env.reports.did.lock().clone();
    let (s, _) = err_of(env.get(Some(&u), "app.bsky.feed.getTimeline").header("atproto-proxy", format!("{other_did}#other_svc")).send().await.unwrap()).await;
    assert_eq!(s, 400);
    assert_eq!(env.reports.count(), 0);
    // The operator-configured AppView is trusted even on loopback.
    assert_eq!(env.get(Some(&u), "app.bsky.feed.getTimeline").send().await.unwrap().status(), 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preferences_round_trip_and_validation() {
    let env = spawn_env(true).await;
    let u = env.create_account("gina").await;

    let get = || async { env.get(Some(&u), "app.bsky.actor.getPreferences").send().await.unwrap().json::<J>().await.unwrap() };
    assert_eq!(get().await, json!({"preferences": []}));

    let prefs = json!([
        {"$type": "app.bsky.actor.defs#adultContentPref", "enabled": true},
        {"$type": "app.bsky.actor.defs#savedFeedsPrefV2", "items": [{"id": "1", "type": "timeline", "value": "following", "pinned": true}]},
        {"$type": "app.bsky.actor.defs#personalDetailsPref", "birthDate": "2000-01-01T00:00:00.000Z"},
        {"$type": "app.bsky.actor.defs#declaredAgePref", "isOverAge13": false},
    ]);
    let r = env.post(&u, "app.bsky.actor.putPreferences").json(&json!({"preferences": prefs})).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let got = get().await["preferences"].as_array().unwrap().clone();
    let types: Vec<&str> = got.iter().map(|p| p["$type"].as_str().unwrap()).collect();
    assert_eq!(
        types,
        [
            "app.bsky.actor.defs#adultContentPref",
            "app.bsky.actor.defs#savedFeedsPrefV2",
            "app.bsky.actor.defs#personalDetailsPref",
            "app.bsky.actor.defs#declaredAgePref"
        ]
    );
    assert_eq!(got[1], prefs[1]);
    assert_eq!(got[3], json!({"$type": "app.bsky.actor.defs#declaredAgePref", "isOverAge13": true, "isOverAge16": true, "isOverAge18": true}));

    // Replace: the namespace is overwritten as a whole.
    let r = env
        .post(&u, "app.bsky.actor.putPreferences")
        .json(&json!({"preferences": [{"$type": "app.bsky.actor.defs#adultContentPref", "enabled": false}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(get().await, json!({"preferences": [{"$type": "app.bsky.actor.defs#adultContentPref", "enabled": false}]}));

    for (body, msg) in [
        (json!({"preferences": [{"enabled": true}]}), "Preference is missing a $type"),
        (json!({"preferences": [{"$type": "com.example.pref"}]}), "Some preferences are not in the app.bsky namespace"),
        (json!({"preferences": [{"$type": "app.bskyx.pref"}]}), "Some preferences are not in the app.bsky namespace"),
        (json!({"nope": []}), "Input must have the property \"preferences\""),
    ] {
        let (s, b) = err_of(env.post(&u, "app.bsky.actor.putPreferences").json(&body).send().await.unwrap()).await;
        assert_eq!((s, b["error"].as_str(), b["message"].as_str()), (400, Some("InvalidRequest"), Some(msg)), "{body}");
    }
    // Failed writes changed nothing; the AppView was never called.
    assert_eq!(get().await["preferences"].as_array().unwrap().len(), 1);
    assert_eq!(env.appview.count(), 0);
    // Preferences are per-account.
    let other = env.create_account("hank").await;
    let v: J = env.get(Some(&other), "app.bsky.actor.getPreferences").send().await.unwrap().json().await.unwrap();
    assert_eq!(v, json!({"preferences": []}));
    // Auth required.
    assert_eq!(env.get(None, "app.bsky.actor.getPreferences").send().await.unwrap().status(), 401);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preferences_for_another_appview_are_proxied() {
    let env = spawn_env(true).await;
    let u = env.create_account("ivy").await;
    let other_did = env.reports.did.lock().clone();
    let r = env.get(Some(&u), "app.bsky.actor.getPreferences").header("atproto-proxy", format!("{other_did}#other_svc")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let seen = env.reports.last();
    assert_eq!(seen.uri, "/xrpc/app.bsky.actor.getPreferences");
    let claims = verify_service_jwt(&seen.headers, &env.signing_key(&u.did).await);
    assert_eq!(claims["lxm"], "app.bsky.actor.getPreferences");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_report_goes_to_report_service() {
    let env = spawn_env(true).await;
    let u = env.create_account("jack").await;
    let key = env.signing_key(&u.did).await;
    let report =
        json!({"reasonType": "com.atproto.moderation.defs#reasonSpam", "subject": {"$type": "com.atproto.admin.defs#repoRef", "did": "did:plc:spammer"}});
    let r = env.post(&u, "com.atproto.moderation.createReport").json(&report).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let v: J = r.json().await.unwrap();
    assert_eq!(v["id"], 42);
    assert_eq!(v["reasonType"], report["reasonType"]);
    let seen = env.reports.last();
    assert_eq!(seen.method, "POST");
    assert_eq!(serde_json::from_slice::<J>(&seen.body).unwrap(), report);
    let claims = verify_service_jwt(&seen.headers, &key);
    assert_eq!(claims["iss"], u.did.as_str());
    assert_eq!(claims["aud"], REPORT_DID);
    assert_eq!(claims["lxm"], "com.atproto.moderation.createReport");
    assert_eq!(env.appview.count(), 0);

    let (s, _) = err_of(env.post(&u, "com.atproto.moderation.createReport").json(&json!({"subject": {}})).send().await.unwrap()).await;
    assert_eq!(s, 400);
}

/// A takendown-scope session (createSession allowTakendown) may appeal via
/// tools.ozone.inbox.appealActionedSubject, which is proxied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takendown_session_can_appeal() {
    let env = spawn_env(true).await;
    let u = env.create_account("appealer").await;
    let r = env
        .http
        .post(format!("{}/xrpc/com.atproto.admin.updateSubjectStatus", env.url))
        .basic_auth("admin", Some(server::DEV_ADMIN_TOKEN))
        .json(&json!({"subject": {"$type": "com.atproto.admin.defs#repoRef", "did": u.did}, "takedown": {"applied": true}}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = env
        .http
        .post(format!("{}/xrpc/com.atproto.server.createSession", env.url))
        .json(&json!({"identifier": "appealer.vlpds.test", "password": "hunter22", "allowTakendown": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: J = r.json().await.unwrap();
    let tk = User { did: u.did.clone(), jwt: v["accessJwt"].as_str().unwrap().into() };

    let r = env
        .post(&tk, "tools.ozone.inbox.appealActionedSubject")
        .json(&json!({"subject": {"$type": "com.atproto.admin.defs#repoRef", "did": u.did}}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "{}", r.text().await.unwrap());
    assert_eq!(env.appview.last().uri, "/xrpc/tools.ozone.inbox.appealActionedSubject");
    // other proxied methods stay closed to the takendown scope
    let (s, e) = err_of(env.get(Some(&tk), "app.bsky.feed.getTimeline").send().await.unwrap()).await;
    assert_eq!((s, e["error"].as_str()), (400, Some("InvalidToken")));
}

/// repo.getRecord for a repo not hosted here pipes through to the AppView
/// without credentials (reference getRecord -> pipethrough).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_record_for_unhosted_repo_goes_to_appview() {
    let env = spawn_env(true).await;
    let u = env.create_account("hoster").await;
    for repo in ["did:plc:z72i7hdynmk6r22z27h6tvur", "someone.elsewhere.test"] {
        let q = format!("com.atproto.repo.getRecord?repo={repo}&collection=app.bsky.actor.profile&rkey=self");
        let r = env.get(Some(&u), &q).send().await.unwrap();
        assert_eq!(r.status(), 200);
        let body: J = r.json().await.unwrap();
        assert_eq!(body["path"], "/xrpc/com.atproto.repo.getRecord");
        let seen = env.appview.last();
        assert_eq!(seen.uri, format!("/xrpc/{q}"));
        assert!(seen.headers.get("authorization").is_none(), "pipethrough is unauthenticated");
    }
    // a local repo is served locally, misses included
    let n = env.appview.count();
    let (s, e) =
        err_of(env.get(None, &format!("com.atproto.repo.getRecord?repo={}&collection=app.bsky.actor.profile&rkey=self", u.did)).send().await.unwrap()).await;
    assert_eq!((s, e["error"].as_str()), (400, Some("RecordNotFound")));
    assert_eq!(env.appview.count(), n);
}
