//! Shared harness for the vlpds conformance suite.
//!
//! - `TestServer::spawn()` boots an in-process PDS (in-memory object store,
//!   dev mode) on 127.0.0.1:0.
//! - `Xrpc` is a small typed client: `get`/`post` return a `Resp` with status,
//!   decoded JSON and helpers to assert success or a specific XRPC error.
//! - `TestAccount` + `TestServer::create_account` for account fixtures.
//! - `Sub` is a firehose subscriber that decodes frames into `Frame`s.
//! - Repo helpers: CAR parsing, commit signature checks, MST loading and
//!   sync 1.1 commit inversion.
#![allow(dead_code)]

pub use serde_json::{json, Value as J};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
pub use vlpds::cbor::Value;
pub use vlpds::cid::Cid;
use vlpds::mst::Tree;

pub const ADMIN_TOKEN: &str = "dev-admin-token";
pub const HANDLE_DOMAIN: &str = "vlpds.test";
pub const PASSWORD: &str = "hunter2-password";

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Held by tests that set or depend on the process-wide active feature
/// level (`vlpds::version::active`, what writers emit).
pub static ACTIVE_LEVEL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Unique, valid handle label (lowercase alnum), e.g. "alice3k9x".
/// A unique name that still fits one 18-character handle label: the
/// counter and random suffix are base-36, and a long prefix is cut so the
/// whole name never exceeds 18 characters however many names a run makes.
pub fn unique_name(prefix: &str) -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let r: u32 = rand::random::<u32>() % 46656;
    let suffix = format!("{}x{}", radix36(n), radix36(r as u64));
    let keep = 18usize.saturating_sub(suffix.len()).min(prefix.len());
    format!("{}{suffix}", &prefix[..keep])
}

fn radix36(mut n: u64) -> String {
    const A: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut s = Vec::new();
    loop {
        s.push(A[(n % 36) as usize]);
        n /= 36;
        if n == 0 {
            break;
        }
    }
    s.reverse();
    String::from_utf8(s).unwrap()
}

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("VLPDS_TEST_LOG")
                .unwrap_or_else(|_| "off".into()),
        )
        .with_test_writer()
        .try_init();
}

// ---------------------------------------------------------------------------
// server
// ---------------------------------------------------------------------------

pub struct TestServer {
    pub app: Arc<vlpds::xrpc::App>,
    pub addr: SocketAddr,
    pub url: String,
    pub xrpc: Xrpc,
}

impl TestServer {
    pub async fn spawn() -> TestServer {
        Self::spawn_with(|_| {}).await
    }

    pub async fn spawn_with(f: impl FnOnce(&mut vlpds::server::Config)) -> TestServer {
        init_tracing();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}");
        let mut cfg = vlpds::server::Config {
            dev_mode: true,
            public_url: url.clone(),
            // Off by default, as in the reference's dev-env test network: the
            // suite drives thousands of writes from one IP and DID.
            // tests/rate_limits.rs turns them on.
            rate_limits_enabled: false,
            // small ring is fine; tests are tiny
            ..Default::default()
        };
        f(&mut cfg);
        let (app, addr) = vlpds::server::spawn(cfg, listener)
            .await
            .expect("spawn server");
        TestServer {
            app,
            addr,
            url: url.clone(),
            xrpc: Xrpc::new(&url),
        }
    }

    pub fn ws_url(&self, cursor: Option<i64>) -> String {
        match cursor {
            Some(c) => format!(
                "ws://{}/xrpc/com.atproto.sync.subscribeRepos?cursor={c}",
                self.addr
            ),
            None => format!("ws://{}/xrpc/com.atproto.sync.subscribeRepos", self.addr),
        }
    }

    /// Creates an account with a unique handle `{prefix}N.vlpds.test` and an
    /// email `{handle}@example.com`; panics unless it succeeds.
    pub async fn create_account(&self, prefix: &str) -> TestAccount {
        let handle = format!("{}.{HANDLE_DOMAIN}", unique_name(prefix));
        self.create_account_with(&handle, PASSWORD).await
    }

    pub async fn create_account_with(&self, handle: &str, password: &str) -> TestAccount {
        let email = format!("{}@example.com", handle.replace('.', "-"));
        let r = self
            .xrpc
            .post(
                "com.atproto.server.createAccount",
                &json!({"handle": handle, "password": password, "email": email}),
                &Auth::None,
            )
            .await;
        let j = r.ok();
        TestAccount {
            did: j["did"].as_str().unwrap_or_else(|| panic!("createAccount failed: {j}")).to_string(),
            handle: j["handle"].as_str().unwrap_or(handle).to_string(),
            password: password.to_string(),
            email,
            access: j["accessJwt"].as_str().expect("accessJwt").to_string(),
            refresh: j["refreshJwt"].as_str().unwrap_or_default().to_string(),
        }
    }

    pub async fn create_session(&self, identifier: &str, password: &str) -> Resp {
        self.xrpc
            .post(
                "com.atproto.server.createSession",
                &json!({"identifier": identifier, "password": password}),
                &Auth::None,
            )
            .await
    }

    // ---- record helpers ----

    pub async fn create_record(&self, a: &TestAccount, collection: &str, record: J) -> RecordRef {
        let r = self
            .xrpc
            .post(
                "com.atproto.repo.createRecord",
                &json!({"repo": a.did, "collection": collection, "record": record}),
                &a.auth(),
            )
            .await
            .ok();
        RecordRef::from_json(&r)
    }

    pub async fn post(&self, a: &TestAccount, text: &str) -> RecordRef {
        self.create_record(a, "app.bsky.feed.post", post_record(text))
            .await
    }

    pub async fn get_record(&self, did: &str, collection: &str, rkey: &str) -> Resp {
        self.xrpc
            .get(
                "com.atproto.repo.getRecord",
                &[("repo", did), ("collection", collection), ("rkey", rkey)],
                &Auth::None,
            )
            .await
    }

    pub async fn list_records(&self, did: &str, collection: &str, extra: &[(&str, &str)]) -> Resp {
        let mut q = vec![("repo", did), ("collection", collection)];
        q.extend_from_slice(extra);
        self.xrpc
            .get("com.atproto.repo.listRecords", &q, &Auth::None)
            .await
    }

    pub async fn latest_commit(&self, did: &str) -> (Cid, String) {
        let j = self
            .xrpc
            .get(
                "com.atproto.sync.getLatestCommit",
                &[("did", did)],
                &Auth::None,
            )
            .await
            .ok();
        (
            Cid::parse(j["cid"].as_str().unwrap()).unwrap(),
            j["rev"].as_str().unwrap().to_string(),
        )
    }

    /// Downloads and parses `sync.getRepo`.
    pub async fn get_repo(&self, did: &str) -> Repo {
        let r = self
            .xrpc
            .get("com.atproto.sync.getRepo", &[("did", did)], &Auth::None)
            .await;
        assert_eq!(r.status, 200, "getRepo failed: {}", r.text());
        Repo::from_car(&r.body).expect("parse getRepo CAR")
    }

    /// Signing key from the account's DID document (via describeRepo).
    pub async fn signing_key(&self, did: &str) -> k256::ecdsa::VerifyingKey {
        let j = self
            .xrpc
            .get(
                "com.atproto.repo.describeRepo",
                &[("repo", did)],
                &Auth::None,
            )
            .await
            .ok();
        let vms = j["didDoc"]["verificationMethod"]
            .as_array()
            .expect("verificationMethod");
        let vm = vms
            .iter()
            .find(|v| {
                v["id"]
                    .as_str()
                    .map(|s| s.ends_with("#atproto"))
                    .unwrap_or(false)
            })
            .unwrap_or(&vms[0]);
        decode_k256_multibase(vm["publicKeyMultibase"].as_str().unwrap()).expect("k256 multikey")
    }

    // ---- admin / dev helpers ----

    /// Messages "sent" to `email` in dev mode (vlpds.admin.getDevMail).
    pub async fn dev_mail(&self, email: &str) -> Resp {
        self.xrpc
            .get("vlpds.admin.getDevMail", &[("email", email)], &Auth::Admin)
            .await
    }

    /// Latest emailed token for `email` (searches the dev-mail JSON for a
    /// "token" field, else for an XXXXX-XXXXX code in any string).
    pub async fn mail_token(&self, email: &str) -> Option<String> {
        let r = self.dev_mail(email).await;
        if r.status != 200 {
            return None;
        }
        find_token(&r.json)
    }

    pub async fn subscribe(&self, cursor: Option<i64>) -> Sub {
        Sub::connect(&self.ws_url(cursor)).await
    }

    /// Subscribes with a cursor at the current head, so only events sequenced
    /// after this call are delivered. (A cursor-less live subscription may
    /// still receive events for writes acked just before it connected: the
    /// broadcast waits for every partition's watermark, which can trail acks.)
    ///
    /// The cursor is a seq, not an event: every event acked before this call
    /// is at or below this node log's durable watermark (or the clock, for
    /// other nodes' logs), and the call returns once the firehose has settled
    /// past it. (It used to replay from 0 and take the highest seq seen
    /// within 400 ms of idleness, which a slow backfill start or emission
    /// lag under load turned into cursor 0: the account creation's events
    /// were then replayed to tests expecting only later ones.)
    pub async fn subscribe_from_now(&self) -> Sub {
        let head = self.settled_now().await;
        self.subscribe(Some(head)).await
    }

    /// A firehose cursor after every event acked before this call, once the
    /// firehose has settled up to it (events at or below it are never sent
    /// to a subscriber with this cursor; everything above it is).
    pub async fn settled_now(&self) -> i64 {
        let clock = vlpds::nodelog::seq_floor(vlpds::tid::now_micros()) - 1;
        let target = self.app.log.wm.get().max(clock);
        let deadline = tokio::time::Instant::now() + FH_TIMEOUT;
        while self.app.firehose.position() < target {
            assert!(tokio::time::Instant::now() < deadline, "firehose never settled past {target}");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        target
    }

    /// Waits until every subscription in `subs` is live, without a fixed sleep:
    /// a cursor-less subscription only sees events broadcast after the server
    /// registered it, which can trail the websocket handshake. Writes probe
    /// posts as `a` until each sub has received one, then consumes each stream
    /// up to the last probe's commit. Returns that commit's seq; everything
    /// read afterwards was sequenced after it.
    pub async fn sync_subs(&self, a: &TestAccount, subs: &mut [Sub]) -> i64 {
        let deadline = tokio::time::Instant::now() + FH_TIMEOUT;
        let is_probe = |f: &Frame| f.kind() == "#commit" && f.did() == Some(a.did.as_str());
        let mut seen: Vec<Vec<Frame>> = subs.iter().map(|_| Vec::new()).collect();
        let mut live = vec![false; subs.len()];
        let last_rev = loop {
            assert!(tokio::time::Instant::now() < deadline, "subscriptions never went live");
            let rev = self.post(a, "probe").await.rev.expect("probe rev");
            for (i, sub) in subs.iter_mut().enumerate() {
                if !live[i] {
                    let (fs, ok) = sub.try_until(Duration::from_millis(250), |fs| fs.iter().any(is_probe)).await;
                    seen[i].extend(fs);
                    live[i] = ok;
                }
            }
            if live.iter().all(|l| *l) {
                break rev;
            }
        };
        let is_last = |f: &Frame| is_probe(f) && f.str("rev") == Some(last_rev.as_str());
        let mut seq = 0;
        for (i, sub) in subs.iter_mut().enumerate() {
            if !seen[i].iter().any(is_last) {
                let fs = sub.until(FH_TIMEOUT, |fs| fs.last().map(is_last).unwrap_or(false)).await;
                seen[i].extend(fs);
            }
            let last = seen[i].iter().rev().find(|f| is_last(f)).expect("last probe");
            seq = last.seq().expect("probe seq");
        }
        seq
    }

    /// Highest seq currently on the firehose (replays from 0 and stops when idle).
    pub async fn current_seq(&self) -> i64 {
        let mut sub = self.subscribe(Some(0)).await;
        let frames = sub.drain(Duration::from_millis(400)).await;
        frames.iter().filter_map(|f| f.seq()).max().unwrap_or(0)
    }
}

pub fn post_record(text: &str) -> J {
    json!({"$type": "app.bsky.feed.post", "text": text, "createdAt": now_iso()})
}

pub fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn find_token(j: &J) -> Option<String> {
    fn by_key(j: &J) -> Option<String> {
        match j {
            J::Object(o) => {
                for k in ["token", "code"] {
                    if let Some(J::String(s)) = o.get(k) {
                        return Some(s.clone());
                    }
                }
                o.values().rev().find_map(by_key)
            }
            J::Array(a) => a.iter().rev().find_map(by_key),
            _ => None,
        }
    }
    fn by_pattern(j: &J) -> Option<String> {
        match j {
            J::String(s) => {
                let b = s.as_bytes();
                (0..b.len().saturating_sub(10)).rev().find_map(|i| {
                    let w = &b[i..i + 11];
                    let ok = w[5] == b'-'
                        && w[..5].iter().chain(&w[6..]).all(|c| {
                            c.is_ascii_uppercase() || c.is_ascii_digit() || c.is_ascii_lowercase()
                        });
                    ok.then(|| String::from_utf8_lossy(w).to_string())
                })
            }
            J::Object(o) => o.values().rev().find_map(by_pattern),
            J::Array(a) => a.iter().rev().find_map(by_pattern),
            _ => None,
        }
    }
    by_key(j).or_else(|| by_pattern(j))
}

// ---------------------------------------------------------------------------
// accounts / records
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct TestAccount {
    pub did: String,
    pub handle: String,
    pub password: String,
    pub email: String,
    pub access: String,
    pub refresh: String,
}

impl TestAccount {
    pub fn auth(&self) -> Auth {
        Auth::Bearer(self.access.clone())
    }
    pub fn refresh_auth(&self) -> Auth {
        Auth::Bearer(self.refresh.clone())
    }
}

#[derive(Clone, Debug)]
pub struct RecordRef {
    pub uri: String,
    pub cid: String,
    pub commit_cid: Option<String>,
    pub rev: Option<String>,
}

impl RecordRef {
    pub fn from_json(j: &J) -> RecordRef {
        RecordRef {
            uri: j["uri"].as_str().expect("uri").to_string(),
            cid: j["cid"].as_str().expect("cid").to_string(),
            commit_cid: j["commit"]["cid"].as_str().map(String::from),
            rev: j["commit"]["rev"].as_str().map(String::from),
        }
    }
    pub fn rkey(&self) -> &str {
        self.uri.rsplit('/').next().unwrap()
    }
    pub fn collection(&self) -> &str {
        let mut it = self.uri.rsplit('/');
        it.next();
        it.next().unwrap()
    }
    pub fn did(&self) -> &str {
        self.uri
            .trim_start_matches("at://")
            .split('/')
            .next()
            .unwrap()
    }
}

// ---------------------------------------------------------------------------
// XRPC client
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub enum Auth {
    None,
    Bearer(String),
    Admin,
    Basic(String, String),
    Raw(String),
}

#[derive(Clone)]
pub struct Xrpc {
    pub http: reqwest::Client,
    pub base: String,
}

pub struct Resp {
    pub status: u16,
    pub headers: reqwest::header::HeaderMap,
    pub body: bytes::Bytes,
    /// Parsed JSON body (Null if not JSON).
    pub json: J,
}

impl std::fmt::Debug for Resp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.status, self.text())
    }
}

impl Resp {
    pub fn text(&self) -> String {
        let s = String::from_utf8_lossy(&self.body);
        if s.len() > 2000 {
            format!("{}…", &s[..2000])
        } else {
            s.to_string()
        }
    }

    pub fn is_ok(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// Asserts 2xx and returns the JSON body.
    #[track_caller]
    pub fn ok(&self) -> J {
        assert!(
            self.is_ok(),
            "expected success, got {} {}",
            self.status,
            self.text()
        );
        self.json.clone()
    }

    pub fn error_name(&self) -> Option<&str> {
        self.json.get("error").and_then(|e| e.as_str())
    }

    /// Asserts an XRPC error with this HTTP status and error name.
    #[track_caller]
    pub fn err(&self, status: u16, name: &str) {
        assert_eq!(
            (self.status, self.error_name()),
            (status, Some(name)),
            "unexpected response: {}",
            self.text()
        );
    }

    /// Asserts an XRPC error with this status (any error name).
    #[track_caller]
    pub fn err_status(&self, status: u16) {
        assert_eq!(self.status, status, "unexpected response: {}", self.text());
    }

    /// Asserts a 4xx failure (any).
    #[track_caller]
    pub fn client_err(&self) {
        assert!(
            (400..500).contains(&self.status),
            "expected 4xx, got {} {}",
            self.status,
            self.text()
        );
    }

    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(String::from)
    }
}

impl Xrpc {
    pub fn new(base: &str) -> Xrpc {
        Xrpc {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap(),
            base: base.trim_end_matches('/').to_string(),
        }
    }

    fn url(&self, nsid: &str) -> String {
        format!("{}/xrpc/{nsid}", self.base)
    }

    fn apply(rb: reqwest::RequestBuilder, auth: &Auth) -> reqwest::RequestBuilder {
        use base64::Engine;
        match auth {
            Auth::None => rb,
            Auth::Bearer(t) => rb.header("authorization", format!("Bearer {t}")),
            Auth::Admin => rb.header(
                "authorization",
                format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD
                        .encode(format!("admin:{ADMIN_TOKEN}"))
                ),
            ),
            Auth::Basic(u, p) => rb.header(
                "authorization",
                format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}"))
                ),
            ),
            Auth::Raw(h) => rb.header("authorization", h.clone()),
        }
    }

    pub async fn send(&self, rb: reqwest::RequestBuilder) -> Resp {
        self.try_send(rb).await.expect("http request")
    }

    /// Like `send`, but a transport error (e.g. the server answered and closed
    /// before the request body was fully written) comes back as `Err`.
    pub async fn try_send(&self, rb: reqwest::RequestBuilder) -> reqwest::Result<Resp> {
        let r = rb.send().await?;
        let status = r.status().as_u16();
        let headers = r.headers().clone();
        let body = r.bytes().await.unwrap_or_default();
        let json = serde_json::from_slice(&body).unwrap_or(J::Null);
        Ok(Resp {
            status,
            headers,
            body,
            json,
        })
    }

    pub async fn get(&self, nsid: &str, query: &[(&str, &str)], auth: &Auth) -> Resp {
        let rb = self.http.get(self.url(nsid)).query(query);
        self.send(Self::apply(rb, auth)).await
    }

    /// GET with repeated params (e.g. cids=a&cids=b).
    pub async fn get_multi(&self, nsid: &str, query: &[(&str, String)], auth: &Auth) -> Resp {
        let rb = self.http.get(self.url(nsid)).query(query);
        self.send(Self::apply(rb, auth)).await
    }

    pub async fn post(&self, nsid: &str, body: &J, auth: &Auth) -> Resp {
        let rb = self.http.post(self.url(nsid)).json(body);
        self.send(Self::apply(rb, auth)).await
    }

    /// POST with no body (procedures without input).
    pub async fn post_empty(&self, nsid: &str, auth: &Auth) -> Resp {
        let rb = self.http.post(self.url(nsid));
        self.send(Self::apply(rb, auth)).await
    }

    pub async fn post_bytes(
        &self,
        nsid: &str,
        body: Vec<u8>,
        content_type: &str,
        auth: &Auth,
    ) -> Resp {
        let rb = self
            .http
            .post(self.url(nsid))
            .header("content-type", content_type)
            .body(body);
        self.send(Self::apply(rb, auth)).await
    }

    /// `post_bytes` that tolerates the server closing early (see `try_send`).
    pub async fn try_post_bytes(
        &self,
        nsid: &str,
        body: Vec<u8>,
        content_type: &str,
        auth: &Auth,
    ) -> reqwest::Result<Resp> {
        let rb = self
            .http
            .post(self.url(nsid))
            .header("content-type", content_type)
            .body(body);
        self.try_send(Self::apply(rb, auth)).await
    }
}

// ---------------------------------------------------------------------------
// firehose
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Frame {
    /// 1 = message, -1 = error
    pub op: i64,
    /// "#commit", "#sync", "#identity", "#account", "#info", ...
    pub t: Option<String>,
    pub body: Value,
    pub raw: Vec<u8>,
}

impl Frame {
    pub fn decode(raw: &[u8]) -> anyhow::Result<Frame> {
        let (header, n) = Value::decode_prefix(raw)?;
        let body = Value::decode(&raw[n..])?;
        let op = match header.get("op") {
            Some(Value::Int(i)) => *i,
            _ => anyhow::bail!("frame header without op"),
        };
        let t = header.get("t").and_then(|v| v.as_str()).map(String::from);
        Ok(Frame {
            op,
            t,
            body,
            raw: raw.to_vec(),
        })
    }

    pub fn kind(&self) -> &str {
        self.t.as_deref().unwrap_or("")
    }

    pub fn seq(&self) -> Option<i64> {
        match self.body.get("seq") {
            Some(Value::Int(i)) => Some(*i),
            _ => None,
        }
    }

    /// `repo` for #commit, `did` for everything else.
    pub fn did(&self) -> Option<&str> {
        self.body
            .get("repo")
            .or_else(|| self.body.get("did"))
            .and_then(|v| v.as_str())
    }

    pub fn str(&self, k: &str) -> Option<&str> {
        self.body.get(k).and_then(|v| v.as_str())
    }

    pub fn bool(&self, k: &str) -> Option<bool> {
        match self.body.get(k) {
            Some(Value::Bool(b)) => Some(*b),
            _ => None,
        }
    }

    pub fn commit(&self) -> Option<CommitEvt> {
        (self.kind() == "#commit")
            .then(|| CommitEvt::from_body(&self.body).expect("decode #commit"))
    }

    pub fn sync(&self) -> Option<SyncEvt> {
        (self.kind() == "#sync").then(|| SyncEvt::from_body(&self.body).expect("decode #sync"))
    }
}

pub struct Sub {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    pub closed: bool,
}

impl Sub {
    pub async fn connect(url: &str) -> Sub {
        let (ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("ws connect");
        Sub { ws, closed: false }
    }

    /// Next decoded frame, or None on timeout / close.
    pub async fn next(&mut self, timeout: Duration) -> Option<Frame> {
        use futures::StreamExt;
        use tokio_tungstenite::tungstenite::Message;
        if self.closed {
            return None;
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let m = match tokio::time::timeout_at(deadline, self.ws.next()).await {
                Err(_) => return None,
                Ok(None) => {
                    self.closed = true;
                    return None;
                }
                Ok(Some(Err(_))) => {
                    self.closed = true;
                    return None;
                }
                Ok(Some(Ok(m))) => m,
            };
            match m {
                Message::Binary(b) => return Some(Frame::decode(&b).expect("decode frame")),
                Message::Close(_) => {
                    self.closed = true;
                    return None;
                }
                _ => continue,
            }
        }
    }

    /// Collects frames until `pred` holds for the collected list (checked after
    /// each frame). Panics with what it has on timeout.
    #[track_caller]
    pub fn until<'a>(
        &'a mut self,
        timeout: Duration,
        mut pred: impl FnMut(&[Frame]) -> bool + 'a,
    ) -> impl std::future::Future<Output = Vec<Frame>> + 'a {
        let loc = std::panic::Location::caller();
        async move {
            let deadline = tokio::time::Instant::now() + timeout;
            let mut out = Vec::new();
            loop {
                if pred(&out) {
                    return out;
                }
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                match self.next(left).await {
                    Some(f) => out.push(f),
                    None => panic!(
                        "{loc}: firehose condition not met within {timeout:?} (closed={}); got {} frames: {:?}",
                        self.closed,
                        out.len(),
                        out.iter().map(|f| (f.kind().to_string(), f.seq(), f.did().map(String::from))).collect::<Vec<_>>()
                    ),
                }
            }
        }
    }

    /// Like `until` but returns whatever arrived instead of panicking.
    pub async fn try_until(
        &mut self,
        timeout: Duration,
        mut pred: impl FnMut(&[Frame]) -> bool,
    ) -> (Vec<Frame>, bool) {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut out = Vec::new();
        loop {
            if pred(&out) {
                return (out, true);
            }
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match self.next(left).await {
                Some(f) => out.push(f),
                None => return (out, false),
            }
        }
    }

    /// Reads frames until none arrive for `idle`.
    pub async fn drain(&mut self, idle: Duration) -> Vec<Frame> {
        let mut out = Vec::new();
        while let Some(f) = self.next(idle).await {
            out.push(f);
        }
        out
    }

    /// Collects frames until one for `did` of kind `kind` matching `pred` arrives.
    pub async fn wait_for(&mut self, timeout: Duration, did: &str, kind: &str) -> Vec<Frame> {
        let did = did.to_string();
        let kind = kind.to_string();
        self.until(timeout, move |fs| {
            fs.last()
                .map(|f| f.did() == Some(did.as_str()) && f.kind() == kind)
                .unwrap_or(false)
        })
        .await
    }
}

#[derive(Clone, Debug)]
pub struct RepoOp {
    pub action: String,
    pub path: String,
    pub cid: Option<Cid>,
    pub prev: Option<Cid>,
}

#[derive(Clone, Debug)]
pub struct CommitEvt {
    pub seq: i64,
    pub repo: String,
    pub rev: String,
    pub since: Option<String>,
    pub commit: Cid,
    pub prev_data: Option<Cid>,
    pub blocks: HashMap<Cid, Vec<u8>>,
    pub blocks_roots: Vec<Cid>,
    pub ops: Vec<RepoOp>,
    pub blobs: Vec<Cid>,
    pub too_big: bool,
    pub rebase: bool,
    pub time: String,
}

fn link(v: Option<&Value>) -> Option<Cid> {
    match v {
        Some(Value::Link(c)) => Some(*c),
        _ => None,
    }
}

impl CommitEvt {
    pub fn from_body(b: &Value) -> anyhow::Result<CommitEvt> {
        let blocks_raw = match b.get("blocks") {
            Some(Value::Bytes(x)) => x.clone(),
            _ => anyhow::bail!("#commit without blocks"),
        };
        let (roots, blocks) = vlpds::car::read_car(&blocks_raw)?;
        let ops = match b.get("ops") {
            Some(Value::Array(a)) => a
                .iter()
                .map(|o| RepoOp {
                    action: o
                        .get("action")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    path: o
                        .get("path")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    cid: link(o.get("cid")),
                    prev: link(o.get("prev")),
                })
                .collect(),
            _ => vec![],
        };
        let blobs = match b.get("blobs") {
            Some(Value::Array(a)) => a.iter().filter_map(|v| link(Some(v))).collect(),
            _ => vec![],
        };
        Ok(CommitEvt {
            seq: match b.get("seq") {
                Some(Value::Int(i)) => *i,
                _ => anyhow::bail!("no seq"),
            },
            repo: b
                .get("repo")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            rev: b
                .get("rev")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            since: b.get("since").and_then(|v| v.as_str()).map(String::from),
            commit: link(b.get("commit")).ok_or_else(|| anyhow::anyhow!("no commit"))?,
            prev_data: link(b.get("prevData")),
            blocks: blocks.into_iter().map(|(c, d)| (c, d.to_vec())).collect(),
            blocks_roots: roots,
            ops,
            blobs,
            too_big: matches!(b.get("tooBig"), Some(Value::Bool(true))),
            rebase: matches!(b.get("rebase"), Some(Value::Bool(true))),
            time: b
                .get("time")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        })
    }

    pub fn commit_obj(&self) -> CommitObj {
        CommitObj::decode(
            self.blocks
                .get(&self.commit)
                .expect("commit block missing from #commit blocks"),
        )
        .expect("decode commit")
    }

    /// sync 1.1 inversion: undo `ops` on the partial tree from `blocks` and
    /// return the resulting root (must equal prevData).
    pub fn invert(&self) -> anyhow::Result<Cid> {
        let c = self.commit_obj();
        let mut tree = Tree::load_from_blocks(&self.blocks, c.data)
            .map_err(|e| anyhow::anyhow!("load partial tree: {e}"))?;
        // Check the post-state of each op first.
        for op in &self.ops {
            let got = tree
                .get(op.path.as_bytes())
                .map_err(|e| anyhow::anyhow!("get {}: {e}", op.path))?;
            let want = if op.action == "delete" { None } else { op.cid };
            anyhow::ensure!(
                got == want,
                "op {} {}: tree has {:?}, op says {:?}",
                op.action,
                op.path,
                got,
                want
            );
        }
        for op in &self.ops {
            match op.action.as_str() {
                "create" => {
                    anyhow::ensure!(op.prev.is_none(), "create {} has prev", op.path);
                    tree.remove(op.path.as_bytes())
                        .map_err(|e| anyhow::anyhow!("invert create {}: {e}", op.path))?;
                }
                "update" | "delete" => {
                    let p = op
                        .prev
                        .ok_or_else(|| anyhow::anyhow!("{} {} without prev", op.action, op.path))?;
                    tree.insert(op.path.as_bytes(), p)
                        .map_err(|e| anyhow::anyhow!("invert {} {}: {e}", op.action, op.path))?;
                }
                a => anyhow::bail!("unknown action {a}"),
            }
        }
        tree.root_cid()
            .map_err(|e| anyhow::anyhow!("root after inversion: {e}"))
    }
}

#[derive(Clone, Debug)]
pub struct SyncEvt {
    pub seq: i64,
    pub did: String,
    pub rev: String,
    pub commit: Cid,
    pub blocks: HashMap<Cid, Vec<u8>>,
}

impl SyncEvt {
    pub fn from_body(b: &Value) -> anyhow::Result<SyncEvt> {
        let raw = match b.get("blocks") {
            Some(Value::Bytes(x)) => x.clone(),
            _ => anyhow::bail!("#sync without blocks"),
        };
        let (roots, blocks) = vlpds::car::read_car(&raw)?;
        Ok(SyncEvt {
            seq: match b.get("seq") {
                Some(Value::Int(i)) => *i,
                _ => anyhow::bail!("no seq"),
            },
            did: b
                .get("did")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            rev: b
                .get("rev")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            commit: *roots
                .first()
                .ok_or_else(|| anyhow::anyhow!("#sync blocks without root"))?,
            blocks: blocks.into_iter().map(|(c, d)| (c, d.to_vec())).collect(),
        })
    }

    pub fn commit_obj(&self) -> CommitObj {
        CommitObj::decode(self.blocks.get(&self.commit).expect("#sync commit block"))
            .expect("decode commit")
    }
}

// ---------------------------------------------------------------------------
// repo / commit helpers
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct CommitObj {
    pub did: String,
    pub rev: String,
    pub data: Cid,
    pub version: i64,
    pub prev: Option<Cid>,
    pub sig: Vec<u8>,
    pub value: Value,
}

impl CommitObj {
    pub fn decode(b: &[u8]) -> anyhow::Result<CommitObj> {
        let v = Value::decode(b)?;
        Ok(CommitObj {
            did: v.get("did").and_then(|x| x.as_str()).unwrap_or("").into(),
            rev: v.get("rev").and_then(|x| x.as_str()).unwrap_or("").into(),
            data: link(v.get("data")).ok_or_else(|| anyhow::anyhow!("commit without data"))?,
            version: match v.get("version") {
                Some(Value::Int(i)) => *i,
                _ => 0,
            },
            prev: link(v.get("prev")),
            sig: match v.get("sig") {
                Some(Value::Bytes(s)) => s.clone(),
                _ => vec![],
            },
            value: v,
        })
    }

    /// The unsigned commit bytes (commit object minus `sig`).
    pub fn unsigned_bytes(&self) -> Vec<u8> {
        match &self.value {
            Value::Map(m) => {
                Value::Map(m.iter().filter(|(k, _)| k != "sig").cloned().collect()).to_cbor()
            }
            _ => panic!("commit is not a map"),
        }
    }

    /// Verifies the signature (low-S ES256K over sha256(unsigned bytes)).
    pub fn verify(&self, key: &k256::ecdsa::VerifyingKey) -> anyhow::Result<()> {
        use k256::ecdsa::signature::Verifier;
        let sig = k256::ecdsa::Signature::from_slice(&self.sig)?;
        anyhow::ensure!(sig.normalize_s().is_none(), "signature is not low-S");
        key.verify(&self.unsigned_bytes(), &sig)?;
        Ok(())
    }
}

pub fn decode_k256_multibase(s: &str) -> anyhow::Result<k256::ecdsa::VerifyingKey> {
    let b = bs58::decode(
        s.strip_prefix('z')
            .ok_or_else(|| anyhow::anyhow!("not base58btc"))?,
    )
    .into_vec()?;
    anyhow::ensure!(
        b.len() == 35 && b[0] == 0xe7 && b[1] == 0x01,
        "not a secp256k1 multikey"
    );
    Ok(k256::ecdsa::VerifyingKey::from_sec1_bytes(&b[2..])?)
}

pub fn decode_did_key_k256(did: &str) -> anyhow::Result<k256::ecdsa::VerifyingKey> {
    decode_k256_multibase(
        did.strip_prefix("did:key:")
            .ok_or_else(|| anyhow::anyhow!("not did:key"))?,
    )
}

/// A parsed repo CAR (getRepo / getRecord / getBlocks / firehose blocks).
pub struct Repo {
    pub root: Cid,
    pub blocks: HashMap<Cid, Vec<u8>>,
    pub order: Vec<Cid>,
}

impl Repo {
    pub fn from_car(b: &[u8]) -> anyhow::Result<Repo> {
        let (roots, blocks) = vlpds::car::read_car(b)?;
        let order = blocks.iter().map(|(c, _)| *c).collect();
        Ok(Repo {
            root: *roots
                .first()
                .ok_or_else(|| anyhow::anyhow!("CAR without root"))?,
            blocks: blocks.into_iter().map(|(c, d)| (c, d.to_vec())).collect(),
            order,
        })
    }

    pub fn commit(&self) -> CommitObj {
        CommitObj::decode(self.blocks.get(&self.root).expect("root block")).expect("decode commit")
    }

    pub fn tree(&self) -> Tree {
        Tree::load_from_blocks(&self.blocks, self.commit().data).expect("load MST")
    }

    /// All (path, cid) entries of a complete repo.
    pub fn entries(&self) -> Vec<(String, Cid)> {
        let mut out = Vec::new();
        self.tree()
            .walk(&mut |k, v| out.push((String::from_utf8_lossy(k).to_string(), v)));
        out
    }

    pub fn record(&self, path: &str) -> Option<J> {
        let c = self.tree().get(path.as_bytes()).ok()??;
        self.blocks
            .get(&c)
            .map(|b| Value::decode(b).unwrap().to_json())
    }

    /// Every block's CID matches its content hash.
    pub fn check_block_hashes(&self) -> anyhow::Result<()> {
        for (c, b) in &self.blocks {
            let want = if c.codec == vlpds::cid::CODEC_RAW {
                Cid::raw(b)
            } else {
                Cid::dag_cbor(b)
            };
            anyhow::ensure!(*c == want, "block {c} hashes to {want}");
        }
        Ok(())
    }
}

/// Checks that `path` maps to `cid` (or is absent when None) in the proof CAR
/// returned by sync.getRecord, and that the commit is signed by `key`.
pub fn verify_record_proof(
    car: &[u8],
    did: &str,
    path: &str,
    key: Option<&k256::ecdsa::VerifyingKey>,
) -> anyhow::Result<Option<Cid>> {
    let repo = Repo::from_car(car)?;
    repo.check_block_hashes()?;
    let c = repo.commit();
    anyhow::ensure!(c.did == did, "commit did {} != {did}", c.did);
    if let Some(k) = key {
        c.verify(k)?;
    }
    let tree = Tree::load_from_blocks(&repo.blocks, c.data).map_err(|e| anyhow::anyhow!("{e}"))?;
    let got = tree
        .get(path.as_bytes())
        .map_err(|e| anyhow::anyhow!("proof incomplete for {path}: {e}"))?;
    if let Some(cid) = got {
        anyhow::ensure!(
            repo.blocks.contains_key(&cid),
            "record block {cid} missing from proof CAR"
        );
    }
    Ok(got)
}

pub fn is_tid(s: &str) -> bool {
    s.len() == 13
        && s.bytes().enumerate().all(|(i, b)| {
            let ok = matches!(b, b'2'..=b'7' | b'a'..=b'z');
            ok && (i != 0 || matches!(b, b'2'..=b'7' | b'a'..=b'j'))
        })
}

/// Waits until `f` returns Some or the timeout passes.
pub async fn eventually<T, F: std::future::Future<Output = Option<T>>>(
    timeout: Duration,
    mut f: impl FnMut() -> F,
) -> Option<T> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f().await {
            return Some(v);
        }
        if tokio::time::Instant::now() > deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub const SEC: Duration = Duration::from_secs(1);
pub const FH_TIMEOUT: Duration = Duration::from_secs(10);

pub fn fixture_path(rel: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join(rel)
}

pub fn read_fixture(rel: &str) -> String {
    std::fs::read_to_string(fixture_path(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"))
}

/// Non-comment, non-empty lines of an interop syntax fixture.
pub fn fixture_lines(rel: &str) -> Vec<String> {
    read_fixture(rel)
        .lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        .map(String::from)
        .collect()
}

/// A minimal 1x1 PNG.
pub const PNG_1X1: &[u8] = &[
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
    0x89, 0x00, 0x00, 0x00, 0x0a, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00,
    0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae,
    0x42, 0x60, 0x82,
];

// ---- ref group A helpers ----

/// The newest dev-mode mail sent to `email` (vlpds.admin.getDevMail):
/// `{to, subject, body, html, purpose, token, sentAt}`.
pub async fn latest_mail(s: &TestServer, email: &str) -> J {
    let j = s.dev_mail(email).await.ok();
    j["messages"].as_array().and_then(|m| m.last().cloned()).unwrap_or_else(|| panic!("no mail to {email}: {j}"))
}

/// Moves the stored email token for `purpose` `ms` milliseconds into the
/// past (the reference tests rewrite `email_token.requestedAt`).
pub async fn age_email_token(s: &TestServer, did: &str, purpose: &str, ms: u64) {
    let name = format!("etok/{purpose}");
    let raw = s.app.get_private(did, &name).await.ok().flatten().unwrap_or_else(|| panic!("no stored {name} token"));
    let mut rec: J = serde_json::from_slice(&raw).unwrap();
    rec["requested_at"] = json!(rec["requested_at"].as_u64().unwrap() - ms);
    s.app
        .put_private(
            did,
            vec![vlpds::segment::Mutation {
                key: vlpds::state::private_key(did, &name).into(),
                val: Some(serde_json::to_vec(&rec).unwrap().into()),
            }],
        )
        .await
        .unwrap_or_else(|e| panic!("put_private: {}", e.message));
}

/// admin.updateSubjectStatus on a repoRef with `takedown: {applied}`.
pub async fn set_repo_takedown(s: &TestServer, did: &str, applied: bool) {
    s.xrpc
        .post(
            "com.atproto.admin.updateSubjectStatus",
            &json!({"subject": {"$type": "com.atproto.admin.defs#repoRef", "did": did}, "takedown": {"applied": applied}}),
            &Auth::Admin,
        )
        .await
        .ok();
}
