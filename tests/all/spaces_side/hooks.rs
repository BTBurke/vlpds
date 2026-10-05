//! Crash harness pieces for the space durability tests:
//!
//! - [`HookedStore`]: one node's view of a shared in-memory bucket that can
//!   hold or fail the PUT of a log segment carrying given bytes (before or
//!   after it lands), and "die" like a killed process: every call after
//!   [`HookedStore::kill`] hangs, so nothing the node had in flight lands
//!   later (the pattern of reshard.rs's Killable and fast_failover.rs's
//!   LeaseHooks).
//! - [`Front`]: a stable address in front of whichever incarnation is live.
//!   OAuth binds tokens and DPoP proofs to the PDS's public URL, so every
//!   incarnation is configured with the front's URL and clients keep it
//!   across a restart.
//! - [`StubDid`]: a did:web on loopback (a remote space authority, writer or
//!   syncer) that records the notifyWrite calls reaching it and can refuse
//!   them.

use crate::common::*;
use object_store::path::Path;
use object_store::ObjectStore as _;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions, PutOptions, PutPayload,
    PutResult,
};
use parking_lot::Mutex;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// The segment never reaches the bucket.
    BeforePut,
    /// The segment is in the bucket; the node hasn't heard back.
    AfterPut,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Act {
    /// Hold the call until released (or forever, past a kill).
    Pause,
    /// Answer the call with an error.
    Fail,
}

struct Hook {
    stage: Stage,
    act: Act,
    needle: Vec<u8>,
    reached: Option<tokio::sync::oneshot::Sender<()>>,
    go: Option<tokio::sync::oneshot::Receiver<()>>,
}

/// An armed hook: `reached` fires when a matching segment PUT gets to the
/// hook. A paused PUT carries on once the `Armed` is dropped (into a dead
/// bucket's hang, past a kill).
pub struct Armed {
    pub reached: tokio::sync::oneshot::Receiver<()>,
    _go: tokio::sync::oneshot::Sender<()>,
}

impl Armed {
    pub async fn wait(&mut self, what: &str) {
        tokio::time::timeout(Duration::from_secs(20), &mut self.reached)
            .await
            .unwrap_or_else(|_| panic!("{what}: no matching segment PUT"))
            .expect("hook dropped");
    }
}

#[derive(Debug)]
pub struct HookedStore {
    pub inner: Arc<object_store::memory::InMemory>,
    dead: AtomicBool,
    hooks: Mutex<Vec<Hook>>,
}

impl std::fmt::Debug for Hook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hook({:?} {:?})", self.stage, self.act)
    }
}

impl std::fmt::Display for HookedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HookedStore")
    }
}

fn is_segment(location: &Path) -> bool {
    let p = location.as_ref();
    p.contains("/log/") && p.ends_with(".seg")
}

/// Whether some entry of the segment carries `needle` in a mutation's key or
/// value. Segments are compressed, so the bytes are looked for decoded.
fn segment_carries(payload: &PutPayload, needle: &[u8]) -> bool {
    let data = bytes::Bytes::from(payload.clone());
    let Ok(vlpds::segment::LogObject::Segment(_, entries)) = vlpds::segment::parse(data, true, None) else {
        return false;
    };
    let has = |b: &[u8]| b.windows(needle.len()).any(|w| w == needle);
    entries.iter().any(|e| e.muts.iter().any(|m| has(&m.key) || m.val.as_deref().is_some_and(has)))
}

impl HookedStore {
    pub fn new(inner: &Arc<object_store::memory::InMemory>) -> Arc<HookedStore> {
        Arc::new(HookedStore { inner: inner.clone(), dead: AtomicBool::new(false), hooks: Default::default() })
    }

    /// Catches the next log segment PUT whose entries carry `needle`.
    pub fn arm(&self, stage: Stage, act: Act, needle: impl AsRef<[u8]>) -> Armed {
        let ((reached_tx, reached), (go, go_rx)) = (tokio::sync::oneshot::channel(), tokio::sync::oneshot::channel());
        self.hooks.lock().push(Hook {
            stage,
            act,
            needle: needle.as_ref().to_vec(),
            reached: Some(reached_tx),
            go: Some(go_rx),
        });
        Armed { reached, _go: go }
    }

    /// kill -9 as the bucket sees it: from now on every call hangs.
    pub fn kill(&self) {
        self.dead.store(true, Ordering::SeqCst);
    }

    pub fn dead(&self) -> bool {
        self.dead.load(Ordering::SeqCst)
    }

    async fn gate(&self) {
        if self.dead() {
            futures::future::pending::<()>().await;
        }
    }

    fn take_hook(&self, location: &Path, payload: &PutPayload) -> Option<Hook> {
        if !is_segment(location) {
            return None;
        }
        let mut hooks = self.hooks.lock();
        let i = hooks.iter().position(|h| segment_carries(payload, &h.needle))?;
        Some(hooks.remove(i))
    }

    async fn fire(&self, mut h: Hook) -> object_store::Result<()> {
        if let Some(r) = h.reached.take() {
            let _ = r.send(());
        }
        match h.act {
            Act::Pause => {
                let _ = h.go.take().unwrap().await;
                self.gate().await;
                Ok(())
            }
            Act::Fail => {
                Err(object_store::Error::Generic { store: "HookedStore", source: "injected PUT failure".into() })
            }
        }
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for HookedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.gate().await;
        match self.take_hook(location, &payload) {
            Some(h) if h.stage == Stage::BeforePut => {
                self.fire(h).await?;
                self.put_opts_inner(location, payload, opts).await
            }
            Some(h) => {
                let r = self.put_opts_inner(location, payload, opts).await;
                self.fire(h).await?;
                r
            }
            None => self.put_opts_inner(location, payload, opts).await,
        }
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.gate().await;
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.gate().await;
        let r = self.inner.get_opts(location, options).await;
        self.gate().await;
        r
    }
    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        use futures::StreamExt;
        if self.dead() {
            return futures::stream::pending().boxed();
        }
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        use futures::StreamExt;
        if self.dead() {
            return futures::stream::pending().boxed();
        }
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.gate().await;
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: object_store::CopyOptions) -> object_store::Result<()> {
        self.gate().await;
        self.inner.copy_opts(from, to, options).await
    }
}

impl HookedStore {
    async fn put_opts_inner(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.gate().await;
        let r = self.inner.put_opts(location, payload, opts).await;
        self.gate().await;
        r
    }
}

/// A TCP forwarder on a fixed loopback address to whichever backend it
/// points at; repointing cuts every connection to the old one.
pub struct Front {
    pub url: String,
    target: Arc<Mutex<Option<SocketAddr>>>,
    conns: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl Front {
    pub async fn new() -> Front {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        let (target, conns) =
            (Arc::new(Mutex::new(None::<SocketAddr>)), Arc::new(Mutex::new(Vec::<tokio::task::JoinHandle<()>>::new())));
        let (t, c) = (target.clone(), conns.clone());
        tokio::spawn(async move {
            while let Ok((mut down, _)) = l.accept().await {
                let Some(to) = *t.lock() else { continue };
                let job = tokio::spawn(async move {
                    if let Ok(mut up) = tokio::net::TcpStream::connect(to).await {
                        let _ = tokio::io::copy_bidirectional(&mut down, &mut up).await;
                    }
                });
                let mut c = c.lock();
                c.retain(|j| !j.is_finished());
                c.push(job);
            }
        });
        Front { url, target, conns }
    }

    pub fn point(&self, s: &TestServer) {
        *self.target.lock() = Some(s.addr);
        for j in self.conns.lock().drain(..) {
            j.abort();
        }
    }

    /// `s` as clients see it: the same node, at the front's URL.
    pub fn view(&self, s: &TestServer) -> TestServer {
        TestServer {
            app: s.app.clone(),
            addr: s.addr,
            url: self.url.clone(),
            peer_url: s.peer_url.clone(),
            xrpc: Xrpc::new(&self.url),
        }
    }
}

/// A cluster node on its own [`HookedStore`] over `bucket`, with `--spaces`
/// on and `front`'s URL as its public URL.
pub async fn hooked_node(
    id: &str,
    bucket: &Arc<object_store::memory::InMemory>,
    shards: u32,
    front: &Front,
) -> (TestServer, Arc<HookedStore>) {
    hooked_node_with(id, bucket, shards, front, |_| {}).await
}

/// [`hooked_node`], then `f` adjusts the config.
pub async fn hooked_node_with(
    id: &str,
    bucket: &Arc<object_store::memory::InMemory>,
    shards: u32,
    front: &Front,
    f: impl FnOnce(&mut vlpds::server::Config),
) -> (TestServer, Arc<HookedStore>) {
    let store = HookedStore::new(bucket);
    let public = front.url.clone();
    let s = cluster_node(id, store.clone(), shards, move |c| {
        c.spaces = true;
        c.public_url = public;
        f(c);
    })
    .await;
    (s, store)
}

/// kill -9: the bucket stops answering and the node drops its shards. Its
/// listener lives on (in process), so clients must already be elsewhere.
pub fn kill9(s: &TestServer, store: &HookedStore) {
    store.kill();
    s.app.node.halt();
}

/// One notifyWrite call that reached a [`StubDid`].
#[derive(Clone, Debug)]
pub struct Notified {
    pub body: J,
    /// The service JWT's claims (unverified).
    pub claims: J,
    pub accepted: bool,
    pub at: Instant,
}

pub struct StubDid {
    pub did: String,
    pub key: Arc<vlpds::crypto::Keypair>,
    refuse: Arc<AtomicBool>,
    seen: Arc<Mutex<Vec<Notified>>>,
}

fn jwt_claims(authorization: Option<&str>) -> J {
    use base64::Engine;
    let payload = authorization.and_then(|a| a.strip_prefix("Bearer ")).and_then(|t| t.split('.').nth(1));
    payload
        .and_then(|p| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(p).ok())
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or(J::Null)
}

impl StubDid {
    /// A did:web whose document names its #atproto key and points
    /// #atproto_pds, #atproto_space_host and #atproto_space_syncer at itself.
    pub async fn spawn() -> StubDid {
        use axum::response::IntoResponse;
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let (did, base) = (format!("did:web:127.0.0.1%3A{}", addr.port()), format!("http://{addr}"));
        let key = Arc::new(vlpds::crypto::Keypair::generate());
        let doc = json!({
            "@context": ["https://www.w3.org/ns/did/v1", "https://w3id.org/security/multikey/v1"],
            "id": did,
            "verificationMethod": [{
                "id": format!("{did}#atproto"),
                "type": "Multikey",
                "controller": did,
                "publicKeyMultibase": key.did_key().strip_prefix("did:key:").unwrap(),
            }],
            "service": [
                {"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": base},
                {"id": "#atproto_space_host", "type": "AtprotoSpaceHost", "serviceEndpoint": base},
                {"id": "#atproto_space_syncer", "type": "AtprotoSpaceSyncer", "serviceEndpoint": base},
            ],
        });
        let (refuse, seen) = (Arc::new(AtomicBool::new(false)), Arc::new(Mutex::new(Vec::<Notified>::new())));
        let (r, s) = (refuse.clone(), seen.clone());
        let router = axum::Router::new()
            .route("/.well-known/did.json", axum::routing::get(move || std::future::ready(axum::Json(doc.clone()))))
            .route(
                "/xrpc/com.atproto.space.notifyWrite",
                axum::routing::post(move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                    let (r, s) = (r.clone(), s.clone());
                    async move {
                        let accepted = !r.load(Ordering::SeqCst);
                        let auth = headers.get("authorization").and_then(|v| v.to_str().ok());
                        s.lock().push(Notified {
                            body: serde_json::from_slice(&body).unwrap_or(J::Null),
                            claims: jwt_claims(auth),
                            accepted,
                            at: Instant::now(),
                        });
                        match accepted {
                            true => axum::Json(json!({})).into_response(),
                            false => (
                                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                                axum::Json(json!({"error": "InternalServerError", "message": "refusing"})),
                            )
                                .into_response(),
                        }
                    }
                }),
            );
        tokio::spawn(async move { axum::serve(l, router).await.unwrap() });
        StubDid { did, key, refuse, seen }
    }

    /// Answer notifyWrite with a 503 (retryable) while `on`.
    pub fn refuse(&self, on: bool) {
        self.refuse.store(on, Ordering::SeqCst);
    }

    pub fn seen(&self) -> Vec<Notified> {
        self.seen.lock().clone()
    }

    pub fn accepted(&self) -> Vec<Notified> {
        self.seen().into_iter().filter(|n| n.accepted).collect()
    }

    /// A service JWT from this DID for `aud`/`lxm`.
    pub fn service_jwt(&self, aud: &str, lxm: &str) -> String {
        vlpds::auth::service_auth_jwt(&self.key, &self.did, aud, Some(lxm), 60).unwrap()
    }
}
