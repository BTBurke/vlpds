//! Handle domains added and removed at runtime (DESIGN.md "Handle
//! domains"). They live in the bucket object
//! `{prefix}/config/handle-domains.json`, next to the configured ones
//! (`--handle-domains`, which the API can't change). Every node re-reads it
//! every [`REFRESH_EVERY`] and when the node that changed it asks.
//!
//! Adding a domain scans every shard's accounts first: it is refused while a
//! node can't answer (unreachable, or a build without this endpoint) or an
//! account's handle would move under it. Removing takes two calls: the
//! first marks the domain retiring (it serves the handles under it but gives
//! out no new ones); after the grace period a second call removes it, once a
//! scan of every shard finds no account still holding a handle under it.

use super::admin::require_admin;
use super::internal::HDR as INTERNAL_HDR;
use super::*;
use crate::handle_domains::{normalize, HandleDomains, Source, State as DomainState};
use crate::slots::ShardId;
use object_store::GetOptions;
use serde::Serialize;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

pub const REFRESH_EVERY: Duration = Duration::from_secs(10);
const STORE_TIMEOUT: Duration = Duration::from_secs(5);
const CAS_RETRIES: usize = 5;
const RELOAD_TIMEOUT: Duration = Duration::from_secs(5);
/// Each peer's scan of its shards' accounts.
const SCAN_TIMEOUT: Duration = Duration::from_secs(120);
/// Accounts named in a refusal.
const SAMPLE: usize = 10;
const INITIAL_LOAD_TRIES: u32 = 5;

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route("/xrpc/vlpds.admin.getHandleDomains", get(get_handle_domains))
        .route("/xrpc/vlpds.admin.addHandleDomain", post(add_handle_domain))
        .route("/xrpc/vlpds.admin.removeHandleDomain", post(remove_handle_domain))
}

pub fn internal_routes() -> Router<Arc<App>> {
    Router::new()
        .route("/internal/v1/handle-domains/accounts", get(internal_scan))
        .route("/internal/v1/handle-domains/reload", post(internal_reload))
}

/// The stored object. Order is the order domains were added in.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Doc {
    #[serde(default)]
    pub domains: Vec<Entry>,
    #[serde(default, flatten)]
    pub extra: serde_json::Map<String, J>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    /// Normalized (`crate::handle_domains::normalize`).
    pub domain: String,
    pub state: DomainState,
    /// RFC 3339.
    pub added_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retiring_since: Option<String>,
    #[serde(default, flatten)]
    pub extra: serde_json::Map<String, J>,
}

/// This node's view: the configured domains plus the stored ones.
pub struct Live {
    config: Arc<HandleDomains>,
    current: parking_lot::RwLock<Arc<HandleDomains>>,
    /// The ETag of the object last applied (None: absent).
    etag: parking_lot::Mutex<Option<String>>,
    io: tokio::sync::Mutex<()>,
    wake: tokio::sync::Notify,
    started: AtomicBool,
}

impl Live {
    pub fn new(config: HandleDomains) -> Live {
        let config = Arc::new(config);
        Live {
            current: parking_lot::RwLock::new(config.clone()),
            config,
            etag: Default::default(),
            io: Default::default(),
            wake: Default::default(),
            started: AtomicBool::new(false),
        }
    }

    pub fn current(&self) -> Arc<HandleDomains> {
        self.current.read().clone()
    }

    pub fn configured(&self) -> &HandleDomains {
        &self.config
    }

    /// An entry that doesn't normalize is skipped (and logged), so one bad
    /// edit of the object can't take the others down.
    fn apply(&self, doc: &Doc, etag: Option<String>) {
        let managed = doc.domains.iter().filter_map(|e| match normalize(&e.domain) {
            Ok(n) => Some((n, e.state)),
            Err(err) => {
                tracing::warn!("handle-domains.json: entry skipped: {err}");
                None
            }
        });
        *self.current.write() = Arc::new(self.config.with_managed(managed));
        *self.etag.lock() = etag;
    }
}

fn path(store: &Store) -> object_store::path::Path {
    object_store::path::Path::from(format!("{}/config/handle-domains.json", store.prefix))
}

async fn bounded<T>(f: impl std::future::Future<Output = object_store::Result<T>>) -> anyhow::Result<T> {
    match tokio::time::timeout(STORE_TIMEOUT, f).await {
        Ok(r) => Ok(r?),
        Err(_) => anyhow::bail!("handle domains config call timed out"),
    }
}

/// The stored doc (empty when absent) and its ETag.
pub async fn load(store: &Store) -> anyhow::Result<(Doc, Option<String>)> {
    let got = bounded(async {
        let r = store.raw.get_opts(&path(store), GetOptions::default()).await?;
        let e = r.meta.e_tag.clone();
        Ok((r.bytes().await?, e))
    })
    .await;
    match got {
        Ok((b, e)) => {
            Ok((serde_json::from_slice(&b).map_err(|err| anyhow::anyhow!("handle-domains.json unreadable: {err}"))?, e))
        }
        Err(e) if matches!(e.downcast_ref::<object_store::Error>(), Some(object_store::Error::NotFound { .. })) => {
            Ok((Doc::default(), None))
        }
        Err(e) => Err(e),
    }
}

/// Re-reads the object and installs it when it changed.
pub async fn refresh(live: &Live, store: &Store) -> anyhow::Result<()> {
    let _io = live.io.lock().await;
    let (doc, etag) = load(store).await?;
    if etag.is_some() && *live.etag.lock() == etag {
        return Ok(());
    }
    live.apply(&doc, etag);
    Ok(())
}

/// At startup, before the node serves: a handle under a stored domain must
/// not look unknown. Gives up (the node doesn't start) after a few tries.
pub async fn load_initial(live: &Live, store: &Store) -> anyhow::Result<()> {
    let mut wait = Duration::from_millis(200);
    for attempt in 1..=INITIAL_LOAD_TRIES {
        match refresh(live, store).await {
            Ok(()) => return Ok(()),
            Err(e) if attempt == INITIAL_LOAD_TRIES => {
                return Err(e.context("reading the stored handle domains (config/handle-domains.json)"))
            }
            Err(e) => tracing::warn!("reading the stored handle domains failed (retrying): {e:#}"),
        }
        tokio::time::sleep(wait).await;
        wait *= 2;
    }
    unreachable!()
}

/// Idempotent.
pub fn start(app: &Arc<App>) {
    let live = app.handle_domain_set.clone();
    if live.started.swap(true, Ordering::AcqRel) {
        return;
    }
    let weak = Arc::downgrade(&live);
    let store = app.store.clone();
    tokio::spawn(async move {
        loop {
            let Some(l) = weak.upgrade() else { return };
            if let Err(e) = refresh(&l, &store).await {
                tracing::warn!("handle domains refresh failed (keeping the last ones read): {e:#}");
            }
            tokio::select! {
                _ = tokio::time::sleep(REFRESH_EVERY) => {}
                _ = l.wake.notified() => {}
            }
        }
    });
}

fn upstream(e: anyhow::Error) -> XrpcError {
    XrpcError { status: StatusCode::BAD_GATEWAY, error: "UpstreamFailure".into(), message: format!("{e:#}") }
}

/// Read-modify-write under CAS, retried on a concurrent change. `f` returns
/// whether to write.
async fn update<T>(store: &Store, f: impl Fn(&mut Doc) -> XResult<(bool, T)>) -> XResult<T> {
    for _ in 0..CAS_RETRIES {
        let (mut doc, etag) = load(store).await.map_err(upstream)?;
        let (write, out) = f(&mut doc)?;
        if !write {
            return Ok(out);
        }
        let mode = match etag {
            Some(e) => crate::cluster::if_match(Some(e)),
            None => PutMode::Create,
        };
        let body = serde_json::to_vec_pretty(&doc).map_err(XrpcError::from_err)?;
        let put = bounded(store.raw.put_opts(&path(store), PutPayload::from(body), PutOptions::from(mode))).await;
        match put {
            Ok(_) => return Ok(out),
            Err(e)
                if matches!(
                    e.downcast_ref::<object_store::Error>(),
                    Some(object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. })
                ) =>
            {
                continue
            }
            Err(e) => return Err(upstream(e)),
        }
    }
    Err(XrpcError {
        status: StatusCode::CONFLICT,
        error: "ConfigConflict".into(),
        message: "handle domains changed concurrently, retry".into(),
    })
}

/// This node now, then every peer (each also re-reads within
/// [`REFRESH_EVERY`] on its own).
async fn apply_everywhere(app: &App) {
    if let Err(e) = refresh(&app.handle_domain_set, &app.store).await {
        tracing::warn!("handle domains refresh failed: {e:#}");
    }
    let Some(c) = &app.cluster else { return };
    let me = c.cfg.node_id.clone();
    let sends = c.peers().into_iter().filter(|l| l.node_id != me).map(|l| async move {
        let r = app
            .http
            .post(format!("{}/internal/v1/handle-domains/reload", l.addr.trim_end_matches('/')))
            .header(INTERNAL_HDR, &app.config.internal_token)
            .timeout(RELOAD_TIMEOUT)
            .send()
            .await
            .and_then(|r| r.error_for_status());
        if let Err(e) = r {
            tracing::warn!(peer = %l.node_id, "handle domains reload nudge failed (it re-reads on its own): {e}");
        }
    });
    futures::future::join_all(sends).await;
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn removable_after(since: &str, grace: Duration) -> Option<String> {
    let t = chrono::DateTime::parse_from_rfc3339(since).ok()?.with_timezone(&chrono::Utc);
    let grace = chrono::Duration::from_std(grace).ok()?;
    Some((t + grace).to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

fn entry_view(e: &Entry, grace: Duration) -> J {
    let mut v = json!({"domain": e.domain, "source": Source::Managed, "state": e.state, "addedAt": e.added_at});
    if let Some(since) = &e.retiring_since {
        v["retiringSince"] = json!(since);
        v["removableAfter"] = json!(removable_after(since, grace));
    }
    v
}

fn configured_view(domain: &str) -> J {
    json!({"domain": domain, "source": Source::Config, "state": DomainState::Active})
}

async fn get_handle_domains(State(app): AppState, Auth(creds): Auth) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let (doc, _) = load(&app.store).await.map_err(upstream)?;
    let configured = app.handle_domain_set.configured();
    let grace = app.config.handle_domain_retire_grace;
    let mut out: Vec<J> = configured.domains().iter().map(|d| configured_view(&d.name)).collect();
    out.extend(doc.domains.iter().filter(|e| !configured.contains(&e.domain)).map(|e| entry_view(e, grace)));
    Ok(Json(json!({"domains": out, "retireGraceSecs": grace.as_secs()})))
}

#[derive(Deserialize)]
struct DomainIn {
    domain: String,
}

fn parse_domain(raw: &str) -> XResult<String> {
    normalize(raw).map_err(|e| XrpcError::bad("InvalidRequest", e))
}

async fn add_handle_domain(State(app): AppState, Auth(creds): Auth, Json(inp): Json<DomainIn>) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let domain = parse_domain(&inp.domain)?;
    if app.handle_domain_set.configured().contains(&domain) {
        return Ok(Json(configured_view(&domain)));
    }
    let (doc, _) = load(&app.store).await.map_err(upstream)?;
    let known = doc.domains.iter().any(|e| e.domain == domain);
    if !known {
        // every node answers, and no account's handle moves under it
        let found = scan(&app, &domain, Purpose::Add).await?;
        if found.count > 0 {
            return Err(XrpcError::bad(
                "HandleDomainInUse",
                format!(
                    "{} account(s) hold handles that would come under {domain} ({}); change or delete them first",
                    found.count,
                    found.names()
                ),
            ));
        }
    }
    let d = domain.clone();
    let entry = update(&app.store, move |doc| {
        match doc.domains.iter_mut().find(|e| e.domain == d) {
            Some(e) if e.state == DomainState::Active => Ok((false, e.clone())),
            // retiring: back in service
            Some(e) => {
                e.state = DomainState::Active;
                e.retiring_since = None;
                Ok((true, e.clone()))
            }
            None if known => Err(XrpcError {
                status: StatusCode::CONFLICT,
                error: "ConfigConflict".into(),
                message: format!("{d} was removed meanwhile, retry"),
            }),
            None => {
                let e = Entry {
                    domain: d.clone(),
                    state: DomainState::Active,
                    added_at: now_rfc3339(),
                    retiring_since: None,
                    extra: Default::default(),
                };
                doc.domains.push(e.clone());
                Ok((true, e))
            }
        }
    })
    .await?;
    apply_everywhere(&app).await;
    tracing::info!(%domain, "handle domain added");
    Ok(Json(entry_view(&entry, app.config.handle_domain_retire_grace)))
}

async fn remove_handle_domain(State(app): AppState, Auth(creds): Auth, Json(inp): Json<DomainIn>) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let domain = parse_domain(&inp.domain)?;
    if app.handle_domain_set.configured().contains(&domain) {
        return Err(XrpcError::bad(
            "HandleDomainConfigured",
            format!("{domain} is set by --handle-domains; remove it from the node configuration instead"),
        ));
    }
    let grace = app.config.handle_domain_retire_grace;
    let (doc, _) = load(&app.store).await.map_err(upstream)?;
    let Some(entry) = doc.domains.iter().find(|e| e.domain == domain).cloned() else {
        return Err(XrpcError::bad("HandleDomainNotFound", format!("{domain} is not a managed handle domain")));
    };
    if entry.state == DomainState::Active {
        // step 1: no new handles under it anywhere
        let d = domain.clone();
        let entry = update(&app.store, move |doc| match doc.domains.iter_mut().find(|e| e.domain == d) {
            Some(e) if e.state == DomainState::Active => {
                e.state = DomainState::Retiring;
                e.retiring_since = Some(now_rfc3339());
                Ok((true, e.clone()))
            }
            Some(e) => Ok((false, e.clone())),
            None => Err(XrpcError::bad("HandleDomainNotFound", format!("{d} is not a managed handle domain"))),
        })
        .await?;
        apply_everywhere(&app).await;
        tracing::info!(%domain, "handle domain retiring");
        let mut v = entry_view(&entry, grace);
        // a first look; the removal scans again
        v["blockingAccounts"] = match scan(&app, &domain, Purpose::Remove).await {
            Ok(f) => json!(f.count),
            Err(_) => J::Null,
        };
        return Ok(Json(v));
    }
    // step 2: after the grace period, with no account left under it
    let since = entry.retiring_since.clone().unwrap_or_default();
    let after = removable_after(&since, grace);
    let due = after
        .as_deref()
        .and_then(|a| chrono::DateTime::parse_from_rfc3339(a).ok())
        .is_none_or(|a| chrono::Utc::now() >= a);
    if !due {
        return Err(XrpcError::bad(
            "HandleDomainRetiring",
            format!("{domain} is retiring; it can be removed after {}", after.unwrap_or_default()),
        ));
    }
    refresh(&app.handle_domain_set, &app.store).await.map_err(upstream)?;
    let found = scan(&app, &domain, Purpose::Remove).await?;
    if found.count > 0 {
        return Err(XrpcError::bad(
            "HandleDomainInUse",
            format!(
                "{} account(s) still hold handles under {domain} ({}); migrate, rename or delete them first",
                found.count,
                found.names()
            ),
        ));
    }
    let d = domain.clone();
    update(&app.store, move |doc| {
        let before = doc.domains.len();
        doc.domains.retain(|e| {
            !(e.domain == d && e.state == DomainState::Retiring && e.retiring_since == entry.retiring_since)
        });
        if doc.domains.len() == before {
            return Err(XrpcError {
                status: StatusCode::CONFLICT,
                error: "ConfigConflict".into(),
                message: format!("{d} changed meanwhile, retry"),
            });
        }
        Ok((true, ()))
    })
    .await?;
    apply_everywhere(&app).await;
    tracing::info!(%domain, "handle domain removed");
    Ok(Json(json!({"domain": domain, "source": Source::Managed, "state": "removed"})))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Purpose {
    Add,
    Remove,
}

/// Whether the account holding `handle` stands in the way of adding or
/// removing `domain`, under this node's domains `set`.
fn blocks(set: &HandleDomains, domain: &str, purpose: Purpose, handle: &str) -> bool {
    match purpose {
        // it would come under the new domain (or is the domain itself)
        Purpose::Add => {
            handle == domain
                || (handle.strip_suffix(domain).is_some_and(|f| f.len() > 1 && f.ends_with('.'))
                    && set.longest_match(handle).is_none_or(|d| d.len() < domain.len()))
        }
        Purpose::Remove => set.longest_match(handle) == Some(domain),
    }
}

/// The `#atproto_pds` endpoint of a DID document.
fn pds_endpoint(doc: &J) -> Option<&str> {
    doc["service"].as_array()?.iter().find(|s| s["id"].as_str().is_some_and(|id| id.ends_with("#atproto_pds")))?
        ["serviceEndpoint"]
        .as_str()
}

/// A deactivated account whose DID document now names another PDS has
/// moved away; its old handle here no longer matters. Unresolvable: not
/// known to have moved.
async fn migrated_away(app: &App, did: &str) -> bool {
    match app.did_resolver.resolve(did).await {
        Ok(doc) => pds_endpoint(&doc).is_some_and(|e| e.trim_end_matches('/') != app.public_url.trim_end_matches('/')),
        Err(_) => false,
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Found {
    count: u64,
    sample: Vec<Hit>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Hit {
    did: String,
    handle: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    status: Option<String>,
}

impl Found {
    fn add(&mut self, other: Found) {
        self.count += other.count;
        self.sample.extend(other.sample);
        self.sample.truncate(SAMPLE);
    }

    fn names(&self) -> String {
        let mut s = self.sample.iter().map(|h| format!("{} {}", h.handle, h.did)).collect::<Vec<_>>().join(", ");
        if self.count > self.sample.len() as u64 {
            s.push_str(", ...");
        }
        s
    }
}

/// This node's shards (deleted accounts don't count).
async fn scan_local(app: &App, domain: &str, purpose: Purpose) -> XResult<(Found, Vec<ShardId>)> {
    let set = app.handle_domains();
    let owned = app.partitions.owned();
    let ids = owned.iter().map(|p| p.id).collect();
    let mut found = Found::default();
    for p in owned {
        let mut iter = state::FamilyScan::new(p.db.as_ref(), state::ACCOUNT_FAMILY, None, &Default::default())
            .await
            .map_err(XrpcError::from_err)?;
        while let Some(kv) = iter.next().await.map_err(XrpcError::from_err)? {
            let Ok(a) = serde_json::from_slice::<Account>(&kv.value) else { continue };
            if a.status.as_deref() == Some("deleted") || !blocks(&set, domain, purpose, &a.handle) {
                continue;
            }
            if purpose == Purpose::Remove
                && a.status.as_deref() == Some("deactivated")
                && migrated_away(app, &a.did).await
            {
                continue;
            }
            found.count += 1;
            if found.sample.len() < SAMPLE {
                found.sample.push(Hit { did: a.did, handle: a.handle, status: a.status });
            }
        }
    }
    Ok((found, ids))
}

/// Every shard, or an error naming what didn't answer: a node that can't
/// (or whose build doesn't know this endpoint) might hold a blocking
/// account, or not know about the domain at all.
async fn scan(app: &App, domain: &str, purpose: Purpose) -> XResult<Found> {
    let (mut found, owned) = scan_local(app, domain, purpose).await?;
    let purpose_q = match purpose {
        Purpose::Add => "add",
        Purpose::Remove => "remove",
    };
    let q = [("domain", domain.to_string()), ("purpose", purpose_q.to_string())];
    let g = super::internal::gather_timeout(app, "/internal/v1/handle-domains/accounts", &q, SCAN_TIMEOUT).await;
    let mut covered: HashSet<ShardId> = owned.into_iter().collect();
    for r in g.replies {
        covered.extend(r.owned);
        found.add(serde_json::from_value(r.body["found"].clone()).map_err(XrpcError::from_err)?);
    }
    let missing: Vec<ShardId> =
        app.partitions.layout().shards.iter().map(|r| r.id).filter(|id| !covered.contains(id)).collect();
    if !g.unreachable.is_empty() || !g.unsupported.is_empty() || !missing.is_empty() {
        let mut why = Vec::new();
        if !g.unsupported.is_empty() {
            why.push(format!("nodes on a build without handle domain management: {}", g.unsupported.join(", ")));
        }
        if !g.unreachable.is_empty() {
            why.push(format!("unreachable nodes: {}", g.unreachable.join(", ")));
        }
        if !missing.is_empty() {
            why.push(format!("shards no node answered for: {missing:?}"));
        }
        return Err(XrpcError::unavailable(
            "HandleDomainScanIncomplete",
            format!("couldn't check every account ({}); retry once every node is up and upgraded", why.join("; ")),
        ));
    }
    Ok(found)
}

#[derive(Deserialize)]
struct ScanQ {
    domain: String,
    purpose: Purpose,
}

async fn internal_scan(State(app): AppState, headers: HeaderMap, Query(q): Query<ScanQ>) -> XResult<Json<J>> {
    super::internal::check(&app, &headers)?;
    let domain = parse_domain(&q.domain)?;
    let (found, owned) = scan_local(&app, &domain, q.purpose).await?;
    Ok(Json(json!({"owned": owned, "found": found})))
}

async fn internal_reload(State(app): AppState, headers: HeaderMap) -> XResult<Json<J>> {
    super::internal::check(&app, &headers)?;
    refresh(&app.handle_domain_set, &app.store).await.map_err(upstream)?;
    Ok(Json(json!({"domains": app.handle_domains().claimable_names()})))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(list: &[&str]) -> HandleDomains {
        HandleDomains::new(list).unwrap()
    }

    #[test]
    fn what_blocks_adding_a_domain() {
        let s = set(&["a.com", "at.new.com"]);
        let add = |h| blocks(&s, "new.com", Purpose::Add, h);
        assert!(add("new.com"), "the domain itself");
        assert!(add("bob.new.com"), "an own-domain handle under it");
        assert!(add("bob.x.new.com"), "deeper too");
        assert!(!add("bob.at.new.com"), "already under a longer domain: unaffected");
        assert!(!add("bob.a.com") && !add("bob.renew.com") && !add("bob.net"));
        // a domain nested in a listed one takes its handles over
        assert!(blocks(&set(&["example.com"]), "at.example.com", Purpose::Add, "bob.at.example.com"));
    }

    #[test]
    fn what_blocks_removing_a_domain() {
        let s = set(&["a.com"]).with_managed([
            ("old.com".to_string(), DomainState::Retiring),
            ("at.old.com".to_string(), DomainState::Active),
        ]);
        let rm = |h| blocks(&s, "old.com", Purpose::Remove, h);
        assert!(rm("bob.old.com"));
        assert!(!rm("bob.at.old.com"), "under the longer domain");
        assert!(!rm("old.com") && !rm("bob.a.com") && !rm("bob.net"));
    }

    #[test]
    fn finds_the_pds_endpoint() {
        let doc = json!({"service": [
            {"id": "#atproto_labeler", "serviceEndpoint": "https://l.example"},
            {"id": "did:plc:x#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": "https://pds.example"},
        ]});
        assert_eq!(pds_endpoint(&doc), Some("https://pds.example"));
        assert_eq!(pds_endpoint(&json!({})), None);
    }

    #[test]
    fn removable_after_adds_the_grace() {
        assert_eq!(
            removable_after("2026-10-06T10:00:00.000Z", Duration::from_secs(120)).as_deref(),
            Some("2026-10-06T10:02:00.000Z")
        );
        assert_eq!(removable_after("garbage", Duration::from_secs(1)), None);
    }

    #[test]
    fn stored_doc_round_trips_unknown_fields() {
        let raw = json!({"domains": [{"domain": "m.com", "state": "retiring", "addedAt": "t", "retiringSince": "u", "note": 1}], "v": 2});
        let doc: Doc = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(doc.domains[0].state, DomainState::Retiring);
        assert_eq!(serde_json::to_value(&doc).unwrap(), raw);
    }
}
