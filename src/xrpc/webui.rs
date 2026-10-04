//! The embedded web UI (built from `ui/` into `ui/dist`; `build.rs` leaves a
//! placeholder when it isn't built) and `vlpds.admin.getClusterStatus`, the
//! view the operator console polls.

use super::*;
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "ui/dist"]
struct Assets;

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route("/", get(landing_shell))
        .route("/account", get(account_shell))
        .route("/account/", get(account_shell))
        .route("/account/{*rest}", get(account_shell))
        .route("/admin", get(admin_shell))
        .route("/admin/", get(admin_shell))
        .route("/admin/{*rest}", get(admin_shell))
        .route("/docs", get(docs_shell))
        .route("/docs/", get(docs_shell))
        .route("/docs/{*rest}", get(docs_shell))
        .route("/migrate", get(migrate_shell))
        .route("/migrate/", get(migrate_shell))
        .route("/assets/{*path}", get(asset))
        .route("/fonts/{*path}", get(asset))
        .route("/og/{*path}", get(asset))
        .route("/favicon.svg", get(asset))
        .route("/robots.txt", get(robots))
        .route("/sitemap.xml", get(sitemap))
        .route("/xrpc/vlpds.admin.getClusterStatus", get(cluster_status))
}

/// Same-origin only: scripts and styles come from the bundle, API calls go
/// to this server. Inline style *attributes* set through the CSSOM (React
/// `style`, uPlot) are not governed by style-src.
const SPA_CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; \
img-src 'self' data: blob:; connect-src 'self'; manifest-src 'self'; frame-ancestors 'none'; \
base-uri 'none'; form-action 'none'";

/// The migration page talks to the account's current PDS, which can be any
/// host; a dev server's (and a local e2e's) old PDS may be plain http.
const MIGRATE_CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; \
img-src 'self' data: blob:; connect-src 'self' https:; manifest-src 'self'; frame-ancestors 'none'; \
base-uri 'none'; form-action 'none'";
const MIGRATE_CSP_DEV: &str = "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; \
img-src 'self' data: blob:; connect-src 'self' https: http:; manifest-src 'self'; frame-ancestors 'none'; \
base-uri 'none'; form-action 'none'";

/// What the UI build wrote to `og/manifest.json`: the link-preview cards and
/// each docs page's front matter.
#[derive(Deserialize)]
struct OgManifest {
    width: u32,
    height: u32,
    site: Card,
    migrate: Card,
    docs: std::collections::BTreeMap<String, DocMeta>,
}

#[derive(Deserialize)]
struct Card {
    image: String,
    alt: String,
}

#[derive(Deserialize)]
struct DocMeta {
    title: String,
    summary: String,
    status: String,
    #[serde(flatten)]
    card: Card,
}

static OG: std::sync::LazyLock<Option<OgManifest>> = std::sync::LazyLock::new(|| {
    let f = Assets::get("og/manifest.json")?;
    serde_json::from_slice(&f.data).inspect_err(|e| tracing::warn!("ui og/manifest.json: {e}")).ok()
});

/// index.html cut around its `<title>`, where the page's head goes.
static SHELL: std::sync::LazyLock<Option<(String, String)>> = std::sync::LazyLock::new(|| {
    let f = Assets::get("index.html")?;
    let html = String::from_utf8_lossy(&f.data).into_owned();
    let (start, end) = match (html.find("<title>"), html.find("</title>")) {
        (Some(a), Some(b)) if b > a => (a, b + "</title>".len()),
        _ => {
            let at = html.find("</head>").unwrap_or(0);
            (at, at)
        }
    };
    Some((html[..start].to_string(), html[end..].to_string()))
});

/// Per-route `<head>` tags, server-rendered so link-preview fetchers (which
/// don't run the SPA) see a title, a description and a card.
struct PageHead {
    title: String,
    description: String,
    /// Path of the canonical URL.
    path: String,
    card: Option<&'static Card>,
    og_type: &'static str,
    noindex: bool,
}

fn esc(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => o.push_str("&amp;"),
            '<' => o.push_str("&lt;"),
            '>' => o.push_str("&gt;"),
            '"' => o.push_str("&quot;"),
            '\'' => o.push_str("&#39;"),
            c => o.push(c),
        }
    }
    o
}

/// `https://pds.example.com` (no trailing slash) and `pds.example.com`.
fn origin_and_host(public_url: &str) -> (String, String) {
    let origin = public_url.trim_end_matches('/').to_string();
    let host = reqwest::Url::parse(&origin)
        .ok()
        .and_then(|u| u.host_str().map(|h| match u.port() {
            Some(p) => format!("{h}:{p}"),
            None => h.to_string(),
        }))
        .unwrap_or_else(|| origin.clone());
    (origin, host)
}

fn site_name(app: &App, host: &str) -> String {
    app.config.email_branding.name.clone().unwrap_or_else(|| host.to_string())
}

impl PageHead {
    fn render(&self, app: &App) -> String {
        let (origin, host) = origin_and_host(&app.config.public_url);
        let site = esc(&site_name(app, &host));
        let (title, desc, url) = (esc(&self.title), esc(&self.description), esc(&format!("{origin}{}", self.path)));
        let mut h = format!("<title>{title}</title>\n    <meta name=\"description\" content=\"{desc}\" />\n    <link rel=\"canonical\" href=\"{url}\" />\n");
        if self.noindex {
            h.push_str("    <meta name=\"robots\" content=\"noindex, nofollow\" />\n");
        }
        h.push_str(&format!(
            "    <meta property=\"og:type\" content=\"{}\" />\n    <meta property=\"og:site_name\" content=\"{site}\" />\n    <meta property=\"og:title\" content=\"{title}\" />\n    <meta property=\"og:description\" content=\"{desc}\" />\n    <meta property=\"og:url\" content=\"{url}\" />\n",
            self.og_type
        ));
        match (self.card, OG.as_ref()) {
            (Some(c), Some(m)) => {
                let (img, alt) = (esc(&format!("{origin}{}", c.image)), esc(&c.alt));
                h.push_str(&format!(
                    "    <meta property=\"og:image\" content=\"{img}\" />\n    <meta property=\"og:image:type\" content=\"image/png\" />\n    <meta property=\"og:image:width\" content=\"{}\" />\n    <meta property=\"og:image:height\" content=\"{}\" />\n    <meta property=\"og:image:alt\" content=\"{alt}\" />\n    <meta name=\"twitter:card\" content=\"summary_large_image\" />\n    <meta name=\"twitter:image\" content=\"{img}\" />\n    <meta name=\"twitter:image:alt\" content=\"{alt}\" />\n",
                    m.width, m.height
                ));
            }
            _ => h.push_str("    <meta name=\"twitter:card\" content=\"summary\" />\n"),
        }
        h.push_str(&format!("    <meta name=\"twitter:title\" content=\"{title}\" />\n    <meta name=\"twitter:description\" content=\"{desc}\" />"));
        h
    }
}

async fn landing_shell(State(app): AppState) -> Response {
    let (_, host) = origin_and_host(&app.config.public_url);
    let site = site_name(&app, &host);
    let head = PageHead {
        title: format!("{site} · vlpds"),
        description: format!(
            "{site} is a personal data server for the AT Protocol: Bluesky accounts whose posts, likes and follows live here as signed repositories. \
Sign in, create an account, or move an existing Bluesky account here."
        ),
        path: "/".into(),
        card: OG.as_ref().map(|m| &m.site),
        og_type: "website",
        noindex: false,
    };
    shell_with(&app, StatusCode::OK, &head, SPA_CSP)
}

async fn account_shell(State(app): AppState) -> Response {
    let (_, host) = origin_and_host(&app.config.public_url);
    let head = PageHead {
        title: "Account · vlpds".into(),
        description: format!("Manage your AT Protocol account on {host}: sign-in security, app passwords, your repository and data."),
        path: "/account".into(),
        card: OG.as_ref().map(|m| &m.site),
        og_type: "website",
        noindex: true,
    };
    shell_with(&app, StatusCode::OK, &head, SPA_CSP)
}

async fn admin_shell(State(app): AppState) -> Response {
    let (_, host) = origin_and_host(&app.config.public_url);
    let head = PageHead {
        title: "Console · vlpds".into(),
        description: format!("The operator console for {host}."),
        path: "/admin".into(),
        card: None,
        og_type: "website",
        noindex: true,
    };
    shell_with(&app, StatusCode::OK, &head, SPA_CSP)
}

async fn migrate_shell(State(app): AppState) -> Response {
    let (_, host) = origin_and_host(&app.config.public_url);
    let head = PageHead {
        title: format!("Move your Bluesky account to {host}"),
        description: format!(
            "Bring your Bluesky / atproto account to {host}: your repository, blobs and preferences, then your identity. \
Your handle, followers and posts come with you."
        ),
        path: "/migrate".into(),
        card: OG.as_ref().map(|m| &m.migrate),
        og_type: "website",
        noindex: false,
    };
    shell_with(&app, StatusCode::OK, &head, if app.config.dev_mode { MIGRATE_CSP_DEV } else { MIGRATE_CSP })
}

/// The page's slug as DocsApp reads it (`/docs` is the overview).
fn doc_slug(path: &str) -> &str {
    let s = path.trim_start_matches("/docs").trim_matches('/');
    if s.is_empty() { "overview" } else { s }
}

async fn docs_shell(State(app): AppState, uri: axum::http::Uri) -> Response {
    let slug = doc_slug(uri.path());
    let Some(doc) = OG.as_ref().and_then(|m| m.docs.get(slug)) else {
        // without a built UI there is no manifest: every page is unknown
        let status = if OG.is_some() { StatusCode::NOT_FOUND } else { StatusCode::OK };
        let head = PageHead {
            title: "Not found · vlpds docs".into(),
            description: "This documentation page doesn't exist.".into(),
            path: uri.path().to_string(),
            card: None,
            og_type: "website",
            noindex: true,
        };
        return shell_with(&app, status, &head, SPA_CSP);
    };
    let head = PageHead {
        title: format!("{} · vlpds docs", doc.title),
        description: doc.summary.clone(),
        path: format!("/docs/{slug}"),
        card: Some(&doc.card),
        og_type: "article",
        noindex: doc.status == "stub",
    };
    shell_with(&app, StatusCode::OK, &head, SPA_CSP)
}

fn shell_with(app: &App, status: StatusCode, head: &PageHead, csp: &'static str) -> Response {
    let Some((before, after)) = SHELL.as_ref() else {
        return (StatusCode::NOT_FOUND, "UI not built: run `just ui`").into_response();
    };
    let html = format!("{before}{}{after}", head.render(app));
    let mut r = (status, [(header::CONTENT_TYPE, "text/html; charset=utf-8"), (header::CACHE_CONTROL, "no-cache")], html).into_response();
    let h = r.headers_mut();
    h.insert(header::CONTENT_SECURITY_POLICY, header::HeaderValue::from_static(csp));
    h.insert(header::X_CONTENT_TYPE_OPTIONS, header::HeaderValue::from_static("nosniff"));
    h.insert(header::REFERRER_POLICY, header::HeaderValue::from_static("no-referrer"));
    h.insert(header::X_FRAME_OPTIONS, header::HeaderValue::from_static("DENY"));
    r
}

async fn robots(State(app): AppState) -> Response {
    let (origin, _) = origin_and_host(&app.config.public_url);
    let body = format!(
        "User-agent: *\nDisallow: /admin\nDisallow: /account\nDisallow: /xrpc/\nDisallow: /oauth/\nAllow: /\n\nSitemap: {origin}/sitemap.xml\n"
    );
    ([(header::CONTENT_TYPE, "text/plain; charset=utf-8"), (header::CACHE_CONTROL, "public, max-age=3600")], body).into_response()
}

/// The landing page, /migrate and every written docs page.
async fn sitemap(State(app): AppState) -> Response {
    let (origin, _) = origin_and_host(&app.config.public_url);
    let mut paths = vec!["/".to_string(), "/migrate".to_string()];
    if let Some(m) = OG.as_ref() {
        paths.extend(m.docs.iter().filter(|(_, d)| d.status != "stub").map(|(slug, _)| format!("/docs/{slug}")));
    }
    let mut body = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n");
    for p in paths {
        body.push_str(&format!("  <url><loc>{}</loc></url>\n", esc(&format!("{origin}{p}"))));
    }
    body.push_str("</urlset>\n");
    ([(header::CONTENT_TYPE, "application/xml; charset=utf-8"), (header::CACHE_CONTROL, "public, max-age=3600")], body).into_response()
}

async fn asset(uri: axum::http::Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let Some(f) = Assets::get(path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // hashed bundle files and cards never change; fonts, the icon and the email logo keep stable names
    let hashed = path.starts_with("assets/")
        || (path.starts_with("og/") && path.ends_with(".png") && path != "og/email-logo.png");
    let cache = if hashed { "public, max-age=31536000, immutable" } else { "public, max-age=86400" };
    let mime = f.metadata.mimetype().to_string();
    (
        [
            (header::CONTENT_TYPE, mime),
            (header::CACHE_CONTROL, cache.to_string()),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff".to_string()),
        ],
        f.data,
    )
        .into_response()
}

/// Peers' own durable ordinal and lease state come from their
/// `/internal/v1/cluster`.
async fn cluster_status(State(app): AppState, Auth(creds): Auth) -> XResult<Json<J>> {
    super::admin::require_admin(&creds)?;
    let owned: Vec<crate::slots::ShardId> = app.partitions.owned().iter().map(|p| p.id).collect();
    let durable = app.log.durable_ordinal.load(Ordering::Acquire);
    let durable = (durable != u64::MAX).then_some(durable);
    let sources: Vec<J> = {
        let mut v: Vec<J> = app
            .firehose
            .sources
            .read()
            .iter()
            .map(|(log, src)| {
                let (wm, local) = match src {
                    crate::firehose::Source::Local(w) => (w.get(), true),
                    crate::firehose::Source::Remote(a) => (a.load(Ordering::Acquire), false),
                };
                json!({"log": log.to_string(), "watermark": wm.to_string(), "local": local})
            })
            .collect();
        v.sort_by(|a, b| a["log"].as_str().cmp(&b["log"].as_str()));
        v
    };
    let mut out = json!({
        "node": "",
        "log": app.log.log_id.to_string(),
        "logDurableOrdinal": durable,
        "owned": owned,
        "shards": app.partitions.len(),
        "table": [],
        "nodes": [],
        "leaseValid": false,
        "firehose": {
            // seqs are unix_micros × 256 + writer: beyond JS's 2^53, so strings
            "lastEmitted": app.firehose.last_emitted.load(Ordering::Acquire).to_string(),
            "minWatermark": app.firehose.min_watermark().map(|w| w.to_string()),
            "sources": sources,
        },
        "fencedLogs": {},
        "time": crate::tid::now_micros() / 1000,
    });
    let Some(c) = &app.cluster else {
        return Ok(Json(out));
    };
    let me = c.cfg.node_id.clone();
    out["node"] = json!(me);
    out["leaseValid"] = json!(c.lease_valid());
    out["leaseExpiresMs"] = json!(c.lease_expiry_us() / 1000);
    // in slot order
    let layout = c.layout();
    out["table"] = json!(layout.shards.iter().map(|r| c.owner_of(r.id).map(|(id, _)| id)).collect::<Vec<_>>());
    out["layout"] = json!({"version": layout.version, "shards": layout.shards, "op": layout.op});
    out["fencedLogs"] = json!(c.fenced_logs());
    let mut peers = c.peers();
    if !peers.iter().any(|l| l.node_id == me) {
        let mut own = c.own_lease();
        own.expires_ms = c.lease_expiry_us() / 1000;
        own.next_ordinal = app.log.next_ordinal();
        own.joined = c.joined();
        peers.push(own);
    }
    out["version"] = feature_levels(c, &peers).await;
    peers.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    let fetches = peers.iter().map(|l| {
        let (app, l, me) = (app.clone(), l.clone(), me.clone());
        async move {
            let mut n = json!({
                "node": l.node_id, "log": l.log_id, "addr": l.addr,
                "writer": l.writer, "expiresMs": l.expires_ms, "self": l.node_id == me,
                "rev": l.rev, "minLevel": l.min_level, "maxLevel": l.max_level, "seenLevel": l.seen_level,
            });
            if l.node_id == me {
                n["reachable"] = json!(true);
                n["leaseValid"] = json!(app.cluster.as_ref().is_some_and(|c| c.lease_valid()));
                n["logDurableOrdinal"] = json!(durable);
                n["owned"] = json!(app.partitions.owned().len());
                return n;
            }
            let r = app
                .http
                .get(format!("{}/internal/v1/cluster", l.addr.trim_end_matches('/')))
                .header(super::internal::HDR, &app.config.internal_token)
                .timeout(std::time::Duration::from_millis(1500))
                .send()
                .await;
            match r {
                Ok(r) if r.status().is_success() => {
                    let v: J = r.json().await.unwrap_or(J::Null);
                    n["reachable"] = json!(true);
                    n["leaseValid"] = v["lease_valid"].clone();
                    let o = v["log_durable_ordinal"].as_u64().filter(|o| *o != u64::MAX);
                    n["logDurableOrdinal"] = json!(o);
                    n["owned"] = json!(v["owned"].as_array().map(|a| a.len()).unwrap_or(0));
                }
                _ => n["reachable"] = json!(false),
            }
            n
        }
    });
    out["nodes"] = json!(futures::future::join_all(fetches).await);
    Ok(Json(out))
}

/// `finalizable`: the highest level every live node can run, when above the
/// active one. `finalizedAt`: when the active level was raised (older builds
/// can no longer join).
async fn feature_levels(c: &crate::cluster::Cluster, nodes: &[crate::cluster::NodeLease]) -> J {
    let (v, error) = match c.read_version().await {
        Ok(Some((v, _))) => (Some(v), None),
        Ok(None) => (None, Some(format!("{} is missing", crate::version::OBJECT))),
        Err(e) => (c.cluster_version(), Some(format!("{e:#}"))),
    };
    let revs: std::collections::BTreeSet<&str> = nodes.iter().map(|l| l.rev.as_str()).collect();
    let common_max = nodes.iter().map(|l| l.max_level).min();
    let active = v.as_ref().map(|v| v.active);
    let finalizable = common_max.filter(|m| active.is_some_and(|a| *m > a));
    let finalized_at = v.as_ref().and_then(|v| v.history.iter().rev().find(|h| h.level == v.active && v.history.len() > 1).map(|h| h.at.clone()));
    let mut out = json!({
        "active": active,
        "target": v.as_ref().and_then(|v| v.target),
        "history": v.as_ref().map(|v| v.history.clone()).unwrap_or_default(),
        "binary": {"min": c.cfg.levels.min, "max": c.cfg.levels.max, "rev": crate::version::build_rev()},
        "mixedBuilds": revs.len() > 1,
        "revs": revs,
        "finalizable": finalizable,
        "finalizedAt": finalized_at,
    });
    if let Some(e) = error {
        out["error"] = json!(e);
    }
    out
}
