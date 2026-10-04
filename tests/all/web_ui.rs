//! The web UI is read from `--ui-dir` at startup: a configured directory
//! that isn't a complete build fails startup, only files found there are
//! served (no traversal out of it), with their content types and caching.

use crate::common::*;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Removed on drop.
struct Dir(PathBuf);

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn write(root: &Path, rel: &str, data: &[u8]) {
    let p = root.join(rel);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, data).unwrap();
}

/// `<tmp>/<name>/ui` (a small but complete build) beside `<tmp>/<name>/secret.txt`.
fn fake_ui() -> (Dir, PathBuf) {
    let base = std::env::temp_dir().join(unique_name("vlpds-ui"));
    let ui = base.join("ui");
    write(
        &ui,
        "index.html",
        b"<!doctype html><html><head><title>shell</title></head><body><div id=root></div></body></html>",
    );
    write(
        &ui,
        "og/manifest.json",
        br#"{"width":1200,"height":630,"site":{"image":"/og/site-abc.png","alt":"site"},"migrate":{"image":"/og/migrate-abc.png","alt":"migrate"},
"docs":{"overview":{"title":"Overview","summary":"The summary.","status":"ready","image":"/og/docs-overview-abc.png","alt":"overview"}}}"#,
    );
    write(&ui, "og/site-abc.png", b"\x89PNG card");
    write(&ui, "og/email-logo.png", b"\x89PNG logo");
    write(&ui, "assets/index-abc123.js", b"console.log(1)");
    write(&ui, "assets/index-abc123.css", b"body{}");
    write(&ui, "fonts/inter.woff2", b"wOF2");
    write(&ui, "favicon.svg", b"<svg/>");
    write(&base, "secret.txt", b"secret");
    (Dir(base), ui)
}

/// One request sent as written: HTTP clients resolve `..` and `%2e%2e`
/// before sending.
async fn raw_get(addr: std::net::SocketAddr, target: &str) -> (u16, String) {
    let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
    c.write_all(format!("GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut out = Vec::new();
    c.read_to_end(&mut out).await.unwrap();
    let out = String::from_utf8_lossy(&out).into_owned();
    let status = out.split(' ').nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    (status, out)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_ui_dir_must_be_a_complete_build() {
    let (base, ui) = fake_ui();
    let start = |dir: PathBuf| vlpds::server::build(vlpds::server::Config { ui_dir: Some(dir), ..Default::default() });

    let err = start(base.0.join("missing")).await.err().expect("a missing dir fails startup");
    assert!(format!("{err:#}").contains("--ui-dir"), "{err:#}");
    let empty = base.0.join("empty");
    std::fs::create_dir_all(&empty).unwrap();
    let err = start(empty).await.err().expect("an empty dir fails startup");
    assert!(format!("{err:#}").contains("index.html"), "{err:#}");
    std::fs::remove_file(ui.join("og/manifest.json")).unwrap();
    let err = start(ui.clone()).await.err().expect("a build without its manifest fails startup");
    assert!(format!("{err:#}").contains("manifest.json"), "{err:#}");
    write(&ui, "og/manifest.json", b"{not json");
    let err = start(ui).await.err().expect("a broken manifest fails startup");
    assert!(format!("{err:#}").contains("manifest.json"), "{err:#}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ui_dir_files_are_served_and_nothing_else() {
    let (_base, ui) = fake_ui();
    let s = TestServer::spawn_with(|c| c.ui_dir = Some(ui.clone())).await;
    let get = |path: &str| {
        let x = s.xrpc.clone();
        let url = format!("{}{path}", s.url);
        async move { x.send(x.http.get(url)).await }
    };

    for (path, mime, cache) in [
        ("/assets/index-abc123.js", "text/javascript", "public, max-age=31536000, immutable"),
        ("/assets/index-abc123.css", "text/css", "public, max-age=31536000, immutable"),
        ("/og/site-abc.png", "image/png", "public, max-age=31536000, immutable"),
        ("/og/email-logo.png", "image/png", "public, max-age=86400"),
        ("/fonts/inter.woff2", "font/woff2", "public, max-age=86400"),
        ("/favicon.svg", "image/svg+xml", "public, max-age=86400"),
    ] {
        let r = get(path).await;
        assert_eq!(r.status, 200, "{path}");
        assert_eq!(r.header("content-type").unwrap(), mime, "{path}");
        assert_eq!(r.header("cache-control").unwrap(), cache, "{path}");
        assert_eq!(r.header("x-content-type-options").unwrap(), "nosniff", "{path}");
    }
    assert_eq!(get("/assets/index-abc123.js").await.body.as_ref(), b"console.log(1)");
    assert_eq!(get("/assets/nope.js").await.status, 404);
    assert_eq!(get("/og/manifest.json").await.status, 200, "served like any og/ file, as before");

    // the shell is the configured index.html, with the route's head
    let r = get("/docs/overview").await;
    let html = String::from_utf8_lossy(&r.body).into_owned();
    assert!(html.contains("<title>Overview · vlpds docs</title>") && html.contains("<div id=root>"), "{html}");
    assert!(html.contains("/og/docs-overview-abc.png"), "card from the manifest: {html}");
    assert_eq!(get("/docs/unknown").await.status, 404, "the manifest lists the pages");

    for target in [
        "/assets/../secret.txt",
        "/assets/../../secret.txt",
        "/assets/%2e%2e/%2e%2e/secret.txt",
        "/assets/..%2f..%2fsecret.txt",
        "/og/../index.html",
        "/fonts/..%2F..%2Fsecret.txt",
        "/assets//etc/passwd",
        "/assets/%2Fetc%2Fpasswd",
    ] {
        let (status, out) = raw_get(s.addr, target).await;
        assert!(status == 404 || status == 400, "{target}: {out}");
        assert!(!out.contains("secret") && !out.contains("root:"), "{target}: {out}");
    }
}
