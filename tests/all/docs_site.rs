//! The documentation site (`docs/*.md`, rendered into the UI bundle at build
//! time by `ui/docs-build`) is served at /docs under the same-origin CSP as
//! the rest of the UI. `npm run check-docs` validates the pages themselves
//! (front matter, heroes, diagrams, links).

use crate::common::*;

/// The whole body (`Resp::text` is cut for assertion messages).
fn body(r: &Resp) -> String {
    String::from_utf8_lossy(&r.body).into_owned()
}

/// Every `assets/{stem}-….js` that `text` imports (as `/assets/…` from
/// index.html, as `./…` from one chunk to another).
fn chunks(text: &str, stem: &str) -> Vec<String> {
    let pat = format!("/{stem}-");
    let mut out = Vec::new();
    for (i, _) in text.match_indices(&pat) {
        let tail = &text[i + 1..];
        let Some(end) = tail.find(".js") else { continue };
        let file = &tail[..end + 3];
        let name = format!("assets/{file}");
        if !file.contains(['"', '\'', ' ', '(', '/']) && !out.contains(&name) {
            out.push(name);
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn docs_pages_are_served_with_the_spa_csp() {
    let s = TestServer::spawn().await;
    let get = |path: String| {
        let x = s.xrpc.clone();
        let url = format!("{}{path}", s.url);
        async move { x.send(x.http.get(url)).await }
    };
    for path in ["/docs", "/docs/", "/docs/overview", "/docs/operations", "/docs/operations/deploy"] {
        let r = get(path.to_string()).await;
        assert_eq!(r.status, 200, "{path}");
        assert!(r.header("content-type").unwrap().starts_with("text/html"), "{path}");
        let csp = r.header("content-security-policy").unwrap();
        assert!(csp.contains("connect-src 'self';") && csp.contains("script-src 'self';"), "{path}: {csp}");
    }

    let shell = body(&get("/docs/overview".into()).await);
    if shell.contains("without its web UI") {
        eprintln!("ui/dist is the placeholder (run `just ui`): skipping the bundle checks");
        return;
    }
    // index → the lazily loaded DocsApp chunk (nav + page index) → the
    // overview page's own chunk.
    let mut entry = String::new();
    for name in chunks(&shell, "index") {
        let r = get(format!("/{name}")).await;
        assert_eq!(r.status, 200, "{name}");
        entry.push_str(&body(&r));
    }
    let docs_app = chunks(&entry, "DocsApp");
    assert_eq!(docs_app.len(), 1, "one DocsApp chunk in the entry: {docs_app:?}");
    let app = get(format!("/{}", docs_app[0])).await;
    assert_eq!(app.status, 200);
    let app = body(&app);
    for slug in ["overview", "architecture", "operations/deploy", "operations/runbook"] {
        assert!(app.contains(&format!("slug:\"{slug}\"")) || app.contains(&format!("\"slug\":\"{slug}\"")), "nav lists {slug}");
    }
    let overview = chunks(&app, "overview");
    assert_eq!(overview.len(), 1, "{overview:?}");
    let page = get(format!("/{}", overview[0])).await;
    assert_eq!(page.status, 200);
    assert!(page.header("content-type").unwrap().contains("javascript"));
    let html = body(&page);
    assert!(html.contains(r#"<section class="hero">"#) && html.contains(r#"<svg class="dg""#), "overview has its hero diagram");
}
