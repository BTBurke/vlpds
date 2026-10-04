//! The cluster's mail budget (src/ratelimit/mail_budget.rs): one daily
//! count in the bucket, shared by every node, kept through a node's loss,
//! set by --mail-daily-budget and editable in the console.
use crate::common::*;
use std::sync::Arc;
use std::time::Duration;

const SHARDS: u32 = 8;

async fn node(id: &str, store: &Arc<object_store::memory::InMemory>, budget: u32) -> TestServer {
    cluster_node(id, store.clone(), SHARDS, move |c| {
        c.rate_limits_enabled = true;
        c.mail_daily_budget = budget;
    })
    .await
}

fn scraped(text: &str, series: &str) -> f64 {
    text.lines().find_map(|l| l.strip_prefix(series).and_then(|v| v.trim().parse().ok())).unwrap_or(0.0)
}

async fn metric(s: &TestServer, series: &str) -> f64 {
    scraped(&reqwest::get(format!("{}/metrics", s.url)).await.unwrap().text().await.unwrap(), series)
}

/// requestEmailConfirmation for `a` through `n`: 200 and one mail, or 429
/// and none.
async fn confirm_mail(n: &TestServer, a: &TestAccount, sent: bool) {
    let (r, _) = mailed_n(
        n,
        &a.email,
        sent as usize,
        n.xrpc.post_empty("com.atproto.server.requestEmailConfirmation", &a.auth()),
    )
    .await;
    if sent {
        r.ok();
    } else {
        r.err(429, "RateLimitExceeded");
        assert!(r.text().contains("Too many emails"), "{}", r.text());
    }
}

/// Mail sent on node a counts against node b: with a budget of 5, a sends
/// 3 and b only 2. Over it, a user's request is refused 429, a password
/// reset answers as if mailed, and admin sendEmail still goes out. With b
/// down the count stands on a; raised in the console, a mails again up to
/// the new limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_budget_is_shared_and_survives_a_node() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let (a, b) = (node("a", &store, 5).await, node("b", &store, 5).await);
    balanced(&[&a, &b]).await;
    let mut on_a = Vec::new();
    let mut on_b = Vec::new();
    for _ in 0..3 {
        on_a.push(a.create_account("mcb").await);
        on_b.push(b.create_account("mcb").await);
    }
    assert!(a.app.remote_owner(&on_a[0].did).is_none() && b.app.remote_owner(&on_b[0].did).is_none());
    let series = r#"vlpds_mail_suppressed_total{purpose="confirm_email",reason="cluster_limit"}"#;
    let before = metric(&b, series).await;

    for acct in &on_a {
        confirm_mail(&a, acct, true).await;
    }
    confirm_mail(&b, &on_b[0], true).await;
    confirm_mail(&b, &on_b[1], true).await;
    confirm_mail(&b, &on_b[2], false).await;
    confirm_mail(&a, &on_a[0], false).await;
    assert!(metric(&b, series).await > before, "{series} moved");

    // a password reset over it answers as if mailed and mails nothing
    let (r, _) = mailed_n(
        &b,
        &on_b[2].email,
        0,
        b.xrpc.post("com.atproto.server.requestPasswordReset", &json!({"email": on_b[2].email}), &Auth::None),
    )
    .await;
    r.ok();
    // moderation mail is exempt
    let body = json!({"recipientDid": on_b[2].did, "content": "<p>hello</p>", "senderDid": "did:example:mod"});
    let (r, _) = mailed_n(&b, &on_b[2].email, 1, b.xrpc.post("com.atproto.admin.sendEmail", &body, &Auth::Admin)).await;
    r.ok();

    // the console shows the one cluster count and the flag's default
    let s = a.xrpc.get("vlpds.admin.getRateLimits", &[("top", "5")], &Auth::Admin).await.ok();
    let row = s["limiters"].as_array().unwrap().iter().find(|l| l["name"] == "mail-cluster-day").unwrap().clone();
    assert_eq!(
        (row["key"].as_str(), row["points"].as_u64(), row["default"]["points"].as_u64()),
        (Some("cluster"), Some(5), Some(5)),
        "{row}"
    );
    assert_eq!(row["windowSecs"], 86400);
    let top = &s["top"]["mail-cluster-day"][0];
    assert_eq!(
        (top["key"].as_str(), top["used"].as_u64(), top["limit"].as_u64()),
        (Some("cluster"), Some(5), Some(5)),
        "{s}"
    );

    // b goes down; a takes its accounts and the count stands
    b.app.node.halt();
    eventually(Duration::from_secs(20), || async { (owned(&a) == SHARDS as usize).then_some(()) })
        .await
        .expect("a takes over");
    confirm_mail(&a, &on_b[2], false).await;

    // raised in the console: two more, then refused again
    let r = a
        .xrpc
        .post(
            "vlpds.admin.updateRateLimits",
            &json!({"config": {"limiters": {"mail-cluster-day": {"points": 7}}}, "ifVersion": 0, "actor": "it-test"}),
            &Auth::Admin,
        )
        .await
        .ok();
    assert_eq!(r["config"]["history"][0]["changes"][0], "mail-cluster-day: points 5→7", "{r}");
    let s = a.xrpc.get("vlpds.admin.getRateLimits", &[("local", "true")], &Auth::Admin).await.ok();
    assert_eq!(s["config"]["limiters"]["mail-cluster-day"]["points"], 7);
    confirm_mail(&a, &on_b[2], true).await;
    confirm_mail(&a, &on_a[1], true).await;
    confirm_mail(&a, &on_a[2], false).await;

    // disabled in the console: no cluster budget at all
    let r = a
        .xrpc
        .post(
            "vlpds.admin.updateRateLimits",
            &json!({"config": {"limiters": {"mail-cluster-day": {"enabled": false}}}, "ifVersion": 1, "actor": "it-test"}),
            &Auth::Admin,
        )
        .await;
    r.ok();
    confirm_mail(&a, &on_a[2], true).await;
}

/// A node that never mailed reads the cluster's count from the bucket:
/// one that restarts, or joins, can't start the day over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_node_reads_the_count() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node("a", &store, 2).await;
    let x = a.create_account("mcn").await;
    let y = a.create_account("mcn").await;
    confirm_mail(&a, &x, true).await;
    confirm_mail(&a, &y, true).await;
    a.app.node.halt();
    let b = node("b", &store, 2).await;
    eventually(Duration::from_secs(20), || async { (owned(&b) == SHARDS as usize).then_some(()) })
        .await
        .expect("b takes over");
    confirm_mail(&b, &x, false).await;
}
