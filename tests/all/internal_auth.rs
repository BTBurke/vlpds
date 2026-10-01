//! Node-to-node endpoint auth: the internal token (not the admin token
//! outside dev mode) and the private-put key scope.

use crate::common::*;
use base64::Engine;

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

async fn cluster_status(s: &TestServer, token: &str) -> u16 {
    let rb = s.xrpc.http.get(format!("{}/internal/v1/cluster", s.url)).header("x-vlpds-internal", token);
    s.xrpc.send(rb).await.status
}

async fn private_put(s: &TestServer, routing: &str, key: &[u8]) -> Resp {
    let body = json!({"routing": routing, "muts": [[B64.encode(key), B64.encode(b"x")]]});
    let rb = s
        .xrpc
        .http
        .post(format!("{}/internal/v1/private/put", s.url))
        .header("x-vlpds-internal", vlpds::server::DEV_INTERNAL_TOKEN)
        .json(&body);
    s.xrpc.send(rb).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn internal_token_and_private_put_scope() {
    let s = TestServer::spawn().await;
    let a = s.create_account("int").await;
    let b = s.create_account("int").await;
    assert_eq!(cluster_status(&s, vlpds::server::DEV_INTERNAL_TOKEN).await, 200);
    assert_eq!(cluster_status(&s, "nope").await, 401);
    assert_eq!(cluster_status(&s, "").await, 401);
    // dev mode still takes the admin token from senders not yet switched
    assert_eq!(cluster_status(&s, ADMIN_TOKEN).await, 200);

    // only the routing key's own private state
    let r = private_put(&s, &a.did, &vlpds::state::private_key(&a.did, "scratch")).await;
    assert_eq!(r.status, 200, "{}", r.text());
    private_put(&s, &a.did, &vlpds::state::head_key(&a.did)).await.err(400, "InvalidRequest");
    private_put(&s, &a.did, &vlpds::state::record_key(&a.did, "app.bsky.feed.post/x")).await.err(400, "InvalidRequest");
    private_put(&s, &a.did, &vlpds::state::private_key(&b.did, "totp")).await.err(400, "InvalidRequest");
    // a DID that is a prefix of another's can't reach into it
    private_put(&s, &a.did[..a.did.len() - 1], &vlpds::state::private_key(&a.did, "totp")).await.err(400, "InvalidRequest");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_token_is_not_internal_outside_dev_mode() {
    let s = TestServer::spawn_with(|c| c.dev_mode = false).await;
    assert_eq!(cluster_status(&s, ADMIN_TOKEN).await, 401);
    assert_eq!(cluster_status(&s, vlpds::server::DEV_INTERNAL_TOKEN).await, 200);
}
