//! An entry node resends a DPoP-authenticated request the owner answered
//! ShardMoved / RepoLoading (`forward::with_retries`), with the client's
//! proof unchanged. The proof's `jti` is claimed for that client request
//! (the entry node's peer-only resend marker), so the resend is served and
//! the proof still authorizes only one request.

use crate::common::*;
use crate::ha_auth::{node, Browser, Client, PUBLIC};
use std::sync::Arc;
use std::time::Duration;
use vlpds::worker::WorkerMsg;

/// Takes `did`'s shard out of `n`'s table (and its workers' caches) for
/// `away`, as a close does when the shard moves: requests reaching `n` for
/// it meanwhile are answered ShardMoved.
async fn shard_away(n: &TestServer, did: &str, away: Duration) -> tokio::task::JoinHandle<()> {
    let shard = n.app.partitions.shard_of(did);
    let p = n.app.partitions.get(shard).expect("owned");
    n.app.partitions.set(shard, None);
    for w in n.app.workers.senders.iter() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        w.send(WorkerMsg::DropPartition(shard, tx)).unwrap();
        let _ = rx.await;
    }
    let table = n.app.partitions.clone();
    tokio::spawn(async move {
        tokio::time::sleep(away).await;
        table.set(shard, Some(p));
    })
}

async fn dpop_get(
    entry: &TestServer,
    client: &Client,
    token: &str,
    nsid: &str,
    query: &[(&str, &str)],
    proof: &str,
) -> (u16, J) {
    let r = reqwest::Client::new()
        .get(format!("{}/xrpc/{nsid}", entry.url))
        .query(query)
        .header("authorization", format!("DPoP {token}"))
        .header("dpop", proof)
        .send()
        .await
        .unwrap();
    if let Some(n) = r.headers().get("dpop-nonce") {
        *client.key.nonce.lock() = Some(n.to_str().unwrap().to_string());
    }
    (r.status().as_u16(), r.json().await.unwrap_or(J::Null))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dpop_query_resent_after_shard_moved() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node("dr-a", &store, Some(PUBLIC)).await;
    let b = node("dr-b", &store, Some(PUBLIC)).await;
    balanced(&[&a, &b]).await;
    // the OAuth user's account on the entry node, so its proof is claimed
    // there; the record read on the other node
    let user = a.create_account("dr").await;
    let other = loop {
        let o = b.create_account("dr").await;
        if b.app.partitions.for_key(&o.did).is_some() && a.app.partitions.for_key(&user.did).is_some() {
            break o;
        }
    };
    let rec = b.post(&other, "read through a moving shard").await;

    let client = Client::new();
    let (code, verifier) = client.authorize(&mut Browser::default(), [&a, &a, &a, &a], &user.handle, &user.did).await;
    let (st, t) = client.exchange(&a, &code, &verifier).await;
    assert_eq!(st, 200, "{t}");
    let access = t["access_token"].as_str().unwrap().to_string();
    // a server nonce for the proofs below
    let (st, j) = client.create_post(&a, &access, &user.did).await;
    assert_eq!(st, 200, "{j}");

    let nsid = "com.atproto.repo.getRecord";
    let q = [("repo", rec.did()), ("collection", rec.collection()), ("rkey", rec.rkey())];
    let htu = format!("{PUBLIC}/xrpc/{nsid}");
    let retries = || vlpds::metrics::READ_RETRIES.with_label_values(&["moved"]).get();
    let before = retries();
    let back = shard_away(&b, &other.did, Duration::from_millis(300)).await;
    let proof = client.key.proof("GET", &htu, Some(&access));
    let (st, j) = dpop_get(&a, &client, &access, nsid, &q, &proof).await;
    back.await.unwrap();
    assert!(retries() > before, "the first attempt was answered ShardMoved and resent");
    assert_eq!(st, 200, "resent with the same proof: {j}");
    assert_eq!(j["uri"], rec.uri.as_str());

    // the proof was still used once: a client (or anyone) sending it again
    // is refused, through either node
    for n in [&a, &b] {
        let (st, j) = dpop_get(n, &client, &access, nsid, &q, &proof).await;
        assert_eq!((st, j["error"].as_str()), (401, Some("invalid_dpop_proof")), "{j}");
    }
}

/// `rb` (a GET of `nsid`) with a DPoP proof and extra headers.
async fn dpop_get_with(rb: reqwest::RequestBuilder, token: &str, proof: &str, headers: &[(&str, &str)]) -> (u16, J) {
    let mut rb = rb.header("authorization", format!("DPoP {token}")).header("dpop", proof);
    for (k, v) in headers {
        rb = rb.header(*k, *v);
    }
    let r = rb.send().await.unwrap();
    (r.status().as_u16(), r.json().await.unwrap_or(J::Null))
}

/// The claim belongs to the client request: its own later attempts (the
/// same resend id) pass it, any other request presenting the proof is
/// refused, with 401 on a first attempt as ever, and with a 503 the entry
/// node doesn't resend on a later one (never a definite refusal for a
/// resend). Only a peer's marker counts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dpop_claim_belongs_to_the_resent_request() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node("rb-a", &store, Some(PUBLIC)).await;
    let user = a.create_account("rb").await;
    let rec = a.post(&user, "read by one request, twice").await;
    let client = Client::new();
    let (code, verifier) = client.authorize(&mut Browser::default(), [&a, &a, &a, &a], &user.handle, &user.did).await;
    let (st, t) = client.exchange(&a, &code, &verifier).await;
    assert_eq!(st, 200, "{t}");
    let access = t["access_token"].as_str().unwrap().to_string();
    let (st, j) = client.create_post(&a, &access, &user.did).await;
    assert_eq!(st, 200, "{j}");

    let nsid = "com.atproto.repo.getRecord";
    let q = [("repo", rec.did()), ("collection", rec.collection()), ("rkey", rec.rkey())];
    let proof = client.key.proof("GET", &format!("{PUBLIC}/xrpc/{nsid}"), Some(&access));
    let token = a.app.config.internal_token.clone();
    let (peer_url, public_url) = (format!("{}/xrpc/{nsid}", a.peer_url), format!("{}/xrpc/{nsid}", a.url));
    let public = reqwest::Client::new();
    let peer = |resend: &str| {
        let h = [("x-vlpds-forwarded", token.as_str()), ("x-vlpds-resend", resend)];
        let rb = peer_client().get(&peer_url).query(&q);
        let (access, proof) = (access.clone(), proof.clone());
        let h: Vec<(String, String)> = h.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        async move {
            let h: Vec<(&str, &str)> = h.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            dpop_get_with(rb, &access, &proof, &h).await
        }
    };
    let (st, j) = peer("1f.0").await;
    assert_eq!(st, 200, "first attempt: {j}");
    let (st, j) = peer("1f.1").await;
    assert_eq!(st, 200, "the same request resent: {j}");
    let (st, j) = peer("2e.0").await;
    assert_eq!((st, j["error"].as_str()), (401, Some("invalid_dpop_proof")), "another request: {j}");
    let (st, j) = peer("2e.1").await;
    assert_eq!((st, j["error"].as_str()), (503, Some("ResendRefused")), "another request, resent: {j}");
    // a client's copy of the marker is dropped: a replay like any other
    let h = [("x-vlpds-forwarded", token.as_str()), ("x-vlpds-resend", "1f.1")];
    let (st, j) = dpop_get_with(public.get(&public_url).query(&q), &access, &proof, &h).await;
    assert_eq!((st, j["error"].as_str()), (401, Some("invalid_dpop_proof")), "client-sent marker: {j}");
    let (st, j) = dpop_get_with(public.get(&public_url).query(&q), &access, &proof, &[]).await;
    assert_eq!((st, j["error"].as_str()), (401, Some("invalid_dpop_proof")), "plain replay: {j}");
}
