//! An entry node resends a DPoP-authenticated request the owner answered
//! ShardMoved / RepoLoading (`forward::with_retries`), with the client's
//! proof unchanged. The owner had already claimed that proof's `jti`, so it
//! gives the claim back with such an answer: the resend is served, and the
//! proof still authorizes only one request.

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

async fn dpop_get(entry: &TestServer, client: &Client, token: &str, nsid: &str, query: &[(&str, &str)], proof: &str) -> (u16, J) {
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
