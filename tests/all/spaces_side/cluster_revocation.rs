//! Revocation across a cluster (plan §2.6, phase 2): a credential revoked
//! with notifyCredentialRevoked on one node is refused on every other node
//! within 1 s of the 200, including nodes that had it cached; and it stays
//! refused on a node that restarts, one that was down when it was revoked
//! (its nudge never arrived), and one that joins afterwards.

use super::cluster::Plc;
use super::ref_net::{post_service, service_jwt};
use crate::common::spaces::{signed_get_as, SpaceClient};
use crate::common::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

const SHARDS: u32 = 6;
const REVOKE: &str = "com.atproto.space.notifyCredentialRevoked";
const SLA: Duration = Duration::from_secs(1);

type Bucket = Arc<object_store::memory::InMemory>;

fn tag() -> String {
    random_bytes(5).iter().map(|b| format!("{b:02x}")).collect()
}

fn jti(credential: &str) -> String {
    use base64::Engine;
    let payload = credential.split('.').nth(1).expect("a JWT");
    let claims: J = serde_json::from_slice(&base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload).unwrap())
        .expect("JSON claims");
    claims["jti"].as_str().expect("jti").to_string()
}

/// The authority (on `entry`), a member whose repo `owner` holds, the space
/// and the member's record in it.
struct Fixture {
    auth: SpaceClient,
    member: SpaceClient,
    space: String,
    collection: String,
    http: reqwest::Client,
}

impl Fixture {
    async fn new(entry: &TestServer, owner: &TestServer) -> Fixture {
        let t = tag();
        let (st, collection) = (format!("com.example.rv{t}.space"), format!("com.example.rv{t}.note"));
        let scope = format!(
            "space:{st}?authority=*&collection={collection}&action=read&action=create&manage=create&manage=update"
        );
        let auth = SpaceClient::new(entry, &unique_name("rva"), &scope).await;
        let member = SpaceClient::new(owner, &unique_name("rvm"), &scope).await;
        let space = auth.create_space(&st, "rv").await;
        let m = json!({"space": space, "did": member.did, "read": true, "write": true});
        auth.post("com.atproto.simplespace.putMember", m).await.ok();
        let rec = json!({"$type": collection, "text": "revocation target", "createdAt": now_iso()});
        member.create_record(&space, &collection, Some("r"), rec).await.ok();
        Fixture { auth, member, space, collection, http: reqwest::Client::new() }
    }

    /// getRecord of the member's record at `node` with `credential`, retried
    /// while the node answers 503 (shards moving, revocations still loading).
    async fn read(&self, node: &TestServer, credential: &str) -> Resp {
        let q = [
            ("space", self.space.as_str()),
            ("repo", self.member.did.as_str()),
            ("collection", self.collection.as_str()),
            ("rkey", "r"),
        ];
        let t = Instant::now();
        loop {
            let r = signed_get_as(
                &self.http,
                &self.auth.holder,
                &node.url,
                "com.atproto.space.getRecord",
                &q,
                credential,
                &self.member.did,
            )
            .await;
            if r.status != 503 || t.elapsed() > Duration::from_secs(20) {
                return r;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn revoke(&self, at: &TestServer, credentials: &[&str]) -> Resp {
        let jwt = service_jwt(&self.auth, &self.member.did, REVOKE).await;
        let jtis: Vec<String> = credentials.iter().map(|c| jti(c)).collect();
        post_service(&at.url, REVOKE, &jwt, json!({"space": self.space, "credentials": jtis})).await
    }
}

fn refused(r: &Resp, ctx: &str) {
    assert_eq!(r.error_name(), Some("CredentialRevoked"), "{ctx}: {} {}", r.status, r.text());
}

/// Polls a read at `node` until it's refused; how long that took from `since`.
async fn refused_within(fx: &Fixture, node: &TestServer, credential: &str, since: Instant, ctx: &str) -> Duration {
    loop {
        let r = fx.read(node, credential).await;
        if r.error_name() == Some("CredentialRevoked") {
            return since.elapsed();
        }
        assert_eq!(r.status, 200, "{ctx}: neither served nor refused as revoked: {}", r.text());
        assert!(
            since.elapsed() < Duration::from_secs(10),
            "{ctx}: still served {:?} after the revoke",
            since.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Three nodes, the credential warm in every node's cache; revoked at a
/// node that doesn't hold the member's repo.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_revocation_on_one_node_is_refused_on_every_node_within_a_second() {
    let (bucket, plc) = (Bucket::default(), Plc::start().await);
    let node = |id: &'static str| super::cluster::node(id, &bucket, SHARDS, &plc);
    let (n1, n2, n3) = (node("rv-1").await, node("rv-2").await, node("rv-3").await);
    let nodes = [&n1, &n2, &n3];
    balanced(&nodes).await;
    let fx = Fixture::new(&n1, &n2).await;
    let revoked = fx.auth.credential(&fx.space).await;
    let kept = fx.auth.credential(&fx.space).await;
    assert_ne!(jti(&revoked), jti(&kept));
    for n in nodes {
        for c in [&revoked, &kept] {
            // twice: the second is a cache hit
            fx.read(n, c).await.ok();
            fx.read(n, c).await.ok();
        }
    }
    let sent = Instant::now();
    fx.revoke(&n3, &[&revoked]).await.ok();
    let acked = Instant::now();
    eprintln!("notifyCredentialRevoked took {:?}", acked - sent);
    for n in nodes {
        let id = &cluster(n).cfg.node_id;
        let took = refused_within(&fx, n, &revoked, acked, id).await;
        eprintln!("{id}: refused {took:?} after the revoke's 200");
        assert!(took <= SLA, "{id}: the revoked credential was served for {took:?} after the 200");
        // and it stays refused
        refused(&fx.read(n, &revoked).await, id);
        fx.read(n, &kept).await.ok();
    }
}

/// A peer that restarts reads the revocations back before it serves a
/// credential; one that was down when a credential was revoked (no nudge
/// reached it) and one that joins afterwards refuse it too.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn revocations_hold_across_restarts_missed_nudges_and_joins() {
    let (bucket, plc) = (Bucket::default(), Plc::start().await);
    let node = |id: &'static str| super::cluster::node(id, &bucket, SHARDS, &plc);
    let (n1, n2) = (node("rvr-1").await, node("rvr-2").await);
    balanced(&[&n1, &n2]).await;
    let fx = Fixture::new(&n1, &n2).await;
    let (first, second, kept) =
        (fx.auth.credential(&fx.space).await, fx.auth.credential(&fx.space).await, fx.auth.credential(&fx.space).await);
    for c in [&first, &second, &kept] {
        fx.read(&n1, c).await.ok();
        fx.read(&n2, c).await.ok();
    }
    fx.revoke(&n1, &[&first]).await.ok();
    refused(&fx.read(&n2, &first).await, "rvr-2 before its restart");

    // a graceful restart of the node that owns the member's repo
    vlpds::server::shutdown(&n2.app).await;
    let n2 = node("rvr-2").await;
    balanced(&[&n1, &n2]).await;
    refused(&fx.read(&n2, &first).await, "rvr-2 after its restart");
    fx.read(&n2, &second).await.ok();

    // revoked while rvr-2 is down: its nudge goes nowhere
    vlpds::server::shutdown(&n2.app).await;
    wait_until("rvr-1 holds every shard", Duration::from_secs(30), || owned(&n1) == SHARDS as usize).await;
    fx.revoke(&n1, &[&second]).await.ok();
    refused(&fx.read(&n1, &second).await, "rvr-1");
    let n2 = node("rvr-2").await;
    let n3 = node("rvr-3").await;
    balanced(&[&n1, &n2, &n3]).await;
    for n in [&n1, &n2, &n3] {
        let id = &cluster(n).cfg.node_id;
        refused(&fx.read(n, &first).await, &format!("{id}: first"));
        refused(&fx.read(n, &second).await, &format!("{id}: second"));
        fx.read(n, &kept).await.ok();
    }
}
