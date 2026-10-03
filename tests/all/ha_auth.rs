//! Cluster-wide auth state: session revocations and record/blob takedowns
//! live in the account's own partition and are enforced by every node, also
//! after the owner hands its shards over; OAuth flows work whichever node
//! each step hits, with single-use codes, refresh tokens and DPoP proofs
//! checked once cluster-wide. In-process nodes share one in-memory object
//! store (as in admin_cluster.rs).

use crate::common::*;
use crate::oauth::DpopKey;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;

const SHARDS: u32 = 8;

/// A cluster node. `public` overrides the public URL (OAuth: every node
/// must present the same issuer), the node still forwards to its own address.
pub(crate) async fn node(id: &str, store: &Arc<object_store::memory::InMemory>, public: Option<&str>) -> TestServer {
    let public = public.map(String::from);
    cluster_node(id, store.clone(), SHARDS, |c| {
        if let Some(p) = public {
            c.public_url = p;
        }
    })
    .await
}

async fn get_record(s: &TestServer, r: &RecordRef) -> Resp {
    s.get_record(r.did(), r.collection(), r.rkey()).await
}

/// Every node sees `rec` / `blob` taken down and `revoked` rejected, while
/// `live` still authenticates.
async fn enforced_everywhere(nodes: &[&TestServer], rec: &RecordRef, did: &str, blob: &str, revoked: &str, live: &str) {
    for s in nodes {
        retry("takedowns and revocations enforced", || async {
            let r = get_record(s, rec).await;
            let b = s.get_blob(did, blob).await;
            let dead = s.get_session(&Auth::Bearer(revoked.into())).await;
            let ok = s.get_session(&Auth::Bearer(live.into())).await;
            (r.error_name() == Some("RecordNotFound") && b.error_name() == Some("BlobNotFound") && dead.error_name() == Some("ExpiredToken") && ok.is_ok())
                .then_some(())
        })
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takedowns_and_revocations_are_cluster_wide_and_survive_failover() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node("hat-a", &store, None).await;
    let b = node("hat-b", &store, None).await;
    let c = node("hat-c", &store, None).await;
    balanced(&[&a, &b, &c]).await;

    // an account owned by a; everything below goes through b and c
    let acct = a.create_account("hat").await;
    assert!(std::ptr::eq(owner_of(&[&a, &b, &c], &acct.did), &a));
    let rec = b.create_record(&acct, "app.bsky.feed.post", post_record("taken down soon")).await;
    let keep = c.create_record(&acct, "app.bsky.feed.post", post_record("stays")).await;
    let blob = c.upload_blob(&acct, b"some blob bytes", "image/png").await["ref"]["$link"].as_str().unwrap().to_string();

    // record takedown through b, blob takedown through c (routed by subject)
    let strong_ref = json!({"$type": "com.atproto.repo.strongRef", "uri": rec.uri, "cid": rec.cid});
    let status = |n: &TestServer, subject: J, takedown: J| {
        n.xrpc.post_owned("com.atproto.admin.updateSubjectStatus", json!({"subject": subject, "takedown": takedown}), Auth::Admin)
    };
    status(&b, strong_ref.clone(), json!({"applied": true, "ref": "t1"})).await.ok();
    status(&c, json!({"$type": "com.atproto.admin.defs#repoBlobRef", "did": acct.did, "cid": blob}), json!({"applied": true})).await.ok();
    for s in [&a, &b, &c] {
        let st = s.xrpc.get("com.atproto.admin.getSubjectStatus", &[("uri", &rec.uri)], &Auth::Admin).await.ok();
        assert_eq!(st["takedown"], json!({"applied": true, "ref": "t1"}), "{st}");
        let st = s.xrpc.get("com.atproto.admin.getSubjectStatus", &[("did", &acct.did), ("blob", &blob)], &Auth::Admin).await.ok();
        assert_eq!(st["takedown"]["applied"], true, "{st}");
        let l = s.list_records(&acct.did, "app.bsky.feed.post", &[]).await.ok();
        let uris: Vec<&str> = l["records"].as_array().unwrap().iter().map(|r| r["uri"].as_str().unwrap()).collect();
        assert_eq!(uris, vec![keep.uri.as_str()], "listRecords hides the taken-down record");
        get_record(s, &keep).await.ok();
    }

    // a second session, ended with deleteSession through c (its owner, a,
    // records the revocation in the account's partition)
    let sess = b.create_session(&acct.handle, PASSWORD).await.ok();
    let (access, refresh) = (sess["accessJwt"].as_str().unwrap().to_string(), sess["refreshJwt"].as_str().unwrap().to_string());
    c.get_session(&Auth::Bearer(access.clone())).await.ok();
    c.xrpc.post_empty("com.atproto.server.deleteSession", &Auth::Bearer(refresh)).await.ok();
    enforced_everywhere(&[&a, &b, &c], &rec, &acct.did, &blob, &access, &acct.access).await;

    // failover: a hands its shards over; the new owner reads the account's
    // revocations and takedowns back from its partition
    vlpds::server::shutdown(&a.app).await;
    balanced(&[&b, &c]).await;
    let new_owner = owner_of(&[&b, &c], &acct.did);
    assert!(!std::ptr::eq(new_owner, &a));
    enforced_everywhere(&[&b, &c], &rec, &acct.did, &blob, &access, &acct.access).await;

    // restart: a fresh node joins and takes shards; same answers there
    let d = node("hat-d", &store, None).await;
    balanced(&[&b, &c, &d]).await;
    enforced_everywhere(&[&b, &c, &d], &rec, &acct.did, &blob, &access, &acct.access).await;

    // lifting the takedown (through any node) is seen everywhere too
    status(&d, strong_ref, json!({"applied": false})).await.ok();
    for s in [&b, &c, &d] {
        retry("takedown lifted", || async { get_record(s, &rec).await.is_ok().then_some(()) }).await;
    }
}

// ---------- OAuth across nodes ----------

pub(crate) const PUBLIC: &str = "http://pds.cluster.test";

fn http() -> reqwest::Client {
    reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap()
}

/// POST to an AS endpoint of `node` with a fresh DPoP proof (htu on the
/// shared issuer); retries once for a nonce. `proof` overrides the proof.
pub(crate) async fn as_post(node: &TestServer, key: &DpopKey, path: &str, pairs: &[(&str, &str)], proof: Option<&str>) -> (u16, J) {
    for attempt in 0..2 {
        let p = proof.map(String::from).unwrap_or_else(|| key.proof("POST", &format!("{PUBLIC}{path}"), None));
        let r = http()
            .post(format!("{}{path}", node.url))
            .header("content-type", "application/x-www-form-urlencoded")
            .header("dpop", p)
            .body(form_body(pairs))
            .send()
            .await
            .unwrap();
        let status = r.status().as_u16();
        if let Some(n) = r.headers().get("dpop-nonce") {
            *key.nonce.lock() = Some(n.to_str().unwrap().to_string());
        }
        let body: J = r.json().await.unwrap_or(J::Null);
        if attempt == 0 && proof.is_none() && body["error"] == "use_dpop_nonce" {
            continue;
        }
        return (status, body);
    }
    unreachable!()
}

/// DPoP-authenticated XRPC call on `node`, with an explicit proof.
async fn xrpc_dpop(node: &TestServer, token: &str, proof: &str, nsid: &str, body: &J) -> (u16, J, Option<String>) {
    let r = http()
        .post(format!("{}/xrpc/{nsid}", node.url))
        .header("authorization", format!("DPoP {token}"))
        .header("dpop", proof)
        .json(body)
        .send()
        .await
        .unwrap();
    let status = r.status().as_u16();
    let nonce = r.headers().get("dpop-nonce").map(|v| v.to_str().unwrap().to_string());
    (status, r.json().await.unwrap_or(J::Null), nonce)
}

#[derive(Default)]
pub(crate) struct Browser {
    cookie: Option<String>,
}

impl Browser {
    async fn send(&mut self, rb: reqwest::RequestBuilder) -> (u16, reqwest::header::HeaderMap, String) {
        let rb = match &self.cookie {
            Some(c) => rb.header("cookie", c),
            None => rb,
        };
        let r = rb.send().await.unwrap();
        let (status, headers) = (r.status().as_u16(), r.headers().clone());
        for sc in headers.get_all("set-cookie") {
            let c = sc.to_str().unwrap().split(';').next().unwrap();
            if c.starts_with("vlpds-device=") {
                self.cookie = Some(c.to_string());
            }
        }
        (status, headers, r.text().await.unwrap())
    }

    pub(crate) async fn get(&mut self, node: &TestServer, path: &str) -> (u16, reqwest::header::HeaderMap, String) {
        self.send(http().get(format!("{}{path}", node.url))).await
    }

    pub(crate) async fn post(&mut self, node: &TestServer, path: &str, pairs: &[(&str, &str)]) -> (u16, reqwest::header::HeaderMap, String) {
        let rb = http().post(format!("{}{path}", node.url)).header("content-type", "application/x-www-form-urlencoded").body(form_body(pairs));
        self.send(rb).await
    }
}

pub(crate) fn csrf_of(html: &str) -> String {
    let i = html.find("name=\"csrf\" value=\"").expect("csrf field") + "name=\"csrf\" value=\"".len();
    html[i..i + html[i..].find('"').unwrap()].to_string()
}

fn location_params(h: &reqwest::header::HeaderMap) -> HashMap<String, String> {
    let loc = h.get("location").expect("location").to_str().unwrap();
    let q = loc.split_once(['?', '#']).map(|(_, q)| q).unwrap_or("");
    vlpds::oauth::util::parse_form(q).into_iter().collect()
}

pub(crate) struct Client {
    pub(crate) id: String,
    redirect: String,
    pub(crate) key: DpopKey,
}

impl Client {
    pub(crate) fn new() -> Client {
        let redirect = "http://127.0.0.1/callback".to_string();
        let enc = vlpds::oauth::util::form_encode_component;
        let id = format!("http://localhost?scope={}&redirect_uri={}", enc("atproto transition:generic"), enc(&redirect));
        Client { id, redirect, key: DpopKey::new() }
    }

    /// PAR on `par`, then the browser: authorization page on `page`, sign-in
    /// on `sign_in`, consent on `consent`. Returns (code, verifier).
    pub(crate) async fn authorize(&self, b: &mut Browser, [par, page, sign_in, consent]: [&TestServer; 4], handle: &str, did: &str) -> (String, String) {
        let verifier = rand_b64url(32);
        let challenge = b64url(Sha256::digest(&verifier));
        let (st, j) = as_post(
            par,
            &self.key,
            "/oauth/par",
            &[
                ("client_id", &self.id),
                ("response_type", "code"),
                ("redirect_uri", &self.redirect),
                ("scope", "atproto transition:generic"),
                ("state", "st"),
                ("code_challenge", &challenge),
                ("code_challenge_method", "S256"),
            ],
            None,
        )
        .await;
        assert_eq!(st, 201, "PAR: {j}");
        let request_uri = j["request_uri"].as_str().unwrap().to_string();
        let enc = vlpds::oauth::util::form_encode_component;
        let (st, _, html) = b.get(page, &format!("/oauth/authorize?client_id={}&request_uri={}", enc(&self.id), enc(&request_uri))).await;
        assert_eq!(st, 200, "{html}");
        assert!(html.contains("name=\"password\""), "sign-in page: {html}");
        let (st, _, html) = b
            .post(
                sign_in,
                "/oauth/authorize/sign-in",
                &[("request_uri", &request_uri), ("csrf", &csrf_of(&html)), ("identifier", handle), ("password", PASSWORD), ("action", "sign-in")],
            )
            .await;
        assert_eq!(st, 200, "{html}");
        assert!(html.contains("Authorize access"), "consent page: {html}");
        let (st, h, html) =
            b.post(consent, "/oauth/authorize/consent", &[("request_uri", &request_uri), ("csrf", &csrf_of(&html)), ("did", did), ("action", "allow")]).await;
        assert_eq!(st, 303, "{html}");
        let q = location_params(&h);
        assert_eq!(q.get("iss").map(String::as_str), Some(PUBLIC));
        (q.get("code").expect("code").clone(), verifier)
    }

    pub(crate) async fn exchange(&self, node: &TestServer, code: &str, verifier: &str) -> (u16, J) {
        as_post(
            node,
            &self.key,
            "/oauth/token",
            &[("grant_type", "authorization_code"), ("client_id", &self.id), ("code", code), ("redirect_uri", &self.redirect), ("code_verifier", verifier)],
            None,
        )
        .await
    }

    pub(crate) async fn refresh(&self, node: &TestServer, rt: &str) -> (u16, J) {
        as_post(node, &self.key, "/oauth/token", &[("grant_type", "refresh_token"), ("client_id", &self.id), ("refresh_token", rt)], None).await
    }

    /// createRecord with the access token on `node` (fresh proof, nonce retry).
    pub(crate) async fn create_post(&self, node: &TestServer, token: &str, did: &str) -> (u16, J) {
        let body = json!({"repo": did, "collection": "app.bsky.feed.post", "record": post_record("via oauth")});
        let htu = format!("{PUBLIC}/xrpc/com.atproto.repo.createRecord");
        for _ in 0..2 {
            let (st, j, nonce) = xrpc_dpop(node, token, &self.key.proof("POST", &htu, Some(token)), "com.atproto.repo.createRecord", &body).await;
            if let Some(n) = nonce {
                *self.key.nonce.lock() = Some(n);
            }
            if st == 401 && j["error"] == "use_dpop_nonce" {
                continue;
            }
            return (st, j);
        }
        unreachable!()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oauth_flow_across_nodes_single_use_cluster_wide() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node("hao-a", &store, Some(PUBLIC)).await;
    let b = node("hao-b", &store, Some(PUBLIC)).await;
    let c = node("hao-c", &store, Some(PUBLIC)).await;
    let all = [&a, &b, &c];
    balanced(&all).await;
    // an account per node, so some step is always off the owner
    let accts = [a.create_account("hao").await, b.create_account("hao").await, c.create_account("hao").await];

    for (i, acct) in accts.iter().enumerate() {
        let n = |k: usize| all[(i + k) % 3];
        let client = Client::new();
        let mut browser = Browser::default();
        // every step of the flow on another node than the one before; the
        // consent POST off the pushed request's node, so its 303 to the
        // client is forwarded back as is (not followed by the forwarder)
        let (code, verifier) = client.authorize(&mut browser, [n(0), n(1), n(2), n(1)], &acct.handle, &acct.did).await;
        let (st, t) = client.exchange(n(1), &code, &verifier).await;
        assert_eq!(st, 200, "token: {t}");
        assert_eq!(t["sub"], acct.did.as_str());
        let access = t["access_token"].as_str().unwrap().to_string();
        let rt = t["refresh_token"].as_str().unwrap().to_string();
        for k in 0..3 {
            let (st, j) = client.create_post(n(k), &access, &acct.did).await;
            assert_eq!(st, 200, "createRecord via node {k}: {j}");
        }

        // the same DPoP proof on two nodes: used once
        let htu = format!("{PUBLIC}/xrpc/com.atproto.repo.createRecord");
        let proof = client.key.proof("POST", &htu, Some(&access));
        let body = json!({"repo": acct.did, "collection": "app.bsky.feed.post", "record": post_record("x")});
        let (st, j, _) = xrpc_dpop(n(1), &access, &proof, "com.atproto.repo.createRecord", &body).await;
        assert_eq!(st, 200, "{j}");
        let (st, j, _) = xrpc_dpop(n(2), &access, &proof, "com.atproto.repo.createRecord", &body).await;
        assert_eq!((st, j["error"].as_str()), (401, Some("invalid_dpop_proof")), "{j}");
        // ... also at the token endpoint (proofs there are claimed per key)
        let p = client.key.proof("POST", &format!("{PUBLIC}/oauth/token"), None);
        let bogus = [("grant_type", "refresh_token"), ("client_id", client.id.as_str()), ("refresh_token", "ref-bogus")];
        let (_, j) = as_post(n(0), &client.key, "/oauth/token", &bogus, Some(&p)).await;
        assert_ne!(j["error"], "invalid_dpop_proof", "{j}");
        let (_, j) = as_post(n(1), &client.key, "/oauth/token", &bogus, Some(&p)).await;
        assert_eq!(j["error"], "invalid_dpop_proof", "{j}");

        // refresh through yet another node; rotated-out tokens are dead everywhere
        let (st, t2) = client.refresh(n(2), &rt).await;
        assert_eq!(st, 200, "refresh: {t2}");
        let access2 = t2["access_token"].as_str().unwrap().to_string();
        let (st, _) = client.create_post(n(0), &access, &acct.did).await;
        assert_eq!(st, 401, "rotated-out access token");
        let (st, j) = client.create_post(n(1), &access2, &acct.did).await;
        assert_eq!(st, 200, "{j}");

        // the device's account page on a node that doesn't own the account
        let (st, _, html) = browser.get(n(1), "/oauth/account").await;
        assert_eq!(st, 200, "{html}");
        assert!(html.contains(&acct.handle) && html.contains("localhost"), "account page lists the grant: {html}");

        // revocation through a third node ends the session
        let (st, j) = as_post(n(2), &client.key, "/oauth/revoke", &[("client_id", &client.id), ("token", t2["refresh_token"].as_str().unwrap())], None).await;
        assert_eq!(st, 200, "{j}");
        let (st, _) = client.create_post(n(0), &access2, &acct.did).await;
        assert_eq!(st, 401, "revoked session");
    }

    // double code exchange on two nodes at once: exactly one wins
    for acct in &accts {
        let client = Client::new();
        let mut browser = Browser::default();
        let (code, verifier) = client.authorize(&mut browser, [&a, &b, &c, &a], &acct.handle, &acct.did).await;
        let (r1, r2) = tokio::join!(client.exchange(&b, &code, &verifier), client.exchange(&c, &code, &verifier));
        let oks = [&r1, &r2].iter().filter(|(st, _)| *st == 200).count();
        assert_eq!(oks, 1, "exactly one exchange succeeds: {r1:?} {r2:?}");
        let lost = if r1.0 == 200 { &r2 } else { &r1 };
        assert_eq!(lost.1["error"], "invalid_grant", "{lost:?}");

        // and two concurrent refreshes of one token: one rotation
        let client = Client::new();
        let mut browser = Browser::default();
        let (code, verifier) = client.authorize(&mut browser, [&c, &a, &b, &c], &acct.handle, &acct.did).await;
        let (st, t) = client.exchange(&a, &code, &verifier).await;
        assert_eq!(st, 200, "{t}");
        let rt = t["refresh_token"].as_str().unwrap();
        let (r1, r2) = tokio::join!(client.refresh(&b, rt), client.refresh(&c, rt));
        let oks = [&r1, &r2].iter().filter(|(st, _)| *st == 200).count();
        assert_eq!(oks, 1, "exactly one refresh succeeds: {r1:?} {r2:?}");
    }
}

/// A user service JWT on uploadBlob (the video service uploading for a
/// user) carries no `sub`: the other nodes route it by its `iss` to the
/// account's owner, which verifies it and stores the blob as the user's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_service_auth_uploads_route_to_the_owner() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node("husa-a", &store, None).await;
    let b = node("husa-b", &store, None).await;
    let c = node("husa-c", &store, None).await;
    let nodes = [&a, &b, &c];
    balanced(&nodes).await;
    let acct = a.create_account("husa").await;
    let owner = owner_of(&nodes, &acct.did);
    let pds = a.pds_did().await;
    for n in nodes.iter().filter(|n| !std::ptr::eq(**n, owner)) {
        let q = [("aud", pds.as_str()), ("lxm", "com.atproto.repo.uploadBlob")];
        let tok = n.xrpc.get("com.atproto.server.getServiceAuth", &q, &acct.auth()).await.ok()["token"].as_str().unwrap().to_string();
        let bytes = format!("video bytes via {}", n.url).into_bytes();
        let up = n.xrpc.post_bytes("com.atproto.repo.uploadBlob", bytes.clone(), "video/mp4", &Auth::Bearer(tok)).await.ok();
        let cid = up["blob"]["ref"]["$link"].as_str().unwrap().to_string();
        let embed = json!({"$type": "app.bsky.embed.video", "video": up["blob"]});
        n.create_record(&acct, "app.bsky.feed.post", json!({"$type": "app.bsky.feed.post", "text": "v", "createdAt": now_iso(), "embed": embed})).await;
        let g = n.get_blob(&acct.did, &cid).await;
        assert_eq!(g.status, 200, "{}", g.text());
        assert_eq!(g.body.as_ref() as &[u8], bytes.as_slice());
    }
}
