//! Account migration in, between two in-process vlpds servers, following
//! the reference flow (createAccount with an existing DID and service auth
//! from the old PDS -> importRepo -> listMissingBlobs / uploadBlob ->
//! checkAccountStatus -> DID document update -> activateAccount ->
//! deactivate on the old PDS). A stub PLC directory serves the DID
//! documents both servers resolve.

use crate::common::*;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type Docs = Arc<Mutex<HashMap<String, J>>>;

/// A PLC directory stand-in: `GET /{did}` -> the document set for it.
async fn stub_plc() -> (String, Docs) {
    let docs: Docs = Arc::default();
    let app = axum::Router::new()
        .route(
            "/{did}",
            get(|State(d): State<Docs>, Path(did): Path<String>| async move {
                match d.lock().unwrap().get(&did) {
                    Some(doc) => Ok(axum::Json(doc.clone())),
                    None => Err(StatusCode::NOT_FOUND),
                }
            }),
        )
        .with_state(docs.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (url, docs)
}

async fn pds(plc: &str, service_did: &str) -> TestServer {
    let (plc, sd) = (plc.to_string(), service_did.to_string());
    TestServer::spawn_with(move |c| {
        c.plc_url = plc;
        c.service_did = sd;
    })
    .await
}

async fn service_auth(s: &TestServer, a: &TestAccount, aud: &str, lxm: &str) -> String {
    s.xrpc.get("com.atproto.server.getServiceAuth", &[("aud", aud), ("lxm", lxm)], &a.auth()).await.ok()["token"].as_str().unwrap().to_string()
}

async fn account_status(s: &TestServer, auth: &Auth) -> J {
    s.xrpc.get("com.atproto.server.checkAccountStatus", &[], auth).await.ok()
}

const PNG: &[u8] = b"\x89PNG\r\n\x1a\n migrated image bytes";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn migrate_account_with_records_and_blobs() {
    let (plc, docs) = stub_plc().await;
    let old = pds(&plc, "did:web:old-pds.test").await;
    let new = pds(&plc, "did:web:new-pds.test").await;
    let new_did = new.xrpc.get("com.atproto.server.describeServer", &[], &Auth::None).await.ok()["did"].as_str().unwrap().to_string();
    assert_eq!(new_did, "did:web:new-pds.test");

    // an account on the old PDS: posts, one with an image blob
    let alice = old.create_account("mig").await;
    let did = alice.did.clone();
    let mut posts = Vec::new();
    for i in 0..3 {
        posts.push(old.post(&alice, &format!("post {i}")).await);
    }
    let up = old.xrpc.post_bytes("com.atproto.repo.uploadBlob", PNG.to_vec(), "image/png", &alice.auth()).await.ok();
    let blob = up["blob"].clone();
    let blob_cid = blob["ref"]["$link"].as_str().unwrap().to_string();
    let with_image = json!({
        "$type": "app.bsky.feed.post", "text": "with image", "createdAt": now_iso(),
        "embed": {"$type": "app.bsky.embed.images", "images": [{"image": blob, "alt": "x"}]},
    });
    posts.push(old.create_record(&alice, "app.bsky.feed.post", with_image).await);
    // its DID document as published by the old PDS
    let doc = old.xrpc.get("com.atproto.identity.resolveDid", &[("did", &did)], &Auth::None).await.ok()["didDoc"].clone();
    docs.lock().unwrap().insert(did.clone(), doc);

    let handle = format!("{}.{HANDLE_DOMAIN}", unique_name("moved"));
    let email = format!("{}@example.com", unique_name("moved"));
    let body = json!({"handle": handle, "email": email, "password": PASSWORD, "did": did});

    // bringing a DID requires service auth from it, for this method and PDS
    let r = new.xrpc.post("com.atproto.server.createAccount", &body, &Auth::None).await;
    r.err(401, "AuthenticationRequired");
    let wrong_lxm = service_auth(&old, &alice, &new_did, "com.atproto.repo.createRecord").await;
    let r = new.xrpc.post("com.atproto.server.createAccount", &body, &Auth::Bearer(wrong_lxm)).await;
    r.err(401, "BadJwtLexiconMethod");
    let wrong_aud = service_auth(&old, &alice, "did:web:old-pds.test", "com.atproto.server.createAccount").await;
    let r = new.xrpc.post("com.atproto.server.createAccount", &body, &Auth::Bearer(wrong_aud)).await;
    r.err(401, "BadJwtAudience");
    let other = old.create_account("mig").await;
    let others = service_auth(&old, &other, &new_did, "com.atproto.server.createAccount").await;
    let r = new.xrpc.post("com.atproto.server.createAccount", &body, &Auth::Bearer(others)).await;
    r.err(401, "AuthenticationRequired");
    let mut with_op = body.clone();
    with_op["plcOp"] = json!({});
    let token = service_auth(&old, &alice, &new_did, "com.atproto.server.createAccount").await;
    let r = new.xrpc.post("com.atproto.server.createAccount", &with_op, &Auth::Bearer(token.clone())).await;
    r.err(400, "InvalidRequest");

    // the new PDS announces nothing until activation
    let mut sub = new.subscribe_from_now().await;
    let created = new.xrpc.post("com.atproto.server.createAccount", &body, &Auth::Bearer(token.clone())).await.ok();
    assert_eq!(created["did"], json!(did));
    assert_eq!(created["handle"], json!(handle));
    let auth = Auth::Bearer(created["accessJwt"].as_str().unwrap().to_string());
    // the same DID can't be created twice
    let token2 = service_auth(&old, &alice, &new_did, "com.atproto.server.createAccount").await;
    new.xrpc.post("com.atproto.server.createAccount", &body, &Auth::Bearer(token2)).await.client_err();

    let st = account_status(&new, &auth).await;
    assert_eq!((st["activated"].clone(), st["validDid"].clone()), (json!(false), json!(false)), "{st}");
    assert_eq!((st["indexedRecords"].clone(), st["expectedBlobs"].clone()), (json!(0), json!(0)), "{st}");
    let rs = new.xrpc.get("com.atproto.sync.getRepoStatus", &[("did", &did)], &Auth::None).await.ok();
    assert_eq!((rs["active"].clone(), rs["status"].clone()), (json!(false), json!("deactivated")), "{rs}");

    // repo
    let car = old.xrpc.get("com.atproto.sync.getRepo", &[("did", &did)], &Auth::None).await;
    assert_eq!(car.status, 200);
    new.xrpc.post_bytes("com.atproto.repo.importRepo", car.body.to_vec(), "application/vnd.ipld.car", &auth).await.ok();
    let st = account_status(&new, &auth).await;
    assert_eq!(st["indexedRecords"], json!(4), "{st}");
    assert_eq!((st["expectedBlobs"].clone(), st["importedBlobs"].clone()), (json!(1), json!(0)), "{st}");
    // commit + record blocks + at least the MST root
    assert!(st["repoBlocks"].as_u64().unwrap() >= 6, "{st}");

    // blobs
    let missing = new.xrpc.get("com.atproto.repo.listMissingBlobs", &[], &auth).await.ok();
    let missing: Vec<&str> = missing["blobs"].as_array().unwrap().iter().map(|b| b["cid"].as_str().unwrap()).collect();
    assert_eq!(missing, vec![blob_cid.as_str()]);
    let bytes = old.xrpc.get("com.atproto.sync.getBlob", &[("did", &did), ("cid", &blob_cid)], &Auth::None).await;
    assert_eq!(bytes.status, 200);
    let up = new.xrpc.post_bytes("com.atproto.repo.uploadBlob", bytes.body.to_vec(), "image/png", &auth).await.ok();
    assert_eq!(up["blob"]["ref"]["$link"], json!(blob_cid));
    let missing = new.xrpc.get("com.atproto.repo.listMissingBlobs", &[], &auth).await.ok();
    assert_eq!(missing["blobs"], json!([]));
    let st = account_status(&new, &auth).await;
    assert_eq!((st["expectedBlobs"].clone(), st["importedBlobs"].clone()), (json!(1), json!(1)), "{st}");

    // activation needs the DID document to point here first
    let r = new.xrpc.post_empty("com.atproto.server.activateAccount", &auth).await;
    r.err(400, "InvalidRequest");
    let rec = new.xrpc.get("com.atproto.identity.getRecommendedDidCredentials", &[], &auth).await.ok();
    let key = rec["verificationMethods"]["atproto"].as_str().unwrap();
    let new_doc = json!({
        "@context": ["https://www.w3.org/ns/did/v1"],
        "id": did,
        "alsoKnownAs": rec["alsoKnownAs"],
        "verificationMethod": [{
            "id": format!("{did}#atproto"), "type": "Multikey", "controller": did,
            "publicKeyMultibase": key.strip_prefix("did:key:").unwrap(),
        }],
        "service": [{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": rec["services"]["atproto_pds"]["endpoint"]}],
    });
    assert_eq!(rec["services"]["atproto_pds"]["endpoint"], json!(new.url));
    docs.lock().unwrap().insert(did.clone(), new_doc);
    assert_eq!(account_status(&new, &auth).await["validDid"], json!(true));
    new.xrpc.post_empty("com.atproto.server.activateAccount", &auth).await.ok();
    let st = account_status(&new, &auth).await;
    assert_eq!(st["activated"], json!(true), "{st}");

    // the first events for the DID here are the activation's
    let frames = sub.until(FH_TIMEOUT, |fs| fs.iter().any(|f| f.did() == Some(did.as_str()) && f.kind() == "#sync")).await;
    let mine: Vec<&Frame> = frames.iter().filter(|f| f.did() == Some(did.as_str())).collect();
    let kinds: Vec<&str> = mine.iter().map(|f| f.kind()).collect();
    assert_eq!(kinds, vec!["#account", "#identity", "#sync"], "{mine:?}");
    assert_eq!(mine[0].bool("active"), Some(true));
    assert_eq!(mine[1].str("handle"), Some(handle.as_str()));

    // the old PDS steps back
    old.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &alice.auth()).await.ok();

    // the new PDS serves the repo and blob, signed with its own key, and takes writes
    let l = new.list_records(&did, "app.bsky.feed.post", &[]).await.ok();
    assert_eq!(l["records"].as_array().unwrap().len(), 4, "{l}");
    for p in &posts {
        let r = new.get_record(&did, p.collection(), p.rkey()).await.ok();
        assert_eq!(r["cid"], json!(p.cid));
    }
    let b = new.xrpc.get("com.atproto.sync.getBlob", &[("did", &did), ("cid", &blob_cid)], &Auth::None).await;
    assert_eq!((b.status, &b.body[..]), (200, PNG));
    let session = TestAccount {
        did: did.clone(),
        handle: handle.clone(),
        password: PASSWORD.into(),
        email: email.clone(),
        access: created["accessJwt"].as_str().unwrap().into(),
        refresh: created["refreshJwt"].as_str().unwrap().into(),
    };
    new.post(&session, "hello from the new PDS").await;
    let repo = new.get_repo(&did).await;
    repo.commit().verify(&new.signing_key(&did).await).expect("commit signed with the new key");
    assert_eq!(repo.entries().len(), 5);
    // and the password works there
    new.create_session(&handle, PASSWORD).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn import_of_another_dids_repo_is_accepted_like_the_reference() {
    let s = TestServer::spawn().await;
    let a = s.create_account("imp").await;
    let b = s.create_account("imp").await;
    let p = s.post(&a, "from a").await;
    let car = s.xrpc.get("com.atproto.sync.getRepo", &[("did", &a.did)], &Auth::None).await;
    s.xrpc.post_bytes("com.atproto.repo.importRepo", car.body.to_vec(), "application/vnd.ipld.car", &b.auth()).await.ok();
    // re-signed for b, with a's records
    let r = s.get_record(&b.did, p.collection(), p.rkey()).await.ok();
    assert_eq!(r["cid"], json!(p.cid));
    let repo = s.get_repo(&b.did).await;
    assert_eq!(repo.commit().did, b.did);
    repo.commit().verify(&s.signing_key(&b.did).await).unwrap();
}
