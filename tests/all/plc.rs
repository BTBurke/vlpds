//! PLC identity (src/plc, DESIGN.md "PLC identity") against the in-process
//! mock PLC directory (`vlpds::plc::mock`; the real plc.directory is never
//! contacted): genesis registration at createAccount, handle and
//! signing-key updates, request/sign/submitPlcOperation, account migration
//! out to another PDS, and PLC failures (nothing acknowledged, nothing
//! changed locally).

use crate::common::*;
use std::sync::Arc;
use vlpds::crypto::Keypair;
use vlpds::plc::mock::MockPlc;
use vlpds::plc::{PlcConfig, RotationKey};

/// A PDS registering DIDs with `plc` under rotation key `key`.
async fn pds(plc: &MockPlc, key: &Arc<Keypair>, service_did: &str, recovery: Option<String>) -> TestServer {
    let (url, key, sd) = (plc.url.clone(), key.clone(), service_did.to_string());
    TestServer::spawn_with(move |c| {
        c.plc_url = url;
        c.service_did = sd;
        c.plc = PlcConfig { rotation_key: Some(RotationKey::Key(key)), recovery_did_key: recovery, ..Default::default() };
    })
    .await
}

fn new_key() -> Arc<Keypair> {
    Arc::new(Keypair::generate())
}

fn account_body(prefix: &str) -> (String, String, J) {
    let handle = format!("{}.{HANDLE_DOMAIN}", unique_name(prefix));
    let email = format!("{}@example.com", handle.replace('.', "-"));
    let body = json!({"handle": handle, "email": email, "password": PASSWORD});
    (handle, email, body)
}

async fn signing_did_key(s: &TestServer, did: &str) -> String {
    format!("did:key:{}", s.app.account(did).await.ok().unwrap().signing_pubkey)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_account_registers_the_genesis_op() {
    let plc = MockPlc::start().await;
    let rot = new_key();
    let s = pds(&plc, &rot, "did:web:pds.test", None).await;
    let a = s.create_account("gen").await;

    // exactly the reference genesis op, accepted by the directory
    let ops = plc.ops(&a.did);
    assert_eq!(ops.len(), 1, "{ops:?}");
    let op = &ops[0];
    assert_eq!(vlpds::plc::did_for_genesis(op).unwrap(), a.did, "DID = hash of the signed genesis op");
    assert_eq!(op["type"], "plc_operation");
    assert_eq!(op["prev"], J::Null);
    assert_eq!(op["rotationKeys"], json!([rot.did_key()]));
    assert_eq!(op["verificationMethods"], json!({"atproto": signing_did_key(&s, &a.did).await}));
    assert_eq!(op["alsoKnownAs"], json!([format!("at://{}", a.handle)]));
    assert_eq!(op["services"], json!({"atproto_pds": {"type": "AtprotoPersonalDataServer", "endpoint": s.url}}));
    vlpds::plc::verify_sig(&[rot.did_key()], op).expect("signed by the server rotation key");
    // the directory's document is the one this PDS serves
    let data = plc.data(&a.did).unwrap();
    let local = s.xrpc.get("com.atproto.identity.resolveDid", &[("did", &a.did)], &Auth::None).await.ok()["didDoc"].clone();
    let served = vlpds::plc::format_did_doc(&data);
    for k in ["id", "alsoKnownAs", "verificationMethod", "service"] {
        assert_eq!(served[k], local[k], "{k}");
    }
    // the server's rotation key is recommended
    let rec = s.xrpc.get("com.atproto.identity.getRecommendedDidCredentials", &[], &a.auth()).await.ok();
    assert_eq!(rec["rotationKeys"], json!([rot.did_key()]));
    // the account works: posts, activation check against the directory
    s.post(&a, "hello").await;
    let st = s.xrpc.get("com.atproto.server.checkAccountStatus", &[], &a.auth()).await.ok();
    assert_eq!(st["validDid"], json!(true), "{st}");

    // a user recovery key goes first (reference ordering)
    let user = Keypair::generate().did_key();
    let (_, _, mut body) = account_body("rk");
    body["recoveryKey"] = json!(user);
    let j = s.xrpc.post("com.atproto.server.createAccount", &body, &Auth::None).await.ok();
    let did = j["did"].as_str().unwrap();
    assert_eq!(plc.last_op(did).unwrap()["rotationKeys"], json!([user, rot.did_key()]));
    let (_, _, mut body) = account_body("rk");
    body["recoveryKey"] = json!("did:key:notakey");
    s.xrpc.post("com.atproto.server.createAccount", &body, &Auth::None).await.err(400, "InvalidRequest");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_recovery_key_is_ahead_of_the_rotation_key() {
    let plc = MockPlc::start().await;
    let (rot, recovery) = (new_key(), Keypair::generate().did_key());
    let s = pds(&plc, &rot, "did:web:pds.test", Some(recovery.clone())).await;
    let user = Keypair::generate().did_key();
    let (_, _, mut body) = account_body("srk");
    body["recoveryKey"] = json!(user);
    let j = s.xrpc.post("com.atproto.server.createAccount", &body, &Auth::None).await.ok();
    let did = j["did"].as_str().unwrap();
    assert_eq!(plc.last_op(did).unwrap()["rotationKeys"], json!([user, recovery, rot.did_key()]));
    let auth = Auth::Bearer(j["accessJwt"].as_str().unwrap().into());
    let rec = s.xrpc.get("com.atproto.identity.getRecommendedDidCredentials", &[], &auth).await.ok();
    assert_eq!(rec["rotationKeys"], json!([recovery, rot.did_key()]));
}

/// createAccount is not acknowledged before the directory accepted the
/// genesis op; a refusal or outage leaves nothing behind (the handle and
/// email are free again, no account, no events).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plc_failure_at_create_leaves_nothing() {
    let plc = MockPlc::start().await;
    let s = pds(&plc, &new_key(), "did:web:pds.test", None).await;
    let (handle, email, body) = account_body("fail");
    let mut sub = s.subscribe_from_now().await;
    for (n, status) in [(1, 500), (1, 400), (1, 429)] {
        plc.fail_posts(n, status);
        let r = s.xrpc.post("com.atproto.server.createAccount", &body, &Auth::None).await;
        r.err(500, "InternalServerError");
        assert!(r.text().contains("PLC directory"), "{}", r.text());
    }
    plc.set_down(true);
    s.xrpc.post("com.atproto.server.createAccount", &body, &Auth::None).await.err(500, "InternalServerError");
    plc.set_down(false);
    assert!(plc.dids().is_empty());
    s.xrpc.get("com.atproto.identity.resolveHandle", &[("handle", &handle)], &Auth::None).await.err(400, "HandleNotFound");
    s.create_session(&email, PASSWORD).await.client_err();
    assert!(sub.next(std::time::Duration::from_millis(300)).await.is_none(), "no events for a failed creation");
    // the same handle and email then work
    let j = s.xrpc.post("com.atproto.server.createAccount", &body, &Auth::None).await.ok();
    assert_eq!(j["handle"], json!(handle));
    assert_eq!(plc.dids(), vec![j["did"].as_str().unwrap().to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_handle_submits_a_plc_update_first() {
    let plc = MockPlc::start().await;
    let s = pds(&plc, &new_key(), "did:web:pds.test", None).await;
    let a = s.create_account("uh").await;
    let mut sub = s.subscribe_from_now().await;
    let h2 = format!("{}.{HANDLE_DOMAIN}", unique_name("uh2"));
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": h2}), &a.auth()).await.ok();
    let ops = plc.ops(&a.did);
    assert_eq!(ops.len(), 2);
    assert_eq!(ops[1]["alsoKnownAs"], json!([format!("at://{h2}")]));
    assert_eq!(ops[1]["prev"], json!(vlpds::plc::op_cid(&ops[0]).unwrap().to_string()));
    assert_eq!(ops[1]["verificationMethods"], ops[0]["verificationMethods"]);
    let frames = sub.until(FH_TIMEOUT, |fs| fs.iter().any(|f| f.kind() == "#identity" && f.did() == Some(a.did.as_str()))).await;
    let id = frames.iter().find(|f| f.kind() == "#identity").unwrap();
    assert_eq!(id.str("handle"), Some(h2.as_str()));
    // the same handle again: re-announced, nothing new for the directory
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": h2}), &a.auth()).await.ok();
    assert_eq!(plc.ops(&a.did).len(), 2);

    // a failed PLC update changes nothing here, and frees the new handle
    let h3 = format!("{}.{HANDLE_DOMAIN}", unique_name("uh3"));
    plc.fail_posts(1, 503);
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": h3}), &a.auth()).await.err(500, "InternalServerError");
    assert_eq!(s.app.account(&a.did).await.ok().unwrap().handle, h2);
    assert_eq!(plc.ops(&a.did).len(), 2);
    let r = s.xrpc.get("com.atproto.identity.resolveHandle", &[("handle", &h2)], &Auth::None).await.ok();
    assert_eq!(r["did"], json!(a.did));
    let b = s.create_account("uhb").await;
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": h3}), &b.auth()).await.ok();
    // admin updateAccountHandle goes through PLC too
    let h4 = format!("{}.{HANDLE_DOMAIN}", unique_name("uh4"));
    s.xrpc.post("com.atproto.admin.updateAccountHandle", &json!({"did": a.did, "handle": h4}), &Auth::Admin).await.ok();
    assert_eq!(plc.last_op(&a.did).unwrap()["alsoKnownAs"], json!([format!("at://{h4}")]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signing_key_rotation_updates_the_directory() {
    let plc = MockPlc::start().await;
    let s = pds(&plc, &new_key(), "did:web:pds.test", None).await;
    let a = s.create_account("sk").await;
    let before = signing_did_key(&s, &a.did).await;
    plc.fail_posts(1, 500);
    s.xrpc.post("com.atproto.admin.updateAccountSigningKey", &json!({"did": a.did}), &Auth::Admin).await.err(500, "InternalServerError");
    assert_eq!(signing_did_key(&s, &a.did).await, before, "no local change on PLC failure");
    let j = s.xrpc.post("com.atproto.admin.updateAccountSigningKey", &json!({"did": a.did}), &Auth::Admin).await.ok();
    let new = j["signingKey"].as_str().unwrap();
    assert_ne!(new, before);
    assert_eq!(signing_did_key(&s, &a.did).await, new);
    assert_eq!(plc.data(&a.did).unwrap()["verificationMethods"]["atproto"], json!(new));
    s.post(&a, "signed with the new key").await;
    let st = s.xrpc.get("com.atproto.server.checkAccountStatus", &[], &a.auth()).await.ok();
    assert_eq!(st["validDid"], json!(true), "{st}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sign_and_submit_plc_operations() {
    let plc = MockPlc::start().await;
    let rot = new_key();
    let s = pds(&plc, &rot, "did:web:pds.test", None).await;
    let a = s.create_account("sp").await;

    // signing needs the emailed token
    let sign = |body: J| {
        let (s, a) = (&s, &a);
        async move { s.xrpc.post("com.atproto.identity.signPlcOperation", &body, &a.auth()).await }
    };
    sign(json!({})).await.err(400, "InvalidRequest");
    sign(json!({"token": "AAAAA-BBBBB"})).await.err(400, "InvalidToken");
    // app passwords can't
    let ap = s.xrpc.post("com.atproto.server.createAppPassword", &json!({"name": "x"}), &a.auth()).await.ok();
    let ap_sess = s.create_session(&a.handle, ap["password"].as_str().unwrap()).await.ok();
    let ap_auth = Auth::Bearer(ap_sess["accessJwt"].as_str().unwrap().into());
    s.xrpc.post_empty("com.atproto.identity.requestPlcOperationSignature", &ap_auth).await.client_err();

    s.xrpc.post_empty("com.atproto.identity.requestPlcOperationSignature", &a.auth()).await.ok();
    let token = s.mail_token(&a.email).await.expect("plc_operation token mailed");
    let extra = Keypair::generate().did_key();
    let j = sign(json!({"token": token, "rotationKeys": [extra, rot.did_key()]})).await.ok();
    let op = j["operation"].clone();
    // the last op with the requested field replaced, prev = its CID, signed by the server key
    let genesis = plc.last_op(&a.did).unwrap();
    assert_eq!(op["prev"], json!(vlpds::plc::op_cid(&genesis).unwrap().to_string()));
    assert_eq!(op["rotationKeys"], json!([extra, rot.did_key()]));
    for k in ["alsoKnownAs", "verificationMethods", "services", "type"] {
        assert_eq!(op[k], genesis[k], "{k}");
    }
    vlpds::plc::verify_sig(&[rot.did_key()], &op).unwrap();
    // the token is single-use
    sign(json!({"token": token})).await.err(400, "InvalidToken");
    assert_eq!(plc.ops(&a.did).len(), 1, "signing submits nothing");

    // submitPlcOperation's checks (reference)
    let submit = |op: J| {
        let (s, a) = (&s, &a);
        async move { s.xrpc.post("com.atproto.identity.submitPlcOperation", &json!({"operation": op}), &a.auth()).await }
    };
    let resign = |f: &dyn Fn(&mut J)| {
        let mut u = op.clone();
        u.as_object_mut().unwrap().remove("sig");
        f(&mut u);
        vlpds::plc::sign(u, &rot).unwrap()
    };
    let cases: Vec<(J, &str)> = vec![
        (json!({"type": "plc_tombstone", "prev": op["prev"], "sig": "x"}), "Invalid operation"),
        (resign(&|u| u["rotationKeys"] = json!([extra])), "Rotation keys do not include server's rotation key"),
        (resign(&|u| u["services"]["atproto_pds"]["type"] = json!("Other")), "Incorrect type on atproto_pds service"),
        (resign(&|u| u["services"]["atproto_pds"]["endpoint"] = json!("https://elsewhere.test")), "Incorrect endpoint on atproto_pds service"),
        (resign(&|u| u["verificationMethods"]["atproto"] = json!(Keypair::generate().did_key())), "Incorrect signing key"),
        (resign(&|u| u["alsoKnownAs"] = json!(["at://someone.else"])), "Incorrect handle in alsoKnownAs"),
    ];
    for (bad, msg) in cases {
        let r = submit(bad).await;
        r.err(400, "InvalidRequest");
        assert!(r.text().contains(msg), "{msg}: {}", r.text());
    }
    // a directory refusal (here: an op signed by a key that isn't a rotation key) is a 500, no event
    let stranger = Keypair::generate();
    let mut u = op.clone();
    u.as_object_mut().unwrap().remove("sig");
    let forged = vlpds::plc::sign(u, &stranger).unwrap();
    submit(forged).await.err(500, "InternalServerError");
    assert_eq!(plc.ops(&a.did).len(), 1);

    let mut sub = s.subscribe_from_now().await;
    submit(op.clone()).await.ok();
    assert_eq!(plc.last_op(&a.did).unwrap(), op);
    sub.until(FH_TIMEOUT, |fs| fs.iter().any(|f| f.kind() == "#identity" && f.did() == Some(a.did.as_str()))).await;
}

/// Account migration out (reference flow): the new PDS creates the account
/// with the DID and service auth from the old one, the repo moves, the old
/// PDS signs a PLC op with the new PDS's recommended credentials, the new
/// PDS submits it and activates, the old one deactivates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn migration_out_to_another_pds() {
    let plc = MockPlc::start().await;
    let (old_key, new_key_) = (new_key(), new_key());
    let old = pds(&plc, &old_key, "did:web:old-pds.test", None).await;
    let new = pds(&plc, &new_key_, "did:web:new-pds.test", None).await;
    let alice = old.create_account("out").await;
    let did = alice.did.clone();
    for i in 0..3 {
        old.post(&alice, &format!("post {i}")).await;
    }

    // 1. the account on the new PDS, deactivated, from service auth
    let token = old
        .xrpc
        .get("com.atproto.server.getServiceAuth", &[("aud", "did:web:new-pds.test"), ("lxm", "com.atproto.server.createAccount")], &alice.auth())
        .await
        .ok()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let handle = format!("{}.{HANDLE_DOMAIN}", unique_name("moved"));
    let email = format!("{}@example.com", unique_name("moved"));
    let created = new
        .xrpc
        .post("com.atproto.server.createAccount", &json!({"handle": handle, "email": email, "password": PASSWORD, "did": did}), &Auth::Bearer(token))
        .await
        .ok();
    assert_eq!(created["did"], json!(did));
    assert_eq!(plc.ops(&did).len(), 1, "bringing a DID registers nothing");
    let auth = Auth::Bearer(created["accessJwt"].as_str().unwrap().into());
    // 2. the repo
    let car = old.xrpc.get("com.atproto.sync.getRepo", &[("did", &did)], &Auth::None).await;
    new.xrpc.post_bytes("com.atproto.repo.importRepo", car.body.to_vec(), "application/vnd.ipld.car", &auth).await.ok();
    // not yet: the DID still points at the old PDS
    new.xrpc.post_empty("com.atproto.server.activateAccount", &auth).await.err(400, "InvalidRequest");
    // 3. the old PDS signs the new PDS's recommended credentials
    let rec = new.xrpc.get("com.atproto.identity.getRecommendedDidCredentials", &[], &auth).await.ok();
    assert_eq!(rec["rotationKeys"], json!([new_key_.did_key()]));
    old.xrpc.post_empty("com.atproto.identity.requestPlcOperationSignature", &alice.auth()).await.ok();
    let tok = old.mail_token(&alice.email).await.unwrap();
    let mut body = rec.clone();
    body["token"] = json!(tok);
    let op = old.xrpc.post("com.atproto.identity.signPlcOperation", &body, &alice.auth()).await.ok()["operation"].clone();
    // 4. the new PDS submits it and activates
    new.xrpc.post("com.atproto.identity.submitPlcOperation", &json!({"operation": op}), &auth).await.ok();
    let data = plc.data(&did).unwrap();
    assert_eq!(data["services"]["atproto_pds"]["endpoint"], json!(new.url));
    assert_eq!(data["rotationKeys"], json!([new_key_.did_key()]));
    assert_eq!(data["alsoKnownAs"], json!([format!("at://{handle}")]));
    let st = new.xrpc.get("com.atproto.server.checkAccountStatus", &[], &auth).await.ok();
    assert_eq!(st["validDid"], json!(true), "{st}");
    new.xrpc.post_empty("com.atproto.server.activateAccount", &auth).await.ok();
    // 5. the old PDS steps back; it no longer controls the DID
    old.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &alice.auth()).await.ok();
    let st = old.xrpc.get("com.atproto.server.checkAccountStatus", &[], &alice.auth()).await.ok();
    assert_eq!(st["validDid"], json!(false), "{st}");
    old.xrpc.post_empty("com.atproto.server.activateAccount", &alice.auth()).await.err(400, "InvalidRequest");
    old.xrpc.post_empty("com.atproto.identity.requestPlcOperationSignature", &alice.auth()).await.ok();
    let tok = old.mail_token(&alice.email).await.unwrap();
    old.xrpc.post("com.atproto.identity.signPlcOperation", &json!({"token": tok}), &alice.auth()).await.ok();
    // (an op it signs now is refused by the directory: not a rotation key any more)
    // the new PDS takes writes, signed with its key, under the new handle
    let session = TestAccount {
        did: did.clone(),
        handle: handle.clone(),
        password: PASSWORD.into(),
        email,
        access: created["accessJwt"].as_str().unwrap().into(),
        refresh: created["refreshJwt"].as_str().unwrap().into(),
    };
    new.post(&session, "hello from the new PDS").await;
    let repo = new.get_repo(&did).await;
    repo.commit().verify(&new.signing_key(&did).await).expect("signed with the new PDS's key");
    // and its handle updates go to the directory with its own rotation key
    let h2 = format!("{}.{HANDLE_DOMAIN}", unique_name("moved2"));
    new.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": h2}), &auth).await.ok();
    assert_eq!(plc.data(&did).unwrap()["alsoKnownAs"], json!([format!("at://{h2}")]));
}

/// With registration off (dev mode, no rotation key) DIDs stay local and the
/// PLC operation endpoints answer 501.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unregistered_mode_keeps_local_dids() {
    let plc = MockPlc::start().await;
    let url = plc.url.clone();
    let s = TestServer::spawn_with(move |c| c.plc_url = url).await;
    assert!(s.app.plc.is_none());
    let a = s.create_account("unreg").await;
    assert!(plc.dids().is_empty() && plc.posts() == 0);
    let rec = s.xrpc.get("com.atproto.identity.getRecommendedDidCredentials", &[], &a.auth()).await.ok();
    assert_eq!(rec["rotationKeys"], json!([]));
    for m in ["com.atproto.identity.requestPlcOperationSignature", "com.atproto.identity.signPlcOperation", "com.atproto.identity.submitPlcOperation"] {
        s.xrpc.post(m, &json!({"operation": {}}), &a.auth()).await.err(501, "MethodNotImplemented");
    }
    let h2 = format!("{}.{HANDLE_DOMAIN}", unique_name("unreg2"));
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": h2}), &a.auth()).await.ok();
    assert_eq!(plc.posts(), 0);
}

/// bulkCreate (synthetic accounts) never reaches the directory, even with
/// registration on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_create_skips_plc() {
    let plc = MockPlc::start().await;
    let s = pds(&plc, &new_key(), "did:web:pds.test", None).await;
    let j = s.xrpc.post("vlpds.admin.bulkCreate", &json!({"start": 0, "count": 20, "records": 1}), &Auth::Bearer(ADMIN_TOKEN.into())).await.ok();
    assert_eq!(j["created"], json!(20), "{j}");
    assert_eq!(plc.posts(), 0);
}

/// Server rotation-key rotation: a node restarted with a new key and the old
/// one retired keeps updating DIDs that list only the old key (signed by
/// it, listing the new one instead), and `vlpds.admin.rotatePlcKeys` moves
/// the rest; afterwards the old key is not needed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rotation_key_rotation() {
    let plc = MockPlc::start().await;
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let node = |cfg: PlcConfig| {
        let (store, url) = (store.clone(), plc.url.clone());
        TestServer::spawn_with(move |c| {
            c.memory_store = Some(store);
            c.plc_url = url;
            c.plc = cfg;
            c.shards = 4;
            c.cluster = Some(vlpds::cluster::ClusterConfig {
                node_id: "rot".into(),
                addr: c.public_url.clone(),
                shards: 4,
                ttl: std::time::Duration::from_millis(1500),
                renew_every: std::time::Duration::from_millis(100),
                skew: std::time::Duration::from_millis(300),
                ..Default::default()
            });
        })
    };
    let (k1, k2) = (new_key(), new_key());
    let a = node(PlcConfig { rotation_key: Some(RotationKey::Key(k1.clone())), ..Default::default() }).await;
    let mut accts = Vec::new();
    for _ in 0..3 {
        accts.push(a.create_account("rot").await);
    }
    a.app.log.checkpoint_all().await;
    vlpds::server::shutdown(&a.app).await;

    let b = node(PlcConfig {
        rotation_key: Some(RotationKey::Key(k2.clone())),
        old_rotation_keys: vec![RotationKey::Key(k1.clone())],
        ..Default::default()
    })
    .await;
    let rotate = |dry: bool| {
        let x = b.xrpc.clone();
        async move { x.post("vlpds.admin.rotatePlcKeys", &json!({"dryRun": dry}), &Auth::Admin).await.ok() }
    };
    // a handle update of one account moves it at once
    let h = format!("{}.{HANDLE_DOMAIN}", unique_name("rot"));
    b.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": h}), &accts[0].auth()).await.ok();
    let last = plc.last_op(&accts[0].did).unwrap();
    assert_eq!(last["rotationKeys"], json!([k2.did_key()]));
    vlpds::plc::verify_sig(&[k1.did_key()], &last).expect("signed by the retired key");
    let dry = rotate(true).await;
    assert_eq!((dry["current"].clone(), dry["rotated"].clone(), dry["failed"].clone()), (json!(1), json!(2), json!(0)), "{dry}");
    assert_eq!(plc.ops(&accts[1].did).len(), 1, "dry run changes nothing");
    let done = rotate(false).await;
    assert_eq!((done["rotated"].clone(), done["failed"].clone()), (json!(2), json!(0)), "{done}");
    let again = rotate(true).await;
    assert_eq!((again["current"].clone(), again["rotated"].clone()), (json!(3), json!(0)), "{again}");
    for t in &accts {
        assert_eq!(plc.data(&t.did).unwrap()["rotationKeys"], json!([k2.did_key()]));
    }
    b.app.log.checkpoint_all().await;
    vlpds::server::shutdown(&b.app).await;
    // the old key retired: the new key alone updates every DID
    let c = node(PlcConfig { rotation_key: Some(RotationKey::Key(k2.clone())), ..Default::default() }).await;
    let h = format!("{}.{HANDLE_DOMAIN}", unique_name("rot"));
    c.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": h}), &accts[2].auth()).await.ok();
    vlpds::plc::verify_sig(&[k2.did_key()], &plc.last_op(&accts[2].did).unwrap()).unwrap();
}
