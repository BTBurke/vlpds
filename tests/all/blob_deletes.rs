//! Port of atproto/packages/pds/tests/blob-deletes.test.ts. vlpds deletes
//! unreferenced blobs with a GC sweep (`blobs::sweep_blobs`) rather than in
//! the write transaction, so each test drops references, runs a sweep with
//! zero grace, and checks listBlobs/getBlob from the outside.
use crate::common::*;
use std::time::Duration;

async fn upload(s: &TestServer, a: &TestAccount, bytes: &[u8]) -> J {
    s.upload_blob(a, bytes, "image/jpeg").await
}

fn file(tag: u8) -> Vec<u8> {
    let mut v = random_bytes(4000);
    v[0] = tag;
    v
}

fn link(blob: &J) -> String {
    blob["ref"]["$link"].as_str().unwrap().to_string()
}

async fn post_with_image(s: &TestServer, a: &TestAccount, blob: &J) -> RecordRef {
    s.create_record(a, "app.bsky.feed.post", image_post("img", blob)).await
}

async fn update_profile(s: &TestServer, a: &TestAccount, avatar: Option<&J>, banner: Option<&J>) {
    let mut rec = json!({"$type": "app.bsky.actor.profile"});
    if let Some(b) = avatar {
        rec["avatar"] = b.clone();
    }
    if let Some(b) = banner {
        rec["banner"] = b.clone();
    }
    s.put_record(a, "app.bsky.actor.profile", "self", rec).await.ok();
}

async fn listed(s: &TestServer, did: &str) -> Vec<String> {
    let mut v = s.list_blobs(did).await;
    v.sort();
    v
}

async fn stored(s: &TestServer, did: &str, cid: &str) -> bool {
    let r = s.get_blob(did, cid).await;
    match r.status {
        200 => true,
        _ => {
            r.err(400, "BlobNotFound");
            false
        }
    }
}

async fn gc(s: &TestServer) {
    vlpds::xrpc::blobs::sweep_blobs(&s.app, Duration::ZERO).await.expect("blob gc sweep");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletes_blob_when_record_is_deleted() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let img = upload(&s, &a, &file(1)).await;
    let post = post_with_image(&s, &a, &img).await;
    assert_eq!(listed(&s, &a.did).await, vec![link(&img)]);
    s.delete_record(&a, "app.bsky.feed.post", post.rkey()).await.ok();
    assert!(listed(&s, &a.did).await.is_empty(), "listBlobs must drop the blob once unreferenced");
    gc(&s).await;
    assert!(!stored(&s, &a.did, &link(&img)).await, "unreferenced blob must be deleted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletes_blob_when_ref_is_updated() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let img = upload(&s, &a, &file(1)).await;
    let img2 = upload(&s, &a, &file(2)).await;
    update_profile(&s, &a, Some(&img), Some(&img)).await;
    update_profile(&s, &a, Some(&img2), Some(&img2)).await;
    assert_eq!(listed(&s, &a.did).await, vec![link(&img2)]);
    gc(&s).await;
    assert!(!stored(&s, &a.did, &link(&img)).await);
    assert!(stored(&s, &a.did, &link(&img2)).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keeps_blob_when_ref_is_not_updated() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let img = upload(&s, &a, &file(1)).await;
    let img2 = upload(&s, &a, &file(2)).await;
    update_profile(&s, &a, Some(&img), Some(&img)).await;
    update_profile(&s, &a, Some(&img), Some(&img2)).await;
    let mut want = vec![link(&img), link(&img2)];
    want.sort();
    assert_eq!(listed(&s, &a.did).await, want);
    gc(&s).await;
    assert!(stored(&s, &a.did, &link(&img)).await);
    assert!(stored(&s, &a.did, &link(&img2)).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keeps_blob_reused_by_another_record_in_same_commit() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let img = upload(&s, &a, &file(1)).await;
    let post = post_with_image(&s, &a, &img).await;
    let writes = json!([
        {"$type": "com.atproto.repo.applyWrites#delete", "collection": "app.bsky.feed.post", "rkey": post.rkey()},
        {"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.post", "value": image_post("post2", &img)},
    ]);
    s.apply_writes(&a, writes).await.ok();
    assert_eq!(listed(&s, &a.did).await, vec![link(&img)]);
    gc(&s).await;
    assert!(stored(&s, &a.did, &link(&img)).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletes_from_own_store_even_if_another_user_uses_it() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let b = s.create_account("bob").await;
    let bytes = file(9);
    let ia = upload(&s, &a, &bytes).await;
    let ib = upload(&s, &b, &bytes).await;
    assert_eq!(link(&ia), link(&ib));
    let pa = post_with_image(&s, &a, &ia).await;
    post_with_image(&s, &b, &ib).await;
    s.delete_record(&a, "app.bsky.feed.post", pa.rkey()).await.ok();
    gc(&s).await;
    assert!(!stored(&s, &a.did, &link(&ia)).await, "alice's copy is gone");
    assert!(stored(&s, &b.did, &link(&ib)).await, "bob's copy remains");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_grace_protects_recent_uploads() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let img = upload(&s, &a, &file(3)).await;
    // nothing references it yet, but it's within the grace period
    vlpds::xrpc::blobs::sweep_blobs(&s.app, Duration::from_secs(3600)).await.unwrap();
    // so it can still be referenced and served
    post_with_image(&s, &a, &img).await;
    assert!(stored(&s, &a.did, &link(&img)).await);
}

/// Blob refs found in a record (JSON form).
fn refs_in(v: &J, out: &mut Vec<String>) {
    match v {
        J::Object(m) => {
            if m.get("$type").and_then(|t| t.as_str()) == Some("blob") {
                if let Some(l) = m.get("ref").and_then(|r| r["$link"].as_str()) {
                    out.push(l.to_string());
                }
            }
            m.values().for_each(|c| refs_in(c, out));
        }
        J::Array(a) => a.iter().for_each(|c| refs_in(c, out)),
        _ => {}
    }
}

/// The worker loads a repo's blob refs only when a write needs them (an
/// update or delete drops the old ones, a create with blobs counts their
/// other refs; other creates run without them, and the paths they create
/// stay in memory until the load). With every idle repo's state
/// dropped after each pass, a one-repo cache, and creates, updates and
/// deletes racing in the same passes (a load while creates are in flight),
/// the b/ index (listBlobs) always matches the records' refs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blob_refs_load_lazily() {
    let s = TestServer::spawn_with(|c| {
        c.lazy_mst_unload_idle = true;
        c.cache_per_worker = 1;
        c.workers = 1;
    })
    .await;
    let a = s.create_account("lazyblobs").await;
    let other = s.create_account("evictor").await;
    let mut imgs = Vec::new();
    for i in 0..3 {
        imgs.push(upload(&s, &a, &file(i)).await);
    }
    for round in 0..24usize {
        let auth = a.auth();
        let cbody = json!({"repo": a.did, "collection": "app.bsky.feed.post", "rkey": format!("p{round}"), "record": image_post("img", &imgs[round % 3])});
        let dbody = json!({"repo": a.did, "collection": "app.bsky.feed.post", "rkey": format!("p{}", round.saturating_sub(1))});
        let mut profile = json!({"$type": "app.bsky.actor.profile"});
        if round % 4 != 3 {
            profile["avatar"] = imgs[(round + 1) % 3].clone();
        }
        let pbody = json!({"repo": a.did, "collection": "app.bsky.actor.profile", "rkey": "self", "record": profile});
        let create = s.xrpc.post("com.atproto.repo.createRecord", &cbody, &auth);
        let delete = s.xrpc.post("com.atproto.repo.deleteRecord", &dbody, &auth);
        let put = s.xrpc.post("com.atproto.repo.putRecord", &pbody, &auth);
        match round % 3 {
            0 => {
                tokio::join!(create, put);
            }
            1 => {
                tokio::join!(create, delete);
            }
            _ => {
                tokio::join!(create, delete, put);
            }
        }
        if round % 5 == 4 {
            // another repo takes the worker's one cache slot: the next
            // write to `a` opens it cold
            s.create_record(&other, "app.bsky.feed.post", post_record("evict")).await;
        }
        let mut want = Vec::new();
        for coll in ["app.bsky.feed.post", "app.bsky.actor.profile"] {
            let recs = s.list_records(&a.did, coll, &[("limit", "100")]).await.ok();
            for r in recs["records"].as_array().unwrap() {
                refs_in(&r["value"], &mut want);
            }
        }
        want.sort();
        want.dedup();
        assert_eq!(listed(&s, &a.did).await, want, "round {round}");
    }
}
