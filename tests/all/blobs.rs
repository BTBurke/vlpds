//! Blob upload (single PUT and streamed multipart), getBlob headers, size
//! limit, listBlobs / listMissingBlobs, and the unreferenced-blob GC.
use crate::common::*;
use serde_json::json;
use std::time::Duration;

fn bytes(n: usize, seed: u8) -> Vec<u8> {
    (0..n)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_get_list_and_gc() {
    let s = TestServer::spawn_with(|c| c.max_blob_size = 20 << 20).await;
    let a = s.create_account("blobs").await;

    let small = bytes(3000, 1);
    let big = bytes(12_000_000, 2); // > one 8 MiB part: multipart path
    let up = |b: Vec<u8>, mime: &'static str| {
        let (x, auth) = (s.xrpc.clone(), a.auth());
        async move {
            x.post_bytes("com.atproto.repo.uploadBlob", b, mime, &auth)
                .await
        }
    };
    let rs = up(small.clone(), "image/png").await.ok();
    let rb = up(big.clone(), "video/mp4").await.ok();
    let small_cid = rs["blob"]["ref"]["$link"].as_str().unwrap().to_string();
    let big_cid = rb["blob"]["ref"]["$link"].as_str().unwrap().to_string();
    assert_eq!(rb["blob"]["size"], 12_000_000);
    assert_eq!(rb["blob"]["mimeType"], "video/mp4");

    // too large: rejected from Content-Length before the body is read, so the
    // client may see the 413 or a reset while it is still sending.
    let r = s
        .xrpc
        .http
        .post(format!("{}/xrpc/com.atproto.repo.uploadBlob", s.xrpc.base))
        .header("authorization", format!("Bearer {}", a.access))
        .header("content-type", "video/mp4")
        .body(bytes(21 << 20, 3))
        .send()
        .await;
    if let Ok(r) = r {
        assert_eq!(r.status().as_u16(), 413);
    }

    // getBlob: bytes, type, length, hardening headers
    let g = s
        .xrpc
        .get(
            "com.atproto.sync.getBlob",
            &[("did", &a.did), ("cid", &big_cid)],
            &Auth::None,
        )
        .await;
    assert_eq!(g.status, 200);
    assert_eq!(g.body.as_ref() as &[u8], big.as_slice());
    assert_eq!(g.header("content-type").as_deref(), Some("video/mp4"));
    assert_eq!(g.header("content-length").as_deref(), Some("12000000"));
    assert_eq!(
        g.header("x-content-type-options").as_deref(),
        Some("nosniff")
    );

    // reference the small blob plus one whose bytes then go missing. (A record
    // can't reference a never-uploaded blob: the reference's processWriteBlobs
    // refuses it with "Could not find blob"; missing blobs normally come from
    // importRepo during migration. Deleting the stored object is the cheap
    // way to get one here.)
    let gone = up(bytes(10, 9), "image/png").await.ok();
    let missing_cid = gone["blob"]["ref"]["$link"].as_str().unwrap().to_string();
    let missing = missing_cid.as_str();
    let blob = |cid: &str, size: usize| json!({"$type": "blob", "ref": {"$link": cid}, "mimeType": "image/png", "size": size});
    s.create_record(&a, "app.bsky.feed.post", json!({"$type": "app.bsky.feed.post", "text": "x", "createdAt": "2026-01-01T00:00:00Z", "a": blob(&small_cid, 3000), "b": blob(missing, 10)}))
        .await;
    use object_store::ObjectStoreExt;
    s.app
        .store
        .raw
        .delete(&object_store::path::Path::from(format!("{}/blob/{}/{}", s.app.store.prefix, a.did, missing)))
        .await
        .unwrap();

    let lb = s
        .xrpc
        .get(
            "com.atproto.sync.listBlobs",
            &[("did", &a.did)],
            &Auth::None,
        )
        .await
        .ok();
    let mut want = vec![small_cid.clone(), missing.to_string()];
    want.sort();
    assert_eq!(lb["cids"], json!(want));

    let lm = s
        .xrpc
        .get("com.atproto.repo.listMissingBlobs", &[], &a.auth())
        .await
        .ok();
    assert_eq!(lm["blobs"].as_array().unwrap().len(), 1);
    assert_eq!(lm["blobs"][0]["cid"], missing);

    // GC with zero grace: the unreferenced big blob goes, the referenced one stays
    let (_, deleted) = vlpds::xrpc::blobs::sweep_blobs(&s.app, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(deleted, 1);
    s.xrpc
        .get(
            "com.atproto.sync.getBlob",
            &[("did", &a.did), ("cid", &big_cid)],
            &Auth::None,
        )
        .await
        .err(400, "BlobNotFound");
    let g = s
        .xrpc
        .get(
            "com.atproto.sync.getBlob",
            &[("did", &a.did), ("cid", &small_cid)],
            &Auth::None,
        )
        .await;
    assert_eq!(g.status, 200);
    assert_eq!(g.header("content-type").as_deref(), Some("image/png"));

    // with a real grace period nothing young is collected
    up(big.clone(), "video/mp4").await.ok();
    let (_, deleted) = vlpds::xrpc::blobs::sweep_blobs(&s.app, Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(deleted, 0);
}

/// Declared blob refs must match the stored blob (reference verifyBlob), so
/// lexicon accept/maxSize can't be bypassed by lying about size or type.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blob_refs_must_match_stored_blob() {
    let s = TestServer::spawn().await;
    let a = s.create_account("blobref").await;
    let png = s.xrpc.post_bytes("com.atproto.repo.uploadBlob", PNG_1X1.to_vec(), "image/png", &a.auth()).await.ok()["blob"].clone();
    let text = s.xrpc.post_bytes("com.atproto.repo.uploadBlob", b"just some text".to_vec(), "text/plain", &a.auth()).await.ok()["blob"].clone();
    let profile = |avatar: &serde_json::Value| json!({"repo": a.did, "collection": "app.bsky.actor.profile", "rkey": "self", "record": {"$type": "app.bsky.actor.profile", "avatar": avatar}});
    let put = |body: serde_json::Value| {
        let (x, auth) = (s.xrpc.clone(), a.auth());
        async move { x.post("com.atproto.repo.putRecord", &body, &auth).await }
    };

    let mut lie = png.clone();
    lie["size"] = json!(png["size"].as_i64().unwrap() + 1);
    put(profile(&lie)).await.err(400, "InvalidSize");
    let mut lie = png.clone();
    lie["mimeType"] = json!("image/jpeg");
    put(profile(&lie)).await.err(400, "InvalidMimeType");
    // a non-image declared as an image to pass the avatar's accept list
    let mut lie = text.clone();
    lie["mimeType"] = json!("image/png");
    put(profile(&lie)).await.err(400, "InvalidMimeType");
    // same check through applyWrites and createRecord
    let mut lie = png.clone();
    lie["size"] = json!(1);
    s.xrpc
        .post(
            "com.atproto.repo.applyWrites",
            &json!({"repo": a.did, "writes": [{"$type": "com.atproto.repo.applyWrites#create", "collection": "com.example.thing", "value": {"file": lie}}]}),
            &a.auth(),
        )
        .await
        .err(400, "InvalidSize");
    s.xrpc
        .post(
            "com.atproto.repo.createRecord",
            &json!({"repo": a.did, "collection": "com.example.thing", "record": {"file": lie}}),
            &a.auth(),
        )
        .await
        .err(400, "InvalidSize");

    put(profile(&png)).await.ok();
}
