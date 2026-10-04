//! Port of atproto/packages/pds/tests/file-uploads.test.ts: uploadBlob,
//! referencing blobs from records, getBlob (bytes + headers), listBlobs,
//! listMissingBlobs, duplicate uploads, gzip uploads, MIME handling, limits.
use crate::common::*;

async fn set_avatar(s: &TestServer, a: &TestAccount, blob: &J) -> Resp {
    s.put_record(
        a,
        "app.bsky.actor.profile",
        "self",
        json!({"$type": "app.bsky.actor.profile", "displayName": "Alice", "avatar": blob}),
    )
    .await
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 { (crc >> 1) ^ 0xedb8_8320 } else { crc >> 1 };
        }
    }
    !crc
}

/// gzip with stored (uncompressed) deflate blocks — valid gzip, no deps.
fn gzip_stored(data: &[u8]) -> Vec<u8> {
    let mut out = vec![0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff];
    let chunks: Vec<&[u8]> = if data.is_empty() { vec![&[][..]] } else { data.chunks(65535).collect() };
    for (i, c) in chunks.iter().enumerate() {
        out.push(if i + 1 == chunks.len() { 1 } else { 0 });
        let len = c.len() as u16;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&(!len).to_le_bytes());
        out.extend_from_slice(c);
    }
    out.extend_from_slice(&crc32(data).to_le_bytes());
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uploads_references_and_serves_a_blob() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let blob = s.upload_blob(&a, PNG_1X1, "image/png").await;
    assert_eq!(blob["$type"], json!("blob"));
    assert_eq!(blob["mimeType"], json!("image/png"));
    assert_eq!(blob["size"], json!(PNG_1X1.len()));
    let cid = blob["ref"]["$link"].as_str().unwrap().to_string();
    assert_eq!(cid, Cid::raw(PNG_1X1).to_string(), "blob ref is the raw-codec sha256 CID of the bytes");

    set_avatar(&s, &a, &blob).await.ok();
    let g = s.get_record(&a.did, "app.bsky.actor.profile", "self").await.ok();
    assert_eq!(g["value"]["avatar"], blob);

    let r = s.get_blob(&a.did, &cid).await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(&r.body[..], PNG_1X1);
    assert_eq!(r.header("content-type").as_deref(), Some("image/png"));
    assert_eq!(r.header("content-security-policy").as_deref(), Some("default-src 'none'; sandbox"));
    assert_eq!(r.header("x-content-type-options").as_deref(), Some("nosniff"));

    assert_eq!(s.list_blobs(&a.did).await, vec![cid]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_requires_auth() {
    let s = TestServer::spawn().await;
    let r = s.xrpc.post_bytes("com.atproto.repo.uploadBlob", PNG_1X1.to_vec(), "image/png", &Auth::None).await;
    r.err(401, "AuthenticationRequired");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn permits_duplicate_uploads() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let b = s.create_account("bob").await;
    let file = random_bytes(5000);
    let ua = s.upload_blob(&a, &file, "image/jpeg").await;
    let ub = s.upload_blob(&b, &file, "image/jpeg").await;
    assert_eq!(ua, ub);
    set_avatar(&s, &a, &ua).await.ok();
    set_avatar(&s, &b, &ub).await.ok();
    let again = s.upload_blob(&a, &file, "image/jpeg").await;
    assert_eq!(again, ua);
    let cid = ua["ref"]["$link"].as_str().unwrap();
    assert_eq!(&s.get_blob(&a.did, cid).await.body[..], &file[..]);
    assert_eq!(&s.get_blob(&b.did, cid).await.body[..], &file[..]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supports_gzip_upload() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let file = random_bytes(3000);
    let plain = s.upload_blob(&a, &file, "image/jpeg").await;
    let rb = s
        .xrpc
        .http
        .post(format!("{}/xrpc/com.atproto.repo.uploadBlob", s.url))
        .header("authorization", format!("Bearer {}", a.access))
        .header("content-type", "image/jpeg")
        .header("content-encoding", "gzip")
        .body(gzip_stored(&file));
    let r = s.xrpc.send(rb).await.ok();
    assert_eq!(r["blob"]["ref"], plain["ref"], "gzip-encoded upload must hash the decoded bytes");
    assert_eq!(r["blob"]["size"], json!(file.len()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mime_types() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    // unknown / non-image types are preserved
    for (bytes, mime) in [
        (random_bytes(2000), "test/fake"),
        (b"hello world!".to_vec(), "text/plain"),
        (br#"{"hello":"world"}"#.to_vec(), "application/json"),
        (PNG_1X1.to_vec(), "image/png"),
    ] {
        let blob = s.upload_blob(&a, &bytes, mime).await;
        assert_eq!(blob["mimeType"], json!(mime));
    }
    // a PNG uploaded as video/mp4 is sniffed and stored as image/png
    let mut png = PNG_1X1.to_vec();
    png.extend_from_slice(&random_bytes(16)); // unique cid
    let blob = s.upload_blob(&a, &png, "video/mp4").await;
    s.create_record(&a, "com.example.media", json!({"$type": "com.example.media", "file": blob})).await;
    let r = s.get_blob(&a.did, blob["ref"]["$link"].as_str().unwrap()).await;
    assert_eq!(r.header("content-type").as_deref(), Some("image/png"), "bad mimetype should be corrected by sniffing");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enforces_max_blob_size() {
    let s = TestServer::spawn_with(|c| c.max_blob_size = 10_000).await;
    let a = s.create_account("alice").await;
    let r = s.xrpc.post_bytes("com.atproto.repo.uploadBlob", random_bytes(20_000), "image/jpeg", &a.auth()).await;
    assert_eq!(r.status, 413, "{}", r.text());
    // at the limit is fine
    s.upload_blob(&a, &random_bytes(10_000), "image/jpeg").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_blob_outside_lexicon_constraints() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    // app.bsky.actor.profile#avatar: image/png|image/jpeg, maxSize 1,000,000
    let big = s.upload_blob(&a, &random_bytes(1_100_000), "image/jpeg").await;
    set_avatar(&s, &a, &big).await.err(400, "InvalidRequest");
    let txt = s.upload_blob(&a, b"not an image", "text/plain").await;
    set_avatar(&s, &a, &txt).await.err(400, "InvalidRequest");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn referencing_unknown_blob_fails() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let ghost = json!({"$type": "blob", "ref": {"$link": Cid::raw(b"never uploaded").to_string()}, "mimeType": "image/png", "size": 14});
    let body = json!({"repo": a.did, "collection": "com.example.media", "record": {"$type": "com.example.media", "file": ghost}});
    s.xrpc.post("com.atproto.repo.createRecord", &body, &a.auth()).await.err(400, "BlobNotFound");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_blob_errors() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    s.get_blob(&a.did, &Cid::raw(b"nope").to_string()).await.err(400, "BlobNotFound");
    s.get_blob(&a.did, "not-a-cid").await.err(400, "InvalidRequest");
    s.get_blob("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", &Cid::raw(b"nope").to_string()).await.err(400, "RepoNotFound");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_blobs_paginates() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let mut want = Vec::new();
    for i in 0..5 {
        let blob = s.upload_blob(&a, &random_bytes(100 + i), "image/jpeg").await;
        want.push(blob["ref"]["$link"].as_str().unwrap().to_string());
        s.create_record(&a, "com.example.media", json!({"$type": "com.example.media", "file": blob})).await;
    }
    // an unreferenced upload is not listed
    s.upload_blob(&a, &random_bytes(77), "image/jpeg").await;
    want.sort();
    let mut got = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..10 {
        let mut q = vec![("did", a.did.as_str()), ("limit", "2")];
        if let Some(c) = &cursor {
            q.push(("cursor", c.as_str()));
        }
        let j = s.xrpc.get("com.atproto.sync.listBlobs", &q, &Auth::None).await.ok();
        let cids: Vec<String> = j["cids"].as_array().unwrap().iter().map(|c| c.as_str().unwrap().to_string()).collect();
        assert!(cids.len() <= 2);
        let done = cids.is_empty();
        got.extend(cids);
        cursor = j["cursor"].as_str().map(String::from);
        if cursor.is_none() || done {
            break;
        }
    }
    got.sort();
    assert_eq!(got, want);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_missing_blobs() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    s.xrpc.get("com.atproto.repo.listMissingBlobs", &[], &Auth::None).await.err(401, "AuthenticationRequired");
    let blob = s.upload_blob(&a, PNG_1X1, "image/png").await;
    set_avatar(&s, &a, &blob).await.ok();
    let j = s.xrpc.get("com.atproto.repo.listMissingBlobs", &[], &a.auth()).await.ok();
    assert_eq!(j["blobs"], json!([]), "all referenced blobs are present");
}
