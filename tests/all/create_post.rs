//! Port of atproto/packages/pds/tests/create-post.test.ts: posts with tags and
//! richtext facets are stored and returned verbatim.
use crate::common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creates_posts_with_tags() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let rec = json!({"$type": "app.bsky.feed.post", "text": "hello world", "tags": ["javascript", "hehe"], "createdAt": now_iso()});
    let r = s.create_record(&a, "app.bsky.feed.post", rec.clone()).await;
    let g = s.get_record(&a.did, "app.bsky.feed.post", r.rkey()).await.ok();
    assert_eq!(g["value"]["tags"], json!(["javascript", "hehe"]));
    assert_eq!(g["value"], rec);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creates_posts_with_tag_facets() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let rec = json!({
        "$type": "app.bsky.feed.post",
        "text": "hello #world",
        "facets": [{
            "index": {"byteStart": 6, "byteEnd": 12},
            "features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "world"}]
        }],
        "createdAt": now_iso(),
    });
    let r = s.create_record(&a, "app.bsky.feed.post", rec.clone()).await;
    let g = s.get_record(&a.did, "app.bsky.feed.post", r.rkey()).await.ok();
    let facets = g["value"]["facets"].as_array().unwrap();
    assert!(facets.iter().all(|f| f["features"][0]["$type"] == json!("app.bsky.richtext.facet#tag")));
    assert_eq!(g["value"], rec);
}
