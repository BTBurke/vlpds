//! A write that was applied is never answered as refused (4xx) or as
//! "nothing done" (503 ShardMoved / RepoLoading, which the entry node
//! resends).

use super::durability::{create, scope};
use super::hooks::*;
use super::phase3::takedown_record;
use crate::common::spaces::SpaceClient;
use crate::common::*;
use std::sync::Arc;
use std::time::Duration;

fn tag() -> String {
    random_bytes(5).iter().map(|b| format!("{b:02x}")).collect()
}

/// The authority's write whose records are partly taken down pushes its
/// served hash after the ack. Its shard leaving in between fails that push:
/// the answer is a 500 (outcome unknown), not ShardMoved, which the entry
/// node would resend (a createRecord then refused RecordAlreadyExists).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn space_write_failing_after_it_applied_is_unknown() {
    let bucket = Arc::default();
    let store = HookedStore::new(&bucket);
    let s = cluster_node("aa", store.clone(), 4, |c| {
        c.spaces = true;
        c.hedge_after = Duration::from_secs(3600);
    })
    .await;
    let t = tag();
    let (st, coll) = (format!("com.example.aa{t}.space"), format!("com.example.aa{t}.note"));
    let auth = SpaceClient::new(&s, "aa", &scope(&st, &coll)).await;
    let space = auth.create_space(&st, "aa").await;
    let r = create(&auth, &space, &coll, "hidden", "taken down").await;
    assert_eq!(r.status, 200, "{r:?}");
    takedown_record(&s, r.json["uri"].as_str().unwrap(), r.json["cid"].as_str().unwrap(), true).await;

    let needle = format!("applied, then its shard left {t}");
    let held = store.arm(Stage::AfterPut, Act::Pause, &needle);
    let shard = s.app.partitions.shard_of(&auth.did);
    let app = &s.app;
    let mover = async move {
        let mut held = held;
        held.wait("the write's segment").await;
        let p = app.partitions.get(shard).expect("owned");
        app.partitions.set(shard, None);
        drop(held);
        tokio::time::sleep(Duration::from_millis(500)).await;
        app.partitions.set(shard, Some(p));
    };
    let (r, ()) = tokio::join!(create(&auth, &space, &coll, "after-ack", &needle), mover);
    assert!(r.status >= 500 && r.json["error"] != "ShardMoved", "{r:?}");
    let q = [("space", space.as_str()), ("repo", auth.did.as_str()), ("collection", &coll), ("rkey", "after-ack")];
    let got = auth.get("com.atproto.space.getRecord", &q).await;
    assert_eq!(got.status, 200, "the write was applied: {got:?}");
}
