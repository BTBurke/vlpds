//! Online shard split/merge (DESIGN.md "Online shard split/merge"):
//! in-process nodes sharing one in-memory object store split and merge
//! shards under write load, with the owner or driver "crashing" at each
//! phase (its object-store calls hang and its control plane stops, as if
//! the process died), concurrent with a rebalance, while a firehose
//! subscription and a listRepos enumeration span the layout change. Every
//! acked write must be readable afterwards and appear exactly once, in
//! order, on the firehose.

use crate::common::*;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SHARDS: u16 = 6;

/// A node's view of the shared store that can "die": every call made after
/// `kill` hangs forever (nothing it had in flight lands later either: those
/// calls already completed or hang too).
#[derive(Debug)]
pub struct Killable {
    inner: Arc<object_store::memory::InMemory>,
    dead: AtomicBool,
}

impl std::fmt::Display for Killable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Killable")
    }
}

impl Killable {
    fn kill(&self) {
        self.dead.store(true, Ordering::SeqCst);
    }

    async fn gate(&self) {
        if self.dead.load(Ordering::SeqCst) {
            futures::future::pending::<()>().await;
        }
    }
}

use object_store::path::Path;
use object_store::{GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions, PutOptions, PutPayload, PutResult};

#[async_trait::async_trait]
impl object_store::ObjectStore for Killable {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        self.gate().await;
        let r = self.inner.put_opts(location, payload, opts).await;
        self.gate().await;
        r
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.gate().await;
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.gate().await;
        let r = self.inner.get_opts(location, options).await;
        self.gate().await;
        r
    }
    fn delete_stream(&self, locations: futures::stream::BoxStream<'static, object_store::Result<Path>>) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        use futures::StreamExt;
        if self.dead.load(Ordering::SeqCst) {
            return futures::stream::pending().boxed();
        }
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        use futures::StreamExt;
        if self.dead.load(Ordering::SeqCst) {
            return futures::stream::pending().boxed();
        }
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.gate().await;
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: object_store::CopyOptions) -> object_store::Result<()> {
        self.gate().await;
        self.inner.copy_opts(from, to, options).await
    }
}

pub struct Node {
    pub s: TestServer,
    pub store: Arc<Killable>,
}

impl Node {
    fn id(&self) -> String {
        cluster(&self.s).cfg.node_id.clone()
    }

    fn alive(&self) -> bool {
        !self.store.dead.load(Ordering::SeqCst)
    }

}

async fn node(id: &str, store: &Arc<object_store::memory::InMemory>) -> Node {
    let k = Arc::new(Killable { inner: store.clone(), dead: AtomicBool::new(false) });
    let (id, raw) = (id.to_string(), k.clone() as Arc<dyn object_store::ObjectStore>);
    let s = TestServer::spawn_with(move |c| {
        c.memory_store = Some(raw);
        c.shards = SHARDS;
        c.cluster = Some(vlpds::cluster::ClusterConfig {
            node_id: id,
            addr: c.public_url.clone(),
            shards: SHARDS,
            ttl: Duration::from_millis(1500),
            renew_every: Duration::from_millis(100),
            skew: Duration::from_millis(200),
            ..Default::default()
        });
    })
    .await;
    Node { s, store: k }
}

fn cluster(n: &TestServer) -> &vlpds::cluster::Cluster {
    n.app.cluster.as_deref().unwrap()
}

fn layout(n: &Node) -> Arc<vlpds::slots::Layout> {
    cluster(&n.s).layout()
}

fn owned(n: &Node) -> Vec<u16> {
    let mut v: Vec<u16> = n.s.app.partitions.owned().iter().map(|p| p.id).collect();
    v.sort();
    v
}

/// Until every live node routes by the same layout with no op in flight,
/// holding `want_version` or later, and its shards are each open on exactly
/// one live node. Returns that layout.
async fn settled(nodes: &[&Node], want_version: u64, timeout: Duration) -> Arc<vlpds::slots::Layout> {
    let deadline = Instant::now() + timeout;
    loop {
        let live: Vec<&&Node> = nodes.iter().filter(|n| n.alive()).collect();
        let l = layout(live[0]);
        let same = live.iter().all(|n| *layout(n) == *l);
        let mut seen = HashMap::new();
        let mut dup = false;
        for n in &live {
            for s in owned(n) {
                dup |= seen.insert(s, n.id()).is_some();
            }
        }
        let complete = !dup && l.ids().iter().all(|s| seen.contains_key(s)) && seen.len() == l.shards.len();
        if same && l.op.is_none() && l.version >= want_version && complete {
            return l;
        }
        assert!(
            Instant::now() < deadline,
            "never settled at v{want_version}+: layouts {:?}, owned {:?}",
            live.iter().map(|n| (n.id(), layout(n).version, layout(n).op.clone(), layout(n).ids())).collect::<Vec<_>>(),
            live.iter().map(|n| (n.id(), owned(n))).collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Until the shards are spread over `n` nodes at fair share.
async fn balanced(nodes: &[&Node]) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let l = settled(nodes, 1, Duration::from_secs(20)).await;
        let sizes: Vec<usize> = nodes.iter().filter(|n| n.alive()).map(|n| owned(n).len()).collect();
        let fair = l.shards.len().div_ceil(sizes.len());
        if sizes.iter().all(|s| *s >= 1 && *s <= fair) {
            return;
        }
        assert!(Instant::now() < deadline, "never balanced: {sizes:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// One acked write: repo, record uri, commit rev.
#[derive(Clone, Debug)]
struct Acked {
    did: String,
    uri: String,
    rev: String,
}

/// Writers (one per account, sequential creates) through the given nodes,
/// round-robin per attempt; failures (503 while a shard moves, a dead node)
/// are retried on the next node.
struct Load {
    stop: Arc<AtomicBool>,
    acked: Arc<parking_lot::Mutex<Vec<Acked>>>,
    failed: Arc<AtomicUsize>,
    /// Failed attempts by HTTP status (0 = no response / timed out).
    statuses: Arc<parking_lot::Mutex<HashMap<u16, usize>>>,
    /// Longest time an account went without an acked write (unavailability).
    gaps: Arc<parking_lot::Mutex<Vec<Duration>>>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Load {
    fn start(urls: Vec<String>, accounts: &[TestAccount]) -> Load {
        let stop = Arc::new(AtomicBool::new(false));
        let acked = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let failed = Arc::new(AtomicUsize::new(0));
        let statuses = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let gaps = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let urls = Arc::new(parking_lot::Mutex::new(urls));
        let mut handles = Vec::new();
        for (i, acct) in accounts.iter().cloned().enumerate() {
            let (stop, acked, failed, gaps, urls, statuses) = (stop.clone(), acked.clone(), failed.clone(), gaps.clone(), urls.clone(), statuses.clone());
            handles.push(tokio::spawn(async move {
                // one client per node, reused (a client per attempt ran the
                // box out of ephemeral ports)
                let clients: Vec<(String, Xrpc)> = urls.lock().iter().map(|u| (u.clone(), Xrpc::new(u))).collect();
                let mut k = i;
                let mut last_ok = Instant::now();
                let mut worst = Duration::ZERO;
                while !stop.load(Ordering::Acquire) {
                    k += 1;
                    let (url, x) = &clients[k % clients.len()];
                    let body = json!({"repo": acct.did, "collection": "app.bsky.feed.post", "record": post_record(&format!("w{k}"))});
                    let r = tokio::time::timeout(Duration::from_secs(5), x.try_send(x.http.post(format!("{url}/xrpc/com.atproto.repo.createRecord")).json(&body).bearer_auth(&acct.access))).await;
                    match r {
                        Ok(Ok(r)) if r.is_ok() => {
                            let j = r.ok();
                            acked.lock().push(Acked {
                                did: acct.did.clone(),
                                uri: j["uri"].as_str().unwrap().to_string(),
                                rev: j["commit"]["rev"].as_str().unwrap().to_string(),
                            });
                            worst = worst.max(last_ok.elapsed());
                            last_ok = Instant::now();
                        }
                        other => {
                            let code = match &other {
                                Ok(Ok(r)) => r.status,
                                _ => 0,
                            };
                            *statuses.lock().entry(code).or_default() += 1;
                            failed.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                gaps.lock().push(worst);
            }));
        }
        Load { stop, acked, failed, statuses, gaps, handles }
    }

    fn acked(&self) -> usize {
        self.acked.lock().len()
    }

    /// Waits for `n` more acked writes.
    async fn progress(&self, n: usize) {
        let want = self.acked() + n;
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.acked() < want {
            assert!(Instant::now() < deadline, "writes stalled at {} acked ({} failed)", self.acked(), self.failed.load(Ordering::Relaxed));
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn stop(self) -> (Vec<Acked>, usize, Duration) {
        self.stop.store(true, Ordering::Release);
        for h in self.handles {
            h.await.unwrap();
        }
        let statuses = self.statuses.lock().clone();
        eprintln!("failed attempts by status: {statuses:?}");
        assert!(!statuses.contains_key(&500), "a write failed with 500 (not retryable): {statuses:?}");
        let gaps = self.gaps.lock().iter().copied().max().unwrap_or_default();
        let acked = self.acked.lock().clone();
        (acked, self.failed.load(Ordering::Relaxed), gaps)
    }
}

/// Every acked record reads back through `n` (forwarded to its owner). A
/// 503 (a node whose routing hasn't caught up with a move yet) is retried,
/// as clients do.
async fn verify_readable(n: &TestServer, acked: &[Acked]) {
    use futures::StreamExt;
    futures::stream::iter(acked)
        .for_each_concurrent(16, |a| async move {
            let rkey = a.uri.rsplit('/').next().unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let r = n.get_record(&a.did, "app.bsky.feed.post", rkey).await;
                if r.status == 503 && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                if !r.is_ok() {
                    diagnose(n, a).await;
                }
                assert!(r.is_ok(), "acked record {} lost: {r:?}", a.uri);
                break;
            }
        })
        .await;
}

/// On a failed read-back: where the record is. This node's routing and open
/// shards (record, head, L0 view ids that repeat), then every shard DB in the
/// bucket read directly. A record in the shard that routes it but not in its
/// open DB was lost by that shard's state, not misrouted.
async fn diagnose(n: &TestServer, a: &Acked) {
    let path = a.uri.splitn(4, '/').nth(3).unwrap().to_string();
    let rk = vlpds::state::record_key(&a.did, &path);
    let l = n.app.partitions.layout();
    eprintln!("DIAG {} slot {} rev {} layout v{} routes to shard {} of {:?}", a.uri, vlpds::slots::slot_of(&a.did), a.rev, l.version, l.shard_of(&a.did), l.ids());
    for p in n.app.partitions.owned() {
        let rec = p.db.get(&rk).await.map(|v| v.is_some());
        let head = p.db.get(vlpds::state::head_key(&a.did)).await.map(|v| v.is_some());
        let m = p.db.manifest();
        let ids: Vec<_> = m.l0().iter().map(|v| v.id).collect();
        let repeats = ids.len() - ids.iter().collect::<HashSet<_>>().len();
        eprintln!("DIAG open shard {}: record {rec:?} head {head:?}; L0 {} ({repeats} repeated view ids), {} sorted runs", p.id, ids.len(), m.compacted().len());
    }
    for id in 0..256u16 {
        let path = vlpds::partition::db_path(&n.app.store, id);
        let Ok(r) = slatedb::DbReader::builder(path.clone(), n.app.store.raw.clone()).build().await else { continue };
        if let Ok(Some(_)) = r.get(&rk).await {
            eprintln!("DIAG {path} holds the record");
        }
        let _ = r.close().await;
    }
}

/// The firehose from `sub` holds every acked commit exactly once, in seq
/// order, each repo's commits chained (`since` = the previous rev).
async fn verify_firehose(sub: &mut Sub, acked: &[Acked]) {
    let want: HashSet<(String, String)> = acked.iter().map(|a| (a.did.clone(), a.rev.clone())).collect();
    let dids: HashSet<&str> = acked.iter().map(|a| a.did.as_str()).collect();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut last_seq = 0i64;
    let mut prev_rev: HashMap<String, String> = HashMap::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while !want.is_subset(&seen) {
        let left = deadline.saturating_duration_since(Instant::now());
        let Some(f) = sub.next(left).await else {
            let missing: Vec<_> = want.difference(&seen).take(5).collect();
            panic!("firehose ended or timed out: {} of {} acked commits seen; missing e.g. {missing:?}", want.intersection(&seen).count(), want.len());
        };
        let Some(c) = f.commit() else { continue };
        assert!(c.seq > last_seq, "seqs out of order: {} after {last_seq}", c.seq);
        last_seq = c.seq;
        if !dids.contains(c.repo.as_str()) {
            continue;
        }
        if let (Some(prev), Some(since)) = (prev_rev.get(&c.repo), &c.since) {
            assert_eq!(prev, since, "chain break for {} at seq {}", c.repo, c.seq);
        }
        prev_rev.insert(c.repo.clone(), c.rev.clone());
        assert!(seen.insert((c.repo.clone(), c.rev.clone())), "duplicate commit {} {}", c.repo, c.rev);
    }
}

async fn accounts(n: &TestServer, k: usize) -> Vec<TestAccount> {
    futures::future::join_all((0..k).map(|_| n.create_account("rs"))).await
}

async fn admin(n: &TestServer, nsid: &str, body: J) -> J {
    n.xrpc.post(nsid, &body, &Auth::Admin).await.ok()
}

/// Splits and merges under write load across three nodes: a split of one
/// node's shard, a merge of its two children back, and a merge of two
/// shards held by different nodes. Writes keep flowing (only the moving
/// slots see a short 503 window), every acked write reads back from every
/// node and shows up exactly once, in order, on firehoses subscribed before
/// the change (live) and replayed after it (backfill), and every node routes
/// by the same layout.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn split_and_merge_under_write_load() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node("rsl-a", &store).await;
    let accts = accounts(&a.s, 9).await;
    let b = node("rsl-b", &store).await;
    let c = node("rsl-c", &store).await;
    balanced(&[&a, &b, &c]).await;
    let cursor = a.s.settled_now().await;
    let mut live = a.s.subscribe(Some(cursor)).await;
    let load = Load::start(vec![a.s.url.clone(), b.s.url.clone(), c.s.url.clone()], &accts);
    load.progress(30).await;

    // split a shard b owns, asked of a (the owner drives it)
    let l1 = layout(&a);
    let target = *owned(&b).first().unwrap();
    let t = Instant::now();
    let r = admin(&a.s, "vlpds.admin.splitShard", json!({"shard": target, "wait": true})).await;
    eprintln!("split of {target}: {:?} ({r})", t.elapsed());
    assert_eq!(r["done"], json!(true), "{r}");
    let l2 = settled(&[&a, &b, &c], l1.version + 1, Duration::from_secs(20)).await;
    let kids: Vec<u16> = r["op"]["children"].as_array().unwrap().iter().map(|c| c["id"].as_u64().unwrap() as u16).collect();
    assert!(!l2.contains(target) && kids.iter().all(|k| l2.contains(*k)), "{l2:?}");
    load.progress(30).await;

    // merge the two children back (one node holds both after the split)
    let r = admin(&b.s, "vlpds.admin.mergeShards", json!({"left": kids[0], "right": kids[1], "wait": true})).await;
    assert_eq!(r["done"], json!(true), "{r}");
    let l3 = settled(&[&a, &b, &c], l2.version + 1, Duration::from_secs(20)).await;
    load.progress(30).await;

    // merge two adjacent shards held by different nodes
    let (x, y) = {
        let owner = |s: u16| [&a, &b, &c].iter().position(|n| owned(n).contains(&s));
        let pair = l3.shards.windows(2).find(|w| owner(w[0].id) != owner(w[1].id)).expect("adjacent shards on two nodes");
        (pair[0].id, pair[1].id)
    };
    let r = admin(&c.s, "vlpds.admin.mergeShards", json!({"left": x, "right": y, "wait": true})).await;
    assert_eq!(r["done"], json!(true), "{r}");
    let l4 = settled(&[&a, &b, &c], l3.version + 1, Duration::from_secs(20)).await;
    assert_eq!(l4.shards.len(), SHARDS as usize - 1);
    load.progress(30).await;
    let (acked, failed, gap) = load.stop().await;
    eprintln!("{} acked, {failed} failed attempts, longest per-account gap {gap:?}", acked.len());
    assert!(gap < Duration::from_secs(5), "writes unavailable for {gap:?}");
    for n in [&a, &b, &c] {
        verify_readable(&n.s, &acked).await;
    }
    verify_firehose(&mut live, &acked).await;
    let mut replay = c.s.subscribe(Some(cursor)).await;
    verify_firehose(&mut replay, &acked).await;
}

/// A single node splits and merges its own shards (no peers).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_split_and_merge() {
    let s = TestServer::spawn().await;
    let accts = accounts(&s, 6).await;
    let mut refs = Vec::new();
    for a in &accts {
        refs.push((a.did.clone(), s.post(a, "before").await));
    }
    let l = cluster(&s).layout();
    let r = admin(&s, "vlpds.admin.splitShard", json!({"shard": l.shards[0].id, "at": 1000, "wait": true})).await;
    assert_eq!(r["done"], json!(true), "{r}");
    let kids: Vec<u16> = r["op"]["children"].as_array().unwrap().iter().map(|c| c["id"].as_u64().unwrap() as u16).collect();
    assert_eq!(r["layout"]["shards"][0]["hi"], json!(1000));
    let r = admin(&s, "vlpds.admin.mergeShards", json!({"left": kids[1], "right": l.shards[1].id, "wait": true})).await;
    assert_eq!(r["done"], json!(true), "{r}");
    for a in &accts {
        refs.push((a.did.clone(), s.post(a, "after").await));
    }
    for (did, r) in &refs {
        assert!(s.get_record(did, "app.bsky.feed.post", r.rkey()).await.is_ok());
    }
    let l = cluster(&s).layout();
    assert_eq!((l.version, l.shards.len()), (3, 8));
    let ids: Vec<u16> = s.app.partitions.owned().iter().map(|p| p.id).collect();
    assert_eq!(ids.len(), 8, "every shard of the new layout open: {ids:?}");
    // bad requests are refused without changing anything
    let r = s.xrpc.post("vlpds.admin.splitShard", &json!({"shard": 999}), &Auth::Admin).await;
    r.err(400, "InvalidRequest");
    let r = s.xrpc.post("vlpds.admin.mergeShards", &json!({"left": kids[0], "right": 7}), &Auth::Admin).await;
    r.err(400, "InvalidRequest");
    s.xrpc.post("vlpds.admin.splitShard", &json!({"shard": kids[0]}), &Auth::None).await.err_status(401);
}

/// The owner (and driver) of a split's parent crashes at `phase`; the
/// survivors take over (fence + replay its log, adopt the driver role) and
/// finish the split. Every acked write survives.
async fn crash_mid_split(phase: &'static str) {
    let store = Arc::new(object_store::memory::InMemory::new());
    let tag = format!("rsc-{phase}");
    let a = node(&format!("{tag}-a"), &store).await;
    let accts = accounts(&a.s, 9).await;
    let b = node(&format!("{tag}-b"), &store).await;
    let c = node(&format!("{tag}-c"), &store).await;
    balanced(&[&a, &b, &c]).await;
    let cursor = a.s.settled_now().await;
    let mut live = a.s.subscribe(Some(cursor)).await;
    let load = Load::start(vec![a.s.url.clone(), c.s.url.clone()], &accts);
    load.progress(30).await;

    // b owns the parent, so b drives; b dies at `phase`
    let l1 = layout(&a);
    let target = *owned(&b).first().unwrap();
    let fired = Arc::new(AtomicBool::new(false));
    {
        let (fired, store, app) = (fired.clone(), b.store.clone(), b.s.app.clone());
        vlpds::reshard::set_crash_hook(
            &b.id(),
            Some(Arc::new(move |p: &str| {
                if p != phase || fired.swap(true, Ordering::SeqCst) {
                    return false;
                }
                store.kill();
                app.node.halt();
                true
            })),
        );
    }
    // "planned" fires in the planner: ask b itself (it returns at once)
    let asked = if phase == "planned" { &b } else { &a };
    let r = admin(&asked.s, "vlpds.admin.splitShard", json!({"shard": target, "wait": false})).await;
    let kids: Vec<u16> = r["op"]["children"].as_array().unwrap().iter().map(|c| c["id"].as_u64().unwrap() as u16).collect();
    let t = Instant::now();
    while !fired.load(Ordering::SeqCst) {
        assert!(t.elapsed() < Duration::from_secs(20), "phase {phase} never reached");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let t = Instant::now();
    let l2 = settled(&[&a, &b, &c], l1.version + 1, Duration::from_secs(30)).await;
    eprintln!("{phase}: split finished {:?} after the driver died", t.elapsed());
    assert!(!l2.contains(target) && kids.iter().all(|k| l2.contains(*k)), "{l2:?}");
    load.progress(30).await;
    let (acked, failed, gap) = load.stop().await;
    eprintln!("{phase}: {} acked, {failed} failed attempts, longest gap {gap:?}", acked.len());
    vlpds::reshard::set_crash_hook(&b.id(), None);
    verify_readable(&a.s, &acked).await;
    verify_readable(&c.s, &acked).await;
    verify_firehose(&mut live, &acked).await;
    let fenced = cluster(&a.s).fenced_logs().len() + cluster(&c.s).fenced_logs().len();
    assert!(fenced > 0, "the dead node's log was fenced");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn crash_after_plan() {
    crash_mid_split("planned").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn crash_after_parent_closed() {
    crash_mid_split("closed").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn crash_after_freeze() {
    crash_mid_split("frozen").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn crash_after_clone() {
    crash_mid_split("cloned").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn crash_after_children_assigned() {
    crash_mid_split("children").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn crash_after_flip() {
    crash_mid_split("flipped").await;
}

/// An op stuck before its flip (its driver keeps failing after the clone)
/// is aborted: the parent unfreezes, is served again under the old layout,
/// and a later split of it succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn abort_before_flip() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node("rsab-a", &store).await;
    let accts = accounts(&a.s, 6).await;
    let b = node("rsab-b", &store).await;
    balanced(&[&a, &b]).await;
    let load = Load::start(vec![a.s.url.clone(), b.s.url.clone()], &accts);
    load.progress(20).await;
    let l1 = layout(&a);
    let target = *owned(&b).first().unwrap();
    let stuck = Arc::new(AtomicUsize::new(0));
    {
        let stuck = stuck.clone();
        vlpds::reshard::set_crash_hook(&b.id(), Some(Arc::new(move |p: &str| p == "cloned" && stuck.fetch_add(1, Ordering::SeqCst) < 1_000_000)));
    }
    admin(&a.s, "vlpds.admin.splitShard", json!({"shard": target})).await;
    let t = Instant::now();
    while stuck.load(Ordering::SeqCst) < 3 {
        assert!(t.elapsed() < Duration::from_secs(20), "driver never got stuck after cloning");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let r = admin(&a.s, "vlpds.admin.abortReshard", json!({})).await;
    assert_eq!(r["aborted"]["parents"], json!([target]), "{r}");
    vlpds::reshard::set_crash_hook(&b.id(), None);
    let l = settled(&[&a, &b], 1, Duration::from_secs(20)).await;
    assert_eq!((l.version, l.ids()), (l1.version, l1.ids()), "nothing flipped");
    let owner = [&a, &b].into_iter().find(|n| owned(n).contains(&target)).expect("the parent is served again");
    assert!(cluster(&owner.s).assignment(target).unwrap().frozen.is_none());
    load.progress(20).await;
    // and it can be split for real now
    let r = admin(&a.s, "vlpds.admin.splitShard", json!({"shard": target, "wait": true})).await;
    assert_eq!(r["done"], json!(true), "{r}");
    settled(&[&a, &b], l1.version + 1, Duration::from_secs(20)).await;
    load.progress(20).await;
    let (acked, ..) = load.stop().await;
    verify_readable(&a.s, &acked).await;
}

/// A node joins (and peers hand shards back to it) while a merge across two
/// nodes is in flight: everything converges and no acked write is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn reshard_concurrent_with_rebalance() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node("rsrb-a", &store).await;
    let accts = accounts(&a.s, 9).await;
    let b = node("rsrb-b", &store).await;
    balanced(&[&a, &b]).await;
    let cursor = a.s.settled_now().await;
    let load = Load::start(vec![a.s.url.clone(), b.s.url.clone()], &accts);
    load.progress(20).await;
    let l1 = layout(&a);
    let (x, y) = {
        let owner = |s: u16| owned(&a).contains(&s);
        let pair = l1.shards.windows(2).find(|w| owner(w[0].id) != owner(w[1].id)).expect("adjacent shards on two nodes");
        (pair[0].id, pair[1].id)
    };
    let split_of = *owned(&b).last().unwrap();
    let (joined, r) = tokio::join!(node("rsrb-c", &store), admin(&a.s, "vlpds.admin.mergeShards", json!({"left": x, "right": y, "wait": true})));
    assert_eq!(r["done"], json!(true), "{r}");
    let c = joined;
    // and a split while the joiner is still taking its share
    if layout(&a).contains(split_of) {
        let r = admin(&c.s, "vlpds.admin.splitShard", json!({"shard": split_of, "wait": true})).await;
        assert_eq!(r["done"], json!(true), "{r}");
    }
    balanced(&[&a, &b, &c]).await;
    let l = settled(&[&a, &b, &c], l1.version + 1, Duration::from_secs(20)).await;
    assert!(!owned(&c).is_empty(), "the joiner got a share: {:?}", l.ids());
    load.progress(30).await;
    let (acked, ..) = load.stop().await;
    for n in [&a, &b, &c] {
        verify_readable(&n.s, &acked).await;
    }
    let mut sub = c.s.subscribe(Some(cursor)).await;
    verify_firehose(&mut sub, &acked).await;
}

/// listRepos pages in (slot, DID) order with a layout-independent cursor:
/// an enumeration spanning a split and a merge lists every repo exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn list_repos_across_layout_changes() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node("rslr-a", &store).await;
    let b = node("rslr-b", &store).await;
    balanced(&[&a, &b]).await;
    let mut want: HashSet<String> = HashSet::new();
    for n in [&a, &b] {
        for acct in accounts(&n.s, 15).await {
            want.insert(acct.did);
        }
    }
    let mut got: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut page = 0;
    loop {
        let mut q = vec![("limit", "4".to_string())];
        if let Some(c) = &cursor {
            q.push(("cursor", c.clone()));
        }
        let via = [&a, &b][page % 2];
        let r = via.s.xrpc.get_multi("com.atproto.sync.listRepos", &q, &Auth::None).await.ok();
        got.extend(r["repos"].as_array().unwrap().iter().map(|x| x["did"].as_str().unwrap().to_string()));
        page += 1;
        let l = layout(&a);
        if page == 2 {
            // split the shard the cursor is in
            let slot = r["cursor"].as_str().unwrap().split(':').next().unwrap().parse::<u16>().unwrap();
            let r = admin(&a.s, "vlpds.admin.splitShard", json!({"shard": l.shard_of_slot(slot), "wait": true})).await;
            assert_eq!(r["done"], json!(true), "{r}");
            settled(&[&a, &b], l.version + 1, Duration::from_secs(20)).await;
        }
        if page == 4 {
            // merge the shard the cursor is in with the next one
            let slot = r["cursor"].as_str().unwrap().split(':').next().unwrap().parse::<u16>().unwrap();
            let i = l.index_of_slot(slot).min(l.shards.len() - 2);
            let r = admin(&b.s, "vlpds.admin.mergeShards", json!({"left": l.shards[i].id, "right": l.shards[i + 1].id, "wait": true})).await;
            assert_eq!(r["done"], json!(true), "{r}");
            settled(&[&a, &b], l.version + 1, Duration::from_secs(20)).await;
        }
        match r["cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => break,
        }
        assert!(page < 100);
    }
    let set: HashSet<String> = got.iter().cloned().collect();
    assert_eq!(set.len(), got.len(), "a repo listed twice");
    assert!(want.is_subset(&set), "missing {:?}", want.difference(&set).collect::<Vec<_>>());
    let order: Vec<(u16, String)> = got.iter().map(|d| (vlpds::slots::slot_of(d), d.clone())).collect();
    assert!(order.windows(2).all(|w| w[0] < w[1]), "pages in (slot, DID) order");
}

/// The policy hook (off by default) splits a shard applying more writes per
/// second than its threshold, on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn policy_splits_a_hot_shard() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let raw = store.clone() as Arc<dyn object_store::ObjectStore>;
    let s = TestServer::spawn_with(move |c| {
        c.memory_store = Some(raw);
        c.shards = SHARDS;
        c.cluster = Some(vlpds::cluster::ClusterConfig {
            node_id: "rspol".into(),
            addr: c.public_url.clone(),
            shards: SHARDS,
            ttl: Duration::from_millis(1500),
            renew_every: Duration::from_millis(100),
            skew: Duration::from_millis(200),
            ..Default::default()
        });
        c.reshard_policy = vlpds::reshard::Policy { split_bytes: None, split_writes_per_sec: Some(20.0) };
    })
    .await;
    let acct = s.create_account("pol").await;
    let hot = s.app.partitions.shard_of(&acct.did);
    let t = Instant::now();
    let mut n = 0;
    // a write may see 503 while its shard is frozen for the split: retried
    let post = |text: String| {
        let body = json!({"repo": acct.did, "collection": "app.bsky.feed.post", "record": post_record(&text)});
        let (x, auth) = (&s.xrpc, acct.auth());
        async move {
            for _ in 0..100 {
                let r = x.post("com.atproto.repo.createRecord", &body, &auth).await;
                if r.status != 503 {
                    assert!(r.is_ok(), "{r:?}");
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            panic!("write kept failing with 503");
        }
    };
    while cluster(&s).layout().contains(hot) {
        assert!(t.elapsed() < Duration::from_secs(20), "the hot shard never split");
        post(format!("hot {n}")).await;
        n += 1;
    }
    let l = cluster(&s).layout();
    assert_eq!((l.version, l.shards.len()), (2, SHARDS as usize + 1));
    // and it stops after one: the rate limit holds it for a minute
    for i in 0..50 {
        post(format!("after {i}")).await;
    }
    assert_eq!(cluster(&s).layout().version, 2);
}

/// Splits and merges back to back under write load (a split, a merge of its
/// halves, a merge across nodes, a split of that), every acked write read
/// back after each op. Merging a split's halves while they still held the
/// parent's L0 SSTs lost one half's keys at the merged shard's first
/// compaction (SlateDB repeated the shared L0 view id; DESIGN.md "Patched
/// SlateDB"): without the fix this failed 9 runs in 10 (the deterministic
/// check is partition.rs `merging_a_splits_halves_keeps_their_shared_l0s`).
/// `RESHARD_CYCLES=6` for a longer run.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn repeated_splits_and_merges_under_write_load() {
    let cycles: usize = std::env::var("RESHARD_CYCLES").ok().and_then(|v| v.parse().ok()).unwrap_or(2);
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node("rss-a", &store).await;
    let accts = accounts(&a.s, 18).await;
    let b = node("rss-b", &store).await;
    let c = node("rss-c", &store).await;
    balanced(&[&a, &b, &c]).await;
    let load = Load::start(vec![a.s.url.clone(), b.s.url.clone(), c.s.url.clone()], &accts);
    load.progress(30).await;
    let nodes = [&a, &b, &c];
    // after each op: writes keep landing for a while (the new shards flush
    // L0s, run deep and compact, inherited L0s included), then every acked
    // write so far reads back
    let after_op = || async {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        load.progress(20).await;
        let snap = load.acked.lock().clone();
        verify_readable(&a.s, &snap).await;
    };
    for cyc in 0..cycles {
        // split a shard of one node, asked of another
        let l = layout(&a);
        let target = *owned(nodes[cyc % 3]).first().unwrap();
        let r = admin(&nodes[(cyc + 1) % 3].s, "vlpds.admin.splitShard", json!({"shard": target, "wait": true})).await;
        assert_eq!(r["done"], json!(true), "{r}");
        let l2 = settled(&nodes, l.version + 1, Duration::from_secs(20)).await;
        let kids: Vec<u16> = r["op"]["children"].as_array().unwrap().iter().map(|c| c["id"].as_u64().unwrap() as u16).collect();
        after_op().await;
        // merge its halves back, asked of a third node
        let r = admin(&nodes[(cyc + 2) % 3].s, "vlpds.admin.mergeShards", json!({"left": kids[0], "right": kids[1], "wait": true})).await;
        assert_eq!(r["done"], json!(true), "{r}");
        let l3 = settled(&nodes, l2.version + 1, Duration::from_secs(20)).await;
        after_op().await;
        // a merge across nodes, then split that back
        let (x, y) = {
            let owner = |s: u16| nodes.iter().position(|n| owned(n).contains(&s));
            let pair = l3.shards.windows(2).find(|w| owner(w[0].id) != owner(w[1].id)).expect("adjacent shards on two nodes");
            (pair[0].id, pair[1].id)
        };
        let r = admin(&nodes[cyc % 3].s, "vlpds.admin.mergeShards", json!({"left": x, "right": y, "wait": true})).await;
        assert_eq!(r["done"], json!(true), "{r}");
        let l4 = settled(&nodes, l3.version + 1, Duration::from_secs(20)).await;
        let m = r["op"]["children"][0]["id"].as_u64().unwrap() as u16;
        after_op().await;
        let r = admin(&nodes[(cyc + 1) % 3].s, "vlpds.admin.splitShard", json!({"shard": m, "wait": true})).await;
        assert_eq!(r["done"], json!(true), "{r}");
        settled(&nodes, l4.version + 1, Duration::from_secs(20)).await;
        after_op().await;
    }
    let (acked, failed, gap) = load.stop().await;
    eprintln!("{} acked, {failed} failed attempts, longest per-account gap {gap:?}", acked.len());
    for n in nodes {
        verify_readable(&n.s, &acked).await;
    }
}
