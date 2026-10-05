//! Forwarding of sequenced writes to the services registered for a space's
//! notifications (`sN`), as the reference's `processNotifyWrite` queues
//! them: best effort, after the write is durable, never on the request's
//! path; a service that misses one catches up with listRepos.
//!
//! Jobs leave the authority's worker in ack order (so spaceRev order) for
//! a dispatcher (one of [`DISPATCHERS`], by space, so a space's jobs stay in
//! order and one big space holds up only its share), which reads the
//! space's registrations and hands each (space, service) its own lane. A
//! lane sends one at a time, so a service sees a space's spaceRevs in
//! order, and a slow one holds up only itself. A taken-down space forwards
//! nothing.
//!
//! Only a writer's newest state is worth sending: a forward waiting in a
//! lane is replaced by a newer one of the same writer, and a failed one is
//! retried only while nothing newer of its writer waits. The `prevSpaceRev`
//! a lane sends is the last spaceRev it tried to send, so what it skipped
//! this way leaves no gap (the service pulls the writer from its own last
//! rev anyway). Otherwise it is the true predecessor from the sequence. A
//! service may not hear some of them, and a gap only sends it to listRepos;
//! two forwards naming one prevSpaceRev with different successors would
//! fork the chain it follows, so the lane's memory counts a send it tried
//! (which may have arrived though it failed) and is only used within the
//! shard lease that sequenced the forward: a lease's first forward per
//! registration, and anything after a lost one, names the true predecessor.
//!
//! Bounds: the dispatchers' queues, each lane, and each service host's
//! queued forwards across its lanes (all drop the oldest and count it), a
//! host's sends in flight and all sends in flight. Registrations per space
//! are capped where they're made. Sends go over their own pooled guarded
//! client.

use super::host::Forward;
use super::repo::Sequenced;
use crate::state::SpaceId;
use crate::tid::Tid;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Weak};
use std::time::Duration;

/// Jobs waiting for each dispatcher.
pub const QUEUE: usize = 4096;
pub const DISPATCHERS: usize = 8;
/// Sends in flight to all hosts.
pub const SENDS: usize = 512;
/// Forwards waiting in one (space, service) lane: one per writer.
pub const LANE: usize = 256;
/// Forwards waiting across one service host's lanes.
pub const HOST_QUEUE: usize = 4096;
/// Sends in flight to one service host.
pub const HOST_SENDS: usize = 16;
pub const RETRY_BASE: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(60);
const ATTEMPTS: u32 = 6;

/// A writer's state the authority just sequenced.
pub struct Job {
    pub authority: Arc<str>,
    pub uri: Arc<str>,
    pub sid: SpaceId,
    pub writer: String,
    pub repo_rev: Tid,
    pub hash: [u8; 32],
    pub seq: Sequenced,
    /// The lease epoch of the authority's shard that sequenced it.
    pub epoch: u64,
}

type LaneKey = (Arc<str>, SpaceId, String);

struct Queued {
    f: Forward,
    epoch: u64,
    host: String,
    /// Something before it in the space's sequence was lost.
    gap: bool,
}

#[derive(Default)]
struct Lane {
    queue: VecDeque<Queued>,
    running: bool,
    /// The lease epoch of the newest forward handed to the lane, and its
    /// spaceRev.
    epoch: u64,
    pushed: Option<Tid>,
    /// The spaceRev the lane last tried to send, and its lease epoch.
    sent: Option<(Tid, u64)>,
    /// The next forward follows a lost one.
    gap_next: bool,
}

impl Lane {
    /// The next forward and the prevSpaceRev to send it with (None: it is
    /// behind one already tried, so not sent), counted as tried.
    #[allow(clippy::option_option)]
    fn next(&mut self) -> Option<(Queued, Option<Option<Tid>>)> {
        let q = self.queue.pop_front()?;
        if self.sent.is_some_and(|(t, _)| t >= q.f.space_rev) {
            return Some((q, None));
        }
        let prev = match self.sent {
            Some((s, epoch)) if !q.gap && epoch == q.epoch => Some(s),
            _ => q.f.prev_space_rev,
        };
        self.sent = Some((q.f.space_rev, q.epoch));
        Some((q, Some(prev)))
    }

    /// The forward at the head was lost.
    fn mark_gap(&mut self) {
        match self.queue.front_mut() {
            Some(q) => q.gap = true,
            None => self.gap_next = true,
        }
    }
}

struct Host {
    queued: usize,
    sends: Arc<tokio::sync::Semaphore>,
}

#[derive(Default)]
struct State {
    lanes: HashMap<LaneKey, Lane>,
    hosts: HashMap<String, Host>,
}

impl State {
    fn host(&mut self, host: &str) -> &mut Host {
        self.hosts
            .entry(host.to_string())
            .or_insert_with(|| Host { queued: 0, sends: Arc::new(tokio::sync::Semaphore::new(HOST_SENDS)) })
    }

    /// Drops `host`'s entry if nothing of it is queued or in flight.
    fn unhost(&mut self, host: &str) {
        if self.hosts.get(host).is_some_and(|h| h.queued == 0 && Arc::strong_count(&h.sends) == 1) {
            self.hosts.remove(host);
        }
    }

    fn unqueued(&mut self, host: &str) {
        crate::metrics::space_fanout_depth(-1);
        if let Some(h) = self.hosts.get_mut(host) {
            h.queued -= 1;
            if h.queued == 0 && Arc::strong_count(&h.sends) == 1 {
                self.hosts.remove(host);
            }
        }
    }
}

/// How a lane's forward ended.
enum Sent {
    Delivered,
    /// A newer forward of its writer waits: that one carries its state.
    Superseded,
    Lost,
}

pub struct Fanout {
    tx: Vec<tokio::sync::mpsc::Sender<Job>>,
    rx: parking_lot::Mutex<Option<Vec<tokio::sync::mpsc::Receiver<Job>>>>,
    sends: Arc<tokio::sync::Semaphore>,
    state: parking_lot::Mutex<State>,
    /// Expired registrations being pruned.
    pruning: parking_lot::Mutex<HashSet<(Arc<str>, String)>>,
    /// The first retry's pause.
    retry_base: Duration,
}

/// The `host:port` that sends to `endpoint` share.
fn host_of(endpoint: &str) -> String {
    match reqwest::Url::parse(endpoint) {
        Ok(u) => format!("{}:{}", u.host_str().unwrap_or_default(), u.port_or_known_default().unwrap_or(0)),
        Err(_) => endpoint.to_string(),
    }
}

/// `base` doubling to a minute, then 50-100% of that.
fn backoff(base: Duration, attempts: u32) -> Duration {
    let d = base.saturating_mul(1 << attempts.saturating_sub(1).min(16)).min(RETRY_MAX);
    d.mul_f64(0.5 + rand::random::<f64>() / 2.0)
}

impl Fanout {
    pub fn new(queue: usize, retry_base: Duration) -> Fanout {
        let (tx, rx) = (0..DISPATCHERS).map(|_| tokio::sync::mpsc::channel(queue)).unzip();
        Fanout {
            tx,
            rx: parking_lot::Mutex::new(Some(rx)),
            sends: Arc::new(tokio::sync::Semaphore::new(SENDS)),
            state: Default::default(),
            pruning: Default::default(),
            retry_base,
        }
    }

    /// Called from the worker's ack: never waits. A job dropped here shows
    /// up as a gap in its lanes (the next forward's prevSpaceRev isn't the
    /// last one they were handed).
    pub fn notify(&self, job: Job) {
        let tx = &self.tx[job.sid[0] as usize % self.tx.len()];
        if let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = tx.try_send(job) {
            crate::metrics::space_fanout_dropped("queue_full");
        }
    }

    /// Starts the dispatcher (once). `app` is held weakly: it ends with the
    /// server.
    pub fn start(self: &Arc<Self>, app: Weak<crate::xrpc::App>) {
        let Some(rxs) = self.rx.lock().take() else { return };
        for rx in rxs {
            self.clone().dispatch(app.clone(), rx);
        }
    }

    fn dispatch(self: Arc<Self>, app: Weak<crate::xrpc::App>, mut rx: tokio::sync::mpsc::Receiver<Job>) {
        let me = self;
        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                let Some(a) = app.upgrade() else { return };
                match crate::xrpc::space::space_sid_takendown(&a, &job.authority, &job.sid).await {
                    Ok(false) => {}
                    Ok(true) => {
                        crate::metrics::space_fanout_dropped("takendown");
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(space = %hex::encode(job.sid), "space takedown unreadable: {}", e.message);
                        crate::metrics::space_notify("fanout", "error");
                        continue;
                    }
                }
                let (regs, expired) = match super::host::registrations(&a, &job.authority, &job.sid).await {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!(space = %job.uri, "space notify registrations unreadable: {e:#}");
                        crate::metrics::space_notify("fanout", "error");
                        continue;
                    }
                };
                for service in expired {
                    me.prune(&a, &job.uri, service);
                }
                drop(a);
                for (service, row) in regs {
                    let f = Forward {
                        authority: job.authority.clone(),
                        uri: job.uri.clone(),
                        service,
                        endpoint: row.endpoint,
                        writer: job.writer.clone(),
                        repo_rev: job.repo_rev,
                        hash: job.hash,
                        space_rev: job.seq.space_rev,
                        prev_space_rev: job.seq.prev,
                        expires: row.expires,
                    };
                    me.push(&app, job.sid, f, job.epoch);
                }
            }
        });
    }

    /// Deletes an expired registration of `uri` in the background (one
    /// prune per registration at a time), unless it was renewed meanwhile.
    pub fn prune(self: &Arc<Self>, app: &Arc<crate::xrpc::App>, uri: &Arc<str>, service: String) {
        if !self.pruning.lock().insert((uri.clone(), service.clone())) {
            return;
        }
        let (me, app, uri) = (self.clone(), app.clone(), uri.clone());
        tokio::spawn(async move {
            if let Err(e) = crate::xrpc::space::prune_registration(&app, &uri, &service).await {
                tracing::info!(space = %uri, service, "expired space notify registration not pruned: {e:#}");
            }
            me.pruning.lock().remove(&(uri, service));
        });
    }

    fn push(self: &Arc<Self>, app: &Weak<crate::xrpc::App>, sid: SpaceId, f: Forward, epoch: u64) {
        let key: LaneKey = (f.authority.clone(), sid, f.service.clone());
        let host = host_of(&f.endpoint);
        let mut st = self.state.lock();
        let lane = st.lanes.entry(key.clone()).or_default();
        if lane.epoch != epoch {
            (lane.epoch, lane.pushed, lane.gap_next) = (epoch, None, false);
        }
        // a catch-up forward behind what this lease already sequenced: the
        // newer one names it as its predecessor
        let stale = |t: Tid| t >= f.space_rev;
        if lane.pushed.is_some_and(stale) || lane.sent.is_some_and(|(t, _)| stale(t)) {
            if lane.queue.is_empty() && !lane.running {
                st.lanes.remove(&key);
                st.unhost(&host);
            }
            return;
        }
        let mut gap = std::mem::take(&mut lane.gap_next) || lane.pushed.is_some_and(|p| f.prev_space_rev != Some(p));
        lane.pushed = Some(f.space_rev);
        if let Some(i) = lane.queue.iter().position(|q| q.f.writer == f.writer) {
            let old = lane.queue.remove(i).expect("present");
            if old.gap {
                match lane.queue.get_mut(i) {
                    Some(q) => q.gap = true,
                    None => gap = true,
                }
            }
            crate::metrics::space_fanout_coalesced();
            st.unqueued(&old.host);
        }
        let host_full = st.host(&host).queued >= HOST_QUEUE;
        let lane = st.lanes.get_mut(&key).expect("present");
        if lane.queue.len() >= LANE || host_full {
            crate::metrics::space_fanout_dropped(if host_full { "host_full" } else { "lane_full" });
            let Some(old) = lane.queue.pop_front() else {
                // an idle lane keeps nothing worth holding: its next push
                // starts from the forward's own prevSpaceRev anyway
                if !lane.running {
                    st.lanes.remove(&key);
                    st.unhost(&host);
                } else {
                    lane.gap_next = true;
                }
                return;
            };
            lane.mark_gap();
            gap |= lane.queue.is_empty();
            st.unqueued(&old.host);
        }
        let lane = st.lanes.get_mut(&key).expect("present");
        lane.queue.push_back(Queued { f, epoch, host: host.clone(), gap });
        let start = !std::mem::replace(&mut lane.running, true);
        st.host(&host).queued += 1;
        crate::metrics::space_fanout_depth(1);
        drop(st);
        if start {
            let (me, app) = (self.clone(), app.clone());
            tokio::spawn(async move { me.run(app, key).await });
        }
    }

    /// A lane's sender: one forward at a time, until the lane is empty.
    async fn run(self: Arc<Self>, app: Weak<crate::xrpc::App>, key: LaneKey) {
        loop {
            let (q, prev, sends) = {
                let mut st = self.state.lock();
                let Some(lane) = st.lanes.get_mut(&key) else { return };
                let Some((q, prev)) = lane.next() else {
                    st.lanes.remove(&key);
                    return;
                };
                st.unqueued(&q.host);
                let Some(prev) = prev else { continue };
                let sends = st.host(&q.host).sends.clone();
                (q, prev, sends)
            };
            let mut attempts = 0;
            let sent = loop {
                let Some(a) = app.upgrade() else { return };
                let result = {
                    let _host = sends.acquire().await;
                    let _all = self.sends.acquire().await;
                    super::host::forward(&a, &q.f, prev).await
                };
                crate::metrics::space_notify("fanout", result);
                match result {
                    "ok" => break Sent::Delivered,
                    "error" => {}
                    _ => break Sent::Lost,
                }
                attempts += 1;
                if q.f.expires <= crate::tid::now_micros() {
                    crate::metrics::space_fanout_dropped("expired");
                    self.prune(&a, &q.f.uri, q.f.service.clone());
                    break Sent::Lost;
                }
                drop(a);
                if attempts >= ATTEMPTS {
                    crate::metrics::space_fanout_dropped("gave_up");
                    break Sent::Lost;
                }
                tokio::time::sleep(backoff(self.retry_base, attempts)).await;
                if self.superseded(&key, &q.f.writer) {
                    crate::metrics::space_fanout_coalesced();
                    break Sent::Superseded;
                }
            };
            let mut st = self.state.lock();
            let Some(lane) = st.lanes.get_mut(&key) else { return };
            if let Sent::Lost = sent {
                lane.mark_gap();
            }
        }
    }

    /// A newer forward of `writer` waits in the lane.
    fn superseded(&self, key: &LaneKey, writer: &str) -> bool {
        self.state.lock().lanes.get(key).is_some_and(|l| l.queue.iter().any(|q| q.f.writer == writer))
    }

    /// Forwards waiting in lanes.
    pub fn pending(&self) -> usize {
        self.state.lock().lanes.values().map(|l| l.queue.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fanout() -> Arc<Fanout> {
        Arc::new(Fanout::new(16, RETRY_BASE))
    }

    fn fwd(service: &str, writer: &str, rev: u64, prev: Option<u64>) -> Forward {
        Forward {
            authority: "did:plc:auth".into(),
            uri: "at://did:plc:auth/space/t/k".into(),
            service: service.into(),
            endpoint: "https://syncer.example".into(),
            writer: writer.into(),
            repo_rev: Tid(rev),
            hash: [0; 32],
            space_rev: Tid(rev),
            prev_space_rev: prev.map(Tid),
            expires: u64::MAX,
        }
    }

    fn lane(f: &Fanout) -> Vec<(String, u64, bool)> {
        let st = f.state.lock();
        let l = st.lanes.values().next().unwrap();
        l.queue.iter().map(|q| (q.f.writer.clone(), q.f.space_rev.0, q.gap)).collect()
    }

    /// No runner starts without an app: pushes only queue.
    #[test]
    fn coalesces_per_writer_and_marks_gaps() {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            let f = fanout();
            let app = Weak::new();
            // the lane is "running" (its sender would be in flight)
            f.push(&app, [0; 16], fwd("s", "a", 1, None), 1);
            f.state.lock().lanes.values_mut().next().unwrap().queue.clear();
            f.push(&app, [0; 16], fwd("s", "a", 2, Some(1)), 1);
            f.push(&app, [0; 16], fwd("s", "b", 3, Some(2)), 1);
            f.push(&app, [0; 16], fwd("s", "a", 4, Some(3)), 1);
            assert_eq!(lane(&f), vec![("b".into(), 3, false), ("a".into(), 4, false)]);
            // a job lost upstream: the next one follows a gap
            f.push(&app, [0; 16], fwd("s", "c", 6, Some(5)), 1);
            assert_eq!(lane(&f)[2], ("c".into(), 6, true));
            // replacing a gap-marked forward keeps the mark in place
            f.push(&app, [0; 16], fwd("s", "c", 7, Some(6)), 1);
            assert_eq!(lane(&f)[2], ("c".into(), 7, true));
            assert_eq!(f.pending(), 3);
        });
    }

    fn send_next(f: &Fanout) -> Option<(u64, Option<u64>)> {
        let mut st = f.state.lock();
        let l = st.lanes.values_mut().next().unwrap();
        let (q, prev) = l.next()?;
        Some((q.f.space_rev.0, prev?.map(|p| p.0)))
    }

    /// prevSpaceRev never forks: a lane names the last spaceRev it tried
    /// (delivered or not) only within one lease, and drops what's behind it.
    #[test]
    fn prev_space_rev_is_tried_memory_within_a_lease_else_the_true_one() {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            let f = fanout();
            let app = Weak::new();
            f.push(&app, [0; 16], fwd("s", "a", 1, None), 1);
            assert_eq!(send_next(&f), Some((1, None)));
            // tried (it may have arrived), then 2 coalesced away by 3
            f.push(&app, [0; 16], fwd("s", "a", 2, Some(1)), 1);
            f.push(&app, [0; 16], fwd("s", "a", 3, Some(2)), 1);
            f.push(&app, [0; 16], fwd("s", "b", 4, Some(3)), 1);
            assert_eq!(send_next(&f), Some((3, Some(1))));
            // a new lease (a takeover, a handback): the true predecessor
            f.push(&app, [0; 16], fwd("s", "c", 6, Some(5)), 2);
            assert_eq!(send_next(&f), Some((4, Some(3))));
            assert_eq!(send_next(&f), Some((6, Some(5))));
            // a catch-up forward behind what the lane has: dropped
            f.push(&app, [0; 16], fwd("s", "c", 5, Some(4)), 2);
            assert_eq!(f.pending(), 0);
            f.push(&app, [0; 16], fwd("s", "d", 8, Some(7)), 2);
            assert_eq!(send_next(&f), Some((8, Some(7))));
        });
    }

    #[test]
    fn lanes_and_hosts_are_bounded() {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            let f = fanout();
            let app = Weak::new();
            for i in 0..LANE as u64 + 5 {
                f.push(&app, [0; 16], fwd("s", &format!("w{i}"), i + 1, i.checked_sub(0).filter(|p| *p > 0)), 1);
            }
            let q = lane(&f);
            assert_eq!(q.len(), LANE);
            assert_eq!(q[0].1, 6, "the oldest went");
            assert!(q[0].2, "after a gap");
            assert_eq!(f.state.lock().hosts["syncer.example:443"].queued, LANE);
        });
    }

    #[test]
    fn backoff_is_jittered_and_capped() {
        for a in 1..20 {
            let d = backoff(RETRY_BASE, a);
            assert!(d >= RETRY_BASE / 2 && d <= RETRY_MAX, "{a}: {d:?}");
        }
        assert!(backoff(RETRY_BASE, 1) <= RETRY_BASE);
    }
}
