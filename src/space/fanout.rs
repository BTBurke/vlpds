//! Forwarding of sequenced writes to the services registered for a space's
//! notifications (`sN`), as the reference's `processNotifyWrite` queues
//! them: best effort, after the write is durable, never on the request's
//! path; a service that misses one catches up with listRepos.
//!
//! Jobs leave the authority's worker in ack order (so spaceRev order) for
//! one dispatcher, which reads the space's registrations and hands each
//! service its own lane. A lane sends one at a time, so a service sees a
//! space's spaceRevs in order, and a slow one holds up only itself. Lanes
//! are bounded: when one is full its oldest pending forward goes, which the
//! service sees as a prevSpaceRev gap.

use super::host::Forward;
use super::repo::Sequenced;
use crate::state::SpaceId;
use crate::tid::Tid;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Weak};

/// Jobs waiting for the dispatcher.
pub const QUEUE: usize = 4096;
/// Forwards waiting in one (space, service) lane.
pub const LANE: usize = 256;

/// A writer's state the authority just sequenced.
pub struct Job {
    pub authority: Arc<str>,
    pub uri: Arc<str>,
    pub sid: SpaceId,
    pub writer: String,
    pub repo_rev: Tid,
    pub hash: [u8; 32],
    pub seq: Sequenced,
}

type LaneKey = (Arc<str>, SpaceId, String);

#[derive(Default)]
struct Lane {
    queue: VecDeque<Forward>,
    running: bool,
}

pub struct Fanout {
    tx: tokio::sync::mpsc::Sender<Job>,
    rx: parking_lot::Mutex<Option<tokio::sync::mpsc::Receiver<Job>>>,
    lanes: parking_lot::Mutex<HashMap<LaneKey, Lane>>,
}

impl Fanout {
    pub fn new(queue: usize) -> Fanout {
        let (tx, rx) = tokio::sync::mpsc::channel(queue);
        Fanout { tx, rx: parking_lot::Mutex::new(Some(rx)), lanes: Default::default() }
    }

    /// Called from the worker's ack: never waits.
    pub fn notify(&self, job: Job) {
        if let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = self.tx.try_send(job) {
            crate::metrics::space_fanout_dropped("queue_full");
        }
    }

    /// Starts the dispatcher (once). `app` is held weakly: it ends with the
    /// server.
    pub fn start(self: &Arc<Self>, app: Weak<crate::xrpc::App>) {
        let Some(mut rx) = self.rx.lock().take() else { return };
        let me = self.clone();
        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                let Some(a) = app.upgrade() else { return };
                let regs = match super::host::registrations(&a, &job.authority, &job.sid).await {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!(space = %job.uri, "space notify registrations unreadable: {e:#}");
                        crate::metrics::space_notify("fanout", "error");
                        continue;
                    }
                };
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
                    };
                    me.push(&app, job.sid, f);
                }
            }
        });
    }

    fn push(self: &Arc<Self>, app: &Weak<crate::xrpc::App>, sid: SpaceId, f: Forward) {
        let key: LaneKey = (f.authority.clone(), sid, f.service.clone());
        let mut lanes = self.lanes.lock();
        let lane = lanes.entry(key.clone()).or_default();
        if lane.queue.len() >= LANE {
            lane.queue.pop_front();
            crate::metrics::space_fanout_dropped("lane_full");
        }
        lane.queue.push_back(f);
        crate::metrics::space_fanout_depth(1);
        if std::mem::replace(&mut lane.running, true) {
            return;
        }
        drop(lanes);
        let (me, app) = (self.clone(), app.clone());
        tokio::spawn(async move {
            loop {
                let next = {
                    let mut lanes = me.lanes.lock();
                    let Some(lane) = lanes.get_mut(&key) else { return };
                    match lane.queue.pop_front() {
                        Some(f) => f,
                        None => {
                            lanes.remove(&key);
                            return;
                        }
                    }
                };
                crate::metrics::space_fanout_depth(-1);
                let Some(a) = app.upgrade() else { return };
                let result = super::host::forward(&a, &next).await;
                crate::metrics::space_notify("fanout", result);
            }
        });
    }

    /// Forwards waiting in lanes.
    pub fn pending(&self) -> usize {
        self.lanes.lock().values().map(|l| l.queue.len()).sum()
    }
}
