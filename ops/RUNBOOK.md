# vlpds operator runbook

Companion to `ops/alerts.yml`. Each alert has a section below whose heading is
the alert name (the `runbook_url` anchors point here). Everything is grounded in
`DESIGN.md` and `src/`. Where something is inferred rather than read from code,
it is marked **(unverified)**.

- [Background you need](#background-you-need)
- [Tools: endpoints, CLI, logs, exit codes](#tools-endpoints-cli-logs-exit-codes)
- [Alerts](#alerts)
- [Procedures](#procedures)
- [What NOT to do](#what-not-to-do)
- [Metric gaps](#metric-gaps)

---

## Background you need

- **All durable state is in the object store** under `--prefix` in `--s3-bucket`:
  `log/{log_id}/{ordinal}.seg` (each node incarnation's commit log), `state/{shard}/`
  (one SlateDB per shard, its WAL disabled), `assign/{shard}` + `assign/layout`
  (who owns what, and the slot -> shard map), `nodes/{node_id}` (node leases),
  `writers/{w}` (unique seq low byte per live node), `retain/{log_id}` (retention
  reports), `handle/`, `email/`, `blob/`. The local disk is only a SlateDB SST cache.
- **Shards.** 65,536 hash slots grouped into shards (default `--shards 64`,
  changed online by split/merge). Each shard has exactly one owner node at a time.
  A node takes free or orphaned shards up to its fair share, `ceil(shards / live
  nodes)`, and hands extras to joiners.
- **One log per node incarnation.** A write is acked only after its segment and
  every earlier one are durable (`If-None-Match` PUTs, up to `--log-inflight` 4 in
  flight, finalized in order) **and** only while the node's lease is valid.
- **Leases.** `nodes/{node_id}` is CAS-renewed every TTL/5 (2 s at the default
  `--lease-ttl-ms 10000`). A node's own validity ends `TTL - skew` (8 s) after the
  *send time* of its last successful renewal. A renewal round trip over
  `0.4 x TTL` = **4 s** opens a validity gap and the node **fail-stops**. A
  cluster-wide object-store brownout past 4 s stops every node.
- **Takeover.** Peers presume a node dead after its lease has not changed for
  `TTL + skew` (12 s) of their own monotonic time, or within ~1.5-2.5 renew
  intervals (3-5 s) if its advertised port refuses TCP connections (process gone).
  The new owner **fences** the dead log (conditional create at the end of its
  durable prefix), CASes the assignment, replays the shard's spans, waits out the
  previous owner's `seq_floor` (commit-wait, max 30 s), then serves.
- **Fail-stop is the safety mechanism.** A node that might be wrong exits; the
  supervisor restarts it and it rejoins. A wrong "dead" presumption costs
  availability, never an acked write (DESIGN "Why safety needs no clocks").
- **Firehose.** Every node merges every node's log and emits an event once
  `seq <= min watermark` over all logs. One slow, stalled or unfenced log holds
  the firehose back on every node. Merged emit lags by the largest wall-clock
  offset between nodes.
- **Forwarding.** Any node accepts any request; repo writes/reads for a shard it
  doesn't own are proxied to the owner with a 3 s time-to-first-byte deadline (30 s
  for exports, uploads, proxying). An owner that doesn't answer in time yields 503
  `PartitionUnavailable` (ambiguous, never resent). An owner that can't start a
  forwarded write within `--forwarded-write-start-ms` (1 s) answers 503
  `RepoLoading` (never applied); a write that finds the shard moving gets 503
  `ShardMoved`. The entry node resends those (and connect-refused forwards) to the
  current owner for up to 20 s.

## Tools: endpoints, CLI, logs, exit codes

**Endpoints** (any node; `/metrics` moves to `--metrics-listen` if set):

| What | How |
|---|---|
| Liveness | `GET /xrpc/_health` -> `{"version":"vlpds"}` |
| Metrics | `GET /metrics` (Prometheus text) |
| Cluster view (admin) | `GET /xrpc/vlpds.admin.getClusterStatus` with `Authorization: Basic base64(admin:$VLPDS_ADMIN_TOKEN)`. Returns `node`, `log`, `logDurableOrdinal`, `owned` (shard ids), `shards`, `table` (owner per shard in slot order, `null` = unowned), `layout` (`version`, `shards`, `op` = split/merge in progress), `leaseValid`, `leaseExpiresMs`, `fencedLogs`, `firehose.{lastEmitted,minWatermark,sources[{log,watermark,local}]}`, and `nodes[]` with each peer's `reachable`, `leaseValid`, `logDurableOrdinal`, `owned` count, `writer`, `expiresMs` (peers fetched with a 1.5 s timeout). |
| Cluster view (node-to-node) | `GET /internal/v1/cluster` with header `x-vlpds-internal: $VLPDS_INTERNAL_TOKEN`: this node's `owned`, `table`, `layout`, `peers`, `lease_valid`, `log_durable_ordinal`, `firehose_last_emitted`, `firehose_min_watermark`. |
| Operator console | `/admin` (Cluster page polls getClusterStatus), `/admin/metrics` (live metrics). |
| Shard layout | `vlpds admin layout --url http://<node>:2583` (`VLPDS_ADMIN_TOKEN` env), or `GET /xrpc/vlpds.admin.getShardLayout`. Also `shard-split`, `shard-merge`, `reshard-abort` (abort only before the flip). |
| CPU profile | `just profile <node:port> [seconds]` (`/debug/pprof/`) |

A quick cluster check:

```sh
curl -s -u "admin:$VLPDS_ADMIN_TOKEN" http://NODE:2583/xrpc/vlpds.admin.getClusterStatus \
  | jq '{node, leaseValid, owned: (.owned|length), shards, unowned: ([.table[]|select(.==null)]|length),
         op: .layout.op, fenced: .fencedLogs,
         nodes: [.nodes[] | {node, reachable, leaseValid, owned, logDurableOrdinal}],
         firehose: .firehose.minWatermark}'
```

**Exit codes** (fail-stops; the supervisor must restart on any of them):

| Code | `reason` | Meaning | Log line (error level) |
|---|---|---|---|
| 2 | `segment_upload` | Segment upload task failed | `segment upload task failed: ...; exiting` |
| 3 | `fenced` / `ordinal_taken` | Our log was fenced by a successor, or another writer took our segment ordinal | `our log was fenced by a successor: fail-stop` / `segment ordinal taken by another writer: fail-stop` |
| 4 | `state_apply` | SlateDB apply of a durable segment failed | `state apply failed: ...; exiting` |
| 5 | `lease_lost` / `lease_lapsed` | Lease lost or lapsed (any reason) | `node lease lost unexpectedly: fail-stop` (`lease_lost`), preceded by one of: `node lease lost (CAS conflict)`, `node lease lapsed before renewal`, `node lease lapsed past takeover` (watchdog), `a shard we hold was reassigned`, `a shard failed to close cleanly`, `our log did not quiesce`; or `node lease lapsed before segment PUT` / `before ack` (`lease_lapsed`) |
| 6 | `signature_fault` | 3 signatures failed verification right after signing within a minute (suspected memory/CPU fault; see [VlpdsSignatureFault](#vlpdssignaturefault)) | `repeated signature faults: fail-stop (suspect this host's memory or CPU)`, preceded by `signature failed verification against the signing key's public key` (purpose, recent) |

**How the previous process ended** is a metric on the next one
(`src/lifecycle.rs`): each fail-stop writes its `reason` and code to the
exit-state file (`--exit-state-file`, default `vlpds-exit-<node-id>.json` in
`--cache-dir`; with neither set nothing is kept) just before exiting, and the
next start exports `vlpds_last_exit_reason_info{reason,code}` = 1 for as long
as it runs. Besides the table: `clean` (graceful stop, code 0), `error` (code
1: startup or serve error), `crash` (the file still says `running`: SIGKILL,
OOM kill, abort, host loss), `none` (first start or no file).
`vlpds_process_start_time_seconds` (and the standard
`process_start_time_seconds`) dates the restart. Cluster-side, whoever fences
an incarnation that ended without fencing its own log counts
`vlpds_peer_takeovers_total{reason="peer"|"restart"}` (a graceful stop fences
its own log, so it never counts): that survives a node that never comes back.

**Useful log lines** (tracing, info/warn unless noted):
`acquired shards` (shards, owned, fair, live), `shards opened` (shards,
`segments_replayed`, `replayed_ms`, `elapsed_ms`), `shards closed`,
`handing back extra shards`, `fenced dead node's log` (log_id, fence_ordinal),
`peer missed a renewal and refuses connections: presumed dead`,
`node lease renew error (will retry)`, `control-plane <op> timed out after`,
`waited for our clock to pass the previous owner's last seq`,
`previous owner's clock is more than 30s ahead of ours: serving anyway` (error),
`tokio runtime stall` (late_ms), `log retention pass failed`,
`reshard step failed (retried next step)`, `fencing our log on shutdown failed`.

---

## Alerts

### VlpdsNodeDown

**Means:** Prometheus can't scrape a node for 2 minutes. If the process is gone,
peers took its shards within 3-5 s (refused probe) or ~12 s (TTL + skew) plus
replay. If the host is up but frozen/partitioned, its socket still exists and
takeover waits the full TTL + skew; the frozen node's own watchdog fail-stops it
2 x skew after its validity lapses.

**Causes:** crash or fail-stop without a restart, OOM kill, host loss, network
partition between Prometheus and the node, or `--metrics-listen` misconfigured.

**Confirm:** `getClusterStatus` from another node: is the node in `nodes[]`,
`reachable`? `VlpdsShardsUnowned` firing? Host/supervisor status and the last
log lines (exit code table above). A bench-scrape config lists ~30 ports that are
normally down: check this is a real node.

**Do:** If `VlpdsShardsUnowned` isn't firing, data-wise nothing is urgent; the
cluster is running with less capacity. Restart the process (same `--node-id`) or
follow [Replacing a dead host](#replacing-a-dead-host). If several nodes are down at
once, look for an object-store outage first ([procedure](#object-store-outage)).

### VlpdsNodeRestarted

**Means:** `vlpds_process_start_time_seconds` changed: a new process. Expected
during a deploy. Otherwise a fail-stop, crash or OOM kill.

**Confirm:** `vlpds_last_exit_reason_info` on the node (reason and code: table
above; `crash` = no exit recorded), then the error line just before the exit.
`vlpds_build_info{rev}` changed? (deploy). Kernel/cgroup OOM logs. Exit 5 with
`renew error` warnings before it: store latency (see
[VlpdsLeaseRenewalNearCeiling](#vlpdsleaserenewalnearceiling)). Exit 3: a peer
presumed this node dead and fenced it (it was frozen, partitioned, or its
renewals were slow). Exit 3 right after another process started with the same
`--node-id`: see [What NOT to do](#what-not-to-do).

**Do:** nothing if it rejoined (it owns ~fair share again within a step or two)
and the cause is understood. Investigate exit 2/4 (store errors / SlateDB apply)
before they repeat.

### VlpdsNodeFailStopped

**Means:** the node restarted in the last 30 minutes and its previous process
ended with a fail-stop (`reason` and `code` labels, see the exit code table) or
`crash` (no exit recorded: SIGKILL, OOM kill, abort, host loss). `error` = the
previous process failed to start or serve (exit 1). Needs the exit-state file
(`--exit-state-file`, or `--cache-dir`) on a disk that survives restarts.

**Confirm / Do:** as [VlpdsNodeRestarted](#vlpdsnoderestarted) for that reason.
`crash`: kernel/cgroup OOM logs, supervisor logs.

### VlpdsUncleanNodeExit

**Means:** some node fenced the log of an incarnation that ended without
fencing it itself. `reason="peer"`: a survivor took over a dead peer's shards
(crash, kill -9, OOM, fail-stop, partition, frozen past TTL + skew).
`reason="restart"`: a node fenced its own previous incarnation at startup (it
came back before a peer took over). Graceful stops never count. This is
counted on a live node, so it reports deaths of nodes that never come back.

**Confirm:** `fenced dead node's log` (log_id) in the fencer's logs; the dead
incarnation's `vlpds_last_exit_reason_info` once it restarts.

**Do:** as [VlpdsNodeRestarted](#vlpdsnoderestarted).

### VlpdsNodeCrashLooping

**Means:** 3+ restarts in an hour. Each restart moves its shards out and back
(ownership churn, cold repo loads, write resends).

**Causes:** persistent store errors (exit 2/4), renewals regularly over 4 s
(exit 5), two processes with the same `--node-id` fencing each other (exit 3
alternating) **(the ping-pong is inferred from `join` fencing the previous
incarnation's log)**, OOM (see [VlpdsMemoryCritical](#vlpdsmemorycritical)), a
bad binary.

**Do:** stop the node (SIGTERM) and leave it down while you diagnose; its shards
move to peers. Roll back the binary if the loop began with a deploy. Never
"fix" a crash loop by deleting objects.

### VlpdsPeerPresumedDead

**Means:** a node saw a peer miss a renewal **and** its address refuse TCP: the
peer process is gone; takeover starts immediately (fence, CAS, replay).

**Confirm:** `peer missed a renewal and refuses connections` and `fenced dead
node's log` in the observer's logs; the dead node's exit code.

**Do:** treat like [VlpdsNodeRestarted](#vlpdsnoderestarted) for the dead peer.

### VlpdsMixedVersions

**Means:** more than one `rev` in `vlpds_build_info` for over an hour. Normal
for the minutes of a rolling deploy.

**Do:** finish or roll back the deploy. **(unverified)** Mixed-version clusters are
not tested beyond a deploy window; segment format changes (e.g. `VLSEG05`) need
every reader upgraded before a writer emits the new format.

### VlpdsShardsUnowned

**Means:** fewer shards are owned by scraped nodes than the layout has
(`vlpds:layout_shards`: the nodes' `vlpds_shard_layout_shards`, or the last
value seen in the past hour when none is up) for 2 minutes. Requests for repos in
those shards fail or wait for the 20 s resend window to run out. Normal takeover
is seconds; 2 minutes is not.

**Causes:**
- A frozen (not dead) node: its socket accepts, so takeover waits TTL + skew
  (12 s), then fence + replay. Long replay (see
  [VlpdsTakeoverReplaySlow](#vlpdstakeoverreplayslow)) stretches it.
- Survivors can't take shards: control-plane calls timing out
  ([VlpdsControlPlaneTimeouts](#vlpdscontrolplanetimeouts)), fence or assignment
  CAS failing, SlateDB open failing ([VlpdsShardOpenErrors](#vlpdsshardopenerrors)).
- Commit-wait: the previous owner's clock was ahead; a new owner waits up to 30 s
  (`waited for our clock to pass`).
- A merge just lowered the count and a node that was down during it reports its
  old layout from the past hour (only while no node is up).
- A node is up but not scraped (its gauge is missing, not its ownership).

**Confirm:** `getClusterStatus`: `table` entries that are `null`, `layout.op`,
each node's `owned` and `leaseValid`. Logs on survivors: `acquired shards`,
`shards opened` (`segments_replayed`, `replayed_ms`), control-plane timeouts.
`vlpds_shard_open_seconds{kind="replay"}` and `vlpds_shards_opened_total{result}`
on survivors.

**Do:** fix the blocker (store, frozen host: kill the frozen process so the refused
probe kicks in). Do **not** edit `assign/` objects. If all nodes are down, start
them; each takes its share at its first steps.

### VlpdsShardsOverOwned

**Means:** the nodes' `vlpds_owned_partitions` sum to more than the layout's
shard count (`vlpds:layout_shards`) for 5 minutes.

**Causes:** nodes disagreeing on the layout for that long (a split/merge stuck
mid-flip: `layout.op`, `vlpds_shard_layout_version` per node). Otherwise a
zombie: a node that still believes it owns shards it lost. Safety holds (its next
segment PUT collides with the fence and it exits 3; a step that sees a reassigned
shard exits 5), but a zombie whose monotonic clock was paused (VM suspend) serves
stale reads until then.

**Confirm:** compare `owned` lists across nodes in `getClusterStatus`; the same
shard on two nodes identifies the zombie. `vlpds admin layout` for the count.

**Do:** layout disagreement -> see [Shard split / merge](#shard-split--merge).
Zombie -> SIGKILL it (it has nothing it may ack; a successor already fenced its
log).

### VlpdsOwnershipFlapping

**Means:** more than 4 shard opens per shard in an hour. Every move costs a close
barrier + checkpoint, a fence or handoff, a SlateDB open, replay, and cold repo
loads on the new owner.

**Causes:** nodes restarting repeatedly (see [VlpdsNodeCrashLooping](#vlpdsnodecrashlooping));
a node whose renewals keep lapsing (slow store, CPU starvation: see
[VlpdsRuntimeStalls](#vlpdsruntimestalls)); repeated deploys; reshard activity
(`vlpds_reshard_events_total`).

**Confirm:** `sum by (instance) (increase(vlpds_lease_events_total[1h]))` by
`event`; which node's `opened`/`closed` dominate; restarts.

**Do:** stabilize the flapping node (stop it if needed). Don't lower the TTL to
"speed up" recovery: it shrinks the renewal ceiling and causes more fail-stops.

### VlpdsOwnershipImbalanced

**Means:** one node holds over 1.5x the fair share for 30 minutes.

**Causes:** a joiner hasn't been greeted/settled (handback only goes to peers seen
for the join grace or that confirmed the hello), the over-full node's releases
fail, or nodes are `draining`. A reshard in progress keeps parents in place.

**Confirm:** `handing back extra shards` log lines on the full node; joiner's
`nodes[]` entry `reachable`/`leaseValid`; `layout.op`.

**Do:** usually resolves by itself; if not, a graceful restart (SIGTERM) of the
over-full node hands its shards out evenly.

### VlpdsShardOpenErrors

**Means:** shard opens failed on this node (`vlpds_shards_opened_total{result="error"}`):
the SlateDB open, the replay of previous owners' log spans, or the post-replay
flush. A failed shard is released (nothing was logged for it) and retried by
the next step, here or elsewhere.

**Confirm:** `open failed: ...; releasing` and `replay failed` log lines (shard,
error); [VlpdsObjectStoreRequestErrors](#vlpdsobjectstorerequesterrors) for
`log_segment` reads; `VlpdsObjectStoreErrors` (SlateDB). Replay treats a hole
inside a span as an error: someone deleted log objects by hand?

**Do:** fix the store problem. Do not edit `assign/` or `log/` to "unstick" a
shard.

### VlpdsTakeoverReplaySlow

**Means:** a batch of shard opens that replayed a dead owner's log tail took over
20 s before serving (`vlpds_shard_open_seconds{kind="replay"}`; the replay step
alone is `vlpds_recovery_replay_seconds`, its size
`vlpds_recovery_replayed_segments_total`). Those shards were unavailable for
that long on top of the takeover delay.

**Causes:** the dead node hadn't checkpointed for a while
([VlpdsReplayBacklogHigh](#vlpdsreplaybackloghigh),
[VlpdsCheckpointsStalled](#vlpdscheckpointsstalled)); slow `log_segment` GETs
(`vlpds_object_store_request_seconds{component="log_segment"}`); slow SlateDB
opens (store latency).

**Do:** fix checkpointing on the nodes; investigate store latency.

### VlpdsLeaseRenewalNearCeiling

**Means:** at least one lease renewal (one CAS PUT of `nodes/{node_id}`) took over
2 s in the last 5 minutes (`vlpds_lease_renew_seconds`). Validity ends TTL - skew
(8 s) after a renewal's *send time* and renewals go out every 2 s, so a round
trip over 0.4 x TTL (4 s) lapses the lease and the node fail-stops (exit 5).
Several nodes at once: a store brownout that will stop the whole cluster past
4 s.

**Confirm:** `vlpds_lease_validity_seconds` (dips below ~6 s), renew errors,
`vlpds_object_store_request_seconds{component="ctl_lease"}` and the other
components on the same node (node-side network vs store), runtime stalls
([VlpdsRuntimeStalls](#vlpdsruntimestalls): a starved runtime delays the renew
task itself, not the store).

**Do:** one node -> its network path to the store, CPU. Many -> the
[Object-store outage](#object-store-outage) procedure. Don't lower the TTL.

### VlpdsLeaseRenewalSlow

**Means:** renewal p99 over 500 ms for 10 minutes (normal: one small PUT,
~25-50 ms). Not dangerous yet; the trend toward the 4 s ceiling is.

**Do:** as [VlpdsLeaseRenewalNearCeiling](#vlpdsleaserenewalnearceiling), without
the urgency.

### VlpdsLeaseRenewErrors

**Means:** renewals failed with a store error (`kind="error"`) or the HTTP
client's timeout (`kind="timeout"`) and will be retried at the next tick (2 s).
Every failed renewal eats into the 8 s validity: four in a row lapse it.
`kind="conflict"` (someone rewrote our lease) and `kind="lapsed"` (validity
ended before a renewal) fail-stop at once and show as restarts.

**Confirm:** `node lease renew error (will retry)` warnings with the error text;
[VlpdsObjectStoreRequestErrors](#vlpdsobjectstorerequesterrors) for `ctl_lease`.

**Do:** credentials, throttling, network, provider status.

### VlpdsLeaseValidityLow

**Means:** a scrape saw this node with under 4 s of lease validity left
(`vlpds_lease_validity_seconds`, normally 6-8 s with a 2 s renew interval): its
renewals were 2+ s overdue, so it came within seconds of a fail-stop. Sampled at
scrape time, so short dips can be missed: the renewal histogram is the complete
record.

**Do:** as [VlpdsLeaseRenewalNearCeiling](#vlpdsleaserenewalnearceiling).

### VlpdsCommitLatencyHigh

**Means:** p99 enqueue -> durable + applied + acked over 500 ms for 10 minutes
(design: ~40-50 ms p50, ~150 ms p99 on S3 Standard).

**Confirm:** break it down with `vlpds_commit_stage_seconds{stage}`:
`seal_wait` (queueing before the PUT: load or too few PUT slots), `put` (object
store, see `vlpds_segment_put_seconds`, hedges), `apply_lock` (shard locks held by
exports/checkpoints), `apply` (SlateDB: L0 stalls, backpressure), `ack`.
`vlpds_runtime_tick_late_seconds` for CPU starvation.

**Do:** store slow -> see [VlpdsSegmentPutLatencyHigh](#vlpdssegmentputlatencyhigh).
CPU -> add nodes or shed load. Apply slow -> [VlpdsSlateDbL0Stalls](#vlpdsslatedbl0stalls).

### VlpdsCommitLatencyCritical

**Means:** p99 over 2 s. Forwarded writes start failing at the 3 s deadline and
the node is within 2x of the 4 s renewal ceiling if the cause is the store.

**Do:** as [VlpdsCommitLatencyHigh](#vlpdscommitlatencyhigh), urgently. If every
node is affected, assume an object-store brownout ([procedure](#object-store-outage)).

### VlpdsCommitLogStalled

**Means:** entries are queued for the sequencer, yet no segment became durable in
2 minutes. Segment PUTs retry until they succeed (fail-stop policy), so a stall
is a store that keeps failing or hanging, or a wedged finalizer.

**Confirm:** `vlpds_segment_puts_inflight`, `vlpds_segment_put_attempts_total{result}`,
`vlpds_segment_put_hedges_total`; logs for PUT errors; `leaseValid` (an invalid
lease stops PUTs, then the node exits 5).

**Do:** store problem -> [object-store outage](#object-store-outage). A node that
is wedged with a healthy store: SIGTERM it (if it doesn't exit within a minute,
SIGKILL; a successor fences its log and replays everything it acked).

### VlpdsWatermarkLagHigh

**Means:** the node's own log watermark (every event <= it is durable) is over 2 s
behind its clock for 5 minutes while it owns shards. An idle log advertises the
clock, so lag means entries are assigned seqs but not durable.

**Do:** as [VlpdsCommitLatencyHigh](#vlpdscommitlatencyhigh). Every node's firehose
waits for this log, so expect [VlpdsFirehoseEmitDelayHigh](#vlpdsfirehoseemitdelayhigh) too.

### VlpdsSegmentPutLatencyHigh

**Means:** p99 log segment PUT (including hedges/retries) over 250 ms for 10
minutes. Design: ~25 ms; a hedged duplicate PUT starts at 100 ms (`--hedge-after-ms`).

**Causes:** object-store latency (region/AZ issue, throttling: S3 503 SlowDown),
network saturation on the `log` HTTP pool, oversized segments (`vlpds_segment_bytes`).

**Confirm:** `vlpds_object_store_bytes_total{client="log",dir="up"}` rate vs NIC,
`vlpds_segment_stall_seals_total`, SlateDB's latency on the same store
(`slatedb_object_store_request_duration_seconds`) to tell store vs. node.

**Do:** store-side: provider status, request rate per prefix. Node-side: network.
Do not raise `--max-segment-mb` to compensate (bigger PUTs are slower; DESIGN
"Pipelined segment PUTs").

### VlpdsSegmentPutErrors

**Means:** segment PUT attempts are failing (not `already_exists`, which is a lost
hedge race verified by content). They are retried; acks wait. Unrecoverable
failure of the upload task exits 2.

**Confirm:** node logs for the store error text; [VlpdsObjectStoreErrors](#vlpdsobjectstoreerrors)
on the same node (credentials, bucket policy, throttling).

**Do:** fix credentials/permissions/quotas. A `segment PUT conflicted but no object
is there; retrying` warning is handled by the code.

### VlpdsWritesShed

**Means:** writes rejected with 503 `Overloaded` because more than
`--max-inflight-writes` (20,000) are in flight. This is admission control working:
without it a latency blip snowballs into connection storms.

**Do:** find why writes are slow (commit latency, cold loads) or add capacity.
Raising the limit only helps if the node has headroom.

### VlpdsWriteInternalErrors

**Means:** writes failing with `internal` or `unavailable` (other kinds such as
`invalid_swap`, `invalid`, `repo_not_found` are client errors).

**Confirm:** node logs around the errors; `vlpds_repo_loads_total{result="error"}`;
store errors.

**Do:** follow the underlying cause; `unavailable` usually tracks shard moves or
loads (see [VlpdsWriteResendsSustained](#vlpdswriteresendssustained)).

### VlpdsHttp5xxHigh

**Means:** over 5% of locally served XRPC requests return 5xx (appview-proxied
calls excluded). Every 503 vlpds sends carries `Retry-After: 1`.

**Confirm:** `sum by (method, status) (rate(vlpds_http_requests_total{status=~"5.."}[5m]))`
-> which methods. 503 on repo writes: shard moves, `Overloaded`,
`PartitionUnavailable`. 500: internal errors in logs.

**Do:** follow the matching alert (ownership, commit latency, forwards, store).

### VlpdsForwardErrorsHigh

**Means:** over 5% of forwards to shard owners return 5xx, which includes an
unreachable owner or one past its 3 s TTFB deadline (presumed frozen; the client
gets 503 `PartitionUnavailable`).

**Causes:** owner node frozen/overloaded, network between nodes, an owner that
just died (until takeover), `--advertise-url` not reachable from peers.

**Confirm:** `vlpds_forward_seconds` p99; target owner's commit latency and
runtime stalls; `vlpds_http_client_connects_total{role="peer"}` climbing (should
stay flat under steady load).

**Do:** fix the owner; a frozen owner should lose its shards after TTL + skew.

### VlpdsForwardLatencyHigh

**Means:** p99 forward to the owner's response head over 1 s (deadline 3 s).

**Do:** look at the owners' latency (commit, cold loads `vlpds_repo_load_seconds`,
runtime) and the peer network.

### VlpdsWriteResendsSustained

**Means:** an entry node keeps resending writes answered `RepoLoading`
(`reason="loading"`: the owner's worker didn't start them within 1 s, usually a cold
repo load), `ShardMoved` (`moved`) or unreachable owners. Bursts for seconds after a
restart/handoff are by design; 10 minutes is not.

**Confirm:** `vlpds_writes_abandoned_total` on the owners, `vlpds_repos_loading`,
`vlpds_repo_load_seconds` p99, `vlpds_lease_events_total` (moves).

**Do:** `loading`: cold loads too slow (store latency, repo cache too small: see
[VlpdsRepoCacheMissRateHigh](#vlpdsrepocachemissratehigh)). `moved`: ownership
flapping. `unreachable`: an owner is down but still assigned.

### VlpdsFirehoseEmitDelayHigh

**Means:** p99 from seq assignment of a batch's oldest event to its emit over 2 s.
The merger emits only at the minimum watermark over every node log, so one slow
log, a slow peer stream, or a large wall-clock offset between nodes delays the
firehose on every node.

**Confirm:** `getClusterStatus` `firehose.sources[]`: the log whose `watermark`
trails (seqs are `unix_micros x 256 + writer`; divide by 256 for micros). Which
node owns that log (`nodes[].log`)? Its commit latency/watermark lag. Clock sync
on all hosts (NTP/chrony offset). `vlpds_firehose_merge_queue_bytes`.

**Do:** fix the slow node; fix clock sync.

### VlpdsFirehoseEmitDelayCritical

**Means:** p99 over 20 s: relays/consumers see minutes-old data soon.

**Do:** as above. If one node's log is the laggard and the node is unhealthy,
SIGTERM it: a graceful stop fences its own log so followers drain it and drop it
as a source.

### VlpdsFirehoseStalled

**Means:** the cluster commits but this node's merger emitted nothing for 5
minutes: the merge is held at some log's watermark.

**Causes:** a dead log that nobody fenced (followers drain a log only up to a
fence; DESIGN notes an unfenced log stalls every peer's firehose), a peer stream
stuck and S3 catch-up failing, a node whose clock is far behind (its idle
watermark advertises its clock).

**Confirm:** `firehose.minWatermark` and `sources[]` in `getClusterStatus`; the
stuck log's lease (`nodes[]`) and `fencedLogs`.

**Do:** a dead node's log is fenced by the node that takes over its shards; if
none were left to take, **(unverified)** restarting the dead node with the same
`--node-id` fences its previous log at join. Fix clocks. Restart the stalled node
as a last resort.

### VlpdsFirehoseConsumersTooSlow

**Means:** subscribers more than `--firehose-max-lag-mb` (128 MiB) behind are cut
off with `ConsumerTooSlow` and resume from their cursor. Isolated cases are the
consumer's problem; a high rate across subscribers points at the server.

**Confirm:** `vlpds_firehose_subscribers`, `vlpds_firehose_bytes_sent_total` rate vs
NIC, `vlpds_runtime_tick_late_seconds`, `--firehose-threads` (default 4) CPU.

**Do:** server-side: network or firehose threads. Consumer-side: nothing.

### VlpdsFirehoseMergeSpilling

**Means:** a log exceeded the merger's queue budget (`--firehose-merge-queue-mb`,
256 MiB) while waiting for the minimum watermark; the merger now reads it back
from S3 in chunks (`vlpds_firehose_merge_spill_segments_total`, extra GETs).

**Do:** find the laggard log as in [VlpdsFirehoseEmitDelayHigh](#vlpdsfirehoseemitdelayhigh).

### VlpdsPeerLogStreamLagging

**Means:** a peer following this node's log fell behind the 128 MiB live ring
(`--live-ring-mb`) and was dropped; it catches up from S3 segments.

**Do:** look at the follower node (CPU, network). Frequent drops raise S3 GETs and
firehose delay.

### VlpdsControlPlaneTimeouts

**Means:** control-plane object-store calls (`get`, `put`, `list`, `delete`,
`fence`, `fence-scan`) were abandoned at `min(TTL, 5 s)`; the step retries next
tick. Lease **renewals** are separate: they are never timed out and not counted
here (their own metrics: `vlpds_lease_renew_seconds`, `vlpds_lease_renew_errors_total`).
But a store that takes 5 s for control-plane calls is past the 4 s renewal
ceiling, so lease lapses (exit 5) are likely next.

**Confirm:** logs `control-plane <op> timed out`, `node lease renew error`;
`vlpds_object_store_request_seconds{component=~"ctl_.*"}` and SlateDB latency on
the same node; `vlpds_object_store_requests_total{result="cancelled"}` (the
abandoned calls); one node or many (see
[VlpdsObjectStoreBrownout](#vlpdsobjectstorebrownout)).

**Do:** single node -> its network path to the store. Many -> provider status.

### VlpdsObjectStoreBrownout

**Means:** two or more nodes timing out control-plane calls in the same 5
minutes. DESIGN: a cluster-wide brownout past the 4 s renewal ceiling stops every
node.

**Do:** [Object-store outage](#object-store-outage) procedure.

### VlpdsObjectStoreRequestErrors

**Means:** over 1/s of this node's own object-store requests failed (`result`
`error` or `timeout`) on one key component, for 5 minutes. Counted at the
bottom of vlpds' store clients (`src/objstats.rs`), so it covers SlateDB and
everything SlateDB's metrics don't: control plane (`ctl_lease`, `ctl_assign`,
`ctl_writer`), `log_segment` (segment PUTs, fences, replay, firehose backfill
and follower catch-up), `retention_report`, `account_index`, `blob`.
`not_found` and `precondition` (a lost CAS / create race) are normal answers;
`cancelled` is a caller that gave up (control-plane deadline, a lost hedge).

**Confirm:** `sum by (component, op, result) (rate(vlpds_object_store_requests_total{instance="..."}[5m]))`;
the matching warn/error log lines.

**Do:** credentials, permissions, throttling (S3 503 SlowDown), provider status.

### VlpdsControlPlaneLatencyHigh

**Means:** p99 of control-plane object-store requests (`ctl_*` components:
leases, assignments, writer claims) over 1 s for 10 minutes; normally tens of
ms. Every takeover step is a few of these in sequence, and the lease renewal is
one: past 4 s nodes fail-stop.

**Confirm:** `vlpds_object_store_request_seconds` by `component` and `op` on the
node; [VlpdsLeaseRenewalSlow](#vlpdsleaserenewalslow); the same on other nodes.

**Do:** as [VlpdsControlPlaneTimeouts](#vlpdscontrolplanetimeouts).

### VlpdsObjectStoreErrors

**Means:** SlateDB (shard state: memtable flushes, manifests, SST reads, GC,
compaction) sees object-store errors over 1/s. SlateDB retries; persistent apply
failure exits 4.

**Confirm:** `sum by (component, op, api) (rate(slatedb_object_store_error_count_total[5m]))`;
logs. Errors only from `gc`/`compactor` don't affect acks directly but let L0 grow.

**Do:** credentials, permissions, throttling, provider status.

### VlpdsObjectStoreLatencyHigh

**Means:** SlateDB's store p99 over 1 s. Hurts cold repo loads, checkpoints
(~37 ms each normally) and apply.

**Do:** as [VlpdsSegmentPutLatencyHigh](#vlpdssegmentputlatencyhigh). Check the
local SST disk cache (`--cache-dir`) is set and healthy (hit rates in
`slatedb_db_cache_access_count_total`).

### VlpdsSlateDbL0Stalls

**Means:** SlateDB blocked writes because a shard has too many L0 SSTs:
compaction is behind. Segment apply (and so acks) waits.

**Confirm:** `slatedb_db_l0_sst_count`, `vlpds_compaction_poll_switches_total`,
compactor errors in `slatedb_object_store_error_count_total{component="compactor"}`,
`vlpds_commit_stage_seconds{stage="apply"}`.

**Do:** fix store errors/latency; check CPU for the compactor. **(unverified)**
`--compaction-polling` is adaptive by default; switching to fast polling may help.

### VlpdsCheckpointsStalled

**Means:** the node writes segments but checkpointed no shard for 10 minutes.
Checkpoints (applied marker + memtable flush, one shard every `interval/shards`)
bound a successor's replay and let retention delete the log.

**Confirm:** logs for flush/write errors; `vlpds_checkpoint_shard_seconds`;
`vlpds_retention_replay_hold_segments` growing.

**Do:** fix store errors. A node that cannot checkpoint will cost a long replay
on takeover; consider a graceful restart once the store is healthy (a graceful
close checkpoints every shard, so the successor replays nothing).

### VlpdsReplayBacklogHigh

**Means:** this node keeps over ~15 minutes of its log only because a crash
replay could need it (`vlpds_retention_replay_hold_segments`, divided by the
segment rate). A takeover would replay about that much before serving those
shards. Past takeovers' replay: `vlpds_recovery_replayed_segments_total`,
`vlpds_recovery_replay_seconds`, `vlpds_shard_open_seconds{kind="replay"}` on the
nodes that took over.

**Do:** as [VlpdsCheckpointsStalled](#vlpdscheckpointsstalled).

### VlpdsRetentionFailing

**Means:** most retention passes in the last hour failed (`log retention pass
failed`). Logs stop shrinking: storage and LIST cost grow; nothing is lost.

**Do:** read the error; usually store permissions (DELETE) or throttling. Never
delete log objects by hand to compensate.

### VlpdsRetentionNotRunning

**Means:** no pass (ok or error) for 15 minutes on a scraped node. A pass runs
every 60 s; a pass that hangs on a store call would look like this
**(unverified: retention calls are not individually timed out)**.
`vlpds_retention_pass_seconds` shows how long the finished passes took (a
creeping p99 precedes a hang), and `vlpds_object_store_requests_total{result="cancelled"}`
/ request latency for `log_segment` and `retention_report` the store side.

**Do:** check logs; restart the node gracefully if the task is wedged.

### VlpdsDeadLogUnfenced

**Means:** the dead-log pruner (the owner of slot 0's shard) has seen a log with
no live writer that nobody fenced, for 30 minutes
(`vlpds_retention_dead_logs{state="unfenced"}`). Followers drain a log only up to
its fence, so an unfenced dead log holds every node's merged firehose at its
watermark, and retention never prunes it
(`vlpds_retention_dead_log_segments` counts what dead logs still hold).

**Causes:** its node died owning no shards (nobody takes over, so nobody fences);
its shards' takeover keeps failing ([VlpdsShardOpenErrors](#vlpdsshardopenerrors),
control-plane errors).

**Confirm:** `fencedLogs` and `firehose.sources[]` in `getClusterStatus`; the
log id in `log/` (`<node-id>.<micros>`) names the node.

**Do:** restart that node id (startup fences its previous incarnation's log) or
fix the failing takeover. Never write a fence object by hand.

### VlpdsMemoryHigh

**Means:** RSS over 85% of `vlpds_memory_limit_bytes` (physical RAM, or the
cgroup limit when lower, as the node reads it) for 10 minutes. A node that can't
read either exports no limit and these alerts stay silent.

**Confirm:** `vlpds_jemalloc_bytes{stat}` (allocated vs resident vs retained),
`sum by (instance) (vlpds_repo_cache_bytes)` (loaded MST paths) vs `vlpds_repo_cache_capacity_bytes`
(`--repo-cache-mb`, 4 GiB default), `mst_store` node cache (`--lazy-mst-node-cache-mb`, 256 MiB), `vlpds_cache_bytes`
(`--cache-budget-mb`, default 10% of RAM/cgroup), SlateDB `--block-cache-mb`
(4 GiB), `vlpds_firehose_ring_bytes`, `vlpds_log_live_ring_bytes`,
`vlpds_firehose_merge_queue_bytes`, `slatedb_db_total_mem_size_bytes`.

**Do:** lower the budgets that dominate, or move to a bigger box.

### VlpdsMemoryCritical

**Means:** RSS over 95%: an OOM kill is close. An OOM kill is a crash: peers take
over in 3-5 s (refused probe) and replay; nothing acked is lost.

**Do:** a graceful restart (SIGTERM) now is cheaper than the OOM (handoff without
replay). Then fix as in [VlpdsMemoryHigh](#vlpdsmemoryhigh).

### VlpdsRepoCacheMissRateHigh

**Means:** over 20% of repo lookups for queued requests start a cold load
(head + account reads, `M/` prefetch, MST root and paths) for 30 minutes.

**Confirm:** `vlpds_repo_evictions_total` rate, `vlpds_repo_cache_bytes` near
`--repo-cache-mb`/workers, `vlpds_cached_repos` near `--cache-per-worker`
(50,000), ownership churn (each move empties the cache for those shards),
`vlpds_lazy_mst_fallbacks_total` (opens rebuilt from all records: slow).

**Do:** raise `--repo-cache-mb` / `--cache-per-worker` if memory allows; stop churn.

### VlpdsRepoLoadErrors

**Means:** cold repo loads fail (`result="error"`; `not_found` and `stale` are not
errors). Requests for those repos fail.

**Do:** logs for the repo/shard and error; store errors on the node.

### VlpdsLazyMstInvalid

**Means:** a repo's persisted MST node (or rebuilt subtree) did not
match its link; the repo was rebuilt from its records (correct result, slower).
It indicates inconsistent derived state and should not happen.

**Do:** capture the log lines (repo DID, shard) and file a bug. No operator action
on data.

### VlpdsKeyServiceUnavailable

**Means:** calls to the KEK's key service (Cloud KMS) fail or time out
(`vlpds_kms_requests_total{result="unavailable"}`; the `key service unavailable`
warn log names the key and the error). Accounts whose signing key is cached keep
writing. Cold accounts (first write since the node started or took the shard)
get **503 `KeyUnavailable`**, and nothing is written. createAccount, reserveSigningKey,
setupTotp and TOTP logins fail with 503. Reads, exports, the firehose and the
proxy for warm accounts are unaffected.

**Do:** follow [Key service (KMS) outage](#key-service-kms-outage).

### VlpdsSecretUnwrapRejected

**Means:** a wrapped secret failed authentication under its KEK: a KEK file
with the right id but other bytes, a Cloud KMS key that rejects the ciphertext,
or a corrupt row. That account's writes fail with 500. A blob under a KEK the
node doesn't have at all fails the same way but only logs (`wrapped under unknown
key-encryption key L…/G…`): usually an old KEK retired before the rewrap finished.

**Do:** find the DID in the `secret unwrap failed` / `repo load failed` logs. If
the kid is unknown, add the old KEK back (`--kek-old-file` / `--gcp-kms-old-key`)
on every node and rerun the rewrap ([KEK rotation](#kek-rotation)). Otherwise
compare the node's KEK config with its peers'. Never "fix" a row by hand.

### VlpdsSignatureFault

Also covers **VlpdsSignatureFaultFailStop** (the previous process exited
with code 6, `signature_fault`).

**Means:** a signature failed verification against the key's own public key
right after it was made (`vlpds_signature_verify_failures_total{purpose}`:
`commit`, `service_auth`, `oauth_token`; `key_load` = a cached signing key's
scalar no longer derives its public key). Correct code never does this: the
CPU or memory of this host computed something wrong (bad DIMM, Rowhammer,
overheating, failing CPU). The bad signature was **not** emitted: the node
signed once more with a fresh nonce, and if that failed too the write got a
503 `SignatureFault` with nothing applied. Nonces are hedged, so a faulty
signature leaks nothing even if one had got out, but the host is no longer
trustworthy. Three failures within a minute fail-stop the node (exit 6);
the supervisor restarts it on the same host, so expect it to recur.

**Confirm:** `signature failed verification against the signing key's public
key` error logs (`purpose`, `recent`); `vlpds_last_exit_reason_info{reason="signature_fault"}`.
On the host: `journalctl -k | grep -iE 'mce|edac|machine check|hardware error'`,
`edac-util -v` (or `/sys/devices/system/edac/mc/mc*/ce_count`/`ue_count`),
`rasdaemon`/`ras-mc-ctl --summary` if installed, CPU temperatures, and the
cloud provider's host-maintenance or hardware-degradation events.

**Do:**
1. Drain the host now, even after a single fault: stop vlpds there
   gracefully (`SIGTERM`; peers take its shards over) and keep the
   supervisor from restarting it on that host.
2. Check ECC/EDAC and machine-check logs as above. Corrected-error counts
   that climb, any uncorrected error, or MCEs = hardware fault.
3. Replace the host (cloud: stop/start onto new hardware, or recreate the
   VM; bare metal: pull it and run memtest86+ / the vendor diagnostics)
   before it serves again. Don't return it on the strength of a clean
   restart.
4. Nothing to repair in data: no faulty signature was sequenced, and the
   failed writes were refused with a retryable 503.
5. Faults on several hosts at once point at a software or build problem
   rather than hardware: compare the vlpds versions (VlpdsMixedVersions)
   and escalate to development.

### VlpdsCacheAtCapacity

**Means:** a bounded in-memory cache (`session_tokens`, `oauth_tokens`,
`proxy_accounts`, `proxy_jwts`, `did_docs`, `lexicons`, `oauth_clients`,
`permission_sets`, `security_controls`) has been at its entry cap for 6 hours. A
full LRU is normal for hot caches; it matters only with symptoms (proxy latency,
PLC lookups). `vlpds_proxy_cache_total{result}` gives a hit ratio for the proxy
fast path; the others have no hit/miss metric.

**Do:** raise `--cache-budget-mb` or set `--cache-entries <cache>=<n>` if a
symptom correlates.

### VlpdsFirehoseMergeQueueNearBudget

**Means:** the merger holds over 80% of its 256 MiB budget waiting for the minimum
watermark. Past the budget it spills (see
[VlpdsFirehoseMergeSpilling](#vlpdsfirehosemergespilling)).

**Do:** find the laggard log (same as emit delay).

### VlpdsRuntimeStalls

**Means:** the 10 ms ticker on the tokio runtime was late over 5% of the time:
runtime threads are blocked or starved. Lease renewals, step loops and acks run on
it, so heavy stalls risk lease lapses.

**Confirm:** `tokio runtime stall` logs (late_ms), host CPU/load, a CPU profile
(`just profile`).

**Do:** reduce co-located load; add CPU; profile for blocking work on the runtime.

### VlpdsRuntimeSaturated

**Means:** tokio workers over 90% busy for 15 minutes: the node is CPU bound
(DESIGN: ~12k commits/s per core including HTTP).

**Do:** add nodes (shards rebalance automatically) or bigger instances.

---

## Procedures

### KEK provisioning

Repo signing keys, reserved keys and TOTP secrets are stored wrapped under a
key-encryption key (DESIGN "Secrets at rest"). Outside `--dev-mode` a node
refuses to start without one. Every node of a cluster needs the same KEK set.

- **Cloud KMS (production on GCS).** Create a symmetric ENCRYPT_DECRYPT key in a
  multi-region or dual-region location, e.g.
  `gcloud kms keyrings create vlpds --location us` and
  `gcloud kms keys create secrets --keyring vlpds --location us --purpose encryption`.
  Grant the nodes' service account `roles/cloudkms.cryptoKeyEncrypterDecrypter`
  on that key only. Nobody routinely gets `cloudkms.cryptoKeyVersions.destroy`.
  Set the destroy-scheduled duration to its maximum. Start nodes with
  `--gcp-kms-key projects/P/locations/us/keyRings/vlpds/cryptoKeys/secrets`
  (`VLPDS_GCP_KMS_KEY`). Tokens come from the metadata server
  (`GCE_METADATA_HOST` overrides it). Losing this key loses every account's
  signing key (each would need a PLC rotation), so it is part of the backup plan.
- **Local KEK.** `openssl rand -out kek.bin 32` (raw 32 bytes; 64 hex chars or
  base64 also work). Distribute it like the other secrets (sops / Ansible Vault),
  mode 0400. Pass `--kek-file /path/kek.bin` (`VLPDS_KEK_FILE`) or the value in
  `VLPDS_KEK`. Back it up offline: it is the only way to read the stored keys.
- Check after start: the `secrets at rest` log line prints `kek=` (the current
  key id: `L…` local, `G…` Cloud KMS) and `unwrap_keks=`. It must match on
  every node. `vlpds_kms_requests_total` shows wraps (account creation) and
  unwraps (cold loads).
- The node caches unwrapped signing keys (`signing_keys` cache, sized from
  `--cache-budget-mb`; `--cache-entries signing_keys=N`). Cloud KMS calls are
  limited to `--kms-concurrency` (64) in flight per node, 5 s each.

### KEK rotation

New wraps use the current KEK. Old blobs keep working as long as their KEK is
configured for unwrap.

1. Roll every node with the new KEK as current and the old one as unwrap-only:
   - local: `--kek-file new.bin --kek-old-file old.bin`;
   - Cloud KMS, new key version in the same CryptoKey: nothing to configure
     (`gcloud kms keys versions create` + set primary). KMS keeps decrypting
     old versions;
   - Cloud KMS, another CryptoKey (or local -> KMS): `--gcp-kms-key NEW
     --gcp-kms-old-key OLD` (or `--gcp-kms-key NEW --kek-file old.bin`; with
     `--gcp-kms-key` set, the local KEK is unwrap-only).
2. On **every** node (each covers the shards it owns):
   `curl -XPOST -u admin:$ADMIN -H 'content-type: application/json' -d '{}' $NODE/xrpc/vlpds.admin.rewrapSecrets`.
   For a version rotation inside one CryptoKey, pass `{"checkVersions": true}`
   (one KMS decrypt per secret). It reports `stale` (rewrapped), `failed` and
   the first errors. Rewrapping doesn't change keys, emits no events and doesn't
   evict repos. Re-run until `failed` is 0.
3. Verify on every node with `{"dryRun": true}` (plus `checkVersions` as above)
   until `stale` is 0. Shards that moved during step 2 show up here: rerun step 2.
4. Only then drop the old KEK (`--kek-old-file`), or disable the old KMS
   version. **Keep the old KEK material (or keep the version disabled, not
   destroyed) for the backup retention period**: backups and log segments still
   hold blobs wrapped under it.

### Key service (KMS) outage

1. Confirm: `vlpds_kms_requests_total{result="unavailable"}` on all nodes, the
   `key service unavailable` log (HTTP status or timeout), Google Cloud status,
   and IAM (a 403 from a removed role looks the same). Clients retry 503s; after
   a failure a node fails cold unwraps fast for 1 s before it tries KMS again.
2. **Don't restart nodes and don't move shards** (no rolling deploys, splits or
   handbacks) while KMS is down. A restart or takeover empties the key cache,
   which turns warm accounts cold: every account the node owns becomes
   unwritable until KMS is back.
3. If one node is affected (network or metadata-server problem), drain it with
   SIGTERM. Its shards move to nodes that can reach KMS.
4. When KMS is back, cold writes succeed on their next retry. Nothing needs
   replaying: refused writes were never applied.
5. If KMS is lost for good (key destroyed), restore the key from a backup if one
   exists. Otherwise, start nodes with a new KEK: accounts can no longer sign, and
   each needs a new signing key installed (`admin.updateAccountSigningKey`) and a
   PLC rotation, which needs the PLC rotation keys the users or their recovery
   flow hold.

### Rolling deploy

1. One node at a time. Send **SIGTERM** (never SIGKILL). A graceful stop
   (`Cluster::shutdown`): marks its lease `draining` so peers stop counting it,
   closes its shards with one barrier segment + checkpoint and hands them straight
   to settled peers (their nudges skip the control-plane read), waits for its log
   to quiesce (up to 10 s), **fences its own log**, deletes its lease, nudges peers,
   keeps answering 500 ms more, and exits 0.
2. Give the supervisor a stop timeout well above the worst case: the close barrier
   may wait 30 s and the quiesce 10 s, so **at least 60 s (derived estimate)**. A
   stop that times out into SIGKILL becomes a crash (fence + replay by peers).
3. Start the new binary with the **same `--node-id`** and the same bucket, prefix,
   tokens and `--advertise-url`. It greets peers; peers hand back its fair share
   at their next step.
4. Before the next node, confirm: `sum(vlpds_owned_partitions)` equals
   `vlpds_shard_layout_shards`; the restarted node's `owned` is about `shards / nodes`;
   its `vlpds_last_exit_reason_info{reason="clean"}`;
   `leaseValid: true` on all `nodes[]`; `vlpds_build_info{rev}` is the new rev;
   commit p99 and `vlpds_write_retries_total` back to baseline.
5. Expect: zero client errors for a clean SIGTERM (3-8 when a forward is in
   flight at exit, per DESIGN), resend bursts, cold-load latency on moved repos.

### Replacing a dead host

1. Nothing must be done for data safety: peers fence the dead log and take its
   shards (3-5 s if the port refuses, ~12 s + replay if the host is unreachable).
2. Make sure the old process can never come back on its own (power it off). If it
   did, it would find its log fenced or its shards reassigned and fail-stop, but
   don't rely on it.
3. Start the replacement. Reusing the old `--node-id` lets it fence and reclaim
   its previous incarnation's assignments right away; a new id works too. The dead
   node's `nodes/` lease object is deleted automatically once all its shards have
   moved and its log is fenced.
4. Point `--cache-dir` at local NVMe; the cache starts cold (expect cold-load
   latency for a while).
5. Verify as in rolling deploy step 4.

### Adding a node

1. Pick a **unique** `--node-id`. Same bucket, prefix, KEK flags, `--jwt-secret`,
   `--admin-token`, `--internal-token`; set `--advertise-url` to an address all
   peers can reach; add it to `--trusted-proxies` lists if used.
2. Start it. After it greets every peer, each peer above the new fair share
   `ceil(shards / live)` hands extras to it at its next step.
3. Add the target to Prometheus (job `vlpds`).
4. Verify `owned` per node converges and `VlpdsOwnershipImbalanced` stays quiet.
5. Writer ids are a single byte (seq low byte), so a cluster can't exceed 256
   live node incarnations **(derived from the seq format)**.

### Object-store outage

What happens, from DESIGN and the code:
- Segment PUTs retry until they succeed; commits queue, acks stop, write latency
  climbs, then admission control sheds (`Overloaded`) and forwards time out.
- Renewals slower than 4 s (at TTL 10 s) open validity gaps: nodes stop acking and
  fail-stop (exit 5). A cluster-wide brownout past that ceiling stops **every**
  node. No acked write is lost: acks require durable segments.
- When the store recovers, restarted nodes rejoin, fence the dead incarnations'
  logs (including their own previous one) and replay.

What to do:
1. Confirm it's the store (provider status, `slatedb_object_store_*`,
   `vlpds_object_store_requests_total{result}` / `vlpds_object_store_request_seconds`,
   `vlpds_lease_renew_seconds` and `vlpds_cluster_store_timeouts_total` on all
   nodes, logs).
2. Make sure the supervisor keeps restarting nodes (with backoff) so they rejoin
   as soon as the store answers.
3. Don't lower `--lease-ttl-ms` (smaller ceiling) and don't delete anything. Raising
   the TTL during an incident is not a supported live operation **(unverified)**.
4. After recovery, watch `VlpdsShardsUnowned`, replay time
   (`vlpds_shard_open_seconds{kind="replay"}`, `shards opened` `replayed_ms`),
   `vlpds_last_exit_reason_info` / `vlpds_peer_takeovers_total` (who fail-stopped),
   firehose emit delay and retention catching up.

### Shard split / merge

`vlpds admin shard-split <shard> [--at <slot>]`, `shard-merge <left> <right>`,
`reshard-abort` (only before the flip), `layout` to watch `op`. Every node's
`vlpds_shard_layout_shards` / `vlpds_shard_layout_version` follow the flip; the
ownership alerts read the count from there.

---

## What NOT to do

- **Never run two processes with the same `--node-id`.** At startup a node fences
  the log of the previous incarnation of its id; the one already running then hits
  the fence on its next segment PUT and exits 3. With a supervisor restarting both,
  they keep fencing each other.
- **Never delete or edit objects by hand** under `log/`, `assign/`, `nodes/`,
  `writers/`, `retain/` or `state/`:
  - `log/`: segments are the WAL; anything a shard hasn't checkpointed lives only
    there. A missing segment is a hole: readers stop there and replay treats a hole
    inside a span as an error.
  - Fence objects in `log/` are what makes a zombie of that incarnation fail-stop
    whatever its clock says; retention deliberately keeps them forever.
  - `assign/` holds each shard's epoch and span history: what successors replay.
    `assign/layout` is the slot map.
  - `nodes/` and `writers/`: deleting a live lease or claim breaks liveness and
    seq uniqueness.
  - Retention deletes old log segments safely; let it.
- **Never run `--lease-ttl-ms` below 10 s in production** (the node warns). The
  renewal ceiling is 0.4 x TTL.
- **Don't SIGKILL for routine restarts.** Use SIGTERM and wait.
- **Don't suspend/snapshot-pause a running node's VM.** A paused monotonic clock
  makes the node think its lease is still valid on wake: it serves stale reads
  until its next PUT hits the fence (it still acks nothing).
- **Don't let host clocks drift.** Offsets don't affect safety but delay the
  merged firehose by the largest offset, and a new owner waits out the previous
  owner's `seq_floor` (up to 30 s) before serving.
- **Don't point two clusters at the same bucket + prefix**, and don't change
  `--shards` expecting it to reshard an existing prefix (it applies when a prefix
  is created; use split/merge).
- **Never retire an old KEK** (`--kek-old-file`, a KMS key version) before
  `vlpds.admin.rewrapSecrets` with `dryRun` reports `stale: 0` on every node, and
  never destroy KEK material that backups still need.
- **Don't shrink `--log-retention`** below what firehose consumers need for
  cursor resume; older cursors get `OutdatedCursor`.

---


## Metric gaps

Signals these alerts would want but that no metric exports today (not invented in
`alerts.yml`):

1. **Per-log firehose watermark lag** (merge input lag per source log) and clock
   offset between nodes; only visible in `getClusterStatus`.
2. **Cluster identity label** on metrics (all alerts assume one cluster per
   Prometheus).
3. **Hit/miss counters** for the in-memory caches other than the proxy fast path.
4. **The specific cause of an exit 5** in `vlpds_last_exit_reason_info`:
   `lease_lost` covers a CAS conflict, a lapse before renewal, the watchdog, a
   reassigned shard, a failed close and an unquiesced log (the log line before the
   exit tells them apart; `vlpds_lease_renew_errors_total{kind="conflict"|"lapsed"}`
   covers two of them, but dies with the process).
5. **Lease validity between scrapes.** `vlpds_lease_validity_seconds` is computed
   at scrape time; dips shorter than the scrape interval are only visible through
   the renewal histogram.

Closed (were gaps when the alerts were first written):

| Gap | Now |
|---|---|
| Lease renewal RTT and failures | `vlpds_lease_renew_seconds` (histogram, 1 ms .. 8 s), `vlpds_lease_renew_errors_total{kind=timeout\|error\|conflict\|lapsed}` |
| Lease validity remaining | `vlpds_lease_validity_seconds{node_id}` (at scrape; negative = lapsed) |
| Fail-stops unscrapeable | `vlpds_last_exit_reason_info{reason,code}` + `vlpds_last_exit_time_seconds` from the exit-state file on the next start; `vlpds_peer_takeovers_total{reason=peer\|restart}` on the fencer ([exit codes](#tools-endpoints-cli-logs-exit-codes)) |
| Process start time | `vlpds_process_start_time_seconds` and `process_start_time_seconds` |
| Replay activity | `vlpds_recovery_replayed_segments_total` (wired), `vlpds_recovery_replay_seconds`, `vlpds_shard_open_seconds{kind=replay\|clean}`, `vlpds_shards_opened_total{result}` |
| Shard count (was the hand-kept `vlpds:expected_shards`) | `vlpds_shard_layout_shards`; `vlpds:layout_shards` derives from it |
| Memory limit (was the hand-kept `vlpds:memory_limit_bytes`) | `vlpds_memory_limit_bytes` |
| Repo cache capacity | `vlpds_repo_cache_capacity_bytes` (all workers) |
| Object-store errors/latency on vlpds' own clients | `vlpds_object_store_requests_total{...,result=ok\|not_found\|precondition\|timeout\|error\|cancelled}`, `vlpds_object_store_request_seconds{op,component}` (every request through `objstats.rs`: control plane, segments, replay, backfill, retention, SlateDB) |
| Retention pass duration, dead-log holdings | `vlpds_retention_pass_seconds`, `vlpds_retention_dead_logs{state=unfenced\|needed\|pruning}`, `vlpds_retention_dead_log_segments` |
