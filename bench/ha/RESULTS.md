# vlpds HA end-to-end results: baseline on the per-partition-lease model

These results come from the **per-partition lease model**: 16 partitions, each with its own S3 lease object and segment log. That model is being replaced (256 shards, one log per node, node leases, and a CAS shard-assignment map). Treat these numbers as the baseline to compare the new design against.

- Run ID: `base1`, plus the smoke runs `smoke1`–`smoke3`. Outputs are in `bench/ha/out/<run>/<scenario>/`.
- Setup: native processes on one Mac (14 cores), with native MinIO on 127.0.0.1:9200.
- Lease TTL: 3 s, so renew and skew margin are both 600 ms.
- Load: 150 creates/s through **every** node, using one `loadgen` per node over all accounts, so most writes are forwarded.
- Probes: one 10 Hz probe writer per hash bucket, sent through n1.

The run was stopped partway at the lead's request, so not every scenario ran (see "Not run").

## How to run

```
bench/ha/run_all.sh                    # whole matrix (native + containers); builds everything first
bench/ha/run_all.sh native             # native-process scenarios only
bench/ha/run_all.sh ctr                # container scenarios (docker network disconnect, docker pause, libfaketime)
bench/ha/run_all.sh kill9-1of3 zombie  # named scenarios
SKIP_BUILD=1 HA_RUN_ID=x bench/ha/run_all.sh ...
python3 bench/ha/hactl.py list         # scenario catalogue
```

**Knobs** (environment variables). They let the harness run unchanged against the new design.

| Variable | Default | What it sets |
|---|---|---|
| `VLPDS_HA_NODE_ARGS` | see below | Node flag template. Placeholders: `{listen} {url} {advertise} {s3} {prefix} {id} {ttl_ms} {partitions}`. |
| `VLPDS_HA_PARTITIONS` | 16 | Expected total owned units, and the probe bucket count. |
| `VLPDS_HA_TTL_MS` | 3000 | Lease TTL. |
| `VLPDS_HA_RATE` | 150 | Writes/s per loadgen. |
| `VLPDS_HA_BASE_PORT` | 7100 | Node ports are base+i. Peer proxies are base+200+i. S3 proxies are base+2300+i. Containers are base+600+i. |
| `VLPDS_HA_OWNED_METRICS` | `vlpds_owned_shards,vlpds_owned_partitions` | Ownership gauges to observe. |
| `VLPDS_BIN_DIR` | | Directory containing `vlpds` and `loadgen`. |
| `VLPDS_HA_S3` | | S3 endpoint. |
| `VLPDS_HA_IMAGE` | | Container image. |
| `VLPDS_HA_DOCKER_S3` | | S3 endpoint as seen from containers. |

The default node template is:

```
--listen {listen} --public-url {url} --advertise-url {advertise} --s3-endpoint {s3} --prefix {prefix}
--cluster --node-id {id} --lease-ttl-ms {ttl_ms} --partitions {partitions} --dev-mode --workers 2 --io-threads 3 --firehose-ring-mb 256
```

**Ownership is an optional observation.** The harness tries these sources in order:
1. The `/internal/v1/cluster` status endpoint, if the build has it.
2. Otherwise, the first owned-units gauge from `VLPDS_HA_OWNED_METRICS`.
3. Otherwise, it treats "every live node is healthy" as converged.

Verdicts never depend on lease or assignment object layout.

### Files

| Path | Purpose |
|---|---|
| `hactl.py` | Orchestrator (stdlib Python): nodes, proxies, load, probes, checker, audits, verdicts. |
| `run_all.sh` | Builds the tools and runs the matrix. |
| `faultproxy/` | Go fault proxy, one HTTP instance and one TCP instance per node (details below). |
| `fhaudit/` | Go firehose audit (details below). |
| `Dockerfile` (+ `Dockerfile.dockerignore`) | Debian node image with `faketime`, for the `ctr-*` scenarios. It is built and libfaketime was verified to shift a container's wall clock (+2.5 s), but **no container scenario has been run yet**. |
| `out/<run>/summary.md` | One line per scenario. |
| `out/<run>/<scenario>/result.json` | Full metrics for one scenario. |

**faultproxy** runs in two modes:
- **HTTP mode**, in front of MinIO for each node. It injects latency, jitter, and S3-style 503/500 errors, or blackholes requests (they hang until healed).
- **TCP mode**, in front of the address each node *advertises* to peers. It can blackhole (stall) traffic, add latency, or reset connections. Clients still reach the node directly.

A control API is on its own port: `/set`, `/clear`, `/reset`, `/stats`.

**fhaudit** subscribes to `subscribeRepos`, optionally from a cursor. It records every created record path, and every commit's (seq, did, rev). On SIGINT it writes everything as JSON.

### What each scenario checks

1. **Acked writes:** every create acknowledged by any loadgen or probe is readable via `listRecords` (`loadgen verify`).
2. **Checker:** the sync-1.1 checker on n1 (which is never faulted) reports PASS. That means inversion proofs, signatures, and per-repo `since`/`prevData` chains are all intact, with no forks.
3. **Live firehose audit:** a live `fhaudit` runs on every node from the start. For each node that stayed up, every acked create must appear on its firehose, with no reorders. **The checker cannot catch a stall: a merged stream that stops just looks like a quiet repo. This audit is what catches it.**
4. **Replay audit:** after the load, a cursor replay runs on each surviving node from just before the run. Nodes that stayed up must be complete and must agree with each other, with the same commit count and last seq. Nodes that rejoined are reported but not judged.
5. **Probe outages:** the 10 Hz probes are reported as outage windows. A probe is "bad" if it failed or took more than 2 s. Each window is `[start s, end s, failed probes]` relative to the load start.
6. **Loadgen metrics:** errors per node and the maximum p99 from the 5 s lines.
7. **End state:** final ownership and node exit codes.

## Results (`base1`)

How to read the table:
- **"FH missing (live)"** counts acked creates that never appeared on a surviving node's live firehose, per node that stayed up.
- **"Outage windows"** comes from the probes.
- **"Max part. outage"** is the longest outage of any single partition, including the rebalance blip after a restart.

| Scenario | Verdict | Acked / lost | Checker | FH missing (live) | Outage windows (s) | Max part. outage | Loadgen errors (per node) | Exit codes |
|---|---|---|---|---|---|---|---|---|
| baseline-2 | FAIL¹ | 14261 / **0** | PASS | n1 0, n2 0 | none | 0 | 0 / 0 | – |
| baseline-3 | FAIL¹ | 18444 / **0** | PASS | 0, 0, 0 | none | 0 | 0 / 0 / 0 | – |
| baseline-5 | FAIL¹ | 27764 / **0** | PASS | 0 ×5 | none | 0 | 0 ×5 | – |
| kill9-1of3 | FAIL² | 31571 / **0** | PASS | n1 11753, n3 11912 | [15.0–25.6, 466 failed], [40.1–41.2, 30] | 26.2 | 473 / *3770 (killed node)* / 491 | n2 −9 |
| kill9-2of5 | FAIL² | 45512 / **0** | PASS | n1 17595, n2 17591, n5 17334 | [15.0–24.9, 506], [40.4–41.1, 25] | 26.0 | 441 / 442 / *3781* / *3769* / 435 | n3, n4 −9 |
| sigterm | FAIL² | 26530 / **0** | PASS | n1 8901, n3 8901 | [15.0–24.9, 373], [35.2–35.8, 24] | 20.7 | 504 / *3016* / 504 | n2 −15 (no handler) |
| rolling-restart | PASS³ | 28196 / **0** | PASS (+ cursor checker on n2 PASS) | n/a (every node restarted) | [10.0–14.2, 230], [22.0–26.5, 247], [34.0–38.9, 257] | 28.8 | 681 / 685 / 666 | all −15 |
| zombie (SIGSTOP 12 s) | FAIL² | 30920 / **0** | PASS | n1 8696, n3 8714 | [15.0–27.0, 6 failed + hung probes], [45.5–46.1, 24] | 31.0 | 406 / *4509* / 398 | n2 **5** (fenced) |
| zombie-short (3.3 s) | FAIL² | 28565 / **0** | PASS | n1 8695, n3 8898 | [15.0–24.9, 259], [40.1–41.2, 42] | 26.1 | 494 / *3766* / 501 | n2 **5** |
| s3-partition (12 s) | FAIL² | 29829 / **0** | PASS | n1 8636, n3 8679 | [15.0–27.0, 5 + hung], [40.5–41.1, 22] | 26.0 | 408 / *2574* / 410 | n2 **5** |
| peer-partition (12 s) | FAIL¹ | 30202 / **0** | PASS | 0, 0, 0 | [15.0–27.1, 0 failed, all hung] | 12.1 | 0 / 0 / 0 (p99 11.9 s) | – |
| full-partition (12 s) | FAIL² | 29689 / **0** | PASS | n1 8739, n3 8635 | [15.0–27.1, 5 + hung], [40.5–41.7, 32] | 26.2 | 517 / *2547* / 486 | n2 **5** |
| s3-slow, s3-slow-all, s3-5xx | ERROR⁴ | – | – | – | – | – | – | – |
| add-remove, cas-contention, handoff-firehose, all `ctr-*` | not run⁵ | | | | | | | |

**Footnotes:**

¹ **Replay disagreement** (bug B5): replays from the same cursor give different commit counts on different nodes. Every acked create is still present everywhere.
- baseline-2: n1 14276 vs n2 14280.
- baseline-3: 18521, 18506, 18512.
- baseline-5: 27613 vs 27604.
- peer-partition: 29140, 29156, 29153.

² **Survivors' firehoses stall permanently** after the next ownership move (bug B1). This happened in every scenario with a restart or rejoin, and accounts for 28–40 % of acked creates missing from the firehose.

³ **Rolling restart:** every node restarted, so there is no node to run a "stayed-up" audit on. The checker on n1 and the cursor checker on n2 both passed. The replay audits on the rejoined nodes are each incomplete in a different way (B7), as expected.

⁴ **ERROR = MinIO returned `507 Insufficient Storage`.** The host disk was at 100 % (8 GiB free), so `createAccount` and the node heartbeats failed. This is an environment problem, not vlpds: these scenarios need re-running once there is disk space.

⁵ Stopped at the lead's request before these ran.

### What held up

- **No acknowledged write was lost in any scenario.** That covers kill -9 of one and two nodes, SIGTERM, a rolling restart, SIGSTOP zombies (short and long), and S3, peer and full partitions.
- **The checker never saw a fork, a chain break, or a bad proof or signature.**
- **Zombies and partitioned nodes fail-stop as designed.** Their logs show one of:
  - `lease lapsed before segment PUT: fail-stop`
  - `lease lost unexpectedly: fail-stop` (after SIGCONT or heal, the renew's compare-and-swap fails)

  Either way the node exits with code 5, and it never acked anything stale.
- **Steady state works:** ownership is exactly once (status endpoint), and initial convergence takes 1.1–2.7 s. Forwarding works: in baseline-3, n1 forwarded 6895 requests and n2/n3 about 3000 each. There are no errors, and p99 stays under 110 ms at 150/s per node.

## Bugs found, on the per-partition model (none fixed: the lead froze those files)

**B1 – Followers never switch to a partition's new owner, so followers' firehoses stall permanently.** Severity: high. Files: `remote.rs`, `node.rs`.

- **Cause, part 1:** `remote::spawn_subscriber`'s loop only calls `owner()` again when the websocket ends. The *old* owner's `serve_stream` keeps running after `Node::close()`, because it holds an `Arc<Partition>`, so `live` never closes.
- **Cause, part 2:** while it keeps running, it heartbeats `wm.get()`, which keeps advancing with the clock (idle partition) until it reaches the old lease-expiry cap. Then it plateaus. Every third node's merged firehose then stops at `min_p W_p`, for every partition, forever.
- **Why the 2-node test missed it:** with two nodes, the only follower is the acquirer itself, and `open()` drops its subscription.
- **Evidence:** `smoke3/kill9-1of3`. After n2 restarts (at 04:42:41), n1 and n3 release partitions to it. n1's and n3's firehoses stop at 04:42:43.7 and 04:42:44.0 (about one lease TTL later), while load runs until about 04:43:02. 11,893 and 11,703 acked creates never appear. The same signature shows up in every `base1` scenario with a restart. The checker still PASSes, because nothing it receives is inconsistent.
- **Fix direction:**
  - The old owner ends its streams (and freezes its watermark at durable) on close.
  - Subscribers re-check `owner()` periodically and on a read timeout, then reconnect and catch up from S3.

**B2 – Watermark cap does not hold on graceful handoff.** Severity: high (correctness of the merged order). Files: `cluster.rs`, `node.rs`.

- **Cause:** `release()` writes `expires_ms = 0`, and the successor opens immediately (wait 0). The old owner's announced watermark may already have run ahead to "now" (bounded only by its old expiry, up to TTL in the future). The successor assigns seqs from its own clock, which can be below a watermark the followers have already used to emit.
- **Effect:** the merger then emits those events late and out of order. Live subscribers drop them (`send_batches` skips `seq <= last`), and ring replays drop them too. This is masked today by B1.
- **Fix direction:** on close, cap the watermark at durable. Record the cap in the released lease, and have the successor floor its seqs above it. That also covers skew.

**B3 – Failover takes about 10 s with TTL 3 s, because of the heartbeat liveness window.** Severity: medium (availability). File: `cluster.rs`.

- **Cause:** survivors only acquire up to `ceil(P / live)`, and a dead node counts as live until its heartbeat is older than `3 × TTL` (9 s). So orphaned partitions wait out the heartbeat, not the lease (TTL + skew = 3.6 s).
- **Evidence:** the outage windows are 10.2–10.6 s for kill -9 (1 of 3 and 2 of 5) and for SIGTERM.
- **Fix direction:** always take orphaned (expired) units regardless of fair share, and rebalance later. Or use TTL-scale liveness.

**B4 – No graceful shutdown.** Severity: medium. Files: `main.rs`, `server.rs`, `cluster.rs`.

- **Cause:** SIGTERM uses the default action and the process dies (exit −15), so a "graceful" restart is a crash. `Cluster::shutdown` exists but nothing calls it, and the `PartitionHost` isn't reachable from `main`. A rolling restart costs 4–5 s of outage per node.
- **Why not fixed:** wiring it means keeping the host in `Cluster`, and adding a stop flag plus a step lock so the lease loop can't re-acquire during shutdown. That is in the frozen files, so it was not done.

**B5 – Merged history differs between nodes.** Severity: medium; cause not yet found.

- **Evidence:** replaying from the same cursor on each node gives the same first and last seq, but different commit counts. Examples: baseline-2 has 14276 vs 14280; baseline-3 has 18521 / 18506 / 18512. This happens even with no faults. All acked creates are present on all nodes, so the differing commits are ones the load generators didn't track.
- **Hypotheses:**
  - Late or out-of-order events being dropped by `send_batches`' `seq <= last` filter. Each node would lose a different set, depending on merge timing.
  - Or something in the startup handoff.
- **Next step:** `fhaudit` now records every commit's (seq, did, rev), so a single rerun of `baseline-2` will show exactly which commits differ.

**B6 – Requests for a unit with no owner yet get HTTP 500 instead of a retryable 503.** Severity: low.

- **Cause:** when the lease has expired (so it's filtered out of the routing table) but no one has acquired it yet, `App::remote_owner` returns None. The request is handled locally and fails with `500 InternalServerError "repo load failed: partition not owned by this node"`.
- **Evidence:** 265 of these during kill9-1of3 (probe.csv).
- **Fix direction:** a 503 `PartitionUnavailable` with Retry-After, in `xrpc/mod.rs` and the worker.

**B7 – Firehose history starts when a node joins.** Severity: low (known TODO).

- **Cause:** a (re)started node only has events from its join onwards in memory. A cursor from before that replays from the oldest event it has. It does send one `OutdatedCursor` `#info` frame first, which is the correct behaviour: the replay audit on the rejoined n2 in kill9-1of3 received exactly one `#info` frame.
- **Evidence:** in kill9-1of3, n2's replay after it rejoined misses 18.5k creates.
- **Fix direction:** S3 cursor backfill, as already planned.

**B8 – Requests forwarded to a node cut off from its peers hang instead of failing fast.** Severity: low/medium.

- **Cause:** while peers can't reach the owner but the owner still holds its leases (peer-partition), forwarded requests hang until the network heals. The forward client has a 30 s timeout. The followers' firehose also pauses for the whole partition (no watermark heartbeats), though nothing is lost after the heal.
- **Evidence:** peer-partition. The p99 was 11.9 s and all of n2's units were unavailable for 12.1 s through peers, with zero failures.
- **Fix direction:** a shorter connect/first-byte timeout on forwarding. Possibly also peer-reachability input to lease decisions.

**B9 – A lease renew error doesn't stop the node acking writes.** Severity: low (observation).

- **Cause:** on a renew error that isn't a CAS conflict, the node keeps serving reads. It only fail-stops at the next PUT or ack.
- **Evidence:** with S3 cut (s3-partition, full-partition), n2 fail-stopped about 3.5–7 s into the cut. That is correct, but a single 600 ms renew hiccup kills the process once the cut lasts past `valid_until`. Expect exit-5 restarts under sustained S3 brownouts.
- **Note:** the s3-slow, s3-slow-all and s3-5xx scenarios exist to measure exactly this, but they hit the full disk (footnote 4).

## Code changes made

These are diagnostics only. No HA logic was changed.

- **`src/xrpc/internal.rs`:** added `GET /internal/v1/cluster`, using the same internal-token auth as the other internal endpoints. It returns `{node, owned, table, lease_valid, firehose_last_emitted, firehose_min_watermark}`. The harness uses it as an optional ownership observation. The new design can drop it or reshape it: the harness falls back to the metric gauges.
- **`src/firehose.rs`:** `Firehose::min_watermark` is now `pub`, for the endpoint above. This was made before the files were frozen. It is a visibility change only.

## Not run, or environment issues

- **Disk:** the host disk is full (MinIO 507 below its free-space threshold). The scenarios that hit it are s3-slow, s3-slow-all and s3-5xx. Free space before re-running.
- **Stopped by the lead:** add-remove, cas-contention, handoff-firehose and the `ctr-*` scenarios (network partition, docker pause, and clock skew ±250 ms / ±2.5 s) were never run. They are implemented and ready. The container image is built and libfaketime is verified working.
- **Clock skew:** this is the `ctr-skew-*` scenarios. They use libfaketime with `DONT_FAKE_MONOTONIC=1`, so only wall time is skewed and the `Instant`-based lease validity is not. The expectation is that skew inside the TTL/5 margin is safe. Beyond the margin, a successor can take over while the old owner's `valid_until` is still in the future; log fencing (If-None-Match) prevents forks, but the merged order can break (as in B2) and fh-missing shows up.
