# vlpds HA end-to-end results: per-node-log design

These results are for the **per-node-log design**:
- 65,536 hash slots grouped into `--shards N` (64 here, plus 256 for the 5-node runs);
- one log per node incarnation;
- node leases;
- CAS shard assignments;
- fencing of dead logs.

The previous baseline, on the per-partition-lease design, is summarised under "Comparison with the old baseline" below, with its bug list B1–B9 and each bug's status today.

- **Run IDs:** `final` is the whole native matrix. `final2` and `final3` re-run the scenarios affected by the last fix (the log-stream idle timeout). `final-rep` holds repeats. `final-256` and `final3-256` are the 256-shard runs. `final2-ctr` (and `final-ctr`) are the containers. Outputs are in `bench/ha/out/<run>/<scenario>/`.
- **Setup:** native processes on one Mac (14 cores), with native MinIO on 127.0.0.1:9200.
- **Lease TTL:** 3 s, so renew and skew margin are both 600 ms.
- **Load:** 150 creates/s through **every** node, using one `loadgen` per node over all accounts, so most writes are forwarded.
- **Probes:** 32 probe writers at 10 Hz, one per sampled shard, sent through n1.
- **Node flags:** `--shards N --no-rate-limits --dev-mode --workers 2 --io-threads 3`.
- **Host load:** the host was shared with other agents' builds and benchmarks. The load average was 25–67 during `final` and 8–25 later. Absolute latencies are therefore pessimistic, but failover and outage times are dominated by TTL and skew.
- **Binaries:** every run used binaries built from the tree with all of the HA fixes below. The exception is the final idle-timeout fix in `remote.rs`. The `final` native matrix predates it, so every scenario that could involve a peer stream stall was re-run after it (`final2`, `final3`, `final2-ctr`).

## How to run

```
bench/ha/run_all.sh                    # whole matrix (native + containers); builds everything first
bench/ha/run_all.sh native             # native-process scenarios only
bench/ha/run_all.sh ctr                # container scenarios (docker network disconnect, docker pause, libfaketime)
bench/ha/run_all.sh kill9-1of3 zombie  # named scenarios
SKIP_BUILD=1 HA_RUN_ID=x VLPDS_HA_PARTITIONS=256 bench/ha/run_all.sh baseline-5 kill9-2of5
python3 bench/ha/hactl.py list         # scenario catalogue
```

**Knobs** (environment variables):

| Variable | Default | What it sets |
|---|---|---|
| `VLPDS_HA_NODE_ARGS` | see below | Node flag template. Placeholders: `{listen} {url} {advertise} {s3} {prefix} {id} {ttl_ms} {partitions}`. |
| `VLPDS_HA_PARTITIONS` | 64 | Shard count: passed as `--shards`, and used for the probe shard mapping. |
| `VLPDS_HA_TTL_MS` | 3000 | Lease TTL. |
| `VLPDS_HA_RATE` | 150 | Writes/s per loadgen. |
| `VLPDS_HA_PROBES` | 32 | Probe writers, each on a shard sampled at random with a fixed seed. |
| `VLPDS_HA_CLEANUP` | 1 | Delete the scenario's bucket prefix once its results are recorded. Deletion runs through `mc` in `vlpds-minio:local`. |
| `VLPDS_HA_INTERNAL_TOKEN` | `dev-internal-token` | The `x-vlpds-internal` token. It falls back to the admin token on 401, for older dev-mode builds. |
| `VLPDS_HA_BASE_PORT`, `VLPDS_BIN_DIR`, `VLPDS_HA_S3`, `VLPDS_HA_IMAGE`, `VLPDS_HA_DOCKER_S3` | | As before. |

The default node template is:

```
--listen {listen} --public-url {url} --advertise-url {advertise} --s3-endpoint {s3} --prefix {prefix}
--node-id {id} --lease-ttl-ms {ttl_ms} --shards {partitions} --no-rate-limits --dev-mode --workers 2 --io-threads 3 --firehose-ring-mb 256
```

**Harness changes for the new design:**
- **Shard mapping:** the probe mapping now follows `src/slots.rs`: `(top 16 bits of sha256(did)) * N / 65536`.
- **`--shards` and `--no-rate-limits`** replace the old flags.
- **Probe sampling:** probe shards are sampled across all nodes. Before, they were the first N in account order, which meant only n1 and n2's shards.
- **Outage metric:** the per-shard outage is now the longest *contiguous* window. It no longer spans from a kill to the rebalance blip at a later restart.
- **Late joiners:** a node that joined mid-run was not judged on cursor-replay completeness. Since the O1 fix every survivor is judged.
- **Replay window:** the replay window is 30 s when a rejoined node backfills from S3, versus 8 s otherwise.
- **New checks:**
  - **History diff:** a cross-node comparison of merged history, over (seq, did, rev) sequences. It covers both the cursor replays and the live audits over their common range.
  - **Exit codes:** expected exit codes, e.g. a zombie must exit 3 or 5.
  - **Unexpected exits:** a scenario fails on any unexpected exit (used by `grow-1-to-3`).
- **Diagnostics added to the code:**
  - a `checkpoint start` log line, which `kill9-mid-checkpoint` keys off;
  - `acquired shards` and `releasing extra shards` log lines;
  - a firehose-merger warning for late events (an event at or below the emitted watermark). It never fired in any run.

### What each scenario checks

These are as before:
1. **Acked writes:** every create acknowledged by any loadgen or probe is readable (`loadgen verify`).
2. **Checker:** the sync-1.1 checker reports `-strict` PASS on n1. Some scenarios also run a second, cursor-based checker.
3. **Live firehose audit:** for every node that stayed up, every acked create is on its live firehose.
4. **Replay audit:** a cursor replay from before the run on each survivor is complete.
5. **Merged-history agreement (new):** every node that stayed up emits the identical commit sequence, both in the replay and in the live audit.
6. **Availability:** probe outage windows. A probe is bad if it failed or took more than 2 s.
7. **Exit codes:** these are recorded, and expected codes are checked where a scenario sets them.

## Results

How to read the tables:
- "Outage windows" are relative to load start: `start–end s (failed probes)`. Long windows with few failed probes are hung requests (see O2).
- "Max shard outage" is the longest contiguous outage of a single shard.
- Exit code −9 is the harness's kill, 0 a graceful exit, 5 a lease fail-stop, 3 a fenced-log fail-stop, and 137 a docker kill.

### Native, 64 shards (`final`, every native scenario)

| Scenario | Verdict | Acked / lost | Checker | FH missing (live, stayed-up nodes) | History agree (replay / live) | Outage windows [start–end s, failed probes] | Max shard outage | Exits |
|---|---|---|---|---|---|---|---|---|
| baseline-2 | PASS | 19365 / **0** | PASS | all 0 | yes / yes | none | 0.0 s | – |
| baseline-3 | PASS | 24092 / **0** | PASS | all 0 | yes / yes | none | 0.0 s | – |
| baseline-5 | PASS | 33084 / **0** | PASS | all 0 | yes / yes | none | 0.0 s | – |
| kill9-1of3 | PASS | 41804 / **0** | PASS | all 0 | yes / yes | 15–19.7 (745), 40.2–42 (110) | 4.73 s | n2 -9 |
| kill9-2of5 | PASS | 55522 / **0** | PASS | all 0 | yes / yes | 15–19.8 (410), 40.4–41.6 (115) | 4.73 s | n3 -9, n4 -9 |
| sigterm | PASS | 35884 / **0** | PASS | all 0 | yes / yes | 15–16.1 (134), 35.3–36.5 (172) | 1.13 s | n2 0 |
| rolling-restart | PASS | 37411 / **0** | PASS | – | yes / yes | 10.3–12.5 (296), 22.1–22.7 (13), 34–36.7 (587) | 2.76 s | n1 0, n2 0, n3 0 |
| zombie | PASS | 40547 / **0** | PASS | all 0 | yes / yes | 15–27 (13), 45.7–46.8 (93) | 12.01 s | n2 5 |
| zombie-short | PASS | 38212 / **0** | PASS | all 0 | yes / yes | 15–18.6 (75), 40.3–41.5 (162) | 3.61 s | n2 5 |
| zombie-check | PASS | 28665 / **0** | PASS | all 0 | yes / yes | 15–27 (15) | 12.01 s | n2 5 |
| s3-partition | PASS | 38241 / **0** | PASS | all 0 | yes / yes | 15–19.8 (161), 40.2–41.4 (82) | 4.67 s | n2 5 |
| peer-partition | PASS | 37178 / **0** | PASS | all 0 | yes / yes | 15–27.1 (0) | 12.09 s | – |
| full-partition | PASS | 36829 / **0** | PASS | all 0 | yes / yes | 15–27 (16), 40.4–41.7 (114) | 12.02 s | n2 5 |
| s3-slow | PASS | 40248 / **0** | PASS | all 0 | yes / yes | 42–43.8 (242) | 1.78 s | n2 -9 |
| s3-slow-one-long | PASS | 35416 / **0** | PASS | all 0 | yes / yes | 15–19.3 (180), 35.1–36.4 (101) | 4.27 s | n2 5 |
| s3-slow-all | PASS | 22038 / **0** | PASS | – | yes / yes | 15–40 (5405) | 24.98 s | n1 5, n2 5, n3 5 |
| s3-5xx | PASS | 43020 / **0** | PASS | all 0 | yes / yes | 30.1–34 (261), 45.5–46.6 (85) | 3.94 s | n2 5 |
| s3-5xx-all | PASS | 38240 / **0** | PASS | all 0 | yes / yes | none | 0.0 s | – |
| add-remove | PASS | 36237 / **0** | PASS | all 0 | yes / yes | 10.3–11.4 (91), 20.3–21.6 (89), 32–33.7 (108), 44–48.5 (330) | 4.46 s | n3 -9, n4 0 |
| grow-1-to-3 | PASS | 15881 / **0** | PASS | all 0 | yes / yes | 8.4–9.5 (129), 16.2–17.4 (92) | 1.14 s | – |
| cas-contention | PASS | 9600 / **0** | PASS | – | – / – | converged in 1.65 s |  | – |
| handoff-firehose | PASS | 40189 / **0** | PASS | all 0 | yes / yes | 10–12 (297), 18.2–19.8 (105), 34.3–35.4 (168), 42–43.8 (89) | 1.96 s | n2 0, n3 -9, n4 0 |
| kill9-rebalance-drainer | PASS | 39110 / **0** | PASS | all 0 | yes / yes | 12.6–18.7 (632), 32.5–33.6 (57) | 5.0 s | n2 -9 |
| kill9-rebalance-joiner | PASS | 41925 / **0** | PASS | all 0 | yes / yes | 12.1–13.3 (86), 13.7–17.5 (267), 32.5–33.7 (144) | 5.43 s | n4 -9 |
| kill9-mid-checkpoint | PASS | 43526 / **0** | PASS | all 0 | yes / yes | 8.1–11.9 (538), 25.2–26.4 (110), 35.1–38.9 (337), 50.3–51.5 (196) | 3.83 s | n2 -9/-9 |

### Re-run after the log-stream idle-timeout fix (`final2`, `final3`, 64 shards)

| Scenario | Verdict | Acked / lost | Checker | FH missing (live, stayed-up nodes) | History agree (replay / live) | Outage windows [start–end s, failed probes] | Max shard outage | Exits |
|---|---|---|---|---|---|---|---|---|
| peer-partition | PASS | 37908 / **0** | PASS | all 0 | yes / yes | 15–27.1 (0) | 12.08 s | – |
| full-partition | PASS | 37657 / **0** | PASS | all 0 | yes / yes | 15–27 (11), 40.2–41.4 (130) | 12.0 s | n2 5 |
| zombie | PASS | 40314 / **0** | PASS | all 0 | yes / yes | 15–27 (12), 45.1–46.8 (96) | 11.98 s | n2 5 |
| zombie-short | PASS | 38245 / **0** | PASS | all 0 | yes / yes | 15–19 (88), 40.2–41.3 (116) | 4.0 s | n2 5 |
| zombie-check | PASS | 28910 / **0** | PASS | all 0 | yes / yes | 15–27 (12) | 12.01 s | n2 5 |
| kill9-1of3 | PASS | 41791 / **0** | PASS | all 0 | yes / yes | 15–19.5 (463), 40.2–41.4 (99) | 4.44 s | n2 -9 |
| s3-partition | PASS | 38264 / **0** | PASS | all 0 | yes / yes | 15–19.4 (135), 40.4–41.5 (121) | 4.4 s | n2 5 |
| handoff-firehose | PASS | 39673 / **0** | PASS | all 0 | yes / yes | 10–12 (232), 18.3–19.8 (77), 26–30.6 (441), 34.2–35.4 (91), 42–43.9 (161) | 4.57 s | n2 0, n3 -9, n4 0 |
| sigterm | PASS | 35742 / **0** | PASS | all 0 | yes / yes | 15–16.1 (115), 35.4–36.5 (111) | 1.14 s | n2 0 |
| kill9-2of5 | PASS | 55418 / **0** | PASS | all 0 | yes / yes | 15–19.5 (523), 40.4–41.7 (168) | 4.48 s | n3 -9, n4 -9 |

### 256 shards (`final-256`, `final3-256`)

| Scenario | Verdict | Acked / lost | Checker | FH missing (live, stayed-up nodes) | History agree (replay / live) | Outage windows [start–end s, failed probes] | Max shard outage | Exits |
|---|---|---|---|---|---|---|---|---|
| baseline-5 | PASS | 33128 / **0** | PASS | all 0 | yes / yes | none | 0.0 s | – |
| sigterm | PASS | 35989 / **0** | PASS | all 0 | yes / yes | 15–16.4 (73), 35.6–36.8 (153) | 1.34 s | n2 0 |
| rolling-restart | PASS | 37307 / **0** | PASS | – | yes / yes | 10.2–12.4 (175), 23.5–24.7 (61), 34–36.8 (720) | 2.79 s | n1 0, n2 0, n3 0 |
| grow-1-to-3 | PASS | 17230 / **0** | PASS | all 0 | yes / yes | 8.5–9.9 (140), 16.3–17.8 (83) | 1.44 s | – |
| kill9-2of5 | PASS | 55307 / **0** | PASS | all 0 | yes / yes | 15–19.9 (616), 40.2–42.1 (200) | 4.87 s | n3 -9, n4 -9 |

`baseline-5` at 256 shards converged in 2.47 s (52/52/52/52/48 shards). In `grow-1-to-3`, n1 took all 256 shards alone on first start (the lead's lease-lapse repro, now passing) and then rebalanced to 86/86/84 under load.

### Repeats of the race-prone scenarios (`final-rep`)

| Scenario | Verdict | Acked / lost | Checker | FH missing (live, stayed-up nodes) | History agree (replay / live) | Outage windows [start–end s, failed probes] | Max shard outage | Exits |
|---|---|---|---|---|---|---|---|---|
| kill9-rebalance-drainer | PASS | 39263 / **0** | PASS | all 0 | yes / yes | 12.6–17.9 (478), 32.4–33.5 (85) | 4.3 s | n2 -9 |
| kill9-rebalance-joiner | PASS | 41910 / **0** | PASS | all 0 | yes / yes | 12.1–13.5 (92), 13.7–17.6 (315), 32.1–33.8 (138) | 5.24 s | n4 -9 |
| zombie-short | PASS | 37949 / **0** | PASS | all 0 | yes / yes | 15–19.8 (196), 40.3–41.4 (60) | 4.75 s | n2 5 |

`zombie-short` ran twice in `final-rep`. Both runs passed (`out/final-rep/summary.md`); the table shows the second, whose `result.json` overwrote the first.

### Containers (`final2-ctr`): docker network disconnect, docker pause, libfaketime clock skew

| Scenario | Verdict | Acked / lost | Checker | FH missing (live, stayed-up nodes) | History agree (replay / live) | Outage windows [start–end s, failed probes] | Max shard outage | Exits |
|---|---|---|---|---|---|---|---|---|
| ctr-baseline-3 | PASS | 19363 / **0** | PASS | all 0 | yes / yes | none | 0.0 s | – |
| ctr-partition | PASS | 33027 / **0** | PASS | all 0 | yes / yes | 15.1–30.1 (423), 41–42.2 (79) | 15.01 s | n2 5 |
| ctr-pause | PASS | 31154 / **0** | PASS | all 0 | yes / yes | 15–27.1 (14), 41.4–42.9 (44) | 12.09 s | n2 5 |
| ctr-skew-small | PASS | 30424 / **0** | PASS | all 0 | yes / yes | 15.2–20.3 (374), 36.8–38.1 (57) | 4.93 s | n2 137 |

| Scenario | Verdict | Notes |
|---|---|---|
| ctr-skew-large (n2 +2.5 s, n3 −2.5 s) | ERROR (expected: out of spec) | Clock skew of 2.5 s exceeds the skew margin (600 ms). See O3. |
| ctr-skew-steady (same skew, no faults) | ERROR (same cause) | Same as above (`final-ctr`). |

In both skew-large runs, n3 (clock −2.5 s) wrote leases that looked expired to its peers 1.1 s after each renewal. Its peers declared it dead and took the shards it had opened a second earlier (its SlateDBs logged `Fenced`), fenced its log, and n3 exited 3 (`our log was fenced by a successor`). Setup failed because n3 died mid-`createAccount`. Safety held, availability did not.

### Specific checks

- **No acknowledged write was lost in any run.** That covers 35 native runs at 64 shards (`final`, `final2`, `final3`), 6 at 256 shards, 4 repeats and 4 container runs, plus every earlier smoke run. Checker `-strict` passed in every run on the final binaries.
- **Merged history agrees across nodes everywhere** (the old B5). Replays from one cursor give identical (seq, did, rev) sequences on every node that stayed up. The live audits agree over their common range in every scenario. The merger's late-event warning never fired.
- **Zombie (SIGSTOP 4×TTL, then SIGCONT):**
  - n2 exits 5 within about 0.5 s of waking (`node lease lapsed past takeover` or `lapsed before renewal`), and acks nothing stale: verify finds 0 lost and the checker passes.
  - With a short pause (1.1×TTL, waking around takeover) n2 also exits 5 and nothing is lost. Before the fixes below, this exact scenario produced a firehose chain break and two permanently unloadable repos (N6, N7).
- **kill -9 mid-checkpoint (twice):** the kill lands about 10–30 ms after `checkpoint start`. The successors replay the whole log tail (1,284 and 1,388 segments) in 0.4–0.6 s, and every survivor ends its span at the same fence ordinal. Nothing is lost.
- **kill -9 mid-rebalance:**
  - Killing the draining node 0.4 s into the join, or the joining node while it opens its shards, loses nothing.
  - Windows are about 5–6 s: TTL + skew plus the rebalance.
- **CAS contention:** 8 nodes started at the same instant converge in 1.65–2.1 s, with exactly 8 shard opens per node: no CAS churn and no double ownership.
- **Failover time:**
  - Takeover after kill -9 is 4.4–4.9 s (TTL 3 s + skew 0.6 s + replay), versus 9.9–10.6 s before.
  - Shards opened with 1,100–1,900 segments replayed take 150–1,000 ms (`segments_replayed` in the logs).
  - A graceful SIGTERM moves its shards in 1.1–1.4 s, and exit is 0 after 0.5–0.9 s.
- **S3 brownouts:**
  - **400 ± 400 ms latency on one node:** no outage, no fail-stop (this used to fail-stop, see N2).
  - **1500 ms latency, on one node or on all nodes:** the affected nodes fail-stop with exit 5. This is a protocol limit at TTL 3 s, not a bug: renewals are sequential CAS PUTs and validity is send time + TTL − skew, so a renewal RTT above (TTL − skew)/2 = 1.2 s opens a validity gap. With the production default TTL of 10 s the tolerance is 4 s. With all three nodes down, the cluster is unavailable until the harness restarts them (25 s).
  - **30 % 503s on one node or on all nodes:** no outage at all; the retries absorb them.
  - **30 % 503s, then 100 % 500s on one node:** that node's lease lapses, it is fenced, and it exits 3 or 5. The outage is about 4 s.

## Comparison with the old baseline (`base1`, per-partition leases, 16 partitions)

| Scenario | Old verdict | Old outage / FH missing | New verdict | New outage / FH missing |
|---|---|---|---|---|
| baseline-2/3/5 | FAIL (B5 history disagreement) | – | PASS | identical history on all nodes |
| kill9-1of3 | FAIL (B1 firehose stall) | 10.6 s / 11.7k missing | PASS | 4.4–4.7 s / 0 |
| kill9-2of5 | FAIL (B1) | 9.9 s / 17.6k missing | PASS | 4.5 s (64 shards), 4.9 s (256) / 0 |
| sigterm | FAIL (B1, no SIGTERM handler) | 9.9 s / 8.9k missing | PASS | 1.1 s / 0, exit 0 |
| rolling-restart | PASS | 4.2–4.9 s per node | PASS | 0.6–2.7 s per node |
| zombie / zombie-short | FAIL (B1) | 12 s / 25.9 s, about 8.7k missing | PASS | 12 s (hung forwards, O2) / 4 s; 0 missing |
| s3-partition | FAIL (B1) | 12 s / 8.6k missing | PASS | 4.4 s / 0 |
| peer-partition | FAIL (B5) | 12.1 s (hung) | PASS | 12.1 s (hung, O2) |
| full-partition | FAIL (B1) | 12 s / 8.7k missing | PASS | 12 s (hung forwards, O2) / 0 |
| s3-slow, s3-slow-all, s3-5xx | ERROR (disk full) | – | PASS | see S3 brownouts above |
| add-remove, cas-contention, handoff-firehose, all `ctr-*` | not run | – | PASS (except ctr-skew-large and ctr-skew-steady, out of spec) | – |

### Status of the old bugs

- **B1 – followers never switch owners / firehose stalls: gone, with two regressions found and fixed.**
  - The design itself removes the old mechanism: followers follow node logs, and a dead log is drained to its fence.
  - It came back twice by other routes: graceful shutdown never fenced its log (N1), and half-open peer connections never timed out (N8). Both are fixed.
- **B2 – watermark cap on graceful handoff: not observed.**
  - The merger's late-event diagnostic never fired in any run, including joins, rebalances, handoffs and container skew within the margin.
  - Shards no longer move between per-shard streams; per-log watermarks plus the join grace cover it.
- **B3 – slow failover from the heartbeat liveness window: fixed by design.** Takeover is TTL + skew + replay, 4.4–4.9 s.
- **B4 – no graceful shutdown: fixed by design**, with a race fixed here (N1): the step loop could re-acquire shards during shutdown. Exit 0 in 0.5–0.9 s.
- **B5 – merged history differs between nodes: gone.** Every node that stayed up agrees on identical (seq, did, rev) sequences in every scenario.
- **B6 – 500 instead of 503 for an unowned shard: fixed by design.** Moving or unowned shards return 503 `PartitionUnavailable`.
- **B7 – firehose history starts at join: mostly fixed by another agent's S3 cursor backfill**, which landed during this work.
  - Rejoined nodes' cursor replays are now complete.
  - The seam at a node's start (O1) is fixed (`o1fix`, below).
- **B8 – forwards to an unreachable owner hang: partly fixed.**
  - The 1 s connect timeout fails fast when the peer is gone.
  - A peer whose TCP endpoint accepts but stalls (frozen process, blackholed path, disconnected container) still holds forwarded requests up to the 15 s total timeout (O2).
- **B9 – a lease renew error doesn't stop the node acking: fixed** (N4, N5). The node now fail-stops once its lease is past takeover. It no longer waits for the next PUT, which may be hung.

## Bugs found and fixed in this round (all in the HA files, each commented `HA fix` in the code)

**N1 – Graceful shutdown never fenced its log, so every peer's firehose stalled permanently.** Severity: high. File: `cluster.rs` (`shutdown`).

- **Cause:** shutdown released the shards and deleted the node lease, but never closed the log. Peers drain a dead log from S3 *up to its fence* before removing its watermark source. With no fence, they waited forever, and their merged firehose stopped at the dead node's last watermark. Since the lease was deleted, a restart with the same node id did not fence the old log either.
- **Evidence:** `new-smoke1/sigterm`. All three firehoses stopped at 05:39:53.64, the instant of the SIGTERM. 25.6k acked creates were missing on n1 and n3, and their logs never showed "dead peer log drained".
- **Fix:**
  - Shutdown fences its own idle log after releasing its shards.
  - It also sets a stop flag and takes a step lock, so a concurrent step cannot re-acquire the shards being released (B4's race).
- **After the fix:** sigterm passes (0 missing, exit 0), and so does rolling-restart.

**N2 – Lease renewal was serialised behind an O(shards) sequential scan, so a mild S3 brownout fail-stopped the node.** Severity: high. File: `cluster.rs`.

- **Cause:** the lease was renewed only at the top of `step()`. The step then made about 70 sequential S3 round trips: LIST, the node leases, and one GET per assignment.
- **Evidence:** `new-smoke2/s3-slow`. With 400 ± 400 ms latency, a step took about 25 s against a 2.4 s validity window, and n2 exited 5, 2.1 s into the brownout (`lease lapsed before segment PUT`).
- **Fix:**
  - Renewal runs on its own loop, every renew interval, independent of the step.
  - Assignment GETs are concurrent (32 in flight).
  - A node without a valid lease never acquires or releases shards.
- **After the fix:** s3-slow shows no outage and no fail-stop.

**N3 – Fresh lone node fail-stopped on first start: the inline first step outlived the lease.** Severity: high. File: `cluster.rs`. This is the lead and UI-agent report.

- **Cause:** `server::build` runs the first step inline, before the renew loop exists. A node starting alone acquires its whole share there: one CAS PUT per shard, then it opens all of them. For the UI agent that was 181 shards in 19 s against a 10 s TTL.
- **Repro:** a fresh prefix and 256 shards, with n1 starting 3 s before n2 and n3, at TTL 1 s. n1's inline step took 1.77 s, and n1 exited 5 right after "node ready" (`repro-alone-before`). A simultaneous start does not reproduce it: peers are visible, so the join grace defers acquisition to spawned steps.
- **Fix:**
  - The whole inline step runs under a keepalive that renews every interval. Renewals are never cancelled mid-flight, because a dropped CAS PUT could land with an ETag we never learn.
  - The renew loop and watchdog keep running through a graceful shutdown's drain, and stop only when the lease is deleted.
- **After the fix:**
  - At TTL 1 s and 3 s, all three nodes stay up and rebalance.
  - SIGTERM of a node holding 256 shards at TTL 1 s drains in 3.4 s with exit 0.
  - The `grow-1-to-3` regression scenario passes at 64 and at 256 shards.

**N4 – No lease watchdog: a node with hung S3 calls stayed up as a zombie.** Severity: medium. File: `cluster.rs`.

- **Cause:** validity was checked only before a segment PUT or an ack. A node whose PUT hung in an S3 blackhole kept client and forwarded requests open until the network healed, long after its peers had fenced its log.
- **Evidence:** in s3-partition, the longest shard outage was 12.0 s.
- **Fix:** a watchdog fail-stops (exit 5) once the lease has been invalid for longer than 2 × skew.
- **After the fix:** s3-partition's longest shard outage is 4.4 s.

**N5 – A zombie resurrected its own lapsed lease.** Severity: high (availability; it amplified N6). File: `cluster.rs`.

- **Cause:** after a SIGSTOP, the renew loop's CAS on our own lease object succeeds, because nobody else writes it, even though peers have already fenced our log. Peers then count the dead node as live for another TTL. They shrink their fair share and release the shards they had just taken over: n3 released 10 shards 20 ms after opening them, and they sat unowned for about 3 s.
- **Evidence:** `final` (pre-fix binary)/zombie-short. n3 logged `releasing extra shards owned=32 fair=22 live=3` right after taking n2's shards with live=2.
- **Fix:** never renew a lease that has already lapsed; fail-stop instead.

**N6 – Stale worker repo cache across a shard bounce: firehose chain break and permanently unloadable repos.** Severity: **critical** (data corruption). Files: `node.rs`, `nodelog.rs`. The root cause is in `worker.rs`, which is not in my ownership.

- **Cause:**
  - A repo load in flight when `close()` purged the workers completes afterwards and re-caches the repo, still bound to the closed shard.
  - Writes then build commits on that cached state. The cached head advances, but the entries cannot be durably applied.
  - When the node later takes the shard back, the next durable commit chains on those never-logged commits.
- **Evidence:** `final` (pre-fix binary)/zombie-short, combined with N5's bounce.
  - n3 logged 25 rejected log entries for shard 51 (my N7 guard), then re-acquired shard 51.
  - The checker reported `chain_since` and `chain_prevdata` failures for two repos: the `since` named a rev that was never on the firehose.
  - On the next owner both repos failed every load with `rebuilt MST root … != head data …`. That is a 20 s outage window that never recovers for those repos: their stored records no longer match their head.
- **Fix:**
  - `open_many` purges every worker's cache for a shard before serving it.
  - `close()` purges again after the drain.
- **After the fix:** zombie-short passes 4 out of 4 (`final`, `final-rep` ×2, `final2`), and so do all the rebalance scenarios.
- **Still wanted:** a fix in `worker.rs`, so that a `Loaded` result whose shard has since changed (`Arc::ptr_eq` against the current partition) is dropped. The lead has queued it.

**N7 – Writes for a shard the node no longer holds were acked but never replayable (lost acked writes).** Severity: high. File: `nodelog.rs` (`Open::push`).

- **Cause:** such an entry got epoch 0 and was acked. Replay applies only entries whose epoch matches a span, so the successor never saw it.
- **Fix:** reject the entry (the ack fails, so the client gets an error) instead of logging it under epoch 0.
- **Evidence:** this is the path the N6 race took (25 rejections). Without the guard, those 25 writes would have been acknowledged and lost.

**N8 – A half-open peer log stream hung the follower forever: a permanent firehose stall after a network partition.** Severity: high. File: `remote.rs`.

- **Cause:**
  - `stream_live` awaited `ws.next()` with no timeout. The follower only re-checks whether its peer is alive after the socket ends.
  - With `docker network disconnect`, the dead peer never sends a FIN or RST, so the survivors never drained its log to the fence.
  - The native faultproxy tests miss this because healing the proxy releases the held connection.
- **Evidence:** `final-ctr/ctr-partition`. n1 and n3 were each missing 24,499 acked creates, and neither logged "dead peer log drained", although n3 had fenced n2's log at ordinal 3045.
- **Fix:** a 2 s idle timeout on the stream (heartbeats come every 5 ms), and a 2 s connect timeout.
- **After the fix:** `final2-ctr/ctr-partition` passes with 0 missing, and so do peer-partition, full-partition, the zombies and handoff-firehose (`final2`).

**N9 – Survivors stacked extra fences on an already-fenced log.** Severity: low. File: `cluster.rs` (`fence`).

- **Cause:** a second survivor's LIST counted the first survivor's fence object as a segment and wrote another fence after it.
- **Evidence:** `new-smoke2/kill9-1of3` (n1 fenced at 2742, then n3 at 2743).
- **Fix:** if the last object is already a fence, its ordinal is the log's end.
- **After the fix:** every survivor used the same end ordinal (`kill9-mid-checkpoint`: 1284 on both).

## Open issues

**O1 – Firehose seam on a node that just (re)started.** Severity: medium. Files: `remote.rs`, `firehose.rs`. **Fixed** (see "O1 fix and re-run" below).

- **Cause:**
  - A first-time follower skips every peer batch broadcast before its subscription, but the ring floor is set from the first merged batch.
  - Another peer's skipped events can have seqs above that floor, so they are in neither the ring nor the S3 backfill.
  - Live subscribers of the new node miss them too.
- **Evidence:**
  - `final/kill9-2of5` (pre-fix run): n3's replay is missing 15 commits, all in 07:19:10.843–.878, at n3's join.
  - `final/rolling-restart` (pre-fix run): the cursor checker on n2 reports `chain_since` FAILs for 4 repos, at 07:22:34.55–.73, just after n2 restarted.
  - `final2-ctr/ctr-skew-small`: 74 missing on the rejoined n2.
  - Nodes that stayed up are unaffected, and so is every acked write.
- **Suggested fix:** use max over the initial followers of their first heartbeat watermark as the start floor. Drop events at or below it, and let the S3 backfill serve them.

### O1 fix and re-run (`o1fix`)

- **Root cause, confirmed.** Two holes, both in `remote.rs`:
  - A first-time follower (no known ordinal) skipped everything its peer broadcast before the subscription, while the ring floor came from the first merged batch. A peer subscribed later lost its events in between: in neither the ring nor the backfill (<= floor). The new in-process test reproduces it every run on the old code (thousands of events missing on a joining node).
  - The S3 catch-up on reconnect started right after the websocket handshake, but the owner subscribes only after the upgrade. A batch PUT in between was in neither, and the first heartbeat already covered it. That is also why the suggested max(w0) floor alone would not do: the finalizer broadcasts a batch before it advances the watermark, so w0 can trail a batch the subscription missed.
- **Fix:**
  - The merged stream starts at a floor F, the clock at startup. The ring floor starts at F, and cursors <= F backfill from S3 once the merger's min watermark has passed F (so every log's events <= F are in S3).
  - Every follower owes every event of its log above its floor: F for the followers registered by the first step (the merger starts only after it), or the merger's position when a later log is followed (`Firehose::add_remote`, taken under the sources lock). Its first ordinal is the first segment in S3 past that floor (`backfill::seek`).
  - On every (re)connect, the follower waits for the owner's first message, so it knows it is subscribed, then catches up from S3 and dedupes by ordinal. A dead log is drained to its fence even if it was never streamed.
  - The merger drops events at or below its position. Events <= F are expected; any above it are logged as late.
- **Harness:** every survivor's cursor replay is now judged, including rejoined and late-joined nodes, and all must agree. New `audit_from_start` attaches a live audit the moment a node (re)starts. It must match a stayed-up node's replay over its range, with no gaps. kill9-2of5 and grow-1-to-3 also run a cursor checker on the (re)started node.

| Scenario | Verdict | Acked / lost | Checker / cursor checker | Replay missing (all survivors) | Live audit from (re)start | History agree |
|---|---|---|---|---|---|---|
| kill9-2of5 | PASS | 55453 / **0** | PASS / n3 PASS | all 0 (55270 commits on every node, n3 and n4 rejoined) | n3, n4: identical to n1 (23.9k commits each) | yes / yes |
| rolling-restart | PASS | 37767 / **0** | PASS / n2 PASS | all 0 (37738 on every node, all rejoined) | n2, n3, n1: identical to n1 (31.0k / 22.2k / 13.7k) | yes / yes |
| grow-1-to-3 | PASS | 15037 / **0** | PASS / n3 PASS | all 0 (15035 on every node, n2 and n3 joined mid-run) | n2, n3: identical to n1 (12.4k / 9.7k) | yes / yes |

No merger late-event warnings and no follower stream gaps in any node log. Tests: `tests/all/firehose_startup.rs` (4 nodes join one by one under load; each node's cursor-0 subscriber attached at its start, live subscriber attached at its start, and post-hoc cursor-0 replay must equal the union of all logs in S3).

**O2 – Forwarded requests hang for up to 15 s on an owner that accepts TCP but doesn't respond** (the rest of B8). Severity: medium (availability). Files: `forward.rs` and the client in `server.rs`.

- **Affected scenarios:** peer-partition, full-partition, zombie, ctr-pause and ctr-partition all show a 12–15 s window with very few failed probes.
- **Cause:** those are requests hung on the frozen or unreachable owner. Takeover itself happens at about 3.6 s, and new requests route to the new owner.
- **Possible fix:** a time-to-first-byte deadline for buffered JSON requests (for example 3–5 s, returning 503). Streaming blob uploads need the long timeout.
- **Why it wasn't changed here:** it changes client-visible semantics (at-least-once on retry).

**O3 – Lease liveness compares wall clocks across nodes.** Severity: medium (availability only). File: `cluster.rs`.

- **Cause:** a node whose clock is behind by more than the skew margin (TTL/5) looks dead to its peers between renewals, and gets fenced repeatedly. Safety holds: SlateDB fencing plus log fencing, and the node exits 3.
- **Evidence:** `ctr-skew-large`; see above.
- **Possible fix:** peers could judge liveness by observing the lease object *change* (ETag or version), timed on their own monotonic clock. With that, no cross-node wall-clock comparison is needed.

**O4 – Renewal-RTT ceiling.** Severity: low. Validity has gaps once the renewal RTT exceeds (TTL − skew)/2 (1.2 s at TTL 3 s, 4 s at TTL 10 s). A cluster-wide S3 brownout above that fail-stops every node at once (s3-slow-all). That is inherent to sequential CAS renewals; keep the TTL at 10 s or more in production.

**O5 – Control-plane GET volume (observation).** Severity: low. Every node reads every assignment object on every step (renew/5 of TTL). At 256 shards and the production TTL of 10 s, that is about 128 GET/s per node: roughly $130/month per node on S3 Standard. That is fine at 5 nodes, but at planet scale a LIST plus an ETag cache, or a single assignment-map object, would be better.

**O6 – Graceful drain cost (observation).** Severity: low. Closing a shard writes a barrier segment and a checkpoint flush. Draining 256 shards on SIGTERM takes about 3.4 s (one segment PUT per shard). Batching the barriers would make that one PUT.

## Code changes (HA files only)

| File | Change |
|---|---|
| `src/cluster.rs` | N1, N2, N3, N4, N5 and N9, plus `acquired shards` / `releasing extra shards` logs. The unit test now keeps b renewing while a's lease runs out, since a node that stops renewing now fail-stops. |
| `src/node.rs` | N6: `purge_worker_caches` on open, and again at the end of close. |
| `src/nodelog.rs` | N7: reject entries for shards we don't hold. Also the `checkpoint start` log line. |
| `src/remote.rs` | N8: stream idle and connect timeouts (2 s). |
| `src/firehose.rs` | A diagnostic warning when the merger emits an event at or below the already-emitted watermark. It never fired. Another agent's S3 backfill also landed in this file during this work. |
| `bench/ha/*` | The harness changes above; the new scenarios `kill9-rebalance-drainer`, `kill9-rebalance-joiner`, `zombie-check`, `s3-5xx-all`, `s3-slow-one-long`, `kill9-mid-checkpoint` and `grow-1-to-3`; the Dockerfile now copies `lexicons/` and `ui/dist` (new compile-time inputs). |

`cargo test --lib`: 59 passed.
