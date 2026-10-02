## Benchbox soak notes (hand-written) — BASELINE, unfixed 3cdbfac

Binaries: `bench/benchbox/sync.sh 3cdbfac`, run from `target-prof` (`--features profiling`, `NODE_EXTRA="--pyroscope-url
http://127.0.0.1:4100"`); the generated header's "binaries /home/target/release" is wrong (the `report` call has no
BENCH_BIN). MinIO in Docker on benchbox's NVMe. Command as recommended (no changes): 4 nodes, 64 shards, 200k x 10 +
32 x 5000, 1,500 writes/s + 200 reads/s, `--procs 6`, cycle `restart:5400,calm:1800,reshard:5400,calm:1800,mixed:3600`,
retention 90 s, checkpoint lifetime / GC min age 2 m. The generator held 1,500 w/s from the first 10 s sample (6 load
procs at ~11% CPU each, 0 dropped); `--procs` was not raised.

Two windows: 02:00-06:50 UTC (0 -> 4.83 h, paused 25 min before the 07:15 batch pipeline) and 07:49-09:59 UTC
(4.83 -> 7.00 h). The resume restarts every node, so n1 ("never restarted") was restarted once at 4.83 h. History: 168
incarnations (160 restarts, 38 kill -9), 100 split/merge ops (0 failed), 37.7 M acked writes. Peak MinIO data 12 GB,
free space >= 550 GB throughout (cap 150 GB never approached).

Harness bug fixed during the run (bench/soak/soak.py, uncommitted): on pause, the load processes (which ignore SIGTERM,
so `terminate()` was a no-op) hung at exit, their final stats flush blocked on a stats queue nobody drained; the
sequential `join(60)` x 6 cost 6 min, then soak.py never exited and runner.sh's 10-min guard abort killed it
(rc 10 -> the loop still resumed correctly; state had been saved). Fix: drain `stats_q` (counting the last windows'
ops into history) while the children wind down, then `kill()` stragglers. Window 2 stopped in 10 s with it.

### Verdicts per path (unfixed baseline)

- **(a) parent pinning -> read amplification: grows with reshards; read cost up, latency hidden by cache.** Pinned
  retired bytes +2.2 GB per 100 reshards (r2 0.92; 1.84 GB at the end), live shards with external SSTs 0 -> 32-35 of
  64, external dbs per live shard 0 -> 0.56-0.64. Compaction detaches some (pinned bytes fall 0.6-0.8 GB across the
  restart/calm segments) but net growth is linear in reshards. State SST GET+HEAD per client read (calm medians):
  3.8 -> 5.2 before any reshard, 9.6 / 9.4 in the reshard hours, 8.4-9.5 after, 6.4 in the last hour; SST requests/s
  calm medians 1,096 -> 1,942 -> 854. Read p99s flat (getRecord 1.8-2.5 ms, listRecords 2.2-3.3 ms calm medians).
  The automatic "getRecord p99 26 -> 168 ms, grows with data" verdict is driven by stall windows (see below), not by
  a drifting steady state.
- **(b) retired state dirs: unbounded, linear in reshards.** Exactly 1.5 dirs per op (150 after 100) and +64 MB per op
  (6.44 GB retired vs 4.30 GB live at the end, total/live 1.0 -> 2.5). Zero change in calm/restart segments.
- **(c) assign/ records: linear in reshards, cost not yet visible.** 65 -> 215 objects (+1.5 per op), LIST response
  13.9 -> 45.3 KB (+314 B per op), still 1 page. assign/ requests/s calm medians flat at 2/s (GETs 0); the MR
  "grows with history" on the means comes from spikes right after ops.
- **(d) dead-log fences: +1.00 log id per incarnation** (168 ids for 168 incarnations, 164 fence-only, r2 1.0),
  log/ delimiter LIST +80 B per incarnation (2.0 -> 13.6 KB). LIST pages/s and backfill time-to-first-event flat. At
  this rate one LIST page (1,000 keys) is crossed after ~1,000 restarts.
- **(e) metric cardinality: flat.** n1 series 1,524 -> 1,601 (+5%, all from label combinations first touched in the
  first reshard hour; restarts add none).
- **(f) tombstones: not visible.** live bytes/record 207 -> 123 B (compaction catches up), SSTs per live shard 69-103
  (grows in calm segments, compacted down at restarts; 69 at the end), delete-heavy listRecords calm p99 medians
  2.7 -> 3.8 -> 3.2 ms.

### Generic trends

- Write latency (calm medians): p50 22.5 -> 24.5 ms (+9% over 7 h); p99 52-59 -> 75-79 ms (+35-50%, rising in each
  calm segment: 50.5 -> 70.8 -> 79.9). Object-store requests per commit 1.0 -> 1.2 -> 1.9-2.0 after the reshard phase
  -> 1.4 in the last hour. Firehose lag p99 73 -> 96 ms. The ~22 ms write p50 floor is benchbox's MinIO fsync path.
- **Restart cost grows** (not in the auto verdicts; table below): SIGTERM exit median 0.9 -> 1.25 s over the first
  60 restarts, 1.62 s after the reshard phase, 1.75 s at the end; post-restart object-store requests/commit 2.0 ->
  2.7 -> 3.6 -> 4.1; worst write p99 within 40 s of a SIGTERM restart 0.27-0.40 s -> 0.68-0.88 s. The step happened
  across the reshard-only hours (between restart 60 and 61), so it tracks reshard history (more shards with external
  dbs / pinned parents to flush and reopen per takeover). Serving 0.25 s and converged ~5 s stay flat.
- Reshard op time flat (median 0.39-0.59 s; max 3.5 s in the mixed phase).
- n1 RSS 1.2 -> 6.4 GB: cache fill, not a leak. jemalloc allocated minus the repo cache is ~3.2-3.5 GB from 0.75 h
  on (block cache ~1.9 GB + base); the repo cache filled 0.17 -> 1.7 GB of its ~1.9 GB budget. Tokio tasks flat
  (~610-650).
- Stall windows (10 s samples with write p50 > 200 ms or p99 > 1 s): 30/14/9/14/37/29/22 per soak hour. 136 of 155
  are within 40 s of a restart or reshard (kill -9 failovers show the expected 7-8 s p99 = lease TTL; SIGTERM restarts
  0.3-0.9 s). 19 are op-free; the worst (09:58:40-55) is cluster-wide object-store latency (segment PUT p50 16 ->
  200-280 ms on all four nodes at once, commits/s halved), amplified by the generator: 288 worker threads cap
  in-flight requests, so commit latency above ~190 ms backs the open-loop queue up and client p50 reaches seconds.
  Op-free stalls rose 4 -> 6 -> 8 across the three calm segments, i.e. with data/compaction I/O on the shared disk.

### vlpds bugs found

1. **Client-visible 500 during split/merge right after a restart**: `InternalServerError "repo load failed: Closed
   error: db is closed"` (103 createRecord, 5 deleteRecord, 1 sync.getRecord). Only in the `mixed` phase, where a
   split/merge runs ~1 s after a restarted node converged; the moving shard's DB is closed under in-flight repo loads
   and the error is surfaced as a 500 instead of a retryable 503/PartitionUnavailable (the load balancer and the
   generator only retry 502/503/504). Also logged as `large-repo index scan failed: Closed error` and
   `oauth gc: Closed error: db is closed`. Evidence: `evidence/n*-closed-errors.log`; full node logs on benchbox in
   `~/vlpds-bench/results/soak-2026-10-02-benchbox-nodelogs/`.
2. **OutdatedCursor inside retention** (1 of 210 probes, 0.78 h, restart phase): n3 answered a 44.7 s-old cursor
   (retention 90 s) with `outdated` and a seq gap of 8.15e9, i.e. events in range were already pruned. Suspect
   retention of a just-dead incarnation's log. Worth a targeted test.
3. (harness-side, minor) one backfill probe at 6.5 h got `ConsumerTooSlow` reading at ~87k ev/s: the Python consumer,
   not the server.

No crashes, panics, data loss or stuck shards: 100/100 reshards done, every restart converged (4.1-9.3 s), firehose
1.00 events/commit, 0 out-of-order, 0 reconnects.

### What to fix first

1. The `db is closed` 500 (map Closed to retryable PartitionUnavailable / retry the load on the new owner): the only
   client-visible failure.
2. (b) retired-parent GC, together with (a) forced detach: 64 MB + 1.5 dirs per reshard is the only unbounded storage
   growth, and pinned parents are the likely cause of the restart-cost step (exit time and post-restart
   requests/commit up ~2x). (a) first if detach makes (b)'s GC trivially safe.
3. (d) fence GC, then (c): both linear but ~1,000 ops away from costing an extra LIST page per step.
4. (e)/(f): nothing to do at this horizon.

Side note: batch pipeline's 07:15 UTC run failed with "OpenJev never came up" (the OpenJev container never answered on
:8093). The soak's nodes were stopped and only the idle MinIO container (ports 9200/9201) was up; the same 07:15 run
also failed this way on Oct 01. Probably unrelated, but flagging it.
