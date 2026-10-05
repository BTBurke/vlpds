# Spaces sync micro-bench

Run on 2026-10-05 at `spaces-1` 3ebf852d (C1 through C7 plus the side lane), alone on
benchbox (Ryzen AI Max+ 395, 16C/32T, 62 GB, rustc 1.99.0) between batch pipeline runs,
under a 16 GB memory cap. Every target is met and nothing needed fixing.

## How to run

```text
cd packages/vlpds
systemd-run --user --scope -p MemoryMax=16G env CARGO_TARGET_DIR=<target> \
  cargo test --profile dev-release --features bench-jemalloc --test all \
  spaces_side::bench::spaces_microbench -- --ignored --nocapture --test-threads=1
```

`just spaces-microbench` runs the same thing. It's one in-process node with
`--spaces` on and an in-memory bucket, and its log segment PUTs are delayed like S3
(`SPACES_BENCH_PUT_MS`, 25 ms median, lognormal sigma 0.3). Metrics are
process-wide, so nothing else may run in that test binary at the time.

Knobs (env):

| Variable | Default | What |
|---|---|---|
| `SPACES_BENCH_ONLY` | `noop,delta,cred,conc,bucket,load` | sections to run |
| `SPACES_BENCH_SECS` | 20 | each timed window of `bucket` and `load` |
| `SPACES_BENCH_REQS` | 20000 | sequential no-op polls |
| `SPACES_BENCH_DELTA` / `_DELTA_REQS` | `1,10,100` / 2000 | delta sizes K, pulls per K |
| `SPACES_BENCH_CREDS` | 200 | fresh credentials for the miss path |
| `SPACES_BENCH_CONC` | `1,4,16` | concurrent writers on one account (`conc`) |
| `SPACES_BENCH_N` / `_M` / `_K` | 20 / 5 / 3 | spaces, members per space, pollers per space |
| `SPACES_BENCH_SYNCERS` | 1 | registered syncers per space, each pulling on every notify |
| `SPACES_BENCH_POLL_MS` / `_WRITE_MS` | 1000 / 500 | poll interval, space write interval per writer |
| `SPACES_BENCH_PUBLIC` | 16 | public writers posting back to back |

Members (`_M` > 0) need simplespace `putMember` (C3), and syncers need
`registerNotify` and fan-out (C3, C4). With only C1 in, run with
`SPACES_BENCH_M=0 SPACES_BENCH_SYNCERS=0`.

## What each number means

- No-op poll: `listRepoOps` with `since` at the head. The client loop is
  sequential over loopback. Server CPU per request is the process CPU of that
  loop minus the same loop against `/xrpc/_health`, so the client's share and
  the HTTP stack mostly cancel out. The server histogram
  (`vlpds_space_list_repo_ops_seconds{path="noop"}`) is handler time after auth.
- Delta pull: `listRepoOps` returning the last K ops with values, so a scan of K
  `sO` rows joined to `sR`.
- Credential miss vs hit: the first use of each of 200 fresh credentials, then
  one credential reused. Before C2 there's no cache and both rows verify the
  whole chain.
- One repo, C concurrent: C writers on one account back to back for half a
  window, public `createRecord` and then space `createRecord`, with log segment
  PUTs per write. A space write should share segments the way a public one does.
- Bucket ops per space write: object-store requests by op and component
  (`vlpds_object_store_requests_total`) while 16 public writers run, first alone
  and then with space writes on top. Added PUTs per space write is the PUT rate
  difference over the space write rate. That moves with the public commit rate,
  so the bench also prints PUTs per write of either kind, which drops when space
  writes share segments with public ones.
- Public commit p99 under load: client-side `createRecord` latency for the
  public writers, alone and then with N x M space writers, K pollers per space
  and the syncers' notify-driven pulls running.

## Targets and the reference baseline

The targets are from the phase 1 brief. The reference column is the Mac
harness's client-side numbers for the reference PDS at 5b95b2f2.

| | Target | Reference PDS |
|---|---|---|
| No-op poll | well under 1 ms of server time | 6.7 ms p50 |
| Delta pull | a few ms | 8.3 ms |
| Notify (write to syncer) | | 4.5 ms p50, 8.3 ms p99 |
| Added bucket PUTs per space write | 0 | |
| Public commit p99 under space load | unchanged | 29 ms to 39 ms |

## Results

One full run with the defaults, then `SPACES_BENCH_ONLY=load SPACES_BENCH_SECS=30`
three more times (the public p99 rows) and `SPACES_BENCH_ONLY=conc` (the one-repo
rows, which came later). Log segment PUTs are delayed 25 ms median, so any write's
latency is mostly waiting for its segment.

| Measurement | p50 | p99 | CPU/req |
|---|---|---|---|
| No-op `listRepoOps`, client | 0.165 ms | 0.288 ms | 178 µs process, ~143 µs above `_health` |
| No-op `listRepoOps`, server histogram | 0.05 ms (mean 0.014 ms) | 0.099 ms | |
| No-op `listRepoOps`, client, cold signing key | 0.195 ms | 0.636 ms | |
| Delta pull K = 1, client | 0.180 ms | 0.398 ms | 204 µs |
| Delta pull K = 10, client | 0.216 ms | 0.732 ms | 277 µs |
| Delta pull K = 100, client | 0.522 ms | 0.927 ms | 561 µs |
| Delta pull K = 1 / 10 / 100, server histogram | 0.05 / 0.05 / 0.30 ms | 0.10 / 0.19 / 0.42 ms | |
| Credential miss (full chain), 200 fresh | 0.298 ms | 0.449 ms | 317 µs |
| Credential hit (cache), 1,000 | 0.241 ms | 0.519 ms | 261 µs |
| Polls under the load (all deltas), client | 0.44 ms | 1.0 ms | |
| Member write ack to authority ack (`vlpds_space_notify_ack_seconds`) | 52 ms | 125 ms | |
| Member write sent to syncer notified | 93 ms | 142 ms | |
| Syncer notified to pull done | 1.9 ms | 5.5 ms | |

CPU/req is process CPU (client and server share it) over the loop. The cache counter
showed 200 misses then 1,000 hits, so the two credential rows measure what they say.
The hit row costs more CPU than the no-op row above it although it's the same request.
It's 1,000 requests right after the miss phase against 20,000, so that's noise.

Write ack to authority ack is one more durable entry (the authority's `sW`/`sQ`
entry), so it costs what a public commit does (52 ms against 54 ms here). Write sent to
syncer notified is two of them in a row, ~93 ms.

| Bucket ops (16 public writers, 20 s windows) | Alone | With space writes |
|---|---|---|
| Commits/s, space writes/s | 285.5, 0 | 288.9, 219.9 |
| PUT/s (all) | 40.79 | 39.60 |
| Log segment PUT/s | 35.69 | 36.15 |
| Added PUTs per space write | | -0.005 (0 within noise) |
| PUTs per write (public + space) | 0.143 | 0.078 |

| One repo, C concurrent writers | Public PUTs/write | Space PUTs/write | Public p50 / p99 | Space p50 / p99 |
|---|---|---|---|---|
| 1 | 1.000 | 1.000 | 28.0 / 52.0 ms | 27.7 / 51.8 ms |
| 4 | 0.500 | 0.501 | 55.4 / 83.4 ms | 53.9 / 85.7 ms |
| 16 | 0.125 | 0.126 | 52.3 / 89.4 ms | 52.3 / 85.4 ms |

Space writes to one repo share segments exactly like public ones. The Mac harness's
1.0 PUT per write at a concurrency of 4 (docs/spaces/operating.md) doesn't show up
in process, so it's something about that harness's client or MinIO, not the write path.

| Public commit latency (16 writers) | Alone p50 / p99 | With the spaces load p50 / p99 |
|---|---|---|
| Run 1 (20 s) | 54.5 / 80.3 ms | 53.4 / 87.4 ms |
| Run 2 (30 s) | 54.2 / 85.6 ms | 53.1 / 84.6 ms |
| Run 3 (30 s) | 53.9 / 85.3 ms | 54.5 / 82.6 ms |
| Run 4 (30 s) | 53.7 / 85.9 ms | 53.6 / 85.3 ms |

The load is 20 spaces x 5 members x 3 pollers a second, one syncer per space pulling
on every notify, and a space write every ~0.5 s per writer (~220 space writes/s,
~5,500 notifies in 30 s, all delivered, no retries). Run 1's +7 ms p99 didn't repeat
in the next three runs, so public commit latency is unchanged.

### Targets

| | Target | Measured | |
|---|---|---|---|
| No-op poll | well under 1 ms of server time | ~0.014 ms handler mean, ~143 µs CPU end to end, 0.29 ms p99 client | met |
| Delta pull | a few ms | 0.4 / 0.7 / 0.9 ms p99 client for 1 / 10 / 100 ops | met |
| Added bucket PUTs per space write | 0 | -0.005, and 0.50 vs 0.50 at 4 on one repo | met |
| Public commit p99 under space load | unchanged | 85.6 / 85.3 / 85.9 ms alone vs 84.6 / 82.6 / 85.3 ms with | met |

The flag-off comparison against main (the benchbox A/B grid) is in
`docs/spaces/operating.md` under "What it costs".

### createAccount under the benchbox methods sample

One thing in that A/B isn't explained yet. In the methods sample, `createAccount` (64 in
flight, right after the `createRecord` phase) has a second mode at ~250-300 ms. It showed up in
4 of 4 runs of `3ebf852d` (p99 286 / 298 / 247 / 227 ms, 850-1,040 accounts/s) and 1 of 4 runs of
`36f0be7b` (p99 290 ms, the other three ~70 ms at ~1,125/s). Run first, without the phases before
it, `createAccount` matched across six alternating runs (p99 72-93 ms, ~1,080/s each). Since main
shows the same mode, it's not something `--spaces` code adds to account creation directly, but
it's worth a profile before the PR (`methods-abba/` and `create-account/` in
`spaces1-ab-2026-10-05`).
