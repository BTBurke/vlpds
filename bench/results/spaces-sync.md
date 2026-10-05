# Spaces sync micro-bench

Status: not run yet. The harness is in place (`tests/all/spaces_side/bench.rs`), and
the numbers go here once core C1 through C4 are in and the bench has run alone on
benchbox, outside an batch pipeline window. Each table says which slices its row
needs.

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
| `SPACES_BENCH_ONLY` | `noop,delta,cred,bucket,load` | sections to run |
| `SPACES_BENCH_SECS` | 20 | each timed window of `bucket` and `load` |
| `SPACES_BENCH_REQS` | 20000 | sequential no-op polls |
| `SPACES_BENCH_DELTA` / `_DELTA_REQS` | `1,10,100` / 2000 | delta sizes K, pulls per K |
| `SPACES_BENCH_CREDS` | 200 | fresh credentials for the miss path |
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

Not run yet.

| Measurement | Needs | p50 | p99 | CPU/req |
|---|---|---|---|---|
| No-op `listRepoOps`, client | C1 | | | |
| No-op `listRepoOps`, server histogram | C1 | | | |
| Delta pull, K = 1 / 10 / 100 | C1 | | | |
| Credential miss (full chain) | C1 | | | |
| Credential hit (cache) | C2 | | | |
| Write ack to authority ack (`vlpds_space_notify_ack_seconds`) | C3 | | | |
| Write sent to syncer notified | C3, C4 | | | |
| Syncer notified to pull done | C3, C4 | | | |

| Bucket ops | Needs | Alone | With space writes |
|---|---|---|---|
| Commits/s, space writes/s | C1 | | |
| PUT/s | C1 | | |
| Added PUTs per space write | C1 | | |
| PUTs per write (public + space) | C1 | | |

| Public commit latency | Needs | p50 | p99 |
|---|---|---|---|
| Public writers alone | C1 | | |
| With the spaces load | C1 (C3, C4 for members and syncers) | | |

The cold-key case (a signing-key cache miss, which unwraps through the key
service) isn't measured here yet. It needs a test hook to drop the key cache.
