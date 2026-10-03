# SST metadata cache under bulk import (laptop, 2026-10-02)

Reproduces the 100M capacity run's collapse (benchbox-2026-10-02-round2 block 6:
bulk 78k -> 3k accounts/s past ~75M, 1-2.4 GB/s of ~800 KB SST GETs per
node) at laptop scale by shrinking the metadata cache, and measures the fix.

Setup (`repro.py`): one native node, 4 shards (so each shard holds as much as
a 100M-account shard would hold at ~6M accounts), local MinIO, `--block-cache-mb
64`, bulk in 500k-account chunks of the capacity test's records distribution
(`real`, scale 128, knee 2: ~5.3 records per account), concurrency 4. Per
chunk: accounts/s, SST GET bytes per created account (`vlpds_object_store_bytes_total{dir="down",component="state_sst"}`,
compaction reads included), filter misses. A shared laptop (load average
~40 from other work): throughput is noisy, GET bytes are not.

- `base`: f602430 (per-CLOCK-shard budget: 64 shards x 1 MiB floor = 64 MiB).
- `new2`: cache-wide CLOCK budget, single-flight misses, compaction output
  seeded into the cache (fork 2a79077), 256 MiB SSTs, 64 MiB meta cache.
- `new3`: new2 + 64 MiB compacted SSTs + f78c1cb (warm on open) with the
  warm capped to the cache's free room. `new3-64`: 64 MiB meta cache;
  `new3-fit`: 1 GiB.

| accounts | base | new2 (64 MiB) | new3 (64 MiB) | new3 (1 GiB) |
|---|---|---|---|---|
| 1.0M | 19.7k/s, 5.3 KB | 19.2k/s, 5.0 KB | 9.5k/s, 4.5 KB | 36.5k/s, 2.0 KB |
| 1.5M | **3.4k/s, 513 KB** (330k filter misses) | 14.0k/s, 28 KB | 3.7k/s, 23 KB | 7.4k/s, 7.8 KB |
| 2.0M | **~700/s, bulkCreate timed out** | 4.9k/s, 121 KB | 6.7k/s, 19 KB | 13.1k/s, 3.6 KB |
| 2.5M | - | 4.2k/s, 95 KB | 5.9k/s, 34 KB | 13.6k/s, 3.5 KB |
| 3.0M | - | 3.3k/s, 233 KB | 1.8k/s, 170 KB | 23.5k/s, 4.6 KB |

(Accounts/s of the chunk ending there; SST GET bytes per created account.) `new3-fit` fetched no filter or index during the bulk: what it reads is
data blocks (the 64 MiB block cache) and compaction input.

Footprint (`new3-fit` restarted on its 3M-account prefix: the open-time warm
loads every SST's filter + index): 86.6 MB decoded in the cache for 67.7 MB
encoded (`vlpds_sst_meta_bytes`: filters 57.9 MB, indexes 9.8 MB), i.e.
**~29 MB decoded per million accounts**, ~1.3x encoded. At 64 MiB the 1.5M+
chunks are past the cache (~44 MB at 1.5M plus compactions in flight and
L0s), so `new2`/`new3-64` show overflow behaviour, not the fixed cliff: the
per-shard budget made `base` collapse well before that (513 KB/account at
1.5M, 18x `new2`). Projection for the benchbox 100M layout (4 nodes x 16 shards):
25M accounts per node -> ~0.72 GB, ~0.96 GB with one node down, against the
0.9 GB the benchbox flags gave (`--block-cache-mb 3612` / 4).

Steady state (`steady.py`, default caches, 64 shards, 2000 accounts x 50
records, 3k writes/s open loop then closed-loop reads; ABAB, `steady.out`):
writes p50 4.6-5.9 ms (base) vs 5.3-6.9 ms (new3), p99 33-53 vs 38-60 ms;
getRecord 14.0-14.2k/s (base) vs 22.3-24.9k/s (new3); listRecords /
describeRepo swing 2-3x between runs of the same binary. No regression
visible above the noise of the shared machine.

Files: `chunks.jsonl` (one line per chunk, all runs), `steady.out`,
`repro.py`, `steady.py`, `summ.py`.
