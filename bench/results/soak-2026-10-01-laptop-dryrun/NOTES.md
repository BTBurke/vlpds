## Dry-run notes (hand-written)

Laptop (M-series), HEAD 3cdbfac built in isolation (git archive + own CARGO_TARGET_DIR), native MinIO shared with
the cost-model lane's benchmark (so `minio.*` counters include its traffic; use the `req.*` wrapper counters).
25 min of soak clock is far too short for attribution between the three history counters (reshards~commits r =
0.95); the phase-segment table is the reliable view here. Early signals per suspected path:

- **(a) parent pinning -> read amplification: present, read cost inconclusive.** After 19 reshards 9-10 of 16-17
  live shards still reference a parent (external dbs per live shard ~1, max 2), and 29-37 MB of retired dirs are
  still referenced by live manifests: size-tiered compaction did not detach children within 25 min. SST
  GET+HEAD per client read rose 1.6 -> 2.4 -> 1.5 -> 3.1 across calm segments (noisy; the block cache hides most
  of it at this size); read p99s flat/falling. Needs the long run with a bigger dataset than the block cache.
- **(b) retired state dirs: grows with reshard ops, linearly.** +1.5 dirs per reshard (28 dirs after 19 ops), all
  of the growth inside reshard segments; retired bytes 67 MB vs 51 MB live (total/live 1.0 -> 2.3). Unbounded at
  HEAD: no retired-parent GC.
- **(c) retired assign/ records: grows with reshard ops.** assign/ 17 -> 46 objects (+1.5 per op), LIST response
  +215 B per op; still 1 LIST page, so per-step request counts are flat until ~1,000 shard ids. Grows only in
  reshard segments.
- **(d) dead-log fences: grows with incarnations, exactly +1 log id per restart** (22 ids for 22 incarnations;
  16 are fence-only, the rest wait for retention). log/ delimiter LIST +~45 B per incarnation; backfill time to
  first event flat (< 0.3 s). Pages flat until ~1,000 incarnations, then every list_logs (backfill, retention)
  pays an extra LIST page per 1,000 restarts.
- **(e) metric cardinality: flat at HEAD** (retention metrics are labelled own/dead, not per log). n1's series
  1,496 -> 1,587, growth from label combinations first touched early; 1,583-1,587 over the last 10 min (new series are now logged per sample).
- **(f) tombstones: not visible yet.** listRecords on delete-heavy repos flat (~5-8 ms p99); live bytes/record
  fell (compaction caught up). SSTs per live shard rose 27 -> 72 (grows in calm segments, drops at restarts),
  and manifest objects grow in calm segments: compaction/GC lag under the 1 min checkpoint/GC windows, not
  tombstones per se. Watch on benchbox.
- **Other:** write p50 ~3 ms / p99 ~12-14 ms in every calm segment (flat); firehose 1.00 events per commit, 0
  out-of-order, lag p99 ~13-15 ms; restart -> converged 4-6 s throughout (no growth); reshard op time 0.04 s
  early vs 0.1-0.6 s late (small sample, watch). n1 (never restarted) RSS climbed 67 -> 745 MB over 20 min then
  fell to 117 MB with no restart (cache/ring fill and release, jemalloc stats are now sampled to tell which).
  Object-store requests per commit ~2-3 in calm samples, spikes to 7+ around restarts (replay + opens).
