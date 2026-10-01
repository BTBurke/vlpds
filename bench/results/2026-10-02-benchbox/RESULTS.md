# vlpds Linux baseline on benchbox, 2026-10-01 (commit f81857c)

Same driver and shapes as `../2026-10-02/RESULTS.md` (the laptop numbers),
run on benchbox through `bench/benchbox/run.sh` (see `bench/benchbox/README.md`).

## Machine

- AMD Ryzen AI Max+ 395 (Framework Desktop), 16 cores / 32 threads, max
  5.19 GHz, 64 MiB L3; `amd-pstate` active, governor `powersave`, EPP
  `balance_performance`, platform profile `balanced`.
- 62 GB RAM, 7 GB swap (unused).
- Samsung SSD 970 EVO Plus 1TB (NVMe), ext4 on LVM, volatile write cache
  on. A 4 KiB `dd oflag=dsync` write takes **~6.5 ms** (every fsync flushes
  the drive cache). 8 MiB `O_DIRECT|O_DSYNC` streams at 690 MB/s.
- Ubuntu 24.04.3, kernel 6.14.0-1020-oem; Docker 29.2.0.
- Rust 1.98.1 (pinned), `cargo build --release --bins` (fat LTO, jemalloc).
  `ui/dist` is build.rs's placeholder (no node on benchbox).
- MinIO RELEASE.2025-09-07 in Docker (`--network host`, 127.0.0.1:9200), with
  data bind-mounted on the NVMe (`~/vlpds-bench/minio`). The tmpfs A/B rows
  use `--tmpfs /data` instead.
- Also running: `other services.service` (idle API, ~0 CPU) and a 7-month-old
  procmon-agent container. `batch pipeline` was inactive for every run
  (`guard.sh` checked before each run and every 10–30 s during it).
- Load generator, MinIO and every vlpds node share the box, as on the laptop.
  CPU % comes from `/proc/<pid>/stat` deltas (Linux `ps %cpu` is a lifetime
  average), so it's comparable to the laptop's `ps` numbers.
- Server flags are the defaults (8 workers, **6 IO threads**), as on the
  laptop. On 32 threads that's the cap for several results below.

## Headline vs laptop (M4 Pro, 14 cores)

| Benchmark | Laptop | benchbox (MinIO on NVMe) | benchbox (MinIO on tmpfs) |
|---|---|---|---|
| 10k/5k inj0, 10k/s | p50 1.8 p99 16.4 | p50 14.3 p99 27.3 | p50 1.6 p99 3.1 |
| 10k/5k inj0, 50k/s | p50 5.3 p99 28.3 | p50 54.0 p99 259 | p50 2.7 p99 5.0 |
| 10k/5k inj0, 75k/s | p50 41.5 p99 65.9 | **saturated, 54.7k/s** | p50 4.8–5.4 p99 8.9–9.8 |
| 10k/5k inj0 ceiling | 88k/s achieved at 100k | ~54k/s | 92.7k/s (95k/s with 16 IO threads + 16 workers) |
| 10k/5k inj25, 25k/s | p50 63.6 p99 135 | p50 90.6 p99 191 | – |
| 10k/5k inj25 ceiling | 44k/s at 50k | 31k/s at 50k | – |
| 1M/50k inj0, 50k/s | p50 4.3 p99 20.3 | p50 48.0 p99 216 | – |
| 1M/50k inj0 ceiling | 86k/s at 100k | 69k/s at 75k | – |
| 1M/50k inj25, 25k/s | p50 62.8 p99 141 | p50 92.8 p99 179 | – |
| 1M/50k inj25 ceiling | 54k/s at 75k (50k/s clean) | 38k/s at 50k | – |
| Hot repo 20k/s inj0 / inj25 | p99 22.5 / 132 | p99 77.8 / 160 | – |
| Proxy, 50k active, 128 in flight | 81.6k req/s p99 2.6 | 154.7k p99 1.6 (srv 598%) | – |
| Proxy, 50k active, 512, `--io-threads 16` | – | **204k req/s** p99 5.1 (srv 1454%) | – |
| Proxy, 1M active, 512, `--io-threads 16` | 72.8k (6 IO) | 155k p99 7.2 | – |
| 3 nodes, inj25, 50k/s | p50 59 p99 150 | p50 107 p99 372 | – |
| 3 nodes ceiling (inj0 and inj25) | ~68–70k/s at 75k | 75k/s clean (inj0), ~74k/s at 100k | – |
| createRecord, 64 in flight, inj0 | 32.2k/s p50 1.9 | 4.9k/s p50 11.9 | – |
| getRecord / describeServer | 70.9k / 86.4k | 115.6k / 200k | – |
| createAccount / createSession | 566 / 611 | 792 / 829 | – |

## Findings

1. **The disk dominates benchbox's write numbers.** The laptop's MinIO
   ran on macOS, where `fsync` doesn't flush the drive cache. Here every
   MinIO PUT pays real flushes (~6.5 ms each). Segment PUT p50 was 4–14 ms
   (laptop 0.5–5 ms), so inj0 commit latency is ~10× the laptop's, and
   the single-log ceiling (one segment PUT in flight per node log, finding 1
   on the laptop) drops to ~54k/s for 10k/5k and ~69k/s for 1M/50k. With
   MinIO on tmpfs, benchbox beats the laptop: 75k/s at p99 9.8 ms (laptop
   65.9 ms). The laptop inj0 numbers are best read as "S3 with ~1 ms PUTs";
   benchbox's NVMe rows as "S3 with ~10 ms PUTs". In both cases, the fix in
   TODO (2–4 segment PUTs in flight per log) is what moves the ceiling.
2. **The single-node ceiling is the log, not CPU or threads.** On tmpfs
   the ceiling is 92.7k/s with defaults and 94.9k/s with
   `--io-threads 16 --workers 16`, at 956% and 1277% CPU, with p50
   pinned ~210 ms at saturation both times.
3. **The proxy is IO-thread-bound.** Server CPU sat at exactly ~600% (6
   IO threads) with the defaults. With `--io-threads 16` it reaches 197–204k
   req/s (50k active) and 149–155k (1M active) at ~1450% CPU, about 2.5× the
   laptop. That's ~70 µs of CPU per request, the same as the laptop's
   ~65 µs. Size `--io-threads` to the box.
4. **Cluster:** 3 nodes (3 IO threads each) reach 75k/s clean at inj0
   (laptop saturated at ~68k). They top out at ~74k/s with each node at
   ~390% CPU, the 3 IO threads plus the 3 workers. Latency is higher than on
   the laptop (fsync PUTs).
5. **Methods:** reads are 1.4–2.3× the laptop (describeServer 200k/s,
   getRecord 116k/s, p99 ≤ 1.3 ms). Commit-producing writes at 64 in flight
   are fsync-bound (createRecord 4.9k/s at p50 12 ms). With inj25 they match
   the laptop (~0.9k/s, p50 ~65 ms). Argon2 logins: ~800/s.
6. **Hot repo:** 20k writes/s to one repo holds at inj0 p50 38 / p99 78 ms
   with 5.5 requests/commit (laptop p99 22.5). With inj25 the latencies
   match the laptop.

## Not run / caveats

- **10M/50k was not run.** Two attempts (`run-100126`, `run-100420`) aborted
  ~2.5 min into the 10M bulk, when `~/vlpds-bench` passed the 60 GB cap (69
  and 64 GB). The second attempt purged MinIO's `.trash` every 10 s, so the
  live 10M dataset itself exceeds 60 GB on MinIO. 1M/50k ran instead. 10M
  needs a cap of ~120 GB+.
- 10k/5k on tmpfs at 100k/s (`grid-tmpfs.jsonl`, last row) filled the 24 GB
  tmpfs (`XMinioStorageFull`), and the inj25 restart then failed. Ignore that
  row. The 100k ceiling numbers come from `grid-tmpfs-{default,io16w16}.jsonl`
  (32 GB tmpfs, 10 s windows).
- `grid-first-dockerproxy-nofile1024.jsonl`: the first attempt, kept for the
  record. The soft `nofile` limit was 1024: SlateDB's disk cache hit EMFILE on
  restart, and every inj25 request failed. `runner.sh` now raises it to the
  hard limit (524288). MinIO then went from Docker port publishing to
  `--network host`, with no measurable change.
- No failover/SIGTERM cluster runs, firehose, sweep or resource runs.
- The failover/repo-size sections of the laptop report have no benchbox
  counterpart yet.

## Files

`grid.jsonl` (10k/5k and 1M/50k, NVMe), `grid-tmpfs*.jsonl`, `hot.jsonl`,
`proxy.jsonl` (6 IO threads), `proxy-io16.jsonl`, `cluster.jsonl` (inj25,
then inj0), `methods.jsonl` (inj0, then the inj25 writes), and `run-*.log`
(runner output per invocation, with guard lines). Tables:
`python3 ../2026-10-02/tables.py grid $PWD/grid.jsonl`.

## Disk state on benchbox at the end

MinIO container removed. MinIO data wiped (220 KB of `.minio.sys`
metadata left). Scratch caches and logs deleted. `~/vlpds-bench` = 1.9 GB:
`target/` (release `vlpds`, `loadgen`), `src/` (f81857c), scripts and
`results/`. The root filesystem had 566 GB free at the end, unchanged from
the start.
