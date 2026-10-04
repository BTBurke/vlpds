# Release profile A/B (benchbox, 2026-10-03)

Build time versus runtime of the `[profile.release]` LTO / codegen-unit
settings. Source: d5ae192 (one tree); the variants differ only in the
profile. The shas below were throwaway commits (`ab-*` branches, not
merged) that keyed the campaign's `bin/<sha>`. Every binary was built
natively on benchbox with `RUSTFLAGS="-C target-cpu=x86-64"` (the image's
target) and `debug = 0`, under `systemd-run --user --scope -p MemoryMax=24G`.
All variants used the same loadgen.

| sha | variant | native cold | native warm (one-line change) | warm: lib frontend + lib codegen + bin (LTO/link) |
|---|---|---|---|---|
| 42c4617 | fat LTO, 1 CGU (production) | 291 s | 252 s | 24 + 88 + 139 s |
| 1d0c09a | fat LTO, 16 CGU | 261 s | 226 s | 24 + 11 + 190 s |
| 19b5c0f | thin LTO, 16 CGU | 91 s | 55 s | 24 + 11 + 20 s |
| 3d0ad75 | thin LTO, 16 CGU, x86-64-v3 | 92 s | 55 s | 24 + 11 + 20 s |
| 3cfc7b6 | no LTO, 16 CGU | 82 s | 44 s | 24 + 14 + 5 s |

Runtime: `campaign.sh bisect` with `ROUNDS=3` (grid 10k accounts / 5k
active / 25 ms injected PUT at 10k-100k commits/s, then the methods
sample), order ABCDE EDCBA ABCDE. Medians [min-max] of 3 rounds, deltas
of medians against fat/1; raw JSONL and step logs beside this file.

CPU per commit below saturation (10k, 25k/s), the cleanest efficiency
number: the spread of one variant's 3 rounds is about +-1%.

| variant | 10k/s CPU µs/commit | 25k/s CPU µs/commit | getRecord ops/s | getLatestCommit ops/s | createRecord ops/s |
|---|---|---|---|---|---|
| fat/1 | 213.8 [213.5-215.8] | 210.5 [209.3-211.1] | 76976 [76475-81162] | 83488 [83475-85023] | 5761 [5122-5914] |
| fat/16 | +0.4% | +0.4% | -1.4% [74795-76097] | -1.2% [81207-82883] | -1.5% |
| thin/16 | +3.4% | +2.5% | -3.9% | -3.2% | -11.9% |
| thin/16 v3 | +2.7% | +1.6% | -2.5% | -1.5% | -12.1% |
| off/16 | +11.2% | +10.5% | -17.0% | -15.0% | +0.4% |

Verdict: production stays fat LTO / 1 CGU. thin/16 is 4.6x faster to build
warm but costs 2.5-3.4% CPU per commit and 3-4% read throughput (outside
the noise band); fat/16 saves only ~10% of the build and its read
throughput ranges don't overlap fat/1's.
