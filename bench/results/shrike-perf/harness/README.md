# shrike-perf

Micro-benchmarks of shrike 0.7.0's hot paths: DAG-CBOR decode/encode,
JSON <-> DAG-CBOR, CIDs, MST (node codec, bulk build, single-key commits,
record proofs), K-256/P-256 signatures, lexicon validation, CAR read/write
and firehose `#commit` frame parsing.

## Build modes

The comparison implementation is not public, so the `compare` mode only
builds on the machine it came from. Every shrike number, and every A/B that
backs a suggestion (`sha2` backend, libsecp256k1, `decode_bump`,
`next_block_into`, the firehose floor, MST commit cost vs tree size), runs
without it.

```sh
# shrike exactly as a downstream crate gets it (sha2 without "asm")
cargo run --release -- --out shipped.jsonl
# shrike with sha2's "asm" feature (on aarch64: the ARMv8 SHA-256 instructions)
cargo run --release --features sha2-asm -- --out sha2asm.jsonl
# against the comparison implementation (its sha2 has "asm" on, which
# Cargo's feature unification applies to shrike's sha2 too)
cargo run --release --features compare -- --out compare.jsonl
```

Options: `--rounds N` (default 11), `--ms T` (batch target, default 60 ms),
`--mst-n N` (synthetic MST size, default 1,000,000), and substring filters on
`group/case` (`cbor.decode`, `json->cbor`, `mst.commit`, `lexicon`, ...).

Inputs (environment variables):

- `REPO_CAR`: a real repository CAR (default `~/repo.car`); any account's
  `com.atproto.sync.getRepo` output works.
- `SHRIKE_FIREHOSE`: shrike's `testdata/repo_proofs/firehose_commits/`.
- `INTEROP_TESTDATA`: atproto-interop-tests' `data-model/` directory.
- `LEXICON_BUNDLE`: a JSON object `{nsid: lexicon document}` holding every
  record lexicon of the atproto repo and the lexicons they reference.

## Method

Each comparison runs `--rounds` rounds; in each round each side runs one
batch sized to `--ms` by a calibration pass, and the order alternates
between rounds. Reported: the median per side, and the median of per-round
ratios (shrike / other) with its min..max. Large MST builds and commits are
timed per run (5-7 rounds, also alternating). System allocator for both
sides; `lto = "fat"`, `codegen-units = 1`.
