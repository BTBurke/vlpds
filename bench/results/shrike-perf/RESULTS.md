# shrike 0.7.0 vs vlpds: hot-path micro-benchmarks

Evidence for a performance-suggestions issue on
[jcalabro/shrike](https://github.com/jcalabro/shrike) (draft: [ISSUE.md](ISSUE.md)).
shrike 0.7.0 = commit
[`359f0d2`](https://github.com/jcalabro/shrike/commit/359f0d2fd0804609b5ce73b3747f69ec112f5cc9)
(there is no tag); all `S/...` links below are
`https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/...`.

## Headline

| area | shrike vs vlpds | cause in shrike | suggestion made |
|---|---|---|---|
| MST commit on a long-lived tree (1 key + flush) | **0.9 ms vs 4.3 µs at 43k keys; 55 ms vs 6.8 µs at 1M** | `flush` walks every loaded node into a fresh `HashSet` to compute `retired` | yes (1) |
| SHA-256 as shipped, aarch64 | CID compute **4.9x** slower than with `sha2/asm`; CAR verify 4.0x; MST flush 1.75x | `sha2 = "0.10"` without `asm` = software SHA on ARM | yes (2) |
| lexicon `validate_record` | post **3.75x**, like/repost 1.44x, follow 0.91x | interprets the schema per call; `format!` paths, `String` refs, `HashSet` per object | yes (3) |
| CAR `read_all` (16.6 MB repo) | **8.9-9.2x** (vs borrowed blocks) | copies every block; byte-at-a-time varint via `Read` | yes (4) |
| record proof generation | **6.8-7.0x** | re-decodes path nodes from the block source per proof | yes (5) |
| k256 sign / verify | **1.66x / 2.74x** (vs libsecp256k1) | RustCrypto `k256` | yes, optional (6) |
| DAG-CBOR -> JSON text | **1.5-2.15x** | builds a `serde_json::Value` tree first | yes (7) |
| JSON text -> DAG-CBOR | **1.35-1.56x** | parse into `serde_json::Value` (owned, BTreeMap) | yes (8) |
| firehose `#commit` frame | **2.5x** over its own floor | `String`-keyed block index, op `Value` clones, block copies | yes (9) |
| DAG-CBOR decode | shrike **2.1-2.8x faster** | borrowed `Value<'a>` (vlpds decodes to owned) | no: shrike wins |
| CID parse / to_string | shrike **3.9x / 1.55x faster** | stack buffers | no: shrike wins |
| MST node decode / encode | shrike **2.7x / 1.2x faster** | | no: shrike wins |
| MST bulk build (inserts) | shrike **1.1-1.45x faster** | | no: shrike wins |
| P-256 sign/verify | parity (same RustCrypto `p256`) | | no |
| DAG-CBOR encode | 1.1-1.3x | `io::Write` per item, sort check per map | minor note only |
| MST flush of a fully dirty tree | 1.3-1.5x | `node_to_data` builds `NodeData` + a `Vec` per key suffix | minor note only |

## Method

- Machine: Apple M4 Pro (10P + 4E cores, 48 GB), macOS 26.6.2, rustc 1.98.1.
  Shared laptop: other agents' work kept the load average at 7.5-10 during
  the runs, so every comparison is interleaved (below) and run under
  `nice -n 5`.
- Harness: [harness/](harness/) (a scratch crate; copy of
  `scratchpad/shrike-perf`). `shrike = "=0.7.0"` (features syntax, cbor,
  crypto, mst, repo, car, lexicon, streaming) and vlpds as a path dependency
  (`default-features = false`: no jemalloc). Both sides use the **system
  allocator**. `opt-level = 3`, `lto = "fat"`, `codegen-units = 1`.
  vlpds was the working tree at `3f0a2a0` (uncommitted edits only in
  cluster/main/node/forward/xrpc files, none of which the harness touches).
- Each comparison: calibrate a batch to ~60 ms per side, then 11 rounds
  (21 for the re-run of the noisier groups), each running one batch per side
  with the order alternating between rounds. Reported: median ns per unit
  per side and the **median of per-round ratios** (shrike / vlpds) with its
  min..max in the raw logs. Large MST builds: 7 (43k) / 5 (1M) runs per side,
  alternating, after a warm-up run each; commits: up to 2,000 single-key
  commits or 1.5 s per round.
- Three builds (Cargo feature unification matters for `sha2`):
  - `compare`: shrike vs vlpds. vlpds enables `sha2/asm`, which unifies onto
    shrike's `sha2` too, so the algorithmic comparisons use the same SHA
    backend on both sides.
  - `shipped`: shrike alone, exactly as a downstream crate gets it.
  - `sha2asm`: shrike alone plus `sha2/asm`.
- Inputs: `~/repo.car` (a real repo: 16.6 MB, 55,245 blocks, **43,649
  records**: 36,934 likes, 4,492 posts, 1,895 reposts, 258 follows, 1
  profile, ...; capped at 5,000 per type), the atproto interop data-model
  fixtures, shrike's own `firehose_commits` vectors (4 real `#commit`
  bodies, re-encoded as frames), and vlpds's `lexicons/bundle.json` (133
  lexicon documents) loaded into both validators. A synthetic 1M-key tree
  (60% like / 25% post / 10% follow / 5% repost keys, increasing TIDs,
  inserted shuffled) for MST scale.
- Every conversion is checked for identical output before timing (CBOR
  round-trips byte-identical; JSON <-> CBOR equal both ways; MST roots equal;
  both validators accept every timed record: 0 rejections either side).
- Raw output: [raw/](raw/) (`compare.txt`, `compare-rerun.txt`, `shipped.txt`, `sha2asm.txt`, the
  `.jsonl` files with every round, two `sample` profiles).

Ratios > 1 mean shrike is slower.

## 1. MST: per-commit cost grows with the loaded tree

| case | shrike | vlpds | ratio |
|---|---|---|---|
| 43,649 real keys: insert 1 key + root/new blocks | 906 µs | 4.31 µs | 212x |
| 43,649 real keys: delete 1 key + root/new blocks | 907 µs | 4.20 µs | 221x |
| 43,649: insert + covering proof | 918 µs | 4.42 µs | 207x |
| 43,649: same insert, tree **reopened per commit** (`DetachedTree::load` + insert + flush) | 16.8 µs | 4.31 µs | 3.9x |
| 1M synthetic: insert 1 key + root/new blocks | 55.8 ms | 6.83 µs | ~8,000x |
| 1M synthetic: delete 1 key + root/new blocks | 54.6 ms | 6.52 µs | ~8,300x |
| 1M synthetic: tree reopened per commit | 24.7 µs | 6.83 µs | 3.6x |
| 200k synthetic (shipped build): insert + flush | 7.53 ms | | |

The shrike-only evidence is the materialized-vs-reopened pair: the same
insert costs 17-25 µs on a freshly loaded tree and 0.9-56 ms on a tree that
has every node in memory (after a bulk build, or after a long-lived
`DetachedTree`/`Repo` has touched most of the tree), and it scales with the
tree (43k: 0.9 ms, 200k: 7.5 ms, 1M: 55 ms).

How shrike does it: `DetachedTree::flush`
([S/src/mst/tree.rs#L236-L262](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/mst/tree.rs#L236-L262))
writes only dirty nodes (`write_node`, L886), but then computes `retired`
by collecting the CID of **every loaded node** into a new `HashSet`
(`collect_cids`, [L915-L930](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/mst/tree.rs#L915-L930))
and diffing it against `persisted`. A `sample` profile of the 200k commit
loop ([raw/sample-mst-commit.txt](raw/sample-mst-commit.txt)): 33%
`collect_cids`, 48% SipHash of `Cid`s, 13% `HashSet::insert`, 4% set
difference, <1% SHA-256 (the actual node writes). `Repo::commit` calls
`flush` every commit
([S/src/repo/repo.rs#L378-L379](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/repo/repo.rs#L378-L379)),
and its tree lives as long as the `Repo`.

vlpds: nodes carry a `dirty` flag; writing the diff walks only dirty
subtrees, and the replaced CIDs are known from the nodes being rewritten,
so a commit costs O(depth x node size).

Suggestion: make `retired` incremental. When a clean node is first dirtied
(or dropped by a merge/split/prune), push its old CID to a
`replaced: Vec<Cid>` on the tree; `flush` reports
`replaced ∩ persisted` (minus any CID that is still reachable, which in an
MST can only be a re-created identical node, checkable against the
`new_blocks` just written) and inserts the new CIDs into `persisted`.
Expected: per-commit flush back to the reopened-tree cost or lower (tens of
µs at any size), i.e. **~50x at 43k keys and ~2,000x at 1M** for
long-lived trees. Smaller, independent win: `persisted` hashes `Cid`s (already
SHA-256 output) with SipHash; an identity/`FxHash`-style hasher on the first
8 bytes would cut the set cost even before the algorithmic fix.

## 2. SHA-256 on aarch64: software as shipped

| case (shrike only) | shipped | + `sha2/asm` | ratio |
|---|---|---|---|
| `Cid::compute`, 597 B avg post | 1.15 µs | 238 ns | **4.9x** |
| `car::verify`, 16.6 MB | 1.85 ms/MB | 457 µs/MB | **4.0x** |
| firehose `#commit` frame | 15.3 µs | 8.34 µs | 1.8x |
| MST flush, all blocks, 1M keys | 336 ns/key | 193 ns/key | 1.75x |
| MST build total, 1M keys | 3.06 µs/key | 2.43 µs/key | 1.26x |
| record proof verify | 55.2 µs | 48.0 µs | 1.15x |
| k256 sign (RFC 6979 HMAC-SHA256) | 25.8 µs | 22.7 µs | 1.13x |
| MST commit, reopened tree, 1M | 36.3 µs | 23.7 µs | 1.53x |

How shrike does it: `sha2 = "0.10"`
([S/Cargo.toml#L58](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/Cargo.toml#L58)),
no features. In `sha2` 0.10.9 the aarch64 SHA-2 instruction backend is
compiled only `#[cfg(all(feature = "asm", target_arch = "aarch64"))]`
(`sha2-0.10.9/src/sha256.rs` L19-22); otherwise it is the portable software
compressor. (On x86/x86_64 0.10 already detects SHA-NI at runtime without the
feature, so this is ARM-only: Apple silicon, Graviton, Ampere.)

Suggestion: `[target.'cfg(target_arch = "aarch64")'.dependencies] sha2 = { version = "0.10", features = ["asm"] }`
(the aarch64 path is intrinsics with runtime `cpufeatures` detection and a
software fallback; the feature also pulls `sha2-asm`, which needs a C
toolchain to build), or move shrike's own hashing to `sha2` 0.11, which
detects the aarch64 extension at runtime by default. Because Cargo unifies
features, this also speeds up the SHA-256 inside `k256`/`p256` (RFC 6979).
With the same backend, CID compute is at parity with vlpds (236.6 vs 236.8 ns).

## 3. Lexicon validation

| records (all accepted by both) | shrike | vlpds | ratio |
|---|---|---|---|
| post x4,492 | 2.92 µs | 779 ns | **3.75x** |
| like x5,000 | 593 ns | 410 ns | 1.44x |
| repost x1,895 | 581 ns | 404 ns | 1.44x |
| follow x258 | 177 ns | 192 ns | **0.91x (shrike faster)** |
| profile x1 | 2.27 µs | 232 ns | 9.8x (n=1, indicative only) |

How shrike does it: `validate_record`
([S/src/lexicon/validate.rs#L16-L62](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/lexicon/validate.rs#L16-L62))
interprets the parsed `Schema` on every call: `validate_object_inner`
builds a `HashSet` of nullable names per object
([L1028](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/lexicon/validate.rs#L1028)),
formats a path `String` for every present field and array element whether
or not it fails ([L1054](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/lexicon/validate.rs#L1054),
`child_path`/`index_path` L323-333), resolves refs and union members through
`split_ref`, which allocates the target NSID `String` for every ref tried
([S/src/lexicon/schema.rs#L367](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/lexicon/schema.rs#L367),
`union_ref_match` validate.rs L1194), then two `HashMap` lookups; `cid`
format and blob refs go through `parse_link`, which allocates (an uppercase
`String`, a `Vec`, a `to_vec` of the CID prefix:
[S/src/cbor/json.rs#L189-L223](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/cbor/json.rs#L189-L223), L290-298).
Profile of the post corpus ([raw/sample-lexicon.txt](raw/sample-lexicon.txt)):
19% malloc/free/realloc, 5% `fmt`, 9% memcmp (map lookups, ref matching),
3% `parse_link`, 4% graphemes (needed), the rest inlined into
`validate_field`.

vlpds: lexicons are compiled once into an arena: refs resolved to indexes,
union members pre-split, required/nullable precomputed per object, formats
as an enum, error text prepared; the validator carries the path as a stack
of borrowed segments and renders it only on error.

Suggestion (incremental, each independently useful): (a) build the path
lazily: pass a borrowed path stack/`&dyn Display` and format only when
pushing an error; (b) resolve refs once: at `add_schema` time (or lazily
cached), store `(nsid, def)` pairs pre-split so union matching compares
`&str`s; (c) precompute the nullable set per `ObjectDef` at load; (d) a
non-allocating CID-syntax check for the `cid` format / blob `$link`.
Expected: most of the 1.44-3.75x gap (the post/profile gap comes from
facets/embeds: unions, arrays, refs, formats, which is where (a)-(d) apply).

## 4. CAR reading

| case | shrike | vlpds | ratio |
|---|---|---|---|
| `car::read_all`, 16.6 MB, 55,245 blocks | 144-166 µs/MB | 16-18 µs/MB | **8.9-9.2x** |
| shrike only: `read_all` vs `Reader::next_block_into` (reused buffer) | 152 µs/MB | 54 µs/MB | 2.8x |
| `car::verify` (hash every block), same SHA backend | 464-477 µs/MB | 363-377 µs/MB | 1.27x |
| `car::write_all` | 38-39 µs/MB | 31-33 µs/MB | 1.2x |

How shrike does it: `read_all` takes `impl Read`
([S/src/car/mod.rs#L55-L65](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/car/mod.rs#L55-L65));
each block's varint is read one byte per `read` call
([S/src/car/reader.rs#L219-L238](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/car/reader.rs#L219-L238))
and its data is copied into an owned `Vec` (`read_to_end`, L75-109), so a
repo CAR in memory costs one allocation + copy per block.

vlpds: `read_car(&[u8]) -> (roots, Vec<(Cid, &[u8])>)`, slice indexing with
explicit bounds checks against the remaining length.

Suggestion: a slice API next to the streaming one, e.g.
`car::read_slice(&[u8]) -> Result<(Vec<Cid>, Vec<BlockRef<'_>>), CarError>`
(or an iterator of borrowed blocks), with the varint decoded from the slice
(`varint_from_slice` already exists, reader.rs L242). Most callers have the
whole CAR in memory (getRepo/sync responses, `#commit` blocks, proofs).
Expected: up to ~9x for `read_all`-style use; also lets `open_proof`,
`parse_commit_blocks` and `Repo::load_car` stop copying.

## 5. Record proof generation

| case (real repo, 43,649 keys, 1,000 random keys) | shrike | vlpds | ratio |
|---|---|---|---|
| build a getRecord proof CAR | 10.5 µs | 1.51-1.54 µs | **6.8-7.0x** |

How shrike does it: `record_proofs_car`
([S/src/repo/proof.rs#L220-L262](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/repo/proof.rs#L220-L262))
reads and decodes the commit (`Commit::from_cbor`), opens a fresh
`DetachedTree::load`, decodes every node on the key's path from the block
source through a `Recorder` that copies each block (`Cow::into_owned`),
then copies them again into `Block`s for `write_all`. `Repo::record_proof`
goes through the same function even though the `Repo` holds a loaded tree.
(Not profiled; from code reading.)

vlpds: proofs come from the live in-memory tree; internal nodes keep their
encoded bytes (`Arc<[u8]>`) from the last write, so the path's blocks are
copied straight into the CAR (leaves re-encode).

Suggestion: let a loaded `DetachedTree` (and `Repo`) produce the path
blocks itself (e.g. `DetachedTree::path_blocks(key) -> Vec<(Cid, Cow<[u8]>)>`)
by keeping each clean node's encoded bytes from `write_node`/load (an
`Option<Arc<[u8]>>` per node, as the CID is already cached), and write the
CAR directly from borrowed slices. Expected: ~5-7x for repeated proofs from
a live repo (sync.getRecord, multi-record proofs).

## 6. K-256 signatures

| case | shrike (RustCrypto `k256` 0.13.4) | libsecp256k1 (`secp256k1` 0.30) | ratio |
|---|---|---|---|
| sign, 150 B message | 22.6 µs | 13.6 µs | **1.66x** |
| verify, parsed key | 35.5 µs | 12.9-13.0 µs | **2.74x** |
| vlpds `verify_k256` (re-parses the SEC1 key per call) | | 15.3-16.8 µs | 2.3x |
| record proof verify (CAR + sig + MST lookup), vs vlpds | 46.8-47.1 µs | 36.9-37.4 µs | 1.27x |

How shrike does it: `K256SigningKey::sign` / `K256VerifyingKey::verify`
wrap `k256` prehash signing/verification
([S/src/crypto/k256_impl.rs#L73-L84](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/crypto/k256_impl.rs#L73-L84),
[L105-L123](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/crypto/k256_impl.rs#L105-L123)).
Signatures are byte-identical (both RFC 6979 + low-S; asserted in the harness).

Suggestion: an optional `libsecp256k1` feature backing `K256SigningKey` /
`K256VerifyingKey` with the `secp256k1` crate (pure-Rust `k256` stays the
default and the wasm path). Tradeoff: a vendored C dependency built with
`cc`. Expected: 1.7x signing, 2.7x verification; ~1.3x on proof
verification; the firehose/sync verifier is dominated by commit signature
checks, so it gains the most.
P-256: shrike's wrappers are at parity with calling `p256` directly
(86.6 vs 86.1 µs sign, 148 vs 147 µs verify); nothing to suggest.

## 7. DAG-CBOR -> JSON

| records | shrike `drisl_to_json` + `serde_json::to_vec` | vlpds `write_json` | ratio | shrike `drisl_to_json` alone vs vlpds |
|---|---|---|---|---|
| post x4,492 | 1.69 µs | 786 ns | **2.15x** | 1.50x |
| like x5,000 | 583 ns | 318 ns | 1.82x | 1.27x |
| repost x1,895 | 581 ns | 325 ns | 1.77x | 1.27x |
| follow x258 | 331 ns | 223 ns | 1.50x | 1.12x |
| profile x1 | 1.71 µs | 806 ns | 2.12x | 1.50x |

(21-round re-run; first run agrees within ~10%.)

How shrike does it: `drisl_to_json`
([S/src/cbor/json.rs#L62-L90](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/cbor/json.rs#L62-L90))
decodes to `Value`, then builds a `serde_json::Value` (an owned `String` per
key and text, a `BTreeMap` per object, a `String` per `$link`/`$bytes`);
serving it means a second pass to serialize.

Suggestion: add a streaming transcoder, e.g.
`drisl_to_json_writer(bytes: &[u8], out: &mut impl Write)` (or into a
`Vec<u8>`): one recursive pass over the CBOR that writes JSON directly:
JSON-escape text, `{"$link":"b..."}` via the existing stack base32 path,
`{"$bytes":"..."}` base64 into the output buffer; since DRISL keys are
already in canonical order and unique, no map is needed. Keep
`drisl_to_json` for callers that want a tree. Expected: 1.5-2.15x for
getRecord/listRecords-style output (vlpds saw 5.8x on its own getRecord path
from this change, but that included its tree's owned-string decode, which
shrike doesn't have).

## 8. JSON text -> DAG-CBOR

| records | end-to-end (parse text + encode) | parse only | encode pre-parsed |
|---|---|---|---|
| post x4,492 | **1.35x** (1.49 µs vs 1.10 µs) | 1.58x | 1.05x |
| like x5,000 | 1.48x | 1.89x | 1.11x |
| follow x258 | 1.55x | 2.00x | 1.07x |
| repost x1,895 | 1.56x | 1.91x | 1.11x |
| profile x1 | 1.11x | 1.55x | **0.74x (shrike faster)** |

How shrike does it: `json_to_drisl(&serde_json::Value, ..)`
([S/src/cbor/json.rs#L55-L59](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/cbor/json.rs#L55-L59),
`encode` L119-150) takes an already-parsed `serde_json::Value`, so a caller
holding request bytes first builds an owned tree (`String` per key/text,
`BTreeMap` per object); `encode` then collects and sorts each map
(L139-140). The encode step itself is at parity (1.05-1.11x; faster on the
profile).

vlpds: parses with a serde visitor into a borrowed tree (`Cow<'a, str>`,
objects as `Vec<(key, value)>` sorted into canonical CBOR order once, at
parse time), then one encode pass that also collects blob refs.

Suggestion: `json_slice_to_drisl(&[u8], Integers) -> Result<Vec<u8>, _>`
that deserializes into a borrowed intermediate (or drives the encoder from a
serde `Visitor` directly, buffering only object entries to sort them).
Expected: ~1.35-1.55x on record writes (createRecord/applyWrites bodies). The
parse half is where the time is.

## 9. Firehose `#commit` frames

| case (4 real frames, 6.5 KB avg) | `parse_firehose_frame` | floor (same build) | ratio |
|---|---|---|---|
| with `sha2/asm` | 8.34-8.53 µs | 3.29-3.37 µs | **2.5x** |
| shipped (software SHA) | 15.3 µs | 10.2 µs | 1.5x |

The floor is the generic work any decoder must do, built from shrike's own
primitives: decode header and body, walk the `blocks` CAR with
`next_block_into`, hash every block. (vlpds has no frame decoder, so this is
shrike vs shrike; it is reproducible without vlpds.)

How shrike does it: `parse_commit_blocks` copies every block out of the CAR
(`read_all`) and indexes them by `cid.to_string()` in a
`HashMap<String, Vec<u8>>`
([S/src/streaming/mod.rs#L325-L361](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/streaming/mod.rs#L325-L361));
`parse_commit_ops` deep-clones each op `Value` (`item.clone()`,
[L394](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/streaming/mod.rs#L394)),
formats the op's CID to a string to look it up, and clones the record bytes
again (`.cloned()`, [L418-L419](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/streaming/mod.rs#L418-L419)).

Suggestion: index blocks as `HashMap<Cid, &[u8]>` (or a small `Vec` with
linear search: commits carry a handful of blocks) over the borrowed CAR
(see 4), match ops by reference instead of cloning, and move each record's
bytes out once. Expected: most of the 2.5x over the floor.

## Where shrike is already as fast or faster (no suggestion)

| case | shrike | vlpds | ratio |
|---|---|---|---|
| DAG-CBOR decode, post x4,492 | 365 ns | 764 ns | **0.48x** |
| DAG-CBOR decode, like / follow / repost | 70-117 ns | 174-279 ns | 0.40-0.41x |
| DAG-CBOR decode, interop fixtures | 196 ns | 436 ns | 0.45x |
| shrike `decode_bump` vs `decode` (post) | 248 ns | (370 ns) | 1.49x faster still |
| CID parse (`Cid::from_str`) | 27 ns | 106 ns | **0.25x** |
| CID `to_string` | 64 ns | 100 ns | 0.64x |
| CID compute (same SHA backend) | 237 ns | 237 ns | 1.00x |
| `$link` parse (`json::parse_cid`) | 115 ns | 107 ns | 1.07x (allocates; see 3d) |
| MST node decode, 11,595 real nodes | 476 ns | 1.28 µs | **0.37x** |
| MST node encode | 70 ns | 83 ns | 0.85x |
| MST bulk build, inserts (43k real / 1M) | 0.89 / 2.70 µs/key | 1.29 / 3.06 µs/key | **0.69x / 0.88x** |
| MST bulk build, total incl. root | 1.01 / 2.91 µs/key | 1.36 / 3.23 µs/key | 0.74x / 0.90x |
| lexicon, follow | 177 ns | 192 ns | 0.91x |
| JSON -> CBOR encode step, profile | 598 ns | 810 ns | 0.74x |
| P-256 sign / verify | 86.6 / 148 µs | 86.1 / 147 µs | 1.00x |

Near-misses, not suggested: DAG-CBOR encode is 1.1-1.3x slower than
vlpds's direct `Vec` pushes (shrike goes through `io::Write` per item and
re-checks key order per map,
[S/src/cbor/mod.rs#L113-L157](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/cbor/mod.rs#L113-L157)),
and the root computation of a fully dirty tree is 1.3-1.5x slower
(`node_to_data` allocates a `NodeData` and a `Vec` per key suffix before
encoding, [S/src/mst/tree.rs#L933-L964](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/mst/tree.rs#L933-L964));
both are small next to the items above and are mentioned only in passing.

## Caveats

- One machine (aarch64). The `sha2` finding (2) is ARM-specific; the others
  are algorithmic/allocation-bound and should carry over to x86_64, but
  absolute numbers will differ.
- vlpds's verify/proof paths do some extra work (multibase decode per call,
  JSON conversion of the record, a lexicon-type check that fails at the
  very end on these app.bsky records after all proof steps ran), so its
  proof-verify numbers are conservative.
- The 1 profile record (`profile x1`) is a single input; treat it as
  indicative.
- The comparison implementation is not public, so the issue relies on
  shrike-only A/Bs wherever possible: (1) materialized vs reopened tree and
  scaling with size, (2) `shipped` vs `sha2asm` builds, (4) `read_all` vs
  `next_block_into`, (6) `k256` vs the `secp256k1` crate, (9) frame vs floor.
