# Performance suggestions from benchmarking 0.7.0 (MST flush, sha2 on aarch64, lexicon, CAR, proofs, ...)

Hi! Thanks again for shrike. I benchmarked 0.7.0's hot paths on real data
against an independent atproto implementation in Rust, and wanted to pass on
the places where shrike has a measured gap and a fix that looks like it fits
shrike's API. Biggest first; each one has a shrike-only reproduction, so you
don't need the other implementation to check it.

**Setup:** Apple M4 Pro, macOS 26.6, rustc 1.98.1, `lto = "fat"`,
`codegen-units = 1`, system allocator on both sides. Every comparison is
interleaved (11-21 alternating rounds, medians reported). Inputs: a real
repo CAR (16.6 MB, 43,649 records: likes, posts, reposts, follows), your
`firehose_commits` vectors, the interop data-model fixtures, and the atproto
lexicons. Outputs were checked identical before timing. The benchmark harness builds against the other (not yet public)
implementation, so it isn't linked here, but I'm happy to share it; the
shrike-only checks in each section reproduce the shrike numbers on their own
(for item 2, toggling `sha2`'s `asm` feature is the A/B).

Ratios are shrike time / other time.

## 1. `DetachedTree::flush` is O(loaded nodes), so commits on a long-lived tree get slow

| one-key commit (insert + `flush`) | shrike | other impl |
|---|---|---|
| 43,649 keys, tree fully in memory | **906 µs** | 4.3 µs |
| 1M keys, tree fully in memory | **55.8 ms** | 6.8 µs |
| same insert on a freshly `load`ed tree (43k / 1M) | 16.8 / 24.7 µs | |

You can see it with shrike alone: the same commit costs ~17-25 µs on a freshly
loaded tree, and 0.9 ms (43k), 7.5 ms (200k) and 55 ms (1M) once every node is
loaded (after a bulk build, or once a long-lived `Repo`/`DetachedTree` has
touched most of the tree). Delete and `covering_proof` commits behave the same.

`flush` only writes dirty nodes, but to compute `retired` it collects the CID
of every loaded node into a new `HashSet` (`collect_cids`) and diffs it
against `persisted`
([tree.rs L236-262](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/mst/tree.rs#L236-L262),
[L915-930](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/mst/tree.rs#L915-L930)).
A profile of the 200k commit loop: ~33% `collect_cids`, ~48% SipHash of
`Cid`s, ~13% `HashSet::insert`, <1% hashing the new nodes.

**Sketch:** track replaced CIDs as they happen. When a clean node is first
dirtied, or dropped by a merge/split/prune, push its old CID onto a
`replaced: Vec<Cid>`; `flush` reports the ones in `persisted` as `retired`
(minus any re-created identical node, which you can check against the
`new_blocks` just written) and adds the new CIDs. That makes a commit
O(depth x node size) again: **~50x at 43k keys and ~2,000x at 1M** for
long-lived trees. Separately, `Cid` is already a SHA-256 digest, so a
non-SipHash hasher for these sets (e.g. the first 8 bytes) would help
wherever CID sets remain.

## 2. SHA-256 runs in software on aarch64 as shipped

`sha2 = "0.10"` has no features
([Cargo.toml L58](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/Cargo.toml#L58)).
In `sha2` 0.10 the ARMv8 SHA-2 instructions are only used with the `asm`
feature (`sha2-0.10.9/src/sha256.rs` L19). Without it you get the portable
compressor on Apple silicon, Graviton and Ampere. On x86_64, 0.10 already
detects SHA-NI at runtime, so this only affects ARM.

| shrike only | as shipped | with `sha2/asm` | |
|---|---|---|---|
| `Cid::compute`, 597 B post | 1.15 µs | 238 ns | **4.9x** |
| `car::verify` | 1.85 ms/MB | 457 µs/MB | 4.0x |
| `parse_firehose_frame` (`#commit`) | 15.3 µs | 8.3 µs | 1.8x |
| MST flush of 1M fresh keys | 336 ns/key | 193 ns/key | 1.75x |
| k256 sign (RFC 6979 uses SHA-256) | 25.8 µs | 22.7 µs | 1.13x |

**Sketch:**
`[target.'cfg(target_arch = "aarch64")'.dependencies] sha2 = { version = "0.10", features = ["asm"] }`.
On aarch64 that path uses intrinsics with runtime detection and a software
fallback, but it also builds `sha2-asm`, which needs a C toolchain. The
alternative is `sha2` 0.11, which detects the extension at runtime by
default. Because Cargo unifies features, `k256`/`p256` pick it up too.

## 3. Lexicon validation: a lot of allocation per field

| `validate_record` (all records valid) | shrike | other impl | |
|---|---|---|---|
| post x4,492 | 2.92 µs | 779 ns | **3.75x** |
| like x5,000 / repost x1,895 | 593 / 581 ns | 410 / 404 ns | 1.44x |
| follow x258 | 177 ns | 192 ns | 0.91x (shrike faster) |

On the success path, `validate_object_inner` builds a `HashSet` of nullable
names per object
([L1028](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/lexicon/validate.rs#L1028)).
It also `format!`s a path for every present field and array element
([L1054](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/lexicon/validate.rs#L1054)).
Every ref and union member tried goes through `split_ref`, which allocates
the NSID ([schema.rs L367](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/lexicon/schema.rs#L367)),
and the `cid` format and blob refs go through the allocating `parse_link`.
In a profile of the post corpus, about 19% of the time is malloc/free and
about 5% is formatting.

**Sketch** (each piece is independently useful):
- (a) Build the path lazily: pass a borrowed path stack and render it only when pushing an error.
- (b) Resolve refs and union members once, at `add_schema` or cached, as pre-split `(nsid, def)`.
- (c) Precompute each object's nullable/required sets at load.
- (d) Use a non-allocating CID syntax check.

The other implementation compiles lexicons into this kind of indexed form
and validates a post in 0.78 µs. Most of the post gap is in
facets/embeds: unions, arrays, refs and formats.

## 4. A borrowed, slice-based CAR reader

| 16.6 MB repo CAR, 55k blocks | shrike | |
|---|---|---|
| `car::read_all` | 144-166 µs/MB | **8.9-9.2x** vs a reader returning `&[u8]` blocks |
| `Reader::next_block_into` (reused buffer) | 54 µs/MB | (`read_all` is 2.8x slower than this) |

`read_all` goes through `impl Read`: one `read` call per varint byte
([reader.rs L219](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/car/reader.rs#L219-L238)),
plus an owned copy of every block. Most callers already hold the CAR in
memory: sync responses, `#commit` blocks and proofs.

**Sketch:** add `car::read_slice(&[u8]) -> (Vec<Cid>, Vec<(Cid, &[u8])>)`
(or a borrowing iterator) using the existing `varint_from_slice`. Up to ~9x
for this use. `open_proof`, `parse_commit_blocks` and `Repo::load_car` could
then stop copying.

## 5. Record proofs from a loaded tree

| getRecord proof CAR, real repo (43k keys), 1,000 random keys | shrike | other impl |
|---|---|---|
| per proof | 10.5 µs | 1.5 µs (**~7x**) |

`record_proofs_car`
([proof.rs L220-262](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/repo/proof.rs#L220-L262))
re-decodes the commit and every path node from the block source on each call,
copying each block twice. That includes `Repo::record_proof`, even though the
`Repo` already holds a loaded tree. I didn't profile this one; the cause is
from reading the code.

**Sketch:** keep each clean node's encoded bytes (e.g. `Option<Arc<[u8]>>`
next to the cached CID, filled by `write_node` and by load). Then a
`DetachedTree::path_blocks(key)` can hand them to the CAR writer without
decoding or re-encoding.

## 6. Optional libsecp256k1 backend for K-256

| | `k256` 0.13.4 (shrike) | `secp256k1` 0.30 (libsecp256k1) | |
|---|---|---|---|
| sign | 22.6 µs | 13.6 µs | 1.66x |
| verify (parsed key) | 35.5 µs | 13.0 µs | **2.74x** |

Signatures are byte-identical (both RFC 6979 + low-S). An opt-in feature
backing `K256SigningKey`/`K256VerifyingKey` with the `secp256k1` crate
would roughly halve commit-signature checking in a sync/firehose verifier;
record-proof verification is 1.27x faster end to end. The cost is a vendored C
dependency, so pure-Rust `k256` should stay the default (and the wasm path).
P-256 is at parity with calling `p256` directly, so nothing to change there.

## 7. A streaming DRISL -> JSON writer

| `drisl_to_json` + `serde_json::to_vec` vs a one-pass CBOR->JSON transcoder | |
|---|---|
| post | **2.15x** (1.69 µs vs 0.79 µs) |
| like / repost / follow | 1.82x / 1.77x / 1.50x |

**Sketch:** `drisl_to_json_writer(&[u8], &mut impl Write)`: one recursive
pass that writes JSON directly (escaped text, `$link` via the stack base32
path, `$bytes` base64 into the buffer). No map is needed because DRISL keys
are already canonical and unique. Keep `drisl_to_json` for callers who want
a tree.

## 8. JSON bytes -> DRISL without a `serde_json::Value`

Converting request bytes end to end (parse + `json_to_drisl`) is
**1.35-1.56x** slower than the other implementation. The encode step itself is
at parity (1.05-1.11x, and faster on a profile record), so the whole gap is
parsing into an owned `serde_json::Value` (1.6-2.0x).

**Sketch:** `json_slice_to_drisl(&[u8], Integers)` that deserializes into a
borrowed intermediate (`Cow<str>`, objects as `Vec` sorted once into CBOR
key order), or drives the encoder from a serde visitor and buffers only
object entries for sorting.

## 9. `parse_firehose_frame` does ~2.5x the necessary work

Compared with a floor built from shrike's own primitives (decode header and
body, walk the blocks CAR, hash each block), `parse_firehose_frame` takes
8.3 µs vs 3.3 µs per real `#commit` frame. The extra comes from:
- a `HashMap<String, Vec<u8>>` keyed by `cid.to_string()` with copied blocks
  ([L325-361](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/streaming/mod.rs#L325-L361));
- `item.clone()` of each op ([L394](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/streaming/mod.rs#L394));
- a second copy of the record bytes ([L418-419](https://github.com/jcalabro/shrike/blob/359f0d2fd0804609b5ce73b3747f69ec112f5cc9/src/streaming/mod.rs#L418-L419)).

**Sketch:** key blocks by `Cid` over borrowed slices (or a small `Vec`, since
commits carry few blocks), match ops by reference, and move each record's
bytes once.

## Already fast: shrike is ahead of the other implementation here

- **DAG-CBOR decode is 2.1-2.8x faster** (borrowed `Value<'a>`), and `decode_bump` is another 1.5x on top of that.
- CID parsing is 3.9x faster, and `to_string` is 1.55x faster.
- MST node decode is 2.7x faster, and node encode 1.2x.
- MST bulk inserts are 1.1-1.45x faster.
- Lexicon validation of follows is 1.1x faster.
- P-256 and CID hashing are at parity, given the same SHA backend.

Smaller gaps I'm only mentioning:
- DAG-CBOR encode is 1.1-1.3x slower (`io::Write` per item, a sort check per map).
- The root of a fully dirty tree is 1.3-1.5x slower (`node_to_data` allocates a `NodeData` and a `Vec` per key suffix).

Happy to share more detail or turn any of these into a PR if you'd like.

Filed as https://github.com/jcalabro/shrike/issues/4 (2026-10-02).
