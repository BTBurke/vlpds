# shrike 0.7.0: differential findings

Found by `tests/all/differential_shrike.rs` (vlpds vs shrike) and
`checker-rs` (shrike's sync verifier on a vlpds firehose), with the
TypeScript reference as the tiebreaker (`@atproto/syntax` 0.7.5,
`@atproto/lex-json` 0.1.6 / `lex-data` 0.1.7, via `syntax_oracle.mjs` /
`json_oracle.mjs` here) and the atproto interop fixtures. Filed upstream as
https://github.com/jcalabro/shrike/issues/3 (2026-10-02). Each issue has a minimal repro against shrike alone; the
differential suite pins every one (it fails when shrike changes, so the
workaround can be dropped).

## 1. Inverting an update op needs blocks the commit doesn't carry (sync verifier)

`DetachedTree::insert` of a key that already exists (how
`sync::invert_decoded_commit` undoes an `update` op) loads the key's
neighbour subtrees on both sides: `visit_key_path` continues "into both
subtrees next to an entry equal to `key`". That's what a *remove* needs (it
merges the two subtrees), but an update only changes the value at an existing
key: the tree's shape is unchanged and only the root-to-key path is needed.

indigo's sync 1.1 producer (`proveMutation`, which vlpds ports) only adds
neighbour nodes for creates and deletes, so an update commit from an
indigo-style PDS carries just the changed path. shrike's verifier then fails
the inversion with `BlockNotFound`, and that error is not covered by
`lenient_inversion` (it fires in `previous_root`, before the lenient check), so
the commit goes to `handle_recoverable_resync`: a resync (getRepo) under the
`Resync` policy, an error under `Error`. On a vlpds firehose under loadgen
(25% updates), 367 of 9,020 commits hit this (every update whose key had
subtrees on both sides); indigo's `VerifyCommitMessage`, the Go checker and
checker-rs's own inverter (path-only rewrite for updates) verify all of them.
The TypeScript PDS happens to work because its `relevantBlocks` include a full
covering proof even for updates.

Repro (shrike only):

```rust
use shrike::cbor::{Cid, Codec};
use shrike::mst::{height_for_key, DetachedTree, NoBlocks};
use std::collections::HashMap;

let leaf = Cid::compute(Codec::Drisl, b"leaf");
let new = Cid::compute(Codec::Drisl, b"new");
let at = |h: u8, n: usize| -> Vec<String> {
    (0..).map(|i| format!("com.example.k/{i:06}")).filter(|k| height_for_key(k) == h).take(n).collect()
};
let mut t = DetachedTree::new();
for k in at(0, 6).into_iter().chain(at(1, 3)) {
    t.insert(&NoBlocks, k, leaf).unwrap();
}
t.flush().unwrap();
let key = at(1, 2)[1].clone(); // a height-1 key with height-0 subtrees on both sides
t.insert(&NoBlocks, key.clone(), new).unwrap(); // the update
let w = t.flush().unwrap(); // new_blocks: the nodes the update changed (the root-to-key path)
let path: HashMap<Cid, Vec<u8>> = w.new_blocks.into_iter().collect();

let mut inv = DetachedTree::load(w.root);
assert!(inv.missing_blocks(&path, [key.as_str()]).unwrap().is_empty()); // fails: wants the neighbours
inv.insert(&path, key, leaf).unwrap(); // fails: BlockNotFound
```

Expected: undoing an update (insert of an existing key with its previous
value) succeeds with the root-to-key path alone. Suggested fix: in
`visit_key_path`, descend into both neighbours only for a removal (or when the
key is absent), not for an insert of an existing key.

## 2. DIDs with `%` are rejected

`Did::try_from` allows only `[a-zA-Z0-9._:-]` in the method-specific id. The
DID syntax spec, `@atproto/syntax` `isValidDid` and the atproto interop
fixtures (`did_syntax_valid.txt`) allow `%` (percent-encoding, e.g. a did:web
with a port). shrike's copy of the fixtures moved these lines to its invalid
file.

```rust
for did in ["did:web:localhost%3A1234", "did:method:val%BB", "did:method:-:_:.:%ab"] {
    assert!(shrike::syntax::Did::try_from(did).is_ok()); // fails for each
}
```

Since `sync::raw::parse_raw_sync_frame` parses `repo` / `did` with `Did`, a
firehose event for such a DID is a parse error. (In the generated corpus: 68
DIDs the reference accepts and shrike rejects; none the other way.)

## 3. Language tags: a simplified check that differs from the reference

`Language::try_from` accepts any 1-8 alphanumeric subtags after a lowercase
2-3 letter (or `i`) primary subtag and rejects private-use tags. The
reference (`@atproto/syntax` `parseLanguageString`, used by lex-schema's
`isLanguageString` for the `language` format) checks the RFC 5646 grammar.
Against the reference on 15k generated tags: 562 valid tags rejected, 2053
invalid tags accepted. Examples:

```rust
use shrike::syntax::Language;
// valid (reference and interop fixture language_syntax_valid.txt), rejected:
assert!(Language::try_from("X-fr-CH").is_ok()); // fails (private use, in the interop fixtures)
assert!(Language::try_from("x-foo").is_ok());   // fails
// invalid, accepted:
assert!(Language::try_from("i-foo").is_err());          // fails (only the grandfathered i-* tags)
assert!(Language::try_from("en-a").is_err());           // fails (an extension singleton needs a subtag)
assert!(Language::try_from("sl-rozaj-rozaj").is_err()); // fails (repeated variant)
```

## 4. AT-URI fragments are rejected

`AtUri::try_from` rejects a `#` fragment. The AT URI spec allows a fragment
holding a JSON pointer, and `@atproto/syntax` `isAtUriString` (the lexicon
`at-uri` format) accepts `#/...` fragments with the URI path charset and valid
percent-encoding.

```rust
assert!(shrike::syntax::AtUri::try_from("at://did:plc:abc/app.bsky.feed.post/3jzfcijpj2z2a#/text").is_ok()); // fails
```

## 5. `{"$bytes": "AQ="}` (partial padding) stays a plain map

`cbor::json::json_to_drisl` keeps a `$bytes` object whose string has partial
padding as a map. The reference (`@atproto/lex-json` `lexParse`, strict and
non-strict, on Node) decodes it to bytes: lex-data's `fromBase64` accepts any
padding that doesn't run past the padded length. shrike's `base64::decode`
says it matches `fromBase64` but requires all-or-nothing padding. The same
JSON then hashes to a different CID than in the reference.

```rust
use shrike::cbor::json::{json_to_drisl, Integers};
let j = serde_json::json!({"a": {"$bytes": "AQ="}});
assert_eq!(json_to_drisl(&j, Integers::Any).unwrap(), hex::decode("a1616141 01".replace(' ', "")).unwrap()); // fails: {"a": {"$bytes": "AQ="}} as a map
```

(The reference on Node also takes the URL-safe alphabet, `"_-8"`, through
`Buffer.from`; that looks like a runtime artifact rather than intent, and
neither shrike nor vlpds accepts it.)

## Not bugs: policy differences between the two

Pinned by the differential suite; listed so they aren't mistaken for issues.

- **Floats**: shrike's DRISL decoder accepts canonical 64-bit floats (DRISL
  allows them) and rejects them one layer up (JSON conversion); vlpds rejects
  them while decoding (atproto data model). Same for f16/f32/NaN/Infinity: both
  reject.
- **Nesting**: shrike caps CBOR nesting at 64 levels, vlpds at 128.
- **lex-json mode**: shrike's `json_to_drisl` is the reference's non-strict
  mode (a malformed `$link` / `$bytes` / blob object stays a map); vlpds parses
  record writes like the strict mode and rejects them.
- **MST loading**: shrike's loader, like indigo's, doesn't check that a node's
  keys share one height, that children sit exactly one level down, canonical
  prefix compression, that `l` is present, or that intermediate nodes are
  non-empty (indigo has a separate `Tree.Verify`). vlpds's `decode_node` /
  `load_from_blocks` check all of these. For a verifier this matters little
  (a non-canonical tree can't invert to the signed `prevData`), but a
  structure check on load would make proofs from a hostile host fail early.
  shrike requires UTF-8 MST keys; vlpds and indigo take any 1-1024 bytes.
- **Impossible dates** (`2024-02-30T00:00:00Z`): shrike and the TS reference
  (JS `Date` rolls them over) accept, indigo (`time.Parse`) and vlpds reject.

## vlpds bugs the comparison found (fixed in vlpds)

For the record; these were vlpds's side of the disagreements.

- `mst::Tree::proof_blocks` (getRecord proofs) stopped at the absent key's
  own height. Verifiers walk proofs by key order (reference `cidsForPath`,
  shrike's `verify_record_proof`), so exclusion proofs for keys that sort
  below that level failed to verify.
- Lexicon `language` format: accepted `JA`, `x`, `en-a`, `enU-9`, rejected
  `X-fr-CH` (interop fixture). Now the reference's grammar.
- Lexicon `at-uri` format rejected `#/json-pointer` fragments.
- `$bytes`: rejected non-zero trailing bits (`"AR"`), accepted over-padding
  (`"AQID="`); now as lex-data's `fromBase64`.
- Blob refs: accepted a negative or unsafe `size`, extra keys and a non-raw
  `ref` CID; now as lex-data's strict `isTypedBlobRef` (except its `/` in
  `mimeType`: the data model only asks for a non-empty string, and real
  records carry `"mimeType": "jpeg"`).
- CAR reader: accepted headers without `version: 1` (or with non-CID roots).

Left as vlpds policy (reference strict lex-json differs): integers outside the
JS-safe range are accepted (the data model says 64-bit); `$link` must be a
base32 dag-cbor/raw sha-256 CIDv1 (the reference takes base58btc, CIDv0 and
other codecs; vlpds's `Cid` can't represent them).
