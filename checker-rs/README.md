# checker-rs

An independent sync 1.1 firehose verifier for vlpds, written against
[shrike](https://github.com/jcalabro/shrike) 0.7.0 (a second Rust atproto
implementation) instead of vlpds's own CBOR / CAR / MST / crypto code. It is
the Rust twin of the Go checker in `../checker` (built on indigo): same flags,
failure kinds and summary, so the two can run side by side.

```sh
just checker-rs http://127.0.0.1:2620 -cursor 0 -strict
# or
cd checker-rs && cargo run --release -- -host http://127.0.0.1:2620 -cursor 0 -strict
```

Flags: `-host` (http/https or ws/wss), `-cursor N` (replay from seq; omit to
start live), `-max-events N`, `-strict` (exit 1 on any failure), `-quiet` (no
5 s progress lines), `-dense` (require seq == previous + 1), `-idle-exit SECS`
(stop after SECS without a frame). Ctrl-C prints the summary. Exit codes: 0
ok, 1 failures under `-strict`, 2 bad flags / no connection.

Every event goes through two verifiers:

1. Its own checks, made with shrike's primitives: seq order; canonical
   DAG-CBOR frames and blocks (no floats); for `#commit` the CAR (version 1,
   block hashes, root == `commit`), the commit object (DID, rev, version 3),
   the signature against the account's `#atproto` key (vlpds's `describeRepo`,
   else `resolveDid`: vlpds mints non-resolvable did:plc ids), unique op
   paths, `prev` on updates/deletes, op CIDs against the new tree, the sync 1.1
   inversion of the ops back to `prevData` with only the commit's blocks, and
   the per-DID chain (`since` == previous rev, `prevData` == previous data,
   rev increasing); `#sync` signature / rev; `#identity` / `#account` fields;
   per-DID event times never going backwards.
2. shrike's stock `sync::Verifier` (strict inversion, `Error` policy, getRepo
   for `#sync` resyncs). Its verdicts are tallied on the `shrike verifier:`
   summary line. Its one known false positive (it loads both neighbour
   subtrees of an updated key, which indigo-style producers such as vlpds
   don't send and an update's inversion doesn't need; see
   `../bench/results/differential/SHRIKE_ISSUES.md`) is counted as
   `shrike_update_overfetch`, not as a failure, when the commit inverts
   correctly under check 1. Any other rejection is a `shrike_verifier` failure.

Failure kinds: `decode`, `seq_reorder`, `seq_gap`, `upstream_error`,
`non_canonical`, `car`, `commit_cid`, `did_mismatch`, `rev_mismatch`,
`too_big`, `rebase`, `blocks_size`, `dup_op_path`, `op_missing_prev`,
`missing_prevdata`, `op_cid`, `prevdata_mismatch`, `key_fetch`, `signature`,
`chain_since`, `chain_prevdata`, `chain_rev`, `bad_field`, `time_order`,
`shrike_verifier`.

It is a standalone crate (its own `[workspace]`), not part of the vlpds build.
`../bench/results/differential/checker-run.sh` runs it next to the Go checker
against an in-memory vlpds under loadgen writes.
