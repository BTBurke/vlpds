did:plc audit-log interop vectors, copied unchanged from
https://github.com/did-method-plc/did-method-plc `interop_tests/audit_log`
(MIT/Apache-2.0; their canonical home is go-didplc `testdata`). Used by the
`src/plc/mod.rs` tests: DID derivation, op CIDs (DAG-CBOR bytes),
signatures, and the nullification / tombstone rules of `PlcLog::apply`.
