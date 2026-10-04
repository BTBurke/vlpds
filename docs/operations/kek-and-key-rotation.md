---
title: KEK and key rotation
section: Operations
order: 106
status: ready
summary: "Provisioning and rotating the key-encryption key (local or Cloud KMS), the PLC rotation key and the operator recovery key, and surviving a KMS outage."
---

```hero
diagram:
  caption: "Production keys on GCP. Every node holds the same KEK configuration: a Cloud KMS key it can only encrypt and decrypt with, and the PLC rotation key as a file wrapped under that KEK. `rewrap-secrets` moves stored secrets to the current KEK."
  nodes:
    - { id: kms, label: Cloud KMS key, sub: "`vlpds/secrets` · us", at: [0, 0], size: [10, 3], tone: muted }
    - { id: sa, label: service account, sub: encrypt/decrypt only, at: [0, 5], size: [10, 3] }
    - { id: node, label: vlpds nodes, sub: same KEK set on each, at: [17, 0.5], size: [9, 7], tone: accent, stack: true }
    - { id: rows, label: wrapped secrets, sub: "`a/` `p/` rows", at: [33, 0], size: [10, 3], shape: store, tone: amber }
    - { id: plcf, label: "`plc-rotation.key`", sub: "`vw1.` file on each host", at: [33, 5], size: [10, 3] }
  edges:
    - "node <-> kms: wrap · unwrap"
    - "sa -> node: token"
    - "node -> rows: rewrap-secrets"
    - "plcf -> node: unwrapped at start"
facts:
  - { value: "120 d", label: before a KMS key version is destroyed, note: "the maximum; keys and the ring are prevent_destroy in OpenTofu", tone: amber }
  - { value: "1 s", label: fail-fast after a KMS error, note: "cold writes 503 KeyUnavailable; warm accounts keep writing", tone: rust }
  - { value: "72 h", label: to undo a bad PLC op, note: "with the operator recovery key, signed offline", tone: violet }
  - { value: "0", label: restarts during a KMS outage, note: "a restart empties the key cache and turns every account cold", tone: muted }
```

This page is the operator's view of the keys described in [Keys and security](../keys-security.md): what to
create before the first deploy, how to rotate each key without downtime, and what to do when the key service is
down. Exact commands for each procedure are in `ops/RUNBOOK.md`, in the section named at the end of each part here.

## KEK provisioning

```diagram
caption: Two ways to provide the KEK. With both set, Cloud KMS wraps and the local KEK only unwraps, which is how a server moves from one to the other.
nodes:
  - { id: kms, label: "`--gcp-kms-key`", sub: Cloud KMS CryptoKey, at: [0, 0], size: [10, 3], tone: muted }
  - { id: cred, label: "`--gcp-credentials-file`", sub: or the GCE metadata server, at: [0, 4.5], size: [11, 3] }
  - { id: local, label: "`--kek-file`", sub: 32 random bytes, at: [0, 9], size: [10, 3] }
  - { id: ring, label: keyring, sub: wraps with the current KEK, at: [20, 0], size: [10, 12], tone: accent }
edges:
  - "kms -> ring: current"
  - "cred -> ring: auth"
  - "local -> ring: current, or unwrap-only"
```

A node refuses to start outside `--dev-mode` without a KEK, and every node of a cluster needs the same KEK set.

**Cloud KMS** (recommended for anything beyond a personal server; vlpds-node1 runs this way):

```steps
- title: Create the key
  body: "A symmetric `ENCRYPT_DECRYPT` key in a multi-region location. `deploy/gcp` does this in OpenTofu: key ring `vlpds` and key `secrets` in `us`, a 120-day destroy-scheduled duration and `prevent_destroy` (`just plan`, `just apply`)."
- title: Grant one identity
  body: "The nodes' service account gets `roles/cloudkms.cryptoKeyEncrypterDecrypter` on that key only. Nobody routinely holds `cloudkms.cryptoKeyVersions.destroy`. Off GCE, create a JSON key for that account and store it with the other secrets (vlpds-node1: straight into sops)."
- title: Point the nodes at it
  body: "`--gcp-kms-key projects/P/locations/us/keyRings/vlpds/cryptoKeys/secrets`, plus `--gcp-credentials-file` off GCE. The Ansible role writes the JSON key to `/run/vlpds/gcp-sa.json` (0400) from `vlpds_gcp_credentials_json`."
- title: Check
  body: "The `secrets at rest` startup line prints `kek=G…` and `unwrap_keks=`, which must match on every node. `vlpds_kms_requests_total` shows wraps (new accounts) and unwraps (cold loads)."
```

**Local KEK**: `openssl rand -out kek.bin 32` (64 hex chars or base64 also work), distributed like the other secrets
at mode 0400 and passed as `--kek-file` (Ansible: `vlpds_kek_hex`). Back it up offline: it is the only way to read
the stored keys.

> [!WARNING]
> Losing the KEK loses every account's signing key. It belongs in the backup plan: a multi-region KMS key with
> deletion protection, or offline copies of the KEK file.

Runbook: "KEK provisioning", "Secrets as files".

## KEK rotation

```steps
- title: Roll the new KEK out as current
  body: "Inside one CryptoKey, create a new version and make it primary: nothing to configure, KMS still decrypts old versions. Moving to another key, or from local to KMS: `--gcp-kms-key NEW` with `--gcp-kms-old-key OLD` (or `--kek-file old.bin`). Local to local: `--kek-file new.bin --kek-old-file old.bin`."
- title: Rewrap
  body: "`vlpds admin rewrap-secrets` runs `vlpds.admin.rewrapSecrets` on every node, each over the shards it owns: signing keys, reserved keys and TOTP secrets. Add `--check-versions` for a version rotation inside one CryptoKey (one KMS decrypt per secret). No events, no evictions. Re-run until `failed` is 0."
- title: Verify
  body: "`--dry-run` until `stale` is 0 on every node; shards that moved during the rewrap show up here."
- title: Rewrap the PLC rotation key file
  body: "It is a file, not a row: pipe the old `vw1.` file through `vlpds --wrap-plc-rotation-key` with the new KEK configured, and roll the result out."
- title: Retire the old KEK
  body: "Drop `--kek-old-file` / `--gcp-kms-old-key`, or disable the old KMS version. Keep the material (disabled, not destroyed) for the backup retention: backups and log segments still hold blobs wrapped under it."
```

A signing-key rotation that is still pending keeps its new key wrapped under the KEK it started with, and
`rewrap-secrets` doesn't touch it: finish pending rotations before retiring a KEK. A blob under a KEK no node has
fails with `wrapped under unknown key-encryption key` and `VlpdsSecretUnwrapRejected`; put the old KEK back on every
node and rerun the rewrap. Never edit a row by hand.

Runbook: "KEK rotation", "VlpdsSecretUnwrapRejected".

## Key service outage

```diagram
caption: "During a KMS outage only cold accounts are affected: their key isn't in a node's cache yet, so their writes are refused with nothing applied."
nodes:
  - { id: warm, label: warm account, sub: key cached, at: [0, 0], size: [9, 3], tone: accent }
  - { id: cold, label: cold account, sub: first write since load, at: [0, 5], size: [9, 3], tone: accent }
  - { id: ok, label: writes as normal, at: [14, 0], size: [9, 3], tone: ok }
  - { id: no, label: 503 KeyUnavailable, sub: nothing written, at: [14, 5], size: [9, 3], tone: danger }
  - { id: kms, label: Cloud KMS, sub: down, at: [31, 5], size: [8, 3], tone: muted }
edges:
  - warm -> ok
  - "cold -> no: unwrap fails"
  - { from: no.r, to: kms.l, label: retried after 1 s, dash: true }
```

`VlpdsKeyServiceUnavailable` fires on `vlpds_kms_requests_total{result="unavailable"}`. Warm accounts keep writing;
cold accounts' writes, `createAccount`, `reserveSigningKey`, TOTP setup and TOTP logins get 503. Reads, exports,
the firehose and proxying are unaffected. Clients retry 503s, and when KMS comes back the next retry succeeds:
refused writes were never applied, so nothing needs replaying.

- **Don't restart nodes or move shards** (no rolling deploys, splits or handbacks) while KMS is down. A restart or
  takeover empties the key cache and makes every account on that node cold.
- **One node affected** (its network or metadata server): drain it with SIGTERM so its shards move to nodes that can
  reach KMS.
- **IAM**: a removed role looks the same as an outage (403s). Check it before blaming Google.
- **Key destroyed for good**: restore it if a backup exists. Otherwise every account needs a new signing key and a PLC
  update, which needs the PLC rotation key and, for accounts that no longer list it, the users' own keys.

A node won't *start* during a KMS outage if its PLC rotation key file is KMS-wrapped: it is unwrapped at startup.
Another reason not to restart.

Runbook: "Key service (KMS) outage".

## PLC rotation key

```steps
- title: Generate it wrapped
  body: "On a host with the node's KEK config: `vlpds --gcp-kms-key … --wrap-plc-rotation-key </dev/null >plc-rotation.key`. Empty stdin makes a new key; 64 hex chars on stdin wrap an existing one (a reference PDS's `PDS_PLC_ROTATION_KEY_K256_PRIVATE_KEY_HEX`). The did:key goes to stderr: record it. Check the file is one `vw1.` line."
- title: Distribute and start
  body: "Same file on every node, mode 0400, `--plc-rotation-key-file` (Ansible: `vlpds_plc_rotation_key`). The `PLC registration on` startup line shows the same `rotation_key` on every node."
- title: Back it up
  body: "With the KEK backup plan: the file needs the KEK to open, and it is not in the bucket."
```

**Rotating it.** Roll every node with the new key as current and the old one in `--plc-rotation-key-old-file`. New DIDs
list the new key, and any update of an old DID is signed by the old key and swaps in the new one. Then
`vlpds admin rotate-plc-keys --dry-run` reports per node `current`, `rotated` (still on the old key), `foreign` (DIDs
that list neither: migrated away, or synthetic) and `failed`; run it without `--dry-run` to submit the updates
(4 in flight per node; the directory rate-limits, so millions of accounts take a while). When dry runs show
`rotated` 0 everywhere, drop the old key. For a **compromised** key, also use the
[operator recovery key](#operator-recovery-key) to undo ops the attacker signed in the last 72 h.

Never point a test cluster at `plc.directory`: use a local did-method-plc server, or `--dev-mode` without a key.
`VlpdsPlcDirectoryUnavailable` and `VlpdsPlcOpsRejected` cover directory trouble.

Runbook: "PLC rotation key provisioning", "PLC rotation key rotation", "PLC directory outage".

## Operator recovery key

```diagram
caption: "`ensure-recovery-key` adds the operator's did:key to DIDs that lack it, just ahead of the server key, so keys the user added keep their priority."
nodes:
  - { id: before, label: "[user?, server]", sub: before, at: [0, 0], size: [10, 3] }
  - { id: run, label: ensure-recovery-key, sub: one PLC update per DID, at: [14, 0], size: [11, 3], tone: accent }
  - { id: after, label: "[user?, operator, server]", sub: after, at: [29, 0], size: [12, 3], tone: violet }
edges:
  - before -> run
  - run -> after
```

A secp256k1 key the operator keeps offline. It outranks the server rotation key: within 72 h of an op signed by the
server key (a leaked key, a bad deploy), an op signed by the recovery key replaces it.

```steps
- title: Make it offline
  body: "`vlpds --generate-did-key` prints the private key (hex) and its did:key and needs no other configuration. Store the hex offline in two places (paper, a vault, the password manager). It never goes on a node."
- title: Roll it out
  body: "`--plc-recovery-did-key did:key:…` on every node (or the reference's `PDS_RECOVERY_DID_KEY`; Ansible `vlpds_plc_recovery_did_key`). New accounts and `getRecommendedDidCredentials` list it from then on."
- title: Backfill existing accounts
  body: "`vlpds admin ensure-recovery-key --dry-run` reports `present`, `added`, `foreign`, `full` (already 10 keys) and `failed` per node; `--json` shows sample changes. Then run it without `--dry-run`, paced at `--per-second` (default 4) DIDs per node. Re-run until `failed` is 0; it is idempotent."
- title: Use it only in an emergency
  body: "Build the corrective op (prev = the last good op's CID, the good keys and services), sign it offline with the recovery key (for example `goat plc`) and post it to the directory within 72 h of the bad op. Then rotate the server rotation key."
```

Changing the recovery key: roll out the new did:key and rerun `ensure-recovery-key`. The old one stays listed until a
DID's keys are rewritten; remove it only if it leaked. On vlpds-node1 the recovery key is set and has been backfilled onto
every account. Users add their own keys, ahead of the operator's, on the account page or in `/migrate`'s advanced
mode ([Keys and security](../keys-security.md#plc-rotation-key-and-recovery-keys)).

Runbook: "Operator recovery key".

## Repo signing-key rotation

```steps
- title: Begin
  body: "The new key, wrapped under the KEK, is written to the account row as pending in one durable log entry. From here the repo's writes get a retryable 503 `KeyUnavailable`, so nothing is signed with the old key once the DID document may change."
- title: PLC
  body: "The DID's `atproto` verification method is set to the new key (did:plc only; a did:web's document is its owner's to change)."
- title: Finish
  body: "The account takes the new key and the head commit is re-signed (same data, next rev). One log entry carries the head, the row, `#identity` and `#sync`; writes resume."
```

`vlpds admin rotate-keys --generate <did>` (admin `updateAccountSigningKey`) rotates to a fresh key. Without
`--generate` it re-publishes the current key to PLC and re-signs the head, like the reference's rotate-keys script.
Relays see commits signed with the old key, then `#identity` and `#sync`, then commits signed with the new key.

A rotation interrupted between Begin and Finish (a directory or KMS outage, a crash, a shard move) stays pending with
the repo's writes fenced. Nothing sweeps for them: **re-run the same command**, or let the first refused write finish it
in the background (one attempt per second per account). It is abandoned only if the directory definitely refused the
update and still doesn't list the key. Pending keys aren't covered by `rewrap-secrets`, so finish rotations before
retiring a KEK.
