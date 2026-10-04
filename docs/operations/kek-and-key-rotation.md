---
title: KEK and key rotation
section: Operations
order: 106
status: ready
summary: "Provisioning and rotating the key-encryption key (local, Cloud KMS or Vault Transit), the PLC rotation key and the operator recovery key, and surviving a key service outage."
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

[Keys and security](../keys-security.md) describes the keys. Here's what to create before the first deploy, how
to rotate each key without downtime, and what to do when the key service is down. The exact commands are in
`ops/RUNBOOK.md`, in the section named at the end of each part below.

## KEK provisioning

```diagram
caption: Three ways to provide the KEK. Cloud KMS or Vault Transit (one of them) wraps when it's set, and the local KEK then only unwraps, which is how a server moves from one to the other.
nodes:
  - { id: kms, label: "`--gcp-kms-key`", sub: Cloud KMS CryptoKey, at: [0, 0], size: [11, 3], tone: muted }
  - { id: vault, label: "`--vault-transit-key`", sub: Vault Transit key, at: [0, 4.5], size: [11, 3], tone: muted }
  - { id: local, label: "`--kek-file`", sub: 32 random bytes, at: [0, 9], size: [11, 3] }
  - { id: ring, label: keyring, sub: wraps with the current KEK, at: [20, 0], size: [10, 12], tone: accent }
edges:
  - "kms -> ring: current"
  - "vault -> ring: current"
  - "local -> ring: current, or unwrap-only"
```

Outside `--dev-mode`, a node refuses to start without a KEK. Every node of a cluster needs the same KEK set.

A key service is the recommended setup for anything beyond a personal server. That's Cloud KMS on GCP, or
[Vault Transit](#vault-transit) if you already run Vault or OpenBao. On GCP:

```steps
- title: Create the key
  body: "Create a symmetric `ENCRYPT_DECRYPT` key in a multi-region location. `deploy/gcp` does this in OpenTofu with key ring `vlpds` and key `secrets` in `us`, a 120-day destroy-scheduled duration and `prevent_destroy` (`just plan`, `just apply`)."
- title: Grant one identity
  body: "Give the nodes' service account `roles/cloudkms.cryptoKeyEncrypterDecrypter` on that key only. Nobody routinely holds `cloudkms.cryptoKeyVersions.destroy`. Off GCE, create a JSON key for that account and store it with the other secrets (e.g. in sops or Ansible Vault)."
- title: Point the nodes at it
  body: "Set `--gcp-kms-key projects/P/locations/us/keyRings/vlpds/cryptoKeys/secrets`, plus `--gcp-credentials-file` off GCE. The Ansible role writes the JSON key to `/run/vlpds/gcp-sa.json` (0400) from `vlpds_gcp_credentials_json`."
- title: Check
  body: "The `secrets at rest` startup line prints `kek=G…` (`V…` for Vault) and `unwrap_keks=`, and those must match on every node. `vlpds_kms_requests_total` shows wraps (new accounts) and unwraps (cold loads)."
```

For a local KEK, run `openssl rand -out kek.bin 32` (64 hex chars or base64 also work). Distribute it like the other
secrets at mode 0400 and pass it as `--kek-file` (Ansible: `vlpds_kek_hex`). Back it up offline, since it's the only
way to read the stored keys.

> [!WARNING]
> Losing the KEK loses every account's signing key. Protect it with a multi-region KMS key with deletion protection,
> or with offline copies of the KEK file.

Runbook: "KEK provisioning", "Secrets as files".

## Vault Transit

```diagram
caption: "With `--vault-transit-key`, each node logs in to Vault, caches the token and sends each secret to Transit `encrypt` with its purpose and DID as `associated_data`. The stored blob is Transit's own `vault:vN:…` ciphertext."
nodes:
  - { id: auth, label: auth method, sub: token file · AppRole · k8s, at: [0, 0], size: [11, 3] }
  - { id: node, label: vlpds nodes, sub: cached token, at: [17, 0], size: [9, 3], tone: accent, stack: true }
  - { id: transit, label: Transit key, sub: "`transit/vlpds` · aes256-gcm96", at: [33, 0], size: [11, 3], tone: muted }
  - { id: rows, label: wrapped secrets, sub: "`vw1.V….{vault:vN:…}`", at: [17, 5.5], size: [11, 3], shape: store, tone: amber }
edges:
  - "auth -> node: token"
  - "node <-> transit: encrypt · decrypt"
  - "node -> rows: wrapped"
```

vlpds can use a Transit key on HashiCorp Vault or OpenBao as the KEK, in place of Cloud KMS. It's the same
model as Cloud KMS: the key never leaves Vault, every unwrap is a Vault call (and an audit log entry if you
have an audit device), and a copy of the bucket is useless without decrypt permission on the key. A node
uses one key service at a time, so `--vault-transit-key` and `--gcp-kms-key` can't both be set.

vlpds binds every wrapped secret to its purpose and DID through Transit's `associated_data`. Vault added
that parameter in 1.13. Older Vaults accept it and drop it with only a warning, so a blob copied into
another account's row would unwrap there. So a node checks before it uses the current key. It wraps a
random value under one AAD and requires the decrypt under another AAD to fail. A Vault that drops the AAD,
or a key that can't take it (RSA or a derived key), stops the node at startup with an
error naming the key. If Vault is unreachable at startup, the node starts anyway and runs the check before
its first wrap or unwrap. Every Transit response is also checked for the "ignored parameters" warning.

```steps
- title: Create the key
  body: "`vault secrets enable transit`, then `vault write -f transit/keys/vlpds type=aes256-gcm96` (`aes128-gcm96` and `chacha20-poly1305` work too). Keep `deletion_allowed` false, which is the default."
- title: Write the policy
  body: "The policy below, as `vault policy write vlpds-kek vlpds-kek.hcl`. It grants `update` and not `create`, so a typo in the key name fails instead of creating a new key."
- title: Pick an auth method
  body: "A token file, AppRole or Kubernetes (below). Exactly one."
- title: Point the nodes at it
  body: "`--vault-addr https://vault.example:8200 --vault-transit-key transit/vlpds`, the auth flags, and `--vault-ca-file` if Vault's certificate comes from a private CA. Add `--vault-namespace` on Vault Enterprise or OpenBao namespaces."
- title: Check
  body: "The `vault transit key` startup line maps the kid to the key (`kid=V… key=vault:vault.example/transit/vlpds`), and `secrets at rest` shows `kek=V…`. Both must match on every node."
```

```hcl
# vlpds-kek.hcl
path "transit/encrypt/vlpds" { capabilities = ["update"] }
path "transit/decrypt/vlpds" { capabilities = ["update"] }

# Optional. Lets the startup check name a wrong key type instead of only failing the AAD test.
path "transit/keys/vlpds" { capabilities = ["read"] }

# AppRole and Kubernetes tokens renew themselves. This is in Vault's `default` policy,
# so it's only needed if the role sets token_no_default_policy.
path "auth/token/renew-self" { capabilities = ["update"] }
```

The login endpoints (`auth/approle/login`, `auth/kubernetes/login`) need no policy. While you move to another
key, also grant `update` on the old key's `decrypt` path.

The kid is a hash of Vault's host name, namespace, mount and key name. It has no version in it, so
Transit's `rotate` keeps it. Use the same `--vault-addr` host name on every node and keep it stable (a DNS
name you control). A node that reaches the same Vault by another name sees another kid, and its
stored blobs fail as `wrapped under unknown key-encryption key`.

### Auth methods

| Method | Flags | What vlpds does |
|---|---|---|
| Token file | `--vault-token-file` | Reads the file at start, every minute and after a 403. Pair it with a Vault Agent `auto_auth` sink that renews and rewrites the token. |
| AppRole | `--vault-approle-role-id[-file]`, `--vault-approle-secret-id-file`, `--vault-approle-mount` (`approle`) | Logs in at first use and caches the token. It renews the token with `renew-self` before it expires (a third of the TTL early, at most a minute) and logs in again when the token isn't renewable or a renewal comes back capped by `token_max_ttl`. The secret ID file is read at every login, so you can replace it without a restart. |
| Kubernetes | `--vault-k8s-role`, `--vault-k8s-mount` (`kubernetes`), `--vault-k8s-jwt-file` (the pod's service-account token) | Same as AppRole, logging in with the service-account token. The token file is read at every login, since projected tokens rotate. |

For AppRole, something like `vault write auth/approle/role/vlpds token_policies=vlpds-kek token_ttl=1h
token_max_ttl=24h`, then `vault read -field=role_id auth/approle/role/vlpds/role-id` and `vault write -f
-field=secret_id auth/approle/role/vlpds/secret-id`. Store the secret ID like the other secrets (0400 under
`/run/vlpds`). For Kubernetes, bind the role to the pod's service account and namespace
(`bound_service_account_names=vlpds bound_service_account_namespaces=pds token_policies=vlpds-kek`).

There's no flag for a token value. Tokens, secret IDs and service-account tokens are only read from files,
and none of them is ever logged. On a 403 (an expired or revoked token, a rotated Agent token) a node logs
in or re-reads the file once and retries. If that fails too, the call fails as unavailable.

### Rotation

`vault write -f transit/keys/vlpds/rotate` makes a new key version current, with nothing to change on the
nodes. New wraps use the new version right away. A blob under an older version than the latest one a node
has seen counts as stale. A node learns the latest version from its own wraps, or with one dummy encrypt
when an unwrap finds its answer is over a minute old. So wait a minute after `rotate`, then run
`vlpds admin rewrap-secrets --check-versions` (one decrypt per secret) until a dry run shows `stale` 0
everywhere. Then you can retire the old version with `vault write transit/keys/vlpds/config
min_decryption_version=N`. That's reversible, unlike `trim`. Blobs under older versions then fail as
rejected, and log segments still hold some until retention deletes them, so don't trim.

To move to another key or mount (or between Vault and Cloud KMS or a local KEK), roll the nodes with the new
key as current and the old one unwrap-only, then rewrap without `--check-versions`:

| From → to | Flags during the move |
|---|---|
| local → Vault | `--vault-transit-key transit/vlpds --kek-file old.bin` |
| Vault key A → key B | `--vault-transit-key transit2/vlpds --vault-transit-old-key transit/vlpds` |
| Vault → Cloud KMS | `--gcp-kms-key projects/… --vault-transit-old-key transit/vlpds` (plus the Vault address and auth) |
| Vault → local | `--kek-file new.bin --vault-transit-old-key transit/vlpds` |

`--vault-transit-old-key` takes a comma-separated list. Old keys live on the same Vault, with the same login.

### Vault outage

A Vault outage plays out exactly like a [Cloud KMS outage](#key-service-outage). Warm accounts keep
writing, cold writes get 503 `KeyUnavailable`, and a node fails cold unwraps fast for 1 s after a failure.
Timeouts, connection errors, 5xx (a sealed Vault is 503), 429s, failed logins and a 403 that survives a
re-login all count as unavailable. A 400 from Transit counts as rejected and fires `VlpdsSecretUnwrapRejected`.
That's a wrong AAD, a ciphertext of another key, or a version below `min_decryption_version`. The metrics
carry `backend="vault"`, and calls share `--kms-concurrency` with 5 s each like Cloud KMS.

Runbook: "KEK provisioning", "Key service (KMS) outage".

## KEK rotation

```steps
- title: Roll the new KEK out as current
  body: "Inside one CryptoKey, create a new version and make it primary (on Vault, `rotate` the Transit key). There's nothing to configure, since the key service still decrypts old versions. To move to another key, or from local to a key service, set `--gcp-kms-key NEW` with `--gcp-kms-old-key OLD` (or `--kek-file old.bin`). Vault works the same way with `--vault-transit-key` and `--vault-transit-old-key`. For local to local, set `--kek-file new.bin --kek-old-file old.bin`."
- title: Rewrap
  body: "`vlpds admin rewrap-secrets` runs `vlpds.admin.rewrapSecrets` on every node, each over the shards it owns. It covers signing keys, reserved keys and TOTP secrets. Add `--check-versions` for a version rotation inside one CryptoKey or Transit key (one decrypt per secret). It emits no events and evicts nothing. Re-run it until `failed` is 0."
- title: Verify
  body: "Run it with `--dry-run` until `stale` is 0 on every node. Shards that moved during the rewrap show up here."
- title: Rewrap the PLC rotation key file
  body: "The PLC rotation key is a file and not a row. Pipe the old `vw1.` file through `vlpds --wrap-plc-rotation-key` with the new KEK configured, and roll the result out."
- title: Retire the old KEK
  body: "Drop `--kek-old-file` / `--gcp-kms-old-key` / `--vault-transit-old-key`, or disable the old KMS version (on Vault, raise `min_decryption_version`). Keep the material (disabled, not destroyed), because log segments still hold secrets wrapped under it until retention deletes them."
```

A signing-key rotation that's still pending keeps its new key wrapped under the KEK it started with, and
`rewrap-secrets` doesn't touch it. So finish pending rotations before retiring a KEK. A secret wrapped under a KEK that no node
has fails with `wrapped under unknown key-encryption key` and `VlpdsSecretUnwrapRejected`. Put the old KEK back on
every node and rerun the rewrap. Never edit a row by hand.

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

`VlpdsKeyServiceUnavailable` fires on `vlpds_kms_requests_total{result="unavailable"}`. Warm accounts keep writing.
Cold accounts' writes, `createAccount`, `reserveSigningKey`, TOTP setup and TOTP logins get 503. Reads, exports,
the firehose and proxying are unaffected. Clients retry 503s, and when KMS comes back the next retry succeeds.
Refused writes were never applied, so nothing needs replaying.

- Don't restart nodes or move shards (no rolling deploys, splits or handbacks) while KMS is down. A restart or
  takeover empties the key cache and makes every account on that node cold.
- If only one node is affected (its network or metadata server), drain it with SIGTERM so its shards move to nodes
  that can reach KMS.
- A removed IAM role looks the same as an outage (403s). Check IAM before blaming Google. On Vault, a
  policy that lost a path, a revoked AppRole secret ID and a sealed Vault all look like an outage too.
- If the key is destroyed for good, restore it from an offline copy if one exists. Otherwise every account needs a
  new signing key and a PLC update. That needs the PLC rotation key, plus the users' own keys for accounts that no
  longer list it.

If a node's PLC rotation key file is KMS-wrapped, the node can't start during a KMS outage, because the file is
unwrapped at startup. That's another reason not to restart.

Runbook: "Key service (KMS) outage".

## PLC rotation key

```steps
- title: Generate it wrapped
  body: "On a host with the node's KEK config, run `vlpds --gcp-kms-key … --wrap-plc-rotation-key </dev/null >plc-rotation.key`. Empty stdin makes a new key, and 64 hex chars on stdin wrap an existing one (a reference PDS's `PDS_PLC_ROTATION_KEY_K256_PRIVATE_KEY_HEX`). The did:key goes to stderr, so record it. Check that the file is one `vw1.` line."
- title: Distribute and start
  body: "Put the same file on every node at mode 0400 and pass `--plc-rotation-key-file` (Ansible: `vlpds_plc_rotation_key`). The `PLC registration on` startup line shows the same `rotation_key` on every node."
- title: Back it up
  body: "Keep an offline copy next to the KEK's. The file needs the KEK to open, and it isn't in the bucket."
```

To rotate it, roll every node with the new key as current and the old one in `--plc-rotation-key-old-file`. New
DIDs list the new key. Any update of an old DID is signed by the old key and swaps in the new one. Then
`vlpds admin rotate-plc-keys --dry-run` reports `current`, `rotated` (still on the old key), `foreign` (DIDs that
list neither, because they migrated away or are synthetic) and `failed` per node. Run it without `--dry-run` to
submit the updates (4 in flight per node). The directory rate-limits, so millions of accounts take a while. When
dry runs show `rotated` 0 everywhere, drop the old key. If the key was compromised, also use the
[operator recovery key](#operator-recovery-key) to undo ops the attacker signed in the last 72 h.

Never point a test cluster at `plc.directory`. Use a local did-method-plc server, or `--dev-mode` without a key.
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

The operator recovery key is a secp256k1 key the operator keeps offline. It outranks the server rotation key. Within
72 h of an op signed by the server key (a leaked key, a bad deploy), an op signed by the recovery key replaces it.

```steps
- title: Make it offline
  body: "`vlpds --generate-did-key` prints the private key (hex) and its did:key, and it needs no other configuration. Store the hex offline in two places (paper, a vault, the password manager). It never goes on a node."
- title: Roll it out
  body: "Set `--plc-recovery-did-key did:key:…` on every node (or the reference's `PDS_RECOVERY_DID_KEY`, or Ansible's `vlpds_plc_recovery_did_key`). New accounts and `getRecommendedDidCredentials` list it from then on."
- title: Backfill existing accounts
  body: "`vlpds admin ensure-recovery-key --dry-run` reports `present`, `added`, `foreign`, `full` (already 10 keys) and `failed` per node, and `--json` shows sample changes. Then run it without `--dry-run`, paced at `--per-second` (default 4) DIDs per node. It's idempotent, so re-run it until `failed` is 0."
- title: Use it only in an emergency
  body: "Build the corrective op (prev = the last good op's CID, plus the good keys and services). Sign it offline with the recovery key (for example with `goat plc`) and post it to the directory within 72 h of the bad op. Then rotate the server rotation key."
```

To change the recovery key, roll out the new did:key and rerun `ensure-recovery-key`. The old one stays listed until
a DID's keys are rewritten. Remove it only if it leaked. Where you can, set it before the first
account. `ensure-recovery-key` backfills it onto accounts created earlier. Users add their own keys, ahead of the
operator's, on the account page or in `/migrate`'s advanced mode
([Keys and security](../keys-security.md#plc-rotation-key-and-recovery-keys)).

Runbook: "Operator recovery key".

## Repo signing-key rotation

```steps
- title: Begin
  body: "vlpds writes the new key, wrapped under the KEK, to the account row as pending in one durable log entry. From here the repo's writes get a retryable 503 `KeyUnavailable`, so nothing is signed with the old key once the DID document may change."
- title: PLC
  body: "The DID's `atproto` verification method is set to the new key. This is did:plc only, since a did:web's document is its owner's to change."
- title: Finish
  body: "The account takes the new key and the head commit is re-signed (same data, next rev). One log entry carries the head, the row, `#identity` and `#sync`, and then writes resume."
```

`vlpds admin rotate-keys --generate <did>` (admin `updateAccountSigningKey`) rotates to a fresh key. Without
`--generate`, it re-publishes the current key to PLC and re-signs the head, like the reference's rotate-keys script.
Relays see commits signed with the old key, then `#identity` and `#sync`, then commits signed with the new key.

A rotation interrupted between Begin and Finish (a directory or KMS outage, a crash, a shard move) stays pending with
the repo's writes fenced. Nothing sweeps for these, so re-run the same command. You can also let the first refused
write finish it in the background (one attempt per second per account). It's abandoned only if the directory
definitely refused the update and still doesn't list the key. Pending keys aren't covered by `rewrap-secrets`, so
finish rotations before retiring a KEK.
