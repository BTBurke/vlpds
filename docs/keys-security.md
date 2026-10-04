---
title: Keys and security
section: vlPDS
order: 8
status: ready
summary: "Which keys exist, where they live and how they are wrapped: the KEK (local or Cloud KMS), repo signing keys, the PLC rotation key, recovery keys, peer mTLS and the HTTP security headers."
---

```hero
diagram:
  caption: Secrets live in three places. The bucket holds signing keys and TOTP secrets only wrapped under the KEK. The host holds the secrets the node starts with, as 0400 files. The operator's recovery key and the peer CA key stay offline.
  nodes:
    - { id: kms, label: Cloud KMS, sub: the KEK · never leaves, at: [0, 1], size: [9, 3], tone: muted }
    - { id: files, label: "`/run/vlpds/*`", sub: 0400 secret files, at: [0, 5], size: [9, 3] }
    - { id: node, label: vlpds node, sub: unwrapped-key cache, at: [16, 1.5], size: [9, 6], tone: accent }
    - { id: rows, label: "`a/{did}` `p/{did}`", sub: wrapped keys · hashes, at: [32, 1], size: [11, 3], shape: store, tone: amber }
    - { id: plc, label: PLC directory, sub: rotation keys per DID, at: [32, 5], size: [11, 3], tone: muted }
    - { id: rec, label: operator recovery key, sub: offline · outranks server, at: [32, 11], size: [11, 3], shape: note, tone: violet }
  edges:
    - "node <-> kms: wrap · unwrap"
    - "files -> node: read at start"
    - "node -> rows: wrapped only"
    - "node -> plc: signed ops"
    - { from: rec.t, to: plc.b, label: emergency op, dash: true }
facts:
  - { value: "0", unit: plaintext keys, label: in the bucket, note: "signing keys and TOTP secrets are KEK-wrapped; passwords are Argon2id", tone: amber }
  - { value: "1", unit: KMS call, label: per account per cache lifetime, note: "a cold unwrap; warm commits never touch the KEK", tone: blue }
  - { value: "3", unit: key tiers, label: in every DID's rotation keys, note: "user's, then operator's, then the server's" , tone: violet }
  - { value: "~15 µs", label: to verify every signature, note: "before it leaves the node; ~+15% commit CPU" }
```

The bucket is the database, so anyone who can read it, or any backup or replica of it, gets everything
stored there. vlpds therefore never stores a secret it can recover in the clear: keys it must use are
wrapped under a **key-encryption key (KEK)** that is not in the bucket, and secrets it only checks are
stored as hashes. This page lists every key, says where it lives and what protects it, and covers the
signing, identity, node-to-node and browser-facing defenses.

Procedures (provisioning, rotation, outages) are in [KEK and key rotation](operations/kek-and-key-rotation.md).

## Key inventory

```diagram
caption: "Where each secret lives. Only the left column is in the bucket, and none of it is usable without the KEK or is more than a verifier."
nodes:
  - { id: sk, label: repo signing keys, sub: "`a/{did}` · KEK-wrapped", at: [0, 0], size: [10, 2.6], tone: amber }
  - { id: totp, label: TOTP secrets, sub: "`p/{did}` · KEK-wrapped", at: [0, 3.4], size: [10, 2.6], tone: amber }
  - { id: pw, label: password hashes, sub: Argon2id · SHA-256 · HMAC, at: [0, 6.8], size: [10, 2.6], tone: amber }
  - { id: kek, label: KEK access, sub: SA key file or KEK file, at: [15, 0], size: [10, 2.6], tone: accent }
  - { id: plck, label: PLC rotation key, sub: "`vw1.` file", at: [15, 3.4], size: [10, 2.6], tone: accent }
  - { id: tok, label: jwt · admin · internal, sub: "S3 · SMTP · peer TLS", at: [15, 6.8], size: [10, 2.6], tone: accent }
  - { id: rec, label: operator recovery key, sub: paper / vault, at: [30, 0], size: [10, 2.6], tone: violet }
  - { id: ca, label: peer CA key, sub: "`ca.key`", at: [30, 3.4], size: [10, 2.6], tone: violet }
  - { id: user, label: user recovery keys, sub: the user's own device, at: [30, 6.8], size: [10, 2.6], tone: violet }
groups:
  - { label: bucket, around: [sk, totp, pw], tone: amber }
  - { label: "host · /run/vlpds", around: [kek, plck, tok], tone: accent }
  - { label: offline, around: [rec, ca, user], tone: violet }
```

| Key or secret | Used for | Where it lives | Protected by | Rotated by |
|---|---|---|---|---|
| KEK | wrapping the keys below | Cloud KMS (`--gcp-kms-key`) or a 32-byte file (`--kek-file`) | KMS IAM, or file mode 0400 | new KMS version or key, then `rewrap-secrets` |
| Repo signing key (secp256k1, one per account) | commits, service-auth JWTs | account row `a/{did}` | KEK, bound to the DID | admin `updateAccountSigningKey` |
| Reserved signing key | migrations in (`reserveSigningKey`) | `p/_reserved:{did:key}` | KEK, bound to the did:key | used once |
| TOTP secret | second factor | `p/{did}` private row | KEK, bound to the DID | the user re-enrolls |
| PLC rotation key (one per deployment) | signing PLC ops for every DID | a `vw1.` file on each node (`--plc-rotation-key-file`) | KEK; never in the bucket | `rotate-plc-keys` |
| Operator recovery key | undoing a bad PLC op within 72 h | offline; nodes get only its did:key | not on any host | `ensure-recovery-key` with a new one |
| User recovery keys | the user's own control of their DID | the user's device | the user | the account page |
| `jwt_secret` | legacy session JWTs (HMAC); derives the OAuth signing key, DPoP nonce, CSRF, refresh-token and email-token MAC keys | a secret file | file mode | no overlap: changing it signs everyone out |
| Admin and internal tokens | admin XRPC; node-to-node calls | secret files | file mode | restart with a new value |
| Peer TLS CA and node certs | mTLS between nodes | `ca.crt` + node pair in `--peer-tls-dir`; `ca.key` offline | CA key offline | renew without restart ([Peer TLS](#peer-tls)) |
| Passwords, app passwords, recovery codes, email tokens | verifying, not using | private rows | Argon2id; SHA-256; HMAC keyed by the TOTP secret; HMAC under `jwt_secret` | n/a |
| DPoP keys | binding OAuth tokens | the client | never on the server | the client |

Every secret flag has a file form (`--jwt-secret-file`, `--admin-token-file`, `--s3-secret-key-file`,
`--email-smtp-url-file`, ... and `VLPDS_*_FILE`). The node reads each file once at startup, drops one
trailing newline, and refuses to start if a file is empty or unreadable or if the plain form is also set.
Environment variables show up in `docker inspect` and rendered compose files; files don't. The Ansible
role writes each secret as a 0400 file (uid 10001) under `vlpds_secrets_path`, mounts the directory read-only
at `/run/vlpds`, and passes only the `_FILE` variables. Outside `--dev-mode` a node also refuses dev-default
secrets, secrets shorter than 32 bytes, and a JWT secret, admin token and internal token that aren't all
different.

## Secrets at rest

```diagram
caption: A secret is wrapped together with what it is for and whose it is. The stored string names the KEK that wrapped it, so a node with several KEKs knows which one to use.
nodes:
  - { id: sec, label: secret, sub: signing key · TOTP secret, at: [0, 0], size: [9, 3] }
  - { id: aad, label: bound to, sub: purpose + DID, at: [0, 5], size: [9, 3] }
  - { id: kek, label: KEK, sub: Cloud KMS or local, at: [15, 0], size: [8, 8], tone: violet }
  - { id: blob, label: "`vw1.{kid}.{…}`", sub: "in `a/{did}` or `p/{did}`", at: [29, 2.5], size: [10, 3], shape: store, tone: amber }
edges:
  - "sec -> kek: plaintext"
  - "aad -> kek: AAD"
  - "kek -> blob: wrapped"
```

The wrapper takes the secret plus associated data (`vlpds-secret-v1`, the purpose, the DID or did:key) and
produces `vw1.{kid}.{base64url}`. A wrapped key copied into another account's row, or used for another
purpose, fails to unwrap. The `kid` names the KEK: `G…` for a Cloud KMS key, `L…` for a local one.

- **Cloud KMS** (production). The secret goes to KMS `encrypt` / `decrypt` with the associated data and
  CRC32C checks; the KEK never leaves KMS, and every unwrap is in the KMS audit log. A copy of the bucket is
  useless without decrypt permission on the key. Credentials come from the GCE metadata server, or off GCE
  from a service-account JSON key (`--gcp-credentials-file`) holding only `cloudkms.cryptoKeyEncrypterDecrypter`
  on that one key. `deploy/gcp` has an OpenTofu module that creates such a key in a multi-region key
  ring.
- **Local KEK** (`--kek-file`): XChaCha20-Poly1305 with a random nonce per wrap. Fine for a personal server.
  Back the file up offline: it is the only way to read the stored keys. `--dev-mode` falls back to a
  well-known dev KEK, which is refused outside dev mode.

**Hot path.** Unwrapped signing keys are cached per account (the `signing_keys` cache, sized from
`--cache-budget-mb`). A loaded repo holds its key, so commits never touch the KEK. A key is unwrapped once
per account per cache lifetime: at a cold repo load, a proxy service-JWT miss, or `getServiceAuth`. New
accounts never unwrap: their key is cached when it is created. A local unwrap costs ~50 µs; a Cloud KMS
unwrap adds one round trip (typically 5–30 ms in-region). KMS calls are limited per node: 64 unwraps in
flight (`--kms-concurrency`), a separate pool of 16 for wraps, 5 s each.

**When the key service fails**, warm accounts keep writing. A cold account's write gets 503
`KeyUnavailable` with nothing applied, and createAccount, `reserveSigningKey` and TOTP setup fail the same way.
Reads, exports and the firehose are unaffected. After a failure a node fails cold unwraps fast for 1 s
before trying KMS again. A *rejected* unwrap (wrong KEK, corrupt row, unknown `kid`) is a 500 and
`VlpdsSecretUnwrapRejected`. Watch `vlpds_kms_requests_total{backend,op,result}` and
`VlpdsKeyServiceUnavailable`; the procedure is [Key service outage](operations/kek-and-key-rotation.md#key-service-outage).

> [!WARNING]
> The KEK is part of every backup. Lose it (a destroyed KMS key, a lost `--kek-file`) and no account can sign
> again until each gets a new signing key and a PLC update. Keep old KEK versions for at least the backup
> retention.

## Signing hardening

```diagram
caption: Every signature that leaves the node (commits, service-auth JWTs, OAuth access tokens, PLC operations) is checked against the key's own public key first. A bad one is never emitted.
nodes:
  - { id: sign, label: sign, sub: hedged nonce, at: [0, 0], size: [8, 3], tone: accent }
  - { id: verify, label: verify, sub: vs cached public key, at: [12, 0], size: [9, 3], tone: accent }
  - { id: emit, label: sequence · return, sub: on the firehose, at: [25, 0], size: [9, 3], tone: solid }
  - { id: again, label: sign again, sub: fresh nonce, at: [12, 6], size: [9, 3], tone: danger }
  - { id: fail, label: 503 SignatureFault, sub: "3 in 1 min: exit 6", at: [25, 6], size: [9, 3], tone: danger }
edges:
  - sign -> verify
  - "verify -> emit: valid"
  - "verify.b -> again.t: mismatch"
  - "again.r -> fail.l: fails again"
```

ECDSA leaks the private key if one faulty signature (a bad DIMM, Rowhammer, an overheating CPU) and a correct
one over the same message are both public, and commit signatures are public on the firehose. So:

- **Hedged nonces.** 32 fresh random bytes go into the RFC 6979 nonce. Signatures are still low-S compact, but
  no longer reproducible, so a faulty one can't be paired with a correct one.
- **Verify after sign.** The signature is checked over a freshly hashed message against the cached public key
  before it can be sequenced or returned. On a mismatch the node logs, counts
  `vlpds_signature_verify_failures_total{purpose}` and signs again; a second failure answers 503
  `SignatureFault` with nothing applied. Three failures within a minute fail-stop the node (`signature_fault`,
  exit 6). A repo load also checks that the cached private key still derives its public key (`purpose="key_load"`).

Verification costs ~15 µs per signature, about +15% of a commit's CPU, measured. Every signature is
checked: a sampled check would leave the skipped ones free to leak the key.

A `VlpdsSignatureFault` means the host computed something wrong. Drain it and replace the hardware, even after one
fault: see `ops/RUNBOOK.md` "VlpdsSignatureFault".

## PLC rotation key and recovery keys

```diagram
caption: "A did:plc's rotation keys, in priority order. Within 72 h of an op, a key earlier in the list can replace it with its own; the server's key is last."
nodes:
  - { id: user, label: user's keys, sub: account page · /migrate, at: [0, 0], size: [10, 3], tone: blue }
  - { id: op, label: operator recovery key, sub: "`--plc-recovery-did-key`", at: [14, 0], size: [11, 3], tone: violet }
  - { id: srv, label: server rotation key, sub: same on every node, at: [29, 0], size: [10, 3], tone: accent }
groups:
  - { label: a DID's rotation keys, around: [user, op, srv], tone: muted }
edges:
  - "user -> op: outranks"
  - "op -> srv: outranks"
```

Accounts get real `did:plc` identities, registered with the PLC directory (`--plc-url`) before the account
exists. A new DID's `rotationKeys` are `[user key?, operator recovery key?, server rotation key]`:

- **Server rotation key**: one secp256k1 key for the whole deployment, which signs handle changes,
  signing-key updates and migrations out. It comes from `--plc-rotation-key-file`, a `vw1.` file wrapped under
  the KEK by `vlpds --wrap-plc-rotation-key`, so the file alone is useless. It is unwrapped once at startup
  (a KMS outage fails the start) and never written to the bucket. PLC ops it signs are verified like
  commits (`purpose="plc_operation"`). Outside `--dev-mode` a node won't start without it.
- **Operator recovery key**: an offline key the operator generates with `vlpds --generate-did-key`. Nodes only
  get its did:key (`--plc-recovery-did-key`). It outranks the server key, so a leaked rotation key or a bad
  deploy can be undone within 72 h by an op signed offline. `vlpds admin ensure-recovery-key` adds it to
  DIDs created before it was set, just ahead of the server key so user keys keep their priority.
- **User recovery keys**: keys the user holds, always first. The account page's Security tab adds or removes
  one (request an emailed code, sign, submit), and `/migrate`'s advanced mode adds one when moving in. A key
  can be made in the browser (the private key is downloaded, never sent) or pasted as a did:key. A DID that
  lists a key that isn't ours is marked `plcExternalOps`, and its document is then read from the directory.

Only the presence of the server key is checked (on submit and activation), so users may put their own
keys ahead of it. Losing the server key means the server can no longer update its accounts' DIDs; users with
their own recovery key can still recover theirs.
Procedures: [PLC rotation key](operations/kek-and-key-rotation.md#plc-rotation-key),
[Operator recovery key](operations/kek-and-key-rotation.md#operator-recovery-key).
How a move uses these keys: [Migration](migration.md).

## Peer TLS

```diagram
caption: Nodes talk to each other only over mutual TLS on `--peer-listen`, with certificates from the cluster's own CA. Clients reach the client port through Caddy, never the peer port.
nodes:
  - { id: ca, label: cluster CA, sub: "`ca.key` offline", at: [0, 3], size: [9, 3], tone: violet }
  - { id: na, label: node A, sub: "`vlpds://node/a`", at: [14, 0], size: [9, 3], tone: accent }
  - { id: nb, label: node B, sub: "`vlpds://node/b`", at: [14, 6], size: [9, 3], tone: accent }
  - { id: caddy, label: Caddy, sub: "to `--listen`", at: [28, 3], size: [8, 3], tone: muted }
edges:
  - { from: ca.r, to: na.l, label: issues, dash: true }
  - { from: ca.r, to: nb.l, dash: true }
  - "na.b <-> nb.t: h2 · TLS 1.3"
  - caddy.l -> na.r
  - caddy.l -> nb.r
```

Forwards, `/internal/*` and log streams run HTTP/2 over TLS 1.3 with client certificates; there is no cleartext
mode. A node certificate (ECDSA P-256, 365 days by default) names its node in a `vlpds://node/<id>` URI SAN and
carries its `--advertise-url` host. Peers check the chain, the host, and that the certificate names the node
whose lease advertises that address. The internal token is still required on top. A lone node needs none of
this: without `--peer-listen` there is no peer port and no `/internal/*`.

`vlpds admin tls ca` and `vlpds admin tls issue` make the CA and node certificates. Nodes reload changed files on
SIGHUP or within 60 s, so renewal needs no restart, and a CA rotation trusts a bundle of old and new CAs while
certs are reissued. Alerts: `VlpdsPeerTlsCertExpiring` (14 days before expiry),
`VlpdsPeerTlsReloadFailing`, `VlpdsPeerTlsHandshakeFailures`. Procedure: `ops/RUNBOOK.md` "Peer TLS (mTLS
between nodes)"; joining a node: [Scaling and clustering](operations/scaling-and-clustering.md#peer-tls).

## Web security

```facts
- { value: "'self'", label: the only script source, note: "every UI page; OAuth pages allow only hashed inline code", tone: accent }
- { value: "DENY", label: framing, note: "frame-ancestors 'none' + X-Frame-Options" }
- { value: public, label: addresses only for outbound fetches, note: "private, loopback and link-local are refused", tone: blue }
- { value: "25", unit: rate limits, label: built in, note: "per IP, account or node; tunable live", tone: amber }
```

- **Headers.** The web UI (`/`, `/account`, `/admin`, `/docs`) is served with a same-origin CSP
  (`default-src 'none'`, scripts, styles and API calls from `'self'` only, `base-uri` and `form-action 'none'`),
  `X-Frame-Options: DENY`, `nosniff` and `no-referrer`. `/migrate` also allows `connect-src https:`, because it
  talks to the account's old PDS. The OAuth pages are server-rendered with a CSP that allows only their own
  style and auto-submit script by hash, and posts only to this server and the client's redirect.
- **Outbound requests** to hosts that users or clients name (DID and handle resolution, OAuth client metadata,
  services named in DID documents, push registration) go through one client whose resolver drops non-public
  addresses, so a hostname can't be used to reach internal services. Responses are size-capped. See
  [Proxying](proxying.md#outbound-safety).
- **Rate limits** are checked before any expensive work (Argon2, KMS, PLC). Login and OAuth limits are on
  [OAuth and 2FA](oauth-2fa.md#passwords-and-argon2); `reserveSigningKey` is capped at 100/h per IP and 5,000 new
  keys a day per node so a flood can't spend the KMS quota. Limits and overrides live in the
  [admin console](operations/admin-console.md).
- **Handles** follow the reference PDS's reserved-name list and slur filter; see
  [Email and moderation](operations/email-and-moderation.md#handle-policy).
- **Metrics** listen on `127.0.0.1:9583` by default, not on the public port.
