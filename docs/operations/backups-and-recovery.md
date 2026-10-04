---
title: Backups and recovery
section: Operations
order: 105
status: ready
summary: "What can be lost and how to get it back: bucket versioning and replication, point-in-time restore, losing a host, and what is still design rather than built."
---

```hero
diagram:
  caption: "Today. Everything durable is in one prefix of one bucket, and the bucket's own durability is the only protection. The secrets that open it live outside and must be copied offline by hand. The dashed layers are the recommended backup plan: designed, not built."
  nodes:
    - { id: node, label: vlpds nodes, sub: disk is a cache, at: [0, 4], size: [8, 3], tone: accent }
    - { id: bucket, label: bucket + prefix, sub: "log · state · blobs · leases", at: [14, 4], size: [11, 3], shape: store, tone: amber }
    - { id: l1, label: Versioning + lock, sub: "layer 1 · undo deletes", at: [30, 0], size: [10, 2.6], shape: note, tone: muted }
    - { id: l2, label: Shard checkpoints, sub: "layer 2 · undo a bad build", at: [30, 4.2], size: [10, 2.6], shape: note, tone: muted }
    - { id: l3, label: Off-site copy, sub: "layer 3 · lose the bucket", at: [30, 8.4], size: [10, 2.6], shape: note, tone: muted }
    - { id: keys, label: "KEK · PLC key", sub: not in the bucket, at: [14, 13.5], size: [11, 2.6], tone: violet }
    - { id: offline, label: Offline copy, sub: by hand · do it now, at: [30, 13.5], size: [10, 2.6], tone: ok }
  groups:
    - { label: design · not built, around: [l1, l2, l3], tone: muted }
  edges:
    - "node -> bucket: every acked write"
    - { from: bucket.r, to: l1.l, dash: true }
    - { from: bucket.r, to: l2.l, dash: true }
    - { from: bucket.r, to: l3.l, dash: true }
    - { from: keys.l, to: node.b, label: unwrap at start, via: [[4, 14.8]] }
    - { from: keys.r, to: offline.l, label: back up }
facts:
  - { value: "0", label: backup layers built, note: "the bucket's durability only (S3 Standard: multi-AZ, 11 nines)", tone: rust }
  - { value: "2", unit: secrets, label: to copy offline today, note: "the KEK and the wrapped PLC rotation key; without the KEK nothing restored can sign", tone: violet }
  - { value: "0", label: data to restore after losing a host, note: "peers or a restart replay from the bucket", tone: blue }
  - { value: "~$230", unit: /mo, label: "planned layers at Bluesky scale", note: "~9% of the object-store bill; blob replication extra", tone: amber }
```

> [!WARNING]
> vlpds has no backups yet. Every acknowledged write is in the object store, so losing a node, a
> disk or a whole host loses nothing. But nothing protects against a **logical** loss: a delete, a
> bad build writing bad state, or losing the bucket, the cloud account or the region.

This page says what can go wrong, what protects you today (little), what you can switch on at the
provider, and the backup design that isn't built yet. The design in full, with its cost model, is
`DESIGN.md` "Backups and restore".

## Threat model

```diagram
caption: The four threats on the left; the layer of the recommended plan that answers each on the right. None of the layers exists in vlpds yet.
nodes:
  - { id: t1, label: Hand delete, sub: wrong prefix · a script, at: [0, 0], size: [10, 2.6], tone: danger }
  - { id: t2, label: Bucket lost, sub: "deleted · leaked keys", at: [0, 3.6], size: [10, 2.6], tone: danger }
  - { id: t3, label: Region lost, sub: S3 survives an AZ only, at: [0, 7.2], size: [10, 2.6], tone: danger }
  - { id: t4, label: Bad build, sub: "bad state · GC bug", at: [0, 10.8], size: [10, 2.6], tone: danger }
  - { id: p1, label: Versioning + lock, sub: layer 1, at: [22, 0], size: [10, 2.6], shape: note, tone: muted }
  - { id: p3, label: Off-site copy, sub: "layer 3 · other account", at: [22, 5.4], size: [10, 2.6], shape: note, tone: muted }
  - { id: p2, label: Shard checkpoints, sub: "layer 2 · restore to a cut", at: [22, 10.8], size: [10, 2.6], shape: note, tone: muted }
  - { id: p5, label: Logical export, sub: "layer 5 · CARs monthly", at: [22, 14.4], size: [10, 2.6], shape: note, tone: muted }
edges:
  - t1 -> p1
  - t2.r -> p3.l30
  - t3.r -> p3.l70
  - t4 -> p2
  - { from: t4.r, to: p5.l, label: format bugs, dash: true, labelAt: [16.6, 14.6] }
```

1. **The bucket is deleted, or credentials are misused.** Anything that can delete objects can
   delete everything: a leaked node key, a compromised operator account.
2. **A bad build** writes wrong state, writes bad commits (already sent to relays), or deletes too
   much: a garbage-collection or retention bug removing live SSTs or segments still needed for replay.
3. **An operator deletes objects by hand.** The [Runbook](runbook.md#what-not-to-do) lists the
   prefixes never to touch.
4. **A region is lost.** S3 Standard survives an availability zone, not a region.

## What exists today

| Protection | Status |
|---|---|
| Object-store durability | **The only one.** S3 Standard is multi-AZ; R2 and GCS have their own guarantees. |
| Bucket versioning / soft delete | **Off by default; your choice at the provider.** S3 and GCS support it; vlpds's conditional writes apply to the current version, so it should be transparent (design analysis, not tested). R2 has no object versioning. |
| `check-repo` / `rebuild-repo` | **Built.** Repair one repo whose derived state (MST nodes, indexes) is wrong. `rebuild-repo` refuses if records were lost: that needs a restore. See [Admin console and CLI](admin-console.md#admin-cli). |
| Log segments, 72 h | **Built, but not a backup.** `--log-retention` keeps segments for firehose backfill and replay; a bad build's entries are in them too. |
| Logical export | **By hand.** `com.atproto.sync.getRepo` CARs plus blobs per account (e.g. with goat) are a cheap last resort for a personal PDS. They leave out keys, password hashes, email, sessions and takedowns. |
| Named SlateDB checkpoints, restore tooling, off-site copy, log archive | **Not built.** |

What to do on a new deployment, today:

- Copy the **KEK** (or keep the Cloud KMS key undeletable), the **wrapped PLC rotation key** and the
  secrets vault somewhere offline. They are not in the bucket. See
  [KEK and key rotation](kek-and-key-rotation.md#kek-provisioning).
- Give the nodes a key pair scoped to the bucket, with no rights to delete object versions, change
  bucket policy or lifecycle rules (S3 IAM: no `s3:DeleteObjectVersion`). Then versioning, if you
  turn it on, can't be undone by a leaked node key.
- On S3 or GCS, turn on versioning (or keep GCS soft delete) if the cost below is acceptable.

## Options

The recommended plan, priced at Bluesky's scale (56 M repos, ~350 commits/s, 3 nodes). All of it is
**design**: none is implemented in vlpds or the Ansible role.

| Layer | Protects against | Loses (RPO) | Back in (RTO) | ~$/mo on S3 |
|---|---|---|---|---|
| 1. Versioning, 7-day noncurrent expiry, Object Lock (governance, 7 d); node keys can't delete versions | hand deletes, app-key misuse, GC or retention bugs | nothing | hours | ~$26 |
| 2. Named SlateDB checkpoint per shard every 6 h, kept 8 d; `--log-retention 8d` | a bad build: roll back to any cut in 8 days | nothing before the cut | ~30–60 min | ~$20 |
| 3. Off-site: another account and region, Object Lock (compliance, 30 d); daily incremental checkpoint copy, log archived every minute | bucket or account loss, leaked admin keys, region loss | ~1–2 min | ~1–2 h | ~$180 |
| 4. Blobs: cross-region replication to the backup account | the same, for blobs | minutes | needs code to serve | ~$2.4k |
| 5. Logical export: CARs + encrypted account dump, monthly | format bugs, leaving vlpds | a month | days | ~$5 |

Layers 1–3 and 5 add ~$230/mo, ~9% of the ~$2.5k object-store bill at that scale. Blobs (~350 TB)
dominate layer 4 and are a separate decision. Prices are list prices and restore times are
estimates; none of it is measured.

Why not just replicate the bucket? Cross-region replication is asynchronous and per object, so a
replica can hold a manifest before the SSTs it names, or a fence before the segments under it. It is
consistent only at a cut every earlier version has reached, and at today's small segments it costs
~$1.9k/mo in requests. It fits blobs (large, immutable), not the log and state.

## Point-in-time restore of a cluster

```steps
- title: Pick the cut, a firehose seq S
  body: The restored cluster holds exactly the entries with seq ≤ S of every log, which is what subscribers saw through S.
- title: Start each shard from a snapshot at or before S
  body: "A named checkpoint (or a copy of one manifest and its SSTs). Each SlateDB state is a consistent snapshot: its applied marker is written in the same batch as the segment it marks."
- title: Replay each shard's log spans up to S
  body: "The `assign/` span history says which logs and ordinals belong to the shard. Snapshot right after every split or merge so a restore never spans one."
- title: Rebuild the global objects
  body: "`handle/` and `email/` from the restored account rows. `nodes/`, `writers/` and `retain/` start fresh; `assign/` gets a seq floor above the highest seq ever emitted, so seqs never go backwards for subscribers."
- title: Keep blobs as a superset
  body: They are content-addressed. Undelete anything referenced; blob GC reclaims the rest.
- title: Tell the relays
  body: "For each repo with entries after S, emit a `#sync` with the restored head so relays resync instead of rejecting the next commit. Writes after S are lost: that is the point when S is just before a bad build."
```

The restore goes into a **new prefix**, so the old one stays untouched until the result is verified.
None of the tooling exists: named checkpoints, a `restore --cut S` command, the log archiver and the
off-site copy job are all still to be written. If a bad build stored wrong mutations but correct
commit frames, replay with a fixed build can re-derive records and heads from the commits' CARs.

## Losing a host

```steps
- title: Nothing to do for data safety
  body: "In a cluster, peers fence the dead node's log and take its shards: 3–5 s if its port refuses connections, ~12 s plus replay if the host is unreachable (default 10 s lease). A lone node waits for you."
- title: Make sure the old process can't come back
  body: Power it off. A zombie would find its log fenced or its shards reassigned and fail-stop, but don't rely on it.
- title: Start a replacement on the same bucket and prefix
  body: "Same secrets, same `--node-id` if you can (it fences its predecessor's log at once). It serves writes once its clock passes the predecessor's last lease expiry: ~11 s after a crash at a 10 s TTL, ~53 s at the `tiny` profile's 60 s (measured)."
- title: Expect cold caches
  body: "Point `--cache-dir` at local NVMe. The SST disk cache starts empty, so the first reads and repo loads go to the bucket."
- title: Verify
  body: "`vlpds admin cluster status`: lease valid and every shard owned; `vlpds_last_exit_reason_info` on the new process says how the old one ended."
```

There is nothing to restore: the bucket holds every acknowledged write, and the disk only ever held
a cache. The procedure is RUNBOOK
[Replacing a dead host](https://github.com/jazware/vlpds/blob/main/ops/RUNBOOK.md#replacing-a-dead-host);
takeover mechanics are in [Architecture](../architecture.md#failure-and-takeover).

## Signing keys

```facts
- { value: "KEK", label: is part of every backup, note: "signing keys, reserved keys and TOTP secrets are stored only wrapped under it", tone: violet }
- { value: "0", label: users who can sign after losing it, note: "every account would need a new key and a PLC rotation", tone: rust }
- { value: "> 72 h", label: keep a retired KEK at least, note: "longer than log retention and any backup holding secrets wrapped under it", tone: amber }
```

Account signing keys never sit in the bucket in the clear, so a copy of the bucket alone doesn't let
its reader sign as anyone. It still holds password hashes (argon2id), app-password and recovery-code
hashes, and email-token digests. The catch is that **the KEK is now part of every backup**:

- Lose a local KEK file, or let a Cloud KMS key be destroyed, and no restored account can sign. Back
  the local KEK up offline (two copies); for Cloud KMS use a multi-region key, the maximum
  destroy-scheduled duration, and IAM that keeps `cloudkms.cryptoKeyVersions.destroy` away from node
  and operator roles.
- After a KEK rotation, keep the old KEK (or keep the old KMS version disabled, not destroyed) for as
  long as any backup or log segment may still hold secrets wrapped under it.
- The wrapped PLC rotation key needs the KEK to open, and isn't in the bucket either.

Details: [Keys and security](../keys-security.md#secrets-at-rest),
[KEK and key rotation](kek-and-key-rotation.md#kek-rotation).
