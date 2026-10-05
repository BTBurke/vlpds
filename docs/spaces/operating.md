---
title: Operating Spaces
section: Spaces
order: 205
status: draft
summary: "Turning Spaces on, its limits, what to watch and what it costs: flags, metrics, the dashboard row, the alerts, and measured numbers next to the reference PDS."
---

```hero
diagram:
  caption: "Three things to watch and the alert on each. A node without --spaces exports no vlpds_space_* series, and one with it exports what these alerts read at 0 from the start."
  nodes:
    - { id: cr, label: Credential reads, sub: "`…_credential_checks_total`", at: [0, 0], size: [13, 3], tone: blue }
    - { id: ob, label: notifyWrite outbox, sub: "`…_outbox_oldest_seconds`", at: [16, 0], size: [13, 3], tone: accent }
    - { id: fo, label: Fan-out to syncers, sub: "`…_notify_total{hop=fanout}`", at: [32, 0], size: [13, 3], tone: violet }
    - { id: a3, label: VlpdsSpaceCredentialRejectsHigh, sub: "> 25% refused · 15 min", at: [0, 6], size: [13, 3], tone: danger }
    - { id: a1, label: VlpdsSpaceOutboxBacklog, sub: "oldest row > 1 h · 10 min", at: [16, 6], size: [13, 3], tone: danger }
    - { id: a2, label: VlpdsSpaceNotifyFanoutFailing, sub: "> 50% failing · 30 min", at: [32, 6], size: [13, 3], tone: danger }
  edges:
    - cr -> a3
    - ob -> a1
    - fo -> a2
facts:
  - { value: off, label: by default, note: "`--spaces` turns it on", tone: rust }
  - { value: "100k", unit: records, label: per space repo, note: "`--space-repo-max-records`", tone: amber }
  - { value: "7 d", label: of oplog, note: "`--space-oplog-retention`", tone: violet }
  - { value: "4", unit: alerts, label: all tickets, note: "`VlpdsSpace*` in `ops/alerts.yml`", tone: blue }
```

Spaces is an alpha that changes every week upstream, so leave it off unless you're testing against
it. With no space traffic, a node with it on makes one conditional GET of the revocations object every
5 min and runs an oplog sweep about every 6 h, which skips repos younger than the window.

## Flags

| Flag | Default | What it does |
|---|---|---|
| `--spaces` (`VLPDS_SPACES`) | off | serves `com.atproto.space.*` and `com.atproto.simplespace.*` here. An unbuilt one answers 501 |
| `--space-repo-max-records` | 100000 | the most records one account's repo in one space may hold. A write past it gets `InvalidRequest` |
| `--space-oplog-retention` | `7d` | how long ops stay for `listRepoOps`. `off` keeps them all |
| `--max-exports`, `--export-stall-secs` | as for `sync.getRepo` | `space.getRepo` takes the same export slots and stall timeout |

## Fixed limits

| Limit | Value |
|---|---|
| Space credential lifetime | 10 min minted by vlpds, 3,600 s accepted at most, 5 s of clock skew |
| Delegation token and client attestation | 60 s minted, 300 s accepted at most, single use |
| `Signature-Input` and `Signature` headers | 8 KiB each |
| Revocations | 1–100 `jti`s per call, each held 3,610 s |
| `applyWrites` | 200 ops |
| `notifyWrite` with a future `repoRev` | refused past 5 min |
| Outbox | 262,144 rows in memory, 256 sends in flight, retries for 24 h |
| Fan-out | 4,096 queued, 256 per lane, 4,096 and 16 sends in flight per service host |
| Notify registrations | 24 h |
| Memory | space heads cache 64 MiB, credential cache 50,000 entries |

## Metrics

| Metric | What it shows |
|---|---|
| `vlpds_space_writes_total{op,result}` | space writes by method and result |
| `vlpds_space_reads_total{method,auth}` | reads by method and auth (`credential` or `oauth`) |
| `vlpds_space_list_repo_ops_total{path}`, `vlpds_space_list_repo_ops_seconds{path}` | `noop` (answered from memory) vs `scan`, and server time for each |
| `vlpds_space_notify_total{hop,result}` | notify hops: `out` (this node's writes), `in` (as an authority), `fanout` (to syncers) |
| `vlpds_space_notify_ack_seconds` | a write's ack to the authority's 200 |
| `vlpds_space_outbox_rows`, `vlpds_space_outbox_oldest_seconds` | outbox depth and the age of its oldest row (rows held for an inactive writer don't count toward the age) |
| `vlpds_space_outbox_overflow_total` | rows left in the bucket because the outbox was full |
| `vlpds_space_fanout_queue_depth`, `vlpds_space_fanout_dropped_total{reason}`, `vlpds_space_fanout_coalesced_total` | fan-out backlog, drops and replaced forwards |
| `vlpds_space_credential_cache_total{result}` | credential cache hits and misses |
| `vlpds_space_credential_checks_total{result}` | `ok`, or why a read was refused: `bad_sig`, `expired`, `revoked`, `audience`, `space` |
| `vlpds_space_credentials_issued_total{result}`, `vlpds_space_delegations_total` | credentials issued as an authority, delegation tokens minted |
| `vlpds_space_revocations` | revoked credentials in force |
| `vlpds_space_export_bytes`, `vlpds_space_oplog_pruned_total` | `getRepo` memory, oplog ops pruned |
| `vlpds_space_sign_seconds` | signing one commit for a reader (every `getLatestCommit`, `listRepoOps` and `getRepo` signs its own) |
| `vlpds_space_digest_mismatch_total` | space repos whose head disagreed with the set hash recomputed from their records. Should stay 0 |
| `vlpds_space_repos` | space repos in this node's shards, counted by the oplog retention sweep every ~6 h |
| `vlpds_space_imports_total{result}` | `vlpds.space.importRepo` calls: `ok`, `refused` (a bad CAR, signature or hash), `error` |
| `vlpds_space_operator_reads_total{method}` | audited operator reads of space data |

The internals dashboard has a Spaces row built from these: writes, write → notify ack, outbox rows and
oldest row per node, notifies by hop and their failure ratio, `listRepoOps` noop vs scan and its
server time, reads by auth, the credential cache hit ratio, credential checks by result, issuance,
the fan-out queue and drops, and revocations held.

## Alerts

| Alert | Fires when | First thing to check |
|---|---|---|
| `VlpdsSpaceOutboxBacklog` | a node's oldest outbox row is over 1 h old for 10 min | `notifyWrite by hop and result`: `out retry` means the authority is failing. Inactive writers' rows wait without aging the outbox |
| `VlpdsSpaceNotifyFanoutFailing` | over 50% of fan-out sends fail, at over 0.1/s, for 30 min | one syncer down (nothing to do) or this node's egress |
| `VlpdsSpaceCredentialRejectsHigh` | over 25% of credential reads are refused, at over 0.5/s, for 15 min (expired ones left out) | which `result` dominates. One client stuck on `bad_sig` is that app's bug |
| `VlpdsSpaceDigestMismatch` | a space repo's head disagrees with its records | run `vlpds admin check-space DID SPACE` |

Each has a section in `ops/RUNBOOK.md`. All four are tickets, since a space write is durable and
readable at its 200 whatever these say. For an outbox backlog, one failing authority needs nothing
from you (the next retry, at most ~1 h away, delivers the newest rev). Many failing at once points at
this node's DNS or egress.

`VlpdsSpaceDigestMismatch` counts what `vlpds.admin.checkSpace` finds, so it fires only once that
check runs (`vlpds admin check-space`). A refused `importRepo` with a bad set hash is the
uploader's problem and counts in `vlpds_space_imports_total{result="refused"}` instead.

## What it costs

Measured on a laptop with the interop harness against the reference PDS at `5b95b2f2`, with one
vlpds node on MinIO. All of these are client-side.

| | vlpds | Reference PDS |
|---|---|---|
| Space write (createRecord, putRecord, applyWrites) | 0.80 ms p50 · 1.33 ms p99 | ~4.2 ms p50 · 6.5 ms p99 |
| Sequential write throughput, one account | 1,184 writes/s | 232 writes/s |
| No-op `listRepoOps` poll | 0.28 ms p50 · 0.54 ms p99 · 0 bucket ops | 6.7 ms p50 |
| Delta pull | 0.41 ms p50 · 1.9 ms p99 | 8.3 ms |
| Notify end to end | 1.7 ms p50 · 5.5 ms p99 | 4.5 ms p50 · 8.3 ms p99 |
| `listRecords` | 0.5 ms | 5.7 ms |
| `getDelegationToken` | 0.5 ms | 2.5 ms |
| Public commit p99 with the spaces load on | 9.4 → 6.3 ms | 29 → 39 ms |

- Server time for a space write averages 0.59 ms. 0.44 ms of that is the durable commit, nearly all
  of it the segment PUT. Waiting for a segment and applying take ~0.01 ms each.
- Server time for a no-op poll averages 0.12 ms, and 0.17 ms for a delta pull.
- Public commit p99 held up with the spaces load running, but p50 went from 1.3 to 3.1 ms.
- Sequential writes cost one bucket PUT each, the same as public writes. Concurrent space writes to
  one repo don't share segments yet. At a concurrency of 4 they cost 1.0 PUT per write against 0.68
  for public writes. That's the likely cause of the public p50 rise, and the fix isn't built yet.

The in-tree micro-bench (`just spaces-microbench`, write-up in `bench/results/spaces-sync.md`)
measures server CPU per sync request and bucket ops per write. It hasn't been run yet.
