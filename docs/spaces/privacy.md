---
title: Privacy guardrails
section: Spaces
order: 204
status: draft
summary: "What keeps space data private on vlpds: no firehose path, no proxy escape, the blob rule, audited operator access and takedowns. Each guardrail has a test, and the parts not built yet say so."
---

```hero
diagram:
  caption: "The privacy line. A space write is stored and served only to credentialed readers. None of the paths on the right may ever carry it, and the leak tests plant sentinels to prove it."
  nodes:
    - { id: w, label: Space write, sub: "values · rkeys · URIs", at: [0, 4.2], size: [9, 3], tone: ink }
    - { id: e, label: Private entry, sub: "`s*` rows · empty frame", at: [12.5, 4.2], size: [9, 3], tone: accent }
    - { id: rd, label: Credentialed reads, sub: "space.* only", at: [12.5, 10], size: [9, 3], tone: ok }
    - { id: fh, label: Firehose, sub: "live · sharded · cursor · S3 · peers", at: [27, 0], size: [13, 3], tone: danger }
    - { id: pub, label: Public repo, sub: "sync.getRepo · rev · commit", at: [27, 4.2], size: [13, 3], tone: danger }
    - { id: px, label: Other hosts, sub: "via atproto-proxy", at: [27, 8.4], size: [13, 3], tone: danger }
  edges:
    - "w -> e"
    - { from: e.b, to: rd.t, label: served }
    - { from: e.r, to: fh.l, label: never, dash: true, tone: danger }
    - { from: e.r, to: pub.l, label: never, dash: true, tone: danger }
    - { from: e.r, to: px.l, label: "501 here", dash: true, tone: danger }
facts:
  - { value: "0", unit: sentinels, label: found on any public path, note: "single node and three nodes", tone: ok }
  - { value: "501", label: for an unhandled space method, note: "answered locally · never proxied", tone: rust }
  - { value: "3,610 s", label: a revocation is held, note: "longer than any credential can live", tone: violet }
  - { value: audited, label: operator reads, note: "decided · not built yet", tone: muted }
```

A space controls who can read its data, but there's no encryption, so the guardrails are in how
vlpds stores and routes it. Each one below says how it holds and what tests it.

## No firehose leak

A space write is a private log entry with no frame ([How vlpds stores it](storage.md#private-log-entries)),
so nothing that reads frames can see it. The leak tests write records full of sentinel strings and
then look for them everywhere data leaves the node.

| Sentinels planted | Where the tests look |
|---|---|
| record values and field names | the live `subscribeRepos` stream |
| rkeys and the collection | a `?shard=k/n` stream |
| the space type, skey and URI | a cursor-0 replay from segments |
| the space id (`sid`) | a raw replay of the segments in the bucket |
| record CIDs | the author's `sync.*` and `repo.*` surface |
| | the peer log stream between nodes |

They also check that the author's public commit and rev don't move, and that every log entry with
`s*` keys has an empty frame. Nothing was found on one node or across three nodes (every stream,
shard, cursor replay, S3 backfill and peer log stream). The interop harness runs its own leak check in
every configuration, and it passes too.

## No proxy escape

With `--spaces` on, every `com.atproto.space.*` and `com.atproto.simplespace.*` method is answered on
this node. One that isn't built yet answers 501 `MethodNotImplemented`. It never reaches the
`atproto-proxy` fallback, where vlpds would mint service auth for it and send it to another host.

## Blobs

> [!NOTE]
> Not built yet. Space blobs land in a later slice. Until then `com.atproto.space.getBlob` and
> `listBlobs` answer 501, and `sync.getBlob` keeps today's rule.

```diagram
caption: "The rule as decided, for when space blobs land. With `--spaces` on, `sync.getBlob` serves a blob only once a public record references it. A blob that only space records reference is served by `space.getBlob`, to a credential for that same space."
nodes:
  - { id: up, label: uploadBlob, sub: "stored · no refs yet", at: [0, 4.2], size: [9, 3], tone: ink }
  - { id: pubr, label: Public record refs it, sub: "`b/`", at: [13, 0], size: [10, 2.8], tone: accent }
  - { id: spr, label: Only space records, sub: "`sb/`", at: [13, 4.3], size: [10, 2.8], tone: accent }
  - { id: none, label: Nothing refs it, at: [13, 8.6], size: [10, 2.8], tone: muted }
  - { id: s1, label: sync.getBlob serves it, at: [27, 0], size: [12, 2.8], tone: ok }
  - { id: s2, label: space.getBlob only, sub: same space's credential, at: [27, 4.3], size: [12, 2.8], tone: violet }
  - { id: s3, label: BlobNotFound, sub: "vlpds serves these today", at: [27, 8.6], size: [12, 2.8], tone: danger }
edges:
  - up.r -> pubr.l
  - up.r -> spr.l
  - up.r -> none.l
  - pubr -> s1
  - spr -> s2
  - none -> s3
```

The last row is a change. vlpds serves a blob as soon as it's uploaded today, which would leave a
window where a blob meant for a space can be fetched by CID before the space write. With `--spaces`
on, that window closes. The blob GC will count both `b/` and `sb/` refs, `sync.listBlobs` will list
only `b/`, and quotas stay bytes per account with space blobs included.

## Operator access

> [!NOTE]
> Not built yet. Today the admin console has no view of space records.

The decision is that moderators and admins can read space records, for terms-of-service work, from
the console and the admin API. Only moderator and admin auth gets in, and every read goes into the
audit log next to takedowns. Nothing is encrypted, so an operator can read space data on any PDS.
This makes it an explicit path with an audit trail.

## Takedowns

| Takedown | What happens to space data |
|---|---|
| Account | Credential reads of its space repos get `RepoTakendown`. Its space writes are refused. Its outbox rows wait and resume if the takedown is reversed. Its OAuth sessions are revoked and stay revoked after a reversal, so apps have to sign in again. |
| Record | Taken down by its space URI, through the same admin and moderation paths as a public record (`sec/td/space/{sid}/{collection}/{rkey}`). It's hidden from `getRecord`, `listRecords`, `listRepoOps` values and `getRepo`'s blocks. |
| Space | A takedown of a whole space this cluster governs. Not built yet. |

Today a taken-down space record stays in its repo's LtHash and `getRepo` index, the way a taken-down
public record stays in the signed repo. A syncer that compares hashes can then see a mismatch and
fall back to `getRepo`. The reference has no record takedown for space data at all, so record
takedowns are a vlpds extension.

```timeline
caption: "Decided, not built yet: the takedown-adjusted view. While a takedown lasts, every read signs a commit over the repo without the record, so a syncer that held it sees a mismatch at the same rev, refetches and converges. A reversal flips it back the same way."
scale: 46
lanes:
  - { id: op, label: Operator, tone: muted }
  - { id: pds, label: Author's PDS, sub: vlpds, tone: accent }
  - { id: s, label: Syncer, sub: held the record, tone: blue }
spans:
  - { lane: pds, from: 1.4, to: 5.0, label: adjusted commit, dur: "set hash − the record" }
  - { lane: s, from: 5.6, to: 7.4, label: mismatch, tone: danger }
  - { lane: pds, from: 9.6, to: 13.4, label: getRepo without it }
arrows:
  - { from: op, to: pds, at: 0.6, label: take down record }
  - { from: s, to: pds, at: 1.2, side: left, label: listRepoOps, tone: blue }
  - { from: pds, to: s, at: 5.3, label: "same rev, new hash", tone: blue }
  - { from: s, to: pds, at: 8.8, label: getRepo, tone: blue }
  - { from: pds, to: s, at: 13.6, side: left, label: verified repo, tone: blue }
marks:
  - { at: 14.2, label: converged, tone: ok }
```

The adjusted view is cheap because a repo's taken-down set is tiny. The set hash is the LtHash state
minus the taken-down records' elements, and the commit is signed at serve time like every other one.
`listRepoOps` leaves the record's ops out while the takedown lasts, so the incremental and the full
views agree.
