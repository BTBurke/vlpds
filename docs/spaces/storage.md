---
title: How vlpds stores it
section: Spaces
order: 203
status: draft
summary: "No new storage system: space rows are slot-prefixed key families in the author's and the authority's shards, written as private log entries with no firehose frame."
---

```hero
diagram:
  caption: "The author's shard holds its space repos and the authority's shard holds the space host's state. Both are ordinary slot-prefixed families, so they ride the node log and move with their shards like every other row. Space entries carry an empty frame, which the firehose skips."
  nodes:
    - { id: api, label: Apps · syncers, sub: "space.* · simplespace.*", at: [0, 5.5], size: [9, 3], tone: ink }
    - { id: fwd, label: Any node, sub: routes the call, at: [12, 5.5], size: [9, 3], tone: accent }
    - { id: as, label: Author's shard, sub: "sH · sR · sO · sP · sb · sc", at: [25, 1], size: [11, 3], tone: accent }
    - { id: hs, label: Authority's shard, sub: "sS · sM · sW · sQ · sN", at: [25, 10], size: [11, 3], tone: accent }
    - { id: fh, label: Firehose, sub: "merger · peers · backfill", at: [40, 0], size: [11, 2.6], tone: blue }
    - { id: log, label: "`log/` segments", sub: "private entries", at: [40, 5.2], size: [11, 3.2], shape: store, tone: amber }
  edges:
    - "api -> fwd"
    - { from: fwd.r30, to: as.l, label: by repo }
    - { from: fwd.r70, to: hs.l, label: by authority }
    - { from: as.r, to: log.l30, label: one entry }
    - { from: hs.r, to: log.l70, label: one entry }
    - { from: log.t, to: fh.b, label: empty frame · skipped, dash: true, tone: blue }
facts:
  - { value: "11", unit: families, label: of space rows, note: "six per author · five per authority" }
  - { value: "0", unit: frames, label: per space entry, note: "a debug assertion refuses a space row with a frame", tone: blue }
  - { value: "2,048 B", label: LtHash state, note: "in every write's `sH` row · random, so it doesn't compress", tone: amber }
  - { value: "1", unit: control object, label: cluster-wide, note: "the revocations · written only when something is revoked", tone: violet }
```

Spaces adds no storage system. A space write goes through the author's repo worker and becomes one
log entry, the same way a commit does, except the entry has no firehose frame. The rows live in
the shard that owns the account's slot, so split, merge, takeover and replay handle them like any
other rows.

## Key families

`{sid}` is the first 16 bytes of sha256(space URI). URIs can run over 600 B, which is too long to
repeat in every key. The full URI is kept in `sH`, `sS` and `sP`, and every reader checks it, so a
hash collision fails loudly with `space id collision` instead of mixing two spaces.

| Key | In the slot of | Holds |
|---|---|---|
| `sH/{did}\0{sid}` | the author | the space repo head: URI, rev, LtHash state, record count, created. `listSpaces` scans it |
| `sR/{did}\0{sid}{coll}/{rkey}` | the author | CID, rev and record bytes, as `R/` holds them. The source of truth |
| `sO/{did}\0{sid}{rev}{idx}` | the author | the oplog: action, collection, rkey, CID and prev CID. Kept 7 days |
| `sP/{did}\0{sid}` | the author | the notifyWrite outbox: URI, repoRev and hash, one row per (repo, space) |
| `sS/{auth}\0{sid}` | the authority | the space as JSON: URI, policies, created, and `deleted` for a tombstone |
| `sM/{auth}\0{sid}{member}` | the authority | a member's read and write access |
| `sW/{auth}\0{sid}{writer}` | the authority | writer state: repoRev, hash, spaceRev |
| `sQ/{auth}\0{sid}{spaceRev}` | the authority | the writer's DID, in `listRepos` order. It holds each writer's latest state only |
| `sN/{auth}\0{sid}{service}` | the authority | a notify registration: endpoint and expiry (24 h) |
| `sb/{did}\0{sid}{cid}\0{path}` | the author | a space record's blob ref, at the rev that wrote it. `space.listBlobs` scans it in CID order |
| `sc/{did}\0{cid}\0{sid}{path}` | the author | the same ref, CID first, so the blob GC and `sync.getBlob` find a blob's space refs in one scan |

A deleted space keeps its `sS` tombstone so `getSpaceCredential` can answer `SpaceDeleted`. Its
other host rows are swept, and a space created again at the same URI starts fresh. `deleteSpace`
and `deleteAccount` drop the `sb/` and `sc/` refs with the records.

## Private log entries

```diagram
caption: "A space entry reaches the sequencer with `frames: []`. The sequencer gives it one empty frame so it still gets a seq and its rows ride the segment. Every reader of frames skips empty ones."
nodes:
  - { id: w, label: Repo worker, sub: "s* rows · frames: []", at: [0, 3.5], size: [9, 3], tone: accent }
  - { id: sq, label: Sequencer, sub: adds an empty frame, at: [12.5, 3.5], size: [9, 3], tone: accent }
  - { id: seg, label: Segment, sub: "rows + empty frame", at: [25, 3.5], size: [9, 3], shape: store, tone: amber }
  - { id: mem, label: Shard memtable, sub: rows applied, at: [38, 0], size: [10, 2.6], tone: accent }
  - { id: m, label: Merger, sub: live firehose, at: [38, 3.7], size: [10, 2.6], tone: blue }
  - { id: bf, label: Backfill, sub: "`segment::events`", at: [38, 7.4], size: [10, 2.6], tone: blue }
edges:
  - "w -> sq"
  - "sq -> seg: PUT"
  - { from: seg.r, to: mem.l, label: apply }
  - { from: seg.r, to: m.l, label: skipped, dash: true, tone: blue }
  - { from: seg.r, to: bf.l, dash: true, tone: blue }
```

A debug assertion in the sequencer refuses any entry that carries `s*` rows and a frame, and the leak
tests check every way out ([Privacy guardrails](privacy.md#no-firehose-leak)). So a space write never
changes the author's public repo, its rev or its commit.

Each write's `sH` row carries the whole 2,048 B LtHash state. It's random, so it doesn't compress.
That's fine for the alpha. Logging only the element deltas and deriving the state at apply would cut
it, and isn't built.

## Moving a repo in

`vlpds.space.importRepo` takes the 2-root CAR that `space.getRepo` serves on the old host, for an
account moving in (it may still be deactivated). Before anything is written, the commit's signature
and MAC must verify against the DID's current `#atproto` key, and the set hash recomputed from the
index must be the commit's. The CAR must be laid out as the reference's `verifyRepoCarFull` reads it:
the commit, the index, then one block per index entry in the index's order, and nothing else. The
records then stream in, each block checked against its CID, in frameless entries of at most 1,000
rows or 4 MiB. One last entry on the repo's worker puts the head
in at the CAR's rev with an empty oplog, and the authority is owed a notify like after any write.
The account's writes to that space are refused while it imports.

- A rev more than 5 min in the future gets `FutureRev`.
- When the space's authority is hosted on the same node, the account must be allowed to write in the
  space (`NotAuthorized` otherwise). An authority elsewhere refuses a non-writer's notify as it
  would any write's.
- An import that fails clears what it staged. If the node dies part way, the rows it left have no
  head over them, so nothing serves them. The next import or the repo's first write clears them
  before it lands.
- An import over a repo with records is refused. One over a repo that's been emptied works if the
  CAR's rev is newer, and the old head goes in the same entry.

It takes an OAuth session that may create records in the space, or the account's own password
session while the account is still deactivated (an account moving in can't sign in with OAuth
until it's active). App passwords can't import.

## Revocations

The one piece of cluster-wide Spaces state is `{prefix}/spaces/revocations.json`, since a credential
can read any repo the cluster hosts.

```json
{"revoked": [{"space": "at://did:plc:…/space/com.example.group/3kfa", "jti": "…", "until": 1791234567}]}
```

- It's appended with a CAS on its ETag, and pruned of entries past `until` whenever it's rewritten.
- It's written only when something is revoked, so it costs nothing when idle.
- It's capped at 2,000 live entries per authority and 50,000 in all (see
  [Revocation](reading.md#revocation)), so a read stays under ~7 MB.
- Every node loads it before serving a credential read, then re-reads it every 5 min with a
  conditional GET, and at once when nudged.

## Split, merge and takeover

| Event | `s*` rows | Space heads cache | Outbox |
|---|---|---|---|
| Split or merge | move with their slots like any family | dropped with the old shard | each new shard's `sP` rows are rescanned when it opens |
| Planned handoff | the same | dropped, reloaded by the new owner | rescanned on open |
| Crash and takeover | the next owner replays the log, private entries included | dropped | rescanned on open, newest rev sent |

Fan-out queues live only in memory on the authority's node. A forward still queued when the shard
moves can be lost, so the new owner sends each live registration one catch-up forward when the shard
opens ([Fan-out](writing-and-sync.md#fan-out)).

The phase 2 cluster tests cover each row. Split and merge keep every `s*` row under writes, and a
kill -9 mid-burst on three nodes loses no acked write and keeps spaceRevs moving forward. The heads
cache remembers the shard and epoch each entry was read under, so a node never serves a head for a
shard it no longer owns.

## Retention, caps and deletion

- The oplog keeps 7 days (`--space-oplog-retention`, `off` keeps everything). Each node sweeps the
  shards it owns about every 6 h, one range scan per space repo, and deletes old ops in frameless
  entries of a bounded size. It never prunes on a write.
- A space repo holds at most 100k records (`--space-repo-max-records`). The 2-root CAR that
  `getRepo` sends has one index block, about 6 MB at 100k records.
- `deleteAccount` sweeps every `s*` family for the DID, in frameless entries.
