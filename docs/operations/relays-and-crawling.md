---
title: Relays and crawling
section: Operations
order: 110
status: ready
summary: "Getting this server's repos onto the network: requestCrawl to relays, what relays see, backfill windows, and checking sync 1.1 conformance."
---

```hero
diagram:
  caption: "One node asks each relay to crawl the PDS's hostname. The relay then subscribes to the firehose through any node, lists repos and fetches whole ones when it needs to resync. AppViews read the relay, not the PDS."
  nodes:
    - { id: lead, label: slot-0 leader, sub: sends requestCrawl, at: [0, 0], size: [9, 2.6], tone: accent }
    - { id: nodes, label: any node, sub: behind Caddy, at: [0, 5], size: [9, 2.6], tone: accent, stack: true }
    - { id: relay, label: Relay, sub: bsky.network, at: [24, 5], size: [9, 2.6], tone: blue }
    - { id: av, label: AppView, sub: indexes the relay, at: [38, 5], size: [9, 2.6], tone: muted }
    - { id: cfg, label: "`config/crawlers.json`", sub: relays · last asks, at: [0, 10], size: [9, 2.6], shape: store, tone: amber }
  edges:
    - "lead.r -> relay.t: requestCrawl"
    - { from: relay.l, to: nodes.r, label: subscribeRepos · listRepos · getRepo, tone: blue }
    - "relay -> av: firehose"
    - { from: lead.l, to: cfg.l, via: [[-1.5, 1.3], [-1.5, 11.3]], dash: true }
facts:
  - { value: "20", unit: min, label: between crawl requests, note: "per relay, only after new activity", tone: accent }
  - { value: "1", unit: sender, label: per cluster, note: "the owner of slot 0's shard", tone: violet }
  - { value: "72 h", label: firehose backfill window, note: "a relay offline longer resyncs repos", tone: amber }
  - { value: "~12", unit: Mbit/s, label: per full subscriber, note: "at Bluesky's write rate today", tone: blue }
```

A PDS is on the network once a relay consumes its firehose. Relays find a PDS when it asks them to
(`com.atproto.sync.requestCrawl`); after that they keep a firehose connection open, page through
`listRepos` for the full set of repos, and fetch a whole repo with `getRepo` when they need to
resync one.

vlpds does the asking itself, keeps the state in the bucket so a cluster asks once, and serves the
relay-facing methods from every node. This page covers what to configure, what to watch and how
to check that what you serve is valid sync 1.1. How the stream is built is on
[Firehose](../firehose.md).

## Crawl requests

```steps
- title: Something happens
  body: The node starts or takes over shards, or the merged firehose emits a batch. That counts as activity.
- title: The sender checks each relay
  body: A relay is due when it was never asked, or when there was activity since its last ask and the interval (`--crawl-interval-secs`, 20 min) has passed. A failed ask waits for the same.
- title: It asks
  body: "`POST /xrpc/com.atproto.sync.requestCrawl {hostname}` with the host of `--public-url`, 10 s timeout. A node whose public URL is local (loopback, private, `localhost`, `.test`) records \"not sent\" instead of bothering a remote relay."
- title: The result goes to the bucket
  body: "Time, node, accepted or the HTTP status and body, and the last success, in `{prefix}/config/crawlers.json`. The next sender reads it, so a restart or a new leader inside the interval doesn't ask again."
```

**One sender.** Only the node that owns slot 0's shard sends (the same leader that runs retention
and reshard GC); the others re-check leadership every 60 s. On a single node that is the node.

**Configuration.** `--crawlers` (default `bsky.network`; comma-separated hostnames or `https://`
origins; empty for none) and `--crawl-interval-secs` (default 1,200). The console's **Relays** page
(`/admin/relays`) overrides either and stores the override in the bucket, where every node sees it.
The flag list is not copied in: until someone stores a list in the console, changing the flag
takes effect, and `vlpds.admin.setCrawlers {relays: null}` returns to it. Up to 32 relays, an
interval from 1 s to 7 days.

**Asking now.** The console's **Crawl all now** (or a relay's **Crawl now**) button, or
`vlpds admin request-crawl [RELAY,...]` (the `pdsadmin request-crawl` equivalent), asks
immediately, whatever the throttle, and prints a result per relay (exit 1 if any refused). Do it
after a first deploy and after [taking over an existing hostname](deploy.md#taking-over-an-existing-pds-hostname).

**Watch** `vlpds_request_crawl_total{relay,result}` and
`vlpds_request_crawl_last_success_time_seconds{relay}`, which exist at 0 for every configured relay.
A relay that keeps refusing usually has a host-level policy (new hosts, account limits per host)
to sort out with its operator; the stored response body says which.

## What relays consume

| Method | Served by | Notes |
|---|---|---|
| `subscribeRepos` | any node | the same merged stream and cursors on every node; cursors up to 72 h old are replayed from the bucket |
| `listRepos` | any node, paging across owners | cursor is `{slot}:{last DID}`; 500 per page (up to 1,000); every repo that exists for the whole walk is listed exactly once, across shard splits and merges |
| `getRepo` | the repo's owner (forwarded) | a CAR export; the reference's `since` is supported |
| `getRepoStatus`, `getLatestCommit` | the owner | |
| `getRecord`, `getBlocks`, `getBlob` | the owner | |

- **A node that can't reach a shard's owner** ends a `listRepos` page early with a cursor at that
  shard (503 when nothing was listed), so a relay retries rather than skipping repos.
- **Cursors are portable.** A relay reconnecting through the load balancer to a different node
  resumes from the same cursor. Seqs increase but are not consecutive; see
  [Firehose](../firehose.md#sequence-numbers-and-watermarks).
- **Outages.** A relay away for less than the retention window (`--log-retention`, 72 h) catches up
  from its cursor without missing anything. One away longer gets `#info OutdatedCursor` and the
  stream from the oldest retained event; it has to resync affected repos with `getRepo`.
- **Relay-side methods** (`requestCrawl`, `listHosts`, `getHostStatus`, `notifyOfUpdate` sent *to*
  vlpds) answer 501. vlpds is a PDS, not a relay.
- **Per-client cap.** A relay may hold up to 256 `subscribeRepos` connections per address
  (`--firehose-max-per-ip`), enough for one per `?shard=k/n` slice; more get 429.

## Sharded consumers

```diagram
caption: "A relay or indexer that wants to split the work runs one worker per slice: `subscribeRepos?shard=k/n` for its events and `listRepos` from the slice's first slot for its repos."
nodes:
  - { id: pds, label: vlpds, sub: any node, at: [0, 4], size: [8, 2.6], tone: accent }
  - { id: w0, label: worker 0, sub: "?shard=0/4 · cursor 0:", at: [14, 0], size: [10, 2.4], tone: blue }
  - { id: w1, label: worker 1, sub: "?shard=1/4 · cursor 16384:", at: [14, 2.7], size: [10, 2.4], tone: blue }
  - { id: w2, label: worker 2, sub: "?shard=2/4 · cursor 32768:", at: [14, 5.4], size: [10, 2.4], tone: blue }
  - { id: w3, label: worker 3, sub: "?shard=3/4 · cursor 49152:", at: [14, 8.1], size: [10, 2.4], tone: blue }
  - { id: idx, label: index, sub: union = full stream, at: [30, 4], size: [8, 2.6], tone: muted }
edges:
  - pds.r -> w0.l
  - pds.r -> w1.l
  - pds.r -> w2.l
  - pds.r -> w3.l
  - w0.r -> idx.l
  - w1.r -> idx.l
  - w2.r -> idx.l
  - w3.r -> idx.l
```

`subscribeRepos?shard=k/n` (a vlpds extension) carries only the events whose repo DID hashes into
slice k of n of the 65,536 slots: slots s with s·n/65,536 = k. A worker for that slice:

1. Lists its repos with `listRepos?cursor={k·65536/n}:` and stops once the page cursor's slot (the
   number before the colon) reaches `(k+1)·65536/n`.
2. Subscribes with `?shard=k/n` and its own cursor. Seqs and cursors are the full stream's, so one
   saved cursor per worker works, and so does a cursor from the full stream.

The n streams together are exactly the full stream, each in the full stream's order. Filtering costs the
server almost nothing: slots are computed once per batch and shared. The other side of the
mechanism is on [Firehose](../firehose.md#sharded-subscriptions). Unmodified relays use the full
stream, which is fine at today's rates.

## Checking conformance

```diagram
caption: Two independent checkers verify a live or replayed firehose event by event, built on other implementations' code rather than vlpds's.
nodes:
  - { id: pds, label: vlpds, sub: subscribeRepos, at: [0, 2.5], size: [8, 2.6], tone: accent }
  - { id: go, label: "`just checker`", sub: Go · indigo, at: [13, 0], size: [9, 2.6], tone: blue }
  - { id: rs, label: "`just checker-rs`", sub: Rust · shrike, at: [13, 5], size: [9, 2.6], tone: blue }
  - { id: ok, label: summary, sub: "exit 0 = conforms", at: [27, 2.5], size: [9, 2.6], tone: solid }
edges:
  - pds.r -> go.l
  - pds.r -> rs.l
  - go.r -> ok.l
  - rs.r -> ok.l
```

```bash
just checker https://pds.example.com -cursor 0 -strict        # replay everything retained, exit 1 on any failure
just checker https://pds.example.com -strict -reconnect       # tail live, across node restarts
just checker-rs https://pds.example.com -cursor 0 -strict     # the same replay through shrike
```

For every event both check: increasing seqs, canonical DAG-CBOR frames and blocks, the `#commit`
CAR (block hashes, root = commit CID, DID and rev), the signature against the account's key, the
sync 1.1 inversion of the operations back to `prevData` using only the commit's blocks, and each
DID's chain (`since` = previous rev, `prevData` = previous data root). `#sync`, `#identity` and
`#account` fields are checked too. `checker` uses indigo's `VerifyCommitMessage`; `checker-rs` also
runs shrike's stock verifier.

Flags: `-cursor N` (omit to start live), `-max-events N`, `-strict` (exit 1 on any failure),
`-quiet`, `-reconnect` (resume from the last seq after a restart; Go only). **Don't pass `-dense`**:
vlpds seqs are not consecutive, and that flag reports every gap. Exit codes are 0 for ok, 1 for
failures under `-strict` and 2 for a bad flag or no connection. Run one after a deploy or an
upgrade, and against a cursor of 0 after anything that touched the log.

## Bandwidth

```facts
- { value: "~12", unit: Mbit/s, label: one full subscriber today, note: "~4.5 KB frame per commit at ~340 commits/s", tone: blue }
- { value: "~120", unit: Mbit/s, label: at 10× Bluesky, note: "3.3k commits/s average", tone: violet }
- { value: "~25", label: full subscribers per 10 Gbit node, note: "at 20× today's writes", tone: amber }
- { value: "~0", label: for a personal PDS, note: "a few commits a day", tone: muted }
```

A full subscriber receives every commit's frame, its MST proof and record blocks included, about
4.5 KB each. At Bluesky's whole write rate that is ~12 Mbit/s per subscriber; a handful of relays and
indexers is well under 100 Mbit/s. Egress is a memory copy on the server (1,000 subscribers at
10k events/s each kept up on one node in testing), so the limit is the NIC and its price, not CPU.
Backfill from an old cursor reads the bucket at up to ~1.7 GB/s per subscriber on a fast store,
which costs GETs and, on S3, egress if the bucket is in another region.

Every full node serves the firehose, and relays that need to split the stream use `?shard=k/n`. Details: [Firehose](../firehose.md#serving-subscribers),
[Scaling and clustering](scaling-and-clustering.md#sizing-rules).
