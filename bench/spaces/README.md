# Spaces harness

This harness checks that vlpds's Spaces support works end to end and talks to the reference
implementation the same way the reference talks to itself. It also runs a randomized workload, the
same workload under faults, and a sync-cost run, so changes to the Spaces code can be measured.

```
just spaces-e2e                  # the scenario matrix (bench/spaces/run.sh e2e)
just spaces-sim 42 medium        # seeded random workload with invariants
just spaces-fault 42             # the same under faults
just spaces-cost                 # server time, notify latency and bucket ops against the targets
just spaces-cost vlpds warm-writes   # only the warm write-latency step (ref-a for the reference)
```

Everything runs on this machine, and nothing talks to the real PLC, a relay or bsky. Accounts,
passwords and keys are generated per run (or are fixed test values in this directory).

## What runs

`run.sh` starts the stack in docker (`docker-compose.yml`, project `vlpds-spaces`):

- A PLC directory on `127.0.0.1:2860` (the `did-method-plc` image bench/migrate builds).
- Two reference PDSes, `ref-a` on `localhost:2861` and `ref-b` on `localhost:2862`. They're built by
  `refpds.sh` from bluesky-social/atproto at the pin `5b95b2f2` (PR #5187), cloned blobless into
  `.scratch/atproto`. The published `pds-spaces-alpha` image has the same packages but is amd64
  only, so it doesn't run here. Set `REF_PDS_IMAGE` to use it on a host that can.
- MinIO on `127.0.0.1:2868` (the repo's `vlpds-minio:local` image). vlpds keeps its state there, so a
  `kill -9` loses nothing that was acked and the bucket's request counters are there to read.

Every service URL is `http://localhost:<port>` everywhere. Each reference PDS shares the network
namespace of a small socat container that forwards the other harness ports (2861-2889) to the other
PDS or to the host. So a DID document points at the same URL whether the driver, vlpds or a
reference PDS reads it.

`build-vlpds.sh` fetches `origin/spaces-1` (falling back to `origin/spaces-0`), checks it out
detached under `.scratch/vlpds-src` and builds `--profile dev-release` into `.scratch/target`. It
skips the build when the branch's SHA hasn't changed since the last one, and it follows force
pushes. `VLPDS_BIN=...` skips it, and `BRANCH=origin/spaces-0` pins a branch.

The drivers start vlpds themselves (`lib/vlpds.mjs`) with `--dev-mode --spaces` on port 2863, so the
fault sim can kill it. `CLUSTER=1` starts 3 nodes on MinIO behind a small round-robin balancer on
the same port, as in `tests/E2E.md`. `MEMORY=1` uses `--memory` instead of MinIO.

## How the driver acts like an app

The wire is what a real app sends. Calls go through `@atproto/api` at the Spaces alpha
(`0.0.0-spaces-alpha-20261001173819`), which also validates every response against the lexicons.
Credentials, HTTP message signatures, LtHash, commit verification and CAR verification come from
`@atproto/space` at the same version.

- On a reference PDS the driver uses a password session, as the reference's own tests do.
- vlpds only takes OAuth for spaces, so each vlpds account also authorizes a loopback client
  headlessly (`lib/oauth.mjs`: PAR, PKCE, the sign-in and consent forms, DPoP). The scope asks for
  every space action on `com.example.group` at any authority, plus `blob:*/*`, `repo:*` and the two
  `rpc:` grants the revocation and notify tests need (`APP_SCOPE` in `lib/actor.mjs`).
- A syncer (`lib/syncer.mjs`) gets credentials through a read-only member's delegation token. It
  catches up with `listRepos` from a spaceRev checkpoint and pulls each repo from its own host with
  `listRepoOps`. It keeps a running LtHash per repo and checks it against the verified commit,
  falling back to a verified `getRepo` on a mismatch.
- Forwarded notifications land on `lib/notifysvc.mjs`. It's a real did:plc whose
  `#atproto_space_syncer` entry points at it, and it checks each call's service auth against the
  authority's key. It also answers `checkUserAccess` as a managing app.

## The matrix (e2e)

Each configuration puts the roles on hosts and runs the same steps against them:

| config | authority | writer 1 | writer 2 | reader (syncer) | outsider |
|---|---|---|---|---|---|
| ref-ref | ref-a | ref-a | ref-b | ref-b | ref-a |
| vlpds-authority | vlpds | vlpds | ref-a | ref-b | ref-b |
| ref-authority | ref-a | ref-b | vlpds | vlpds | vlpds |
| vlpds-only | vlpds | vlpds | vlpds | vlpds | vlpds |

ref-ref runs first. It checks the harness, so a failure in another column points at vlpds. The
steps cover space creation and policies, members, record writes (create, put, delete, applyWrites
and its atomicity), self-reads, the credential chain (claims, single-use delegation, outsiders,
DER refused, high-S accepted, wrong audience, wrong key, a second signature label), full and
incremental sync with LtHash checks, notify registration and delivery (service auth, spaceRev and
prevSpaceRev order, listRepos paging), read-only members not being tracked, blobs (the reference
rule for `sync.getBlob`), revocation, `FutureRev` and stale notifies sent straight to the space
host, admin takedowns, app passwords, member removal, `vlpds.space.importRepo`, space deletion and
the leak check.

Two steps cover vlpds extensions and run only when the writer (and, for the second, the space) is on
vlpds:

- `takedown.record`: an admin takedown of one space record (`updateSubjectStatus` with a strongRef
  to its 7-segment URI). While it's down, credential reads don't see it, `getRepo` verifies without
  it, the commit hash is the LtHash of the remaining records, `listRepoOps` agrees, its blob answers
  `BlobNotFound`, and a syncer converges on that view, then back to the full one after reversal.
  A vlpds that hides the record but keeps it in the digest reports not impl.
- `operator.read`: `vlpds.admin.getSpaceRecord` with admin auth reads a space record, a member's
  OAuth token, an unrelated account and a wrong admin password can't, and the read shows up in
  `vlpds.admin.getAuditLog`. Not impl. while the method is missing.

Every step ends in one of these states:

- pass
- fail, with the checks that failed
- not impl., when a method answered 501, which is how the table tracks progress on vlpds
- blocked, when a step it needs didn't pass
- skip, when it doesn't apply to that placement

The leak check runs in every configuration. Every space write carries the run's sentinel (in
values, record keys and the space's skey). The harness taps each host's `subscribeRepos` from
before the first write, replays it from cursor 0 at the end, and reads `sync.getRepo` and
`sync.listBlobs` for every account. None of them may contain the sentinel, and space-only blobs must
answer `BlobNotFound` on `sync.getBlob`. `LEAK_SELFTEST=1` writes the sentinel into a public record
on purpose, which must fail the step on all three surfaces.

## The sims

`sim.mjs [seed] [scale]` creates N spaces with M writers each and K registered syncers per space,
spread over the hosts in `HOSTS` (default `ref-a,ref-b,vlpds`, or `ref-a,ref-b` without vlpds).
Then it runs random creates, puts, deletes, batches and membership changes from a seeded PRNG. The
scales are `small` (3 spaces × 4 writers × 2 syncers, 300 ops), `medium` (6 × 6 × 3, 1,500 ops),
`large` (12 × 8 × 4, 6,000 ops), or a number of ops. Writes to one repo go one at a time, as an
app's do.

After the workload it checks these invariants:

- I1: each host holds exactly the acked writes for every repo. Writes whose outcome is unknown (a
  crash or a 5xx) may land either way, and the host's answer is taken for those paths.
- I2: the authority's `listRepos` has every active writer at its head. This is notify delivery,
  eventually. With faults it waits up to 4 minutes (`CONVERGE_MS`), since dropped notifies are
  retried with backoff.
- I3: every syncer, after catching up from its checkpoint, holds the acked state of every listed
  repo, and its LtHash matches the listed hash.
- I4: no spaceRev is given to two different updates, and the prevSpaceRev chain never forks.
- I5: no syncer saw a protocol violation (cursor rules, ordering, a rev going backwards).

`FAULTS=1` (`just spaces-fault`) adds faults during the workload:

- A fault proxy in front of every syncer drops 20% of forwarded notifies (as a 503), duplicates
  20% and delays them by up to 1.5 s.
- On reference authorities, the authority's `#atproto_space_host` is pointed at a proxy that drops
  30% of inbound `notifyWrite`s. That exercises the writer's retry path, which is the outbox on
  vlpds.
- vlpds gets a `kill -9` at a third of the ops and a SIGTERM at two thirds, each restarted right
  away. With `CLUSTER=1`, node 1 is killed and stays down 20 s, so its shards move.
- Three syncers restart, two from a stale snapshot and one from nothing.

The proxies heal before the invariants run.

## Cost

`cost.mjs` (`just spaces-cost`) measures each thing against its target:

| what | how | target |
|---|---|---|
| no-op poll | 300 `listRepoOps(since=head)`, server time from `vlpds_http_request_duration_seconds` | mean well under 1 ms, 0 bucket ops |
| delta pull | write one record, then `listRepoOps(since)`, 200 times | a few ms |
| notify | write, then time to the forwarded `notifyWrite` | p50 under 100 ms locally (no linger) |
| bucket ops | `vlpds_object_store_requests_total` and MinIO's counters per write, net of idle | space write ≤ public write |
| public p99 | 300 public writes alone, then with 8 space writers running | within 25% + 2 ms |
| warm writes | after 30 warmup rounds, `WARM_N` (210) rounds of createRecord, putRecord and a one-op applyWrites in turn, one account in one space | reported, not gated |

The warm-writes step reports client p50/p90/p99 per method, and on vlpds the deltas over the run of
`vlpds_http_request_duration_seconds` per method, `vlpds_commit_stage_seconds` per stage (seal_wait,
put, apply_lock, apply, ack; these are per segment, not per write), and `vlpds_commit_durable_seconds`. Histogram quantiles are bucket upper bounds.

Against a reference PDS (`just spaces-cost ref-a`) it reports the client-side numbers only. A
second argument picks steps (a comma list); setup always runs. The report is
`out/cost-<host>[-<steps>].md`.

## Reading the results

Each run writes `out/<mode>.json` (every check) and `out/<mode>.md` (the table, failures, metrics and
notes), and prints the markdown at the end.

Both also carry the driver's client-side latency for every space call, keyed by scope (the e2e
config, the sim phase, the cost step), host (`vlpds`, `ref-a`, `ref-b`, a fault proxy as
`<host>~proxy`) and outcome (`ok`, `refused` for a 4xx, `error` for a 5xx or a dropped connection,
`ni` for 501). The md has a by-host table of ok calls pooled over configs, then the full breakdown
(`client_ms` in the json). A call the auth layer retried (a DPoP nonce, an expired token) is timed
on its final attempt only, and counted in `retried`. The e2e samples are small and include each
host's cold first calls; the cost run's warm-writes step is the latency number to quote. vlpds's log is `out/vlpds-n<i>.log`. The exit code is 0
when nothing failed, 1 on a failure, and 3 for a sim that stopped at a method that isn't implemented.

## Iterating

```
KEEP=1 just spaces-e2e ref-ref              # leave the stack up
cd bench/spaces
VLPDS_BIN=.scratch/target/dev-release/vlpds node e2e.mjs vlpds-only
HOSTS=ref-a,ref-b node sim.mjs 7 small      # all-ref sim
docker compose -p vlpds-spaces down -v      # when done
```

`.scratch/` holds the atproto clone (~50 MB), the vlpds checkout and its target dir (several GB).
Delete `.scratch/target` when you're done with vlpds builds for a while.
