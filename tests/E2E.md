# vlpds end-to-end exercise (2026-10-01)

Every feature was driven over real HTTP against a 3-node cluster:
- **Nodes:** ports 2800–2802, `--shards 64 --lease-ttl-ms 10000 --dev-mode --no-rate-limits`.
- **Storage:** native MinIO, a fresh prefix.
- **Public URL:** one shared URL, as behind a load balancer, so OAuth issuer and DPoP `htu` are the same on every node.
- **AppView:** a local stub AppView.
- **Driver:** a Python driver (scratchpad, not kept). Consecutive steps for the same account go to different nodes, so most calls are forwarded.

Background load ran for the whole session: `loadgen run` at 20 creates/s, rotating across nodes.

Two Go checkers ran with `-strict -reconnect`:
- one on n0 for the whole session;
- one started late, from cursor 0, on n2.

**Faults:**
- `kill -9`: n1 twice and n2 once, each with the full driver suite running through the survivors during the outage.
- SIGTERM and restart: every node at least twice, including three rolling deploys of the fixes below under a full-suite run.

## Result

| Area | Result | Notes |
|---|---|---|
| Accounts: createAccount (email required, handle unique cluster-wide), sessions, refresh/delete | pass | |
| App passwords (normal + privileged, scopes, chat lxm, revoke) | pass | A revoked app password's live access token gets 400 ExpiredToken. That is stricter than the reference. |
| TOTP enroll/confirm/login/recovery/disable, 5-strike lockout | pass | The lockout holds cluster-wide, with wrong codes spread over 3 nodes. |
| Email confirm / update, password reset (dev mail sink) | pass after fix 3 | The dev mailbox is per node. Mail lands on the node that served the request, which is the owner after routing. |
| Deactivate/activate, requestAccountDelete/deleteAccount, admin deleteAccount | pass | Checked on all 3 nodes. |
| Handle change, reserveSigningKey (reused per DID across nodes), getServiceAuth | pass | |
| Invite codes | pass | Covered with invites optional, and on a separate `--invite-required` node. The use is not recorded when invites are optional (see notes). |
| Repo CRUD, swapCommit/swapRecord, applyWrites atomicity, validate true/false/unknown | pass | |
| 900 KB record, over 1 MB rejected, listRecords paging both directions | pass | |
| importRepo | pass | Exported, changed, then re-imported: every node shows the snapshot again. A CAR of another DID is refused (see notes). |
| Blobs: upload (incl. 12 MB), reference, getBlob, listBlobs, mime mismatch, GC after dereference | pass | GC ran with `--blob-gc-grace-secs 20`. Dereferenced and orphan blobs disappeared on every node, and referenced ones survived. |
| Sync: getRepo (full, since), getLatestCommit, getRecord proofs, getBlocks, getRepoStatus | pass | Byte-identical CAR blocks from every node. |
| listRepos / listReposByCollection | pass after fix 1 | They used to 500 on every node of a cluster. |
| subscribeRepos: cursor 0 (S3 backfill on restarted nodes), mid cursor, FutureCursor, live | pass | |
| Identity: resolveHandle/Did/Identity, refreshIdentity, describeServer, well-knowns | pass after fixes 4, 6, 7 | |
| Admin: searchAccounts/getInviteCodes/getAccountInfos across nodes | pass after fix 5 | |
| Admin: takedown of account/record/blob | pass | Enforced on all nodes. A `#account` takendown event is emitted, getSubjectStatus reflects it, and reversal works. |
| Admin account updates (password/email/handle), sendEmail | pass after fix 5 | |
| OAuth: PAR, authorize, sign-in + TOTP, consent, token, DPoP, refresh, revoke | pass after fix 2 | Each step ran on a different node. A wrong DPoP key and a DPoP token sent as Bearer are refused. Code replay and refresh-token reuse revoke the grant (RFC 6749 §4.1.2), so clients must not retry them. |
| Proxy: atproto-proxy to the stub | pass | iss/aud/lxm/exp and ES256K are correct. Also covered: default AppView, did:web target, procedure bodies, the getRecord fallback for an unhosted repo, and app-password chat restrictions. |
| Rate limits (separate node without `--no-rate-limits`) | pass after fix 8 | createSession allows 30 per 5 min per identifier. The 31st gets 429 with RateLimit-* and Retry-After headers. |
| Web UI | pass | `/`, `/admin*`, `/account*`, assets and fonts load on every node, with CSP. Also checked: `/metrics`, the console's cluster status (tables agree), and listSessions/revokeSession. |
| Failover | pass | kill -9: writes to the dead node's shards 503 for about 17 s (TTL + skew + step), then succeed. SIGTERM: 0 to 4 s of 503s across the release and the re-acquire. Retries always succeeded. |

**Final checks:**
- `loadgen verify`: 47,364 acked creates, 0 missing, on each of the 3 nodes.
- Driver-tracked records: all 2,364 have the same CID from every node.
- Firehose: every node replays an identical 50,805 events from cursor 0, with the same sequence digest. That is 50,143 `#commit`, 202 `#sync`, 221 `#identity` and 239 `#account`.
- Both checkers: `RESULT: PASS`, 0 failures, 50,805 events each. Checker 1 reconnected 4 times and checker 2 3 times.
  - Signatures of deleted accounts are counted as skipped, not checked: 11 on checker 1 and 14 on checker 2. Their keys are gone.

**Earlier checker runs (before the checker fixes in bug 9):**
- One run reported `key_fetch=4`: deactivated, taken-down and deleted accounts whose keys it fetched late.
- One reported `key_fetch` failures from 503s while it was started against a restarting node.

These were limitations of the oracle, not PDS bugs.

## Bugs

Regression tests are in `tests/all/e2e_regressions.rs` unless noted.

1. **HIGH: listRepos / listReposByCollection returned 500 "partition not owned" on every node of a multi-node cluster.** No relay could enumerate the repos.
   - Fix: both now scatter-gather over `/internal/v1/sync/listRepos{,ByCollection}`, merged in (shard, did) order (`src/xrpc/sync.rs`).
   - A listRepos page stops before a shard that no answering node owns, so a relay never skips its repos. If that shard is the very next one, the call returns 503.
   - listReposByCollection returns 503 while any shard is uncovered.
   - Test: `list_repos_spans_the_cluster`.
2. **HIGH: an OAuth consent posted to a node other than the one that ran PAR failed with 503 "owner unreachable".**
   - Cause: the internal forwarding client followed the owner's 303 to the client's `redirect_uri`.
   - Fix: the internal client never follows redirects (`src/server.rs`).
   - Test: `ha_auth::oauth_flow_across_nodes_single_use_cluster_wide` now consents on a node other than the PAR node.
3. **MED: password reset through a non-owner node failed.**
   - resetPassword looped on 503 "repo load failed".
   - requestPasswordReset returned 400 "account does not have an email address".
   - Fix: `forward.rs` routes requestPasswordReset by its `email` and resetPassword by the account its token was issued for (`xrpc::reset_token_did`).
   - Test: `reset_password_through_any_node`.
4. **MED: `resolveIdentity?identifier=<handle>` returned 503 indefinitely on non-owner nodes.** `identifier` wasn't a query routing key.
   - Fix: it is one now.
   - Test: `resolve_identity_routes_by_identifier`.
5. **MED: admin calls failed on non-owner nodes.**
   - updateAccountEmail `{account}` and sendEmail `{recipientDid}` weren't routed. They answered "Account does not exist" and "Recipient not found".
   - getAccountInfos read only local shards. It silently dropped accounts and returned 503 on a remote account's invites.
   - Fix: `account`/`recipientDid` are admin body routing keys. getAccountInfos uses `account_anywhere`, and `account_invites` uses `scan_private_anywhere`.
   - Tests: `admin_account_calls_reach_the_owner`, plus `forward::tests::body_parsing`.
6. **MED: during a shard move, resolveHandle answered 400 HandleNotFound and createSession 401 "Invalid identifier or password".** Unavailability was swallowed as "no such account" (`.ok()`), so clients were told the handle or password was wrong.
   - Fix: `account_if_exists` (only AccountNotFound counts as absent) in `login_account`, resolveHandle and requestPasswordReset. These now answer 503 and retry works.
   - Test: `unowned_shard_is_unavailable_not_missing`.
7. **LOW-MED: `/.well-known/atproto-did` (HTTPS handle verification by Host) was not served.** Subdomain handles could only be verified over DNS, which vlpds doesn't publish.
   - Fix: added it, answering on every node; 404 for inactive or unknown handles, 503 while the owner is unreachable.
   - Test: `well_known_atproto_did_by_host`.
8. **LOW-MED (open TODO item): every case variant of a handle got its own 30 createSession attempts per 5 min.**
   - Fix: the key is normalized like the OAuth sign-in key, whose buckets it shares.
   - Test: `rate_limits::create_session_identifier_variants_share_a_bucket`.
   - Still separate buckets: the DID, handle and email forms of one account. That is reference behavior, and the per-IP global limit still applies.
9. **Checker (`checker/`), the test oracle:**
   - Its describeRepo key fetch failed for deactivated or taken-down accounts and on transient 503s. It now falls back to resolveDid and retries 503s for up to 30 s.
   - A deleted account's key is unavailable, which a late replay always hits. Such signatures are now counted as "sigs skipped", not failures.
   - New `-reconnect` flag: resume from the last seq after the PDS restarts.

## Not fixed / notes

- **Invite uses are not recorded when invites are optional.** When an optional `inviteCode` is passed while invites aren't required, vlpds ignores it; the reference records the use. LOW.
- **importRepo refuses a CAR whose commit is for another DID.** The reference doesn't check the DID. This is deliberate in vlpds: there is no migration-in yet (TODO "createAccount with existing did").
- **Dev mailbox is per node.** `vlpds.admin.getDevMail` only sees mail sent by the node it asks. This is dev-only; tooling has to ask every node.
- **Proxied request bodies are sent chunked, without content-length.** Upstreams must accept chunked bodies; the stub AppView had to.
