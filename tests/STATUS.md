# vlpds conformance suite: status

Last full run: 2026-09-30, `CARGO_TARGET_DIR=target/agent-fix cargo test --profile dev-release --no-fail-fast --tests`.
The lead was landing log/cluster/firehose changes in `src/` during the run.

**Totals:** 264 passed, 3 failed, 2 ignored, across 36 integration test files (the 6 new `rate_limits.rs` tests included).
The 2 ignored tests are the email-2FA cases in `auth.rs`: vlpds replaces email sign-in codes with TOTP, which `totp.rs` covers.

| File | Pass | Fail | Ign | Failing tests → endpoint/behavior at fault | Kind |
|---|---:|---:|---:|---|---|
| account.rs | 15 | 0 | 0 | | |
| account_deactivation.rs | 6 | 0 | 0 | | |
| account_status.rs | 8 | 0 | 0 | | |
| app_passwords.rs | 3 | 0 | 0 | | |
| auth.rs | 13 | 0 | 2 | (ignored: email 2FA, replaced by TOTP by design) | |
| blob_deletes.rs | 6 | 0 | 0 | | |
| blobs.rs | 1 | 0 | 0 | | |
| create_post.rs | 2 | 0 | 0 | | |
| crud.rs | 30 | 0 | 0 | | |
| email_flows.rs | 6 | 0 | 0 | | |
| file_uploads.rs | 11 | 0 | 0 | | |
| firehose_backfill.rs | 2 | 0 | 0 | | |
| go_checker.rs | 2 | 0 | 0 | | |
| handle_validation.rs | 5 | 0 | 0 | | |
| handles.rs | 12 | 0 | 0 | | |
| harness.rs | 1 | 0 | 0 | | |
| interop_crypto.rs | 7 | 0 | 0 | | |
| interop_data_model.rs | 8 | 0 | 0 | | |
| interop_mst.rs | 8 | 0 | 0 | | |
| interop_syntax.rs | 15 | 0 | 0 | | |
| invertible_ops.rs | 1 | 0 | 0 | | |
| invite_codes.rs | 10 | 0 | 0 | | |
| moderation.rs | 5 | 0 | 0 | | |
| oauth.rs | 10 | 0 | 0 | | |
| preferences.rs | 6 | 0 | 0 | | |
| proxy.rs | 9 | 0 | 0 | | |
| races.rs | 4 | 0 | 0 | | |
| rate_limits.rs *(new)* | 6 | 0 | 0 | | |
| sequencer.rs | 5 | 1 | 0 | `buffers_events_that_are_not_being_read`: subscribing with `cursor=0` now yields a frame without `seq` (likely the new cursor-backfill path's `#info`/OutdatedCursor frame); fails 3/3 locally. Firehose code is the lead's (`src/firehose.rs`) | new with the firehose backfill work |
| server_basics.rs | 6 | 0 | 0 | | |
| service_auth.rs | 7 | 0 | 0 | | |
| subscribe_repos.rs | 14 | 0 | 0 | | |
| sync.rs | 8 | 2 | 0 | `get_repo_since_returns_diff`, `list_blobs`: `getRepo?since=` / `listBlobs?since=` ignore `since` (lead's scope) | scope gap |
| sync11_property.rs | 3 | 0 | 0 | | |
| sync_list.rs | 3 | 0 | 0 | | |
| totp.rs | 6 | 0 | 0 | | |

## What this pass fixed (src/)

- **Invite race:** each use of a code is a conditional-create claim object `invite-use/{hex code}/{slot}` (`If-None-Match: *`), released if the signup fails (`admin::claim_invite_use` / `release_invite_use`).
- **Writes:** `applyWrites#update` of a missing record fails (400 InvalidRequest; the reference's MST update throws, which surfaces there as a 500). An identical `putRecord` makes no commit and returns no `commit`. `$type` defaults to the collection, and any other value (including null, non-string or empty) is rejected. `listRecords` limit must be 1..=100. `applyWrites` over 200 writes is rejected.
- **Lexicon validation** (`src/lexicon.rs`, `lexicons/records.json`): every record lexicon of the atproto repo plus its refs. It covers record keys, required/nullable fields, string formats (datetime, at-uri, did, handle, …), byte and grapheme limits, enum/const, integer ranges, arrays, refs, open/closed unions and blob accept/maxSize. `validate` is honored, and `validationStatus` is `valid`, `unknown` or omitted.
- **Data model:** `cbor::Value::decode` rejects non-minimal heads, unsorted map keys and duplicate map keys. JSON→record rejects malformed `$link`/`$bytes`/blob objects and bad `$type` values, and accepts integer-valued floats (`123.0`). Legacy blob refs are refused. Records may only reference blobs that the repo uploaded and that aren't taken down (400 BlobNotFound). Request bodies with `Content-Encoding` gzip/deflate are decoded, and uploads are hashed over the decoded bytes. Upload MIME types are sniffed from magic bytes.
- **Takedowns:**
  - `uploadBlob` refuses a taken-down blob.
  - `createSession{allowTakendown}` issues a `com.atproto.takendown`-scoped token, which is accepted only by the reference's `additional: [Takendown]` methods.
  - Taking down an account revokes refresh tokens and OAuth sessions but not live access tokens, as the reference's `takedownAccount` does. The owner can still sync, and `verify_dpop` stops accepting the deleted sessions.
- **activateAccount** emits `#account`, `#identity` and `#sync` (new `AccountOp::Activate`).
- **identity:** `resolveHandle` fails for inactive accounts. `updateHandle` applies the service-domain rules, including the reserved names.
- **getServiceAuth:** `aud` must be an atproto DID (did:plc, or did:web without a path), with an optional non-empty `#fragment`.
- **XRPC hygiene:** `Json`/`Query` extractors (`src/xrpc/extract.rs`) return the `{error, message}` envelope:
  - 400 InvalidRequest for malformed input, 413 over the reference's 150 KiB JSON limit;
  - `did`/`repo`/`cid`/`handle` params are syntax-checked;
  - JSON and CAR responses over 1 KiB are gzip-compressed;
  - server-wide CORS (expose DPoP-Nonce, WWW-Authenticate, atproto-*, RateLimit-*, Retry-After).
- **putPreferences** is serialized per account.
- **Rate limits** (`src/ratelimit.rs`): the reference buckets and values, a 429 RateLimitExceeded envelope, `RateLimit-*` headers and `Retry-After`. Admin, internal and bypass-key requests are exempt. `X-Forwarded-For` is trusted only from `trusted_proxies`. `--no-rate-limits` turns them off. They are off in the test harness by default, as in the reference dev env.

## Test fixes made in this pass (tests were wrong about the reference)

- **`proxy::target_selection_and_rejections`:** a locally implemented protected method (`listAppPasswords`) with an `atproto-proxy` header is served locally, not refused. In the reference, xrpc-server mounts `this.routes` before the proxy catchall (packages/xrpc-server/src/server.ts: `this.router.use(this.routes); this.router.use(this.catchall)`), and pipethrough.ts's `PROTECTED_METHODS` "Bad token method" check runs only in that catchall. The test now asserts 200 and that the upstream saw nothing.
- **`crud::profile_gets_self_rkey`:** in the reference test the *client* fills in rkey `self`. The server validates the key against the schema (repo/prepare.ts `validateRecord` → `schema.keySchema.safeValidate`), so `createRecord` of a profile without an rkey is a 400. The test now checks both behaviours.
- **`interop_data_model::fixture_records_round_trip_through_server_with_matching_cids`**, **`blobs::upload_get_list_and_gc`:** both referenced never-uploaded blobs, which the reference refuses (blob/transactor.ts processWriteBlobs → "Could not find blob"). The fixture with a blob now expects 400 BlobNotFound. The blobs test uploads its "missing" blob and then deletes the stored object.
- **`sync11_property`, `go_checker`, `races`, `account_deactivation`:** they wrote app.bsky records with non-TID keys or non-schema bodies, which known-lexicon validation now rejects as the reference does. They now use `com.example.*` collections, `validate: false` or rkey `self`. What they test (sync semantics, races) is unchanged.
- **Harness:** `TestServer` runs with `rate_limits_enabled: false`. `rate_limits.rs` turns them on.
