---
title: OAuth and 2FA
section: vlPDS
order: 9
status: ready
summary: "Signing in: the OAuth authorization server, DPoP, app passwords and legacy sessions, TOTP and email second factors, and how auth state stays correct under concurrency."
---

```hero
diagram:
  caption: "An OAuth sign-in. The app pushes its request, the user signs in on this server's page (with a second factor if enabled), and the app trades the code for DPoP-bound tokens. Any node serves each step: rows are routed to their owner."
  nodes:
    - { id: app, label: OAuth client, sub: an app, at: [0, 0], size: [8, 3] }
    - { id: par, label: "`/oauth/par`", sub: pushed request, at: [14, 0], size: [8, 3], tone: accent }
    - { id: page, label: sign-in page, sub: "`/oauth/authorize`", at: [28, 0], size: [9, 3], tone: accent }
    - { id: tfa, label: second factor, sub: TOTP or email code, at: [28, 6.5], size: [9, 3], tone: violet }
    - { id: token, label: "`/oauth/token`", sub: code → tokens, at: [14, 6.5], size: [8, 3], tone: accent }
    - { id: xrpc, label: XRPC, sub: access token + DPoP, at: [0, 6.5], size: [8, 3], tone: blue }
  edges:
    - "app -> par: 1 · PAR"
    - "par -> page: 2 · authorize"
    - "page.b -> tfa.t: if enabled"
    - "tfa.l -> token.r: 3 · code"
    - "token.l -> xrpc.r: 4 · tokens"
facts:
  - { value: "15 min", label: OAuth access token, note: "ES256, bound to the client's DPoP key; checked against its session on every request", tone: accent }
  - { value: "14 d", label: public-client refresh, note: "91 d idle / 730 d total for confidential clients", tone: blue }
  - { value: "5", unit: wrong codes, label: lock a second factor, note: "for 5 min, doubling up to a day; across all nodes", tone: violet }
  - { value: "~20 ms", label: Argon2id per password, note: "one permit per core (max 16); 503 after a 2 s wait", tone: amber }
```

vlpds is its own OAuth authorization server, with the same flows, tokens and error codes as the reference PDS,
plus the legacy `createSession` login and app passwords. Every auth row (OAuth sessions, codes, legacy
sessions, second-factor state) lives in the account's private state in the bucket, so any node can serve any
step and nothing is lost when a node dies. This page covers what an operator needs to know: the lifetimes,
the second factors, how revocation works across nodes, and how password hashing is kept from taking a node down.

## Sign-in methods

```diagram
caption: Three ways to get a session. All of them end in tokens this server checks itself; app passwords skip the second factor.
nodes:
  - { id: oauth, label: OAuth, sub: PAR · PKCE · DPoP, at: [0, 0], size: [9, 3], tone: accent }
  - { id: legacy, label: createSession, sub: password + factor, at: [0, 4.5], size: [9, 3], tone: accent }
  - { id: apppw, label: app password, sub: createSession, at: [0, 9], size: [9, 3], tone: muted }
  - { id: at, label: ES256 access JWT, sub: "15 min · DPoP-bound", at: [14, 0], size: [10, 3], tone: blue }
  - { id: hs, label: HMAC session JWT, sub: "2 h access · 90 d refresh", at: [14, 4.5], size: [10, 3], tone: blue }
  - { id: scoped, label: app-password JWT, sub: narrower scope, at: [14, 9], size: [10, 3], tone: blue }
edges:
  - oauth -> at
  - legacy -> hs
  - apppw -> scoped
```

| | OAuth | `createSession` (legacy) | App password |
|---|---|---|---|
| Used by | third-party apps | this server's account page, password-login clients, scripts | apps the user doesn't want to give a full login |
| Access token | ES256 JWT, 15 min, bound to the client's DPoP key | HMAC JWT under `jwt_secret`, 2 h | as legacy, scope `com.atproto.appPass` (or privileged) |
| Refresh | rotated each use; 14 d (public clients) or 91 d idle and 730 d total (confidential, `private_key_jwt`) | rotated each use; 90 d | as legacy |
| Second factor | on the sign-in page | `authFactorToken` | skipped, as in the reference |
| Revoked by | `/oauth/revoke`, the account page, any revoke-all | `deleteSession`, any revoke-all | `revokeAppPassword`, any revoke-all |

App passwords are server-generated (~80 bits) and stored as SHA-256 hashes. They can't change the account's
handle or identity: `updateHandle` and `submitPlcOperation` refuse them, which is stricter than the reference.
Other services get short-lived **service-auth JWTs** signed with the account's repo key (`getServiceAuth`); the one
inbound use is video upload, where the video service calls `uploadBlob` with the user's token.

## The OAuth flow

```steps
- title: Pushed authorization request
  body: "The client posts its request to `/oauth/par` (PKCE challenge, scopes, redirect URI; a `private_key_jwt` assertion if it is confidential) and gets a `request_uri` valid for 5 min. Client metadata is fetched from its `client_id` URL (10 s, 64 KiB, cached 10 min) through the SSRF-guarded client."
- title: Sign in on this server
  body: "The browser opens `/oauth/authorize`. A device cookie remembers accounts that signed in on that browser within 7 days, so the user can pick one without a password unless the client sends `prompt=login`. Otherwise: handle and password, then the second factor if one is on. Sign-up (`prompt=create`) goes through the same checks as `createAccount`."
- title: Consent
  body: "Public clients always show the consent screen. A confidential client's consent is remembered per account and asked again only for scopes it hasn't been granted."
- title: Code exchange
  body: "The client posts the code, its PKCE verifier and a DPoP proof to `/oauth/token`. A code lives 5 min and works once; reusing it revokes the session it created. A PKCE challenge seen in the last 24 h is refused."
- title: Use and refresh
  body: "Every XRPC call carries the access token and a fresh, single-use DPoP proof (its `iat` within 10 s of now, allowing 3 min of clock skew, and a server nonce that rotates every 60 s). A refresh rotates the refresh token and replaces the access token; the old ones stop working at once."
```

**Keys.** The OAuth signing key (P-256), DPoP nonces, CSRF tokens and refresh-token MACs are all derived from
`jwt_secret`, so every node can issue and check them with no shared state. Changing `jwt_secret` invalidates
every OAuth token and every legacy session.

**Routing.** PAR mints a request id that lands on a shard of the node that served it, and codes and refresh
tokens carry their account and session id, so `/oauth/*` calls are forwarded to the row's owner without an
index. Code exchange and refresh rotation run only on that owner; mid-handoff they answer 503 and the client
retries.

**Replay.** DPoP proofs, client assertions and request objects are claimed once, cluster-wide, at the owner of
their routing key. Authorization-server claims are also written to the bucket before they count, so a new owner
after a failover still refuses a proof its predecessor accepted. Proofs on ordinary XRPC requests are claimed in
memory only (as in the reference): right after a failover, a captured proof could be replayed once within its
short window, and only together with the access token it is bound to.

Users see and revoke their OAuth sessions on the account page and at `/oauth/account`. `vlpds.oauth.listSessions`
and `vlpds.oauth.revokeSession` are the API.

## Second factors

```diagram
caption: After the password, the strongest enabled factor is asked for. TOTP wins when both are on; the email code is never accepted in its place.
nodes:
  - { id: pw, label: password ok, at: [0, 3.5], size: [8, 3], tone: accent }
  - { id: totp, label: TOTP code, sub: or a recovery code, at: [13, 0], size: [9, 3], tone: violet }
  - { id: email, label: email code, sub: "mailed · 15 min", at: [13, 7], size: [9, 3], tone: violet }
  - { id: none, label: no factor, at: [13, 3.5], size: [9, 3], tone: muted }
  - { id: ok, label: session, at: [27, 3.5], size: [8, 3], tone: solid }
edges:
  - "pw.r25 -> totp.l: TOTP on"
  - pw.r -> none.l
  - "pw.r75 -> email.l: email factor on"
  - totp.r -> ok.l25
  - none.r -> ok.l
  - email.r -> ok.l75
```

| | TOTP | Email code |
|---|---|---|
| What it is | RFC 6238: SHA-1, 6 digits, 30 s steps, ±1 step | the reference's `emailAuthFactor`; what the Bluesky app offers |
| Turned on by | `vlpds.server.setupTotp` + `confirmTotp` (account page) | `updateEmail` with `emailAuthFactor: true` and a confirmed address |
| Stored | secret KEK-wrapped in `p/{did}`; 10 one-time recovery codes as HMACs keyed by it | a keyed digest of the mailed code; newest replaces older |
| Lost it | a recovery code works in place of a code; there is no admin reset | an admin email change (`updateAccountEmail`) drops the factor |

Both factors share one guessing bound: **5 wrong codes lock the factor for 5 min**, doubling with each further
lockout up to a day (429 `RateLimitExceeded`; no mail is sent while locked). The counter lives in the account's
private state, so it holds across nodes, restarts, and both the OAuth page and `createSession`. The OAuth page
also drops a pending sign-in after 3 wrong codes. An accepted code is spent cluster-wide, and a TOTP step can't be
reused inside its window. Because TOTP secrets are KEK-wrapped, enrolling or checking TOTP needs the key service
(503 during a KMS outage).

A locked-out user is handled in `ops/RUNBOOK.md` "A user locked out by a second factor": lockouts clear on
their own, and the per-account sign-in limit can be lifted early with a DID override in the
[admin console](operations/admin-console.md).

## Auth state under concurrency

```diagram
caption: Two nodes acting on one account at once. Every auth write goes to the account's owner and is applied only if the rows are still what the caller read.
nodes:
  - { id: na, label: node A, sub: OAuth refresh, at: [0, 0], size: [8, 3], tone: accent }
  - { id: nb, label: node B, sub: password change, at: [0, 6], size: [8, 3], tone: accent }
  - { id: owner, label: account's owner, sub: per-key lock · compare, at: [14, 0], size: [10, 9], tone: accent }
  - { id: log, label: "`log/`", sub: one conditional write, at: [30, 3], size: [9, 3], shape: store, tone: amber }
edges:
  - "na -> owner: forward"
  - "nb -> owner: forward"
  - "owner -> log: if unchanged"
```

Correctness never rests on a node-local lock or on comparing clocks:

- **Conditional writes.** OAuth rows, legacy sessions, the credential epoch and second-factor state are written
  only with a compare-and-set at the account's owner, in one log write. A shard moving between check and write
  fails the write.
- **Credential epoch.** Every revoke-all (password change or reset, takedown, deletion) replaces a per-account
  random epoch in the same write that deletes the sessions. Logins and codes carry the epoch they started with
  and are written only if it is unchanged, so a login racing a password change either landed first and was deleted,
  or fails.
- **Rotations.** A refresh rewrites its session only if the row is still what it read, so a revocation that
  deleted it meanwhile wins instead of being undone.
- **Second factors.** Attempts are recorded the same way, so N nodes don't get N times the guesses and a code is
  accepted once.

Details and the reasoning: `DESIGN.md` "Auth state under concurrency".

## Passwords and Argon2

```facts
- { value: "19 MiB", label: Argon2id memory per hash, note: "2 passes, OWASP baseline; ~20 ms of one core", tone: amber }
- { value: "≤16", unit: permits, label: hashes at once per node, note: "one per core; more only adds memory" }
- { value: "2 s", label: wait for a permit, note: "then 503 Overloaded + Retry-After 1", tone: rust }
- { value: "100", unit: /h, label: sign-in attempts per account, note: "from any IP; plus per identifier + IP limits", tone: violet }
```

Password checks are the most expensive thing a node does: a login costs ~20 ms of CPU, a commit ~0.1 ms. Hashing
runs on a fixed pool with one permit per core (at most 16). A request on a login path (`createSession`,
`createAccount`, OAuth sign-in and sign-up, `resetPassword`, `deleteAccount`, `disableTotp`) that waits 2 s
without a permit is shed with 503 `Overloaded` instead of queueing, counted in `vlpds_argon2_shed_total` and
alerted as `VlpdsPasswordHashingShed`. Admin password changes still wait their turn.

Rate limits run before any hashing, so a flood from a few addresses or one account is a 429, not a 503:

| Limit | Key | Default |
|---|---|---|
| `sign-in-account` | account, any IP (`createSession` and OAuth sign-in) | 100 per hour |
| `com.atproto.server.createSession-0` / `-1` | identifier + IP | 300 per day / 30 per 5 min |
| `oauth-sign-in-ip` | IP, OAuth sign-in form posts | 100 per 5 min |
| `oauth-ip` | IP, `/oauth/par`, `/oauth/token`, `/oauth/revoke` | 3,000 per 5 min |

An OAuth client whose backend calls `/oauth/token` for all its users from one address may need an IP override.
Overrides and live changes are in the [admin console](operations/admin-console.md).

## Revocation

```diagram
caption: "A revocation is a write at the account's owner. The owner enforces it at once; other nodes re-read the account's security rows within 10 s, and refuse to serve on a view older than 5 min."
nodes:
  - { id: ev, label: revoke, sub: password · takedown · logout, at: [0, 2], size: [9, 3], tone: danger }
  - { id: owner, label: account's owner, sub: "`sec/` rows · sessions", at: [14, 0], size: [10, 7], tone: accent }
  - { id: other, label: other nodes, sub: cached view · 10 s, at: [30, 0], size: [9, 2.6], tone: accent }
  - { id: oauth, label: OAuth requests, sub: session row checked, at: [30, 4.4], size: [9, 2.6], tone: blue }
edges:
  - "ev -> owner: one write"
  - "owner -> other: re-read"
  - "owner -> oauth: at once"
```

- **OAuth access tokens** are checked against their session row on every request, so `/oauth/revoke`, a refresh
  (which replaces the token) or a revoke-all takes effect immediately, despite the 15 min lifetime. A takedown
  is checked there too.
- **Legacy and app-password tokens** are stateless JWTs. Revoking one writes a row under `sec/rvk/` (a session
  family, or "everything issued before now"). Every authenticated request reads the account's `sec/` rows through
  a per-node view: on the owner, cached until it changes; elsewhere, re-read every 10 s. The check fails closed:
  if the owner can't be read and the cached view is more than 300 s old, the request gets 503 rather than a guess.
- **Revoke-all** happens on a password change or reset, takedown, account deletion and OAuth credential deletion.
  It deletes every legacy and OAuth session and replaces the credential epoch in one write. Its row is kept for
  the refresh-token lifetime (90 d), so a session that somehow survived still fails.
- **Cleanup.** A per-node sweep every 60 s deletes expired OAuth rows (requests, codes, sessions past their
  lifetime, devices unused for 7 d) and revocation rows once every token they cover has expired, each on condition
  it is unchanged.
