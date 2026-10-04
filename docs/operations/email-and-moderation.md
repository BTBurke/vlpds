---
title: Email and moderation
section: Operations
order: 112
status: ready
summary: "Outgoing mail (SMTP, branding, disposable-address policy), the moderation service, earned invites and external handles."
---

```hero
diagram:
  caption: "What a PDS operator configures around accounts. Mail leaves over SMTP from whichever node handled the request; a moderation service (Ozone) calls a fixed set of admin methods with its own service token; sign-up and handle changes pass the reference PDS's policies."
  nodes:
    - { id: user, label: Users, sub: "sign-up · handle · email", at: [0, 0], size: [9, 3] }
    - { id: ozone, label: Ozone, sub: "--mod-service-did", at: [0, 7], size: [9, 3], tone: muted }
    - { id: pds, label: vlpds, sub: any node, at: [14, 3.5], size: [8, 3], tone: accent }
    - { id: policy, label: Policies, sub: "handles · disposable mail · invites", at: [14, 10], size: [11, 2.6], shape: note, tone: muted }
    - { id: smtp, label: SMTP mailer, sub: "account mail · queue 1,024", at: [30, 0], size: [10, 3], tone: violet }
    - { id: modmail, label: moderation mailer, sub: "admin sendEmail", at: [30, 4.6], size: [10, 3], tone: violet }
    - { id: report, label: report service, sub: "createReport", at: [30, 9.2], size: [10, 3], tone: muted }
  edges:
    - { from: user.r, to: pds.l30 }
    - { from: ozone.r, to: pds.l70, label: service JWT }
    - { from: pds.b, to: policy.t, dash: true }
    - { from: pds.r, to: smtp.l, label: mail }
    - { from: pds.r, to: modmail.l }
    - { from: pds.r, to: report.l, label: proxied }
facts:
  - { value: "SMTP", label: only, note: "smtp:// with STARTTLS or smtps://; unset, mail is only logged" }
  - { value: "3", unit: retries, label: per message, note: "after ~2 s, 10 s and 60 s; queued mail is lost if the node stops", tone: violet }
  - { value: "8,883", unit: domains, label: refused as disposable, note: "the reference's list, compiled in", tone: amber }
  - { value: "≤ 5", unit: codes, label: earned and unused per account, note: "one per --invite-interval-ms of account age", tone: blue }
```

These are the reference PDS's account-facing features, ported with its behaviour and messages. Each
flag falls back to the reference's environment variable, so a reference `pds.env` works as is.
**Every node of a cluster needs the same values.** Sign-in second factors (email codes, TOTP) are in
[OAuth and 2FA](../oauth-2fa.md#second-factors).

## Email

```diagram
caption: "The request path never waits on SMTP. A node queues the message and a background task sends up to 4 at a time over a pooled connection. Tokens live in the account's state, so any node verifies a code another node mailed."
nodes:
  - { id: req, label: request, sub: "reset · confirm · PLC op", at: [0, 2], size: [9, 3] }
  - { id: queue, label: queue, sub: "1,024 · full = dropped", at: [14, 2], size: [9, 3], tone: accent }
  - { id: send, label: sender, sub: "4 at a time · 30 s", at: [28, 2], size: [9, 3], tone: accent }
  - { id: relay, label: SMTP relay, sub: your provider, at: [42, 2], size: [8, 3], tone: muted }
  - { id: retry, label: retry, sub: "2 s · 10 s · 60 s", at: [28, 7.5], size: [9, 2.6], shape: note, tone: muted }
edges:
  - "req -> queue: enqueue"
  - queue -> send
  - "send -> relay: deliver"
  - { from: send.b, to: retry.t, label: 4xx · timeout, dash: true }
```

| Flag | Reference env | Notes |
|---|---|---|
| `--email-smtp-url` | `PDS_EMAIL_SMTP_URL` | `smtp://user:pass@host:587` (STARTTLS when offered; `?tls=required` insists) or `smtps://…:465`. Has credentials: use `--email-smtp-url-file` |
| `--email-from-address` | `PDS_EMAIL_FROM_ADDRESS` | required with the URL; `addr@host` or `Name <addr@host>` |
| `--moderation-email-smtp-url` | `PDS_MODERATION_EMAIL_SMTP_URL` | admin `sendEmail` only; unset, moderation mail goes through the main mailer |
| `--moderation-email-address` | `PDS_MODERATION_EMAIL_ADDRESS` | required with the moderation URL |
| `--email-brand-name` | `PDS_SERVICE_NAME` | default "{hostname} PDS" |
| `--email-home-url` | `PDS_HOME_URL` | footer link; default https://bsky.app |
| `--email-logo-url` | `PDS_LOGO_URL` | default the Bluesky logo |
| `--email-primary-color` | `PDS_PRIMARY_COLOR` | default `#067df7` |
| `--email-disable-confirmation-link` | `PDS_EMAIL_DISABLE_CONFIRMATION_LINK` | drops the bsky.app "click here" link |

- **What is mailed:** the reference's six account mails (password reset, account deletion, email
  confirmation, email update, PLC operation, sign-in code) with its subjects and wording, as plain
  text plus HTML, and admin `sendEmail` from a moderator.
- **A URL without its address**, or the reverse, fails startup. With neither, mail is logged
  (recipient, subject, purpose) and not sent: sign-up still works but nobody receives codes. In
  `--dev-mode` every mail is also kept in the node's dev mailbox, which the console's account page
  shows.
- **Unlike the reference**, admin `sendEmail` without a moderation mailer goes through the main
  mailer, so a single-SMTP deployment still delivers moderation mail. (The reference logs it and
  answers `sent: true`.)
- **Deliverability is your provider's job:** send from a domain with SPF, DKIM and DMARC for that
  sender. vlpds speaks SMTP only, with no HTTP mail API.
- **If mail isn't arriving:** `vlpds_mail_messages_total{result="failed"|"dropped"}` (by `purpose`;
  `admin` is moderation mail), `vlpds_mail_queue_depth`, and the `mail not sent` / `mail dropped`
  warnings, which name the recipient and purpose but never the token. 5xx rejections aren't retried.

Procedure: RUNBOOK
[Email](https://github.com/jazware/vlpds/blob/main/ops/RUNBOOK.md#email-smtp-moderation-mail-branding).

## Moderation service

```diagram
caption: "A service JWT from `--mod-service-did` (or its `#atproto_labeler`) is accepted on the moderator methods only. Everything else that changes an account stays behind the admin token."
nodes:
  - { id: ozone, label: Ozone, sub: "service JWT · aud = our DID", at: [0, 3], size: [10, 3], tone: muted }
  - { id: check, label: verify, sub: "issuer · key · lxm · exp", at: [14, 3], size: [9, 3], tone: accent }
  - { id: mod, label: moderator methods, sub: "takedown · info · sendEmail", at: [28, 0], size: [11, 3], tone: ok }
  - { id: admin, label: admin-only methods, sub: "delete · update* · invites", at: [28, 6], size: [11, 3], tone: danger }
edges:
  - ozone -> check
  - "check.r -> mod.l: allowed"
  - { from: check.r, to: admin.l, label: "401", dash: true }
```

- **The moderator methods** a service token may call: `getAccountInfo(s)`,
  `get/updateSubjectStatus` (takedowns), `sendEmail`, `getInviteCodes`, `disableInviteCodes`,
  `enable/disableAccountInvites`, and reading any account's `app.bsky.actor.getPreferences?did=`.
- **Admin token only:** `deleteAccount`, `updateAccountEmail/Handle/Password/SigningKey`,
  `createInviteCode(s)` and every `vlpds.admin.*` method.
- **Unset**, only the admin token works for any of them.
- `tools.ozone.*` calls from users go to the AppView like other proxied calls, not to the moderation
  service. User reports (`createReport`) without an `atproto-proxy` header go to `--report-service`
  (`<url>,<service did>`); without one configured they fail.

When Ozone gets **401**: `UntrustedIss` means the token's issuer isn't `--mod-service-did` on the
node that answered; `BadJwtSignature` means Ozone's DID document doesn't list the key it signs with
(vlpds re-resolves once first, so a just-rotated key works); `BadJwtAudience` means the token wasn't
addressed to this PDS's `--service-did`. Takedowns from either path take effect on every node; see
[Admin console and CLI](admin-console.md#common-tasks).

## Handle policy

```facts
- { value: "~1,000", unit: labels, label: reserved, note: "first label of a handle under --handle-domain: 400 HandleNotAvailable" }
- { value: "7", unit: patterns, label: refused as slurs, note: "any user-chosen handle, also with . - _ removed; and record keys", tone: rust }
- { value: "3 s", label: to prove an external handle, note: "DNS TXT and HTTPS well-known, tried at once", tone: blue }
```

The reference's reserved-handle list and slur filter are compiled in verbatim
(`src/handle_policy/`), so updating them is a release. They apply at createAccount, OAuth sign-up and
updateHandle, with the reference's error names. An admin (`updateAccountHandle`) skips both, but a
handle under `--handle-domain` must still be one 3–18 character label.

**External handles** (a domain outside `--handle-domain`) need proof, exactly as in the reference:
a DNS TXT record `_atproto.<handle>` = `did=<the account's DID>`, or
`https://<handle>/.well-known/atproto-did` serving the DID. Both are tried at once with a 3 s deadline
each, through the host's own resolver. "External handle did not resolve to DID" for a handle the
user swears is set up usually means the node can't reach the zone (`dig TXT _atproto.<handle>` from
the node) or there is more than one `did=` record. `--dev-mode` skips the proof.

Handles under `--handle-domain` resolve over HTTPS through Caddy's certificates for each handle; see
[Deploy](deploy.md#first-deploy).

## Invites

```steps
- title: Require invites
  body: "`--invite-required` (on in the Ansible defaults). createAccount then needs a code."
- title: Hand out codes
  body: "`vlpds admin create-invite-code [--count N] [--uses N]` or the console's Invite codes page. Admin-made codes don't count toward anyone's earned limit."
- title: Let accounts earn codes (optional)
  body: "`--invite-interval-ms` (reference `PDS_INVITE_INTERVAL`): an account earns one single-use code per interval of age, at most 5 unused, created when its app asks (`getAccountInviteCodes`). `--invite-epoch-ms` counts only age after that time."
- title: Stop or restrict
  body: "Unset `--invite-interval-ms` (rolling restart) to stop new earning; existing codes stay. Cut one account off with `disableAccountInvites`: its codes are disabled, and codes it earns later are created disabled. Setting the epoch to now restarts everyone's earning from zero."
```

**Disposable email domains** are refused at createAccount and updateEmail with "This email address
is not supported, please use a different email.", as in the reference. The list (8,883 domains,
`src/email_policy/disposable_email_domains.txt`) is compiled in; an admin `updateAccountEmail`
doesn't check it.

Procedure and troubleshooting: RUNBOOK
[Moderation service, earned invites, external handles](https://github.com/jazware/vlpds/blob/main/ops/RUNBOOK.md#moderation-service-earned-invites-external-handles).
