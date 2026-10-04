---
title: Migration
section: vlPDS
order: 10
status: ready
summary: "Moving an existing account to this server with /migrate: what the page does in simple and advanced modes, what is copied, and how identity moves safely."
---

```hero
diagram:
  caption: "`/migrate` runs in the user's browser. It copies the repo, blobs and preferences from the old server to this one, then has the old server sign a PLC operation that this server submits. Relays and the AppView follow the DID to its new home."
  nodes:
    - { id: old, label: current PDS, sub: any atproto PDS, at: [0, 0], size: [9, 3] }
    - { id: page, label: "/migrate", sub: "in the user's browser", at: [16, 0], size: [10, 3] }
    - { id: new, label: this server, sub: vlpds, at: [32, 0], size: [9, 3], tone: accent }
    - { id: net, label: relays · AppView, sub: follow the DID, at: [34, 7], size: [9, 3], tone: blue }
    - { id: plc, label: PLC directory, sub: "DID → PDS, keys, handle", at: [16, 7], size: [10, 3], tone: muted }
  edges:
    - { from: old, to: page, label: "getRepo\ngetBlob" }
    - { from: page, to: new, label: "importRepo\nuploadBlob" }
    - "new -> net: firehose"
    - { from: old.b, to: plc.l, label: signs the op, dash: true }
    - { from: new.b5, to: plc.r, label: submits it }
facts:
  - { value: "1", unit: tab, label: does the whole move, note: "nothing to install; a reload picks up where it stopped" }
  - { value: "1 GiB", label: largest repo, note: "`--max-import-mb`; imports stream in bounded batches", tone: amber }
  - { value: "1", unit: code, label: emailed by the old server, note: "it authorises the identity move; nothing else needs one", tone: blue }
  - { value: "72 h", label: to undo a bad PLC op, note: "with a recovery key listed ahead of the server's", tone: violet }
```

An account moves to vlpds the same way it moves between any two atproto servers: create the account
here with its existing DID, copy its data, point the DID at the new server, switch the old one off.
vlpds serves a page that does all of it in the browser, at `/migrate`. This page explains what that
page does, what an operator should set up first, and how to check a move.

Accounts keep their DID, followers, posts and likes. Only `did:plc` accounts can move with the page;
a `did:web` is refused.

## Before you start

| Who | Needs | Why |
|---|---|---|
| User | the account's **main password**, not an app password | creating the account here and signing the PLC operation need a full session; the page refuses an app password |
| User | an email address on the old account, and access to it | the old server emails the code that authorises the identity move |
| User | an invite code, if this server requires one | `--invite-required`; a `/migrate?invite=<code>` link fills it in |
| User | the tab open for the copy | the browser does the copying; a reload resumes |
| Operator | `vlpds admin create-invite-code` (or the console) | to hand out invites |
| Operator | `--plc-recovery-did-key` set | so migrated accounts get the operator recovery key in their rotation keys; see [Keys and security](keys-security.md#plc-rotation-key-and-recovery-keys) |
| Operator | `--crawlers` set and the server announced | so relays pick up the account once it's active; see [Relays and crawling](operations/relays-and-crawling.md) |

**Handles.** A handle under the old server's domain (`alice.bsky.social`) stops working when the
account leaves, so the page asks for a new one under this server's handle domain and checks it's free.
A handle on the user's own domain can be kept: the DID doesn't change, so its DNS or `.well-known`
record keeps pointing at it.

## Simple and advanced modes

```diagram
caption: "One state machine, two views. Simple mode (the default) groups the eight steps into four and uses plain words; advanced mode shows each step, the DIDs, keys and counts, and the option to add your own recovery key."
nodes:
  - { id: s1, label: Find your account, sub: "find · sign in", at: [0, 0], size: [10, 3], tone: accent }
  - { id: s2, label: Get ready, sub: "checks · handle · create", at: [13, 0], size: [10, 3], tone: accent }
  - { id: s3, label: Copy, sub: "repo · blobs · preferences", at: [26, 0], size: [10, 3], tone: accent }
  - { id: s4, label: Switch over, sub: "identity · finish", at: [39, 0], size: [10, 3], tone: solid }
edges:
  - s1 -> s2
  - s2 -> s3
  - s3 -> s4
notes:
  - { at: [0, 5], text: "Nothing changes anywhere before \"Create\". Nothing is final before the identity step.", align: start }
```

- **Simple mode** is written for someone who has never heard of a PDS. It avoids protocol words
  (no DID, PLC, repo, blobs or rotation keys) and shows counts people recognise: "About 30 posts and 3
  follows, plus 6 photos & videos".
- **Advanced mode** (a checkbox, remembered per browser) shows the same flow with the details: the
  DID, the old and new signing keys, rotation keys and PDS endpoints side by side, record and blob
  counts, raw error codes, and an option to generate or paste the user's own recovery key.

The **pre-flight checks** change nothing. They confirm this server is open (or needs an invite), the
account isn't already active here (an earlier unfinished copy is reused), the old account is readable,
how big it is (over 500k records or 20k blobs is flagged as slow), and which email the code will go to.

**Progress survives a reload.** What has been done (no secrets) is kept in the browser's
localStorage, so a closed tab resumes at the same step. Session tokens and a signed-but-unsubmitted
PLC operation are kept only in sessionStorage; passwords stay in memory.

## What gets copied

```diagram
caption: "The copy step, in order. Each part is marked done only when it checks out, and re-running any of them is safe."
nodes:
  - { id: acct, label: createAccount, sub: "service auth from old\nstarts deactivated", at: [0, 0], size: [10, 3.6], tone: accent }
  - { id: repo, label: repo, sub: "getRepo → importRepo\nrecord count checked", at: [13, 0], size: [10, 3.6], tone: accent }
  - { id: blobs, label: blobs, sub: "listMissingBlobs loop\n4 at a time", at: [26, 0], size: [10, 3.6], tone: accent }
  - { id: prefs, label: preferences, sub: "get → putPreferences", at: [39, 0], size: [10, 3.6], tone: accent }
edges:
  - acct -> repo
  - repo -> blobs
  - blobs -> prefs
```

1. **The account.** The old server issues a service-auth token for `createAccount` here, and the
   account is created with the existing DID, the chosen handle, email, password and invite. It starts
   **deactivated**: it serves nothing and emits nothing until the switch.
2. **The repo.** The browser downloads `com.atproto.sync.getRepo` from the old server and uploads the
   CAR to `com.atproto.repo.importRepo` here, with progress for both. vlpds verifies the CAR as it
   streams and writes it in bounded batches under a new repo generation, then swaps it in with one
   small commit, so a large import holds a few batches of memory, not the whole repo. The page then
   compares `checkAccountStatus.indexedRecords` on both sides and stops on a mismatch (advanced mode
   can accept it). Re-running replaces the copy.
3. **Blobs.** `listMissingBlobs` here lists what the repo references and this server lacks. The browser
   fetches each from the old server and uploads it, four at a time, checking that each upload hashes
   to the same CID. A 429 pauses every worker until the rate-limit window resets. It makes up to five
   passes; blobs the old server can't produce are reported, and the user can continue without them.
4. **Preferences.** `app.bsky.actor.getPreferences` there, `putPreferences` here.

Not copied: sessions, app passwords and OAuth authorisations (the user signs in again), and any
two-factor setting on the old server.

How big imports are admitted, and why they stay cheap: [Record storage](record-storage.md#imports).

## Moving the identity

```steps
- title: Ask this server what the DID should say
  body: "`getRecommendedDidCredentials`: this server's PDS endpoint, the account's new signing key, its handle, and rotation keys `[operator recovery key?, server rotation key]`. In advanced mode the user's own recovery key goes first."
- title: The old server emails a code
  body: "`requestPlcOperationSignature` on the old server. The code proves the account holder agrees to move."
- title: The old server signs the change
  body: "`signPlcOperation` with the code and the recommended values. The old server still holds a rotation key for the DID, so it is the one that can authorise handing it over. The signed operation is kept in the tab, so a failed submit can be retried without a new code."
- title: This server submits it
  body: "`submitPlcOperation` here. vlpds checks the operation lists its rotation key, names it as the PDS and carries the account's signing key, then sends it to the PLC directory and emits `#identity`."
```

This is the switch: once the directory accepts the operation, apps and relays resolve the DID to this
server, and only this server's keys (and any recovery key listed ahead of them) can change it. The old
server can no longer move it back. Rotation keys the user had added on the old server are replaced by
the new list, which is why the page offers to add one here.

**Recovery keys.** A rotation key listed ahead of the server's key can override any operation the
server key signs, for 72 hours after it. vlpds puts the operator recovery key ahead of its own, and a
user's own key ahead of both. Generating one in the page shows the private key once, never stores it,
and waits for "I saved it" before moving on. Users can also add or remove one later from their account
page. Details: [Keys and security](keys-security.md#plc-rotation-key-and-recovery-keys),
[KEK and key rotation](operations/kek-and-key-rotation.md#operator-recovery-key).

## Afterwards

```steps
- title: Activate here, deactivate there
  body: "`activateAccount` on this server (it checks the DID now points here, then serves the account and emits `#account`, `#identity` and `#sync`), then `deactivateAccount` on the old one. The old server keeps its copy of the data as of the move."
- title: Relays notice
  body: "vlpds asks its `--crawlers` to crawl after new activity, at most once per relay per 20 minutes. For a new server, announce it once by hand: `vlpds admin request-crawl bsky.network`."
- title: Check it
  body: "`checkAccountStatus` here: `activated` and `validDid` true, `indexedRecords` the record count, `importedBlobs` equal to `expectedBlobs`. `vlpds admin check-repo <did>` is clean. The relay's `getRepoStatus?did=` is active with this server's rev, the profile loads in the app, and the handle resolves."
```

Moving back, or onward, is another migration from this server: the account is a normal atproto
account here, and `/migrate` on any server, or `goat account migrate`, can take it. Posts made here
after the move are not on the old server.

If the move stops halfway, nothing is lost. Before the identity step, the account here is a
deactivated copy and the old account is untouched: reload and continue, or abandon it. After the
identity step, the page only needs to finish activating here and deactivating there; reloading resumes
it.

## Testing a migration

`just migrate-e2e` runs the whole page end to end on one machine: a local PLC directory, a reference
PDS (`ghcr.io/bluesky-social/pds:0.4`) and a mail catcher in Docker, and a local vlpds with rate limits
on. Headless Chromium drives the page for four accounts that cover both modes, email 2FA on the old
server, a dropped `createAccount` answer, a wrong PLC code, a reload in the middle of the blob copy,
a 1.5 MB-blob account, a kept custom-domain handle, and user recovery keys, generated and pasted. It
then checks on both sides that every record CID and blob byte matches, the preferences are equal, the
PLC document names this server with the right rotation keys, the account is active here and deactivated
there, and a new post is accepted. `KEEP=1` leaves the stack running; details are in
`bench/migrate/README.md`.

Before migrating real accounts to a new server, migrate one test account and check it from outside
(the last steps of [Verify](operations/deploy.md#verify)).
