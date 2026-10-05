# Passkeys in a browser

`just passkeys-e2e` (or `bench/passkeys/run.sh`) runs vlpds in memory on `http://localhost:2790`, which
browsers treat as a secure context, and drives it with headless Chromium. A CDP virtual authenticator
(`WebAuthn.addVirtualAuthenticator`: CTAP2, internal, resident keys, user verification) stands in for a
phone or security key.

It checks, in order:

1. The Security page: sign in with the password, add a passkey (the password first), save the recovery
   codes. The credential is discoverable, its RP ID is `localhost` and its user handle is the DID.
2. The OAuth page's second step: a pushed request from a loopback client, the password, then "Use your
   passkey". No emailed code is offered in its place.
3. The OAuth page without a password: "Sign in with a passkey", with autofill offered on the handle field.
4. The account page without a password.
5. `createSession` with the password alone answers `PasskeyRequired`.

Any CSP violation or page error in the console fails the run. Screenshots go to `out/shots/` (`SHOTS`
overrides it), and `HEADED=1` shows the browser. It isn't part of `cargo test`: the Rust suite covers the
same flows with a software authenticator (`tests/all/passkeys.rs`).
