# Migration e2e

`/migrate` moves an account from another PDS to this one, in the browser.
This harness runs the page end to end, entirely on this machine:

```
just migrate-e2e          # or bench/migrate/run.sh
```

It starts, in docker (`docker-compose.yml`, project `vlpds-migrate-e2e`):

- **PLC directory** (`did-method-plc`, built from source at a pinned commit, because the published images are amd64-only) with Postgres, on `127.0.0.1:2782`.
- **Reference PDS** (`ghcr.io/bluesky-social/pds:0.4`, dev mode, hostname `localhost`, handles `*.test`) on `localhost:2783`. It registers DIDs with the local PLC.
- **Mailpit** on `127.0.0.1:2785`. It catches the reference PDS's email, so the harness can read the 2FA and PLC codes.

It then builds the UI and `vlpds`, and runs vlpds on `127.0.0.1:2784` (rate limits on) with these flags: `--memory --dev-mode --plc-mode directory --plc-url <local PLC> --invite-required`. Then `e2e.mjs` runs three phases.

1. **Seed.** It creates four accounts on the reference PDS. They have posts (some with images), a profile with an avatar and a banner, follows and likes between the accounts, and preferences. Bob also has email 2FA turned on. The harness records every record CID, blob hash and preference.
2. **Drive.** Headless Chromium (Playwright) goes through the whole wizard for each account:
   - **alice**: found by handle plus a server address. An app password is refused first. She uses the one-click `?invite=` link and keeps her old password.
   - **bob**: starts from the landing page link and signs in with his email 2FA code. The harness drops the `createAccount` answer, so the account is created but the page sees an error; he retries. He sets a new password, types a wrong PLC code, then the right one.
   - **carol**: about 420 records and 42 blobs, some of them 1.5 MB. The page reloads in the middle of the blob copy and must resume without importing the repo again. Then a new tab resumes at the identity step, after signing in to both servers again.
   - **dave**: keeps his handle as if it were a custom domain. The old server's `describeServer` is faked so that it doesn't seem to own `.test`.
   - After the four accounts, the harness checks two refusals: an account that already moved here, and a `did:web`.
3. **Verify.** For each account, on vlpds:
   - Every record has the same CID, and every blob has identical bytes.
   - `listMissingBlobs` is empty, and the preferences are equal.
   - The local PLC names vlpds as the PDS, and its signing key, rotation keys and handle are vlpds's.
   - The account is active here and deactivated on the reference PDS.
   - Login works, `getRepo` and `getLatestCommit` answer, and a new post is accepted.

Screenshots of every screen go to `out/shots/`, and the vlpds log goes to `out/vlpds.log`. Everything is torn down at the end: containers, volumes and the vlpds process. To keep it running, set `KEEP=1`. To iterate on a running stack:

```
cd bench/migrate
node e2e.mjs            # fresh accounts each run; `node e2e.mjs verify` re-checks the last run (out/state.json)
docker compose -p vlpds-migrate-e2e down -v       # when done
```

Other options: `HEADED=1` shows the browser, and `VLPDS_BIN=...` skips the build.
