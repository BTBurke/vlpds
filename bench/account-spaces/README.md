# Your spaces on the account page, in a browser

`just account-spaces-e2e` (or `bench/account-spaces/run.sh`) runs two in-memory vlpds on
`http://127.0.0.1`, one with `--spaces` (port 2793) and one without (2794), and drives the account
page's Spaces tab with headless Chromium. On `http://127.0.0.1` the page uses a loopback OAuth client
whose scope is the served metadata's, so the flow is the one an https deploy runs, with the redirect
on an IP.

The seed is three accounts. Alice runs `bookclub` (bob writes there) and `garden`, and she writes in
bob's `notes`. Carol gets added and removed. The run checks, in order:

1. Screenshots first: phone (390 px) and desktop, light and dark, of the Connect panel, both consent
   screens, the lists, a space with a record open, the owner grant panel, the owner controls and both
   confirmations. They go to `out/shots/` (`SHOTS` overrides it) with an `index.html`.
2. The Spaces link is in the nav, Connect runs the OAuth consent (read your own space repos, nothing
   to manage) and the callback leaves no code in the address bar.
3. Spaces you write in (2, with bob's verified handle, 3 and 2 records) and spaces you run (members,
   writers, policy). Garden isn't listed as written in.
4. Bookclub lists alice's 3 records and not bob's, and one opens to its value.
5. Manage asks for the owner grant. The second consent says "manage your spaces", the token carries
   both grants (the owner one naming alice's DID), and the first grant's session and key are gone.
6. Add carol by handle as a reader, then remove her, each confirmed in the page and checked with
   `listMembers`.
7. Delete garden. The button stays disabled until `garden` is typed, and `getSpace` then answers
   `SpaceNotFound`.
8. Disconnect, leaving the tab and signing out each revoke. A token kept from before (with its key
   object) works, then gets 401 `invalid_token`. No session or key is left in the tab, and
   `vlpds.oauth.listSessions` shows no account-page session.
9. On the server without `--spaces` there's no Spaces link, and `/account/spaces` says so.

Any page error or console error (a CSP violation, say) fails the run, and `HEADED=1` shows the
browser. The Rust side is `tests/all/spaces_account_page.rs` (the metadata, both grants, the owner
grant bound to the user's spaces, revocation) and the unit tests in `src/oauth/client.rs`.
