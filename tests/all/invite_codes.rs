//! Port of atproto/packages/pds/tests/invite-codes.test.ts (the parts that do
//! not poke the reference PDS's SQL tables directly), on a server started with
//! `invite_required = true`.
use crate::common::*;

async fn server() -> TestServer {
    TestServer::spawn_with(|c| c.invite_required = true).await
}

async fn create_invite(s: &TestServer, uses: u32, for_account: Option<&str>) -> String {
    let mut body = json!({"useCount": uses});
    if let Some(d) = for_account {
        body["forAccount"] = json!(d);
    }
    s.xrpc
        .post("com.atproto.server.createInviteCode", &body, &Auth::Admin)
        .await
        .ok()["code"]
        .as_str()
        .expect("code")
        .to_string()
}

async fn signup(s: &TestServer, code: &str) -> Resp {
    let name = unique_name("inv");
    s.xrpc
        .post(
            "com.atproto.server.createAccount",
            &json!({"email": format!("{name}@example.com"), "handle": format!("{name}.{HANDLE_DOMAIN}"), "password": PASSWORD, "inviteCode": code}),
            &Auth::None,
        )
        .await
}

async fn signup_ok(s: &TestServer, code: &str) -> TestAccount {
    let r = signup(s, code).await;
    let j = r.ok();
    TestAccount {
        did: j["did"].as_str().unwrap().into(),
        handle: j["handle"].as_str().unwrap().into(),
        password: PASSWORD.into(),
        email: String::new(),
        access: j["accessJwt"].as_str().unwrap().into(),
        refresh: j["refreshJwt"].as_str().unwrap_or_default().into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn describes_that_invites_are_required() {
    let s = server().await;
    let j = s.xrpc.get("com.atproto.server.describeServer", &[], &Auth::None).await.ok();
    assert_eq!(j["inviteCodeRequired"], json!(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn valid_bad_and_missing_codes() {
    let s = server().await;
    let code = create_invite(&s, 1, None).await;
    signup_ok(&s, &code).await;
    signup(&s, "fake-invite").await.err(400, "InvalidInviteCode");
    // no code at all
    let name = unique_name("inv");
    let r = s
        .xrpc
        .post(
            "com.atproto.server.createAccount",
            &json!({"email": format!("{name}@example.com"), "handle": format!("{name}.{HANDLE_DOMAIN}"), "password": PASSWORD}),
            &Auth::None,
        )
        .await;
    r.client_err();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fails_on_invite_code_from_takendown_account() {
    let s = server().await;
    let acct = signup_ok(&s, &create_invite(&s, 1, None).await).await;
    let code = create_invite(&s, 1, Some(&acct.did)).await;
    let subject = json!({"$type": "com.atproto.admin.defs#repoRef", "did": acct.did});
    s.xrpc
        .post("com.atproto.admin.updateSubjectStatus", &json!({"subject": subject, "takedown": {"applied": true}}), &Auth::Admin)
        .await
        .ok();
    signup(&s, &code).await.err(400, "InvalidInviteCode");
    s.xrpc
        .post("com.atproto.admin.updateSubjectStatus", &json!({"subject": subject, "takedown": {"applied": false}}), &Auth::Admin)
        .await
        .ok();
    signup_ok(&s, &code).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fails_on_used_up_invite_code() {
    let s = server().await;
    let code = create_invite(&s, 2, None).await;
    signup_ok(&s, &code).await;
    signup_ok(&s, &code).await;
    signup(&s, &code).await.err(400, "InvalidInviteCode");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn handles_racing_invite_code_uses() {
    let s = std::sync::Arc::new(server().await);
    let code = create_invite(&s, 1, None).await;
    let hs: Vec<_> = (0..10)
        .map(|_| {
            let s = s.clone();
            let code = code.clone();
            tokio::spawn(async move { signup(&s, &code).await.is_ok() })
        })
        .collect();
    let mut ok = 0;
    for h in hs {
        ok += h.await.unwrap() as usize;
    }
    assert_eq!(ok, 1, "exactly one racing signup should consume a single-use invite");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_gifted_codes_are_listed_for_the_account() {
    let s = server().await;
    let acct = signup_ok(&s, &create_invite(&s, 1, None).await).await;
    for _ in 0..3 {
        create_invite(&s, 1, Some(&acct.did)).await;
    }
    let j = s.xrpc.get("com.atproto.server.getAccountInviteCodes", &[], &acct.auth()).await.ok();
    let codes = j["codes"].as_array().unwrap();
    let from_admin = codes.iter().filter(|c| c["createdBy"] == json!("admin")).count();
    assert_eq!(from_admin, 3, "codes: {codes:?}");
    for c in codes {
        assert_eq!(c["forAccount"], json!(acct.did));
        assert!(c["available"].is_number() && c["uses"].is_array(), "invite code view shape: {c}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prevents_use_of_disabled_codes() {
    let s = server().await;
    let first = create_invite(&s, 1, None).await;
    let acct = signup_ok(&s, &create_invite(&s, 1, None).await).await;
    let second = create_invite(&s, 1, Some(&acct.did)).await;
    s.xrpc
        .post("com.atproto.admin.disableInviteCodes", &json!({"codes": [first], "accounts": [acct.did]}), &Auth::Admin)
        .await
        .ok();
    signup(&s, &first).await.err(400, "InvalidInviteCode");
    signup(&s, &second).await.err(400, "InvalidInviteCode");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn does_not_allow_disabling_all_admin_codes() {
    let s = server().await;
    let r = s
        .xrpc
        .post("com.atproto.admin.disableInviteCodes", &json!({"accounts": ["admin"]}), &Auth::Admin)
        .await;
    r.err_status(400);
    assert!(r.text().contains("cannot disable admin invite codes"), "{}", r.text());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creates_many_invite_codes() {
    let s = server().await;
    let accounts = ["did:example:one", "did:example:two", "did:example:three"];
    let j = s
        .xrpc
        .post("com.atproto.server.createInviteCodes", &json!({"useCount": 2, "codeCount": 2, "forAccounts": accounts}), &Auth::Admin)
        .await
        .ok();
    let got = j["codes"].as_array().unwrap();
    assert_eq!(got.len(), 3);
    let mut all = Vec::new();
    for c in got {
        assert!(accounts.contains(&c["account"].as_str().unwrap()));
        let codes = c["codes"].as_array().unwrap();
        assert_eq!(codes.len(), 2);
        all.extend(codes.iter().map(|x| x.as_str().unwrap().to_string()));
    }
    // each code is usable twice
    for code in all.iter().take(1) {
        signup_ok(&s, code).await;
        signup_ok(&s, code).await;
        signup(&s, code).await.err(400, "InvalidInviteCode");
    }
    // admin listing knows about them
    let r = s.xrpc.get("com.atproto.admin.getInviteCodes", &[("limit", "500")], &Auth::Admin).await.ok();
    let listed: Vec<&str> = r["codes"].as_array().unwrap().iter().map(|c| c["code"].as_str().unwrap()).collect();
    for c in &all {
        assert!(listed.contains(&c.as_str()), "{c} missing from admin.getInviteCodes");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invite_admin_endpoints_require_admin() {
    let s = server().await;
    let acct = signup_ok(&s, &create_invite(&s, 1, None).await).await;
    s.xrpc.post("com.atproto.server.createInviteCode", &json!({"useCount": 1}), &acct.auth()).await.client_err();
    s.xrpc.post("com.atproto.server.createInviteCode", &json!({"useCount": 1}), &Auth::None).await.err_status(401);
    s.xrpc.get("com.atproto.admin.getInviteCodes", &[], &acct.auth()).await.client_err();
    s.xrpc.post("com.atproto.admin.disableInviteCodes", &json!({"codes": ["x"]}), &acct.auth()).await.client_err();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_invite_codes_validates_limit() {
    let s = server().await;
    for bad in ["0", "501"] {
        s.xrpc.get("com.atproto.admin.getInviteCodes", &[("limit", bad)], &Auth::Admin).await.err(400, "InvalidRequest");
    }
    s.xrpc.get("com.atproto.admin.getInviteCodes", &[("limit", "500")], &Auth::Admin).await.ok();
}

// ---------------------------------------------------------------------------
// periodic invite codes (`invite_interval`; reference inviteInterval /
// inviteEpoch). The reference cases backdate `actor.createdAt` in SQL; here
// the interval is short and the test waits it out. The epoch arithmetic
// with backdated accounts is unit-tested next to `codes_to_create`
// (src/xrpc/server.rs).
// ---------------------------------------------------------------------------

async fn account_codes(s: &TestServer, a: &TestAccount, q: &[(&str, &str)]) -> Vec<J> {
    s.xrpc.get("com.atproto.server.getAccountInviteCodes", q, &a.auth()).await.ok()["codes"]
        .as_array()
        .unwrap()
        .clone()
}

/// "allow users to get available user invites" and "admin gifted codes to
/// not impact a users available codes" (interval 2 s for 1 day).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_earns_invite_codes_on_an_interval() {
    let s = TestServer::spawn_with(|c| {
        c.invite_required = true;
        c.invite_interval = Some(std::time::Duration::from_secs(2));
    })
    .await;
    let acct = signup_ok(&s, &create_invite(&s, 1, None).await).await;
    // no codes yet
    assert_eq!(account_codes(&s, &acct, &[]).await.len(), 0);
    for _ in 0..3 {
        create_invite(&s, 1, Some(&acct.did)).await;
    }
    // two intervals old
    tokio::time::sleep(std::time::Duration::from_millis(4500)).await;
    // createAvailable=false only lists
    assert_eq!(account_codes(&s, &acct, &[("createAvailable", "false")]).await.len(), 3);
    let codes = account_codes(&s, &acct, &[]).await;
    assert_eq!(codes.len(), 5, "{codes:?}");
    let mine: Vec<&J> = codes.iter().filter(|c| c["createdBy"] == json!(acct.did)).collect();
    assert_eq!(mine.len(), 2, "{codes:?}");
    assert_eq!(codes.iter().filter(|c| c["createdBy"] == json!("admin")).count(), 3);
    for c in &mine {
        assert_eq!(c["available"], json!(1));
        assert_eq!(c["forAccount"], json!(acct.did));
        assert_eq!(c["disabled"], json!(false));
    }
    // use both earned codes: no more are earned, they are listed as used
    for c in &mine {
        signup_ok(&s, c["code"].as_str().unwrap()).await;
    }
    let after = account_codes(&s, &acct, &[]).await;
    assert_eq!(after.iter().filter(|c| c["createdBy"] == json!(acct.did)).count(), 2, "{after:?}");
    let unused = account_codes(&s, &acct, &[("includeUsed", "false")]).await;
    assert_eq!(unused.len(), 3, "the admin codes: {unused:?}");
}

/// Without invites required, an interval earns nothing (reference: codes
/// are created only when `invites.required`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invite_interval_needs_invites_required() {
    let s = TestServer::spawn_with(|c| c.invite_interval = Some(std::time::Duration::from_millis(1))).await;
    let acct = s.create_account("noinv").await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(account_codes(&s, &acct, &[]).await.len(), 0);
}

/// invites-admin.test.ts "creates codes in the background but disables
/// them" (interval 1 ms): an account whose invites are disabled still gets
/// its 5 codes, created disabled, so getAccountInviteCodes lists none; once
/// re-enabled ("re-enables an accounts invites") it has codes again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_creates_disabled_codes_for_a_disabled_account() {
    let s = TestServer::spawn_with(|c| {
        c.invite_required = true;
        c.invite_interval = Some(std::time::Duration::from_millis(1));
    })
    .await;
    let carol = signup_ok(&s, &create_invite(&s, 1, None).await).await;
    s.xrpc.post("com.atproto.admin.disableAccountInvites", &json!({"account": carol.did}), &Auth::Admin).await.ok();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(account_codes(&s, &carol, &[]).await.len(), 0);
    let all = s.xrpc.get("com.atproto.admin.getInviteCodes", &[("limit", "500")], &Auth::Admin).await.ok();
    let hers: Vec<&J> = all["codes"].as_array().unwrap().iter().filter(|c| c["forAccount"] == json!(carol.did)).collect();
    assert_eq!(hers.len(), 5, "{all}");
    assert!(hers.iter().all(|c| c["disabled"] == json!(true) && c["createdBy"] == json!(carol.did)), "{hers:?}");
    // vlpds's enableAccountInvites re-enables the account's codes (the
    // reference only clears the flag and earns fresh ones): 5 unused, the cap
    s.xrpc.post("com.atproto.admin.enableAccountInvites", &json!({"account": carol.did}), &Auth::Admin).await.ok();
    let info = s.xrpc.get("com.atproto.admin.getAccountInfo", &[("did", &carol.did)], &Auth::Admin).await.ok();
    assert_eq!(info["invitesDisabled"], json!(false));
    let codes = account_codes(&s, &carol, &[]).await;
    assert!(!codes.is_empty());
    assert!(codes.iter().all(|c| c["disabled"] == json!(false)), "{codes:?}");
    assert_eq!(account_codes(&s, &carol, &[("includeUsed", "false")]).await.len(), 5, "at most 5 unused");
}
