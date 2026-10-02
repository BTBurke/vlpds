//! Reference PDS invites-admin.test.ts ("pds admin invite views"), adapted:
//! vlpds has no periodic user invite codes (`inviteInterval`), so alice's
//! own codes are admin-gifted instead of interval-generated. The shape
//! checked is the same: one admin code used three times then disabled, codes
//! for alice, a disabled bob, listings by recency and usage, pagination,
//! getAccountInfo hydration and the account-invites switch.
use crate::common::*;
use std::time::Duration;

struct Fixture {
    s: TestServer,
    admin_code: String,
    alice: TestAccount,
    carol: TestAccount,
}

async fn invite(s: &TestServer, uses: u32, for_account: Option<&str>) -> String {
    let mut body = json!({"useCount": uses});
    if let Some(d) = for_account {
        body["forAccount"] = json!(d);
    }
    let code = s.xrpc.post("com.atproto.server.createInviteCode", &body, &Auth::Admin).await.ok()["code"].as_str().unwrap().to_string();
    // distinct createdAt values, so the recency order is unambiguous
    tokio::time::sleep(Duration::from_millis(3)).await;
    code
}

async fn signup(s: &TestServer, prefix: &str, code: &str) -> TestAccount {
    let handle = format!("{}.{HANDLE_DOMAIN}", unique_name(prefix));
    let email = format!("{}@example.com", handle.replace('.', "-"));
    let j = s
        .xrpc
        .post("com.atproto.server.createAccount", &json!({"handle": handle, "email": email, "password": PASSWORD, "inviteCode": code}), &Auth::None)
        .await
        .ok();
    TestAccount {
        did: j["did"].as_str().unwrap().into(),
        handle,
        password: PASSWORD.into(),
        email,
        access: j["accessJwt"].as_str().unwrap().into(),
        refresh: j["refreshJwt"].as_str().unwrap_or_default().into(),
    }
}

async fn fixture() -> Fixture {
    let s = TestServer::spawn_with(|c| c.invite_required = true).await;
    let admin_code = invite(&s, 10, None).await;
    let alice = signup(&s, "alice", &admin_code).await;
    let bob = signup(&s, "bob", &admin_code).await;
    let carol = signup(&s, "carol", &admin_code).await;
    let alice_codes = [invite(&s, 1, Some(&alice.did)).await, invite(&s, 1, Some(&alice.did)).await];
    invite(&s, 5, Some(&alice.did)).await;
    s.xrpc.post("com.atproto.admin.disableInviteCodes", &json!({"codes": [admin_code], "accounts": [bob.did]}), &Auth::Admin).await.ok();
    for c in &alice_codes {
        signup(&s, "invitee", c).await;
    }
    Fixture { s, admin_code, alice, carol }
}

async fn list(s: &TestServer, q: &[(&str, &str)]) -> J {
    s.xrpc.get("com.atproto.admin.getInviteCodes", q, &Auth::Admin).await.ok()
}

async fn list_paged(s: &TestServer, sort: &str, limit: &str) -> Vec<J> {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut q = vec![("sort", sort), ("limit", limit)];
        if let Some(c) = &cursor {
            q.push(("cursor", c.as_str()));
        }
        let j = list(s, &q).await;
        let page = j["codes"].as_array().unwrap().clone();
        assert!(page.len() <= limit.parse().unwrap());
        out.extend(page.clone());
        match j["cursor"].as_str() {
            Some(c) if !page.is_empty() => cursor = Some(c.to_string()),
            _ => break,
        }
        assert!(out.len() < 1000);
    }
    out
}

fn assert_view(c: &J, available: i64, disabled: bool, for_account: &str, uses: usize) {
    assert_eq!(c["available"], json!(available), "{c}");
    assert_eq!(c["disabled"], json!(disabled), "{c}");
    assert_eq!(c["forAccount"], json!(for_account), "{c}");
    assert_eq!(c["createdBy"], json!("admin"), "{c}");
    assert_eq!(c["uses"].as_array().map(|u| u.len()), Some(uses), "{c}");
}

/// "gets a list of invite codes by recency" + "paginates by recency"
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lists_invite_codes_by_recency_and_paginates() {
    let f = fixture().await;
    let full = list(&f.s, &[]).await;
    let codes = full["codes"].as_array().unwrap();
    assert_eq!(codes.len(), 4, "{full}");
    for w in codes.windows(2) {
        assert!(w[0]["createdAt"].as_str() >= w[1]["createdAt"].as_str(), "newest first: {full}");
    }
    assert_view(&codes[0], 5, false, &f.alice.did, 0);
    let last = codes.last().unwrap();
    assert_eq!(last["code"], json!(f.admin_code));
    assert_view(last, 10, true, "admin", 3);
    for u in last["uses"].as_array().unwrap() {
        assert!(u["usedBy"].as_str().unwrap().starts_with("did:") && u["usedAt"].is_string(), "{u}");
    }
    assert_eq!(&list_paged(&f.s, "recent", "1").await, codes);
    // the reference's two-page shape: first page of N, then the rest from its cursor
    let first = list(&f.s, &[("limit", "3")]).await;
    let second = list(&f.s, &[("cursor", first["cursor"].as_str().unwrap())]).await;
    let mut joined = first["codes"].as_array().unwrap().clone();
    joined.extend(second["codes"].as_array().unwrap().clone());
    assert_eq!(&joined, codes);
}

/// "gets a list of invite codes by usage" + "paginates by usage"
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lists_invite_codes_by_usage_and_paginates() {
    let f = fixture().await;
    let full = list(&f.s, &[("sort", "usage")]).await;
    let codes = full["codes"].as_array().unwrap();
    assert_eq!(codes.len(), 4);
    for w in codes.windows(2) {
        assert!(w[0]["uses"].as_array().unwrap().len() >= w[1]["uses"].as_array().unwrap().len(), "most used first: {full}");
    }
    assert_eq!(codes[0]["code"], json!(f.admin_code));
    assert_view(&codes[0], 10, true, "admin", 3);
    assert_eq!(&list_paged(&f.s, "usage", "1").await, codes);
    let first = list(&f.s, &[("sort", "usage"), ("limit", "2")]).await;
    let second = list(&f.s, &[("sort", "usage"), ("cursor", first["cursor"].as_str().unwrap())]).await;
    let mut joined = first["codes"].as_array().unwrap().clone();
    joined.extend(second["codes"].as_array().unwrap().clone());
    assert_eq!(&joined, codes);
    // an unknown sort is refused
    f.s.xrpc.get("com.atproto.admin.getInviteCodes", &[("sort", "bogus")], &Auth::Admin).await.err(400, "InvalidRequest");
}

/// "hydrates invites into admin.getAccountInfo"
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hydrates_invites_into_get_account_info() {
    let f = fixture().await;
    let v = f.s.xrpc.get("com.atproto.admin.getAccountInfo", &[("did", &f.alice.did)], &Auth::Admin).await.ok();
    assert_eq!(v["invitedBy"]["code"], json!(f.admin_code), "{v}");
    assert_eq!(v["invitedBy"]["available"], json!(10));
    assert_eq!(v["invitedBy"]["uses"].as_array().map(|u| u.len()), Some(3));
    let invites = v["invites"].as_array().unwrap();
    assert_eq!(invites.len(), 3, "alice's codes: {v}");
    assert!(invites.iter().all(|c| c["forAccount"] == json!(f.alice.did)));
    // getAccountInfos hydrates the same way
    let vs = f.s.xrpc.get_multi("com.atproto.admin.getAccountInfos", &[("dids", f.alice.did.clone())], &Auth::Admin).await.ok();
    let v2 = &vs["infos"][0];
    assert_eq!(v2["invitedBy"]["code"], json!(f.admin_code), "{vs}");
    assert_eq!(v2["invites"].as_array().map(|a| a.len()), Some(3));
}

/// "disables an account from getting additional invite codes", "allows
/// setting reason when enabling and disabling invite codes", "re-enables an
/// accounts invites". Divergent detail: in the reference the switch only
/// flips `invitesDisabled`, which gates the interval-generated codes vlpds
/// doesn't have; in vlpds it also disables (and re-enables) the account's
/// existing codes, so that is what is checked here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disables_and_reenables_account_invites() {
    let f = fixture().await;
    let carol = &f.carol;
    async fn info(s: &TestServer, did: String) -> J {
        s.xrpc.get("com.atproto.admin.getAccountInfo", &[("did", &did)], &Auth::Admin).await.ok()
    }
    let gifted = invite(&f.s, 2, Some(&carol.did)).await;
    let auth = carol.auth();
    let mine = |s: &TestServer| {
        let x = s.xrpc.clone();
        let auth = auth.clone();
        async move { x.get("com.atproto.server.getAccountInviteCodes", &[], &auth).await }
    };
    assert_eq!(mine(&f.s).await.ok()["codes"].as_array().map(|a| a.len()), Some(1));

    f.s.xrpc.post("com.atproto.admin.disableAccountInvites", &json!({"account": carol.did, "note": "spam"}), &Auth::Admin).await.ok();
    assert_eq!(info(&f.s, carol.did.clone()).await["invitesDisabled"], json!(true));
    assert_eq!(mine(&f.s).await.ok()["codes"], json!([]), "no usable codes while disabled");

    f.s.xrpc.post("com.atproto.admin.enableAccountInvites", &json!({"account": carol.did, "note": "ok now"}), &Auth::Admin).await.ok();
    assert_eq!(info(&f.s, carol.did.clone()).await["invitesDisabled"], json!(false));
    f.s.xrpc.post("com.atproto.admin.disableAccountInvites", &json!({"account": carol.did}), &Auth::Admin).await.ok();
    assert_eq!(info(&f.s, carol.did.clone()).await["invitesDisabled"], json!(true));
    f.s.xrpc.post("com.atproto.admin.enableAccountInvites", &json!({"account": carol.did}), &Auth::Admin).await.ok();
    assert_eq!(info(&f.s, carol.did.clone()).await["invitesDisabled"], json!(false));
    let codes = mine(&f.s).await.ok();
    assert_eq!(codes["codes"][0]["code"], json!(gifted), "codes usable again after re-enabling: {codes}");
    // both are admin-only
    f.s.xrpc.post("com.atproto.admin.disableAccountInvites", &json!({"account": carol.did}), &carol.auth()).await.client_err();
}
