//! An invite code passed while invites are optional is checked and its use
//! recorded, as in the reference (createAccount -> ensureInviteIsAvailable +
//! recordInviteUse whenever `inviteCode` is set).
use crate::common::*;

async fn signup(s: &TestServer, code: Option<&str>) -> Resp {
    let name = unique_name("opt");
    let mut body = json!({"email": format!("{name}@example.com"), "handle": format!("{name}.{HANDLE_DOMAIN}"), "password": PASSWORD});
    if let Some(c) = code {
        body["inviteCode"] = json!(c);
    }
    s.xrpc.post("com.atproto.server.createAccount", &body, &Auth::None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn optional_invite_code_is_checked_and_recorded() {
    let s = TestServer::spawn().await;
    let j = s.xrpc.get("com.atproto.server.describeServer", &[], &Auth::None).await.ok();
    assert_eq!(j["inviteCodeRequired"], json!(false));
    let code = s.xrpc.post("com.atproto.server.createInviteCode", &json!({"useCount": 1}), &Auth::Admin).await.ok()["code"].as_str().unwrap().to_string();

    let did = signup(&s, Some(&code)).await.ok()["did"].as_str().unwrap().to_string();
    let codes = s.xrpc.get("com.atproto.admin.getInviteCodes", &[("limit", "500")], &Auth::Admin).await.ok();
    let c = codes["codes"].as_array().unwrap().iter().find(|c| c["code"] == json!(code)).expect("code listed");
    let used: Vec<&str> = c["uses"].as_array().unwrap().iter().map(|u| u["usedBy"].as_str().unwrap()).collect();
    assert_eq!(used, vec![did.as_str()], "{c}");
    let info = s.account_info(&did).await.ok();
    assert_eq!(info["invitedBy"]["code"], json!(code), "{info}");

    // used up, or unknown: refused even though invites are optional
    signup(&s, Some(&code)).await.err(400, "InvalidInviteCode");
    signup(&s, Some("no-such-code")).await.err(400, "InvalidInviteCode");
    // and no code at all is fine
    signup(&s, None).await.ok();
    signup(&s, Some("  ")).await.ok();
}
