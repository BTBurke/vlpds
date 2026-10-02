//! Reference-coverage ports (tests/REFERENCE_COVERAGE.md) of cases from
//! packages/pds/tests/{handles,handle-validation}.test.ts that the original
//! ports (handles, handle_validation) did not assert: error messages,
//! external-domain handles and long service domains.
use crate::common::*;
use std::time::Duration;

async fn update_handle(s: &TestServer, a: &TestAccount, handle: &str) -> Resp {
    s.xrpc
        .post("com.atproto.identity.updateHandle", &json!({"handle": handle}), &a.auth())
        .await
}

async fn resolve(s: &TestServer, handle: &str) -> Resp {
    s.xrpc
        .get("com.atproto.identity.resolveHandle", &[("handle", handle)], &Auth::None)
        .await
}

async fn describe(s: &TestServer, did: &str) -> J {
    s.xrpc
        .get("com.atproto.repo.describeRepo", &[("repo", did)], &Auth::None)
        .await
        .ok()
}

/// handles.test.ts "does not resolve unknown handles", "allows a user to
/// change their handle" (old handle), "does not allow taking a handle that
/// already exists": the reference's messages.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_handle_error_messages() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let b = s.create_account("bob").await;
    let r = resolve(&s, &format!("{}.{HANDLE_DOMAIN}", unique_name("john"))).await;
    r.err(400, "HandleNotFound");
    assert!(r.text().contains("Unable to resolve handle"), "{}", r.text());

    let r = update_handle(&s, &a, &b.handle.to_uppercase()).await;
    r.err_status(400);
    assert!(r.text().contains(&format!("Handle already taken: {}", b.handle)), "{}", r.text());

    let old = a.handle.clone();
    let new = format!("{}.{HANDLE_DOMAIN}", unique_name("alic"));
    update_handle(&s, &a, &new).await.ok();
    let r = resolve(&s, &old).await;
    r.err(400, "HandleNotFound");
    assert!(r.text().contains("Unable to resolve handle"), "{}", r.text());
}

/// handles.test.ts "allows updating to a dns handles": an external-domain
/// handle is accepted and lands in the account and the DID document. The
/// reference proves it with a mocked DNS TXT record; vlpds's dev mode skips
/// the external proof (DNS TXT verification is not implemented, see
/// REFERENCE_COVERAGE.md), so this checks the update path only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_updates_to_external_handle() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let ext = format!("{}.external", unique_name("alice"));
    update_handle(&s, &a, &ext).await.ok();
    let d = describe(&s, &a.did).await;
    assert_eq!(d["handle"], json!(ext));
    let aka: Vec<&str> = d["didDoc"]["alsoKnownAs"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(aka.contains(&format!("at://{ext}").as_str()), "{aka:?}");
}

/// handles.test.ts "disallows handles that do not resolve to a DID" / "does
/// not allow updating to an invalid dns handle": outside dev mode an
/// unprovable external handle is 400 "External handle did not resolve to
/// DID", and the account keeps its handle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_unresolvable_external_handle_message() {
    let s = TestServer::spawn_with(|c| c.dev_mode = false).await;
    let a = s.create_account("alice").await;
    let r = tokio::time::timeout(
        Duration::from_secs(20),
        update_handle(&s, &a, "noexist-vlpds-ref.example.com"),
    )
    .await
    .expect("bounded");
    r.err(400, "InvalidRequest");
    assert!(r.text().contains("External handle did not resolve to DID"), "{}", r.text());
    assert_eq!(describe(&s, &a.did).await["handle"], json!(a.handle));
}

/// handles.test.ts "requires admin auth": the reference's message.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_admin_update_handle_auth_message() {
    let s = TestServer::spawn().await;
    let b = s.create_account("bob").await;
    let body = json!({"did": b.did, "handle": format!("{}.{HANDLE_DOMAIN}", unique_name("balt"))});
    for auth in [b.auth(), Auth::None] {
        let r = s.xrpc.post("com.atproto.admin.updateAccountHandle", &body, &auth).await;
        r.err(401, "AuthenticationRequired");
    }
}

/// handle-validation.test.ts "validates handle length": the 18-character
/// limit applies to the first label only, so a long service domain is fine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_handle_length_with_long_service_domain() {
    let domain = "loooooooooooooooooong-pds-over18chars.mybsky.mydomain.com";
    let s = TestServer::spawn_with(|c| c.handle_domain = domain.to_string()).await;
    let try_create = |handle: String| {
        let s = &s;
        async move {
            let email = format!("{}@example.com", unique_name("e"));
            s.xrpc
                .post(
                    "com.atproto.server.createAccount",
                    &json!({"handle": handle, "password": "pw-123456", "email": email}),
                    &Auth::None,
                )
                .await
        }
    };
    let r = try_create(format!("usernamepartover18c.{domain}")).await;
    r.err(400, "InvalidHandle");
    assert!(r.text().contains("Handle too long"), "{}", r.text());
    let ok = try_create(format!("u23456789012345678.{domain}")).await;
    assert!(ok.is_ok(), "{}", ok.text());
}
