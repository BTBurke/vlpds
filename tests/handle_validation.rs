//! Port of packages/pds/tests/handle-validation.test.ts, exercised through
//! createAccount (the server's service-domain constraints).
mod common;
use common::*;

async fn try_create(s: &TestServer, handle: &str) -> Resp {
    let email = format!("{}@example.com", unique_name("e"));
    s.xrpc.post("com.atproto.server.createAccount", &json!({"handle": handle, "password": "pw-123456", "email": email}), &Auth::None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validates_service_constraints() {
    let s = TestServer::spawn().await;
    let d = HANDLE_DOMAIN;
    for (handle, err, msg) in [
        (format!("j.{d}"), "InvalidHandle", "too short"),
        (format!("uk.{d}"), "InvalidHandle", "too short"),
        (format!("john.test.{d}"), "InvalidHandle", "Invalid characters"),
        (format!("about.{d}"), "HandleNotAvailable", "Reserved"),
        (format!("atp.{d}"), "HandleNotAvailable", "Reserved"),
        (format!("barackobama.{d}"), "HandleNotAvailable", "Reserved"),
    ] {
        let r = try_create(&s, &handle).await;
        assert_eq!((r.status, r.error_name()), (400, Some(err)), "{handle}: {}", r.text());
        assert!(r.text().contains(msg), "{handle}: expected '{msg}' in {}", r.text());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_handles_outside_service_domains() {
    let s = TestServer::spawn().await;
    for handle in ["john.bsky.io", "john.com", "john.test"] {
        let r = try_create(&s, handle).await;
        assert_eq!(r.status, 400, "{handle}: {}", r.text());
        assert!(
            matches!(r.error_name(), Some("InvalidHandle") | Some("UnsupportedDomain")),
            "{handle}: {}",
            r.text()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_bad_tlds() {
    let s = TestServer::spawn().await;
    for handle in ["atproto.local", "atproto.arpa", "atproto.invalid", "atproto.localhost", "atproto.onion", "atproto.internal"] {
        try_create(&s, handle).await.client_err();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validates_handle_length() {
    let s = TestServer::spawn().await;
    let d = HANDLE_DOMAIN;
    let r = try_create(&s, &format!("usernamepartover18c.{d}")).await;
    r.err(400, "InvalidHandle");
    assert!(r.text().contains("too long"), "{}", r.text());
    // 18 chars in the first label is fine
    let ok = try_create(&s, &format!("u23456789012345678.{d}")).await;
    assert!(ok.is_ok(), "{}", ok.text());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_invalid_handle_syntax() {
    let s = TestServer::spawn().await;
    let d = HANDLE_DOMAIN;
    for bad in [
        format!("jo_hn.{d}"),
        format!("jo hn.{d}"),
        format!("-john.{d}"),
        format!("john-.{d}"),
        format!("{}.{d}", "a".repeat(64)),
        "did:plc:abc".to_string(),
        format!("john..{d}"),
    ] {
        let r = try_create(&s, &bad).await;
        assert_eq!(r.status, 400, "{bad}: {}", r.text());
        assert!(matches!(r.error_name(), Some("InvalidHandle") | Some("InvalidRequest")), "{bad}: {}", r.text());
    }
}
