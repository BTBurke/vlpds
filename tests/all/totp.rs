//! vlpds TOTP second factor (custom lexicons vlpds.server.setupTotp /
//! confirmTotp / disableTotp / getTotpStatus) and its effect on
//! com.atproto.server.createSession. Codes are computed here independently
//! (RFC 6238: HMAC-SHA-1, 30 s, 6 digits).
use crate::common::*;

fn b32_decode(s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let (mut buf, mut bits) = (0u32, 0);
    for c in s.trim_end_matches('=').bytes() {
        let v = match c.to_ascii_uppercase() {
            b'A'..=b'Z' => c.to_ascii_uppercase() - b'A',
            b'2'..=b'7' => c - b'2' + 26,
            _ => panic!("bad base32 char {c}"),
        } as u32;
        buf = (buf << 5) | v;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    out
}

fn hotp(secret: &[u8], counter: u64) -> String {
    use hmac::{Hmac, Mac};
    let mut mac = Hmac::<sha1::Sha1>::new_from_slice(secret).unwrap();
    mac.update(&counter.to_be_bytes());
    let h = mac.finalize().into_bytes();
    let off = (h[19] & 0xf) as usize;
    let bin =
        ((h[off] as u32 & 0x7f) << 24) | ((h[off + 1] as u32) << 16) | ((h[off + 2] as u32) << 8) | h[off + 3] as u32;
    format!("{:06}", bin % 1_000_000)
}

fn step_now() -> u64 {
    chrono::Utc::now().timestamp() as u64 / 30
}

#[test]
fn rfc6238_reference_vectors() {
    let k = b"12345678901234567890";
    assert_eq!(hotp(k, 59 / 30), "287082");
    assert_eq!(hotp(k, 1111111109 / 30), "081804");
    assert_eq!(hotp(k, 2000000000 / 30), "279037");
}

async fn status(s: &TestServer, a: &TestAccount) -> J {
    s.xrpc.get("vlpds.server.getTotpStatus", &[], &a.auth()).await.ok()
}

/// setup + confirm; returns (secret, step used to confirm, recovery codes).
async fn enable(s: &TestServer, a: &TestAccount) -> (Vec<u8>, u64, Vec<String>) {
    let j = s.xrpc.post_empty("vlpds.server.setupTotp", &a.auth()).await.ok();
    let secret_b32 = j["secret"].as_str().expect("secret").to_string();
    let uri = j["uri"].as_str().expect("otpauth uri");
    assert!(uri.starts_with("otpauth://totp/"), "{uri}");
    assert!(uri.contains(&format!("secret={secret_b32}")), "{uri}");
    assert!(uri.contains("period=30") || !uri.contains("period="), "{uri}");
    assert!(uri.contains("digits=6") || !uri.contains("digits="), "{uri}");
    let secret = b32_decode(&secret_b32);
    assert!(secret.len() >= 16, "secret too short: {} bytes", secret.len());
    let st = status(s, a).await;
    assert_eq!(st["enabled"], json!(false));

    // wrong code is rejected and leaves it pending
    s.xrpc.post("vlpds.server.confirmTotp", &json!({"code": "000000"}), &a.auth()).await.client_err();
    let step = step_now();
    let j = s.xrpc.post("vlpds.server.confirmTotp", &json!({"code": hotp(&secret, step)}), &a.auth()).await.ok();
    let codes: Vec<String> =
        j["recoveryCodes"].as_array().expect("recoveryCodes").iter().map(|c| c.as_str().unwrap().to_string()).collect();
    assert!(!codes.is_empty());
    let st = status(s, a).await;
    assert_eq!(st["enabled"], json!(true));
    (secret, step, codes)
}

async fn login(s: &TestServer, a: &TestAccount, code: Option<&str>) -> Resp {
    let mut body = json!({"identifier": a.handle, "password": a.password});
    if let Some(c) = code {
        body["authFactorToken"] = json!(c);
    }
    s.xrpc.post("com.atproto.server.createSession", &body, &Auth::None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn totp_requires_auth() {
    let s = TestServer::spawn().await;
    s.xrpc.post_empty("vlpds.server.setupTotp", &Auth::None).await.err_status(401);
    s.xrpc.get("vlpds.server.getTotpStatus", &[], &Auth::None).await.err_status(401);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn confirm_without_setup_fails() {
    let s = TestServer::spawn().await;
    let a = s.create_account("tq").await;
    s.xrpc.post("vlpds.server.confirmTotp", &json!({"code": "123456"}), &a.auth()).await.client_err();
    assert_eq!(status(&s, &a).await["enabled"], json!(false));
    // login without a factor is fine while TOTP is off
    login(&s, &a, None).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn login_requires_totp_once_enabled() {
    let s = TestServer::spawn().await;
    let a = s.create_account("tl").await;
    let (secret, step, recovery) = enable(&s, &a).await;

    // second setup while enabled is refused
    s.xrpc.post_empty("vlpds.server.setupTotp", &a.auth()).await.client_err();

    login(&s, &a, None).await.err(401, "AuthFactorTokenRequired");
    login(&s, &a, Some("")).await.err(401, "AuthFactorTokenRequired");
    let bad = login(&s, &a, Some("000000")).await;
    bad.client_err();
    assert_ne!(bad.status, 200);

    // the confirm code (step) was consumed; next step is within the skew window
    let next = hotp(&secret, step + 1);
    let j = login(&s, &a, Some(&next)).await.ok();
    assert_eq!(j["did"], json!(a.did));
    // replay of an accepted code fails, as does an older code
    login(&s, &a, Some(&next)).await.client_err();
    login(&s, &a, Some(&hotp(&secret, step))).await.client_err();
    // wrong password with a valid factor still fails as a password error
    let r = s
        .xrpc
        .post(
            "com.atproto.server.createSession",
            &json!({"identifier": a.handle, "password": "nope", "authFactorToken": recovery[0]}),
            &Auth::None,
        )
        .await;
    r.err(401, "AuthenticationRequired");

    // a recovery code works exactly once (case-insensitively)
    login(&s, &a, Some(&recovery[0].to_uppercase())).await.ok();
    login(&s, &a, Some(&recovery[0])).await.client_err();
    let st = status(&s, &a).await;
    if let Some(n) = st["recoveryCodesRemaining"].as_u64() {
        assert_eq!(n as usize, recovery.len() - 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn app_password_login_bypasses_totp() {
    let s = TestServer::spawn().await;
    let a = s.create_account("ta").await;
    let ap = s.xrpc.post("com.atproto.server.createAppPassword", &json!({"name": "ap"}), &a.auth()).await.ok();
    let pw = ap["password"].as_str().unwrap().to_string();
    enable(&s, &a).await;
    login(&s, &a, None).await.err(401, "AuthFactorTokenRequired");
    let j = s.create_session(&a.handle, &pw).await.ok();
    // ...but an app-password session cannot manage TOTP
    let app_auth = Auth::Bearer(j["accessJwt"].as_str().unwrap().into());
    s.xrpc.post_empty("vlpds.server.setupTotp", &app_auth).await.client_err();
    s.xrpc
        .post("vlpds.server.disableTotp", &json!({"password": a.password, "code": "000000"}), &app_auth)
        .await
        .client_err();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disable_totp() {
    let s = TestServer::spawn().await;
    let a = s.create_account("td").await;
    let (_secret, _step, recovery) = enable(&s, &a).await;
    // wrong password
    let r = s
        .xrpc
        .post("vlpds.server.disableTotp", &json!({"password": "nope", "recoveryCode": recovery[1]}), &a.auth())
        .await;
    r.client_err();
    // missing / wrong code
    s.xrpc.post("vlpds.server.disableTotp", &json!({"password": a.password}), &a.auth()).await.client_err();
    s.xrpc
        .post("vlpds.server.disableTotp", &json!({"password": a.password, "code": "000000"}), &a.auth())
        .await
        .client_err();
    assert_eq!(status(&s, &a).await["enabled"], json!(true));
    // valid recovery code disables
    s.xrpc
        .post("vlpds.server.disableTotp", &json!({"password": a.password, "recoveryCode": recovery[1]}), &a.auth())
        .await
        .ok();
    assert_eq!(status(&s, &a).await["enabled"], json!(false));
    login(&s, &a, None).await.ok();
    // can be enabled again with a fresh secret
    let (secret2, step2, _) = enable(&s, &a).await;
    login(&s, &a, None).await.err(401, "AuthFactorTokenRequired");
    login(&s, &a, Some(&hotp(&secret2, step2 + 1))).await.ok();
}
