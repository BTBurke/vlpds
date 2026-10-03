//! Argon2 overload shedding: with every Argon2 permit taken (a login flood),
//! request-path password checks and hashes answer 503 after
//! `state::ARGON2_MAX_WAIT` instead of queueing behind the flood: XRPC
//! `Overloaded` + Retry-After on createSession/createAccount, and a 503 page
//! + Retry-After on the OAuth sign-in form. Once the permits free up, the
//! same calls succeed (createAccount's handle/email claims were released).
//!
//! Its own binary because the Argon2 permits are process-wide: saturating
//! them inside tests/all would shed every other test's logins.

#[path = "all/common/mod.rs"]
mod common;

use common::*;
use std::time::{Duration, Instant};

/// Generous bound on a shed request: the 2 s permit wait plus slack. A
/// request that queued instead would wait for the held permits (forever).
const SHED_BOUND: Duration = Duration::from_secs(15);

fn csrf_of(html: &str) -> String {
    let i = html.find("name=\"csrf\" value=\"").expect("csrf field") + "name=\"csrf\" value=\"".len();
    html[i..i + html[i..].find('"').unwrap()].to_string()
}

fn device_cookie(h: &reqwest::header::HeaderMap) -> Option<String> {
    h.get_all("set-cookie").iter().map(|sc| sc.to_str().unwrap().split(';').next().unwrap().to_string()).find(|c| c.starts_with("vlpds-device="))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saturated_argon2_sheds_with_503() {
    let s = TestServer::spawn().await;
    let acct = s.create_account("shed").await;
    let new_handle = format!("{}.{HANDLE_DOMAIN}", unique_name("shednew"));
    let new_email = format!("{}@example.com", new_handle.replace('.', "-"));
    let new_body = json!({"handle": new_handle, "password": PASSWORD, "email": new_email});
    let create_new = || s.xrpc.post("com.atproto.server.createAccount", &new_body, &Auth::None);

    // the OAuth account page's form (device cookie + CSRF), before saturating
    let http = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let r = http.get(format!("{}/oauth/account", s.url)).send().await.unwrap();
    let cookie = device_cookie(r.headers()).expect("device cookie");
    let csrf = csrf_of(&r.text().await.unwrap());
    let oauth_sign_in = || {
        http.post(format!("{}/oauth/account/sign-in", s.url))
            .header("cookie", &cookie)
            .form(&[("csrf", csrf.as_str()), ("identifier", acct.handle.as_str()), ("password", PASSWORD)])
            .send()
    };

    let held = vlpds::state::hold_all_argon2_permits().await;
    let shed_before = vlpds::metrics::ARGON2_SHED.get();

    // createSession: XRPC 503 Overloaded + Retry-After, after the permit wait
    let t = Instant::now();
    let r = tokio::time::timeout(SHED_BOUND, s.create_session(&acct.handle, PASSWORD)).await.expect("createSession hung on a saturated Argon2 pool");
    r.err(503, "Overloaded");
    assert!(r.header("retry-after").is_some(), "Retry-After on {r:?}");
    assert!(t.elapsed() >= vlpds::state::ARGON2_MAX_WAIT - Duration::from_millis(100), "{:?}", t.elapsed());

    // createAccount: 503 Overloaded, nothing left claimed
    let r = tokio::time::timeout(SHED_BOUND, create_new()).await.expect("createAccount hung");
    r.err(503, "Overloaded");
    assert!(r.header("retry-after").is_some(), "Retry-After on {r:?}");

    // OAuth sign-in form: a 503 page with Retry-After
    let r = tokio::time::timeout(SHED_BOUND, oauth_sign_in()).await.expect("OAuth sign-in hung").unwrap();
    assert_eq!(r.status().as_u16(), 503);
    assert!(r.headers().get("retry-after").is_some(), "{:?}", r.headers());
    let html = r.text().await.unwrap();
    assert!(html.contains("The server is busy"), "{html}");

    assert!(vlpds::metrics::ARGON2_SHED.get() >= shed_before + 3);

    drop(held);

    // permits free: the same calls succeed
    s.create_session(&acct.handle, PASSWORD).await.ok();
    let j = create_new().await.ok();
    assert_eq!(j["handle"], new_handle.as_str(), "the shed createAccount released its handle claim");
    let r = oauth_sign_in().await.unwrap();
    assert_eq!(r.status().as_u16(), 303);
    assert_eq!(r.headers().get("location").unwrap(), "/oauth/account");
}
