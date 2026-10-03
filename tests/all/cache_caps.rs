//! Cache memory caps (src/caches.rs): every capped cache reports its entries,
//! approximate bytes and cap at /metrics. And the signature malleability
//! policy of inbound service-auth JWTs: high-S is accepted, as the reference
//! does (`allowMalleableSig`), while record proofs stay low-S
//! (src/oauth/lexicon.rs unit tests).

use crate::common::*;

fn gauge(metrics: &str, name: &str, cache: &str) -> Option<i64> {
    let prefix = format!("{name}{{cache=\"{cache}\"}} ");
    metrics.lines().find_map(|l| l.strip_prefix(&prefix)?.trim().parse::<f64>().ok().map(|v| v as i64))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cache_metrics_report_entries_bytes_and_caps() {
    let s = TestServer::spawn().await;
    let a = s.create_account("caches").await;
    // a verified access token lands in the session token cache
    s.get_session(&a.auth()).await.ok();
    let m = reqwest::get(format!("{}/metrics", s.url)).await.unwrap().text().await.unwrap();
    for c in vlpds::caches::Cache::ALL {
        let cap = gauge(&m, "vlpds_cache_capacity_entries", c.name()).unwrap_or_else(|| panic!("no cap for {}", c.name()));
        assert!(cap >= 1, "{}: cap {cap}", c.name());
        assert!(gauge(&m, "vlpds_cache_entries", c.name()).is_some(), "{}", c.name());
        assert!(gauge(&m, "vlpds_cache_bytes", c.name()).is_some(), "{}", c.name());
    }
    let n = gauge(&m, "vlpds_cache_entries", "session_tokens").unwrap();
    assert!(n >= 1, "session token cached: {n}");
    assert_eq!(gauge(&m, "vlpds_cache_bytes", "session_tokens").unwrap(), n * vlpds::caches::Cache::SessionTokens.entry_bytes() as i64);
}

/// The high-S form of an ES256K signature.
fn high_s(sig: &[u8]) -> Vec<u8> {
    let s = k256::ecdsa::Signature::from_slice(sig).unwrap();
    assert!(s.normalize_s().is_none(), "issued signatures are low-S");
    let high = k256::ecdsa::Signature::from_scalars(s.r(), -*s.s()).unwrap();
    high.to_bytes().to_vec()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_auth_accepts_high_s_like_the_reference() {
    const LXM: &str = "com.atproto.server.createAccount";
    let s = TestServer::spawn().await;
    let a = s.create_account("malleable").await;
    let aud = s.app.jwt.service_did.clone();
    let r = s.xrpc.get("com.atproto.server.getServiceAuth", &[("aud", &aud), ("lxm", LXM)], &a.auth()).await;
    let token = r.ok()["token"].as_str().unwrap().to_string();
    let (input, sig) = token.rsplit_once('.').unwrap();
    let sig = b64url_decode(sig);
    let verify = |t: String| {
        let app = s.app.clone();
        async move { vlpds::xrpc::authn::verify_service_jwt(&app, &t, Some(LXM)).await.map(|v| v.iss).map_err(|e| e.error) }
    };
    assert_eq!(verify(token.clone()).await, Ok(a.did.clone()));
    let high = format!("{input}.{}", b64url(high_s(&sig)));
    assert_eq!(verify(high).await, Ok(a.did.clone()), "high-S service JWT accepted");
    // still a signature check: a corrupted one fails in either form
    let mut bad = sig.clone();
    bad[40] ^= 1;
    assert_eq!(verify(format!("{input}.{}", b64url(&bad))).await, Err("BadJwtSignature".to_string()));
}
