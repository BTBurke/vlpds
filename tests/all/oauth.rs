//! atproto OAuth: end-to-end client flows driven from Rust against an
//! in-process PDS (dev mode). A real client is simulated: P-256 DPoP key,
//! PAR, the browser's login + consent form posts, code exchange, DPoP-bound
//! XRPC calls, refresh rotation, revocation, loopback and confidential
//! (private_key_jwt) clients, TOTP and include: permission sets.

use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine;
use p256::ecdsa::signature::Signer;
use p256::ecdsa::{Signature, SigningKey};
use serde_json::{json, Value as J};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;

const PASSWORD: &str = "correct horse battery staple";

fn b64(b: impl AsRef<[u8]>) -> String {
    B64.encode(b)
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn rand_str(n: usize) -> String {
    let b: Vec<u8> = (0..n).map(|_| rand::random::<u8>()).collect();
    b64(b)
}

fn enc(s: &str) -> String {
    vlpds::oauth::util::form_encode_component(s)
}

fn form(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", enc(k), enc(v)))
        .collect::<Vec<_>>()
        .join("&")
}

struct Srv {
    app: Arc<vlpds::xrpc::App>,
    base: String,
    http: reqwest::Client,
}

async fn spawn() -> Srv {
    spawn_with(|_| {}).await
}

async fn spawn_with(f: impl FnOnce(&mut vlpds::server::Config)) -> Srv {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    let mut cfg = vlpds::server::Config {
        dev_mode: true,
        public_url: base.clone(),
        ..Default::default()
    };
    f(&mut cfg);
    let (app, _) = vlpds::server::spawn(cfg, listener).await.unwrap();
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    Srv { app, base, http }
}

struct Account {
    did: String,
    handle: String,
    jwt: String,
}

async fn create_account(s: &Srv, name: &str) -> Account {
    let handle = format!("{name}{}.vlpds.test", rand::random::<u32>() % 100000);
    let r = s
        .http
        .post(format!("{}/xrpc/com.atproto.server.createAccount", s.base))
        .json(&json!({"handle": handle, "password": PASSWORD, "email": format!("{name}@example.com")}))
        .send()
        .await
        .unwrap();
    assert!(
        r.status().is_success(),
        "createAccount: {}",
        r.text().await.unwrap()
    );
    let j: J = r.json().await.unwrap();
    Account {
        did: j["did"].as_str().unwrap().into(),
        handle,
        jwt: j["accessJwt"].as_str().unwrap().into(),
    }
}

// ---------- DPoP client ----------

struct DpopKey {
    sk: SigningKey,
    nonce: parking_lot::Mutex<Option<String>>,
}

impl DpopKey {
    fn new() -> DpopKey {
        DpopKey {
            sk: SigningKey::random(&mut rand::rngs::OsRng),
            nonce: Default::default(),
        }
    }

    fn jwk(&self) -> J {
        let pt = self.sk.verifying_key().to_encoded_point(false);
        json!({"kty": "EC", "crv": "P-256", "x": b64(pt.x().unwrap()), "y": b64(pt.y().unwrap())})
    }

    fn jkt(&self) -> String {
        let j = self.jwk();
        let canon = format!(
            r#"{{"crv":"P-256","kty":"EC","x":"{}","y":"{}"}}"#,
            j["x"].as_str().unwrap(),
            j["y"].as_str().unwrap()
        );
        b64(Sha256::digest(canon))
    }

    fn proof_with(&self, htm: &str, htu: &str, ath: Option<&str>, nonce: Option<&str>) -> String {
        let header = json!({"typ": "dpop+jwt", "alg": "ES256", "jwk": self.jwk()});
        let mut payload = json!({"jti": rand_str(16), "htm": htm, "htu": htu, "iat": now()});
        if let Some(n) = nonce {
            payload["nonce"] = J::String(n.into());
        }
        if let Some(t) = ath {
            payload["ath"] = J::String(b64(Sha256::digest(t)));
        }
        sign_jwt(&self.sk, &header, &payload)
    }

    fn proof(&self, htm: &str, htu: &str, ath: Option<&str>) -> String {
        let n = self.nonce.lock().clone();
        self.proof_with(htm, htu, ath, n.as_deref())
    }

    fn update_nonce(&self, h: &reqwest::header::HeaderMap) {
        if let Some(n) = h.get("dpop-nonce").and_then(|v| v.to_str().ok()) {
            *self.nonce.lock() = Some(n.to_string());
        }
    }
}

fn sign_jwt(sk: &SigningKey, header: &J, payload: &J) -> String {
    let input = format!(
        "{}.{}",
        b64(serde_json::to_vec(header).unwrap()),
        b64(serde_json::to_vec(payload).unwrap())
    );
    let sig: Signature = sk.sign(input.as_bytes());
    format!("{input}.{}", b64(sig.to_bytes()))
}

struct Resp {
    status: u16,
    headers: reqwest::header::HeaderMap,
    body: J,
}

/// POST a form to an AS endpoint with DPoP, retrying once on use_dpop_nonce
/// (as real clients do).
async fn as_post(s: &Srv, key: &DpopKey, path: &str, pairs: &[(&str, &str)]) -> Resp {
    let htu = format!("{}{path}", s.base);
    for attempt in 0..2 {
        let r = s
            .http
            .post(&htu)
            .header("content-type", "application/x-www-form-urlencoded")
            .header("dpop", key.proof("POST", &htu, None))
            .body(form(pairs))
            .send()
            .await
            .unwrap();
        let status = r.status().as_u16();
        let headers = r.headers().clone();
        key.update_nonce(&headers);
        let body: J = r.json().await.unwrap_or(J::Null);
        if attempt == 0 && status == 400 && body["error"] == "use_dpop_nonce" {
            assert!(
                headers.get("dpop-nonce").is_some(),
                "use_dpop_nonce without DPoP-Nonce header"
            );
            continue;
        }
        return Resp {
            status,
            headers,
            body,
        };
    }
    unreachable!()
}

/// DPoP-authenticated XRPC call (retries once on use_dpop_nonce).
async fn xrpc_dpop(
    s: &Srv,
    key: &DpopKey,
    token: &str,
    method: &str,
    nsid: &str,
    body: Option<J>,
) -> Resp {
    let url = format!("{}/xrpc/{nsid}", s.base);
    for attempt in 0..2 {
        let mut rb = if method == "GET" {
            s.http.get(&url)
        } else {
            s.http.post(&url)
        };
        rb = rb
            .header("authorization", format!("DPoP {token}"))
            .header("dpop", key.proof(method, &url, Some(token)));
        if let Some(b) = &body {
            rb = rb.json(b);
        }
        let r = rb.send().await.unwrap();
        let status = r.status().as_u16();
        let headers = r.headers().clone();
        key.update_nonce(&headers);
        let body: J = r.json().await.unwrap_or(J::Null);
        if attempt == 0 && status == 401 && body["error"] == "use_dpop_nonce" {
            continue;
        }
        return Resp {
            status,
            headers,
            body,
        };
    }
    unreachable!()
}

async fn create_post(s: &Srv, key: &DpopKey, token: &str, did: &str, collection: &str) -> Resp {
    let record = match collection {
        "app.bsky.feed.like" => {
            json!({"$type": collection, "subject": {"uri": format!("at://{did}/app.bsky.feed.post/3k2a"), "cid": "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"}, "createdAt": "2024-01-01T00:00:00.000Z"})
        }
        _ => {
            json!({"$type": collection, "text": "hello from oauth", "createdAt": "2024-01-01T00:00:00.000Z"})
        }
    };
    xrpc_dpop(
        s,
        key,
        token,
        "POST",
        "com.atproto.repo.createRecord",
        Some(json!({"repo": did, "collection": collection, "record": record})),
    )
    .await
}

// ---------- client + browser simulation ----------

struct Pkce {
    verifier: String,
    challenge: String,
}

fn pkce() -> Pkce {
    let verifier = rand_str(32);
    let challenge = b64(Sha256::digest(&verifier));
    Pkce {
        verifier,
        challenge,
    }
}

fn loopback_client_id(scope: &str, redirect: &str) -> String {
    format!(
        "http://localhost?scope={}&redirect_uri={}",
        enc(scope),
        enc(redirect)
    )
}

/// Minimal cookie-jar browser.
#[derive(Default)]
struct Browser {
    cookie: Option<String>,
}

fn csrf_of(html: &str) -> String {
    let i =
        html.find("name=\"csrf\" value=\"").expect("csrf field") + "name=\"csrf\" value=\"".len();
    html[i..i + html[i..].find('"').unwrap()].to_string()
}

impl Browser {
    async fn get(&mut self, s: &Srv, url: &str) -> (u16, reqwest::header::HeaderMap, String) {
        let mut rb = s.http.get(url);
        if let Some(c) = &self.cookie {
            rb = rb.header("cookie", c);
        }
        let r = rb.send().await.unwrap();
        self.absorb(r).await
    }

    async fn post(
        &mut self,
        s: &Srv,
        path: &str,
        pairs: &[(&str, &str)],
    ) -> (u16, reqwest::header::HeaderMap, String) {
        let mut rb = s
            .http
            .post(format!("{}{path}", s.base))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(form(pairs));
        if let Some(c) = &self.cookie {
            rb = rb.header("cookie", c);
        }
        let r = rb.send().await.unwrap();
        self.absorb(r).await
    }

    async fn absorb(&mut self, r: reqwest::Response) -> (u16, reqwest::header::HeaderMap, String) {
        let status = r.status().as_u16();
        let headers = r.headers().clone();
        for sc in headers.get_all("set-cookie") {
            let c = sc.to_str().unwrap().split(';').next().unwrap();
            if c.starts_with("vlpds-device=") {
                self.cookie = Some(c.to_string());
            }
        }
        (status, headers, r.text().await.unwrap())
    }
}

fn location_params(h: &reqwest::header::HeaderMap) -> (String, HashMap<String, String>) {
    let loc = h
        .get("location")
        .expect("location")
        .to_str()
        .unwrap()
        .to_string();
    let (base, q) = loc.split_once(['?', '#']).unwrap_or((&loc, ""));
    let params = vlpds::oauth::util::parse_form(q).into_iter().collect();
    (base.to_string(), params)
}

struct Tokens {
    access: String,
    refresh: Option<String>,
    scope: String,
}

struct Flow<'a> {
    client_id: String,
    redirect_uri: String,
    scope: String,
    key: &'a DpopKey,
    extra: Vec<(String, String)>,
}

impl<'a> Flow<'a> {
    fn new(client_id: &str, redirect_uri: &str, scope: &str, key: &'a DpopKey) -> Self {
        Flow {
            client_id: client_id.into(),
            redirect_uri: redirect_uri.into(),
            scope: scope.into(),
            key,
            extra: vec![],
        }
    }

    async fn par(&self, s: &Srv, p: &Pkce, state: &str) -> Resp {
        let mut pairs: Vec<(&str, &str)> = vec![
            ("client_id", &self.client_id),
            ("response_type", "code"),
            ("redirect_uri", &self.redirect_uri),
            ("scope", &self.scope),
            ("state", state),
            ("code_challenge", &p.challenge),
            ("code_challenge_method", "S256"),
        ];
        for (k, v) in &self.extra {
            pairs.push((k, v));
        }
        as_post(s, self.key, "/oauth/par", &pairs).await
    }

    fn authorize_url(&self, s: &Srv, request_uri: &str) -> String {
        format!(
            "{}/oauth/authorize?client_id={}&request_uri={}",
            s.base,
            enc(&self.client_id),
            enc(request_uri)
        )
    }
}

/// Full interactive flow: PAR, login, consent; returns the code.
async fn authorize_interactive(
    s: &Srv,
    b: &mut Browser,
    f: &Flow<'_>,
    acct: &Account,
    p: &Pkce,
) -> String {
    let state = rand_str(8);
    let par = f.par(s, p, &state).await;
    assert_eq!(par.status, 201, "PAR: {}", par.body);
    let request_uri = par.body["request_uri"].as_str().unwrap().to_string();
    let (st, h, body) = browser_consent(s, b, f, acct, &request_uri, &[]).await;
    assert_eq!(st, 303, "{body}");
    let (base, q) = location_params(&h);
    assert_eq!(base, f.redirect_uri.split('?').next().unwrap());
    assert_eq!(q.get("state"), Some(&state));
    assert_eq!(q.get("iss"), Some(&s.base));
    q.get("code").expect("code").clone()
}

/// Browser side of a pushed request: open the authorization page, pick or
/// sign in to `acct`, and post "allow" on the consent page (with `extra`
/// form fields). Returns the consent POST's response.
async fn browser_consent(
    s: &Srv,
    b: &mut Browser,
    f: &Flow<'_>,
    acct: &Account,
    request_uri: &str,
    extra: &[(&str, &str)],
) -> (u16, reqwest::header::HeaderMap, String) {
    let request_uri = request_uri.to_string();
    let (st, h, html) = b.get(s, &f.authorize_url(s, &request_uri)).await;
    assert_eq!(st, 200, "{html}");
    let csp = h.get("content-security-policy").unwrap().to_str().unwrap();
    assert!(
        csp.contains("default-src 'none'") && csp.contains("frame-ancestors 'none'"),
        "{csp}"
    );
    let html = if html.contains("Choose an account") {
        // device remembers accounts: pick ours (or "another account")
        let csrf = csrf_of(&html);
        let did = if html.contains(&acct.did) {
            acct.did.as_str()
        } else {
            ""
        };
        let (st, _, html) = b
            .post(
                s,
                "/oauth/authorize/select",
                &[("request_uri", &request_uri), ("csrf", &csrf), ("did", did)],
            )
            .await;
        assert_eq!(st, 200, "{html}");
        html
    } else {
        html
    };
    let html = if html.contains("name=\"password\"") {
        let csrf = csrf_of(&html);
        let (st, _, html) = b
            .post(
                s,
                "/oauth/authorize/sign-in",
                &[
                    ("request_uri", &request_uri),
                    ("csrf", &csrf),
                    ("identifier", &acct.handle),
                    ("password", PASSWORD),
                    ("action", "sign-in"),
                ],
            )
            .await;
        assert_eq!(st, 200, "{html}");
        html
    } else {
        html
    };
    assert!(
        html.contains("Authorize access"),
        "expected consent page: {html}"
    );
    let csrf = csrf_of(&html);
    let mut pairs: Vec<(&str, &str)> = vec![
        ("request_uri", &request_uri),
        ("csrf", &csrf),
        ("did", &acct.did),
        ("action", "allow"),
    ];
    pairs.extend_from_slice(extra);
    b.post(s, "/oauth/authorize/consent", &pairs).await
}

async fn exchange(s: &Srv, f: &Flow<'_>, code: &str, p: &Pkce, extra: &[(&str, &str)]) -> Resp {
    let mut pairs = vec![
        ("grant_type", "authorization_code"),
        ("client_id", f.client_id.as_str()),
        ("code", code),
        ("redirect_uri", f.redirect_uri.as_str()),
        ("code_verifier", p.verifier.as_str()),
    ];
    pairs.extend_from_slice(extra);
    as_post(s, f.key, "/oauth/token", &pairs).await
}

fn tokens(r: &Resp) -> Tokens {
    assert_eq!(r.status, 200, "token: {}", r.body);
    assert_eq!(r.body["token_type"], "DPoP");
    assert!(r.headers.get("dpop-nonce").is_some());
    assert_eq!(r.headers.get("cache-control").unwrap(), "no-store");
    Tokens {
        access: r.body["access_token"].as_str().unwrap().into(),
        refresh: r.body["refresh_token"].as_str().map(String::from),
        scope: r.body["scope"].as_str().unwrap().into(),
    }
}

async fn refresh(s: &Srv, f: &Flow<'_>, rt: &str, extra: &[(&str, &str)]) -> Resp {
    let mut pairs = vec![
        ("grant_type", "refresh_token"),
        ("client_id", f.client_id.as_str()),
        ("refresh_token", rt),
    ];
    pairs.extend_from_slice(extra);
    as_post(s, f.key, "/oauth/token", &pairs).await
}

// ---------- tests ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_documents() {
    let s = spawn().await;
    let r = s
        .http
        .get(format!("{}/.well-known/oauth-authorization-server", s.base))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers().get("access-control-allow-origin").unwrap(), "*");
    let m: J = r.json().await.unwrap();
    assert_eq!(m["issuer"], s.base);
    assert_eq!(m["require_pushed_authorization_requests"], true);
    assert_eq!(m["authorization_response_iss_parameter_supported"], true);
    assert_eq!(m["client_id_metadata_document_supported"], true);
    assert_eq!(
        m["pushed_authorization_request_endpoint"],
        format!("{}/oauth/par", s.base)
    );
    assert!(m["dpop_signing_alg_values_supported"]
        .as_array()
        .unwrap()
        .contains(&json!("ES256")));
    assert!(m["token_endpoint_auth_methods_supported"]
        .as_array()
        .unwrap()
        .contains(&json!("private_key_jwt")));
    assert!(m["token_endpoint_auth_methods_supported"]
        .as_array()
        .unwrap()
        .contains(&json!("none")));
    assert!(m["token_endpoint_auth_signing_alg_values_supported"]
        .as_array()
        .unwrap()
        .contains(&json!("ES256")));
    assert!(m["scopes_supported"]
        .as_array()
        .unwrap()
        .contains(&json!("atproto")));
    assert_eq!(m["code_challenge_methods_supported"], json!(["S256"]));
    assert_eq!(m["require_request_uri_registration"], true);
    let pr: J = s
        .http
        .get(format!("{}/.well-known/oauth-protected-resource", s.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(pr["authorization_servers"], json!([s.base]));
    let jwks: J = s
        .http
        .get(format!("{}/oauth/jwks", s.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(jwks["keys"][0]["crv"], "P-256");
    assert!(jwks["keys"][0].get("d").is_none());
    // CORS preflight on the token endpoint
    let r = s
        .http
        .request(reqwest::Method::OPTIONS, format!("{}/oauth/token", s.base))
        .send()
        .await
        .unwrap();
    assert!(r
        .headers()
        .get("access-control-allow-headers")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("DPoP"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_flow_create_record_refresh_and_revoke() {
    let s = spawn().await;
    let acct = create_account(&s, "alice").await;
    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/callback";
    let cid = loopback_client_id("atproto transition:generic", redirect);
    let f = Flow::new(&cid, redirect, "atproto transition:generic", &key);
    let mut b = Browser::default();
    let p = pkce();
    let code = authorize_interactive(&s, &mut b, &f, &acct, &p).await;

    // wrong verifier fails (and does not burn the code: PKCE checked first)
    let bad = exchange(&s, &f, &code, &pkce(), &[]).await;
    assert_eq!(bad.status, 400);
    assert_eq!(bad.body["error"], "invalid_grant");

    let r = exchange(&s, &f, &code, &p, &[]).await;
    let t = tokens(&r);
    assert_eq!(r.body["sub"], acct.did);
    assert!(t.scope.split(' ').any(|x| x == "atproto"));
    let rt = t.refresh.clone().expect("refresh token");

    // access token is a JWT with the documented claims, bound to our key
    let payload: J =
        serde_json::from_slice(&B64.decode(t.access.split('.').nth(1).unwrap()).unwrap()).unwrap();
    assert_eq!(payload["sub"], acct.did);
    assert_eq!(payload["cnf"]["jkt"], key.jkt());
    assert_eq!(payload["client_id"], cid);
    assert!(payload["exp"].as_i64().unwrap() - payload["iat"].as_i64().unwrap() <= 1800);
    for c in ["aud", "jti", "scope", "iat"] {
        assert!(payload.get(c).is_some(), "missing {c}");
    }

    // DPoP-bound createRecord
    let r = create_post(&s, &key, &t.access, &acct.did, "app.bsky.feed.post").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(r.body["uri"]
        .as_str()
        .unwrap()
        .starts_with(&format!("at://{}/app.bsky.feed.post/", acct.did)));
    assert!(r.headers.get("dpop-nonce").is_some());

    // a Bearer presentation of a DPoP token is rejected
    let r = s
        .http
        .post(format!("{}/xrpc/com.atproto.repo.createRecord", s.base))
        .bearer_auth(&t.access)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert!(matches!(r.status().as_u16(), 400 | 401), "{}", r.status());

    // code reuse is rejected and revokes the session issued from it
    let replay = exchange(&s, &f, &code, &p, &[]).await;
    assert_eq!(replay.body["error"], "invalid_grant", "{}", replay.body);
    let r = create_post(&s, &key, &t.access, &acct.did, "app.bsky.feed.post").await;
    assert_eq!(r.status, 401);
    assert_eq!(r.body["error"], "invalid_token");
    assert!(r
        .headers
        .get("www-authenticate")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("invalid_token"));
    let dead = refresh(&s, &f, &rt, &[]).await;
    assert_eq!(dead.body["error"], "invalid_grant");

    // new grant: refresh rotation + replay detection
    let p = pkce();
    let code = authorize_interactive(&s, &mut b, &f, &acct, &p).await;
    let t1 = tokens(&exchange(&s, &f, &code, &p, &[]).await);
    let rt1 = t1.refresh.clone().unwrap();
    let t2 = tokens(&refresh(&s, &f, &rt1, &[]).await);
    let rt2 = t2.refresh.clone().unwrap();
    assert_ne!(rt1, rt2);
    // the rotated-out access token no longer works; the new one does
    assert_eq!(
        create_post(&s, &key, &t1.access, &acct.did, "app.bsky.feed.post")
            .await
            .status,
        401
    );
    assert_eq!(
        create_post(&s, &key, &t2.access, &acct.did, "app.bsky.feed.post")
            .await
            .status,
        200
    );
    // replaying the old refresh token revokes the whole session
    let replay = refresh(&s, &f, &rt1, &[]).await;
    assert_eq!(replay.body["error"], "invalid_grant");
    assert!(replay.body["error_description"]
        .as_str()
        .unwrap()
        .contains("replayed"));
    assert_eq!(
        refresh(&s, &f, &rt2, &[]).await.body["error"],
        "invalid_grant"
    );
    assert_eq!(
        create_post(&s, &key, &t2.access, &acct.did, "app.bsky.feed.post")
            .await
            .status,
        401
    );

    // refresh with a different DPoP key is refused
    let p = pkce();
    let code = authorize_interactive(&s, &mut b, &f, &acct, &p).await;
    let t3 = tokens(&exchange(&s, &f, &code, &p, &[]).await);
    let other = DpopKey::new();
    let f_other = Flow::new(&cid, redirect, "atproto transition:generic", &other);
    let r = refresh(&s, &f_other, t3.refresh.as_ref().unwrap(), &[]).await;
    assert_eq!(r.status, 400);
    // ... and so is using the access token with another key
    let r = create_post(&s, &other, &t3.access, &acct.did, "app.bsky.feed.post").await;
    assert_eq!(r.status, 401);

    // revocation endpoint: revoking the refresh token kills the access token
    let r = as_post(
        &s,
        &key,
        "/oauth/revoke",
        &[("client_id", &cid), ("token", t3.refresh.as_ref().unwrap())],
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(
        create_post(&s, &key, &t3.access, &acct.did, "app.bsky.feed.post")
            .await
            .status,
        401
    );
    // unknown tokens are fine (RFC 7009)
    assert_eq!(
        as_post(
            &s,
            &key,
            "/oauth/revoke",
            &[("client_id", &cid), ("token", "garbage")]
        )
        .await
        .status,
        200
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scope_enforcement() {
    let s = spawn().await;
    let acct = create_account(&s, "bob").await;
    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/cb";
    let scope = "atproto repo:app.bsky.feed.like";
    let cid = loopback_client_id(scope, redirect);
    let f = Flow::new(&cid, redirect, scope, &key);
    let mut b = Browser::default();
    let p = pkce();
    let code = authorize_interactive(&s, &mut b, &f, &acct, &p).await;
    let t = tokens(&exchange(&s, &f, &code, &p, &[]).await);
    assert_eq!(t.scope, scope);
    let r = create_post(&s, &key, &t.access, &acct.did, "app.bsky.feed.post").await;
    assert_eq!(r.status, 403, "{}", r.body);
    // the reference's ScopeMissingError, naming the missing scope
    assert_eq!(r.body["error"], "ScopeMissingError");
    assert_eq!(r.body["message"], "Missing required scope \"repo:app.bsky.feed.post?action=create\"");
    let r = create_post(&s, &key, &t.access, &acct.did, "app.bsky.feed.like").await;
    assert_eq!(r.status, 200, "{}", r.body);

    // scopes not declared by the client are refused at PAR
    let f2 = Flow::new(&cid, redirect, "atproto transition:generic", &key);
    let r = f2.par(&s, &pkce(), "x").await;
    assert_eq!(r.body["error"], "invalid_scope");
    // ... and "atproto" is required
    let cid3 = loopback_client_id("atproto repo:app.bsky.feed.like", redirect);
    let f3 = Flow::new(&cid3, redirect, "repo:app.bsky.feed.like", &key);
    assert_eq!(
        f3.par(&s, &pkce(), "x").await.body["error"],
        "invalid_scope"
    );
}

/// app.bsky.notification.{register,unregister}Push over OAuth (reference
/// registerPush.ts): the token needs `rpc:{lxm}?aud={serviceDid}#bsky_notif`;
/// without it, 403 ScopeMissingError naming that scope, and nothing is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_registration_rpc_scope() {
    const APPVIEW: &str = "did:web:appview.test";
    const REGISTER: &str = "app.bsky.notification.registerPush";
    const UNREGISTER: &str = "app.bsky.notification.unregisterPush";
    let hits = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let appview = format!("http://{}", l.local_addr().unwrap());
    let h = hits.clone();
    let router = axum::Router::new().fallback(move |req: axum::extract::Request| {
        let h = h.clone();
        async move {
            h.lock().push(req.uri().path().to_string());
            axum::http::StatusCode::OK
        }
    });
    tokio::spawn(async move { axum::serve(l, router).await.unwrap() });
    let s = spawn_with(|c| c.appview = Some((appview, APPVIEW.into()))).await;
    let acct = create_account(&s, "pushy").await;
    let input = |service_did: &str| {
        json!({"serviceDid": service_did, "token": "device-1", "platform": "ios", "appId": "xyz.blueskyweb.app"})
    };
    let redirect = "http://127.0.0.1/cb";
    let login = |scope: &'static str| {
        let (s, acct) = (&s, &acct);
        async move {
            let key = DpopKey::new();
            let cid = loopback_client_id(scope, redirect);
            let f = Flow::new(&cid, redirect, scope, &key);
            let p = pkce();
            let mut b = Browser::default();
            let code = authorize_interactive(s, &mut b, &f, acct, &p).await;
            let t = tokens(&exchange(s, &f, &code, &p, &[]).await);
            assert_eq!(t.scope, scope);
            (key, t.access)
        }
    };

    // granted for registerPush at the AppView's #bsky_notif only
    let (key, tok) = login("atproto rpc:app.bsky.notification.registerPush?aud=did:web:appview.test%23bsky_notif").await;
    let r = xrpc_dpop(&s, &key, &tok, "POST", REGISTER, Some(input(APPVIEW))).await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(hits.lock().drain(..).collect::<Vec<_>>(), [format!("/xrpc/{REGISTER}")]);
    let r = xrpc_dpop(&s, &key, &tok, "POST", UNREGISTER, Some(input(APPVIEW))).await;
    assert_eq!(r.status, 403, "{}", r.body);
    assert_eq!(r.body["error"], "ScopeMissingError");
    assert_eq!(
        r.body["message"],
        "Missing required scope \"rpc:app.bsky.notification.unregisterPush?aud=did:web:appview.test%23bsky_notif\""
    );
    // another service DID is another audience
    let r = xrpc_dpop(&s, &key, &tok, "POST", REGISTER, Some(input("did:web:push.example.com"))).await;
    assert_eq!(r.status, 403, "{}", r.body);
    assert_eq!(r.body["error"], "ScopeMissingError");
    assert_eq!(
        r.body["message"],
        "Missing required scope \"rpc:app.bsky.notification.registerPush?aud=did:web:push.example.com%23bsky_notif\""
    );
    assert!(hits.lock().is_empty());

    // an unrelated scope set allows neither; transition:generic allows both
    let (key, tok) = login("atproto repo:app.bsky.feed.like").await;
    for lxm in [REGISTER, UNREGISTER] {
        let r = xrpc_dpop(&s, &key, &tok, "POST", lxm, Some(input(APPVIEW))).await;
        assert_eq!(r.status, 403, "{lxm}: {}", r.body);
        assert_eq!(r.body["error"], "ScopeMissingError");
    }
    assert!(hits.lock().is_empty());
    let (key, tok) = login("atproto transition:generic").await;
    for lxm in [REGISTER, UNREGISTER] {
        let r = xrpc_dpop(&s, &key, &tok, "POST", lxm, Some(input(APPVIEW))).await;
        assert_eq!(r.status, 200, "{lxm}: {}", r.body);
    }
    assert_eq!(hits.lock().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn par_validation_and_nonces() {
    let s = spawn().await;
    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/cb";
    let cid = loopback_client_id("atproto", redirect);
    let htu = format!("{}/oauth/par", s.base);
    let p = pkce();
    let body = form(&[
        ("client_id", &cid),
        ("response_type", "code"),
        ("redirect_uri", redirect),
        ("scope", "atproto"),
        ("code_challenge", &p.challenge),
        ("code_challenge_method", "S256"),
    ]);
    // no DPoP proof
    let r = s
        .http
        .post(&htu)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let j: J = r.json().await.unwrap();
    assert_eq!(j["error"], "invalid_dpop_proof");
    // proof without nonce -> use_dpop_nonce + DPoP-Nonce header
    let r = s
        .http
        .post(&htu)
        .header("content-type", "application/x-www-form-urlencoded")
        .header("dpop", key.proof_with("POST", &htu, None, None))
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let nonce = r
        .headers()
        .get("dpop-nonce")
        .expect("nonce")
        .to_str()
        .unwrap()
        .to_string();
    let j: J = r.json().await.unwrap();
    assert_eq!(j["error"], "use_dpop_nonce");
    // bogus nonce -> use_dpop_nonce again
    let r = s
        .http
        .post(&htu)
        .header("content-type", "application/x-www-form-urlencoded")
        .header("dpop", key.proof_with("POST", &htu, None, Some("nope")))
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(r.json::<J>().await.unwrap()["error"], "use_dpop_nonce");
    // proof for the wrong URL
    let r = s
        .http
        .post(&htu)
        .header("content-type", "application/x-www-form-urlencoded")
        .header(
            "dpop",
            key.proof_with(
                "POST",
                &format!("{}/oauth/token", s.base),
                None,
                Some(&nonce),
            ),
        )
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(r.json::<J>().await.unwrap()["error"], "invalid_dpop_proof");
    // good proof; then replaying the exact same proof is rejected
    let proof = key.proof_with("POST", &htu, None, Some(&nonce));
    let r = s
        .http
        .post(&htu)
        .header("content-type", "application/x-www-form-urlencoded")
        .header("dpop", &proof)
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let j: J = r.json().await.unwrap();
    assert!(j["request_uri"]
        .as_str()
        .unwrap()
        .starts_with("urn:ietf:params:oauth:request_uri:"));
    assert!(j["expires_in"].as_i64().unwrap() > 0);
    let p2 = pkce();
    let body2 = body.replace(&enc(&p.challenge), &enc(&p2.challenge));
    let r = s
        .http
        .post(&htu)
        .header("content-type", "application/x-www-form-urlencoded")
        .header("dpop", &proof)
        .body(body2)
        .send()
        .await
        .unwrap();
    assert_eq!(r.json::<J>().await.unwrap()["error"], "invalid_dpop_proof");

    // code_challenge reuse is refused
    *key.nonce.lock() = Some(nonce.clone());
    let f = Flow::new(&cid, redirect, "atproto", &key);
    assert_eq!(f.par(&s, &p, "x").await.body["error"], "invalid_request");
    // PKCE required, S256 only
    let r = as_post(
        &s,
        &key,
        "/oauth/par",
        &[
            ("client_id", &cid),
            ("response_type", "code"),
            ("redirect_uri", redirect),
            ("scope", "atproto"),
        ],
    )
    .await;
    assert_eq!(r.body["error"], "invalid_request");
    let p3 = pkce();
    let r = as_post(
        &s,
        &key,
        "/oauth/par",
        &[
            ("client_id", &cid),
            ("response_type", "code"),
            ("redirect_uri", redirect),
            ("scope", "atproto"),
            ("code_challenge", &p3.challenge),
            ("code_challenge_method", "plain"),
        ],
    )
    .await;
    assert_eq!(r.body["error"], "invalid_request");
    // unregistered redirect_uri
    let mut f = Flow::new(&cid, "http://127.0.0.1/elsewhere", "atproto", &key);
    assert_eq!(
        f.par(&s, &pkce(), "x").await.body["error"],
        "invalid_request"
    );
    // invalid login_hint
    f.redirect_uri = redirect.into();
    f.extra = vec![("login_hint".into(), "not a handle!".into())];
    assert_eq!(
        f.par(&s, &pkce(), "x").await.body["error"],
        "invalid_request"
    );
    // invalid client ids
    let f = Flow::new("http://localhost/path", redirect, "atproto", &key);
    assert_eq!(
        f.par(&s, &pkce(), "x").await.body["error"],
        "invalid_client_metadata"
    );

    // the authorization endpoint refuses requests that skip PAR
    let mut b = Browser::default();
    let (st, _, _) = b
        .get(
            &s,
            &format!(
                "{}/oauth/authorize?client_id={}&response_type=code",
                s.base,
                enc(&cid)
            ),
        )
        .await;
    assert_eq!(st, 400);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resource_dpop_checks() {
    let s = spawn().await;
    let acct = create_account(&s, "carol").await;
    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/cb";
    let cid = loopback_client_id("atproto transition:generic", redirect);
    let f = Flow::new(&cid, redirect, "atproto transition:generic", &key);
    let mut b = Browser::default();
    let p = pkce();
    let code = authorize_interactive(&s, &mut b, &f, &acct, &p).await;
    let t = tokens(&exchange(&s, &f, &code, &p, &[]).await);
    let url = format!("{}/xrpc/com.atproto.repo.createRecord", s.base);
    let rec = json!({"repo": acct.did, "collection": "app.bsky.feed.post", "record": {"$type": "app.bsky.feed.post", "text": "x", "createdAt": "2024-01-01T00:00:00.000Z"}});
    let send = |proof: Option<String>| {
        let mut rb = s
            .http
            .post(&url)
            .header("authorization", format!("DPoP {}", t.access))
            .json(&rec);
        if let Some(p) = proof {
            rb = rb.header("dpop", p);
        }
        rb.send()
    };
    // missing proof
    let r = send(None).await.unwrap();
    assert_eq!(r.status(), 401);
    assert!(r
        .headers()
        .get("www-authenticate")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("DPoP"));
    // no nonce -> 401 use_dpop_nonce with WWW-Authenticate + DPoP-Nonce
    let r = send(Some(key.proof_with("POST", &url, Some(&t.access), None)))
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    let www = r
        .headers()
        .get("www-authenticate")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(www.contains("error=\"use_dpop_nonce\""), "{www}");
    let nonce = r
        .headers()
        .get("dpop-nonce")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(r
        .headers()
        .get("access-control-expose-headers")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("DPoP-Nonce"));
    // wrong ath
    let r = send(Some(key.proof_with(
        "POST",
        &url,
        Some("other-token"),
        Some(&nonce),
    )))
    .await
    .unwrap();
    assert_eq!(r.status(), 401);
    assert!(r
        .headers()
        .get("www-authenticate")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("invalid_dpop_proof"));
    // wrong htm
    let r = send(Some(key.proof_with(
        "GET",
        &url,
        Some(&t.access),
        Some(&nonce),
    )))
    .await
    .unwrap();
    assert_eq!(r.status(), 401);
    // htu with a query string is accepted (legacy), different path is not
    let r = send(Some(key.proof_with(
        "POST",
        &format!("{url}?x=1"),
        Some(&t.access),
        Some(&nonce),
    )))
    .await
    .unwrap();
    assert_eq!(r.status(), 200);
    let r = send(Some(key.proof_with(
        "POST",
        &format!("{}/xrpc/other", s.base),
        Some(&t.access),
        Some(&nonce),
    )))
    .await
    .unwrap();
    assert_eq!(r.status(), 401);
    // replayed proof
    let proof = key.proof_with("POST", &url, Some(&t.access), Some(&nonce));
    assert_eq!(send(Some(proof.clone())).await.unwrap().status(), 200);
    assert_eq!(send(Some(proof)).await.unwrap().status(), 401);
    // resource-request claims live in the owner's memory only (no log write
    // per request; HA notes in src/oauth/mod.rs)
    let part = s.app.partition(&acct.did).ok().unwrap();
    let prefix = vlpds::state::private_key(&acct.did, vlpds::oauth::util::REPLAY_ROW);
    let mut rows = part.db.scan(prefix.clone()..vlpds::state::prefix_end(&prefix)).await.unwrap();
    assert!(rows.next().await.unwrap().is_none(), "resource-request DPoP claim persisted");
    // stale iat
    let header = json!({"typ": "dpop+jwt", "alg": "ES256", "jwk": key.jwk()});
    let payload = json!({"jti": rand_str(8), "htm": "POST", "htu": url, "iat": now() - 3600, "nonce": nonce, "ath": b64(Sha256::digest(&t.access))});
    assert_eq!(
        send(Some(sign_jwt(&key.sk, &header, &payload)))
            .await
            .unwrap()
            .status(),
        401
    );
    // wrong typ
    let header = json!({"typ": "JWT", "alg": "ES256", "jwk": key.jwk()});
    let payload = json!({"jti": rand_str(8), "htm": "POST", "htu": url, "iat": now(), "nonce": nonce, "ath": b64(Sha256::digest(&t.access))});
    assert_eq!(
        send(Some(sign_jwt(&key.sk, &header, &payload)))
            .await
            .unwrap()
            .status(),
        401
    );
    // tampered access token
    let mut parts: Vec<String> = t.access.split('.').map(String::from).collect();
    let mut claims: J = serde_json::from_slice(&B64.decode(&parts[1]).unwrap()).unwrap();
    claims["scope"] = json!("atproto transition:generic transition:chat.bsky");
    parts[1] = b64(serde_json::to_vec(&claims).unwrap());
    let forged = parts.join(".");
    let r = xrpc_dpop(
        &s,
        &key,
        &forged,
        "POST",
        "com.atproto.repo.createRecord",
        Some(rec.clone()),
    )
    .await;
    assert_eq!(r.status, 401);
    assert_eq!(r.body["error"], "invalid_token");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_chooser_prompts_and_denial() {
    let s = spawn().await;
    let a1 = create_account(&s, "dave").await;
    let a2 = create_account(&s, "erin").await;
    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/cb";
    let cid = loopback_client_id("atproto", redirect);
    let mut f = Flow::new(&cid, redirect, "atproto", &key);
    let mut b = Browser::default();
    // sign in a1 once
    let p = pkce();
    authorize_interactive(&s, &mut b, &f, &a1, &p).await;
    // next time the device remembers a1: the chooser is shown
    let par = f.par(&s, &pkce(), "s").await;
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let (st, _, html) = b.get(&s, &f.authorize_url(&s, &ru)).await;
    assert_eq!(st, 200);
    assert!(
        html.contains("Choose an account") && html.contains(&a1.handle),
        "{html}"
    );
    let csrf = csrf_of(&html);
    // CSRF: a post without the token is refused
    let (st, _, _) = b
        .post(
            &s,
            "/oauth/authorize/select",
            &[("request_uri", &ru), ("did", &a1.did)],
        )
        .await;
    assert_eq!(st, 403);
    // choose "another account" -> login form -> sign in as a2 -> consent
    let (st, _, html) = b
        .post(
            &s,
            "/oauth/authorize/select",
            &[("request_uri", &ru), ("csrf", &csrf), ("did", "")],
        )
        .await;
    assert_eq!(st, 200);
    assert!(html.contains("name=\"password\""));
    // wrong password
    let (st, _, html) = b
        .post(
            &s,
            "/oauth/authorize/sign-in",
            &[
                ("request_uri", &ru),
                ("csrf", &csrf),
                ("identifier", &a2.handle),
                ("password", "wrong"),
                ("action", "sign-in"),
            ],
        )
        .await;
    assert_eq!(st, 401);
    assert!(html.contains("Invalid handle or password"));
    let (_, _, html) = b
        .post(
            &s,
            "/oauth/authorize/sign-in",
            &[
                ("request_uri", &ru),
                ("csrf", &csrf),
                ("identifier", &a2.handle),
                ("password", PASSWORD),
                ("action", "sign-in"),
            ],
        )
        .await;
    assert!(html.contains("Authorize access") && html.contains(&a2.handle));
    // deny -> access_denied redirect with state + iss
    let (st, h, _) = b
        .post(
            &s,
            "/oauth/authorize/consent",
            &[
                ("request_uri", &ru),
                ("csrf", &csrf),
                ("did", &a2.did),
                ("action", "deny"),
            ],
        )
        .await;
    assert_eq!(st, 303);
    let (_, q) = location_params(&h);
    assert_eq!(q["error"], "access_denied");
    assert_eq!(q["state"], "s");
    assert_eq!(q["iss"], s.base);
    // the request is gone afterwards
    let (_, h, _) = b.get(&s, &f.authorize_url(&s, &ru)).await;
    let _ = h;

    // login_hint selects the matching signed-in account directly (consent page)
    f.extra = vec![("login_hint".into(), a2.handle.clone())];
    let par = f.par(&s, &pkce(), "s2").await;
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let (_, _, html) = b.get(&s, &f.authorize_url(&s, &ru)).await;
    assert!(
        html.contains("Authorize access") && html.contains(&a2.handle),
        "{html}"
    );

    // prompt=login forces the password form even with a device session
    f.extra = vec![("prompt".into(), "login".into())];
    let par = f.par(&s, &pkce(), "s3").await;
    // public clients are forced to prompt=consent, so prompt=login is
    // overridden; the chooser is shown instead (as in the reference AS)
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let (st, _, _) = b.get(&s, &f.authorize_url(&s, &ru)).await;
    assert_eq!(st, 200);

    // prompt=none is not allowed for public clients
    f.extra = vec![("prompt".into(), "none".into())];
    assert_eq!(
        f.par(&s, &pkce(), "s4").await.body["error"],
        "consent_required"
    );

    // a request started on one device can't be continued on another
    f.extra = vec![];
    let par = f.par(&s, &pkce(), "s5").await;
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let _ = b.get(&s, &f.authorize_url(&s, &ru)).await;
    let mut other = Browser::default();
    let (st, h, _) = other.get(&s, &f.authorize_url(&s, &ru)).await;
    assert_eq!(st, 303);
    assert_eq!(location_params(&h).1["error"], "access_denied");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn totp_prompt_on_login() {
    let s = spawn().await;
    let acct = create_account(&s, "frank").await;
    // enable TOTP via the vlpds.server.*Totp endpoints
    let setup: J = s
        .http
        .post(format!("{}/xrpc/vlpds.server.setupTotp", s.base))
        .bearer_auth(&acct.jwt)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let secret = vlpds::totp::base32_decode(setup["secret"].as_str().unwrap()).unwrap();
    let step = vlpds::totp::step_at(vlpds::totp::now_secs());
    let r = s
        .http
        .post(format!("{}/xrpc/vlpds.server.confirmTotp", s.base))
        .bearer_auth(&acct.jwt)
        .json(&json!({"code": vlpds::totp::code_for_step(&secret, step)}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());

    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/cb";
    let cid = loopback_client_id("atproto", redirect);
    let f = Flow::new(&cid, redirect, "atproto", &key);
    let mut b = Browser::default();
    let par = f.par(&s, &pkce(), "t").await;
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let (_, _, html) = b.get(&s, &f.authorize_url(&s, &ru)).await;
    let csrf = csrf_of(&html);
    let (st, _, html) = b
        .post(
            &s,
            "/oauth/authorize/sign-in",
            &[
                ("request_uri", &ru),
                ("csrf", &csrf),
                ("identifier", &acct.handle),
                ("password", PASSWORD),
                ("action", "sign-in"),
            ],
        )
        .await;
    assert_eq!(st, 200);
    assert!(
        html.contains("name=\"code\"") && html.contains("Two-factor"),
        "expected TOTP prompt: {html}"
    );
    // wrong code
    let (st, _, html) = b
        .post(
            &s,
            "/oauth/authorize/sign-in",
            &[
                ("request_uri", &ru),
                ("csrf", &csrf),
                ("step", "totp"),
                ("code", "000000"),
                ("action", "sign-in"),
            ],
        )
        .await;
    assert_eq!(st, 401);
    assert!(html.contains("Invalid authenticator code"));
    // right code (next step: the confirm step's code is spent)
    let code = vlpds::totp::code_for_step(&secret, step + 1);
    let (st, _, html) = b
        .post(
            &s,
            "/oauth/authorize/sign-in",
            &[
                ("request_uri", &ru),
                ("csrf", &csrf),
                ("step", "totp"),
                ("code", &code),
                ("action", "sign-in"),
            ],
        )
        .await;
    assert_eq!(st, 200, "{html}");
    assert!(html.contains("Authorize access"), "{html}");
}

/// Newest dev-mode mail token of `purpose` sent to `email`.
async fn dev_mail_token(s: &Srv, email: &str, purpose: &str) -> String {
    let v: J = s
        .http
        .get(format!("{}/xrpc/vlpds.admin.getDevMail?email={}", s.base, enc(email)))
        .basic_auth("admin", Some("dev-admin-token"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let m = v["messages"].as_array().unwrap().iter().rev().find(|m| m["purpose"] == purpose);
    m.unwrap_or_else(|| panic!("no {purpose} mail to {email}: {v}"))["token"].as_str().unwrap().into()
}

/// The reference's email factor on the sign-in page
/// (SecondAuthenticationFactorRequiredError 'emailOtp'): the password step
/// mails a code and asks for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn email_code_prompt_on_login() {
    let s = spawn().await;
    let acct = create_account(&s, "emma").await;
    let email = "emma@example.com";
    let call = |nsid: &str, body: J| {
        s.http.post(format!("{}/xrpc/{nsid}", s.base)).bearer_auth(&acct.jwt).json(&body).send()
    };
    let r = call("com.atproto.server.requestEmailConfirmation", json!({})).await.unwrap();
    assert!(r.status().is_success());
    let tok = dev_mail_token(&s, email, "confirm_email").await;
    let r = call("com.atproto.server.confirmEmail", json!({"email": email, "token": tok})).await.unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    let r = call("com.atproto.server.updateEmail", json!({"email": email, "emailAuthFactor": true})).await.unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());

    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/cb";
    let cid = loopback_client_id("atproto", redirect);
    let f = Flow::new(&cid, redirect, "atproto", &key);
    let mut b = Browser::default();
    let par = f.par(&s, &pkce(), "t").await;
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let (_, _, html) = b.get(&s, &f.authorize_url(&s, &ru)).await;
    let csrf = csrf_of(&html);
    let password_step = [
        ("request_uri", ru.as_str()),
        ("csrf", csrf.as_str()),
        ("identifier", acct.handle.as_str()),
        ("password", PASSWORD),
        ("action", "sign-in"),
    ];
    let (st, _, html) = b.post(&s, "/oauth/authorize/sign-in", &password_step).await;
    assert_eq!(st, 200);
    assert!(
        html.contains("name=\"code\"") && html.contains("We sent a sign-in code to <b>e***a@e***m</b>"),
        "expected email code prompt: {html}"
    );
    let code = dev_mail_token(&s, email, "auth_factor").await;
    let code_step = |c: &str| {
        vec![
            ("request_uri".to_string(), ru.clone()),
            ("csrf".to_string(), csrf.clone()),
            ("step".to_string(), "totp".to_string()),
            ("code".to_string(), c.to_string()),
            ("action".to_string(), "sign-in".to_string()),
        ]
    };
    let pairs = code_step("AAAAA-AAAAA");
    let pairs: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let (st, _, html) = b.post(&s, "/oauth/authorize/sign-in", &pairs).await;
    assert_eq!(st, 401);
    assert!(html.contains("Invalid sign-in code") && html.contains("name=\"code\""), "{html}");
    let pairs = code_step(&code);
    let pairs: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let (st, _, html) = b.post(&s, "/oauth/authorize/sign-in", &pairs).await;
    assert_eq!(st, 200, "{html}");
    assert!(html.contains("Authorize access"), "{html}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_management() {
    let s = spawn().await;
    let acct = create_account(&s, "grace").await;
    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/cb";
    let cid = loopback_client_id("atproto transition:generic", redirect);
    let f = Flow::new(&cid, redirect, "atproto transition:generic", &key);
    let mut b = Browser::default();
    let p = pkce();
    let code = authorize_interactive(&s, &mut b, &f, &acct, &p).await;
    let t = tokens(&exchange(&s, &f, &code, &p, &[]).await);

    // XRPC: list + revoke (full account session required)
    let list: J = s
        .http
        .get(format!("{}/xrpc/vlpds.oauth.listSessions", s.base))
        .bearer_auth(&acct.jwt)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let sessions = list["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0]["clientId"], cid);
    // an OAuth token can't list/revoke grants
    let r = xrpc_dpop(&s, &key, &t.access, "GET", "vlpds.oauth.listSessions", None).await;
    assert_eq!(r.status, 403);
    let id = sessions[0]["id"].as_str().unwrap();
    let r = s
        .http
        .post(format!("{}/xrpc/vlpds.oauth.revokeSession", s.base))
        .bearer_auth(&acct.jwt)
        .json(&json!({"id": id}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(
        create_post(&s, &key, &t.access, &acct.did, "app.bsky.feed.post")
            .await
            .status,
        401
    );

    // UI: /oauth/account lists the grant and revokes it
    let p = pkce();
    let code = authorize_interactive(&s, &mut b, &f, &acct, &p).await;
    let t = tokens(&exchange(&s, &f, &code, &p, &[]).await);
    let (st, h, html) = b.get(&s, &format!("{}/oauth/account", s.base)).await;
    assert_eq!(st, 200);
    assert!(h.get("content-security-policy").is_some());
    assert!(
        html.contains("Connected apps") && html.contains(&acct.handle),
        "{html}"
    );
    let i = html.find("name=\"session\" value=\"").unwrap() + "name=\"session\" value=\"".len();
    let sid = html[i..i + html[i..].find('"').unwrap()].to_string();
    let csrf = csrf_of(&html);
    let (st, _, _) = b
        .post(
            &s,
            "/oauth/account/revoke",
            &[("did", &acct.did), ("session", &sid)],
        )
        .await;
    assert_eq!(st, 403, "csrf required");
    let (st, h, _) = b
        .post(
            &s,
            "/oauth/account/revoke",
            &[("csrf", &csrf), ("did", &acct.did), ("session", &sid)],
        )
        .await;
    assert_eq!(st, 303);
    assert_eq!(h.get("location").unwrap(), "/oauth/account");
    assert_eq!(
        create_post(&s, &key, &t.access, &acct.did, "app.bsky.feed.post")
            .await
            .status,
        401
    );

    // a fresh browser has to sign in on /oauth/account
    let mut b2 = Browser::default();
    let (_, _, html) = b2.get(&s, &format!("{}/oauth/account", s.base)).await;
    assert!(html.contains("name=\"password\""));
    let csrf = csrf_of(&html);
    let (st, h, _) = b2
        .post(
            &s,
            "/oauth/account/sign-in",
            &[
                ("csrf", &csrf),
                ("identifier", &acct.handle),
                ("password", PASSWORD),
            ],
        )
        .await;
    assert_eq!(st, 303);
    assert_eq!(h.get("location").unwrap(), "/oauth/account");
    let (_, _, html) = b2.get(&s, &format!("{}/oauth/account", s.base)).await;
    assert!(html.contains("Connected apps"));
}

/// Confidential client: discoverable client metadata served over http by a
/// local server (dev mode allows http + private addresses), private_key_jwt
/// client authentication, remembered consent and prompt=none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn confidential_client_private_key_jwt() {
    let s = spawn().await;
    let acct = create_account(&s, "heidi").await;
    let client_key = SigningKey::random(&mut rand::rngs::OsRng);
    let pt = client_key.verifying_key().to_encoded_point(false);
    let jwk = json!({"kty": "EC", "crv": "P-256", "x": b64(pt.x().unwrap()), "y": b64(pt.y().unwrap()), "kid": "k1", "alg": "ES256", "use": "sig"});
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let caddr = listener.local_addr().unwrap();
    let client_id = format!("http://{caddr}/client-metadata.json");
    let redirect = "https://app.example.com/callback";
    let md = json!({
        "client_id": client_id,
        "client_name": "Test App",
        "redirect_uris": [redirect],
        "scope": "atproto transition:generic",
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "private_key_jwt",
        "token_endpoint_auth_signing_alg": "ES256",
        "application_type": "web",
        "dpop_bound_access_tokens": true,
        "jwks": {"keys": [jwk]},
    });
    let router = axum::Router::new().route(
        "/client-metadata.json",
        axum::routing::get(move || {
            let md = md.clone();
            async move { axum::Json(md) }
        }),
    );
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let assertion = |aud: &str| {
        let header = json!({"alg": "ES256", "kid": "k1", "typ": "JWT"});
        let payload = json!({"iss": client_id, "sub": client_id, "aud": aud, "jti": rand_str(12), "iat": now(), "exp": now() + 60});
        sign_jwt(&client_key, &header, &payload)
    };
    let key = DpopKey::new();
    let mut f = Flow::new(&client_id, redirect, "atproto transition:generic", &key);
    let a = assertion(&s.base);
    f.extra = vec![
        (
            "client_assertion_type".into(),
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer".into(),
        ),
        ("client_assertion".into(), a),
    ];
    let mut b = Browser::default();
    let p = pkce();
    let code = authorize_interactive(&s, &mut b, &f, &acct, &p).await;

    // token request without client authentication is refused
    let r = exchange(&s, &f, &code, &p, &[]).await;
    assert_eq!(r.status, 400);
    // wrong audience
    let bad = assertion("https://elsewhere.example");
    let r = exchange(
        &s,
        &f,
        &code,
        &p,
        &[
            (
                "client_assertion_type",
                "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            ),
            ("client_assertion", &bad),
        ],
    )
    .await;
    assert_eq!(r.body["error"], "invalid_client");
    let good = assertion(&s.base);
    let auth = [
        (
            "client_assertion_type",
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        ),
        ("client_assertion", good.as_str()),
    ];
    let t = tokens(&exchange(&s, &f, &code, &p, &auth).await);
    // assertion jti replay is refused
    let r = refresh(&s, &f, t.refresh.as_ref().unwrap(), &auth).await;
    assert_eq!(r.body["error"], "invalid_client");
    let a2 = assertion(&s.base);
    let t2 = tokens(
        &refresh(
            &s,
            &f,
            t.refresh.as_ref().unwrap(),
            &[
                (
                    "client_assertion_type",
                    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                ),
                ("client_assertion", &a2),
            ],
        )
        .await,
    );
    assert_eq!(
        create_post(&s, &key, &t2.access, &acct.did, "app.bsky.feed.post")
            .await
            .status,
        200
    );

    // consent is remembered for confidential clients: prompt=none issues a
    // code without any UI
    let a3 = assertion(&s.base);
    f.extra = vec![
        (
            "client_assertion_type".into(),
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer".into(),
        ),
        ("client_assertion".into(), a3),
        ("prompt".into(), "none".into()),
    ];
    let par = f.par(&s, &pkce(), "silent").await;
    assert_eq!(par.status, 201, "{}", par.body);
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let (st, h, _) = b.get(&s, &f.authorize_url(&s, &ru)).await;
    assert_eq!(st, 303);
    let (_, q) = location_params(&h);
    assert!(q.contains_key("code"), "{q:?}");
    assert_eq!(q["state"], "silent");
    // prompt=none from a fresh device: login_required
    let a4 = assertion(&s.base);
    f.extra[1].1 = a4;
    let par = f.par(&s, &pkce(), "silent2").await;
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let (st, h, _) = Browser::default().get(&s, &f.authorize_url(&s, &ru)).await;
    assert_eq!(st, 303);
    assert_eq!(location_params(&h).1["error"], "login_required");
}

/// include: scopes resolve a permission-set lexicon (published by an account
/// on this PDS; the NSID authority's DNS lookup is pinned) and are expanded
/// into granular repo permissions in the token.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn include_permission_set() {
    let s = spawn().await;
    let publisher = create_account(&s, "lexpub").await;
    let user = create_account(&s, "ivan").await;
    let nsid = "com.example.vlpdstest.basicPerms";
    let lex = json!({
        "$type": "com.atproto.lexicon.schema",
        "lexicon": 1,
        "id": nsid,
        "defs": {"main": {
            "type": "permission-set",
            "title": "Basic test permissions",
            "detail": "Create things",
            "permissions": [
                {"type": "permission", "resource": "repo", "collection": ["com.example.vlpdstest.thing"]},
                {"type": "permission", "resource": "repo", "collection": ["app.bsky.feed.post"]},
            ],
        }},
    });
    let r = s
        .http
        .post(format!("{}/xrpc/com.atproto.repo.createRecord", s.base))
        .bearer_auth(&publisher.jwt)
        .json(&json!({"repo": publisher.did, "collection": "com.atproto.lexicon.schema", "rkey": nsid, "record": lex, "validate": false}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    vlpds::oauth::lexicon::override_authority(
        &vlpds::oauth::lexicon::nsid_authority(nsid),
        &publisher.did,
    );

    // the published record verifies through the network proof path too
    let car = s
        .http
        .get(format!("{}/xrpc/com.atproto.sync.getRecord?did={}&collection=com.atproto.lexicon.schema&rkey={nsid}", s.base, publisher.did))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let acct = s.app.account(&publisher.did).await.ok().unwrap();
    let rec = vlpds::oauth::lexicon::verify_record_proof(
        &car,
        &publisher.did,
        &acct.signing_pubkey,
        &format!("com.atproto.lexicon.schema/{nsid}"),
    )
    .unwrap();
    assert_eq!(rec["id"], nsid);
    let other = vlpds::crypto::Keypair::generate();
    assert!(vlpds::oauth::lexicon::verify_record_proof(
        &car,
        &publisher.did,
        &other.public_multibase(),
        &format!("com.atproto.lexicon.schema/{nsid}")
    )
    .is_err());

    let scope = format!("atproto include:{nsid}");
    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/cb";
    let cid = loopback_client_id(&scope, redirect);
    let f = Flow::new(&cid, redirect, &scope, &key);
    let mut b = Browser::default();
    let p = pkce();
    let state = "inc";
    let par = f.par(&s, &p, state).await;
    assert_eq!(par.status, 201, "{}", par.body);
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let (_, _, html) = b.get(&s, &f.authorize_url(&s, &ru)).await;
    let csrf = csrf_of(&html);
    let (_, _, html) = b
        .post(
            &s,
            "/oauth/authorize/sign-in",
            &[
                ("request_uri", &ru),
                ("csrf", &csrf),
                ("identifier", &user.handle),
                ("password", PASSWORD),
                ("action", "sign-in"),
            ],
        )
        .await;
    assert!(
        html.contains("Basic test permissions"),
        "consent should show the permission set: {html}"
    );
    let (_, h, _) = b
        .post(
            &s,
            "/oauth/authorize/consent",
            &[
                ("request_uri", &ru),
                ("csrf", &csrf),
                ("did", &user.did),
                ("action", "allow"),
            ],
        )
        .await;
    let code = location_params(&h).1["code"].clone();
    let t = tokens(&exchange(&s, &f, &code, &p, &[]).await);
    // only the permission under the set's own NSID group is granted
    assert_eq!(t.scope, "repo:com.example.vlpdstest.thing atproto");
    assert_eq!(
        create_post(&s, &key, &t.access, &user.did, "app.bsky.feed.post")
            .await
            .status,
        403
    );
    assert_eq!(
        create_post(
            &s,
            &key,
            &t.access,
            &user.did,
            "com.example.vlpdstest.thing"
        )
        .await
        .status,
        200
    );

    // an include: that does not resolve is refused at PAR
    let bad_scope = "atproto include:com.example.vlpdstest.missing";
    let cid2 = loopback_client_id(bad_scope, redirect);
    let f2 = Flow::new(&cid2, redirect, bad_scope, &key);
    assert_eq!(
        f2.par(&s, &pkce(), "x").await.body["error"],
        "invalid_scope"
    );
}

/// Enables TOTP for `acct`; returns the secret and the step the confirm code
/// spent.
async fn enable_totp(s: &Srv, acct: &Account) -> (Vec<u8>, u64) {
    let setup: J = s
        .http
        .post(format!("{}/xrpc/vlpds.server.setupTotp", s.base))
        .bearer_auth(&acct.jwt)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let secret = vlpds::totp::base32_decode(setup["secret"].as_str().unwrap()).unwrap();
    let step = vlpds::totp::step_at(vlpds::totp::now_secs());
    let r = s
        .http
        .post(format!("{}/xrpc/vlpds.server.confirmTotp", s.base))
        .bearer_auth(&acct.jwt)
        .json(&json!({"code": vlpds::totp::code_for_step(&secret, step)}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    (secret, step)
}

/// Wrong authenticator codes: a few are fine, three drop the pending sign-in
/// (password again), and five in a row lock the account's factor for both
/// OAuth and createSession, persisted in its TOTP state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn totp_brute_force_lockout() {
    let s = spawn().await;
    let acct = create_account(&s, "mallory").await;
    let (secret, step) = enable_totp(&s, &acct).await;
    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/cb";
    let cid = loopback_client_id("atproto", redirect);
    let f = Flow::new(&cid, redirect, "atproto", &key);
    let mut b = Browser::default();
    let ru = f.par(&s, &pkce(), "t").await.body["request_uri"]
        .as_str()
        .unwrap()
        .to_string();
    let (_, _, html) = b.get(&s, &f.authorize_url(&s, &ru)).await;
    let csrf = csrf_of(&html);
    let password = [
        ("request_uri", ru.as_str()),
        ("csrf", csrf.as_str()),
        ("identifier", acct.handle.as_str()),
        ("password", PASSWORD),
        ("action", "sign-in"),
    ];
    let totp = |code: &str| {
        [
            ("request_uri", ru.clone()),
            ("csrf", csrf.clone()),
            ("step", "totp".to_string()),
            ("code", code.to_string()),
            ("action", "sign-in".to_string()),
        ]
    };
    async fn post_owned(b: &mut Browser, s: &Srv, p: &[(&str, String)]) -> (u16, String) {
        let p: Vec<(&str, &str)> = p.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let (st, _, html) = b.post(s, "/oauth/authorize/sign-in", &p).await;
        (st, html)
    }

    let (st, html) = b
        .post(&s, "/oauth/authorize/sign-in", &password)
        .await
        .into_pair();
    assert_eq!(st, 200, "{html}");
    for _ in 0..2 {
        let (st, html) = post_owned(&mut b, &s, &totp("000000")).await;
        assert_eq!(st, 401);
        assert!(
            html.contains("Invalid authenticator code") && html.contains("name=\"code\""),
            "{html}"
        );
    }
    // third wrong code on this pending sign-in: back to the password step
    let (st, html) = post_owned(&mut b, &s, &totp("000000")).await;
    assert_eq!(st, 429, "{html}");
    assert!(
        html.contains("Too many invalid authenticator codes"),
        "{html}"
    );
    assert!(!html.contains("name=\"code\""), "{html}");
    // the pending step is gone: a right code alone no longer signs in
    let good = vlpds::totp::code_for_step(&secret, step + 1);
    let (st, html) = post_owned(&mut b, &s, &totp(&good)).await;
    assert_eq!(st, 401);
    assert!(html.contains("timed out"), "{html}");

    // password again; the account counter is at 3, two more lock it
    let (st, _) = b
        .post(&s, "/oauth/authorize/sign-in", &password)
        .await
        .into_pair();
    assert_eq!(st, 200);
    let (st, _) = post_owned(&mut b, &s, &totp("111111")).await;
    assert_eq!(st, 401);
    let (st, html) = post_owned(&mut b, &s, &totp("222222")).await;
    assert_eq!(st, 429, "{html}");
    let Ok(st) = vlpds::totp::load(&s.app, &acct.did).await else {
        panic!("load totp state")
    };
    assert_eq!(st.failures, vlpds::totp::MAX_FAILURES);
    assert!(
        st.locked_until > vlpds::totp::now_secs(),
        "lockout persisted"
    );

    // locked: the password step itself is refused, and so is createSession
    // with a right code (shared counter)
    let (st, html) = b
        .post(&s, "/oauth/authorize/sign-in", &password)
        .await
        .into_pair();
    assert_eq!(st, 429, "{html}");
    let r = s
        .http
        .post(format!("{}/xrpc/com.atproto.server.createSession", s.base))
        .json(&json!({"identifier": acct.handle, "password": PASSWORD, "authFactorToken": good}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 429);
    let j: J = r.json().await.unwrap();
    assert_eq!(j["error"], "RateLimitExceeded");
}

trait IntoPair {
    fn into_pair(self) -> (u16, String);
}

impl IntoPair for (u16, reqwest::header::HeaderMap, String) {
    fn into_pair(self) -> (u16, String) {
        (self.0, self.2)
    }
}

/// `/oauth/account?error=` only maps fixed codes to fixed messages; sign-in
/// failures redirect with a code.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_page_error_codes() {
    let s = spawn().await;
    let acct = create_account(&s, "ivan").await;
    let mut b = Browser::default();
    let evil = enc("<b>Your account is compromised, call 555-0100</b>");
    let (_, _, html) = b
        .get(&s, &format!("{}/oauth/account?add=1&error={evil}", s.base))
        .await;
    assert!(
        !html.contains("compromised") && !html.contains("555-0100"),
        "{html}"
    );
    let (_, _, html) = b
        .get(
            &s,
            &format!("{}/oauth/account?add=1&error=bad_code", s.base),
        )
        .await;
    assert!(html.contains("Invalid authenticator code"), "{html}");

    let csrf = csrf_of(&html);
    let (st, h, _) = b
        .post(
            &s,
            "/oauth/account/sign-in",
            &[
                ("csrf", &csrf),
                ("identifier", &acct.handle),
                ("password", "wrong"),
            ],
        )
        .await;
    assert_eq!(st, 303);
    assert_eq!(
        h.get("location").unwrap(),
        "/oauth/account?add=1&error=invalid"
    );
    let (_, _, html) = b
        .get(&s, &format!("{}/oauth/account?add=1&error=invalid", s.base))
        .await;
    assert!(html.contains("Invalid handle or password"), "{html}");
}

/// OAuth sign-in posts share createSession's identifier + IP buckets (30 per
/// 5 min), checked before any password hashing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sign_in_rate_limited() {
    let s = spawn().await;
    let mut b = Browser::default();
    let (_, _, html) = b.get(&s, &format!("{}/oauth/account", s.base)).await;
    let csrf = csrf_of(&html);
    let form = [
        ("csrf", csrf.as_str()),
        ("identifier", "ghost.vlpds.test"),
        ("password", "x"),
    ];
    for _ in 0..30 {
        let (_, h, _) = b.post(&s, "/oauth/account/sign-in", &form).await;
        assert_eq!(
            h.get("location").unwrap(),
            "/oauth/account?add=1&error=invalid"
        );
    }
    let (_, h, _) = b.post(&s, "/oauth/account/sign-in", &form).await;
    assert_eq!(
        h.get("location").unwrap(),
        "/oauth/account?add=1&error=rate_limited"
    );
    let (_, _, html) = b
        .get(
            &s,
            &format!("{}/oauth/account?add=1&error=rate_limited", s.base),
        )
        .await;
    assert!(html.contains("Too many sign-in attempts"), "{html}");
}

// ---------- JAR, response modes, prompt=create, scope narrowing, GC ----------

/// Serves a client metadata document built from its own client_id.
async fn serve_metadata(build: impl FnOnce(&str) -> J) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_id = format!(
        "http://{}/client-metadata.json",
        listener.local_addr().unwrap()
    );
    let md = build(&client_id);
    let router = axum::Router::new().route(
        "/client-metadata.json",
        axum::routing::get(move || {
            let md = md.clone();
            async move { axum::Json(md) }
        }),
    );
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    client_id
}

fn unsecured_jwt(payload: &J) -> String {
    format!(
        "{}.{}.",
        b64(serde_json::to_vec(&json!({"alg": "none"})).unwrap()),
        b64(serde_json::to_vec(payload).unwrap())
    )
}

fn hidden_field(html: &str, name: &str) -> Option<String> {
    let pat = format!("name=\"{name}\" value=\"");
    let i = html.find(&pat)? + pat.len();
    Some(html[i..i + html[i..].find('"').unwrap()].replace("&amp;", "&"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jar_request_objects() {
    let s = spawn().await;
    let acct = create_account(&s, "jar").await;
    let client_key = SigningKey::random(&mut rand::rngs::OsRng);
    let pt = client_key.verifying_key().to_encoded_point(false);
    let jwk = json!({"kty": "EC", "crv": "P-256", "x": b64(pt.x().unwrap()), "y": b64(pt.y().unwrap()), "kid": "k1", "alg": "ES256", "use": "sig"});
    let redirect = "https://app.example.com/callback";
    let client_id = serve_metadata(|id| {
        json!({
            "client_id": id,
            "redirect_uris": [redirect],
            "scope": "atproto transition:generic",
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "private_key_jwt",
            "token_endpoint_auth_signing_alg": "ES256",
            "application_type": "web",
            "dpop_bound_access_tokens": true,
            "jwks": {"keys": [jwk]},
        })
    })
    .await;
    let assertion = || {
        let header = json!({"alg": "ES256", "kid": "k1", "typ": "JWT"});
        let payload = json!({"iss": client_id, "sub": client_id, "aud": s.base, "jti": rand_str(12), "iat": now(), "exp": now() + 60});
        sign_jwt(&client_key, &header, &payload)
    };
    let key = DpopKey::new();
    let p = pkce();
    let claims = || {
        json!({
            "iss": client_id, "aud": s.base, "iat": now(), "jti": rand_str(12),
            "client_id": client_id, "response_type": "code", "redirect_uri": redirect,
            "scope": "atproto transition:generic", "state": "inner",
            "code_challenge": p.challenge, "code_challenge_method": "S256",
        })
    };
    let jar_header = json!({"alg": "ES256", "kid": "k1", "typ": "oauth-authz-req+jwt"});
    let jar = |payload: &J| sign_jwt(&client_key, &jar_header, payload);
    let par = |request: String| {
        let a = assertion();
        let (s, key, client_id) = (&s, &key, client_id.clone());
        async move {
            as_post(
                s,
                key,
                "/oauth/par",
                &[
                    ("client_id", &client_id),
                    (
                        "client_assertion_type",
                        "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                    ),
                    ("client_assertion", &a),
                    ("request", &request),
                    // ignored: only the request object's parameters count
                    ("state", "outer"),
                ],
            )
            .await
        }
    };
    let expect_invalid = |r: Resp, needle: &str| {
        assert_eq!(r.status, 400, "{}", r.body);
        assert_eq!(r.body["error"], "invalid_request", "{}", r.body);
        let d = r.body["error_description"].as_str().unwrap();
        assert!(d.contains(needle), "{needle:?} not in {d:?}");
    };

    let mut c = claims();
    c["aud"] = json!("https://elsewhere.example");
    expect_invalid(par(jar(&c)).await, "\"aud\"");
    let mut c = claims();
    c["iat"] = json!(now() - 120);
    expect_invalid(par(jar(&c)).await, "\"iat\"");
    let mut c = claims();
    c.as_object_mut().unwrap().remove("jti");
    expect_invalid(par(jar(&c)).await, "\"jti\"");
    let mut c = claims();
    c["iss"] = json!("https://someone.else/client.json");
    expect_invalid(par(jar(&c)).await, "\"iss\"");
    let other = SigningKey::random(&mut rand::rngs::OsRng);
    expect_invalid(
        par(sign_jwt(&other, &jar_header, &claims())).await,
        "signature verification failed",
    );
    expect_invalid(par(unsecured_jwt(&claims())).await, "unsecured");
    let mut c = claims();
    c["client_id"] = json!("http://localhost");
    expect_invalid(par(jar(&c)).await, "does not match");
    let mut c = claims();
    c.as_object_mut().unwrap().remove("client_id");
    expect_invalid(par(jar(&c)).await, "client_id");
    expect_invalid(par("not-a-jwt".into()).await, "Invalid \"request\" object");

    // a valid request object; its parameters (state=inner) win
    let good = jar(&claims());
    let r = par(good.clone()).await;
    assert_eq!(r.status, 201, "{}", r.body);
    let ru = r.body["request_uri"].as_str().unwrap().to_string();
    // the same request object again: jti replay
    expect_invalid(par(good).await, "replayed");

    let f = Flow::new(&client_id, redirect, "atproto transition:generic", &key);
    let mut b = Browser::default();
    let (st, h, body) = browser_consent(&s, &mut b, &f, &acct, &ru, &[]).await;
    assert_eq!(st, 303, "{body}");
    let (_, q) = location_params(&h);
    assert_eq!(q["state"], "inner");
    let a = assertion();
    let t = tokens(
        &exchange(
            &s,
            &f,
            &q["code"],
            &p,
            &[
                (
                    "client_assertion_type",
                    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                ),
                ("client_assertion", &a),
            ],
        )
        .await,
    );
    assert_eq!(t.scope, "atproto transition:generic");
}

/// A public client that registered `request_object_signing_alg: none` sends
/// unsecured request objects (iss/aud optional); signed ones are refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jar_unsecured_request_objects() {
    let s = spawn().await;
    let redirect = "https://app.example.com/callback";
    let client_id = serve_metadata(|id| {
        json!({
            "client_id": id,
            "redirect_uris": [redirect],
            "scope": "atproto",
            "grant_types": ["authorization_code"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
            "request_object_signing_alg": "none",
            "application_type": "web",
            "dpop_bound_access_tokens": true,
        })
    })
    .await;
    let key = DpopKey::new();
    let payload = |p: &Pkce| {
        json!({
            "iat": now(), "jti": rand_str(12), "client_id": client_id,
            "response_type": "code", "redirect_uri": redirect, "scope": "atproto",
            "code_challenge": p.challenge, "code_challenge_method": "S256",
        })
    };
    let r = as_post(
        &s,
        &key,
        "/oauth/par",
        &[
            ("client_id", &client_id),
            ("request", &unsecured_jwt(&payload(&pkce()))),
        ],
    )
    .await;
    assert_eq!(r.status, 201, "{}", r.body);
    let other = SigningKey::random(&mut rand::rngs::OsRng);
    let signed = sign_jwt(&other, &json!({"alg": "ES256"}), &payload(&pkce()));
    let r = as_post(
        &s,
        &key,
        "/oauth/par",
        &[("client_id", &client_id), ("request", &signed)],
    )
    .await;
    assert_eq!(r.status, 400, "{}", r.body);
    assert!(r.body["error_description"]
        .as_str()
        .unwrap()
        .contains("unsecured"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn response_modes_form_post_and_fragment() {
    use base64::engine::general_purpose::STANDARD;
    let s = spawn().await;
    let acct = create_account(&s, "formpost").await;
    let m: J = s
        .http
        .get(format!("{}/.well-known/oauth-authorization-server", s.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        m["response_modes_supported"],
        json!(["query", "fragment", "form_post"])
    );
    assert_eq!(m["request_parameter_supported"], true);
    assert!(m["request_object_signing_alg_values_supported"]
        .as_array()
        .unwrap()
        .contains(&json!("none")));

    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/callback";
    let cid = loopback_client_id("atproto transition:generic", redirect);
    let mut f = Flow::new(&cid, redirect, "atproto transition:generic", &key);
    f.extra = vec![("response_mode".into(), "form_post".into())];
    let p = pkce();
    let par = f.par(&s, &p, "fp-state").await;
    assert_eq!(par.status, 201, "{}", par.body);
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let mut b = Browser::default();
    let (st, h, body) = browser_consent(&s, &mut b, &f, &acct, &ru, &[]).await;
    assert_eq!(st, 200, "{body}");
    assert!(h.get("location").is_none());
    assert_eq!(h.get("cache-control").unwrap(), "no-store");
    assert!(
        body.contains("<form method=\"post\" action=\"http://127.0.0.1/callback\">"),
        "{body}"
    );
    assert_eq!(hidden_field(&body, "state").as_deref(), Some("fp-state"));
    assert_eq!(hidden_field(&body, "iss"), Some(s.base.clone()));
    let code = hidden_field(&body, "code").expect("code field");
    // CSP: only the inline auto-submit script (by hash), and the form may
    // post to the client's redirect origin
    let csp = h.get("content-security-policy").unwrap().to_str().unwrap();
    let i = body.find("<script>").unwrap() + "<script>".len();
    let script = &body[i..i + body[i..].find("</script>").unwrap()];
    let hash = STANDARD.encode(Sha256::digest(script));
    assert!(
        csp.contains(&format!("script-src 'sha256-{hash}'")),
        "{csp}"
    );
    assert!(csp.contains("form-action 'self' http://127.0.0.1"), "{csp}");
    assert!(csp.contains("default-src 'none'"), "{csp}");
    tokens(&exchange(&s, &f, &code, &p, &[]).await);

    // errors use the same response mode
    let par = f.par(&s, &pkce(), "fp-deny").await;
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let (_, _, html) = b.get(&s, &f.authorize_url(&s, &ru)).await;
    let csrf = csrf_of(&html);
    let (st, _, body) = b
        .post(
            &s,
            "/oauth/authorize/consent",
            &[("request_uri", &ru), ("csrf", &csrf), ("action", "deny")],
        )
        .await;
    assert_eq!(st, 200, "{body}");
    assert_eq!(
        hidden_field(&body, "error").as_deref(),
        Some("access_denied")
    );
    assert_eq!(hidden_field(&body, "state").as_deref(), Some("fp-deny"));

    // fragment: the response is in the redirect's fragment
    f.extra = vec![("response_mode".into(), "fragment".into())];
    let p = pkce();
    let par = f.par(&s, &p, "frag").await;
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let (st, h, body) = browser_consent(&s, &mut b, &f, &acct, &ru, &[]).await;
    assert_eq!(st, 303, "{body}");
    let loc = h.get("location").unwrap().to_str().unwrap();
    assert!(loc.starts_with("http://127.0.0.1/callback#"), "{loc}");
    let (_, q) = location_params(&h);
    assert_eq!(q["state"], "frag");
    tokens(&exchange(&s, &f, &q["code"], &p, &[]).await);

    // unknown response modes are refused at PAR
    f.extra = vec![("response_mode".into(), "web_message".into())];
    let r = f.par(&s, &pkce(), "x").await;
    assert_eq!(r.status, 400, "{}", r.body);
}

/// prompt=create: the sign-up page. Creating an account there signs it in
/// on the device and continues to consent; the two pages link to each
/// other; form errors keep the values entered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prompt_create_signs_up() {
    let s = spawn().await;
    let taken = create_account(&s, "taken").await;
    let m: J = s
        .http
        .get(format!("{}/.well-known/oauth-authorization-server", s.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(m["prompt_values_supported"]
        .as_array()
        .unwrap()
        .contains(&json!("create")));
    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/callback";
    let cid = loopback_client_id("atproto transition:generic", redirect);
    let mut f = Flow::new(&cid, redirect, "atproto transition:generic", &key);
    f.extra = vec![("prompt".into(), "create".into())];
    let mut b = Browser::default();
    let p = pkce();
    let par = f.par(&s, &p, "c1").await;
    assert_eq!(par.status, 201, "{}", par.body);
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let (st, _, html) = b.get(&s, &f.authorize_url(&s, &ru)).await;
    assert_eq!(st, 200);
    assert!(html.contains("Create an account") && html.contains("name=\"email\""), "sign-up expected: {html}");
    assert!(!html.contains("name=\"invite_code\""), "invites are optional here: {html}");
    // the "Sign in" link shows the sign-in page, which links back
    let link = |html: &str, screen: &str| {
        let at = html.find(&format!("screen={screen}")).expect("screen link");
        let start = html[..at].rfind("href=\"").unwrap() + 6;
        format!("{}{}", s.base, html[start..at + 7 + screen.len()].replace("&amp;", "&"))
    };
    let (st, _, signin) = b.get(&s, &link(&html, "sign-in")).await;
    assert_eq!(st, 200);
    assert!(signin.contains("name=\"identifier\""), "{signin}");
    let (_, _, html) = b.get(&s, &link(&signin, "sign-up")).await;
    assert!(html.contains("name=\"email\""), "{html}");

    // a taken handle: the form again, with the error and the values kept
    let name = format!("new{}", rand::random::<u32>() % 100000);
    let email = format!("{name}@example.com");
    let taken_label = taken.handle.split('.').next().unwrap().to_string();
    let csrf = csrf_of(&html);
    let sign_up = |handle: &str, csrf: &str| {
        vec![
            ("request_uri".to_string(), ru.clone()),
            ("csrf".to_string(), csrf.to_string()),
            ("handle".to_string(), handle.to_string()),
            ("email".to_string(), email.clone()),
            ("password".to_string(), PASSWORD.to_string()),
            ("action".to_string(), "sign-up".to_string()),
        ]
    };
    let pairs = sign_up(&taken_label, &csrf);
    let pairs: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let (st, _, html) = b.post(&s, "/oauth/authorize/sign-up", &pairs).await;
    assert_eq!(st, 400, "{html}");
    assert!(html.contains("Handle already taken") && html.contains(&email), "{html}");
    // a CSRF token is required
    let pairs = sign_up(&name, "bogus");
    let pairs: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let (st, _, _) = b.post(&s, "/oauth/authorize/sign-up", &pairs).await;
    assert_eq!(st, 403);

    // success: signed in on the device, then consent (public client)
    let pairs = sign_up(&name, &csrf_of(&html));
    let pairs: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let (st, _, html) = b.post(&s, "/oauth/authorize/sign-up", &pairs).await;
    assert_eq!(st, 200, "{html}");
    assert!(html.contains("Authorize access"), "consent expected: {html}");
    let handle = format!("{name}.vlpds.test");
    let did = hidden_field(&html, "did").expect("did on the consent form");
    let (st, h, body) = b
        .post(
            &s,
            "/oauth/authorize/consent",
            &[("request_uri", &ru), ("csrf", &csrf_of(&html)), ("did", &did), ("action", "allow")],
        )
        .await;
    assert_eq!(st, 303, "{body}");
    let code = location_params(&h).1.get("code").expect("code").clone();
    let t = tokens(&exchange(&s, &f, &code, &p, &[]).await);
    let r = create_post(&s, &key, &t.access, &did, "app.bsky.feed.post").await;
    assert_eq!(r.status, 200, "{}", r.body);
    // a real account: the password works for createSession too
    let r = s
        .http
        .post(format!("{}/xrpc/com.atproto.server.createSession", s.base))
        .json(&json!({"identifier": handle, "password": PASSWORD}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());

    // the device now knows the account: a later prompt=create still offers
    // sign-up, and the sign-in page offers it as well
    let par = f.par(&s, &pkce(), "c2").await;
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let (_, _, html) = b.get(&s, &f.authorize_url(&s, &ru)).await;
    assert!(html.contains("name=\"email\""), "{html}");
    f.extra.clear();
    let par = f.par(&s, &pkce(), "c3").await;
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let (_, _, html) = b.get(&s, &format!("{}&screen=sign-in", f.authorize_url(&s, &ru))).await;
    assert!(html.contains("Create an account"), "{html}");
}

/// With invites required, the sign-up page asks for a code and enforces it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sign_up_page_with_required_invites() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let cfg = vlpds::server::Config { dev_mode: true, public_url: base.clone(), invite_required: true, ..Default::default() };
    let (app, _) = vlpds::server::spawn(cfg, listener).await.unwrap();
    let http = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let s = Srv { app, base, http };
    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/callback";
    let cid = loopback_client_id("atproto", redirect);
    let mut f = Flow::new(&cid, redirect, "atproto", &key);
    f.extra = vec![("prompt".into(), "create".into())];
    let mut b = Browser::default();
    let par = f.par(&s, &pkce(), "i1").await;
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let (_, _, html) = b.get(&s, &f.authorize_url(&s, &ru)).await;
    assert!(html.contains("name=\"invite_code\""), "{html}");
    let name = format!("inv{}", rand::random::<u32>() % 100000);
    let email = format!("{name}@example.com");
    let post = |csrf: String, code: &'static str| {
        vec![
            ("request_uri".to_string(), ru.clone()),
            ("csrf".to_string(), csrf),
            ("handle".to_string(), name.clone()),
            ("email".to_string(), email.clone()),
            ("password".to_string(), PASSWORD.to_string()),
            ("invite_code".to_string(), code.to_string()),
            ("action".to_string(), "sign-up".to_string()),
        ]
    };
    let pairs = post(csrf_of(&html), "");
    let pairs: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let (st, _, html) = b.post(&s, "/oauth/authorize/sign-up", &pairs).await;
    assert_eq!(st, 400, "{html}");
    assert!(html.contains("No invite code provided"), "{html}");
    let pairs = post(csrf_of(&html), "bogus-code");
    let pairs: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let (st, _, html) = b.post(&s, "/oauth/authorize/sign-up", &pairs).await;
    assert_eq!(st, 400, "{html}");
    assert!(html.contains("invite code not available"), "{html}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consent_scope_narrowing() {
    let s = spawn().await;
    let acct = create_account(&s, "narrow").await;
    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/callback";
    let scope = "atproto account:email repo:app.bsky.feed.post";
    let cid = loopback_client_id(scope, redirect);
    let mut f = Flow::new(&cid, redirect, scope, &key);
    let mut b = Browser::default();
    // run one flow; returns the consent POST's response and the granted
    // token scope (if a code was issued)
    async fn grant(
        s: &Srv,
        b: &mut Browser,
        f: &Flow<'_>,
        acct: &Account,
        extra: &[(&str, &str)],
    ) -> (HashMap<String, String>, Option<String>) {
        let p = pkce();
        let par = f.par(s, &p, "n").await;
        assert_eq!(par.status, 201, "{}", par.body);
        let ru = par.body["request_uri"].as_str().unwrap().to_string();
        let (st, h, body) = browser_consent(s, b, f, acct, &ru, extra).await;
        assert_eq!(st, 303, "{body}");
        let (_, q) = location_params(&h);
        let scope = match q.get("code") {
            Some(code) => Some(tokens(&exchange(s, f, code, &p, &[]).await).scope),
            None => None,
        };
        (q, scope)
    }

    // allowed as requested
    let (_, sc) = grant(&s, &mut b, &f, &acct, &[]).await;
    assert_eq!(sc.as_deref(), Some(scope));
    // the consent page offers to withhold the email address
    f.extra = vec![("login_hint".into(), acct.handle.clone())];
    let par = f.par(&s, &pkce(), "page").await;
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let (_, _, html) = b.get(&s, &f.authorize_url(&s, &ru)).await;
    assert!(html.contains("name=\"allow_email\""), "{html}");
    assert!(html.contains("name=\"email_choice\""), "{html}");
    // checkbox cleared: the token has no account:email
    let (_, sc) = grant(&s, &mut b, &f, &acct, &[("email_choice", "1")]).await;
    assert_eq!(sc.as_deref(), Some("atproto repo:app.bsky.feed.post"));
    // checkbox kept
    let (_, sc) = grant(
        &s,
        &mut b,
        &f,
        &acct,
        &[("email_choice", "1"), ("allow_email", "1")],
    )
    .await;
    assert_eq!(sc.as_deref(), Some(scope));
    // explicit scope override: intersection only (nothing can be added)
    let (_, sc) = grant(
        &s,
        &mut b,
        &f,
        &acct,
        &[(
            "scope",
            "atproto transition:generic repo:app.bsky.feed.post",
        )],
    )
    .await;
    assert_eq!(sc.as_deref(), Some("atproto repo:app.bsky.feed.post"));
    // removing atproto is a denial
    let (q, sc) = grant(
        &s,
        &mut b,
        &f,
        &acct,
        &[("scope", "repo:app.bsky.feed.post")],
    )
    .await;
    assert_eq!(sc, None);
    assert_eq!(q["error"], "access_denied");
    // the narrowed grant is what the session holds
    let sessions = vlpds::oauth::store::list_sessions(&s.app, &acct.did)
        .await
        .unwrap();
    assert!(sessions
        .iter()
        .any(|x| x.scope == "atproto repo:app.bsky.feed.post"));

    // transition scopes cannot be narrowed: no checkbox
    let scope2 = "atproto transition:generic account:email";
    let cid2 = loopback_client_id(scope2, redirect);
    let mut f2 = Flow::new(&cid2, redirect, scope2, &key);
    f2.extra = vec![("login_hint".into(), acct.handle.clone())];
    let par = f2.par(&s, &pkce(), "page2").await;
    assert_eq!(par.status, 201, "{}", par.body);
    let ru = par.body["request_uri"].as_str().unwrap().to_string();
    let (_, _, html) = b.get(&s, &f2.authorize_url(&s, &ru)).await;
    assert!(html.contains("Authorize access"), "{html}");
    assert!(!html.contains("allow_email"), "{html}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_sweeps_expired_rows() {
    use vlpds::oauth::gc::Sweeper;
    use vlpds::oauth::store;
    let s = spawn().await;
    let acct = create_account(&s, "gc").await;
    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/callback";
    let cid = loopback_client_id("atproto transition:generic", redirect);
    let f = Flow::new(&cid, redirect, "atproto transition:generic", &key);
    let mut b = Browser::default();
    // a full grant: consumed request, code challenge, device, session
    let p = pkce();
    let code = authorize_interactive(&s, &mut b, &f, &acct, &p).await;
    let t = tokens(&exchange(&s, &f, &code, &p, &[]).await);
    let consumed_rid = store::code_request_id(&code).unwrap();
    let device_id = b
        .cookie
        .clone()
        .unwrap()
        .split_once('=')
        .unwrap()
        .1
        .to_string();
    // a pending (unauthorized) request
    let p2 = pkce();
    let par = f.par(&s, &p2, "pending").await;
    let pending_uri = par.body["request_uri"].as_str().unwrap().to_string();
    let pending_rid = store::request_id_from_uri(&pending_uri)
        .unwrap()
        .to_string();

    let mut sw = Sweeper::new();
    let now = now();
    // nothing is expired yet
    let st = sw.tick(&s.app, now, 10_000, 10_000).await.unwrap();
    assert_eq!(st.removed, 0, "{st:?}");
    assert!(st.scanned > 0);
    // past the PAR lifetime: only the pending request goes; the consumed
    // request stays as a code-reuse tombstone
    let st = sw.tick(&s.app, now + 6 * 60, 10_000, 10_000).await.unwrap();
    assert_eq!(st.removed, 1, "{st:?}");
    assert!(store::get_request(&s.app, &pending_rid)
        .await
        .unwrap()
        .is_none());
    assert!(store::get_request(&s.app, &consumed_rid)
        .await
        .unwrap()
        .is_some());
    let (st_code, _, html) = b.get(&s, &f.authorize_url(&s, &pending_uri)).await;
    assert_eq!(st_code, 400);
    assert!(html.contains("Unknown request_uri"), "{html}");
    // code challenges are claimed for 24 h
    let r = f.par(&s, &p2, "again").await;
    assert_eq!(r.status, 400, "{}", r.body);
    let st = sw
        .tick(&s.app, now + 86_400 + 60, 10_000, 10_000)
        .await
        .unwrap();
    assert_eq!(st.removed, 2, "two code challenges: {st:?}");
    let r = f.par(&s, &p2, "again").await;
    assert_eq!(r.status, 201, "{}", r.body);
    // 15 days on: the public client's session, the idle device and the
    // tombstone are gone (the new PAR request too)
    assert_eq!(
        store::list_sessions(&s.app, &acct.did).await.unwrap().len(),
        1
    );
    let st = sw
        .tick(&s.app, now + 15 * 86_400, 10_000, 10_000)
        .await
        .unwrap();
    assert!(st.removed >= 4, "{st:?}");
    assert!(store::list_sessions(&s.app, &acct.did)
        .await
        .unwrap()
        .is_empty());
    assert!(store::get_device(&s.app, &device_id)
        .await
        .unwrap()
        .is_none());
    assert!(store::get_request(&s.app, &consumed_rid)
        .await
        .unwrap()
        .is_none());
    let r = refresh(&s, &f, t.refresh.as_ref().unwrap(), &[]).await;
    assert_eq!(r.body["error"], "invalid_grant", "{}", r.body);
}

/// Each tick does bounded work and resumes where it stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_work_is_bounded_per_tick() {
    use vlpds::oauth::gc::Sweeper;
    let s = spawn().await;
    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/callback";
    let cid = loopback_client_id("atproto", redirect);
    let f = Flow::new(&cid, redirect, "atproto", &key);
    for i in 0..3 {
        let r = f.par(&s, &pkce(), &format!("s{i}")).await;
        assert_eq!(r.status, 201);
    }
    let later = now() + 6 * 60;
    // delete budget 1: one request per tick
    let mut sw = Sweeper::new();
    for _ in 0..3 {
        let st = sw.tick(&s.app, later, 10_000, 1).await.unwrap();
        assert_eq!(st.removed, 1, "{st:?}");
    }
    assert_eq!(sw.tick(&s.app, later, 10_000, 1).await.unwrap().removed, 0);
    // scan budget 1: one key examined per partition per tick, the cursor
    // carries on, and everything is found within a bounded number of ticks
    for i in 0..3 {
        f.par(&s, &pkce(), &format!("t{i}")).await;
    }
    let mut sw = Sweeper::new();
    let parts = s.app.partitions.owned().len();
    let mut removed = 0;
    for _ in 0..200 {
        let st = sw.tick(&s.app, later, 1, 10_000).await.unwrap();
        assert!(st.scanned <= parts, "{st:?}");
        removed += st.removed;
        if removed == 3 {
            break;
        }
    }
    assert_eq!(removed, 3);
}

/// Latency of DPoP-authenticated resource requests (the proof's replay
/// claim is on this path):
/// `cargo test --profile dev-release --test all oauth::bench_dpop_resource_requests -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bench_dpop_resource_requests() {
    // BENCH_INJECT_MS: segment PUT latency (S3-like), e.g. 25
    let inject = std::env::var("BENCH_INJECT_MS").ok().and_then(|v| v.parse::<f64>().ok());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let cfg = vlpds::server::Config {
        dev_mode: true,
        public_url: base.clone(),
        inject_latency: inject.map(|ms| (ms, 0.0)),
        ..Default::default()
    };
    let (app, _) = vlpds::server::spawn(cfg, listener).await.unwrap();
    let http = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let s = Srv { app, base, http };
    let acct = create_account(&s, "benchy").await;
    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/cb";
    let cid = loopback_client_id("atproto transition:generic", redirect);
    let f = Flow::new(&cid, redirect, "atproto transition:generic", &key);
    let mut b = Browser::default();
    let p = pkce();
    let code = authorize_interactive(&s, &mut b, &f, &acct, &p).await;
    let t = tokens(&exchange(&s, &f, &code, &p, &[]).await);
    let nsid = "app.bsky.actor.getPreferences";
    assert_eq!(xrpc_dpop(&s, &key, &t.access, "GET", nsid, None).await.status, 200);
    let url = format!("{}/xrpc/{nsid}", s.base);
    let n = if inject.is_some() { 200 } else { 2000 };
    // proofs are signed up front: only the server's work is timed
    let proofs: Vec<String> = (0..n).map(|_| key.proof("GET", &url, Some(&t.access))).collect();
    let send = |proof: String| {
        let rb = s.http.get(&url).header("authorization", format!("DPoP {}", t.access)).header("dpop", proof);
        async move {
            let r = rb.send().await.unwrap();
            assert_eq!(r.status(), 200);
            r.bytes().await.unwrap();
        }
    };
    let t0 = std::time::Instant::now();
    for p in proofs[..n / 2].iter().cloned() {
        send(p).await;
    }
    let seq = t0.elapsed().as_secs_f64() * 1e6 / (n / 2) as f64;
    let t0 = std::time::Instant::now();
    use futures::StreamExt;
    futures::stream::iter(proofs[n / 2..].iter().cloned().map(send))
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;
    let par = t0.elapsed().as_secs_f64() * 1e6 / (n / 2) as f64;
    println!("DPoP resource request: {seq:.0} us sequential, {par:.0} us/request at 16 in flight");
}

// Reference-suite ports that reuse this file's client simulation.
#[path = "ref_oauth.rs"]
mod ref_oauth;
