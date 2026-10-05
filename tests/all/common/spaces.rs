//! Spaces test client ([`SpaceClient`]): an OAuth account granted `space:`
//! scopes (tests/all/oauth.rs `grant`), a P-256 did:key holder for the
//! credential exchange, and signed reads (`vlpds::space::httpsig`).
//!
//! - `SpaceClient::new(&server, name, scope)`: account, PAR, consent, token.
//! - `get`/`post`: DPoP-authenticated XRPC as that account.
//! - `credential(space)`: getDelegationToken, then getSpaceCredential at the
//!   authority's host, signed by the holder key.
//! - `signed_get(base, nsid, query, credential, audience)`: a read with
//!   `Authorization: Atproto-Space`, signed over the authorization and
//!   audience headers.

use super::{Resp, TestServer};
use crate::oauth::{self, Browser, DpopKey, Flow, Srv};
use p256::ecdsa::signature::Signer;
use serde_json::{json, Value as J};
use vlpds::space::httpsig;

pub struct SpaceClient {
    pub srv: Srv,
    pub did: String,
    pub handle: String,
    /// The account's password session (legacy auth).
    pub session_jwt: String,
    pub key: DpopKey,
    pub access: String,
    pub scope: String,
    pub holder: Holder,
}

/// A P-256 did:key: the key a space credential is bound to.
pub struct Holder {
    pub sk: p256::ecdsa::SigningKey,
    pub did: String,
}

impl Holder {
    pub fn new() -> Holder {
        let sk = <p256::ecdsa::SigningKey as p256::elliptic_curve::Generate>::generate();
        let mut mk = vec![0x80, 0x24];
        mk.extend_from_slice(sk.verifying_key().to_sec1_point(true).as_bytes());
        Holder { sk, did: format!("did:key:z{}", bs58::encode(mk).into_string()) }
    }

    fn sign(&self, base: &[u8]) -> String {
        use base64::Engine;
        let sig: p256::ecdsa::Signature = self.sk.sign(base);
        base64::engine::general_purpose::STANDARD.encode(sig.to_bytes())
    }

    /// The headers of a request authorized by `authorization`, signed as the
    /// reference's client signs them (`createSpaceSigHeaders`): with an
    /// audience for a credential read, without one (and with `keyid`) for a
    /// delegation exchange.
    pub fn headers(&self, authorization: &str, audience: Option<&str>) -> Vec<(String, String)> {
        let input = httpsig::signature_input(audience.is_some(), &self.did);
        let sig = self.sign(&httpsig::signature_base(authorization, &input, audience));
        let mut h = vec![("authorization".to_string(), authorization.to_string())];
        if let Some(a) = audience {
            h.push((httpsig::AUDIENCE_HEADER.into(), a.into()));
        }
        h.push(("signature-input".into(), format!("{}={input}", httpsig::LABEL)));
        h.push(("signature".into(), format!("{}=:{sig}:", httpsig::LABEL)));
        h
    }
}

impl Default for Holder {
    fn default() -> Self {
        Holder::new()
    }
}

/// `https?://.../xrpc/{nsid}?{query}`, the query form-encoded.
pub fn xrpc_url(base: &str, nsid: &str, query: &[(&str, &str)]) -> String {
    let q: Vec<String> = query.iter().map(|(k, v)| format!("{}={}", oauth::enc(k), oauth::enc(v))).collect();
    match q.is_empty() {
        true => format!("{base}/xrpc/{nsid}"),
        false => format!("{base}/xrpc/{nsid}?{}", q.join("&")),
    }
}

pub async fn resp(r: reqwest::Response) -> Resp {
    let status = r.status().as_u16();
    let headers = r.headers().clone();
    let body = r.bytes().await.unwrap_or_default();
    let json = serde_json::from_slice(&body).unwrap_or(J::Null);
    Resp { status, headers, body, json }
}

fn oauth_resp(r: oauth::Resp) -> Resp {
    let body = bytes::Bytes::from(serde_json::to_vec(&r.body).unwrap());
    Resp { status: r.status, headers: r.headers, body, json: r.body }
}

/// A space URI.
pub fn space_uri(authority: &str, space_type: &str, skey: &str) -> String {
    format!("at://{authority}/space/{space_type}/{skey}")
}

impl SpaceClient {
    /// A new account on `s`, which a loopback client is granted `scope` for
    /// (`atproto` is added if missing).
    pub async fn new(s: &TestServer, name: &str, scope: &str) -> SpaceClient {
        let srv = Srv {
            app: s.app.clone(),
            base: s.url.clone(),
            http: reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap(),
        };
        let acct = oauth::create_account(&srv, name).await;
        let scope = match scope.split(' ').any(|x| x == "atproto") {
            true => scope.to_string(),
            false => format!("atproto {scope}"),
        };
        let key = DpopKey::new();
        let t = oauth::grant(&srv, &mut Browser::default(), &Flow::loopback(&scope, &key), &acct).await;
        SpaceClient {
            srv,
            did: acct.did,
            handle: acct.handle,
            session_jwt: acct.jwt,
            key,
            access: t.access,
            scope: t.scope,
            holder: Holder::new(),
        }
    }

    pub async fn get(&self, nsid: &str, query: &[(&str, &str)]) -> Resp {
        let path = xrpc_url("", nsid, query);
        let nsid = path.strip_prefix("/xrpc/").unwrap();
        oauth_resp(oauth::xrpc_dpop(&self.srv, &self.key, &self.access, "GET", nsid, None).await)
    }

    pub async fn post(&self, nsid: &str, body: J) -> Resp {
        oauth_resp(oauth::xrpc_dpop(&self.srv, &self.key, &self.access, "POST", nsid, Some(body)).await)
    }

    /// simplespace.createSpace with the default policies; the space's URI.
    pub async fn create_space(&self, space_type: &str, skey: &str) -> String {
        let r = self
            .post(
                "com.atproto.simplespace.createSpace",
                json!({
                    "spaceType": space_type,
                    "skey": skey,
                    "readPolicy": {"$type": "com.atproto.simplespace.defs#memberListPolicy"},
                    "writePolicy": {"$type": "com.atproto.simplespace.defs#memberListPolicy"},
                    "appAccess": {"$type": "com.atproto.simplespace.defs#open"},
                }),
            )
            .await;
        r.ok()["uri"].as_str().unwrap().to_string()
    }

    pub async fn create_record(&self, space: &str, collection: &str, rkey: Option<&str>, record: J) -> Resp {
        let mut body = json!({"space": space, "repo": self.did, "collection": collection, "record": record});
        if let Some(r) = rkey {
            body["rkey"] = json!(r);
        }
        self.post("com.atproto.space.createRecord", body).await
    }

    pub async fn put_record(&self, space: &str, collection: &str, rkey: &str, record: J) -> Resp {
        let body = json!({"space": space, "repo": self.did, "collection": collection, "rkey": rkey, "record": record});
        self.post("com.atproto.space.putRecord", body).await
    }

    pub async fn delete_record(&self, space: &str, collection: &str, rkey: &str) -> Resp {
        let body = json!({"space": space, "repo": self.did, "collection": collection, "rkey": rkey});
        self.post("com.atproto.space.deleteRecord", body).await
    }

    pub async fn apply_writes(&self, space: &str, writes: J) -> Resp {
        self.post("com.atproto.space.applyWrites", json!({"space": space, "repo": self.did, "writes": writes})).await
    }

    pub async fn delegation_token(&self, space: &str) -> Resp {
        self.get("com.atproto.space.getDelegationToken", &[("space", space)]).await
    }

    /// getSpaceCredential at `host` (the authority's PDS) for `token`,
    /// signed by this client's holder key.
    pub async fn exchange(&self, host: &str, space: &str, token: &str) -> Resp {
        self.exchange_as(&self.holder, host, space, token).await
    }

    pub async fn exchange_as(&self, holder: &Holder, host: &str, space: &str, token: &str) -> Resp {
        let mut rb = self.srv.http.post(format!("{host}/xrpc/com.atproto.space.getSpaceCredential"));
        for (k, v) in holder.headers(&format!("Bearer {token}"), None) {
            rb = rb.header(k, v);
        }
        resp(rb.json(&json!({"space": space})).send().await.unwrap()).await
    }

    /// A credential for `space` from the authority at `host`: the whole
    /// chain, each step asserted to succeed.
    pub async fn credential_at(&self, host: &str, space: &str) -> String {
        let token = self.delegation_token(space).await.ok()["token"].as_str().unwrap().to_string();
        let r = self.exchange(host, space, &token).await;
        r.ok()["credential"].as_str().unwrap().to_string()
    }

    /// [`Self::credential_at`] on this client's own PDS.
    pub async fn credential(&self, space: &str) -> String {
        let host = self.srv.base.clone();
        self.credential_at(&host, space).await
    }

    /// A read at `base` with `credential`, signed for `audience` by this
    /// client's holder key.
    pub async fn signed_get(
        &self,
        base: &str,
        nsid: &str,
        query: &[(&str, &str)],
        credential: &str,
        audience: &str,
    ) -> Resp {
        signed_get_as(&self.srv.http, &self.holder, base, nsid, query, credential, audience).await
    }
}

/// A credential read signed by `holder`.
pub async fn signed_get_as(
    http: &reqwest::Client,
    holder: &Holder,
    base: &str,
    nsid: &str,
    query: &[(&str, &str)],
    credential: &str,
    audience: &str,
) -> Resp {
    let mut rb = http.get(xrpc_url(base, nsid, query));
    for (k, v) in holder.headers(&format!("Atproto-Space {credential}"), Some(audience)) {
        rb = rb.header(k, v);
    }
    resp(rb.send().await.unwrap()).await
}
