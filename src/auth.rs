//! HS256 session JWTs (access + refresh).

use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

#[derive(Clone)]
pub struct Jwt {
    secret: Vec<u8>,
    pub service_did: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Claims {
    pub scope: String,
    pub sub: String,
    pub aud: String,
    pub iat: u64,
    pub exp: u64,
    /// Session id (refresh tokens: the token id; access tokens: the session
    /// family id). Used for revocation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jti: Option<String>,
}

impl Jwt {
    pub fn new(secret: &str, service_did: &str) -> Jwt {
        Jwt {
            secret: secret.as_bytes().to_vec(),
            service_did: service_did.to_string(),
        }
    }

    fn mac(&self) -> Hmac<Sha256> {
        Hmac::<Sha256>::new_from_slice(&self.secret).unwrap()
    }

    fn issue(&self, did: &str, scope: &str, ttl_secs: u64, typ: &str) -> String {
        self.issue_with_jti(did, scope, ttl_secs, typ, None)
    }

    /// Signs a session JWT carrying an optional `jti` (session id).
    pub fn issue_with_jti(
        &self,
        did: &str,
        scope: &str,
        ttl_secs: u64,
        typ: &str,
        jti: Option<&str>,
    ) -> String {
        let now = crate::tid::now_micros() / 1_000_000;
        let header = B64.encode(format!(r#"{{"alg":"HS256","typ":"{typ}"}}"#));
        let claims = Claims {
            scope: scope.into(),
            sub: did.into(),
            aud: self.service_did.clone(),
            iat: now,
            exp: now + ttl_secs,
            jti: jti.map(Into::into),
        };
        let payload = B64.encode(serde_json::to_vec(&claims).unwrap());
        let signing_input = format!("{header}.{payload}");
        let mut mac = self.mac();
        mac.update(signing_input.as_bytes());
        format!(
            "{signing_input}.{}",
            B64.encode(mac.finalize().into_bytes())
        )
    }

    pub fn access(&self, did: &str) -> String {
        self.issue(did, "com.atproto.access", 2 * 3600, "at+jwt")
    }

    pub fn refresh(&self, did: &str) -> String {
        self.issue(did, "com.atproto.refresh", 90 * 86400, "refresh+jwt")
    }

    /// Verifies the signature only and returns the claims (expiry and scope
    /// are the caller's to check).
    pub fn verify_signature(&self, token: &str) -> Option<Claims> {
        let (signing_input, sig) = token.rsplit_once('.')?;
        let sig = B64.decode(sig).ok()?;
        let mut mac = self.mac();
        mac.update(signing_input.as_bytes());
        mac.verify_slice(&sig).ok()?;
        let (_, payload) = signing_input.split_once('.')?;
        serde_json::from_slice(&B64.decode(payload).ok()?).ok()
    }

    /// Returns the DID of a valid access token.
    pub fn verify_access(&self, token: &str) -> Option<String> {
        let (signing_input, sig) = token.rsplit_once('.')?;
        let sig = B64.decode(sig).ok()?;
        let mut mac = self.mac();
        mac.update(signing_input.as_bytes());
        mac.verify_slice(&sig).ok()?;
        let (_, payload) = signing_input.split_once('.')?;
        let claims: Claims = serde_json::from_slice(&B64.decode(payload).ok()?).ok()?;
        let now = crate::tid::now_micros() / 1_000_000;
        if claims.scope != "com.atproto.access" || claims.exp < now {
            return None;
        }
        Some(claims.sub)
    }
}

/// Inter-service auth JWT (ES256K) signed with the account's signing key, as
/// used by getServiceAuth and service proxying. `aud` is the target service DID
/// (optionally with a #fragment); `lxm` binds the token to one XRPC method.
pub fn service_auth_jwt(
    key: &crate::crypto::Keypair,
    iss: &str,
    aud: &str,
    lxm: Option<&str>,
    ttl_secs: u64,
) -> String {
    let now = crate::tid::now_micros() / 1_000_000;
    let header = B64.encode(r#"{"typ":"JWT","alg":"ES256K"}"#);
    let mut claims = serde_json::json!({
        "iat": now,
        "iss": iss,
        "aud": aud,
        "exp": now + ttl_secs,
        "jti": hex::encode(rand::random::<[u8; 16]>()),
    });
    if let Some(lxm) = lxm {
        claims["lxm"] = serde_json::Value::String(lxm.to_string());
    }
    let payload = B64.encode(serde_json::to_vec(&claims).unwrap());
    let signing_input = format!("{header}.{payload}");
    let sig = key.sign(signing_input.as_bytes());
    format!("{signing_input}.{}", B64.encode(sig))
}
