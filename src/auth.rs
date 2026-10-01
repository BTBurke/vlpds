//! HS256 session JWTs (access + refresh), and the verified-token cache
//! shared with OAuth access tokens.

use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct Jwt {
    secret: Vec<u8>,
    pub service_did: String,
    /// Tokens whose signature this secret verified, with their claims.
    verified: Arc<TokenCache<Arc<Claims>>>,
}

const TOKEN_CACHE_SHARDS: usize = 64;
/// Legacy access tokens cached: ~400 B each with their claims, enough for
/// the tokens of a million active accounts.
const SESSION_TOKENS_CACHED: usize = 1 << 20;

/// Verified bearer tokens: what a token proves by itself (signature checked,
/// claims parsed), kept until its `exp`, so a token pays for verification
/// and parsing once instead of on every request. Only signed tokens are put
/// here, and a hit compares the whole token. Revocation, sessions and
/// account status are not cached: callers check them on every request.
/// Bounded: a full shard drops its expired entries, then all of them.
pub struct TokenCache<V> {
    /// signature segment -> (whole token, value, exp unix secs)
    shards: Vec<parking_lot::Mutex<HashMap<Box<str>, (Box<str>, V, u64)>>>,
    cap_per_shard: usize,
}

impl<V: Clone> TokenCache<V> {
    /// A cache of about `capacity` tokens.
    pub fn new(capacity: usize) -> Self {
        TokenCache {
            shards: (0..TOKEN_CACHE_SHARDS).map(|_| Default::default()).collect(),
            cap_per_shard: capacity.div_ceil(TOKEN_CACHE_SHARDS).max(1),
        }
    }

    /// (shard, signature segment). The signature is random-looking, so its
    /// last bytes pick the shard without hashing the token.
    fn slot<'t>(&self, token: &'t str) -> (&parking_lot::Mutex<HashMap<Box<str>, (Box<str>, V, u64)>>, &'t str) {
        let sig = token.rsplit_once('.').map_or(token, |(_, s)| s);
        let b = sig.as_bytes();
        let tail = b[b.len().saturating_sub(4)..].iter().fold(0usize, |h, &x| h.wrapping_mul(131).wrapping_add(x as usize));
        (&self.shards[tail % self.shards.len()], sig)
    }

    /// The value cached for exactly `token`, unless it expired before `now`.
    pub fn get(&self, token: &str, now: u64) -> Option<V> {
        let (shard, sig) = self.slot(token);
        let m = shard.lock();
        let (tok, v, exp) = m.get(sig)?;
        (**tok == *token && *exp >= now).then(|| v.clone())
    }

    /// Caches `v` for `token` (whose signature the caller verified) until `exp`.
    pub fn put(&self, token: &str, v: V, exp: u64, now: u64) {
        if exp < now {
            return;
        }
        let (shard, sig) = self.slot(token);
        let mut m = shard.lock();
        if m.len() >= self.cap_per_shard {
            m.retain(|_, (_, _, e)| *e >= now);
            if m.len() >= self.cap_per_shard {
                m.clear();
            }
        }
        m.insert(sig.into(), (token.into(), v, exp));
    }
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
            verified: Arc::new(TokenCache::new(SESSION_TOKENS_CACHED)),
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

    /// [`Jwt::verify_signature`] through the verified-token cache: the
    /// signature is checked and the claims parsed once per token (until its
    /// expiry); expiry, scope and revocation stay the caller's to check.
    pub fn verify_signature_cached(&self, token: &str) -> Option<Arc<Claims>> {
        let now = crate::tid::now_micros() / 1_000_000;
        if let Some(c) = self.verified.get(token, now) {
            return Some(c);
        }
        let c = Arc::new(self.verify_signature(token)?);
        self.verified.put(token, c.clone(), c.exp, now);
        Some(c)
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

/// Constant-time secret comparison for admin / internal / bypass tokens.
/// Compares SHA-256 digests, so neither content nor length leaks through
/// timing. An empty `expected` (unset secret) never matches.
pub fn token_eq(expected: &str, given: &str) -> bool {
    use sha2::Digest;
    if expected.is_empty() {
        return false;
    }
    let (a, b) = (Sha256::digest(expected), Sha256::digest(given));
    a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// `Authorization: Basic <b64>` value (after the scheme) carrying
/// `admin:<admin_token>`.
pub fn basic_admin_ok(b64: &str, admin_token: &str) -> bool {
    let dec = base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .unwrap_or_default();
    match std::str::from_utf8(&dec).ok().and_then(|s| s.strip_prefix("admin:")) {
        Some(tok) => token_eq(admin_token, tok),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_cache() {
        let c: TokenCache<u32> = TokenCache::new(TOKEN_CACHE_SHARDS * 2);
        c.put("h.p.sig", 1, 100, 50);
        assert_eq!(c.get("h.p.sig", 50), Some(1));
        assert_eq!(c.get("h.p.sig", 100), Some(1));
        assert_eq!(c.get("h.p.sig", 101), None, "expired");
        assert_eq!(c.get("h.other.sig", 50), None, "same signature, another token");
        c.put("h.p.old", 2, 10, 50);
        assert_eq!(c.get("h.p.old", 5), None, "already expired when put");
        // bounded: a full shard drops its expired entries, then everything
        for i in 0..10_000 {
            c.put(&format!("h.p.{i:08}"), i, 100, 50);
        }
        let n: usize = c.shards.iter().map(|s| s.lock().len()).sum();
        assert!(n <= TOKEN_CACHE_SHARDS * 2, "{n} entries");

        let jwt = Jwt::new("secret", "did:web:pds.test");
        let tok = jwt.access("did:plc:abc");
        assert_eq!(jwt.verify_signature_cached(&tok).unwrap().sub, "did:plc:abc");
        assert_eq!(jwt.verify_signature_cached(&tok).unwrap().sub, "did:plc:abc");
        let other = Jwt::new("other secret", "did:web:pds.test");
        assert!(other.verify_signature_cached(&tok).is_none(), "cached per secret");
        let (input, _) = tok.rsplit_once('.').unwrap();
        let (_, sig) = other.access("did:plc:abc").rsplit_once('.').map(|(a, b)| (a.to_string(), b.to_string())).unwrap();
        assert!(jwt.verify_signature_cached(&format!("{input}.{sig}")).is_none());
    }

    #[test]
    fn token_compare() {
        assert!(token_eq("abc", "abc"));
        assert!(!token_eq("abc", "abd"));
        assert!(!token_eq("abc", "abcd"));
        assert!(!token_eq("", ""), "an unset token never matches");
        let b = base64::engine::general_purpose::STANDARD.encode("admin:tok");
        assert!(basic_admin_ok(&b, "tok"));
        assert!(!basic_admin_ok(&b, "other"));
        let empty = base64::engine::general_purpose::STANDARD.encode("admin:");
        assert!(!basic_admin_ok(&empty, ""));
    }
}
