//! JOSE pieces: ES256 (P-256) JWK handling, JWT signing/verification, RFC 7638
//! thumbprints, DPoP proof verification (RFC 9449) and server-issued DPoP
//! nonces.

use super::util::{
    b64u, b64u_decode, derive_secret, hmac_sha256, now_secs, sha256_b64u, ReplayCache,
};
use p256::ecdsa::signature::{Signer, Verifier};
use p256::ecdsa::{Signature, SigningKey, VerifyingKey};
use p256::EncodedPoint;
use serde_json::{json, Value as J};
use std::sync::LazyLock;

/// Signing algorithms accepted for DPoP proofs and client assertions.
pub const VERIFY_ALGS: [&str; 1] = ["ES256"];

/// DPoP proof `iat` acceptance window: `maxTokenAge` 10 s plus a 180 s clock
/// tolerance, as in the reference implementation.
const DPOP_MAX_AGE: i64 = 10;
const DPOP_CLOCK_TOLERANCE: i64 = 180;

/// Parses an EC P-256 public JWK. Rejects private keys.
pub fn jwk_to_key(jwk: &J) -> Result<VerifyingKey, String> {
    if jwk.get("kty").and_then(|v| v.as_str()) != Some("EC")
        || jwk.get("crv").and_then(|v| v.as_str()) != Some("P-256")
    {
        return Err("unsupported JWK (expected EC P-256)".into());
    }
    if jwk.get("d").is_some() {
        return Err("JWK must be a public key".into());
    }
    let x = jwk
        .get("x")
        .and_then(|v| v.as_str())
        .and_then(b64u_decode)
        .ok_or("JWK missing x")?;
    let y = jwk
        .get("y")
        .and_then(|v| v.as_str())
        .and_then(b64u_decode)
        .ok_or("JWK missing y")?;
    if x.len() != 32 || y.len() != 32 {
        return Err("invalid JWK coordinates".into());
    }
    let pt = EncodedPoint::from_affine_coordinates(x.as_slice().into(), y.as_slice().into(), false);
    VerifyingKey::from_encoded_point(&pt).map_err(|_| "invalid JWK point".to_string())
}

pub fn key_to_jwk(k: &VerifyingKey) -> J {
    let pt = k.to_encoded_point(false);
    json!({"kty": "EC", "crv": "P-256", "x": b64u(pt.x().unwrap()), "y": b64u(pt.y().unwrap())})
}

/// RFC 7638 SHA-256 thumbprint of an EC P-256 JWK.
pub fn jwk_thumbprint(jwk: &J) -> Result<String, String> {
    let k = jwk_to_key(jwk)?;
    let c = key_to_jwk(&k);
    let canon = format!(
        r#"{{"crv":"P-256","kty":"EC","x":"{}","y":"{}"}}"#,
        c["x"].as_str().unwrap(),
        c["y"].as_str().unwrap()
    );
    Ok(sha256_b64u(canon))
}

pub struct DecodedJwt {
    pub header: J,
    pub payload: J,
    signing_input: String,
    sig: Vec<u8>,
}

impl DecodedJwt {
    pub fn decode(token: &str) -> Result<DecodedJwt, String> {
        let mut parts = token.split('.');
        let (h, p, s) = match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(h), Some(p), Some(s), None) => (h, p, s),
            _ => return Err("malformed JWT".into()),
        };
        let header: J = serde_json::from_slice(&b64u_decode(h).ok_or("malformed JWT header")?)
            .map_err(|_| "malformed JWT header")?;
        let payload: J = serde_json::from_slice(&b64u_decode(p).ok_or("malformed JWT payload")?)
            .map_err(|_| "malformed JWT payload")?;
        if !header.is_object() || !payload.is_object() {
            return Err("malformed JWT".into());
        }
        let sig = b64u_decode(s).ok_or("malformed JWT signature")?;
        Ok(DecodedJwt {
            header,
            payload,
            signing_input: format!("{h}.{p}"),
            sig,
        })
    }

    pub fn alg(&self) -> &str {
        self.header
            .get("alg")
            .and_then(|v| v.as_str())
            .unwrap_or("")
    }

    pub fn verify_es256(&self, key: &VerifyingKey) -> bool {
        if self.alg() != "ES256" || self.sig.len() != 64 {
            return false;
        }
        match Signature::from_slice(&self.sig) {
            Ok(sig) => key.verify(self.signing_input.as_bytes(), &sig).is_ok(),
            Err(_) => false,
        }
    }

    pub fn claim_str(&self, k: &str) -> Option<&str> {
        self.payload.get(k).and_then(|v| v.as_str())
    }

    pub fn claim_i64(&self, k: &str) -> Option<i64> {
        self.payload
            .get(k)
            .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
    }
}

/// The server's access-token signing key (ES256). Derived deterministically
/// from the configured server secret so every node signs/verifies with the
/// same key without shared state; its public half is published at
/// `/oauth/jwks`.
pub struct ServerKey {
    pub sk: SigningKey,
    pub kid: String,
}

impl ServerKey {
    pub fn derive(server_secret: &str) -> ServerKey {
        let mut ctr = 0u32;
        loop {
            let seed = derive_secret(server_secret, &format!("access-token-es256/{ctr}"));
            if let Ok(sk) = SigningKey::from_bytes(&seed.into()) {
                let kid = jwk_thumbprint(&key_to_jwk(sk.verifying_key())).unwrap();
                return ServerKey { sk, kid };
            }
            ctr += 1;
        }
    }

    pub fn public_jwk(&self) -> J {
        let mut j = key_to_jwk(self.sk.verifying_key());
        j["kid"] = J::String(self.kid.clone());
        j["use"] = J::String("sig".into());
        j["alg"] = J::String("ES256".into());
        j
    }

    pub fn sign(&self, typ: &str, payload: &J) -> String {
        let header = json!({"alg": "ES256", "typ": typ, "kid": self.kid});
        let input = format!(
            "{}.{}",
            b64u(serde_json::to_vec(&header).unwrap()),
            b64u(serde_json::to_vec(payload).unwrap())
        );
        let sig: Signature = self.sk.sign(input.as_bytes());
        format!("{input}.{}", b64u(sig.to_bytes()))
    }

    /// Verifies signature and `typ`; claims are checked by the caller.
    pub fn verify(&self, token: &str, typ: &str) -> Result<DecodedJwt, String> {
        let jwt = DecodedJwt::decode(token)?;
        if jwt.header.get("typ").and_then(|v| v.as_str()) != Some(typ) {
            return Err("unexpected token type".into());
        }
        if !jwt.verify_es256(self.sk.verifying_key()) {
            return Err("invalid token signature".into());
        }
        Ok(jwt)
    }
}

// ---------- DPoP nonces ----------

/// Rotating, stateless DPoP nonces (as the reference `DpopNonce`): the nonce
/// for time window `n` is HMAC(secret, n). Windows are 60 s and the previous,
/// current and next nonces are accepted, so a nonce lives at most ~3 minutes
/// (the spec caps it at 5). The secret is derived from the server secret, so
/// all nodes issue and accept the same nonces.
pub struct DpopNonces {
    secret: [u8; 32],
}

pub const NONCE_ROTATION_SECS: i64 = 60;

impl DpopNonces {
    pub fn new(server_secret: &str) -> DpopNonces {
        DpopNonces {
            secret: derive_secret(server_secret, "dpop-nonce"),
        }
    }

    fn compute(&self, counter: i64) -> String {
        b64u(hmac_sha256(&self.secret, &[&counter.to_be_bytes()]))
    }

    /// The nonce clients should use next (the upcoming window's value, so it
    /// stays valid for the longest time).
    pub fn next(&self) -> String {
        self.compute(now_secs() / NONCE_ROTATION_SECS + 1)
    }

    pub fn check(&self, nonce: &str) -> bool {
        let c = now_secs() / NONCE_ROTATION_SECS;
        (c - 1..=c + 1).any(|n| super::util::ct_eq(self.compute(n).as_bytes(), nonce.as_bytes()))
    }
}

// ---------- DPoP proofs ----------

#[derive(Debug)]
pub enum DpopError {
    /// The client must retry with a (fresh) server nonce.
    UseNonce(String),
    Invalid(String),
}

#[derive(Debug, Clone)]
pub struct DpopProof {
    pub jkt: String,
    pub jti: String,
    pub htm: String,
    pub htu: String,
}

/// DPoP proof `jti` replay cache. Process-local (see HA note in mod.rs).
static DPOP_JTIS: LazyLock<ReplayCache> = LazyLock::new(|| ReplayCache::new(1_000_000));

/// Normalizes an absolute http(s) URL for `htu` comparison: scheme + host +
/// port (default ports elided) + normalized path; no query or fragment.
pub fn normalize_htu(u: &str) -> Option<String> {
    let url = reqwest::Url::parse(u).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return None;
    }
    let origin = url.origin().ascii_serialization();
    Some(format!("{origin}{}", url.path()))
}

/// Verifies a DPoP proof (RFC 9449 §4.3) for a request with method `htm` to
/// `expected_htu` (already normalized). `access_token` is Some for resource
/// requests (`ath` required) and None at the authorization server (`ath`
/// forbidden). Nonces are mandatory.
pub fn check_proof(
    proof: &str,
    htm: &str,
    expected_htu: &str,
    access_token: Option<&str>,
    nonces: &DpopNonces,
) -> Result<DpopProof, DpopError> {
    let inv = |m: &str| DpopError::Invalid(m.to_string());
    let jwt = DecodedJwt::decode(proof)
        .map_err(|e| DpopError::Invalid(format!("Failed to verify DPoP proof: {e}")))?;
    if jwt.header.get("typ").and_then(|v| v.as_str()) != Some("dpop+jwt") {
        return Err(inv(
            "Failed to verify DPoP proof: unexpected \"typ\" JWT header value",
        ));
    }
    if !VERIFY_ALGS.contains(&jwt.alg()) {
        return Err(inv("Failed to verify DPoP proof: unsupported \"alg\""));
    }
    let jwk = jwt
        .header
        .get("jwk")
        .ok_or_else(|| inv("Failed to verify DPoP proof: missing \"jwk\" header"))?;
    let key = jwk_to_key(jwk)
        .map_err(|e| DpopError::Invalid(format!("Failed to verify DPoP proof: {e}")))?;
    if !jwt.verify_es256(&key) {
        return Err(inv(
            "Failed to verify DPoP proof: signature verification failed",
        ));
    }
    let now = now_secs();
    let iat = jwt
        .claim_i64("iat")
        .ok_or_else(|| inv("Failed to verify DPoP proof: missing \"iat\" claim"))?;
    if iat > now + DPOP_CLOCK_TOLERANCE {
        return Err(inv("Failed to verify DPoP proof: \"iat\" claim timestamp check failed (it should be in the past)"));
    }
    if iat < now - DPOP_MAX_AGE - DPOP_CLOCK_TOLERANCE {
        return Err(inv("Failed to verify DPoP proof: \"iat\" claim timestamp check failed (too far in the past)"));
    }
    if let Some(exp) = jwt.claim_i64("exp") {
        if exp < now - DPOP_CLOCK_TOLERANCE {
            return Err(inv(
                "Failed to verify DPoP proof: \"exp\" claim timestamp check failed",
            ));
        }
    }
    let nonce = match jwt.payload.get("nonce") {
        None => None,
        Some(J::String(s)) => Some(s.clone()),
        Some(_) => return Err(inv("Invalid DPoP \"nonce\" type")),
    };
    let jti = jwt
        .claim_str("jti")
        .filter(|s| !s.is_empty() && s.len() <= 256)
        .ok_or_else(|| inv("DPoP \"jti\" missing"))?
        .to_string();
    if jwt.claim_str("htm") != Some(htm) {
        return Err(inv("DPoP \"htm\" mismatch"));
    }
    let htu = jwt
        .claim_str("htu")
        .ok_or_else(|| inv("Invalid DPoP \"htu\" type"))?;
    let htu_norm = normalize_htu(htu).ok_or_else(|| inv("DPoP \"htu\" is not a valid URL"))?;
    if htu_norm != expected_htu {
        return Err(inv("DPoP \"htu\" mismatch"));
    }
    match &nonce {
        None => {
            return Err(DpopError::UseNonce(
                "Authorization server requires nonce in DPoP proof".into(),
            ))
        }
        Some(n) if !nonces.check(n) => {
            return Err(DpopError::UseNonce("DPoP \"nonce\" mismatch".into()))
        }
        _ => {}
    }
    let ath = jwt.claim_str("ath");
    match access_token {
        Some(tok) => {
            if ath != Some(sha256_b64u(tok).as_str()) {
                return Err(inv("DPoP \"ath\" mismatch"));
            }
        }
        None => {
            if jwt.payload.get("ath").is_some() {
                return Err(inv("DPoP \"ath\" claim not allowed"));
            }
        }
    }
    let jkt = jwk_thumbprint(jwk)
        .map_err(|e| DpopError::Invalid(format!("Failed to calculate jkt: {e}")))?;
    // Replay protection: a proof may be used once within its validity window.
    if !DPOP_JTIS.insert_unique(
        &format!("{jkt}:{jti}"),
        now + DPOP_MAX_AGE + 2 * DPOP_CLOCK_TOLERANCE,
    ) {
        return Err(inv("DPoP proof replayed"));
    }
    Ok(DpopProof {
        jkt,
        jti,
        htm: htm.to_string(),
        htu: htu_norm,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proof(
        sk: &SigningKey,
        htm: &str,
        htu: &str,
        nonce: Option<&str>,
        ath: Option<&str>,
    ) -> String {
        let jwk = key_to_jwk(sk.verifying_key());
        let header = json!({"typ": "dpop+jwt", "alg": "ES256", "jwk": jwk});
        let mut payload = json!({"jti": super::super::util::random_id("", 12), "htm": htm, "htu": htu, "iat": now_secs()});
        if let Some(n) = nonce {
            payload["nonce"] = J::String(n.into());
        }
        if let Some(a) = ath {
            payload["ath"] = J::String(sha256_b64u(a));
        }
        let input = format!(
            "{}.{}",
            b64u(serde_json::to_vec(&header).unwrap()),
            b64u(serde_json::to_vec(&payload).unwrap())
        );
        let sig: Signature = sk.sign(input.as_bytes());
        format!("{input}.{}", b64u(sig.to_bytes()))
    }

    #[test]
    fn dpop_roundtrip() {
        let nonces = DpopNonces::new("secret");
        let sk = SigningKey::random(&mut rand::rngs::OsRng);
        let htu = "https://pds.example/xrpc/foo";
        let p = proof(&sk, "POST", htu, None, None);
        assert!(matches!(
            check_proof(&p, "POST", htu, None, &nonces),
            Err(DpopError::UseNonce(_))
        ));
        let n = nonces.next();
        let p = proof(
            &sk,
            "POST",
            "https://pds.example/xrpc/foo?x=1",
            Some(&n),
            Some("tok"),
        );
        let ok = check_proof(&p, "POST", htu, Some("tok"), &nonces).unwrap();
        assert_eq!(
            ok.jkt,
            jwk_thumbprint(&key_to_jwk(sk.verifying_key())).unwrap()
        );
        assert!(
            matches!(
                check_proof(&p, "POST", htu, Some("tok"), &nonces),
                Err(DpopError::Invalid(_))
            ),
            "replay"
        );
        let p = proof(&sk, "GET", htu, Some(&n), Some("tok"));
        assert!(check_proof(&p, "POST", htu, Some("tok"), &nonces).is_err());
        let p = proof(&sk, "POST", htu, Some(&n), Some("other"));
        assert!(check_proof(&p, "POST", htu, Some("tok"), &nonces).is_err());
        let p = proof(&sk, "POST", htu, Some("bogus"), None);
        assert!(matches!(
            check_proof(&p, "POST", htu, None, &nonces),
            Err(DpopError::UseNonce(_))
        ));
    }

    #[test]
    fn server_key_stable() {
        let a = ServerKey::derive("s1");
        let b = ServerKey::derive("s1");
        assert_eq!(a.kid, b.kid);
        let t = a.sign("at+jwt", &json!({"sub": "x"}));
        assert!(b.verify(&t, "at+jwt").is_ok());
        assert!(ServerKey::derive("s2").verify(&t, "at+jwt").is_err());
    }
}
