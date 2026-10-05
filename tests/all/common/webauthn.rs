//! A software WebAuthn authenticator (ES256, `p256`) that answers the
//! server's options the way a browser would, and can be told to lie: bad
//! flags, origin, RP ID, type, counter, user handle.

use super::*;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine;
use p256::ecdsa::signature::Signer;
use sha2::{Digest, Sha256};

pub fn b64u(b: impl AsRef<[u8]>) -> String {
    B64.encode(b)
}

pub fn unb64u(s: &str) -> Vec<u8> {
    B64.decode(s).unwrap()
}

/// What to get wrong.
#[derive(Clone, Default)]
pub struct Lie {
    pub origin: Option<String>,
    pub rp_id: Option<String>,
    pub ty: Option<&'static str>,
    pub no_uv: bool,
    pub no_up: bool,
    /// Report this counter instead of the next one.
    pub count: Option<u32>,
    pub user_handle: Option<Vec<u8>>,
    pub cross_origin: bool,
    pub challenge: Option<String>,
}

pub struct SoftKey {
    sk: p256::ecdsa::SigningKey,
    pub id: Vec<u8>,
    pub count: u32,
    /// Synced (backup eligible), like iCloud Keychain: counts stay at 0.
    pub synced: bool,
    pub user_handle: Vec<u8>,
    /// Origin and RP ID: the server's public URL.
    pub origin: String,
}

impl SoftKey {
    /// A hardware-style key (counts up) for `server`.
    pub fn new(origin: &str) -> SoftKey {
        SoftKey {
            sk: p256::ecdsa::SigningKey::random(&mut rand::rngs::OsRng),
            id: rand::random::<[u8; 32]>().to_vec(),
            count: 0,
            synced: false,
            user_handle: Vec::new(),
            origin: origin.trim_end_matches('/').to_string(),
        }
    }

    pub fn synced(origin: &str) -> SoftKey {
        SoftKey { synced: true, ..SoftKey::new(origin) }
    }

    pub fn id_b64(&self) -> String {
        b64u(&self.id)
    }

    fn rp_id(&self) -> String {
        reqwest::Url::parse(&self.origin).unwrap().host_str().unwrap().to_string()
    }

    fn cose(&self) -> Vec<u8> {
        let p = self.sk.verifying_key().to_encoded_point(false);
        let mut out = vec![0xa5, 0x01, 0x02, 0x03, 0x26, 0x20, 0x01, 0x21, 0x58, 0x20];
        out.extend_from_slice(p.x().unwrap());
        out.extend_from_slice(&[0x22, 0x58, 0x20]);
        out.extend_from_slice(p.y().unwrap());
        out
    }

    fn flags(&self, l: &Lie, at: bool) -> u8 {
        let mut f = 0u8;
        if !l.no_up {
            f |= 0x01;
        }
        if !l.no_uv {
            f |= 0x04;
        }
        if self.synced {
            f |= 0x08 | 0x10;
        }
        if at {
            f |= 0x40;
        }
        f
    }

    fn client_data(&self, ty: &str, challenge: &str, l: &Lie) -> Vec<u8> {
        let mut j = json!({
            "type": l.ty.unwrap_or(ty),
            "challenge": l.challenge.clone().unwrap_or_else(|| challenge.to_string()),
            "origin": l.origin.clone().unwrap_or_else(|| self.origin.clone()),
        });
        if l.cross_origin {
            j["crossOrigin"] = json!(true);
        }
        serde_json::to_vec(&j).unwrap()
    }

    fn next_count(&mut self, l: &Lie) -> u32 {
        if let Some(c) = l.count {
            return c;
        }
        if !self.synced {
            self.count += 1;
        }
        self.count
    }

    /// The server's `startPasskeyRegistration` options -> the
    /// `credential` for `finishPasskeyRegistration`.
    pub fn register(&mut self, options: &J, l: &Lie) -> J {
        self.user_handle = unb64u(options["user"]["id"].as_str().unwrap());
        let rp_id = l.rp_id.clone().unwrap_or_else(|| self.rp_id());
        assert_eq!(options["rp"]["id"].as_str(), Some(self.rp_id().as_str()), "{options}");
        let cdj = self.client_data("webauthn.create", options["challenge"].as_str().unwrap(), l);
        let mut ad = Sha256::digest(rp_id.as_bytes()).to_vec();
        ad.push(self.flags(l, true));
        let count = self.next_count(l);
        ad.extend_from_slice(&count.to_be_bytes());
        ad.extend_from_slice(&[0; 16]);
        ad.extend_from_slice(&(self.id.len() as u16).to_be_bytes());
        ad.extend_from_slice(&self.id);
        ad.extend_from_slice(&self.cose());
        let mut att = vec![0xa3, 0x63];
        att.extend_from_slice(b"fmt");
        att.push(0x64);
        att.extend_from_slice(b"none");
        att.push(0x67);
        att.extend_from_slice(b"attStmt");
        att.push(0xa0);
        att.push(0x68);
        att.extend_from_slice(b"authData");
        att.push(0x59);
        att.extend_from_slice(&(ad.len() as u16).to_be_bytes());
        att.extend_from_slice(&ad);
        json!({
            "id": self.id_b64(),
            "rawId": self.id_b64(),
            "type": "public-key",
            "response": {"clientDataJSON": b64u(&cdj), "attestationObject": b64u(&att), "transports": ["internal", "hybrid"]},
            "clientExtensionResults": {"credProps": {"rk": true}},
        })
    }

    /// An assertion over `challenge` (base64url), as the server's
    /// `AssertionIn`.
    pub fn assert(&mut self, challenge: &str, l: &Lie) -> J {
        let rp_id = l.rp_id.clone().unwrap_or_else(|| self.rp_id());
        let cdj = self.client_data("webauthn.get", challenge, l);
        let mut ad = Sha256::digest(rp_id.as_bytes()).to_vec();
        ad.push(self.flags(l, false));
        let count = self.next_count(l);
        ad.extend_from_slice(&count.to_be_bytes());
        let mut msg = ad.clone();
        msg.extend_from_slice(&Sha256::digest(&cdj));
        let sig: p256::ecdsa::Signature = self.sk.sign(&msg);
        let uh = l.user_handle.clone().unwrap_or_else(|| self.user_handle.clone());
        json!({
            "id": self.id_b64(),
            "clientDataJSON": b64u(&cdj),
            "authenticatorData": b64u(&ad),
            "signature": b64u(sig.to_der().as_bytes()),
            "userHandle": b64u(&uh),
        })
    }
}

/// startPasskeyRegistration + finishPasskeyRegistration as `a` on `s`.
pub async fn register_passkey(s: &TestServer, a: &TestAccount, key: &mut SoftKey, name: &str) -> J {
    let opts =
        s.xrpc.post("vlpds.server.startPasskeyRegistration", &json!({"password": a.password}), &a.auth()).await.ok();
    let cred = key.register(&opts, &Lie::default());
    s.xrpc
        .post("vlpds.server.finishPasskeyRegistration", &json!({"name": name, "credential": cred}), &a.auth())
        .await
        .ok()
}
