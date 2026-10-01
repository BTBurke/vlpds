//! k256 signing keys and did:key / multikey encodings.

use k256::ecdsa::{signature::Signer, Signature, SigningKey};

pub struct Keypair {
    pub sk: SigningKey,
}

impl Keypair {
    pub fn generate() -> Keypair {
        Keypair {
            sk: SigningKey::random(&mut rand::rngs::OsRng),
        }
    }

    pub fn from_bytes(b: &[u8]) -> anyhow::Result<Keypair> {
        Ok(Keypair {
            sk: SigningKey::from_slice(b)?,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.sk.to_bytes().to_vec()
    }

    /// Low-S ECDSA signature over sha256(data), 64-byte compact form.
    pub fn sign(&self, data: &[u8]) -> [u8; 64] {
        let sig: Signature = self.sk.sign(data);
        let sig = sig.normalize_s().unwrap_or(sig);
        sig.to_bytes().into()
    }

    /// Multibase (base58btc) multikey: secp256k1-pub multicodec + compressed point.
    pub fn public_multibase(&self) -> String {
        let point = self.sk.verifying_key().to_encoded_point(true);
        let mut b = vec![0xe7, 0x01];
        b.extend_from_slice(point.as_bytes());
        format!("z{}", bs58::encode(b).into_string())
    }

    pub fn did_key(&self) -> String {
        format!("did:key:{}", self.public_multibase())
    }
}

pub fn random_plc_did() -> String {
    let b: [u8; 15] = rand::random();
    format!("did:plc:{}", crate::cid::base32_encode(&b))
}
