//! Materialized-state key layout (one SlateDB per partition).
//!
//! h/{did}                 -> head: commit cid | data cid | rev u64 | signed commit block
//! a/{did}                 -> account JSON
//! n/{handle}              -> did
//! R/{did}\0{coll}/{rkey}  -> record cid | rev | record bytes
//! c/{did}\0{cid8}{path}   -> empty (record CID index for getBlocks)
//! L/{did}                 -> record count u64 (large repos: preloaded on shard open)

use crate::cid::{Cid, CID_BYTES_LEN};
use crate::tid::Tid;
use bytes::{BufMut, Bytes};
use sha2::{Digest, Sha256};

pub fn head_key(did: &str) -> Vec<u8> {
    [b"h/", did.as_bytes()].concat()
}

pub fn account_key(did: &str) -> Vec<u8> {
    [b"a/", did.as_bytes()].concat()
}

pub fn handle_key(handle: &str) -> Vec<u8> {
    [b"n/", handle.as_bytes()].concat()
}

/// Collection index: which repos have records in a collection.
pub fn collection_key(collection: &str, did: &str) -> Vec<u8> {
    [b"C/", collection.as_bytes(), b"\0", did.as_bytes()].concat()
}

pub fn collection_prefix(collection: &str) -> Vec<u8> {
    [b"C/", collection.as_bytes(), b"\0"].concat()
}

/// Blob refs: b/{did}\0{blob cid}\0{record path}
pub fn blob_ref_key(did: &str, blob: &crate::cid::Cid, path: &str) -> Vec<u8> {
    [
        b"b/",
        did.as_bytes(),
        b"\0",
        blob.to_string().as_bytes(),
        b"\0",
        path.as_bytes(),
    ]
    .concat()
}

pub fn blob_ref_prefix(did: &str) -> Vec<u8> {
    [b"b/", did.as_bytes(), b"\0"].concat()
}

/// Private (non-repo) per-account state: p/{did}\0{name}
pub fn private_key(did: &str, name: &str) -> Vec<u8> {
    [b"p/", did.as_bytes(), b"\0", name.as_bytes()].concat()
}

pub fn private_prefix(did: &str) -> Vec<u8> {
    [b"p/", did.as_bytes(), b"\0"].concat()
}

/// Large-repo index: one key per repo with at least the pin threshold of
/// records (written with the commit that crosses it, deleted below half of
/// it), so a new owner finds the repos to preload with one short scan. A
/// hint: a stale key only costs a load.
pub fn large_repo_key(did: &str) -> Vec<u8> {
    [LARGE_REPO_PREFIX, did.as_bytes()].concat()
}

pub const LARGE_REPO_PREFIX: &[u8] = b"L/";

pub fn record_prefix(did: &str) -> Vec<u8> {
    [b"R/", did.as_bytes(), b"\0"].concat()
}

pub fn record_key(did: &str, path: &str) -> Vec<u8> {
    [b"R/", did.as_bytes(), b"\0", path.as_bytes()].concat()
}

/// Bytes of a record CID's digest in its index key: enough to make
/// collisions rare (a lookup checks the record's CID anyway), short enough
/// to keep the extra key per record small.
const RECORD_CID_KEY_BYTES: usize = 8;

/// Record CID index: c/{did}\0{first digest bytes of the cid}{path}. Kept
/// next to the R/ key in the same batch; the same CID can sit at several
/// paths (one key each), so lookups scan [`record_cid_prefix`].
pub fn record_cid_key(did: &str, cid: &Cid, path: &str) -> Vec<u8> {
    [&record_cid_prefix(did, cid)[..], path.as_bytes()].concat()
}

pub fn record_cid_prefix(did: &str, cid: &Cid) -> Vec<u8> {
    [b"c/", did.as_bytes(), b"\0", &cid.digest[..RECORD_CID_KEY_BYTES]].concat()
}

/// Smallest key greater than every key with this prefix.
pub fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return end;
        }
    }
    vec![0xff; prefix.len() + 1]
}

#[derive(Clone, Debug)]
pub struct Head {
    pub commit: Cid,
    pub data: Cid,
    pub rev: Tid,
    pub commit_block: Bytes,
}

impl Head {
    pub fn encode(&self) -> Bytes {
        let mut b = Vec::with_capacity(2 * CID_BYTES_LEN + 8 + self.commit_block.len());
        b.put_slice(&self.commit.to_bytes());
        b.put_slice(&self.data.to_bytes());
        b.put_u64(self.rev.0);
        b.put_slice(&self.commit_block);
        b.into()
    }

    pub fn decode(b: &Bytes) -> anyhow::Result<Head> {
        anyhow::ensure!(b.len() >= 2 * CID_BYTES_LEN + 8, "short head");
        Ok(Head {
            commit: Cid::from_bytes(&b[..CID_BYTES_LEN])?,
            data: Cid::from_bytes(&b[CID_BYTES_LEN..2 * CID_BYTES_LEN])?,
            rev: Tid(u64::from_be_bytes(
                b[2 * CID_BYTES_LEN..2 * CID_BYTES_LEN + 8].try_into()?,
            )),
            commit_block: b.slice(2 * CID_BYTES_LEN + 8..),
        })
    }
}

/// Record value: cid | rev (u64, the commit that last wrote it; drives
/// getRepo/listBlobs `since`) | record bytes.
pub fn record_value(cid: &Cid, rev: u64, bytes: &[u8]) -> Bytes {
    let mut b = Vec::with_capacity(CID_BYTES_LEN + 8 + bytes.len());
    b.put_slice(&cid.to_bytes());
    b.put_u64(rev);
    b.put_slice(bytes);
    b.into()
}

pub fn decode_record_value(v: &Bytes) -> anyhow::Result<(Cid, Bytes)> {
    anyhow::ensure!(v.len() >= CID_BYTES_LEN + 8, "short record value");
    Ok((
        Cid::from_bytes(&v[..CID_BYTES_LEN])?,
        v.slice(CID_BYTES_LEN + 8..),
    ))
}

/// Rev of the commit that last wrote a record value.
pub fn record_value_rev(v: &[u8]) -> u64 {
    v.get(CID_BYTES_LEN..CID_BYTES_LEN + 8).map(|b| u64::from_be_bytes(b.try_into().unwrap())).unwrap_or(0)
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Account {
    pub did: String,
    pub handle: String,
    /// hex secp256k1 secret (prototype: plaintext; KMS-wrapped in production)
    pub signing_key: String,
    pub password_hash: String,
    pub created_at: String,
    /// None = active; otherwise "deactivated" | "takendown" | "suspended" | "deleted".
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub email_confirmed: bool,
    /// Extension fields owned by individual XRPC modules.
    #[serde(default, flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// Argon2id (OWASP baseline: m=19 MiB, t=2, p=1) PHC string. ~20 ms of CPU,
/// so it runs on the blocking pool.
pub async fn hash_password(password: &str) -> String {
    let pw = password.to_string();
    tokio::task::spawn_blocking(move || hash_password_blocking(&pw)).await.expect("argon2 task")
}

pub fn hash_password_blocking(password: &str) -> String {
    use argon2::password_hash::{rand_core::OsRng, PasswordHasher, SaltString};
    let salt = SaltString::generate(&mut OsRng);
    argon2_params().hash_password(password.as_bytes(), &salt).expect("argon2 hash").to_string()
}

pub async fn verify_password_hash(phc: &str, password: &str) -> bool {
    let (phc, pw) = (phc.to_string(), password.to_string());
    tokio::task::spawn_blocking(move || {
        use argon2::password_hash::{PasswordHash, PasswordVerifier};
        PasswordHash::new(&phc).is_ok_and(|h| argon2_params().verify_password(pw.as_bytes(), &h).is_ok())
    })
    .await
    .unwrap_or(false)
}

fn argon2_params() -> argon2::Argon2<'static> {
    argon2::Argon2::new(
        argon2::Algorithm::Argon2id,
        argon2::Version::V0x13,
        argon2::Params::new(19 * 1024, 2, 1, None).expect("argon2 params"),
    )
}

/// Stable across processes and nodes (partition assignment must agree everywhere).
pub fn did_hash(did: &str) -> u64 {
    u64::from_be_bytes(Sha256::digest(did.as_bytes())[..8].try_into().unwrap())
}

/// Shard owning `did`: fixed hash slot -> uniform slot range (see slots.rs).
pub fn partition_of(did: &str, shards: u16) -> u16 {
    crate::slots::shard_of(did, shards)
}

/// Deterministic DIDs for bulk-created simulation accounts, so load generators
/// can address account `i` without a lookup.
pub fn bulk_did(i: u64) -> String {
    let h = Sha256::digest(format!("vlpds-bulk:{i}").as_bytes());
    format!("did:plc:{}", &crate::cid::base32_encode(&h)[..24])
}

pub fn bulk_handle(i: u64) -> String {
    format!("b{i}.bulk.vlpds.test")
}
