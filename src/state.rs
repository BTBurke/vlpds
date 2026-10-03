//! Materialized-state key layout (one SlateDB per shard).
//!
//! Every per-account key is slot-major: `0x01 ‖ slot (u16 BE) ‖ family ‖ rest`,
//! where the slot is that of the key's routing key (`slots::slot_of`). A
//! shard's slot range is then one contiguous key range, so a shard splits or
//! merges by cloning its SlateDB with a projection range (DESIGN.md "Online
//! shard split/merge"). Shard-wide keys (`meta/...`) are plain ASCII and sort
//! outside every slot range.
//!
//! h/{did}                 -> head: commit cid | data cid | rev u64 | signed commit block
//! a/{did}                 -> account JSON (signing key wrapped: src/secrets.rs)
//! n/{handle}              -> did (slot of the account's DID)
//! R/{did}\0{coll}/{rkey}  -> record cid | rev | record bytes
//! c/{did}\0{cid8}{path}   -> empty (record CID index for getBlocks)
//! C/{coll}\0{did}         -> empty (collection index; slot of the DID)
//! b/{did}\0{cid}\0{path}  -> empty (blob references)
//! p/{routing}\0{name}     -> private per-account state (slot of the routing key)
//! M/{did}\0{cid digest}   -> MST node block, height >= 1 (DESIGN.md "Partial MSTs")
//! K/{did}                 -> empty (signing-key rotation pending: `Account::pending_signing_key`)
//! bl/{did}\0{code}{subject} -> rkeys linking `subject` (crate::backlinks, DESIGN.md "Backlinks")

use crate::cid::{Cid, CID_BYTES_LEN};
use crate::tid::Tid;
use bytes::{BufMut, Bytes};
use sha2::{Digest, Sha256};

pub const SLOT_TAG: u8 = 0x01;
pub const SLOT_PREFIX_LEN: usize = 3;

pub fn slot_prefix(slot: u16) -> [u8; SLOT_PREFIX_LEN] {
    let [a, b] = slot.to_be_bytes();
    [SLOT_TAG, a, b]
}

/// Remembers the last DID per thread: a commit builds ~10 keys of one
/// repo, and the slot is a SHA-256 of the DID.
fn slot_cached(routing: &str) -> u16 {
    thread_local! {
        static LAST: std::cell::RefCell<(String, u16)> = const { std::cell::RefCell::new((String::new(), 0)) };
    }
    LAST.with(|c| {
        let mut c = c.borrow_mut();
        if c.0.is_empty() || c.0 != routing {
            c.1 = crate::slots::slot_of(routing);
            c.0.clear();
            c.0.push_str(routing);
        }
        c.1
    })
}

fn keyed(routing: &str, fam: &[u8], parts: &[&[u8]]) -> Vec<u8> {
    let len = SLOT_PREFIX_LEN + fam.len() + parts.iter().map(|p| p.len()).sum::<usize>();
    let mut k = Vec::with_capacity(len);
    k.extend_from_slice(&slot_prefix(slot_cached(routing)));
    k.extend_from_slice(fam);
    for p in parts {
        k.extend_from_slice(p);
    }
    k
}

pub fn slot_family(slot: u16, fam: &[u8]) -> Vec<u8> {
    [&slot_prefix(slot)[..], fam].concat()
}

pub fn key_slot(key: &[u8]) -> Option<u16> {
    (key.len() >= SLOT_PREFIX_LEN && key[0] == SLOT_TAG).then(|| u16::from_be_bytes([key[1], key[2]]))
}

/// family ‖ rest.
pub fn key_body(key: &[u8]) -> &[u8] {
    key.get(SLOT_PREFIX_LEN..).unwrap_or_default()
}

/// Slots [lo, hi), hi <= 65,536.
pub fn slot_range_keys(lo: u32, hi: u32) -> (Bytes, Bytes) {
    let at = |s: u32| -> Bytes {
        if s >= crate::slots::SLOTS {
            Bytes::from_static(&[SLOT_TAG + 1])
        } else {
            Bytes::copy_from_slice(&slot_prefix(s as u16))
        }
    };
    (at(lo), at(hi))
}

pub fn head_key(did: &str) -> Vec<u8> {
    keyed(did, b"h/", &[did.as_bytes()])
}

pub fn account_key(did: &str) -> Vec<u8> {
    keyed(did, b"a/", &[did.as_bytes()])
}

/// In `did`'s slot.
pub fn handle_key(did: &str, handle: &str) -> Vec<u8> {
    keyed(did, b"n/", &[handle.as_bytes()])
}

pub const HEAD_FAMILY: &[u8] = b"h/";
pub const ACCOUNT_FAMILY: &[u8] = b"a/";
pub const PRIVATE_FAMILY: &[u8] = b"p/";

pub fn collection_key(collection: &str, did: &str) -> Vec<u8> {
    keyed(did, b"C/", &[collection.as_bytes(), b"\0", did.as_bytes()])
}

/// For [`FamilyScan`].
pub fn collection_family(collection: &str) -> Vec<u8> {
    [b"C/", collection.as_bytes(), b"\0"].concat()
}

pub fn blob_ref_key(did: &str, blob: &crate::cid::Cid, path: &str) -> Vec<u8> {
    keyed(did, b"b/", &[did.as_bytes(), b"\0", blob.to_string().as_bytes(), b"\0", path.as_bytes()])
}

pub fn blob_ref_prefix(did: &str) -> Vec<u8> {
    keyed(did, b"b/", &[did.as_bytes(), b"\0"])
}

pub fn private_key(did: &str, name: &str) -> Vec<u8> {
    keyed(did, b"p/", &[did.as_bytes(), b"\0", name.as_bytes()])
}

pub fn private_prefix(did: &str) -> Vec<u8> {
    keyed(did, b"p/", &[did.as_bytes(), b"\0"])
}

/// Keyed by the CID's digest alone: every node is dag-cbor sha-256.
/// Written and deleted in the commit's state batch, so `M/{did}` holds
/// exactly the interior nodes of the tree at `h/{did}`'s data root.
pub fn mst_node_key(did: &str, cid: &Cid) -> Vec<u8> {
    keyed(did, MST_NODE_FAMILY, &[did.as_bytes(), b"\0", &cid.digest])
}

pub fn mst_node_prefix(did: &str) -> Vec<u8> {
    keyed(did, MST_NODE_FAMILY, &[did.as_bytes(), b"\0"])
}

pub const MST_NODE_FAMILY: &[u8] = b"M/";

/// Set and cleared with `Account::pending_signing_key`, so recovery finds
/// pending rotations with one family scan instead of reading every account.
pub fn key_rotation_key(did: &str) -> Vec<u8> {
    keyed(did, KEY_ROTATION_FAMILY, &[did.as_bytes()])
}

pub const KEY_ROTATION_FAMILY: &[u8] = b"K/";

pub fn backlink_key(did: &str, link: &[u8]) -> Vec<u8> {
    keyed(did, b"bl/", &[did.as_bytes(), b"\0", link])
}

pub fn backlink_prefix(did: &str) -> Vec<u8> {
    keyed(did, b"bl/", &[did.as_bytes(), b"\0"])
}

pub fn record_prefix(did: &str) -> Vec<u8> {
    keyed(did, b"R/", &[did.as_bytes(), b"\0"])
}

pub fn record_key(did: &str, path: &str) -> Vec<u8> {
    keyed(did, b"R/", &[did.as_bytes(), b"\0", path.as_bytes()])
}

/// Bytes of a record CID's digest in its index key: enough to make
/// collisions rare (a lookup checks the record's CID anyway), short enough
/// to keep the extra key per record small.
const RECORD_CID_KEY_BYTES: usize = 8;

/// The same CID can sit at several paths (one key each), so lookups scan
/// [`record_cid_prefix`].
pub fn record_cid_key(did: &str, cid: &Cid, path: &str) -> Vec<u8> {
    [&record_cid_prefix(did, cid)[..], path.as_bytes()].concat()
}

pub fn record_cid_prefix(did: &str, cid: &Cid) -> Vec<u8> {
    keyed(did, b"c/", &[did.as_bytes(), b"\0", &cid.digest[..RECORD_CID_KEY_BYTES]])
}

const SCAN_BATCH: usize = 256;

/// `DbIterator::next_batch` skips the await (and tracing span) per row down
/// SlateDB's iterator stack. For scans read to (near) their end: it reads up
/// to a batch ahead of what the caller takes.
pub struct BatchedScan {
    iter: slatedb::DbIterator,
    rows: std::vec::IntoIter<slatedb::KeyValue>,
}

impl BatchedScan {
    pub fn new(iter: slatedb::DbIterator) -> BatchedScan {
        BatchedScan { iter, rows: Vec::new().into_iter() }
    }

    pub async fn next(&mut self) -> Result<Option<slatedb::KeyValue>, slatedb::Error> {
        if let Some(kv) = self.rows.next() {
            return Ok(Some(kv));
        }
        self.rows = self.iter.next_batch(SCAN_BATCH).await?.into_iter();
        Ok(self.rows.next())
    }

    pub fn next_buffered(&mut self) -> Option<slatedb::KeyValue> {
        self.rows.next()
    }
}

/// The keys of one family (or a narrower prefix such as
/// [`collection_family`]) across slots, in (slot, key) order. Slot-major
/// keys interleave families, so the scan seeks from the end of one slot's
/// run to the next slot's: an empty slot costs nothing (the seek lands on
/// the next key that exists). A shard's DB holds only its own slots (a
/// projection hides the rest), so no slot bounds are needed.
pub struct FamilyScan {
    iter: slatedb::DbIterator,
    fam: Vec<u8>,
}

impl FamilyScan {
    /// `start` is a full slot-major key, inclusive; None starts at slot 0.
    pub async fn new<R: slatedb::DbReadOps + ?Sized>(
        db: &R,
        fam: &[u8],
        start: Option<Vec<u8>>,
        opts: &slatedb::config::ScanOptions,
    ) -> Result<FamilyScan, slatedb::Error> {
        let lo = start.unwrap_or_else(|| slot_family(0, fam));
        let hi = vec![SLOT_TAG + 1];
        let iter = db.scan_with_options(lo..hi, opts).await?;
        Ok(FamilyScan { iter, fam: fam.to_vec() })
    }

    pub async fn next(&mut self) -> Result<Option<slatedb::KeyValue>, slatedb::Error> {
        loop {
            let Some(kv) = self.iter.next().await? else { return Ok(None) };
            let Some(slot) = key_slot(&kv.key) else { return Ok(None) };
            let body = key_body(&kv.key);
            if body.starts_with(&self.fam) {
                return Ok(Some(kv));
            }
            // this slot's run of the family is ahead (body < fam) or done
            let target = if body < self.fam.as_slice() {
                slot_family(slot, &self.fam)
            } else if slot == u16::MAX {
                return Ok(None);
            } else {
                slot_family(slot + 1, &self.fam)
            };
            self.iter.seek(target).await?;
        }
    }
}

/// (slot, DID) order key of a slot-major `family ‖ did` key (h/, a/, L/).
pub fn slot_did(key: &[u8], fam_len: usize) -> (&[u8], &[u8]) {
    (key.get(1..SLOT_PREFIX_LEN).unwrap_or_default(), key.get(SLOT_PREFIX_LEN + fam_len..).unwrap_or_default())
}

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

/// cid | rev of the commit that last wrote it (getRepo/listBlobs `since`) | bytes.
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

pub fn record_value_parts(v: &[u8]) -> anyhow::Result<(Cid, &[u8])> {
    anyhow::ensure!(v.len() >= CID_BYTES_LEN + 8, "short record value");
    Ok((Cid::from_bytes(&v[..CID_BYTES_LEN])?, &v[CID_BYTES_LEN + 8..]))
}

pub fn record_value_rev(v: &[u8]) -> u64 {
    v.get(CID_BYTES_LEN..CID_BYTES_LEN + 8).map(|b| u64::from_be_bytes(b.try_into().unwrap())).unwrap_or(0)
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Account {
    pub did: String,
    pub handle: String,
    /// Wrapped under the KEK (`secrets::Purpose::SigningKey`, bound to this
    /// DID): rows reach the log and SSTs as-is. Unwrap through
    /// `Secrets::account_signing_key` (cached).
    pub wrapped_signing_key: String,
    /// Multibase multikey, so readers that only need the public half never unwrap.
    pub signing_pubkey: String,
    pub password_hash: String,
    pub created_at: String,
    /// None = active; otherwise "deactivated" | "takendown" | "suspended" | "deleted".
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub email_confirmed: bool,
    /// Recorded before the DID document changes and cleared when the repo is
    /// re-signed with it or the rotation is abandoned; repo writes are
    /// refused meanwhile (DESIGN.md "Signing-key rotation").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_signing_key: Option<PendingSigningKey>,
    #[serde(default, flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PendingSigningKey {
    pub wrapped: String,
    pub pubkey: String,
}

/// As many Argon2 runs at once as there are pooled block buffers: each takes
/// ~20 ms of a core and 19 MiB, so more at once only adds memory and
/// blocking-pool threads.
static ARGON2_PERMITS: std::sync::LazyLock<tokio::sync::Semaphore> = std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(*ARGON2_POOL_MAX));

pub const ARGON2_MAX_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

/// Answer 503.
#[derive(Debug, thiserror::Error)]
#[error("password hashing is saturated; retry shortly")]
pub struct Argon2Busy;

async fn argon2_permit(wait: Option<std::time::Duration>) -> Result<tokio::sync::SemaphorePermit<'static>, Argon2Busy> {
    let acquire = ARGON2_PERMITS.acquire();
    let p = match wait {
        None => acquire.await,
        Some(w) => tokio::time::timeout(w, acquire).await.map_err(|_| Argon2Busy)?,
    };
    Ok(p.expect("the Argon2 semaphore is never closed"))
}

/// Saturates the pool until the guard drops (tests/argon2_shed.rs).
#[doc(hidden)]
pub async fn hold_all_argon2_permits() -> tokio::sync::SemaphorePermit<'static> {
    ARGON2_PERMITS.acquire_many(*ARGON2_POOL_MAX as u32).await.expect("the Argon2 semaphore is never closed")
}

/// Argon2id PHC string, OWASP baseline parameters. Waits for a permit.
pub async fn hash_password(password: &str) -> String {
    let _p = argon2_permit(None).await.expect("unbounded wait");
    let pw = password.to_string();
    tokio::task::spawn_blocking(move || hash_password_blocking(&pw)).await.expect("argon2 task")
}

/// Sheds ([`Argon2Busy`]) after [`ARGON2_MAX_WAIT`].
pub async fn try_hash_password(password: &str) -> Result<String, Argon2Busy> {
    let _p = argon2_permit(Some(ARGON2_MAX_WAIT)).await?;
    let pw = password.to_string();
    Ok(tokio::task::spawn_blocking(move || hash_password_blocking(&pw)).await.expect("argon2 task"))
}

pub fn hash_password_blocking(password: &str) -> String {
    use argon2::password_hash::{rand_core::OsRng, PasswordHasher, SaltString};
    let salt = SaltString::generate(&mut OsRng);
    PooledArgon2
        .hash_password_customized(password.as_bytes(), Some(argon2::Algorithm::Argon2id.ident()), Some(0x13), argon2_params(), &salt)
        .expect("argon2 hash")
        .to_string()
}

/// Sheds ([`Argon2Busy`]) after [`ARGON2_MAX_WAIT`].
pub async fn try_verify_password_hash(phc: &str, password: &str) -> Result<bool, Argon2Busy> {
    let _p = argon2_permit(Some(ARGON2_MAX_WAIT)).await?;
    Ok(verify_blocking(phc, password).await)
}

async fn verify_blocking(phc: &str, password: &str) -> bool {
    let (phc, pw) = (phc.to_string(), password.to_string());
    tokio::task::spawn_blocking(move || {
        use argon2::password_hash::{PasswordHash, PasswordVerifier};
        PasswordHash::new(&phc).is_ok_and(|h| PooledArgon2.verify_password(pw.as_bytes(), &h).is_ok())
    })
    .await
    .unwrap_or(false)
}

fn argon2_params() -> argon2::Params {
    argon2::Params::new(19 * 1024, 2, 1, None).expect("argon2 params")
}

/// Argon2 with its 19 MiB of block memory reused across hashes: a fresh
/// allocation per hash is a new mapping (page faults, zeroing) and an unmap,
/// which takes the process's mmap lock on Linux. Same PHC strings as
/// `argon2::Argon2` (`PasswordVerifier` is the blanket impl over this).
struct PooledArgon2;

/// One per core, at most 16 (~300 MiB): hashing is CPU-bound.
static ARGON2_MEMORY: parking_lot::Mutex<Vec<Vec<argon2::Block>>> = parking_lot::Mutex::new(Vec::new());
static ARGON2_POOL_MAX: std::sync::LazyLock<usize> =
    std::sync::LazyLock::new(|| std::thread::available_parallelism().map_or(8, |n| n.get()).min(16));

impl argon2::password_hash::PasswordHasher for PooledArgon2 {
    type Params = argon2::Params;

    fn hash_password_customized<'a>(
        &self,
        password: &[u8],
        alg_id: Option<argon2::password_hash::Ident<'a>>,
        version: Option<argon2::password_hash::Decimal>,
        params: argon2::Params,
        salt: impl Into<argon2::password_hash::Salt<'a>>,
    ) -> argon2::password_hash::Result<argon2::password_hash::PasswordHash<'a>> {
        let algorithm = alg_id.map(argon2::Algorithm::try_from).transpose()?.unwrap_or_default();
        let version = version.map(argon2::Version::try_from).transpose()?.unwrap_or_default();
        let salt = salt.into();
        let mut salt_arr = [0u8; 64];
        let salt_bytes = salt.decode_b64(&mut salt_arr)?;
        let ctx = argon2::Argon2::new(algorithm, version, params.clone());
        let blocks = params.block_count();
        let output = argon2::password_hash::Output::init_with(params.output_len().unwrap_or(argon2::Params::DEFAULT_OUTPUT_LEN), |out| {
            let mut mem = ARGON2_MEMORY.lock().pop().unwrap_or_default();
            if mem.len() < blocks {
                // every block is written before it is read
                mem.resize(blocks, argon2::Block::default());
            }
            let r = ctx.hash_password_into_with_memory(password, salt_bytes, out, &mut mem[..blocks]);
            let mut pool = ARGON2_MEMORY.lock();
            if pool.len() < *ARGON2_POOL_MAX {
                pool.push(mem);
            }
            Ok(r?)
        })?;
        Ok(argon2::password_hash::PasswordHash {
            algorithm: algorithm.ident(),
            version: Some(version.into()),
            params: argon2::password_hash::ParamsString::try_from(&params)?,
            salt: Some(salt),
            hash: Some(output),
        })
    }
}

/// Stable across processes and nodes: partition assignment must agree everywhere.
pub fn did_hash(did: &str) -> u64 {
    u64::from_be_bytes(Sha256::digest(did.as_bytes())[..8].try_into().unwrap())
}

/// Deterministic, so load generators can address bulk account `i` without a lookup.
pub fn bulk_did(i: u64) -> String {
    let h = Sha256::digest(format!("vlpds-bulk:{i}").as_bytes());
    format!("did:plc:{}", &crate::cid::base32_encode(&h)[..24])
}

pub fn bulk_handle(i: u64) -> String {
    format!("b{i}.bulk.vlpds.test")
}

#[cfg(test)]
mod tests {
    use super::*;
    use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};

    #[test]
    fn pooled_argon2_matches_argon2() {
        let stock = argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, argon2_params());
        for (i, pw) in ["hunter2", "", "correct horse battery staple"].iter().enumerate() {
            let salt = SaltString::encode_b64(&[i as u8 + 1; 16]).unwrap();
            let a = stock.hash_password(pw.as_bytes(), &salt).unwrap().to_string();
            let b = PooledArgon2
                .hash_password_customized(pw.as_bytes(), Some(argon2::Algorithm::Argon2id.ident()), Some(0x13), argon2_params(), &salt)
                .unwrap()
                .to_string();
            assert_eq!(a, b);
            let mine = hash_password_blocking(pw);
            assert!(stock.verify_password(pw.as_bytes(), &PasswordHash::new(&mine).unwrap()).is_ok());
            assert!(PooledArgon2.verify_password(pw.as_bytes(), &PasswordHash::new(&a).unwrap()).is_ok());
            assert!(PooledArgon2.verify_password(b"wrong", &PasswordHash::new(&a).unwrap()).is_err());
        }
        // a hash with other parameters (e.g. made before a cost change) still verifies
        let small = argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, argon2::Params::new(4096, 3, 1, None).unwrap());
        let salt = SaltString::encode_b64(&[9; 16]).unwrap();
        let h = small.hash_password(b"pw", &salt).unwrap().to_string();
        assert!(PooledArgon2.verify_password(b"pw", &PasswordHash::new(&h).unwrap()).is_ok());
    }

    #[tokio::test]
    async fn argon2_concurrency_is_bounded() {
        let held = ARGON2_PERMITS.acquire_many(*ARGON2_POOL_MAX as u32).await.unwrap();
        assert!(argon2_permit(Some(std::time::Duration::from_millis(20))).await.is_err());
        let phc = hash_password_blocking("pw");
        let waiting = tokio::spawn(async move {
            let _p = argon2_permit(None).await.unwrap();
            verify_blocking(&phc, "pw").await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!waiting.is_finished(), "verified without a permit");
        drop(held);
        assert!(waiting.await.unwrap());
    }
}
