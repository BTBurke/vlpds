//! AT Protocol Spaces (permissioned data), tracking the reference's alpha
//! (bluesky-social/atproto PR #5187; pinned commit in
//! testdata/spaces-alpha/SOURCE). This holds the protocol primitives only:
//! pure, no I/O, each checked against vectors from the reference.
//!
//! - [`lthash`]: the space repo's homomorphic set hash.
//! - [`commit`]: the deniable commit (ctx, MAC, signature).
//! - [`httpsig`]: the RFC 9421 subset requests carry with a delegation
//!   token or a space credential.
//! - [`token`]: delegation tokens, space credentials, client attestations.
//!
//! Serving is behind `--spaces` (`Config::spaces`), off by default; with it
//! on, a Spaces NSID without a handler answers 501 rather than reaching the
//! `atproto-proxy` fallback. The serving side:
//!
//! - [`rows`]: the values of the `s*` state families (keys in `crate::state`).
//! - [`repo`]: space writes and space host ops on the repo worker.
//! - [`heads`]: durable space repo heads for reads.
//! - [`outbox`]: delivery of notifyWrite to space authorities.
//! - [`revocations`]: revoked credentials.
//! - [`credcache`]: verified credentials, until they expire.

pub mod commit;
pub mod credcache;
pub mod heads;
pub mod httpsig;
pub mod lthash;
pub mod outbox;
pub mod repo;
pub mod revocations;
pub mod rows;
mod sfv;
pub mod token;

use std::sync::Arc;

/// A node's Spaces state (`App::spaces`, `Node::spaces`), with `--spaces`.
pub struct Spaces {
    pub heads: heads::Heads,
    pub outbox: Arc<outbox::Outbox>,
    pub revocations: revocations::Revocations,
    pub credentials: credcache::CredCache,
}

fn now_secs() -> i64 {
    crate::tid::now_micros() as i64 / 1_000_000
}

impl Spaces {
    pub fn new() -> Spaces {
        Spaces {
            heads: heads::Heads::new(heads::DEFAULT_HEADS_BYTES),
            outbox: Default::default(),
            revocations: Default::default(),
            credentials: credcache::CredCache::new(credcache::DEFAULT_ENTRIES),
        }
    }

    /// Re-reads the revocations object; cached credentials it newly
    /// revokes are dropped.
    pub async fn refresh_revocations(&self, store: &crate::store::Store) -> anyhow::Result<()> {
        let added = self.revocations.refresh(store, now_secs()).await?;
        self.credentials.invalidate(&added);
        Ok(())
    }

    /// Revokes `jtis` of `space` cluster-wide (durable on return; peers
    /// learn of it by a nudge or their next re-read).
    pub async fn revoke(&self, store: &crate::store::Store, space: &str, jtis: &[String]) -> anyhow::Result<()> {
        let added = self.revocations.revoke(store, space, jtis, now_secs()).await?;
        self.credentials.invalidate(&added);
        Ok(())
    }

    /// Loads the revocations (credential reads answer 503 until a load
    /// succeeds), then keeps them fresh in the background: every
    /// [`revocations::REFRESH_EVERY`], or at once when woken by a peer.
    pub async fn start_revocations(self: &Arc<Self>, store: crate::store::Store) {
        if self.revocations.started.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return;
        }
        let mut ok = match self.refresh_revocations(&store).await {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!("space revocations unreadable (credential reads wait for them): {e:#}");
                false
            }
        };
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let wait = if ok { revocations::REFRESH_EVERY } else { revocations::RETRY_EVERY };
                let Some(wake) = weak.upgrade().map(|me| me.revocations.wake.clone()) else { return };
                let _ = tokio::time::timeout(wait, wake.notified()).await;
                let Some(me) = weak.upgrade() else { return };
                ok = match me.refresh_revocations(&store).await {
                    Ok(()) => true,
                    Err(e) => {
                        tracing::warn!("space revocations re-read failed (retrying): {e:#}");
                        false
                    }
                };
            }
        });
    }

    /// An account deleted: nothing of it is served or sent any more.
    pub fn forget_account(&self, did: &str) {
        self.heads.drop_did(did);
        self.outbox.drop_did(did);
    }

    /// Enqueues the `sP` rows of shards just opened (a start, a takeover, a
    /// handback, a split's children): notifies this node now owes. In the
    /// background; a row found twice is enqueued once.
    pub fn spawn_outbox_rescan(self: Arc<Self>, shards: Vec<(crate::slots::ShardId, Arc<slatedb::Db>)>) {
        if shards.is_empty() {
            return;
        }
        tokio::spawn(async move {
            for (shard, db) in shards {
                match self.rescan_outbox(&db).await {
                    Ok(n) if n > 0 => tracing::info!(shard = shard.0, rows = n, "space notify outbox resumed"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(shard = shard.0, "space notify outbox rescan failed: {e:#}"),
                }
            }
        });
    }

    async fn rescan_outbox(&self, db: &slatedb::Db) -> anyhow::Result<usize> {
        let opts = slatedb::config::ScanOptions::default();
        let mut scan = crate::state::FamilyScan::new(db, crate::state::SPACE_OUTBOX_FAMILY, None, &opts).await?;
        let mut n = 0;
        while let Some(kv) = scan.next().await? {
            let Some((did, sid)) = rows::did_sid(&kv.key) else { continue };
            let row = rows::OutboxRow::decode(&kv.value)?;
            anyhow::ensure!(crate::state::space_id(&row.uri) == sid, "outbox row of {did} names another space");
            self.outbox.enqueue(did, sid, &row.uri, row.repo_rev, row.hash, false);
            n += 1;
        }
        Ok(n)
    }
}

impl Default for Spaces {
    fn default() -> Self {
        Spaces::new()
    }
}

/// The XRPC methods of the Spaces lexicons. NSID authorities are
/// case-insensitive, as the proxy's method lists match them.
pub fn is_space_nsid(nsid: &str) -> bool {
    ["com.atproto.space.", "com.atproto.simplespace."]
        .iter()
        .any(|p| nsid.get(..p.len()).is_some_and(|h| h.eq_ignore_ascii_case(p)))
}

/// A did:key's JWT `alg` (@atproto/crypto `parseDidKey`): ES256K for
/// secp256k1, ES256 for P-256.
pub fn did_key_alg(did_key: &str) -> Option<&'static str> {
    let raw = bs58::decode(did_key.strip_prefix("did:key:z")?).into_vec().ok()?;
    match raw.as_slice() {
        [0xe7, 0x01, ..] => Some("ES256K"),
        [0x80, 0x24, ..] => Some("ES256"),
        _ => None,
    }
}

/// A compact (r‖s, 64-byte) ECDSA signature over sha256(msg) by `did_key`;
/// DER is refused. High-S only with `allow_high_s` (@atproto/crypto
/// `allowMalleableSig`). Err: not a usable did:key or signature encoding.
fn verify_did_key(did_key: &str, msg: &[u8], sig: &[u8], allow_high_s: bool) -> Result<bool, String> {
    if sig.len() != 64 {
        return Err("signature must be 64 bytes".into());
    }
    let multibase = did_key.strip_prefix("did:key:").ok_or("not a did:key")?;
    // libsecp256k1 also parses hybrid (0x06/0x07) points; the reference doesn't
    let raw = bs58::decode(multibase.strip_prefix('z').ok_or("unsupported multibase")?)
        .into_vec()
        .map_err(|e| e.to_string())?;
    let key = raw.get(2..).unwrap_or_default();
    if !matches!((key.len(), key.first()), (33, Some(2 | 3)) | (65, Some(4))) {
        return Err("unsupported public key encoding".into());
    }
    if allow_high_s {
        crate::oauth::lexicon::verify_sig_malleable(multibase, msg, sig)
    } else {
        crate::oauth::lexicon::verify_sig(multibase, msg, sig)
    }
}

#[cfg(test)]
pub(crate) mod vectors {
    use std::sync::LazyLock;

    pub static VECTORS: LazyLock<serde_json::Value> = LazyLock::new(|| {
        serde_json::from_str(include_str!("../../testdata/spaces-alpha/vectors.json")).expect("vectors.json")
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn space_nsids() {
        for yes in ["com.atproto.space.getRecord", "com.atproto.simplespace.createSpace", "COM.ATPROTO.Space.x"] {
            assert!(is_space_nsid(yes), "{yes}");
        }
        for no in ["com.atproto.repo.getRecord", "com.atproto.spaces.x", "com.atproto.space", "app.bsky.space.x", ""] {
            assert!(!is_space_nsid(no), "{no}");
        }
    }

    #[test]
    fn did_key_algs() {
        let k256 = crate::crypto::Keypair::generate().did_key();
        assert_eq!(did_key_alg(&k256), Some("ES256K"));
        let p256 = vectors::VECTORS["httpsig"][0]["keyDid"].as_str().unwrap();
        assert_eq!(did_key_alg(p256), Some("ES256"));
        assert_eq!(did_key_alg("did:key:zQ3"), None);
        assert_eq!(did_key_alg("did:plc:abc"), None);
        assert!(verify_did_key("did:plc:abc", b"x", &[0; 64], false).is_err());
        assert!(verify_did_key(&k256, b"x", &[0; 70], false).is_err());
        // a hybrid-encoded point (0x06/0x07 prefix) of a real key
        let key = crate::crypto::Keypair::generate();
        let sig = key.sign(b"x");
        let pk = secp256k1::PublicKey::from_slice(&key.public_key_sec1()).unwrap().serialize_uncompressed();
        let mut hybrid = vec![0xe7, 0x01, 6 | (pk[64] & 1)];
        hybrid.extend_from_slice(&pk[1..]);
        let hybrid = format!("did:key:z{}", bs58::encode(hybrid).into_string());
        assert!(
            crate::oauth::lexicon::verify_sig(hybrid.strip_prefix("did:key:").unwrap(), b"x", &sig).is_ok_and(|v| v)
        );
        assert!(verify_did_key(&hybrid, b"x", &sig, false).is_err());
        assert_eq!(verify_did_key(&key.did_key(), b"x", &sig, false), Ok(true));
    }
}
