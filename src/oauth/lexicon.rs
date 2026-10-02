//! Resolution of permission-set lexicons for `include:` scopes, via the
//! atproto lexicon resolution mechanism:
//!
//! 1. authority: DNS TXT `_lexicon.<reversed NSID authority>` -> `did=<DID>`
//! 2. DID document -> PDS endpoint + `#atproto` signing key
//! 3. `com.atproto.sync.getRecord` for `com.atproto.lexicon.schema/<nsid>`,
//!    verified end to end: block CIDs, commit signature, MST inclusion proof
//! 4. the record must be a lexicon whose `id` is the NSID and whose
//!    `defs.main` is a `permission-set`.
//!
//! Lexicons published by accounts on this PDS are read from local state (no
//! proof needed). Results are cached in memory for 5 minutes and persisted
//! (`oauth:lex:{nsid}`), so token refreshes keep working while a publisher is
//! temporarily unreachable (as the reference LexiconGetter does).
//!
//! The token endpoint ([`build_token_scope_cached`]) never waits on a
//! publisher it has a copy from: it uses the last good copy and re-resolves
//! a stale one in the background, so a slow or failing publisher can't
//! hold a code exchange or refresh open (and with it the window in which
//! it races a revocation). Only a set never seen before is resolved inline,
//! within [`INLINE_BUDGET`].

use super::scopes::{is_nsid, IncludeScope};
use super::store::{self, StoredLexicon};
use super::util::now_secs;
use crate::cbor::Value;
use crate::cid::Cid;
use crate::xrpc::App;
use serde_json::Value as J;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

const REFRESH: Duration = Duration::from_secs(300);
/// A background re-resolution that failed is retried after this.
const RETRY_AFTER: Duration = Duration::from_secs(30);
/// Longest the token endpoint waits for a permission set it has no copy of.
pub const INLINE_BUDGET: Duration = Duration::from_secs(3);
const LEXICON_COLLECTION: &str = "com.atproto.lexicon.schema";
const MAX_CAR_BYTES: usize = 1 << 20;

/// Permission sets by NSID, at most the `permission_sets` cap
/// ([`crate::caches`]); stale entries are the fallback while a publisher is
/// unreachable.
static CACHE: LazyLock<Arc<parking_lot::Mutex<HashMap<String, (Instant, J)>>>> =
    LazyLock::new(|| crate::caches::track(crate::caches::Cache::PermissionSets, Default::default()));
static OVERRIDES: LazyLock<parking_lot::Mutex<HashMap<String, String>>> =
    LazyLock::new(Default::default);
/// NSIDs being re-resolved in the background (one task per NSID).
static IN_FLIGHT: LazyLock<parking_lot::Mutex<std::collections::HashSet<String>>> =
    LazyLock::new(Default::default);
static DNS: LazyLock<Option<hickory_resolver::TokioResolver>> = LazyLock::new(|| {
    hickory_resolver::TokioResolver::builder_tokio()
        .ok()
        .map(|b| b.build())
});

/// Pins the lexicon authority DID for an NSID authority domain (e.g.
/// "example.com" for `com.example.*`), bypassing DNS. Used by tests and for
/// operator overrides; resolution of the DID and record proceeds normally.
pub fn override_authority(authority: &str, did: &str) {
    OVERRIDES
        .lock()
        .insert(authority.to_ascii_lowercase(), did.to_string());
}

/// The NSID authority domain: all segments but the name, reversed.
pub fn nsid_authority(nsid: &str) -> String {
    let segs: Vec<&str> = nsid.split('.').collect();
    segs[..segs.len().saturating_sub(1)]
        .iter()
        .rev()
        .cloned()
        .collect::<Vec<_>>()
        .join(".")
        .to_ascii_lowercase()
}

/// Returns the permission set (`defs.main`) for `nsid`.
pub async fn permission_set(app: &App, nsid: &str) -> Result<J, String> {
    if !is_nsid(nsid) {
        return Err(format!("invalid NSID {nsid}"));
    }
    if let Some((at, doc)) = CACHE.lock().get(nsid) {
        if at.elapsed() < REFRESH {
            return main_def(nsid, doc);
        }
    }
    match resolve(app, nsid).await {
        Ok((uri, doc)) => {
            main_def(nsid, &doc)?;
            cache_put(nsid, Instant::now(), doc.clone());
            let stored = StoredLexicon {
                uri,
                doc: doc.clone(),
                updated_at: now_secs(),
            };
            if let Err(e) = store::put_lexicon(app, nsid, &stored).await {
                tracing::warn!(nsid, "persisting lexicon failed: {}", e.description);
            }
            main_def(nsid, &doc)
        }
        Err(e) => {
            // Fall back to the last good copy (memory, then durable).
            if let Some((_, doc)) = CACHE.lock().get(nsid) {
                return main_def(nsid, doc);
            }
            if let Ok(Some(l)) = store::get_lexicon(app, nsid).await {
                cache_put(nsid, Instant::now() - REFRESH + Duration::from_secs(30), l.doc.clone());
                return main_def(nsid, &l.doc);
            }
            Err(e)
        }
    }
}

/// Caches `doc` for `nsid`; a full cache drops its stale entries, then all.
fn cache_put(nsid: &str, at: Instant, doc: J) {
    let cap = crate::caches::cap(crate::caches::Cache::PermissionSets);
    let mut m = CACHE.lock();
    if m.len() >= cap && !m.contains_key(nsid) {
        m.retain(|_, (at, _)| at.elapsed() < REFRESH);
        if m.len() >= cap {
            m.clear();
        }
    }
    m.insert(nsid.to_string(), (at, doc));
}

fn main_def(nsid: &str, doc: &J) -> Result<J, String> {
    if doc.get("lexicon").and_then(|v| v.as_i64()) != Some(1) {
        return Err(format!("Invalid Lexicon document for {nsid}"));
    }
    if doc.get("id").and_then(|v| v.as_str()) != Some(nsid) {
        return Err(format!("Invalid document id for {nsid}"));
    }
    let main = doc
        .get("defs")
        .and_then(|d| d.get("main"))
        .ok_or_else(|| format!("Lexicon {nsid} has no main definition"))?;
    if main.get("type").and_then(|v| v.as_str()) != Some("permission-set") {
        return Err(format!("Lexicon document is not a permission set: {nsid}"));
    }
    if !main.get("permissions").is_some_and(|p| p.is_array()) {
        return Err(format!("Invalid permission set {nsid}"));
    }
    Ok(main.clone())
}

/// Resolves and fetches the lexicon document for `nsid` (at-uri, doc),
/// uncached. Also used for record validation (crate::lexicon).
pub(crate) async fn resolve(app: &App, nsid: &str) -> Result<(String, J), String> {
    let did = resolve_authority(nsid).await?;
    let uri = format!("at://{did}/{LEXICON_COLLECTION}/{nsid}");
    let doc = fetch_record(app, &did, nsid)
        .await
        .map_err(|e| format!("Failed to fetch lexicon at {uri}: {e}"))?;
    Ok((uri, doc))
}

async fn resolve_authority(nsid: &str) -> Result<String, String> {
    let authority = nsid_authority(nsid);
    if let Some(d) = OVERRIDES.lock().get(&authority) {
        return Ok(d.clone());
    }
    let resolver = DNS.as_ref().ok_or("DNS resolver unavailable")?;
    let name = format!("_lexicon.{authority}.");
    let fail = |m: String| format!("Failed to resolve lexicon DID authority for {nsid}: {m}");
    let lookup = tokio::time::timeout(Duration::from_secs(5), resolver.txt_lookup(name.as_str()))
        .await
        .map_err(|_| fail("DNS timeout".into()))?
        .map_err(|e| fail(e.to_string()))?;
    let dids: Vec<String> = lookup
        .iter()
        .map(|txt| {
            txt.txt_data()
                .iter()
                .map(|c| String::from_utf8_lossy(c).into_owned())
                .collect::<String>()
        })
        .filter_map(|l| l.strip_prefix("did=").map(String::from))
        .collect();
    match dids.as_slice() {
        [d] if super::scopes::is_atproto_did(d) => Ok(d.clone()),
        [_] => Err(fail("invalid DID in DNS TXT record".into())),
        [] => Err(fail("No DID found in DNS TXT records".into())),
        _ => Err(fail("Multiple DIDs found in DNS TXT records".into())),
    }
}

async fn fetch_record(app: &App, did: &str, nsid: &str) -> Result<J, String> {
    let rpath = format!("{LEXICON_COLLECTION}/{nsid}");
    // Hosted here: read our own materialized state.
    if app.account(did).await.is_ok() {
        let p = app.partition(did).map_err(|e| e.message)?;
        let v =
            p.db.get(crate::state::record_key(did, &rpath))
                .await
                .map_err(|e| e.to_string())?
                .ok_or("Record not found")?;
        let (_, bytes) = crate::state::decode_record_value(&v).map_err(|e| e.to_string())?;
        let rec = Value::decode(&bytes).map_err(|e| e.to_string())?;
        return check_record_type(rec.to_json());
    }
    let doc = app
        .did_resolver
        .resolve(did)
        .await
        .map_err(|e| e.to_string())?;
    let pds = crate::did_resolver::service_endpoint(&doc, "atproto_pds")
        .ok_or("No atproto PDS service endpoint in DID document")?;
    let key = crate::did_resolver::signing_key_multibase(&doc)
        .ok_or("No atproto signing key in DID document")?;
    let url = format!(
        "{}/xrpc/com.atproto.sync.getRecord?did={}&collection={}&rkey={}",
        pds.trim_end_matches('/'),
        super::util::encode_uri_component(did),
        LEXICON_COLLECTION,
        super::util::encode_uri_component(nsid)
    );
    let car = fetch_bytes(&url, app.config.dev_mode).await?;
    verify_record_proof(&car, did, &key, &rpath)
}

fn check_record_type(rec: J) -> Result<J, String> {
    if rec.get("$type").and_then(|v| v.as_str()) != Some(LEXICON_COLLECTION) {
        return Err(format!(
            "Invalid record type: expected {LEXICON_COLLECTION}"
        ));
    }
    Ok(rec)
}

async fn fetch_bytes(url: &str, dev_mode: bool) -> Result<Vec<u8>, String> {
    use futures::StreamExt;
    let u = reqwest::Url::parse(url).map_err(|e| e.to_string())?;
    crate::did_resolver::check_outbound_url(&u, dev_mode)?;
    let resp = crate::http::guarded(dev_mode)
        .get(u)
        .header("accept", "application/vnd.ipld.car")
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("status {}", resp.status()));
    }
    let mut buf = Vec::new();
    let mut s = resp.bytes_stream();
    while let Some(c) = s.next().await {
        let c = c.map_err(|e| e.to_string())?;
        if buf.len() + c.len() > MAX_CAR_BYTES {
            return Err("response too large".into());
        }
        buf.extend_from_slice(&c);
    }
    Ok(buf)
}

/// Verifies a getRecord CAR: every block hashes to its CID, the root commit
/// is for `did` and signed by `key_multibase`, and the MST rooted at
/// `commit.data` maps `rpath` to the included record.
pub fn verify_record_proof(
    car: &[u8],
    did: &str,
    key_multibase: &str,
    rpath: &str,
) -> Result<J, String> {
    let (roots, blocks) = crate::car::read_car(car).map_err(|e| e.to_string())?;
    let root = *roots.first().ok_or("CAR has no root")?;
    let mut map: HashMap<Cid, Vec<u8>> = HashMap::new();
    for (c, data) in blocks {
        if Cid::dag_cbor(data) != c {
            return Err("block does not match its CID".into());
        }
        map.insert(c, data.to_vec());
    }
    let commit =
        Value::decode(map.get(&root).ok_or("missing commit block")?).map_err(|e| e.to_string())?;
    if commit.get("did").and_then(|v| v.as_str()) != Some(did) {
        return Err("Invalid repo did".into());
    }
    let Some(Value::Bytes(sig)) = commit.get("sig") else {
        return Err("commit is not signed".into());
    };
    let Value::Map(fields) = &commit else {
        return Err("invalid commit".into());
    };
    let unsigned = Value::Map(fields.iter().filter(|(k, _)| k != "sig").cloned().collect());
    if !verify_sig(key_multibase, &unsigned.to_cbor(), sig)? {
        return Err("Invalid signature on commit".into());
    }
    let Some(Value::Link(data)) = commit.get("data") else {
        return Err("commit has no data".into());
    };
    let tree = crate::mst::Tree::load_from_blocks(&map, *data).map_err(|e| format!("{e:?}"))?;
    let rcid = tree
        .get(rpath.as_bytes())
        .map_err(|e| format!("{e:?}"))?
        .ok_or("Record not found in proof")?;
    let rec =
        Value::decode(map.get(&rcid).ok_or("record block missing")?).map_err(|e| e.to_string())?;
    check_record_type(rec.to_json())
}

/// atproto multikey (secp256k1 or P-256, compressed) signature check, for
/// commits and records: compact 64-byte signatures, low-S only (a high-S
/// signature is Ok(false), as in the reference's default verification).
pub(crate) fn verify_sig(multibase: &str, msg: &[u8], sig: &[u8]) -> Result<bool, String> {
    verify_multikey(multibase, msg, sig, false)
}

/// [`verify_sig`] that also accepts high-S signatures: inter-service JWTs
/// only (the reference verifies them with `allowMalleableSig: true`).
pub(crate) fn verify_sig_malleable(multibase: &str, msg: &[u8], sig: &[u8]) -> Result<bool, String> {
    verify_multikey(multibase, msg, sig, true)
}

fn verify_multikey(multibase: &str, msg: &[u8], sig: &[u8], allow_high_s: bool) -> Result<bool, String> {
    let raw = bs58::decode(multibase.strip_prefix('z').ok_or("unsupported multibase")?)
        .into_vec()
        .map_err(|e| e.to_string())?;
    match raw.as_slice() {
        [0xe7, 0x01, key @ ..] => if allow_high_s {
            crate::crypto::verify_k256_malleable(key, msg, sig)
        } else {
            crate::crypto::verify_k256(key, msg, sig)
        }
        .map_err(|e| e.to_string()),
        [0x80, 0x24, key @ ..] => {
            use p256::ecdsa::signature::Verifier;
            let vk = p256::ecdsa::VerifyingKey::from_sec1_bytes(key).map_err(|e| e.to_string())?;
            let s = p256::ecdsa::Signature::from_slice(sig).map_err(|e| e.to_string())?;
            // `p256` itself accepts both forms; Some = it was high-S
            let s = match s.normalize_s() {
                Some(_) if !allow_high_s => return Ok(false),
                Some(low) => low,
                None => s,
            };
            Ok(vk.verify(msg, &s).is_ok())
        }
        _ => Err("unsupported key type".into()),
    }
}

/// [`permission_set`] for the token endpoint: the last good copy (memory,
/// then durable) at once, re-resolved in the background when stale; only a
/// set with no copy anywhere is resolved inline, within [`INLINE_BUDGET`].
async fn permission_set_cached(app: &Arc<App>, nsid: &str) -> Result<J, String> {
    if !is_nsid(nsid) {
        return Err(format!("invalid NSID {nsid}"));
    }
    let cached = CACHE.lock().get(nsid).map(|(at, doc)| (at.elapsed() >= REFRESH, doc.clone()));
    let cached = match cached {
        Some(c) => Some(c),
        None => match store::get_lexicon(app, nsid).await {
            Ok(Some(l)) => {
                cache_put(nsid, Instant::now() - REFRESH, l.doc.clone());
                Some((true, l.doc))
            }
            _ => None,
        },
    };
    match cached {
        Some((stale, doc)) => {
            if stale {
                refresh_in_background(app, nsid);
            }
            main_def(nsid, &doc)
        }
        None => tokio::time::timeout(INLINE_BUDGET, permission_set(app, nsid))
            .await
            .map_err(|_| format!("Timed out resolving permission set {nsid}"))?,
    }
}

/// Re-resolves `nsid` in a background task (unless one is running); a
/// failure keeps the stale copy and is retried after [`RETRY_AFTER`].
fn refresh_in_background(app: &Arc<App>, nsid: &str) {
    if !IN_FLIGHT.lock().insert(nsid.to_string()) {
        return;
    }
    let (app, nsid) = (app.clone(), nsid.to_string());
    tokio::spawn(async move {
        let _ = permission_set(&app, &nsid).await;
        {
            // still stale = the resolution failed (permission_set fell back
            // to the old copy): back off instead of retrying on every call
            let mut m = CACHE.lock();
            if let Some((at, _)) = m.get_mut(&nsid) {
                if at.elapsed() >= REFRESH {
                    *at = Instant::now() - REFRESH + RETRY_AFTER;
                }
            }
        }
        IN_FLIGHT.lock().remove(&nsid);
    });
}

/// [`build_token_scope`] from [`permission_set_cached`] sets (the token
/// endpoint: bounded time).
pub async fn build_token_scope_cached(app: &Arc<App>, scope: &str) -> Result<String, String> {
    if !scope.split(' ').any(|s| IncludeScope::parse(s).is_some()) {
        return Ok(scope.to_string());
    }
    let mut out: Vec<String> = Vec::new();
    let mut others: Vec<String> = Vec::new();
    for s in scope.split(' ') {
        match IncludeScope::parse(s) {
            Some(inc) => {
                let set = permission_set_cached(app, &inc.nsid).await?;
                out.extend(inc.to_permissions(&set).iter().map(|p| p.to_scope_string()));
            }
            None => others.push(s.to_string()),
        }
    }
    out.extend(others);
    Ok(out.join(" "))
}

/// Every permission set referenced by `include:` scopes in `scope`.
pub async fn permission_sets_for_scope(
    app: &App,
    scope: &str,
) -> Result<Vec<(IncludeScope, J)>, String> {
    let mut out = Vec::new();
    for s in scope.split(' ') {
        if let Some(inc) = IncludeScope::parse(s) {
            let set = permission_set(app, &inc.nsid).await?;
            out.push((inc, set));
        }
    }
    Ok(out)
}

/// Token scope: `include:` scopes replaced by the repo/rpc permissions their
/// permission sets grant (`LexiconManager.buildTokenScope`).
pub async fn build_token_scope(app: &App, scope: &str) -> Result<String, String> {
    if !scope.split(' ').any(|s| IncludeScope::parse(s).is_some()) {
        return Ok(scope.to_string());
    }
    let mut out: Vec<String> = Vec::new();
    let mut others: Vec<String> = Vec::new();
    for s in scope.split(' ') {
        match IncludeScope::parse(s) {
            Some(inc) => {
                let set = permission_set(app, &inc.nsid).await?;
                out.extend(inc.to_permissions(&set).iter().map(|p| p.to_scope_string()));
            }
            None => others.push(s.to_string()),
        }
    }
    out.extend(others);
    Ok(out.join(" "))
}

#[cfg(test)]
mod tests {
    /// Record proofs (commit signatures) take low-S only; service-auth JWTs
    /// also take the high-S form, for both curves.
    #[test]
    fn signature_malleability() {
        let msg = b"signed bytes";
        // P-256
        let sk = p256::ecdsa::SigningKey::random(&mut rand::rngs::OsRng);
        let mut mk = vec![0x80, 0x24];
        mk.extend_from_slice(sk.verifying_key().to_encoded_point(true).as_bytes());
        let p256_key = format!("z{}", bs58::encode(mk).into_string());
        let sig: p256::ecdsa::Signature = p256::ecdsa::signature::Signer::sign(&sk, msg);
        let low = sig.normalize_s().unwrap_or(sig);
        let high = p256::ecdsa::Signature::from_scalars(low.r(), -*low.s()).unwrap();
        assert!(high.normalize_s().is_some(), "high-S form");
        for (key, low, high) in [
            (p256_key, low.to_bytes().to_vec(), high.to_bytes().to_vec()),
            {
                // K-256
                let kp = crate::crypto::Keypair::generate();
                let low = kp.sign(msg);
                let s = k256::ecdsa::Signature::from_slice(&low).unwrap();
                let high = k256::ecdsa::Signature::from_scalars(s.r(), -*s.s()).unwrap();
                (kp.public_multibase(), low.to_vec(), high.to_bytes().to_vec())
            },
        ] {
            assert_eq!(super::verify_sig(&key, msg, &low), Ok(true));
            assert_eq!(super::verify_sig(&key, msg, &high), Ok(false), "record proofs reject high-S");
            assert_eq!(super::verify_sig_malleable(&key, msg, &low), Ok(true));
            assert_eq!(super::verify_sig_malleable(&key, msg, &high), Ok(true), "service auth tolerates high-S");
            assert_eq!(super::verify_sig_malleable(&key, b"other", &high), Ok(false));
            assert_eq!(super::verify_sig(&key, b"other", &low), Ok(false));
        }
    }

    #[test]
    fn authority() {
        assert_eq!(super::nsid_authority("app.bsky.feed.post"), "feed.bsky.app");
        assert_eq!(
            super::nsid_authority("com.example.authBasic"),
            "example.com"
        );
    }
}
