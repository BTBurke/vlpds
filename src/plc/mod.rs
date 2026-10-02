//! PLC identity (DESIGN.md "PLC identity"): did:plc operations, the PLC
//! directory client, and the server's PLC rotation key.
//!
//! Operations follow did-method-plc (`@did-plc/lib` operations.ts / data.ts,
//! which the reference PDS uses) byte for byte:
//! - an operation is DAG-CBOR encoded (canonical map key order) and signed
//!   with a rotation key: ES256K (or ES256 for a P-256 key) over the
//!   encoding of the op without `sig`, compact 64-byte low-S, base64url
//!   without padding in `sig`;
//! - a DID is `did:plc:` + the first 24 chars of base32(sha256(DAG-CBOR of
//!   the signed genesis op));
//! - an update's `prev` is the CID (dag-cbor, sha2-256, base32 CIDv1) of the
//!   op it follows, and it must be signed by one of that op's rotation keys
//!   (a fork within 72 h needs a higher-priority key: [`PlcLog::apply`]).
//!
//! The server holds one rotation key ([`Plc`]; the reference's
//! `PDS_PLC_ROTATION_KEY_K256_PRIVATE_KEY_HEX`), signs every genesis op with
//! it, and submits ops to `--plc-url` before acknowledging the change. The
//! key comes from a flag/env (hex) or a file holding it wrapped under the
//! secrets-at-rest KEK (src/secrets.rs; Cloud KMS in production); it is never
//! written to the bucket.
//!
//! [`mock`] is an in-process PLC directory for tests and local runs.

use crate::cid::Cid;
use crate::crypto::Keypair;
use base64::Engine;
use prometheus::{register_histogram_vec, register_int_counter_vec, HistogramVec, IntCounterVec};
use serde_json::{json, Map, Value as J};
use sha2::{Digest, Sha256};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

#[doc(hidden)]
pub mod mock;

pub const DEFAULT_PLC_URL: &str = "https://plc.directory";
/// One PLC directory request (connect + response) gives up after this.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Authenticated-data subject of the wrapped rotation key
/// (`secrets::Purpose::PlcRotationKey`).
pub const ROTATION_KEY_SUBJECT: &str = "pds";
/// The PLC directory's limits on an incoming operation (did-method-plc
/// server constraints.ts).
pub const MAX_OP_BYTES: usize = 4000;
const MAX_AKA_ENTRIES: usize = 10;
const MAX_AKA_LENGTH: usize = 258;
const MAX_ROTATION_ENTRIES: usize = 10;
const MAX_SERVICE_ENTRIES: usize = 10;
const MAX_SERVICE_TYPE_LENGTH: usize = 256;
const MAX_SERVICE_ENDPOINT_LENGTH: usize = 512;
const MAX_VERIFICATION_METHOD_ENTRIES: usize = 10;
const MAX_ID_LENGTH: usize = 32;
const MAX_DID_KEY_LENGTH: usize = 256;
/// A rotation key may rewrite history signed by a lower-priority key for
/// this long (PLC spec "recovery").
pub const RECOVERY_WINDOW_MS: i64 = 72 * 3600 * 1000;
/// Largest PLC directory response read.
const MAX_RESPONSE_BYTES: usize = 256 << 10;

static PLC_REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "vlpds_plc_requests_total",
        "PLC directory requests by op (create, update_handle, update_signing_key, submit, tombstone, rotate_key, get_last_op, get_data) and result (ok, rejected: 4xx; unavailable: 5xx, timeout, connection; not_found)",
        &["op", "result"]
    )
    .unwrap()
});
static PLC_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "vlpds_plc_request_seconds",
        "PLC directory request latency by op",
        &["op"],
        prometheus::exponential_buckets(0.005, 2.0, 12).unwrap()
    )
    .unwrap()
});

/// Ops that write to the directory (their failure alerts).
pub const WRITE_OPS: [&str; 6] = ["create", "update_handle", "update_signing_key", "submit", "tombstone", "rotate_key"];

/// Exports the write ops' counters at 0, so `increase()` sees the first
/// failure (called at startup when PLC registration is on).
pub fn touch_metrics() {
    for op in WRITE_OPS {
        for r in ["ok", "rejected", "unavailable"] {
            PLC_REQUESTS.with_label_values(&[op, r]);
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum PlcError {
    /// A malformed or invalid operation (ours or a client's).
    #[error("{0}")]
    Invalid(String),
    /// The directory refused the operation (HTTP 4xx).
    #[error("PLC directory rejected the operation (HTTP {status}): {message}")]
    Rejected { status: u16, message: String },
    /// The directory could not be reached or failed (5xx, timeout).
    #[error("PLC directory unavailable: {0}")]
    Unavailable(String),
    #[error("DID not registered with the PLC directory: {0}")]
    NotFound(String),
    #[error("Did is tombstoned")]
    Tombstoned,
    #[error(transparent)]
    Signature(#[from] crate::crypto::SignatureFault),
}

fn invalid(m: impl Into<String>) -> PlcError {
    PlcError::Invalid(m.into())
}

impl From<PlcError> for crate::xrpc::XrpcError {
    /// Client mistakes are 400 InvalidRequest. A directory that refuses or
    /// fails is a 500 InternalServerError, as in the reference (its
    /// `PlcClientError` is not an XRPC error), with the directory's reason.
    fn from(e: PlcError) -> crate::xrpc::XrpcError {
        match e {
            PlcError::Invalid(m) => crate::xrpc::XrpcError::bad("InvalidRequest", m),
            PlcError::Tombstoned => crate::xrpc::XrpcError::bad("InvalidRequest", "Did is tombstoned"),
            PlcError::Signature(f) => f.into(),
            e => crate::xrpc::XrpcError::internal(e.to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// encoding
// ---------------------------------------------------------------------------

/// DAG-CBOR of a JSON operation (maps in canonical key order; strings,
/// arrays, null, bools and integers only, as PLC operations hold).
pub fn dag_cbor(v: &J) -> Result<Vec<u8>, PlcError> {
    let mut out = Vec::with_capacity(512);
    encode(v, &mut out, 0)?;
    Ok(out)
}

fn encode(v: &J, out: &mut Vec<u8>, depth: usize) -> Result<(), PlcError> {
    use crate::cbor::*;
    if depth > 16 {
        return Err(invalid("operation nested too deeply"));
    }
    match v {
        J::Null => write_null(out),
        J::Bool(b) => write_bool(out, *b),
        J::Number(n) => match (n.as_u64(), n.as_i64()) {
            (Some(u), _) => write_uint(out, u),
            (None, Some(i)) => write_int(out, i),
            _ => return Err(invalid("floats are not allowed in an operation")),
        },
        J::String(s) => write_text(out, s),
        J::Array(a) => {
            write_array_head(out, a.len());
            for x in a {
                encode(x, out, depth + 1)?;
            }
        }
        J::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort_by(|a, b| key_cmp(a, b));
            write_map_head(out, m.len());
            for k in keys {
                write_text(out, k);
                encode(&m[k], out, depth + 1)?;
            }
        }
    }
    Ok(())
}

/// The CID of an operation (signed, as stored in the log): `prev` of the
/// next one.
pub fn op_cid(op: &J) -> Result<Cid, PlcError> {
    Ok(Cid::dag_cbor(&dag_cbor(op)?))
}

/// `did:plc:` + base32(sha256(signed genesis op))[..24] (`didForCreateOp`).
pub fn did_for_genesis(op: &J) -> Result<String, PlcError> {
    let h = Sha256::digest(dag_cbor(op)?);
    Ok(format!("did:plc:{}", &crate::cid::base32_encode(&h)[..24]))
}

/// The shape of a did:plc identifier (path-safe).
pub fn valid_plc_did(did: &str) -> bool {
    did.strip_prefix("did:plc:")
        .is_some_and(|id| id.len() == 24 && id.bytes().all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b)))
}

// ---------------------------------------------------------------------------
// shapes (the zod schemas of @did-plc/lib types.ts, strict)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpType {
    /// `plc_operation`
    Operation,
    /// `plc_tombstone`
    Tombstone,
    /// The legacy v1 genesis `create` (genesis only).
    LegacyCreate,
}

fn is_str(v: Option<&J>) -> bool {
    matches!(v, Some(J::String(_)))
}

fn str_array(v: Option<&J>) -> bool {
    matches!(v, Some(J::Array(a)) if a.iter().all(|x| x.is_string()))
}

fn only_keys(m: &Map<String, J>, allowed: &[&str]) -> bool {
    m.keys().all(|k| allowed.contains(&k.as_str())) && m.len() == allowed.len()
}

/// The operation's type if `op` has exactly the fields of one (with `sig`
/// iff `signed`).
pub fn op_type(op: &J, signed: bool) -> Result<OpType, PlcError> {
    let bad = || invalid("Invalid operation");
    let m = op.as_object().ok_or_else(bad)?;
    let sig: &[&str] = if signed { &["sig"] } else { &[] };
    let with = |fields: &[&'static str]| -> Vec<&str> { fields.iter().copied().chain(sig.iter().copied()).collect() };
    if signed && !is_str(m.get("sig")) {
        return Err(bad());
    }
    match m.get("type").and_then(J::as_str) {
        Some("plc_operation") => {
            let ok = only_keys(m, &with(&["type", "rotationKeys", "verificationMethods", "alsoKnownAs", "services", "prev"]))
                && str_array(m.get("rotationKeys"))
                && str_array(m.get("alsoKnownAs"))
                && matches!(m.get("verificationMethods"), Some(J::Object(v)) if v.values().all(J::is_string))
                && matches!(m.get("services"), Some(J::Object(s)) if s.values().all(|s| {
                    matches!(s, J::Object(o) if only_keys(o, &["type", "endpoint"]) && is_str(o.get("type")) && is_str(o.get("endpoint")))
                }))
                && matches!(m.get("prev"), Some(J::String(_) | J::Null));
            ok.then_some(OpType::Operation).ok_or_else(bad)
        }
        Some("plc_tombstone") => (only_keys(m, &with(&["type", "prev"])) && is_str(m.get("prev"))).then_some(OpType::Tombstone).ok_or_else(bad),
        Some("create") => {
            let ok = only_keys(m, &with(&["type", "signingKey", "recoveryKey", "handle", "service", "prev"]))
                && ["signingKey", "recoveryKey", "handle", "service"].iter().all(|k| is_str(m.get(*k)))
                && m.get("prev") == Some(&J::Null);
            ok.then_some(OpType::LegacyCreate).ok_or_else(bad)
        }
        _ => Err(bad()),
    }
}

/// A legacy `create` as the equivalent `plc_operation` (`normalizeOp`);
/// other ops unchanged.
pub fn normalize(op: &J) -> J {
    if op["type"] != "create" {
        return op.clone();
    }
    let mut n = json!({
        "type": "plc_operation",
        "verificationMethods": {"atproto": op["signingKey"]},
        "rotationKeys": [op["recoveryKey"], op["signingKey"]],
        "alsoKnownAs": [ensure_atproto_prefix(op["handle"].as_str().unwrap_or(""))],
        "services": {"atproto_pds": {"type": "AtprotoPersonalDataServer", "endpoint": ensure_http_prefix(op["service"].as_str().unwrap_or(""))}},
        "prev": op["prev"],
    });
    if let Some(s) = op.get("sig") {
        n["sig"] = s.clone();
    }
    n
}

/// The strings of an op's `rotationKeys` (normalized).
pub fn rotation_keys(op: &J) -> Vec<String> {
    normalize(op)["rotationKeys"].as_array().map(|a| a.iter().filter_map(|k| k.as_str().map(str::to_string)).collect()).unwrap_or_default()
}

pub fn ensure_http_prefix(s: &str) -> String {
    if s.starts_with("http://") || s.starts_with("https://") {
        s.to_string()
    } else {
        format!("https://{s}")
    }
}

/// `at://` + the handle (JS `replace` drops the first `http://` and the
/// first `https://`).
pub fn ensure_atproto_prefix(s: &str) -> String {
    if s.starts_with("at://") {
        return s.to_string();
    }
    format!("at://{}", s.replacen("http://", "", 1).replacen("https://", "", 1))
}

/// The unsigned atproto operation (`formatAtprotoOp`).
pub fn format_atproto_op(signing_key: &str, handle: &str, pds: &str, rotation_keys: &[String], prev: Option<&str>) -> J {
    json!({
        "type": "plc_operation",
        "verificationMethods": {"atproto": signing_key},
        "rotationKeys": rotation_keys,
        "alsoKnownAs": [ensure_atproto_prefix(handle)],
        "services": {"atproto_pds": {"type": "AtprotoPersonalDataServer", "endpoint": ensure_http_prefix(pds)}},
        "prev": prev,
    })
}

/// Whether `k` is a did:key this PDS can verify (secp256k1 or P-256,
/// compressed).
pub fn valid_did_key(k: &str) -> bool {
    let Some(mb) = k.strip_prefix("did:key:z") else { return false };
    match bs58::decode(mb).into_vec() {
        Ok(raw) => matches!(raw.as_slice(), [0xe7, 0x01, rest @ ..] | [0x80, 0x24, rest @ ..] if rest.len() == 33),
        Err(_) => false,
    }
}

// ---------------------------------------------------------------------------
// signatures
// ---------------------------------------------------------------------------

/// Signs an unsigned operation (no `sig`) with `key`: hedged, low-S, and
/// verified before it is returned (`Keypair::sign_verified`).
pub fn sign(unsigned: J, key: &Keypair) -> Result<J, PlcError> {
    let J::Object(mut m) = unsigned else { return Err(invalid("operation must be an object")) };
    if m.contains_key("sig") {
        return Err(invalid("operation is already signed"));
    }
    let bytes = dag_cbor(&J::Object(m.clone()))?;
    let sig = key.sign_verified(crate::crypto::Purpose::PlcOperation, &bytes)?;
    m.insert("sig".into(), J::String(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig)));
    Ok(J::Object(m))
}

/// The did:key in `allowed` whose signature `op` carries (`assureValidSig`):
/// base64url without padding (strict: no `=`, no stray bits, no
/// whitespace), compact 64-byte low-S ECDSA over the op without `sig`.
pub fn verify_sig(allowed: &[String], op: &J) -> Result<String, PlcError> {
    let bad = || invalid("Invalid signature on op");
    let m = op.as_object().ok_or_else(bad)?;
    let sig = m.get("sig").and_then(J::as_str).ok_or_else(bad)?;
    if sig.ends_with('=') {
        return Err(bad());
    }
    let sig = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(sig).map_err(|_| bad())?;
    let mut data = m.clone();
    data.remove("sig");
    let bytes = dag_cbor(&J::Object(data))?;
    for k in allowed {
        if let Some(mb) = k.strip_prefix("did:key:") {
            if crate::oauth::lexicon::verify_sig(mb, &bytes, &sig) == Ok(true) {
                return Ok(k.clone());
            }
        }
    }
    Err(bad())
}

/// A genesis op: signed by one of its own rotation keys, `prev` null, and
/// hashing to `did` (`assureValidCreationOp`). Returns the normalized op.
pub fn assure_valid_creation_op(did: &str, op: &J) -> Result<J, PlcError> {
    match op_type(op, true)? {
        OpType::Tombstone => return Err(invalid("Operations not correctly ordered")),
        OpType::Operation | OpType::LegacyCreate => {}
    }
    let n = normalize(op);
    verify_sig(&rotation_keys(&n), op)?;
    let expected = did_for_genesis(op)?;
    if expected != did {
        return Err(invalid(format!("Hash of genesis operation does not match DID identifier: {expected}")));
    }
    if !op["prev"].is_null() {
        return Err(invalid("expected null prev on create"));
    }
    Ok(n)
}

/// The PLC directory's checks on an incoming op (`assertValidIncomingOp`):
/// size, shape (`plc_operation` or `plc_tombstone`) and field limits.
pub fn assert_valid_incoming(op: &J) -> Result<OpType, PlcError> {
    if dag_cbor(op)?.len() > MAX_OP_BYTES {
        return Err(invalid(format!("Operation too large ({MAX_OP_BYTES} bytes maximum in cbor encoding)")));
    }
    let t = op_type(op, true).map_err(|_| invalid(format!("Not a valid operation: {op}")))?;
    match t {
        OpType::Tombstone => return Ok(t),
        OpType::LegacyCreate => return Err(invalid(format!("Not a valid operation: {op}"))),
        OpType::Operation => {}
    }
    let aka = op["alsoKnownAs"].as_array().map(Vec::as_slice).unwrap_or_default();
    if aka.len() > MAX_AKA_ENTRIES {
        return Err(invalid(format!("To many alsoKnownAs entries (max {MAX_AKA_ENTRIES})")));
    }
    let mut seen = std::collections::HashSet::new();
    for a in aka.iter().filter_map(J::as_str) {
        if a.len() > MAX_AKA_LENGTH {
            return Err(invalid(format!("alsoKnownAs entry too long (max {MAX_AKA_LENGTH}): {a}")));
        }
        if !seen.insert(a) {
            return Err(invalid(format!("duplicate alsoKnownAs entry: {a}")));
        }
    }
    let rks = rotation_keys(op);
    if rks.len() > MAX_ROTATION_ENTRIES {
        return Err(invalid(format!("Too many rotationKey entries (max {MAX_ROTATION_ENTRIES})")));
    }
    if let Some(k) = rks.iter().find(|k| !valid_did_key(k)) {
        return Err(invalid(format!("Invalid rotationKey: {k}")));
    }
    let services = op["services"].as_object().cloned().unwrap_or_default();
    if services.len() > MAX_SERVICE_ENTRIES {
        return Err(invalid(format!("To many service entries (max {MAX_SERVICE_ENTRIES})")));
    }
    for (id, s) in &services {
        if id.len() > MAX_ID_LENGTH {
            return Err(invalid(format!("Service id too long (max {MAX_ID_LENGTH}): {id}")));
        }
        if s["type"].as_str().unwrap_or("").len() > MAX_SERVICE_TYPE_LENGTH {
            return Err(invalid(format!("Service type too long (max {MAX_SERVICE_TYPE_LENGTH})")));
        }
        if s["endpoint"].as_str().unwrap_or("").len() > MAX_SERVICE_ENDPOINT_LENGTH {
            return Err(invalid(format!("Service endpoint too long (max {MAX_SERVICE_ENDPOINT_LENGTH})")));
        }
    }
    let vms = op["verificationMethods"].as_object().cloned().unwrap_or_default();
    if vms.len() > MAX_VERIFICATION_METHOD_ENTRIES {
        return Err(invalid(format!("Too many Verification Method entries (max {MAX_VERIFICATION_METHOD_ENTRIES})")));
    }
    for (id, k) in &vms {
        let k = k.as_str().unwrap_or("");
        if id.len() > MAX_ID_LENGTH {
            return Err(invalid(format!("Verification Method id too long (max {MAX_ID_LENGTH}): {id}")));
        }
        if k.len() > MAX_DID_KEY_LENGTH {
            return Err(invalid(format!("Verification Method key too long (max {MAX_DID_KEY_LENGTH}): {k}")));
        }
        if !k.starts_with("did:key:z") {
            return Err(invalid(format!("Invalid verificationMethod key: {k}")));
        }
    }
    Ok(t)
}

// ---------------------------------------------------------------------------
// the log state machine (assureValidNextOp)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub op: J,
    pub cid: String,
    pub nullified: bool,
    pub created_at_ms: i64,
}

/// One DID's operation log as the PLC directory keeps it: every accepted op,
/// with the ones a recovery fork rewrote marked nullified.
#[derive(Clone, Debug)]
pub struct PlcLog {
    pub did: String,
    pub entries: Vec<LogEntry>,
}

impl PlcLog {
    pub fn new(did: &str) -> PlcLog {
        PlcLog { did: did.to_string(), entries: Vec::new() }
    }

    /// The latest op still in the canonical history.
    pub fn last(&self) -> Option<&LogEntry> {
        self.entries.iter().rev().find(|e| !e.nullified)
    }

    /// Validates `op` as the next operation at `at_ms` (did-method-plc
    /// `assureValidNextOp`) and appends it, nullifying the ops a recovery
    /// fork replaces.
    pub fn apply(&mut self, op: J, at_ms: i64) -> Result<(), PlcError> {
        let misordered = || invalid("Operations not correctly ordered");
        let active: Vec<usize> = (0..self.entries.len()).filter(|&i| !self.entries[i].nullified).collect();
        if active.is_empty() {
            assure_valid_creation_op(&self.did, &op)?;
        } else {
            match op_type(&op, true)? {
                OpType::Operation | OpType::Tombstone => {}
                OpType::LegacyCreate => return Err(misordered()),
            }
            let prev = op["prev"].as_str().ok_or_else(misordered)?;
            let pos = active.iter().position(|&i| self.entries[i].cid == prev).ok_or_else(misordered)?;
            let last = &self.entries[active[pos]];
            if op_type(&last.op, true)? == OpType::Tombstone {
                return Err(misordered());
            }
            let keys = rotation_keys(&last.op);
            let nullified = &active[pos + 1..];
            match nullified.first() {
                None => {
                    verify_sig(&keys, &op)?;
                }
                Some(&first) => {
                    let first = &self.entries[first];
                    let disputed = verify_sig(&keys, &first.op)?;
                    let idx = keys.iter().position(|k| *k == disputed).unwrap_or(0);
                    verify_sig(&keys[..idx], &op)?;
                    let most_recent = &self.entries[*active.last().expect("non-empty")];
                    if at_ms <= most_recent.created_at_ms {
                        return Err(misordered());
                    }
                    let lapsed = at_ms - first.created_at_ms;
                    if lapsed > RECOVERY_WINDOW_MS {
                        return Err(invalid(format!("Recovery operation occurred outside of the allowed 72 hr recovery window ({lapsed} ms)")));
                    }
                    for &i in nullified {
                        self.entries[i].nullified = true;
                    }
                }
            }
        }
        let cid = op_cid(&op)?.to_string();
        self.entries.push(LogEntry { op, cid, nullified: false, created_at_ms: at_ms });
        Ok(())
    }

    /// `/data` of the DID (None once tombstoned or empty).
    pub fn data(&self) -> Option<J> {
        op_to_data(&self.did, &self.last()?.op)
    }
}

/// The document data an op describes (`opToData`); None for a tombstone.
pub fn op_to_data(did: &str, op: &J) -> Option<J> {
    if op["type"] == "plc_tombstone" {
        return None;
    }
    let n = normalize(op);
    Some(json!({
        "did": did,
        "verificationMethods": n["verificationMethods"],
        "rotationKeys": n["rotationKeys"],
        "alsoKnownAs": n["alsoKnownAs"],
        "services": n["services"],
    }))
}

/// The DID document of document data (`formatDidDoc`).
pub fn format_did_doc(data: &J) -> J {
    let did = data["did"].as_str().unwrap_or("");
    let mut context = vec!["https://www.w3.org/ns/did/v1".to_string(), "https://w3id.org/security/multikey/v1".to_string()];
    let mut vms = Vec::new();
    for (id, key) in data["verificationMethods"].as_object().into_iter().flatten() {
        let key = key.as_str().unwrap_or("");
        let raw = key.strip_prefix("did:key:z").and_then(|m| bs58::decode(m).into_vec().ok()).unwrap_or_default();
        let ctx = match raw.get(..2) {
            Some([0x80, 0x24]) => Some("https://w3id.org/security/suites/ecdsa-2019/v1"),
            Some([0xe7, 0x01]) => Some("https://w3id.org/security/suites/secp256k1-2019/v1"),
            _ => None,
        };
        if let Some(c) = ctx {
            if !context.iter().any(|x| x == c) {
                context.push(c.to_string());
            }
        }
        vms.push(json!({
            "id": format!("{did}#{id}"),
            "type": "Multikey",
            "controller": did,
            "publicKeyMultibase": key.strip_prefix("did:key:").unwrap_or(key),
        }));
    }
    let services: Vec<J> = data["services"]
        .as_object()
        .into_iter()
        .flatten()
        .map(|(id, s)| json!({"id": format!("#{id}"), "type": s["type"], "serviceEndpoint": s["endpoint"]}))
        .collect();
    json!({
        "@context": context,
        "id": did,
        "alsoKnownAs": data["alsoKnownAs"],
        "verificationMethod": vms,
        "service": services,
    })
}

// ---------------------------------------------------------------------------
// directory client
// ---------------------------------------------------------------------------

/// HTTP client of a PLC directory (`--plc-url`; https://plc.directory, or a
/// local did-method-plc server / [`mock::MockPlc`]).
#[derive(Clone)]
pub struct PlcClient {
    url: String,
    http: reqwest::Client,
}

impl PlcClient {
    pub fn new(url: &str) -> PlcClient {
        PlcClient { url: url.trim_end_matches('/').to_string(), http: crate::http::public().clone() }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    fn did_url(&self, did: &str, suffix: &str) -> Result<String, PlcError> {
        if !valid_plc_did(did) {
            return Err(invalid(format!("not a did:plc identifier: {did}")));
        }
        Ok(format!("{}/{did}{suffix}", self.url))
    }

    /// Runs one request with the timeout and records it. `json`: a 2xx body
    /// is parsed as JSON (the GETs); otherwise it is ignored: the directory
    /// answers an accepted `POST /{did}` with `res.sendStatus(200)`, a
    /// text/plain "OK" (did-method-plc server routes.ts), and any 2xx means
    /// the op was applied.
    async fn call(&self, op: &'static str, rb: reqwest::RequestBuilder, did: &str, json: bool) -> Result<Option<J>, PlcError> {
        let t = Instant::now();
        let r = tokio::time::timeout(REQUEST_TIMEOUT, async {
            let r = rb.send().await.map_err(|e| PlcError::Unavailable(format!("{e}")))?;
            let status = r.status();
            let body = read_capped(r).await?;
            if status.is_success() {
                if body.is_empty() || !json {
                    return Ok(None);
                }
                return serde_json::from_slice(&body).map(Some).map_err(|e| PlcError::Unavailable(format!("bad response: {e}")));
            }
            let message = serde_json::from_slice::<J>(&body)
                .ok()
                .and_then(|j| j["message"].as_str().map(str::to_string))
                .unwrap_or_else(|| String::from_utf8_lossy(&body[..body.len().min(300)]).into_owned());
            match status.as_u16() {
                404 | 410 => Err(PlcError::NotFound(did.to_string())),
                s @ 400..=499 if s != 408 && s != 429 => Err(PlcError::Rejected { status: s, message }),
                s => Err(PlcError::Unavailable(format!("HTTP {s}: {message}"))),
            }
        })
        .await
        .unwrap_or_else(|_| Err(PlcError::Unavailable(format!("timed out after {REQUEST_TIMEOUT:?}"))));
        let result = match &r {
            Ok(_) => "ok",
            Err(PlcError::Rejected { .. }) => "rejected",
            Err(PlcError::NotFound(_)) => "not_found",
            Err(_) => "unavailable",
        };
        PLC_REQUESTS.with_label_values(&[op, result]).inc();
        PLC_SECONDS.with_label_values(&[op]).observe(t.elapsed().as_secs_f64());
        if let Err(e) = &r {
            if !matches!(e, PlcError::NotFound(_)) {
                tracing::warn!(op, did, "PLC directory request failed: {e}");
            }
        }
        r
    }

    /// The latest op in the DID's log (`GET /{did}/log/last`).
    pub async fn last_op(&self, did: &str) -> Result<J, PlcError> {
        let url = self.did_url(did, "/log/last")?;
        self.call("get_last_op", self.http.get(url), did, true).await?.ok_or_else(|| PlcError::Unavailable("empty response".into()))
    }

    /// The DID's document data (`GET /{did}/data`).
    pub async fn document_data(&self, did: &str) -> Result<J, PlcError> {
        let url = self.did_url(did, "/data")?;
        self.call("get_data", self.http.get(url), did, true).await?.ok_or_else(|| PlcError::Unavailable("empty response".into()))
    }

    /// Submits an op (`POST /{did}`); `op_label` names it in metrics.
    pub async fn send(&self, did: &str, op: &J, op_label: &'static str) -> Result<(), PlcError> {
        let url = self.did_url(did, "")?;
        self.call(op_label, self.http.post(url).json(op), did, false).await.map(|_| ())
    }
}

async fn read_capped(r: reqwest::Response) -> Result<Vec<u8>, PlcError> {
    use futures::StreamExt;
    let mut buf = Vec::new();
    let mut s = r.bytes_stream();
    while let Some(c) = s.next().await {
        buf.extend_from_slice(&c.map_err(|e| PlcError::Unavailable(e.to_string()))?);
        if buf.len() > MAX_RESPONSE_BYTES {
            return Err(PlcError::Unavailable("response too large".into()));
        }
    }
    Ok(buf)
}

// ---------------------------------------------------------------------------
// configuration and the server's rotation key
// ---------------------------------------------------------------------------

/// Whether account DIDs are registered with a PLC directory.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PlcMode {
    /// `directory` when a rotation key is configured; otherwise
    /// `unregistered` in dev mode and a startup error outside it.
    #[default]
    Auto,
    /// Genesis ops are submitted to `--plc-url`; handle and signing-key
    /// changes are PLC updates; the PLC operation endpoints work.
    Directory,
    /// Dev/test/bench only: DIDs are minted locally and never registered
    /// (they resolve only on this server); the PLC operation endpoints answer
    /// 501. Refused outside dev mode.
    Unregistered,
}

impl std::str::FromStr for PlcMode {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<PlcMode> {
        match s {
            "auto" | "" => Ok(PlcMode::Auto),
            "directory" => Ok(PlcMode::Directory),
            "unregistered" => Ok(PlcMode::Unregistered),
            _ => anyhow::bail!("--plc-mode must be auto, directory or unregistered (got {s:?})"),
        }
    }
}

/// Where the server's rotation key comes from. Never printed.
#[derive(Clone)]
pub enum RotationKey {
    /// 64 hex chars (`--plc-rotation-key`, the reference's
    /// `PDS_PLC_ROTATION_KEY_K256_PRIVATE_KEY_HEX`).
    Hex(zeroize::Zeroizing<String>),
    /// `vw1.…` wrapped under the KEK (`--plc-rotation-key-file`), unwrapped
    /// at startup.
    Wrapped(String),
    /// An in-process key (tests).
    Key(Arc<Keypair>),
}

impl std::fmt::Debug for RotationKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            RotationKey::Hex(_) => "RotationKey(hex)",
            RotationKey::Wrapped(_) => "RotationKey(wrapped)",
            RotationKey::Key(_) => "RotationKey(key)",
        })
    }
}

impl RotationKey {
    /// A rotation-key file: a `vw1.` wrapped blob (`vlpds
    /// --wrap-plc-rotation-key`), or the key as 64 hex chars (a mounted
    /// secret, never the bucket).
    pub fn from_file(path: &std::path::Path) -> anyhow::Result<RotationKey> {
        let s = zeroize::Zeroizing::new(
            std::fs::read_to_string(path).map_err(|e| anyhow::anyhow!("reading PLC rotation key file {}: {e}", path.display()))?,
        );
        let t = s.trim();
        if t.starts_with("vw1.") {
            return Ok(RotationKey::Wrapped(t.to_string()));
        }
        anyhow::ensure!(
            t.len() == 64 && t.bytes().all(|b| b.is_ascii_hexdigit()),
            "PLC rotation key file {} must hold a vw1. wrapped key or 64 hex chars",
            path.display()
        );
        Ok(RotationKey::Hex(zeroize::Zeroizing::new(t.to_string())))
    }

    /// The key itself (a wrapped one is unwrapped with `secrets`).
    pub async fn load(&self, secrets: &crate::secrets::Secrets) -> anyhow::Result<Arc<Keypair>> {
        let key = match self {
            RotationKey::Key(k) => return Ok(k.clone()),
            RotationKey::Hex(h) => {
                let raw = zeroize::Zeroizing::new(hex::decode(h.trim()).map_err(|_| anyhow::anyhow!("PLC rotation key must be 64 hex chars"))?);
                anyhow::ensure!(raw.len() == 32, "PLC rotation key must be 32 bytes (64 hex chars)");
                Keypair::from_bytes(&raw)?
            }
            RotationKey::Wrapped(b) => {
                let u = secrets
                    .unwrap(crate::secrets::Purpose::PlcRotationKey, ROTATION_KEY_SUBJECT, b)
                    .await
                    .map_err(|e| anyhow::anyhow!("unwrapping the PLC rotation key: {e}"))?;
                Keypair::from_bytes(&u.plaintext)?
            }
        };
        anyhow::ensure!(key.matches_public(""), "PLC rotation key does not derive its public key (memory fault?)");
        Ok(Arc::new(key))
    }
}

/// Wraps a rotation key under the keyring's current KEK (the
/// `--plc-rotation-key-file` form).
pub async fn wrap_rotation_key(secrets: &crate::secrets::Secrets, key: &Keypair) -> Result<String, crate::secrets::SecretError> {
    secrets.wrap(crate::secrets::Purpose::PlcRotationKey, ROTATION_KEY_SUBJECT, &key.to_bytes()).await
}

/// PLC settings (`--plc-mode`, `--plc-rotation-key[-file]`,
/// `--plc-recovery-did-key`; the directory is `Config::plc_url`).
#[derive(Clone, Debug, Default)]
pub struct PlcConfig {
    pub mode: PlcMode,
    pub rotation_key: Option<RotationKey>,
    /// A server-wide recovery did:key put ahead of the server's rotation key
    /// in every genesis op and recommendation (reference
    /// `PDS_RECOVERY_DID_KEY`).
    pub recovery_did_key: Option<String>,
    /// Previous server rotation keys (`--plc-rotation-key-old-file`): kept
    /// to sign updates of DIDs that still list them, each such update
    /// replacing them with the current key (server key rotation; RUNBOOK
    /// "PLC rotation key rotation").
    pub old_rotation_keys: Vec<RotationKey>,
}

impl PlcConfig {
    /// The mode in force: `Auto` resolved.
    pub fn effective_mode(&self) -> PlcMode {
        match (self.mode, &self.rotation_key) {
            (PlcMode::Auto, Some(_)) => PlcMode::Directory,
            (PlcMode::Auto, None) => PlcMode::Unregistered,
            (m, _) => m,
        }
    }

    /// Startup check (the binary's `Config::check_secrets`): outside dev
    /// mode DIDs must be registered, so a rotation key is required.
    pub fn check(&self, dev_mode: bool, plc_url: &str) -> anyhow::Result<()> {
        match (self.mode, &self.rotation_key) {
            (PlcMode::Unregistered, _) if !dev_mode => {
                anyhow::bail!("--plc-mode unregistered (DIDs never registered with PLC) is only allowed with --dev-mode")
            }
            (PlcMode::Directory, None) => anyhow::bail!(
                "--plc-mode directory needs a PLC rotation key: set --plc-rotation-key / VLPDS_PLC_ROTATION_KEY_K256_PRIVATE_KEY_HEX or --plc-rotation-key-file"
            ),
            (PlcMode::Auto, None) if !dev_mode => anyhow::bail!(
                "a PLC rotation key is required outside --dev-mode (accounts' DIDs are registered with --plc-url): set --plc-rotation-key / VLPDS_PLC_ROTATION_KEY_K256_PRIVATE_KEY_HEX or --plc-rotation-key-file"
            ),
            _ => {}
        }
        if self.effective_mode() == PlcMode::Directory {
            let u = reqwest::Url::parse(plc_url).map_err(|e| anyhow::anyhow!("--plc-url {plc_url:?}: {e}"))?;
            anyhow::ensure!(matches!(u.scheme(), "http" | "https"), "--plc-url must be http(s): {plc_url}");
            if u.scheme() == "http" && !dev_mode {
                tracing::warn!(plc_url, "--plc-url is plain http outside dev mode (a local PLC directory?)");
            }
        }
        if let Some(k) = &self.recovery_did_key {
            anyhow::ensure!(valid_did_key(k), "--plc-recovery-did-key must be a secp256k1 or P-256 did:key: {k}");
        }
        Ok(())
    }
}

/// The server's PLC identity service: its rotation key and the directory.
pub struct Plc {
    pub client: PlcClient,
    key: Arc<Keypair>,
    did_key: String,
    recovery_did_key: Option<String>,
    /// Retired server rotation keys and their did:keys.
    old: Vec<(Arc<Keypair>, String)>,
    /// Per-DID update locks ([`Plc::update`]): this node's updates of one
    /// DID (handle, signing key, tombstone, key rotation) build on each
    /// other instead of forking `prev`.
    locks: parking_lot::Mutex<std::collections::HashMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>>,
}

/// Attempts of one update when another op lands between reading the log
/// and submitting (a `prev` race, from another node or a user's key).
const UPDATE_ATTEMPTS: usize = 4;

/// What [`Plc::rotate_server_key`] found (or did) for one DID.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyRotation {
    /// The DID already lists the current server key.
    Current,
    /// It listed a retired server key, now replaced (or, dry run, to replace).
    Rotated,
    /// It lists none of this server's keys (migrated away, or never ours).
    Foreign,
}

impl std::fmt::Debug for Plc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Plc").field("url", &self.client.url).field("rotation_key", &self.did_key).finish()
    }
}

impl Plc {
    pub fn new(plc_url: &str, key: Arc<Keypair>, recovery_did_key: Option<String>) -> Plc {
        let did_key = key.did_key();
        Plc { client: PlcClient::new(plc_url), key, did_key, recovery_did_key, old: Vec::new(), locks: Default::default() }
    }

    /// This node's update lock of `did`.
    async fn lock(&self, did: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let m = {
            let mut g = self.locks.lock();
            if g.len() > 1024 {
                g.retain(|_, w| w.strong_count() > 0);
            }
            match g.get(did).and_then(|w| w.upgrade()) {
                Some(m) => m,
                None => {
                    let m = Arc::new(tokio::sync::Mutex::new(()));
                    g.insert(did.to_string(), Arc::downgrade(&m));
                    m
                }
            }
        };
        m.lock_owned().await
    }

    /// Read-modify-write of the DID's log: `build` makes the op following
    /// the last one (None: nothing to submit), which is submitted as
    /// `label`. Updates of one DID are serialized on this node; when the
    /// directory refuses the op because another one landed after the log
    /// was read (any node, or a user's own rotation key: a same-key fork is
    /// refused), the update is rebuilt on the new last op and retried, so
    /// concurrent updates of different fields (handle, signing key) both
    /// land instead of one failing. Ok(false): nothing submitted.
    async fn update(&self, did: &str, label: &'static str, build: impl Fn(&J) -> Result<Option<J>, PlcError>) -> Result<bool, PlcError> {
        let _g = self.lock(did).await;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let last = self.client.last_op(did).await?;
            let Some(op) = build(&last)? else { return Ok(false) };
            match self.client.send(did, &op, label).await {
                Ok(()) => return Ok(true),
                Err(e @ PlcError::Rejected { .. }) if attempt < UPDATE_ATTEMPTS => {
                    // raced: the log moved on since `last`
                    let now = self.client.last_op(did).await?;
                    if op_cid(&now)? == op_cid(&last)? {
                        return Err(e);
                    }
                    tracing::info!(did, op = label, "PLC log changed while updating, rebuilding the op: {e}");
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Adds retired rotation keys (signing updates of DIDs that still list
    /// them).
    pub fn with_old_keys(mut self, keys: Vec<Arc<Keypair>>) -> Plc {
        self.old = keys.into_iter().map(|k| (k.clone(), k.did_key())).filter(|(_, d)| *d != self.did_key).collect();
        self
    }

    /// Whether `did_key` is one of this server's rotation keys (current or
    /// retired).
    pub fn is_server_key(&self, did_key: &str) -> bool {
        did_key == self.did_key || self.old.iter().any(|(_, d)| d == did_key)
    }

    /// Whether `did_key` is this deployment's (a server rotation key or
    /// the operator's `--plc-recovery-did-key`): ops it signs go through
    /// this PDS or its operator.
    pub fn is_operator_key(&self, did_key: &str) -> bool {
        self.is_server_key(did_key) || self.recovery_did_key.as_deref() == Some(did_key)
    }

    /// The server key to sign the op following `last` with: the current key
    /// if `last` lists it, else a retired one it lists, else the current key
    /// (the directory will refuse it).
    fn signer_for(&self, last: &J) -> (&Arc<Keypair>, bool) {
        let keys = rotation_keys(last);
        if keys.contains(&self.did_key) {
            return (&self.key, false);
        }
        match self.old.iter().find(|(_, d)| keys.contains(d)) {
            Some((k, _)) => (k, true),
            None => (&self.key, false),
        }
    }

    /// The service for `cfg` (None when DIDs are not registered).
    pub async fn from_config(
        cfg: &PlcConfig,
        plc_url: &str,
        dev_mode: bool,
        secrets: &crate::secrets::Secrets,
    ) -> anyhow::Result<Option<Arc<Plc>>> {
        match cfg.effective_mode() {
            PlcMode::Unregistered => {
                if !dev_mode {
                    tracing::warn!("PLC registration is off outside dev mode: new accounts' DIDs exist only on this server");
                }
                Ok(None)
            }
            _ => {
                let src = cfg.rotation_key.as_ref().ok_or_else(|| anyhow::anyhow!("--plc-mode directory needs a PLC rotation key"))?;
                let key = src.load(secrets).await?;
                let mut old = Vec::new();
                for k in &cfg.old_rotation_keys {
                    old.push(k.load(secrets).await?);
                }
                touch_metrics();
                let plc = Plc::new(plc_url, key, cfg.recovery_did_key.clone()).with_old_keys(old);
                tracing::info!(plc_url, rotation_key = plc.did_key, retired_keys = plc.old.len(), "PLC registration on");
                Ok(Some(Arc::new(plc)))
            }
        }
    }

    /// The server rotation key's did:key.
    pub fn rotation_did_key(&self) -> &str {
        &self.did_key
    }

    /// getRecommendedDidCredentials `rotationKeys`: [recovery?, server].
    pub fn recommended_rotation_keys(&self) -> Vec<String> {
        self.recovery_did_key.iter().cloned().chain([self.did_key.clone()]).collect()
    }

    /// The signed genesis op of a new account and its DID (reference
    /// `formatDidAndPlcOp`): rotation keys [user recovery key?, server
    /// recovery key?, server rotation key].
    pub fn genesis(&self, signing_did_key: &str, handle: &str, pds: &str, user_recovery_key: Option<&str>) -> Result<(String, J), PlcError> {
        let rotation_keys: Vec<String> =
            user_recovery_key.map(str::to_string).into_iter().chain(self.recovery_did_key.clone()).chain([self.did_key.clone()]).collect();
        let op = sign(format_atproto_op(signing_did_key, handle, pds, &rotation_keys, None), &self.key)?;
        Ok((did_for_genesis(&op)?, op))
    }

    /// Signs an unsigned op with the server rotation key.
    pub fn sign(&self, unsigned: J) -> Result<J, PlcError> {
        sign(unsigned, &self.key)
    }

    /// `createUpdateOp`: the op following `last` (prev = its CID), built by
    /// `f` from `last` normalized without `sig`/`prev`, signed with the
    /// server rotation key. If `last` lists only a retired server key, that
    /// key signs, and the op lists the current key in its place.
    pub fn update_op(&self, last: &J, f: impl FnOnce(&mut Map<String, J>) -> Result<(), PlcError>) -> Result<J, PlcError> {
        match op_type(last, true)? {
            OpType::Tombstone => return Err(PlcError::Tombstoned),
            OpType::Operation | OpType::LegacyCreate => {}
        }
        let prev = op_cid(last)?.to_string();
        let J::Object(mut m) = normalize(last) else { unreachable!("normalize returns an object") };
        m.remove("sig");
        f(&mut m)?;
        m.insert("prev".into(), J::String(prev));
        let (signer, retired) = self.signer_for(last);
        if retired {
            if let Some(J::Array(keys)) = m.get_mut("rotationKeys") {
                let mut out: Vec<J> = Vec::with_capacity(keys.len());
                for k in keys.drain(..) {
                    let k = match k.as_str() {
                        Some(s) if self.old.iter().any(|(_, d)| d == s) => J::String(self.did_key.clone()),
                        _ => k,
                    };
                    if !out.contains(&k) {
                        out.push(k);
                    }
                }
                *keys = out;
            }
        }
        let unsigned = J::Object(m);
        op_type(&unsigned, false)?;
        sign(unsigned, signer)
    }

    /// Replaces a retired server rotation key in the DID's rotation keys
    /// with the current one (an update signed by the retired key). `dry`:
    /// only report.
    pub async fn rotate_server_key(&self, did: &str, dry: bool) -> Result<KeyRotation, PlcError> {
        let last = self.last_op(did).await?;
        let keys = rotation_keys(&last);
        if keys.contains(&self.did_key) {
            return Ok(KeyRotation::Current);
        }
        if !self.old.iter().any(|(_, d)| keys.contains(d)) {
            return Ok(KeyRotation::Foreign);
        }
        if !dry {
            self.update(did, "rotate_key", |last| {
                let keys = rotation_keys(not_tombstone(last)?);
                if keys.contains(&self.did_key) || !self.old.iter().any(|(_, d)| keys.contains(d)) {
                    return Ok(None);
                }
                self.update_op(last, |_| Ok(())).map(Some)
            })
            .await?;
        }
        Ok(KeyRotation::Rotated)
    }

    /// The DID's last op, refusing a tombstoned DID (`ensureLastOp`).
    pub async fn last_op(&self, did: &str) -> Result<J, PlcError> {
        let last = self.client.last_op(did).await?;
        if last["type"] == "plc_tombstone" {
            return Err(PlcError::Tombstoned);
        }
        Ok(last)
    }

    /// Registers a genesis op (`sendOperation`).
    pub async fn create(&self, did: &str, op: &J) -> Result<(), PlcError> {
        self.client.send(did, op, "create").await
    }

    /// Points the DID's `at://` alias at `handle` (`updateHandleOp`: the
    /// first `at://` entry replaced, else prepended). Ok(false): the log
    /// already says so and nothing was submitted.
    pub async fn update_handle(&self, did: &str, handle: &str) -> Result<bool, PlcError> {
        let formatted = ensure_atproto_prefix(handle);
        self.update(did, "update_handle", |last| {
            let aka: Vec<J> = normalize(not_tombstone(last)?)["alsoKnownAs"].as_array().cloned().unwrap_or_default();
            let i = aka.iter().position(|h| h.as_str().is_some_and(|h| h.starts_with("at://")));
            if i.is_some_and(|i| aka[i] == formatted) {
                return Ok(None);
            }
            self.update_op(last, |m| {
                let mut aka = aka.clone();
                match i {
                    Some(i) => aka[i] = J::String(formatted.clone()),
                    None => aka.insert(0, J::String(formatted.clone())),
                }
                m.insert("alsoKnownAs".into(), J::Array(aka));
                Ok(())
            })
            .map(Some)
        })
        .await
    }

    /// Sets the DID's `atproto` verification method (`updateAtprotoKeyOp`).
    /// Ok(false): already that key.
    pub async fn update_signing_key(&self, did: &str, signing_did_key: &str) -> Result<bool, PlcError> {
        self.update(did, "update_signing_key", |last| {
            if normalize(not_tombstone(last)?)["verificationMethods"]["atproto"] == signing_did_key {
                return Ok(None);
            }
            self.update_op(last, |m| {
                let vms = m.entry("verificationMethods").or_insert_with(|| json!({}));
                vms["atproto"] = J::String(signing_did_key.to_string());
                Ok(())
            })
            .map(Some)
        })
        .await
    }

    /// Tombstones the DID (`tombstone`): undoing a genesis op whose account
    /// creation failed afterwards.
    pub async fn tombstone(&self, did: &str) -> Result<(), PlcError> {
        self.update(did, "tombstone", |last| {
            let last = not_tombstone(last)?;
            sign(json!({"type": "plc_tombstone", "prev": op_cid(last)?.to_string()}), self.signer_for(last).0).map(Some)
        })
        .await
        .map(|_| ())
    }
}

/// `ensureLastOp`'s refusal of a tombstoned DID.
fn not_tombstone(last: &J) -> Result<&J, PlcError> {
    if last["type"] == "plc_tombstone" {
        return Err(PlcError::Tombstoned);
    }
    Ok(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// did-method-plc's interop audit logs (copied from
    /// github.com/did-method-plc/did-method-plc interop_tests/, whose
    /// canonical home is go-didplc testdata).
    const VALID: &[(&str, &str)] = &[
        ("bnewbold_robocracy", include_str!("../../testdata/plc/valid/log_bnewbold_robocracy.json")),
        ("bskyapp", include_str!("../../testdata/plc/valid/log_bskyapp.json")),
        ("duplicate_rotation_keys", include_str!("../../testdata/plc/valid/log_duplicate_rotation_keys.json")),
        ("empty_rotation_keys", include_str!("../../testdata/plc/valid/log_empty_rotation_keys.json")),
        ("legacy_dholms", include_str!("../../testdata/plc/valid/log_legacy_dholms.json")),
        ("nullification", include_str!("../../testdata/plc/valid/log_nullification.json")),
        ("nullification_at_exactly_72h", include_str!("../../testdata/plc/valid/log_nullification_at_exactly_72h.json")),
        ("nullification_nontrivial", include_str!("../../testdata/plc/valid/log_nullification_nontrivial.json")),
        ("nullified_tombstone", include_str!("../../testdata/plc/valid/log_nullified_tombstone.json")),
        ("tombstone", include_str!("../../testdata/plc/valid/log_tombstone.json")),
    ];
    const INVALID: &[(&str, &str)] = &[
        ("nullification_reused_key", include_str!("../../testdata/plc/invalid/log_invalid_nullification_reused_key.json")),
        ("nullification_too_slow", include_str!("../../testdata/plc/invalid/log_invalid_nullification_too_slow.json")),
        ("sig_b64_newline", include_str!("../../testdata/plc/invalid/log_invalid_sig_b64_newline.json")),
        ("sig_b64_padding_bits", include_str!("../../testdata/plc/invalid/log_invalid_sig_b64_padding_bits.json")),
        ("sig_b64_padding_chars", include_str!("../../testdata/plc/invalid/log_invalid_sig_b64_padding_chars.json")),
        ("sig_der", include_str!("../../testdata/plc/invalid/log_invalid_sig_der.json")),
        ("sig_k256_high_s", include_str!("../../testdata/plc/invalid/log_invalid_sig_k256_high_s.json")),
        ("sig_p256_high_s", include_str!("../../testdata/plc/invalid/log_invalid_sig_p256_high_s.json")),
        ("update_nullified", include_str!("../../testdata/plc/invalid/log_invalid_update_nullified.json")),
        ("update_tombstoned", include_str!("../../testdata/plc/invalid/log_invalid_update_tombstoned.json")),
    ];

    fn ms(rfc3339: &str) -> i64 {
        chrono::DateTime::parse_from_rfc3339(rfc3339).unwrap().timestamp_millis()
    }

    /// Replays an audit log through [`PlcLog::apply`]; Ok(computed nullified flags).
    fn replay(log: &J) -> Result<Vec<bool>, PlcError> {
        let entries = log.as_array().unwrap();
        let mut l = PlcLog::new(entries[0]["did"].as_str().unwrap());
        for e in entries {
            l.apply(e["operation"].clone(), ms(e["createdAt"].as_str().unwrap()))?;
        }
        Ok(l.entries.iter().map(|e| e.nullified).collect())
    }

    #[test]
    fn interop_valid_logs_replay_with_matching_cids_dids_and_nullification() {
        for (name, raw) in VALID {
            let log: J = serde_json::from_str(raw).unwrap();
            let entries = log.as_array().unwrap();
            // the DID is the genesis op's hash; every op's CID matches the log's
            let genesis = &entries[0]["operation"];
            assert_eq!(did_for_genesis(genesis).unwrap(), entries[0]["did"], "{name}: DID derivation");
            for e in entries {
                assert_eq!(op_cid(&e["operation"]).unwrap().to_string(), e["cid"], "{name}: op CID (DAG-CBOR bytes)");
            }
            let flags = replay(&log).unwrap_or_else(|e| panic!("{name}: {e}"));
            let want: Vec<bool> = entries.iter().map(|e| e["nullified"].as_bool().unwrap()).collect();
            assert_eq!(flags, want, "{name}: nullified flags");
        }
    }

    #[test]
    fn interop_invalid_logs_are_refused() {
        for (name, raw) in INVALID {
            let log: J = serde_json::from_str(raw).unwrap();
            replay(&log).expect_err(name);
        }
    }

    /// Our genesis op is the reference's `formatAtprotoOp` byte for byte: a
    /// published genesis op rebuilt from its fields (with its signature)
    /// has the published CID and DID.
    #[test]
    fn format_atproto_op_matches_published_genesis_bytes() {
        let log: J = serde_json::from_str(VALID[0].1).unwrap();
        let e = &log[0];
        let op = &e["operation"];
        let rks: Vec<String> = rotation_keys(op);
        let mut rebuilt = format_atproto_op(
            op["verificationMethods"]["atproto"].as_str().unwrap(),
            "bnewbold.pds.robocracy.org",
            "pds.robocracy.org",
            &rks,
            None,
        );
        rebuilt["sig"] = op["sig"].clone();
        assert_eq!(dag_cbor(&rebuilt).unwrap(), dag_cbor(op).unwrap());
        assert_eq!(op_cid(&rebuilt).unwrap().to_string(), e["cid"]);
        assert_eq!(did_for_genesis(&rebuilt).unwrap(), e["did"]);
        verify_sig(&rks, &rebuilt).unwrap();
        // an update rebuilt with prev = the genesis CID matches the second op
        let e2 = &log[1];
        let mut upd = format_atproto_op(
            op["verificationMethods"]["atproto"].as_str().unwrap(),
            "bnewbold.robocracy.org",
            "https://pds.robocracy.org",
            &rks,
            Some(e["cid"].as_str().unwrap()),
        );
        upd["sig"] = e2["operation"]["sig"].clone();
        assert_eq!(op_cid(&upd).unwrap().to_string(), e2["cid"]);
    }

    #[test]
    fn genesis_update_and_tombstone_chain() {
        let rot = Arc::new(Keypair::generate());
        let recovery = Keypair::generate().did_key();
        let plc = Plc::new("http://127.0.0.1:1", rot.clone(), Some(recovery.clone()));
        let user = Keypair::generate().did_key();
        let signing = Keypair::generate().did_key();
        let (did, op) = plc.genesis(&signing, "alice.test", "https://pds.example", Some(&user)).unwrap();
        assert!(valid_plc_did(&did), "{did}");
        // reference ordering: user recovery key, server recovery key, server rotation key
        assert_eq!(rotation_keys(&op), vec![user.clone(), recovery.clone(), rot.did_key()]);
        assert_eq!(op["alsoKnownAs"], json!(["at://alice.test"]));
        assert_eq!(op["services"]["atproto_pds"], json!({"type": "AtprotoPersonalDataServer", "endpoint": "https://pds.example"}));
        assert_eq!(op["prev"], J::Null);
        assert_eq!(plc.recommended_rotation_keys(), vec![recovery, rot.did_key()]);
        // low-S compact base64url signature by the server key
        let sig = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(op["sig"].as_str().unwrap()).unwrap();
        assert_eq!(sig.len(), 64);
        assert!(secp256k1::ecdsa::Signature::from_compact(&sig).map(|s| {
            let mut n = s;
            n.normalize_s();
            n == s
        }).unwrap());
        assert_eq!(verify_sig(&rotation_keys(&op), &op).unwrap(), rot.did_key());
        assert_valid_incoming(&op).unwrap();
        let mut log = PlcLog::new(&did);
        log.apply(op.clone(), 1).unwrap();
        // a hedged signature: signing again gives another op and DID
        let (did2, _) = plc.genesis(&signing, "alice.test", "https://pds.example", Some(&user)).unwrap();
        assert_ne!(did, did2);
        // update: prev is the CID of the last op
        let upd = plc
            .update_op(&op, |m| {
                m.insert("alsoKnownAs".into(), json!(["at://bob.test"]));
                Ok(())
            })
            .unwrap();
        assert_eq!(upd["prev"], json!(op_cid(&op).unwrap().to_string()));
        assert!(upd["rotationKeys"] == op["rotationKeys"] && upd["verificationMethods"] == op["verificationMethods"]);
        log.apply(upd.clone(), 2).unwrap();
        // a stale prev is refused; so is an op signed by a key not in the log
        assert!(log.clone().apply(plc.update_op(&op, |_| Ok(())).unwrap(), 3).is_err());
        let stranger = Plc::new("http://127.0.0.1:1", Arc::new(Keypair::generate()), None);
        assert!(log.clone().apply(stranger.update_op(&upd, |_| Ok(())).unwrap(), 3).is_err());
        // tombstone, then nothing more
        let tomb = plc.sign(json!({"type": "plc_tombstone", "prev": op_cid(&upd).unwrap().to_string()})).unwrap();
        log.apply(tomb.clone(), 4).unwrap();
        assert!(log.data().is_none());
        assert!(matches!(plc.update_op(&tomb, |_| Ok(())), Err(PlcError::Tombstoned)));
    }

    #[test]
    fn shapes_and_limits() {
        let rot = Keypair::generate();
        let op = sign(format_atproto_op(&rot.did_key(), "a.test", "https://p", &[rot.did_key()], None), &rot).unwrap();
        assert_eq!(op_type(&op, true).unwrap(), OpType::Operation);
        assert!(op_type(&op, false).is_err(), "signed op where unsigned expected");
        let mut extra = op.clone();
        extra["extra"] = json!(1);
        assert!(op_type(&extra, true).is_err());
        let mut svc = op.clone();
        svc["services"]["atproto_pds"]["extra"] = json!("x");
        assert!(op_type(&svc, true).is_err());
        let mut big = op.clone();
        big["alsoKnownAs"] = json!((0..11).map(|i| format!("at://h{i}.test")).collect::<Vec<_>>());
        assert!(assert_valid_incoming(&big).is_err());
        let mut badkey = op.clone();
        badkey["rotationKeys"] = json!(["did:key:zNotAKey"]);
        assert!(assert_valid_incoming(&badkey).is_err());
        assert!(dag_cbor(&json!({"f": 1.5})).is_err());
        assert_eq!(ensure_atproto_prefix("https://x.test"), "at://x.test");
        assert_eq!(ensure_atproto_prefix("at://x.test"), "at://x.test");
        assert_eq!(ensure_http_prefix("x.test"), "https://x.test");
        assert!(valid_did_key(&rot.did_key()));
        assert!(!valid_plc_did("did:plc:abc/../x"));
    }

    #[test]
    fn plc_config_modes() {
        let key = || Some(RotationKey::Key(Arc::new(Keypair::generate())));
        let c = |mode, rotation_key| PlcConfig { mode, rotation_key, ..Default::default() };
        assert_eq!(c(PlcMode::Auto, None).effective_mode(), PlcMode::Unregistered);
        assert_eq!(c(PlcMode::Auto, key()).effective_mode(), PlcMode::Directory);
        c(PlcMode::Auto, None).check(true, DEFAULT_PLC_URL).unwrap();
        assert!(c(PlcMode::Auto, None).check(false, DEFAULT_PLC_URL).is_err(), "production needs a rotation key");
        assert!(c(PlcMode::Unregistered, key()).check(false, DEFAULT_PLC_URL).is_err(), "unregistered is dev-only");
        assert!(c(PlcMode::Directory, None).check(true, DEFAULT_PLC_URL).is_err());
        c(PlcMode::Directory, key()).check(false, DEFAULT_PLC_URL).unwrap();
        assert!(c(PlcMode::Directory, key()).check(false, "ftp://x").is_err());
        let bad = PlcConfig { recovery_did_key: Some("did:key:nope".into()), ..c(PlcMode::Auto, key()) };
        assert!(bad.check(false, DEFAULT_PLC_URL).is_err());
        assert!(!format!("{:?}", c(PlcMode::Auto, Some(RotationKey::Hex(zeroize::Zeroizing::new("ab".repeat(32)))))).contains("abab"));
    }

    #[tokio::test]
    async fn rotation_key_wraps_under_the_kek() {
        let s = crate::secrets::Secrets::dev();
        let k = Keypair::generate();
        let w = wrap_rotation_key(&s, &k).await.unwrap();
        assert!(w.starts_with("vw1."));
        assert!(!w.contains(&hex::encode(&*k.to_bytes())));
        let loaded = RotationKey::Wrapped(w.clone()).load(&s).await.unwrap();
        assert_eq!(loaded.did_key(), k.did_key());
        // bound to its purpose: not a signing key of anyone
        assert!(s.unwrap(crate::secrets::Purpose::SigningKey, ROTATION_KEY_SUBJECT, &w).await.is_err());
        let hex_key = RotationKey::Hex(zeroize::Zeroizing::new(hex::encode(&*k.to_bytes())));
        assert_eq!(hex_key.load(&s).await.unwrap().did_key(), k.did_key());
    }
}
