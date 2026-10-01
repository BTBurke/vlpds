//! Lexicon validation with one generic schema interpreter:
//!
//! - Records, like the reference's `validateRecord`
//!   (packages/pds/src/repo/prepare.ts): a record whose `$type` has a
//!   schema is checked (record key + record body) and reported `valid`;
//!   unknown types are `unknown` unless `validate: true` was requested;
//!   `validate: false` skips validation.
//! - XRPC, like @atproto/lexicon's `assertValidXrpcParams` /
//!   `assertValidXrpcInput`: query params and JSON inputs of the bundled
//!   com.atproto.* methods are checked by the extractors
//!   (src/xrpc/extract.rs) before the handler runs (400 `InvalidRequest`).
//!   Outputs are checked in debug builds only ([`validate_output`], wired as
//!   a route layer), to catch handler bugs in the test suite at no prod cost.
//! - Opt-in dynamic resolution (`Config::resolve_lexicons`): record types
//!   without a bundled schema are resolved like permission sets
//!   (src/oauth/lexicon.rs: DNS `_lexicon` TXT -> DID ->
//!   `com.atproto.lexicon.schema` record with proof) and validated too. The
//!   reference has no such resolution yet (`@TODO` in prepare.ts); a write
//!   waits at most the configured timeout and otherwise treats the type as
//!   unknown, while the resolution finishes in the background and fills the
//!   cache (TTL, negative TTL, size bound) for later writes.
//!
//! `lexicons/bundle.json` bundles every record lexicon of the atproto repo
//! and every com.atproto.* query/procedure, plus the lexicons they
//! reference, as `{nsid: lexicon document}` (regenerate with
//! `lexicons/bundle.py`).
//!
//! Checked: object required/nullable fields, string formats, byte and
//! grapheme lengths, enum/const, integer ranges, arrays, refs, open and
//! closed unions, blobs (accepted MIME types, max size), bytes and
//! cid-links. Like the reference, extra object properties are allowed and
//! open-union members of unknown types are not validated; refs from a
//! resolved lexicon into other unbundled lexicons are not followed.
//! Messages follow @atproto/lexicon (`Input/repo must be a string`).

use crate::cbor::{JsonValue, Value};
use crate::xrpc::syntax;
use crate::xrpc::App;
use futures::FutureExt;
use serde_json::Value as J;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use unicode_segmentation::UnicodeSegmentation;

static LEXICONS: LazyLock<HashMap<String, J>> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../lexicons/bundle.json")).expect("bundled lexicons")
});

/// Result of validating a write: `Some("valid" | "unknown")`, or `None`
/// when validation was skipped (`validate: false`).
pub type ValidationStatus = Option<&'static str>;

/// Validates a record (with its `$type` already set to `collection`).
/// `resolved` is a dynamically resolved lexicon document for `collection`
/// ([`resolve_record_schema`]); bundled schemas take precedence.
pub fn validate_record<N: Node>(
    collection: &str,
    rkey: &str,
    record: &N,
    validate: Option<bool>,
    resolved: Option<&J>,
) -> Result<ValidationStatus, String> {
    if validate == Some(false) {
        return Ok(None);
    }
    let resolved = resolved.filter(|d| d["id"] == collection);
    let main = def(collection, "main").filter(|d| is_record(d)).or_else(|| {
        resolved
            .and_then(|d| d.get("defs")?.get("main"))
            .filter(|d| is_record(d))
    });
    let Some(main) = main else {
        if validate == Some(true) {
            return Err(format!("Unknown lexicon type: {collection}"));
        }
        return Ok(Some("unknown"));
    };
    let key = main["key"].as_str().unwrap_or("any");
    let key_ok = match key {
        "tid" => syntax::valid_tid(rkey),
        "nsid" => syntax::valid_nsid(rkey),
        "any" | "record-key" => syntax::valid_rkey(rkey),
        k => match k.strip_prefix("literal:") {
            Some(lit) => rkey == lit,
            None => syntax::valid_rkey(rkey),
        },
    };
    if !key_ok {
        return Err(format!(
            "Invalid record key for {collection}: must be {key}, got {rkey:?}"
        ));
    }
    let mut v = Validator::new("record", resolved);
    v.check(&main["record"], record, collection)
        .map_err(|e| format!("Invalid {collection} record: {e}"))?;
    Ok(Some("valid"))
}

fn is_record(d: &J) -> bool {
    d["type"] == "record"
}

fn def(nsid: &str, name: &str) -> Option<&'static J> {
    LEXICONS.get(nsid)?.get("defs")?.get(name)
}

/// `#name` / `nsid#name` / `nsid` -> (nsid, name), relative to `ctx`.
fn split_ref<'a>(r: &'a str, ctx: &'a str) -> (&'a str, &'a str) {
    match r.split_once('#') {
        Some(("", name)) => (ctx, name),
        Some((nsid, name)) => (nsid, name),
        None => (r, "main"),
    }
}

// ---------------------------------------------------------------------------
// XRPC params / input / output
// ---------------------------------------------------------------------------

/// The bundled `query` / `procedure` definition of `nsid`.
fn method(nsid: &str) -> Option<&'static J> {
    def(nsid, "main").filter(|d| matches!(d["type"].as_str(), Some("query" | "procedure")))
}

/// The JSON schema of a method's `input` / `output` payload.
fn payload_schema<'a>(m: &'a J, which: &str) -> Option<&'a J> {
    let p = m.get(which)?;
    (p["encoding"] == "application/json").then(|| p.get("schema")).flatten()
}

/// Methods whose vlpds input deliberately extends the lexicon:
/// updateAccountSigningKey generates a key when `signingKey` is omitted.
const EXTENDED_INPUTS: &[&str] = &["com.atproto.admin.updateAccountSigningKey"];

/// Whether `nsid` is a bundled method with a JSON input schema.
pub fn has_input_schema(nsid: &str) -> bool {
    !EXTENDED_INPUTS.contains(&nsid) && method(nsid).and_then(|m| payload_schema(m, "input")).is_some()
}

/// Whether `nsid` is a bundled method with parameters.
pub fn has_params(nsid: &str) -> bool {
    method(nsid).is_some_and(|m| m.get("parameters").is_some())
}

/// Checks a procedure's JSON body (`Input ...` messages).
pub fn validate_input<N: Node>(nsid: &str, body: &N) -> Result<(), String> {
    validate_payload(nsid, "input", "Input", body)
}

/// Checks a method's JSON response body (`Output ...` messages).
pub fn validate_output(nsid: &str, body: &J) -> Result<(), String> {
    validate_payload(nsid, "output", "Output", body)
}

fn validate_payload<N: Node>(nsid: &str, which: &str, root: &str, body: &N) -> Result<(), String> {
    if which == "input" && EXTENDED_INPUTS.contains(&nsid) {
        return Ok(());
    }
    let Some(schema) = method(nsid).and_then(|m| payload_schema(m, which)) else {
        return Ok(());
    };
    if !matches!(body.kind(), Kind::Map) {
        return Err(format!("{root} must be an object"));
    }
    Validator::new(root, None).check(schema, body, nsid)
}

/// Checks a method's query params (the raw `key=value` pairs, decoded per
/// the param types like the reference's `decodeQueryParams`). Empty values
/// count as absent; unknown params are ignored.
pub fn validate_params(nsid: &str, pairs: &[(String, String)]) -> Result<(), String> {
    let Some(params) = method(nsid).and_then(|m| m.get("parameters")) else {
        return Ok(());
    };
    let Some(props) = params.get("properties").and_then(|p| p.as_object()) else {
        return Ok(());
    };
    let required = |k: &str| {
        params
            .get("required")
            .and_then(|r| r.as_array())
            .is_some_and(|r| r.iter().any(|x| x == k))
    };
    for (k, pd) in props {
        let is_array = pd["type"] == "array";
        let item_type = if is_array { pd["items"]["type"].as_str() } else { pd["type"].as_str() };
        let decode = |s: &str| match item_type {
            Some("integer") => s.parse::<i64>().map(J::from).unwrap_or_else(|_| J::String(s.into())),
            Some("boolean") => match s {
                "true" => J::Bool(true),
                "false" => J::Bool(false),
                _ => J::String(s.into()),
            },
            _ => J::String(s.into()),
        };
        let mut vals = pairs
            .iter()
            .filter(|(pk, v)| pk == k && !v.is_empty())
            .map(|(_, v)| decode(v));
        let value = if is_array {
            let a: Vec<J> = vals.collect();
            (!a.is_empty()).then_some(J::Array(a))
        } else {
            vals.next()
        };
        match value {
            None if required(k) => return Err(format!("Params must have the property \"{k}\"")),
            None => {}
            Some(v) => Validator::new(k, None).check(pd, &v, nsid)?,
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// dynamic resolution
// ---------------------------------------------------------------------------

/// Default time a write waits for a lexicon resolution (`--resolve-lexicons`).
pub const RESOLVE_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a resolved lexicon is used before it is resolved again.
const RESOLVED_TTL: Duration = Duration::from_secs(600);
/// How long a failed resolution is remembered (negative cache).
const NEGATIVE_TTL: Duration = Duration::from_secs(60);
/// Most cached resolutions (resolved or failed).
const MAX_RESOLVED: usize = 4096;

type Resolution = futures::future::Shared<futures::future::BoxFuture<'static, Option<Arc<J>>>>;

enum Slot {
    Done { at: Instant, doc: Option<Arc<J>> },
    /// In flight; `prev` is the last good document, served meanwhile.
    Pending { fut: Resolution, prev: Option<Arc<J>> },
}

impl Slot {
    fn fresh(&self) -> bool {
        match self {
            Slot::Done { at, doc } => at.elapsed() < if doc.is_some() { RESOLVED_TTL } else { NEGATIVE_TTL },
            Slot::Pending { .. } => true,
        }
    }
}

static RESOLVED: LazyLock<parking_lot::Mutex<HashMap<String, Slot>>> =
    LazyLock::new(Default::default);

/// The dynamically resolved lexicon document for a record type, when
/// resolution is enabled, validation isn't skipped and no schema is bundled.
/// Waits at most `Config::resolve_lexicons` for a resolution; None (the
/// record is then `unknown`) on failure or timeout. Concurrent writes of
/// the same type share one resolution.
pub async fn resolve_record_schema(
    app: &Arc<App>,
    collection: &str,
    validate: Option<bool>,
) -> Option<Arc<J>> {
    let timeout = app.config.resolve_lexicons?;
    if validate == Some(false)
        || def(collection, "main").is_some_and(is_record)
        || !syntax::valid_nsid(collection)
    {
        return None;
    }
    let (fut, prev) = {
        let mut m = RESOLVED.lock();
        match m.get(collection) {
            Some(Slot::Pending { fut, prev }) => (fut.clone(), prev.clone()),
            Some(s @ Slot::Done { doc, .. }) if s.fresh() => return doc.clone(),
            other => {
                let prev = match other {
                    Some(Slot::Done { doc, .. }) => doc.clone(),
                    _ => None,
                };
                if m.len() >= MAX_RESOLVED {
                    evict(&mut m);
                }
                let fut = spawn_resolution(app.clone(), collection.to_string(), prev.clone());
                m.insert(
                    collection.to_string(),
                    Slot::Pending { fut: fut.clone(), prev: prev.clone() },
                );
                (fut, prev)
            }
        }
    };
    tokio::time::timeout(timeout, fut).await.unwrap_or(prev)
}

/// Drops expired entries, then the oldest finished one if still full.
fn evict(m: &mut HashMap<String, Slot>) {
    m.retain(|_, s| s.fresh());
    if m.len() >= MAX_RESOLVED {
        let oldest = m
            .iter()
            .filter_map(|(k, s)| match s {
                Slot::Done { at, .. } => Some((*at, k.clone())),
                Slot::Pending { .. } => None,
            })
            .min();
        if let Some((_, k)) = oldest {
            m.remove(&k);
        }
    }
}

/// Resolves in a task of its own, so a write that stops waiting doesn't
/// cancel it; the result lands in the cache either way. A failed refresh
/// keeps serving the last good document.
fn spawn_resolution(app: Arc<App>, nsid: String, prev: Option<Arc<J>>) -> Resolution {
    let task = tokio::spawn(async move {
        let doc = crate::oauth::lexicon::resolve(&app, &nsid)
            .await
            .and_then(|(_, doc)| record_lexicon(&nsid, doc));
        let doc = match doc {
            Ok(d) => Some(Arc::new(d)),
            Err(e) => {
                tracing::debug!(nsid, "lexicon resolution failed: {e}");
                prev
            }
        };
        RESOLVED
            .lock()
            .insert(nsid, Slot::Done { at: Instant::now(), doc: doc.clone() });
        doc
    });
    async move { task.await.ok().flatten() }.boxed().shared()
}

/// A resolved lexicon document usable for record validation.
fn record_lexicon(nsid: &str, doc: J) -> Result<J, String> {
    if doc["lexicon"].as_i64() != Some(1) || doc["id"] != nsid {
        return Err(format!("Invalid Lexicon document for {nsid}"));
    }
    if !doc["defs"]["main"].get("record").is_some_and(|r| r.is_object())
        || !is_record(&doc["defs"]["main"])
    {
        return Err(format!("Lexicon {nsid} is not a record type"));
    }
    Ok(doc)
}

// ---------------------------------------------------------------------------
// interpreter
// ---------------------------------------------------------------------------

/// A data-model value the interpreter can check: DAG-CBOR records
/// ([`Value`]) and JSON bodies ([`J`], [`JsonValue`]: `$link` / `$bytes`
/// objects are CIDs and bytes, as the reference's `jsonToLex`). Record
/// writes validate the parsed [`JsonValue`] once it has been encoded
/// ([`JsonValue::encode_record`] leaves it equal to the record).
pub trait Node: Sized {
    fn kind(&self) -> Kind<'_, Self>;
    fn get(&self, key: &str) -> Option<&Self>;
}

pub enum Kind<'a, N> {
    Null,
    Bool(bool),
    Int(i64),
    Float,
    Text(&'a str),
    /// Byte length.
    Bytes(usize),
    Link,
    Array(&'a [N]),
    Map,
}

impl Node for Value {
    fn kind(&self) -> Kind<'_, Value> {
        match self {
            Value::Null => Kind::Null,
            Value::Bool(b) => Kind::Bool(*b),
            Value::Int(n) => Kind::Int(*n),
            Value::Bytes(b) => Kind::Bytes(b.len()),
            Value::Text(s) => Kind::Text(s),
            Value::Array(a) => Kind::Array(a),
            Value::Map(_) => Kind::Map,
            Value::Link(_) => Kind::Link,
        }
    }

    fn get(&self, key: &str) -> Option<&Value> {
        Value::get(self, key)
    }
}

impl Node for J {
    fn kind(&self) -> Kind<'_, J> {
        match self {
            J::Null => Kind::Null,
            J::Bool(b) => Kind::Bool(*b),
            J::Number(n) => n.as_i64().map_or(Kind::Float, Kind::Int),
            J::String(s) => Kind::Text(s),
            J::Array(a) => Kind::Array(a),
            J::Object(o) => match (o.len(), o.get("$link"), o.get("$bytes")) {
                (1, Some(J::String(_)), _) => Kind::Link,
                (1, _, Some(J::String(b))) => {
                    Kind::Bytes(b.trim_end_matches('=').len() * 3 / 4)
                }
                _ => Kind::Map,
            },
        }
    }

    fn get(&self, key: &str) -> Option<&J> {
        self.as_object()?.get(key)
    }
}

impl Node for JsonValue<'_> {
    fn kind(&self) -> Kind<'_, Self> {
        match self {
            JsonValue::Null => Kind::Null,
            JsonValue::Bool(b) => Kind::Bool(*b),
            JsonValue::Int(n) => Kind::Int(*n),
            JsonValue::Float(_) | JsonValue::BigUint(_) => Kind::Float,
            JsonValue::Str(s) => Kind::Text(s),
            JsonValue::Array(a) => Kind::Array(a),
            JsonValue::Object(o) => match &o[..] {
                [(k, JsonValue::Str(_))] if k == "$link" => Kind::Link,
                [(k, JsonValue::Str(b))] if k == "$bytes" => {
                    Kind::Bytes(b.trim_end_matches('=').len() * 3 / 4)
                }
                _ => Kind::Map,
            },
        }
    }

    fn get(&self, key: &str) -> Option<&Self> {
        JsonValue::get(self, key)
    }
}

fn text<N: Node>(v: Option<&N>) -> Option<&str> {
    match v?.kind() {
        Kind::Text(s) => Some(s),
        _ => None,
    }
}

/// Most schema indirections (refs, union members) followed at once: a
/// resolved (untrusted) lexicon may reference itself in a cycle.
const MAX_DEPTH: u32 = 128;

/// A step of the path in error messages (`Input/writes/0/collection`),
/// formatted only when there is an error.
enum Seg<'a> {
    Key(&'a str),
    Index(usize),
}

struct Validator<'a> {
    root: String,
    path: Vec<Seg<'a>>,
    /// A resolved lexicon document, consulted before the bundle.
    doc: Option<&'a J>,
    depth: u32,
}

impl<'a> Validator<'a> {
    fn new(root: &str, doc: Option<&'a J>) -> Self {
        Validator { root: root.to_string(), path: Vec::new(), doc, depth: 0 }
    }

    fn def(&self, nsid: &str, name: &str) -> Option<&'a J> {
        match self.doc {
            Some(d) if d["id"] == nsid => d.get("defs")?.get(name),
            _ => def(nsid, name),
        }
    }

    fn err(&self, m: impl std::fmt::Display) -> String {
        let mut p = self.root.clone();
        for seg in &self.path {
            match seg {
                Seg::Key(k) => {
                    p.push('/');
                    p.push_str(k);
                }
                Seg::Index(i) => p.push_str(&format!("/{i}")),
            }
        }
        format!("{p} {m}")
    }

    fn nested<T>(&mut self, seg: Seg<'a>, f: impl FnOnce(&mut Self) -> T) -> T {
        self.path.push(seg);
        let r = f(self);
        self.path.pop();
        r
    }

    /// Checks `v` against the definition `target`, one indirection deeper.
    fn follow<N: Node>(&mut self, target: &'a J, v: &N, ctx: &str) -> Result<(), String> {
        if self.depth >= MAX_DEPTH {
            return Err(self.err("exceeds the maximum schema depth"));
        }
        self.depth += 1;
        let r = self.check(target, v, ctx);
        self.depth -= 1;
        r
    }

    fn check<N: Node>(&mut self, d: &'a J, v: &N, ctx: &str) -> Result<(), String> {
        let t = d["type"].as_str().unwrap_or("");
        match t {
            "object" | "record" => {
                let d = if t == "record" { &d["record"] } else { d };
                self.object(d, v, ctx)
            }
            "ref" => {
                let (nsid, name) = split_ref(d["ref"].as_str().unwrap_or(""), ctx);
                match self.def(nsid, name) {
                    Some(target) => self.follow(target, v, nsid),
                    // a schema we don't have: can't validate further
                    None => Ok(()),
                }
            }
            "union" => self.union(d, v, ctx),
            "string" => self.string(d, v),
            "integer" => {
                let Kind::Int(n) = v.kind() else {
                    return Err(self.err("must be an integer"));
                };
                if let Some(c) = d.get("const") {
                    if c.as_i64() != Some(n) {
                        return Err(self.err(format!("must be {c}")));
                    }
                }
                if let Some(e) = d.get("enum").and_then(|e| e.as_array()) {
                    if !e.iter().any(|x| x.as_i64() == Some(n)) {
                        return Err(self.err(format!("must be one of ({})", join(e, "|"))));
                    }
                }
                if d.get("minimum").and_then(|m| m.as_i64()).is_some_and(|m| n < m) {
                    return Err(self.err(format!("can not be less than {}", d["minimum"])));
                }
                if d.get("maximum").and_then(|m| m.as_i64()).is_some_and(|m| n > m) {
                    return Err(self.err(format!("can not be greater than {}", d["maximum"])));
                }
                Ok(())
            }
            "boolean" => match v.kind() {
                Kind::Bool(b) => match d.get("const").and_then(|c| c.as_bool()) {
                    Some(c) if c != b => Err(self.err(format!("must be {c}"))),
                    _ => Ok(()),
                },
                _ => Err(self.err("must be a boolean")),
            },
            "bytes" => {
                let Kind::Bytes(n) = v.kind() else {
                    return Err(self.err("must be a byte array"));
                };
                if d.get("maxLength").and_then(|m| m.as_u64()).is_some_and(|m| n as u64 > m) {
                    return Err(self.err(format!("must not be larger than {} bytes", d["maxLength"])));
                }
                if d.get("minLength").and_then(|m| m.as_u64()).is_some_and(|m| (n as u64) < m) {
                    return Err(self.err(format!("must not be smaller than {} bytes", d["minLength"])));
                }
                Ok(())
            }
            "cid-link" => match v.kind() {
                Kind::Link => Ok(()),
                _ => Err(self.err("must be a CID")),
            },
            "blob" => self.blob(d, v),
            "array" => {
                let Kind::Array(items) = v.kind() else {
                    return Err(self.err("must be an array"));
                };
                let n = items.len() as u64;
                if d.get("maxLength").and_then(|m| m.as_u64()).is_some_and(|m| n > m) {
                    return Err(self.err(format!("must not have more than {} elements", d["maxLength"])));
                }
                if d.get("minLength").and_then(|m| m.as_u64()).is_some_and(|m| n < m) {
                    return Err(self.err(format!("must not have fewer than {} elements", d["minLength"])));
                }
                if let Some(item) = d.get("items") {
                    for (i, x) in items.iter().enumerate() {
                        self.nested(Seg::Index(i), |s| s.check(item, x, ctx))?;
                    }
                }
                Ok(())
            }
            "unknown" => match v.kind() {
                Kind::Map => Ok(()),
                _ => Err(self.err("must be an object")),
            },
            "null" => match v.kind() {
                Kind::Null => Ok(()),
                _ => Err(self.err("must be null")),
            },
            // token / params / anything else: nothing to check here
            _ => Ok(()),
        }
    }

    fn object<N: Node>(&mut self, d: &'a J, v: &N, ctx: &str) -> Result<(), String> {
        if !matches!(v.kind(), Kind::Map) {
            return Err(self.err("must be an object"));
        }
        let nullable = |k: &str| {
            d.get("nullable")
                .and_then(|n| n.as_array())
                .is_some_and(|n| n.iter().any(|x| x == k))
        };
        if let Some(req) = d.get("required").and_then(|r| r.as_array()) {
            for k in req.iter().filter_map(|k| k.as_str()) {
                let missing = match v.get(k) {
                    None => true,
                    Some(x) => matches!(x.kind(), Kind::Null) && !nullable(k),
                };
                if missing {
                    return Err(self.err(format!("must have the property \"{k}\"")));
                }
            }
        }
        if let Some(props) = d.get("properties").and_then(|p| p.as_object()) {
            for (k, pd) in props {
                let Some(x) = v.get(k) else { continue };
                if matches!(x.kind(), Kind::Null) && nullable(k) {
                    continue;
                }
                self.nested(Seg::Key(k), |s| s.check(pd, x, ctx))?;
            }
        }
        Ok(())
    }

    fn union<N: Node>(&mut self, d: &'a J, v: &N, ctx: &str) -> Result<(), String> {
        let t = match v.kind() {
            Kind::Map => text(v.get("$type")),
            _ => None,
        };
        let Some(t) = t else {
            return Err(self.err("must be an object which includes the \"$type\" property"));
        };
        let (tn, tname) = split_ref(t, ctx);
        let hit = d["refs"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|r| r.as_str())
            .map(|r| split_ref(r, ctx))
            .find(|(n, name)| *n == tn && *name == tname);
        match hit {
            Some((nsid, name)) => match self.def(nsid, name) {
                Some(target) => {
                    let nsid = nsid.to_string();
                    self.follow(target, v, &nsid)
                }
                None => Ok(()),
            },
            None if d["closed"].as_bool() == Some(true) => Err(self.err(format!(
                "$type must be one of {}",
                join(&d["refs"].as_array().cloned().unwrap_or_default(), ", ")
            ))),
            None => Ok(()),
        }
    }

    fn string<N: Node>(&mut self, d: &J, v: &N) -> Result<(), String> {
        let Kind::Text(s) = v.kind() else {
            return Err(self.err("must be a string"));
        };
        if let Some(c) = d.get("const").and_then(|c| c.as_str()) {
            if c != s {
                return Err(self.err(format!("must be {c}")));
            }
        }
        if let Some(e) = d.get("enum").and_then(|e| e.as_array()) {
            if !e.iter().any(|x| x.as_str() == Some(s)) {
                return Err(self.err(format!("must be one of ({})", join(e, "|"))));
            }
        }
        // lengths are UTF-8 bytes, worded as characters like the reference
        let n = s.len() as u64;
        if d.get("maxLength").and_then(|m| m.as_u64()).is_some_and(|m| n > m) {
            return Err(self.err(format!("must not be longer than {} characters", d["maxLength"])));
        }
        if d.get("minLength").and_then(|m| m.as_u64()).is_some_and(|m| n < m) {
            return Err(self.err(format!("must not be shorter than {} characters", d["minLength"])));
        }
        let (min_g, max_g) = (
            d.get("minGraphemes").and_then(|m| m.as_u64()),
            d.get("maxGraphemes").and_then(|m| m.as_u64()),
        );
        if min_g.is_some() || max_g.is_some() {
            // cheap bounds first: graphemes <= chars <= bytes
            let n = if max_g.is_some_and(|m| s.len() as u64 <= m) && min_g.is_none() {
                0
            } else {
                s.graphemes(true).count() as u64
            };
            if min_g.is_some_and(|m| n < m) {
                return Err(self.err(format!("must not be shorter than {} graphemes", min_g.unwrap())));
            }
            if max_g.is_some_and(|m| n > m) {
                return Err(self.err(format!("must not be longer than {} graphemes", max_g.unwrap())));
            }
        }
        if let Some(f) = d.get("format").and_then(|f| f.as_str()) {
            // (valid, @atproto/lexicon message)
            let (ok, msg) = match f {
                "datetime" => (valid_datetime(s), "must be an valid atproto datetime (both RFC-3339 and ISO-8601)"),
                "uri" => (valid_uri(s), "must be a uri"),
                "at-uri" => (valid_at_uri(s), "must be a valid at-uri"),
                "did" => (syntax::valid_did(s), "must be a valid did"),
                "handle" => (syntax::valid_handle(s), "must be a valid handle"),
                "at-identifier" => (
                    crate::xrpc::extract::valid_at_identifier(s),
                    "must be a valid did or a handle",
                ),
                "nsid" => (syntax::valid_nsid(s), "must be a valid nsid"),
                "cid" => (
                    crate::cid::Cid::parse(s).is_ok() || crate::xrpc::extract::valid_cid_syntax(s),
                    "must be a cid string",
                ),
                "language" => (valid_language(s), "must be a well-formed BCP 47 language tag"),
                "tid" => (syntax::valid_tid(s), "must be a valid TID"),
                "record-key" => (syntax::valid_rkey(s), "must be a valid Record Key"),
                _ => (true, ""),
            };
            if !ok {
                return Err(self.err(msg));
            }
        }
        Ok(())
    }

    fn blob<N: Node>(&mut self, d: &J, v: &N) -> Result<(), String> {
        if !matches!(v.kind(), Kind::Map) || text(v.get("$type")) != Some("blob") {
            return Err(self.err("should be a blob ref"));
        }
        let Some(mime) = text(v.get("mimeType")) else {
            return Err(self.err("should be a blob ref"));
        };
        let size = match v.get("size").map(|s| s.kind()) {
            Some(Kind::Int(n)) => Some(n),
            _ => None,
        };
        if let Some(accept) = d.get("accept").and_then(|a| a.as_array()) {
            let ok = accept.iter().filter_map(|a| a.as_str()).any(|a| {
                a == "*/*"
                    || a == mime
                    || a.strip_suffix("/*").is_some_and(|p| mime.split('/').next() == Some(p))
            });
            if !ok {
                return Err(self.err(format!(
                    "mime type {mime:?} is not accepted (accepted: {})",
                    d["accept"]
                )));
            }
        }
        if let (Some(max), Some(size)) = (d.get("maxSize").and_then(|m| m.as_i64()), size) {
            if size > max {
                return Err(self.err(format!("file is too large: {size} bytes (max {max})")));
            }
        }
        Ok(())
    }
}

/// Enum values / refs joined like JS `Array.join` (strings unquoted).
fn join(items: &[J], sep: &str) -> String {
    items
        .iter()
        .map(|x| x.as_str().map_or_else(|| x.to_string(), String::from))
        .collect::<Vec<_>>()
        .join(sep)
}

// ---------------------------------------------------------------------------
// string formats (@atproto/syntax)
// ---------------------------------------------------------------------------

fn digits(s: &str) -> Option<u32> {
    (!s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())).then(|| s.parse().ok())?
}

/// RFC 3339 / ISO 8601 intersection with `Z` or `±hh:mm`, not `-00:00`,
/// a real calendar date, and within years 0000-9999 once normalized to UTC.
pub fn valid_datetime(s: &str) -> bool {
    if s.len() > 64 || s.ends_with("-00:00") || !s.is_ascii() {
        return false;
    }
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return false;
    }
    let (Some(year), Some(month), Some(day), Some(hour), Some(min), Some(sec)) = (
        digits(&s[0..4]),
        digits(&s[5..7]),
        digits(&s[8..10]),
        digits(&s[11..13]),
        digits(&s[14..16]),
        digits(&s[17..19]),
    ) else {
        return false;
    };
    let mut rest = &s[19..];
    if let Some(r) = rest.strip_prefix('.') {
        let n = r.bytes().take_while(|b| b.is_ascii_digit()).count();
        if n == 0 {
            return false;
        }
        rest = &r[n..];
    }
    let offset_min: i64 = match rest {
        "Z" => 0,
        _ => {
            let ob = rest.as_bytes();
            if ob.len() != 6 || !matches!(ob[0], b'+' | b'-') || ob[3] != b':' {
                return false;
            }
            let (Some(oh), Some(om)) = (digits(&rest[1..3]), digits(&rest[4..6])) else {
                return false;
            };
            if oh > 23 || om > 59 {
                return false;
            }
            let m = (oh * 60 + om) as i64;
            if ob[0] == b'-' {
                -m
            } else {
                m
            }
        }
    };
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let mdays = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if !(1..=12).contains(&month) || day < 1 || day > mdays[month as usize - 1] {
        return false;
    }
    if hour > 23 || min > 59 || sec > 59 {
        return false;
    }
    // normalized to UTC it must stay within 0000-01-01 .. 9999-12-31
    let minutes = (hour * 60 + min) as i64 - offset_min;
    if year == 0 && month == 1 && day == 1 && minutes < 0 {
        return false;
    }
    if year == 9999 && month == 12 && day == 31 && minutes >= 24 * 60 {
        return false;
    }
    true
}

/// `scheme:[//]rest` with no whitespace (@atproto/syntax isUriString).
pub fn valid_uri(s: &str) -> bool {
    let Some((scheme, rest)) = s.split_once(':') else {
        return false;
    };
    let rest = rest.strip_prefix("//").unwrap_or(rest);
    s.len() <= 8192
        && !scheme.is_empty()
        && scheme.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && rest.chars().next().is_some_and(|c| c != '/' && !c.is_whitespace())
        && !rest.chars().any(char::is_whitespace)
}

/// Strict AT URI: `at://AUTHORITY[/NSID[/RKEY]]`.
pub fn valid_at_uri(s: &str) -> bool {
    if s.len() > 8192 || !s.is_ascii() {
        return false;
    }
    let Some(rest) = s.strip_prefix("at://") else {
        return false;
    };
    let mut parts = rest.split('/');
    let authority = parts.next().unwrap_or("");
    if !crate::xrpc::extract::valid_at_identifier(authority) {
        return false;
    }
    match (parts.next(), parts.next(), parts.next()) {
        (None, ..) => true,
        (Some(c), None, _) => syntax::valid_nsid(c),
        (Some(c), Some(r), None) => syntax::valid_nsid(c) && syntax::valid_rkey(r),
        _ => false,
    }
}

/// BCP 47 language tag shape (@atproto/syntax isLanguageString, simplified).
pub fn valid_language(s: &str) -> bool {
    let mut parts = s.split('-');
    let first = parts.next().unwrap_or("");
    let first_ok = first == "i"
        || first == "x"
        || ((2..=3).contains(&first.len()) && first.bytes().all(|b| b.is_ascii_alphabetic()));
    first_ok && parts.all(|p| (1..=8).contains(&p.len()) && p.bytes().all(|b| b.is_ascii_alphanumeric()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datetimes() {
        for ok in [
            "1985-04-12T23:20:50.123Z",
            "1985-04-12T23:20:50Z",
            "1985-04-12T23:20:50.123-07:00",
            "0000-01-01T00:00:00.000Z",
            "2024-02-29T00:00:00Z",
        ] {
            assert!(valid_datetime(ok), "{ok}");
        }
        for bad in [
            "1985-04-12T23:20:50.123z",
            "1985-04-12T23:20:50.Z",
            "1985-04-12T23:20:50.123-00:00",
            "1985-04-12T23:20:50.123+0000",
            "1985-00-12T23:20:50.123Z",
            "2023-02-29T00:00:00Z",
            "0000-01-01T00:00:00+01:00",
            "1985-04-12",
        ] {
            assert!(!valid_datetime(bad), "{bad}");
        }
    }

    #[test]
    fn bundled_lexicons_parse() {
        assert!(def("app.bsky.feed.post", "main").is_some());
        let rec = Value::from_json(&serde_json::json!({
            "$type": "app.bsky.feed.post", "text": "hi", "createdAt": "2024-01-01T00:00:00Z"
        }))
        .unwrap();
        assert_eq!(validate_record("app.bsky.feed.post", "3jui7kd54zh2y", &rec, None, None), Ok(Some("valid")));
        let bad = Value::from_json(&serde_json::json!({"$type": "app.bsky.feed.post", "createdAt": "x"})).unwrap();
        assert!(validate_record("app.bsky.feed.post", "3jui7kd54zh2y", &bad, None, None).is_err());
        assert!(validate_record("app.bsky.actor.profile", "3jui7kd54zh2y", &rec, None, None).is_err());
    }

    #[test]
    fn resolved_record_lexicon() {
        let doc = serde_json::json!({
            "lexicon": 1, "id": "com.example.thing",
            "defs": {
                "main": {"type": "record", "key": "tid", "record": {"type": "object",
                    "required": ["n"],
                    "properties": {"n": {"type": "integer", "maximum": 5}, "loop": {"type": "ref", "ref": "#loop"}}}},
                "loop": {"type": "ref", "ref": "#loop"}
            }
        });
        let rec = |j| Value::from_json(&j).unwrap();
        let ok = rec(serde_json::json!({"$type": "com.example.thing", "n": 3}));
        assert_eq!(validate_record("com.example.thing", "3jui7kd54zh2y", &ok, None, Some(&doc)), Ok(Some("valid")));
        assert_eq!(validate_record("com.example.thing", "3jui7kd54zh2y", &ok, None, None), Ok(Some("unknown")));
        let big = rec(serde_json::json!({"$type": "com.example.thing", "n": 9}));
        assert_eq!(
            validate_record("com.example.thing", "3jui7kd54zh2y", &big, None, Some(&doc)),
            Err("Invalid com.example.thing record: record/n can not be greater than 5".into())
        );
        // a self-referencing def is cut off instead of overflowing the stack
        let cyc = rec(serde_json::json!({"$type": "com.example.thing", "n": 1, "loop": {}}));
        assert!(validate_record("com.example.thing", "3jui7kd54zh2y", &cyc, None, Some(&doc))
            .unwrap_err()
            .contains("maximum schema depth"));
        // a document for another NSID is ignored
        assert_eq!(validate_record("com.example.other", "x", &ok, None, Some(&doc)), Ok(Some("unknown")));
    }

    #[test]
    fn xrpc_input_and_params() {
        let n = "com.atproto.repo.createRecord";
        assert!(has_input_schema(n) && !has_input_schema("com.atproto.repo.getRecord"));
        let body = serde_json::json!({"repo": "did:plc:abc", "collection": "app.bsky.feed.post", "record": {}});
        assert_eq!(validate_input(n, &body), Ok(()));
        assert_eq!(
            validate_input(n, &serde_json::json!({"repo": "did:plc:abc", "record": {}})),
            Err("Input must have the property \"collection\"".into())
        );
        assert_eq!(
            validate_input(n, &serde_json::json!({"repo": 1, "collection": "a.b.c", "record": {}})),
            Err("Input/repo must be a string".into())
        );
        assert_eq!(validate_input(n, &serde_json::json!([])), Err("Input must be an object".into()));
        let aw = serde_json::json!({"repo": "did:plc:abc", "writes": [{"$type": "com.atproto.repo.applyWrites#nope"}]});
        assert!(validate_input("com.atproto.repo.applyWrites", &aw)
            .unwrap_err()
            .starts_with("Input/writes/0 $type must be one of #create, #update, #delete"));
        let p = |v: &[(&str, &str)]| v.iter().map(|(a, b)| (a.to_string(), b.to_string())).collect::<Vec<_>>();
        let q = "com.atproto.repo.listRecords";
        assert_eq!(validate_params(q, &p(&[("repo", "did:plc:abc"), ("collection", "a.b.c")])), Ok(()));
        assert_eq!(validate_params(q, &p(&[("repo", "did:plc:abc")])), Err("Params must have the property \"collection\"".into()));
        assert_eq!(
            validate_params(q, &p(&[("repo", "did:plc:abc"), ("collection", "a.b.c"), ("limit", "500")])),
            Err("limit can not be greater than 100".into())
        );
        assert_eq!(
            validate_params(q, &p(&[("repo", "did:plc:abc"), ("collection", "a.b.c"), ("reverse", "maybe")])),
            Err("reverse must be a boolean".into())
        );
        // $link / $bytes objects are CIDs and bytes in JSON
        let out = serde_json::json!({"uri": "at://did:plc:abc/a.b.c/x", "cid": "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm", "value": {}});
        assert_eq!(validate_output("com.atproto.repo.getRecord", &out), Ok(()));
        assert_eq!(
            validate_output("com.atproto.repo.getRecord", &serde_json::json!({"value": {}})),
            Err("Output must have the property \"uri\"".into())
        );
    }
}
