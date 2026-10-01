//! Record validation against known lexicons, like the reference's
//! `validateRecord` (packages/pds/src/repo/prepare.ts): a record whose
//! `$type` has a bundled schema is checked (record key + record body) and
//! reported `valid`; unknown types are `unknown` unless `validate: true`
//! was requested; `validate: false` skips validation.
//!
//! `lexicons/records.json` bundles every record lexicon of the atproto
//! repo plus the lexicons they reference, as `{nsid: lexicon document}`
//! (regenerate with `lexicons/bundle.py`).
//!
//! Checked: object required/nullable fields, string formats, byte and
//! grapheme lengths, enum/const, integer ranges, arrays, refs, open and
//! closed unions, blobs (accepted MIME types, max size), bytes and
//! cid-links. Like the reference, extra object properties are allowed and
//! open-union members of unknown types are not validated.

use crate::cbor::Value;
use crate::xrpc::syntax;
use serde_json::Value as J;
use std::collections::HashMap;
use std::sync::LazyLock;
use unicode_segmentation::UnicodeSegmentation;

static LEXICONS: LazyLock<HashMap<String, J>> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../lexicons/records.json")).expect("bundled lexicons")
});

/// Result of validating a write: `Some("valid" | "unknown")`, or `None`
/// when validation was skipped (`validate: false`).
pub type ValidationStatus = Option<&'static str>;

/// Validates a record (with its `$type` already set to `collection`).
pub fn validate_record(
    collection: &str,
    rkey: &str,
    record: &Value,
    validate: Option<bool>,
) -> Result<ValidationStatus, String> {
    if validate == Some(false) {
        return Ok(None);
    }
    let Some(main) = def(collection, "main").filter(|d| d["type"] == "record") else {
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
    let mut v = Validator { path: vec!["record".into()] };
    v.check(&main["record"], record, collection)
        .map_err(|e| format!("Invalid {collection} record: {e}"))?;
    Ok(Some("valid"))
}

fn def<'a>(nsid: &str, name: &str) -> Option<&'a J> {
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

struct Validator {
    path: Vec<String>,
}

impl Validator {
    fn err(&self, m: impl std::fmt::Display) -> String {
        format!("{} {m}", self.path.join("/"))
    }

    fn nested<T>(&mut self, seg: String, f: impl FnOnce(&mut Self) -> T) -> T {
        self.path.push(seg);
        let r = f(self);
        self.path.pop();
        r
    }

    fn check(&mut self, d: &J, v: &Value, ctx: &str) -> Result<(), String> {
        let t = d["type"].as_str().unwrap_or("");
        match t {
            "object" | "record" => {
                let d = if t == "record" { &d["record"] } else { d };
                self.object(d, v, ctx)
            }
            "ref" => {
                let (nsid, name) = split_ref(d["ref"].as_str().unwrap_or(""), ctx);
                match def(nsid, name) {
                    Some(target) => self.check(target, v, nsid),
                    // a schema we don't bundle: can't validate further
                    None => Ok(()),
                }
            }
            "union" => self.union(d, v, ctx),
            "string" => self.string(d, v),
            "integer" => {
                let Value::Int(n) = v else {
                    return Err(self.err("must be an integer"));
                };
                if let Some(c) = d.get("const") {
                    if c.as_i64() != Some(*n) {
                        return Err(self.err(format!("must be {c}")));
                    }
                }
                if let Some(e) = d.get("enum").and_then(|e| e.as_array()) {
                    if !e.iter().any(|x| x.as_i64() == Some(*n)) {
                        return Err(self.err("must be one of the enumerated values"));
                    }
                }
                if d.get("minimum").and_then(|m| m.as_i64()).is_some_and(|m| *n < m) {
                    return Err(self.err(format!("can not be less than {}", d["minimum"])));
                }
                if d.get("maximum").and_then(|m| m.as_i64()).is_some_and(|m| *n > m) {
                    return Err(self.err(format!("can not be greater than {}", d["maximum"])));
                }
                Ok(())
            }
            "boolean" => match v {
                Value::Bool(b) => match d.get("const").and_then(|c| c.as_bool()) {
                    Some(c) if c != *b => Err(self.err(format!("must be {c}"))),
                    _ => Ok(()),
                },
                _ => Err(self.err("must be a boolean")),
            },
            "bytes" => {
                let Value::Bytes(b) = v else {
                    return Err(self.err("must be a byte array"));
                };
                self.len_bounds(d, b.len(), "bytes")
            }
            "cid-link" => match v {
                Value::Link(_) => Ok(()),
                _ => Err(self.err("must be a CID")),
            },
            "blob" => self.blob(d, v),
            "array" => {
                let Value::Array(items) = v else {
                    return Err(self.err("must be an array"));
                };
                self.len_bounds(d, items.len(), "elements")?;
                if let Some(item) = d.get("items") {
                    for (i, x) in items.iter().enumerate() {
                        self.nested(i.to_string(), |s| s.check(item, x, ctx))?;
                    }
                }
                Ok(())
            }
            "unknown" => match v {
                Value::Map(_) => Ok(()),
                _ => Err(self.err("must be an object")),
            },
            "null" => match v {
                Value::Null => Ok(()),
                _ => Err(self.err("must be null")),
            },
            // token / params / anything else: nothing to check here
            _ => Ok(()),
        }
    }

    fn len_bounds(&self, d: &J, n: usize, what: &str) -> Result<(), String> {
        if d.get("minLength").and_then(|m| m.as_u64()).is_some_and(|m| (n as u64) < m) {
            return Err(self.err(format!("must not be smaller than {} {what}", d["minLength"])));
        }
        if d.get("maxLength").and_then(|m| m.as_u64()).is_some_and(|m| (n as u64) > m) {
            return Err(self.err(format!("must not be larger than {} {what}", d["maxLength"])));
        }
        Ok(())
    }

    fn object(&mut self, d: &J, v: &Value, ctx: &str) -> Result<(), String> {
        if !matches!(v, Value::Map(_)) {
            return Err(self.err("must be an object"));
        }
        let nullable = |k: &str| {
            d.get("nullable")
                .and_then(|n| n.as_array())
                .is_some_and(|n| n.iter().any(|x| x == k))
        };
        if let Some(req) = d.get("required").and_then(|r| r.as_array()) {
            for k in req.iter().filter_map(|k| k.as_str()) {
                match v.get(k) {
                    None => return Err(self.err(format!("must have the property \"{k}\""))),
                    Some(Value::Null) if !nullable(k) => {
                        return Err(self.err(format!("must have the property \"{k}\"")))
                    }
                    _ => {}
                }
            }
        }
        if let Some(props) = d.get("properties").and_then(|p| p.as_object()) {
            for (k, pd) in props {
                let Some(x) = v.get(k) else { continue };
                if matches!(x, Value::Null) && nullable(k) {
                    continue;
                }
                self.nested(k.clone(), |s| s.check(pd, x, ctx))?;
            }
        }
        Ok(())
    }

    fn union(&mut self, d: &J, v: &Value, ctx: &str) -> Result<(), String> {
        let Some(t) = v.get("$type").and_then(|t| t.as_str()) else {
            return Err(self.err("must be an object which includes the \"$type\" property"));
        };
        let refs: Vec<(&str, &str)> = d["refs"]
            .as_array()
            .map(|a| a.iter().filter_map(|r| r.as_str()).map(|r| split_ref(r, ctx)).collect())
            .unwrap_or_default();
        let (tn, tname) = split_ref(t, ctx);
        match refs.iter().find(|(n, name)| *n == tn && *name == tname) {
            Some((nsid, name)) => match def(nsid, name) {
                Some(target) => self.check(target, v, nsid),
                None => Ok(()),
            },
            None if d["closed"].as_bool() == Some(true) => Err(self.err(format!(
                "$type must be one of {}",
                d["refs"]
            ))),
            None => Ok(()),
        }
    }

    fn string(&mut self, d: &J, v: &Value) -> Result<(), String> {
        let Value::Text(s) = v else {
            return Err(self.err("must be a string"));
        };
        if let Some(c) = d.get("const").and_then(|c| c.as_str()) {
            if c != s {
                return Err(self.err(format!("must be {c}")));
            }
        }
        if let Some(e) = d.get("enum").and_then(|e| e.as_array()) {
            if !e.iter().any(|x| x.as_str() == Some(s)) {
                return Err(self.err("must be one of the enumerated values"));
            }
        }
        self.len_bounds(d, s.len(), "bytes")?;
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
            let ok = match f {
                "datetime" => valid_datetime(s),
                "uri" => valid_uri(s),
                "at-uri" => valid_at_uri(s),
                "did" => syntax::valid_did(s),
                "handle" => syntax::valid_handle(s),
                "at-identifier" => crate::xrpc::extract::valid_at_identifier(s),
                "nsid" => syntax::valid_nsid(s),
                "cid" => crate::cid::Cid::parse(s).is_ok() || crate::xrpc::extract::valid_cid_syntax(s),
                "language" => valid_language(s),
                "tid" => syntax::valid_tid(s),
                "record-key" => syntax::valid_rkey(s),
                _ => true,
            };
            if !ok {
                return Err(self.err(format!("must be a valid {f}")));
            }
        }
        Ok(())
    }

    fn blob(&mut self, d: &J, v: &Value) -> Result<(), String> {
        let (mime, size) = match (v.get("$type").and_then(|t| t.as_str()), v) {
            (Some("blob"), _) => (
                v.get("mimeType").and_then(|m| m.as_str()),
                match v.get("size") {
                    Some(Value::Int(n)) => Some(*n),
                    _ => None,
                },
            ),
            _ => return Err(self.err("should be a blob ref")),
        };
        let Some(mime) = mime else {
            return Err(self.err("should be a blob ref"));
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
        assert_eq!(validate_record("app.bsky.feed.post", "3jui7kd54zh2y", &rec, None), Ok(Some("valid")));
        let bad = Value::from_json(&serde_json::json!({"$type": "app.bsky.feed.post", "createdAt": "x"})).unwrap();
        assert!(validate_record("app.bsky.feed.post", "3jui7kd54zh2y", &bad, None).is_err());
        assert!(validate_record("app.bsky.actor.profile", "3jui7kd54zh2y", &rec, None).is_err());
    }
}
