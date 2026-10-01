//! Minimal DAG-CBOR: a byte-level encoder for the hot path (MST nodes,
//! commits, firehose frames) and a `Value` tree for records, with JSON
//! conversion following the atproto data model ($link, $bytes).

use crate::cid::{Cid, CID_BYTES_LEN};
use base64::Engine;
use std::borrow::Cow;

// ---------- low-level encoding ----------

#[inline]
pub fn write_head(out: &mut Vec<u8>, major: u8, n: u64) {
    let m = major << 5;
    if n < 24 {
        out.push(m | n as u8);
    } else if n <= u8::MAX as u64 {
        out.push(m | 24);
        out.push(n as u8);
    } else if n <= u16::MAX as u64 {
        out.push(m | 25);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else if n <= u32::MAX as u64 {
        out.push(m | 26);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    } else {
        out.push(m | 27);
        out.extend_from_slice(&n.to_be_bytes());
    }
}

#[inline]
pub fn write_uint(out: &mut Vec<u8>, n: u64) {
    write_head(out, 0, n);
}

#[inline]
pub fn write_int(out: &mut Vec<u8>, n: i64) {
    if n >= 0 {
        write_head(out, 0, n as u64);
    } else {
        write_head(out, 1, (-1 - n) as u64);
    }
}

#[inline]
pub fn write_bytes(out: &mut Vec<u8>, b: &[u8]) {
    write_head(out, 2, b.len() as u64);
    out.extend_from_slice(b);
}

#[inline]
pub fn write_text(out: &mut Vec<u8>, s: &str) {
    write_head(out, 3, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

#[inline]
pub fn write_array_head(out: &mut Vec<u8>, n: usize) {
    write_head(out, 4, n as u64);
}

#[inline]
pub fn write_map_head(out: &mut Vec<u8>, n: usize) {
    write_head(out, 5, n as u64);
}

#[inline]
pub fn write_null(out: &mut Vec<u8>) {
    out.push(0xf6);
}

#[inline]
pub fn write_bool(out: &mut Vec<u8>, b: bool) {
    out.push(if b { 0xf5 } else { 0xf4 });
}

#[inline]
pub fn write_cid(out: &mut Vec<u8>, c: &Cid) {
    // tag 42, byte string with a leading 0x00 multibase-identity prefix
    out.extend_from_slice(&[0xd8, 0x2a]);
    write_head(out, 2, (CID_BYTES_LEN + 1) as u64);
    out.push(0);
    out.extend_from_slice(&c.to_bytes());
}

#[inline]
pub fn write_opt_cid(out: &mut Vec<u8>, c: Option<&Cid>) {
    match c {
        Some(c) => write_cid(out, c),
        None => write_null(out),
    }
}

/// DAG-CBOR canonical map key order: shorter keys first, then bytewise.
pub fn key_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    a.len()
        .cmp(&b.len())
        .then_with(|| a.as_bytes().cmp(b.as_bytes()))
}

// ---------- value tree ----------

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Bytes(Vec<u8>),
    Text(String),
    Array(Vec<Value>),
    /// Kept sorted in canonical key order.
    Map(Vec<(String, Value)>),
    Link(Cid),
}

#[derive(Debug, thiserror::Error)]
pub enum CborError {
    #[error("unexpected end of input")]
    Eof,
    #[error("invalid cbor: {0}")]
    Invalid(&'static str),
    #[error("invalid data model value: {0}")]
    DataModel(String),
}

impl Value {
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Value::Null => write_null(out),
            Value::Bool(b) => write_bool(out, *b),
            Value::Int(n) => write_int(out, *n),
            Value::Bytes(b) => write_bytes(out, b),
            Value::Text(s) => write_text(out, s),
            Value::Array(a) => {
                write_array_head(out, a.len());
                for v in a {
                    v.encode(out);
                }
            }
            Value::Map(m) => {
                write_map_head(out, m.len());
                for (k, v) in m {
                    write_text(out, k);
                    v.encode(out);
                }
            }
            Value::Link(c) => write_cid(out, c),
        }
    }

    pub fn to_cbor(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256);
        self.encode(&mut out);
        out
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Map(m) => m.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    /// atproto data-model JSON -> value. Strict like the reference's
    /// lex-json: `$link` / `$bytes` objects must have exactly that one
    /// string field holding a valid CID / base64; `$type` must be a non-empty
    /// string; `{"$type": "blob"}` needs a `$link` ref, a string mimeType
    /// and an integer size; numbers must be integers (JSON `123.0` parses
    /// as the integer 123, as in JavaScript).
    pub fn from_json(j: &serde_json::Value) -> Result<Value, CborError> {
        let dm = |m: &str| CborError::DataModel(m.to_string());
        Ok(match j {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(*b),
            serde_json::Value::Number(n) => match n.as_i64() {
                Some(i) => Value::Int(i),
                None => match n.as_f64() {
                    // integer-valued floats within JS's safe integer range
                    Some(f) if f.fract() == 0.0 && f.abs() <= 9_007_199_254_740_991.0 => {
                        Value::Int(f as i64)
                    }
                    _ => return Err(dm("floats are not allowed")),
                },
            },
            serde_json::Value::String(s) => Value::Text(s.clone()),
            serde_json::Value::Array(a) => {
                Value::Array(a.iter().map(Value::from_json).collect::<Result<_, _>>()?)
            }
            serde_json::Value::Object(o) => {
                if let Some(l) = o.get("$link") {
                    return match (l, o.len()) {
                        (serde_json::Value::String(s), 1) => Cid::parse(s)
                            .map(Value::Link)
                            .map_err(|_| CborError::DataModel(format!("bad $link {s}"))),
                        _ => Err(dm("$link must be the only field and a CID string")),
                    };
                }
                if let Some(b) = o.get("$bytes") {
                    return match (b, o.len()) {
                        (serde_json::Value::String(s), 1) => {
                            base64::engine::general_purpose::STANDARD_NO_PAD
                                .decode(s.trim_end_matches('='))
                                .map(Value::Bytes)
                                .map_err(|_| dm("bad $bytes"))
                        }
                        _ => Err(dm("$bytes must be the only field and a base64 string")),
                    };
                }
                match o.get("$type") {
                    None => {}
                    Some(serde_json::Value::String(t)) if !t.is_empty() => {
                        if t == "blob" {
                            let ok = matches!(o.get("ref"), Some(serde_json::Value::Object(r)) if r.len() == 1 && r.get("$link").is_some())
                                && o.get("mimeType").is_some_and(|m| m.is_string())
                                && o.get("size").is_some_and(|n| n.is_i64() || n.is_u64());
                            if !ok {
                                return Err(dm(
                                    "blob needs ref ($link), mimeType (string) and size (integer)",
                                ));
                            }
                        }
                    }
                    Some(_) => return Err(dm("$type must be a non-empty string")),
                }
                let mut m = Vec::with_capacity(o.len());
                for (k, v) in o {
                    m.push((k.clone(), Value::from_json(v)?));
                }
                m.sort_by(|a, b| key_cmp(&a.0, &b.0));
                Value::Map(m)
            }
        })
    }

    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::Value as J;
        match self {
            Value::Null => J::Null,
            Value::Bool(b) => J::Bool(*b),
            Value::Int(n) => J::from(*n),
            Value::Bytes(b) => serde_json::json!({
                "$bytes": base64::engine::general_purpose::STANDARD_NO_PAD.encode(b)
            }),
            Value::Text(s) => J::String(s.clone()),
            Value::Array(a) => J::Array(a.iter().map(|v| v.to_json()).collect()),
            Value::Map(m) => {
                let mut o = serde_json::Map::with_capacity(m.len());
                for (k, v) in m {
                    o.insert(k.clone(), v.to_json());
                }
                J::Object(o)
            }
            Value::Link(c) => serde_json::json!({ "$link": c.to_string() }),
        }
    }

    pub fn decode(data: &[u8]) -> Result<Value, CborError> {
        let mut d = Decoder { data, pos: 0 };
        let v = d.value(0)?;
        if d.pos != data.len() {
            return Err(CborError::Invalid("trailing bytes"));
        }
        Ok(v)
    }

    /// Decodes one value from the front of `data`, returning bytes consumed.
    pub fn decode_prefix(data: &[u8]) -> Result<(Value, usize), CborError> {
        let mut d = Decoder { data, pos: 0 };
        let v = d.value(0)?;
        Ok((v, d.pos))
    }
}

// ---------- JSON -> DAG-CBOR without a Value tree ----------

/// A JSON value borrowing its strings from the request body: what record
/// writes parse into (one pass over the bytes), validate against lexicons
/// ([`crate::lexicon::Node`], with `serde_json::Value`'s semantics) and
/// encode straight to DAG-CBOR ([`JsonValue::encode_record`]).
#[derive(Clone, Debug, PartialEq)]
pub enum JsonValue<'a> {
    Null,
    Bool(bool),
    Int(i64),
    /// A number with a fraction or exponent.
    Float(f64),
    /// An integer above `i64::MAX` (never a valid record value).
    BigUint(u64),
    Str(Cow<'a, str>),
    Array(Vec<JsonValue<'a>>),
    /// Sorted in DAG-CBOR key order; a repeated key keeps its last value,
    /// as `serde_json::Value` does.
    Object(Vec<(Cow<'a, str>, JsonValue<'a>)>),
}

/// Blob references found while encoding a record, in the order a walk of
/// the record's `Value` visits them (map keys in DAG-CBOR order, a map
/// before its children).
#[derive(Debug, Default, PartialEq)]
pub struct RecordRefs {
    /// Every `{"$type": "blob"}` ref: (cid, mimeType, size).
    pub blobs: Vec<(Cid, Option<String>, Option<i64>)>,
    /// The first legacy blob ref (`{"cid", "mimeType"}` strings, no
    /// `$type`) whose `cid` parses.
    pub legacy: Option<String>,
}

impl RecordRefs {
    /// Distinct blob CIDs, in order of first reference.
    pub fn cids(&self) -> Vec<Cid> {
        let mut out: Vec<Cid> = Vec::with_capacity(self.blobs.len());
        for (c, ..) in &self.blobs {
            if !out.contains(c) {
                out.push(*c);
            }
        }
        out
    }
}

/// Largest integer a JSON float may carry into a record (JS's safe range).
const MAX_SAFE_INT: f64 = 9_007_199_254_740_991.0;

fn obj_get<'v, 'a>(m: &'v [(Cow<'a, str>, JsonValue<'a>)], key: &str) -> Option<&'v JsonValue<'a>> {
    m.binary_search_by(|(k, _)| key_cmp(k, key))
        .ok()
        .map(|i| &m[i].1)
}

impl<'a> JsonValue<'a> {
    /// Parses JSON (serde_json's parser, so its syntax errors and nesting
    /// limit). Unescaped strings borrow from `body`.
    pub fn parse(body: &'a [u8]) -> serde_json::Result<JsonValue<'a>> {
        serde_json::from_slice(body)
    }

    pub fn get(&self, key: &str) -> Option<&JsonValue<'a>> {
        match self {
            JsonValue::Object(m) => obj_get(m, key),
            _ => None,
        }
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut JsonValue<'a>> {
        match self {
            JsonValue::Object(m) => match m.binary_search_by(|(k, _)| key_cmp(k, key)) {
                Ok(i) => Some(&mut m[i].1),
                Err(_) => None,
            },
            _ => None,
        }
    }

    /// Sets `key` (keeping the key order).
    pub fn insert(&mut self, key: &'a str, v: JsonValue<'a>) {
        if let JsonValue::Object(m) = self {
            match m.binary_search_by(|(k, _)| key_cmp(k, key)) {
                Ok(i) => m[i].1 = v,
                Err(i) => m.insert(i, (Cow::Borrowed(key), v)),
            }
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            JsonValue::Str(s) => Some(s),
            _ => None,
        }
    }

    /// The equivalent `serde_json::Value`.
    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::Value as J;
        match self {
            JsonValue::Null => J::Null,
            JsonValue::Bool(b) => J::Bool(*b),
            JsonValue::Int(n) => J::from(*n),
            JsonValue::Float(f) => serde_json::Number::from_f64(*f).map_or(J::Null, J::Number),
            JsonValue::BigUint(n) => J::from(*n),
            JsonValue::Str(s) => J::String(s.to_string()),
            JsonValue::Array(a) => J::Array(a.iter().map(|v| v.to_json()).collect()),
            JsonValue::Object(m) => J::Object(m.iter().map(|(k, v)| (k.to_string(), v.to_json())).collect()),
        }
    }

    /// Appends exactly `Value::from_json(self)?.to_cbor()` (same bytes, same
    /// accept/reject decision, same error) and collects the record's blob
    /// refs on the way. Integral floats become `Int` in place, so `self`
    /// then validates as the encoded record would. On error `out` is left
    /// as it was.
    pub fn encode_record(&mut self, out: &mut Vec<u8>, refs: &mut RecordRefs) -> Result<(), CborError> {
        let start = out.len();
        if self.encode(out, refs).is_ok() {
            return Ok(());
        }
        out.truncate(start);
        // Rare: let `from_json` name the error, so it is the one its own
        // traversal order meets first.
        match Value::from_json(&self.to_json()) {
            Err(e) => Err(e),
            Ok(_) => {
                debug_assert!(false, "encode_record rejected what from_json accepts");
                Err(CborError::DataModel("invalid record".into()))
            }
        }
    }

    fn encode(&mut self, out: &mut Vec<u8>, refs: &mut RecordRefs) -> Result<(), ()> {
        match self {
            JsonValue::Null => write_null(out),
            JsonValue::Bool(b) => write_bool(out, *b),
            JsonValue::Int(n) => write_int(out, *n),
            JsonValue::Float(f) => {
                if f.fract() != 0.0 || f.abs() > MAX_SAFE_INT {
                    return Err(());
                }
                let n = *f as i64;
                write_int(out, n);
                *self = JsonValue::Int(n);
            }
            JsonValue::BigUint(_) => return Err(()),
            JsonValue::Str(s) => write_text(out, s),
            JsonValue::Array(a) => {
                write_array_head(out, a.len());
                for v in a {
                    v.encode(out, refs)?;
                }
            }
            JsonValue::Object(m) => {
                if let Some(l) = obj_get(m, "$link") {
                    return match (l, m.len()) {
                        (JsonValue::Str(s), 1) => {
                            write_cid(out, &Cid::parse(s).map_err(|_| ())?);
                            Ok(())
                        }
                        _ => Err(()),
                    };
                }
                if let Some(b) = obj_get(m, "$bytes") {
                    return match (b, m.len()) {
                        (JsonValue::Str(s), 1) => {
                            let b = base64::engine::general_purpose::STANDARD_NO_PAD
                                .decode(s.trim_end_matches('='))
                                .map_err(|_| ())?;
                            write_bytes(out, &b);
                            Ok(())
                        }
                        _ => Err(()),
                    };
                }
                let text = |k: &str| obj_get(m, k).and_then(|v| v.as_str());
                match obj_get(m, "$type") {
                    None => {
                        if let (Some(c), Some(_), None) = (text("cid"), text("mimeType"), &refs.legacy) {
                            if Cid::parse(c).is_ok() {
                                refs.legacy = Some(c.to_string());
                            }
                        }
                    }
                    Some(JsonValue::Str(t)) if !t.is_empty() => {
                        if t == "blob" {
                            let link = match obj_get(m, "ref") {
                                Some(JsonValue::Object(r)) if r.len() == 1 => obj_get(r, "$link").ok_or(())?,
                                _ => return Err(()),
                            };
                            let (Some(mime), Some(JsonValue::Int(size))) = (text("mimeType"), obj_get(m, "size")) else {
                                return Err(());
                            };
                            let JsonValue::Str(link) = link else { return Err(()) };
                            let c = Cid::parse(link).map_err(|_| ())?;
                            refs.blobs.push((c, Some(mime.to_string()), Some(*size)));
                        }
                    }
                    Some(_) => return Err(()),
                }
                write_map_head(out, m.len());
                for (k, v) in m.iter_mut() {
                    write_text(out, k);
                    v.encode(out, refs)?;
                }
            }
        }
        Ok(())
    }
}

impl<'de> serde::Deserialize<'de> for JsonValue<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(JsonVisitor)
    }
}

struct JsonVisitor;

impl<'de> serde::de::Visitor<'de> for JsonVisitor {
    type Value = JsonValue<'de>;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("any valid JSON value")
    }
    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(JsonValue::Null)
    }
    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(JsonValue::Null)
    }
    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> {
        Ok(JsonValue::Bool(v))
    }
    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
        Ok(JsonValue::Int(v))
    }
    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
        Ok(match i64::try_from(v) {
            Ok(n) => JsonValue::Int(n),
            Err(_) => JsonValue::BigUint(v),
        })
    }
    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
        Ok(JsonValue::Float(v))
    }
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E> {
        Ok(JsonValue::Str(Cow::Borrowed(v)))
    }
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
        Ok(JsonValue::Str(Cow::Owned(v.to_string())))
    }
    fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
        Ok(JsonValue::Str(Cow::Owned(v)))
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
        let mut out = Vec::with_capacity(a.size_hint().unwrap_or(0).min(64));
        while let Some(v) = a.next_element()? {
            out.push(v);
        }
        Ok(JsonValue::Array(out))
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
        let mut m: Vec<(Cow<'de, str>, JsonValue<'de>)> = Vec::with_capacity(a.size_hint().unwrap_or(0).min(64));
        while let Some(JsonKey(k)) = a.next_key()? {
            m.push((k, a.next_value()?));
        }
        if m.len() > 1 {
            // stable sort of the reversed entries puts a repeated key's last
            // value first; dedup keeps the first of each run
            m.reverse();
            m.sort_by(|x, y| key_cmp(&x.0, &y.0));
            m.dedup_by(|later, kept| later.0 == kept.0);
        }
        Ok(JsonValue::Object(m))
    }
}

struct JsonKey<'de>(Cow<'de, str>);

impl<'de> serde::Deserialize<'de> for JsonKey<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = JsonKey<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a string key")
            }
            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E> {
                Ok(JsonKey(Cow::Borrowed(v)))
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(JsonKey(Cow::Owned(v.to_string())))
            }
            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                Ok(JsonKey(Cow::Owned(v)))
            }
        }
        d.deserialize_str(V)
    }
}

// ---------- DAG-CBOR -> JSON ----------

/// Transcodes one DAG-CBOR value straight to atproto JSON (`{"$link": cid}`
/// for links, `{"$bytes": base64}` for byte strings) without building a
/// `Value` tree. It accepts exactly what `Value::decode` accepts, and the
/// output parses to the same JSON as `Value::decode(bytes)?.to_json()`.
/// On error `out` is left as it was.
pub fn write_json(bytes: &[u8], out: &mut Vec<u8>) -> Result<(), CborError> {
    let start = out.len();
    let mut d = Decoder {
        data: bytes,
        pos: 0,
    };
    let r = d.json(0, out).and_then(|()| {
        if d.pos != bytes.len() {
            return Err(CborError::Invalid("trailing bytes"));
        }
        Ok(())
    });
    if r.is_err() {
        out.truncate(start);
    }
    r
}

fn json_str(out: &mut Vec<u8>, s: &str) {
    // writing into a Vec cannot fail
    let _ = serde_json::to_writer(&mut *out, s);
}

struct Decoder<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    fn byte(&mut self) -> Result<u8, CborError> {
        let b = *self.data.get(self.pos).ok_or(CborError::Eof)?;
        self.pos += 1;
        Ok(b)
    }

    /// Takes `n` bytes; `n` is an untrusted length (up to u64::MAX), so it is
    /// compared against what remains rather than added to `pos`.
    fn take(&mut self, n: u64) -> Result<&'a [u8], CborError> {
        let rest = self.data.len() - self.pos;
        if n > rest as u64 {
            return Err(CborError::Eof);
        }
        let s = &self.data[self.pos..self.pos + n as usize];
        self.pos += n as usize;
        Ok(s)
    }

    /// Reads a head; DAG-CBOR requires the shortest encoding of every
    /// argument (ints, lengths, tags).
    fn head(&mut self) -> Result<(u8, u64), CborError> {
        let b = self.byte()?;
        let major = b >> 5;
        let info = b & 31;
        // major 7: only the one-byte false/true/null; floats (25-27), two-byte
        // simple values (24) and the other simple values are not data model
        if major == 7 {
            return match info {
                20..=22 => Ok((7, info as u64)),
                _ => Err(CborError::Invalid("floats/simple values not allowed")),
            };
        }
        let (n, min) = match info {
            0..=23 => (info as u64, 0),
            24 => (self.byte()? as u64, 24),
            25 => (
                u16::from_be_bytes(self.take(2)?.try_into().unwrap()) as u64,
                1 << 8,
            ),
            26 => (
                u32::from_be_bytes(self.take(4)?.try_into().unwrap()) as u64,
                1 << 16,
            ),
            27 => (
                u64::from_be_bytes(self.take(8)?.try_into().unwrap()),
                1 << 32,
            ),
            _ => return Err(CborError::Invalid("indefinite length or reserved")),
        };
        if n < min {
            return Err(CborError::Invalid("non-minimal integer encoding"));
        }
        Ok((major, n))
    }

    fn value(&mut self, depth: usize) -> Result<Value, CborError> {
        if depth > 128 {
            return Err(CborError::Invalid("nesting too deep"));
        }
        let (major, n) = self.head()?;
        Ok(match major {
            0 => Value::Int(i64::try_from(n).map_err(|_| CborError::Invalid("int range"))?),
            1 => Value::Int(-1 - i64::try_from(n).map_err(|_| CborError::Invalid("int range"))?),
            2 => Value::Bytes(self.take(n)?.to_vec()),
            3 => Value::Text(
                std::str::from_utf8(self.take(n)?)
                    .map_err(|_| CborError::Invalid("utf8"))?
                    .to_string(),
            ),
            4 => {
                let mut a = Vec::with_capacity((n as usize).min(1024));
                for _ in 0..n {
                    a.push(self.value(depth + 1)?);
                }
                Value::Array(a)
            }
            5 => {
                let mut m: Vec<(String, Value)> = Vec::with_capacity((n as usize).min(1024));
                for _ in 0..n {
                    let k = match self.value(depth + 1)? {
                        Value::Text(s) => s,
                        _ => return Err(CborError::Invalid("non-string map key")),
                    };
                    // canonical order, which also rules out duplicates
                    if let Some((prev, _)) = m.last() {
                        match key_cmp(prev, &k) {
                            std::cmp::Ordering::Less => {}
                            std::cmp::Ordering::Equal => {
                                return Err(CborError::Invalid("duplicate map key"))
                            }
                            std::cmp::Ordering::Greater => {
                                return Err(CborError::Invalid("map keys not in canonical order"))
                            }
                        }
                    }
                    let v = self.value(depth + 1)?;
                    m.push((k, v));
                }
                Value::Map(m)
            }
            6 => {
                if n != 42 {
                    return Err(CborError::Invalid("unsupported tag"));
                }
                match self.value(depth + 1)? {
                    Value::Bytes(b) if b.first() == Some(&0) => Value::Link(
                        Cid::from_bytes(&b[1..]).map_err(|_| CborError::Invalid("bad cid"))?,
                    ),
                    _ => return Err(CborError::Invalid("bad cid link")),
                }
            }
            7 => match n {
                20 => Value::Bool(false),
                21 => Value::Bool(true),
                _ => Value::Null,
            },
            _ => unreachable!(),
        })
    }
}

// The transcoder mirrors `value` check for check (depth limit, minimal
// heads, string keys in canonical order, tag 42 links, major 7) so both
// accept the same inputs.
impl<'a> Decoder<'a> {
    fn json(&mut self, depth: usize, out: &mut Vec<u8>) -> Result<(), CborError> {
        if depth > 128 {
            return Err(CborError::Invalid("nesting too deep"));
        }
        let (major, n) = self.head()?;
        match major {
            0 => {
                let n = i64::try_from(n).map_err(|_| CborError::Invalid("int range"))?;
                let _ = serde_json::to_writer(&mut *out, &n);
            }
            1 => {
                let n = -1 - i64::try_from(n).map_err(|_| CborError::Invalid("int range"))?;
                let _ = serde_json::to_writer(&mut *out, &n);
            }
            2 => {
                let b = self.take(n)?;
                out.extend_from_slice(b"{\"$bytes\":\"");
                let at = out.len();
                out.resize(at + b.len().div_ceil(3) * 4, 0);
                let w = base64::engine::general_purpose::STANDARD_NO_PAD
                    .encode_slice(b, &mut out[at..])
                    .map_err(|_| CborError::Invalid("base64"))?;
                out.truncate(at + w);
                out.extend_from_slice(b"\"}");
            }
            3 => json_str(
                out,
                std::str::from_utf8(self.take(n)?).map_err(|_| CborError::Invalid("utf8"))?,
            ),
            4 => {
                out.push(b'[');
                for i in 0..n {
                    if i > 0 {
                        out.push(b',');
                    }
                    self.json(depth + 1, out)?;
                }
                out.push(b']');
            }
            5 => {
                out.push(b'{');
                let mut prev: Option<&str> = None;
                for i in 0..n {
                    if i > 0 {
                        out.push(b',');
                    }
                    let k = self.json_key(depth + 1)?;
                    if let Some(prev) = prev {
                        match key_cmp(prev, k) {
                            std::cmp::Ordering::Less => {}
                            std::cmp::Ordering::Equal => {
                                return Err(CborError::Invalid("duplicate map key"))
                            }
                            std::cmp::Ordering::Greater => {
                                return Err(CborError::Invalid("map keys not in canonical order"))
                            }
                        }
                    }
                    json_str(out, k);
                    out.push(b':');
                    self.json(depth + 1, out)?;
                    prev = Some(k);
                }
                out.push(b'}');
            }
            6 => {
                if n != 42 {
                    return Err(CborError::Invalid("unsupported tag"));
                }
                let c = self.json_link(depth + 1)?;
                out.extend_from_slice(b"{\"$link\":\"");
                c.write_string(out);
                out.extend_from_slice(b"\"}");
            }
            7 => out.extend_from_slice(match n {
                20 => b"false",
                21 => b"true",
                _ => b"null",
            }),
            _ => unreachable!(),
        }
        Ok(())
    }

    fn json_key(&mut self, depth: usize) -> Result<&'a str, CborError> {
        if depth > 128 {
            return Err(CborError::Invalid("nesting too deep"));
        }
        match self.head()? {
            (3, n) => std::str::from_utf8(self.take(n)?).map_err(|_| CborError::Invalid("utf8")),
            _ => Err(CborError::Invalid("non-string map key")),
        }
    }

    fn json_link(&mut self, depth: usize) -> Result<Cid, CborError> {
        if depth > 128 {
            return Err(CborError::Invalid("nesting too deep"));
        }
        match self.head()? {
            (2, n) => match self.take(n)? {
                [0, c @ ..] => Cid::from_bytes(c).map_err(|_| CborError::Invalid("bad cid")),
                _ => Err(CborError::Invalid("bad cid link")),
            },
            _ => Err(CborError::Invalid("bad cid link")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_roundtrip_sorted() {
        let j = serde_json::json!({"text": "hi", "$type": "app.bsky.feed.post", "createdAt": "2026-01-01T00:00:00Z", "n": -5});
        let v = Value::from_json(&j).unwrap();
        let b = v.to_cbor();
        let back = Value::decode(&b).unwrap();
        assert_eq!(back, v);
        assert_eq!(back.to_json(), j);
        // canonical order: "n" (1), "text" (4), "$type" (5), "createdAt" (9)
        if let Value::Map(m) = &v {
            let keys: Vec<_> = m.iter().map(|(k, _)| k.as_str()).collect();
            assert_eq!(keys, vec!["n", "text", "$type", "createdAt"]);
        }
    }

    #[test]
    fn huge_lengths_are_errors_not_panics() {
        // byte/text strings and arrays of length u64::MAX
        for major in [0x5b, 0x7b, 0x9b, 0xbb] {
            let mut b = vec![major, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
            b.extend_from_slice(&[0; 10]);
            assert!(Value::decode(&b).is_err(), "{major:#x}");
            assert!(write_json(&b, &mut Vec::new()).is_err(), "{major:#x}");
        }
        // one past the end
        assert!(Value::decode(&[0x43, 1, 2]).is_err());
    }

    #[test]
    fn major_7_only_false_true_null() {
        assert_eq!(Value::decode(&[0xf4]).unwrap(), Value::Bool(false));
        assert_eq!(Value::decode(&[0xf5]).unwrap(), Value::Bool(true));
        assert_eq!(Value::decode(&[0xf6]).unwrap(), Value::Null);
        for b in [
            &[0xf7][..],                                             // undefined
            &[0xf0],                                                 // simple(16)
            &[0xf8, 0x14],                                           // two-byte simple(20)
            &[0xf8, 0x16],                                           // two-byte simple(22)
            &[0xf8, 0xff],                                           // simple(255)
            &[0xf9, 0x00, 0x14],                                     // f16 with bits 20
            &[0xf9, 0x3c, 0x00],                                     // f16 1.0
            &[0xfa, 0x00, 0x00, 0x00, 0x15],                         // f32 with bits 21
            &[0xfb, 0, 0, 0, 0, 0, 0, 0, 0x16],                      // f64 with bits 22
            &[0xfb, 0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18], // f64 pi
            &[0xff],                                                 // break
        ] {
            assert!(Value::decode(b).is_err(), "{b:02x?} accepted");
            assert!(
                write_json(b, &mut Vec::new()).is_err(),
                "{b:02x?} transcoded"
            );
        }
    }

    #[test]
    fn write_json_matches_to_json() {
        let j = serde_json::json!({
            "$type": "app.bsky.feed.post",
            "text": "quote \" backslash \\ nl \n tab \t ctl \u{1} emoji \u{1F600}",
            "n": [0, -1, 23, 24, -25, 255, 256, 65536, i64::MAX, i64::MIN + 1, i64::MIN],
            "b": {"$bytes": "AQIDBA"},
            "e": {"$bytes": ""},
            "l": {"$link": "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"},
            "o": {"a": null, "b": true, "c": false, "d": [], "e": {}},
        });
        let b = Value::from_json(&j).unwrap().to_cbor();
        let mut out = b"prefix".to_vec();
        write_json(&b, &mut out).unwrap();
        let got: serde_json::Value = serde_json::from_slice(&out[6..]).unwrap();
        assert_eq!(got, j);
        // errors leave the output untouched
        let before = out.clone();
        let mut bad = b.clone();
        bad.push(0);
        assert!(write_json(&bad, &mut out).is_err());
        assert_eq!(out, before);
    }
}
