//! Minimal DAG-CBOR: a byte-level encoder for the hot path (MST nodes,
//! commits, firehose frames) and a `Value` tree for records, with JSON
//! conversion following the atproto data model ($link, $bytes).

use crate::cid::{Cid, CID_BYTES_LEN};
use base64::Engine;

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
                                return Err(dm("blob needs ref ($link), mimeType (string) and size (integer)"));
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

    fn take(&mut self, n: usize) -> Result<&'a [u8], CborError> {
        if self.pos + n > self.data.len() {
            return Err(CborError::Eof);
        }
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    /// Reads a head; DAG-CBOR requires the shortest encoding of every
    /// argument (ints, lengths, tags).
    fn head(&mut self) -> Result<(u8, u64), CborError> {
        let b = self.byte()?;
        let major = b >> 5;
        let info = b & 31;
        let (n, min) = match info {
            0..=23 => (info as u64, 0),
            24 => (self.byte()? as u64, 24),
            25 => (u16::from_be_bytes(self.take(2)?.try_into().unwrap()) as u64, 1 << 8),
            26 => (u32::from_be_bytes(self.take(4)?.try_into().unwrap()) as u64, 1 << 16),
            27 => (u64::from_be_bytes(self.take(8)?.try_into().unwrap()), 1 << 32),
            _ => return Err(CborError::Invalid("indefinite length or reserved")),
        };
        // major 7 arguments are simple values / floats, not lengths
        if major != 7 && n < min {
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
            2 => Value::Bytes(self.take(n as usize)?.to_vec()),
            3 => Value::Text(
                std::str::from_utf8(self.take(n as usize)?)
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
                22 => Value::Null,
                _ => return Err(CborError::Invalid("floats/simple values not allowed")),
            },
            _ => unreachable!(),
        })
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
}
