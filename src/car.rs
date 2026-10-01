//! CAR v1 encoding (header + varint-framed blocks).

use crate::cbor;
use crate::cid::Cid;

pub fn write_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

pub fn read_varint(b: &[u8]) -> Option<(u64, usize)> {
    let mut n = 0u64;
    for (i, &byte) in b.iter().enumerate().take(10) {
        n |= ((byte & 0x7f) as u64) << (7 * i);
        if byte & 0x80 == 0 {
            return Some((n, i + 1));
        }
    }
    None
}

pub fn write_header(out: &mut Vec<u8>, root: &Cid) {
    let mut h = Vec::with_capacity(64);
    cbor::write_map_head(&mut h, 2);
    cbor::write_text(&mut h, "roots");
    cbor::write_array_head(&mut h, 1);
    cbor::write_cid(&mut h, root);
    cbor::write_text(&mut h, "version");
    cbor::write_uint(&mut h, 1);
    write_varint(out, h.len() as u64);
    out.extend_from_slice(&h);
}

pub fn write_block(out: &mut Vec<u8>, c: &Cid, data: &[u8]) {
    let cb = c.to_bytes();
    write_varint(out, (cb.len() + data.len()) as u64);
    out.extend_from_slice(&cb);
    out.extend_from_slice(data);
}

/// Parses a CAR into (roots, blocks).
pub fn read_car(b: &[u8]) -> anyhow::Result<(Vec<Cid>, Vec<(Cid, &[u8])>)> {
    let (hlen, n) = read_varint(b).ok_or_else(|| anyhow::anyhow!("bad car header"))?;
    let mut pos = n + hlen as usize;
    let header = cbor::Value::decode(b.get(n..pos).ok_or_else(|| anyhow::anyhow!("short car"))?)?;
    let roots = match header.get("roots") {
        Some(cbor::Value::Array(a)) => a
            .iter()
            .filter_map(|v| {
                if let cbor::Value::Link(c) = v {
                    Some(*c)
                } else {
                    None
                }
            })
            .collect(),
        _ => vec![],
    };
    let mut blocks = Vec::new();
    while pos < b.len() {
        let (len, n) = read_varint(&b[pos..]).ok_or_else(|| anyhow::anyhow!("bad block len"))?;
        pos += n;
        let end = pos + len as usize;
        let blk = b
            .get(pos..end)
            .ok_or_else(|| anyhow::anyhow!("short block"))?;
        let (c, cl) = Cid::read_prefix(blk)?;
        blocks.push((c, &blk[cl..]));
        pos = end;
    }
    Ok((roots, blocks))
}
