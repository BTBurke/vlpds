//! atproto identifier syntax (mirrors @atproto/syntax): NSIDs, record keys,
//! handles, TIDs and DIDs.

/// segment = alpha *( alpha / number / "-" ); name = alpha *( alpha / number );
/// at least three segments, 317 chars max.
pub fn valid_nsid(s: &str) -> bool {
    if s.len() > 253 + 1 + 63
        || !s
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-')
    {
        return false;
    }
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() < 3 {
        return false;
    }
    for p in &parts {
        if p.is_empty() || p.len() > 63 || p.starts_with('-') || p.ends_with('-') {
            return false;
        }
    }
    if parts[0].as_bytes()[0].is_ascii_digit() {
        return false;
    }
    let name = parts[parts.len() - 1];
    name.as_bytes()[0].is_ascii_alphabetic() && name.bytes().all(|b| b.is_ascii_alphanumeric())
}

/// 1-512 chars of [A-Za-z0-9._:~-], not "." or "..".
pub fn valid_rkey(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 512
        && s != "."
        && s != ".."
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:~-".contains(&b))
}

/// A repo path `{collection}/{rkey}`.
pub fn valid_record_path(path: &str) -> bool {
    match path.split_once('/') {
        Some((c, r)) => valid_nsid(c) && valid_rkey(r),
        None => false,
    }
}

/// Domain-name handle: >= 2 labels of 1-63 [A-Za-z0-9-] not starting/ending
/// with '-', TLD starts with a letter, 253 chars max.
pub fn valid_handle(h: &str) -> bool {
    if h.len() > 253
        || !h
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-')
    {
        return false;
    }
    let labels: Vec<&str> = h.split('.').collect();
    if labels.len() < 2 {
        return false;
    }
    for l in &labels {
        if l.is_empty() || l.len() > 63 || l.starts_with('-') || l.ends_with('-') {
            return false;
        }
    }
    labels[labels.len() - 1].as_bytes()[0].is_ascii_alphabetic()
}

/// 13 chars of base32-sortable, first char in [234567abcdefghij].
pub fn valid_tid(s: &str) -> bool {
    s.len() == 13
        && b"234567abcdefghij".contains(&s.as_bytes()[0])
        && s.bytes()
            .all(|b| b"234567abcdefghijklmnopqrstuvwxyz".contains(&b))
}

/// did:{method}:{id} with the spec's character rules.
pub fn valid_did(s: &str) -> bool {
    let Some(rest) = s.strip_prefix("did:") else {
        return false;
    };
    let Some((method, id)) = rest.split_once(':') else {
        return false;
    };
    s.len() <= 2048
        && !method.is_empty()
        && method.bytes().all(|b| b.is_ascii_lowercase())
        && !id.is_empty()
        && !id.ends_with(':')
        && !id.ends_with('%')
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:%-".contains(&b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nsids() {
        for ok in [
            "app.bsky.feed.post",
            "com.example.fooBar",
            "a-0.b-1.c",
            "com.example.f00",
        ] {
            assert!(valid_nsid(ok), "{ok}");
        }
        for bad in [
            "app.bsky",
            "1com.example.foo",
            "com.example.foo-bar",
            "com..foo",
            "com.example.3foo",
            "com.exa_mple.foo",
            "-com.example.foo",
        ] {
            assert!(!valid_nsid(bad), "{bad}");
        }
    }

    #[test]
    fn rkeys() {
        for ok in ["3jui7kd54zh2y", "self", "a:b~c_d.e-f", "..."] {
            assert!(valid_rkey(ok), "{ok}");
        }
        for bad in ["", ".", "..", "a/b", "a b", "#x", &"a".repeat(513)] {
            assert!(!valid_rkey(bad), "{bad}");
        }
    }

    #[test]
    fn handles() {
        assert!(valid_handle("alice.bsky.social"));
        assert!(valid_handle("x.test"));
        assert!(!valid_handle("alice"));
        assert!(!valid_handle("alice.-bsky.social"));
        assert!(!valid_handle("alice.bsky.1social"));
        assert!(!valid_handle("al_ice.bsky.social"));
    }

    #[test]
    fn dids_tids() {
        assert!(valid_did("did:plc:abc123"));
        assert!(valid_did("did:web:example.com"));
        assert!(!valid_did("did:PLC:abc"));
        assert!(!valid_did("did:plc:"));
        assert!(valid_tid("3jui7kd54zh2y"));
        assert!(!valid_tid("3jui7kd54zh2"));
    }
}
