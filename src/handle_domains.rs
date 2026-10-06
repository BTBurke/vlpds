//! The service handle domains: the suffixes this server gives out handles
//! under (the reference's `serviceHandleDomains`). One or more, from
//! `--handle-domains` (DESIGN.md "Handle domains"). The first is the primary:
//! accounts with no other claim on a domain change handles under it, which
//! is what a single-domain server has always done.

/// Account `extra` key: the domain an account held a service handle under
/// before it moved to a handle of its own, so it may move back. Written
/// only while there's more than one domain.
pub const HOME_KEY: &str = "homeHandleDomain";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandleDomains {
    /// Bare (no leading dot), lowercase, in configured order.
    domains: Vec<String>,
}

/// A configured domain as stored: lowercase, without the leading dot the
/// reference writes (`.example.com`) or a trailing one.
pub fn normalize(domain: &str) -> Result<String, String> {
    let d = domain.trim().trim_start_matches('.').trim_end_matches('.').to_ascii_lowercase();
    if d.is_empty() {
        return Err(format!("handle domain {domain:?} is empty"));
    }
    if d.len() > 253 {
        return Err(format!("handle domain {domain:?} is too long"));
    }
    let label_ok = |l: &str| {
        !l.is_empty()
            && l.len() <= 63
            && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            && !l.starts_with('-')
            && !l.ends_with('-')
    };
    if !d.split('.').all(label_ok) {
        return Err(format!("handle domain {domain:?} is not a domain name"));
    }
    Ok(d)
}

impl HandleDomains {
    /// Refuses an empty list, an invalid entry or a duplicate.
    pub fn new<S: AsRef<str>>(list: &[S]) -> Result<HandleDomains, String> {
        let mut domains: Vec<String> = Vec::new();
        for d in list {
            let n = normalize(d.as_ref())?;
            if domains.contains(&n) {
                return Err(format!("handle domain {n} is listed twice"));
            }
            domains.push(n);
        }
        if domains.is_empty() {
            return Err("no handle domain configured".into());
        }
        Ok(HandleDomains { domains })
    }

    pub fn primary(&self) -> &str {
        &self.domains[0]
    }

    pub fn all(&self) -> &[String] {
        &self.domains
    }

    pub fn contains(&self, domain: &str) -> bool {
        self.domains.iter().any(|d| d == domain)
    }

    /// As `describeServer`'s `availableUserDomains`: leading dots, configured
    /// order.
    pub fn available(&self) -> Vec<String> {
        self.domains.iter().map(|d| format!(".{d}")).collect()
    }

    /// The domain `handle` is a service handle under: the longest one it
    /// ends with as `.{domain}`. The reference takes the first in its list,
    /// which differs only when one domain is under another (`.example.com`
    /// and `.at.example.com`): there the first match would see
    /// `bob.at.example.com` as a dotted name under `example.com`.
    pub fn longest_match(&self, handle: &str) -> Option<&str> {
        self.domains
            .iter()
            .filter(|d| handle.strip_suffix(d.as_str()).is_some_and(|front| front.len() > 1 && front.ends_with('.')))
            .max_by_key(|d| d.len())
            .map(String::as_str)
    }

    /// The reference's `serviceHandleDomains` check: under a domain, or one
    /// of the domains itself.
    pub fn serves(&self, handle: &str) -> bool {
        self.longest_match(handle).is_some() || self.contains(handle)
    }

    /// The domain an account may take service handles under itself: the
    /// one its handle is under, else the one it last held a service handle
    /// under (`stored`, [`HOME_KEY`]) while that's still configured, else
    /// the primary. None: only an admin may give it a service handle.
    pub fn home<'a>(&'a self, handle: &str, stored: Option<&str>) -> Option<&'a str> {
        if let Some(d) = self.longest_match(handle) {
            return Some(d);
        }
        match stored {
            Some(s) => self.domains.iter().find(|d| *d == s).map(String::as_str),
            None => Some(self.primary()),
        }
    }

    /// The [`HOME_KEY`] value after a move from `old` to `new`: kept when
    /// leaving a service domain for a handle of one's own (only with more
    /// than one domain, so a single-domain server never writes it), cleared
    /// on taking a service handle, unchanged otherwise.
    pub fn stored_home_after(&self, old: &str, new: &str, stored: Option<&str>) -> Option<String> {
        if self.longest_match(new).is_some() {
            return None;
        }
        match self.longest_match(old) {
            Some(d) if self.domains.len() > 1 => Some(d.to_string()),
            _ => stored.map(str::to_string),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(list: &[&str]) -> HandleDomains {
        HandleDomains::new(list).unwrap()
    }

    #[test]
    fn normalizes_the_reference_spelling() {
        assert_eq!(normalize(".Example.COM").unwrap(), "example.com");
        assert_eq!(normalize("example.com.").unwrap(), "example.com");
        assert_eq!(normalize(" vlpds.test ").unwrap(), "vlpds.test");
        assert_eq!(normalize("localhost").unwrap(), "localhost");
        for bad in ["", ".", "a..b", "-a.com", "a-.com", "a_b.com", "a b.com", "http://a.com"] {
            assert!(normalize(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn new_refuses_empty_and_duplicates() {
        assert!(HandleDomains::new::<&str>(&[]).is_err());
        assert!(HandleDomains::new(&["a.com", ".A.com"]).is_err());
        assert_eq!(set(&["b.com", "a.com"]).primary(), "b.com");
        assert_eq!(set(&["b.com", ".a.com"]).available(), vec![".b.com", ".a.com"]);
    }

    #[test]
    fn longest_suffix_wins() {
        let s = set(&["example.com", "at.example.com", "other.org"]);
        assert_eq!(s.longest_match("bob.at.example.com"), Some("at.example.com"));
        assert_eq!(s.longest_match("bob.example.com"), Some("example.com"));
        assert_eq!(s.longest_match("bob.other.org"), Some("other.org"));
        // order doesn't matter
        let r = set(&["at.example.com", "example.com"]);
        assert_eq!(r.longest_match("bob.at.example.com"), Some("at.example.com"));
        // a domain itself, a lookalike and an empty label are not under it
        assert_eq!(s.longest_match("example.com"), None);
        assert_eq!(s.longest_match("badexample.com"), None);
        assert_eq!(s.longest_match(".example.com"), None);
        assert_eq!(s.longest_match("alice.net"), None);
        assert!(s.serves("example.com"));
        assert!(s.serves("x.other.org"));
        assert!(!s.serves("alice.net"));
    }

    #[test]
    fn home_follows_the_handle_then_the_stored_domain_then_the_primary() {
        let s = set(&["a.com", "b.com"]);
        assert_eq!(s.home("bob.b.com", None), Some("b.com"));
        assert_eq!(s.home("bob.b.com", Some("a.com")), Some("b.com"));
        assert_eq!(s.home("bob.net", Some("b.com")), Some("b.com"));
        assert_eq!(s.home("bob.net", None), Some("a.com"));
        // the stored domain is no longer configured
        assert_eq!(s.home("bob.net", Some("gone.com")), None);
    }

    #[test]
    fn home_is_only_recorded_with_several_domains() {
        let one = set(&["a.com"]);
        assert_eq!(one.stored_home_after("bob.a.com", "bob.net", None), None);
        let s = set(&["a.com", "b.com"]);
        // leaving a service domain records it
        assert_eq!(s.stored_home_after("bob.b.com", "bob.net", None).as_deref(), Some("b.com"));
        // own domain to own domain keeps it
        assert_eq!(s.stored_home_after("bob.net", "bob.org", Some("b.com")).as_deref(), Some("b.com"));
        // taking a service handle clears it
        assert_eq!(s.stored_home_after("bob.net", "bob.b.com", Some("b.com")), None);
        assert_eq!(s.stored_home_after("bob.a.com", "bob2.a.com", None), None);
    }
}
