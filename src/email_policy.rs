//! Disposable email domains, refused as the reference PDS does
//! (`isDisposableEmail` from `disposable-email-domains-js`). The list is that
//! package's v1.26.0 blocklist (CC0-1.0), one domain per line so it diffs
//! against upstream. Matching is the package's: exact domain after the last
//! `@`, so subdomains of a listed domain are not matched.

use std::collections::HashSet;
use std::sync::LazyLock;

const DOMAINS_TXT: &str = include_str!("email_policy/disposable_email_domains.txt");

static DOMAINS: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| DOMAINS_TXT.lines().map(str::trim).filter(|l| !l.is_empty()).collect());

pub fn is_disposable_email(email: &str) -> bool {
    let domain = email.rsplit('@').next().unwrap_or(email);
    DOMAINS.contains(domain.trim().to_ascii_lowercase().as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_like_the_reference() {
        assert!(DOMAINS.len() > 8000, "{}", DOMAINS.len());
        // the reference tests' addresses
        assert!(is_disposable_email("bad-email@disposeamail.com"));
        assert!(is_disposable_email("Bad@DisposeAMail.com "));
        assert!(is_disposable_email("a@mailinator.com"));
        assert!(!is_disposable_email("alice@test.com"));
        assert!(!is_disposable_email("alice@gmail.com"));
        // exact domain only
        assert!(!is_disposable_email("a@sub.disposeamail.com"));
    }
}
