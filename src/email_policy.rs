//! Disposable email domains, refused by createAccount and updateEmail as the
//! reference PDS does (`isDisposableEmail` from `disposable-email-domains-js`).
//! The list is that package's v1.26.0 `disposable_email_blocklist.json`
//! (CC0-1.0), one domain per line in `email_policy/disposable_email_domains.txt`
//! so it can be diffed against upstream. Matching is the package's: the text
//! after the last `@`, trimmed and lowercased, exactly equal to a listed
//! domain (subdomains of a listed domain are not matched).

use std::collections::HashSet;
use std::sync::LazyLock;

const DOMAINS_TXT: &str = include_str!("email_policy/disposable_email_domains.txt");

static DOMAINS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    DOMAINS_TXT
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect()
});

/// Is `domain` a listed disposable email domain?
pub fn is_disposable_domain(domain: &str) -> bool {
    DOMAINS.contains(domain.trim().to_ascii_lowercase().as_str())
}

/// Does `email` use a disposable domain?
pub fn is_disposable_email(email: &str) -> bool {
    let domain = email.rsplit('@').next().unwrap_or(email);
    is_disposable_domain(domain)
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
