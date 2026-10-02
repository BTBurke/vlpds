//! Handle policy, ported from the reference PDS (packages/pds/src/handle):
//! the reserved service-domain labels and the explicit-slur filter. Both
//! lists are data files compiled into the binary (`handle_policy/*.txt`,
//! verbatim from the reference) so they can be diffed against upstream.
//!
//! Where they apply (as in the reference's `normalizeAndValidateHandle`):
//! - slur filter: any handle a user picks (createAccount, OAuth sign-up,
//!   updateHandle), service-domain or custom domain, and record keys on
//!   writes. Admins (`com.atproto.admin.updateAccountHandle`) bypass it.
//! - reserved labels: only the first label of a handle under the service
//!   domain; custom domains prove control instead. Admins bypass it too.

use std::collections::HashSet;
use std::sync::LazyLock;

const RESERVED_TXT: &str = include_str!("handle_policy/reserved.txt");
const SLURS_TXT: &str = include_str!("handle_policy/explicit_slurs.txt");

fn data_lines(s: &'static str) -> impl Iterator<Item = &'static str> {
    s.lines().map(str::trim).filter(|l| !l.is_empty() && !l.starts_with('#'))
}

static RESERVED: LazyLock<HashSet<&'static str>> = LazyLock::new(|| data_lines(RESERVED_TXT).collect());

/// The reference's patterns are JS regexes without flags: case-sensitive,
/// and `\b` is an ASCII word boundary. Handles and record keys are ASCII,
/// where Rust's Unicode `\b` agrees.
static SLURS: LazyLock<Vec<regex::Regex>> = LazyLock::new(|| {
    data_lines(SLURS_TXT)
        .map(|p| regex::Regex::new(p).unwrap_or_else(|e| panic!("bad explicit-slur pattern: {e}")))
        .collect()
});

/// Whether `label` (the part of a service-domain handle before the domain)
/// is reserved.
pub fn is_reserved(label: &str) -> bool {
    RESERVED.contains(label)
}

/// Reference `hasExplicitSlur`: a pattern matches the string as is, or with
/// every '.', '-' and '_' removed.
pub fn has_explicit_slur(s: &str) -> bool {
    let squashed: String = s.chars().filter(|c| !matches!(c, '.' | '-' | '_')).collect();
    SLURS.iter().any(|r| r.is_match(s) || r.is_match(&squashed))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test words are stored reversed so the source doesn't spell them out.
    fn rev(s: &str) -> String {
        s.chars().rev().collect()
    }

    #[test]
    fn lists_load() {
        assert_eq!(SLURS.len(), 7);
        assert_eq!(RESERVED.len(), 1029);
        for l in ["at", "atp", "pds", "bsky", "about", "admin", "barackobama", "dril", "zerohora"] {
            assert!(is_reserved(l), "{l}");
        }
        for l in ["alice", "bob123", "atproto"] {
            assert!(!is_reserved(l), "{l}");
        }
    }

    /// Expected values computed with the reference's JS `hasExplicitSlur`
    /// (separators squashed; one unanchored pattern matches inside words).
    #[test]
    fn slurs_match_reference() {
        for (reversed, want) in [
            ("reggin", true),
            ("s-reggin", true),
            ("t0ggaf", true),
            ("reg.gin", true),
            ("re-gg_in", true),
            ("xxregginxx", true),
            ("moc.elpmaxe.reggins", true),
            ("su.nm.sdipar-nooc", true),
            ("ynnart", true),
            ("sekyk", true),
            ("g4f", true),
            ("r3gg1n", true),
            ("sknihc", true),
            ("tset.sdplv.ecila", false),
            ("elpmaxe.ycips", false),
            ("ved.gniknihc", false),
            ("c2b4pbriyzkl3", false),
            ("moc.rekyks", false),
            ("ekiK", false),
            ("gro.snooccar", false),
        ] {
            let s = rev(reversed);
            assert_eq!(has_explicit_slur(&s), want, "{s}");
        }
    }

    /// Differential check against a corpus labelled by the reference's JS
    /// implementation: `VLPDS_SLUR_CORPUS=path.json cargo test --lib
    /// handle_policy -- --ignored` (JSON array of [string, bool]).
    #[test]
    #[ignore]
    fn slurs_corpus_differential() {
        let path = std::env::var("VLPDS_SLUR_CORPUS").expect("VLPDS_SLUR_CORPUS");
        let cases: Vec<(String, bool)> = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let bad: Vec<_> = cases.iter().filter(|(s, want)| has_explicit_slur(s) != *want).collect();
        assert!(bad.is_empty(), "{} of {} differ: {:?}", bad.len(), cases.len(), &bad[..bad.len().min(10)]);
    }
}
