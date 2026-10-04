//! The rate-limit config object (`config/ratelimits.json` in the bucket):
//! a versioned JSON document of changes from the built-in defaults, its
//! validation, and its compilation into a [`Policy`].
//!
//! ```json
//! {
//!   "version": 7,
//!   "enabled": true,
//!   "limiters": {"global-ip": {"points": 6000}, "oauth-sign-in-ip": {"enabled": false}},
//!   "routes": [{"nsid": "app.bsky.feed.getTimeline", "points": 600, "windowSecs": 300}],
//!   "overrides": [
//!     {"ip": "203.0.113.0/24", "limiters": ["global-ip"], "exempt": true, "note": "relay"},
//!     {"did": "did:plc:abc", "limiters": ["repo-write-hour", "repo-write-day"], "points": 50000}
//!   ],
//!   "updatedAt": "...", "updatedBy": "admin", "note": "...", "history": [...]
//! }
//! ```
//!
//! Everything is optional: `{}` is the defaults. The server writes the
//! metadata fields on save. Unknown fields are rejected on save ([`parse`])
//! so a typo never silently does nothing, but dropped when reading the
//! stored object ([`parse_stored`]): a newer feature level may have written
//! them (DESIGN.md "Rolling upgrades").

use super::{Action, Cidr, KeyKind, Ov, Policy, Spec, BUILTIN};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_WINDOW_SECS: u64 = 7 * 24 * 3600;
/// Each route is a metric label value and a top list.
pub const MAX_ROUTES: usize = 64;
/// IP overrides are matched linearly once per request.
pub const MAX_OVERRIDES: usize = 1000;
pub const HISTORY: usize = 50;
const MAX_NOTE: usize = 280;

fn yes() -> bool {
    true
}

fn is_true(b: &bool) -> bool {
    *b
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Doc {
    #[serde(default)]
    pub version: u64,
    /// False limits nothing (the layer still runs).
    #[serde(default = "yes", skip_serializing_if = "is_true")]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub limiters: BTreeMap<String, LimiterCfg>,
    /// Extra IP-keyed buckets, named `route:{nsid}`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<RouteCfg>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub overrides: Vec<OverrideCfg>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history: Vec<Audit>,
}

impl Default for Doc {
    fn default() -> Self {
        Doc {
            version: 0,
            enabled: true,
            limiters: BTreeMap::new(),
            routes: Vec::new(),
            overrides: Vec::new(),
            updated_at: None,
            updated_by: None,
            note: None,
            history: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LimiterCfg {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub points: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_secs: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RouteCfg {
    pub nsid: String,
    pub points: u32,
    pub window_secs: u64,
    #[serde(default = "yes", skip_serializing_if = "is_true")]
    pub enabled: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OverrideCfg {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    /// Matched against DID-keyed buckets' keys.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub did: Option<String>,
    /// Empty: all buckets.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub limiters: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub exempt: bool,
    /// Instead of `exempt`, in the bucket's window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub points: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Audit {
    pub version: u64,
    pub at: String,
    pub by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    pub node: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub changes: Vec<String>,
}

impl Doc {
    /// Without version or audit metadata.
    pub fn editable(&self) -> Doc {
        Doc {
            version: 0,
            enabled: self.enabled,
            limiters: self.limiters.clone(),
            routes: self.routes.clone(),
            overrides: self.overrides.clone(),
            ..Doc::default()
        }
    }
}

fn route_name(nsid: &str) -> String {
    format!("route:{nsid}")
}

fn valid_nsid(s: &str) -> bool {
    s.len() <= 317
        && s.split('.').count() >= 3
        && s.split('.').all(|seg| !seg.is_empty() && seg.len() <= 63 && seg.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'))
}

fn valid_did(s: &str) -> bool {
    let mut parts = s.splitn(3, ':');
    parts.next() == Some("did")
        && parts.next().is_some_and(|m| !m.is_empty() && m.chars().all(|c| c.is_ascii_lowercase()))
        && parts.next().is_some_and(|id| !id.is_empty())
        && s.len() <= 2048
        && s.chars().all(|c| c.is_ascii_alphanumeric() || "._:%-".contains(c))
}

fn check_points(errs: &mut Vec<String>, at: &str, p: u32) {
    if p == 0 {
        errs.push(format!("{at}: points must be at least 1 (disable the bucket instead)"));
    }
}

fn check_window(errs: &mut Vec<String>, at: &str, w: u64) {
    if !(1..=MAX_WINDOW_SECS).contains(&w) {
        errs.push(format!("{at}: windowSecs must be 1..={MAX_WINDOW_SECS}"));
    }
}

fn check_note(errs: &mut Vec<String>, at: &str, n: &Option<String>) {
    if n.as_ref().is_some_and(|n| n.chars().count() > MAX_NOTE) {
        errs.push(format!("{at}: note is longer than {MAX_NOTE} characters"));
    }
}

/// Returns every problem found, each prefixed with its JSON path.
pub fn compile(doc: Option<&Doc>) -> Result<Policy, Vec<String>> {
    compile_with(doc, super::DEFAULT_MAIL_DAILY_BUDGET)
}

/// [`compile`] over [`Policy::defaults`].
pub fn compile_with(doc: Option<&Doc>, mail_daily_budget: u32) -> Result<Policy, Vec<String>> {
    let mut p = Policy::defaults(mail_daily_budget);
    let Some(doc) = doc else { return Ok(p) };
    let mut errs = Vec::new();
    p.version = doc.version;
    p.enabled = doc.enabled;
    check_note(&mut errs, "note", &doc.note);
    for (name, c) in &doc.limiters {
        let at = format!("limiters.{name}");
        let Some(l) = BUILTIN.iter().find(|l| l.name == name) else {
            errs.push(format!("{at}: unknown limiter"));
            continue;
        };
        let s = &mut p.builtin[l.idx];
        if let Some(n) = c.points {
            check_points(&mut errs, &at, n);
            s.points = n;
        }
        if let Some(w) = c.window_secs {
            check_window(&mut errs, &at, w);
            s.window_ms = w.saturating_mul(1000);
        }
        if let Some(e) = c.enabled {
            s.enabled = e;
        }
    }
    if doc.routes.len() > MAX_ROUTES {
        errs.push(format!("routes: at most {MAX_ROUTES}"));
    }
    for (i, r) in doc.routes.iter().enumerate() {
        let at = format!("routes[{i}]");
        let path = format!("/xrpc/{}", r.nsid);
        if !valid_nsid(&r.nsid) {
            errs.push(format!("{at}.nsid: not an NSID: {:?}", r.nsid));
        } else if super::unlimited_path(&path) {
            errs.push(format!("{at}.nsid: {} is never rate limited", r.nsid));
        }
        check_points(&mut errs, &at, r.points);
        check_window(&mut errs, &at, r.window_secs);
        let spec = Spec::new(&route_name(&r.nsid), KeyKind::Ip, &r.nsid, r.window_secs.saturating_mul(1000), r.points, r.enabled);
        if p.routes.insert(path, spec).is_some() {
            errs.push(format!("{at}.nsid: {} listed twice", r.nsid));
        }
    }
    let names: BTreeSet<String> = p.specs().map(|s| s.name.to_string()).collect();
    if doc.overrides.len() > MAX_OVERRIDES {
        errs.push(format!("overrides: at most {MAX_OVERRIDES}"));
    }
    for (i, o) in doc.overrides.iter().enumerate() {
        let at = format!("overrides[{i}]");
        check_note(&mut errs, &at, &o.note);
        for l in &o.limiters {
            if !names.contains(l) {
                errs.push(format!("{at}.limiters: unknown limiter {l:?}"));
            }
        }
        let action = match (o.exempt, o.points) {
            (true, None) => Action::Exempt,
            (false, Some(n)) => {
                check_points(&mut errs, &at, n);
                Action::Points(n)
            }
            (true, Some(_)) => {
                errs.push(format!("{at}: set exempt or points, not both"));
                continue;
            }
            (false, None) => {
                errs.push(format!("{at}: set exempt: true or a points limit"));
                continue;
            }
        };
        let ov = Ov { limiters: o.limiters.clone(), action };
        match (&o.ip, &o.did) {
            (Some(ip), None) => match Cidr::parse(ip) {
                Some(c) => p.ip_ov.push((c, ov)),
                None => errs.push(format!("{at}.ip: not an IP or CIDR block: {ip:?}")),
            },
            (None, Some(did)) => {
                if !valid_did(did) {
                    errs.push(format!("{at}.did: not a DID: {did:?}"));
                } else {
                    if !o.limiters.is_empty() && !o.limiters.iter().any(|l| p.spec(l).is_some_and(|s| s.key == KeyKind::Did)) {
                        errs.push(format!("{at}.limiters: a DID override only applies to DID-keyed buckets; none listed"));
                    }
                    p.did_ov.entry(did.clone()).or_default().push(ov);
                }
            }
            _ => errs.push(format!("{at}: set exactly one of ip or did")),
        }
    }
    if errs.is_empty() {
        Ok(p)
    } else {
        Err(errs)
    }
}

/// Strict: unknown fields are errors (operator input).
pub fn parse(bytes: &[u8]) -> Result<Doc, String> {
    serde_json::from_slice(bytes).map_err(|e| format!("invalid config JSON: {e}"))
}

const DOC_FIELDS: &[&str] = &["version", "enabled", "limiters", "routes", "overrides", "updatedAt", "updatedBy", "note", "history"];
const LIMITER_FIELDS: &[&str] = &["enabled", "points", "windowSecs"];
const ROUTE_FIELDS: &[&str] = &["nsid", "points", "windowSecs", "enabled"];
const OVERRIDE_FIELDS: &[&str] = &["ip", "did", "limiters", "exempt", "points", "note"];

/// Returns the dropped fields' paths (e.g. `routes[0].burst`). Everything
/// else is as strict as [`parse`].
pub fn parse_stored(bytes: &[u8]) -> Result<(Doc, Vec<String>), String> {
    let mut v: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| format!("invalid config JSON: {e}"))?;
    let mut dropped = Vec::new();
    fn strip(v: &mut serde_json::Value, known: &[&str], at: &str, dropped: &mut Vec<String>) {
        if let Some(m) = v.as_object_mut() {
            m.retain(|k, _| {
                let keep = known.contains(&k.as_str());
                if !keep {
                    dropped.push(if at.is_empty() { k.clone() } else { format!("{at}.{k}") });
                }
                keep
            });
        }
    }
    strip(&mut v, DOC_FIELDS, "", &mut dropped);
    if let Some(m) = v.get_mut("limiters").and_then(|l| l.as_object_mut()) {
        for (name, l) in m.iter_mut() {
            strip(l, LIMITER_FIELDS, &format!("limiters.{name}"), &mut dropped);
        }
    }
    for (key, known) in [("routes", ROUTE_FIELDS), ("overrides", OVERRIDE_FIELDS)] {
        if let Some(a) = v.get_mut(key).and_then(|l| l.as_array_mut()) {
            for (i, item) in a.iter_mut().enumerate() {
                strip(item, known, &format!("{key}[{i}]"), &mut dropped);
            }
        }
    }
    let doc = serde_json::from_value(v).map_err(|e| format!("invalid config JSON: {e}"))?;
    Ok((doc, dropped))
}

fn ov_label(o: &OverrideCfg) -> String {
    let who = o.ip.as_deref().map(|ip| format!("ip {ip}")).or_else(|| o.did.as_deref().map(|d| format!("did {d}"))).unwrap_or_default();
    let what = if o.exempt { "exempt".to_string() } else { format!("{} points", o.points.unwrap_or(0)) };
    let on = if o.limiters.is_empty() { "all buckets".to_string() } else { o.limiters.join(",") };
    format!("{who} {what} on {on}")
}

pub fn changes(old: Option<&Doc>, new: &Doc, mail_daily_budget: u32) -> Vec<String> {
    let def = Doc::default();
    let old = old.unwrap_or(&def);
    let mut out = Vec::new();
    if old.enabled != new.enabled {
        out.push(format!("rate limiting {}", if new.enabled { "enabled" } else { "disabled" }));
    }
    let compiled = |d| compile_with(Some(d), mail_daily_budget).unwrap_or_else(|_| Policy::defaults(mail_daily_budget));
    let (po, pn) = (compiled(old), compiled(new));
    for (a, b) in po.builtin.iter().zip(&pn.builtin) {
        let mut d = Vec::new();
        if a.points != b.points {
            d.push(format!("points {}→{}", a.points, b.points));
        }
        if a.window_ms != b.window_ms {
            d.push(format!("window {}s→{}s", a.window_ms / 1000, b.window_ms / 1000));
        }
        if a.enabled != b.enabled {
            d.push((if b.enabled { "enabled" } else { "disabled" }).to_string());
        }
        if !d.is_empty() {
            out.push(format!("{}: {}", b.name, d.join(", ")));
        }
    }
    for r in &new.routes {
        match old.routes.iter().find(|o| o.nsid == r.nsid) {
            None => out.push(format!("+route {} {} per {}s", r.nsid, r.points, r.window_secs)),
            Some(o) if o != r => out.push(format!(
                "route {}: {} per {}s{} → {} per {}s{}",
                r.nsid,
                o.points,
                o.window_secs,
                if o.enabled { "" } else { " (disabled)" },
                r.points,
                r.window_secs,
                if r.enabled { "" } else { " (disabled)" }
            )),
            _ => {}
        }
    }
    for o in &old.routes {
        if !new.routes.iter().any(|r| r.nsid == o.nsid) {
            out.push(format!("-route {}", o.nsid));
        }
    }
    for o in &new.overrides {
        if !old.overrides.contains(o) {
            out.push(format!("+override {}", ov_label(o)));
        }
    }
    for o in &old.overrides {
        if !new.overrides.contains(o) {
            out.push(format!("-override {}", ov_label(o)));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::super::{GLOBAL_IP, REPO_WRITE_DAY, REPO_WRITE_HOUR};
    use super::*;

    fn doc(j: serde_json::Value) -> Doc {
        serde_json::from_value(j).unwrap()
    }

    #[test]
    fn empty_is_defaults() {
        let d = doc(serde_json::json!({}));
        assert!(d.enabled);
        let p = compile(Some(&d)).unwrap();
        assert!(p.enabled);
        for l in BUILTIN {
            let s = p.builtin(l);
            assert_eq!((s.points, s.window_ms, s.enabled), (l.points, l.window_ms, true));
        }
        // and serializes back to {}
        assert_eq!(serde_json::to_value(&d).unwrap(), serde_json::json!({"version": 0}));
    }

    #[test]
    fn limiter_changes_routes_and_overrides_compile() {
        let d = doc(serde_json::json!({
            "version": 3,
            "limiters": {"global-ip": {"points": 10, "windowSecs": 60}, "oauth-sign-in-ip": {"enabled": false}},
            "routes": [{"nsid": "app.bsky.feed.getTimeline", "points": 5, "windowSecs": 30}],
            "overrides": [
                {"ip": "10.0.0.0/8", "limiters": ["global-ip"], "exempt": true, "note": "relay"},
                {"did": "did:plc:abc", "limiters": ["repo-write-hour"], "points": 50000},
                {"did": "did:web:svc.example.com", "exempt": true}
            ]
        }));
        let p = compile(Some(&d)).unwrap();
        assert_eq!(p.version, 3);
        let g = p.builtin(&GLOBAL_IP);
        assert_eq!((g.points, g.window_ms), (10, 60_000));
        assert!(!p.spec("oauth-sign-in-ip").unwrap().enabled);
        let r = &p.routes["/xrpc/app.bsky.feed.getTimeline"];
        assert_eq!((&*r.name, r.points, r.window_ms, r.key), ("route:app.bsky.feed.getTimeline", 5, 30_000, KeyKind::Ip));
        // the IP override covers global-ip for 10.x clients only
        let m = p.ip_matches(Some("10.1.2.3".parse().unwrap()));
        assert_eq!(p.override_for("global-ip", "10.1.2.3", &m), Some(Action::Exempt));
        assert_eq!(p.override_for("route:app.bsky.feed.getTimeline", "10.1.2.3", &m), None);
        assert!(p.ip_matches(Some("11.1.2.3".parse().unwrap())).is_empty());
        // DID overrides match DID keys, per bucket
        assert_eq!(p.override_for(REPO_WRITE_HOUR.name, "did:plc:abc", &[]), Some(Action::Points(50000)));
        assert_eq!(p.override_for(REPO_WRITE_DAY.name, "did:plc:abc", &[]), None);
        assert_eq!(p.override_for(REPO_WRITE_DAY.name, "did:web:svc.example.com", &[]), Some(Action::Exempt));
        assert_eq!(p.limit_for_key(p.builtin(&REPO_WRITE_HOUR), "did:plc:abc"), Some(50000));
        assert_eq!(p.limit_for_key(p.builtin(&GLOBAL_IP), "10.9.9.9"), None);
        assert_eq!(p.limit_for_key(p.builtin(&GLOBAL_IP), "8.8.8.8"), Some(10));
    }

    #[test]
    fn exempt_beats_points_and_the_larger_limit_wins() {
        let d = doc(serde_json::json!({"overrides": [
            {"ip": "10.0.0.0/8", "points": 100},
            {"ip": "10.1.0.0/16", "points": 500},
            {"did": "did:plc:abc", "exempt": true}
        ]}));
        let p = compile(Some(&d)).unwrap();
        let m = p.ip_matches(Some("10.1.0.1".parse().unwrap()));
        assert_eq!(p.override_for("global-ip", "10.1.0.1", &m), Some(Action::Points(500)));
        assert_eq!(p.override_for("repo-write-hour", "did:plc:abc", &m), Some(Action::Exempt));
    }

    #[test]
    fn validation_reports_every_problem() {
        let d = doc(serde_json::json!({
            "limiters": {"nope": {"points": 1}, "global-ip": {"points": 0, "windowSecs": 0}},
            "routes": [
                {"nsid": "not-an-nsid", "points": 1, "windowSecs": 1},
                {"nsid": "com.atproto.sync.subscribeRepos", "points": 1, "windowSecs": 1},
                {"nsid": "a.b.c", "points": 1, "windowSecs": 1},
                {"nsid": "a.b.c", "points": 1, "windowSecs": 1}
            ],
            "overrides": [
                {"ip": "300.1.1.1", "exempt": true},
                {"did": "plc:abc", "exempt": true},
                {"ip": "1.1.1.1", "did": "did:plc:abc", "exempt": true},
                {"ip": "1.1.1.1"},
                {"ip": "1.1.1.1", "exempt": true, "points": 5},
                {"ip": "1.1.1.1", "exempt": true, "limiters": ["route:x.y.z"]},
                {"did": "did:plc:abc", "exempt": true, "limiters": ["global-ip"]}
            ]
        }));
        let errs = compile(Some(&d)).unwrap_err();
        let want = [
            "limiters.nope: unknown limiter",
            "limiters.global-ip: points must be at least 1",
            "limiters.global-ip: windowSecs must be",
            "routes[0].nsid: not an NSID",
            "routes[1].nsid: com.atproto.sync.subscribeRepos is never rate limited",
            "routes[3].nsid: a.b.c listed twice",
            "overrides[0].ip: not an IP or CIDR",
            "overrides[1].did: not a DID",
            "overrides[2]: set exactly one of ip or did",
            "overrides[3]: set exempt: true or a points limit",
            "overrides[4]: set exempt or points, not both",
            "overrides[5].limiters: unknown limiter \"route:x.y.z\"",
            "overrides[6].limiters: a DID override only applies to DID-keyed buckets",
        ];
        for w in want {
            assert!(errs.iter().any(|e| e.starts_with(w)), "missing {w:?} in {errs:#?}");
        }
        assert_eq!(errs.len(), want.len(), "{errs:#?}");
    }

    #[test]
    fn unknown_fields_and_bad_json_are_rejected() {
        assert!(parse(br#"{"limiters": {"global-ip": {"point": 5}}}"#).is_err());
        assert!(parse(br#"{"bogus": 1}"#).is_err());
        // the stored object: a newer level's fields are dropped, not fatal
        let (d, dropped) = parse_stored(
            br#"{"version": 3, "burst": 1, "limiters": {"global-ip": {"points": 5, "jitter": 2}}, "routes": [{"nsid": "a.b.c", "points": 1, "windowSecs": 2, "cost": 9}], "overrides": [{"ip": "10.0.0.1", "exempt": true, "until": "x"}]}"#,
        )
        .unwrap();
        assert_eq!(dropped, ["burst", "limiters.global-ip.jitter", "routes[0].cost", "overrides[0].until"]);
        assert_eq!((d.version, d.limiters["global-ip"].points, d.routes[0].points, d.overrides[0].exempt), (3, Some(5), 1, true));
        assert!(parse_stored(br#"{"routes": [{"nsid": "a.b.c"}]}"#).is_err(), "a missing field is still an error");
        assert!(parse_stored(b"nope").is_err());
        assert!(parse(b"not json").is_err());
        assert!(parse(br#"{"limiters": {"global-ip": {"points": -1}}}"#).is_err());
        assert!(!parse(br#"{"enabled": false}"#).unwrap().enabled);
    }

    #[test]
    fn changes_describe_the_diff() {
        let old = doc(serde_json::json!({"routes": [{"nsid": "a.b.c", "points": 1, "windowSecs": 1}]}));
        let new = doc(serde_json::json!({
            "enabled": false,
            "limiters": {"global-ip": {"points": 10}},
            "routes": [{"nsid": "d.e.f", "points": 2, "windowSecs": 60}],
            "overrides": [{"did": "did:plc:abc", "exempt": true}]
        }));
        assert_eq!(
            changes(Some(&old), &new, 900),
            vec![
                "rate limiting disabled",
                "global-ip: points 3000→10",
                "+route d.e.f 2 per 60s",
                "-route a.b.c",
                "+override did did:plc:abc exempt on all buckets",
            ]
        );
        assert!(changes(Some(&new), &new, 900).is_empty());
    }
}
