//! atproto OAuth permission grammar, ported from `@atproto/oauth-scopes`.
//!
//! Scope values: the static scopes (`atproto`, `transition:generic`,
//! `transition:chat.bsky`, `transition:email`) and the resource permissions
//! `repo:`, `rpc:`, `blob:`, `account:`, `identity:` plus `include:` (lexicon
//! permission sets, expanded into `repo:`/`rpc:` permissions at token time).
//!
//! A permission is `prefix[:positional][?k=v&k=v...]`. Parsing goes through a
//! small schema engine (`Schema::parse` / `Schema::format`) that mirrors the
//! reference `Parser` exactly, including its normalization rules, so scope
//! strings round-trip identically to the TypeScript implementation.

use super::util::{encode_uri_component, form_encode, parse_form, percent_decode_strict};
use serde_json::Value as J;

pub const STATIC_SCOPES: [&str; 4] = [
    "atproto",
    "transition:email",
    "transition:generic",
    "transition:chat.bsky",
];

// ---------- syntax ----------

/// `isScopeStringFor`: value is exactly `prefix`, or `prefix` followed by ':' or '?'.
pub fn is_scope_string_for(value: &str, prefix: &str) -> bool {
    if value.len() > prefix.len() {
        let next = value.as_bytes()[prefix.len()];
        (next == b':' || next == b'?') && value.starts_with(prefix)
    } else {
        value == prefix
    }
}

enum Params {
    None,
    Query(Vec<(String, String)>),
    Lex(serde_json::Map<String, J>),
}

/// A parsed scope syntax: either a scope string or a lexicon permission object.
pub struct Syntax {
    pub prefix: String,
    pub positional: Option<String>,
    params: Params,
}

/// Result of reading a parameter: absent, invalid (wrong arity), or values.
enum Param {
    Absent,
    Invalid,
    Values(Vec<J>),
}

impl Syntax {
    /// `ScopeStringSyntax.fromString`. None when a percent escape is malformed.
    pub fn from_string(scope: &str) -> Option<Syntax> {
        let param_idx = scope.find('?');
        let colon_idx = scope.find(':');
        let prefix_end = match (param_idx, colon_idx) {
            (None, None) => {
                return Some(Syntax {
                    prefix: scope.into(),
                    positional: None,
                    params: Params::None,
                })
            }
            (Some(a), None) | (None, Some(a)) => a,
            (Some(a), Some(b)) => a.min(b),
        };
        let prefix = scope[..prefix_end].to_string();
        let positional = match (colon_idx, param_idx) {
            (Some(c), None) => Some(percent_decode_strict(&scope[c + 1..])?),
            (Some(c), Some(p)) if c < p => Some(percent_decode_strict(&scope[c + 1..p])?),
            _ => None,
        };
        let params = match param_idx {
            Some(p) if p < scope.len() - 1 => Params::Query(parse_form(&scope[p + 1..])),
            _ => Params::None,
        };
        Some(Syntax {
            prefix,
            positional,
            params,
        })
    }

    /// `LexPermissionSyntax`: a permission object from a permission-set lexicon.
    pub fn from_lex(perm: &serde_json::Map<String, J>) -> Option<Syntax> {
        let prefix = perm.get("resource")?.as_str()?.to_string();
        Some(Syntax {
            prefix,
            positional: None,
            params: Params::Lex(perm.clone()),
        })
    }

    fn keys(&self) -> Vec<String> {
        match &self.params {
            Params::None => vec![],
            Params::Query(q) => {
                let mut out: Vec<String> = Vec::new();
                for (k, _) in q {
                    if !out.contains(k) {
                        out.push(k.clone());
                    }
                }
                out
            }
            Params::Lex(m) => m
                .keys()
                .filter(|k| *k != "type" && *k != "resource")
                .cloned()
                .collect(),
        }
    }

    fn get(&self, key: &str, multiple: bool) -> Param {
        match &self.params {
            Params::None => Param::Absent,
            Params::Query(q) => {
                let vals: Vec<J> = q
                    .iter()
                    .filter(|(k, _)| k == key)
                    .map(|(_, v)| J::String(v.clone()))
                    .collect();
                if vals.is_empty() {
                    Param::Absent
                } else if !multiple && vals.len() > 1 {
                    Param::Invalid
                } else {
                    Param::Values(vals)
                }
            }
            Params::Lex(m) => {
                if key == "type" || key == "resource" {
                    return Param::Absent;
                }
                match m.get(key) {
                    None => Param::Absent,
                    Some(J::Null) => Param::Invalid,
                    Some(J::Array(a)) => {
                        if multiple {
                            Param::Values(a.clone())
                        } else {
                            Param::Invalid
                        }
                    }
                    Some(v) => {
                        if multiple {
                            Param::Invalid
                        } else {
                            Param::Values(vec![v.clone()])
                        }
                    }
                }
            }
        }
    }
}

/// Scope-string normalization: these characters are left unescaped.
fn normalize_uri_component(v: &str) -> String {
    v.replace("%3A", ":")
        .replace("%2F", "/")
        .replace("%2B", "+")
        .replace("%2C", ",")
        .replace("%40", "@")
        .replace("%25", "%")
}

fn syntax_to_string(prefix: &str, positional: Option<&str>, params: &[(String, String)]) -> String {
    let mut s = prefix.to_string();
    if let Some(p) = positional {
        s.push(':');
        s.push_str(&normalize_uri_component(&encode_uri_component(p)));
    }
    if !params.is_empty() {
        s.push('?');
        s.push_str(&normalize_uri_component(&form_encode(params)));
    }
    s
}

// ---------- schema engine ----------

type Validate = fn(&str) -> bool;
type Normalize = fn(Vec<String>) -> Vec<String>;

struct ParamDef {
    name: &'static str,
    multiple: bool,
    required: bool,
    default: Option<&'static [&'static str]>,
    validate: Validate,
    normalize: Option<Normalize>,
}

struct Schema {
    prefix: &'static str,
    params: &'static [ParamDef],
    positional: Option<&'static str>,
}

/// Parsed values per parameter (None = undefined).
type Values = Vec<(&'static str, Option<Vec<String>>)>;

fn val<'a>(v: &'a Values, name: &str) -> Option<&'a Vec<String>> {
    v.iter()
        .find(|(k, _)| *k == name)
        .and_then(|(_, v)| v.as_ref())
}

fn as_param_str(v: &J) -> Option<String> {
    match v {
        J::String(s) => Some(s.clone()),
        // Non-string param values never pass any validator below.
        _ => None,
    }
}

impl Schema {
    fn parse(&self, syn: &Syntax) -> Option<Values> {
        for k in syn.keys() {
            if !self.params.iter().any(|d| d.name == k) {
                return None;
            }
        }
        let mut out = Values::new();
        for d in self.params {
            let is_pos = self.positional == Some(d.name);
            match syn.get(d.name, d.multiple) {
                Param::Invalid => return None,
                Param::Values(vals) => {
                    if is_pos && syn.positional.is_some() {
                        return None;
                    }
                    if d.multiple && vals.is_empty() {
                        return None;
                    }
                    let mut strs = Vec::with_capacity(vals.len());
                    for v in &vals {
                        let s = as_param_str(v)?;
                        if !(d.validate)(&s) {
                            return None;
                        }
                        strs.push(s);
                    }
                    out.push((d.name, Some(strs)));
                }
                Param::Absent => {
                    if let (true, Some(p)) = (is_pos, syn.positional.as_ref()) {
                        if !(d.validate)(p) {
                            return None;
                        }
                        out.push((d.name, Some(vec![p.clone()])));
                    } else if d.required {
                        return None;
                    } else {
                        out.push((
                            d.name,
                            d.default.map(|d| d.iter().map(|s| s.to_string()).collect()),
                        ));
                    }
                }
            }
        }
        Some(out)
    }

    fn format(&self, values: &Values) -> String {
        let mut params: Vec<(String, String)> = Vec::new();
        let mut positional: Option<String> = None;
        for d in self.params {
            let Some(v) = val(values, d.name) else {
                continue;
            };
            let norm = match d.normalize {
                Some(f) => f(v.clone()),
                None => v.clone(),
            };
            if !d.required {
                if let Some(def) = d.default {
                    if same_set(def, &norm) {
                        continue;
                    }
                }
            }
            if d.multiple {
                if self.positional == Some(d.name) && norm.len() == 1 {
                    positional = Some(norm[0].clone());
                } else {
                    let mut seen: Vec<&String> = Vec::new();
                    for x in &norm {
                        if !seen.contains(&x) {
                            seen.push(x);
                            params.push((d.name.to_string(), x.clone()));
                        }
                    }
                }
            } else if self.positional == Some(d.name) {
                positional = Some(norm[0].clone());
            } else {
                params.retain(|(k, _)| k != d.name);
                params.push((d.name.to_string(), norm[0].clone()));
            }
        }
        syntax_to_string(self.prefix, positional.as_deref(), &params)
    }
}

fn same_set(a: &[&str], b: &[String]) -> bool {
    a.iter().all(|x| b.iter().any(|y| y == x)) && b.iter().all(|y| a.iter().any(|x| x == y))
}

// ---------- validators ----------

/// atproto NSID syntax (`@atproto/syntax` isValidNsid).
pub fn is_nsid(s: &str) -> bool {
    if s.len() > 317 || !s.is_ascii() {
        return false;
    }
    let segs: Vec<&str> = s.split('.').collect();
    if segs.len() < 3 {
        return false;
    }
    let (name, domain) = segs.split_last().unwrap();
    if domain.iter().map(|d| d.len() + 1).sum::<usize>() - 1 > 253 {
        return false;
    }
    for (i, seg) in domain.iter().enumerate() {
        let b = seg.as_bytes();
        if b.is_empty() || b.len() > 63 || b[0] == b'-' || b[b.len() - 1] == b'-' {
            return false;
        }
        if !b.iter().all(|c| c.is_ascii_alphanumeric() || *c == b'-') {
            return false;
        }
        if i == 0 && b[0].is_ascii_digit() {
            return false;
        }
    }
    let nb = name.as_bytes();
    !nb.is_empty()
        && nb.len() <= 63
        && nb[0].is_ascii_alphabetic()
        && nb.iter().all(|c| c.is_ascii_alphanumeric())
}

/// did:plc (base32, 32 chars) or hostname-level did:web (port only for localhost).
pub fn is_atproto_did(s: &str) -> bool {
    if let Some(id) = s.strip_prefix("did:plc:") {
        return s.len() == 32 && id.bytes().all(|b| matches!(b, b'a'..=b'z' | b'2'..=b'7'));
    }
    if let Some(host) = s.strip_prefix("did:web:") {
        if host.is_empty() || host.contains(':') || host.len() > 253 {
            return false;
        }
        let decoded = host.replace("%3A", ":").replace("%3a", ":");
        let (h, port) = match decoded.split_once(':') {
            Some((h, p)) => (h.to_string(), Some(p.to_string())),
            None => (decoded.clone(), None),
        };
        if let Some(p) = port {
            if h != "localhost" || p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()) {
                return false;
            }
        }
        return !h.is_empty()
            && h.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-');
    }
    false
}

fn is_fragment(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-._~!$&'()*+,;=:@/?%".contains(&b))
}

/// `did:...#fragment` with an atproto DID.
pub fn is_did_ref_absolute(s: &str) -> bool {
    match s.split_once('#') {
        Some((did, frag)) => is_atproto_did(did) && is_fragment(frag),
        None => false,
    }
}

fn is_string_slash_string(v: &str) -> bool {
    match v.find('/') {
        None => false,
        Some(i) => i != 0 && i != v.len() - 1 && !v[i + 1..].contains('/') && !v.contains(' '),
    }
}

pub fn is_mime(v: &str) -> bool {
    is_string_slash_string(v) && !v.contains('*')
}

pub fn is_accept(v: &str) -> bool {
    v == "*/*" || (is_string_slash_string(v) && (!v.contains('*') || v.ends_with("/*")))
}

fn matches_accept(accept: &str, mime: &str) -> bool {
    if accept == "*/*" {
        true
    } else if let Some(base) = accept.strip_suffix('*') {
        mime.starts_with(base)
    } else {
        accept == mime
    }
}

fn v_collection(v: &str) -> bool {
    v == "*" || is_nsid(v)
}
fn v_repo_action(v: &str) -> bool {
    REPO_ACTIONS.contains(&v)
}
fn v_aud(v: &str) -> bool {
    v == "*" || is_did_ref_absolute(v)
}
fn v_account_attr(v: &str) -> bool {
    ["email", "repo", "status"].contains(&v)
}
fn v_account_action(v: &str) -> bool {
    ["read", "manage"].contains(&v)
}
fn v_identity_attr(v: &str) -> bool {
    ["handle", "*"].contains(&v)
}

pub const REPO_ACTIONS: [&str; 3] = ["create", "update", "delete"];

fn n_collection(v: Vec<String>) -> Vec<String> {
    if v.len() > 1 {
        if v.iter().any(|x| x == "*") {
            return vec!["*".into()];
        }
        let mut u = v;
        u.sort();
        u.dedup();
        return u;
    }
    v
}
fn n_repo_action(v: Vec<String>) -> Vec<String> {
    REPO_ACTIONS
        .iter()
        .filter(|a| v.iter().any(|x| x == *a))
        .map(|s| s.to_string())
        .collect()
}
fn n_lxm(v: Vec<String>) -> Vec<String> {
    if v.len() > 1 && v.iter().any(|x| x == "*") {
        return vec!["*".into()];
    }
    let mut u = v;
    u.sort();
    u.dedup();
    u
}
fn n_accept(v: Vec<String>) -> Vec<String> {
    if v.iter().any(|x| x == "*/*") {
        return vec!["*/*".into()];
    }
    let lower: Vec<String> = v.iter().map(|s| s.to_lowercase()).collect();
    let mut out: Vec<String> = lower
        .iter()
        .filter(|x| {
            if x.ends_with("/*") {
                return true;
            }
            let base = x.split('/').next().unwrap_or("");
            !lower.iter().any(|y| *y == format!("{base}/*"))
        })
        .cloned()
        .collect();
    out.sort();
    out
}

static REPO: Schema = Schema {
    prefix: "repo",
    params: &[
        ParamDef {
            name: "collection",
            multiple: true,
            required: true,
            default: None,
            validate: v_collection,
            normalize: Some(n_collection),
        },
        ParamDef {
            name: "action",
            multiple: true,
            required: false,
            default: Some(&REPO_ACTIONS),
            validate: v_repo_action,
            normalize: Some(n_repo_action),
        },
    ],
    positional: Some("collection"),
};

static RPC: Schema = Schema {
    prefix: "rpc",
    params: &[
        ParamDef {
            name: "lxm",
            multiple: true,
            required: true,
            default: None,
            validate: v_collection,
            normalize: Some(n_lxm),
        },
        ParamDef {
            name: "aud",
            multiple: false,
            required: true,
            default: None,
            validate: v_aud,
            normalize: None,
        },
    ],
    positional: Some("lxm"),
};

static BLOB: Schema = Schema {
    prefix: "blob",
    params: &[ParamDef {
        name: "accept",
        multiple: true,
        required: true,
        default: None,
        validate: is_accept,
        normalize: Some(n_accept),
    }],
    positional: Some("accept"),
};

static ACCOUNT: Schema = Schema {
    prefix: "account",
    params: &[
        ParamDef {
            name: "attr",
            multiple: false,
            required: true,
            default: None,
            validate: v_account_attr,
            normalize: None,
        },
        ParamDef {
            name: "action",
            multiple: true,
            required: false,
            default: Some(&["read"]),
            validate: v_account_action,
            normalize: None,
        },
    ],
    positional: Some("attr"),
};

static IDENTITY: Schema = Schema {
    prefix: "identity",
    params: &[ParamDef {
        name: "attr",
        multiple: false,
        required: true,
        default: None,
        validate: v_identity_attr,
        normalize: None,
    }],
    positional: Some("attr"),
};

static INCLUDE: Schema = Schema {
    prefix: "include",
    params: &[
        ParamDef {
            name: "nsid",
            multiple: false,
            required: true,
            default: None,
            validate: is_nsid,
            normalize: None,
        },
        ParamDef {
            name: "aud",
            multiple: false,
            required: false,
            default: None,
            validate: is_did_ref_absolute,
            normalize: None,
        },
    ],
    positional: Some("nsid"),
};

// ---------- permissions ----------

#[derive(Clone, Debug, PartialEq)]
pub enum Permission {
    Repo {
        collection: Vec<String>,
        action: Vec<String>,
    },
    Rpc {
        aud: String,
        lxm: Vec<String>,
    },
    Blob {
        accept: Vec<String>,
    },
    Account {
        attr: String,
        action: Vec<String>,
    },
    Identity {
        attr: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct IncludeScope {
    pub nsid: String,
    pub aud: Option<String>,
}

fn first(v: &Values, k: &str) -> String {
    val(v, k)
        .and_then(|x| x.first().cloned())
        .unwrap_or_default()
}

impl Permission {
    pub fn from_syntax(syn: &Syntax) -> Option<Permission> {
        match syn.prefix.as_str() {
            "repo" => {
                let v = REPO.parse(syn)?;
                Some(Permission::Repo {
                    collection: val(&v, "collection")?.clone(),
                    action: val(&v, "action")?.clone(),
                })
            }
            "rpc" => {
                let v = RPC.parse(syn)?;
                let aud = first(&v, "aud");
                let lxm = val(&v, "lxm")?.clone();
                // rpc:*?aud=* is forbidden
                if aud == "*" && lxm.iter().any(|x| x == "*") {
                    return None;
                }
                Some(Permission::Rpc { aud, lxm })
            }
            "blob" => {
                let v = BLOB.parse(syn)?;
                Some(Permission::Blob {
                    accept: val(&v, "accept")?.clone(),
                })
            }
            "account" => {
                let v = ACCOUNT.parse(syn)?;
                Some(Permission::Account {
                    attr: first(&v, "attr"),
                    action: val(&v, "action")?.clone(),
                })
            }
            "identity" => {
                let v = IDENTITY.parse(syn)?;
                Some(Permission::Identity {
                    attr: first(&v, "attr"),
                })
            }
            _ => None,
        }
    }

    pub fn parse(scope: &str) -> Option<Permission> {
        for p in ["repo", "rpc", "blob", "account", "identity"] {
            if is_scope_string_for(scope, p) {
                return Permission::from_syntax(&Syntax::from_string(scope)?);
            }
        }
        None
    }

    pub fn to_scope_string(&self) -> String {
        match self {
            Permission::Repo { collection, action } => REPO.format(&vec![
                ("collection", Some(collection.clone())),
                ("action", Some(action.clone())),
            ]),
            Permission::Rpc { aud, lxm } => RPC.format(&vec![
                ("lxm", Some(lxm.clone())),
                ("aud", Some(vec![aud.clone()])),
            ]),
            Permission::Blob { accept } => BLOB.format(&vec![("accept", Some(accept.clone()))]),
            Permission::Account { attr, action } => ACCOUNT.format(&vec![
                ("attr", Some(vec![attr.clone()])),
                ("action", Some(action.clone())),
            ]),
            Permission::Identity { attr } => {
                IDENTITY.format(&vec![("attr", Some(vec![attr.clone()]))])
            }
        }
    }

    pub fn matches_repo(&self, coll: &str, act: &str) -> bool {
        matches!(self, Permission::Repo { collection, action }
            if action.iter().any(|a| a == act) && collection.iter().any(|c| c == "*" || c == coll))
    }
    pub fn matches_rpc(&self, l: &str, a: &str) -> bool {
        matches!(self, Permission::Rpc { aud, lxm }
            if (aud == "*" || aud == a) && lxm.iter().any(|x| x == "*" || x == l))
    }
    pub fn matches_blob(&self, mime: &str) -> bool {
        matches!(self, Permission::Blob { accept } if is_mime(mime) && accept.iter().any(|a| matches_accept(a, mime)))
    }
    pub fn matches_account(&self, at: &str, act: &str) -> bool {
        matches!(self, Permission::Account { attr, action }
            if attr == at && (action.iter().any(|a| a == "manage") || action.iter().any(|a| a == act)))
    }
    pub fn matches_identity(&self, at: &str) -> bool {
        matches!(self, Permission::Identity { attr } if attr == "*" || attr == at)
    }
}

impl IncludeScope {
    pub fn parse(scope: &str) -> Option<IncludeScope> {
        if !is_scope_string_for(scope, "include") {
            return None;
        }
        let v = INCLUDE.parse(&Syntax::from_string(scope)?)?;
        Some(IncludeScope {
            nsid: first(&v, "nsid"),
            aud: val(&v, "aud").and_then(|a| a.first().cloned()),
        })
    }

    pub fn to_scope_string(&self) -> String {
        INCLUDE.format(&vec![
            ("nsid", Some(vec![self.nsid.clone()])),
            ("aud", self.aud.clone().map(|a| vec![a])),
        ])
    }

    /// Whether `other` is under this permission set's NSID group (same
    /// authority + group prefix, i.e. everything up to the last '.').
    pub fn is_parent_authority_of(&self, other: &str) -> bool {
        if other == "*" {
            return false;
        }
        let Some(group_end) = self.nsid.rfind('.') else {
            return false;
        };
        if group_end + 1 >= other.len() {
            return false;
        }
        other.as_bytes().get(..=group_end) == self.nsid.as_bytes().get(..=group_end)
    }

    /// Expands a permission-set lexicon (`defs.main`, type "permission-set")
    /// into the repo/rpc permissions it grants under this include scope.
    pub fn to_permissions(&self, permission_set: &J) -> Vec<Permission> {
        let mut out = Vec::new();
        let Some(perms) = permission_set.get("permissions").and_then(|p| p.as_array()) else {
            return out;
        };
        for p in perms {
            let Some(obj) = p.as_object() else { continue };
            let resource = obj.get("resource").and_then(|r| r.as_str()).unwrap_or("");
            let syn = match resource {
                "repo" => Syntax::from_lex(obj),
                "rpc" => {
                    // "rpc" permissions with a fixed audience are not allowed in permission sets.
                    match obj.get("aud") {
                        None => {}
                        Some(J::String(a)) if a == "*" => {}
                        Some(_) => continue,
                    }
                    if obj.get("inheritAud") == Some(&J::Bool(true))
                        && obj.get("aud").is_none()
                        && self.aud.is_some()
                    {
                        let mut o = obj.clone();
                        o.remove("inheritAud");
                        o.insert("aud".into(), J::String(self.aud.clone().unwrap()));
                        Syntax::from_lex(&o)
                    } else {
                        Syntax::from_lex(obj)
                    }
                }
                _ => continue,
            };
            let Some(syn) = syn else { continue };
            let Some(perm) = Permission::from_syntax(&syn) else {
                continue;
            };
            let allowed = match &perm {
                Permission::Rpc { lxm, .. } => lxm.iter().all(|l| self.is_parent_authority_of(l)),
                Permission::Repo { collection, .. } => {
                    collection.iter().all(|c| self.is_parent_authority_of(c))
                }
                _ => false,
            };
            if allowed {
                out.push(perm);
            }
        }
        out
    }
}

/// `isAtprotoOauthScope`: a static scope or a parseable permission/include.
pub fn is_atproto_oauth_scope(v: &str) -> bool {
    STATIC_SCOPES.contains(&v) || Permission::parse(v).is_some() || IncludeScope::parse(v).is_some()
}

/// `normalizeAtprotoOauthScopeValue`.
pub fn normalize_scope_value(v: &str) -> Option<String> {
    if STATIC_SCOPES.contains(&v) {
        return Some(v.to_string());
    }
    if let Some(p) = Permission::parse(v) {
        return Some(p.to_scope_string());
    }
    IncludeScope::parse(v).map(|i| i.to_scope_string())
}

/// The scope a resource check would have needed (for error messages).
pub fn scope_needed_repo(collection: &str, action: &str) -> String {
    REPO.format(&vec![
        ("collection", Some(vec![collection.into()])),
        ("action", Some(vec![action.into()])),
    ])
}

// ---------- granted scopes ----------

/// Granted OAuth scopes of an access token, pre-parsed. The `allows_*`
/// methods implement `ScopePermissionsTransition` (granular permissions plus
/// the transitional `transition:*` scopes).
#[derive(Clone, Debug, Default)]
pub struct ScopeSet {
    pub raw: Vec<String>,
    perms: Vec<Permission>,
    generic: bool,
    chat: bool,
    email: bool,
}

impl ScopeSet {
    pub fn new(scope: &str) -> ScopeSet {
        let raw: Vec<String> = scope
            .split(' ')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        let perms = raw.iter().filter_map(|s| Permission::parse(s)).collect();
        ScopeSet {
            generic: raw.iter().any(|s| s == "transition:generic"),
            chat: raw.iter().any(|s| s == "transition:chat.bsky"),
            email: raw.iter().any(|s| s == "transition:email"),
            raw,
            perms,
        }
    }

    pub fn has(&self, scope: &str) -> bool {
        self.raw.iter().any(|s| s == scope)
    }

    pub fn to_scope_string(&self) -> String {
        self.raw.join(" ")
    }

    /// action: "create" | "update" | "delete"
    pub fn allows_repo(&self, collection: &str, action: &str) -> bool {
        self.generic
            || self
                .perms
                .iter()
                .any(|p| p.matches_repo(collection, action))
    }

    pub fn allows_rpc(&self, lxm: &str, aud: &str) -> bool {
        if self.generic && (lxm == "*" || !lxm.starts_with("chat.bsky.")) {
            return true;
        }
        if self.chat && lxm.starts_with("chat.bsky.") {
            return true;
        }
        self.perms.iter().any(|p| p.matches_rpc(lxm, aud))
    }

    pub fn allows_blob(&self, mime: &str) -> bool {
        self.generic || self.perms.iter().any(|p| p.matches_blob(mime))
    }

    /// attr: "email" | "repo" | "status"; action: "read" | "manage"
    pub fn allows_account(&self, attr: &str, action: &str) -> bool {
        if attr == "email" && action == "read" && self.email {
            return true;
        }
        self.perms.iter().any(|p| p.matches_account(attr, action))
    }

    /// attr: "handle" | "*"
    pub fn allows_identity(&self, attr: &str) -> bool {
        self.perms.iter().any(|p| p.matches_identity(attr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm(s: &str) -> Option<String> {
        normalize_scope_value(s)
    }

    #[test]
    fn repo_parse_and_format() {
        assert_eq!(
            norm("repo:app.bsky.feed.post").unwrap(),
            "repo:app.bsky.feed.post"
        );
        assert_eq!(
            norm("repo:app.bsky.feed.post?action=create&action=update&action=delete").unwrap(),
            "repo:app.bsky.feed.post"
        );
        assert_eq!(
            norm("repo:app.bsky.feed.post?action=delete&action=create").unwrap(),
            "repo:app.bsky.feed.post?action=create&action=delete"
        );
        assert_eq!(
            norm("repo?collection=app.bsky.feed.post&collection=app.bsky.feed.like").unwrap(),
            "repo?collection=app.bsky.feed.like&collection=app.bsky.feed.post"
        );
        assert_eq!(
            norm("repo?collection=*&collection=app.bsky.feed.like").unwrap(),
            "repo:*"
        );
        assert_eq!(norm("repo:*").unwrap(), "repo:*");
        assert!(norm("repo").is_none());
        assert!(norm("repo:not-an-nsid").is_none());
        assert!(norm("repo:app.bsky.feed.post?action=read").is_none());
        assert!(norm("repo:app.bsky.feed.post?collection=app.bsky.feed.like").is_none());
        assert!(norm("repo:app.bsky.feed.post?foo=bar").is_none());
        let p = Permission::parse("repo:app.bsky.feed.like").unwrap();
        assert!(p.matches_repo("app.bsky.feed.like", "create"));
        assert!(!p.matches_repo("app.bsky.feed.post", "create"));
        let p = Permission::parse("repo:*?action=delete").unwrap();
        assert!(p.matches_repo("x.y.z", "delete"));
        assert!(!p.matches_repo("x.y.z", "create"));
    }

    #[test]
    fn rpc() {
        assert!(Permission::parse("rpc:*?aud=*").is_none());
        assert!(
            Permission::parse("rpc:app.bsky.feed.getFeed").is_none(),
            "aud required"
        );
        let p =
            Permission::parse("rpc:app.bsky.feed.getFeed?aud=did:web:api.bsky.app%23bsky_appview")
                .unwrap();
        assert!(p.matches_rpc("app.bsky.feed.getFeed", "did:web:api.bsky.app#bsky_appview"));
        assert!(!p.matches_rpc("app.bsky.feed.getFeed", "did:web:other.example#x"));
        assert_eq!(
            p.to_scope_string(),
            "rpc:app.bsky.feed.getFeed?aud=did:web:api.bsky.app%23bsky_appview"
        );
        let p =
            Permission::parse("rpc?lxm=app.bsky.feed.getFeed&lxm=app.bsky.actor.getProfile&aud=*")
                .unwrap();
        assert_eq!(
            p.to_scope_string(),
            "rpc?lxm=app.bsky.actor.getProfile&lxm=app.bsky.feed.getFeed&aud=*"
        );
        assert!(p.matches_rpc("app.bsky.actor.getProfile", "did:web:x.com#y"));
    }

    #[test]
    fn blob() {
        let p = Permission::parse("blob:image/*").unwrap();
        assert!(p.matches_blob("image/png"));
        assert!(!p.matches_blob("video/mp4"));
        assert!(!p.matches_blob("image/*"));
        assert_eq!(
            norm("blob?accept=image/png&accept=image/*&accept=video/mp4").unwrap(),
            "blob?accept=image/*&accept=video/mp4"
        );
        assert_eq!(
            norm("blob?accept=image/png&accept=*/*").unwrap(),
            "blob:*/*"
        );
        assert!(norm("blob:image").is_none());
        assert!(norm("blob:*/png").is_none());
    }

    #[test]
    fn account_identity() {
        let p = Permission::parse("account:email").unwrap();
        assert!(p.matches_account("email", "read"));
        assert!(!p.matches_account("email", "manage"));
        assert_eq!(p.to_scope_string(), "account:email");
        let p = Permission::parse("account:repo?action=manage").unwrap();
        assert!(p.matches_account("repo", "read"));
        assert!(p.matches_account("repo", "manage"));
        assert!(Permission::parse("account:foo").is_none());
        assert!(Permission::parse("identity:*")
            .unwrap()
            .matches_identity("handle"));
        assert!(Permission::parse("identity:handle")
            .unwrap()
            .matches_identity("handle"));
        assert!(!Permission::parse("identity:handle")
            .unwrap()
            .matches_identity("*"));
        assert!(Permission::parse("identity").is_none());
    }

    #[test]
    fn include() {
        let i = IncludeScope::parse("include:com.example.authBasic?aud=did:web:example.com%23svc")
            .unwrap();
        assert_eq!(i.nsid, "com.example.authBasic");
        assert_eq!(i.aud.as_deref(), Some("did:web:example.com#svc"));
        assert_eq!(
            i.to_scope_string(),
            "include:com.example.authBasic?aud=did:web:example.com%23svc"
        );
        assert!(i.is_parent_authority_of("com.example.foo"));
        assert!(i.is_parent_authority_of("com.example.foo.bar"));
        assert!(!i.is_parent_authority_of("com.other.foo"));
        assert!(!i.is_parent_authority_of("com.example"));
        assert!(!i.is_parent_authority_of("*"));
        let set = serde_json::json!({
            "type": "permission-set",
            "permissions": [
                {"type": "permission", "resource": "repo", "collection": ["com.example.post", "com.example.like"]},
                {"type": "permission", "resource": "repo", "collection": ["app.bsky.feed.post"]},
                {"type": "permission", "resource": "rpc", "lxm": ["com.example.getThing"], "inheritAud": true},
                {"type": "permission", "resource": "rpc", "lxm": ["com.example.fixed"], "aud": "did:web:x.com#y"},
                {"type": "permission", "resource": "blob", "accept": ["*/*"]},
            ]
        });
        let perms: Vec<String> = i
            .to_permissions(&set)
            .iter()
            .map(|p| p.to_scope_string())
            .collect();
        assert_eq!(
            perms,
            vec![
                "repo?collection=com.example.like&collection=com.example.post",
                "rpc:com.example.getThing?aud=did:web:example.com%23svc"
            ]
        );
        // without aud, inheritAud is an unknown key -> dropped
        let i2 = IncludeScope::parse("include:com.example.authBasic").unwrap();
        assert_eq!(i2.to_permissions(&set).len(), 1);
    }

    #[test]
    fn transition() {
        let s = ScopeSet::new("atproto transition:generic");
        assert!(s.allows_repo("app.bsky.feed.post", "create"));
        assert!(s.allows_blob("image/png"));
        assert!(s.allows_rpc(
            "app.bsky.feed.getTimeline",
            "did:web:api.bsky.app#bsky_appview"
        ));
        assert!(!s.allows_rpc(
            "chat.bsky.convo.listConvos",
            "did:web:api.bsky.chat#bsky_chat"
        ));
        assert!(!s.allows_account("email", "read"));
        let s = ScopeSet::new("atproto transition:chat.bsky transition:email");
        assert!(s.allows_rpc(
            "chat.bsky.convo.listConvos",
            "did:web:api.bsky.chat#bsky_chat"
        ));
        assert!(s.allows_account("email", "read"));
        assert!(!s.allows_account("email", "manage"));
        let s = ScopeSet::new("atproto repo:app.bsky.feed.like");
        assert!(s.allows_repo("app.bsky.feed.like", "create"));
        assert!(!s.allows_repo("app.bsky.feed.post", "create"));
        assert!(!s.allows_blob("image/png"));
    }

    #[test]
    fn validity() {
        for s in [
            "atproto",
            "transition:generic",
            "repo:*",
            "include:com.example.foo",
            "identity:handle",
            "account:status?action=manage",
        ] {
            assert!(is_atproto_oauth_scope(s), "{s}");
        }
        for s in [
            "openid",
            "repo",
            "transition:foo",
            "include:*",
            "rpc:*?aud=*",
        ] {
            assert!(!is_atproto_oauth_scope(s), "{s}");
        }
        assert!(is_nsid("app.bsky.feed.post"));
        assert!(!is_nsid("app.bsky"));
        assert!(!is_nsid("1app.bsky.feed"));
        assert!(!is_nsid("app.bsky.feed-post"));
        assert!(is_atproto_did("did:plc:abcdefghijklmnopqrstuvwx"));
        assert!(is_atproto_did("did:web:localhost%3A1234"));
        assert!(!is_atproto_did("did:web:example.com%3A1234"));
    }
}
