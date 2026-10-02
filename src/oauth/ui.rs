//! Server-rendered pages for the authorization flow and session management.
//! No external assets: one inline stylesheet allowed by hash in the
//! Content-Security-Policy. The only script is the fixed auto-submit of the
//! `response_mode=form_post` page, also allowed by hash and only there.

use super::scopes::{IncludeScope, Permission};
use super::util::{html_escape as e, sha256};
use base64::Engine;
use serde_json::Value as J;
use std::sync::LazyLock;

const STYLE: &str = r#"
@font-face{font-family:"Schibsted Grotesk";src:url(/fonts/schibsted-grotesk.woff2) format("woff2");font-weight:400 900;font-display:swap}
@font-face{font-family:"JetBrains Mono";src:url(/fonts/jetbrains-mono.woff2) format("woff2");font-weight:100 800;font-display:swap}
:root{--paper:#edefea;--sheet:#f8f9f6;--ink:#18222d;--ink2:#4d5966;--rule:#c9cfc8;--accent:#17705f;--accent-ink:#fff;--amber:#b9770e;--danger:#a93a24;--focus:#3561c9;color-scheme:light}
@media (prefers-color-scheme:dark){:root{--paper:#121820;--sheet:#19212b;--ink:#dce4e3;--ink2:#93a0a8;--rule:#2a3542;--accent:#4fbf9f;--accent-ink:#0c1a16;--amber:#e3a43a;--danger:#f0806a;--focus:#8fb0ff;color-scheme:dark}}
*{box-sizing:border-box}
body{margin:0;background:var(--paper);color:var(--ink);font:15px/1.55 "Schibsted Grotesk",ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;-webkit-font-smoothing:antialiased}
main{max-width:480px;margin:6vh auto 8vh;padding:0 16px}
.brand{display:flex;align-items:center;gap:10px;margin:0 0 18px;font-weight:800;font-size:17px;letter-spacing:-.01em}
.brand svg{display:block}
.brand span{color:var(--ink2);font-weight:500;font-size:14px}
.card{background:var(--sheet);border:1px solid var(--rule);border-radius:8px;padding:26px 24px 22px;box-shadow:0 1px 0 var(--rule)}
.strata{height:5px;margin:-26px -24px 22px;border-radius:7px 7px 0 0;background:linear-gradient(var(--accent) 0 2px,transparent 2px 3px,var(--amber) 3px 4px,transparent 4px)}
h1{font-size:24px;line-height:1.2;letter-spacing:-.015em;margin:0 0 6px;font-weight:750}
p{margin:8px 0}
b{font-weight:650}
a{color:var(--accent);text-underline-offset:3px}
.muted{color:var(--ink2);font-size:13.5px}
.client{font-family:"JetBrains Mono",ui-monospace,Menlo,monospace;font-size:12.5px;overflow-wrap:anywhere;background:var(--paper);border:1px solid var(--rule);border-left:3px solid var(--accent);border-radius:4px;padding:9px 11px;margin:10px 0}
label{display:block;font-size:13.5px;font-weight:600;margin:16px 0 5px}
input[type=text],input[type=password],input[type=email]{width:100%;padding:10px 12px;border:1px solid var(--rule);border-radius:5px;background:var(--paper);color:var(--ink);font:inherit;font-size:15px}
.affix{display:flex;align-items:stretch}
.affix input{border-radius:5px 0 0 5px;min-width:0}
.affix span{display:flex;align-items:center;padding:0 11px;border:1px solid var(--rule);border-left:0;border-radius:0 5px 5px 0;background:var(--sheet);color:var(--ink2);font-family:"JetBrains Mono",ui-monospace,Menlo,monospace;font-size:13px;white-space:nowrap}
.hint{color:var(--ink2);font-size:12.5px;margin:5px 0 0}
.alt{margin:18px 0 0;padding-top:14px;border-top:1px solid var(--rule);font-size:14px;color:var(--ink2)}
input:focus-visible,button:focus-visible,a:focus-visible{outline:2px solid var(--focus);outline-offset:2px}
button{font:inherit;font-weight:600;border-radius:5px;padding:9px 16px;border:1px solid var(--rule);background:var(--sheet);color:var(--ink);cursor:pointer}
button:hover{border-color:var(--ink2)}
button.primary{background:var(--accent);border-color:var(--accent);color:var(--accent-ink)}
button.primary:hover{filter:brightness(1.08)}
button.danger{color:var(--danger)}
button.danger:hover{border-color:var(--danger)}
.row{display:flex;gap:10px;justify-content:flex-end;margin-top:22px;flex-wrap:wrap}
.err{color:var(--danger);margin:12px 0;padding:8px 11px;border:1px solid var(--danger);border-radius:5px;font-size:14px}
ul.perms{padding-left:0;margin:10px 0;list-style:none}
ul.perms li{margin:0;padding:8px 0 8px 18px;border-top:1px solid var(--rule);position:relative}
ul.perms li:before{content:"";position:absolute;left:2px;top:16px;width:7px;height:7px;border-radius:1px;background:var(--accent)}
ul.perms ul.perms li{border-top:0;padding:3px 0 3px 16px}
ul.perms ul.perms li:before{top:11px;width:5px;height:5px;background:var(--ink2)}
.acct{display:flex;align-items:center;justify-content:space-between;gap:10px;border:1px solid var(--rule);border-radius:6px;padding:10px 12px;margin:8px 0;background:var(--paper)}
.acct form{margin:0}
.acct .who{min-width:0;overflow-wrap:anywhere}
.acct .who .muted{font-family:"JetBrains Mono",ui-monospace,Menlo,monospace;font-size:12px}
table{width:100%;border-collapse:collapse;font-size:13.5px}
td{border-top:1px solid var(--rule);padding:10px 4px;vertical-align:top;overflow-wrap:anywhere}
td:last-child{text-align:right;width:1%;white-space:nowrap;overflow-wrap:normal;padding-left:10px}
footer{margin-top:14px;text-align:center}
label.check{display:flex;align-items:center;gap:8px;font-weight:500;margin:12px 0 4px}
"#;

static STYLE_HASH: LazyLock<String> =
    LazyLock::new(|| base64::engine::general_purpose::STANDARD.encode(sha256(STYLE)));

/// Content-Security-Policy for our pages. `form_action` lists extra
/// form-action sources: browsers apply form-action to the redirect that
/// follows a form post, so the consent page must allow the client's
/// redirect_uri origin/scheme.
pub fn csp(form_action: &[String]) -> String {
    let mut fa = String::from("'self'");
    for s in form_action {
        fa.push(' ');
        fa.push_str(s);
    }
    format!(
        "default-src 'none'; style-src 'sha256-{}'; font-src 'self'; img-src 'self' data:; form-action {fa}; frame-ancestors 'none'; base-uri 'none'",
        *STYLE_HASH
    )
}

/// Auto-submit for the form_post response page. Submits once; the guard
/// keeps a page restored from history from posting the code again.
const AUTO_SUBMIT: &str = "var f=document.forms[0],done=false;\
f.addEventListener('submit',function(e){if(done){e.preventDefault()}done=true});\
setTimeout(function(){if(!done){done=true;f.submit()}},1);";

static AUTO_SUBMIT_HASH: LazyLock<String> =
    LazyLock::new(|| base64::engine::general_purpose::STANDARD.encode(sha256(AUTO_SUBMIT)));

/// CSP for the form_post response page: [`csp`] plus the auto-submit
/// script by hash (nothing else may run).
pub fn csp_form_post(form_action: &[String]) -> String {
    format!(
        "{}; script-src 'sha256-{}'",
        csp(form_action),
        *AUTO_SUBMIT_HASH
    )
}

/// `response_mode=form_post` (OAuth 2.0 Form Post Response Mode): the
/// authorization response as hidden fields posted to the redirect URI. The
/// button is the no-script fallback.
pub fn form_post(redirect_uri: &str, params: &[(String, String)]) -> String {
    let mut fields = String::new();
    for (k, v) in params {
        fields.push_str(&format!(
            "<input type=\"hidden\" name=\"{}\" value=\"{}\">",
            e(k),
            e(v)
        ));
    }
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
<meta name=\"referrer\" content=\"no-referrer\"><title>Returning to the app</title><style>{STYLE}</style></head><body><main>\
<div class=\"brand\">{MARK}vlpds<span>account security</span></div><div class=\"card\"><div class=\"strata\"></div>\
<h1>Returning to the app</h1><form method=\"post\" action=\"{}\">{fields}<div class=\"row\">\
<button type=\"submit\" class=\"primary\">Continue</button></div></form></div></main><script>{AUTO_SUBMIT}</script></body></html>",
        e(redirect_uri)
    )
}

/// The vlpds mark: three strata (log segments settling into state).
const MARK: &str = "<svg width=\"22\" height=\"18\" viewBox=\"0 0 22 18\" aria-hidden=\"true\">\
<rect x=\"0\" y=\"0\" width=\"22\" height=\"4\" rx=\"1\" fill=\"currentColor\"/>\
<rect x=\"3\" y=\"7\" width=\"16\" height=\"4\" rx=\"1\" fill=\"currentColor\" opacity=\".6\"/>\
<rect x=\"6\" y=\"14\" width=\"10\" height=\"4\" rx=\"1\" fill=\"currentColor\" opacity=\".35\"/></svg>";

pub fn page(title: &str, body: &str) -> String {
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
<meta name=\"referrer\" content=\"no-referrer\"><title>{}</title><style>{STYLE}</style></head><body><main>\
<div class=\"brand\">{MARK}vlpds<span>account security</span></div>\
<div class=\"card\"><div class=\"strata\"></div>{body}</div></main></body></html>",
        e(title)
    )
}

/// Hidden fields every authorization form carries.
pub struct Ctx<'a> {
    pub request_uri: &'a str,
    pub csrf: &'a str,
    pub client_id: &'a str,
    pub loopback: bool,
    pub server_name: &'a str,
}

fn hidden(ctx: &Ctx) -> String {
    format!(
        "<input type=\"hidden\" name=\"request_uri\" value=\"{}\"><input type=\"hidden\" name=\"csrf\" value=\"{}\">",
        e(ctx.request_uri),
        e(ctx.csrf)
    )
}

fn client_block(ctx: &Ctx) -> String {
    if ctx.loopback {
        "<div class=\"client\">A development app running on your own computer (http://localhost)</div>".into()
    } else {
        format!("<div class=\"client\">{}</div>", e(ctx.client_id))
    }
}

pub struct LoginForm<'a> {
    pub action: &'a str,
    pub identifier: &'a str,
    pub error: Option<&'a str>,
    /// Password accepted; ask for the second-factor code.
    pub totp: bool,
    /// With `totp`: the code was emailed to this (obfuscated) address
    /// rather than coming from an authenticator app.
    pub email_hint: Option<&'a str>,
}

pub fn login(ctx: Option<&Ctx>, f: &LoginForm, csrf_only: &str) -> String {
    let mut b = String::new();
    match ctx {
        Some(c) => {
            b.push_str(&format!(
                "<h1>Sign in to {}</h1><p class=\"muted\">to continue to</p>{}",
                e(c.server_name),
                client_block(c)
            ));
        }
        None => b.push_str(
            "<h1>Sign in</h1><p class=\"muted\">Manage the apps connected to your account.</p>",
        ),
    }
    if let Some(err) = f.error {
        b.push_str(&format!("<p class=\"err\" role=\"alert\">{}</p>", e(err)));
    }
    b.push_str(&format!(
        "<form method=\"post\" action=\"{}\">",
        e(f.action)
    ));
    match ctx {
        Some(c) => b.push_str(&hidden(c)),
        None => b.push_str(&format!(
            "<input type=\"hidden\" name=\"csrf\" value=\"{}\">",
            e(csrf_only)
        )),
    }
    if let (true, Some(hint)) = (f.totp, f.email_hint) {
        b.push_str(&format!(
            "<p>Two-factor authentication is enabled for <b>{}</b>. We sent a sign-in code to <b>{}</b>.</p>\
<label for=\"code\">Sign-in code from your email</label>\
<input type=\"text\" id=\"code\" name=\"code\" autocomplete=\"one-time-code\" autocapitalize=\"characters\" spellcheck=\"false\" required autofocus>\
<input type=\"hidden\" name=\"step\" value=\"totp\">",
            e(f.identifier),
            e(hint)
        ));
    } else if f.totp {
        b.push_str(&format!(
            "<p>Two-factor authentication is enabled for <b>{}</b>.</p>\
<label for=\"code\">Authenticator code (or a recovery code)</label>\
<input type=\"text\" id=\"code\" name=\"code\" inputmode=\"numeric\" autocomplete=\"one-time-code\" required autofocus>\
<input type=\"hidden\" name=\"step\" value=\"totp\">",
            e(f.identifier)
        ));
    } else {
        b.push_str(&format!(
            "<label for=\"identifier\">Handle or DID</label>\
<input type=\"text\" id=\"identifier\" name=\"identifier\" value=\"{}\" autocomplete=\"username\" autocapitalize=\"none\" spellcheck=\"false\" required{}>\
<label for=\"password\">Password</label>\
<input type=\"password\" id=\"password\" name=\"password\" autocomplete=\"current-password\" required{}>",
            e(f.identifier),
            if f.identifier.is_empty() { " autofocus" } else { "" },
            if f.identifier.is_empty() { "" } else { " autofocus" }
        ));
    }
    b.push_str("<div class=\"row\">");
    if ctx.is_some() {
        b.push_str(
            "<button type=\"submit\" name=\"action\" value=\"deny\" formnovalidate>Cancel</button>",
        );
    }
    b.push_str("<button type=\"submit\" class=\"primary\" name=\"action\" value=\"sign-in\">Sign in</button></div></form>");
    if let (Some(c), false) = (ctx, f.totp) {
        b.push_str(&format!(
            "<p class=\"alt\">New to {}? <a href=\"{}\">Create an account</a></p>",
            e(c.server_name),
            e(&screen_url(c, "sign-up"))
        ));
    }
    page("Sign in", &b)
}

/// The authorization page of this request showing `screen` ("sign-in" or
/// "sign-up").
fn screen_url(c: &Ctx, screen: &str) -> String {
    format!(
        "/oauth/authorize?client_id={}&request_uri={}&screen={screen}",
        super::util::encode_uri_component(c.client_id),
        super::util::encode_uri_component(c.request_uri)
    )
}

pub struct SignupForm<'a> {
    /// The handle's first label (the account gets `{handle}.{domain}`).
    pub handle: &'a str,
    pub domain: &'a str,
    pub email: &'a str,
    pub invite_code: &'a str,
    pub invite_required: bool,
    pub error: Option<&'a str>,
}

/// Account creation inside the authorization flow (prompt=create, or "Create
/// an account" on the sign-in page). Posts to `/oauth/authorize/sign-up`;
/// the new account is signed in on the device and continues to consent.
pub fn signup(ctx: &Ctx, f: &SignupForm) -> String {
    let mut b = format!(
        "<h1>Create an account on {}</h1><p class=\"muted\">to continue to</p>{}",
        e(ctx.server_name),
        client_block(ctx)
    );
    if let Some(err) = f.error {
        b.push_str(&format!("<p class=\"err\" role=\"alert\">{}</p>", e(err)));
    }
    b.push_str("<form method=\"post\" action=\"/oauth/authorize/sign-up\">");
    b.push_str(&hidden(ctx));
    b.push_str(&format!(
        "<label for=\"handle\">Handle</label>\
<div class=\"affix\"><input type=\"text\" id=\"handle\" name=\"handle\" value=\"{}\" autocomplete=\"username\" autocapitalize=\"none\" spellcheck=\"false\" minlength=\"3\" maxlength=\"18\" required autofocus><span>.{}</span></div>\
<p class=\"hint\">3 to 18 letters, digits or hyphens. You can switch to your own domain later.</p>\
<label for=\"email\">Email</label>\
<input type=\"email\" id=\"email\" name=\"email\" value=\"{}\" autocomplete=\"email\" required>\
<label for=\"password\">Password</label>\
<input type=\"password\" id=\"password\" name=\"password\" autocomplete=\"new-password\" minlength=\"8\" maxlength=\"256\" required>",
        e(f.handle),
        e(f.domain),
        e(f.email)
    ));
    if f.invite_required {
        b.push_str(&format!(
            "<label for=\"invite\">Invite code</label>\
<input type=\"text\" id=\"invite\" name=\"invite_code\" value=\"{}\" autocapitalize=\"none\" spellcheck=\"false\" required>",
            e(f.invite_code)
        ));
    }
    b.push_str(
        "<div class=\"row\"><button type=\"submit\" name=\"action\" value=\"deny\" formnovalidate>Cancel</button>\
<button type=\"submit\" class=\"primary\" name=\"action\" value=\"sign-up\">Create account</button></div></form>",
    );
    b.push_str(&format!(
        "<p class=\"alt\">Already have an account? <a href=\"{}\">Sign in</a></p>",
        e(&screen_url(ctx, "sign-in"))
    ));
    page("Create an account", &b)
}

pub fn chooser(ctx: &Ctx, accounts: &[(String, String)]) -> String {
    let mut b = format!(
        "<h1>Choose an account</h1><p class=\"muted\">to continue to</p>{}",
        client_block(ctx)
    );
    for (did, handle) in accounts {
        b.push_str(&format!(
            "<div class=\"acct\"><div class=\"who\"><b>@{}</b><div class=\"muted\">{}</div></div>\
<form method=\"post\" action=\"/oauth/authorize/select\">{}<input type=\"hidden\" name=\"did\" value=\"{}\"><button type=\"submit\" class=\"primary\">Continue</button></form></div>",
            e(handle),
            e(did),
            hidden(ctx),
            e(did)
        ));
    }
    b.push_str(&format!(
        "<div class=\"row\"><form method=\"post\" action=\"/oauth/authorize/select\">{}<input type=\"hidden\" name=\"did\" value=\"\"><button type=\"submit\">Use another account</button></form>\
<form method=\"post\" action=\"/oauth/authorize/consent\">{}<button type=\"submit\" name=\"action\" value=\"deny\">Cancel</button></form></div>",
        hidden(ctx),
        hidden(ctx)
    ));
    page("Choose an account", &b)
}

/// One plain-language line per requested permission.
pub fn describe_scopes(scope: &str, sets: &[(IncludeScope, J)]) -> Vec<String> {
    let mut out = Vec::new();
    for s in scope.split(' ').filter(|s| !s.is_empty()) {
        match s {
            "atproto" => out.push("Know who you are: your account identifier (DID) and handle".into()),
            "transition:generic" => out.push(
                "Full access to your account: create, change and delete any of your public data, upload media, and use other services on your behalf (except private messages)".into(),
            ),
            "transition:chat.bsky" => out.push("Read and send your private messages (Bluesky chat)".into()),
            "transition:email" => out.push("See your email address".into()),
            _ => {
                if let Some(p) = Permission::parse(s) {
                    out.push(describe_permission(&p));
                } else if let Some(inc) = IncludeScope::parse(s) {
                    let set = sets.iter().find(|(i, _)| i == &inc).map(|(_, j)| j);
                    let title = set.and_then(|j| j.get("title")).and_then(|t| t.as_str()).unwrap_or(&inc.nsid);
                    let mut line = format!("{} <span class=\"muted\">({})</span>", e(title), e(&inc.nsid));
                    if let Some(d) = set.and_then(|j| j.get("detail")).and_then(|t| t.as_str()) {
                        line.push_str(&format!("<br><span class=\"muted\">{}</span>", e(d)));
                    }
                    if let Some(set) = set {
                        let inner: Vec<String> = inc.to_permissions(set).iter().map(describe_permission).collect();
                        if !inner.is_empty() {
                            line.push_str("<ul class=\"perms\">");
                            for i in inner {
                                line.push_str(&format!("<li>{i}</li>"));
                            }
                            line.push_str("</ul>");
                        }
                    }
                    // already escaped above; mark with a sentinel prefix
                    out.push(format!("\u{0}{line}"));
                }
            }
        }
    }
    out
}

fn list(v: &[String], any: &str) -> String {
    if v.iter().any(|x| x == "*") {
        any.to_string()
    } else {
        v.join(", ")
    }
}

pub fn describe_permission(p: &Permission) -> String {
    match p {
        Permission::Repo { collection, action } => {
            let verbs: Vec<&str> = action
                .iter()
                .map(|a| match a.as_str() {
                    "create" => "create",
                    "update" => "update",
                    _ => "delete",
                })
                .collect();
            format!(
                "{} records in {}",
                capitalize(&verbs.join(", ")),
                list(collection, "any collection")
            )
        }
        Permission::Rpc { aud, lxm } => {
            let svc = if aud == "*" {
                "any service".to_string()
            } else {
                aud.clone()
            };
            format!(
                "Make requests on your behalf to {} ({})",
                svc,
                list(lxm, "any method")
            )
        }
        Permission::Blob { accept } => format!(
            "Upload files ({})",
            list(accept, "any type").replace("*/*", "any type")
        ),
        Permission::Account { attr, action } => {
            let manage = action.iter().any(|a| a == "manage");
            let what = match attr.as_str() {
                "email" => "your email address",
                "repo" => "your repository (import/export)",
                _ => "your account status (activate/deactivate)",
            };
            format!("{} {what}", if manage { "Read and change" } else { "Read" })
        }
        Permission::Identity { attr } => {
            if attr == "handle" {
                "Change your handle".into()
            } else {
                "Manage your identity (handle and DID document)".into()
            }
        }
    }
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

/// `email_choice`: offer to withhold the email address (the requested scope
/// has an `account:email` read scope and no `transition:` scope, as in the
/// reference consent form).
pub fn consent(ctx: &Ctx, did: &str, handle: &str, perms: &[String], email_choice: bool) -> String {
    let mut b = format!(
        "<h1>Authorize access</h1><p class=\"muted\">Signed in as <b>@{}</b></p><p>This app wants access to your account:</p>{}<p>It will be able to:</p><ul class=\"perms\">",
        e(handle),
        client_block(ctx)
    );
    for p in perms {
        match p.strip_prefix('\u{0}') {
            Some(html) => b.push_str(&format!("<li>{html}</li>")),
            None => b.push_str(&format!("<li>{}</li>", e(p))),
        }
    }
    b.push_str("</ul><p class=\"muted\">You can revoke this access at any time under <b>Connected apps</b> in your account settings on this server.</p>");
    b.push_str(&format!(
        "<form method=\"post\" action=\"/oauth/authorize/consent\">{}<input type=\"hidden\" name=\"did\" value=\"{}\"><div class=\"row\">\
<button type=\"submit\" name=\"action\" value=\"deny\">Deny</button><button type=\"submit\" class=\"primary\" name=\"action\" value=\"allow\" autofocus>Allow</button></div></form>",
        hidden(ctx),
        e(did)
    ));
    if email_choice {
        // the checkbox goes inside the form, before the buttons
        let at = b.rfind("<div class=\"row\">").expect("consent buttons");
        b.insert_str(
            at,
            "<input type=\"hidden\" name=\"email_choice\" value=\"1\">\
<label class=\"check\"><input type=\"checkbox\" name=\"allow_email\" value=\"1\" checked>Share my email address with this app</label>",
        );
    }
    page("Authorize access", &b)
}

pub fn error(title: &str, msg: &str) -> String {
    page(title, &format!("<h1>{}</h1><p>{}</p>", e(title), e(msg)))
}

pub struct SessionRow {
    pub id: String,
    pub client_id: String,
    pub scope: String,
    pub created_at: String,
    pub updated_at: String,
}

pub fn account_page(csrf: &str, accounts: &[(String, String, Vec<SessionRow>)]) -> String {
    let mut b = String::from("<h1>Connected apps</h1><p class=\"muted\">Apps you have authorized with OAuth. Revoking access signs the app out immediately.</p>");
    for (did, handle, sessions) in accounts {
        b.push_str(&format!(
            "<div class=\"acct\"><div class=\"who\"><b>@{}</b><div class=\"muted\">{}</div></div>\
<form method=\"post\" action=\"/oauth/account/sign-out\"><input type=\"hidden\" name=\"csrf\" value=\"{}\"><input type=\"hidden\" name=\"did\" value=\"{}\"><button type=\"submit\">Sign out</button></form></div>",
            e(handle),
            e(did),
            e(csrf),
            e(did)
        ));
        if sessions.is_empty() {
            b.push_str("<p class=\"muted\">No connected apps.</p>");
            continue;
        }
        b.push_str("<table>");
        for s in sessions {
            b.push_str(&format!(
                "<tr><td><div class=\"client\">{}</div><div class=\"muted\">{}</div><div class=\"muted\">authorized {} &middot; last used {}</div></td>\
<td><form method=\"post\" action=\"/oauth/account/revoke\"><input type=\"hidden\" name=\"csrf\" value=\"{}\"><input type=\"hidden\" name=\"did\" value=\"{}\"><input type=\"hidden\" name=\"session\" value=\"{}\"><button type=\"submit\" class=\"danger\">Revoke</button></form></td></tr>",
                e(&s.client_id),
                e(&s.scope),
                e(&s.created_at),
                e(&s.updated_at),
                e(csrf),
                e(did),
                e(&s.id)
            ));
        }
        b.push_str("</table>");
    }
    b.push_str("<div class=\"row\"><a href=\"/oauth/account?add=1\">Add another account</a></div><footer class=\"muted\"><a href=\"/account\">Open account settings</a></footer>");
    page("Connected apps", &b)
}
