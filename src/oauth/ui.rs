//! Server-rendered pages for the authorization flow and session management.
//! No external assets: one inline stylesheet allowed by hash in the
//! Content-Security-Policy. Two fixed scripts, each allowed by hash and
//! only on its pages: the auto-submit of the `response_mode=form_post`
//! page, and the passkey ceremony on the sign-in pages ([`PASSKEY_JS`]).

use super::scopes::{IncludeScope, Permission};
use super::util::{html_escape as e, sha256};
use base64::Engine;
use serde_json::Value as J;
use std::sync::LazyLock;

const STYLE: &str = r#"
.u{position:absolute;left:-9999px;width:1px;height:1px;overflow:hidden}
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
fieldset{border:0;margin:14px 0 0;padding:0;min-width:0}
legend{padding:0;font-weight:600}
ul.scopes{list-style:none;padding:0;margin:8px 0 12px}
ul.scopes>li{display:flex;gap:11px;align-items:flex-start;padding:10px 0;border-top:1px solid var(--rule)}
ul.scopes>li>input[type=checkbox]{flex:none;width:17px;height:17px;margin:3px 0 0;accent-color:var(--accent)}
ul.scopes>li>div{min-width:0;flex:1}
ul.scopes label{display:inline;margin:0;font-size:15px;font-weight:600;cursor:pointer}
ul.scopes input:disabled+div label{cursor:default}
.req{display:inline-block;margin-left:7px;padding:0 5px;border:1px solid var(--rule);border-radius:3px;color:var(--ink2);font-size:11.5px;font-weight:600;vertical-align:1px}
code.sc{display:block;margin-top:2px;color:var(--ink2);font:12px/1.5 "JetBrains Mono",ui-monospace,Menlo,monospace;overflow-wrap:anywhere}
.sd{margin:4px 0 0;color:var(--ink2);font-size:13.5px}
.warn{margin:6px 0 0;padding:6px 9px;border-left:3px solid var(--amber);background:var(--paper);font-size:13.5px}
label.check{display:flex;gap:8px;align-items:center;font-weight:500}
.wide{width:100%}
form.pk{margin:14px 0 4px}
.sep{border-top:1px solid var(--rule);margin:18px 0 0}
.or{text-align:center;margin:14px 0 0}
details.alt-code{margin:16px 0 0}
details.alt-code summary{cursor:pointer;color:var(--accent);font-size:14px}
"#;

static STYLE_HASH: LazyLock<String> = LazyLock::new(|| base64::engine::general_purpose::STANDARD.encode(sha256(STYLE)));

/// `form_action`: extra sources. Browsers apply form-action to the redirect
/// that follows a form post, so the consent page must allow the client's
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

/// The guard keeps a page restored from history from posting the code again.
const AUTO_SUBMIT: &str = "var f=document.forms[0],done=false;\
f.addEventListener('submit',function(e){if(done){e.preventDefault()}done=true});\
setTimeout(function(){if(!done){done=true;f.submit()}},1);";

static AUTO_SUBMIT_HASH: LazyLock<String> =
    LazyLock::new(|| base64::engine::general_purpose::STANDARD.encode(sha256(AUTO_SUBMIT)));

pub fn csp_form_post(form_action: &[String]) -> String {
    format!("{}; script-src 'sha256-{}'", csp(form_action), *AUTO_SUBMIT_HASH)
}

/// `response_mode=form_post`. The button is the no-script fallback.
pub fn form_post(redirect_uri: &str, params: &[(String, String)]) -> String {
    let mut fields = String::new();
    for (k, v) in params {
        fields.push_str(&format!("<input type=\"hidden\" name=\"{}\" value=\"{}\">", e(k), e(v)));
    }
    let body = format!(
        "<h1>Returning to the app</h1><form method=\"post\" action=\"{}\">{fields}<div class=\"row\">\
<button type=\"submit\" class=\"primary\">Continue</button></div></form>",
        e(redirect_uri)
    );
    page_with_script("Returning to the app", &body, AUTO_SUBMIT)
}

/// Three strata: log segments settling into state.
const MARK: &str = "<svg width=\"22\" height=\"18\" viewBox=\"0 0 22 18\" aria-hidden=\"true\">\
<rect x=\"0\" y=\"0\" width=\"22\" height=\"4\" rx=\"1\" fill=\"currentColor\"/>\
<rect x=\"3\" y=\"7\" width=\"16\" height=\"4\" rx=\"1\" fill=\"currentColor\" opacity=\".6\"/>\
<rect x=\"6\" y=\"14\" width=\"10\" height=\"4\" rx=\"1\" fill=\"currentColor\" opacity=\".35\"/></svg>";

pub fn page(title: &str, body: &str) -> String {
    page_with_script(title, body, "")
}

fn page_with_script(title: &str, body: &str, script: &str) -> String {
    let script = if script.is_empty() { String::new() } else { format!("<script>{script}</script>") };
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
<meta name=\"referrer\" content=\"no-referrer\"><title>{}</title><style>{STYLE}</style></head><body><main>\
<div class=\"brand\">{MARK}vlpds<span>account security</span></div>\
<div class=\"card\"><div class=\"strata\"></div>{body}</div>\
<footer class=\"muted\"><a href=\"https://github.com/jazware/vlpds\">Source on GitHub</a></footer></main>{script}</body></html>",
        e(title)
    )
}

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
    /// Password accepted: the second-factor step.
    pub second: Option<Second<'a>>,
    /// The password step: also offer a passkey instead of the password.
    pub passkey: Option<PasskeyUi<'a>>,
}

/// What the second-factor step offers.
pub struct Second<'a> {
    /// The code was emailed to this (obfuscated) address: the only factor.
    pub email_hint: Option<&'a str>,
    pub totp: bool,
    /// Offer to trust this browser for this many days (0: don't).
    pub trust_days: u32,
    /// The account's passkeys.
    pub passkey: Option<PasskeyUi<'a>>,
    /// The account has passkeys, but every one is flagged as copied.
    pub flagged: bool,
}

/// What [`PASSKEY_JS`] reads from the passkey form's `data-*` attributes.
pub struct PasskeyUi<'a> {
    /// base64url
    pub challenge: &'a str,
    pub rp_id: &'a str,
    /// `allowCredentials` as JSON (empty for passwordless).
    pub allow: &'a str,
}

/// The passkey ceremony on the sign-in and second-factor pages. Fixed bytes
/// (allowed by hash, like the style), reading everything from the `#pk`
/// form's data attributes, posting the assertion as hidden fields: no
/// fetch, so no connect-src. Conditional UI (autofill) on the password step.
pub const PASSKEY_JS: &str = "(function(){var f=document.getElementById('pk');if(!f||!window.PublicKeyCredential)return;\
var d=f.dataset,E=f.elements,ctl;\
function b(s){var r=atob(s.replace(/-/g,'+').replace(/_/g,'/')),u=new Uint8Array(r.length);for(var i=0;i<r.length;i++)u[i]=r.charCodeAt(i);return u}\
function e(a){var u=new Uint8Array(a),s='';for(var i=0;i<u.length;i++)s+=String.fromCharCode(u[i]);return btoa(s).replace(/\\+/g,'-').replace(/\\//g,'_').replace(/=+$/,'')}\
var o={challenge:b(d.challenge),rpId:d.rp,timeout:300000,userVerification:d.uv,\
allowCredentials:JSON.parse(d.allow||'[]').map(function(c){return{type:'public-key',id:b(c.id),transports:c.transports}})};\
function go(m){if(ctl)ctl.abort();ctl=new AbortController();var q={publicKey:o,signal:ctl.signal};if(m)q.mediation=m;\
navigator.credentials.get(q).then(function(c){var r=c.response,t=document.querySelector('input[type=checkbox][name=trust]');\
E.passkey_id.value=e(c.rawId);E.client_data.value=e(r.clientDataJSON);E.auth_data.value=e(r.authenticatorData);\
E.signature.value=e(r.signature);E.user_handle.value=r.userHandle?e(r.userHandle):'';\
if(t&&t.checked&&E.trust)E.trust.value='1';f.submit()},\
function(x){if(!m&&x&&x.name!=='AbortError'){var p=document.getElementById('pk-err');if(p)p.hidden=false}})}\
var g=document.getElementById('pk-go');if(g){g.hidden=false;g.addEventListener('click',function(){go()})}\
if(d.mode==='signin'&&PublicKeyCredential.isConditionalMediationAvailable)\
PublicKeyCredential.isConditionalMediationAvailable().then(function(a){if(a)go('conditional')})})();";

static PASSKEY_JS_HASH: LazyLock<String> =
    LazyLock::new(|| base64::engine::general_purpose::STANDARD.encode(sha256(PASSKEY_JS)));

/// [`csp`] plus the passkey script, on the sign-in and second-factor pages.
pub fn csp_passkey(form_action: &[String]) -> String {
    format!("{}; script-src 'sha256-{}'", csp(form_action), *PASSKEY_JS_HASH)
}

/// The second-factor step's "trust this browser" choice, whatever the factor.
fn trust_choice(days: u32) -> String {
    if days == 0 {
        return String::new();
    }
    let n = if days == 1 { "1 day".to_string() } else { format!("{days} days") };
    format!(
        "<label class=\"check\"><input type=\"checkbox\" name=\"trust\" value=\"1\">Trust this browser for {n}</label>\
<p class=\"muted\">You won't be asked for a code on this browser until then. Leave it unticked on a shared computer.</p>"
    )
}

/// The hidden form the script fills and posts; its button stays hidden
/// without the script.
fn passkey_form(hidden: &str, action: &str, pk: &PasskeyUi, second: bool) -> String {
    let (mode, step, uv, label, err) = if second {
        ("2fa", "2fa", "preferred", "Use your passkey", "That passkey wasn't used. Try again, or use a code.")
    } else {
        (
            "signin",
            "passkey",
            "required",
            "Sign in with a passkey",
            "No passkey was used. Try again, or use your password.",
        )
    };
    format!(
        "<form method=\"post\" action=\"{}\" id=\"pk\" class=\"pk\" data-mode=\"{mode}\" data-challenge=\"{}\" data-rp=\"{}\" data-uv=\"{uv}\" data-allow=\"{}\">{hidden}\
<input type=\"hidden\" name=\"step\" value=\"{step}\"><input type=\"hidden\" name=\"action\" value=\"sign-in\">\
<input type=\"hidden\" name=\"passkey_id\"><input type=\"hidden\" name=\"client_data\"><input type=\"hidden\" name=\"auth_data\">\
<input type=\"hidden\" name=\"signature\"><input type=\"hidden\" name=\"user_handle\"><input type=\"hidden\" name=\"trust\">\
<button type=\"button\" id=\"pk-go\" class=\"{}\" hidden>{label}</button>\
<p class=\"err\" id=\"pk-err\" role=\"alert\" hidden>{err}</p></form>",
        e(action),
        e(pk.challenge),
        e(pk.rp_id),
        e(pk.allow),
        if second { "primary wide" } else { "wide" },
    )
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
        None => b.push_str("<h1>Sign in</h1><p class=\"muted\">Manage the apps connected to your account.</p>"),
    }
    if let Some(err) = f.error {
        b.push_str(&format!("<p class=\"err\" role=\"alert\">{}</p>", e(err)));
    }
    let hidden_fields = match ctx {
        Some(c) => hidden(c),
        None => format!("<input type=\"hidden\" name=\"csrf\" value=\"{}\">", e(csrf_only)),
    };
    let username = format!(
        "<input type=\"text\" class=\"u\" autocomplete=\"username\" value=\"{}\" readonly tabindex=\"-1\" aria-hidden=\"true\">",
        e(f.identifier)
    );
    let mut script = false;
    // a passkey-only second factor: the code form is only for recovery codes
    let mut recovery_only = false;
    if let Some(s) = &f.second {
        b.push_str(&format!("<p>Two-factor authentication is enabled for <b>{}</b>.</p>", e(f.identifier)));
        recovery_only = !s.totp && s.email_hint.is_none() && (s.passkey.is_some() || s.flagged);
        if s.flagged {
            b.push_str(
                "<p class=\"warn\">Your passkey was refused because it may have been copied. Sign in with a recovery code, then remove it on the Security page.</p>",
            );
        }
        if let Some(pk) = &s.passkey {
            script = true;
            b.push_str(&passkey_form(&hidden_fields, f.action, pk, true));
            b.push_str("<noscript><p class=\"muted\">Turn on JavaScript to use your passkey.</p></noscript>");
        }
    }
    b.push_str(&format!("<form method=\"post\" action=\"{}\">", e(f.action)));
    b.push_str(&hidden_fields);
    match &f.second {
        Some(Second { email_hint: Some(hint), trust_days, .. }) => b.push_str(&format!(
            "<p>We sent a sign-in code to <b>{}</b>.</p>{username}\
<label for=\"code\">Sign-in code from your email</label>\
<input type=\"text\" id=\"code\" name=\"code\" autocomplete=\"one-time-code\" autocapitalize=\"characters\" spellcheck=\"false\" required autofocus>\
{}<input type=\"hidden\" name=\"step\" value=\"2fa\">",
            e(hint),
            trust_choice(*trust_days)
        )),
        Some(s) if recovery_only => b.push_str(&format!(
            "{username}{}<input type=\"hidden\" name=\"step\" value=\"2fa\">\
<details class=\"alt-code\"{}><summary>Use a recovery code instead</summary>\
<label for=\"code\">Recovery code</label>\
<input type=\"text\" id=\"code\" name=\"code\" autocomplete=\"one-time-code\" autocapitalize=\"none\" spellcheck=\"false\" required>\
<div class=\"row\"><button type=\"submit\" class=\"primary\" name=\"action\" value=\"sign-in\">Sign in</button></div></details>",
            trust_choice(s.trust_days),
            if s.passkey.is_none() { " open" } else { "" }
        )),
        Some(s) => b.push_str(&format!(
            "{}{username}\
<label for=\"code\">Authenticator code (or a recovery code)</label>\
<input type=\"text\" id=\"code\" name=\"code\" inputmode=\"numeric\" autocomplete=\"one-time-code\" required{}>\
{}<input type=\"hidden\" name=\"step\" value=\"2fa\">",
            if s.passkey.is_some() { "<p class=\"muted or\">or enter a code</p>" } else { "" },
            if s.passkey.is_some() { "" } else { " autofocus" },
            trust_choice(s.trust_days)
        )),
        None => b.push_str(&format!(
            "<label for=\"identifier\">Handle or DID</label>\
<input type=\"text\" id=\"identifier\" name=\"identifier\" value=\"{}\" autocomplete=\"username{}\" autocapitalize=\"none\" spellcheck=\"false\" required{}>\
<label for=\"password\">Password</label>\
<input type=\"password\" id=\"password\" name=\"password\" autocomplete=\"current-password\" required{}>",
            e(f.identifier),
            if f.passkey.is_some() { " webauthn" } else { "" },
            if f.identifier.is_empty() { " autofocus" } else { "" },
            if f.identifier.is_empty() { "" } else { " autofocus" }
        )),
    }
    b.push_str("<div class=\"row\">");
    if ctx.is_some() {
        b.push_str("<button type=\"submit\" name=\"action\" value=\"deny\" formnovalidate>Cancel</button>");
    }
    if !recovery_only {
        b.push_str("<button type=\"submit\" class=\"primary\" name=\"action\" value=\"sign-in\">Sign in</button>");
    }
    b.push_str("</div></form>");
    if let (None, Some(pk)) = (&f.second, &f.passkey) {
        script = true;
        b.push_str("<div class=\"sep\" aria-hidden=\"true\"></div>");
        b.push_str(&passkey_form(&hidden_fields, f.action, pk, false));
    }
    if let (Some(c), None) = (ctx, &f.second) {
        b.push_str(&format!(
            "<p class=\"alt\">New to {}? <a href=\"{}\">Create an account</a></p>",
            e(c.server_name),
            e(&screen_url(c, "sign-up"))
        ));
    }
    page_with_script("Sign in", &b, if script { PASSKEY_JS } else { "" })
}

fn screen_url(c: &Ctx, screen: &str) -> String {
    format!(
        "/oauth/authorize?client_id={}&request_uri={}&screen={screen}",
        super::util::encode_uri_component(c.client_id),
        super::util::encode_uri_component(c.request_uri)
    )
}

pub struct SignupForm<'a> {
    /// The first label.
    pub handle: &'a str,
    pub domain: &'a str,
    pub email: &'a str,
    pub invite_code: &'a str,
    pub invite_required: bool,
    pub error: Option<&'a str>,
}

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
    let mut b = format!("<h1>Choose an account</h1><p class=\"muted\">to continue to</p>{}", client_block(ctx));
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

/// One requested scope on the consent page. `title` and `detail` are
/// escaped HTML.
pub struct ScopeRow {
    pub scope: String,
    pub title: String,
    pub detail: String,
    pub required: bool,
    pub warning: Option<&'static str>,
}

/// atproto has no client-declared "required" scopes; only `atproto` itself,
/// without which the token grants nothing.
pub const REQUIRED_SCOPES: [&str; 1] = ["atproto"];

const GENERIC_WARNING: &str = "A broad grant: it covers everything except private messages and account settings, \
and it can't be narrowed. Untick it to refuse it entirely.";
const CHAT_WARNING: &str =
    "A broad grant covering all of your private messages. It only works together with full access to your account.";

/// One row per requested scope, in request order.
pub fn describe_scopes(scope: &str, sets: &[(IncludeScope, J)]) -> Vec<ScopeRow> {
    let mut out = Vec::new();
    for s in scope.split(' ').filter(|s| !s.is_empty()) {
        let row = |title: &str, warning| ScopeRow {
            scope: s.to_string(),
            title: e(title),
            detail: String::new(),
            required: REQUIRED_SCOPES.contains(&s),
            warning,
        };
        match s {
            "atproto" => out.push(row("Know who you are: your account identifier (DID) and handle", None)),
            "transition:generic" => out.push(row(
                "Full access to your account: create, change and delete any of your public data, upload media, and use other services on your behalf",
                Some(GENERIC_WARNING),
            )),
            "transition:chat.bsky" => out.push(row("Read and send your Bluesky private messages", Some(CHAT_WARNING))),
            "transition:email" => out.push(row("Read your email address", None)),
            _ => {
                if let Some(p) = Permission::parse(s) {
                    out.push(row(&describe_permission(&p), None));
                } else if let Some(inc) = IncludeScope::parse(s) {
                    let set = sets.iter().find(|(i, _)| i == &inc).map(|(_, j)| j);
                    let title = set.and_then(|j| j.get("title")).and_then(|t| t.as_str()).unwrap_or(&inc.nsid);
                    let mut r = row(title, None);
                    if let Some(d) = set.and_then(|j| j.get("detail")).and_then(|t| t.as_str()) {
                        r.detail.push_str(&format!("<p class=\"sd\">{}</p>", e(d)));
                    }
                    if let Some(set) = set {
                        let inner: Vec<String> = inc.to_permissions(set).iter().map(describe_permission).collect();
                        if !inner.is_empty() {
                            r.detail.push_str("<ul class=\"perms\">");
                            for i in inner {
                                r.detail.push_str(&format!("<li>{}</li>", e(&i)));
                            }
                            r.detail.push_str("</ul>");
                        }
                    }
                    out.push(r);
                }
            }
        }
    }
    out
}

/// "a, b and c".
fn join_and(v: &[&str]) -> String {
    match v {
        [] => String::new(),
        [one] => one.to_string(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

fn collection_name(nsid: &str) -> Option<&'static str> {
    Some(match nsid {
        "app.bsky.feed.post" => "posts",
        "app.bsky.feed.like" => "likes",
        "app.bsky.feed.repost" => "reposts",
        "app.bsky.graph.follow" => "follows",
        "app.bsky.graph.block" => "blocks",
        "app.bsky.graph.list" => "lists",
        "app.bsky.graph.listitem" => "list members",
        "app.bsky.graph.starterpack" => "starter packs",
        "app.bsky.actor.profile" => "profile",
        "app.bsky.feed.threadgate" => "reply settings",
        "app.bsky.feed.postgate" => "quote settings",
        "app.bsky.feed.generator" => "feeds",
        _ => return None,
    })
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
            let verbs = capitalize(&join_and(&verbs));
            let names: Option<Vec<&str>> = collection.iter().map(|c| collection_name(c)).collect();
            match names {
                Some(n) if !n.is_empty() => format!("{verbs} your {}", join_and(&n)),
                _ if collection.iter().any(|c| c == "*") => format!("{verbs} records in any collection"),
                _ => format!("{verbs} records in {}", collection.join(", ")),
            }
        }
        Permission::Rpc { aud, lxm } => {
            let svc = if aud == "*" { "any service".to_string() } else { aud.clone() };
            if !lxm.is_empty() && lxm.iter().all(|l| l.starts_with("chat.bsky.")) {
                if aud == "did:web:api.bsky.chat#bsky_chat" {
                    "Read and send your Bluesky private messages".to_string()
                } else {
                    format!("Read and send your Bluesky private messages (through {svc})")
                }
            } else if lxm.iter().any(|l| l == "*") {
                format!("Make any request on your behalf to {svc}")
            } else {
                format!("Make requests on your behalf to {svc} ({})", lxm.join(", "))
            }
        }
        Permission::Blob { accept } => {
            let kinds: Vec<&str> = accept
                .iter()
                .map(|a| match a.as_str() {
                    "*/*" => "files of any type",
                    "image/*" => "images",
                    "video/*" => "video",
                    "audio/*" => "audio",
                    other => other,
                })
                .collect();
            format!("Upload {}", join_and(&kinds))
        }
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

/// One checkbox per scope, all ticked, posted as repeated `scope` fields so
/// the page needs no script. Disabled inputs aren't submitted, so a hidden
/// field carries each required scope.
pub fn consent(ctx: &Ctx, did: &str, handle: &str, rows: &[ScopeRow]) -> String {
    let mut b = format!(
        "<h1>Authorize access</h1><p class=\"muted\">Signed in as <b>@{}</b></p><p>This app wants access to your account:</p>{}\
<form method=\"post\" action=\"/oauth/authorize/consent\">{}<input type=\"hidden\" name=\"did\" value=\"{}\">\
<fieldset><legend>It will be able to:</legend><p class=\"hint\">Untick anything you don't want to allow. The app might not work fully without it.</p><ul class=\"scopes\">",
        e(handle),
        client_block(ctx),
        hidden(ctx),
        e(did)
    );
    for (i, r) in rows.iter().enumerate() {
        let sc = e(&r.scope);
        let input = if r.required {
            format!("<input type=\"hidden\" name=\"scope\" value=\"{sc}\"><input type=\"checkbox\" id=\"s{i}\" checked disabled aria-describedby=\"s{i}d\">")
        } else {
            format!("<input type=\"checkbox\" id=\"s{i}\" name=\"scope\" value=\"{sc}\" checked aria-describedby=\"s{i}d\">")
        };
        let req = if r.required { "<span class=\"req\">Required</span>" } else { "" };
        let warn = r.warning.map(|w| format!("<p class=\"warn\">{}</p>", e(w))).unwrap_or_default();
        b.push_str(&format!(
            "<li>{input}<div><label for=\"s{i}\">{}{req}</label><div id=\"s{i}d\"><code class=\"sc\">{sc}</code>{warn}{}</div></div></li>",
            r.title, r.detail
        ));
    }
    b.push_str(
        "</ul></fieldset><p class=\"muted\">You can revoke this access at any time under <b>Connected apps</b> in your account settings on this server.</p>\
<div class=\"row\"><button type=\"submit\" name=\"action\" value=\"deny\">Deny</button><button type=\"submit\" class=\"primary\" name=\"action\" value=\"allow\" autofocus>Allow</button></div></form>",
    );
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
