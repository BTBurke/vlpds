//! Account email templates: HTML + plain-text alternatives (DESIGN.md
//! "Email").
//!
//! `layout.html` and each email's wording are adapted from the reference
//! PDS's mailer templates (bluesky-social/atproto,
//! packages/pds/src/mailer/templates/*.hbs; Copyright (c) 2022-2026 Bluesky
//! Social PBC, and Contributors; MIT, see `NOTICE` beside this file). The
//! six reference templates share one layout and differ only in title,
//! preheader, intro, outro and token, so vlpds keeps the layout once and
//! fills those slots. Subjects match the reference's `ServerMailer`.
//!
//! Rendering is plain `{{name}}` substitution in a single pass (a value is
//! never re-scanned for placeholders). Every value is HTML-escaped unless it
//! is one of the intro/outro fragments built here from escaped pieces;
//! handles, tokens and the operator's branding never reach the HTML raw.

use std::borrow::Cow;

const LAYOUT: &str = include_str!("layout.html");

/// The reference's defaults (`packages/pds/src/mailer/index.ts`).
pub const DEFAULT_LOGO_URL: &str = "https://bsky.social/about/images/email/email_logo_default.png";
pub const DEFAULT_MARK_URL: &str = "https://bsky.social/about/images/email/email_mark_dark.png";
pub const DEFAULT_HOME_URL: &str = "https://bsky.app";
pub const DEFAULT_PRIMARY_COLOR: &str = "#067df7";

const LINK_STYLE: &str = "color:hsl(211, 20%, 53%);text-decoration:none;text-decoration-line:underline;font-family:-apple-system, BlinkMacSystemFont, &#x27;Roboto&#x27;, &#x27;Oxygen&#x27;, &#x27;Ubuntu&#x27;, &#x27;Cantarell&#x27;, &#x27;Fira Sans&#x27;, &#x27;Droid Sans&#x27;, &#x27;Helvetica Neue&#x27;, sans-serif;margin:0px 0px;line-height:1.0;font-size:14px;letter-spacing:0.25px";
const PAD_RIGHT: &str = ";padding-right:32px";

/// Email branding, as the reference's `BrandingConfig` (PDS_SERVICE_NAME,
/// PDS_HOME_URL, PDS_LOGO_URL, PDS_PRIMARY_COLOR) plus
/// PDS_EMAIL_DISABLE_CONFIRMATION_LINK. Unset fields take the reference's
/// defaults; the name defaults to "{hostname} PDS".
#[derive(Clone, Debug, Default)]
pub struct Branding {
    pub name: Option<String>,
    pub home_url: Option<String>,
    /// Header logo and footer mark (the reference uses the one logo for both
    /// once it is set).
    pub logo_url: Option<String>,
    pub primary_color: Option<String>,
    /// Drops confirm-email's "click here" link to bsky.app/intent/verify-email.
    pub disable_confirmation_link: bool,
}

impl Branding {
    /// `--email-*` flags, each falling back to the reference PDS's variable.
    pub fn from_flags(
        name: Option<String>,
        home_url: Option<String>,
        logo_url: Option<String>,
        primary_color: Option<String>,
        disable_confirmation_link: bool,
    ) -> Branding {
        let pick = super::pick;
        let env_bool = |k: &str| {
            std::env::var(k)
                .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
                .unwrap_or(false)
        };
        Branding {
            name: pick(name, "PDS_SERVICE_NAME"),
            home_url: pick(home_url, "PDS_HOME_URL"),
            logo_url: pick(logo_url, "PDS_LOGO_URL"),
            primary_color: pick(primary_color, "PDS_PRIMARY_COLOR"),
            disable_confirmation_link: disable_confirmation_link
                || env_bool("PDS_EMAIL_DISABLE_CONFIRMATION_LINK"),
        }
    }

    /// Service name: the configured one, else "{hostname} PDS" (the
    /// reference's default) from `public_url`.
    pub fn service_name(&self, public_url: &str) -> String {
        if let Some(n) = &self.name {
            return n.clone();
        }
        let host = public_url
            .split_once("://")
            .map_or(public_url, |(_, r)| r)
            .split(['/', '?', '#'])
            .next()
            .unwrap_or_default();
        let host = host.rsplit('@').next().unwrap_or(host);
        // drop a port (an IPv6 literal keeps its brackets)
        let host = match host.find(']') {
            Some(e) if host.starts_with('[') => &host[..=e],
            _ => host.split(':').next().unwrap_or(host),
        };
        format!("{host} PDS")
    }
}

/// One account email. Constructed at the call sites in `xrpc::server` /
/// `xrpc::email2fa` and rendered by [`Email::render`].
#[derive(Clone, Copy, Debug)]
pub enum Email<'a> {
    /// requestPasswordReset.
    ResetPassword { handle: &'a str, token: &'a str },
    /// requestAccountDelete.
    DeleteAccount { token: &'a str },
    /// requestEmailConfirmation.
    ConfirmEmail { token: &'a str },
    /// requestEmailUpdate (and turning email 2FA off).
    UpdateEmail { token: &'a str },
    /// requestPlcOperationSignature.
    PlcOperation { token: &'a str },
    /// Sign-in with email 2FA on (createSession / OAuth sign-in).
    SignInAuthFactor { handle: Option<&'a str>, token: &'a str },
}

/// A rendered email.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rendered {
    pub subject: String,
    pub text: String,
    pub html: String,
}

impl Email<'_> {
    /// The `Mail::purpose` (also the email-token purpose and the metrics
    /// label).
    pub fn purpose(&self) -> &'static str {
        match self {
            Email::ResetPassword { .. } => "reset_password",
            Email::DeleteAccount { .. } => "delete_account",
            Email::ConfirmEmail { .. } => "confirm_email",
            Email::UpdateEmail { .. } => "update_email",
            Email::PlcOperation { .. } => "plc_operation",
            Email::SignInAuthFactor { .. } => "auth_factor",
        }
    }

    pub fn token(&self) -> &str {
        match *self {
            Email::ResetPassword { token, .. }
            | Email::DeleteAccount { token }
            | Email::ConfirmEmail { token }
            | Email::UpdateEmail { token }
            | Email::PlcOperation { token }
            | Email::SignInAuthFactor { token, .. } => token,
        }
    }

    /// The reference `ServerMailer`'s subjects.
    pub fn subject(&self) -> &'static str {
        match self {
            Email::ResetPassword { .. } => "Password Reset Requested",
            Email::DeleteAccount { .. } => "Account Deletion Requested",
            Email::ConfirmEmail { .. } => "Email Confirmation",
            Email::UpdateEmail { .. } => "Email Update Requested",
            Email::PlcOperation { .. } => "PLC Update Operation Requested",
            Email::SignInAuthFactor { .. } => "Sign-in Confirmation",
        }
    }

    /// Renders the HTML (the reference's layout) and its plain-text
    /// alternative. `public_url` is the PDS's own URL: the default service
    /// name's hostname and the sign-in mail's change-password link.
    pub fn render(&self, b: &Branding, public_url: &str) -> Rendered {
        let token = self.token();
        let t = esc(token);
        let color = esc(b.primary_color.as_deref().unwrap_or(DEFAULT_PRIMARY_COLOR));
        let at_handle = |h: &str| format!("<span\n                        style='color:{color}'\n                      >@<!-- -->{}<!-- -->.</span>", esc(h));
        let change_pw = format!("{}/.well-known/change-password", public_url.trim_end_matches('/'));
        let verify_link = format!("https://bsky.app/intent/verify-email?code={token}");

        // (title, preheader, intro html, intro style, outro html, outro style, intro text, outro text)
        let (title, preheader, intro, intro_style, outro, outro_style, intro_txt, outro_txt): (
            &str,
            String,
            String,
            &str,
            String,
            &str,
            String,
            String,
        ) = match *self {
            Email::ResetPassword { handle, .. } => (
                "Reset password",
                format!("We received a request to reset the password for the account @{handle}."),
                format!("We received a request to reset the password for the account<!-- -->\n                      {}", at_handle(handle)),
                "",
                "To choose a new password, please enter the code above in\n                      the app along with your new password.".into(),
                "",
                format!("We received a request to reset the password for the account @{handle}."),
                "To choose a new password, please enter the code above in the app along with your new password.".into(),
            ),
            Email::DeleteAccount { .. } => (
                "Delete your account",
                "To permanently delete your account, please enter the code provided in the app along with your password.".into(),
                "<span style='font-weight:600'>To permanently delete your\n                        account,</span>\n                      <!-- -->please enter the code below in the app along with\n                      your password.".into(),
                PAD_RIGHT,
                "👉 If you didn&#x27;t request an account deletion,<!-- -->\n                      <span style='font-weight:600'>you should update your\n                        password immediately.</span>".into(),
                PAD_RIGHT,
                "To permanently delete your account, please enter the code below in the app along with your password.".into(),
                "👉 If you didn't request an account deletion, you should update your password immediately.".into(),
            ),
            Email::ConfirmEmail { .. } => {
                let (link, link_txt) = if b.disable_confirmation_link {
                    (String::new(), String::new())
                } else {
                    (
                        format!(
                            "\n                        or<!-- -->\n                        <a\n                          href='{}'\n                          style='color:{color};text-decoration:none;text-decoration-line:underline;font-size:16px;letter-spacing:0.25px'\n                          target='_blank'\n                        >click here</a>",
                            esc(&verify_link)
                        ),
                        format!(" or open {verify_link}"),
                    )
                };
                (
                    "Confirm your email",
                    format!("{token} is your verification code."),
                    format!("To confirm this email for your account, please enter the\n                      code below in the app{link}."),
                    PAD_RIGHT,
                    "If you didn&#x27;t request an email confirmation, you can\n                      safely ignore this email.".into(),
                    "",
                    format!("To confirm this email for your account, please enter the code below in the app{link_txt}."),
                    "If you didn't request an email confirmation, you can safely ignore this email.".into(),
                )
            }
            Email::UpdateEmail { .. } => (
                "Update your email",
                "To update the email for your account, enter the code provided in the app along with your new email.".into(),
                "To update the email for your account, enter the code below\n                      in the app along with your new email.".into(),
                PAD_RIGHT,
                "If you didn&#x27;t request an email update, you can safely\n                      ignore this email.".into(),
                "",
                "To update the email for your account, enter the code below in the app along with your new email.".into(),
                "If you didn't request an email update, you can safely ignore this email.".into(),
            ),
            Email::PlcOperation { .. } => (
                "PLC update requested",
                "We received a request to update your PLC.".into(),
                "We received a request to update your PLC identity. Your\n                      confirmation code is:".into(),
                "",
                "Updating your PLC identity is a very sensitive operation.\n                      Please only proceed if you are confident in what you are\n                      doing.".into(),
                "",
                "We received a request to update your PLC identity. Your confirmation code is:".into(),
                "Updating your PLC identity is a very sensitive operation. Please only proceed if you are confident in what you are doing.".into(),
            ),
            Email::SignInAuthFactor { handle, .. } => {
                let (who, who_txt) = match handle {
                    Some(h) => (
                        format!(
                            "the account<!-- -->\n                        <span\n                          style='color:hsl(211, 99%, 53%)'\n                        >@<!-- -->{}<!-- -->.</span>",
                            esc(h)
                        ),
                        format!("the account @{h}."),
                    ),
                    None => ("your\n                        account.".into(), "your account.".into()),
                };
                (
                    "Confirm your sign-in",
                    "We received a sign in request for your account.".into(),
                    format!("We received a sign-in request for\n                      {who}\n                      <!-- -->Use the code below to sign in."),
                    PAD_RIGHT,
                    format!(
                        "If this wasn&#x27;t you, we recommend taking steps to\n                      protect your account by<!-- -->\n                      <a\n                        href='{}'\n                        style='{LINK_STYLE}'\n                        target='_blank'\n                      >changing your password.</a>",
                        esc(&change_pw)
                    ),
                    "",
                    format!("We received a sign-in request for {who_txt} Use the code below to sign in."),
                    format!("If this wasn't you, we recommend taking steps to protect your account by changing your password: {change_pw}"),
                )
            }
        };

        let name = b.service_name(public_url);
        let home = b.home_url.as_deref().unwrap_or(DEFAULT_HOME_URL);
        let logo = b.logo_url.as_deref().unwrap_or(DEFAULT_LOGO_URL);
        let mark = b.logo_url.as_deref().unwrap_or(DEFAULT_MARK_URL);
        let html = fill(LAYOUT, |k| match k {
            "title" => Some(esc(title)),
            "preheader" => Some(esc(&preheader)),
            "intro" => Some(Cow::Borrowed(intro.as_str())),
            "intro_style" => Some(Cow::Borrowed(intro_style)),
            "outro" => Some(Cow::Borrowed(outro.as_str())),
            "outro_style" => Some(Cow::Borrowed(outro_style)),
            "token" => Some(t.clone()),
            "service_name" => Some(esc(&name)),
            "home_url" => Some(esc(home)),
            "logo_url" => Some(esc(logo)),
            "mark_url" => Some(esc(mark)),
            _ => None,
        });
        let text = format!("{title}\n\n{intro_txt}\n\n{token}\n\n{outro_txt}\n\n--\n{name} ({home})\n");
        Rendered { subject: self.subject().to_string(), text, html }
    }
}

/// HTML-escapes text for element content and quoted attributes.
pub fn esc(s: &str) -> Cow<'_, str> {
    if !s.contains(['&', '<', '>', '"', '\'']) {
        return Cow::Borrowed(s);
    }
    let mut o = String::with_capacity(s.len() + 16);
    for c in s.chars() {
        match c {
            '&' => o.push_str("&amp;"),
            '<' => o.push_str("&lt;"),
            '>' => o.push_str("&gt;"),
            '"' => o.push_str("&quot;"),
            '\'' => o.push_str("&#x27;"),
            c => o.push(c),
        }
    }
    Cow::Owned(o)
}

/// Single-pass `{{name}}` substitution. Unknown names stay as they are
/// (a test asserts none are left).
fn fill<'a>(tpl: &str, val: impl Fn(&str) -> Option<Cow<'a, str>>) -> String {
    let mut out = String::with_capacity(tpl.len() + 512);
    let mut rest = tpl;
    while let Some(i) = rest.find("{{") {
        out.push_str(&rest[..i]);
        let after = &rest[i + 2..];
        match after.find("}}").and_then(|j| val(&after[..j]).map(|v| (j, v))) {
            Some((j, v)) => {
                out.push_str(&v);
                rest = &after[j + 2..];
            }
            None => {
                out.push_str("{{");
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Plain-text alternative for an HTML body (admin sendEmail content is HTML,
/// as in the reference's ModerationMailer, which runs nodemailer's
/// html-to-text). Block tags and `<br>` become line breaks, other tags are
/// dropped, the common entities are decoded and blank-line runs collapse.
pub fn html_to_text(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut rest = html;
    while let Some(i) = rest.find('<') {
        out.push_str(&rest[..i]);
        let Some(j) = rest[i..].find('>') else {
            out.push_str(&rest[i..]);
            rest = "";
            break;
        };
        let tag = rest[i + 1..i + j].trim_start_matches('/').to_ascii_lowercase();
        let name: String = tag.chars().take_while(|c| c.is_ascii_alphanumeric()).collect();
        // skip <style>/<script>/<head> contents
        if matches!(name.as_str(), "style" | "script" | "head") && !rest[i + 1..].starts_with('/') {
            let close = format!("</{name}");
            match rest[i + j..].to_ascii_lowercase().find(&close) {
                Some(k) => {
                    rest = &rest[i + j + k..];
                    continue;
                }
                None => {
                    rest = "";
                    break;
                }
            }
        }
        if matches!(name.as_str(), "br" | "p" | "div" | "tr" | "li" | "h1" | "h2" | "h3" | "h4" | "h5" | "h6" | "hr" | "table" | "blockquote" | "pre" | "ul" | "ol") {
            out.push('\n');
        }
        rest = &rest[i + j + 1..];
    }
    out.push_str(rest);
    let decoded = decode_entities(&out);
    let mut text = String::with_capacity(decoded.len());
    let mut blank = 0;
    for line in decoded.lines().map(|l| l.split_whitespace().collect::<Vec<_>>().join(" ")) {
        if line.is_empty() {
            blank += 1;
            continue;
        }
        if !text.is_empty() {
            text.push_str(if blank > 0 { "\n\n" } else { "\n" });
        }
        blank = 0;
        text.push_str(&line);
    }
    text
}

fn decode_entities(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    let mut o = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        o.push_str(&rest[..i]);
        let after = &rest[i + 1..];
        let end = after.find(';').filter(|&e| e <= 10);
        let ent = end.map(|e| &after[..e]);
        let ch = match ent {
            Some("amp") => Some('&'),
            Some("lt") => Some('<'),
            Some("gt") => Some('>'),
            Some("quot") => Some('"'),
            Some("apos") => Some('\''),
            Some("nbsp") => Some(' '),
            Some(e) if e.starts_with("#x") || e.starts_with("#X") => u32::from_str_radix(&e[2..], 16).ok().and_then(char::from_u32),
            Some(e) if e.starts_with('#') => e[1..].parse().ok().and_then(char::from_u32),
            _ => None,
        };
        match (ch, end) {
            (Some(c), Some(e)) => {
                o.push(c);
                rest = &after[e + 1..];
            }
            _ => {
                o.push('&');
                rest = after;
            }
        }
    }
    o.push_str(rest);
    o
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "https://pds.example.com";
    const TOKEN: &str = "ABCDE-12345";

    fn all(handle: &str) -> Vec<Email<'_>> {
        vec![
            Email::ResetPassword { handle, token: TOKEN },
            Email::DeleteAccount { token: TOKEN },
            Email::ConfirmEmail { token: TOKEN },
            Email::UpdateEmail { token: TOKEN },
            Email::PlcOperation { token: TOKEN },
            Email::SignInAuthFactor { handle: Some(handle), token: TOKEN },
            Email::SignInAuthFactor { handle: None, token: TOKEN },
        ]
    }

    #[test]
    fn every_template_renders() {
        let b = Branding::default();
        let subjects = [
            "Password Reset Requested",
            "Account Deletion Requested",
            "Email Confirmation",
            "Email Update Requested",
            "PLC Update Operation Requested",
            "Sign-in Confirmation",
            "Sign-in Confirmation",
        ];
        let titles = [
            "Reset password",
            "Delete your account",
            "Confirm your email",
            "Update your email",
            "PLC update requested",
            "Confirm your sign-in",
            "Confirm your sign-in",
        ];
        for ((e, subject), title) in all("alice.test").into_iter().zip(subjects).zip(titles) {
            let r = e.render(&b, URL);
            assert_eq!(r.subject, subject);
            assert!(!r.html.contains("{{"), "unfilled placeholder in {subject}: {}", r.html);
            assert!(r.html.contains(&format!("<title>{title}</title>")), "{subject}");
            assert!(r.html.contains(&format!(">{title}</h1>")), "{subject}");
            assert!(r.html.contains(&format!(">{TOKEN}</code>")), "{subject}");
            assert!(r.html.contains("alt='pds.example.com PDS'"), "{subject}");
            assert!(r.html.contains(&format!("src='{DEFAULT_LOGO_URL}'")));
            assert!(r.html.contains(&format!("src='{DEFAULT_MARK_URL}'")));
            assert!(r.html.contains(&format!("href='{DEFAULT_HOME_URL}'")));
            assert!(r.text.starts_with(title), "{}", r.text);
            assert!(r.text.contains(&format!("\n\n{TOKEN}\n\n")), "{}", r.text);
            assert!(r.text.contains("pds.example.com PDS (https://bsky.app)"), "{}", r.text);
            assert!(!r.text.contains('<'), "{}", r.text);
        }
    }

    #[test]
    fn reference_wording() {
        let b = Branding::default();
        let r = Email::ResetPassword { handle: "alice.test", token: TOKEN }.render(&b, URL);
        assert!(r.text.contains("We received a request to reset the password for the account @alice.test."));
        assert!(r.html.contains(">@<!-- -->alice.test<!-- -->.</span>"));
        assert!(r.html.contains("style='color:#067df7'"));
        let r = Email::ConfirmEmail { token: TOKEN }.render(&b, URL);
        assert!(r.html.contains("href='https://bsky.app/intent/verify-email?code=ABCDE-12345'"));
        assert!(r.html.contains("content='ABCDE-12345 is your verification code.'"));
        let r = Email::ConfirmEmail { token: TOKEN }
            .render(&Branding { disable_confirmation_link: true, ..Default::default() }, URL);
        assert!(!r.html.contains("verify-email"));
        assert!(r.text.contains("please enter the code below in the app."), "{}", r.text);
        let r = Email::SignInAuthFactor { handle: Some("alice.test"), token: TOKEN }.render(&b, URL);
        assert!(r.html.contains("href='https://pds.example.com/.well-known/change-password'"));
        assert!(r.text.contains("We received a sign-in request for the account @alice.test. Use the code below to sign in."));
        let r = Email::SignInAuthFactor { handle: None, token: TOKEN }.render(&b, URL);
        assert!(r.text.contains("We received a sign-in request for your account. Use"));
    }

    #[test]
    fn user_values_are_html_escaped() {
        let evil = "<script>alert('x')</script>&.test";
        for e in all(evil) {
            let r = e.render(&Branding::default(), URL);
            assert!(!r.html.contains("<script"), "{}: {}", r.subject, r.html);
            if matches!(e, Email::ResetPassword { .. } | Email::SignInAuthFactor { handle: Some(_), .. }) {
                assert!(r.html.contains("&lt;script&gt;alert(&#x27;x&#x27;)&lt;/script&gt;&amp;.test"), "{}", r.html);
                // the text part keeps it verbatim
                assert!(r.text.contains(evil));
            }
        }
        // branding is escaped too (attribute breakout)
        let b = Branding {
            name: Some("Evil' onerror='x".into()),
            home_url: Some("https://h/\"><b>".into()),
            logo_url: Some("x' onload='y".into()),
            primary_color: Some("red'><i>".into()),
            disable_confirmation_link: false,
        };
        let r = Email::ResetPassword { handle: "a.test", token: TOKEN }.render(&b, URL);
        assert!(r.html.contains("alt='Evil&#x27; onerror=&#x27;x'"));
        assert!(r.html.contains("href='https://h/&quot;&gt;&lt;b&gt;'"));
        assert!(r.html.contains("src='x&#x27; onload=&#x27;y'"));
        assert!(r.html.contains("style='color:red&#x27;&gt;&lt;i&gt;'"));
        assert!(!r.html.contains("<b>") && !r.html.contains("<i>"));
    }

    #[test]
    fn service_name_defaults_to_hostname() {
        let b = Branding::default();
        assert_eq!(b.service_name("https://pds.example.com"), "pds.example.com PDS");
        assert_eq!(b.service_name("http://localhost:2583/"), "localhost PDS");
        assert_eq!(b.service_name("http://[::1]:2583"), "[::1] PDS");
        let b = Branding { name: Some("Acme".into()), ..Default::default() };
        assert_eq!(b.service_name("https://x"), "Acme");
        let r = Email::DeleteAccount { token: TOKEN }.render(&b, URL);
        assert!(r.html.contains(">Acme</a>"));
    }

    #[test]
    fn html_to_text_basics() {
        let t = html_to_text("<html><head><style>p{color:red}</style></head><body><h1>Hi &amp; bye</h1><p>Line one<br>line&nbsp;two</p><p>a &lt;b&gt; &#x27;c&#39;</p></body></html>");
        assert_eq!(t, "Hi & bye\n\nLine one\nline two\n\na <b> 'c'");
        assert_eq!(html_to_text("plain text, no tags"), "plain text, no tags");
        assert_eq!(html_to_text("a & b"), "a & b");
    }
}
