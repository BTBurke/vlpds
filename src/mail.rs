//! Outbound email over SMTP (DESIGN.md "Email"). Sending never blocks the
//! request path: a full queue drops the mail and counts it. The queue is in
//! memory, so mail queued when a node stops is lost (the user asks again),
//! as with the reference PDS's in-process nodemailer. Neither this nor the
//! log-only mailer logs the token or body above debug level.

mod templates;

pub use templates::{html_to_text, Branding, Email};

use crate::xrpc::{Mail, Mailer};
use lettre::message::{header::ContentType, Mailbox, MultiPart};
use lettre::transport::smtp::client::{Certificate, Tls, TlsParameters};
use lettre::transport::smtp::PoolConfig;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use prometheus::{
    exponential_buckets, register_histogram, register_int_counter, register_int_counter_vec,
    register_int_gauge, Histogram, IntCounter, IntCounterVec, IntGauge,
};
use std::path::Path;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};

/// TCP connect, and in lettre each SMTP command's read/write.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// One whole attempt: connect, EHLO/STARTTLS/AUTH, envelope and DATA.
const SEND_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_QUEUE: usize = 1024;
const DEFAULT_CONCURRENCY: usize = 4;
/// Waits before the 2nd, 3rd and 4th attempts.
const DEFAULT_BACKOFF: [Duration; 3] = [
    Duration::from_secs(2),
    Duration::from_secs(10),
    Duration::from_secs(60),
];

macro_rules! lazy {
    ($name:ident: $t:ty = $e:expr) => {
        pub static $name: LazyLock<$t> = LazyLock::new(|| $e.unwrap());
    };
}
lazy!(MAIL_MESSAGES: IntCounterVec = register_int_counter_vec!("vlpds_mail_messages_total", "Outbound mail by result (sent; failed: permanent rejection or retries exhausted; dropped: queue full or mailer stopped) and purpose", &["result", "purpose"]));
lazy!(MAIL_SUPPRESSED: IntCounterVec = register_int_counter_vec!("vlpds_mail_suppressed_total", "Account mails not sent, by purpose and reason (recipient_limit: mail-recipient-*; node_limit: mail-node-hour; cluster_limit: mail-cluster-day; account_limit: password-reset-account-*, answered OK; dedup: an email sign-in code under a minute old is still live)", &["purpose", "reason"]));
lazy!(MAIL_RETRIES: IntCounter = register_int_counter!("vlpds_mail_retries_total", "SMTP send attempts retried after a transient failure (4xx, connection error, timeout)"));
lazy!(MAIL_QUEUE: IntGauge = register_int_gauge!("vlpds_mail_queue_depth", "Mails queued or being sent (all of this process's SMTP mailers)"));
lazy!(MAIL_SEND_SECONDS: Histogram = register_histogram!("vlpds_mail_send_seconds", "One successful SMTP send, enqueue to accepted (incl. retries)", exponential_buckets(0.01, 2.0, 14).unwrap()));

/// For `server::Config`, which is `Clone + Debug`.
#[derive(Clone)]
pub struct SharedMailer(pub Arc<dyn Mailer>);

impl std::fmt::Debug for SharedMailer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SharedMailer")
    }
}

impl std::ops::Deref for SharedMailer {
    type Target = dyn Mailer;
    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

#[derive(Clone)]
pub struct SmtpConfig {
    /// Nodemailer's URL form, as the reference's PDS_EMAIL_SMTP_URL:
    /// `smtp://` upgrades with STARTTLS when offered (`?tls=required`
    /// insists, `?tls=none` is plaintext), `smtps://` is implicit TLS. A
    /// path sets the EHLO name.
    pub url: String,
    /// `addr@host` or `Name <addr@host>`.
    pub from: String,
    pub queue: usize,
    /// Also the SMTP connection pool's size.
    pub concurrency: usize,
    pub backoff: Vec<Duration>,
    /// PEM CA certificate(s) trusted for the server's certificate on top of
    /// the webpki roots (a relay with a private CA).
    pub ca_pem: Option<Vec<u8>>,
}

/// Redacts the URL's password.
impl std::fmt::Debug for SmtpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let url = match reqwest::Url::parse(&self.url) {
            Ok(mut u) if u.password().is_some() => {
                let _ = u.set_password(Some("redacted"));
                u.to_string()
            }
            Ok(u) => u.to_string(),
            Err(_) => "<unparsable>".into(),
        };
        f.debug_struct("SmtpConfig")
            .field("url", &url)
            .field("from", &self.from)
            .field("queue", &self.queue)
            .field("concurrency", &self.concurrency)
            .field("backoff", &self.backoff)
            .field("ca_pem", &self.ca_pem.is_some())
            .finish()
    }
}

impl SmtpConfig {
    pub fn new(url: impl Into<String>, from: impl Into<String>) -> Self {
        SmtpConfig {
            url: url.into(),
            from: from.into(),
            queue: DEFAULT_QUEUE,
            concurrency: DEFAULT_CONCURRENCY,
            backoff: DEFAULT_BACKOFF.to_vec(),
            ca_pem: None,
        }
    }
}

/// None: log-only. Setting only one of the two is an error, as in the
/// reference PDS. Must run inside the tokio runtime.
pub fn from_flags(url: Option<String>, from: Option<String>, ca_file: Option<&Path>) -> anyhow::Result<Option<SharedMailer>> {
    let m = start_if_set(
        (url, "PDS_EMAIL_SMTP_URL"),
        (from, "PDS_EMAIL_FROM_ADDRESS"),
        ca_file,
        "email config: set both --email-smtp-url and --email-from-address",
    )?;
    if m.is_none() {
        tracing::info!("email disabled (no --email-smtp-url): mail is logged without its token and not sent");
    }
    Ok(m)
}

/// The reference's ModerationMailer, for admin sendEmail. None: it falls
/// back to the main mailer (DESIGN.md "Email").
pub fn moderation_from_flags(url: Option<String>, from: Option<String>, ca_file: Option<&Path>) -> anyhow::Result<Option<SharedMailer>> {
    let m = start_if_set(
        (url, "PDS_MODERATION_EMAIL_SMTP_URL"),
        (from, "PDS_MODERATION_EMAIL_ADDRESS"),
        ca_file,
        "moderation email config: set both --moderation-email-smtp-url and --moderation-email-address",
    )?;
    if m.is_some() {
        tracing::info!("moderation email (admin sendEmail) has its own SMTP mailer");
    }
    Ok(m)
}

/// Each flag falls back to the reference PDS's env var.
fn start_if_set(
    (url, url_env): (Option<String>, &str),
    (from, from_env): (Option<String>, &str),
    ca_file: Option<&Path>,
    what: &str,
) -> anyhow::Result<Option<SharedMailer>> {
    match (pick(url, url_env), pick(from, from_env)) {
        (None, None) => Ok(None),
        (Some(url), Some(from)) => {
            let ca_pem = match ca_file {
                Some(p) => Some(std::fs::read(p).map_err(|e| anyhow::anyhow!("--email-smtp-ca-file {}: {e}", p.display()))?),
                None => None,
            };
            Ok(Some(SharedMailer(Arc::new(SmtpMailer::start(SmtpConfig { ca_pem, ..SmtpConfig::new(url, from) })?))))
        }
        _ => anyhow::bail!("partial {what} (or neither)"),
    }
}

/// A non-empty flag value, else the non-empty env var `k`.
fn pick(v: Option<String>, k: &str) -> Option<String> {
    v.filter(|v| !v.is_empty()).or_else(|| std::env::var(k).ok().filter(|v| !v.is_empty()))
}

pub struct SmtpMailer {
    tx: mpsc::Sender<Mail>,
}

impl SmtpMailer {
    /// Needs a tokio runtime.
    pub fn start(cfg: SmtpConfig) -> anyhow::Result<SmtpMailer> {
        let from: Mailbox = cfg
            .from
            .parse()
            .map_err(|e| anyhow::anyhow!("--email-from-address {:?}: {e}", cfg.from))?;
        let (transport, host) = transport(&cfg)?;
        tracing::info!(smtp_host = %host, from = %from, "email enabled (SMTP)");
        let (tx, rx) = mpsc::channel(cfg.queue.max(1));
        tokio::spawn(run(rx, transport, from, cfg));
        Ok(SmtpMailer { tx })
    }
}

impl Mailer for SmtpMailer {
    fn send(&self, mail: &Mail) {
        match self.tx.try_send(mail.clone()) {
            Ok(()) => MAIL_QUEUE.inc(),
            Err(e) => {
                let why = match e {
                    mpsc::error::TrySendError::Full(_) => "queue full",
                    mpsc::error::TrySendError::Closed(_) => "mailer stopped",
                };
                MAIL_MESSAGES.with_label_values(&["dropped", &mail.purpose]).inc();
                tracing::warn!(to = %mail.to, purpose = %mail.purpose, "mail dropped: {why}");
            }
        }
    }
}

/// nodemailer's default for smtp:// is opportunistic STARTTLS, lettre's is
/// plaintext (which it spells without `tls=`).
fn normalize_url(url: &str) -> anyhow::Result<String> {
    let (base, query) = url.split_once('?').unwrap_or((url, ""));
    let mut params: Vec<&str> = query.split('&').filter(|p| !p.is_empty()).collect();
    let tls = params.iter().position(|p| p.starts_with("tls="));
    if base.starts_with("smtp://") {
        match tls.map(|i| params[i]) {
            // nodemailer's smtp://: upgrade with STARTTLS when the server offers it
            None => params.push("tls=opportunistic"),
            Some("tls=none") => {
                params.remove(tls.unwrap());
            }
            Some("tls=required" | "tls=opportunistic") => {}
            Some(other) => anyhow::bail!("--email-smtp-url: unknown {other} (required, opportunistic, none)"),
        }
    } else if !base.starts_with("smtps://") {
        anyhow::bail!("--email-smtp-url must be smtp:// or smtps://");
    }
    Ok(if params.is_empty() { base.to_string() } else { format!("{base}?{}", params.join("&")) })
}

/// Also returns the host for logs: the URL may hold a password.
fn transport(cfg: &SmtpConfig) -> anyhow::Result<(AsyncSmtpTransport<Tokio1Executor>, String)> {
    let url = normalize_url(&cfg.url)?;
    let host = url
        .split_once("://")
        .map(|(_, r)| r)
        .and_then(|r| r.split(['/', '?']).next())
        .map(|r| r.rsplit('@').next().unwrap_or(r).to_string())
        .unwrap_or_default();
    let mut b = AsyncSmtpTransport::<Tokio1Executor>::from_url(&url)
        .map_err(|e| anyhow::anyhow!("--email-smtp-url (host {host:?}): {e}"))?;
    if let Some(pem) = &cfg.ca_pem {
        b = b.tls(with_ca(&url, pem)?);
    }
    let t = b
        .timeout(Some(CONNECT_TIMEOUT))
        .pool_config(PoolConfig::new().max_size(cfg.concurrency.max(1) as u32))
        .build();
    Ok((t, host))
}

/// The TLS mode `from_url` picked for the normalized `url`, re-made with
/// `pem`'s certificates added to the trusted roots.
fn with_ca(url: &str, pem: &[u8]) -> anyhow::Result<Tls> {
    let u = reqwest::Url::parse(url).map_err(|e| anyhow::anyhow!("--email-smtp-url: {e}"))?;
    let domain = u.host_str().unwrap_or_default().trim_matches(['[', ']']).to_string();
    let mode = u.query_pairs().find(|(k, _)| k == "tls").map(|(_, v)| v.into_owned());
    let wrap: fn(TlsParameters) -> Tls = match (u.scheme(), mode.as_deref()) {
        ("smtps", _) => Tls::Wrapper,
        (_, Some("required")) => Tls::Required,
        (_, Some("opportunistic")) => Tls::Opportunistic,
        _ => return Ok(Tls::None),
    };
    let cert = Certificate::from_pem(pem).map_err(|e| anyhow::anyhow!("--email-smtp-ca-file: {e}"))?;
    let params = TlsParameters::builder(domain)
        .add_root_certificate(cert)
        .build_rustls()
        .map_err(|e| anyhow::anyhow!("--email-smtp-ca-file: {e}"))?;
    Ok(wrap(params))
}

async fn run(
    mut rx: mpsc::Receiver<Mail>,
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: Mailbox,
    cfg: SmtpConfig,
) {
    let slots = Arc::new(Semaphore::new(cfg.concurrency.max(1)));
    let cfg = Arc::new(cfg);
    while let Some(mail) = rx.recv().await {
        let Ok(slot) = slots.clone().acquire_owned().await else { break };
        let (t, from, cfg) = (transport.clone(), from.clone(), cfg.clone());
        tokio::spawn(async move {
            send_one(&t, &from, &cfg, mail).await;
            MAIL_QUEUE.dec();
            drop(slot);
        });
    }
}

fn message(from: &Mailbox, m: &Mail) -> anyhow::Result<Message> {
    let to: Mailbox = m.to.parse().map_err(|e| anyhow::anyhow!("recipient: {e}"))?;
    // lettre sets no Message-ID, and some receivers (Gmail) refuse or junk
    // mail without one; the sender's domain, not the container's hostname
    let id = format!("<{}@{}>", hex::encode(rand::random::<[u8; 16]>()), from.email.domain());
    let b = Message::builder().from(from.clone()).to(to).subject(m.subject.as_str()).message_id(Some(id));
    Ok(match &m.html {
        // multipart/alternative: text/plain first, text/html preferred
        Some(html) => b.multipart(MultiPart::alternative_plain_html(m.body.clone(), html.clone()))?,
        None => b.header(ContentType::TEXT_PLAIN).body(m.body.clone())?,
    })
}

fn retryable(e: &lettre::transport::smtp::Error) -> bool {
    !(e.is_permanent() || e.is_client())
}

/// ±25%.
fn jitter(d: Duration) -> Duration {
    d.mul_f64(0.75 + rand::random::<f64>() * 0.5)
}

async fn send_one(t: &AsyncSmtpTransport<Tokio1Executor>, from: &Mailbox, cfg: &SmtpConfig, mail: Mail) {
    let started = std::time::Instant::now();
    let fail = |why: &str, attempts: usize| {
        MAIL_MESSAGES.with_label_values(&["failed", &mail.purpose]).inc();
        tracing::warn!(to = %mail.to, purpose = %mail.purpose, attempts, "mail not sent: {why}");
    };
    let msg = match message(from, &mail) {
        Ok(m) => m,
        Err(e) => return fail(&format!("invalid message: {e}"), 0),
    };
    let mut attempt = 0;
    loop {
        attempt += 1;
        let (retry, why) = match tokio::time::timeout(SEND_TIMEOUT, t.send(msg.clone())).await {
            Ok(Ok(_)) => {
                MAIL_MESSAGES.with_label_values(&["sent", &mail.purpose]).inc();
                MAIL_SEND_SECONDS.observe(started.elapsed().as_secs_f64());
                tracing::info!(to = %mail.to, purpose = %mail.purpose, attempts = attempt, "mail sent");
                return;
            }
            Ok(Err(e)) => (retryable(&e), e.to_string()),
            Err(_) => (true, format!("send timed out after {SEND_TIMEOUT:?}")),
        };
        let Some(wait) = cfg.backoff.get(attempt - 1).filter(|_| retry) else {
            return fail(&why, attempt);
        };
        MAIL_RETRIES.inc();
        tracing::info!(to = %mail.to, purpose = %mail.purpose, attempt, "mail send failed, retrying: {why}");
        tokio::time::sleep(jitter(*wait)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_defaults_follow_nodemailer() {
        assert_eq!(normalize_url("smtp://u:p@h:587").unwrap(), "smtp://u:p@h:587?tls=opportunistic");
        assert_eq!(normalize_url("smtp://h?tls=none").unwrap(), "smtp://h");
        assert_eq!(normalize_url("smtp://h?tls=required").unwrap(), "smtp://h?tls=required");
        assert_eq!(normalize_url("smtps://u:p@h").unwrap(), "smtps://u:p@h");
        assert!(normalize_url("http://h").is_err());
        assert!(normalize_url("smtp://h?tls=bogus").is_err());
    }

    #[tokio::test]
    async fn partial_config_is_an_error() {
        assert!(from_flags(Some("smtp://h".into()), Some(String::new()), None).is_err() || std::env::var("PDS_EMAIL_FROM_ADDRESS").is_ok());
        assert!(SmtpMailer::start(SmtpConfig::new("smtp://h", "not an address")).is_err());
        let (_, host) = transport(&SmtpConfig::new("smtps://user:secret@mail.example.com:465/ehlo", "a@b.c")).unwrap();
        assert_eq!(host, "mail.example.com:465");
    }
}
