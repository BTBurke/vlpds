//! Blob endpoints and the unreferenced-blob GC (DESIGN.md "6. Blobs").
//! `{prefix}/blob/{did}/{cid}` holds the bytes, MIME type as its
//! Content-Type attribute; large uploads go through a multipart upload to
//! `blob-tmp/` since the CID is only known at the end.

use super::sync::assert_available;
use super::*;
use futures::StreamExt;
use object_store::{Attribute, Attributes, ObjectStore, PutMultipartOptions, WriteMultipart};
use sha2::{Digest, Sha256};
use std::time::Duration;

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route("/xrpc/com.atproto.repo.uploadBlob", post(upload_blob))
        .route(
            "/xrpc/com.atproto.repo.listMissingBlobs",
            get(list_missing_blobs),
        )
        .route("/xrpc/com.atproto.sync.getBlob", get(get_blob))
        .route("/xrpc/com.atproto.sync.listBlobs", get(list_blobs))
}

/// S3's minimum part is 5 MiB. Smaller bodies are one PUT to the final key.
const PART_SIZE: usize = 8 << 20;
const PART_CONCURRENCY: usize = 4;
const TMP_GRACE: Duration = Duration::from_secs(24 * 3600);

pub(super) fn blob_path(app: &App, did: &str, cid: impl std::fmt::Display) -> object_store::path::Path {
    object_store::path::Path::from(format!("{}/blob/{}/{}", app.store.prefix, did, cid))
}

fn too_large(max: u64) -> XrpcError {
    XrpcError {
        status: StatusCode::PAYLOAD_TOO_LARGE,
        error: "PayloadTooLarge".into(),
        message: format!("request entity too large (max {max} bytes)"),
    }
}

fn blob_json(cid: &Cid, mime: &str, size: u64) -> J {
    json!({"blob": {"$type": "blob", "ref": {"$link": cid.to_string()}, "mimeType": mime, "size": size}})
}

async fn upload_blob(
    State(app): AppState,
    Auth(creds): Auth,
    headers: HeaderMap,
    body: Body,
) -> XResult<Json<J>> {
    let did = creds.user_did()?.to_string();
    let mime = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .unwrap_or("application/octet-stream")
        .to_string();
    creds.need_blob(&mime)?;
    // Deactivated accounts may upload (migration). Unlike the reference this
    // applies to user service JWTs too: one issued before a takedown would
    // otherwise still upload for up to an hour.
    if super::server::is_takendown_account(&app.account(&did).await?) {
        return Err(super::takedown_error());
    }
    let max = app.config.max_blob_size;
    let declared = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if declared.is_some_and(|n| n > max) {
        return Err(too_large(max));
    }
    let mut attrs = Attributes::new();
    attrs.insert(Attribute::ContentType, mime.clone().into());

    let mut up = Upload {
        app: &app,
        did: &did,
        attrs,
        mime,
        head: Vec::new(),
        buf: Vec::new(),
        buffered: 0,
        multipart: None,
    };
    let res = up.run(body, max).await;
    let (cid, size) = match res {
        Ok(v) => v,
        Err(e) => {
            if let Some((w, tmp)) = up.multipart.take() {
                let _ = w.abort().await;
                let _ = app.store.raw.delete(&tmp).await;
            }
            return Err(e);
        }
    };
    if declared.is_some_and(|n| n != size) {
        tracing::debug!(%did, declared = ?declared, size, "uploadBlob: content-length mismatch");
    }
    // checked after storing: the bytes are content-addressed, so that changed nothing
    if super::admin::is_blob_takendown(&app, &did, &cid.to_string()).await? {
        return Err(XrpcError::bad(
            "InvalidRequest",
            "Blob has been takendown, cannot re-upload",
        ));
    }
    crate::metrics::BLOB_UPLOADS.with_label_values(&[crate::metrics::blob_kind(&up.mime)]).inc();
    crate::metrics::BLOB_UPLOAD_BYTES.inc_by(size);
    Ok(Json(blob_json(&cid, &up.mime, size)))
}

struct Upload<'a> {
    app: &'a App,
    did: &'a str,
    attrs: Attributes,
    /// Client-declared type until the first bytes are sniffed.
    mime: String,
    head: Vec<u8>,
    /// Held until we know whether a multipart upload is needed.
    buf: Vec<Bytes>,
    buffered: usize,
    multipart: Option<(WriteMultipart, object_store::path::Path)>,
}

impl Upload<'_> {
    async fn run(&mut self, body: Body, max: u64) -> XResult<(Cid, u64)> {
        let mut stream = body.into_data_stream();
        let mut hasher = Sha256::new();
        let mut size: u64 = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| {
                XrpcError::bad("InvalidRequest", format!("error reading body: {e}"))
            })?;
            size += chunk.len() as u64;
            if size > max {
                return Err(too_large(max));
            }
            hasher.update(&chunk);
            if self.head.len() < SNIFF_BYTES {
                let n = (SNIFF_BYTES - self.head.len()).min(chunk.len());
                self.head.extend_from_slice(&chunk[..n]);
            }
            match &mut self.multipart {
                Some((w, _)) => {
                    w.wait_for_capacity(PART_CONCURRENCY)
                        .await
                        .map_err(XrpcError::from_err)?;
                    w.put(chunk);
                }
                None => {
                    self.buffered += chunk.len();
                    self.buf.push(chunk);
                    if self.buffered >= PART_SIZE {
                        self.start_multipart().await?;
                    }
                }
            }
        }
        let cid = Cid {
            codec: crate::cid::CODEC_RAW,
            digest: hasher.finalize().into(),
        };
        let dest = blob_path(self.app, self.did, cid);
        let store = &self.app.store.raw;
        match self.multipart.take() {
            None => {
                self.sniff();
                let payload: PutPayload = std::mem::take(&mut self.buf).into_iter().collect();
                let opts = PutOptions {
                    attributes: self.attrs.clone(),
                    ..Default::default()
                };
                store
                    .put_opts(&dest, payload, opts)
                    .await
                    .map_err(XrpcError::from_err)?;
            }
            Some((w, tmp)) => {
                if let Err(e) = w.finish().await {
                    let _ = store.delete(&tmp).await;
                    return Err(XrpcError::from_err(e));
                }
                // the copy restarts a re-uploaded blob's GC grace period
                let copied = store.copy(&tmp, &dest).await;
                let _ = store.delete(&tmp).await;
                copied.map_err(XrpcError::from_err)?;
            }
        }
        Ok((cid, size))
    }

    /// As the reference (file-type), a recognized signature overrides the
    /// client's Content-Type.
    fn sniff(&mut self) {
        if let Some(m) = sniff_mime(&self.head) {
            self.mime = m.to_string();
        }
        self.attrs.insert(Attribute::ContentType, self.mime.clone().into());
    }

    async fn start_multipart(&mut self) -> XResult<()> {
        self.sniff();
        let tmp = object_store::path::Path::from(format!(
            "{}/blob-tmp/{}/{}",
            self.app.store.prefix,
            self.did,
            hex::encode(rand::random::<[u8; 16]>())
        ));
        let opts = PutMultipartOptions {
            attributes: self.attrs.clone(),
            ..Default::default()
        };
        let upload = self
            .app
            .store
            .raw
            .put_multipart_opts(&tmp, opts)
            .await
            .map_err(XrpcError::from_err)?;
        let mut w = WriteMultipart::new_with_chunk_size(upload, PART_SIZE);
        for b in self.buf.drain(..) {
            w.put(b);
        }
        self.buffered = 0;
        self.multipart = Some((w, tmp));
        Ok(())
    }
}

const SNIFF_BYTES: usize = 64;

/// The common subset of what the reference's `file-type` detects for media.
fn sniff_mime(b: &[u8]) -> Option<&'static str> {
    let at = |off: usize, sig: &[u8]| b.len() >= off + sig.len() && &b[off..off + sig.len()] == sig;
    if at(0, b"\x89PNG\r\n\x1a\n") {
        return Some("image/png");
    }
    if at(0, &[0xff, 0xd8, 0xff]) {
        return Some("image/jpeg");
    }
    if at(0, b"GIF87a") || at(0, b"GIF89a") {
        return Some("image/gif");
    }
    if at(0, b"RIFF") && at(8, b"WEBP") {
        return Some("image/webp");
    }
    if at(0, &[0x1a, 0x45, 0xdf, 0xa3]) {
        return Some("video/webm");
    }
    if at(0, b"%PDF-") {
        return Some("application/pdf");
    }
    if at(4, b"ftyp") {
        let brand = b.get(8..12)?;
        return Some(match brand {
            b"avif" | b"avis" => "image/avif",
            b"heic" | b"heix" | b"heim" | b"heis" => "image/heic",
            b"mif1" | b"msf1" => "image/heif",
            b"qt  " => "video/quicktime",
            b"M4A " | b"M4B " => "audio/mp4",
            b"3gp4" | b"3gp5" | b"3gp6" | b"3gs7" => "video/3gpp",
            _ => "video/mp4",
        });
    }
    None
}

pub(super) fn stored_mime(attrs: &Attributes) -> String {
    attrs
        .get(&Attribute::ContentType)
        .map(|v| v.as_ref().to_string())
        .unwrap_or_else(|| "application/octet-stream".into())
}

#[derive(Deserialize)]
struct BlobQ {
    did: String,
    cid: String,
}

async fn get_blob(
    State(app): AppState,
    MaybeAuth(creds): MaybeAuth,
    Query(q): Query<BlobQ>,
) -> XResult<Response> {
    let cid = Cid::parse(&q.cid).map_err(|_| XrpcError::bad("InvalidRequest", "Invalid cid"))?;
    assert_available(&app, &q.did, creds.as_ref()).await?;
    let is_admin = matches!(creds, Some(Credentials::Admin));
    if !is_admin && super::admin::is_blob_takendown(&app, &q.did, &cid.to_string()).await? {
        return Err(XrpcError::bad("BlobNotFound", "Blob not found"));
    }
    let r = match app.store.raw.get(&blob_path(&app, &q.did, cid)).await {
        Ok(r) => r,
        Err(object_store::Error::NotFound { .. }) => {
            return Err(XrpcError::bad("BlobNotFound", "Blob not found"))
        }
        Err(e) => return Err(XrpcError::from_err(e)),
    };
    let mime = stored_mime(&r.attributes);
    let size = r.meta.size;
    let mut resp = Body::from_stream(r.into_stream()).into_response();
    let h = resp.headers_mut();
    let hv = |s: String| {
        header::HeaderValue::from_str(&s)
            .unwrap_or(header::HeaderValue::from_static("application/octet-stream"))
    };
    h.insert(header::CONTENT_TYPE, hv(mime));
    h.insert(header::CONTENT_LENGTH, header::HeaderValue::from(size));
    // the reference PDS's hardening headers
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::CONTENT_DISPOSITION,
        hv(format!("attachment; filename=\"{cid}\"")),
    );
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        header::HeaderValue::from_static("default-src 'none'; sandbox"),
    );
    Ok(resp)
}

/// Distinct (cid, one referencing record path) of `did`, in CID order after
/// `cursor`; with `since`, only refs from records written after that rev.
async fn referenced_blobs(
    app: &App,
    did: &str,
    cursor: Option<&str>,
    limit: usize,
    since: Option<u64>,
) -> XResult<Vec<(String, String)>> {
    let p = app.partition(did)?;
    let prefix = state::blob_ref_prefix(did);
    let lo = match cursor {
        // skip every key of the cursor cid: b/{did}\0{cursor}\0...
        Some(c) => state::prefix_end(&[prefix.as_slice(), c.as_bytes(), b"\0"].concat()),
        None => prefix.clone(),
    };
    let mut iter =
        p.db.scan(lo..state::prefix_end(&prefix))
            .await
            .map_err(XrpcError::from_err)?;
    let mut out: Vec<(String, String)> = Vec::new();
    while let Some(kv) = iter.next().await.map_err(XrpcError::from_err)? {
        let rest = String::from_utf8_lossy(&kv.key[prefix.len()..]).into_owned();
        let Some((cid, path)) = rest.split_once('\0') else {
            continue;
        };
        if out.last().is_some_and(|(c, _)| c == cid) {
            continue;
        }
        // ref values carry the rev of the record that references the blob
        if let Some(s) = since {
            let rev = kv.value.get(..8).map(|b| u64::from_be_bytes(b.try_into().unwrap())).unwrap_or(0);
            if rev <= s {
                continue;
            }
        }
        if out.len() == limit {
            break;
        }
        out.push((cid.to_string(), path.to_string()));
    }
    Ok(out)
}

#[derive(Deserialize)]
struct ListBlobsQ {
    did: String,
    since: Option<String>,
    limit: Option<i64>,
    cursor: Option<String>,
}

async fn list_blobs(
    State(app): AppState,
    MaybeAuth(creds): MaybeAuth,
    Query(q): Query<ListBlobsQ>,
) -> XResult<Json<J>> {
    let since = match q.since.as_deref() {
        Some(s) => Some(crate::tid::Tid::parse(s).ok_or_else(|| XrpcError::bad("InvalidRequest", "since must be a TID"))?.0),
        None => None,
    };
    let limit = super::extract::limit_param(q.limit, 500, 1, 1000)?;
    assert_available(&app, &q.did, creds.as_ref()).await?;
    let blobs = referenced_blobs(&app, &q.did, q.cursor.as_deref(), limit, since).await?;
    let cids: Vec<&str> = blobs.iter().map(|(c, _)| c.as_str()).collect();
    let mut out = json!({"cids": cids});
    if let Some(last) = cids.last() {
        out["cursor"] = json!(last);
    }
    Ok(Json(out))
}

#[derive(Deserialize)]
struct MissingQ {
    limit: Option<usize>,
    cursor: Option<String>,
}

async fn list_missing_blobs(
    State(app): AppState,
    Auth(creds): Auth,
    Query(q): Query<MissingQ>,
) -> XResult<Json<J>> {
    let did = creds.user_did()?.to_string();
    let limit = q.limit.unwrap_or(500).clamp(1, 1000);
    let mut cursor = q.cursor.clone();
    let mut missing: Vec<J> = Vec::new();
    'outer: loop {
        let page = referenced_blobs(&app, &did, cursor.as_deref(), 256, None).await?;
        if page.is_empty() {
            break;
        }
        cursor = page.last().map(|(c, _)| c.clone());
        let checks: Vec<_> = page
            .iter()
            .map(|(cid, _)| {
                let path = blob_path(&app, &did, cid);
                let store = app.store.raw.clone();
                async move {
                    match store.head(&path).await {
                        Ok(_) => Ok(false),
                        Err(object_store::Error::NotFound { .. }) => Ok(true),
                        Err(e) => Err(XrpcError::from_err(e)),
                    }
                }
            })
            .collect();
        let results: Vec<XResult<bool>> =
            futures::stream::iter(checks).buffered(32).collect().await;
        for ((cid, path), r) in page.iter().zip(results) {
            if r? {
                missing.push(json!({"cid": cid, "recordUri": format!("at://{did}/{path}")}));
                if missing.len() == limit {
                    break 'outer;
                }
            }
        }
        if page.len() < 256 {
            break;
        }
    }
    let mut out = json!({"blobs": missing});
    if let Some(last) = missing.last() {
        out["cursor"] = last["cid"].clone();
    }
    Ok(Json(out))
}

/// Only DIDs whose partition this node owns are swept.
pub fn spawn_blob_gc(app: Arc<App>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let grace = app.config.blob_gc_grace;
        let every = (grace / 4).clamp(Duration::from_secs(10), Duration::from_secs(3600));
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            match sweep_blobs(&app, grace).await {
                Ok((scanned, deleted)) if deleted > 0 => {
                    tracing::info!(scanned, deleted, "blob gc")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!("blob gc: {e:#}"),
            }
        }
    })
}

/// Longer than a write that checked the blob (repo.rs `check_blobs`)
/// usually takes to apply its reference; one still in flight past it holds
/// the blob ([`HeldBlobs`]).
const QUARANTINE_SETTLE: Duration = Duration::from_secs(60);

/// (did, cid) -> writes in flight on this node that checked the blob. A
/// quarantined blob held here isn't purged however long its write takes to
/// apply (a cold repo load, a store brownout). Writes run on the repo's
/// owner, the node whose GC sweeps its blobs.
static IN_FLIGHT: std::sync::LazyLock<parking_lot::Mutex<std::collections::HashMap<(String, Cid), usize>>> =
    std::sync::LazyLock::new(Default::default);

/// Holds a write's checked blobs in [`IN_FLIGHT`] until dropped.
pub struct HeldBlobs {
    keys: Vec<(String, Cid)>,
}

impl HeldBlobs {
    pub fn hold(did: &str, cids: impl IntoIterator<Item = Cid>) -> HeldBlobs {
        let keys: Vec<(String, Cid)> = cids.into_iter().map(|c| (did.to_string(), c)).collect();
        let mut m = IN_FLIGHT.lock();
        for k in &keys {
            *m.entry(k.clone()).or_default() += 1;
        }
        HeldBlobs { keys }
    }
}

impl Drop for HeldBlobs {
    fn drop(&mut self) {
        let mut m = IN_FLIGHT.lock();
        for k in &self.keys {
            if let Some(n) = m.get_mut(k) {
                *n -= 1;
                if *n == 0 {
                    m.remove(k);
                }
            }
        }
    }
}

fn held(did: &str, cid: &Cid) -> bool {
    IN_FLIGHT.lock().contains_key(&(did.to_string(), *cid))
}

fn quarantine_path(app: &App, did: &str, cid: &str) -> object_store::path::Path {
    object_store::path::Path::from(format!("{}/blob-gc/{}/{}", app.store.prefix, did, cid))
}

async fn referenced(p: &Partition, did: &str, cid: &str) -> anyhow::Result<bool> {
    let prefix = [state::blob_ref_prefix(did).as_slice(), cid.as_bytes(), b"\0"].concat();
    let mut iter = p.db.scan(prefix.clone()..state::prefix_end(&prefix)).await?;
    Ok(iter.next().await?.is_some())
}

/// (did, cid) of a `.../{did}/{cid}` object path.
fn did_cid(path: &object_store::path::Path) -> Option<(String, String)> {
    let parts: Vec<String> = path.parts().map(|p| super::sync::pct_decode(p.as_ref(), false)).collect();
    let n = parts.len();
    (n >= 2).then(|| (parts[n - 2].clone(), parts[n - 1].clone()))
}

/// Returns (blobs scanned, objects deleted).
pub async fn sweep_blobs(app: &App, grace: Duration) -> anyhow::Result<(usize, usize)> {
    sweep_blobs_settle(app, grace, grace.min(QUARANTINE_SETTLE)).await
}

/// A blob moved to quarantine counts as deleted (it is no longer served).
///
/// The race: a write checks that its blob exists (`check_blobs`) and is
/// applied a little later; a sweep that read "no reference" in between would
/// delete the blob under it. So an unreferenced blob older than `grace` is
/// first moved to `blob-gc/` (writes referencing it then fail their check),
/// and only after `settle` are its references checked again: one that
/// appeared moves it back, a write still in flight ([`HeldBlobs`]) defers
/// it, otherwise it is deleted.
///
/// Orphaned multipart uploads can't be listed through object_store: DESIGN.md
/// "6. Blobs" has the bucket lifecycle rule that aborts them.
pub async fn sweep_blobs_settle(app: &App, grace: Duration, settle: Duration) -> anyhow::Result<(usize, usize)> {
    let now = chrono::Utc::now();
    let cutoff = now - chrono::Duration::from_std(grace)?;
    let store = app.store.raw.clone();
    let (mut scanned, mut deleted) = (0usize, 0usize);

    let blob_root = object_store::path::Path::from(format!("{}/blob", app.store.prefix));
    let mut list = store.list(Some(&blob_root));
    while let Some(meta) = list.next().await {
        let meta = meta?;
        scanned += 1;
        if meta.last_modified > cutoff {
            continue;
        }
        let Some((did, cid)) = did_cid(&meta.location) else { continue };
        let Ok(p) = app.partition(&did) else { continue };
        if referenced(&p, &did, &cid).await? {
            continue;
        }
        // the copy's last-modified time starts the settle period
        if let Err(e) = store.copy(&meta.location, &quarantine_path(app, &did, &cid)).await {
            if !matches!(e, object_store::Error::NotFound { .. }) {
                tracing::warn!(path = %meta.location, "blob gc quarantine: {e}");
            }
            continue;
        }
        match store.delete(&meta.location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => deleted += 1,
            Err(e) => tracing::warn!(path = %meta.location, "blob gc delete: {e}"),
        }
    }

    let settled = now - chrono::Duration::from_std(settle)?;
    let gc_root = object_store::path::Path::from(format!("{}/blob-gc", app.store.prefix));
    let mut list = store.list(Some(&gc_root));
    let (mut restored, mut purged) = (0usize, 0usize);
    while let Some(meta) = list.next().await {
        let meta = meta?;
        if meta.last_modified > settled {
            continue;
        }
        let Some((did, cid)) = did_cid(&meta.location) else { continue };
        let Ok(p) = app.partition(&did) else { continue };
        if referenced(&p, &did, &cid).await? {
            // a write that checked the blob before the move: put it back
            let Ok(c) = Cid::parse(&cid) else { continue };
            if let Err(e) = store.copy(&meta.location, &blob_path(app, &did, c)).await {
                tracing::warn!(path = %meta.location, "blob gc restore: {e}");
                continue;
            }
            restored += 1;
            tracing::warn!(%did, %cid, "blob gc: a reference appeared during quarantine; restored");
        } else if Cid::parse(&cid).is_ok_and(|c| held(&did, &c)) {
            // a write that checked it hasn't applied yet: decide next pass
            continue;
        } else {
            purged += 1;
        }
        if let Err(e) = store.delete(&meta.location).await {
            tracing::warn!(path = %meta.location, "blob gc purge: {e}");
        }
    }
    if restored + purged > 0 {
        tracing::debug!(restored, purged, "blob gc quarantine");
    }

    let tmp_cutoff = now - chrono::Duration::from_std(TMP_GRACE.max(grace))?;
    let tmp_root = object_store::path::Path::from(format!("{}/blob-tmp", app.store.prefix));
    let mut list = store.list(Some(&tmp_root));
    while let Some(meta) = list.next().await {
        let meta = meta?;
        if meta.last_modified <= tmp_cutoff && store.delete(&meta.location).await.is_ok() {
            deleted += 1;
        }
    }
    Ok((scanned, deleted))
}
