//! Blobs: uploadBlob (streamed to the object store), sync.getBlob,
//! sync.listBlobs, repo.listMissingBlobs, and the unreferenced-blob GC.
//!
//! Layout: `{prefix}/blob/{did}/{cid}` holds the bytes, with the MIME type
//! as the object's Content-Type attribute. Large uploads stream through a
//! multipart upload to `{prefix}/blob-tmp/{did}/{random}` (the CID is only
//! known at the end), then are copied into place. References live in
//! SlateDB as `b/{did}\0{cid}\0{record path}`, maintained at commit time.

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

/// Multipart part size (S3's minimum is 5 MiB). Bodies smaller than this are
/// sent as a single PUT straight to the final key.
const PART_SIZE: usize = 8 << 20;
/// Parts uploading concurrently per blob.
const PART_CONCURRENCY: usize = 4;
/// Orphaned temp objects (crashed uploads) are removed after this long.
const TMP_GRACE: Duration = Duration::from_secs(24 * 3600);

pub(super) fn blob_path(app: &App, did: &str, cid: &Cid) -> object_store::path::Path {
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
    let did = creds
        .did()
        .ok_or_else(|| XrpcError::auth("user credentials required"))?
        .to_string();
    let mime = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .unwrap_or("application/octet-stream")
        .to_string();
    creds.require(creds.allows_blob(&mime))?;
    // Deactivated accounts may upload (migration); taken-down ones may not.
    let acct = app.account(&did).await?;
    if matches!(
        acct.status.as_deref(),
        Some("takendown") | Some("suspended")
    ) {
        return Err(XrpcError {
            status: StatusCode::UNAUTHORIZED,
            error: "AccountTakedown".into(),
            message: "Account has been taken down".into(),
        });
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
    // (the bytes are content-addressed, so storing them again changed nothing)
    if super::admin::is_blob_takendown(&app, &did, &cid.to_string()).await {
        return Err(XrpcError::bad(
            "InvalidRequest",
            "Blob has been takendown, cannot re-upload",
        ));
    }
    Ok(Json(blob_json(&cid, &up.mime, size)))
}

struct Upload<'a> {
    app: &'a App,
    did: &'a str,
    attrs: Attributes,
    /// Client-declared type until the first bytes are sniffed.
    mime: String,
    /// First bytes of the body, for content sniffing.
    head: Vec<u8>,
    /// Body chunks held until we know whether a multipart upload is needed.
    buf: Vec<Bytes>,
    buffered: usize,
    multipart: Option<(WriteMultipart, object_store::path::Path)>,
}

impl Upload<'_> {
    /// Streams the body, hashing as it goes; returns (cid, size).
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
        let dest = blob_path(self.app, self.did, &cid);
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
                // Copy also refreshes the final object's last-modified time,
                // which restarts its GC grace period for a re-upload.
                let copied = store.copy(&tmp, &dest).await;
                let _ = store.delete(&tmp).await;
                copied.map_err(XrpcError::from_err)?;
            }
        }
        Ok((cid, size))
    }

    /// Content sniffing as the reference (file-type): a recognized
    /// signature overrides the client's Content-Type.
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

/// MIME type from well-known file signatures (the common subset of what the
/// reference's `file-type` detects for media uploads).
pub fn sniff_mime(b: &[u8]) -> Option<&'static str> {
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
    if !is_admin && super::admin::is_blob_takendown(&app, &q.did, &cid.to_string()).await {
        return Err(XrpcError::bad("BlobNotFound", "Blob not found"));
    }
    let r = match app.store.raw.get(&blob_path(&app, &q.did, &cid)).await {
        Ok(r) => r,
        Err(object_store::Error::NotFound { .. }) => {
            return Err(XrpcError::bad("BlobNotFound", "Blob not found"))
        }
        Err(e) => return Err(XrpcError::from_err(e)),
    };
    let mime = r
        .attributes
        .get(&Attribute::ContentType)
        .map(|v| v.as_ref().to_string())
        .unwrap_or_else(|| "application/octet-stream".into());
    let size = r.meta.size;
    let mut resp = Body::from_stream(r.into_stream()).into_response();
    let h = resp.headers_mut();
    let hv = |s: String| {
        header::HeaderValue::from_str(&s)
            .unwrap_or(header::HeaderValue::from_static("application/octet-stream"))
    };
    h.insert(header::CONTENT_TYPE, hv(mime));
    h.insert(header::CONTENT_LENGTH, header::HeaderValue::from(size));
    // same hardening headers as the reference PDS
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

/// Distinct blob CIDs referenced by `did`'s records, in CID order, starting
/// after `cursor`. Returns (cid, one referencing record path).
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
    limit: Option<usize>,
    cursor: Option<String>,
}

/// With `since`, only blobs referenced by records written after that rev
/// (as the reference does via record.repoRev).
async fn list_blobs(
    State(app): AppState,
    MaybeAuth(creds): MaybeAuth,
    Query(q): Query<ListBlobsQ>,
) -> XResult<Json<J>> {
    let since = match q.since.as_deref() {
        Some(s) => Some(crate::tid::Tid::parse(s).ok_or_else(|| XrpcError::bad("InvalidRequest", "since must be a TID"))?.0),
        None => None,
    };
    let limit = q.limit.unwrap_or(500).clamp(1, 1000);
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

/// Blobs referenced by the caller's records whose bytes aren't in the store
/// (e.g. after importRepo, before the blobs are re-uploaded).
async fn list_missing_blobs(
    State(app): AppState,
    Auth(creds): Auth,
    Query(q): Query<MissingQ>,
) -> XResult<Json<J>> {
    let did = creds
        .did()
        .ok_or_else(|| XrpcError::auth("user credentials required"))?
        .to_string();
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
                object_store::path::Path::from(format!("{}/blob/{}/{}", app.store.prefix, did, cid))
            })
            .map(|path| {
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

// ---------- unreferenced-blob GC ----------

/// Starts the background sweeper: every `min(grace / 4, 1h)` (at least 10 s)
/// it deletes blobs last written more than `config.blob_gc_grace` ago that no
/// record references (no `b/{did}\0{cid}\0` key), plus stale temp uploads.
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

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && b[i + 1].is_ascii_hexdigit()
            && b[i + 2].is_ascii_hexdigit()
        {
            out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap_or(b'%'));
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// One GC pass. Returns (blobs scanned, objects deleted).
///
/// Race: a record referencing a blob committed between the reference check
/// and the delete loses the blob (listMissingBlobs then reports it). The
/// grace period makes this require a client to reference a blob it uploaded
/// more than `grace` ago that was unreferenced for that whole time.
pub async fn sweep_blobs(app: &App, grace: Duration) -> anyhow::Result<(usize, usize)> {
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
        let parts: Vec<String> = meta
            .location
            .parts()
            .map(|p| pct_decode(p.as_ref()))
            .collect();
        let n = parts.len();
        if n < 2 {
            continue;
        }
        let (did, cid) = (&parts[n - 2], &parts[n - 1]);
        let Ok(p) = app.partition(did) else { continue };
        let prefix = [
            state::blob_ref_prefix(did).as_slice(),
            cid.as_bytes(),
            b"\0",
        ]
        .concat();
        let mut iter =
            p.db.scan(prefix.clone()..state::prefix_end(&prefix))
                .await?;
        if iter.next().await?.is_some() {
            continue;
        }
        match store.delete(&meta.location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => deleted += 1,
            Err(e) => tracing::warn!(path = %meta.location, "blob gc delete: {e}"),
        }
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
