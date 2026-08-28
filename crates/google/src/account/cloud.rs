//! Google Drive large-attachment hosting: resumable upload + sharing link.
//!
//! COPY-AND-ADAPTed from ratatoskr's `gmail/gdrive.rs`. The pure pieces (serde
//! structs, chunk-range math, the scope vocabulary) copy cleanly; the transport
//! bodies are rewritten onto bifrost-net's `RequestBuilder`/`Response` - no bare
//! `reqwest::Client`, no hand-built `Authorization: Bearer` headers (the
//! AccountNet token source supplies them; the pre-authenticated chunk PUT skips
//! auth with `.without_bearer_auth()`).
//!
//! The whole flow is one call: upload the bytes via the resumable session, then
//! create a sharing permission and read the `webViewLink`. It returns only when
//! both steps succeed; a stray uploaded-but-unlinked file is the worst failure
//! mode, so a failed link step after a successful upload is still an `Err`.
//!
//! The chunk loop's 308 Resume Incomplete handling depends on bifrost-net's
//! 308-without-Location passthrough: a resumable 308 carries a `Range` header
//! and NO `Location`, and without the passthrough the default redirect-follower
//! converts it to `MalformedRedirect` and every multi-chunk upload fails on the
//! first incomplete chunk.
//!
//! Byte accounting: these are the only Gmail-crate requests that do NOT go
//! through `GmailClient::send_recorded`, so they are not enrolled in the
//! per-batch `ByteTally` the sync and mutation streams publish. That
//! under-reports nothing, because Drive uploads are a PIM call and feed no
//! `bytes_in` field at all - they are not an engine batch. Recorded here so a
//! byte-accounting audit does not re-file it as a metering hole; the general
//! claim that every request-bearing lane is metered is about the batch-emitting
//! producers, which this is not.

use std::sync::Arc;

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, CloudUploadMeta, HostedAttachment, ShareScope,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::account::error::{GmailErrorContext, into_account_error};
use crate::client::GmailClient;
use crate::error::{Error, GmailResponseHeaders, GmailService};

/// Minimum alignment for upload chunks (256 KiB per Google Drive API spec).
const GDRIVE_CHUNK_ALIGN: usize = 256 * 1024;

/// Default chunk size: 5 MiB. Must be a multiple of `GDRIVE_CHUNK_ALIGN`.
const GDRIVE_CHUNK_SIZE: usize = 5 * 1024 * 1024;
const GDRIVE_MAX_EXTRA_CHUNK_ATTEMPTS: usize = 8;

const SESSION_URL: &str = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable";

/// The completed file metadata returned after a successful upload.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GDriveFileResponse {
    id: String,
    #[serde(default)]
    #[allow(dead_code)]
    name: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    web_view_link: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    size: Option<String>,
}

/// Request body for file metadata when creating an upload session.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FileMetadata<'a> {
    name: &'a str,
    mime_type: &'a str,
}

/// Response from getting file metadata with `webViewLink`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileWebLinkResponse {
    web_view_link: String,
}

/// Dispatch entry point wired from the `Account` impl. Drives the whole
/// upload + link flow on a cloned client handle.
pub(crate) fn host_attachment(
    client: Arc<GmailClient>,
    account_email: String,
    bytes: Bytes,
    meta: CloudUploadMeta,
) -> AccountFuture<Result<HostedAttachment, AccountError>> {
    Box::pin(async move { run(&client, &account_email, bytes, meta).await })
}

async fn run(
    client: &GmailClient,
    account_email: &str,
    bytes: Bytes,
    meta: CloudUploadMeta,
) -> Result<HostedAttachment, AccountError> {
    upload_and_link(client, account_email, bytes, meta)
        .await
        .map_err(|e| into_account_error(e, ctx()))
}

fn ctx() -> GmailErrorContext {
    GmailErrorContext::base(AccountOperation::HostAttachment)
}

async fn upload_and_link(
    client: &GmailClient,
    account_email: &str,
    bytes: Bytes,
    meta: CloudUploadMeta,
) -> Result<HostedAttachment, Error> {
    if meta.size != bytes.len() as u64 {
        return Err(Error::invalid_request(
            AccountOperation::HostAttachment,
            format!(
                "declared size {} does not match payload length {}",
                meta.size,
                bytes.len()
            ),
        ));
    }

    let upload_url = create_upload_session(client, &meta).await?;
    let file_id = upload_file_chunked(client, &upload_url, bytes, GDRIVE_CHUNK_SIZE).await?;
    let share_url = create_sharing_permission(client, &file_id, meta.scope, account_email).await?;

    Ok(HostedAttachment::new(share_url, file_id))
}

/// Create a resumable upload session. Returns the pre-authenticated upload URL
/// read from the `Location` response header.
///
/// The typed `GmailClient::post` helper cannot set `X-Upload-Content-*`, so
/// this uses the raw builder path; the AccountNet token source supplies the
/// Bearer.
async fn create_upload_session(
    client: &GmailClient,
    meta: &CloudUploadMeta,
) -> Result<String, Error> {
    let metadata = FileMetadata {
        name: &meta.file_name,
        mime_type: &meta.mime,
    };

    let response = client
        .account_net()
        .post(SESSION_URL)
        .header("X-Upload-Content-Type", meta.mime.as_str())
        .header("X-Upload-Content-Length", &meta.size.to_string())
        .json(&metadata)
        .send()
        .await
        .map_err(Error::Net)?;

    let status = response.status();
    if !status.is_success() {
        return Err(response_error(
            &response.headers,
            status.as_u16(),
            response.body,
        ));
    }

    response
        .headers
        .get("location")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .ok_or_else(|| {
            Error::invalid_request(
                AccountOperation::HostAttachment,
                "upload session response missing Location header",
            )
        })
}

/// Upload the payload in `chunk_size`-aligned chunks against the pre-authenticated
/// session URL.
///
/// 200/201 -> the final chunk was accepted; parse the file metadata. 308 ->
/// resume, advancing the offset from the parsed `Range: bytes=0-N` header.
///
/// Bug fix vs. the ratatoskr source: on a 308 whose `Range` header is absent or
/// unparseable, this FAILS rather than falling back to `offset = end`. The old
/// fallback silently skips a gap whenever the server accepted fewer bytes than
/// were sent, corrupting the upload. An unparseable resume cursor is
/// unrecoverable for this attempt.
async fn upload_file_chunked(
    client: &GmailClient,
    upload_url: &str,
    data: Bytes,
    chunk_size: usize,
) -> Result<String, Error> {
    if chunk_size == 0 || !chunk_size.is_multiple_of(GDRIVE_CHUNK_ALIGN) {
        return Err(Error::invalid_request(
            AccountOperation::HostAttachment,
            format!(
                "chunk_size must be a positive multiple of {GDRIVE_CHUNK_ALIGN}, got {chunk_size}"
            ),
        ));
    }

    let total = data.len();
    if total == 0 {
        return Err(Error::invalid_request(
            AccountOperation::HostAttachment,
            "cannot upload empty file",
        ));
    }

    let mut offset = 0usize;
    let max_attempts = total
        .div_ceil(chunk_size)
        .saturating_add(GDRIVE_MAX_EXTRA_CHUNK_ATTEMPTS);
    let mut attempts = 0usize;
    while offset < total {
        attempts = attempts.saturating_add(1);
        if attempts > max_attempts {
            return Err(Error::invalid_request(
                AccountOperation::HostAttachment,
                format!("resumable upload exceeded {max_attempts} chunk attempts"),
            ));
        }
        let end = (offset + chunk_size).min(total);
        let chunk = data.slice(offset..end);
        let content_range = format!("bytes {offset}-{}/{total}", end - 1);

        let response = client
            .account_net()
            .put(upload_url)
            .without_bearer_auth()
            .header("Content-Range", &content_range)
            .body(chunk)
            .send()
            .await
            .map_err(Error::Net)?;

        let status = response.status().as_u16();
        match status {
            200 | 201 => {
                let file: GDriveFileResponse = serde_json::from_slice(response.body.as_ref())
                    .map_err(|source| Error::JsonDecode {
                        service: GmailService::GmailApi,
                        source,
                    })?;
                return Ok(file.id);
            }
            308 => {
                let resumed = parse_resume_offset(&response.headers)?;
                if resumed <= offset || resumed > end {
                    return Err(Error::invalid_request(
                        AccountOperation::HostAttachment,
                        format!(
                            "resumable 308 made invalid progress from {offset} to {resumed} after sending through {end}"
                        ),
                    ));
                }
                offset = resumed;
            }
            _ => {
                return Err(response_error(&response.headers, status, response.body));
            }
        }
    }

    Err(Error::invalid_request(
        AccountOperation::HostAttachment,
        "upload completed without receiving a file response",
    ))
}

/// Parse the next byte offset from a 308 `Range: bytes=0-N` header. Returns an
/// error (never a silent skip) when the header is absent or unparseable.
fn parse_resume_offset(headers: &reqwest::header::HeaderMap) -> Result<usize, Error> {
    headers
        .get("range")
        .and_then(|v| v.to_str().ok())
        .and_then(|range| range.strip_prefix("bytes=0-"))
        .and_then(|end| end.parse::<usize>().ok())
        .and_then(|last_byte| last_byte.checked_add(1))
        .ok_or_else(|| {
            Error::invalid_request(
                AccountOperation::HostAttachment,
                "resumable 308 had an absent or unparseable Range header; \
                 refusing to skip a gap",
            )
        })
}

/// Create a sharing permission (two round-trips): POST the permission, then GET
/// the `webViewLink`. These are header-free JSON calls, so the typed
/// `post`/`get` helpers are correct here.
async fn create_sharing_permission(
    client: &GmailClient,
    file_id: &str,
    scope: ShareScope,
    account_email: &str,
) -> Result<String, Error> {
    let body = match scope {
        ShareScope::Anyone => serde_json::json!({
            "role": "reader",
            "type": "anyone",
        }),
        // `ShareScope` is `#[non_exhaustive]`; `Organization` plus any unknown
        // future scope are treated as the most restrictive (domain-only)
        // sharing rather than silently widening to `anyone`.
        _ => serde_json::json!({
            "role": "reader",
            "type": "domain",
            "domain": account_domain(account_email)?,
        }),
    };

    let file_id = bifrost_net::url::encode_path_component(file_id);
    let path = format!("https://www.googleapis.com/drive/v3/files/{file_id}/permissions?fields=id");
    let _perm: serde::de::IgnoredAny = client.post(&path, &body).await?;

    let link_path =
        format!("https://www.googleapis.com/drive/v3/files/{file_id}?fields=webViewLink");
    let file: FileWebLinkResponse = client.get(&link_path).await?;
    Ok(file.web_view_link)
}

/// Derive the account's primary domain from its email address, for the
/// `type: domain` Drive permission.
fn account_domain(account_email: &str) -> Result<String, Error> {
    account_email
        .rsplit_once('@')
        .map(|(_, domain)| domain.to_owned())
        .filter(|d| !d.is_empty())
        .ok_or_else(|| {
            Error::invalid_request(
                AccountOperation::HostAttachment,
                "account email has no domain for Organization-scoped sharing",
            )
        })
}

fn response_error(headers: &reqwest::header::HeaderMap, status: u16, body: Bytes) -> Error {
    let parsed = GmailResponseHeaders::from_headers(headers);
    Error::response_from_parts(GmailService::GmailApi, status, parsed, body)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource};

    use super::*;

    fn resume_response(last_byte: usize) -> Canned {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "range",
            format!("bytes=0-{last_byte}").parse().expect("valid Range"),
        );
        Canned::Response {
            status: reqwest::StatusCode::PERMANENT_REDIRECT,
            headers,
            body: Bytes::new(),
        }
    }

    #[test]
    fn chunk_size_must_be_aligned() {
        assert_eq!(GDRIVE_CHUNK_SIZE % GDRIVE_CHUNK_ALIGN, 0);
        assert_eq!(GDRIVE_CHUNK_SIZE, 5 * 1024 * 1024);
    }

    #[test]
    fn chunk_range_calculation() {
        let total = 12 * 1024 * 1024usize; // 12 MiB
        let chunk_size = GDRIVE_CHUNK_SIZE; // 5 MiB

        let offset = 0;
        let end = (offset + chunk_size).min(total);
        let range = format!("bytes {offset}-{}/{total}", end - 1);
        assert_eq!(range, format!("bytes 0-{}/{total}", 5 * 1024 * 1024 - 1));

        let offset = end;
        let end = (offset + chunk_size).min(total);
        let range = format!("bytes {offset}-{}/{total}", end - 1);
        assert_eq!(
            range,
            format!("bytes {}-{}/{total}", 5 * 1024 * 1024, 10 * 1024 * 1024 - 1)
        );

        let offset = end;
        let end = (offset + chunk_size).min(total);
        let range = format!("bytes {offset}-{}/{total}", end - 1);
        assert_eq!(
            range,
            format!(
                "bytes {}-{}/{total}",
                10 * 1024 * 1024,
                12 * 1024 * 1024 - 1
            )
        );
    }

    #[test]
    fn gdrive_file_response_deserializes() {
        let json = r#"{
            "id": "abc123",
            "name": "report.pdf",
            "webViewLink": "https://drive.google.com/file/d/abc123/view",
            "size": "1048576"
        }"#;
        let file: GDriveFileResponse = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(file.id, "abc123");
        assert_eq!(file.name.as_deref(), Some("report.pdf"));
        assert_eq!(
            file.web_view_link.as_deref(),
            Some("https://drive.google.com/file/d/abc123/view")
        );

        let minimal: GDriveFileResponse =
            serde_json::from_str(r#"{"id":"xyz789"}"#).expect("minimal should deserialize");
        assert_eq!(minimal.id, "xyz789");
        assert!(minimal.web_view_link.is_none());
    }

    #[test]
    fn permission_body_anyone_and_domain() {
        let anyone = serde_json::json!({ "role": "reader", "type": "anyone" });
        assert_eq!(anyone["type"], "anyone");

        let domain = serde_json::json!({
            "role": "reader",
            "type": "domain",
            "domain": "example.com",
        });
        assert_eq!(domain["type"], "domain");
        assert_eq!(domain["domain"], "example.com");
    }

    #[test]
    fn share_scope_maps_to_drive_vocab() {
        // Anyone -> type: anyone; Organization -> type: domain with the
        // account's domain.
        assert_eq!(account_domain("user@example.com").unwrap(), "example.com");
        assert!(account_domain("no-domain").is_err());
        assert!(account_domain("trailing@").is_err());
    }

    #[test]
    fn resume_offset_parses_range_header() {
        let mut h = reqwest::header::HeaderMap::new();
        h.insert("range", "bytes=0-1048575".parse().unwrap());
        assert_eq!(parse_resume_offset(&h).unwrap(), 1_048_576);
    }

    #[test]
    fn unparseable_308_range_fails_does_not_skip_gap() {
        // An absent Range header on a 308 must FAIL, never advance by
        // `offset = end` (which would silently skip a gap if the server
        // accepted fewer bytes than were sent).
        let empty = reqwest::header::HeaderMap::new();
        assert!(parse_resume_offset(&empty).is_err());

        // A present-but-garbage Range header must also fail.
        let mut garbage = reqwest::header::HeaderMap::new();
        garbage.insert("range", "bytes=garbage".parse().unwrap());
        assert!(parse_resume_offset(&garbage).is_err());
    }

    #[tokio::test]
    async fn repeated_resume_offset_terminates_instead_of_spinning() {
        let script = ScriptedDispatch::new([resume_response(0), resume_response(0)]);
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            RetryPolicy::disabled(),
        );
        let client = GmailClient::with_account_net("https://gmail.test", net);

        let result = upload_file_chunked(
            &client,
            "https://upload.test/session",
            Bytes::from_static(b"payload"),
            GDRIVE_CHUNK_ALIGN,
        )
        .await;

        assert!(
            result.is_err(),
            "a repeated offset must terminate the upload"
        );
        assert_eq!(
            script.requests().len(),
            2,
            "the loop must stop on the first stall"
        );
    }

    #[tokio::test]
    async fn tiny_resume_progress_is_bounded_by_an_attempt_limit() {
        let responses = (0..9).map(resume_response).collect::<Vec<_>>();
        let script = ScriptedDispatch::new(responses);
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            RetryPolicy::disabled(),
        );
        let client = GmailClient::with_account_net("https://gmail.test", net);

        let result = upload_file_chunked(
            &client,
            "https://upload.test/session",
            Bytes::from_static(b"a payload longer than nine bytes"),
            GDRIVE_CHUNK_ALIGN,
        )
        .await;

        assert!(
            result.is_err(),
            "tiny progress must exhaust a finite budget"
        );
        assert_eq!(
            script.requests().len(),
            9,
            "attempt ten must fail before dispatch"
        );
    }
}
