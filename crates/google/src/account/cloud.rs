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
//! The UPLOAD step has an equivalent, and it is handled here rather than left to
//! the consumer. A resumable session is server-side state created before the
//! first byte is sent, and Drive holds an incomplete one for about a week. Any
//! failure between `create_upload_session` and a completed upload therefore
//! leaves a partial upload behind, so the upload step cancels its own session on
//! the way out (`DELETE` against the session URI) before returning the original
//! error. Cancellation is one bounded, un-retried request whose own failure is
//! swallowed: it must never replace or delay the error the caller actually needs.
//!
//! Two things this deliberately does NOT do. It does not reclassify the failure
//! as `Protocol(PartialResponse)` / `Acknowledged` the way the cross-calendar
//! event move does - that vocabulary says "the first leg landed and a consumer
//! can go look at the target", and here there is no target: Drive publishes no
//! file until the upload completes, so a `CheckTarget` / `DedupeByClientId`
//! directive would send a consumer looking for something that provably does not
//! exist, and would demote a transient transport failure out of the retry lane
//! it belongs in. It also does not resume the session on a later call: the
//! session URI is per-call state that no `HostAttachment` request carries back
//! in, so resumption would require a published surface change.
//!
//! Where cancellation itself fails, the returned `AccountError` is decorated
//! with support-only text saying the session was left behind, so a support
//! export can tell "cleaned up" from "expires in a week". The decoration goes
//! through `into_builder`, which preserves the kind and hence the recovery
//! class. The session URI is pre-authenticated and is deliberately NOT written
//! into that text.
//!
//! Residual, accepted: a caller who drops the returned future mid-upload gets no
//! cancellation at all, because a `Drop` impl cannot await. The alternative - a
//! guard that spawns the DELETE from `drop` - trades an expiring server-side
//! session for a detached task outliving the account handle, which is a worse
//! leak in a crate whose shutdown story is the caller dropping the future.
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
use std::time::Duration;

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, CloudUploadMeta, DiagnosticText,
    HostedAttachment, ShareScope,
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

/// Wall-clock bound on the session-cancel request. The account default request
/// timeout is `None`, so without an explicit bound a cleanup DELETE against an
/// unresponsive host would hold the caller's already-failed upload open
/// indefinitely. Cleanup is allowed to fail; it is not allowed to hang.
const GDRIVE_CANCEL_TIMEOUT: Duration = Duration::from_secs(30);

/// Support-only marker recorded when an upload failed AND the session cancel
/// also failed, so Drive keeps the partial upload until it expires.
const ABANDONED_SESSION_TEXT: &str = "google drive resumable upload session could not be cancelled after the upload failed; \
     the partial upload remains on the drive until Drive expires it (about one week)";

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

/// What became of the resumable session on a failing path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionDisposition {
    /// No session was open when the failure happened - it had not been created
    /// yet, or the upload had already completed and closed it.
    NotOpen,
    /// The session was open and the cancel request was accepted, so no partial
    /// upload is left on the drive.
    Cancelled,
    /// The session was open and could not be cancelled. Drive holds the partial
    /// upload until it expires.
    Abandoned,
}

async fn run(
    client: &GmailClient,
    account_email: &str,
    bytes: Bytes,
    meta: CloudUploadMeta,
) -> Result<HostedAttachment, AccountError> {
    match upload_and_link(client, account_email, bytes, meta).await {
        Ok(hosted) => Ok(hosted),
        Err((error, disposition)) => {
            let error = into_account_error(error, ctx());
            Err(match disposition {
                SessionDisposition::NotOpen | SessionDisposition::Cancelled => error,
                // Decoration, not reclassification: the kind and primary cause
                // ride through unchanged, so `recovery` derives to exactly what
                // it would have without the note.
                SessionDisposition::Abandoned => error
                    .into_builder()
                    .text(DiagnosticText::support_only(ABANDONED_SESSION_TEXT))
                    .try_build()
                    .expect("decorating an existing classification preserves its invariants"),
            })
        }
    }
}

fn ctx() -> GmailErrorContext {
    GmailErrorContext::base(AccountOperation::HostAttachment)
}

async fn upload_and_link(
    client: &GmailClient,
    account_email: &str,
    bytes: Bytes,
    meta: CloudUploadMeta,
) -> Result<HostedAttachment, (Error, SessionDisposition)> {
    if meta.size != bytes.len() as u64 {
        return Err((
            Error::invalid_request(
                AccountOperation::HostAttachment,
                format!(
                    "declared size {} does not match payload length {}",
                    meta.size,
                    bytes.len()
                ),
            ),
            SessionDisposition::NotOpen,
        ));
    }

    let upload_url = create_upload_session(client, &meta)
        .await
        .map_err(|e| (e, SessionDisposition::NotOpen))?;

    // From here until the upload completes there is server-side state to clean
    // up, so every exit on this leg goes through the cancel.
    let file_id = match upload_file_chunked(client, &upload_url, bytes, GDRIVE_CHUNK_SIZE).await {
        Ok(file_id) => file_id,
        Err(error) => {
            let disposition = cancel_upload_session(client, &upload_url).await;
            return Err((error, disposition));
        }
    };

    // The session closed itself when the final chunk was accepted; a failure
    // from here leaves an uploaded-but-unlinked file, not a partial session,
    // and the DELETE below would not address it.
    let share_url = create_sharing_permission(client, &file_id, meta.scope, account_email)
        .await
        .map_err(|e| (e, SessionDisposition::NotOpen))?;

    Ok(HostedAttachment::new(share_url, file_id))
}

/// Cancel an abandoned resumable session so Drive does not hold the partial
/// upload for a week.
///
/// Deliberately one un-retried, deadline-bounded request against the
/// pre-authenticated session URI, and deliberately infallible from the caller's
/// point of view: this runs on a path that already has an error to report, and
/// the cleanup must neither replace that error nor extend the failure by a
/// retry budget's worth of backoff.
///
/// Google answers an accepted cancel with `499 Client Closed Request`, which
/// bifrost-net's retry loop surfaces as `Err(Status)` rather than
/// `Ok(Response)` - a 4xx never arrives as a success here. A `404`/`410` means
/// the session is already gone, which is the outcome we wanted. Anything else
/// is reported as abandoned.
async fn cancel_upload_session(client: &GmailClient, upload_url: &str) -> SessionDisposition {
    let outcome = client
        .account_net()
        .delete(upload_url)
        .without_bearer_auth()
        .retry(bifrost_net::RetryPolicy::disabled())
        .timeout(GDRIVE_CANCEL_TIMEOUT)
        .send()
        .await;

    match outcome {
        Ok(_) => SessionDisposition::Cancelled,
        Err(bifrost_net::Error::Status { code, .. })
            if matches!(code.as_u16(), 404 | 410 | 499) =>
        {
            SessionDisposition::Cancelled
        }
        Err(_) => SessionDisposition::Abandoned,
    }
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

    fn session_created() -> Canned {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "location",
            "https://upload.test/session/abc"
                .parse()
                .expect("valid URL"),
        );
        Canned::Response {
            status: reqwest::StatusCode::OK,
            headers,
            body: Bytes::new(),
        }
    }

    fn network_failure() -> Canned {
        Canned::Error(bifrost_net::Error::Network {
            message: "connection reset mid-chunk".to_owned(),
            transmission_state: bifrost_types::TransmissionState::InFlight,
            source: None,
        })
    }

    fn scripted_client(script: &Arc<ScriptedDispatch>) -> GmailClient {
        let net = bifrost_net::test_support::scripted_account(
            script,
            NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            RetryPolicy::disabled(),
        );
        GmailClient::with_account_net("https://gmail.test", net)
    }

    fn upload_meta() -> CloudUploadMeta {
        CloudUploadMeta::new("report.pdf", "application/pdf", 7, ShareScope::Anyone)
    }

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

    /// A `Net` failure mid-upload must not walk away from the resumable
    /// session: Drive would hold the partial upload for about a week.
    #[tokio::test]
    async fn net_failure_mid_upload_cancels_the_resumable_session() {
        let script = ScriptedDispatch::new([
            session_created(),
            network_failure(),
            Canned::Response {
                status: reqwest::StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::new(),
            },
        ]);
        let client = scripted_client(&script);

        let error = run(
            &client,
            "user@example.com",
            Bytes::from_static(b"payload"),
            upload_meta(),
        )
        .await
        .expect_err("a dropped connection mid-chunk fails the upload");

        let requests = script.requests();
        assert_eq!(
            requests.len(),
            3,
            "session create, the failing chunk PUT, and the cancel DELETE"
        );
        assert_eq!(
            requests[2].method,
            reqwest::Method::DELETE,
            "the session is cancelled with a DELETE"
        );
        assert_eq!(
            requests[2].url.as_str(),
            "https://upload.test/session/abc",
            "the cancel targets the session URI, not the create endpoint"
        );
        assert_eq!(script.remaining(), 0, "the whole script was consumed");
        assert!(
            !requests[2]
                .headers
                .contains_key(reqwest::header::AUTHORIZATION),
            "the session URI is pre-authenticated, like the chunk PUT"
        );

        let consented = error.support_consented();
        assert!(
            !consented.support_text.contains(&ABANDONED_SESSION_TEXT),
            "a session that was cancelled must not be reported as abandoned"
        );
    }

    /// The cancel is best-effort. When it fails too, the original error is
    /// still what the caller gets - decorated so a support export can tell a
    /// cleaned-up failure from one that left a partial upload behind.
    #[tokio::test]
    async fn a_failed_cancel_is_recorded_as_an_abandoned_session() {
        let script =
            ScriptedDispatch::new([session_created(), network_failure(), network_failure()]);
        let client = scripted_client(&script);

        let error = run(
            &client,
            "user@example.com",
            Bytes::from_static(b"payload"),
            upload_meta(),
        )
        .await
        .expect_err("the upload still failed");

        assert_eq!(script.requests().len(), 3, "the cancel was attempted once");
        let consented = error.support_consented();
        assert!(
            consented.support_text.contains(&ABANDONED_SESSION_TEXT),
            "an uncancellable session must be recorded, got {:?}",
            consented.support_text
        );
        assert!(
            !consented
                .support_text
                .iter()
                .any(|text| text.contains("upload.test/session")),
            "the pre-authenticated session URI must not reach a support export"
        );
    }

    /// Drive answers an accepted cancel with 499, which bifrost-net's retry
    /// loop surfaces as `Err(Status)` and never as `Ok(Response)`. Reading that
    /// as a failed cancel would report every successful cleanup as abandoned.
    #[tokio::test]
    async fn a_499_cancel_response_counts_as_a_cancelled_session() {
        let script = ScriptedDispatch::new([
            session_created(),
            network_failure(),
            Canned::Response {
                status: reqwest::StatusCode::from_u16(499).expect("valid status"),
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::new(),
            },
        ]);
        let client = scripted_client(&script);

        let error = run(
            &client,
            "user@example.com",
            Bytes::from_static(b"payload"),
            upload_meta(),
        )
        .await
        .expect_err("the upload still failed");

        assert!(
            !error
                .support_consented()
                .support_text
                .contains(&ABANDONED_SESSION_TEXT),
            "499 Client Closed Request is Drive accepting the cancel"
        );
    }

    /// The cancel must not touch the recovery classification. A transport drop
    /// on a non-idempotent upload reconciles; adding cleanup text to it must
    /// leave that answer exactly where it was.
    #[tokio::test]
    async fn cleanup_does_not_change_the_recovery_class() {
        let cancelled = ScriptedDispatch::new([
            session_created(),
            network_failure(),
            Canned::Response {
                status: reqwest::StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::new(),
            },
        ]);
        let abandoned =
            ScriptedDispatch::new([session_created(), network_failure(), network_failure()]);

        let a = run(
            &scripted_client(&cancelled),
            "user@example.com",
            Bytes::from_static(b"payload"),
            upload_meta(),
        )
        .await
        .expect_err("upload failed");
        let b = run(
            &scripted_client(&abandoned),
            "user@example.com",
            Bytes::from_static(b"payload"),
            upload_meta(),
        )
        .await
        .expect_err("upload failed");

        assert_eq!(a.kind(), b.kind(), "decoration must not reclassify");
        assert_eq!(
            a.message_key(),
            b.message_key(),
            "decoration must not move the telemetry key"
        );
        assert_eq!(
            format!("{:?}", a.recovery()),
            format!("{:?}", b.recovery()),
            "decoration must not change what the engine does next"
        );
    }
}
