//! OneDrive large-attachment hosting: resumable upload + sharing link.
//!
//! COPY-AND-ADAPTed from ratatoskr's `graph/onedrive.rs`. The pure pieces
//! (serde structs, `encode_onedrive_path`, the chunk-range math) copy cleanly;
//! the transport bodies are rewritten onto bifrost-net. The ratatoskr source
//! used `reqwest::Client` directly for the chunk PUT and threaded a
//! `db: &ReadDbState` ratatoskr handle into the session/link `client.post`
//! calls; the rewrite drops both. The session/link POSTs route through
//! `GraphClient::post` (AccountNet supplies the Bearer); the pre-authenticated
//! chunk PUT goes through the raw builder with `.without_bearer_auth()`.
//!
//! De-brand: ratatoskr uploaded into a `"Ratatoskr Attachments"` folder. This
//! uses a neutral, consumer-agnostic `"Attachments"` folder. A future
//! consumer-configurable folder name can replace `ATTACHMENTS_FOLDER` without
//! touching the wire path.
//!
//! Mailbox routing: hosting always uploads into the *primary* user's OneDrive
//! (`account.client`, the `/me/drive` prefix), even when the draft being
//! composed belongs to a shared/delegate mailbox (`send_as`). This is
//! deliberate - a shared Exchange mailbox has no OneDrive of its own, and the
//! attachment is shared via a link rather than embedded, so the authenticated
//! user's drive is the only sensible host. The recipient sees a link, not a
//! drive location, so the hosting mailbox is invisible downstream.

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, CloudUploadMeta, HostedAttachment, ShareScope,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::graph_error::{GraphErrorContext, into_account_error, invalid_account_error};
use crate::account::GraphAccount;
use crate::client::GraphClient;
use crate::error::{GraphError, GraphResponseError};

/// Minimum alignment for upload chunks (320 KiB per Graph API spec).
const CHUNK_ALIGNMENT: usize = 320 * 1024;

/// Default chunk size: 5 MiB. Must be a multiple of `CHUNK_ALIGNMENT`.
const DEFAULT_CHUNK_SIZE: usize = 5 * 1024 * 1024;

/// Neutral, de-branded folder the hosted attachments land in. A later
/// consumer-configurable folder name can replace this constant without
/// touching the wire path below.
const ATTACHMENTS_FOLDER: &str = "Attachments";

/// An active OneDrive resumable upload session.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UploadSession {
    upload_url: String,
    #[serde(default)]
    #[allow(dead_code)]
    expiration_date_time: String,
}

/// Request body for creating an upload session via `createUploadSession`.
#[derive(Debug, Serialize)]
struct CreateUploadSessionRequest<'a> {
    item: DriveItemUploadable<'a>,
    #[serde(rename = "@microsoft.graph.conflictBehavior")]
    conflict_behavior: &'a str,
}

/// Properties for the file being uploaded.
#[derive(Debug, Serialize)]
struct DriveItemUploadable<'a> {
    name: &'a str,
}

/// The completed drive item returned after the final upload chunk.
#[derive(Debug, Deserialize)]
struct DriveItemResponse {
    id: String,
}

/// The 202 Accepted body OneDrive returns mid-upload. `nextExpectedRanges`
/// is a list of `"start-end"` (or `"start-"`) byte ranges the server still
/// wants; the first range's start is the authoritative resume offset. The
/// server may accept fewer bytes than were sent (it is allowed to), so the
/// client must resume from the server's reported offset rather than blindly
/// advancing to the end of the chunk it just PUT.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UploadProgress {
    #[serde(default)]
    next_expected_ranges: Vec<String>,
}

impl UploadProgress {
    /// The server's authoritative resume offset: the start of the first
    /// still-expected range. `None` if the body had no parseable range.
    fn resume_offset(&self) -> Option<usize> {
        self.next_expected_ranges
            .first()
            .and_then(|range| range.split('-').next())
            .and_then(|start| start.trim().parse::<usize>().ok())
    }
}

/// Overall wall-clock budget for the whole chunked upload. A server that
/// keeps returning 202 (or hangs) without ever completing must not stall
/// the send pipeline indefinitely.
const UPLOAD_TOTAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// Response from creating a sharing link.
#[derive(Debug, Deserialize)]
struct CreateLinkResponse {
    link: SharingLink,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SharingLink {
    web_url: String,
}

/// Dispatch entry point wired from the `Account` impl.
pub(crate) fn host_attachment(
    account: GraphAccount,
    bytes: Bytes,
    meta: CloudUploadMeta,
) -> AccountFuture<Result<HostedAttachment, AccountError>> {
    Box::pin(async move {
        if meta.size != bytes.len() as u64 {
            return Err(invalid_account_error(
                AccountOperation::HostAttachment,
                format!(
                    "declared size {} does not match payload length {}",
                    meta.size,
                    bytes.len()
                ),
            ));
        }
        if bytes.is_empty() {
            return Err(invalid_account_error(
                AccountOperation::HostAttachment,
                "cannot host an empty attachment",
            ));
        }
        upload_and_link(&account, bytes, meta)
            .await
            .map_err(|e| into_account_error(e, ctx()))
    })
}

fn ctx() -> GraphErrorContext {
    GraphErrorContext::graph(AccountOperation::HostAttachment)
}

async fn upload_and_link(
    account: &GraphAccount,
    bytes: Bytes,
    meta: CloudUploadMeta,
) -> Result<HostedAttachment, GraphError> {
    let client = &account.client;
    let session = create_upload_session(client, &meta.file_name).await?;
    let item_id = upload_file_chunked(
        client,
        &session.upload_url,
        bytes,
        DEFAULT_CHUNK_SIZE,
        &account.shutdown,
    )
    .await?;
    let share_url = create_sharing_link(client, &item_id, meta.scope).await?;

    Ok(HostedAttachment::new(share_url, item_id))
}

/// Create an upload session under the de-branded `Attachments` folder. The
/// folder is created implicitly by OneDrive on first upload; `rename` conflict
/// behavior means uploads never overwrite an existing file.
async fn create_upload_session(
    client: &GraphClient,
    file_name: &str,
) -> Result<UploadSession, GraphError> {
    let encoded = encode_onedrive_path(file_name);
    let prefix = client.api_path_prefix();
    let path = format!("{prefix}/drive/root:/{ATTACHMENTS_FOLDER}/{encoded}:/createUploadSession");

    let body = CreateUploadSessionRequest {
        item: DriveItemUploadable { name: file_name },
        conflict_behavior: "rename",
    };

    client.post(&path, &body).await
}

/// Upload the payload in `chunk_size`-aligned chunks against the
/// pre-authenticated session URL.
///
/// 200/201 -> the final chunk was accepted; parse the drive item id. 202 ->
/// the server reports the next byte range it expects via `nextExpectedRanges`;
/// resume from that offset (the server may accept fewer bytes than were sent,
/// so blindly advancing `offset = end` can skip unaccepted bytes and corrupt
/// the upload). Any other status -> classified error. OneDrive's 202 resume
/// signal never enters bifrost-net's redirect path, so the Drive-specific 308
/// passthrough does not apply here. The whole loop is bounded by
/// `UPLOAD_TOTAL_TIMEOUT` and aborts if the account shutdown token fires.
async fn upload_file_chunked(
    client: &GraphClient,
    upload_url: &str,
    data: Bytes,
    chunk_size: usize,
    shutdown: &tokio_util::sync::CancellationToken,
) -> Result<String, GraphError> {
    // Defensive guard over the caller-supplied chunk size; the sole caller
    // passes the `DEFAULT_CHUNK_SIZE` constant, so this is a producer-bug
    // check, not a runtime path - a `debug_assert` keeps it out of release.
    debug_assert!(
        chunk_size != 0 && chunk_size.is_multiple_of(CHUNK_ALIGNMENT),
        "chunk_size must be a positive multiple of {CHUNK_ALIGNMENT}, got {chunk_size}"
    );

    tokio::select! {
        () = shutdown.cancelled() => Err(shutdown_during_upload()),
        result = tokio::time::timeout(
            UPLOAD_TOTAL_TIMEOUT,
            upload_chunks(client, upload_url, data, chunk_size, shutdown),
        ) => match result {
            Ok(inner) => inner,
            Err(_elapsed) => Err(malformed_response(format!(
                "OneDrive upload exceeded the {}s budget without completing",
                UPLOAD_TOTAL_TIMEOUT.as_secs()
            ))),
        },
    }
}

async fn upload_chunks(
    client: &GraphClient,
    upload_url: &str,
    data: Bytes,
    chunk_size: usize,
    shutdown: &tokio_util::sync::CancellationToken,
) -> Result<String, GraphError> {
    // Empty payloads are rejected up front in `host_attachment`, so by here the
    // data is non-empty and the loop always runs at least once.
    let total = data.len();

    let account_net = client.account_net().ok_or_else(|| {
        GraphError::Net(bifrost_net::Error::Network {
            message: "Graph client is not attached to an account".to_string(),
            transmission_state: bifrost_types::TransmissionState::Unsent,
            source: None,
        })
    })?;

    let mut offset = 0usize;
    while offset < total {
        if shutdown.is_cancelled() {
            return Err(shutdown_during_upload());
        }
        let end = (offset + chunk_size).min(total);
        let chunk = data.slice(offset..end);
        let content_range = format!("bytes {offset}-{}/{total}", end - 1);

        let response = account_net
            .put(upload_url)
            .without_bearer_auth()
            .header("Content-Range", &content_range)
            .body(chunk)
            .send()
            .await
            .map_err(GraphError::Net)?;

        let status = response.status();
        match status.as_u16() {
            200 | 201 => {
                let item: DriveItemResponse = serde_json::from_slice(response.body.as_ref())
                    .map_err(|e| GraphError::Json {
                        message: e.to_string(),
                        body: if response.body.is_empty() {
                            None
                        } else {
                            Some(response.body)
                        },
                    })?;
                return Ok(item.id);
            }
            // 202 Accepted: read the server's authoritative resume offset
            // from `nextExpectedRanges`. The server is allowed to accept
            // fewer bytes than the chunk we PUT, so resuming from `end`
            // unconditionally would skip the unaccepted tail. Fall back to
            // `end` only when the body carried no parseable range (and
            // guard against a non-advancing offset, which would spin).
            202 => {
                let progress: UploadProgress =
                    serde_json::from_slice(response.body.as_ref()).unwrap_or_default();
                let next = progress.resume_offset().unwrap_or(end);
                if next <= offset {
                    return Err(malformed_response(format!(
                        "OneDrive 202 resume offset {next} did not advance past {offset}"
                    )));
                }
                offset = next;
            }
            _ => {
                let err =
                    GraphResponseError::from_response(status, response.headers, response.body);
                return Err(GraphError::Response(err));
            }
        }
    }

    // Reached only if the server 202-acknowledged the final chunk instead of
    // returning the completed drive item (200/201) - a server protocol
    // violation, classified as a malformed response.
    Err(malformed_response(
        "upload completed without receiving a drive item response".to_string(),
    ))
}

/// The account was shut down (close / reopen) while an upload was in
/// flight. `Unsent`-state network error so the recovery mapping treats it
/// as a retryable client-side abort.
fn shutdown_during_upload() -> GraphError {
    GraphError::Net(bifrost_net::Error::Network {
        message: "OneDrive upload aborted: account shutting down".to_string(),
        transmission_state: bifrost_types::TransmissionState::Unsent,
        source: None,
    })
}

/// Create a sharing link (one round-trip): POST `createLink`, return
/// `link.webUrl`.
async fn create_sharing_link(
    client: &GraphClient,
    item_id: &str,
    scope: ShareScope,
) -> Result<String, GraphError> {
    let prefix = client.api_path_prefix();
    let path = format!("{prefix}/drive/items/{item_id}/createLink");
    let body = serde_json::json!({
        "type": "view",
        "scope": onedrive_scope(scope),
    });

    let response: CreateLinkResponse = client.post(&path, &body).await?;
    Ok(response.link.web_url)
}

/// Map the uniform `ShareScope` onto OneDrive's `createLink` scope vocabulary.
fn onedrive_scope(scope: ShareScope) -> &'static str {
    match scope {
        ShareScope::Anyone => "anonymous",
        // `ShareScope` is `#[non_exhaustive]`; `Organization` plus any unknown
        // future scope map to the most restrictive tenant-only link rather
        // than a public one.
        _ => "organization",
    }
}

/// Percent-encode characters invalid in OneDrive path segments (`%`, `#`, `?`).
/// Spaces are allowed in OneDrive paths and kept as-is.
fn encode_onedrive_path(filename: &str) -> String {
    filename
        .replace('%', "%25")
        .replace('#', "%23")
        .replace('?', "%3F")
}

/// Carry a server-protocol violation (a response that does not match the
/// resumable-upload contract) through the graph error funnel, where
/// `GraphError::Json` classifies as `Protocol(ParseFailed)` /
/// `Wire(MalformedResponse)`.
fn malformed_response(detail: String) -> GraphError {
    GraphError::Json {
        message: detail,
        body: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn onedrive_chunk_aligned() {
        assert_eq!(DEFAULT_CHUNK_SIZE % CHUNK_ALIGNMENT, 0);
        assert_eq!(DEFAULT_CHUNK_SIZE, 5 * 1024 * 1024);
    }

    #[test]
    fn encode_onedrive_path_escapes() {
        assert_eq!(encode_onedrive_path("file.txt"), "file.txt");
        assert_eq!(encode_onedrive_path("file #1.txt"), "file %231.txt");
        assert_eq!(encode_onedrive_path("100% done?.txt"), "100%25 done%3F.txt");
    }

    #[test]
    fn session_path_is_debranded() {
        // The de-branded folder must appear and the ratatoskr brand must not.
        let path = format!(
            "/me/drive/root:/{ATTACHMENTS_FOLDER}/{}:/createUploadSession",
            encode_onedrive_path("report.pdf")
        );
        assert!(path.contains("/Attachments/"), "{path}");
        assert!(!path.contains("Ratatoskr"), "{path}");
    }

    #[test]
    fn share_scope_maps_to_onedrive_scope() {
        assert_eq!(onedrive_scope(ShareScope::Anyone), "anonymous");
        assert_eq!(onedrive_scope(ShareScope::Organization), "organization");
    }

    #[test]
    fn create_link_response_deserializes() {
        let json = r#"{"link":{"webUrl":"https://1drv.ms/x/abc"}}"#;
        let resp: CreateLinkResponse = serde_json::from_str(json).expect("deserializes");
        assert_eq!(resp.link.web_url, "https://1drv.ms/x/abc");
    }

    #[test]
    fn resume_offset_reads_next_expected_range_start() {
        // The server's `nextExpectedRanges` start is the authoritative
        // resume offset, even when it is short of the chunk end the client
        // just PUT (a partially-accepted chunk).
        let progress = UploadProgress {
            next_expected_ranges: vec!["1024-".to_string()],
        };
        assert_eq!(progress.resume_offset(), Some(1024));

        let bounded = UploadProgress {
            next_expected_ranges: vec!["524288-1048575".to_string()],
        };
        assert_eq!(bounded.resume_offset(), Some(524_288));
    }

    #[test]
    fn resume_offset_none_without_ranges() {
        assert_eq!(UploadProgress::default().resume_offset(), None);
        let junk = UploadProgress {
            next_expected_ranges: vec!["-".to_string()],
        };
        assert_eq!(junk.resume_offset(), None);
    }

    #[test]
    fn upload_progress_deserializes_202_body() {
        let json = r#"{"nextExpectedRanges":["327680-"]}"#;
        let progress: UploadProgress = serde_json::from_str(json).expect("deserializes");
        assert_eq!(progress.resume_offset(), Some(327_680));
    }

    #[test]
    fn upload_session_deserializes() {
        let json = r#"{"uploadUrl":"https://upload.example/123","expirationDateTime":"2025-01-01T00:00:00Z"}"#;
        let session: UploadSession = serde_json::from_str(json).expect("deserializes");
        assert_eq!(session.upload_url, "https://upload.example/123");
    }
}
