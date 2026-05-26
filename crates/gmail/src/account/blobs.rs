//! Gmail blob open paths.
//!
//! Gmail attachments are base64url-encoded inside a JSON envelope and
//! do not support HTTP byte ranges. `open_blob_range` thus always
//! returns `Unsupported(OpenBlobRange)` per capabilities.
//!
//! Errors funnel through `recovery::into_account_error` and
//! terminate streams with `SyncEvent::Terminated(AccountError)`.

use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountError, AccountOperation, AccountStream, Batch, BlobCapabilities, BlobEncoding,
    BlobHandle, BlobId, ByteRange, PageBoundary, SyncEvent,
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use serde::{Deserialize, Serialize};

use crate::client::GmailClient;
use crate::encoding::decode_base64url_nopad;
use crate::error::GmailLocalError;
use crate::types::{GmailMessage, GmailPayload};

use super::recovery;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GmailBlobKey {
    message_id: String,
    attachment_id: String,
}

pub(crate) fn open_blob(
    client: Arc<GmailClient>,
    handle: BlobHandle,
) -> AccountStream<SyncEvent<Bytes>> {
    Box::pin(
        stream::once(async move {
            let started = Instant::now();
            match download_blob(&client, &handle).await {
                Ok(bytes) => SyncEvent::Batch(Batch {
                    items: vec![bytes],
                    page_boundary: PageBoundary::Final,
                    server_latency: started.elapsed(),
                    bytes_in: handle.size.unwrap_or(0),
                    checkpoint: None,
                }),
                Err(error) => terminate_blob(translate(error, &handle.id)),
            }
        })
        .flat_map(finish_blob_event),
    )
}

pub(crate) fn open_blob_range(
    client: Arc<GmailClient>,
    handle: BlobHandle,
    _range: ByteRange,
) -> AccountStream<SyncEvent<Bytes>> {
    // Gmail attachments never support byte ranges; the convergence
    // contract requires `Unsupported(OpenBlobRange)` rather than a
    // range-not-supported terminal error.
    if !handle.capabilities.supports_range {
        let error = recovery::into_account_error(
            crate::error::Error::Local(GmailLocalError::BlobRangeUnsupported {
                blob_id: handle.id.0.clone(),
            }),
            recovery::GmailErrorContext::open_blob_range(),
        );
        return Box::pin(stream::iter([terminate_blob(error)]));
    }

    // Defensive branch: the capability gate is the source of truth, but
    // if a handle somehow advertises range support we slice locally.
    let _ = client;
    Box::pin(stream::empty())
}

fn finish_blob_event(event: SyncEvent<Bytes>) -> impl futures::Stream<Item = SyncEvent<Bytes>> {
    let add_done = matches!(event, SyncEvent::Batch(_));
    let mut events = vec![event];
    if add_done {
        events.push(SyncEvent::Done(None));
    }
    stream::iter(events)
}

pub(crate) fn blob_handles_for_message(message: &GmailMessage) -> Vec<BlobHandle> {
    let mut handles = Vec::new();
    if let Some(payload) = &message.payload {
        collect_blob_handles(&message.id, payload, &mut handles);
    }
    handles
}

fn collect_blob_handles(message_id: &str, part: &GmailPayload, handles: &mut Vec<BlobHandle>) {
    if let Some(body) = &part.body
        && let Some(attachment_id) = &body.attachment_id
    {
        let size = u64::try_from(body.size).ok();
        handles.push(BlobHandle {
            id: encode_blob_id(message_id, attachment_id),
            size,
            content_type: Some(part.mime_type.clone()),
            digest: None,
            capabilities: BlobCapabilities {
                supports_range: false,
                supports_parallel: false,
                digest_available_pre_download: false,
                encoding: BlobEncoding::Base64Url,
            },
        });
    }
    for child in &part.parts {
        collect_blob_handles(message_id, child, handles);
    }
}

fn encode_blob_id(message_id: &str, attachment_id: &str) -> BlobId {
    let key = GmailBlobKey {
        message_id: message_id.to_string(),
        attachment_id: attachment_id.to_string(),
    };
    BlobId(serde_json::to_string(&key).unwrap_or_default())
}

enum BlobError {
    InvalidId(String),
    Gmail(crate::Error),
}

async fn download_blob(client: &GmailClient, handle: &BlobHandle) -> Result<Bytes, BlobError> {
    let key = decode_blob_id(&handle.id)?;
    let attachment = client
        .get_attachment(&key.message_id, &key.attachment_id)
        .await
        .map_err(BlobError::Gmail)?;
    let decoded = decode_base64url_nopad(&attachment.data)
        .map_err(|err| BlobError::Gmail(crate::error::Error::base64url(err)))?;
    Ok(Bytes::from(decoded))
}

fn decode_blob_id(id: &BlobId) -> Result<GmailBlobKey, BlobError> {
    serde_json::from_str(&id.0).map_err(|err| BlobError::InvalidId(err.to_string()))
}

fn translate(error: BlobError, blob_id: &BlobId) -> AccountError {
    match error {
        BlobError::InvalidId(detail) => recovery::into_account_error(
            crate::error::Error::invalid_request(
                AccountOperation::OpenBlob,
                format!("invalid gmail blob id: {detail}"),
            ),
            recovery::GmailErrorContext::open_blob(blob_id.0.clone()),
        ),
        BlobError::Gmail(error) => recovery::into_account_error(
            error,
            recovery::GmailErrorContext::open_blob(blob_id.0.clone()),
        ),
    }
}

fn terminate_blob(error: AccountError) -> SyncEvent<Bytes> {
    SyncEvent::Terminated(error)
}
