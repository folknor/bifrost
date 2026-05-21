use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountStream, Batch, BlobCapabilities, BlobEncoding, BlobHandle, BlobId, ByteRange,
    Error as AccountError, PageBoundary, RecoveryClass, SyncEvent,
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use serde::{Deserialize, Serialize};

use crate::client::GmailClient;
use crate::encoding::decode_base64url_nopad;
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
                Err(OpenBlobError::Account(error)) => SyncEvent::Fatal(
                    recovery::fatal_for_account_error(error, RecoveryClass::Fatal),
                ),
                Err(OpenBlobError::Gmail(error)) => {
                    let recovery = recovery::classify_general_error(&error);
                    SyncEvent::Fatal(recovery::fatal_for_error(error, recovery))
                }
            }
        })
        .flat_map(finish_blob_event),
    )
}

pub(crate) fn open_blob_range(
    client: Arc<GmailClient>,
    handle: BlobHandle,
    range: ByteRange,
) -> AccountStream<SyncEvent<Bytes>> {
    if !handle.capabilities.supports_range {
        return Box::pin(stream::iter([SyncEvent::Fatal(
            recovery::fatal_for_account_error(
                AccountError::RangeNotSupported,
                RecoveryClass::Fatal,
            ),
        )]));
    }

    Box::pin(
        stream::once(async move {
            let started = Instant::now();
            match download_blob(&client, &handle)
                .await
                .and_then(|bytes| slice_range(bytes, range))
            {
                Ok(bytes) => SyncEvent::Batch(Batch {
                    items: vec![bytes],
                    page_boundary: PageBoundary::Final,
                    server_latency: started.elapsed(),
                    bytes_in: handle.size.unwrap_or(0),
                    checkpoint: None,
                }),
                Err(OpenBlobError::Account(error)) => SyncEvent::Fatal(
                    recovery::fatal_for_account_error(error, RecoveryClass::Fatal),
                ),
                Err(OpenBlobError::Gmail(error)) => {
                    let recovery = recovery::classify_general_error(&error);
                    SyncEvent::Fatal(recovery::fatal_for_error(error, recovery))
                }
            }
        })
        .flat_map(finish_blob_event),
    )
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
                encoding: BlobEncoding::Base64,
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

async fn download_blob(client: &GmailClient, handle: &BlobHandle) -> Result<Bytes, OpenBlobError> {
    let key = decode_blob_id(&handle.id)?;
    let attachment = client
        .get_attachment(&key.message_id, &key.attachment_id)
        .await
        .map_err(OpenBlobError::Gmail)?;
    // Gmail attachments are base64url inside JSON. The existing wire
    // method materializes that JSON string before decode, so v1 keeps
    // the documented full-buffered path.
    let decoded = decode_base64url_nopad(&attachment.data).map_err(OpenBlobError::Gmail)?;
    Ok(Bytes::from(decoded))
}

fn decode_blob_id(id: &BlobId) -> Result<GmailBlobKey, OpenBlobError> {
    serde_json::from_str(&id.0).map_err(|err| {
        OpenBlobError::Account(AccountError::Other(format!("invalid gmail blob id: {err}")))
    })
}

fn slice_range(bytes: Bytes, range: ByteRange) -> Result<Bytes, OpenBlobError> {
    let total = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if range.start > total {
        return Err(OpenBlobError::Account(AccountError::RangeOutOfBounds {
            start: range.start,
            total,
        }));
    }
    let end = match range.length {
        Some(length) => range
            .start
            .checked_add(length)
            .filter(|end| *end <= total)
            .ok_or(OpenBlobError::Account(AccountError::RangeOutOfBounds {
                start: range.start,
                total,
            }))?,
        None => total,
    };
    let start = usize::try_from(range.start).map_err(|_| {
        OpenBlobError::Account(AccountError::RangeOutOfBounds {
            start: range.start,
            total,
        })
    })?;
    let end = usize::try_from(end).map_err(|_| {
        OpenBlobError::Account(AccountError::RangeOutOfBounds {
            start: range.start,
            total,
        })
    })?;
    Ok(bytes.slice(start..end))
}

enum OpenBlobError {
    Account(AccountError),
    Gmail(crate::Error),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bifrost_types::{BlobCapabilities, BlobEncoding, BlobId};
    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn range_request_on_non_range_handle_fails_before_download() {
        let client = Arc::new(GmailClient::new("token"));
        let handle = BlobHandle {
            id: BlobId("{\"message_id\":\"m1\",\"attachment_id\":\"a1\"}".to_string()),
            size: Some(4),
            content_type: None,
            digest: None,
            capabilities: BlobCapabilities {
                supports_range: false,
                supports_parallel: false,
                digest_available_pre_download: false,
                encoding: BlobEncoding::Base64,
            },
        };

        let mut stream = open_blob_range(
            client,
            handle,
            ByteRange {
                start: 0,
                length: Some(1),
            },
        );

        let Some(SyncEvent::Fatal(fatal)) = stream.next().await else {
            panic!("expected fatal range error");
        };
        assert!(matches!(
            fatal.source,
            Some(AccountError::RangeNotSupported)
        ));
    }
}
