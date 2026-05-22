use bifrost_net::StreamingResponse;
use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use bifrost_types::{
    AccountStream, Batch, BlobCapabilities, BlobEncoding, BlobHandle, BlobId, ByteRange,
    Checkpoint, Error, ObjectId, PageBoundary, RecoveryClass, SyncEvent,
};

use super::GraphAccount;
use super::error::{fatal_from_recovery, graph_error_to_fatal, warning_blob_not_byte_stream};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum GraphBlobKind {
    File,
    Item,
    Reference,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct GraphBlobLocator {
    message_id: String,
    attachment_id: String,
    kind: GraphBlobKind,
}

pub(crate) fn open_blob_stream(
    account: GraphAccount,
    handle: BlobHandle,
) -> AccountStream<SyncEvent<Bytes>> {
    open_blob_inner_stream(account, handle, None)
}

pub(crate) fn open_blob_range_stream(
    account: GraphAccount,
    handle: BlobHandle,
    range: ByteRange,
) -> AccountStream<SyncEvent<Bytes>> {
    open_blob_inner_stream(account, handle, Some(range))
}

pub(crate) fn blob_handle_from_graph_attachment(
    message_id: &ObjectId,
    attachment: &Value,
) -> Option<BlobHandle> {
    let attachment_id = attachment.get("id")?.as_str()?.to_string();
    let odata_type = attachment
        .get("@odata.type")
        .and_then(Value::as_str)
        .unwrap_or("#microsoft.graph.fileAttachment");
    let kind = match odata_type {
        "#microsoft.graph.fileAttachment" => GraphBlobKind::File,
        "#microsoft.graph.itemAttachment" => GraphBlobKind::Item,
        "#microsoft.graph.referenceAttachment" => GraphBlobKind::Reference,
        _ => GraphBlobKind::Unknown,
    };
    let supports_range = kind == GraphBlobKind::File;
    let locator = GraphBlobLocator {
        message_id: message_id.0.clone(),
        attachment_id,
        kind,
    };
    let id = serde_json::to_string(&locator)
        .unwrap_or_else(|_| format!("{}:{}", locator.message_id, locator.attachment_id));
    let size = attachment
        .get("size")
        .and_then(Value::as_i64)
        .and_then(|size| u64::try_from(size).ok());

    Some(BlobHandle {
        id: BlobId(id),
        size,
        content_type: attachment
            .get("contentType")
            .and_then(Value::as_str)
            .map(str::to_string),
        digest: None,
        capabilities: BlobCapabilities {
            supports_range,
            supports_parallel: supports_range,
            digest_available_pre_download: false,
            encoding: BlobEncoding::Raw8Bit,
        },
    })
}

fn open_blob_inner_stream(
    account: GraphAccount,
    handle: BlobHandle,
    range: Option<ByteRange>,
) -> AccountStream<SyncEvent<Bytes>> {
    Box::pin(async_stream::stream! {
        let locator = match decode_locator(&handle) {
            Ok(locator) => locator,
            Err(error) => {
                yield SyncEvent::Fatal(bifrost_types::Fatal {
                    recovery: RecoveryClass::Fatal,
                    message: error.to_string(),
                    source: Some(error),
                });
                yield SyncEvent::Done(None);
                return;
            }
        };
        if locator.kind == GraphBlobKind::Reference {
            yield SyncEvent::Warning(warning_blob_not_byte_stream(&ObjectId(locator.message_id)));
            yield SyncEvent::Done(None);
            return;
        }
        if range.is_some() && !handle.capabilities.supports_range {
            yield SyncEvent::Fatal(bifrost_types::Fatal {
                recovery: RecoveryClass::Fatal,
                message: "Graph blob does not support range fetches".to_string(),
                source: Some(Error::RangeNotSupported),
            });
            yield SyncEvent::Done(None);
            return;
        }

        let response = match fetch_blob_response(&account, &locator, range).await {
            Ok(response) => response,
            Err(BlobFetchError::MethodNotAllowed) => {
                yield SyncEvent::Warning(warning_blob_not_byte_stream(&ObjectId(locator.message_id)));
                yield SyncEvent::Done(None);
                return;
            }
            Err(BlobFetchError::Failed(error)) => {
                yield SyncEvent::Fatal(graph_error_to_fatal(
                    error,
                    bifrost_types::CursorScope::Account,
                ));
                yield SyncEvent::Done(None);
                return;
            }
        };
        let status = response.status();
        if range.is_some() && status != reqwest::StatusCode::PARTIAL_CONTENT {
            yield SyncEvent::Fatal(fatal_from_recovery(
                RecoveryClass::Fatal,
                format!("Graph range request returned HTTP {status} instead of 206"),
            ));
            yield SyncEvent::Done(None);
            return;
        }
        if !status.is_success() {
            yield SyncEvent::Fatal(graph_error_to_fatal(
                format!("Graph blob request failed with HTTP {status}"),
                bifrost_types::CursorScope::Account,
            ));
            yield SyncEvent::Done(None);
            return;
        }

        let mut stream = response.body;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => yield SyncEvent::Batch(Batch {
                    items: vec![bytes],
                    page_boundary: PageBoundary::Page,
                    server_latency: std::time::Duration::default(),
                    bytes_in: 0,
                    checkpoint: None::<Checkpoint>,
                }),
                Err(error) => {
                    yield SyncEvent::Fatal(graph_error_to_fatal(
                        format!("Graph blob stream failed: {error}"),
                        bifrost_types::CursorScope::Account,
                    ));
                    break;
                }
            }
        }
        yield SyncEvent::Done(None);
    })
}

async fn fetch_blob_response(
    account: &GraphAccount,
    locator: &GraphBlobLocator,
    range: Option<ByteRange>,
) -> Result<StreamingResponse, BlobFetchError> {
    let prefix = account.client.api_path_prefix();
    let enc_message_id = bifrost_net::url::encode_component(&locator.message_id);
    let enc_attachment_id = bifrost_net::url::encode_component(&locator.attachment_id);
    let url = format!(
        "{}{prefix}/messages/{enc_message_id}/attachments/{enc_attachment_id}/$value",
        account.client.api_base()
    );
    let mut request = account.client.account_net().get(&url);
    if let Some(range) = range {
        let range = encode_range(range).map_err(BlobFetchError::Failed)?;
        request = request.header(reqwest::header::RANGE.as_str(), &range);
    }
    request.send_streaming().await.map_err(BlobFetchError::from)
}

enum BlobFetchError {
    MethodNotAllowed,
    Failed(String),
}

impl From<bifrost_net::Error> for BlobFetchError {
    fn from(error: bifrost_net::Error) -> Self {
        match error {
            bifrost_net::Error::Status { code, .. }
                if code == reqwest::StatusCode::METHOD_NOT_ALLOWED =>
            {
                Self::MethodNotAllowed
            }
            bifrost_net::Error::Status { code, body, .. } => Self::Failed(format!(
                "Graph blob request failed with HTTP {code}: {}",
                String::from_utf8_lossy(body.as_ref())
            )),
            other => Self::Failed(format!("Graph blob request failed: {other}")),
        }
    }
}

fn decode_locator(handle: &BlobHandle) -> Result<GraphBlobLocator, Error> {
    serde_json::from_str(&handle.id.0).map_err(|error| {
        Error::Other(format!(
            "Graph blob handle is not an account blob locator: {error}"
        ))
    })
}

fn encode_range(range: ByteRange) -> Result<String, String> {
    match range.length {
        Some(0) => Err("zero-length blob range".to_string()),
        Some(length) => {
            let end = range
                .start
                .checked_add(length)
                .and_then(|end| end.checked_sub(1))
                .ok_or_else(|| "blob range overflow".to_string())?;
            Ok(format!("bytes={}-{}", range.start, end))
        }
        None => Ok(format!("bytes={}-", range.start)),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn file_attachment_handle_supports_range() {
        let handle = blob_handle_from_graph_attachment(
            &ObjectId("m1".to_string()),
            &json!({
                "id": "a1",
                "@odata.type": "#microsoft.graph.fileAttachment",
                "size": 42,
                "contentType": "text/plain"
            }),
        )
        .expect("handle expected");
        assert!(handle.capabilities.supports_range);
        assert_eq!(handle.size, Some(42));
    }

    #[test]
    fn reference_attachment_handle_is_not_byte_stream() {
        let handle = blob_handle_from_graph_attachment(
            &ObjectId("m1".to_string()),
            &json!({
                "id": "a1",
                "@odata.type": "#microsoft.graph.referenceAttachment"
            }),
        )
        .expect("handle expected");
        let locator = decode_locator(&handle).expect("locator expected");
        assert_eq!(locator.kind, GraphBlobKind::Reference);
        assert!(!handle.capabilities.supports_range);
    }
}
