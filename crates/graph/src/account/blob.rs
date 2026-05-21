use bytes::Bytes;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use bifrost_types::{
    Batch, BlobCapabilities, BlobEncoding, BlobHandle, BlobId, ByteRange, Checkpoint, Error, Fatal,
    ObjectId, PageBoundary, RecoveryClass, SyncEvent,
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

pub(crate) async fn open_blob_events(
    account: GraphAccount,
    handle: BlobHandle,
) -> Vec<SyncEvent<Bytes>> {
    match open_blob_inner(&account, handle, None).await {
        Ok(events) => events,
        Err(fatal) => vec![SyncEvent::Fatal(fatal), SyncEvent::Done(None)],
    }
}

pub(crate) async fn open_blob_range_events(
    account: GraphAccount,
    handle: BlobHandle,
    range: ByteRange,
) -> Vec<SyncEvent<Bytes>> {
    match open_blob_inner(&account, handle, Some(range)).await {
        Ok(events) => events,
        Err(fatal) => vec![SyncEvent::Fatal(fatal), SyncEvent::Done(None)],
    }
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

async fn open_blob_inner(
    account: &GraphAccount,
    handle: BlobHandle,
    range: Option<ByteRange>,
) -> Result<Vec<SyncEvent<Bytes>>, Fatal> {
    let locator = decode_locator(&handle).map_err(|error| Fatal {
        recovery: RecoveryClass::Fatal,
        message: error.to_string(),
        source: Some(error),
    })?;
    if locator.kind == GraphBlobKind::Reference {
        return Ok(vec![
            SyncEvent::Warning(warning_blob_not_byte_stream(&ObjectId(locator.message_id))),
            SyncEvent::Done(None),
        ]);
    }
    if range.is_some() && !handle.capabilities.supports_range {
        return Err(Fatal {
            recovery: RecoveryClass::Fatal,
            message: "Graph blob does not support range fetches".to_string(),
            source: Some(Error::RangeNotSupported),
        });
    }

    let response = fetch_blob_response(account, &locator, range)
        .await
        .map_err(|error| graph_error_to_fatal(error, bifrost_types::CursorScope::Account))?;
    let status = response.status();
    if range.is_some() && status != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(fatal_from_recovery(
            RecoveryClass::Fatal,
            format!("Graph range request returned HTTP {status} instead of 206"),
        ));
    }
    if status == reqwest::StatusCode::METHOD_NOT_ALLOWED {
        return Ok(vec![
            SyncEvent::Warning(warning_blob_not_byte_stream(&ObjectId(locator.message_id))),
            SyncEvent::Done(None),
        ]);
    }
    if !status.is_success() {
        return Err(graph_error_to_fatal(
            format!("Graph blob request failed with HTTP {status}"),
            bifrost_types::CursorScope::Account,
        ));
    }

    let mut events = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => events.push(SyncEvent::Batch(Batch {
                items: vec![bytes],
                page_boundary: PageBoundary::Page,
                server_latency: std::time::Duration::default(),
                bytes_in: 0,
                checkpoint: None::<Checkpoint>,
            })),
            Err(error) => {
                events.push(SyncEvent::Fatal(graph_error_to_fatal(
                    format!("Graph blob stream failed: {error}"),
                    bifrost_types::CursorScope::Account,
                )));
                break;
            }
        }
    }
    events.push(SyncEvent::Done(None));
    Ok(events)
}

async fn fetch_blob_response(
    account: &GraphAccount,
    locator: &GraphBlobLocator,
    range: Option<ByteRange>,
) -> Result<reqwest::Response, String> {
    let prefix = account.client.api_path_prefix();
    let enc_message_id = urlencoding::encode(&locator.message_id);
    let enc_attachment_id = urlencoding::encode(&locator.attachment_id);
    let url = format!(
        "{}{prefix}/messages/{enc_message_id}/attachments/{enc_attachment_id}/$value",
        account.client.api_base()
    );
    let token = account.client.access_token().await;
    let mut request = account
        .client
        .http_client()
        .get(url)
        .header("Authorization", format!("Bearer {token}"));
    if let Some(range) = range {
        request = request.header(reqwest::header::RANGE, encode_range(range)?);
    }
    request
        .send()
        .await
        .map_err(|error| format!("Graph blob request failed: {error}"))
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
