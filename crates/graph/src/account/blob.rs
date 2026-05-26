use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use bifrost_types::{
    AccountErrorBuilder, AccountErrorKind, AccountOperation, AccountStream, Batch,
    BlobCapabilities, BlobEncoding, BlobHandle, BlobId, ByteRange, Cause, Checkpoint,
    DiagnosticText, ErrorScope, ObjectId, PageBoundary, Protocol, ProtocolErrorKind, Provider,
    RequestCause, SyncEvent, WireCause,
};

use super::GraphAccount;
use super::error::warning_blob_not_byte_stream;
use super::graph_error::{GraphErrorContext, into_account_error};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum GraphBlobKind {
    File,
    Item,
    Reference,
    Unknown,
}

// Graph blob ids need message id, attachment id, and attachment kind inside bifrost_types::BlobId.
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
                // Malformed blob handle - this is a protocol contract
                // violation; the locator was minted by this crate.
                let account_error = AccountErrorBuilder::new(
                    AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
                    Cause::Wire(WireCause::MalformedResponse {
                        protocol: Protocol::Graph,
                        detail: Some(DiagnosticText::support_only(error)),
                    }),
                )
                .operation(AccountOperation::OpenBlob)
                .provider(Provider::Microsoft)
                .protocol(Protocol::Graph)
                .build();
                yield SyncEvent::Terminated(account_error);
                yield SyncEvent::Done(None);
                return;
            }
        };
        if locator.kind == GraphBlobKind::Reference {
            yield SyncEvent::Warning(warning_blob_not_byte_stream(&ObjectId(locator.message_id)));
            yield SyncEvent::Done(None);
            return;
        }
        let op = if range.is_some() {
            AccountOperation::OpenBlobRange
        } else {
            AccountOperation::OpenBlob
        };
        if range.is_some() && !handle.capabilities.supports_range {
            let account_error = AccountErrorBuilder::new(
                AccountErrorKind::Unsupported(AccountOperation::OpenBlobRange),
                Cause::Request(RequestCause::Unsupported {
                    operation: AccountOperation::OpenBlobRange,
                }),
            )
            .operation(AccountOperation::OpenBlobRange)
            .provider(Provider::Microsoft)
            .protocol(Protocol::Graph)
            .scope(ErrorScope::Account)
            .build();
            yield SyncEvent::Terminated(account_error);
            yield SyncEvent::Done(None);
            return;
        }

        let mut stream = match fetch_blob_stream(&account, &locator, range).await {
            Ok(stream) => stream,
            Err(BlobFetchError::MethodNotAllowed) => {
                yield SyncEvent::Warning(warning_blob_not_byte_stream(&ObjectId(locator.message_id)));
                yield SyncEvent::Done(None);
                return;
            }
            Err(BlobFetchError::Failed(error)) => {
                let ctx = GraphErrorContext::graph(op).with_scope(ErrorScope::Account);
                yield SyncEvent::Terminated(into_account_error(*error, ctx));
                yield SyncEvent::Done(None);
                return;
            }
        };

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
                    let account_error = AccountErrorBuilder::new(
                        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
                        Cause::Wire(WireCause::MalformedResponse {
                            protocol: Protocol::Graph,
                            detail: Some(DiagnosticText::support_only(format!(
                                "Graph blob stream error: {error}"
                            ))),
                        }),
                    )
                    .operation(op)
                    .provider(Provider::Microsoft)
                    .protocol(Protocol::Graph)
                    .scope(ErrorScope::Account)
                    .build();
                    yield SyncEvent::Terminated(account_error);
                    break;
                }
            }
        }
        yield SyncEvent::Done(None);
    })
}

async fn fetch_blob_stream(
    account: &GraphAccount,
    locator: &GraphBlobLocator,
    range: Option<ByteRange>,
) -> Result<bifrost_net::ByteStream, BlobFetchError> {
    let prefix = account.client.api_path_prefix();
    let enc_message_id = bifrost_net::url::encode_component(&locator.message_id);
    let enc_attachment_id = bifrost_net::url::encode_component(&locator.attachment_id);
    let url = format!(
        "{}{prefix}/messages/{enc_message_id}/attachments/{enc_attachment_id}/$value",
        account.client.api_base()
    );
    let account_net = account.client.account_net().ok_or_else(|| {
        BlobFetchError::Failed(Box::new(crate::error::GraphError::Net(
            bifrost_net::Error::Network {
                message: "Graph client is not attached to an account".to_string(),
                transmission_state: bifrost_types::TransmissionState::Unsent,
                source: None,
            },
        )))
    })?;
    account_net
        .download_stream(&url, range)
        .await
        .map_err(BlobFetchError::from)
}

enum BlobFetchError {
    MethodNotAllowed,
    Failed(Box<crate::error::GraphError>),
}

impl From<bifrost_net::Error> for BlobFetchError {
    fn from(error: bifrost_net::Error) -> Self {
        match error {
            bifrost_net::Error::Status { code, .. }
                if code == reqwest::StatusCode::METHOD_NOT_ALLOWED =>
            {
                Self::MethodNotAllowed
            }
            // All other net errors flow through the typed GraphError boundary.
            other => Self::Failed(Box::new(crate::error::GraphError::Net(other))),
        }
    }
}

fn decode_locator(handle: &BlobHandle) -> Result<GraphBlobLocator, String> {
    serde_json::from_str(&handle.id.0)
        .map_err(|error| format!("Graph blob handle is not an account blob locator: {error}"))
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
