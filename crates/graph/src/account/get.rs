use std::collections::HashSet;

use bifrost_types::{
    AccountOperation, AccountStream, Batch, BatchFailure, BatchItemId, BatchSuccess, Checkpoint,
    CursorScope, ErrorScope, FolderId, HydratedObject, HydratedObjectKind, ItemOutcome,
    MembershipScope, ObjectId, ObjectType, PageBoundary, Projection, SyncEvent,
};
use bytes::Bytes;
use futures::StreamExt;
use reqwest::StatusCode;
use reqwest::header::HeaderMap;
use serde_json::Value;

use crate::error::GraphResponseError;
use crate::types::{BatchRequest, BatchRequestItem, BatchResponse, MESSAGE_SELECT};

use super::GraphAccount;
use super::blob::blob_handle_from_graph_attachment;
use super::graph_error::{GraphErrorContext, into_account_error, response_to_account_error_pub};
use super::inventory::{graph_etag, inventory_entry_from_value};

pub(crate) fn get_stream(
    account: GraphAccount,
    mut ids: AccountStream<ObjectId>,
    projection: Projection,
) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
    Box::pin(async_stream::stream! {
        let max_items = account.capabilities.batching_policy.max_items.max(1);
        let mut chunk = Vec::with_capacity(max_items);
        while let Some(id) = ids.next().await {
            chunk.push(id);
            if chunk.len() >= max_items {
                match fetch_batch(&account, &chunk, projection).await {
                    Ok(batch_events) => {
                        for event in batch_events {
                            yield event;
                        }
                    }
                    Err(error) => {
                        let ctx = GraphErrorContext::graph(AccountOperation::Hydrate)
                            .with_scope(ErrorScope::Account);
                        yield SyncEvent::Terminated(into_account_error(error, ctx));
                        yield SyncEvent::Done(None);
                        return;
                    }
                }
                chunk.clear();
            }
        }

        if !chunk.is_empty() {
            match fetch_batch(&account, &chunk, projection).await {
                Ok(batch_events) => {
                    for event in batch_events {
                        yield event;
                    }
                }
                Err(error) => {
                    let ctx = GraphErrorContext::graph(AccountOperation::Hydrate)
                        .with_scope(ErrorScope::Account);
                    yield SyncEvent::Terminated(into_account_error(error, ctx));
                    yield SyncEvent::Done(None);
                    return;
                }
            }
        }

        yield SyncEvent::Done(None);
    })
}

/// Project a Graph `/$batch` response into `ItemOutcome` envelopes.
/// Per-item 2xx hydrate; per-item 4xx/5xx emit `ItemOutcome::Failed`
/// with a structured `AccountError` (Protocol::Graph, AttemptCause
/// Acknowledged, classified `RecoveryClass`). Locally-invalid items
/// (no body where one is required) also emit `Failed` rather than
/// poisoning the rest of the batch.
async fn fetch_batch(
    account: &GraphAccount,
    ids: &[ObjectId],
    projection: Projection,
) -> Result<Vec<SyncEvent<ItemOutcome<HydratedObject>>>, crate::error::GraphError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let select = select_for_projection(projection);
    let requests = ids
        .iter()
        .enumerate()
        .map(|(index, id)| {
            let enc_id = bifrost_net::url::encode_component(&id.0);
            BatchRequestItem {
                id: index.to_string(),
                method: "GET".to_string(),
                url: format!(
                    "{}/messages/{enc_id}?$select={select}",
                    account.client.api_path_prefix()
                ),
                body: None,
                headers: None,
            }
        })
        .collect();
    let request = BatchRequest { requests };
    let response: BatchResponse = account.client.post_batch(&request).await?;
    let mut outcomes: Vec<ItemOutcome<HydratedObject>> = Vec::new();
    let mut etags = Vec::new();

    for item in response.responses {
        let index = item.id.parse::<usize>().ok();
        let id = index
            .and_then(|idx| ids.get(idx))
            .cloned()
            .unwrap_or_else(|| ObjectId(item.id.clone()));
        let batch_id = BatchItemId(id.0.clone());
        if !(200..=299).contains(&item.status) {
            // Per-item failure: build a structured AccountError via
            // the same response classifier the top-level boundary
            // uses. Headers and body flow through so retry-after,
            // throttle scope, and inner error envelope are preserved.
            let headers = item
                .headers
                .as_ref()
                .map(reconstruct_headers)
                .unwrap_or_default();
            let body = item
                .body
                .as_ref()
                .map(|v| Bytes::from(serde_json::to_vec(v).unwrap_or_default()))
                .unwrap_or_default();
            let response = GraphResponseError::from_response(
                StatusCode::from_u16(item.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                headers,
                body,
            );
            let ctx = GraphErrorContext::graph(AccountOperation::Hydrate)
                .with_scope(ErrorScope::Message { id: id.0.clone() });
            let error = response_to_account_error_pub(response, &ctx);
            outcomes.push(ItemOutcome::Failed(BatchFailure::new(batch_id, error)));
            continue;
        }
        let Some(body) = item.body else {
            // 2xx with no body where a hydration was expected: the
            // provider promised a payload it did not ship. Surface
            // as a per-item failure so the consumer can decide
            // whether to retry or drop the id.
            let error = super::graph_error::protocol_violation(
                bifrost_types::ProtocolErrorKind::MissingField,
                AccountOperation::Hydrate,
                Some(ErrorScope::Message { id: id.0.clone() }),
                format!("Graph $batch GET for {} returned 2xx with no body", id.0),
            );
            outcomes.push(ItemOutcome::Failed(BatchFailure::new(batch_id, error)));
            continue;
        };
        if let Some(etag) = graph_etag(&body) {
            etags.push((id.0.clone(), etag));
        }
        let hydrated = hydrated_from_value(id, &body, projection);
        outcomes.push(ItemOutcome::Succeeded(BatchSuccess::new(
            batch_id, hydrated,
        )));
    }

    if !etags.is_empty() {
        let mut cache = account.etag_index.write().await;
        for (id, etag) in etags {
            cache.insert(id, etag);
        }
    }

    Ok(vec![SyncEvent::Batch(Batch {
        items: outcomes,
        page_boundary: PageBoundary::Page,
        server_latency: std::time::Duration::default(),
        bytes_in: 0,
        checkpoint: None::<Checkpoint>,
    })])
}

fn reconstruct_headers(h: &std::collections::HashMap<String, String>) -> HeaderMap {
    let mut hm = HeaderMap::new();
    for (k, v) in h {
        if let (Ok(name), Ok(val)) = (
            reqwest::header::HeaderName::from_bytes(k.as_bytes()),
            reqwest::header::HeaderValue::from_str(v),
        ) {
            hm.insert(name, val);
        }
    }
    hm
}

fn hydrated_from_value(id: ObjectId, value: &Value, projection: Projection) -> HydratedObject {
    let kind = match projection {
        Projection::FlagsOnly => HydratedObjectKind::FlagsOnly(flags_from_value(value)),
        Projection::Metadata => {
            let scope = CursorScope::FolderType {
                folder: FolderId(
                    value
                        .get("parentFolderId")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                ),
                ty: ObjectType::Email,
            };
            inventory_entry_from_value(&scope, value)
                .map(HydratedObjectKind::Metadata)
                .unwrap_or_else(|| HydratedObjectKind::FlagsOnly(flags_from_value(value)))
        }
        Projection::Headers
        | Projection::Preview(_)
        | Projection::TextOnly
        | Projection::Full
        | Projection::FullWithBlobs => {
            let bytes = serde_json::to_vec(value).unwrap_or_default();
            HydratedObjectKind::RawMime(Bytes::from(bytes))
        }
        _ => HydratedObjectKind::FlagsOnly(flags_from_value(value)),
    };
    let blobs = value
        .get("attachments")
        .and_then(Value::as_array)
        .map(|attachments| {
            attachments
                .iter()
                .filter_map(|attachment| blob_handle_from_graph_attachment(&id, attachment))
                .collect()
        })
        .unwrap_or_default();

    HydratedObject { id, kind, blobs }
}

fn flags_from_value(value: &Value) -> HashSet<String> {
    let mut flags = HashSet::new();
    if value
        .get("isRead")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        flags.insert("\\seen".to_string());
    }
    if value
        .get("flag")
        .and_then(|flag| flag.get("flagStatus"))
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("flagged"))
    {
        flags.insert("\\flagged".to_string());
    }
    if let Some(categories) = value.get("categories").and_then(Value::as_array) {
        for category in categories {
            if let Some(category) = category.as_str() {
                flags.insert(format!("category:{category}"));
            }
        }
    }
    flags
}

fn select_for_projection(projection: Projection) -> &'static str {
    match projection {
        Projection::FlagsOnly => "id,isRead,categories,flag,changeKey,parentFolderId",
        Projection::Metadata | Projection::Headers | Projection::Preview(_) => MESSAGE_SELECT,
        Projection::TextOnly | Projection::Full | Projection::FullWithBlobs => {
            "id,body,uniqueBody,attachments,isRead,categories,flag,changeKey,parentFolderId"
        }
        _ => "id,isRead,categories,flag,changeKey,parentFolderId",
    }
}

pub(crate) fn folder_destination(destination: MembershipScope) -> Option<FolderId> {
    match destination {
        MembershipScope::Folder(folder) => Some(folder),
        _ => None,
    }
}
