use std::collections::HashSet;

use bifrost_types::{
    AccountStream, Batch, Checkpoint, CursorScope, FolderId, HydratedObject, HydratedObjectKind,
    MembershipScope, ObjectId, ObjectType, PageBoundary, Projection, SyncEvent, Warning,
    WarningKind,
};
use bytes::Bytes;
use futures::StreamExt;
use serde_json::Value;

use crate::types::{BatchRequest, BatchRequestItem, BatchResponse, MESSAGE_SELECT};

use super::GraphAccount;
use super::blob::blob_handle_from_graph_attachment;
use super::error::graph_error_to_fatal;
use super::inventory::{graph_etag, inventory_entry_from_value};

pub(crate) fn get_stream(
    account: GraphAccount,
    mut ids: AccountStream<ObjectId>,
    projection: Projection,
) -> AccountStream<SyncEvent<HydratedObject>> {
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
                        yield SyncEvent::Fatal(graph_error_to_fatal(error, CursorScope::Account));
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
                    yield SyncEvent::Fatal(graph_error_to_fatal(error, CursorScope::Account));
                    yield SyncEvent::Done(None);
                    return;
                }
            }
        }

        yield SyncEvent::Done(None);
    })
}

async fn fetch_batch(
    account: &GraphAccount,
    ids: &[ObjectId],
    projection: Projection,
) -> Result<Vec<SyncEvent<HydratedObject>>, String> {
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
    let mut hydrated = Vec::new();
    let mut warnings = Vec::new();
    let mut etags = Vec::new();

    for item in response.responses {
        let index = item.id.parse::<usize>().ok();
        let id = index
            .and_then(|idx| ids.get(idx))
            .cloned()
            .unwrap_or_else(|| ObjectId(item.id.clone()));
        if !(200..=299).contains(&item.status) {
            warnings.push(SyncEvent::Warning(Warning {
                kind: WarningKind::Other("graph_get_item_failed".to_string()),
                message: format!("Graph get for {} failed with HTTP {}", id.0, item.status),
                retry_count: 0,
                next_action: None,
                protocol_detail: None,
            }));
            continue;
        }
        let Some(body) = item.body else {
            continue;
        };
        if let Some(etag) = graph_etag(&body) {
            etags.push((id.0.clone(), etag));
        }
        hydrated.push(hydrated_from_value(id, &body, projection));
    }

    if !etags.is_empty() {
        let mut cache = account.etag_index.write().await;
        for (id, etag) in etags {
            cache.insert(id, etag);
        }
    }

    let mut events = warnings;
    events.push(SyncEvent::Batch(Batch {
        items: hydrated,
        page_boundary: PageBoundary::Page,
        server_latency: std::time::Duration::default(),
        bytes_in: 0,
        checkpoint: None::<Checkpoint>,
    }));
    Ok(events)
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
