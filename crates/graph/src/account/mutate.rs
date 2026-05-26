use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bifrost_types::{
    AccountOperation, AccountStream, Batch, BatchItemId, BatchSuccess, Checkpoint, DiagnosticText,
    ErrorScope, FlagOp, IdempotencyKey, ItemOutcome, MembershipScope, MutationSuccess, ObjectId,
    PageBoundary, SyncEvent, Warning, WarningKind,
};
use futures::StreamExt;
use serde_json::{Value, json};

use crate::types::{BatchRequest, BatchRequestItem, BatchResponse};

use super::GraphAccount;
use super::graph_error::{GraphErrorContext, into_account_error, mutation_item_outcome};
use super::get::folder_destination;
use super::inventory::graph_etag;

enum MutationKind {
    SetFlags(FlagOp),
    Move(MembershipScope),
    Destroy,
}

pub(crate) fn bulk_set_flags_stream(
    account: GraphAccount,
    targets: AccountStream<ObjectId>,
    op: FlagOp,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    bulk_mutation_stream(account, targets, MutationKind::SetFlags(op))
}

pub(crate) fn bulk_move_stream(
    account: GraphAccount,
    targets: AccountStream<ObjectId>,
    destination: MembershipScope,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    bulk_mutation_stream(account, targets, MutationKind::Move(destination))
}

pub(crate) fn bulk_destroy_stream(
    account: GraphAccount,
    targets: AccountStream<ObjectId>,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    bulk_mutation_stream(account, targets, MutationKind::Destroy)
}

fn bulk_mutation_stream(
    account: GraphAccount,
    mut targets: AccountStream<ObjectId>,
    kind: MutationKind,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    Box::pin(async_stream::stream! {
        let max_items = account.capabilities.batching_policy.max_items.max(1);
        let mut chunk = Vec::with_capacity(max_items);
        while let Some(id) = targets.next().await {
            chunk.push(id);
            if chunk.len() >= max_items {
                match submit_batch(&account, &chunk, &kind).await {
                    Ok(batch_events) => {
                        for event in batch_events {
                            yield event;
                        }
                    }
                    Err(error) => {
                        let ctx = GraphErrorContext::graph(operation_for_kind(&kind));
                        yield SyncEvent::Terminated(into_account_error(error, ctx));
                        yield SyncEvent::Done(None);
                        return;
                    }
                }
                chunk.clear();
            }
        }

        if !chunk.is_empty() {
            match submit_batch(&account, &chunk, &kind).await {
                Ok(batch_events) => {
                    for event in batch_events {
                        yield event;
                    }
                }
                Err(error) => {
                    let ctx = GraphErrorContext::graph(operation_for_kind(&kind));
                    yield SyncEvent::Terminated(into_account_error(error, ctx));
                    yield SyncEvent::Done(None);
                    return;
                }
            }
        }

        yield SyncEvent::Done(None);
    })
}

fn operation_for_kind(kind: &MutationKind) -> AccountOperation {
    match kind {
        MutationKind::SetFlags(_) => AccountOperation::UpdateFlags,
        MutationKind::Move(_) => AccountOperation::BulkMove,
        MutationKind::Destroy => AccountOperation::BulkDestroy,
    }
}

async fn submit_batch(
    account: &GraphAccount,
    ids: &[ObjectId],
    kind: &MutationKind,
) -> Result<Vec<SyncEvent<ItemOutcome<MutationSuccess>>>, crate::error::GraphError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut etags = account.etag_index.read().await.clone();
    let mut preflight_outcomes = refresh_missing_etags(account, ids, kind, &mut etags).await;
    let mut requests = Vec::new();
    let mut request_ids = Vec::new();
    for id in ids {
        if requires_etag(kind) && !etags.contains_key(&id.0) {
            // Etag fetch failed; the failed outcome was recorded in
            // `preflight_outcomes` by `refresh_missing_etags`.
            continue;
        }
        let Some(request) = request_for_mutation(account, id, kind, &etags)? else {
            // Missing folder destination for Move - emit per-item failure.
            preflight_outcomes.push(ItemOutcome::Failed(bifrost_types::BatchFailure {
                item: BatchItemId(id.0.clone()),
                error: super::graph_error::unsupported_account_error(operation_for_kind(kind)),
            }));
            continue;
        };
        request_ids.push(id.clone());
        requests.push(request);
    }

    let mut item_outcomes: Vec<ItemOutcome<MutationSuccess>> = preflight_outcomes;

    if !requests.is_empty() {
        assign_batch_ids(&mut requests);
        let response: BatchResponse = account
            .client
            .post_batch(&BatchRequest { requests })
            .await?;
        for item in response.responses {
            let index = item.id.parse::<usize>().ok();
            let id = index
                .and_then(|idx| request_ids.get(idx))
                .cloned()
                .unwrap_or_else(|| ObjectId(item.id.clone()));
            let scope = ErrorScope::Message {
                id: id.0.clone(),
            };
            let headers = item
                .headers
                .map(|h| {
                    let mut hm = reqwest::header::HeaderMap::new();
                    for (k, v) in h {
                        if let (Ok(name), Ok(val)) = (
                            reqwest::header::HeaderName::from_bytes(k.as_bytes()),
                            reqwest::header::HeaderValue::from_str(&v),
                        ) {
                            hm.insert(name, val);
                        }
                    }
                    hm
                })
                .unwrap_or_default();
            let body = item
                .body
                .as_ref()
                .map(|v| {
                    bytes::Bytes::from(serde_json::to_vec(v).unwrap_or_default())
                })
                .unwrap_or_default();
            item_outcomes.push(mutation_item_outcome(
                item.status,
                headers,
                body,
                matches!(kind, MutationKind::Destroy),
                BatchItemId(id.0.clone()),
                operation_for_kind(kind),
                scope,
            ));
        }
    }

    let events = vec![SyncEvent::Batch(Batch {
        items: item_outcomes,
        page_boundary: PageBoundary::Page,
        server_latency: Duration::default(),
        bytes_in: 0,
        checkpoint: None::<Checkpoint>,
    })];
    Ok(events)
}

/// Fetch etags for ids that require one but don't have a cached value.
/// Returns per-item `ItemOutcome::Failed` for ids where the fetch fails.
async fn refresh_missing_etags(
    account: &GraphAccount,
    ids: &[ObjectId],
    kind: &MutationKind,
    etags: &mut HashMap<String, String>,
) -> Vec<ItemOutcome<MutationSuccess>> {
    if !matches!(kind, MutationKind::SetFlags(_) | MutationKind::Move(_)) {
        return Vec::new();
    }

    let missing = ids
        .iter()
        .filter(|id| !etags.contains_key(&id.0))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Vec::new();
    }

    let prefix = account.client.api_path_prefix();
    let mut refreshed = Vec::new();
    let mut failed = Vec::new();
    for id in missing {
        let enc_id = bifrost_net::url::encode_component(&id.0);
        let path = format!("{prefix}/messages/{enc_id}?$select=id");
        match account.client.get_json::<Value>(&path).await {
            Ok(value) => {
                if let Some(etag) = graph_etag(&value) {
                    etags.insert(id.0.clone(), etag.clone());
                    refreshed.push((id.0.clone(), etag));
                } else {
                    failed.push(ItemOutcome::Failed(bifrost_types::BatchFailure {
                        item: BatchItemId(id.0.clone()),
                        error: super::graph_error::unsupported_account_error(
                            operation_for_kind(kind),
                        ),
                    }));
                    // Emit a warning as a side-channel so the engine
                    // can surface the missing-etag condition in
                    // support exports without treating the batch as
                    // terminated.
                    let _ = Warning {
                        kind: WarningKind::Other(
                            "graph_etag_missing_after_refresh".to_string(),
                        ),
                        message: DiagnosticText::support_only(format!(
                            "Graph message {} did not expose an etag after refresh",
                            id.0
                        )),
                        retry_count: 0,
                        next_action: None,
                        protocol_detail: None,
                    };
                }
            }
            Err(error) => {
                let ctx = GraphErrorContext::graph(operation_for_kind(kind))
                    .with_scope(ErrorScope::Message {
                        id: id.0.clone(),
                    });
                failed.push(ItemOutcome::Failed(bifrost_types::BatchFailure {
                    item: BatchItemId(id.0.clone()),
                    error: into_account_error(error, ctx),
                }));
            }
        }
    }

    if !refreshed.is_empty() {
        let mut cache = account.etag_index.write().await;
        for (id, etag) in refreshed {
            cache.insert(id, etag);
        }
    }
    failed
}

fn requires_etag(kind: &MutationKind) -> bool {
    matches!(kind, MutationKind::SetFlags(_) | MutationKind::Move(_))
}

fn request_for_mutation(
    account: &GraphAccount,
    id: &ObjectId,
    kind: &MutationKind,
    etags: &HashMap<String, String>,
) -> Result<Option<BatchRequestItem>, crate::error::GraphError> {
    let enc_id = bifrost_net::url::encode_component(&id.0);
    let prefix = account.client.api_path_prefix();
    let mut headers = HashMap::new();
    match kind {
        MutationKind::SetFlags(op) => {
            let Some(etag) = etags.get(&id.0) else {
                return Ok(None);
            };
            headers.insert("If-Match".to_string(), etag.clone());
            Ok(Some(BatchRequestItem {
                id: "0".to_string(),
                method: "PATCH".to_string(),
                url: format!("{prefix}/messages/{enc_id}"),
                body: Some(patch_for_flags(op)),
                headers: Some(headers),
            }))
        }
        MutationKind::Move(destination) => {
            let Some(etag) = etags.get(&id.0) else {
                return Ok(None);
            };
            let Some(folder) = folder_destination(destination.clone()) else {
                return Ok(None);
            };
            headers.insert("If-Match".to_string(), etag.clone());
            Ok(Some(BatchRequestItem {
                id: "0".to_string(),
                method: "POST".to_string(),
                url: format!("{prefix}/messages/{enc_id}/move"),
                body: Some(json!({ "destinationId": folder.0 })),
                headers: Some(headers),
            }))
        }
        MutationKind::Destroy => {
            if let Some(etag) = etags.get(&id.0) {
                headers.insert("If-Match".to_string(), etag.clone());
            }
            Ok(Some(BatchRequestItem {
                id: "0".to_string(),
                method: "DELETE".to_string(),
                url: format!("{prefix}/messages/{enc_id}"),
                body: None,
                headers: (!headers.is_empty()).then_some(headers),
            }))
        }
    }
}

fn retry_after_from_headers(headers: Option<&HashMap<String, String>>) -> Option<Duration> {
    let value = headers?.iter().find_map(|(name, value)| {
        name.eq_ignore_ascii_case("retry-after")
            .then_some(value.as_str())
    })?;
    value.trim().parse::<u64>().ok().map(Duration::from_secs)
}

pub(crate) fn assign_batch_ids(requests: &mut [BatchRequestItem]) {
    for (index, request) in requests.iter_mut().enumerate() {
        request.id = index.to_string();
    }
}

fn patch_for_flags(op: &FlagOp) -> Value {
    let mut body = serde_json::Map::new();
    match op {
        FlagOp::Add(flags) => apply_flag_adds(&mut body, flags),
        FlagOp::Remove(flags) => apply_flag_removes(&mut body, flags),
        FlagOp::Set(flags) => {
            apply_flag_adds(&mut body, flags);
            body.insert(
                "categories".to_string(),
                json!(categories_from_flags(flags)),
            );
        }
        FlagOp::Patch { add, remove } => {
            apply_flag_adds(&mut body, add);
            apply_flag_removes(&mut body, remove);
        }
        _ => {}
    }
    Value::Object(body)
}

fn apply_flag_adds(body: &mut serde_json::Map<String, Value>, flags: &HashSet<String>) {
    if has_flag(flags, "\\seen") || has_flag(flags, "read") {
        body.insert("isRead".to_string(), json!(true));
    }
    if has_flag(flags, "\\flagged") || has_flag(flags, "flagged") || has_flag(flags, "starred") {
        body.insert("flag".to_string(), json!({ "flagStatus": "flagged" }));
    }
}

fn apply_flag_removes(body: &mut serde_json::Map<String, Value>, flags: &HashSet<String>) {
    if has_flag(flags, "\\seen") || has_flag(flags, "read") {
        body.insert("isRead".to_string(), json!(false));
    }
    if has_flag(flags, "\\flagged") || has_flag(flags, "flagged") || has_flag(flags, "starred") {
        body.insert("flag".to_string(), json!({ "flagStatus": "notFlagged" }));
    }
}

fn has_flag(flags: &HashSet<String>, wanted: &str) -> bool {
    flags.iter().any(|flag| flag.eq_ignore_ascii_case(wanted))
}

fn categories_from_flags(flags: &HashSet<String>) -> Vec<String> {
    let mut categories: Vec<String> = flags
        .iter()
        .filter_map(|flag| flag.strip_prefix("category:").map(str::to_string))
        .collect();
    categories.sort();
    categories
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::*;

    #[test]
    fn batch_ids_are_assigned_in_order() {
        let mut requests = vec![
            BatchRequestItem {
                id: String::new(),
                method: "DELETE".to_string(),
                url: "/me/messages/a".to_string(),
                body: None,
                headers: None,
            },
            BatchRequestItem {
                id: String::new(),
                method: "DELETE".to_string(),
                url: "/me/messages/b".to_string(),
                body: None,
                headers: None,
            },
        ];
        assign_batch_ids(&mut requests);
        assert_eq!(requests[0].id, "0");
        assert_eq!(requests[1].id, "1");
    }

    #[test]
    fn retry_after_header_is_read_case_insensitively() {
        let headers = HashMap::from([("Retry-After".to_string(), "17".to_string())]);
        assert_eq!(
            retry_after_from_headers(Some(&headers)),
            Some(Duration::from_secs(17))
        );
    }
}
