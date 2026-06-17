use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bifrost_types::{
    AccountOperation, AccountStream, Batch, BatchFailure, BatchItemId, Checkpoint, ErrorScope,
    FlagOp, IdempotencyKey, ItemOutcome, MembershipScope, MutationSuccess, ObjectId, PageBoundary,
    ProtocolErrorKind, SyncEvent,
};
use futures::StreamExt;
use serde_json::{Value, json};

use crate::types::{BatchRequest, BatchRequestItem, BatchResponse};

use super::GraphAccount;
use super::get::folder_destination;
use super::graph_error::{
    GraphErrorContext, into_account_error, mutation_item_outcome, protocol_violation,
};
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
            // `request_for_mutation` returns `Ok(None)` when the
            // mutation cannot be built. For `Move` kinds the only
            // reason this fires (etag is preflight-checked above) is
            // a destination that isn't a folder - a caller-side
            // malformed request. Classify as `Request(Malformed)` so
            // recovery routes to `ClientBug` rather than the
            // misleading `Unsupported(BulkMove)` shape that suggests
            // the protocol doesn't support moves at all. (graph-F4)
            preflight_outcomes.push(ItemOutcome::Failed(BatchFailure::new(
                BatchItemId(id.0.clone()),
                super::graph_error::protocol_violation(
                    bifrost_types::ProtocolErrorKind::ContractViolation,
                    operation_for_kind(kind),
                    Some(bifrost_types::ErrorScope::Message { id: id.0.clone() }),
                    format!(
                        "Graph Move request for {} did not resolve to a folder destination",
                        id.0
                    ),
                ),
            )));
            continue;
        };
        request_ids.push(id.clone());
        requests.push(request);
    }

    let mut item_outcomes: Vec<ItemOutcome<MutationSuccess>> = preflight_outcomes;

    if !requests.is_empty() {
        assign_batch_ids(&mut requests);
        // Reconcile returned responses against submitted request ids:
        // Graph can return fewer `$batch` responses than requests, and a
        // missing id would otherwise surface on no lane, violating the
        // streaming "every id accounted for exactly once" contract. Track
        // which request indices we saw and emit a failed outcome for any
        // that never came back.
        let submitted_indices: Vec<usize> = (0..requests.len()).collect();
        let mut seen_indices: HashSet<usize> = HashSet::new();
        let response: BatchResponse = account
            .client
            .post_batch(&BatchRequest { requests })
            .await?;
        for item in response.responses {
            let index = item.id.parse::<usize>().ok();
            if let Some(idx) = index {
                seen_indices.insert(idx);
            }
            let id = index
                .and_then(|idx| request_ids.get(idx))
                .cloned()
                .unwrap_or_else(|| ObjectId(item.id.clone()));
            let scope = ErrorScope::Message { id: id.0.clone() };
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
                .map(|v| bytes::Bytes::from(serde_json::to_vec(v).unwrap_or_default()))
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

        // Emit a failed outcome for any submitted request that got no
        // response: the item was never applied, and dropping it silently
        // would leave its id on no lane.
        for idx in submitted_indices {
            if seen_indices.contains(&idx) {
                continue;
            }
            let Some(id) = request_ids.get(idx) else {
                continue;
            };
            item_outcomes.push(ItemOutcome::Failed(BatchFailure::new(
                BatchItemId(id.0.clone()),
                protocol_violation(
                    ProtocolErrorKind::ContractViolation,
                    operation_for_kind(kind),
                    Some(ErrorScope::Message { id: id.0.clone() }),
                    format!(
                        "Graph $batch returned no response for {} (item was not applied)",
                        id.0
                    ),
                ),
            )));
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
                    // Missing etag after a refresh is a Graph
                    // contract violation (`Protocol(MissingField)`),
                    // not an `Unsupported` op. Surface it on the
                    // failed lane so the consumer sees structured
                    // evidence rather than a misclassified terminal.
                    failed.push(ItemOutcome::Failed(BatchFailure::new(
                        BatchItemId(id.0.clone()),
                        protocol_violation(
                            ProtocolErrorKind::MissingField,
                            operation_for_kind(kind),
                            Some(ErrorScope::Message { id: id.0.clone() }),
                            format!(
                                "Graph message {} did not expose an etag after refresh",
                                id.0
                            ),
                        ),
                    )));
                }
            }
            Err(error) => {
                let ctx = GraphErrorContext::graph(operation_for_kind(kind))
                    .with_scope(ErrorScope::Message { id: id.0.clone() });
                failed.push(ItemOutcome::Failed(BatchFailure::new(
                    BatchItemId(id.0.clone()),
                    into_account_error(error, ctx),
                )));
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
            // `Set` is full-replace across the flags Graph owns: any flag
            // absent from the set must be cleared, not left untouched.
            // `apply_flag_adds` alone only ever writes `isRead:true` /
            // `flag:flagged` for present flags, so `Set({})` or
            // `Set({category:x})` would leave a previously-read/flagged
            // message read/flagged. Emit the authoritative value for each
            // owned field.
            body.insert(
                "isRead".to_string(),
                json!(has_flag(flags, "\\seen") || has_flag(flags, "read")),
            );
            let flagged = has_flag(flags, "\\flagged")
                || has_flag(flags, "flagged")
                || has_flag(flags, "starred");
            body.insert(
                "flag".to_string(),
                json!({ "flagStatus": if flagged { "flagged" } else { "notFlagged" } }),
            );
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

    fn flag_set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn set_clears_unflagged_owned_fields() {
        // `Set` with only a category present must still authoritatively
        // clear isRead and the flag: a previously-read/flagged message
        // becomes unread/unflagged. (Full-replace semantics.)
        let body = patch_for_flags(&FlagOp::Set(flag_set(&["category:Work"])));
        assert_eq!(body.get("isRead"), Some(&json!(false)));
        assert_eq!(
            body.get("flag"),
            Some(&json!({ "flagStatus": "notFlagged" }))
        );
        assert_eq!(body.get("categories"), Some(&json!(["Work"])));
    }

    #[test]
    fn set_empty_clears_all_owned_fields() {
        let body = patch_for_flags(&FlagOp::Set(HashSet::new()));
        assert_eq!(body.get("isRead"), Some(&json!(false)));
        assert_eq!(
            body.get("flag"),
            Some(&json!({ "flagStatus": "notFlagged" }))
        );
        assert_eq!(body.get("categories"), Some(&json!([] as [&str; 0])));
    }

    #[test]
    fn set_with_read_and_flagged_writes_true() {
        let body = patch_for_flags(&FlagOp::Set(flag_set(&["\\Seen", "\\Flagged"])));
        assert_eq!(body.get("isRead"), Some(&json!(true)));
        assert_eq!(body.get("flag"), Some(&json!({ "flagStatus": "flagged" })));
    }

    #[test]
    fn add_does_not_clear_absent_fields() {
        // `Add` is incremental: only the present flag is written, others
        // are untouched.
        let body = patch_for_flags(&FlagOp::Add(flag_set(&["\\Seen"])));
        assert_eq!(body.get("isRead"), Some(&json!(true)));
        assert!(body.get("flag").is_none());
        assert!(body.get("categories").is_none());
    }
}
