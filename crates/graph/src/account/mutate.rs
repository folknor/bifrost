use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bifrost_types::{
    AccountStream, Batch, Checkpoint, Error, FlagOp, IdempotencyKey, MembershipScope,
    MutationOutcome, MutationResult, ObjectId, PageBoundary, RecoveryClass, SyncEvent,
};
use futures::StreamExt;
use serde_json::{Value, json};

use crate::types::{BatchRequest, BatchRequestItem, BatchResponse};

use super::GraphAccount;
use super::error::{fatal_from_recovery, graph_error_to_fatal, mutation_outcome_for_status};
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
) -> AccountStream<SyncEvent<MutationResult>> {
    bulk_mutation_stream(account, targets, MutationKind::SetFlags(op))
}

pub(crate) fn bulk_move_stream(
    account: GraphAccount,
    targets: AccountStream<ObjectId>,
    destination: MembershipScope,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<MutationResult>> {
    bulk_mutation_stream(account, targets, MutationKind::Move(destination))
}

pub(crate) fn bulk_destroy_stream(
    account: GraphAccount,
    targets: AccountStream<ObjectId>,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<MutationResult>> {
    bulk_mutation_stream(account, targets, MutationKind::Destroy)
}

fn bulk_mutation_stream(
    account: GraphAccount,
    mut targets: AccountStream<ObjectId>,
    kind: MutationKind,
) -> AccountStream<SyncEvent<MutationResult>> {
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
                        yield SyncEvent::Fatal(graph_error_to_fatal(
                            error,
                            bifrost_types::CursorScope::Account,
                        ));
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
                    yield SyncEvent::Fatal(graph_error_to_fatal(
                        error,
                        bifrost_types::CursorScope::Account,
                    ));
                    yield SyncEvent::Done(None);
                    return;
                }
            }
        }

        yield SyncEvent::Done(None);
    })
}

async fn submit_batch(
    account: &GraphAccount,
    ids: &[ObjectId],
    kind: &MutationKind,
) -> Result<Vec<SyncEvent<MutationResult>>, String> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut etags = account.etag_index.read().await.clone();
    let mut results = refresh_missing_etags(account, ids, kind, &mut etags).await;
    let mut preflight = Vec::new();
    let mut requests = Vec::new();
    let mut request_ids = Vec::new();
    for id in ids {
        if requires_etag(kind) && !etags.contains_key(&id.0) {
            continue;
        }
        let Some(request) = request_for_mutation(account, id, kind, &etags)? else {
            preflight.push(MutationResult {
                id: id.clone(),
                outcome: MutationOutcome::Failed(Error::MissingCoreCapability),
            });
            continue;
        };
        request_ids.push(id.clone());
        requests.push(request);
    }

    results.extend(preflight);
    let mut retry_after = None;
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
            if item.status == 429 {
                retry_after = retry_after_from_headers(item.headers.as_ref())
                    .or(retry_after)
                    .or(Some(Duration::from_secs(30)));
            }
            results.push(MutationResult {
                id: id.clone(),
                outcome: mutation_outcome_for_status(
                    item.status,
                    matches!(kind, MutationKind::Destroy),
                    &id,
                ),
            });
        }
    }

    let mut events = vec![SyncEvent::Batch(Batch {
        items: results,
        page_boundary: PageBoundary::Page,
        server_latency: Duration::default(),
        bytes_in: 0,
        checkpoint: None::<Checkpoint>,
    })];
    if let Some(after) = retry_after {
        events.push(SyncEvent::Fatal(fatal_from_recovery(
            RecoveryClass::Retry { after },
            "Graph mutation batch was throttled",
        )));
    }
    Ok(events)
}

async fn refresh_missing_etags(
    account: &GraphAccount,
    ids: &[ObjectId],
    kind: &MutationKind,
    etags: &mut HashMap<String, String>,
) -> Vec<MutationResult> {
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
                    failed.push(MutationResult {
                        id,
                        outcome: MutationOutcome::Failed(Error::Other(
                            "Graph message did not expose an etag".to_string(),
                        )),
                    });
                }
            }
            Err(error) => failed.push(MutationResult {
                id,
                outcome: MutationOutcome::Failed(Error::Transport(error)),
            }),
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
) -> Result<Option<BatchRequestItem>, String> {
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
                return Err("Graph bulk_move destination must be a folder".to_string());
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
