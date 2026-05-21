use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bifrost_types::{
    AccountStream, Batch, Checkpoint, Error, FlagOp, IdempotencyKey, MembershipScope,
    MutationOutcome, MutationResult, ObjectId, PageBoundary, RecoveryClass, SyncEvent,
};
use futures_util::StreamExt;
use serde_json::{Value, json};

use crate::types::{BatchRequest, BatchRequestItem, BatchResponse};

use super::GraphAccount;
use super::error::{fatal_from_recovery, graph_error_to_fatal, mutation_outcome_for_status};
use super::get::folder_destination;

enum MutationKind {
    SetFlags(FlagOp),
    Move(MembershipScope),
    Destroy,
}

pub(crate) async fn bulk_set_flags_events(
    account: GraphAccount,
    targets: AccountStream<ObjectId>,
    op: FlagOp,
    _key: IdempotencyKey,
) -> Vec<SyncEvent<MutationResult>> {
    bulk_mutation_events(account, targets, MutationKind::SetFlags(op)).await
}

pub(crate) async fn bulk_move_events(
    account: GraphAccount,
    targets: AccountStream<ObjectId>,
    destination: MembershipScope,
    _key: IdempotencyKey,
) -> Vec<SyncEvent<MutationResult>> {
    bulk_mutation_events(account, targets, MutationKind::Move(destination)).await
}

pub(crate) async fn bulk_destroy_events(
    account: GraphAccount,
    targets: AccountStream<ObjectId>,
    _key: IdempotencyKey,
) -> Vec<SyncEvent<MutationResult>> {
    bulk_mutation_events(account, targets, MutationKind::Destroy).await
}

async fn bulk_mutation_events(
    account: GraphAccount,
    mut targets: AccountStream<ObjectId>,
    kind: MutationKind,
) -> Vec<SyncEvent<MutationResult>> {
    let mut ids = Vec::new();
    while let Some(id) = targets.next().await {
        ids.push(id);
    }

    let mut events = Vec::new();
    for chunk in ids.chunks(account.capabilities.batching_policy.max_items) {
        match submit_batch(&account, chunk, &kind).await {
            Ok(mut batch_events) => events.append(&mut batch_events),
            Err(error) => {
                events.push(SyncEvent::Fatal(graph_error_to_fatal(
                    error,
                    bifrost_types::CursorScope::Account,
                )));
                events.push(SyncEvent::Done(None));
                return events;
            }
        }
    }
    events.push(SyncEvent::Done(None));
    events
}

async fn submit_batch(
    account: &GraphAccount,
    ids: &[ObjectId],
    kind: &MutationKind,
) -> Result<Vec<SyncEvent<MutationResult>>, String> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }

    let etags = account.etag_index.read().await;
    let mut preflight = Vec::new();
    let mut requests = Vec::new();
    let mut request_ids = Vec::new();
    for id in ids {
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
    drop(etags);

    let mut results = preflight;
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
                retry_after = Some(Duration::from_secs(30));
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

fn request_for_mutation(
    account: &GraphAccount,
    id: &ObjectId,
    kind: &MutationKind,
    etags: &HashMap<String, String>,
) -> Result<Option<BatchRequestItem>, String> {
    let enc_id = urlencoding::encode(&id.0);
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
}
