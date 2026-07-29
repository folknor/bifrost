use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bifrost_types::{
    AccountOperation, AccountStream, Batch, BatchFailure, BatchItemId, BatchUncertain, Checkpoint,
    ErrorScope, FlagOp, IdempotencyKey, ItemOutcome, MembershipScope, MutationSuccess, ObjectId,
    PageBoundary, ProtocolErrorKind, SyncEvent,
};
use futures::StreamExt;
use serde_json::{Value, json};

use crate::types::{BatchRequest, BatchRequestItem, BatchResponse};

use super::GraphAccount;
use super::get::folder_destination;
use super::graph_error::{
    GraphErrorContext, into_account_error, mutation_item_outcome, protocol_violation,
    unsupported_account_error,
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

    // Graph only accepts a full replacement for `categories`; its PATCH
    // surface has no add/remove member operation. Sending the partial flag
    // patch below would omit category tokens entirely, and a category-only
    // request would become `{}` and falsely report Applied on a 2xx. A
    // read-modify-write implementation can replace this rejection later,
    // but it must never silently claim the incremental category change.
    if matches!(kind, MutationKind::SetFlags(op) if flag_op_requires_category_rmw(op)) {
        let error = unsupported_account_error(AccountOperation::UpdateFlags);
        return Ok(vec![SyncEvent::Batch(Batch {
            items: ids
                .iter()
                .map(|id| {
                    ItemOutcome::Failed(BatchFailure::new(BatchItemId(id.0.clone()), error.clone()))
                })
                .collect(),
            page_boundary: PageBoundary::Page,
            server_latency: Duration::default(),
            bytes_in: 0,
            checkpoint: None::<Checkpoint>,
        })]);
    }

    let mut etags = HashMap::new();
    {
        let mut cache = account.etag_index.write().await;
        for id in ids {
            if let Some(etag) = cache.get(&id.0) {
                etags.insert(id.0.clone(), etag);
            }
        }
    }
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
            // mutation cannot be built. For `Move` kinds the etag is
            // preflight-checked above, so this fires either because the
            // destination isn't a folder, or because the destination
            // folder belongs to a different mailbox than the source (a
            // cross-mailbox move is not expressible against one
            // `/users/{owner}` endpoint). Both are caller-side malformed
            // requests. Classify as `Request(Malformed)` so recovery
            // routes to `ClientBug` rather than the misleading
            // `Unsupported(BulkMove)` shape that suggests the protocol
            // doesn't support moves at all. (graph-F4 / F1b)
            preflight_outcomes.push(ItemOutcome::Failed(BatchFailure::new(
                BatchItemId(id.0.clone()),
                super::graph_error::protocol_violation(
                    bifrost_types::ProtocolErrorKind::ContractViolation,
                    operation_for_kind(kind),
                    Some(bifrost_types::ErrorScope::Message { id: id.0.clone() }),
                    format!(
                        "Graph Move request for {} did not resolve to a same-mailbox folder destination",
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
        let response: BatchResponse = account
            .client
            .post_batch(&BatchRequest { requests })
            .await?;
        reconcile_mutation_responses(&request_ids, response.responses, kind, &mut item_outcomes);
        if matches!(kind, MutationKind::Destroy) {
            let mut cache = account.etag_index.write().await;
            for outcome in &item_outcomes {
                if let ItemOutcome::Succeeded(success) = outcome {
                    cache.remove(&success.item.0);
                }
            }
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

/// Project the `$batch` responses onto the submitted request ids,
/// appending exactly one `ItemOutcome` per id to `item_outcomes`.
///
/// Pure over the decoded response so the accounting rules are
/// unit-pinnable without a live `$batch` endpoint. A response id that does
/// not parse as a submitted index, parses out of range, or repeats one
/// already answered is DISCARDED - fabricating an id the caller never
/// submitted would put a stranger on the lane while the real id stays
/// unanswered.
///
/// An id Graph never answered lands on the UNCERTAIN lane. The `$batch`
/// envelope decoded, which is evidence about the envelope and nothing
/// else: a `move` or `DELETE` that committed and then lost its subresponse
/// is byte-identical to one that never ran. The uncertain lane is exactly
/// the lane for "this write may have landed, read it back rather than
/// replay it". Reporting `Failed(ContractViolation)` instead classified
/// terminal, so the engine recorded `failed_terminal` with no read-back
/// and permanently mis-stated an applied mutation as a lost one.
fn reconcile_mutation_responses(
    request_ids: &[ObjectId],
    responses: Vec<crate::types::BatchResponseItem>,
    kind: &MutationKind,
    item_outcomes: &mut Vec<ItemOutcome<MutationSuccess>>,
) {
    let mut seen_indices: HashSet<usize> = HashSet::new();
    for item in responses {
        let Some(index) = item
            .id
            .parse::<usize>()
            .ok()
            .filter(|idx| *idx < request_ids.len())
        else {
            tracing::warn!(
                target: "bifrost_graph::batch",
                response_id = %item.id,
                "ignoring Graph $batch mutation response with an invalid request id"
            );
            continue;
        };
        if !seen_indices.insert(index) {
            tracing::warn!(
                target: "bifrost_graph::batch",
                response_id = %item.id,
                "ignoring duplicate Graph $batch mutation response"
            );
            continue;
        }
        let id = request_ids[index].clone();
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

    for (index, id) in request_ids.iter().enumerate() {
        if seen_indices.contains(&index) {
            continue;
        }
        item_outcomes.push(ItemOutcome::Uncertain(BatchUncertain::new(
            BatchItemId(id.0.clone()),
            super::graph_error::batch_response_missing(
                operation_for_kind(kind),
                Some(ErrorScope::Message { id: id.0.clone() }),
                format!(
                    "Graph $batch returned no response for {}; the item's fate is unknown",
                    id.0
                ),
            ),
        )));
    }
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

    let mut refreshed = Vec::new();
    let mut failed = Vec::new();
    for id in missing {
        // Decode the (possibly foreign-encoded) id so the etag refresh
        // hits the owning mailbox: a shared-mailbox message lives under
        // `/users/{owner}/messages/{native}`, not `/me`. The cache key
        // stays the encoded `id.0` (F1's etag_index keying), only the
        // request URL uses the native id + owner prefix.
        let parsed = super::foreign::parse_message_id(&id);
        let client = account.client_for_owner(parsed.owner());
        let prefix = client.api_path_prefix();
        let enc_id = bifrost_net::url::encode_path_component(parsed.native_id());
        let path = format!("{prefix}/messages/{enc_id}?$select=id");
        match client.get_json::<Value>(&path).await {
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
    // Decode the (possibly foreign-encoded) id and route to the owning
    // mailbox: a shared-mailbox item builds `/users/{owner}/messages/{native}`,
    // a primary item `/me/messages/{id}`. The etag cache is keyed by the
    // encoded `id.0` (F1's etag_index), so the lookups below use `id.0`;
    // only the URL is built from the decoded native id + owner prefix.
    let parsed = super::foreign::parse_message_id(id);
    let prefix = account.client_for_owner(parsed.owner()).api_path_prefix();
    let enc_id = bifrost_net::url::encode_path_component(parsed.native_id());
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
            // The destination folder id is itself a (possibly
            // foreign-encoded) FolderId. Decode it: `destinationId` must
            // carry the native folder id (a `\u{1f}`-bearing id is not a
            // valid Graph folder id), and the destination must belong to
            // the same mailbox as the source. A cross-mailbox move is not
            // expressible against one `/users/{owner}` (or `/me`) endpoint,
            // so reject it as a malformed request rather than emit a wrong
            // one - returning `None` routes through the caller's
            // `Request(Malformed)` lane.
            let dest = super::foreign::parse_folder(&folder);
            if dest.foreign().map(|f| f.mailbox.as_str()) != parsed.owner() {
                return Ok(None);
            }
            headers.insert("If-Match".to_string(), etag.clone());
            Ok(Some(BatchRequestItem {
                id: "0".to_string(),
                method: "POST".to_string(),
                url: format!("{prefix}/messages/{enc_id}/move"),
                body: Some(json!({ "destinationId": dest.native_id() })),
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

fn flag_op_requires_category_rmw(op: &FlagOp) -> bool {
    let has_categories =
        |flags: &HashSet<String>| flags.iter().any(|flag| flag.starts_with("category:"));
    match op {
        FlagOp::Add(flags) | FlagOp::Remove(flags) => has_categories(flags),
        FlagOp::Patch { add, remove } => has_categories(add) || has_categories(remove),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::PushMode;
    use crate::account::foreign::{encode_foreign, encode_message_id};
    use crate::client::GraphClient;
    use bifrost_types::{CursorScope, FolderId, ObjectType};

    fn shared_account() -> GraphAccount {
        GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["shared@contoso.com".to_string()],
        )
    }

    fn foreign_message_id(mailbox: &str, folder: &str, native: &str) -> ObjectId {
        let scope = CursorScope::FolderType {
            folder: encode_foreign(mailbox, folder),
            ty: ObjectType::Email,
        };
        encode_message_id(&scope, native)
    }

    fn etag_map(id: &ObjectId) -> HashMap<String, String> {
        let mut etags = HashMap::new();
        etags.insert(id.0.clone(), "W/\"CK1\"".to_string());
        etags
    }

    fn ok_response(id: &str) -> crate::types::BatchResponseItem {
        crate::types::BatchResponseItem {
            id: id.to_string(),
            status: 200,
            headers: None,
            body: None,
        }
    }

    fn outcome_item(outcome: &ItemOutcome<MutationSuccess>) -> String {
        match outcome {
            ItemOutcome::Succeeded(success) => success.item.0.clone(),
            ItemOutcome::Failed(failure) => failure.item.0.clone(),
            ItemOutcome::Uncertain(uncertain) => uncertain.item.0.clone(),
        }
    }

    /// A `$batch` response that omits a `move` subresponse is NOT proof
    /// the move was skipped - a move that committed and then lost its
    /// subresponse is byte-identical on the wire. The id belongs on the
    /// uncertain lane so the engine reads it back; the terminal
    /// `Failed(ContractViolation)` this replaced made the engine record
    /// `failed_terminal` with no read-back at all, so a landed move was
    /// reported to the consumer as permanently lost.
    #[test]
    fn an_unanswered_move_lands_on_the_uncertain_lane_for_readback() {
        let request_ids = vec![ObjectId("m0".to_string()), ObjectId("m1".to_string())];
        let mut outcomes = Vec::new();
        reconcile_mutation_responses(
            &request_ids,
            vec![ok_response("0")],
            &MutationKind::Move(MembershipScope::Folder(FolderId("archive".to_string()))),
            &mut outcomes,
        );

        assert_eq!(outcomes.len(), 2);
        let ItemOutcome::Uncertain(uncertain) = &outcomes[1] else {
            panic!("expected uncertain, got {:?}", outcomes[1]);
        };
        assert_eq!(uncertain.item.0, "m1");
        assert!(matches!(
            uncertain.error.kind(),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::PartialResponse
            )
        ));
        // Non-idempotent: probe the target rather than replaying the move.
        assert!(uncertain.error.recovery().requires_reconciliation());
        assert!(!uncertain.error.recovery().is_terminal());
    }

    /// An absolute-state flag write is idempotent, so the same ambiguity
    /// classifies retryable - but it still rides the uncertain lane,
    /// because the engine's read-back guard is what turns "may have
    /// landed" into a fact for either shape.
    #[test]
    fn an_unanswered_flag_write_is_uncertain_and_retryable() {
        let request_ids = vec![ObjectId("m0".to_string())];
        let mut outcomes = Vec::new();
        reconcile_mutation_responses(
            &request_ids,
            Vec::new(),
            &MutationKind::SetFlags(FlagOp::Add(HashSet::from(["\\seen".to_string()]))),
            &mut outcomes,
        );

        let ItemOutcome::Uncertain(uncertain) = &outcomes[0] else {
            panic!("expected uncertain, got {:?}", outcomes[0]);
        };
        assert!(uncertain.error.recovery().is_retryable());
    }

    /// A response id outside the submitted range, unparsable, or repeated
    /// is discarded rather than answered. Accepting one would put an id
    /// the caller never submitted on a lane (or two outcomes on one id)
    /// while the real id stays unanswered.
    #[test]
    fn invalid_and_duplicate_mutation_response_ids_are_discarded() {
        let request_ids = vec![ObjectId("m0".to_string())];
        let mut outcomes = Vec::new();
        reconcile_mutation_responses(
            &request_ids,
            vec![
                ok_response("0"),
                ok_response("0"),
                ok_response("9"),
                ok_response("nope"),
            ],
            &MutationKind::Destroy,
            &mut outcomes,
        );

        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcome_item(&outcomes[0]), "m0");
        assert!(matches!(outcomes[0], ItemOutcome::Succeeded(_)));
    }

    #[test]
    fn foreign_flag_mutation_routes_to_owner_with_native_id() {
        let account = shared_account();
        let id = foreign_message_id("shared@contoso.com", "AAMkfolder", "AAMkmsg");
        let etags = etag_map(&id);
        let request = request_for_mutation(
            &account,
            &id,
            &MutationKind::SetFlags(FlagOp::Add(HashSet::from(["\\seen".to_string()]))),
            &etags,
        )
        .expect("builds")
        .expect("some request");
        assert_eq!(request.url, "/users/shared%40contoso.com/messages/AAMkmsg");
        assert!(!request.url.contains('\u{1f}'), "{}", request.url);
    }

    #[test]
    fn primary_flag_mutation_stays_on_me() {
        let account = shared_account();
        let id = ObjectId("AAMkmsg".to_string());
        let etags = etag_map(&id);
        let request = request_for_mutation(
            &account,
            &id,
            &MutationKind::SetFlags(FlagOp::Add(HashSet::from(["\\seen".to_string()]))),
            &etags,
        )
        .expect("builds")
        .expect("some request");
        assert_eq!(request.url, "/me/messages/AAMkmsg");
    }

    #[test]
    fn foreign_destroy_routes_to_owner() {
        let account = shared_account();
        let id = foreign_message_id("shared@contoso.com", "AAMkfolder", "AAMkmsg");
        let request = request_for_mutation(&account, &id, &MutationKind::Destroy, &HashMap::new())
            .expect("builds")
            .expect("some request");
        assert_eq!(request.method, "DELETE");
        assert_eq!(request.url, "/users/shared%40contoso.com/messages/AAMkmsg");
    }

    #[test]
    fn bulk_move_routes_to_owner_with_native_destination_folder() {
        let account = shared_account();
        let id = foreign_message_id("shared@contoso.com", "AAMkfolder", "AAMkmsg");
        let etags = etag_map(&id);
        // The destination folder belongs to the same shared mailbox.
        let destination = MembershipScope::Folder(encode_foreign("shared@contoso.com", "AAMkdest"));
        let request = request_for_mutation(&account, &id, &MutationKind::Move(destination), &etags)
            .expect("builds")
            .expect("some request");
        assert_eq!(
            request.url,
            "/users/shared%40contoso.com/messages/AAMkmsg/move"
        );
        // The move body's destinationId is the native folder id, not the
        // `\u{1f}`-encoded FolderId.
        assert_eq!(request.body, Some(json!({ "destinationId": "AAMkdest" })));
    }

    #[test]
    fn cross_mailbox_move_fails_cleanly() {
        let account = shared_account();
        // Source message in the shared mailbox, destination in the primary
        // mailbox: not expressible against one endpoint -> None (the caller
        // turns this into a Request(Malformed) failure).
        let id = foreign_message_id("shared@contoso.com", "AAMkfolder", "AAMkmsg");
        let etags = etag_map(&id);
        let primary_dest = MembershipScope::Folder(FolderId("inbox".to_string()));
        assert!(
            request_for_mutation(&account, &id, &MutationKind::Move(primary_dest), &etags)
                .expect("builds")
                .is_none()
        );

        // The mirror: a primary source moving into a shared-mailbox folder
        // is equally inexpressible.
        let primary_id = ObjectId("AAMkmsg".to_string());
        let primary_etags = etag_map(&primary_id);
        let foreign_dest =
            MembershipScope::Folder(encode_foreign("shared@contoso.com", "AAMkdest"));
        assert!(
            request_for_mutation(
                &account,
                &primary_id,
                &MutationKind::Move(foreign_dest),
                &primary_etags,
            )
            .expect("builds")
            .is_none()
        );
    }

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

    #[test]
    fn remove_clears_the_named_fields() {
        let body = patch_for_flags(&FlagOp::Remove(flag_set(&["\\Seen", "starred"])));
        assert_eq!(body.get("isRead"), Some(&json!(false)));
        assert_eq!(
            body.get("flag"),
            Some(&json!({ "flagStatus": "notFlagged" }))
        );
    }

    #[test]
    fn patch_applies_removes_after_adds_so_remove_wins_on_a_contested_flag() {
        // `Patch` is one wire operation, so a flag present in both halves
        // has to resolve deterministically. `apply_flag_removes` runs last.
        let body = patch_for_flags(&FlagOp::Patch {
            add: flag_set(&["\\Seen", "\\Flagged"]),
            remove: flag_set(&["\\Seen"]),
        });
        assert_eq!(body.get("isRead"), Some(&json!(false)));
        assert_eq!(body.get("flag"), Some(&json!({ "flagStatus": "flagged" })));
    }

    #[test]
    fn set_sorts_categories_for_a_deterministic_body() {
        let body = patch_for_flags(&FlagOp::Set(flag_set(&[
            "category:Zeta",
            "category:alpha",
            "category:Mid",
        ])));
        // Byte order, not locale order - the point is only that the same
        // input set always produces the same JSON.
        assert_eq!(
            body.get("categories"),
            Some(&json!(["Mid", "Zeta", "alpha"]))
        );
    }

    #[test]
    fn flag_names_are_matched_case_insensitively() {
        for token in ["\\seen", "\\SEEN", "Read", "read"] {
            let body = patch_for_flags(&FlagOp::Add(flag_set(&[token])));
            assert_eq!(
                body.get("isRead"),
                Some(&json!(true)),
                "token {token} did not set isRead"
            );
        }
        for token in ["\\flagged", "\\FLAGGED", "Flagged", "Starred"] {
            let body = patch_for_flags(&FlagOp::Add(flag_set(&[token])));
            assert_eq!(
                body.get("flag"),
                Some(&json!({ "flagStatus": "flagged" })),
                "token {token} did not set the flag"
            );
        }
    }

    /// Incremental category changes need a category-array read-modify-write.
    /// Until that exists, every shape containing a category is rejected as a
    /// unit so a mixed flag/category request cannot partially apply either.
    #[test]
    fn category_incremental_ops_are_detected_before_building_a_patch() {
        assert!(flag_op_requires_category_rmw(&FlagOp::Add(flag_set(&[
            "category:Work"
        ]))));
        assert!(flag_op_requires_category_rmw(&FlagOp::Remove(flag_set(&[
            "category:Work"
        ]))));
        assert!(flag_op_requires_category_rmw(&FlagOp::Patch {
            add: flag_set(&["category:Work"]),
            remove: flag_set(&["category:Old"]),
        }));
        assert!(!flag_op_requires_category_rmw(&FlagOp::Set(flag_set(&[
            "category:Work"
        ]))));
        assert!(!flag_op_requires_category_rmw(&FlagOp::Add(flag_set(&[
            "\\Seen"
        ]))));
    }

    #[tokio::test]
    async fn category_incremental_ops_fail_without_emitting_an_empty_patch() {
        let account = shared_account();
        let id = ObjectId("AAMkmsg".to_string());
        let events = submit_batch(
            &account,
            std::slice::from_ref(&id),
            &MutationKind::SetFlags(FlagOp::Add(flag_set(&["category:Work"]))),
        )
        .await
        .expect("preflight category rejection is local");
        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("expected a batch outcome");
        };
        let ItemOutcome::Failed(failure) = &batch.items[0] else {
            panic!("category operation must not be applied");
        };
        assert!(matches!(
            failure.error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::UpdateFlags)
        ));
    }

    #[test]
    fn set_flags_without_an_etag_refuses_to_build_a_request() {
        // `If-Match` is mandatory for SetFlags/Move; without a cached or
        // refreshed etag the item must fail rather than write unconditioned.
        let account = shared_account();
        let id = ObjectId("AAMkmsg".to_string());
        assert!(
            request_for_mutation(
                &account,
                &id,
                &MutationKind::SetFlags(FlagOp::Add(HashSet::from(["\\seen".to_string()]))),
                &HashMap::new(),
            )
            .expect("builds")
            .is_none()
        );
        assert!(
            request_for_mutation(
                &account,
                &id,
                &MutationKind::Move(MembershipScope::Folder(FolderId("archive".to_string()))),
                &HashMap::new(),
            )
            .expect("builds")
            .is_none()
        );
    }

    #[test]
    fn destroy_attaches_if_match_opportunistically() {
        let account = shared_account();
        let id = ObjectId("AAMkmsg".to_string());

        let unconditioned =
            request_for_mutation(&account, &id, &MutationKind::Destroy, &HashMap::new())
                .expect("builds")
                .expect("destroy needs no etag");
        assert!(unconditioned.headers.is_none());

        let conditioned =
            request_for_mutation(&account, &id, &MutationKind::Destroy, &etag_map(&id))
                .expect("builds")
                .expect("some request");
        assert_eq!(
            conditioned
                .headers
                .as_ref()
                .and_then(|h| h.get("If-Match"))
                .map(String::as_str),
            Some("W/\"CK1\"")
        );
        assert!(conditioned.body.is_none());
    }

    #[test]
    fn move_to_a_non_folder_destination_is_rejected() {
        // Only `MembershipScope::Folder` names a Graph move target; a
        // mailbox-scoped destination is a caller error, surfaced by the
        // caller as `Request(Malformed)`.
        let account = shared_account();
        let id = ObjectId("AAMkmsg".to_string());
        let etags = etag_map(&id);
        assert!(
            request_for_mutation(
                &account,
                &id,
                &MutationKind::Move(MembershipScope::Mailbox(bifrost_types::MailboxId(
                    "shared@contoso.com".to_string()
                ))),
                &etags,
            )
            .expect("builds")
            .is_none()
        );
    }

    #[test]
    fn primary_move_uses_the_bare_destination_folder_id() {
        let account = shared_account();
        let id = ObjectId("AAMkmsg".to_string());
        let etags = etag_map(&id);
        let request = request_for_mutation(
            &account,
            &id,
            &MutationKind::Move(MembershipScope::Folder(FolderId("archive".to_string()))),
            &etags,
        )
        .expect("builds")
        .expect("some request");
        assert_eq!(request.method, "POST");
        assert_eq!(request.url, "/me/messages/AAMkmsg/move");
        assert_eq!(request.body, Some(json!({ "destinationId": "archive" })));
    }

    #[test]
    fn set_flags_conditions_on_the_cached_etag_keyed_by_the_encoded_id() {
        // The etag cache is keyed by the CALLER-facing (foreign-encoded) id
        // even though the URL carries the native one; a lookup by native id
        // would miss and drop the mutation.
        let account = shared_account();
        let id = foreign_message_id("shared@contoso.com", "AAMkfolder", "AAMkmsg");
        let mut native_keyed = HashMap::new();
        native_keyed.insert("AAMkmsg".to_string(), "W/\"CK1\"".to_string());
        assert!(
            request_for_mutation(
                &account,
                &id,
                &MutationKind::SetFlags(FlagOp::Add(HashSet::from(["\\seen".to_string()]))),
                &native_keyed,
            )
            .expect("builds")
            .is_none(),
            "a native-keyed etag must not satisfy an encoded-id lookup"
        );

        let request = request_for_mutation(
            &account,
            &id,
            &MutationKind::SetFlags(FlagOp::Add(HashSet::from(["\\seen".to_string()]))),
            &etag_map(&id),
        )
        .expect("builds")
        .expect("some request");
        assert_eq!(
            request
                .headers
                .as_ref()
                .and_then(|h| h.get("If-Match"))
                .map(String::as_str),
            Some("W/\"CK1\"")
        );
    }

    #[test]
    fn operation_labels_match_the_mutation_kind() {
        assert_eq!(
            operation_for_kind(&MutationKind::SetFlags(FlagOp::Add(HashSet::new()))),
            AccountOperation::UpdateFlags
        );
        assert_eq!(
            operation_for_kind(&MutationKind::Move(MembershipScope::Folder(FolderId(
                "archive".to_string()
            )))),
            AccountOperation::BulkMove
        );
        assert_eq!(
            operation_for_kind(&MutationKind::Destroy),
            AccountOperation::BulkDestroy
        );
    }

    #[test]
    fn only_flag_bearing_kinds_require_an_etag() {
        assert!(requires_etag(&MutationKind::SetFlags(FlagOp::Add(
            HashSet::new()
        ))));
        assert!(requires_etag(&MutationKind::Move(MembershipScope::Folder(
            FolderId("archive".to_string())
        ))));
        assert!(!requires_etag(&MutationKind::Destroy));
    }

    fn idempotency_key() -> IdempotencyKey {
        IdempotencyKey {
            run_id: bifrost_types::RunId("run-1".to_string()),
            sequence: 0,
            protocol_salt: bifrost_types::ProtocolSalt::Graph("salt".to_string()),
        }
    }

    #[tokio::test]
    async fn an_empty_target_stream_yields_only_done() {
        // No ids means no `$batch`; the stream must not emit an empty
        // `Batch` (which the engine would read as "0 of N applied").
        let account = shared_account();
        let targets: AccountStream<ObjectId> = Box::pin(futures::stream::empty());
        let mut stream = bulk_destroy_stream(account, targets, idempotency_key());
        assert!(matches!(stream.next().await, Some(SyncEvent::Done(None))));
        assert!(stream.next().await.is_none());
    }
}
