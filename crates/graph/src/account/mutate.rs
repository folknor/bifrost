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
        let enc_id = bifrost_net::url::encode_component(parsed.native_id());
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
    let enc_id = bifrost_net::url::encode_component(parsed.native_id());
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

    /// Documents a defect, NOT the intended contract.
    /// `FlagOp::Add` / `Remove` / `Patch`
    /// ignore `category:` flags entirely - only `Set` writes `categories`.
    /// A `bulk_set_flags(Add{category:Work})` therefore PATCHes an EMPTY
    /// object, Graph answers 200, and `mutation_item_outcome` reports
    /// `Succeeded(Applied)` for a mutation that changed nothing. The
    /// consumer's read-back guard is the only thing that would notice.
    #[test]
    fn add_and_remove_silently_drop_category_flags_into_an_empty_patch() {
        assert_eq!(
            patch_for_flags(&FlagOp::Add(flag_set(&["category:Work"]))),
            json!({})
        );
        assert_eq!(
            patch_for_flags(&FlagOp::Remove(flag_set(&["category:Work"]))),
            json!({})
        );
        assert_eq!(
            patch_for_flags(&FlagOp::Patch {
                add: flag_set(&["category:Work"]),
                remove: flag_set(&["category:Old"]),
            }),
            json!({})
        );
        // An unrecognized flag is dropped the same way.
        assert_eq!(
            patch_for_flags(&FlagOp::Add(flag_set(&["\\Draft"]))),
            json!({})
        );
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
