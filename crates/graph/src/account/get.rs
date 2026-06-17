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
        // The input is a stream, so "is this the last chunk" is only
        // known after the input ends. Hold a filled chunk back by one
        // step: once the stream is exhausted, the held chunk (if any) is
        // the final batch and carries `PageBoundary::Final`, matching the
        // inventory/changes streams (a hydration that fired chunks tagged
        // only `Page` never told the engine where the stream terminated).
        let mut pending: Option<Vec<ObjectId>> = None;
        while let Some(id) = ids.next().await {
            chunk.push(id);
            if chunk.len() >= max_items {
                if let Some(ready) = pending.take() {
                    match fetch_batch(&account, &ready, projection, false).await {
                        Ok(batch_events) => for event in batch_events { yield event; },
                        Err(error) => {
                            let ctx = GraphErrorContext::graph(AccountOperation::Hydrate)
                                .with_scope(ErrorScope::Account);
                            yield SyncEvent::Terminated(into_account_error(error, ctx));
                            yield SyncEvent::Done(None);
                            return;
                        }
                    }
                }
                pending = Some(std::mem::replace(&mut chunk, Vec::with_capacity(max_items)));
            }
        }

        // Flush the held-back full chunk (non-final iff a trailing
        // partial chunk follows) then the trailing partial chunk.
        let trailing_nonempty = !chunk.is_empty();
        if let Some(ready) = pending.take() {
            match fetch_batch(&account, &ready, projection, !trailing_nonempty).await {
                Ok(batch_events) => for event in batch_events { yield event; },
                Err(error) => {
                    let ctx = GraphErrorContext::graph(AccountOperation::Hydrate)
                        .with_scope(ErrorScope::Account);
                    yield SyncEvent::Terminated(into_account_error(error, ctx));
                    yield SyncEvent::Done(None);
                    return;
                }
            }
        }

        if trailing_nonempty {
            match fetch_batch(&account, &chunk, projection, true).await {
                Ok(batch_events) => for event in batch_events { yield event; },
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
    is_final: bool,
) -> Result<Vec<SyncEvent<ItemOutcome<HydratedObject>>>, crate::error::GraphError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let select = select_for_projection(projection);
    let requests = ids
        .iter()
        .enumerate()
        .map(|(index, id)| {
            // Decode the (possibly foreign-encoded) id: a shared-mailbox
            // item routes to `/users/{owner}/messages/{native}`, a primary
            // item to `/me/messages/{id}`. The owning mailbox rides in the
            // path prefix, the native id in the `/messages/{id}` segment.
            BatchRequestItem {
                id: index.to_string(),
                method: "GET".to_string(),
                url: hydrate_url_for_id(account, id, select),
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
        page_boundary: if is_final {
            PageBoundary::Final
        } else {
            PageBoundary::Page
        },
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
        // Graph's JSON message resource is not assembled RFC822, so the
        // body-bearing projections cannot honestly produce `RawMime`
        // here (the prior code minted a JSON blob, violating the
        // RawMime = RFC822-bytes contract). A3 has no per-item `$value`
        // fetch in the hydration path (that N+1 is A1's call), so these
        // projections degrade to `Metadata` (falling back to `FlagsOnly`
        // when the inventory entry cannot be built). The assembled bytes
        // come exclusively through the dedicated `open_raw_rfc822` read.
        Projection::Metadata
        | Projection::Headers
        | Projection::Preview(_)
        | Projection::TextOnly
        | Projection::Full
        | Projection::FullWithBlobs => metadata_or_flags(value),
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

/// Build a `Metadata` kind from the Graph message JSON, falling back to
/// `FlagsOnly` when an inventory entry cannot be constructed. Shared by
/// the `Metadata` projection and the body-bearing projections that A3
/// degrades to metadata (the real body path is `open_raw_rfc822`).
fn metadata_or_flags(value: &Value) -> HydratedObjectKind {
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

/// Build the per-id Graph `/$batch` GET URL for hydration, decoding the
/// (possibly foreign-encoded) id to route to `/users/{owner}` vs `/me`.
/// Extracted so the routing is unit-testable without a live `$batch`.
fn hydrate_url_for_id(account: &GraphAccount, id: &ObjectId, select: &str) -> String {
    let parsed = super::foreign::parse_message_id(id);
    let prefix = account.client_for_owner(parsed.owner()).api_path_prefix();
    let enc_id = bifrost_net::url::encode_component(parsed.native_id());
    format!("{prefix}/messages/{enc_id}?$select={select}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::PushMode;
    use crate::client::GraphClient;
    use serde_json::json;

    #[test]
    fn hydrate_routes_foreign_id_to_owner_and_primary_to_me() {
        let account = GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["shared@contoso.com".to_string()],
        );
        let foreign_scope = CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: ObjectType::Email,
        };
        // A foreign-scope mint encodes the owner into the message id; the
        // request site decodes it back to `/users/{owner}/messages/{native}`.
        let foreign_id = super::super::foreign::encode_message_id(&foreign_scope, "AAMkmsg");
        assert_eq!(
            hydrate_url_for_id(&account, &foreign_id, "id"),
            "/users/shared%40contoso.com/messages/AAMkmsg?$select=id"
        );
        // A primary id stays bare and routes through `/me`.
        let primary_id = ObjectId("AAMkmsg".to_string());
        assert_eq!(
            hydrate_url_for_id(&account, &primary_id, "id"),
            "/me/messages/AAMkmsg?$select=id"
        );
    }

    /// A3: Graph hydration body projections no longer mint a JSON
    /// `RawMime` (the prior contract violation). They degrade to
    /// `Metadata` (or `FlagsOnly` on the fallback path), never
    /// `RawMime`. Asserting the exact degraded variant records the
    /// stopgap as a deliberate contract, not an accident. The assembled
    /// bytes come exclusively through `open_raw_rfc822`.
    #[test]
    fn hydration_body_projection_degrades_to_metadata() {
        let value = json!({
            "id": "AAMkmessage",
            "parentFolderId": "inbox",
            "isRead": true,
            "changeKey": "CK1"
        });

        for projection in [Projection::Headers, Projection::Full] {
            let hydrated =
                hydrated_from_value(ObjectId("AAMkmessage".to_string()), &value, projection);
            match hydrated.kind {
                HydratedObjectKind::Metadata(_) => {}
                other => panic!("expected Metadata for {projection:?}, got {other:?}"),
            }
        }

        // Fallback path: a value with no `id` cannot build an inventory
        // entry, so it degrades to `FlagsOnly` - still never `RawMime`.
        let no_id = json!({ "parentFolderId": "inbox", "isRead": false });
        let hydrated =
            hydrated_from_value(ObjectId("missing".to_string()), &no_id, Projection::Headers);
        assert!(matches!(hydrated.kind, HydratedObjectKind::FlagsOnly(_)));
    }
}
