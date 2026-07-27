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

/// Split a hydration chunk into the ids that must be read over EWS
/// `GetItem` and the ids that go through the Graph REST `$batch`.
///
/// A public-folder item is a raw EWS `ItemId` carried inside a
/// folder-qualified `ObjectId`; Graph REST has no route that can address it
/// (`/me/messages/{ItemId}` 404s), so before this split every public-folder
/// item hydrated as `Failed`. Membership in the routing map is the
/// discriminator - a folder-qualified id whose folder was never discovered
/// falls back to the REST arm, which reports the real miss rather than a
/// fabricated local error.
async fn partition_ews_ids(
    account: &GraphAccount,
    ids: &[ObjectId],
) -> (Vec<(ObjectId, FolderId)>, Vec<ObjectId>) {
    let mut ews = Vec::new();
    let mut rest = Vec::new();
    for id in ids {
        match super::foreign::parse_message_id(id).public_folder() {
            Some(folder) => {
                let folder = FolderId(folder.to_string());
                if account.public_folder_routing(&folder).await.is_some() {
                    ews.push((id.clone(), folder));
                } else {
                    rest.push(id.clone());
                }
            }
            None => rest.push(id.clone()),
        }
    }
    (ews, rest)
}

/// Hydrate the public-folder ids of a chunk through EWS `GetItem`, one
/// request per item (EWS `GetItem` takes an id list, but each id can sit in
/// a different public folder with a different routing header pair, so the
/// per-folder routing is what forces the fan-out).
async fn fetch_ews_outcomes(
    account: &GraphAccount,
    ids: &[(ObjectId, FolderId)],
    projection: Projection,
) -> Vec<ItemOutcome<HydratedObject>> {
    let mut outcomes = Vec::new();
    for (id, folder) in ids {
        let batch_id = BatchItemId(id.0.clone());
        let Some(routing) = account.public_folder_routing(folder).await else {
            // Raced with a routing-map eviction; report it per item rather
            // than poisoning the rest of the chunk.
            outcomes.push(ItemOutcome::Failed(BatchFailure::new(
                batch_id,
                super::graph_error::protocol_violation(
                    bifrost_types::ProtocolErrorKind::MissingField,
                    AccountOperation::Hydrate,
                    Some(ErrorScope::Message { id: id.0.clone() }),
                    format!("public folder {} has no routing entry", folder.0),
                ),
            )));
            continue;
        };
        let Some(ews) = super::public_folder::ews_client(account) else {
            outcomes.push(ItemOutcome::Failed(BatchFailure::new(
                batch_id,
                super::graph_error::ews_error_to_account_error(
                    crate::ews::EwsError::Transport(bifrost_net::Error::Network {
                        message: "EWS account net not attached".to_string(),
                        transmission_state: bifrost_types::TransmissionState::Unsent,
                        source: None,
                    }),
                    GraphErrorContext::ews(AccountOperation::Hydrate)
                        .with_scope(ErrorScope::Message { id: id.0.clone() }),
                ),
            )));
            continue;
        };
        let native = super::foreign::parse_message_id(id).native_id().to_string();
        match ews.get_item(&native, &routing.headers()).await {
            Ok(item) => outcomes.push(ItemOutcome::Succeeded(BatchSuccess::new(
                batch_id,
                hydrated_from_ews_item(
                    id.clone(),
                    &item,
                    folder,
                    &routing.anchor_mailbox,
                    projection,
                ),
            ))),
            Err(error) => outcomes.push(ItemOutcome::Failed(BatchFailure::new(
                batch_id,
                super::graph_error::ews_error_to_account_error(
                    error,
                    GraphErrorContext::ews(AccountOperation::Hydrate)
                        .with_scope(ErrorScope::Message { id: id.0.clone() }),
                ),
            ))),
        }
    }
    outcomes
}

/// Project an EWS `GetItem` result into the SAME `HydratedObject` shape the
/// Graph REST arm returns.
///
/// `Metadata` reuses the public-folder inventory projection, so a hydrated
/// item is byte-identical to its inventory entry (same id, same memberships,
/// same change-key fingerprint). The body-bearing projections degrade to
/// `Metadata` for the same reason the REST arm does: `HydratedObjectKind` can
/// only carry assembled RFC822 in `RawMime`, and an EWS `GetItem` returns a
/// parsed HTML body, not MIME octets - minting it as `RawMime` would violate
/// that contract. Attachment DESCRIPTORS still ride out as blob handles, so
/// the consumer can pull the bytes through `open_blob`.
///
/// Pure over the already-fetched item, so the projection is unit-pinnable
/// without a live EWS server.
pub(crate) fn hydrated_from_ews_item(
    id: ObjectId,
    item: &crate::ews::EwsItem,
    folder: &FolderId,
    content_mailbox: &str,
    projection: Projection,
) -> HydratedObject {
    let kind = match projection {
        Projection::FlagsOnly => HydratedObjectKind::FlagsOnly(ews_flags(item)),
        _ => HydratedObjectKind::Metadata(super::public_folder::item_to_inventory_entry(
            item,
            folder,
            content_mailbox,
        )),
    };
    let blobs = item
        .attachments
        .iter()
        .map(|attachment| super::blob::blob_handle_from_ews_attachment(&id, attachment))
        .collect();
    HydratedObject { id, kind, blobs }
}

/// The canonical flag set an EWS item carries. EWS surfaces only the
/// read/unread bit on this shape (no categories, no flag status), so
/// `\seen` is the whole vocabulary.
fn ews_flags(item: &crate::ews::EwsItem) -> HashSet<String> {
    let mut flags = HashSet::new();
    if item.is_read {
        flags.insert("\\seen".to_string());
    }
    flags
}

/// Project a hydration chunk into `ItemOutcome` envelopes.
///
/// Public-folder ids are served by EWS `GetItem`, everything else by the
/// Graph `/$batch`; both arms land in ONE `Batch` so the consumer still sees
/// exactly one outcome per pulled id. Per-item 2xx hydrate; per-item 4xx/5xx
/// (or an EWS SOAP fault) emit `ItemOutcome::Failed` with a structured
/// `AccountError` (Protocol::Graph / Protocol::Ews, AttemptCause
/// Acknowledged, classified `RecoveryClass`). Locally-invalid items (no body
/// where one is required) also emit `Failed` rather than poisoning the rest
/// of the batch.
async fn fetch_batch(
    account: &GraphAccount,
    ids: &[ObjectId],
    projection: Projection,
    is_final: bool,
) -> Result<Vec<SyncEvent<ItemOutcome<HydratedObject>>>, crate::error::GraphError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    // Public-folder ids read over EWS; everything else over Graph REST.
    let (ews_ids, ids) = partition_ews_ids(account, ids).await;
    let mut ews_outcomes = fetch_ews_outcomes(account, &ews_ids, projection).await;
    if ids.is_empty() {
        return Ok(vec![SyncEvent::Batch(Batch {
            items: ews_outcomes,
            page_boundary: if is_final {
                PageBoundary::Final
            } else {
                PageBoundary::Page
            },
            server_latency: std::time::Duration::default(),
            bytes_in: 0,
            checkpoint: None::<Checkpoint>,
        })]);
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
    // The EWS-hydrated items ride in the same batch as the REST ones: the
    // consumer sees one outcome per pulled id regardless of which arm served
    // it.
    let mut outcomes: Vec<ItemOutcome<HydratedObject>> = std::mem::take(&mut ews_outcomes);
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

    fn ews_item() -> crate::ews::EwsItem {
        crate::ews::EwsItem {
            item_id: "AAMkItem=".to_string(),
            change_key: Some("CK1".to_string()),
            subject: Some("Company Policy".to_string()),
            sender_email: Some("hr@contoso.com".to_string()),
            sender_name: Some("HR".to_string()),
            received_at: Some("2026-03-01T10:30:00Z".to_string()),
            body_preview: None,
            body_html: Some("<p>Read this</p>".to_string()),
            is_read: true,
            item_class: "IPM.Note".to_string(),
            to_recipients: Vec::new(),
            cc_recipients: Vec::new(),
            attachments: vec![
                crate::ews::EwsAttachment {
                    attachment_id: "AAMkAtt1=".to_string(),
                    name: Some("policy.pdf".to_string()),
                    content_type: Some("application/pdf".to_string()),
                    size: Some(2048),
                    is_inline: false,
                    is_item: false,
                },
                crate::ews::EwsAttachment {
                    attachment_id: "AAMkAtt2=".to_string(),
                    name: Some("Forwarded".to_string()),
                    content_type: None,
                    size: None,
                    is_inline: false,
                    is_item: true,
                },
            ],
        }
    }

    /// The EWS arm projects into the SAME `HydratedObject` shape the Graph
    /// REST arm does: `Metadata` for the body-bearing projections (never
    /// `RawMime`, which must be assembled RFC822), `FlagsOnly` for
    /// `FlagsOnly`, and one blob handle per attachment descriptor.
    #[test]
    fn ews_get_item_projects_into_the_graph_hydrated_shape() {
        let folder = FolderId("AAMkPF=".to_string());
        let id = super::super::foreign::encode_public_item_id(&folder, "AAMkItem=");
        let item = ews_item();

        for projection in [Projection::Metadata, Projection::Headers, Projection::Full] {
            let hydrated = hydrated_from_ews_item(
                id.clone(),
                &item,
                &folder,
                "content@contoso.com",
                projection,
            );
            // The hydrated id is the folder-qualified id the caller asked
            // for, identical to the inventory entry's.
            assert_eq!(hydrated.id, id);
            match &hydrated.kind {
                HydratedObjectKind::Metadata(entry) => {
                    assert_eq!(entry.id, id);
                    assert!(
                        entry
                            .memberships
                            .contains(&MembershipScope::Folder(folder.clone()))
                    );
                    assert!(entry.memberships.contains(&MembershipScope::Mailbox(
                        bifrost_types::MailboxId("content@contoso.com".to_string())
                    )));
                }
                other => panic!("expected Metadata for {projection:?}, got {other:?}"),
            }
            // Attachment metadata rides out as blob handles; the bytes come
            // through `open_blob` (EWS GetAttachment).
            assert_eq!(hydrated.blobs.len(), 2);
            // EWS has no byte-range attachment form.
            assert!(
                hydrated
                    .blobs
                    .iter()
                    .all(|b| !b.capabilities.supports_range)
            );
            assert_eq!(hydrated.blobs[0].size, Some(2048));
            assert_eq!(
                hydrated.blobs[0].content_type.as_deref(),
                Some("application/pdf")
            );
        }

        let flags = hydrated_from_ews_item(
            id.clone(),
            &item,
            &folder,
            "content@contoso.com",
            Projection::FlagsOnly,
        );
        match flags.kind {
            HydratedObjectKind::FlagsOnly(flags) => {
                assert!(flags.contains("\\seen"));
                assert_eq!(flags.len(), 1);
            }
            other => panic!("expected FlagsOnly, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn public_folder_ids_partition_onto_the_ews_arm() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
        let folder = FolderId("AAMkPF=".to_string());
        account
            .seed_public_folder_for_tests(
                folder.clone(),
                crate::account::cursor::PublicFolderRouting {
                    anchor_mailbox: "content@contoso.com".to_string(),
                    public_folder_mailbox: Some("pf@contoso.com".to_string()),
                },
            )
            .await;

        let public = super::super::foreign::encode_public_item_id(&folder, "AAMkItem=");
        let primary = ObjectId("AAMkmsg".to_string());
        // A folder-qualified id whose folder was never discovered has no
        // routing, so it stays on the REST arm rather than being dropped.
        let unknown = super::super::foreign::encode_public_item_id(
            &FolderId("AAMkOther=".to_string()),
            "AAMkItem=",
        );

        let (ews, rest) = partition_ews_ids(
            &account,
            &[public.clone(), primary.clone(), unknown.clone()],
        )
        .await;
        assert_eq!(ews, vec![(public, folder)]);
        assert_eq!(rest, vec![primary, unknown]);
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
