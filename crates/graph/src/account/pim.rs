use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

use base64::Engine;
use bifrost_types::{
    AccountError, AccountOperation, Address, AttachmentInline, Container, ContainerContentClass,
    ContainerId, ContainerKind, ContainerNamespace, ContainerRights, DraftHandle, DraftPatch,
    ErrorScope, FolderId, FolderRole, HydrationProjection, Identity, IdentityId, Importance,
    LabelId, MailboxId, Message, MutationTarget, ObjectId, Page, ProtocolErrorKind, ProtocolKind,
    Provenance, SearchFilter, SearchRequest, SendAs, ThreadHydration, ThreadId, VacationConfig,
    Warning, WarningKind,
};
use chrono::TimeZone;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::types::{
    BatchRequest, BatchRequestItem, BatchResponse, GraphMailFolder, ODataCollection,
};

use super::GraphAccount;
use super::GraphClient;
use super::blob::blob_handle_from_graph_attachment;
use super::graph_error::{
    GraphErrorContext, into_account_error, invalid_account_error, mutation_item_outcome,
    protocol_violation, unsupported_account_error,
};
use super::inventory::graph_etag;

const STARRED_CATEGORY: &str = "$flagged";
const PR_LAST_VERB_EXECUTED_ALIAS: &str = "PR_LAST_VERB_EXECUTED";
const PR_LAST_VERB_EXECUTED_GRAPH_ID: &str = "Integer 0x1081";
const DELETED_ITEMS: &str = "deletedItems";
const MSG_FOLDER_ROOT: &str = "msgfolderroot";

// Returned by send_message / draft_create when the request carries an
// AttachmentHandle minted by attachment_upload. Graph does not
// implement upload-session attachment_upload (capability flag false),
pub(crate) async fn add_to_container(
    account: GraphAccount,
    target: MutationTarget,
    container: ContainerId,
) -> Result<(), AccountError> {
    let ids = resolve_target_ids(&account, target, AccountOperation::AddToContainer).await?;
    move_messages(
        &account,
        &ids,
        &container.0,
        AccountOperation::AddToContainer,
    )
    .await
}

pub(crate) async fn set_category(
    account: GraphAccount,
    target: MutationTarget,
    category: String,
    value: bool,
) -> Result<(), AccountError> {
    let values = resolve_target_values(
        &account,
        target,
        "id,categories,flag,changeKey",
        AccountOperation::SetCategory,
    )
    .await?;
    let mut patches = Vec::new();
    for ResolvedMessage {
        routing_id,
        value: message,
    } in values
    {
        let id = routing_id;
        let etag = graph_etag(&message).ok_or_else(|| {
            pim_protocol_error(
                AccountOperation::SetCategory,
                Some(ErrorScope::Message { id: id.0.clone() }),
                format!("Graph message {} did not expose an etag", id.0),
            )
        })?;
        let body = if is_starred_category(&category) {
            json!({
                "flag": {
                    "flagStatus": if value { "flagged" } else { "notFlagged" }
                }
            })
        } else {
            let mut categories = categories_from_value(&message);
            if value {
                if !categories.iter().any(|existing| existing == &category) {
                    categories.push(category.clone());
                }
            } else {
                categories.retain(|existing| existing != &category);
            }
            categories.sort();
            json!({ "categories": categories })
        };
        patches.push(MessagePatch { id, body, etag });
    }
    patch_messages(&account, patches, AccountOperation::SetCategory).await
}

pub(crate) async fn set_extended_property(
    account: GraphAccount,
    target: MutationTarget,
    property_id: String,
    value: Option<String>,
) -> Result<(), AccountError> {
    let property_id = graph_extended_property_id(&property_id);
    match value {
        Some(value) => {
            let values = resolve_target_values(
                &account,
                target,
                "id,changeKey",
                AccountOperation::SetExtendedProperty,
            )
            .await?;
            let mut patches = Vec::new();
            for ResolvedMessage {
                routing_id,
                value: message,
            } in values
            {
                let id = routing_id;
                let etag = graph_etag(&message).ok_or_else(|| {
                    pim_protocol_error(
                        AccountOperation::SetExtendedProperty,
                        Some(ErrorScope::Message { id: id.0.clone() }),
                        format!("Graph message {} did not expose an etag", id.0),
                    )
                })?;
                patches.push(MessagePatch {
                    id,
                    body: json!({
                        "singleValueExtendedProperties": [{
                            "id": property_id,
                            "value": value
                        }]
                    }),
                    etag,
                });
            }
            patch_messages(&account, patches, AccountOperation::SetExtendedProperty).await
        }
        None => {
            // Clear: DELETE the property's navigation entry on each
            // resolved message. Graph answers 204 No Content on
            // success and 404 if the property never existed; the
            // batch helper tolerates 404 for the clear path so a
            // partial / never-set state still resolves to Ok.
            let ids =
                resolve_target_ids(&account, target, AccountOperation::SetExtendedProperty).await?;
            delete_extended_property(
                &account,
                &ids,
                &property_id,
                AccountOperation::SetExtendedProperty,
            )
            .await
        }
    }
}

async fn delete_extended_property(
    account: &GraphAccount,
    ids: &[ObjectId],
    property_id: &str,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    let mut requests = Vec::new();
    for (index, id) in ids.iter().enumerate() {
        // `ids` are the caller-supplied (possibly foreign-encoded) routing
        // ids; route the clear DELETE to the owning mailbox.
        let suffix = format!(
            "/singleValueExtendedProperties/{}",
            bifrost_net::url::encode_component(property_id)
        );
        requests.push(BatchRequestItem {
            id: index.to_string(),
            method: "DELETE".to_string(),
            url: message_batch_url(account, id, &suffix),
            body: None,
            headers: None,
        });
    }
    submit_write_batch(account, requests, true, operation).await
}

pub(crate) async fn set_is_read(
    account: GraphAccount,
    target: MutationTarget,
    is_read: bool,
) -> Result<(), AccountError> {
    let values = resolve_target_values(
        &account,
        target,
        "id,changeKey",
        AccountOperation::SetIsRead,
    )
    .await?;
    let mut patches = Vec::new();
    for ResolvedMessage {
        routing_id,
        value: message,
    } in values
    {
        let id = routing_id;
        let etag = graph_etag(&message).ok_or_else(|| {
            pim_protocol_error(
                AccountOperation::SetIsRead,
                Some(ErrorScope::Message { id: id.0.clone() }),
                format!("Graph message {} did not expose an etag", id.0),
            )
        })?;
        patches.push(MessagePatch {
            id,
            body: json!({ "isRead": is_read }),
            etag,
        });
    }
    patch_messages(&account, patches, AccountOperation::SetIsRead).await
}

/// Exclusive importance overwrite: one `If-Match`-conditioned PATCH of
/// `{ importance }` per resolved message. Graph's `importance` is a
/// single-valued field, so this is a strict single overwrite with no
/// read-modify-write - the consumer never expands one change into two.
pub(crate) async fn set_importance(
    account: GraphAccount,
    target: MutationTarget,
    level: Importance,
) -> Result<(), AccountError> {
    let values = resolve_target_values(
        &account,
        target,
        "id,changeKey",
        AccountOperation::SetImportance,
    )
    .await?;
    let body = graph_importance_body(level);
    let mut patches = Vec::new();
    for ResolvedMessage {
        routing_id,
        value: message,
    } in values
    {
        let id = routing_id;
        let etag = graph_etag(&message).ok_or_else(|| {
            pim_protocol_error(
                AccountOperation::SetImportance,
                Some(ErrorScope::Message { id: id.0.clone() }),
                format!("Graph message {} did not expose an etag", id.0),
            )
        })?;
        patches.push(MessagePatch {
            id,
            body: body.clone(),
            etag,
        });
    }
    patch_messages(&account, patches, AccountOperation::SetImportance).await
}

/// The single exclusive PATCH body for `set_importance`. Maps the
/// uniform level onto Graph's single-valued `importance` field - one
/// overwrite, never a clear-then-set pair.
fn graph_importance_body(level: Importance) -> Value {
    let wire = match level {
        Importance::Low => "low",
        Importance::High => "high",
        // `Normal` and any future variant collapse to Graph's `normal`.
        _ => "normal",
    };
    json!({ "importance": wire })
}

pub(crate) async fn send_message(
    account: GraphAccount,
    request: bifrost_types::SendRequest,
) -> Result<ObjectId, AccountError> {
    if !request.attachments_uploaded.is_empty() {
        return Err(unsupported_account_error(AccountOperation::Send));
    }
    if let Some(at) = request.scheduled {
        // Graph has no documented hard cap on deferred-send time;
        // rely on server rejection for an unreasonable window. Only
        // the past-instant rule is enforced client-side.
        bifrost_types::validate_scheduled(at, None)?;
    }
    let scheduled = request.scheduled;
    // Resolve the API-path client up front: a personal send uses the
    // primary `/me` client; a send-as routes the entire
    // draft-create-stamp-send cycle through the shared mailbox's
    // `/users/{id}` client so the draft is owned by the shared mailbox.
    let client = match &request.send_as {
        None => &account.client,
        Some(send_as) => account
            .shared_clients
            .get(&send_as.mailbox().0)
            .ok_or_else(|| send_as_unknown_mailbox(send_as.mailbox()))?,
    };
    let mut message = message_from_send_request(&request)?;
    if let Some(send_as) = &request.send_as {
        apply_send_as(&mut message, send_as, account.user_email.as_deref());
    }
    let draft = create_draft_message(client, message).await?;
    if let Some(at) = scheduled {
        // Stamp PidTagDeferredSendTime on the draft before send so
        // Graph queues it for deferred delivery.
        stamp_deferred_send_time(client, &draft, at, AccountOperation::Send).await?;
    }
    send_draft_message(client, &draft).await?;
    // For a scheduled send-as, the draft lives in the shared mailbox, not
    // `/me`; the cancel/reschedule handle must carry the owning mailbox so
    // those ops route to the same `/users/{id}` client the draft was
    // created on (a bare draft id would 404 against `/me`). A non-scheduled
    // send has nothing to cancel, so the discriminator is moot there.
    let owning_mailbox = scheduled
        .and(request.send_as.as_ref())
        .map(|send_as| send_as.mailbox().0.clone());
    Ok(ObjectId(encode_scheduled_send_handle(
        owning_mailbox.as_deref(),
        &draft.0,
    )))
}

pub(crate) async fn send_raw_message(
    account: GraphAccount,
    raw: bytes::Bytes,
    _save_to_sent: Option<bool>,
) -> Result<ObjectId, AccountError> {
    // Graph has no raw-MIME sendMail. It imports raw MIME by POSTing the
    // base64 of the octets to `/messages` with `Content-Type: text/plain`,
    // which creates a draft; we then send that draft. Graph always files
    // the sent copy in Sent, so `save_to_sent` has no Graph-side toggle.
    let client = &account.client;
    let base64_mime = base64::engine::general_purpose::STANDARD.encode(&raw);
    let path = format!("{}/messages", client.api_path_prefix());
    let created: Value = client
        .post_mime(&path, bytes::Bytes::from(base64_mime))
        .await
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(AccountOperation::Send)))?;
    let id = created.get("id").and_then(Value::as_str).ok_or_else(|| {
        pim_protocol_error(
            AccountOperation::Send,
            None,
            "Graph MIME import did not return an id",
        )
    })?;
    let draft = DraftHandle(id.to_string());
    send_draft_message(client, &draft).await?;
    Ok(ObjectId(draft.0))
}

/// Reserved separator for the scheduled-send cancel/reschedule handle.
/// A handle of the form `"<mailbox>\u{1f}<draft id>"` is a send-as
/// scheduled draft owned by a shared mailbox; a plain id is a primary
/// (`/me`) draft. `\u{1f}` (US) cannot appear in a Graph message id or an
/// SMTP address / user id, so it is an unambiguous delimiter (mirrors the
/// foreign-folder codec in `foreign.rs`).
const SCHEDULED_SEND_HANDLE_SEP: char = '\u{1f}';

/// Encode the owning-mailbox discriminator into a scheduled-send handle.
/// `None` mailbox yields a bare draft id (primary `/me` draft).
fn encode_scheduled_send_handle(mailbox: Option<&str>, draft_id: &str) -> String {
    match mailbox {
        Some(mailbox) => format!("{mailbox}{SCHEDULED_SEND_HANDLE_SEP}{draft_id}"),
        None => draft_id.to_string(),
    }
}

/// Split a scheduled-send handle back into `(owning mailbox, draft id)`.
/// A handle with no separator is a primary draft (`None` mailbox).
fn decode_scheduled_send_handle(handle: &str) -> (Option<&str>, &str) {
    match handle.split_once(SCHEDULED_SEND_HANDLE_SEP) {
        Some((mailbox, draft_id)) => (Some(mailbox), draft_id),
        None => (None, handle),
    }
}

/// Resolve the `GraphClient` a scheduled-send handle was created on: the
/// shared-mailbox client when the handle carries an owning mailbox, else
/// the primary client.
fn client_for_scheduled_send_handle<'a>(
    account: &'a GraphAccount,
    mailbox: Option<&str>,
) -> Result<&'a GraphClient, AccountError> {
    match mailbox {
        None => Ok(&account.client),
        Some(mailbox) => account
            .shared_clients
            .get(mailbox)
            .ok_or_else(|| send_as_unknown_mailbox(&MailboxId(mailbox.to_string()))),
    }
}

/// Stamp Graph's `from` / `sender` fields from a `SendAs` directive.
/// `from` is the author header; `sender` is the on-behalf-of
/// discriminator. The two arms differ deliberately on `from`:
/// `As` overrides any consumer-set `from` (`insert`), because its
/// contract is author == sender == mailbox; `OnBehalfOf` honors a
/// consumer-set `from` (`entry(..).or_insert_with`), filling it only
/// when absent, because an explicit author is a legitimate divergence
/// there.
fn apply_send_as(message: &mut Value, send_as: &SendAs, user_email: Option<&str>) {
    let mailbox = send_as.mailbox();
    let mailbox_recipient = json!({ "emailAddress": { "address": mailbox.0 } });
    // `message_from_draft_patch` always returns `Value::Object`.
    let obj = message.as_object_mut().expect("message is an object");
    match send_as {
        SendAs::As(_) => {
            obj.insert("from".to_string(), mailbox_recipient.clone());
            obj.insert("sender".to_string(), mailbox_recipient);
        }
        SendAs::OnBehalfOf(_) => {
            obj.entry("from")
                .or_insert_with(|| mailbox_recipient.clone());
            // When the config carries the authenticated user's own
            // address, stamp it as `sender`; otherwise omit `sender`
            // and let Graph populate it from the authenticated context.
            if let Some(me) = user_email {
                obj.insert(
                    "sender".to_string(),
                    json!({ "emailAddress": { "address": me } }),
                );
            }
        }
        // `SendAs` is `#[non_exhaustive]`; a future mode falls back to
        // the most conservative stamping (author == sender == mailbox),
        // so a new variant can never leak the authenticated user's own
        // mailbox as the visible sender.
        _ => {
            obj.insert("from".to_string(), mailbox_recipient.clone());
            obj.insert("sender".to_string(), mailbox_recipient);
        }
    }
}

/// A `send_as` request targeting a mailbox not registered on this
/// account (`shared_clients` is seeded at construction). The provider
/// supports send-as; this specific mailbox is just not configured, so
/// it is a caller error (`Request(Malformed)`), not `Unsupported`.
#[must_use]
fn send_as_unknown_mailbox(mailbox: &MailboxId) -> AccountError {
    invalid_account_error(
        AccountOperation::Send,
        format!(
            "shared mailbox not configured on this account: {}",
            mailbox.0
        ),
    )
}

/// MAPI proptag form for `PidTagDeferredSendTime` (`PT_SYSTIME 0x3FEF`),
/// the single-valued extended property Graph reads to defer a send.
const DEFERRED_SEND_TIME_PROPERTY_ID: &str = "SystemTime 0x3FEF";

/// PATCH a draft's `PidTagDeferredSendTime` extended property to `at`,
/// serialized as ISO-8601 UTC. Used both by the scheduled send path and
/// by `reschedule_send` (Graph reschedule is an in-place PATCH).
async fn stamp_deferred_send_time(
    client: &GraphClient,
    draft: &DraftHandle,
    at: std::time::SystemTime,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    let path = format!(
        "{}/messages/{}",
        client.api_path_prefix(),
        bifrost_net::url::encode_component(&draft.0)
    );
    let body = deferred_send_time_body(at);
    client
        .patch(&path, &body)
        .await
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(operation)))
}

/// The `MessagePatch` body that stamps `PidTagDeferredSendTime` on a
/// Graph draft.
fn deferred_send_time_body(at: std::time::SystemTime) -> Value {
    json!({
        "singleValueExtendedProperties": [{
            "id": DEFERRED_SEND_TIME_PROPERTY_ID,
            "value": graph_iso8601_utc(at)
        }]
    })
}

/// Format an absolute instant as ISO-8601 UTC for the Graph
/// `singleValueExtendedProperty` value (matches Graph's PT_SYSTIME wire
/// shape, e.g. `2026-06-16T10:00:00Z`).
fn graph_iso8601_utc(at: std::time::SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(at)
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

pub(crate) async fn cancel_scheduled_send(
    account: GraphAccount,
    handle: ObjectId,
) -> Result<(), AccountError> {
    // A Graph deferred message sits in the mailbox until its send time;
    // deleting it cancels the send. The handle carries the owning mailbox
    // for a send-as draft so the DELETE routes to the same client the
    // draft was created on (else a shared-mailbox draft 404s against /me).
    let (mailbox, draft_id) = decode_scheduled_send_handle(&handle.0);
    let client = client_for_scheduled_send_handle(&account, mailbox)?;
    let path = format!(
        "{}/messages/{}",
        client.api_path_prefix(),
        bifrost_net::url::encode_component(draft_id)
    );
    client.delete(&path).await.map_err(|e| {
        into_account_error(
            e,
            GraphErrorContext::graph(AccountOperation::CancelScheduledSend),
        )
    })
}

pub(crate) async fn reschedule_send(
    account: GraphAccount,
    handle: ObjectId,
    scheduled: std::time::SystemTime,
) -> Result<ObjectId, AccountError> {
    bifrost_types::validate_scheduled(scheduled, None)?;
    // Graph reschedule is an in-place PATCH of the deferred-send time.
    // Route through the client the draft was created on (the handle's
    // owning-mailbox discriminator) and patch the native draft id.
    let (mailbox, draft_id) = decode_scheduled_send_handle(&handle.0);
    let client = client_for_scheduled_send_handle(&account, mailbox)?;
    let draft = DraftHandle(draft_id.to_string());
    stamp_deferred_send_time(client, &draft, scheduled, AccountOperation::RescheduleSend).await?;
    Ok(handle)
}

pub(crate) async fn draft_create(
    account: GraphAccount,
    patch: DraftPatch,
) -> Result<DraftHandle, AccountError> {
    if patch
        .attachments_uploaded
        .as_ref()
        .is_some_and(|attachments| !attachments.is_empty())
    {
        return Err(unsupported_account_error(AccountOperation::DraftCreate));
    }
    let message = message_from_draft_patch(&patch, true)?;
    create_draft_message(&account.client, message).await
}

pub(crate) async fn draft_update(
    account: GraphAccount,
    draft: DraftHandle,
    patch: DraftPatch,
) -> Result<(), AccountError> {
    if patch.attachments_inline.is_some() || patch.attachments_uploaded.is_some() {
        return Err(unsupported_account_error(AccountOperation::DraftUpdate));
    }
    let message = message_from_draft_patch(&patch, false)?;
    let path = format!(
        "{}/messages/{}",
        account.client.api_path_prefix(),
        bifrost_net::url::encode_component(&draft.0)
    );
    account
        .client
        .patch(&path, &message)
        .await
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(AccountOperation::DraftUpdate)))
}

pub(crate) async fn draft_discard(
    account: GraphAccount,
    draft: DraftHandle,
) -> Result<(), AccountError> {
    let path = format!(
        "{}/messages/{}",
        account.client.api_path_prefix(),
        bifrost_net::url::encode_component(&draft.0)
    );
    account.client.delete(&path).await.map_err(|e| {
        into_account_error(e, GraphErrorContext::graph(AccountOperation::DraftDiscard))
    })
}

pub(crate) async fn draft_send(
    account: GraphAccount,
    draft: DraftHandle,
) -> Result<ObjectId, AccountError> {
    send_draft_message(&account.client, &draft).await?;
    Ok(ObjectId(draft.0))
}

pub(crate) async fn search(
    account: GraphAccount,
    request: SearchRequest,
) -> Result<Page<ThreadId>, AccountError> {
    let page = search_message_rows(&account, request).await?;
    let mut seen = HashSet::new();
    let mut threads = Vec::new();
    for row in page.items {
        let thread = row.thread_id.unwrap_or(ThreadId(row.id.0));
        if seen.insert(thread.0.clone()) {
            threads.push(thread);
        }
    }
    Ok(Page {
        items: threads,
        next_cursor: page.next_cursor,
        estimated_total: page.estimated_total,
        failed_ids: Vec::new(),
    })
}

pub(crate) async fn search_messages(
    account: GraphAccount,
    request: SearchRequest,
) -> Result<Page<ObjectId>, AccountError> {
    let page = search_message_rows(&account, request).await?;
    Ok(Page {
        items: page.items.into_iter().map(|row| row.id).collect(),
        next_cursor: page.next_cursor,
        estimated_total: page.estimated_total,
        failed_ids: Vec::new(),
    })
}

pub(crate) async fn containers_list(account: GraphAccount) -> Result<Vec<Container>, AccountError> {
    let folders = account
        .client
        .list_mail_folders_recursive()
        .await
        .map_err(|e| {
            into_account_error(
                e,
                GraphErrorContext::graph(AccountOperation::DiscoverMemberships),
            )
        })?;
    account.folder_tree.write().await.replace_mail_folders(
        folders
            .iter()
            .map(|folder| (folder.id.clone(), folder.parent_folder_id.clone())),
    );
    let roles = well_known_folder_roles(&account).await;
    let mut containers: Vec<Container> = folders
        .into_iter()
        .map(|folder| container_from_folder(folder, &roles, None))
        .collect();

    // Shared (delegate) mailboxes, then public folders. Both are additive:
    // a failure in either leg leaves the primary mailbox's containers intact.
    let (shared, warnings) = shared_containers(&account).await;
    containers.extend(shared);
    // `containers_list` has no warning lane in the `Account` trait, so a
    // per-mailbox degradation is logged here; the `Warning` values are built
    // in the same shape the discovery path emits so the behavior stays
    // testable.
    for warning in &warnings {
        tracing::warn!("[Graph] containers_list: {}", warning.message.as_str());
    }
    containers.extend(public_folder_containers(&account).await);
    Ok(containers)
}

/// Enumerate each configured shared (delegate) mailbox's folders as
/// `Shared`-namespace containers.
///
/// Each container's `native_id` is the foreign-encoded
/// `encode_foreign(mailbox, folderId)` - byte-identical to the
/// `CursorScope::FolderType` string `discover_cursor_scopes` emits for the
/// same folder, which is what lets the consumer join a container to its sync
/// scope - while `owner_local_id` keeps the bare Graph folder id for requests
/// made against the owner's own mailbox.
///
/// A per-mailbox enumeration failure degrades to a `Warning` plus the
/// remaining containers, matching the shape the discovery path already uses:
/// one revoked share must not blank the whole sidebar.
async fn shared_containers(account: &GraphAccount) -> (Vec<Container>, Vec<Warning>) {
    let mut containers = Vec::new();
    let mut warnings = Vec::new();
    // Deterministic order so the projection is stable across calls.
    let mut mailboxes: Vec<&String> = account.shared_clients.keys().collect();
    mailboxes.sort();
    for mailbox in mailboxes {
        let client = &account.shared_clients[mailbox];
        match client.list_mail_folders_recursive().await {
            Ok(folders) => containers.extend(
                folders
                    .into_iter()
                    // A shared mailbox's well-known folder ids are not the
                    // primary's, so the primary role map must not be applied
                    // here; role falls back to the well-known-name match.
                    .map(|folder| container_from_folder(folder, &HashMap::new(), Some(mailbox))),
            ),
            Err(_) => warnings.push(Warning::support_only(
                WarningKind::OperatorAttentionNeeded,
                format!("shared mailbox {mailbox} skipped: folder listing failed"),
            )),
        }
    }
    (containers, warnings)
}

/// Project every discovered public folder as a `Public`-namespace container.
///
/// Reads the `routing_map` / `public_folder_meta` pair that
/// `discover_public_folder_scopes` seeds, so this is purely local - no EWS
/// round-trip. `SyncEngine::attach` drives scope discovery synchronously
/// before any `containers_list` call, which is what makes the maps populated
/// by the time this runs.
///
/// The full readable hierarchy projects here, including folders that are NOT
/// pinned for sync: the consumer has to see a folder before it can decide to
/// pin it.
async fn public_folder_containers(account: &GraphAccount) -> Vec<Container> {
    // Snapshot the routing keys and release that guard before taking the
    // metadata one: the two maps are never held simultaneously.
    let mut native_ids: Vec<String> = {
        let routing = account.routing_map.read().await;
        routing.keys().map(|folder| folder.0.clone()).collect()
    };
    // Deterministic order so the projection is stable across calls.
    native_ids.sort();
    let meta = account.public_folder_meta.read().await;
    native_ids
        .into_iter()
        .map(|native| {
            let meta = meta.get(&bifrost_types::FolderId(native.clone()));
            let name = meta
                .map(|meta| meta.display_name.clone())
                .unwrap_or_else(|| native.clone());
            Container::new(
                ContainerId(native.clone()),
                ContainerKind::Folder,
                // A public folder plays no ratatoskr mailbox role: it is not
                // anyone's Inbox / Sent / Trash.
                None,
                Provenance {
                    provider: ProtocolKind::Graph,
                    kind: ContainerKind::Folder,
                    native: native.clone(),
                },
                name,
                meta.and_then(|meta| meta.parent.clone())
                    .map(|parent| ContainerId(parent.0)),
            )
            .with_namespace(ContainerNamespace::Public)
            // A public folder is owned by the organization, not by a
            // principal, so there is no owner mailbox to name and no
            // owner-local id space to translate into.
            .with_content_class(
                meta.and_then(|meta| content_class_from_folder_class(meta.folder_class.as_deref())),
            )
            .with_rights(meta.map(|meta| rights_from_effective_rights(&meta.effective_rights)))
        })
        .collect()
}

/// Map an EWS `FolderClass` onto the unified [`ContainerContentClass`].
///
/// A public folder is typed at the folder level and a mail client must not
/// present an `IPF.Appointment` folder as a mail folder. `None` when the
/// server reported no class at all (distinct from
/// `Some(ContainerContentClass::Other)`, which is "typed, but as something
/// this surface does not model").
fn content_class_from_folder_class(folder_class: Option<&str>) -> Option<ContainerContentClass> {
    let class = folder_class?;
    // The wire form is `IPF.Note`, `IPF.Note.Something`, `IPF.Appointment`,
    // ...; match on the leading segment so a subtype does not fall to Other.
    let normalized = class.trim().to_ascii_lowercase();
    Some(if normalized.starts_with("ipf.note") {
        ContainerContentClass::Mail
    } else if normalized.starts_with("ipf.appointment") {
        ContainerContentClass::Calendar
    } else if normalized.starts_with("ipf.contact") {
        ContainerContentClass::Contacts
    } else {
        ContainerContentClass::Other
    })
}

/// Project EWS folder `EffectiveRights` onto the unified
/// [`ContainerRights`].
///
/// Every member the EWS shape speaks to is `Some(_)`: the server answered,
/// so an absent right is a definite "no". `may_submit` stays `None` - EWS
/// folder rights say nothing about submission (a public folder has no
/// submission address), and reporting `Some(false)` would claim knowledge the
/// wire never provided.
fn rights_from_effective_rights(rights: &crate::ews::EwsEffectiveRights) -> ContainerRights {
    ContainerRights {
        may_read_items: Some(rights.read),
        may_add_items: Some(rights.create_contents),
        may_remove_items: Some(rights.delete),
        // EWS has no per-flag right; `Modify` is the whole item-mutation
        // gate, so both keyword members map to it.
        may_set_seen: Some(rights.modify),
        may_set_keywords: Some(rights.modify),
        may_create_child: Some(rights.create_hierarchy),
        may_rename: Some(rights.modify),
        may_delete: Some(rights.delete),
        may_submit: None,
    }
}

pub(crate) async fn container_create(
    account: GraphAccount,
    kind: ContainerKind,
    name: String,
    parent: Option<ContainerId>,
    // Graph mail folders carry no container color (Graph categories
    // are message flags, not containers); accepted for trait parity
    // with the colorable (Gmail) path and ignored.
    _style: Option<bifrost_types::ContainerStyle>,
) -> Result<ContainerId, AccountError> {
    if kind != ContainerKind::Folder {
        return Err(unsupported_account_error(AccountOperation::ContainerCreate));
    }
    let prefix = account.client.api_path_prefix();
    let path = match parent {
        Some(parent) => format!(
            "{prefix}/mailFolders/{}/childFolders",
            bifrost_net::url::encode_component(&parent.0)
        ),
        None => format!("{prefix}/mailFolders"),
    };
    let folder: GraphMailFolder = account
        .client
        .post(&path, &json!({ "displayName": name }))
        .await
        .map_err(|e| {
            into_account_error(
                e,
                GraphErrorContext::graph(AccountOperation::ContainerCreate),
            )
        })?;
    Ok(ContainerId(folder.id))
}

pub(crate) async fn container_rename(
    account: GraphAccount,
    container: ContainerId,
    name: String,
    // Graph has no mail-folder recolor; accepted for trait parity and ignored.
    _style: Option<bifrost_types::ContainerStyle>,
) -> Result<(), AccountError> {
    let path = format!(
        "{}/mailFolders/{}",
        account.client.api_path_prefix(),
        bifrost_net::url::encode_component(&container.0)
    );
    account
        .client
        .patch(&path, &json!({ "displayName": name }))
        .await
        .map_err(|e| {
            into_account_error(
                e,
                GraphErrorContext::graph(AccountOperation::ContainerRename),
            )
        })
}

pub(crate) async fn container_move(
    account: GraphAccount,
    container: ContainerId,
    new_parent: Option<ContainerId>,
) -> Result<(), AccountError> {
    let destination_id = new_parent.map_or_else(|| MSG_FOLDER_ROOT.to_string(), |id| id.0);
    let path = format!(
        "{}/mailFolders/{}/move",
        account.client.api_path_prefix(),
        bifrost_net::url::encode_component(&container.0)
    );
    let _: GraphMailFolder = account
        .client
        .post(&path, &json!({ "destinationId": destination_id }))
        .await
        .map_err(|e| {
            into_account_error(e, GraphErrorContext::graph(AccountOperation::ContainerMove))
        })?;
    Ok(())
}

pub(crate) async fn container_delete(
    account: GraphAccount,
    container: ContainerId,
) -> Result<(), AccountError> {
    let path = format!(
        "{}/mailFolders/{}",
        account.client.api_path_prefix(),
        bifrost_net::url::encode_component(&container.0)
    );
    account.client.delete(&path).await.map_err(|e| {
        into_account_error(
            e,
            GraphErrorContext::graph(AccountOperation::ContainerDelete),
        )
    })
}

pub(crate) async fn identities_list(account: GraphAccount) -> Result<Vec<Identity>, AccountError> {
    let profile = account.client.get_profile().await.map_err(|e| {
        into_account_error(
            e,
            GraphErrorContext::graph(AccountOperation::IdentitiesList),
        )
    })?;
    let address = profile.mail.or(profile.user_principal_name);
    let Some(address) = address else {
        return Ok(Vec::new());
    };
    Ok(vec![Identity {
        id: IdentityId(address.clone()),
        name: profile.display_name.unwrap_or_default(),
        address,
        signature_text: None,
        signature_html: None,
        reply_to: None,
        is_default: true,
    }])
}

pub(crate) async fn vacation_get(
    account: GraphAccount,
) -> Result<Option<VacationConfig>, AccountError> {
    let path = format!(
        "{}/mailboxSettings?$select=automaticRepliesSetting",
        account.client.api_path_prefix()
    );
    let settings: MailboxSettings = account.client.get_json(&path).await.map_err(|e| {
        into_account_error(e, GraphErrorContext::graph(AccountOperation::VacationGet))
    })?;
    Ok(settings.automatic_replies_setting.map(vacation_from_graph))
}

pub(crate) async fn vacation_set(
    account: GraphAccount,
    config: VacationConfig,
) -> Result<(), AccountError> {
    let status = if config.is_enabled {
        if config.starts_at.is_some() || config.ends_at.is_some() {
            "scheduled"
        } else {
            "alwaysEnabled"
        }
    } else {
        "disabled"
    };
    let body = config
        .body_html
        .clone()
        .or(config.body_text.clone())
        .unwrap_or_default();
    let setting = json!({
        "automaticRepliesSetting": {
            "status": status,
            "internalReplyMessage": body,
            "externalReplyMessage": body,
            "externalAudience": "all",
            "scheduledStartDateTime": graph_datetime_or_default(config.starts_at),
            "scheduledEndDateTime": graph_datetime_or_default(config.ends_at)
        }
    });
    let path = format!("{}/mailboxSettings", account.client.api_path_prefix());
    account
        .client
        .patch(&path, &setting)
        .await
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(AccountOperation::VacationSet)))
}

pub(crate) async fn thread_hydrate(
    account: GraphAccount,
    thread: ThreadId,
) -> Result<ThreadHydration, AccountError> {
    let values = message_values_for_thread(&account, &thread, hydrate_select(true)).await?;
    let mut messages = Vec::new();
    for value in values {
        messages.push(message_from_value(&value, HydrationProjection::Full)?);
    }
    messages.sort_by_key(|message| message.date);
    Ok(ThreadHydration {
        id: thread,
        messages,
    })
}

pub(crate) async fn message_hydrate(
    account: GraphAccount,
    message: ObjectId,
    projection: HydrationProjection,
) -> Result<Message, AccountError> {
    // The single-id hydration door has to make the SAME transport decision
    // the batch door (`get.rs::partition_ews_ids`) makes: a public-folder id
    // wraps a raw EWS `ItemId`, and no Graph REST route can address one.
    // Sending it down `/me/messages/{native}` 404s naming the bare item id,
    // which is precisely what a consumer reaching hydration through the
    // engine's passthrough saw.
    if let Some(folder) = ews_read_folder(&message) {
        return public_message_hydrate(&account, &message, &folder, projection).await;
    }
    let value =
        fetch_message_value(&account, &message, hydrate_select(expand_blobs(projection))).await?;
    message_from_value(&value, projection)
}

/// The public folder a read of `id` must route through EWS `GetItem`, or
/// `None` when the id reads over Graph REST (primary or shared-mailbox).
///
/// The transport decision lives in the id itself: `PUBLIC_SEP` is the
/// discriminator, and the folder it carries is the `routing_map` key holding
/// the item's `X-AnchorMailbox` / `X-PublicFolderMailbox` pair. Extracted so
/// every door (batch hydrate, single hydrate, blob read) decides identically
/// and the decision is unit-pinnable without a live server.
pub(crate) fn ews_read_folder(id: &ObjectId) -> Option<FolderId> {
    super::foreign::parse_message_id(id)
        .public_folder()
        .map(|folder| FolderId(folder.to_string()))
}

/// Hydrate one public-folder item over EWS `GetItem`, routed by its folder's
/// `routing_map` headers - the single-id peer of `get.rs::fetch_ews_outcomes`.
async fn public_message_hydrate(
    account: &GraphAccount,
    id: &ObjectId,
    folder: &FolderId,
    projection: HydrationProjection,
) -> Result<Message, AccountError> {
    let Some(routing) = account.public_folder_routing(folder).await else {
        return Err(protocol_violation(
            ProtocolErrorKind::MissingField,
            AccountOperation::HydrateMessage,
            Some(ErrorScope::Message { id: id.0.clone() }),
            format!("public folder {} has no routing entry", folder.0),
        ));
    };
    let Some(ews) = super::public_folder::ews_client(account) else {
        return Err(super::graph_error::ews_error_to_account_error(
            crate::ews::EwsError::Transport(bifrost_net::Error::Network {
                message: "EWS account net not attached".to_string(),
                transmission_state: bifrost_types::TransmissionState::Unsent,
                source: None,
            }),
            GraphErrorContext::ews(AccountOperation::HydrateMessage)
                .with_scope(ErrorScope::Message { id: id.0.clone() }),
        ));
    };
    let native = super::foreign::parse_message_id(id).native_id().to_string();
    let item = ews
        .get_item(&native, &routing.headers())
        .await
        .map_err(|error| {
            super::graph_error::ews_error_to_account_error(
                error,
                GraphErrorContext::ews(AccountOperation::HydrateMessage)
                    .with_scope(ErrorScope::Message { id: id.0.clone() }),
            )
        })?;
    Ok(message_from_ews_item(id.clone(), &item, folder, projection))
}

/// Project an EWS `GetItem` result into the user-facing `Message` shape.
///
/// EWS returns a parsed body, not MIME octets, so `body_html` carries the
/// HTML part and `body_text` the `BodyPreview` text. Attachments ride out as
/// EWS blob handles (`GetAttachment` fetches the bytes), and the containing
/// public folder is the item's one membership. Pure over the already-fetched
/// item so the projection is unit-pinnable without a live EWS server.
fn message_from_ews_item(
    id: ObjectId,
    item: &crate::ews::EwsItem,
    folder: &FolderId,
    projection: HydrationProjection,
) -> Message {
    let (body_text, body_html) = match projection {
        HydrationProjection::Headers => (None, None),
        HydrationProjection::Preview(limit) => (
            item.body_preview
                .as_ref()
                .map(|preview| preview.chars().take(limit).collect()),
            None,
        ),
        _ => (item.body_preview.clone(), item.body_html.clone()),
    };
    let attachments = if expand_blobs(projection) {
        item.attachments
            .iter()
            .map(|attachment| super::blob::blob_handle_from_ews_attachment(&id, attachment))
            .collect()
    } else {
        Vec::new()
    };
    let mut flags = HashSet::new();
    if item.is_read {
        flags.insert("\\seen".to_string());
    }
    Message {
        id,
        // EWS `GetItem` on the message shape returns no conversation id,
        // and a public item has no Graph conversation to join.
        thread_id: None,
        from: item
            .sender_email
            .iter()
            .map(|address| Address {
                name: item.sender_name.clone(),
                address: address.clone(),
            })
            .collect(),
        to: item.to_recipients.iter().map(ews_address).collect(),
        cc: item.cc_recipients.iter().map(ews_address).collect(),
        bcc: Vec::new(),
        reply_to: Vec::new(),
        subject: item.subject.clone(),
        date: item.received_at.as_deref().and_then(parse_graph_datetime),
        containers: vec![ContainerId(folder.0.clone())],
        flags,
        // EWS surfaces only the read bit on this shape; no importance field
        // is requested, so every public item is Normal.
        importance: Importance::Normal,
        body_text,
        body_html,
        attachments,
        size_bytes: None,
        in_reply_to: None,
        references: Vec::new(),
    }
}

fn ews_address(recipient: &crate::ews::EwsRecipient) -> Address {
    Address {
        name: recipient.name.clone(),
        address: recipient.email.clone(),
    }
}

pub(crate) async fn move_thread(
    account: GraphAccount,
    thread: ThreadId,
    target: ContainerId,
) -> Result<(), AccountError> {
    add_to_container(account, MutationTarget::Thread(thread), target).await
}

pub(crate) async fn delete_thread(
    account: GraphAccount,
    thread: ThreadId,
    current: Option<ContainerId>,
) -> Result<(), AccountError> {
    let ids = resolve_target_ids(
        &account,
        MutationTarget::Thread(thread),
        AccountOperation::BulkMove,
    )
    .await?;
    let trash = trash_container_id(&account).await;
    let already_in_trash = current
        .as_ref()
        .is_some_and(|id| id.0 == trash.0 || id.0.eq_ignore_ascii_case(DELETED_ITEMS));
    if already_in_trash {
        destroy_messages(&account, &ids, AccountOperation::BulkDestroy).await
    } else {
        move_messages(&account, &ids, &trash.0, AccountOperation::BulkMove).await
    }
}

struct MessagePatch {
    id: ObjectId,
    body: Value,
    etag: String,
}

async fn resolve_target_ids(
    account: &GraphAccount,
    target: MutationTarget,
    operation: AccountOperation,
) -> Result<Vec<ObjectId>, AccountError> {
    match target {
        MutationTarget::Message(id) => Ok(vec![id]),
        MutationTarget::Thread(thread) => {
            let values = message_values_for_thread(account, &thread, "id").await?;
            values
                .iter()
                .map(|value| object_id_from_value(value, operation))
                .collect()
        }
        _ => Err(unsupported_account_error(operation)),
    }
}

/// A resolved message for a write: the routing id (the caller-supplied,
/// possibly foreign-encoded `ObjectId` that the conditioned write must
/// route by) paired with its fetched value (carrying the etag/body). For
/// a `Thread` target the routing id is the native id the server listed -
/// thread queries run against `/me`, so those items are primary-mailbox.
struct ResolvedMessage {
    routing_id: ObjectId,
    value: Value,
}

async fn resolve_target_values(
    account: &GraphAccount,
    target: MutationTarget,
    select: &str,
    operation: AccountOperation,
) -> Result<Vec<ResolvedMessage>, AccountError> {
    match target {
        MutationTarget::Message(id) => {
            let value = fetch_message_value(account, &id, select).await?;
            Ok(vec![ResolvedMessage {
                routing_id: id,
                value,
            }])
        }
        MutationTarget::Thread(thread) => {
            let values = message_values_for_thread(account, &thread, select).await?;
            values
                .into_iter()
                .map(|value| {
                    let routing_id = object_id_from_value(&value, operation)?;
                    Ok(ResolvedMessage { routing_id, value })
                })
                .collect()
        }
        _ => Err(unsupported_account_error(operation)),
    }
}

async fn fetch_message_value(
    account: &GraphAccount,
    id: &ObjectId,
    select: &str,
) -> Result<Value, AccountError> {
    // Decode the (possibly foreign-encoded) id so a shared-mailbox
    // message reads from `/users/{owner}/messages/{native}`; a primary
    // (bare) id stays on `/me`. This makes the typed `message_hydrate`
    // and every pim read-modify-write that starts from `fetch_message_value`
    // mailbox-correct.
    let parsed = super::foreign::parse_message_id(id);
    let client = account.client_for_owner(parsed.owner());
    let path = format!(
        "{}/messages/{}?{}",
        client.api_path_prefix(),
        bifrost_net::url::encode_component(parsed.native_id()),
        select_query(select)
    );
    let value = client
        .get_json(&path)
        .await
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(AccountOperation::Hydrate)))?;
    // Cache the etag under the encoded id (the key the mutation paths
    // look up), preserving the owner so the conditioned write routes
    // back to the same mailbox.
    cache_etag_for(account, id, &value).await;
    Ok(value)
}

async fn message_values_for_thread(
    account: &GraphAccount,
    thread: &ThreadId,
    select: &str,
) -> Result<Vec<Value>, AccountError> {
    let filter = format!("conversationId eq {}", odata_quoted(&thread.0));
    let path = format!(
        "{}/messages?{}&$filter={}&$top=50",
        account.client.api_path_prefix(),
        select_query(select),
        bifrost_net::url::encode_component(&filter)
    );
    fetch_paged_values(account, path).await
}

async fn fetch_paged_values(
    account: &GraphAccount,
    first_url: String,
) -> Result<Vec<Value>, AccountError> {
    let ctx = GraphErrorContext::graph(AccountOperation::Hydrate);
    let mut values = Vec::new();
    let mut next_url = Some(first_url);
    while let Some(url) = next_url {
        let page: ODataCollection<Value> = if url.starts_with("http") {
            account.client.get_absolute(&url).await
        } else {
            account.client.get_json(&url).await
        }
        .map_err(|e| into_account_error(e, ctx.clone()))?;
        values.extend(page.value);
        next_url = page.next_link;
    }
    Ok(values)
}

/// Build a per-message `$batch` request URL, decoding the (possibly
/// foreign-encoded) id and routing to the owning mailbox: a shared-mailbox
/// message yields `/users/{owner}/messages/{native}{suffix}`, a primary
/// (bare) id `/me/messages/{id}{suffix}`. `suffix` is the trailing path
/// segment after the message id (`""`, `"/move"`, or a
/// `/singleValueExtendedProperties/...` clear). The `$batch` envelope is
/// always posted on the primary client; routing rides entirely in the
/// per-item URL prefix, mirroring the read paths in `get.rs`.
pub(crate) fn message_batch_url(account: &GraphAccount, id: &ObjectId, suffix: &str) -> String {
    let parsed = super::foreign::parse_message_id(id);
    let prefix = account.client_for_owner(parsed.owner()).api_path_prefix();
    let enc_id = bifrost_net::url::encode_component(parsed.native_id());
    format!("{prefix}/messages/{enc_id}{suffix}")
}

async fn patch_messages(
    account: &GraphAccount,
    patches: Vec<MessagePatch>,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    let mut requests = Vec::new();
    let mut targets = Vec::new();
    for (index, patch) in patches.iter().enumerate() {
        let mut headers = HashMap::new();
        headers.insert("If-Match".to_string(), patch.etag.clone());
        requests.push(BatchRequestItem {
            id: index.to_string(),
            method: "PATCH".to_string(),
            url: message_batch_url(account, &patch.id, ""),
            body: Some(patch.body.clone()),
            headers: Some(headers),
        });
        targets.push(patch.id.clone());
    }
    submit_write_batch_with_targets(account, requests, &targets, false, operation).await
}

async fn move_messages(
    account: &GraphAccount,
    ids: &[ObjectId],
    destination: &str,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    let values = message_values_for_ids(account, ids, "id,changeKey").await?;
    // The destination may itself be foreign-encoded (a shared-mailbox
    // folder); the `move` body's `destinationId` must carry the native
    // folder id, never a `\u{1f}`-bearing one. Decode once up front.
    let dest = super::foreign::parse_folder(&bifrost_types::FolderId(destination.to_string()));
    let mut requests = Vec::new();
    let mut targets = Vec::new();
    // `ids[i]` is the caller-supplied (possibly foreign-encoded) routing
    // id; `values[i]` is its fetched value (etag). Route the request by
    // the routing id so a shared-mailbox message moves under
    // `/users/{owner}`, and guard that the destination folder belongs to
    // the same mailbox - a cross-mailbox move is not expressible against
    // one endpoint.
    for (index, (id, value)) in ids.iter().zip(values.iter()).enumerate() {
        let etag = graph_etag(value).ok_or_else(|| {
            pim_protocol_error(
                operation,
                Some(ErrorScope::Message { id: id.0.clone() }),
                format!("Graph message {} did not expose an etag", id.0),
            )
        })?;
        let source_owner = super::foreign::parse_message_id(id)
            .owner()
            .map(str::to_string);
        if dest.foreign().map(|f| f.mailbox.as_str()) != source_owner.as_deref() {
            return Err(pim_protocol_error(
                operation,
                Some(ErrorScope::Message { id: id.0.clone() }),
                format!(
                    "Graph move for {} targets a folder in a different mailbox than the message",
                    id.0
                ),
            ));
        }
        let mut headers = HashMap::new();
        headers.insert("If-Match".to_string(), etag);
        requests.push(BatchRequestItem {
            id: index.to_string(),
            method: "POST".to_string(),
            url: message_batch_url(account, id, "/move"),
            body: Some(json!({ "destinationId": dest.native_id() })),
            headers: Some(headers),
        });
        targets.push(id.clone());
    }
    submit_write_batch_with_targets(account, requests, &targets, false, operation).await
}

async fn destroy_messages(
    account: &GraphAccount,
    ids: &[ObjectId],
    operation: AccountOperation,
) -> Result<(), AccountError> {
    let values = message_values_for_ids(account, ids, "id,changeKey").await?;
    let mut requests = Vec::new();
    let mut targets = Vec::new();
    // Route each DELETE by the caller-supplied (possibly foreign-encoded)
    // routing id `ids[i]`, paired with its fetched etag `values[i]`.
    for (index, (id, value)) in ids.iter().zip(values.iter()).enumerate() {
        let mut headers = HashMap::new();
        if let Some(etag) = graph_etag(value) {
            headers.insert("If-Match".to_string(), etag);
        }
        requests.push(BatchRequestItem {
            id: index.to_string(),
            method: "DELETE".to_string(),
            url: message_batch_url(account, id, ""),
            body: None,
            headers: (!headers.is_empty()).then_some(headers),
        });
        targets.push(id.clone());
    }
    submit_write_batch_with_targets(account, requests, &targets, true, operation).await
}

async fn message_values_for_ids(
    account: &GraphAccount,
    ids: &[ObjectId],
    select: &str,
) -> Result<Vec<Value>, AccountError> {
    let mut values = Vec::new();
    for id in ids {
        values.push(fetch_message_value(account, id, select).await?);
    }
    Ok(values)
}

async fn submit_write_batch(
    account: &GraphAccount,
    requests: Vec<BatchRequestItem>,
    destroy: bool,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    submit_write_batch_with_targets(account, requests, &[], destroy, operation).await
}

/// Variant of `submit_write_batch` that threads per-request target
/// message ids so per-item failures carry `ErrorScope::Message { id }`
/// rather than the coarser `ErrorScope::Account`. `targets[i]` is the
/// `ObjectId` of the request whose `BatchRequestItem::id == i.to_string()`;
/// an empty `targets` slice falls back to `ErrorScope::Account` for
/// callers that have no per-request id.
async fn submit_write_batch_with_targets(
    account: &GraphAccount,
    requests: Vec<BatchRequestItem>,
    targets: &[ObjectId],
    destroy: bool,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if requests.is_empty() {
        return Ok(());
    }
    let ctx = GraphErrorContext::graph(operation);
    // Track every submitted request id (`BatchRequestItem::id`, assigned
    // `index.to_string()` 0..N-1 by the builders) so we can reconcile the
    // returned responses against them: Graph occasionally returns fewer
    // `$batch` responses than requests, and a missing id means that
    // message was never patched. Silently returning `Ok(())` would
    // violate the batch accounting contract (every id accounted for
    // exactly once), so an unaccounted id surfaces as an error.
    let expected_ids: HashSet<String> = requests.iter().map(|r| r.id.clone()).collect();
    let mut seen_ids: HashSet<String> = HashSet::new();
    let response: BatchResponse = account
        .client
        .post_batch(&BatchRequest { requests })
        .await
        .map_err(|e| into_account_error(e, ctx.clone()))?;
    for item in response.responses {
        seen_ids.insert(item.id.clone());
        // Reuse the per-item outcome projector so 4xx/5xx items
        // build structured `AccountError`s with `Protocol::Graph`,
        // `AttemptCause(Acknowledged)`, `WireCause::Graph(signal)`
        // (when an envelope is present), and `Retry-After` -> throttle
        // scope translation. Per-item 2xx and destroy-404 short-
        // circuit as `Succeeded`; everything else surfaces as
        // `Failed(_)` carrying the classified error which we then
        // unwrap into the function's single-error return contract.
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
        // Per-item failures carry `ErrorScope::Message { id }` when
        // the caller provided a parallel `targets` slice; otherwise
        // we fall back to `ErrorScope::Account` (graph-F3 done for
        // patch/move/destroy paths; remaining callers pass empty).
        let scope = item
            .id
            .parse::<usize>()
            .ok()
            .and_then(|idx| targets.get(idx))
            .map(|id| ErrorScope::Message { id: id.0.clone() })
            .unwrap_or(ErrorScope::Account);
        let outcome = mutation_item_outcome(
            item.status,
            headers,
            body,
            destroy,
            bifrost_types::BatchItemId(item.id.clone()),
            operation,
            scope,
        );
        match outcome {
            bifrost_types::ItemOutcome::Succeeded(_) => {}
            bifrost_types::ItemOutcome::Failed(failure) => return Err(failure.error),
            bifrost_types::ItemOutcome::Uncertain(uncertain) => return Err(uncertain.error),
        }
    }
    // Any submitted id with no corresponding response was never applied
    // server-side. The whole request was acknowledged (we got a 200 for
    // the `$batch` envelope) but this item's fate is unknown, so surface
    // it rather than reporting a clean `Ok(())`.
    if let Some(missing) = expected_ids.difference(&seen_ids).next() {
        let scope = missing
            .parse::<usize>()
            .ok()
            .and_then(|idx| targets.get(idx))
            .map(|id| ErrorScope::Message { id: id.0.clone() });
        return Err(protocol_violation(
            ProtocolErrorKind::ContractViolation,
            operation,
            scope,
            format!(
                "Graph $batch returned no response for request {missing} (item was not applied)"
            ),
        ));
    }
    Ok(())
}

/// Cache the message etag under the caller-supplied (possibly
/// foreign-encoded) id - the same key the mutation paths look up - so a
/// conditioned write on a shared-mailbox message finds its etag and
/// routes back to the owning mailbox. Keying by the native id from the
/// response body instead would lose the owner and orphan the cache entry.
async fn cache_etag_for(account: &GraphAccount, id: &ObjectId, value: &Value) {
    let Some(etag) = graph_etag(value) else {
        return;
    };
    account.etag_index.write().await.insert(id.0.clone(), etag);
}

fn object_id_from_value(
    value: &Value,
    operation: AccountOperation,
) -> Result<ObjectId, AccountError> {
    value
        .get("id")
        .and_then(Value::as_str)
        .map(|id| ObjectId(id.to_string()))
        .ok_or_else(|| pim_protocol_error(operation, None, "Graph message did not include an id"))
}

/// Build a `Protocol(ContractViolation)` `AccountError` for pim-layer
/// data-shape violations (missing id, missing etag, etc.). The
/// caller threads its `AccountOperation` and the scope of the
/// affected resource (typically `ErrorScope::Message { id }`); both
/// flow through into telemetry and support exports.
fn pim_protocol_error(
    operation: AccountOperation,
    scope: Option<ErrorScope>,
    msg: impl Into<String>,
) -> AccountError {
    protocol_violation(ProtocolErrorKind::ContractViolation, operation, scope, msg)
}

fn is_starred_category(category: &str) -> bool {
    category.eq_ignore_ascii_case(STARRED_CATEGORY)
        || category.eq_ignore_ascii_case("\\flagged")
        || category.eq_ignore_ascii_case("flagged")
        || category.eq_ignore_ascii_case("starred")
}

fn graph_extended_property_id(property_id: &str) -> String {
    if property_id.eq_ignore_ascii_case(PR_LAST_VERB_EXECUTED_ALIAS)
        || property_id.eq_ignore_ascii_case("PidTagLastVerbExecuted")
    {
        PR_LAST_VERB_EXECUTED_GRAPH_ID.to_string()
    } else {
        property_id.to_string()
    }
}

fn categories_from_value(value: &Value) -> Vec<String> {
    value
        .get("categories")
        .and_then(Value::as_array)
        .map(|categories| {
            categories
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn message_from_send_request(request: &bifrost_types::SendRequest) -> Result<Value, AccountError> {
    let mut patch = DraftPatch::default();
    patch.identity.clone_from(&request.identity);
    patch.from = request.from.clone().map(Some);
    patch.to = Some(request.to.clone());
    patch.cc = Some(request.cc.clone());
    patch.bcc = Some(request.bcc.clone());
    patch.reply_to = Some(request.reply_to.clone());
    patch.subject = Some(request.subject.clone());
    patch.body_text = Some(request.body_text.clone());
    patch.body_html = Some(request.body_html.clone());
    patch.attachments_inline = Some(request.attachments_inline.clone());
    patch.attachments_uploaded = Some(request.attachments_uploaded.clone());
    patch.in_reply_to = request.in_reply_to.clone().map(Some);
    patch.references = Some(request.references.clone());
    let mut message = message_from_draft_patch(&patch, true)?;
    // RFC 8098 read receipt: Graph models this as the message-level
    // `isReadReceiptRequested` bit rather than a MIME header. Set it only
    // when requested so existing sends are unaffected.
    if request.request_read_receipt
        && let Some(map) = message.as_object_mut()
    {
        map.insert("isReadReceiptRequested".to_string(), Value::Bool(true));
    }
    Ok(message)
}

fn message_from_draft_patch(
    patch: &DraftPatch,
    include_empty: bool,
) -> Result<Value, AccountError> {
    if patch
        .attachments_uploaded
        .as_ref()
        .is_some_and(|attachments| !attachments.is_empty())
    {
        return Err(unsupported_account_error(AccountOperation::DraftCreate));
    }
    let mut message = Map::new();
    if let Some(from) = &patch.from {
        insert_nullable_address(&mut message, "from", from);
    }
    insert_recipient_list(
        &mut message,
        "toRecipients",
        patch.to.as_ref(),
        include_empty,
    );
    insert_recipient_list(
        &mut message,
        "ccRecipients",
        patch.cc.as_ref(),
        include_empty,
    );
    insert_recipient_list(
        &mut message,
        "bccRecipients",
        patch.bcc.as_ref(),
        include_empty,
    );
    insert_recipient_list(
        &mut message,
        "replyTo",
        patch.reply_to.as_ref(),
        include_empty,
    );
    if let Some(subject) = &patch.subject {
        message.insert(
            "subject".to_string(),
            Value::String(subject.clone().unwrap_or_default()),
        );
    } else if include_empty {
        message.insert("subject".to_string(), Value::String(String::new()));
    }
    insert_body(&mut message, patch, include_empty);
    if let Some(attachments) = &patch.attachments_inline {
        message.insert(
            "attachments".to_string(),
            Value::Array(
                attachments
                    .iter()
                    .map(graph_attachment_from_inline)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        );
    }
    Ok(Value::Object(message))
}

fn insert_nullable_address(map: &mut Map<String, Value>, key: &str, address: &Option<Address>) {
    let value = address.as_ref().map(graph_recipient).unwrap_or(Value::Null);
    map.insert(key.to_string(), value);
}

fn insert_recipient_list(
    map: &mut Map<String, Value>,
    key: &str,
    addresses: Option<&Vec<Address>>,
    include_empty: bool,
) {
    if let Some(addresses) = addresses {
        map.insert(
            key.to_string(),
            Value::Array(addresses.iter().map(graph_recipient).collect()),
        );
    } else if include_empty {
        map.insert(key.to_string(), Value::Array(Vec::new()));
    }
}

fn insert_body(map: &mut Map<String, Value>, patch: &DraftPatch, include_empty: bool) {
    let body = match (&patch.body_html, &patch.body_text) {
        (Some(Some(html)), _) => Some(("HTML", html.clone())),
        (Some(None), _) => Some(("HTML", String::new())),
        (_, Some(Some(text))) => Some(("Text", text.clone())),
        (_, Some(None)) => Some(("Text", String::new())),
        _ if include_empty => Some(("Text", String::new())),
        _ => None,
    };
    if let Some((content_type, content)) = body {
        map.insert(
            "body".to_string(),
            json!({ "contentType": content_type, "content": content }),
        );
    }
}

fn graph_recipient(address: &Address) -> Value {
    let mut email = Map::new();
    email.insert(
        "address".to_string(),
        Value::String(address.address.clone()),
    );
    if let Some(name) = &address.name {
        email.insert("name".to_string(), Value::String(name.clone()));
    }
    json!({ "emailAddress": email })
}

fn graph_attachment_from_inline(attachment: &AttachmentInline) -> Result<Value, AccountError> {
    let content_bytes = base64::engine::general_purpose::STANDARD.encode(&attachment.data);
    let mut obj = json!({
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": attachment.filename.clone(),
        "contentType": attachment.mime.clone(),
        "isInline": attachment.inline,
        "contentBytes": content_bytes
    });
    // Stamp the contentId so a `cid:` reference in the HTML body resolves
    // to this fileAttachment (Graph's inline-image linkage). Bare value,
    // angle brackets stripped, matching the structured-send contract.
    let content_id = attachment
        .content_id
        .as_deref()
        .map(|cid| {
            cid.trim()
                .trim_matches(|c| c == '<' || c == '>')
                .to_string()
        })
        .filter(|cid| !cid.is_empty());
    if let Some(content_id) = content_id
        && let Some(map) = obj.as_object_mut()
    {
        map.insert("contentId".to_string(), Value::String(content_id));
    }
    Ok(obj)
}

async fn create_draft_message(
    client: &GraphClient,
    message: Value,
) -> Result<DraftHandle, AccountError> {
    let path = format!("{}/messages", client.api_path_prefix());
    let created: Value = client.post(&path, &message).await.map_err(|e| {
        into_account_error(e, GraphErrorContext::graph(AccountOperation::DraftCreate))
    })?;
    let id = created.get("id").and_then(Value::as_str).ok_or_else(|| {
        pim_protocol_error(
            AccountOperation::DraftCreate,
            None,
            "Graph draft create did not return an id",
        )
    })?;
    Ok(DraftHandle(id.to_string()))
}

async fn send_draft_message(client: &GraphClient, draft: &DraftHandle) -> Result<(), AccountError> {
    let path = format!(
        "{}/messages/{}/send",
        client.api_path_prefix(),
        bifrost_net::url::encode_component(&draft.0)
    );
    client
        .post_empty(&path)
        .await
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(AccountOperation::Send)))
}

#[derive(Debug)]
struct SearchRow {
    id: ObjectId,
    thread_id: Option<ThreadId>,
}

async fn search_message_rows(
    account: &GraphAccount,
    request: SearchRequest,
) -> Result<Page<SearchRow>, AccountError> {
    let url = search_url(&account.client.api_path_prefix(), &request)?;
    let ctx = GraphErrorContext::graph(AccountOperation::Search);
    let page: ODataCollection<Value> = if url.starts_with("http") {
        account.client.get_absolute(&url).await
    } else {
        account.client.get_json(&url).await
    }
    .map_err(|e| into_account_error(e, ctx))?;
    let mut rows = Vec::new();
    for value in page.value {
        let id = object_id_from_value(&value, AccountOperation::Search)?;
        let thread_id = value
            .get("conversationId")
            .and_then(Value::as_str)
            .map(|id| ThreadId(id.to_string()));
        rows.push(SearchRow { id, thread_id });
    }
    Ok(Page {
        items: rows,
        next_cursor: page.next_link.map(String::into_bytes),
        estimated_total: None,
        failed_ids: Vec::new(),
    })
}

fn search_url(prefix: &str, request: &SearchRequest) -> Result<String, AccountError> {
    if let Some(cursor) = &request.page_cursor {
        return String::from_utf8(cursor.clone()).map_err(|error| {
            pim_protocol_error(
                AccountOperation::Search,
                None,
                format!("Graph search cursor is not UTF-8: {error}"),
            )
        });
    }
    let mut params = vec![
        "$select=id,conversationId".to_string(),
        format!("$top={}", request.limit.unwrap_or(50).clamp(1, 250)),
    ];
    // Graph `/messages` forbids combining `$search` with `$filter` in one
    // request (it answers 400). A structured filter that needs `$search`
    // (any From/To substring leaf, since `$filter` `contains()` on the
    // sender/recipient navigation properties is rejected) therefore forces
    // the *whole* request onto `$search`/KQL: the structured filter is
    // expressed as KQL and AND-combined with any raw `provider_query`.
    // Otherwise the OData `$filter` path stays in force, and a bare
    // `provider_query` (no structured filter) still goes through `$search`.
    let needs_search = request.filter.as_ref().is_some_and(filter_requires_search);
    if needs_search {
        let mut kql_parts = Vec::new();
        if let Some(filter) = &request.filter {
            let kql = kql_filter(filter)?;
            if !kql.is_empty() {
                kql_parts.push(kql);
            }
        }
        if let Some(provider_query) = &request.provider_query {
            kql_parts.push(graph_search_escape(provider_query));
        }
        let search = if kql_parts.len() == 1 {
            kql_parts.remove(0)
        } else {
            kql_parts
                .into_iter()
                .map(|part| format!("({part})"))
                .collect::<Vec<_>>()
                .join(" AND ")
        };
        params.push(format!(
            "$search={}",
            bifrost_net::url::encode_component(&format!("\"{search}\""))
        ));
    } else {
        if let Some(filter) = &request.filter {
            let filter = odata_filter(filter)?;
            if !filter.is_empty() {
                params.push(format!(
                    "$filter={}",
                    bifrost_net::url::encode_component(&filter)
                ));
            }
        }
        if let Some(provider_query) = &request.provider_query {
            params.push(format!(
                "$search={}",
                bifrost_net::url::encode_component(&format!(
                    "\"{}\"",
                    graph_search_escape(provider_query)
                ))
            ));
        }
    }
    Ok(format!("{prefix}/messages?{}", params.join("&")))
}

/// True if any leaf of the filter tree is a `From`/`To` substring match.
/// Graph `$filter` rejects `contains()` on the sender/recipient navigation
/// properties, so the whole request must route through `$search`/KQL when
/// one of these is present anywhere in the boolean composition.
fn filter_requires_search(filter: &SearchFilter) -> bool {
    match filter {
        SearchFilter::From(_) | SearchFilter::To(_) => true,
        SearchFilter::And(filters) | SearchFilter::Or(filters) => {
            filters.iter().any(filter_requires_search)
        }
        SearchFilter::Not(inner) => filter_requires_search(inner),
        _ => false,
    }
}

/// Express the whole filter tree as a KQL `$search` string.
///
/// Used only when `filter_requires_search` holds, because Graph cannot mix
/// `$search` with `$filter`. KQL property restrictions cover sender,
/// recipient, subject, body, attachment presence, category, and a send-date
/// range; KQL terms are AND/OR/NOT-composed. The one structured leaf with no
/// KQL equivalent on `/messages?$search` is `In` (folder scoping), which has
/// no KQL property - rather than silently drop it (returning matches from
/// other folders) or emit an invalid mixed request, it is a clean
/// `Request(Malformed)` so the caller learns the combination is unsupported.
fn kql_filter(filter: &SearchFilter) -> Result<String, AccountError> {
    match filter {
        SearchFilter::From(value) => Ok(format!("from:{}", kql_quoted(value))),
        // KQL `to:` and `cc:` cover the recipient set; there is no KQL
        // bcc property, matching Graph's search surface.
        SearchFilter::To(value) => Ok(format!("(to:{0} OR cc:{0})", kql_quoted(value))),
        SearchFilter::Subject(value) => Ok(format!("subject:{}", kql_quoted(value))),
        SearchFilter::Body(value) => Ok(format!("body:{}", kql_quoted(value))),
        SearchFilter::Has(value) => {
            if value.is_empty() {
                Ok("hasattachment:true".to_string())
            } else {
                Err(unsupported_account_error(AccountOperation::Search))
            }
        }
        SearchFilter::Labeled(label) => {
            Ok(format!("category:{}", kql_quoted(&label_id_native(label))))
        }
        SearchFilter::DateRange { after, before } => {
            let mut parts = Vec::new();
            if let Some(after) = after {
                parts.push(format!("received>={}", system_time_date(*after)));
            }
            if let Some(before) = before {
                parts.push(format!("received<{}", system_time_date(*before)));
            }
            Ok(parts.join(" AND "))
        }
        SearchFilter::And(filters) => kql_join(filters, "AND"),
        SearchFilter::Or(filters) => kql_join(filters, "OR"),
        SearchFilter::Not(filter) => Ok(format!("NOT ({})", kql_filter(filter)?)),
        // `In` (folder scoping) has no KQL property; failing cleanly beats
        // shipping a request that would silently search every folder.
        SearchFilter::In(_) => Err(invalid_account_error(
            AccountOperation::Search,
            "Graph search cannot combine a folder restriction with a \
             sender/recipient substring match (no KQL folder property); \
             use a folder-scoped search or drop the From/To term",
        )),
        _ => Err(unsupported_account_error(AccountOperation::Search)),
    }
}

fn kql_join(filters: &[SearchFilter], op: &str) -> Result<String, AccountError> {
    let mut parts = Vec::new();
    for filter in filters {
        let part = kql_filter(filter)?;
        if !part.is_empty() {
            parts.push(format!("({part})"));
        }
    }
    Ok(parts.join(&format!(" {op} ")))
}

/// KQL-quoted string value: wrap in double quotes (so multi-word values are
/// one phrase, not OR-ed tokens) and escape embedded double quotes.
fn kql_quoted(value: &str) -> String {
    format!("\"{}\"", graph_search_escape(value))
}

/// KQL date literal (`YYYY-MM-DD`) for the `received` range predicates.
fn system_time_date(value: SystemTime) -> String {
    let dt: chrono::DateTime<chrono::Utc> = value.into();
    dt.format("%Y-%m-%d").to_string()
}

fn odata_filter(filter: &SearchFilter) -> Result<String, AccountError> {
    // `From`/`To` are never reached here: any filter tree containing a
    // sender/recipient substring leaf is detected by `filter_requires_search`
    // in `search_url` and routed onto `$search`/KQL instead (Graph `$filter`
    // rejects `contains()` on those navigation properties with a 400). The
    // arms below are kept as a defensive fallback for a direct `odata_filter`
    // call and use the rejected `contains()` shape, but the live `search_url`
    // path no longer emits them.
    match filter {
        SearchFilter::From(value) => Ok(format!(
            "(contains(from/emailAddress/address,{0}) or contains(from/emailAddress/name,{0}))",
            odata_quoted(value)
        )),
        SearchFilter::To(value) => Ok(format!(
            "(toRecipients/any(r:contains(r/emailAddress/address,{0}) or contains(r/emailAddress/name,{0})) or ccRecipients/any(r:contains(r/emailAddress/address,{0}) or contains(r/emailAddress/name,{0})) or bccRecipients/any(r:contains(r/emailAddress/address,{0}) or contains(r/emailAddress/name,{0})))",
            odata_quoted(value)
        )),
        SearchFilter::Subject(value) => Ok(format!("contains(subject,{})", odata_quoted(value))),
        SearchFilter::Body(value) => Ok(format!("contains(body/content,{})", odata_quoted(value))),
        SearchFilter::Has(value) => {
            if value.is_empty() {
                Ok("hasAttachments eq true".to_string())
            } else {
                Err(unsupported_account_error(AccountOperation::Search))
            }
        }
        SearchFilter::In(container) => {
            Ok(format!("parentFolderId eq {}", odata_quoted(&container.0)))
        }
        SearchFilter::Labeled(label) => Ok(format!(
            "categories/any(c:c eq {})",
            odata_quoted(&label_id_native(label))
        )),
        SearchFilter::DateRange { after, before } => {
            let mut parts = Vec::new();
            if let Some(after) = after {
                parts.push(format!("sentDateTime ge {}", system_time_rfc3339(*after)));
            }
            if let Some(before) = before {
                parts.push(format!("sentDateTime lt {}", system_time_rfc3339(*before)));
            }
            Ok(parts.join(" and "))
        }
        SearchFilter::And(filters) => join_filters(filters, "and"),
        SearchFilter::Or(filters) => join_filters(filters, "or"),
        SearchFilter::Not(filter) => Ok(format!("not ({})", odata_filter(filter)?)),
        _ => Err(unsupported_account_error(AccountOperation::Search)),
    }
}

fn join_filters(filters: &[SearchFilter], op: &str) -> Result<String, AccountError> {
    let mut parts = Vec::new();
    for filter in filters {
        let part = odata_filter(filter)?;
        if !part.is_empty() {
            parts.push(format!("({part})"));
        }
    }
    Ok(parts.join(&format!(" {op} ")))
}

fn label_id_native(label: &LabelId) -> String {
    label.0.clone()
}

fn odata_quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn graph_search_escape(value: &str) -> String {
    value.replace('"', "\\\"")
}

fn select_query(select: &str) -> String {
    if select.contains("$expand") {
        select.to_string()
    } else {
        format!("$select={select}")
    }
}

fn hydrate_select(expand_attachments: bool) -> &'static str {
    if expand_attachments {
        "$select=id,conversationId,subject,bodyPreview,body,uniqueBody,from,toRecipients,ccRecipients,bccRecipients,replyTo,receivedDateTime,sentDateTime,parentFolderId,isRead,importance,categories,flag,internetMessageHeaders,internetMessageId,hasAttachments,changeKey&$expand=attachments"
    } else {
        "id,conversationId,subject,bodyPreview,body,uniqueBody,from,toRecipients,ccRecipients,bccRecipients,replyTo,receivedDateTime,sentDateTime,parentFolderId,isRead,importance,categories,flag,internetMessageHeaders,internetMessageId,hasAttachments,changeKey"
    }
}

fn expand_blobs(projection: HydrationProjection) -> bool {
    matches!(projection, HydrationProjection::FullWithBlobs)
}

fn message_from_value(
    value: &Value,
    projection: HydrationProjection,
) -> Result<Message, AccountError> {
    let id = object_id_from_value(value, AccountOperation::HydrateMessage)?;
    let body = value.get("body");
    let (mut body_text, mut body_html) = match body {
        Some(body) => body_parts_from_graph_body(body),
        None => (None, None),
    };
    if matches!(projection, HydrationProjection::Headers) {
        body_text = None;
        body_html = None;
    }
    if let HydrationProjection::Preview(limit) = projection {
        body_text = value
            .get("bodyPreview")
            .and_then(Value::as_str)
            .map(|preview| preview.chars().take(limit).collect());
        body_html = None;
    }
    if matches!(
        projection,
        HydrationProjection::FullWithBlobs | HydrationProjection::Full
    ) {
        body_text = body_text.or_else(|| {
            value
                .get("bodyPreview")
                .and_then(Value::as_str)
                .map(str::to_string)
        });
    }
    let attachments = if expand_blobs(projection) {
        value
            .get("attachments")
            .and_then(Value::as_array)
            .map(|attachments| {
                attachments
                    .iter()
                    .filter_map(|attachment| blob_handle_from_graph_attachment(&id, attachment))
                    .collect()
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    Ok(Message {
        id,
        thread_id: value
            .get("conversationId")
            .and_then(Value::as_str)
            .map(|id| ThreadId(id.to_string())),
        from: value
            .get("from")
            .and_then(address_from_recipient)
            .into_iter()
            .collect(),
        to: addresses_from_array(value.get("toRecipients")),
        cc: addresses_from_array(value.get("ccRecipients")),
        bcc: addresses_from_array(value.get("bccRecipients")),
        reply_to: addresses_from_array(value.get("replyTo")),
        subject: value
            .get("subject")
            .and_then(Value::as_str)
            .map(str::to_string),
        date: graph_message_date(value),
        containers: value
            .get("parentFolderId")
            .and_then(Value::as_str)
            .map(|id| vec![ContainerId(id.to_string())])
            .unwrap_or_default(),
        flags: flags_from_message(value),
        importance: importance_from_graph(value),
        body_text,
        body_html,
        attachments,
        size_bytes: value.get("size").and_then(Value::as_u64),
        in_reply_to: internet_header(value, "In-Reply-To"),
        references: internet_header(value, "References")
            .map(|header| header.split_whitespace().map(str::to_string).collect())
            .unwrap_or_default(),
    })
}

/// Map Graph's single-valued `importance` wire field
/// (`low|normal|high`) onto the uniform `Importance` enum. Absent or
/// unrecognized -> `Normal`.
fn importance_from_graph(value: &Value) -> Importance {
    match value.get("importance").and_then(Value::as_str) {
        Some(level) if level.eq_ignore_ascii_case("low") => Importance::Low,
        Some(level) if level.eq_ignore_ascii_case("high") => Importance::High,
        _ => Importance::Normal,
    }
}

fn body_parts_from_graph_body(body: &Value) -> (Option<String>, Option<String>) {
    let content = body
        .get("content")
        .and_then(Value::as_str)
        .map(str::to_string);
    let content_type = body
        .get("contentType")
        .and_then(Value::as_str)
        .unwrap_or("text");
    if content_type.eq_ignore_ascii_case("html") {
        (None, content)
    } else {
        (content, None)
    }
}

fn address_from_recipient(value: &Value) -> Option<Address> {
    let email = value.get("emailAddress")?;
    let address = email.get("address")?.as_str()?.to_string();
    let name = email
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .map(str::to_string);
    Some(Address { name, address })
}

fn addresses_from_array(value: Option<&Value>) -> Vec<Address> {
    value
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(address_from_recipient).collect())
        .unwrap_or_default()
}

fn graph_message_date(value: &Value) -> Option<SystemTime> {
    value
        .get("receivedDateTime")
        .and_then(Value::as_str)
        .or_else(|| value.get("sentDateTime").and_then(Value::as_str))
        .and_then(parse_graph_datetime)
}

fn flags_from_message(value: &Value) -> HashSet<String> {
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
    for category in categories_from_value(value) {
        flags.insert(format!("category:{category}"));
    }
    flags
}

fn internet_header(value: &Value, name: &str) -> Option<String> {
    value
        .get("internetMessageHeaders")?
        .as_array()?
        .iter()
        .find_map(|header| {
            let header_name = header.get("name")?.as_str()?;
            header_name.eq_ignore_ascii_case(name).then(|| {
                header
                    .get("value")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string()
            })
        })
}

async fn well_known_folder_roles(account: &GraphAccount) -> HashMap<String, FolderRole> {
    let mut roles = HashMap::new();
    for (name, role) in WELL_KNOWN_FOLDERS {
        let path = format!(
            "{}/mailFolders/{}?$select=id",
            account.client.api_path_prefix(),
            bifrost_net::url::encode_component(name)
        );
        if let Ok(value) = account.client.get_json::<Value>(&path).await
            && let Some(id) = value.get("id").and_then(Value::as_str)
        {
            roles.insert(id.to_string(), *role);
        }
    }
    roles
}

const WELL_KNOWN_FOLDERS: &[(&str, FolderRole)] = &[
    ("inbox", FolderRole::Inbox),
    ("sentItems", FolderRole::Sent),
    ("drafts", FolderRole::Drafts),
    (DELETED_ITEMS, FolderRole::Trash),
    ("junkEmail", FolderRole::Spam),
    ("archive", FolderRole::Archive),
];

/// Project one Graph mail folder onto a `Container`.
///
/// `owner` is `Some(mailbox)` for a shared (delegate) mailbox's folder,
/// which namespaces the ids: `native_id` becomes the foreign-encoded form
/// (byte-identical to the `CursorScope::FolderType` string discovery emits
/// for the same folder) while `owner_local_id` keeps the bare Graph folder
/// id. The parent is encoded in the same namespace, so a shared child never
/// points at a same-id primary folder.
fn container_from_folder(
    folder: GraphMailFolder,
    roles: &HashMap<String, FolderRole>,
    owner: Option<&str>,
) -> Container {
    let role = roles
        .get(&folder.id)
        .copied()
        .or_else(|| role_from_well_known_name(&folder.id));
    let native = match owner {
        Some(mailbox) => super::foreign::encode_foreign(mailbox, &folder.id).0,
        None => folder.id.clone(),
    };
    let parent = folder.parent_folder_id.map(|parent| {
        ContainerId(match owner {
            Some(mailbox) => super::foreign::encode_foreign(mailbox, &parent).0,
            None => parent,
        })
    });
    Container::new(
        ContainerId(native.clone()),
        ContainerKind::Folder,
        role,
        Provenance {
            provider: ProtocolKind::Graph,
            kind: ContainerKind::Folder,
            native: native.clone(),
        },
        folder.display_name.unwrap_or_else(|| native.clone()),
        parent,
    )
    // Graph mail folders carry no container color (categories are message
    // flags, not containers), and Graph is folder-shaped (well-known folders
    // already map into `role`), so `style` and `system` keep their
    // `Container::new` defaults. Graph REST exposes no per-folder ACL or
    // subscription state on mail folders either; the EWS `EffectiveRights`
    // that DO exist are a public-folder-only surface.
    .with_namespace(match owner {
        Some(_) => ContainerNamespace::Shared,
        None => ContainerNamespace::Personal,
    })
    .with_owner(owner.map(|mailbox| MailboxId(mailbox.to_string())))
    // The `/users/{id}` routing key doubles as the owner email exactly when
    // it is addressable (a UPN/SMTP address the account already holds);
    // an object-id-shaped key carries no email and projects `None`.
    .with_owner_email(
        owner
            .filter(|mailbox| mailbox.contains('@'))
            .map(str::to_string),
    )
    .with_owner_local_id(owner.map(|_| folder.id))
}

fn role_from_well_known_name(name: &str) -> Option<FolderRole> {
    WELL_KNOWN_FOLDERS
        .iter()
        .find_map(|(known, role)| name.eq_ignore_ascii_case(known).then_some(*role))
}

async fn trash_container_id(account: &GraphAccount) -> ContainerId {
    let roles = well_known_folder_roles(account).await;
    roles
        .into_iter()
        .find_map(|(id, role)| (role == FolderRole::Trash).then_some(ContainerId(id)))
        .unwrap_or_else(|| ContainerId(DELETED_ITEMS.to_string()))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MailboxSettings {
    automatic_replies_setting: Option<AutomaticRepliesSetting>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AutomaticRepliesSetting {
    status: Option<String>,
    internal_reply_message: Option<String>,
    external_reply_message: Option<String>,
    scheduled_start_date_time: Option<DateTimeTimeZone>,
    scheduled_end_date_time: Option<DateTimeTimeZone>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DateTimeTimeZone {
    date_time: Option<String>,
    time_zone: Option<String>,
}

fn vacation_from_graph(setting: AutomaticRepliesSetting) -> VacationConfig {
    let status = setting.status.unwrap_or_else(|| "disabled".to_string());
    let body_html = setting
        .internal_reply_message
        .or(setting.external_reply_message)
        .filter(|body| !body.is_empty());
    VacationConfig {
        is_enabled: !status.eq_ignore_ascii_case("disabled"),
        subject: None,
        body_text: None,
        body_html,
        starts_at: setting
            .scheduled_start_date_time
            .and_then(|dt| graph_datetime_to_system_time(&dt)),
        ends_at: setting
            .scheduled_end_date_time
            .and_then(|dt| graph_datetime_to_system_time(&dt)),
    }
}

fn graph_datetime_or_default(time: Option<SystemTime>) -> Value {
    json!({
        "dateTime": time
            .map(system_time_naive_utc)
            .unwrap_or_else(|| "0001-01-01T00:00:00".to_string()),
        "timeZone": "UTC"
    })
}

fn graph_datetime_to_system_time(value: &DateTimeTimeZone) -> Option<SystemTime> {
    let date_time = value.date_time.as_deref()?;
    let _time_zone = value.time_zone.as_deref().unwrap_or("UTC");
    parse_graph_datetime(date_time)
}

fn parse_graph_datetime(value: &str) -> Option<SystemTime> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
        return Some(dt.with_timezone(&chrono::Utc).into());
    }
    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .map(|dt| chrono::Utc.from_utc_datetime(&dt).into())
}

fn system_time_rfc3339(value: SystemTime) -> String {
    let dt: chrono::DateTime<chrono::Utc> = value.into();
    dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn system_time_naive_utc(value: SystemTime) -> String {
    let dt: chrono::DateTime<chrono::Utc> = value.into();
    dt.format("%Y-%m-%dT%H:%M:%S").to_string()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use serde_json::json;

    use super::*;
    use crate::account::PushMode;
    use crate::account::foreign::{encode_foreign, encode_message_id, encode_public_item_id};
    use crate::client::GraphClient;
    use bifrost_types::{CursorScope, ObjectType};

    // The single-id hydration door must reach the EWS arm for a public
    // item. Before this, `message_hydrate` fell through to
    // `/me/messages/{native}` and the server answered
    // `ErrorItemNotFound` naming the BARE item id, even though the stored
    // id was folder-qualified all along.
    #[test]
    fn public_item_id_selects_the_ews_read_arm() {
        let folder = FolderId("AAMkPF=".to_string());
        let public = encode_public_item_id(&folder, "notice-1");
        assert_eq!(ews_read_folder(&public), Some(folder));
    }

    #[test]
    fn primary_and_foreign_ids_stay_on_the_rest_read_arm() {
        assert_eq!(ews_read_folder(&ObjectId("notice-1".to_string())), None);
        let foreign = encode_message_id(
            &CursorScope::FolderType {
                folder: encode_foreign("shared@contoso.com", "AAMkfolder"),
                ty: ObjectType::Email,
            },
            "AAMkmsg",
        );
        assert_eq!(ews_read_folder(&foreign), None);
    }

    fn ews_item() -> crate::ews::EwsItem {
        crate::ews::EwsItem {
            item_id: "notice-1".to_string(),
            change_key: Some("CK1".to_string()),
            subject: Some("Notice".to_string()),
            sender_email: Some("poster@contoso.com".to_string()),
            sender_name: Some("Poster".to_string()),
            received_at: Some("2026-01-02T03:04:05Z".to_string()),
            body_preview: Some("preview text".to_string()),
            body_html: Some("<p>preview text</p>".to_string()),
            is_read: true,
            item_class: "IPM.Note".to_string(),
            to_recipients: vec![crate::ews::EwsRecipient {
                email: "reader@contoso.com".to_string(),
                name: Some("Reader".to_string()),
            }],
            cc_recipients: Vec::new(),
            attachments: Vec::new(),
        }
    }

    // The EWS projection keeps the folder-qualified id the consumer
    // stored, so a hydrated public item still round-trips back through
    // the EWS arm on the next read.
    #[test]
    fn ews_item_projects_to_message_keyed_by_the_qualified_id() {
        let folder = FolderId("AAMkPF=".to_string());
        let id = encode_public_item_id(&folder, "notice-1");
        let message =
            message_from_ews_item(id.clone(), &ews_item(), &folder, HydrationProjection::Full);
        assert_eq!(message.id, id);
        assert_eq!(ews_read_folder(&message.id), Some(folder.clone()));
        assert_eq!(message.subject.as_deref(), Some("Notice"));
        assert_eq!(message.from.len(), 1);
        assert_eq!(message.from[0].address, "poster@contoso.com");
        assert_eq!(message.to[0].address, "reader@contoso.com");
        assert_eq!(message.body_html.as_deref(), Some("<p>preview text</p>"));
        assert_eq!(message.body_text.as_deref(), Some("preview text"));
        assert_eq!(message.containers, vec![ContainerId(folder.0)]);
        assert!(message.flags.contains("\\seen"));
        assert!(message.date.is_some());
    }

    #[test]
    fn ews_headers_projection_drops_the_body() {
        let folder = FolderId("AAMkPF=".to_string());
        let message = message_from_ews_item(
            encode_public_item_id(&folder, "notice-1"),
            &ews_item(),
            &folder,
            HydrationProjection::Headers,
        );
        assert!(message.body_text.is_none());
        assert!(message.body_html.is_none());
    }

    fn shared_account() -> GraphAccount {
        GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["shared@contoso.com".to_string()],
        )
    }

    fn mail_folder(id: &str, parent: Option<&str>) -> GraphMailFolder {
        serde_json::from_value(json!({
            "id": id,
            "displayName": "Reports",
            "parentFolderId": parent,
        }))
        .expect("mail folder deserializes")
    }

    #[test]
    fn shared_mailbox_folder_projects_as_shared_namespaced_container() {
        let container = container_from_folder(
            mail_folder("AAMkChild", Some("AAMkParent")),
            &HashMap::new(),
            Some("shared@contoso.com"),
        );
        assert_eq!(container.namespace, ContainerNamespace::Shared);
        assert_eq!(
            container.owner,
            Some(MailboxId("shared@contoso.com".to_string()))
        );
        // `native_id` is the foreign-encoded form; `owner_local_id` is the
        // bare Graph folder id, never the encoded one.
        assert_eq!(
            container.native_id,
            encode_foreign("shared@contoso.com", "AAMkChild").0
        );
        assert_eq!(container.owner_local_id.as_deref(), Some("AAMkChild"));
        assert_ne!(
            container.owner_local_id.as_deref(),
            Some(container.native_id.as_str())
        );
        // The parent is encoded in the same namespace.
        assert_eq!(
            container.parent,
            Some(ContainerId(
                encode_foreign("shared@contoso.com", "AAMkParent").0
            ))
        );
        // Graph REST mail folders expose no ACL; only public folders do.
        assert!(container.rights.is_none());
        assert!(container.content_class.is_none());
        // An addressable routing key IS the owner email.
        assert_eq!(container.owner_email.as_deref(), Some("shared@contoso.com"));

        // An object-id-shaped routing key carries no email.
        let opaque = container_from_folder(
            mail_folder("AAMkChild", Some("AAMkParent")),
            &HashMap::new(),
            Some("48d31887-5fad-4d73-a9f5-3c356e68a038"),
        );
        assert_eq!(opaque.namespace, ContainerNamespace::Shared);
        assert!(opaque.owner_email.is_none());

        // A primary folder stays personal and unqualified.
        let primary = container_from_folder(
            mail_folder("AAMkChild", Some("AAMkParent")),
            &HashMap::new(),
            None,
        );
        assert_eq!(primary.namespace, ContainerNamespace::Personal);
        assert!(primary.owner.is_none());
        assert!(primary.owner_local_id.is_none());
        assert!(primary.owner_email.is_none());
        assert_eq!(primary.native_id, "AAMkChild");
    }

    /// A shared container's `native_id` must be byte-identical to the
    /// `CursorScope::FolderType` string `discover_cursor_scopes` emits for
    /// the same folder - that identity is the join key between a container
    /// and its sync scope.
    #[test]
    fn shared_container_native_id_matches_discovered_cursor_scope() {
        let container = container_from_folder(
            mail_folder("AAMkChild", None),
            &HashMap::new(),
            Some("shared@contoso.com"),
        );
        // The exact expression `discover_cursor_scopes_inner` uses for a
        // shared mailbox's folder.
        let scope = CursorScope::FolderType {
            folder: encode_foreign("shared@contoso.com", "AAMkChild"),
            ty: ObjectType::Email,
        };
        assert_eq!(
            scope,
            CursorScope::FolderType {
                folder: bifrost_types::FolderId(container.native_id.clone()),
                ty: ObjectType::Email,
            },
        );
    }

    #[tokio::test]
    async fn public_folders_project_with_content_class_and_rights() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
        let routing = crate::account::cursor::PublicFolderRouting {
            anchor_mailbox: "content@contoso.com".to_string(),
            public_folder_mailbox: Some("pf@contoso.com".to_string()),
        };
        account
            .seed_public_folder_meta_for_tests(
                bifrost_types::FolderId("AAMkPF=".to_string()),
                routing.clone(),
                crate::account::public_folder::PublicFolderMeta {
                    display_name: "Company Announcements".to_string(),
                    folder_class: Some("IPF.Note".to_string()),
                    parent: None,
                    effective_rights: crate::ews::EwsEffectiveRights {
                        create_associated: false,
                        create_contents: false,
                        create_hierarchy: false,
                        delete: false,
                        modify: false,
                        read: true,
                    },
                },
            )
            .await;
        account
            .seed_public_folder_meta_for_tests(
                bifrost_types::FolderId("AAMkCal=".to_string()),
                routing,
                crate::account::public_folder::PublicFolderMeta {
                    display_name: "Team Calendar".to_string(),
                    folder_class: Some("IPF.Appointment".to_string()),
                    parent: Some(bifrost_types::FolderId("AAMkPF=".to_string())),
                    effective_rights: crate::ews::EwsEffectiveRights {
                        create_associated: true,
                        create_contents: true,
                        create_hierarchy: true,
                        delete: true,
                        modify: true,
                        read: true,
                    },
                },
            )
            .await;

        let containers = public_folder_containers(&account).await;
        assert_eq!(containers.len(), 2);
        let calendar = containers
            .iter()
            .find(|c| c.native_id == "AAMkCal=")
            .expect("calendar public folder");
        assert_eq!(calendar.namespace, ContainerNamespace::Public);
        // A public folder has no owning principal.
        assert!(calendar.owner.is_none());
        assert!(calendar.owner_local_id.is_none());
        assert_eq!(calendar.name, "Team Calendar");
        assert_eq!(calendar.parent, Some(ContainerId("AAMkPF=".to_string())));
        assert_eq!(
            calendar.content_class,
            Some(ContainerContentClass::Calendar)
        );

        // A read-only public folder is distinguishable from a writable one.
        let mail = containers
            .iter()
            .find(|c| c.native_id == "AAMkPF=")
            .expect("mail public folder");
        assert_eq!(mail.content_class, Some(ContainerContentClass::Mail));
        let read_only = mail.rights.as_ref().expect("rights projected");
        assert_eq!(read_only.may_read_items, Some(true));
        assert_eq!(read_only.may_add_items, Some(false));
        assert_eq!(read_only.may_remove_items, Some(false));
        assert_eq!(read_only.may_set_keywords, Some(false));
        // EWS folder rights say nothing about submission.
        assert_eq!(read_only.may_submit, None);
        let writable = calendar.rights.as_ref().expect("rights projected");
        assert_eq!(writable.may_add_items, Some(true));
        assert_eq!(writable.may_create_child, Some(true));
        assert_eq!(writable.may_delete, Some(true));
    }

    #[test]
    fn folder_class_maps_onto_content_class() {
        assert_eq!(
            content_class_from_folder_class(Some("IPF.Note")),
            Some(ContainerContentClass::Mail)
        );
        // A subtype must not fall through to Other.
        assert_eq!(
            content_class_from_folder_class(Some("IPF.Note.Microsoft.Oof.Log")),
            Some(ContainerContentClass::Mail)
        );
        assert_eq!(
            content_class_from_folder_class(Some("IPF.Appointment")),
            Some(ContainerContentClass::Calendar)
        );
        assert_eq!(
            content_class_from_folder_class(Some("IPF.Contact")),
            Some(ContainerContentClass::Contacts)
        );
        assert_eq!(
            content_class_from_folder_class(Some("IPF.Task")),
            Some(ContainerContentClass::Other)
        );
        // Unreported is distinct from "typed as something we don't model".
        assert_eq!(content_class_from_folder_class(None), None);
    }

    fn foreign_message_id(mailbox: &str, folder: &str, native: &str) -> ObjectId {
        let scope = CursorScope::FolderType {
            folder: encode_foreign(mailbox, folder),
            ty: ObjectType::Email,
        };
        encode_message_id(&scope, native)
    }

    #[test]
    fn message_batch_url_routes_foreign_id_to_owner_with_native_id() {
        let account = shared_account();
        let id = foreign_message_id("shared@contoso.com", "AAMkfolder", "AAMkmsg");
        // Per-message PATCH / DELETE (suffix "") and the move suffix both
        // route to `/users/{owner}/messages/{native}` with no `\u{1f}`.
        assert_eq!(
            message_batch_url(&account, &id, ""),
            "/users/shared%40contoso.com/messages/AAMkmsg"
        );
        assert_eq!(
            message_batch_url(&account, &id, "/move"),
            "/users/shared%40contoso.com/messages/AAMkmsg/move"
        );
        assert!(!message_batch_url(&account, &id, "").contains('\u{1f}'));
    }

    #[test]
    fn message_batch_url_keeps_primary_id_on_me() {
        let account = shared_account();
        let id = ObjectId("AAMkmsg".to_string());
        assert_eq!(message_batch_url(&account, &id, ""), "/me/messages/AAMkmsg");
    }

    #[test]
    fn maps_well_known_folder_roles() {
        assert_eq!(role_from_well_known_name("inbox"), Some(FolderRole::Inbox));
        assert_eq!(
            role_from_well_known_name("sentItems"),
            Some(FolderRole::Sent)
        );
        assert_eq!(
            role_from_well_known_name("deletedItems"),
            Some(FolderRole::Trash)
        );
    }

    #[test]
    fn scheduled_send_deferred_body_shape() {
        // 1970-01-01T00:00:00Z plus one day -> a stable ISO-8601 UTC value.
        let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(86_400);
        let body = deferred_send_time_body(at);
        let props = body
            .get("singleValueExtendedProperties")
            .and_then(serde_json::Value::as_array)
            .expect("singleValueExtendedProperties array");
        let prop = &props[0];
        assert_eq!(
            prop.get("id").and_then(serde_json::Value::as_str),
            Some("SystemTime 0x3FEF")
        );
        assert_eq!(
            prop.get("value").and_then(serde_json::Value::as_str),
            Some("1970-01-02T00:00:00Z")
        );
    }

    #[test]
    fn scheduled_send_capability_is_true() {
        let caps = crate::account::capabilities::build_capabilities(
            crate::account::PushMode::GraphSubscriptions,
        );
        assert!(caps.pim_methods.scheduled_send);
    }

    #[test]
    fn send_as_capability_is_true() {
        let caps = crate::account::capabilities::build_capabilities(
            crate::account::PushMode::GraphSubscriptions,
        );
        assert!(caps.pim_methods.send_as);
    }

    fn mailbox_address(value: &Value) -> Option<&str> {
        value
            .get("emailAddress")
            .and_then(|e| e.get("address"))
            .and_then(Value::as_str)
    }

    #[test]
    fn apply_send_as_as_sets_from_and_sender() {
        let mut message = json!({});
        let send_as = SendAs::As(MailboxId("shared@contoso.com".to_string()));
        apply_send_as(&mut message, &send_as, Some("user@contoso.com"));
        assert_eq!(
            mailbox_address(message.get("from").expect("from")),
            Some("shared@contoso.com")
        );
        assert_eq!(
            mailbox_address(message.get("sender").expect("sender")),
            Some("shared@contoso.com")
        );
    }

    #[test]
    fn apply_send_as_on_behalf_sets_sender_to_user() {
        let mut message = json!({});
        let send_as = SendAs::OnBehalfOf(MailboxId("shared@contoso.com".to_string()));
        apply_send_as(&mut message, &send_as, Some("user@contoso.com"));
        assert_eq!(
            mailbox_address(message.get("from").expect("from")),
            Some("shared@contoso.com")
        );
        assert_eq!(
            mailbox_address(message.get("sender").expect("sender")),
            Some("user@contoso.com")
        );
    }

    #[test]
    fn apply_send_as_on_behalf_omits_sender_without_user_email() {
        let mut message = json!({});
        let send_as = SendAs::OnBehalfOf(MailboxId("shared@contoso.com".to_string()));
        apply_send_as(&mut message, &send_as, None);
        assert_eq!(
            mailbox_address(message.get("from").expect("from")),
            Some("shared@contoso.com")
        );
        assert!(message.get("sender").is_none());
    }

    #[test]
    fn apply_send_as_on_behalf_honors_explicit_from() {
        let mut message = json!({
            "from": { "emailAddress": { "address": "author@contoso.com" } }
        });
        let send_as = SendAs::OnBehalfOf(MailboxId("shared@contoso.com".to_string()));
        apply_send_as(&mut message, &send_as, Some("user@contoso.com"));
        // Explicit author survives; only sender is stamped.
        assert_eq!(
            mailbox_address(message.get("from").expect("from")),
            Some("author@contoso.com")
        );
        assert_eq!(
            mailbox_address(message.get("sender").expect("sender")),
            Some("user@contoso.com")
        );
    }

    #[test]
    fn apply_send_as_as_overrides_explicit_from() {
        let mut message = json!({
            "from": { "emailAddress": { "address": "author@contoso.com" } }
        });
        let send_as = SendAs::As(MailboxId("shared@contoso.com".to_string()));
        apply_send_as(&mut message, &send_as, Some("user@contoso.com"));
        // `As` forces author == sender == mailbox.
        assert_eq!(
            mailbox_address(message.get("from").expect("from")),
            Some("shared@contoso.com")
        );
        assert_eq!(
            mailbox_address(message.get("sender").expect("sender")),
            Some("shared@contoso.com")
        );
    }

    #[test]
    fn send_as_unknown_mailbox_is_malformed() {
        let err = send_as_unknown_mailbox(&MailboxId("nobody@contoso.com".to_string()));
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert_eq!(err.operation(), Some(AccountOperation::Send));
    }

    #[test]
    fn scheduled_send_handle_round_trips_owning_mailbox() {
        let handle = encode_scheduled_send_handle(Some("shared@contoso.com"), "AAMkAGI2");
        let (mailbox, draft_id) = decode_scheduled_send_handle(&handle);
        assert_eq!(mailbox, Some("shared@contoso.com"));
        assert_eq!(draft_id, "AAMkAGI2");
    }

    #[test]
    fn scheduled_send_handle_without_mailbox_is_bare_draft_id() {
        let handle = encode_scheduled_send_handle(None, "AAMkAGI2");
        assert_eq!(handle, "AAMkAGI2");
        let (mailbox, draft_id) = decode_scheduled_send_handle(&handle);
        assert_eq!(mailbox, None);
        assert_eq!(draft_id, "AAMkAGI2");
    }

    #[test]
    fn maps_last_verb_property_alias() {
        assert_eq!(
            graph_extended_property_id("PR_LAST_VERB_EXECUTED"),
            "Integer 0x1081"
        );
        assert_eq!(graph_extended_property_id("String 0x4001"), "String 0x4001");
    }

    #[test]
    fn search_filter_escapes_odata_strings() {
        let filter =
            odata_filter(&SearchFilter::Subject("Bob's plan".to_string())).expect("filter builds");
        assert_eq!(filter, "contains(subject,'Bob''s plan')");
    }

    // The query string carries the KQL phrase percent-encoded; compare
    // against the same encoder the production path uses rather than a
    // hand-maintained decode table.
    fn search_param(url: &str) -> String {
        url.split("$search=")
            .nth(1)
            .expect("url has a $search param")
            .to_string()
    }

    #[test]
    fn from_to_filters_route_through_search_kql_not_filter() {
        // A From substring must hit `$search` (KQL), never the `$filter`
        // `contains()` Graph rejects.
        let req = SearchRequest::filter(SearchFilter::From("alice".to_string()));
        let url = search_url("/me", &req).expect("url builds");
        assert!(url.contains("$search="), "{url}");
        assert!(!url.contains("$filter="), "{url}");
        assert_eq!(
            search_param(&url),
            bifrost_net::url::encode_component("\"from:\"alice\"\"")
        );

        let to = SearchRequest::filter(SearchFilter::To("bob@x".to_string()));
        let to_url = search_url("/me", &to).expect("url builds");
        assert_eq!(
            search_param(&to_url),
            bifrost_net::url::encode_component("\"(to:\"bob@x\" OR cc:\"bob@x\")\"")
        );
    }

    #[test]
    fn non_sender_filter_still_uses_odata_filter() {
        // Subject-only search has a clean `$filter` shape; it must not be
        // forced onto `$search`.
        let req = SearchRequest::filter(SearchFilter::Subject("invoice".to_string()));
        let url = search_url("/me", &req).expect("url builds");
        assert!(url.contains("$filter="), "{url}");
        assert!(!url.contains("$search="), "{url}");
    }

    #[test]
    fn mixed_from_and_date_range_collapses_to_one_kql_search() {
        // From substring AND a date range: the whole thing goes to KQL,
        // never a mixed `$search`+`$filter` request.
        let req = SearchRequest::filter(SearchFilter::And(vec![
            SearchFilter::From("alice".to_string()),
            SearchFilter::DateRange {
                after: Some(SystemTime::UNIX_EPOCH),
                before: None,
            },
        ]));
        let url = search_url("/me", &req).expect("url builds");
        assert!(url.contains("$search="), "{url}");
        assert!(!url.contains("$filter="), "{url}");
        assert_eq!(
            search_param(&url),
            bifrost_net::url::encode_component("\"(from:\"alice\") AND (received>=1970-01-01)\"")
        );
    }

    #[test]
    fn from_filter_and_provider_query_combine_in_kql() {
        // A structured From plus a raw provider query AND together into one
        // `$search`; the request must not emit both `$filter` and `$search`.
        let mut req = SearchRequest::filter(SearchFilter::From("alice".to_string()));
        req.provider_query = Some("importance:high".to_string());
        let url = search_url("/me", &req).expect("url builds");
        assert!(url.contains("$search="), "{url}");
        assert!(!url.contains("$filter="), "{url}");
        assert_eq!(
            search_param(&url),
            bifrost_net::url::encode_component("\"(from:\"alice\") AND (importance:high)\"")
        );
    }

    #[test]
    fn from_combined_with_folder_restriction_is_malformed() {
        // `In` has no KQL property; combining it with a From substring is an
        // inexpressible query and must fail cleanly rather than ship an
        // invalid request or silently search every folder.
        let req = SearchRequest::filter(SearchFilter::And(vec![
            SearchFilter::From("alice".to_string()),
            SearchFilter::In(ContainerId("inbox".to_string())),
        ]));
        let err = search_url("/me", &req).expect_err("inexpressible combination");
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert_eq!(err.operation(), Some(AccountOperation::Search));
    }

    #[test]
    fn kql_quoting_escapes_embedded_double_quotes() {
        assert_eq!(kql_quoted(r#"a"b"#), r#""a\"b""#);
    }

    #[test]
    fn inline_attachment_is_base64_encoded() {
        let attachment = AttachmentInline {
            filename: "note.txt".to_string(),
            mime: "text/plain".to_string(),
            data: Bytes::from_static(b"hello"),
            inline: false,
            content_id: None,
        };
        let value = graph_attachment_from_inline(&attachment).expect("attachment builds");
        assert_eq!(value["contentBytes"], json!("aGVsbG8="));
        // No content_id -> no contentId key emitted.
        assert!(value.get("contentId").is_none());
    }

    #[test]
    fn inline_attachment_emits_content_id_when_set() {
        let attachment = AttachmentInline {
            filename: "logo.png".to_string(),
            mime: "image/png".to_string(),
            data: Bytes::from_static(b"img"),
            inline: true,
            content_id: Some("<logo@x>".to_string()),
        };
        let value = graph_attachment_from_inline(&attachment).expect("attachment builds");
        // Angle brackets are stripped to the bare cid token.
        assert_eq!(value["contentId"], json!("logo@x"));
    }

    #[test]
    fn importance_read_maps_graph_wire_field() {
        assert_eq!(
            importance_from_graph(&json!({ "importance": "high" })),
            Importance::High
        );
        assert_eq!(
            importance_from_graph(&json!({ "importance": "low" })),
            Importance::Low
        );
        assert_eq!(
            importance_from_graph(&json!({ "importance": "normal" })),
            Importance::Normal
        );
        // Absent or unrecognized -> Normal.
        assert_eq!(importance_from_graph(&json!({})), Importance::Normal);
        assert_eq!(
            importance_from_graph(&json!({ "importance": "URGENT" })),
            Importance::Normal
        );
    }

    #[test]
    fn set_importance_produces_one_exclusive_patch_body() {
        // Exactly one wire op: a single `{ "importance": "high" }` body,
        // no clear-then-set pair. The `If-Match` etag is attached by
        // `patch_messages` from `MessagePatch::etag`, not the body.
        let body = graph_importance_body(Importance::High);
        assert_eq!(body, json!({ "importance": "high" }));
        assert_eq!(body.as_object().expect("object").len(), 1);
        assert_eq!(
            graph_importance_body(Importance::Low),
            json!({ "importance": "low" })
        );
        assert_eq!(
            graph_importance_body(Importance::Normal),
            json!({ "importance": "normal" })
        );
    }

    /// The `bifrost-types` convenience layer routes `set_starred` through
    /// `set_category` with the reserved `"$flagged"` sentinel (Graph
    /// advertises `StarredFlagShape::Category`). Pin the sentinel Graph
    /// honors, plus the historical aliases, so a rename on either side is a
    /// test failure rather than a silently-created literal category named
    /// `$flagged` on every starred message.
    #[test]
    fn the_reserved_starred_sentinel_and_its_aliases_are_recognized() {
        assert_eq!(STARRED_CATEGORY, "$flagged");
        for token in [
            "$flagged",
            "$FLAGGED",
            "\\flagged",
            "\\Flagged",
            "flagged",
            "Flagged",
            "starred",
            "STARRED",
        ] {
            assert!(is_starred_category(token), "{token} must map to the flag");
        }
    }

    #[test]
    fn ordinary_category_names_are_not_mistaken_for_the_starred_sentinel() {
        for token in ["Work", "flag", "$flagged-ish", "un-flagged", "$starred", ""] {
            assert!(
                !is_starred_category(token),
                "{token} must stay an ordinary category"
            );
        }
    }

    #[test]
    fn setting_an_ordinary_category_is_a_read_modify_write_over_the_existing_array() {
        // `set_category` reads `categories`, edits, sorts, and writes the
        // whole array back; a naive `["new"]` write would drop the others.
        let existing = json!({ "categories": ["Zeta", "Alpha"] });
        let mut categories = categories_from_value(&existing);
        assert_eq!(categories, vec!["Zeta".to_string(), "Alpha".to_string()]);
        categories.push("Mid".to_string());
        categories.sort();
        assert_eq!(categories, vec!["Alpha", "Mid", "Zeta"]);

        // Absent / non-array `categories` reads as empty rather than
        // panicking, so a sparse `$select` cannot poison the write.
        assert!(categories_from_value(&json!({})).is_empty());
        assert!(categories_from_value(&json!({ "categories": "Work" })).is_empty());
        // Non-string members are skipped.
        assert_eq!(
            categories_from_value(&json!({ "categories": ["Work", 7, null] })),
            vec!["Work".to_string()]
        );
    }

    #[test]
    fn last_verb_executed_aliases_map_onto_the_graph_proptag() {
        // `replied_via_extended_property` / `forwarded_via_extended_property`
        // are advertised true, and the convenience layer passes the MAPI
        // alias; Graph only accepts the proptag form.
        for alias in [
            "PR_LAST_VERB_EXECUTED",
            "pr_last_verb_executed",
            "PidTagLastVerbExecuted",
            "pidtaglastverbexecuted",
        ] {
            assert_eq!(
                graph_extended_property_id(alias),
                PR_LAST_VERB_EXECUTED_GRAPH_ID
            );
        }
        // Anything else passes through verbatim - the caller owns the id.
        assert_eq!(
            graph_extended_property_id("SystemTime 0x3FEF"),
            "SystemTime 0x3FEF"
        );
    }

    #[test]
    fn the_deferred_send_property_is_the_pidtag_deferred_send_time_proptag() {
        let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_800_000_000);
        let body = deferred_send_time_body(at);
        let props = body
            .get("singleValueExtendedProperties")
            .and_then(Value::as_array)
            .expect("array");
        assert_eq!(props.len(), 1);
        assert_eq!(props[0]["id"], json!(DEFERRED_SEND_TIME_PROPERTY_ID));
        // ISO-8601 UTC with a `Z`, second precision, no fraction.
        let value = props[0]["value"].as_str().expect("string value");
        assert!(value.ends_with('Z'), "{value}");
        assert!(!value.contains('.'), "{value}");
        assert_eq!(value, graph_iso8601_utc(at));
    }

    #[test]
    fn send_as_on_behalf_of_keeps_an_explicit_author_but_stamps_the_sender() {
        let mut message =
            json!({ "from": { "emailAddress": { "address": "author@contoso.com" } } });
        apply_send_as(
            &mut message,
            &SendAs::OnBehalfOf(MailboxId("shared@contoso.com".to_string())),
            Some("me@contoso.com"),
        );
        assert_eq!(
            message["from"]["emailAddress"]["address"],
            json!("author@contoso.com")
        );
        assert_eq!(
            message["sender"]["emailAddress"]["address"],
            json!("me@contoso.com")
        );
    }

    #[test]
    fn send_as_on_behalf_of_fills_a_missing_author_with_the_mailbox() {
        let mut message = json!({});
        apply_send_as(
            &mut message,
            &SendAs::OnBehalfOf(MailboxId("shared@contoso.com".to_string())),
            None,
        );
        assert_eq!(
            message["from"]["emailAddress"]["address"],
            json!("shared@contoso.com")
        );
        // Without a known user email `sender` is omitted so Graph fills it
        // from the authenticated context rather than being told wrongly.
        assert!(message.get("sender").is_none());
    }

    #[test]
    fn send_as_overrides_any_consumer_supplied_author() {
        // `As` means author == sender == mailbox; a consumer-set `from`
        // must not survive, or the message would claim an identity the
        // send-as grant does not cover.
        let mut message =
            json!({ "from": { "emailAddress": { "address": "author@contoso.com" } } });
        apply_send_as(
            &mut message,
            &SendAs::As(MailboxId("shared@contoso.com".to_string())),
            Some("me@contoso.com"),
        );
        assert_eq!(
            message["from"]["emailAddress"]["address"],
            json!("shared@contoso.com")
        );
        assert_eq!(
            message["sender"]["emailAddress"]["address"],
            json!("shared@contoso.com")
        );
    }

    /// `DraftPatch` is `#[non_exhaustive]`, so functional-update syntax is
    /// unavailable outside `bifrost-types`; build through a closure so the
    /// tests below read as one expression each.
    fn draft_patch(fill: impl FnOnce(&mut DraftPatch)) -> DraftPatch {
        let mut patch = DraftPatch::default();
        fill(&mut patch);
        patch
    }

    #[test]
    fn a_draft_patch_of_one_field_touches_only_that_field() {
        // The partial-update seam: `draft_update` passes
        // `include_empty = false`, so a subject-only rename must not emit
        // `null` / `[]` for the untouched recipient and body buckets (Graph
        // reads those as clears).
        let patch = draft_patch(|patch| {
            patch.subject = Some(Some("Renamed".to_string()));
        });
        let message = message_from_draft_patch(&patch, false).expect("patch builds");
        let object = message.as_object().expect("object");
        assert_eq!(object.get("subject"), Some(&json!("Renamed")));
        for untouched in [
            "toRecipients",
            "ccRecipients",
            "bccRecipients",
            "replyTo",
            "from",
            "body",
            "attachments",
        ] {
            assert!(
                !object.contains_key(untouched),
                "{untouched} must not appear in a sparse draft patch: {message}"
            );
        }
        assert_eq!(object.len(), 1);
    }

    #[test]
    fn an_explicitly_cleared_draft_field_still_emits_its_clear() {
        // The flip side: `Some(None)` is a deliberate clear and MUST reach
        // the wire, so the sparse rule cannot just drop every empty value.
        let patch = draft_patch(|patch| {
            patch.subject = Some(None);
            patch.from = Some(None);
            patch.cc = Some(Vec::new());
        });
        let message = message_from_draft_patch(&patch, false).expect("patch builds");
        assert_eq!(message.get("subject"), Some(&json!("")));
        assert_eq!(message.get("from"), Some(&Value::Null));
        assert_eq!(message.get("cc"), None);
        assert_eq!(message.get("ccRecipients"), Some(&json!([])));
    }
}
