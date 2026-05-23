use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

use base64::Engine;
use bifrost_types::{
    Address, AttachmentInline, Container, ContainerId, ContainerKind, DraftHandle, DraftPatch,
    Error, FolderRole, HydrationProjection, Identity, IdentityId, LabelId, Message, MutationTarget,
    ObjectId, Page, ProtocolKind, Provenance, SearchFilter, SearchRequest, ThreadHydration,
    ThreadId, VacationConfig,
};
use chrono::TimeZone;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::types::{
    BatchRequest, BatchRequestItem, BatchResponse, GraphMailFolder, ODataCollection,
};

use super::GraphAccount;
use super::blob::blob_handle_from_graph_attachment;
use super::inventory::graph_etag;

const STARRED_CATEGORY: &str = "$flagged";
const PR_LAST_VERB_EXECUTED_ALIAS: &str = "PR_LAST_VERB_EXECUTED";
const PR_LAST_VERB_EXECUTED_GRAPH_ID: &str = "Integer 0x1081";
const DELETED_ITEMS: &str = "deletedItems";
const MSG_FOLDER_ROOT: &str = "msgfolderroot";

// Returned by send_message / draft_create when the request carries an
// AttachmentHandle minted by attachment_upload. Graph does not
// implement upload-session attachment_upload (capability flag false),
// so any handle here was minted on another account; embed the bytes
// inline via SendRequest.attachments_inline / DraftPatch.attachments_inline
// instead.
const GRAPH_PRE_UPLOADED_ATTACHMENTS_MSG: &str =
    "graph: pre-uploaded attachment handles are not supported; embed inline via attachments_inline";

// Returned by draft_update when the patch tries to add or replace
// attachments. Graph attachment management on existing drafts needs
// an upload-session primitive that Stage 1 does not expose.
const GRAPH_DRAFT_UPDATE_ATTACHMENTS_MSG: &str =
    "graph: draft_update cannot add or replace attachments; recreate the draft instead";

pub(crate) async fn add_to_container(
    account: GraphAccount,
    target: MutationTarget,
    container: ContainerId,
) -> Result<(), Error> {
    let ids = resolve_target_ids(&account, target).await?;
    move_messages(&account, &ids, &container.0).await
}

pub(crate) async fn set_category(
    account: GraphAccount,
    target: MutationTarget,
    category: String,
    value: bool,
) -> Result<(), Error> {
    let values = resolve_target_values(&account, target, "id,categories,flag,changeKey").await?;
    let mut patches = Vec::new();
    for message in values {
        let id = object_id_from_value(&message)?;
        let etag = graph_etag(&message).ok_or_else(|| {
            Error::Other(format!("Graph message {} did not expose an etag", id.0))
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
    patch_messages(&account, patches).await
}

pub(crate) async fn set_extended_property(
    account: GraphAccount,
    target: MutationTarget,
    property_id: String,
    value: Option<String>,
) -> Result<(), Error> {
    let property_id = graph_extended_property_id(&property_id);
    match value {
        Some(value) => {
            let values = resolve_target_values(&account, target, "id,changeKey").await?;
            let mut patches = Vec::new();
            for message in values {
                let id = object_id_from_value(&message)?;
                let etag = graph_etag(&message).ok_or_else(|| {
                    Error::Other(format!("Graph message {} did not expose an etag", id.0))
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
            patch_messages(&account, patches).await
        }
        None => {
            // Clear: DELETE the property's navigation entry on each
            // resolved message. Graph answers 204 No Content on
            // success and 404 if the property never existed; the
            // batch helper tolerates 404 for the clear path so a
            // partial / never-set state still resolves to Ok.
            let ids = resolve_target_ids(&account, target).await?;
            delete_extended_property(&account, &ids, &property_id).await
        }
    }
}

async fn delete_extended_property(
    account: &GraphAccount,
    ids: &[ObjectId],
    property_id: &str,
) -> Result<(), Error> {
    let prefix = account.client.api_path_prefix();
    let mut requests = Vec::new();
    for (index, id) in ids.iter().enumerate() {
        requests.push(BatchRequestItem {
            id: index.to_string(),
            method: "DELETE".to_string(),
            url: format!(
                "{prefix}/messages/{}/singleValueExtendedProperties/{}",
                bifrost_net::url::encode_component(&id.0),
                bifrost_net::url::encode_component(property_id),
            ),
            body: None,
            headers: None,
        });
    }
    submit_write_batch(account, requests, true).await
}

pub(crate) async fn set_is_read(
    account: GraphAccount,
    target: MutationTarget,
    is_read: bool,
) -> Result<(), Error> {
    let values = resolve_target_values(&account, target, "id,changeKey").await?;
    let mut patches = Vec::new();
    for message in values {
        let id = object_id_from_value(&message)?;
        let etag = graph_etag(&message).ok_or_else(|| {
            Error::Other(format!("Graph message {} did not expose an etag", id.0))
        })?;
        patches.push(MessagePatch {
            id,
            body: json!({ "isRead": is_read }),
            etag,
        });
    }
    patch_messages(&account, patches).await
}

pub(crate) async fn send_message(
    account: GraphAccount,
    request: bifrost_types::SendRequest,
) -> Result<ObjectId, Error> {
    if !request.attachments_uploaded.is_empty() {
        return Err(Error::Other(GRAPH_PRE_UPLOADED_ATTACHMENTS_MSG.to_string()));
    }
    let message = message_from_send_request(&request)?;
    let draft = create_draft_message(&account, message).await?;
    send_draft_message(&account, &draft).await?;
    Ok(ObjectId(draft.0))
}

pub(crate) async fn draft_create(
    account: GraphAccount,
    patch: DraftPatch,
) -> Result<DraftHandle, Error> {
    if patch
        .attachments_uploaded
        .as_ref()
        .is_some_and(|attachments| !attachments.is_empty())
    {
        return Err(Error::Other(GRAPH_PRE_UPLOADED_ATTACHMENTS_MSG.to_string()));
    }
    let message = message_from_draft_patch(&patch, true)?;
    create_draft_message(&account, message).await
}

pub(crate) async fn draft_update(
    account: GraphAccount,
    draft: DraftHandle,
    patch: DraftPatch,
) -> Result<(), Error> {
    if patch.attachments_inline.is_some() || patch.attachments_uploaded.is_some() {
        return Err(Error::Other(GRAPH_DRAFT_UPDATE_ATTACHMENTS_MSG.to_string()));
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
        .map_err(Error::Transport)
}

pub(crate) async fn draft_discard(account: GraphAccount, draft: DraftHandle) -> Result<(), Error> {
    let path = format!(
        "{}/messages/{}",
        account.client.api_path_prefix(),
        bifrost_net::url::encode_component(&draft.0)
    );
    account.client.delete(&path).await.map_err(Error::Transport)
}

pub(crate) async fn draft_send(
    account: GraphAccount,
    draft: DraftHandle,
) -> Result<ObjectId, Error> {
    send_draft_message(&account, &draft).await?;
    Ok(ObjectId(draft.0))
}

pub(crate) async fn search(
    account: GraphAccount,
    request: SearchRequest,
) -> Result<Page<ThreadId>, Error> {
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
    })
}

pub(crate) async fn search_messages(
    account: GraphAccount,
    request: SearchRequest,
) -> Result<Page<ObjectId>, Error> {
    let page = search_message_rows(&account, request).await?;
    Ok(Page {
        items: page.items.into_iter().map(|row| row.id).collect(),
        next_cursor: page.next_cursor,
        estimated_total: page.estimated_total,
    })
}

pub(crate) async fn containers_list(account: GraphAccount) -> Result<Vec<Container>, Error> {
    let folders = account
        .client
        .list_mail_folders_recursive()
        .await
        .map_err(Error::Transport)?;
    account.folder_tree.write().await.replace_mail_folders(
        folders
            .iter()
            .map(|folder| (folder.id.clone(), folder.parent_folder_id.clone())),
    );
    let roles = well_known_folder_roles(&account).await;
    Ok(folders
        .into_iter()
        .map(|folder| container_from_folder(folder, &roles))
        .collect())
}

pub(crate) async fn container_create(
    account: GraphAccount,
    kind: ContainerKind,
    name: String,
    parent: Option<ContainerId>,
) -> Result<ContainerId, Error> {
    if kind != ContainerKind::Folder {
        return Err(Error::Unsupported);
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
        .map_err(Error::Transport)?;
    Ok(ContainerId(folder.id))
}

pub(crate) async fn container_rename(
    account: GraphAccount,
    container: ContainerId,
    name: String,
) -> Result<(), Error> {
    let path = format!(
        "{}/mailFolders/{}",
        account.client.api_path_prefix(),
        bifrost_net::url::encode_component(&container.0)
    );
    account
        .client
        .patch(&path, &json!({ "displayName": name }))
        .await
        .map_err(Error::Transport)
}

pub(crate) async fn container_move(
    account: GraphAccount,
    container: ContainerId,
    new_parent: Option<ContainerId>,
) -> Result<(), Error> {
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
        .map_err(Error::Transport)?;
    Ok(())
}

pub(crate) async fn container_delete(
    account: GraphAccount,
    container: ContainerId,
) -> Result<(), Error> {
    let path = format!(
        "{}/mailFolders/{}",
        account.client.api_path_prefix(),
        bifrost_net::url::encode_component(&container.0)
    );
    account.client.delete(&path).await.map_err(Error::Transport)
}

pub(crate) async fn identities_list(account: GraphAccount) -> Result<Vec<Identity>, Error> {
    let profile = account
        .client
        .get_profile()
        .await
        .map_err(Error::Transport)?;
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

pub(crate) async fn vacation_get(account: GraphAccount) -> Result<Option<VacationConfig>, Error> {
    let path = format!(
        "{}/mailboxSettings?$select=automaticRepliesSetting",
        account.client.api_path_prefix()
    );
    let settings: MailboxSettings = account
        .client
        .get_json(&path)
        .await
        .map_err(Error::Transport)?;
    Ok(settings.automatic_replies_setting.map(vacation_from_graph))
}

pub(crate) async fn vacation_set(
    account: GraphAccount,
    config: VacationConfig,
) -> Result<(), Error> {
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
        .map_err(Error::Transport)
}

pub(crate) async fn thread_hydrate(
    account: GraphAccount,
    thread: ThreadId,
) -> Result<ThreadHydration, Error> {
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
) -> Result<Message, Error> {
    let value =
        fetch_message_value(&account, &message, hydrate_select(expand_blobs(projection))).await?;
    message_from_value(&value, projection)
}

pub(crate) async fn move_thread(
    account: GraphAccount,
    thread: ThreadId,
    target: ContainerId,
) -> Result<(), Error> {
    add_to_container(account, MutationTarget::Thread(thread), target).await
}

pub(crate) async fn delete_thread(
    account: GraphAccount,
    thread: ThreadId,
    current: Option<ContainerId>,
) -> Result<(), Error> {
    let ids = resolve_target_ids(&account, MutationTarget::Thread(thread)).await?;
    let trash = trash_container_id(&account).await;
    let already_in_trash = current
        .as_ref()
        .is_some_and(|id| id.0 == trash.0 || id.0.eq_ignore_ascii_case(DELETED_ITEMS));
    if already_in_trash {
        destroy_messages(&account, &ids).await
    } else {
        move_messages(&account, &ids, &trash.0).await
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
) -> Result<Vec<ObjectId>, Error> {
    match target {
        MutationTarget::Message(id) => Ok(vec![id]),
        MutationTarget::Thread(thread) => {
            let values = message_values_for_thread(account, &thread, "id").await?;
            values.iter().map(object_id_from_value).collect()
        }
        _ => Err(Error::Unsupported),
    }
}

async fn resolve_target_values(
    account: &GraphAccount,
    target: MutationTarget,
    select: &str,
) -> Result<Vec<Value>, Error> {
    match target {
        MutationTarget::Message(id) => Ok(vec![fetch_message_value(account, &id, select).await?]),
        MutationTarget::Thread(thread) => message_values_for_thread(account, &thread, select).await,
        _ => Err(Error::Unsupported),
    }
}

async fn fetch_message_value(
    account: &GraphAccount,
    id: &ObjectId,
    select: &str,
) -> Result<Value, Error> {
    let path = format!(
        "{}/messages/{}?{}",
        account.client.api_path_prefix(),
        bifrost_net::url::encode_component(&id.0),
        select_query(select)
    );
    let value = account
        .client
        .get_json(&path)
        .await
        .map_err(Error::Transport)?;
    cache_etag(account, &value).await?;
    Ok(value)
}

async fn message_values_for_thread(
    account: &GraphAccount,
    thread: &ThreadId,
    select: &str,
) -> Result<Vec<Value>, Error> {
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
) -> Result<Vec<Value>, Error> {
    let mut values = Vec::new();
    let mut next_url = Some(first_url);
    while let Some(url) = next_url {
        let page: ODataCollection<Value> = if url.starts_with("http") {
            account.client.get_absolute(&url).await
        } else {
            account.client.get_json(&url).await
        }
        .map_err(Error::Transport)?;
        values.extend(page.value);
        next_url = page.next_link;
    }
    Ok(values)
}

async fn patch_messages(account: &GraphAccount, patches: Vec<MessagePatch>) -> Result<(), Error> {
    let prefix = account.client.api_path_prefix();
    let mut requests = Vec::new();
    for (index, patch) in patches.iter().enumerate() {
        let mut headers = HashMap::new();
        headers.insert("If-Match".to_string(), patch.etag.clone());
        requests.push(BatchRequestItem {
            id: index.to_string(),
            method: "PATCH".to_string(),
            url: format!(
                "{prefix}/messages/{}",
                bifrost_net::url::encode_component(&patch.id.0)
            ),
            body: Some(patch.body.clone()),
            headers: Some(headers),
        });
    }
    submit_write_batch(account, requests, false).await
}

async fn move_messages(
    account: &GraphAccount,
    ids: &[ObjectId],
    destination: &str,
) -> Result<(), Error> {
    let values = message_values_for_ids(account, ids, "id,changeKey").await?;
    let prefix = account.client.api_path_prefix();
    let mut requests = Vec::new();
    for (index, value) in values.iter().enumerate() {
        let id = object_id_from_value(value)?;
        let etag = graph_etag(value).ok_or_else(|| {
            Error::Other(format!("Graph message {} did not expose an etag", id.0))
        })?;
        let mut headers = HashMap::new();
        headers.insert("If-Match".to_string(), etag);
        requests.push(BatchRequestItem {
            id: index.to_string(),
            method: "POST".to_string(),
            url: format!(
                "{prefix}/messages/{}/move",
                bifrost_net::url::encode_component(&id.0)
            ),
            body: Some(json!({ "destinationId": destination })),
            headers: Some(headers),
        });
    }
    submit_write_batch(account, requests, false).await
}

async fn destroy_messages(account: &GraphAccount, ids: &[ObjectId]) -> Result<(), Error> {
    let values = message_values_for_ids(account, ids, "id,changeKey").await?;
    let prefix = account.client.api_path_prefix();
    let mut requests = Vec::new();
    for (index, value) in values.iter().enumerate() {
        let id = object_id_from_value(value)?;
        let mut headers = HashMap::new();
        if let Some(etag) = graph_etag(value) {
            headers.insert("If-Match".to_string(), etag);
        }
        requests.push(BatchRequestItem {
            id: index.to_string(),
            method: "DELETE".to_string(),
            url: format!(
                "{prefix}/messages/{}",
                bifrost_net::url::encode_component(&id.0)
            ),
            body: None,
            headers: (!headers.is_empty()).then_some(headers),
        });
    }
    submit_write_batch(account, requests, true).await
}

async fn message_values_for_ids(
    account: &GraphAccount,
    ids: &[ObjectId],
    select: &str,
) -> Result<Vec<Value>, Error> {
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
) -> Result<(), Error> {
    if requests.is_empty() {
        return Ok(());
    }
    let response: BatchResponse = account
        .client
        .post_batch(&BatchRequest { requests })
        .await
        .map_err(Error::Transport)?;
    for item in response.responses {
        match item.status {
            200..=299 => {}
            404 if destroy => {}
            412 => return Err(Error::ConcurrencyConflict),
            status => {
                return Err(Error::Transport(format!(
                    "Graph write batch item {} failed with HTTP {status}",
                    item.id
                )));
            }
        }
    }
    Ok(())
}

async fn cache_etag(account: &GraphAccount, value: &Value) -> Result<(), Error> {
    let Some(etag) = graph_etag(value) else {
        return Ok(());
    };
    let id = object_id_from_value(value)?;
    account.etag_index.write().await.insert(id.0, etag);
    Ok(())
}

fn object_id_from_value(value: &Value) -> Result<ObjectId, Error> {
    value
        .get("id")
        .and_then(Value::as_str)
        .map(|id| ObjectId(id.to_string()))
        .ok_or_else(|| Error::Other("Graph message did not include an id".to_string()))
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

fn message_from_send_request(request: &bifrost_types::SendRequest) -> Result<Value, Error> {
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
    message_from_draft_patch(&patch, true)
}

fn message_from_draft_patch(patch: &DraftPatch, include_empty: bool) -> Result<Value, Error> {
    if patch
        .attachments_uploaded
        .as_ref()
        .is_some_and(|attachments| !attachments.is_empty())
    {
        return Err(Error::Unsupported);
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

fn graph_attachment_from_inline(attachment: &AttachmentInline) -> Result<Value, Error> {
    let content_bytes = base64::engine::general_purpose::STANDARD.encode(&attachment.data);
    Ok(json!({
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": attachment.filename.clone(),
        "contentType": attachment.mime.clone(),
        "isInline": attachment.inline,
        "contentBytes": content_bytes
    }))
}

async fn create_draft_message(
    account: &GraphAccount,
    message: Value,
) -> Result<DraftHandle, Error> {
    let path = format!("{}/messages", account.client.api_path_prefix());
    let created: Value = account
        .client
        .post(&path, &message)
        .await
        .map_err(Error::Transport)?;
    let id = created
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Other("Graph draft create did not return an id".to_string()))?;
    Ok(DraftHandle(id.to_string()))
}

async fn send_draft_message(account: &GraphAccount, draft: &DraftHandle) -> Result<(), Error> {
    let path = format!(
        "{}/messages/{}/send",
        account.client.api_path_prefix(),
        bifrost_net::url::encode_component(&draft.0)
    );
    account
        .client
        .post_empty(&path)
        .await
        .map_err(Error::Transport)
}

#[derive(Debug)]
struct SearchRow {
    id: ObjectId,
    thread_id: Option<ThreadId>,
}

async fn search_message_rows(
    account: &GraphAccount,
    request: SearchRequest,
) -> Result<Page<SearchRow>, Error> {
    let url = search_url(account, &request)?;
    let page: ODataCollection<Value> = if url.starts_with("http") {
        account.client.get_absolute(&url).await
    } else {
        account.client.get_json(&url).await
    }
    .map_err(Error::Transport)?;
    let mut rows = Vec::new();
    for value in page.value {
        let id = object_id_from_value(&value)?;
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
    })
}

fn search_url(account: &GraphAccount, request: &SearchRequest) -> Result<String, Error> {
    if let Some(cursor) = &request.page_cursor {
        return String::from_utf8(cursor.clone())
            .map_err(|error| Error::Other(format!("Graph search cursor is not UTF-8: {error}")));
    }
    let mut params = vec![
        "$select=id,conversationId".to_string(),
        format!("$top={}", request.limit.unwrap_or(50).clamp(1, 250)),
    ];
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
    Ok(format!(
        "{}/messages?{}",
        account.client.api_path_prefix(),
        params.join("&")
    ))
}

fn odata_filter(filter: &SearchFilter) -> Result<String, Error> {
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
                Err(Error::Unsupported)
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
        _ => Err(Error::Unsupported),
    }
}

fn join_filters(filters: &[SearchFilter], op: &str) -> Result<String, Error> {
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
        "$select=id,conversationId,subject,bodyPreview,body,uniqueBody,from,toRecipients,ccRecipients,bccRecipients,replyTo,receivedDateTime,sentDateTime,parentFolderId,isRead,categories,flag,internetMessageHeaders,internetMessageId,hasAttachments,changeKey&$expand=attachments"
    } else {
        "id,conversationId,subject,bodyPreview,body,uniqueBody,from,toRecipients,ccRecipients,bccRecipients,replyTo,receivedDateTime,sentDateTime,parentFolderId,isRead,categories,flag,internetMessageHeaders,internetMessageId,hasAttachments,changeKey"
    }
}

fn expand_blobs(projection: HydrationProjection) -> bool {
    matches!(projection, HydrationProjection::FullWithBlobs)
}

fn message_from_value(value: &Value, projection: HydrationProjection) -> Result<Message, Error> {
    let id = object_id_from_value(value)?;
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

fn container_from_folder(
    folder: GraphMailFolder,
    roles: &HashMap<String, FolderRole>,
) -> Container {
    let role = roles
        .get(&folder.id)
        .copied()
        .or_else(|| role_from_well_known_name(&folder.id));
    let id = ContainerId(folder.id.clone());
    Container {
        id: id.clone(),
        kind: ContainerKind::Folder,
        role,
        provenance: Provenance {
            provider: ProtocolKind::Graph,
            kind: ContainerKind::Folder,
            native: folder.id.clone(),
        },
        native_id: folder.id,
        name: folder.display_name.unwrap_or_else(|| id.0.clone()),
        parent: folder.parent_folder_id.map(ContainerId),
    }
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

    #[test]
    fn inline_attachment_is_base64_encoded() {
        let attachment = AttachmentInline {
            filename: "note.txt".to_string(),
            mime: "text/plain".to_string(),
            data: Bytes::from_static(b"hello"),
            inline: false,
        };
        let value = graph_attachment_from_inline(&attachment).expect("attachment builds");
        assert_eq!(value["contentBytes"], json!("aGVsbG8="));
    }
}
