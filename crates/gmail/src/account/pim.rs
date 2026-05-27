use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use bifrost_types::{
    AccountError, AccountFuture, AccountStream, Address, AttachmentHandle, AttachmentInline,
    Container, ContainerId, ContainerKind, DraftHandle, DraftPatch, FolderRole,
    HydrationProjection, Identity, IdentityId, IdentityPatch, Message, MutationTarget, ObjectId,
    Page, ProtocolKind, Provenance, QuotaInfo, SearchFilter, SearchRequest, SendRequest,
    ThreadHydration, ThreadId, VacationConfig,
};
use bytes::Bytes;
use chrono::{DateTime, Datelike, Utc};
use serde_json::json;

use crate::client::GmailClient;
use crate::encoding::decode_base64url_nopad;
use crate::headers::find_header_value_case_insensitive;
use crate::types::{GmailHeader, GmailLabel, GmailMessage, GmailPayload, GmailVacationSettings};

use super::blobs;
use super::error;
use super::flags;
use super::scopes::{ScopeCache, labels_for_flags, refresh_scope_snapshot};

const LABEL_INBOX: &str = "INBOX";
const LABEL_SENT: &str = "SENT";
const LABEL_DRAFT: &str = "DRAFT";
const LABEL_TRASH: &str = "TRASH";
const LABEL_SPAM: &str = "SPAM";
const LABEL_UNREAD: &str = "UNREAD";
const ARCHIVE_ID: &str = "archive";
const MAX_GMAIL_PAGE_SIZE: u32 = 500;

pub(crate) fn add_to_container(
    client: Arc<GmailClient>,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let (add, remove) = add_container_patch(&container);
        modify_target(
            &client,
            target,
            add,
            remove,
            bifrost_types::AccountOperation::AddToContainer,
        )
        .await
    })
}

pub(crate) fn remove_from_container(
    client: Arc<GmailClient>,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let (add, remove) = remove_container_patch(&container);
        modify_target(
            &client,
            target,
            add,
            remove,
            bifrost_types::AccountOperation::RemoveFromContainer,
        )
        .await
    })
}

pub(crate) fn set_label_membership(
    client: Arc<GmailClient>,
    target: MutationTarget,
    label: ContainerId,
    value: bool,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let (add, remove) = if value {
            add_container_patch(&label)
        } else {
            remove_container_patch(&label)
        };
        modify_target(
            &client,
            target,
            add,
            remove,
            bifrost_types::AccountOperation::SetLabelMembership,
        )
        .await
    })
}

pub(crate) fn set_is_read(
    client: Arc<GmailClient>,
    target: MutationTarget,
    is_read: bool,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let add = if is_read {
            Vec::new()
        } else {
            vec![LABEL_UNREAD.to_string()]
        };
        let remove = if is_read {
            vec![LABEL_UNREAD.to_string()]
        } else {
            Vec::new()
        };
        modify_target(
            &client,
            target,
            add,
            remove,
            bifrost_types::AccountOperation::SetIsRead,
        )
        .await
    })
}

pub(crate) fn send_message(
    client: Arc<GmailClient>,
    default_address: String,
    request: SendRequest,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        if !request.attachments_uploaded.is_empty() {
            return Err(unsupported(
                bifrost_types::AccountOperation::AttachmentUpload,
            ));
        }
        let doc = MailDocument::from_send(request, &default_address);
        let raw = render_message(&doc, &default_address, true)?;
        let message = client
            .send_message(&raw, doc.thread_id.as_deref())
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::send()))?;
        Ok(ObjectId(message.id))
    })
}

pub(crate) fn attachment_upload(
    _bytes: AccountStream<Result<Bytes, AccountError>>,
    _mime: String,
) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
    Box::pin(async {
        Err(unsupported(
            bifrost_types::AccountOperation::AttachmentUpload,
        ))
    })
}

pub(crate) fn draft_create(
    client: Arc<GmailClient>,
    default_address: String,
    patch: DraftPatch,
) -> AccountFuture<Result<DraftHandle, AccountError>> {
    Box::pin(async move {
        let doc = MailDocument::from_draft_patch(patch, &default_address);
        let raw = render_message(&doc, &default_address, false)?;
        let draft = client
            .create_draft(&raw, doc.thread_id.as_deref())
            .await
            .map_err(|e| {
                account_error_for(
                    e,
                    error::GmailErrorContext::draft(bifrost_types::AccountOperation::DraftCreate),
                )
            })?;
        Ok(DraftHandle(draft.id))
    })
}

pub(crate) fn draft_update(
    client: Arc<GmailClient>,
    default_address: String,
    draft: DraftHandle,
    patch: DraftPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let ctx = error::GmailErrorContext::draft(bifrost_types::AccountOperation::DraftUpdate);
        let existing = client
            .get_draft(&draft.0, "full")
            .await
            .map_err(|e| account_error_for(e, ctx.clone()))?;
        let mut doc = document_from_message(&client, &existing.message, true).await?;
        doc.apply_patch(patch);
        let raw = render_message(&doc, &default_address, false)?;
        client
            .update_draft(&draft.0, &raw, doc.thread_id.as_deref())
            .await
            .map_err(|e| account_error_for(e, ctx))?;
        Ok(())
    })
}

pub(crate) fn draft_discard(
    client: Arc<GmailClient>,
    draft: DraftHandle,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        client.delete_draft(&draft.0).await.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::draft(bifrost_types::AccountOperation::DraftDiscard),
            )
        })
    })
}

pub(crate) fn draft_send(
    client: Arc<GmailClient>,
    draft: DraftHandle,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        let message = client.send_draft(&draft.0).await.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::draft(bifrost_types::AccountOperation::DraftSend),
            )
        })?;
        Ok(ObjectId(message.id))
    })
}

pub(crate) fn search(
    client: Arc<GmailClient>,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
    Box::pin(async move {
        let query = gmail_query(&request)?;
        let page_token = page_token(&request)?;
        let max = request
            .limit
            .unwrap_or(MAX_GMAIL_PAGE_SIZE)
            .min(MAX_GMAIL_PAGE_SIZE);
        let (threads, next) = client
            .list_threads(query.as_deref(), Some(max), page_token.as_deref())
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::search()))?;
        Ok(Page {
            items: threads
                .into_iter()
                .map(|thread| ThreadId(thread.id))
                .collect(),
            next_cursor: next.map(String::into_bytes),
            estimated_total: None,
        })
    })
}

pub(crate) fn search_messages(
    client: Arc<GmailClient>,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
    Box::pin(async move {
        let query = gmail_query(&request)?;
        let page_token = page_token(&request)?;
        let max = request
            .limit
            .unwrap_or(MAX_GMAIL_PAGE_SIZE)
            .min(MAX_GMAIL_PAGE_SIZE);
        let (messages, next, estimate) = client
            .list_messages(query.as_deref(), Some(max), page_token.as_deref())
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::search_messages()))?;
        Ok(Page {
            items: messages
                .into_iter()
                .map(|message| ObjectId(message.id))
                .collect(),
            next_cursor: next.map(String::into_bytes),
            estimated_total: estimate.and_then(non_negative_i64),
        })
    })
}

pub(crate) fn containers_list(
    client: Arc<GmailClient>,
    cache: ScopeCache,
) -> AccountFuture<Result<Vec<Container>, AccountError>> {
    Box::pin(async move {
        let snapshot = refresh_scope_snapshot(&client, &cache)
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::containers_list()))?;
        let mut containers = Vec::with_capacity(snapshot.labels.len() + 1);
        containers.push(archive_container());
        containers.extend(snapshot.labels.iter().map(container_from_label));
        Ok(containers)
    })
}

pub(crate) fn container_create(
    client: Arc<GmailClient>,
    kind: ContainerKind,
    name: String,
    parent: Option<ContainerId>,
) -> AccountFuture<Result<ContainerId, AccountError>> {
    Box::pin(async move {
        if parent.is_some() || !matches!(kind, ContainerKind::Label) {
            return Err(unsupported(
                bifrost_types::AccountOperation::ContainerCreate,
            ));
        }
        let label = client.create_label(&name, None).await.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::container(
                    bifrost_types::AccountOperation::ContainerCreate,
                ),
            )
        })?;
        Ok(ContainerId(label.id))
    })
}

pub(crate) fn container_rename(
    client: Arc<GmailClient>,
    container: ContainerId,
    name: String,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if is_archive_id(&container.0) {
            return Err(unsupported(
                bifrost_types::AccountOperation::ContainerRename,
            ));
        }
        client
            .update_label(&container.0, Some(&name), None)
            .await
            .map_err(|e| {
                account_error_for(
                    e,
                    error::GmailErrorContext::container(
                        bifrost_types::AccountOperation::ContainerRename,
                    ),
                )
            })?;
        Ok(())
    })
}

pub(crate) fn container_move(
    _container: ContainerId,
    _new_parent: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContainerMove)) })
}

pub(crate) fn container_delete(
    client: Arc<GmailClient>,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if is_archive_id(&container.0) {
            return Err(unsupported(
                bifrost_types::AccountOperation::ContainerDelete,
            ));
        }
        client.delete_label(&container.0).await.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::container(
                    bifrost_types::AccountOperation::ContainerDelete,
                ),
            )
        })
    })
}

pub(crate) fn identities_list(
    client: Arc<GmailClient>,
) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
    Box::pin(async move {
        let identities = client
            .list_send_as()
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::identities_list()))?
            .into_iter()
            .map(|send_as| Identity {
                id: IdentityId(send_as.send_as_email.clone()),
                name: send_as.display_name.unwrap_or_default(),
                address: send_as.send_as_email,
                signature_text: None,
                signature_html: send_as.signature,
                reply_to: send_as.reply_to_address.map(Address::bare),
                is_default: send_as.is_default.unwrap_or(false),
            })
            .collect();
        Ok(identities)
    })
}

pub(crate) fn identity_update(
    client: Arc<GmailClient>,
    identity: IdentityId,
    patch: IdentityPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let mut body = json!({});
        if let Some(name) = patch.name {
            body["displayName"] = json!(name);
        }
        if let Some(signature) = patch.signature_html {
            body["signature"] =
                signature.map_or(serde_json::Value::Null, serde_json::Value::String);
        } else if let Some(signature) = patch.signature_text {
            body["signature"] =
                signature.map_or(serde_json::Value::Null, serde_json::Value::String);
        }
        if let Some(reply_to) = patch.reply_to {
            body["replyToAddress"] =
                reply_to.map_or(serde_json::Value::Null, |addr| json!(addr.address));
        }
        if let Some(is_default) = patch.is_default {
            body["isDefault"] = json!(is_default);
        }
        if body.as_object().is_none_or(serde_json::Map::is_empty) {
            return Ok(());
        }
        client
            .patch_send_as(&identity.0, &body)
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::identity_update()))?;
        Ok(())
    })
}

pub(crate) fn vacation_get(
    client: Arc<GmailClient>,
) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
    Box::pin(async move {
        let settings = client
            .get_vacation()
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::vacation_get()))?;
        Ok(Some(VacationConfig {
            is_enabled: settings.enable_auto_reply.unwrap_or(false),
            subject: settings.response_subject,
            body_text: settings.response_body_plain_text,
            body_html: settings.response_body_html,
            starts_at: settings
                .start_time
                .as_deref()
                .and_then(system_time_from_millis),
            ends_at: settings
                .end_time
                .as_deref()
                .and_then(system_time_from_millis),
        }))
    })
}

pub(crate) fn vacation_set(
    client: Arc<GmailClient>,
    config: VacationConfig,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let settings = GmailVacationSettings {
            enable_auto_reply: Some(config.is_enabled),
            response_subject: config.subject,
            response_body_plain_text: config.body_text,
            response_body_html: config.body_html,
            start_time: config.starts_at.map(system_time_to_millis_string),
            end_time: config.ends_at.map(system_time_to_millis_string),
        };
        client
            .update_vacation(&settings)
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::vacation_set()))?;
        Ok(())
    })
}

pub(crate) fn quota_get() -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
    Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::QuotaGet)) })
}

pub(crate) fn thread_hydrate(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    thread: ThreadId,
) -> AccountFuture<Result<ThreadHydration, AccountError>> {
    Box::pin(async move {
        let labels = labels_for_flags(&client, &cache).await;
        let gmail_thread = client.get_thread(&thread.0, "full").await.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::hydrate_thread(thread.0.clone()),
            )
        })?;
        let mut messages = Vec::with_capacity(gmail_thread.messages.len());
        for message in &gmail_thread.messages {
            messages.push(message_from_gmail(&labels, message, HydrationProjection::Full).await?);
        }
        Ok(ThreadHydration {
            id: ThreadId(gmail_thread.id),
            messages,
        })
    })
}

pub(crate) fn message_hydrate(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    message: ObjectId,
    projection: HydrationProjection,
) -> AccountFuture<Result<Message, AccountError>> {
    Box::pin(async move {
        let labels = labels_for_flags(&client, &cache).await;
        let format = match projection {
            HydrationProjection::Headers | HydrationProjection::Preview(_) => "metadata",
            HydrationProjection::Full | HydrationProjection::FullWithBlobs => "full",
            _ => "full",
        };
        let gmail_message = client.get_message(&message.0, format).await.map_err(|e| {
            account_error_for(
                e,
                error::GmailErrorContext::hydrate_message(message.0.clone()),
            )
        })?;
        message_from_gmail(&labels, &gmail_message, projection).await
    })
}

pub(crate) fn move_thread(
    client: Arc<GmailClient>,
    thread: ThreadId,
    target: ContainerId,
    source: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let target_target = MutationTarget::Thread(thread.clone());
        let (add, remove) = add_container_patch(&target);
        modify_target(
            &client,
            target_target,
            add,
            remove,
            bifrost_types::AccountOperation::BulkMove,
        )
        .await?;
        if let Some(source) = source
            && source != target
        {
            let (add, remove) = remove_container_patch(&source);
            modify_target(
                &client,
                MutationTarget::Thread(thread),
                add,
                remove,
                bifrost_types::AccountOperation::RemoveFromContainer,
            )
            .await?;
        }
        Ok(())
    })
}

pub(crate) fn delete_thread(
    client: Arc<GmailClient>,
    thread: ThreadId,
    current: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        if current
            .as_ref()
            .is_some_and(|id| id.0.eq_ignore_ascii_case(LABEL_TRASH))
        {
            return client.delete_thread(&thread.0).await.map_err(|e| {
                account_error_for(
                    e,
                    error::GmailErrorContext::mutation(
                        bifrost_types::AccountOperation::BulkDestroy,
                    ),
                )
            });
        }
        let (add, remove) = add_container_patch(&ContainerId(LABEL_TRASH.to_string()));
        modify_target(
            &client,
            MutationTarget::Thread(thread),
            add,
            remove,
            bifrost_types::AccountOperation::BulkMove,
        )
        .await
    })
}

async fn modify_target(
    client: &GmailClient,
    target: MutationTarget,
    add_labels: Vec<String>,
    remove_labels: Vec<String>,
    operation: bifrost_types::AccountOperation,
) -> Result<(), AccountError> {
    if add_labels.is_empty() && remove_labels.is_empty() {
        return Ok(());
    }
    match target {
        MutationTarget::Message(id) => {
            client
                .modify_message(&id.0, &add_labels, &remove_labels)
                .await
                .map_err(|e| account_error_for(e, error::GmailErrorContext::base(operation)))?;
        }
        MutationTarget::Thread(id) => {
            client
                .modify_thread(&id.0, &add_labels, &remove_labels)
                .await
                .map_err(|e| account_error_for(e, error::GmailErrorContext::base(operation)))?;
        }
        _ => return Err(unsupported(operation)),
    }
    Ok(())
}

fn add_container_patch(container: &ContainerId) -> (Vec<String>, Vec<String>) {
    if is_archive_id(&container.0) {
        (Vec::new(), vec![LABEL_INBOX.to_string()])
    } else {
        (vec![container.0.clone()], Vec::new())
    }
}

fn remove_container_patch(container: &ContainerId) -> (Vec<String>, Vec<String>) {
    if is_archive_id(&container.0) {
        (Vec::new(), Vec::new())
    } else {
        (Vec::new(), vec![container.0.clone()])
    }
}

fn gmail_query(request: &SearchRequest) -> Result<Option<String>, AccountError> {
    let mut parts = Vec::new();
    if let Some(filter) = &request.filter {
        parts.push(filter_query(filter)?);
    }
    if let Some(provider) = &request.provider_query
        && !provider.trim().is_empty()
    {
        parts.push(provider.trim().to_string());
    }
    Ok((!parts.is_empty()).then(|| parts.join(" ")))
}

fn filter_query(filter: &SearchFilter) -> Result<String, AccountError> {
    match filter {
        SearchFilter::From(value) => Ok(format!("from:{}", query_term(value))),
        SearchFilter::To(value) => Ok(format!("to:{}", query_term(value))),
        SearchFilter::Subject(value) => Ok(format!("subject:{}", query_term(value))),
        SearchFilter::Body(value) => Ok(query_term(value)),
        SearchFilter::Has(value) => {
            if value.eq_ignore_ascii_case("attachment") {
                Ok("has:attachment".to_string())
            } else {
                Ok(format!("filename:{}", query_term(value)))
            }
        }
        SearchFilter::In(container) => Ok(container_query(&container.0)),
        SearchFilter::Labeled(label) => Ok(format!("label:{}", query_term(&label.0))),
        SearchFilter::DateRange { after, before } => {
            let mut parts = Vec::new();
            if let Some(after) = after {
                parts.push(format!("after:{}", gmail_date(*after)));
            }
            if let Some(before) = before {
                parts.push(format!("before:{}", gmail_date(*before)));
            }
            Ok(parts.join(" "))
        }
        SearchFilter::And(filters) => filters
            .iter()
            .map(filter_query)
            .collect::<Result<Vec<_>, _>>()
            .map(|items| items.join(" ")),
        SearchFilter::Or(filters) => filters
            .iter()
            .map(filter_query)
            .collect::<Result<Vec<_>, _>>()
            .map(|items| format!("({})", items.join(" OR "))),
        SearchFilter::Not(filter) => Ok(format!("-({})", filter_query(filter)?)),
        _ => Err(unsupported(bifrost_types::AccountOperation::Search)),
    }
}

fn page_token(request: &SearchRequest) -> Result<Option<String>, AccountError> {
    request
        .page_cursor
        .as_ref()
        .map(|cursor| {
            String::from_utf8(cursor.clone()).map_err(|error| {
                other_error(
                    bifrost_types::AccountOperation::Search,
                    format!("invalid gmail page cursor: {error}"),
                )
            })
        })
        .transpose()
}

fn query_term(value: &str) -> String {
    let sanitized = sanitize_header(value);
    if sanitized
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '@' | '/' | ':'))
    {
        sanitized
    } else {
        format!(
            "\"{}\"",
            sanitized.replace('\\', "\\\\").replace('"', "\\\"")
        )
    }
}

fn container_query(id: &str) -> String {
    match id {
        LABEL_INBOX => "in:inbox".to_string(),
        LABEL_SENT => "in:sent".to_string(),
        LABEL_DRAFT => "in:drafts".to_string(),
        LABEL_TRASH => "in:trash".to_string(),
        LABEL_SPAM => "in:spam".to_string(),
        _ if is_archive_id(id) => "-in:inbox -in:sent -in:drafts -in:trash -in:spam".to_string(),
        _ => format!("label:{}", query_term(id)),
    }
}

fn gmail_date(time: SystemTime) -> String {
    let dt: DateTime<Utc> = time.into();
    format!("{:04}/{:02}/{:02}", dt.year(), dt.month(), dt.day())
}

fn archive_container() -> Container {
    Container {
        id: ContainerId(ARCHIVE_ID.to_string()),
        kind: ContainerKind::Label,
        role: Some(FolderRole::Archive),
        provenance: Provenance {
            provider: ProtocolKind::Gmail,
            kind: ContainerKind::Label,
            native: ARCHIVE_ID.to_string(),
        },
        native_id: ARCHIVE_ID.to_string(),
        name: "Archive".to_string(),
        parent: None,
    }
}

fn container_from_label(label: &GmailLabel) -> Container {
    let role = match label.id.as_str() {
        LABEL_INBOX => Some(FolderRole::Inbox),
        LABEL_SENT => Some(FolderRole::Sent),
        LABEL_DRAFT => Some(FolderRole::Drafts),
        LABEL_TRASH => Some(FolderRole::Trash),
        LABEL_SPAM => Some(FolderRole::Spam),
        _ => None,
    };
    Container {
        id: ContainerId(label.id.clone()),
        kind: ContainerKind::Label,
        role,
        provenance: Provenance {
            provider: ProtocolKind::Gmail,
            kind: ContainerKind::Label,
            native: label.id.clone(),
        },
        native_id: label.id.clone(),
        name: label.name.clone(),
        parent: None,
    }
}

async fn message_from_gmail(
    labels: &[GmailLabel],
    message: &GmailMessage,
    projection: HydrationProjection,
) -> Result<Message, AccountError> {
    let headers = message_headers(message);
    let (body_text, body_html) = match projection {
        HydrationProjection::Headers => (None, None),
        HydrationProjection::Preview(limit) => (
            Some(message.snippet.chars().take(limit).collect::<String>()),
            None,
        ),
        HydrationProjection::Full | HydrationProjection::FullWithBlobs => {
            body_parts(message.payload.as_ref())
        }
        _ => body_parts(message.payload.as_ref()),
    };
    let attachments = if matches!(projection, HydrationProjection::FullWithBlobs) {
        blobs::blob_handles_for_message(message)
    } else {
        Vec::new()
    };
    let flags = flags::flag_set(&message.label_ids, labels);

    Ok(Message {
        id: ObjectId(message.id.clone()),
        thread_id: Some(ThreadId(message.thread_id.clone())),
        from: parse_address_list(header_from(headers, "From").as_deref()),
        to: parse_address_list(header_from(headers, "To").as_deref()),
        cc: parse_address_list(header_from(headers, "Cc").as_deref()),
        bcc: parse_address_list(header_from(headers, "Bcc").as_deref()),
        reply_to: parse_address_list(header_from(headers, "Reply-To").as_deref()),
        subject: header_from(headers, "Subject"),
        date: internal_date(&message.internal_date),
        containers: message
            .label_ids
            .iter()
            .map(|id| ContainerId(id.clone()))
            .collect(),
        flags,
        body_text,
        body_html,
        attachments,
        size_bytes: message.size_estimate.and_then(non_negative_i64),
        in_reply_to: header_from(headers, "In-Reply-To"),
        references: references_from_header(header_from(headers, "References").as_deref()),
    })
}

async fn document_from_message(
    client: &GmailClient,
    message: &GmailMessage,
    include_attachments: bool,
) -> Result<MailDocument, AccountError> {
    let headers = message_headers(message);
    let (body_text, body_html) = body_parts(message.payload.as_ref());
    let attachments_inline = if include_attachments {
        attachment_inlines(client, message).await?
    } else {
        Vec::new()
    };
    Ok(MailDocument {
        identity: None,
        from: parse_address_list(header_from(headers, "From").as_deref())
            .into_iter()
            .next(),
        to: parse_address_list(header_from(headers, "To").as_deref()),
        cc: parse_address_list(header_from(headers, "Cc").as_deref()),
        bcc: parse_address_list(header_from(headers, "Bcc").as_deref()),
        reply_to: parse_address_list(header_from(headers, "Reply-To").as_deref()),
        subject: header_from(headers, "Subject"),
        body_text,
        body_html,
        attachments_inline,
        attachments_uploaded: Vec::new(),
        in_reply_to: header_from(headers, "In-Reply-To"),
        references: references_from_header(header_from(headers, "References").as_deref()),
        thread_id: Some(message.thread_id.clone()),
    })
}

async fn attachment_inlines(
    client: &GmailClient,
    message: &GmailMessage,
) -> Result<Vec<AttachmentInline>, AccountError> {
    let mut refs = Vec::new();
    if let Some(payload) = &message.payload {
        collect_attachment_refs(payload, &mut refs);
    }
    let mut attachments = Vec::with_capacity(refs.len());
    // attachment_inlines is used during draft hydration (draft_update);
    // the error context is DraftUpdate.
    let ctx = error::GmailErrorContext::draft(bifrost_types::AccountOperation::DraftUpdate);
    for item in refs {
        let attachment = client
            .get_attachment(&message.id, &item.attachment_id)
            .await
            .map_err(|e| account_error_for(e, ctx.clone()))?;
        let data = decode_base64url_nopad(&attachment.data)
            .map_err(|e| account_error_for(e, ctx.clone()))?;
        attachments.push(AttachmentInline {
            filename: item.filename,
            mime: item.mime,
            data: Bytes::from(data),
            inline: item.inline,
        });
    }
    Ok(attachments)
}

struct AttachmentRef {
    attachment_id: String,
    filename: String,
    mime: String,
    inline: bool,
}

fn collect_attachment_refs(part: &GmailPayload, refs: &mut Vec<AttachmentRef>) {
    if let Some(body) = &part.body
        && let Some(attachment_id) = &body.attachment_id
    {
        refs.push(AttachmentRef {
            attachment_id: attachment_id.clone(),
            filename: part.filename.clone(),
            mime: part.mime_type.clone(),
            inline: content_disposition_inline(&part.headers),
        });
    }
    for child in &part.parts {
        collect_attachment_refs(child, refs);
    }
}

fn content_disposition_inline(headers: &[GmailHeader]) -> bool {
    header_from(headers, "Content-Disposition")
        .as_deref()
        .is_some_and(|value| value.to_ascii_lowercase().contains("inline"))
}

fn body_parts(payload: Option<&GmailPayload>) -> (Option<String>, Option<String>) {
    let mut text = None;
    let mut html = None;
    if let Some(payload) = payload {
        collect_body_parts(payload, &mut text, &mut html);
    }
    (text, html)
}

fn collect_body_parts(part: &GmailPayload, text: &mut Option<String>, html: &mut Option<String>) {
    if let Some(data) = part.body.as_ref().and_then(|body| body.data.as_deref())
        && (text.is_none() || html.is_none())
        && let Ok(bytes) = decode_base64url_nopad(data)
        && let Ok(decoded) = String::from_utf8(bytes)
    {
        if part.mime_type.eq_ignore_ascii_case("text/plain") && text.is_none() {
            *text = Some(decoded);
        } else if part.mime_type.eq_ignore_ascii_case("text/html") && html.is_none() {
            *html = Some(decoded);
        }
    }
    for child in &part.parts {
        collect_body_parts(child, text, html);
    }
}

fn message_headers(message: &GmailMessage) -> &[GmailHeader] {
    message
        .payload
        .as_ref()
        .map_or(&[] as &[GmailHeader], |payload| payload.headers.as_slice())
}

fn header_from(headers: &[GmailHeader], name: &str) -> Option<String> {
    find_header_value_case_insensitive(
        headers,
        name,
        |header| header.name.as_str(),
        |header| header.value.as_str(),
    )
}

fn internal_date(value: &Option<String>) -> Option<SystemTime> {
    value
        .as_deref()
        .and_then(|value| value.parse::<u64>().ok())
        .map(|millis| UNIX_EPOCH + Duration::from_millis(millis))
}

fn references_from_header(value: Option<&str>) -> Vec<String> {
    value.map_or_else(Vec::new, |value| {
        value
            .split_whitespace()
            .map(|item| item.trim_matches(|ch| ch == '<' || ch == '>').to_string())
            .filter(|item| !item.is_empty())
            .collect()
    })
}

fn parse_address_list(value: Option<&str>) -> Vec<Address> {
    value.map_or_else(Vec::new, |value| {
        value.split(',').filter_map(parse_address).collect()
    })
}

fn parse_address(value: &str) -> Option<Address> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Some((name, rest)) = value.split_once('<')
        && let Some((address, _)) = rest.split_once('>')
    {
        let name = name.trim().trim_matches('"').trim();
        return Some(Address {
            name: (!name.is_empty()).then(|| name.to_string()),
            address: address.trim().to_string(),
        });
    }
    Some(Address::bare(value.to_string()))
}

#[derive(Debug, Clone, Default)]
struct MailDocument {
    identity: Option<IdentityId>,
    from: Option<Address>,
    to: Vec<Address>,
    cc: Vec<Address>,
    bcc: Vec<Address>,
    reply_to: Vec<Address>,
    subject: Option<String>,
    body_text: Option<String>,
    body_html: Option<String>,
    attachments_inline: Vec<AttachmentInline>,
    attachments_uploaded: Vec<AttachmentHandle>,
    in_reply_to: Option<String>,
    references: Vec<String>,
    thread_id: Option<String>,
}

impl MailDocument {
    fn from_send(request: SendRequest, default_address: &str) -> Self {
        let mut doc = Self {
            identity: request.identity,
            from: request.from,
            to: request.to,
            cc: request.cc,
            bcc: request.bcc,
            reply_to: request.reply_to,
            subject: request.subject,
            body_text: request.body_text,
            body_html: request.body_html,
            attachments_inline: request.attachments_inline,
            attachments_uploaded: request.attachments_uploaded,
            in_reply_to: request.in_reply_to,
            references: request.references,
            thread_id: None,
        };
        doc.ensure_from(default_address);
        doc
    }

    fn from_draft_patch(patch: DraftPatch, default_address: &str) -> Self {
        let mut doc = Self {
            identity: patch.identity,
            from: patch.from.flatten(),
            to: patch.to.unwrap_or_default(),
            cc: patch.cc.unwrap_or_default(),
            bcc: patch.bcc.unwrap_or_default(),
            reply_to: patch.reply_to.unwrap_or_default(),
            subject: patch.subject.flatten(),
            body_text: patch.body_text.flatten(),
            body_html: patch.body_html.flatten(),
            attachments_inline: patch.attachments_inline.unwrap_or_default(),
            attachments_uploaded: patch.attachments_uploaded.unwrap_or_default(),
            in_reply_to: patch.in_reply_to.flatten(),
            references: patch.references.unwrap_or_default(),
            thread_id: None,
        };
        doc.ensure_from(default_address);
        doc
    }

    fn apply_patch(&mut self, patch: DraftPatch) {
        if let Some(identity) = patch.identity {
            self.identity = Some(identity);
        }
        if let Some(from) = patch.from {
            self.from = from;
        }
        if let Some(to) = patch.to {
            self.to = to;
        }
        if let Some(cc) = patch.cc {
            self.cc = cc;
        }
        if let Some(bcc) = patch.bcc {
            self.bcc = bcc;
        }
        if let Some(reply_to) = patch.reply_to {
            self.reply_to = reply_to;
        }
        if let Some(subject) = patch.subject {
            self.subject = subject;
        }
        if let Some(body_text) = patch.body_text {
            self.body_text = body_text;
        }
        if let Some(body_html) = patch.body_html {
            self.body_html = body_html;
        }
        if let Some(attachments_inline) = patch.attachments_inline {
            self.attachments_inline = attachments_inline;
        }
        if let Some(attachments_uploaded) = patch.attachments_uploaded {
            self.attachments_uploaded = attachments_uploaded;
        }
        if let Some(in_reply_to) = patch.in_reply_to {
            self.in_reply_to = in_reply_to;
        }
        if let Some(references) = patch.references {
            self.references = references;
        }
    }

    fn ensure_from(&mut self, default_address: &str) {
        if self.from.is_none() {
            let address = self
                .identity
                .as_ref()
                .map_or_else(|| default_address.to_string(), |id| id.0.clone());
            self.from = Some(Address::bare(address));
        }
    }
}

fn render_message(
    doc: &MailDocument,
    default_address: &str,
    require_recipient: bool,
) -> Result<String, AccountError> {
    if require_recipient && doc.to.is_empty() && doc.cc.is_empty() && doc.bcc.is_empty() {
        return Err(other_error(
            bifrost_types::AccountOperation::Send,
            "gmail send requires at least one recipient",
        ));
    }
    if !doc.attachments_uploaded.is_empty() {
        return Err(unsupported(
            bifrost_types::AccountOperation::AttachmentUpload,
        ));
    }

    let mut headers = Vec::new();
    let from = doc
        .from
        .clone()
        .unwrap_or_else(|| Address::bare(default_address.to_string()));
    push_header(&mut headers, "From", &format_address(&from));
    push_address_header(&mut headers, "To", &doc.to);
    push_address_header(&mut headers, "Cc", &doc.cc);
    push_address_header(&mut headers, "Bcc", &doc.bcc);
    push_address_header(&mut headers, "Reply-To", &doc.reply_to);
    if let Some(subject) = &doc.subject {
        push_header(&mut headers, "Subject", &encode_header_value(subject));
    }
    if let Some(in_reply_to) = &doc.in_reply_to {
        push_header(&mut headers, "In-Reply-To", in_reply_to);
    }
    if !doc.references.is_empty() {
        push_header(&mut headers, "References", &doc.references.join(" "));
    }
    headers.push("MIME-Version: 1.0".to_string());

    let entity = render_entity(doc);
    let raw = format!("{}\r\n{}\r\n", headers.join("\r\n"), entity);
    Ok(URL_SAFE_NO_PAD.encode(raw.as_bytes()))
}

fn render_entity(doc: &MailDocument) -> String {
    if doc.attachments_inline.is_empty() {
        return render_body_entity(doc);
    }

    let boundary = boundary("mixed");
    let mut out = String::new();
    out.push_str(&format!(
        "Content-Type: multipart/mixed; boundary=\"{boundary}\"\r\n\r\n"
    ));
    push_part(&mut out, &boundary, &render_body_entity(doc));
    for attachment in &doc.attachments_inline {
        push_part(&mut out, &boundary, &render_attachment_entity(attachment));
    }
    out.push_str(&format!("--{boundary}--\r\n"));
    out
}

fn render_body_entity(doc: &MailDocument) -> String {
    match (&doc.body_text, &doc.body_html) {
        (Some(text), Some(html)) => {
            let boundary = boundary("alternative");
            let mut out = String::new();
            out.push_str(&format!(
                "Content-Type: multipart/alternative; boundary=\"{boundary}\"\r\n\r\n"
            ));
            push_part(&mut out, &boundary, &render_text_entity("text/plain", text));
            push_part(&mut out, &boundary, &render_text_entity("text/html", html));
            out.push_str(&format!("--{boundary}--\r\n"));
            out
        }
        (Some(text), None) => render_text_entity("text/plain", text),
        (None, Some(html)) => render_text_entity("text/html", html),
        (None, None) => render_text_entity("text/plain", ""),
    }
}

fn render_text_entity(mime: &str, body: &str) -> String {
    format!(
        "Content-Type: {mime}; charset=UTF-8\r\nContent-Transfer-Encoding: base64\r\n\r\n{}\r\n",
        wrap_base64(body.as_bytes())
    )
}

fn render_attachment_entity(attachment: &AttachmentInline) -> String {
    let disposition = if attachment.inline {
        "inline"
    } else {
        "attachment"
    };
    let filename = sanitize_header(&attachment.filename);
    format!(
        "Content-Type: {}; name=\"{}\"\r\nContent-Disposition: {disposition}; filename=\"{}\"\r\nContent-Transfer-Encoding: base64\r\n\r\n{}\r\n",
        sanitize_header(&attachment.mime),
        filename,
        filename,
        wrap_base64(&attachment.data)
    )
}

fn push_part(out: &mut String, boundary: &str, part: &str) {
    out.push_str(&format!("--{boundary}\r\n"));
    out.push_str(part);
}

fn push_header(headers: &mut Vec<String>, name: &str, value: &str) {
    headers.push(format!("{name}: {}", sanitize_header(value)));
}

fn push_address_header(headers: &mut Vec<String>, name: &str, addresses: &[Address]) {
    if !addresses.is_empty() {
        push_header(
            headers,
            name,
            &addresses
                .iter()
                .map(format_address)
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
}

fn format_address(address: &Address) -> String {
    let email = sanitize_header(&address.address);
    match address
        .name
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        Some(name) => format!("{} <{email}>", encode_phrase(name)),
        None => email,
    }
}

fn encode_phrase(value: &str) -> String {
    if value.is_ascii() {
        format!("\"{}\"", sanitize_header(value).replace('"', "\\\""))
    } else {
        encode_header_value(value)
    }
}

fn encode_header_value(value: &str) -> String {
    let sanitized = sanitize_header(value);
    if sanitized.is_ascii() {
        sanitized
    } else {
        format!("=?UTF-8?B?{}?=", STANDARD.encode(sanitized.as_bytes()))
    }
}

fn sanitize_header(value: &str) -> String {
    value.replace(['\r', '\n'], " ").trim().to_string()
}

fn wrap_base64(bytes: &[u8]) -> String {
    let encoded = STANDARD.encode(bytes);
    encoded
        .as_bytes()
        .chunks(76)
        .map(|chunk| std::str::from_utf8(chunk).unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\r\n")
}

fn boundary(kind: &str) -> String {
    static NEXT_BOUNDARY: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT_BOUNDARY.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("bifrost-gmail-{kind}-{nanos}-{sequence}")
}

fn system_time_to_millis_string(time: SystemTime) -> String {
    let millis = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();
    millis.to_string()
}

fn system_time_from_millis(value: &str) -> Option<SystemTime> {
    value
        .parse::<u64>()
        .ok()
        .map(|millis| UNIX_EPOCH + Duration::from_millis(millis))
}

fn is_archive_id(id: &str) -> bool {
    id.eq_ignore_ascii_case(ARCHIVE_ID)
}

fn non_negative_i64(value: i64) -> Option<u64> {
    u64::try_from(value).ok()
}

fn unsupported(op: bifrost_types::AccountOperation) -> AccountError {
    error::into_account_error(
        crate::error::Error::unsupported(op),
        error::GmailErrorContext::base(op),
    )
}

fn other_error(op: bifrost_types::AccountOperation, detail: impl Into<String>) -> AccountError {
    error::into_account_error(
        crate::error::Error::invalid_request(op, detail),
        error::GmailErrorContext::base(op),
    )
}

/// Translate a `crate::Error` into `AccountError` with the given
/// per-call-site context. Every PIM call site uses this so the
/// operation, scope, and resource are correct for the classification.
fn account_error_for(error: crate::Error, ctx: error::GmailErrorContext) -> AccountError {
    error::into_account_error(error, ctx)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn container_roles_map_gmail_system_labels() {
        let label = GmailLabel {
            id: LABEL_INBOX.to_string(),
            name: "Inbox".to_string(),
            label_type: Some("system".to_string()),
        };
        let container = container_from_label(&label);
        assert_eq!(container.role, Some(FolderRole::Inbox));
        assert_eq!(container.native_id, LABEL_INBOX);
        assert!(matches!(container.kind, ContainerKind::Label));
    }

    #[test]
    fn archive_container_is_explicit() {
        let container = archive_container();
        assert_eq!(container.id.0, ARCHIVE_ID);
        assert_eq!(container.role, Some(FolderRole::Archive));
    }

    #[test]
    fn read_state_uses_unread_label_inverse() {
        let (add, remove) = add_container_patch(&ContainerId(ARCHIVE_ID.to_string()));
        assert!(add.is_empty());
        assert_eq!(remove, vec![LABEL_INBOX.to_string()]);
    }

    #[test]
    fn search_query_combines_structured_and_provider_terms() {
        let mut request = SearchRequest::default();
        request.filter = Some(SearchFilter::From("ada@example.com".to_string()));
        request.provider_query = Some("larger:5M".to_string());
        let query = gmail_query(&request).expect("query").expect("some query");
        assert_eq!(query, "from:ada@example.com larger:5M");
    }

    #[test]
    fn date_query_uses_gmail_slash_date() {
        let time = UNIX_EPOCH + Duration::from_secs(86_400);
        assert_eq!(gmail_date(time), "1970/01/02");
    }
}
