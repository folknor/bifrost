use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bifrost_types::{
    AccountFuture, AccountStream, BlobCapabilities, BlobEncoding, BlobHandle, BlobId, Container,
    ContainerId, ContainerKind, Error, FolderRole, HydrationProjection, LabelId, Message,
    MutationTarget, ObjectId, Page, Provenance, QuotaInfo, SearchFilter, SearchRequest,
    ThreadHydration, ThreadId, VacationConfig,
};
use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::Mutex;

use crate::core::SetCreate;
use crate::core::query;
use crate::email::{
    BodyProperty, Email, EmailAddress as JmapEmailAddress, EmailBodyPart, EmailBodyValue, EmailGet,
    EmailId, EmailPatch, EmailSet, Property as EmailProperty,
};
use crate::email_submission::{Address as SubmissionAddress, EmailSubmissionSet, UndoStatus};
use crate::identity::{IdentityGet, IdentityId as JmapIdentityId, IdentitySet};
use crate::mailbox::{
    Mailbox, MailboxGet, MailboxId, MailboxSet, Property as MailboxProperty, Role,
};
use crate::quota::{Property as QuotaProperty, QuotaGet};
use crate::thread::{ThreadGet, ThreadId as JmapThreadId};
use crate::transport_reqwest::ReqwestTransport;
use crate::vacation_response::{VacationResponseGet, VacationResponseId, VacationResponseSet};

type MailAccount = crate::account::Account<ReqwestTransport>;

const SEEN_KEYWORD: &str = "$seen";
const DRAFT_KEYWORD: &str = "$draft";
const SUBMISSION_CREATE_ID: &str = "submit0";
const ATTACHMENT_HANDLE_PREFIX: &str = "jmap:";

pub(crate) fn add_to_container(
    mail: MailAccount,
    email_state: Arc<Mutex<Option<String>>>,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), Error>> {
    Box::pin(
        async move { patch_mailbox_membership(&mail, &email_state, target, container, true).await },
    )
}

pub(crate) fn remove_from_container(
    mail: MailAccount,
    email_state: Arc<Mutex<Option<String>>>,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), Error>> {
    Box::pin(async move {
        patch_mailbox_membership(&mail, &email_state, target, container, false).await
    })
}

pub(crate) fn set_keyword(
    mail: MailAccount,
    email_state: Arc<Mutex<Option<String>>>,
    target: MutationTarget,
    keyword: String,
    value: bool,
) -> AccountFuture<Result<(), Error>> {
    Box::pin(async move {
        let ids = resolve_target(&mail, target).await?;
        let keyword_for_set = keyword.clone();
        let ids_for_set = ids.clone();
        let mut response = send_email_set_with_retry(&mail, &email_state, move |state| {
            let mut set = EmailSet::new().if_in_state(state.to_string());
            for id in &ids_for_set {
                set.update(id.clone()).keyword(&keyword_for_set, value);
            }
            set
        })
        .await?;
        for id in &ids {
            response
                .updated(id)
                .map_err(super::error::to_account_error)?;
        }
        Ok(())
    })
}

pub(crate) fn set_is_read(
    mail: MailAccount,
    email_state: Arc<Mutex<Option<String>>>,
    target: MutationTarget,
    is_read: bool,
) -> AccountFuture<Result<(), Error>> {
    set_keyword(mail, email_state, target, SEEN_KEYWORD.to_string(), is_read)
}

pub(crate) fn send_message(
    mail: MailAccount,
    email_state: Arc<Mutex<Option<String>>>,
    request: bifrost_types::SendRequest,
) -> AccountFuture<Result<ObjectId, Error>> {
    Box::pin(async move {
        let identity = request.identity.clone();
        let envelope_from = request.from.clone();
        let envelope_recipients = request
            .to
            .iter()
            .chain(&request.cc)
            .chain(&request.bcc)
            .cloned()
            .collect::<Vec<_>>();
        let draft_mailbox = role_mailbox(&mail, FolderRole::Drafts).await?;
        let sent_mailbox = if request.save_to_sent == Some(false) {
            None
        } else {
            Some(role_mailbox(&mail, FolderRole::Sent).await?)
        };

        let create = build_email_create_from_send(&mail, request, draft_mailbox).await?;
        let mut email_set = EmailSet::new();
        let email_create_id = email_set.create_item(create);

        let mut submission_set = EmailSubmissionSet::new();
        {
            let submit = submission_set.create_with_id(SUBMISSION_CREATE_ID);
            submit.undo_status(UndoStatus::Final);
            if let Some(identity) = identity {
                submit.identity_id(JmapIdentityId::new(identity.0));
            }
            if let Some(from) = envelope_from.as_ref()
                && !envelope_recipients.is_empty()
            {
                submit.envelope(
                    submission_address_from_compose(from),
                    envelope_recipients
                        .iter()
                        .map(submission_address_from_compose),
                );
            }
        }

        if let Some(sent) = sent_mailbox {
            submission_set
                .on_success_update_email(SUBMISSION_CREATE_ID)
                .mailbox_ids([sent]);
        } else {
            submission_set = submission_set.on_success_destroy_email(SUBMISSION_CREATE_ID);
        }

        let mut batch = mail.build();
        let email_handle = batch
            .call(email_set)
            .map_err(super::error::to_account_error)?;
        let email_ref = email_handle.result_reference(format!("/created/{email_create_id}/id"));
        submission_set
            .create_with_id(SUBMISSION_CREATE_ID)
            .email_id_ref(email_ref);
        let submission_handle = batch
            .call(submission_set)
            .map_err(super::error::to_account_error)?;

        let mut response = batch.send().await.map_err(super::error::to_account_error)?;
        let mut email_response = response
            .get(&email_handle)
            .map_err(super::error::to_account_error)?;
        let mut submission_response = response
            .get(&submission_handle)
            .map_err(super::error::to_account_error)?;
        let mut email = email_response
            .created(&email_create_id)
            .map_err(super::error::to_account_error)?;
        submission_response
            .created(SUBMISSION_CREATE_ID)
            .map_err(super::error::to_account_error)?;

        if !email_response.new_state().is_empty() {
            advance_email_state(&email_state, None, email_response.new_state().to_string()).await;
        }

        Ok(ObjectId(email.take_id().into_string()))
    })
}

pub(crate) fn attachment_upload(
    mail: MailAccount,
    mut bytes: AccountStream<Result<Bytes, Error>>,
    mime: String,
) -> AccountFuture<Result<bifrost_types::AttachmentHandle, Error>> {
    Box::pin(async move {
        let mut data = Vec::new();
        while let Some(chunk) = bytes.next().await {
            let chunk = chunk?;
            data.extend_from_slice(&chunk);
        }
        let blob = mail
            .upload(data, Some(&mime))
            .await
            .map_err(super::error::to_account_error)?;
        Ok(bifrost_types::AttachmentHandle(encode_attachment_handle(
            blob.blob_id.as_str(),
            Some(&mime),
        )))
    })
}

pub(crate) fn draft_create(
    mail: MailAccount,
    email_state: Arc<Mutex<Option<String>>>,
    patch: bifrost_types::DraftPatch,
) -> AccountFuture<Result<bifrost_types::DraftHandle, Error>> {
    Box::pin(async move {
        let draft_mailbox = role_mailbox(&mail, FolderRole::Drafts).await?;
        let create = build_email_create_from_draft(&mail, patch, draft_mailbox).await?;
        let mut set = EmailSet::new();
        let create_id = set.create_item(create);
        let mut response = mail
            .call(set)
            .await
            .map_err(super::error::to_account_error)?;
        if !response.new_state().is_empty() {
            advance_email_state(&email_state, None, response.new_state().to_string()).await;
        }
        let mut email = response
            .created(&create_id)
            .map_err(super::error::to_account_error)?;
        Ok(bifrost_types::DraftHandle(email.take_id().into_string()))
    })
}

pub(crate) fn draft_update(
    mail: MailAccount,
    email_state: Arc<Mutex<Option<String>>>,
    draft: bifrost_types::DraftHandle,
    patch: bifrost_types::DraftPatch,
) -> AccountFuture<Result<(), Error>> {
    Box::pin(async move {
        let email_id = EmailId::new(draft.0);
        let mut email_patch = EmailPatch::default();
        apply_draft_patch_to_email_patch(&mail, &mut email_patch, patch).await?;
        let email_id_for_set = email_id.clone();
        let mut response = send_email_set_with_retry(&mail, &email_state, move |state| {
            let mut set = EmailSet::new().if_in_state(state.to_string());
            set.update_item(email_id_for_set.clone(), email_patch.clone());
            set
        })
        .await?;
        response
            .updated(&email_id)
            .map_err(super::error::to_account_error)?;
        Ok(())
    })
}

pub(crate) fn draft_discard(
    mail: MailAccount,
    email_state: Arc<Mutex<Option<String>>>,
    draft: bifrost_types::DraftHandle,
) -> AccountFuture<Result<(), Error>> {
    Box::pin(async move { destroy_emails(&mail, &email_state, [EmailId::new(draft.0)]).await })
}

pub(crate) fn draft_send(
    mail: MailAccount,
    email_state: Arc<Mutex<Option<String>>>,
    draft: bifrost_types::DraftHandle,
) -> AccountFuture<Result<ObjectId, Error>> {
    Box::pin(async move {
        let draft_id = EmailId::new(draft.0.clone());
        let sent_mailbox = role_mailbox(&mail, FolderRole::Sent).await?;
        let mut submission_set = EmailSubmissionSet::new();
        submission_set
            .create_with_id(SUBMISSION_CREATE_ID)
            .email_id(draft_id.clone())
            .undo_status(UndoStatus::Final);
        submission_set
            .on_success_update_email(SUBMISSION_CREATE_ID)
            .mailbox_ids([sent_mailbox]);
        let mut response = mail
            .call(submission_set)
            .await
            .map_err(super::error::to_account_error)?;
        response
            .created(SUBMISSION_CREATE_ID)
            .map_err(super::error::to_account_error)?;
        let fresh = super::mutation::probe_email_state(&mail)
            .await
            .map_err(super::error::to_account_error)?;
        set_email_state(&email_state, fresh).await;
        Ok(ObjectId(draft_id.into_string()))
    })
}

pub(crate) fn search(
    mail: MailAccount,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ThreadId>, Error>> {
    Box::pin(async move {
        let page = search_email_ids(&mail, request, true).await?;
        if page.items.is_empty() {
            return Ok(Page {
                items: Vec::new(),
                next_cursor: page.next_cursor,
                estimated_total: page.estimated_total,
            });
        }
        let response = mail
            .call(
                EmailGet::new()
                    .ids(page.items)
                    .properties([EmailProperty::Id, EmailProperty::ThreadId]),
            )
            .await
            .map_err(super::error::to_account_error)?;
        let mut items = Vec::new();
        for email in response.into_list() {
            if let Some(thread) = email.thread_id() {
                items.push(ThreadId(thread.to_string()));
            }
        }
        Ok(Page {
            items,
            next_cursor: page.next_cursor,
            estimated_total: page.estimated_total,
        })
    })
}

pub(crate) fn search_messages(
    mail: MailAccount,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ObjectId>, Error>> {
    Box::pin(async move {
        let page = search_email_ids(&mail, request, false).await?;
        Ok(Page {
            items: page
                .items
                .into_iter()
                .map(|id| ObjectId(id.into_string()))
                .collect(),
            next_cursor: page.next_cursor,
            estimated_total: page.estimated_total,
        })
    })
}

pub(crate) fn containers_list(mail: MailAccount) -> AccountFuture<Result<Vec<Container>, Error>> {
    Box::pin(async move { fetch_containers(&mail).await })
}

pub(crate) fn container_create(
    mail: MailAccount,
    mailbox_state: Arc<Mutex<Option<String>>>,
    kind: ContainerKind,
    name: String,
    parent: Option<ContainerId>,
) -> AccountFuture<Result<ContainerId, Error>> {
    Box::pin(async move {
        if !matches!(kind, ContainerKind::Folder) {
            return Err(Error::Unsupported);
        }
        let mut set = MailboxSet::new();
        let mut create = crate::mailbox::MailboxCreate::new(None);
        create.name(name);
        create.parent_id(parent.map(|id| MailboxId::new(id.0)));
        let create_id = set.create_item(create);
        let mut response = mail
            .call(set)
            .await
            .map_err(super::error::to_account_error)?;
        if !response.new_state().is_empty() {
            set_mailbox_state(&mailbox_state, response.new_state().to_string()).await;
        }
        let mut mailbox = response
            .created(&create_id)
            .map_err(super::error::to_account_error)?;
        Ok(ContainerId(mailbox.take_id().into_string()))
    })
}

pub(crate) fn container_rename(
    mail: MailAccount,
    mailbox_state: Arc<Mutex<Option<String>>>,
    container: ContainerId,
    name: String,
) -> AccountFuture<Result<(), Error>> {
    Box::pin(async move {
        let mailbox = MailboxId::new(container.0);
        let mut set = MailboxSet::new();
        set.update(mailbox).name(name);
        let response = mail
            .call(set)
            .await
            .map_err(super::error::to_account_error)?;
        response
            .unwrap_update_errors()
            .map_err(super::error::to_account_error)?;
        if !response.new_state().is_empty() {
            set_mailbox_state(&mailbox_state, response.new_state().to_string()).await;
        }
        Ok(())
    })
}

pub(crate) fn container_move(
    mail: MailAccount,
    mailbox_state: Arc<Mutex<Option<String>>>,
    container: ContainerId,
    new_parent: Option<ContainerId>,
) -> AccountFuture<Result<(), Error>> {
    Box::pin(async move {
        let mailbox = MailboxId::new(container.0);
        let mut set = MailboxSet::new();
        set.update(mailbox)
            .parent_id(new_parent.map(|id| MailboxId::new(id.0)));
        let response = mail
            .call(set)
            .await
            .map_err(super::error::to_account_error)?;
        response
            .unwrap_update_errors()
            .map_err(super::error::to_account_error)?;
        if !response.new_state().is_empty() {
            set_mailbox_state(&mailbox_state, response.new_state().to_string()).await;
        }
        Ok(())
    })
}

pub(crate) fn container_delete(
    mail: MailAccount,
    mailbox_state: Arc<Mutex<Option<String>>>,
    container: ContainerId,
) -> AccountFuture<Result<(), Error>> {
    Box::pin(async move {
        let mailbox = MailboxId::new(container.0);
        let mut response = mail
            .call(
                MailboxSet::new()
                    .destroy([mailbox.clone()])
                    .on_destroy_remove_emails(false),
            )
            .await
            .map_err(super::error::to_account_error)?;
        response
            .destroyed(&mailbox)
            .map_err(super::error::to_account_error)?;
        if !response.new_state().is_empty() {
            set_mailbox_state(&mailbox_state, response.new_state().to_string()).await;
        }
        Ok(())
    })
}

pub(crate) fn identities_list(
    submission: Option<MailAccount>,
) -> AccountFuture<Result<Vec<bifrost_types::Identity>, Error>> {
    Box::pin(async move {
        let Some(account) = submission else {
            return Err(Error::Unsupported);
        };
        let response = account
            .call(IdentityGet::new().properties([
                crate::identity::Property::Id,
                crate::identity::Property::Name,
                crate::identity::Property::Email,
                crate::identity::Property::ReplyTo,
                crate::identity::Property::TextSignature,
                crate::identity::Property::HtmlSignature,
            ]))
            .await
            .map_err(super::error::to_account_error)?;
        let mut identities = Vec::new();
        for (idx, mut identity) in response.into_list().into_iter().enumerate() {
            let id = identity.take_id().into_string();
            let reply_to = identity
                .reply_to()
                .and_then(|values| values.first())
                .map(address_from_jmap);
            identities.push(bifrost_types::Identity {
                id: bifrost_types::IdentityId(id),
                name: identity.name().unwrap_or("").to_string(),
                address: identity.email().unwrap_or("").to_string(),
                signature_text: identity.text_signature().map(str::to_string),
                signature_html: identity.html_signature().map(str::to_string),
                reply_to,
                is_default: idx == 0,
            });
        }
        Ok(identities)
    })
}

pub(crate) fn identity_update(
    submission: Option<MailAccount>,
    identity: bifrost_types::IdentityId,
    patch: bifrost_types::IdentityPatch,
) -> AccountFuture<Result<(), Error>> {
    Box::pin(async move {
        let Some(account) = submission else {
            return Err(Error::Unsupported);
        };
        if patch.is_default.is_some() {
            return Err(Error::Unsupported);
        }
        let mut set = IdentitySet::new();
        let item = set.update(JmapIdentityId::new(identity.0));
        if let Some(name) = patch.name {
            item.name(name);
        }
        if let Some(signature) = patch.signature_text {
            if let Some(value) = signature {
                item.text_signature(value);
            } else {
                item.text_signature("");
            }
        }
        if let Some(signature) = patch.signature_html {
            if let Some(value) = signature {
                item.html_signature(value);
            } else {
                item.html_signature("");
            }
        }
        if let Some(reply_to) = patch.reply_to {
            let values = reply_to.into_iter().map(address_to_jmap);
            item.reply_to(Some(values));
        }
        let response = account
            .call(set)
            .await
            .map_err(super::error::to_account_error)?;
        response
            .unwrap_update_errors()
            .map_err(super::error::to_account_error)
    })
}

pub(crate) fn vacation_get(
    vacation: Option<MailAccount>,
) -> AccountFuture<Result<Option<VacationConfig>, Error>> {
    Box::pin(async move {
        let Some(account) = vacation else {
            return Err(Error::Unsupported);
        };
        let mut response = account
            .call(VacationResponseGet::new().ids([VacationResponseId::new("singleton")]))
            .await
            .map_err(super::error::to_account_error)?;
        let Some(vacation) = response.pop() else {
            return Ok(None);
        };
        Ok(Some(VacationConfig {
            is_enabled: vacation.is_enabled(),
            subject: vacation.subject().map(str::to_string),
            body_text: vacation.text_body().map(str::to_string),
            body_html: vacation.html_body().map(str::to_string),
            starts_at: vacation.from_date().and_then(unix_to_system_time),
            ends_at: vacation.to_date().and_then(unix_to_system_time),
        }))
    })
}

pub(crate) fn vacation_set(
    vacation: Option<MailAccount>,
    config: VacationConfig,
) -> AccountFuture<Result<(), Error>> {
    Box::pin(async move {
        let Some(account) = vacation else {
            return Err(Error::Unsupported);
        };
        let mut set = VacationResponseSet::new();
        let patch = set.update(VacationResponseId::new("singleton"));
        patch.is_enabled(config.is_enabled);
        if let Some(subject) = config.subject {
            patch.subject(Some(subject));
        } else {
            patch.null_property("subject");
        }
        if let Some(body_text) = config.body_text {
            patch.text_body(Some(body_text));
        } else {
            patch.null_property("textBody");
        }
        if let Some(body_html) = config.body_html {
            patch.html_body(Some(body_html));
        } else {
            patch.null_property("htmlBody");
        }
        if let Some(starts_at) = config.starts_at {
            patch.from_date(Some(system_time_to_unix(starts_at)));
        } else {
            patch.null_property("fromDate");
        }
        if let Some(ends_at) = config.ends_at {
            patch.to_date(Some(system_time_to_unix(ends_at)));
        } else {
            patch.null_property("toDate");
        }
        let response = account
            .call(set)
            .await
            .map_err(super::error::to_account_error)?;
        response
            .unwrap_update_errors()
            .map_err(super::error::to_account_error)
    })
}

pub(crate) fn quota_get(
    quota: Option<MailAccount>,
) -> AccountFuture<Result<Option<QuotaInfo>, Error>> {
    Box::pin(async move {
        let Some(account) = quota else {
            return Err(Error::Unsupported);
        };
        let response = account
            .call(QuotaGet::new().properties([
                QuotaProperty::ResourceType,
                QuotaProperty::Used,
                QuotaProperty::HardLimit,
            ]))
            .await
            .map_err(super::error::to_account_error)?;
        let mut fallback = None;
        for quota in response.into_list() {
            let info = QuotaInfo {
                used_bytes: quota.used().unwrap_or(0),
                total_bytes: quota.hard_limit(),
            };
            if matches!(quota.resource_type(), Some("octets" | "storage")) {
                return Ok(Some(info));
            }
            fallback.get_or_insert(info);
        }
        Ok(fallback)
    })
}

pub(crate) fn thread_hydrate(
    mail: MailAccount,
    thread: ThreadId,
) -> AccountFuture<Result<ThreadHydration, Error>> {
    Box::pin(async move {
        let jmap_thread_id = JmapThreadId::new(thread.0.clone());
        let mut thread_response = mail
            .call(ThreadGet::new().ids([jmap_thread_id.clone()]).properties([
                crate::thread::Property::Id,
                crate::thread::Property::EmailIds,
            ]))
            .await
            .map_err(super::error::to_account_error)?;
        let thread_object = thread_response
            .pop()
            .ok_or_else(|| Error::Other(format!("thread {} was not found", thread.0)))?;
        let order = thread_object.email_ids().to_vec();
        if order.is_empty() {
            return Ok(ThreadHydration {
                id: thread,
                messages: Vec::new(),
            });
        }
        let response = mail
            .call(
                EmailGet::new()
                    .ids(order.clone())
                    .properties(message_properties(HydrationProjection::Full))
                    .fetch_text_body_values(true)
                    .fetch_html_body_values(true)
                    .body_properties(body_properties()),
            )
            .await
            .map_err(super::error::to_account_error)?;
        let mut by_id = response
            .into_list()
            .into_iter()
            .filter_map(|email| email.id().map(ToString::to_string).map(|id| (id, email)))
            .collect::<HashMap<_, _>>();
        let mut messages = Vec::new();
        for id in order {
            if let Some(email) = by_id.remove(id.as_str()) {
                messages.push(email_to_message(email, HydrationProjection::Full));
            }
        }
        Ok(ThreadHydration {
            id: thread,
            messages,
        })
    })
}

pub(crate) fn message_hydrate(
    mail: MailAccount,
    message: ObjectId,
    projection: HydrationProjection,
) -> AccountFuture<Result<Message, Error>> {
    Box::pin(async move {
        let mut get = EmailGet::new()
            .ids([EmailId::new(message.0.clone())])
            .properties(message_properties(projection));
        if matches!(
            projection,
            HydrationProjection::Preview(_)
                | HydrationProjection::Full
                | HydrationProjection::FullWithBlobs
        ) {
            get = get
                .fetch_text_body_values(true)
                .fetch_html_body_values(true)
                .body_properties(body_properties());
        }
        if let HydrationProjection::Preview(max) = projection {
            get = get.max_body_value_bytes(max);
        }
        let mut response = mail
            .call(get)
            .await
            .map_err(super::error::to_account_error)?;
        let email = response
            .pop()
            .ok_or_else(|| Error::Other(format!("message {} was not found", message.0)))?;
        Ok(email_to_message(email, projection))
    })
}

pub(crate) fn move_thread(
    mail: MailAccount,
    email_state: Arc<Mutex<Option<String>>>,
    thread: ThreadId,
    target: ContainerId,
    source: Option<ContainerId>,
) -> AccountFuture<Result<(), Error>> {
    Box::pin(async move {
        patch_mailbox_membership(
            &mail,
            &email_state,
            MutationTarget::Thread(thread.clone()),
            target,
            true,
        )
        .await?;
        if let Some(source) = source {
            patch_mailbox_membership(
                &mail,
                &email_state,
                MutationTarget::Thread(thread),
                source,
                false,
            )
            .await?;
        }
        Ok(())
    })
}

pub(crate) fn delete_thread(
    mail: MailAccount,
    email_state: Arc<Mutex<Option<String>>>,
    thread: ThreadId,
    current: Option<ContainerId>,
) -> AccountFuture<Result<(), Error>> {
    Box::pin(async move {
        let trash = role_mailbox(&mail, FolderRole::Trash).await?;
        // JMAP mailbox ids are server-issued opaque strings and RFC
        // 8620 leaves case-sensitivity to the server, but two
        // mailboxes that differ only in case is a pathological shape
        // we are happy to misclassify in favor of defensiveness:
        // case-insensitive compare here means a caller who normalized
        // the id elsewhere in their stack still resolves a "thread is
        // already in Trash" branch correctly.
        let already_in_trash = current
            .as_ref()
            .is_some_and(|id| id.0.eq_ignore_ascii_case(trash.as_str()));
        if already_in_trash {
            let ids = resolve_target(&mail, MutationTarget::Thread(thread)).await?;
            destroy_emails(&mail, &email_state, ids).await
        } else {
            patch_mailbox_membership(
                &mail,
                &email_state,
                MutationTarget::Thread(thread.clone()),
                ContainerId(trash.into_string()),
                true,
            )
            .await?;
            if let Some(source) = current {
                patch_mailbox_membership(
                    &mail,
                    &email_state,
                    MutationTarget::Thread(thread),
                    source,
                    false,
                )
                .await?;
            }
            Ok(())
        }
    })
}

async fn patch_mailbox_membership(
    mail: &MailAccount,
    email_state: &Arc<Mutex<Option<String>>>,
    target: MutationTarget,
    container: ContainerId,
    value: bool,
) -> Result<(), Error> {
    let ids = resolve_target(mail, target).await?;
    let mailbox = MailboxId::new(container.0);
    let ids_for_set = ids.clone();
    let mut response = send_email_set_with_retry(mail, email_state, move |state| {
        let mut set = EmailSet::new().if_in_state(state.to_string());
        for id in &ids_for_set {
            set.update(id.clone()).mailbox_id(&mailbox, value);
        }
        set
    })
    .await?;
    for id in &ids {
        response
            .updated(id)
            .map_err(super::error::to_account_error)?;
    }
    Ok(())
}

async fn resolve_target(mail: &MailAccount, target: MutationTarget) -> Result<Vec<EmailId>, Error> {
    match target {
        MutationTarget::Message(id) => Ok(vec![EmailId::new(id.0)]),
        MutationTarget::Thread(thread) => {
            let mut response = mail
                .call(
                    ThreadGet::new()
                        .ids([JmapThreadId::new(thread.0.clone())])
                        .properties([crate::thread::Property::EmailIds]),
                )
                .await
                .map_err(super::error::to_account_error)?;
            let thread = response
                .pop()
                .ok_or_else(|| Error::Other(format!("thread {} was not found", thread.0)))?;
            Ok(thread.email_ids().to_vec())
        }
        _ => Err(Error::Unsupported),
    }
}

async fn destroy_emails(
    mail: &MailAccount,
    email_state: &Arc<Mutex<Option<String>>>,
    ids: impl IntoIterator<Item = EmailId> + Clone + Send + 'static,
) -> Result<(), Error> {
    let ids_vec = ids.into_iter().collect::<Vec<_>>();
    if ids_vec.is_empty() {
        return Ok(());
    }
    let ids_for_set = ids_vec.clone();
    let mut response = send_email_set_with_retry(mail, email_state, move |state| {
        EmailSet::new()
            .if_in_state(state.to_string())
            .destroy(ids_for_set.clone())
    })
    .await?;
    for id in &ids_vec {
        response
            .destroyed(id)
            .map_err(super::error::to_account_error)?;
    }
    Ok(())
}

async fn send_email_set_with_retry<F>(
    mail: &MailAccount,
    email_state: &Arc<Mutex<Option<String>>>,
    mut make_set: F,
) -> Result<crate::core::set::SetResponse<Email>, Error>
where
    F: FnMut(&str) -> EmailSet,
{
    let mut state = current_or_probe_email_state(mail, email_state).await?;
    let response = match mail.call(make_set(&state)).await {
        Ok(response) => response,
        Err(err) => {
            if !super::error::is_state_mismatch(&err) {
                return Err(super::error::to_account_error(err));
            }
            let fresh = super::mutation::probe_email_state(mail)
                .await
                .map_err(super::error::to_account_error)?;
            set_email_state(email_state, fresh.clone()).await;
            state = fresh;
            mail.call(make_set(&state))
                .await
                .map_err(super::error::to_account_error)?
        }
    };
    if !response.new_state().is_empty() {
        advance_email_state(email_state, Some(&state), response.new_state().to_string()).await;
    }
    Ok(response)
}

async fn current_or_probe_email_state(
    mail: &MailAccount,
    email_state: &Arc<Mutex<Option<String>>>,
) -> Result<String, Error> {
    let cached = {
        let guard = email_state.lock().await;
        guard.clone()
    };
    match cached {
        Some(state) => Ok(state),
        None => {
            let state = super::mutation::probe_email_state(mail)
                .await
                .map_err(super::error::to_account_error)?;
            let mut guard = email_state.lock().await;
            match guard.clone() {
                Some(existing) => Ok(existing),
                None => {
                    *guard = Some(state.clone());
                    Ok(state)
                }
            }
        }
    }
}

async fn set_email_state(email_state: &Arc<Mutex<Option<String>>>, state: String) {
    let mut guard = email_state.lock().await;
    *guard = Some(state);
}

async fn advance_email_state(
    email_state: &Arc<Mutex<Option<String>>>,
    expected: Option<&str>,
    state: String,
) {
    let mut guard = email_state.lock().await;
    match (guard.as_deref(), expected) {
        (Some(current), Some(expected)) if current != expected => {}
        _ => *guard = Some(state),
    }
}

async fn set_mailbox_state(mailbox_state: &Arc<Mutex<Option<String>>>, state: String) {
    let mut guard = mailbox_state.lock().await;
    *guard = Some(state);
}

async fn role_mailbox(mail: &MailAccount, role: FolderRole) -> Result<MailboxId, Error> {
    fetch_mailboxes(mail)
        .await?
        .into_iter()
        .find(|mailbox| map_role(mailbox.role()) == Some(role))
        .and_then(|mut mailbox| {
            let id = mailbox.take_id();
            if id.as_str().is_empty() {
                None
            } else {
                Some(id)
            }
        })
        .ok_or(Error::Unsupported)
}

async fn fetch_containers(mail: &MailAccount) -> Result<Vec<Container>, Error> {
    Ok(fetch_mailboxes(mail)
        .await?
        .into_iter()
        .filter_map(container_from_mailbox)
        .collect())
}

async fn fetch_mailboxes(mail: &MailAccount) -> Result<Vec<Mailbox>, Error> {
    Ok(mail
        .call(MailboxGet::new().properties([
            MailboxProperty::Id,
            MailboxProperty::Name,
            MailboxProperty::ParentId,
            MailboxProperty::Role,
        ]))
        .await
        .map_err(super::error::to_account_error)?
        .into_list())
}

fn container_from_mailbox(mut mailbox: Mailbox) -> Option<Container> {
    let id = mailbox.take_id();
    if id.as_str().is_empty() {
        return None;
    }
    let native = id.into_string();
    let parent = mailbox
        .parent_id()
        .map(|parent| ContainerId(parent.to_string()));
    Some(Container {
        id: ContainerId(native.clone()),
        kind: ContainerKind::Folder,
        role: map_role(mailbox.role()),
        provenance: Provenance {
            provider: bifrost_types::ProtocolKind::Jmap,
            kind: ContainerKind::Folder,
            native: native.clone(),
        },
        native_id: native,
        name: mailbox.name().unwrap_or("").to_string(),
        parent,
    })
}

fn map_role(role: Option<&Role>) -> Option<FolderRole> {
    match role {
        Some(Role::Inbox) => Some(FolderRole::Inbox),
        Some(Role::Sent) => Some(FolderRole::Sent),
        Some(Role::Drafts) => Some(FolderRole::Drafts),
        Some(Role::Archive) => Some(FolderRole::Archive),
        Some(Role::Trash) => Some(FolderRole::Trash),
        Some(Role::Junk) => Some(FolderRole::Spam),
        _ => None,
    }
}

async fn search_email_ids(
    mail: &MailAccount,
    request: SearchRequest,
    collapse_threads: bool,
) -> Result<Page<EmailId>, Error> {
    let position = decode_position(request.page_cursor.as_deref())?;
    let limit = request.limit.unwrap_or(50).max(1);
    let mut query_request = crate::email::EmailQuery::new()
        .collapse_threads(collapse_threads)
        .position(position)
        .limit(usize::try_from(limit).unwrap_or(usize::MAX))
        .calculate_total(true);
    if let Some(filter) = build_search_filter(request.filter, request.provider_query) {
        query_request = query_request.filter(filter);
    }
    let response = mail
        .call(query_request)
        .await
        .map_err(super::error::to_account_error)?;
    let total = response.total().and_then(|v| u64::try_from(v).ok());
    let ids = response.into_ids();
    let next_cursor = if ids.len() == usize::try_from(limit).unwrap_or(usize::MAX) {
        let next = position
            .checked_add(i32::try_from(ids.len()).map_err(|_| Error::SchemaIncompatible)?)
            .ok_or(Error::SchemaIncompatible)?;
        Some(next.to_string().into_bytes())
    } else {
        None
    };
    Ok(Page {
        items: ids,
        next_cursor,
        estimated_total: total,
    })
}

fn decode_position(cursor: Option<&[u8]>) -> Result<i32, Error> {
    match cursor {
        None => Ok(0),
        Some(bytes) => {
            let text = std::str::from_utf8(bytes).map_err(|_| Error::SchemaIncompatible)?;
            text.parse::<i32>().map_err(|_| Error::SchemaIncompatible)
        }
    }
}

fn build_search_filter(
    filter: Option<SearchFilter>,
    provider_query: Option<String>,
) -> Option<query::Filter<crate::email::query::Filter>> {
    let mut filters = Vec::new();
    if let Some(filter) = filter {
        filters.push(search_filter_to_jmap(filter));
    }
    if let Some(provider_query) = provider_query
        && !provider_query.is_empty()
    {
        filters.push(crate::email::query::Filter::text(provider_query).into());
    }
    match filters.len() {
        0 => None,
        1 => filters.pop(),
        _ => Some(query::Filter::and(filters)),
    }
}

fn search_filter_to_jmap(filter: SearchFilter) -> query::Filter<crate::email::query::Filter> {
    match filter {
        SearchFilter::From(value) => crate::email::query::Filter::from(value).into(),
        SearchFilter::To(value) => query::Filter::or([
            crate::email::query::Filter::to(value.clone()),
            crate::email::query::Filter::cc(value.clone()),
            crate::email::query::Filter::bcc(value),
        ]),
        SearchFilter::Subject(value) => crate::email::query::Filter::subject(value).into(),
        SearchFilter::Body(value) => crate::email::query::Filter::body(value).into(),
        SearchFilter::Has(value) if value.is_empty() => {
            crate::email::query::Filter::has_attachment(true).into()
        }
        SearchFilter::Has(value) => {
            let filters: Vec<query::Filter<crate::email::query::Filter>> = vec![
                crate::email::query::Filter::has_attachment(true).into(),
                crate::email::query::Filter::text(value).into(),
            ];
            query::Filter::and(filters)
        }
        SearchFilter::In(container) => {
            crate::email::query::Filter::in_mailbox(MailboxId::new(container.0)).into()
        }
        SearchFilter::Labeled(LabelId(label)) => {
            crate::email::query::Filter::has_keyword(label).into()
        }
        SearchFilter::DateRange { after, before } => {
            let mut filters: Vec<query::Filter<crate::email::query::Filter>> = Vec::new();
            if let Some(after) = after {
                filters.push(
                    crate::email::query::Filter::sent_after(system_time_to_unix(after)).into(),
                );
            }
            if let Some(before) = before {
                filters.push(
                    crate::email::query::Filter::sent_before(system_time_to_unix(before)).into(),
                );
            }
            query::Filter::and(filters)
        }
        SearchFilter::And(filters) => {
            query::Filter::and(filters.into_iter().map(search_filter_to_jmap))
        }
        SearchFilter::Or(filters) => {
            query::Filter::or(filters.into_iter().map(search_filter_to_jmap))
        }
        SearchFilter::Not(filter) => query::Filter::not([search_filter_to_jmap(*filter)]),
        _ => query::Filter::and(Vec::<query::Filter<crate::email::query::Filter>>::new()),
    }
}

async fn build_email_create_from_send(
    mail: &MailAccount,
    request: bifrost_types::SendRequest,
    mailbox: MailboxId,
) -> Result<crate::email::EmailCreate, Error> {
    let mut patch = bifrost_types::DraftPatch::default();
    patch.identity = request.identity;
    patch.from = Some(request.from);
    patch.to = Some(request.to);
    patch.cc = Some(request.cc);
    patch.bcc = Some(request.bcc);
    patch.reply_to = Some(request.reply_to);
    patch.subject = Some(request.subject);
    patch.body_text = Some(request.body_text);
    patch.body_html = Some(request.body_html);
    patch.attachments_inline = Some(request.attachments_inline);
    patch.attachments_uploaded = Some(request.attachments_uploaded);
    patch.in_reply_to = Some(request.in_reply_to);
    patch.references = Some(request.references);
    build_email_create_from_draft(mail, patch, mailbox).await
}

async fn build_email_create_from_draft(
    mail: &MailAccount,
    patch: bifrost_types::DraftPatch,
    mailbox: MailboxId,
) -> Result<crate::email::EmailCreate, Error> {
    let mut create = crate::email::EmailCreate::new(None);
    let body_patch = patch.clone();
    create.mailbox_ids([mailbox]);
    create.keywords([DRAFT_KEYWORD]);
    if let Some(from) = patch.from.flatten() {
        create.from([address_to_jmap(from)]);
    }
    if let Some(to) = patch.to {
        create.to(to.into_iter().map(address_to_jmap));
    }
    if let Some(cc) = patch.cc {
        create.cc(cc.into_iter().map(address_to_jmap));
    }
    if let Some(bcc) = patch.bcc {
        create.bcc(bcc.into_iter().map(address_to_jmap));
    }
    if let Some(reply_to) = patch.reply_to {
        create.reply_to(reply_to.into_iter().map(address_to_jmap));
    }
    if let Some(Some(subject)) = patch.subject {
        create.subject(subject);
    }
    if let Some(Some(in_reply_to)) = patch.in_reply_to {
        create.in_reply_to([in_reply_to]);
    }
    if let Some(references) = patch.references {
        create.references(references);
    }
    apply_body_to_create(mail, &mut create, body_patch).await?;
    Ok(create)
}

async fn apply_body_to_create(
    mail: &MailAccount,
    create: &mut crate::email::EmailCreate,
    patch: bifrost_types::DraftPatch,
) -> Result<(), Error> {
    let body = build_body(
        mail,
        patch.body_text.flatten(),
        patch.body_html.flatten(),
        patch.attachments_inline.unwrap_or_default(),
        patch.attachments_uploaded.unwrap_or_default(),
    )
    .await?;
    if let Some(structure) = body.body_structure {
        create.body_structure(structure);
    }
    for (id, value) in body.body_values {
        create.body_value(id, value);
    }
    for part in body.text_body {
        create.text_body(part);
    }
    for part in body.html_body {
        create.html_body(part);
    }
    for part in body.attachments {
        create.attachment(part);
    }
    Ok(())
}

async fn apply_draft_patch_to_email_patch(
    mail: &MailAccount,
    email_patch: &mut EmailPatch,
    patch: bifrost_types::DraftPatch,
) -> Result<(), Error> {
    if let Some(from) = patch.from {
        match from {
            Some(value) => {
                email_patch
                    .raw_property("from", &vec![address_to_jmap(value)])
                    .map_err(crate::Error::from)
                    .map_err(super::error::to_account_error)?;
            }
            None => {
                email_patch.null_property("from");
            }
        }
    }
    if let Some(to) = patch.to {
        let values = to.into_iter().map(address_to_jmap).collect::<Vec<_>>();
        email_patch
            .raw_property("to", &values)
            .map_err(crate::Error::from)
            .map_err(super::error::to_account_error)?;
    }
    if let Some(cc) = patch.cc {
        let values = cc.into_iter().map(address_to_jmap).collect::<Vec<_>>();
        email_patch
            .raw_property("cc", &values)
            .map_err(crate::Error::from)
            .map_err(super::error::to_account_error)?;
    }
    if let Some(bcc) = patch.bcc {
        let values = bcc.into_iter().map(address_to_jmap).collect::<Vec<_>>();
        email_patch
            .raw_property("bcc", &values)
            .map_err(crate::Error::from)
            .map_err(super::error::to_account_error)?;
    }
    if let Some(reply_to) = patch.reply_to {
        let values = reply_to
            .into_iter()
            .map(address_to_jmap)
            .collect::<Vec<_>>();
        email_patch
            .raw_property("replyTo", &values)
            .map_err(crate::Error::from)
            .map_err(super::error::to_account_error)?;
    }
    if let Some(subject) = patch.subject {
        if let Some(subject) = subject {
            email_patch.subject(subject);
        } else {
            email_patch.null_property("subject");
        }
    }
    if let Some(in_reply_to) = patch.in_reply_to {
        if let Some(in_reply_to) = in_reply_to {
            email_patch
                .raw_property("inReplyTo", &vec![in_reply_to])
                .map_err(crate::Error::from)
                .map_err(super::error::to_account_error)?;
        } else {
            email_patch.null_property("inReplyTo");
        }
    }
    if let Some(references) = patch.references {
        email_patch
            .raw_property("references", &references)
            .map_err(crate::Error::from)
            .map_err(super::error::to_account_error)?;
    }
    if patch.body_text.is_some()
        || patch.body_html.is_some()
        || patch.attachments_inline.is_some()
        || patch.attachments_uploaded.is_some()
    {
        let body = build_body(
            mail,
            patch.body_text.flatten(),
            patch.body_html.flatten(),
            patch.attachments_inline.unwrap_or_default(),
            patch.attachments_uploaded.unwrap_or_default(),
        )
        .await?;
        if let Some(structure) = body.body_structure {
            email_patch
                .raw_property("bodyStructure", &structure)
                .map_err(crate::Error::from)
                .map_err(super::error::to_account_error)?;
        }
        email_patch
            .raw_property("bodyValues", &body.body_values)
            .map_err(crate::Error::from)
            .map_err(super::error::to_account_error)?;
        email_patch
            .raw_property("textBody", &body.text_body)
            .map_err(crate::Error::from)
            .map_err(super::error::to_account_error)?;
        email_patch
            .raw_property("htmlBody", &body.html_body)
            .map_err(crate::Error::from)
            .map_err(super::error::to_account_error)?;
        email_patch
            .raw_property("attachments", &body.attachments)
            .map_err(crate::Error::from)
            .map_err(super::error::to_account_error)?;
    }
    Ok(())
}

struct BuiltBody {
    body_structure: Option<EmailBodyPart>,
    body_values: HashMap<String, EmailBodyValue>,
    text_body: Vec<EmailBodyPart>,
    html_body: Vec<EmailBodyPart>,
    attachments: Vec<EmailBodyPart>,
}

async fn build_body(
    mail: &MailAccount,
    text: Option<String>,
    html: Option<String>,
    inline: Vec<bifrost_types::AttachmentInline>,
    uploaded: Vec<bifrost_types::AttachmentHandle>,
) -> Result<BuiltBody, Error> {
    let mut body_values = HashMap::new();
    let mut text_body = Vec::new();
    let mut html_body = Vec::new();
    let mut body_parts = Vec::new();

    if let Some(text) = text {
        let part = EmailBodyPart::new()
            .with_part_id("text")
            .with_content_type("text/plain");
        body_values.insert("text".to_string(), EmailBodyValue::from(text));
        text_body.push(part.clone());
        body_parts.push(part);
    }
    if let Some(html) = html {
        let part = EmailBodyPart::new()
            .with_part_id("html")
            .with_content_type("text/html");
        body_values.insert("html".to_string(), EmailBodyValue::from(html));
        html_body.push(part.clone());
        body_parts.push(part);
    }
    if body_parts.is_empty() {
        let part = EmailBodyPart::new()
            .with_part_id("text")
            .with_content_type("text/plain");
        body_values.insert("text".to_string(), EmailBodyValue::from(""));
        text_body.push(part.clone());
        body_parts.push(part);
    }

    let mut attachments = Vec::new();
    for attachment in inline {
        let blob = mail
            .upload(attachment.data.to_vec(), Some(&attachment.mime))
            .await
            .map_err(super::error::to_account_error)?;
        attachments.push(
            EmailBodyPart::new()
                .with_blob_id(blob.blob_id)
                .with_name(attachment.filename)
                .with_content_type(attachment.mime),
        );
    }
    for handle in uploaded {
        let (blob_id, mime) = decode_attachment_handle(&handle.0);
        let mut part = EmailBodyPart::new().with_blob_id(crate::core::id::BlobId::new(blob_id));
        if let Some(mime) = mime {
            part = part.with_content_type(mime);
        }
        attachments.push(part);
    }

    let content = if body_parts.len() == 1 {
        body_parts.remove(0)
    } else {
        body_parts.into_iter().fold(
            EmailBodyPart::new().with_content_type("multipart/alternative"),
            EmailBodyPart::with_sub_part,
        )
    };
    let body_structure = if attachments.is_empty() {
        Some(content)
    } else {
        let mut mixed = EmailBodyPart::new()
            .with_content_type("multipart/mixed")
            .with_sub_part(content);
        for attachment in &attachments {
            mixed = mixed.with_sub_part(attachment.clone());
        }
        Some(mixed)
    };

    Ok(BuiltBody {
        body_structure,
        body_values,
        text_body,
        html_body,
        attachments,
    })
}

fn encode_attachment_handle(blob_id: &str, mime: Option<&str>) -> String {
    match mime {
        Some(mime) => format!("{ATTACHMENT_HANDLE_PREFIX}{blob_id}\n{mime}"),
        None => format!("{ATTACHMENT_HANDLE_PREFIX}{blob_id}"),
    }
}

fn decode_attachment_handle(handle: &str) -> (String, Option<String>) {
    let raw = handle
        .strip_prefix(ATTACHMENT_HANDLE_PREFIX)
        .unwrap_or(handle);
    let mut parts = raw.splitn(2, '\n');
    let blob = parts.next().unwrap_or("").to_string();
    let mime = parts.next().filter(|v| !v.is_empty()).map(str::to_string);
    (blob, mime)
}

fn address_to_jmap(address: bifrost_types::Address) -> JmapEmailAddress {
    let mut out = JmapEmailAddress::new(address.address);
    if let Some(name) = address.name {
        out = out.with_name(name);
    }
    out
}

fn address_from_jmap(address: &JmapEmailAddress) -> bifrost_types::Address {
    bifrost_types::Address {
        name: address.name().map(str::to_string),
        address: address.email().to_string(),
    }
}

fn submission_address_from_compose(address: &bifrost_types::Address) -> SubmissionAddress {
    SubmissionAddress::new(address.address.clone())
}

fn message_properties(projection: HydrationProjection) -> Vec<EmailProperty> {
    let mut properties = vec![
        EmailProperty::Id,
        EmailProperty::ThreadId,
        EmailProperty::MailboxIds,
        EmailProperty::Keywords,
        EmailProperty::From,
        EmailProperty::To,
        EmailProperty::Cc,
        EmailProperty::Bcc,
        EmailProperty::ReplyTo,
        EmailProperty::Subject,
        EmailProperty::SentAt,
        EmailProperty::ReceivedAt,
        EmailProperty::Size,
        EmailProperty::InReplyTo,
        EmailProperty::References,
        EmailProperty::BlobId,
    ];
    if matches!(
        projection,
        HydrationProjection::Preview(_)
            | HydrationProjection::Full
            | HydrationProjection::FullWithBlobs
    ) {
        properties.extend([
            EmailProperty::Preview,
            EmailProperty::TextBody,
            EmailProperty::HtmlBody,
            EmailProperty::BodyValues,
            EmailProperty::Attachments,
        ]);
    }
    properties
}

fn body_properties() -> Vec<BodyProperty> {
    vec![
        BodyProperty::PartId,
        BodyProperty::BlobId,
        BodyProperty::Size,
        BodyProperty::Name,
        BodyProperty::Type,
    ]
}

fn email_to_message(email: Email, projection: HydrationProjection) -> Message {
    let id = ObjectId(email.id().map(ToString::to_string).unwrap_or_default());
    let thread_id = email.thread_id().map(|id| ThreadId(id.to_string()));
    let containers = email
        .mailbox_ids()
        .into_iter()
        .map(|id| ContainerId(id.to_string()))
        .collect();
    let flags = email
        .keywords()
        .into_iter()
        .map(str::to_string)
        .collect::<HashSet<_>>();
    let body_text = if matches!(
        projection,
        HydrationProjection::Preview(_)
            | HydrationProjection::Full
            | HydrationProjection::FullWithBlobs
    ) {
        collect_body_values(email.text_body(), &email)
            .or_else(|| email.preview().map(str::to_string))
    } else {
        None
    };
    let body_html = if matches!(
        projection,
        HydrationProjection::Full | HydrationProjection::FullWithBlobs
    ) {
        collect_body_values(email.html_body(), &email)
    } else {
        None
    };

    Message {
        id,
        thread_id,
        from: email
            .from()
            .unwrap_or_default()
            .iter()
            .map(address_from_jmap)
            .collect(),
        to: email
            .to()
            .unwrap_or_default()
            .iter()
            .map(address_from_jmap)
            .collect(),
        cc: email
            .cc()
            .unwrap_or_default()
            .iter()
            .map(address_from_jmap)
            .collect(),
        bcc: email
            .bcc()
            .unwrap_or_default()
            .iter()
            .map(address_from_jmap)
            .collect(),
        reply_to: email
            .reply_to()
            .unwrap_or_default()
            .iter()
            .map(address_from_jmap)
            .collect(),
        subject: email.subject().map(str::to_string),
        date: email
            .sent_at()
            .or_else(|| email.received_at())
            .and_then(unix_to_system_time),
        containers,
        flags,
        body_text,
        body_html,
        attachments: email
            .attachments()
            .unwrap_or_default()
            .iter()
            .filter_map(blob_handle_from_part)
            .collect(),
        size_bytes: u64::try_from(email.size()).ok(),
        in_reply_to: email.in_reply_to().and_then(|ids| ids.first().cloned()),
        references: email.references().map(<[_]>::to_vec).unwrap_or_default(),
    }
}

fn collect_body_values(parts: Option<&[EmailBodyPart]>, email: &Email) -> Option<String> {
    let mut values = Vec::new();
    for part in parts.unwrap_or_default() {
        if let Some(part_id) = part.part_id()
            && let Some(value) = email.body_value(part_id)
        {
            values.push(value.value().to_string());
        }
    }
    if values.is_empty() {
        None
    } else {
        Some(values.join("\n"))
    }
}

fn blob_handle_from_part(part: &EmailBodyPart) -> Option<BlobHandle> {
    let id = part.blob_id()?.to_string();
    Some(BlobHandle {
        id: BlobId(id),
        size: u64::try_from(part.size()).ok(),
        content_type: part.content_type().map(str::to_string),
        digest: None,
        capabilities: BlobCapabilities {
            supports_range: false,
            supports_parallel: false,
            digest_available_pre_download: false,
            encoding: BlobEncoding::Raw8Bit,
        },
    })
}

fn system_time_to_unix(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(err) => -i64::try_from(err.duration().as_secs()).unwrap_or(i64::MAX),
    }
}

fn unix_to_system_time(timestamp: i64) -> Option<SystemTime> {
    if timestamp >= 0 {
        Some(UNIX_EPOCH + std::time::Duration::from_secs(u64::try_from(timestamp).ok()?))
    } else {
        UNIX_EPOCH.checked_sub(std::time::Duration::from_secs(timestamp.unsigned_abs()))
    }
}
