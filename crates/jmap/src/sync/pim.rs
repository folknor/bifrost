use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, AccountStream, BlobCapabilities, BlobEncoding,
    BlobHandle, BlobId, Container, ContainerId, ContainerKind, ContainerList, ContainerNamespace,
    ContainerRights, FolderRole, HydrationProjection, Importance, LabelId, Message, MutationTarget,
    ObjectId, Page, Provenance, QuotaInfo, SearchFilter, SearchRequest, SkippedScope,
    ThreadHydration, ThreadId, VacationConfig,
};
/// Convert a crate-internal error to `AccountError` with the correct
/// `AccountOperation` for this call site. Every call site in this
/// module passes the operation that produced the error; `Discover` is
/// no longer used as a default.
#[inline]
fn to_acct_err(op: AccountOperation) -> impl Fn(crate::Error) -> AccountError {
    move |err| super::error::into_account_error(err, super::error::JmapErrorContext::new(op))
}

/// Local helper for search-cursor decode failures: the page cursor
/// JMAP stores is opaque, and a malformed value is a schema mismatch
/// from the caller's point of view. Maps to `SyncState(SchemaIncompatible)`.
fn schema_incompatible_search_cursor() -> AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::SyncState(
            bifrost_types::SyncStateErrorKind::SchemaIncompatible,
        ),
        bifrost_types::Cause::State(bifrost_types::StateCause::SchemaIncompatible),
    )
    .protocol(bifrost_types::Protocol::Jmap)
    .operation(AccountOperation::Search)
    .text(bifrost_types::DiagnosticText::support_only(
        "search page cursor was malformed",
    ))
    .try_build()
    .expect("valid account error classification")
}
use bytes::Bytes;
use futures::StreamExt;

use super::state_cache::{self, StateMap};
use crate::core::SetCreate;
use crate::core::query;
use crate::core::transport::HttpTransport;
use crate::email::{
    BodyProperty, DRAFT_KEYWORD, Email, EmailAddress as JmapEmailAddress, EmailBodyPart,
    EmailBodyValue, EmailGet, EmailId, EmailPatch, EmailSet, Property as EmailProperty,
};
use crate::email_submission::{Address as SubmissionAddress, EmailSubmissionSet, UndoStatus};
use crate::identity::{IdentityGet, IdentityId as JmapIdentityId, IdentitySet};
use crate::mailbox::{
    Mailbox, MailboxGet, MailboxId, MailboxRights, MailboxSet, Property as MailboxProperty, Role,
};
use crate::quota::{Property as QuotaProperty, QuotaGet};
use crate::thread::{ThreadGet, ThreadId as JmapThreadId};
use crate::vacation_response::{VacationResponseGet, VacationResponseId, VacationResponseSet};

type MailAccount<T> = crate::account::Account<T>;

const SEEN_KEYWORD: &str = "$seen";
const IMPORTANT_KEYWORD: &str = "$important";
const SUBMISSION_CREATE_ID: &str = "submit0";
const ATTACHMENT_HANDLE_PREFIX: &str = "jmap:";

/// Foreign (shared/delegate) submission context selected by the account
/// layer. `mail` already targets the foreign JMAP account.
pub(crate) struct ForeignSubmission {
    pub(crate) mode: bifrost_types::SendAs,
    pub(crate) self_address: Option<bifrost_types::Address>,
}

pub(crate) fn add_to_container<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        patch_mailbox_membership(
            &mail,
            &email_states,
            &account_id,
            target,
            container,
            true,
            AccountOperation::AddToContainer,
        )
        .await
    })
}

pub(crate) fn remove_from_container<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    target: MutationTarget,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        patch_mailbox_membership(
            &mail,
            &email_states,
            &account_id,
            target,
            container,
            false,
            AccountOperation::RemoveFromContainer,
        )
        .await
    })
}

pub(crate) fn set_keyword<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    target: MutationTarget,
    keyword: String,
    value: bool,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let ids = resolve_target(&mail, target, AccountOperation::SetKeyword).await?;
        let keyword_for_set = keyword.clone();
        let ids_for_set = ids.clone();
        let mut response = send_email_set_with_retry(
            &mail,
            &email_states,
            &account_id,
            AccountOperation::SetKeyword,
            move |state| {
                let mut set = EmailSet::new().if_in_state(state.to_string());
                for id in &ids_for_set {
                    set.update(id.clone()).keyword(&keyword_for_set, value);
                }
                set
            },
        )
        .await?;
        for id in &ids {
            response
                .updated(id)
                .map_err(to_acct_err(AccountOperation::SetKeyword))?;
        }
        Ok(())
    })
}

pub(crate) fn set_is_read<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    target: MutationTarget,
    is_read: bool,
) -> AccountFuture<Result<(), AccountError>> {
    set_keyword(
        mail,
        email_states,
        account_id,
        target,
        SEEN_KEYWORD.to_string(),
        is_read,
    )
}

/// Exclusive importance overwrite via the `$important` keyword. JMAP's
/// model is two-valued: `High` sets `$important`, `Normal`/`Low` clear
/// it. One `Email/set` keyword update, no expand-into-two.
pub(crate) fn set_importance<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    target: MutationTarget,
    level: Importance,
) -> AccountFuture<Result<(), AccountError>> {
    set_keyword(
        mail,
        email_states,
        account_id,
        target,
        IMPORTANT_KEYWORD.to_string(),
        importance_sets_important_keyword(level),
    )
}

/// JMAP's two-valued importance mapping: `High` sets `$important`,
/// `Normal`/`Low` clear it.
fn importance_sets_important_keyword(level: Importance) -> bool {
    matches!(level, Importance::High)
}

/// Resolve From/Sender for a foreign submission. `OnBehalfOf` keeps an
/// explicit consumer From for compatibility with Graph; otherwise the
/// foreign identity supplies it. The envelope deliberately follows From.
fn resolve_foreign_headers(
    mode: &bifrost_types::SendAs,
    ident: &bifrost_types::Address,
    consumer_from: Option<bifrost_types::Address>,
    self_address: Option<bifrost_types::Address>,
) -> (bifrost_types::Address, Option<bifrost_types::Address>) {
    match mode {
        bifrost_types::SendAs::As(_) => (ident.clone(), None),
        bifrost_types::SendAs::OnBehalfOf(_) => {
            (consumer_from.unwrap_or_else(|| ident.clone()), self_address)
        }
        _ => (ident.clone(), None),
    }
}

pub(crate) fn send_message<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    max_delayed_send: usize,
    foreign: Option<ForeignSubmission>,
    mut request: bifrost_types::SendRequest,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        let personal_identity = request.identity.clone();
        let identity = if let Some(foreign) = foreign.as_ref() {
            let selected = foreign_sending_identity(&mail, request.identity.as_ref()).await?;
            let ident_address = bifrost_types::Address {
                name: selected.name,
                address: selected.email,
            };
            let (from, sender) = resolve_foreign_headers(
                &foreign.mode,
                &ident_address,
                request.from.take(),
                foreign.self_address.clone(),
            );
            request.from = Some(from);
            Some((JmapIdentityId::new(selected.id), sender))
        } else {
            None
        };
        let scheduled = request.scheduled;
        if let Some(at) = scheduled {
            // A scheduled request on a relay with no delay window
            // (`maxDelayedSend == 0`, the `scheduled_send` flag false) is
            // unsupported, not malformed - mirror the cross-provider
            // contract (gmail/imap surface `Unsupported(Send)` too).
            if max_delayed_send == 0 {
                return Err(super::error::unsupported_error(
                    AccountOperation::Send,
                    None,
                    "JMAP relay advertises no scheduled-send window",
                ));
            }
            // The window source is the server's maxDelayedSend seconds.
            let window = std::time::Duration::from_secs(max_delayed_send as u64);
            bifrost_types::validate_scheduled(at, Some(window))?;
        }
        let envelope_from = request.from.clone();
        let envelope_recipients = request
            .to
            .iter()
            .chain(&request.cc)
            .chain(&request.bcc)
            .cloned()
            .collect::<Vec<_>>();
        let draft_mailbox = role_mailbox(&mail, FolderRole::Drafts, AccountOperation::Send).await?;
        let sent_mailbox = if request.save_to_sent == Some(false) {
            None
        } else {
            Some(role_mailbox(&mail, FolderRole::Sent, AccountOperation::Send).await?)
        };

        let mut create =
            build_email_create_from_send(&mail, request, draft_mailbox.clone()).await?;
        if let Some((_, Some(sender))) = identity.as_ref() {
            create.sender([address_to_jmap(sender.clone())]);
        }
        let mut email_set = EmailSet::new();
        let email_create_id = email_set.create_item(create);

        let mut submission_set = EmailSubmissionSet::new();
        {
            let submit = submission_set.create_with_id(SUBMISSION_CREATE_ID);
            submit.undo_status(UndoStatus::Final);
            if let Some((identity, _)) = identity.as_ref() {
                submit.identity_id(identity.clone());
            } else if let Some(identity) = personal_identity.as_ref() {
                submit.identity_id(JmapIdentityId::new(identity.0.clone()));
            }
            if let Some(from) = envelope_from.as_ref()
                && !envelope_recipients.is_empty()
            {
                // RFC 8621 carries SMTP FUTURERELEASE params on the
                // envelope mailFrom address. `holduntil` is the
                // absolute-time form matching our `SystemTime`.
                let mut mail_from = submission_address_from_compose(from);
                if let Some(at) = scheduled {
                    mail_from = mail_from.with_parameter("holduntil", Some(rfc3339(at)));
                }
                submit.envelope(
                    mail_from,
                    envelope_recipients
                        .iter()
                        .map(submission_address_from_compose),
                );
            } else if scheduled.is_some() {
                // A scheduled send needs an envelope mailFrom to carry
                // the hold parameter; without an explicit `from` and
                // recipients there is nowhere to stamp it.
                return Err(scheduled_requires_envelope());
            }
        }

        if let Some(sent) = sent_mailbox.as_ref() {
            submission_set
                .on_success_update_email(SUBMISSION_CREATE_ID)
                .submitted_to_sent(sent, Some(&draft_mailbox));
        } else {
            submission_set = submission_set.on_success_destroy_email(SUBMISSION_CREATE_ID);
        }

        // Capture the per-account email state observed before the set so
        // the post-call advance is compare-and-swap, not an
        // unconditional overwrite. A slow `Email/set` response carrying
        // an older `newState` must not clobber a newer state the
        // `Email/changes` loop has since CAS-set.
        let prior_state = state_cache::get(&email_states, &account_id).await;
        let mut batch = mail.build();
        let email_handle = batch
            .call(email_set)
            .map_err(to_acct_err(AccountOperation::Send))?;
        let email_ref = email_handle.result_reference(format!("/created/{email_create_id}/id"));
        submission_set
            .create_with_id(SUBMISSION_CREATE_ID)
            .email_id_ref(email_ref);
        let submission_handle = batch
            .call(submission_set)
            .map_err(to_acct_err(AccountOperation::Send))?;

        let mut response = batch
            .send()
            .await
            .map_err(to_acct_err(AccountOperation::Send))?;
        let mut email_response = response
            .get(&email_handle)
            .map_err(to_acct_err(AccountOperation::Send))?;
        let mut submission_response = response
            .get(&submission_handle)
            .map_err(to_acct_err(AccountOperation::Send))?;
        let mut email = email_response
            .created(&email_create_id)
            .map_err(to_acct_err(AccountOperation::Send))?;
        let mut submission = submission_response
            .created(SUBMISSION_CREATE_ID)
            .map_err(to_acct_err(AccountOperation::Send))?;

        if !email_response.new_state().is_empty() {
            state_cache::advance(
                &email_states,
                &account_id,
                prior_state.as_deref(),
                email_response.new_state().to_string(),
            )
            .await;
        }

        // Handle contract (A4): a scheduled send returns the
        // EmailSubmission id - the undo-addressable object for
        // cancel/reschedule. An immediate send keeps returning the
        // email id (the submission is final, not addressable for undo).
        if scheduled.is_some() {
            Ok(ObjectId(submission.take_id().into_string()))
        } else {
            Ok(ObjectId(email.take_id().into_string()))
        }
    })
}

pub(crate) fn send_raw_message<T: HttpTransport>(
    mail: MailAccount<T>,
    raw: Bytes,
    save_to_sent: Option<bool>,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        // JMAP has no raw-bytes send. The pre-assembled RFC 5322 / RFC 8098
        // octets are uploaded as a blob, imported into Drafts via
        // `Email/import`, then submitted via `EmailSubmission/set`. The
        // submission omits an explicit envelope, so the server derives
        // MAIL FROM / RCPT TO from the message's own header fields
        // (RFC 8621 §7) - no MIME parse needed on our side.
        let draft_mailbox = role_mailbox(&mail, FolderRole::Drafts, AccountOperation::Send).await?;
        let sent_mailbox = if save_to_sent == Some(false) {
            None
        } else {
            Some(role_mailbox(&mail, FolderRole::Sent, AccountOperation::Send).await?)
        };

        let blob = mail
            .upload(raw.to_vec(), Some("message/rfc822"))
            .await
            .map_err(to_acct_err(AccountOperation::Send))?;

        let mut import_req = crate::email::import::EmailImportRequest::new();
        let import_create_id = {
            let entry = import_req.email(blob.blob_id);
            entry.mailbox_ids([draft_mailbox.clone()]);
            entry.keywords([DRAFT_KEYWORD]);
            entry.create_id()
        };

        let mut submission_set = EmailSubmissionSet::new();
        submission_set
            .create_with_id(SUBMISSION_CREATE_ID)
            .undo_status(UndoStatus::Final);
        if let Some(sent) = sent_mailbox.as_ref() {
            submission_set
                .on_success_update_email(SUBMISSION_CREATE_ID)
                .submitted_to_sent(sent, Some(&draft_mailbox));
        } else {
            submission_set = submission_set.on_success_destroy_email(SUBMISSION_CREATE_ID);
        }

        let mut batch = mail.build();
        let import_handle = batch
            .call(import_req)
            .map_err(to_acct_err(AccountOperation::Send))?;
        let email_ref = import_handle.result_reference(format!("/created/{import_create_id}/id"));
        submission_set
            .create_with_id(SUBMISSION_CREATE_ID)
            .email_id_ref(email_ref);
        let submission_handle = batch
            .call(submission_set)
            .map_err(to_acct_err(AccountOperation::Send))?;

        let mut response = batch
            .send()
            .await
            .map_err(to_acct_err(AccountOperation::Send))?;
        let mut import_response = response
            .get(&import_handle)
            .map_err(to_acct_err(AccountOperation::Send))?;
        let mut submission_response = response
            .get(&submission_handle)
            .map_err(to_acct_err(AccountOperation::Send))?;
        // Surface a submission failure rather than reporting the import id
        // as if the send committed.
        submission_response
            .created(SUBMISSION_CREATE_ID)
            .map_err(to_acct_err(AccountOperation::Send))?;
        let mut email = import_response
            .created(&import_create_id)
            .map_err(to_acct_err(AccountOperation::Send))?;
        Ok(ObjectId(email.take_id().into_string()))
    })
}

pub(crate) fn attachment_upload<T: HttpTransport>(
    mail: MailAccount<T>,
    mut bytes: AccountStream<Result<Bytes, AccountError>>,
    mime: String,
) -> AccountFuture<Result<bifrost_types::AttachmentHandle, AccountError>> {
    Box::pin(async move {
        let mut data = Vec::new();
        while let Some(chunk) = bytes.next().await {
            let chunk = chunk?;
            data.extend_from_slice(&chunk);
        }
        let blob = mail
            .upload(data, Some(&mime))
            .await
            .map_err(to_acct_err(AccountOperation::AttachmentUpload))?;
        Ok(bifrost_types::AttachmentHandle(encode_attachment_handle(
            blob.blob_id.as_str(),
            Some(&mime),
        )))
    })
}

pub(crate) fn draft_create<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    patch: bifrost_types::DraftPatch,
) -> AccountFuture<Result<bifrost_types::DraftHandle, AccountError>> {
    Box::pin(async move {
        let draft_mailbox =
            role_mailbox(&mail, FolderRole::Drafts, AccountOperation::DraftCreate).await?;
        let create = build_email_create_from_draft(
            &mail,
            patch,
            draft_mailbox,
            AccountOperation::DraftCreate,
        )
        .await?;
        let mut set = EmailSet::new();
        let create_id = set.create_item(create);
        // CAS the post-create state against the state seen before the
        // call (see `send_message`) so a slow response cannot clobber a
        // newer state the changes loop already advanced to.
        let prior_state = state_cache::get(&email_states, &account_id).await;
        let mut response = mail
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::DraftCreate))?;
        if !response.new_state().is_empty() {
            state_cache::advance(
                &email_states,
                &account_id,
                prior_state.as_deref(),
                response.new_state().to_string(),
            )
            .await;
        }
        let mut email = response
            .created(&create_id)
            .map_err(to_acct_err(AccountOperation::DraftCreate))?;
        Ok(bifrost_types::DraftHandle(email.take_id().into_string()))
    })
}

pub(crate) fn draft_update<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    draft: bifrost_types::DraftHandle,
    patch: bifrost_types::DraftPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let email_id = EmailId::new(draft.0);
        let mut email_patch = EmailPatch::default();
        apply_draft_patch_to_email_patch(
            &mail,
            &mut email_patch,
            patch,
            AccountOperation::DraftUpdate,
        )
        .await?;
        let email_id_for_set = email_id.clone();
        let mut response = send_email_set_with_retry(
            &mail,
            &email_states,
            &account_id,
            AccountOperation::DraftUpdate,
            move |state| {
                let mut set = EmailSet::new().if_in_state(state.to_string());
                set.update_item(email_id_for_set.clone(), email_patch.clone());
                set
            },
        )
        .await?;
        response
            .updated(&email_id)
            .map_err(to_acct_err(AccountOperation::DraftUpdate))?;
        Ok(())
    })
}

pub(crate) fn draft_discard<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    draft: bifrost_types::DraftHandle,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        destroy_emails(
            &mail,
            &email_states,
            &account_id,
            [EmailId::new(draft.0)],
            AccountOperation::DraftDiscard,
        )
        .await
    })
}

pub(crate) fn draft_send<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    draft: bifrost_types::DraftHandle,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        let draft_id = EmailId::new(draft.0.clone());
        // Sent and Drafts in one `Mailbox/get`: the submission patch
        // needs the Drafts id to un-file the draft by path rather than
        // by replacing `mailboxIds` wholesale.
        let mut roles = role_mailboxes(
            &mail,
            &[FolderRole::Sent, FolderRole::Drafts],
            AccountOperation::DraftSend,
        )
        .await?;
        let sent_mailbox = roles
            .remove(&FolderRole::Sent)
            .ok_or_else(|| missing_role_mailbox(AccountOperation::DraftSend))?;
        let draft_mailbox = roles.remove(&FolderRole::Drafts);
        let mut submission_set = EmailSubmissionSet::new();
        submission_set
            .create_with_id(SUBMISSION_CREATE_ID)
            .email_id(draft_id.clone())
            .undo_status(UndoStatus::Final);
        submission_set
            .on_success_update_email(SUBMISSION_CREATE_ID)
            .submitted_to_sent(&sent_mailbox, draft_mailbox.as_ref());
        let mut response = mail
            .call(submission_set)
            .await
            .map_err(to_acct_err(AccountOperation::DraftSend))?;
        response
            .created(SUBMISSION_CREATE_ID)
            .map_err(to_acct_err(AccountOperation::DraftSend))?;
        let fresh = super::mutation::probe_email_state(&mail)
            .await
            .map_err(to_acct_err(AccountOperation::DraftSend))?;
        state_cache::set(&email_states, &account_id, fresh).await;
        Ok(ObjectId(draft_id.into_string()))
    })
}

pub(crate) fn search<T: HttpTransport>(
    mail: MailAccount<T>,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
    Box::pin(async move {
        let page = search_email_ids(&mail, request, true, AccountOperation::Search).await?;
        if page.items.is_empty() {
            return Ok(Page {
                items: Vec::new(),
                next_cursor: page.next_cursor,
                estimated_total: page.estimated_total,
                failed_ids: Vec::new(),
                skipped_scopes: Vec::new(),
            });
        }
        let response = mail
            .call(
                EmailGet::new()
                    .ids(page.items)
                    .properties([EmailProperty::Id, EmailProperty::ThreadId]),
            )
            .await
            .map_err(to_acct_err(AccountOperation::Search))?;
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
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        })
    })
}

pub(crate) fn search_messages<T: HttpTransport>(
    mail: MailAccount<T>,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
    Box::pin(async move {
        let page =
            search_email_ids(&mail, request, false, AccountOperation::SearchMessages).await?;
        Ok(Page {
            items: page
                .items
                .into_iter()
                .map(|id| ObjectId(id.into_string()))
                .collect(),
            next_cursor: page.next_cursor,
            estimated_total: page.estimated_total,
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        })
    })
}

/// Every container the account exposes: the primary account's mailboxes
/// plus each foreign (shared/delegate) account's, namespaced by owner.
///
/// A per-share enumeration failure degrades to a `SkippedScope` on the
/// returned `ContainerList` plus the remaining containers: one
/// unreachable share must not blank the whole sidebar, and it must not
/// vanish from it silently either - the skip entry names the foreign
/// account and carries the classified error, so the consumer can tell
/// "share degraded" from "share deleted". A primary enumeration
/// failure still fails the call.
pub(crate) fn containers_list<T: HttpTransport>(
    mail: MailAccount<T>,
    foreign_mail: Arc<HashMap<String, MailAccount<T>>>,
) -> AccountFuture<Result<ContainerList, AccountError>> {
    Box::pin(async move {
        let mut containers = fetch_containers(&mail, AccountOperation::ContainersList).await?;
        // Owner emails are resolved once per foreign account per call
        // (never per container) and are best-effort metadata: any
        // resolution failure yields `None` for that account and MUST NOT
        // propagate out of `containers_list` - the consumer treats a
        // container-listing error as an attach failure, and a missing
        // cosmetic email must never cause an account outage.
        let owner_emails = resolve_owner_emails(&mail, &foreign_mail).await;
        let (foreign, skipped_scopes) = fetch_foreign_containers(
            &foreign_mail,
            &owner_emails,
            AccountOperation::ContainersList,
        )
        .await;
        containers.extend(foreign);
        Ok(ContainerList {
            containers,
            skipped_scopes,
        })
    })
}

pub(crate) fn container_create<T: HttpTransport>(
    mail: MailAccount<T>,
    mailbox_states: StateMap,
    account_id: String,
    kind: ContainerKind,
    name: String,
    parent: Option<ContainerId>,
    // JMAP mailboxes carry no container color; accepted for trait
    // parity with the colorable (Gmail) path and ignored.
    _style: Option<bifrost_types::ContainerStyle>,
) -> AccountFuture<Result<ContainerId, AccountError>> {
    Box::pin(async move {
        if !matches!(kind, ContainerKind::Folder) {
            return Err(super::error::unsupported_error(
                AccountOperation::ContainerCreate,
                None,
                "JMAP only supports folder containers",
            ));
        }
        let mut set = MailboxSet::new();
        let mut create = crate::mailbox::MailboxCreate::new(None);
        create.name(name);
        create.parent_id(parent.map(|id| MailboxId::new(id.0)));
        let create_id = set.create_item(create);
        let mut response = mail
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::ContainerCreate))?;
        if !response.new_state().is_empty() {
            state_cache::set(
                &mailbox_states,
                &account_id,
                response.new_state().to_string(),
            )
            .await;
        }
        let mut mailbox = response
            .created(&create_id)
            .map_err(to_acct_err(AccountOperation::ContainerCreate))?;
        Ok(ContainerId(mailbox.take_id().into_string()))
    })
}

pub(crate) fn container_rename<T: HttpTransport>(
    mail: MailAccount<T>,
    mailbox_states: StateMap,
    account_id: String,
    container: ContainerId,
    name: String,
    // JMAP has no mailbox recolor; accepted for trait parity and ignored.
    _style: Option<bifrost_types::ContainerStyle>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let mailbox = MailboxId::new(container.0);
        let mut set = MailboxSet::new();
        set.update(mailbox).name(name);
        let response = mail
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::ContainerRename))?;
        response
            .unwrap_update_errors()
            .map_err(to_acct_err(AccountOperation::ContainerRename))?;
        if !response.new_state().is_empty() {
            state_cache::set(
                &mailbox_states,
                &account_id,
                response.new_state().to_string(),
            )
            .await;
        }
        Ok(())
    })
}

pub(crate) fn container_move<T: HttpTransport>(
    mail: MailAccount<T>,
    mailbox_states: StateMap,
    account_id: String,
    container: ContainerId,
    new_parent: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let mailbox = MailboxId::new(container.0);
        let mut set = MailboxSet::new();
        set.update(mailbox)
            .parent_id(new_parent.map(|id| MailboxId::new(id.0)));
        let response = mail
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::ContainerMove))?;
        response
            .unwrap_update_errors()
            .map_err(to_acct_err(AccountOperation::ContainerMove))?;
        if !response.new_state().is_empty() {
            state_cache::set(
                &mailbox_states,
                &account_id,
                response.new_state().to_string(),
            )
            .await;
        }
        Ok(())
    })
}

pub(crate) fn container_delete<T: HttpTransport>(
    mail: MailAccount<T>,
    mailbox_states: StateMap,
    account_id: String,
    container: ContainerId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let mailbox = MailboxId::new(container.0);
        let mut response = mail
            .call(
                MailboxSet::new()
                    .destroy([mailbox.clone()])
                    .on_destroy_remove_emails(false),
            )
            .await
            .map_err(to_acct_err(AccountOperation::ContainerDelete))?;
        response
            .destroyed(&mailbox)
            .map_err(to_acct_err(AccountOperation::ContainerDelete))?;
        if !response.new_state().is_empty() {
            state_cache::set(
                &mailbox_states,
                &account_id,
                response.new_state().to_string(),
            )
            .await;
        }
        Ok(())
    })
}

pub(crate) fn identities_list<T: HttpTransport>(
    submission: Option<MailAccount<T>>,
) -> AccountFuture<Result<Vec<bifrost_types::Identity>, AccountError>> {
    Box::pin(async move {
        let Some(account) = submission else {
            return Err(super::error::unsupported_error(
                AccountOperation::IdentitiesList,
                None,
                "JMAP submission capability not available",
            ));
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
            .map_err(to_acct_err(AccountOperation::IdentitiesList))?;
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

#[cfg_attr(test, derive(Debug))]
struct ForeignIdentity {
    id: String,
    name: Option<String>,
    email: String,
}

/// Select a concrete foreign identity. JMAP does not designate a default
/// identity, so the first concrete address returned by the server is the
/// documented best-effort convention. Wildcard identities cannot be used in
/// an RFC 5322 From header.
async fn foreign_sending_identity<T: HttpTransport>(
    mail: &MailAccount<T>,
    requested: Option<&bifrost_types::IdentityId>,
) -> Result<ForeignIdentity, AccountError> {
    let get = IdentityGet::new().properties([
        crate::identity::Property::Id,
        crate::identity::Property::Name,
        crate::identity::Property::Email,
    ]);
    let get = match requested {
        Some(requested) => get.ids([JmapIdentityId::new(requested.0.clone())]),
        None => get,
    };
    let response = mail
        .call(get)
        .await
        .map_err(to_acct_err(AccountOperation::Send))?;
    select_concrete_identity(response.into_list(), requested.is_some())
}

/// Pure identity selection: first row with a concrete (non-empty,
/// non-wildcard) email. `requested` only shapes the rejection detail (the
/// server was asked for a specific id, so an empty result means that id is
/// absent or wildcard-only, not that the account has no identity at all).
fn select_concrete_identity(
    rows: Vec<crate::identity::Identity>,
    requested: bool,
) -> Result<ForeignIdentity, AccountError> {
    let mut concrete = rows.into_iter().filter_map(|mut identity| {
        let id = identity.take_id().into_string();
        let email = identity.email()?.to_string();
        (!id.is_empty() && !email.is_empty() && !email.contains('*')).then(|| ForeignIdentity {
            id,
            name: identity.name().map(str::to_string),
            email,
        })
    });
    if let Some(identity) = concrete.next() {
        return Ok(identity);
    }
    let detail = if requested {
        "requested foreign sending identity is absent or has no concrete email"
    } else {
        "foreign account advertises no sending identity"
    };
    Err(super::error::unsupported_error(
        AccountOperation::Send,
        None,
        detail,
    ))
}

pub(crate) fn identity_update<T: HttpTransport>(
    submission: Option<MailAccount<T>>,
    identity: bifrost_types::IdentityId,
    patch: bifrost_types::IdentityPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let Some(account) = submission else {
            return Err(super::error::unsupported_error(
                AccountOperation::IdentityUpdate,
                None,
                "JMAP submission capability not available",
            ));
        };
        if patch.is_default.is_some() {
            return Err(super::error::unsupported_error(
                AccountOperation::IdentityUpdate,
                None,
                "JMAP does not support setting default identity",
            ));
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
            .map_err(to_acct_err(AccountOperation::IdentityUpdate))?;
        response
            .unwrap_update_errors()
            .map_err(to_acct_err(AccountOperation::IdentityUpdate))
    })
}

pub(crate) fn vacation_get<T: HttpTransport>(
    vacation: Option<MailAccount<T>>,
) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
    Box::pin(async move {
        let Some(account) = vacation else {
            return Err(super::error::unsupported_error(
                AccountOperation::VacationGet,
                None,
                "JMAP vacation capability not available",
            ));
        };
        let mut response = account
            .call(VacationResponseGet::new().ids([VacationResponseId::new("singleton")]))
            .await
            .map_err(to_acct_err(AccountOperation::VacationGet))?;
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

pub(crate) fn vacation_set<T: HttpTransport>(
    vacation: Option<MailAccount<T>>,
    config: VacationConfig,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let Some(account) = vacation else {
            return Err(super::error::unsupported_error(
                AccountOperation::VacationSet,
                None,
                "JMAP vacation capability not available",
            ));
        };
        let mut set = VacationResponseSet::new();
        let patch = set.update(VacationResponseId::new("singleton"));
        patch.is_enabled(config.is_enabled);
        patch.subject(config.subject);
        patch.text_body(config.body_text);
        patch.html_body(config.body_html);
        patch.from_date(config.starts_at.map(system_time_to_unix));
        patch.to_date(config.ends_at.map(system_time_to_unix));
        let response = account
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::VacationSet))?;
        response
            .unwrap_update_errors()
            .map_err(to_acct_err(AccountOperation::VacationSet))
    })
}

pub(crate) fn quota_get<T: HttpTransport>(
    quota: Option<MailAccount<T>>,
) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
    Box::pin(async move {
        let Some(account) = quota else {
            return Err(super::error::unsupported_error(
                AccountOperation::QuotaGet,
                None,
                "JMAP quota capability not available",
            ));
        };
        let response = account
            .call(QuotaGet::new().properties([
                QuotaProperty::ResourceType,
                QuotaProperty::Used,
                QuotaProperty::HardLimit,
            ]))
            .await
            .map_err(to_acct_err(AccountOperation::QuotaGet))?;
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

/// Route a thread id to the account that owns it.
///
/// `Thread/get` is accountId-scoped and the thread id is the ONLY operand,
/// so an owner-qualified foreign thread id has to select its own account's
/// handle before the qualification is stripped. An id naming an account
/// this session has no handle for stays on the primary route with its
/// LITERAL (still-qualified) form - the same rule `hydrate::route_for_id`
/// plus `wire_id_for_mail` apply to message ids: stripped, the bare native
/// id could resolve an unrelated same-id primary thread; literal, it can
/// name no real primary thread (`\u{1f}` never occurs in an RFC 8620 id)
/// and the server reports the honest `notFound`.
/// Pure over the registration predicate, so the selection is unit-pinnable
/// without a live session - the same shape as `hydrate::route_for_id`.
pub(crate) fn thread_owner<F>(thread: &ThreadId, is_registered: F) -> Option<String>
where
    F: Fn(&str) -> bool,
{
    super::foreign::parse_object(&thread.0)
        .filter(|(account, _)| is_registered(account))
        .map(|(account, _)| account.to_string())
}

pub(crate) fn thread_hydrate<T: HttpTransport>(
    mail: MailAccount<T>,
    foreign_mail: Arc<HashMap<String, MailAccount<T>>>,
    thread: ThreadId,
) -> AccountFuture<Result<ThreadHydration, AccountError>> {
    Box::pin(async move {
        let owner = thread_owner(&thread, |account| foreign_mail.contains_key(account));
        let mail = match owner
            .as_deref()
            .and_then(|account| foreign_mail.get(account))
        {
            Some(handle) => handle.clone(),
            None => mail,
        };
        let jmap_thread_id = JmapThreadId::new(wire_id_for_mail(&thread.0, mail.id_str()));
        let mut thread_response = mail
            .call(ThreadGet::new().ids([jmap_thread_id]).properties([
                crate::thread::Property::Id,
                crate::thread::Property::EmailIds,
            ]))
            .await
            .map_err(to_acct_err(AccountOperation::HydrateThread))?;
        let thread_object = thread_response.pop().ok_or_else(|| {
            super::error::into_account_error(
                crate::Error::IdNotFound(thread.0.clone()),
                super::error::JmapErrorContext::new(AccountOperation::HydrateThread).with_scope(
                    bifrost_types::ErrorScope::Thread {
                        id: thread.0.clone(),
                    },
                ),
            )
        })?;
        let order = thread_object
            .email_ids()
            .ok_or_else(|| {
                to_acct_err(AccountOperation::HydrateThread)(crate::Error::NotParsable(
                    "Thread/get response omitted requested emailIds".to_string(),
                ))
            })?
            .to_vec();
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
            .map_err(to_acct_err(AccountOperation::HydrateThread))?;
        let mut by_id = response
            .into_list()
            .into_iter()
            .filter_map(|email| email.id().map(ToString::to_string).map(|id| (id, email)))
            .collect::<HashMap<_, _>>();
        let mut messages = Vec::new();
        for id in order {
            if let Some(email) = by_id.remove(id.as_str()) {
                let mut message = email_to_message(email, HydrationProjection::Full);
                // The member ids came off the wire in the foreign
                // account's own namespace. Handed back bare they would
                // route every follow-on request (blob download, container
                // join, a per-message mutation) through the PRIMARY
                // account, so re-qualify them into the same namespace the
                // foreign inventory and `message_hydrate` mint.
                if let Some(account) = owner.as_deref() {
                    qualify_foreign_message_ids(
                        &mut message.id,
                        message.thread_id.as_mut(),
                        &mut message.containers,
                        &mut message.attachments,
                        account,
                    );
                }
                messages.push(message);
            }
        }
        // The hydration's own id is the id the CALLER submitted, verbatim,
        // so a round-trip through this door is byte-stable.
        Ok(ThreadHydration {
            id: thread,
            messages,
        })
    })
}

/// Re-qualify a foreign account's `Message` ids with their owning account.
///
/// `email_to_message` reads the bare native ids off the wire, but every id a
/// consumer hands back to this crate (`open_blob`, `get_stream`, a container
/// join against `containers_list`) has to be self-routing. Without this the
/// hydrated message's own id, its container ids, and its attachment blob ids
/// all come back in the PRIMARY namespace, so the follow-up blob download
/// 404s against the primary account and the container ids join nothing.
///
/// The `thread_id` is qualified for the same reason, and it is the
/// sharpest one: the consumer hands it back as `thread_hydrate(thread)`
/// and as `MutationTarget::Thread`, each of which expands it through an
/// accountId-scoped `Thread/get`. A bare foreign thread id asserts primary
/// ownership, so on an id collision the write lands on an unrelated
/// primary thread's messages.
///
/// Pure over the id slices so the qualification is unit-pinnable
/// without constructing a whole `Message`.
fn qualify_foreign_message_ids(
    id: &mut ObjectId,
    thread_id: Option<&mut ThreadId>,
    containers: &mut [ContainerId],
    attachments: &mut [BlobHandle],
    account: &str,
) {
    id.0 = super::foreign::encode_object(account, &id.0);
    if let Some(thread) = thread_id {
        thread.0 = super::foreign::encode_object(account, &thread.0);
    }
    for container in containers {
        // A container id is a MAILBOX id, so it carries the same namespace
        // `containers_list` mints for a foreign mailbox - not the object
        // namespace - and the two must not be confused.
        *container = ContainerId(super::foreign::encode_foreign(account, &container.0).0);
    }
    for attachment in attachments {
        attachment.id = BlobId(super::foreign::encode_object(account, &attachment.id.0));
    }
}

pub(crate) fn message_hydrate<T: HttpTransport>(
    mail: MailAccount<T>,
    foreign_mail: Arc<HashMap<String, MailAccount<T>>>,
    message: ObjectId,
    projection: HydrationProjection,
) -> AccountFuture<Result<Message, AccountError>> {
    Box::pin(async move {
        // `Email/get` is accountId-scoped. A foreign-qualified id therefore
        // has to run against its OWNING account's handle: on the primary
        // handle it either 404s (reporting the encoded id verbatim, which is
        // how this surfaced downstream) or resolves an unrelated primary
        // object that happens to share the native id. `get_stream` already
        // routed per id; this one-id door did not, and it is the door the
        // engine's `message_hydrate` passthrough funnels into.
        let owner = match super::hydrate::route_for_id(&message, |account| {
            foreign_mail.contains_key(account)
        }) {
            super::hydrate::HydrationRoute::Foreign(account) => Some(account),
            super::hydrate::HydrationRoute::Primary => None,
        };
        let mail = match owner
            .as_deref()
            .and_then(|account| foreign_mail.get(account))
        {
            Some(handle) => handle.clone(),
            None => mail,
        };
        // The wire call takes the native id only when the call runs against
        // the id's own foreign account. An id for an UNREGISTERED account
        // rides the primary route with its literal (still-qualified) form,
        // matching `wire_id_for_mail` and the bulk pipeline: stripped, the
        // bare native id could resolve an unrelated same-id primary message;
        // literal, the primary `Email/get` reports the honest `notFound`.
        let native = super::hydrate::wire_object_id(&message, owner.as_deref()).to_string();
        let mut get = EmailGet::new()
            .ids([EmailId::new(native)])
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
            .map_err(to_acct_err(AccountOperation::HydrateMessage))?;
        let email = response.pop().ok_or_else(|| {
            super::error::into_account_error(
                crate::Error::IdNotFound(message.0.clone()),
                super::error::JmapErrorContext::new(AccountOperation::HydrateMessage).with_scope(
                    bifrost_types::ErrorScope::Message {
                        id: message.0.clone(),
                    },
                ),
            )
        })?;
        let mut hydrated = email_to_message(email, projection);
        if let Some(account) = owner.as_deref() {
            qualify_foreign_message_ids(
                &mut hydrated.id,
                hydrated.thread_id.as_mut(),
                &mut hydrated.containers,
                &mut hydrated.attachments,
                account,
            );
        }
        Ok(hydrated)
    })
}

pub(crate) fn move_thread<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    thread: ThreadId,
    target: ContainerId,
    source: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        patch_mailbox_membership(
            &mail,
            &email_states,
            &account_id,
            MutationTarget::Thread(thread.clone()),
            target,
            true,
            AccountOperation::BulkMove,
        )
        .await?;
        if let Some(source) = source {
            patch_mailbox_membership(
                &mail,
                &email_states,
                &account_id,
                MutationTarget::Thread(thread),
                source,
                false,
                AccountOperation::RemoveFromContainer,
            )
            .await?;
        }
        Ok(())
    })
}

pub(crate) fn delete_thread<T: HttpTransport>(
    mail: MailAccount<T>,
    email_states: StateMap,
    account_id: String,
    thread: ThreadId,
    current: Option<ContainerId>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let trash = role_mailbox(&mail, FolderRole::Trash, AccountOperation::BulkDestroy).await?;
        // `role_mailbox` ran against the account the thread id routed to,
        // so it answered a NATIVE mailbox id in that account. Every other
        // container id crossing this boundary - the caller's `current`, the
        // ids `containers_list` mints - is owner-qualified for a foreign
        // account, and `cross_account_container` compares the two owners
        // before anything is sent. Mint the trash id in the thread's own
        // namespace so the comparison sees one account, not a bare id that
        // reads as primary.
        let trash = match super::foreign::owner_of(&thread.0) {
            Some(owner) => ContainerId(super::foreign::encode_foreign(owner, trash.as_str()).0),
            None => ContainerId(trash.into_string()),
        };
        // JMAP mailbox ids are server-issued opaque strings and RFC
        // 8620 leaves case-sensitivity to the server, but two
        // mailboxes that differ only in case is a pathological shape
        // we are happy to misclassify in favor of defensiveness:
        // case-insensitive compare here means a caller who normalized
        // the id elsewhere in their stack still resolves a "thread is
        // already in Trash" branch correctly.
        let already_in_trash = current
            .as_ref()
            .is_some_and(|id| id.0.eq_ignore_ascii_case(&trash.0));
        if already_in_trash {
            let ids = resolve_target(
                &mail,
                MutationTarget::Thread(thread),
                AccountOperation::BulkDestroy,
            )
            .await?;
            destroy_emails(
                &mail,
                &email_states,
                &account_id,
                ids,
                AccountOperation::BulkDestroy,
            )
            .await
        } else {
            patch_mailbox_membership(
                &mail,
                &email_states,
                &account_id,
                MutationTarget::Thread(thread.clone()),
                trash,
                true,
                AccountOperation::BulkMove,
            )
            .await?;
            if let Some(source) = current {
                patch_mailbox_membership(
                    &mail,
                    &email_states,
                    &account_id,
                    MutationTarget::Thread(thread),
                    source,
                    false,
                    AccountOperation::RemoveFromContainer,
                )
                .await?;
            }
            Ok(())
        }
    })
}

async fn patch_mailbox_membership<T: HttpTransport>(
    mail: &MailAccount<T>,
    email_states: &StateMap,
    account_id: &str,
    target: MutationTarget,
    container: ContainerId,
    value: bool,
    op: AccountOperation,
) -> Result<(), AccountError> {
    cross_account_container(&target, &container, op)?;
    let ids = resolve_target(mail, target, op).await?;
    // The owner check above has established that the container names the
    // same account as the target, so the only work left is stripping the
    // qualification the selected account does not use.
    let mailbox = MailboxId::new(wire_id_for_mail(&container.0, mail.id_str()));
    let ids_for_set = ids.clone();
    let mut response =
        send_email_set_with_retry(mail, email_states, account_id, op, move |state| {
            let mut set = EmailSet::new().if_in_state(state.to_string());
            for id in &ids_for_set {
                set.update(id.clone()).mailbox_id(&mailbox, value);
            }
            set
        })
        .await?;
    for id in &ids {
        response.updated(id).map_err(to_acct_err(op))?;
    }
    Ok(())
}

async fn resolve_target<T: HttpTransport>(
    mail: &MailAccount<T>,
    target: MutationTarget,
    op: AccountOperation,
) -> Result<Vec<EmailId>, AccountError> {
    match target {
        MutationTarget::Message(id) => {
            Ok(vec![EmailId::new(wire_id_for_mail(&id.0, mail.id_str()))])
        }
        MutationTarget::Thread(thread) => {
            // Same rule as the `Message` arm above: the account layer has
            // already selected `mail` from the thread id's owner, so strip
            // the qualification only when it names this exact account. An
            // unreachable foreign thread id stays literal and the server
            // reports the honest miss instead of a same-id primary thread.
            let mut response = mail
                .call(
                    ThreadGet::new()
                        .ids([JmapThreadId::new(wire_id_for_mail(
                            &thread.0,
                            mail.id_str(),
                        ))])
                        .properties([crate::thread::Property::EmailIds]),
                )
                .await
                .map_err(to_acct_err(op))?;
            let thread = response.pop().ok_or_else(|| {
                // The operation is the MUTATION being performed, not
                // `HydrateThread`: the thread lookup here only expands a
                // mutation target, and mislabelling it sends the caller a
                // hydration failure for an operation it never issued.
                super::error::into_account_error(
                    crate::Error::IdNotFound(thread.0.clone()),
                    super::error::JmapErrorContext::new(op).with_scope(
                        bifrost_types::ErrorScope::Thread {
                            id: thread.0.clone(),
                        },
                    ),
                )
            })?;
            thread
                .email_ids()
                .ok_or_else(|| {
                    to_acct_err(op)(crate::Error::NotParsable(
                        "Thread/get response omitted requested emailIds".to_string(),
                    ))
                })
                .map(ToOwned::to_owned)
        }
        _ => Err(super::error::unsupported_error(
            op,
            None,
            "JMAP does not support this mutation target",
        )),
    }
}

/// Refuse a container membership patch whose object and whose container
/// belong to different JMAP accounts.
///
/// The account layer routes the `Email/set` by the TARGET's owner but takes
/// the container id as given, and the server resolves both operands inside
/// that one `accountId`. So a bare (primary) container id addressed to a
/// foreign account does not fail: it names whatever mailbox that account
/// holds under the same id, and the message is filed somewhere the caller
/// never asked for with no error reported. Compare owners before anything is
/// sent. A thread target declares its owner the same way a message does -
/// a foreign thread id is owner-qualified in the object namespace, a bare
/// one is primary - so the two arms are symmetric; other target shapes keep
/// `resolve_target`'s `Unsupported` classification and are left alone here.
///
/// `Request(Malformed)` matches bifrost-graph's cross-mailbox `bulk_move`
/// rejection: the caller asked for something one endpoint cannot express.
fn cross_account_container(
    target: &MutationTarget,
    container: &ContainerId,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let (target_id, target_owner) = match target {
        MutationTarget::Message(id) => (id.0.as_str(), super::foreign::owner_of(&id.0)),
        MutationTarget::Thread(thread) => (thread.0.as_str(), super::foreign::owner_of(&thread.0)),
        _ => return Ok(()),
    };
    let container_owner = super::foreign::owner_of(&container.0);
    if target_owner == container_owner {
        return Ok(());
    }
    Err(super::error::cross_account_destination(
        op,
        target_id,
        target_owner,
        &container.0,
        container_owner,
    ))
}

/// The account layer has already selected `mail` from a registered foreign
/// object id. Strip that routing prefix only when it names this exact account;
/// an unreachable foreign id remains literal on the primary route so the
/// server produces the normal not-found response.
fn wire_id_for_mail(id: &str, account_id: &str) -> String {
    match super::foreign::parse_object(id) {
        Some((owner, native)) if owner == account_id => native.to_string(),
        _ => id.to_string(),
    }
}

async fn destroy_emails<T: HttpTransport>(
    mail: &MailAccount<T>,
    email_states: &StateMap,
    account_id: &str,
    ids: impl IntoIterator<Item = EmailId> + Clone + Send + 'static,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let ids_vec = ids.into_iter().collect::<Vec<_>>();
    if ids_vec.is_empty() {
        return Ok(());
    }
    let ids_for_set = ids_vec.clone();
    let mut response =
        send_email_set_with_retry(mail, email_states, account_id, op, move |state| {
            EmailSet::new()
                .if_in_state(state.to_string())
                .destroy(ids_for_set.clone())
        })
        .await?;
    for id in &ids_vec {
        response.destroyed(id).map_err(to_acct_err(op))?;
    }
    Ok(())
}

async fn send_email_set_with_retry<T: HttpTransport, F>(
    mail: &MailAccount<T>,
    email_states: &StateMap,
    account_id: &str,
    op: AccountOperation,
    mut make_set: F,
) -> Result<crate::core::set::SetResponse<Email>, AccountError>
where
    F: FnMut(&str) -> EmailSet,
{
    let mut state = current_or_probe_email_state(mail, email_states, account_id, op).await?;
    let err_fn = to_acct_err(op);
    let response = match mail.call(make_set(&state)).await {
        Ok(response) => response,
        Err(err) => {
            if !super::error::is_state_mismatch(&err) {
                return Err(err_fn(err));
            }
            let fresh = super::mutation::probe_email_state(mail)
                .await
                .map_err(to_acct_err(op))?;
            state_cache::set(email_states, account_id, fresh.clone()).await;
            state = fresh;
            mail.call(make_set(&state)).await.map_err(to_acct_err(op))?
        }
    };
    if !response.new_state().is_empty() {
        state_cache::advance(
            email_states,
            account_id,
            Some(&state),
            response.new_state().to_string(),
        )
        .await;
    }
    Ok(response)
}

async fn current_or_probe_email_state<T: HttpTransport>(
    mail: &MailAccount<T>,
    email_states: &StateMap,
    account_id: &str,
    op: AccountOperation,
) -> Result<String, AccountError> {
    if let Some(state) = state_cache::get(email_states, account_id).await {
        return Ok(state);
    }
    let state = super::mutation::probe_email_state(mail)
        .await
        .map_err(to_acct_err(op))?;
    if let Some(existing) = state_cache::get(email_states, account_id).await {
        return Ok(existing);
    }
    state_cache::set(email_states, account_id, state.clone()).await;
    Ok(state)
}

async fn role_mailbox<T: HttpTransport>(
    mail: &MailAccount<T>,
    role: FolderRole,
    op: AccountOperation,
) -> Result<MailboxId, AccountError> {
    role_mailboxes(mail, &[role], op)
        .await?
        .remove(&role)
        .ok_or_else(|| missing_role_mailbox(op))
}

/// Resolve several role mailboxes from ONE `Mailbox/get`. Roles the
/// account does not expose are simply absent from the map, so the
/// caller decides which of them are required. Batching matters because
/// the submission paths need Drafts and Sent together and
/// `fetch_mailboxes` is an uncached round trip.
async fn role_mailboxes<T: HttpTransport>(
    mail: &MailAccount<T>,
    roles: &[FolderRole],
    op: AccountOperation,
) -> Result<HashMap<FolderRole, MailboxId>, AccountError> {
    let mut found = HashMap::new();
    for mut mailbox in fetch_mailboxes(mail, op).await? {
        let Some(role) = map_role(mailbox.role()) else {
            continue;
        };
        if !roles.contains(&role) {
            continue;
        }
        let id = mailbox.take_id();
        if id.as_str().is_empty() {
            continue;
        }
        found.entry(role).or_insert(id);
    }
    Ok(found)
}

fn missing_role_mailbox(op: AccountOperation) -> AccountError {
    super::error::unsupported_error(op, None, "JMAP required mailbox role not found")
}

async fn fetch_containers<T: HttpTransport>(
    mail: &MailAccount<T>,
    op: AccountOperation,
) -> Result<Vec<Container>, AccountError> {
    Ok(fetch_mailboxes(mail, op)
        .await?
        .into_iter()
        .filter_map(|mailbox| container_from_mailbox(mailbox, None, None))
        .collect())
}

/// The JMAP principals capabilities (RFC 9670) gating owner-email
/// resolution.
const PRINCIPALS_CAPABILITY: &str = "urn:ietf:params:jmap:principals";
const PRINCIPALS_OWNER_CAPABILITY: &str = "urn:ietf:params:jmap:principals:owner";

/// How one foreign account's owner email resolves. Pure decision core of
/// `resolve_owner_emails`, split out so the gate is unit-pinnable.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OwnerEmailPlan {
    /// No email for this account. Also the fail-soft landing for every
    /// error path.
    Skip,
    /// The account advertises `urn:ietf:params:jmap:principals:owner`
    /// with a principal id: resolve via `Principal/get`, and if that
    /// yields nothing, degrade to `name_fallback` when the session name
    /// is itself an address.
    FromPrincipal {
        principal_id: String,
        name_fallback: Option<String>,
    },
    /// No owner principal, but the account's session NAME is an address:
    /// use it directly. Only reachable when the SESSION advertises the
    /// principals capability - the caller gates on that first.
    FromName(String),
}

/// Decide how a foreign account's owner email resolves, given the
/// account-level owner principal id (if advertised) and the account's
/// session name.
///
/// A principal id is always preferred: `Principal/get` is authoritative
/// and the session name is only a label. A principal the server answers
/// for and does not know is the case the fallback exists for, but the
/// fallback applies ONLY when the lookup actually completed - see
/// `PrincipalEmail`. A lookup that failed teaches nothing, and guessing
/// over it would overwrite real ownership with metadata.
fn owner_email_plan(owner_principal_id: Option<&str>, account_name: &str) -> OwnerEmailPlan {
    let name_email = account_name_as_address(account_name);
    if let Some(principal_id) = owner_principal_id {
        return OwnerEmailPlan::FromPrincipal {
            principal_id: principal_id.to_string(),
            name_fallback: name_email,
        };
    }
    match name_email {
        Some(email) => OwnerEmailPlan::FromName(email),
        None => OwnerEmailPlan::Skip,
    }
}

/// Interpret an `Account.name` as an owner address by PARSING it, not
/// by sniffing for an `@`.
///
/// RFC 8620 defines `Account.name` as a user-facing label ("e.g. the
/// email address"); an address is one possible shape, not a contract.
/// A containment test accepted anything with an `@` anywhere in it and
/// stored the whole string, so `Support <support@example.com>` became
/// an owner "address" verbatim.
///
/// Only a bare addr-spec is accepted. Pulling the address out of a
/// `Name <addr>` form is deliberately NOT done: that guesses which
/// address inside a free-form label identifies the owner, and a wrong
/// owner is worse than no owner. Requiring a dotted domain rejects
/// local-only labels that could never route.
fn account_name_as_address(name: &str) -> Option<String> {
    let candidate = name.trim();
    if candidate.is_empty() || candidate.chars().any(char::is_whitespace) {
        return None;
    }
    if candidate.contains(['<', '>', ',', ';', '"', '\\']) {
        return None;
    }
    let (local, domain) = candidate.split_once('@')?;
    if local.is_empty() || domain.is_empty() || domain.contains('@') {
        return None;
    }
    if !domain.contains('.') || domain.starts_with('.') || domain.ends_with('.') {
        return None;
    }
    Some(candidate.to_string())
}

/// Plan owner-email resolution for every foreign account in the session.
///
/// Level ONE of the two-level gate lives here: a session that does not
/// advertise `urn:ietf:params:jmap:principals` yields no plans at all -
/// not even the name fallback. Resolving from the account name on a
/// session with no principals capability would populate owner emails
/// where the legacy behavior left them NULL.
fn owner_email_plans(
    session: &crate::core::session::Session,
    foreign_account_ids: &[&String],
) -> Vec<(String, OwnerEmailPlan)> {
    if !session.has_capability(PRINCIPALS_CAPABILITY) {
        return Vec::new();
    }
    foreign_account_ids
        .iter()
        .filter_map(|account_id| {
            let account = session.account(account_id)?;
            // Level TWO: the individual account's owner capability.
            let owner_principal_id =
                account
                    .capability(PRINCIPALS_OWNER_CAPABILITY)
                    .and_then(|capability| match capability {
                        crate::core::session::Capabilities::PrincipalsOwner(owner) => {
                            owner.principal_id().map(|id| id.as_str().to_string())
                        }
                        _ => None,
                    });
            Some((
                (*account_id).clone(),
                owner_email_plan(owner_principal_id.as_deref(), account.name()),
            ))
        })
        .collect()
}

/// Resolve owner emails for the foreign accounts, once per account.
/// Returns only the resolved entries; every failure lands as an absent
/// key (fail-soft, mandatory - see `containers_list`).
async fn resolve_owner_emails<T: HttpTransport>(
    mail: &MailAccount<T>,
    foreign_mail: &HashMap<String, MailAccount<T>>,
) -> HashMap<String, String> {
    let mut resolved = HashMap::new();
    if foreign_mail.is_empty() {
        return resolved;
    }
    let session = mail.client().session();
    // Deterministic order so the resolution (and its logging, if any is
    // ever added) is stable across calls.
    let mut foreign_ids: Vec<&String> = foreign_mail.keys().collect();
    foreign_ids.sort();
    let principals_account_id = session
        .principals_capabilities()
        .and_then(|capabilities| capabilities.account_id_for_principal())
        .map(|id| id.as_str().to_string());
    for (account_id, plan) in owner_email_plans(&session, &foreign_ids) {
        match plan {
            OwnerEmailPlan::Skip => {}
            OwnerEmailPlan::FromName(email) => {
                resolved.insert(account_id, email);
            }
            OwnerEmailPlan::FromPrincipal {
                principal_id,
                name_fallback,
            } => {
                let outcome =
                    fetch_principal_email(mail, principals_account_id.as_deref(), &principal_id)
                        .await;
                if let Some(email) = owner_email_from_lookup(outcome, name_fallback) {
                    resolved.insert(account_id, email);
                }
            }
        }
    }
    resolved
}

/// Outcome of one owner-principal lookup.
///
/// The distinction that matters is between "the server answered and
/// there is no email" and "we never got an answer". Collapsing both to
/// `None` meant a transport blip or an unimplemented `Principal/get`
/// was indistinguishable from genuine absence, and the name fallback
/// fired on all of them - so a transient failure could overwrite real
/// ownership with a guess derived from a display label.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PrincipalEmail {
    /// Authoritative address from `Principal/get`.
    Resolved(String),
    /// The call succeeded and yielded no address: either the principal
    /// is not one the server knows, or it carries no email property.
    /// There is nothing better to be had, so a parsed account name may
    /// stand in.
    Absent,
    /// The call did not complete. This is not evidence of anything;
    /// never fall back on it, or a blip rewrites ownership.
    Unavailable,
}

/// Combine a principal lookup with the plan's parsed name fallback.
///
/// The whole point of classifying the lookup: only a COMPLETED one
/// licenses the fallback. `Absent` means the server answered and there
/// is no address, so the account name is the best remaining evidence.
/// `Unavailable` means we never got an answer, and substituting a guess
/// there would overwrite authoritative ownership with metadata on every
/// transient failure.
fn owner_email_from_lookup(
    outcome: PrincipalEmail,
    name_fallback: Option<String>,
) -> Option<String> {
    match outcome {
        PrincipalEmail::Resolved(email) => Some(email),
        PrincipalEmail::Absent => name_fallback,
        PrincipalEmail::Unavailable => None,
    }
}

/// `Principal/get` for one owner principal, against the session's
/// principals account when it names one (falling back to the mail
/// account's id, matching the legacy default-account behavior).
///
/// Still fail-soft - no error escapes - but the failure is now
/// classified rather than flattened.
async fn fetch_principal_email<T: HttpTransport>(
    mail: &MailAccount<T>,
    principals_account_id: Option<&str>,
    principal_id: &str,
) -> PrincipalEmail {
    let account = match principals_account_id {
        Some(id) => crate::account::Account::new(mail.client().clone(), id),
        None => mail.clone(),
    };
    let get = crate::principal::PrincipalGet::new()
        .ids([crate::principal::PrincipalId::new(principal_id)])
        .properties([
            crate::principal::Property::Email,
            crate::principal::Property::Name,
        ]);
    match account.call(get).await {
        Ok(response) => response
            .into_list()
            .into_iter()
            .next()
            .and_then(|principal| principal.email().map(String::from))
            .map_or(PrincipalEmail::Absent, PrincipalEmail::Resolved),
        // Transport failure, method error, unimplemented method: we
        // learned nothing about who owns this account.
        Err(_) => PrincipalEmail::Unavailable,
    }
}

/// Enumerate every foreign (shared/delegate) account's mailboxes as
/// `Shared`-namespace containers.
///
/// Each foreign account is independent, so a per-account `Mailbox/get`
/// failure degrades to a `SkippedScope` (naming the foreign accountId,
/// carrying the classified error) plus the remaining containers -
/// matching the shape open-time seeding uses for an unreachable share.
/// One unreachable share must not blank the whole sidebar.
async fn fetch_foreign_containers<T: HttpTransport>(
    foreign_mail: &HashMap<String, MailAccount<T>>,
    owner_emails: &HashMap<String, String>,
    op: AccountOperation,
) -> (Vec<Container>, Vec<SkippedScope>) {
    let mut containers = Vec::new();
    let mut skipped = Vec::new();
    // Deterministic order so the projection is stable across calls.
    let mut account_ids: Vec<&String> = foreign_mail.keys().collect();
    account_ids.sort();
    for account_id in account_ids {
        let mail = &foreign_mail[account_id];
        let owner_email = owner_emails.get(account_id).map(String::as_str);
        match fetch_mailboxes(mail, op).await {
            Ok(mailboxes) => containers.extend(mailboxes.into_iter().filter_map(|mailbox| {
                container_from_mailbox(mailbox, Some(account_id), owner_email)
            })),
            Err(error) => skipped.push(SkippedScope {
                scope: bifrost_types::ErrorScope::Mailbox {
                    id: account_id.clone(),
                },
                error,
            }),
        }
    }
    (containers, skipped)
}

async fn fetch_mailboxes<T: HttpTransport>(
    mail: &MailAccount<T>,
    op: AccountOperation,
) -> Result<Vec<Mailbox>, AccountError> {
    Ok(mail
        .call(MailboxGet::new().properties([
            MailboxProperty::Id,
            MailboxProperty::Name,
            MailboxProperty::ParentId,
            MailboxProperty::Role,
            MailboxProperty::MyRights,
            MailboxProperty::IsSubscribed,
        ]))
        .await
        .map_err(to_acct_err(op))?
        .into_list())
}

/// Project one `Mailbox` onto a `Container`.
///
/// `owner_account` is `Some(accountId)` for a foreign (shared/delegate)
/// account's mailbox. In that case the container's `native_id` is
/// `encode_foreign(accountId, mailboxId)` - byte-identical to the
/// `MembershipScope::Folder` qualification the foreign inventory and
/// hydration stamp on that account's messages, which is what lets the
/// consumer join a message's membership to its container. (The account's
/// SYNC scope is coarser: one account-level `Folder` scope per share,
/// since `Email/changes` cannot be filtered by mailbox.) `owner_local_id`
/// keeps the bare mailbox id for calls made against the owner's own
/// account. The parent is re-encoded in the same namespace so a foreign
/// child never points at a primary mailbox that happens to share the
/// parent's id.
fn container_from_mailbox(
    mut mailbox: Mailbox,
    owner_account: Option<&str>,
    owner_email: Option<&str>,
) -> Option<Container> {
    let id = mailbox.take_id();
    if id.as_str().is_empty() {
        return None;
    }
    let local = id.into_string();
    let native = match owner_account {
        Some(account) => super::foreign::encode_foreign(account, &local).0,
        None => local.clone(),
    };
    let parent = mailbox.parent_id().map(|parent| {
        let parent = parent.to_string();
        ContainerId(match owner_account {
            Some(account) => super::foreign::encode_foreign(account, &parent).0,
            None => parent,
        })
    });
    let role = map_role(mailbox.role());
    Some(
        Container::new(
            ContainerId(native.clone()),
            ContainerKind::Folder,
            role,
            Provenance {
                provider: bifrost_types::ProtocolKind::Jmap,
                kind: ContainerKind::Folder,
                native,
            },
            mailbox.name().unwrap_or("").to_string(),
            parent,
        )
        // JMAP mailboxes carry no container color, so `style` keeps its
        // `Container::new` default.
        //
        // A role-bearing mailbox is JMAP's native system notion; for
        // folder-shaped JMAP that is exactly what `role` already captures,
        // so there is no hidden split to surface.
        .with_system(role.is_some())
        // JMAP's `Mailbox.myRights` / `Mailbox.isSubscribed` carry the
        // per-folder ACL and subscription state the shared-mailbox sidebar
        // gates submit on.
        .with_rights(mailbox.my_rights().map(rights_from_mailbox))
        .with_subscription(mailbox.is_subscribed())
        .with_namespace(match owner_account {
            Some(_) => ContainerNamespace::Shared,
            None => ContainerNamespace::Personal,
        })
        .with_owner(owner_account.map(|account| bifrost_types::MailboxId(account.to_string())))
        // Best-effort metadata resolved once per foreign account by
        // `resolve_owner_emails`; `None` for a personal mailbox and for
        // every unresolvable owner (fail-soft).
        .with_owner_email(owner_email.map(str::to_string))
        .with_owner_local_id(owner_account.map(|_| local)),
    )
}

/// Map the JMAP `Mailbox/myRights` object onto the unified
/// [`ContainerRights`]. JMAP always emits every member as a concrete
/// boolean when it emits the object at all, so each maps to `Some(_)`.
fn rights_from_mailbox(rights: &MailboxRights) -> ContainerRights {
    ContainerRights {
        may_read_items: Some(rights.may_read_items()),
        may_add_items: Some(rights.may_add_items()),
        may_remove_items: Some(rights.may_remove_items()),
        may_set_seen: Some(rights.may_set_seen()),
        may_set_keywords: Some(rights.may_set_keywords()),
        may_create_child: Some(rights.may_create_child()),
        may_rename: Some(rights.may_rename()),
        may_delete: Some(rights.may_delete()),
        may_submit: Some(rights.may_submit()),
    }
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

async fn search_email_ids<T: HttpTransport>(
    mail: &MailAccount<T>,
    request: SearchRequest,
    collapse_threads: bool,
    op: AccountOperation,
) -> Result<Page<EmailId>, AccountError> {
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
    let response = mail.call(query_request).await.map_err(to_acct_err(op))?;
    let total = response.total().and_then(|v| u64::try_from(v).ok());
    let ids = response.into_ids();
    let next_cursor = if ids.len() == usize::try_from(limit).unwrap_or(usize::MAX) {
        let next = position
            .checked_add(i32::try_from(ids.len()).map_err(|_| schema_incompatible_search_cursor())?)
            .ok_or(schema_incompatible_search_cursor())?;
        Some(next.to_string().into_bytes())
    } else {
        None
    };
    Ok(Page {
        items: ids,
        next_cursor,
        estimated_total: total,
        failed_ids: Vec::new(),
        skipped_scopes: Vec::new(),
    })
}

fn decode_position(cursor: Option<&[u8]>) -> Result<i32, AccountError> {
    match cursor {
        None => Ok(0),
        Some(bytes) => {
            let text =
                std::str::from_utf8(bytes).map_err(|_| schema_incompatible_search_cursor())?;
            text.parse::<i32>()
                .map_err(|_| schema_incompatible_search_cursor())
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

async fn build_email_create_from_send<T: HttpTransport>(
    mail: &MailAccount<T>,
    request: bifrost_types::SendRequest,
    mailbox: MailboxId,
) -> Result<crate::email::EmailCreate, AccountError> {
    // RFC 8098 read receipt: targets the resolved sender. JMAP carries
    // arbitrary headers as structured Email properties, so this becomes a
    // `Disposition-Notification-To` address header on the create. Captured
    // before `request` is consumed field-by-field below.
    let read_receipt_to = request
        .request_read_receipt
        .then(|| request.from.clone())
        .flatten();
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
    let mut create =
        build_email_create_from_draft(mail, patch, mailbox, AccountOperation::Send).await?;
    if let Some(addr) = read_receipt_to {
        create.header(
            crate::email::Header {
                name: "Disposition-Notification-To".to_string(),
                form: crate::email::HeaderForm::Addresses,
                all: false,
            },
            crate::email::HeaderValue::AsAddresses(vec![address_to_jmap(addr)]),
        );
    }
    Ok(create)
}

async fn build_email_create_from_draft<T: HttpTransport>(
    mail: &MailAccount<T>,
    patch: bifrost_types::DraftPatch,
    mailbox: MailboxId,
    op: AccountOperation,
) -> Result<crate::email::EmailCreate, AccountError> {
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
    apply_body_to_create(mail, &mut create, body_patch, op).await?;
    Ok(create)
}

async fn apply_body_to_create<T: HttpTransport>(
    mail: &MailAccount<T>,
    create: &mut crate::email::EmailCreate,
    patch: bifrost_types::DraftPatch,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let body = build_body(
        mail,
        patch.body_text.flatten(),
        patch.body_html.flatten(),
        patch.attachments_inline.unwrap_or_default(),
        patch.attachments_uploaded.unwrap_or_default(),
        op,
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

async fn apply_draft_patch_to_email_patch<T: HttpTransport>(
    mail: &MailAccount<T>,
    email_patch: &mut EmailPatch,
    patch: bifrost_types::DraftPatch,
    op: AccountOperation,
) -> Result<(), AccountError> {
    if let Some(from) = patch.from {
        match from {
            Some(value) => {
                email_patch
                    .raw_property("from", &vec![address_to_jmap(value)])
                    .map_err(crate::Error::from)
                    .map_err(to_acct_err(op))?;
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
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
    }
    if let Some(cc) = patch.cc {
        let values = cc.into_iter().map(address_to_jmap).collect::<Vec<_>>();
        email_patch
            .raw_property("cc", &values)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
    }
    if let Some(bcc) = patch.bcc {
        let values = bcc.into_iter().map(address_to_jmap).collect::<Vec<_>>();
        email_patch
            .raw_property("bcc", &values)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
    }
    if let Some(reply_to) = patch.reply_to {
        let values = reply_to
            .into_iter()
            .map(address_to_jmap)
            .collect::<Vec<_>>();
        email_patch
            .raw_property("replyTo", &values)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
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
                .map_err(to_acct_err(op))?;
        } else {
            email_patch.null_property("inReplyTo");
        }
    }
    if let Some(references) = patch.references {
        email_patch
            .raw_property("references", &references)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
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
            op,
        )
        .await?;
        if let Some(structure) = body.body_structure {
            email_patch
                .raw_property("bodyStructure", &structure)
                .map_err(crate::Error::from)
                .map_err(to_acct_err(op))?;
        }
        email_patch
            .raw_property("bodyValues", &body.body_values)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
        email_patch
            .raw_property("textBody", &body.text_body)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
        email_patch
            .raw_property("htmlBody", &body.html_body)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
        email_patch
            .raw_property("attachments", &body.attachments)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
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

async fn build_body<T: HttpTransport>(
    mail: &MailAccount<T>,
    text: Option<String>,
    html: Option<String>,
    inline: Vec<bifrost_types::AttachmentInline>,
    uploaded: Vec<bifrost_types::AttachmentHandle>,
    op: AccountOperation,
) -> Result<BuiltBody, AccountError> {
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
            .map_err(to_acct_err(op))?;
        let mut part = EmailBodyPart::new()
            .with_blob_id(blob.blob_id)
            .with_name(attachment.filename)
            .with_content_type(attachment.mime);
        // Carry the Content-ID so a `cid:` reference in the HTML body
        // resolves to this part. JMAP's `cid` property is the bare token
        // (no angle brackets); strip any the caller supplied.
        if let Some(cid) = attachment.content_id.as_deref().map(|cid| {
            cid.trim()
                .trim_matches(|c| c == '<' || c == '>')
                .to_string()
        }) && !cid.is_empty()
        {
            part = part.with_content_id(cid);
        }
        attachments.push(part);
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

/// Format an absolute instant as RFC 3339 / ISO 8601 UTC for the SMTP
/// FUTURERELEASE `holduntil` envelope parameter.
fn rfc3339(at: SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(at).to_rfc3339()
}

/// A scheduled JMAP send needs an envelope mailFrom (explicit `from` +
/// recipients) to carry the hold parameter. Maps to `Request(Malformed)`.
fn scheduled_requires_envelope() -> AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
        bifrost_types::Cause::Request(bifrost_types::RequestCause::Malformed {
            detail: bifrost_types::DiagnosticText::user_safe(
                "Scheduled send requires an explicit from address and at least one recipient.",
            ),
        }),
    )
    .protocol(bifrost_types::Protocol::Jmap)
    .operation(AccountOperation::Send)
    .try_build()
    .expect("valid account error classification")
}

/// Cancel a scheduled JMAP submission by id: `EmailSubmission/set`
/// update setting `undoStatus: canceled`.
pub(crate) fn cancel_scheduled_send<T: HttpTransport>(
    submission_account: MailAccount<T>,
    handle: ObjectId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let submission_id = crate::email_submission::EmailSubmissionId::from(handle.0.as_str());
        let mut set = EmailSubmissionSet::new();
        set.update(submission_id.clone())
            .undo_status(UndoStatus::Canceled);
        let mut batch = submission_account.build();
        let handle_ref = batch
            .call(set)
            .map_err(to_acct_err(AccountOperation::CancelScheduledSend))?;
        let mut response = batch
            .send()
            .await
            .map_err(to_acct_err(AccountOperation::CancelScheduledSend))?;
        let mut set_response = response
            .get(&handle_ref)
            .map_err(to_acct_err(AccountOperation::CancelScheduledSend))?;
        set_response
            .updated(&submission_id)
            .map_err(to_acct_err(AccountOperation::CancelScheduledSend))?;
        Ok(())
    })
}

/// Reschedule a scheduled JMAP submission. JMAP has no in-place
/// reschedule: cancel the existing submission and create a new one
/// referencing the same `emailId` with the new `holduntil`. Returns the
/// new submission id.
pub(crate) fn reschedule_send<T: HttpTransport>(
    submission_account: MailAccount<T>,
    max_delayed_send: usize,
    handle: ObjectId,
    scheduled: SystemTime,
) -> AccountFuture<Result<ObjectId, AccountError>> {
    Box::pin(async move {
        let window = std::time::Duration::from_secs(max_delayed_send as u64);
        bifrost_types::validate_scheduled(scheduled, Some(window))?;

        let submission_id = crate::email_submission::EmailSubmissionId::new(handle.0.as_str());

        // Fetch the existing submission to recover its emailId and
        // envelope (the mailFrom we must restamp with the new hold).
        let mut existing = submission_account
            .call(crate::email_submission::EmailSubmissionGet::new().ids([submission_id.clone()]))
            .await
            .map_err(to_acct_err(AccountOperation::RescheduleSend))?;
        let existing = existing
            .pop()
            .ok_or_else(|| reschedule_not_found(&handle.0))?;
        let email_id = existing
            .email_id()
            .cloned()
            .ok_or_else(|| reschedule_missing_field("the scheduled submission has no emailId"))?;
        let mail_from_email = existing
            .mail_from()
            .ok_or_else(|| reschedule_missing_field("the scheduled submission has no envelope"))?
            .email()
            .to_string();
        let rcpt_to: Vec<String> = existing
            .rcpt_to()
            .unwrap_or(&[])
            .iter()
            .map(|a| a.email().to_string())
            .collect();

        // Cancel the old submission and create the replacement in one
        // batch.
        let mut set = EmailSubmissionSet::new();
        set.update(submission_id.clone())
            .undo_status(UndoStatus::Canceled);
        {
            let submit = set.create_with_id(SUBMISSION_CREATE_ID);
            submit.undo_status(UndoStatus::Final);
            submit.email_id(email_id);
            let mail_from = SubmissionAddress::new(mail_from_email)
                .with_parameter("holduntil", Some(rfc3339(scheduled)));
            submit.envelope(mail_from, rcpt_to.into_iter().map(SubmissionAddress::new));
        }

        let mut batch = submission_account.build();
        let handle_ref = batch
            .call(set)
            .map_err(to_acct_err(AccountOperation::RescheduleSend))?;
        let mut response = batch
            .send()
            .await
            .map_err(to_acct_err(AccountOperation::RescheduleSend))?;
        let mut set_response = response
            .get(&handle_ref)
            .map_err(to_acct_err(AccountOperation::RescheduleSend))?;
        // Verify the cancel of the OLD submission succeeded BEFORE
        // accepting the new one. A rejected cancel (e.g. `cannotUnsend`,
        // the relay already released the deferred message) combined with
        // an accepted create would leave two live submissions for the
        // same email - a double-send. The cancel and create ride in one
        // `EmailSubmission/set`, so a partial outcome is possible;
        // failing the whole reschedule on a failed cancel is the
        // double-send guard (mirrors `cancel_scheduled_send`, which
        // checks `updated`). The new submission, if any, is left in place
        // but the caller is told the reschedule failed and must
        // reconcile.
        set_response
            .updated(&submission_id)
            .map_err(to_acct_err(AccountOperation::RescheduleSend))?;
        let mut new_submission = set_response
            .created(SUBMISSION_CREATE_ID)
            .map_err(to_acct_err(AccountOperation::RescheduleSend))?;
        Ok(ObjectId(new_submission.take_id().into_string()))
    })
}

fn reschedule_missing_field(detail: &'static str) -> AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::MissingField),
        bifrost_types::Cause::Wire(bifrost_types::WireCause::MalformedResponse {
            protocol: bifrost_types::Protocol::Jmap,
            detail: Some(bifrost_types::DiagnosticText::support_only(detail)),
        }),
    )
    .protocol(bifrost_types::Protocol::Jmap)
    .operation(AccountOperation::RescheduleSend)
    .try_build()
    .expect("valid account error classification")
}

fn reschedule_not_found(id: &str) -> AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::NotFound(bifrost_types::ResourceKind::Message),
        bifrost_types::Cause::Request(bifrost_types::RequestCause::NotFound {
            what: bifrost_types::ResourceKind::Message,
            id: Some(id.to_string()),
        }),
    )
    .protocol(bifrost_types::Protocol::Jmap)
    .operation(AccountOperation::RescheduleSend)
    .try_build()
    .expect("valid account error classification")
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
        importance: if flags.contains("$important") {
            Importance::High
        } else {
            Importance::Normal
        },
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

#[cfg(test)]
mod tests {
    use super::*;

    fn mailbox_json(id: &str, parent: Option<&str>, rights: serde_json::Value) -> Mailbox {
        let mut value = serde_json::json!({
            "id": id,
            "name": "Reports",
            "myRights": rights,
            "isSubscribed": true,
        });
        if let Some(parent) = parent {
            value["parentId"] = serde_json::Value::String(parent.to_string());
        }
        serde_json::from_value(value).expect("mailbox deserializes")
    }

    // A foreign-account hydration must come back in the SAME id namespace
    // the foreign inventory minted, or the consumer's follow-up blob read
    // and container join both address the primary account.
    #[test]
    fn foreign_hydration_requalifies_message_container_and_blob_ids() {
        let mut id = ObjectId("M1".to_string());
        let mut thread_id = ThreadId("T1".to_string());
        let mut containers = vec![ContainerId("inbox".to_string())];
        let mut attachments = vec![BlobHandle {
            id: BlobId("B1".to_string()),
            size: None,
            content_type: None,
            digest: None,
            capabilities: BlobCapabilities {
                supports_range: false,
                supports_parallel: false,
                digest_available_pre_download: false,
                encoding: BlobEncoding::Raw8Bit,
            },
        }];

        qualify_foreign_message_ids(
            &mut id,
            Some(&mut thread_id),
            &mut containers,
            &mut attachments,
            "acct-9",
        );

        // Object ids (message, blob) ride the OBJECT namespace, which
        // `open_blob` / `get_stream` decode.
        assert_eq!(id.0, super::super::foreign::encode_object("acct-9", "M1"));
        // The thread id rides the same namespace: it is what the consumer
        // hands to `thread_hydrate` and `MutationTarget::Thread`, and both
        // expand it through an accountId-scoped `Thread/get`.
        assert_eq!(
            thread_id.0,
            super::super::foreign::encode_object("acct-9", "T1")
        );
        assert_eq!(
            attachments[0].id.0,
            super::super::foreign::encode_object("acct-9", "B1")
        );
        // A container id is a mailbox id, so it rides the FOLDER namespace
        // `containers_list` and the qualified memberships key on -
        // byte-identical, or the join fails.
        assert_eq!(
            containers[0].0,
            super::super::foreign::encode_foreign("acct-9", "inbox").0
        );
    }

    /// The routing decision behind every thread-keyed door.
    ///
    /// A bare id is PRIMARY by construction (only foreign ids are ever
    /// encoded), a qualified id names its share, and a qualified id for a
    /// share that has gone away deliberately does NOT resolve to a
    /// registered account - it rides the primary route with its literal
    /// form so the server reports the miss, rather than being stripped
    /// into a bare native id that could collide with a real primary
    /// thread.
    #[test]
    fn a_thread_id_declares_the_account_that_must_expand_it() {
        let registered = |account: &str| account == "shared";

        let foreign = ThreadId(super::super::foreign::encode_object("shared", "T9"));
        assert_eq!(
            thread_owner(&foreign, registered),
            Some("shared".to_string())
        );

        assert_eq!(thread_owner(&ThreadId("T9".to_string()), registered), None);

        let departed = ThreadId(super::super::foreign::encode_object("revoked", "T9"));
        assert_eq!(thread_owner(&departed, registered), None);
        // ...and the primary route keeps it literal, so the wire id can
        // name no real primary thread.
        assert_eq!(wire_id_for_mail(&departed.0, "primary"), departed.0);
    }

    /// The qualification round-trip a thread-keyed door performs: qualify
    /// at the projection site, decode at the door, and the wire id is the
    /// native id the owning account issued.
    #[test]
    fn a_qualified_thread_id_round_trips_to_its_native_wire_form() {
        let qualified = super::super::foreign::encode_object("shared", "T9");
        assert_eq!(wire_id_for_mail(&qualified, "shared"), "T9");
        // Byte-stable: re-qualifying the decoded native id reproduces it.
        assert_eq!(
            super::super::foreign::encode_object("shared", &wire_id_for_mail(&qualified, "shared")),
            qualified
        );
        // A bare (primary) thread id is untouched on the primary route.
        assert_eq!(wire_id_for_mail("T9", "primary"), "T9");
    }

    /// A thread target answers the cross-account question the same way a
    /// message target does. Before this, threads asserted primary
    /// ownership unconditionally, so a foreign thread paired with a bare
    /// primary container passed the guard.
    #[test]
    fn a_thread_target_and_its_container_must_name_the_same_account() {
        let foreign_thread = MutationTarget::Thread(ThreadId(
            super::super::foreign::encode_object("shared", "T9"),
        ));
        let foreign_container =
            ContainerId(super::super::foreign::encode_foreign("shared", "inbox").0);
        let primary_container = ContainerId("inbox".to_string());

        assert!(
            cross_account_container(
                &foreign_thread,
                &foreign_container,
                AccountOperation::BulkMove
            )
            .is_ok()
        );
        let err = cross_account_container(
            &foreign_thread,
            &primary_container,
            AccountOperation::BulkMove,
        )
        .expect_err("a foreign thread cannot be filed into a primary mailbox id");
        assert_eq!(
            err.kind(),
            &bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );

        // The mirror case: a primary thread and a foreign container.
        let primary_thread = MutationTarget::Thread(ThreadId("T9".to_string()));
        assert!(
            cross_account_container(
                &primary_thread,
                &primary_container,
                AccountOperation::BulkMove
            )
            .is_ok()
        );
        assert!(
            cross_account_container(
                &primary_thread,
                &foreign_container,
                AccountOperation::BulkMove
            )
            .is_err()
        );
    }

    // A primary (bare) id is left alone: one logical object, one wire form.
    #[test]
    fn primary_hydration_ids_are_not_qualified() {
        let route = super::super::hydrate::route_for_id(&ObjectId("M1".to_string()), |_| true);
        assert_eq!(route, super::super::hydrate::HydrationRoute::Primary);
        assert_eq!(super::super::foreign::native_object("M1"), "M1");
    }

    fn full_rights() -> serde_json::Value {
        serde_json::json!({
            "mayReadItems": true,
            "mayAddItems": true,
            "mayRemoveItems": true,
            "maySetSeen": true,
            "maySetKeywords": true,
            "mayCreateChild": true,
            "mayRename": true,
            "mayDelete": true,
            "maySubmit": true,
        })
    }

    #[test]
    fn foreign_mailbox_projects_as_shared_namespaced_container() {
        let container = container_from_mailbox(
            mailbox_json("mbx-12", Some("mbx-1"), full_rights()),
            Some("acct-9"),
            Some("owner@example.test"),
        )
        .expect("container");

        assert_eq!(container.namespace, ContainerNamespace::Shared);
        assert_eq!(container.owner_email.as_deref(), Some("owner@example.test"));
        assert_eq!(
            container.owner,
            Some(bifrost_types::MailboxId("acct-9".to_string()))
        );
        // `native_id` is the per-account namespaced form (what the cursor
        // scope keys on); `owner_local_id` is the bare mailbox id.
        assert_eq!(
            container.native_id,
            super::super::foreign::encode_foreign("acct-9", "mbx-12").0
        );
        assert_eq!(container.owner_local_id.as_deref(), Some("mbx-12"));
        // The parent is re-encoded in the same namespace, so a foreign child
        // never points at a same-id primary mailbox.
        assert_eq!(
            container.parent,
            Some(ContainerId(
                super::super::foreign::encode_foreign("acct-9", "mbx-1").0
            ))
        );
        // JMAP rights still project (they already did) and JMAP folders
        // carry no content class.
        assert_eq!(
            container.rights.as_ref().and_then(|r| r.may_submit),
            Some(true)
        );
        assert!(container.content_class.is_none());
    }

    /// The container's `native_id` must be byte-identical to the
    /// `MembershipScope::Folder` qualification the foreign inventory and
    /// hydration stamp on the same mailbox - that identity is the join
    /// key between a message's membership and its container. (The
    /// share's SYNC scope is coarser: one account-level `Folder` scope
    /// per account, pinned in `factory.rs`.)
    #[test]
    fn foreign_container_native_id_matches_qualified_membership() {
        let container = container_from_mailbox(
            mailbox_json("mbx-12", None, full_rights()),
            Some("acct-9"),
            None,
        )
        .expect("container");
        let mut memberships = vec![bifrost_types::MembershipScope::Mailbox(
            bifrost_types::MailboxId("mbx-12".to_string()),
        )];
        super::super::inventory::qualify_foreign_memberships(
            &mut memberships,
            &bifrost_types::MailboxId("acct-9".to_string()),
        );
        assert!(
            memberships.contains(&bifrost_types::MembershipScope::Folder(
                bifrost_types::FolderId(container.native_id.clone())
            )),
            "container {:?} does not join the qualified memberships {memberships:?}",
            container.native_id
        );
    }

    #[test]
    fn primary_mailbox_stays_personal_and_unqualified() {
        let container = container_from_mailbox(
            mailbox_json("mbx-12", Some("mbx-1"), full_rights()),
            None,
            None,
        )
        .expect("container");
        assert_eq!(container.namespace, ContainerNamespace::Personal);
        assert!(container.owner.is_none());
        assert!(container.owner_email.is_none());
        assert!(container.owner_local_id.is_none());
        assert_eq!(container.native_id, "mbx-12");
        assert_eq!(container.parent, Some(ContainerId("mbx-1".to_string())));
    }

    // -- owner-email resolution (the two-level RFC 9670 gate) ------------

    #[test]
    fn owner_email_plan_prefers_principal_over_name() {
        // A present principal id always routes through `Principal/get`
        // first, even when the account name looks like an address.
        assert_eq!(
            owner_email_plan(Some("p-1"), "shared@example.test"),
            OwnerEmailPlan::FromPrincipal {
                principal_id: "p-1".to_string(),
                name_fallback: Some("shared@example.test".to_string()),
            }
        );
        assert_eq!(
            owner_email_plan(None, "shared@example.test"),
            OwnerEmailPlan::FromName("shared@example.test".to_string())
        );
        assert_eq!(
            owner_email_plan(None, "Shared Mailbox"),
            OwnerEmailPlan::Skip
        );
    }

    #[test]
    fn unresolvable_principal_degrades_to_the_name_instead_of_blanking() {
        // A principal the server answers for and does not know leaves
        // nothing authoritative, so the plan carries the parsed session
        // name to degrade to. (Only `PrincipalEmail::Absent` actually
        // consumes it; a failed lookup does not.)
        let plan = owner_email_plan(Some("p-unreadable"), "shared@example.test");
        let OwnerEmailPlan::FromPrincipal { name_fallback, .. } = plan else {
            panic!("a present principal id must plan a principal lookup");
        };
        assert_eq!(name_fallback.as_deref(), Some("shared@example.test"));
    }

    #[test]
    fn account_name_must_parse_as_an_address_not_merely_contain_an_at() {
        // RFC 8620 Account.name is a user-facing label. Sniffing for
        // '@' stored display-name forms wholesale as owner addresses.
        assert_eq!(
            account_name_as_address("shared@example.test").as_deref(),
            Some("shared@example.test")
        );
        assert_eq!(
            account_name_as_address("  shared@example.test  ").as_deref(),
            Some("shared@example.test"),
            "surrounding whitespace is trimmed, not a rejection"
        );

        for rejected in [
            "Support <support@example.com>",
            "Shared Mailbox",
            "Ada Lovelace ada@example.test",
            "@example.test",
            "ada@",
            "ada@@example.test",
            "ada@localhost",
            "ada@.example.test",
            "ada@example.",
            "\"quoted\"@example.test",
            "a@b.test, c@d.test",
            "",
            "   ",
        ] {
            assert_eq!(
                account_name_as_address(rejected),
                None,
                "{rejected:?} must not be treated as an owner address"
            );
        }
    }

    #[test]
    fn a_display_name_account_never_becomes_a_fallback() {
        // The plan for an account whose name is a display string must
        // carry no fallback at all, so a completed-but-empty lookup
        // resolves to nothing rather than to a label.
        assert_eq!(
            owner_email_plan(Some("p-1"), "Support <support@example.com>"),
            OwnerEmailPlan::FromPrincipal {
                principal_id: "p-1".to_string(),
                name_fallback: None,
            }
        );
        assert_eq!(
            owner_email_plan(None, "Support <support@example.com>"),
            OwnerEmailPlan::Skip
        );
    }

    #[test]
    fn only_a_completed_lookup_permits_the_name_fallback() {
        // The rule the resolver applies, stated directly: `Absent`
        // (the server answered, no address) may degrade to the parsed
        // name; `Unavailable` (no answer) must not, or a transient
        // failure overwrites real ownership with a guess.
        let fallback = || Some("shared@example.test".to_string());
        assert_eq!(
            owner_email_from_lookup(
                PrincipalEmail::Resolved("owner@example.test".into()),
                fallback()
            )
            .as_deref(),
            Some("owner@example.test"),
            "an authoritative answer always wins over the name"
        );
        assert_eq!(
            owner_email_from_lookup(PrincipalEmail::Absent, fallback()).as_deref(),
            Some("shared@example.test"),
            "a completed lookup with no address may degrade to the name"
        );
        assert_eq!(
            owner_email_from_lookup(PrincipalEmail::Unavailable, fallback()),
            None,
            "a failed lookup must not be papered over with the account name"
        );
        assert_eq!(
            owner_email_from_lookup(PrincipalEmail::Absent, None),
            None,
            "and with no parseable name there is nothing to degrade to"
        );
    }

    #[test]
    fn unresolvable_principal_without_an_address_name_has_nothing_to_fall_back_to() {
        // A non-address name is not an email and must never be guessed
        // into one; the fallback stays empty and the account resolves to
        // no owner email at all.
        assert_eq!(
            owner_email_plan(Some("p-unreadable"), "Shared Mailbox"),
            OwnerEmailPlan::FromPrincipal {
                principal_id: "p-unreadable".to_string(),
                name_fallback: None,
            }
        );
    }

    fn gate_session(with_principals_capability: bool) -> crate::core::session::Session {
        let mut capabilities = serde_json::json!({
            "urn:ietf:params:jmap:core": {
                "maxSizeUpload": 50000000,
                "maxConcurrentUpload": 4,
                "maxSizeRequest": 10000000,
                "maxConcurrentRequests": 4,
                "maxCallsInRequest": 16,
                "maxObjectsInGet": 500,
                "maxObjectsInSet": 500,
                "collationAlgorithms": []
            },
        });
        if with_principals_capability {
            capabilities["urn:ietf:params:jmap:principals"] = serde_json::json!({});
        }
        serde_json::from_value(serde_json::json!({
            "capabilities": capabilities,
            "accounts": {
                "acct-owner": {
                    "name": "Shared Mailbox",
                    "isPersonal": false,
                    "isReadOnly": false,
                    "accountCapabilities": {
                        "urn:ietf:params:jmap:principals:owner": {
                            "accountIdForPrincipal": "acct-principals",
                            "principalId": "p-owner"
                        }
                    }
                },
                "acct-name": {
                    "name": "shared@example.test",
                    "isPersonal": false,
                    "isReadOnly": false,
                    "accountCapabilities": {}
                },
                "acct-plain": {
                    "name": "Shared Mailbox",
                    "isPersonal": false,
                    "isReadOnly": false,
                    "accountCapabilities": {}
                }
            },
            "primaryAccounts": {},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/",
            "downloadUrl": "https://example.test/jmap/download/{accountId}/{blobId}/{name}",
            "uploadUrl": "https://example.test/jmap/upload/{accountId}/",
            "eventSourceUrl": "https://example.test/jmap/es/",
            "state": "s1"
        }))
        .expect("gate session deserializes")
    }

    #[test]
    fn session_without_principals_capability_plans_nothing() {
        // Level ONE of the gate: no session principals capability means no
        // resolution at all - INCLUDING the name fallback. Resolving from
        // the account name here would populate emails where the legacy
        // behavior left them NULL.
        let session = gate_session(false);
        let acct_owner = "acct-owner".to_string();
        let acct_name = "acct-name".to_string();
        let ids = vec![&acct_owner, &acct_name];
        assert!(owner_email_plans(&session, &ids).is_empty());
    }

    #[test]
    fn session_with_principals_capability_plans_per_account() {
        let session = gate_session(true);
        let acct_owner = "acct-owner".to_string();
        let acct_name = "acct-name".to_string();
        let acct_plain = "acct-plain".to_string();
        let acct_unknown = "acct-unknown".to_string();
        let ids = vec![&acct_owner, &acct_name, &acct_plain, &acct_unknown];
        let plans = owner_email_plans(&session, &ids);
        assert_eq!(
            plans,
            vec![
                (
                    "acct-owner".to_string(),
                    // "Shared Mailbox" is not address-shaped, so this
                    // account has no name to degrade to.
                    OwnerEmailPlan::FromPrincipal {
                        principal_id: "p-owner".to_string(),
                        name_fallback: None,
                    }
                ),
                (
                    "acct-name".to_string(),
                    OwnerEmailPlan::FromName("shared@example.test".to_string())
                ),
                ("acct-plain".to_string(), OwnerEmailPlan::Skip),
                // `acct-unknown` is absent from the session and yields no
                // plan at all (fail-soft: absent, never an error).
            ]
        );
    }

    #[test]
    fn importance_high_sets_keyword_others_clear() {
        // High -> set `$important`; Normal/Low -> clear it. One keyword op,
        // never an expand-into-two.
        assert!(importance_sets_important_keyword(Importance::High));
        assert!(!importance_sets_important_keyword(Importance::Normal));
        assert!(!importance_sets_important_keyword(Importance::Low));
    }

    #[test]
    fn resolve_foreign_headers_as_overrides_consumer_from() {
        let identity = bifrost_types::Address::bare("shared@example.test");
        let (from, sender) = resolve_foreign_headers(
            &bifrost_types::SendAs::As(bifrost_types::MailboxId("foreign".to_string())),
            &identity,
            Some(bifrost_types::Address::bare("consumer@example.test")),
            Some(bifrost_types::Address::bare("user@example.test")),
        );
        assert_eq!(from.address, "shared@example.test");
        assert!(sender.is_none());
    }

    #[test]
    fn resolve_foreign_headers_on_behalf_of_honors_consumer_from() {
        let identity = bifrost_types::Address::bare("shared@example.test");
        let (from, sender) = resolve_foreign_headers(
            &bifrost_types::SendAs::OnBehalfOf(bifrost_types::MailboxId("foreign".to_string())),
            &identity,
            Some(bifrost_types::Address::bare("author@example.test")),
            Some(bifrost_types::Address::bare("user@example.test")),
        );
        assert_eq!(from.address, "author@example.test");
        assert_eq!(
            sender.expect("known self address").address,
            "user@example.test"
        );
    }

    #[test]
    fn resolve_foreign_headers_on_behalf_of_omits_unknown_sender() {
        let identity = bifrost_types::Address::bare("shared@example.test");
        let (from, sender) = resolve_foreign_headers(
            &bifrost_types::SendAs::OnBehalfOf(bifrost_types::MailboxId("foreign".to_string())),
            &identity,
            None,
            None,
        );
        assert_eq!(from.address, "shared@example.test");
        assert!(sender.is_none());
    }

    fn identity_rows(json: serde_json::Value) -> Vec<crate::identity::Identity> {
        serde_json::from_value(json).expect("identity rows deserialize")
    }

    #[test]
    fn select_concrete_identity_takes_first_concrete_email() {
        let rows = identity_rows(serde_json::json!([
            {"id": "i0", "name": "Shared", "email": "shared@example.test"},
            {"id": "i1", "email": "other@example.test"},
        ]));
        let picked = select_concrete_identity(rows, false).expect("first concrete identity");
        assert_eq!(picked.id, "i0");
        assert_eq!(picked.email, "shared@example.test");
        assert_eq!(picked.name.as_deref(), Some("Shared"));
    }

    #[test]
    fn select_concrete_identity_skips_wildcard_email() {
        let rows = identity_rows(serde_json::json!([
            {"id": "i0", "email": "*"},
            {"id": "i1", "email": "*@example.test"},
            {"id": "i2", "email": "concrete@example.test"},
        ]));
        let picked = select_concrete_identity(rows, false).expect("concrete over wildcard");
        assert_eq!(picked.id, "i2");
    }

    #[test]
    fn select_concrete_identity_empty_list_rejects_unsupported() {
        let err = select_concrete_identity(Vec::new(), false).expect_err("no identity to send as");
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Send)
        ));
    }

    #[test]
    fn select_concrete_identity_requested_absent_rejects() {
        // A by-id get that returns nothing usable is a distinct rejection
        // detail from the empty-account case, but still `Unsupported(Send)`.
        let rows = identity_rows(serde_json::json!([{"id": "i0", "email": "*"}]));
        let err = select_concrete_identity(rows, true)
            .expect_err("requested identity has no concrete email");
        assert!(matches!(
            err.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Send)
        ));
    }
}
