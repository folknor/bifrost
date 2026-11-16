//! Draft editing and attachment upload doors.

use super::body::{
    apply_draft_patch_to_email_patch, build_email_create_from_draft, encode_attachment_handle,
};
use super::containers::role_mailbox;
use super::mail_mutation::{destroy_emails, send_email_set};
use super::{MailAccount, to_acct_err};
use crate::email::{EmailId, EmailPatch, EmailSet};
use crate::sync::error::answer_error;
use crate::sync::state_cache;
use bifrost_types::{AccountError, AccountFuture, AccountOperation, AccountStream, FolderRole};
use bytes::Bytes;
use futures::StreamExt;

pub(crate) fn attachment_upload<C: crate::core::capability::Capability>(
    mail: crate::sync::registry::Slot<C>,
    mut bytes: AccountStream<Result<Bytes, AccountError>>,
    mime: String,
) -> AccountFuture<Result<bifrost_types::AttachmentHandle, AccountError>> {
    Box::pin(async move {
        let state = mail.client().session_state();
        let max = crate::sync::capabilities::upload_size_limit(state.session())?;
        let mut data = Vec::new();
        while let Some(chunk) = bytes.next().await {
            let chunk = chunk?;
            // Check before copying or allocating. Subtraction avoids overflow
            // even when the advertised maximum is usize::MAX.
            if chunk.len() > max - data.len() {
                return Err(to_acct_err(AccountOperation::AttachmentUpload)(
                    crate::Error::UploadSizeLimit {
                        max,
                        size: data.len().saturating_add(chunk.len()),
                    },
                ));
            }
            // Keep geometric growth within the advertised limit.
            let size = data.len() + chunk.len();
            if size > data.capacity() {
                let capacity = data.capacity().saturating_mul(2).max(size).min(max);
                data.reserve_exact(capacity - data.len());
            }
            data.extend_from_slice(&chunk);
        }
        let blob = mail
            .upload(data, Some(&mime))
            .await
            .map_err(to_acct_err(AccountOperation::AttachmentUpload))?;
        Ok(bifrost_types::AttachmentHandle(encode_attachment_handle(
            mail.id_str(),
            blob.blob_id.as_str(),
            Some(&mime),
        )))
    })
}

pub(crate) fn draft_create(
    mail: MailAccount,
    patch: bifrost_types::DraftPatch,
) -> AccountFuture<Result<bifrost_types::Created<bifrost_types::DraftHandle>, AccountError>> {
    let email_states = mail.email_states.clone();

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
        // Advance only across the transition proved by this response.
        let mut response = mail
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::DraftCreate))?;
        email_states
            .observe_set(state_cache::set_transition(
                response.old_state(),
                response.new_state(),
            ))
            .await;
        let email = response
            .created(&create_id)
            .map_err(answer_error(AccountOperation::DraftCreate))?;
        // The draft exists either way: an id-less answer is an unidentified
        // success, not a failure that invites saving it again.
        Ok(crate::sync::error::created_or_unidentified(
            email.id().map(EmailId::as_str),
            AccountOperation::DraftCreate,
            "Email/set",
        )
        .map(bifrost_types::DraftHandle))
    })
}

pub(crate) fn draft_update(
    mail: MailAccount,
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
        let mut response = send_email_set(&mail, AccountOperation::DraftUpdate, move || {
            let mut set = EmailSet::new();
            set.update_item(email_id_for_set.clone(), email_patch.clone());
            set
        })
        .await?;
        response
            .updated(&email_id)
            .map_err(crate::sync::error::answer_error(
                AccountOperation::DraftUpdate,
            ))?;
        Ok(())
    })
}

pub(crate) fn draft_discard(
    mail: MailAccount,
    draft: bifrost_types::DraftHandle,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        destroy_emails(
            &mail,
            [EmailId::new(draft.0)],
            AccountOperation::DraftDiscard,
        )
        .await
    })
}
