use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, Checkpoint,
    DiagnosticText, ObjectId, PageBoundary, Protocol, RequestCause, RequestErrorKind, SyncEvent,
};
use bytes::Bytes;

use crate::types::{FetchAttr, MailboxName};

use super::{
    ImapAccount, batch, boxed_receiver_stream, decode_object_id, terminated_event, uid_set_from_u32,
};

enum BlobError {
    Account(AccountError),
    Imap(crate::Error),
    ChannelDropped,
}

impl From<AccountError> for BlobError {
    fn from(value: AccountError) -> Self {
        Self::Account(value)
    }
}

impl From<crate::Error> for BlobError {
    fn from(value: crate::Error) -> Self {
        Self::Imap(value)
    }
}

/// Open a message's assembled RFC822 octets (`BODY.PEEK[]`).
///
/// Mirrors `open_blob` but decodes an `ObjectId` (folder / uidvalidity /
/// uid, no section) and fetches the whole message with no section or
/// partial, tagging transport errors `OpenRawRfc822`.
pub(crate) fn open_raw_rfc822(
    account: ImapAccount,
    message: ObjectId,
) -> bifrost_types::AccountStream<SyncEvent<Bytes>> {
    let (tx, rx) = tokio::sync::mpsc::channel(super::STREAM_CAPACITY);
    let scope_id = message.0.clone();
    tokio::spawn(async move {
        match run_raw(account, message, &tx).await {
            Ok(()) => {
                let _ = tx.send(SyncEvent::Done(None::<Checkpoint>)).await;
            }
            Err(BlobError::ChannelDropped) => {}
            Err(BlobError::Account(err)) => {
                let _ = tx.send(terminated_event(err)).await;
            }
            Err(BlobError::Imap(err)) => {
                let _ = tx
                    .send(terminated_event((
                        err,
                        super::error::ImapErrorContext::operation(AccountOperation::OpenRawRfc822)
                            .with_message_id(scope_id),
                    )))
                    .await;
            }
        }
    });
    boxed_receiver_stream(rx)
}

async fn run_raw(
    account: ImapAccount,
    message: ObjectId,
    tx: &tokio::sync::mpsc::Sender<SyncEvent<Bytes>>,
) -> Result<(), BlobError> {
    let decoded = decode_object_id(&message)?;
    let attr = FetchAttr::BodySection {
        peek: true,
        section: None,
        partial: None,
    };
    run_fetch(
        &account,
        &decoded.folder,
        decoded.uidvalidity,
        decoded.uid,
        attr,
        AccountOperation::OpenRawRfc822,
        tx,
    )
    .await
}

/// Shared selection / UIDVALIDITY recheck / `uid_fetch` core for the
/// blob and raw-message reads. `op` tags the UIDVALIDITY-mismatch error
/// so the caller's operation is preserved.
#[allow(clippy::too_many_arguments)]
async fn run_fetch(
    account: &ImapAccount,
    folder: &MailboxName,
    expected_uidvalidity: u32,
    uid: u32,
    attr: FetchAttr,
    op: AccountOperation,
    tx: &tokio::sync::mpsc::Sender<SyncEvent<Bytes>>,
) -> Result<(), BlobError> {
    let mut conn = account.checkout_for_folder(folder).await?;
    let folder_entry = account.folders.get(folder);
    let cursor = folder_entry.as_ref().and_then(|entry| entry.cursor());
    let shared_owner = folder_entry
        .as_ref()
        .and_then(|entry| entry.shared_owner.clone());
    let selected = match account
        .select_folder(&mut conn, folder, cursor.as_ref(), true)
        .await
    {
        Ok(selected) => selected,
        // A permission denial on a shared folder quarantines just that
        // scope (`ScopeRevoked`) instead of escalating to account-level
        // terminal; a personal folder, or any non-permission failure,
        // flows through the normal mapping.
        Err(err) if shared_owner.is_some() => {
            return Err(BlobError::Account(super::error::shared_folder_error(
                err,
                folder,
                shared_owner.as_ref(),
                super::error::ImapErrorContext::operation(op).with_folder_scope(folder),
            )));
        }
        Err(err) => return Err(err.into()),
    };
    let uidvalidity = selected
        .mailbox
        .uid_validity
        .ok_or_else(|| crate::Error::Protocol("SELECT missing UIDVALIDITY".into()))?;
    if uidvalidity != expected_uidvalidity {
        return Err(BlobError::Account(
            AccountErrorBuilder::new(
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::Malformed {
                    detail: DiagnosticText::support_only("UIDVALIDITY changed before fetch"),
                }),
            )
            .protocol(Protocol::Imap)
            .operation(op)
            // Cursor(Folder(..)), not ErrorScope::Mailbox: the folder
            // producers' documented scope shape (see get.rs / mutate.rs).
            .scope(bifrost_types::ErrorScope::Cursor(
                bifrost_types::CursorScope::Folder(bifrost_types::FolderId(
                    folder.as_str().to_owned(),
                )),
            ))
            .try_build()
            .expect("valid account error classification"),
        ));
    }
    let Some(uid_set) = uid_set_from_u32(&[uid]) else {
        return Ok(());
    };
    let fetches = conn
        .connection()
        .uid_fetch(
            uid_set.as_sequence_set(),
            &[attr],
            account.command_timeout(),
        )
        .await?;
    for fetch in fetches {
        for section in fetch.body_sections {
            if let Some(data) = section.data {
                tx.send(batch(
                    vec![Bytes::from(data)],
                    PageBoundary::Page,
                    None::<Checkpoint>,
                ))
                .await
                .map_err(|_| BlobError::ChannelDropped)?;
            }
        }
    }
    Ok(())
}
