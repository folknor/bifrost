use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, Checkpoint,
    DiagnosticText, ObjectId, PageBoundary, Protocol, RequestCause, RequestErrorKind, SyncEvent,
};
use bytes::Bytes;

use crate::types::{FetchAttr, MailboxName};

use super::{
    ImapAccount, batch, boxed_receiver_stream, decode_object_id, terminated_event, uid_set_from_u32,
};

pub(super) enum BlobError {
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

/// Client-side ceiling on one raw-message read.
///
/// Every other body path carries a budget (hydration 256 MiB, draft fetch
/// 64 MiB) because the response size is the server's to choose: a corrupt
/// or adversarial peer can answer a single `BODY.PEEK[]` with an arbitrary
/// number of octets. This lane streams, so the budget bounds what one
/// message may cost in total rather than what is held at once; crossing it
/// is `Error::FetchLimit`.
const RAW_FETCH_BUDGET: usize = 256 * 1024 * 1024;

/// Open a message's assembled RFC822 octets (`BODY.PEEK[]`).
///
/// Mirrors `open_blob` but decodes an `ObjectId` (folder / uidvalidity /
/// uid, no section) and fetches the whole message with no section or
/// partial, tagging transport errors `OpenRawRfc822`.
///
/// The FETCH is streamed: each response's body sections are forwarded to
/// the consumer as they arrive off the socket, under `RAW_FETCH_BUDGET`.
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
        RAW_FETCH_BUDGET,
        tx,
    )
    .await
}

/// Shared selection / UIDVALIDITY recheck / streaming-FETCH core for the
/// blob and raw-message reads. `op` tags the UIDVALIDITY-mismatch error
/// so the caller's operation is preserved, and `budget` is the client-side
/// ceiling on the octets one read may stream before `Error::FetchLimit`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_fetch(
    account: &ImapAccount,
    folder: &MailboxName,
    expected_uidvalidity: u32,
    uid: u32,
    attr: FetchAttr,
    op: AccountOperation,
    budget: usize,
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
    let attrs = [attr];
    let connection = conn.connection();
    let (mut rx, fetch_fut) = connection.uid_fetch_stream(
        uid_set.as_sequence_set(),
        &attrs,
        account.command_timeout(),
    )?;
    // Forward each body section as it comes off the socket. The bounded
    // receiver is the back-pressure: a slow consumer stalls this drain,
    // which stalls the driver's next `reserve_owned`, which stalls the
    // socket read. Dropping `rx` on the way out (every exit below) is what
    // releases the driver to read through the tagged completion instead of
    // parking on a permit that will never be taken.
    let drain = async {
        let mut streamed: usize = 0;
        let mut outcome: Result<(), BlobError> = Ok(());
        while let Some(item) = rx.recv().await {
            let fetch = match item {
                Ok(fetch) => fetch,
                Err(err) => {
                    outcome = Err(err.into());
                    break;
                }
            };
            let (seq, uid) = (fetch.seq, fetch.uid);
            for section in fetch.body_sections {
                let Some(data) = section.data else { continue };
                streamed = streamed.saturating_add(data.len());
                if streamed > budget {
                    outcome = Err(BlobError::Imap(crate::Error::FetchLimit {
                        estimated: streamed,
                        limit: budget,
                        seq,
                        uid,
                    }));
                    break;
                }
                if tx
                    .send(batch(
                        vec![Bytes::from(data)],
                        PageBoundary::Page,
                        None::<Checkpoint>,
                    ))
                    .await
                    .is_err()
                {
                    outcome = Err(BlobError::ChannelDropped);
                    break;
                }
            }
            if outcome.is_err() {
                break;
            }
        }
        drop(rx);
        outcome
    };
    // The command future is authoritative for the wire outcome, exactly as
    // in inventory / QRESYNC / CONDSTORE: the driver drops the streaming
    // consumer before answering its oneshot, so the receiver closing says
    // nothing about whether the FETCH succeeded. A local drain failure
    // (limit crossed, dropped consumer) still wins, since it describes this
    // caller rather than the server.
    let (fetch_result, drain_result) = tokio::join!(fetch_fut, drain);
    drain_result?;
    fetch_result?;
    Ok(())
}
