use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, BlobHandle, ByteRange, Cause, Checkpoint,
    DiagnosticText, PageBoundary, Protocol, RequestCause, RequestErrorKind, SyncEvent,
};
use bytes::Bytes;

use crate::types::FetchAttr;

use super::{
    ImapAccount, batch, boxed_receiver_stream, decode_blob_id, fatal_event, terminated_event,
    uid_set_from_u32,
};

pub(crate) fn open_blob(
    account: ImapAccount,
    handle: BlobHandle,
) -> bifrost_types::AccountStream<SyncEvent<Bytes>> {
    open_blob_inner(account, handle, None)
}

pub(crate) fn open_blob_range(
    account: ImapAccount,
    handle: BlobHandle,
    range: ByteRange,
) -> bifrost_types::AccountStream<SyncEvent<Bytes>> {
    open_blob_inner(account, handle, Some(range))
}

fn open_blob_inner(
    account: ImapAccount,
    handle: BlobHandle,
    range: Option<ByteRange>,
) -> bifrost_types::AccountStream<SyncEvent<Bytes>> {
    let (tx, rx) = tokio::sync::mpsc::channel(super::STREAM_CAPACITY);
    let scope_id = handle.id.0.clone();
    tokio::spawn(async move {
        match run_blob(account, handle, range, &tx).await {
            Ok(()) => {
                let _ = tx.send(SyncEvent::Done(None::<Checkpoint>)).await;
            }
            Err(BlobError::ChannelDropped) => {
                // Consumer dropped the receiver; stop silently per the
                // streaming output-channel contract.
            }
            Err(BlobError::Account(err)) => {
                let _ = tx.send(terminated_event(err)).await;
            }
            Err(BlobError::Imap(err)) => {
                let _ = tx
                    .send(fatal_event(
                        err,
                        super::error::ImapErrorContext::operation(
                            bifrost_types::AccountOperation::OpenBlob,
                        )
                        .with_message_id(scope_id),
                    ))
                    .await;
            }
        }
    });
    boxed_receiver_stream(rx)
}

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

async fn run_blob(
    account: ImapAccount,
    handle: BlobHandle,
    range: Option<ByteRange>,
    tx: &tokio::sync::mpsc::Sender<SyncEvent<Bytes>>,
) -> Result<(), BlobError> {
    let decoded = decode_blob_id(&handle.id)?;
    if let Some(range) = range {
        if !handle.capabilities.supports_range {
            return Err(BlobError::Account(
                AccountErrorBuilder::new(
                    AccountErrorKind::Request(RequestErrorKind::Malformed),
                    Cause::Request(RequestCause::Malformed {
                        detail: DiagnosticText::support_only(
                            "blob range reads are not supported for this handle",
                        ),
                    }),
                )
                .protocol(Protocol::Imap)
                .operation(bifrost_types::AccountOperation::OpenBlobRange)
                .try_build()
                .expect("valid account error classification"),
            ));
        }
        if let Some(total) = handle.size
            && range.start > total
        {
            return Err(BlobError::Account(
                AccountErrorBuilder::new(
                    AccountErrorKind::Request(RequestErrorKind::Malformed),
                    Cause::Request(RequestCause::Malformed {
                        detail: DiagnosticText::support_only(format!(
                            "blob range start {} exceeds total size {}",
                            range.start, total
                        )),
                    }),
                )
                .protocol(Protocol::Imap)
                .operation(bifrost_types::AccountOperation::OpenBlobRange)
                .try_build()
                .expect("valid account error classification"),
            ));
        }
    }

    let mut conn = account.checkout_for_folder(&decoded.folder).await?;
    let cursor = account
        .folders
        .get(&decoded.folder)
        .and_then(|entry| entry.cursor());
    let selected = account
        .select_folder(&mut conn, &decoded.folder, cursor.as_ref(), true)
        .await?;
    let uidvalidity = selected
        .mailbox
        .uid_validity
        .ok_or_else(|| crate::Error::Protocol("SELECT missing UIDVALIDITY".into()))?;
    if uidvalidity != decoded.uidvalidity {
        return Err(BlobError::Account(
            AccountErrorBuilder::new(
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::Malformed {
                    detail: DiagnosticText::support_only("UIDVALIDITY changed before blob fetch"),
                }),
            )
            .protocol(Protocol::Imap)
            .operation(bifrost_types::AccountOperation::OpenBlob)
            .scope(bifrost_types::ErrorScope::Mailbox {
                id: decoded.folder.as_str().to_owned(),
            })
            .try_build()
            .expect("valid account error classification"),
        ));
    }
    let Some(uid_set) = uid_set_from_u32(&[decoded.uid]) else {
        return Ok(());
    };
    let attr = blob_attr(decoded.section.as_deref(), range, handle.size);
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

fn blob_attr(section: Option<&str>, range: Option<ByteRange>, size: Option<u64>) -> FetchAttr {
    let partial = range.map(|range| {
        let length = range
            .length
            .or_else(|| size.map(|size| size.saturating_sub(range.start)))
            .unwrap_or(u32::MAX as u64);
        (range.start, length)
    });
    FetchAttr::BodySection {
        peek: true,
        section: section.map(str::to_owned),
        partial,
    }
}
