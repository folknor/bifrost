use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, BlobHandle, ByteRange,
    Cause, Checkpoint, DiagnosticText, ObjectId, PageBoundary, Protocol, RequestCause,
    RequestErrorKind, SyncEvent,
};
use bytes::Bytes;

use crate::types::{FetchAttr, MailboxName};

use super::{
    ImapAccount, batch, boxed_receiver_stream, decode_blob_id, decode_object_id, fatal_event,
    terminated_event, uid_set_from_u32,
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

    let Some(attr) = blob_attr(decoded.section.as_deref(), range, handle.size) else {
        tx.send(batch(
            vec![Bytes::new()],
            PageBoundary::Page,
            None::<Checkpoint>,
        ))
        .await
        .map_err(|_| BlobError::ChannelDropped)?;
        return Ok(());
    };
    run_fetch(
        &account,
        &decoded.folder,
        decoded.uidvalidity,
        decoded.uid,
        attr,
        AccountOperation::OpenBlob,
        tx,
    )
    .await
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
                    .send(fatal_event(
                        err,
                        super::error::ImapErrorContext::operation(AccountOperation::OpenRawRfc822)
                            .with_message_id(scope_id),
                    ))
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
            .scope(bifrost_types::ErrorScope::Mailbox {
                id: folder.as_str().to_owned(),
            })
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

fn blob_attr(
    section: Option<&str>,
    range: Option<ByteRange>,
    size: Option<u64>,
) -> Option<FetchAttr> {
    let partial = range.map(|range| {
        let length = range
            .length
            .or_else(|| size.map(|size| size.saturating_sub(range.start)))
            .unwrap_or(u32::MAX as u64);
        (range.start, length)
    });
    if partial.is_some_and(|(_, length)| length == 0) {
        return None;
    }
    Some(FetchAttr::BodySection {
        peek: true,
        section: section.map(str::to_owned),
        partial,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(attr: &FetchAttr) -> (bool, Option<&str>, Option<(u64, u64)>) {
        match attr {
            FetchAttr::BodySection {
                peek,
                section,
                partial,
            } => (*peek, section.as_deref(), *partial),
            other => panic!("expected a body section, got {other:?}"),
        }
    }

    // A blob read must never flip `\Seen`, and a whole-blob read must
    // carry no `<origin.count>` so the server streams the entire section.
    #[test]
    fn blob_attr_without_a_range_peeks_the_whole_section() {
        let attr = blob_attr(Some("2.1"), None, Some(4096)).expect("fetch attr");
        let (peek, section, partial) = parts(&attr);
        assert!(peek, "blob reads must use BODY.PEEK");
        assert_eq!(section, Some("2.1"));
        assert_eq!(partial, None);

        let attr = blob_attr(None, None, None).expect("fetch attr");
        let (_, section, partial) = parts(&attr);
        assert_eq!(section, None, "no section means the whole message");
        assert_eq!(partial, None);
    }

    #[test]
    fn blob_attr_uses_the_explicit_range_length_when_given() {
        let attr = blob_attr(
            None,
            Some(ByteRange {
                start: 100,
                length: Some(50),
            }),
            Some(4096),
        )
        .expect("fetch attr");
        let (peek, _, partial) = parts(&attr);
        assert!(peek);
        assert_eq!(partial, Some((100, 50)));
    }

    // An open-ended range ("from here to the end") has to be turned into
    // an explicit count, because IMAP `<origin.count>` has no
    // "rest of section" form.
    #[test]
    fn blob_attr_derives_the_remaining_length_from_the_known_total() {
        let attr = blob_attr(
            None,
            Some(ByteRange {
                start: 100,
                length: None,
            }),
            Some(4096),
        )
        .expect("fetch attr");
        let (_, _, partial) = parts(&attr);
        assert_eq!(partial, Some((100, 3996)));

        // Start at the very end: saturating, never a wrapped huge count.
        let attr = blob_attr(
            None,
            Some(ByteRange {
                start: 4096,
                length: None,
            }),
            Some(4096),
        );
        assert!(attr.is_none(), "a range at EOF is answered locally");
    }

    #[test]
    fn blob_attr_falls_back_to_a_bounded_count_when_the_size_is_unknown() {
        let attr = blob_attr(
            None,
            Some(ByteRange {
                start: 0,
                length: None,
            }),
            None,
        )
        .expect("fetch attr");
        let (_, _, partial) = parts(&attr);
        assert_eq!(
            partial,
            Some((0, u64::from(u32::MAX))),
            "an unknown total falls back to the u32 partial-count ceiling",
        );
    }

    #[test]
    fn blob_attr_rejects_a_zero_length_partial() {
        let attr = blob_attr(
            None,
            Some(ByteRange {
                start: 10,
                length: Some(0),
            }),
            Some(4096),
        );
        assert!(attr.is_none());
    }
}
