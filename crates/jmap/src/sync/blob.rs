use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountErrorBuilder, AccountErrorKind, AccountOperation, AccountStream, Batch, BlobHandle,
    ByteRange, Cause, ObjectId, PageBoundary, Protocol, RequestCause, ResourceKind, SyncEvent,
};

use crate::blob::BlobRef;
use crate::client::Client;
use crate::core::id::{AccountId, BlobId};
use crate::core::transport::HttpTransport;
use crate::email::{EmailGet, EmailId, Property};

type MailAccount<T> = crate::account::Account<T>;

/// Resolve the JMAP `accountId` a foreign-qualified id belongs to.
///
/// Blob download is accountId-scoped (`/download/{accountId}/{blobId}`), so
/// a foreign (shared/delegate) blob must be requested under its OWNING
/// account - the primary account's download URL would 404 on a blob it does
/// not own. `Some((accountId, native))` when the id was qualified by the
/// foreign inventory projection AND that account is still reachable;
/// `None` for a primary id (or an id naming an account that vanished, which
/// falls back to the primary account so the miss surfaces as a real 404
/// rather than a fabricated local error). Pure so the selection is
/// unit-pinnable without a live session.
pub(crate) fn foreign_split<F>(id: &str, is_registered: F) -> Option<(&str, &str)>
where
    F: Fn(&str) -> bool,
{
    super::foreign::parse_object(id).filter(|(account, _)| is_registered(account))
}

pub(crate) fn open<T: HttpTransport>(
    client: Client<T>,
    account_id: AccountId,
    foreign_accounts: Arc<HashMap<String, MailAccount<T>>>,
    handle: BlobHandle,
) -> AccountStream<SyncEvent<bytes::Bytes>> {
    Box::pin(async_stream::stream! {
        let started = Instant::now();
        // A foreign-qualified blob id names its owning account; download
        // under that account, with the native blobId on the wire.
        let (account_id, native_blob) =
            match foreign_split(&handle.id.0, |account| foreign_accounts.contains_key(account)) {
                Some((account, native)) => (AccountId::new(account), native.to_string()),
                None => (account_id, handle.id.0.clone()),
            };
        let mut blob = BlobRef::new(account_id, BlobId::new(native_blob));
        if let Some(content_type) = handle.content_type {
            blob = blob.with_content_type(content_type);
        }

        match client.download(&blob).await {
            Ok(bytes) => {
                let bytes_in = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                yield SyncEvent::Batch(Batch {
                    items: vec![bytes],
                    page_boundary: PageBoundary::Final,
                    server_latency: started.elapsed(),
                    bytes_in,
                    checkpoint: None,
                });
                yield SyncEvent::Done(None);
            }
            Err(err) => {
                yield super::error::terminated_from_jmap(
                    err,
                    super::error::JmapErrorContext::new(AccountOperation::OpenBlob),
                );
            }
        }
    })
}

/// Open a message's assembled RFC822 octets.
///
/// One `Email/get` for the whole-message `blobId`, then `client.download`
/// of that blob. The `hydrate.rs` raw-projection fatal is intentionally
/// untouched: hydration's raw projection is A1's concern; this is the
/// dedicated raw read.
pub(crate) fn open_raw_rfc822<T: HttpTransport>(
    client: Client<T>,
    account_id: AccountId,
    mail: MailAccount<T>,
    foreign_accounts: Arc<HashMap<String, MailAccount<T>>>,
    message: ObjectId,
) -> AccountStream<SyncEvent<bytes::Bytes>> {
    Box::pin(async_stream::stream! {
        let started = Instant::now();
        // A foreign-qualified message id routes BOTH legs (the `Email/get`
        // for the whole-message blobId and the blob download) to the owning
        // account. Routing only one of them would fetch a blobId from one
        // account and download it from another.
        let (account_id, mail, native_message) =
            match foreign_split(&message.0, |account| foreign_accounts.contains_key(account)) {
                Some((account, native)) => (
                    AccountId::new(account),
                    foreign_accounts
                        .get(account)
                        .cloned()
                        .expect("foreign_split only returns a registered account"),
                    native.to_string(),
                ),
                None => (account_id, mail, message.0.clone()),
            };
        let response = match mail
            .call(
                EmailGet::new()
                    .ids([EmailId::new(native_message.clone())])
                    // `Id` is requested explicitly so the correlation below
                    // has something to match on.
                    .properties([Property::Id, Property::BlobId]),
            )
            .await
        {
            Ok(response) => response,
            Err(err) => {
                // Email absent: the transport surfaces `IdNotFound`,
                // which `message(op, id)` classifies as
                // `NotFound(ResourceKind::Message)` via the scoped
                // `ErrorScope::Message`.
                yield super::error::terminated_from_jmap(
                    err,
                    super::error::JmapErrorContext::message(
                        AccountOperation::OpenRawRfc822,
                        message.0.clone(),
                    ),
                );
                return;
            }
        };

        // Correlate on the id we submitted rather than taking the head of the
        // echoed list. One id was requested, so positional trust happens to be
        // safe today, but it is the pattern `reconcile_hydration` was written
        // to eliminate: a server echoing an unrelated object would have ITS
        // blobId downloaded and returned as this caller's message body. An
        // uncorrelated response falls through to the NotFound below.
        let blob_id = response
            .into_list()
            .into_iter()
            .find(|email| email.id().is_some_and(|id| id.as_str() == native_message))
            .and_then(|email| email.blob_id().cloned());
        let Some(blob_id) = blob_id else {
            // Email present but `blobId` is `None`: no transport error to
            // convert, so build the NotFound directly. `terminated_unsupported`
            // would be wrong - the operation is supported; the
            // whole-message blob is missing.
            let err = AccountErrorBuilder::new(
                AccountErrorKind::NotFound(ResourceKind::Message),
                Cause::Request(RequestCause::NotFound {
                    what: ResourceKind::Message,
                    id: Some(message.0.clone()),
                }),
            )
            .operation(AccountOperation::OpenRawRfc822)
            .protocol(Protocol::Jmap)
            .try_build()
            .expect("valid account error classification");
            yield super::error::terminated(err);
            return;
        };

        let blob = BlobRef::new(account_id, blob_id);
        match client.download(&blob).await {
            Ok(bytes) => {
                let bytes_in = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                yield SyncEvent::Batch(Batch {
                    items: vec![bytes],
                    page_boundary: PageBoundary::Final,
                    server_latency: started.elapsed(),
                    bytes_in,
                    checkpoint: None,
                });
                yield SyncEvent::Done(None);
            }
            Err(err) => {
                yield super::error::terminated_from_jmap(
                    err,
                    super::error::JmapErrorContext::message(
                        AccountOperation::OpenRawRfc822,
                        message.0.clone(),
                    ),
                );
            }
        }
    })
}

pub(crate) fn open_range(
    handle: BlobHandle,
    range: ByteRange,
) -> AccountStream<SyncEvent<bytes::Bytes>> {
    Box::pin(async_stream::stream! {
        if let (Some(total), start) = (handle.size, range.start)
            && start >= total
        {
            // A start past the known size is a caller argument fault, not a
            // capability gap: `Request(Malformed)` derives to `ClientBug`,
            // where `Unsupported` would tell the engine the protocol has no
            // ranged read at all and suppress the operation wholesale.
            let err = AccountErrorBuilder::new(
                AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
                Cause::Request(RequestCause::InvalidArgument {
                    field: Some("range.start"),
                    message: Some(bifrost_types::DiagnosticText::support_only(format!(
                        "JMAP blob range starts past the known blob size (start {start}, total {total})"
                    ))),
                }),
            )
            .operation(AccountOperation::OpenBlobRange)
            .protocol(Protocol::Jmap)
            .try_build()
            .expect("valid account error classification");
            yield super::error::terminated(err);
            return;
        }

        if !handle.capabilities.supports_range {
            yield super::error::terminated_unsupported(
                AccountOperation::OpenBlobRange,
                None,
                "JMAP blob handle does not support range fetches",
            );
            return;
        }

        yield super::error::terminated_unsupported(
            AccountOperation::OpenBlobRange,
            None,
            "JMAP ranged blob download needs a Range-capable transport hook",
        );
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::foreign_split;

    #[test]
    fn foreign_blob_id_selects_the_owning_account() {
        let registered: HashSet<String> = ["acct-9".to_string()].into_iter().collect();
        let is_registered = |account: &str| registered.contains(account);

        let foreign = super::super::foreign::encode_object("acct-9", "B1");
        // The download runs under the FOREIGN accountId with the native
        // blobId on the wire.
        assert_eq!(
            foreign_split(&foreign, is_registered),
            Some(("acct-9", "B1"))
        );

        // A bare (primary) id keeps the primary account.
        assert_eq!(foreign_split("B1", is_registered), None);
        // An id naming an unreachable account falls back to primary, so the
        // miss surfaces as a real download 404.
        let gone = super::super::foreign::encode_object("acct-gone", "B1");
        assert_eq!(foreign_split(&gone, is_registered), None);
    }
}
