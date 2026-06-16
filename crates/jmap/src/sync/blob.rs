use std::time::Instant;

use bifrost_types::{
    AccountErrorBuilder, AccountErrorKind, AccountOperation, AccountStream, Batch, BlobHandle,
    ByteRange, Cause, ObjectId, PageBoundary, Protocol, RequestCause, ResourceKind, SyncEvent,
};

use crate::blob::BlobRef;
use crate::client::Client;
use crate::core::id::{AccountId, BlobId};
use crate::email::{EmailGet, EmailId, Property};

type MailAccount = crate::account::Account<crate::transport_reqwest::ReqwestTransport>;

pub(crate) fn open(
    client: Client,
    account_id: AccountId,
    handle: BlobHandle,
) -> AccountStream<SyncEvent<bytes::Bytes>> {
    Box::pin(async_stream::stream! {
        let started = Instant::now();
        let mut blob = BlobRef::new(account_id, BlobId::new(handle.id.0));
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
pub(crate) fn open_raw_rfc822(
    client: Client,
    account_id: AccountId,
    mail: MailAccount,
    message: ObjectId,
) -> AccountStream<SyncEvent<bytes::Bytes>> {
    Box::pin(async_stream::stream! {
        let started = Instant::now();
        let response = match mail
            .call(
                EmailGet::new()
                    .ids([EmailId::new(message.0.clone())])
                    .properties([Property::BlobId]),
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

        let blob_id = match response.into_list().into_iter().next() {
            Some(email) => email.blob_id().cloned(),
            None => None,
        };
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
            yield super::error::terminated_unsupported(
                AccountOperation::OpenBlobRange,
                None,
                format!(
                    "JMAP blob range starts past the known blob size (start {start}, total {total})",
                ),
            );
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
