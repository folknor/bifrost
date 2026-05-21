use std::time::Instant;

use bifrost_types::{AccountStream, Batch, BlobHandle, ByteRange, Error, PageBoundary, SyncEvent};

use crate::blob::BlobRef;
use crate::client::Client;
use crate::core::id::{AccountId, BlobId};

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
                yield super::error::fatal_from_jmap(err, None);
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
            yield super::error::fatal_from_account_error(
                Error::RangeOutOfBounds { start, total },
                None,
                "JMAP blob range starts past the known blob size",
            );
            return;
        }

        if !handle.capabilities.supports_range {
            yield super::error::fatal_from_account_error(
                Error::RangeNotSupported,
                None,
                "JMAP blob handle does not support range fetches",
            );
            return;
        }

        yield super::error::fatal_from_account_error(
            Error::Unsupported,
            None,
            "JMAP ranged blob download needs a Range-capable transport hook",
        );
    })
}
