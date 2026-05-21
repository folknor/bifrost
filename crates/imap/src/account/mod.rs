//! IMAP implementation of the shared `Account` trait.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bifrost_types::{
    Account, AccountFuture, AccountStream, BlobHandle, ByteRange, Change, ChangeCursor,
    CursorDescriptor, CursorEstablishment, CursorScope, Error as AccountError, HydratedObject,
    IdempotencyKey, InventoryEntry, MembershipScope, MutationResult, Priority, Projection,
    SubscriptionHandle, SyncEvent, WatchEvent,
};
use futures::stream::Stream;
use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::types::{MailboxName, SyncSelectOptions, SyncSelectResult, UidSet, UidValidity};

mod blob;
mod capabilities;
mod changes;
mod close;
mod envelope;
mod factory;
mod folder_registry;
mod get;
mod inventory;
mod mutate;
mod pool;
mod push;
mod scopes;

pub use factory::{ImapAccountConfig, ImapAccountFactory};

pub(crate) use envelope::{
    DecodedObjectId, FolderCursor, decode_blob_id, decode_cursor, decode_object_id, encode_cursor,
    encode_object_id,
};
pub(crate) use folder_registry::{CompactUidSet, FolderRegistry};
pub(crate) use pool::{Pool, PooledConn};

const STREAM_CAPACITY: usize = 64;
const BATCH_ITEMS: usize = 128;
const UNLIMITED_BANDWIDTH: u64 = u64::MAX;

/// Open IMAP account handle. The handle owns the connection pool.
#[derive(Clone)]
pub struct ImapAccount {
    inner: Arc<ImapAccountInner>,
}

pub struct ImapAccountInner {
    pub(crate) config: Arc<ImapAccountConfig>,
    pub(crate) capabilities: bifrost_types::AccountCapabilities,
    pub(crate) pool: Arc<Pool>,
    pub(crate) folders: Arc<FolderRegistry>,
    pub(crate) qresync_enabled: bool,
    pub(crate) shutdown: CancellationToken,
    pub(crate) closed: AtomicBool,
    pub(crate) priority: AtomicU8,
    pub(crate) bandwidth_cap: AtomicU64,
    pub(crate) push: push::PushState,
}

impl ImapAccount {
    pub(crate) fn new(
        config: Arc<ImapAccountConfig>,
        capabilities: bifrost_types::AccountCapabilities,
        pool: Arc<Pool>,
        folders: Arc<FolderRegistry>,
        qresync_enabled: bool,
    ) -> Self {
        Self {
            inner: Arc::new(ImapAccountInner {
                config,
                capabilities,
                pool,
                folders,
                qresync_enabled,
                shutdown: CancellationToken::new(),
                closed: AtomicBool::new(false),
                priority: AtomicU8::new(Priority::Normal as u8),
                bandwidth_cap: AtomicU64::new(UNLIMITED_BANDWIDTH),
                push: push::PushState::new(),
            }),
        }
    }
}

impl std::ops::Deref for ImapAccount {
    type Target = ImapAccountInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ImapAccount {
    pub(crate) fn command_timeout(&self) -> Duration {
        self.config.imap.command_timeout
    }

    pub(crate) async fn checkout_for_folder(
        &self,
        folder: &MailboxName,
    ) -> Result<PooledConn, Error> {
        self.pool.checkout_for_folder(folder).await
    }

    pub(crate) async fn select_folder(
        &self,
        conn: &mut PooledConn,
        folder: &MailboxName,
        cursor: Option<&FolderCursor>,
        read_only: bool,
    ) -> Result<SyncSelectResult, Error> {
        let options = self.select_options(cursor, read_only);
        let result = conn
            .connection()
            .select_for_sync(folder.as_str(), &options, self.command_timeout())
            .await?;
        conn.set_selected(folder.clone());
        self.folders.mark_seen(folder);
        Ok(result)
    }

    fn select_options(&self, cursor: Option<&FolderCursor>, read_only: bool) -> SyncSelectOptions {
        let mut options = if read_only {
            SyncSelectOptions::read_only()
        } else {
            SyncSelectOptions::read_write()
        };

        if self.qresync_enabled
            && let Some(FolderCursor::QResync {
                uidvalidity,
                modseq,
            }) = cursor
            && let Some(validity) = UidValidity::new(*uidvalidity)
        {
            options = options.with_qresync(validity, crate::types::ModSeq::new(*modseq), None);
        }
        options
    }

    pub(crate) fn cursor_from_select(
        &self,
        selected: &crate::types::SelectedMailbox,
        known_uids: Option<CompactUidSet>,
    ) -> FolderCursor {
        let uidvalidity = selected.uid_validity.unwrap_or_default();
        if self.qresync_enabled
            && let Some(modseq) = selected.highest_mod_seq
            && !selected.no_mod_seq
        {
            return FolderCursor::QResync {
                uidvalidity,
                modseq,
            };
        }
        if let Some(modseq) = selected.highest_mod_seq
            && !selected.no_mod_seq
        {
            return FolderCursor::Condstore {
                uidvalidity,
                modseq,
                known_uids: known_uids.unwrap_or_default(),
            };
        }
        FolderCursor::Basic {
            uidvalidity,
            uidnext: selected.uid_next.unwrap_or_default(),
            known_uids: known_uids.unwrap_or_default(),
        }
    }
}

impl Account for ImapAccount {
    fn capabilities(&self) -> &bifrost_types::AccountCapabilities {
        &self.capabilities
    }

    fn set_priority(&self, priority: Priority) {
        self.priority.store(priority as u8, Ordering::Release);
    }

    fn set_bandwidth_cap(&self, bps: Option<u64>) {
        self.bandwidth_cap
            .store(bps.unwrap_or(UNLIMITED_BANDWIDTH), Ordering::Release);
    }

    fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor {
        changes::describe_cursor(self, cursor)
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        scopes::discover_cursor_scopes(self.clone())
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        scopes::discover_memberships(self.clone())
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<bifrost_types::ScopeLifecycle> {
        scopes::scope_lifecycle_stream(self.clone())
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        inventory::establish_initial_cursor(self.clone(), scope)
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<SyncEvent<InventoryEntry>> {
        inventory::inventory_stream(self.clone(), scope)
    }

    fn get_stream(
        &self,
        ids: AccountStream<bifrost_types::ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<HydratedObject>> {
        get::get_stream(self.clone(), ids, projection)
    }

    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        changes::changes_stream(self.clone(), cursor)
    }

    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<SubscriptionHandle, AccountError>> {
        push::push_subscribe(self.clone(), scopes.to_vec())
    }

    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        push::push_unsubscribe(self.clone(), handle)
    }

    fn push_stream(&self) -> AccountStream<WatchEvent> {
        push::push_stream(self.clone())
    }

    fn open_blob(&self, handle: BlobHandle) -> AccountStream<SyncEvent<bytes::Bytes>> {
        blob::open_blob(self.clone(), handle)
    }

    fn open_blob_range(
        &self,
        handle: BlobHandle,
        range: ByteRange,
    ) -> AccountStream<SyncEvent<bytes::Bytes>> {
        blob::open_blob_range(self.clone(), handle, range)
    }

    fn bulk_set_flags(
        &self,
        targets: AccountStream<bifrost_types::ObjectId>,
        op: bifrost_types::FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<MutationResult>> {
        mutate::bulk_set_flags(self.clone(), targets, op, key)
    }

    fn bulk_move(
        &self,
        targets: AccountStream<bifrost_types::ObjectId>,
        destination: MembershipScope,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<MutationResult>> {
        mutate::bulk_move(self.clone(), targets, destination, key)
    }

    fn bulk_destroy(
        &self,
        targets: AccountStream<bifrost_types::ObjectId>,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<MutationResult>> {
        mutate::bulk_destroy(self.clone(), targets, key)
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        close::close(self.clone())
    }
}

pub(crate) fn account_error(err: Error) -> AccountError {
    match err {
        Error::Auth { text, .. } => AccountError::Auth(text),
        Error::AuthPolicy(text) => AccountError::Auth(text.to_string()),
        Error::Io(e) => AccountError::Transport(e.to_string()),
        Error::Closed | Error::DriverGone | Error::DriverPanicked(_) => {
            AccountError::Transport(err.to_string())
        }
        other => AccountError::Other(other.to_string()),
    }
}

pub(crate) fn fatal_event<T>(err: Error) -> SyncEvent<T> {
    SyncEvent::Fatal(bifrost_types::Fatal {
        recovery: match err.recovery() {
            crate::Recovery::Reconnect
            | crate::Recovery::RetryOrReconnect
            | crate::Recovery::RetryAfter => bifrost_types::RecoveryClass::Retry {
                after: Duration::from_secs(5),
            },
            crate::Recovery::Reauthenticate => bifrost_types::RecoveryClass::AuthLost,
            crate::Recovery::ResyncMailbox => bifrost_types::RecoveryClass::RestartAccount,
            _ => bifrost_types::RecoveryClass::Fatal,
        },
        message: err.to_string(),
        source: Some(account_error(err)),
    })
}

pub(crate) fn boxed_receiver_stream<T: Send + 'static>(
    rx: tokio::sync::mpsc::Receiver<T>,
) -> AccountStream<T> {
    Box::pin(ReceiverStream { rx })
}

pub(crate) fn iter_stream<T: Send + Unpin + 'static>(items: Vec<T>) -> AccountStream<T> {
    Box::pin(IterStream {
        items: items.into(),
    })
}

struct ReceiverStream<T> {
    rx: tokio::sync::mpsc::Receiver<T>,
}

impl<T> Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

struct IterStream<T> {
    items: VecDeque<T>,
}

impl<T: Unpin> Stream for IterStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.items.pop_front())
    }
}

pub(crate) fn batch<T>(
    items: Vec<T>,
    boundary: bifrost_types::PageBoundary,
    checkpoint: Option<bifrost_types::Checkpoint>,
) -> SyncEvent<T> {
    SyncEvent::Batch(bifrost_types::Batch {
        items,
        page_boundary: boundary,
        server_latency: Duration::ZERO,
        bytes_in: 0,
        checkpoint,
    })
}

pub(crate) fn folder_from_scope(scope: &CursorScope) -> Result<MailboxName, AccountError> {
    match scope {
        CursorScope::Folder(folder) => {
            MailboxName::new(folder.0.clone()).map_err(|e| AccountError::Other(e.to_string()))
        }
        _ => Err(AccountError::Unsupported),
    }
}

pub(crate) fn folder_scope(folder: &MailboxName) -> CursorScope {
    CursorScope::Folder(bifrost_types::FolderId(folder.as_str().to_owned()))
}

pub(crate) fn membership_scope(folder: &MailboxName) -> MembershipScope {
    MembershipScope::Folder(bifrost_types::FolderId(folder.as_str().to_owned()))
}

pub(crate) fn uid_set_from_u32(uids: &[u32]) -> Option<UidSet> {
    UidSet::from_uids(uids.iter().filter_map(|uid| crate::types::Uid::new(*uid)))
}
