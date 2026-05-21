//! The `Account` trait and the `AccountFactory` companion trait.
//!
//! The trait is dyn-safe by construction: all stream and future
//! returns are erased through `AccountStream<T>` / `AccountFuture<T>`,
//! the cursor state is a concrete tagged blob rather than an
//! associated type, and `close` takes `&self` so it composes with
//! `Arc<dyn Account>`. A compile-time `_dyn_safe` check in
//! `lib.rs` verifies this at every build.

use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::Future;
use futures::stream::Stream;

use crate::blob::{BlobHandle, ByteRange};
use crate::capabilities::AccountCapabilities;
use crate::cursor::{
    ChangeCursor, CursorDescriptor, CursorEstablishment, CursorScope, MembershipScope,
    ScopeLifecycle,
};
use crate::error::Error;
use crate::events::{Change, InventoryEntry, SyncEvent, WatchEvent};
use crate::ids::{ObjectId, SubscriptionHandle};
use crate::mutation::{FlagOp, HydratedObject, IdempotencyKey, MutationResult, Projection};

/// Erased streaming return type for `Account` methods.
///
/// Each Account method that streams returns a boxed `Stream`. The
/// erasure is required for `dyn Account` to be reachable.
pub type AccountStream<T> = Pin<Box<dyn Stream<Item = T> + Send + 'static>>;

/// Erased async return type for `Account` methods.
///
/// `'static` because the returned future may outlive the Account
/// handle's local borrow.
pub type AccountFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// The contract every protocol crate implements and the engine drives.
///
/// All implementations are `Send + Sync`. Construction is consumer-
/// side via an `AccountFactory`; the engine owns the open
/// `Arc<dyn Account>` and calls the factory on reopen.
pub trait Account: Send + Sync {
    /// Read-once snapshot of the account's capabilities. Mid-session
    /// transitions are signaled by ending streams with
    /// `RecoveryClass::CapabilityChanged`, never through a live
    /// channel here.
    fn capabilities(&self) -> &AccountCapabilities;

    /// Cursor introspection. Account-aware because cost depends on
    /// capability state, not the cursor in isolation.
    fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor;

    /// Cursor-scope discovery: what scopes does the engine multiplex
    /// `changes_stream` over? Bounded; terminates with `Done`.
    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>>;

    /// Membership-scope discovery: what containers can the consumer
    /// ask about (folder trees, labels, mailboxes, queries)? Bounded.
    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>>;

    /// Ongoing scope lifecycle events (folder created, renamed, deleted).
    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycle>;

    /// Initial cursor establishment. The engine calls this exactly
    /// once per `(account, scope)` pair before its first
    /// `changes_stream(cursor)` call. See `CursorEstablishment` for
    /// the two outcomes.
    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, Error>>;

    /// Inventory: projection-only cold-start primitive. Also the
    /// cursor-establishment pass for scopes that returned
    /// `CursorEstablishment::EstablishViaInventory`; for those scopes
    /// the terminal `SyncEvent::Done` carries the established cursor
    /// in its checkpoint.
    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<SyncEvent<InventoryEntry>>;

    /// Hydrate known ids at a chosen projection. Input ids are
    /// streamed so the engine can backpressure long fetch passes.
    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<HydratedObject>>;

    /// Post-cursor diff. Yields `Change` (the sum of `ObjectChange`
    /// and `ScopeChange`).
    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>>;

    /// Server-side push subscription CRUD: create.
    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<SubscriptionHandle, Error>>;

    /// Server-side push subscription CRUD: destroy.
    fn push_unsubscribe(&self, handle: SubscriptionHandle) -> AccountFuture<Result<(), Error>>;

    /// Event stream from the protocol crate to the engine. Contents
    /// depend on `push_in_process`:
    ///
    /// - In-process (IMAP IDLE, JMAP WebSocket, EWS streaming): full
    ///   `WatchEvent` traffic - `Invalidated` + `Disconnected` +
    ///   `Reconnected`.
    /// - Out-of-process (Gmail Pub/Sub, Graph webhooks):
    ///   `Invalidated` events flow through `InvalidationSink`, not
    ///   here. The stream may carry `Disconnected` / `Reconnected` on
    ///   sustained subscription-health failure, or yield nothing.
    fn push_stream(&self) -> AccountStream<WatchEvent>;

    /// Open a blob for streaming download.
    fn open_blob(&self, handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>>;

    /// Open a byte range of a blob. Errors with `Error::RangeNotSupported`
    /// where the blob's capability flag is false.
    fn open_blob_range(
        &self,
        handle: BlobHandle,
        range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>>;

    /// Bulk flag mutation. `targets` is a streaming input so
    /// engine-driven mutation pipelines backpressure cleanly.
    /// `op` carries both the operation kind AND the flag set per
    /// variant; a `FlagOp::Set(HashSet)` pins the target flag set
    /// directly.
    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        op: FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<MutationResult>>;

    /// Bulk move. Targets are moved into `destination`.
    fn bulk_move(
        &self,
        targets: AccountStream<ObjectId>,
        destination: MembershipScope,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<MutationResult>>;

    /// Bulk destroy.
    fn bulk_destroy(
        &self,
        targets: AccountStream<ObjectId>,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<MutationResult>>;

    /// Graceful local-handle teardown. IMAP `LOGOUT` + pool drain,
    /// JMAP WebSocket close, Graph subscription stream end, local
    /// worker shutdown. Idempotent: takes `&self` so it composes
    /// with `Arc<dyn Account>` and is safe to call more than once.
    ///
    /// Does NOT destroy durable server-side push subscriptions; those
    /// go through `push_unsubscribe` explicitly.
    fn close(&self) -> AccountFuture<Result<(), Error>>;
}

/// Engine-facing factory.
///
/// Consumers register one per account so the engine can perform
/// reopen cycles (capability change, transport reset) without
/// knowing protocol config. The engine owns the current open
/// `Arc<dyn Account>` and calls `open()` when it needs a fresh one.
pub trait AccountFactory: Send + Sync + 'static {
    fn open(&self) -> AccountFuture<Result<Arc<dyn Account>, Error>>;
}
