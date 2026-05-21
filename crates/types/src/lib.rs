//! `bifrost-types` is the trait crate every other bifrost workspace
//! crate depends on.
//!
//! It defines the `Account` trait protocol crates implement, the
//! sync-engine event vocabulary (`SyncEvent`, `Batch`, `Change`,
//! `Checkpoint`), the capability snapshot the engine reads at open,
//! and the error / recovery taxonomy used to wire fault recovery
//! between protocol crates and the engine.
//!
//! Zero workspace-internal dependencies. The engine, every protocol
//! crate, and `bifrost-net` depend on this crate; this crate depends
//! on nothing else in the workspace.

#![forbid(unsafe_code)]

pub mod account;
pub mod blob;
pub mod capabilities;
pub mod cursor;
pub mod error;
pub mod events;
pub mod ids;
pub mod mutation;

// Account trait + factory + erased return aliases.
pub use account::{Account, AccountFactory, AccountFuture, AccountStream};

// Blob types.
pub use blob::{BlobCapabilities, BlobEncoding, BlobHandle, ByteRange, Digest, DigestAlgorithm};

// Capability snapshot.
pub use capabilities::{
    AccountCapabilities, BatchingPolicy, BlobRangeSupport, CapabilityDelta, CapabilityKey,
    CursorFreshness, MutationCapabilities, MutationConcurrency, MutationReplaySafety, NewValue,
    OldValue, PushCapability, QuotaSignal, RateLimitClass,
};

// Cursor + scope types.
pub use cursor::{
    ChangeCursor, CostClass, CursorDescriptor, CursorEstablishment, CursorScope, MembershipScope,
    ObjectType, OpaqueChangeState, OpaqueProgressBytes, ProtocolKind, ScopeLifecycle, SyncStrategy,
};

// Error + recovery vocabulary.
pub use error::{Error, Fatal, RecoveryClass, StrategyDowngrade, Warning, WarningKind};

// Stream-event types.
pub use events::{
    BackfillCheckpoint, BackfillProgress, Batch, Change, Checkpoint, Control, CursorDelta,
    HintPayload, InvalidationHint, InvalidationSink, InventoryEntry, ObjectChange,
    ObjectChangeKind, PageBoundary, PageBoundaryKind, Partition, Priority, Progress, PushSource,
    ScopeChange, ScopeChangeKind, SyncEvent, WatchEvent,
};

// Newtype ids.
pub use ids::{
    AccountId, BlobId, FolderId, LabelId, MailboxId, ObjectId, QueryId, RunId, SubscriptionHandle,
    ThreadId,
};

// Mutation + projection + hydration types.
pub use mutation::{
    Fingerprint, FlagOp, FlagSet, HydratedObject, HydratedObjectKind, IdempotencyKey,
    MutationOutcome, MutationResult, Projection, ProtocolSalt, ServerVersion,
};

/// Compile-time dyn-safety check for `Account`. If this function
/// fails to compile, the trait has gained a non-dyn-safe method
/// signature and the workspace will not link.
#[allow(dead_code)]
fn _dyn_safe(_: &dyn Account) {}
