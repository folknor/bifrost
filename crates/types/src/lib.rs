//! `bifrost-types` is the trait crate every other bifrost workspace
//! crate depends on.
//!
//! It defines the `Account` trait protocol crates implement, the
//! sync-engine event vocabulary (`SyncEvent`, `Batch`, `Change`,
//! `Checkpoint`), the capability snapshot the engine reads at open,
//! the unified PIM surface (containers, send, search, drafts,
//! settings, hydration), and the error / recovery taxonomy used to
//! wire fault recovery between protocol crates and the engine.
//!
//! Zero workspace-internal dependencies. The engine, every protocol
//! crate, and `bifrost-net` depend on this crate; this crate depends
//! on nothing else in the workspace.

#![forbid(unsafe_code)]

pub mod account;
pub mod blob;
pub mod capabilities;
pub mod compose;
pub mod container;
pub mod cursor;
pub mod error;
pub mod events;
pub mod hydration;
pub mod ids;
pub mod mutation;
pub mod page;
pub mod search;
pub mod settings;

// Account trait + factory + erased return aliases.
pub use account::{Account, AccountFactory, AccountFuture, AccountStream};

// Blob types.
pub use blob::{BlobCapabilities, BlobEncoding, BlobHandle, ByteRange, Digest, DigestAlgorithm};

// Capability snapshot + per-method support flags.
pub use capabilities::{
    AccountCapabilities, BatchingPolicy, BlobRangeSupport, CapabilityChange, CapabilityDelta,
    CapabilityKey, CapabilityValue, ConvenienceShape, CursorFreshness, MutationCapabilities,
    MutationConcurrency, MutationReplaySafety, PimMethodSupport, PushCapability, QuotaSignal,
    RateLimitClass, StarredFlagShape,
};

// Mail composition types.
pub use compose::{
    Address, AttachmentHandle, AttachmentInline, DraftHandle, DraftPatch, IdentityId, SendRequest,
};

// Container / label / mutation-target types.
pub use container::{
    Container, ContainerId, ContainerKind, FolderRole, Label, MutationTarget, Provenance,
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
    BackfillCheckpoint, BackfillProgress, Batch, Change, Checkpoint, Control, HintPayload,
    InvalidationHint, InvalidationSink, InventoryEntry, InventoryPartition, InventoryPartitioning,
    ObjectChange, ObjectChangeKind, PageBoundary, Partition, Priority, Progress, PushSource,
    ScopeChange, ScopeChangeKind, SyncEvent, WatchEvent,
};

// Threading + hydration types.
pub use hydration::{HydrationProjection, Message, ThreadHydration};

// Newtype ids.
pub use ids::{
    AccountId, BlobId, FolderId, LabelId, MailboxId, ObjectId, QueryId, RunId, SubscriptionHandle,
    ThreadId,
};

// Mutation + projection + hydration types.
pub use mutation::{
    Fingerprint, FlagOp, HydratedObject, HydratedObjectKind, IdempotencyKey, MutationOutcome,
    MutationResult, Projection, ProtocolSalt, ServerVersion,
};

// Pagination.
pub use page::Page;

// Search request AST.
pub use search::{SearchFilter, SearchRequest};

// Settings: identities, vacation, quota.
pub use settings::{Identity, IdentityPatch, QuotaInfo, VacationConfig};

/// Compile-time dyn-safety check for `Account`. If this function
/// fails to compile, the trait has gained a non-dyn-safe method
/// signature and the workspace will not link.
#[allow(dead_code)]
fn _dyn_safe(_: &dyn Account) {}
