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
pub mod calendar;
pub mod capabilities;
pub mod compose;
pub mod contact;
pub mod container;
pub mod cursor;
pub mod error;
pub mod events;
pub mod filter;
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

// Calendar primitives.
pub use calendar::{
    AttendeeRole, Calendar, CalendarEvent, CalendarId, CalendarProvenance, EventAttendee,
    EventAvailability, EventCreate, EventId, EventOrganizer, EventPatch, EventRange,
    EventRecurrence, EventSearchRequest, EventStatus, EventTime, EventVisibility, RsvpStatus,
};

// Server-side filter rules and scripts.
pub use filter::{
    FilterAction, FilterCondition, FilterDiagnostic, FilterDiagnosticSeverity, FilterRule,
    FilterRuleCreate, FilterRulePatch, FilterRuleShape, FilterScript, FilterScriptCreate,
    FilterScriptPatch, FilterValidation, ScriptLanguage, ServerFilter, ServerFilterCreate,
    ServerFilterId, ServerFilterPatch,
};

// Mail composition types.
pub use compose::{
    Address, AttachmentHandle, AttachmentInline, DraftHandle, DraftPatch, IdentityId, SendRequest,
};

// Container / label / mutation-target types.
pub use container::{
    Container, ContainerId, ContainerKind, FolderRole, Label, MutationTarget, Provenance,
};

// Address book and contact-card primitives.
pub use contact::{
    AddressBook, AddressBookId, ContactAddress, ContactCard, ContactCreate, ContactEmail,
    ContactId, ContactOrganization, ContactPatch, ContactPhone, ContactPhoto, ContactProvenance,
    ContactSearchRequest,
};

// Cursor + scope types.
pub use cursor::{
    ChangeCursor, CostClass, CursorDescriptor, CursorEstablishment, CursorScope, MembershipScope,
    ObjectType, OpaqueChangeState, OpaqueProgressBytes, ProtocolKind, ScopeLifecycle,
    ScopeLifecycleEvent, SyncStrategy,
};

// Error + recovery vocabulary.
pub use error::{
    AccessCause, AccessErrorKind, AccountError, AccountErrorBuildError, AccountErrorBuilder,
    AccountErrorKind, AccountOperation, AttemptCause, AuthCause, AuthErrorKind, BatchFailure,
    BatchInputInvalidItem, BatchInputInvalidReason, BatchInvariantError, BatchItem, BatchItemId,
    BatchItemOutcome, BatchOutcome, BatchOutcomeBuilder, BatchSuccess, BatchUncertain, Cause,
    CauseChain, DetailVisibility, DiagnosticInfo, DiagnosticText, EngineDirective,
    EnhancedStatusCode, ErrorScope, Fatal, GmailSignal, GraphSignal, ImapResponseCode, ItemOutcome,
    JmapMethod, MailboxUnavailableKind, MutationSuccess, Protocol, ProtocolErrorKind, Provider,
    ReconcileAction, ReconcileAdvice, ReconcileGuidance, ReconcileReason, RecoveryClass,
    RemediationAction, RequestCause, RequestErrorKind, ResourceKind, RetryAdvice, RetryDisposition,
    RetryHint, RetryReason, ServerCause, ServerErrorKind, StateCause, StrategyDowngrade,
    SupportExportConsented, SupportExportInternal, SupportExportMinimal, SyncStateErrorKind,
    TelemetryView, ThrottleKey, ThrottleScope, TransmissionState, TransportCause,
    TransportErrorKind, TransportKind, Warning, WarningKind, WireCause, validate_batch_input,
};

// Stream-event types.
pub use events::{
    AccountControl, BackfillCheckpoint, BackfillProgress, Batch, Change, Checkpoint, Control,
    HintPayload, InvalidationHint, InvalidationSink, InventoryEntry, InventoryPartition,
    InventoryPartitioning, ObjectChange, ObjectChangeKind, PageBoundary, Partition, PauseReason,
    Priority, Progress, PushSource, ScopeChange, ScopeChangeKind, SyncEvent, WatchEvent,
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
    Fingerprint, FlagOp, HydratedObject, HydratedObjectKind, IdempotencyKey, Projection,
    ProtocolSalt, ServerVersion,
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
