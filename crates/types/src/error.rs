//! Error taxonomy.
//!
//! Three layers:
//! - `Error`: per-operation failure type returned from Account-trait
//!   methods.
//! - `Warning`: non-fatal per-batch event surfaced through
//!   `SyncEvent::Warning`.
//! - `Fatal`: terminating per-stream event surfaced through
//!   `SyncEvent::Fatal`, carrying a `RecoveryClass` so the engine
//!   knows which recovery action to take.

use std::time::Duration;

use crate::capabilities::CapabilityDelta;
use crate::cursor::{CursorScope, SyncStrategy};

/// Per-operation failure type. Returned by methods that yield a
/// future (`establish_initial_cursor`, `push_subscribe`,
/// `push_unsubscribe`, `close`) and as the inner type of
/// `MutationOutcome::Failed`.
///
/// Variants are intentionally coarse. Protocol crates can use
/// `Transport`, `Auth`, and `Other` as escape hatches for protocol-
/// specific failure detail without growing this enum unboundedly.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A cursor minted for one protocol was handed to a different
    /// protocol's `Account` impl on dispatch.
    #[error("cursor protocol mismatch")]
    CursorProtocolMismatch,
    /// A cursor's envelope version is unknown to this implementation.
    #[error("cursor envelope version is unknown")]
    CursorEnvelopeUnknown,
    /// Cursor envelope older than `MIN_MIGRATABLE`; consumer must
    /// clear cursor state and re-establish from inventory.
    #[error("cursor envelope schema is incompatible")]
    SchemaIncompatible,
    /// Operation is not supported by this account's capabilities.
    #[error("operation unsupported by this account")]
    Unsupported,
    /// A capability required by the engine was not advertised by the
    /// account.
    #[error("missing required capability")]
    MissingCoreCapability,
    /// Push primitive is currently held by another task (IMAP IDLE
    /// busy on another checkout) and cannot accept this call.
    #[error("push primitive is busy")]
    IdleBusy,
    /// Requested byte range starts past the blob's known size.
    #[error("range out of bounds (start = {start}, total = {total})")]
    RangeOutOfBounds { start: u64, total: u64 },
    /// Requested byte range, but the blob's transport does not
    /// support range fetches.
    #[error("range fetch not supported by this blob")]
    RangeNotSupported,
    /// Requested a blob fetch on a handle whose target is not a byte
    /// stream (Graph reference attachment, JMAP non-blob property).
    #[error("blob is not a byte stream")]
    BlobNotByteStream,
    /// Optimistic concurrency rejected the write (JMAP `stateMismatch`,
    /// Graph 412 Precondition Failed, IMAP `STORE UNCHANGEDSINCE`
    /// modseq advanced).
    #[error("concurrency conflict")]
    ConcurrencyConflict,
    /// Transport-layer failure (connection reset, TLS error, HTTP
    /// 5xx after retries exhausted). Free-form message because the
    /// underlying transport vocabulary varies per protocol.
    #[error("transport error: {0}")]
    Transport(String),
    /// Authentication failure (token refresh failed, OAuth scope
    /// missing, IMAP LOGIN rejected).
    #[error("auth error: {0}")]
    Auth(String),
    /// Escape hatch for protocol-specific failures that do not fit
    /// any other variant. Use sparingly.
    #[error("{0}")]
    Other(String),
}

/// Recovery vocabulary surfaced from protocol crates to the engine.
///
/// The engine maps these to the coarser `Fatal` recovery classes
/// described in the sync-engine reference, and applies the
/// corresponding recovery action (retry with backoff, restart scope,
/// restart account, etc.).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RecoveryClass {
    /// Network or transient server failure; engine retries after the
    /// stated delay.
    Retry { after: Duration },
    /// Engine should downgrade the cursor's strategy and restart
    /// (QRESYNC -> CONDSTORE, CONDSTORE -> Basic).
    DowngradeStrategy(StrategyDowngrade),
    /// Engine should drop a capability for a specific scope (Gmail
    /// folder reports modseq=0 on subsequent open).
    DowngradeCapabilityForScope(CursorScope),
    /// Engine should restart this one scope from scratch (IMAP
    /// UIDVALIDITY change, modseq reset).
    RestartScope(CursorScope),
    /// Engine should restart the entire account (Gmail stale
    /// historyId beyond recovery).
    RestartAccount,
    /// Authentication has lapsed; consumer must re-authenticate.
    /// Cursor preserved.
    AuthLost,
    /// Cursor envelope_version older than the engine can migrate.
    /// Full resync, but consumer may preserve inventory.
    SchemaIncompatible,
    /// Account capabilities changed mid-session. Engine re-opens
    /// the account and restarts streams.
    CapabilityChanged { delta: CapabilityDelta },
    /// Consumer intervention required (e.g., persistent QRESYNC
    /// failure suggests flipping the account to CONDSTORE-only).
    OperatorOverrideRequired { reason: String },
    /// Unrecoverable; engine cannot make further progress on this
    /// stream without consumer action beyond the routine recovery
    /// classes above.
    Fatal,
}

/// Strategy downgrade paths.
///
/// Only IMAP exercises this today; future protocols may add their
/// own downgrade paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StrategyDowngrade {
    QResyncToCondstore,
    CondstoreToBasic,
}

/// Stream-terminating Fatal event surfaced through `SyncEvent::Fatal`.
#[derive(Debug)]
pub struct Fatal {
    pub recovery: RecoveryClass,
    pub message: String,
    pub source: Option<Error>,
}

/// Non-fatal per-batch event surfaced through `SyncEvent::Warning`.
#[derive(Debug)]
pub struct Warning {
    pub kind: WarningKind,
    pub message: String,
    pub retry_count: u32,
    pub next_action: Option<String>,
    pub protocol_detail: Option<String>,
}

/// Named warning kinds the engine surfaces for observability.
///
/// `Other` is the escape hatch for protocol-specific warnings the
/// engine has no opinion on.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum WarningKind {
    /// Protocol downgraded its strategy mid-stream (QRESYNC ->
    /// CONDSTORE on iCloud `ENABLE` failure, etc.). Recovery is
    /// transparent; the warning surfaces for observability only.
    /// `from`/`to` are `SyncStrategy` states (the protocol's
    /// strategy *position*), not `StrategyDowngrade` transitions -
    /// "QResync -> Condstore" is one downgrade, naming the two
    /// positions is unambiguous; naming the transition twice would
    /// be nonsense.
    StrategyDowngraded {
        from: SyncStrategy,
        to: SyncStrategy,
    },
    /// Persistent strategy failure pattern; consumer should consider
    /// a runtime configuration override.
    OperatorAttentionNeeded { reason: String },
    /// Server requested throttling.
    Throttled { wait: Duration, source: String },
    /// Server reported a clock skew large enough to affect
    /// time-windowed backfill partitioning.
    ClockSkew { delta: Duration },
    /// Blob fetch returned a non-byte-stream resource (Graph
    /// reference attachments that 405 on `$value`).
    BlobNotByteStream,
    /// Read-back guard observed the target already matched the
    /// requested state; no write was issued for those ids.
    ReadbackSkipped { skipped: u64 },
    /// Catch-all for protocol-specific warnings.
    Other(String),
}
