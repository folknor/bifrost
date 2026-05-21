//! Account-level capabilities.
//!
//! Read once at account-open via `Account::capabilities()`. Capability
//! transitions mid-session are not delivered through this surface; the
//! protocol crate ends affected streams with a Fatal carrying
//! `RecoveryClass::CapabilityChanged { delta }` and the engine re-opens
//! the account.

use std::time::Duration;

/// How the cursor's identity is established.
///
/// - `ServerIssued`: cursor bytes are minted by the server and we
///   trust them (JMAP `State`, Gmail `historyId`, Graph `@odata.deltaLink`).
/// - `Hybrid`: cursor bytes are derived partly from server state and
///   partly from client-side derivation (IMAP UIDVALIDITY +
///   HIGHESTMODSEQ + UID-list digest in basic mode).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CursorFreshness {
    ServerIssued,
    Hybrid,
}

/// Whether the protocol supports byte-range blob fetches.
///
/// `Conditional` means support varies per handle: Graph file attachments
/// support range, item attachments do not; the per-handle truth lives on
/// `BlobCapabilities::supports_range`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BlobRangeSupport {
    Yes,
    No,
    Conditional,
}

/// Push surface model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PushCapability {
    /// JMAP WebSocket / SSE, IMAP IDLE/NOTIFY, EWS streaming
    /// notifications. Push events flow on the in-process
    /// `push_stream`.
    InProcess,
    /// Gmail Pub/Sub. Subscription CRUD lives on the Account; the
    /// listener is wired by the consumer and feeds the engine's
    /// `InvalidationSink`.
    OutOfProcessPubsub,
    /// Microsoft Graph `/subscriptions` webhooks. Subscription CRUD
    /// lives on the Account; the receiver is wired by the consumer
    /// and feeds the `InvalidationSink`.
    WebhookOrEwsStream,
    /// Protocol exposes no push primitive; engine falls back to
    /// poll-only scheduling.
    None,
}

/// How mutation concurrency is enforced on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MutationConcurrency {
    /// JMAP `ifInState`, Graph `If-Match: <etag>`, IMAP
    /// `STORE UNCHANGEDSINCE`.
    StateBased,
    /// No protocol-level concurrency control. Last write wins.
    None,
}

/// Client-mintable wire dedup token availability.
///
/// `ReplayToken` is reserved for a future protocol that documents a
/// real dedup primitive; no protocol today qualifies. Every current
/// Account impl declares `None` and the engine reads back affected
/// items after retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MutationReplaySafety {
    ReplayToken,
    None,
}

/// Two distinct mutation concerns: lost-update prevention (concurrency)
/// and double-apply prevention (replay safety). Independent axes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutationCapabilities {
    pub concurrency: MutationConcurrency,
    pub replay_safety: MutationReplaySafety,
}

/// How the protocol crate's bulk methods batch their input stream.
///
/// Per-protocol defaults: JMAP `{ 500, 100ms }`, Gmail
/// `{ 1000, 200ms }`, Graph `{ 20, 100ms }`, IMAP `{ 5000, 50ms }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchingPolicy {
    pub max_items: usize,
    pub max_wait: Duration,
    pub flush_on_input_close: bool,
}

/// Coarse rate-limit posture. Used by `bifrost-net` and the engine to
/// pick a default request-rate budget for the account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RateLimitClass {
    /// No documented rate ceiling, or one so high it never matters in
    /// practice (well-run JMAP self-hosting).
    Generous,
    /// Public cloud tiers with documented quotas (Gmail's
    /// quota-units-per-second tier, Graph's mailbox concurrency tier).
    Tiered,
    /// IMAP per-connection serialization; not rate-limited but
    /// command-serialized.
    PerConnection,
}

/// Quota-shape signal the engine reacts to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum QuotaSignal {
    /// Server returns explicit `Retry-After` on 429.
    RetryAfter,
    /// Server emits a quota counter (Gmail quota units in response
    /// headers).
    QuotaUnits,
    /// No explicit signal; engine backs off on observed latency
    /// increase.
    Implicit,
    /// No quota whatsoever (self-hosted IMAP without throttle policy).
    None,
}

/// Snapshot of an account's behavior read at open-time.
#[derive(Debug, Clone)]
pub struct AccountCapabilities {
    pub cursor_freshness: CursorFreshness,
    pub blob_range: BlobRangeSupport,
    pub blob_digest_pre_download: bool,
    pub push: PushCapability,
    pub mutation: MutationCapabilities,
    pub batching_policy: BatchingPolicy,
    pub rate_limit_class: RateLimitClass,
    pub quota_signal: QuotaSignal,
    /// IMAP: the consumer must re-check UIDVALIDITY on every
    /// folder open.
    pub requires_uidvalidity_recheck: bool,
    /// Gmail says historyIds are *typically* valid for at least a
    /// week but can age out in hours. `None` because a deterministic
    /// value misleads scheduling.
    pub historyid_expires_after: Option<Duration>,
    /// Graph delta tokens have no fixed lifetime per Microsoft docs;
    /// expiry is an event, not a budget.
    pub delta_token_expires_after: Option<Duration>,
}

impl AccountCapabilities {
    /// True iff `push_stream` delivers `Invalidated` events directly
    /// (IMAP IDLE / NOTIFY, JMAP WebSocket, EWS streaming
    /// notifications). False for out-of-process push paths (Gmail
    /// Pub/Sub, Graph webhooks), where `Invalidated` arrives via
    /// the engine's `InvalidationSink` and `push_stream` carries
    /// only optional subscription-health transitions.
    ///
    /// Derived from `push` so the two encodings cannot drift.
    #[must_use]
    pub fn push_in_process(&self) -> bool {
        matches!(self.push, PushCapability::InProcess)
    }
}

/// Capability-key surface used to describe changes in
/// `CapabilityDelta`. Opaque newtype around a String so the engine and
/// observability layers can name capabilities without coupling to a
/// specific enum.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CapabilityKey(pub String);

/// A capability's stringified value at some point in time. Used for
/// both the "before" and "after" sides of a transition; the position
/// in `CapabilityChange` distinguishes them. (The prior draft had
/// separate `OldValue` / `NewValue` newtypes wrapping the same
/// `String` shape, which was redundant.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityValue(pub String);

/// One named capability transition.
#[derive(Debug, Clone)]
pub struct CapabilityChange {
    pub key: CapabilityKey,
    pub from: CapabilityValue,
    pub to: CapabilityValue,
}

/// Delta between two `AccountCapabilities` snapshots. Carried on
/// `RecoveryClass::CapabilityChanged` so the engine can reason about
/// what changed without re-reading the full snapshot.
#[derive(Debug, Clone, Default)]
pub struct CapabilityDelta {
    pub added: Vec<CapabilityKey>,
    pub removed: Vec<CapabilityKey>,
    pub changed: Vec<CapabilityChange>,
}
