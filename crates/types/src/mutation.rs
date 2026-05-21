//! Mutation, projection, and hydration types.
//!
//! Read- and write-side both share the streaming `SyncEvent<Batch<T>>`
//! contract; the types here are the per-item payloads on each side
//! plus the fingerprint shape consumers diff inventory against.

use std::collections::HashSet;

use bytes::Bytes;

use crate::blob::BlobHandle;
use crate::error::Error;
use crate::ids::{ObjectId, RunId};

/// Server-side version stamp used in `Fingerprint`.
///
/// Cross-protocol diff degrades gracefully: when the protocol cannot
/// offer a server version, `Unavailable` falls back to
/// `(size, flags_hash)`-only comparison.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ServerVersion {
    /// IMAP CONDSTORE / QRESYNC per-message MODSEQ.
    ModSeq(u64),
    /// Microsoft Graph entity tag.
    ETag(String),
    /// Gmail historyId snapshot covering this item.
    HistoryAt(u64),
    /// JMAP State string covering this item.
    StateAt(String),
    /// IMAP Basic, or any protocol without a server version stamp.
    Unavailable,
}

/// Cross-protocol inventory diff fingerprint. The consumer compares
/// `local.fingerprint != server.fingerprint` to decide whether to
/// refetch. `size` is `Option` because Graph does not expose message
/// size on the message resource; consumers fall back to
/// `(server_version, flags_hash)` there.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fingerprint {
    pub server_version: ServerVersion,
    pub size: Option<u64>,
    pub flags_hash: u64,
}

/// Projection level for `Account::get_stream`.
///
/// Maps to JMAP `properties`, IMAP `FETCH` part list, Gmail `format`,
/// and Graph `$select`. `Inventory` is intentionally absent because
/// inventory has its own streaming primitive on the trait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Projection {
    /// Id + flags only. Cheapest projection; used by the read-back
    /// guard after a retry.
    FlagsOnly,
    /// Id, flags, size, threading headers, memberships. The
    /// `Metadata`-shaped projection an inventory consumer would
    /// otherwise re-derive.
    Metadata,
    /// `Metadata` + Subject, From, To, Cc, Date for list-view UI.
    Headers,
    /// `Headers` + N bytes of decoded text/plain for preview.
    Preview(usize),
    /// `Headers` + the full text/plain part only.
    TextOnly,
    /// All parts decoded, no attachment bytes.
    Full,
    /// `Full` + inline attachment bytes.
    FullWithBlobs,
}

/// Variant of a hydrated object selected by `Projection`.
///
/// The engine treats the inner shapes as protocol-derived blobs of
/// fields. Consumers down-cast based on the projection they asked
/// for. Variants are intentionally coarse; the protocol crate fills
/// in the fields the projection requested.
///
/// Boxing the heaviest arm (Metadata) is not worth it: hydrated
/// streams of the FlagsOnly projection are rare on the hot path,
/// and Metadata is itself the inventory shape consumers expect to
/// pattern-match without a deref step.
#[derive(Debug, Clone)]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum HydratedObjectKind {
    /// `Projection::FlagsOnly`. Carries the canonical flag set as
    /// the protocol observes it (current state, not a mutation).
    FlagsOnly(HashSet<String>),
    /// `Projection::Metadata`. Same shape inventory uses, hydrated
    /// from a fresh fetch.
    Metadata(crate::events::InventoryEntry),
    /// `Projection::Headers`, `Preview`, `TextOnly`, `Full`,
    /// `FullWithBlobs`. The raw RFC 822 / MIME bytes the protocol
    /// returned for the requested projection; consumers parse with
    /// their own MIME library. `Bytes` not `Vec<u8>` so a 5MB
    /// `HydratedObject::clone()` is a cheap refcount bump, not a
    /// copy.
    RawMime(Bytes),
}

/// Hydrated object payload yielded by `get_stream`.
///
/// The shape varies by `Projection`; the engine treats this as
/// opaque-ish and forwards to the consumer. The protocol crate
/// chooses one of the `HydratedObjectKind` variants based on the
/// requested projection.
#[derive(Debug, Clone)]
pub struct HydratedObject {
    pub id: ObjectId,
    pub kind: HydratedObjectKind,
    /// Blob handles for attached parts, when the projection requested
    /// them or when they are cheap to expose.
    pub blobs: Vec<BlobHandle>,
}

/// Idempotency surface for safe retry of bulk mutations.
///
/// Distinct from `MutationConcurrency::StateBased`: that is
/// optimistic concurrency on the wire, this is engine bookkeeping
/// for "did we already submit this exact batch?" No protocol today
/// emits `ProtocolSalt` onto the wire; all four rely on the engine's
/// read-back guard.
#[derive(Debug, Clone)]
pub struct IdempotencyKey {
    pub run_id: RunId,
    pub sequence: u64,
    pub protocol_salt: ProtocolSalt,
}

/// Protocol-tagged salt used by the engine's idempotency bookkeeping.
///
/// Carried alongside `RunId` and `sequence` so the engine can hash
/// requests deterministically per protocol without leaking the salt
/// onto the wire.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ProtocolSalt {
    /// JMAP has no documented wire replay token; `ifInState` is the
    /// orthogonal concurrency primitive.
    Jmap(String),
    /// Gmail has no documented wire replay token; `X-Goog-Request-Id`
    /// is not a dedup primitive on the messages endpoints.
    Gmail(String),
    /// Microsoft Graph has no documented wire replay token;
    /// `client-request-id` is debugging correlation only.
    Graph(String),
    /// IMAP carries no replay token; engine relies on the read-back
    /// guard after retry.
    Imap,
}

/// Per-item mutation outcome.
#[derive(Debug)]
pub struct MutationResult {
    pub id: ObjectId,
    pub outcome: MutationOutcome,
}

/// What happened to the item the mutation targeted.
#[derive(Debug)]
#[non_exhaustive]
pub enum MutationOutcome {
    Applied,
    /// The read-back guard determined the target was already in the
    /// requested state. No write was attempted.
    Skipped,
    Failed(Error),
}

/// Bulk flag mutation. Carries both the operation and the flag set
/// in one type so a `FlagOp::Set` actually pins the target state
/// (the prior `FlagSet { add, remove } + separate FlagOp` shape
/// couldn't - `Set` had nowhere to put the target flags). Use
/// `Patch` for the IMAP-style additive+subtractive case where a
/// single STORE needs to mention both adds and removes.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum FlagOp {
    /// Union the carried flag set into existing flags
    /// (IMAP `+FLAGS`, Gmail `addLabelIds`).
    Add(HashSet<String>),
    /// Difference the carried flag set from existing flags
    /// (IMAP `-FLAGS`, Gmail `removeLabelIds`).
    Remove(HashSet<String>),
    /// Replace existing flags with the carried set wholesale
    /// (IMAP `FLAGS`). Not directly expressible on Gmail; the
    /// Account impl translates via add+remove.
    Set(HashSet<String>),
    /// Both add and remove in one wire operation
    /// (IMAP `+FLAGS` then `-FLAGS` is two STOREs; Gmail
    /// `messages.batchModify` accepts both in one request).
    Patch {
        add: HashSet<String>,
        remove: HashSet<String>,
    },
}
