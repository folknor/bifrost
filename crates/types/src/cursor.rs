//! Cursor and scope types for the Account trait.
//!
//! Two distinct concepts that protocols disagree about: what a cursor
//! tracks (`CursorScope`) and what containers an object belongs to
//! (`MembershipScope`). Keeping them as separate enums is the difference
//! between an engine that can multiplex four protocols and one that
//! pretends one of them does not exist.

use crate::ids::{FolderId, LabelId, MailboxId, QueryId};

/// What does a change cursor track?
///
/// - Gmail: `Account` (singleton historyId).
/// - JMAP: `Type(ObjectType)` per typed object; `Query(QueryId)` for
///   `queryChanges` cursors.
/// - IMAP: `Folder(FolderId)` (per-folder modseq).
/// - Graph: `FolderType { folder, ty }` (delta is per-folder per-type).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CursorScope {
    Account,
    Type(ObjectType),
    Query(QueryId),
    Folder(FolderId),
    FolderType { folder: FolderId, ty: ObjectType },
}

/// Container an object lives in.
///
/// Distinct from `CursorScope` because a single message can sit in many
/// labels or mailboxes simultaneously even when the change cursor for
/// that account is account-wide.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MembershipScope {
    Folder(FolderId),
    Label(LabelId),
    Mailbox(MailboxId),
    Query(QueryId),
}

/// Object type tag for type-scoped cursors and discovery.
///
/// Open-ended; `#[non_exhaustive]` so a future protocol that exposes a
/// novel object type does not break consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ObjectType {
    Email,
    Mailbox,
    Thread,
    Event,
    Contact,
    EmailSubmission,
    CalendarEvent,
    ContactGroup,
}

/// Which protocol minted an opaque cursor. Tagged so a JMAP cursor
/// cannot be silently handed to the Graph impl on dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ProtocolKind {
    Jmap,
    Imap,
    Gmail,
    Graph,
}

/// Concrete opaque cursor state. `protocol` and `envelope_version`
/// let the protocol impl reject a cursor minted for a different
/// protocol or an older schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpaqueChangeState {
    pub protocol: ProtocolKind,
    pub envelope_version: u32,
    pub bytes: Vec<u8>,
}

/// Mid-page resumption marker, opaque bytes owned by the protocol crate.
/// `None` when the protocol's pagination has no mid-page resume point
/// (Gmail history pages, Graph delta pages).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpaqueProgressBytes(pub Vec<u8>);

/// Cursor handed back to the protocol crate on resume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeCursor {
    pub scope: CursorScope,
    pub server_state: OpaqueChangeState,
    pub advanced_through: Option<OpaqueProgressBytes>,
    pub envelope_version: u32,
}

/// Result of `Account::establish_initial_cursor(scope)`.
///
/// - `Ready(cursor)`: the protocol minted a cursor cheaply (one
///   round-trip or factory-cached). The engine starts
///   `changes_stream(cursor)` immediately and runs `inventory_stream`
///   in parallel as backfill.
/// - `EstablishViaInventory`: the protocol has no cheap-mint
///   primitive. The engine must call `inventory_stream(scope)` first;
///   its terminal `Done` event carries the established cursor in its
///   checkpoint.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum CursorEstablishment {
    Ready(ChangeCursor),
    EstablishViaInventory,
}

/// Scope lifecycle event surfaced from `Account::scope_lifecycle_stream`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ScopeLifecycle {
    Created(MembershipScope),
    Renamed {
        old: MembershipScope,
        new: MembershipScope,
    },
    Deleted(MembershipScope),
}

/// Engine-facing introspection over an opaque cursor.
///
/// Account-aware (not pure-cursor) because cost depends on capability
/// state, not the cursor in isolation: a fresh JMAP `State` is `Cheap`;
/// a Gmail `historyId` that will fall back to full resync is `Expensive`.
#[derive(Debug, Clone)]
pub struct CursorDescriptor {
    pub cost_class: CostClass,
    pub strategy: SyncStrategy,
    pub freshness: Option<std::time::Instant>,
}

/// Coarse cost class used by the engine to schedule work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CostClass {
    Cheap,
    Medium,
    Expensive,
}

/// Resolved strategy a cursor will use to fetch its next batch.
///
/// IMAP picks one of `QResync`/`Condstore`/`Basic`; JMAP and Gmail
/// surface `ServerCursor` (their cursors are always server-issued);
/// Graph delta is `ServerCursor`; `None` means a cursor type that does
/// not advance (used in tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SyncStrategy {
    QResync,
    Condstore,
    Basic,
    ServerCursor,
    None,
}
