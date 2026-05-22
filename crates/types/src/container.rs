//! Container, label, and provenance types for the unified PIM
//! surface.
//!
//! A `Container` is a folder, label, or mailbox the account exposes
//! as a place a message can live. The unified shape lets the consumer
//! branch on `kind` (Folder | Label) when it wants a folder-binary
//! UI, on `role` when it wants ratatoskr's canonical INBOX / SENT /
//! DRAFT / ARCHIVE / TRASH / SPAM rendering, or on `provenance` when
//! it needs the wire-level native id to round-trip back to the
//! protocol.

use crate::cursor::ProtocolKind;
use crate::ids::ObjectId;

/// Folder/label classification. Folder-shaped (IMAP, Graph) versus
/// label-shaped (Gmail). JMAP mailboxes are folder-shaped on the
/// wire (a message lives in one or more mailboxes) but consumers
/// often render them like labels - so consumers branch on `kind`
/// when the rendering distinction matters and ignore it when it does
/// not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ContainerKind {
    /// Hierarchical folder (IMAP mailbox path, Graph mail folder,
    /// JMAP `Mailbox` object).
    Folder,
    /// Flat label (Gmail user label, Gmail system label).
    Label,
}

/// Canonical role a container plays in ratatoskr's UI. `None` (encoded
/// as `Option<FolderRole>::None`) means the container is a user-created
/// container without a system role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FolderRole {
    Inbox,
    Sent,
    Drafts,
    Archive,
    Trash,
    Spam,
}

/// Wire-level provenance for a container or label id.
///
/// Carries enough context for the consumer to know which protocol
/// minted the id, what kind of object it points at, and what the
/// native id string actually was on the wire. The convenience layer
/// inspects this to decide whether to dispatch `apply_label` into
/// `set_keyword`, `set_label_membership`, `set_category`, or
/// `set_extended_property`.
///
/// Not `#[non_exhaustive]` because protocol Account impls construct
/// `Provenance` directly when populating containers and labels.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Provenance {
    /// Which protocol family the id was minted by.
    pub provider: ProtocolKind,
    /// Folder versus label, as the protocol exposed it.
    pub kind: ContainerKind,
    /// Native id string the protocol uses on the wire.
    pub native: String,
}

/// Engine-facing identifier for a container. Wraps `ObjectId` so the
/// trait surface stays consistent with the rest of bifrost-types but
/// carries the dedicated semantic meaning of "this id points at a
/// container, not a message".
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContainerId(pub String);

impl From<ObjectId> for ContainerId {
    fn from(id: ObjectId) -> Self {
        Self(id.0)
    }
}

impl From<ContainerId> for ObjectId {
    fn from(id: ContainerId) -> Self {
        Self(id.0)
    }
}

/// Unified container shape returned by `Account::containers_list`.
///
/// `kind` is the folder/label classification. `role` is the canonical
/// ratatoskr role when the protocol claims one. `provenance` is the
/// wire-level metadata so the consumer can round-trip back to the
/// protocol without ambiguity. `native_id` is the same string that
/// `provenance.native` carries; surfaced separately for ergonomic
/// access without descending into the provenance.
///
/// Not `#[non_exhaustive]` because protocol Account impls construct
/// `Container` values directly in `containers_list`. Future
/// additions land as new fields with sensible defaults plus a
/// builder helper rather than as breaking changes.
#[derive(Debug, Clone)]
pub struct Container {
    /// Engine-facing identifier.
    pub id: ContainerId,
    /// Folder versus label.
    pub kind: ContainerKind,
    /// Canonical role, when the container plays one.
    pub role: Option<FolderRole>,
    /// Wire-level provenance.
    pub provenance: Provenance,
    /// Native id string, duplicated from `provenance.native` for
    /// callers that already have a `Container` and would rather not
    /// descend.
    pub native_id: String,
    /// Display name as the protocol surfaces it.
    pub name: String,
    /// Parent container, when nested (Graph mail folders, IMAP
    /// hierarchical mailboxes). `None` for top-level containers and
    /// for flat-namespace protocols.
    pub parent: Option<ContainerId>,
}

/// Label shape parallel to `Container` for protocols that draw a
/// hard distinction between containers (where a message lives) and
/// labels (a flag-like marker on a message).
///
/// Gmail user labels can be rendered either way - they walk like a
/// flat container but spawn like a flag. `Label` is the side that
/// `apply_label` / `remove_label` conveniences dispatch through; the
/// same id may also appear in `containers_list` when the consumer
/// wants to render labels as containers.
///
/// Not `#[non_exhaustive]` so protocol Account impls can construct
/// `Label` values directly when populating their containers list.
#[derive(Debug, Clone)]
pub struct Label {
    /// Engine-facing identifier.
    pub id: ContainerId,
    /// Wire-level provenance. The convenience layer inspects this
    /// to dispatch into the right primitive.
    pub provenance: Provenance,
    /// Display name.
    pub name: String,
    /// Role, when the label plays a canonical one (Gmail STARRED,
    /// UNREAD, INBOX, etc.).
    pub role: Option<FolderRole>,
}

/// Mutation target shape used by every mail-mutation primitive.
///
/// JMAP, Gmail, and Graph natively address either a thread or a
/// single message; IMAP only addresses single messages. Account
/// impls that only support per-message mutation fan a `Thread`
/// target out internally rather than forcing the caller to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MutationTarget {
    /// Apply the mutation to every message in the thread.
    Thread(crate::ids::ThreadId),
    /// Apply the mutation to a single message.
    Message(crate::ids::ObjectId),
}

impl MutationTarget {
    /// Thread-shaped target.
    #[must_use]
    pub fn thread(id: crate::ids::ThreadId) -> Self {
        Self::Thread(id)
    }

    /// Message-shaped target.
    #[must_use]
    pub fn message(id: crate::ids::ObjectId) -> Self {
        Self::Message(id)
    }

    /// True iff this target is thread-shaped.
    #[must_use]
    pub fn is_thread(&self) -> bool {
        matches!(self, Self::Thread(_))
    }
}
