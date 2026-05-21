//! Opaque newtype identifiers carried across the bifrost workspace.
//!
//! Each id wraps a `String` because every protocol stringifies its native
//! identifier shape (JMAP uses base32hex-looking strings, Gmail uses lowercase
//! hex, Graph uses long opaque hashes, IMAP uses ASCII digits for UIDs). The
//! engine never inspects these; they round-trip from server to consumer.

/// Stable identifier for a single object on an account (a message, an event,
/// a contact, an email submission). Opaque within a protocol; not portable
/// across protocols.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectId(pub String);

/// Folder identifier (IMAP mailbox path encoded by the protocol crate, Graph
/// folder id, etc.). Opaque to the engine.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FolderId(pub String);

/// Gmail label identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LabelId(pub String);

/// JMAP mailbox identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MailboxId(pub String);

/// Blob handle identifier. Opaque; protocol crates resolve it to a URL
/// (JMAP, Gmail, Graph) or to `UID + section` (IMAP).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlobId(pub String);

/// Native thread identifier (JMAP `threadId`, Gmail `threadId`,
/// Graph `conversationId`). Opaque within a protocol; not comparable
/// across protocols.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ThreadId(pub String);

/// Identifier for a registered account (engine-side).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AccountId(pub String);

/// Handle for a push subscription created by `Account::push_subscribe`.
/// Returned to the engine and passed back on `push_unsubscribe`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionHandle(pub String);

/// JMAP `queryChanges` query identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryId(pub String);

/// Mutation campaign run identifier. Consumer-minted, persisted across
/// process restarts so retries correlate with prior-attempt state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RunId(pub String);
