use std::fmt;

use serde::{Deserialize, Serialize};

/// A strongly-typed JMAP identifier.
///
/// Wraps a `String` with a phantom type parameter to prevent mixing
/// IDs from different object types at compile time.
///
/// ```ignore
/// let email_id: Id<Email> = Id::from("msg-123");
/// let mailbox_id: Id<Mailbox> = Id::from("mbox-1");
/// // email_id == mailbox_id  // compile error - different types
/// ```
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Id<T: ?Sized>(String, #[serde(skip)] std::marker::PhantomData<T>);

impl<T: ?Sized> Id<T> {
    pub fn new(id: impl Into<String>) -> Self {
        Id(id.into(), std::marker::PhantomData)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl<T: ?Sized> fmt::Debug for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Id({:?})", self.0)
    }
}

impl<T: ?Sized> fmt::Display for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<T: ?Sized> AsRef<str> for Id<T> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<T: ?Sized> From<String> for Id<T> {
    fn from(s: String) -> Self {
        Id::new(s)
    }
}

impl<T: ?Sized> From<&str> for Id<T> {
    fn from(s: &str) -> Self {
        Id::new(s)
    }
}

impl<T: ?Sized> From<Id<T>> for String {
    fn from(id: Id<T>) -> Self {
        id.0
    }
}

// Marker types for common JMAP object IDs.
// These are zero-sized types used only as phantom parameters.

/// Marker for account IDs.
pub enum Account {}
/// Marker for blob IDs.
pub enum BlobMarker {}
/// Marker for JMAP state tokens.
pub enum StateMarker {}

/// Convenience type aliases.
pub type AccountId = Id<Account>;
pub type BlobId = Id<BlobMarker>;
pub type State = Id<StateMarker>;
