//! Address book and contact-card primitives for the unified PIM surface.
//!
//! Providers expose different native shapes: JMAP has AddressBook and
//! JSContact ContactCard objects, Google has People API resources, Graph
//! has contact folders and contacts, and CardDAV has address books and
//! vCard resources. These types keep the shared surface small while
//! preserving native ids, etags, and protocol provenance for round trips.

use crate::cursor::ProtocolKind;

/// Engine-facing address book identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AddressBookId(pub String);

/// Engine-facing contact identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContactId(pub String);

/// Wire-level provenance for an address book or contact id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContactProvenance {
    /// Which protocol family minted the native id.
    pub provider: ProtocolKind,
    /// Native id string used by the provider on the wire.
    pub native: String,
    /// Native address-book or folder id, when the provider scopes
    /// contacts under a collection.
    pub address_book_native: Option<String>,
}

/// Address book or contact folder exposed by a provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressBook {
    /// Engine-facing identifier.
    pub id: AddressBookId,
    /// Native id string, duplicated from `provenance.native` for
    /// ergonomic access.
    pub native_id: String,
    /// Display name surfaced by the provider.
    pub name: String,
    /// Wire-level metadata for round trips.
    pub provenance: ContactProvenance,
    /// True when this is the provider's default personal address book.
    pub is_default: bool,
    /// True when contacts can be created in this address book.
    pub can_create_contacts: bool,
    /// True when contacts in this address book can be updated.
    pub can_update_contacts: bool,
    /// True when contacts in this address book can be deleted.
    pub can_delete_contacts: bool,
}

/// A typed email address on a contact card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContactEmail {
    pub value: String,
    /// Provider-native label such as `home`, `work`, or `other`.
    pub kind: Option<String>,
    pub is_primary: bool,
}

/// A typed phone number on a contact card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContactPhone {
    pub value: String,
    /// Provider-native label such as `mobile`, `home`, or `work`.
    pub kind: Option<String>,
    pub is_primary: bool,
}

/// Organization data on a contact card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContactOrganization {
    pub name: String,
    pub title: Option<String>,
}

/// Unified contact card returned by contact primitives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContactCard {
    /// Engine-facing identifier.
    pub id: ContactId,
    /// Address book containing this contact, when known.
    pub address_book_id: Option<AddressBookId>,
    /// Native contact id string, duplicated from `provenance.native`.
    pub native_id: String,
    /// Provider etag, change key, or similar optimistic-concurrency
    /// token when available.
    pub etag: Option<String>,
    /// Wire-level metadata for round trips.
    pub provenance: ContactProvenance,
    pub display_name: Option<String>,
    pub emails: Vec<ContactEmail>,
    pub phones: Vec<ContactPhone>,
    pub organizations: Vec<ContactOrganization>,
    pub notes: Option<String>,
    pub photo_url: Option<String>,
}

/// Contact creation payload.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContactCreate {
    pub address_book_id: Option<AddressBookId>,
    pub display_name: Option<String>,
    pub emails: Vec<ContactEmail>,
    pub phones: Vec<ContactPhone>,
    pub organizations: Vec<ContactOrganization>,
    pub notes: Option<String>,
    pub photo_url: Option<String>,
}

/// Partial contact update payload.
///
/// `None` means leave the field untouched. `Some(None)` clears a
/// nullable scalar. Repeated fields replace the full provider-side
/// collection when present.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContactPatch {
    pub address_book_id: Option<AddressBookId>,
    pub display_name: Option<Option<String>>,
    pub emails: Option<Vec<ContactEmail>>,
    pub phones: Option<Vec<ContactPhone>>,
    pub organizations: Option<Vec<ContactOrganization>>,
    pub notes: Option<Option<String>>,
    pub photo_url: Option<Option<String>>,
}

/// Search request for provider-side contact lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContactSearchRequest {
    pub query: String,
    pub address_book_id: Option<AddressBookId>,
    pub page_cursor: Option<Vec<u8>>,
    pub limit: Option<u32>,
}

impl ContactSearchRequest {
    #[must_use]
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            address_book_id: None,
            page_cursor: None,
            limit: None,
        }
    }
}
