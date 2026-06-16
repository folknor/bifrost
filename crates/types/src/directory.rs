//! Organization-directory (Global Address List) result type.
//!
//! The directory corpus is the org-wide, read-only address list - a
//! distinct corpus from the per-account address books `ContactCard`
//! models. It is surfaced by `Account::directory_search` and backed by
//! provider directory endpoints (Microsoft Graph `/users`, Google
//! People `listDirectoryPeople` / `searchDirectoryPeople`).

use crate::cursor::ProtocolKind;

/// One organization-directory (Global Address List) entry.
///
/// The directory corpus is org-wide and read-only - distinct from the
/// per-account address books `ContactCard` models. A `DirectoryCard` carries
/// no engine-facing id, no etag, and no address-book membership, because no
/// contact/mutation primitive operates on it: it is a lookup result, full
/// stop. `email` is the stable key (both backends skip rows without one).
///
/// Not `#[non_exhaustive]`: like `ContactCard` and `Page`, protocol Account
/// impls construct `DirectoryCard` values directly outside this crate, so a
/// non-exhaustive marker would make every provider mapping uncompilable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryCard {
    /// Primary email address. Always present (rows without one are dropped
    /// at the provider boundary, matching ratatoskr's GAL behavior).
    pub email: String,
    pub display_name: Option<String>,
    /// Additional email addresses beyond `email`, in provider order.
    pub additional_emails: Vec<String>,
    /// Phone numbers in provider order (business/home/mobile flattened;
    /// the directory does not reliably type them).
    pub phones: Vec<String>,
    pub company: Option<String>,
    pub title: Option<String>,
    pub department: Option<String>,
    /// Which protocol family produced this row.
    pub provider: ProtocolKind,
}
