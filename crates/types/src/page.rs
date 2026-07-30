//! Generic paginated result envelope.
//!
//! Used by `Account::search` and `Account::search_messages` to return
//! a slice of results plus an opaque cursor a follow-up call can pass
//! back to fetch the next page. The cursor is protocol-owned bytes;
//! the engine and consumers treat it as opaque the same way they
//! treat `OpaqueProgressBytes` in the inventory pipeline.

use crate::error::{AccountError, ErrorScope};

/// Paginated result envelope.
///
/// Not `#[non_exhaustive]` because protocol Account impls construct
/// `Page` values directly - and deliberately so: adding a lane to this
/// envelope must break every constructor, so each one answers the new
/// question instead of silently defaulting it.
#[derive(Debug, Clone)]
pub struct Page<T> {
    /// Items in this page.
    pub items: Vec<T>,
    /// Opaque cursor to pass to the next call to resume after the
    /// last item in `items`. `None` when the underlying primitive
    /// returned its final page.
    pub next_cursor: Option<Vec<u8>>,
    /// Server-reported total when known. `None` when the protocol
    /// does not expose an estimate (most do not for search).
    pub estimated_total: Option<u64>,
    /// Native identifiers of resources the provider fetched for this
    /// page but could not materialize into an item - for example a
    /// per-resource parse failure inside an otherwise successful
    /// multi-status response. Surfacing them lets a consumer tell a
    /// transient per-resource failure apart from a real remote
    /// deletion. Empty for primitives that have no such notion.
    pub failed_ids: Vec<String>,
    /// Scopes a multi-scope walk quarantined instead of visiting - for
    /// example a shared mailbox whose delegate access has been revoked
    /// mid-walk. An entry means the walk did not search (or did not
    /// finish searching) that scope, which is a different fact from
    /// "no matches there": `items` (and every earlier page's items)
    /// remain valid, but absence of results from a skipped scope is
    /// not evidence of absence. Distinct from
    /// `failed_ids`, which names RESOURCES in the item id namespace;
    /// these name the scope that was skipped and carry the classified
    /// failure that caused the skip. Empty for single-scope
    /// primitives and for walks where every scope answered.
    pub skipped_scopes: Vec<SkippedScope>,
}

/// One scope a paginated walk skipped rather than visited.
///
/// Like `Page`, deliberately not `#[non_exhaustive]`: protocol Account
/// impls construct it directly.
#[derive(Debug, Clone)]
pub struct SkippedScope {
    /// Where the skip happened - e.g. `ErrorScope::Mailbox` for a
    /// shared mailbox the account has lost delegate access to. Kept
    /// mandatory and separate from `error`'s own optional scope: the
    /// whole point of the entry is WHICH scope went unsearched.
    pub scope: ErrorScope,
    /// The classified failure that caused the skip, carrying the full
    /// recovery classification and diagnostics (for a revoked shared
    /// mailbox: terminal `NoPermission` plus the provider's native
    /// code). Advisory here - the walk continued - so it never rides
    /// the call's `Err` arm.
    pub error: AccountError,
}

impl<T> Page<T> {
    /// Single-page result.
    #[must_use]
    pub fn single(items: Vec<T>) -> Self {
        Self {
            items,
            next_cursor: None,
            estimated_total: None,
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        }
    }

    /// True iff the protocol indicated this page is the last.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.next_cursor.is_none()
    }
}
