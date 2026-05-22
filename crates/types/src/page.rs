//! Generic paginated result envelope.
//!
//! Used by `Account::search` and `Account::search_messages` to return
//! a slice of results plus an opaque cursor a follow-up call can pass
//! back to fetch the next page. The cursor is protocol-owned bytes;
//! the engine and consumers treat it as opaque the same way they
//! treat `OpaqueProgressBytes` in the inventory pipeline.

/// Paginated result envelope.
///
/// Not `#[non_exhaustive]` because protocol Account impls construct
/// `Page` values directly.
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
}

impl<T> Page<T> {
    /// Single-page result.
    #[must_use]
    pub fn single(items: Vec<T>) -> Self {
        Self {
            items,
            next_cursor: None,
            estimated_total: None,
        }
    }

    /// True iff the protocol indicated this page is the last.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.next_cursor.is_none()
    }
}
