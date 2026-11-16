//! Caller-owned Google page walks, distinct from durable sync cursors.
//!
//! Every surface's cursor is tagged with its stable AccountOperation ledger
//! tag. Non-search surfaces use `google-page-1:{tag}:{token}`. Gmail search
//! (threads and messages) uses `google-page-2:{tag}:{len}:{q}{token}`, which
//! also binds the exact `q` the provider token was issued for: Gmail does not
//! define a `pageToken` sent with a different `q`, and an enterprise Outlook
//! user who changes a query expects fresh results, not a continuation through
//! another query. Search cursors in the older layout are refused.

use bifrost_types::{AccountError, AccountOperation, PageCursor, Protocol};

const PAGE_V1: &str = "google-page-1";
const SEARCH_V2: &str = "google-page-2";

fn protocol(operation: AccountOperation) -> Protocol {
    match operation {
        AccountOperation::Search | AccountOperation::SearchMessages => Protocol::Gmail,
        AccountOperation::EventsInRange | AccountOperation::EventSearch => Protocol::GoogleCalendar,
        _ => Protocol::GooglePeople,
    }
}

fn is_search(operation: AccountOperation) -> bool {
    matches!(
        operation,
        AccountOperation::Search | AccountOperation::SearchMessages
    )
}

fn invalid(operation: AccountOperation, detail: &'static str) -> AccountError {
    AccountError::invalid_page_cursor(operation, protocol(operation), detail)
}

/// Non-search surfaces only; Gmail search mints through `encode_search`.
pub(crate) fn encode(operation: AccountOperation, token: &str) -> PageCursor {
    debug_assert!(!is_search(operation), "search cursors bind their query");
    format!("{PAGE_V1}:{}:{token}", operation.ledger_tag())
        .into_bytes()
        .into()
}

pub(crate) fn token(cursor: &[u8], operation: AccountOperation) -> Result<&str, AccountError> {
    let malformed = || {
        invalid(
            operation,
            "malformed, retired or foreign Google page cursor",
        )
    };
    if is_search(operation) {
        return Err(malformed());
    }
    let text = std::str::from_utf8(cursor).map_err(|_| malformed())?;
    // An Outlook user expects stale saved results to reload, without clearing
    // durable account state or sending another surface's token to Google.
    text.strip_prefix(&format!("{PAGE_V1}:{}:", operation.ledger_tag()))
        .ok_or_else(malformed)
}

pub(crate) fn decode(
    cursor: Option<PageCursor>,
    operation: AccountOperation,
) -> Result<Option<String>, AccountError> {
    cursor
        .map(|cursor| token(&cursor, operation).map(str::to_owned))
        .transpose()
}

/// A Gmail search cursor for `token`, bound to the exact `query` (the `q`
/// sent with the request that returned it; empty when none was sent).
pub(crate) fn encode_search(operation: AccountOperation, query: &str, token: &str) -> PageCursor {
    debug_assert!(is_search(operation));
    format!(
        "{SEARCH_V2}:{}:{}:{query}{token}",
        operation.ledger_tag(),
        query.len()
    )
    .into_bytes()
    .into()
}

/// The provider token of a Gmail search cursor, refused unless it was minted
/// by the same surface for exactly `query`. The binding is the emitted `q`,
/// not the structured request: two requests that compile to the same `q` are
/// the same Gmail search, and `limit` is presentation only. If the request
/// ever gains another selection parameter (for example a caller-chosen
/// `includeSpamTrash`), it must join the binding.
pub(crate) fn search_token<'a>(
    cursor: &'a [u8],
    operation: AccountOperation,
    query: &str,
) -> Result<&'a str, AccountError> {
    let malformed = || {
        invalid(
            operation,
            "malformed, retired or foreign Google page cursor",
        )
    };
    if !is_search(operation) {
        return Err(malformed());
    }
    let text = std::str::from_utf8(cursor).map_err(|_| malformed())?;
    let rest = text
        .strip_prefix(&format!("{SEARCH_V2}:{}:", operation.ledger_tag()))
        .ok_or_else(malformed)?;
    let (len, rest) = rest.split_once(':').ok_or_else(malformed)?;
    if len.is_empty() || !len.bytes().all(|b| b.is_ascii_digit()) {
        return Err(malformed());
    }
    let len: usize = len.parse().map_err(|_| malformed())?;
    let bound = rest.get(..len).ok_or_else(malformed)?;
    let token = &rest[len..];
    if token.is_empty() {
        return Err(malformed());
    }
    if bound != query {
        return Err(invalid(
            operation,
            "Gmail page cursor does not match this search",
        ));
    }
    Ok(token)
}

pub(crate) fn decode_search(
    cursor: Option<&PageCursor>,
    operation: AccountOperation,
    query: &str,
) -> Result<Option<String>, AccountError> {
    cursor
        .map(|cursor| search_token(cursor, operation, query).map(str::to_owned))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{AccountErrorKind, Cause, RequestCause, RequestErrorKind};

    const SEARCHES: [AccountOperation; 2] =
        [AccountOperation::Search, AccountOperation::SearchMessages];
    const OTHERS: [AccountOperation; 5] = [
        AccountOperation::ContactsList,
        AccountOperation::ContactSearch,
        AccountOperation::DirectorySearch,
        AccountOperation::EventsInRange,
        AccountOperation::EventSearch,
    ];

    fn assert_restart(error: &AccountError, used: AccountOperation) {
        assert_eq!(error.operation(), Some(used));
        assert_eq!(error.protocol(), Some(protocol(used)));
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
        assert!(error.chain().iter().any(|cause| matches!(
            cause,
            Cause::Request(RequestCause::InvalidArgument {
                field: Some("page_cursor"),
                ..
            })
        )));
    }

    fn any_token(
        cursor: &[u8],
        used: AccountOperation,
        query: &str,
    ) -> Result<String, AccountError> {
        if is_search(used) {
            search_token(cursor, used, query).map(str::to_owned)
        } else {
            token(cursor, used).map(str::to_owned)
        }
    }

    fn mint(operation: AccountOperation, query: &str, token: &str) -> PageCursor {
        if is_search(operation) {
            encode_search(operation, query, token)
        } else {
            encode(operation, token)
        }
    }

    #[test]
    fn page_cursors_are_surface_bound_and_share_the_restart_signal() {
        let operations = SEARCHES.into_iter().chain(OTHERS).collect::<Vec<_>>();
        for &minted in &operations {
            let cursor = mint(minted, "from:a", "opaque:token");
            assert_eq!(
                any_token(&cursor, minted, "from:a").unwrap(),
                "opaque:token"
            );
            for &used in operations.iter().filter(|used| **used != minted) {
                let error = any_token(&cursor, used, "from:a").expect_err("wrong surface");
                assert_restart(&error, used);
            }
        }
        assert!(token(&[0xff], AccountOperation::ContactsList).is_err());
        assert!(search_token(&[0xff], AccountOperation::SearchMessages, "").is_err());
        assert!(token(b"bare old token", AccountOperation::ContactsList).is_err());
    }

    /// A search cursor answers only the `q` it was minted for; `q` may hold
    /// the layout's own delimiter, and the retired unbound layout is refused.
    #[test]
    fn search_cursors_are_bound_to_their_query() {
        for op in SEARCHES {
            for query in ["", "from:a", "subject:\"a:b\" 12:x", "é"] {
                let cursor = encode_search(op, query, "tok:en");
                assert_eq!(search_token(&cursor, op, query).unwrap(), "tok:en");
                for other in ["", "from:b", "from:a ", "subject:\"a:b\""] {
                    if other == query {
                        continue;
                    }
                    let error = search_token(&cursor, op, other).expect_err("other query");
                    assert_restart(&error, op);
                }
            }
            let retired = format!("{PAGE_V1}:{}:token", op.ledger_tag()).into_bytes();
            assert_restart(&search_token(&retired, op, "").expect_err("retired"), op);
            assert_restart(&token(&retired, op).expect_err("retired"), op);
            for bad in ["9:x", "1:", "x:ab", ":ab", "01a"] {
                let cursor = format!("{SEARCH_V2}:{}:{bad}", op.ledger_tag()).into_bytes();
                assert!(search_token(&cursor, op, "x").is_err(), "{bad}");
            }
        }
    }
}
