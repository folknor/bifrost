//! HTTP status-line parsing.
//!
//! Split out because WebDAV bodies carry status LINES as element text
//! rather than as a transport-level status: RFC 4918's `<D:status>`
//! inside a `<D:propstat>` holds an HTTP status-line, and a 207
//! Multi-Status response is only meaningful once each of those is read.
//! Both DAV clients need it, so it lives here rather than being spelled
//! out once per crate - it was previously duplicated across
//! `bifrost-caldav` and `bifrost-carddav`, kept in step by a comment in
//! one of them saying it matched the other.
//!
//! HTTP rather than DAV by nature: what is parsed is
//! `HTTP-version SP status-code SP reason-phrase`, which is RFC 9112's
//! status line, not anything WebDAV invented.

/// Extract the numeric status from an HTTP status line.
///
/// Takes the first whitespace-delimited token that parses as a number,
/// rather than positionally assuming the leading `HTTP/x` token is
/// present. Servers do emit the protocol-less form (`200 OK`) inside
/// `<D:status>`, and reading position 1 unconditionally would take
/// `OK` as the code there and classify a perfectly good propstat as
/// failed.
///
/// Returns `None` when no token parses as a number.
#[must_use]
pub fn status_line_code(status: &str) -> Option<u16> {
    status
        .split_whitespace()
        .find_map(|part| part.parse::<u16>().ok())
}

/// True when an HTTP status line carries a 2xx code.
///
/// The single spelling of the success predicate. It previously existed
/// in three: `matches!(code, 200..=299)`, `(200..=299).contains(&code)`,
/// and a named `is_success_status` in the other DAV crate.
///
/// A line with no parseable code is NOT success. An unreadable status is
/// not evidence that the property was returned - treating it as success
/// would commit a value the server may have refused.
#[must_use]
pub fn status_line_is_success(status: &str) -> bool {
    status_line_code(status).is_some_and(|code| (200..=299).contains(&code))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The case the positional parse gets wrong: no `HTTP/x` token.
    #[test]
    fn a_status_line_without_a_protocol_token_still_classifies() {
        assert_eq!(status_line_code("200 OK"), Some(200));
        assert!(status_line_is_success("200 OK"));
        assert!(!status_line_is_success("404 Not Found"));
    }

    #[test]
    fn a_full_status_line_classifies_on_its_code() {
        assert_eq!(status_line_code("HTTP/1.1 207 Multi-Status"), Some(207));
        assert!(status_line_is_success("HTTP/1.1 207 Multi-Status"));
        assert!(!status_line_is_success("HTTP/1.1 404 Not Found"));
    }

    /// 207 and 226 are inside 2xx and must classify as success; 3xx must
    /// not. Pins the range ends rather than trusting the literal.
    #[test]
    fn the_success_range_covers_all_of_2xx_and_nothing_else() {
        assert!(status_line_is_success("HTTP/1.1 200 OK"));
        assert!(status_line_is_success("HTTP/1.1 299 Whatever"));
        assert!(!status_line_is_success("HTTP/1.1 199 Early Hints"));
        assert!(!status_line_is_success("HTTP/1.1 300 Multiple Choices"));
    }

    /// An unreadable status is not success. Committing a property whose
    /// status could not be read would silently accept one the server may
    /// have refused.
    #[test]
    fn an_unparseable_status_is_not_success() {
        assert_eq!(status_line_code("HTTP/1.1 OK"), None);
        assert!(!status_line_is_success("HTTP/1.1 OK"));
        assert!(!status_line_is_success(""));
        assert!(!status_line_is_success("garbage"));
    }

    /// `HTTP/1.1` contains no bare numeric token (the `1.1` fails a `u16`
    /// parse), so the version cannot be mistaken for the code.
    #[test]
    fn the_http_version_is_not_read_as_the_code() {
        assert_eq!(status_line_code("HTTP/1.1 500 Server Error"), Some(500));
    }
}
