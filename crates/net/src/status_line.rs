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
/// Reads the first token for the protocol-less form (`200 OK`), or the
/// token immediately after an `HTTP/` version. It never scans prose for
/// a later number.
///
/// A token only counts when it parses as a `u16` in RFC 9110's
/// `100..=599` status range. Without the range constraint any bare
/// number anywhere in the line was accepted, so a server's prose
/// (`"Error 42 occurred"`, a `<D:status>` a proxy filled with free
/// text) parsed as status 42 and the caller classified it against a
/// code the server never sent. An out-of-range number is not a status,
/// and reporting `None` puts the line in the unreadable bucket, which
/// `status_line_is_success` already fails closed on.
///
/// Returns `None` when the status position does not contain an in-range code.
#[must_use]
pub fn status_line_code(status: &str) -> Option<u16> {
    let mut parts = status.split_whitespace();
    let first = parts.next()?;
    // The version token is matched case-insensitively. RFC 9112 spells
    // it uppercase, but a lowercase `http/1.1` used to reach the code
    // anyway (the version token simply failed to parse as a number),
    // and tightening the parser must not turn a status line some server
    // already emits into an unreadable one.
    let code = if first
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("HTTP/"))
    {
        parts.next()?
    } else {
        first
    };
    code.parse::<u16>()
        .ok()
        .filter(|code| (100..=599).contains(code))
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

    /// A bare number in prose is not a status code. Without the range
    /// constraint this parsed as status 42 and the caller classified a
    /// `<D:status>` against a code the server never sent.
    #[test]
    fn a_number_outside_the_status_range_is_not_a_code() {
        assert_eq!(status_line_code("Error 42 occurred"), None);
        assert_eq!(status_line_code("Error 404 occurred"), None);
        assert!(!status_line_is_success("Error 42 occurred"));
        assert_eq!(status_line_code("HTTP/1.1 600 Nonsense"), None);
        assert_eq!(status_line_code("HTTP/1.1 0 Nonsense"), None);
        assert_eq!(status_line_code("99 Too Low"), None);
    }

    /// Boundaries of the positional read itself. The version token is
    /// matched case-insensitively because a lowercase `http/1.1` reached
    /// the code under the old scan-for-a-number parser, and tightening
    /// must not make a line that already worked unreadable.
    #[test]
    fn the_status_position_is_read_positionally_and_tolerates_version_case() {
        assert_eq!(status_line_code("http/1.1 200 OK"), Some(200));
        assert_eq!(status_line_code("HTTP/2 200 OK"), Some(200));
        assert_eq!(status_line_code("   HTTP/1.1 204 No Content"), Some(204));
        // Code with no reason phrase: the shortest legal shape either way.
        assert_eq!(status_line_code("204"), Some(204));
        assert_eq!(status_line_code("HTTP/1.1 204"), Some(204));
        // Version present but truncated before the status position.
        assert_eq!(status_line_code("HTTP/1.1"), None);
        assert_eq!(status_line_code("HTTP/1.1 "), None);
        // A first token whose fifth byte falls inside a multi-byte char
        // must not panic on the prefix slice.
        assert_eq!(status_line_code("1234\u{e9} 404"), None);
        // Something that is not a version and not a code stays unreadable
        // even when a valid-looking code follows it.
        assert_eq!(status_line_code("Status: 404 Not Found"), None);
    }

    /// The range ends are inclusive, so a legitimate 1xx or 5xx line
    /// still reads.
    #[test]
    fn the_status_range_ends_are_inclusive() {
        assert_eq!(status_line_code("HTTP/1.1 100 Continue"), Some(100));
        assert_eq!(status_line_code("HTTP/1.1 599 Whatever"), Some(599));
    }
}
