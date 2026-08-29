//! ETag normalization and conditional-write preparation.
//!
//! Both DAV crates carried these three functions byte for byte.

use reqwest::header::HeaderMap;

/// How a PUT is conditioned.
#[derive(Debug, Clone, Copy)]
pub enum PutCondition<'a> {
    /// The resource must not already exist.
    IfNoneMatch,
    /// The resource must still carry this validator.
    IfMatch(&'a str),
    /// Unconditional.
    None,
}

#[must_use]
pub fn response_etag(headers: &HeaderMap) -> Option<String> {
    headers
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .map(normalize_http_etag)
}

/// Normalize a wire ETag for snapshot comparison.
///
/// A weak validator KEEPS its `W/` marker, because the marker is what
/// `prepare_if_match` later reads to refuse an unsafe conditional write. A
/// strong one is unquoted so snapshot equality does not depend on quoting.
#[must_use]
pub fn normalize_http_etag(value: &str) -> String {
    let value = value.trim();
    if value
        .get(..2)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("W/"))
    {
        format!("W/{}", value[2..].trim())
    } else {
        value.trim_matches('"').to_string()
    }
}

/// The `If-Match` value for an etag, or `None` when no conforming one exists.
///
/// RFC 7232 requires STRONG comparison for `If-Match`, so a weak validator has
/// no conforming conditional form and the write must go out unconditional.
/// Against a server that only ever emits weak ETags this means updates have no
/// lost-update protection at all; no better option exists inside HTTP, and a
/// consumer needing the guarantee needs an application-level revision check.
#[must_use]
pub fn prepare_if_match(etag: &str) -> Option<String> {
    if etag
        .get(..2)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("W/"))
    {
        None
    } else if etag.starts_with('"') {
        Some(etag.to_string())
    } else {
        Some(format!("\"{etag}\""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weak_etag_is_never_sent_in_if_match() {
        assert_eq!(prepare_if_match("W/\"abc\""), None);
        assert_eq!(prepare_if_match("w/\"abc\""), None);
        assert_eq!(prepare_if_match("abc").as_deref(), Some("\"abc\""));
        assert_eq!(prepare_if_match("\"abc\"").as_deref(), Some("\"abc\""));
    }

    #[test]
    fn normalization_keeps_the_weak_marker_and_drops_strong_quotes() {
        assert_eq!(normalize_http_etag("  \"abc\" "), "abc");
        assert_eq!(normalize_http_etag("W/ \"abc\""), "W/\"abc\"");
        assert_eq!(normalize_http_etag("w/\"abc\""), "W/\"abc\"");
    }
}
