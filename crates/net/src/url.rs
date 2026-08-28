//! URL encoding helpers shared by HTTP protocol crates.

use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};

const COMPONENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'=')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b']')
    .add(b'!')
    .add(b'<')
    .add(b'>')
    .add(b'\\')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

/// Percent-encode one URL query value while preserving RFC 3986
/// unreserved characters.
#[must_use]
pub fn encode_query_value(value: &str) -> String {
    utf8_percent_encode(value, COMPONENT).to_string()
}

/// Percent-encode one URL path component.
///
/// Complete `.` and `..` components are double-escaped because the
/// WHATWG URL parser treats both literal and singly escaped forms as
/// path navigation. Query values must use [`encode_query_value`]
/// instead, where dots have no structural meaning.
#[must_use]
pub fn encode_path_component(value: &str) -> String {
    // WHATWG URL parsing treats literal and percent-encoded `.` and
    // `..` path segments as navigation. Double-escape the percent
    // signs for those two complete components so parsing the full URL
    // cannot resolve a provider id as a parent/current segment.
    if value == "." {
        return "%252E".to_string();
    }
    if value == ".." {
        return "%252E%252E".to_string();
    }
    encode_query_value(value)
}

/// Return the parent collection URL of an absolute resource URL.
///
/// Query and fragment data are discarded before removing the final path
/// segment, so slashes in either cannot be mistaken for path separators.
#[must_use]
pub fn parent_collection_url(resource_url: &str) -> Option<String> {
    let mut url = reqwest::Url::parse(resource_url).ok()?;
    url.set_query(None);
    url.set_fragment(None);
    {
        let mut segments = url.path_segments_mut().ok()?;
        segments.pop_if_empty();
        segments.pop();
        segments.push("");
    }
    Some(url.to_string())
}

/// Return the origin-rooted `/.well-known/<service>` URL for a base URL.
///
/// RFC 6764 well-known discovery is defined at the ORIGIN root, not
/// relative to whatever path the account happens to be configured with.
/// A configured base of `https://host/service` therefore probes
/// `https://host/.well-known/caldav`, never
/// `https://host/service/.well-known/caldav`. Query and fragment on the
/// base are irrelevant to discovery and are dropped.
///
/// Returns `None` when `base_url` does not parse or cannot carry a path
/// (a cannot-be-a-base URL such as `mailto:`); callers treat that as
/// "no well-known probe available" and go straight to the configured
/// base.
#[must_use]
pub fn well_known_url(base_url: &str, service: &str) -> Option<String> {
    let mut url = reqwest::Url::parse(base_url).ok()?;
    if url.cannot_be_a_base() {
        return None;
    }
    url.set_query(None);
    url.set_fragment(None);
    url.set_path(&format!("/.well-known/{service}"));
    Some(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::{encode_path_component, encode_query_value, parent_collection_url, well_known_url};

    /// The path-bearing base is the case that separates origin-rooted
    /// construction from string concatenation; a path-less base makes
    /// both spellings agree and proves nothing.
    #[test]
    fn well_known_is_rooted_at_the_origin() {
        assert_eq!(
            well_known_url("https://dav.example.test/service", "caldav").as_deref(),
            Some("https://dav.example.test/.well-known/caldav")
        );
        assert_eq!(
            well_known_url("https://dav.example.test/a/b/c/", "carddav").as_deref(),
            Some("https://dav.example.test/.well-known/carddav")
        );
        assert_eq!(
            well_known_url("https://dav.example.test", "caldav").as_deref(),
            Some("https://dav.example.test/.well-known/caldav")
        );
    }

    #[test]
    fn well_known_drops_query_and_fragment_and_keeps_port_and_userinfo() {
        assert_eq!(
            well_known_url("https://dav.example.test/service?a=b#frag", "caldav").as_deref(),
            Some("https://dav.example.test/.well-known/caldav")
        );
        assert_eq!(
            well_known_url("https://dav.example.test:8443/service", "carddav").as_deref(),
            Some("https://dav.example.test:8443/.well-known/carddav")
        );
    }

    #[test]
    fn well_known_rejects_unusable_bases() {
        assert_eq!(well_known_url("not-a-url", "caldav"), None);
        assert_eq!(well_known_url("mailto:user@example.test", "caldav"), None);
    }

    #[test]
    fn parent_collection_uses_only_path_segments() {
        assert_eq!(
            parent_collection_url("https://dav.test/cal/one.ics?redirect=/foo").as_deref(),
            Some("https://dav.test/cal/")
        );
        assert_eq!(
            parent_collection_url("https://dav.test/cal/one.ics#a/b").as_deref(),
            Some("https://dav.test/cal/")
        );
        assert_eq!(parent_collection_url("not-a-url"), None);
    }

    #[test]
    fn unreserved_characters_survive_untouched() {
        assert_eq!(
            encode_query_value("aZ0-._~"),
            "aZ0-._~",
            "RFC 3986 unreserved characters must never be escaped"
        );
        assert_eq!(encode_query_value(""), "");
    }

    /// The delimiters that would otherwise change the meaning of the
    /// spliced URL. `/` splits a path segment, `?` opens a query, `#`
    /// opens a fragment, `&` and `=` split query pairs, `%` would start
    /// a bogus escape.
    #[test]
    fn structural_delimiters_are_escaped() {
        assert_eq!(encode_query_value("a/b"), "a%2Fb");
        assert_eq!(encode_query_value("a?b"), "a%3Fb");
        assert_eq!(encode_query_value("a#b"), "a%23b");
        assert_eq!(encode_query_value("a&b=c"), "a%26b%3Dc");
        assert_eq!(encode_query_value("100%"), "100%25");
        assert_eq!(encode_query_value("a b"), "a%20b");
        assert_eq!(encode_query_value("a+b"), "a%2Bb");
        assert_eq!(encode_query_value("user@host"), "user%40host");
        assert_eq!(encode_query_value("a:b"), "a%3Ab");
    }

    #[test]
    fn control_characters_and_non_ascii_are_escaped() {
        assert_eq!(encode_query_value("a\nb"), "a%0Ab");
        assert_eq!(encode_query_value("a\0b"), "a%00b");
        assert_eq!(encode_query_value("\u{7f}"), "%7F");
        assert_eq!(
            encode_query_value("naïve"),
            "na%C3%AFve",
            "non-ASCII is UTF-8 percent-encoded byte by byte"
        );
        assert_eq!(encode_query_value("日本"), "%E6%97%A5%E6%9C%AC");
    }

    #[test]
    fn every_non_unreserved_ascii_character_is_escaped() {
        assert_eq!(encode_query_value("a\\b"), "a%5Cb");
        assert_eq!(encode_query_value("a<b>c"), "a%3Cb%3Ec");
        assert_eq!(encode_query_value("a{b}c"), "a%7Bb%7Dc");
        assert_eq!(encode_query_value("a|b"), "a%7Cb");
        assert_eq!(encode_query_value("a^b`c"), "a%5Eb%60c");
        assert_eq!(encode_query_value("a!b"), "a%21b");
    }

    #[test]
    fn complete_dot_segments_cannot_navigate_the_parsed_url() {
        assert_eq!(encode_path_component("."), "%252E");
        assert_eq!(encode_path_component(".."), "%252E%252E");
        let resolved = reqwest::Url::parse("https://h.example/a/b/")
            .expect("base parses")
            .join(&encode_path_component(".."))
            .expect("join succeeds");
        assert_eq!(
            resolved.path(),
            "/a/b/%252E%252E",
            "the encoded provider id remains in its original path position"
        );
    }

    #[test]
    fn complete_dot_query_values_are_not_double_escaped() {
        assert_eq!(encode_query_value("."), ".");
        assert_eq!(encode_query_value(".."), "..");
    }

    /// Idempotence check: encoding an already-encoded value double
    /// escapes the `%`, so callers must encode exactly once.
    #[test]
    fn encoding_is_not_idempotent() {
        assert_eq!(encode_query_value(&encode_query_value("a/b")), "a%252Fb");
    }
}
