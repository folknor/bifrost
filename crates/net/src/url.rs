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
    .add(b']');

/// Percent-encode one URL path or query component while preserving
/// RFC 3986 unreserved characters.
#[must_use]
pub fn encode_component(value: &str) -> String {
    utf8_percent_encode(value, COMPONENT).to_string()
}

#[cfg(test)]
mod tests {
    use super::encode_component;

    #[test]
    fn unreserved_characters_survive_untouched() {
        assert_eq!(
            encode_component("aZ0-._~"),
            "aZ0-._~",
            "RFC 3986 unreserved characters must never be escaped"
        );
        assert_eq!(encode_component(""), "");
    }

    /// The delimiters that would otherwise change the meaning of the
    /// spliced URL. `/` splits a path segment, `?` opens a query, `#`
    /// opens a fragment, `&` and `=` split query pairs, `%` would start
    /// a bogus escape.
    #[test]
    fn structural_delimiters_are_escaped() {
        assert_eq!(encode_component("a/b"), "a%2Fb");
        assert_eq!(encode_component("a?b"), "a%3Fb");
        assert_eq!(encode_component("a#b"), "a%23b");
        assert_eq!(encode_component("a&b=c"), "a%26b%3Dc");
        assert_eq!(encode_component("100%"), "100%25");
        assert_eq!(encode_component("a b"), "a%20b");
        assert_eq!(encode_component("a+b"), "a%2Bb");
        assert_eq!(encode_component("user@host"), "user%40host");
        assert_eq!(encode_component("a:b"), "a%3Ab");
    }

    #[test]
    fn control_characters_and_non_ascii_are_escaped() {
        assert_eq!(encode_component("a\nb"), "a%0Ab");
        assert_eq!(encode_component("a\0b"), "a%00b");
        assert_eq!(encode_component("\u{7f}"), "%7F");
        assert_eq!(
            encode_component("naïve"),
            "na%C3%AFve",
            "non-ASCII is UTF-8 percent-encoded byte by byte"
        );
        assert_eq!(encode_component("日本"), "%E6%97%A5%E6%9C%AC");
    }

    /// DOCUMENTS CURRENT BEHAVIOUR, NOT AN ENDORSEMENT. The doc comment
    /// claims everything outside the unreserved set is escaped, but the
    /// `COMPONENT` set omits `<`, `>`, `\`, `^`, backtick, `{`, `|`,
    /// `}` and `!`. The backslash is the load-bearing one: the WHATWG
    /// URL parser that `reqwest::Url` implements treats `\` as a path
    /// separator for `http`/`https`, so an encoded component containing
    /// one still splits the path. See the `..` case below.
    #[test]
    fn characters_the_component_set_currently_leaves_unescaped() {
        assert_eq!(encode_component("a\\b"), "a\\b");
        assert_eq!(encode_component("a<b>c"), "a<b>c");
        assert_eq!(encode_component("a{b}c"), "a{b}c");
        assert_eq!(encode_component("a|b"), "a|b");
        assert_eq!(encode_component("a^b`c"), "a^b`c");
        assert_eq!(encode_component("a!b"), "a!b");
    }

    /// DOCUMENTS CURRENT BEHAVIOUR, NOT AN ENDORSEMENT. `.` is
    /// unreserved so a component that is exactly `..` passes through
    /// intact, and the URL parser then resolves it as a parent-segment
    /// traversal. A provider id that a consumer does not sanitise can
    /// therefore retarget the request path.
    #[test]
    fn dot_dot_component_passes_through_and_is_resolved_by_the_parser() {
        assert_eq!(encode_component(".."), "..");
        let resolved = reqwest::Url::parse("https://h.example/a/b/")
            .expect("base parses")
            .join(&encode_component(".."))
            .expect("join succeeds");
        assert_eq!(
            resolved.path(),
            "/a/",
            "the traversal is honored by the parser, not neutralised by the encoder"
        );
    }

    /// Idempotence check: encoding an already-encoded value double
    /// escapes the `%`, so callers must encode exactly once.
    #[test]
    fn encoding_is_not_idempotent() {
        assert_eq!(encode_component(&encode_component("a/b")), "a%252Fb");
    }
}
