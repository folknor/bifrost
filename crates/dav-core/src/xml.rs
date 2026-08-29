//! XML decoding primitives shared by both DAV parsers.
//!
//! These five were byte-identical in `bifrost-caldav::parse` and
//! `bifrost-carddav::parse`. The parsers ABOVE them are not shared and are not
//! meant to be: `PropStat`, `ResponseParts` and `parse_multiget_report` carry
//! genuinely different property sets and entry types (calendar-data versus
//! address-data), and unifying those would mean parameterizing the parser over
//! the resource kind - a redesign, not a move. See `reference/caldav.md` for
//! where the extraction stops and why.

use quick_xml::escape::unescape;
use reqwest::Url;

/// The local part of a namespaced XML name (`D:href` -> `href`).
#[must_use]
pub fn local_name(raw: &[u8]) -> String {
    let full = String::from_utf8_lossy(raw);
    match full.rfind(':') {
        Some(index) => full[index + 1..].to_string(),
        None => full.to_string(),
    }
}

/// Normalize an ETag out of a DAV property.
///
/// A weak validator keeps its `W/` marker; a strong one loses its quotes, so
/// snapshot equality does not depend on quoting.
#[must_use]
pub fn normalize_etag(text: &str) -> Option<String> {
    trimmed(text).map(|value| {
        value
            .get(..2)
            .filter(|prefix| prefix.eq_ignore_ascii_case("W/"))
            .map_or_else(
                || value.trim_matches('"').to_string(),
                |_| format!("W/{}", value[2..].trim()),
            )
    })
}

/// Rebase a DAV response href against its request URI at the XML decoding
/// boundary. Client callers never expose parsed relative hrefs to the account
/// layer.
#[must_use]
pub fn resolve_href(request_url: &str, href: &str) -> String {
    if href.starts_with("http://") || href.starts_with("https://") {
        return href.to_string();
    }
    if let Ok(base) = Url::parse(request_url)
        && let Ok(resolved) = base.join(href)
    {
        return resolved.to_string();
    }
    if request_url.ends_with('/') || href.starts_with('/') {
        format!("{request_url}{href}")
    } else {
        format!("{request_url}/{href}")
    }
}

/// Append unescaped XML text to an accumulator, so a value split across text
/// and CDATA events arrives whole.
pub fn push_text(target: &mut String, raw: &[u8]) -> Result<(), String> {
    let raw =
        std::str::from_utf8(raw).map_err(|error| format!("XML text is not UTF-8: {error}"))?;
    let text = unescape(raw).map_err(|error| format!("XML text escape error: {error}"))?;
    target.push_str(&text);
    Ok(())
}

#[must_use]
pub fn trimmed(text: &str) -> Option<String> {
    let value = text.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

/// Escape a value for inclusion in a DAV request body.
#[must_use]
pub fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Join a collection URL and a resource file name.
#[must_use]
pub fn append_path(base: &str, path: &str) -> String {
    if base.ends_with('/') {
        format!("{base}{path}")
    } else {
        format!("{base}/{path}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_name_drops_any_namespace_prefix() {
        assert_eq!(local_name(b"D:href"), "href");
        assert_eq!(local_name(b"href"), "href");
        assert_eq!(local_name(b"C:calendar-data"), "calendar-data");
    }

    #[test]
    fn normalize_etag_keeps_the_weak_marker_and_drops_strong_quotes() {
        assert_eq!(normalize_etag("  \"abc\" ").as_deref(), Some("abc"));
        assert_eq!(normalize_etag("W/ \"abc\"").as_deref(), Some("W/\"abc\""));
        assert_eq!(normalize_etag("   "), None);
    }

    /// A slash inside a query or fragment is not a path separator, and a
    /// response href must rebase against the URI that actually served it.
    #[test]
    fn resolve_href_rebases_against_the_request_uri() {
        assert_eq!(
            resolve_href("https://dav.example.test/cal/work/", "one.ics"),
            "https://dav.example.test/cal/work/one.ics"
        );
        assert_eq!(
            resolve_href("https://dav.example.test/cal/work/", "/other/one.ics"),
            "https://dav.example.test/other/one.ics"
        );
        assert_eq!(
            resolve_href(
                "https://dav.example.test/cal/",
                "https://other.test/one.ics"
            ),
            "https://other.test/one.ics"
        );
    }

    #[test]
    fn escape_xml_covers_all_five_predefined_entities() {
        assert_eq!(
            escape_xml("a&b<c>d\"e'f"),
            "a&amp;b&lt;c&gt;d&quot;e&apos;f"
        );
    }

    #[test]
    fn append_path_does_not_double_the_separator() {
        assert_eq!(
            append_path("https://x.test/c", "a.ics"),
            "https://x.test/c/a.ics"
        );
        assert_eq!(
            append_path("https://x.test/c/", "a.ics"),
            "https://x.test/c/a.ics"
        );
    }
}
