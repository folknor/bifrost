//! XML decoding primitives shared by both DAV parsers.
//!
//! These five were byte-identical in `bifrost-caldav::parse` and
//! `bifrost-carddav::parse`. The 207 Multi-Status machine above them - the
//! `<response>` / `<propstat>` state walk, `ResponseParts`, and the multiget
//! parse - was once judged unshareable for carrying different property sets and
//! entry types (calendar-data versus address-data). That redesign was since
//! done: the crate-private `multistatus` module parameterizes the walk over a `PropSet` naming
//! which properties a dialect stages plus the constructors that build its own
//! entry type, and both crates run through it. What remains dialect-local above
//! this module is the iCalendar and vCard decoding of the property VALUES, not
//! the document structure.

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

/// Compare two DAV URLs for resource identity.
///
/// A byte comparison after trimming a trailing slash reads two spellings of the
/// SAME resource as different ones: percent-encoding (`/My%20Cal/` against
/// `/My Cal/`), host case (`DAV.Example.Test` against `dav.example.test`), and a
/// redundant default port (`https://h:443/c`) all appear in restated ids and in
/// derived parent URLs. Where the answer drives a relocation - a `calendar_id`
/// or `AddressBookId` that merely restates the resource's own collection - a
/// false "different" issues a MOVE to the collection the resource is already
/// in, which `Overwrite: F` then refuses with a 412 the consumer sees as a
/// conflict.
///
/// Path segments are decoded and compared one by one rather than as one string,
/// so an encoded separator (`%2F`) stays distinct from a real one. Query and
/// fragment are compared verbatim; anything that will not parse as an absolute
/// URL falls back to the old slash-trimmed byte comparison.
#[must_use]
pub fn same_dav_url(left: &str, right: &str) -> bool {
    match (Url::parse(left), Url::parse(right)) {
        (Ok(left), Ok(right)) => {
            left.scheme() == right.scheme()
                && left.host_str().map(str::to_ascii_lowercase)
                    == right.host_str().map(str::to_ascii_lowercase)
                && left.port_or_known_default() == right.port_or_known_default()
                && decoded_path_segments(left.path()) == decoded_path_segments(right.path())
                && left.query() == right.query()
        }
        _ => left.trim_end_matches('/') == right.trim_end_matches('/'),
    }
}

/// Percent-decode each path segment, dropping the empty trailing segment a
/// collection URL's slash produces so `/c` and `/c/` are one resource.
fn decoded_path_segments(path: &str) -> Vec<String> {
    let mut segments: Vec<String> = path.split('/').map(percent_decode).collect();
    if segments.last().is_some_and(String::is_empty) {
        segments.pop();
    }
    segments
}

fn percent_decode(segment: &str) -> String {
    let raw = segment.as_bytes();
    let mut out = Vec::with_capacity(raw.len());
    let mut index = 0;
    while index < raw.len() {
        if raw[index] == b'%'
            && index + 2 < raw.len()
            && let Some(byte) = hex_pair(raw[index + 1], raw[index + 2])
        {
            out.push(byte);
            index += 3;
        } else {
            out.push(raw[index]);
            index += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_pair(high: u8, low: u8) -> Option<u8> {
    let high = (high as char).to_digit(16)?;
    let low = (low as char).to_digit(16)?;
    u8::try_from(high * 16 + low).ok()
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

    /// Two spellings of one collection must compare equal, or a restated
    /// collection id reads as a relocation and issues a MOVE onto itself.
    #[test]
    fn same_dav_url_sees_through_encoding_host_case_and_default_ports() {
        assert!(same_dav_url(
            "https://dav.example.test/cal/My%20Cal/",
            "https://DAV.Example.Test/cal/My Cal"
        ));
        assert!(same_dav_url(
            "https://dav.example.test:443/cal/work",
            "https://dav.example.test/cal/work/"
        ));
        assert!(same_dav_url("/cal/work/", "/cal/work"));
    }

    /// An encoded separator is not a separator, and neither the host nor the
    /// scheme is allowed to slide.
    #[test]
    fn same_dav_url_keeps_genuinely_different_resources_apart() {
        assert!(!same_dav_url(
            "https://dav.example.test/cal/a%2Fb",
            "https://dav.example.test/cal/a/b"
        ));
        assert!(!same_dav_url(
            "https://dav.example.test/cal/work",
            "https://other.example.test/cal/work"
        ));
        assert!(!same_dav_url(
            "https://dav.example.test/cal/work",
            "http://dav.example.test/cal/work"
        ));
        assert!(!same_dav_url(
            "https://dav.example.test/cal/one",
            "https://dav.example.test/cal/two"
        ));
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
