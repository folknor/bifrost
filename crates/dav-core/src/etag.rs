//! ETag normalization and conditional-write preparation.
//!
//! DAV adapters delegate token classification to bifrost-types. Keep these
//! string-facing helpers as adapters for XML snapshots and HTTP callers; they
//! do not maintain a second strong/weak parser.

use reqwest::header::HeaderMap;

pub use bifrost_types::{EntityTag, StrongTag};

/// How a PUT is conditioned.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PutCondition {
    /// The resource must not already exist.
    IfNoneMatch,
    /// The resource must still carry this strong validator.
    IfMatch(StrongTag),
    /// The resource must still exist (`If-Match: *`), whatever its version.
    /// An update without a strong validator uses this so it cannot recreate
    /// a resource deleted after it was read. It is existence protection, not
    /// lost-update protection.
    IfExists,
    /// Unconditional.
    None,
}

impl PutCondition {
    #[must_use]
    pub fn header(&self) -> Option<(&'static str, String)> {
        match self {
            Self::IfNoneMatch => Some(("If-None-Match", "*".to_string())),
            Self::IfMatch(tag) => Some(("If-Match", tag.to_header())),
            Self::IfExists => Some(("If-Match", "*".to_string())),
            Self::None => None,
        }
    }

    /// The condition for updating an existing resource. A server offering only
    /// weak tags cannot protect against lost updates; keep saving the
    /// enterprise user's changes rather than refusing every edit on such a
    /// server, but never recreate a resource deleted since it was read.
    #[must_use]
    pub fn from_etag(etag: Option<&str>) -> Self {
        Self::from_tag(etag.map(EntityTag::parse).as_ref())
    }

    /// [`Self::from_etag`] for an already classified validator: a strong tag
    /// is `IfMatch`, a weak or absent one `IfExists`.
    #[must_use]
    pub fn from_tag(etag: Option<&EntityTag>) -> Self {
        match etag.cloned() {
            Some(EntityTag::Strong(tag)) => Self::IfMatch(tag),
            _ => Self::IfExists,
        }
    }
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
    EntityTag::parse(value).normalized()
}

/// The `If-Match` value for an etag, or `None` when no conforming one exists.
///
/// RFC 7232 requires STRONG comparison for `If-Match`, so a weak validator has
/// no conforming version condition. Against a server that only ever emits weak
/// ETags, updates have no lost-update protection; a consumer needing that
/// guarantee needs an application-level revision check. HTTP does still offer
/// existence protection (`If-Match: *`, [`PutCondition::IfExists`]), which
/// update paths use so a weak-tag write cannot recreate a deleted resource.
#[must_use]
pub fn prepare_if_match(etag: &str) -> Option<String> {
    match EntityTag::parse(etag) {
        EntityTag::Strong(tag) => Some(tag.to_header()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_conditions_admit_only_strong_validators() {
        assert_eq!(
            PutCondition::from_etag(Some("W/\"abc\"")).header(),
            Some(("If-Match", "*".to_string())),
            "a weak tag still guards existence"
        );
        assert_eq!(
            PutCondition::from_etag(None).header(),
            Some(("If-Match", "*".to_string())),
        );
        assert_eq!(PutCondition::None.header(), None);
        assert_eq!(
            PutCondition::from_etag(Some("\"abc\"")).header(),
            Some(("If-Match", "\"abc\"".to_string())),
        );
        assert_eq!(
            PutCondition::IfNoneMatch.header(),
            Some(("If-None-Match", "*".to_string())),
        );
    }

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
