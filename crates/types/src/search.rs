//! Search request AST.
//!
//! Each leaf names one search intent (a header, the body, attachment
//! presence, a folder, ...). Each protocol Account impl translates the AST
//! into the provider's native query and matches each leaf the way the
//! provider's own search matches it, so tokenization, stemming and case
//! handling are the provider's. A leaf a provider has no operator for is
//! refused as `Unsupported`, never approximated by a search over a
//! different field. Operators outside the AST are reached through the
//! protocol-specific raw query, either request-wide
//! (`SearchRequest::provider_query`) or as a composable leaf
//! (`SearchFilter::ProviderQuery`). The raw string means the same thing in
//! both positions: Gmail query syntax, Graph KQL (`$search`), a JMAP `text`
//! filter, or raw IMAP SEARCH criteria.

use std::time::SystemTime;

use crate::container::ContainerId;
use crate::ids::LabelId;

/// Search filter AST.
///
/// `And`, `Or`, and `Not` recurse for boolean composition. Leaf
/// variants are the canonical intersection of provider operators.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SearchFilter {
    /// Free-text in the `From` header.
    From(String),
    /// Free-text in `To` / `Cc` / `Bcc`.
    To(String),
    /// Free-text in the `Subject` header.
    Subject(String),
    /// Free-text anywhere in the body.
    Body(String),
    /// Attachment search. See [`AttachmentFilter`] for what each form
    /// means and which providers evaluate it.
    Attachment(AttachmentFilter),
    /// The provider's raw query language as a composable leaf: the same
    /// string `SearchRequest::provider_query` accepts, with the same
    /// per-protocol meaning (Gmail query syntax, Graph KQL, a JMAP `text`
    /// filter, raw IMAP SEARCH criteria), placed inside the boolean tree
    /// instead of conjoined with the whole request. An empty or
    /// whitespace-only operand is `Request(Malformed)`. Gmail and Graph
    /// splice the string into a query they build, so the operand must pass
    /// [`balanced_raw_query`] and cannot escape its group; IMAP applies the
    /// same validation it applies to `provider_query`. A failing operand is
    /// `Request(Malformed)`. JMAP sends the operand verbatim as text data.
    /// On Graph the leaf routes the whole filter onto `$search`/KQL, as a
    /// `From`/`To` leaf does.
    ProviderQuery(String),
    /// Restrict to a specific container (folder, mailbox, or label
    /// rendered as a container).
    In(ContainerId),
    /// Restrict to messages carrying a specific label.
    Labeled(LabelId),
    /// Inclusive-exclusive date range against the message's send
    /// time. Either bound may be `None` for an open-ended range.
    DateRange {
        after: Option<SystemTime>,
        before: Option<SystemTime>,
    },
    /// All branches must match.
    And(Vec<SearchFilter>),
    /// At least one branch must match.
    Or(Vec<SearchFilter>),
    /// Negate the inner branch.
    Not(Box<SearchFilter>),
}

/// What a [`SearchFilter::Attachment`] leaf asks for.
///
/// One intent per form, matched by the provider's own search where it has
/// one. Providers disagree about what counts as an attachment (Graph's
/// `hasAttachments` excludes inline attachments, JMAP's `hasAttachment` is a
/// server heuristic, IMAP is computed by bifrost from BODYSTRUCTURE), and an
/// attachment-name index need not agree with the presence flag. So `Name`
/// does not imply `Any` on every provider, and a consumer must not simplify
/// `And(Any, Name(x))` to `Name(x)` or the reverse.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AttachmentFilter {
    /// The message carries attachments by the account's attachment-presence
    /// classification: Gmail `has:attachment`, Graph `hasAttachments`
    /// (KQL `hasattachment:true`), JMAP `hasAttachment`, and on IMAP any
    /// MIME entity with `Content-Disposition: attachment`, a filename, or an
    /// embedded message type.
    Any,
    /// An attachment whose name matches the text, matched by the
    /// provider's attachment-name search: Gmail `filename:`, Graph KQL
    /// `attachment:` (word matching, so a fragment inside a longer word can
    /// miss), and on IMAP a case-insensitive substring of the decoded
    /// filename. JMAP has no attachment-name condition (RFC 8621 `text`
    /// searches other fields), so it refuses this form as `Unsupported`;
    /// the old JMAP behaviour of a full-text match among attachment-bearing
    /// messages is `And(Attachment(Any), ProviderQuery(text))`. An empty or
    /// whitespace-only name is `Request(Malformed)`.
    Name(String),
    /// An attachment of this MIME type. Only IMAP evaluates it (from
    /// BODYSTRUCTURE); Gmail, Graph and JMAP have no type operator and
    /// refuse it as `Unsupported`.
    MediaType(MediaTypePattern),
}

/// A validated MIME type pattern: `type/subtype`, or `type/*` for every
/// subtype of a top-level type. Case-insensitive; stored lowercased.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MediaTypePattern {
    media_type: String,
    subtype: Option<String>,
}

impl MediaTypePattern {
    /// Parse `type/subtype` or `type/*`. Both halves must be RFC 6838
    /// restricted names (an ASCII alphanumeric, then up to 126 of ASCII
    /// alphanumerics and `!#$&-^_.+`), with no surrounding whitespace; `*` is accepted only
    /// as the whole subtype. Anything else is `None`, so a malformed type
    /// cannot become a search that silently matches nothing. The old
    /// `type/` spelling of a whole-type match is `type/*`.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        // RFC 6838 section 4.2: an alphanumeric first character, then up to
        // 126 more restricted-name characters.
        fn restricted_name(part: &str) -> bool {
            part.len() <= 127
                && part
                    .bytes()
                    .next()
                    .is_some_and(|b| b.is_ascii_alphanumeric())
                && part.bytes().all(|b| {
                    b.is_ascii_alphanumeric()
                        || matches!(
                            b,
                            b'!' | b'#' | b'$' | b'&' | b'-' | b'^' | b'_' | b'.' | b'+'
                        )
                })
        }
        let (media_type, subtype) = value.split_once('/')?;
        if !restricted_name(media_type) {
            return None;
        }
        let subtype = if subtype == "*" {
            None
        } else if restricted_name(subtype) {
            Some(subtype.to_ascii_lowercase())
        } else {
            return None;
        };
        Some(Self {
            media_type: media_type.to_ascii_lowercase(),
            subtype,
        })
    }

    /// Lowercased top-level type.
    #[must_use]
    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    /// Lowercased subtype, or `None` for `type/*`.
    #[must_use]
    pub fn subtype(&self) -> Option<&str> {
        self.subtype.as_deref()
    }

    /// Whether a `type/subtype` pair satisfies the pattern, ignoring case.
    #[must_use]
    pub fn matches(&self, media_type: &str, subtype: &str) -> bool {
        self.media_type.eq_ignore_ascii_case(media_type)
            && self
                .subtype
                .as_deref()
                .is_none_or(|own| own.eq_ignore_ascii_case(subtype))
    }
}

impl std::fmt::Display for MediaTypePattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}",
            self.media_type,
            self.subtype.as_deref().unwrap_or("*")
        )
    }
}

/// Whether a raw query operand can be spliced into a generated Gmail or
/// KQL query without escaping its group: every double quote is closed and
/// parentheses outside quotes balance and never close below zero. The
/// check knows nothing of either grammar's operators; it only guards the
/// boundary the encoder puts around the operand.
#[must_use]
pub fn balanced_raw_query(raw: &str) -> bool {
    let mut depth = 0usize;
    let mut quoted = false;
    for ch in raw.chars() {
        match ch {
            '"' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => match depth.checked_sub(1) {
                Some(next) => depth = next,
                None => return false,
            },
            _ => {}
        }
    }
    !quoted && depth == 0
}

/// Search request shape passed to `Account::search` and
/// `Account::search_messages`.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct SearchRequest {
    /// Structured filter. `None` means "every message"; protocols
    /// that disallow that escape via `provider_query`.
    pub filter: Option<SearchFilter>,
    /// Max items per page. `None` lets the protocol pick its
    /// default page size.
    pub limit: Option<u32>,
    /// Opaque cursor returned by a prior call's `Page::next_cursor`.
    /// `None` starts a fresh search.
    pub page_cursor: Option<crate::PageCursor>,
    /// Provider-specific raw query, appended to or substituted for
    /// the structured filter at the protocol layer. Lets consumers
    /// reach Gmail-specific operators like `larger:5M` or Graph
    /// `$search` strings without forcing every other provider to
    /// model them.
    pub provider_query: Option<String>,
}

impl SearchRequest {
    /// Shorthand for "structured filter only".
    #[must_use]
    pub fn filter(f: SearchFilter) -> Self {
        Self {
            filter: Some(f),
            ..Default::default()
        }
    }

    /// Shorthand for "raw provider query only".
    #[must_use]
    pub fn provider(query: impl Into<String>) -> Self {
        Self {
            provider_query: Some(query.into()),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_type_patterns_parse_strictly() {
        let pdf = MediaTypePattern::parse("Application/PDF").expect("type/subtype");
        assert_eq!(pdf.to_string(), "application/pdf");
        assert!(pdf.matches("application", "pdf"));
        assert!(pdf.matches("APPLICATION", "Pdf"));
        assert!(!pdf.matches("application", "zip"));

        let image = MediaTypePattern::parse("image/*").expect("whole type");
        assert_eq!(image.subtype(), None);
        assert!(image.matches("image", "png"));
        assert!(!image.matches("text", "plain"));

        for bad in [
            "",
            "pdf",
            "image/",
            "/pdf",
            " image/png",
            "image/png ",
            "image/p*",
            "*/*",
            "a/b/c",
            "application/+pdf",
            "!/pdf",
        ] {
            assert!(
                MediaTypePattern::parse(bad).is_none(),
                "{bad:?} must be refused"
            );
        }
        let long = "a".repeat(128);
        for bad in [format!("{long}/pdf"), format!("image/{long}")] {
            assert!(MediaTypePattern::parse(&bad).is_none(), "too long");
        }
        assert!(MediaTypePattern::parse(&format!("image/{}", &long[1..])).is_some());
    }

    #[test]
    fn raw_query_balance_guards_the_group_boundary() {
        for good in [
            "from:a OR from:b",
            "(a OR b) c",
            "subject:\"(x\"",
            "\"a b\"",
        ] {
            assert!(balanced_raw_query(good), "{good:?}");
        }
        for bad in ["a) OR (b", "(a", "a)", "\"open", "(\")\""] {
            assert!(!balanced_raw_query(bad), "{bad:?}");
        }
    }
}
