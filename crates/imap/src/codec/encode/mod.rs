//! IMAP command encoder.
//!
//! Serializes [`Command`] values into bytes suitable for sending to the server.
//! Each command is prefixed with a unique tag (e.g. `A001`, `A002`, ...).
//!
//! Command syntax is defined in RFC 3501 Section 6 / RFC 9051 Section 6.
//! String encoding (quoted vs literal) follows RFC 3501 Section 9 / RFC 9051 Section 9.

mod commands;
mod string_helpers;

#[cfg(test)]
#[path = "tests.rs"]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;

use base64::Engine as _;
use bytes::BytesMut;
use std::collections::HashSet;

use crate::types::response::Capability;
use crate::types::{Command, MailboxAttribute, QresyncParams};

// Re-export sub-module items that are part of this module's public API.
#[cfg(test)]
pub(crate) use commands::encode_multi_append_header;
pub(crate) use commands::encode_multi_append_header_with_literal8;
pub(crate) use string_helpers::{encode_quoted_or_literal, encode_quoted_or_literal_utf8};

// Make sub-module items available within this module for dispatch.
pub(crate) use commands::encode_mailbox_str;
use commands::{
    encode_authenticate, encode_create_special_use, encode_fetch, encode_getmetadata, encode_id,
    encode_list_extended, encode_list_status, encode_login, encode_mailbox_cmd,
    encode_mailbox_name, encode_notify_set, encode_search, encode_select_or_examine,
    encode_set_acl, encode_set_quota, encode_setmetadata, encode_simple, encode_status,
    encode_store, encode_thread_or_sort_cmd, encode_two_arg, encode_two_quoted_args,
    encode_uid_expunge, encode_uid_fetch,
};
use string_helpers::encode_metadata_value;

/// Controls how the encoder produces literal markers.
///
/// RFC 3501 Section 4.3 defines synchronizing literals (`{N}\r\n`) which
/// require the client to wait for a `+` continuation response. RFC 7888
/// introduces two extensions that allow non-synchronizing literals (`{N+}\r\n`):
///
/// - **LITERAL+** (RFC 7888 Section 4): non-synchronizing literals of any size.
/// - **LITERAL-** (RFC 7888 Section 5): non-synchronizing literals up to 4096
///   bytes; larger literals MUST use synchronizing form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiteralMode {
    /// No literal extension  -  all literals use synchronizing form `{N}\r\n`
    /// (RFC 3501 Section 4.3).
    Synchronizing,
    /// LITERAL+ (RFC 7888 Section 4)  -  non-synchronizing `{N+}\r\n` for all sizes.
    LiteralPlus,
    /// LITERAL- (RFC 7888 Section 5)  -  non-synchronizing `{N+}\r\n` only when
    /// the literal is <= 4096 bytes; larger literals use synchronizing `{N}\r\n`.
    LiteralMinus,
}

/// Encoding context derived from the current connection state.
///
/// Carries the capability set, enabled extensions, and negotiated literal
/// mode so the encoder can:
/// - reject commands whose prerequisite capability is not advertised (I6),
/// - select the correct wire encoding for mailbox names and literals.
///
/// Constructed by `ImapConnection::encode_options()`.
#[derive(Debug, Clone)]
pub(crate) struct EncodeOptions {
    /// RFC 6855 / RFC 9051 `UTF8=ACCEPT` mode.
    ///
    /// When `true`, non-ASCII UTF-8 bytes are allowed in quoted strings
    /// (RFC 6855 Section 3 / RFC 9051 Section 9) and mailbox names are
    /// sent as raw UTF-8 instead of modified UTF-7.
    pub(crate) utf8_mode: bool,
    /// RFC 7888 LITERAL+ vs synchronizing literal (RFC 3501 Section 4.3).
    pub(crate) literal_mode: LiteralMode,
    /// Server-advertised capabilities (RFC 3501 Section 7.2.1).
    pub(crate) capabilities: Vec<Capability>,
    /// Extensions successfully enabled with ENABLE.
    pub(crate) enabled: Vec<String>,
}

impl EncodeOptions {
    /// Check whether the server advertises a specific capability.
    fn has_capability(&self, cap: &Capability) -> bool {
        self.capabilities.contains(cap) || self.rev2_implies(cap)
    }

    /// Check whether CONDSTORE is available (explicitly or via QRESYNC).
    ///
    /// RFC 7162 Section 3.2.3: a server that advertises QRESYNC implicitly
    /// supports CONDSTORE. The encoder checks this whenever a CONDSTORE
    /// modifier (CHANGEDSINCE, UNCHANGEDSINCE, VANISHED) is present.
    fn has_condstore(&self) -> bool {
        self.has_capability(&Capability::Condstore) || self.has_capability(&Capability::QResync)
    }

    fn imap4rev2_active(&self) -> bool {
        let has_rev2 = self.capabilities.contains(&Capability::Imap4Rev2);
        let has_rev1 = self.capabilities.contains(&Capability::Imap4Rev1);
        if has_rev2 && has_rev1 {
            self.enabled
                .iter()
                .any(|extension| extension.eq_ignore_ascii_case("IMAP4rev2"))
        } else {
            has_rev2
        }
    }

    fn rev2_implies(&self, capability: &Capability) -> bool {
        self.imap4rev2_active()
            && matches!(
                capability,
                Capability::Binary
                    | Capability::Enable
                    | Capability::Esearch
                    | Capability::Idle
                    | Capability::ListExtended
                    | Capability::LiteralMinus
                    | Capability::LiteralPlus
                    | Capability::Move
                    | Capability::Namespace
                    | Capability::ObjectId
                    | Capability::SaslIr
                    | Capability::SaveDate
                    | Capability::SearchRes
                    | Capability::SpecialUse
                    | Capability::StatusDeleted
                    | Capability::StatusSize
                    | Capability::UidPlus
                    | Capability::Unselect
            )
    }
}

/// Error produced when the encoder cannot produce a valid wire command.
///
/// Separate from [`crate::Error`] so codec-level callers can distinguish
/// encoding failures from I/O and protocol errors. The connection layer
/// converts this to [`crate::Error`] at the call site.
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum EncodeError {
    /// The command requires a capability that the server has not advertised
    /// or the client has not `ENABLE`d (RFC 3501 Section 6.1.1).
    #[error("command {cmd} requires capability {cap} which is not available")]
    MissingCapability {
        /// The command that requires the capability.
        cmd: &'static str,
        /// The wire name of the missing capability.
        cap: String,
    },
    /// A protocol-level validation failure detected during encoding
    /// (e.g., CRLF in a parameter, invalid atom, etc.).
    #[error("encode validation error: {0}")]
    Validation(String),
}

impl From<crate::Error> for EncodeError {
    /// Convert legacy `crate::Error::Protocol` validation errors into
    /// `EncodeError::Validation`. This bridge exists so existing per-command
    /// encoders that return `Result<(), crate::Error>` can be composed with
    /// the new `EncodeError` return path without rewriting every encoder at
    /// once.
    fn from(e: crate::Error) -> Self {
        Self::Validation(e.to_string())
    }
}

/// RFC 7888 Section 5: maximum non-synchronizing literal size for LITERAL-.
const LITERAL_MINUS_MAX: usize = 4096;

/// Encoded IMAP command split into segments at synchronizing literal boundaries.
///
/// RFC 3501 Section 4.3: when a command contains a synchronizing literal
/// (`{N}\r\n`), the client must send the bytes up to and including the literal
/// marker, then wait for a `+` continuation response before sending the literal
/// body. This struct represents that split so callers can implement the
/// send-wait-send cycle without rescanning the wire bytes.
///
/// - When `literal_mode` is [`LiteralMode::LiteralPlus`] (RFC 7888 Section 4) or
///   the command contains no literals, `segments` has exactly one element.
/// - When `literal_mode` is [`LiteralMode::Synchronizing`] and the command
///   contains synchronizing literals, `segments` has N+1 elements (one for each
///   literal boundary plus the trailing data). Between consecutive segments the
///   caller must wait for a server `+` continuation response.
/// - When `literal_mode` is [`LiteralMode::LiteralMinus`] (RFC 7888 Section 5),
///   literals <= 4096 bytes are non-synchronizing; only literals > 4096 bytes
///   produce segment splits.
#[derive(Debug, Clone)]
pub(crate) struct EncodedCommand {
    /// Sequential wire segments. The caller sends `segments[0]`, waits for `+`,
    /// sends `segments[1]`, waits for `+`, ..., sends `segments[N]` (no wait
    /// after the last segment). Each segment is never empty.
    segments: Vec<BytesMut>,
}

impl EncodedCommand {
    /// Return the wire segments. Between consecutive segments, the connection
    /// must wait for a `+` continuation response (RFC 3501 Section 4.3).
    pub(crate) fn segments(&self) -> &[BytesMut] {
        &self.segments
    }

    /// Concatenate all segments into a single buffer.
    ///
    /// Useful for non-synchronizing paths (LITERAL+) where the entire command
    /// can be sent in one shot, or for tests that verify total wire output.
    pub(crate) fn into_buf(mut self) -> BytesMut {
        if self.segments.len() == 1 {
            // Single-segment fast path  -  avoid reallocation.
            return self.segments.swap_remove(0);
        }
        let total: usize = self.segments.iter().map(BytesMut::len).sum();
        let mut buf = BytesMut::with_capacity(total);
        for seg in &self.segments {
            buf.extend_from_slice(seg);
        }
        buf
    }

    /// Build an `EncodedCommand` from a flat buffer by scanning for
    /// synchronizing literal markers (`{N}\r\n`) and splitting at each
    /// boundary.
    ///
    /// A synchronizing literal marker is `{digits}\r\n` where digits parse as
    /// a valid `usize` and there is no `+` before `}`. The buffer is split so
    /// that the marker ends the current segment and the literal body begins
    /// the next segment. The caller sends each segment and waits for a `+`
    /// continuation response between consecutive segments.
    ///
    /// RFC 3501 Section 4.3: "The client MUST wait for a continuation request
    /// before sending the octets of a synchronizing literal."
    fn from_flat_buffer(buf: &[u8]) -> Self {
        let mut segments = Vec::new();
        let mut seg_start = 0;
        // `scan_pos` tracks our scanning position; it may jump ahead past
        // literal bodies to avoid matching `{N}\r\n` patterns inside literal
        // data.
        let mut scan_pos = 0;

        while scan_pos < buf.len() {
            if let Some((marker_end_rel, literal_size)) =
                find_sync_literal_boundary(&buf[scan_pos..])
            {
                // Absolute offset of the byte just past `{N}\r\n`.
                let abs_marker_end = scan_pos + marker_end_rel;
                // Current segment: from seg_start through the marker (inclusive
                // of `{N}\r\n`).
                segments.push(BytesMut::from(&buf[seg_start..abs_marker_end]));
                // Next segment begins at the literal body.
                seg_start = abs_marker_end;
                // Skip past the literal body so we don't rescan it for markers.
                scan_pos = abs_marker_end + literal_size;
            } else {
                // No more synchronizing literals in the remainder.
                break;
            }
        }

        // Remaining bytes (literal body + any trailing command text) form the
        // final segment.
        if seg_start < buf.len() {
            segments.push(BytesMut::from(&buf[seg_start..]));
        }

        // If no synchronizing literals were found, the whole buffer is one
        // segment (already pushed above when seg_start == 0).
        Self { segments }
    }
}

/// Find the first synchronizing literal marker (`{digits}\r\n`) in `buf`.
///
/// Returns `(marker_end, literal_size)` where `marker_end` is the offset
/// past the `\r\n` of the marker, and `literal_size` is the parsed digit
/// count (the number of octets in the literal body).
///
/// Skips non-synchronizing markers (`{digits+}\r\n`).
///
/// Matches both regular synchronizing literals (`{N}\r\n`) and literal8
/// markers (`~{N}\r\n`). RFC 9051 Section 9 defines
/// `literal8 = "~{" number64 "}" CRLF *OCTET` with no `["+"]` modifier,
/// so literal8 is unconditionally synchronizing (RFC 3516 Section 4).
fn find_sync_literal_boundary(buf: &[u8]) -> Option<(usize, usize)> {
    let mut i = 0;
    while i < buf.len() {
        if buf[i] == b'{' {
            let start = i + 1;
            let mut j = start;
            while j < buf.len() && buf[j].is_ascii_digit() {
                j += 1;
            }
            // Must have at least one digit, then `}\r\n`
            if j > start
                && j + 2 < buf.len()
                && buf[j] == b'}'
                && buf[j + 1] == b'\r'
                && buf[j + 2] == b'\n'
            {
                // Verify this is synchronizing (no `+` before `}`)
                //  -  since we checked buf[j] == b'}' and digits end at j,
                // there is no `+`. Non-synchronizing would have `+` at buf[j]
                // which would fail the buf[j] == b'}' check.
                let Ok(size_str) = std::str::from_utf8(&buf[start..j]) else {
                    i += 1;
                    continue;
                };
                let Ok(size) = size_str.parse::<usize>() else {
                    i += 1;
                    continue;
                };
                return Some((j + 3, size));
            }
        }
        i += 1;
    }
    None
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

/// Reject raw protocol parameters that contain CR or LF bytes.
///
/// IMAP commands are line-delimited by CRLF (RFC 3501 Section 2.2).
/// Embedding CR/LF in unquoted parameters would terminate the command
/// prematurely and allow injection of arbitrary commands.
fn validate_no_crlf(s: &str, context: &str) -> Result<(), crate::Error> {
    if s.bytes().any(|b| b == b'\r' || b == b'\n') {
        return Err(crate::Error::Protocol(format!(
            "{context} must not contain CR or LF  -  IMAP commands are \
             CRLF-delimited (RFC 3501 Section 2.2)"
        )));
    }
    Ok(())
}

/// Validate SEARCH-family criteria while permitting IMAP literals that match
/// the negotiated literal mode.
///
/// RFC 3501 Section 6.4.4 defines many SEARCH keys in terms of `astring`, and
/// RFC 3501 Section 4.3 defines classic synchronizing literals as `{number}`
/// followed by CRLF and exactly `number` octets of data. RFC 7888 Section 3
/// adds the non-synchronizing `{number+}` form only when `LITERAL+` or
/// `LITERAL-` has been advertised. RFC 7888 Section 5 and RFC 9051 Section 4.3
/// limit non-synchronizing literals in `LITERAL-` / `IMAP4rev2` mode to 4096
/// octets. SORT and THREAD inherit the same search-criteria grammar
/// (RFC 5256 Section 5), so their criteria arguments need the same checks.
///
/// This validator therefore rejects raw CR/LF bytes except when they are part
/// of a syntactically valid IMAP literal marker and body, and it rejects
/// non-synchronizing literal markers that are not permitted by `literal_mode`.
fn validate_search_criteria_crlf(
    criteria: &str,
    context: &str,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    let bytes = criteria.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if bytes[i] == b'{' {
            let mut j = i + 1;
            let digits_start = j;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }

            let has_plus = j > digits_start && j < bytes.len() && bytes[j] == b'+';
            if has_plus {
                j += 1;
            }

            if j > digits_start
                && j + 2 < bytes.len()
                && bytes[j] == b'}'
                && bytes[j + 1] == b'\r'
                && bytes[j + 2] == b'\n'
            {
                let size = std::str::from_utf8(
                    &bytes[digits_start..j - usize::from(bytes[j - 1] == b'+')],
                )
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .ok_or_else(|| {
                    crate::Error::Protocol(format!(
                        "{context} contains an invalid literal octet count \
                             (RFC 3501 Section 4.3)"
                    ))
                })?;
                if has_plus {
                    match literal_mode {
                        LiteralMode::Synchronizing => {
                            return Err(crate::Error::Protocol(format!(
                                "{context} uses a non-synchronizing literal \
                                 without negotiated LITERAL+/LITERAL- support \
                                 (RFC 7888 Section 3)"
                            )));
                        }
                        LiteralMode::LiteralMinus if size > LITERAL_MINUS_MAX => {
                            return Err(crate::Error::Protocol(format!(
                                "{context} uses a non-synchronizing literal of {size} octets, \
                                 which exceeds the 4096-octet limit for LITERAL- / IMAP4rev2 \
                                 mode (RFC 7888 Section 5 / RFC 9051 Section 4.3)"
                            )));
                        }
                        LiteralMode::LiteralMinus | LiteralMode::LiteralPlus => {}
                    }
                }
                let data_start = j + 3;
                let data_end = data_start.checked_add(size).ok_or_else(|| {
                    crate::Error::Protocol(format!(
                        "{context} literal length overflows usize \
                         (RFC 3501 Section 4.3)"
                    ))
                })?;
                if data_end > bytes.len() {
                    return Err(crate::Error::Protocol(format!(
                        "{context} literal declares {size} octets but the provided \
                         criteria fragment ends early (RFC 3501 Section 4.3)"
                    )));
                }
                i = data_end;
                continue;
            }
        }

        if matches!(bytes[i], b'\r' | b'\n') {
            return Err(crate::Error::Protocol(format!(
                "{context} must not contain raw CR or LF except inside an IMAP literal \
                 (RFC 3501 Sections 2.2 and 4.3)"
            )));
        }

        i += 1;
    }

    Ok(())
}

/// Detect whether SEARCH criteria begin with a CHARSET specification.
///
/// RFC 3501 Section 6.4.4 places the optional `CHARSET <astring>` prefix at
/// the start of the SEARCH criteria syntax.
fn search_criteria_starts_with_charset(criteria: &str) -> bool {
    criteria
        .split_whitespace()
        .next()
        .is_some_and(|token| token.eq_ignore_ascii_case("CHARSET"))
}

/// Validate that LOGIN credentials remain ASCII-only.
///
/// RFC 6855 Section 5 does not extend `LOGIN` to permit UTF-8 usernames or
/// passwords. Clients needing internationalized credentials MUST use
/// `AUTHENTICATE` instead.
fn validate_login_credential_ascii(value: &str, field: &str) -> Result<(), crate::Error> {
    if !value.is_ascii() {
        return Err(crate::Error::Protocol(format!(
            "LOGIN {field} must be ASCII-only; RFC 6855 Section 5 requires \
             AUTHENTICATE for non-ASCII credentials"
        )));
    }
    Ok(())
}

/// Validate that a SASL initial-response string contains only valid base64
/// characters per RFC 4959 Section 3.
///
/// The ABNF from RFC 4959:
/// ```text
/// initial-response = base64 / "="
/// ```
/// where `base64` is defined by RFC 4648 Section 4.
///
/// An empty string is allowed  -  the caller encodes it as `"="` on the wire
/// (RFC 4959 Section 3).
fn validate_sasl_initial_response(ir: &str) -> Result<(), crate::Error> {
    // Reject CRLF first  -  this is a command injection vector (RFC 3501 Section 2.2).
    validate_no_crlf(ir, "AUTHENTICATE initial response")?;

    // RFC 4959 Section 3: the wire form is either the special `=` marker
    // for an empty initial response, or a syntactically valid RFC 4648
    // base64 string.
    if ir.is_empty() || ir == "=" {
        return Ok(());
    }

    base64::engine::general_purpose::STANDARD
        .decode(ir.as_bytes())
        .map(|_| ())
        .map_err(|_| {
            crate::Error::Protocol(
                "AUTHENTICATE initial response must be RFC 4648 base64 or the special \
                 \"=\" empty marker (RFC 4959 Section 3)"
                    .into(),
            )
        })
}

// Sequence-set validation has moved to `SequenceSet::new()` in
// `crate::types::validated` (RFC 3501 Section 9). The `SequenceSet` newtype
// guarantees validity at construction time, so callers no longer need
// runtime re-validation.

/// Maximum value for `mod-sequence-value` and `mod-sequence-valzer`
/// (RFC 7162 Section 7): a positive unsigned 63-bit integer.
const MOD_SEQ_MAX: u64 = i64::MAX as u64;

/// Validates a `mod-sequence-value` per RFC 7162 Section 7:
/// `mod-sequence-value = 1*DIGIT` where `1 <= n <= 9,223,372,036,854,775,807`.
///
/// Used for CHANGEDSINCE (RFC 7162 Section 3.1.4.1) and QRESYNC `mod_seq`
/// (RFC 7162 Section 3.2.5.2).
fn validate_mod_sequence_value(val: u64, context: &str) -> Result<(), crate::Error> {
    if val == 0 {
        return Err(crate::Error::Protocol(format!(
            "{context} mod-sequence-value must be >= 1 per RFC 7162 Section 7, got 0"
        )));
    }
    if val > MOD_SEQ_MAX {
        return Err(crate::Error::Protocol(format!(
            "{context} mod-sequence-value must be <= {MOD_SEQ_MAX} per RFC 7162 Section 7, got {val}"
        )));
    }
    Ok(())
}

/// Validates a `mod-sequence-valzer` per RFC 7162 Section 7:
/// `mod-sequence-valzer = "0" / mod-sequence-value` where `0 <= n <= 9,223,372,036,854,775,807`.
///
/// Used for UNCHANGEDSINCE (RFC 7162 Section 3.1.3).
fn validate_mod_sequence_valzer(val: u64, context: &str) -> Result<(), crate::Error> {
    if val > MOD_SEQ_MAX {
        return Err(crate::Error::Protocol(format!(
            "{context} mod-sequence-valzer must be <= {MOD_SEQ_MAX} per RFC 7162 Section 7, got {val}"
        )));
    }
    Ok(())
}

/// Encode the CHANGEDSINCE fetch modifier (RFC 7162 Section 3.1.4.1),
/// optionally followed by VANISHED (RFC 7162 Section 3.2.6).
///
/// Emits ` (CHANGEDSINCE <modseq>)` or ` (CHANGEDSINCE <modseq> VANISHED)`.
/// Validates that `modseq` satisfies the `mod-sequence-value` production
/// (>= 1, <= `i64::MAX`).
fn encode_changedsince_modifier(
    buf: &mut BytesMut,
    modseq: u64,
    vanished: bool,
) -> Result<(), crate::Error> {
    validate_mod_sequence_value(modseq, "CHANGEDSINCE")?;
    buf.extend_from_slice(b" (CHANGEDSINCE ");
    buf.extend_from_slice(modseq.to_string().as_bytes());
    // RFC 7162 Section 3.2.6: VANISHED modifier appended in the same
    // parenthesized fetch modifier list as CHANGEDSINCE.
    if vanished {
        buf.extend_from_slice(b" VANISHED");
    }
    buf.extend_from_slice(b")");
    Ok(())
}

/// Encode ENABLE command (RFC 5161 Section 3).
///
/// RFC 5161 Section 3.1 ABNF:
/// `enable = "ENABLE" 1*(SP capability)`
/// where each capability is an `atom` per RFC 3501 Section 9.
/// The capability list must be non-empty.
fn encode_enable(
    buf: &mut BytesMut,
    tag: &str,
    capabilities: &[String],
) -> Result<(), EncodeError> {
    if capabilities.is_empty() {
        return Err(EncodeError::Validation(
            "ENABLE requires at least one capability (RFC 5161 Section 3.1)".into(),
        ));
    }
    for cap in capabilities {
        validate_atom(cap, "ENABLE capability")?;
    }
    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" ENABLE");
    for cap in capabilities {
        buf.extend_from_slice(b" ");
        buf.extend_from_slice(cap.as_bytes());
    }
    buf.extend_from_slice(b"\r\n");
    Ok(())
}

/// Validate that a string is a valid `atom` per RFC 3501 Section 9.
///
/// Delegates to the shared validation logic in
/// [`crate::types::validated::validate_atom_bytes`].
///
/// `context` is a human-readable label for error messages (e.g., "flag keyword",
/// "SETQUOTA resource name").
fn validate_atom(s: &str, context: &str) -> Result<(), crate::Error> {
    crate::types::validated::validate_atom_bytes(s.as_bytes(), context)?;
    Ok(())
}

/// Validate a `charset` argument for SORT/THREAD.
///
/// RFC 5256 Section 5 defines `charset = atom / quoted`, so the encoder must
/// accept either an IMAP atom or an RFC 3501 Section 9 quoted-string.
fn validate_sort_thread_charset(charset: &str) -> Result<(), crate::Error> {
    if charset.starts_with('"') || charset.ends_with('"') {
        return validate_imap_quoted_string(charset, "THREAD/SORT charset");
    }
    validate_atom(charset, "THREAD/SORT charset")
}

/// Validate an RFC 3501 quoted-string token.
///
/// RFC 3501 Section 9:
/// - `quoted = DQUOTE *QUOTED-CHAR DQUOTE`
/// - `QUOTED-CHAR = <any TEXT-CHAR except quoted-specials> / "\\" quoted-specials`
/// - `quoted-specials = DQUOTE / "\\"`
fn validate_imap_quoted_string(s: &str, context: &str) -> Result<(), crate::Error> {
    let bytes = s.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'"' || bytes[bytes.len() - 1] != b'"' {
        return Err(crate::Error::Protocol(format!(
            "{context} must be either an atom or a quoted-string \
             (RFC 5256 Section 5 / RFC 3501 Section 9): {s:?}"
        )));
    }

    let inner = &bytes[1..bytes.len() - 1];
    if inner.is_empty() {
        return Err(crate::Error::Protocol(format!(
            "{context} must not be empty (RFC 5256 Section 5: charset values \
             name an IANA-registered charset)"
        )));
    }

    let mut idx = 0usize;
    while idx < inner.len() {
        match inner[idx] {
            b'\\' => {
                let Some(&escaped) = inner.get(idx + 1) else {
                    return Err(crate::Error::Protocol(format!(
                        "{context} quoted-string must not end with a bare backslash \
                         (RFC 3501 Section 9)"
                    )));
                };
                if escaped != b'"' && escaped != b'\\' {
                    return Err(crate::Error::Protocol(format!(
                        "{context} quoted-string may only escape DQUOTE or backslash \
                         (RFC 3501 Section 9): {s:?}"
                    )));
                }
                idx += 2;
            }
            b'"' => {
                return Err(crate::Error::Protocol(format!(
                    "{context} quoted-string contains an unescaped DQUOTE \
                     (RFC 3501 Section 9): {s:?}"
                )));
            }
            b'\r' | b'\n' | 0x00 => {
                return Err(crate::Error::Protocol(format!(
                    "{context} quoted-string contains CR, LF, or NUL \
                     (RFC 3501 Section 9): {s:?}"
                )));
            }
            byte if !byte.is_ascii() => {
                return Err(crate::Error::Protocol(format!(
                    "{context} quoted-string must be ASCII-only \
                     (RFC 3501 Section 9 CHAR): {s:?}"
                )));
            }
            _ => {
                idx += 1;
            }
        }
    }

    Ok(())
}

/// Validate that a custom flag keyword contains only ATOM-CHARs.
///
/// RFC 3501 Section 9: `flag-keyword = atom`.
/// Delegates to [`validate_atom`].
pub(crate) fn validate_flag_keyword(s: &str) -> Result<(), crate::Error> {
    validate_atom(s, "flag keyword")
}

/// Validate a METADATA entry name before sending GETMETADATA/SETMETADATA.
///
/// RFC 5464 Section 3.2 imposes additional restrictions beyond generic
/// IMAP strings: entry names are slash-separated paths and MUST NOT contain
/// `*`, `%`, non-ASCII characters, octets in the range `0x00..=0x19`,
/// consecutive `/`, or a trailing `/`. The first path component also defines
/// the annotation scope and is currently limited to `/private` or `/shared`.
fn validate_metadata_entry_name(entry: &str, context: &str) -> Result<(), crate::Error> {
    if entry.contains("//") {
        return Err(crate::Error::Protocol(format!(
            "{context} must not contain consecutive '/' characters \
             (RFC 5464 Section 3.2): {entry:?}"
        )));
    }

    if entry.ends_with('/') {
        return Err(crate::Error::Protocol(format!(
            "{context} must not end with '/' (RFC 5464 Section 3.2): {entry:?}"
        )));
    }

    if !entry.is_ascii() {
        return Err(crate::Error::Protocol(format!(
            "{context} must not contain non-ASCII characters \
             (RFC 5464 Section 3.2): {entry:?}"
        )));
    }

    if entry
        .bytes()
        .any(|byte| matches!(byte, 0x00..=0x19) || byte == b'*' || byte == b'%')
    {
        return Err(crate::Error::Protocol(format!(
            "{context} must not contain '*', '%', or control octets \
             0x00..=0x19 (RFC 5464 Section 3.2): {entry:?}"
        )));
    }

    // RFC 5464 Section 3.2: entry names are slash-separated hierarchical
    // paths whose first component defines the annotation scope. The only
    // standardized scope prefixes are `/private` and `/shared`.
    let Some(stripped) = entry.strip_prefix('/') else {
        return Err(crate::Error::Protocol(format!(
            "{context} must start with '/' and use the /private or /shared \
             scope prefixes (RFC 5464 Section 3.2): {entry:?}"
        )));
    };
    let scope = stripped.split('/').next().unwrap_or_default();
    if !matches!(scope.to_ascii_lowercase().as_str(), "private" | "shared") {
        return Err(crate::Error::Protocol(format!(
            "{context} must use the /private or /shared scope prefixes \
             (RFC 5464 Section 3.2): {entry:?}"
        )));
    }

    Ok(())
}

/// Filter and validate flags for client commands (STORE, APPEND).
///
/// RFC 3501 Section 9: the `flag` production excludes `\Recent`
/// (read-only, server-set) and `\*` (only valid in `flag-perm` for
/// PERMANENTFLAGS responses).  Custom flag keywords are validated
/// against ATOM-CHAR (RFC 3501 Section 9: `flag-keyword = atom`).
///
/// Returns the filtered flags with `\Recent` and `\*` removed.
fn validate_and_filter_flags<'a>(
    flags: &'a [crate::types::Flag],
    context: &str,
) -> Result<Vec<&'a crate::types::Flag>, crate::Error> {
    let valid_flags: Vec<_> = flags
        .iter()
        .filter(|f| {
            if matches!(f, crate::types::Flag::Recent | crate::types::Flag::Wildcard) {
                tracing::debug!(
                    flag = %f,
                    "Skipping flag not permitted in {context} per RFC 3501 Section 9 (flag production)"
                );
                false
            } else {
                true
            }
        })
        .collect();
    for flag in &valid_flags {
        if let crate::types::Flag::Custom(s) = flag {
            validate_flag_keyword(s)?;
        }
    }
    Ok(valid_flags)
}

/// Validate an APPEND date-time string against RFC 3501 Section 9.
///
/// The expected format (without the surrounding DQUOTEs, which the caller adds):
///
/// ```text
/// date-day-fixed "-" date-month "-" date-year SP time SP zone
/// date-day-fixed = (SP DIGIT) / 2DIGIT          ; 1-31, space-padded or zero-padded
/// date-month     = "Jan" / "Feb" / "Mar" / "Apr" / "May" / "Jun" /
///                  "Jul" / "Aug" / "Sep" / "Oct" / "Nov" / "Dec"
/// date-year      = 4DIGIT
/// time           = 2DIGIT ":" 2DIGIT ":" 2DIGIT  ; HH:MM:SS
/// zone           = ("+" / "-") 4DIGIT
/// ```
#[allow(clippy::too_many_lines)]
pub(crate) fn validate_append_datetime(date: &str) -> Result<(), crate::Error> {
    const MONTHS: [&[u8]; 12] = [
        b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov",
        b"Dec",
    ];

    let b = date.as_bytes();

    // Expected length: " 7-Jul-1996 02:44:25 -0700" = 26 bytes.
    // date-day-fixed (2) + "-" (1) + month (3) + "-" (1) + year (4)
    //   + SP (1) + time (8) + SP (1) + zone (5) = 26
    if b.len() != 26 {
        return Err(crate::Error::InvalidAppendDate(format!(
            "expected 26 characters, got {}  -  \
             date-time must be date-day-fixed \"-\" date-month \"-\" date-year \
             SP time SP zone (RFC 3501 Section 9)",
            b.len()
        )));
    }

    // date-day-fixed = (SP DIGIT) / 2DIGIT (RFC 3501 Section 9).
    //
    // The `(SP DIGIT)` form covers space-padded single-digit days (` 1`-` 9`).
    // The `2DIGIT` form covers any two-digit day; semantically valid days are
    // `01`-`31`. Zero-padded single-digit days (`01`-`09`) are valid per the
    // `2DIGIT` alternative  -  RFC 5234 Section 3.4 defines `2DIGIT = DIGIT DIGIT`.
    let day_hi = b[0];
    let day_lo = b[1];
    let valid_day = match day_hi {
        b' ' => day_lo.is_ascii_digit() && day_lo != b'0',
        b'0' => (b'1'..=b'9').contains(&day_lo),
        b'1'..=b'2' => day_lo.is_ascii_digit(),
        b'3' => day_lo == b'0' || day_lo == b'1',
        _ => false,
    };
    if !valid_day {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid day {:?}  -  date-day-fixed must be (SP DIGIT) / 2DIGIT \
             with values 1-31 (RFC 3501 Section 9)",
            std::str::from_utf8(&b[0..2]).unwrap_or("??")
        )));
    }

    // First separator: "-" (RFC 3501 Section 9).
    if b[2] != b'-' {
        return Err(crate::Error::InvalidAppendDate(format!(
            "expected '-' at position 2, got {:?} (RFC 3501 Section 9)",
            b[2] as char
        )));
    }

    // date-month (RFC 3501 Section 9).
    // RFC 3501 Section 9 paragraph (1): "all alphabetic characters are
    // case-insensitive ... Implementations MUST accept these strings in a
    // case-insensitive fashion."  RFC 5234 Section 2.3 confirms ABNF
    // literal strings are case-insensitive.
    let month = &b[3..6];
    if !MONTHS.iter().any(|m| m.eq_ignore_ascii_case(month)) {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid month {:?}  -  date-month must be Jan/Feb/.../Dec (RFC 3501 Section 9)",
            std::str::from_utf8(month).unwrap_or("???")
        )));
    }

    let day: u8 = match day_hi {
        b' ' => day_lo - b'0',
        _ => (day_hi - b'0') * 10 + (day_lo - b'0'),
    };

    // Second separator: "-" (RFC 3501 Section 9).
    if b[6] != b'-' {
        return Err(crate::Error::InvalidAppendDate(format!(
            "expected '-' at position 6, got {:?} (RFC 3501 Section 9)",
            b[6] as char
        )));
    }

    // date-year = 4DIGIT (RFC 3501 Section 9).
    if !b[7..11].iter().all(u8::is_ascii_digit) {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid year {:?}  -  date-year must be 4DIGIT (RFC 3501 Section 9)",
            std::str::from_utf8(&b[7..11]).unwrap_or("????")
        )));
    }

    // Parse year for the day-vs-month cross-check below (needed for
    // leap-year validation of February 29).
    let year: u16 = u16::from(b[7] - b'0') * 1000
        + u16::from(b[8] - b'0') * 100
        + u16::from(b[9] - b'0') * 10
        + u16::from(b[10] - b'0');

    // Day-vs-month cross-check (RFC 3501 Section 9).
    //
    // The ABNF `date-day-fixed` allows 1-31 structurally, but not every day
    // is valid for every month.  Postel's law says "be conservative in what
    // you send", so we reject invalid dates before transmitting them.
    //   - 31-day months: Jan, Mar, May, Jul, Aug, Oct, Dec
    //   - 30-day months: Apr, Jun, Sep, Nov
    //   - Feb: 29 in leap years, 28 otherwise
    let max_day: u8 = match month[0].to_ascii_lowercase() {
        // Feb  -  28 or 29 depending on leap year.
        // Leap year rules: divisible by 400 -> leap; else by 100 -> not;
        // else by 4 -> leap; else -> not.
        b'f' => {
            let y = u32::from(year);
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        // Apr -> 30
        b'a' if month[1].eq_ignore_ascii_case(&b'p') => 30,
        // Jun -> 30
        b'j' if month[1].eq_ignore_ascii_case(&b'u') && month[2].eq_ignore_ascii_case(&b'n') => 30,
        // Sep, Nov -> 30
        b's' | b'n' => 30,
        // Jan, Mar, May, Jul, Aug, Oct, Dec -> 31
        _ => 31,
    };
    if day > max_day {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid day {} for month {:?} year {}  -  maximum is {} (RFC 3501 Section 9)",
            day,
            std::str::from_utf8(month).unwrap_or("???"),
            year,
            max_day
        )));
    }

    // SP between date and time (RFC 3501 Section 9).
    if b[11] != b' ' {
        return Err(crate::Error::InvalidAppendDate(format!(
            "expected SP at position 11, got {:?} (RFC 3501 Section 9)",
            b[11] as char
        )));
    }

    // time = 2DIGIT ":" 2DIGIT ":" 2DIGIT (RFC 3501 Section 9).
    let time = &b[12..20];
    let valid_time = time[0].is_ascii_digit()
        && time[1].is_ascii_digit()
        && time[2] == b':'
        && time[3].is_ascii_digit()
        && time[4].is_ascii_digit()
        && time[5] == b':'
        && time[6].is_ascii_digit()
        && time[7].is_ascii_digit();
    if !valid_time {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid time {:?}  -  time must be 2DIGIT \":\" 2DIGIT \":\" 2DIGIT \
             (RFC 3501 Section 9)",
            std::str::from_utf8(time).unwrap_or("????????")
        )));
    }

    // Semantic range checks for time components (RFC 3501 Section 9).
    // The ABNF `time = 2DIGIT ":" 2DIGIT ":" 2DIGIT` is purely structural,
    // but since we already enforce semantic day ranges (1-31) and month names,
    // we apply the same rigour here.  Valid ranges follow RFC 5322 Section 3.3:
    //   hour   = 00-23
    //   minute = 00-59
    //   second = 00-60  (60 allows for leap seconds)
    let hour = (time[0] - b'0') * 10 + (time[1] - b'0');
    let minute = (time[3] - b'0') * 10 + (time[4] - b'0');
    let second = (time[6] - b'0') * 10 + (time[7] - b'0');

    if hour > 23 {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid hour {hour:02}  -  must be 00-23 (RFC 3501 Section 9, \
             RFC 5322 Section 3.3)"
        )));
    }
    if minute > 59 {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid minute {minute:02}  -  must be 00-59 (RFC 3501 Section 9, \
             RFC 5322 Section 3.3)"
        )));
    }
    if second > 60 {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid second {second:02}  -  must be 00-60 (RFC 3501 Section 9, \
             RFC 5322 Section 3.3; 60 permits leap seconds)"
        )));
    }

    // SP between time and zone (RFC 3501 Section 9).
    if b[20] != b' ' {
        return Err(crate::Error::InvalidAppendDate(format!(
            "expected SP at position 20, got {:?} (RFC 3501 Section 9)",
            b[20] as char
        )));
    }

    // zone = ("+" / "-") 4DIGIT (RFC 3501 Section 9).
    let zone = &b[21..26];
    let valid_zone =
        (zone[0] == b'+' || zone[0] == b'-') && zone[1..].iter().all(u8::is_ascii_digit);
    if !valid_zone {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid zone {:?}  -  zone must be (\"+\" / \"-\") 4DIGIT (RFC 3501 Section 9)",
            std::str::from_utf8(zone).unwrap_or("?????")
        )));
    }

    // Semantic range checks for zone components (RFC 3501 Section 9).
    // The zone is ±HHMM.  RFC 5322 Section 4.3 (obs-zone) does not
    // constrain the digits, but real UTC offsets range from -1200 to +1400,
    // and minutes are always 00-59.  We enforce those limits for consistency
    // with the semantic day/time checks above.
    let zone_hh = (zone[1] - b'0') * 10 + (zone[2] - b'0');
    let zone_mm = (zone[3] - b'0') * 10 + (zone[4] - b'0');

    if zone_hh > 14 {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid zone hour {zone_hh:02}  -  must be 00-14 \
             (maximum real UTC offset is ±14:00, RFC 3501 Section 9)"
        )));
    }
    if zone_mm > 59 {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid zone minute {zone_mm:02}  -  must be 00-59 \
             (RFC 3501 Section 9)"
        )));
    }
    // RFC 3501 Section 9: `zone = ("+" / "-") 4DIGIT`  -  the grammar
    // imposes no semantic constraint beyond valid digits.  We cap the
    // hour at 14 and minute at 59 above for basic sanity, but do NOT
    // reject non-zero minutes when the hour is 14.  Real-world offsets
    // with sub-hour components exist (e.g., +12:45 Chatham Islands
    // standard, +13:45 Chatham Islands DST), and future timezone
    // changes could introduce new offsets at any hour.

    Ok(())
}

// ---------------------------------------------------------------------------
// Main dispatch
// ---------------------------------------------------------------------------

/// Encodes an IMAP command into an [`EncodedCommand`] split at synchronizing
/// literal boundaries (RFC 3501 Section 4.3).
///
/// The `literal_mode` parameter controls literal marker style:
/// - [`LiteralMode::LiteralPlus`] (RFC 7888 Section 4): non-synchronizing for
///   all sizes  -  the result is a single segment.
/// - [`LiteralMode::LiteralMinus`] (RFC 7888 Section 5): non-synchronizing for
///   literals <= 4096 bytes; larger literals use synchronizing form and produce
///   segment splits.
/// - [`LiteralMode::Synchronizing`] (RFC 3501 Section 4.3): all literals are
///   synchronizing  -  segments split at each literal boundary.
///
/// When `opts.utf8_mode` is `true` (UTF8=ACCEPT is active per RFC 6855
/// Section 3), non-ASCII UTF-8 bytes are permitted in quoted strings.
/// RFC 6855 Section 3: "The server MUST accept UTF-8 in quoted strings."
/// RFC 9051 Section 9 extends CHAR to include UTF8-2/UTF8-3/UTF8-4.
///
/// Capability prerequisite checks (I6) are performed before any bytes are
/// produced: commands that require capabilities not present in
/// `opts.capabilities` / `opts.enabled` return `EncodeError::MissingCapability`.
///
/// APPEND is handled separately by `ImapConnection::append()`.
/// Tag-command format per RFC 3501 Section 2.2.1 / RFC 9051 Section 2.2.1.
#[allow(clippy::too_many_lines)]
pub(crate) fn encode_command(
    tag: &str,
    command: &Command,
    opts: &EncodeOptions,
) -> Result<EncodedCommand, EncodeError> {
    let mut buf = BytesMut::new();
    encode_command_to_buf(&mut buf, tag, command, opts)?;
    Ok(EncodedCommand::from_flat_buffer(&buf))
}

/// Encodes an IMAP command into the provided buffer as a flat byte sequence.
///
/// This is the internal encoding engine. Callers that need synchronizing-literal
/// awareness should use [`encode_command`] which wraps this and returns an
/// [`EncodedCommand`] with properly split segments.
///
/// Capability prerequisite checks (I6) run before bytes are produced.
///
/// Tag-command format per RFC 3501 Section 2.2.1 / RFC 9051 Section 2.2.1.
#[allow(clippy::too_many_lines)]
fn encode_command_to_buf(
    buf: &mut BytesMut,
    tag: &str,
    command: &Command,
    opts: &EncodeOptions,
) -> Result<(), EncodeError> {
    let literal_mode = opts.literal_mode;
    let utf8 = opts.utf8_mode;
    match command {
        Command::Login { user, pass } => {
            encode_login(buf, tag, user, pass, utf8, literal_mode)?;
        }
        Command::Authenticate {
            mechanism,
            initial_response,
        } => {
            // RFC 3501 Section 6.2.2: auth-type = atom
            validate_atom(mechanism, "AUTHENTICATE mechanism")?;
            encode_authenticate(buf, tag, mechanism, initial_response.as_deref())?;
        }
        // RFC 3501 Section 6.2.1 / RFC 9051 Section 6.2.1
        Command::StartTls => {
            if !opts.has_capability(&Capability::StartTls) {
                return Err(EncodeError::MissingCapability {
                    cmd: "STARTTLS",
                    cap: "STARTTLS".into(),
                });
            }
            encode_simple(buf, tag, "STARTTLS");
        }
        // RFC 3501 Section 6.1.3 / RFC 9051 Section 6.1.3
        Command::Logout => {
            encode_simple(buf, tag, "LOGOUT");
        }
        // RFC 5161 Section 3
        Command::Enable { capabilities } => {
            // RFC 5161 Section 3.1: ENABLE requires ENABLE capability.
            if !opts.has_capability(&Capability::Enable) {
                return Err(EncodeError::MissingCapability {
                    cmd: "ENABLE",
                    cap: "ENABLE".into(),
                });
            }
            encode_enable(buf, tag, capabilities)?;
        }
        Command::List { reference, pattern } => {
            let wire_ref = encode_mailbox_str(reference, utf8);
            let wire_pat = encode_mailbox_str(pattern, utf8);
            encode_two_quoted_args(buf, tag, "LIST", &wire_ref, &wire_pat, utf8, literal_mode);
        }
        Command::ListExtended {
            selection_options,
            reference,
            patterns,
            return_options,
        } => {
            encode_list_extended(
                buf,
                tag,
                selection_options,
                reference,
                patterns,
                return_options,
                utf8,
                literal_mode,
            )?;
        }
        Command::ListStatus {
            reference,
            pattern,
            status_items,
        } => {
            encode_list_status(
                buf,
                tag,
                reference,
                pattern,
                status_items,
                utf8,
                literal_mode,
            )?;
        }
        Command::Select {
            mailbox,
            condstore,
            qresync,
        } => {
            // RFC 7162 Section 3.1.1: CONDSTORE requires CONDSTORE capability.
            if *condstore && !opts.has_condstore() {
                return Err(EncodeError::MissingCapability {
                    cmd: "SELECT (CONDSTORE)",
                    cap: "CONDSTORE".into(),
                });
            }
            // RFC 7162 Section 3.2.5.2: QRESYNC requires QRESYNC capability.
            if qresync.is_some() && !opts.has_capability(&Capability::QResync) {
                return Err(EncodeError::MissingCapability {
                    cmd: "SELECT (QRESYNC)",
                    cap: "QRESYNC".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_select_or_examine(
                buf,
                tag,
                "SELECT",
                &wire,
                *condstore,
                qresync.as_ref(),
                utf8,
                literal_mode,
            )?;
        }
        Command::Examine {
            mailbox,
            condstore,
            qresync,
        } => {
            // RFC 7162 Section 3.1.1: CONDSTORE requires CONDSTORE capability.
            if *condstore && !opts.has_condstore() {
                return Err(EncodeError::MissingCapability {
                    cmd: "EXAMINE (CONDSTORE)",
                    cap: "CONDSTORE".into(),
                });
            }
            // RFC 7162 Section 3.2.5.2: QRESYNC requires QRESYNC capability.
            if qresync.is_some() && !opts.has_capability(&Capability::QResync) {
                return Err(EncodeError::MissingCapability {
                    cmd: "EXAMINE (QRESYNC)",
                    cap: "QRESYNC".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_select_or_examine(
                buf,
                tag,
                "EXAMINE",
                &wire,
                *condstore,
                qresync.as_ref(),
                utf8,
                literal_mode,
            )?;
        }
        Command::Create { mailbox } => {
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_mailbox_cmd(buf, tag, "CREATE", &wire, utf8, literal_mode);
        }
        Command::CreateSpecialUse {
            mailbox,
            special_use,
        } => {
            // RFC 6154 Section 3: CREATE-SPECIAL-USE capability required.
            if !opts.has_capability(&Capability::CreateSpecialUse) {
                return Err(EncodeError::MissingCapability {
                    cmd: "CREATE (USE)",
                    cap: "CREATE-SPECIAL-USE".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_create_special_use(buf, tag, &wire, special_use, utf8, literal_mode);
        }
        Command::Delete { mailbox } => {
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_mailbox_cmd(buf, tag, "DELETE", &wire, utf8, literal_mode);
        }
        Command::Rename { mailbox, new_name } => {
            let wire_old = encode_mailbox_name(mailbox, utf8);
            let wire_new = encode_mailbox_name(new_name, utf8);
            encode_two_quoted_args(buf, tag, "RENAME", &wire_old, &wire_new, utf8, literal_mode);
        }
        Command::Subscribe { mailbox } => {
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_mailbox_cmd(buf, tag, "SUBSCRIBE", &wire, utf8, literal_mode);
        }
        Command::Unsubscribe { mailbox } => {
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_mailbox_cmd(buf, tag, "UNSUBSCRIBE", &wire, utf8, literal_mode);
        }
        Command::Lsub { reference, pattern } => {
            let wire_ref = encode_mailbox_str(reference, utf8);
            let wire_pat = encode_mailbox_str(pattern, utf8);
            encode_two_quoted_args(buf, tag, "LSUB", &wire_ref, &wire_pat, utf8, literal_mode);
        }
        // RFC 3501 Section 6.4.2 / RFC 9051 Section 6.4.1
        Command::Close => {
            encode_simple(buf, tag, "CLOSE");
        }
        // RFC 8437 Section 2
        Command::Unauthenticate => {
            encode_simple(buf, tag, "UNAUTHENTICATE");
        }
        // RFC 3691 Section 3
        Command::Unselect => {
            // RFC 3691 Section 2: UNSELECT requires UNSELECT capability.
            if !opts.has_capability(&Capability::Unselect) {
                return Err(EncodeError::MissingCapability {
                    cmd: "UNSELECT",
                    cap: "UNSELECT".into(),
                });
            }
            encode_simple(buf, tag, "UNSELECT");
        }
        Command::Status { mailbox, items } => {
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_status(buf, tag, &wire, items, utf8, literal_mode)?;
        }
        Command::Search { criteria } => {
            encode_search(buf, tag, "SEARCH", criteria, None, utf8, literal_mode)?;
        }
        Command::SearchReturn {
            criteria,
            return_opts,
        } => {
            encode_search(
                buf,
                tag,
                "SEARCH",
                criteria,
                Some(return_opts),
                utf8,
                literal_mode,
            )?;
        }
        Command::SearchSave { criteria } => {
            encode_search(
                buf,
                tag,
                "SEARCH RETURN (SAVE)",
                criteria,
                None,
                utf8,
                literal_mode,
            )?;
        }
        Command::Fetch {
            sequence_set,
            items,
            changed_since,
        } => {
            // RFC 7162 Section 3.1.4.1: CHANGEDSINCE requires CONDSTORE.
            if changed_since.is_some() && !opts.has_condstore() {
                return Err(EncodeError::MissingCapability {
                    cmd: "FETCH (CHANGEDSINCE)",
                    cap: "CONDSTORE".into(),
                });
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_fetch(buf, tag, sequence_set.as_str(), items, *changed_since)?;
        }
        Command::Store {
            sequence_set,
            operation,
            flags,
            unchanged_since,
        } => {
            // RFC 7162 Section 3.1.3: UNCHANGEDSINCE requires CONDSTORE.
            if unchanged_since.is_some() && !opts.has_condstore() {
                return Err(EncodeError::MissingCapability {
                    cmd: "STORE (UNCHANGEDSINCE)",
                    cap: "CONDSTORE".into(),
                });
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_store(
                buf,
                tag,
                false,
                sequence_set.as_str(),
                *operation,
                flags,
                *unchanged_since,
            )?;
        }
        Command::Copy {
            sequence_set,
            mailbox,
        } => {
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_two_arg(
                buf,
                tag,
                false,
                "COPY",
                sequence_set.as_str(),
                &wire,
                utf8,
                literal_mode,
            )?;
        }
        Command::Move {
            sequence_set,
            mailbox,
        } => {
            // RFC 6851 Section 3: MOVE requires MOVE capability.
            if !opts.has_capability(&Capability::Move) {
                return Err(EncodeError::MissingCapability {
                    cmd: "MOVE",
                    cap: "MOVE".into(),
                });
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_two_arg(
                buf,
                tag,
                false,
                "MOVE",
                sequence_set.as_str(),
                &wire,
                utf8,
                literal_mode,
            )?;
        }
        Command::UidSearch { criteria } => {
            encode_search(buf, tag, "UID SEARCH", criteria, None, utf8, literal_mode)?;
        }
        Command::UidSearchReturn {
            criteria,
            return_opts,
        } => {
            encode_search(
                buf,
                tag,
                "UID SEARCH",
                criteria,
                Some(return_opts),
                utf8,
                literal_mode,
            )?;
        }
        Command::UidSearchSave { criteria } => {
            encode_search(
                buf,
                tag,
                "UID SEARCH RETURN (SAVE)",
                criteria,
                None,
                utf8,
                literal_mode,
            )?;
        }
        Command::UidFetch {
            sequence_set,
            items,
            changed_since,
            vanished,
        } => {
            // RFC 7162 Section 3.1.4.1: CHANGEDSINCE requires CONDSTORE.
            if changed_since.is_some() && !opts.has_condstore() {
                return Err(EncodeError::MissingCapability {
                    cmd: "UID FETCH (CHANGEDSINCE)",
                    cap: "CONDSTORE".into(),
                });
            }
            // RFC 7162 Section 3.2.6: VANISHED requires QRESYNC.
            if *vanished && !opts.has_capability(&Capability::QResync) {
                return Err(EncodeError::MissingCapability {
                    cmd: "UID FETCH (VANISHED)",
                    cap: "QRESYNC".into(),
                });
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_uid_fetch(
                buf,
                tag,
                sequence_set.as_str(),
                items,
                *changed_since,
                *vanished,
            )?;
        }
        Command::UidStore {
            sequence_set,
            operation,
            flags,
            unchanged_since,
        } => {
            // RFC 7162 Section 3.1.3: UNCHANGEDSINCE requires CONDSTORE.
            if unchanged_since.is_some() && !opts.has_condstore() {
                return Err(EncodeError::MissingCapability {
                    cmd: "UID STORE (UNCHANGEDSINCE)",
                    cap: "CONDSTORE".into(),
                });
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_store(
                buf,
                tag,
                true,
                sequence_set.as_str(),
                *operation,
                flags,
                *unchanged_since,
            )?;
        }
        Command::UidMove {
            sequence_set,
            mailbox,
        } => {
            // RFC 6851 Section 3: MOVE requires MOVE capability.
            if !opts.has_capability(&Capability::Move) {
                return Err(EncodeError::MissingCapability {
                    cmd: "UID MOVE",
                    cap: "MOVE".into(),
                });
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_two_arg(
                buf,
                tag,
                true,
                "MOVE",
                sequence_set.as_str(),
                &wire,
                utf8,
                literal_mode,
            )?;
        }
        Command::UidCopy {
            sequence_set,
            mailbox,
        } => {
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_two_arg(
                buf,
                tag,
                true,
                "COPY",
                sequence_set.as_str(),
                &wire,
                utf8,
                literal_mode,
            )?;
        }
        Command::UidExpunge { sequence_set } => {
            // RFC 4315: UID EXPUNGE requires UIDPLUS capability.
            if !opts.has_capability(&Capability::UidPlus) {
                return Err(EncodeError::MissingCapability {
                    cmd: "UID EXPUNGE",
                    cap: "UIDPLUS".into(),
                });
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_uid_expunge(buf, tag, sequence_set.as_str())?;
        }
        // RFC 2342 Section 5 / RFC 9051 Section 6.3.10
        Command::Namespace => {
            if !opts.has_capability(&Capability::Namespace) {
                return Err(EncodeError::MissingCapability {
                    cmd: "NAMESPACE",
                    cap: "NAMESPACE".into(),
                });
            }
            encode_simple(buf, tag, "NAMESPACE");
        }
        // RFC 3501 Section 6.4.1 (removed in IMAP4rev2)
        Command::Check => {
            encode_simple(buf, tag, "CHECK");
        }
        // RFC 3501 Section 6.4.3 / RFC 9051 Section 6.4.3
        Command::Expunge => {
            encode_simple(buf, tag, "EXPUNGE");
        }
        // RFC 2177 Section 3 / RFC 9051 Section 6.3.13
        Command::Idle => {
            // RFC 2177 Section 1: IDLE requires IDLE capability.
            if !opts.has_capability(&Capability::Idle) {
                return Err(EncodeError::MissingCapability {
                    cmd: "IDLE",
                    cap: "IDLE".into(),
                });
            }
            encode_simple(buf, tag, "IDLE");
        }
        // RFC 3501 Section 6.1.1 / RFC 9051 Section 6.1.1
        Command::Capability => {
            encode_simple(buf, tag, "CAPABILITY");
        }
        // RFC 3501 Section 6.1.2 / RFC 9051 Section 6.1.2
        Command::Noop => {
            encode_simple(buf, tag, "NOOP");
        }
        Command::Id(params) => {
            // RFC 2971 Section 2: ID requires ID capability.
            if !opts.has_capability(&Capability::Id) {
                return Err(EncodeError::MissingCapability {
                    cmd: "ID",
                    cap: "ID".into(),
                });
            }
            encode_id(buf, tag, params, utf8, literal_mode)?;
        }
        Command::GetMetadata {
            mailbox,
            entries,
            max_size,
            depth,
        } => {
            // RFC 5464 Section 1: METADATA or METADATA-SERVER capability required.
            if !opts.has_capability(&Capability::Metadata)
                && !opts.has_capability(&Capability::MetadataServer)
            {
                return Err(EncodeError::MissingCapability {
                    cmd: "GETMETADATA",
                    cap: "METADATA".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_getmetadata(
                buf,
                tag,
                &wire,
                entries,
                *max_size,
                depth.as_deref(),
                utf8,
                literal_mode,
            )?;
        }
        Command::SetMetadata { mailbox, entries } => {
            // RFC 5464 Section 1: METADATA or METADATA-SERVER capability required.
            if !opts.has_capability(&Capability::Metadata)
                && !opts.has_capability(&Capability::MetadataServer)
            {
                return Err(EncodeError::MissingCapability {
                    cmd: "SETMETADATA",
                    cap: "METADATA".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_setmetadata(buf, tag, &wire, entries, utf8, literal_mode)?;
        }
        Command::Thread {
            algorithm,
            charset,
            criteria,
        } => {
            encode_thread_or_sort_cmd(
                buf,
                tag,
                "THREAD",
                algorithm,
                charset,
                criteria,
                false,
                literal_mode,
            )?;
        }
        Command::UidThread {
            algorithm,
            charset,
            criteria,
        } => {
            encode_thread_or_sort_cmd(
                buf,
                tag,
                "UID THREAD",
                algorithm,
                charset,
                criteria,
                false,
                literal_mode,
            )?;
        }
        Command::Sort {
            sort_criteria,
            charset,
            criteria,
        } => {
            encode_thread_or_sort_cmd(
                buf,
                tag,
                "SORT",
                sort_criteria,
                charset,
                criteria,
                true,
                literal_mode,
            )?;
        }
        Command::UidSort {
            sort_criteria,
            charset,
            criteria,
        } => {
            encode_thread_or_sort_cmd(
                buf,
                tag,
                "UID SORT",
                sort_criteria,
                charset,
                criteria,
                true,
                literal_mode,
            )?;
        }
        Command::Compress => {
            // RFC 4978 Section 4: COMPRESS DEFLATE requires COMPRESS=DEFLATE.
            if !opts.has_capability(&Capability::CompressDeflate) {
                return Err(EncodeError::MissingCapability {
                    cmd: "COMPRESS",
                    cap: "COMPRESS=DEFLATE".into(),
                });
            }
            buf.extend_from_slice(tag.as_bytes());
            buf.extend_from_slice(b" COMPRESS DEFLATE\r\n");
        }

        // --- QUOTA (RFC 2087) ---
        Command::GetQuota { root } => {
            // RFC 2087 Section 4.2: GETQUOTA requires QUOTA capability.
            if !opts.has_capability(&Capability::Quota) {
                return Err(EncodeError::MissingCapability {
                    cmd: "GETQUOTA",
                    cap: "QUOTA".into(),
                });
            }
            encode_mailbox_cmd(buf, tag, "GETQUOTA", root, utf8, literal_mode);
        }
        Command::GetQuotaRoot { mailbox } => {
            // RFC 2087 Section 4.3: GETQUOTAROOT requires QUOTA capability.
            if !opts.has_capability(&Capability::Quota) {
                return Err(EncodeError::MissingCapability {
                    cmd: "GETQUOTAROOT",
                    cap: "QUOTA".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_mailbox_cmd(buf, tag, "GETQUOTAROOT", &wire, utf8, literal_mode);
        }
        Command::SetQuota { root, resources } => {
            // RFC 9208 Section 4.1.3: SETQUOTA requires QUOTASET capability.
            if !opts.has_capability(&Capability::QuotaSet) {
                return Err(EncodeError::MissingCapability {
                    cmd: "SETQUOTA",
                    cap: "QUOTASET".into(),
                });
            }
            encode_set_quota(buf, tag, root, resources, utf8, literal_mode)?;
        }

        // --- ACL (RFC 4314) ---
        Command::SetAcl {
            mailbox,
            identifier,
            rights,
        } => {
            // RFC 4314 Section 3.1: SETACL requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(EncodeError::MissingCapability {
                    cmd: "SETACL",
                    cap: "ACL".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_set_acl(buf, tag, &wire, identifier, rights, utf8, literal_mode);
        }
        Command::DeleteAcl {
            mailbox,
            identifier,
        } => {
            // RFC 4314 Section 3.2: DELETEACL requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(EncodeError::MissingCapability {
                    cmd: "DELETEACL",
                    cap: "ACL".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_two_quoted_args(buf, tag, "DELETEACL", &wire, identifier, utf8, literal_mode);
        }
        Command::GetAcl { mailbox } => {
            // RFC 4314 Section 3.3: GETACL requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(EncodeError::MissingCapability {
                    cmd: "GETACL",
                    cap: "ACL".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_mailbox_cmd(buf, tag, "GETACL", &wire, utf8, literal_mode);
        }
        Command::ListRights {
            mailbox,
            identifier,
        } => {
            // RFC 4314 Section 3.4: LISTRIGHTS requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(EncodeError::MissingCapability {
                    cmd: "LISTRIGHTS",
                    cap: "ACL".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_two_quoted_args(
                buf,
                tag,
                "LISTRIGHTS",
                &wire,
                identifier,
                utf8,
                literal_mode,
            );
        }
        Command::MyRights { mailbox } => {
            // RFC 4314 Section 3.5: MYRIGHTS requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(EncodeError::MissingCapability {
                    cmd: "MYRIGHTS",
                    cap: "ACL".into(),
                });
            }
            let wire = encode_mailbox_name(mailbox, utf8);
            encode_mailbox_cmd(buf, tag, "MYRIGHTS", &wire, utf8, literal_mode);
        }

        // --- NOTIFY (RFC 5465) ---
        Command::NotifySet(params) => {
            // RFC 5465 Section 3: NOTIFY requires NOTIFY capability.
            if !opts.has_capability(&Capability::Notify) {
                return Err(EncodeError::MissingCapability {
                    cmd: "NOTIFY SET",
                    cap: "NOTIFY".into(),
                });
            }
            encode_notify_set(buf, tag, params, utf8, literal_mode)?;
        }
        Command::NotifyNone => {
            // RFC 5465 Section 3: NOTIFY requires NOTIFY capability.
            if !opts.has_capability(&Capability::Notify) {
                return Err(EncodeError::MissingCapability {
                    cmd: "NOTIFY NONE",
                    cap: "NOTIFY".into(),
                });
            }
            encode_simple(buf, tag, "NOTIFY NONE");
        }
    }
    Ok(())
}
