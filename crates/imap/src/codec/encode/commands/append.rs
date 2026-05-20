//! APPEND-family command helpers.

use super::{
    BytesMut, LITERAL_MINUS_MAX, LiteralMode, encode_quoted_or_literal,
    encode_quoted_or_literal_utf8, validate_and_filter_flags, validate_append_datetime,
};

/// Encode the header portion of one message in a MULTIAPPEND command (RFC 3502 Section 3).
///
/// Writes: `[tag " APPEND " mailbox] [" (" flags ")"] [" " quoted-date] " {" size ["+" ] "}"`.
/// If `first` is `true`, the `tag APPEND mailbox` prefix is included.
/// The literal data itself is NOT included; the caller must send it separately
/// because literal synchronization may require a server continuation.
///
/// `literal_mode` controls literal marker style per [`LiteralMode`]
/// (RFC 7888 Sections 4-5).
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_multi_append_header(
    buf: &mut BytesMut,
    tag: &str,
    mailbox: &str,
    flags: &[crate::types::Flag],
    date: Option<&str>,
    message_len: usize,
    first: bool,
    literal_mode: LiteralMode,
    utf8: bool,
) -> Result<(), crate::Error> {
    encode_multi_append_header_with_literal8(
        buf,
        tag,
        mailbox,
        flags,
        date,
        message_len,
        first,
        literal_mode,
        utf8,
        utf8,
    )
}

/// Encode the header portion of one message in a MULTIAPPEND command (RFC 3502 Section 3).
///
/// `literal8` selects the binary literal syntax from RFC 3516 / RFC 9051 Section 9:
/// `~{size}\r\n`. Callers MUST only enable `literal8` when the server accepts
/// literal8 in the relevant command context.
#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_multi_append_header_with_literal8(
    buf: &mut BytesMut,
    tag: &str,
    mailbox: &str,
    flags: &[crate::types::Flag],
    date: Option<&str>,
    message_len: usize,
    first: bool,
    literal_mode: LiteralMode,
    utf8: bool,
    literal8: bool,
) -> Result<(), crate::Error> {
    if first {
        // Tag + APPEND + mailbox (RFC 3502 Section 3).
        // RFC 6855 Section 3: when UTF8=ACCEPT is active, the server MUST accept
        // UTF-8 in quoted strings, so non-ASCII mailbox names can use quoted form
        // instead of falling back to a synchronizing literal.
        buf.extend_from_slice(tag.as_bytes());
        buf.extend_from_slice(b" APPEND ");
        encode_quoted_or_literal_utf8(buf, mailbox.as_bytes(), utf8, literal_mode);
    }

    // Flags (RFC 3501 Section 6.3.11).
    let valid_flags = validate_and_filter_flags(flags, "APPEND")?;
    if !valid_flags.is_empty() {
        buf.extend_from_slice(b" (");
        for (i, flag) in valid_flags.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b" ");
            }
            buf.extend_from_slice(flag.as_imap_str().as_bytes());
        }
        buf.extend_from_slice(b")");
    }

    // Internal date (RFC 3501 Section 6.3.11).
    // Validate against the date-time production (RFC 3501 Section 9).
    if let Some(d) = date {
        validate_append_datetime(d)?;
        buf.extend_from_slice(b" ");
        encode_quoted_or_literal(buf, d.as_bytes(), literal_mode);
    }

    // Literal header (RFC 3501 Section 9 / RFC 7888 for LITERAL+).
    // RFC 6855 Section 4: when UTF8=ACCEPT is enabled, use the UTF8
    // APPEND data extension: `UTF8 (~{size}\r\n<message>)`.
    // RFC 3516 Section 4.4 / RFC 9051 Section 9: binary APPEND data with NUL
    // octets uses `literal8` (`~{size}\r\n<data>`).
    // RFC 9051 Section 9: literal8 = "~{" number64 "}" CRLF *OCTET; no `["+"]`
    // modifier, so the non-synchronizing `+` suffix must NOT be used with literal8.
    if utf8 {
        buf.extend_from_slice(b" UTF8 (~{");
    } else if literal8 {
        buf.extend_from_slice(b" ~{");
    } else {
        buf.extend_from_slice(b" {");
    }
    buf.extend_from_slice(message_len.to_string().as_bytes());
    // RFC 7888 Section 4: LITERAL+ is non-synchronizing for any size.
    // RFC 7888 Section 5: LITERAL- is non-synchronizing only up to 4096 bytes.
    // RFC 9051 Section 9: literal8 never gets `+`.
    let use_non_sync = !utf8
        && !literal8
        && match literal_mode {
            LiteralMode::LiteralPlus => true,
            LiteralMode::LiteralMinus => message_len <= LITERAL_MINUS_MAX,
            LiteralMode::Synchronizing => false,
        };
    if use_non_sync {
        buf.extend_from_slice(b"+");
    }
    buf.extend_from_slice(b"}\r\n");
    Ok(())
}
