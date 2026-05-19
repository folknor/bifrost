//! String and literal encoding helpers for IMAP commands.
//!
//! Provides functions for encoding byte slices as IMAP quoted strings or
//! literals (RFC 3501 Section 9 / RFC 9051 Section 9), with support for
//! UTF-8 mode (RFC 6855 Section 3) and literal8 (RFC 3516).

use super::{BytesMut, LITERAL_MINUS_MAX, LiteralMode};

/// Encode a byte slice as a quoted string or literal, depending on content.
///
/// Uses quoted form when safe (no CR, LF, NUL, or backslash/quote without escaping issues).
/// Falls back to literal for binary-unsafe content.
/// Quoted and literal formats per RFC 3501 Section 9 / RFC 9051 Section 9.
///
/// NUL bytes (%x00) are stripped from the data before encoding, per
/// RFC 3501 Section 9 rule (3): "The ASCII NUL character, %x00, MUST NOT
/// be used at any time." A `tracing::warn!` is emitted when NUL bytes are
/// encountered so that callers passing NUL bytes are noticed in logs.
pub(crate) fn encode_quoted_or_literal(buf: &mut BytesMut, data: &[u8], literal_mode: LiteralMode) {
    let data = strip_nul_bytes(data);

    // Check if the data can be safely quoted.
    // TEXT-CHAR per RFC 3501 Section 9: CHAR = %x01-7F, TEXT-CHAR = <any CHAR except CR and LF>.
    // NUL (%x00) is already stripped above.
    // RFC 9051 Section 9: CHAR = %x01-7E. DEL (%x7F) is excluded in
    // IMAP4rev2; using a literal for DEL is safe for both rev1 and rev2.
    // Additionally, while control chars %x01-1F (except CR/LF) are technically
    // valid CHAR per RFC 3501, many servers reject them in quoted strings.
    // Per "be conservative in what you send," use literal form for all control
    // characters to maximize interoperability.
    let quotable = data.iter().all(|&b| (0x20..0x7F).contains(&b));

    emit_quoted_or_literal(buf, &data, quotable, literal_mode);
}

/// Encode a byte slice as a quoted string or literal, with optional UTF-8 mode.
///
/// When `utf8_mode` is `true` (i.e., UTF8=ACCEPT is active per RFC 6855 Section 3),
/// valid UTF-8 sequences are permitted in quoted strings. RFC 6855 Section 3:
/// "The server MUST accept UTF-8 in quoted strings." RFC 9051 Section 9 extends
/// `CHAR` to include `UTF8-2 / UTF8-3 / UTF8-4`, making non-ASCII UTF-8 bytes
/// legal in quoted strings.
///
/// When `utf8_mode` is `false`, uses the standard RFC 3501 Section 9 check where
/// only `%x01-7F` (minus CR/LF) is quotable.
///
/// NUL bytes (%x00) are stripped regardless of mode, per RFC 3501 Section 9
/// rule (3): "The ASCII NUL character, %x00, MUST NOT be used at any time."
pub(crate) fn encode_quoted_or_literal_utf8(
    buf: &mut BytesMut,
    data: &[u8],
    utf8_mode: bool,
    literal_mode: LiteralMode,
) {
    if !utf8_mode {
        // Delegate to the existing ASCII-only encoder.
        encode_quoted_or_literal(buf, data, literal_mode);
        return;
    }

    let data = strip_nul_bytes(data);

    // RFC 6855 Section 3 / RFC 9051 Section 9: when UTF8=ACCEPT is active,
    // quoted strings may contain UTF-8 sequences. The quotable check becomes:
    // the data must be valid UTF-8, and must not contain CR (%x0D), LF (%x0A),
    // DEL (%x7F), or ASCII control characters (%x01-1F). NUL is already
    // stripped above. Control characters are excluded for the same
    // interoperability reason as in `encode_quoted_or_literal`.
    // RFC 9051 Section 9: CHAR = %x01-7E / UTF8-2 / UTF8-3 / UTF8-4  -
    // DEL (0x7F) is a single-byte ASCII character excluded from CHAR.
    let quotable = std::str::from_utf8(&data).is_ok()
        && data.iter().all(|&b| (b >= 0x20 && b != 0x7F) || b >= 0x80);

    emit_quoted_or_literal(buf, &data, quotable, literal_mode);
}

/// Defensively strip NUL bytes from IMAP string data.
///
/// RFC 3501 Section 9: CHAR8 = %x01-ff  -  NUL (%x00) is forbidden.
/// Returns a `Cow` to avoid allocation when no NUL bytes are present.
fn strip_nul_bytes(data: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    if data.contains(&0x00) {
        tracing::warn!(
            "Stripped NUL bytes from IMAP string data  -  RFC 3501 Section 9 forbids %x00"
        );
        std::borrow::Cow::Owned(data.iter().copied().filter(|&b| b != 0x00).collect())
    } else {
        std::borrow::Cow::Borrowed(data)
    }
}

/// Write data as a quoted string, escaping `\` and `"`.
///
/// RFC 3501 Section 9: `quoted = DQUOTE *QUOTED-CHAR DQUOTE`, where
/// quoted-specials (backslash and double-quote) are escaped with backslash.
///
/// The caller must ensure the data is quotable (no CR, LF, or bytes outside
/// the allowed range for the current encoding mode).
pub(super) fn emit_quoted_string(buf: &mut BytesMut, data: &[u8]) {
    buf.extend_from_slice(b"\"");
    for &byte in data {
        // Escape backslash and double-quote per RFC 3501 Section 9.
        if byte == b'\\' || byte == b'"' {
            buf.extend_from_slice(b"\\");
        }
        buf.extend_from_slice(&[byte]);
    }
    buf.extend_from_slice(b"\"");
}

/// Write data as either a quoted string or literal to the buffer.
///
/// RFC 3501 Section 9: uses quoted form when `quotable` is true,
/// otherwise falls back to literal form.
///
/// The `literal_mode` parameter controls the literal marker style:
/// - [`LiteralMode::LiteralPlus`]: non-synchronizing `{N+}\r\n` for all sizes
///   (RFC 7888 Section 4).
/// - [`LiteralMode::LiteralMinus`]: non-synchronizing `{N+}\r\n` only when
///   `data.len() <= 4096`; larger literals use synchronizing `{N}\r\n`
///   (RFC 7888 Section 5).
/// - [`LiteralMode::Synchronizing`]: synchronizing `{N}\r\n` always
///   (RFC 3501 Section 4.3).
pub(super) fn emit_quoted_or_literal(
    buf: &mut BytesMut,
    data: &[u8],
    quotable: bool,
    literal_mode: LiteralMode,
) {
    if quotable {
        emit_quoted_string(buf, data);
    } else {
        // RFC 3501 Section 9 / RFC 7888: literal form.
        buf.extend_from_slice(b"{");
        buf.extend_from_slice(data.len().to_string().as_bytes());
        // RFC 7888 Section 4: LITERAL+  -  non-synchronizing for any size.
        // RFC 7888 Section 5: LITERAL-  -  non-synchronizing only up to 4096 bytes.
        // RFC 3501 Section 4.3: no extension  -  always synchronizing.
        let use_non_sync = match literal_mode {
            LiteralMode::LiteralPlus => true,
            LiteralMode::LiteralMinus => data.len() <= LITERAL_MINUS_MAX,
            LiteralMode::Synchronizing => false,
        };
        if use_non_sync {
            buf.extend_from_slice(b"+}\r\n");
        } else {
            buf.extend_from_slice(b"}\r\n");
        }
        buf.extend_from_slice(data);
    }
}

/// Encode a byte slice as a literal8 (`~{N}\r\n<data>`).
///
/// RFC 3516: `literal8 = "~{" number "}" CRLF *OCTET`
/// Unlike standard literals, literal8 uses `*OCTET` (%x00-FF) which includes
/// NUL bytes. This is required for binary data such as METADATA values.
pub(super) fn encode_literal8(buf: &mut BytesMut, data: &[u8]) {
    // RFC 3516: ~{<size>}\r\n<data>
    buf.extend_from_slice(b"~{");
    buf.extend_from_slice(data.len().to_string().as_bytes());
    buf.extend_from_slice(b"}\r\n");
    buf.extend_from_slice(data);
}

/// Encode a METADATA value as quoted string, classic literal, or literal8.
///
/// RFC 5464 Section 5 defines `value = nstring / literal8`. `nstring`
/// expands to `string / nil` (RFC 3501 Section 9 / RFC 9051 Section 9), and
/// `string` includes classic IMAP literals carrying `*CHAR8`, where
/// `CHAR8 = %x01-ff`. As a result:
/// - printable ASCII can use quoted form;
/// - any non-NUL non-quotable octets can use classic literal form; and
/// - only NUL (`%x00`) requires literal8 (`*OCTET`) from RFC 3516.
pub(super) fn encode_metadata_value(buf: &mut BytesMut, data: &[u8], literal_mode: LiteralMode) {
    // Per "be conservative in what you send" (Postel's law), only allow
    // printable ASCII (0x20-0x7E) in quoted strings. Control bytes, DEL,
    // CR/LF, and high bytes all fall back to literal form instead.
    let quotable = data.iter().all(|&b| (0x20..0x7F).contains(&b));

    if data.contains(&0) {
        // RFC 3516 literal8 is only needed for true binary NUL-bearing data.
        encode_literal8(buf, data);
    } else {
        emit_quoted_or_literal(buf, data, quotable, literal_mode);
    }
}
