use bytes::BytesMut;

/// Wire literal syntax for APPEND message data.
///
/// RFC 3501 Section 4.3 / RFC 9051 Section 4.3 define classic `literal`
/// syntax for `CHAR8` data (no NUL octets). RFC 3516 Section 4.4 extends
/// APPEND with `literal8` for binary data, and RFC 6855 Section 4 wraps
/// `literal8` in `UTF8 (...)` when UTF8=ACCEPT is enabled for UTF-8 headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AppendLiteralKind {
    /// Classic `{N}` / `{N+}` APPEND literal for `CHAR8` data.
    /// RFC 3501 Section 4.3 / RFC 9051 Section 4.3.
    Literal,
    /// Binary `~{N}` APPEND literal for data containing NUL octets.
    /// RFC 3516 Section 4.4.
    Literal8,
    /// UTF8 APPEND wrapper using `UTF8 (~{N})`.
    /// RFC 6855 Section 4.
    Utf8Literal8,
}

/// Find the next synchronizing literal boundary (`{digits}\r\n`) in `buf`.
///
/// Returns `Some((offset, size))` where `offset` is past the `\r\n` (i.e., the
/// literal data starts at `buf[offset..]`), and `size` is the literal byte count.
/// Returns `None` if no literal is found.
///
/// Only matches `{digits}\r\n` (synchronizing), NOT `{digits+}\r\n` (LITERAL+).
pub(super) fn find_literal_boundary(buf: &[u8]) -> Option<(usize, usize)> {
    let mut i = 0;
    while i < buf.len() {
        if buf[i] == b'{' {
            let start = i + 1;
            // Scan for digits
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
                // Parse the literal size so callers can skip the body.
                // If the digit sequence is not valid UTF-8 or overflows usize,
                // skip this candidate and keep scanning  -  returning a 0-byte
                // literal would desynchronize the caller (RFC 3501 Section 4.3).
                let Ok(size_str) = std::str::from_utf8(&buf[start..j]) else {
                    i += 1;
                    continue;
                };
                let Ok(size) = size_str.parse::<usize>() else {
                    i += 1;
                    continue;
                };
                // This is a synchronizing literal (no `+` before `}`)
                return Some((j + 3, size));
            }
        }
        i += 1;
    }
    None
}

/// Patch all synchronizing literal markers in `buf` to non-synchronizing (LITERAL+).
///
/// Replaces every `{digits}\r\n` with `{digits+}\r\n` (RFC 7888 Section 4).
/// Length-aware: after patching a marker, skips the literal body so that
/// `{digits}\r\n` patterns inside literal data are not modified.
///
/// Literal8 markers (`~{digits}\r\n`, RFC 3516) are only converted when the
/// caller indicates that both BINARY and the relevant literal extension are
/// active (RFC 7888 Section 6).
pub(super) fn patch_literals_to_plus_with_binary(buf: &[u8], allow_literal8: bool) -> BytesMut {
    let mut result = BytesMut::with_capacity(buf.len() + 16);
    let mut i = 0;
    while i < buf.len() {
        if buf[i] == b'{' {
            let start = i + 1;
            let mut j = start;
            while j < buf.len() && buf[j].is_ascii_digit() {
                j += 1;
            }
            if j > start
                && j + 2 < buf.len()
                && buf[j] == b'}'
                && buf[j + 1] == b'\r'
                && buf[j + 2] == b'\n'
            {
                // Parse the literal size to skip the body.
                // If the digit sequence overflows usize, treat the `{...}` as
                // plain text  -  do not patch it (RFC 3501 Section 4.3).
                let Ok(size_str) = std::str::from_utf8(&buf[start..j]) else {
                    result.extend_from_slice(&buf[i..=i]);
                    i += 1;
                    continue;
                };
                let Ok(size) = size_str.parse::<usize>() else {
                    result.extend_from_slice(&buf[i..=i]);
                    i += 1;
                    continue;
                };

                // RFC 7888 Section 6 / RFC 3516: literal8 markers (`~{N}\r\n`)
                // may only use the non-synchronizing form when BINARY is also
                // advertised alongside LITERAL+.
                let is_literal8 = i > 0 && buf[i - 1] == b'~';

                // Copy `{digits` then insert `+}\r\n` for non-synchronizing
                // literals that the server advertised support for.
                result.extend_from_slice(&buf[i..j]);
                if !is_literal8 || allow_literal8 {
                    result.extend_from_slice(b"+}\r\n");
                } else {
                    result.extend_from_slice(b"}\r\n");
                }
                let body_start = j + 3;
                // Copy the literal body verbatim (RFC 3501 Section 4.3).
                // Use checked arithmetic to prevent overflow when `size`
                // is near `usize::MAX` (e.g., from a crafted command).
                let body_end = body_start
                    .checked_add(size)
                    .map_or(buf.len(), |end| end.min(buf.len()));
                result.extend_from_slice(&buf[body_start..body_end]);
                i = body_end;
                continue;
            }
        }
        result.extend_from_slice(&buf[i..=i]);
        i += 1;
    }
    result
}

/// Patch synchronizing literals up to 4096 bytes to non-synchronizing (LITERAL-).
///
/// Converts `{digits}\r\n` to `{digits+}\r\n` only when `digits` (the
/// literal octet count) is <= 4096.
/// Larger literals are left as synchronizing, per RFC 7888 Section 5.
///
/// Literal8 markers (`~{digits}\r\n`, RFC 3516) are only converted when the
/// caller indicates that both BINARY and the relevant literal extension are
/// active (RFC 7888 Section 6).
///
/// Length-aware: after patching (or skipping) a marker, skips the literal
/// body so that `{digits}\r\n` patterns inside literal data are not modified
/// (RFC 3501 Section 4.3).
pub(super) fn patch_small_literals_to_plus_with_binary(
    buf: &[u8],
    allow_literal8: bool,
) -> BytesMut {
    /// RFC 7888 Section 5: LITERAL- limit.
    const LITERAL_MINUS_MAX: usize = 4096;

    let mut result = BytesMut::with_capacity(buf.len() + 16);
    let mut i = 0;
    while i < buf.len() {
        if buf[i] == b'{' {
            let start = i + 1;
            let mut j = start;
            while j < buf.len() && buf[j].is_ascii_digit() {
                j += 1;
            }
            if j > start
                && j + 2 < buf.len()
                && buf[j] == b'}'
                && buf[j + 1] == b'\r'
                && buf[j + 2] == b'\n'
            {
                // Parse the literal size to decide whether to patch and to skip the body.
                // If the digit sequence is not valid UTF-8 or overflows usize,
                // treat the `{` as plain text  -  do not patch it (RFC 3501 Section 4.3).
                let Ok(size_str) = std::str::from_utf8(&buf[start..j]) else {
                    result.extend_from_slice(&buf[i..=i]);
                    i += 1;
                    continue;
                };
                let Ok(size) = size_str.parse::<usize>() else {
                    result.extend_from_slice(&buf[i..=i]);
                    i += 1;
                    continue;
                };

                // RFC 7888 Section 6 / RFC 3516: literal8 markers (`~{N}\r\n`)
                // may only use the non-synchronizing form when BINARY is also
                // advertised alongside the literal extension.
                let is_literal8 = i > 0 && buf[i - 1] == b'~';

                // Copy `{digits`
                result.extend_from_slice(&buf[i..j]);
                if size <= LITERAL_MINUS_MAX && (!is_literal8 || allow_literal8) {
                    // RFC 7888 Section 5: small literal, upgrade to non-synchronizing.
                    result.extend_from_slice(b"+}\r\n");
                } else {
                    // Large literal or literal8: leave as synchronizing.
                    result.extend_from_slice(b"}\r\n");
                }
                let body_start = j + 3;
                // Copy the literal body verbatim (RFC 3501 Section 4.3).
                // Use checked arithmetic to prevent overflow when `size`
                // is near `usize::MAX` (e.g., from a crafted command).
                let body_end = body_start
                    .checked_add(size)
                    .map_or(buf.len(), |end| end.min(buf.len()));
                result.extend_from_slice(&buf[body_start..body_end]);
                i = body_end;
                continue;
            }
        }
        result.extend_from_slice(&buf[i..=i]);
        i += 1;
    }
    result
}

#[cfg(test)]
#[path = "literals_tests.rs"]
mod tests;
