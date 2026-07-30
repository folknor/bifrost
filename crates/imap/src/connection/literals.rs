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

/// Parse a literal marker at `pos`, returning its data offset, byte count, and
/// whether it is synchronizing.
///
/// The count is returned as `u64`, not `usize`: RFC 9051 Section 9 declares it
/// as `number64`, so a marker can name a value that no `usize` on a 32-bit
/// target can hold. Narrowing here would make the parse result depend on the
/// pointer width; callers decide what an unrepresentable count means for them.
///
/// Both classic and LITERAL+ markers use the same counted framing. Callers
/// that scan a complete command must skip an already non-synchronizing body,
/// too, or marker-like data in that body becomes command syntax.
pub(super) fn literal_marker_at(buf: &[u8], pos: usize) -> Option<(usize, u64, bool)> {
    if buf.get(pos) != Some(&b'{') {
        return None;
    }

    let start = pos + 1;
    let mut end = start;
    while end < buf.len() && buf[end].is_ascii_digit() {
        end += 1;
    }
    if end == start {
        return None;
    }

    let synchronizing = match buf.get(end) {
        Some(b'}') => true,
        Some(b'+') => {
            end += 1;
            false
        }
        _ => return None,
    };
    if buf.get(end) != Some(&b'}')
        || buf.get(end + 1) != Some(&b'\r')
        || buf.get(end + 2) != Some(&b'\n')
    {
        return None;
    }

    let digit_end = if synchronizing { end } else { end - 1 };
    let size = std::str::from_utf8(&buf[start..digit_end])
        .ok()?
        .parse::<u64>()
        .ok()?;
    Some((end + 3, size, synchronizing))
}

/// [`literal_marker_at`] for callers that scan a buffer we built ourselves, so
/// a count that no `usize` can hold cannot be a marker they own: it is treated
/// as ordinary text, exactly like a malformed marker.
fn literal_marker_at_usize(buf: &[u8], pos: usize) -> Option<(usize, usize, bool)> {
    let (data_start, size, synchronizing) = literal_marker_at(buf, pos)?;
    Some((data_start, usize::try_from(size).ok()?, synchronizing))
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
        if let Some((data_start, size, synchronizing)) = literal_marker_at_usize(buf, i) {
            if synchronizing {
                return Some((data_start, size));
            }

            // LITERAL+ has no continuation, but its counted payload is still
            // opaque command data. Do not inspect marker-like bytes in it.
            let data_end = data_start.checked_add(size)?;
            if data_end > buf.len() {
                return None;
            }
            i = data_end;
            continue;
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
        if let Some((data_start, size, synchronizing)) = literal_marker_at_usize(buf, i) {
            if synchronizing {
                // RFC 7888 Section 6 / RFC 3516: literal8 markers (`~{N}\r\n`)
                // may only use the non-synchronizing form when BINARY is also
                // advertised alongside LITERAL+.
                let is_literal8 = i > 0 && buf[i - 1] == b'~';
                result.extend_from_slice(&buf[i..data_start - 3]);
                if !is_literal8 || allow_literal8 {
                    result.extend_from_slice(b"+}\r\n");
                } else {
                    result.extend_from_slice(b"}\r\n");
                }
            } else {
                result.extend_from_slice(&buf[i..data_start]);
            }

            let body_end = data_start
                .checked_add(size)
                .map_or(buf.len(), |end| end.min(buf.len()));
            result.extend_from_slice(&buf[data_start..body_end]);
            i = body_end;
            continue;
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
        if let Some((data_start, size, synchronizing)) = literal_marker_at_usize(buf, i) {
            if synchronizing {
                // RFC 7888 Section 6 / RFC 3516: literal8 markers (`~{N}\r\n`)
                // may only use the non-synchronizing form when BINARY is also
                // advertised alongside the literal extension.
                let is_literal8 = i > 0 && buf[i - 1] == b'~';
                result.extend_from_slice(&buf[i..data_start - 3]);
                if size <= LITERAL_MINUS_MAX && (!is_literal8 || allow_literal8) {
                    // RFC 7888 Section 5: small literal, upgrade to non-synchronizing.
                    result.extend_from_slice(b"+}\r\n");
                } else {
                    // Large literal or literal8: leave as synchronizing.
                    result.extend_from_slice(b"}\r\n");
                }
            } else {
                result.extend_from_slice(&buf[i..data_start]);
            }

            let body_end = data_start
                .checked_add(size)
                .map_or(buf.len(), |end| end.min(buf.len()));
            result.extend_from_slice(&buf[data_start..body_end]);
            i = body_end;
            continue;
        }
        result.extend_from_slice(&buf[i..=i]);
        i += 1;
    }
    result
}

#[cfg(test)]
#[path = "literals_tests.rs"]
mod tests;
