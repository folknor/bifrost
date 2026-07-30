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

/// RFC 9051 Section 9 ceiling for `number64`: an unsigned 63-bit integer, so
/// the largest legal literal count is `i64::MAX`.
pub(crate) const NUMBER64_MAX: u64 = i64::MAX as u64;

/// The three answers [`literal_marker_at`] can give.
///
/// "Not a marker" and "a marker naming octets that cannot legally exist" are
/// different facts and the right reaction differs by call site: on the read
/// path an out-of-range count is fatal framing (waiting for those octets would
/// stall forever), while on the send path it is text in a buffer we built, so
/// it simply is not a boundary we own. Collapsing the two into `None` is what
/// let the ceiling go unenforced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiteralMarker {
    /// Well-formed marker whose count is a legal RFC 9051 `number64`.
    Counted {
        /// Offset of the first literal-body octet.
        data_start: usize,
        /// Declared octet count; always `<= NUMBER64_MAX`.
        size: u64,
        /// `false` for the LITERAL+/LITERAL- `{N+}` form.
        synchronizing: bool,
    },
    /// Well-formed framing whose count is above the `number64` ceiling.
    /// `data_start` is where the body would have begun, so a caller can still
    /// tell whether this marker sits at the boundary it was asking about.
    CountOutOfRange { data_start: usize, size: u64 },
    /// No literal marker begins at this position.
    NotAMarker,
}

/// Parse a literal marker at `pos`.
///
/// The count is carried as `u64`, not `usize`: RFC 9051 Section 9 declares it
/// as `number64`, so a marker can name a value that no `usize` on a 32-bit
/// target can hold. Narrowing here would make the parse result depend on the
/// pointer width; callers decide what an unrepresentable count means for them.
///
/// A digit run too long for `u64` is [`LiteralMarker::NotAMarker`], not
/// `CountOutOfRange`: such a line is not recognizable framing at all, and the
/// read path deliberately hands it to the decoder rather than declaring a
/// framing error of its own.
///
/// Both classic and LITERAL+ markers use the same counted framing. Callers
/// that scan a complete command must skip an already non-synchronizing body,
/// too, or marker-like data in that body becomes command syntax.
pub(crate) fn literal_marker_at(buf: &[u8], pos: usize) -> LiteralMarker {
    if buf.get(pos) != Some(&b'{') {
        return LiteralMarker::NotAMarker;
    }

    let start = pos + 1;
    let mut end = start;
    while end < buf.len() && buf[end].is_ascii_digit() {
        end += 1;
    }
    if end == start {
        return LiteralMarker::NotAMarker;
    }

    let synchronizing = match buf.get(end) {
        Some(b'}') => true,
        Some(b'+') => {
            end += 1;
            false
        }
        _ => return LiteralMarker::NotAMarker,
    };
    if buf.get(end) != Some(&b'}')
        || buf.get(end + 1) != Some(&b'\r')
        || buf.get(end + 2) != Some(&b'\n')
    {
        return LiteralMarker::NotAMarker;
    }

    let digit_end = if synchronizing { end } else { end - 1 };
    let Some(size) = std::str::from_utf8(&buf[start..digit_end])
        .ok()
        .and_then(|digits| digits.parse::<u64>().ok())
    else {
        return LiteralMarker::NotAMarker;
    };
    if size > NUMBER64_MAX {
        return LiteralMarker::CountOutOfRange {
            data_start: end + 3,
            size,
        };
    }
    LiteralMarker::Counted {
        data_start: end + 3,
        size,
        synchronizing,
    }
}

/// [`literal_marker_at`] for callers that scan a buffer we built ourselves, so
/// neither an out-of-range count nor one that no `usize` can hold can be a
/// marker they own: both are ordinary text, exactly like a malformed marker.
fn literal_marker_at_usize(buf: &[u8], pos: usize) -> Option<(usize, usize, bool)> {
    match literal_marker_at(buf, pos) {
        LiteralMarker::Counted {
            data_start,
            size,
            synchronizing,
        } => Some((data_start, usize::try_from(size).ok()?, synchronizing)),
        LiteralMarker::CountOutOfRange { .. } | LiteralMarker::NotAMarker => None,
    }
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
