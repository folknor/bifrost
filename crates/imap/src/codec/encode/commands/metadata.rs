//! METADATA command encoders (RFC 5464).

use super::super::{string_helpers::encode_metadata_value, validate_metadata_entry_name};
use super::{BytesMut, LiteralMode, encode_quoted_or_literal_utf8};

/// Encode GETMETADATA command (RFC 5464 Section 4.2).
///
/// Single entry: `GETMETADATA [options] "<mailbox>" <entry>`
/// Multiple entries: `GETMETADATA [options] "<mailbox>" (<entry1> <entry2> ...)`
///
/// Returns an error if `entries` is empty or if `depth` is not a valid value.
#[allow(clippy::too_many_arguments)]
pub(in crate::codec::encode) fn encode_getmetadata(
    buf: &mut BytesMut,
    tag: &str,
    mailbox: &str,
    entries: &[String],
    max_size: Option<u64>,
    depth: Option<&str>,
    utf8: bool,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    // RFC 5464 Section 5: `maxsize-opt = "MAXSIZE" SP number`
    // RFC 3501 Section 9: `number = 1*DIGIT`, unsigned 32-bit integer.
    if let Some(n) = max_size
        && n > u64::from(u32::MAX)
    {
        return Err(crate::Error::Protocol(format!(
            "GETMETADATA MAXSIZE must fit in number (u32) per RFC 5464 Section 5 / RFC 3501 Section 9, got {n}"
        )));
    }

    // RFC 5464 Section 4.2 ABNF: `entries = entry / "(" entry *(SP entry) ")"`.
    if entries.is_empty() {
        return Err(crate::Error::Protocol(
            "GETMETADATA requires at least one entry (RFC 5464 Section 4.2)".into(),
        ));
    }

    for entry in entries {
        validate_metadata_entry_name(entry, "GETMETADATA entry name")?;
    }

    // RFC 5464 Section 4.2.2: `scope-opt = "DEPTH" SP ("0" / "1" / "infinity")`
    if let Some(d) = depth
        && d != "0"
        && d != "1"
        && d != "infinity"
    {
        return Err(crate::Error::Protocol(format!(
            "GETMETADATA DEPTH must be \"0\", \"1\", or \"infinity\" \
             (RFC 5464 Section 4.2.2), got: {d:?}"
        )));
    }

    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" GETMETADATA");

    // RFC 5464 Section 5 ABNF:
    // getmetadata = "GETMETADATA" [SP getmetadata-options] SP mailbox SP entries
    // Verified errata 2785 / 2786 correct the examples in Sections 4.2.1-4.2.2.
    if max_size.is_some() || depth.is_some() {
        buf.extend_from_slice(b" (");
        let first_opt = if let Some(n) = max_size {
            buf.extend_from_slice(b"MAXSIZE ");
            buf.extend_from_slice(n.to_string().as_bytes());
            false
        } else {
            true
        };
        if let Some(d) = depth {
            if !first_opt {
                buf.extend_from_slice(b" ");
            }
            buf.extend_from_slice(b"DEPTH ");
            buf.extend_from_slice(d.as_bytes());
        }
        buf.extend_from_slice(b")");
    }

    buf.extend_from_slice(b" ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    encode_quoted_or_literal_utf8(buf, mailbox.as_bytes(), utf8, literal_mode);

    buf.extend_from_slice(b" ");
    if entries.len() == 1 {
        // Single entry uses no parentheses per RFC 5464 Section 4.2.
        encode_quoted_or_literal_utf8(buf, entries[0].as_bytes(), utf8, literal_mode);
    } else {
        buf.extend_from_slice(b"(");
        for (i, entry) in entries.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b" ");
            }
            encode_quoted_or_literal_utf8(buf, entry.as_bytes(), utf8, literal_mode);
        }
        buf.extend_from_slice(b")");
    }
    buf.extend_from_slice(b"\r\n");
    Ok(())
}

/// Encode SETMETADATA command (RFC 5464 Section 4.3).
///
/// Format: `SETMETADATA <mailbox> (<entry> <value> ...)`.
/// A `None` value is encoded as `NIL` to delete the entry.
/// RFC 5464 Section 5: `value = nstring / literal8`; values are raw bytes.
pub(in crate::codec::encode) fn encode_setmetadata(
    buf: &mut BytesMut,
    tag: &str,
    mailbox: &str,
    entries: &[(String, Option<Vec<u8>>)],
    utf8: bool,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    // RFC 5464 Section 5 ABNF: `entry-values = "(" entry *(SP entry) ")"`.
    if entries.is_empty() {
        return Err(crate::Error::Protocol(
            "SETMETADATA requires at least one entry (RFC 5464 Section 5)".into(),
        ));
    }

    for (name, _) in entries {
        validate_metadata_entry_name(name, "SETMETADATA entry name")?;
    }

    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" SETMETADATA ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    encode_quoted_or_literal_utf8(buf, mailbox.as_bytes(), utf8, literal_mode);
    buf.extend_from_slice(b" (");
    for (i, (name, value)) in entries.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b" ");
        }
        encode_quoted_or_literal_utf8(buf, name.as_bytes(), utf8, literal_mode);
        buf.extend_from_slice(b" ");
        match value {
            // RFC 5464 Section 5: value = nstring / literal8. `nstring`
            // includes classic literals via `string`, so only NUL-bearing data
            // requires literal8.
            Some(v) => encode_metadata_value(buf, v, literal_mode),
            None => buf.extend_from_slice(b"NIL"),
        }
    }
    buf.extend_from_slice(b")\r\n");
    Ok(())
}
