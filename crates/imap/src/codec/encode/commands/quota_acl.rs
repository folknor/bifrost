//! QUOTA and ACL command encoders.

use super::{BytesMut, LiteralMode, encode_quoted_or_literal_utf8, validate_atom};

/// Encode SETQUOTA command (RFC 2087 Section 4.1).
///
/// Format: `SETQUOTA "<root>" (<resource> <limit> ...)`.
/// RFC 2087 Section 4.1:
/// `setquota = "SETQUOTA" SP astring SP setquota_list`
/// `setquota_list = "(" 0#setquota_resource ")"`
/// `setquota_resource = atom SP number`
///
/// `number` is defined as `1*DIGIT` in RFC 3501 Section 9, constrained to u32.
/// Returns an error if any resource name is not a valid atom or any limit exceeds `u32::MAX`.
pub(in crate::codec::encode) fn encode_set_quota(
    buf: &mut BytesMut,
    tag: &str,
    root: &str,
    resources: &[(String, u64)],
    utf8: bool,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    // RFC 2087 Section 4.1: setquota_resource = atom SP number
    // RFC 3501 Section 9: number = 1*DIGIT (u32 range)
    for (resource, limit) in resources {
        // RFC 2087 Section 4.1: resource name must be an atom
        validate_atom(resource, "SETQUOTA resource name")?;
        if *limit > u64::from(u32::MAX) {
            return Err(crate::Error::Protocol(format!(
                "SETQUOTA resource limit {limit} for \"{resource}\" exceeds u32::MAX \
                 (RFC 2087 Section 4.1: number is constrained to 32 bits per RFC 3501 Section 9)"
            )));
        }
    }
    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" SETQUOTA ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    encode_quoted_or_literal_utf8(buf, root.as_bytes(), utf8, literal_mode);
    buf.extend_from_slice(b" (");
    for (i, (resource, limit)) in resources.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b" ");
        }
        buf.extend_from_slice(resource.as_bytes());
        buf.extend_from_slice(b" ");
        buf.extend_from_slice(limit.to_string().as_bytes());
    }
    buf.extend_from_slice(b")\r\n");
    Ok(())
}

/// Encode SETACL command (RFC 4314 Section 3.1).
///
/// Format: `SETACL <mailbox> <identifier> <rights>`.
pub(in crate::codec::encode) fn encode_set_acl(
    buf: &mut BytesMut,
    tag: &str,
    mailbox: &str,
    identifier: &str,
    rights: &str,
    utf8: bool,
    literal_mode: LiteralMode,
) {
    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" SETACL ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    encode_quoted_or_literal_utf8(buf, mailbox.as_bytes(), utf8, literal_mode);
    buf.extend_from_slice(b" ");
    encode_quoted_or_literal_utf8(buf, identifier.as_bytes(), utf8, literal_mode);
    buf.extend_from_slice(b" ");
    encode_quoted_or_literal_utf8(buf, rights.as_bytes(), utf8, literal_mode);
    buf.extend_from_slice(b"\r\n");
}
