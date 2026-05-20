//! ID command encoder (RFC 2971).

use std::collections::HashSet;

use super::{BytesMut, LiteralMode, encode_quoted_or_literal_utf8};

/// Encode ID command (RFC 2971 Section 3.1).
///
/// Empty params produce `ID NIL` per RFC 2971 Section 3.1:
/// `id ::= "ID" SPACE id_params_list`
/// `id_params_list ::= "(" #(string SPACE nstring) ")" / nil`
///
/// Values are `nstring` per RFC 2971 Section 3.1: a `None` value is encoded as `NIL`.
///
/// RFC 2971 Section 3.3 limits are enforced:
/// - No more than 30 field-value pairs.
/// - Field strings MUST NOT exceed 30 octets.
/// - Value strings MUST NOT exceed 1024 octets.
pub(in crate::codec::encode) fn encode_id(
    buf: &mut BytesMut,
    tag: &str,
    params: &[(String, Option<String>)],
    utf8: bool,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    // RFC 2971 Section 3.3: "Implementations MUST NOT send more than 30
    // field-value pairs."
    if params.len() > 30 {
        return Err(crate::Error::Protocol(format!(
            "ID command has {} field-value pairs, but RFC 2971 Section 3.3 \
             allows at most 30",
            params.len()
        )));
    }

    for (key, value) in params {
        // RFC 2971 Section 3.3: "Field strings MUST NOT be longer than
        // 30 octets."
        if key.len() > 30 {
            return Err(crate::Error::Protocol(format!(
                "ID field name is {} octets, but RFC 2971 Section 3.3 \
                 allows at most 30",
                key.len()
            )));
        }
        // RFC 2971 Section 3.3: "Value strings MUST NOT be longer than
        // 1024 octets."
        if let Some(v) = value
            && v.len() > 1024
        {
            return Err(crate::Error::Protocol(format!(
                "ID value is {} octets, but RFC 2971 Section 3.3 \
                 allows at most 1024",
                v.len()
            )));
        }
    }

    // RFC 2971 Section 3.3: field names are case-insensitive and
    // "Implementations MUST NOT send the same field name more than once."
    let mut seen_keys = HashSet::with_capacity(params.len());
    for (key, _) in params {
        let normalized = key.to_ascii_lowercase();
        if !seen_keys.insert(normalized) {
            return Err(crate::Error::Protocol(format!(
                "ID command repeats the same field name more than once: {key} \
                 (RFC 2971 Section 3.3)"
            )));
        }
    }

    buf.extend_from_slice(tag.as_bytes());
    if params.is_empty() {
        // RFC 2971 Section 3.1: NIL means "no data to send".
        buf.extend_from_slice(b" ID NIL\r\n");
    } else {
        buf.extend_from_slice(b" ID (");
        for (i, (key, value)) in params.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b" ");
            }
            // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
            encode_quoted_or_literal_utf8(buf, key.as_bytes(), utf8, literal_mode);
            buf.extend_from_slice(b" ");
            // RFC 2971 Section 3.1: values are nstring. None encodes as NIL.
            match value {
                Some(v) => encode_quoted_or_literal_utf8(buf, v.as_bytes(), utf8, literal_mode),
                None => buf.extend_from_slice(b"NIL"),
            }
        }
        buf.extend_from_slice(b")\r\n");
    }
    Ok(())
}
