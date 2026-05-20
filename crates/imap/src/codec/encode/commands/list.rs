//! LIST-family command encoders.

use super::{
    BytesMut, LiteralMode, encode_mailbox_str, encode_quoted_or_literal_utf8,
    normalize_status_items_body, validate_no_crlf,
};
use crate::types::MailboxAttribute;

/// Encode LIST with STATUS return option (RFC 5819 Section 2).
///
/// Format: `LIST <reference> <pattern> RETURN (STATUS (<items>))`.
/// The server returns interleaved LIST and STATUS untagged responses.
pub(in crate::codec::encode) fn encode_list_status(
    buf: &mut BytesMut,
    tag: &str,
    reference: &str,
    pattern: &str,
    status_items: &str,
    utf8: bool,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    // Reject CRLF in status items to prevent command injection (RFC 3501 Section 2.2).
    validate_no_crlf(status_items, "LIST-STATUS status items")?;
    let status_items = normalize_status_items_body(status_items, "LIST-STATUS status items")?;
    // RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1: encode reference and
    // pattern with INBOX normalization and MUTF-7 when not in UTF-8 mode.
    let wire_ref = encode_mailbox_str(reference, utf8);
    let wire_pat = encode_mailbox_str(pattern, utf8);
    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" LIST ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    encode_quoted_or_literal_utf8(buf, wire_ref.as_bytes(), utf8, literal_mode);
    buf.extend_from_slice(b" ");
    encode_quoted_or_literal_utf8(buf, wire_pat.as_bytes(), utf8, literal_mode);
    buf.extend_from_slice(b" RETURN (STATUS (");
    buf.extend_from_slice(status_items.as_bytes());
    buf.extend_from_slice(b"))\r\n");
    Ok(())
}

/// Encode LIST with selection options, multiple patterns, and return options
/// (RFC 5258 Section 3 / RFC 9051 Section 6.3.9).
///
/// Wire format:
/// `tag SP LIST [SP "(" select-opts ")"] SP reference SP pattern-or-list [SP RETURN SP "(" return-opts ")"] CRLF`
///
/// `patterns` must contain at least one mailbox pattern. When multiple
/// patterns are present, the encoder emits the parenthesized pattern list
/// form from RFC 5258 Section 3.
#[allow(clippy::too_many_arguments)]
pub(in crate::codec::encode) fn encode_list_extended(
    buf: &mut BytesMut,
    tag: &str,
    selection_options: &[String],
    reference: &str,
    patterns: &[String],
    return_options: &[String],
    utf8: bool,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    if patterns.is_empty() {
        return Err(crate::Error::Protocol(
            "LIST-EXTENDED requires at least one mailbox pattern \
             (RFC 5258 Section 3 / RFC 9051 Section 6.3.9)"
                .into(),
        ));
    }

    for option in selection_options {
        validate_no_crlf(option, "LIST-EXTENDED selection option")?;
    }
    for option in return_options {
        validate_no_crlf(option, "LIST-EXTENDED return option")?;
    }
    validate_list_extended_option_syntax(selection_options, return_options)?;

    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" LIST");

    if !selection_options.is_empty() {
        buf.extend_from_slice(b" (");
        for (index, option) in selection_options.iter().enumerate() {
            if index > 0 {
                buf.extend_from_slice(b" ");
            }
            buf.extend_from_slice(option.trim().as_bytes());
        }
        buf.extend_from_slice(b")");
    }

    // RFC 3501 Section 5.1.3 / RFC 9051 Section 5.1: encode reference and
    // patterns with INBOX normalization and MUTF-7 when not in UTF-8 mode.
    let wire_ref = encode_mailbox_str(reference, utf8);
    buf.extend_from_slice(b" ");
    encode_quoted_or_literal_utf8(buf, wire_ref.as_bytes(), utf8, literal_mode);
    buf.extend_from_slice(b" ");

    if patterns.len() == 1 {
        let wire_pat = encode_mailbox_str(&patterns[0], utf8);
        encode_quoted_or_literal_utf8(buf, wire_pat.as_bytes(), utf8, literal_mode);
    } else {
        buf.extend_from_slice(b"(");
        for (index, pattern) in patterns.iter().enumerate() {
            if index > 0 {
                buf.extend_from_slice(b" ");
            }
            let wire_pat = encode_mailbox_str(pattern, utf8);
            encode_quoted_or_literal_utf8(buf, wire_pat.as_bytes(), utf8, literal_mode);
        }
        buf.extend_from_slice(b")");
    }

    if !return_options.is_empty() {
        buf.extend_from_slice(b" RETURN (");
        for (index, option) in return_options.iter().enumerate() {
            if index > 0 {
                buf.extend_from_slice(b" ");
            }
            buf.extend_from_slice(option.trim().as_bytes());
        }
        buf.extend_from_slice(b")");
    }

    buf.extend_from_slice(b"\r\n");
    Ok(())
}

/// Validate LIST-EXTENDED selection/return option syntax before encoding.
///
/// RFC 5258 Section 3 / RFC 9051 Section 6.3.9 require `RECURSIVEMATCH` to
/// appear only alongside another non-`REMOTE` selection option. RFC 5819
/// Section 4 / RFC 9051 Section 7 reserve the exact `STATUS SP "("
/// status-att *(SP status-att) ")"` return-option form.
fn validate_list_extended_option_syntax(
    selection_options: &[String],
    return_options: &[String],
) -> Result<(), crate::Error> {
    let has_recursivematch = selection_options
        .iter()
        .any(|option| option.trim().eq_ignore_ascii_case("RECURSIVEMATCH"));
    if has_recursivematch
        && !selection_options.iter().any(|option| {
            let trimmed = option.trim();
            !trimmed.is_empty()
                && !trimmed.eq_ignore_ascii_case("RECURSIVEMATCH")
                && !trimmed.eq_ignore_ascii_case("REMOTE")
        })
    {
        return Err(crate::Error::Protocol(
            "LIST-EXTENDED selection option RECURSIVEMATCH requires another \
             non-REMOTE selection option (RFC 5258 Section 3 / RFC 9051 Section 6.3.9)"
                .into(),
        ));
    }

    for option in selection_options {
        if option.trim().is_empty() {
            return Err(crate::Error::Protocol(
                "LIST-EXTENDED selection options must not be empty \
                 (RFC 5258 Section 3 / RFC 9051 Section 6.3.9)"
                    .into(),
            ));
        }
    }

    for option in return_options {
        let trimmed = option.trim();
        if trimmed.is_empty() {
            return Err(crate::Error::Protocol(
                "LIST-EXTENDED return options must not be empty \
                 (RFC 5258 Section 3 / RFC 9051 Section 6.3.9)"
                    .into(),
            ));
        }

        if let Some(status_items) = list_status_return_option_items(trimmed).transpose()? {
            // RFC 5819 Section 4 reuses STATUS's flat `status-att` list, so
            // nested or empty lists remain invalid inside RETURN (STATUS ...).
            let wrapped = format!("({status_items})");
            let _ = normalize_status_items_body(&wrapped, "LIST-EXTENDED STATUS return option")?;
        }
    }

    Ok(())
}

/// RFC 5819 Section 4 / RFC 9051 Section 7: only the reserved
/// `STATUS SP "(" status-att *(SP status-att) ")"` form is LIST-STATUS.
/// Longer atoms such as `STATUSX` remain generic RFC 5258 option extensions.
fn list_status_return_option_items(option: &str) -> Option<Result<&str, crate::Error>> {
    if !option
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("STATUS"))
    {
        return None;
    }

    match option.as_bytes().get(6).copied() {
        Some(next) if next != b' ' && !next.is_ascii_whitespace() && next != b'(' => return None,
        _ => {}
    }

    Some(if let Some(suffix) = option[6..].strip_prefix(" (") {
        if suffix.ends_with(')') && suffix.len() >= 2 {
            Ok(&suffix[1..suffix.len() - 1])
        } else {
            Err(crate::Error::Protocol(
                "LIST-EXTENDED STATUS return option must be STATUS (<items>) \
                 per RFC 5819 Section 4 / RFC 9051 Section 7"
                    .into(),
            ))
        }
    } else {
        Err(crate::Error::Protocol(
            "LIST-EXTENDED STATUS return option must be STATUS (<items>) \
             per RFC 5819 Section 4 / RFC 9051 Section 7"
                .into(),
        ))
    })
}

/// Encode CREATE with USE special-use attributes (RFC 6154 Section 3).
///
/// RFC 6154 Section 3 / Section 6 ABNF:
/// `create-param =/ "USE" SP "(" [use-attr *(SP use-attr)] ")"`
/// where `use-attr = "\All" / "\Archive" / "\Drafts" / "\Flagged" /
///                    "\Junk" / "\Sent" / "\Trash" / use-attr-ext`
///
/// Wire format: `tag CREATE mailbox (USE (\Attr1 \Attr2))\r\n`
pub(in crate::codec::encode) fn encode_create_special_use(
    buf: &mut BytesMut,
    tag: &str,
    mailbox: &str,
    special_use: &[MailboxAttribute],
    utf8: bool,
    literal_mode: LiteralMode,
) {
    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" CREATE ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    encode_quoted_or_literal_utf8(buf, mailbox.as_bytes(), utf8, literal_mode);
    buf.extend_from_slice(b" (USE (");
    for (i, attr) in special_use.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b" ");
        }
        buf.extend_from_slice(attr.as_imap_str().as_bytes());
    }
    buf.extend_from_slice(b"))\r\n");
}
