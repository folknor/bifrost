//! Individual IMAP command encoders.
//!
//! Each function encodes a specific IMAP command into wire bytes. These are
//! called from the main dispatch in [`super::encode_command_to_buf`].
//!
//! Command syntax is defined in RFC 3501 Section 6 / RFC 9051 Section 6.

use super::{
    BytesMut, HashSet, LITERAL_MINUS_MAX, LiteralMode, MailboxAttribute, QresyncParams,
    encode_changedsince_modifier, encode_metadata_value, encode_quoted_or_literal,
    encode_quoted_or_literal_utf8, search_criteria_starts_with_charset, validate_and_filter_flags,
    validate_append_datetime, validate_atom, validate_login_credential_ascii,
    validate_metadata_entry_name, validate_mod_sequence_value, validate_mod_sequence_valzer,
    validate_no_crlf, validate_sasl_initial_response, validate_search_criteria_crlf,
    validate_sort_thread_charset,
};
use crate::types::notify::{MailboxFilter, NotifyEvent, NotifySetParams};
use crate::types::validated::MailboxName;

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

/// Encode a simple command with no arguments (e.g. `NOOP`, `LOGOUT`).
///
/// Used for commands whose ABNF is just the command name followed by CRLF
/// (RFC 3501 Section 6 / RFC 9051 Section 6).
pub(super) fn encode_simple(buf: &mut BytesMut, tag: &str, command: &str) {
    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(command.as_bytes());
    buf.extend_from_slice(b"\r\n");
}

/// Encode LOGIN command (RFC 3501 Section 6.2.3 / RFC 9051 Section 6.2.3).
///
/// RFC 6855 Section 5 does not extend `LOGIN` to permit UTF-8 credentials.
/// Clients needing non-ASCII usernames or passwords MUST use `AUTHENTICATE`.
pub(super) fn encode_login(
    buf: &mut BytesMut,
    tag: &str,
    user: &str,
    pass: &str,
    utf8: bool,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    validate_login_credential_ascii(user, "user")?;
    validate_login_credential_ascii(pass, "password")?;
    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" LOGIN ");
    // RFC 3501 Section 6.2.3 / RFC 9051 Section 6.2.3: LOGIN arguments are strings.
    encode_quoted_or_literal_utf8(buf, user.as_bytes(), utf8, literal_mode);
    buf.extend_from_slice(b" ");
    encode_quoted_or_literal_utf8(buf, pass.as_bytes(), utf8, literal_mode);
    buf.extend_from_slice(b"\r\n");
    Ok(())
}

/// Encode AUTHENTICATE command (RFC 3501 Section 6.2.2 / RFC 9051 Section 6.2.2).
///
/// When `initial_response` is present (SASL-IR, RFC 4959), the value is
/// validated to contain only base64 characters (`[A-Za-z0-9+/=]`) and no
/// CRLF bytes (RFC 3501 Section 2.2) before being written to the buffer.
pub(super) fn encode_authenticate(
    buf: &mut BytesMut,
    tag: &str,
    mechanism: &str,
    initial_response: Option<&str>,
) -> Result<(), crate::Error> {
    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" AUTHENTICATE ");
    buf.extend_from_slice(mechanism.as_bytes());
    if let Some(ir) = initial_response {
        // Validate before writing (RFC 4959 Section 3, RFC 3501 Section 2.2).
        validate_sasl_initial_response(ir)?;
        // SASL-IR (RFC 4959): send initial response on the same line.
        // RFC 4959 Section 3: empty initial response MUST be sent as `=`.
        if ir.is_empty() {
            buf.extend_from_slice(b" =");
        } else {
            buf.extend_from_slice(b" ");
            buf.extend_from_slice(ir.as_bytes());
        }
    }
    buf.extend_from_slice(b"\r\n");
    Ok(())
}

/// Encode STATUS command (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11).
pub(super) fn encode_status(
    buf: &mut BytesMut,
    tag: &str,
    mailbox: &str,
    items: &str,
    utf8: bool,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    // Reject CRLF in status items to prevent command injection (RFC 3501 Section 2.2).
    validate_no_crlf(items, "STATUS items")?;
    let status_items = normalize_status_items(items, "STATUS items")?;
    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" STATUS ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    encode_quoted_or_literal_utf8(buf, mailbox.as_bytes(), utf8, literal_mode);
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(status_items.as_bytes());
    buf.extend_from_slice(b"\r\n");
    Ok(())
}

/// Encode a SEARCH-family command (RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4).
///
/// Shared by all SEARCH variants. `cmd` is the command prefix
/// (e.g. `"SEARCH"`, `"UID SEARCH"`, `"SEARCH RETURN (SAVE)"`).
///
/// When `return_opts` is `Some`, emits `RETURN (<opts>)` between the command
/// and criteria (RFC 4731 Section 3.2 / Section 4 ABNF).
pub(super) fn encode_search(
    buf: &mut BytesMut,
    tag: &str,
    cmd: &str,
    criteria: &str,
    return_opts: Option<&[String]>,
    utf8: bool,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    validate_search_criteria_crlf(criteria, "SEARCH criteria", literal_mode)?;
    validate_non_empty_search_criteria(criteria, cmd)?;
    // RFC 6855 Section 3: after ENABLE UTF8=ACCEPT, clients MUST NOT issue a
    // SEARCH command that contains a charset specification.
    if utf8 && search_criteria_starts_with_charset(criteria) {
        return Err(crate::Error::Protocol(format!(
            "{cmd} must not include a CHARSET specification when UTF8=ACCEPT is active \
             (RFC 6855 Section 3)"
        )));
    }
    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(cmd.as_bytes());
    if let Some(opts) = return_opts {
        // RFC 4731 Section 3.2: RETURN (opt1 opt2 ...)
        for opt in opts {
            // Reject CRLF in each return option to prevent command injection
            // (RFC 3501 Section 2.2).
            validate_no_crlf(opt, "SEARCH RETURN option")?;
            // RFC 4731 Section 3.2 / RFC 4466 Section 2.6.2: each
            // search-return option is a single IMAP atom token. Outer
            // whitespace may be trimmed for interoperability, but embedded
            // whitespace would change the wire option list and must be
            // rejected rather than serialized.
            validate_atom(opt.trim(), "SEARCH RETURN option")?;
        }
        buf.extend_from_slice(b" RETURN (");
        for (i, opt) in opts.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b" ");
            }
            buf.extend_from_slice(opt.trim().as_bytes());
        }
        buf.extend_from_slice(b")");
    }
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(criteria.as_bytes());
    buf.extend_from_slice(b"\r\n");
    Ok(())
}

/// Encode FETCH command (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5).
///
/// When `changed_since` is `Some`, appends the CHANGEDSINCE modifier
/// per RFC 7162 Section 3.1.4.1:
/// `"FETCH" SP sequence-set SP fetch-att SP "(CHANGEDSINCE" SP mod-sequence-value ")"`
pub(super) fn encode_fetch(
    buf: &mut BytesMut,
    tag: &str,
    sequence_set: &str,
    items: &str,
    changed_since: Option<u64>,
) -> Result<(), crate::Error> {
    // Reject CRLF in raw parameters to prevent command injection (RFC 3501 Section 2.2).
    validate_no_crlf(sequence_set, "FETCH sequence set")?;
    validate_no_crlf(items, "FETCH items")?;
    validate_non_empty_fetch_items(items, "FETCH items")?;
    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" FETCH ");
    buf.extend_from_slice(sequence_set.as_bytes());
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(items.as_bytes());
    if let Some(modseq) = changed_since {
        encode_changedsince_modifier(buf, modseq, false)?;
    }
    buf.extend_from_slice(b"\r\n");
    Ok(())
}

/// Encode STORE / UID STORE command (RFC 3501 Section 6.4.6 / RFC 9051 Section 6.4.6).
pub(super) fn encode_store(
    buf: &mut BytesMut,
    tag: &str,
    uid: bool,
    sequence_set: &str,
    operation: crate::types::StoreOperation,
    flags: &[crate::types::Flag],
    unchanged_since: Option<u64>,
) -> Result<(), crate::Error> {
    // Reject CRLF in sequence set to prevent command injection (RFC 3501 Section 2.2).
    validate_no_crlf(sequence_set, "STORE sequence set")?;
    buf.extend_from_slice(tag.as_bytes());
    if uid {
        buf.extend_from_slice(b" UID STORE ");
    } else {
        buf.extend_from_slice(b" STORE ");
    }
    buf.extend_from_slice(sequence_set.as_bytes());

    // CONDSTORE modifier (RFC 7162 Section 3.1.3): UNCHANGEDSINCE uses mod-sequence-valzer
    if let Some(modseq) = unchanged_since {
        validate_mod_sequence_valzer(modseq, "UNCHANGEDSINCE")?;
        buf.extend_from_slice(b" (UNCHANGEDSINCE ");
        buf.extend_from_slice(modseq.to_string().as_bytes());
        buf.extend_from_slice(b")");
    }

    encode_store_flags(buf, operation, flags)
}

/// Encode a command with two arguments: one atom and one quoted-or-literal string.
///
/// Used for COPY (RFC 3501 Section 6.4.7 / RFC 9051 Section 6.4.7),
/// UID COPY (RFC 3501 Section 6.4.8 / RFC 9051 Section 6.4.9),
/// and MOVE / UID MOVE (RFC 6851 Section 3 / RFC 9051 Section 6.4.8).
#[allow(clippy::too_many_arguments)]
pub(super) fn encode_two_arg(
    buf: &mut BytesMut,
    tag: &str,
    uid: bool,
    cmd: &str,
    arg1: &str,
    arg2: &str,
    utf8: bool,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    // Reject CRLF in the unquoted first argument (typically a sequence set)
    // to prevent command injection (RFC 3501 Section 2.2).
    validate_no_crlf(arg1, &format!("{cmd} sequence set"))?;
    buf.extend_from_slice(tag.as_bytes());
    if uid {
        buf.extend_from_slice(b" UID ");
    } else {
        buf.extend_from_slice(b" ");
    }
    buf.extend_from_slice(cmd.as_bytes());
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(arg1.as_bytes());
    buf.extend_from_slice(b" ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    encode_quoted_or_literal_utf8(buf, arg2.as_bytes(), utf8, literal_mode);
    buf.extend_from_slice(b"\r\n");
    Ok(())
}

/// Encode a command with two quoted-or-literal arguments.
///
/// Format: `tag SP cmd SP quoted-or-literal(arg1) SP quoted-or-literal(arg2) CRLF`.
/// Used by LIST (RFC 3501 Section 6.3.8), LSUB (RFC 3501 Section 6.3.9),
/// and RENAME (RFC 3501 Section 6.3.5).
pub(super) fn encode_two_quoted_args(
    buf: &mut BytesMut,
    tag: &str,
    cmd: &str,
    arg1: &str,
    arg2: &str,
    utf8: bool,
    literal_mode: LiteralMode,
) {
    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(cmd.as_bytes());
    buf.extend_from_slice(b" ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    encode_quoted_or_literal_utf8(buf, arg1.as_bytes(), utf8, literal_mode);
    buf.extend_from_slice(b" ");
    encode_quoted_or_literal_utf8(buf, arg2.as_bytes(), utf8, literal_mode);
    buf.extend_from_slice(b"\r\n");
}

/// Encode LIST with STATUS return option (RFC 5819 Section 2).
///
/// Format: `LIST <reference> <pattern> RETURN (STATUS (<items>))`.
/// The server returns interleaved LIST and STATUS untagged responses.
pub(super) fn encode_list_status(
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

/// Normalize STATUS data items to the on-wire `(<items>)` form.
///
/// RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11 require the STATUS
/// command to carry a parenthesized `status-att-list`. For ergonomics, the
/// encoder accepts either a raw item list (`"MESSAGES UNSEEN"`) or an already
/// parenthesized list (`"(MESSAGES UNSEEN)"`) and canonicalizes both forms to
/// the required wire syntax.
fn normalize_status_items(items: &str, context: &str) -> Result<String, crate::Error> {
    let body = normalize_status_items_body(items, context)?;
    Ok(format!("({body})"))
}

/// Normalize the body of a STATUS data item list.
///
/// Accepts either a raw space-separated item list or a parenthesized
/// `status-att-list` and returns only the inner items. The list must contain
/// at least one atom (RFC 3501 Section 6.3.10 / RFC 5819 Section 2).
fn normalize_status_items_body(items: &str, context: &str) -> Result<String, crate::Error> {
    let trimmed = items.trim();
    if trimmed.is_empty() {
        return Err(crate::Error::Protocol(format!(
            "{context} must contain at least one status data item \
             (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
        )));
    }

    let body = match (trimmed.strip_prefix('('), trimmed.strip_suffix(')')) {
        (Some(without_open), Some(_)) => {
            let inner = without_open
                .strip_suffix(')')
                .ok_or_else(|| {
                    crate::Error::Protocol(format!(
                        "{context} must be a single parenthesized status-att-list \
                         (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                    ))
                })?
                .trim();
            if inner.is_empty() {
                return Err(crate::Error::Protocol(format!(
                    "{context} must contain at least one status data item \
                     (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                )));
            }
            inner
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(crate::Error::Protocol(format!(
                "{context} must use balanced parentheses for status-att-list \
                 syntax (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
            )));
        }
        (None, None) => trimmed,
    };

    if body.contains('(') || body.contains(')') {
        return Err(crate::Error::Protocol(format!(
            "{context} must be a flat status-att-list without nested parentheses \
             (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
        )));
    }

    Ok(body.to_owned())
}

/// Validate that a SEARCH-family command has at least one search criterion.
///
/// RFC 3501 Section 6.4.4 defines SEARCH as requiring one or more search
/// keys. RFC 5256 Sections 2-3 inherit the same `search-criteria` production
/// for SORT and THREAD.
fn validate_non_empty_search_criteria(criteria: &str, context: &str) -> Result<(), crate::Error> {
    if criteria.trim().is_empty() {
        return Err(crate::Error::Protocol(format!(
            "{context} must contain at least one search criterion \
             (RFC 3501 Section 6.4.4 / RFC 5256 Section 6)"
        )));
    }
    Ok(())
}

/// Validate that a FETCH-family command has at least one message data item
/// and structurally balanced `fetch-att` syntax.
///
/// RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5 require FETCH and UID
/// FETCH to include either a macro or a non-empty parenthesized `fetch-att`
/// list. Individual `fetch-att` values can also contain nested delimiters,
/// such as `BODY[HEADER.FIELDS (DATE FROM)]`, partial specifiers
/// `<start.count>`, and extension modifiers like `PREVIEW (LAZY)`, so the
/// encoder validates balanced quotes and delimiters before writing the
/// command to the wire.
/// Validate a single `fetch-att` value for NOTIFY `MessageNew`.
///
/// RFC 5465 Section 8 ABNF:
/// ```text
/// message-event = ("MessageNew" [SP "(" fetch-att *(SP fetch-att) ")"])
/// ```
///
/// Each element of the `fetch_attrs` vector must be a single `fetch-att`,
/// not a parenthesized list of multiple items. The encoder wraps the
/// individual values in `(...)` with spaces. Accepting a parenthesized
/// list like `(UID FLAGS)` would produce `MessageNew ((UID FLAGS))` which
/// is malformed per the ABNF.
fn validate_single_fetch_att(attr: &str) -> Result<(), crate::Error> {
    let trimmed = attr.trim();
    if trimmed.is_empty() {
        return Err(crate::Error::Protocol(
            "NOTIFY MessageNew fetch-att must not be empty \
             (RFC 5465 Section 8)"
                .into(),
        ));
    }
    // A single fetch-att never starts with '('  -  that's a fetch-att list.
    // Individual fetch-atts like BODY[HEADER.FIELDS (From)] contain
    // parentheses only after brackets, not at the top level.
    if trimmed.starts_with('(') {
        return Err(crate::Error::Protocol(
            "NOTIFY MessageNew fetch-att must be a single item, not a \
             parenthesized list  -  use separate vector elements for each \
             fetch-att (RFC 5465 Section 8)"
                .into(),
        ));
    }
    // Delegate balanced-delimiter validation to the general helper.
    validate_non_empty_fetch_items(trimmed, "NOTIFY fetch-att")
}

fn validate_non_empty_fetch_items(items: &str, context: &str) -> Result<(), crate::Error> {
    let trimmed = items.trim();
    if trimmed.is_empty() {
        return Err(crate::Error::Protocol(format!(
            "{context} must contain at least one message data item \
             (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5)"
        )));
    }

    if let Some(inner) = trimmed.strip_prefix('(') {
        let inner = inner.strip_suffix(')').ok_or_else(|| {
            crate::Error::Protocol(format!(
                "{context} must use balanced parentheses for fetch-att list \
                 syntax (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5)"
            ))
        })?;
        if inner.trim().is_empty() {
            return Err(crate::Error::Protocol(format!(
                "{context} must contain at least one message data item \
                 (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5)"
            )));
        }
    }

    let mut in_quote = false;
    let mut escaped = false;
    let mut bracket_depth = 0u32;
    let mut angle_depth = 0u32;
    let mut paren_depth = 0u32;

    for ch in trimmed.chars() {
        if in_quote {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_quote = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '"' => in_quote = true,
            '[' => bracket_depth += 1,
            ']' => {
                if bracket_depth == 0 {
                    return Err(crate::Error::Protocol(format!(
                        "{context} must use balanced quotes and delimiters for fetch-att \
                         syntax (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5)"
                    )));
                }
                bracket_depth -= 1;
            }
            '<' if bracket_depth == 0 => angle_depth += 1,
            '>' if bracket_depth == 0 => {
                if angle_depth == 0 {
                    return Err(crate::Error::Protocol(format!(
                        "{context} must use balanced quotes and delimiters for fetch-att \
                         syntax (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5)"
                    )));
                }
                angle_depth -= 1;
            }
            '(' if bracket_depth == 0 && angle_depth == 0 => paren_depth += 1,
            ')' if bracket_depth == 0 && angle_depth == 0 => {
                if paren_depth == 0 {
                    return Err(crate::Error::Protocol(format!(
                        "{context} must use balanced quotes and delimiters for fetch-att \
                         syntax (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5)"
                    )));
                }
                paren_depth -= 1;
            }
            _ => {}
        }
    }

    if in_quote || bracket_depth != 0 || angle_depth != 0 || paren_depth != 0 {
        return Err(crate::Error::Protocol(format!(
            "{context} must use balanced quotes and delimiters for fetch-att \
             syntax (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5)"
        )));
    }

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
pub(super) fn encode_list_extended(
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
pub(super) fn encode_create_special_use(
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

/// Encode an arbitrary mailbox name string for the wire.
///
/// INBOX is case-normalized per RFC 3501 Section 5.1.
/// Non-ASCII characters are MUTF-7 encoded when `utf8` is false,
/// per RFC 3501 Section 5.1.3.
///
/// Used for LIST/LSUB reference and pattern arguments, NOTIFY filter
/// mailboxes, and any other site that carries a decoded mailbox name
/// as `&str` rather than [`MailboxName`].
pub(crate) fn encode_mailbox_str(name: &str, utf8: bool) -> String {
    // RFC 3501 Section 5.1: INBOX is case-insensitive.
    if name.eq_ignore_ascii_case("INBOX") {
        return "INBOX".to_owned();
    }
    // RFC 3501 Section 5.1 / 5.1.1: check for INBOX as hierarchical prefix.
    // The exact special name is only the case-insensitive "INBOX"; child
    // mailboxes use a single hierarchy separator after that token. Restrict
    // the heuristic to ASCII separator characters so names like "Inboxé"
    // are preserved as distinct mailboxes.
    if name.len() > 5 && name.is_char_boundary(5) {
        let prefix = &name[..5];
        let sep = name[5..].chars().next();
        if prefix.eq_ignore_ascii_case("INBOX")
            && sep.is_some_and(|ch| ch.is_ascii() && !ch.is_ascii_alphanumeric())
        {
            let child = &name[5..];
            let encoded_child = if utf8 {
                child.to_owned()
            } else {
                crate::codec::utf7::encode_utf7(child)
            };
            return format!("INBOX{encoded_child}");
        }
    }
    if utf8 {
        name.to_owned()
    } else {
        crate::codec::utf7::encode_utf7(name)
    }
}

/// Encode a [`MailboxName`] for the wire.
///
/// Delegates to [`encode_mailbox_str`] on the inner string.
pub(super) fn encode_mailbox_name(name: &MailboxName, utf8: bool) -> String {
    encode_mailbox_str(name.as_str(), utf8)
}

/// Encode a command that takes a single mailbox argument as a quoted-or-literal string.
///
/// Used for SELECT, EXAMINE, CREATE, DELETE, SUBSCRIBE, UNSUBSCRIBE
/// (RFC 3501 Sections 6.3.1-6.3.7 / RFC 9051 Sections 6.3.1-6.3.7).
pub(super) fn encode_mailbox_cmd(
    buf: &mut BytesMut,
    tag: &str,
    cmd: &str,
    mailbox: &str,
    utf8: bool,
    literal_mode: LiteralMode,
) {
    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(cmd.as_bytes());
    buf.extend_from_slice(b" ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    encode_quoted_or_literal_utf8(buf, mailbox.as_bytes(), utf8, literal_mode);
    buf.extend_from_slice(b"\r\n");
}

// Known-sequence-set validation has moved to `SequenceSet::new_known()` in
// `crate::types::validated` (RFC 7162 Section 3.2.5.2).

/// Encode SELECT or EXAMINE with optional CONDSTORE/QRESYNC parameters.
///
/// Without parameters: `tag SELECT mailbox\r\n` (RFC 3501 Section 6.3.1).
/// With CONDSTORE: `tag SELECT mailbox (CONDSTORE)\r\n` (RFC 7162 Section 3.1.1).
/// With QRESYNC: `tag SELECT mailbox (QRESYNC (uidvalidity modseq [known-uids [seq-match-data]]))\r\n`
/// (RFC 7162 Section 3.2.5.2).
#[allow(clippy::too_many_arguments)]
pub(super) fn encode_select_or_examine(
    buf: &mut BytesMut,
    tag: &str,
    cmd: &str,
    mailbox: &str,
    condstore: bool,
    qresync: Option<&QresyncParams>,
    utf8: bool,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(cmd.as_bytes());
    buf.extend_from_slice(b" ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    encode_quoted_or_literal_utf8(buf, mailbox.as_bytes(), utf8, literal_mode);

    if condstore && qresync.is_none() {
        // RFC 7162 Section 3.1.1: SELECT mailbox (CONDSTORE)
        buf.extend_from_slice(b" (CONDSTORE)");
    }

    if let Some(params) = qresync {
        // RFC 7162 Section 3.2.5.2:
        // SELECT mailbox (QRESYNC (uidvalidity modseq [known-uids [seq-match-data]]))
        // seq-match-data = "(" known-sequence-set SP known-uid-set ")"
        // RFC 3501 Section 9: uidvalidity = nz-number (must be non-zero).
        if params.uid_validity == 0 {
            return Err(crate::Error::Protocol(
                "QRESYNC uid_validity must be non-zero (nz-number per RFC 3501 Section 9)".into(),
            ));
        }
        // mod_seq is mod-sequence-value per RFC 7162 Section 7 (>= 1, <= i64::MAX)
        validate_mod_sequence_value(params.mod_seq, "QRESYNC mod_seq")?;
        buf.extend_from_slice(b" (QRESYNC (");
        buf.extend_from_slice(params.uid_validity.to_string().as_bytes());
        buf.extend_from_slice(b" ");
        buf.extend_from_slice(params.mod_seq.to_string().as_bytes());
        if let Some(known_uids) = &params.known_uids {
            // Reject CRLF to prevent command injection (RFC 3501 Section 2.2).
            validate_no_crlf(known_uids, "QRESYNC known-uids")?;
            // RFC 7162 Section 3.2.5.2: "*" and "$" are not allowed in known-uids.
            // Validate via SequenceSet::new_known() which enforces this.
            crate::types::SequenceSet::new_known(known_uids.as_str())?;
            buf.extend_from_slice(b" ");
            buf.extend_from_slice(known_uids.as_bytes());
        }
        if let Some((seq_set, uid_set)) = &params.seq_match_data {
            // RFC 7162 Section 7: The ABNF places [SP known-uids] and
            // [SP seq-match-data] at the same nesting level, but
            // seq-match-data is semantically meaningless without known-uids
            // (it maps message numbers to UIDs), so we require it.
            if params.known_uids.is_none() {
                return Err(crate::Error::Protocol(
                    "seq-match-data requires known-uids (RFC 7162 Section 3.2.5.2)".into(),
                ));
            }
            // Reject CRLF to prevent command injection (RFC 3501 Section 2.2).
            validate_no_crlf(seq_set, "QRESYNC seq-match-data sequence set")?;
            validate_no_crlf(uid_set, "QRESYNC seq-match-data UID set")?;
            // RFC 7162 Section 3.2.5.2: "*" and "$" are not allowed in
            // known-sequence-set or known-uid-set.
            crate::types::SequenceSet::new_known(seq_set.as_str())?;
            crate::types::SequenceSet::new_known(uid_set.as_str())?;
            buf.extend_from_slice(b" (");
            buf.extend_from_slice(seq_set.as_bytes());
            buf.extend_from_slice(b" ");
            buf.extend_from_slice(uid_set.as_bytes());
            buf.extend_from_slice(b")");
        }
        buf.extend_from_slice(b"))");
    }

    buf.extend_from_slice(b"\r\n");
    Ok(())
}

/// Encode UID FETCH command (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5).
///
/// When `changed_since` is `Some`, appends the CHANGEDSINCE modifier
/// per RFC 7162 Section 3.1.4.1.
///
/// When `vanished` is `true`, appends the VANISHED modifier to the
/// fetch modifier list per RFC 7162 Section 3.2.6.  VANISHED requires
/// `changed_since` to be set (the modifier list must include CHANGEDSINCE).
pub(super) fn encode_uid_fetch(
    buf: &mut BytesMut,
    tag: &str,
    sequence_set: &str,
    items: &str,
    changed_since: Option<u64>,
    vanished: bool,
) -> Result<(), crate::Error> {
    // RFC 7162 Section 3.2.6: VANISHED requires CHANGEDSINCE in the same
    // fetch modifier list.
    if vanished && changed_since.is_none() {
        return Err(crate::Error::Protocol(
            "VANISHED modifier requires CHANGEDSINCE per RFC 7162 Section 3.2.6".into(),
        ));
    }
    // Reject CRLF in raw parameters to prevent command injection (RFC 3501 Section 2.2).
    validate_no_crlf(sequence_set, "UID FETCH sequence set")?;
    validate_no_crlf(items, "UID FETCH items")?;
    validate_non_empty_fetch_items(items, "UID FETCH items")?;
    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" UID FETCH ");
    buf.extend_from_slice(sequence_set.as_bytes());
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(items.as_bytes());
    if let Some(modseq) = changed_since {
        encode_changedsince_modifier(buf, modseq, vanished)?;
    }
    buf.extend_from_slice(b"\r\n");
    Ok(())
}

/// Encode the `store-att-flags` portion shared by STORE and UID STORE
/// (RFC 3501 Section 6.4.6 / RFC 9051 Section 6.4.6).
///
/// `store-att-flags = (["+" / "-"] "FLAGS" [".SILENT"]) SP (flag-list / (flag *(SP flag)))`
///
/// RFC 3501 Section 9: STORE uses the `flag` production, which excludes
/// `\Recent` (read-only, server-set) and `\*` (permanent-flag wildcard,
/// only valid in PERMANENTFLAGS responses).
fn encode_store_flags(
    buf: &mut BytesMut,
    operation: crate::types::StoreOperation,
    flags: &[crate::types::Flag],
) -> Result<(), crate::Error> {
    buf.extend_from_slice(b" ");
    match operation {
        crate::types::StoreOperation::Add => buf.extend_from_slice(b"+FLAGS"),
        crate::types::StoreOperation::Remove => buf.extend_from_slice(b"-FLAGS"),
        crate::types::StoreOperation::Replace => buf.extend_from_slice(b"FLAGS"),
        crate::types::StoreOperation::AddSilent => buf.extend_from_slice(b"+FLAGS.SILENT"),
        crate::types::StoreOperation::RemoveSilent => buf.extend_from_slice(b"-FLAGS.SILENT"),
        crate::types::StoreOperation::ReplaceSilent => buf.extend_from_slice(b"FLAGS.SILENT"),
    }
    let valid_flags = validate_and_filter_flags(flags, "STORE")?;
    // RFC 3501 Section 9 / RFC 9051 Section 9: `flag-list = "(" [flag *(SP flag)] ")"`.
    // An empty flag-list is valid ABNF and meaningful for Replace/ReplaceSilent:
    // `FLAGS ()` clears all flags. For Add/Remove variants, adding or removing
    // zero flags is semantically useless and likely a caller bug  -  reject early.
    if valid_flags.is_empty() {
        match operation {
            crate::types::StoreOperation::Replace | crate::types::StoreOperation::ReplaceSilent => {
                // FLAGS () / FLAGS.SILENT ()  -  clear all flags.
                // Valid per RFC 3501 Section 6.4.6 flag-list ABNF.
            }
            _ => {
                return Err(crate::Error::Protocol(
                    "STORE +FLAGS/-FLAGS requires at least one flag; adding or \
                     removing zero flags is a no-op (RFC 3501 Section 6.4.6)"
                        .into(),
                ));
            }
        }
    }
    buf.extend_from_slice(b" (");
    for (i, flag) in valid_flags.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b" ");
        }
        buf.extend_from_slice(flag.as_imap_str().as_bytes());
    }
    buf.extend_from_slice(b")\r\n");
    Ok(())
}

/// Encode UID EXPUNGE command (RFC 4315 UIDPLUS Section 2.1).
pub(super) fn encode_uid_expunge(
    buf: &mut BytesMut,
    tag: &str,
    sequence_set: &str,
) -> Result<(), crate::Error> {
    // Reject CRLF in sequence set to prevent command injection (RFC 3501 Section 2.2).
    validate_no_crlf(sequence_set, "UID EXPUNGE sequence set")?;
    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" UID EXPUNGE ");
    buf.extend_from_slice(sequence_set.as_bytes());
    buf.extend_from_slice(b"\r\n");
    Ok(())
}

/// Encode GETMETADATA command (RFC 5464 Section 4.2).
///
/// Single entry: `GETMETADATA [options] "<mailbox>" <entry>`
/// Multiple entries: `GETMETADATA [options] "<mailbox>" (<entry1> <entry2> ...)`
///
/// Returns an error if `entries` is empty or if `depth` is not a valid value.
#[allow(clippy::too_many_arguments)]
pub(super) fn encode_getmetadata(
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
    // RFC 3501 Section 9: `number = 1*DIGIT`  -  unsigned 32-bit integer (0..4_294_967_295).
    if let Some(n) = max_size {
        if n > u64::from(u32::MAX) {
            return Err(crate::Error::Protocol(format!(
                "GETMETADATA MAXSIZE must fit in number (u32) per RFC 5464 Section 5 / RFC 3501 Section 9, got {n}"
            )));
        }
    }

    // RFC 5464 Section 4.2 ABNF: `entries = entry / "(" entry *(SP entry) ")"`  -  at least one.
    if entries.is_empty() {
        return Err(crate::Error::Protocol(
            "GETMETADATA requires at least one entry (RFC 5464 Section 4.2)".into(),
        ));
    }

    for entry in entries {
        validate_metadata_entry_name(entry, "GETMETADATA entry name")?;
    }

    // RFC 5464 Section 4.2.2: `scope-opt = "DEPTH" SP ("0" / "1" / "infinity")`
    if let Some(d) = depth {
        if d != "0" && d != "1" && d != "infinity" {
            return Err(crate::Error::Protocol(format!(
                "GETMETADATA DEPTH must be \"0\", \"1\", or \"infinity\" \
                 (RFC 5464 Section 4.2.2), got: {d:?}"
            )));
        }
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
        // Single entry  -  no parentheses per RFC 5464 Section 4.2
        // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
        encode_quoted_or_literal_utf8(buf, entries[0].as_bytes(), utf8, literal_mode);
    } else {
        buf.extend_from_slice(b"(");
        for (i, entry) in entries.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b" ");
            }
            // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
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
/// RFC 5464 Section 5: `value = nstring / literal8`  -  values are raw bytes.
pub(super) fn encode_setmetadata(
    buf: &mut BytesMut,
    tag: &str,
    mailbox: &str,
    entries: &[(String, Option<Vec<u8>>)],
    utf8: bool,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    // RFC 5464 Section 5 ABNF: `entry-values = "(" entry *(SP entry) ")"`  -  at least one.
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
        // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
        encode_quoted_or_literal_utf8(buf, name.as_bytes(), utf8, literal_mode);
        buf.extend_from_slice(b" ");
        match value {
            // RFC 5464 Section 5: value = nstring / literal8. `nstring`
            // includes classic literals via `string` (RFC 3501/9051 Section 9),
            // so only NUL-bearing data requires literal8.
            Some(v) => encode_metadata_value(buf, v, literal_mode),
            None => buf.extend_from_slice(b"NIL"),
        }
    }
    buf.extend_from_slice(b")\r\n");
    Ok(())
}

/// Encode a THREAD or SORT command (RFC 5256 Sections 2-3).
///
/// Shared by THREAD, UID THREAD, SORT, and UID SORT.
/// THREAD format: `<cmd> <algorithm> <charset> <criteria>`.
/// SORT format:   `<cmd> (<algorithm>) <charset> <criteria>`.
/// `parenthesize_algo` controls whether the algorithm is wrapped in `()`.
#[allow(clippy::too_many_arguments)]
pub(super) fn encode_thread_or_sort_cmd(
    buf: &mut BytesMut,
    tag: &str,
    cmd: &str,
    algorithm: &str,
    charset: &str,
    criteria: &str,
    parenthesize_algo: bool,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    if parenthesize_algo {
        // RFC 5256 Section 2: sort-criteria = "(" sort-key *(SP sort-key) ")"
        // Each sort-key is an atom (RFC 3501 Section 9).
        for key in algorithm.split(' ') {
            validate_atom(key, "SORT sort-key")?;
        }
    } else {
        // RFC 5256 Section 3: thread-alg = atom (RFC 3501 Section 9).
        validate_atom(algorithm, "THREAD algorithm")?;
    }
    // RFC 5256 Section 5: charset = atom / quoted.
    validate_sort_thread_charset(charset)?;
    validate_search_criteria_crlf(criteria, &format!("{cmd} criteria"), literal_mode)?;
    validate_non_empty_search_criteria(criteria, cmd)?;

    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(cmd.as_bytes());
    if parenthesize_algo {
        // RFC 5256 Section 2: sort-criteria = "(" sort-key *(SP sort-key) ")"
        buf.extend_from_slice(b" (");
        buf.extend_from_slice(algorithm.as_bytes());
        buf.extend_from_slice(b") ");
    } else {
        // RFC 5256 Section 3: thread-alg is unparenthesized
        buf.extend_from_slice(b" ");
        buf.extend_from_slice(algorithm.as_bytes());
        buf.extend_from_slice(b" ");
    }
    buf.extend_from_slice(charset.as_bytes());
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(criteria.as_bytes());
    buf.extend_from_slice(b"\r\n");
    Ok(())
}

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
pub(super) fn encode_id(
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
        if let Some(v) = value {
            if v.len() > 1024 {
                return Err(crate::Error::Protocol(format!(
                    "ID value is {} octets, but RFC 2971 Section 3.3 \
                     allows at most 1024",
                    v.len()
                )));
            }
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
            // RFC 2971 Section 3.1: values are nstring  -  None encodes as NIL.
            match value {
                // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
                Some(v) => encode_quoted_or_literal_utf8(buf, v.as_bytes(), utf8, literal_mode),
                None => buf.extend_from_slice(b"NIL"),
            }
        }
        buf.extend_from_slice(b")\r\n");
    }
    Ok(())
}

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
pub(super) fn encode_set_quota(
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
pub(super) fn encode_set_acl(
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

/// Encode the header portion of one message in a MULTIAPPEND command (RFC 3502 Section 3).
///
/// Writes: `[tag " APPEND " mailbox] [" (" flags ")"] [" " quoted-date] " {" size ["+" ] "}"`.
/// If `first` is `true`, the `tag APPEND mailbox` prefix is included.
/// The literal data itself is NOT included  -  the caller must send it separately
/// (because literal synchronization may require a server continuation).
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
    // RFC 9051 Section 9: literal8 = "~{" number64 "}" CRLF *OCTET  -  no `["+"]`
    // modifier, so the non-synchronizing `+` suffix must NOT be used with literal8.
    if utf8 {
        buf.extend_from_slice(b" UTF8 (~{");
    } else if literal8 {
        buf.extend_from_slice(b" ~{");
    } else {
        buf.extend_from_slice(b" {");
    }
    buf.extend_from_slice(message_len.to_string().as_bytes());
    // RFC 7888 Section 4: LITERAL+  -  non-synchronizing for any size.
    // RFC 7888 Section 5: LITERAL-  -  non-synchronizing only up to 4096 bytes.
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

// ---------------------------------------------------------------------------
// NOTIFY (RFC 5465)
// ---------------------------------------------------------------------------

/// Encode a `NOTIFY SET` command (RFC 5465 Section 3).
///
/// RFC 5465 Section 8 ABNF:
/// ```text
/// notify     = "NOTIFY" SP (notify-set / notify-none)
/// notify-set = "SET" [status-indicator] SP event-groups
/// event-groups = event-group *(SP event-group)
/// event-group  = "(" filter-mailboxes SP events ")"
/// ```
pub(super) fn encode_notify_set(
    buf: &mut BytesMut,
    tag: &str,
    params: &NotifySetParams,
    utf8: bool,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    // RFC 5465 Section 8: event-groups = event-group *(SP event-group)
    //  -  at least one event-group is required.
    if params.event_groups.is_empty() {
        return Err(crate::Error::Protocol(
            "NOTIFY SET requires at least one event group (RFC 5465 Section 8)".into(),
        ));
    }

    // RFC 5465 Section 3: "The command MUST NOT contain more than one
    // event group with a selected or selected-delayed filter."
    // This means at most one event-group whose filter is either `selected`
    // or `selected-delayed`  -  they are mutually exclusive and non-repeatable.
    {
        let selected_count = params
            .event_groups
            .iter()
            .filter(|g| {
                matches!(
                    g.filter,
                    MailboxFilter::Selected | MailboxFilter::SelectedDelayed
                )
            })
            .count();
        if selected_count > 1 {
            return Err(crate::Error::Protocol(
                "NOTIFY SET must not contain more than one event group with a \
                 selected or selected-delayed filter (RFC 5465 Section 3)"
                    .into(),
            ));
        }
    }

    for group in &params.event_groups {
        validate_notify_event_group(group)?;
    }

    buf.extend_from_slice(tag.as_bytes());
    buf.extend_from_slice(b" NOTIFY SET");

    // RFC 5465 Section 4: optional STATUS indicator.
    if params.status {
        buf.extend_from_slice(b" STATUS");
    }

    for group in &params.event_groups {
        buf.extend_from_slice(b" (");
        encode_mailbox_filter(buf, &group.filter, utf8, literal_mode)?;
        buf.extend_from_slice(b" ");
        encode_events(buf, &group.events)?;
        buf.extend_from_slice(b")");
    }

    buf.extend_from_slice(b"\r\n");
    Ok(())
}

/// Validate a single NOTIFY event group's semantic constraints
/// (RFC 5465 Sections 5.1-5.4, 6.1).
fn validate_notify_event_group(
    group: &crate::types::notify::NotifyEventGroup,
) -> Result<(), crate::Error> {
    let is_selected_filter = matches!(
        group.filter,
        MailboxFilter::Selected | MailboxFilter::SelectedDelayed
    );

    // RFC 5465 Section 6.1: selected/selected-delayed filters only accept
    // message events (MessageNew, MessageExpunge, FlagChange, AnnotationChange).
    if is_selected_filter {
        for event in &group.events {
            if !is_message_event(event) {
                return Err(crate::Error::Protocol(format!(
                    "selected/selected-delayed filters only accept message events \
                     (MessageNew, MessageExpunge, FlagChange, AnnotationChange), \
                     got {event:?} (RFC 5465 Section 6.1)"
                )));
            }
        }
    }

    // RFC 5465 Section 8 ABNF: fetch attributes in MessageNew are only
    // valid with selected/selected-delayed filters per the message-event
    // production comment.
    if !is_selected_filter {
        for event in &group.events {
            if let NotifyEvent::MessageNew { fetch_attrs } = event {
                if !fetch_attrs.is_empty() {
                    return Err(crate::Error::Protocol(
                        "MessageNew fetch attributes are only valid with \
                         selected/selected-delayed filters (RFC 5465 Section 8)"
                            .into(),
                    ));
                }
            }
        }
    }

    // RFC 5465 Section 5: event dependency constraints  -  these are stated
    // in the introductory text of Section 5 (Event Types) before the
    // per-event subsections, and reiterated in the Section 8 ABNF.
    // - MessageExpunge requires MessageNew (and vice versa).
    // - FlagChange requires both MessageNew and MessageExpunge.
    // - AnnotationChange requires both MessageNew and MessageExpunge.
    let has_new = group
        .events
        .iter()
        .any(|e| matches!(e, NotifyEvent::MessageNew { .. }));
    let has_expunge = group
        .events
        .iter()
        .any(|e| matches!(e, NotifyEvent::MessageExpunge));
    let has_flag_change = group
        .events
        .iter()
        .any(|e| matches!(e, NotifyEvent::FlagChange));
    let has_annotation_change = group
        .events
        .iter()
        .any(|e| matches!(e, NotifyEvent::AnnotationChange));

    // RFC 5465 Section 5: "If one of MessageNew or MessageExpunge is
    // specified, then both events MUST be specified."  This constraint
    // applies globally regardless of mailbox filter  -  the Section 8 ABNF
    // encodes the dependency at the grammar level.
    if has_new && !has_expunge {
        return Err(crate::Error::Protocol(
            "MessageNew requires MessageExpunge to also be specified \
             (RFC 5465 Section 5)"
                .into(),
        ));
    }
    if has_expunge && !has_new {
        return Err(crate::Error::Protocol(
            "MessageExpunge requires MessageNew to also be specified \
             (RFC 5465 Section 5)"
                .into(),
        ));
    }
    // RFC 5465 Section 5: "If the FlagChange and/or AnnotationChange events
    // are specified, MessageNew and MessageExpunge MUST also be specified
    // by the client."
    if has_flag_change && (!has_new || !has_expunge) {
        return Err(crate::Error::Protocol(
            "FlagChange requires both MessageNew and MessageExpunge to also \
             be specified (RFC 5465 Section 5)"
                .into(),
        ));
    }
    if has_annotation_change && (!has_new || !has_expunge) {
        return Err(crate::Error::Protocol(
            "AnnotationChange requires both MessageNew and MessageExpunge to \
             also be specified (RFC 5465 Section 5)"
                .into(),
        ));
    }
    Ok(())
}

/// Returns `true` if the event is a message event (RFC 5465 Sections 5.1-5.3).
///
/// Message events are `FlagChange`/`AnnotationChange` (RFC 5465 Section 5.1),
/// `MessageNew` (RFC 5465 Section 5.2), and `MessageExpunge` (RFC 5465
/// Section 5.3). Extension events (`Other(...)`) are NOT message events  -
/// RFC 5465 Section 8 defines `event-ext` as a separate ABNF production
/// from `message-event`, so they must be rejected for selected /
/// selected-delayed filters (Section 6.1). Mailbox events (`MailboxName`,
/// `SubscriptionChange`) and metadata events are also excluded.
fn is_message_event(event: &NotifyEvent) -> bool {
    // RFC 5465 Section 8 ABNF: `message-event` is MessageNew /
    // MessageExpunge / FlagChange / AnnotationChange.  `event-ext`
    // (modelled as `Other(...)`) is a separate production and MUST NOT
    // be accepted under selected / selected-delayed filters (Section 6.1).
    matches!(
        event,
        NotifyEvent::MessageNew { .. }
            | NotifyEvent::MessageExpunge
            | NotifyEvent::FlagChange
            | NotifyEvent::AnnotationChange
    )
}

/// Encode a mailbox filter for a NOTIFY event group (RFC 5465 Section 6).
///
/// RFC 5465 Section 8 ABNF:
/// ```text
/// filter-mailboxes-selected = "selected" / "selected-delayed"
/// filter-mailboxes-other    = "inboxes" / "personal" / "subscribed" /
///                             ("subtree" SP one-or-more-mailbox) /
///                             ("mailboxes" SP one-or-more-mailbox)
/// one-or-more-mailbox       = mailbox / many-mailboxes
/// many-mailboxes            = "(" mailbox *(SP mailbox) ")"
/// ```
fn encode_mailbox_filter(
    buf: &mut BytesMut,
    filter: &MailboxFilter,
    utf8: bool,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    match filter {
        MailboxFilter::Selected => buf.extend_from_slice(b"selected"),
        MailboxFilter::SelectedDelayed => buf.extend_from_slice(b"selected-delayed"),
        MailboxFilter::Inboxes => buf.extend_from_slice(b"inboxes"),
        MailboxFilter::Personal => buf.extend_from_slice(b"personal"),
        MailboxFilter::Subscribed => buf.extend_from_slice(b"subscribed"),
        MailboxFilter::Subtree(mailboxes) => {
            // RFC 5465 Section 8: one-or-more-mailbox requires at least one.
            if mailboxes.is_empty() {
                return Err(crate::Error::Protocol(
                    "subtree filter requires at least one mailbox (RFC 5465 Section 8)".into(),
                ));
            }
            buf.extend_from_slice(b"subtree");
            encode_one_or_more_mailbox(buf, mailboxes, utf8, literal_mode);
        }
        MailboxFilter::Mailboxes(mailboxes) => {
            // RFC 5465 Section 8: one-or-more-mailbox requires at least one.
            if mailboxes.is_empty() {
                return Err(crate::Error::Protocol(
                    "mailboxes filter requires at least one mailbox (RFC 5465 Section 8)".into(),
                ));
            }
            buf.extend_from_slice(b"mailboxes");
            encode_one_or_more_mailbox(buf, mailboxes, utf8, literal_mode);
        }
    }
    Ok(())
}

/// Encode `one-or-more-mailbox` (RFC 5465 Section 8).
///
/// RFC 5465 Section 8 ABNF:
/// ```text
/// one-or-more-mailbox = mailbox / many-mailboxes
/// many-mailboxes      = "(" mailbox *(SP mailbox) ")"
/// ```
///
/// A single mailbox is encoded bare; two or more are parenthesized.
fn encode_one_or_more_mailbox(
    buf: &mut BytesMut,
    mailboxes: &[String],
    utf8: bool,
    literal_mode: LiteralMode,
) {
    if mailboxes.len() == 1 {
        // RFC 5465 Section 8: one-or-more-mailbox = mailbox
        // RFC 3501 Section 5.1.3: encode with INBOX normalization and MUTF-7.
        let wire = encode_mailbox_str(&mailboxes[0], utf8);
        buf.extend_from_slice(b" ");
        encode_quoted_or_literal_utf8(buf, wire.as_bytes(), utf8, literal_mode);
    } else {
        // RFC 5465 Section 8: many-mailboxes = "(" mailbox *(SP mailbox) ")"
        buf.extend_from_slice(b" (");
        for (i, mbox) in mailboxes.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b" ");
            }
            // RFC 3501 Section 5.1.3: encode with INBOX normalization and MUTF-7.
            let wire = encode_mailbox_str(mbox, utf8);
            encode_quoted_or_literal_utf8(buf, wire.as_bytes(), utf8, literal_mode);
        }
        buf.extend_from_slice(b")");
    }
}

/// Encode the events portion of an event group (RFC 5465 Section 8).
///
/// RFC 5465 Section 8 ABNF:
/// ```text
/// events = ("(" event *(SP event) ")") / "NONE"
/// ```
fn encode_events(buf: &mut BytesMut, events: &[NotifyEvent]) -> Result<(), crate::Error> {
    if events.is_empty() {
        // RFC 5465 Section 8: events = "NONE"
        buf.extend_from_slice(b"NONE");
        return Ok(());
    }
    buf.extend_from_slice(b"(");
    for (i, event) in events.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b" ");
        }
        encode_single_event(buf, event)?;
    }
    buf.extend_from_slice(b")");
    Ok(())
}

/// Encode a single NOTIFY event (RFC 5465 Section 5, Section 8).
///
/// RFC 5465 Section 8 ABNF:
/// ```text
/// message-event = ("MessageNew" [SP "(" fetch-att *(SP fetch-att) ")"])
///               / "MessageExpunge" / "FlagChange" / "AnnotationChange"
/// ```
fn encode_single_event(buf: &mut BytesMut, event: &NotifyEvent) -> Result<(), crate::Error> {
    match event {
        NotifyEvent::MessageNew { fetch_attrs } => {
            buf.extend_from_slice(b"MessageNew");
            if !fetch_attrs.is_empty() {
                // RFC 5465 Section 5.2: optional fetch attributes for
                // selected/selected-delayed (per Section 8 ABNF).
                buf.extend_from_slice(b" (");
                for (i, attr) in fetch_attrs.iter().enumerate() {
                    if i > 0 {
                        buf.extend_from_slice(b" ");
                    }
                    // Reject CRLF in fetch attributes to prevent command injection
                    // (RFC 3501 Section 2.2).
                    validate_no_crlf(attr, "NOTIFY fetch-att")?;
                    // Reject empty attrs and unbalanced delimiters  -  they produce
                    // malformed wire output (RFC 3501 Section 6.4.5 fetch-att).
                    validate_single_fetch_att(attr)?;
                    buf.extend_from_slice(attr.as_bytes());
                }
                buf.extend_from_slice(b")");
            }
        }
        NotifyEvent::MessageExpunge => buf.extend_from_slice(b"MessageExpunge"),
        NotifyEvent::FlagChange => buf.extend_from_slice(b"FlagChange"),
        NotifyEvent::AnnotationChange => buf.extend_from_slice(b"AnnotationChange"),
        NotifyEvent::MailboxName => buf.extend_from_slice(b"MailboxName"),
        NotifyEvent::SubscriptionChange => buf.extend_from_slice(b"SubscriptionChange"),
        NotifyEvent::MailboxMetadataChange => buf.extend_from_slice(b"MailboxMetadataChange"),
        NotifyEvent::ServerMetadataChange => buf.extend_from_slice(b"ServerMetadataChange"),
        NotifyEvent::Other(name) => {
            // RFC 5465 Section 8: event-ext = atom  -  validate as IMAP atom.
            validate_atom(name, "NOTIFY event-ext")?;
            buf.extend_from_slice(name.as_bytes());
        }
    }
    Ok(())
}
