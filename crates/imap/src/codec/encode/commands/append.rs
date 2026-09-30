//! APPEND / MULTIAPPEND encoder.
//!
//! The driver calls [`encode_append`] at send time, against the connection
//! state it owns, so every decision the wire bytes bake in (mailbox encoding,
//! the RFC 6855 `UTF8 (` wrapper, RFC 7888 `+` markers, literal8 eligibility)
//! is made from live state and cannot go stale while the command is queued.

use bytes::Bytes;

use super::{
    BytesMut, LITERAL_MINUS_MAX, LiteralMode, encode_mailbox_str, encode_quoted_or_literal,
    encode_quoted_or_literal_utf8, validate_and_filter_flags, validate_append_datetime,
};
use crate::codec::encode::{ChunkedCommand, EncodeOptions, EncodedCommand};
use crate::types::response::Capability;
use crate::types::{AppendMessage, Flag};

/// Which literal syntax carries one message body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiteralForm {
    /// Classic `{n}` literal (RFC 3501 Section 4.3).
    Classic,
    /// `~{n}` literal8, required for bodies containing NUL (RFC 3516
    /// Section 4.4 / RFC 9051 Section 9).
    Literal8,
    /// `UTF8 (~{n}` ... `)`, the RFC 6855 Section 4 data extension, used for
    /// every message once `UTF8=ACCEPT` is enabled.
    Utf8Literal8,
}

/// Encode APPEND (`multi == false`, exactly one message) or MULTIAPPEND
/// (`multi == true`, RFC 3502) into a [`ChunkedCommand`].
///
/// Everything is decided from `opts`, which the driver builds from live
/// protocol state immediately before sending:
///
/// - **Validation.** MULTIAPPEND requires the MULTIAPPEND capability; the
///   message list must be non-empty (and a single APPEND carries exactly one);
///   flags and dates are validated; a body containing NUL requires BINARY.
///   Failures are returned as [`crate::Error`] variants
///   (`MissingCapability`, `InvalidInput`, `InvalidAppendDate`, ...) rather than
///   `EncodeError`, because the account layer maps the variants.
/// - **Mailbox.** INBOX-normalized and modified UTF-7 unless `opts.utf8_mode`
///   (`UTF8=ACCEPT` or active `IMAP4rev2`), where the raw UTF-8 name is used
///   and quoted-string form is preferred over a literal (RFC 6855 Section 3).
/// - **Flags.** `\Recent` and `\*` are not valid in APPEND and are dropped
///   (RFC 3501 Section 9); custom keywords must be ATOM-CHARs.
/// - **Literal form.** `UTF8 (~{n}` under `UTF8=ACCEPT` (the group is closed
///   with `)` after the body), `~{n}` for a NUL-bearing body, `{n}` otherwise.
/// - **Marker.** Non-synchronizing (`+`) per RFC 7888: LITERAL+ for any size,
///   LITERAL- (and `IMAP4rev2`) up to 4096 octets. Both `literal8` forms
///   additionally need BINARY and a non-rev2 connection (RFC 7888 Section 6,
///   RFC 9051 Section 9); otherwise they stay synchronizing.
///
/// Bodies are never copied: each message's [`Bytes`] is cloned into the output
/// (a reference-count bump) and only the surrounding command syntax is
/// allocated. A synchronizing literal, whether a message body or a
/// non-quotable mailbox or date, ends its segment on the marker, so N
/// synchronizing literals yield N+1 segments.
pub(crate) fn encode_append(
    tag: &str,
    mailbox: &str,
    messages: &[AppendMessage],
    multi: bool,
    opts: &EncodeOptions,
) -> Result<ChunkedCommand, crate::Error> {
    if multi && !opts.has_capability(&Capability::MultiAppend) {
        // RFC 3502 Section 3.
        return Err(crate::Error::MissingCapability("MULTIAPPEND".into()));
    }
    if messages.is_empty() {
        return Err(crate::Error::InvalidInput(
            "MULTIAPPEND requires at least one message".into(),
        ));
    }
    if !multi && messages.len() != 1 {
        return Err(crate::Error::InvalidInput(
            "APPEND carries exactly one message; use MULTIAPPEND for several".into(),
        ));
    }

    let literal_mode = opts.literal_mode;
    let utf8 = opts.utf8_mode;
    let wrapper = opts.utf8_accept_enabled();
    let literal8_plus = opts.literal8_non_sync_allowed();
    let wire_mailbox = encode_mailbox_str(mailbox, utf8);

    let mut segments: Vec<Vec<Bytes>> = Vec::new();
    let mut current: Vec<Bytes> = Vec::new();

    for (i, msg) in messages.iter().enumerate() {
        let mut head = BytesMut::new();
        if i == 0 {
            // Tag + APPEND + mailbox (RFC 3502 Section 3). RFC 6855 Section 3:
            // under UTF-8 mode the mailbox may be a quoted string.
            head.extend_from_slice(tag.as_bytes());
            head.extend_from_slice(b" APPEND ");
            encode_quoted_or_literal_utf8(&mut head, wire_mailbox.as_bytes(), utf8, literal_mode);
        } else if wrapper {
            // RFC 6855 Section 4: close the previous message's UTF8 group.
            head.extend_from_slice(b")");
        }

        // Flags (RFC 3501 Section 6.3.11).
        let flags: Vec<&Flag> = validate_and_filter_flags(&msg.flags, "APPEND")?;
        if !flags.is_empty() {
            head.extend_from_slice(b" (");
            for (n, flag) in flags.iter().enumerate() {
                if n > 0 {
                    head.extend_from_slice(b" ");
                }
                head.extend_from_slice(flag.as_imap_str().as_bytes());
            }
            head.extend_from_slice(b")");
        }

        // Internal date: validated against the date-time production, then a
        // quoted string (RFC 3501 Section 9).
        if let Some(date) = msg.date.as_deref() {
            validate_append_datetime(date)?;
            head.extend_from_slice(b" ");
            encode_quoted_or_literal(&mut head, date.as_bytes(), literal_mode);
        }

        let form = literal_form(msg, wrapper, opts)?;
        let plus_by_mode = match literal_mode {
            LiteralMode::LiteralPlus => true,
            LiteralMode::LiteralMinus => msg.data.len() <= LITERAL_MINUS_MAX,
            LiteralMode::Synchronizing => false,
        };
        let non_sync = plus_by_mode && (form == LiteralForm::Classic || literal8_plus);

        // The mailbox or date may itself have been a synchronizing literal.
        // Split the (small) syntax buffer at those boundaries; the message
        // body is not in it, so this never scans or copies a body.
        let mut head_segments = if head.is_empty() {
            Vec::new()
        } else {
            EncodedCommand::from_flat_buffer(&head).into_segments()
        };
        let mut tail = head_segments.pop().unwrap_or_default();
        for segment in head_segments {
            current.push(segment.freeze());
            segments.push(std::mem::take(&mut current));
        }

        tail.extend_from_slice(match form {
            LiteralForm::Utf8Literal8 => b" UTF8 (~{".as_slice(),
            LiteralForm::Literal8 => b" ~{".as_slice(),
            LiteralForm::Classic => b" {".as_slice(),
        });
        tail.extend_from_slice(msg.data.len().to_string().as_bytes());
        if non_sync {
            tail.extend_from_slice(b"+");
        }
        tail.extend_from_slice(b"}\r\n");
        current.push(tail.freeze());
        if !non_sync {
            // The server must grant a continuation before the body.
            segments.push(std::mem::take(&mut current));
        }
        current.push(msg.data.clone());
    }

    // RFC 6855 Section 4 closes the last UTF8 group; RFC 3501 Section 2.2
    // ends the command line.
    current.push(Bytes::from_static(if wrapper {
        b")\r\n".as_slice()
    } else {
        b"\r\n".as_slice()
    }));
    segments.push(current);
    Ok(ChunkedCommand::new(segments))
}

/// Choose the literal syntax for one message body.
fn literal_form(
    msg: &AppendMessage,
    wrapper: bool,
    opts: &EncodeOptions,
) -> Result<LiteralForm, crate::Error> {
    if wrapper {
        return Ok(LiteralForm::Utf8Literal8);
    }
    if !msg.data.contains(&0) {
        return Ok(LiteralForm::Classic);
    }
    // RFC 3516 Section 4.4: NUL octets need literal8, which needs BINARY.
    // Active IMAP4rev2 does not stand in for it: RFC 9051 Appendix B folds
    // in only the FETCH side of BINARY, not its APPEND extension, and the
    // capability authority answers BINARY accordingly (advertised only).
    if opts.has_capability(&Capability::Binary) {
        Ok(LiteralForm::Literal8)
    } else {
        // The body is expressible; the server lacks the capability that
        // would carry it, so this is `Unsupported`, not a malformed request.
        Err(crate::Error::MissingCapability(
            "APPEND data containing NUL requires BINARY literal8 support \
             (RFC 3516 Section 4.4)"
                .into(),
        ))
    }
}
