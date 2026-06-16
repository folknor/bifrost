//! Shared RFC 5322 / MIME assembler for the send and draft paths.
//!
//! This is the single message serializer the protocol crates compose
//! through. It operates entirely in [`Address`](crate::compose::Address)
//! space - no protocol-specific mailbox type is constructed here - so
//! every account crate (Google, IMAP, and any future SMTP-backed
//! provider) shares one body-building path instead of maintaining a
//! divergent hand-rolled serializer each.
//!
//! It emits:
//! - text-only, html-only, or `multipart/alternative` body entities,
//! - a `multipart/mixed` wrapper when inline attachments are present,
//! - `Content-Disposition: inline` / `attachment` per
//!   [`AttachmentInline::inline`],
//! - RFC 2047 encoded-word display names and subjects,
//! - a deterministic, controlled-domain `Message-ID` (the sender's
//!   domain, never a leaked machine hostname).
//!
//! The result is the raw RFC 5322 octets plus a [`SubmissionEnvelope`]
//! (reverse path + recipient set, still in `Address` space). The narrow
//! `Address -> protocol envelope` conversion is the consuming crate's
//! responsibility at its own transport boundary.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use crate::compose::{Address, AttachmentInline, SendRequest};
use crate::error::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, DiagnosticText,
    RequestCause, RequestErrorKind,
};

/// Bifrost's own reverse-path / recipient pair, in `Address` space.
///
/// Distinct from any protocol `Envelope`: the consuming crate converts
/// the bare addr-specs here into its transport's envelope at the send
/// boundary. The `from` reverse path is the resolved sender; the
/// `recipients` set is `to` union `cc` union `bcc`.
#[derive(Debug, Clone)]
pub struct SubmissionEnvelope {
    /// MAIL FROM reverse path.
    pub from: Address,
    /// RCPT TO recipient set (`to` + `cc` + `bcc`, in that order).
    pub recipients: Vec<Address>,
}

/// Outcome of serializing a [`SendRequest`].
#[derive(Debug, Clone)]
pub struct RenderedMessage {
    /// Reverse path + recipient set for the transport envelope.
    pub envelope: SubmissionEnvelope,
    /// The serialized RFC 5322 octets for transmission. The `Bcc:` header
    /// is omitted so blind recipients are never disclosed on the wire.
    pub raw: Vec<u8>,
    /// The serialized RFC 5322 octets for the sender's own Sent-folder
    /// copy. Identical to [`raw`](Self::raw) except it retains the `Bcc:`
    /// header: the sender's Sent copy is their record of who was blind
    /// copied, so it keeps the blind-recipient list that the transmitted
    /// body drops. `None` when the request carries no Bcc recipients (then
    /// the two bodies would be identical and the caller reuses `raw`).
    pub sent_copy: Option<Vec<u8>>,
    /// The `Message-ID` value (without angle brackets) emitted in the
    /// message headers. Callers use this as a stable fallback id when no
    /// server-side id is available.
    pub message_id: String,
}

/// Serialize a [`SendRequest`] into RFC 5322 octets plus the submission
/// envelope.
///
/// Resolves `from` from `request.from` or `default_from`, validates that
/// at least one recipient is present, rejects uploaded-attachment handles
/// (those belong to the cloud-attachment lane), and emits a
/// controlled-domain `Message-ID`.
///
/// # Errors
///
/// - `Unsupported(AttachmentUpload)` when `attachments_uploaded` is
///   non-empty.
/// - `Request(Malformed)` when `to`, `cc`, and `bcc` are all empty.
pub fn send_request_to_rfc5322(
    request: &SendRequest,
    default_from: &Address,
) -> Result<RenderedMessage, AccountError> {
    if !request.attachments_uploaded.is_empty() {
        return Err(unsupported(AccountOperation::AttachmentUpload));
    }
    if request.to.is_empty() && request.cc.is_empty() && request.bcc.is_empty() {
        return Err(malformed("send requires at least one recipient"));
    }

    let from = request.from.clone().unwrap_or_else(|| default_from.clone());
    let message_id = generate_message_id(&from);

    let mut composed = ComposedMessage {
        from: Some(&from),
        to: &request.to,
        cc: &request.cc,
        bcc: &request.bcc,
        reply_to: &request.reply_to,
        subject: request.subject.as_deref(),
        body_text: request.body_text.as_deref(),
        body_html: request.body_html.as_deref(),
        attachments_inline: &request.attachments_inline,
        in_reply_to: request.in_reply_to.as_deref(),
        references: &request.references,
        message_id: Some(&message_id),
        include_bcc_header: false,
    };

    // The transmitted body drops `Bcc:` so blind recipients are never
    // disclosed on the wire. The sender's own Sent copy retains it: it is
    // the sender's record of who was blind copied. Only render the second
    // variant when there is actually a Bcc to preserve.
    let raw = render_rfc5322(&composed);
    let sent_copy = if request.bcc.is_empty() {
        None
    } else {
        composed.include_bcc_header = true;
        Some(render_rfc5322(&composed))
    };

    let mut recipients =
        Vec::with_capacity(request.to.len() + request.cc.len() + request.bcc.len());
    recipients.extend(request.to.iter().cloned());
    recipients.extend(request.cc.iter().cloned());
    recipients.extend(request.bcc.iter().cloned());

    Ok(RenderedMessage {
        envelope: SubmissionEnvelope { from, recipients },
        raw,
        sent_copy,
        message_id,
    })
}

/// Borrowed field set the renderer consumes. Protocol crates that hold
/// their own composition struct (e.g. Google's `MailDocument`) build one
/// of these to share the body-building path.
pub struct ComposedMessage<'a> {
    /// `From` header. `None` omits the header (a draft without a resolved
    /// sender); the send path always supplies one.
    pub from: Option<&'a Address>,
    pub to: &'a [Address],
    pub cc: &'a [Address],
    pub bcc: &'a [Address],
    pub reply_to: &'a [Address],
    pub subject: Option<&'a str>,
    pub body_text: Option<&'a str>,
    pub body_html: Option<&'a str>,
    pub attachments_inline: &'a [AttachmentInline],
    pub in_reply_to: Option<&'a str>,
    pub references: &'a [String],
    /// `Message-ID` to emit, without angle brackets. `None` omits the
    /// header (e.g. a draft serializer that does not mint one).
    pub message_id: Option<&'a str>,
    /// Emit a `Bcc:` header in the body. Submission strips Bcc, so the
    /// send path leaves this `false`; the draft path sets it `true` so a
    /// saved draft preserves the blind-recipient list.
    pub include_bcc_header: bool,
}

/// Render the message headers and MIME body into raw RFC 5322 octets.
///
/// This performs no recipient validation: it serializes whatever fields
/// it is given. Callers that need the "at least one recipient" guard
/// (the send path) check before calling.
#[must_use]
pub fn render_rfc5322(msg: &ComposedMessage<'_>) -> Vec<u8> {
    let mut headers = Vec::new();
    if let Some(from) = msg.from {
        push_header(&mut headers, "From", &format_address(from));
    }
    push_address_header(&mut headers, "To", msg.to);
    push_address_header(&mut headers, "Cc", msg.cc);
    if msg.include_bcc_header {
        push_address_header(&mut headers, "Bcc", msg.bcc);
    }
    push_address_header(&mut headers, "Reply-To", msg.reply_to);
    if let Some(subject) = msg.subject {
        push_header(&mut headers, "Subject", &encode_header_value(subject));
    }
    if let Some(in_reply_to) = msg.in_reply_to {
        push_header(&mut headers, "In-Reply-To", in_reply_to);
    }
    if !msg.references.is_empty() {
        push_header(&mut headers, "References", &msg.references.join(" "));
    }
    if let Some(message_id) = msg.message_id {
        push_header(&mut headers, "Message-ID", &format!("<{message_id}>"));
    }
    headers.push("MIME-Version: 1.0".to_string());

    let entity = render_entity(msg);
    format!("{}\r\n{entity}\r\n", headers.join("\r\n")).into_bytes()
}

fn render_entity(msg: &ComposedMessage<'_>) -> String {
    if msg.attachments_inline.is_empty() {
        return render_body_entity(msg);
    }

    let boundary = boundary("mixed");
    let mut out = String::new();
    out.push_str(&format!(
        "Content-Type: multipart/mixed; boundary=\"{boundary}\"\r\n\r\n"
    ));
    push_part(&mut out, &boundary, &render_body_entity(msg));
    for attachment in msg.attachments_inline {
        push_part(&mut out, &boundary, &render_attachment_entity(attachment));
    }
    out.push_str(&format!("--{boundary}--\r\n"));
    out
}

fn render_body_entity(msg: &ComposedMessage<'_>) -> String {
    match (msg.body_text, msg.body_html) {
        (Some(text), Some(html)) => {
            let boundary = boundary("alternative");
            let mut out = String::new();
            out.push_str(&format!(
                "Content-Type: multipart/alternative; boundary=\"{boundary}\"\r\n\r\n"
            ));
            push_part(&mut out, &boundary, &render_text_entity("text/plain", text));
            push_part(&mut out, &boundary, &render_text_entity("text/html", html));
            out.push_str(&format!("--{boundary}--\r\n"));
            out
        }
        (Some(text), None) => render_text_entity("text/plain", text),
        (None, Some(html)) => render_text_entity("text/html", html),
        (None, None) => render_text_entity("text/plain", ""),
    }
}

fn render_text_entity(mime: &str, body: &str) -> String {
    format!(
        "Content-Type: {mime}; charset=UTF-8\r\nContent-Transfer-Encoding: base64\r\n\r\n{}\r\n",
        wrap_base64(body.as_bytes())
    )
}

fn render_attachment_entity(attachment: &AttachmentInline) -> String {
    let disposition = if attachment.inline {
        "inline"
    } else {
        "attachment"
    };
    let filename = sanitize_header(&attachment.filename);
    format!(
        "Content-Type: {}; name=\"{filename}\"\r\nContent-Disposition: {disposition}; filename=\"{filename}\"\r\nContent-Transfer-Encoding: base64\r\n\r\n{}\r\n",
        sanitize_header(&attachment.mime),
        wrap_base64(&attachment.data)
    )
}

fn push_part(out: &mut String, boundary: &str, part: &str) {
    out.push_str(&format!("--{boundary}\r\n"));
    out.push_str(part);
}

fn push_header(headers: &mut Vec<String>, name: &str, value: &str) {
    let value = sanitize_header(value);
    if value.trim().is_empty() {
        return;
    }
    headers.push(format!("{name}: {value}"));
}

fn push_address_header(headers: &mut Vec<String>, name: &str, addresses: &[Address]) {
    if addresses.is_empty() {
        return;
    }
    let rendered = addresses
        .iter()
        .map(format_address)
        .collect::<Vec<_>>()
        .join(", ");
    push_header(headers, name, &rendered);
}

/// Format an [`Address`] as an RFC 5322 header value: `Name <addr>` with
/// the display name RFC 2047 encoded when non-ASCII, or the bare
/// addr-spec when there is no name.
#[must_use]
pub fn format_address(address: &Address) -> String {
    let email = sanitize_header(&address.address);
    match address
        .name
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        Some(name) => format!("{} <{email}>", encode_phrase(name)),
        None => email,
    }
}

fn encode_phrase(value: &str) -> String {
    if value.is_ascii() {
        format!("\"{}\"", sanitize_header(value).replace('"', "\\\""))
    } else {
        encode_header_value(value)
    }
}

fn encode_header_value(value: &str) -> String {
    let sanitized = sanitize_header(value);
    if sanitized.is_ascii() {
        sanitized
    } else {
        format!("=?UTF-8?B?{}?=", STANDARD.encode(sanitized.as_bytes()))
    }
}

fn sanitize_header(value: &str) -> String {
    value.replace(['\r', '\n'], " ").trim().to_string()
}

fn wrap_base64(bytes: &[u8]) -> String {
    let encoded = STANDARD.encode(bytes);
    encoded
        .as_bytes()
        .chunks(76)
        .map(|chunk| std::str::from_utf8(chunk).unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\r\n")
}

fn boundary(kind: &str) -> String {
    static NEXT_BOUNDARY: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT_BOUNDARY.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("bifrost-{kind}-{nanos}-{sequence}")
}

/// Mint a `Message-ID` value (without angle brackets) whose right-hand
/// side is the sender's domain. Never derives from a machine hostname,
/// so the value can safely surface to the consumer as a stable id.
fn generate_message_id(from: &Address) -> String {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let domain = from
        .address
        .rsplit_once('@')
        .map(|(_, domain)| domain.trim())
        .filter(|domain| !domain.is_empty())
        .unwrap_or("localhost");
    format!("bifrost.{nanos}.{sequence}@{domain}")
}

fn unsupported(operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(operation),
        Cause::Request(RequestCause::Unsupported { operation }),
    )
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

// Provider-neutral: this assembler is shared (IMAP today, any future
// SMTP-backed provider tomorrow), so it must not assert a protocol. The
// operation is `Send` because `send_request_to_rfc5322` is the send path;
// the consuming crate stamps its protocol via the account-boundary
// context as the error propagates.
fn malformed(detail: &str) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(detail),
        }),
    )
    .operation(AccountOperation::Send)
    .try_build()
    .expect("valid account error classification")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn addr(name: Option<&str>, address: &str) -> Address {
        Address {
            name: name.map(ToOwned::to_owned),
            address: address.to_owned(),
        }
    }

    #[test]
    fn send_request_builds_multipart_mixed_with_alternative() {
        let request = SendRequest {
            to: vec![addr(Some("Hei There"), "hei@example.com")],
            subject: Some("Happy new year".to_owned()),
            body_text: Some("plain body".to_owned()),
            body_html: Some("<p>html body</p>".to_owned()),
            attachments_inline: vec![AttachmentInline {
                filename: "a.txt".to_owned(),
                mime: "text/plain".to_owned(),
                data: Bytes::from_static(b"hello"),
                inline: false,
            }],
            ..SendRequest::default()
        };
        let default_from = addr(None, "me@sender.test");
        let rendered = send_request_to_rfc5322(&request, &default_from).expect("renders");
        let raw = String::from_utf8(rendered.raw).expect("utf8");

        assert!(raw.contains("Content-Type: multipart/mixed;"));
        assert!(raw.contains("Content-Type: multipart/alternative;"));
        assert!(raw.contains("Content-Disposition: attachment; filename=\"a.txt\""));
        // From defaults to default_from when request.from is None.
        assert!(raw.contains("From: me@sender.test"));
        assert!(raw.contains("To: \"Hei There\" <hei@example.com>"));
        // Message-ID domain is the sender's domain, never a hostname.
        assert!(rendered.message_id.ends_with("@sender.test"));
        assert!(raw.contains(&format!("Message-ID: <{}>", rendered.message_id)));
        // No Bcc header on the send path.
        assert!(!raw.contains("Bcc:"));
    }

    #[test]
    fn send_request_envelope_unions_recipients() {
        let request = SendRequest {
            to: vec![addr(None, "a@example.com")],
            cc: vec![addr(None, "b@example.com")],
            bcc: vec![addr(None, "c@example.com")],
            body_text: Some("hi".to_owned()),
            ..SendRequest::default()
        };
        let rendered =
            send_request_to_rfc5322(&request, &addr(None, "me@sender.test")).expect("renders");
        let recipients: Vec<&str> = rendered
            .envelope
            .recipients
            .iter()
            .map(|a| a.address.as_str())
            .collect();
        assert_eq!(
            recipients,
            ["a@example.com", "b@example.com", "c@example.com"]
        );
        assert_eq!(rendered.envelope.from.address, "me@sender.test");

        // The transmitted body strips Bcc; the sender's Sent copy retains
        // it as their record of who was blind copied.
        let wire = String::from_utf8(rendered.raw).expect("utf8");
        assert!(!wire.to_ascii_lowercase().contains("bcc:"));
        let sent =
            String::from_utf8(rendered.sent_copy.expect("bcc present -> sent copy")).expect("utf8");
        assert!(sent.contains("Bcc: c@example.com"));
    }

    #[test]
    fn send_request_without_bcc_has_no_separate_sent_copy() {
        let request = SendRequest {
            to: vec![addr(None, "a@example.com")],
            body_text: Some("hi".to_owned()),
            ..SendRequest::default()
        };
        let rendered =
            send_request_to_rfc5322(&request, &addr(None, "me@sender.test")).expect("renders");
        // No Bcc -> the two bodies would be identical, so no separate copy.
        assert!(rendered.sent_copy.is_none());
    }

    #[test]
    fn send_request_rejects_uploaded_attachments() {
        let request = SendRequest {
            to: vec![addr(None, "a@example.com")],
            attachments_uploaded: vec![crate::compose::AttachmentHandle("h".to_owned())],
            ..SendRequest::default()
        };
        let err = send_request_to_rfc5322(&request, &addr(None, "me@sender.test"))
            .expect_err("uploaded handles rejected");
        assert_eq!(
            *err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::AttachmentUpload)
        );
    }

    #[test]
    fn send_request_requires_recipient() {
        let request = SendRequest {
            body_text: Some("hi".to_owned()),
            ..SendRequest::default()
        };
        let err = send_request_to_rfc5322(&request, &addr(None, "me@sender.test"))
            .expect_err("no recipient");
        assert_eq!(
            *err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
    }
}
