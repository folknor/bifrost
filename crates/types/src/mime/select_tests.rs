use super::*;

#[test]
fn select_alternative_prefers_last_representation() {
    let message = crate::mime::parse_message(b"Content-Type: multipart/alternative; boundary=x\r\n\r\n--x\r\nContent-Type: text/plain\r\n\r\nplain\r\n--x\r\nContent-Type: text/html\r\n\r\n<b>html</b>\r\n--x--\r\n");
    let selected = select_body(&message);
    assert_eq!(selected.text.as_deref(), Some("plain"));
    assert_eq!(selected.html.as_deref(), Some("<b>html</b>"));
}

/// The common "HTML with inline images" shape: the richest alternative is a
/// nested `multipart/related`, not a bare `text/html`. Skipping non-text
/// alternatives loses the HTML body AND its `cid:` assets entirely.
#[test]
fn select_alternative_recurses_into_a_nested_related_alternative() {
    let message = crate::mime::parse_message(b"Content-Type: multipart/alternative; boundary=alt\r\n\r\n--alt\r\nContent-Type: text/plain\r\n\r\nplain\r\n--alt\r\nContent-Type: multipart/related; boundary=rel\r\n\r\n--rel\r\nContent-Type: text/html\r\n\r\n<img src=\"cid:asset\">\r\n--rel\r\nContent-Type: image/png\r\nContent-ID: <asset>\r\n\r\ndata\r\n--rel--\r\n--alt--\r\n");
    let selected = select_body(&message);
    assert_eq!(selected.text.as_deref(), Some("plain"));
    assert_eq!(selected.html.as_deref(), Some("<img src=\"cid:asset\">"));
    assert_eq!(selected.attachments.len(), 1);
    assert!(selected.attachments[0].inline);
}

/// Rule 9 fills in for an ABSENT disposition. An explicit `attachment` is the
/// sender's answer and a `cid:` reference does not overrule it.
#[test]
fn select_cid_reference_does_not_override_an_explicit_disposition() {
    let message = crate::mime::parse_message(b"Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Type: text/html\r\n\r\n<img src=cid:image>\r\n--x\r\nContent-Type: image/png\r\nContent-Disposition: attachment\r\nContent-ID: <image>\r\n\r\ndata\r\n--x--\r\n");
    assert!(!select_body(&message).attachments[0].inline);
}

/// The scheme is case-insensitive, the id is not (RFC 2392).
#[test]
fn select_cid_match_is_exact_on_the_id() {
    let message = crate::mime::parse_message(b"Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Type: text/html\r\n\r\n<img src=CID:Image>\r\n--x\r\nContent-Type: image/png\r\nContent-ID: <Image>\r\n\r\ndata\r\n--x\r\nContent-Type: image/png\r\nContent-ID: <image>\r\n\r\ndata\r\n--x--\r\n");
    let selected = select_body(&message);
    assert!(selected.attachments[0].inline);
    assert!(!selected.attachments[1].inline);
}

#[test]
fn select_text_part_with_filename_is_an_attachment() {
    let message =
        crate::mime::parse_message(b"Content-Type: text/plain; name=file.txt\r\n\r\nbody");
    let selected = select_body(&message);
    assert!(selected.text.is_none());
    assert_eq!(selected.attachments.len(), 1);
}

#[test]
fn select_related_start_part_is_body_rest_are_attachments() {
    let message = crate::mime::parse_message(b"Content-Type: multipart/related; boundary=x; start=<root>\r\n\r\n--x\r\nContent-Type: image/png\r\nContent-ID: <asset>\r\n\r\ndata\r\n--x\r\nContent-Type: text/html\r\nContent-ID: <root>\r\n\r\n<img src=\"cid:asset\">\r\n--x--\r\n");
    let selected = select_body(&message);
    assert_eq!(selected.html.as_deref(), Some("<img src=\"cid:asset\">"));
    assert_eq!(selected.attachments.len(), 1);
    assert!(selected.attachments[0].inline);
}

#[test]
fn select_signed_takes_first_child_signature_is_attachment() {
    let message = crate::mime::parse_message(b"Content-Type: multipart/signed; boundary=x\r\n\r\n--x\r\nContent-Type: text/plain\r\n\r\nbody\r\n--x\r\nContent-Type: application/pgp-signature\r\n\r\nsig\r\n--x--\r\n");
    let selected = select_body(&message);
    assert_eq!(selected.text.as_deref(), Some("body"));
    assert_eq!(selected.attachments.len(), 1);
}

#[test]
fn select_encrypted_yields_no_body_and_two_attachments() {
    let message = crate::mime::parse_message(b"Content-Type: multipart/encrypted; boundary=x\r\n\r\n--x\r\nContent-Type: application/pgp-encrypted\r\n\r\nVersion: 1\r\n--x\r\nContent-Type: application/octet-stream\r\n\r\ncipher\r\n--x--\r\n");
    let selected = select_body(&message);
    assert!(selected.text.is_none() && selected.html.is_none());
    assert_eq!(selected.attachments.len(), 2);
}

#[test]
fn select_inline_flag_set_by_cid_reference_in_html() {
    let message = crate::mime::parse_message(b"Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Type: text/html\r\n\r\n<img src=cid:image>\r\n--x\r\nContent-Type: image/png\r\nContent-ID: <image>\r\n\r\ndata\r\n--x--\r\n");
    assert!(select_body(&message).attachments[0].inline);
}

#[test]
fn select_metadata_only_still_reports_size() {
    let message = crate::mime::parse_message(b"Content-Type: application/octet-stream\r\n\r\nabc");
    let selected = select_body_with(
        &message,
        SelectOptions {
            include_attachment_bytes: false,
            ..SelectOptions::default()
        },
    );
    assert_eq!(selected.attachments[0].size, 3);
    assert!(selected.attachments[0].data.is_none());
}

#[test]
fn select_embedded_message_is_an_attachment_not_a_body() {
    let message = crate::mime::parse_message(
        b"Content-Type: message/rfc822\r\n\r\nSubject: forwarded\r\n\r\nbody",
    );
    let selected = select_body(&message);
    assert!(selected.text.is_none());
    assert_eq!(selected.attachments[0].content_type, "message/rfc822");
}

#[test]
fn select_embedded_attachment_bytes_are_the_original_octets() {
    let raw = b"Content-Type: message/rfc822\r\n\r\nSubject: forwarded\r\n\r\nbody";
    let selected = select_body(&crate::mime::parse_message(raw));
    assert!(
        matches!(selected.attachments[0].data.as_ref(), Some(bytes) if bytes.as_ref() == b"Subject: forwarded\r\n\r\nbody")
    );
}

#[test]
fn select_metadata_only_does_not_decode_base64() {
    let message = crate::mime::parse_message(
        b"Content-Type: application/octet-stream\r\nContent-Transfer-Encoding: base64\r\n\r\nYWJj",
    );
    let selected = select_body_with(
        &message,
        SelectOptions {
            include_attachment_bytes: false,
            ..SelectOptions::default()
        },
    );
    assert_eq!(selected.attachments[0].size, 3);
    assert!(selected.attachments[0].data.is_none());
}
