use super::*;

#[test]
fn parse_plain_text_message_without_mime_version() {
    let message = parse_message(b"Subject: Test\r\n\r\nhello");
    assert_eq!(message.root.content_type.essence(), "text/plain");
    assert_eq!(message.root.text(), Some("hello".into()));
}

#[test]
fn parse_multipart_alternative_yields_two_leaves() {
    let message = parse_message(b"Content-Type: multipart/alternative; boundary=x\r\n\r\n--x\r\nContent-Type: text/plain\r\n\r\nplain\r\n--x\r\nContent-Type: text/html\r\n\r\n<b>html</b>\r\n--x--\r\n");
    assert!(matches!(&message.root.body, PartBody::Multipart(parts) if parts.len() == 2));
}

#[test]
fn parse_nested_multipart_mixed_related_tree() {
    let message = parse_message(b"Content-Type: multipart/mixed; boundary=out\r\n\r\n--out\r\nContent-Type: multipart/related; boundary=in\r\n\r\n--in\r\nContent-Type: text/html\r\n\r\n<p>hi</p>\r\n--in\r\nContent-Type: image/png\r\n\r\npng\r\n--in--\r\n--out\r\nContent-Type: application/pdf\r\n\r\npdf\r\n--out--\r\n");
    let PartBody::Multipart(outer) = &message.root.body else {
        panic!("mixed root is a multipart");
    };
    assert_eq!(outer.len(), 2);
    assert!(matches!(&outer[0].body, PartBody::Multipart(inner) if inner.len() == 2));
    assert_eq!(outer[1].content_type.essence(), "application/pdf");
}

#[test]
fn parse_missing_close_delimiter_records_truncated() {
    let message =
        parse_message(b"Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\n\r\nbody");
    assert!(message.defects.contains(&Defect::Truncated));
}

#[test]
fn parse_discards_preamble_and_epilogue() {
    let parsed = parse_message(b"Content-Type: multipart/mixed; boundary=x\r\n\r\npreamble\r\n--x\r\nContent-Type: text/plain\r\n\r\nbody\r\n--x--\r\nepilogue");
    let PartBody::Multipart(parts) = &parsed.root.body else {
        panic!("mixed root is a multipart");
    };
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].text().as_deref(), Some("body"));
}

#[test]
fn parse_multipart_without_boundary_is_a_leaf() {
    let missing = parse_message(b"Content-Type: multipart/mixed\r\n\r\nbody");
    assert!(matches!(missing.root.body, PartBody::Leaf(_)));
    assert!(missing.defects.contains(&Defect::MissingBoundary));
}

#[test]
fn parse_depth_limit_stops_recursion() {
    let mut raw = b"Content-Type: multipart/mixed; boundary=b0\r\n\r\n".to_vec();
    for level in 1..6 {
        raw.extend_from_slice(
            format!(
                "--b{}\r\nContent-Type: multipart/mixed; boundary=b{level}\r\n\r\n",
                level - 1
            )
            .as_bytes(),
        );
    }
    raw.extend_from_slice(b"--b5\r\nContent-Type: text/plain\r\n\r\ndeep\r\n--b5--\r\n");
    let limits = MimeLimits {
        max_depth: 2,
        ..MimeLimits::default()
    };
    let parsed = parse_message_with_limits(&raw, limits);
    assert!(parsed.defects.contains(&Defect::DepthExceeded));
}

#[test]
fn parse_part_limit_stops_expansion() {
    let mut raw = b"Content-Type: multipart/mixed; boundary=x\r\n\r\n".to_vec();
    for index in 0..50 {
        raw.extend_from_slice(
            format!("--x\r\nContent-Type: text/plain\r\n\r\n{index}\r\n").as_bytes(),
        );
    }
    raw.extend_from_slice(b"--x--\r\n");
    let limits = MimeLimits {
        max_parts: 8,
        ..MimeLimits::default()
    };
    let parsed = parse_message_with_limits(&raw, limits);
    assert!(parsed.defects.contains(&Defect::PartCountExceeded));
    let PartBody::Multipart(parts) = &parsed.root.body else {
        panic!("mixed root is a multipart");
    };
    assert!(parts.len() < 50);
}

#[test]
fn parse_message_rfc822_is_embedded_not_flattened() {
    let parsed =
        parse_message(b"Content-Type: message/rfc822\r\n\r\nSubject: inner\r\n\r\ninner body");
    let PartBody::Embedded { raw, message } = &parsed.root.body else {
        panic!("message/rfc822 is embedded");
    };
    assert_eq!(raw.as_ref(), b"Subject: inner\r\n\r\ninner body");
    assert_eq!(message.headers.get("subject"), Some("inner"));
}

/// A chain of `message/rfc822` wrappers used to reset the depth budget at
/// every level, so nesting was bounded only by the input length: a stack
/// overflow on hostile mail. The embedded parse shares the enclosing budget.
#[test]
fn parse_nested_message_rfc822_chain_is_depth_bounded() {
    let mut raw = Vec::new();
    for _ in 0..64 {
        raw.extend_from_slice(b"Content-Type: message/rfc822\r\n\r\n");
    }
    raw.extend_from_slice(b"Subject: innermost\r\n\r\nbody");
    let limits = MimeLimits {
        max_depth: 4,
        ..MimeLimits::default()
    };
    let parsed = parse_message_with_limits(&raw, limits);
    assert!(parsed.defects.contains(&Defect::DepthExceeded));

    let mut depth = 0;
    let mut part = &parsed.root;
    while let PartBody::Embedded { message, .. } = &part.body {
        depth += 1;
        part = &message.root;
    }
    assert!(
        depth <= limits.max_depth,
        "nesting stayed inside the budget"
    );
}

#[test]
fn parse_accepts_bare_lf_line_endings() {
    let parsed = parse_message(
        b"Content-Type: multipart/mixed; boundary=x\n\n--x\nContent-Type: text/plain\n\nbody\n--x--\n",
    );
    let PartBody::Multipart(parts) = &parsed.root.body else {
        panic!("mixed root is a multipart");
    };
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].text().as_deref(), Some("body"));
}

#[test]
fn parse_unknown_encoding_forces_application_octet_stream() {
    let parsed = parse_message(
        b"Content-Type: text/plain\r\nContent-Transfer-Encoding: x-weird\r\n\r\nopaque",
    );
    assert_eq!(
        parsed.root.content_type.essence(),
        "application/octet-stream"
    );
    // RFC 2045 section 6.4 forces the media type; the DECLARED type must stay
    // recoverable for a faithful re-emit.
    assert_eq!(parsed.root.headers.get("Content-Type"), Some("text/plain"));
    assert!(parsed.defects.contains(&Defect::UnknownTransferEncoding));
}

#[test]
fn digest_child_without_content_type_defaults_to_message_rfc822() {
    let parsed = parse_message(b"Content-Type: multipart/digest; boundary=x\r\n\r\n--x\r\n\r\nSubject: forwarded\r\n\r\nbody\r\n--x--\r\n");
    let PartBody::Multipart(parts) = &parsed.root.body else {
        panic!("digest root is a multipart");
    };
    assert_eq!(parts[0].content_type.essence(), "message/rfc822");
    assert!(matches!(parts[0].body, PartBody::Embedded { .. }));
}

#[test]
fn filename_strips_path_separators() {
    let parsed = parse_message(
        b"Content-Type: application/pdf\r\nContent-Disposition: attachment; filename=\"../../etc/passwd\"\r\n\r\n%PDF",
    );
    assert_eq!(parsed.root.filename.as_deref(), Some("....etcpasswd"));
}

#[test]
fn charset_unknown_label_falls_back_to_windows1252() {
    let parsed = parse_message(b"Content-Type: text/plain; charset=x-nonsense\r\n\r\n\xe9");
    let mut defects = Vec::new();
    assert_eq!(
        parsed
            .root
            .text_with(&MimeLimits::default(), &mut defects)
            .as_deref(),
        Some("\u{e9}")
    );
    assert!(defects.contains(&Defect::UnknownCharset));
}

#[test]
fn text_over_limit_truncates_on_char_boundary() {
    let parsed = parse_message("Content-Type: text/plain; charset=utf-8\r\n\r\nææææ".as_bytes());
    let limits = MimeLimits {
        max_text_bytes: 3,
        ..MimeLimits::default()
    };
    let mut defects = Vec::new();
    // Three bytes lands mid-character; the cut must fall back to two.
    assert_eq!(
        parsed.root.text_with(&limits, &mut defects).as_deref(),
        Some("æ")
    );
    assert!(defects.contains(&Defect::TextTruncated));
}
