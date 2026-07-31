use super::*;

fn headers(raw: &[u8]) -> HeaderMap {
    split_headers(raw, &MimeLimits::default(), &mut Vec::new()).0
}

#[test]
fn header_unfolds_continuation_lines() {
    assert_eq!(
        headers(b"Subject: first\r\n second\r\n\r\nbody").get("subject"),
        Some("first second")
    );
}

#[test]
fn header_lookup_is_case_insensitive() {
    assert_eq!(headers(b"X-Test: one\r\n\r\n").get("X-TEST"), Some("one"));
}

#[test]
fn header_get_all_preserves_wire_order() {
    assert_eq!(
        headers(b"X-Test: one\r\nx-test: two\r\n\r\n")
            .get_all("x-test")
            .collect::<Vec<_>>(),
        ["one", "two"]
    );
}

#[test]
fn header_addresses_parse_display_name_and_group() {
    let parsed = headers(b"To: Team: Ada <ada@example.test>, \"B, C\" <bc@example.test>;\r\n\r\n");
    let addresses = parsed.addresses("to");
    assert_eq!(addresses.len(), 2);
    assert_eq!(addresses[0].address, "ada@example.test");
    assert_eq!(addresses[1].name.as_deref(), Some("B, C"));
}

#[test]
fn header_addresses_ignore_comma_inside_quotes() {
    let parsed = headers(b"To: \"Smith, John\" <john@test>, jane@test\r\n\r\n");
    let addresses = parsed.addresses("to");
    assert_eq!(addresses.len(), 2);
    assert_eq!(addresses[0].name.as_deref(), Some("Smith, John"));
}

#[test]
fn header_addresses_decode_encoded_word_display_name() {
    let parsed = headers(b"To: =?UTF-8?Q?Doe=2C_Jane?= <jane@test>\r\n\r\n");
    assert_eq!(parsed.addresses("to")[0].name.as_deref(), Some("Doe, Jane"));
}

#[test]
fn header_date_parses_rfc5322_and_obsolete_forms() {
    // Four-digit year with a numeric zone, then the obsolete alphabetic zone
    // and a missing day-of-week comma, which real archives both carry.
    assert_eq!(
        headers(b"Date: Thu, 1 Jan 1970 00:00:00 +0000\r\n\r\n").date("date"),
        Some(std::time::UNIX_EPOCH)
    );
    assert!(
        headers(b"Date: 21 Nov 1997 09:55:06 GMT\r\n\r\n")
            .date("date")
            .is_some()
    );
    assert!(
        headers(b"Date: Fri 21 Nov 1997 09:55 EST\r\n\r\n")
            .date("date")
            .is_some()
    );
    assert!(headers(b"Date: not a date\r\n\r\n").date("date").is_none());
}

#[test]
fn header_date_two_digit_year_windows_correctly() {
    assert_eq!(
        headers(b"Date: Fri, 21 Nov 97 09:55:06 +0000\r\n\r\n").date("date"),
        headers(b"Date: Fri, 21 Nov 1997 09:55:06 +0000\r\n\r\n").date("date")
    );
    assert_eq!(
        headers(b"Date: Sat, 1 Jan 05 00:00:00 +0000\r\n\r\n").date("date"),
        headers(b"Date: Sat, 1 Jan 2005 00:00:00 +0000\r\n\r\n").date("date")
    );
}

#[test]
fn header_message_ids_strip_angle_brackets() {
    let parsed = headers(b"References: <a@test>\r\n <b@test>\r\nIn-Reply-To: <b@test>\r\n\r\n");
    assert_eq!(parsed.message_ids("references"), ["a@test", "b@test"]);
    assert_eq!(parsed.message_ids("in-reply-to"), ["b@test"]);
}

#[test]
fn header_block_over_limit_records_defect() {
    let mut raw = b"Subject: ".to_vec();
    raw.extend(std::iter::repeat_n(b'x', 4096));
    raw.extend_from_slice(b"\r\n\r\nbody");
    let mut defects = Vec::new();
    let limits = MimeLimits {
        max_header_bytes: 64,
        ..MimeLimits::default()
    };
    let (parsed, body) = split_headers(&raw, &limits, &mut defects);
    assert!(defects.contains(&Defect::HeaderBlockTooLarge));
    assert!(
        parsed
            .get("subject")
            .is_some_and(|value| value.len() < 4096)
    );
    // The body survives a capped header block; it used to be discarded.
    assert_eq!(body, b"body");
}

#[test]
fn header_missing_separator_header_like_input_is_all_headers() {
    let mut defects = Vec::new();
    let (parsed, body) = split_headers(
        b"Subject: truncated fetch",
        &MimeLimits::default(),
        &mut defects,
    );
    assert!(defects.contains(&Defect::MissingHeaderSeparator));
    assert_eq!(parsed.get("subject"), Some("truncated fetch"));
    assert!(body.is_empty());
}

#[test]
fn header_missing_separator_bare_body_input_is_all_body() {
    let mut defects = Vec::new();
    let (parsed, body) = split_headers(
        b"just some prose\r\nwith no headers",
        &MimeLimits::default(),
        &mut defects,
    );
    assert!(defects.contains(&Defect::MissingHeaderSeparator));
    assert!(parsed.is_empty());
    assert_eq!(body, b"just some prose\r\nwith no headers");
}
