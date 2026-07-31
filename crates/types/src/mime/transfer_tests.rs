use super::*;

#[test]
fn transfer_base64_tolerates_missing_padding() {
    assert_eq!(
        decode_transfer(TransferEncoding::Base64, b"SGVsbG8", &mut Vec::new()),
        b"Hello"
    );
}

#[test]
fn transfer_base64_skips_non_alphabet_bytes() {
    let mut defects = Vec::new();
    assert_eq!(
        decode_transfer(TransferEncoding::Base64, b"SGVs\r\nbG8=\n*!", &mut defects),
        b"Hello"
    );
    assert!(defects.contains(&Defect::MalformedBase64));
}

/// A stray character used to cost the WHOLE payload: the decoder padded to a
/// multiple of four and then let a strict decode fail into an empty vector.
#[test]
fn transfer_base64_keeps_the_payload_when_a_group_is_short() {
    let mut defects = Vec::new();
    assert_eq!(
        decode_transfer(TransferEncoding::Base64, b"SGVsbG8gd29ybGQhI", &mut defects),
        b"Hello world!"
    );
    assert!(defects.contains(&Defect::MalformedBase64));
}

#[test]
fn transfer_quoted_printable_soft_line_breaks() {
    assert_eq!(
        decode_transfer(
            TransferEncoding::QuotedPrintable,
            b"hel=\r\nlo",
            &mut Vec::new()
        ),
        b"hello"
    );
    assert_eq!(
        decode_transfer(
            TransferEncoding::QuotedPrintable,
            b"hel=\nlo",
            &mut Vec::new()
        ),
        b"hello"
    );
}

#[test]
fn transfer_quoted_printable_strips_trailing_whitespace() {
    assert_eq!(
        decode_transfer(
            TransferEncoding::QuotedPrintable,
            b"line one  \t\r\nline=20two",
            &mut Vec::new()
        ),
        b"line one\r\nline two"
    );
}

#[test]
fn transfer_quoted_printable_malformed_escape_is_literal() {
    let mut defects = Vec::new();
    assert_eq!(
        decode_transfer(TransferEncoding::QuotedPrintable, b"a=ZZb", &mut defects),
        b"a=ZZb"
    );
    assert!(defects.contains(&Defect::MalformedQuotedPrintable));
}

#[test]
fn transfer_unknown_encoding_passes_through_with_defect() {
    let mut defects = Vec::new();
    assert_eq!(
        decode_transfer(
            TransferEncoding::from_token("x-gzip64"),
            b"opaque",
            &mut defects
        ),
        b"opaque"
    );
    assert!(defects.contains(&Defect::UnknownTransferEncoding));
}
