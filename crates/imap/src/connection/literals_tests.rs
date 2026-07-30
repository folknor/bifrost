#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

// ---------------------------------------------------------------------------
// find_literal_boundary (RFC 3501 Section 4.3)
// ---------------------------------------------------------------------------

#[test]
fn boundary_finds_synchronizing_marker() {
    // `{5}\r\n` at offset 16..20 -> data starts at 21, five octets long.
    let buf = b"A1 APPEND INBOX {5}\r\nHELLO\r\n";
    assert_eq!(find_literal_boundary(buf), Some((21, 5)));
}

#[test]
fn boundary_ignores_non_synchronizing_marker() {
    // RFC 7888 Section 4: `{N+}` needs no continuation, so it is not a
    // send boundary.
    let buf = b"A1 APPEND INBOX {5+}\r\nHELLO\r\n";
    assert_eq!(find_literal_boundary(buf), None);
}

#[test]
fn boundary_none_without_marker() {
    assert_eq!(find_literal_boundary(b"A1 NOOP\r\n"), None);
}

#[test]
fn boundary_ignores_braces_without_digits() {
    // `{abc}` is ordinary text, not a literal marker.
    assert_eq!(find_literal_boundary(b"A1 LIST \"\" \"{abc}\"\r\n"), None);
}

#[test]
fn boundary_requires_crlf_immediately_after_brace() {
    // `{5}` followed by a space is not a literal marker.
    assert_eq!(find_literal_boundary(b"A1 X {5} Y\r\n"), None);
}

#[test]
fn boundary_zero_length_literal() {
    let buf = b"A1 X {0}\r\n\r\n";
    assert_eq!(find_literal_boundary(buf), Some((10, 0)));
}

#[test]
fn boundary_skips_markers_inside_a_non_synchronizing_literal_payload() {
    // `{10+}` is the real marker; `{3}` is opaque payload data.
    let buf = b"A1 APPEND INBOX {10+}\r\nabc{3}\r\ndef\r\n";
    assert_eq!(find_literal_boundary(buf), None);
}

// ---------------------------------------------------------------------------
// patch_literals_to_plus_with_binary (RFC 7888 Section 4)
// ---------------------------------------------------------------------------

#[test]
fn literal_plus_patches_every_classic_marker() {
    let out = patch_literals_to_plus_with_binary(b"A1 LOGIN {5}\r\nalice {3}\r\nbob\r\n", false);
    assert_eq!(&out[..], b"A1 LOGIN {5+}\r\nalice {3+}\r\nbob\r\n");
}

#[test]
fn literal_plus_skips_markers_inside_payload() {
    // Length-aware: the `{9}\r\n` inside the 7-octet payload stays intact.
    let out = patch_literals_to_plus_with_binary(b"A1 X {7}\r\n{9}\r\nab\r\n", false);
    assert_eq!(&out[..], b"A1 X {7+}\r\n{9}\r\nab\r\n");
}

#[test]
fn literal_plus_skips_payload_of_an_already_non_synchronizing_literal() {
    let out = patch_literals_to_plus_with_binary(b"A1 X {7+}\r\n{9}\r\nab\r\n", false);
    assert_eq!(&out[..], b"A1 X {7+}\r\n{9}\r\nab\r\n");
}

#[test]
fn literal_plus_leaves_literal8_synchronizing_without_binary() {
    // RFC 7888 Section 6: `~{N+}` requires BINARY alongside LITERAL+.
    let out = patch_literals_to_plus_with_binary(b"A1 APPEND INBOX ~{3}\r\nabc\r\n", false);
    assert_eq!(&out[..], b"A1 APPEND INBOX ~{3}\r\nabc\r\n");
}

#[test]
fn literal_plus_patches_literal8_with_binary() {
    let out = patch_literals_to_plus_with_binary(b"A1 APPEND INBOX ~{3}\r\nabc\r\n", true);
    assert_eq!(&out[..], b"A1 APPEND INBOX ~{3+}\r\nabc\r\n");
}

#[test]
fn literal_plus_leaves_non_marker_braces_alone() {
    let out = patch_literals_to_plus_with_binary(b"A1 LIST \"\" \"{abc}\"\r\n", false);
    assert_eq!(&out[..], b"A1 LIST \"\" \"{abc}\"\r\n");
}

#[test]
fn literal_plus_is_identity_without_literals() {
    let out = patch_literals_to_plus_with_binary(b"A1 NOOP\r\n", true);
    assert_eq!(&out[..], b"A1 NOOP\r\n");
}

#[test]
fn literal_plus_clamps_oversized_declared_body() {
    // A declared size past the end of the buffer must not panic; the
    // copy clamps at `buf.len()`.
    let out = patch_literals_to_plus_with_binary(b"A1 X {9999}\r\nab", false);
    assert_eq!(&out[..], b"A1 X {9999+}\r\nab");
}

// ---------------------------------------------------------------------------
// patch_small_literals_to_plus_with_binary (RFC 7888 Section 5)
// ---------------------------------------------------------------------------

#[test]
fn literal_minus_patches_small_literal() {
    let out = patch_small_literals_to_plus_with_binary(b"A1 X {5}\r\nHELLO\r\n", false);
    assert_eq!(&out[..], b"A1 X {5+}\r\nHELLO\r\n");
}

#[test]
fn literal_minus_patches_at_the_4096_boundary() {
    let out = patch_small_literals_to_plus_with_binary(b"A1 X {4096}\r\n", false);
    assert_eq!(&out[..], b"A1 X {4096+}\r\n");
}

#[test]
fn literal_minus_leaves_oversized_literal_synchronizing() {
    // RFC 7888 Section 5 caps the non-synchronizing form at 4096 octets.
    let out = patch_small_literals_to_plus_with_binary(b"A1 X {4097}\r\n", false);
    assert_eq!(&out[..], b"A1 X {4097}\r\n");
}

#[test]
fn literal_minus_leaves_literal8_synchronizing_without_binary() {
    let out = patch_small_literals_to_plus_with_binary(b"A1 APPEND INBOX ~{3}\r\nabc\r\n", false);
    assert_eq!(&out[..], b"A1 APPEND INBOX ~{3}\r\nabc\r\n");
}

#[test]
fn literal_minus_patches_small_literal8_with_binary() {
    let out = patch_small_literals_to_plus_with_binary(b"A1 APPEND INBOX ~{3}\r\nabc\r\n", true);
    assert_eq!(&out[..], b"A1 APPEND INBOX ~{3+}\r\nabc\r\n");
}

#[test]
fn literal_minus_skips_markers_inside_payload() {
    let out = patch_small_literals_to_plus_with_binary(b"A1 X {7}\r\n{9}\r\nab\r\n", false);
    assert_eq!(&out[..], b"A1 X {7+}\r\n{9}\r\nab\r\n");
}

// ---------------------------------------------------------------------------
// AppendLiteralKind
// ---------------------------------------------------------------------------

#[test]
fn append_literal_kinds_are_distinct() {
    assert_ne!(AppendLiteralKind::Literal, AppendLiteralKind::Literal8);
    assert_ne!(AppendLiteralKind::Literal8, AppendLiteralKind::Utf8Literal8);
    assert_eq!(AppendLiteralKind::Literal, AppendLiteralKind::Literal);
}
