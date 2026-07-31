use super::*;

#[test]
fn decode_encoded_words_decodes_base64() {
    assert_eq!(decode_encoded_words(b"=?UTF-8?B?SGVsbG8=?="), "Hello");
}

#[test]
fn q_encoding_forms_decode_through_encoded_word() {
    assert_eq!(
        decode_encoded_words(b"=?UTF-8?Q?Hello_World?="),
        "Hello World"
    );
    assert_eq!(decode_encoded_words(b"=?UTF-8?Q?caf=C3=A9?="), "café");
}

// ===== Expansion bounds (imap-T4) =====
//
// The recorded concern was that a ~100 KB base64 payload in a legacy
// multi-byte charset expands several-fold with nothing capping the resulting
// subject. These tests pin what actually bounds the decoder today:
//
//   * `ENCODED_WORD_SCAN_LIMIT` bounds a *single* candidate word, so the
//     one-giant-word shape decodes to nothing at all.
//   * Nothing bounds the *total*. A header made of many in-window words
//     expands linearly, and the constant factor is above 2x for Shift_JIS
//     halfwidth katakana (one input octet becomes a three-octet UTF-8 char).
//     `decode_encoded_words` therefore inherits whatever cap its caller
//     applies to the raw header; it applies none of its own.

/// A single encoded word whose closing `?=` lies past `ENCODED_WORD_SCAN_LIMIT`
/// is never recognized, so a 100 KB one-word payload costs one bounded window
/// scan and produces zero decoded output - the header is echoed verbatim per
/// RFC 2047 Section 6.3.
#[test]
fn an_encoded_word_larger_than_the_scan_window_is_echoed_verbatim() {
    let payload = "QQ".repeat(60_000);
    let input = format!("=?Shift_JIS?B?{payload}?=");
    assert!(input.len() > 100_000);
    assert_eq!(decode_encoded_words(input.as_bytes()), input);
}

/// The exact scan boundary. The window starts *after* the `=?` sigil, so the
/// last byte of the closing `?=` must land within `ENCODED_WORD_SCAN_LIMIT`
/// bytes of that point: `"UTF-8?Q?"` (8) + text + `"?="` (2) <= 998.
#[test]
fn encoded_word_scan_window_boundary_is_exact() {
    let fits = format!("=?UTF-8?Q?{}?=", "A".repeat(988));
    assert_eq!(decode_encoded_words(fits.as_bytes()), "A".repeat(988));

    let one_over = format!("=?UTF-8?Q?{}?=", "A".repeat(989));
    assert_eq!(
        decode_encoded_words(one_over.as_bytes()),
        one_over,
        "one byte past the window and the word is not decoded at all"
    );
}

/// FINDING (gap, not fixed here - production changes are out of scope for
/// imap-T4): there is no cap on the *decoded* length. A ~100 KB header of
/// chained, individually in-window Shift_JIS words decodes to ~220 KB, and the
/// same construction scales linearly with header size. The only backstop is
/// whatever bounds the caller puts on the raw header bytes.
#[test]
fn chained_encoded_words_expand_past_the_input_with_no_output_cap() {
    use base64::Engine;

    // Shift_JIS 0xA1 is U+FF61 (halfwidth ideographic full stop): one input
    // octet in, three UTF-8 octets out - the worst single-byte-charset ratio
    // reachable through base64's own 4:3 contraction.
    const RAW_OCTETS: usize = 735;
    const WORDS: usize = 100;

    let payload = base64::engine::general_purpose::STANDARD.encode(vec![0xA1_u8; RAW_OCTETS]);
    let word = format!("=?Shift_JIS?B?{payload}?=");
    assert_eq!(
        word.len(),
        996,
        "each word must stay inside the 998-byte scan window"
    );

    let input = vec![word.as_str(); WORDS].join(" ");
    let decoded = decode_encoded_words(input.as_bytes());

    // RFC 2047 Section 6.2: the separating whitespace between two valid words
    // is dropped, so the payloads concatenate with nothing in between.
    assert_eq!(decoded.chars().count(), RAW_OCTETS * WORDS);
    assert!(decoded.chars().all(|c| c == '\u{FF61}'));
    assert_eq!(decoded.len(), RAW_OCTETS * 3 * WORDS);
    assert!(
        decoded.len() > input.len() * 2,
        "decoded {} bytes from {} input bytes; nothing caps this",
        decoded.len(),
        input.len()
    );
}
