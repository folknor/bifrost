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
//   * `DECODED_OUTPUT_LIMIT` bounds the *total*. Without it a header made of
//     many in-window words expanded linearly at above 2x for Shift_JIS
//     halfwidth katakana (one input octet becomes a three-octet UTF-8 char),
//     inheriting whatever cap the caller happened to put on the raw header.

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

/// Shift_JIS 0xA1 is U+FF61 (halfwidth ideographic full stop): one input octet
/// in, three UTF-8 octets out - the worst single-byte-charset ratio reachable
/// through base64's own 4:3 contraction. Each word stays inside the 998-byte
/// scan window, so the expansion comes from chaining, not from any one word.
fn shift_jis_word(raw_octets: usize) -> String {
    use base64::Engine;

    let payload = base64::engine::general_purpose::STANDARD.encode(vec![0xA1_u8; raw_octets]);
    let word = format!("=?Shift_JIS?B?{payload}?=");
    assert!(
        word.len() <= 998,
        "each word must stay inside the scan window"
    );
    word
}

/// Chained words still expand past their own input - that is inherent to the
/// encoding and is not what the cap exists to stop. Below the cap the decode
/// is exact.
#[test]
fn chained_encoded_words_expand_past_the_input_below_the_cap() {
    const RAW_OCTETS: usize = 735;
    const WORDS: usize = 10;

    let input = vec![shift_jis_word(RAW_OCTETS); WORDS].join(" ");
    let decoded = decode_encoded_words(input.as_bytes());

    // RFC 2047 Section 6.2: the separating whitespace between two valid words
    // is dropped, so the payloads concatenate with nothing in between.
    assert_eq!(decoded.chars().count(), RAW_OCTETS * WORDS);
    assert!(decoded.chars().all(|c| c == '\u{FF61}'));
    assert_eq!(decoded.len(), RAW_OCTETS * 3 * WORDS);
    assert!(
        decoded.len() > input.len() * 2,
        "the 2.2x expansion is real: {} decoded bytes from {} input bytes",
        decoded.len(),
        input.len()
    );
}

/// Past `DECODED_OUTPUT_LIMIT` the decoder stops decoding and emits the
/// remaining words verbatim (RFC 2047 Section 6.3), so output is bounded by
/// the cap plus the input rather than by the caller's header limit alone.
/// Nothing is truncated: the undecoded tail is still there as source text.
#[test]
fn chained_encoded_words_stop_decoding_at_the_output_cap() {
    const RAW_OCTETS: usize = 735;
    // Enough words to blow well past the 64 KiB cap: 735 * 3 bytes each.
    const WORDS: usize = 100;

    let word = shift_jis_word(RAW_OCTETS);
    let input = vec![word.as_str(); WORDS].join(" ");
    let decoded = decode_encoded_words(input.as_bytes());

    assert!(
        decoded.len() <= DECODED_OUTPUT_LIMIT + input.len(),
        "decoded {} bytes, past the cap plus input",
        decoded.len(),
    );
    // The decoded prefix is whole words: the cap is checked before a word is
    // appended, never mid-word.
    let decoded_chars = decoded.chars().take_while(|c| *c == '\u{FF61}').count();
    assert_eq!(decoded_chars % RAW_OCTETS, 0, "a word was cut in half");
    assert!(decoded_chars > 0, "words below the cap must still decode");
    assert!(
        decoded_chars < RAW_OCTETS * WORDS,
        "the cap must actually stop the chain"
    );
    // The undecoded tail rides through as verbatim source text.
    assert!(decoded.ends_with(&word), "the tail must survive verbatim");
}
