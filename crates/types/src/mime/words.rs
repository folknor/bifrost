//! RFC 2047 encoded-word decoder.

/// Decode RFC 2047 encoded words in a byte slice.
///
/// Handles `=?charset?encoding?text?=` sequences. Non-UTF-8 charsets are
/// lossy-converted to UTF-8 via `encoding_rs`.
///
/// Decoding can expand: base64 contracts 4:3, but a legacy single-byte
/// charset can then triple, so a chain of words nets roughly 2.2x. Total
/// decoded output is bounded by [`DECODED_OUTPUT_LIMIT`]; past it, further
/// words are left verbatim (see [`DECODED_OUTPUT_LIMIT`] for why that is the
/// right degradation), so the returned string never exceeds that limit plus
/// the input length.
pub fn decode_encoded_words(input: &[u8]) -> String {
    let s = String::from_utf8_lossy(input);
    decode_rfc2047_str(&s)
}

/// Cap on the total bytes of DECODED encoded-word output one call may produce.
///
/// [`ENCODED_WORD_SCAN_LIMIT`] bounds a single word, but nothing bounded the
/// sum: a header of chained, individually in-window Shift_JIS words decodes to
/// roughly 2.2x its own size, and that scales with however large a header the
/// caller accepted. 64 KiB is orders of magnitude above any real header field
/// (RFC 5322 Section 2.1.1 caps one line at 998 octets, and even a heavily
/// folded recipient list of encoded display names stays in the low kilobytes),
/// so nothing legitimate reaches it.
///
/// Past the cap, remaining words are emitted verbatim rather than truncated.
/// That is the behavior RFC 2047 Section 6.3 already specifies for a word this
/// decoder will not decode, so the degradation reuses an existing, tested path
/// and loses no bytes - the reader sees `=?...?=` source text instead of a
/// silently shortened string.
const DECODED_OUTPUT_LIMIT: usize = 64 * 1024;

/// Decode encoded words in a string (RFC 2047 Section 2).
fn decode_rfc2047_str(input: &str) -> String {
    let mut result = String::new();
    let mut remaining = input;
    let mut last_was_encoded = false;
    let mut decoded_bytes = 0usize;

    while let Some(start) = remaining.find("=?") {
        let before = &remaining[..start];

        // RFC 2047 Section 6.2: whitespace between adjacent encoded words
        // is ignored - but ONLY when both adjacent tokens are valid encoded
        // words.  We must defer the decision to drop whitespace until after
        // we know whether the upcoming `=?...?=` token decodes successfully.
        // RFC 2047 Section 6.3: unrecognized encoded words are displayed as
        // ordinary text, so whitespace preceding them must be preserved.
        let ws_deferred = last_was_encoded && before.chars().all(|c| c == ' ' || c == '\t');
        if !ws_deferred {
            result.push_str(before);
        }

        let candidate_has_valid_prefix = before.is_empty() || before.ends_with([' ', '\t']);
        let saved_after_prefix = &remaining[start + 2..];
        remaining = saved_after_prefix;

        // Parse: charset?encoding?text?=
        if candidate_has_valid_prefix && let Some(decoded) = parse_encoded_word(&mut remaining) {
            let candidate_has_valid_suffix = match remaining.chars().next() {
                None => true,
                Some(c) => c == ' ' || c == '\t',
            };

            // Over the aggregate budget this word is treated as one this
            // decoder will not decode, so it falls through to the verbatim
            // path below (RFC 2047 Section 6.3).
            let within_budget = decoded_bytes.saturating_add(decoded.len()) <= DECODED_OUTPUT_LIMIT;
            if candidate_has_valid_suffix && within_budget {
                // RFC 2047 Section 5: decode only when the token is
                // separated from adjacent text by linear whitespace.
                // RFC 2047 Section 6.2: both adjacent words are valid,
                // so drop the inter-word whitespace.
                decoded_bytes += decoded.len();
                result.push_str(&decoded);
                last_was_encoded = true;
                continue;
            }
        }
        remaining = saved_after_prefix;

        // Not a valid encoded word - restore deferred whitespace before
        // emitting the literal `=?` prefix (RFC 2047 Section 6.3).
        if ws_deferred {
            result.push_str(before);
        }
        result.push_str("=?");
        last_was_encoded = false;
    }
    result.push_str(remaining);
    result
}

/// Parse a single encoded word after the `=?` prefix (RFC 2047 Section 2).
/// Advances `remaining` past the closing `?=` on success.
///
/// On failure, `remaining` is restored to its original position so the caller
/// can emit the `=?` prefix and full encoded-word text verbatim, per
/// RFC 2047 Section 6.3 ("Display of encoded words").
fn parse_encoded_word(remaining: &mut &str) -> Option<String> {
    // Save original position so we can restore on failure
    // (RFC 2047 Section 6.3: unrecognized encoded words displayed as-is)
    let saved = *remaining;

    let result = parse_encoded_word_inner(remaining);
    if result.is_none() {
        // Restore position so caller emits the full `=?...` text verbatim
        *remaining = saved;
    }
    result
}

/// Inner implementation of encoded-word parsing (RFC 2047 Section 2).
/// Separated so that `parse_encoded_word` can restore position on failure.
fn parse_encoded_word_inner(remaining: &mut &str) -> Option<String> {
    let input = *remaining;
    let window = encoded_word_window(input);

    // Find charset.
    let q1 = window.find('?')?;
    let charset_raw = &window[..q1];
    // RFC 2231 Section 5: charset may include "*language" suffix (e.g., "UTF-8*EN").
    // Strip the language tag if present.
    let charset = match charset_raw.find('*') {
        Some(pos) => &charset_raw[..pos],
        None => charset_raw,
    };
    // Find encoding.
    let after_charset = &window[q1 + 1..];
    let q2 = after_charset.find('?')?;
    let encoding = &after_charset[..q2];

    // Find encoded text (ends with ?=).
    let after_encoding = &after_charset[q2 + 1..];
    let end = after_encoding.find("?=")?;
    let encoded_text = &after_encoding[..end];

    // RFC 2047 Section 2: charset and encoding are required components.
    // The 75-character limit and non-empty encoded-text requirement are
    // ENCODER constraints; per Postel's law (be liberal in what you accept),
    // the decoder tolerates overlong words and empty payloads since many
    // real-world servers produce them.
    // Printable-ASCII validation on encoded-text is relaxed to only reject
    // the `?` character (which would break delimiter parsing).
    if charset.is_empty()
        || encoding.is_empty()
        || !encoded_text
            .bytes()
            .all(|b| (33..=126).contains(&b) && b != b'?')
    {
        return None;
    }

    // Advance only after every delimiter and payload check has succeeded.
    let consumed = q1 + 1 + q2 + 1 + end + 2;
    *remaining = &input[consumed..];

    // Decode the payload.
    let raw_bytes = match encoding.to_ascii_uppercase().as_str() {
        "B" => {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(encoded_text)
                .ok()?
        }
        "Q" => decode_q_encoding(encoded_text),
        _ => return None,
    };

    // Convert charset to UTF-8
    let charset_upper = charset.to_ascii_uppercase();
    if charset_upper == "UTF-8" || charset_upper == "US-ASCII" || charset_upper == "ASCII" {
        Some(String::from_utf8_lossy(&raw_bytes).into_owned())
    } else {
        super::charset::decode_charset_opt(charset, &raw_bytes)
    }
}

/// Hard cap on the bytes a single encoded-word candidate may scan.
///
/// RFC 2047 Section 2 caps an encoded word at 75 characters, but this decoder
/// deliberately accepts overlong words (many real servers emit them), so the
/// spec limit is not usable as the scan bound. RFC 5322 Section 2.1.1 caps a
/// header line at 998 octets, which is the largest window a conformant word
/// can occupy, and a constant bound is what keeps the outer loop linear: a
/// header made only of unterminated `=?` candidates would otherwise cost a
/// full-remainder scan per candidate.
const ENCODED_WORD_SCAN_LIMIT: usize = 998;

/// Return the prefix in which an RFC 2047 encoded word may occur: printable
/// non-space ASCII, capped at [`ENCODED_WORD_SCAN_LIMIT`]. RFC 2047 tokens
/// cannot cross whitespace, controls, DEL, or UTF-8, and bytes outside
/// `33..=126` are never inside a multi-byte `char`, so both bounds land on a
/// `char` boundary.
fn encoded_word_window(input: &str) -> &str {
    let len = input
        .bytes()
        .take(ENCODED_WORD_SCAN_LIMIT)
        .position(|b| !(33..=126).contains(&b))
        .unwrap_or_else(|| input.len().min(ENCODED_WORD_SCAN_LIMIT));
    &input[..len]
}

/// Decode Q-encoding per RFC 2047 Section 4.2.
///
/// Handles `=XX` hex-encoded bytes, `_` as space. Also strips `=\r\n` / `=\n`
/// sequences as a Postel's-law leniency - RFC 2047 Section 4.2 Q-encoding does
/// NOT define soft line breaks (that is a Quoted-Printable concept from
/// RFC 2045 Section 6.7).
fn decode_q_encoding(input: &str) -> Vec<u8> {
    let mut result = Vec::new();
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'=' if i + 1 < bytes.len() => {
                // Postel's law: strip =\r\n / =\n soft line breaks from
                // non-conformant encoders. RFC 2047 Section 4.2 Q-encoding
                // does not define soft line breaks; this is borrowed from
                // RFC 2045 Section 6.7 Quoted-Printable.
                if bytes[i + 1] == b'\r' && i + 2 < bytes.len() && bytes[i + 2] == b'\n' {
                    i += 3;
                } else if bytes[i + 1] == b'\n' {
                    i += 2;
                } else if i + 2 < bytes.len() {
                    // Hex-encoded byte
                    if let (Some(hi), Some(lo)) = (
                        super::charset::hex_digit(bytes[i + 1]),
                        super::charset::hex_digit(bytes[i + 2]),
                    ) {
                        result.push(hi << 4 | lo);
                        i += 3;
                    } else {
                        result.push(b'=');
                        i += 1;
                    }
                } else {
                    // Trailing '=' with only one char left - emit literally
                    result.push(b'=');
                    i += 1;
                }
            }
            b'_' => {
                // Underscore represents space in Q-encoding
                result.push(b' ');
                i += 1;
            }
            b => {
                result.push(b);
                i += 1;
            }
        }
    }
    result
}

#[cfg(test)]
#[path = "words_tests.rs"]
mod tests;
