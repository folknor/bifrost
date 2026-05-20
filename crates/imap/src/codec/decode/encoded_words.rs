//! RFC 2047 encoded-word decoder.

/// Decode RFC 2047 encoded words in a byte slice.
///
/// Handles `=?charset?encoding?text?=` sequences. Non-UTF-8 charsets are
/// lossy-converted to UTF-8 via `encoding_rs`.
pub(crate) fn decode_rfc2047(input: &[u8]) -> String {
    let s = String::from_utf8_lossy(input);
    decode_rfc2047_str(&s)
}

/// Decode encoded words in a string (RFC 2047 Section 2).
fn decode_rfc2047_str(input: &str) -> String {
    let mut result = String::new();
    let mut remaining = input;
    let mut last_was_encoded = false;

    while let Some(start) = remaining.find("=?") {
        let before = &remaining[..start];

        // RFC 2047 Section 6.2: whitespace between adjacent encoded words
        // is ignored  -  but ONLY when both adjacent tokens are valid encoded
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

            if candidate_has_valid_suffix {
                // RFC 2047 Section 5: decode only when the token is
                // separated from adjacent text by linear whitespace.
                // RFC 2047 Section 6.2: both adjacent words are valid,
                // so drop the inter-word whitespace.
                result.push_str(&decoded);
                last_was_encoded = true;
                continue;
            }
        }
        remaining = saved_after_prefix;

        // Not a valid encoded word  -  restore deferred whitespace before
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
    // Find charset
    let q1 = remaining.find('?')?;
    let charset_raw = &remaining[..q1];
    // RFC 2231 Section 5: charset may include "*language" suffix (e.g., "UTF-8*EN").
    // Strip the language tag if present.
    let charset = match charset_raw.find('*') {
        Some(pos) => &charset_raw[..pos],
        None => charset_raw,
    };
    *remaining = &remaining[q1 + 1..];

    // Find encoding
    let q2 = remaining.find('?')?;
    let encoding = &remaining[..q2];
    *remaining = &remaining[q2 + 1..];

    // Find encoded text (ends with ?=)
    let end = remaining.find("?=")?;
    let encoded_text = &remaining[..end];
    *remaining = &remaining[end + 2..];

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

    // Decode the payload
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
        // Use encoding_rs for other charsets
        let encoding = encoding_rs::Encoding::for_label(charset.as_bytes())?;
        // Use decode_without_bom_handling to preserve a leading U+FEFF if
        // it is genuinely part of the value rather than a BOM artefact.
        // RFC 2047 encoded words are header text fragments (Section 2),
        // not standalone documents, so stripping a leading FEFF would
        // corrupt legitimate content.
        let (cow, _) = encoding.decode_without_bom_handling(&raw_bytes);
        Some(cow.into_owned())
    }
}

/// Decode Q-encoding per RFC 2047 Section 4.2.
///
/// Handles `=XX` hex-encoded bytes, `_` as space. Also strips `=\r\n` / `=\n`
/// sequences as a Postel's-law leniency  -  RFC 2047 Section 4.2 Q-encoding does
/// NOT define soft line breaks (that is a Quoted-Printable concept from
/// RFC 2045 Section 6.7).
pub(super) fn decode_q_encoding(input: &str) -> Vec<u8> {
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
                    if let (Some(hi), Some(lo)) = (hex_digit(bytes[i + 1]), hex_digit(bytes[i + 2]))
                    {
                        result.push(hi << 4 | lo);
                        i += 3;
                    } else {
                        result.push(b'=');
                        i += 1;
                    }
                } else {
                    // Trailing '=' with only one char left  -  emit literally
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

/// Decode a single hex digit (RFC 2047 Section 4.2 Q-encoding).
pub(super) fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'A'..=b'F' => Some(b - b'A' + 10),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}
