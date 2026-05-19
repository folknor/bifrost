#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

fn p(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|&(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

// --- Plain passthrough ---

#[test]
fn plain_passthrough() {
    let params = p(&[("charset", "utf-8"), ("name", "file.txt")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result, p(&[("charset", "utf-8"), ("name", "file.txt")]));
}

// --- Standalone charset-encoded ---

#[test]
fn standalone_charset_encoded() {
    // RFC 2231 Section 4 example: title*=us-ascii'en-us'This%20is%20fun
    let params = p(&[("title*", "us-ascii'en-us'This%20is%20fun")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "title");
    assert_eq!(result[0].1, "This is fun");
}

// --- Continuation reassembly ---

#[test]
fn continuation_reassembly() {
    // RFC 2231 Section 3: name*0="first"; name*1="second"
    let params = p(&[("name*0", "first"), ("name*1", "second")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "name");
    assert_eq!(result[0].1, "firstsecond");
}

// --- Combined charset + continuation ---

#[test]
fn charset_continuation_combined() {
    // RFC 2231 combined: first segment has charset, subsequent are percent-encoded.
    // "Language and character set information only appear at the beginning"
    let params = p(&[
        ("title*0*", "us-ascii'en'This%20is"),
        ("title*1*", "%20fun"),
    ]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "title");
    assert_eq!(result[0].1, "This is fun");
}

// --- Out-of-order indices ---

#[test]
fn out_of_order_indices() {
    let params = p(&[("name*1", "second"), ("name*0", "first")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "name");
    // BTreeMap orders by key, so segments are reassembled in order.
    assert_eq!(result[0].1, "firstsecond");
}

// --- Non-UTF-8 charset (ISO-8859-1) ---

#[test]
fn non_utf8_charset_iso8859_1() {
    // ISO-8859-1: 0xe9 = 'é'
    let params = p(&[("title*", "iso-8859-1'en'caf%E9")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "title");
    assert_eq!(result[0].1, "café");
}

// --- Unknown charset (lossy fallback) ---

#[test]
fn unknown_charset_lossy_fallback() {
    let params = p(&[("title*", "x-nonexistent'en'hello%20world")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "title");
    assert_eq!(result[0].1, "hello world");
}

// --- Mixed plain + encoded ordering ---

#[test]
fn mixed_plain_and_encoded_ordering() {
    let params = p(&[
        ("charset", "utf-8"),
        ("name*0", "long"),
        ("name*1", "file.txt"),
        ("disposition", "inline"),
    ]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].0, "charset");
    assert_eq!(result[0].1, "utf-8");
    // Continuation group appears at position of first segment.
    assert_eq!(result[1].0, "name");
    assert_eq!(result[1].1, "longfile.txt");
    assert_eq!(result[2].0, "disposition");
    assert_eq!(result[2].1, "inline");
}

// --- Charset from non-first encoded section ---

/// RFC 2231 Section 3: when section 0 is plain (non-encoded) and a later
/// section is the first charset-encoded one, the charset must be extracted
/// from that later section.  Previously, charset extraction only triggered
/// for section 0 (via `first_encoded`), so a plain section 0 caused the
/// charset prefix to be included verbatim in the decoded value.
#[test]
fn charset_from_non_first_encoded_section() {
    // Section *0 is plain, section *1* is the first encoded section.
    let params = p(&[("name*0", "hello "), ("name*1*", "UTF-8''w%C3%B6rld.txt")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "name");
    assert_eq!(
        result[0].1, "hello wörld.txt",
        "RFC 2231 Section 3: charset from the first encoded section (not section 0) \
             must be used to decode percent-encoded bytes"
    );
}

// --- Empty params ---

#[test]
fn empty_params() {
    let params: Vec<(String, String)> = Vec::new();
    let result = decode_rfc2231_params(&params);
    assert!(result.is_empty());
}

// --- Missing language tag ---

#[test]
fn missing_language_tag() {
    // charset present, language empty: charset''encoded
    let params = p(&[("title*", "utf-8''hello%20world")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "title");
    assert_eq!(result[0].1, "hello world");
}

// --- Malformed value (graceful passthrough) ---

#[test]
fn malformed_value_no_quotes() {
    // No single quotes  -  graceful fallback: value returned as-is.
    let params = p(&[("title*", "just-some-value")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "title");
    assert_eq!(result[0].1, "just-some-value");
}

// --- Case-insensitive key grouping ---

#[test]
fn case_insensitive_key_grouping() {
    let params = p(&[("Name*0", "hello"), ("NAME*1", " world")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    // Base name uses the case from the first occurrence.
    assert_eq!(result[0].0, "Name");
    assert_eq!(result[0].1, "hello world");
}

// --- percent_decode unit tests ---

#[test]
fn percent_decode_basic() {
    assert_eq!(percent_decode("hello%20world"), b"hello world");
    assert_eq!(percent_decode("%2A%2A%2A"), b"***");
    assert_eq!(percent_decode("no-encoding"), b"no-encoding");
}

#[test]
fn percent_decode_truncated_sequence() {
    // Truncated % at end  -  pass through.
    assert_eq!(percent_decode("abc%2"), b"abc%2");
    assert_eq!(percent_decode("abc%"), b"abc%");
}

#[test]
fn percent_decode_invalid_hex() {
    // Invalid hex chars  -  pass through.
    assert_eq!(percent_decode("%GG"), b"%GG");
}

// --- Edge cases ---

#[test]
fn continuation_missing_segment_0() {
    // Only segment 1 exists  -  no segment 0. RFC 2231 Section 3 requires
    // continuations to start at 0, so the malformed group is skipped.
    let params = p(&[("name*1", "world")]);
    let result = decode_rfc2231_params(&params);
    assert!(result.is_empty());
}

#[test]
fn continuation_requires_segment_zero() {
    let params = p(&[("name*1", "world")]);
    let result = decode_rfc2231_params(&params);
    assert!(
        result.iter().all(|(key, _)| key != "name"),
        "RFC 2231 Section 3: continuation groups without section 0 must not be reassembled"
    );
}

#[test]
fn continuation_stops_at_first_gap() {
    let params = p(&[("name*0", "first"), ("name*2", "third")]);
    let result = decode_rfc2231_params(&params);
    let name_val = result
        .iter()
        .find(|(key, _)| key == "name")
        .map(|(_, value)| value.as_str());
    assert_eq!(
        name_val,
        Some("first"),
        "RFC 2231 Section 3: continuation reassembly must stop at the first missing index"
    );
}

#[test]
fn continuation_mixed_encoded_plain() {
    // Segment 0 is charset-encoded, segment 1 is plain (unencoded).
    // RFC 2231 Section 4: "Language and character set information only appear
    // at the beginning of a given parameter value."
    // Charset from segment 0 applies to the whole reassembled value.
    let params = p(&[("name*0*", "utf-8'en'caf%C3%A9"), ("name*1", ".txt")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "name");
    assert_eq!(result[0].1, "café.txt");
}

#[test]
fn standalone_empty_charset_empty_language() {
    // Both charset and language empty: `title*=''hello%20world`
    // split_charset_value returns charset=None, so lossy UTF-8 applies.
    let params = p(&[("title*", "''hello%20world")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "title");
    assert_eq!(result[0].1, "hello world");
}

#[test]
fn empty_base_name_key() {
    // Key is `*0`  -  empty base name. classify_key extracts base_name="".
    // Should not panic; produces a parameter with empty key name.
    let params = p(&[("*0", "value")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "");
    assert_eq!(result[0].1, "value");
}

// --- L10: RFC 2231 continuation gap detection ---

#[test]
fn continuation_gap_assembles_all_segments() {
    // RFC 2231 Section 3 forbids gaps. Keep only the contiguous prefix
    // beginning at section 0.
    let params = p(&[("name*0", "first"), ("name*2", "third")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "name");
    assert_eq!(result[0].1, "first");
}

#[test]
fn continuation_gap_mid_sequence_assembles_all() {
    // Stop once section 2 is missing instead of stitching section 3 on.
    let params = p(&[("f*0", "A"), ("f*1", "B"), ("f*3", "D")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1, "AB");
}

#[test]
fn continuation_no_gap() {
    // Contiguous segments  -  no gap, straightforward reassembly.
    let params = p(&[("f*0", "A"), ("f*1", "B"), ("f*2", "C")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1, "ABC");
}

// --- Postel's law: tolerate non-contiguous continuation indices ---

#[test]
fn continuation_gap_tolerates_non_contiguous_indices() {
    // RFC 2231 Section 3: a missing section terminates the usable prefix.
    let params = p(&[("name*0", "first"), ("name*2", "third")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "name");
    assert_eq!(result[0].1, "first");
}

#[test]
fn continuation_gap_mid_sequence_tolerates() {
    // Segments 0, 1, 3 present but 2 missing. Stop at the gap.
    let params = p(&[("f*0", "A"), ("f*1", "B"), ("f*3", "D")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1, "AB");
}

// --- L11: RFC 2047 encoded-word fallback in BODYSTRUCTURE parameters ---

#[test]
fn rfc2047_encoded_word_fallback_base64() {
    // Some servers put RFC 2047 encoded words in parameter values instead
    // of using RFC 2231. Verify the fallback decodes them.
    // "test.txt" in base64 = "dGVzdC50eHQ="
    let params = p(&[("filename", "=?UTF-8?B?dGVzdC50eHQ=?=")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "filename");
    assert_eq!(result[0].1, "test.txt");
}

#[test]
fn rfc2047_encoded_word_fallback_quoted_printable() {
    // RFC 2047 Q-encoding: =?UTF-8?Q?caf=C3=A9.txt?=
    let params = p(&[("filename", "=?UTF-8?Q?caf=C3=A9.txt?=")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "filename");
    assert_eq!(result[0].1, "caf\u{e9}.txt");
}

#[test]
fn rfc2047_fallback_does_not_corrupt_plain_values() {
    // Plain values without =? ... ?= markers must be left untouched.
    let params = p(&[("filename", "report.pdf")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result[0].1, "report.pdf");
}

// --- L12: decode_without_bom_handling preserves leading U+FEFF ---

#[test]
fn non_utf8_charset_preserves_leading_feff() {
    // U+FEFF encoded in UTF-16LE is 0xFF 0xFE. When present as genuine
    // content (not a BOM), it must be preserved.
    // Build: iso-8859-1 value with byte 0xEF 0xBB 0xBF (UTF-8 BOM) should
    // NOT be stripped  -  decode_without_bom_handling preserves it.
    //
    // We use windows-1252 which maps bytes 1:1 for 0x00-0xFF.
    // Byte 0xC0 in windows-1252 = U+00C0 (À).
    // We verify a BOM-like prefix (0xEF 0xBB 0xBF) is preserved when
    // re-decoded from windows-1252.
    let bom_bytes_hex = "%EF%BB%BF%C0"; // 0xEF 0xBB 0xBF 0xC0
    let params = p(&[("title*", &format!("windows-1252''{bom_bytes_hex}"))]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    // In windows-1252: 0xEF=ï, 0xBB=», 0xBF=¿, 0xC0=À
    // With decode_without_bom_handling, all bytes are preserved.
    assert_eq!(result[0].1, "ï»¿À");
}

// --- Issue #10: RFC 2231-decoded values must not be double-decoded as RFC 2047 ---

#[test]
fn rfc2231_decoded_value_not_double_decoded_as_rfc2047() {
    // A value that literally contains =?...?= after RFC 2231 decoding
    // should NOT be re-decoded by the RFC 2047 fallback.
    // "=?UTF-8?B?dGVzdA==?=" percent-encoded: %3D%3FUTF-8%3FB%3FdGVzdA%3D%3D%3F%3D
    let params = p(&[("name*", "utf-8''%3D%3FUTF-8%3FB%3FdGVzdA%3D%3D%3F%3D")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "name");
    // Should be the literal string, NOT "test"
    assert_eq!(result[0].1, "=?UTF-8?B?dGVzdA==?=");
}

// --- Issue #11: Duplicate continuation indices must keep first value ---

#[test]
fn duplicate_continuation_index_keeps_first() {
    // RFC 2231 Section 3: each index appears exactly once.
    // When duplicates exist, first value should win.
    let params = p(&[("name*0", "correct"), ("name*0", "wrong")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "name");
    assert_eq!(result[0].1, "correct"); // First value should win
}

// --- RFC 2231 Section 5: encoded params override plain duplicates ---

#[test]
fn rfc2231_section5_encoded_overrides_plain_duplicate() {
    // RFC 2231 Section 5: when both `name` (plain) and `name*` (charset-
    // encoded) exist, the RFC 2231 form takes precedence. The plain form
    // is a fallback for clients that don't understand RFC 2231.
    let params = p(&[
        ("name", "fallback.txt"),
        ("name*", "utf-8''encoded%2Dname.txt"),
    ]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(
        result,
        p(&[("name", "encoded-name.txt")]),
        "RFC 2231 Section 5: encoded form should override plain duplicate"
    );
}

#[test]
fn rfc2231_section5_plain_without_encoded_kept() {
    // Plain parameter without any RFC 2231 version must be kept as-is.
    let params = p(&[("name", "plain.txt")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result, p(&[("name", "plain.txt")]));
}

#[test]
fn rfc2231_section5_encoded_without_plain_kept() {
    // RFC 2231 parameter without any plain version must be kept as-is.
    let params = p(&[("name*", "utf-8''encoded.txt")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result, p(&[("name", "encoded.txt")]));
}

#[test]
fn rfc2231_section5_case_insensitive_dedup() {
    // RFC 2231 Section 5: deduplication should be case-insensitive.
    let params = p(&[("Name", "fallback.txt"), ("NAME*", "utf-8''encoded.txt")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(
        result.len(),
        1,
        "RFC 2231 Section 5: case-insensitive dedup should produce one entry; got {result:?}"
    );
    assert_eq!(result[0].1, "encoded.txt");
}

#[test]
fn rfc2231_section5_continuation_overrides_plain() {
    // RFC 2231 Section 5: continuation groups (`name*0*=...`, `name*1=...`)
    // also override a plain `name` parameter.
    let params = p(&[
        ("name", "fallback.txt"),
        ("name*0*", "utf-8''encoded"),
        ("name*1", ".txt"),
    ]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(
        result.len(),
        1,
        "RFC 2231 Section 5: continuation group should override plain; got {result:?}"
    );
    assert_eq!(result[0].1, "encoded.txt");
}

#[test]
fn rfc2231_section5_plain_before_encoded_preserves_order() {
    // When the plain entry appears before the encoded form, the encoded
    // form should replace it and unrelated params keep their positions.
    let params = p(&[
        ("charset", "utf-8"),
        ("name", "fallback.txt"),
        ("name*", "utf-8''real%2Dname.txt"),
        ("disposition", "attachment"),
    ]);
    let result = decode_rfc2231_params(&params);
    // "charset" and "disposition" must be kept; only one "name" entry.
    assert_eq!(result.len(), 3);
    assert_eq!(result[0], ("charset".to_owned(), "utf-8".to_owned()));
    assert_eq!(
        result[2],
        ("disposition".to_owned(), "attachment".to_owned())
    );
    // The sole "name" entry should be the RFC 2231 decoded value.
    let name_entries: Vec<_> = result.iter().filter(|(k, _)| k == "name").collect();
    assert_eq!(name_entries.len(), 1);
    assert_eq!(name_entries[0].1, "real-name.txt");
}

// --- split_charset_value: single-quote edge cases ---

#[test]
fn split_charset_value_one_quote_only() {
    // RFC 2231 Section 4: malformed value with only one single-quote.
    // split_charset_value should return (None, raw_bytes) as a graceful fallback.
    let (cs, bytes) = split_charset_value("utf-8'hello");
    assert!(cs.is_none());
    assert_eq!(bytes, b"utf-8'hello");
}

// --- hex_val: lowercase hex digits ---

#[test]
fn percent_decode_lowercase_hex() {
    // RFC 2231 Section 4: ext-octet = "%" 2(DIGIT / "A"..."F").
    // Real-world servers may emit lowercase hex digits; Postel's law says
    // we should accept them.
    assert_eq!(percent_decode("%2a%2b%2c"), b"*+,");
    assert_eq!(percent_decode("caf%c3%a9"), "café".as_bytes());
}

// --- find_original_base_name: fallback when no key has a '*' matching lower_name ---

#[test]
fn find_original_base_name_fallback_to_lowercase() {
    // When no key in the params list has a '*' with a matching base name,
    // the function falls back to returning the lowercase name.
    let params = vec![
        ("plain_key".to_owned(), "value".to_owned()),
        ("another".to_owned(), "value2".to_owned()),
    ];
    let result = find_original_base_name(&params, "nonexistent");
    assert_eq!(result, "nonexistent");
}

#[test]
fn find_original_base_name_no_star_keys() {
    // Params with no '*' in any key  -  should hit the fallback path.
    let params = vec![("charset".to_owned(), "utf-8".to_owned())];
    let result = find_original_base_name(&params, "charset");
    assert_eq!(result, "charset");
}

// --- standalone charset-encoded value with no encoded parts ---

#[test]
fn standalone_charset_no_encoded_bytes() {
    // RFC 2231 Section 4: `name*=charset'lang'value` where value has no
    // percent-encoded parts  -  the value is plain ASCII.
    let params = p(&[("filename*", "us-ascii'en'plain-text-file.txt")]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "filename");
    assert_eq!(result[0].1, "plain-text-file.txt");
}

// --- Postel's law: gap and leading-zero tolerance ---

/// RFC 2231 Section 3 forbids gaps in continuation indices. Preserve only
/// the contiguous prefix before the first missing section.
#[test]
fn continuation_gap_tolerance() {
    let params = vec![
        ("name*0".to_string(), "part0".to_string()),
        ("name*2".to_string(), "part2".to_string()),
    ];
    let result = decode_rfc2231_params(&params);
    let name_val = result
        .iter()
        .find(|(k, _)| k == "name")
        .map(|(_, v)| v.as_str());
    assert_eq!(
        name_val,
        Some("part0"),
        "RFC 2231 Section 3: reassembly must stop at the first gap"
    );
}

/// RFC 2231 Section 3 forbids leading zeroes in continuation indices.
/// Verify the parser rejects them: `name*00` and `name*01` are not
/// recognized as continuation segments and fall through as plain
/// parameters rather than being reassembled.
#[test]
fn continuation_leading_zeros_rejected() {
    let params = vec![
        ("name*00".to_string(), "part0".to_string()),
        ("name*01".to_string(), "part1".to_string()),
    ];
    let result = decode_rfc2231_params(&params);
    // Leading-zero keys are rejected by classify_key(), so they are
    // treated as plain (non-RFC-2231) parameters and passed through
    // unchanged  -  they should NOT be reassembled into "name".
    let reassembled = result.iter().any(|(k, v)| k == "name" && v == "part0part1");
    assert!(
        !reassembled,
        "leading zeros in continuation indices (*00, *01) must be rejected \
             per RFC 2231 Section 3, not silently normalized; got {result:?}"
    );
}

// ===== Spec audit: prior deviations =====

#[test]
fn spec_audit_m6_leading_zeroes_in_continuation_rejected() {
    // RFC 2231 Section 3/7: "neither leading zeroes nor gaps in the
    // sequence are allowed."
    //
    // Keys with leading zeroes like `name*00` and `name*01` must be
    // rejected. The classify_key() function validates this at line 259
    // by checking `digits.len() > 1 && digits.starts_with('0')`.
    let params = p(&[("name*00", "first"), ("name*01", "second")]);
    let result = decode_rfc2231_params(&params);

    // If leading zeroes were properly rejected, these segments would NOT
    // be reassembled into a single "name" parameter with value "firstsecond".
    let has_reassembled = result
        .iter()
        .any(|(k, v)| k == "name" && v == "firstsecond");
    assert!(
        !has_reassembled,
        "Leading zeroes in continuation indices (*00, *01) should be rejected \
             per RFC 2231 Section 3, not silently normalized; got {result:?}"
    );
}

#[test]
fn spec_audit_m6_leading_zero_collision() {
    // RFC 2231 Section 3/7: "neither leading zeroes nor gaps in the
    // sequence are allowed."
    //
    // When both `name*0` (valid) and `name*00` (invalid, leading zero)
    // are present, the valid form `*0` must win. The classify_key()
    // function rejects `*00` (leading zero), so only `*0` is accepted.
    let params = p(&[("name*0", "correct"), ("name*00", "wrong")]);
    let result = decode_rfc2231_params(&params);
    let name_value = result.iter().find(|(k, _)| k == "name");
    assert!(
        name_value.is_some(),
        "Expected a 'name' parameter in the result"
    );
    assert_eq!(
        name_value.unwrap().1,
        "correct",
        "name*0 (valid) should take precedence over name*00 (leading zero); \
             got {result:?}"
    );
}

// ===== Edge-case bug probe tests =====

/// RFC 2231 Section 3: Continuation parameters with `*0`, `*1` suffixes
/// must be reassembled in index order into a single parameter value.
/// This verifies that three segments are concatenated correctly and the
/// original base name is preserved.
#[test]
fn edge_continuation_three_segments_reassembled() {
    let params = p(&[
        ("name*0", "first_"),
        ("name*1", "second_"),
        ("name*2", "third"),
    ]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(
        result.len(),
        1,
        "three continuation segments must reassemble into one parameter; got {result:?}"
    );
    assert_eq!(
        result[0].0, "name",
        "reassembled param key must be the base name (RFC 2231 Section 3)"
    );
    assert_eq!(
        result[0].1, "first_second_third",
        "continuation segments must be concatenated in index order \
             (RFC 2231 Section 3)"
    );
}

/// RFC 2231 Sections 3-4: Continuation parameters with charset encoding
/// on the first segment (`*0*`) must decode the charset prefix and
/// percent-encoded bytes, then concatenate with subsequent segments.
#[test]
fn edge_continuation_with_charset_encoding() {
    let params = p(&[
        ("filename*0*", "UTF-8''caf%C3%A9_"),
        ("filename*1", "report.pdf"),
    ]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(
        result.len(),
        1,
        "charset-encoded continuation must reassemble into one parameter; got {result:?}"
    );
    assert_eq!(result[0].0, "filename");
    assert_eq!(
        result[0].1, "caf\u{e9}_report.pdf",
        "first segment must be charset-decoded, then concatenated with \
             subsequent plain segments (RFC 2231 Sections 3-4)"
    );
}

/// RFC 2231 Section 3: Continuation parameters mixed with plain parameters.
/// Plain parameters must pass through unchanged while continuations are
/// reassembled. The output order must place the reassembled parameter at
/// the position of its first segment.
#[test]
fn edge_continuation_mixed_with_plain_params() {
    let params = p(&[
        ("charset", "utf-8"),
        ("name*0", "long_"),
        ("name*1", "value.txt"),
        ("format", "flowed"),
    ]);
    let result = decode_rfc2231_params(&params);
    assert_eq!(
        result.len(),
        3,
        "one plain + one reassembled + one plain = 3 params; got {result:?}"
    );
    assert_eq!(result[0], ("charset".to_string(), "utf-8".to_string()));
    assert_eq!(
        result[1],
        ("name".to_string(), "long_value.txt".to_string()),
        "continuation must be reassembled at the position of its first segment"
    );
    assert_eq!(result[2], ("format".to_string(), "flowed".to_string()));
}
