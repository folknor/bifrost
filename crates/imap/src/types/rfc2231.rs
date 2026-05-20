//! RFC 2231 MIME parameter decoding (continuations and charset-encoded values).
//!
//! RFC 2231 extends MIME parameter handling (RFC 2045) with three mechanisms:
//! - **Section 3 (Continuations):** Long parameter values split across multiple
//!   segments: `name*0="part1"; name*1="part2"`. "The count starts at 0 and
//!   increments by 1 for each subsequent section."
//! - **Section 4 (Charset/Language):** Charset-encoded values using
//!   `parameter*=charset'language'percent-encoded`. The ABNF is:
//!   `extended-initial-value = [charset] "'" [language] "'" extended-other-values`
//!   `ext-octet = "%" 2(DIGIT / "A" / "B" / "C" / "D" / "E" / "F")`
//! - **Combined:** Continuations with charset encoding on the first segment:
//!   `name*0*=charset'language'part1; name*1*=part2`
//!   "Language and character set information only appear at the beginning of a
//!   given parameter value."

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};

/// Decode RFC 2231 continuation and charset-encoded parameters.
///
/// Takes raw MIME parameters (as produced by the BODYSTRUCTURE parser) and returns
/// decoded parameters with continuations reassembled and charset-encoded values
/// converted to UTF-8.
///
/// RFC 2231 Section 3 (Continuations):
/// `name*0="first_part"; name*1="second_part"` -> `name="first_partsecond_part"`
/// "The count starts at 0 and increments by 1 for each subsequent section."
///
/// RFC 2231 Section 4 (Charset/Language encoding):
/// `name*=charset'language'percent-encoded` -> `name="decoded_value"`
/// `extended-initial-value = [charset] "'" [language] "'" extended-other-values`
///
/// Non-continuation parameters are passed through unchanged. Continuation groups
/// appear at the position of their first segment.
#[allow(clippy::too_many_lines)]
pub(crate) fn decode_rfc2231_params(params: &[(String, String)]) -> Vec<(String, String)> {
    // Phase 1: classify each parameter.
    //
    // We track three categories:
    // - Plain: no RFC 2231 markers, pass through unchanged.
    // - Standalone charset-encoded: `name*=charset'lang'value` (no digit index).
    // - Continuation: `name*N` or `name*N*` where N is a decimal index.
    //
    // For continuations we group segments by base name (case-insensitive) and
    // record the original insertion position of the first segment seen.

    /// A continuation group: (lowercase base name, result index, segments).
    type ContinuationGroup = (String, usize, BTreeMap<u32, (String, bool)>);

    let mut result: Vec<Option<(String, String)>> = Vec::with_capacity(params.len());
    let mut continuations: Vec<ContinuationGroup> = Vec::new();
    // Track indices of values decoded via RFC 2231 charset mechanisms so the
    // RFC 2047 fallback (which handles non-conformant servers) does not
    // double-decode them. RFC 2047 Section 5 / RFC 2231 Section 4.
    let mut rfc2231_decoded: HashSet<usize> = HashSet::new();

    for (key, value) in params {
        if let Some(classification) = classify_key(key) {
            match classification {
                KeyClass::StandaloneEncoded { base_name } => {
                    // RFC 2231 Section 4: standalone `name*=charset'lang'encoded`
                    let decoded = decode_charset_value(value);
                    rfc2231_decoded.insert(result.len());
                    result.push(Some((base_name, decoded)));
                }
                KeyClass::Continuation {
                    base_name,
                    index,
                    encoded,
                } => {
                    let lower = base_name.to_ascii_lowercase();
                    // Find or create the continuation group.
                    let group = continuations.iter_mut().find(|(name, _, _)| *name == lower);
                    if let Some((_, _, segments)) = group {
                        // RFC 2231 Section 3: each index appears exactly once.
                        // Keep the first value if a duplicate index is encountered.
                        match segments.entry(index) {
                            Entry::Vacant(e) => {
                                e.insert((value.clone(), encoded));
                            }
                            Entry::Occupied(_) => {
                                tracing::warn!(
                                    base_name = lower.as_str(),
                                    index = index,
                                    "RFC 2231 Section 3: duplicate continuation index {}, keeping first value",
                                    index,
                                );
                            }
                        }
                    } else {
                        let insert_pos = result.len();
                        // Reserve a slot in the result vector.
                        result.push(None);
                        let mut segments = BTreeMap::new();
                        segments.insert(index, (value.clone(), encoded));
                        continuations.push((lower, insert_pos, segments));
                    }
                }
            }
        } else {
            // Plain parameter  -  pass through unchanged.
            result.push(Some((key.clone(), value.clone())));
        }
    }

    // Phase 2: reassemble continuation groups.
    for (lower_name, insert_pos, segments) in continuations {
        // RFC 2231 Section 3: continuation counts start at 0.
        // Without section 0, there is no contiguous prefix to decode.
        let Some((&0, _)) = segments.first_key_value() else {
            continue;
        };

        // Determine charset from the first *encoded* segment.
        // RFC 2231 Section 3: "Language and character set information only
        // appear at the beginning of a given parameter value."  The charset
        // comes from the first encoded section, which may not be section 0
        // if earlier sections are plain (non-encoded).

        let mut charset: Option<String> = None;
        let mut raw_bytes = Vec::new();
        let mut expected_idx: u32 = 0;

        for (idx, (value, is_encoded)) in &segments {
            // RFC 2231 Section 3: neither leading zeroes nor gaps are allowed.
            // Preserve only the contiguous prefix so malformed trailing
            // fragments do not get stitched into a fabricated value.
            if *idx != expected_idx {
                tracing::warn!(
                    base_name = lower_name.as_str(),
                    expected = expected_idx,
                    actual = *idx,
                    "RFC 2231 Section 3: stopping continuation reassembly at gap \
                     (expected index {}, found {})",
                    expected_idx,
                    idx,
                );
                break;
            }
            expected_idx = idx + 1;

            if *is_encoded && charset.is_none() {
                // First encoded segment: parse charset'language'encoded.
                let (cs, bytes) = split_charset_value(value);
                charset = cs;
                raw_bytes.extend_from_slice(&bytes);
            } else if *is_encoded {
                // Subsequent encoded segments: just percent-decode (no charset prefix).
                raw_bytes.extend_from_slice(&percent_decode(value));
            } else {
                // Unencoded segment  -  raw bytes.
                raw_bytes.extend_from_slice(value.as_bytes());
            }
        }

        // Convert to UTF-8.
        let decoded = match &charset {
            Some(cs) => decode_bytes_with_charset(cs, &raw_bytes),
            None => String::from_utf8_lossy(&raw_bytes).into_owned(),
        };

        // Reconstruct the base name preserving original case from the first segment.
        // We stored lowercase for grouping, but we need to recover the original.
        // Use the first key's base name from the original params.
        let original_base = find_original_base_name(params, &lower_name);
        // Track continuation groups that used RFC 2231 charset encoding so the
        // RFC 2047 fallback does not double-decode them.
        if charset.is_some() {
            rfc2231_decoded.insert(insert_pos);
        }
        result[insert_pos] = Some((original_base, decoded));
    }

    // Phase 2.5: RFC 2231 Section 5  -  when both `name` (plain) and `name*`
    // (or `name*0*` continuations) exist for the same parameter, the RFC 2231
    // charset-encoded form takes precedence. The plain form is merely "a
    // default for clients that do not understand the extended syntax."
    // Remove plain duplicates that are superseded by RFC 2231-decoded values.
    let mut rfc2231_names: HashSet<String> = HashSet::new();
    for &idx in &rfc2231_decoded {
        if let Some(Some((key, _))) = result.get(idx) {
            rfc2231_names.insert(key.to_ascii_lowercase());
        }
    }
    if !rfc2231_names.is_empty() {
        let mut new_result: Vec<Option<(String, String)>> = Vec::with_capacity(result.len());
        let mut new_decoded: HashSet<usize> = HashSet::new();
        for (i, entry) in result.into_iter().enumerate() {
            let Some(entry) = entry else {
                continue;
            };
            // A plain entry is "dominated" if an RFC 2231-decoded entry exists
            // for the same base name (case-insensitive).
            let dominated = !rfc2231_decoded.contains(&i)
                && rfc2231_names.contains(&entry.0.to_ascii_lowercase());
            if !dominated {
                if rfc2231_decoded.contains(&i) {
                    new_decoded.insert(new_result.len());
                }
                new_result.push(Some(entry));
            }
        }
        result = new_result;
        rfc2231_decoded = new_decoded;
    }

    // Fallback pass: some servers (non-conformantly) emit RFC 2047 encoded
    // words inside BODYSTRUCTURE parameter values instead of using RFC 2231
    // charset encoding. Decode any such values so callers get plain text.
    // RFC 2047 Section 1: "encoded-word = =?charset?encoding?encoded-text?="
    //
    // Skip values already decoded via RFC 2231 charset mechanisms to avoid
    // double-decoding. A legitimate RFC 2231-decoded value may contain literal
    // `=?...?=` sequences that must not be reinterpreted as RFC 2047 encoded
    // words. RFC 2047 Section 5 / RFC 2231 Section 4.
    for (i, entry) in result.iter_mut().enumerate() {
        let Some((_key, value)) = entry.as_mut() else {
            continue;
        };
        if rfc2231_decoded.contains(&i) {
            continue;
        }
        if value.contains("=?") && value.contains("?=") {
            *value = crate::codec::decode::decode_rfc2047(value.as_bytes());
        }
    }

    result.into_iter().flatten().collect()
}

/// Classification of an RFC 2231 parameter key.
enum KeyClass {
    /// `name*`  -  standalone charset-encoded (no continuation index).
    StandaloneEncoded { base_name: String },
    /// `name*N` or `name*N*`  -  continuation segment.
    Continuation {
        base_name: String,
        index: u32,
        encoded: bool,
    },
}

/// Classify a parameter key as plain, standalone charset-encoded, or continuation.
///
/// RFC 2231 Section 3: continuation keys have the form `name*N` or `name*N*`.
/// RFC 2231 Section 4: standalone charset keys have the form `name*` (no digit).
fn classify_key(key: &str) -> Option<KeyClass> {
    // Must contain at least one '*' to be RFC 2231.
    let star_pos = key.find('*')?;

    let base_name = key[..star_pos].to_owned();
    let suffix = &key[star_pos + 1..];

    if suffix.is_empty() {
        // `name*`  -  standalone charset-encoded.
        return Some(KeyClass::StandaloneEncoded { base_name });
    }

    // Check for `name*N` or `name*N*`.
    let (digits, is_encoded) = if let Some(stripped) = suffix.strip_suffix('*') {
        (stripped, true)
    } else {
        (suffix, false)
    };

    // RFC 2231 Section 3: "neither leading zeroes nor gaps in the sequence
    // are allowed." Reject indices with leading zeroes (e.g. *00, *01, *007)
    // while keeping *0 valid.
    if digits.len() > 1 && digits.starts_with('0') {
        return None;
    }
    let index: u32 = digits.parse().ok()?;

    Some(KeyClass::Continuation {
        base_name,
        index,
        encoded: is_encoded,
    })
}

/// Decode a standalone charset-encoded value per RFC 2231 Section 4.
///
/// Format: `charset'language'percent-encoded-value`
/// `extended-initial-value = [charset] "'" [language] "'" extended-other-values`
/// "Single quote delimiters MUST be present even when one of the field values is
/// omitted."
///
/// Falls back to returning the raw value if the format is malformed.
fn decode_charset_value(value: &str) -> String {
    let (charset, bytes) = split_charset_value(value);
    match charset {
        Some(cs) => decode_bytes_with_charset(&cs, &bytes),
        None => String::from_utf8_lossy(&bytes).into_owned(),
    }
}

/// Split `charset'language'encoded` into (charset, percent-decoded bytes).
///
/// RFC 2231 Section 4:
/// `extended-initial-value = [charset] "'" [language] "'" extended-other-values`
///
/// Returns `(None, raw_bytes)` if the format is malformed (graceful fallback).
fn split_charset_value(value: &str) -> (Option<String>, Vec<u8>) {
    // Find the two single-quote delimiters.
    let Some(first_quote) = value.find('\'') else {
        return (None, value.as_bytes().to_vec());
    };
    let Some(offset) = value[first_quote + 1..].find('\'') else {
        return (None, value.as_bytes().to_vec());
    };
    let second_quote = first_quote + 1 + offset;

    let charset = &value[..first_quote];
    let encoded_part = &value[second_quote + 1..];
    let bytes = percent_decode(encoded_part);

    let cs = if charset.is_empty() {
        None
    } else {
        Some(charset.to_owned())
    };

    (cs, bytes)
}

/// Percent-decode a string per RFC 2231 Section 4.
///
/// RFC 2231 Section 4 defines `ext-octet = "%" 2(DIGIT / "A"..."F")`.
/// Characters not preceded by `%` are passed through as-is.
fn percent_decode(input: &str) -> Vec<u8> {
    let bytes = input.as_bytes();
    let mut result = Vec::with_capacity(bytes.len());
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]))
        {
            result.push((hi << 4) | lo);
            i += 3;
            continue;
        }
        result.push(bytes[i]);
        i += 1;
    }

    result
}

/// Decode a single hex digit.
///
/// RFC 2231 Section 4: `ext-octet = "%" 2(DIGIT / "A" / "B" / "C" / "D" / "E" / "F")`
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'A'..=b'F' => Some(b - b'A' + 10),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}

/// Convert raw bytes to UTF-8 using the specified charset via `encoding_rs`.
///
/// Falls back to lossy UTF-8 conversion if the charset is unknown.
fn decode_bytes_with_charset(charset: &str, bytes: &[u8]) -> String {
    // UTF-8 fast path
    let cs_lower = charset.to_ascii_lowercase();
    if cs_lower == "utf-8" || cs_lower == "utf8" {
        return String::from_utf8_lossy(bytes).into_owned();
    }

    // Use encoding_rs for non-UTF-8 charsets.
    match encoding_rs::Encoding::for_label(charset.as_bytes()) {
        Some(encoding) => {
            // Use decode_without_bom_handling to preserve a leading U+FEFF if
            // it is genuinely part of the value rather than a BOM artefact.
            // RFC 2231 values are parameter fragments, not standalone documents,
            // so stripping a leading FEFF would corrupt legitimate content.
            let (cow, _) = encoding.decode_without_bom_handling(bytes);
            cow.into_owned()
        }
        None => {
            // Unknown charset  -  lossy fallback.
            String::from_utf8_lossy(bytes).into_owned()
        }
    }
}

/// Find the original (non-lowercased) base name from the first matching parameter key.
fn find_original_base_name(params: &[(String, String)], lower_name: &str) -> String {
    for (key, _) in params {
        if let Some(star_pos) = key.find('*') {
            let base = &key[..star_pos];
            if base.eq_ignore_ascii_case(lower_name) {
                return base.to_owned();
            }
        }
    }
    // Should not happen, but fallback to lowercase name.
    lower_name.to_owned()
}

#[cfg(test)]
#[path = "rfc2231_tests.rs"]
mod tests;
