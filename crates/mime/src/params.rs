//! MIME content-type and RFC 2231 parameter handling.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};

use super::charset::hex_digit;
use super::limits::MimeLimits;
use super::words::decode_encoded_words;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentType {
    pub ty: String,
    pub subtype: String,
    pub params: Vec<(String, String)>,
}

impl ContentType {
    pub fn essence(&self) -> String {
        format!("{}/{}", self.ty, self.subtype)
    }

    pub fn param(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn charset(&self) -> Option<&str> {
        self.param("charset")
    }
    pub fn boundary(&self) -> Option<&str> {
        self.param("boundary")
    }

    pub fn text_plain_us_ascii() -> Self {
        Self {
            ty: "text".into(),
            subtype: "plain".into(),
            params: vec![("charset".into(), "us-ascii".into())],
        }
    }

    pub(crate) fn message_rfc822() -> Self {
        Self {
            ty: "message".into(),
            subtype: "rfc822".into(),
            params: Vec::new(),
        }
    }
}

pub fn parse_content_type(value: &str) -> ContentType {
    let (head, params) = tokenize(value);
    let Some((ty, subtype)) = head.split_once('/') else {
        return ContentType::text_plain_us_ascii();
    };
    if !is_token(ty.trim()) || !is_token(subtype.trim()) {
        return ContentType::text_plain_us_ascii();
    }
    ContentType {
        ty: ty.trim().to_ascii_lowercase(),
        subtype: subtype.trim().to_ascii_lowercase(),
        params: decode_rfc2231_params(&params)
            .into_iter()
            .map(|(name, value)| (name.to_ascii_lowercase(), value))
            .collect(),
    }
}

pub fn parse_disposition(value: &str) -> (String, Vec<(String, String)>) {
    let (head, params) = tokenize(value);
    let disposition = if is_token(head.trim()) {
        head.trim().to_ascii_lowercase()
    } else {
        String::new()
    };
    let params = decode_rfc2231_params(&params)
        .into_iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value))
        .collect();
    (disposition, params)
}

fn is_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b > 32 && b < 127 && !b"()<>@,;:\\\"/[]?=".contains(&b))
}

fn tokenize(value: &str) -> (String, Vec<(String, String)>) {
    let bytes = value.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i] != b';' {
        i += 1;
    }
    let head = value[..i].trim().to_owned();
    let mut params = Vec::new();
    while i < bytes.len() {
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let name_start = i;
        while i < bytes.len() && bytes[i] != b'=' && bytes[i] != b';' {
            i += 1;
        }
        let name = value[name_start..i].trim();
        if i == bytes.len() || bytes[i] != b'=' {
            while i < bytes.len() && bytes[i] != b';' {
                i += 1;
            }
            continue;
        }
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let mut decoded = String::new();
        if i < bytes.len() && bytes[i] == b'\"' {
            i += 1;
            // Accumulated as OCTETS, not `char`s: a quoted filename can carry
            // UTF-8, and casting each byte to `char` would latin1-mojibake it.
            let mut unquoted = Vec::new();
            while i < bytes.len() {
                match bytes[i] {
                    b'\"' => {
                        i += 1;
                        break;
                    }
                    b'\\' if i + 1 < bytes.len() => {
                        unquoted.push(bytes[i + 1]);
                        i += 2;
                    }
                    byte => {
                        unquoted.push(byte);
                        i += 1;
                    }
                }
            }
            decoded.push_str(&String::from_utf8_lossy(&unquoted));
            while i < bytes.len() && bytes[i] != b';' {
                i += 1;
            }
        } else {
            let start = i;
            while i < bytes.len() && bytes[i] != b';' {
                i += 1;
            }
            decoded.push_str(value[start..i].trim());
        }
        if !name.is_empty() {
            params.push((name.to_owned(), decoded));
        }
    }
    (head, params)
}

enum KeyClass {
    StandaloneEncoded {
        base_name: String,
    },
    Continuation {
        base_name: String,
        index: u32,
        encoded: bool,
    },
}

fn classify_key(key: &str) -> Option<KeyClass> {
    let star = key.find('*')?;
    let base_name = key[..star].to_owned();
    let suffix = &key[star + 1..];
    if suffix.is_empty() {
        return Some(KeyClass::StandaloneEncoded { base_name });
    }
    let (digits, encoded) = suffix
        .strip_suffix('*')
        .map_or((suffix, false), |digits| (digits, true));
    if digits.len() > 1 && digits.starts_with('0') {
        return None;
    }
    Some(KeyClass::Continuation {
        base_name,
        index: digits.parse().ok()?,
        encoded,
    })
}

#[derive(Default)]
struct Group {
    original: String,
    result_index: usize,
    segments: BTreeMap<u32, (String, bool)>,
}

pub fn decode_rfc2231_params(params: &[(String, String)]) -> Vec<(String, String)> {
    let mut result = Vec::<Option<(String, String)>>::with_capacity(params.len());
    let mut groups = Vec::<Group>::new();
    let mut indices = HashMap::<String, usize>::new();
    let mut decoded = HashSet::<usize>::new();
    for (key, value) in params {
        match classify_key(key) {
            Some(KeyClass::StandaloneEncoded { base_name }) => {
                decoded.insert(result.len());
                result.push(Some((base_name, decode_charset_value(value))));
            }
            Some(KeyClass::Continuation {
                base_name,
                index,
                encoded: encoded_segment,
            }) => {
                let lower = base_name.to_ascii_lowercase();
                let group_index = *indices.entry(lower.clone()).or_insert_with(|| {
                    let result_index = result.len();
                    result.push(None);
                    groups.push(Group {
                        original: base_name.clone(),
                        result_index,
                        segments: BTreeMap::new(),
                    });
                    groups.len() - 1
                });
                let group = &mut groups[group_index];
                match group.segments.entry(index) {
                    Entry::Occupied(_) => {
                        tracing::warn!(
                            base_name = lower.as_str(),
                            index,
                            "duplicate RFC 2231 continuation index, keeping first value"
                        );
                    }
                    Entry::Vacant(slot) => {
                        slot.insert((value.clone(), encoded_segment));
                    }
                }
            }
            None => result.push(Some((key.clone(), value.clone()))),
        }
    }
    let cap = MimeLimits::default().max_header_bytes;
    for group in groups {
        let Some(_) = group.segments.get(&0) else {
            continue;
        };
        let mut charset = None;
        let mut bytes = Vec::new();
        for (expected, (index, (value, encoded_segment))) in group.segments.into_iter().enumerate()
        {
            if usize::try_from(index).ok() != Some(expected) {
                break;
            }
            let addition = if encoded_segment && charset.is_none() {
                let (label, data) = split_charset_value(&value);
                charset = label;
                data
            } else if encoded_segment {
                percent_decode(&value)
            } else {
                value.into_bytes()
            };
            if bytes.len().saturating_add(addition.len()) > cap {
                tracing::warn!(
                    base_name = group.original.as_str(),
                    "RFC 2231 continuation exceeded allocation cap"
                );
                break;
            }
            bytes.extend_from_slice(&addition);
        }
        let value = charset.as_deref().map_or_else(
            || String::from_utf8_lossy(&bytes).into_owned(),
            |label| super::charset::decode_charset_lossy(label, &bytes),
        );
        if charset.is_some() {
            decoded.insert(group.result_index);
        }
        result[group.result_index] = Some((group.original, value));
    }
    let encoded_names: HashSet<String> = decoded
        .iter()
        .filter_map(|index| result.get(*index)?.as_ref())
        .map(|(key, _)| key.to_ascii_lowercase())
        .collect();
    let mut output = Vec::new();
    for (index, entry) in result.into_iter().enumerate() {
        let Some((key, mut value)) = entry else {
            continue;
        };
        if !decoded.contains(&index) && encoded_names.contains(&key.to_ascii_lowercase()) {
            continue;
        }
        if !decoded.contains(&index) && value.contains("=?") && value.contains("?=") {
            value = decode_encoded_words(value.as_bytes());
        }
        output.push((key, value));
    }
    output
}

fn decode_charset_value(value: &str) -> String {
    let (charset, bytes) = split_charset_value(value);
    charset.as_deref().map_or_else(
        || String::from_utf8_lossy(&bytes).into_owned(),
        |label| super::charset::decode_charset_lossy(label, &bytes),
    )
}

fn split_charset_value(value: &str) -> (Option<String>, Vec<u8>) {
    let Some(first) = value.find('\'') else {
        return (None, value.as_bytes().to_vec());
    };
    let Some(second_relative) = value[first + 1..].find('\'') else {
        return (None, value.as_bytes().to_vec());
    };
    let second = first + 1 + second_relative;
    let label = (!value[..first].is_empty()).then(|| value[..first].to_owned());
    (label, percent_decode(&value[second + 1..]))
}

fn percent_decode(value: &str) -> Vec<u8> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(high), Some(low)) = (hex_digit(bytes[i + 1]), hex_digit(bytes[i + 2]))
        {
            output.push(high << 4 | low);
            i += 3;
        } else {
            output.push(bytes[i]);
            i += 1;
        }
    }
    output
}

#[cfg(test)]
#[path = "params_tests.rs"]
mod tests;
