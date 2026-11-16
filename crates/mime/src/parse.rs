use bytes::Bytes;

use super::charset::decode_charset;
use super::header::{HeaderMap, split_headers};
use super::limits::{Defect, MimeLimits};
use super::params::{ContentType, parse_content_type, parse_disposition};
use super::transfer::{TransferEncoding, decode_transfer};
use super::words::decode_encoded_words;

/// Transfer-decoded contents of a MIME entity.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PartBody {
    Leaf(Vec<u8>),
    Multipart(Vec<MimePart>),
    Embedded {
        raw: Bytes,
        message: Box<ParsedMessage>,
    },
}

#[derive(Debug, Clone)]
pub struct MimePart {
    pub headers: HeaderMap,
    pub content_type: ContentType,
    pub encoding: TransferEncoding,
    pub disposition: String,
    pub filename: Option<String>,
    pub content_id: Option<String>,
    pub description: Option<String>,
    pub body: PartBody,
}

impl MimePart {
    pub fn text(&self) -> Option<String> {
        self.text_with(&MimeLimits::default(), &mut Vec::new())
    }

    pub fn text_with(&self, limits: &MimeLimits, defects: &mut Vec<Defect>) -> Option<String> {
        let PartBody::Leaf(bytes) = &self.body else {
            return None;
        };
        let text = decode_charset(
            self.content_type.charset().unwrap_or("us-ascii"),
            bytes,
            defects,
        );
        if text.len() <= limits.max_text_bytes {
            return Some(text);
        }
        defects.push(Defect::TextTruncated);
        let mut end = limits.max_text_bytes.min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        Some(text[..end].to_owned())
    }

    pub fn bytes(&self) -> Option<&[u8]> {
        match &self.body {
            PartBody::Leaf(bytes) => Some(bytes),
            _ => None,
        }
    }
    pub fn is_multipart(&self) -> bool {
        matches!(self.body, PartBody::Multipart(_))
    }
    pub fn is_attachment(&self) -> bool {
        self.disposition == "attachment"
            || self.filename.is_some()
            || (self.content_type.ty != "text" && self.content_type.ty != "multipart")
    }
}

#[derive(Debug, Clone)]
pub struct ParsedMessage {
    pub headers: HeaderMap,
    pub root: MimePart,
    pub defects: Vec<Defect>,
}

pub fn parse_message(raw: &[u8]) -> ParsedMessage {
    parse_message_with_limits(raw, MimeLimits::default())
}

pub fn parse_message_with_limits(raw: &[u8], limits: MimeLimits) -> ParsedMessage {
    let mut defects = Vec::new();
    let raw = if raw.len() > limits.max_input_bytes {
        defects.push(Defect::Truncated);
        &raw[..limits.max_input_bytes]
    } else {
        raw
    };
    let mut count = 0;
    let root = parse_entity(
        raw,
        ContentType::text_plain_us_ascii(),
        0,
        &mut count,
        &limits,
        &mut defects,
    );
    ParsedMessage {
        headers: root.headers.clone(),
        root,
        defects,
    }
}

fn parse_entity(
    raw: &[u8],
    default_type: ContentType,
    depth: usize,
    count: &mut usize,
    limits: &MimeLimits,
    defects: &mut Vec<Defect>,
) -> MimePart {
    *count += 1;
    let (headers, body) = split_headers(raw, limits, defects);
    let mut content_type = headers
        .get("Content-Type")
        .map(parse_content_type)
        .unwrap_or(default_type);
    let encoding = headers
        .get("Content-Transfer-Encoding")
        .map(TransferEncoding::from_token)
        .unwrap_or(TransferEncoding::SevenBit);
    let (disposition, disposition_params) = headers
        .get("Content-Disposition")
        .map(parse_disposition)
        .unwrap_or_default();
    let filename = disposition_params
        .iter()
        .find(|(key, _)| key == "filename")
        .map(|(_, value)| value.as_str())
        .or_else(|| content_type.param("name"))
        .map(|value| sanitize_filename(&decode_encoded_words(value.as_bytes())))
        .filter(|value| !value.is_empty());
    let content_id = headers
        .get("Content-ID")
        .map(|value| value.trim().trim_matches(['<', '>']).to_owned())
        .filter(|value| !value.is_empty());
    let description = headers.get_decoded("Content-Description");
    let decoded = decode_transfer(encoding, &body, defects);
    if matches!(encoding, TransferEncoding::Unknown) {
        content_type = ContentType {
            ty: "application".into(),
            subtype: "octet-stream".into(),
            params: Vec::new(),
        };
    }
    let is_multipart = content_type.ty == "multipart";
    let body = if is_multipart {
        if let Some(boundary) = content_type.boundary().filter(|value| !value.is_empty()) {
            if depth >= limits.max_depth {
                defects.push(Defect::DepthExceeded);
                PartBody::Leaf(decoded)
            } else if *count >= limits.max_parts {
                defects.push(Defect::PartCountExceeded);
                PartBody::Leaf(decoded)
            } else {
                let (segments, closed) = split_multipart(&decoded, boundary);
                if !closed {
                    defects.push(Defect::Truncated);
                }
                let child_default = if content_type.subtype == "digest" {
                    ContentType::message_rfc822()
                } else {
                    ContentType::text_plain_us_ascii()
                };
                let mut children = Vec::new();
                for segment in segments {
                    if *count >= limits.max_parts {
                        defects.push(Defect::PartCountExceeded);
                        break;
                    }
                    children.push(parse_entity(
                        &segment,
                        child_default.clone(),
                        depth + 1,
                        count,
                        limits,
                        defects,
                    ));
                }
                PartBody::Multipart(children)
            }
        } else {
            defects.push(Defect::MissingBoundary);
            PartBody::Leaf(decoded)
        }
    } else if content_type.ty == "message" && content_type.subtype == "rfc822" {
        // The embedded message shares the ENCLOSING depth and part budget. A
        // fresh budget per nesting level (which a plain `parse_message` call
        // here would give it) means a chain of `message/rfc822` wrappers
        // recurses as deep as the input is long, which is a stack overflow on
        // hostile mail rather than a bounded parse.
        if depth >= limits.max_depth {
            defects.push(Defect::DepthExceeded);
            PartBody::Leaf(decoded)
        } else if *count >= limits.max_parts {
            defects.push(Defect::PartCountExceeded);
            PartBody::Leaf(decoded)
        } else {
            let mut inner_defects = Vec::new();
            let inner = parse_entity(
                &decoded,
                ContentType::text_plain_us_ascii(),
                depth + 1,
                count,
                limits,
                &mut inner_defects,
            );
            defects.extend(inner_defects.iter().cloned());
            PartBody::Embedded {
                raw: Bytes::from(decoded),
                message: Box::new(ParsedMessage {
                    headers: inner.headers.clone(),
                    root: inner,
                    defects: inner_defects,
                }),
            }
        }
    } else {
        PartBody::Leaf(decoded)
    };
    MimePart {
        headers,
        content_type,
        encoding,
        disposition,
        filename,
        content_id,
        description,
        body,
    }
}

fn sanitize_filename(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !ch.is_control() && !matches!(*ch, '/' | '\\'))
        .collect::<String>()
        .trim()
        .to_owned()
}

fn split_multipart(body: &[u8], boundary: &str) -> (Vec<Vec<u8>>, bool) {
    let marker = format!("--{boundary}").into_bytes();
    let mut parts = Vec::new();
    let mut current = None::<Vec<u8>>;
    let mut offset = 0;
    let mut closed = false;
    while offset < body.len() {
        let end = body[offset..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(body.len(), |position| offset + position + 1);
        let line = &body[offset..end];
        let unterminated = line.strip_suffix(b"\n").unwrap_or(line);
        let clean = unterminated.strip_suffix(b"\r").unwrap_or(unterminated);
        if clean.starts_with(&marker)
            && clean[marker.len()..]
                .iter()
                .all(|byte| byte.is_ascii_whitespace() || *byte == b'-')
        {
            let rest = &clean[marker.len()..];
            if let Some(part) = current.take() {
                parts.push(trim_final_newline(part));
            }
            if rest.starts_with(b"--") {
                closed = true;
                break;
            }
            current = Some(Vec::new());
        } else if let Some(part) = &mut current {
            part.extend_from_slice(line);
        }
        offset = end;
    }
    if let Some(part) = current {
        parts.push(trim_final_newline(part));
    }
    (parts, closed)
}

fn trim_final_newline(mut value: Vec<u8>) -> Vec<u8> {
    if value.ends_with(b"\n") {
        value.pop();
        if value.ends_with(b"\r") {
            value.pop();
        }
    }
    value
}

#[cfg(test)]
#[path = "parse_tests.rs"]
mod tests;
