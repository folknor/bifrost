use bytes::Bytes;

use super::limits::{Defect, MimeLimits};
use super::parse::{MimePart, ParsedMessage, PartBody};

#[derive(Debug, Clone, Default)]
pub struct DecodedBody {
    pub text: Option<String>,
    pub html: Option<String>,
    pub attachments: Vec<DecodedAttachment>,
    pub defects: Vec<Defect>,
}

#[derive(Debug, Clone)]
pub struct DecodedAttachment {
    pub filename: Option<String>,
    pub content_type: String,
    pub content_id: Option<String>,
    pub inline: bool,
    pub size: u64,
    pub data: Option<Bytes>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct SelectOptions {
    pub include_attachment_bytes: bool,
    pub max_body_bytes: Option<usize>,
    pub limits: MimeLimits,
}

impl Default for SelectOptions {
    fn default() -> Self {
        Self {
            include_attachment_bytes: true,
            max_body_bytes: None,
            limits: MimeLimits::default(),
        }
    }
}

/// Selection state. `cid_candidates` parallels `body.attachments` and marks the
/// entries selection rule 9 may still flip to inline: a `Content-ID` with NO
/// explicit `Content-Disposition`. An explicit disposition is the sender's
/// answer and a `cid:` reference does not get to overrule it.
#[derive(Default)]
struct Selection {
    body: DecodedBody,
    cid_candidates: Vec<bool>,
}

pub fn select_body(message: &ParsedMessage) -> DecodedBody {
    select_body_with(message, SelectOptions::default())
}

pub fn select_body_with(message: &ParsedMessage, options: SelectOptions) -> DecodedBody {
    let mut selection = Selection::default();
    let truncated = message.defects.iter().any(is_truncation);
    walk(&message.root, &mut selection, options, false, truncated);
    let Selection {
        mut body,
        cid_candidates,
    } = selection;
    let html = body.html.clone();
    for (attachment, candidate) in body.attachments.iter_mut().zip(cid_candidates) {
        if !candidate {
            continue;
        }
        let Some(id) = attachment.content_id.as_deref() else {
            continue;
        };
        attachment.inline = html.as_deref().is_some_and(|body| contains_cid(body, id));
    }
    body
}

fn is_truncation(defect: &Defect) -> bool {
    matches!(
        defect,
        Defect::Truncated
            | Defect::DepthExceeded
            | Defect::PartCountExceeded
            | Defect::HeaderBlockTooLarge
    )
}

fn walk(
    part: &MimePart,
    selection: &mut Selection,
    options: SelectOptions,
    force_attachment: bool,
    truncated: bool,
) {
    match &part.body {
        PartBody::Multipart(children) if part.content_type.subtype == "alternative" => {
            // RFC 2046 section 5.1.4: the LAST alternative is the richest, so
            // the scan runs in reverse and the first representation of each
            // kind wins. A child that is neither text/plain nor text/html (a
            // nested multipart/related carrying the HTML plus its inline
            // images) is recursed into; alternatives that are not chosen
            // contribute nothing at all, not even attachments.
            for child in children.iter().rev() {
                match child.content_type.essence().as_str() {
                    "text/plain" if !child.is_attachment() => {
                        if selection.body.text.is_none() {
                            selection.body.text = decode_text(child, selection, options);
                        }
                    }
                    "text/html" if !child.is_attachment() => {
                        if selection.body.html.is_none() {
                            selection.body.html = decode_text(child, selection, options);
                        }
                    }
                    _ if child.is_multipart() => {
                        walk(child, selection, options, force_attachment, truncated);
                    }
                    _ => {}
                }
            }
        }
        PartBody::Multipart(children) if part.content_type.subtype == "related" => {
            let start = part
                .content_type
                .param("start")
                .map(|value| value.trim_matches(['<', '>']));
            let root = start
                .and_then(|id| {
                    children
                        .iter()
                        .position(|child| child.content_id.as_deref() == Some(id))
                })
                .unwrap_or(0);
            if let Some(child) = children.get(root) {
                walk(child, selection, options, force_attachment, truncated);
            }
            for (index, child) in children.iter().enumerate() {
                if index != root {
                    walk(child, selection, options, true, truncated);
                }
            }
        }
        PartBody::Multipart(children) if part.content_type.subtype == "signed" => {
            if let Some((first, rest)) = children.split_first() {
                walk(first, selection, options, force_attachment, truncated);
                for child in rest {
                    walk(child, selection, options, true, truncated);
                }
            }
        }
        PartBody::Multipart(children) if part.content_type.subtype == "encrypted" => {
            // RFC 1847 section 2.2: the first child is control information, not
            // content. Neither child is a body.
            for child in children {
                walk(child, selection, options, true, truncated);
            }
        }
        PartBody::Multipart(children) => {
            for child in children {
                walk(child, selection, options, force_attachment, truncated);
            }
        }
        PartBody::Embedded { raw, .. } => push_attachment(
            part,
            raw.len() as u64,
            || raw.clone(),
            selection,
            options,
            truncated,
        ),
        PartBody::Leaf(_) if force_attachment => {
            push_leaf_attachment(part, selection, options, truncated);
        }
        PartBody::Leaf(_)
            if part.content_type.essence() == "text/plain" && !part.is_attachment() =>
        {
            if selection.body.text.is_none() {
                selection.body.text = decode_text(part, selection, options);
            } else {
                push_leaf_attachment(part, selection, options, truncated);
            }
        }
        PartBody::Leaf(_)
            if part.content_type.essence() == "text/html" && !part.is_attachment() =>
        {
            if selection.body.html.is_none() {
                selection.body.html = decode_text(part, selection, options);
            } else {
                push_leaf_attachment(part, selection, options, truncated);
            }
        }
        PartBody::Leaf(_) => push_leaf_attachment(part, selection, options, truncated),
    }
}

fn decode_text(
    part: &MimePart,
    selection: &mut Selection,
    options: SelectOptions,
) -> Option<String> {
    let mut limits = options.limits;
    if let Some(max) = options.max_body_bytes {
        limits.max_text_bytes = limits.max_text_bytes.min(max);
    }
    part.text_with(&limits, &mut selection.body.defects)
}

fn push_leaf_attachment(
    part: &MimePart,
    selection: &mut Selection,
    options: SelectOptions,
    truncated: bool,
) {
    let bytes = part.bytes().unwrap_or_default();
    push_attachment(
        part,
        bytes.len() as u64,
        || Bytes::copy_from_slice(bytes),
        selection,
        options,
        truncated,
    );
}

/// `data` is a thunk, so the metadata-only lane measures the payload without
/// copying it.
fn push_attachment(
    part: &MimePart,
    size: u64,
    data: impl FnOnce() -> Bytes,
    selection: &mut Selection,
    options: SelectOptions,
    truncated: bool,
) {
    selection.body.attachments.push(DecodedAttachment {
        filename: part.filename.clone(),
        content_type: part.content_type.essence(),
        content_id: part.content_id.clone(),
        inline: part.disposition == "inline",
        size,
        data: options.include_attachment_bytes.then(data),
        truncated,
    });
    selection
        .cid_candidates
        .push(part.disposition.is_empty() && part.content_id.is_some());
}

/// Selection rule 9: the `cid:` SCHEME is matched case-insensitively, the id
/// itself exactly (RFC 2392 leaves the id case-sensitive).
fn contains_cid(html: &str, id: &str) -> bool {
    html.match_indices(id).any(|(index, _)| {
        index >= 4
            && html
                .get(index - 4..index)
                .is_some_and(|scheme| scheme.eq_ignore_ascii_case("cid:"))
    })
}

#[cfg(test)]
#[path = "select_tests.rs"]
mod tests;
