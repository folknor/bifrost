//! `bifrost-mime` - RFC 5322 / MIME for the bifrost workspace.
//!
//! The tolerant inbound parser, header and transfer decoding, body
//! selection, and the outbound renderer the account crates compose through.
//! It sits above `bifrost-types` (it renders a `SendRequest` and reports
//! `AccountError`), so crates that only need the account contract do not
//! compile MIME. Inbound parsing is deliberately tolerant and is never an
//! outbound validator.

#![forbid(unsafe_code)]

mod charset;
mod header;
mod limits;
mod params;
mod parse;
mod render;
mod select;
mod transfer;
mod words;

pub use header::{HeaderMap, first_message_id, parse_address_list, parse_address_list_checked};
pub use limits::{Defect, MimeLimits};
pub use params::{ContentType, decode_rfc2231_params, parse_content_type, parse_disposition};
pub use parse::{MimePart, ParsedMessage, PartBody, parse_message, parse_message_with_limits};
pub use render::{
    ComposedMessage, RenderedMessage, SubmissionEnvelope, format_address, format_rfc5322_date,
    generate_message_id, render_rfc5322, send_request_to_rfc5322,
};
pub use select::{DecodedAttachment, DecodedBody, SelectOptions, select_body, select_body_with};
pub use transfer::TransferEncoding;
pub use words::decode_encoded_words;
