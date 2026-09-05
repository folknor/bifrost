//! Private shared WebDAV layer for `bifrost-caldav` and `bifrost-carddav`.
//!
//! These two crates are near-duplicates by construction: CalDAV and CardDAV are
//! the same protocol with different property names, query bodies and payload
//! projections. Everything below that difference - the transport seam, the
//! redirect policy, the credential-origin gate, the status-to-`AccountError`
//! ladder, etag handling - was hand-mirrored between them, and nothing compared
//! the copies, so divergence was silent. Eight separate defects were exactly
//! that: a fix landing in one crate and not its twin.
//!
//! This crate exists to delete the mirror rather than to keep policing it. It
//! is PRIVATE - the precedent is `bifrost-sasl` - so neither published surface
//! moves: `CalDavConfig`, `CardDavConfig`, both factories and both `Account`
//! impls are exactly what they were. Consumers see nothing.
//!
//! What stays in the protocol crates is what genuinely differs: property
//! constants, query XML, the iCalendar and vCard projections, and the lanes only
//! one side has (CalDAV's `sync-collection` and scheduling, CardDAV's
//! `getctag`).

mod dispatch;
mod error;
mod etag;
mod multistatus;
mod snapshot;
#[cfg(feature = "test-support")]
pub mod test_support;
mod transport;
mod xml;

pub use dispatch::{DavCredentials, DavDispatch};

pub use error::{
    DavProtocol, local_error, not_found_error, parse_error, recovery_rank, response_read_error,
    status_error, transport_error, unsupported_error, worse_recovery,
};
pub use etag::{PutCondition, normalize_http_etag, prepare_if_match, response_etag};
pub use multistatus::{
    MultiStatusSink, PropSet, ResponseParts, commit_if_present, extract_href_properties,
    extract_href_property, parse_collection_property, parse_multistatus,
};
pub use snapshot::{
    DecodedSnapshot, PageSlice, SnapshotEntry, decode_snapshot, decode_watermark_cursor,
    diff_snapshots, encode_snapshot, encode_watermark_cursor, inventory_entry, object_change,
    page_after_watermark, preserve_unobserved_entries, slice_after_watermark,
};
pub use transport::{
    DAV_CLIENT_TIMEOUT, DavBody, DavRequest, DavResponse, origin_is_secure, settle_body, url_origin,
};
pub use xml::{
    append_path, escape_xml, local_name, normalize_etag, push_text, resolve_href, same_dav_url,
    trimmed,
};
