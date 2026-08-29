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

mod error;
mod etag;
mod transport;

pub use error::{
    DavProtocol, local_error, not_found_error, parse_error, recovery_rank, response_read_error,
    status_error, transport_error, unsupported_error, worse_recovery,
};
pub use etag::{PutCondition, normalize_http_etag, prepare_if_match, response_etag};
pub use transport::{
    DAV_CLIENT_TIMEOUT, DavBody, DavResponse, DavTransport, ReqwestDavTransport,
    dav_redirect_policy, origin_is_secure, read_capped_body, settle_body, transport_failure,
    url_origin,
};
