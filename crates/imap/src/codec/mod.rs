//! IMAP wire codec: parsing server responses and encoding client commands.
//!
//! # References
//! - RFC 3501 (`IMAP4rev1`)
//! - RFC 9051 (`IMAP4rev2`)
//! - RFC 2047 (MIME encoded words in ENVELOPE fields)
//! - RFC 2231 (MIME parameter encoding in BODYSTRUCTURE)
//! - RFC 6855 (UTF8=ACCEPT  -  raw UTF-8 in quoted strings)

pub(crate) mod classification;
pub(crate) mod decode;
pub(crate) mod encode;
pub(crate) mod utf7;
