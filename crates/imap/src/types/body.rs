//! `BODYSTRUCTURE` types (RFC 3501 Section 7.4.2, RFC 9051 Section 7.5.2).
//!
//! Models both single-part and multipart MIME structures as returned by BODYSTRUCTURE
//! and BODY fetch items.

/// Parsed IMAP BODYSTRUCTURE (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BodyStructure {
    /// A single non-text, non-message part (e.g. image/png, application/pdf)
    /// (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
    Basic {
        /// MIME type (e.g. `"image"`, `"application"`) (RFC 2045 Section 5.1).
        media_type: String,
        /// MIME subtype (e.g. `"png"`, `"pdf"`) (RFC 2045 Section 5.1).
        media_subtype: String,
        /// MIME content-type parameters (RFC 2045 Section 5.1 / RFC 2231).
        params: Vec<(String, String)>,
        /// Content-ID header value (RFC 2045 Section 7).
        id: Option<String>,
        /// Content-Description header value (RFC 2045 Section 8).
        description: Option<String>,
        /// Content-Transfer-Encoding (RFC 2045 Section 6).
        encoding: String,
        /// Body size in octets (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
        ///
        /// RFC 3501 and RFC 9051 both define `body-fld-octets` as `number` (u32),
        /// but stored as `u64` for Postel's-law leniency with servers that send
        /// larger values.
        size: u64,
        // Extension data (may be absent).
        /// Content-MD5 value (RFC 3501 Section 7.4.2 extension data).
        md5: Option<String>,
        /// Content-Disposition (RFC 2183 / RFC 3501 Section 7.4.2 extension data).
        disposition: Option<ContentDisposition>,
        /// Body language (RFC 3501 Section 7.4.2 extension data).
        language: Option<Vec<String>>,
        /// Body content location (RFC 3501 Section 7.4.2 extension data).
        location: Option<String>,
    },
    /// A `text/*` part (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
    Text {
        /// MIME subtype (e.g. `"plain"`, `"html"`) (RFC 2045 Section 5.1).
        media_subtype: String,
        /// MIME content-type parameters (RFC 2045 Section 5.1 / RFC 2231).
        params: Vec<(String, String)>,
        /// Content-ID header value (RFC 2045 Section 7).
        id: Option<String>,
        /// Content-Description header value (RFC 2045 Section 8).
        description: Option<String>,
        /// Content-Transfer-Encoding (RFC 2045 Section 6).
        encoding: String,
        /// Body size in octets (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
        ///
        /// RFC 3501 and RFC 9051 both define `body-fld-octets` as `number` (u32),
        /// but stored as `u64` for Postel's-law leniency with servers that send
        /// larger values.
        size: u64,
        /// Number of lines in the text body (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
        ///
        /// RFC 9051 widens `body-fld-lines` from `number` to `number64`.
        lines: u64,
        // Extension data.
        /// Content-MD5 value (RFC 3501 Section 7.4.2 extension data).
        md5: Option<String>,
        /// Content-Disposition (RFC 2183 / RFC 3501 Section 7.4.2 extension data).
        disposition: Option<ContentDisposition>,
        /// Body language (RFC 3501 Section 7.4.2 extension data).
        language: Option<Vec<String>>,
        /// Body content location (RFC 3501 Section 7.4.2 extension data).
        location: Option<String>,
    },
    /// A `message/rfc822` or `message/global` part  -  embedded message
    /// (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
    Message {
        /// MIME subtype (`"rfc822"` or `"global"`) (RFC 9051 Section 7.5.2).
        media_subtype: String,
        /// MIME content-type parameters (RFC 2045 Section 5.1 / RFC 2231).
        params: Vec<(String, String)>,
        /// Content-ID header value (RFC 2045 Section 7).
        id: Option<String>,
        /// Content-Description header value (RFC 2045 Section 8).
        description: Option<String>,
        /// Content-Transfer-Encoding (RFC 2045 Section 6).
        encoding: String,
        /// Body size in octets (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
        ///
        /// RFC 3501 and RFC 9051 both define `body-fld-octets` as `number` (u32),
        /// but stored as `u64` for Postel's-law leniency with servers that send
        /// larger values.
        size: u64,
        /// Envelope of the embedded message (RFC 3501 Section 7.4.2).
        envelope: Box<super::Envelope>,
        /// Body structure of the embedded message (RFC 3501 Section 7.4.2).
        body: Box<Self>,
        /// Number of lines in the embedded message (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
        ///
        /// RFC 9051 widens `body-fld-lines` from `number` to `number64`.
        lines: u64,
        // Extension data.
        /// Content-MD5 value (RFC 3501 Section 7.4.2 extension data).
        md5: Option<String>,
        /// Content-Disposition (RFC 2183 / RFC 3501 Section 7.4.2 extension data).
        disposition: Option<ContentDisposition>,
        /// Body language (RFC 3501 Section 7.4.2 extension data).
        language: Option<Vec<String>>,
        /// Body content location (RFC 3501 Section 7.4.2 extension data).
        location: Option<String>,
    },
    /// A `multipart/*` container (RFC 2046 Section 5 / RFC 3501 Section 7.4.2).
    Multipart {
        /// Multipart subtype (e.g. `"mixed"`, `"alternative"`) (RFC 2046 Section 5).
        media_subtype: String,
        /// Child body parts (RFC 2046 Section 5).
        bodies: Vec<Self>,
        // Extension data.
        /// MIME content-type parameters (RFC 2045 Section 5.1 / RFC 2231).
        params: Vec<(String, String)>,
        /// Content-Disposition (RFC 2183 / RFC 3501 Section 7.4.2 extension data).
        disposition: Option<ContentDisposition>,
        /// Body language (RFC 3501 Section 7.4.2 extension data).
        language: Option<Vec<String>>,
        /// Body content location (RFC 3501 Section 7.4.2 extension data).
        location: Option<String>,
    },
}

/// Content-Disposition from BODYSTRUCTURE extension data (RFC 2183).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct ContentDisposition {
    /// Disposition type (e.g. `"inline"`, `"attachment"`) (RFC 2183 Section 2).
    pub disposition_type: String,
    /// Disposition parameters (e.g. `("filename", "report.pdf")`)
    /// (RFC 2183 Section 2 / RFC 2231).
    pub params: Vec<(String, String)>,
}

#[cfg(test)]
#[path = "body_tests.rs"]
mod tests;
