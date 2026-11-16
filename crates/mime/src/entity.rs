//! The one outbound MIME entity serializer, shared by the `SendRequest`
//! renderer and `bifrost-smtp`'s message builder.
//!
//! Three layers, each with one owner:
//!
//! - **Encoding** ([`choose_encoding`], [`encode_payload`],
//!   [`canonicalize_text_crlf`]): a composer decides, once, how a payload is
//!   transfer-encoded and stores the encoded bytes. `encode_payload` is pure:
//!   it never rewrites line endings. Text canonicalization is a separate,
//!   named step a composer applies to input it knows is text; opaque bytes
//!   are never canonicalized.
//! - **Framing** ([`write_entity`]): writes a prepared tree verbatim. It never
//!   chooses an encoding, repairs a header or touches a payload.
//! - **Boundaries** ([`mint_boundary`], [`contains_delimiter`]): a boundary is
//!   checked against the exact bytes [`write_entity`] emits for a child, so
//!   collision checking, DKIM signing and delivery read one definition of
//!   nested framing.
//!
//! Framing contract. A leaf payload is the complete, already-encoded body and
//! is written as is. A multipart writes, per child, `--boundary CRLF`, the
//! child, then CRLF; and finally `--boundary--` with NO trailing CRLF. The
//! CRLF before each delimiter therefore belongs to the parent and is never
//! merged with a CRLF the child payload already ends in (that one is
//! content). Root termination is the composer's policy, applied after
//! [`write_entity`]: it is not something the serializer may decide, because
//! a CRLF after a 7bit or quoted-printable leaf is decoded content.

use std::borrow::Cow;

use email_encoding::body::Encoding;

/// A transfer encoding a composer may emit. Distinct from the inbound
/// [`TransferEncoding`](crate::TransferEncoding), whose tolerant `Unknown`
/// is never an outbound choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum OutboundEncoding {
    /// `7bit`: ASCII lines within 998 octets, emitted verbatim.
    SevenBit,
    /// `8bit`: lines within 998 octets, emitted verbatim; needs 8BITMIME.
    EightBit,
    /// `quoted-printable`.
    QuotedPrintable,
    /// `base64`, wrapped at 76 columns with CRLF.
    Base64,
    /// `binary`: emitted verbatim; needs BINARYMIME. Never chosen
    /// automatically.
    Binary,
}

impl OutboundEncoding {
    /// The `Content-Transfer-Encoding` token.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SevenBit => "7bit",
            Self::EightBit => "8bit",
            Self::QuotedPrintable => "quoted-printable",
            Self::Base64 => "base64",
            Self::Binary => "binary",
        }
    }
}

/// What the chooser is told about a payload. The distinction is load
/// bearing: non-ASCII `Bytes` are opaque and choose base64 even when they
/// happen to be valid UTF-8, while `Text` may choose quoted-printable or
/// 8bit. Callers must not recover it by trial UTF-8 decoding of bytes.
#[derive(Debug, Clone, Copy)]
pub enum EncodingInput<'a> {
    /// Text the caller declared as a string.
    Text(&'a str),
    /// Opaque octets.
    Bytes(&'a [u8]),
}

/// Choose the most compact valid encoding among 7bit, 8bit (only when
/// `allow_8bit`), quoted-printable and base64. Never `Binary`.
pub fn choose_encoding(input: EncodingInput<'_>, allow_8bit: bool) -> OutboundEncoding {
    let chosen = match input {
        EncodingInput::Text(text) => Encoding::choose(text, allow_8bit),
        EncodingInput::Bytes(bytes) => Encoding::choose(bytes, allow_8bit),
    };
    match chosen {
        Encoding::SevenBit => OutboundEncoding::SevenBit,
        Encoding::EightBit => OutboundEncoding::EightBit,
        Encoding::QuotedPrintable => OutboundEncoding::QuotedPrintable,
        Encoding::Base64 => OutboundEncoding::Base64,
    }
}

/// Whether `bytes` may be emitted under `encoding` without re-encoding:
/// always for the encodings that transform, and for 7bit / 8bit only when the
/// chooser would accept them.
pub fn encoding_fits(input: EncodingInput<'_>, encoding: OutboundEncoding) -> bool {
    let best = choose_encoding(input, true);
    match encoding {
        OutboundEncoding::SevenBit => best == OutboundEncoding::SevenBit,
        OutboundEncoding::EightBit => {
            matches!(
                best,
                OutboundEncoding::SevenBit | OutboundEncoding::EightBit
            )
        }
        OutboundEncoding::QuotedPrintable | OutboundEncoding::Base64 | OutboundEncoding::Binary => {
            true
        }
    }
}

/// Transfer-encode `bytes`. Pure: no line ending is rewritten under any
/// encoding. Quoted-printable runs in text mode, which keeps an existing CRLF
/// as a hard line break and escapes a bare CR or LF, so the decoded octets
/// always equal the input; a composer that wants bare LF to be a line break
/// canonicalizes first.
pub fn encode_payload(bytes: &[u8], encoding: OutboundEncoding) -> Vec<u8> {
    match encoding {
        OutboundEncoding::SevenBit | OutboundEncoding::EightBit | OutboundEncoding::Binary => {
            bytes.to_vec()
        }
        OutboundEncoding::QuotedPrintable => quoted_printable::encode(bytes),
        OutboundEncoding::Base64 => {
            let mut out =
                String::with_capacity(email_encoding::body::base64::encoded_len(bytes.len()));
            email_encoding::body::base64::encode(bytes, &mut out)
                .expect("writing base64 into a String cannot fail");
            out.into_bytes()
        }
    }
}

/// Canonicalize text line endings: a CR is inserted before every bare LF.
/// A bare CR is kept. Only for input the composer knows is text; applying it
/// to opaque bytes corrupts them.
pub fn canonicalize_text_crlf(bytes: &mut Vec<u8>) {
    if !bytes
        .iter()
        .enumerate()
        .any(|(index, byte)| *byte == b'\n' && (index == 0 || bytes[index - 1] != b'\r'))
    {
        return;
    }
    let mut normalized = Vec::with_capacity(bytes.len() + bytes.len() / 16);
    let mut previous = None;
    for byte in bytes.drain(..) {
        if byte == b'\n' && previous != Some(b'\r') {
            normalized.push(b'\r');
        }
        normalized.push(byte);
        previous = Some(byte);
    }
    *bytes = normalized;
}

/// Mint a fresh RFC 2046 boundary: 40 lowercase hex characters from the
/// operating system's random source. Unpredictable, so a hostile body cannot
/// pre-forge it; [`contains_delimiter`] still checks every candidate.
pub fn mint_boundary() -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut random = [0_u8; 20];
    getrandom::fill(&mut random).expect("operating system random source is unavailable");
    let mut boundary = String::with_capacity(random.len() * 2);
    for byte in random {
        boundary.push(char::from(HEX[usize::from(byte >> 4)]));
        boundary.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    boundary
}

/// The body of a prepared entity, borrowed for serialization.
#[derive(Debug)]
pub enum EntityBody<'a, E> {
    /// The complete, already transfer-encoded payload.
    Encoded(&'a [u8]),
    /// A multipart: its boundary (already declared in its own
    /// Content-Type) and its children.
    Multipart {
        /// The boundary, without the leading `--`. Owned when the entity
        /// reads it out of a parsed header rather than storing it.
        boundary: Cow<'a, str>,
        /// The body parts, in order.
        children: &'a [E],
    },
}

/// A prepared MIME entity: validated header fields plus a body view. The
/// tree is homogeneous and statically dispatched.
pub trait MimeEntity: Sized {
    /// Write this entity's header fields, each with its terminating CRLF,
    /// and NOT the empty line that separates them from the body.
    fn write_header_fields(&self, out: &mut Vec<u8>);

    /// This entity's body.
    fn body(&self) -> EntityBody<'_, Self>;
}

/// Serialize `entity` under the framing contract in the module docs.
pub fn write_entity<E: MimeEntity>(entity: &E, out: &mut Vec<u8>) {
    entity.write_header_fields(out);
    out.extend_from_slice(b"\r\n");
    match entity.body() {
        EntityBody::Encoded(payload) => out.extend_from_slice(payload),
        EntityBody::Multipart { boundary, children } => {
            write_multipart_body(&boundary, children, out);
        }
    }
}

/// The body of a multipart entity: every child framed by its delimiter and
/// the parent's CRLF, then the close delimiter without a trailing CRLF. For
/// a composer that holds a multipart outside its entity type (its own
/// header block written separately); [`write_entity`] uses it too.
pub fn write_multipart_body<E: MimeEntity>(boundary: &str, children: &[E], out: &mut Vec<u8>) {
    for child in children {
        out.extend_from_slice(b"--");
        out.extend_from_slice(boundary.as_bytes());
        out.extend_from_slice(b"\r\n");
        write_entity(child, out);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"--");
    out.extend_from_slice(boundary.as_bytes());
    out.extend_from_slice(b"--");
}

/// Whether the serialized `entity` holds a line starting `--boundary`
/// (at its start or after a CRLF). Scans exactly what [`write_entity`]
/// emits, so nested delimiters and opaque pre-encoded payloads are covered,
/// and a nested boundary that `boundary` prefixes is caught too.
pub fn contains_delimiter<E: MimeEntity>(entity: &E, boundary: &str) -> bool {
    let mut serialized = Vec::new();
    write_entity(entity, &mut serialized);
    let opening = format!("--{boundary}");
    let marker = format!("\r\n{opening}");
    serialized.starts_with(opening.as_bytes())
        || serialized
            .windows(marker.len())
            .any(|window| window == marker.as_bytes())
}

#[cfg(test)]
#[path = "entity_tests.rs"]
mod tests;
