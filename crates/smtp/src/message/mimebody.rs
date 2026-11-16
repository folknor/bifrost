use std::io::Write;

use bifrost_mime::entity::{
    EntityBody, MimeEntity, contains_delimiter, mint_boundary, write_entity, write_multipart_body,
};
use mime::Mime;

use crate::message::{
    EmailFormat, IntoBody,
    header::{self, ContentTransferEncoding, ContentType, Header, Headers},
};

/// MIME part variants
#[derive(Debug, Clone)]
pub(super) enum Part {
    /// Single part with content
    Single(SinglePart),

    /// Multiple parts of content
    Multi(MultiPart),
}

/// Parts serialize through the shared `bifrost_mime::entity` walk, so the
/// boundary collision scan, DKIM signing and delivery read one definition
/// of nested framing.
impl MimeEntity for Part {
    fn write_header_fields(&self, out: &mut Vec<u8>) {
        let headers = match self {
            Part::Single(part) => &part.headers,
            Part::Multi(part) => &part.headers,
        };
        write!(out, "{headers}").expect("A Write implementation panicked while formatting headers");
    }

    fn body(&self) -> EntityBody<'_, Self> {
        match self {
            Part::Single(part) => EntityBody::Encoded(&part.body),
            Part::Multi(part) => EntityBody::Multipart {
                boundary: part.boundary().into(),
                children: &part.parts,
            },
        }
    }
}

/// This builder's root termination: one CRLF after the root entity,
/// unconditionally. That is the byte layout it has always emitted (and DKIM
/// tests pin), for a multipart root and a single-part root alike.
impl EmailFormat for Part {
    fn format(&self, out: &mut Vec<u8>) {
        write_entity(self, out);
        out.extend_from_slice(b"\r\n");
    }
}

/// Creates builder for single part
#[derive(Debug, Clone)]
pub struct SinglePartBuilder {
    headers: Headers,
}

impl SinglePartBuilder {
    /// Creates a default singlepart builder
    pub fn new() -> Self {
        Self {
            headers: Headers::new(),
        }
    }

    /// Set the header to singlepart
    ///
    /// # Errors
    ///
    /// `InvalidInput` when the header cannot be encoded (see
    /// [`Headers::set`]).
    pub fn header<H: Header>(mut self, header: H) -> Result<Self, crate::error::Error> {
        self.headers.set(header)?;
        Ok(self)
    }

    /// Set the Content-Type header of the singlepart
    ///
    /// # Errors
    ///
    /// As [`SinglePartBuilder::header`].
    pub fn content_type(self, content_type: ContentType) -> Result<Self, crate::error::Error> {
        self.header(content_type)
    }

    /// Build singlepart using body
    pub fn body<T: IntoBody>(mut self, body: T) -> SinglePart {
        let maybe_encoding = self.headers.get::<ContentTransferEncoding>();
        let body = body.into_body(maybe_encoding);

        if self.headers.get::<ContentType>().is_none()
            && let Some(content_type) = body.default_content_type()
        {
            self.headers.set_known(content_type);
        }
        self.headers.set_known(body.encoding());

        SinglePart {
            headers: self.headers,
            body: body.into_vec(),
        }
    }
}

impl Default for SinglePartBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Single part
///
/// # Example
///
/// ```
/// use bifrost_smtp::message::{SinglePart, header};
///
/// # use std::error::Error;
/// # fn main() -> Result<(), Box<dyn Error>> {
/// let part = SinglePart::builder()
///     .header(header::ContentType::TEXT_PLAIN)?
///     .body(String::from("Текст письма в уникоде"));
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct SinglePart {
    headers: Headers,
    body: Vec<u8>,
}

impl SinglePart {
    /// Creates a builder for singlepart
    #[inline]
    pub fn builder() -> SinglePartBuilder {
        SinglePartBuilder::new()
    }

    /// Directly create a `SinglePart` from a plain UTF-8 content
    pub fn plain<T: IntoBody>(body: T) -> Self {
        Self::with_known_type(header::ContentType::TEXT_PLAIN, body)
    }

    /// Directly create a `SinglePart` from a UTF-8 HTML content
    pub fn html<T: IntoBody>(body: T) -> Self {
        Self::with_known_type(header::ContentType::TEXT_HTML, body)
    }

    fn with_known_type<T: IntoBody>(content_type: ContentType, body: T) -> Self {
        let mut builder = Self::builder();
        builder.headers.set_known(content_type);
        builder.body(body)
    }

    /// Get the headers from singlepart
    #[inline]
    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    /// Get the encoded body
    #[inline]
    pub fn raw_body(&self) -> &[u8] {
        &self.body
    }

    /// Get message content formatted for sending
    pub fn formatted(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.format(&mut out);
        out
    }
}

/// The shared leaf framing (header block, separator, the encoded payload
/// verbatim) plus this builder's root CRLF; see `EmailFormat for Part`.
impl EmailFormat for SinglePart {
    fn format(&self, out: &mut Vec<u8>) {
        write!(out, "{}", self.headers)
            .expect("A Write implementation panicked while formatting headers");
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(&self.body);
        out.extend_from_slice(b"\r\n");
    }
}

/// The kind of multipart
#[derive(Debug, Clone)]
pub enum MultiPartKind {
    /// Mixed kind to combine unrelated content parts
    ///
    /// For example, this kind can be used to mix an email message and attachments.
    Mixed,

    /// Alternative kind to join several variants of same email contents.
    ///
    /// That kind is recommended to use for joining plain (text) and rich (HTML) messages into a single email message.
    Alternative,

    /// Related kind to mix content and related resources.
    ///
    /// For example, you can include images in HTML content using that.
    Related,

    /// Report kind for delivery-status, disposition-notification, and other reports.
    Report { report_type: String },

    /// Encrypted kind for encrypted messages
    Encrypted { protocol: String },

    /// Signed kind for signed messages
    Signed { protocol: String, micalg: String },
}

/// Edit the caller's `Content-Type` spelling so its only `boundary` parameter
/// is `boundary`, keeping every other parameter, the subtype and all other
/// bytes as written.
///
/// Every existing `boundary` parameter is removed (case-insensitive, quote
/// aware), together with its own separator, then the new one is spliced in
/// right after the first `;` as `; boundary="..";` followed by that `;`'s
/// original whitespace. Folding units (a token and the whitespace run before
/// it) therefore never grow: the type unit already owns its `;`, retained
/// parameters keep their runs, and the new unit has a fresh single space and
/// is at most 85 octets. A value that encoded before still encodes after,
/// which is what lets the boundary re-roll stay infallible. With no
/// parameter at all the type unit gains one `;` - that case only arises when
/// a Content-Type first enters a multipart builder, where insertion is
/// fallible.
fn with_boundary(source: &str, boundary: &str) -> String {
    let mut segments = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in source.bytes().enumerate() {
        if escaped {
            escaped = false;
        } else if quoted && byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            quoted = !quoted;
        } else if byte == b';' && !quoted {
            segments.push(&source[start..index]);
            start = index + 1;
        }
    }
    segments.push(&source[start..]);
    let type_unit = segments.remove(0);
    let new = format!("boundary=\"{boundary}\"");
    let kept: Vec<&str> = segments
        .into_iter()
        .filter(|segment| {
            let name = segment.split('=').next().unwrap_or_default().trim();
            !name.eq_ignore_ascii_case("boundary")
        })
        .collect();
    if kept.is_empty() {
        return format!("{type_unit}; {new}");
    }
    format!("{type_unit}; {new};{}", kept.join(";"))
}

/// Create a cryptographically random MIME boundary (the shared minter).
fn make_boundary() -> String {
    mint_boundary()
}

impl MultiPartKind {
    pub(crate) fn to_mime<S: Into<String>>(&self, boundary: Option<S>) -> Mime {
        let boundary = boundary.map_or_else(make_boundary, Into::into);
        assert!(
            is_mime_boundary(&boundary),
            "multipart boundary must contain 1 to 70 RFC 2046 boundary characters and must not end in a space"
        );
        if let Self::Report { report_type } = self {
            assert!(
                is_mime_token(report_type),
                "multipart/report report-type must be a MIME token"
            );
        }
        match self {
            Self::Encrypted { protocol } | Self::Signed { protocol, .. } => assert!(
                is_mime_quoted_value(protocol),
                "multipart protocol must be printable ASCII without quotes or backslashes"
            ),
            _ => {}
        }
        if let Self::Signed { micalg, .. } = self {
            assert!(
                is_mime_quoted_value(micalg),
                "multipart micalg must be printable ASCII without quotes or backslashes"
            );
        }

        format!(
            "multipart/{}; boundary=\"{}\"{}",
            match self {
                Self::Mixed => "mixed",
                Self::Alternative => "alternative",
                Self::Related => "related",
                Self::Report { .. } => "report",
                Self::Encrypted { .. } => "encrypted",
                Self::Signed { .. } => "signed",
            },
            boundary,
            match self {
                Self::Report { report_type } => format!("; report-type={report_type}"),
                Self::Encrypted { protocol } => format!("; protocol=\"{protocol}\""),
                Self::Signed { protocol, micalg } =>
                    format!("; protocol=\"{protocol}\"; micalg=\"{micalg}\""),
                _ => String::new(),
            }
        )
        .parse()
        .unwrap()
    }
}

/// Longest kind parameter value accepted, so a kind-derived Content-Type,
/// where each parameter is its own folding unit, always fits 998 octets.
const KIND_PARAM_MAX: usize = 900;

fn is_mime_token(value: &str) -> bool {
    const TSPECIALS: &[u8] = b"()<>@,;:\\\"/[]?=";

    !value.is_empty()
        && value.len() <= KIND_PARAM_MAX
        && value
            .bytes()
            .all(|byte| byte > b' ' && byte < 0x7f && !TSPECIALS.contains(&byte))
}

fn is_mime_boundary(value: &str) -> bool {
    const BCHARS_NO_SPACE: &[u8] =
        b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'()+_,-./:=?";

    (1..=70).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte == b' ' || BCHARS_NO_SPACE.contains(&byte))
        && !value.ends_with(' ')
}

fn is_mime_quoted_value(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= KIND_PARAM_MAX
        && value
            .bytes()
            .all(|byte| (b' '..=b'~').contains(&byte) && !matches!(byte, b'"' | b'\\'))
}

fn invalid(message: &str) -> crate::error::Error {
    crate::error::Error::InvalidInput(message.to_owned())
}

/// The one structural check every multipart header change passes through:
/// builder insertion, [`MultiPart::edit_headers`] and nothing else, since a
/// built multipart exposes no unchecked `&mut Headers`.
///
/// A multipart's Content-Type must be `multipart/*` with a legal RFC 2046
/// boundary that no child contains; a missing boundary is added, a colliding
/// one re-rolled (as adding a child does). The entry is always re-set from the
/// parsed value, so the bytes emitted are the spelling the typed getter and
/// `boundary()` read: a raw, parameterized or pre-encoded Content-Type cannot
/// carry a different delimiter on the wire, and duplicate `boundary`
/// parameters collapse to one. Adding a boundary can push an indivisible
/// token past the line limit, which is why this runs at a fallible boundary
/// and never at format time. Content-Transfer-Encoding of a multipart may only
/// be 7bit, 8bit or binary (RFC 2046 section 5.1), since the delimiters and
/// child headers are written unencoded.
///
/// `require_content_type` is false only for a builder that has not set one
/// yet; `build` supplies multipart/mixed.
fn validate_multipart_headers(
    headers: &mut Headers,
    children: &[Part],
    require_content_type: bool,
) -> Result<(), crate::error::Error> {
    let cte_name = ContentTransferEncoding::name();
    if headers.get_raw(&cte_name).is_some() {
        match headers.get::<ContentTransferEncoding>() {
            Some(
                encoding @ (ContentTransferEncoding::SevenBit
                | ContentTransferEncoding::EightBit
                | ContentTransferEncoding::Binary),
            ) => headers.set(encoding)?,
            _ => {
                return Err(invalid(
                    "multipart Content-Transfer-Encoding must be 7bit, 8bit or binary",
                ));
            }
        }
    }

    let content_type_name = ContentType::name();
    if headers.get_raw(&content_type_name).is_none() {
        return if require_content_type {
            Err(invalid("a multipart requires a Content-Type"))
        } else {
            Ok(())
        };
    }
    let content_type = headers
        .get::<ContentType>()
        .ok_or_else(|| invalid("multipart Content-Type does not parse"))?;
    let mime = content_type.as_ref();
    if mime.type_() != mime::MULTIPART {
        return Err(invalid("a multipart Content-Type must be multipart/*"));
    }
    let existing = match mime.get_param(mime::BOUNDARY) {
        Some(boundary) if is_mime_boundary(boundary.as_str()) => Some(boundary.as_str().to_owned()),
        Some(_) => {
            return Err(invalid(
                "multipart boundary must contain 1 to 70 RFC 2046 boundary characters and must not end in a space",
            ));
        }
        None => None,
    };
    let mut boundary = existing.unwrap_or_else(make_boundary);
    while children_contain_boundary(children, &boundary) {
        boundary = make_boundary();
    }
    let spelled = with_boundary(mime.as_ref(), &boundary)
        .parse::<Mime>()
        .map_err(|_| invalid("multipart Content-Type does not accept a boundary"))?;
    headers.set(ContentType::from_mime(spelled))
}

fn children_contain_boundary(children: &[Part], boundary: &str) -> bool {
    children
        .iter()
        .any(|part| contains_delimiter(part, boundary))
}

/// The delivery gate's structural check, run when a part becomes a
/// [`Message`](crate::Message) body: every assembled `multipart/report`
/// (found by subtype, so a missing `report-type` is caught rather than
/// skipped) has two or three children and a token `report-type`. Standalone
/// parts are assembly objects whose chained API passes through incomplete
/// states, so this is not checked while building or formatting them. A
/// `SinglePart` body is opaque caller-encoded bytes and is not inspected.
pub(super) fn validate_for_message(part: &Part) -> Result<(), crate::error::Error> {
    let Part::Multi(multi) = part else {
        return Ok(());
    };
    if let Some(content_type) = multi.headers.get::<ContentType>()
        && content_type.as_ref().subtype() == "report"
    {
        if !(2..=3).contains(&multi.parts.len()) {
            return Err(invalid("multipart/report requires two or three body parts"));
        }
        if !content_type
            .as_ref()
            .get_param("report-type")
            .is_some_and(|value| is_mime_token(value.as_str()))
        {
            return Err(invalid("multipart/report report-type must be a MIME token"));
        }
    }
    multi.parts.iter().try_for_each(validate_for_message)
}

/// Multipart builder
#[derive(Debug, Clone)]
pub struct MultiPartBuilder {
    headers: Headers,
}

impl MultiPartBuilder {
    /// Creates default multipart builder
    pub fn new() -> Self {
        Self {
            headers: Headers::new(),
        }
    }

    /// Set a header
    ///
    /// A `Content-Type` without a `boundary` gets a generated one here, so
    /// every later boundary edit only replaces an existing parameter.
    ///
    /// # Errors
    ///
    /// `InvalidInput` when the header, or the Content-Type with its added
    /// boundary, cannot be encoded (see [`Headers::set`]).
    pub fn header<H: Header>(mut self, header: H) -> Result<Self, crate::error::Error> {
        let mut candidate = self.headers.clone();
        candidate.set(header)?;
        // A builder may still lack its Content-Type; `build` supplies
        // multipart/mixed then.
        validate_multipart_headers(&mut candidate, &[], false)?;
        self.headers = candidate;
        Ok(self)
    }

    /// Set `Content-Type` header using [`MultiPartKind`]
    pub fn kind(mut self, kind: MultiPartKind) -> Self {
        // The kind's parameters are validated and bounded by `to_mime`.
        self.headers
            .set_known(ContentType::from_mime(kind.to_mime::<String>(None)));
        self
    }

    /// Set custom boundary
    pub fn boundary<S: Into<String>>(self, boundary: S) -> Self {
        self.try_boundary(boundary)
            .expect("invalid multipart boundary")
    }

    /// Set a custom boundary after validating it against RFC 2046.
    pub fn try_boundary<S: Into<String>>(
        mut self,
        boundary: S,
    ) -> Result<Self, crate::error::Error> {
        let boundary = boundary.into();
        if !is_mime_boundary(&boundary) {
            return Err(crate::error::Error::InvalidInput(
                "multipart boundary must contain 1 to 70 RFC 2046 boundary characters and must not end in a space"
                    .to_owned(),
            ));
        }
        match self.headers.get::<ContentType>() {
            // A Content-Type here already carries a boundary (added when it
            // was inserted), so this edit only replaces it and stays
            // encodable; see `with_boundary`.
            Some(content_type) => {
                let mime = with_boundary(content_type.as_ref().as_ref(), &boundary)
                    .parse::<Mime>()
                    .map_err(|_| invalid("multipart Content-Type does not accept a boundary"))?;
                self.headers.set(ContentType::from_mime(mime))?;
            }
            None => self.headers.set_known(ContentType::from_mime(
                MultiPartKind::Mixed.to_mime(Some(boundary)),
            )),
        }
        Ok(self)
    }

    /// Creates multipart without parts
    pub fn build(mut self) -> MultiPart {
        // Any Content-Type set through `header` or `kind` already has a
        // boundary; only the absent case remains.
        if self.headers.get::<ContentType>().is_none() {
            self.headers.set_known(ContentType::from_mime(
                MultiPartKind::Mixed.to_mime::<String>(None),
            ));
        }

        MultiPart {
            headers: self.headers,
            parts: Vec::new(),
        }
    }

    /// Creates multipart using singlepart
    pub fn singlepart(self, part: SinglePart) -> MultiPart {
        self.build().singlepart(part)
    }

    /// Creates multipart using multiple singleparts
    pub fn singleparts<I>(self, parts: I) -> MultiPart
    where
        I: IntoIterator<Item = SinglePart>,
    {
        self.build().singleparts(parts)
    }

    /// Creates multipart using multipart
    pub fn multipart(self, part: MultiPart) -> MultiPart {
        self.build().multipart(part)
    }

    /// Creates multipart using multiple multiparts
    pub fn multiparts<I>(self, parts: I) -> MultiPart
    where
        I: IntoIterator<Item = MultiPart>,
    {
        self.build().multiparts(parts)
    }
}

impl Default for MultiPartBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Multipart variant with parts
#[derive(Debug, Clone)]
pub struct MultiPart {
    headers: Headers,
    parts: Vec<Part>,
}

impl MultiPart {
    /// Creates multipart builder
    pub fn builder() -> MultiPartBuilder {
        MultiPartBuilder::new()
    }

    /// Creates mixed multipart builder
    ///
    /// Shortcut for `MultiPart::builder().kind(MultiPartKind::Mixed)`
    pub fn mixed() -> MultiPartBuilder {
        MultiPart::builder().kind(MultiPartKind::Mixed)
    }

    /// Creates alternative multipart builder
    ///
    /// Shortcut for `MultiPart::builder().kind(MultiPartKind::Alternative)`
    pub fn alternative() -> MultiPartBuilder {
        MultiPart::builder().kind(MultiPartKind::Alternative)
    }

    /// Creates related multipart builder
    ///
    /// Shortcut for `MultiPart::builder().kind(MultiPartKind::Related)`
    pub fn related() -> MultiPartBuilder {
        MultiPart::builder().kind(MultiPartKind::Related)
    }

    /// Creates report multipart builder.
    ///
    /// Shortcut for `MultiPart::builder().kind(MultiPartKind::Report { report_type })`
    pub fn report(report_type: String) -> MultiPartBuilder {
        Self::try_report(report_type).expect("multipart/report report-type must be a MIME token")
    }

    /// Creates a report multipart builder after validating `report_type` as a MIME token.
    pub fn try_report(
        report_type: impl Into<String>,
    ) -> Result<MultiPartBuilder, crate::error::Error> {
        let report_type = report_type.into();
        if !is_mime_token(&report_type) {
            return Err(crate::error::Error::InvalidInput(
                "multipart/report report-type must be a MIME token".to_owned(),
            ));
        }

        Ok(MultiPart::builder().kind(MultiPartKind::Report { report_type }))
    }

    /// Creates encrypted multipart builder
    ///
    /// Shortcut for `MultiPart::builder().kind(MultiPartKind::Encrypted{ protocol })`
    pub fn encrypted(protocol: String) -> MultiPartBuilder {
        Self::try_encrypted(protocol)
            .expect("multipart/encrypted protocol must be a safe quoted MIME value")
    }

    /// Creates an encrypted multipart builder after validating `protocol`.
    pub fn try_encrypted(
        protocol: impl Into<String>,
    ) -> Result<MultiPartBuilder, crate::error::Error> {
        let protocol = protocol.into();
        if !is_mime_quoted_value(&protocol) {
            return Err(crate::error::Error::InvalidInput(
                "multipart/encrypted protocol must be printable ASCII without quotes or backslashes"
                    .to_owned(),
            ));
        }
        Ok(MultiPart::builder().kind(MultiPartKind::Encrypted { protocol }))
    }

    /// Creates signed multipart builder
    ///
    /// Shortcut for `MultiPart::builder().kind(MultiPartKind::Signed{ protocol, micalg })`
    pub fn signed(protocol: String, micalg: String) -> MultiPartBuilder {
        Self::try_signed(protocol, micalg)
            .expect("multipart/signed parameters must be safe quoted MIME values")
    }

    /// Creates a signed multipart builder after validating its MIME parameters.
    pub fn try_signed(
        protocol: impl Into<String>,
        micalg: impl Into<String>,
    ) -> Result<MultiPartBuilder, crate::error::Error> {
        let protocol = protocol.into();
        let micalg = micalg.into();
        if !is_mime_quoted_value(&protocol) || !is_mime_quoted_value(&micalg) {
            return Err(crate::error::Error::InvalidInput(
                "multipart/signed parameters must be printable ASCII without quotes or backslashes"
                    .to_owned(),
            ));
        }
        Ok(MultiPart::builder().kind(MultiPartKind::Signed { protocol, micalg }))
    }

    /// Alias for HTML and plain text versions of an email
    pub fn alternative_plain_html<T: IntoBody, V: IntoBody>(plain: T, html: V) -> Self {
        Self::alternative()
            .singlepart(SinglePart::plain(plain))
            .singlepart(SinglePart::html(html))
    }

    /// Add single part to multipart
    pub fn singlepart(mut self, part: SinglePart) -> Self {
        let added = self.parts.len();
        self.parts.push(Part::Single(part));
        self.ensure_boundary_absent(added);
        self
    }

    /// Add multiple single parts to multipart
    pub fn singleparts<I>(mut self, parts: I) -> Self
    where
        I: IntoIterator<Item = SinglePart>,
    {
        let added = self.parts.len();
        self.parts.extend(parts.into_iter().map(Part::Single));
        self.ensure_boundary_absent(added);
        self
    }

    /// Add multi part to multipart
    pub fn multipart(mut self, part: MultiPart) -> Self {
        let added = self.parts.len();
        self.parts.push(Part::Multi(part));
        self.ensure_boundary_absent(added);
        self
    }

    /// Add multiple multipart parts to multipart
    pub fn multiparts<I>(mut self, parts: I) -> Self
    where
        I: IntoIterator<Item = MultiPart>,
    {
        let added = self.parts.len();
        self.parts.extend(parts.into_iter().map(Part::Multi));
        self.ensure_boundary_absent(added);
        self
    }

    /// Get the boundary of multipart contents
    pub fn boundary(&self) -> String {
        let content_type = self.headers.get::<ContentType>().unwrap();
        content_type
            .as_ref()
            .get_param("boundary")
            .unwrap()
            .as_str()
            .into()
    }

    /// Get the headers from the multipart
    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    /// Edit the headers as one transaction.
    ///
    /// `edit` runs on a copy; the result must still describe a multipart:
    /// a `multipart/*` Content-Type (a missing boundary is added, one that a
    /// child contains is re-rolled, an illegal one refused) and a
    /// Content-Transfer-Encoding, if any, of 7bit, 8bit or binary. On an
    /// error from `edit` or from that check the headers are left unchanged.
    ///
    /// # Errors
    ///
    /// The error `edit` returned, or `InvalidInput` for a refused edit.
    pub fn edit_headers<F>(&mut self, edit: F) -> Result<(), crate::error::Error>
    where
        F: FnOnce(&mut Headers) -> Result<(), crate::error::Error>,
    {
        let mut candidate = self.headers.clone();
        edit(&mut candidate)?;
        validate_multipart_headers(&mut candidate, &self.parts, true)?;
        self.headers = candidate;
        Ok(())
    }

    /// Get message content formatted for SMTP
    pub fn formatted(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.format(&mut out);
        out
    }

    /// Re-roll the boundary when a part added at or after `from` would forge a
    /// delimiter.
    ///
    /// Parts before `from` were already cleared against the current boundary by
    /// the call that added them, so only the newcomers need scanning; a re-roll
    /// is the sole case that has to re-scan everything. Without that split,
    /// assembling an N-part message re-encodes every earlier part on each add
    /// and the cost is quadratic in total body size.
    fn ensure_boundary_absent(&mut self, from: usize) {
        let current = self.boundary();
        if !children_contain_boundary(&self.parts[from..], &current) {
            return;
        }

        let existing = self
            .headers
            .get::<ContentType>()
            .expect("a built multipart has a Content-Type with a boundary");
        loop {
            let boundary = make_boundary();
            if !children_contain_boundary(&self.parts, &boundary) {
                // Re-roll the BOUNDARY, not the whole Content-Type: the edit
                // keeps every other parameter and the subtype as the caller
                // spelled them, and keeps the value encodable (see
                // `with_boundary`), so no parameter is ever dropped.
                let mime = with_boundary(existing.as_ref().as_ref(), &boundary)
                    .parse::<Mime>()
                    .expect("replacing a valid boundary keeps the Content-Type parseable");
                self.headers.set_known(ContentType::from_mime(mime));
                return;
            }
        }
    }
}

/// Test-only chaining over the fallible part-builder `header` step, so the
/// many fixtures that insert valid headers read as before; any refusal
/// panics the test that hit it.
#[cfg(test)]
#[allow(clippy::unwrap_in_result)]
pub(crate) trait ChainOk<B> {
    fn header<H: Header>(self, header: H) -> Result<B, crate::error::Error>;
}

#[cfg(test)]
#[allow(clippy::unwrap_in_result)]
impl ChainOk<SinglePartBuilder> for Result<SinglePartBuilder, crate::error::Error> {
    fn header<H: Header>(self, header: H) -> Result<SinglePartBuilder, crate::error::Error> {
        self.expect("valid fixture header").header(header)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_in_result)]
impl ChainOk<MultiPartBuilder> for Result<MultiPartBuilder, crate::error::Error> {
    fn header<H: Header>(self, header: H) -> Result<MultiPartBuilder, crate::error::Error> {
        self.expect("valid fixture header").header(header)
    }
}

#[cfg(test)]
pub(crate) trait BodyOk {
    fn body<T: IntoBody>(self, body: T) -> SinglePart;
}

#[cfg(test)]
impl BodyOk for Result<SinglePartBuilder, crate::error::Error> {
    fn body<T: IntoBody>(self, body: T) -> SinglePart {
        self.expect("valid fixture header").body(body)
    }
}

#[cfg(test)]
pub(crate) trait PartsOk {
    fn singlepart(self, part: SinglePart) -> MultiPart;
}

#[cfg(test)]
impl PartsOk for Result<MultiPartBuilder, crate::error::Error> {
    fn singlepart(self, part: SinglePart) -> MultiPart {
        self.expect("valid fixture header").singlepart(part)
    }
}

impl EmailFormat for MultiPart {
    /// Infallible and never panics: an incomplete standalone
    /// `multipart/report` is emitted as assembled. Report validity is
    /// checked where the part becomes a `Message` body (see
    /// `validate_for_message`).
    fn format(&self, out: &mut Vec<u8>) {
        write!(out, "{}", self.headers)
            .expect("A Write implementation panicked while formatting headers");
        out.extend_from_slice(b"\r\n");
        write_multipart_body(&self.boundary(), &self.parts, out);
        out.extend_from_slice(b"\r\n");
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn single_part_binary() {
        let part = SinglePart::builder()
            .header(header::ContentType::TEXT_PLAIN)
            .header(header::ContentTransferEncoding::Binary)
            .body(String::from("Текст письма в уникоде"));

        assert_eq!(
            String::from_utf8(part.formatted()).unwrap(),
            concat!(
                "Content-Type: text/plain; charset=utf-8\r\n",
                "Content-Transfer-Encoding: binary\r\n",
                "\r\n",
                "Текст письма в уникоде\r\n"
            )
        );
    }

    #[test]
    fn single_part_quoted_printable() {
        let part = SinglePart::builder()
            .header(header::ContentType::TEXT_PLAIN)
            .header(header::ContentTransferEncoding::QuotedPrintable)
            .body(String::from("Текст письма в уникоде"));

        assert_eq!(
            String::from_utf8(part.formatted()).unwrap(),
            concat!(
                "Content-Type: text/plain; charset=utf-8\r\n",
                "Content-Transfer-Encoding: quoted-printable\r\n",
                "\r\n",
                "=D0=A2=D0=B5=D0=BA=D1=81=D1=82 =D0=BF=D0=B8=D1=81=D1=8C=D0=BC=D0=B0 =D0=B2 =\r\n",
                "=D1=83=D0=BD=D0=B8=D0=BA=D0=BE=D0=B4=D0=B5\r\n"
            )
        );
    }

    #[test]
    fn single_part_base64() {
        let part = SinglePart::builder()
            .header(header::ContentType::TEXT_PLAIN)
            .header(header::ContentTransferEncoding::Base64)
            .body(String::from("Текст письма в уникоде"));

        assert_eq!(
            String::from_utf8(part.formatted()).unwrap(),
            concat!(
                "Content-Type: text/plain; charset=utf-8\r\n",
                "Content-Transfer-Encoding: base64\r\n",
                "\r\n",
                "0KLQtdC60YHRgiDQv9C40YHRjNC80LAg0LIg0YPQvdC40LrQvtC00LU=\r\n"
            )
        );
    }

    #[test]
    fn multi_part_mixed() {
        let part = MultiPart::mixed()
            .boundary("0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1")
            .singlepart(
                SinglePart::builder()
                    .header(header::ContentType::TEXT_PLAIN)
                    .header(header::ContentTransferEncoding::Binary)
                    .body(String::from("Текст письма в уникоде")),
            )
            .singlepart(
                SinglePart::builder()
                    .header(header::ContentType::TEXT_PLAIN)
                    .header(header::ContentDisposition::attachment("example.c"))
                    .header(header::ContentTransferEncoding::Binary)
                    .body(String::from("int main() { return 0; }")),
            );

        assert_eq!(
            String::from_utf8(part.formatted()).unwrap(),
            concat!(
                "Content-Type: multipart/mixed;\r\n",
                " boundary=\"0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\"\r\n",
                "\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Type: text/plain; charset=utf-8\r\n",
                "Content-Transfer-Encoding: binary\r\n",
                "\r\n",
                "Текст письма в уникоде\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Type: text/plain; charset=utf-8\r\n",
                "Content-Disposition: attachment; filename=\"example.c\"\r\n",
                "Content-Transfer-Encoding: binary\r\n",
                "\r\n",
                "int main() { return 0; }\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1--\r\n"
            )
        );
    }

    #[test]
    fn multi_part_builder_defaults_to_mixed() {
        let part = MultiPart::builder()
            .boundary("0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1")
            .singlepart(SinglePart::plain("hello".to_owned()));

        assert_eq!(
            String::from_utf8(part.formatted()).unwrap(),
            concat!(
                "Content-Type: multipart/mixed;\r\n",
                " boundary=\"0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\"\r\n",
                "\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Type: text/plain; charset=utf-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "hello\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1--\r\n"
            )
        );
    }

    #[test]
    fn multi_part_accepts_multiple_singleparts() {
        let attachments = vec![
            SinglePart::builder()
                .header(header::ContentDisposition::attachment("a.txt"))
                .body(String::from("alpha")),
            SinglePart::builder()
                .header(header::ContentDisposition::attachment("b.txt"))
                .body(String::from("beta")),
        ];
        let part = MultiPart::mixed()
            .boundary("0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1")
            .singleparts(attachments);

        assert_eq!(
            String::from_utf8(part.formatted()).unwrap(),
            concat!(
                "Content-Type: multipart/mixed;\r\n",
                " boundary=\"0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\"\r\n",
                "\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Disposition: attachment; filename=\"a.txt\"\r\n",
                "Content-Type: text/plain; charset=utf-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "alpha\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Disposition: attachment; filename=\"b.txt\"\r\n",
                "Content-Type: text/plain; charset=utf-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "beta\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1--\r\n"
            )
        );
    }

    #[test]
    fn multi_part_report() {
        let part = MultiPart::report("delivery-status".to_owned())
            .boundary("0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1")
            .singlepart(SinglePart::plain("Delivery failed".to_owned()))
            .singlepart(
                SinglePart::builder()
                    .header(header::ContentType::parse("message/delivery-status").unwrap())
                    .body(String::from("Final-Recipient: rfc822; user@example.com")),
            );

        assert_eq!(
            String::from_utf8(part.formatted()).unwrap(),
            concat!(
                "Content-Type: multipart/report;\r\n",
                " boundary=\"0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\";\r\n",
                " report-type=delivery-status\r\n",
                "\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Type: text/plain; charset=utf-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "Delivery failed\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Type: message/delivery-status\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "Final-Recipient: rfc822; user@example.com\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1--\r\n"
            )
        );
    }

    #[test]
    fn multi_part_report_rejects_invalid_report_type() {
        assert!(MultiPart::try_report("delivery-status").is_ok());
        assert!(MultiPart::try_report("message/delivery-status").is_err());
        assert!(MultiPart::try_report("delivery\r\nstatus").is_err());
        assert!(MultiPart::try_report("delivery\"status").is_err());
    }

    #[test]
    fn multi_part_encrypted() {
        let part = MultiPart::encrypted("application/pgp-encrypted".to_owned())
            .boundary("0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1")
            .singlepart(
                SinglePart::builder()
                    .header(header::ContentType::parse("application/pgp-encrypted").unwrap())
                    .body(String::from("Version: 1")),
            )
            .singlepart(
                SinglePart::builder()
                    .header(
                        ContentType::parse("application/octet-stream; name=\"encrypted.asc\"")
                            .unwrap(),
                    )
                    .header(header::ContentDisposition::inline_with_name(
                        "encrypted.asc",
                    ))
                    .body(String::from(concat!(
                        "-----BEGIN PGP MESSAGE-----\r\n",
                        "wV4D0dz5vDXklO8SAQdA5lGX1UU/eVQqDxNYdHa7tukoingHzqUB6wQssbMfHl8w\r\n",
                        "...\r\n",
                        "-----END PGP MESSAGE-----\r\n"
                    ))),
            );

        assert_eq!(
            String::from_utf8(part.formatted()).unwrap(),
            concat!(
                "Content-Type: multipart/encrypted;\r\n",
                " boundary=\"0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\";\r\n",
                " protocol=\"application/pgp-encrypted\"\r\n",
                "\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Type: application/pgp-encrypted\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "Version: 1\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Type: application/octet-stream; name=\"encrypted.asc\"\r\n",
                "Content-Disposition: inline; filename=\"encrypted.asc\"\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "-----BEGIN PGP MESSAGE-----\r\n",
                "wV4D0dz5vDXklO8SAQdA5lGX1UU/eVQqDxNYdHa7tukoingHzqUB6wQssbMfHl8w\r\n",
                "...\r\n",
                "-----END PGP MESSAGE-----\r\n",
                "\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1--\r\n"
            )
        );
    }
    #[test]
    fn multi_part_signed() {
        let part = MultiPart::signed(
            "application/pgp-signature".to_owned(),
            "pgp-sha256".to_owned(),
        )
        .boundary("0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1")
        .singlepart(
            SinglePart::builder()
                .header(header::ContentType::TEXT_PLAIN)
                .body(String::from("Test email for signature")),
        )
        .singlepart(
            SinglePart::builder()
                .header(
                    ContentType::parse("application/pgp-signature; name=\"signature.asc\"")
                        .unwrap(),
                )
                .header(header::ContentDisposition::attachment("signature.asc"))
                .body(String::from(concat!(
                    "-----BEGIN PGP SIGNATURE-----\r\n",
                    "\r\n",
                    "iHUEARYIAB0WIQTNsp3S/GbdE0KoiQ+IGQOscREZuQUCXyOzDAAKCRCIGQOscREZ\r\n",
                    "udgDAQCv3FJ3QWW5bRaGZAa0Ug6vASFdkvDMKoRwcoFnHPthjQEAiQ8skkIyE2GE\r\n",
                    "PoLpAXiKpT+NU8S8+8dfvwutnb4dSwM=\r\n",
                    "=3FYZ\r\n",
                    "-----END PGP SIGNATURE-----\r\n",
                ))),
        );

        assert_eq!(
            String::from_utf8(part.formatted()).unwrap(),
            concat!(
                "Content-Type: multipart/signed;\r\n",
                " boundary=\"0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\";\r\n",
                " protocol=\"application/pgp-signature\";",
                " micalg=\"pgp-sha256\"\r\n",
                "\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Type: text/plain; charset=utf-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "Test email for signature\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Type: application/pgp-signature; name=\"signature.asc\"\r\n",
                "Content-Disposition: attachment; filename=\"signature.asc\"\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "-----BEGIN PGP SIGNATURE-----\r\n",
                "\r\n",
                "iHUEARYIAB0WIQTNsp3S/GbdE0KoiQ+IGQOscREZuQUCXyOzDAAKCRCIGQOscREZ\r\n",
                "udgDAQCv3FJ3QWW5bRaGZAa0Ug6vASFdkvDMKoRwcoFnHPthjQEAiQ8skkIyE2GE\r\n",
                "PoLpAXiKpT+NU8S8+8dfvwutnb4dSwM=\r\n",
                "=3FYZ\r\n",
                "-----END PGP SIGNATURE-----\r\n",
                "\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1--\r\n"
            )
        );
    }

    #[test]
    fn multi_part_alternative() {
        let part = MultiPart::alternative()
            .boundary("0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1")
            .singlepart(SinglePart::builder()
                             .header(header::ContentType::TEXT_PLAIN)
                             .header(header::ContentTransferEncoding::Binary)
                             .body(String::from("Текст письма в уникоде")))
            .singlepart(SinglePart::builder()
                             .header(header::ContentType::TEXT_HTML)
                             .header(header::ContentTransferEncoding::Binary)
                             .body(String::from("<p>Текст <em>письма</em> в <a href=\"https://ru.wikipedia.org/wiki/Юникод\">уникоде</a><p>")));

        assert_eq!(
            String::from_utf8(part.formatted()).unwrap(),
            concat!(
                "Content-Type: multipart/alternative;\r\n",
                " boundary=\"0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\"\r\n",
                "\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Type: text/plain; charset=utf-8\r\n",
                "Content-Transfer-Encoding: binary\r\n",
                "\r\n",
                "Текст письма в уникоде\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Type: text/html; charset=utf-8\r\n",
                "Content-Transfer-Encoding: binary\r\n",
                "\r\n",
                "<p>Текст <em>письма</em> в <a href=\"https://ru.wikipedia.org/wiki/Юникод\">уникоде</a><p>\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1--\r\n"
            )
        );
    }

    #[test]
    fn multi_part_mixed_related() {
        let part = MultiPart::mixed()
            .boundary("0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1")
            .multipart(MultiPart::related()
                            .boundary("1oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1")
                            .singlepart(SinglePart::builder()
                                             .header(header::ContentType::TEXT_HTML)
                                             .header(header::ContentTransferEncoding::Binary)
                                             .body(String::from("<p>Текст <em>письма</em> в <a href=\"https://ru.wikipedia.org/wiki/Юникод\">уникоде</a><p>")))
                            .singlepart(SinglePart::builder()
                                             .header(header::ContentType::parse("image/png").unwrap())
                                             .header(header::ContentLocation::from(String::from("/image.png")))
                                             .header(header::ContentTransferEncoding::Base64)
                                             .body(String::from("1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"))))
            .singlepart(SinglePart::builder()
                             .header(header::ContentType::TEXT_PLAIN)
                             .header(header::ContentDisposition::attachment("example.c"))
                             .header(header::ContentTransferEncoding::Binary)
                             .body(String::from("int main() { return 0; }")));

        assert_eq!(
            String::from_utf8(part.formatted()).unwrap(),
            concat!(
                "Content-Type: multipart/mixed;\r\n",
                " boundary=\"0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\"\r\n",
                "\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Type: multipart/related;\r\n",
                " boundary=\"1oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\"\r\n",
                "\r\n",
                "--1oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Type: text/html; charset=utf-8\r\n",
                "Content-Transfer-Encoding: binary\r\n",
                "\r\n",
                "<p>Текст <em>письма</em> в <a href=\"https://ru.wikipedia.org/wiki/Юникод\">уникоде</a><p>\r\n",
                "--1oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Type: image/png\r\n",
                "Content-Location: /image.png\r\n",
                "Content-Transfer-Encoding: base64\r\n",
                "\r\n",
                "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3\r\n",
                "ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0\r\n",
                "NTY3ODkwMTIzNDU2Nzg5MA==\r\n",
                "--1oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1--\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1\r\n",
                "Content-Type: text/plain; charset=utf-8\r\n",
                "Content-Disposition: attachment; filename=\"example.c\"\r\n",
                "Content-Transfer-Encoding: binary\r\n",
                "\r\n",
                "int main() { return 0; }\r\n",
                "--0oVZ2r6AoLAhLlb0gPNSKy6BEqdS2IfwxrcbUuo1--\r\n"
            )
        );
    }

    #[test]
    fn multipart_with_a_boundary_less_content_type_gets_a_boundary() {
        let part = MultiPart::builder()
            .header(ContentType::parse("multipart/mixed").unwrap())
            .singlepart(SinglePart::plain("hello".to_owned()));

        assert!(!part.boundary().is_empty());
        assert!(!part.formatted().is_empty());
    }

    /// Every way the old unrestricted `headers_mut` could leave a multipart
    /// that panics or forges delimiters at format time is refused or repaired
    /// by the transaction, and a refusal leaves the headers untouched.
    #[test]
    fn multipart_header_edits_are_transactional() {
        let mut part = MultiPart::mixed()
            .boundary("BOUNDARY")
            .singlepart(SinglePart::plain("a\r\n--COLLIDE\r\nb".to_owned()));
        let before = part.headers().to_string();

        for refused in [
            ContentType::TEXT_PLAIN,
            ContentType::parse(&format!("multipart/mixed; boundary={}", "a".repeat(71))).unwrap(),
            ContentType::parse("multipart/mixed; boundary=\"ends in space \"").unwrap(),
        ] {
            assert!(part.edit_headers(|h| h.set(refused)).is_err());
            assert_eq!(part.headers().to_string(), before);
        }
        assert!(
            part.edit_headers(|h| {
                h.remove::<ContentType>();
                Ok(())
            })
            .is_err()
        );
        assert!(
            part.edit_headers(|h| h.set(ContentTransferEncoding::Base64))
                .is_err()
        );
        assert_eq!(part.headers().to_string(), before);

        // A boundary-less type gets one; a colliding one is re-rolled; a
        // raw spelling is re-set so the wire and `boundary()` agree.
        part.edit_headers(|h| h.set(ContentType::parse("multipart/alternative").unwrap()))
            .unwrap();
        assert!(is_mime_boundary(&part.boundary()));
        part.edit_headers(|h| {
            h.insert_raw(header::HeaderValue::new(
                ContentType::name(),
                "multipart/related; boundary=COLLIDE; boundary=other".to_owned(),
            ))
        })
        .unwrap();
        assert_ne!(part.boundary(), "COLLIDE");
        let wire = String::from_utf8(part.formatted()).unwrap();
        assert_eq!(wire.matches("boundary=").count(), 1);
        assert_eq!(wire.matches(&format!("--{}--", part.boundary())).count(), 1);
    }

    #[test]
    fn multipart_builder_refuses_non_multipart_types_and_encodings() {
        assert!(
            MultiPart::builder()
                .header(ContentType::TEXT_PLAIN)
                .is_err()
        );
        assert!(
            MultiPart::builder()
                .header(ContentTransferEncoding::QuotedPrintable)
                .is_err()
        );
        assert!(
            MultiPart::builder()
                .header(ContentTransferEncoding::SevenBit)
                .is_ok()
        );
    }

    /// Report cardinality moved from a format-time panic to the Message gate.
    #[test]
    fn incomplete_reports_are_refused_at_the_message_gate() {
        let from = || {
            crate::Message::builder()
                .from("a@example.org".parse().unwrap())
                .to("b@example.org".parse().unwrap())
        };
        let one = MultiPart::report("delivery-status".to_owned())
            .singlepart(SinglePart::plain("x".to_owned()));
        assert!(!one.formatted().is_empty());
        assert!(from().multipart(one.clone()).is_err());
        assert!(
            from()
                .multipart(MultiPart::mixed().multipart(one.clone()))
                .is_err()
        );
        let two = one.clone().singlepart(SinglePart::plain("y".to_owned()));
        assert!(from().multipart(two.clone()).is_ok());

        let mut bad_type = two.clone();
        bad_type
            .edit_headers(|h| {
                h.set(ContentType::parse("multipart/report; report-type=\"bad/type\"").unwrap())
            })
            .unwrap();
        assert!(from().multipart(bad_type).is_err());

        assert!(
            from()
                .header(ContentType::TEXT_PLAIN)
                .multipart(two)
                .is_err()
        );
    }

    #[test]
    fn a_body_containing_the_boundary_regenerates_it() {
        let part = MultiPart::mixed()
            .boundary("BOUNDARY")
            .singlepart(SinglePart::plain(
                "before\r\n--BOUNDARY\r\nafter".to_owned(),
            ));

        let formatted = String::from_utf8(part.formatted()).unwrap();

        assert_ne!(part.boundary(), "BOUNDARY");
        assert_eq!(formatted.matches("\r\n--BOUNDARY\r\n").count(), 1);
    }

    /// Finding 11: a boundary re-roll must replace the BOUNDARY, not rebuild
    /// the whole Content-Type from `MultiPartKind`. Rebuilding reproduces only
    /// the parameters this crate has a vocabulary for, so a caller's own
    /// parameter on the multipart Content-Type disappeared the moment a part
    /// forced a re-roll - and a re-roll is data-driven, so the same message
    /// keeps or loses the parameter depending on its body.
    #[test]
    fn a_boundary_re_roll_keeps_foreign_content_type_parameters() {
        let content_type =
            ContentType::parse("multipart/mixed; boundary=\"BOUNDARY\"; x-vendor=\"keep-me\"")
                .unwrap();

        // No collision: the Content-Type is untouched, parameter included.
        let quiet = MultiPart::builder()
            .header(content_type.clone())
            .singlepart(SinglePart::plain("nothing to see".to_owned()));
        assert_eq!(quiet.boundary(), "BOUNDARY");
        assert_eq!(
            quiet
                .headers()
                .get::<ContentType>()
                .unwrap()
                .as_ref()
                .get_param("x-vendor")
                .map(|value| value.as_str().to_owned()),
            Some("keep-me".to_owned())
        );

        // Collision: the boundary must change and the parameter must not.
        let re_rolled = MultiPart::builder()
            .header(content_type)
            .singlepart(SinglePart::plain(
                "before\r\n--BOUNDARY\r\nafter".to_owned(),
            ));
        assert_ne!(re_rolled.boundary(), "BOUNDARY");
        let mime = re_rolled.headers().get::<ContentType>().unwrap();
        assert_eq!(mime.as_ref().subtype().as_ref(), "mixed");
        assert_eq!(
            mime.as_ref()
                .get_param("x-vendor")
                .map(|value| value.as_str().to_owned()),
            Some("keep-me".to_owned()),
            "a re-roll must not drop parameters it does not understand"
        );
    }

    #[test]
    fn invalid_multipart_parameters_are_rejected() {
        assert!(MultiPart::mixed().try_boundary("").is_err());
        assert!(MultiPart::mixed().try_boundary("a\"b").is_err());
        assert!(MultiPart::mixed().try_boundary("a\r\nb").is_err());
        assert!(MultiPart::mixed().try_boundary("a".repeat(71)).is_err());
        assert!(MultiPart::try_encrypted("application/pgp\r\nRSET").is_err());
        assert!(MultiPart::try_signed("application/pgp-signature", "sha256\"").is_err());
    }

    #[test]
    fn report_type_token_validation_is_shared_by_both_constructors() {
        // `MultiPart::report` panics where `try_report` returns an error; both
        // go through `is_mime_token`.
        assert!(is_mime_token("delivery-status"));
        assert!(!is_mime_token(""));
        assert!(!is_mime_token("delivery status"));
        assert!(!is_mime_token("delivery\r\nstatus"));
        assert!(!is_mime_token("delivery/status"));
        assert!(!is_mime_token("delivery\u{80}status"));
    }

    /// The boundary edit drops every old boundary (quote and case aware),
    /// keeps other parameters and their whitespace, and gives the new unit a
    /// fresh single space, so a value that encoded keeps encoding.
    #[test]
    fn boundary_edit_replaces_every_boundary_and_keeps_spelling() {
        assert_eq!(
            with_boundary(
                "multipart/x-vendor; BOUNDARY=a;  x=\"q;boundary=z\";boundary=\"b\"",
                "new"
            ),
            "multipart/x-vendor; boundary=\"new\";  x=\"q;boundary=z\""
        );
        let edited = with_boundary(
            &format!("multipart/mixed;{}boundary=b", " ".repeat(988)),
            &make_boundary(),
        );
        let mut headers = Headers::new();
        headers.set_known(ContentType::parse(&edited).unwrap());
        let mime: Mime = edited.parse().unwrap();
        assert_eq!(mime.get_param(mime::BOUNDARY).unwrap().as_str().len(), 40);
    }

    #[test]
    fn test_make_boundary() {
        let mut boundaries = std::collections::HashSet::with_capacity(10);
        for _ in 0..1000 {
            boundaries.insert(make_boundary());
        }

        // Ensure there are no duplicates
        assert_eq!(1000, boundaries.len());

        // Ensure correct length
        for boundary in boundaries {
            assert_eq!(40, boundary.len());
        }
    }
}
