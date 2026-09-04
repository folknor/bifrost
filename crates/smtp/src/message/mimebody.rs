use std::io::Write;

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

impl Part {
    #[cfg(feature = "dkim")]
    pub(super) fn format_body(&self, out: &mut Vec<u8>) {
        match self {
            Part::Single(part) => part.format_body(out),
            Part::Multi(part) => part.format_body(out),
        }
    }
}

impl EmailFormat for Part {
    fn format(&self, out: &mut Vec<u8>) {
        match self {
            Part::Single(part) => part.format(out),
            Part::Multi(part) => part.format(out),
        }
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
    pub fn header<H: Header>(mut self, header: H) -> Self {
        self.headers.set(header);
        self
    }

    /// Set the Content-Type header of the singlepart
    pub fn content_type(mut self, content_type: ContentType) -> Self {
        self.headers.set(content_type);
        self
    }

    /// Build singlepart using body
    pub fn body<T: IntoBody>(mut self, body: T) -> SinglePart {
        let maybe_encoding = self.headers.get::<ContentTransferEncoding>();
        let body = body.into_body(maybe_encoding);

        if self.headers.get::<ContentType>().is_none()
            && let Some(content_type) = body.default_content_type()
        {
            self.headers.set(content_type);
        }
        self.headers.set(body.encoding());

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
///     .header(header::ContentType::TEXT_PLAIN)
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
        Self::builder()
            .header(header::ContentType::TEXT_PLAIN)
            .body(body)
    }

    /// Directly create a `SinglePart` from a UTF-8 HTML content
    pub fn html<T: IntoBody>(body: T) -> Self {
        Self::builder()
            .header(header::ContentType::TEXT_HTML)
            .body(body)
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

    /// Format only the signlepart body
    fn format_body(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.body);
        out.extend_from_slice(b"\r\n");
    }
}

impl EmailFormat for SinglePart {
    fn format(&self, out: &mut Vec<u8>) {
        write!(out, "{}", self.headers)
            .expect("A Write implementation panicked while formatting headers");
        out.extend_from_slice(b"\r\n");
        self.format_body(out);
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

/// Rebuild `mime` with its `boundary` parameter replaced, keeping every other
/// parameter - including ones this crate has no vocabulary for - exactly as
/// the caller wrote it.
///
/// Returns `None` when a parameter value cannot be re-emitted losslessly as a
/// quoted string (it contains a `"` or a `\`); the caller then falls back to
/// rebuilding from `MultiPartKind`, which is what always happened before.
fn replace_boundary_param(mime: &Mime, boundary: &str) -> Option<Mime> {
    let mut rebuilt = format!("{}/{}", mime.type_(), mime.subtype());
    if let Some(suffix) = mime.suffix() {
        rebuilt.push('+');
        rebuilt.push_str(suffix.as_str());
    }
    rebuilt.push_str(&format!("; boundary=\"{boundary}\""));

    for (name, value) in mime.params() {
        if name == mime::BOUNDARY {
            continue;
        }
        let value = value.as_str();
        if value.contains('"') || value.contains('\\') {
            return None;
        }
        rebuilt.push_str(&format!("; {name}=\"{value}\""));
    }

    rebuilt.parse().ok()
}

/// Create a cryptographically random MIME boundary.
fn make_boundary() -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut random = [0_u8; 20];
    getrandom::fill(&mut random).expect("operating system random source is unavailable");
    let mut boundary = String::with_capacity(random.len() * 2);
    for byte in random {
        boundary.push(HEX[usize::from(byte >> 4)] as char);
        boundary.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    boundary
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

    fn from_mime(m: &Mime) -> Option<Self> {
        match m.subtype().as_ref() {
            "mixed" => Some(Self::Mixed),
            "alternative" => Some(Self::Alternative),
            "related" => Some(Self::Related),
            "report" => m.get_param("report-type").map(|report_type| Self::Report {
                report_type: report_type.as_str().to_owned(),
            }),
            "signed" => m.get_param("protocol").and_then(|p| {
                m.get_param("micalg").map(|micalg| Self::Signed {
                    protocol: p.as_str().to_owned(),
                    micalg: micalg.as_str().to_owned(),
                })
            }),
            "encrypted" => m.get_param("protocol").map(|p| Self::Encrypted {
                protocol: p.as_str().to_owned(),
            }),
            _ => None,
        }
    }
}

fn is_mime_token(value: &str) -> bool {
    const TSPECIALS: &[u8] = b"()<>@,;:\\\"/[]?=";

    !value.is_empty()
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
        && value
            .bytes()
            .all(|byte| (b' '..=b'~').contains(&byte) && !matches!(byte, b'"' | b'\\'))
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
    pub fn header<H: Header>(mut self, header: H) -> Self {
        self.headers.set(header);
        self
    }

    /// Set `Content-Type` header using [`MultiPartKind`]
    pub fn kind(self, kind: MultiPartKind) -> Self {
        self.header(ContentType::from_mime(kind.to_mime::<String>(None)))
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
        let kind = self
            .headers
            .get::<ContentType>()
            .and_then(|content_type| MultiPartKind::from_mime(content_type.as_ref()))
            .unwrap_or(MultiPartKind::Mixed);
        let mime = kind.to_mime(Some(boundary));
        self.headers.set(ContentType::from_mime(mime));
        Ok(self)
    }

    /// Creates multipart without parts
    pub fn build(mut self) -> MultiPart {
        match self.headers.get::<ContentType>() {
            None => self.headers.set(ContentType::from_mime(
                MultiPartKind::Mixed.to_mime::<String>(None),
            )),
            Some(content_type) if content_type.as_ref().get_param("boundary").is_none() => {
                let kind =
                    MultiPartKind::from_mime(content_type.as_ref()).unwrap_or(MultiPartKind::Mixed);
                self.headers
                    .set(ContentType::from_mime(kind.to_mime::<String>(None)));
            }
            Some(_) => {}
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

    /// Get a mutable reference to the headers
    pub fn headers_mut(&mut self) -> &mut Headers {
        &mut self.headers
    }

    /// Get message content formatted for SMTP
    pub fn formatted(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.format(&mut out);
        out
    }

    /// Format only the multipart body
    fn format_body(&self, out: &mut Vec<u8>) {
        let boundary = self.boundary();

        for part in &self.parts {
            out.extend_from_slice(b"--");
            out.extend_from_slice(boundary.as_bytes());
            out.extend_from_slice(b"\r\n");
            part.format(out);
        }

        out.extend_from_slice(b"--");
        out.extend_from_slice(boundary.as_bytes());
        out.extend_from_slice(b"--\r\n");
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
        if !self.parts_contain_boundary(&current, from) {
            return;
        }

        let existing = self.headers.get::<ContentType>();
        let kind = existing
            .as_ref()
            .and_then(|content_type| MultiPartKind::from_mime(content_type.as_ref()))
            .unwrap_or(MultiPartKind::Mixed);
        loop {
            let boundary = make_boundary();
            if !self.parts_contain_boundary(&boundary, 0) {
                // Re-roll the BOUNDARY, not the whole Content-Type. Rebuilding
                // from `MultiPartKind` alone reproduces only the parameters
                // this crate knows about, so any parameter a caller set on the
                // multipart Content-Type itself - a vendor param, a `charset`,
                // anything outside the kind's own vocabulary - vanished the
                // moment a part forced a re-roll.
                let mime = existing
                    .as_ref()
                    .and_then(|content_type| {
                        replace_boundary_param(content_type.as_ref(), &boundary)
                    })
                    .unwrap_or_else(|| kind.to_mime(Some(boundary)));
                self.headers.set(ContentType::from_mime(mime));
                return;
            }
        }
    }

    fn parts_contain_boundary(&self, boundary: &str, from: usize) -> bool {
        let opening = format!("--{boundary}");
        let marker = format!("\r\n{opening}");
        self.parts[from..].iter().any(|part| {
            let mut formatted = Vec::new();
            part.format(&mut formatted);
            formatted.starts_with(opening.as_bytes())
                || formatted
                    .windows(marker.len())
                    .any(|window| window == marker.as_bytes())
        })
    }
}

impl EmailFormat for MultiPart {
    fn format(&self, out: &mut Vec<u8>) {
        if self
            .headers
            .get::<ContentType>()
            .and_then(|content_type| MultiPartKind::from_mime(content_type.as_ref()))
            .is_some_and(|kind| matches!(kind, MultiPartKind::Report { .. }))
        {
            assert!(
                (2..=3).contains(&self.parts.len()),
                "multipart/report requires two or three body parts"
            );
        }

        write!(out, "{}", self.headers)
            .expect("A Write implementation panicked while formatting headers");
        out.extend_from_slice(b"\r\n");
        self.format_body(out);
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
    #[should_panic(expected = "multipart/report requires two or three body parts")]
    fn multi_part_report_rejects_wrong_part_count() {
        let part = MultiPart::report("delivery-status".to_owned())
            .singlepart(SinglePart::plain("Delivery failed".to_owned()));

        let _ = part.formatted();
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
