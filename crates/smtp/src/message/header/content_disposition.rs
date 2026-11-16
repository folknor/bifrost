use super::{Header, HeaderName, HeaderValue};
use crate::BoxError;

/// `Content-Disposition` of an attachment
///
/// Defined in [RFC2183](https://tools.ietf.org/html/rfc2183)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentDisposition {
    kind: &'static str,
    file_name: Option<String>,
}

impl ContentDisposition {
    /// An attachment which should be displayed inline into the message
    pub fn inline() -> Self {
        Self {
            kind: "inline",
            file_name: None,
        }
    }

    /// An attachment which should be displayed inline into the message, but that also
    /// species the filename in case it is downloaded
    pub fn inline_with_name(file_name: &str) -> Self {
        Self::with_name("inline", file_name)
    }

    /// An attachment which is separate from the body of the message, and can be downloaded separately
    pub fn attachment(file_name: &str) -> Self {
        Self::with_name("attachment", file_name)
    }

    fn with_name(kind: &'static str, file_name: &str) -> Self {
        Self {
            kind,
            file_name: Some(file_name.to_owned()),
        }
    }
}

impl Header for ContentDisposition {
    fn name() -> HeaderName {
        HeaderName::new_from_ascii_str("Content-Disposition")
    }

    fn parse(s: &str) -> Result<Self, BoxError> {
        match (s.split_once(';'), s) {
            (_, "inline") => Ok(Self::inline()),
            (Some((kind @ ("inline" | "attachment"), file_name)), _) => file_name
                .split_once(" filename=\"")
                .and_then(|(_, file_name)| file_name.strip_suffix('"'))
                .map(|file_name| {
                    Self::with_name(
                        if kind == "inline" {
                            "inline"
                        } else {
                            "attachment"
                        },
                        file_name,
                    )
                })
                .ok_or_else(|| "Unsupported ContentDisposition value".into()),
            _ => Err("Unsupported ContentDisposition value".into()),
        }
    }

    fn display(&self) -> HeaderValue {
        match &self.file_name {
            None => HeaderValue::structured(Self::name(), self.kind.to_owned()),
            // The raw spelling is the one `parse` reads back; the wire form
            // comes from the shared writer (RFC 2231 when long or non-ASCII).
            Some(file_name) => HeaderValue::parameterized(
                Self::name(),
                format!("{}; filename=\"{file_name}\"", self.kind),
                self.kind.to_owned(),
                vec![("filename".to_owned(), file_name.clone())],
            ),
        }
    }
}

#[cfg(test)]
mod test {

    use super::ContentDisposition;
    use crate::message::header::{HeaderName, HeaderValue, Headers};

    #[test]
    fn format_content_disposition() {
        let mut headers = Headers::new();

        headers.set_known(ContentDisposition::inline());

        assert_eq!(format!("{headers}"), "Content-Disposition: inline\r\n");

        headers.set_known(ContentDisposition::attachment("something.txt"));

        assert_eq!(
            format!("{headers}"),
            "Content-Disposition: attachment; filename=\"something.txt\"\r\n"
        );
    }

    #[test]
    fn parse_content_disposition() {
        let mut headers = Headers::new();

        headers.insert_known(HeaderValue::new(
            HeaderName::new_from_ascii_str("Content-Disposition"),
            "inline".to_owned(),
        ));

        assert_eq!(
            headers.get::<ContentDisposition>(),
            Some(ContentDisposition::inline())
        );

        headers.insert_known(HeaderValue::new(
            HeaderName::new_from_ascii_str("Content-Disposition"),
            "attachment; filename=\"something.txt\"".to_owned(),
        ));

        assert_eq!(
            headers.get::<ContentDisposition>(),
            Some(ContentDisposition::attachment("something.txt"))
        );
    }
}
