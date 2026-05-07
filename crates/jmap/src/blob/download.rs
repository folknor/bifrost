use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};

use crate::{
    blob::BlobRef,
    client::Client,
    core::{session::URLPart, transport::HttpTransport},
};

/// Characters to percent-encode inside a URL path segment. Mirrors the
/// `path` set from RFC 3986 with `/` added so a content-type like
/// `image/png` cannot break out into a new path segment.
const PATH_SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'`')
    .add(b'#')
    .add(b'?')
    .add(b'{')
    .add(b'}')
    .add(b'/')
    .add(b'%');

impl<Tr: HttpTransport> Client<Tr> {
    /// Download a blob.
    ///
    /// The account ID, blob ID, and (when the server's download URL
    /// template references them) `name` and `type` come from the
    /// `BlobRef`. RFC 8620 §6 specifies `accountId` as required, and
    /// `name` / `type` as advisory hints used to populate
    /// `Content-Disposition` and `Content-Type` on the response. When
    /// the ref leaves them as `None` and the template needs a value,
    /// neutral defaults (`"download"`, `"application/octet-stream"`)
    /// are substituted.
    ///
    /// `name` and `content_type` are percent-encoded for path-segment
    /// safety - a content type like `"image/png"` won't be misread as
    /// a directory boundary.
    pub async fn download(&self, blob: &BlobRef) -> crate::Result<bytes::Bytes> {
        let mut download_url = String::with_capacity(self.session().download_url().len() + 64);

        for part in self.download_url() {
            match part {
                URLPart::Value(value) => download_url.push_str(value),
                URLPart::Parameter(param) => match param {
                    super::URLParameter::AccountId => {
                        download_url
                            .extend(utf8_percent_encode(blob.account_id.as_str(), PATH_SEGMENT));
                    }
                    super::URLParameter::BlobId => {
                        download_url
                            .extend(utf8_percent_encode(blob.blob_id.as_str(), PATH_SEGMENT));
                    }
                    super::URLParameter::Name => {
                        let name = blob.name.as_deref().unwrap_or("download");
                        download_url.extend(utf8_percent_encode(name, PATH_SEGMENT));
                    }
                    super::URLParameter::Type => {
                        let ctype = blob
                            .content_type
                            .as_deref()
                            .unwrap_or("application/octet-stream");
                        download_url.extend(utf8_percent_encode(ctype, PATH_SEGMENT));
                    }
                },
            }
        }

        self.transport()
            .download(&download_url)
            .await
            .map_err(crate::Error::from)
    }
}
