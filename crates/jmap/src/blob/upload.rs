use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde::Deserialize;

use crate::{
    account::Account,
    blob::BlobRef,
    client::Client,
    core::{
        id::{AccountId, BlobId},
        session::URLPart,
        transport::HttpTransport,
    },
};

/// Server response shape for an HTTP upload (RFC 8620 §6.1).
///
/// The deserialized form is mostly internal: callers receive a
/// [`BlobRef`] from [`Account::upload`] and don't need to handle
/// `UploadResponse` directly. It is `pub` so the lower-level
/// [`Client::upload_to`] is usable when the caller needs the raw
/// `accountId` echo or `size` field.
#[derive(Debug, Deserialize)]
pub struct UploadResponse {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "blobId")]
    blob_id: BlobId,

    #[serde(rename = "type")]
    type_: String,

    #[serde(rename = "size")]
    size: usize,
}

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
    /// Upload `data` to the named account's blob store. Lower-level
    /// counterpart to [`Account::upload`] for callers that need the
    /// raw [`UploadResponse`] (echoing server-reported size, etc.).
    pub async fn upload_to(
        &self,
        account_id: &AccountId,
        data: impl Into<Vec<u8>>,
        content_type: Option<&str>,
    ) -> crate::Result<UploadResponse> {
        let mut upload_url =
            String::with_capacity(self.session().upload_url().len() + account_id.as_str().len());

        for part in self.upload_url() {
            match part {
                URLPart::Value(value) => upload_url.push_str(value),
                URLPart::Parameter(param) => {
                    if let super::URLParameter::AccountId = param {
                        upload_url.extend(utf8_percent_encode(account_id.as_str(), PATH_SEGMENT));
                    }
                }
            }
        }

        let bytes = self
            .transport()
            .upload(&upload_url, data.into(), content_type)
            .await
            .map_err(crate::Error::from)?;
        serde_json::from_slice::<UploadResponse>(&bytes).map_err(std::convert::Into::into)
    }
}

impl<Tr: HttpTransport> Account<Tr> {
    /// Upload `data` to this account's blob store.
    ///
    /// The returned [`BlobRef`] is bound to this account and carries
    /// the server-assigned `blobId` and content type. The hint name
    /// (used by `Client::download` to populate
    /// `Content-Disposition`) defaults to `None`; chain
    /// [`BlobRef::with_name`] if you have one.
    ///
    /// Asymmetric placement vs. [`Client::download`] is intentional:
    /// the upload URL is templated with `accountId`, so it needs an
    /// account context that does not yet exist in `BlobRef` form.
    /// After the upload the returned `BlobRef` records the binding,
    /// and `Client::download(&blob_ref)` works against any account
    /// the ref points at.
    pub async fn upload(
        &self,
        data: impl Into<Vec<u8>>,
        content_type: Option<&str>,
    ) -> crate::Result<BlobRef> {
        let response = self
            .client()
            .upload_to(self.id(), data, content_type)
            .await?;

        let mut blob = BlobRef::new(response.account_id, response.blob_id);
        if !response.type_.is_empty() {
            blob.content_type = Some(response.type_);
        }
        Ok(blob)
    }
}

impl UploadResponse {
    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn blob_id(&self) -> &BlobId {
        &self.blob_id
    }

    pub fn content_type(&self) -> &str {
        &self.type_
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn into_blob_id(self) -> BlobId {
        self.blob_id
    }
}
