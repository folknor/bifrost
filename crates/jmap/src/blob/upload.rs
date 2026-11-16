use serde::Deserialize;

use crate::{
    account::Account,
    blob::BlobRef,
    client::Client,
    core::{
        id::{AccountId, BlobId},
        session::{TemplateValue, expand},
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
pub(crate) struct UploadResponse {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "blobId")]
    blob_id: BlobId,

    #[serde(rename = "type")]
    type_: String,

    #[serde(rename = "size")]
    size: usize,
}

impl Client {
    /// Upload `data` to the named account's blob store. Lower-level
    /// counterpart to [`Account::upload`] for callers that need the
    /// raw [`UploadResponse`] (echoing server-reported size, etc.).
    pub(crate) async fn upload_to(
        &self,
        account_id: &AccountId,
        data: impl Into<Vec<u8>>,
        content_type: Option<&str>,
    ) -> crate::Result<UploadResponse> {
        let state = self.session_state();
        let data: Vec<u8> = data.into();
        // Refuse a known oversize file before handing its bytes to the
        // transport; the server's maxSizeUpload applies per upload request.
        if let Some(max) = state.session().core_limits().max_size_upload()
            && data.len() > max
        {
            return Err(crate::Error::UploadSizeLimit {
                max,
                size: data.len(),
            });
        }
        // A malformed upload template is refused here, per upload, as the
        // server's contract violation; the rest of the session stays usable.
        let upload_url = expand(state.upload_url()?, |param| {
            TemplateValue::Scalar(match param {
                super::UploadParameter::AccountId => account_id.as_str().into(),
            })
        });

        let bytes = self
            .transport()
            .upload(&upload_url, data, content_type)
            .await
            .map_err(crate::Error::from)?;
        serde_json::from_slice::<UploadResponse>(&bytes).map_err(crate::Error::EnvelopeDecode)
    }
}

impl<C: crate::core::capability::Capability> Account<C> {
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
    pub(crate) async fn upload(
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
    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn blob_id(&self) -> &BlobId {
        &self.blob_id
    }

    pub(crate) fn content_type(&self) -> &str {
        &self.type_
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }

    pub(crate) fn into_blob_id(self) -> BlobId {
        self.blob_id
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        client::Client,
        core::{id::AccountId, session::Session},
    };

    fn upload_server() -> std::sync::Arc<bifrost_net::test_support::ScriptedDispatch> {
        crate::test_wire::server(|request| {
            assert_eq!(
                request.method,
                reqwest::Method::POST,
                "only uploads reach this server"
            );
            crate::test_wire::ok(
                &br#"{"accountId":"a","blobId":"b","type":"text/plain","size":4}"#[..],
            )
        })
    }

    fn session() -> Session {
        serde_json::from_value(json!({
            "capabilities": {},
            "accounts": {},
            "primaryAccounts": {},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/dl/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload?account={accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session fixture parses")
    }

    fn session_with_upload_limit(max_size_upload: usize) -> Session {
        serde_json::from_value(json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {"maxSizeUpload": max_size_upload}
            },
            "accounts": {},
            "primaryAccounts": {},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/dl/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload?account={accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session fixture parses")
    }

    /// Same RFC 6570 §3.2.2 rule as the download template: the account
    /// id is expanded with everything outside the unreserved set
    /// percent-encoded, not just the two query delimiters.
    #[tokio::test]
    async fn account_id_is_rfc6570_level_one_encoded() {
        let script = upload_server();
        let client: Client = crate::test_wire::client(&script, session());

        client
            .upload_to(&AccountId::new("a&b=c+d;e@f"), b"data".to_vec(), None)
            .await
            .expect("upload runs");

        let requests = script.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].url.as_str(),
            "https://example.test/upload?account=a%26b%3Dc%2Bd%3Be%40f"
        );
    }

    #[tokio::test]
    async fn upload_larger_than_the_session_limit_never_reaches_the_transport() {
        let script = upload_server();
        let client: Client = crate::test_wire::client(&script, session_with_upload_limit(3));

        let error = client
            .upload_to(&AccountId::new("a"), b"four".to_vec(), None)
            .await
            .expect_err("the advertised maximum is enforced locally");
        assert!(matches!(
            error,
            crate::Error::UploadSizeLimit { max: 3, size: 4 }
        ));
        assert!(script.requests().is_empty());
    }

    #[tokio::test]
    async fn upload_equal_to_the_session_limit_is_allowed() {
        let script = upload_server();
        let client: Client = crate::test_wire::client(&script, session_with_upload_limit(4));

        client
            .upload_to(&AccountId::new("a"), b"four".to_vec(), None)
            .await
            .expect("an upload at the advertised maximum is allowed");
        assert_eq!(script.requests().len(), 1);
    }
}
