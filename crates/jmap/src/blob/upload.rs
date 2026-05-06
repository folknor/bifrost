use serde::Deserialize;

use crate::{
    client::Client,
    core::session::URLPart,
};

#[derive(Debug, Deserialize)]
pub struct UploadResponse {
    #[serde(rename = "accountId")]
    account_id: String,

    #[serde(rename = "blobId")]
    blob_id: String,

    #[serde(rename = "type")]
    type_: String,

    #[serde(rename = "size")]
    size: usize,
}

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn upload(
        &self,
        account_id: Option<&str>,
        blob: Vec<u8>,
        content_type: Option<&str>,
    ) -> crate::Result<UploadResponse> {
        let account_id = account_id.unwrap_or_else(|| self.default_account_id());
        let mut upload_url =
            String::with_capacity(self.session().upload_url().len() + account_id.len());

        for part in self.upload_url() {
            match part {
                URLPart::Value(value) => upload_url.push_str(value),
                URLPart::Parameter(param) => {
                    if let super::URLParameter::AccountId = param {
                        upload_url.push_str(account_id);
                    }
                }
            }
        }

        let bytes = self
            .transport()
            .upload(&upload_url, blob, content_type)
            .await
            .map_err(crate::Error::from)?;
        serde_json::from_slice::<UploadResponse>(&bytes).map_err(std::convert::Into::into)
    }
}

impl UploadResponse {
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub fn blob_id(&self) -> &str {
        &self.blob_id
    }

    pub fn content_type(&self) -> &str {
        &self.type_
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn take_blob_id(&mut self) -> String {
        std::mem::take(&mut self.blob_id)
    }
}
