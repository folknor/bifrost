use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{Error, core::set::SetError};

#[derive(Debug, Clone, Serialize)]
pub struct CopyBlobRequest {
    #[serde(rename = "fromAccountId")]
    from_account_id: String,
    #[serde(rename = "accountId")]
    account_id: String,
    #[serde(rename = "blobIds")]
    blob_ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CopyBlobResponse {
    #[serde(rename = "fromAccountId")]
    from_account_id: String,
    #[serde(rename = "accountId")]
    account_id: String,
    #[serde(rename = "copied")]
    copied: Option<HashMap<String, String>>,
    #[serde(rename = "notCopied")]
    not_copied: Option<HashMap<String, SetError<String>>>,
}

impl crate::core::method::JmapMethod for CopyBlobRequest {
    const NAME: &'static str = "Blob/copy";
    type Cap = crate::core::capability::Core;
    type Response = CopyBlobResponse;

    fn set_account_id(&mut self, account_id: &str) {
        self.account_id = account_id.to_string();
    }
}

impl CopyBlobRequest {
    /// Construct a `CopyBlobRequest`. The destination `accountId` is
    /// left empty and filled in by
    /// [`crate::core::request::Request::call`] when the method is
    /// added to a request batch (the destination is the account that
    /// owns the request). The source `fromAccountId` is the only
    /// account argument here, since it is genuinely a per-call value.
    pub fn new(from_account_id: impl Into<String>) -> Self {
        CopyBlobRequest {
            from_account_id: from_account_id.into(),
            account_id: String::new(),
            blob_ids: vec![],
        }
    }

    #[must_use]
    pub fn blob_id(mut self, blob_id: impl Into<String>) -> Self {
        self.blob_ids.push(blob_id.into());
        self
    }
}

impl CopyBlobResponse {
    pub fn from_account_id(&self) -> &str {
        &self.from_account_id
    }

    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub fn copied(&mut self, id: &str) -> crate::Result<String> {
        if let Some(result) = self.copied.as_mut().and_then(|r| r.remove(id)) {
            Ok(result)
        } else if let Some(error) = self.not_copied.as_mut().and_then(|r| r.remove(id)) {
            Err(error.to_string_error().into())
        } else {
            Err(Error::IdNotFound(id.to_string()))
        }
    }

    pub fn copied_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.copied.as_ref().map(|map| map.keys())
    }

    pub fn not_copied_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.not_copied.as_ref().map(|map| map.keys())
    }

    pub fn not_copied_reason(&self, id: &str) -> Option<&SetError<String>> {
        self.not_copied.as_ref().and_then(|map| map.get(id))
    }
}
