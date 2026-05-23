use std::collections::HashMap;

use serde::Deserialize;

use crate::Error;

use super::id::{AccountId, BlobId};

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ParseResponse<T> {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "parsed")]
    parsed: Option<HashMap<BlobId, T>>,

    #[serde(rename = "notParsable")]
    not_parsable: Option<Vec<BlobId>>,

    #[serde(rename = "notFound")]
    not_found: Option<Vec<BlobId>>,
}

impl<T> ParseResponse<T> {
    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn parsed(&mut self, blob_id: &BlobId) -> crate::Result<T> {
        if let Some(result) = self.parsed.as_mut().and_then(|r| r.remove(blob_id)) {
            Ok(result)
        } else if self
            .not_parsable
            .as_ref()
            .is_some_and(|np| np.iter().any(|id| id == blob_id))
        {
            Err(Error::NotParsable(blob_id.to_string()))
        } else {
            Err(Error::IdNotFound(blob_id.to_string()))
        }
    }

    pub(crate) fn parsed_list(&self) -> Option<impl Iterator<Item = (&BlobId, &T)>> {
        self.parsed.as_ref().map(|map| map.iter())
    }

    pub(crate) fn not_parsable(&self) -> Option<&[BlobId]> {
        self.not_parsable.as_deref()
    }

    pub(crate) fn not_found(&self) -> Option<&[BlobId]> {
        self.not_found.as_deref()
    }
}
