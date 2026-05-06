use std::collections::HashMap;

use serde::Deserialize;

use crate::Error;

#[derive(Debug, Clone, Deserialize)]
pub struct ParseResponse<T> {
    #[serde(rename = "accountId")]
    account_id: String,

    #[serde(rename = "parsed")]
    parsed: Option<HashMap<String, T>>,

    #[serde(rename = "notParsable")]
    not_parsable: Option<Vec<String>>,

    #[serde(rename = "notFound")]
    not_found: Option<Vec<String>>,
}

impl<T> ParseResponse<T> {
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub fn parsed(&mut self, blob_id: &str) -> crate::Result<T> {
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

    pub fn parsed_list(&self) -> Option<impl Iterator<Item = (&String, &T)>> {
        self.parsed.as_ref().map(|map| map.iter())
    }

    pub fn not_parsable(&self) -> Option<&[String]> {
        self.not_parsable.as_deref()
    }

    pub fn not_found(&self) -> Option<&[String]> {
        self.not_found.as_deref()
    }
}
