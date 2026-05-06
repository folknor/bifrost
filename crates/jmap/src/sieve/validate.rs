use serde::{Deserialize, Serialize};

use crate::core::set::SetError;

#[derive(Debug, Clone, Serialize)]
pub struct SieveScriptValidateRequest {
    #[serde(rename = "accountId")]
    account_id: String,

    #[serde(rename = "blobId")]
    blob_id: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct SieveScriptValidateResponse {
    #[serde(rename = "accountId")]
    account_id: String,

    error: Option<SetError<String>>,
}

impl crate::core::method::JmapMethod for SieveScriptValidateRequest {
    const NAME: &'static str = "SieveScript/validate";
    type Cap = crate::core::capability::Sieve;
    type Response = SieveScriptValidateResponse;
}

impl SieveScriptValidateRequest {
    pub fn new(account_id: impl Into<String>, blob_id: impl Into<String>) -> Self {
        SieveScriptValidateRequest {
            account_id: account_id.into(),
            blob_id: blob_id.into(),
        }
    }
}

impl SieveScriptValidateResponse {
    pub fn unwrap_error(self) -> crate::Result<()> {
        match self.error {
            Some(err) => Err(err.into()),
            None => Ok(()),
        }
    }
}
