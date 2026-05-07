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

    fn set_account_id(&mut self, account_id: &str) {
        self.account_id = account_id.to_string();
    }
}

impl SieveScriptValidateRequest {
    pub fn new(blob_id: impl Into<String>) -> Self {
        SieveScriptValidateRequest {
            account_id: String::new(),
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
