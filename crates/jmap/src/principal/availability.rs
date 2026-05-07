use serde::{Deserialize, Serialize};

/// Request for `Principal/getAvailability`.
///
/// Given a principal and time range, returns free/busy availability.
#[derive(Debug, Clone, Serialize)]
pub struct PrincipalGetAvailabilityRequest {
    #[serde(rename = "accountId")]
    account_id: String,

    #[serde(rename = "id")]
    id: String,

    #[serde(rename = "utcStart")]
    utc_start: String,

    #[serde(rename = "utcEnd")]
    utc_end: String,

    #[serde(rename = "showDetails")]
    #[serde(skip_serializing_if = "Option::is_none")]
    show_details: Option<bool>,
}

/// Response for `Principal/getAvailability`.
#[derive(Debug, Clone, Deserialize)]
pub struct PrincipalGetAvailabilityResponse {
    #[serde(rename = "accountId")]
    account_id: String,

    #[serde(rename = "list")]
    list: Vec<AvailabilityEntry>,
}

/// A single availability entry (busy period).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvailabilityEntry {
    #[serde(rename = "utcStart")]
    pub utc_start: String,

    #[serde(rename = "utcEnd")]
    pub utc_end: String,

    #[serde(rename = "busyStatus")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub busy_status: Option<String>,

    #[serde(rename = "event")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<serde_json::Value>,
}

impl crate::core::method::JmapMethod for PrincipalGetAvailabilityRequest {
    const NAME: &'static str = "Principal/getAvailability";
    type Cap = crate::core::capability::Principals;
    type Response = PrincipalGetAvailabilityResponse;
}

impl PrincipalGetAvailabilityRequest {
    pub fn new(
        account_id: impl Into<String>,
        id: impl Into<String>,
        utc_start: impl Into<String>,
        utc_end: impl Into<String>,
    ) -> Self {
        PrincipalGetAvailabilityRequest {
            account_id: account_id.into(),
            id: id.into(),
            utc_start: utc_start.into(),
            utc_end: utc_end.into(),
            show_details: None,
        }
    }

    pub fn show_details(&mut self, show: bool) -> &mut Self {
        self.show_details = Some(show);
        self
    }
}

impl PrincipalGetAvailabilityResponse {
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub fn list(&self) -> &[AvailabilityEntry] {
        &self.list
    }

    pub fn take_list(&mut self) -> Vec<AvailabilityEntry> {
        std::mem::take(&mut self.list)
    }
}
