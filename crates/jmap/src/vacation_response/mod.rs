pub mod get;
pub mod set;

use std::fmt::Display;

use crate::core::set::skip_if_empty_str;
use crate::core::set::skip_if_zero_date;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VacationResponse {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<String>,

    #[serde(rename = "isEnabled")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_enabled: Option<bool>,

    #[serde(rename = "fromDate")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) from_date: Option<DateTime<Utc>>,

    #[serde(rename = "toDate")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) to_date: Option<DateTime<Utc>>,

    #[serde(rename = "subject")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) subject: Option<String>,

    #[serde(rename = "textBody")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) text_body: Option<String>,

    #[serde(rename = "htmlBody")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) html_body: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VacationResponseCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<usize>,

    #[serde(rename = "isEnabled")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_enabled: Option<bool>,

    #[serde(rename = "fromDate")]
    #[serde(skip_serializing_if = "skip_if_zero_date")]
    pub(super) from_date: Option<DateTime<Utc>>,

    #[serde(rename = "toDate")]
    #[serde(skip_serializing_if = "skip_if_zero_date")]
    pub(super) to_date: Option<DateTime<Utc>>,

    #[serde(rename = "subject")]
    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub(super) subject: Option<String>,

    #[serde(rename = "textBody")]
    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub(super) text_body: Option<String>,

    #[serde(rename = "htmlBody")]
    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub(super) html_body: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct VacationResponsePatch {
    #[serde(rename = "isEnabled")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_enabled: Option<bool>,

    #[serde(rename = "fromDate")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) from_date: Option<DateTime<Utc>>,

    #[serde(rename = "toDate")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) to_date: Option<DateTime<Utc>>,

    #[serde(rename = "subject")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) subject: Option<String>,

    #[serde(rename = "textBody")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) text_body: Option<String>,

    #[serde(rename = "htmlBody")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) html_body: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub enum Property {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "isEnabled")]
    IsEnabled,
    #[serde(rename = "fromDate")]
    FromDate,
    #[serde(rename = "toDate")]
    ToDate,
    #[serde(rename = "subject")]
    Subject,
    #[serde(rename = "textBody")]
    TextBody,
    #[serde(rename = "htmlBody")]
    HtmlBody,
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::IsEnabled => write!(f, "isEnabled"),
            Property::FromDate => write!(f, "fromDate"),
            Property::ToDate => write!(f, "toDate"),
            Property::Subject => write!(f, "subject"),
            Property::TextBody => write!(f, "textBody"),
            Property::HtmlBody => write!(f, "htmlBody"),
        }
    }
}

impl crate::core::Object for VacationResponse {
    type Property = Property;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for VacationResponse {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for VacationResponse {
    type GetArguments = ();
}

impl crate::core::set::SetObject for VacationResponse {
    type Create = VacationResponseCreate;
    type Patch = VacationResponsePatch;
    type SetArguments = ();
}

impl crate::core::SetCreate for VacationResponseCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }

    fn new(create_id: Option<usize>) -> Self {
        use crate::core::set::from_timestamp;
        VacationResponseCreate {
            _create_id: create_id,
            is_enabled: None,
            from_date: from_timestamp(0).into(),
            to_date: from_timestamp(0).into(),
            subject: String::new().into(),
            text_body: String::new().into(),
            html_body: String::new().into(),
        }
    }
}

crate::define_get_method!(
    VacationResponseGet,
    VacationResponse,
    "VacationResponse/get",
    crate::core::capability::VacationResponseCap
);
crate::define_set_method!(
    VacationResponseSet,
    VacationResponse,
    "VacationResponse/set",
    crate::core::capability::VacationResponseCap
);
