pub mod get;
pub mod helpers;
pub mod set;

use std::fmt::Display;

use crate::Get;
use crate::core::set::skip_if_empty_str;
use crate::core::set::skip_if_zero_date;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VacationResponse<State = Get> {
    #[serde(skip)]
    _create_id: Option<usize>,

    #[serde(skip)]
    _state: std::marker::PhantomData<State>,

    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,

    #[serde(rename = "isEnabled")]
    #[serde(skip_serializing_if = "Option::is_none")]
    is_enabled: Option<bool>,

    #[serde(rename = "fromDate")]
    #[serde(skip_serializing_if = "skip_if_zero_date")]
    from_date: Option<DateTime<Utc>>,

    #[serde(rename = "toDate")]
    #[serde(skip_serializing_if = "skip_if_zero_date")]
    to_date: Option<DateTime<Utc>>,

    #[serde(rename = "subject")]
    #[serde(skip_serializing_if = "skip_if_empty_str")]
    subject: Option<String>,

    #[serde(rename = "textBody")]
    #[serde(skip_serializing_if = "skip_if_empty_str")]
    text_body: Option<String>,

    #[serde(rename = "htmlBody")]
    #[serde(skip_serializing_if = "skip_if_empty_str")]
    html_body: Option<String>,
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

crate::impl_jmap_object!(VacationResponse<State>, Property, true);

use crate::Set;

// Method structs for the new architecture
crate::define_get_method!(
    VacationResponseGet,
    VacationResponse<Set>,
    "VacationResponse/get",
    crate::core::capability::VacationResponseCap,
    crate::core::get::GetResponse<VacationResponse<Get>>
);
crate::define_set_method!(
    VacationResponseSet,
    VacationResponse<Set>,
    "VacationResponse/set",
    crate::core::capability::VacationResponseCap,
    crate::core::set::SetResponse<VacationResponse<Get>>
);
