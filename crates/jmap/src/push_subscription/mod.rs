pub mod get;
pub mod set;

use std::fmt::Display;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::core::set::skip_if_empty_list;
use crate::{DataType, Get, Set};

mod marker {
    pub enum PushSubscription {}
}
/// Strongly-typed PushSubscription ID.
pub type PushSubscriptionId = crate::core::id::Id<marker::PushSubscription>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushSubscription<State = Get> {
    #[serde(skip)]
    _create_id: Option<usize>,

    #[serde(skip)]
    _state: std::marker::PhantomData<State>,

    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,

    #[serde(rename = "deviceClientId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    device_client_id: Option<String>,

    #[serde(rename = "url")]
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,

    #[serde(rename = "keys")]
    #[serde(skip_serializing_if = "Option::is_none")]
    keys: Option<Keys>,

    #[serde(rename = "verificationCode")]
    #[serde(skip_serializing_if = "Option::is_none")]
    verification_code: Option<String>,

    #[serde(rename = "expires")]
    #[serde(skip_serializing_if = "Option::is_none")]
    expires: Option<DateTime<Utc>>,

    #[serde(rename = "types")]
    #[serde(skip_serializing_if = "skip_if_empty_list")]
    types: Option<Vec<DataType>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub enum Property {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "deviceClientId")]
    DeviceClientId,
    #[serde(rename = "url")]
    Url,
    #[serde(rename = "keys")]
    Keys,
    #[serde(rename = "verificationCode")]
    VerificationCode,
    #[serde(rename = "expires")]
    Expires,
    #[serde(rename = "types")]
    Types,
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::DeviceClientId => write!(f, "deviceClientId"),
            Property::Url => write!(f, "url"),
            Property::Keys => write!(f, "keys"),
            Property::VerificationCode => write!(f, "verificationCode"),
            Property::Expires => write!(f, "expires"),
            Property::Types => write!(f, "types"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Keys {
    p256dh: String,
    auth: String,
}

crate::impl_jmap_object!(PushSubscription<State>, Property, false);

// Method structs for the new architecture
crate::define_get_method!(
    PushSubscriptionGet,
    PushSubscription<Set>,
    "PushSubscription/get",
    crate::core::capability::Core,
    crate::core::get::GetResponse<PushSubscription<Get>>
);
crate::define_set_method!(
    PushSubscriptionSet,
    PushSubscription<Set>,
    "PushSubscription/set",
    crate::core::capability::Core,
    crate::core::set::SetResponse<PushSubscription<Get>>
);
