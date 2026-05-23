// The Account impl uses the WebSocket push path instead of the JMAP
// PushSubscription/* RFC 8620 §7.2 surface; the types stay for the
// HTTP-push consumer a future stage may want.
#![allow(dead_code)]

pub(crate) mod get;
pub(crate) mod set;

use std::fmt::Display;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::DataType;
use crate::core::set::skip_if_empty_list;

mod marker {
    pub(crate) enum PushSubscription {}
}
/// Strongly-typed PushSubscription ID.
pub(crate) type PushSubscriptionId = crate::core::id::Id<marker::PushSubscription>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PushSubscription {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<PushSubscriptionId>,

    #[serde(rename = "deviceClientId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) device_client_id: Option<String>,

    #[serde(rename = "url")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) url: Option<String>,

    #[serde(rename = "keys")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) keys: Option<Keys>,

    #[serde(rename = "verificationCode")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) verification_code: Option<String>,

    #[serde(rename = "expires")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) expires: Option<DateTime<Utc>>,

    #[serde(rename = "types")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) types: Option<Vec<DataType>>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PushSubscriptionCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<usize>,

    #[serde(rename = "deviceClientId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) device_client_id: Option<String>,

    #[serde(rename = "url")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) url: Option<String>,

    #[serde(rename = "keys")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) keys: Option<Keys>,

    #[serde(rename = "verificationCode")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) verification_code: Option<String>,

    #[serde(rename = "expires")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) expires: Option<DateTime<Utc>>,

    #[serde(rename = "types")]
    #[serde(skip_serializing_if = "skip_if_empty_list")]
    pub(super) types: Option<Vec<DataType>>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct PushSubscriptionPatch {
    #[serde(rename = "verificationCode")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) verification_code: Option<String>,

    #[serde(rename = "expires")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) expires: Option<DateTime<Utc>>,

    #[serde(rename = "types")]
    #[serde(skip_serializing_if = "skip_if_empty_list")]
    pub(super) types: Option<Vec<DataType>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub(crate) enum Property {
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
pub(crate) struct Keys {
    p256dh: String,
    auth: String,
}

impl crate::core::Object for PushSubscription {
    type Property = Property;
    type Id = PushSubscriptionId;
    fn requires_account_id() -> bool {
        false
    }
}

impl crate::core::changes::ChangesObject for PushSubscription {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for PushSubscription {
    type GetArguments = ();
}

impl crate::core::set::SetObject for PushSubscription {
    type Create = PushSubscriptionCreate;
    type Patch = PushSubscriptionPatch;
    type SetArguments = ();
}

impl crate::core::SetCreate for PushSubscriptionCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }

    fn new(create_id: Option<usize>) -> Self {
        PushSubscriptionCreate {
            _create_id: create_id,
            device_client_id: None,
            url: None,
            keys: None,
            verification_code: None,
            expires: None,
            types: Vec::with_capacity(0).into(),
        }
    }
}

crate::define_get_method!(
    PushSubscriptionGet,
    PushSubscription,
    "PushSubscription/get",
    crate::core::capability::Core
);
crate::define_set_method!(
    PushSubscriptionSet,
    PushSubscription,
    "PushSubscription/set",
    crate::core::capability::Core
);
