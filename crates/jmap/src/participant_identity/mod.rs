// Stage 4 (calendar conveniences) will wire these RFC types into the
// Account impl; until then the module is fully built but unused.
#![allow(dead_code)]

pub(crate) mod get;
pub(crate) mod set;

use std::fmt::Display;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::core::field::Field;
use crate::core::set::skip_if_empty_map;

mod marker {
    pub(crate) enum ParticipantIdentity {}
}
/// Strongly-typed ParticipantIdentity ID.
pub(crate) type ParticipantIdentityId = crate::core::id::Id<marker::ParticipantIdentity>;

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct ParticipantIdentitySetArguments {
    #[serde(rename = "onSuccessSetIsDefault")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) on_success_set_is_default: Option<ParticipantIdentityId>,
}

impl ParticipantIdentitySetArguments {
    pub(crate) fn on_success_set_is_default(
        &mut self,
        id: impl Into<ParticipantIdentityId>,
    ) -> &mut Self {
        self.on_success_set_is_default = Some(id.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ParticipantIdentity {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<ParticipantIdentityId>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "sendTo")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) send_to: Option<HashMap<String, String>>,

    #[serde(rename = "isDefault")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_default: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ParticipantIdentityCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<usize>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "sendTo")]
    #[serde(skip_serializing_if = "skip_if_empty_map")]
    pub(super) send_to: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct ParticipantIdentityPatch {
    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "sendTo")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) send_to: Field<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub(crate) enum Property {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "sendTo")]
    SendTo,
    #[serde(rename = "isDefault")]
    IsDefault,
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::Name => write!(f, "name"),
            Property::SendTo => write!(f, "sendTo"),
            Property::IsDefault => write!(f, "isDefault"),
        }
    }
}

impl crate::core::Object for ParticipantIdentity {
    type Property = Property;
    type Id = ParticipantIdentityId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for ParticipantIdentity {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for ParticipantIdentity {
    type GetArguments = ();
}

impl crate::core::set::SetObject for ParticipantIdentity {
    type Create = ParticipantIdentityCreate;
    type Patch = ParticipantIdentityPatch;
    type SetArguments = ParticipantIdentitySetArguments;
}

impl crate::core::SetCreate for ParticipantIdentityCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }

    fn new(create_id: Option<usize>) -> Self {
        ParticipantIdentityCreate {
            _create_id: create_id,
            name: None,
            send_to: Some(HashMap::new()),
        }
    }
}

crate::define_get_method!(
    ParticipantIdentityGet,
    ParticipantIdentity,
    "ParticipantIdentity/get",
    crate::core::capability::Calendars
);
crate::define_set_method!(
    ParticipantIdentitySet,
    ParticipantIdentity,
    "ParticipantIdentity/set",
    crate::core::capability::Calendars
);
crate::define_changes_method!(
    ParticipantIdentityChanges,
    ParticipantIdentity,
    "ParticipantIdentity/changes",
    crate::core::capability::Calendars
);

impl ParticipantIdentitySet {
    #[must_use]
    pub(crate) fn on_success_set_is_default(
        mut self,
        id: impl Into<ParticipantIdentityId>,
    ) -> Self {
        self.arguments().on_success_set_is_default(id);
        self
    }
}
