pub mod get;
pub mod helpers;
pub mod set;

use std::fmt::Display;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::Get;
use crate::core::set::skip_if_empty_map;

#[derive(Debug, Clone, Default, Serialize)]
pub struct ParticipantIdentitySetArguments {
    #[serde(rename = "onSuccessSetIsDefault")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_success_set_is_default: Option<String>,
}

impl ParticipantIdentitySetArguments {
    /// Set the given participant identity as default after a successful
    /// create/update. The value is a creation id reference (e.g. `"#c0"`)
    /// or an existing id.
    pub fn on_success_set_is_default(&mut self, id: impl Into<String>) -> &mut Self {
        self.on_success_set_is_default = Some(id.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParticipantIdentity<State = Get> {
    #[serde(skip)]
    _create_id: Option<usize>,

    #[serde(skip)]
    _state: std::marker::PhantomData<State>,

    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(rename = "sendTo")]
    #[serde(skip_serializing_if = "skip_if_empty_map")]
    pub send_to: Option<HashMap<String, String>>,

    #[serde(rename = "isDefault")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_default: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub enum Property {
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

crate::impl_jmap_object!(ParticipantIdentity<State>, Property, true);

use crate::Set;

// Method structs for the new architecture
crate::define_get_method!(
    ParticipantIdentityGet,
    ParticipantIdentity<Set>,
    "ParticipantIdentity/get",
    crate::core::capability::Calendars,
    crate::core::get::GetResponse<ParticipantIdentity<Get>>
);
crate::define_set_method!(
    ParticipantIdentitySet,
    ParticipantIdentity<Set>,
    "ParticipantIdentity/set",
    crate::core::capability::Calendars,
    crate::core::set::SetResponse<ParticipantIdentity<Get>>
);
crate::define_changes_method!(
    ParticipantIdentityChanges,
    "ParticipantIdentity/changes",
    crate::core::capability::Calendars,
    crate::core::changes::ChangesResponse<ParticipantIdentity<Get>>
);

// -- Lifted method arguments (plans/API.md §5) --

impl ParticipantIdentitySet {
    pub fn on_success_set_is_default(&mut self, id: impl Into<String>) -> &mut Self {
        self.arguments().on_success_set_is_default(id);
        self
    }
}
