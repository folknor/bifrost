pub mod get;
pub mod helpers;
pub mod query;
pub mod set;
pub mod validate;

use std::fmt::Display;

use crate::Get;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SieveScript<State = Get> {
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

    #[serde(rename = "blobId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_id: Option<String>,

    #[serde(rename = "isActive")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_active: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct SetArguments {
    #[serde(rename = "onSuccessActivateScript")]
    #[serde(skip_serializing_if = "Option::is_none")]
    on_success_activate_script: Option<String>,
    #[serde(rename = "onSuccessDeactivateScript")]
    #[serde(skip_serializing_if = "Option::is_none")]
    on_success_deactivate_script: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub enum Property {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "blobId")]
    BlobId,
    #[serde(rename = "isActive")]
    IsActive,
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::Name => write!(f, "name"),
            Property::BlobId => write!(f, "blobId"),
            Property::IsActive => write!(f, "isActive"),
        }
    }
}

crate::impl_jmap_object!(SieveScript<State>, Property, true);

use crate::Set;

// Method structs for the new architecture
crate::define_get_method!(
    SieveScriptGet,
    SieveScript<Set>,
    "SieveScript/get",
    crate::core::capability::Sieve,
    crate::core::get::GetResponse<SieveScript<Get>>
);
crate::define_set_method!(
    SieveScriptSet,
    SieveScript<Set>,
    "SieveScript/set",
    crate::core::capability::Sieve,
    crate::core::set::SetResponse<SieveScript<Get>>
);
crate::define_query_method!(
    SieveScriptQuery,
    SieveScript<Set>,
    "SieveScript/query",
    crate::core::capability::Sieve
);
