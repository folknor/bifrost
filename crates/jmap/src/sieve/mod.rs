pub mod get;
pub mod query;
pub mod set;
pub mod validate;

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::core::id::BlobId;

mod marker {
    pub enum SieveScript {}
}
/// Strongly-typed SieveScript ID.
pub type SieveScriptId = crate::core::id::Id<marker::SieveScript>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SieveScript {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<SieveScriptId>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "blobId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) blob_id: Option<BlobId>,

    #[serde(rename = "isActive")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_active: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SieveScriptCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<usize>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "blobId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) blob_id: Option<BlobId>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct SieveScriptPatch {
    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "blobId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) blob_id: Option<BlobId>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct SetArguments {
    #[serde(rename = "onSuccessActivateScript")]
    #[serde(skip_serializing_if = "Option::is_none")]
    on_success_activate_script: Option<SieveScriptId>,
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

impl crate::core::Object for SieveScript {
    type Property = Property;
    type Id = SieveScriptId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for SieveScript {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for SieveScript {
    type GetArguments = ();
}

impl crate::core::set::SetObject for SieveScript {
    type Create = SieveScriptCreate;
    type Patch = SieveScriptPatch;
    type SetArguments = SetArguments;
}

impl crate::core::SetCreate for SieveScriptCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }

    fn new(create_id: Option<usize>) -> Self {
        SieveScriptCreate {
            _create_id: create_id,
            name: None,
            blob_id: None,
        }
    }
}

crate::define_get_method!(
    SieveScriptGet,
    SieveScript,
    "SieveScript/get",
    crate::core::capability::Sieve
);
crate::define_set_method!(
    SieveScriptSet,
    SieveScript,
    "SieveScript/set",
    crate::core::capability::Sieve
);
crate::define_query_method!(
    SieveScriptQuery,
    SieveScript,
    "SieveScript/query",
    crate::core::capability::Sieve
);
