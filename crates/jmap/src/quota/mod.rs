pub mod get;
pub mod helpers;
pub mod query;

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::Get;
use crate::core::field::Field;

/// A quota object representing a storage or count limit (RFC 9425).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quota<State = Get> {
    #[serde(skip)]
    _create_id: Option<usize>,

    #[serde(skip)]
    _state: std::marker::PhantomData<State>,

    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    #[serde(rename = "resourceType")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_type: Option<String>,

    #[serde(rename = "used")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub used: Option<u64>,

    #[serde(rename = "hardLimit")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hard_limit: Option<u64>,

    #[serde(rename = "scope")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(rename = "types")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub types: Option<Vec<String>>,

    #[serde(rename = "warnLimit")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub warn_limit: Field<u64>,

    #[serde(rename = "softLimit")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub soft_limit: Field<u64>,

    #[serde(rename = "description")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub description: Field<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub enum Property {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "resourceType")]
    ResourceType,
    #[serde(rename = "used")]
    Used,
    #[serde(rename = "hardLimit")]
    HardLimit,
    #[serde(rename = "scope")]
    Scope,
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "types")]
    Types,
    #[serde(rename = "warnLimit")]
    WarnLimit,
    #[serde(rename = "softLimit")]
    SoftLimit,
    #[serde(rename = "description")]
    Description,
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::ResourceType => write!(f, "resourceType"),
            Property::Used => write!(f, "used"),
            Property::HardLimit => write!(f, "hardLimit"),
            Property::Scope => write!(f, "scope"),
            Property::Name => write!(f, "name"),
            Property::Types => write!(f, "types"),
            Property::WarnLimit => write!(f, "warnLimit"),
            Property::SoftLimit => write!(f, "softLimit"),
            Property::Description => write!(f, "description"),
        }
    }
}

crate::impl_jmap_object!(Quota<State>, Property, true);

use crate::Set;

// Method structs for the new architecture
crate::define_get_method!(
    QuotaGet,
    Quota<Set>,
    "Quota/get",
    crate::core::capability::Quota,
    crate::core::get::GetResponse<Quota<Get>>
);
crate::define_changes_method!(
    QuotaChanges,
    "Quota/changes",
    crate::core::capability::Quota,
    crate::core::changes::ChangesResponse<Quota<Get>>
);
crate::define_query_method!(
    QuotaQuery,
    Quota<Set>,
    "Quota/query",
    crate::core::capability::Quota
);
crate::define_query_changes_method!(
    QuotaQueryChanges,
    Quota<Set>,
    "Quota/queryChanges",
    crate::core::capability::Quota
);
