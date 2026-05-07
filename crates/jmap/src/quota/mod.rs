pub mod get;
pub mod query;

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::core::field::Field;

mod marker {
    pub enum Quota {}
}
/// Strongly-typed Quota ID.
pub type QuotaId = crate::core::id::Id<marker::Quota>;

/// A quota object representing a storage or count limit (RFC 9425).
/// Quota is read-only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quota {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<QuotaId>,

    #[serde(rename = "resourceType")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) resource_type: Option<String>,

    #[serde(rename = "used")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) used: Option<u64>,

    #[serde(rename = "hardLimit")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) hard_limit: Option<u64>,

    #[serde(rename = "scope")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) scope: Option<String>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "types")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) types: Option<Vec<String>>,

    #[serde(rename = "warnLimit")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) warn_limit: Field<u64>,

    #[serde(rename = "softLimit")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) soft_limit: Field<u64>,

    #[serde(rename = "description")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) description: Field<String>,
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

impl crate::core::Object for Quota {
    type Property = Property;
    type Id = QuotaId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for Quota {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for Quota {
    type GetArguments = ();
}

crate::define_get_method!(QuotaGet, Quota, "Quota/get", crate::core::capability::Quota);
crate::define_changes_method!(
    QuotaChanges,
    Quota,
    "Quota/changes",
    crate::core::capability::Quota
);
crate::define_query_method!(
    QuotaQuery,
    Quota,
    "Quota/query",
    crate::core::capability::Quota
);
crate::define_query_changes_method!(
    QuotaQueryChanges,
    Quota,
    "Quota/queryChanges",
    crate::core::capability::Quota
);
