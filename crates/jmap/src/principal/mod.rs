pub mod availability;
pub mod get;
pub mod helpers;
pub mod query;
pub mod set;

use crate::core::set::{skip_if_empty_list, skip_if_empty_map, skip_if_empty_str};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;

use crate::Get;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal<State = Get> {
    #[serde(skip)]
    _create_id: Option<usize>,

    #[serde(skip)]
    _state: std::marker::PhantomData<State>,

    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,

    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    ptype: Option<Type>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    name: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    description: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    email: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    timezone: Option<String>,

    /// RFC 9670: Map of JMAP capability URI to domain-specific metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    capabilities: Option<HashMap<String, serde_json::Value>>,

    /// RFC 9670: Map of account ID to account info for each JMAP account
    /// accessible to this principal, or null if none.
    #[serde(skip_serializing_if = "Option::is_none")]
    accounts: Option<HashMap<String, PrincipalAccount>>,

    #[serde(skip_serializing_if = "skip_if_empty_list")]
    aliases: Option<Vec<String>>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    secret: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    dkim: Option<DKIM>,

    #[serde(skip_serializing_if = "Option::is_none")]
    quota: Option<u32>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    picture: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_list")]
    members: Option<Vec<String>>,

    #[serde(skip_serializing_if = "skip_if_empty_map")]
    acl: Option<HashMap<String, Vec<ACL>>>,

    #[serde(flatten)]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    property_patch: Option<HashMap<String, bool>>,
}

/// Account info within a Principal's `accounts` map (RFC 9670).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrincipalAccount {
    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,

    #[serde(rename = "isPersonal")]
    #[serde(default)]
    is_personal: bool,

    #[serde(rename = "isReadOnly")]
    #[serde(default)]
    is_read_only: bool,

    #[serde(rename = "accountCapabilities")]
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    account_capabilities: HashMap<String, serde_json::Value>,
}

impl PrincipalAccount {
    pub fn new(name: impl Into<String>, is_personal: bool, is_read_only: bool) -> Self {
        PrincipalAccount {
            name: Some(name.into()),
            is_personal,
            is_read_only,
            account_capabilities: HashMap::new(),
        }
    }

    pub fn account_capability(mut self, uri: impl Into<String>, config: serde_json::Value) -> Self {
        self.account_capabilities.insert(uri.into(), config);
        self
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn is_personal(&self) -> bool {
        self.is_personal
    }

    pub fn is_read_only(&self) -> bool {
        self.is_read_only
    }

    pub fn account_capabilities(&self) -> &HashMap<String, serde_json::Value> {
        &self.account_capabilities
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub enum Property {
    #[serde(rename = "id")]
    Id = 0,
    #[serde(rename = "type")]
    Type = 1,
    #[serde(rename = "name")]
    Name = 2,
    #[serde(rename = "description")]
    Description = 3,
    #[serde(rename = "email")]
    Email = 4,
    #[serde(rename = "timezone")]
    Timezone = 5,
    #[serde(rename = "capabilities")]
    Capabilities = 6,
    #[serde(rename = "aliases")]
    Aliases = 7,
    #[serde(rename = "secret")]
    Secret = 8,
    #[serde(rename = "dkim")]
    DKIM = 9,
    #[serde(rename = "quota")]
    Quota = 10,
    #[serde(rename = "picture")]
    Picture = 11,
    #[serde(rename = "members")]
    Members = 12,
    #[serde(rename = "accounts")]
    Accounts = 13,
    #[serde(rename = "shareWith")]
    ShareWith = 14,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub enum ACL {
    #[serde(rename = "mayRename")]
    Rename = 1,
    #[serde(rename = "mayDelete")]
    Delete = 2,
    #[serde(rename = "mayReadItems")]
    ReadItems = 3,
    #[serde(rename = "mayAddItems")]
    AddItems = 4,
    #[serde(rename = "maySetKeywords")]
    SetKeywords = 5,
    #[serde(rename = "mayRemoveItems")]
    RemoveItems = 6,
    #[serde(rename = "mayCreateChild")]
    CreateChild = 7,
    #[serde(rename = "mayShare")]
    Administer = 8,
    #[serde(rename = "maySubmit")]
    Submit = 10,
    #[serde(rename = "maySetSeen")]
    SetSeen = 11,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Type {
    #[serde(rename = "individual")]
    Individual,
    #[serde(rename = "group")]
    Group,
    #[serde(rename = "resource")]
    Resource,
    #[serde(rename = "location")]
    Location,
    #[serde(rename = "domain")]
    Domain,
    #[serde(rename = "list")]
    List,
    #[serde(rename = "other")]
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DKIM {
    #[serde(rename = "dkimSelector")]
    dkim_selector: Option<String>,
    #[serde(rename = "dkimExpiration")]
    dkim_expiration: Option<i64>,
}

impl DKIM {
    pub fn new(dkim_selector: Option<impl Into<String>>, dkim_expiration: Option<i64>) -> DKIM {
        DKIM {
            dkim_selector: dkim_selector.map(Into::into),
            dkim_expiration,
        }
    }

    pub fn selector(&self) -> Option<&str> {
        self.dkim_selector.as_deref()
    }

    pub fn expiration(&self) -> Option<i64> {
        self.dkim_expiration
    }
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::Type => write!(f, "type"),
            Property::Name => write!(f, "name"),
            Property::Description => write!(f, "description"),
            Property::Email => write!(f, "email"),
            Property::Timezone => write!(f, "timezone"),
            Property::Capabilities => write!(f, "capabilities"),
            Property::Aliases => write!(f, "aliases"),
            Property::Secret => write!(f, "secret"),
            Property::DKIM => write!(f, "dkim"),
            Property::Quota => write!(f, "quota"),
            Property::Picture => write!(f, "picture"),
            Property::Members => write!(f, "members"),
            Property::Accounts => write!(f, "accounts"),
            Property::ShareWith => write!(f, "shareWith"),
        }
    }
}

impl Display for ACL {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ACL::Rename => write!(f, "rename"),
            ACL::Delete => write!(f, "delete"),
            ACL::ReadItems => write!(f, "readItems"),
            ACL::AddItems => write!(f, "addItems"),
            ACL::SetKeywords => write!(f, "setKeywords"),
            ACL::RemoveItems => write!(f, "removeItems"),
            ACL::CreateChild => write!(f, "createChild"),
            ACL::Administer => write!(f, "administer"),
            ACL::Submit => write!(f, "submit"),
            ACL::SetSeen => write!(f, "setSeen"),
        }
    }
}

crate::impl_jmap_object!(Principal<State>, Property, true);

use crate::Set;

// Method structs for the new architecture
crate::define_get_method!(
    PrincipalGet,
    Principal<Set>,
    "Principal/get",
    crate::core::capability::Principals,
    crate::core::get::GetResponse<Principal<Get>>
);
crate::define_set_method!(
    PrincipalSet,
    Principal<Set>,
    "Principal/set",
    crate::core::capability::Principals,
    crate::core::set::SetResponse<Principal<Get>>
);
crate::define_changes_method!(
    PrincipalChanges,
    "Principal/changes",
    crate::core::capability::Principals,
    crate::core::changes::ChangesResponse<Principal<Get>>
);
crate::define_query_method!(
    PrincipalQuery,
    Principal<Set>,
    "Principal/query",
    crate::core::capability::Principals
);
crate::define_query_changes_method!(
    PrincipalQueryChanges,
    Principal<Set>,
    "Principal/queryChanges",
    crate::core::capability::Principals
);
