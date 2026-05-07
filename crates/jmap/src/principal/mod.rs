pub mod availability;
pub mod get;
pub mod query;
pub mod set;

use crate::core::set::{skip_if_empty_list, skip_if_empty_map, skip_if_empty_str};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;

mod marker {
    pub enum Principal {}
}
/// Strongly-typed Principal ID.
pub type PrincipalId = crate::core::id::Id<marker::Principal>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<PrincipalId>,

    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) ptype: Option<Type>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) description: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) email: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) timezone: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) capabilities: Option<HashMap<String, serde_json::Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) accounts: Option<HashMap<crate::core::id::AccountId, PrincipalAccount>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) aliases: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) secret: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) dkim: Option<DKIM>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) quota: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) picture: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) members: Option<Vec<PrincipalId>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) acl: Option<HashMap<PrincipalId, Vec<ACL>>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PrincipalCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<usize>,

    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) ptype: Option<Type>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub(super) name: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub(super) description: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub(super) email: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub(super) timezone: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) capabilities: Option<HashMap<String, serde_json::Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) accounts: Option<HashMap<crate::core::id::AccountId, PrincipalAccount>>,

    #[serde(skip_serializing_if = "skip_if_empty_list")]
    pub(super) aliases: Option<Vec<String>>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub(super) secret: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) dkim: Option<DKIM>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) quota: Option<u32>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub(super) picture: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_list")]
    pub(super) members: Option<Vec<PrincipalId>>,

    #[serde(skip_serializing_if = "skip_if_empty_map")]
    pub(super) acl: Option<HashMap<PrincipalId, Vec<ACL>>>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PrincipalPatch {
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) ptype: Option<Type>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) description: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) email: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) timezone: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) capabilities: Option<HashMap<String, serde_json::Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) accounts: Option<HashMap<crate::core::id::AccountId, PrincipalAccount>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) aliases: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) secret: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) dkim: Option<DKIM>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) quota: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) picture: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) members: Option<Vec<PrincipalId>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) acl: Option<HashMap<PrincipalId, Vec<ACL>>>,

    #[serde(flatten)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) property_patch: Option<HashMap<String, bool>>,
}

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

impl crate::core::Object for Principal {
    type Property = Property;
    type Id = PrincipalId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for Principal {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for Principal {
    type GetArguments = ();
}

impl crate::core::set::SetObject for Principal {
    type Create = PrincipalCreate;
    type Patch = PrincipalPatch;
    type SetArguments = ();
}

impl crate::core::SetCreate for PrincipalCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }

    fn new(create_id: Option<usize>) -> Self {
        PrincipalCreate {
            _create_id: create_id,
            ptype: None,
            name: String::new().into(),
            description: String::new().into(),
            email: String::new().into(),
            timezone: String::new().into(),
            capabilities: None,
            accounts: None,
            aliases: Vec::with_capacity(0).into(),
            secret: String::new().into(),
            dkim: None,
            quota: None,
            picture: String::new().into(),
            members: Vec::with_capacity(0).into(),
            acl: HashMap::with_capacity(0).into(),
        }
    }
}

crate::define_get_method!(
    PrincipalGet,
    Principal,
    "Principal/get",
    crate::core::capability::Principals
);
crate::define_set_method!(
    PrincipalSet,
    Principal,
    "Principal/set",
    crate::core::capability::Principals
);
crate::define_changes_method!(
    PrincipalChanges,
    Principal,
    "Principal/changes",
    crate::core::capability::Principals
);
crate::define_query_method!(
    PrincipalQuery,
    Principal,
    "Principal/query",
    crate::core::capability::Principals
);
crate::define_query_changes_method!(
    PrincipalQueryChanges,
    Principal,
    "Principal/queryChanges",
    crate::core::capability::Principals
);
