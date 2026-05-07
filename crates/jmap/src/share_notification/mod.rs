//! JMAP ShareNotification (RFC 9670).
//!
//! Records when permissions change on shared objects. ShareNotifications are
//! read-only - only destroy is permitted via `ShareNotification/set`.

pub mod get;
pub mod query;
pub mod set;

use std::collections::HashMap;
use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::Get;

mod marker {
    pub enum ShareNotification {}
}
/// Strongly-typed ShareNotification ID.
pub type ShareNotificationId = crate::core::id::Id<marker::ShareNotification>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareNotification<State = Get> {
    #[serde(skip)]
    _create_id: Option<usize>,

    #[serde(skip)]
    _state: std::marker::PhantomData<State>,

    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    created: Option<String>,

    #[serde(rename = "changedBy")]
    #[serde(skip_serializing_if = "Option::is_none")]
    changed_by: Option<ChangedBy>,

    #[serde(rename = "objectType")]
    #[serde(skip_serializing_if = "Option::is_none")]
    object_type: Option<String>,

    #[serde(rename = "objectAccountId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    object_account_id: Option<String>,

    #[serde(rename = "objectId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    object_id: Option<String>,

    #[serde(rename = "oldRights")]
    #[serde(skip_serializing_if = "Option::is_none")]
    old_rights: Option<HashMap<String, bool>>,

    #[serde(rename = "newRights")]
    #[serde(skip_serializing_if = "Option::is_none")]
    new_rights: Option<HashMap<String, bool>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

/// The principal who changed the sharing permissions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangedBy {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    email: Option<String>,

    #[serde(rename = "principalId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    principal_id: Option<String>,
}

impl ChangedBy {
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn email(&self) -> Option<&str> {
        self.email.as_deref()
    }

    pub fn principal_id(&self) -> Option<&str> {
        self.principal_id.as_deref()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub enum Property {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "created")]
    Created,
    #[serde(rename = "changedBy")]
    ChangedBy,
    #[serde(rename = "objectType")]
    ObjectType,
    #[serde(rename = "objectAccountId")]
    ObjectAccountId,
    #[serde(rename = "objectId")]
    ObjectId,
    #[serde(rename = "oldRights")]
    OldRights,
    #[serde(rename = "newRights")]
    NewRights,
    #[serde(rename = "name")]
    Name,
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::Created => write!(f, "created"),
            Property::ChangedBy => write!(f, "changedBy"),
            Property::ObjectType => write!(f, "objectType"),
            Property::ObjectAccountId => write!(f, "objectAccountId"),
            Property::ObjectId => write!(f, "objectId"),
            Property::OldRights => write!(f, "oldRights"),
            Property::NewRights => write!(f, "newRights"),
            Property::Name => write!(f, "name"),
        }
    }
}

crate::impl_jmap_object!(ShareNotification<State>, Property, true);

use crate::Set;

crate::define_get_method!(
    ShareNotificationGet,
    ShareNotification<Set>,
    "ShareNotification/get",
    crate::core::capability::Principals,
    crate::core::get::GetResponse<ShareNotification<Get>>
);
crate::define_set_method!(
    ShareNotificationSet,
    ShareNotification<Set>,
    "ShareNotification/set",
    crate::core::capability::Principals,
    crate::core::set::SetResponse<ShareNotification<Get>>
);
crate::define_changes_method!(
    ShareNotificationChanges,
    "ShareNotification/changes",
    crate::core::capability::Principals,
    crate::core::changes::ChangesResponse<ShareNotification<Get>>
);
crate::define_query_method!(
    ShareNotificationQuery,
    ShareNotification<Set>,
    "ShareNotification/query",
    crate::core::capability::Principals
);
crate::define_query_changes_method!(
    ShareNotificationQueryChanges,
    ShareNotification<Set>,
    "ShareNotification/queryChanges",
    crate::core::capability::Principals
);
