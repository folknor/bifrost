//! JMAP ShareNotification (RFC 9670). Destroy-only.

pub mod get;
pub mod query;
pub mod set;

use std::collections::HashMap;
use std::fmt::Display;

use serde::{Deserialize, Serialize};

mod marker {
    pub enum ShareNotification {}
}
/// Strongly-typed ShareNotification ID.
pub type ShareNotificationId = crate::core::id::Id<marker::ShareNotification>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareNotification {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) created: Option<String>,

    #[serde(rename = "changedBy")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) changed_by: Option<ChangedBy>,

    #[serde(rename = "objectType")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) object_type: Option<String>,

    #[serde(rename = "objectAccountId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) object_account_id: Option<String>,

    #[serde(rename = "objectId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) object_id: Option<String>,

    #[serde(rename = "oldRights")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) old_rights: Option<HashMap<String, bool>>,

    #[serde(rename = "newRights")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) new_rights: Option<HashMap<String, bool>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,
}

/// Uninhabitable Create-shape: ShareNotifications cannot be created.
/// `SetRequest::create()` will not resolve because this type does not
/// impl `SetCreate`.
#[derive(Debug, Clone, Serialize)]
pub enum ShareNotificationCreate {}

/// Uninhabitable Patch-shape: ShareNotifications cannot be updated.
/// `SetRequest::update()` will not resolve because this type does not
/// impl `Default`.
#[derive(Debug, Clone, Serialize)]
pub enum ShareNotificationPatch {}

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

impl crate::core::Object for ShareNotification {
    type Property = Property;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for ShareNotification {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for ShareNotification {
    type GetArguments = ();
}

impl crate::core::set::SetObject for ShareNotification {
    type Create = ShareNotificationCreate;
    type Patch = ShareNotificationPatch;
    type SetArguments = ();
}

crate::define_get_method!(
    ShareNotificationGet,
    ShareNotification,
    "ShareNotification/get",
    crate::core::capability::Principals
);
crate::define_set_method!(
    ShareNotificationSet,
    ShareNotification,
    "ShareNotification/set",
    crate::core::capability::Principals
);
crate::define_changes_method!(
    ShareNotificationChanges,
    ShareNotification,
    "ShareNotification/changes",
    crate::core::capability::Principals
);
crate::define_query_method!(
    ShareNotificationQuery,
    ShareNotification,
    "ShareNotification/query",
    crate::core::capability::Principals
);
crate::define_query_changes_method!(
    ShareNotificationQueryChanges,
    ShareNotification,
    "ShareNotification/queryChanges",
    crate::core::capability::Principals
);
