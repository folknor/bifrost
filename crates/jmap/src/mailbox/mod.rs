pub mod get;
pub mod query;
pub mod set;

use crate::core::set::{skip_if_empty_map, skip_if_empty_str};
use crate::mailbox::set::role_not_set;
use crate::principal::ACL;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;

mod marker {
    pub enum Mailbox {}
}
/// Strongly-typed Mailbox ID.
pub type MailboxId = crate::core::id::Id<marker::Mailbox>;

#[derive(Debug, Clone, Serialize, Default)]
pub struct SetArguments {
    #[serde(rename = "onDestroyRemoveEmails")]
    #[serde(skip_serializing_if = "Option::is_none")]
    on_destroy_remove_emails: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct QueryArguments {
    #[serde(rename = "sortAsTree")]
    sort_as_tree: bool,
    #[serde(rename = "filterAsTree")]
    filter_as_tree: bool,
}

// -- Lifted method arguments (plans/API.md §5) --

impl MailboxSet {
    #[must_use]
    pub fn on_destroy_remove_emails(mut self, value: bool) -> Self {
        self.arguments().on_destroy_remove_emails(value);
        self
    }
}

impl MailboxQuery {
    #[must_use]
    pub fn sort_as_tree(mut self, value: bool) -> Self {
        self.arguments().sort_as_tree(value);
        self
    }

    #[must_use]
    pub fn filter_as_tree(mut self, value: bool) -> Self {
        self.arguments().filter_as_tree(value);
        self
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct ChangesResponse {
    #[serde(rename = "updatedProperties")]
    updated_properties: Option<Vec<Property>>,
}

/// Server-returned Mailbox object (RFC 8621 §2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mailbox {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<String>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "parentId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) parent_id: Option<String>,

    #[serde(rename = "role")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) role: Option<Role>,

    #[serde(rename = "sortOrder")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sort_order: Option<u32>,

    #[serde(rename = "totalEmails")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) total_emails: Option<usize>,

    #[serde(rename = "unreadEmails")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) unread_emails: Option<usize>,

    #[serde(rename = "totalThreads")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) total_threads: Option<usize>,

    #[serde(rename = "unreadThreads")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) unread_threads: Option<usize>,

    #[serde(rename = "myRights")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) my_rights: Option<MailboxRights>,

    #[serde(rename = "isSubscribed")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_subscribed: Option<bool>,

    #[serde(rename = "shareWith")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) share_with: Option<HashMap<String, HashMap<ACL, bool>>>,
}

/// Client-sent Mailbox/set `create` payload.
#[derive(Debug, Clone, Serialize)]
pub struct MailboxCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<usize>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "parentId")]
    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub(super) parent_id: Option<String>,

    #[serde(rename = "role")]
    #[serde(skip_serializing_if = "role_not_set")]
    pub(super) role: Option<Role>,

    #[serde(rename = "sortOrder")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sort_order: Option<u32>,

    #[serde(rename = "isSubscribed")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_subscribed: Option<bool>,

    #[serde(rename = "shareWith")]
    #[serde(skip_serializing_if = "skip_if_empty_map")]
    pub(super) share_with: Option<HashMap<String, HashMap<ACL, bool>>>,
}

/// Client-sent Mailbox/set `update` patch payload.
#[derive(Debug, Clone, Serialize, Default)]
pub struct MailboxPatch {
    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "parentId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) parent_id: Option<String>,

    #[serde(rename = "role")]
    #[serde(skip_serializing_if = "role_not_set")]
    pub(super) role: Option<Role>,

    #[serde(rename = "sortOrder")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sort_order: Option<u32>,

    #[serde(rename = "isSubscribed")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_subscribed: Option<bool>,

    #[serde(rename = "shareWith")]
    #[serde(skip_serializing_if = "skip_if_empty_map")]
    pub(super) share_with: Option<HashMap<String, HashMap<ACL, bool>>>,

    #[serde(flatten)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) acl_patch: Option<HashMap<String, ACLPatch>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub(crate) enum ACLPatch {
    Replace(HashMap<ACL, bool>),
    Set(bool),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Role {
    Archive,
    Drafts,
    Important,
    Inbox,
    Junk,
    Sent,
    Trash,
    Other(String),
    #[default]
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MailboxRights {
    #[serde(rename = "mayReadItems")]
    #[serde(default)]
    pub(super) may_read_items: bool,

    #[serde(rename = "mayAddItems")]
    #[serde(default)]
    pub(super) may_add_items: bool,

    #[serde(rename = "mayRemoveItems")]
    #[serde(default)]
    pub(super) may_remove_items: bool,

    #[serde(rename = "maySetSeen")]
    #[serde(default)]
    pub(super) may_set_seen: bool,

    #[serde(rename = "maySetKeywords")]
    #[serde(default)]
    pub(super) may_set_keywords: bool,

    #[serde(rename = "mayCreateChild")]
    #[serde(default)]
    pub(super) may_create_child: bool,

    #[serde(rename = "mayRename")]
    #[serde(default)]
    pub(super) may_rename: bool,

    #[serde(rename = "mayDelete")]
    #[serde(default)]
    pub(super) may_delete: bool,

    #[serde(rename = "maySubmit")]
    #[serde(default)]
    pub(super) may_submit: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub enum Property {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "parentId")]
    ParentId,
    #[serde(rename = "role")]
    Role,
    #[serde(rename = "sortOrder")]
    SortOrder,
    #[serde(rename = "totalEmails")]
    TotalEmails,
    #[serde(rename = "unreadEmails")]
    UnreadEmails,
    #[serde(rename = "totalThreads")]
    TotalThreads,
    #[serde(rename = "unreadThreads")]
    UnreadThreads,
    #[serde(rename = "myRights")]
    MyRights,
    #[serde(rename = "isSubscribed")]
    IsSubscribed,
    #[serde(rename = "shareWith")]
    ShareWith,
}

impl Property {
    pub fn is_count(&self) -> bool {
        matches!(
            self,
            Property::TotalEmails
                | Property::UnreadEmails
                | Property::TotalThreads
                | Property::UnreadThreads
        )
    }
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::Name => write!(f, "name"),
            Property::ParentId => write!(f, "parentId"),
            Property::Role => write!(f, "role"),
            Property::SortOrder => write!(f, "sortOrder"),
            Property::TotalEmails => write!(f, "totalEmails"),
            Property::UnreadEmails => write!(f, "unreadEmails"),
            Property::TotalThreads => write!(f, "totalThreads"),
            Property::UnreadThreads => write!(f, "unreadThreads"),
            Property::MyRights => write!(f, "myRights"),
            Property::IsSubscribed => write!(f, "isSubscribed"),
            Property::ShareWith => write!(f, "shareWith"),
        }
    }
}

impl ChangesResponse {
    pub fn updated_properties(&self) -> Option<&[Property]> {
        self.updated_properties.as_deref()
    }
}

// -- Trait impls --

impl crate::core::Object for Mailbox {
    type Property = Property;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for Mailbox {
    type ChangesResponse = ChangesResponse;
}

impl crate::core::get::GetObject for Mailbox {
    type GetArguments = ();
}

impl crate::core::set::SetObject for Mailbox {
    type Create = MailboxCreate;
    type Patch = MailboxPatch;
    type SetArguments = SetArguments;
}

impl crate::core::SetCreate for MailboxCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }

    fn new(create_id: Option<usize>) -> Self {
        MailboxCreate {
            _create_id: create_id,
            name: None,
            parent_id: String::new().into(),
            role: Role::None.into(),
            sort_order: None,
            is_subscribed: None,
            share_with: HashMap::with_capacity(0).into(),
        }
    }
}

// Method struct definitions

crate::define_get_method!(
    MailboxGet,
    Mailbox,
    "Mailbox/get",
    crate::core::capability::Mail
);
crate::define_set_method!(
    MailboxSet,
    Mailbox,
    "Mailbox/set",
    crate::core::capability::Mail
);
crate::define_changes_method!(
    MailboxChanges,
    Mailbox,
    "Mailbox/changes",
    crate::core::capability::Mail
);
crate::define_query_method!(
    MailboxQuery,
    Mailbox,
    "Mailbox/query",
    crate::core::capability::Mail
);
crate::define_query_changes_method!(
    MailboxQueryChanges,
    Mailbox,
    "Mailbox/queryChanges",
    crate::core::capability::Mail
);

impl<'de> Deserialize<'de> for Role {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match <&str>::deserialize(deserializer)?
            .to_ascii_lowercase()
            .as_str()
        {
            "inbox" => Ok(Role::Inbox),
            "sent" => Ok(Role::Sent),
            "trash" => Ok(Role::Trash),
            "drafts" => Ok(Role::Drafts),
            "junk" => Ok(Role::Junk),
            "archive" => Ok(Role::Archive),
            "important" => Ok(Role::Important),
            other => Ok(Role::Other(other.to_string())),
        }
    }
}

impl Serialize for Role {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(match self {
            Role::Inbox => "inbox",
            Role::Sent => "sent",
            Role::Trash => "trash",
            Role::Drafts => "drafts",
            Role::Junk => "junk",
            Role::Archive => "archive",
            Role::Important => "important",
            Role::Other(other) => other,
            Role::None => "",
        })
    }
}
