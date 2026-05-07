pub mod get;
pub mod helpers;
pub mod set;

use std::fmt::Display;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::Get;
use crate::core::field::Field;
use crate::core::set::skip_if_empty_str;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressBook<State = Get> {
    #[serde(skip)]
    _create_id: Option<usize>,

    #[serde(skip)]
    _state: std::marker::PhantomData<State>,

    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub name: Option<String>,

    #[serde(rename = "description")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub description: Field<String>,

    #[serde(rename = "sortOrder")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order: Option<u32>,

    #[serde(rename = "isDefault")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_default: Option<bool>,

    #[serde(rename = "isSubscribed")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_subscribed: Option<bool>,

    #[serde(rename = "shareWith")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub share_with: Field<HashMap<String, AddressBookRights>>,

    #[serde(rename = "myRights")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub my_rights: Option<AddressBookRights>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressBookRights {
    #[serde(rename = "mayRead")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub may_read: Option<bool>,

    #[serde(rename = "mayWrite")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub may_write: Option<bool>,

    #[serde(rename = "mayShare")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub may_share: Option<bool>,

    #[serde(rename = "mayDelete")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub may_delete: Option<bool>,
}

// ---- AddressBook/set arguments ----

#[derive(Debug, Clone, Default, Serialize)]
pub struct AddressBookSetArguments {
    #[serde(rename = "onDestroyRemoveContents")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_destroy_remove_contents: Option<bool>,

    #[serde(rename = "onSuccessSetIsDefault")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_success_set_is_default: Option<String>,
}

impl AddressBookSetArguments {
    pub fn on_destroy_remove_contents(&mut self, remove: bool) -> &mut Self {
        self.on_destroy_remove_contents = Some(remove);
        self
    }

    /// Set the given address book as default after a successful create/update.
    /// The value is a creation id reference (e.g. `"#c0"`) or an existing id.
    pub fn on_success_set_is_default(&mut self, id: impl Into<String>) -> &mut Self {
        self.on_success_set_is_default = Some(id.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub enum Property {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "description")]
    Description,
    #[serde(rename = "sortOrder")]
    SortOrder,
    #[serde(rename = "isDefault")]
    IsDefault,
    #[serde(rename = "isSubscribed")]
    IsSubscribed,
    #[serde(rename = "shareWith")]
    ShareWith,
    #[serde(rename = "myRights")]
    MyRights,
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::Name => write!(f, "name"),
            Property::Description => write!(f, "description"),
            Property::SortOrder => write!(f, "sortOrder"),
            Property::IsDefault => write!(f, "isDefault"),
            Property::IsSubscribed => write!(f, "isSubscribed"),
            Property::ShareWith => write!(f, "shareWith"),
            Property::MyRights => write!(f, "myRights"),
        }
    }
}

crate::impl_jmap_object!(AddressBook<State>, Property, true);

use crate::Set;

// Method structs for the new architecture
crate::define_get_method!(
    AddressBookGet,
    AddressBook<Set>,
    "AddressBook/get",
    crate::core::capability::Contacts,
    crate::core::get::GetResponse<AddressBook<Get>>
);
crate::define_set_method!(
    AddressBookSet,
    AddressBook<Set>,
    "AddressBook/set",
    crate::core::capability::Contacts,
    crate::core::set::SetResponse<AddressBook<Get>>
);
crate::define_changes_method!(
    AddressBookChanges,
    "AddressBook/changes",
    crate::core::capability::Contacts,
    crate::core::changes::ChangesResponse<AddressBook<Get>>
);

// -- Lifted method arguments (plans/API.md §5) --

impl AddressBookSet {
    pub fn on_destroy_remove_contents(&mut self, remove: bool) -> &mut Self {
        self.arguments().on_destroy_remove_contents(remove);
        self
    }

    pub fn on_success_set_is_default(&mut self, id: impl Into<String>) -> &mut Self {
        self.arguments().on_success_set_is_default(id);
        self
    }
}
