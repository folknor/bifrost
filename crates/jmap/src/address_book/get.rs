use std::collections::HashMap;

use crate::{
    core::field::Field,
    Get,
};

use super::{AddressBook, AddressBookRights};

impl AddressBook<Get> {
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    pub fn take_id(&mut self) -> String {
        self.id.take().unwrap_or_default()
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_value().map(String::as_str)
    }

    /// Full three-state access to the description field.
    pub fn description_field(&self) -> &Field<String> {
        &self.description
    }

    pub fn sort_order(&self) -> Option<u32> {
        self.sort_order
    }

    pub fn is_default(&self) -> Option<bool> {
        self.is_default
    }

    pub fn is_subscribed(&self) -> Option<bool> {
        self.is_subscribed
    }

    pub fn share_with(&self) -> Option<&HashMap<String, AddressBookRights>> {
        self.share_with.as_value()
    }

    /// Full three-state access to the share_with field.
    pub fn share_with_field(&self) -> &Field<HashMap<String, AddressBookRights>> {
        &self.share_with
    }

    pub fn my_rights(&self) -> Option<&AddressBookRights> {
        self.my_rights.as_ref()
    }
}

crate::impl_get_object!(AddressBook, ());
