use std::collections::HashMap;

use crate::core::field::Field;

use super::{AddressBook, AddressBookId, AddressBookRights};

impl AddressBook {
    pub(crate) fn id(&self) -> Option<&AddressBookId> {
        self.id.as_ref()
    }

    pub(crate) fn take_id(&mut self) -> AddressBookId {
        self.id.take().unwrap_or_else(|| AddressBookId::new(""))
    }

    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub(crate) fn description(&self) -> Option<&str> {
        self.description.as_value().map(String::as_str)
    }

    pub(crate) fn description_field(&self) -> &Field<String> {
        &self.description
    }

    pub(crate) fn sort_order(&self) -> Option<u32> {
        self.sort_order
    }

    pub(crate) fn is_default(&self) -> Option<bool> {
        self.is_default
    }

    pub(crate) fn is_subscribed(&self) -> Option<bool> {
        self.is_subscribed
    }

    pub(crate) fn share_with(&self) -> Option<&HashMap<String, AddressBookRights>> {
        self.share_with.as_value()
    }

    pub(crate) fn share_with_field(&self) -> &Field<HashMap<String, AddressBookRights>> {
        &self.share_with
    }

    pub(crate) fn my_rights(&self) -> Option<&AddressBookRights> {
        self.my_rights.as_ref()
    }
}
