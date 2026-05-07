//! ContactCard wraps a JSContact Card (RFC 9553) object.

pub mod get;
pub mod parse;
pub mod query;
pub mod set;

mod marker {
    pub enum ContactCard {}
}
/// Strongly-typed ContactCard ID.
pub type ContactCardId = crate::core::id::Id<marker::ContactCard>;

crate::json_object_struct!(
    ContactCard,
    ContactCardCreate,
    ContactCardPatch,
    "a JSContact object"
);

crate::define_open_property_enum! {
    #[non_exhaustive]
    pub enum Property {
        Id => "id",
        Uid => "uid",
        AddressBookIds => "addressBookIds",
        Kind => "kind",
        Name => "name",
        Nicknames => "nicknames",
        Emails => "emails",
        Phones => "phones",
        Addresses => "addresses",
        Organizations => "organizations",
        OnlineServices => "onlineServices",
        Notes => "notes",
        Media => "media",
        Created => "created",
        Updated => "updated",
    }
}

impl crate::core::Object for ContactCard {
    type Property = Property;
    type Id = ContactCardId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for ContactCard {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for ContactCard {
    type GetArguments = ();
}

impl crate::core::set::SetObject for ContactCard {
    type Create = ContactCardCreate;
    type Patch = ContactCardPatch;
    type SetArguments = ();
}

crate::define_get_method!(
    ContactCardGet,
    ContactCard,
    "ContactCard/get",
    crate::core::capability::Contacts
);
crate::define_set_method!(
    ContactCardSet,
    ContactCard,
    "ContactCard/set",
    crate::core::capability::Contacts
);
crate::define_changes_method!(
    ContactCardChanges,
    ContactCard,
    "ContactCard/changes",
    crate::core::capability::Contacts
);
crate::define_query_method!(
    ContactCardQuery,
    ContactCard,
    "ContactCard/query",
    crate::core::capability::Contacts
);
crate::define_query_changes_method!(
    ContactCardQueryChanges,
    ContactCard,
    "ContactCard/queryChanges",
    crate::core::capability::Contacts
);
crate::define_copy_method!(
    ContactCardCopy,
    ContactCard,
    "ContactCard/copy",
    crate::core::capability::Contacts
);
