use serde::Serialize;

use crate::cursor::{CursorScope, ObjectType};

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorScope {
    Account,
    Cursor(CursorScope),
    Mailbox { id: String },
    Message { id: String },
    Thread { id: String },
    Calendar { id: String },
    CalendarCollection,
    Contact { id: String },
    ContactCollection,
}

impl Serialize for ErrorScope {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("ErrorScope", 4)?;
        match self {
            Self::Account => {
                state.serialize_field("kind", "account")?;
            }
            Self::Cursor(scope) => {
                state.serialize_field("kind", "cursor")?;
                serialize_cursor_scope(&mut state, scope)?;
            }
            Self::Mailbox { id } => {
                state.serialize_field("kind", "mailbox")?;
                state.serialize_field("id", id)?;
            }
            Self::Message { id } => {
                state.serialize_field("kind", "message")?;
                state.serialize_field("id", id)?;
            }
            Self::Thread { id } => {
                state.serialize_field("kind", "thread")?;
                state.serialize_field("id", id)?;
            }
            Self::Calendar { id } => {
                state.serialize_field("kind", "calendar")?;
                state.serialize_field("id", id)?;
            }
            Self::CalendarCollection => {
                state.serialize_field("kind", "calendar_collection")?;
            }
            Self::Contact { id } => {
                state.serialize_field("kind", "contact")?;
                state.serialize_field("id", id)?;
            }
            Self::ContactCollection => {
                state.serialize_field("kind", "contact_collection")?;
            }
        }
        state.end()
    }
}

fn serialize_cursor_scope<S>(
    state: &mut S,
    scope: &CursorScope,
) -> Result<(), <S as serde::ser::SerializeStruct>::Error>
where
    S: serde::ser::SerializeStruct,
{
    match scope {
        CursorScope::Account => {
            state.serialize_field("scope_kind", "account")?;
        }
        CursorScope::Type(ty) => {
            state.serialize_field("scope_kind", "type")?;
            state.serialize_field("object_type", object_type_name(*ty))?;
        }
        CursorScope::Query(id) => {
            state.serialize_field("scope_kind", "query")?;
            state.serialize_field("query_id", &id.0)?;
        }
        CursorScope::Folder(id) => {
            state.serialize_field("scope_kind", "folder")?;
            state.serialize_field("folder_id", &id.0)?;
        }
        CursorScope::FolderType { folder, ty } => {
            state.serialize_field("scope_kind", "folder_type")?;
            state.serialize_field("folder_id", &folder.0)?;
            state.serialize_field("object_type", object_type_name(*ty))?;
        }
    }
    Ok(())
}

fn object_type_name(ty: ObjectType) -> &'static str {
    match ty {
        ObjectType::Email => "email",
        ObjectType::Mailbox => "mailbox",
        ObjectType::Thread => "thread",
        ObjectType::Event => "event",
        ObjectType::Contact => "contact",
        ObjectType::EmailSubmission => "email_submission",
        ObjectType::CalendarEvent => "calendar_event",
        ObjectType::ContactGroup => "contact_group",
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum AccountOperation {
    Discover,
    EstablishCursor,
    DiscoverCursorScopes,
    DiscoverMemberships,
    ScopeLifecycle,
    SyncInventory,
    SyncChanges,
    Hydrate,
    HydrateThread,
    HydrateMessage,
    OpenBlob,
    OpenBlobRange,
    OpenRawRfc822,
    PushSubscribe,
    PushUnsubscribe,
    PushStream,
    UpdateFlags,
    BulkMove,
    BulkDestroy,
    AddToContainer,
    RemoveFromContainer,
    SetKeyword,
    SetLabelMembership,
    SetCategory,
    SetExtendedProperty,
    SetIsRead,
    Send,
    AttachmentUpload,
    HostAttachment,
    DraftCreate,
    DraftUpdate,
    DraftDiscard,
    DraftSend,
    CancelScheduledSend,
    RescheduleSend,
    Search,
    SearchMessages,
    ContainersList,
    ContainerCreate,
    ContainerRename,
    ContainerMove,
    ContainerDelete,
    IdentitiesList,
    IdentityUpdate,
    VacationGet,
    VacationSet,
    QuotaGet,
    FiltersList,
    FilterCreate,
    FilterUpdate,
    FilterDelete,
    FilterValidate,
    AddressBooksList,
    ContactsList,
    ContactGet,
    ContactCreate,
    ContactUpdate,
    ContactDelete,
    ContactSearch,
    ContactAutocomplete,
    CalendarsList,
    EventsInRange,
    EventGet,
    EventCreate,
    EventUpdate,
    EventDelete,
    EventRsvp,
    EventSearch,
    EventAutocomplete,
    Close,
    Expunge,
}

impl AccountOperation {
    #[must_use]
    pub fn is_idempotent(self) -> bool {
        !matches!(
            self,
            Self::PushSubscribe
                | Self::Send
                | Self::BulkMove
                | Self::AddToContainer
                | Self::RemoveFromContainer
                | Self::AttachmentUpload
                | Self::HostAttachment
                | Self::DraftCreate
                | Self::DraftUpdate
                | Self::DraftSend
                | Self::RescheduleSend
                | Self::ContainerCreate
                | Self::ContainerRename
                | Self::ContainerMove
                | Self::ContainerDelete
                | Self::IdentityUpdate
                | Self::VacationSet
                | Self::FilterCreate
                | Self::FilterUpdate
                | Self::FilterDelete
                | Self::ContactCreate
                | Self::ContactUpdate
                | Self::ContactDelete
                | Self::EventCreate
                | Self::EventUpdate
                | Self::EventDelete
                | Self::EventRsvp
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[non_exhaustive]
pub enum Provider {
    Fastmail,
    Gmail,
    Microsoft,
    Icloud,
    Yahoo,
    Custom,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum Protocol {
    Jmap,
    Imap,
    CardDav,
    Smtp,
    Lmtp,
    Gmail,
    Graph,
    Ews,
    CalDav,
}

#[cfg(test)]
mod tests {
    use super::AccountOperation;

    #[test]
    fn host_attachment_is_not_idempotent() {
        // An interrupted host may have created a partial/duplicate Drive item;
        // a blind retry is unsafe, so the operation must be non-idempotent.
        assert!(!AccountOperation::HostAttachment.is_idempotent());
    }
}
