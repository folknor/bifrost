use std::collections::HashMap;

use super::{ChangedBy, ShareNotification, ShareNotificationId};
use crate::core::id::AccountId;

impl ShareNotification {
    pub(crate) fn id(&self) -> Option<&ShareNotificationId> {
        self.id.as_ref()
    }

    pub(crate) fn take_id(&mut self) -> ShareNotificationId {
        self.id
            .take()
            .unwrap_or_else(|| ShareNotificationId::new(""))
    }

    /// UTCDate when this notification was created.
    pub(crate) fn created(&self) -> Option<&str> {
        self.created.as_deref()
    }

    /// The principal who changed the sharing permissions.
    pub(crate) fn changed_by(&self) -> Option<&ChangedBy> {
        self.changed_by.as_ref()
    }

    /// The JMAP type name of the shared object (e.g., `"Calendar"`, `"Mailbox"`).
    pub(crate) fn object_type(&self) -> Option<&str> {
        self.object_type.as_deref()
    }

    /// The account ID where the shared object resides.
    pub(crate) fn object_account_id(&self) -> Option<&AccountId> {
        self.object_account_id.as_ref()
    }

    /// The ID of the shared object.
    pub(crate) fn object_id(&self) -> Option<&str> {
        self.object_id.as_deref()
    }

    /// Previous permissions, or `None` if newly shared.
    pub(crate) fn old_rights(&self) -> Option<&HashMap<String, bool>> {
        self.old_rights.as_ref()
    }

    /// New permissions, or `None` if sharing was revoked.
    pub(crate) fn new_rights(&self) -> Option<&HashMap<String, bool>> {
        self.new_rights.as_ref()
    }

    /// The name of the shared object at the time of notification.
    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}
