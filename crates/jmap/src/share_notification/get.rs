use std::collections::HashMap;

use super::{ChangedBy, ShareNotification, ShareNotificationId};
use crate::core::id::AccountId;

impl ShareNotification {
    pub fn id(&self) -> Option<&ShareNotificationId> {
        self.id.as_ref()
    }

    pub fn take_id(&mut self) -> ShareNotificationId {
        self.id
            .take()
            .unwrap_or_else(|| ShareNotificationId::new(""))
    }

    /// UTCDate when this notification was created.
    pub fn created(&self) -> Option<&str> {
        self.created.as_deref()
    }

    /// The principal who changed the sharing permissions.
    pub fn changed_by(&self) -> Option<&ChangedBy> {
        self.changed_by.as_ref()
    }

    /// The JMAP type name of the shared object (e.g., `"Calendar"`, `"Mailbox"`).
    pub fn object_type(&self) -> Option<&str> {
        self.object_type.as_deref()
    }

    /// The account ID where the shared object resides.
    pub fn object_account_id(&self) -> Option<&AccountId> {
        self.object_account_id.as_ref()
    }

    /// The ID of the shared object.
    pub fn object_id(&self) -> Option<&str> {
        self.object_id.as_deref()
    }

    /// Previous permissions, or `None` if newly shared.
    pub fn old_rights(&self) -> Option<&HashMap<String, bool>> {
        self.old_rights.as_ref()
    }

    /// New permissions, or `None` if sharing was revoked.
    pub fn new_rights(&self) -> Option<&HashMap<String, bool>> {
        self.new_rights.as_ref()
    }

    /// The name of the shared object at the time of notification.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}
