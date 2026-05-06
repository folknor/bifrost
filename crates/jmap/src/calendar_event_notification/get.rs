use crate::Get;

use super::{CalendarEventNotification, ChangedBy, NotificationType};

impl CalendarEventNotification<Get> {
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    pub fn take_id(&mut self) -> String {
        self.id.take().unwrap_or_default()
    }

    pub fn created(&self) -> Option<&str> {
        self.created.as_deref()
    }

    pub fn changed_by(&self) -> Option<&ChangedBy> {
        self.changed_by.as_ref()
    }

    pub fn calendar_event_id(&self) -> Option<&str> {
        self.calendar_event_id.as_deref()
    }

    pub fn is_draft(&self) -> Option<bool> {
        self.is_draft
    }

    pub fn notification_type(&self) -> Option<&NotificationType> {
        self.type_.as_ref()
    }

    pub fn event(&self) -> Option<&serde_json::Value> {
        self.event.as_ref()
    }

    pub fn event_patch(&self) -> Option<&serde_json::Value> {
        self.event_patch.as_ref()
    }
}

crate::impl_get_object!(CalendarEventNotification, ());
