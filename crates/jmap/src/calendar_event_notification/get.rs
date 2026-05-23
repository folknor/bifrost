use super::{CalendarEventNotification, CalendarEventNotificationId, ChangedBy, NotificationType};
use crate::calendar_event::CalendarEventId;

impl CalendarEventNotification {
    pub(crate) fn id(&self) -> Option<&CalendarEventNotificationId> {
        self.id.as_ref()
    }

    pub(crate) fn take_id(&mut self) -> CalendarEventNotificationId {
        self.id
            .take()
            .unwrap_or_else(|| CalendarEventNotificationId::new(""))
    }

    pub(crate) fn created(&self) -> Option<&str> {
        self.created.as_deref()
    }

    pub(crate) fn changed_by(&self) -> Option<&ChangedBy> {
        self.changed_by.as_ref()
    }

    pub(crate) fn calendar_event_id(&self) -> Option<&CalendarEventId> {
        self.calendar_event_id.as_ref()
    }

    pub(crate) fn is_draft(&self) -> Option<bool> {
        self.is_draft
    }

    pub(crate) fn notification_type(&self) -> Option<&NotificationType> {
        self.type_.as_ref()
    }

    pub(crate) fn event(&self) -> Option<&serde_json::Value> {
        self.event.as_ref()
    }

    pub(crate) fn event_patch(&self) -> Option<&serde_json::Value> {
        self.event_patch.as_ref()
    }
}
