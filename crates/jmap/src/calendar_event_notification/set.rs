use crate::{core::set::{SetObject, SetObjectCreatable}, Get, Set};

use super::CalendarEventNotification;

impl SetObject for CalendarEventNotification<Set> {
    type SetArguments = ();

    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }
}

impl SetObjectCreatable for CalendarEventNotification<Set> {
    fn new(_create_id: Option<usize>) -> Self {
        CalendarEventNotification {
            _create_id,
            _state: Default::default(),
            id: None,
            created: None,
            changed_by: None,
            calendar_event_id: None,
            is_draft: None,
            type_: None,
            event: None,
            event_patch: None,
        }
    }
}

impl SetObject for CalendarEventNotification<Get> {
    type SetArguments = ();

    fn create_id(&self) -> Option<String> {
        None
    }
}
