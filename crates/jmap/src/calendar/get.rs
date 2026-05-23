use std::collections::HashMap;

use crate::{calendar_event::Alert, core::field::Field};

use super::{Calendar, CalendarId, CalendarRights, IncludeInAvailability};

impl Calendar {
    pub(crate) fn id(&self) -> Option<&CalendarId> {
        self.id.as_ref()
    }

    pub(crate) fn take_id(&mut self) -> CalendarId {
        self.id.take().unwrap_or_else(|| CalendarId::new(""))
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

    pub(crate) fn color(&self) -> Option<&str> {
        self.color.as_value().map(String::as_str)
    }

    pub(crate) fn color_field(&self) -> &Field<String> {
        &self.color
    }

    pub(crate) fn sort_order(&self) -> Option<u32> {
        self.sort_order
    }

    pub(crate) fn is_subscribed(&self) -> Option<bool> {
        self.is_subscribed
    }

    pub(crate) fn is_visible(&self) -> Option<bool> {
        self.is_visible
    }

    pub(crate) fn is_default(&self) -> Option<bool> {
        self.is_default
    }

    pub(crate) fn include_in_availability(&self) -> Option<&IncludeInAvailability> {
        self.include_in_availability.as_ref()
    }

    pub(crate) fn default_alerts_with_time(&self) -> Option<&HashMap<String, Alert>> {
        self.default_alerts_with_time.as_value()
    }

    pub(crate) fn default_alerts_with_time_field(&self) -> &Field<HashMap<String, Alert>> {
        &self.default_alerts_with_time
    }

    pub(crate) fn default_alerts_without_time(&self) -> Option<&HashMap<String, Alert>> {
        self.default_alerts_without_time.as_value()
    }

    pub(crate) fn default_alerts_without_time_field(&self) -> &Field<HashMap<String, Alert>> {
        &self.default_alerts_without_time
    }

    pub(crate) fn time_zone(&self) -> Option<&str> {
        self.time_zone.as_value().map(String::as_str)
    }

    pub(crate) fn time_zone_field(&self) -> &Field<String> {
        &self.time_zone
    }

    pub(crate) fn share_with(&self) -> Option<&HashMap<String, CalendarRights>> {
        self.share_with.as_value()
    }

    pub(crate) fn share_with_field(&self) -> &Field<HashMap<String, CalendarRights>> {
        &self.share_with
    }

    pub(crate) fn my_rights(&self) -> Option<&CalendarRights> {
        self.my_rights.as_ref()
    }
}
