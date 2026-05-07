use std::collections::HashMap;

use crate::{calendar_event::Alert, core::field::Field};

use super::{Calendar, CalendarRights, IncludeInAvailability};

impl Calendar {
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

    pub fn description_field(&self) -> &Field<String> {
        &self.description
    }

    pub fn color(&self) -> Option<&str> {
        self.color.as_value().map(String::as_str)
    }

    pub fn color_field(&self) -> &Field<String> {
        &self.color
    }

    pub fn sort_order(&self) -> Option<u32> {
        self.sort_order
    }

    pub fn is_subscribed(&self) -> Option<bool> {
        self.is_subscribed
    }

    pub fn is_visible(&self) -> Option<bool> {
        self.is_visible
    }

    pub fn is_default(&self) -> Option<bool> {
        self.is_default
    }

    pub fn include_in_availability(&self) -> Option<&IncludeInAvailability> {
        self.include_in_availability.as_ref()
    }

    pub fn default_alerts_with_time(&self) -> Option<&HashMap<String, Alert>> {
        self.default_alerts_with_time.as_value()
    }

    pub fn default_alerts_with_time_field(&self) -> &Field<HashMap<String, Alert>> {
        &self.default_alerts_with_time
    }

    pub fn default_alerts_without_time(&self) -> Option<&HashMap<String, Alert>> {
        self.default_alerts_without_time.as_value()
    }

    pub fn default_alerts_without_time_field(&self) -> &Field<HashMap<String, Alert>> {
        &self.default_alerts_without_time
    }

    pub fn time_zone(&self) -> Option<&str> {
        self.time_zone.as_value().map(String::as_str)
    }

    pub fn time_zone_field(&self) -> &Field<String> {
        &self.time_zone
    }

    pub fn share_with(&self) -> Option<&HashMap<String, CalendarRights>> {
        self.share_with.as_value()
    }

    pub fn share_with_field(&self) -> &Field<HashMap<String, CalendarRights>> {
        &self.share_with
    }

    pub fn my_rights(&self) -> Option<&CalendarRights> {
        self.my_rights.as_ref()
    }
}
