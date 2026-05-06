use crate::{core::field::Field, Get};

use super::{CalendarEvent, GetArguments};

impl CalendarEvent<Get> {
    pub fn id(&self) -> Option<&str> {
        self.properties.get("id")?.as_str()
    }

    pub fn take_id(&mut self) -> String {
        self.properties
            .remove("id")
            .and_then(|v| match v {
                serde_json::Value::String(s) => Some(s),
                _ => None,
            })
            .unwrap_or_default()
    }

    pub fn uid(&self) -> Option<&str> {
        self.properties.get("uid")?.as_str()
    }

    pub fn calendar_ids(
        &self,
    ) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("calendarIds")?.as_object()
    }

    pub fn is_draft(&self) -> Option<bool> {
        self.properties.get("isDraft")?.as_bool()
    }

    pub fn title(&self) -> Option<&str> {
        self.properties.get("title")?.as_str()
    }

    pub fn description(&self) -> Option<&str> {
        self.properties.get("description")?.as_str()
    }

    pub fn description_content_type(&self) -> Option<&str> {
        self.properties.get("descriptionContentType")?.as_str()
    }

    pub fn created(&self) -> Option<&str> {
        self.properties.get("created")?.as_str()
    }

    pub fn updated(&self) -> Option<&str> {
        self.properties.get("updated")?.as_str()
    }

    pub fn start(&self) -> Option<&str> {
        self.properties.get("start")?.as_str()
    }

    pub fn duration(&self) -> Option<&str> {
        self.properties.get("duration")?.as_str()
    }

    /// Returns `Omitted` if the property is absent, `Null` if explicitly
    /// null, or `Value(tz)` if set to a timezone string.
    pub fn time_zone(&self) -> Field<&str> {
        match self.properties.get("timeZone") {
            None => Field::Omitted,
            Some(v) if v.is_null() => Field::Null,
            Some(v) => match v.as_str() {
                Some(s) => Field::Value(s),
                None => Field::Null,
            },
        }
    }

    pub fn show_without_time(&self) -> Option<bool> {
        self.properties.get("showWithoutTime")?.as_bool()
    }

    pub fn status(&self) -> Option<&str> {
        self.properties.get("status")?.as_str()
    }

    pub fn free_busy_status(&self) -> Option<&str> {
        self.properties.get("freeBusyStatus")?.as_str()
    }

    pub fn recurrence_id(&self) -> Option<&str> {
        self.properties.get("recurrenceId")?.as_str()
    }

    pub fn recurrence_rules(&self) -> Option<&Vec<serde_json::Value>> {
        self.properties.get("recurrenceRules")?.as_array()
    }

    pub fn recurrence_overrides(
        &self,
    ) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("recurrenceOverrides")?.as_object()
    }

    pub fn excluded_recurrence_rules(&self) -> Option<&Vec<serde_json::Value>> {
        self.properties.get("excludedRecurrenceRules")?.as_array()
    }

    pub fn excluded_dates(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("excludedDates")?.as_object()
    }

    pub fn priority(&self) -> Option<u64> {
        self.properties.get("priority")?.as_u64()
    }

    /// Returns `Omitted` if absent, `Null` if explicitly null,
    /// `Value(color)` if set.
    pub fn color(&self) -> Field<&str> {
        match self.properties.get("color") {
            None => Field::Omitted,
            Some(v) if v.is_null() => Field::Null,
            Some(v) => match v.as_str() {
                Some(s) => Field::Value(s),
                None => Field::Null,
            },
        }
    }

    /// Returns `Omitted` if absent, `Null` if explicitly null,
    /// `Value(locale)` if set.
    pub fn locale(&self) -> Field<&str> {
        match self.properties.get("locale") {
            None => Field::Omitted,
            Some(v) if v.is_null() => Field::Null,
            Some(v) => match v.as_str() {
                Some(s) => Field::Value(s),
                None => Field::Null,
            },
        }
    }

    pub fn keywords(
        &self,
    ) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("keywords")?.as_object()
    }

    pub fn categories(
        &self,
    ) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("categories")?.as_object()
    }

    pub fn prod_id(&self) -> Option<&str> {
        self.properties.get("prodId")?.as_str()
    }

    pub fn reply_to(
        &self,
    ) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("replyTo")?.as_object()
    }

    pub fn participants(
        &self,
    ) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("participants")?.as_object()
    }

    pub fn use_default_alerts(&self) -> Option<bool> {
        self.properties.get("useDefaultAlerts")?.as_bool()
    }

    /// Returns `Omitted` if absent, `Null` if explicitly null (no alerts),
    /// `Value(map)` if set to an alerts object.
    pub fn alerts(
        &self,
    ) -> Field<&serde_json::Map<String, serde_json::Value>> {
        match self.properties.get("alerts") {
            None => Field::Omitted,
            Some(v) if v.is_null() => Field::Null,
            Some(v) => match v.as_object() {
                Some(m) => Field::Value(m),
                None => Field::Null,
            },
        }
    }

    pub fn locations(
        &self,
    ) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("locations")?.as_object()
    }

    pub fn virtual_locations(
        &self,
    ) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("virtualLocations")?.as_object()
    }

    pub fn links(
        &self,
    ) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("links")?.as_object()
    }

    pub fn related_to(
        &self,
    ) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("relatedTo")?.as_object()
    }

    pub fn method(&self) -> Option<&str> {
        self.properties.get("method")?.as_str()
    }

    pub fn sequence(&self) -> Option<u64> {
        self.properties.get("sequence")?.as_u64()
    }

    /// Access any property as a raw JSON value, including extension
    /// properties not covered by the typed accessors.
    pub fn property(&self, name: &str) -> Option<&serde_json::Value> {
        self.properties.get(name)
    }

    /// Access the full underlying JSCalendar properties map.
    pub fn as_properties(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.properties
    }
}

crate::impl_get_object!(CalendarEvent, GetArguments);
