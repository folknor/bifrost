use serde_json::json;

use crate::Set;

use super::CalendarEvent;

impl CalendarEvent<Set> {
    pub fn uid(&mut self, uid: impl Into<String>) -> &mut Self {
        self.properties
            .insert("uid".into(), serde_json::Value::String(uid.into()));
        self
    }

    pub fn calendar_ids<U, V>(&mut self, calendar_ids: U) -> &mut Self
    where
        U: IntoIterator<Item = V>,
        V: Into<String>,
    {
        let map: serde_json::Map<String, serde_json::Value> = calendar_ids
            .into_iter()
            .map(|id| (id.into(), json!(true)))
            .collect();
        self.properties
            .insert("calendarIds".into(), serde_json::Value::Object(map));
        self
    }

    pub fn calendar_id(&mut self, calendar_id: impl Into<String>, set: bool) -> &mut Self {
        let entry = self
            .properties
            .entry("calendarIds")
            .or_insert_with(|| json!({}));
        if let Some(map) = entry.as_object_mut() {
            map.insert(
                calendar_id.into(),
                if set {
                    serde_json::Value::Bool(true)
                } else {
                    serde_json::Value::Null
                },
            );
        }
        self
    }

    pub fn title(&mut self, title: impl Into<String>) -> &mut Self {
        self.properties
            .insert("title".into(), serde_json::Value::String(title.into()));
        self
    }

    pub fn description(&mut self, description: impl Into<String>) -> &mut Self {
        self.properties.insert(
            "description".into(),
            serde_json::Value::String(description.into()),
        );
        self
    }

    pub fn description_content_type(&mut self, content_type: impl Into<String>) -> &mut Self {
        self.properties.insert(
            "descriptionContentType".into(),
            serde_json::Value::String(content_type.into()),
        );
        self
    }

    pub fn start(&mut self, start: impl Into<String>) -> &mut Self {
        self.properties
            .insert("start".into(), serde_json::Value::String(start.into()));
        self
    }

    pub fn duration(&mut self, duration: impl Into<String>) -> &mut Self {
        self.properties.insert(
            "duration".into(),
            serde_json::Value::String(duration.into()),
        );
        self
    }

    pub fn time_zone(&mut self, time_zone: Option<impl Into<String>>) -> &mut Self {
        self.properties.insert(
            "timeZone".into(),
            match time_zone {
                Some(tz) => serde_json::Value::String(tz.into()),
                None => serde_json::Value::Null,
            },
        );
        self
    }

    pub fn show_without_time(&mut self, show: bool) -> &mut Self {
        self.properties
            .insert("showWithoutTime".into(), json!(show));
        self
    }

    pub fn status(&mut self, status: impl Into<String>) -> &mut Self {
        self.properties
            .insert("status".into(), serde_json::Value::String(status.into()));
        self
    }

    pub fn free_busy_status(&mut self, status: impl Into<String>) -> &mut Self {
        self.properties.insert(
            "freeBusyStatus".into(),
            serde_json::Value::String(status.into()),
        );
        self
    }

    pub fn recurrence_rules(&mut self, rules: Vec<serde_json::Value>) -> &mut Self {
        self.properties
            .insert("recurrenceRules".into(), serde_json::Value::Array(rules));
        self
    }

    pub fn recurrence_overrides(
        &mut self,
        overrides: serde_json::Map<String, serde_json::Value>,
    ) -> &mut Self {
        self.properties.insert(
            "recurrenceOverrides".into(),
            serde_json::Value::Object(overrides),
        );
        self
    }

    pub fn excluded_recurrence_rules(&mut self, rules: Vec<serde_json::Value>) -> &mut Self {
        self.properties.insert(
            "excludedRecurrenceRules".into(),
            serde_json::Value::Array(rules),
        );
        self
    }

    pub fn priority(&mut self, priority: u8) -> &mut Self {
        self.properties.insert("priority".into(), json!(priority));
        self
    }

    pub fn color(&mut self, color: Option<impl Into<String>>) -> &mut Self {
        self.properties.insert(
            "color".into(),
            match color {
                Some(c) => serde_json::Value::String(c.into()),
                None => serde_json::Value::Null,
            },
        );
        self
    }

    pub fn locale(&mut self, locale: Option<impl Into<String>>) -> &mut Self {
        self.properties.insert(
            "locale".into(),
            match locale {
                Some(l) => serde_json::Value::String(l.into()),
                None => serde_json::Value::Null,
            },
        );
        self
    }

    pub fn keywords(&mut self, keywords: serde_json::Map<String, serde_json::Value>) -> &mut Self {
        self.properties
            .insert("keywords".into(), serde_json::Value::Object(keywords));
        self
    }

    pub fn categories(
        &mut self,
        categories: serde_json::Map<String, serde_json::Value>,
    ) -> &mut Self {
        self.properties
            .insert("categories".into(), serde_json::Value::Object(categories));
        self
    }

    pub fn reply_to(&mut self, reply_to: serde_json::Map<String, serde_json::Value>) -> &mut Self {
        self.properties
            .insert("replyTo".into(), serde_json::Value::Object(reply_to));
        self
    }

    pub fn participants(
        &mut self,
        participants: serde_json::Map<String, serde_json::Value>,
    ) -> &mut Self {
        self.properties.insert(
            "participants".into(),
            serde_json::Value::Object(participants),
        );
        self
    }

    pub fn use_default_alerts(&mut self, use_default: bool) -> &mut Self {
        self.properties
            .insert("useDefaultAlerts".into(), json!(use_default));
        self
    }

    pub fn alerts(
        &mut self,
        alerts: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> &mut Self {
        self.properties.insert(
            "alerts".into(),
            match alerts {
                Some(a) => serde_json::Value::Object(a),
                None => serde_json::Value::Null,
            },
        );
        self
    }

    pub fn locations(
        &mut self,
        locations: serde_json::Map<String, serde_json::Value>,
    ) -> &mut Self {
        self.properties
            .insert("locations".into(), serde_json::Value::Object(locations));
        self
    }

    pub fn virtual_locations(
        &mut self,
        virtual_locations: serde_json::Map<String, serde_json::Value>,
    ) -> &mut Self {
        self.properties.insert(
            "virtualLocations".into(),
            serde_json::Value::Object(virtual_locations),
        );
        self
    }

    pub fn links(&mut self, links: serde_json::Map<String, serde_json::Value>) -> &mut Self {
        self.properties
            .insert("links".into(), serde_json::Value::Object(links));
        self
    }

    /// Set any property by name. Use this for extension properties or
    /// less-common JSCalendar properties not covered by typed methods.
    pub fn set_property(&mut self, name: impl Into<String>, value: serde_json::Value) -> &mut Self {
        self.properties.insert(name.into(), value);
        self
    }
}

// SetObject impls generated by json_object_struct! macro in mod.rs.
