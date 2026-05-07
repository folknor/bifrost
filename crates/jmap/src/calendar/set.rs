use std::collections::HashMap;

use crate::{calendar_event::Alert, core::field::Field};

use super::{CalendarCreate, CalendarPatch, CalendarRights, IncludeInAvailability};

macro_rules! calendar_setters {
    ($t:ty) => {
        impl $t {
            pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
                self.name = Some(name.into());
                self
            }

            pub fn description(&mut self, description: Option<impl Into<String>>) -> &mut Self {
                self.description = match description {
                    Some(d) => Field::Value(d.into()),
                    None => Field::Null,
                };
                self
            }

            pub fn color(&mut self, color: Option<impl Into<String>>) -> &mut Self {
                self.color = match color {
                    Some(c) => Field::Value(c.into()),
                    None => Field::Null,
                };
                self
            }

            pub fn sort_order(&mut self, sort_order: u32) -> &mut Self {
                self.sort_order = Some(sort_order);
                self
            }

            pub fn is_subscribed(&mut self, is_subscribed: bool) -> &mut Self {
                self.is_subscribed = Some(is_subscribed);
                self
            }

            pub fn is_visible(&mut self, is_visible: bool) -> &mut Self {
                self.is_visible = Some(is_visible);
                self
            }

            pub fn include_in_availability(&mut self, include: IncludeInAvailability) -> &mut Self {
                self.include_in_availability = Some(include);
                self
            }

            pub fn default_alerts_with_time(
                &mut self,
                alerts: Option<HashMap<String, Alert>>,
            ) -> &mut Self {
                self.default_alerts_with_time = match alerts {
                    Some(a) => Field::Value(a),
                    None => Field::Null,
                };
                self
            }

            pub fn default_alerts_without_time(
                &mut self,
                alerts: Option<HashMap<String, Alert>>,
            ) -> &mut Self {
                self.default_alerts_without_time = match alerts {
                    Some(a) => Field::Value(a),
                    None => Field::Null,
                };
                self
            }

            pub fn time_zone(&mut self, time_zone: Option<impl Into<String>>) -> &mut Self {
                self.time_zone = match time_zone {
                    Some(tz) => Field::Value(tz.into()),
                    None => Field::Null,
                };
                self
            }

            pub fn share_with(
                &mut self,
                share_with: Option<HashMap<String, CalendarRights>>,
            ) -> &mut Self {
                self.share_with = match share_with {
                    Some(sw) => Field::Value(sw),
                    None => Field::Null,
                };
                self
            }
        }
    };
}

calendar_setters!(CalendarCreate);
calendar_setters!(CalendarPatch);
