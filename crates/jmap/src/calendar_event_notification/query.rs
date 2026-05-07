use serde::Serialize;

use crate::core::query::{self, QueryObject};

use super::CalendarEventNotification;

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
#[non_exhaustive]
pub enum Filter {
    Type {
        #[serde(rename = "type")]
        value: String,
    },
    CalendarEventId {
        #[serde(rename = "calendarEventId")]
        value: String,
    },
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "property")]
#[non_exhaustive]
pub enum Comparator {
    #[serde(rename = "created")]
    Created,
}

impl Filter {
    pub fn type_(value: impl Into<String>) -> Self {
        Filter::Type {
            value: value.into(),
        }
    }

    pub fn calendar_event_id(value: impl Into<String>) -> Self {
        Filter::CalendarEventId {
            value: value.into(),
        }
    }
}

impl Comparator {
    pub fn created() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Created)
    }
}

impl QueryObject for CalendarEventNotification {
    type QueryArguments = ();
    type Filter = Filter;
    type Sort = Comparator;
}
