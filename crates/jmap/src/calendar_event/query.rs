use serde::Serialize;

use crate::{
    core::query::{self, QueryObject},
    Set,
};

use super::{CalendarEvent, QueryArguments};

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
#[non_exhaustive]
pub enum Filter {
    /// Filter by calendar ID (singular). Used by Stalwart.
    InCalendar {
        #[serde(rename = "inCalendar")]
        value: String,
    },
    /// Filter by calendar IDs (plural, spec draft).
    InCalendars {
        #[serde(rename = "inCalendars")]
        value: Vec<String>,
    },
    Uid {
        #[serde(rename = "uid")]
        value: String,
    },
    After {
        #[serde(rename = "after")]
        value: String,
    },
    Before {
        #[serde(rename = "before")]
        value: String,
    },
    Text {
        #[serde(rename = "text")]
        value: String,
    },
    Title {
        #[serde(rename = "title")]
        value: String,
    },
    Description {
        #[serde(rename = "description")]
        value: String,
    },
    Location {
        #[serde(rename = "location")]
        value: String,
    },
    Owner {
        #[serde(rename = "owner")]
        value: String,
    },
    Attendee {
        #[serde(rename = "attendee")]
        value: String,
    },
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "property")]
#[non_exhaustive]
pub enum Comparator {
    #[serde(rename = "start")]
    Start,
    #[serde(rename = "uid")]
    Uid,
    #[serde(rename = "recurrenceId")]
    RecurrenceId,
    #[serde(rename = "created")]
    Created,
    #[serde(rename = "updated")]
    Updated,
}

impl Filter {
    /// Filter by a single calendar ID. Used by Stalwart.
    pub fn in_calendar(value: impl Into<String>) -> Self {
        Filter::InCalendar {
            value: value.into(),
        }
    }

    /// Filter by multiple calendar IDs (spec draft).
    pub fn in_calendars<U, V>(value: U) -> Self
    where
        U: IntoIterator<Item = V>,
        V: Into<String>,
    {
        Filter::InCalendars {
            value: value.into_iter().map(std::convert::Into::into).collect(),
        }
    }

    pub fn uid(value: impl Into<String>) -> Self {
        Filter::Uid {
            value: value.into(),
        }
    }

    pub fn after(value: impl Into<String>) -> Self {
        Filter::After {
            value: value.into(),
        }
    }

    pub fn before(value: impl Into<String>) -> Self {
        Filter::Before {
            value: value.into(),
        }
    }

    pub fn text(value: impl Into<String>) -> Self {
        Filter::Text {
            value: value.into(),
        }
    }

    pub fn title(value: impl Into<String>) -> Self {
        Filter::Title {
            value: value.into(),
        }
    }

    pub fn description(value: impl Into<String>) -> Self {
        Filter::Description {
            value: value.into(),
        }
    }

    pub fn location(value: impl Into<String>) -> Self {
        Filter::Location {
            value: value.into(),
        }
    }

    pub fn owner(value: impl Into<String>) -> Self {
        Filter::Owner {
            value: value.into(),
        }
    }

    pub fn attendee(value: impl Into<String>) -> Self {
        Filter::Attendee {
            value: value.into(),
        }
    }
}

impl Comparator {
    pub fn start() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Start)
    }

    pub fn uid() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Uid)
    }

    pub fn recurrence_id() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::RecurrenceId)
    }

    pub fn created() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Created)
    }

    pub fn updated() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Updated)
    }
}

impl QueryObject for CalendarEvent<Set> {
    type QueryArguments = QueryArguments;
    type Filter = Filter;
    type Sort = Comparator;
}
