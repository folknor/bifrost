pub mod get;
pub mod helpers;
pub mod set;

use std::fmt::Display;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::Get;
use crate::core::field::Field;
use crate::core::set::skip_if_empty_str;

use crate::calendar_event::Alert;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Calendar<State = Get> {
    #[serde(skip)]
    _create_id: Option<usize>,

    #[serde(skip)]
    _state: std::marker::PhantomData<State>,

    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub name: Option<String>,

    #[serde(rename = "description")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub description: Field<String>,

    #[serde(rename = "color")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub color: Field<String>,

    #[serde(rename = "sortOrder")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order: Option<u32>,

    #[serde(rename = "isSubscribed")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_subscribed: Option<bool>,

    #[serde(rename = "isVisible")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_visible: Option<bool>,

    #[serde(rename = "isDefault")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_default: Option<bool>,

    #[serde(rename = "includeInAvailability")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_in_availability: Option<IncludeInAvailability>,

    #[serde(rename = "defaultAlertsWithTime")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub default_alerts_with_time: Field<HashMap<String, Alert>>,

    #[serde(rename = "defaultAlertsWithoutTime")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub default_alerts_without_time: Field<HashMap<String, Alert>>,

    #[serde(rename = "timeZone")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub time_zone: Field<String>,

    #[serde(rename = "shareWith")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub share_with: Field<HashMap<String, CalendarRights>>,

    #[serde(rename = "myRights")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub my_rights: Option<CalendarRights>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum IncludeInAvailability {
    #[serde(rename = "all")]
    All,
    #[serde(rename = "attending")]
    Attending,
    #[serde(rename = "none")]
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalendarRights {
    #[serde(rename = "mayReadFreeBusy")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub may_read_free_busy: Option<bool>,

    #[serde(rename = "mayReadItems")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub may_read_items: Option<bool>,

    #[serde(rename = "mayWriteAll")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub may_write_all: Option<bool>,

    #[serde(rename = "mayWriteOwn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub may_write_own: Option<bool>,

    #[serde(rename = "mayUpdatePrivate")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub may_update_private: Option<bool>,

    #[serde(rename = "mayRSVP")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub may_rsvp: Option<bool>,

    #[serde(rename = "mayShare")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub may_share: Option<bool>,

    #[serde(rename = "mayDelete")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub may_delete: Option<bool>,
}

// ---- Calendar/set arguments ----

#[derive(Debug, Clone, Default, Serialize)]
pub struct CalendarSetArguments {
    #[serde(rename = "onDestroyRemoveEvents")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_destroy_remove_events: Option<bool>,

    #[serde(rename = "onSuccessSetIsDefault")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_success_set_is_default: Option<String>,
}

impl CalendarSetArguments {
    pub fn on_destroy_remove_events(&mut self, remove: bool) -> &mut Self {
        self.on_destroy_remove_events = Some(remove);
        self
    }

    /// Set the given calendar as default after a successful create/update.
    /// The value is a creation id reference (e.g. `"#c0"`) or an existing id.
    pub fn on_success_set_is_default(&mut self, id: impl Into<String>) -> &mut Self {
        self.on_success_set_is_default = Some(id.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub enum Property {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "description")]
    Description,
    #[serde(rename = "color")]
    Color,
    #[serde(rename = "sortOrder")]
    SortOrder,
    #[serde(rename = "isSubscribed")]
    IsSubscribed,
    #[serde(rename = "isVisible")]
    IsVisible,
    #[serde(rename = "isDefault")]
    IsDefault,
    #[serde(rename = "includeInAvailability")]
    IncludeInAvailability,
    #[serde(rename = "defaultAlertsWithTime")]
    DefaultAlertsWithTime,
    #[serde(rename = "defaultAlertsWithoutTime")]
    DefaultAlertsWithoutTime,
    #[serde(rename = "timeZone")]
    TimeZone,
    #[serde(rename = "shareWith")]
    ShareWith,
    #[serde(rename = "myRights")]
    MyRights,
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::Name => write!(f, "name"),
            Property::Description => write!(f, "description"),
            Property::Color => write!(f, "color"),
            Property::SortOrder => write!(f, "sortOrder"),
            Property::IsSubscribed => write!(f, "isSubscribed"),
            Property::IsVisible => write!(f, "isVisible"),
            Property::IsDefault => write!(f, "isDefault"),
            Property::IncludeInAvailability => write!(f, "includeInAvailability"),
            Property::DefaultAlertsWithTime => write!(f, "defaultAlertsWithTime"),
            Property::DefaultAlertsWithoutTime => write!(f, "defaultAlertsWithoutTime"),
            Property::TimeZone => write!(f, "timeZone"),
            Property::ShareWith => write!(f, "shareWith"),
            Property::MyRights => write!(f, "myRights"),
        }
    }
}

crate::impl_jmap_object!(Calendar<State>, Property, true);

use crate::Set;

// Method structs for the new architecture
crate::define_get_method!(
    CalendarGet,
    Calendar<Set>,
    "Calendar/get",
    crate::core::capability::Calendars,
    crate::core::get::GetResponse<Calendar<Get>>
);
crate::define_set_method!(
    CalendarSet,
    Calendar<Set>,
    "Calendar/set",
    crate::core::capability::Calendars,
    crate::core::set::SetResponse<Calendar<Get>>
);
crate::define_changes_method!(
    CalendarChanges,
    "Calendar/changes",
    crate::core::capability::Calendars,
    crate::core::changes::ChangesResponse<Calendar<Get>>
);
