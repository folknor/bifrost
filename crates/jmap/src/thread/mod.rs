pub mod get;
pub mod helpers;

use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Thread {
    id: String,
    #[serde(rename = "emailIds")]
    email_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub enum Property {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "emailIds")]
    EmailIds,
}

crate::impl_jmap_object!(Thread, Property, true);

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::EmailIds => write!(f, "emailIds"),
        }
    }
}

// Method structs for the new architecture
crate::define_get_method!(ThreadGet, Thread, "Thread/get", crate::core::capability::Mail, crate::core::get::GetResponse<Thread>);
crate::define_changes_method!(ThreadChanges, "Thread/changes", crate::core::capability::Mail, crate::core::changes::ChangesResponse<Thread>);

