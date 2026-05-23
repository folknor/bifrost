pub(crate) mod get;

use std::fmt::Display;

use serde::{Deserialize, Serialize};

mod marker {
    pub(crate) enum Thread {}
}
/// Strongly-typed Thread ID.
pub(crate) type ThreadId = crate::core::id::Id<marker::Thread>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Thread {
    id: ThreadId,
    #[serde(rename = "emailIds")]
    email_ids: Vec<crate::email::EmailId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub(crate) enum Property {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "emailIds")]
    EmailIds,
}

impl crate::core::Object for Thread {
    type Property = Property;
    type Id = ThreadId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for Thread {
    type ChangesResponse = ();
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::EmailIds => write!(f, "emailIds"),
        }
    }
}

crate::define_get_method!(
    ThreadGet,
    Thread,
    "Thread/get",
    crate::core::capability::Mail
);
crate::define_changes_method!(
    ThreadChanges,
    Thread,
    "Thread/changes",
    crate::core::capability::Mail
);
