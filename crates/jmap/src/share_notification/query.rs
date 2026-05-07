use serde::Serialize;

use crate::core::query::{self, QueryObject};

use super::ShareNotification;

/// RFC 9670 ShareNotification filter conditions.
#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
#[non_exhaustive]
pub enum Filter {
    /// Notifications created on or after this UTCDate.
    After {
        #[serde(rename = "after")]
        value: String,
    },
    /// Notifications created before this UTCDate.
    Before {
        #[serde(rename = "before")]
        value: String,
    },
    /// Match by JMAP object type name (e.g., `"Calendar"`, `"Mailbox"`).
    ObjectType {
        #[serde(rename = "objectType")]
        value: String,
    },
    /// Match by the account ID where the shared object resides.
    ObjectAccountId {
        #[serde(rename = "objectAccountId")]
        value: String,
    },
}

/// RFC 9670 ShareNotification sort properties.
#[derive(Serialize, Debug, Clone)]
#[serde(tag = "property")]
#[non_exhaustive]
pub enum Comparator {
    #[serde(rename = "created")]
    Created,
}

impl Filter {
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

    pub fn object_type(value: impl Into<String>) -> Self {
        Filter::ObjectType {
            value: value.into(),
        }
    }

    pub fn object_account_id(value: impl Into<String>) -> Self {
        Filter::ObjectAccountId {
            value: value.into(),
        }
    }
}

impl Comparator {
    pub fn created() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Created)
    }
}

impl QueryObject for ShareNotification {
    type QueryArguments = ();
    type Filter = Filter;
    type Sort = Comparator;
}
