use serde::Serialize;

use crate::{
    Set,
    core::query::{self, QueryObject},
};

use super::SieveScript;

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
#[non_exhaustive]
pub enum Filter {
    Name {
        #[serde(rename = "name")]
        value: String,
    },
    IsActive {
        #[serde(rename = "isActive")]
        value: bool,
    },
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "property")]
#[non_exhaustive]
pub enum Comparator {
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "isActive")]
    IsActive,
}

impl Filter {
    pub fn name(value: impl Into<String>) -> Self {
        Filter::Name {
            value: value.into(),
        }
    }

    pub fn is_active(value: bool) -> Self {
        Filter::IsActive { value }
    }
}

impl Comparator {
    pub fn name() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Name)
    }

    pub fn is_active() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::IsActive)
    }
}

impl QueryObject for SieveScript<Set> {
    type QueryArguments = ();

    type Filter = Filter;

    type Sort = Comparator;
}
