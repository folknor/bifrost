use serde::Serialize;

use crate::{
    Set,
    core::query::{self, QueryObject},
};

use super::Quota;

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
#[non_exhaustive]
pub enum Filter {
    Name {
        #[serde(rename = "name")]
        value: String,
    },
    Scope {
        #[serde(rename = "scope")]
        value: String,
    },
    ResourceType {
        #[serde(rename = "resourceType")]
        value: String,
    },
    Type {
        #[serde(rename = "type")]
        value: String,
    },
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "property")]
#[non_exhaustive]
pub enum Comparator {
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "used")]
    Used,
}

impl Filter {
    pub fn name(value: impl Into<String>) -> Self {
        Filter::Name {
            value: value.into(),
        }
    }

    pub fn scope(value: impl Into<String>) -> Self {
        Filter::Scope {
            value: value.into(),
        }
    }

    pub fn resource_type(value: impl Into<String>) -> Self {
        Filter::ResourceType {
            value: value.into(),
        }
    }

    pub fn type_(value: impl Into<String>) -> Self {
        Filter::Type {
            value: value.into(),
        }
    }
}

impl Comparator {
    pub fn name() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Name)
    }

    pub fn used() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Used)
    }
}

impl QueryObject for Quota<Set> {
    type QueryArguments = ();
    type Filter = Filter;
    type Sort = Comparator;
}
