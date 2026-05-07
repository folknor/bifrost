use serde::Serialize;

use crate::core::query::{self, QueryObject};

use super::ContactCard;

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
#[non_exhaustive]
pub enum Filter {
    InAddressBook {
        #[serde(rename = "inAddressBook")]
        value: String,
    },
    Uid {
        #[serde(rename = "uid")]
        value: String,
    },
    HasMember {
        #[serde(rename = "hasMember")]
        value: String,
    },
    Kind {
        #[serde(rename = "kind")]
        value: String,
    },
    CreatedBefore {
        #[serde(rename = "createdBefore")]
        value: String,
    },
    CreatedAfter {
        #[serde(rename = "createdAfter")]
        value: String,
    },
    UpdatedBefore {
        #[serde(rename = "updatedBefore")]
        value: String,
    },
    UpdatedAfter {
        #[serde(rename = "updatedAfter")]
        value: String,
    },
    Text {
        #[serde(rename = "text")]
        value: String,
    },
    Name {
        #[serde(rename = "name")]
        value: String,
    },
    NameGiven {
        #[serde(rename = "name/given")]
        value: String,
    },
    NameSurname {
        #[serde(rename = "name/surname")]
        value: String,
    },
    NameSurname2 {
        #[serde(rename = "name/surname2")]
        value: String,
    },
    Nickname {
        #[serde(rename = "nickname")]
        value: String,
    },
    Organization {
        #[serde(rename = "organization")]
        value: String,
    },
    Email {
        #[serde(rename = "email")]
        value: String,
    },
    Phone {
        #[serde(rename = "phone")]
        value: String,
    },
    OnlineService {
        #[serde(rename = "onlineService")]
        value: String,
    },
    Address {
        #[serde(rename = "address")]
        value: String,
    },
    Note {
        #[serde(rename = "note")]
        value: String,
    },
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "property")]
#[non_exhaustive]
pub enum Comparator {
    #[serde(rename = "created")]
    Created,
    #[serde(rename = "updated")]
    Updated,
    #[serde(rename = "name/given")]
    NameGiven,
    #[serde(rename = "name/surname")]
    NameSurname,
    #[serde(rename = "name/surname2")]
    NameSurname2,
}

impl Filter {
    pub fn in_address_book(value: impl Into<String>) -> Self {
        Filter::InAddressBook {
            value: value.into(),
        }
    }

    pub fn uid(value: impl Into<String>) -> Self {
        Filter::Uid {
            value: value.into(),
        }
    }

    pub fn has_member(value: impl Into<String>) -> Self {
        Filter::HasMember {
            value: value.into(),
        }
    }

    pub fn kind(value: impl Into<String>) -> Self {
        Filter::Kind {
            value: value.into(),
        }
    }

    pub fn created_before(value: impl Into<String>) -> Self {
        Filter::CreatedBefore {
            value: value.into(),
        }
    }

    pub fn created_after(value: impl Into<String>) -> Self {
        Filter::CreatedAfter {
            value: value.into(),
        }
    }

    pub fn updated_before(value: impl Into<String>) -> Self {
        Filter::UpdatedBefore {
            value: value.into(),
        }
    }

    pub fn updated_after(value: impl Into<String>) -> Self {
        Filter::UpdatedAfter {
            value: value.into(),
        }
    }

    pub fn text(value: impl Into<String>) -> Self {
        Filter::Text {
            value: value.into(),
        }
    }

    pub fn name(value: impl Into<String>) -> Self {
        Filter::Name {
            value: value.into(),
        }
    }

    pub fn name_given(value: impl Into<String>) -> Self {
        Filter::NameGiven {
            value: value.into(),
        }
    }

    pub fn name_surname(value: impl Into<String>) -> Self {
        Filter::NameSurname {
            value: value.into(),
        }
    }

    pub fn name_surname2(value: impl Into<String>) -> Self {
        Filter::NameSurname2 {
            value: value.into(),
        }
    }

    pub fn nickname(value: impl Into<String>) -> Self {
        Filter::Nickname {
            value: value.into(),
        }
    }

    pub fn organization(value: impl Into<String>) -> Self {
        Filter::Organization {
            value: value.into(),
        }
    }

    pub fn email(value: impl Into<String>) -> Self {
        Filter::Email {
            value: value.into(),
        }
    }

    pub fn phone(value: impl Into<String>) -> Self {
        Filter::Phone {
            value: value.into(),
        }
    }

    pub fn online_service(value: impl Into<String>) -> Self {
        Filter::OnlineService {
            value: value.into(),
        }
    }

    pub fn address(value: impl Into<String>) -> Self {
        Filter::Address {
            value: value.into(),
        }
    }

    pub fn note(value: impl Into<String>) -> Self {
        Filter::Note {
            value: value.into(),
        }
    }
}

impl Comparator {
    pub fn created() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Created)
    }

    pub fn updated() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Updated)
    }

    pub fn name_given() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::NameGiven)
    }

    pub fn name_surname() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::NameSurname)
    }

    pub fn name_surname2() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::NameSurname2)
    }
}

impl QueryObject for ContactCard {
    type QueryArguments = ();
    type Filter = Filter;
    type Sort = Comparator;
}
