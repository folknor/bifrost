use crate::core::set::from_timestamp;

use super::{VacationResponseCreate, VacationResponsePatch};

macro_rules! vacation_setters {
    ($t:ty) => {
        impl $t {
            pub fn is_enabled(&mut self, is_enabled: bool) -> &mut Self {
                self.is_enabled = Some(is_enabled);
                self
            }

            pub fn from_date(&mut self, from_date: Option<i64>) -> &mut Self {
                self.from_date = from_date.map(from_timestamp);
                self
            }

            pub fn to_date(&mut self, to_date: Option<i64>) -> &mut Self {
                self.to_date = to_date.map(from_timestamp);
                self
            }

            pub fn subject(&mut self, subject: Option<impl Into<String>>) -> &mut Self {
                self.subject = subject.map(std::convert::Into::into);
                self
            }

            pub fn text_body(&mut self, text_body: Option<impl Into<String>>) -> &mut Self {
                self.text_body = text_body.map(std::convert::Into::into);
                self
            }

            pub fn html_body(&mut self, html_body: Option<impl Into<String>>) -> &mut Self {
                self.html_body = html_body.map(std::convert::Into::into);
                self
            }
        }
    };
}

vacation_setters!(VacationResponseCreate);
vacation_setters!(VacationResponsePatch);

impl VacationResponsePatch {
    pub(crate) fn null_property(&mut self, property: impl Into<String>) -> &mut Self {
        self.patch
            .get_or_insert_with(std::collections::HashMap::new)
            .insert(property.into(), serde_json::Value::Null);
        self
    }
}
