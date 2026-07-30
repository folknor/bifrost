use std::collections::HashMap;

use super::{ParticipantIdentityCreate, ParticipantIdentityPatch};
use crate::core::field::Field;

macro_rules! pi_setters {
    ($t:ty) => {
        impl $t {
            pub(crate) fn name(&mut self, name: impl Into<String>) -> &mut Self {
                self.name = Some(name.into());
                self
            }

            pub(crate) fn send_to(&mut self, send_to: HashMap<String, String>) -> &mut Self {
                self.send_to = Some(send_to);
                self
            }
        }
    };
}

pi_setters!(ParticipantIdentityCreate);

impl ParticipantIdentityPatch {
    pub(crate) fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    pub(crate) fn send_to(&mut self, send_to: Option<HashMap<String, String>>) -> &mut Self {
        self.send_to = match send_to {
            Some(send_to) => Field::Value(send_to),
            None => Field::Null,
        };
        self
    }
}
