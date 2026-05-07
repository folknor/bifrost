use std::collections::HashMap;

use super::{ParticipantIdentityCreate, ParticipantIdentityPatch};

macro_rules! pi_setters {
    ($t:ty) => {
        impl $t {
            pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
                self.name = Some(name.into());
                self
            }

            pub fn send_to(&mut self, send_to: HashMap<String, String>) -> &mut Self {
                self.send_to = Some(send_to);
                self
            }
        }
    };
}

pi_setters!(ParticipantIdentityCreate);
pi_setters!(ParticipantIdentityPatch);
