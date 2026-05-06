use std::collections::HashMap;

use crate::Get;

use super::ParticipantIdentity;

impl ParticipantIdentity<Get> {
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    pub fn take_id(&mut self) -> String {
        self.id.take().unwrap_or_default()
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn send_to(&self) -> Option<&HashMap<String, String>> {
        self.send_to.as_ref()
    }

    pub fn is_default(&self) -> Option<bool> {
        self.is_default
    }
}

crate::impl_get_object!(ParticipantIdentity, ());
