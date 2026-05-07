use std::collections::HashMap;

use super::{ParticipantIdentity, ParticipantIdentityId};

impl ParticipantIdentity {
    pub fn id(&self) -> Option<&ParticipantIdentityId> {
        self.id.as_ref()
    }

    pub fn take_id(&mut self) -> ParticipantIdentityId {
        self.id
            .take()
            .unwrap_or_else(|| ParticipantIdentityId::new(""))
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
