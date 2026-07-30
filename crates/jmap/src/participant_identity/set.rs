use super::{ParticipantIdentityCreate, ParticipantIdentityPatch};

macro_rules! pi_setters {
    ($t:ty) => {
        impl $t {
            pub(crate) fn name(&mut self, name: impl Into<String>) -> &mut Self {
                self.name = Some(name.into());
                self
            }

            pub(crate) fn calendar_address(
                &mut self,
                calendar_address: impl Into<String>,
            ) -> &mut Self {
                self.calendar_address = calendar_address.into();
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

    /// Set `calendarAddress`. Two-state, not three: draft-26 §3 defines
    /// the property as a required, non-nullable String with no default,
    /// so a PatchObject may either carry a new value or omit the
    /// property. A `null` here would be a property *removal* the server
    /// is obliged to reject with `invalidProperties`, so the setter does
    /// not offer one.
    pub(crate) fn calendar_address(&mut self, calendar_address: impl Into<String>) -> &mut Self {
        self.calendar_address = Some(calendar_address.into());
        self
    }
}
