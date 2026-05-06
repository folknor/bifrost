use crate::Get;

use super::SieveScript;

impl SieveScript<Get> {
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    pub fn take_id(&mut self) -> String {
        self.id.take().unwrap_or_default()
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn blob_id(&self) -> Option<&str> {
        self.blob_id.as_deref()
    }

    pub fn is_active(&self) -> bool {
        self.is_active.unwrap_or(false)
    }
}

crate::impl_get_object!(SieveScript, ());
