use super::{SieveScript, SieveScriptId};
use crate::core::id::BlobId;

impl SieveScript {
    pub fn id(&self) -> Option<&SieveScriptId> {
        self.id.as_ref()
    }

    pub fn take_id(&mut self) -> SieveScriptId {
        self.id.take().unwrap_or_else(|| SieveScriptId::new(""))
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn blob_id(&self) -> Option<&BlobId> {
        self.blob_id.as_ref()
    }

    pub fn is_active(&self) -> bool {
        self.is_active.unwrap_or(false)
    }
}
