use super::{SieveScript, SieveScriptId};
use crate::core::id::BlobId;

impl SieveScript {
    pub(crate) fn id(&self) -> Option<&SieveScriptId> {
        self.id.as_ref()
    }

    pub(crate) fn take_id(&mut self) -> SieveScriptId {
        self.id.take().unwrap_or_else(|| SieveScriptId::new(""))
    }

    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub(crate) fn blob_id(&self) -> Option<&BlobId> {
        self.blob_id.as_ref()
    }

    pub(crate) fn is_active(&self) -> bool {
        self.is_active.unwrap_or(false)
    }
}
