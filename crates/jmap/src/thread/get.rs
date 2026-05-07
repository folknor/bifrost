use crate::core::get::GetObject;
use crate::email::EmailId;

use super::{Thread, ThreadId};

impl Thread {
    pub fn id(&self) -> &ThreadId {
        &self.id
    }

    pub fn email_ids(&self) -> &[EmailId] {
        &self.email_ids
    }
}

impl GetObject for Thread {
    type GetArguments = ();
}
