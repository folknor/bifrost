use crate::core::get::GetObject;
use crate::email::EmailId;

use super::Thread;

impl Thread {
    pub(crate) fn email_ids(&self) -> &[EmailId] {
        &self.email_ids
    }
}

impl GetObject for Thread {
    type GetArguments = ();
}
