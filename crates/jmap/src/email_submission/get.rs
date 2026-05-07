use super::{
    Address, Delivered, DeliveryStatus, Displayed, EmailSubmission, EmailSubmissionId, UndoStatus,
};
use crate::core::id::BlobId;
use crate::email::EmailId;
use crate::identity::IdentityId;
use crate::thread::ThreadId;
use std::collections::HashMap;

impl EmailSubmission {
    pub fn id(&self) -> Option<&EmailSubmissionId> {
        self.id.as_ref()
    }

    pub fn take_id(&mut self) -> EmailSubmissionId {
        self.id.take().unwrap_or_else(|| EmailSubmissionId::new(""))
    }

    pub fn identity_id(&self) -> Option<&IdentityId> {
        self.identity_id.as_ref()
    }

    pub fn email_id(&self) -> Option<&EmailId> {
        self.email_id.as_ref()
    }

    pub fn thread_id(&self) -> Option<&ThreadId> {
        self.thread_id.as_ref()
    }

    pub fn mail_from(&self) -> Option<&Address> {
        self.envelope.as_ref().map(|e| &e.mail_from)
    }

    pub fn rcpt_to(&self) -> Option<&[Address]> {
        self.envelope.as_ref().map(|e| e.rcpt_to.as_ref())
    }

    pub fn send_at(&self) -> Option<i64> {
        self.send_at.as_ref().map(chrono::DateTime::timestamp)
    }

    pub fn undo_status(&self) -> Option<&UndoStatus> {
        self.undo_status.as_ref()
    }

    pub fn delivery_status_email(&self, email: &str) -> Option<&DeliveryStatus> {
        self.delivery_status.as_ref().and_then(|ds| ds.get(email))
    }

    pub fn delivery_status(&self) -> Option<&HashMap<String, DeliveryStatus>> {
        self.delivery_status.as_ref()
    }

    pub fn dsn_blob_ids(&self) -> Option<&[BlobId]> {
        self.dsn_blob_ids.as_deref()
    }

    pub fn mdn_blob_ids(&self) -> Option<&[BlobId]> {
        self.mdn_blob_ids.as_deref()
    }
}

impl Address {
    pub fn email(&self) -> &str {
        &self.email
    }

    pub fn parameter(&self, param: &str) -> Option<&str> {
        self.parameters.as_ref()?.get(param)?.as_deref()
    }

    pub fn has_parameter(&self, param: &str) -> bool {
        self.parameters
            .as_ref()
            .map(|ps| ps.contains_key(param))
            .unwrap_or(false)
    }
}

impl DeliveryStatus {
    #[cfg(feature = "debug")]
    pub fn new(smtp_reply: impl Into<String>, delivered: Delivered, displayed: Displayed) -> Self {
        Self {
            smtp_reply: smtp_reply.into(),
            delivered,
            displayed,
        }
    }

    pub fn smtp_reply(&self) -> &str {
        &self.smtp_reply
    }

    pub fn delivered(&self) -> &Delivered {
        &self.delivered
    }

    pub fn displayed(&self) -> &Displayed {
        &self.displayed
    }
}
