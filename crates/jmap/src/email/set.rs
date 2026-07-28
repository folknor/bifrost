use super::{
    EmailAddress, EmailAddressGroup, EmailBodyPart, EmailBodyValue, EmailCreate, EmailHeader,
    EmailPatch, Header, HeaderValue,
};
use crate::core::id::BlobId;
use crate::core::{request::ResultReference, set::from_timestamp};
use crate::mailbox::MailboxId;
use serde::Serialize;
use std::collections::HashMap;

impl EmailCreate {
    pub(crate) fn mailbox_ids<T, U>(&mut self, mailbox_ids: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<MailboxId>,
    {
        self.mailbox_ids = Some(mailbox_ids.into_iter().map(|s| (s.into(), true)).collect());
        self.mailbox_ids_ref = None;
        self
    }

    pub(crate) fn mailbox_ids_ref(&mut self, reference: ResultReference) -> &mut Self {
        self.mailbox_ids_ref = reference.into();
        self.mailbox_ids = None;
        self
    }

    pub(crate) fn keywords<T, U>(&mut self, keywords: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
    {
        self.keywords = Some(keywords.into_iter().map(|s| (s.into(), true)).collect());
        self
    }

    pub(crate) fn message_id<T, U>(&mut self, message_id: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
    {
        self.message_id = Some(
            message_id
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
        );
        self
    }

    pub(crate) fn in_reply_to<T, U>(&mut self, in_reply_to: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
    {
        self.in_reply_to = Some(
            in_reply_to
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
        );
        self
    }

    pub(crate) fn references<T, U>(&mut self, references: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
    {
        self.references = Some(
            references
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
        );
        self
    }

    pub(crate) fn sender<T, U>(&mut self, sender: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.sender = Some(sender.into_iter().map(std::convert::Into::into).collect());
        self
    }

    pub(crate) fn from<T, U>(&mut self, from: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.from = Some(from.into_iter().map(std::convert::Into::into).collect());
        self
    }

    pub(crate) fn to<T, U>(&mut self, to: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.to = Some(to.into_iter().map(std::convert::Into::into).collect());
        self
    }

    pub(crate) fn cc<T, U>(&mut self, cc: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.cc = Some(cc.into_iter().map(std::convert::Into::into).collect());
        self
    }

    pub(crate) fn bcc<T, U>(&mut self, bcc: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.bcc = Some(bcc.into_iter().map(std::convert::Into::into).collect());
        self
    }

    pub(crate) fn reply_to<T, U>(&mut self, reply_to: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.reply_to = Some(reply_to.into_iter().map(std::convert::Into::into).collect());
        self
    }

    pub(crate) fn subject(&mut self, subject: impl Into<String>) -> &mut Self {
        self.subject = Some(subject.into());
        self
    }

    pub(crate) fn sent_at(&mut self, sent_at: i64) -> &mut Self {
        self.sent_at = Some(from_timestamp(sent_at));
        self
    }

    pub(crate) fn body_structure(&mut self, body_structure: EmailBodyPart) -> &mut Self {
        self.body_structure = Some(body_structure.into());
        self
    }

    pub(crate) fn body_value(
        &mut self,
        id: String,
        body_value: impl Into<EmailBodyValue>,
    ) -> &mut Self {
        self.body_values
            .get_or_insert_with(HashMap::new)
            .insert(id, body_value.into());
        self
    }

    pub(crate) fn text_body(&mut self, text_body: impl Into<EmailBodyPart>) -> &mut Self {
        self.text_body
            .get_or_insert_with(Vec::new)
            .push(text_body.into());
        self
    }

    pub(crate) fn html_body(&mut self, html_body: impl Into<EmailBodyPart>) -> &mut Self {
        self.html_body
            .get_or_insert_with(Vec::new)
            .push(html_body.into());
        self
    }

    pub(crate) fn attachment(&mut self, attachment: impl Into<EmailBodyPart>) -> &mut Self {
        self.attachments
            .get_or_insert_with(Vec::new)
            .push(attachment.into());
        self
    }

    pub(crate) fn header(&mut self, header: Header, value: impl Into<HeaderValue>) -> &mut Self {
        self.headers.insert(header, Some(value.into()));
        self
    }

    pub(crate) fn received_at(&mut self, received_at: i64) -> &mut Self {
        self.received_at = Some(from_timestamp(received_at));
        self
    }
}

impl EmailPatch {
    /// Set/clear a single mailbox membership via dotted-path patch.
    pub(crate) fn mailbox_id(&mut self, mailbox_id: &MailboxId, set: bool) -> &mut Self {
        self.mailbox_ids = None;
        self.patch.get_or_insert_with(HashMap::new).insert(
            format!("mailboxIds/{mailbox_id}"),
            if set {
                serde_json::Value::Bool(true)
            } else {
                serde_json::Value::Null
            },
        );
        self
    }

    pub(crate) fn mailbox_ids<T, U>(&mut self, mailbox_ids: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<MailboxId>,
    {
        self.mailbox_ids = Some(mailbox_ids.into_iter().map(|s| (s.into(), true)).collect());
        self
    }

    /// Set/clear a single keyword via dotted-path patch.
    pub(crate) fn keyword(&mut self, keyword: &str, set: bool) -> &mut Self {
        self.keywords = None;
        self.patch.get_or_insert_with(HashMap::new).insert(
            format!("keywords/{keyword}"),
            if set {
                serde_json::Value::Bool(true)
            } else {
                serde_json::Value::Null
            },
        );
        self
    }

    pub(crate) fn keywords<T, U>(&mut self, keywords: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
    {
        self.keywords = Some(keywords.into_iter().map(|s| (s.into(), true)).collect());
        self
    }

    pub(crate) fn subject(&mut self, subject: impl Into<String>) -> &mut Self {
        self.subject = Some(subject.into());
        self
    }

    pub(crate) fn raw_property<T: Serialize>(
        &mut self,
        property: impl Into<String>,
        value: &T,
    ) -> serde_json::Result<&mut Self> {
        self.patch
            .get_or_insert_with(HashMap::new)
            .insert(property.into(), serde_json::to_value(value)?);
        Ok(self)
    }

    pub(crate) fn null_property(&mut self, property: impl Into<String>) -> &mut Self {
        self.patch
            .get_or_insert_with(HashMap::new)
            .insert(property.into(), serde_json::Value::Null);
        self
    }

    /// The `onSuccessUpdateEmail` patch for a message that has just
    /// been submitted: relocate it to the Sent mailbox and stop it
    /// being a draft.
    ///
    /// Rewriting `mailboxIds` alone is not enough. RFC 8621 s4.1.1
    /// makes `$draft` the authoritative "this is a draft" signal -
    /// membership of the Drafts mailbox is a consequence, not the
    /// cause - so a submitted message that keeps the keyword is listed
    /// under BOTH Drafts and Sent by any client that filters on it.
    /// The two edits touch different top-level properties
    /// (`mailboxIds` wholesale, `keywords/$draft` by patch path), which
    /// RFC 8620 s5.3 permits; only mixing a property with a path into
    /// that same property is forbidden.
    pub(crate) fn submitted_to_sent(&mut self, sent: impl Into<MailboxId>) -> &mut Self {
        self.mailbox_ids([sent]);
        self.keyword(super::DRAFT_KEYWORD, false)
    }
}

impl EmailBodyPart {
    pub(crate) fn new() -> EmailBodyPart {
        EmailBodyPart::default()
    }

    pub(crate) fn with_part_id(mut self, part_id: impl Into<String>) -> Self {
        self.part_id = Some(part_id.into());
        self
    }

    pub(crate) fn with_blob_id(mut self, blob_id: impl Into<BlobId>) -> Self {
        self.blob_id = Some(blob_id.into());
        self
    }

    pub(crate) fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub(crate) fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.type_ = Some(content_type.into());
        self
    }

    pub(crate) fn with_content_id(mut self, content_id: impl Into<String>) -> Self {
        self.cid = Some(content_id.into());
        self
    }

    pub(crate) fn with_content_language<T, U>(mut self, content_language: T) -> Self
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
    {
        self.language = Some(
            content_language
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
        );
        self
    }

    pub(crate) fn with_content_location(mut self, content_location: impl Into<String>) -> Self {
        self.location = Some(content_location.into());
        self
    }

    pub(crate) fn with_sub_part(mut self, sub_part: EmailBodyPart) -> Self {
        self.sub_parts.get_or_insert_with(Vec::new).push(sub_part);
        self
    }
}

impl From<String> for EmailBodyValue {
    fn from(value: String) -> Self {
        EmailBodyValue {
            value,
            is_encoding_problem: None,
            is_truncated: None,
        }
    }
}

impl From<&str> for EmailBodyValue {
    fn from(value: &str) -> Self {
        EmailBodyValue {
            value: value.to_string(),
            is_encoding_problem: None,
            is_truncated: None,
        }
    }
}

impl EmailAddress {
    pub(crate) fn new(email: String) -> EmailAddress {
        EmailAddress { name: None, email }
    }

    pub(crate) fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }
}

impl From<String> for EmailAddress {
    fn from(email: String) -> Self {
        EmailAddress { name: None, email }
    }
}

impl From<(String, String)> for EmailAddress {
    fn from(parts: (String, String)) -> Self {
        EmailAddress {
            name: parts.0.into(),
            email: parts.1,
        }
    }
}

impl From<&str> for EmailAddress {
    fn from(email: &str) -> Self {
        EmailAddress {
            name: None,
            email: email.to_string(),
        }
    }
}

impl From<(&str, &str)> for EmailAddress {
    fn from(parts: (&str, &str)) -> Self {
        EmailAddress {
            name: parts.0.to_string().into(),
            email: parts.1.to_string(),
        }
    }
}

impl EmailAddressGroup {
    pub(crate) fn new() -> EmailAddressGroup {
        EmailAddressGroup {
            name: None,
            addresses: Vec::new(),
        }
    }

    pub(crate) fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub(crate) fn with_address(mut self, address: impl Into<EmailAddress>) -> Self {
        self.addresses.push(address.into());
        self
    }
}

impl Default for EmailAddressGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl EmailHeader {
    pub(crate) fn new(name: String, value: String) -> EmailHeader {
        EmailHeader { name, value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submitted_to_sent_moves_and_clears_the_draft_keyword() {
        let mut patch = EmailPatch::default();
        patch.submitted_to_sent(MailboxId::new("sent-1"));
        let json = serde_json::to_value(&patch).expect("serializable patch");

        assert_eq!(
            json.get("mailboxIds")
                .and_then(|v| v.get("sent-1"))
                .and_then(serde_json::Value::as_bool),
            Some(true),
            "the sent message must land in the Sent mailbox"
        );
        assert_eq!(
            json.get("keywords/$draft"),
            Some(&serde_json::Value::Null),
            "a submitted message that keeps $draft shows under Drafts AND Sent"
        );
        // RFC 8620 s5.3: a property and a patch path into that same
        // property must not both appear. `keywords` wholesale must stay
        // absent now that `keywords/$draft` is present.
        assert!(json.get("keywords").is_none());
    }
}
