use super::{Mailbox, MailboxId, MailboxRights, Role};
use crate::principal::ACL;
use std::collections::HashMap;

impl Mailbox {
    pub fn id(&self) -> Option<&MailboxId> {
        self.id.as_ref()
    }

    pub fn take_id(&mut self) -> MailboxId {
        self.id.take().unwrap_or_else(|| MailboxId::new(""))
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn parent_id(&self) -> Option<&MailboxId> {
        self.parent_id.as_ref()
    }

    pub fn role(&self) -> Option<&Role> {
        self.role.as_ref()
    }

    pub fn sort_order(&self) -> Option<u32> {
        self.sort_order
    }

    pub fn total_emails(&self) -> Option<usize> {
        self.total_emails
    }

    pub fn unread_emails(&self) -> Option<usize> {
        self.unread_emails
    }

    pub fn total_threads(&self) -> Option<usize> {
        self.total_threads
    }

    pub fn unread_threads(&self) -> Option<usize> {
        self.unread_threads
    }

    pub fn is_subscribed(&self) -> Option<bool> {
        self.is_subscribed
    }

    pub fn my_rights(&self) -> Option<&MailboxRights> {
        self.my_rights.as_ref()
    }

    pub fn acl(&self) -> Option<&HashMap<String, HashMap<ACL, bool>>> {
        self.share_with.as_ref()
    }

    pub fn take_acl(&mut self) -> Option<HashMap<String, HashMap<ACL, bool>>> {
        self.share_with.take()
    }
}

impl MailboxRights {
    pub fn may_read_items(&self) -> bool {
        self.may_read_items
    }

    pub fn may_add_items(&self) -> bool {
        self.may_add_items
    }

    pub fn may_remove_items(&self) -> bool {
        self.may_remove_items
    }

    pub fn may_set_seen(&self) -> bool {
        self.may_set_seen
    }

    pub fn may_set_keywords(&self) -> bool {
        self.may_set_keywords
    }

    pub fn may_create_child(&self) -> bool {
        self.may_create_child
    }

    pub fn may_rename(&self) -> bool {
        self.may_rename
    }

    pub fn may_delete(&self) -> bool {
        self.may_delete
    }

    pub fn may_submit(&self) -> bool {
        self.may_submit
    }

    pub fn acl_list(&self) -> Vec<ACL> {
        let mut acl_list = Vec::new();
        for (is_set, acl) in [
            (self.may_read_items, ACL::ReadItems),
            (self.may_add_items, ACL::AddItems),
            (self.may_remove_items, ACL::RemoveItems),
            (self.may_set_seen, ACL::SetSeen),
            (self.may_set_keywords, ACL::SetKeywords),
            (self.may_create_child, ACL::CreateChild),
            (self.may_rename, ACL::Rename),
            (self.may_delete, ACL::Delete),
            (self.may_submit, ACL::Submit),
        ] {
            if is_set && !acl_list.contains(&acl) {
                acl_list.push(acl);
            }
        }
        acl_list
    }
}
