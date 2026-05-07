use super::{Mailbox, MailboxRights, Role};
use crate::{Get, principal::ACL};
use std::collections::HashMap;

impl Mailbox<Get> {
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    pub fn take_id(&mut self) -> String {
        self.id.take().unwrap_or_default()
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn parent_id(&self) -> Option<&str> {
        self.parent_id.as_deref()
    }

    /// The mailbox's role.
    ///
    /// Returns `None` if the server did not include a `role` value
    /// (RFC 8621 lets an implementation omit the property when no
    /// role applies). Returns `Some(Role::None)` when the server
    /// explicitly sent the JMAP-defined "no specific role" value -
    /// distinct from "server omitted the property" but rarely
    /// distinguished by callers in practice.
    pub fn role(&self) -> Option<&Role> {
        self.role.as_ref()
    }

    /// The mailbox's `sortOrder` if the server included one. Was
    /// previously `usize` with a silent `0` for omitted - that
    /// collapsed "server omitted" with "explicit 0".
    pub fn sort_order(&self) -> Option<u32> {
        self.sort_order
    }

    /// Total emails in this mailbox if the server reported it.
    /// `None` when the server omitted the property (e.g. count not
    /// yet computed); was previously `usize` with a silent `0` for
    /// omitted.
    pub fn total_emails(&self) -> Option<usize> {
        self.total_emails
    }

    /// Unread emails in this mailbox if the server reported it.
    /// `None` semantics match [`Self::total_emails`].
    pub fn unread_emails(&self) -> Option<usize> {
        self.unread_emails
    }

    /// Total threads in this mailbox if the server reported it.
    pub fn total_threads(&self) -> Option<usize> {
        self.total_threads
    }

    /// Unread threads in this mailbox if the server reported it.
    pub fn unread_threads(&self) -> Option<usize> {
        self.unread_threads
    }

    /// Whether the user is subscribed to this mailbox, if the server
    /// reported it. Was previously `bool` returning `false` for
    /// omitted - which conflicted with "explicitly unsubscribed".
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

crate::impl_get_object!(Mailbox, ());
