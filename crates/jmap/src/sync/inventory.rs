use std::time::Instant;

use bifrost_types::{
    AccountStream, Batch, BlobId, CursorScope, Fingerprint, InventoryEntry, MembershipScope,
    ObjectId, ObjectType, PageBoundary, ServerVersion, SyncEvent, ThreadId,
};

use crate::core::query;
use crate::email::{Email, EmailGet, EmailId, EmailQuery, Property as EmailProperty};
use crate::mailbox::{Mailbox, MailboxGet, Property as MailboxProperty};
use crate::transport_reqwest::ReqwestTransport;

use super::capabilities::CoreLimits;

type MailAccount = crate::account::Account<ReqwestTransport>;

pub(crate) fn stream(
    mail: MailAccount,
    limits: CoreLimits,
    scope: CursorScope,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    match scope {
        CursorScope::Type(ObjectType::Email) => email_inventory(mail, limits),
        CursorScope::Type(ObjectType::Mailbox) => mailbox_inventory(mail),
        CursorScope::Type(ObjectType::Thread) => Box::pin(async_stream::stream! {
                yield super::error::fatal_unsupported(
                    "JMAP thread inventory is derived from Email inventory in this implementation",
                );
        }),
        CursorScope::Query(_) => Box::pin(async_stream::stream! {
                yield super::error::fatal_unsupported(
                    "JMAP query inventory requires registered query definitions outside the v1 Account trait",
                );
        }),
        _ => Box::pin(async_stream::stream! {
                yield super::error::fatal_unsupported("cursor scope is not supported by JMAP");
        }),
    }
}

fn email_inventory(
    mail: MailAccount,
    limits: CoreLimits,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    Box::pin(async_stream::stream! {
        let limit = limits.max_objects_in_get.max(1);
        let mut position: i32 = 0;

        loop {
            let started = Instant::now();
            let query_response = mail
                .call(
                    EmailQuery::new()
                        .sort([query::Comparator::new(crate::email::query::Comparator::ReceivedAt).descending()])
                        .position(position)
                        .limit(limit),
                )
                .await;

            let query_response = match query_response {
                Ok(response) => response,
                Err(err) => {
                    yield super::error::fatal_from_jmap(
                        err,
                        Some(CursorScope::Type(ObjectType::Email)),
                    );
                    break;
                }
            };

            let ids = query_response.ids().to_vec();
            if ids.is_empty() {
                yield SyncEvent::Done(None);
                break;
            }

            let get_response = mail
                .call(EmailGet::new().ids(ids.clone()).properties(inventory_properties()))
                .await;

            let get_response = match get_response {
                Ok(response) => response,
                Err(err) => {
                    yield super::error::fatal_from_jmap(
                        err,
                        Some(CursorScope::Type(ObjectType::Email)),
                    );
                    break;
                }
            };

            let state = get_response.state().to_string();
            let mut items = Vec::new();
            for email in get_response.into_list() {
                items.push(email_to_inventory(email, &state));
            }

            let batch_len = items.len();
            if batch_len != 0 {
                yield SyncEvent::Batch(Batch {
                    items,
                    page_boundary: PageBoundary::Page,
                    server_latency: started.elapsed(),
                    bytes_in: 0,
                    checkpoint: None,
                });
            }

            if batch_len < limit {
                yield SyncEvent::Done(None);
                break;
            }

            let advance = match i32::try_from(batch_len) {
                Ok(value) => value,
                Err(_) => {
                    yield super::error::fatal_unsupported(
                        "JMAP inventory page was too large to advance an i32 position",
                    );
                    break;
                }
            };
            position = match position.checked_add(advance) {
                Some(next) => next,
                None => {
                    yield super::error::fatal_unsupported(
                        "JMAP inventory position overflowed",
                    );
                    break;
                }
            };
        }
    })
}

pub(crate) fn inventory_properties() -> Vec<EmailProperty> {
    vec![
        EmailProperty::Id,
        EmailProperty::MailboxIds,
        EmailProperty::ThreadId,
        EmailProperty::BlobId,
        EmailProperty::Size,
        EmailProperty::Keywords,
        EmailProperty::MessageId,
        EmailProperty::References,
        EmailProperty::InReplyTo,
        EmailProperty::ReceivedAt,
    ]
}

fn mailbox_inventory(mail: MailAccount) -> AccountStream<SyncEvent<InventoryEntry>> {
    Box::pin(async_stream::stream! {
        let started = Instant::now();
        let response = mail
            .call(MailboxGet::new().properties(mailbox_inventory_properties()))
            .await;

        let response = match response {
            Ok(response) => response,
            Err(err) => {
                yield super::error::fatal_from_jmap(
                    err,
                    Some(CursorScope::Type(ObjectType::Mailbox)),
                );
                return;
            }
        };

        let state = response.state().to_string();
        let items = response
            .into_list()
            .into_iter()
            .map(|mailbox| mailbox_to_inventory(mailbox, &state))
            .collect::<Vec<_>>();

        yield SyncEvent::Batch(Batch {
            items,
            page_boundary: PageBoundary::Final,
            server_latency: started.elapsed(),
            bytes_in: 0,
            checkpoint: None,
        });
        yield SyncEvent::Done(None);
    })
}

fn mailbox_inventory_properties() -> Vec<MailboxProperty> {
    vec![
        MailboxProperty::Id,
        MailboxProperty::Name,
        MailboxProperty::ParentId,
        MailboxProperty::Role,
        MailboxProperty::SortOrder,
        MailboxProperty::TotalEmails,
        MailboxProperty::UnreadEmails,
        MailboxProperty::TotalThreads,
        MailboxProperty::UnreadThreads,
        MailboxProperty::IsSubscribed,
    ]
}

pub(crate) fn email_to_inventory(email: Email, state: &str) -> InventoryEntry {
    let id = email.id().map(ToString::to_string).unwrap_or_default();
    let memberships = email
        .mailbox_ids()
        .into_iter()
        .map(|mailbox| MembershipScope::Mailbox(bifrost_types::MailboxId(mailbox.to_string())))
        .collect::<Vec<_>>();
    let size = u64::try_from(email.size()).ok();
    let blob_id = email.blob_id().map(|id| BlobId(id.to_string()));
    let thread_id = email.thread_id().map(|id| ThreadId(id.to_string()));
    let message_id = email.message_id().and_then(|ids| ids.first().cloned());
    let references = email.references().map(<[_]>::to_vec).unwrap_or_default();
    let in_reply_to = email.in_reply_to().and_then(|ids| ids.first().cloned());

    InventoryEntry {
        id: ObjectId(id),
        memberships,
        size,
        blob_id,
        fingerprint: Fingerprint {
            server_version: ServerVersion::StateAt(state.to_string()),
            size,
            flags_hash: flags_hash(&email),
        },
        thread_id,
        message_id,
        references,
        in_reply_to,
    }
}

fn mailbox_to_inventory(mailbox: Mailbox, state: &str) -> InventoryEntry {
    let id = mailbox.id().map(ToString::to_string).unwrap_or_default();
    let memberships = mailbox
        .parent_id()
        .map(|parent| MembershipScope::Mailbox(bifrost_types::MailboxId(parent.to_string())))
        .into_iter()
        .collect::<Vec<_>>();
    let flags_hash = mailbox_flags_hash(&mailbox);

    InventoryEntry {
        id: ObjectId(id),
        memberships,
        size: None,
        blob_id: None,
        fingerprint: Fingerprint {
            server_version: ServerVersion::StateAt(state.to_string()),
            size: None,
            flags_hash,
        },
        thread_id: None,
        message_id: None,
        references: Vec::new(),
        in_reply_to: None,
    }
}

fn mailbox_flags_hash(mailbox: &Mailbox) -> u64 {
    let mut parts = Vec::new();
    if let Some(name) = mailbox.name() {
        parts.push(format!("name={name}"));
    }
    if let Some(parent) = mailbox.parent_id() {
        parts.push(format!("parent={parent}"));
    }
    if let Some(role) = mailbox.role() {
        parts.push(format!("role={role:?}"));
    }
    if let Some(sort_order) = mailbox.sort_order() {
        parts.push(format!("sort={sort_order}"));
    }
    if let Some(total) = mailbox.total_emails() {
        parts.push(format!("totalEmails={total}"));
    }
    if let Some(unread) = mailbox.unread_emails() {
        parts.push(format!("unreadEmails={unread}"));
    }
    if let Some(total) = mailbox.total_threads() {
        parts.push(format!("totalThreads={total}"));
    }
    if let Some(unread) = mailbox.unread_threads() {
        parts.push(format!("unreadThreads={unread}"));
    }
    if let Some(is_subscribed) = mailbox.is_subscribed() {
        parts.push(format!("subscribed={is_subscribed}"));
    }
    fnv1a64(parts)
}

pub(crate) fn flags_hash(email: &Email) -> u64 {
    let mut parts = Vec::new();
    let mut keywords = email
        .keywords()
        .into_iter()
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    keywords.sort_unstable();
    parts.extend(keywords);

    let mut mailboxes = email
        .mailbox_ids()
        .into_iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    mailboxes.sort_unstable();
    parts.extend(mailboxes);

    if let Some(received_at) = email.received_at() {
        parts.push(received_at.to_string());
    }
    parts.push(email.size().to_string());

    fnv1a64(parts)
}

pub(crate) fn fnv1a64(parts: impl IntoIterator<Item = String>) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    let mut hash = OFFSET;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}
