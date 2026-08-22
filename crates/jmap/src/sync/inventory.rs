use std::time::Instant;

use bifrost_types::{
    AccountStream, Batch, BlobId, CursorScope, Fingerprint, InventoryEntry, InventoryPartition,
    MailboxId as TypesMailboxId, MembershipScope, ObjectId, ObjectType, PageBoundary,
    ServerVersion, SyncEvent, ThreadId,
};

use crate::core::query;
use crate::core::transport::HttpTransport;
use crate::email::{Email, EmailGet, EmailQuery, Property as EmailProperty};
use crate::mailbox::{Mailbox, MailboxGet, Property as MailboxProperty};

use super::capabilities::CoreLimits;

type MailAccount<T> = crate::account::Account<T>;

pub(crate) fn stream<T: HttpTransport>(
    mail: MailAccount<T>,
    limits: CoreLimits,
    scope: CursorScope,
    owner: Option<TypesMailboxId>,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    match scope {
        CursorScope::Type(ObjectType::Email) => email_inventory(mail, limits),
        CursorScope::Type(ObjectType::Mailbox) => mailbox_inventory(mail),
        // A foreign (shared/delegate) account scope: page its emails via
        // `Email/query` against the foreign account, hydrating inventory
        // entries there. The seeded shape is the ACCOUNT-LEVEL scope
        // (empty mailbox part), which walks the whole account unfiltered
        // - one walk per share, mirroring the primary `Type(Email)`
        // inventory. A legacy per-mailbox scope still decodes and takes
        // the `inMailbox`-filtered walk.
        CursorScope::Folder(ref folder) => match super::foreign::parse_foreign(folder) {
            Some(parsed) => {
                foreign_email_inventory(mail, limits, scope.clone(), parsed.mailbox_id, owner)
            }
            None => Box::pin(async_stream::stream! {
                yield super::error::terminated_unsupported(
                    bifrost_types::AccountOperation::SyncInventory,
                    Some(bifrost_types::ErrorScope::Cursor(scope.clone())),
                    "JMAP folder scope is not a foreign-mailbox scope",
                );
            }),
        },
        CursorScope::Type(ObjectType::Thread) => Box::pin(async_stream::stream! {
                yield super::error::terminated_unsupported(
                    bifrost_types::AccountOperation::SyncInventory,
                    Some(bifrost_types::ErrorScope::Cursor(CursorScope::Type(ObjectType::Thread))),
                    "JMAP thread inventory is derived from Email inventory in this implementation",
                );
        }),
        CursorScope::Query(_) => Box::pin(async_stream::stream! {
                yield super::error::terminated_unsupported(
                    bifrost_types::AccountOperation::SyncInventory,
                    None,
                    "JMAP query inventory requires registered query definitions outside the v1 Account trait",
                );
        }),
        _ => Box::pin(async_stream::stream! {
                yield super::error::terminated_unsupported(
                    bifrost_types::AccountOperation::SyncInventory,
                    None,
                    "cursor scope is not supported by JMAP",
                );
        }),
    }
}

pub(crate) fn stream_partition<T: HttpTransport>(
    mail: MailAccount<T>,
    limits: CoreLimits,
    scope: CursorScope,
    partition: InventoryPartition,
    owner: Option<TypesMailboxId>,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    match partition {
        InventoryPartition::Full => stream(mail, limits, scope, owner),
        InventoryPartition::Page { from, to }
            if matches!(scope, CursorScope::Type(ObjectType::Email)) =>
        {
            email_inventory_page(mail, limits, from, to)
        }
        _ => Box::pin(async_stream::stream! {
            yield super::error::terminated_unsupported(
                bifrost_types::AccountOperation::SyncInventory,
                None,
                "JMAP inventory partition is not supported for this cursor scope",
            );
        }),
    }
}

fn foreign_email_inventory<T: HttpTransport>(
    mail: MailAccount<T>,
    limits: CoreLimits,
    scope: CursorScope,
    mailbox_id: String,
    owner: Option<TypesMailboxId>,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    Box::pin(async_stream::stream! {
        let limit = limits.max_objects_in_get.max(1);
        let mut position: i32 = 0;

        loop {
            let started = Instant::now();
            // The account-level scope (empty mailbox part) queries the
            // whole account; only a legacy per-mailbox scope filters.
            let mut query = EmailQuery::new();
            if !mailbox_id.is_empty() {
                query = query.filter(crate::email::query::Filter::in_mailbox(mailbox_id.clone()));
            }
            let query = query
                .sort([query::Comparator::new(crate::email::query::Comparator::ReceivedAt).descending()])
                .position(position)
                .limit(limit);
            let query_response = mail.call(query).await;

            let query_response = match query_response {
                Ok(response) => response,
                Err(err) => {
                    yield super::error::terminated(super::error::shared_scope_error(
                        err,
                        &scope,
                        owner.as_ref(),
                        super::error::JmapErrorContext::cursor(
                            bifrost_types::AccountOperation::SyncInventory,
                            scope.clone(),
                        ),
                    ));
                    break;
                }
            };

            let ids = query_response.ids().to_vec();
            if ids.is_empty() {
                yield SyncEvent::Done(None);
                break;
            }
            // Position is in the query result space, not the hydrated
            // result space. A server may cap Email/query below this request's
            // limit, and an id can disappear between query and get.
            let consumed = ids.len();

            let get_response = mail
                .call(EmailGet::new().ids(ids).properties(inventory_properties()))
                .await;

            let get_response = match get_response {
                Ok(response) => response,
                Err(err) => {
                    yield super::error::terminated(super::error::shared_scope_error(
                        err,
                        &scope,
                        owner.as_ref(),
                        super::error::JmapErrorContext::cursor(
                            bifrost_types::AccountOperation::SyncInventory,
                            scope.clone(),
                        ),
                    ));
                    break;
                }
            };

            let state = get_response.state().to_string();
            let mut items = Vec::new();
            for email in get_response.into_list().into_iter().filter(email_has_id) {
                let mut entry = email_to_inventory(email, &state);
                // Each foreign (shared/delegate) inventory item carries
                // its owning account's `Mailbox(accountId)` membership in
                // addition to its native mailbox memberships, so the
                // consumer maps the item to its shared-account owner (the
                // A5c-established owner-tag pattern).
                if let Some(owner) = &owner {
                    qualify_foreign_memberships(&mut entry.memberships, owner);
                    qualify_foreign_ids(&mut entry, owner);
                }
                items.push(entry);
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

            let advance = match i32::try_from(consumed) {
                Ok(value) => value,
                Err(_) => {
                    yield super::error::terminated_contract_violation(
                        bifrost_types::AccountOperation::SyncInventory,
                        Some(bifrost_types::ErrorScope::Cursor(scope.clone())),
                        "JMAP foreign inventory page was too large to advance an i32 position",
                    );
                    break;
                }
            };
            position = match position.checked_add(advance) {
                Some(next) => next,
                None => {
                    yield super::error::terminated_contract_violation(
                        bifrost_types::AccountOperation::SyncInventory,
                        Some(bifrost_types::ErrorScope::Cursor(scope.clone())),
                        "JMAP foreign inventory position overflowed",
                    );
                    break;
                }
            };
        }
    })
}

/// Qualify a foreign (shared/delegate) item's memberships with its
/// owning account. The `owner` tag IS the foreign JMAP accountId.
/// Each native `Mailbox(native)` membership is re-encoded as
/// `Folder(encode_foreign(accountId, native))` - byte-identical to the
/// container ids `containers_list` mints for the share - and the owner
/// `Mailbox(accountId)` tag is appended. Without the re-encoding, a
/// foreign native mailbox id (e.g. `inbox`) collides with the primary's
/// identical id in the engine's membership index and the two accounts'
/// messages conflate. Shared by the foreign inventory walk and by
/// `hydrate`'s Metadata projection: hydration is where the consumer
/// learns which folder a foreign change landed in (the account-level
/// change stream cannot know), so the two must speak one namespace.
pub(crate) fn qualify_foreign_memberships(
    memberships: &mut Vec<MembershipScope>,
    owner: &TypesMailboxId,
) {
    for membership in memberships.iter_mut() {
        if let MembershipScope::Mailbox(native) = membership {
            *membership =
                MembershipScope::Folder(super::foreign::encode_foreign(&owner.0, &native.0));
        }
    }
    memberships.push(MembershipScope::Mailbox(owner.clone()));
}

/// Qualify a foreign inventory item's OBJECT ids with its owning account.
///
/// `Email/get` and blob download are accountId-scoped, but `get_stream` /
/// `open_blob` receive only an id - no scope - so a bare native id would
/// route hydration and blob reads through the PRIMARY account and fail (or
/// silently resolve a same-id primary object). Encoding the owning
/// accountId into both the object id and the whole-message `blobId` is what
/// makes those reads self-routing, exactly as the folder codec makes the
/// cursor scope self-routing on a cold resume.
///
/// The `thread_id` rides the same object namespace. It is not a read-only
/// grouping key: the consumer hands it straight back as
/// `thread_hydrate(thread)` and as `MutationTarget::Thread`, and every one
/// of those doors expands it through an accountId-scoped `Thread/get`.
/// Left bare, a foreign thread id is indistinguishable from a primary one,
/// so on an id collision the primary `Thread/get` resolves an UNRELATED
/// thread and the mutation lands on its messages - `delete_thread`
/// destroys them.
pub(crate) fn qualify_foreign_ids(entry: &mut InventoryEntry, owner: &TypesMailboxId) {
    entry.id = ObjectId(super::foreign::encode_object(&owner.0, &entry.id.0));
    if let Some(blob) = entry.blob_id.take() {
        entry.blob_id = Some(BlobId(super::foreign::encode_object(&owner.0, &blob.0)));
    }
    if let Some(thread) = entry.thread_id.take() {
        entry.thread_id = Some(ThreadId(super::foreign::encode_object(&owner.0, &thread.0)));
    }
}

fn email_inventory<T: HttpTransport>(
    mail: MailAccount<T>,
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
                    yield super::error::terminated_from_jmap(
                        err,
                        super::error::JmapErrorContext::cursor(
                            bifrost_types::AccountOperation::SyncInventory,
                            CursorScope::Type(ObjectType::Email),
                        ),
                    );
                    break;
                }
            };

            let ids = query_response.ids().to_vec();
            if ids.is_empty() {
                yield SyncEvent::Done(None);
                break;
            }
            // See the corresponding foreign-inventory loop: only an empty
            // query page means end-of-inventory.
            let consumed = ids.len();

            let get_response = mail
                .call(EmailGet::new().ids(ids).properties(inventory_properties()))
                .await;

            let get_response = match get_response {
                Ok(response) => response,
                Err(err) => {
                    yield super::error::terminated_from_jmap(
                        err,
                        super::error::JmapErrorContext::cursor(
                            bifrost_types::AccountOperation::SyncInventory,
                            CursorScope::Type(ObjectType::Email),
                        ),
                    );
                    break;
                }
            };

            let state = get_response.state().to_string();
            let mut items = Vec::new();
            for email in get_response.into_list().into_iter().filter(email_has_id) {
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

            let advance = match i32::try_from(consumed) {
                Ok(value) => value,
                Err(_) => {
                    // Pagination shape mismatch: the server returned a
                    // page so large that the protocol can't advance the
                    // position. Classify as `Protocol(ContractViolation)`,
                    // not `Unsupported` - the operation is supported,
                    // the response shape is not.
                    yield super::error::terminated_contract_violation(
                        bifrost_types::AccountOperation::SyncInventory,
                        Some(bifrost_types::ErrorScope::Cursor(
                            CursorScope::Type(ObjectType::Email),
                        )),
                        "JMAP inventory page was too large to advance an i32 position",
                    );
                    break;
                }
            };
            position = match position.checked_add(advance) {
                Some(next) => next,
                None => {
                    yield super::error::terminated_contract_violation(
                        bifrost_types::AccountOperation::SyncInventory,
                        Some(bifrost_types::ErrorScope::Cursor(
                            CursorScope::Type(ObjectType::Email),
                        )),
                        "JMAP inventory position overflowed",
                    );
                    break;
                }
            };
        }
    })
}

fn email_inventory_page<T: HttpTransport>(
    mail: MailAccount<T>,
    _limits: CoreLimits,
    from: u32,
    to: u32,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    Box::pin(async_stream::stream! {
        if to <= from {
            yield SyncEvent::Done(None);
            return;
        }

        // The Page partition contract is "yield every entry whose query
        // position falls in [from, to)". A single Email/query cannot be
        // trusted to honor that span: a server whose query page cap is
        // below the requested window width (saehrimnir caps queries below
        // its get cap) returns fewer ids than asked for, and the
        // orchestrator reads a short window as end-of-inventory and
        // silently drops every later page. So we page internally -
        // advancing position by the count the server actually returned -
        // until the window is filled or the server runs out of results.
        let window = match usize::try_from(to - from) {
            Ok(window) if window != 0 => window,
            _ => {
                yield super::error::terminated_contract_violation(
                    bifrost_types::AccountOperation::SyncInventory,
                    Some(bifrost_types::ErrorScope::Cursor(
                        CursorScope::Type(ObjectType::Email),
                    )),
                    "JMAP inventory page range could not be converted to usize",
                );
                return;
            }
        };
        let mut position = match i32::try_from(from) {
            Ok(position) => position,
            Err(_) => {
                yield super::error::terminated_contract_violation(
                    bifrost_types::AccountOperation::SyncInventory,
                    Some(bifrost_types::ErrorScope::Cursor(
                        CursorScope::Type(ObjectType::Email),
                    )),
                    "JMAP inventory page position exceeded i32",
                );
                return;
            }
        };
        let mut remaining = window;

        while remaining != 0 {
            let started = Instant::now();
            let query_response = mail
                .call(
                    EmailQuery::new()
                        .sort([query::Comparator::new(crate::email::query::Comparator::ReceivedAt).descending()])
                        .position(position)
                        .limit(remaining),
                )
                .await;

            let query_response = match query_response {
                Ok(response) => response,
                Err(err) => {
                    yield super::error::terminated_from_jmap(
                        err,
                        super::error::JmapErrorContext::cursor(
                            bifrost_types::AccountOperation::SyncInventory,
                            CursorScope::Type(ObjectType::Email),
                        ),
                    );
                    return;
                }
            };

            let ids = query_response.ids().to_vec();
            // An empty query page is the real end-of-results signal: the
            // window is not full but the server has nothing past this
            // position. Stop here rather than re-reading the same offset.
            if ids.is_empty() {
                break;
            }
            // Position walks the query result space, so advance by the
            // number of ids consumed from the query - not by the count of
            // hydrated objects, which can be smaller when an id vanished
            // between query and get.
            let consumed = ids.len();

            let get_response = mail
                .call(EmailGet::new().ids(ids).properties(inventory_properties()))
                .await;

            let get_response = match get_response {
                Ok(response) => response,
                Err(err) => {
                    yield super::error::terminated_from_jmap(
                        err,
                        super::error::JmapErrorContext::cursor(
                            bifrost_types::AccountOperation::SyncInventory,
                            CursorScope::Type(ObjectType::Email),
                        ),
                    );
                    return;
                }
            };

            let state = get_response.state().to_string();
            let items = get_response
                .into_list()
                .into_iter()
                .filter(email_has_id)
                .map(|email| email_to_inventory(email, &state))
                .collect::<Vec<_>>();

            if !items.is_empty() {
                yield SyncEvent::Batch(Batch {
                    items,
                    page_boundary: PageBoundary::Page,
                    server_latency: started.elapsed(),
                    bytes_in: 0,
                    checkpoint: None,
                });
            }

            remaining = remaining.saturating_sub(consumed);
            let advance = match i32::try_from(consumed) {
                Ok(value) => value,
                Err(_) => {
                    yield super::error::terminated_contract_violation(
                        bifrost_types::AccountOperation::SyncInventory,
                        Some(bifrost_types::ErrorScope::Cursor(
                            CursorScope::Type(ObjectType::Email),
                        )),
                        "JMAP inventory page was too large to advance an i32 position",
                    );
                    return;
                }
            };
            position = match position.checked_add(advance) {
                Some(next) => next,
                None => {
                    yield super::error::terminated_contract_violation(
                        bifrost_types::AccountOperation::SyncInventory,
                        Some(bifrost_types::ErrorScope::Cursor(
                            CursorScope::Type(ObjectType::Email),
                        )),
                        "JMAP inventory position overflowed",
                    );
                    return;
                }
            };
        }

        yield SyncEvent::Done(None);
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

fn mailbox_inventory<T: HttpTransport>(
    mail: MailAccount<T>,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    Box::pin(async_stream::stream! {
        let started = Instant::now();
        let response = mail
            .call(MailboxGet::new().properties(mailbox_inventory_properties()))
            .await;

        let response = match response {
            Ok(response) => response,
            Err(err) => {
                yield super::error::terminated_from_jmap(
                    err,
                    super::error::JmapErrorContext::cursor(
                        bifrost_types::AccountOperation::SyncInventory,
                        CursorScope::Type(ObjectType::Mailbox),
                    ),
                );
                return;
            }
        };

        let state = response.state().to_string();
        let items = response
            .into_list()
            .into_iter()
            .filter(mailbox_has_id)
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

/// Whether a fetched object carries an id we can hand to a consumer.
///
/// An object arriving with no `id`, or an empty one, cannot become an
/// inventory row: `ObjectId("")` is a handle to nothing, and once qualified
/// for a foreign account it becomes the equally bogus `"acct-9\u{1f}"`, which
/// then routes later reads at whatever that string happens to collide with.
/// The rest of the crate already refuses the shape - hydration drops the
/// object so the submitted id falls through to the `PartialResponse` lane, and
/// container discovery filters it - so the inventory walks, the one path that
/// used to manufacture the row instead, drop it here. Under the crate's closed
/// per-item accounting an unidentifiable object is not an item.
fn email_has_id(email: &Email) -> bool {
    email.id().is_some_and(|id| !id.as_str().is_empty())
}

/// Mailbox counterpart of [`email_has_id`]; same reasoning.
fn mailbox_has_id(mailbox: &Mailbox) -> bool {
    mailbox.id().is_some_and(|id| !id.as_str().is_empty())
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

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::MailboxId;

    #[test]
    fn foreign_memberships_are_qualified_with_owner_account() {
        let owner = TypesMailboxId("acct-9".to_string());
        let mut memberships = vec![
            MembershipScope::Mailbox(MailboxId("inbox".to_string())),
            MembershipScope::Mailbox(MailboxId("mbx-2".to_string())),
        ];
        qualify_foreign_memberships(&mut memberships, &owner);

        // Each native membership is re-encoded into the foreign account's
        // namespace; a bare `Mailbox(inbox)` (which would collide with
        // the primary's `inbox`) must not survive.
        let inbox_qualified = super::super::foreign::encode_foreign("acct-9", "inbox");
        assert!(memberships.contains(&MembershipScope::Folder(inbox_qualified)));
        assert!(
            !memberships.contains(&MembershipScope::Mailbox(MailboxId("inbox".to_string()))),
            "native mailbox membership must be qualified, not bare"
        );
        // The owner tag is appended.
        assert!(memberships.contains(&MembershipScope::Mailbox(owner.clone())));
        // Two native memberships qualified + one owner tag.
        assert_eq!(memberships.len(), 3);
    }

    #[test]
    fn foreign_object_and_blob_ids_are_qualified_with_owner_account() {
        let owner = TypesMailboxId("acct-9".to_string());
        let mut entry = InventoryEntry {
            id: ObjectId("M1".to_string()),
            memberships: Vec::new(),
            size: Some(10),
            blob_id: Some(BlobId("B1".to_string())),
            fingerprint: Fingerprint {
                server_version: ServerVersion::StateAt("s1".to_string()),
                size: Some(10),
                flags_hash: 0,
            },
            thread_id: Some(ThreadId("T1".to_string())),
            message_id: None,
            references: Vec::new(),
            in_reply_to: None,
        };
        qualify_foreign_ids(&mut entry, &owner);

        // Both ids carry the owning account so hydration / blob reads route
        // there instead of the primary account.
        assert_eq!(
            entry.id.0,
            super::super::foreign::encode_object("acct-9", "M1")
        );
        assert_eq!(super::super::foreign::native_object(&entry.id.0), "M1");
        let blob = entry.blob_id.expect("blob id present");
        assert_eq!(blob.0, super::super::foreign::encode_object("acct-9", "B1"));
        assert_eq!(super::super::foreign::native_object(&blob.0), "B1");

        // The thread id is qualified in the same namespace and decodes
        // back to the native id the foreign `Thread/get` expects. Left
        // bare it would assert PRIMARY ownership, and a thread-keyed
        // mutation on an id collision would rewrite an unrelated primary
        // thread's messages.
        let thread = entry.thread_id.expect("thread id present");
        assert_eq!(
            thread.0,
            super::super::foreign::encode_object("acct-9", "T1")
        );
        assert_eq!(super::super::foreign::native_object(&thread.0), "T1");
    }

    /// A primary entry keeps every id bare: one logical object, one wire
    /// form, and a bare thread id is exactly the assertion "this is the
    /// primary account's thread".
    #[test]
    fn a_primary_entry_is_never_qualified() {
        let email: Email = serde_json::from_value(serde_json::json!({
            "id": "M1",
            "blobId": "B1",
            "threadId": "T1",
            "size": 10,
            "mailboxIds": {"inbox": true},
            "keywords": {}
        }))
        .expect("email deserializes");
        let entry = email_to_inventory(email, "s1");

        assert_eq!(entry.id.0, "M1");
        assert_eq!(
            entry.blob_id.as_ref().map(|blob| blob.0.as_str()),
            Some("B1")
        );
        assert_eq!(
            entry.thread_id.as_ref().map(|thread| thread.0.as_str()),
            Some("T1")
        );
    }

    /// An object the server returned without a usable `id` is not an
    /// inventory item: the walks filter it rather than minting
    /// `ObjectId("")`, which would qualify into `"acct-9\u{1f}"` for a
    /// foreign share and route later reads at nothing.
    #[test]
    fn an_object_without_an_id_is_not_an_inventory_item() {
        let missing: Email = serde_json::from_value(serde_json::json!({
            "blobId": "B1",
            "size": 10,
            "mailboxIds": {"inbox": true},
            "keywords": {}
        }))
        .expect("email deserializes");
        assert!(!super::email_has_id(&missing));

        let empty: Email = serde_json::from_value(serde_json::json!({
            "id": "",
            "blobId": "B1",
            "size": 10,
            "mailboxIds": {"inbox": true},
            "keywords": {}
        }))
        .expect("email deserializes");
        assert!(!super::email_has_id(&empty));

        let present: Email = serde_json::from_value(serde_json::json!({
            "id": "M1",
            "blobId": "B1",
            "size": 10,
            "mailboxIds": {"inbox": true},
            "keywords": {}
        }))
        .expect("email deserializes");
        assert!(super::email_has_id(&present));

        let mailbox: Mailbox = serde_json::from_value(serde_json::json!({"name": "Inbox"}))
            .expect("mailbox deserializes");
        assert!(!super::mailbox_has_id(&mailbox));
        let mailbox: Mailbox =
            serde_json::from_value(serde_json::json!({"id": "X1", "name": "Inbox"}))
                .expect("mailbox deserializes");
        assert!(super::mailbox_has_id(&mailbox));
    }
}
