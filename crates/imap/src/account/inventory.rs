use std::collections::HashSet;

use bifrost_types::{
    AccountError, AccountFuture, AccountStream, Checkpoint, CursorEstablishment, CursorScope,
    DiagnosticText, Fingerprint, InventoryEntry, PageBoundary, ServerVersion, SyncEvent,
    SyncStrategy, ThreadId, Warning, WarningKind,
};

use crate::types::{FetchAttr, FetchResponse, MailboxName, UidSet};

use super::{
    BATCH_ITEMS, CompactUidSet, ImapAccount, ScopeHandler, batch, boxed_receiver_stream,
    encode_cursor, encode_object_id, fatal_event, folder_from_scope, folder_scope,
    membership_scope, route_scope,
};

pub(crate) fn establish_initial_cursor(
    account: ImapAccount,
    scope: CursorScope,
) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
    Box::pin(async move {
        match route_scope(
            &account,
            &scope,
            bifrost_types::AccountOperation::EstablishCursor,
        )? {
            ScopeHandler::Folder(_) => Ok(CursorEstablishment::EstablishViaInventory),
            // A composed sub-account owns this typed scope: defer the
            // whole cursor-establishment decision to it.
            ScopeHandler::Delegate(sub) => sub.establish_initial_cursor(scope).await,
        }
    })
}

pub(crate) fn inventory_stream(
    account: ImapAccount,
    scope: CursorScope,
) -> AccountStream<SyncEvent<InventoryEntry>> {
    // Route first: a typed scope owned by a sub-account delegates the
    // whole inventory stream rather than checking out an IMAP folder.
    match route_scope(
        &account,
        &scope,
        bifrost_types::AccountOperation::SyncInventory,
    ) {
        Ok(ScopeHandler::Delegate(sub)) => return sub.inventory_stream(scope),
        Ok(ScopeHandler::Folder(_)) => {}
        Err(error) => {
            return Box::pin(futures::stream::iter([SyncEvent::Terminated(error)]));
        }
    }
    let (tx, rx) = tokio::sync::mpsc::channel(super::STREAM_CAPACITY);
    tokio::spawn(async move {
        match run_inventory(account, scope.clone(), tx.clone()).await {
            Ok(()) | Err(InventoryError::ChannelDropped) => {}
            Err(InventoryError::Imap(err)) => {
                let _ = tx
                    .send(fatal_event(
                        err,
                        super::error::ImapErrorContext::operation(
                            bifrost_types::AccountOperation::SyncInventory,
                        )
                        .with_cursor_scope(scope),
                    ))
                    .await;
            }
            Err(InventoryError::Account(err)) => {
                let _ = tx
                    .send(super::terminated_event::<InventoryEntry, _>(err))
                    .await;
            }
        }
    });
    boxed_receiver_stream(rx)
}

/// Marker for an output-channel-dropped send failure. Per the IMAP plan,
/// a dropped consumer is not an error to escalate: the streaming task
/// just returns. We model it as a separate variant so the spawn handler
/// can distinguish "consumer left" (silent return) from "wire failed"
/// (`fatal_event`).
struct ChannelDropped;

enum InventoryError {
    Imap(crate::Error),
    /// A pre-classified `AccountError` (the shared-folder revocation path
    /// builds `ScopeRevoked` rather than letting the raw permission denial
    /// derive terminal `NoPermission`).
    Account(bifrost_types::AccountError),
    ChannelDropped,
}

impl From<crate::Error> for InventoryError {
    fn from(value: crate::Error) -> Self {
        Self::Imap(value)
    }
}

impl From<ChannelDropped> for InventoryError {
    fn from(_: ChannelDropped) -> Self {
        Self::ChannelDropped
    }
}

async fn run_inventory(
    account: ImapAccount,
    scope: CursorScope,
    tx: tokio::sync::mpsc::Sender<SyncEvent<InventoryEntry>>,
) -> Result<(), InventoryError> {
    if let Some(reason) = account.take_qresync_negotiation_warning() {
        tx.send(SyncEvent::Warning(
            Warning::support_only(WarningKind::StrategyDowngraded, reason).with_protocol_detail(
                DiagnosticText::support_only(format!(
                    "{:?}->{:?}",
                    SyncStrategy::QResync,
                    SyncStrategy::Condstore
                )),
            ),
        ))
        .await
        .map_err(|_| ChannelDropped)?;
    }
    let folder = folder_from_scope(&scope, bifrost_types::AccountOperation::SyncInventory)
        .map_err(|e| crate::Error::Protocol(e.to_string()))?;
    let shared_owner = account
        .folders
        .get(&folder)
        .and_then(|entry| entry.shared_owner.clone());
    let mut conn = account.checkout_for_folder(&folder).await?;
    let selected = match account.select_folder(&mut conn, &folder, None, true).await {
        Ok(selected) => selected,
        // A permission denial on a shared folder quarantines just that
        // scope (`ScopeRevoked` -> `DisableScope`) instead of escalating
        // account-wide. A personal folder, or any non-permission failure,
        // flows through the normal mapping.
        Err(err) if shared_owner.is_some() => {
            return Err(InventoryError::Account(super::error::shared_folder_error(
                err,
                &folder,
                shared_owner.as_ref(),
                super::error::ImapErrorContext::operation(
                    bifrost_types::AccountOperation::SyncInventory,
                )
                .with_folder_scope(&folder),
            )));
        }
        Err(err) => return Err(err.into()),
    };
    let uidvalidity = selected
        .mailbox
        .uid_validity
        .ok_or_else(|| crate::Error::Protocol("SELECT missing UIDVALIDITY".into()))?;
    let include_modseq = selected.mailbox.highest_mod_seq.is_some() && !selected.mailbox.no_mod_seq;
    let attrs = inventory_attrs(include_modseq);
    let all_uids = UidSet::all();
    let (mut fetch_rx, fetch_fut) = conn.connection().uid_fetch_stream(
        all_uids.as_sequence_set(),
        &attrs,
        account.command_timeout(),
    )?;
    tokio::pin!(fetch_fut);

    let mut batch_items = Vec::with_capacity(BATCH_ITEMS);
    let mut known = Vec::new();
    let mut fetch_result = None;

    loop {
        tokio::select! {
            result = &mut fetch_fut, if fetch_result.is_none() => {
                fetch_result = Some(result);
            }
            item = fetch_rx.recv() => {
                match item {
                    Some(Ok(fetch)) => {
                        if let Some(uid) = fetch.uid {
                            if let Some(modseq) = fetch.mod_seq {
                                account
                                    .folders
                                    .record_modseq(&folder, uidvalidity, uid, modseq)?;
                            }
                            known.push(uid);
                            batch_items.push(fetch_to_inventory(&folder, uidvalidity, fetch, shared_owner.as_ref()));
                            if batch_items.len() >= BATCH_ITEMS {
                                let out = std::mem::take(&mut batch_items);
                                tx.send(batch(out, PageBoundary::Page, None)).await.map_err(|_| ChannelDropped)?;
                            }
                        }
                    }
                    Some(Err(err)) => return Err(err.into()),
                    None => {
                        if let Some(result) = fetch_result.take() {
                            result?;
                        }
                        break;
                    }
                }
            }
        }
    }

    let known_uids = CompactUidSet::from_uids(known);
    let cursor = account.cursor_from_select(&selected.mailbox, Some(known_uids))?;
    account.folders.set_cursor(&folder, cursor.clone());
    let checkpoint = Some(Checkpoint::Change(encode_cursor(
        folder_scope(&folder),
        &cursor,
    )));
    if batch_items.is_empty() {
        tx.send(SyncEvent::Done(checkpoint))
            .await
            .map_err(|_| ChannelDropped)?;
    } else {
        tx.send(batch(batch_items, PageBoundary::Final, checkpoint.clone()))
            .await
            .map_err(|_| ChannelDropped)?;
        tx.send(SyncEvent::Done(checkpoint))
            .await
            .map_err(|_| ChannelDropped)?;
    }
    Ok(())
}

pub(crate) fn inventory_attrs(include_modseq: bool) -> Vec<FetchAttr> {
    let mut attrs = vec![
        FetchAttr::Uid,
        FetchAttr::Flags,
        FetchAttr::Envelope,
        FetchAttr::Rfc822Size,
    ];
    if include_modseq {
        attrs.push(FetchAttr::ModSeq);
    }
    attrs
}

pub(crate) fn fetch_to_inventory(
    folder: &MailboxName,
    uidvalidity: u32,
    fetch: FetchResponse,
    shared_owner: Option<&bifrost_types::MailboxId>,
) -> InventoryEntry {
    let flags_hash = flags_hash(fetch.flags.as_deref().unwrap_or(&[]));
    let envelope = fetch.envelope;
    // A shared/other-user folder's inventory item carries both its
    // `Folder` membership and its owning `Mailbox(owner)` membership so
    // the consumer can map the item to a shared mailbox identity (A5c).
    let mut memberships = vec![membership_scope(folder)];
    if let Some(owner) = shared_owner {
        memberships.push(bifrost_types::MembershipScope::Mailbox(owner.clone()));
    }
    InventoryEntry {
        id: encode_object_id(folder, uidvalidity, fetch.uid.unwrap_or_default()),
        memberships,
        size: fetch.rfc822_size,
        blob_id: None,
        fingerprint: Fingerprint {
            server_version: fetch
                .mod_seq
                .map(ServerVersion::ModSeq)
                .unwrap_or(ServerVersion::Unavailable),
            size: fetch.rfc822_size,
            flags_hash,
        },
        thread_id: fetch
            .thread_id
            .or_else(|| fetch.gmail_thread_id.map(|id| id.to_string()))
            .map(ThreadId),
        message_id: envelope
            .as_ref()
            .and_then(|env| env.bare_message_id().map(str::to_owned)),
        references: Vec::new(),
        in_reply_to: envelope
            .as_ref()
            .and_then(|env| env.first_in_reply_to().map(str::to_owned)),
    }
}

pub(crate) fn flags_hash(flags: &[crate::types::Flag]) -> u64 {
    let mut canonical: Vec<String> = flags
        .iter()
        .map(|flag| flag.as_imap_str().to_ascii_lowercase())
        .collect();
    canonical.sort_unstable();
    canonical.dedup();

    let mut hash = 0xcbf29ce484222325u64;
    for byte in canonical.join("\n").as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub(crate) fn flags_set(flags: &[crate::types::Flag]) -> HashSet<String> {
    flags
        .iter()
        .map(|flag| flag.as_imap_str().to_ascii_lowercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_attrs_request_modseq_only_when_available() {
        assert!(
            inventory_attrs(true)
                .iter()
                .any(|attr| matches!(attr, FetchAttr::ModSeq))
        );
        assert!(
            inventory_attrs(false)
                .iter()
                .all(|attr| !matches!(attr, FetchAttr::ModSeq))
        );
    }
}
