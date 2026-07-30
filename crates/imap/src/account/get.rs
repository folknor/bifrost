use std::collections::{HashMap, HashSet};

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, AccountStream,
    BatchFailure, BatchItemId, BatchSuccess, BatchUncertain, Cause, DiagnosticText, HydratedObject,
    HydratedObjectKind, ItemOutcome, PageBoundary, Projection, Protocol, RequestCause,
    RequestErrorKind, ResourceKind, SyncEvent,
};
use futures::StreamExt;

use crate::types::{FetchAttr, FetchResponse, MailboxName};

use super::inventory::{fetch_to_inventory, flags_set};
use super::{
    BATCH_ITEMS, DecodedObjectId, ImapAccount, batch, boxed_receiver_stream, decode_object_id,
    uid_set_from_u32,
};

pub(crate) fn get_stream(
    account: ImapAccount,
    mut ids: AccountStream<bifrost_types::ObjectId>,
    projection: Projection,
) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
    let (tx, rx) = tokio::sync::mpsc::channel(super::STREAM_CAPACITY);
    tokio::spawn(async move {
        let mut grouped: HashMap<String, (MailboxName, Vec<DecodedObjectId>)> = HashMap::new();
        while let Some(id) = ids.next().await {
            match decode_object_id(&id) {
                Ok(decoded) => {
                    grouped
                        .entry(decoded.folder.as_str().to_owned())
                        .or_insert_with(|| (decoded.folder.clone(), Vec::new()))
                        .1
                        .push(decoded);
                }
                Err(err) => {
                    // Locally-invalid id: surface as per-item Failed
                    // rather than a free-form Warning. Drops the id (a
                    // bifrost-shaped value that the caller will fail to
                    // hydrate elsewhere anyway) into the structured
                    // failure lane.
                    let item_id = BatchItemId(id.0.clone());
                    let outcome = ItemOutcome::Failed(BatchFailure::new(item_id, err));
                    if tx
                        .send(batch(vec![outcome], PageBoundary::Page, None))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }

        // Iterate folders in a deterministic order so cross-folder output
        // ordering is reproducible run-to-run (a `HashMap` iteration order
        // is not). Within a folder the UID FETCH order is server-driven;
        // only the folder grouping is sorted here.
        let mut groups: Vec<(MailboxName, Vec<DecodedObjectId>)> = grouped.into_values().collect();
        groups.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
        for (folder, ids) in groups {
            match run_folder_get(&account, &folder, ids.clone(), projection, &tx).await {
                Ok(()) => {}
                Err(GetError::ChannelDropped) => return,
                Err(other) => {
                    // Per-folder failure does not collapse the stream:
                    // remaining items in this folder surface as
                    // `Uncertain`, the next folder still runs. A
                    // shared-folder SELECT denial is pre-classified to
                    // `ScopeRevoked` so it quarantines that scope rather
                    // than masquerading as a generic hydration failure.
                    let account_err = match other {
                        GetError::Account(err) => err,
                        GetError::Imap(err) => super::account_error_with(
                            err,
                            super::error::ImapErrorContext::operation(
                                bifrost_types::AccountOperation::Hydrate,
                            )
                            .with_folder_scope(&folder),
                        ),
                        GetError::ChannelDropped => unreachable!("handled above"),
                    };
                    let uncertain: Vec<ItemOutcome<HydratedObject>> = ids
                        .into_iter()
                        .map(|id| {
                            let item = BatchItemId(
                                super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0,
                            );
                            ItemOutcome::Uncertain(BatchUncertain::new(item, account_err.clone()))
                        })
                        .collect();
                    let _ = tx.send(batch(uncertain, PageBoundary::Page, None)).await;
                }
            }
        }
        let _ = tx.send(SyncEvent::Done(None)).await;
    });
    boxed_receiver_stream(rx)
}

/// Per-folder hydration failure: either an IMAP wire error to classify
/// per-item, or a dropped output channel (silent return).
enum GetError {
    Imap(crate::Error),
    /// A pre-classified `AccountError` (shared-folder SELECT denial built
    /// as `ScopeRevoked` rather than letting the raw permission denial
    /// derive account-terminal `NoPermission`).
    Account(bifrost_types::AccountError),
    ChannelDropped,
}

impl From<crate::Error> for GetError {
    fn from(value: crate::Error) -> Self {
        Self::Imap(value)
    }
}

fn uidvalidity_changed_error(folder: &MailboxName) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("UIDVALIDITY changed before hydration"),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::Hydrate)
    .scope(bifrost_types::ErrorScope::Mailbox {
        id: folder.as_str().to_owned(),
    })
    .try_build()
    .expect("valid account error classification")
}

fn message_not_found_error(id: &DecodedObjectId) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::NotFound(ResourceKind::Message),
        Cause::Request(RequestCause::NotFound {
            what: ResourceKind::Message,
            id: Some(super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::Hydrate)
    .scope(bifrost_types::ErrorScope::Message {
        id: super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0,
    })
    .try_build()
    .expect("valid account error classification")
}

fn failed_hydration(
    ids: impl IntoIterator<Item = DecodedObjectId>,
    error: AccountError,
) -> Vec<ItemOutcome<HydratedObject>> {
    ids.into_iter()
        .map(|id| {
            let item = BatchItemId(super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0);
            ItemOutcome::Failed(BatchFailure::new(item, error.clone()))
        })
        .collect()
}

/// Publish the stale-UIDVALIDITY failures for a folder. Called before the
/// hydration FETCH: these outcomes are already known, and holding them until
/// after a fallible command would lose them whenever that command fails and
/// the folder collapses into the uncertain lane.
async fn emit_stale_failures(
    tx: &tokio::sync::mpsc::Sender<SyncEvent<ItemOutcome<HydratedObject>>>,
    stale: Vec<DecodedObjectId>,
    folder: &MailboxName,
) -> Result<(), GetError> {
    if stale.is_empty() {
        return Ok(());
    }
    let outcomes = failed_hydration(stale, uidvalidity_changed_error(folder));
    tx.send(batch(outcomes, PageBoundary::Page, None))
        .await
        .map_err(|_| GetError::ChannelDropped)
}

async fn run_folder_get(
    account: &ImapAccount,
    folder: &MailboxName,
    ids: Vec<DecodedObjectId>,
    projection: Projection,
    tx: &tokio::sync::mpsc::Sender<SyncEvent<ItemOutcome<HydratedObject>>>,
) -> Result<(), GetError> {
    let mut conn = account.checkout_for_folder(folder).await?;
    let folder_entry = account.folders.get(folder);
    let cursor = folder_entry.as_ref().and_then(|entry| entry.cursor());
    let shared_owner = folder_entry
        .as_ref()
        .and_then(|entry| entry.shared_owner.clone());
    let selected = match account
        .select_folder(&mut conn, folder, cursor.as_ref(), true)
        .await
    {
        Ok(selected) => selected,
        // A permission denial on a shared folder quarantines just that
        // scope (`ScopeRevoked`) instead of surfacing as a generic
        // hydration failure that could escalate account-wide; a personal
        // folder, or any non-permission failure, flows through the normal
        // mapping.
        Err(err) if shared_owner.is_some() => {
            return Err(GetError::Account(super::error::shared_folder_error(
                err,
                folder,
                shared_owner.as_ref(),
                super::error::ImapErrorContext::operation(bifrost_types::AccountOperation::Hydrate)
                    .with_folder_scope(folder),
            )));
        }
        Err(err) => return Err(err.into()),
    };
    let uidvalidity = selected
        .mailbox
        .uid_validity
        .ok_or_else(|| crate::Error::Protocol("SELECT missing UIDVALIDITY".into()))?;
    let (valid, stale): (Vec<_>, Vec<_>) = ids
        .into_iter()
        .partition(|id| id.uidvalidity == uidvalidity);
    let requested: Vec<u32> = valid.iter().map(|id| id.uid).collect();
    let requested_uids: HashSet<u32> = requested.iter().copied().collect();
    // Stale-UIDVALIDITY failures are known before any wire work, so they are
    // emitted first. Buffering them behind the FETCH would lose established
    // per-item truth if that FETCH fails and the folder falls back to the
    // uncertain lane.
    emit_stale_failures(tx, stale, folder).await?;
    let mut out: Vec<ItemOutcome<HydratedObject>> = Vec::with_capacity(BATCH_ITEMS);
    let Some(uid_set) = uid_set_from_u32(&requested) else {
        return Ok(());
    };
    let include_modseq = selected.mailbox.highest_mod_seq.is_some() && !selected.mailbox.no_mod_seq;
    let fetches = conn
        .connection()
        .uid_fetch(
            uid_set.as_sequence_set(),
            &attrs_for_projection(projection, include_modseq),
            account.command_timeout(),
        )
        .await?;
    // A server may emit several FETCH responses for one UID: the solicited
    // one plus unsolicited FLAGS updates that carry none of the requested
    // data items. Merge them per UID before conversion so a trailing partial
    // response cannot replace the complete one.
    let mut fetched_by_uid: HashMap<u32, FetchResponse> = HashMap::new();
    for fetch in fetches {
        let Some(uid) = fetch.uid else {
            continue;
        };
        if !requested_uids.contains(&uid) {
            continue;
        }
        if let Some(modseq) = fetch.mod_seq {
            account
                .folders
                .record_modseq(folder, uidvalidity, uid, modseq)?;
        }
        match fetched_by_uid.entry(uid) {
            std::collections::hash_map::Entry::Occupied(mut existing) => {
                merge_fetch_response(existing.get_mut(), fetch);
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(fetch);
            }
        }
    }
    let hydrated_by_uid: HashMap<u32, HydratedObject> = fetched_by_uid
        .into_iter()
        .filter_map(|(uid, fetch)| {
            let object = fetch_to_hydrated(
                folder,
                uidvalidity,
                fetch,
                projection,
                shared_owner.as_ref(),
            )?;
            Some((uid, object))
        })
        .collect();
    for id in valid {
        match hydrated_by_uid.get(&id.uid).cloned() {
            Some(object) => {
                let item = BatchItemId(object.id.0.clone());
                out.push(ItemOutcome::Succeeded(BatchSuccess::new(item, object)));
            }
            None => {
                let item =
                    BatchItemId(super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0);
                out.push(ItemOutcome::Failed(BatchFailure::new(
                    item,
                    message_not_found_error(&id),
                )));
            }
        }
        if out.len() >= BATCH_ITEMS {
            tx.send(batch(std::mem::take(&mut out), PageBoundary::Page, None))
                .await
                .map_err(|_| GetError::ChannelDropped)?;
        }
    }
    if !out.is_empty() {
        tx.send(batch(out, PageBoundary::Page, None))
            .await
            .map_err(|_| GetError::ChannelDropped)?;
    }
    Ok(())
}

fn attrs_for_projection(projection: Projection, include_modseq: bool) -> Vec<FetchAttr> {
    match projection {
        Projection::FlagsOnly => {
            let mut attrs = vec![FetchAttr::Uid, FetchAttr::Flags];
            if include_modseq {
                attrs.push(FetchAttr::ModSeq);
            }
            attrs
        }
        Projection::Metadata => {
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
        Projection::Headers => vec![FetchAttr::Uid, FetchAttr::Rfc822Header],
        Projection::Preview(count) => vec![
            FetchAttr::Uid,
            FetchAttr::Rfc822Header,
            FetchAttr::BodySection {
                peek: true,
                section: Some("TEXT".into()),
                partial: Some((0, count as u64)),
            },
        ],
        Projection::TextOnly => vec![
            FetchAttr::Uid,
            FetchAttr::BodySection {
                peek: true,
                section: Some("TEXT".into()),
                partial: None,
            },
        ],
        Projection::Full | Projection::FullWithBlobs => vec![
            FetchAttr::Uid,
            FetchAttr::Rfc822Size,
            FetchAttr::BodySection {
                peek: true,
                section: None,
                partial: None,
            },
        ],
        _ => vec![FetchAttr::Uid, FetchAttr::Flags],
    }
}

fn fetch_to_hydrated(
    folder: &MailboxName,
    uidvalidity: u32,
    fetch: FetchResponse,
    projection: Projection,
    shared_owner: Option<&bifrost_types::MailboxId>,
) -> Option<HydratedObject> {
    let uid = fetch.uid?;
    let id = super::encode_object_id(folder, uidvalidity, uid);
    let kind = match projection {
        Projection::FlagsOnly => {
            HydratedObjectKind::FlagsOnly(flags_set(fetch.flags.as_deref().unwrap_or(&[])))
        }
        Projection::Metadata => HydratedObjectKind::Metadata(fetch_to_inventory(
            folder,
            uidvalidity,
            fetch,
            shared_owner,
        )),
        _ => HydratedObjectKind::RawMime(bytes::Bytes::from(raw_mime_bytes(
            fetch.body_sections,
            projection,
        ))),
    };
    Some(HydratedObject {
        id,
        kind,
        blobs: Vec::new(),
    })
}

/// Fold a later FETCH response for the same UID into the one already held.
/// Data items only ever fill gaps: a second response may add `FLAGS` or a
/// section the first lacked, but it must never blank out data the first
/// response carried.
fn merge_fetch_response(existing: &mut FetchResponse, later: FetchResponse) {
    if later.flags.is_some() {
        existing.flags = later.flags;
    }
    if later.mod_seq.is_some() {
        existing.mod_seq = later.mod_seq;
    }
    if existing.envelope.is_none() {
        existing.envelope = later.envelope;
    }
    if existing.body_structure.is_none() {
        existing.body_structure = later.body_structure;
    }
    if existing.rfc822_size.is_none() {
        existing.rfc822_size = later.rfc822_size;
    }
    if existing.internal_date.is_none() {
        existing.internal_date = later.internal_date;
    }
    if existing.save_date.is_none() {
        existing.save_date = later.save_date;
    }
    for section in later.body_sections {
        if existing
            .body_sections
            .iter()
            .any(|held| held.section.eq_ignore_ascii_case(&section.section) && held.data.is_some())
        {
            continue;
        }
        existing
            .body_sections
            .retain(|held| !held.section.eq_ignore_ascii_case(&section.section));
        existing.body_sections.push(section);
    }
    for section in later.binary_sections {
        if existing
            .binary_sections
            .iter()
            .any(|held| held.section == section.section)
        {
            continue;
        }
        existing.binary_sections.push(section);
    }
}

fn raw_mime_bytes(
    sections: Vec<crate::types::fetch::BodySection>,
    projection: Projection,
) -> Vec<u8> {
    if matches!(projection, Projection::Preview(_)) {
        let mut header = None;
        let mut text = None;
        for section in sections {
            if section.section.eq_ignore_ascii_case("HEADER") {
                header = section.data;
            } else if section.section.eq_ignore_ascii_case("TEXT") {
                text = section.data;
            }
        }
        let mut bytes = header.unwrap_or_default();
        bytes.extend(text.unwrap_or_default());
        return bytes;
    }
    sections
        .into_iter()
        .find_map(|section| section.data)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{AccountErrorKind, ObjectId, RequestErrorKind};

    #[test]
    fn locally_invalid_object_id_classifies_as_request_malformed() {
        // imap-D4: get_stream emits ItemOutcome::Failed carrying a
        // structured `AccountError` for locally-invalid ids (rather
        // than dropping the failure as a free-form Warning). The
        // streaming wrapper plumbs the structured failure straight
        // through; this test pins the classification of the failure
        // that `decode_object_id` produces - if that classification
        // changes, the lane's `AccountError` changes with it.
        let bad = ObjectId("not-a-valid-imap-id".into());
        let err = super::super::decode_object_id(&bad).expect_err("invalid id should not decode");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
    }

    #[test]
    fn metadata_projection_requests_modseq_only_when_available() {
        assert!(
            attrs_for_projection(Projection::Metadata, true)
                .iter()
                .any(|attr| matches!(attr, FetchAttr::ModSeq))
        );
        assert!(
            attrs_for_projection(Projection::Metadata, false)
                .iter()
                .all(|attr| !matches!(attr, FetchAttr::ModSeq))
        );
    }

    fn folder() -> MailboxName {
        MailboxName::new("INBOX").expect("valid mailbox")
    }

    // Every projection must ask for UID: without it `fetch_to_hydrated`
    // cannot mint an ObjectId and silently drops the item.
    #[test]
    fn every_projection_requests_the_uid() {
        for projection in [
            Projection::FlagsOnly,
            Projection::Metadata,
            Projection::Headers,
            Projection::Preview(512),
            Projection::TextOnly,
            Projection::Full,
            Projection::FullWithBlobs,
        ] {
            let attrs = attrs_for_projection(projection, false);
            assert!(
                attrs.iter().any(|attr| matches!(attr, FetchAttr::Uid)),
                "{projection:?} must request UID",
            );
        }
    }

    #[test]
    fn body_bearing_projections_peek_rather_than_setting_seen() {
        // A hydration read must never flip `\Seen` as a side effect, so
        // every BODY section the read paths request is a PEEK.
        for projection in [
            Projection::Preview(64),
            Projection::TextOnly,
            Projection::Full,
            Projection::FullWithBlobs,
        ] {
            let attrs = attrs_for_projection(projection, false);
            let peeks: Vec<bool> = attrs
                .iter()
                .filter_map(|attr| match attr {
                    FetchAttr::BodySection { peek, .. } => Some(*peek),
                    _ => None,
                })
                .collect();
            assert!(!peeks.is_empty(), "{projection:?} must fetch a body");
            assert!(peeks.iter().all(|peek| *peek), "{projection:?} must PEEK");
        }
    }

    #[test]
    fn flags_only_and_metadata_never_fetch_a_body_section() {
        for projection in [Projection::FlagsOnly, Projection::Metadata] {
            assert!(
                attrs_for_projection(projection, true)
                    .iter()
                    .all(|attr| !matches!(attr, FetchAttr::BodySection { .. })),
                "{projection:?} must stay a metadata-only fetch",
            );
        }
    }

    #[test]
    fn hydrated_object_requires_a_uid() {
        let no_uid = FetchResponse {
            flags: Some(vec![crate::types::Flag::Seen]),
            ..Default::default()
        };
        assert!(
            fetch_to_hydrated(&folder(), 9, no_uid, Projection::FlagsOnly, None).is_none(),
            "a FETCH without UID cannot be addressed and must be dropped",
        );
    }

    #[test]
    fn flags_only_hydration_lowercases_the_flag_set() {
        let fetch = FetchResponse {
            uid: Some(3),
            flags: Some(vec![
                crate::types::Flag::Seen,
                crate::types::Flag::Custom("$Important".to_owned()),
            ]),
            ..Default::default()
        };
        let object = fetch_to_hydrated(&folder(), 9, fetch, Projection::FlagsOnly, None)
            .expect("uid present");
        assert_eq!(object.id, super::super::encode_object_id(&folder(), 9, 3));
        assert!(object.blobs.is_empty());
        match object.kind {
            HydratedObjectKind::FlagsOnly(flags) => {
                assert!(flags.contains("\\seen"));
                assert!(flags.contains("$important"));
            }
            other => panic!("expected FlagsOnly, got {other:?}"),
        }
    }

    #[test]
    fn metadata_hydration_reuses_the_inventory_projection_and_owner_tag() {
        let fetch = FetchResponse {
            uid: Some(3),
            rfc822_size: Some(77),
            ..Default::default()
        };
        let owner = bifrost_types::MailboxId("alice".to_owned());
        let object = fetch_to_hydrated(&folder(), 9, fetch, Projection::Metadata, Some(&owner))
            .expect("uid present");
        match object.kind {
            HydratedObjectKind::Metadata(entry) => {
                assert_eq!(entry.size, Some(77));
                assert!(
                    entry
                        .memberships
                        .contains(&bifrost_types::MembershipScope::Mailbox(owner)),
                    "the shared-mailbox membership must survive hydration",
                );
            }
            other => panic!("expected Metadata, got {other:?}"),
        }
    }

    // A raw-MIME projection with no returned section yields empty bytes
    // rather than dropping the item: the id still reaches the consumer.
    #[test]
    fn raw_mime_hydration_tolerates_a_missing_section() {
        let fetch = FetchResponse {
            uid: Some(3),
            ..Default::default()
        };
        let object =
            fetch_to_hydrated(&folder(), 9, fetch, Projection::Full, None).expect("uid present");
        match object.kind {
            HydratedObjectKind::RawMime(bytes) => assert!(bytes.is_empty()),
            other => panic!("expected RawMime, got {other:?}"),
        }
    }

    #[test]
    fn preview_hydration_combines_headers_and_preview_text() {
        let fetch = FetchResponse {
            uid: Some(3),
            body_sections: vec![
                crate::types::fetch::BodySection {
                    section: "HEADER".to_owned(),
                    origin: None,
                    data: Some(b"Subject: hi\r\n\r\n".to_vec()),
                },
                crate::types::fetch::BodySection {
                    section: "TEXT".to_owned(),
                    origin: Some(0),
                    data: Some(b"preview body".to_vec()),
                },
            ],
            ..Default::default()
        };
        let object = fetch_to_hydrated(&folder(), 9, fetch, Projection::Preview(16), None)
            .expect("uid present");
        match object.kind {
            HydratedObjectKind::RawMime(bytes) => {
                assert!(
                    bytes.as_ref().ends_with(b"Subject: hi\r\n\r\npreview body"),
                    "preview MIME must retain the requested text after its headers",
                );
            }
            other => panic!("expected RawMime, got {other:?}"),
        }
    }

    #[test]
    fn unreturned_and_stale_hydration_ids_are_failed() {
        let stale = DecodedObjectId {
            folder: folder(),
            uidvalidity: 8,
            uid: 2,
        };
        let stale = failed_hydration(vec![stale], uidvalidity_changed_error(&folder()));
        assert!(
            matches!(stale.as_slice(), [ItemOutcome::Failed(failure)] if matches!(
                failure.error.kind(),
                AccountErrorKind::Request(RequestErrorKind::Malformed)
            ))
        );

        let missing = DecodedObjectId {
            folder: folder(),
            uidvalidity: 9,
            uid: 3,
        };
        let error = message_not_found_error(&missing);
        assert!(matches!(
            error.kind(),
            AccountErrorKind::NotFound(bifrost_types::ResourceKind::Message)
        ));
    }

    fn body_section(section: &str, data: &[u8]) -> crate::types::fetch::BodySection {
        crate::types::fetch::BodySection {
            section: section.to_owned(),
            origin: None,
            data: Some(data.to_vec()),
        }
    }

    // A server is free to append an unsolicited FLAGS-only FETCH for a UID
    // it has already answered fully. Folding it in must not blank the body
    // that the solicited response carried.
    #[test]
    fn trailing_flags_update_does_not_erase_a_complete_hydration() {
        let mut complete = FetchResponse {
            uid: Some(7),
            rfc822_size: Some(120),
            body_sections: vec![body_section("", b"From: a\r\n\r\nfull body")],
            ..Default::default()
        };
        let trailing = FetchResponse {
            uid: Some(7),
            flags: Some(vec![crate::types::Flag::Seen]),
            mod_seq: Some(42),
            ..Default::default()
        };
        merge_fetch_response(&mut complete, trailing);
        assert_eq!(complete.mod_seq, Some(42));
        assert_eq!(complete.rfc822_size, Some(120));
        assert!(
            complete
                .flags
                .as_ref()
                .is_some_and(|flags| flags.contains(&crate::types::Flag::Seen)),
            "the later FLAGS update must be adopted",
        );
        let object =
            fetch_to_hydrated(&folder(), 9, complete, Projection::Full, None).expect("uid present");
        match object.kind {
            HydratedObjectKind::RawMime(bytes) => assert_eq!(
                bytes.as_ref(),
                &b"From: a\r\n\r\nfull body"[..],
                "a partial trailing FETCH must not replace the fetched body",
            ),
            other => panic!("expected RawMime, got {other:?}"),
        }
    }

    // The preview projection spans two sections; a trailing partial response
    // may add a section but must not overwrite one that already has data.
    #[test]
    fn merging_fetches_fills_gaps_without_overwriting_returned_sections() {
        let mut first = FetchResponse {
            uid: Some(7),
            body_sections: vec![body_section("HEADER", b"Subject: hi\r\n\r\n")],
            ..Default::default()
        };
        merge_fetch_response(
            &mut first,
            FetchResponse {
                uid: Some(7),
                body_sections: vec![
                    body_section("HEADER", b"Subject: WRONG\r\n\r\n"),
                    body_section("TEXT", b"preview body"),
                ],
                ..Default::default()
            },
        );
        let object = fetch_to_hydrated(&folder(), 9, first, Projection::Preview(16), None)
            .expect("uid present");
        match object.kind {
            HydratedObjectKind::RawMime(bytes) => {
                assert_eq!(bytes.as_ref(), &b"Subject: hi\r\n\r\npreview body"[..]);
            }
            other => panic!("expected RawMime, got {other:?}"),
        }
    }

    // Stale-UIDVALIDITY failures are known truth before the FETCH runs. They
    // must already be on the channel when a later step fails, otherwise the
    // folder-level error path relabels them uncertain.
    #[tokio::test]
    async fn stale_failures_reach_the_consumer_before_the_fetch_can_fail() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let stale = vec![DecodedObjectId {
            folder: folder(),
            uidvalidity: 8,
            uid: 2,
        }];
        assert!(emit_stale_failures(&tx, stale, &folder()).await.is_ok());
        // Simulate the hydration FETCH blowing up after this point.
        drop(tx);
        let event = rx.recv().await.expect("stale failures already published");
        match event {
            SyncEvent::Batch(batch) => assert!(matches!(
                batch.items.as_slice(),
                [ItemOutcome::Failed(failure)] if matches!(
                    failure.error.kind(),
                    AccountErrorKind::Request(RequestErrorKind::Malformed)
                )
            )),
            other => panic!("expected a batch, got {other:?}"),
        }
    }
}
