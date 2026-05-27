use std::collections::HashMap;

use bifrost_types::{
    AccountStream, BatchFailure, BatchItemId, BatchSuccess, BatchUncertain, HydratedObject,
    HydratedObjectKind, ItemOutcome, PageBoundary, Projection, SyncEvent,
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

        for (_key, (folder, ids)) in grouped {
            match run_folder_get(&account, &folder, ids.clone(), projection, &tx).await {
                Ok(()) => {}
                Err(GetError::ChannelDropped) => return,
                Err(GetError::Imap(err)) => {
                    // Per-folder failure does not collapse the stream:
                    // remaining items in this folder surface as
                    // `Uncertain`, the next folder still runs.
                    let account_err = super::account_error_with(
                        err,
                        super::error::ImapErrorContext::operation(
                            bifrost_types::AccountOperation::Hydrate,
                        )
                        .with_mailbox(&folder),
                    );
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
    ChannelDropped,
}

impl From<crate::Error> for GetError {
    fn from(value: crate::Error) -> Self {
        Self::Imap(value)
    }
}

async fn run_folder_get(
    account: &ImapAccount,
    folder: &MailboxName,
    ids: Vec<DecodedObjectId>,
    projection: Projection,
    tx: &tokio::sync::mpsc::Sender<SyncEvent<ItemOutcome<HydratedObject>>>,
) -> Result<(), GetError> {
    let mut conn = account.checkout_for_folder(folder).await?;
    let cursor = account.folders.get(folder).and_then(|entry| entry.cursor());
    let selected = account
        .select_folder(&mut conn, folder, cursor.as_ref(), true)
        .await?;
    let uidvalidity = selected
        .mailbox
        .uid_validity
        .ok_or_else(|| crate::Error::Protocol("SELECT missing UIDVALIDITY".into()))?;
    let valid: Vec<u32> = ids
        .into_iter()
        .filter(|id| id.uidvalidity == uidvalidity)
        .map(|id| id.uid)
        .collect();
    let Some(uid_set) = uid_set_from_u32(&valid) else {
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
    let mut out: Vec<ItemOutcome<HydratedObject>> = Vec::with_capacity(BATCH_ITEMS);
    for fetch in fetches {
        if let (Some(uid), Some(modseq)) = (fetch.uid, fetch.mod_seq) {
            account
                .folders
                .record_modseq(folder, uidvalidity, uid, modseq)?;
        }
        if let Some(object) = fetch_to_hydrated(folder, uidvalidity, fetch, projection) {
            let item = BatchItemId(object.id.0.clone());
            out.push(ItemOutcome::Succeeded(BatchSuccess::new(item, object)));
            if out.len() >= BATCH_ITEMS {
                tx.send(batch(std::mem::take(&mut out), PageBoundary::Page, None))
                    .await
                    .map_err(|_| GetError::ChannelDropped)?;
            }
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
) -> Option<HydratedObject> {
    let uid = fetch.uid?;
    let id = super::encode_object_id(folder, uidvalidity, uid);
    let kind = match projection {
        Projection::FlagsOnly => {
            HydratedObjectKind::FlagsOnly(flags_set(fetch.flags.as_deref().unwrap_or(&[])))
        }
        Projection::Metadata => {
            HydratedObjectKind::Metadata(fetch_to_inventory(folder, uidvalidity, fetch))
        }
        _ => {
            let bytes = fetch
                .body_sections
                .into_iter()
                .find_map(|section| section.data)
                .unwrap_or_default();
            HydratedObjectKind::RawMime(bytes::Bytes::from(bytes))
        }
    };
    Some(HydratedObject {
        id,
        kind,
        blobs: Vec::new(),
    })
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
}
