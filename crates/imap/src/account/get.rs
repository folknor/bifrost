use std::collections::HashMap;

use bifrost_types::{
    AccountStream, HydratedObject, HydratedObjectKind, PageBoundary, Projection, SyncEvent,
};
use futures::StreamExt;

use crate::types::{FetchAttr, FetchResponse, MailboxName};

use super::inventory::{fetch_to_inventory, flags_set};
use super::{
    BATCH_ITEMS, DecodedObjectId, ImapAccount, batch, boxed_receiver_stream, decode_object_id,
    fatal_event, uid_set_from_u32,
};

pub(crate) fn get_stream(
    account: ImapAccount,
    mut ids: AccountStream<bifrost_types::ObjectId>,
    projection: Projection,
) -> AccountStream<SyncEvent<HydratedObject>> {
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
                    let _ = tx
                        .send(SyncEvent::Warning(bifrost_types::Warning {
                            kind: bifrost_types::WarningKind::Other(
                                "invalid_imap_object_id".into(),
                            ),
                            message: err.to_string(),
                            retry_count: 0,
                            next_action: None,
                            protocol_detail: None,
                        }))
                        .await;
                }
            }
        }

        for (_key, (folder, ids)) in grouped {
            if let Err(err) = run_folder_get(&account, &folder, ids, projection, &tx).await {
                let _ = tx
                    .send(fatal_event(
                        err,
                        super::error::ImapErrorContext::operation(
                            bifrost_types::AccountOperation::Hydrate,
                        ),
                    ))
                    .await;
                return;
            }
        }
        let _ = tx.send(SyncEvent::Done(None)).await;
    });
    boxed_receiver_stream(rx)
}

async fn run_folder_get(
    account: &ImapAccount,
    folder: &MailboxName,
    ids: Vec<DecodedObjectId>,
    projection: Projection,
    tx: &tokio::sync::mpsc::Sender<SyncEvent<HydratedObject>>,
) -> Result<(), crate::Error> {
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
    let mut out = Vec::with_capacity(BATCH_ITEMS);
    for fetch in fetches {
        if let (Some(uid), Some(modseq)) = (fetch.uid, fetch.mod_seq) {
            account
                .folders
                .record_modseq(folder, uidvalidity, uid, modseq)?;
        }
        if let Some(object) = fetch_to_hydrated(folder, uidvalidity, fetch, projection) {
            out.push(object);
            if out.len() >= BATCH_ITEMS {
                tx.send(batch(std::mem::take(&mut out), PageBoundary::Page, None))
                    .await
                    .map_err(|_| crate::Error::closed())?;
            }
        }
    }
    if !out.is_empty() {
        tx.send(batch(out, PageBoundary::Page, None))
            .await
            .map_err(|_| crate::Error::closed())?;
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
