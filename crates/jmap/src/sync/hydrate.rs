use std::collections::HashSet;
use std::time::Instant;

use bifrost_types::{
    AccountStream, Batch, HydratedObject, HydratedObjectKind, ObjectId, PageBoundary, Projection,
    SyncEvent,
};
use futures_util::StreamExt;

use crate::email::{EmailGet, EmailId, Property};
use crate::transport_reqwest::ReqwestTransport;

use super::capabilities::CoreLimits;
use super::inventory::{email_to_inventory, inventory_properties};

type MailAccount = crate::account::Account<ReqwestTransport>;

pub(crate) fn stream(
    mail: MailAccount,
    limits: CoreLimits,
    mut ids: AccountStream<ObjectId>,
    projection: Projection,
) -> AccountStream<SyncEvent<HydratedObject>> {
    Box::pin(async_stream::stream! {
        if !matches!(projection, Projection::FlagsOnly | Projection::Metadata) {
            yield super::error::fatal_unsupported(
                "JMAP raw-MIME hydration projections need a MIME assembly path outside this wave",
            );
            return;
        }

        let mut batch = Vec::with_capacity(limits.max_objects_in_get.max(1));
        while let Some(id) = ids.next().await {
            batch.push(id);
            if batch.len() >= limits.max_objects_in_get.max(1) {
                match fetch_batch(&mail, projection, &mut batch).await {
                    Ok(Some(batch)) => yield SyncEvent::Batch(batch),
                    Ok(None) => {}
                    Err(err) => {
                        yield super::error::fatal_from_jmap(err, None);
                        return;
                    }
                }
            }
        }

        if !batch.is_empty() {
            match fetch_batch(&mail, projection, &mut batch).await {
                Ok(Some(batch)) => yield SyncEvent::Batch(batch),
                Ok(None) => {}
                Err(err) => {
                    yield super::error::fatal_from_jmap(err, None);
                    return;
                }
            }
        }

        yield SyncEvent::Done(None);
    })
}

async fn fetch_batch(
    mail: &MailAccount,
    projection: Projection,
    batch: &mut Vec<ObjectId>,
) -> crate::Result<Option<Batch<HydratedObject>>> {
    let started = Instant::now();
    let ids = batch
        .drain(..)
        .map(|id| EmailId::new(id.0))
        .collect::<Vec<_>>();
    let properties = properties_for_projection(projection);
    let response = mail
        .call(EmailGet::new().ids(ids).properties(properties))
        .await?;

    let state = response.state().to_string();
    let mut items = Vec::new();
    for email in response.into_list() {
        let id = ObjectId(email.id().map(ToString::to_string).unwrap_or_default());
        let kind = match projection {
            Projection::FlagsOnly => HydratedObjectKind::FlagsOnly(
                email
                    .keywords()
                    .into_iter()
                    .map(str::to_string)
                    .collect::<HashSet<_>>(),
            ),
            Projection::Metadata => HydratedObjectKind::Metadata(email_to_inventory(email, &state)),
            _ => HydratedObjectKind::FlagsOnly(HashSet::new()),
        };
        items.push(HydratedObject {
            id,
            kind,
            blobs: Vec::new(),
        });
    }

    if items.is_empty() {
        return Ok(None);
    }

    Ok(Some(Batch {
        items,
        page_boundary: PageBoundary::Page,
        server_latency: started.elapsed(),
        bytes_in: 0,
        checkpoint: None,
    }))
}

fn properties_for_projection(projection: Projection) -> Vec<Property> {
    match projection {
        Projection::FlagsOnly => vec![Property::Id, Property::Keywords, Property::MailboxIds],
        Projection::Metadata => inventory_properties(),
        _ => vec![Property::Id],
    }
}
