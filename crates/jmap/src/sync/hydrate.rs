use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountStream, Batch, BatchItemId, BatchSuccess, HydratedObject, HydratedObjectKind,
    ItemOutcome, ObjectId, PageBoundary, Projection, SyncEvent,
};
use futures::StreamExt;

use crate::email::{EmailGet, EmailId, Property};
use crate::transport_reqwest::ReqwestTransport;

use super::capabilities::CoreLimits;
use super::inventory::{email_to_inventory, inventory_properties};

type MailAccount = crate::account::Account<ReqwestTransport>;

/// Which JMAP account an incoming hydration id routes to.
///
/// `Primary` is a bare native id. `Foreign(accountId)` is an id the foreign
/// inventory / changes projection qualified with its owning account, and it
/// MUST be fetched against that account's handle: `Email/get` is
/// accountId-scoped, so running it on the primary account either 404s or
/// resolves an unrelated primary object with the same id.
///
/// An id that parses as foreign but names an account this session has no
/// handle for routes `Primary` deliberately: the account disappeared
/// (revoked delegation), and the primary `Email/get` will report the id as
/// not found - which is the honest answer for an id we can no longer reach.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum HydrationRoute {
    Primary,
    Foreign(String),
}

/// Route one hydration id. Pure over the registration predicate so the
/// selection is unit-pinnable without a live session.
pub(crate) fn route_for_id<F>(id: &ObjectId, is_registered: F) -> HydrationRoute
where
    F: Fn(&str) -> bool,
{
    match super::foreign::parse_object(&id.0) {
        Some((account, _)) if is_registered(account) => {
            HydrationRoute::Foreign(account.to_string())
        }
        _ => HydrationRoute::Primary,
    }
}

pub(crate) fn stream(
    mail: MailAccount,
    foreign_mail: Arc<HashMap<String, MailAccount>>,
    limits: CoreLimits,
    mut ids: AccountStream<ObjectId>,
    projection: Projection,
) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
    Box::pin(async_stream::stream! {
        if !matches!(projection, Projection::FlagsOnly | Projection::Metadata) {
            yield super::error::terminated_unsupported(
                bifrost_types::AccountOperation::Hydrate,
                None,
                "JMAP raw-MIME hydration projections need a MIME assembly path outside this wave",
            );
            return;
        }

        let limit = limits.max_objects_in_get.max(1);
        // One buffer per routing target: ids for the primary account and
        // ids for each foreign account cannot ride in the same
        // `Email/get`, because the call is accountId-scoped.
        let mut buffers: HashMap<HydrationRoute, Vec<ObjectId>> = HashMap::new();
        while let Some(id) = ids.next().await {
            let route = route_for_id(&id, |account| foreign_mail.contains_key(account));
            let full = {
                let buffer = buffers.entry(route.clone()).or_default();
                buffer.push(id);
                buffer.len() >= limit
            };
            if full {
                let mut drained = buffers.get_mut(&route).map(std::mem::take).unwrap_or_default();
                match fetch_route(&mail, &foreign_mail, &route, projection, &mut drained).await {
                    Ok(Some(batch)) => yield SyncEvent::Batch(batch),
                    Ok(None) => {}
                    Err(err) => {
                        yield super::error::terminated_from_jmap(
                            err,
                            super::error::JmapErrorContext::new(bifrost_types::AccountOperation::Hydrate),
                        );
                        return;
                    }
                }
            }
        }

        for (route, mut buffer) in buffers {
            if buffer.is_empty() {
                continue;
            }
            match fetch_route(&mail, &foreign_mail, &route, projection, &mut buffer).await {
                Ok(Some(batch)) => yield SyncEvent::Batch(batch),
                Ok(None) => {}
                Err(err) => {
                    yield super::error::terminated_from_jmap(
                        err,
                        super::error::JmapErrorContext::new(bifrost_types::AccountOperation::Hydrate),
                    );
                    return;
                }
            }
        }

        yield SyncEvent::Done(None);
    })
}

/// Select the account handle for a route and fetch its buffered ids. A
/// foreign route whose handle vanished mid-stream falls back to the primary
/// handle rather than dropping the ids silently (the primary `Email/get`
/// then reports them as not found).
async fn fetch_route(
    mail: &MailAccount,
    foreign_mail: &HashMap<String, MailAccount>,
    route: &HydrationRoute,
    projection: Projection,
    batch: &mut Vec<ObjectId>,
) -> crate::Result<Option<Batch<ItemOutcome<HydratedObject>>>> {
    let (handle, owner) = match route {
        HydrationRoute::Primary => (mail, None),
        HydrationRoute::Foreign(account) => match foreign_mail.get(account) {
            Some(handle) => (handle, Some(account.as_str())),
            None => (mail, None),
        },
    };
    fetch_batch(handle, projection, batch, owner).await
}

async fn fetch_batch(
    mail: &MailAccount,
    projection: Projection,
    batch: &mut Vec<ObjectId>,
    owner_account: Option<&str>,
) -> crate::Result<Option<Batch<ItemOutcome<HydratedObject>>>> {
    let started = Instant::now();
    // The wire call takes the NATIVE id: the owning account is expressed by
    // the handle we selected, not by the id string.
    let ids = batch
        .drain(..)
        .map(|id| EmailId::new(super::foreign::native_object(&id.0).to_string()))
        .collect::<Vec<_>>();
    let properties = properties_for_projection(projection);
    let response = mail
        .call(EmailGet::new().ids(ids).properties(properties))
        .await?;

    let state = response.state().to_string();
    let mut items = Vec::new();
    for email in response.into_list() {
        let native = email.id().map(ToString::to_string).unwrap_or_default();
        // Re-qualify on the way out so the outcome id is byte-identical to
        // the id the caller handed in (and to the inventory entry it came
        // from).
        let id = ObjectId(match owner_account {
            Some(account) => super::foreign::encode_object(account, &native),
            None => native,
        });
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
        // Per-item lane: every hydrated email emits
        // `ItemOutcome::Succeeded`. The streaming bulk contract is
        // "every pulled item produces exactly one outcome"; items the
        // server omitted from the response surface elsewhere (the
        // engine cross-references with the input id stream).
        let item_id = BatchItemId(id.0.clone());
        let hydrated = HydratedObject {
            id,
            kind,
            blobs: Vec::new(),
        };
        items.push(ItemOutcome::Succeeded(BatchSuccess::new(item_id, hydrated)));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreign_id_routes_to_the_foreign_account_and_strips_to_native() {
        let registered: HashSet<String> = ["acct-9".to_string()].into_iter().collect();
        let is_registered = |account: &str| registered.contains(account);

        let foreign = ObjectId(super::super::foreign::encode_object("acct-9", "M1"));
        assert_eq!(
            route_for_id(&foreign, is_registered),
            HydrationRoute::Foreign("acct-9".to_string()),
        );
        // The `Email/get` call carries the bare native id; the accountId
        // rides on the selected handle, not the id string.
        assert_eq!(super::super::foreign::native_object(&foreign.0), "M1");

        // A bare id is primary.
        assert_eq!(
            route_for_id(&ObjectId("M1".to_string()), is_registered),
            HydrationRoute::Primary,
        );
        // An id naming an account this session cannot reach falls back to
        // primary rather than fabricating a handle.
        let gone = ObjectId(super::super::foreign::encode_object("acct-gone", "M1"));
        assert_eq!(route_for_id(&gone, is_registered), HydrationRoute::Primary);
    }
}
