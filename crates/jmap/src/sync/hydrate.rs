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
    let handle = match route {
        HydrationRoute::Primary => mail,
        HydrationRoute::Foreign(account) => foreign_mail.get(account).unwrap_or(mail),
    };
    fetch_batch(handle, projection, batch).await
}

/// Fetch one route's buffered ids and reconcile the answer against them.
///
/// Outcome ids are the ids the CALLER submitted, verbatim - the foreign
/// qualification already rides on them, so nothing is re-encoded from the
/// native id the server echoed back.
async fn fetch_batch(
    mail: &MailAccount,
    projection: Projection,
    batch: &mut Vec<ObjectId>,
) -> crate::Result<Option<Batch<ItemOutcome<HydratedObject>>>> {
    let started = Instant::now();
    let requested: Vec<ObjectId> = std::mem::take(batch);
    if requested.is_empty() {
        return Ok(None);
    }
    // The wire call takes the NATIVE id: the owning account is expressed by
    // the handle we selected, not by the id string.
    let ids = requested
        .iter()
        .map(|id| EmailId::new(super::foreign::native_object(&id.0).to_string()))
        .collect::<Vec<_>>();
    let properties = properties_for_projection(projection);
    let response = mail
        .call(EmailGet::new().ids(ids).properties(properties))
        .await?;

    let state = response.state().to_string();
    let not_found = response.not_found().to_vec();
    let items = reconcile_hydration(
        &requested,
        response.into_list(),
        &not_found,
        projection,
        &state,
    );

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

/// Reconcile one `Email/get` answer against the ids that were submitted.
///
/// Pure, so the accounting is unit-pinnable without a live transport
/// (the sync layer hardwires `ReqwestTransport`).
///
/// The closed per-item contract is "every submitted id leaves on exactly
/// one lane", and neither half of the response can carry that alone:
///
/// - `notFound` decodes as empty when the server omits it (the decoder is
///   deliberately lenient so one absent empty array cannot fail every
///   sibling call in the request), and a present `notFound` can still miss
///   an id the server also left out of `list`.
/// - `list` can echo an id that was never requested, or echo one twice.
///
/// So ids are matched from the SUBMITTED side. Anything the server sent
/// that does not correlate is discarded rather than minted into an
/// outcome, and any submitted id left over becomes a retryable
/// `Protocol(PartialResponse)` - not a terminal contract violation, which
/// would drop the id permanently for a condition the next `Email/get`
/// usually clears.
fn reconcile_hydration(
    requested: &[ObjectId],
    list: Vec<crate::email::Email>,
    not_found: &[EmailId],
    projection: Projection,
    state: &str,
) -> Vec<ItemOutcome<HydratedObject>> {
    let by_native: HashMap<&str, &ObjectId> = requested
        .iter()
        .map(|id| (super::foreign::native_object(&id.0), id))
        .collect();
    let mut answered: HashSet<&str> = HashSet::new();
    let mut items = Vec::new();

    for email in list {
        let Some(native) = email.id().map(ToString::to_string) else {
            // An object with no `id` cannot be correlated with anything
            // that was asked for. Dropping it leaves its requested id in
            // the unanswered sweep below.
            continue;
        };
        // Answer the id the CALLER submitted, not a re-encoding of the id
        // the server echoed: an unrequested or duplicated id would
        // otherwise mint an outcome for something never asked for while
        // the real id stayed silent.
        let Some((native, id)) = by_native.get_key_value(native.as_str()) else {
            continue;
        };
        if !answered.insert(native) {
            continue;
        }
        let id = (*id).clone();
        let kind = match projection {
            Projection::FlagsOnly => HydratedObjectKind::FlagsOnly(
                email
                    .keywords()
                    .into_iter()
                    .map(str::to_string)
                    .collect::<HashSet<_>>(),
            ),
            Projection::Metadata => HydratedObjectKind::Metadata(email_to_inventory(email, state)),
            _ => HydratedObjectKind::FlagsOnly(HashSet::new()),
        };
        let item_id = BatchItemId(id.0.clone());
        let hydrated = HydratedObject {
            id,
            kind,
            blobs: Vec::new(),
        };
        items.push(ItemOutcome::Succeeded(BatchSuccess::new(item_id, hydrated)));
    }

    for missing in not_found {
        let native = missing.to_string();
        let Some((native, id)) = by_native.get_key_value(native.as_str()) else {
            continue;
        };
        if !answered.insert(native) {
            continue;
        }
        items.push(ItemOutcome::Failed(bifrost_types::BatchFailure::new(
            BatchItemId(id.0.clone()),
            super::error::get_id_not_found(
                id.0.clone(),
                super::error::JmapErrorContext::message(
                    bifrost_types::AccountOperation::Hydrate,
                    id.0.clone(),
                ),
            ),
        )));
    }

    for id in requested {
        let native = super::foreign::native_object(&id.0);
        if !answered.insert(native) {
            continue;
        }
        items.push(ItemOutcome::Failed(bifrost_types::BatchFailure::new(
            BatchItemId(id.0.clone()),
            super::error::get_id_unanswered(
                &id.0,
                super::error::JmapErrorContext::message(
                    bifrost_types::AccountOperation::Hydrate,
                    id.0.clone(),
                ),
            ),
        )));
    }

    items
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

    #[test]
    fn each_projection_requests_the_properties_it_actually_reads() {
        // `FlagsOnly` builds its outcome from `keywords`, so dropping that
        // property would silently hydrate every message with an empty flag
        // set instead of failing. `Metadata` must ask for exactly the
        // inventory property set, or a hydrated entry and the inventory
        // entry it replaces would carry different fingerprints.
        let flags_only = properties_for_projection(Projection::FlagsOnly);
        assert!(flags_only.contains(&Property::Keywords));
        assert!(flags_only.contains(&Property::Id));

        assert_eq!(
            properties_for_projection(Projection::Metadata),
            inventory_properties()
        );
    }

    #[test]
    fn a_foreign_route_and_a_primary_route_never_share_a_batch_buffer() {
        // `stream` keys its per-request buffers on `HydrationRoute`, so the
        // routes must not compare equal - one `Email/get` is scoped to one
        // accountId and cannot carry ids from two accounts.
        assert_ne!(
            HydrationRoute::Primary,
            HydrationRoute::Foreign("acct-9".to_string())
        );
        assert_ne!(
            HydrationRoute::Foreign("acct-9".to_string()),
            HydrationRoute::Foreign("acct-7".to_string())
        );
    }

    fn email(id: &str) -> crate::email::Email {
        serde_json::from_value(serde_json::json!({ "id": id, "keywords": { "$seen": true } }))
            .expect("email fixture decodes")
    }

    fn outcome_id(outcome: &ItemOutcome<HydratedObject>) -> &str {
        match outcome {
            ItemOutcome::Succeeded(success) => &success.item.0,
            ItemOutcome::Failed(failure) => &failure.item.0,
            ItemOutcome::Uncertain(uncertain) => &uncertain.item.0,
        }
    }

    fn reported_ids(outcomes: &[ItemOutcome<HydratedObject>]) -> Vec<&str> {
        let mut ids: Vec<&str> = outcomes.iter().map(outcome_id).collect();
        ids.sort_unstable();
        ids
    }

    /// The closed per-item contract: an id the server answered in NEITHER
    /// `list` nor `notFound` still has to leave on a lane. `notFound`
    /// decodes as empty when a server omits it, so relying on it alone
    /// makes the id evaporate - the engine's read-back then sees a
    /// hydration that reported nothing at all for a target it submitted.
    ///
    /// The class matters as much as the presence: `Protocol(PartialResponse)`
    /// retries (the next `Email/get` normally answers), where a terminal
    /// contract violation would drop the id for good.
    #[test]
    fn an_id_answered_in_neither_list_nor_not_found_becomes_a_retryable_partial_response() {
        let requested = vec![
            ObjectId("m0".to_string()),
            ObjectId("m1".to_string()),
            ObjectId("m2".to_string()),
        ];
        // `m1` came back; `m2` was declared missing; `m0` was simply
        // dropped, with an EMPTY `notFound` - exactly what an omitted
        // `notFound` decodes to.
        let outcomes = reconcile_hydration(
            &requested,
            vec![email("m1")],
            &[EmailId::new("m2")],
            Projection::FlagsOnly,
            "s1",
        );

        assert_eq!(reported_ids(&outcomes), vec!["m0", "m1", "m2"]);

        let m0 = outcomes
            .iter()
            .find(|outcome| outcome_id(outcome) == "m0")
            .expect("m0 is accounted for");
        let ItemOutcome::Failed(failure) = m0 else {
            panic!("an unanswered id is a failure, got {m0:?}");
        };
        assert!(matches!(
            failure.error.kind(),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::PartialResponse
            )
        ));
        assert!(failure.error.recovery().is_retryable());
        assert!(!failure.error.recovery().is_terminal());
        // The method response itself arrived; only this id's answer did
        // not. Defaulting to `Unsent` would tell the consumer nothing
        // crossed the wire.
        assert_eq!(
            failure.error.telemetry_fields().transmission_state,
            Some(bifrost_types::TransmissionState::Acknowledged)
        );

        let m2 = outcomes
            .iter()
            .find(|outcome| outcome_id(outcome) == "m2")
            .expect("m2 is accounted for");
        let ItemOutcome::Failed(failure) = m2 else {
            panic!("a notFound id is a failure, got {m2:?}");
        };
        // A declared `notFound` is a different fact from an unanswered
        // id: the object is gone, so retrying it forever is wrong.
        assert!(matches!(
            failure.error.kind(),
            bifrost_types::AccountErrorKind::NotFound(bifrost_types::ResourceKind::Message)
        ));

        assert!(matches!(
            outcomes
                .iter()
                .find(|outcome| outcome_id(outcome) == "m1")
                .expect("m1 is accounted for"),
            ItemOutcome::Succeeded(_)
        ));
    }

    /// Outcomes are keyed by the id the CALLER submitted. A foreign id is
    /// qualified with its owning account and goes on the wire stripped to
    /// its native form, so transcribing the echoed id would answer under
    /// an id the caller never handed in.
    #[test]
    fn a_foreign_id_is_answered_under_the_qualified_id_the_caller_submitted() {
        let qualified = ObjectId(super::super::foreign::encode_object("acct-9", "M1"));
        let requested = vec![qualified.clone()];
        let outcomes = reconcile_hydration(
            &requested,
            vec![email("M1")],
            &[],
            Projection::FlagsOnly,
            "s1",
        );

        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcome_id(&outcomes[0]), qualified.0);
        let ItemOutcome::Succeeded(success) = &outcomes[0] else {
            panic!("the foreign id hydrated");
        };
        assert_eq!(success.output.id, qualified);
    }

    /// A response object that does not correlate with a submitted id is
    /// DISCARDED rather than minted into an outcome: emitting it would
    /// hand the consumer an id it never asked for while the id it did ask
    /// for stayed silent. Same for a repeated answer, which would put two
    /// outcomes on one id's lane.
    #[test]
    fn unrequested_repeated_and_id_less_response_objects_are_discarded() {
        let requested = vec![ObjectId("m0".to_string()), ObjectId("m1".to_string())];
        let id_less: crate::email::Email =
            serde_json::from_value(serde_json::json!({ "keywords": {} })).expect("decodes");
        let outcomes = reconcile_hydration(
            &requested,
            vec![email("m0"), email("m0"), email("stranger"), id_less],
            // A `notFound` naming an id that was never requested is
            // discarded on the same grounds.
            &[EmailId::new("also-a-stranger")],
            Projection::FlagsOnly,
            "s1",
        );

        assert_eq!(reported_ids(&outcomes), vec!["m0", "m1"]);
        assert!(matches!(
            outcomes
                .iter()
                .find(|outcome| outcome_id(outcome) == "m0")
                .expect("m0 is accounted for"),
            ItemOutcome::Succeeded(_)
        ));
        // `m1` was never answered at all, so it rides the partial lane.
        assert!(matches!(
            outcomes
                .iter()
                .find(|outcome| outcome_id(outcome) == "m1")
                .expect("m1 is accounted for"),
            ItemOutcome::Failed(_)
        ));
    }
}
