//! Webhook maintenance sessions and the store-owned renewal and recreate
//! effects behind them.
//!
//! A session belongs to one opened account and renews the rows that account
//! holds a live lease on. Rows are shared through the factory store (see
//! `super::registry`), so during a reopen the old and the new account may
//! both hold one: every renewal step therefore runs under the store's
//! exchange, re-reads the row before it sends anything, and commits before it
//! lets go, so two sessions never PATCH or recreate the same row twice and no
//! stale answer is ever applied. What a step finds out that concerns every
//! holder (a recreated row's gap, a terminal verdict) is queued to each
//! holding account's sink, and each session delivers only its own.

use std::time::Duration;

use bifrost_types::{AccountError, AccountOperation, CursorScope, ErrorScope};

use crate::account::GraphAccount;
use crate::account::graph_error::{GraphErrorContext, into_account_error};
use crate::webhooks::{
    ExpiryCheck, check_expiry, create_subscription, delete_subscription, renew_subscription,
    subscription_is_gone,
};

use super::registry::{DueRow, Exchange, InstanceEvent, Row};

pub(crate) const RENEWAL_CHECK_INTERVAL: Duration = Duration::from_secs(10 * 60);
pub(super) const RENEWAL_THRESHOLD_MINUTES: i64 = 30;

pub(crate) fn renewal_session(
    account: GraphAccount,
    stop: bifrost_types::SessionStop,
    gate: bifrost_types::PushGate,
) -> bifrost_types::AccountStream<bifrost_types::PushSessionEvent> {
    use bifrost_types::{PushSessionEnd as End, PushSessionEvent as E};
    Box::pin(async_stream::stream! {
        let Some(_lease) = account.push_lease.try_claim() else { yield E::Failed(bifrost_types::push_lane_contract_error(bifrost_types::Protocol::Graph, bifrost_types::PushLane(0), "already driven")); return; };
        let mut topology = account.webhooks.topology();
        let mut established = false;
        loop {
            // Mark the topology seen BEFORE reading events and rows: an
            // enqueue that lands after this is a change the next wait sees,
            // never one marked seen without having been read.
            topology.borrow_and_update();
            // A terminal verdict delivered here (another account's step
            // terminated a row this account shares) ends the session like one
            // found by its own pass, so the engine's lane health and pacing
            // see it, even when that row was this account's only one.
            let mut delivered_terminal = false;
            while let Some(event) = deliverable(&account) {
                delivered_terminal |= matches!(event, E::UnitFailed(_));
                yield event;
            }
            if delivered_terminal { yield E::Ended(End::Incomplete); return; }
            if !account.webhooks.has_live() {
                tokio::select! { () = stop.stopped() => break, () = account.shutdown.cancelled() => break, _ = topology.changed() => {} }
                continue;
            }
            if !established { established = true; yield E::Established; }
            let due = account
                .webhooks
                .store()
                .renewable(account.webhooks.instance(), is_due);
            // Whether any row in this pass failed non-terminally. Every row is
            // attempted once regardless (a failing row must not starve its
            // siblings); the pass then ends `Incomplete` instead of `Healthy`.
            let mut had_error = false;
            for snapshot in due {
                tokio::select! { () = stop.stopped() => { yield E::Ended(End::Stopped); return; }, () = account.shutdown.cancelled() => { yield E::Ended(End::Stopped); return; }, () = gate.clear() => {} }
                let outcome = tokio::select! {
                    biased;
                    () = account.shutdown.cancelled() => { yield E::Ended(End::Stopped); return; },
                    () = stop.stopped() => { yield E::Ended(End::Stopped); return; },
                    outcome = maintain_row(&account, snapshot) => outcome,
                };
                // A terminal verdict reaches every holder through its queue,
                // this session included, and is delivered by the drain below.
                if let RowOutcome::Failed(error) = outcome {
                    had_error = true;
                    yield E::UnitFailed(error);
                }
            }
            while let Some(event) = deliverable(&account) { yield event; }
            let retired = account.webhooks.has_terminated();
            if had_error || retired { yield E::Ended(End::Incomplete); return; }
            yield E::Healthy;
            tokio::select! { () = stop.stopped() => break, () = account.shutdown.cancelled() => break, () = tokio::time::sleep(RENEWAL_CHECK_INTERVAL) => {}, _ = topology.changed() => {} }
        }
        yield E::Ended(End::Stopped);
    })
}

/// The next queued event this session still owes the engine. A terminal
/// verdict is attributed to this account's CURRENT live scopes on the row,
/// and dropped once the row recovered or this account no longer holds it.
fn deliverable(account: &GraphAccount) -> Option<bifrost_types::PushSessionEvent> {
    use bifrost_types::PushSessionEvent as E;
    loop {
        match account.webhooks.pop_event()? {
            InstanceEvent::Gap(scope) => return Some(E::Invalidated(gap_hint(scope))),
            InstanceEvent::Terminal {
                resource,
                generation,
                error,
            } => {
                let Some(scopes) = account.webhooks.store().terminal_scopes(
                    account.webhooks.instance(),
                    &resource,
                    generation,
                ) else {
                    continue;
                };
                return Some(E::UnitFailed(attribute(error, &scopes)));
            }
        }
    }
}

/// Name what lost coverage when the row covers exactly one of this
/// account's scopes; otherwise stay account-scoped rather than pick one
/// arbitrarily.
fn attribute(error: AccountError, scopes: &[CursorScope]) -> AccountError {
    match scopes {
        [only] => error
            .clone()
            .into_builder()
            .scope(ErrorScope::Cursor(only.clone()))
            .try_build()
            .unwrap_or(error),
        _ => error,
    }
}

/// A recreated row's coverage gap: one precise scope, so it names the Graph
/// subscription source rather than the wide-wake-up `Coalesced`.
fn gap_hint(scope: CursorScope) -> bifrost_types::InvalidationHint {
    bifrost_types::InvalidationHint {
        source: bifrost_types::PushSource::GraphSubscription,
        payload: bifrost_types::HintPayload::SpecificCursorScope(scope),
    }
}

/// Whether a row is inside the renewal threshold, warning once about an
/// expiry it cannot read (renewing is the safe direction for those).
fn is_due(row: &mut Row) -> bool {
    match check_expiry(&row.expires_at, RENEWAL_THRESHOLD_MINUTES) {
        ExpiryCheck::Live => false,
        ExpiryCheck::ExpiringSoon => true,
        ExpiryCheck::Unparseable => {
            if !row.warned_unparseable_expiry {
                row.warned_unparseable_expiry = true;
                tracing::warn!(
                    subscription = row.server_id,
                    expiration = row.expires_at,
                    "[Graph webhooks] Unparseable subscription expiry; treating as due for renewal"
                );
            }
            true
        }
    }
}

/// What one maintenance step reports to the session that asked for it.
#[derive(Debug)]
pub(super) enum RowOutcome {
    /// Renewed, recreated, terminally failed (queued to every holder), or
    /// found obsolete: nothing for this session to report itself.
    Settled,
    /// A non-terminal failure worth another tick, attributed to this
    /// account's scopes.
    Failed(AccountError),
}

/// Renew one snapshotted row, or recreate it if Graph lost it.
///
/// The step runs in a task the account owns, holding the store's exchange
/// from the re-read through the commit. A session dropped mid-step only
/// drops the reply: a PATCH that succeeded still records its expiry, and a
/// create that succeeded is still installed or undone.
pub(super) async fn maintain_row(account: &GraphAccount, snapshot: DueRow) -> RowOutcome {
    let owner = account.clone();
    let (reply, response) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let store = std::sync::Arc::clone(owner.webhooks.store());
        let exchange = store.exchange().await;
        let _ = reply.send(maintain_locked(&owner, &exchange, &snapshot).await);
    });
    crate::account::push_tasks::track(account, task);
    response.await.unwrap_or_else(|_| {
        // The task panicked or the runtime is shutting down under it.
        RowOutcome::Failed(into_account_error(
            crate::error::GraphError::RuntimeFailure {
                message: "the webhook renewal task ended without an answer".to_string(),
            },
            GraphErrorContext::graph(AccountOperation::PushSubscribe),
        ))
    })
}

async fn maintain_locked(
    account: &GraphAccount,
    exchange: &Exchange,
    snapshot: &DueRow,
) -> RowOutcome {
    let store = account.webhooks.store();
    let instance = account.webhooks.instance();
    // Another session (the other account of a reopen) may have renewed,
    // recreated or terminated the row since the snapshot.
    if !store.still_due(exchange, instance, snapshot, is_due) {
        return RowOutcome::Settled;
    }
    let client = account.mailboxes.primary();
    let error = match renew_subscription(client, &snapshot.server_id, None).await {
        Ok(expires_at) => {
            store.renewed(exchange, snapshot, expires_at);
            return RowOutcome::Settled;
        }
        Err(error) => error,
    };
    // Graph retains no deleted subscription to PATCH, so a vanished (404/410)
    // row is replaced by a fresh create for the same resource; otherwise
    // every tick retries a permanent 404. The stale row stays recorded until
    // the replacement is in hand, so a failed create leaves a row that reads
    // as due rather than coverage lost until reopen.
    let (phase, error) = match account.push_config.endpoint() {
        Some(endpoint) if subscription_is_gone(&error) => {
            match recreate_locked(account, exchange, endpoint, snapshot).await {
                Ok(_) => return RowOutcome::Settled,
                // Classify the failure that actually blocks recovery: the
                // original 404 only says the old row is gone.
                Err(recreate_error) => ("recreate", recreate_error),
            }
        }
        _ => ("renew", error),
    };
    let account_error = into_account_error(
        error,
        GraphErrorContext::graph(AccountOperation::PushSubscribe),
    );
    {
        let telemetry = account_error.telemetry_fields();
        tracing::warn!(
            target: "bifrost_graph::webhooks",
            provider = ?telemetry.provider,
            protocol = ?telemetry.protocol,
            message_key = telemetry.message_key,
            recovery = telemetry.recovery_discriminant,
            server_id = %snapshot.server_id,
            resource = %snapshot.resource,
            phase = phase,
            "Graph webhook renewal failed"
        );
    }
    if account_error.recovery().is_terminal() {
        // A 404 from the replacement create is a permanent refusal for this
        // resource (its folder was deleted, say). Flagged, not removed:
        // renewing stops, but release and close must still DELETE it.
        store.terminate(exchange, snapshot, &account_error);
        return RowOutcome::Settled;
    }
    RowOutcome::Failed(attribute(
        account_error,
        &store.scopes_of(instance, &snapshot.resource),
    ))
}

/// What happened to a row a renewal step had to recreate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Replacement {
    /// The fresh subscription took the vanished one's place, and every live
    /// holder is owed a gap.
    Installed,
    /// Nobody holds the row live any more, so the fresh subscription was
    /// deleted again instead of installed.
    Unwanted,
    /// The row changed under the snapshot; another step already handled it.
    Obsolete,
}

/// Create a replacement for a vanished row and install it, under the held
/// exchange.
///
/// Admission is "the row still has a live lease", of any account: a recreate
/// started by an account that is closing still finishes for the other
/// holders, and one whose holders all left undoes its create. Never the
/// initiating account's shutdown token.
///
/// What cannot be made safe: a create whose response never arrived may have
/// succeeded on Graph and its id is unknowable (see `run_subscribe` in
/// `webhook.rs`); it lives until Graph's own expiry.
async fn recreate_locked(
    account: &GraphAccount,
    exchange: &Exchange,
    endpoint: &super::common::PushEndpoint,
    snapshot: &DueRow,
) -> Result<Replacement, crate::error::GraphError> {
    let store = account.webhooks.store();
    if !store.unchanged(exchange, snapshot) {
        return Ok(Replacement::Obsolete);
    }
    if !store.wanted(exchange, &snapshot.resource) {
        return Ok(Replacement::Unwanted);
    }
    let response = create_subscription(
        account.mailboxes.primary(),
        &snapshot.resource,
        &endpoint.webhook_url,
        &endpoint.client_state,
        None,
    )
    .await?;
    store.pass_commit_gate().await;
    if store.wanted(exchange, &snapshot.resource) {
        store.recover(
            exchange,
            &snapshot.resource,
            Some(response.id),
            response.expiration_date_time,
        );
        return Ok(Replacement::Installed);
    }
    // Every holder left while the create was in flight (a lease whose
    // account was dropped stops counting). Installing would keep
    // notifications flowing for coverage nobody wants. Best effort: there may
    // be nobody left to report a cleanup failure to.
    if let Err(cleanup_error) = delete_subscription(account.mailboxes.primary(), &response.id).await
    {
        tracing::warn!(
            target: "bifrost_graph::webhooks",
            server_id = %response.id,
            error = ?cleanup_error,
            "failed to delete a Graph webhook subscription recreated for a row nobody holds"
        );
    }
    Ok(Replacement::Unwanted)
}
