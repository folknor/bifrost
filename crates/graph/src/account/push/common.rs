//! The pieces both push arms and the dispatcher share: the webhook endpoint
//! configuration, the two `Unsupported(PushSubscribe)` refusals, and the
//! subscription handle mint.

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, CursorScope,
    ErrorScope, Protocol, Provider, RequestCause, SubscriptionHandle,
};

use crate::account::graph_error::{GraphErrorContext, into_account_error};

#[derive(Debug, Clone)]
pub(crate) struct PushEndpoint {
    pub(crate) webhook_url: String,
    /// A consumer-owned account-wide secret carried in every Graph webhook
    /// subscription so its out-of-process receiver can validate clientState.
    ///
    /// Mandatory. The alternative was a per-resource random value minted
    /// inside `create_subscription` and dropped on the floor, which produced
    /// subscriptions no receiver could authenticate - a secret nobody holds
    /// is not a secret, it is an unvalidated webhook with a field filled in.
    pub(crate) client_state: String,
}

pub(super) fn unsupported_push_error() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(AccountOperation::PushSubscribe),
        Cause::Request(RequestCause::Unsupported {
            operation: AccountOperation::PushSubscribe,
        }),
    )
    .operation(AccountOperation::PushSubscribe)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .try_build()
    .expect("valid account error classification")
}

/// The same refusal, correlated to the scope that caused it. A per-scope lane
/// entry the caller cannot attribute is not a report it can act on: it would
/// know a scope was declined without knowing which registration to drop.
pub(super) fn unsupported_push_scope_error(scope: CursorScope) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(AccountOperation::PushSubscribe),
        Cause::Request(RequestCause::Unsupported {
            operation: AccountOperation::PushSubscribe,
        }),
    )
    .operation(AccountOperation::PushSubscribe)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .scope(ErrorScope::Cursor(scope))
    .try_build()
    .expect("valid account error classification")
}

/// The whole-request refusal for a request whose scopes all died INSIDE an
/// arm: every resource unresolvable in `subscribe_graph`, or every id refused
/// or omitted by `translateExchangeIds`.
///
/// `Err(_)` from `push_subscribe` means nothing was subscribed, and the arms
/// used to answer that case with `Ok(PushSubscription)` carrying no handle and
/// an all-failed ledger - a shape the documented contract does not have and
/// which reads to a caller matching on `Ok` as a partial success with an
/// unusable handle.
///
/// The kind is DERIVED from the scopes rather than minted fresh. The jmap
/// precedent (`no_mappable_push_scopes`) answers `Request(Malformed)` from a
/// named constructor, and that is right there because its whole population has
/// exactly one failure mode - the scope names no JMAP data type, which is
/// always a caller-shape fault. These arms do not: a scope can die as
/// `Unsupported(PushSubscribe)` (no Graph resource for its shape), as
/// `Request(Malformed)` (a shared mailbox this account no longer configures,
/// or an id Graph declines to convert), as `Protocol(ContractViolation)`
/// (`translateExchangeIds` omitted an answer it owed), or as a transport
/// failure on the translation POST. Flattening a provider contract violation
/// or a retryable transport drop into `Request(Malformed)` would tell the
/// engine the CALLER is buggy and derive `ClientBug` for a fault the caller
/// cannot fix. So the first failure is promoted verbatim - it is already
/// classified, already scope-correlated - and every other failure's causes
/// ride along as secondary chain evidence, so the per-scope diagnosis the
/// ledger held is not lost with the ledger.
pub(super) fn no_subscribable_push_scopes(failed: &[bifrost_types::BatchFailure]) -> AccountError {
    let Some((first, rest)) = failed.split_first() else {
        // Unreachable through `push_subscribe`: an arm answers `None` only
        // when every eligible scope was filed on the failed lane. Answer the
        // uncorrelated refusal rather than panicking on a lane invariant that
        // `finalize` already enforces.
        return unsupported_push_error();
    };
    let mut builder = first.error.clone().into_builder();
    for failure in rest {
        for cause in failure.error.chain().iter() {
            builder = builder.push_cause(cause.clone());
        }
    }
    builder.try_build().unwrap_or_else(|_| first.error.clone())
}

/// Raise the account's push-down latch, emitting `Disconnected` on the edge.
///
/// The latch moves whether or not the send lands. `push_tx` is a broadcast
/// sender, so a send with no subscriber is discarded, and a consumer that
/// attaches mid-outage will later be handed a `Reconnected` whose matching
/// `Disconnected` it never saw. That asymmetry is deliberate, because the
/// surviving half is the conservative one: `Reconnected` costs the engine a
/// whole-account `Coalesced` reconcile, while the lost `Disconnected` is an
/// advisory warning carrying no coverage obligation (the sync reconciler
/// answers it with a `Warning` and nothing else, and holds no connectivity
/// state that the missing half could make WRONG).
///
/// Gating the latch on a successful send inverts that trade. The outage
/// would go unrecorded, so the recovery would raise no edge, and a consumer
/// present for the recovery would lose the reconcile that covers the gap -
/// a missed reconcile in place of a superfluous one. The latch records the
/// outage the account had, not the outage somebody was listening for.
///
/// If the lost warning ever matters, the sound repair is prime-on-subscribe:
/// have `push_stream` yield `Disconnected` as its first item while the latch
/// is up. That adds no reconciles, only the advisory warning. Not built,
/// because nothing needs it yet.
pub(super) fn mark_push_disconnected(account: &crate::account::GraphAccount) {
    if !account
        .push_disconnected
        .swap(true, std::sync::atomic::Ordering::SeqCst)
    {
        let _ = account
            .push_tx
            .send(bifrost_types::WatchEvent::Disconnected);
    }
}

/// Clear the account's push-down latch, emitting `Reconnected` on the edge.
///
/// Edge-triggered on purpose: `Reconnected` costs the engine a full reconcile
/// of every registered cursor scope, so it is owed only where coverage was
/// actually lost and regained. A subscribe on a healthy account is not that
/// edge. Returns whether the event was published.
pub(super) fn mark_push_reconnected(account: &crate::account::GraphAccount) -> bool {
    if account
        .push_disconnected
        .swap(false, std::sync::atomic::Ordering::SeqCst)
    {
        let _ = account.push_tx.send(bifrost_types::WatchEvent::Reconnected);
        return true;
    }
    false
}

/// Publish `Reconnected` unconditionally and clear the latch.
///
/// For the one caller that KNOWS a coverage gap happened without the latch
/// ever going up: the renewal worker's recreate of a subscription Graph had
/// already dropped. The resource had no live subscription between its
/// disappearance and the create, and Graph replays nothing for that window,
/// so the reconcile is owed even though no renewal tick reported a failure.
pub(super) fn announce_push_recovered(account: &crate::account::GraphAccount) {
    account
        .push_disconnected
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let _ = account.push_tx.send(bifrost_types::WatchEvent::Reconnected);
}

/// `push_subscribe` on (or racing) a closed account. A runtime failure, never
/// retried; shared by both arms.
pub(super) fn account_closed_error() -> AccountError {
    into_account_error(
        crate::error::GraphError::RuntimeFailure {
            message: "the account is closed".to_string(),
        },
        GraphErrorContext::graph(AccountOperation::PushSubscribe),
    )
}

/// Prefix of a webhook handle: `graph1:<token>:<id>,<id>,...`.
///
/// The handle is opaque to the engine but is the ONLY record of the
/// subscription that survives a reopen. The engine keeps a handle whose
/// teardown failed and retries `push_unsubscribe` on whatever account instance
/// is current, which after a reopen is a newer connection whose in-memory
/// subscription map has never heard of it. So the handle carries the Graph
/// subscription ids themselves; Graph ids are server-global per tenant and any
/// instance holding the account's credentials can DELETE them.
const GRAPH_HANDLE_PREFIX: &str = "graph1:";
/// Prefix of an EWS handle: `ews1:<token>`. An EWS streaming subscription is
/// bound to the worker's connection and dies with it, so the handle needs no
/// server state; the prefix only lets any instance recognise it as such.
const EWS_HANDLE_PREFIX: &str = "ews1:";

/// What a handle asks an instance to tear down.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum DecodedHandle {
    /// A webhook handle and the Graph subscription ids it was minted over.
    Graph(Vec<String>),
    /// An EWS handle: no server state outlives the connection.
    Ews,
    /// Not a handle this crate mints in a shape it can act on (including
    /// handles minted before ids were embedded). Nothing to delete.
    Unrecognized,
}

/// Whether a subscription id is safe to embed in a handle and later place in
/// a `/subscriptions/{id}` URL. Graph ids are GUIDs; the allowlist exists so a
/// forged handle cannot smuggle path segments or query text into a DELETE.
fn is_embeddable_subscription_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

/// Build the webhook handle for a registered group. An id that cannot be
/// embedded is left out (and logged): this instance still tears it down
/// through its own map, but no other instance could.
pub(super) fn graph_handle<'a>(
    token: &str,
    server_ids: impl IntoIterator<Item = &'a str>,
) -> SubscriptionHandle {
    let mut ids = Vec::new();
    for id in server_ids {
        if is_embeddable_subscription_id(id) {
            ids.push(id);
        } else {
            tracing::warn!(
                target: "bifrost_graph::push",
                "a Graph subscription id is not embeddable in a handle; only this instance can retire it"
            );
        }
    }
    SubscriptionHandle(format!("{GRAPH_HANDLE_PREFIX}{token}:{}", ids.join(",")))
}

pub(super) fn ews_handle(token: &str) -> SubscriptionHandle {
    SubscriptionHandle(format!("{EWS_HANDLE_PREFIX}{token}"))
}

/// Classify a handle. Any malformed webhook handle (bad token, empty or
/// unembeddable id) is `Unrecognized` as a whole rather than partially
/// honoured: a forged handle must not get to choose what is DELETEd.
pub(super) fn decode_handle(handle: &SubscriptionHandle) -> DecodedHandle {
    let raw = handle.0.as_str();
    if raw.starts_with(EWS_HANDLE_PREFIX) {
        return DecodedHandle::Ews;
    }
    let Some(rest) = raw.strip_prefix(GRAPH_HANDLE_PREFIX) else {
        return DecodedHandle::Unrecognized;
    };
    let Some((token, ids)) = rest.split_once(':') else {
        return DecodedHandle::Unrecognized;
    };
    if token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return DecodedHandle::Unrecognized;
    }
    let ids: Vec<String> = ids.split(',').map(str::to_string).collect();
    if ids.iter().all(|id| is_embeddable_subscription_id(id)) {
        DecodedHandle::Graph(ids)
    } else {
        DecodedHandle::Unrecognized
    }
}

/// A fresh random token, the uniqueness part of a handle.
pub(super) fn new_handle_token() -> Result<String, AccountError> {
    token_from_entropy(getrandom::fill)
}

/// Mint a handle token from the given entropy source.
///
/// A failing source is `Internal(RuntimeFailure)`: a local facility failed,
/// which is neither the network's fault nor the provider's. It used to be
/// minted as a `bifrost_net` `Network` error so it would retry, but a retry
/// buys nothing here. `getrandom` already absorbs the one transient case
/// (`EINTR`) internally and BLOCKS, rather than failing, while the kernel pool
/// initializes, so what reaches this point is a persistent condition of the
/// host - the syscall denied by a sandbox, no `/dev/urandom`, an unsupported
/// target - and a `Transport` retry loop against it spins forever while
/// telling the operator the network is down. The IMAP SCRAM nonce draws from
/// the same source and classifies the same way. Raised before any
/// subscription request is built, so there is no transmission to record.
fn token_from_entropy<E: std::fmt::Display>(
    fill: impl FnOnce(&mut [u8]) -> Result<(), E>,
) -> Result<String, AccountError> {
    let mut bytes = [0_u8; 16];
    fill(&mut bytes).map_err(|error| {
        into_account_error(
            crate::error::GraphError::RuntimeFailure {
                message: format!("subscription handle entropy source failed: {error}"),
            },
            GraphErrorContext::graph(AccountOperation::PushSubscribe),
        )
    })?;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{DecodedHandle, decode_handle, ews_handle, graph_handle, token_from_entropy};
    use bifrost_types::{AccountErrorKind, InternalErrorKind, RecoveryClass, SubscriptionHandle};

    #[test]
    fn a_graph_handle_round_trips_its_server_ids() {
        let handle = graph_handle("ab12", ["id-1", "ID_2"]);
        assert_eq!(
            decode_handle(&handle),
            DecodedHandle::Graph(vec!["id-1".to_string(), "ID_2".to_string()])
        );
    }

    #[test]
    fn an_ews_handle_decodes_as_ews() {
        assert_eq!(decode_handle(&ews_handle("ab12")), DecodedHandle::Ews);
    }

    /// A forged or legacy handle must not get to choose what is DELETEd.
    #[test]
    fn malformed_handles_decode_to_nothing() {
        for raw in [
            "h",
            "ab12",
            "graph1:",
            "graph1:ab12",
            "graph1:ab12:",
            "graph1:zz:id",
            "graph1:ab12:id,",
            "graph1:ab12:../me/messages",
            "graph1:ab12:a?b=c",
            "graph1:ab12:ok,a/b",
        ] {
            assert_eq!(
                decode_handle(&SubscriptionHandle(raw.to_string())),
                DecodedHandle::Unrecognized,
                "{raw}"
            );
        }
    }

    /// A failed entropy source is the client's runtime failure, terminal. It
    /// was a `Transport(Network)` error, which derives `Retry(SameRequest)`
    /// and blames the network for a host that cannot produce randomness.
    #[test]
    fn a_failed_entropy_source_is_a_runtime_failure_not_a_network_retry() {
        let error = token_from_entropy(|_| Err("entropy source unavailable"))
            .expect_err("a failed source must not mint a handle");
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Internal(InternalErrorKind::RuntimeFailure)
        );
        assert_eq!(error.recovery(), &RecoveryClass::InternalFailure);
    }

    #[test]
    fn a_working_entropy_source_mints_a_hex_token() {
        let token = token_from_entropy(|bytes: &mut [u8]| {
            bytes.fill(0xab);
            Ok::<(), &str>(())
        })
        .expect("mint");
        assert_eq!(token, "ab".repeat(16));
    }
}
