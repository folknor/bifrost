//! The EWS streaming arm of `push_subscribe`: which scopes the streaming
//! worker can subscribe to at all, the one-shot `restId` -> `ewsId`
//! translation their Subscribe body needs, and the registered subscription
//! state the worker reads.
//!
//! The worker itself lives in `super::super::ews_stream`; it is spawned by
//! `push_stream::ensure_ews_worker`.

use std::collections::{HashMap, HashSet};

use bifrost_types::{
    AccountError, AccountOperation, CursorScope, ErrorScope, ProtocolErrorKind, SubscriptionHandle,
};
use serde::{Deserialize, Serialize};

use crate::account::GraphAccount;
use crate::account::graph_error::{
    GraphErrorContext, id_translation_refused, into_account_error, protocol_violation,
};

use super::common::{new_handle, unsupported_push_scope_error};

#[derive(Debug, Clone)]
pub(crate) struct EwsSubscriptionState {
    /// The Graph cursor scope and EWS id for its folder. Graph `restId` and
    /// EWS `ewsId` are distinct opaque formats. The live EWS subscription id
    /// is NOT stored here: it is worker-local state, minted per Subscribe
    /// and abandoned (with a best-effort Unsubscribe) on every reconnect or
    /// topology handoff, so nothing outside the worker's loop may hold it.
    pub(crate) scopes: Vec<EwsSubscriptionScope>,
}

#[derive(Debug, Clone)]
pub(crate) struct EwsSubscriptionScope {
    pub(crate) scope: CursorScope,
    pub(crate) ews_folder_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TranslateExchangeIdsRequest {
    input_ids: Vec<String>,
    source_id_type: &'static str,
    target_id_type: &'static str,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TranslateExchangeIdsResponse {
    pub(super) value: Vec<TranslatedExchangeId>,
}

/// One `convertIdResult`.
///
/// Graph answers PER input id inside an otherwise successful 200: a converted
/// id carries `targetId`, an id it could not convert carries `errorDetails`
/// and no target. Requiring `targetId` made a single refused id fail
/// deserialization of the whole response, so one stale folder surfaced as a
/// terminal `Protocol(ParseFailed)` naming nothing instead of a
/// scope-correlated refusal naming the folder and Graph's own code.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TranslatedExchangeId {
    pub(super) source_id: String,
    pub(super) target_id: Option<String>,
    pub(super) error_details: Option<ConvertIdError>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ConvertIdError {
    pub(super) code: Option<String>,
    pub(super) message: Option<String>,
}

/// Graph caps `translateExchangeIds`' `inputIds` collection at 1,000 strings
/// and rejects the whole request above it.
pub(super) const TRANSLATE_EXCHANGE_IDS_MAX_INPUTS: usize = 1_000;

/// The Graph `restId` this scope contributes to the EWS Subscribe request, or
/// `None` when the EWS streaming worker cannot subscribe to it at all.
///
/// Two exclusions, both of which the worker would otherwise turn into a
/// remote failure that reads as a provider fault instead of the caller's
/// unsupported request:
///
/// - A non-`FolderType` scope contributes nothing to the Subscribe body's
///   `FolderIds`, so a request built only from such scopes ships an empty
///   `<t:FolderIds></t:FolderIds>` and EWS rejects the whole subscription.
/// - A foreign (shared-mailbox) folder is addressable only with that
///   mailbox's EWS routing headers, and the Subscribe path sends
///   `EwsHeaders::default()`. Its native folder id would be resolved
///   against the PRIMARY mailbox's namespace - a wrong folder or a miss,
///   never the intended one. Mailbox-grouped EWS subscriptions are the fix;
///   until then this rejects rather than silently mis-targets.
///
/// Returning the id rather than a bool fuses the check with the extraction:
/// the translation request, the reconciliation, and the retained state all
/// read the one string this produced, so no later phase can re-derive it
/// differently or have to assert a shape the predicate already guaranteed.
pub(super) fn ews_subscribable_folder_id(scope: &CursorScope) -> Option<String> {
    let CursorScope::FolderType { folder, .. } = scope else {
        return None;
    };
    let parsed = crate::account::foreign::parse_folder(folder);
    if parsed.foreign().is_some() {
        return None;
    }
    Some(parsed.native_id().to_string())
}

pub(super) async fn subscribe_ews(
    account: GraphAccount,
    eligible: Vec<(bifrost_types::BatchItemId, CursorScope)>,
    outcomes: &mut bifrost_types::BatchOutcomeBuilder<CursorScope>,
) -> Result<Option<SubscriptionHandle>, AccountError> {
    let mut pending = Vec::with_capacity(eligible.len());
    for (item, scope) in eligible {
        // An unsubscribable shape (non-`FolderType`, or a foreign mailbox
        // this Subscribe cannot route to) is that scope's refusal, not the
        // request's: its valid siblings still get a live subscription.
        let Some(source_id) = ews_subscribable_folder_id(&scope) else {
            outcomes.push_failed(item, unsupported_push_scope_error(scope));
            continue;
        };
        pending.push((item, scope, source_id));
    }
    if pending.is_empty() {
        return Ok(None);
    }
    let scopes = translate_ews_scopes(&account, pending, outcomes).await?;
    if scopes.is_empty() {
        return Ok(None);
    }
    let handle = new_handle()?;
    account
        .ews_subscriptions
        .write()
        .await
        .insert(handle.clone(), EwsSubscriptionState { scopes });
    // Bump the topology generation AFTER the map write: the worker only
    // re-reads the map after observing a generation it has not seen, so the
    // read this bump provokes is guaranteed to include the new registration.
    account
        .ews_topology
        .send_modify(|generation| *generation = generation.wrapping_add(1));
    crate::account::push_stream::ensure_ews_worker(account).await;
    Ok(Some(handle))
}

/// The error context for the translation request itself.
///
/// The operation stays `PushSubscribe` - that IS what the caller asked for,
/// and borrowing an unrelated idempotent operation would put a name in every
/// telemetry export that no call site matches. What the context corrects is
/// the recovery derivation: `PushSubscribe` is non-idempotent, so an
/// in-flight transport drop would derive
/// `Reconcile(TransportDropAfterSend, [CheckTarget])` and send the engine
/// looking for a subscription to probe. This POST is a read-only id
/// conversion that runs BEFORE any local subscription state, EWS
/// subscription, or handle exists: there is no target, nothing was created,
/// and repeating it is free.
pub(super) fn translate_error_context() -> GraphErrorContext {
    GraphErrorContext::graph(AccountOperation::PushSubscribe).idempotent()
}

/// Converts Graph REST ids into the EWS ids required by this subscription's
/// SOAP request and its notification folder ids. The response can be
/// unordered, and is fanned out over several requests once the folder count
/// passes Graph's cap, so each scope is preserved by matching its `sourceId`
/// rather than by position.
pub(super) async fn translate_ews_scopes(
    account: &GraphAccount,
    pending: Vec<PendingEwsScope>,
    outcomes: &mut bifrost_types::BatchOutcomeBuilder<CursorScope>,
) -> Result<Vec<EwsSubscriptionScope>, AccountError> {
    let mut translated = Vec::new();
    for input_ids in translation_input_chunks(&pending) {
        let request = TranslateExchangeIdsRequest {
            input_ids,
            source_id_type: "restId",
            target_id_type: "ewsId",
        };
        let response: TranslateExchangeIdsResponse = account
            .client
            .post("/me/translateExchangeIds", &request)
            .await
            .map_err(|error| into_account_error(error, translate_error_context()))?;
        translated.extend(response.value);
    }
    Ok(reconcile_translated_ews_scopes(
        pending, translated, outcomes,
    ))
}

/// One scope that passed the shape checks, carried with its lane id and the
/// REST folder id the translation request will submit for it.
pub(super) type PendingEwsScope = (bifrost_types::BatchItemId, CursorScope, String);

/// The `inputIds` collections one subscription's translation needs.
///
/// Two scopes can name the same folder - an `Email` and a `Contact` scope
/// over one container decode to the same native id - and Graph caps
/// `inputIds` at `TRANSLATE_EXCHANGE_IDS_MAX_INPUTS`, rejecting the whole
/// request above it. So deduplicate first (that alone keeps a mailbox under
/// the cap for the folder counts that produce duplicates) and then chunk, so
/// a genuinely large mailbox fans out instead of failing before EWS setup is
/// even attempted. First-seen order is preserved so the chunk boundaries are
/// deterministic and a failure names a stable set of ids.
pub(super) fn translation_input_chunks(pending: &[PendingEwsScope]) -> Vec<Vec<String>> {
    let mut seen = HashSet::new();
    let mut unique = Vec::with_capacity(pending.len());
    for (_, _, source_id) in pending {
        if seen.insert(source_id.as_str()) {
            unique.push(source_id.clone());
        }
    }
    unique
        .chunks(TRANSLATE_EXCHANGE_IDS_MAX_INPUTS)
        .map(<[String]>::to_vec)
        .collect()
}

/// Pairs every submitted scope back with its translated EWS id, or fails the
/// subscription.
///
/// One answer can serve several scopes (the deduplicated request asked once
/// for a folder two scopes share), and the three ways an answer can fail are
/// distinct enough to classify apart:
///
/// - refused (`errorDetails`, no target): Graph answered, and the answer is
///   "not this id". Terminal, scope-correlated, carries Graph's code.
/// - omitted: Graph must answer every id it was given. A missing answer is
///   the provider breaking its own contract, not a malformed request.
/// - answered with neither a target nor an error: the same contract
///   violation in a different shape, and the one case where falling through
///   would leave a scope with no id at all.
///
/// None of the three may degrade to sending the untranslated `restId`: EWS
/// cannot parse it, so the Subscribe would fail as an opaque
/// `SoapFaultCode::Unknown` instead of the diagnosable local error.
pub(super) fn reconcile_translated_ews_scopes(
    pending: Vec<PendingEwsScope>,
    translated: Vec<TranslatedExchangeId>,
    outcomes: &mut bifrost_types::BatchOutcomeBuilder<CursorScope>,
) -> Vec<EwsSubscriptionScope> {
    let answers: HashMap<String, TranslatedExchangeId> = translated
        .into_iter()
        .map(|entry| (entry.source_id.clone(), entry))
        .collect();
    let mut subscribed = Vec::new();
    for (item, scope, source_id) in pending {
        let error_scope = ErrorScope::Cursor(scope.clone());
        let Some(answer) = answers.get(&source_id) else {
            outcomes.push_failed(
                item,
                protocol_violation(
                    ProtocolErrorKind::ContractViolation,
                    AccountOperation::PushSubscribe,
                    Some(error_scope),
                    "translateExchangeIds omitted a subscribed folder",
                ),
            );
            continue;
        };
        if let Some(ews_folder_id) = answer
            .target_id
            .as_ref()
            .filter(|target| !target.trim().is_empty())
        {
            outcomes.push_succeeded(item, scope.clone());
            subscribed.push(EwsSubscriptionScope {
                scope,
                ews_folder_id: ews_folder_id.clone(),
            });
            continue;
        }
        let error = match answer.error_details.as_ref() {
            Some(details) => id_translation_refused(
                AccountOperation::PushSubscribe,
                error_scope,
                details.code.as_deref(),
                details.message.as_deref(),
            ),
            None => protocol_violation(
                ProtocolErrorKind::ContractViolation,
                AccountOperation::PushSubscribe,
                Some(error_scope),
                "translateExchangeIds answered without a target id or error details",
            ),
        };
        outcomes.push_failed(item, error);
    }
    subscribed
}

pub(super) async fn unsubscribe_ews(
    account: GraphAccount,
    handle: SubscriptionHandle,
) -> Result<(), AccountError> {
    let removed = account
        .ews_subscriptions
        .write()
        .await
        .remove(&handle)
        .is_some();
    // Only a registration that actually existed changes the scope union;
    // bumping on an idempotent re-unsubscribe would churn the live stream
    // through a pointless resubscribe.
    if removed {
        account
            .ews_topology
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
    Ok(())
}
