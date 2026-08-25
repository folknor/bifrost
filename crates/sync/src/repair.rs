//! The repair pass: working inventory coverage debt off.
//!
//! Reads the ledger back, asks the account to re-read what it could not
//! represent, publishes what it recovered, and discharges only what the
//! consumer durably acknowledged.
//!
//! # Why this needs no exclusion against the live change stream
//!
//! It looks like it should. A repaired object is read at one moment and applied
//! at another, so a live deletion in between could be overwritten - the classic
//! resurrection. The reason it cannot happen here is a property of the existing
//! inventory contract rather than anything this module does:
//!
//! **No inventory path delivers object state to a consumer.** `InventoryFusion`
//! and `BackfillRunner` both take the account's `InventoryEntry`, keep the id,
//! discard the entry, and publish `ObjectChange::Created`. The consumer
//! hydrates from the id.
//!
//! Repair mirrors that exactly. A repair publication carries ids and nothing
//! else, so there is no payload that could overwrite a newer representation and
//! nothing to insert after a tombstone. A stale repair `Created` is resolved
//! the same way a stale backfill `Created` already is: hydration returns
//! not-found and the consumer stores nothing. That is why there is no
//! conditional application here, no tombstone requirement, no version-relation
//! hook, and no lease held against polling.
//!
//! The account still returns the rebuilt entry across the ACCOUNT boundary,
//! because constructing it is the proof that the representation failure which
//! raised the obligation has healed - an id alone would only prove the object
//! still exists. The engine validates it, keeps the id, and drops the rest.
//!
//! # What discharge means
//!
//! An obligation says the walk failed to tell the consumer an object exists and
//! the cursor then advanced past it, so the changes stream will never mention
//! it again. A durably acknowledged `Created` is that telling, and it is the
//! most a successful walk ever achieves for any object. Demanding proof of
//! successful hydration would hold repair to a bar the non-degraded path does
//! not meet, and would fuse two failure domains: coverage asks whether
//! enumeration announced the object, hydration asks whether a projection can be
//! fetched right now. A later hydration failure is the hydration lane's, with
//! its own `ItemOutcome`.

use std::collections::HashMap;
use std::sync::Arc;

use bifrost_types::{
    Account, AccountId, Change, CursorScope, InventoryEntry, InventoryRepairEvent,
    InventoryRepairOutcome, InventoryRepairRequest, InventoryRepairTarget, ObjectChange,
    ObjectChangeKind, ObligationKey, RegionRepairProof, RepairAttemptId, SyncEvent,
};
use futures::stream::StreamExt;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::cursor::DebtLedger;
use crate::error::Error;
use crate::multiplexer::{MultiplexerEvent, WriterRequest};

/// How many completed attempts a lineage gets before it stops being retried
/// automatically.
///
/// Expiry yields `OperatorBlocked`, never abandonment. Attempts accrue at the
/// lineage root so re-minting an equivalent obligation cannot reset it.
pub const DEFAULT_REPAIR_BUDGET: u32 = 5;

/// One resolved repair, ready for the writer.
#[derive(Debug, Clone)]
pub enum RepairResolution {
    /// Recovered and published. Discharge once the consumer acknowledges.
    Recovered {
        key: ObligationKey,
        attempt: RepairAttemptId,
        generation: u64,
    },
    /// Proved not owed. Nothing was published, so nothing is awaited.
    Irrelevant {
        key: ObligationKey,
        detail: String,
        generation: u64,
    },
    /// Split into children. Atomic parent-to-children swap.
    Replaced {
        key: ObligationKey,
        proof: Box<RegionRepairProof>,
        generation: u64,
    },
    /// This attempt did not resolve it. Costs one attempt against the lineage.
    Deferred { key: ObligationKey },
}

/// Drive one repair pass for `account`.
///
/// Returns the number of obligations that reached a terminal resolution.
///
/// Errors from the account are per-request wherever the account correlates
/// them. A stream that ends with requests outstanding does NOT mean the account
/// classified them: those become local deferrals, because recording a
/// conclusion nobody reached is exactly the kind of invented certainty the
/// coverage model exists to prevent.
pub async fn run_repair_pass(
    account: &dyn Account,
    account_id: &AccountId,
    ledger: &DebtLedger,
    changes_tx: Option<&broadcast::Sender<MultiplexerEvent>>,
    writer_tx: &mpsc::Sender<WriterRequest>,
    coverage: &Arc<crate::cursor::PendingCoverage>,
    max_requests: usize,
) -> Result<usize, Error> {
    let planned = plan_requests(ledger, coverage, max_requests);
    if planned.is_empty() {
        return Ok(0);
    }

    let mut outstanding: HashMap<RepairAttemptId, PlannedRequest> = planned
        .iter()
        .map(|planned| (planned.request.attempt, planned.clone()))
        .collect();
    let requests: Vec<InventoryRepairRequest> = planned.iter().map(|p| p.request.clone()).collect();

    let mut stream = account.repair_inventory(Box::pin(futures::stream::iter(requests)));
    let mut resolutions: Vec<RepairResolution> = Vec::new();
    let mut recovered_ids: Vec<(CursorScope, bifrost_types::ObjectId)> = Vec::new();

    while let Some(event) = stream.next().await {
        let outcome = match event {
            InventoryRepairEvent::Outcome(outcome) => outcome,
            // Explains why the stream stopped. It does not answer for the
            // requests still outstanding - those fall through to the local
            // deferral below.
            InventoryRepairEvent::Terminated(error) => {
                tracing::warn!(
                    target: "bifrost.sync.repair",
                    account = ?account_id,
                    error = %error,
                    "repair stream terminated with attempts outstanding"
                );
                break;
            }
            _ => continue,
        };

        let attempt = outcome.attempt();
        // An unknown attempt, or a second outcome for one already answered, is
        // a contract violation and is dropped rather than applied to whatever
        // obligation happens to share the key.
        let Some(planned) = outstanding.remove(&attempt) else {
            tracing::warn!(
                target: "bifrost.sync.repair",
                account = ?account_id,
                attempt = attempt.0,
                "repair outcome for an unknown or already-answered attempt"
            );
            continue;
        };

        match outcome {
            InventoryRepairOutcome::ObjectRecovered { entry, .. } => {
                match validate_object_recovery(&planned, &entry) {
                    Ok(id) => {
                        recovered_ids.push((planned.scope.clone(), id));
                        resolutions.push(RepairResolution::Recovered {
                            key: planned.request.key.clone(),
                            attempt,
                            generation: planned.generation,
                        });
                    }
                    Err(reason) => {
                        tracing::warn!(
                            target: "bifrost.sync.repair",
                            account = ?account_id,
                            attempt = attempt.0,
                            reason,
                            "rejecting an object recovery that does not match its request"
                        );
                        resolutions.push(RepairResolution::Deferred {
                            key: planned.request.key.clone(),
                        });
                    }
                }
            }
            InventoryRepairOutcome::RegionRecovered { entries, proof, .. } => {
                if !matches!(planned.request.target, InventoryRepairTarget::Region { .. }) {
                    tracing::warn!(
                        target: "bifrost.sync.repair",
                        account = ?account_id,
                        attempt = attempt.0,
                        "region recovery returned for an object request"
                    );
                    resolutions.push(RepairResolution::Deferred {
                        key: planned.request.key.clone(),
                    });
                    continue;
                }
                for entry in &entries {
                    recovered_ids.push((planned.scope.clone(), entry.id.clone()));
                }
                // Some entries never discharge a region; the completeness proof
                // is what does, and the writer checks it against the
                // obligation's own domain.
                resolutions.push(RepairResolution::Replaced {
                    key: planned.request.key.clone(),
                    proof: Box::new(proof),
                    generation: planned.generation,
                });
            }
            InventoryRepairOutcome::DefinitivelyIrrelevant { evidence, .. } => {
                let detail = match &evidence {
                    bifrost_types::DefinitiveIrrelevance::AbsentUnderCursorBridge { detail }
                    | bifrost_types::DefinitiveIrrelevance::OutOfScope { detail } => detail.clone(),
                    _ => String::new(),
                };
                resolutions.push(RepairResolution::Irrelevant {
                    key: planned.request.key.clone(),
                    detail,
                    generation: planned.generation,
                });
            }
            InventoryRepairOutcome::Deferred { error, .. } => {
                tracing::debug!(
                    target: "bifrost.sync.repair",
                    account = ?account_id,
                    attempt = attempt.0,
                    error = %error,
                    "repair attempt deferred"
                );
                resolutions.push(RepairResolution::Deferred {
                    key: planned.request.key.clone(),
                });
            }
            InventoryRepairOutcome::Replaced { proof, .. } => {
                resolutions.push(RepairResolution::Replaced {
                    key: planned.request.key.clone(),
                    proof: Box::new(proof),
                    generation: planned.generation,
                });
            }
            _ => {
                resolutions.push(RepairResolution::Deferred {
                    key: planned.request.key.clone(),
                });
            }
        }
    }

    // Requests the account never answered. A LOCAL deferral: the engine records
    // that an attempt happened and produced nothing, without claiming the
    // account reached any conclusion about it.
    for planned in outstanding.into_values() {
        resolutions.push(RepairResolution::Deferred {
            key: planned.request.key.clone(),
        });
    }

    let publication = publish_recovered(changes_tx, coverage, &recovered_ids);
    let resolved = resolutions.len();
    let (done, wait) = oneshot::channel();
    writer_tx
        .send(WriterRequest::ApplyRepair {
            resolutions,
            publication,
            done,
        })
        .await
        .map_err(|e| Error::Other(format!("writer channel closed: {e}")))?;
    wait.await
        .map_err(|e| Error::Other(format!("writer dropped before applying repair: {e}")))??;
    Ok(resolved)
}

#[derive(Debug, Clone)]
struct PlannedRequest {
    request: InventoryRepairRequest,
    scope: CursorScope,
    generation: u64,
}

fn plan_requests(
    ledger: &DebtLedger,
    coverage: &Arc<crate::cursor::PendingCoverage>,
    max_requests: usize,
) -> Vec<PlannedRequest> {
    ledger
        .repairable()
        .take(max_requests)
        .filter_map(|entry| {
            let target = entry.target.clone()?;
            Some(PlannedRequest {
                request: InventoryRepairRequest {
                    attempt: RepairAttemptId(coverage.next_generation()),
                    key: entry.key.clone(),
                    domain: entry.domain.clone(),
                    target,
                },
                scope: entry.domain.scope.clone(),
                generation: entry.generation,
            })
        })
        .collect()
}

/// An object recovery must answer the request it was given.
fn validate_object_recovery(
    planned: &PlannedRequest,
    entry: &InventoryEntry,
) -> Result<bifrost_types::ObjectId, &'static str> {
    let InventoryRepairTarget::Object { id, .. } = &planned.request.target else {
        return Err("object recovery returned for a region request");
    };
    if entry.id != *id {
        return Err("recovered entry names a different object than the obligation");
    }
    Ok(entry.id.clone())
}

/// Publish recovered ids as ordinary `Created` signals.
///
/// Ids only, exactly as a successful inventory walk publishes them. Nothing
/// here carries object state, which is what keeps repair free of the
/// version-ordering problem it superficially resembles.
///
/// One publication for the whole batch, so discharge is all-or-nothing: partial
/// consumer application would otherwise force the writer to manufacture
/// residual obligations for whichever ids did not land.
fn publish_recovered(
    changes_tx: Option<&broadcast::Sender<MultiplexerEvent>>,
    coverage: &Arc<crate::cursor::PendingCoverage>,
    recovered: &[(CursorScope, bifrost_types::ObjectId)],
) -> Option<crate::cursor::PublicationId> {
    let tx = changes_tx?;
    if recovered.is_empty() {
        return None;
    }
    let scope = recovered[0].0.clone();
    let changes: Vec<Change> = recovered
        .iter()
        .map(|(_, id)| {
            Change::ObjectChange(ObjectChange {
                id: id.clone(),
                kind: ObjectChangeKind::Created,
            })
        })
        .collect();
    // Carries no coverage report: a repair proves things about specific
    // obligations, not about a region of an enumeration, so it must not touch
    // the coverage lattice.
    let publication = coverage.publish_without_report(0);
    let batch = bifrost_types::Batch {
        items: changes,
        page_boundary: bifrost_types::PageBoundary::Final,
        server_latency: std::time::Duration::ZERO,
        bytes_in: 0,
        checkpoint: None,
    };
    let event = MultiplexerEvent {
        scope,
        event: Arc::new(SyncEvent::Batch(batch)),
        checkpoint: None,
        publication: Some(publication),
    };
    let delivered = tx.send(event).unwrap_or(0);
    if crate::multiplexer::delivered_to_real_subscriber(delivered) {
        Some(publication)
    } else {
        // Nothing can acknowledge it, so nothing may be discharged on it.
        coverage.retire(publication);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{PlannedRequest, RepairResolution, validate_object_recovery};
    use bifrost_types::{
        CoverageDomain, CursorScope, Fingerprint, InventoryEntry, InventoryRepairRequest,
        InventoryRepairTarget, ObjectId, ObjectType, ObligationKey, RepairAttemptId, ServerVersion,
    };

    fn scope() -> CursorScope {
        CursorScope::Type(ObjectType::Email)
    }

    fn planned(target: InventoryRepairTarget) -> PlannedRequest {
        PlannedRequest {
            request: InventoryRepairRequest {
                attempt: RepairAttemptId(1),
                key: ObligationKey(b"k".to_vec()),
                domain: CoverageDomain::full(scope()),
                target,
            },
            scope: scope(),
            generation: 1,
        }
    }

    fn entry(id: &str) -> InventoryEntry {
        InventoryEntry {
            id: ObjectId(id.into()),
            memberships: Vec::new(),
            size: None,
            blob_id: None,
            fingerprint: Fingerprint {
                server_version: ServerVersion::Unavailable,
                size: None,
                flags_hash: bifrost_types::canonical_flags_hash(std::iter::empty::<&str>()),
            },
            thread_id: None,
            message_id: None,
            references: Vec::new(),
            in_reply_to: None,
        }
    }

    /// A recovery that names a DIFFERENT object than the one asked about must
    /// not discharge the obligation. Publishing its id would announce an object
    /// nobody owed while the owed one stayed missing, and the ledger would
    /// record the gap as closed.
    #[test]
    fn a_recovery_naming_another_object_is_rejected() {
        let planned = planned(InventoryRepairTarget::Object {
            id: ObjectId("wanted".into()),
            repair: Vec::new(),
        });
        assert!(validate_object_recovery(&planned, &entry("something-else")).is_err());
        assert!(validate_object_recovery(&planned, &entry("wanted")).is_ok());
    }

    /// An object outcome answering a region request is a kind mismatch. The
    /// engine must not treat it as having accounted for the region, which has
    /// completeness requirements a single object cannot meet.
    #[test]
    fn an_object_recovery_for_a_region_request_is_rejected() {
        let planned = planned(InventoryRepairTarget::Region {
            replay: b"tok".to_vec(),
        });
        assert!(validate_object_recovery(&planned, &entry("anything")).is_err());
    }

    #[test]
    fn a_deferred_resolution_names_its_obligation() {
        let resolution = RepairResolution::Deferred {
            key: ObligationKey(b"k".to_vec()),
        };
        let RepairResolution::Deferred { key } = resolution else {
            panic!("shape");
        };
        assert_eq!(key.0, b"k".to_vec());
    }
}
