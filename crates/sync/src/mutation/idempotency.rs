//! `IdempotencyKey` vending.
//!
//! Each mutation campaign uses a consumer-supplied `RunId`. The consumer owns
//! persistence across process restarts; `IdempotencyVendor`
//! then mints `IdempotencyKey`s with monotonically increasing
//! `sequence` numbers. Keys stay inside the engine and are returned in
//! `MutationCounters::cohort_keys` for consumer bookkeeping.
//!
//! Accounts never receive the key or carry `ProtocolSalt` onto the wire. Consumer
//! factories may attach bookkeeping payloads; the engine preserves the salt
//! across retries without interpreting it.

use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{ProtocolKind, RunId};
use uuid::Uuid;

/// Engine-local identity for a bulk mutation cohort, frozen across retries.
///
/// Distinct from `bifrost_types::MutationConcurrency::StateBased`: that is
/// optimistic concurrency enforced on the wire. This key only correlates the
/// engine's attempts at one cohort; it neither deduplicates server writes nor
/// authorizes replay. Accounts never receive it. Uncertain writes still require
/// the engine's read-back guard and declared desired-state reassertion policy.
#[derive(Debug, Clone)]
pub struct IdempotencyKey {
    pub run_id: RunId,
    pub sequence: u64,
    pub protocol_salt: ProtocolSalt,
}

/// Protocol-tagged salt used by the engine's idempotency bookkeeping.
///
/// Carried alongside `RunId` and `sequence` for protocol-specific correlation.
/// The protocol is the campaign's own, stamped by `IdempotencyVendor`; a
/// consumer factory supplies only the payload, as bookkeeping identity, not
/// cursor provenance. The engine preserves the salt across retries without
/// interpreting it; accounts never receive it. No supported protocol has a
/// wire replay token (JMAP `ifInState`, Gmail `X-Goog-Request-Id` and Graph
/// `client-request-id` are not dedup primitives), so the salt never reaches
/// the wire.
///
/// This is the protocol plus a payload rather than a parallel per-protocol
/// enum: a parallel enum needs a mapping from the non-exhaustive
/// `ProtocolKind`, and that mapping's mandatory catch-all would silently give
/// a new protocol another protocol's salt.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProtocolSalt {
    protocol: ProtocolKind,
    payload: String,
}

impl ProtocolSalt {
    /// The campaign protocol the vendor stamped.
    #[must_use]
    pub fn protocol(&self) -> ProtocolKind {
        self.protocol
    }

    /// The consumer factory's payload; empty under the default factory.
    #[must_use]
    pub fn payload(&self) -> &str {
        &self.payload
    }
}

/// Consumer payload hook: given the campaign protocol, returns the salt
/// payload. The protocol itself is not the hook's to choose.
pub type SaltPayloadFactory = Box<dyn Fn(ProtocolKind) -> String + Send + Sync>;

/// Consumer-facing campaign identifier. Wraps a `Uuid` so engine code
/// can correlate vendor + retry queue + counters in one map.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MutationCampaignId(pub Uuid);

impl MutationCampaignId {
    /// Mint a fresh campaign id.
    #[must_use]
    pub fn fresh() -> Self {
        Self(Uuid::new_v4())
    }
}

/// Vends `IdempotencyKey`s for a mutation campaign's throttle cohorts.
/// Each cohort allocates once and retains its key across retries.
///
/// Callers that need correlation across process restarts own persistence of
/// `run_id` and supply it through `with_run_id` when resuming. This vendor
/// keeps the supplied identity in memory; it does not persist campaign state.
pub struct IdempotencyVendor {
    run_id: RunId,
    sequence: AtomicU64,
    // A consumer payload hook, not a second protocol registry.
    salt_for: SaltPayloadFactory,
}

impl IdempotencyVendor {
    /// Construct a vendor with a freshly-minted `run_id`. Callers that
    /// resume a campaign across process restarts must instead use
    /// `with_run_id`.
    #[must_use]
    pub fn fresh(salt_for: SaltPayloadFactory) -> Self {
        Self::with_run_id(RunId(Uuid::new_v4().to_string()), salt_for)
    }

    #[must_use]
    pub fn with_run_id(run_id: RunId, salt_for: SaltPayloadFactory) -> Self {
        Self {
            run_id,
            sequence: AtomicU64::new(0),
            salt_for,
        }
    }

    #[must_use]
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    /// Mint the next key for `protocol`. Atomic + monotonic.
    pub fn next(&self, protocol: ProtocolKind) -> IdempotencyKey {
        IdempotencyKey {
            run_id: self.run_id.clone(),
            sequence: self.sequence.fetch_add(1, Ordering::AcqRel),
            protocol_salt: ProtocolSalt {
                protocol,
                payload: (self.salt_for)(protocol),
            },
        }
    }
}

/// Default bookkeeping salt factory: an empty payload for every protocol.
pub fn default_salt_factory() -> SaltPayloadFactory {
    Box::new(|_| String::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_is_monotonic() {
        let v = IdempotencyVendor::fresh(default_salt_factory());
        let a = v.next(ProtocolKind::Jmap);
        let b = v.next(ProtocolKind::Jmap);
        assert_eq!(a.run_id, b.run_id);
        assert!(b.sequence > a.sequence);
    }

    #[test]
    fn run_id_persists_across_mints() {
        let v = IdempotencyVendor::with_run_id(RunId("fixed".into()), default_salt_factory());
        let a = v.next(ProtocolKind::Imap);
        let b = v.next(ProtocolKind::Imap);
        assert_eq!(a.run_id, RunId("fixed".into()));
        assert_eq!(b.run_id, RunId("fixed".into()));
    }

    #[test]
    fn custom_payload_is_preserved_for_consumer_bookkeeping() {
        let vendor = IdempotencyVendor::with_run_id(
            RunId("custom-run".into()),
            Box::new(|_| "consumer-correlation".into()),
        );
        let key = vendor.next(ProtocolKind::Jmap);
        assert_eq!(key.run_id, RunId("custom-run".into()));
        assert_eq!(key.protocol_salt.protocol(), ProtocolKind::Jmap);
        assert_eq!(key.protocol_salt.payload(), "consumer-correlation");
    }

    /// The vendor stamps the campaign's own protocol; no factory can
    /// substitute another, and no protocol falls through to a shared salt.
    #[test]
    fn every_protocol_salt_carries_the_campaign_protocol() {
        let vendor = IdempotencyVendor::fresh(Box::new(|kind| format!("{kind:?}")));
        for kind in [
            ProtocolKind::Jmap,
            ProtocolKind::Gmail,
            ProtocolKind::Graph,
            ProtocolKind::Imap,
            ProtocolKind::CardDav,
            ProtocolKind::CalDav,
        ] {
            let salt = vendor.next(kind).protocol_salt;
            assert_eq!(salt.protocol(), kind);
            assert_eq!(salt.payload(), format!("{kind:?}"));
        }
    }
}
