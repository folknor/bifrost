//! `IdempotencyKey` vending.
//!
//! Each mutation campaign mints a `RunId` once and persists it via the
//! `CheckpointStore` under a campaign-scoped key. `IdempotencyVendor`
//! then mints `IdempotencyKey`s with monotonically increasing
//! `sequence` numbers; the engine threads them through the protocol
//! crate's `bulk_*` surface for engine-side correlation.
//!
//! No protocol today carries `ProtocolSalt` onto the wire. The salt
//! is engine bookkeeping for retry-queue dedup and campaign
//! correlation.

use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{IdempotencyKey, ProtocolKind, ProtocolSalt, RunId};
use uuid::Uuid;

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

/// Vends `IdempotencyKey`s for a single mutation campaign.
///
/// `run_id` is persisted across process restarts so retries from the
/// previous process look correlated to engine bookkeeping; the
/// consumer-facing `MutationCampaignId` is the key into the
/// `CheckpointStore` slot that holds it.
pub struct IdempotencyVendor {
    run_id: RunId,
    sequence: AtomicU64,
    salt_for: Box<dyn Fn(ProtocolKind) -> ProtocolSalt + Send + Sync>,
}

impl IdempotencyVendor {
    /// Construct a vendor with a freshly-minted `run_id`. Callers that
    /// resume a campaign across process restarts must instead use
    /// `with_run_id`.
    #[must_use]
    pub fn fresh(salt_for: Box<dyn Fn(ProtocolKind) -> ProtocolSalt + Send + Sync>) -> Self {
        Self::with_run_id(RunId(Uuid::new_v4().to_string()), salt_for)
    }

    #[must_use]
    pub fn with_run_id(
        run_id: RunId,
        salt_for: Box<dyn Fn(ProtocolKind) -> ProtocolSalt + Send + Sync>,
    ) -> Self {
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
            protocol_salt: (self.salt_for)(protocol),
        }
    }
}

/// Default salt factory that returns the empty-string variant per
/// protocol. Engine-internal bookkeeping; never travels on the wire.
#[must_use]
pub fn default_salt_factory() -> Box<dyn Fn(ProtocolKind) -> ProtocolSalt + Send + Sync> {
    Box::new(|protocol| match protocol {
        ProtocolKind::Jmap => ProtocolSalt::Jmap(String::new()),
        ProtocolKind::Gmail => ProtocolSalt::Gmail(String::new()),
        ProtocolKind::Graph => ProtocolSalt::Graph(String::new()),
        ProtocolKind::Imap => ProtocolSalt::Imap,
        // `ProtocolKind` is `#[non_exhaustive]`; fall back to the IMAP
        // salt for unknown future protocols. The salt is engine
        // bookkeeping; it never travels on the wire so this is safe.
        _ => ProtocolSalt::Imap,
    })
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
}
