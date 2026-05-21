use std::time::Duration;

use bifrost_types::{
    AccountCapabilities, BatchingPolicy, BlobRangeSupport, CursorFreshness, MutationCapabilities,
    MutationConcurrency, MutationReplaySafety, PushCapability, QuotaSignal, RateLimitClass,
};

use crate::types::{Capability, ServerProfile};

pub(crate) fn build_capabilities(profile: &ServerProfile) -> AccountCapabilities {
    AccountCapabilities {
        cursor_freshness: CursorFreshness::Hybrid,
        blob_range: BlobRangeSupport::Yes,
        blob_digest_pre_download: false,
        push: if profile.supports(Capability::Idle) {
            PushCapability::InProcess
        } else {
            PushCapability::None
        },
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::None,
            replay_safety: MutationReplaySafety::None,
        },
        batching_policy: BatchingPolicy {
            max_items: 1024,
            max_wait: Duration::from_millis(50),
            flush_on_input_close: true,
        },
        rate_limit_class: RateLimitClass::PerConnection,
        quota_signal: if profile.supports(Capability::Quota) {
            QuotaSignal::Implicit
        } else {
            QuotaSignal::None
        },
        requires_uidvalidity_recheck: true,
        historyid_expires_after: None,
        delta_token_expires_after: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_builder_sets_imap_tier_values() {
        let profile = ServerProfile::new(
            vec![Capability::Idle, Capability::Condstore, Capability::Quota],
            Vec::new(),
        );
        let caps = build_capabilities(&profile);
        assert_eq!(caps.cursor_freshness, CursorFreshness::Hybrid);
        assert_eq!(caps.blob_range, BlobRangeSupport::Yes);
        assert_eq!(caps.push, PushCapability::InProcess);
        assert_eq!(caps.mutation.concurrency, MutationConcurrency::None);
        assert_eq!(caps.mutation.replay_safety, MutationReplaySafety::None);
        assert!(caps.requires_uidvalidity_recheck);
    }
}
