use std::time::Duration;

use bifrost_types::{
    AccountCapabilities, BatchingPolicy, BlobRangeSupport, CursorFreshness, MutationCapabilities,
    MutationConcurrency, MutationReplaySafety, PushCapability, QuotaSignal, RateLimitClass,
};

pub(crate) const GMAIL_BATCH_MODIFY_LIMIT: usize = 1000;

pub(crate) fn gmail_capabilities() -> AccountCapabilities {
    AccountCapabilities {
        cursor_freshness: CursorFreshness::ServerIssued,
        blob_range: BlobRangeSupport::No,
        blob_digest_pre_download: false,
        push: PushCapability::OutOfProcessPubsub,
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::None,
            replay_safety: MutationReplaySafety::None,
        },
        batching_policy: BatchingPolicy {
            max_items: GMAIL_BATCH_MODIFY_LIMIT,
            max_wait: Duration::from_millis(75),
            flush_on_input_close: true,
        },
        rate_limit_class: RateLimitClass::Tiered,
        quota_signal: QuotaSignal::QuotaUnits,
        requires_uidvalidity_recheck: false,
        historyid_expires_after: None,
        delta_token_expires_after: None,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bifrost_types::{
        BlobRangeSupport, CursorFreshness, MutationConcurrency, MutationReplaySafety,
        PushCapability, QuotaSignal, RateLimitClass,
    };

    use super::*;

    #[test]
    fn capability_builder_matches_gmail_posture() {
        let caps = gmail_capabilities();
        assert_eq!(caps.cursor_freshness, CursorFreshness::ServerIssued);
        assert_eq!(caps.blob_range, BlobRangeSupport::No);
        assert!(!caps.blob_digest_pre_download);
        assert_eq!(caps.push, PushCapability::OutOfProcessPubsub);
        assert!(!caps.push_in_process());
        assert_eq!(caps.mutation.concurrency, MutationConcurrency::None);
        assert_eq!(caps.mutation.replay_safety, MutationReplaySafety::None);
        assert_eq!(caps.batching_policy.max_items, 1000);
        assert!(caps.historyid_expires_after.is_none());
    }

    #[test]
    fn batching_policy_matches_gmail_batch_modify_limit() {
        let caps = gmail_capabilities();
        assert_eq!(caps.batching_policy.max_items, GMAIL_BATCH_MODIFY_LIMIT);
        assert_eq!(caps.batching_policy.max_wait, Duration::from_millis(75));
        assert!(caps.batching_policy.flush_on_input_close);
    }

    #[test]
    fn rate_limit_and_quota_match_gmail_quota_units() {
        let caps = gmail_capabilities();
        assert_eq!(caps.rate_limit_class, RateLimitClass::Tiered);
        assert_eq!(caps.quota_signal, QuotaSignal::QuotaUnits);
    }

    #[test]
    fn no_uidvalidity_recheck_or_delta_token() {
        let caps = gmail_capabilities();
        assert!(!caps.requires_uidvalidity_recheck);
        assert!(caps.delta_token_expires_after.is_none());
    }
}
