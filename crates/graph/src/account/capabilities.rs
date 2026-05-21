use std::time::Duration;

use bifrost_types::{
    AccountCapabilities, BatchingPolicy, BlobRangeSupport, CursorFreshness, MutationCapabilities,
    MutationConcurrency, MutationReplaySafety, PushCapability, QuotaSignal, RateLimitClass,
};

use super::PushMode;

pub(crate) fn build_capabilities(push_mode: PushMode) -> AccountCapabilities {
    AccountCapabilities {
        cursor_freshness: CursorFreshness::ServerIssued,
        blob_range: BlobRangeSupport::Conditional,
        blob_digest_pre_download: false,
        push: match push_mode {
            PushMode::GraphSubscriptions => PushCapability::WebhookOrEwsStream,
            PushMode::EwsStreaming => PushCapability::InProcess,
        },
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::StateBased,
            replay_safety: MutationReplaySafety::None,
        },
        batching_policy: BatchingPolicy {
            max_items: 20,
            max_wait: Duration::from_millis(100),
            flush_on_input_close: true,
        },
        rate_limit_class: RateLimitClass::Tiered,
        quota_signal: QuotaSignal::RetryAfter,
        requires_uidvalidity_recheck: false,
        historyid_expires_after: None,
        delta_token_expires_after: None,
    }
}

#[cfg(test)]
mod tests {
    use bifrost_types::{BlobRangeSupport, MutationConcurrency, PushCapability};

    use super::*;

    #[test]
    fn graph_subscription_capabilities_are_out_of_process() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert_eq!(caps.blob_range, BlobRangeSupport::Conditional);
        assert_eq!(caps.push, PushCapability::WebhookOrEwsStream);
        assert!(!caps.push_in_process());
        assert_eq!(caps.mutation.concurrency, MutationConcurrency::StateBased);
        assert_eq!(caps.batching_policy.max_items, 20);
        assert!(caps.delta_token_expires_after.is_none());
    }

    #[test]
    fn ews_capabilities_are_in_process() {
        let caps = build_capabilities(PushMode::EwsStreaming);
        assert_eq!(caps.push, PushCapability::InProcess);
        assert!(caps.push_in_process());
    }
}
