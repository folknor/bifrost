use std::time::Duration;

use bifrost_types::{
    AccountCapabilities, BatchingPolicy, BlobRangeSupport, CursorFreshness, MutationCapabilities,
    MutationConcurrency, MutationReplaySafety, PimMethodSupport, PushCapability, QuotaSignal,
    RateLimitClass, StarredFlagShape,
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
        pim_methods: PimMethodSupport {
            add_to_container: true,
            remove_from_container: false,
            set_keyword: false,
            set_label_membership: false,
            set_category: true,
            set_extended_property: true,
            set_is_read: true,
            send_message: true,
            attachment_upload: false,
            draft_create: true,
            draft_update: true,
            draft_discard: true,
            draft_send: true,
            search: true,
            search_messages: true,
            containers_list: true,
            container_create: true,
            container_rename: true,
            container_move: true,
            container_delete: true,
            identities_list: true,
            identity_update: false,
            vacation_get: true,
            vacation_set: true,
            quota_get: false,
            thread_hydrate: true,
            message_hydrate: true,
        },
        conveniences: bifrost_types::ConvenienceShape {
            starred: StarredFlagShape::Category,
            replied_via_keyword: false,
            replied_via_extended_property: true,
            forwarded_via_keyword: false,
            forwarded_via_extended_property: true,
        },
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

    #[test]
    fn cursor_freshness_is_server_issued() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert_eq!(caps.cursor_freshness, CursorFreshness::ServerIssued);
    }

    #[test]
    fn blob_digest_pre_download_is_false() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert!(!caps.blob_digest_pre_download);
    }

    #[test]
    fn mutation_replay_safety_is_none() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert_eq!(caps.mutation.replay_safety, MutationReplaySafety::None);
    }

    #[test]
    fn pim_method_support_matches_graph_surface() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert!(caps.pim_methods.add_to_container);
        assert!(!caps.pim_methods.remove_from_container);
        assert!(caps.pim_methods.set_category);
        assert!(caps.pim_methods.set_extended_property);
        assert!(caps.pim_methods.send_message);
        assert!(!caps.pim_methods.attachment_upload);
        assert!(caps.pim_methods.vacation_get);
        assert!(!caps.pim_methods.quota_get);
        assert_eq!(caps.conveniences.starred, StarredFlagShape::Category);
        assert!(caps.conveniences.replied_via_extended_property);
        assert!(caps.conveniences.forwarded_via_extended_property);
    }

    #[test]
    fn batching_policy_matches_graph_batch_limit() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert_eq!(caps.batching_policy.max_items, 20);
        assert_eq!(caps.batching_policy.max_wait, Duration::from_millis(100));
        assert!(caps.batching_policy.flush_on_input_close);
    }

    #[test]
    fn rate_limit_class_is_tiered() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert_eq!(caps.rate_limit_class, RateLimitClass::Tiered);
    }

    #[test]
    fn quota_signal_is_retry_after() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert_eq!(caps.quota_signal, QuotaSignal::RetryAfter);
    }

    #[test]
    fn no_uidvalidity_recheck_required() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert!(!caps.requires_uidvalidity_recheck);
    }

    #[test]
    fn historyid_expires_after_is_unset() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert!(caps.historyid_expires_after.is_none());
    }

    #[test]
    fn push_mode_does_not_affect_other_capability_fields() {
        let webhook = build_capabilities(PushMode::GraphSubscriptions);
        let ews = build_capabilities(PushMode::EwsStreaming);
        assert_eq!(webhook.cursor_freshness, ews.cursor_freshness);
        assert_eq!(webhook.blob_range, ews.blob_range);
        assert_eq!(webhook.mutation.concurrency, ews.mutation.concurrency);
        assert_eq!(webhook.mutation.replay_safety, ews.mutation.replay_safety);
        assert_eq!(
            webhook.batching_policy.max_items,
            ews.batching_policy.max_items
        );
        assert_eq!(webhook.rate_limit_class, ews.rate_limit_class);
        assert_eq!(webhook.quota_signal, ews.quota_signal);
        assert_eq!(
            webhook.delta_token_expires_after,
            ews.delta_token_expires_after
        );
    }
}
