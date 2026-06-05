use bifrost_types::{
    AccountCapabilities, BatchingPolicy, BlobRangeSupport, ConvenienceShape, CursorFreshness,
    FilterRuleShape, MutationCapabilities, MutationConcurrency, MutationReplaySafety,
    PimMethodSupport, PushCapability, QuotaSignal, RateLimitClass,
};

pub(crate) fn caldav_capabilities() -> AccountCapabilities {
    AccountCapabilities {
        cursor_freshness: CursorFreshness::Hybrid,
        blob_range: BlobRangeSupport::No,
        blob_digest_pre_download: false,
        push: PushCapability::None,
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::StateBased,
            replay_safety: MutationReplaySafety::None,
        },
        batching_policy: BatchingPolicy {
            max_items: 0,
            max_wait: Default::default(),
            flush_on_input_close: false,
        },
        rate_limit_class: RateLimitClass::Generous,
        quota_signal: QuotaSignal::Implicit,
        requires_uidvalidity_recheck: false,
        historyid_expires_after: None,
        delta_token_expires_after: None,
        pim_methods: PimMethodSupport {
            calendars_list: true,
            events_in_range: true,
            event_get: true,
            event_create: true,
            event_update: true,
            event_delete: true,
            event_rsvp: true,
            event_search: true,
            event_autocomplete: true,
            ..PimMethodSupport::default()
        },
        filter_rule_shape: FilterRuleShape::None,
        conveniences: ConvenienceShape::default(),
    }
}

#[cfg(test)]
mod tests {
    use bifrost_types::{BlobRangeSupport, CursorFreshness, PushCapability};

    use super::*;

    #[test]
    fn capability_builder_matches_calendar_shape() {
        let caps = caldav_capabilities();
        assert_eq!(caps.cursor_freshness, CursorFreshness::Hybrid);
        assert_eq!(caps.batching_policy.max_items, 0);
        assert!(!caps.batching_policy.flush_on_input_close);
        assert_eq!(caps.blob_range, BlobRangeSupport::No);
        assert_eq!(caps.push, PushCapability::None);
        assert!(caps.pim_methods.calendars_list);
        assert!(caps.pim_methods.events_in_range);
        assert!(caps.pim_methods.event_get);
        assert!(caps.pim_methods.event_create);
        assert!(caps.pim_methods.event_update);
        assert!(caps.pim_methods.event_delete);
        assert!(caps.pim_methods.event_rsvp);
        assert!(caps.pim_methods.event_search);
    }
}
