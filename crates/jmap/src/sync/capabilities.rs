use std::time::Duration;

use bifrost_types::{
    AccountCapabilities, AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation,
    BatchingPolicy, BlobRangeSupport, Cause, ConvenienceShape, CursorFreshness, DiagnosticText,
    MutationCapabilities, MutationConcurrency, MutationReplaySafety, PimMethodSupport, Protocol,
    PushCapability, QuotaSignal, RateLimitClass, StarredFlagShape, StateCause, SyncStateErrorKind,
};

fn missing_core_capability() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
        Cause::State(StateCause::CapabilityChanged {
            delta: bifrost_types::CapabilityDelta::default(),
        }),
    )
    .protocol(Protocol::Jmap)
    .operation(AccountOperation::Discover)
    .text(DiagnosticText::support_only(
        "JMAP session is missing or has zero-valued core limits",
    ))
    .build()
}

use crate::core::session::Session;

#[derive(Debug, Clone, Copy)]
pub(crate) struct CoreLimits {
    pub(crate) max_objects_in_get: usize,
    pub(crate) max_objects_in_set: usize,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PimSupport {
    pub(crate) submission: bool,
    pub(crate) vacation: bool,
    pub(crate) quota: bool,
}

pub(crate) fn build(
    session: &Session,
    support: PimSupport,
) -> Result<(AccountCapabilities, CoreLimits), AccountError> {
    let core = session
        .core_capabilities()
        .ok_or_else(missing_core_capability)?;

    if core.max_calls_in_request() == 0
        || core.max_objects_in_get() == 0
        || core.max_objects_in_set() == 0
        || core.max_size_request() == 0
    {
        return Err(missing_core_capability());
    }

    let ws_push = session
        .websocket_capabilities()
        .is_some_and(crate::core::session::WebSocketCapabilities::supports_push);

    let max_items = core.max_objects_in_set().clamp(1, 500);
    let caps = AccountCapabilities {
        cursor_freshness: CursorFreshness::ServerIssued,
        // The existing JMAP transport exposes whole-blob downloads only.
        // Range support needs a request hook for the Range header.
        blob_range: BlobRangeSupport::No,
        blob_digest_pre_download: false,
        push: if ws_push {
            PushCapability::InProcess
        } else {
            PushCapability::None
        },
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::StateBased,
            replay_safety: MutationReplaySafety::None,
        },
        batching_policy: BatchingPolicy {
            max_items,
            max_wait: Duration::from_millis(100),
            flush_on_input_close: true,
        },
        rate_limit_class: RateLimitClass::Generous,
        quota_signal: QuotaSignal::None,
        requires_uidvalidity_recheck: false,
        historyid_expires_after: None,
        delta_token_expires_after: None,
        pim_methods: PimMethodSupport {
            add_to_container: true,
            remove_from_container: true,
            set_keyword: true,
            set_label_membership: false,
            set_category: false,
            set_extended_property: false,
            set_is_read: true,
            send_message: support.submission,
            attachment_upload: true,
            draft_create: true,
            draft_update: true,
            draft_discard: true,
            draft_send: support.submission,
            search: true,
            search_messages: true,
            containers_list: true,
            container_create: true,
            container_rename: true,
            container_move: true,
            container_delete: true,
            identities_list: support.submission,
            identity_update: support.submission,
            vacation_get: support.vacation,
            vacation_set: support.vacation,
            quota_get: support.quota,
            thread_hydrate: true,
            message_hydrate: true,
        },
        conveniences: ConvenienceShape {
            starred: StarredFlagShape::Keyword,
            replied_via_keyword: true,
            replied_via_extended_property: false,
            forwarded_via_keyword: true,
            forwarded_via_extended_property: false,
        },
    };

    let limits = CoreLimits {
        max_objects_in_get: core.max_objects_in_get(),
        max_objects_in_set: core.max_objects_in_set(),
    };

    Ok((caps, limits))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(json: &str) -> Session {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn capability_builder_uses_core_limits_and_ws_push() {
        let session = session(
            r#"{
                "capabilities": {
                    "urn:ietf:params:jmap:core": {
                        "maxSizeUpload": 1000,
                        "maxConcurrentUpload": 2,
                        "maxSizeRequest": 100000,
                        "maxConcurrentRequests": 4,
                        "maxCallsInRequest": 8,
                        "maxObjectsInGet": 256,
                        "maxObjectsInSet": 700,
                        "collationAlgorithms": []
                    },
                    "urn:ietf:params:jmap:websocket": {
                        "url": "wss://example.test/jmap/ws",
                        "supportsPush": true
                    },
                    "urn:ietf:params:jmap:mail": {}
                },
                "accounts": {},
                "primaryAccounts": {},
                "username": "user",
                "apiUrl": "https://example.test/jmap/api",
                "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
                "uploadUrl": "https://example.test/upload/{accountId}",
                "eventSourceUrl": "https://example.test/eventsource",
                "state": "session-state"
            }"#,
        );

        let (caps, limits) = build(
            &session,
            PimSupport {
                submission: true,
                vacation: true,
                quota: true,
            },
        )
        .unwrap();
        assert_eq!(caps.cursor_freshness, CursorFreshness::ServerIssued);
        assert_eq!(caps.push, PushCapability::InProcess);
        assert_eq!(caps.blob_range, BlobRangeSupport::No);
        assert_eq!(caps.mutation.concurrency, MutationConcurrency::StateBased);
        assert_eq!(caps.mutation.replay_safety, MutationReplaySafety::None);
        assert_eq!(caps.batching_policy.max_items, 500);
        assert!(caps.pim_methods.add_to_container);
        assert!(caps.pim_methods.send_message);
        assert!(!caps.pim_methods.set_label_membership);
        assert_eq!(caps.conveniences.starred, StarredFlagShape::Keyword);
        assert!(caps.conveniences.replied_via_keyword);
        assert!(caps.conveniences.forwarded_via_keyword);
        assert_eq!(limits.max_objects_in_get, 256);
        assert_eq!(limits.max_objects_in_set, 700);
    }
}
