use std::time::Duration;

use bifrost_types::{
    AccountCapabilities, BatchingPolicy, BlobRangeSupport, CursorFreshness, Error,
    MutationCapabilities, MutationConcurrency, MutationReplaySafety, PushCapability, QuotaSignal,
    RateLimitClass,
};

use crate::core::session::Session;

#[derive(Debug, Clone, Copy)]
pub(crate) struct CoreLimits {
    pub(crate) max_objects_in_get: usize,
    pub(crate) max_objects_in_set: usize,
}

pub(crate) fn build(session: &Session) -> Result<(AccountCapabilities, CoreLimits), Error> {
    let core = session
        .core_capabilities()
        .ok_or(Error::MissingCoreCapability)?;

    if core.max_calls_in_request() == 0
        || core.max_objects_in_get() == 0
        || core.max_objects_in_set() == 0
        || core.max_size_request() == 0
    {
        return Err(Error::MissingCoreCapability);
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

        let (caps, limits) = build(&session).unwrap();
        assert_eq!(caps.cursor_freshness, CursorFreshness::ServerIssued);
        assert_eq!(caps.push, PushCapability::InProcess);
        assert_eq!(caps.blob_range, BlobRangeSupport::No);
        assert_eq!(caps.mutation.concurrency, MutationConcurrency::StateBased);
        assert_eq!(caps.mutation.replay_safety, MutationReplaySafety::None);
        assert_eq!(caps.batching_policy.max_items, 500);
        assert_eq!(limits.max_objects_in_get, 256);
        assert_eq!(limits.max_objects_in_set, 700);
    }
}
