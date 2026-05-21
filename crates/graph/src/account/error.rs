use std::time::Duration;

use bifrost_types::{
    CursorScope, Error, Fatal, MutationOutcome, ObjectId, RecoveryClass, Warning, WarningKind,
};

const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(30);

pub(crate) fn graph_error_to_fatal(message: impl Into<String>, scope: CursorScope) -> Fatal {
    let message = message.into();
    let recovery = recovery_for_graph_error(&message, &scope).unwrap_or_else(|| {
        if message.contains("401") || message.to_ascii_lowercase().contains("unauthorized") {
            RecoveryClass::AuthLost
        } else {
            RecoveryClass::Retry {
                after: DEFAULT_RETRY_AFTER,
            }
        }
    });
    Fatal {
        recovery,
        message,
        source: None,
    }
}

pub(crate) fn recovery_for_graph_error(
    message: &str,
    scope: &CursorScope,
) -> Option<RecoveryClass> {
    let lower = message.to_ascii_lowercase();
    if message.contains("410") || lower.contains("gone") {
        return Some(RecoveryClass::RestartScope(scope.clone()));
    }
    if message.contains("400")
        && (lower.contains("invaliddeltatoken")
            || lower.contains("invalid delta token")
            || lower.contains("syncstatenotfound"))
    {
        return Some(RecoveryClass::RestartScope(scope.clone()));
    }
    if message.contains("429") || lower.contains("too many requests") {
        return Some(RecoveryClass::Retry {
            after: DEFAULT_RETRY_AFTER,
        });
    }
    if message.contains("503") || message.contains("504") {
        return Some(RecoveryClass::Retry {
            after: DEFAULT_RETRY_AFTER,
        });
    }
    if message.contains("401") || lower.contains("unauthorized") {
        return Some(RecoveryClass::AuthLost);
    }
    None
}

pub(crate) fn fatal_from_recovery(recovery: RecoveryClass, message: impl Into<String>) -> Fatal {
    Fatal {
        recovery,
        message: message.into(),
        source: None,
    }
}

pub(crate) fn warning_blob_not_byte_stream(id: &ObjectId) -> Warning {
    Warning {
        kind: WarningKind::BlobNotByteStream,
        message: format!("Graph attachment for object {} is not a byte stream", id.0),
        retry_count: 0,
        next_action: None,
        protocol_detail: None,
    }
}

pub(crate) fn mutation_outcome_for_status(
    status: u16,
    destroy: bool,
    id: &ObjectId,
) -> MutationOutcome {
    match status {
        200..=299 => MutationOutcome::Applied,
        404 if destroy => MutationOutcome::Skipped,
        412 => MutationOutcome::Skipped,
        429 => MutationOutcome::Failed(Error::Transport(format!(
            "Graph throttled mutation for {}",
            id.0
        ))),
        _ => MutationOutcome::Failed(Error::Transport(format!(
            "Graph mutation for {} failed with HTTP {status}",
            id.0
        ))),
    }
}

#[cfg(test)]
mod tests {
    use bifrost_types::{FolderId, ObjectType};

    use super::*;

    fn scope() -> CursorScope {
        CursorScope::FolderType {
            folder: FolderId("f1".to_string()),
            ty: ObjectType::Email,
        }
    }

    #[test]
    fn maps_gone_to_restart_scope() {
        let recovery = recovery_for_graph_error("Graph API error 410 Gone: expired", &scope())
            .expect("recovery expected");
        assert!(matches!(recovery, RecoveryClass::RestartScope(_)));
    }

    #[test]
    fn maps_invalid_delta_to_restart_scope() {
        let recovery = recovery_for_graph_error(
            "Graph API error 400 Bad Request: InvalidDeltaToken",
            &scope(),
        )
        .expect("recovery expected");
        assert!(matches!(recovery, RecoveryClass::RestartScope(_)));
    }

    #[test]
    fn maps_throttle_to_retry() {
        let recovery = recovery_for_graph_error("Graph API error 429 Too Many Requests", &scope())
            .expect("recovery expected");
        assert!(matches!(recovery, RecoveryClass::Retry { .. }));
    }

    #[test]
    fn maps_precondition_failed_to_skipped_mutation() {
        let id = ObjectId("m1".to_string());
        assert!(matches!(
            mutation_outcome_for_status(412, false, &id),
            MutationOutcome::Skipped
        ));
    }
}
