//! Graph error helpers.
//!
//! The substring-matching recovery helpers that used to live here
//! (`graph_error_to_fatal`, `recovery_for_graph_error`,
//! `fatal_from_recovery`, `mutation_outcome_for_status`) were removed
//! when Phase 2.2 introduced the structured Graph error boundary.
//! Classification is now driven entirely by the typed `GraphSignal`
//! in `crate::error` and the `into_account_error` translation in
//! `account::graph_error`.

use bifrost_types::{DiagnosticText, ObjectId, Warning, WarningKind};

pub(crate) fn warning_blob_not_byte_stream(id: &ObjectId) -> Warning {
    Warning {
        kind: WarningKind::BlobNotByteStream,
        message: DiagnosticText::user_safe(format!(
            "Graph attachment for object {} is not a byte stream",
            id.0
        )),
        retry_count: 0,
        next_action: None,
        protocol_detail: None,
    }
}
