//! Graph error helpers.
//!
//! Classification is driven by the typed `GraphSignal` in `crate::error`
//! and the `into_account_error` translation in `account::graph_error`.

use bifrost_types::{ObjectId, Warning, WarningKind};

pub(crate) fn warning_blob_not_byte_stream(id: &ObjectId) -> Warning {
    Warning::user_safe(
        WarningKind::BlobNotByteStream,
        format!("Graph attachment for object {} is not a byte stream", id.0),
    )
}
