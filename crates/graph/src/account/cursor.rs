use std::time::{SystemTime, UNIX_EPOCH};

use bifrost_types::{
    ChangeCursor, CursorScope, FolderId, ObjectType, OpaqueChangeState, OpaqueProgressBytes,
    ProtocolKind,
};
use serde::{Deserialize, Serialize};

/// Internal cursor-layer errors. Callers translate these to
/// `AccountError` via `cursor_error_to_account_error` before
/// emitting them at the account boundary.
#[derive(Debug)]
pub(crate) enum CursorError {
    /// Cursor belongs to a different protocol.
    ProtocolMismatch,
    /// Cursor envelope version is ahead of what this build understands.
    EnvelopeUnknown,
    /// Cursor envelope version is older than this build can migrate.
    SchemaIncompatible,
    /// Cursor operation is not supported for this scope.
    Unsupported,
    /// Serialization / deserialization error.
    Encode(String),
}

impl std::fmt::Display for CursorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProtocolMismatch => f.write_str("cursor protocol mismatch"),
            Self::EnvelopeUnknown => f.write_str("cursor envelope version unknown"),
            Self::SchemaIncompatible => f.write_str("cursor schema incompatible"),
            Self::Unsupported => f.write_str("unsupported cursor scope"),
            Self::Encode(msg) => write!(f, "cursor encode/decode error: {msg}"),
        }
    }
}

pub(crate) const GRAPH_CURSOR_ENVELOPE_VERSION: u32 = 1;
pub(crate) const CHANGE_CURSOR_ENVELOPE_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) enum GraphCursorKind {
    Messages { folder_id: String },
    Events { calendar_id: String },
    Contacts { folder_id: String },
}

// Graph delta cursors need their nextLink plus last id inside the shared opaque progress bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GraphPageMarker {
    pub(crate) next_link: String,
    pub(crate) last_seen_id: Option<String>,
}

// Graph deltaLink payloads are protocol-specific and intentionally live inside OpaqueChangeState.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GraphCursorPayload {
    pub(crate) kind: GraphCursorKind,
    pub(crate) delta_link: String,
    pub(crate) issued_at_unix_secs: u64,
    #[serde(default)]
    pub(crate) advanced_through: Option<GraphPageMarker>,
}

impl GraphCursorPayload {
    pub(crate) fn new(
        kind: GraphCursorKind,
        delta_link: String,
        advanced_through: Option<GraphPageMarker>,
    ) -> Self {
        Self {
            kind,
            delta_link,
            issued_at_unix_secs: now_unix_secs(),
            advanced_through,
        }
    }

    pub(crate) fn resume_url(&self) -> &str {
        self.advanced_through
            .as_ref()
            .map_or(self.delta_link.as_str(), |marker| marker.next_link.as_str())
    }
}

pub(crate) fn kind_for_scope(scope: &CursorScope) -> Result<GraphCursorKind, CursorError> {
    match scope {
        CursorScope::FolderType { folder, ty } => match ty {
            ObjectType::Email => Ok(GraphCursorKind::Messages {
                folder_id: folder.0.clone(),
            }),
            ObjectType::Event | ObjectType::CalendarEvent => Ok(GraphCursorKind::Events {
                calendar_id: folder.0.clone(),
            }),
            ObjectType::Contact => Ok(GraphCursorKind::Contacts {
                folder_id: folder.0.clone(),
            }),
            _ => Err(CursorError::Unsupported),
        },
        _ => Err(CursorError::Unsupported),
    }
}

pub(crate) fn scope_for_kind(kind: &GraphCursorKind) -> CursorScope {
    match kind {
        GraphCursorKind::Messages { folder_id } => CursorScope::FolderType {
            folder: FolderId(folder_id.clone()),
            ty: ObjectType::Email,
        },
        GraphCursorKind::Events { calendar_id } => CursorScope::FolderType {
            folder: FolderId(calendar_id.clone()),
            ty: ObjectType::Event,
        },
        GraphCursorKind::Contacts { folder_id } => CursorScope::FolderType {
            folder: FolderId(folder_id.clone()),
            ty: ObjectType::Contact,
        },
    }
}

pub(crate) fn encode_cursor(
    scope: CursorScope,
    payload: GraphCursorPayload,
) -> Result<ChangeCursor, CursorError> {
    let progress = payload
        .advanced_through
        .as_ref()
        .map(encode_page_marker)
        .transpose()?;
    let bytes =
        serde_json::to_vec(&payload).map_err(|error| CursorError::Encode(error.to_string()))?;
    Ok(ChangeCursor {
        scope,
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::Graph,
            envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION,
            bytes,
        },
        advanced_through: progress,
        envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
    })
}

pub(crate) fn decode_cursor(cursor: &ChangeCursor) -> Result<GraphCursorPayload, CursorError> {
    if cursor.server_state.protocol != ProtocolKind::Graph {
        return Err(CursorError::ProtocolMismatch);
    }
    if cursor.server_state.envelope_version > GRAPH_CURSOR_ENVELOPE_VERSION {
        return Err(CursorError::EnvelopeUnknown);
    }
    if cursor.server_state.envelope_version < GRAPH_CURSOR_ENVELOPE_VERSION {
        return Err(CursorError::SchemaIncompatible);
    }

    let mut payload: GraphCursorPayload = serde_json::from_slice(&cursor.server_state.bytes)
        .map_err(|error| CursorError::Encode(error.to_string()))?;
    if let Some(progress) = cursor.advanced_through.as_ref() {
        payload.advanced_through = Some(decode_page_marker(progress)?);
    }
    Ok(payload)
}

pub(crate) fn encode_page_marker(
    marker: &GraphPageMarker,
) -> Result<OpaqueProgressBytes, CursorError> {
    serde_json::to_vec(marker)
        .map(OpaqueProgressBytes)
        .map_err(|error| CursorError::Encode(error.to_string()))
}

pub(crate) fn decode_page_marker(
    progress: &OpaqueProgressBytes,
) -> Result<GraphPageMarker, CursorError> {
    serde_json::from_slice(&progress.0).map_err(|error| CursorError::Encode(error.to_string()))
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_cursor_round_trips() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let marker = GraphPageMarker {
            next_link: "https://graph.example/next".to_string(),
            last_seen_id: Some("m1".to_string()),
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&scope).expect("scope should map"),
            "https://graph.example/delta".to_string(),
            Some(marker.clone()),
        );

        let cursor = encode_cursor(scope.clone(), payload).expect("cursor should encode");
        assert_eq!(cursor.scope, scope);
        assert_eq!(cursor.server_state.protocol, ProtocolKind::Graph);
        assert_eq!(
            cursor.server_state.envelope_version,
            GRAPH_CURSOR_ENVELOPE_VERSION
        );

        let decoded = decode_cursor(&cursor).expect("cursor should decode");
        assert_eq!(decoded.advanced_through, Some(marker));
        assert_eq!(decoded.resume_url(), "https://graph.example/next");
    }

    #[test]
    fn rejects_other_protocol_cursor() {
        let cursor = ChangeCursor {
            scope: CursorScope::Account,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Gmail,
                envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION,
                bytes: Vec::new(),
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };

        assert!(matches!(
            decode_cursor(&cursor),
            Err(CursorError::ProtocolMismatch)
        ));
    }

    #[test]
    fn rejects_future_envelope_version() {
        let cursor = ChangeCursor {
            scope: CursorScope::Account,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Graph,
                envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION + 1,
                bytes: b"{}".to_vec(),
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };

        assert!(matches!(
            decode_cursor(&cursor),
            Err(CursorError::EnvelopeUnknown)
        ));
    }

    #[test]
    fn rejects_past_envelope_version_as_schema_incompatible() {
        let cursor = ChangeCursor {
            scope: CursorScope::Account,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Graph,
                envelope_version: 0,
                bytes: b"{}".to_vec(),
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };

        assert!(matches!(
            decode_cursor(&cursor),
            Err(CursorError::SchemaIncompatible)
        ));
    }

    #[test]
    fn rejects_truncated_payload_bytes() {
        let cursor = ChangeCursor {
            scope: CursorScope::Account,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Graph,
                envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION,
                bytes: b"{not-json".to_vec(),
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };

        assert!(matches!(
            decode_cursor(&cursor),
            Err(CursorError::Encode(_))
        ));
    }

    #[test]
    fn rejects_truncated_progress_bytes() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&scope).expect("scope should map"),
            "https://graph.example/delta".to_string(),
            None,
        );
        let cursor = ChangeCursor {
            scope: scope.clone(),
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Graph,
                envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION,
                bytes: serde_json::to_vec(&payload).expect("serialize"),
            },
            advanced_through: Some(OpaqueProgressBytes(b"{nope".to_vec())),
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };

        assert!(matches!(
            decode_cursor(&cursor),
            Err(CursorError::Encode(_))
        ));
    }

    #[test]
    fn kind_for_scope_rejects_unsupported_object_type() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Mailbox,
        };
        assert!(matches!(
            kind_for_scope(&scope),
            Err(CursorError::Unsupported)
        ));
    }

    #[test]
    fn kind_for_scope_rejects_non_folder_type_scope() {
        assert!(matches!(
            kind_for_scope(&CursorScope::Account),
            Err(CursorError::Unsupported)
        ));
    }
}
