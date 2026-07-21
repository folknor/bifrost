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
    Messages {
        folder_id: String,
    },
    Events {
        calendar_id: String,
    },
    Contacts {
        folder_id: String,
    },
    /// Public folder: no delta token. Routing + poll state live here.
    /// The enclosing `GraphCursorPayload`'s `delta_link` /
    /// `advanced_through` fields are unused for this kind (a public
    /// folder is never a server-issued delta cursor).
    PublicFolder(PublicFolderCursor),
}

/// The maximum live-id snapshot a public-folder cursor carries. The
/// snapshot rides inside the opaque cursor the consumer re-persists on
/// every checkpoint, so an unbounded vector would be a steady write-
/// amplification tax. Above the cap the folder degrades to additions-
/// only (no deletion reconcile) and stores an empty snapshot.
pub(crate) const PUBLIC_FOLDER_LIVE_IDS_CAP: usize = 10_000;

/// How long to wait between full-id deletion reconcile scans, in
/// seconds (ported from ratatoskr `DELETION_SCAN_INTERVAL_SECS`).
pub(crate) const FULL_SCAN_INTERVAL_SECS: u64 = 3600;

/// Public-folder cursor: the entire sync state for a no-delta-token
/// public folder. The watermark drives the incremental timestamp poll;
/// `last_full_scan_at` throttles the deletion reconcile; `live_ids` is
/// the deletion baseline the next scan diffs against. All of it rides
/// in the opaque cursor so a cold resume reconstructs everything with
/// no engine-side side table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PublicFolderCursor {
    /// Native EWS folder id of the public folder.
    pub(crate) folder_id: String,
    /// Routing context, reconstructable on a cold resume.
    pub(crate) routing: PublicFolderRouting,
    /// High-water `DateTimeReceived` (RFC-3339 UTC). `None` before the
    /// first poll.
    pub(crate) watermark: Option<String>,
    /// Unix seconds of the last full-id deletion scan. `None` if never
    /// scanned.
    pub(crate) last_full_scan_at: Option<u64>,
    /// Authoritative live-id snapshot from the last full scan, the
    /// deletion baseline the next scan diffs against. Empty when the
    /// last scan exceeded `PUBLIC_FOLDER_LIVE_IDS_CAP` - in that
    /// degraded state the folder syncs additions only and reconcile is
    /// skipped.
    #[serde(default)]
    pub(crate) live_ids: Vec<String>,
    /// Ids whose `received_at` equals the current `watermark`, captured
    /// on the last incremental poll. The incremental `FindItem`
    /// restriction is `>=` (so items sharing the boundary second are not
    /// missed), which means the boundary item(s) re-appear on every
    /// subsequent poll. Skipping ids in this set prevents a quiet folder
    /// from re-emitting its newest item as `Updated`+`Added` forever,
    /// while still admitting a genuinely new item that lands on the same
    /// boundary second. Additive (`serde(default)`); a v1 cursor without
    /// it just re-emits the boundary once, then converges.
    #[serde(default)]
    pub(crate) boundary_ids: Vec<String>,
    /// Whether the folder is in the over-cap additions-only degraded
    /// mode (deletion reconcile disabled). Distinct from "empty
    /// `live_ids`", which is ambiguous (an empty folder also has none).
    /// Tracking it explicitly lets the changes path warn only on the
    /// *transition* into degraded mode instead of re-warning on every
    /// hourly scan. Additive (`serde(default)` -> `false`).
    #[serde(default)]
    pub(crate) degraded: bool,
    /// Item-class local-names (`Task`, `PostItem`, ...) already surfaced
    /// in an unhandled-class `Warning` for this folder. Without this the
    /// same class re-warns on every poll (the operator sees identical
    /// noise hourly); recording the already-reported classes lets a poll
    /// warn only on NEWLY-seen classes. Both the incremental poll and the
    /// full scan contribute observed classes. Additive (`serde(default)`
    /// -> empty; a v1 cursor re-warns once, then converges).
    #[serde(default)]
    pub(crate) warned_classes: Vec<String>,
}

/// Public-folder EWS routing context. Carried in the cursor so the
/// hierarchy/content mailbox pair survives a cold resume. Mirrors the
/// `X-AnchorMailbox` / `X-PublicFolderMailbox` routing pair EWS expects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PublicFolderRouting {
    /// `X-AnchorMailbox` value: the content mailbox SMTP for content
    /// ops, the hierarchy mailbox for hierarchy (FindFolder) ops.
    pub(crate) anchor_mailbox: String,
    /// `X-PublicFolderMailbox` value: the content mailbox (content ops)
    /// or the hierarchy server (hierarchy ops). `None` when unset.
    pub(crate) public_folder_mailbox: Option<String>,
}

impl PublicFolderRouting {
    /// Materialize the EWS routing headers for a request against this
    /// public folder. Owned->owned clone of both fields.
    pub(crate) fn headers(&self) -> crate::ews::EwsHeaders {
        crate::ews::EwsHeaders {
            anchor_mailbox: Some(self.anchor_mailbox.clone()),
            public_folder_mailbox: self.public_folder_mailbox.clone(),
        }
    }
}

/// Apply the live-id cap: returns the scan set when at/below `cap`, or
/// `None` (degrade to additions-only, store no snapshot) when above it.
/// Pure so the cap decision is unit-pinnable.
pub(crate) fn cap_live_ids(ids: Vec<String>, cap: usize) -> Option<Vec<String>> {
    if ids.len() > cap { None } else { Some(ids) }
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

    /// Build a public-folder payload. There is no delta link, so the
    /// delta-specific fields are left empty; the entire state lives in
    /// the `PublicFolderCursor` carried by the `kind`.
    pub(crate) fn public_folder(cursor: PublicFolderCursor) -> Self {
        Self {
            kind: GraphCursorKind::PublicFolder(cursor),
            delta_link: String::new(),
            issued_at_unix_secs: now_unix_secs(),
            advanced_through: None,
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
        // A public folder surfaces as a bare `CursorScope::Folder`
        // carrying the native EWS folder id; all routing rides in the
        // cursor payload, not the scope.
        GraphCursorKind::PublicFolder(pf) => CursorScope::Folder(FolderId(pf.folder_id.clone())),
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

    fn public_folder_cursor() -> PublicFolderCursor {
        PublicFolderCursor {
            folder_id: "AAMkPF=".to_string(),
            routing: PublicFolderRouting {
                anchor_mailbox: "content@contoso.com".to_string(),
                public_folder_mailbox: Some("pf@contoso.com".to_string()),
            },
            watermark: Some("2026-03-01T10:00:00Z".to_string()),
            last_full_scan_at: Some(1_700_000_000),
            live_ids: vec!["a".to_string(), "b".to_string()],
            boundary_ids: vec!["b".to_string()],
            degraded: false,
            warned_classes: vec!["Task".to_string()],
        }
    }

    #[test]
    fn public_folder_cursor_round_trips() {
        let pf = public_folder_cursor();
        let scope = CursorScope::Folder(FolderId(pf.folder_id.clone()));
        let payload = GraphCursorPayload::public_folder(pf.clone());

        let cursor = encode_cursor(scope.clone(), payload).expect("encode");
        assert_eq!(cursor.scope, scope);
        assert_eq!(cursor.server_state.protocol, ProtocolKind::Graph);
        assert_eq!(
            cursor.server_state.envelope_version,
            GRAPH_CURSOR_ENVELOPE_VERSION
        );

        let decoded = decode_cursor(&cursor).expect("decode");
        match decoded.kind {
            GraphCursorKind::PublicFolder(got) => assert_eq!(got, pf),
            other => panic!("expected PublicFolder kind, got {other:?}"),
        }
        // `scope_for_kind` round-trips the native folder id.
        let kind = GraphCursorKind::PublicFolder(pf.clone());
        assert_eq!(scope_for_kind(&kind), scope);
    }

    #[test]
    fn old_message_cursor_still_decodes_after_public_folder_variant() {
        // A `Messages` cursor encoded with the same envelope version
        // still decodes unchanged - proof the additive `PublicFolder`
        // variant did not break the existing kinds (no version bump).
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&scope).expect("scope should map"),
            "https://graph.example/delta".to_string(),
            None,
        );
        let cursor = encode_cursor(scope, payload).expect("encode");
        let decoded = decode_cursor(&cursor).expect("decode");
        assert!(matches!(decoded.kind, GraphCursorKind::Messages { .. }));
    }

    #[test]
    fn live_ids_capped_degrades_to_additions_only() {
        let under: Vec<String> = (0..3).map(|i| i.to_string()).collect();
        assert_eq!(cap_live_ids(under.clone(), 3), Some(under));

        let over: Vec<String> = (0..5).map(|i| i.to_string()).collect();
        assert_eq!(cap_live_ids(over, 3), None);
    }
}
