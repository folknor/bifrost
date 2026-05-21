use bifrost_types::{
    ChangeCursor, CursorScope, Error, ObjectType, OpaqueChangeState, ProtocolKind, QueryId,
};
use serde::{Deserialize, Serialize};

pub(crate) const ENVELOPE_VERSION_V1: u32 = 1;
pub(crate) const CHANGE_CURSOR_ENVELOPE_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum JmapCursorState {
    V1 {
        scope: JmapScopeRepr,
        state_string: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum JmapScopeRepr {
    Email,
    Mailbox,
    Thread,
    Query(String),
}

impl JmapScopeRepr {
    pub(crate) fn from_cursor_scope(scope: &CursorScope) -> Result<Self, Error> {
        match scope {
            CursorScope::Type(ObjectType::Email) => Ok(Self::Email),
            CursorScope::Type(ObjectType::Mailbox) => Ok(Self::Mailbox),
            CursorScope::Type(ObjectType::Thread) => Ok(Self::Thread),
            CursorScope::Query(query) => Ok(Self::Query(query.0.clone())),
            _ => Err(Error::Unsupported),
        }
    }

    pub(crate) fn to_cursor_scope(&self) -> CursorScope {
        match self {
            Self::Email => CursorScope::Type(ObjectType::Email),
            Self::Mailbox => CursorScope::Type(ObjectType::Mailbox),
            Self::Thread => CursorScope::Type(ObjectType::Thread),
            Self::Query(query) => CursorScope::Query(QueryId(query.clone())),
        }
    }
}

pub(crate) fn encode(state: &JmapCursorState) -> OpaqueChangeState {
    OpaqueChangeState {
        protocol: ProtocolKind::Jmap,
        envelope_version: ENVELOPE_VERSION_V1,
        bytes: bincode::serialize(state).expect("JMAP cursor state serialization cannot fail"),
    }
}

pub(crate) fn encode_for_scope(
    scope: &CursorScope,
    state_string: impl Into<String>,
) -> Result<OpaqueChangeState, Error> {
    Ok(encode(&JmapCursorState::V1 {
        scope: JmapScopeRepr::from_cursor_scope(scope)?,
        state_string: state_string.into(),
    }))
}

pub(crate) fn decode(raw: &OpaqueChangeState) -> Result<JmapCursorState, Error> {
    if raw.protocol != ProtocolKind::Jmap {
        return Err(Error::CursorProtocolMismatch);
    }
    if raw.envelope_version != ENVELOPE_VERSION_V1 {
        return Err(Error::CursorEnvelopeUnknown);
    }

    bincode::deserialize(&raw.bytes).map_err(|_| Error::SchemaIncompatible)
}

pub(crate) fn cursor_for_scope(
    scope: CursorScope,
    state_string: impl Into<String>,
) -> Result<ChangeCursor, Error> {
    let server_state = encode_for_scope(&scope, state_string)?;
    Ok(ChangeCursor {
        scope,
        server_state,
        advanced_through: None,
        envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
    })
}

pub(crate) fn decode_cursor(cursor: &ChangeCursor) -> Result<(JmapScopeRepr, String), Error> {
    match decode(&cursor.server_state)? {
        JmapCursorState::V1 {
            scope,
            state_string,
        } => {
            if scope.to_cursor_scope() != cursor.scope {
                return Err(Error::Other(
                    "JMAP cursor payload scope does not match ChangeCursor scope".to_string(),
                ));
            }
            Ok((scope, state_string))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_state_round_trips_through_opaque_bytes() {
        let state = JmapCursorState::V1 {
            scope: JmapScopeRepr::Email,
            state_string: "s123".to_string(),
        };

        let encoded = encode(&state);
        assert_eq!(encoded.protocol, ProtocolKind::Jmap);
        assert_eq!(encoded.envelope_version, ENVELOPE_VERSION_V1);
        assert_eq!(decode(&encoded).unwrap(), state);
    }

    #[test]
    fn rejects_other_protocol_cursor() {
        let raw = OpaqueChangeState {
            protocol: ProtocolKind::Gmail,
            envelope_version: ENVELOPE_VERSION_V1,
            bytes: Vec::new(),
        };

        assert!(matches!(decode(&raw), Err(Error::CursorProtocolMismatch)));
    }

    #[test]
    fn rejects_unknown_envelope_version() {
        let mut raw = encode(&JmapCursorState::V1 {
            scope: JmapScopeRepr::Mailbox,
            state_string: "s456".to_string(),
        });
        raw.envelope_version = ENVELOPE_VERSION_V1 + 1;

        assert!(matches!(decode(&raw), Err(Error::CursorEnvelopeUnknown)));
    }
}
