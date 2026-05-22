use bifrost_types::{
    ChangeCursor, CursorScope, Error, ObjectType, OpaqueChangeState, ProtocolKind, QueryId,
};

pub(crate) const ENVELOPE_VERSION_V1: u32 = 1;
pub(crate) const CHANGE_CURSOR_ENVELOPE_VERSION: u32 = 1;

const STATE_TAG_V1: u8 = 1;

const SCOPE_TAG_EMAIL: u8 = 1;
const SCOPE_TAG_MAILBOX: u8 = 2;
const SCOPE_TAG_THREAD: u8 = 3;
const SCOPE_TAG_QUERY: u8 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JmapCursorState {
    V1 {
        scope: JmapScopeRepr,
        state_string: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
        bytes: encode_state(state),
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

    decode_state(&raw.bytes)
}

fn encode_state(state: &JmapCursorState) -> Vec<u8> {
    let mut out = Vec::new();
    match state {
        JmapCursorState::V1 {
            scope,
            state_string,
        } => {
            out.push(STATE_TAG_V1);
            encode_scope(scope, &mut out);
            encode_string(state_string, &mut out);
        }
    }
    out
}

fn encode_scope(scope: &JmapScopeRepr, out: &mut Vec<u8>) {
    match scope {
        JmapScopeRepr::Email => out.push(SCOPE_TAG_EMAIL),
        JmapScopeRepr::Mailbox => out.push(SCOPE_TAG_MAILBOX),
        JmapScopeRepr::Thread => out.push(SCOPE_TAG_THREAD),
        JmapScopeRepr::Query(query) => {
            out.push(SCOPE_TAG_QUERY);
            encode_string(query, out);
        }
    }
}

fn encode_string(value: &str, out: &mut Vec<u8>) {
    let bytes = value.as_bytes();
    let len: u32 = bytes
        .len()
        .try_into()
        .expect("JMAP cursor string longer than 4 GiB");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
}

fn decode_state(bytes: &[u8]) -> Result<JmapCursorState, Error> {
    let mut input = bytes;
    let tag = read_u8(&mut input)?;
    let state = match tag {
        STATE_TAG_V1 => {
            let scope = decode_scope(&mut input)?;
            let state_string = decode_string(&mut input)?;
            JmapCursorState::V1 {
                scope,
                state_string,
            }
        }
        _ => return Err(Error::SchemaIncompatible),
    };
    if !input.is_empty() {
        return Err(Error::SchemaIncompatible);
    }
    Ok(state)
}

fn decode_scope(input: &mut &[u8]) -> Result<JmapScopeRepr, Error> {
    let tag = read_u8(input)?;
    match tag {
        SCOPE_TAG_EMAIL => Ok(JmapScopeRepr::Email),
        SCOPE_TAG_MAILBOX => Ok(JmapScopeRepr::Mailbox),
        SCOPE_TAG_THREAD => Ok(JmapScopeRepr::Thread),
        SCOPE_TAG_QUERY => Ok(JmapScopeRepr::Query(decode_string(input)?)),
        _ => Err(Error::SchemaIncompatible),
    }
}

fn decode_string(input: &mut &[u8]) -> Result<String, Error> {
    let len = read_u32_le(input)? as usize;
    if input.len() < len {
        return Err(Error::SchemaIncompatible);
    }
    let (bytes, rest) = input.split_at(len);
    *input = rest;
    String::from_utf8(bytes.to_vec()).map_err(|_| Error::SchemaIncompatible)
}

fn read_u8(input: &mut &[u8]) -> Result<u8, Error> {
    let (first, rest) = input.split_first().ok_or(Error::SchemaIncompatible)?;
    *input = rest;
    Ok(*first)
}

fn read_u32_le(input: &mut &[u8]) -> Result<u32, Error> {
    if input.len() < 4 {
        return Err(Error::SchemaIncompatible);
    }
    let (head, rest) = input.split_at(4);
    *input = rest;
    let mut buf = [0u8; 4];
    buf.copy_from_slice(head);
    Ok(u32::from_le_bytes(buf))
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

    #[test]
    fn query_scope_round_trips() {
        let state = JmapCursorState::V1 {
            scope: JmapScopeRepr::Query("q-789".to_string()),
            state_string: "s-789".to_string(),
        };
        assert_eq!(decode(&encode(&state)).unwrap(), state);
    }

    #[test]
    fn rejects_unknown_state_tag() {
        let raw = OpaqueChangeState {
            protocol: ProtocolKind::Jmap,
            envelope_version: ENVELOPE_VERSION_V1,
            bytes: vec![0xff],
        };
        assert!(matches!(decode(&raw), Err(Error::SchemaIncompatible)));
    }

    #[test]
    fn rejects_unknown_scope_tag() {
        let raw = OpaqueChangeState {
            protocol: ProtocolKind::Jmap,
            envelope_version: ENVELOPE_VERSION_V1,
            bytes: vec![STATE_TAG_V1, 0xff, 0, 0, 0, 0],
        };
        assert!(matches!(decode(&raw), Err(Error::SchemaIncompatible)));
    }

    #[test]
    fn rejects_truncated_payload() {
        let mut bytes = encode(&JmapCursorState::V1 {
            scope: JmapScopeRepr::Email,
            state_string: "abc".to_string(),
        })
        .bytes;
        bytes.pop();
        let raw = OpaqueChangeState {
            protocol: ProtocolKind::Jmap,
            envelope_version: ENVELOPE_VERSION_V1,
            bytes,
        };
        assert!(matches!(decode(&raw), Err(Error::SchemaIncompatible)));
    }

    #[test]
    fn rejects_trailing_garbage() {
        let mut bytes = encode(&JmapCursorState::V1 {
            scope: JmapScopeRepr::Email,
            state_string: "abc".to_string(),
        })
        .bytes;
        bytes.push(0);
        let raw = OpaqueChangeState {
            protocol: ProtocolKind::Jmap,
            envelope_version: ENVELOPE_VERSION_V1,
            bytes,
        };
        assert!(matches!(decode(&raw), Err(Error::SchemaIncompatible)));
    }

    #[test]
    fn rejects_non_utf8_string() {
        let raw = OpaqueChangeState {
            protocol: ProtocolKind::Jmap,
            envelope_version: ENVELOPE_VERSION_V1,
            bytes: vec![STATE_TAG_V1, SCOPE_TAG_EMAIL, 1, 0, 0, 0, 0xff],
        };
        assert!(matches!(decode(&raw), Err(Error::SchemaIncompatible)));
    }
}
