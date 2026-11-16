//! Helpers shared by more than one PIM module: id and error
//! construction, extended-property naming, and the Graph time formats.

use crate::account::graph_error::{invalid_account_error, protocol_violation};
use bifrost_types::{
    AccountError, AccountOperation, AttemptCause, ErrorScope, ObjectId, ProtocolErrorKind,
    TransmissionState,
};
use jiff::tz::Offset;
use jiff::{Timestamp, civil};
use serde_json::Value;
use std::time::SystemTime;

use super::messages::*;

/// The message id of a decoded Graph message. `attempt` is the failing
/// operation's evidence (see `protocol_violation`): `Acknowledged` for a
/// read whose own answer lacks the id, `Unsent` when the lookup is a
/// preflight to a write that is then never sent.
pub(super) fn object_id_from_value(
    value: &Value,
    operation: AccountOperation,
    owner: Option<&str>,
    attempt: AttemptCause,
) -> Result<ObjectId, AccountError> {
    value
        .get("id")
        .and_then(Value::as_str)
        .map(|id| ObjectId(crate::account::foreign::qualify_with_owner(owner, id)))
        .ok_or_else(|| {
            pim_protocol_error(
                operation,
                None,
                attempt,
                "Graph message did not include an id",
            )
        })
}

/// Build a `Protocol(ContractViolation)` `AccountError` for pim-layer
/// data-shape violations (missing id, missing etag, etc.). The
/// caller threads its `AccountOperation`, the scope of the
/// affected resource (typically `ErrorScope::Message { id }`) and its
/// transmission evidence (see `protocol_violation`); all three
/// flow through into telemetry and support exports.
pub(super) fn pim_protocol_error(
    operation: AccountOperation,
    scope: Option<ErrorScope>,
    attempt: AttemptCause,
    msg: impl Into<String>,
) -> AccountError {
    protocol_violation(
        ProtocolErrorKind::ContractViolation,
        operation,
        scope,
        attempt,
        msg,
    )
}

/// Evidence for a violation in the failing operation's own answer.
pub(super) fn acknowledged() -> AttemptCause {
    AttemptCause::new(TransmissionState::Acknowledged)
}

/// Evidence for a preflight answer that aborts a write before it is sent.
pub(super) fn write_unsent() -> AttemptCause {
    AttemptCause::new(TransmissionState::Unsent)
}

pub(super) fn graph_extended_property_id(property_id: &str) -> String {
    if property_id.eq_ignore_ascii_case(PR_LAST_VERB_EXECUTED_ALIAS)
        || property_id.eq_ignore_ascii_case("PidTagLastVerbExecuted")
    {
        PR_LAST_VERB_EXECUTED_GRAPH_ID.to_string()
    } else {
        property_id.to_string()
    }
}

/// Format an absolute instant as ISO-8601 UTC for the Graph
/// `singleValueExtendedProperty` value (matches Graph's PT_SYSTIME wire
/// shape, e.g. `2026-06-16T10:00:00Z`).
pub(super) fn graph_iso8601_utc(
    at: std::time::SystemTime,
    operation: AccountOperation,
) -> Result<String, AccountError> {
    Ok(utc_civil(at, operation)?
        .strftime("%Y-%m-%dT%H:%M:%SZ")
        .to_string())
}

/// Parse a Graph date-time (offset or zoneless UTC) into an instant.
pub(super) fn parse_graph_datetime(value: &str) -> Option<SystemTime> {
    if let Ok(ts) = value.parse::<Timestamp>() {
        return Some(ts.into());
    }
    // Graph also emits zoneless date-times; those are UTC by contract.
    let naive = civil::DateTime::strptime("%Y-%m-%dT%H:%M:%S%.f", value).ok()?;
    Offset::UTC.to_timestamp(naive).ok().map(SystemTime::from)
}

/// A `SystemTime` as the UTC wall clock it names. An instant outside the
/// representable range (beyond years -9999..=9999) is refused as malformed
/// input for `operation`, never substituted: a clamped instant would move a
/// search bound so it drops matches, or turn a far-future deferred send into
/// an immediate one, and an Outlook user expects neither.
pub(super) fn utc_civil(
    value: SystemTime,
    operation: AccountOperation,
) -> Result<civil::DateTime, AccountError> {
    Timestamp::try_from(value)
        .map(|timestamp| Offset::UTC.to_datetime(timestamp))
        .map_err(|_| {
            invalid_account_error(
                operation,
                "instant is outside the range Graph date-times can represent",
            )
        })
}

pub(super) fn system_time_rfc3339(
    value: SystemTime,
    operation: AccountOperation,
) -> Result<String, AccountError> {
    Ok(utc_civil(value, operation)?
        .strftime("%Y-%m-%dT%H:%M:%SZ")
        .to_string())
}

pub(super) fn system_time_naive_utc(
    value: SystemTime,
    operation: AccountOperation,
) -> Result<String, AccountError> {
    Ok(utc_civil(value, operation)?
        .strftime("%Y-%m-%dT%H:%M:%S")
        .to_string())
}

pub(super) fn finish_partial(
    error: AccountError,
    record: bifrost_types::PartialCommitBuilder,
) -> AccountError {
    error
        .into_builder()
        .partial_commit(record)
        .try_build()
        .expect("valid Graph partial commitment")
}
