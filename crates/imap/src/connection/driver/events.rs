use crate::types::response::{ResponseCode, TaggedResponse, UntaggedResponse};

use super::super::typed_event::TypedEvent;
use super::event_sink;

/// Emit [`TypedEvent::Alert`] or [`TypedEvent::NotificationOverflow`]
/// from an untagged response's response code, if present.
///
/// RFC 3501 Section7.1: `[ALERT]` response codes MUST be presented to the
/// user. RFC 5465 Section5.8: `[NOTIFICATIONOVERFLOW]` means the NOTIFY
/// registration was dropped. Both must reach the event queue regardless
/// of how the response is classified (solicited, unsolicited, or
/// impossible). The driver calls this before classification so the
/// event is emitted even when the response is routed to a consumer.
///
/// Returns `true` if a critical event was extracted (caller may skip
/// the redundant `From<UntaggedResponse>` conversion to avoid
/// double-emitting the same data).
pub(super) fn emit_untagged_response_code_events(
    u: &UntaggedResponse,
    event_sink: &mut event_sink::DriverEventSink,
) -> bool {
    match u {
        UntaggedResponse::Status {
            code: Some(ResponseCode::Alert),
            text,
            ..
        } => {
            let _ = event_sink.emit(TypedEvent::Alert(text.clone()));
            true
        }
        UntaggedResponse::Status {
            code: Some(ResponseCode::NotificationOverflow(detail)),
            text,
            ..
        } => {
            let _ = event_sink.emit(TypedEvent::NotificationOverflow {
                code: detail.clone(),
                text: text.clone(),
            });
            true
        }
        _ => false,
    }
}

/// Emit [`TypedEvent::Alert`] or [`TypedEvent::NotificationOverflow`]
/// from a tagged response's response code, if present.
///
/// Tagged responses go to `consumer.finalize_erased()` and never reach
/// the `From<UntaggedResponse>` event conversion. Without this
/// explicit emission, `[ALERT]` and `[NOTIFICATIONOVERFLOW]` in tagged
/// responses would be captured by `apply_side_effects` (state mutation)
/// but never published as typed events (bug B7, I13).
pub(super) fn emit_tagged_response_code_events(
    t: &TaggedResponse,
    event_sink: &mut event_sink::DriverEventSink,
) {
    match &t.code {
        Some(ResponseCode::Alert) => {
            let _ = event_sink.emit(TypedEvent::Alert(t.text.clone()));
        }
        Some(ResponseCode::NotificationOverflow(detail)) => {
            let _ = event_sink.emit(TypedEvent::NotificationOverflow {
                code: detail.clone(),
                text: t.text.clone(),
            });
        }
        _ => {}
    }
}

/// Check whether an untagged response carries `[ALERT]` or
/// `[NOTIFICATIONOVERFLOW]`: response codes that were already emitted
/// as typed events in the pre-classification pass.
///
/// Used by the reclassified-as-events loop: if a consumer reclassifies
/// a response that was already emitted by `emit_untagged_response_code_events`,
/// the reclassified copy must be skipped to prevent double-delivery.
pub(super) fn has_critical_response_code(u: &UntaggedResponse) -> bool {
    matches!(
        u,
        UntaggedResponse::Status {
            status,
            code: Some(ResponseCode::Alert),
            ..
        } | UntaggedResponse::Status {
            status,
            code: Some(ResponseCode::NotificationOverflow(_)),
            ..
        }
        if !matches!(status, crate::types::response::UntaggedStatus::Bye)
    )
}
