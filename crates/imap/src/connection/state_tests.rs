#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::types::response::{
    Capability, GreetingResponse, GreetingStatus, ResponseCode, StatusKind, TaggedResponse,
};

// ---------------------------------------------------------------------------
// apply_greeting  -  OK (no caps, no alert)
// ---------------------------------------------------------------------------

#[test]
fn apply_greeting_ok_no_caps_no_alert() {
    let mut state = ProtocolState::new();
    let greeting = GreetingResponse {
        status: GreetingStatus::Ok,
        code: None,
        text: "ready".into(),
    };
    let result = state.apply_greeting(&greeting).unwrap();
    assert!(result.is_none(), "no alert expected");
    assert_eq!(state.session_state(), SessionState::NotAuthenticated);
    assert!(state.capabilities().is_empty());
}

// ---------------------------------------------------------------------------
// apply_greeting  -  OK + [CAPABILITY]
// ---------------------------------------------------------------------------

#[test]
fn apply_greeting_ok_with_capabilities() {
    let mut state = ProtocolState::new();
    let caps = vec![Capability::Imap4Rev1, Capability::StartTls];
    let greeting = GreetingResponse {
        status: GreetingStatus::Ok,
        code: Some(ResponseCode::Capability(caps.clone())),
        text: "ready".into(),
    };
    let result = state.apply_greeting(&greeting).unwrap();
    assert!(result.is_none(), "no alert expected");
    assert_eq!(state.session_state(), SessionState::NotAuthenticated);
    assert_eq!(state.capabilities(), &caps);
}

// ---------------------------------------------------------------------------
// apply_greeting  -  OK + [ALERT]
// ---------------------------------------------------------------------------

#[test]
fn apply_greeting_ok_with_alert() {
    let mut state = ProtocolState::new();
    let greeting = GreetingResponse {
        status: GreetingStatus::Ok,
        code: Some(ResponseCode::Alert),
        text: "disk almost full".into(),
    };
    let result = state.apply_greeting(&greeting).unwrap();
    assert_eq!(result, Some("disk almost full".to_owned()));
    assert_eq!(state.session_state(), SessionState::NotAuthenticated);
}

// ---------------------------------------------------------------------------
// apply_greeting  -  PREAUTH
// ---------------------------------------------------------------------------

#[test]
fn apply_greeting_preauth() {
    let mut state = ProtocolState::new();
    let greeting = GreetingResponse {
        status: GreetingStatus::PreAuth,
        code: None,
        text: "logged in via TLS client cert".into(),
    };
    let result = state.apply_greeting(&greeting).unwrap();
    assert!(result.is_none());
    assert_eq!(state.session_state(), SessionState::Authenticated);
}

// ---------------------------------------------------------------------------
// apply_greeting  -  BYE (returns Err)
// ---------------------------------------------------------------------------

#[test]
fn apply_greeting_bye_returns_error() {
    let mut state = ProtocolState::new();
    let greeting = GreetingResponse {
        status: GreetingStatus::Bye,
        code: None,
        text: "server shutting down".into(),
    };
    let result = state.apply_greeting(&greeting);
    assert!(result.is_err(), "BYE greeting must return Err");
    assert_eq!(state.session_state(), SessionState::Logout);
}

// ---------------------------------------------------------------------------
// apply_infrastructure_failure  -  sets Logout
// ---------------------------------------------------------------------------

#[test]
fn apply_infrastructure_failure_sets_logout() {
    let mut state = ProtocolState::new();
    // Start in NotAuthenticated (default).
    assert_eq!(state.session_state(), SessionState::NotAuthenticated);
    state.apply_infrastructure_failure();
    assert_eq!(state.session_state(), SessionState::Logout);
}

// ---------------------------------------------------------------------------
// apply_capability_fetch  -  replaces and clears capabilities
// ---------------------------------------------------------------------------

#[test]
fn apply_capability_fetch_replaces_caps() {
    let mut state = ProtocolState::new();
    assert!(state.capabilities().is_empty());

    let caps = vec![Capability::Imap4Rev1, Capability::Idle];
    state.apply_capability_fetch(caps.clone());
    assert_eq!(state.capabilities(), &caps);

    // Replace with different caps.
    let new_caps = vec![Capability::StartTls];
    state.apply_capability_fetch(new_caps.clone());
    assert_eq!(state.capabilities(), &new_caps);

    // Clear caps.
    state.apply_capability_fetch(Vec::new());
    assert!(state.capabilities().is_empty());
}

// ---------------------------------------------------------------------------
// apply_side_effects  -  ENABLED appends to state
// ---------------------------------------------------------------------------

#[test]
fn enabled_appends_to_state() {
    let mut state = ProtocolState::new();
    assert!(state.enabled().is_empty());

    let resp = Response::Untagged(Box::new(UntaggedResponse::Enabled(vec![
        "CONDSTORE".into(),
        "QRESYNC".into(),
    ])));
    let digest = state.apply_side_effects(&resp);

    assert_eq!(state.enabled(), &["CONDSTORE", "QRESYNC"]);
    assert!(!digest.had_bye);
    assert!(!digest.had_notification_overflow);
}

// ---------------------------------------------------------------------------
// apply_side_effects  -  ENABLED deduplicates case-insensitively
// ---------------------------------------------------------------------------

#[test]
fn enabled_dedup_case_insensitive() {
    let mut state = ProtocolState::new();

    let resp1 = Response::Untagged(Box::new(UntaggedResponse::Enabled(vec![
        "IMAP4rev2".into(),
    ])));
    state.apply_side_effects(&resp1);

    // Same extension, different case  -  should NOT duplicate.
    let resp2 = Response::Untagged(Box::new(UntaggedResponse::Enabled(vec![
        "imap4rev2".into(),
    ])));
    state.apply_side_effects(&resp2);

    assert_eq!(state.enabled().len(), 1);
    assert_eq!(state.enabled()[0], "IMAP4rev2");
}

// ---------------------------------------------------------------------------
// apply_side_effects  -  ENABLED accumulates across calls
// ---------------------------------------------------------------------------

#[test]
fn enabled_accumulates_across_calls() {
    let mut state = ProtocolState::new();

    let resp1 = Response::Untagged(Box::new(UntaggedResponse::Enabled(vec![
        "CONDSTORE".into(),
    ])));
    state.apply_side_effects(&resp1);

    let resp2 = Response::Untagged(Box::new(UntaggedResponse::Enabled(vec!["QRESYNC".into()])));
    state.apply_side_effects(&resp2);

    assert_eq!(state.enabled().len(), 2);
    assert!(state.enabled().contains(&"CONDSTORE".to_owned()));
    assert!(state.enabled().contains(&"QRESYNC".to_owned()));
}

// ---------------------------------------------------------------------------
// UNAUTHENTICATE  -  tagged OK transitions to NotAuthenticated (RFC 8437 Section2)
// ---------------------------------------------------------------------------

#[test]
fn unauthenticate_ok_transitions_to_not_authenticated() {
    let mut state = ProtocolState::new();

    // Simulate authenticated state with capabilities and ENABLED extensions.
    let greeting = GreetingResponse {
        status: GreetingStatus::PreAuth,
        code: Some(ResponseCode::Capability(vec![
            Capability::Imap4Rev1,
            Capability::Unauthenticate,
        ])),
        text: "ready".into(),
    };
    state.apply_greeting(&greeting).unwrap();
    assert_eq!(state.session_state(), SessionState::Authenticated);

    // Simulate ENABLED extensions.
    let enabled_resp = Response::Untagged(Box::new(UntaggedResponse::Enabled(vec![
        "CONDSTORE".into(),
    ])));
    state.apply_side_effects(&enabled_resp);
    assert_eq!(state.enabled().len(), 1);

    // Mark UNAUTHENTICATE in flight.
    state.set_in_unauthenticate(true);

    // Tagged OK -> NotAuthenticated, all per-user state cleared.
    let tagged_ok = Response::Tagged(TaggedResponse {
        tag: "A1".into(),
        status: StatusKind::Ok,
        code: None,
        text: "unauthenticated".into(),
    });
    state.apply_side_effects(&tagged_ok);

    assert_eq!(state.session_state(), SessionState::NotAuthenticated);
    assert!(state.enabled().is_empty());
    let nf = state.notify();
    assert!(
        !nf.list && !nf.status && !nf.metadata,
        "notify flags should be default"
    );
}

// ---------------------------------------------------------------------------
// UNAUTHENTICATE  -  tagged OK from Selected clears mailbox (RFC 8437 Section2)
// ---------------------------------------------------------------------------

#[test]
fn unauthenticate_ok_from_selected_clears_mailbox() {
    use crate::types::validated::MailboxName;

    let mut state = ProtocolState::new();

    // Simulate authenticated state.
    let greeting = GreetingResponse {
        status: GreetingStatus::PreAuth,
        code: None,
        text: "ready".into(),
    };
    state.apply_greeting(&greeting).unwrap();

    // Simulate SELECT -> Selected state.
    state.set_in_select(Some(MailboxName::new("INBOX").unwrap()));
    let select_ok = Response::Tagged(TaggedResponse {
        tag: "A1".into(),
        status: StatusKind::Ok,
        code: None,
        text: "selected".into(),
    });
    state.apply_side_effects(&select_ok);
    assert_eq!(state.session_state(), SessionState::Selected);

    // Mark UNAUTHENTICATE in flight.
    state.set_in_unauthenticate(true);

    // Tagged OK -> NotAuthenticated, selected mailbox cleared.
    let tagged_ok = Response::Tagged(TaggedResponse {
        tag: "A2".into(),
        status: StatusKind::Ok,
        code: None,
        text: "unauthenticated".into(),
    });
    state.apply_side_effects(&tagged_ok);

    assert_eq!(state.session_state(), SessionState::NotAuthenticated);
}

// ---------------------------------------------------------------------------
// UNAUTHENTICATE  -  tagged NO preserves state (RFC 8437 Section2)
// ---------------------------------------------------------------------------

#[test]
fn unauthenticate_no_preserves_state() {
    let mut state = ProtocolState::new();

    let greeting = GreetingResponse {
        status: GreetingStatus::PreAuth,
        code: None,
        text: "ready".into(),
    };
    state.apply_greeting(&greeting).unwrap();
    assert_eq!(state.session_state(), SessionState::Authenticated);

    state.set_in_unauthenticate(true);

    let tagged_no = Response::Tagged(TaggedResponse {
        tag: "A1".into(),
        status: StatusKind::No,
        code: None,
        text: "not allowed".into(),
    });
    state.apply_side_effects(&tagged_no);

    assert_eq!(state.session_state(), SessionState::Authenticated);
}

// ---------------------------------------------------------------------------
// UNAUTHENTICATE  -  tagged BAD preserves state (RFC 8437 Section2)
// ---------------------------------------------------------------------------

#[test]
fn unauthenticate_bad_preserves_state() {
    let mut state = ProtocolState::new();

    let greeting = GreetingResponse {
        status: GreetingStatus::PreAuth,
        code: None,
        text: "ready".into(),
    };
    state.apply_greeting(&greeting).unwrap();
    assert_eq!(state.session_state(), SessionState::Authenticated);

    state.set_in_unauthenticate(true);

    let tagged_bad = Response::Tagged(TaggedResponse {
        tag: "A1".into(),
        status: StatusKind::Bad,
        code: None,
        text: "unknown command".into(),
    });
    state.apply_side_effects(&tagged_bad);

    assert_eq!(state.session_state(), SessionState::Authenticated);
}
