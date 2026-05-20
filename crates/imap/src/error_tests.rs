#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use super::*;

fn auth_policy_failure() -> AuthPolicyFailure {
    AuthPolicyFailure::new(
        vec!["AUTH=PLAIN".to_owned()],
        vec![AuthMechanismRejection::new(
            crate::types::AuthMechanism::Plain,
            AuthMechanismRejectionReason::CleartextWithoutTls,
        )],
    )
}

// --- Construction and Debug ---

#[test]
fn io_variant_debug() {
    let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
    let err = Error::Io(Arc::new(io_err));
    let dbg = format!("{err:?}");
    assert!(dbg.contains("Io"));
    assert!(dbg.contains("refused"));
}

#[test]
fn auth_variant_debug() {
    let err = Error::Auth {
        text: "bad credentials".into(),
        code: None,
    };
    let dbg = format!("{err:?}");
    assert!(dbg.contains("Auth"));
    assert!(dbg.contains("bad credentials"));
}

#[test]
fn no_variant_debug() {
    let err = Error::no_with_code("SELECT failed".into(), None);
    let dbg = format!("{err:?}");
    assert!(dbg.contains("No"));
    assert!(dbg.contains("SELECT failed"));
}

#[test]
fn bad_variant_debug() {
    let err = Error::bad_with_code("unknown command".into(), None);
    let dbg = format!("{err:?}");
    assert!(dbg.contains("Bad"));
    assert!(dbg.contains("unknown command"));
}

#[test]
fn bye_variant_debug() {
    let err = Error::Bye {
        text: "server shutting down".into(),
        code: None,
    };
    let dbg = format!("{err:?}");
    assert!(dbg.contains("Bye"));
    assert!(dbg.contains("server shutting down"));
}

#[test]
fn protocol_variant_debug() {
    let err = Error::Protocol("unexpected tag".into());
    let dbg = format!("{err:?}");
    assert!(dbg.contains("Protocol"));
    assert!(dbg.contains("unexpected tag"));
}

#[test]
fn parse_variant_debug() {
    let err = Error::Parse("invalid envelope".into());
    let dbg = format!("{err:?}");
    assert!(dbg.contains("Parse"));
    assert!(dbg.contains("invalid envelope"));
}

#[test]
fn timeout_variant_debug() {
    let err = Error::Timeout;
    let dbg = format!("{err:?}");
    assert!(dbg.contains("Timeout"));
}

#[test]
fn closed_variant_debug() {
    let err = Error::Closed;
    let dbg = format!("{err:?}");
    assert!(dbg.contains("Closed"));
}

#[test]
fn starttls_unavailable_variant_debug() {
    let err = Error::StartTlsUnavailable;
    let dbg = format!("{err:?}");
    assert!(dbg.contains("StartTlsUnavailable"));
}

#[test]
fn missing_capability_variant_debug() {
    let err = Error::MissingCapability("IDLE".into());
    let dbg = format!("{err:?}");
    assert!(dbg.contains("MissingCapability"));
    assert!(dbg.contains("IDLE"));
}

#[test]
fn append_limit_variant_debug() {
    let err = Error::AppendLimit {
        size: 50_000_000,
        limit: 25_000_000,
    };
    let dbg = format!("{err:?}");
    assert!(dbg.contains("AppendLimit"));
    assert!(dbg.contains("50000000"));
    assert!(dbg.contains("25000000"));
}

#[test]
fn invalid_append_date_variant_debug() {
    let err = Error::InvalidAppendDate("not a date".into());
    let dbg = format!("{err:?}");
    assert!(dbg.contains("InvalidAppendDate"));
    assert!(dbg.contains("not a date"));
}

// --- Display formatting ---

#[test]
fn display_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broken");
    let err = Error::Io(Arc::new(io_err));
    let msg = err.to_string();
    assert!(msg.contains("I/O error"));
    assert!(msg.contains("pipe broken"));
}

#[test]
fn display_auth() {
    let err = Error::Auth {
        text: "LOGIN denied".into(),
        code: None,
    };
    let msg = err.to_string();
    assert_eq!(msg, "authentication failed: LOGIN denied");
}

#[test]
fn display_no() {
    let err = Error::no_with_code("[NOPERM] SELECT not allowed".into(), None);
    let msg = err.to_string();
    assert_eq!(msg, "server rejected command: [NOPERM] SELECT not allowed");
}

#[test]
fn display_bad() {
    let err = Error::bad_with_code("syntax error in FETCH".into(), None);
    let msg = err.to_string();
    assert_eq!(msg, "server reported bad command: syntax error in FETCH");
}

#[test]
fn display_bye() {
    let err = Error::Bye {
        text: "Too many connections".into(),
        code: None,
    };
    let msg = err.to_string();
    assert_eq!(msg, "server closing connection: Too many connections");
}

#[test]
fn display_protocol() {
    let err = Error::Protocol("missing CRLF".into());
    let msg = err.to_string();
    assert_eq!(msg, "protocol error: missing CRLF");
}

#[test]
fn display_parse() {
    let err = Error::Parse("unexpected NIL in address".into());
    let msg = err.to_string();
    assert_eq!(msg, "parse error: unexpected NIL in address");
}

#[test]
fn display_timeout() {
    let err = Error::Timeout;
    assert_eq!(err.to_string(), "operation timed out");
}

#[test]
fn display_closed() {
    let err = Error::Closed;
    assert_eq!(err.to_string(), "connection closed");
}

#[test]
fn display_starttls_unavailable() {
    let err = Error::StartTlsUnavailable;
    assert_eq!(err.to_string(), "STARTTLS not supported by server");
}

#[test]
fn display_missing_capability() {
    let err = Error::MissingCapability("COMPRESS=DEFLATE".into());
    assert_eq!(
        err.to_string(),
        "missing required capability: COMPRESS=DEFLATE"
    );
}

#[test]
fn display_append_limit() {
    let err = Error::AppendLimit {
        size: 10_000,
        limit: 5_000,
    };
    assert_eq!(
        err.to_string(),
        "message size 10000 exceeds server APPENDLIMIT of 5000"
    );
}

/// Display for `InvalidAppendDate` includes the offending date string
/// (RFC 3501 Section 9 date-time production).
#[test]
fn display_invalid_append_date() {
    let err = Error::InvalidAppendDate("32-Jan-2025 00:00:00 +0000".into());
    assert_eq!(
        err.to_string(),
        "invalid APPEND date-time: 32-Jan-2025 00:00:00 +0000"
    );
}

// --- From<std::io::Error> conversion ---

#[test]
fn from_io_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timed out");
    let err: Error = io_err.into();
    assert!(matches!(err, Error::Io(_)));
    assert!(err.to_string().contains("connect timed out"));
}

#[test]
fn from_io_error_preserves_kind() {
    let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");
    let err: Error = io_err.into();
    match &err {
        Error::Io(inner) => {
            assert_eq!(inner.kind(), std::io::ErrorKind::PermissionDenied);
        }
        _ => panic!("expected Io variant"),
    }
}

// --- std::error::Error source() ---

#[test]
fn io_variant_has_source() {
    use std::error::Error as StdError;
    let io_err = std::io::Error::other("disk full");
    let err = Error::Io(Arc::new(io_err));
    // thiserror #[source] sets the source to the inner io::Error.
    assert!(err.source().is_some());
}

#[test]
fn non_io_variants_have_no_source() {
    use std::error::Error as StdError;
    let variants: Vec<Error> = vec![
        Error::auth_with_code("fail".into(), None),
        Error::no_with_code("no".into(), None),
        Error::bad_with_code("bad".into(), None),
        Error::Bye {
            text: "bye".into(),
            code: None,
        },
        Error::Protocol("proto".into()),
        Error::Parse("parse".into()),
        Error::Timeout,
        Error::Closed,
        Error::StartTlsUnavailable,
        Error::MissingCapability("CAP".into()),
        Error::AppendLimit { size: 1, limit: 0 },
        Error::InvalidAppendDate("bad".into()),
        Error::Internal("test".into()),
        Error::DriverPanicked("test panic".into()),
        Error::DriverGone,
    ];
    for variant in &variants {
        assert!(
            variant.source().is_none(),
            "expected no source for: {variant:?}"
        );
    }
}

// --- Pattern matching for error recovery ---

#[test]
fn pattern_match_io() {
    let err = Error::Io(Arc::new(std::io::Error::other("oops")));
    match &err {
        Error::Io(io) => assert_eq!(io.kind(), std::io::ErrorKind::Other),
        _ => panic!("expected Io"),
    }
}

#[test]
fn pattern_match_auth() {
    let err = Error::Auth {
        text: "AUTHENTICATIONFAILED".into(),
        code: None,
    };
    match &err {
        Error::Auth { text, .. } => assert_eq!(text, "AUTHENTICATIONFAILED"),
        _ => panic!("expected Auth"),
    }
}

#[test]
fn pattern_match_no_extracts_response_text() {
    let err = Error::no_with_code("[NOPERM] Cannot delete INBOX".into(), None);
    match &err {
        Error::No { text, .. } => {
            assert!(text.contains("NOPERM"));
            assert!(text.contains("Cannot delete INBOX"));
        }
        _ => panic!("expected No"),
    }
}

#[test]
fn pattern_match_bad_extracts_response_text() {
    let err = Error::bad_with_code("Command unknown: XYZZY".into(), None);
    match &err {
        Error::Bad { text, .. } => assert!(text.contains("XYZZY")),
        _ => panic!("expected Bad"),
    }
}

#[test]
fn pattern_match_bye_extracts_response_text() {
    let err = Error::Bye {
        text: "Autologout; idle for too long".into(),
        code: None,
    };
    match &err {
        Error::Bye { text, .. } => assert!(text.contains("Autologout")),
        _ => panic!("expected Bye"),
    }
}

#[test]
fn pattern_match_protocol() {
    let err = Error::Protocol("unexpected continuation".into());
    match &err {
        Error::Protocol(msg) => assert!(msg.contains("continuation")),
        _ => panic!("expected Protocol"),
    }
}

#[test]
fn pattern_match_parse() {
    let err = Error::Parse("malformed literal count".into());
    match &err {
        Error::Parse(msg) => assert!(msg.contains("literal")),
        _ => panic!("expected Parse"),
    }
}

#[test]
fn pattern_match_unit_variants() {
    assert!(matches!(Error::Timeout, Error::Timeout));
    assert!(matches!(Error::Closed, Error::Closed));
    assert!(matches!(
        Error::StartTlsUnavailable,
        Error::StartTlsUnavailable
    ));
}

#[test]
fn pattern_match_missing_capability() {
    let err = Error::MissingCapability("MOVE".into());
    match &err {
        Error::MissingCapability(cap) => assert_eq!(cap, "MOVE"),
        _ => panic!("expected MissingCapability"),
    }
}

#[test]
fn pattern_match_append_limit() {
    let err = Error::AppendLimit {
        size: 100_000,
        limit: 50_000,
    };
    match &err {
        Error::AppendLimit { size, limit } => {
            assert_eq!(*size, 100_000);
            assert_eq!(*limit, 50_000);
        }
        _ => panic!("expected AppendLimit"),
    }
}

/// Pattern match on `InvalidAppendDate` to extract the offending date string
/// (RFC 3501 Section 9 date-time production).
#[test]
fn pattern_match_invalid_append_date() {
    let err = Error::InvalidAppendDate("missing timezone".into());
    match &err {
        Error::InvalidAppendDate(msg) => assert!(msg.contains("missing timezone")),
        _ => panic!("expected InvalidAppendDate"),
    }
}

// --- Error messages contain useful context ---

#[test]
fn display_messages_contain_context_strings() {
    // Verify that the user-facing message includes the context passed at construction.
    let cases: Vec<(Error, &str)> = vec![
        (
            Error::auth_with_code("PLAIN rejected".into(), None),
            "PLAIN rejected",
        ),
        (
            Error::no_with_code("SELECT denied for INBOX".into(), None),
            "SELECT denied for INBOX",
        ),
        (
            Error::bad_with_code("Unrecognized argument XATTR".into(), None),
            "Unrecognized argument XATTR",
        ),
        (
            Error::Bye {
                text: "BYE Logging out".into(),
                code: None,
            },
            "BYE Logging out",
        ),
        (
            Error::Protocol("tag mismatch: expected A001 got A002".into()),
            "tag mismatch: expected A001 got A002",
        ),
        (
            Error::Parse("incomplete BODYSTRUCTURE at offset 42".into()),
            "incomplete BODYSTRUCTURE at offset 42",
        ),
        (Error::MissingCapability("UIDPLUS".into()), "UIDPLUS"),
    ];
    for (err, expected_substr) in &cases {
        let msg = err.to_string();
        assert!(
            msg.contains(expected_substr),
            "expected {msg:?} to contain {expected_substr:?}"
        );
    }
}

#[test]
fn append_limit_display_contains_both_values() {
    let err = Error::AppendLimit {
        size: 1_048_576,
        limit: 524_288,
    };
    let msg = err.to_string();
    assert!(msg.contains("1048576"), "should contain the message size");
    assert!(msg.contains("524288"), "should contain the server limit");
}

// --- RFC 3501 Section 7.1.5 + RFC 5530: BYE response code preservation ---

/// BYE responses can carry response codes like [ALERT]
/// per RFC 3501 Section 7.1.5 and RFC 5530. The `Error::Bye` variant must
/// preserve the code so callers can act on it (e.g., present ALERT to the user).
#[test]
fn bye_with_alert_code_preserved() {
    let err = Error::bye_with_code("Server shutting down".into(), Some(ResponseCode::Alert));
    match &err {
        Error::Bye { code, text } => {
            assert_eq!(text, "Server shutting down");
            assert_eq!(*code, Some(ResponseCode::Alert));
        }
        _ => panic!("expected Bye variant"),
    }
}

/// BYE without a response code still works.
#[test]
fn bye_without_code_preserved() {
    let err = Error::bye_with_code("Logging out".into(), None);
    match &err {
        Error::Bye { code, text } => {
            assert_eq!(text, "Logging out");
            assert_eq!(*code, None);
        }
        _ => panic!("expected Bye variant"),
    }
}

/// BYE with UNAVAILABLE code (RFC 5530 Section 3).
#[test]
fn bye_with_unavailable_code_preserved() {
    let err = Error::bye_with_code("Try again later".into(), Some(ResponseCode::Unavailable));
    match &err {
        Error::Bye {
            code: Some(ResponseCode::Unavailable),
            ..
        } => {}
        _ => panic!("expected Bye with Unavailable code"),
    }
}

// --- RFC 5530 response code propagation  ---

#[test]
fn no_with_response_code_construction() {
    let err = Error::no_with_code("not allowed".into(), Some(ResponseCode::NoPerm));
    match &err {
        Error::No { text, code } => {
            assert_eq!(text, "not allowed");
            assert_eq!(*code, Some(ResponseCode::NoPerm));
        }
        _ => panic!("expected No"),
    }
}

#[test]
fn bad_with_response_code_construction() {
    let err = Error::bad_with_code("syntax error".into(), Some(ResponseCode::ClientBug));
    match &err {
        Error::Bad { text, code } => {
            assert_eq!(text, "syntax error");
            assert_eq!(*code, Some(ResponseCode::ClientBug));
        }
        _ => panic!("expected Bad"),
    }
}

/// Auth errors can carry response codes like `[AUTHENTICATIONFAILED]`
/// per RFC 5530 Section 3. The `Error::Auth` variant must preserve the code
/// so callers can distinguish failure reasons programmatically.
#[test]
fn auth_with_response_code_construction() {
    let err = Error::auth_with_code("bad creds".into(), Some(ResponseCode::AuthenticationFailed));
    match &err {
        Error::Auth { text, code } => {
            assert_eq!(text, "bad creds");
            assert_eq!(*code, Some(ResponseCode::AuthenticationFailed));
        }
        _ => panic!("expected Auth"),
    }
}

#[test]
fn no_with_code_display_shows_text_only() {
    let err = Error::no_with_code("not allowed".into(), Some(ResponseCode::NoPerm));
    assert_eq!(err.to_string(), "server rejected command: not allowed");
}

#[test]
fn bad_with_code_display_shows_text_only() {
    let err = Error::bad_with_code("bad syntax".into(), Some(ResponseCode::ClientBug));
    assert_eq!(err.to_string(), "server reported bad command: bad syntax");
}

/// Display for Auth with a response code shows only the text, not the code
/// (RFC 5530 Section 3 -- the code is structured data, not for display).
#[test]
fn auth_with_code_display_shows_text_only() {
    let err = Error::auth_with_code("denied".into(), Some(ResponseCode::AuthenticationFailed));
    assert_eq!(err.to_string(), "authentication failed: denied");
}

#[test]
fn response_code_categories_cover_policy_hooks() {
    let cases = vec![
        (
            ResponseCode::Unavailable,
            ErrorCategory::Transient,
            Recovery::RetryAfter,
        ),
        (
            ResponseCode::InUse,
            ErrorCategory::Transient,
            Recovery::RetryAfter,
        ),
        (
            ResponseCode::TempFail(None),
            ErrorCategory::Transient,
            Recovery::RetryAfter,
        ),
        (
            ResponseCode::Corruption,
            ErrorCategory::Transient,
            Recovery::RetryAfter,
        ),
        (
            ResponseCode::ExpungeIssued,
            ErrorCategory::MailboxState,
            Recovery::ResyncMailbox,
        ),
        (
            ResponseCode::Closed,
            ErrorCategory::MailboxState,
            Recovery::ResyncMailbox,
        ),
        (
            ResponseCode::ContactAdmin,
            ErrorCategory::Authorization,
            Recovery::DoNotRetry,
        ),
        (
            ResponseCode::AlreadyExists,
            ErrorCategory::MailboxState,
            Recovery::ResyncMailbox,
        ),
        (
            ResponseCode::NonExistent,
            ErrorCategory::MailboxState,
            Recovery::ResyncMailbox,
        ),
        (
            ResponseCode::TooBig,
            ErrorCategory::Limit,
            Recovery::DoNotRetry,
        ),
        (
            ResponseCode::MetadataMaxSize(1024),
            ErrorCategory::Limit,
            Recovery::DoNotRetry,
        ),
        (
            ResponseCode::Referral(Some("imap://example.test/".to_owned())),
            ErrorCategory::Referral,
            Recovery::FollowReferral,
        ),
        (
            ResponseCode::NotificationOverflow(None),
            ErrorCategory::NotificationOverflow,
            Recovery::RebuildNotificationRegistration,
        ),
    ];

    for (code, category, recovery) in cases {
        let err = Error::no_with_code("status".into(), Some(code));
        assert_eq!(err.category(), category);
        assert_eq!(err.recovery(), recovery);
    }
}

#[test]
fn local_error_categories_cover_connection_and_security_policy() {
    let cases = vec![
        (
            Error::Closed,
            ErrorCategory::Connection,
            Recovery::Reconnect,
        ),
        (
            Error::DriverGone,
            ErrorCategory::Connection,
            Recovery::Reconnect,
        ),
        (
            Error::StartTlsUnavailable,
            ErrorCategory::SecurityPolicy,
            Recovery::DoNotRetry,
        ),
        (
            Error::Bye {
                text: "server closed".to_owned(),
                code: None,
            },
            ErrorCategory::Connection,
            Recovery::Reconnect,
        ),
    ];

    for (err, category, recovery) in cases {
        assert_eq!(err.category(), category);
        assert_eq!(err.recovery(), recovery);
    }
}

#[test]
fn response_code_accessor_returns_status_code() {
    let err = Error::no_with_code("try later".to_owned(), Some(ResponseCode::Unavailable));
    assert_eq!(err.response_code(), Some(&ResponseCode::Unavailable));
    assert_eq!(Error::Timeout.response_code(), None);
}

#[test]
fn pattern_match_no_extracts_response_code() {
    let err = Error::no_with_code("over quota".into(), Some(ResponseCode::OverQuota));
    match &err {
        Error::No {
            code: Some(ResponseCode::OverQuota),
            ..
        } => {}
        _ => panic!("expected No with OverQuota code"),
    }
}

#[test]
fn pattern_match_bad_extracts_response_code() {
    let err = Error::bad_with_code("server bug".into(), Some(ResponseCode::ServerBug));
    match &err {
        Error::Bad {
            code: Some(ResponseCode::ServerBug),
            ..
        } => {}
        _ => panic!("expected Bad with ServerBug code"),
    }
}

/// Pattern match on Auth with response code to extract the structured reason
/// (RFC 5530 Section 3).
#[test]
fn pattern_match_auth_extracts_response_code() {
    let err = Error::auth_with_code("expired".into(), Some(ResponseCode::AuthenticationFailed));
    match &err {
        Error::Auth {
            code: Some(ResponseCode::AuthenticationFailed),
            ..
        } => {}
        _ => panic!("expected Auth with AuthenticationFailed code"),
    }
}

// --- PartialEq / Eq ---

#[test]
fn equal_unit_variants() {
    assert_eq!(Error::Timeout, Error::Timeout);
    assert_eq!(Error::Closed, Error::Closed);
    assert_eq!(Error::StartTlsUnavailable, Error::StartTlsUnavailable);
}

#[test]
fn equal_string_variants() {
    assert_eq!(Error::Protocol("x".into()), Error::Protocol("x".into()));
    assert_eq!(Error::Parse("y".into()), Error::Parse("y".into()));
    assert_eq!(
        Error::MissingCapability("IDLE".into()),
        Error::MissingCapability("IDLE".into())
    );
    assert_eq!(
        Error::InvalidAppendDate("d".into()),
        Error::InvalidAppendDate("d".into())
    );
    assert_eq!(Error::Internal("msg".into()), Error::Internal("msg".into()));
}

#[test]
fn equal_struct_variants() {
    assert_eq!(
        Error::Auth {
            text: "fail".into(),
            code: Some(ResponseCode::AuthenticationFailed),
        },
        Error::Auth {
            text: "fail".into(),
            code: Some(ResponseCode::AuthenticationFailed),
        }
    );
    assert_eq!(
        Error::No {
            text: "no".into(),
            code: None,
        },
        Error::No {
            text: "no".into(),
            code: None,
        }
    );
    assert_eq!(
        Error::AppendLimit {
            size: 100,
            limit: 50,
        },
        Error::AppendLimit {
            size: 100,
            limit: 50,
        }
    );
}

#[test]
fn io_equal_by_kind() {
    let a = Error::Io(Arc::new(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "message A",
    )));
    let b = Error::Io(Arc::new(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "message B",
    )));
    assert_eq!(a, b, "Io variants with same ErrorKind should be equal");
}

#[test]
fn io_not_equal_different_kind() {
    let a = Error::Io(Arc::new(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "x",
    )));
    let b = Error::Io(Arc::new(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "x",
    )));
    assert_ne!(
        a, b,
        "Io variants with different ErrorKind should not be equal"
    );
}

#[test]
fn different_variants_not_equal() {
    assert_ne!(Error::Timeout, Error::Closed);
    assert_ne!(Error::Protocol("x".into()), Error::Parse("x".into()));
}

#[test]
fn same_variant_different_payload_not_equal() {
    assert_ne!(
        Error::No {
            text: "a".into(),
            code: None,
        },
        Error::No {
            text: "b".into(),
            code: None,
        }
    );
}

// --- Exhaustive variant discrimination ---

/// Ensure all variants are distinguishable via pattern matching.
/// This test will fail to compile if a new variant is added
/// without being handled here, serving as a reminder to add tests.
#[test]
fn all_variants_are_distinguishable() {
    let errors: Vec<Error> = vec![
        Error::Io(Arc::new(std::io::Error::other("test"))),
        Error::auth_with_code("a".into(), None),
        Error::no_with_code("n".into(), None),
        Error::bad_with_code("b".into(), None),
        Error::Bye {
            text: "y".into(),
            code: None,
        },
        Error::Protocol("p".into()),
        Error::Parse("r".into()),
        Error::Timeout,
        Error::Closed,
        Error::AuthPolicy(auth_policy_failure()),
        Error::StartTlsUnavailable,
        Error::MissingCapability("c".into()),
        Error::AppendLimit { size: 1, limit: 0 },
        Error::FetchLimit {
            estimated: 2,
            limit: 1,
            seq: 3,
            uid: Some(4),
        },
        Error::InvalidAppendDate("bad date".into()),
        Error::Internal("internal err".into()),
        Error::DriverPanicked("test panic".into()),
        Error::DriverGone,
    ];

    for err in &errors {
        // Every variant must match exactly one arm.
        let label = match err {
            Error::Io(_) => "io",
            Error::Auth { .. } => "auth",
            Error::No { .. } => "no",
            Error::Bad { .. } => "bad",
            Error::Bye { .. } => "bye",
            Error::Protocol(_) => "protocol",
            Error::Parse(_) => "parse",
            Error::Timeout => "timeout",
            Error::Closed => "closed",
            Error::AuthPolicy(_) => "authpolicy",
            Error::StartTlsUnavailable => "starttls",
            Error::MissingCapability(_) => "capability",
            Error::AppendLimit { .. } => "appendlimit",
            Error::FetchLimit { .. } => "fetchlimit",
            Error::InvalidAppendDate(_) => "invalidappenddate",
            Error::Internal(_) => "internal",
            Error::DriverPanicked(_) => "driverpanicked",
            Error::DriverGone => "drivergone",
        };
        // Smoke check: the label should be non-empty.
        assert!(!label.is_empty());
    }
}
