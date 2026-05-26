//! Synthetic classification tests for the IMAP -> AccountError boundary.
//!
//! Tests construct `crate::Error` values directly and verify the
//! resulting `AccountError` kind, cause chain shape, scope/operation,
//! attempt evidence, and derived recovery. No live server, no mock
//! harness.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use bifrost_types::{
    AccessErrorKind, AccountErrorKind, AccountOperation, AuthErrorKind, Cause, CursorScope,
    EngineDirective, FolderId, ImapResponseCode, ProtocolErrorKind, RecoveryClass,
    RequestErrorKind, ResourceKind, ServerErrorKind, StrategyDowngrade, SyncStateErrorKind,
    ThrottleScope, TransmissionState, TransportErrorKind, WireCause,
};

use super::{ImapErrorContext, into_account_error, strategy_failure};
use crate::Error;
use crate::types::{MailboxName, ResponseCode};

fn mailbox(name: &str) -> MailboxName {
    MailboxName::new(name).expect("valid mailbox name")
}

fn folder_cursor(name: &str) -> CursorScope {
    CursorScope::Folder(FolderId(name.to_owned()))
}

#[test]
fn io_unsent_builds_transport_network_with_retry() {
    let err = Error::io(std::io::Error::other("refused")).with_attempt(TransmissionState::Unsent);
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Transport(TransportErrorKind::Network)
    ));
    assert!(account.recovery().is_retryable());
    let attempt = account.chain().iter().find_map(|c| match c {
        Cause::Attempt(a) => Some(a.transmission_state),
        _ => None,
    });
    assert_eq!(attempt, Some(TransmissionState::Unsent));
}

#[test]
fn io_inflight_non_idempotent_reconciles() {
    let err = Error::io(std::io::Error::other("eof")).with_attempt(TransmissionState::InFlight);
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::BulkMove));

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Transport(TransportErrorKind::Network)
    ));
    assert!(account.recovery().requires_reconciliation());
}

#[test]
fn timeout_inflight_idempotent_retries() {
    let err = Error::timeout().with_attempt(TransmissionState::InFlight);
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Transport(TransportErrorKind::Timeout)
    ));
    assert!(account.recovery().is_retryable());
}

#[test]
fn closed_inflight_carries_attempt_cause() {
    let err = Error::closed().with_attempt(TransmissionState::InFlight);
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Transport(TransportErrorKind::Network)
    ));
    assert!(account.chain().iter().any(|c| matches!(
        c,
        Cause::Attempt(a) if a.transmission_state == TransmissionState::InFlight
    )));
}

#[test]
fn auth_with_authentication_failed_maps_to_reauth() {
    let err = Error::auth_with_code(
        "credentials rejected".into(),
        Some(ResponseCode::AuthenticationFailed),
    );
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::Discover));

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
    ));
    assert!(account.chain().iter().any(|c| matches!(
        c,
        Cause::Wire(WireCause::Imap(ImapResponseCode::AuthenticationFailed))
    )));
}

#[test]
fn auth_with_expired_maps_to_expired() {
    let err = Error::auth_with_code("expired".into(), Some(ResponseCode::Expired));
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::Discover));

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Authentication(AuthErrorKind::Expired)
    ));
}

#[test]
fn auth_policy_maps_to_policy_blocked() {
    let err = Error::AuthPolicy(crate::error::AuthPolicyFailure::new(
        vec!["PLAIN".into()],
        Vec::new(),
    ));
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::Discover));

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked)
    ));
}

#[test]
fn no_with_noperm_maps_to_permission_denied_with_scope_resource() {
    let err = Error::no_with_code("no perm".into(), Some(ResponseCode::NoPerm));
    let ctx = ImapErrorContext::operation(AccountOperation::UpdateFlags).with_message_id("msg-1");
    let account = into_account_error(err, ctx);

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
    ));
    let resource = account.chain().iter().find_map(|c| match c {
        Cause::Access(bifrost_types::AccessCause::PermissionDenied { resource }) => Some(*resource),
        _ => None,
    });
    assert_eq!(resource, Some(Some(ResourceKind::Message)));
}

#[test]
fn bad_without_code_maps_to_request_malformed() {
    let err = Error::bad_with_code("syntax".into(), None);
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::Discover));
    assert!(matches!(
        account.kind(),
        AccountErrorKind::Request(RequestErrorKind::Malformed)
    ));
}

#[test]
fn bad_with_serverbug_maps_to_contract_violation() {
    let err = Error::bad_with_code("bug".into(), Some(ResponseCode::ServerBug));
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::Discover));
    assert!(matches!(
        account.kind(),
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
    ));
}

#[test]
fn bye_with_unavailable_inflight_is_unavailable() {
    let err = Error::Bye {
        text: "going away".into(),
        code: Some(ResponseCode::Unavailable),
        attempt: None,
    }
    .with_attempt(TransmissionState::InFlight);
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::BulkMove));

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Server(ServerErrorKind::Unavailable)
    ));
    // Non-idempotent operation + InFlight => reconcile.
    assert!(account.recovery().requires_reconciliation());
}

#[test]
fn parse_maps_to_protocol_parse_failed() {
    let err = Error::Parse("garbled".into());
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );
    assert!(matches!(
        account.kind(),
        AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
    ));
}

#[test]
fn invalid_input_maps_to_request_malformed() {
    let err = Error::InvalidInput("bad mailbox name".into());
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::ContainerCreate),
    );
    assert!(matches!(
        account.kind(),
        AccountErrorKind::Request(RequestErrorKind::Malformed)
    ));
}

#[test]
fn missing_capability_with_push_subscribe_maps_to_unsupported() {
    let err = Error::MissingCapability("IDLE".into());
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::PushSubscribe),
    );
    assert!(matches!(
        account.kind(),
        AccountErrorKind::Unsupported(AccountOperation::PushSubscribe)
    ));
}

#[test]
fn response_code_modified_maps_to_concurrency_conflict() {
    let err = Error::no_with_code("modified".into(), Some(ResponseCode::Modified(Vec::new())));
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::UpdateFlags),
    );
    assert_eq!(*account.kind(), AccountErrorKind::ConcurrencyConflict);
}

#[test]
fn response_code_nomodseq_maps_to_strategy_failure_condstore_to_basic() {
    let err = Error::no_with_code("nomodseq".into(), Some(ResponseCode::NoModSeq));
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::SyncState(SyncStateErrorKind::StrategyFailure)
    ));
    let downgrade = account.chain().iter().find_map(|c| match c {
        Cause::State(bifrost_types::StateCause::StrategyFailure { downgrade }) => Some(*downgrade),
        _ => None,
    });
    assert_eq!(downgrade, Some(StrategyDowngrade::CondstoreToBasic));
}

#[test]
fn response_code_overquota_maps_to_quota_with_account_throttle() {
    let err = Error::no_with_code("over".into(), Some(ResponseCode::OverQuota));
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Server(ServerErrorKind::QuotaExhausted)
    ));
    if let RecoveryClass::Retry(advice) = account.recovery() {
        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
    } else {
        panic!("expected Retry");
    }
}

#[test]
fn response_code_limit_in_mailbox_scope_throttles_mailbox() {
    let err = Error::no_with_code("limit".into(), Some(ResponseCode::Limit));
    let ctx =
        ImapErrorContext::operation(AccountOperation::SyncChanges).with_mailbox(&mailbox("INBOX"));
    let account = into_account_error(err, ctx);

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Server(ServerErrorKind::RateLimited)
    ));
    if let RecoveryClass::Retry(advice) = account.recovery() {
        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Mailbox));
    } else {
        panic!("expected Retry");
    }
}

#[test]
fn response_code_nonexistent_in_mailbox_scope_maps_to_notfound_mailbox() {
    let err = Error::no_with_code("nope".into(), Some(ResponseCode::NonExistent));
    let ctx = ImapErrorContext::operation(AccountOperation::ContainerDelete)
        .with_mailbox(&mailbox("INBOX/old"));
    let account = into_account_error(err, ctx);

    assert!(matches!(
        account.kind(),
        AccountErrorKind::NotFound(ResourceKind::Mailbox)
    ));
}

#[test]
fn response_code_notification_overflow_maps_to_cursor_invalid() {
    let err = Error::no_with_code(
        "overflow".into(),
        Some(ResponseCode::NotificationOverflow(None)),
    );
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::PushStream),
    );
    assert!(matches!(
        account.kind(),
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
    ));
}

#[test]
fn every_response_code_variant_maps_to_imap_response_code() {
    // Exercise the wire-code mapping for every well-known variant.
    // Compilation is the meaningful assertion: if a `ResponseCode`
    // variant is added without extending `imap_response_code`, the
    // exhaustive match in `account/error.rs` won't compile.
    let codes: Vec<ResponseCode> = vec![
        ResponseCode::Alert,
        ResponseCode::BadCharset(Vec::new()),
        ResponseCode::Parse,
        ResponseCode::ReadOnly,
        ResponseCode::ReadWrite,
        ResponseCode::TryCreate,
        ResponseCode::UidNext(1),
        ResponseCode::UidValidity(1),
        ResponseCode::Unseen(1),
        ResponseCode::HighestModSeq(1),
        ResponseCode::Modified(Vec::new()),
        ResponseCode::NoModSeq,
        ResponseCode::Closed,
        ResponseCode::MailboxId("x".into()),
        ResponseCode::Unavailable,
        ResponseCode::AuthenticationFailed,
        ResponseCode::AuthorizationFailed,
        ResponseCode::Expired,
        ResponseCode::PrivacyRequired,
        ResponseCode::ContactAdmin,
        ResponseCode::NoPerm,
        ResponseCode::InUse,
        ResponseCode::ExpungeIssued,
        ResponseCode::Corruption,
        ResponseCode::ServerBug,
        ResponseCode::ClientBug,
        ResponseCode::Cannot,
        ResponseCode::Limit,
        ResponseCode::OverQuota,
        ResponseCode::AlreadyExists,
        ResponseCode::NonExistent,
        ResponseCode::NewName(None),
        ResponseCode::Referral(None),
        ResponseCode::UrlMech(None),
        ResponseCode::BadUrl(None),
        ResponseCode::BadComparator(None),
        ResponseCode::Annotate(None),
        ResponseCode::Annotations(None),
        ResponseCode::TempFail(None),
        ResponseCode::MaxConvertMessages(None),
        ResponseCode::MaxConvertParts(None),
        ResponseCode::NoUpdate(None),
        ResponseCode::NotificationOverflow(None),
        ResponseCode::BadEvent(None),
        ResponseCode::UndefinedFilter(None),
        ResponseCode::UidNotSticky,
        ResponseCode::NotSaved,
        ResponseCode::HasChildren,
        ResponseCode::UnknownCte,
        ResponseCode::TooBig,
        ResponseCode::CompressionActive,
        ResponseCode::UseAttr,
        ResponseCode::MetadataLongEntries(0),
        ResponseCode::MetadataMaxSize(0),
        ResponseCode::MetadataTooMany,
        ResponseCode::MetadataNoPrivate,
        ResponseCode::Other {
            name: "X-FOO".into(),
            value: Some("bar".into()),
        },
    ];
    for code in codes {
        let err = Error::no_with_code("t".into(), Some(code.clone()));
        let _account =
            into_account_error(err, ImapErrorContext::operation(AccountOperation::Discover));
    }
}

#[test]
fn other_response_code_preserves_payload_in_wire_unknown() {
    let err = Error::no_with_code(
        "x".into(),
        Some(ResponseCode::Other {
            name: "X-CUSTOM".into(),
            value: Some("payload".into()),
        }),
    );
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::Discover));
    let has_unknown = account.chain().iter().any(|c| {
        matches!(
            c,
            Cause::Wire(WireCause::Imap(ImapResponseCode::Unknown { code, value }))
                if code == "X-CUSTOM" && value.as_ref().map(|v| v.value.as_str()) == Some("payload")
        )
    });
    assert!(has_unknown);
}

#[test]
fn uidvalidity_changed_derives_restart_scope() {
    let account = super::uidvalidity_changed(&mailbox("INBOX"), 1, 2);
    assert!(matches!(
        account.recovery(),
        RecoveryClass::Engine(EngineDirective::RestartScope(CursorScope::Folder(_)))
    ));
}

#[test]
fn modseq_reset_derives_restart_scope() {
    let account = super::modseq_reset(&mailbox("INBOX"), 10, Some(5));
    assert!(matches!(
        account.recovery(),
        RecoveryClass::Engine(EngineDirective::RestartScope(CursorScope::Folder(_)))
    ));
}

#[test]
fn qresync_to_condstore_strategy_failure_derives_downgrade() {
    let account = strategy_failure(&mailbox("INBOX"), StrategyDowngrade::QResyncToCondstore);
    match account.recovery() {
        RecoveryClass::Engine(EngineDirective::DowngradeStrategy(
            StrategyDowngrade::QResyncToCondstore,
        )) => {}
        other => panic!("expected DowngradeStrategy(QResyncToCondstore), got {other:?}"),
    }
}

#[test]
fn condstore_to_basic_strategy_failure_derives_downgrade() {
    let account = strategy_failure(&mailbox("INBOX"), StrategyDowngrade::CondstoreToBasic);
    match account.recovery() {
        RecoveryClass::Engine(EngineDirective::DowngradeStrategy(
            StrategyDowngrade::CondstoreToBasic,
        )) => {}
        other => panic!("expected DowngradeStrategy(CondstoreToBasic), got {other:?}"),
    }
}

#[test]
fn ctx_with_cursor_scope_helper_round_trip() {
    let ctx = ImapErrorContext::operation(AccountOperation::SyncChanges)
        .with_cursor_scope(folder_cursor("INBOX"));
    let err = Error::no_with_code("nope".into(), Some(ResponseCode::ExpungeIssued));
    let account = into_account_error(err, ctx);
    assert!(matches!(
        account.kind(),
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
    ));
}
