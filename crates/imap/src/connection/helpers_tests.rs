#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::codec::encode::LiteralMode;
use crate::connection::literals::AppendLiteralKind;
use crate::connection::test_support::detached;
use crate::connection::{ImapConnection, SessionState};
use crate::error::Error;
use crate::types::{Capability, Command, FetchAttr};

fn conn(caps: Vec<Capability>) -> ImapConnection {
    detached(SessionState::Authenticated, caps, &[])
}

fn conn_enabled(caps: Vec<Capability>, enabled: &[&str]) -> ImapConnection {
    detached(SessionState::Authenticated, caps, enabled)
}

// ---------------------------------------------------------------------------
// inbox_eq (RFC 3501 Section 5.1)
// ---------------------------------------------------------------------------

#[test]
fn inbox_compare_is_case_insensitive_for_inbox_only() {
    assert!(inbox_eq("INBOX", "inbox"));
    assert!(inbox_eq("InBoX", "INBOX"));
    assert!(inbox_eq("Archive", "Archive"));
    assert!(!inbox_eq("Archive", "archive"));
    assert!(!inbox_eq("INBOX", "INBOX/Sub"));
}

// ---------------------------------------------------------------------------
// status_item_tokens (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)
// ---------------------------------------------------------------------------

#[test]
fn status_tokens_accept_bare_list() {
    let tokens = ImapConnection::status_item_tokens("MESSAGES UNSEEN").unwrap();
    assert_eq!(tokens, vec!["MESSAGES", "UNSEEN"]);
}

#[test]
fn status_tokens_accept_parenthesized_list() {
    let tokens = ImapConnection::status_item_tokens("(MESSAGES  UIDNEXT)").unwrap();
    assert_eq!(tokens, vec!["MESSAGES", "UIDNEXT"]);
}

#[test]
fn status_tokens_trim_surrounding_whitespace() {
    let tokens = ImapConnection::status_item_tokens("  ( MESSAGES ) ").unwrap();
    assert_eq!(tokens, vec!["MESSAGES"]);
}

#[test]
fn status_tokens_reject_empty() {
    assert!(ImapConnection::status_item_tokens("").is_err());
    assert!(ImapConnection::status_item_tokens("   ").is_err());
    assert!(ImapConnection::status_item_tokens("()").is_err());
    assert!(ImapConnection::status_item_tokens("(   )").is_err());
}

#[test]
fn status_tokens_reject_unbalanced_parentheses() {
    assert!(ImapConnection::status_item_tokens("(MESSAGES").is_err());
    assert!(ImapConnection::status_item_tokens("MESSAGES)").is_err());
}

#[test]
fn status_tokens_reject_nested_parentheses() {
    assert!(ImapConnection::status_item_tokens("(MESSAGES (UNSEEN))").is_err());
    assert!(ImapConnection::status_item_tokens("MESSAGES (UNSEEN)").is_err());
}

// ---------------------------------------------------------------------------
// list_status_return_option_items (RFC 5819 Section 2)
// ---------------------------------------------------------------------------

/// RFC 5819 Section 2: the reserved return option is `STATUS SP "("
/// status-att-list ")"`, so the extracted body is exactly the item list. The
/// items must survive intact or the capability gate downstream
/// (`validate_requested_status_items`) stops recognizing the names it gates:
/// a truncated `RECENT` would pass on an IMAP4rev2 connection and a truncated
/// `HIGHESTMODSEQ` would pass without CONDSTORE.
#[test]
fn list_status_option_extracts_the_whole_item_list() {
    let items = ImapConnection::list_status_return_option_items("STATUS (MESSAGES UNSEEN)")
        .expect("recognized as STATUS")
        .unwrap();
    assert_eq!(items, "MESSAGES UNSEEN");

    let single = ImapConnection::list_status_return_option_items("STATUS (RECENT)")
        .expect("recognized as STATUS")
        .unwrap();
    assert_eq!(single, "RECENT");
}

#[test]
fn list_status_option_keyword_is_case_insensitive() {
    let items = ImapConnection::list_status_return_option_items("status (MESSAGES)")
        .expect("recognized as STATUS")
        .unwrap();
    assert_eq!(items, "MESSAGES");
}

#[test]
fn list_status_option_ignores_longer_atoms() {
    // RFC 5258 option-extension: STATUSX is a different option, not
    // LIST-STATUS with a malformed argument.
    assert!(
        ImapConnection::list_status_return_option_items("STATUSX").is_none(),
        "STATUSX must stay a generic option-extension"
    );
    assert!(ImapConnection::list_status_return_option_items("CHILDREN").is_none());
}

#[test]
fn list_status_option_rejects_missing_parentheses() {
    assert!(
        ImapConnection::list_status_return_option_items("STATUS MESSAGES")
            .expect("recognized as STATUS")
            .is_err()
    );
    assert!(
        ImapConnection::list_status_return_option_items("STATUS")
            .expect("recognized as STATUS")
            .is_err()
    );
    assert!(
        ImapConnection::list_status_return_option_items("STATUS (MESSAGES")
            .expect("recognized as STATUS")
            .is_err()
    );
}

// ---------------------------------------------------------------------------
// quota_resource_name (RFC 9208 Section 3.1.1)
// ---------------------------------------------------------------------------

#[test]
fn quota_resource_name_from_typed_capability() {
    let cap = Capability::QuotaResource("STORAGE".to_owned());
    assert_eq!(ImapConnection::quota_resource_name(&cap), Some("STORAGE"));
}

#[test]
fn quota_resource_name_from_other_capability() {
    let cap = Capability::Other("QUOTA=RES-MESSAGE".to_owned());
    assert_eq!(ImapConnection::quota_resource_name(&cap), Some("MESSAGE"));
    let lower = Capability::Other("quota=res-message".to_owned());
    assert_eq!(ImapConnection::quota_resource_name(&lower), Some("message"));
}

#[test]
fn quota_resource_name_rejects_bare_prefix_and_others() {
    // Exactly `QUOTA=RES-` names no resource.
    let bare = Capability::Other("QUOTA=RES-".to_owned());
    assert_eq!(ImapConnection::quota_resource_name(&bare), None);
    assert_eq!(
        ImapConnection::quota_resource_name(&Capability::Quota),
        None
    );
}

#[test]
fn has_quota_resource_matches_case_insensitively() {
    let c = conn(vec![Capability::Other("QUOTA=RES-STORAGE".to_owned())]);
    assert!(c.has_quota_resource("storage"));
    assert!(!c.has_quota_resource("MESSAGE"));
}

// ---------------------------------------------------------------------------
// search_return_requests_save (RFC 5182 Section 2)
// ---------------------------------------------------------------------------

#[test]
fn search_return_save_detected_on_both_forms() {
    let cmd = Command::SearchReturn {
        criteria: "ALL".to_owned(),
        return_opts: vec![" save ".to_owned()],
    };
    assert!(ImapConnection::search_return_requests_save(&cmd));

    let uid_cmd = Command::UidSearchReturn {
        criteria: "ALL".to_owned(),
        return_opts: vec!["MIN".to_owned(), "SAVE".to_owned()],
    };
    assert!(ImapConnection::search_return_requests_save(&uid_cmd));
}

#[test]
fn search_return_save_absent_and_on_other_commands() {
    let cmd = Command::SearchReturn {
        criteria: "ALL".to_owned(),
        return_opts: vec!["COUNT".to_owned()],
    };
    assert!(!ImapConnection::search_return_requests_save(&cmd));
    assert!(!ImapConnection::search_return_requests_save(&Command::Noop));
}

// ---------------------------------------------------------------------------
// Capability gates
// ---------------------------------------------------------------------------

#[test]
fn require_condstore_accepts_either_capability() {
    assert!(
        conn(vec![Capability::Condstore])
            .require_condstore()
            .is_ok()
    );
    // RFC 7162 Section 3.2.3: QRESYNC implies CONDSTORE.
    assert!(conn(vec![Capability::QResync]).require_condstore().is_ok());
    assert!(matches!(
        conn(vec![]).require_condstore(),
        Err(Error::MissingCapability(_))
    ));
}

#[test]
fn require_searchres_is_implied_by_rev2() {
    assert!(
        conn(vec![Capability::SearchRes])
            .require_searchres()
            .is_ok()
    );
    assert!(
        conn(vec![Capability::Imap4Rev2])
            .require_searchres()
            .is_ok()
    );
    assert!(matches!(
        conn(vec![Capability::Imap4Rev1]).require_searchres(),
        Err(Error::MissingCapability(_))
    ));
}

#[test]
fn dual_mode_rev2_requires_explicit_enable() {
    // RFC 9051 Section 6.3.1: rev1+rev2 advertised means rev2 behavior is
    // off until ENABLE IMAP4rev2.
    let dual = conn(vec![Capability::Imap4Rev1, Capability::Imap4Rev2]);
    assert!(!dual.is_rev2());
    let enabled = conn_enabled(
        vec![Capability::Imap4Rev1, Capability::Imap4Rev2],
        &["imap4rev2"],
    );
    assert!(enabled.is_rev2(), "ENABLE match is case-insensitive");
}

#[test]
fn require_state_gates_on_session_state() {
    let c = detached(SessionState::Authenticated, vec![], &[]);
    assert!(c.require_state(&[SessionState::Authenticated]).is_ok());
    assert!(
        c.require_state(&[SessionState::Selected]).is_err(),
        "Authenticated must not satisfy a Selected-only command"
    );
}

#[test]
fn utf8_only_gate_is_released_by_enable_or_rev2() {
    let blocked = conn(vec![Capability::Utf8Only]);
    assert!(blocked.check_utf8_only_enforced().is_err());

    let enabled = conn_enabled(vec![Capability::Utf8Only], &["UTF8=ACCEPT"]);
    assert!(enabled.check_utf8_only_enforced().is_ok());

    // RFC 9051 Appendix A: pure rev2 is already UTF-8 capable.
    let rev2 = conn(vec![Capability::Utf8Only, Capability::Imap4Rev2]);
    assert!(rev2.check_utf8_only_enforced().is_ok());

    assert!(conn(vec![]).check_utf8_only_enforced().is_ok());
}

// ---------------------------------------------------------------------------
// STATUS item validation
// ---------------------------------------------------------------------------

#[test]
fn status_recent_is_rejected_on_rev2() {
    let rev1 = conn(vec![Capability::Imap4Rev1]);
    assert!(rev1.validate_requested_status_items("RECENT").is_ok());
    let rev2 = conn(vec![Capability::Imap4Rev2]);
    assert!(rev2.validate_requested_status_items("RECENT").is_err());
}

#[test]
fn status_deleted_requires_rev2_or_quota_res_message() {
    assert!(
        conn(vec![Capability::Imap4Rev1])
            .validate_requested_status_items("DELETED")
            .is_err()
    );
    assert!(
        conn(vec![Capability::Imap4Rev2])
            .validate_requested_status_items("DELETED")
            .is_ok()
    );
    assert!(
        conn(vec![Capability::QuotaResource("MESSAGE".to_owned())])
            .validate_requested_status_items("DELETED")
            .is_ok()
    );
}

#[test]
fn status_deleted_storage_requires_quota_res_storage() {
    assert!(matches!(
        conn(vec![Capability::Imap4Rev2]).validate_requested_status_items("DELETED-STORAGE"),
        Err(Error::MissingCapability(_))
    ));
    assert!(
        conn(vec![Capability::QuotaResource("STORAGE".to_owned())])
            .validate_requested_status_items("DELETED-STORAGE")
            .is_ok()
    );
}

#[test]
fn status_size_requires_status_size_or_rev2() {
    assert!(
        conn(vec![Capability::Imap4Rev1])
            .validate_requested_status_items("SIZE")
            .is_err()
    );
    assert!(
        conn(vec![Capability::StatusSize])
            .validate_requested_status_items("SIZE")
            .is_ok()
    );
    assert!(
        conn(vec![Capability::Imap4Rev2])
            .validate_requested_status_items("SIZE")
            .is_ok()
    );
}

#[test]
fn status_extension_items_are_gated() {
    assert!(
        conn(vec![])
            .validate_requested_status_items("HIGHESTMODSEQ")
            .is_err()
    );
    assert!(
        conn(vec![Capability::Condstore])
            .validate_requested_status_items("HIGHESTMODSEQ")
            .is_ok()
    );
    assert!(
        conn(vec![])
            .validate_requested_status_items("APPENDLIMIT")
            .is_err()
    );
    assert!(
        conn(vec![Capability::AppendLimit(None)])
            .validate_requested_status_items("APPENDLIMIT")
            .is_ok()
    );
    assert!(
        conn(vec![])
            .validate_requested_status_items("MAILBOXID")
            .is_err()
    );
    assert!(
        conn(vec![Capability::ObjectId])
            .validate_requested_status_items("MAILBOXID")
            .is_ok()
    );
}

#[test]
fn status_items_are_matched_case_insensitively() {
    let rev2 = conn(vec![Capability::Imap4Rev2]);
    assert!(rev2.validate_requested_status_items("recent").is_err());
    assert!(
        rev2.validate_requested_status_items("(MESSAGES uidnext)")
            .is_ok()
    );
}

// ---------------------------------------------------------------------------
// FETCH item validation
// ---------------------------------------------------------------------------

#[test]
fn fetch_modseq_requires_condstore_and_is_not_implied_by_rev2() {
    assert!(
        conn(vec![Capability::Imap4Rev2])
            .validate_requested_fetch_items(&[FetchAttr::ModSeq])
            .is_err(),
        "CONDSTORE is not part of the rev2 baseline"
    );
    assert!(
        conn(vec![Capability::QResync])
            .validate_requested_fetch_items(&[FetchAttr::ModSeq])
            .is_ok()
    );
}

#[test]
fn fetch_objectid_items_are_implied_by_rev2() {
    assert!(
        conn(vec![Capability::Imap4Rev1])
            .validate_requested_fetch_items(&[FetchAttr::EmailId])
            .is_err()
    );
    assert!(
        conn(vec![Capability::Imap4Rev2])
            .validate_requested_fetch_items(&[FetchAttr::ThreadId])
            .is_ok()
    );
}

#[test]
fn fetch_binary_items_are_implied_by_rev2() {
    let item = FetchAttr::BinarySize { section: vec![1] };
    assert!(
        conn(vec![Capability::Imap4Rev1])
            .validate_requested_fetch_items(std::slice::from_ref(&item))
            .is_err()
    );
    assert!(
        conn(vec![Capability::Imap4Rev2])
            .validate_requested_fetch_items(std::slice::from_ref(&item))
            .is_ok()
    );
    assert!(
        conn(vec![Capability::Binary])
            .validate_requested_fetch_items(std::slice::from_ref(&item))
            .is_ok()
    );
}

#[test]
fn fetch_gmail_items_require_x_gm_ext_1() {
    assert!(
        conn(vec![Capability::Imap4Rev2])
            .validate_requested_fetch_items(&[FetchAttr::GmailMsgId])
            .is_err()
    );
    assert!(
        conn(vec![Capability::XGmExt1])
            .validate_requested_fetch_items(&[FetchAttr::GmailThreadId])
            .is_ok()
    );
}

#[test]
fn fetch_base_items_need_no_capability() {
    assert!(
        conn(vec![])
            .validate_requested_fetch_items(&[FetchAttr::Uid, FetchAttr::Flags])
            .is_ok()
    );
}

// ---------------------------------------------------------------------------
// Literal-mode selection (RFC 7888)
// ---------------------------------------------------------------------------

#[test]
fn literal_mode_prefers_literal_plus() {
    assert!(matches!(
        conn(vec![Capability::LiteralPlus, Capability::LiteralMinus]).literal_mode(),
        LiteralMode::LiteralPlus
    ));
    assert!(matches!(
        conn(vec![Capability::LiteralMinus]).literal_mode(),
        LiteralMode::LiteralMinus
    ));
    // RFC 9051 Appendix E item 2: pure rev2 has LITERAL- behavior.
    assert!(matches!(
        conn(vec![Capability::Imap4Rev2]).literal_mode(),
        LiteralMode::LiteralMinus
    ));
    assert!(matches!(
        conn(vec![Capability::Imap4Rev1]).literal_mode(),
        LiteralMode::Synchronizing
    ));
}

#[test]
fn non_sync_literal_size_gate() {
    let plus = conn(vec![Capability::LiteralPlus]);
    assert!(plus.supports_non_sync_literal(1_000_000));

    let minus = conn(vec![Capability::LiteralMinus]);
    assert!(minus.supports_non_sync_literal(4096));
    assert!(!minus.supports_non_sync_literal(4097));

    assert!(!conn(vec![]).supports_non_sync_literal(1));
}

#[test]
fn non_sync_literal8_requires_binary_and_is_off_for_pure_rev2() {
    // RFC 7888 Section 6: literal8 needs BINARY as well.
    assert!(!conn(vec![Capability::LiteralPlus]).supports_non_sync_literal8(1));
    assert!(
        conn(vec![Capability::LiteralPlus, Capability::Binary]).supports_non_sync_literal8(100_000)
    );
    // RFC 9051 Section 9: rev2 literal8 is always synchronizing.
    assert!(
        !conn(vec![
            Capability::LiteralPlus,
            Capability::Binary,
            Capability::Imap4Rev2
        ])
        .supports_non_sync_literal8(1)
    );
    let minus = conn(vec![Capability::LiteralMinus, Capability::Binary]);
    assert!(minus.supports_non_sync_literal8(4096));
    assert!(!minus.supports_non_sync_literal8(4097));
}

// ---------------------------------------------------------------------------
// APPEND literal kind (RFC 3516 Section 4.4 / RFC 6855 Section 4)
// ---------------------------------------------------------------------------

#[test]
fn append_literal_kind_plain_for_char8_data() {
    let c = conn(vec![]);
    assert_eq!(
        c.append_literal_kind(b"hello").unwrap(),
        AppendLiteralKind::Literal
    );
}

#[test]
fn append_literal_kind_requires_binary_for_nul_data() {
    let without = conn(vec![]);
    assert!(matches!(
        without.append_literal_kind(b"a\0b"),
        Err(Error::Protocol(_))
    ));
    let with = conn(vec![Capability::Binary]);
    assert_eq!(
        with.append_literal_kind(b"a\0b").unwrap(),
        AppendLiteralKind::Literal8
    );
}

#[test]
fn append_literal_kind_is_utf8_wrapped_once_enabled() {
    // RFC 6855 Section 4: after ENABLE UTF8=ACCEPT every APPEND uses the
    // `UTF8 (~{n})` wrapper, NUL or not.
    let c = conn_enabled(vec![], &["utf8=accept"]);
    assert_eq!(
        c.append_literal_kind(b"plain ascii").unwrap(),
        AppendLiteralKind::Utf8Literal8
    );
}

#[test]
fn append_literal_non_sync_follows_the_kind() {
    let c = conn(vec![Capability::LiteralPlus]);
    // Classic literal follows LITERAL+ ...
    assert!(c.append_literal_is_non_sync(AppendLiteralKind::Literal, 100_000));
    // ... but literal8 additionally needs BINARY.
    assert!(!c.append_literal_is_non_sync(AppendLiteralKind::Literal8, 1));
    assert!(!c.append_literal_is_non_sync(AppendLiteralKind::Utf8Literal8, 1));
}

// ---------------------------------------------------------------------------
// LIST-EXTENDED request validation (RFC 5258 Section 3)
// ---------------------------------------------------------------------------

#[test]
fn list_extended_requires_at_least_one_pattern() {
    let c = conn(vec![Capability::ListExtended]);
    assert!(matches!(
        c.validate_list_extended_request(&[], &[], &[]),
        Err(Error::Protocol(_))
    ));
}

#[test]
fn list_extended_multiple_patterns_need_the_capability() {
    let plain = conn(vec![Capability::Imap4Rev1]);
    assert!(matches!(
        plain.validate_list_extended_request(&["a", "b"], &[], &[]),
        Err(Error::MissingCapability(_))
    ));
    assert!(
        plain
            .validate_list_extended_request(&["a"], &[], &[])
            .is_ok()
    );
    let rev2 = conn(vec![Capability::Imap4Rev2]);
    assert!(
        rev2.validate_list_extended_request(&["a", "b"], &[], &[])
            .is_ok()
    );
}

#[test]
fn list_extended_options_need_the_capability() {
    let plain = conn(vec![Capability::Imap4Rev1]);
    assert!(matches!(
        plain.validate_list_extended_request(&["*"], &["SUBSCRIBED"], &[]),
        Err(Error::MissingCapability(_))
    ));
    assert!(matches!(
        plain.validate_list_extended_request(&["*"], &[], &["CHILDREN"]),
        Err(Error::MissingCapability(_))
    ));
}

#[test]
fn list_extended_rejects_empty_option_strings() {
    let c = conn(vec![Capability::ListExtended]);
    assert!(matches!(
        c.validate_list_extended_request(&["*"], &["  "], &[]),
        Err(Error::Protocol(_))
    ));
    assert!(matches!(
        c.validate_list_extended_request(&["*"], &[], &["  "]),
        Err(Error::Protocol(_))
    ));
}

#[test]
fn list_extended_special_use_option_is_gated() {
    let c = conn(vec![Capability::ListExtended]);
    assert!(matches!(
        c.validate_list_extended_request(&["*"], &["SPECIAL-USE"], &[]),
        Err(Error::MissingCapability(_))
    ));
    let with = conn(vec![Capability::ListExtended, Capability::SpecialUse]);
    assert!(
        with.validate_list_extended_request(&["*"], &["SPECIAL-USE"], &[])
            .is_ok()
    );
}

#[test]
fn list_extended_status_return_option_is_gated_by_list_status() {
    let c = conn(vec![Capability::ListExtended]);
    assert!(matches!(
        c.validate_list_extended_request(&["*"], &[], &["STATUS (MESSAGES)"]),
        Err(Error::MissingCapability(_))
    ));
    let with = conn(vec![Capability::ListExtended, Capability::ListStatus]);
    assert!(
        with.validate_list_extended_request(&["*"], &[], &["STATUS (MESSAGES)"])
            .is_ok()
    );
    // The nested STATUS items go through the same item validation.
    assert!(
        with.validate_list_extended_request(&["*"], &[], &["STATUS (BOGUS-ITEM)"])
            .is_ok(),
        "unknown items are passed through to the server"
    );
    assert!(
        with.validate_list_extended_request(&["*"], &[], &["STATUS ()"])
            .is_err()
    );
}

#[test]
fn list_extended_recursivematch_needs_a_partner_option() {
    // RFC 5258 Section 3: RECURSIVEMATCH alone (or with only REMOTE) is
    // a syntax error.
    let c = conn(vec![Capability::ListExtended]);
    assert!(matches!(
        c.validate_list_extended_request(&["*"], &["RECURSIVEMATCH"], &[]),
        Err(Error::Protocol(_))
    ));
    assert!(matches!(
        c.validate_list_extended_request(&["*"], &["RECURSIVEMATCH", "REMOTE"], &[]),
        Err(Error::Protocol(_))
    ));
    assert!(
        c.validate_list_extended_request(&["*"], &["RECURSIVEMATCH", "SUBSCRIBED"], &[])
            .is_ok()
    );
}
