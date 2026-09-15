use super::*;

/// The complete truth table for the RFC 9051 Section 6.3.1 dual-mode rule,
/// against the single authority every view now delegates to.
///
/// Worth stating why this table and not a test that calls all four wrappers and
/// asserts they agree: once they delegate, such a test is tautological. It
/// cannot fail when someone re-inlines a CORRECT copy, and it fails only if a
/// wrapper is miswired - so it would pin the wiring on the day it was written
/// and nothing after. No behavioural test can detect a semantically identical
/// re-inlining; preventing future copies is a review property, not a runtime
/// one. What a test CAN do is pin the rule itself, so that a centralized
/// implementation which is wrong is caught however many wrappers delegate to
/// it. That is this table.
///
/// The rule used to be written out five times, over four different views, and
/// only two of the copies had any test. The three without were the ones that
/// decide wire bytes and SELECT validation.
#[test]
fn the_dual_mode_rule_truth_table() {
    let rev1 = || vec![Capability::Imap4Rev1];
    let rev2 = || vec![Capability::Imap4Rev2];
    let both = || vec![Capability::Imap4Rev1, Capability::Imap4Rev2];
    let enabled = |s: &str| vec![s.to_owned()];

    for (caps, en, expected, why) in [
        (Vec::new(), Vec::new(), false, "neither revision advertised"),
        (rev1(), Vec::new(), false, "rev1 only"),
        (rev2(), Vec::new(), true, "rev2 only is rev2 from the start"),
        (
            both(),
            Vec::new(),
            false,
            "dual-mode without ENABLE stays rev1",
        ),
        (
            both(),
            enabled("IMAP4rev2"),
            true,
            "dual-mode with ENABLE is rev2",
        ),
        (
            both(),
            enabled("imap4rev2"),
            true,
            "the ENABLE match is case-insensitive (RFC 9051 capability atoms)",
        ),
        (
            both(),
            enabled("CONDSTORE"),
            false,
            "an unrelated ENABLE does not activate rev2",
        ),
        (
            rev2(),
            enabled("IMAP4rev2"),
            true,
            "rev2-only stays rev2 when redundantly enabled",
        ),
        (
            rev1(),
            enabled("IMAP4rev2"),
            false,
            "ENABLE cannot conjure a revision the server never advertised",
        ),
    ] {
        assert_eq!(
            imap4rev2_active(&caps, &en),
            expected,
            "{why} (capabilities={caps:?}, enabled={en:?})"
        );
    }
}

#[test]
fn dual_rev_server_requires_enable_for_rev2_profile() {
    let profile = ServerProfile::new(vec![Capability::Imap4Rev1, Capability::Imap4Rev2], vec![]);
    assert!(!profile.imap4rev2);

    let profile = ServerProfile::new(
        vec![Capability::Imap4Rev1, Capability::Imap4Rev2],
        vec!["IMAP4rev2".to_owned()],
    );
    assert!(profile.imap4rev2);
    assert!(profile.supports(Capability::Move));
}

#[test]
fn imap4rev2_profile_implies_base_extensions() {
    let profile = ServerProfile::new(vec![Capability::Imap4Rev2], vec![]);
    let implied = [
        Capability::Binary,
        Capability::Enable,
        Capability::Esearch,
        Capability::Idle,
        Capability::ListExtended,
        Capability::ListStatus,
        Capability::LiteralMinus,
        Capability::LiteralPlus,
        Capability::Move,
        Capability::Namespace,
        Capability::ObjectId,
        Capability::SaslIr,
        Capability::SaveDate,
        Capability::SearchRes,
        Capability::SpecialUse,
        Capability::StatusDeleted,
        Capability::StatusSize,
        Capability::UidPlus,
        Capability::Unselect,
    ];

    for capability in implied {
        assert!(profile.supports(capability));
    }
}

#[test]
fn auth_mechanisms_are_case_insensitive() {
    let profile = ServerProfile::new(vec![Capability::Auth("plain".to_owned())], vec![]);
    assert!(profile.supports_auth(AuthMechanism::Plain));
    assert!(profile.supports_sasl_auth(AuthMechanism::Plain));
    assert!(!profile.supports_auth(AuthMechanism::ScramSha256));
}

#[test]
fn scram_plus_does_not_satisfy_non_plus_scram() {
    let profile = ServerProfile::new(
        vec![Capability::Auth("SCRAM-SHA-256-PLUS".to_owned())],
        vec![],
    );

    assert!(!profile.supports_sasl_auth(AuthMechanism::ScramSha256));
    // The PLUS advertisement is matched by its own variant.
    assert!(profile.supports_sasl_auth(AuthMechanism::ScramSha256Plus));
    assert_eq!(AuthMechanism::ScramSha256Plus.name(), "SCRAM-SHA-256-PLUS");
    assert_eq!(AuthMechanism::ScramSha1Plus.name(), "SCRAM-SHA-1-PLUS");
}

#[test]
fn login_command_is_not_sasl_auth_login() {
    let profile = ServerProfile::new(vec![], vec![]);
    assert!(profile.supports_auth(AuthMechanism::Login));
    assert!(profile.supports_login_command());
    assert!(!profile.supports_sasl_auth(AuthMechanism::Login));

    let profile = ServerProfile::new(vec![Capability::LoginDisabled], vec![]);
    assert!(!profile.supports_login_command());
    assert!(!profile.supports_auth(AuthMechanism::Login));
}

#[test]
fn append_limit_policy_is_explicit() {
    let profile = ServerProfile::new(vec![], vec![]);
    assert_eq!(profile.append_limit, AppendLimitPolicy::NotAdvertised);

    let profile = ServerProfile::new(vec![Capability::AppendLimit(None)], vec![]);
    assert_eq!(profile.append_limit, AppendLimitPolicy::PerMailbox);

    let profile = ServerProfile::new(vec![Capability::AppendLimit(Some(1024))], vec![]);
    assert_eq!(profile.append_limit, AppendLimitPolicy::Limit(1024));

    let profile = ServerProfile::new(
        vec![
            Capability::AppendLimit(Some(1024)),
            Capability::AppendLimit(None),
        ],
        vec![],
    );
    assert_eq!(profile.append_limit, AppendLimitPolicy::PerMailbox);
}

#[test]
fn thread_algorithms_are_normalized() {
    let profile = ServerProfile::new(vec![Capability::Thread("references".to_owned())], vec![]);
    assert_eq!(profile.thread_algorithms, vec!["REFERENCES"]);
}

#[test]
fn enabled_checks_are_case_insensitive() {
    let profile = ServerProfile::new(vec![], vec!["qresync".to_owned()]);
    assert!(profile.enabled("QRESYNC"));
}
