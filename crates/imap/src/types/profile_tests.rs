use super::*;

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
