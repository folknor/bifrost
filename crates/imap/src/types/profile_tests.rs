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
    assert!(!profile.supports_auth(AuthMechanism::ScramSha256));
}
