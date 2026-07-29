#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

#[test]
fn constructors_pick_the_conventional_ports_and_modes() {
    let tls = ImapConfig::tls("imap.example.test");
    assert_eq!(tls.port, 993);
    assert_eq!(tls.tls_mode, TlsMode::Implicit);

    let starttls = ImapConfig::starttls("imap.example.test");
    assert_eq!(starttls.port, 143);
    assert_eq!(starttls.tls_mode, TlsMode::StartTls);

    let plain = ImapConfig::plaintext("imap.example.test");
    assert_eq!(plain.port, 143);
    assert_eq!(plain.tls_mode, TlsMode::None);
}

#[test]
fn defaults_carry_timeouts_and_keepalive() {
    let cfg = ImapConfig::tls("imap.example.test");
    assert_eq!(cfg.connect_timeout, Duration::from_secs(30));
    assert_eq!(cfg.command_timeout, Duration::from_secs(60));
    assert_eq!(
        cfg.keepalive,
        Some(TcpKeepalive::new(
            Duration::from_secs(120),
            Duration::from_secs(60)
        ))
    );
    assert!(cfg.tls_connector.is_none());
}

#[test]
fn builders_override_without_disturbing_the_rest() {
    let cfg = ImapConfig::starttls("imap.example.test")
        .with_port(1143)
        .with_connect_timeout(Duration::from_secs(5))
        .with_command_timeout(Duration::from_secs(7))
        .without_keepalive();

    assert_eq!(cfg.host, "imap.example.test");
    assert_eq!(cfg.port, 1143);
    assert_eq!(cfg.tls_mode, TlsMode::StartTls);
    assert_eq!(cfg.connect_timeout, Duration::from_secs(5));
    assert_eq!(cfg.command_timeout, Duration::from_secs(7));
    assert!(cfg.keepalive.is_none());
}

#[test]
fn tls_mode_predicates() {
    assert!(TlsMode::Implicit.uses_implicit_tls());
    assert!(!TlsMode::Implicit.uses_starttls());
    assert!(TlsMode::StartTls.uses_starttls());
    assert!(!TlsMode::StartTls.uses_implicit_tls());
    assert!(!TlsMode::None.uses_implicit_tls());
    assert!(!TlsMode::None.uses_starttls());
}

#[test]
fn debug_redacts_the_custom_connector() {
    let cfg = ImapConfig::tls("imap.example.test");
    let rendered = format!("{cfg:?}");
    assert!(rendered.contains("imap.example.test"));
    assert!(rendered.contains("993"));
    assert!(!rendered.contains("<custom>"));
}
