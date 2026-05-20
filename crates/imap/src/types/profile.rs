//! Capability profile helpers.

use super::{AuthMechanism, Capability};

/// Server-wide APPENDLIMIT policy advertised in CAPABILITY.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum AppendLimitPolicy {
    /// The server did not advertise APPENDLIMIT.
    #[default]
    NotAdvertised,
    /// Bare `APPENDLIMIT` was advertised; check selected-mailbox status for
    /// a mailbox-specific limit.
    PerMailbox,
    /// `APPENDLIMIT=<n>` advertised a server-wide limit in octets.
    Limit(u64),
}

/// Caller-friendly snapshot of server capabilities and enabled extensions.
///
/// This is not a live view. Capabilities and enabled extensions can change
/// after STARTTLS, authentication, and ENABLE, so callers that cross those
/// boundaries should call `server_profile()` again instead of caching an old
/// profile.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerProfile {
    /// Raw advertised capabilities.
    pub capabilities: Vec<Capability>,
    /// Extensions successfully enabled with ENABLE.
    pub enabled: Vec<String>,
    /// True when IMAP4rev2 behavior is active.
    pub imap4rev2: bool,
    /// AUTH mechanisms advertised by the server.
    pub auth_mechanisms: Vec<String>,
    /// Server APPENDLIMIT policy advertised in CAPABILITY.
    pub append_limit: AppendLimitPolicy,
    /// Advertised THREAD algorithms.
    pub thread_algorithms: Vec<String>,
}

impl ServerProfile {
    /// Build a profile from capability and enabled-extension snapshots.
    pub fn new(capabilities: Vec<Capability>, enabled: Vec<String>) -> Self {
        let imap4rev2 = imap4rev2_active(&capabilities, &enabled);
        let auth_mechanisms = capabilities
            .iter()
            .filter_map(|cap| match cap {
                Capability::Auth(mechanism) => Some(mechanism.to_ascii_uppercase()),
                _ => None,
            })
            .collect();
        let append_limit = capabilities
            .iter()
            .find_map(|cap| match cap {
                Capability::AppendLimit(None) => Some(AppendLimitPolicy::PerMailbox),
                Capability::AppendLimit(Some(limit)) => Some(AppendLimitPolicy::Limit(*limit)),
                _ => None,
            })
            .unwrap_or_default();
        let thread_algorithms = capabilities
            .iter()
            .filter_map(|cap| match cap {
                Capability::Thread(algorithm) => Some(algorithm.to_ascii_uppercase()),
                _ => None,
            })
            .collect();

        Self {
            capabilities,
            enabled,
            imap4rev2,
            auth_mechanisms,
            append_limit,
            thread_algorithms,
        }
    }

    /// Return `true` if the capability is advertised or folded into active IMAP4rev2.
    pub fn supports(&self, capability: Capability) -> bool {
        self.capabilities.contains(&capability) || self.rev2_implies(&capability)
    }

    /// Return `true` if the server advertises the SASL mechanism.
    ///
    /// [`AuthMechanism::Login`] represents the legacy IMAP LOGIN command in
    /// this crate, not the non-standard `AUTH=LOGIN` SASL mechanism. For
    /// clearer call sites, prefer [`supports_sasl_auth`](Self::supports_sasl_auth)
    /// for SASL and [`supports_login_command`](Self::supports_login_command)
    /// for the legacy command.
    pub fn supports_auth(&self, mechanism: AuthMechanism) -> bool {
        if mechanism == AuthMechanism::Login {
            return self.supports_login_command();
        }
        self.supports_sasl_auth(mechanism)
    }

    /// Return `true` if the server advertises a SASL AUTH mechanism.
    pub fn supports_sasl_auth(&self, mechanism: AuthMechanism) -> bool {
        if mechanism == AuthMechanism::Login {
            return false;
        }
        self.auth_mechanisms
            .iter()
            .any(|m| m.eq_ignore_ascii_case(mechanism.name()))
    }

    /// Return `true` if the legacy IMAP LOGIN command is available.
    pub fn supports_login_command(&self) -> bool {
        !self.supports(Capability::LoginDisabled)
    }

    /// Return `true` if the extension has been enabled.
    pub fn enabled(&self, extension: &str) -> bool {
        self.enabled
            .iter()
            .any(|enabled| enabled.eq_ignore_ascii_case(extension))
    }

    /// Return `true` when UIDPLUS commands are available.
    pub fn supports_uidplus(&self) -> bool {
        self.supports(Capability::UidPlus)
    }

    /// Return `true` when MOVE is available.
    pub fn supports_move(&self) -> bool {
        self.supports(Capability::Move)
    }

    /// Return `true` when CONDSTORE behavior is available.
    pub fn supports_condstore(&self) -> bool {
        self.supports(Capability::Condstore) || self.supports(Capability::QResync)
    }

    /// Return `true` when QRESYNC is advertised.
    pub fn supports_qresync(&self) -> bool {
        self.supports(Capability::QResync)
    }

    /// Return `true` when UTF8 mode is active or available.
    pub fn utf8_available(&self) -> bool {
        self.imap4rev2
            || self.supports(Capability::Utf8Accept)
            || self.supports(Capability::Utf8Only)
    }

    /// Return `true` when NOTIFY is available.
    pub fn supports_notify(&self) -> bool {
        self.supports(Capability::Notify)
    }

    /// Return `true` when COMPRESS=DEFLATE is available.
    pub fn supports_compress(&self) -> bool {
        self.supports(Capability::CompressDeflate)
    }

    fn rev2_implies(&self, capability: &Capability) -> bool {
        self.imap4rev2
            && matches!(
                capability,
                Capability::Binary
                    | Capability::Enable
                    | Capability::Esearch
                    | Capability::Idle
                    | Capability::ListExtended
                    | Capability::ListStatus
                    | Capability::LiteralMinus
                    | Capability::LiteralPlus
                    | Capability::Move
                    | Capability::Namespace
                    | Capability::ObjectId
                    | Capability::SaslIr
                    | Capability::SaveDate
                    | Capability::SearchRes
                    | Capability::SpecialUse
                    | Capability::StatusDeleted
                    | Capability::StatusSize
                    | Capability::UidPlus
                    | Capability::Unselect
            )
    }
}

fn imap4rev2_active(capabilities: &[Capability], enabled: &[String]) -> bool {
    let has_rev2 = capabilities.contains(&Capability::Imap4Rev2);
    let has_rev1 = capabilities.contains(&Capability::Imap4Rev1);
    if has_rev2 && has_rev1 {
        enabled
            .iter()
            .any(|extension| extension.eq_ignore_ascii_case("IMAP4rev2"))
    } else {
        has_rev2
    }
}

#[cfg(test)]
#[path = "profile_tests.rs"]
mod tests;
