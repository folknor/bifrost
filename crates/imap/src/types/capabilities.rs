use std::collections::HashSet;
use std::collections::hash_set::Iter;

use imap_proto::types::Capability as CapabilityRef;

use crate::authenticator::SaslAuthenticator;

const IMAP4REV1_CAPABILITY: &str = "IMAP4rev1";
const IMAP4REV2_CAPABILITY: &str = "IMAP4rev2";
const AUTH_CAPABILITY_PREFIX: &str = "AUTH=";

/// List of available Capabilities.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum Capability {
    /// IMAP4rev1 protocol support.
    Imap4rev1,
    /// IMAP4rev2 protocol support.
    Imap4rev2,
    /// Auth type capability.
    Auth(String),
    /// Any other atoms.
    Atom(String),
}

/// Highest IMAP protocol revision advertised by the server.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum ProtocolRevision {
    /// RFC 3501 IMAP4rev1.
    Imap4rev1,
    /// RFC 9051 IMAP4rev2.
    Imap4rev2,
}

impl From<&CapabilityRef<'_>> for Capability {
    fn from(c: &CapabilityRef<'_>) -> Self {
        match c {
            CapabilityRef::Imap4rev1 => Capability::Imap4rev1,
            CapabilityRef::Imap4rev2 => Capability::Imap4rev2,
            CapabilityRef::Auth(s) => Capability::Auth(s.clone().into_owned()),
            CapabilityRef::Atom(s) => Capability::Atom(s.clone().into_owned()),
            _ => Capability::Atom(format!("{c:?}")),
        }
    }
}

impl Capability {
    fn matches(&self, other: &Capability) -> bool {
        match (self, other) {
            (Capability::Imap4rev1, Capability::Imap4rev1) => true,
            (Capability::Imap4rev2, Capability::Imap4rev2) => true,
            (Capability::Auth(a), Capability::Auth(b))
            | (Capability::Atom(a), Capability::Atom(b)) => a.eq_ignore_ascii_case(b),
            _ => false,
        }
    }
}

/// From [section 7.2.1 of RFC 3501](https://tools.ietf.org/html/rfc3501#section-7.2.1).
///
/// A list of capabilities that the server supports.
/// The capability list will include `IMAP4rev1`, `IMAP4rev2`, or both.
///
/// In addition, IMAP4rev1 servers implement the `STARTTLS`, `LOGINDISABLED`, and `AUTH=PLAIN`
/// (described in [IMAP-TLS](https://tools.ietf.org/html/rfc2595)) capabilities. See the
/// [Security Considerations section of the RFC](https://tools.ietf.org/html/rfc3501#section-11)
/// for important information.
///
/// A capability name which begins with `AUTH=` indicates that the server supports that particular
/// authentication mechanism.
///
/// The `LOGINDISABLED` capability indicates that the `LOGIN` command is disabled, and that the
/// server will respond with a [`crate::error::Error::No`] response to any attempt to use the `LOGIN`
/// command even if the user name and password are valid.  An IMAP client MUST NOT issue the
/// `LOGIN` command if the server advertises the `LOGINDISABLED` capability.
///
/// Other capability names indicate that the server supports an extension, revision, or amendment
/// to the IMAP protocol. Capability names either begin with `X` or they are standard or
/// standards-track extensions, revisions, or amendments registered with IANA.
///
/// Client implementations SHOULD NOT require any capability name other than an IMAP4 protocol
/// revision, and MUST ignore any unknown capability names.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Capabilities(pub(crate) HashSet<Capability>);

impl Capabilities {
    /// Check if the server has the given capability.
    pub fn has(&self, cap: &Capability) -> bool {
        self.0.iter().any(|candidate| candidate.matches(cap))
    }

    /// Check if the server has the given capability.
    ///
    /// `AUTH=<mechanism>` values are matched as SASL mechanisms. Matching is
    /// ASCII-case-insensitive because IMAP capability atoms and SASL mechanism
    /// names are protocol tokens, not display strings.
    pub fn contains<S: AsRef<str>>(&self, cap: S) -> bool {
        let s = cap.as_ref();
        if s.eq_ignore_ascii_case(IMAP4REV1_CAPABILITY) {
            return self.has(&Capability::Imap4rev1);
        }
        if s.eq_ignore_ascii_case(IMAP4REV2_CAPABILITY) {
            return self.has(&Capability::Imap4rev2);
        }
        if s.len() > AUTH_CAPABILITY_PREFIX.len() {
            let (pre, val) = s.split_at(AUTH_CAPABILITY_PREFIX.len());
            if pre.eq_ignore_ascii_case(AUTH_CAPABILITY_PREFIX) {
                return self.supports_sasl(val);
            }
        }
        self.has_atom(s)
    }

    /// Returns true if the server advertised `IMAP4rev1`.
    pub fn supports_imap4rev1(&self) -> bool {
        self.has(&Capability::Imap4rev1)
    }

    /// Returns true if the server advertised `IMAP4rev2`.
    pub fn supports_imap4rev2(&self) -> bool {
        self.has(&Capability::Imap4rev2)
    }

    /// Highest IMAP protocol revision advertised by the server.
    pub fn protocol_revision(&self) -> Option<ProtocolRevision> {
        if self.supports_imap4rev2() {
            Some(ProtocolRevision::Imap4rev2)
        } else if self.supports_imap4rev1() {
            Some(ProtocolRevision::Imap4rev1)
        } else {
            None
        }
    }

    /// Returns true when `IMAP4rev2` is advertised alongside `IMAP4rev1`.
    ///
    /// RFC 9051 keeps those sessions in IMAP4rev1 mode until the client sends
    /// `ENABLE IMAP4rev2`.
    pub fn imap4rev2_requires_enable(&self) -> bool {
        self.supports_imap4rev1() && self.supports_imap4rev2()
    }

    /// Check if the server advertised an extension or capability atom.
    ///
    /// For SASL mechanisms, prefer [`Capabilities::supports_sasl`].
    pub fn has_atom<S: AsRef<str>>(&self, atom: S) -> bool {
        let atom = atom.as_ref();
        self.0.iter().any(|capability| match capability {
            Capability::Atom(candidate) => candidate.eq_ignore_ascii_case(atom),
            _ => false,
        })
    }

    /// Returns true if the server advertised `AUTH=<mechanism>`.
    pub fn supports_sasl<S: AsRef<str>>(&self, mechanism: S) -> bool {
        let mechanism = mechanism.as_ref();
        self.sasl_mechanisms()
            .any(|candidate| candidate.eq_ignore_ascii_case(mechanism))
    }

    /// Returns true if the server advertised the mechanism used by `A`.
    pub fn supports_sasl_authenticator<A: SaslAuthenticator>(&self) -> bool {
        self.supports_sasl(A::MECHANISM)
    }

    /// Iterate over the advertised SASL mechanisms from `AUTH=<mechanism>`.
    pub fn sasl_mechanisms(&self) -> impl Iterator<Item = &str> {
        self.0.iter().filter_map(|capability| match capability {
            Capability::Auth(mechanism) => Some(mechanism.as_str()),
            _ => None,
        })
    }

    /// Iterate over all the server's capabilities
    pub fn iter(&self) -> Iter<'_, Capability> {
        self.0.iter()
    }

    /// Returns how many capabilities the server has.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true if the server purports to have no capabilities.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
