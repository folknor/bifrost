use std::{borrow::Cow, fmt};

/// This trait allows for pluggable authentication schemes. It is used by [`Client::authenticate`]
/// to [authenticate using SASL](https://tools.ietf.org/html/rfc3501#section-6.2.2).
///
/// [`Client::authenticate`]: crate::Client::authenticate
pub trait Authenticator {
    /// The type of the response to the challenge. This will usually be a `Vec<u8>` or `String`.
    type Response: AsRef<[u8]>;

    /// Each base64-decoded server challenge is passed to `process`.
    /// The returned byte-string is base64-encoded and then sent back to the server.
    fn process(&mut self, challenge: &[u8]) -> Self::Response;
}

/// A SASL authenticator with a known mechanism name.
///
/// Use this with [`Client::authenticate_with`] when the mechanism is known by
/// the authenticator type, avoiding a duplicated string such as `"XOAUTH2"` at
/// the call site.
/// Implementations should provide a valid SASL mechanism name. Invalid names
/// are rejected by [`Client::authenticate_with`] before transmission.
///
/// [`Client::authenticate_with`]: crate::Client::authenticate_with
pub trait SaslAuthenticator: Authenticator {
    /// The SASL mechanism name used in the `AUTHENTICATE` command.
    const MECHANISM: &'static str;
}

/// SASL PLAIN authentication.
///
/// The response payload is `authzid NUL authcid NUL password` as defined by
/// RFC 4616. Use [`Plain::new`] for the common empty authorization identity,
/// or [`Plain::with_authorization_identity`] when authenticating as one
/// identity and authorizing as another.
///
/// `Debug` redacts the password, but includes the authorization and
/// authentication identities.
#[derive(Clone, Eq, PartialEq)]
pub struct Plain<'a> {
    authorization_identity: Cow<'a, str>,
    authentication_identity: Cow<'a, str>,
    password: Cow<'a, str>,
}

impl fmt::Debug for Plain<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Plain")
            .field("authorization_identity", &self.authorization_identity)
            .field("authentication_identity", &self.authentication_identity)
            .field("password", &"<redacted>")
            .finish()
    }
}

impl<'a> Plain<'a> {
    /// Create a PLAIN authenticator with an empty authorization identity.
    pub fn new(
        authentication_identity: impl Into<Cow<'a, str>>,
        password: impl Into<Cow<'a, str>>,
    ) -> Self {
        Self::with_authorization_identity("", authentication_identity, password)
    }

    /// Create a PLAIN authenticator with an explicit authorization identity.
    pub fn with_authorization_identity(
        authorization_identity: impl Into<Cow<'a, str>>,
        authentication_identity: impl Into<Cow<'a, str>>,
        password: impl Into<Cow<'a, str>>,
    ) -> Self {
        Self {
            authorization_identity: authorization_identity.into(),
            authentication_identity: authentication_identity.into(),
            password: password.into(),
        }
    }
}

impl Authenticator for Plain<'_> {
    type Response = Vec<u8>;

    fn process(&mut self, _: &[u8]) -> Self::Response {
        let authorization_identity = self.authorization_identity.as_bytes();
        let authentication_identity = self.authentication_identity.as_bytes();
        let password = self.password.as_bytes();

        let mut response = Vec::with_capacity(
            authorization_identity.len() + authentication_identity.len() + password.len() + 2,
        );
        response.extend_from_slice(authorization_identity);
        response.push(0);
        response.extend_from_slice(authentication_identity);
        response.push(0);
        response.extend_from_slice(password);
        response
    }
}

impl SaslAuthenticator for Plain<'_> {
    const MECHANISM: &'static str = "PLAIN";
}

/// SASL XOAUTH2 bearer-token authentication.
///
/// The response payload is the format used by Gmail and other IMAP servers
/// that advertise `AUTH=XOAUTH2`.
///
/// `Debug` redacts the access token, but includes the username.
#[derive(Clone, Eq, PartialEq)]
pub struct XOAuth2<'a> {
    username: Cow<'a, str>,
    access_token: Cow<'a, str>,
}

impl fmt::Debug for XOAuth2<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("XOAuth2")
            .field("username", &self.username)
            .field("access_token", &"<redacted>")
            .finish()
    }
}

impl<'a> XOAuth2<'a> {
    /// Create an XOAUTH2 authenticator for the given user and bearer token.
    pub fn new(username: impl Into<Cow<'a, str>>, access_token: impl Into<Cow<'a, str>>) -> Self {
        Self {
            username: username.into(),
            access_token: access_token.into(),
        }
    }
}

impl Authenticator for XOAuth2<'_> {
    type Response = Vec<u8>;

    fn process(&mut self, _: &[u8]) -> Self::Response {
        let username = self.username.as_bytes();
        let access_token = self.access_token.as_bytes();

        let mut response = Vec::with_capacity(
            b"user=".len() + username.len() + b"\x01auth=Bearer ".len() + access_token.len() + 2,
        );
        response.extend_from_slice(b"user=");
        response.extend_from_slice(username);
        response.extend_from_slice(b"\x01auth=Bearer ");
        response.extend_from_slice(access_token);
        response.extend_from_slice(b"\x01\x01");
        response
    }
}

impl SaslAuthenticator for XOAuth2<'_> {
    const MECHANISM: &'static str = "XOAUTH2";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_authenticator_secrets() {
        let plain = format!("{:?}", Plain::new("user", "secret-password"));
        assert!(plain.contains("user"));
        assert!(plain.contains("<redacted>"));
        assert!(!plain.contains("secret-password"));

        let xoauth2 = format!("{:?}", XOAuth2::new("user", "secret-token"));
        assert!(xoauth2.contains("user"));
        assert!(xoauth2.contains("<redacted>"));
        assert!(!xoauth2.contains("secret-token"));
    }
}
