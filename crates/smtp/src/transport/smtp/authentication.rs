//! Provides limited SASL authentication mechanisms

use std::fmt::{self, Debug, Display, Formatter};

use crate::transport::smtp::error::{self, Error};
pub use zeroize::Zeroizing;

/// Accepted password authentication mechanisms.
///
/// Trying LOGIN last as it is deprecated.
pub const PASSWORD_MECHANISMS: &[Mechanism] = &[Mechanism::Plain, Mechanism::Login];

/// Accepted OAuth 2.0 bearer-token authentication mechanisms.
///
/// `OAUTHBEARER` is the standard mechanism. `XOAUTH2` is kept for providers
/// that only expose the older non-standard mechanism.
pub const OAUTH2_MECHANISMS: &[Mechanism] = &[Mechanism::OAuthBearer, Mechanism::Xoauth2];

/// Default authentication mechanisms.
pub const DEFAULT_MECHANISMS: &[Mechanism] = PASSWORD_MECHANISMS;

/// Contains user credentials
#[derive(PartialEq, Eq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Credentials {
    /// Username and password credentials.
    Password {
        /// Authentication identity.
        username: String,
        /// Password or app password.
        password: Zeroizing<String>,
    },
    /// OAuth 2.0 bearer-token credentials.
    OAuth2 {
        /// Authorization identity, usually the email address being accessed.
        identity: String,
        /// OAuth 2.0 access token.
        access_token: Zeroizing<String>,
    },
}

/// Converts owned secret material into a zeroizing string.
pub trait IntoSecretString {
    /// Move the secret into zeroizing storage.
    fn into_secret_string(self) -> Zeroizing<String>;
}

impl IntoSecretString for String {
    fn into_secret_string(self) -> Zeroizing<String> {
        Zeroizing::new(self)
    }
}

impl IntoSecretString for &str {
    fn into_secret_string(self) -> Zeroizing<String> {
        Zeroizing::new(self.to_owned())
    }
}

impl IntoSecretString for Zeroizing<String> {
    fn into_secret_string(self) -> Zeroizing<String> {
        self
    }
}

impl Credentials {
    /// Create username and password credentials.
    pub fn password<U, P>(username: U, password: P) -> Credentials
    where
        U: Into<String>,
        P: IntoSecretString,
    {
        Credentials::Password {
            username: username.into(),
            password: password.into_secret_string(),
        }
    }

    /// Create OAuth 2.0 bearer-token credentials.
    pub fn oauth2<I, T>(identity: I, access_token: T) -> Credentials
    where
        I: Into<String>,
        T: IntoSecretString,
    {
        Credentials::OAuth2 {
            identity: identity.into(),
            access_token: access_token.into_secret_string(),
        }
    }

    pub(crate) fn preferred_mechanisms(&self) -> &'static [Mechanism] {
        match self {
            Credentials::Password { .. } => PASSWORD_MECHANISMS,
            Credentials::OAuth2 { .. } => OAUTH2_MECHANISMS,
        }
    }

    fn password_parts(&self) -> Result<(&str, &str), Error> {
        match self {
            Credentials::Password { username, password } => Ok((username, password.as_str())),
            Credentials::OAuth2 { .. } => Err(error::invalid_input(
                "OAuth2 credentials cannot be used with password authentication mechanisms",
            )),
        }
    }

    fn oauth2_parts(&self) -> Result<(&str, &str), Error> {
        match self {
            Credentials::OAuth2 {
                identity,
                access_token,
            } => Ok((identity, access_token.as_str())),
            Credentials::Password { .. } => Err(error::invalid_input(
                "password credentials cannot be used with OAuth2 authentication mechanisms",
            )),
        }
    }
}

impl Debug for Credentials {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Credentials::Password { .. } => f.debug_struct("Credentials::Password").finish(),
            Credentials::OAuth2 { .. } => f.debug_struct("Credentials::OAuth2").finish(),
        }
    }
}

/// Represents authentication mechanisms
#[derive(PartialEq, Eq, Copy, Clone, Hash, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Mechanism {
    /// PLAIN authentication mechanism, defined in
    /// [RFC 4616](https://tools.ietf.org/html/rfc4616)
    Plain,
    /// LOGIN authentication mechanism
    /// Obsolete but needed for some providers (like Office 365)
    ///
    /// Defined in [draft-murchison-sasl-login-00](https://www.ietf.org/archive/id/draft-murchison-sasl-login-00.txt).
    Login,
    /// Non-standard XOAUTH2 mechanism, defined in
    /// [xoauth2-protocol](https://developers.google.com/gmail/imap/xoauth2-protocol)
    Xoauth2,
    /// OAUTHBEARER authentication mechanism, defined in
    /// [RFC 7628](https://www.rfc-editor.org/rfc/rfc7628.html)
    OAuthBearer,
}

impl Display for Mechanism {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match *self {
            Mechanism::Plain => "PLAIN",
            Mechanism::Login => "LOGIN",
            Mechanism::Xoauth2 => "XOAUTH2",
            Mechanism::OAuthBearer => "OAUTHBEARER",
        })
    }
}

impl Mechanism {
    /// Does the mechanism support initial response?
    pub fn supports_initial_response(self) -> bool {
        match self {
            Mechanism::Plain | Mechanism::Xoauth2 | Mechanism::OAuthBearer => true,
            Mechanism::Login => false,
        }
    }

    /// Returns the string to send to the server, using the provided username, password and
    /// challenge in some cases
    pub fn response(
        self,
        credentials: &Credentials,
        challenge: Option<&str>,
    ) -> Result<String, Error> {
        match self {
            Mechanism::Plain => match challenge {
                Some(_) => Err(error::invalid_input(
                    "This mechanism does not expect a challenge",
                )),
                None => {
                    let (username, password) = credentials.password_parts()?;
                    Ok(format!("\u{0}{username}\u{0}{password}"))
                }
            },
            Mechanism::Login => {
                let (username, password) = credentials.password_parts()?;
                let decoded_challenge = challenge.ok_or_else(|| {
                    error::invalid_input("This mechanism does expect a challenge")
                })?;

                if contains_ignore_ascii_case(
                    decoded_challenge,
                    ["User Name", "Username:", "Username", "User Name\0"],
                ) {
                    return Ok(username.to_owned());
                }

                if contains_ignore_ascii_case(
                    decoded_challenge,
                    ["Password", "Password:", "Password\0"],
                ) {
                    return Ok(password.to_owned());
                }

                Err(error::invalid_input("Unrecognized challenge"))
            }
            Mechanism::Xoauth2 => match challenge {
                Some(_) => Err(error::invalid_input(
                    "This mechanism does not expect a challenge",
                )),
                None => {
                    let (identity, access_token) = credentials.oauth2_parts()?;
                    Ok(format!(
                        "user={identity}\x01auth=Bearer {access_token}\x01\x01"
                    ))
                }
            },
            Mechanism::OAuthBearer => match challenge {
                Some(_) => Ok("\x01".to_owned()),
                None => {
                    let (identity, access_token) = credentials.oauth2_parts()?;
                    let identity = gs2_escape(identity);
                    // RFC 7628 examples include host and port, but bearer
                    // token authentication does not require them. Keeping the
                    // mechanism encoder transport-agnostic avoids coupling
                    // credentials to SMTP connection state.
                    Ok(format!(
                        "n,a={identity},\x01auth=Bearer {access_token}\x01\x01"
                    ))
                }
            },
        }
    }
}

fn gs2_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            ',' => escaped.push_str("=2C"),
            '=' => escaped.push_str("=3D"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn contains_ignore_ascii_case<'a>(
    haystack: &str,
    needles: impl IntoIterator<Item = &'a str>,
) -> bool {
    needles
        .into_iter()
        .any(|item| item.eq_ignore_ascii_case(haystack))
}

#[cfg(test)]
mod test {
    use super::{Credentials, Mechanism, Zeroizing};

    #[test]
    fn test_plain() {
        let mechanism = Mechanism::Plain;

        let credentials = Credentials::password("username".to_owned(), "password".to_owned());

        assert_eq!(
            mechanism.response(&credentials, None).unwrap(),
            "\u{0}username\u{0}password"
        );
        assert!(mechanism.response(&credentials, Some("test")).is_err());
    }

    #[test]
    fn test_login() {
        let mechanism = Mechanism::Login;

        let credentials = Credentials::password("alice".to_owned(), "wonderland".to_owned());

        assert_eq!(
            mechanism.response(&credentials, Some("Username")).unwrap(),
            "alice"
        );
        assert_eq!(
            mechanism.response(&credentials, Some("Password")).unwrap(),
            "wonderland"
        );
        assert!(mechanism.response(&credentials, None).is_err());
    }

    #[test]
    fn test_login_case_insensitive() {
        let mechanism = Mechanism::Login;

        let credentials = Credentials::password("alice".to_owned(), "wonderland".to_owned());

        assert_eq!(
            mechanism.response(&credentials, Some("username")).unwrap(),
            "alice"
        );
        assert_eq!(
            mechanism.response(&credentials, Some("password")).unwrap(),
            "wonderland"
        );
        assert!(mechanism.response(&credentials, None).is_err());
    }

    #[test]
    fn test_xoauth2() {
        let mechanism = Mechanism::Xoauth2;

        let credentials = Credentials::oauth2(
            "username".to_owned(),
            "vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg==".to_owned(),
        );

        assert_eq!(
            mechanism.response(&credentials, None).unwrap(),
            "user=username\x01auth=Bearer vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg==\x01\x01"
        );
        assert!(mechanism.response(&credentials, Some("test")).is_err());
    }

    #[test]
    fn test_oauthbearer() {
        let mechanism = Mechanism::OAuthBearer;

        let credentials = Credentials::oauth2(
            "user@example.com".to_owned(),
            "vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg==".to_owned(),
        );

        let response = mechanism.response(&credentials, None).unwrap();
        assert_eq!(
            response,
            "n,a=user@example.com,\x01auth=Bearer vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg==\x01\x01"
        );
        assert!(!response.contains("\x01host="));
        assert!(!response.contains("\x01port="));
        assert_eq!(
            mechanism.response(&credentials, Some("{}")).unwrap(),
            "\x01"
        );
        assert_eq!(
            mechanism
                .response(&credentials, Some(r#"{"status":"invalid_token"}"#))
                .unwrap(),
            "\x01"
        );
    }

    #[test]
    fn test_oauthbearer_escapes_gs2_identity() {
        let mechanism = Mechanism::OAuthBearer;
        let credentials = Credentials::oauth2("a,b=c".to_owned(), "token".to_owned());

        assert_eq!(
            mechanism.response(&credentials, None).unwrap(),
            "n,a=a=2Cb=3Dc,\x01auth=Bearer token\x01\x01"
        );
    }

    #[test]
    fn test_rejects_wrong_credential_kind() {
        assert!(
            Mechanism::Plain
                .response(
                    &Credentials::oauth2("alice".to_owned(), "token".to_owned()),
                    None
                )
                .is_err()
        );
        assert!(
            Mechanism::Xoauth2
                .response(
                    &Credentials::password("alice".to_owned(), "wonderland".to_owned()),
                    None,
                )
                .is_err()
        );
    }

    #[test]
    fn test_accepts_zeroizing_secrets() {
        let credentials = Credentials::oauth2(
            "alice".to_owned(),
            Zeroizing::new("access-token".to_owned()),
        );

        assert_eq!(
            Mechanism::Xoauth2.response(&credentials, None).unwrap(),
            "user=alice\x01auth=Bearer access-token\x01\x01"
        );
    }
}
