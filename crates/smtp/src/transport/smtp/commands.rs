//! SMTP commands

use std::fmt::{self, Display, Formatter};

use crate::{
    address::Address,
    transport::smtp::{
        authentication::{Credentials, Mechanism},
        error::{self, Error},
        extension::{ClientId, MailParameter, RcptParameter},
        response::Response,
    },
};

fn validate_single_line_argument(command: &str, argument: &str) -> Result<(), Error> {
    if argument.chars().any(char::is_control) {
        return Err(error::invalid_input(format!(
            "{command} argument must not contain control characters"
        )));
    }

    Ok(())
}

/// EHLO command
#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Ehlo {
    client_id: ClientId,
}

impl Display for Ehlo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "EHLO {}\r\n", self.client_id)
    }
}

impl Ehlo {
    /// Creates a EHLO command
    pub(crate) fn new(client_id: ClientId) -> Ehlo {
        Ehlo { client_id }
    }
}

/// LHLO command
#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Lhlo {
    client_id: ClientId,
}

impl Display for Lhlo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "LHLO {}\r\n", self.client_id)
    }
}

impl Lhlo {
    /// Creates a LHLO command
    pub(crate) fn new(client_id: ClientId) -> Lhlo {
        Lhlo { client_id }
    }
}

/// STARTTLS command
#[derive(PartialEq, Eq, Clone, Debug, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Starttls;

impl Display for Starttls {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("STARTTLS\r\n")
    }
}

/// MAIL command
#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Mail {
    sender: Option<Address>,
    parameters: Vec<MailParameter>,
}

impl Display for Mail {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MAIL FROM:<{}>",
            self.sender.as_ref().map_or("", |s| s.as_ref())
        )?;
        for parameter in &self.parameters {
            write!(f, " {parameter}")?;
        }
        f.write_str("\r\n")
    }
}

impl Mail {
    /// Creates a MAIL command
    pub(crate) fn new(sender: Option<Address>, parameters: Vec<MailParameter>) -> Mail {
        Mail { sender, parameters }
    }
}

/// RCPT command
#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Rcpt {
    recipient: Address,
    parameters: Vec<RcptParameter>,
}

impl Display for Rcpt {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RCPT TO:<{}>", self.recipient)?;
        for parameter in &self.parameters {
            write!(f, " {parameter}")?;
        }
        f.write_str("\r\n")
    }
}

impl Rcpt {
    /// Creates an RCPT command
    pub(crate) fn new(recipient: Address, parameters: Vec<RcptParameter>) -> Rcpt {
        Rcpt {
            recipient,
            parameters,
        }
    }
}

/// DATA command
#[derive(PartialEq, Eq, Clone, Debug, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Data;

impl Display for Data {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("DATA\r\n")
    }
}

/// BDAT command
#[derive(PartialEq, Eq, Clone, Debug, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Bdat {
    size: usize,
    last: bool,
}

impl Display for Bdat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "BDAT {}", self.size)?;
        if self.last {
            f.write_str(" LAST")?;
        }
        f.write_str("\r\n")
    }
}

impl Bdat {
    /// Creates a BDAT command for the final chunk.
    pub(crate) fn last(size: usize) -> Bdat {
        Bdat { size, last: true }
    }
}

/// NOOP command
#[derive(PartialEq, Eq, Clone, Debug, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Noop;

impl Display for Noop {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("NOOP\r\n")
    }
}

/// VRFY command
#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Vrfy {
    argument: String,
}

impl Display for Vrfy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "VRFY {}\r\n", self.argument)
    }
}

impl Vrfy {
    /// Creates a VRFY command
    pub(crate) fn new(argument: String) -> Result<Vrfy, Error> {
        validate_single_line_argument("VRFY", &argument)?;
        Ok(Vrfy { argument })
    }
}

/// EXPN command
#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Expn {
    argument: String,
}

impl Display for Expn {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "EXPN {}\r\n", self.argument)
    }
}

impl Expn {
    /// Creates an EXPN command
    pub(crate) fn new(argument: String) -> Result<Expn, Error> {
        validate_single_line_argument("EXPN", &argument)?;
        Ok(Expn { argument })
    }
}

/// RSET command
#[derive(PartialEq, Eq, Clone, Debug, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Rset;

impl Display for Rset {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("RSET\r\n")
    }
}

/// AUTH command
//
// No `PartialEq`/`Eq`/`serde` derives: `Credentials` carries a live
// `Arc<dyn TokenSource>` for OAuth, which is neither comparable nor
// serializable. The wire bytes flow through `Display` over the
// precomputed `response`, so these derives were never load-bearing.
#[derive(Clone)]
pub(crate) struct Auth {
    mechanism: Mechanism,
    challenge: Option<String>,
    response: Option<String>,
}

impl Display for Auth {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let encoded_response = self.response.as_ref().map(crate::base64::encode);

        if self.challenge.is_some() {
            f.write_str(&encoded_response.unwrap())?;
        } else if self.mechanism.supports_initial_response() {
            write!(f, "AUTH {} {}", self.mechanism, encoded_response.unwrap())?;
        } else {
            match encoded_response {
                Some(response) => f.write_str(&response)?,
                None => write!(f, "AUTH {}", self.mechanism)?,
            }
        }
        f.write_str("\r\n")
    }
}

impl Auth {
    /// Creates an AUTH command (from a challenge if provided).
    ///
    /// `oauth_token` is the access token resolved by the connection's
    /// auth driver from the credential's `TokenSource`; `None` for
    /// password / SCRAM mechanisms.
    pub(crate) fn new(
        mechanism: Mechanism,
        credentials: Credentials,
        challenge: Option<String>,
        oauth_token: Option<&str>,
    ) -> Result<Auth, Error> {
        let response = if mechanism.supports_initial_response() || challenge.is_some() {
            Some(mechanism.response_with_token(&credentials, challenge.as_deref(), oauth_token)?)
        } else {
            None
        };
        Ok(Auth {
            mechanism,
            challenge,
            response,
        })
    }

    /// Creates an AUTH command from a response that needs to be a
    /// valid challenge (with 334 response code)
    pub(crate) fn new_from_response(
        mechanism: Mechanism,
        credentials: Credentials,
        response: &Response,
        oauth_token: Option<&str>,
    ) -> Result<Auth, Error> {
        if !response.has_code(334) {
            return Err(error::parse("Expecting a challenge"));
        }

        let encoded_challenge = response
            .first_word()
            .ok_or_else(|| error::parse("Could not read auth challenge"))?;
        #[cfg(feature = "tracing")]
        tracing::debug!("auth encoded challenge: {}", encoded_challenge);

        let decoded_base64 = crate::base64::decode(encoded_challenge).map_err(error::parse)?;
        let decoded_challenge = String::from_utf8(decoded_base64).map_err(error::parse)?;
        #[cfg(feature = "tracing")]
        tracing::debug!("auth decoded challenge: {}", decoded_challenge);

        let response = Some(mechanism.response_with_token(
            &credentials,
            Some(decoded_challenge.as_ref()),
            oauth_token,
        )?);

        Ok(Auth {
            mechanism,
            challenge: Some(decoded_challenge),
            response,
        })
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use super::*;
    use crate::transport::smtp::extension::MailBodyParameter;

    #[test]
    fn test_display() {
        let id = ClientId::Domain("localhost".to_owned());
        let email = Address::from_str("test@example.com").unwrap();
        let mail_parameter = MailParameter::Other {
            keyword: "TEST".to_owned(),
            value: Some("value".to_owned()),
        };
        let rcpt_parameter = RcptParameter::Other {
            keyword: "TEST".to_owned(),
            value: Some("value".to_owned()),
        };
        assert_eq!(format!("{}", Ehlo::new(id.clone())), "EHLO localhost\r\n");
        assert_eq!(format!("{}", Lhlo::new(id)), "LHLO localhost\r\n");
        assert_eq!(
            format!("{}", Mail::new(Some(email.clone()), vec![])),
            "MAIL FROM:<test@example.com>\r\n"
        );
        assert_eq!(format!("{}", Mail::new(None, vec![])), "MAIL FROM:<>\r\n");
        assert_eq!(
            format!(
                "{}",
                Mail::new(Some(email.clone()), vec![MailParameter::Size(42)])
            ),
            "MAIL FROM:<test@example.com> SIZE=42\r\n"
        );
        assert_eq!(
            format!(
                "{}",
                Mail::new(
                    Some(email.clone()),
                    vec![
                        MailParameter::Size(42),
                        MailParameter::Body(MailBodyParameter::EightBitMime),
                        mail_parameter,
                    ],
                )
            ),
            "MAIL FROM:<test@example.com> SIZE=42 BODY=8BITMIME TEST=value\r\n"
        );
        assert_eq!(
            format!("{}", Rcpt::new(email.clone(), vec![])),
            "RCPT TO:<test@example.com>\r\n"
        );
        assert_eq!(
            format!("{}", Rcpt::new(email, vec![rcpt_parameter])),
            "RCPT TO:<test@example.com> TEST=value\r\n"
        );
        assert_eq!(format!("{}", Bdat::last(42)), "BDAT 42 LAST\r\n");
        assert_eq!(format!("{Data}"), "DATA\r\n");
        assert_eq!(format!("{Noop}"), "NOOP\r\n");
        assert_eq!(
            format!("{}", Vrfy::new("test".to_owned()).unwrap()),
            "VRFY test\r\n"
        );
        assert_eq!(
            format!("{}", Expn::new("test".to_owned()).unwrap()),
            "EXPN test\r\n"
        );
        assert!(Vrfy::new("safe\r\nNOOP".to_owned()).is_err());
        assert!(Expn::new("safe\u{85}NOOP".to_owned()).is_err());
        assert_eq!(format!("{Rset}"), "RSET\r\n");
        let credentials = Credentials::password("user".to_owned(), "password".to_owned());
        assert_eq!(
            format!(
                "{}",
                Auth::new(Mechanism::Plain, credentials.clone(), None, None).unwrap()
            ),
            "AUTH PLAIN AHVzZXIAcGFzc3dvcmQ=\r\n"
        );
        assert_eq!(
            format!(
                "{}",
                Auth::new(Mechanism::Login, credentials, None, None).unwrap()
            ),
            "AUTH LOGIN\r\n"
        );
        let credentials = Credentials::oauth2("user".to_owned(), "token".to_owned());
        assert_eq!(
            format!(
                "{}",
                Auth::new(Mechanism::Xoauth2, credentials.clone(), None, Some("token")).unwrap()
            ),
            "AUTH XOAUTH2 dXNlcj11c2VyAWF1dGg9QmVhcmVyIHRva2VuAQE=\r\n"
        );
        assert_eq!(
            format!(
                "{}",
                Auth::new(
                    Mechanism::OAuthBearer,
                    credentials.clone(),
                    None,
                    Some("token")
                )
                .unwrap()
            ),
            "AUTH OAUTHBEARER bixhPXVzZXIsAWF1dGg9QmVhcmVyIHRva2VuAQE=\r\n"
        );
        let continuation = Response::new(
            crate::transport::smtp::response::Code {
                severity: crate::transport::smtp::response::Severity::PositiveIntermediate,
                category: crate::transport::smtp::response::Category::Unspecified3,
                detail: crate::transport::smtp::response::Detail::Four,
            },
            vec![crate::base64::encode("{}")],
        );
        assert_eq!(
            format!(
                "{}",
                Auth::new_from_response(
                    Mechanism::OAuthBearer,
                    credentials,
                    &continuation,
                    Some("token")
                )
                .unwrap()
            ),
            "AQ==\r\n"
        );
    }

    #[test]
    fn xoauth2_challenge_emits_dummy_cancel() {
        // A failed XOAUTH2 auth returns `334 <base64-json-error>`. Building the
        // continuation reply from that challenge must emit the dummy-cancel
        // line (`AQ==` = base64 of `\x01`), matching OAUTHBEARER, so the server
        // emits the tagged failure reply instead of the exchange leaking
        // through as an untagged "does not expect a challenge" error.
        let credentials = Credentials::oauth2("user".to_owned(), "token".to_owned());
        let continuation = Response::new(
            crate::transport::smtp::response::Code {
                severity: crate::transport::smtp::response::Severity::PositiveIntermediate,
                category: crate::transport::smtp::response::Category::Unspecified3,
                detail: crate::transport::smtp::response::Detail::Four,
            },
            vec![crate::base64::encode(r#"{"status":"401"}"#)],
        );
        assert_eq!(
            format!(
                "{}",
                Auth::new_from_response(
                    Mechanism::Xoauth2,
                    credentials,
                    &continuation,
                    Some("token")
                )
                .unwrap()
            ),
            "AQ==\r\n"
        );
    }

    #[test]
    fn ehlo_and_lhlo_client_ids_are_validated_before_the_driver_writes_them() {
        let id = ClientId::Domain("host\r\nRSET".to_owned());

        assert!(id.validate().is_err());
        assert!(ClientId::domain("mail.example.org").is_ok());
    }

    #[test]
    fn vrfy_and_expn_reject_every_control_character() {
        assert!(Vrfy::new("ok\r\nRSET".to_owned()).is_err());
        assert!(Vrfy::new("ok\nRSET".to_owned()).is_err());
        assert!(Vrfy::new("ok\0".to_owned()).is_err());
        assert!(Expn::new("list\u{7f}".to_owned()).is_err());
        assert!(Expn::new("list\u{9f}".to_owned()).is_err());
        // A plain argument with an embedded space is still fine: the command
        // is a single line, spaces are not a framing character.
        assert!(Vrfy::new("Smith John".to_owned()).is_ok());
    }

    #[test]
    fn bdat_renders_a_zero_length_final_chunk() {
        assert_eq!(format!("{}", Bdat::last(0)), "BDAT 0 LAST\r\n");
    }

    #[test]
    fn mail_from_renders_parameters_in_insertion_order() {
        let email = Address::from_str("test@example.com").unwrap();
        let mail = Mail::new(
            Some(email),
            vec![
                MailParameter::Size(10),
                MailParameter::RequireTls,
                MailParameter::SmtpUtfEight,
            ],
        );

        assert_eq!(
            format!("{mail}"),
            "MAIL FROM:<test@example.com> SIZE=10 REQUIRETLS SMTPUTF8\r\n"
        );
    }

    #[test]
    fn auth_continuation_requires_a_334_challenge() {
        let credentials = Credentials::password("user".to_owned(), "password".to_owned());
        let not_a_challenge = Response::new(
            crate::transport::smtp::response::Code {
                severity: crate::transport::smtp::response::Severity::PositiveCompletion,
                category: crate::transport::smtp::response::Category::MailSystem,
                detail: crate::transport::smtp::response::Detail::Zero,
            },
            vec![crate::base64::encode("x")],
        );

        assert!(
            Auth::new_from_response(Mechanism::Plain, credentials, &not_a_challenge, None).is_err()
        );
    }

    #[test]
    fn auth_continuation_rejects_a_non_base64_challenge() {
        let credentials = Credentials::password("user".to_owned(), "password".to_owned());
        let challenge = Response::new(
            crate::transport::smtp::response::Code {
                severity: crate::transport::smtp::response::Severity::PositiveIntermediate,
                category: crate::transport::smtp::response::Category::Unspecified3,
                detail: crate::transport::smtp::response::Detail::Four,
            },
            vec!["!!!!".to_owned()],
        );

        assert!(Auth::new_from_response(Mechanism::Plain, credentials, &challenge, None).is_err());
    }

    #[test]
    fn scram_initial_command_is_bare_auth() {
        // SCRAM has no initial response and is driven by the SCRAM exchange,
        // so `Auth::new(.., None)` must not invoke `Mechanism::response` (which
        // errors for SCRAM) and must emit a bare `AUTH SCRAM-SHA-256`.
        let credentials = Credentials::password("user".to_owned(), "password".to_owned());
        let auth = Auth::new(Mechanism::ScramSha256, credentials, None, None).unwrap();
        assert_eq!(format!("{auth}"), "AUTH SCRAM-SHA-256\r\n");
    }
}
