//! ESMTP features

use std::{
    collections::HashSet,
    fmt::{self, Display, Formatter},
    net::{Ipv4Addr, Ipv6Addr},
};

use crate::{
    address::Address,
    transport::smtp::{
        authentication::Mechanism,
        error::{self, Error},
        response::Response,
        util::XText,
    },
};

/// Client identifier, the parameter to `EHLO`
#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum ClientId {
    /// A fully-qualified domain name
    Domain(String),
    /// An IPv4 address
    Ipv4(Ipv4Addr),
    /// An IPv6 address
    Ipv6(Ipv6Addr),
}

const LOCALHOST_CLIENT: ClientId = ClientId::Ipv4(Ipv4Addr::new(127, 0, 0, 1));

impl Default for ClientId {
    fn default() -> Self {
        // https://tools.ietf.org/html/rfc5321#section-4.1.4
        //
        // The SMTP client MUST, if possible, ensure that the domain parameter
        // to the EHLO command is a primary host name as specified for this
        // command in Section 2.3.5.  If this is not possible (e.g., when the
        // client's address is dynamically assigned and the client does not have
        // an obvious name), an address literal SHOULD be substituted for the
        // domain name.
        #[cfg(feature = "hostname")]
        {
            hostname::get()
                .ok()
                .and_then(|s| s.into_string().map(Self::Domain).ok())
                .unwrap_or(LOCALHOST_CLIENT)
        }
        #[cfg(not(feature = "hostname"))]
        LOCALHOST_CLIENT
    }
}

impl Display for ClientId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Domain(value) => f.write_str(value),
            Self::Ipv4(value) => write!(f, "[{value}]"),
            Self::Ipv6(value) => write!(f, "[IPv6:{value}]"),
        }
    }
}

/// Supported ESMTP keywords
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum Extension {
    /// 8BITMIME keyword
    ///
    /// Defined in [RFC 6152](https://tools.ietf.org/html/rfc6152)
    EightBitMime,
    /// PIPELINING keyword
    ///
    /// Defined in [RFC 2920](https://www.rfc-editor.org/rfc/rfc2920)
    Pipelining,
    /// SIZE keyword with an optional advertised limit
    ///
    /// Defined in [RFC 1870](https://www.rfc-editor.org/rfc/rfc1870)
    Size(Option<usize>),
    /// SMTPUTF8 keyword
    ///
    /// Defined in [RFC 6531](https://tools.ietf.org/html/rfc6531)
    SmtpUtfEight,
    /// STARTTLS keyword
    ///
    /// Defined in [RFC 2487](https://tools.ietf.org/html/rfc2487)
    StartTls,
    /// CHUNKING keyword
    ///
    /// Defined in [RFC 3030](https://www.rfc-editor.org/rfc/rfc3030)
    Chunking,
    /// BINARYMIME keyword
    ///
    /// Defined in [RFC 3030](https://www.rfc-editor.org/rfc/rfc3030)
    BinaryMime,
    /// ENHANCEDSTATUSCODES keyword
    ///
    /// Defined in [RFC 2034](https://www.rfc-editor.org/rfc/rfc2034)
    EnhancedStatusCodes,
    /// DSN keyword
    ///
    /// Defined in [RFC 3461](https://www.rfc-editor.org/rfc/rfc3461)
    Dsn,
    /// REQUIRETLS keyword
    ///
    /// Defined in [RFC 8689](https://www.rfc-editor.org/rfc/rfc8689)
    RequireTls,
    /// FUTURERELEASE keyword with optional advertised limits
    ///
    /// Defined in [RFC 4865](https://www.rfc-editor.org/rfc/rfc4865)
    FutureRelease {
        /// Maximum hold interval in seconds, if advertised.
        max_interval: Option<u64>,
        /// Maximum hold-until datetime, if advertised.
        max_datetime: Option<String>,
    },
    /// DELIVERBY keyword with an optional advertised minimum
    ///
    /// Defined in [RFC 2852](https://www.rfc-editor.org/rfc/rfc2852)
    DeliverBy(Option<i64>),
    /// MT-PRIORITY keyword
    ///
    /// Defined in [RFC 6710](https://www.rfc-editor.org/rfc/rfc6710)
    MtPriority,
    /// VRFY keyword
    ///
    /// Defined in [RFC 5321](https://www.rfc-editor.org/rfc/rfc5321)
    Vrfy,
    /// EXPN keyword
    ///
    /// Defined in [RFC 5321](https://www.rfc-editor.org/rfc/rfc5321)
    Expn,
    /// AUTH mechanism
    Authentication(Mechanism),
}

impl Display for Extension {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Extension::EightBitMime => f.write_str("8BITMIME"),
            Extension::Pipelining => f.write_str("PIPELINING"),
            Extension::Size(Some(limit)) => write!(f, "SIZE {limit}"),
            Extension::Size(None) => f.write_str("SIZE"),
            Extension::SmtpUtfEight => f.write_str("SMTPUTF8"),
            Extension::StartTls => f.write_str("STARTTLS"),
            Extension::Chunking => f.write_str("CHUNKING"),
            Extension::BinaryMime => f.write_str("BINARYMIME"),
            Extension::EnhancedStatusCodes => f.write_str("ENHANCEDSTATUSCODES"),
            Extension::Dsn => f.write_str("DSN"),
            Extension::RequireTls => f.write_str("REQUIRETLS"),
            Extension::FutureRelease {
                max_interval,
                max_datetime,
            } => {
                f.write_str("FUTURERELEASE")?;
                if let Some(interval) = max_interval {
                    write!(f, " {interval}")?;
                }
                if let Some(datetime) = max_datetime {
                    write!(f, " {datetime}")?;
                }
                Ok(())
            }
            Extension::DeliverBy(Some(minimum)) => write!(f, "DELIVERBY {minimum}"),
            Extension::DeliverBy(None) => f.write_str("DELIVERBY"),
            Extension::MtPriority => f.write_str("MT-PRIORITY"),
            Extension::Vrfy => f.write_str("VRFY"),
            Extension::Expn => f.write_str("EXPN"),
            Extension::Authentication(mechanism) => write!(f, "AUTH {mechanism}"),
        }
    }
}

/// Contains information about an SMTP server
#[derive(Clone, Debug, Eq, PartialEq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ServerInfo {
    /// Server name
    ///
    /// The name given in the server banner
    name: String,
    /// ESMTP features supported by the server
    ///
    /// It contains the features supported by the server and known by the `Extension` module.
    features: HashSet<Extension>,
}

impl Display for ServerInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let features = if self.features.is_empty() {
            "no supported features".to_owned()
        } else {
            format!("{:?}", self.features)
        };
        write!(f, "{} with {}", self.name, features)
    }
}

impl ServerInfo {
    /// Parses a EHLO response to create a `ServerInfo`
    pub fn from_response(response: &Response) -> Result<ServerInfo, Error> {
        let Some(name) = response.first_word() else {
            return Err(error::response("Could not read server name"));
        };

        let mut features: HashSet<Extension> = HashSet::new();

        for line in response.message() {
            if line.is_empty() {
                continue;
            }

            let mut split = line.split_whitespace();
            let keyword = split.next().unwrap();
            match keyword.to_ascii_uppercase().as_str() {
                "8BITMIME" => {
                    features.insert(Extension::EightBitMime);
                }
                "PIPELINING" => {
                    features.insert(Extension::Pipelining);
                }
                "SIZE" => {
                    features.insert(Extension::Size(
                        split.next().and_then(|limit| limit.parse().ok()),
                    ));
                }
                "SMTPUTF8" => {
                    features.insert(Extension::SmtpUtfEight);
                }
                "STARTTLS" => {
                    features.insert(Extension::StartTls);
                }
                "CHUNKING" => {
                    features.insert(Extension::Chunking);
                }
                "BINARYMIME" => {
                    features.insert(Extension::BinaryMime);
                }
                "ENHANCEDSTATUSCODES" => {
                    features.insert(Extension::EnhancedStatusCodes);
                }
                "DSN" => {
                    features.insert(Extension::Dsn);
                }
                "REQUIRETLS" => {
                    features.insert(Extension::RequireTls);
                }
                "FUTURERELEASE" => {
                    let max_interval =
                        match split.next() {
                            Some(value) => Some(value.parse::<u64>().map_err(|_| {
                                error::response("invalid FUTURERELEASE max-interval")
                            })?),
                            None => None,
                        };
                    let max_datetime = split.next().map(str::to_owned);
                    if split.next().is_some() {
                        return Err(error::response("invalid FUTURERELEASE EHLO response"));
                    }
                    features.insert(Extension::FutureRelease {
                        max_interval,
                        max_datetime,
                    });
                }
                "DELIVERBY" => {
                    features.insert(Extension::DeliverBy(
                        split.next().and_then(|minimum| minimum.parse().ok()),
                    ));
                }
                "MT-PRIORITY" => {
                    features.insert(Extension::MtPriority);
                }
                "VRFY" => {
                    features.insert(Extension::Vrfy);
                }
                "EXPN" => {
                    features.insert(Extension::Expn);
                }
                "AUTH" => {
                    for mechanism in split {
                        match mechanism.to_ascii_uppercase().as_str() {
                            "PLAIN" => {
                                features.insert(Extension::Authentication(Mechanism::Plain));
                            }
                            "LOGIN" => {
                                features.insert(Extension::Authentication(Mechanism::Login));
                            }
                            "XOAUTH2" => {
                                features.insert(Extension::Authentication(Mechanism::Xoauth2));
                            }
                            "OAUTHBEARER" => {
                                features.insert(Extension::Authentication(Mechanism::OAuthBearer));
                            }
                            _ => (),
                        }
                    }
                }
                _ => (),
            }
        }

        Ok(ServerInfo {
            name: name.to_owned(),
            features,
        })
    }

    /// Checks if the server supports an ESMTP feature
    pub fn supports_feature(&self, keyword: Extension) -> bool {
        self.features.contains(&keyword)
    }

    /// Returns the server-advertised SIZE limit, if any.
    pub fn size_limit(&self) -> Option<usize> {
        self.features.iter().find_map(|feature| {
            if let Extension::Size(limit) = feature {
                *limit
            } else {
                None
            }
        })
    }

    /// Checks if the server supports SIZE.
    pub fn supports_size(&self) -> bool {
        self.features
            .iter()
            .any(|feature| matches!(feature, Extension::Size(_)))
    }

    /// Checks if the server supports PIPELINING.
    pub fn supports_pipelining(&self) -> bool {
        self.supports_feature(Extension::Pipelining)
    }

    /// Checks if the server supports CHUNKING.
    pub fn supports_chunking(&self) -> bool {
        self.supports_feature(Extension::Chunking)
    }

    /// Checks if the server supports BINARYMIME.
    pub fn supports_binary_mime(&self) -> bool {
        self.supports_feature(Extension::BinaryMime)
    }

    /// Checks if the server supports ENHANCEDSTATUSCODES.
    pub fn supports_enhanced_status_codes(&self) -> bool {
        self.supports_feature(Extension::EnhancedStatusCodes)
    }

    /// Checks if the server supports DSN.
    pub fn supports_dsn(&self) -> bool {
        self.supports_feature(Extension::Dsn)
    }

    /// Checks if the server supports REQUIRETLS.
    pub fn supports_require_tls(&self) -> bool {
        self.supports_feature(Extension::RequireTls)
    }

    /// Checks if the server supports FUTURERELEASE.
    pub fn supports_future_release(&self) -> bool {
        self.features
            .iter()
            .any(|feature| matches!(feature, Extension::FutureRelease { .. }))
    }

    /// Returns the server-advertised FUTURERELEASE maximum hold interval.
    pub fn future_release_max_interval(&self) -> Option<u64> {
        self.features.iter().find_map(|feature| {
            if let Extension::FutureRelease { max_interval, .. } = feature {
                *max_interval
            } else {
                None
            }
        })
    }

    /// Returns the server-advertised FUTURERELEASE maximum hold-until datetime.
    pub fn future_release_max_datetime(&self) -> Option<&str> {
        self.features.iter().find_map(|feature| {
            if let Extension::FutureRelease { max_datetime, .. } = feature {
                max_datetime.as_deref()
            } else {
                None
            }
        })
    }

    /// Checks if the server supports DELIVERBY.
    pub fn supports_deliver_by(&self) -> bool {
        self.features
            .iter()
            .any(|feature| matches!(feature, Extension::DeliverBy(_)))
    }

    /// Returns the server-advertised DELIVERBY minimum time, if any.
    pub fn deliver_by_minimum(&self) -> Option<i64> {
        self.features.iter().find_map(|feature| {
            if let Extension::DeliverBy(minimum) = feature {
                *minimum
            } else {
                None
            }
        })
    }

    /// Checks if the server supports MT-PRIORITY.
    pub fn supports_mt_priority(&self) -> bool {
        self.supports_feature(Extension::MtPriority)
    }

    /// Checks if the server supports VRFY.
    pub fn supports_vrfy(&self) -> bool {
        self.supports_feature(Extension::Vrfy)
    }

    /// Checks if the server supports EXPN.
    pub fn supports_expn(&self) -> bool {
        self.supports_feature(Extension::Expn)
    }

    /// Checks if the server supports an ESMTP feature
    pub fn supports_auth_mechanism(&self, mechanism: Mechanism) -> bool {
        self.features
            .contains(&Extension::Authentication(mechanism))
    }

    /// Gets a compatible mechanism from a list
    pub fn get_auth_mechanism(&self, mechanisms: &[Mechanism]) -> Option<Mechanism> {
        for mechanism in mechanisms {
            if self.supports_auth_mechanism(*mechanism) {
                return Some(*mechanism);
            }
        }
        None
    }

    /// The name given in the server banner
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }
}

/// A `MAIL FROM` extension parameter
#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MailParameter {
    /// `BODY` parameter
    Body(MailBodyParameter),
    /// `SIZE` parameter
    Size(usize),
    /// `SMTPUTF8` parameter
    SmtpUtfEight,
    /// `REQUIRETLS` parameter
    RequireTls,
    /// `HOLDFOR` or `HOLDUNTIL` parameter
    FutureRelease(FutureReleaseParameter),
    /// `BY` parameter
    DeliverBy(DeliverByParameter),
    /// `MT-PRIORITY` parameter
    MtPriority(MtPriorityParameter),
    /// `RET` parameter
    DsnReturn(DsnReturn),
    /// `ENVID` parameter
    EnvelopeId(String),
    /// Custom parameter
    Other {
        /// Parameter keyword
        keyword: String,
        /// Parameter value
        value: Option<String>,
    },
    /// Custom parameter whose value is emitted without xtext encoding.
    OtherRaw {
        /// Parameter keyword
        keyword: String,
        /// Parameter value
        value: Option<String>,
    },
}

impl Display for MailParameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            MailParameter::Body(value) => write!(f, "BODY={value}"),
            MailParameter::Size(size) => write!(f, "SIZE={size}"),
            MailParameter::SmtpUtfEight => f.write_str("SMTPUTF8"),
            MailParameter::RequireTls => f.write_str("REQUIRETLS"),
            MailParameter::FutureRelease(value) => write!(f, "{value}"),
            MailParameter::DeliverBy(value) => write!(f, "{value}"),
            MailParameter::MtPriority(value) => write!(f, "MT-PRIORITY={value}"),
            MailParameter::DsnReturn(value) => write!(f, "RET={value}"),
            MailParameter::EnvelopeId(value) => write!(f, "ENVID={}", XText(value)),
            MailParameter::Other {
                keyword,
                value: Some(value),
            } => write!(f, "{}={}", keyword, XText(value)),
            MailParameter::Other {
                keyword,
                value: None,
            } => f.write_str(keyword),
            MailParameter::OtherRaw {
                keyword,
                value: Some(value),
            } => write!(f, "{keyword}={value}"),
            MailParameter::OtherRaw {
                keyword,
                value: None,
            } => f.write_str(keyword),
        }
    }
}

impl MailParameter {
    /// Creates a validated `BY` parameter.
    pub fn deliver_by(
        seconds: i64,
        mode: DeliverByMode,
        trace: bool,
    ) -> Result<MailParameter, Error> {
        Ok(MailParameter::DeliverBy(DeliverByParameter::new(
            seconds, mode, trace,
        )?))
    }

    /// Creates a validated `MT-PRIORITY` parameter.
    pub fn mt_priority(value: i8) -> Result<MailParameter, Error> {
        Ok(MailParameter::MtPriority(MtPriorityParameter::new(value)?))
    }

    pub(crate) fn validate_syntax(&self) -> Result<(), Error> {
        match self {
            MailParameter::EnvelopeId(value) => validate_envelope_id(value),
            MailParameter::Other { keyword, .. } => validate_esmtp_keyword(keyword),
            MailParameter::OtherRaw {
                keyword,
                value: Some(value),
            } => {
                validate_esmtp_keyword(keyword)?;
                validate_esmtp_raw_value(value)
            }
            MailParameter::OtherRaw {
                keyword,
                value: None,
            } => validate_esmtp_keyword(keyword),
            _ => Ok(()),
        }
    }
}

/// DSN `RET` parameter value.
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum DsnReturn {
    /// Return the full message in delivery status notifications.
    Full,
    /// Return only headers in delivery status notifications.
    Headers,
}

impl Display for DsnReturn {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DsnReturn::Full => f.write_str("FULL"),
            DsnReturn::Headers => f.write_str("HDRS"),
        }
    }
}

/// Per-message SMTP send options.
///
/// These options append validated ESMTP parameters to `MAIL FROM` and `RCPT TO`. They are
/// intentionally per-send rather than transport-wide because options such as
/// `DELIVERBY` and `FUTURERELEASE` describe one message, not the SMTP session.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SendOptions {
    mail_parameters: Vec<MailParameter>,
    rcpt_parameters: Vec<RcptParameter>,
    recipient_parameters: Vec<(Address, Vec<RcptParameter>)>,
}

impl SendOptions {
    /// Creates empty send options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a raw `MAIL FROM` parameter.
    ///
    /// Prefer typed helpers such as [`Self::require_tls`] and
    /// [`Self::deliver_by`] when one exists.
    pub fn mail_parameter(mut self, parameter: MailParameter) -> Self {
        self.mail_parameters.push(parameter);
        self
    }

    /// Adds multiple raw `MAIL FROM` parameters.
    pub fn extend_mail_parameters<I>(mut self, parameters: I) -> Self
    where
        I: IntoIterator<Item = MailParameter>,
    {
        self.mail_parameters.extend(parameters);
        self
    }

    /// Adds a raw `RCPT TO` parameter applied to every recipient.
    pub fn rcpt_parameter(mut self, parameter: RcptParameter) -> Self {
        self.rcpt_parameters.push(parameter);
        self
    }

    /// Adds multiple raw `RCPT TO` parameters applied to every recipient.
    pub fn extend_rcpt_parameters<I>(mut self, parameters: I) -> Self
    where
        I: IntoIterator<Item = RcptParameter>,
    {
        self.rcpt_parameters.extend(parameters);
        self
    }

    /// Adds a raw `RCPT TO` parameter for one recipient.
    pub fn recipient_parameter(mut self, recipient: Address, parameter: RcptParameter) -> Self {
        self.recipient_parameters.push((recipient, vec![parameter]));
        self
    }

    /// Adds multiple raw `RCPT TO` parameters for one recipient.
    pub fn extend_recipient_parameters<I>(mut self, recipient: Address, parameters: I) -> Self
    where
        I: IntoIterator<Item = RcptParameter>,
    {
        self.recipient_parameters
            .push((recipient, parameters.into_iter().collect()));
        self
    }

    /// Adds `REQUIRETLS`.
    pub fn require_tls(self) -> Self {
        self.mail_parameter(MailParameter::RequireTls)
    }

    /// Adds `HOLDFOR=<seconds>`.
    pub fn hold_for(self, seconds: u64) -> Self {
        self.mail_parameter(MailParameter::FutureRelease(
            FutureReleaseParameter::HoldFor(seconds),
        ))
    }

    /// Adds `HOLDUNTIL=<datetime>`.
    pub fn hold_until(self, datetime: impl Into<String>) -> Self {
        self.mail_parameter(MailParameter::FutureRelease(
            FutureReleaseParameter::HoldUntil(datetime.into()),
        ))
    }

    /// Adds a validated `BY` parameter.
    pub fn deliver_by(self, seconds: i64, mode: DeliverByMode, trace: bool) -> Result<Self, Error> {
        Ok(self.mail_parameter(MailParameter::deliver_by(seconds, mode, trace)?))
    }

    /// Adds a validated `MT-PRIORITY` parameter.
    pub fn mt_priority(self, value: i8) -> Result<Self, Error> {
        Ok(self.mail_parameter(MailParameter::mt_priority(value)?))
    }

    /// Adds a DSN `RET` parameter.
    pub fn dsn_return(self, value: DsnReturn) -> Self {
        self.mail_parameter(MailParameter::DsnReturn(value))
    }

    /// Adds a DSN `ENVID` parameter.
    pub fn envelope_id(self, value: impl Into<String>) -> Self {
        self.mail_parameter(MailParameter::EnvelopeId(value.into()))
    }

    /// Adds a DSN `NOTIFY` parameter applied to every recipient.
    pub fn notify<I>(self, values: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = DsnNotify>,
    {
        Ok(self.rcpt_parameter(RcptParameter::Notify(DsnNotifyParameter::new(values)?)))
    }

    /// Adds `NOTIFY=NEVER` applied to every recipient.
    pub fn never_notify(self) -> Self {
        self.rcpt_parameter(RcptParameter::Notify(DsnNotifyParameter::never()))
    }

    /// Adds a DSN `ORCPT` parameter applied to every recipient.
    ///
    /// This is mainly useful for single-recipient sends. A later API can carry
    /// recipient-specific parameters without changing the transport trait.
    pub fn original_recipient(
        self,
        address_type: impl Into<String>,
        address: impl Into<String>,
    ) -> Self {
        self.rcpt_parameter(RcptParameter::OriginalRecipient {
            address_type: address_type.into(),
            address: address.into(),
        })
    }

    /// Adds a DSN `NOTIFY` parameter for one recipient.
    pub fn recipient_notify<I>(self, recipient: Address, values: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = DsnNotify>,
    {
        Ok(self.recipient_parameter(
            recipient,
            RcptParameter::Notify(DsnNotifyParameter::new(values)?),
        ))
    }

    /// Adds `NOTIFY=NEVER` for one recipient.
    pub fn recipient_never_notify(self, recipient: Address) -> Self {
        self.recipient_parameter(
            recipient,
            RcptParameter::Notify(DsnNotifyParameter::never()),
        )
    }

    /// Adds a DSN `ORCPT` parameter for one recipient.
    pub fn recipient_original_recipient(
        self,
        recipient: Address,
        address_type: impl Into<String>,
        address: impl Into<String>,
    ) -> Self {
        self.recipient_parameter(
            recipient,
            RcptParameter::OriginalRecipient {
                address_type: address_type.into(),
                address: address.into(),
            },
        )
    }

    /// Returns the configured `MAIL FROM` parameters.
    pub fn mail_parameters(&self) -> &[MailParameter] {
        &self.mail_parameters
    }

    /// Returns the configured `RCPT TO` parameters.
    pub fn rcpt_parameters(&self) -> &[RcptParameter] {
        &self.rcpt_parameters
    }

    /// Returns the configured recipient-specific `RCPT TO` parameters.
    pub fn recipient_parameters(&self) -> &[(Address, Vec<RcptParameter>)] {
        &self.recipient_parameters
    }

    pub(crate) fn rcpt_parameters_for(&self, recipient: &Address) -> Vec<RcptParameter> {
        let mut parameters = Vec::with_capacity(self.rcpt_parameters.len());
        for parameter in &self.rcpt_parameters {
            upsert_rcpt_parameter(&mut parameters, parameter.clone());
        }

        for (configured_recipient, configured_parameters) in &self.recipient_parameters {
            if addresses_match_for_recipient_options(configured_recipient, recipient) {
                for parameter in configured_parameters {
                    upsert_rcpt_parameter(&mut parameters, parameter.clone());
                }
            }
        }
        parameters
    }
}

pub(crate) fn addresses_match_for_recipient_options(
    configured: &Address,
    recipient: &Address,
) -> bool {
    configured.user() == recipient.user()
        && configured.domain().eq_ignore_ascii_case(recipient.domain())
}

fn upsert_rcpt_parameter(parameters: &mut Vec<RcptParameter>, parameter: RcptParameter) {
    if let Some(index) = parameters
        .iter()
        .position(|existing| existing.has_same_keyword(&parameter))
    {
        parameters.remove(index);
    }
    parameters.push(parameter);
}

/// A `FUTURERELEASE` parameter value for `MAIL FROM`.
#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum FutureReleaseParameter {
    /// `HOLDFOR=<seconds>`
    HoldFor(u64),
    /// `HOLDUNTIL=<datetime>`
    HoldUntil(String),
}

impl Display for FutureReleaseParameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FutureReleaseParameter::HoldFor(seconds) => write!(f, "HOLDFOR={seconds}"),
            FutureReleaseParameter::HoldUntil(datetime) => {
                write!(f, "HOLDUNTIL={datetime}")
            }
        }
    }
}

/// A `DELIVERBY` parameter value for `MAIL FROM`.
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct DeliverByParameter {
    /// Seconds until delivery deadline.
    ///
    /// Negative values indicate that the message has already been in transit.
    seconds: i64,
    /// Delivery behavior when the deadline cannot be met.
    mode: DeliverByMode,
    /// Request trace information in the delivery status notification.
    trace: bool,
}

impl DeliverByParameter {
    /// Creates a DELIVERBY parameter after validating RFC 2852 syntax limits.
    pub fn new(seconds: i64, mode: DeliverByMode, trace: bool) -> Result<Self, Error> {
        if seconds.unsigned_abs() > 999_999_999 {
            return Err(error::client("DELIVERBY time must not exceed 9 digits"));
        }

        if mode == DeliverByMode::Return && seconds <= 0 {
            return Err(error::client(
                "DELIVERBY return mode requires a positive time",
            ));
        }

        Ok(Self {
            seconds,
            mode,
            trace,
        })
    }

    /// Seconds until delivery deadline.
    pub fn seconds(&self) -> i64 {
        self.seconds
    }

    /// Delivery behavior when the deadline cannot be met.
    pub fn mode(&self) -> DeliverByMode {
        self.mode
    }

    /// Whether trace information was requested.
    pub fn trace(&self) -> bool {
        self.trace
    }
}

impl Display for DeliverByParameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "BY={};{}", self.seconds, self.mode)?;
        if self.trace {
            f.write_str(";T")?;
        }
        Ok(())
    }
}

/// DELIVERBY deadline handling mode.
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum DeliverByMode {
    /// Notify if delivery fails within the time limit.
    Notify,
    /// Return the message if delivery fails within the time limit.
    Return,
}

impl Display for DeliverByMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DeliverByMode::Notify => f.write_str("N"),
            DeliverByMode::Return => f.write_str("R"),
        }
    }
}

/// An `MT-PRIORITY` parameter value.
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct MtPriorityParameter {
    value: i8,
}

impl MtPriorityParameter {
    /// Creates an MT-PRIORITY value after validating RFC 6710 bounds.
    pub fn new(value: i8) -> Result<Self, Error> {
        if !(-9..=9).contains(&value) {
            return Err(error::client(
                "MT-PRIORITY value must be in the range -9..=9",
            ));
        }

        Ok(Self { value })
    }

    /// Returns the priority value.
    pub fn value(&self) -> i8 {
        self.value
    }
}

impl Display for MtPriorityParameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

/// Values for the `BODY` parameter to `MAIL FROM`
#[derive(PartialEq, Eq, Clone, Debug, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MailBodyParameter {
    /// `7BIT`
    SevenBit,
    /// `8BITMIME`
    EightBitMime,
    /// `BINARYMIME`
    BinaryMime,
}

impl Display for MailBodyParameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            MailBodyParameter::SevenBit => f.write_str("7BIT"),
            MailBodyParameter::EightBitMime => f.write_str("8BITMIME"),
            MailBodyParameter::BinaryMime => f.write_str("BINARYMIME"),
        }
    }
}

/// A `RCPT TO` extension parameter
#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RcptParameter {
    /// `NOTIFY` parameter
    Notify(DsnNotifyParameter),
    /// `ORCPT` parameter
    OriginalRecipient {
        /// Original recipient address type, for example `rfc822`.
        address_type: String,
        /// Original recipient address.
        address: String,
    },
    /// Custom parameter
    Other {
        /// Parameter keyword
        keyword: String,
        /// Parameter value
        value: Option<String>,
    },
}

impl Display for RcptParameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            RcptParameter::Notify(value) => write!(f, "NOTIFY={value}"),
            RcptParameter::OriginalRecipient {
                address_type,
                address,
            } => {
                write!(f, "ORCPT={};{}", address_type, XText(address))
            }
            RcptParameter::Other {
                keyword,
                value: Some(value),
            } => write!(f, "{keyword}={}", XText(value)),
            RcptParameter::Other {
                keyword,
                value: None,
            } => f.write_str(keyword),
        }
    }
}

impl RcptParameter {
    fn keyword(&self) -> &str {
        match self {
            RcptParameter::Notify(_) => "NOTIFY",
            RcptParameter::OriginalRecipient { .. } => "ORCPT",
            RcptParameter::Other { keyword, .. } => keyword,
        }
    }

    fn has_same_keyword(&self, other: &Self) -> bool {
        self.keyword().eq_ignore_ascii_case(other.keyword())
    }

    pub(crate) fn validate_syntax(&self) -> Result<(), Error> {
        match self {
            RcptParameter::OriginalRecipient {
                address_type,
                address,
            } => validate_original_recipient(address_type, address),
            RcptParameter::Other { keyword, .. } => validate_esmtp_keyword(keyword),
            _ => Ok(()),
        }
    }
}

/// DSN `NOTIFY` parameter value.
#[derive(PartialEq, Eq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct DsnNotifyParameter {
    values: Vec<DsnNotify>,
}

impl DsnNotifyParameter {
    /// Creates a validated `NOTIFY` parameter.
    pub fn new<I>(values: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = DsnNotify>,
    {
        let mut deduped = Vec::new();
        for value in values {
            if !deduped.contains(&value) {
                deduped.push(value);
            }
        }
        let values = deduped;
        if values.is_empty() {
            return Err(error::client("NOTIFY must contain at least one value"));
        }
        if values.contains(&DsnNotify::Never) && values.len() > 1 {
            return Err(error::client(
                "NOTIFY=NEVER cannot be combined with other NOTIFY values",
            ));
        }
        Ok(Self { values })
    }

    /// Creates `NOTIFY=NEVER`.
    pub fn never() -> Self {
        Self {
            values: vec![DsnNotify::Never],
        }
    }

    /// Returns the notification values.
    pub fn values(&self) -> &[DsnNotify] {
        &self.values
    }
}

impl Display for DsnNotifyParameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for (idx, value) in self.values.iter().enumerate() {
            if idx > 0 {
                f.write_str(",")?;
            }
            write!(f, "{value}")?;
        }
        Ok(())
    }
}

/// DSN `NOTIFY` condition.
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum DsnNotify {
    /// Notify on successful delivery.
    Success,
    /// Notify on delivery failure.
    Failure,
    /// Notify on delivery delay.
    Delay,
    /// Never send a delivery status notification.
    Never,
}

impl Display for DsnNotify {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DsnNotify::Success => f.write_str("SUCCESS"),
            DsnNotify::Failure => f.write_str("FAILURE"),
            DsnNotify::Delay => f.write_str("DELAY"),
            DsnNotify::Never => f.write_str("NEVER"),
        }
    }
}

fn validate_envelope_id(value: &str) -> Result<(), Error> {
    if value.is_empty() {
        return Err(error::client("ENVID must not be empty"));
    }
    if !value.bytes().all(|byte| (b'!'..=b'~').contains(&byte)) {
        return Err(error::client("ENVID must contain printable ASCII only"));
    }
    if xtext_len(value) > 100 {
        return Err(error::client("ENVID must not exceed 100 xtext bytes"));
    }

    Ok(())
}

fn validate_original_recipient(address_type: &str, address: &str) -> Result<(), Error> {
    validate_atom(address_type, "ORCPT address type")?;
    if address.is_empty() {
        return Err(error::client("ORCPT address must not be empty"));
    }
    if address.chars().any(char::is_control) {
        return Err(error::client(
            "ORCPT address must not contain control characters",
        ));
    }

    Ok(())
}

fn validate_esmtp_keyword(keyword: &str) -> Result<(), Error> {
    if keyword.is_empty()
        || !keyword
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(error::client("ESMTP parameter keyword is invalid"));
    }

    Ok(())
}

fn validate_esmtp_raw_value(value: &str) -> Result<(), Error> {
    if value.is_empty() || !value.bytes().all(|byte| (b'!'..=b'~').contains(&byte)) {
        return Err(error::client(
            "raw ESMTP parameter value must contain printable ASCII without spaces",
        ));
    }

    Ok(())
}

fn validate_atom(value: &str, name: &str) -> Result<(), Error> {
    const SPECIALS: &[u8] = b"()<>@,;:\\\".[]";

    if value.is_empty()
        || !value.is_ascii()
        || value
            .bytes()
            .any(|byte| byte <= b' ' || byte >= 0x7f || SPECIALS.contains(&byte))
    {
        return Err(error::client(format!("{name} must be an RFC 5322 atom")));
    }

    Ok(())
}

fn xtext_len(value: &str) -> usize {
    value
        .as_bytes()
        .iter()
        .map(|byte| match *byte {
            b'!'..=b'~' if *byte != b'+' && *byte != b'=' => 1,
            _ => 3,
        })
        .sum()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::transport::smtp::response::{Category, Code, Detail, Severity};

    #[test]
    fn test_clientid_fmt() {
        assert_eq!(
            format!("{}", ClientId::Domain("test".to_owned())),
            "test".to_owned()
        );
        assert_eq!(format!("{LOCALHOST_CLIENT}"), "[127.0.0.1]".to_owned());
    }

    #[test]
    fn test_extension_fmt() {
        assert_eq!(
            format!("{}", Extension::EightBitMime),
            "8BITMIME".to_owned()
        );
        assert_eq!(
            format!("{}", Extension::Authentication(Mechanism::Plain)),
            "AUTH PLAIN".to_owned()
        );
        assert_eq!(
            format!("{}", Extension::Authentication(Mechanism::OAuthBearer)),
            "AUTH OAUTHBEARER".to_owned()
        );
        assert_eq!(format!("{}", Extension::Dsn), "DSN");
        assert_eq!(format!("{}", Extension::Pipelining), "PIPELINING");
        assert_eq!(format!("{}", Extension::Size(Some(42))), "SIZE 42");
        assert_eq!(
            format!(
                "{}",
                Extension::FutureRelease {
                    max_interval: Some(3600),
                    max_datetime: Some("20260519T120000Z".to_owned()),
                }
            ),
            "FUTURERELEASE 3600 20260519T120000Z"
        );
    }

    #[test]
    fn test_serverinfo_fmt() {
        let mut eightbitmime = HashSet::new();
        assert!(eightbitmime.insert(Extension::EightBitMime));

        assert_eq!(
            format!(
                "{}",
                ServerInfo {
                    name: "name".to_owned(),
                    features: eightbitmime,
                }
            ),
            "name with {EightBitMime}".to_owned()
        );

        let empty = HashSet::new();

        assert_eq!(
            format!(
                "{}",
                ServerInfo {
                    name: "name".to_owned(),
                    features: empty,
                }
            ),
            "name with no supported features".to_owned()
        );

        let mut plain = HashSet::new();
        assert!(plain.insert(Extension::Authentication(Mechanism::Plain)));

        assert_eq!(
            format!(
                "{}",
                ServerInfo {
                    name: "name".to_owned(),
                    features: plain,
                }
            ),
            "name with {Authentication(Plain)}".to_owned()
        );
    }

    #[test]
    fn test_serverinfo() {
        let response = Response::new(
            Code::new(
                Severity::PositiveCompletion,
                Category::Unspecified4,
                Detail::One,
            ),
            vec!["me".to_owned(), "8BITMIME".to_owned(), "SIZE 42".to_owned()],
        );

        let mut features = HashSet::new();
        assert!(features.insert(Extension::EightBitMime));
        assert!(features.insert(Extension::Size(Some(42))));

        let server_info = ServerInfo {
            name: "me".to_owned(),
            features,
        };

        assert_eq!(ServerInfo::from_response(&response).unwrap(), server_info);

        assert!(server_info.supports_feature(Extension::EightBitMime));
        assert!(!server_info.supports_feature(Extension::StartTls));

        let response2 = Response::new(
            Code::new(
                Severity::PositiveCompletion,
                Category::Unspecified4,
                Detail::One,
            ),
            vec![
                "me".to_owned(),
                "AUTH PLAIN CRAM-MD5 XOAUTH2 OAUTHBEARER OTHER".to_owned(),
                "8BITMIME".to_owned(),
                "SIZE 42".to_owned(),
                "PIPELINING".to_owned(),
                "CHUNKING".to_owned(),
                "BINARYMIME".to_owned(),
                "ENHANCEDSTATUSCODES".to_owned(),
                "DSN".to_owned(),
                "REQUIRETLS".to_owned(),
                "FUTURERELEASE 3600 20260519T120000Z".to_owned(),
                "DELIVERBY 240".to_owned(),
                "MT-PRIORITY".to_owned(),
                "VRFY".to_owned(),
                "EXPN".to_owned(),
            ],
        );

        let mut features2 = HashSet::new();
        assert!(features2.insert(Extension::EightBitMime));
        assert!(features2.insert(Extension::Size(Some(42))));
        assert!(features2.insert(Extension::Pipelining));
        assert!(features2.insert(Extension::Chunking));
        assert!(features2.insert(Extension::BinaryMime));
        assert!(features2.insert(Extension::EnhancedStatusCodes));
        assert!(features2.insert(Extension::Dsn));
        assert!(features2.insert(Extension::RequireTls));
        assert!(features2.insert(Extension::FutureRelease {
            max_interval: Some(3600),
            max_datetime: Some("20260519T120000Z".to_owned()),
        }));
        assert!(features2.insert(Extension::DeliverBy(Some(240))));
        assert!(features2.insert(Extension::MtPriority));
        assert!(features2.insert(Extension::Vrfy));
        assert!(features2.insert(Extension::Expn));
        assert!(features2.insert(Extension::Authentication(Mechanism::Plain),));
        assert!(features2.insert(Extension::Authentication(Mechanism::Xoauth2),));
        assert!(features2.insert(Extension::Authentication(Mechanism::OAuthBearer),));

        let server_info2 = ServerInfo {
            name: "me".to_owned(),
            features: features2,
        };

        assert_eq!(ServerInfo::from_response(&response2).unwrap(), server_info2);

        assert!(server_info2.supports_feature(Extension::EightBitMime));
        assert!(server_info2.supports_size());
        assert_eq!(server_info2.size_limit(), Some(42));
        assert!(server_info2.supports_pipelining());
        assert!(server_info2.supports_chunking());
        assert!(server_info2.supports_binary_mime());
        assert!(server_info2.supports_enhanced_status_codes());
        assert!(server_info2.supports_dsn());
        assert!(server_info2.supports_require_tls());
        assert!(server_info2.supports_future_release());
        assert_eq!(server_info2.future_release_max_interval(), Some(3600));
        assert_eq!(
            server_info2.future_release_max_datetime(),
            Some("20260519T120000Z")
        );
        assert!(server_info2.supports_deliver_by());
        assert_eq!(server_info2.deliver_by_minimum(), Some(240));
        assert!(server_info2.supports_mt_priority());
        assert!(server_info2.supports_vrfy());
        assert!(server_info2.supports_expn());
        assert!(server_info2.supports_auth_mechanism(Mechanism::Plain));
        assert!(server_info2.supports_auth_mechanism(Mechanism::OAuthBearer));
        assert!(!server_info2.supports_feature(Extension::StartTls));
    }

    #[test]
    fn test_serverinfo_rejects_malformed_future_release() {
        let response = Response::new(
            Code::new(
                Severity::PositiveCompletion,
                Category::Unspecified4,
                Detail::One,
            ),
            vec!["me".to_owned(), "FUTURERELEASE 20260519T120000Z".to_owned()],
        );

        assert!(ServerInfo::from_response(&response).is_err());
    }

    #[test]
    fn test_serverinfo_ignores_binary_alias() {
        let response = Response::new(
            Code::new(
                Severity::PositiveCompletion,
                Category::Unspecified4,
                Detail::One,
            ),
            vec!["me".to_owned(), "BINARY".to_owned()],
        );

        let server_info = ServerInfo::from_response(&response).unwrap();
        assert!(!server_info.supports_binary_mime());
    }

    #[test]
    fn test_mail_parameter_fmt() {
        assert_eq!(
            format!("{}", MailParameter::Body(MailBodyParameter::BinaryMime)),
            "BODY=BINARYMIME"
        );
        assert_eq!(format!("{}", MailParameter::RequireTls), "REQUIRETLS");
        assert_eq!(
            format!(
                "{}",
                MailParameter::FutureRelease(FutureReleaseParameter::HoldFor(60))
            ),
            "HOLDFOR=60"
        );
        assert_eq!(
            format!(
                "{}",
                MailParameter::FutureRelease(FutureReleaseParameter::HoldUntil(
                    "20260519T120000Z".to_owned()
                ))
            ),
            "HOLDUNTIL=20260519T120000Z"
        );
        assert_eq!(
            format!(
                "{}",
                MailParameter::deliver_by(300, DeliverByMode::Notify, true).unwrap()
            ),
            "BY=300;N;T"
        );
        assert_eq!(
            format!("{}", MailParameter::mt_priority(-3).unwrap()),
            "MT-PRIORITY=-3"
        );
        assert_eq!(
            format!("{}", MailParameter::DsnReturn(DsnReturn::Full)),
            "RET=FULL"
        );
        assert_eq!(
            format!("{}", MailParameter::EnvelopeId("env=1".to_owned())),
            "ENVID=env+3D1"
        );
        assert_eq!(
            format!(
                "{}",
                MailParameter::Other {
                    keyword: "XTEST".to_owned(),
                    value: Some("raw=value".to_owned()),
                }
            ),
            "XTEST=raw+3Dvalue"
        );
        assert_eq!(
            format!(
                "{}",
                MailParameter::OtherRaw {
                    keyword: "XTEST".to_owned(),
                    value: Some("raw=value".to_owned()),
                }
            ),
            "XTEST=raw=value"
        );
        assert_eq!(
            format!(
                "{}",
                RcptParameter::Notify(
                    DsnNotifyParameter::new([DsnNotify::Success, DsnNotify::Failure]).unwrap()
                )
            ),
            "NOTIFY=SUCCESS,FAILURE"
        );
        assert_eq!(
            format!(
                "{}",
                RcptParameter::OriginalRecipient {
                    address_type: "rfc822".to_owned(),
                    address: "user+tag@example.com".to_owned(),
                }
            ),
            "ORCPT=rfc822;user+2Btag@example.com"
        );
        assert!(MailParameter::mt_priority(10).is_err());
        assert!(MailParameter::deliver_by(0, DeliverByMode::Return, false).is_err());
        assert!(DsnNotifyParameter::new([]).is_err());
        assert!(DsnNotifyParameter::new([DsnNotify::Never, DsnNotify::Delay]).is_err());
    }

    #[test]
    fn test_send_options_builds_message_parameters() {
        let first_recipient: Address = "first@example.com".parse().unwrap();
        let options = SendOptions::new()
            .require_tls()
            .hold_for(60)
            .deliver_by(300, DeliverByMode::Return, false)
            .unwrap()
            .mt_priority(4)
            .unwrap()
            .dsn_return(DsnReturn::Headers)
            .envelope_id("env1")
            .notify([DsnNotify::Failure, DsnNotify::Delay])
            .unwrap()
            .recipient_original_recipient(first_recipient.clone(), "rfc822", "alias@example.com");

        assert_eq!(options.mail_parameters().len(), 6);
        assert_eq!(options.rcpt_parameters().len(), 1);
        assert_eq!(options.recipient_parameters().len(), 1);
        assert_eq!(options.rcpt_parameters_for(&first_recipient).len(), 2);
        assert!(matches!(
            options.mail_parameters()[0],
            MailParameter::RequireTls
        ));
    }

    #[test]
    fn test_recipient_parameters_override_global_keyword() {
        let recipient: Address = "first@example.com".parse().unwrap();
        let options = SendOptions::new()
            .notify([DsnNotify::Failure])
            .unwrap()
            .recipient_notify(recipient.clone(), [DsnNotify::Success])
            .unwrap();

        let parameters = options.rcpt_parameters_for(&recipient);
        assert_eq!(parameters.len(), 1);
        assert_eq!(format!("{}", parameters[0]), "NOTIFY=SUCCESS");
    }

    #[test]
    fn test_recipient_parameters_match_domain_case_insensitively() {
        let configured: Address = "first@EXAMPLE.COM".parse().unwrap();
        let envelope_recipient: Address = "first@example.com".parse().unwrap();
        let different_local: Address = "FIRST@example.com".parse().unwrap();
        let options = SendOptions::new()
            .recipient_notify(configured, [DsnNotify::Success])
            .unwrap();

        assert_eq!(options.rcpt_parameters_for(&envelope_recipient).len(), 1);
        assert_eq!(options.rcpt_parameters_for(&different_local).len(), 0);
    }

    #[test]
    fn test_dsn_parameter_validation() {
        assert!(validate_envelope_id("env=1").is_ok());
        assert!(validate_envelope_id("env 1").is_err());
        assert!(validate_envelope_id(&"=".repeat(34)).is_err());

        assert!(validate_original_recipient("rfc822", "alias@example.com").is_ok());
        assert!(validate_original_recipient("rfc822 name", "alias@example.com").is_err());
        assert!(validate_original_recipient("rfc822", "alias\r\n@example.com").is_err());

        assert!(
            MailParameter::OtherRaw {
                keyword: "XTEST".to_owned(),
                value: Some("raw=value".to_owned()),
            }
            .validate_syntax()
            .is_ok()
        );
        assert!(
            MailParameter::OtherRaw {
                keyword: "XTEST".to_owned(),
                value: Some("raw value".to_owned()),
            }
            .validate_syntax()
            .is_err()
        );
    }
}
