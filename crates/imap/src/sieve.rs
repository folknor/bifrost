//! ManageSieve protocol client: transport, framing, SASL and script commands.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use base64::Engine;
use bifrost_types::{FilterDiagnostic, FilterDiagnosticSeverity, FilterValidation};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

use crate::error::{AuthMechanismRejection, AuthMechanismRejectionReason, AuthPolicyFailure};
use crate::types::{AuthMechanism, AuthPolicy, Credentials, CredentialsKind};

/// Protocol failures stay separate from IMAP wire errors. The account adapter
/// maps them at the boundary so filter users retain the same recovery behavior.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("I/O error: {source}")]
    Io {
        #[source]
        source: Arc<std::io::Error>,
    },
    #[error("ManageSieve timed out")]
    Timeout,
    #[error("required STARTTLS is unavailable")]
    StartTlsUnavailable,
    #[error("{0}")]
    Internal(String),
    #[error("{0}")]
    Parse(String),
    #[error("{0}")]
    InvalidInput(String),
    #[error("{0}")]
    AuthPolicy(AuthPolicyFailure),
    #[error("authentication failed: {message}")]
    Auth {
        message: String,
        mechanism: AuthMechanism,
    },
    #[error("OAuth token lookup failed: {0}")]
    OAuthToken(#[source] bifrost_net::Error),
    #[error("SASL failed: {0}")]
    Sasl(#[source] bifrost_sasl::SaslError),
    #[error("ManageSieve rejected command: {message}")]
    Sieve {
        code: Option<SieveResponseCode>,
        message: String,
    },
}

impl From<bifrost_sasl::SaslError> for Error {
    fn from(error: bifrost_sasl::SaslError) -> Self {
        match error {
            bifrost_sasl::SaslError::InvalidCredential(message) => Self::InvalidInput(message),
            error => Self::Sasl(error),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Self::Io {
            source: Arc::new(source),
        }
    }
}

pub use bifrost_net::credentials::TlsMode as ManageSieveTlsMode;

#[non_exhaustive]
#[derive(Clone)]
pub struct ManageSieveConfig {
    host: String,
    port: u16,
    tls_mode: ManageSieveTlsMode,
    connect_timeout: Duration,
    command_timeout: Duration,
    tls_connector: Option<native_tls::TlsConnector>,
}

impl ManageSieveConfig {
    pub fn starttls(host: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port: ManageSieveTlsMode::StartTls
                .port_for(bifrost_net::credentials::TlsService::ManageSieve),
            tls_mode: ManageSieveTlsMode::StartTls,
            connect_timeout: Duration::from_secs(30),
            command_timeout: Duration::from_secs(60),
            tls_connector: None,
        }
    }

    pub fn tls(host: impl Into<String>) -> Self {
        Self {
            tls_mode: ManageSieveTlsMode::Implicit,
            ..Self::starttls(host)
        }
    }

    pub fn plaintext(host: impl Into<String>) -> Self {
        Self {
            tls_mode: ManageSieveTlsMode::None,
            ..Self::starttls(host)
        }
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn with_command_timeout(mut self, timeout: Duration) -> Self {
        self.command_timeout = timeout;
        self
    }

    pub fn with_tls_connector(mut self, connector: native_tls::TlsConnector) -> Self {
        self.tls_connector = Some(connector);
        self
    }
}

impl std::fmt::Debug for ManageSieveConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManageSieveConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("tls_mode", &self.tls_mode)
            .field("connect_timeout", &self.connect_timeout)
            .field("command_timeout", &self.command_timeout)
            .field(
                "tls_connector",
                &self.tls_connector.as_ref().map(|_| "<custom>"),
            )
            .finish()
    }
}

pub(crate) fn validation_error(message: impl Into<String>) -> FilterValidation {
    FilterValidation {
        diagnostics: vec![FilterDiagnostic {
            severity: FilterDiagnosticSeverity::Error,
            message: message.into(),
            line: None,
            column: None,
        }],
    }
}

pub(crate) struct ManageSieveClient {
    stream: SieveStream,
    timeout: Duration,
}

pub(crate) struct ListedScript {
    pub(crate) name: String,
    pub(crate) is_active: bool,
}

/// How a `SETACTIVE` ended.
pub(crate) enum Activation {
    /// The server answered `OK`, possibly with an advisory.
    Done { advisory: Option<String> },
    /// The server answered `NO`, or the request was refused before sending.
    Refused(Error),
    /// Nothing confirmed the outcome: `BYE`, a lost or timed-out connection,
    /// or an answer that could not be read.
    Unconfirmed(Error),
}

impl Activation {
    /// The plain command result, for a caller with no use for the
    /// refused-versus-unconfirmed distinction.
    pub(crate) fn into_result(self) -> Result<Option<String>, Error> {
        match self {
            Self::Done { advisory } => Ok(advisory),
            Self::Refused(error) | Self::Unconfirmed(error) => Err(error),
        }
    }
}

impl ManageSieveClient {
    pub(crate) async fn connect(
        config: &ManageSieveConfig,
        credentials: &Credentials,
        policy: &AuthPolicy,
        budget: bifrost_net::BandwidthBudget,
        sink: Option<bifrost_net::MeterSinkHandle>,
    ) -> Result<Self, Error> {
        validate_tls_server_name(&config.host)?;
        let tcp = tokio::time::timeout(
            config.connect_timeout,
            TcpStream::connect((config.host.as_str(), config.port)),
        )
        .await
        .map_err(|_| Error::Timeout)??;
        let transport = if config.tls_mode.uses_implicit_tls() {
            SieveTransport::Tls(Box::new(tls_connect(config, tcp).await?))
        } else {
            SieveTransport::Plain(tcp)
        };
        let mut stream = SieveStream {
            transport,
            budget,
            sink,
        };
        let mut capabilities = read_response(&mut stream, config.command_timeout)
            .await?
            .ensure_ok()?
            .capabilities()?;
        let mut tls_active = config.tls_mode.uses_implicit_tls();

        if config.tls_mode.uses_starttls() {
            if !capabilities.starttls {
                return Err(Error::StartTlsUnavailable);
            }
            write_command(&mut stream, config.command_timeout, b"STARTTLS\r\n").await?;
            read_response(&mut stream, config.command_timeout)
                .await?
                .ensure_ok()?;
            stream.transport = match stream.transport {
                SieveTransport::Plain(tcp) => {
                    SieveTransport::Tls(Box::new(tls_connect(config, tcp).await?))
                }
                #[cfg(test)]
                SieveTransport::Memory(_) => unreachable!("memory streams cannot negotiate TLS"),
                SieveTransport::Tls(_) => {
                    return Err(Error::Internal("STARTTLS on TLS stream".into()));
                }
            };
            tls_active = true;
            capabilities = request_capability(&mut stream, config.command_timeout).await?;
        }

        authenticate(
            &mut stream,
            config.command_timeout,
            credentials,
            policy,
            &capabilities,
            tls_active,
        )
        .await?;

        Ok(Self {
            stream,
            timeout: config.command_timeout,
        })
    }

    pub(crate) async fn list_scripts(&mut self) -> Result<Vec<ListedScript>, Error> {
        write_command(&mut self.stream, self.timeout, b"LISTSCRIPTS\r\n").await?;
        let response = read_response(&mut self.stream, self.timeout)
            .await?
            .ensure_ok()?;
        list_scripts_from(response.records)
    }

    pub(crate) async fn get_script(&mut self, name: &str) -> Result<String, Error> {
        let command = format!("GETSCRIPT {}\r\n", script_name_arg(name)?);
        write_command(&mut self.stream, self.timeout, command.as_bytes()).await?;
        let response = read_response_within(&mut self.stream, self.timeout, ResponseLimits::SCRIPT)
            .await?
            .ensure_ok()?;
        script_from(response.records)
    }

    /// Store a script. `Ok` carries the server's advisory when it answered
    /// `OK (WARNINGS)` (RFC 5804 2.6): the script is stored, and the user
    /// saving it expects to see what the server warned about.
    pub(crate) async fn put_script(
        &mut self,
        name: &str,
        body: &str,
    ) -> Result<Option<String>, Error> {
        let command = literal_command("PUTSCRIPT", Some(name), body)?;
        write_command(&mut self.stream, self.timeout, &command).await?;
        let response = read_response(&mut self.stream, self.timeout)
            .await?
            .ensure_ok()?;
        Ok(response.status.advisory())
    }

    /// Make `name` the active script, or deactivate with `None`.
    ///
    /// Not a `Result`: a caller that has already stored the script needs to
    /// tell a server refusal (`NO`) from an answer that never confirmed
    /// anything (`BYE`, a lost connection, an unreadable reply), and
    /// [`Error::Sieve`] does not keep that distinction.
    pub(crate) async fn set_active(&mut self, name: Option<&str>) -> Activation {
        let command = match name.map_or_else(|| quote_string(""), script_name_arg) {
            Ok(arg) => format!("SETACTIVE {arg}\r\n"),
            Err(error) => return Activation::Refused(error),
        };
        if let Err(error) = write_command(&mut self.stream, self.timeout, command.as_bytes()).await
        {
            return Activation::Unconfirmed(error);
        }
        match read_response(&mut self.stream, self.timeout).await {
            Ok(response) => match response.status {
                status @ SieveStatus::Ok { .. } => Activation::Done {
                    advisory: status.advisory(),
                },
                SieveStatus::No { code, message } => {
                    Activation::Refused(Error::Sieve { code, message })
                }
                SieveStatus::Bye { code, message } => {
                    Activation::Unconfirmed(Error::Sieve { code, message })
                }
            },
            Err(error) => Activation::Unconfirmed(error),
        }
    }

    pub(crate) async fn delete_script(&mut self, name: &str) -> Result<(), Error> {
        let command = format!("DELETESCRIPT {}\r\n", script_name_arg(name)?);
        write_command(&mut self.stream, self.timeout, command.as_bytes()).await?;
        read_response(&mut self.stream, self.timeout)
            .await?
            .ensure_ok()?;
        Ok(())
    }

    pub(crate) async fn check_script(&mut self, body: &str) -> Result<FilterValidation, Error> {
        let command = literal_command("CHECKSCRIPT", None, body)?;
        write_command(&mut self.stream, self.timeout, &command).await?;
        let response = read_response(&mut self.stream, self.timeout).await?;
        validation_outcome(response.status)
    }
}

/// Decide whether a CHECKSCRIPT reply is a validation verdict or a
/// failure to reach one.
///
/// A `NO` normally IS the verdict - the server compiled the script and
/// rejected it - so it becomes diagnostics rather than an error. A `NO`
/// whose code names an operational condition (see
/// [`SieveResponseCode::is_verdict`]) stays an error, as does every `BYE`:
/// a server closing the connection has not judged the script.
///
/// An `OK (WARNINGS)` is a valid script the server still has something to
/// say about (RFC 5804 2.12). An Outlook user checking a rule expects to see
/// those warnings, not a clean bill, so the text becomes one Warning
/// diagnostic, which leaves the script valid. Server text is human-readable,
/// not a structured diagnostic list, so it is kept whole rather than split
/// per line. Any other `OK` text is informational and dropped.
fn validation_outcome(status: SieveStatus) -> Result<FilterValidation, Error> {
    match status {
        status @ SieveStatus::Ok { .. } => {
            let Some(message) = status.advisory() else {
                return Ok(FilterValidation::default());
            };
            let message = if message.trim().is_empty() {
                "Script compiled with warnings".to_owned()
            } else {
                message
            };
            Ok(FilterValidation {
                diagnostics: vec![FilterDiagnostic {
                    severity: FilterDiagnosticSeverity::Warning,
                    message,
                    line: None,
                    column: None,
                }],
            })
        }
        SieveStatus::Bye { code, message } => Err(Error::Sieve { code, message }),
        SieveStatus::No { code, message } => {
            if code.as_ref().is_some_and(|code| !code.is_verdict()) {
                return Err(Error::Sieve { code, message });
            }
            Ok(validation_error(message))
        }
    }
}

async fn request_capability(
    stream: &mut SieveStream,
    timeout: Duration,
) -> Result<SieveCapabilities, Error> {
    write_command(stream, timeout, b"CAPABILITY\r\n").await?;
    read_response(stream, timeout)
        .await?
        .ensure_ok()?
        .capabilities()
}

// Base64 uses no quoted-string metacharacters. Keep both credential-bearing
// allocations zeroizing without passing them through ordinary string escaping.
fn authentication_command(
    mechanism: AuthMechanism,
    payload: &bifrost_sasl::Secret,
) -> bifrost_sasl::Secret {
    let encoded = bifrost_sasl::Secret::from(
        base64::engine::general_purpose::STANDARD.encode(payload.as_bytes()),
    );
    bifrost_sasl::Secret::from(format!(
        "AUTHENTICATE \"{}\" \"{}\"\r\n",
        mechanism.name(),
        encoded.as_str()
    ))
}

async fn authenticate(
    stream: &mut SieveStream,
    timeout: Duration,
    credentials: &Credentials,
    policy: &AuthPolicy,
    capabilities: &SieveCapabilities,
    tls_active: bool,
) -> Result<(), Error> {
    let offered = capabilities.sasl.clone();
    let (allowed, oauth): (&[_], _) = match credentials {
        CredentialsKind::Password { .. } => (
            &[
                AuthMechanism::ScramSha256Plus,
                AuthMechanism::ScramSha1Plus,
                AuthMechanism::ScramSha256,
                AuthMechanism::ScramSha1,
                AuthMechanism::Plain,
            ],
            false,
        ),
        CredentialsKind::OAuth2 { .. } => {
            (&[AuthMechanism::OAuthBearer, AuthMechanism::Xoauth2], true)
        }
        _ => return Err(Error::Internal("unsupported credential shape".into())),
    };
    let binding = if !oauth
        && allowed
            .iter()
            .any(|m| m.scram().is_some() && supports_mechanism(&offered, m.name()))
    {
        let der = match &stream.transport {
            SieveTransport::Tls(tls) => tls
                .get_ref()
                .peer_certificate()
                .map_err(io_error)?
                .map(|cert| cert.to_der().map_err(io_error))
                .transpose()?,
            _ => None,
        };
        bifrost_sasl::binding_from_peer_certificate(der.as_deref())?
    } else {
        None
    };
    let mut rejected = Vec::new();
    let mut selected = None;
    for (mechanism, result) in bifrost_sasl::negotiate(
        |m| allowed.contains(&m) && supports_mechanism(&offered, m.name()),
        &bifrost_sasl::NegotiationPolicy {
            allowed,
            allow_insecure: policy.allow_cleartext_without_tls,
            oauth,
        },
        tls_active,
        binding.as_ref(),
    ) {
        match result {
            Ok(selection) => {
                selected = Some(selection);
                break;
            }
            Err(reason) => rejected.push(AuthMechanismRejection::new(
                mechanism,
                match reason {
                    bifrost_sasl::NegotiationSkip::TlsRequired => {
                        AuthMechanismRejectionReason::CleartextWithoutTls
                    }
                    _ => AuthMechanismRejectionReason::DisabledByPolicy,
                },
            )),
        }
    }
    let selection =
        selected.ok_or_else(|| Error::AuthPolicy(AuthPolicyFailure::new(offered, rejected)))?;
    let mechanism = selection.mechanism;
    // Resolve tokens only after policy admission, as for IMAP and SMTP.
    let mut client = match credentials {
        CredentialsKind::Password { username, password } => {
            if let Some(rung) = selection.scram {
                let hash = mechanism
                    .scram()
                    .ok_or_else(|| Error::Internal("SCRAM selection without hash".into()))?
                    .0;
                let mut bytes = [0u8; 18];
                getrandom::fill(&mut bytes).map_err(|e| {
                    Error::Sasl(bifrost_sasl::SaslError::LocalRuntime(format!(
                        "failed to generate SCRAM nonce: {e}"
                    )))
                })?;
                let nonce = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
                bifrost_sasl::SaslClient::scram(hash, rung.binding, username, password, nonce)?
            } else {
                bifrost_sasl::SaslClient::plain("", username, password)?
            }
        }
        CredentialsKind::OAuth2 {
            identity,
            token_source,
        } => {
            let token = token_source.current().await.map_err(Error::OAuthToken)?;
            let oauth = if mechanism == AuthMechanism::OAuthBearer {
                bifrost_sasl::OAuthMechanism::OAuthBearer
            } else {
                bifrost_sasl::OAuthMechanism::Xoauth2
            };
            bifrost_sasl::SaslClient::oauth(oauth, identity, token.as_str())?
        }
        _ => return Err(Error::Internal("unsupported credential shape".into())),
    };
    let payload = client
        .initial_response()
        .ok_or_else(|| Error::Internal("SASL initial response missing".into()))?;
    let command = authentication_command(mechanism, &payload);
    write_command(stream, timeout, command.as_bytes()).await?;
    loop {
        let line = read_line(stream, timeout).await?;
        if is_status_line(&line) {
            let record = read_status(stream, timeout, line).await?;
            return match record.status {
                SieveStatus::Ok { code, .. } => {
                    // RFC 5804 permits SASL success data in the terminal OK.
                    if let Some(data) = sasl_success_data(code.as_ref(), &record.code_args)? {
                        match client.step(&data)? {
                            bifrost_sasl::Step::Done => {}
                            _ => {
                                return Err(Error::Parse(
                                    "unexpected SASL reply on success".into(),
                                ));
                            }
                        }
                    }
                    client.finish().map_err(Error::from)
                }
                // The four RFC 5804 codes that say WHY authentication was refused
                // classify better than a bare auth failure: a too-weak mechanism
                // or a missing TLS layer is a policy block the user cannot fix by
                // re-entering a password. Anything else (including no code at all)
                // stays a plain auth failure rather than falling through to the
                // sieve table's server-refused default.
                SieveStatus::No { code, message } => Err(match code {
                    Some(
                        code @ (SieveResponseCode::AuthTooWeak
                        | SieveResponseCode::EncryptNeeded
                        | SieveResponseCode::TransitionNeeded
                        | SieveResponseCode::Sasl),
                    ) => Error::Sieve {
                        code: Some(code),
                        message,
                    },
                    // The mechanism is known here, so the refusal can say which rung
                    // the server turned down rather than only that one was turned
                    // down.
                    _ => Error::Auth { message, mechanism },
                }),
                SieveStatus::Bye { code, message } => Err(Error::Sieve { code, message }),
            };
        }
        let encoded = if let Some(len) = literal_len(&line) {
            if len > 64 * 1024 {
                return Err(Error::Parse("SASL challenge exceeded 64KiB".into()));
            }
            let bytes = read_exact_len(stream, timeout, len).await?;
            if !read_line(stream, timeout).await?.is_empty() {
                return Err(Error::Parse("invalid SASL literal terminator".into()));
            }
            String::from_utf8(bytes).map_err(|_| Error::Parse("non-ASCII SASL challenge".into()))?
        } else {
            let (value, end) = parse_quoted(&line, 0)
                .ok_or_else(|| Error::Parse("invalid SASL challenge string".into()))?;
            if !line[end..].trim().is_empty() {
                return Err(Error::Parse("trailing SASL challenge data".into()));
            }
            value
        };
        let challenge = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|e| Error::Parse(format!("invalid SASL challenge base64: {e}")))?;
        let reply = match client.step(&challenge)? {
            bifrost_sasl::Step::Reply(reply) => reply,
            bifrost_sasl::Step::Done => bifrost_sasl::Secret::default(),
            _ => return Err(Error::Internal("unmapped SASL step".into())),
        };
        let encoded = zeroize::Zeroizing::new(
            base64::engine::general_purpose::STANDARD.encode(reply.as_bytes()),
        );
        let command = bifrost_sasl::Secret::from(format!("\"{}\"\r\n", encoded.as_str()));
        write_command(stream, timeout, command.as_bytes()).await?;
    }
}

/// The SASL success data of a terminal `OK (SASL string)`. The code takes
/// exactly one string argument (quoted or literal); a bare `SASL` or one
/// with other arguments is malformed, not absent.
fn sasl_success_data(
    code: Option<&SieveResponseCode>,
    args: &[Option<String>],
) -> Result<Option<Vec<u8>>, Error> {
    if code != Some(&SieveResponseCode::Sasl) {
        return Ok(None);
    }
    let [Some(encoded)] = args else {
        return Err(Error::Parse("invalid SASL success code".into()));
    };
    base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map(Some)
        .map_err(|e| Error::Parse(format!("invalid SASL success base64: {e}")))
}

fn supports_mechanism(offered: &[String], mechanism: &str) -> bool {
    offered
        .iter()
        .any(|item| item.eq_ignore_ascii_case(mechanism))
}

async fn tls_connect(
    config: &ManageSieveConfig,
    tcp: TcpStream,
) -> Result<tokio_native_tls::TlsStream<TcpStream>, Error> {
    let connector = match &config.tls_connector {
        Some(connector) => connector.clone(),
        None => native_tls::TlsConnector::new().map_err(io_error)?,
    };
    let connector = tokio_native_tls::TlsConnector::from(connector);
    tokio::time::timeout(config.connect_timeout, connector.connect(&config.host, tcp))
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(io_error)
}

fn io_error(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Error {
    Error::Io {
        source: Arc::new(std::io::Error::other(error)),
    }
}

fn validate_tls_server_name(host: &str) -> Result<(), Error> {
    if host.is_empty() {
        return Err(Error::InvalidInput(
            "TLS server name must not be empty".into(),
        ));
    }
    if host.bytes().any(|b| b == 0 || b.is_ascii_whitespace()) {
        return Err(Error::InvalidInput(format!(
            "invalid TLS server name: {host:?}"
        )));
    }
    Ok(())
}

struct SieveStream {
    transport: SieveTransport,
    budget: bifrost_net::BandwidthBudget,
    sink: Option<bifrost_net::MeterSinkHandle>,
}

enum SieveTransport {
    Plain(TcpStream),
    #[cfg(test)]
    Memory(tokio::io::DuplexStream),
    Tls(Box<tokio_native_tls::TlsStream<TcpStream>>),
}

impl AsyncRead for SieveTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(test)]
            Self::Memory(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for SieveTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(test)]
            Self::Memory(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(test)]
            Self::Memory(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(test)]
            Self::Memory(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

async fn write_command(
    stream: &mut SieveStream,
    timeout: Duration,
    bytes: &[u8],
) -> Result<(), Error> {
    let mut stream = bifrost_net::MeteredStream::new(&mut stream.transport, stream.budget.clone())
        .with_meter_sink(stream.sink.clone());
    tokio::time::timeout(timeout, async {
        stream.write_all(bytes).await?;
        stream.flush().await
    })
    .await
    .map_err(|_| Error::Timeout)?
    .map_err(Error::from)
}

/// The most a `GETSCRIPT` body may occupy. A policy bound, not a server
/// figure: far above any script a user writes by hand, and small enough that
/// a server's literal count can never make the client allocate without
/// limit. It is checked before the body is allocated.
const SCRIPT_BODY_LIMIT: usize = 16 * 1024 * 1024;

/// What one response's data records may pull off the wire. Every bound is
/// charged before the bytes it covers are read or allocated, line
/// terminators included; the status record keeps its own
/// [`STATUS_RECORD_LIMIT`].
#[derive(Clone, Copy, Debug)]
struct ResponseLimits {
    /// One data record: its lines plus the literals they pull in.
    record: usize,
    /// Every data record of the response together.
    response: usize,
    /// How many data records the response may hold.
    records: usize,
    /// One literal's octets, apart from the framing around it.
    literal: usize,
}

impl ResponseLimits {
    /// `CAPABILITY`, `LISTSCRIPTS` and every command answered by a status
    /// alone.
    const DEFAULT: Self = Self {
        record: 64 * 1024,
        response: 1024 * 1024,
        records: 10_000,
        literal: 64 * 1024,
    };
    /// `GETSCRIPT`: exactly one record, the script, so the larger body
    /// allowance cannot be spent on a second one. The record allows the
    /// body limit plus room for its framing, so a body exactly at the limit
    /// fits and one octet over it does not.
    const SCRIPT: Self = Self {
        record: SCRIPT_BODY_LIMIT + 1024,
        response: SCRIPT_BODY_LIMIT + 1024,
        records: 1,
        literal: SCRIPT_BODY_LIMIT,
    };
}

/// Debit `cost` from an allowance, refusing once it is spent.
fn charge(left: &mut usize, cost: usize) -> Result<(), Error> {
    *left = left
        .checked_sub(cost)
        .ok_or_else(|| Error::Parse("ManageSieve response exceeded its size limit".into()))?;
    Ok(())
}

async fn read_response(
    stream: &mut SieveStream,
    timeout: Duration,
) -> Result<SieveResponse, Error> {
    read_response_within(stream, timeout, ResponseLimits::DEFAULT).await
}

/// Read one complete response: its data records, then the status record.
///
/// RFC 5804 lets every string in a data response be a literal (`{n}` CRLF
/// then n octets) followed by more of the same record on the next line
/// (`{4}` CRLF `main` ` ACTIVE` CRLF is one LISTSCRIPTS entry). Each record
/// is framed whole before anything classifies it, so literal octets are
/// never read as a line and a continuation line is never mistaken for the
/// next record or for the status. A framing refusal leaves the stream
/// unusable; every client is per-operation and dropped on error.
async fn read_response_within(
    stream: &mut SieveStream,
    timeout: Duration,
    limits: ResponseLimits,
) -> Result<SieveResponse, Error> {
    let mut left = limits.response;
    let mut records = Vec::new();
    loop {
        // The first byte says which allowance governs the line: a data
        // record opens with a string (`"` or `{`), anything else may be the
        // status and gets the status line's own bound. A data line gets
        // only what its record and response have left, nothing once the
        // command's record count is spent, so no bound is charged after
        // the bytes it covers were read.
        let data_cap = if records.len() < limits.records {
            left.min(limits.record).min(LINE_LIMIT)
        } else {
            0
        };
        let (line, raw) = read_line_within(stream, timeout, |first| {
            if matches!(first, b'"' | b'{') {
                data_cap
            } else {
                LINE_LIMIT
            }
        })
        .await?;
        if is_status_line(&line) {
            let status = read_status(stream, timeout, line).await?.status;
            return Ok(SieveResponse { records, status });
        }
        if !matches!(line.as_bytes().first(), Some(b'"' | b'{')) {
            return Err(Error::Parse(
                "ManageSieve data record does not open with a string".into(),
            ));
        }
        if records.len() >= limits.records {
            return Err(Error::Parse(
                "ManageSieve response holds more data records than its command allows".into(),
            ));
        }
        let mut record_left = left.min(limits.record);
        let before = record_left;
        charge(&mut record_left, raw)?;
        let segments =
            read_data_record(stream, timeout, line, &mut record_left, limits.literal).await?;
        left -= before - record_left;
        records.push(data_tokens(segments)?);
    }
}

/// Read the rest of a data record whose first line is `first`, charging
/// `left` for every literal and continuation line before reading it.
async fn read_data_record(
    stream: &mut SieveStream,
    timeout: Duration,
    first: String,
    left: &mut usize,
    literal_limit: usize,
) -> Result<Vec<Segment>, Error> {
    let mut segments = Vec::new();
    let mut line = first;
    loop {
        let Some((prefix, len)) = data_literal(&line)? else {
            segments.push(Segment::Text(line));
            return Ok(segments);
        };
        if len > literal_limit {
            return Err(Error::Parse(
                "ManageSieve literal exceeded its size limit".into(),
            ));
        }
        charge(left, len)?;
        line.truncate(prefix);
        segments.push(Segment::Text(line));
        segments.push(Segment::Literal(
            read_exact_len(stream, timeout, len).await?,
        ));
        let cap = (*left).min(LINE_LIMIT);
        let (next, raw) = read_line_within(stream, timeout, |_| cap).await?;
        charge(left, raw)?;
        line = next;
    }
}

/// The literal marker ending a data line, strictly: a line whose last byte
/// is `}` is a literal or a refusal, never text. RFC 5804 4: a server
/// literal is `{` number `}` CRLF, number a 32-bit count without leading
/// zeros, preceded by SP or opening the record; `{n+}` is client-only.
fn data_literal(line: &str) -> Result<Option<(usize, usize)>, Error> {
    let Some(body) = line.strip_suffix('}') else {
        return Ok(None);
    };
    let malformed = || Error::Parse(format!("malformed ManageSieve literal marker in {line:?}"));
    let open = body.rfind('{').ok_or_else(malformed)?;
    let digits = &body[open + 1..];
    let well_formed = !digits.is_empty()
        && digits.bytes().all(|byte| byte.is_ascii_digit())
        && (digits == "0" || !digits.starts_with('0'))
        && (open == 0 || line.as_bytes()[open - 1] == b' ');
    if !well_formed {
        return Err(malformed());
    }
    let len: u32 = digits.parse().map_err(|_| malformed())?;
    Ok(Some((open, len as usize)))
}

/// A data-response token. Data responses carry no parenthesized lists.
#[derive(Debug, PartialEq, Eq)]
enum DataToken {
    Str(String),
    Atom(String),
}

/// Tokenize a framed data record strictly: tokens are separated by exactly
/// one SP, a quoted string admits only the `\"` and `\\` escapes and no CR,
/// LF or NUL, and its bytes are decoded as UTF-8 once. The tolerances
/// status parsing keeps for human-readable text do not apply here: a data
/// record is structure, and malformed structure is a refusal.
fn data_tokens(segments: Vec<Segment>) -> Result<Vec<DataToken>, Error> {
    let malformed = || Error::Parse("malformed ManageSieve data record".into());
    let count = segments.len();
    let mut tokens = Vec::new();
    // Whether the next token must be preceded by SP.
    let mut need_sep = false;
    for (index, segment) in segments.into_iter().enumerate() {
        let text = match segment {
            Segment::Literal(bytes) => {
                if need_sep {
                    return Err(malformed());
                }
                tokens.push(DataToken::Str(String::from_utf8(bytes).map_err(|_| {
                    Error::Parse("ManageSieve data literal is not UTF-8".into())
                })?));
                need_sep = true;
                continue;
            }
            Segment::Text(text) => text,
        };
        let bytes = text.as_bytes();
        let mut at = 0;
        while at < bytes.len() {
            if need_sep {
                if bytes[at] != b' ' {
                    return Err(malformed());
                }
                at += 1;
                need_sep = false;
                if at == bytes.len() {
                    // A separator ending the text must introduce a literal.
                    if index + 1 == count {
                        return Err(malformed());
                    }
                    break;
                }
            }
            if bytes[at] == b'"' {
                let mut value = Vec::new();
                at += 1;
                loop {
                    match bytes.get(at) {
                        None | Some(0 | b'\r' | b'\n') => return Err(malformed()),
                        Some(b'"') => break,
                        Some(b'\\') => match bytes.get(at + 1) {
                            Some(&escaped @ (b'"' | b'\\')) => {
                                value.push(escaped);
                                at += 1;
                            }
                            _ => return Err(malformed()),
                        },
                        Some(&byte) => value.push(byte),
                    }
                    at += 1;
                }
                at += 1;
                tokens.push(DataToken::Str(String::from_utf8(value).map_err(|_| {
                    Error::Parse("ManageSieve quoted string is not UTF-8".into())
                })?));
            } else {
                let start = at;
                while at < bytes.len() && bytes[at] != b' ' {
                    if matches!(bytes[at], b'"' | b'\\' | b'(' | b')' | b'{' | b'}') {
                        return Err(malformed());
                    }
                    at += 1;
                }
                tokens.push(DataToken::Atom(text[start..at].to_owned()));
            }
            need_sep = true;
        }
    }
    if tokens.is_empty() {
        return Err(malformed());
    }
    Ok(tokens)
}

/// LISTSCRIPTS records (RFC 5804 2.7): a non-empty name, optionally
/// followed by `ACTIVE`; at most one script is active.
fn list_scripts_from(records: Vec<Vec<DataToken>>) -> Result<Vec<ListedScript>, Error> {
    let mut scripts = Vec::with_capacity(records.len());
    for record in records {
        let mut tokens = record.into_iter();
        let (Some(DataToken::Str(name)), marker, None) =
            (tokens.next(), tokens.next(), tokens.next())
        else {
            return Err(Error::Parse("invalid LISTSCRIPTS entry".into()));
        };
        let is_active = match marker {
            None => false,
            Some(DataToken::Atom(atom)) if atom.eq_ignore_ascii_case("ACTIVE") => true,
            Some(_) => return Err(Error::Parse("invalid LISTSCRIPTS entry".into())),
        };
        if name.is_empty() {
            return Err(Error::Parse("LISTSCRIPTS named an empty script".into()));
        }
        scripts.push(ListedScript { name, is_active });
    }
    if scripts.iter().filter(|script| script.is_active).count() > 1 {
        return Err(Error::Parse(
            "LISTSCRIPTS marked more than one script active".into(),
        ));
    }
    Ok(scripts)
}

/// A GETSCRIPT answer (RFC 5804 2.9): exactly one record holding exactly
/// one string, literal or quoted. An empty script is a script.
fn script_from(records: Vec<Vec<DataToken>>) -> Result<String, Error> {
    let mut records = records.into_iter();
    match (records.next(), records.next()) {
        (Some(record), None) => match <[DataToken; 1]>::try_from(record) {
            Ok([DataToken::Str(body)]) => Ok(body),
            _ => Err(Error::Parse(
                "GETSCRIPT answer is not one script string".into(),
            )),
        },
        _ => Err(Error::Parse(
            "GETSCRIPT answer is not one script string".into(),
        )),
    }
}

/// The most a whole status record may occupy: every line plus every
/// literal it pulls in. The same bound `read_line` puts on one line; it is
/// cumulative so an extension code cannot stream unbounded data through
/// many small literals.
const STATUS_RECORD_LIMIT: usize = 64 * 1024;

/// Whether a line opens a status record (`OK`, `NO` or `BYE`).
fn is_status_line(line: &str) -> bool {
    let (token, _) = split_token(line);
    ["OK", "NO", "BYE"]
        .iter()
        .any(|status| token.eq_ignore_ascii_case(status))
}

/// Read one complete status record whose first line is `first`.
///
/// RFC 5804 lets any string in a status line be a literal (`{n}` CRLF then
/// n octets), both the trailing message and arguments inside the response
/// code (`SASL`, `TAG`, extension data). Returning on the first line would
/// leave those octets on the stream to be misread as the next response, so
/// the whole record is read here, for every caller, before it is parsed.
/// A refusal (`Error::Parse`) leaves the stream unusable; every client is
/// per-operation and dropped on error, so nothing reads past it.
async fn read_status(
    stream: &mut SieveStream,
    timeout: Duration,
    first: String,
) -> Result<StatusRecord, Error> {
    // Wire bytes, CRLF included, charged before each read.
    let mut used = first.len().saturating_add(2);
    if used > STATUS_RECORD_LIMIT {
        return Err(Error::Parse(
            "ManageSieve status response exceeded 64KiB".into(),
        ));
    }
    let mut segments = Vec::new();
    let mut line = first;
    loop {
        let Some((prefix, len)) = trailing_literal(&line)? else {
            segments.push(Segment::Text(line));
            break;
        };
        used = used.saturating_add(len);
        if used > STATUS_RECORD_LIMIT {
            return Err(Error::Parse(
                "ManageSieve status response exceeded 64KiB".into(),
            ));
        }
        line.truncate(prefix);
        segments.push(Segment::Text(line));
        segments.push(Segment::Literal(
            read_exact_len(stream, timeout, len).await?,
        ));
        let cap = STATUS_RECORD_LIMIT - used;
        let (next, raw) = read_line_within(stream, timeout, |_| cap).await?;
        line = next;
        used = used.saturating_add(raw);
        if used > STATUS_RECORD_LIMIT {
            return Err(Error::Parse(
                "ManageSieve status response exceeded 64KiB".into(),
            ));
        }
    }
    parse_status(&segments)
}

/// If `line` ends in a server literal marker `{n}` outside a quoted string,
/// the byte offset where the marker starts and `n`. A marker whose digits do
/// not fit is a refusal, never message text.
fn trailing_literal(line: &str) -> Result<Option<(usize, usize)>, Error> {
    let Some(body) = line.strip_suffix('}') else {
        return Ok(None);
    };
    let Some(open) = body.rfind('{') else {
        return Ok(None);
    };
    let digits = &body[open + 1..];
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(None);
    }
    if open > 0 && !matches!(line.as_bytes()[open - 1], b' ' | b'(') {
        return Ok(None);
    }
    // The marker holds no quote, so the quote state at its start is the
    // state at the end of the line.
    let mut quoted = false;
    let mut escaped = false;
    for byte in line[..open].bytes() {
        match byte {
            _ if escaped => escaped = false,
            b'\\' if quoted => escaped = true,
            b'"' => quoted = !quoted,
            _ => {}
        }
    }
    if quoted {
        return Ok(None);
    }
    let len = digits
        .parse()
        .map_err(|_| Error::Parse(format!("ManageSieve literal length {digits} overflows")))?;
    Ok(Some((open, len)))
}

/// A record (status or data) as read off the wire: text runs with the
/// literals that interrupt them, in order.
#[derive(Debug)]
enum Segment {
    Text(String),
    Literal(Vec<u8>),
}

/// A parsed status line with its response-code arguments.
#[derive(Debug)]
struct StatusRecord {
    status: SieveStatus,
    /// The response code's top-level arguments: `Some` for a string,
    /// `None` for an atom or a parenthesized list.
    code_args: Vec<Option<String>>,
}

#[derive(Debug)]
enum StatusToken {
    Str(String),
    Atom(String),
    Open,
    Close,
}

/// Walks the segments of a status record token by token. A literal segment
/// is one string token; text is split per RFC 5804 lexical rules.
struct StatusCursor<'a> {
    segments: &'a [Segment],
    index: usize,
    offset: usize,
}

impl<'a> StatusCursor<'a> {
    /// The unread text, when only the final text segment remains.
    fn rest_of_last_text(&self) -> Option<&'a str> {
        if self.index + 1 != self.segments.len() {
            return None;
        }
        match &self.segments[self.index] {
            Segment::Text(text) => Some(&text[self.offset.min(text.len())..]),
            Segment::Literal(_) => None,
        }
    }

    fn next(&mut self) -> Result<Option<StatusToken>, Error> {
        loop {
            let Some(segment) = self.segments.get(self.index) else {
                return Ok(None);
            };
            let text = match segment {
                Segment::Literal(bytes) => {
                    self.index += 1;
                    self.offset = 0;
                    let text = String::from_utf8(bytes.clone()).map_err(|_| {
                        Error::Parse("ManageSieve status literal is not UTF-8".into())
                    })?;
                    return Ok(Some(StatusToken::Str(text)));
                }
                Segment::Text(text) => text,
            };
            let bytes = text.as_bytes();
            while bytes.get(self.offset).is_some_and(u8::is_ascii_whitespace) {
                self.offset += 1;
            }
            let Some(&byte) = bytes.get(self.offset) else {
                self.index += 1;
                self.offset = 0;
                continue;
            };
            return Ok(Some(match byte {
                b'(' => {
                    self.offset += 1;
                    StatusToken::Open
                }
                b')' => {
                    self.offset += 1;
                    StatusToken::Close
                }
                b'"' => {
                    let (value, end) = parse_quoted(text, self.offset).ok_or_else(|| {
                        Error::Parse("unterminated ManageSieve quoted string".into())
                    })?;
                    self.offset = end;
                    StatusToken::Str(value)
                }
                _ => {
                    let start = self.offset;
                    while bytes.get(self.offset).is_some_and(|b| {
                        !b.is_ascii_whitespace() && !matches!(b, b'(' | b')' | b'"')
                    }) {
                        self.offset += 1;
                    }
                    StatusToken::Atom(text[start..self.offset].to_owned())
                }
            }));
        }
    }
}

/// Parse a complete status record.
///
/// RFC 5804 1.3 and 4:
///   response-oknobye = ("OK"/"NO"/"BYE") [SP "(" resp-code ")"]
///                      [SP string] CRLF
/// The parenthesized code comes BEFORE the human-readable string, so it has
/// to be lifted off first or it lands inside the message as opaque text and
/// its semantics are lost. Code arguments may be strings (quoted or
/// literal) or nested parenthesized extension data, so the code ends at the
/// `)` that balances its `(`, not at the first one.
///
/// A single-line record keeps two tolerances: an unbalanced `(` is message
/// text rather than an invented code, and an unquoted message is taken as
/// trimmed raw text. Once a literal has been read the record is committed,
/// and malformed structure is a refusal.
fn parse_status(segments: &[Segment]) -> Result<StatusRecord, Error> {
    let lenient = segments.len() == 1;
    let mut cursor = StatusCursor {
        segments,
        index: 0,
        offset: 0,
    };
    let Some(StatusToken::Atom(word)) = cursor.next()? else {
        return Err(Error::Parse("ManageSieve status line has no status".into()));
    };
    let raw_rest = cursor
        .rest_of_last_text()
        .map(str::trim_start)
        .unwrap_or_default();

    let mut code = None;
    let mut code_args = Vec::new();
    let mut token = cursor.next()?;
    let mut unbalanced = false;
    if matches!(token, Some(StatusToken::Open)) {
        match parse_code(&mut cursor) {
            Ok((parsed, args)) => {
                code = Some(parsed);
                code_args = args;
                token = cursor.next()?;
            }
            Err(_) if lenient => unbalanced = true,
            Err(error) => return Err(error),
        }
    }
    let message = if unbalanced {
        status_message(raw_rest)
    } else {
        match token {
            None => String::new(),
            Some(StatusToken::Str(text)) if cursor.next()?.is_none() => text,
            _ if lenient => status_message(text_after_code(raw_rest)),
            _ => {
                return Err(Error::Parse("malformed ManageSieve status message".into()));
            }
        }
    };
    let status = if word.eq_ignore_ascii_case("OK") {
        SieveStatus::Ok { code, message }
    } else if word.eq_ignore_ascii_case("NO") {
        SieveStatus::No { code, message }
    } else if word.eq_ignore_ascii_case("BYE") {
        SieveStatus::Bye { code, message }
    } else {
        return Err(Error::Parse(format!("not a ManageSieve status: {word}")));
    };
    Ok(StatusRecord { status, code_args })
}

/// What follows a balanced leading code in a single-line remainder, for the
/// raw-text message tolerance.
fn text_after_code(rest: &str) -> &str {
    let Some(after) = rest.strip_prefix('(') else {
        return rest;
    };
    let mut depth = 1_usize;
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in after.bytes().enumerate() {
        match byte {
            _ if escaped => escaped = false,
            b'\\' if quoted => escaped = true,
            b'"' => quoted = !quoted,
            b'(' if !quoted => depth += 1,
            b')' if !quoted => {
                depth -= 1;
                if depth == 0 {
                    return after[index + 1..].trim_start();
                }
            }
            _ => {}
        }
    }
    rest
}

/// Parse a response code after its opening `(`, through its balancing `)`.
/// Returns the code (built from its on-wire spelling, arguments re-quoted)
/// and its top-level arguments.
fn parse_code(
    cursor: &mut StatusCursor<'_>,
) -> Result<(SieveResponseCode, Vec<Option<String>>), Error> {
    let unbalanced = || Error::Parse("unbalanced ManageSieve response code".into());
    let Some(StatusToken::Atom(atom)) = cursor.next()? else {
        return Err(unbalanced());
    };
    let mut raw = atom;
    let mut args = Vec::new();
    let mut depth = 0_usize;
    loop {
        let token = cursor.next()?.ok_or_else(unbalanced)?;
        let piece = match &token {
            StatusToken::Str(text) => {
                format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
            }
            StatusToken::Atom(atom) => atom.clone(),
            StatusToken::Open => "(".to_owned(),
            StatusToken::Close if depth == 0 => break,
            StatusToken::Close => ")".to_owned(),
        };
        if depth == 0 {
            args.push(match token {
                StatusToken::Str(text) => Some(text),
                _ => None,
            });
        }
        match piece.as_str() {
            "(" => depth += 1,
            ")" => depth -= 1,
            _ => {}
        }
        raw.push(' ');
        raw.push_str(&piece);
    }
    Ok((SieveResponseCode::parse(&raw), args))
}

// This adapter sits below admission, so its timer exists only while peer I/O
// is pending. A cap wait cannot consume the peer's response timeout.
struct PeerReadTimeout<S> {
    inner: S,
    remaining: Duration,
    pending_since: Option<tokio::time::Instant>,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<S: AsyncRead + Unpin> AsyncRead for PeerReadTimeout<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        use std::future::Future;
        if let Some(started) = self.pending_since.take() {
            self.remaining = self.remaining.saturating_sub(started.elapsed());
        }
        if self.remaining.is_zero() {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "ManageSieve peer read timed out",
            )));
        }
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(result) => {
                self.timer = None;
                Poll::Ready(result)
            }
            Poll::Pending => {
                let timeout = self.remaining;
                self.pending_since = Some(tokio::time::Instant::now());
                let timer = self
                    .timer
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
                if timer.as_mut().poll(cx).is_ready() {
                    Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "ManageSieve peer read timed out",
                    )))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

fn metered_reader(
    stream: &mut SieveStream,
    timeout: Duration,
) -> bifrost_net::MeteredStream<PeerReadTimeout<&mut SieveTransport>> {
    bifrost_net::MeteredStream::new(
        PeerReadTimeout {
            inner: &mut stream.transport,
            remaining: timeout,
            pending_since: None,
            timer: None,
        },
        stream.budget.clone(),
    )
    .with_meter_sink(stream.sink.clone())
}

fn peer_read_error(error: std::io::Error) -> Error {
    if error.kind() == std::io::ErrorKind::TimedOut {
        Error::Timeout
    } else {
        Error::from(error)
    }
}

async fn read_line(stream: &mut SieveStream, timeout: Duration) -> Result<String, Error> {
    Ok(read_line_within(stream, timeout, |_| LINE_LIMIT).await?.0)
}

/// The most one line may occupy: 64 KiB of content plus its LF.
const LINE_LIMIT: usize = 64 * 1024 + 1;

/// Read one line, refusing as soon as it would pass the bound `cap` gives
/// for its first byte, terminator included. Returns the line without its
/// terminator and the bytes it occupied on the wire.
async fn read_line_within(
    stream: &mut SieveStream,
    timeout: Duration,
    cap: impl Fn(u8) -> usize,
) -> Result<(String, usize), Error> {
    let mut stream = metered_reader(stream, timeout);
    let mut buf = Vec::new();
    let mut byte = [0_u8; 1];
    let mut max = LINE_LIMIT;
    loop {
        if buf.len() == 1 {
            max = cap(buf[0]);
        }
        if buf.len() >= max {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "ManageSieve response line exceeded its size limit",
            )
            .into());
        }
        stream
            .read_exact(&mut byte)
            .await
            .map_err(peer_read_error)?;
        buf.push(byte[0]);
        if byte[0] == b'\n' {
            break;
        }
    }
    let raw = buf.len();
    buf.pop();
    if buf.ends_with(b"\r") {
        buf.pop();
    }
    let line = String::from_utf8(buf)
        .map_err(|error| Error::Parse(format!("ManageSieve response is not UTF-8: {error}")))?;
    Ok((line, raw))
}

async fn read_exact_len(
    stream: &mut SieveStream,
    timeout: Duration,
    len: usize,
) -> Result<Vec<u8>, Error> {
    let mut stream = metered_reader(stream, timeout);
    let mut buf = vec![0_u8; len];
    stream.read_exact(&mut buf).await.map_err(peer_read_error)?;
    Ok(buf)
}

struct SieveResponse {
    /// The data records before the status, each tokenized whole.
    records: Vec<Vec<DataToken>>,
    status: SieveStatus,
}

impl SieveResponse {
    fn ensure_ok(self) -> Result<Self, Error> {
        match self.status {
            SieveStatus::Ok { .. } => Ok(self),
            // Both carry the ManageSieve code so the account boundary can
            // classify it; a `BYE` additionally means the server is
            // closing the connection, but the rejection reason is what
            // decides recovery.
            SieveStatus::No { code, message } | SieveStatus::Bye { code, message } => {
                Err(Error::Sieve { code, message })
            }
        }
    }

    /// Capability records (RFC 5804 1.7): a name string, optionally followed
    /// by a value string, either of which may be a literal.
    fn capabilities(&self) -> Result<SieveCapabilities, Error> {
        let mut capabilities = SieveCapabilities::default();
        for record in &self.records {
            let (key, value) = match record.as_slice() {
                [DataToken::Str(key)] => (key, None),
                [DataToken::Str(key), DataToken::Str(value)] => (key, Some(value)),
                _ => return Err(Error::Parse("invalid ManageSieve capability".into())),
            };
            if key.eq_ignore_ascii_case("STARTTLS") {
                capabilities.starttls = true;
            } else if key.eq_ignore_ascii_case("SASL")
                && let Some(mechanisms) = value
            {
                capabilities.sasl = mechanisms.split_whitespace().map(str::to_string).collect();
            }
        }
        Ok(capabilities)
    }
}

#[derive(Debug)]
enum SieveStatus {
    /// Success. The code and text are kept: `OK (WARNINGS)` on CHECKSCRIPT,
    /// PUTSCRIPT and SETACTIVE carries the warnings the user must see.
    Ok {
        code: Option<SieveResponseCode>,
        message: String,
    },
    No {
        code: Option<SieveResponseCode>,
        message: String,
    },
    Bye {
        code: Option<SieveResponseCode>,
        message: String,
    },
}

impl SieveStatus {
    /// The server's warning text on an `OK (WARNINGS)`, hierarchical
    /// `WARNINGS/...` codes included; empty when it sent none. `None` for
    /// any other answer: other `OK` text is informational.
    fn advisory(&self) -> Option<String> {
        match self {
            Self::Ok {
                code: Some(code),
                message,
            } if code.meaning() == SieveResponseCode::Warnings => Some(message.clone()),
            _ => None,
        }
    }
}

/// A ManageSieve response code (RFC 5804 1.3).
///
/// Its own vocabulary, deliberately not folded into the IMAP
/// `ResponseCode`: these arrive on a different protocol and recording an
/// IMAP code the server never sent would corrupt the wire evidence a
/// support export exists to preserve.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SieveResponseCode {
    AuthTooWeak,
    EncryptNeeded,
    /// Bare `QUOTA`: a site-defined quota, with no sub-kind saying which.
    Quota,
    /// `QUOTA/MAXSCRIPTS`: the user already stores the maximum number of
    /// scripts. Shared account capacity, recovered by deleting a script.
    QuotaMaxScripts,
    /// `QUOTA/MAXSIZE`: this script exceeds the per-script size limit. A
    /// fixed limit on the submitted item; the unchanged request never
    /// succeeds, whatever else is deleted.
    QuotaMaxSize,
    Referral,
    Sasl,
    TransitionNeeded,
    /// The one that matters most: an explicitly TRANSIENT failure. Left
    /// unparsed it derived terminal `ProviderRefused`, so the engine
    /// never retried a server that asked to be retried.
    TryLater,
    /// The named script is the active one (e.g. DELETESCRIPT on it).
    Active,
    NonExistent,
    AlreadyExists,
    /// Script compiled with warnings. Advisory - it rides on `OK` too.
    Warnings,
    Tag,
    /// An extension code this client does not model. Preserved verbatim
    /// for diagnostics; classification falls back to the status.
    Other(String),
}

impl SieveResponseCode {
    fn parse(raw: &str) -> Self {
        // `SASL "..."` / `REFERRAL "..."` carry a payload after the code;
        // key off the code alone. A hierarchical code (`QUOTA/MAXSIZE`) is
        // kept whole: its sub-kind decides the classification.
        let atom = raw.split(char::is_whitespace).next().unwrap_or(raw);
        match atom.to_ascii_uppercase().as_str() {
            "AUTH-TOO-WEAK" => Self::AuthTooWeak,
            "ENCRYPT-NEEDED" => Self::EncryptNeeded,
            "QUOTA" => Self::Quota,
            "QUOTA/MAXSCRIPTS" => Self::QuotaMaxScripts,
            "QUOTA/MAXSIZE" => Self::QuotaMaxSize,
            "REFERRAL" => Self::Referral,
            "SASL" => Self::Sasl,
            "TRANSITION-NEEDED" => Self::TransitionNeeded,
            "TRYLATER" => Self::TryLater,
            "ACTIVE" => Self::Active,
            "NONEXISTENT" => Self::NonExistent,
            "ALREADYEXISTS" => Self::AlreadyExists,
            "WARNINGS" => Self::Warnings,
            "TAG" => Self::Tag,
            _ => Self::Other(raw.trim().to_owned()),
        }
    }

    /// The on-wire spelling, for `native_code` telemetry.
    pub(crate) fn code(&self) -> &str {
        match self {
            Self::AuthTooWeak => "AUTH-TOO-WEAK",
            Self::EncryptNeeded => "ENCRYPT-NEEDED",
            Self::Quota => "QUOTA",
            Self::QuotaMaxScripts => "QUOTA/MAXSCRIPTS",
            Self::QuotaMaxSize => "QUOTA/MAXSIZE",
            Self::Referral => "REFERRAL",
            Self::Sasl => "SASL",
            Self::TransitionNeeded => "TRANSITION-NEEDED",
            Self::TryLater => "TRYLATER",
            Self::Active => "ACTIVE",
            Self::NonExistent => "NONEXISTENT",
            Self::AlreadyExists => "ALREADYEXISTS",
            Self::Warnings => "WARNINGS",
            Self::Tag => "TAG",
            Self::Other(raw) => raw,
        }
    }

    /// The code as this client understands it. RFC 5804 1.3: a client
    /// MUST tolerate hierarchical detail it does not recognize, treating
    /// `QUOTA/MAXFROB` as `QUOTA`. So an unrecognized code resolves to its
    /// longest recognized slash-delimited ancestor (`QUOTA/MAXSIZE/X` is
    /// `QUOTA/MAXSIZE`, `TRYLATER/X` is `TRYLATER`). Only the code atom is
    /// read, never its payload, and only whole segments, so `QUOTAX/MAXSIZE`
    /// stays unknown. The original stays the wire evidence: callers record
    /// `code()` of the received value, not of its meaning.
    pub(crate) fn meaning(&self) -> Self {
        let Self::Other(raw) = self else {
            return self.clone();
        };
        let mut atom = raw.split(char::is_whitespace).next().unwrap_or(raw);
        while let Some((parent, _)) = atom.rsplit_once('/') {
            let resolved = Self::parse(parent);
            if !matches!(resolved, Self::Other(_)) {
                return resolved;
            }
            atom = parent;
        }
        self.clone()
    }

    /// Whether a CHECKSCRIPT `NO` carrying this code is read as the
    /// server's verdict on the script.
    ///
    /// RFC 5804 2.12: the server checks syntax and MUST NOT check quota,
    /// so a `NO` normally carries a compile error. A code naming an
    /// operational condition (busy server, quota, authentication, a missing
    /// or active script) says the server refused for a reason other than
    /// the script's content; reporting it as "your script is invalid"
    /// tells the user something false and discards the classification
    /// `classify_sieve` gives that code. `WARNINGS` and `TAG` are advisory
    /// and an unknown extension code says nothing, so those keep the plain
    /// reading of a `NO`. That is a policy for codes this client cannot
    /// interpret, not proof the server compiled the script. Exhaustive so
    /// a new variant has to be placed.
    fn is_verdict(&self) -> bool {
        match self.meaning() {
            Self::Warnings | Self::Tag | Self::Other(_) => true,
            Self::AuthTooWeak
            | Self::EncryptNeeded
            | Self::Quota
            | Self::QuotaMaxScripts
            | Self::QuotaMaxSize
            | Self::Referral
            | Self::Sasl
            | Self::TransitionNeeded
            | Self::TryLater
            | Self::Active
            | Self::NonExistent
            | Self::AlreadyExists => false,
        }
    }
}

#[derive(Default)]
struct SieveCapabilities {
    starttls: bool,
    sasl: Vec<String>,
}

fn split_token(line: &str) -> (&str, &str) {
    match line.split_once(char::is_whitespace) {
        Some((token, rest)) => (token, rest.trim_start()),
        None => (line, ""),
    }
}

fn status_message(rest: &str) -> String {
    parse_quoted(rest, 0)
        .map(|(message, _)| message)
        .unwrap_or_else(|| rest.trim().to_string())
}

fn literal_len(line: &str) -> Option<usize> {
    let trimmed = line.trim();
    let inner = trimmed.strip_prefix('{')?.strip_suffix('}')?;
    inner.parse().ok()
}

/// A quoted string at `start` (after optional whitespace), leniently: any
/// escaped byte stands for itself. The bytes are collected and decoded as
/// UTF-8 once, so a non-ASCII string survives; invalid UTF-8 is replaced
/// rather than refused, since only human-readable status text and SASL
/// base64 come through here. Data records use the strict lexer in
/// `data_tokens`.
fn parse_quoted(input: &str, start: usize) -> Option<(String, usize)> {
    let bytes = input.as_bytes();
    let mut index = start;
    while index < bytes.len() && bytes[index].is_ascii_whitespace() {
        index += 1;
    }
    if bytes.get(index) != Some(&b'"') {
        return None;
    }
    index += 1;
    let mut out = Vec::new();
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => {
                index += 1;
                out.push(*bytes.get(index)?);
                index += 1;
            }
            b'"' => return Some((String::from_utf8_lossy(&out).into_owned(), index + 1)),
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    None
}

fn quote_string(value: &str) -> Result<String, Error> {
    if value.bytes().any(|byte| matches!(byte, 0 | b'\r' | b'\n')) {
        return Err(Error::InvalidInput(
            "ManageSieve strings cannot contain NUL or CRLF".into(),
        ));
    }
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    Ok(format!("\"{escaped}\""))
}

fn script_name_arg(name: &str) -> Result<String, Error> {
    if name.is_empty() {
        return Err(Error::InvalidInput(
            "ManageSieve script names cannot be empty".into(),
        ));
    }
    quote_string(name)
}

fn literal_command(command: &str, name: Option<&str>, body: &str) -> Result<Vec<u8>, Error> {
    if command
        .bytes()
        .any(|byte| !byte.is_ascii_uppercase() && byte != b'_')
    {
        return Err(Error::InvalidInput(
            "ManageSieve command token must be uppercase ASCII".into(),
        ));
    }
    let mut bytes = match name {
        Some(name) => format!(
            "{command} {} {{{}+}}\r\n",
            script_name_arg(name)?,
            body.len()
        )
        .into_bytes(),
        None => format!("{command} {{{}+}}\r\n", body.len()).into_bytes(),
    };
    bytes.extend_from_slice(body.as_bytes());
    bytes.extend_from_slice(b"\r\n");
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn authentication_payloads_use_shared_framing_and_refuse_nul_locally() {
        for (credentials, mechanism, expected) in [
            (
                Credentials::password("user", "pw"),
                "PLAIN",
                Some(b"\0user\0pw".as_slice()),
            ),
            (
                Credentials::oauth2("user", "token"),
                "XOAUTH2",
                Some(b"user=user\x01auth=Bearer token\x01\x01".as_slice()),
            ),
            (Credentials::oauth2("u\x01ser", "token"), "XOAUTH2", None),
            (Credentials::oauth2("user", "tok\x01en"), "XOAUTH2", None),
            (Credentials::password("user\0other", "pw"), "PLAIN", None),
            (Credentials::password("user", "pw\0other"), "PLAIN", None),
        ] {
            let (client, mut peer) = tokio::io::duplex(4096);
            let mut stream = SieveStream {
                transport: SieveTransport::Memory(client),
                budget: bifrost_net::BandwidthBudget::default(),
                sink: None,
            };
            peer.write_all(b"OK\r\n").await.unwrap();
            let result = authenticate(
                &mut stream,
                Duration::from_secs(5),
                &credentials,
                &AuthPolicy::default(),
                &SieveCapabilities {
                    sasl: vec![mechanism.to_owned()],
                    ..SieveCapabilities::default()
                },
                true,
            )
            .await;
            drop(stream);
            let mut written = Vec::new();
            peer.read_to_end(&mut written).await.unwrap();
            if let Some(expected) = expected {
                result.unwrap();
                let encoded = base64::engine::general_purpose::STANDARD.encode(expected);
                assert_eq!(
                    written,
                    format!("AUTHENTICATE \"{mechanism}\" \"{encoded}\"\r\n").as_bytes()
                );
            } else {
                assert!(matches!(result, Err(Error::InvalidInput(_))));
                assert!(
                    written.is_empty(),
                    "local refusal must write no credentials"
                );
            }
        }
    }

    async fn peer_line(peer: &mut tokio::io::DuplexStream) -> String {
        let mut line = Vec::new();
        loop {
            let byte = peer.read_u8().await.unwrap();
            line.push(byte);
            if byte == b'\n' {
                break;
            }
        }
        String::from_utf8(line).unwrap()
    }

    #[tokio::test]
    async fn scram_authentication_frames_challenges_and_terminal_success_data() {
        for hash in [
            bifrost_sasl::ScramHash::Sha1,
            bifrost_sasl::ScramHash::Sha256,
        ] {
            for success_data in [false, true] {
                let mechanism = hash.mechanism_name(bifrost_sasl::ChannelBinding::None);
                let (client, mut peer) = tokio::io::duplex(4096);
                let mut stream = SieveStream {
                    transport: SieveTransport::Memory(client),
                    budget: bifrost_net::BandwidthBudget::default(),
                    sink: None,
                };
                let credentials = Credentials::password("user", "pencil");
                let capabilities = SieveCapabilities {
                    sasl: vec![mechanism.to_owned(), "PLAIN".into()],
                    ..SieveCapabilities::default()
                };
                let policy = AuthPolicy::default();
                let auth = authenticate(
                    &mut stream,
                    Duration::from_secs(5),
                    &credentials,
                    &policy,
                    &capabilities,
                    true,
                );
                let server = async {
                    let first = peer_line(&mut peer).await;
                    assert!(first.starts_with(&format!("AUTHENTICATE \"{mechanism}\" \"")));
                    let (_, offset) =
                        parse_quoted(first.trim_start_matches("AUTHENTICATE "), 0).unwrap();
                    let (encoded, _) =
                        parse_quoted(first.trim_start_matches("AUTHENTICATE "), offset).unwrap();
                    let first = String::from_utf8(
                        base64::engine::general_purpose::STANDARD
                            .decode(encoded)
                            .unwrap(),
                    )
                    .unwrap();
                    let bare = first.strip_prefix("n,,").unwrap();
                    let nonce = bare.strip_prefix("n=user,r=").unwrap();
                    let server_first = format!("r={nonce}server,s=QSXCR+Q6sek8bf92,i=4096");
                    let (expected, signature) = bifrost_sasl::scram_client_final(
                        hash,
                        &bifrost_sasl::prepare_scram_password("pencil").unwrap(),
                        nonce,
                        bare,
                        &server_first,
                        &bifrost_sasl::ScramChannelBinding::None,
                    )
                    .unwrap();
                    let encoded = base64::engine::general_purpose::STANDARD.encode(server_first);
                    // Both RFC 5804 string forms carry the same SASL bytes.
                    let challenge = if success_data {
                        format!("{{{}}}\r\n{encoded}\r\n", encoded.len())
                    } else {
                        format!("\"{encoded}\"\r\n")
                    };
                    peer.write_all(challenge.as_bytes()).await.unwrap();
                    assert_eq!(
                        peer_line(&mut peer).await,
                        format!("\"{}\"\r\n", expected.as_str())
                    );
                    let verifier = format!(
                        "v={}",
                        base64::engine::general_purpose::STANDARD.encode(signature)
                    );
                    let encoded = base64::engine::general_purpose::STANDARD.encode(verifier);
                    if success_data {
                        peer.write_all(format!("OK (SASL \"{encoded}\")\r\n").as_bytes())
                            .await
                            .unwrap();
                    } else {
                        peer.write_all(format!("\"{encoded}\"\r\n").as_bytes())
                            .await
                            .unwrap();
                        assert_eq!(peer_line(&mut peer).await, "\"\"\r\n");
                        peer.write_all(b"OK\r\n").await.unwrap();
                    }
                };
                let (result, ()) = tokio::join!(auth, server);
                result.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn oauthbearer_error_continuation_is_acknowledged_before_refusal() {
        let (client, mut peer) = tokio::io::duplex(4096);
        let mut stream = SieveStream {
            transport: SieveTransport::Memory(client),
            budget: bifrost_net::BandwidthBudget::default(),
            sink: None,
        };
        let credentials = Credentials::oauth2("user", "token");
        let policy = AuthPolicy::default();
        let capabilities = SieveCapabilities {
            sasl: vec!["XOAUTH2".into(), "OAUTHBEARER".into()],
            ..SieveCapabilities::default()
        };
        let auth = authenticate(
            &mut stream,
            Duration::from_secs(5),
            &credentials,
            &policy,
            &capabilities,
            true,
        );
        let server = async {
            assert!(
                peer_line(&mut peer)
                    .await
                    .starts_with("AUTHENTICATE \"OAUTHBEARER\"")
            );
            peer.write_all(b"\"e30=\"\r\n").await.unwrap();
            assert_eq!(peer_line(&mut peer).await, "\"AQ==\"\r\n");
            peer.write_all(b"NO \"refused\"\r\n").await.unwrap();
        };
        let (result, ()) = tokio::join!(auth, server);
        assert!(matches!(
            result,
            Err(Error::Auth {
                mechanism: AuthMechanism::OAuthBearer,
                ..
            })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn bandwidth_sieve_charges_the_account_and_excludes_admission_from_peer_timeout() {
        use bifrost_net::{BandwidthBudget, BandwidthDirection};
        use tokio::time::Instant;
        let meter = bifrost_net::BandwidthMeter::new();
        let id = bifrost_net::AccountId("sieve".to_owned());
        meter.register_account(id.clone());
        let observed = meter.account(id);
        let budget = BandwidthBudget::new(Some(4));
        budget.admit(BandwidthDirection::Inbound, 4).await.finish(4);
        let (client, mut peer) = tokio::io::duplex(64);
        let mut stream = SieveStream {
            transport: SieveTransport::Memory(client),
            budget: budget.clone(),
            sink: Some(observed.sink_handle()),
        };
        peer.write_all(b"OK\r\n").await.unwrap();
        let start = Instant::now();
        assert_eq!(
            read_line(&mut stream, Duration::from_millis(10))
                .await
                .unwrap(),
            "OK"
        );
        assert_eq!(start.elapsed(), Duration::from_secs(1));
        let next = budget.reserve(BandwidthDirection::Inbound, 4);
        assert_eq!(
            next.ready_at().duration_since(Instant::now()),
            Duration::from_secs(1)
        );
        write_command(&mut stream, Duration::from_secs(5), b"NOOP\r\n")
            .await
            .unwrap();
        assert_eq!(start.elapsed(), Duration::from_millis(1500));
        assert_eq!(observed.bytes_in(), 4);
        assert_eq!(observed.bytes_out(), 6);
        let next = budget.reserve(BandwidthDirection::Outbound, 4);
        assert_eq!(
            next.ready_at().duration_since(Instant::now()),
            Duration::from_secs(1)
        );
    }

    async fn sieve_trickle_case(literal: bool) {
        use tokio::time::Instant;
        let (client, mut peer) = tokio::io::duplex(64);
        let mut stream = SieveStream {
            transport: SieveTransport::Memory(client),
            budget: bifrost_net::BandwidthBudget::default(),
            sink: None,
        };
        let writer = tokio::spawn(async move {
            for byte in b"OK\r\n" {
                tokio::time::sleep(Duration::from_millis(40)).await;
                if peer.write_all(&[*byte]).await.is_err() {
                    return;
                }
            }
        });
        let start = Instant::now();
        let result = if literal {
            read_exact_len(&mut stream, Duration::from_millis(100), 4)
                .await
                .map(|_| ())
        } else {
            read_line(&mut stream, Duration::from_millis(100))
                .await
                .map(|_| ())
        };
        assert!(matches!(result, Err(Error::Timeout)));
        assert_eq!(start.elapsed(), Duration::from_millis(100));
        writer.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn bandwidth_sieve_trickled_line_spends_one_peer_budget() {
        sieve_trickle_case(false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn bandwidth_sieve_trickled_literal_spends_one_peer_budget() {
        sieve_trickle_case(true).await;
    }

    use super::*;

    fn no_status(line: &str) -> (Option<SieveResponseCode>, String) {
        match parse_status(&[Segment::Text(line.to_owned())])
            .expect("status parses")
            .status
        {
            SieveStatus::No { code, message } => (code, message),
            other => panic!("expected NO, got {other:?}"),
        }
    }

    /// Feed `wire` to `read_response` and return its status plus whatever
    /// the reader left unread on the stream.
    async fn read_wire(wire: &[u8]) -> (Result<SieveStatus, Error>, Vec<u8>) {
        let (client, mut peer) = tokio::io::duplex(256 * 1024);
        let mut stream = SieveStream {
            transport: SieveTransport::Memory(client),
            budget: bifrost_net::BandwidthBudget::default(),
            sink: None,
        };
        peer.write_all(wire).await.unwrap();
        drop(peer);
        let result = read_response(&mut stream, Duration::from_secs(5))
            .await
            .map(|response| response.status);
        let mut rest = Vec::new();
        if let SieveTransport::Memory(client) = &mut stream.transport {
            client.read_to_end(&mut rest).await.unwrap();
        }
        (result, rest)
    }

    /// RFC 5804 lets the message be a literal. The octets and the closing
    /// CRLF belong to this response; left on the stream they would be read
    /// as the next one.
    #[tokio::test]
    async fn a_literal_status_message_is_read_whole() {
        let (status, rest) = read_wire(b"NO {15}\r\nline 1: bad\r\nx;\r\nNEXT\r\n").await;
        match status.unwrap() {
            SieveStatus::No { code, message } => {
                assert_eq!(code, None);
                assert_eq!(message, "line 1: bad\r\nx;");
            }
            other => panic!("expected NO, got {other:?}"),
        }
        assert_eq!(rest, b"NEXT\r\n");

        let (status, rest) = read_wire(b"OK (WARNINGS) {7}\r\nline 2\n\r\nNEXT\r\n").await;
        match status.unwrap() {
            SieveStatus::Ok { code, message } => {
                assert_eq!(code, Some(SieveResponseCode::Warnings));
                assert_eq!(message, "line 2\n");
            }
            other => panic!("expected OK, got {other:?}"),
        }
        assert_eq!(rest, b"NEXT\r\n");
    }

    /// A quoted "{3}" is a string, not a literal marker.
    #[tokio::test]
    async fn a_quoted_brace_message_is_not_a_literal() {
        let (status, rest) = read_wire(b"NO \"{3}\"\r\nNEXT\r\n").await;
        assert!(matches!(status.unwrap(), SieveStatus::No { message, .. } if message == "{3}"));
        assert_eq!(rest, b"NEXT\r\n");
    }

    /// Code arguments may be literals and nested lists; the code ends at
    /// the `)` that balances its `(`.
    #[tokio::test]
    async fn code_arguments_may_be_literals_and_nested_lists() {
        let (status, rest) =
            read_wire(b"NO (FROB (\"x\" {3}\r\nabc) \"y\") \"rejected\"\r\nNEXT\r\n").await;
        match status.unwrap() {
            SieveStatus::No { code, message } => {
                assert_eq!(
                    code,
                    Some(SieveResponseCode::Other(
                        "FROB ( \"x\" \"abc\" ) \"y\"".to_owned()
                    ))
                );
                assert_eq!(message, "rejected");
            }
            other => panic!("expected NO, got {other:?}"),
        }
        assert_eq!(rest, b"NEXT\r\n");
    }

    #[tokio::test]
    async fn a_status_record_is_bounded_in_total() {
        let mut wire = b"NO (FROB".to_vec();
        for _ in 0..3 {
            wire.extend_from_slice(b" {30000}\r\n");
            wire.extend(std::iter::repeat_n(b'a', 30000));
        }
        wire.extend_from_slice(b") \"x\"\r\n");
        assert!(matches!(read_wire(&wire).await.0, Err(Error::Parse(_))));
        let (status, _) = read_wire(b"NO {99999999999999999999999}\r\n").await;
        assert!(matches!(status, Err(Error::Parse(_))));
    }

    /// SASL success data may arrive as a literal; a SASL code without its
    /// one string argument is malformed, not absent.
    #[test]
    fn sasl_success_data_takes_exactly_one_string_argument() {
        let record = parse_status(&[
            Segment::Text("OK (SASL ".to_owned()),
            Segment::Literal(b"dj14".to_vec()),
            Segment::Text(") \"authenticated\"".to_owned()),
        ])
        .unwrap();
        let SieveStatus::Ok { code, message } = &record.status else {
            panic!("expected OK");
        };
        assert_eq!(message, "authenticated");
        assert_eq!(
            sasl_success_data(code.as_ref(), &record.code_args).unwrap(),
            Some(b"v=x".to_vec())
        );
        for line in ["OK (SASL)", "OK (SASL atom \"dj14\")"] {
            let record = parse_status(&[Segment::Text(line.to_owned())]).unwrap();
            let SieveStatus::Ok { code, .. } = &record.status else {
                panic!("expected OK");
            };
            assert!(
                sasl_success_data(code.as_ref(), &record.code_args).is_err(),
                "{line}"
            );
        }
    }

    /// A quoted `)` inside a code argument does not close the code.
    #[test]
    fn a_quoted_paren_does_not_close_the_code() {
        let record =
            parse_status(&[Segment::Text(r#"OK (TAG ")") "informational""#.to_owned())]).unwrap();
        let SieveStatus::Ok { code, message } = record.status else {
            panic!("expected OK");
        };
        assert_eq!(
            code.map(|code| code.meaning()),
            Some(SieveResponseCode::Tag)
        );
        assert_eq!(message, "informational");
    }

    /// `OK (WARNINGS)` on CHECKSCRIPT is a valid script with something to
    /// say; the user must see it rather than a clean result.
    #[test]
    fn checkscript_ok_warnings_become_warning_diagnostics() {
        for (code, message, expected) in [
            (
                SieveResponseCode::Warnings,
                "line 3: deprecated",
                "line 3: deprecated",
            ),
            (
                SieveResponseCode::parse("WARNINGS/FROB"),
                "",
                "Script compiled with warnings",
            ),
        ] {
            let outcome = validation_outcome(SieveStatus::Ok {
                code: Some(code),
                message: message.into(),
            })
            .unwrap();
            assert!(outcome.is_valid());
            assert_eq!(outcome.diagnostics.len(), 1);
            assert_eq!(
                outcome.diagnostics[0].severity,
                FilterDiagnosticSeverity::Warning
            );
            assert_eq!(outcome.diagnostics[0].message, expected);
        }
        let clean = validation_outcome(SieveStatus::Ok {
            code: None,
            message: "Script is fine".into(),
        })
        .unwrap();
        assert!(clean.diagnostics.is_empty());
    }

    /// RFC 5804 puts the parenthesized code BEFORE the human string, so a
    /// parser that reads the message first swallows the code as opaque
    /// text - which is how `[TRYLATER]` came to derive terminal.
    #[test]
    fn a_response_code_is_lifted_off_ahead_of_the_message() {
        let (code, message) = no_status(r#"NO (TRYLATER) "Server busy, try again""#);
        assert_eq!(code, Some(SieveResponseCode::TryLater));
        assert_eq!(message, "Server busy, try again");
    }

    /// The quota sub-kind is kept: `MAXSIZE` is a fixed per-script limit and
    /// `MAXSCRIPTS` is account capacity, so they classify differently. An
    /// unknown sub-kind is preserved whole rather than folded into `QUOTA`.
    #[test]
    fn a_quota_subkind_is_kept() {
        let (code, _) = no_status(r#"NO (QUOTA/MAXSCRIPTS) "Too many scripts""#);
        assert_eq!(code, Some(SieveResponseCode::QuotaMaxScripts));
        let (code, _) = no_status(r#"NO (quota/maxsize) "Script too large""#);
        assert_eq!(code, Some(SieveResponseCode::QuotaMaxSize));
        assert_eq!(
            code.as_ref().map(SieveResponseCode::code),
            Some("QUOTA/MAXSIZE")
        );
        let (code, _) = no_status(r#"NO (QUOTA) "Over quota""#);
        assert_eq!(code, Some(SieveResponseCode::Quota));
        let (code, _) = no_status(r#"NO (QUOTA/MAXFROB) "?""#);
        assert_eq!(
            code,
            Some(SieveResponseCode::Other("QUOTA/MAXFROB".to_owned()))
        );
    }

    /// An unmodelled extension code must be preserved verbatim rather
    /// than dropped, and must not be mistaken for one we do model.
    #[test]
    fn an_unknown_response_code_is_preserved_verbatim() {
        let (code, message) = no_status(r#"NO (FROBNICATE) "nope""#);
        assert_eq!(code, Some(SieveResponseCode::Other("FROBNICATE".into())));
        assert_eq!(message, "nope");
    }

    #[test]
    fn a_status_line_without_a_code_still_parses_its_message() {
        let (code, message) = no_status(r#"NO "plain refusal""#);
        assert_eq!(code, None);
        assert_eq!(message, "plain refusal");
    }

    /// An unbalanced parenthesis must not be read as a code - inventing
    /// one would classify off text the server never framed as a code.
    #[test]
    fn an_unbalanced_paren_is_treated_as_message_text() {
        let (code, _) = no_status("NO (TRYLATER oops");
        assert_eq!(code, None);
    }

    /// A real compile failure is the verdict, and stays diagnostics.
    #[test]
    fn checkscript_rejection_without_a_transient_code_is_a_verdict() {
        let outcome = validation_outcome(SieveStatus::No {
            code: None,
            message: "line 3: unknown command".into(),
        })
        .expect("a compile failure is a validation result, not an error");
        assert_eq!(outcome.diagnostics.len(), 1);
        assert_eq!(outcome.diagnostics[0].message, "line 3: unknown command");
    }

    /// But a busy server never judged the script. Reporting that as a
    /// validation failure tells the user their script is broken when it
    /// may be fine, and throws away the retryable classification.
    #[test]
    fn checkscript_trylater_is_an_error_not_a_validation_verdict() {
        let error = validation_outcome(SieveStatus::No {
            code: Some(SieveResponseCode::TryLater),
            message: "server busy".into(),
        })
        .expect_err("a transient refusal is not a verdict about the script");
        assert!(matches!(
            error,
            Error::Sieve {
                code: Some(SieveResponseCode::TryLater),
                ..
            }
        ));
    }

    /// RFC 5804 2.12 forbids CHECKSCRIPT from checking quota, so a quota
    /// code (including unrecognized detail under QUOTA) is not a syntax
    /// verdict, and neither is a BYE whatever its code.
    #[test]
    fn checkscript_operational_refusals_are_errors_not_verdicts() {
        for raw in [
            "QUOTA",
            "QUOTA/MAXSCRIPTS",
            "QUOTA/MAXSIZE",
            "QUOTA/MAXFROB",
            "TRYLATER/FROB",
            "NONEXISTENT",
        ] {
            let code = SieveResponseCode::parse(raw);
            let error = validation_outcome(SieveStatus::No {
                code: Some(code.clone()),
                message: "refused".into(),
            })
            .expect_err(raw);
            assert!(
                matches!(&error, Error::Sieve { code: Some(c), .. } if *c == code),
                "{raw}"
            );
        }
        for code in [None, Some(SieveResponseCode::Warnings)] {
            validation_outcome(SieveStatus::Bye {
                code,
                message: "closing".into(),
            })
            .expect_err("a closing connection has not judged the script");
        }
    }

    /// Advisory and unknown codes keep the plain reading of a NO.
    #[test]
    fn checkscript_advisory_or_unknown_code_is_a_verdict() {
        for raw in [
            "WARNINGS",
            "TAG \"x\"",
            "FROB \"QUOTA/MAXSIZE\"",
            "QUOTAX/MAXSIZE",
        ] {
            let outcome = validation_outcome(SieveStatus::No {
                code: Some(SieveResponseCode::parse(raw)),
                message: "line 1: bad".into(),
            })
            .expect(raw);
            assert!(!outcome.is_valid(), "{raw}");
        }
    }

    #[test]
    fn unrecognized_hierarchical_detail_resolves_to_its_ancestor() {
        use SieveResponseCode as C;
        for (raw, meaning) in [
            ("quota/maxsize/frob \"p\"", C::QuotaMaxSize),
            ("QUOTA/MAXFROB", C::Quota),
            ("TRYLATER/FROB", C::TryLater),
            ("WARNINGS/FROB", C::Warnings),
            ("TAG/FROB \"t\"", C::Tag),
        ] {
            let code = C::parse(raw);
            assert_eq!(code.meaning(), meaning, "{raw}");
            assert_eq!(code.code(), raw, "wire spelling kept");
        }
        for raw in ["QUOTAX/MAXSIZE", "FROB \"QUOTA/MAXSIZE\""] {
            assert_eq!(C::parse(raw).meaning(), C::Other(raw.into()), "{raw}");
        }
    }

    /// Feed `wire` to a reader with `limits` and return the whole response
    /// plus whatever was left unread.
    async fn read_records(
        wire: &[u8],
        limits: ResponseLimits,
    ) -> (Result<SieveResponse, Error>, Vec<u8>) {
        let (client, mut peer) = tokio::io::duplex(256 * 1024);
        let mut stream = SieveStream {
            transport: SieveTransport::Memory(client),
            budget: bifrost_net::BandwidthBudget::default(),
            sink: None,
        };
        peer.write_all(wire).await.unwrap();
        drop(peer);
        let result = read_response_within(&mut stream, Duration::from_secs(5), limits).await;
        let mut rest = Vec::new();
        if let SieveTransport::Memory(client) = &mut stream.transport {
            client.read_to_end(&mut rest).await.unwrap();
        }
        (result, rest)
    }

    async fn listed(wire: &[u8]) -> Result<Vec<(String, bool)>, Error> {
        let (response, _) = read_records(wire, ResponseLimits::DEFAULT).await;
        Ok(list_scripts_from(response?.records)?
            .into_iter()
            .map(|script| (script.name, script.is_active))
            .collect())
    }

    #[tokio::test]
    async fn capabilities_read_quoted_and_literal_values() {
        let (response, rest) = read_records(
            b"\"IMPLEMENTATION\" \"Example\"\r\n\"SASL\" {13}\r\nPLAIN XOAUTH2\r\n{8}\r\nSTARTTLS\r\nOK\r\nNEXT\r\n",
            ResponseLimits::DEFAULT,
        )
        .await;
        let caps = response.unwrap().capabilities().unwrap();
        assert!(caps.starttls);
        assert_eq!(caps.sasl, vec!["PLAIN".to_string(), "XOAUTH2".to_string()]);
        assert_eq!(rest, b"NEXT\r\n");
    }

    /// A literal script name and the ` ACTIVE` continuing its record are one
    /// entry, and literal octets that look like a status are data.
    #[tokio::test]
    async fn listscripts_reads_literal_names_as_records() {
        let scripts = listed(
            b"\"plain\"\r\n{4}\r\nmain ACTIVE\r\n{2}\r\nOK\r\n\"caf\xc3\xa9 \\\"q\\\"\"\r\nOK\r\n",
        )
        .await
        .unwrap();
        assert_eq!(
            scripts,
            vec![
                ("plain".to_owned(), false),
                ("main".to_owned(), true),
                ("OK".to_owned(), false),
                ("caf\u{e9} \"q\"".to_owned(), false),
            ]
        );
    }

    #[tokio::test]
    async fn listscripts_refuses_malformed_entries() {
        for wire in [
            b"\"main\"ACTIVE\r\nOK\r\n".as_slice(),
            b"\"main\"  ACTIVE\r\nOK\r\n",
            b"\"\"\r\nOK\r\n",
            b"\"a\" ACTIVE\r\n\"b\" ACTIVE\r\nOK\r\n",
            b"\"a\\n\"\r\nOK\r\n",
            b"\"a\" ACTIVE extra\r\nOK\r\n",
            b"main\r\nOK\r\n",
        ] {
            assert!(
                matches!(listed(wire).await, Err(Error::Parse(_))),
                "{:?}",
                String::from_utf8_lossy(wire)
            );
        }
    }

    /// A data line ending in `}` is a literal or a refusal: never text, and
    /// never a count the client allocates unchecked.
    #[tokio::test]
    async fn data_literal_markers_are_strict() {
        for wire in [
            b"{4+}\r\nmain\r\nOK\r\n".as_slice(),
            b"{}\r\nOK\r\n",
            b"{04}\r\nmain\r\nOK\r\n",
            b"{99999999999}\r\nOK\r\n",
            b"\"a\"{4}\r\nmain\r\nOK\r\n",
        ] {
            let (response, _) = read_records(wire, ResponseLimits::DEFAULT).await;
            assert!(
                matches!(response, Err(Error::Parse(_))),
                "{:?}",
                String::from_utf8_lossy(wire)
            );
        }
    }

    #[tokio::test]
    async fn data_responses_are_bounded_before_reading() {
        let limits = ResponseLimits {
            record: 32,
            response: 48,
            records: 2,
            literal: 32,
        };
        // A literal within its own bound that passes what its record has
        // left: refused on the count, its octets unread.
        let (response, rest) =
            read_records(b"{30}\r\n012345678901234567890123456789\r\nNEXT", limits).await;
        assert!(matches!(response, Err(Error::Parse(_))));
        assert_eq!(rest, b"012345678901234567890123456789\r\nNEXT");
        // Records within their own bound that together pass the response's:
        // the second is refused before its line is read out.
        let (response, rest) = read_records(
            b"\"aaaaaaaaaaaaaaaaaaaa\"\r\n\"bbbbbbbbbbbbbbbbbbbbb\"\r\nOK\r\n",
            limits,
        )
        .await;
        assert!(response.is_err());
        assert_eq!(rest, b"\nOK\r\n");
        // More records than the command allows: refused at the first byte.
        let (response, rest) = read_records(b"\"a\"\r\n\"b\"\r\n\"ccc\"\r\nOK\r\n", limits).await;
        assert!(response.is_err());
        assert_eq!(rest, b"ccc\"\r\nOK\r\n");
        // A status line keeps its own bound even where a data record may
        // be large.
        let mut long_status = b"OK \"".to_vec();
        long_status.extend(std::iter::repeat_n(b'a', 70 * 1024));
        long_status.extend_from_slice(b"\"\r\n");
        let (response, _) = read_records(&long_status, ResponseLimits::SCRIPT).await;
        assert!(response.is_err());
        // A status continuation gets only what the record has left, so it
        // is refused before it is read out.
        let mut continued = b"NO (FROB {65500}\r\n".to_vec();
        continued.extend(std::iter::repeat_n(b'x', 65_500));
        continued.extend_from_slice(b" \"");
        continued.extend(std::iter::repeat_n(b'a', 1000));
        continued.extend_from_slice(b"\")\r\nNEXT");
        let (response, rest) = read_records(&continued, ResponseLimits::DEFAULT).await;
        assert!(response.is_err());
        assert!(
            rest.ends_with(b"\")\r\nNEXT") && rest.len() > 900,
            "{}",
            rest.len()
        );
        let (response, _) = read_records(b"\"a\"\r\n\"b\"\r\nOK\r\n", limits).await;
        assert_eq!(response.unwrap().records.len(), 2);
        // A literal exactly at its own bound fits with its framing; one
        // octet over is refused before it is read.
        let tight = ResponseLimits {
            record: 16,
            response: 16,
            records: 1,
            literal: 8,
        };
        let (response, _) = read_records(b"{8}\r\n12345678\r\nOK\r\n", tight).await;
        assert_eq!(response.unwrap().records.len(), 1);
        let (response, rest) = read_records(b"{9}\r\n123456789\r\nOK\r\n", tight).await;
        assert!(matches!(response, Err(Error::Parse(_))));
        assert_eq!(rest, b"123456789\r\nOK\r\n");
    }

    #[tokio::test]
    async fn getscript_takes_one_string_literal_or_quoted() {
        let script = |wire: &'static [u8]| async move {
            let (response, _) = read_records(wire, ResponseLimits::SCRIPT).await;
            script_from(response?.records)
        };
        assert_eq!(script(b"{5}\r\nkeep;\r\nOK\r\n").await.unwrap(), "keep;");
        assert_eq!(script(b"\"keep;\"\r\nOK\r\n").await.unwrap(), "keep;");
        assert_eq!(script(b"{0}\r\n\r\nOK\r\n").await.unwrap(), "");
        assert!(script(b"OK\r\n").await.is_err());
        assert!(script(b"{1}\r\na\r\n{1}\r\nb\r\nOK\r\n").await.is_err());
        assert!(script(b"{1}\r\na ACTIVE\r\nOK\r\n").await.is_err());
        let oversized = format!("{{{}}}\r\n", SCRIPT_BODY_LIMIT + 1);
        let (response, rest) = read_records(oversized.as_bytes(), ResponseLimits::SCRIPT).await;
        assert!(matches!(response, Err(Error::Parse(_))));
        assert!(rest.is_empty());
    }

    #[test]
    fn advisory_is_warnings_text_only() {
        let ok = |code: Option<SieveResponseCode>, message: &str| SieveStatus::Ok {
            code,
            message: message.to_owned(),
        };
        assert_eq!(
            ok(Some(SieveResponseCode::Warnings), "line 3: x").advisory(),
            Some("line 3: x".to_owned())
        );
        assert_eq!(
            ok(Some(SieveResponseCode::parse("WARNINGS/FROB")), "").advisory(),
            Some(String::new())
        );
        assert_eq!(ok(None, "done").advisory(), None);
        assert_eq!(
            SieveStatus::No {
                code: Some(SieveResponseCode::Warnings),
                message: "x".to_owned()
            }
            .advisory(),
            None
        );
    }

    #[test]
    fn quote_string_escapes_quotes_and_backslashes() {
        assert_eq!(quote_string("a\"b\\c").expect("quoted"), "\"a\\\"b\\\\c\"");
    }

    #[test]
    fn literal_len_accepts_server_literal_shape_only() {
        assert_eq!(literal_len("{12}"), Some(12));
        assert_eq!(literal_len("{12+}"), None);
    }

    #[test]
    fn script_name_arg_rejects_empty_name() {
        assert!(script_name_arg("").is_err());
        assert_eq!(script_name_arg("main").expect("name"), "\"main\"");
    }

    #[test]
    fn putscript_literal_command_includes_trailing_crlf() {
        let command =
            literal_command("PUTSCRIPT", Some("main"), "keep;").expect("command serialized");
        assert_eq!(command, b"PUTSCRIPT \"main\" {5+}\r\nkeep;\r\n");
    }

    #[test]
    fn checkscript_literal_command_includes_trailing_crlf() {
        let command = literal_command("CHECKSCRIPT", None, "keep;").expect("command serialized");
        assert_eq!(command, b"CHECKSCRIPT {5+}\r\nkeep;\r\n");
    }
}
