#![allow(clippy::wildcard_imports)]
use super::*;

/// Connection configuration for [`ImapConnection`].
#[non_exhaustive]
#[derive(Clone)]
pub struct ImapConfig {
    /// Server hostname.
    pub host: String,
    /// Server port.
    pub port: u16,
    /// TLS policy.
    pub tls_mode: TlsMode,
    /// Timeout for connect and initial greeting/capability negotiation.
    pub connect_timeout: Duration,
    /// Default timeout for higher-level helper operations.
    pub command_timeout: Duration,
    /// Optional TCP keepalive configuration.
    pub keepalive: Option<TcpKeepalive>,
    /// Optional custom native-tls connector.
    pub tls_connector: Option<native_tls::TlsConnector>,
}

impl ImapConfig {
    /// Create an implicit-TLS IMAP configuration for port 993.
    pub fn tls(host: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port: 993,
            tls_mode: TlsMode::Implicit,
            connect_timeout: Duration::from_secs(30),
            command_timeout: Duration::from_secs(60),
            keepalive: Some(TcpKeepalive::new(
                Duration::from_secs(120),
                Duration::from_secs(60),
            )),
            tls_connector: None,
        }
    }

    /// Create a STARTTLS IMAP configuration for port 143.
    pub fn starttls(host: impl Into<String>) -> Self {
        Self {
            tls_mode: TlsMode::StartTls,
            port: 143,
            ..Self::tls(host)
        }
    }

    /// Create a plaintext IMAP configuration for port 143.
    pub fn plaintext(host: impl Into<String>) -> Self {
        Self {
            tls_mode: TlsMode::None,
            port: 143,
            ..Self::tls(host)
        }
    }

    /// Override the port.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Override the TLS mode.
    pub fn with_tls_mode(mut self, tls_mode: TlsMode) -> Self {
        self.tls_mode = tls_mode;
        self
    }

    /// Override the connect timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Override the default command timeout.
    pub fn with_command_timeout(mut self, timeout: Duration) -> Self {
        self.command_timeout = timeout;
        self
    }

    /// Override TCP keepalive.
    pub fn with_keepalive(mut self, keepalive: Option<TcpKeepalive>) -> Self {
        self.keepalive = keepalive;
        self
    }

    /// Use a custom native-tls connector.
    pub fn with_tls_connector(mut self, connector: native_tls::TlsConnector) -> Self {
        self.tls_connector = Some(connector);
        self
    }

    /// Connect using this configuration.
    pub async fn connect(&self) -> Result<ImapConnection, Error> {
        ImapConnection::connect_config(self).await
    }

    /// Connect and authenticate using automatic mechanism selection.
    pub async fn connect_authenticated(
        &self,
        credentials: &crate::types::Credentials,
        policy: &crate::types::AuthPolicy,
    ) -> Result<(ImapConnection, crate::types::AuthOutcome), Error> {
        let conn = self.connect().await?;
        let outcome = conn
            .authenticate_best(credentials, policy, self.command_timeout)
            .await?;
        Ok((conn, outcome))
    }
}

impl std::fmt::Debug for ImapConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImapConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("tls_mode", &self.tls_mode)
            .field("connect_timeout", &self.connect_timeout)
            .field("command_timeout", &self.command_timeout)
            .field("keepalive", &self.keepalive)
            .field(
                "tls_connector",
                &self.tls_connector.as_ref().map(|_| "<custom>"),
            )
            .finish()
    }
}

impl ImapConnection {
    /// Connect using a configuration object.
    pub async fn connect_config(config: &ImapConfig) -> Result<Self, Error> {
        let conn = if let Some(connector) = &config.tls_connector {
            Self::connect_with_tls_connector(
                &config.host,
                config.port,
                config.tls_mode,
                connector.clone(),
                config.connect_timeout,
            )
            .await?
        } else {
            Self::connect(
                &config.host,
                config.port,
                config.tls_mode,
                config.connect_timeout,
            )
            .await?
        };

        if let Some(keepalive) = config.keepalive {
            conn.set_keepalive(keepalive).await?;
        }

        Ok(conn)
    }
}
