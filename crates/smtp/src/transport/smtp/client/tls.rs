use std::fmt::{self, Debug};

#[cfg(feature = "native-tls")]
use native_tls::{Protocol, TlsConnector};

#[cfg(feature = "native-tls")]
use crate::transport::smtp::{Error, error};

#[cfg(not(feature = "native-tls"))]
#[derive(Clone)]
struct NoTlsParameters;

/// TLS protocol versions.
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum TlsVersion {
    /// TLS 1.0
    ///
    /// Should only be used when trying to support legacy SMTP servers that
    /// have not updated to at least TLS 1.2 yet.
    Tlsv10,
    /// TLS 1.1
    ///
    /// Should only be used when trying to support legacy SMTP servers that
    /// have not updated to at least TLS 1.2 yet.
    Tlsv11,
    /// TLS 1.2
    ///
    /// A good option for most SMTP servers.
    Tlsv12,
    /// TLS 1.3
    ///
    /// This is not configurable through native-tls and will return an error
    /// if used as the minimum version.
    Tlsv13,
}

/// Specifies how to establish a TLS connection.
///
/// Use [`Tls::Wrapper`] or [`Tls::Required`] when connecting to a remote
/// server, and [`Tls::None`] when connecting to a trusted local server.
#[derive(Clone)]
#[allow(missing_copy_implementations)]
pub enum Tls {
    /// Insecure plaintext connection only.
    ///
    /// This option should only be used for trusted local relays. Remote
    /// servers can reject it, and credentials or messages sent over it are
    /// observable on the network.
    None,
    /// Begin with a plaintext connection and attempt to use `STARTTLS` if
    /// available.
    ///
    /// This is kept for compatibility with servers that advertise STARTTLS
    /// opportunistically. Prefer [`Tls::Required`] or [`Tls::Wrapper`] for
    /// authenticated remote SMTP.
    #[cfg(feature = "native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
    Opportunistic(TlsParameters),
    /// Begin with a plaintext connection and require `STARTTLS` before
    /// transmitting credentials or messages.
    #[cfg(feature = "native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
    Required(TlsParameters),
    /// Establish a TLS connection immediately.
    #[cfg(feature = "native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
    Wrapper(TlsParameters),
}

impl Debug for Tls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::None => f.pad("None"),
            #[cfg(feature = "native-tls")]
            Self::Opportunistic(_) => f.pad("Opportunistic"),
            #[cfg(feature = "native-tls")]
            Self::Required(_) => f.pad("Required"),
            #[cfg(feature = "native-tls")]
            Self::Wrapper(_) => f.pad("Wrapper"),
        }
    }
}

/// Source for the base set of root certificates to trust.
#[allow(missing_copy_implementations)]
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub enum CertificateStore {
    /// Use the platform certificate store selected by native-tls.
    ///
    /// Since bifrost-smtp only supports native-tls, this variant always means
    /// the native-tls platform verifier rather than a backend-dependent
    /// default.
    ///
    /// This uses schannel on Windows, Security-Framework on macOS, and
    /// OpenSSL directories on Linux.
    #[default]
    Default,
    /// Do not use any system certificates.
    None,
}

/// Parameters to use for secure clients.
#[derive(Clone)]
pub struct TlsParameters {
    #[cfg(feature = "native-tls")]
    pub(crate) connector: InnerTlsParameters,
    /// The domain name expected in the server TLS certificate.
    #[cfg(feature = "native-tls")]
    pub(super) domain: String,
    /// Placeholder for builds where TLS cannot be constructed.
    #[cfg(not(feature = "native-tls"))]
    _private: NoTlsParameters,
}

/// Builder for [`TlsParameters`].
#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "native-tls"), allow(dead_code))]
pub struct TlsParametersBuilder {
    domain: String,
    cert_store: CertificateStore,
    #[cfg(feature = "native-tls")]
    root_certs: Vec<Certificate>,
    #[cfg(feature = "native-tls")]
    identity: Option<Identity>,
    accept_invalid_hostnames: bool,
    accept_invalid_certs: bool,
    min_tls_version: TlsVersion,
}

impl TlsParametersBuilder {
    /// Creates a new builder for [`TlsParameters`].
    pub fn new(domain: String) -> Self {
        Self {
            domain,
            cert_store: CertificateStore::Default,
            #[cfg(feature = "native-tls")]
            root_certs: Vec::new(),
            #[cfg(feature = "native-tls")]
            identity: None,
            accept_invalid_hostnames: false,
            accept_invalid_certs: false,
            min_tls_version: TlsVersion::Tlsv12,
        }
    }

    /// Set the source for the base set of root certificates to trust.
    pub fn certificate_store(mut self, cert_store: CertificateStore) -> Self {
        self.cert_store = cert_store;
        self
    }

    /// Add a custom root certificate.
    ///
    /// Can be used to safely connect to a server using a self-signed
    /// certificate, for example.
    #[cfg(feature = "native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
    pub fn add_root_certificate(mut self, cert: Certificate) -> Self {
        self.root_certs.push(cert);
        self
    }

    /// Add a client certificate.
    #[cfg(feature = "native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
    pub fn identify_with(mut self, identity: Identity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Controls whether certificates with an invalid hostname are accepted.
    ///
    /// Defaults to `false`.
    ///
    /// You should think very carefully before using this method. If hostname
    /// verification is disabled, any valid certificate, including those from
    /// other sites, is trusted.
    pub fn dangerous_accept_invalid_hostnames(mut self, accept_invalid_hostnames: bool) -> Self {
        self.accept_invalid_hostnames = accept_invalid_hostnames;
        self
    }

    /// Controls which minimum TLS version is allowed.
    ///
    /// Defaults to [`TlsVersion::Tlsv12`].
    pub fn set_min_tls_version(mut self, min_tls_version: TlsVersion) -> Self {
        self.min_tls_version = min_tls_version;
        self
    }

    /// Controls whether invalid certificates are accepted.
    ///
    /// Defaults to `false`.
    ///
    /// This should only be used as a last resort, as it accepts self-signed,
    /// wrong-host, and expired certificates.
    pub fn dangerous_accept_invalid_certs(mut self, accept_invalid_certs: bool) -> Self {
        self.accept_invalid_certs = accept_invalid_certs;
        self
    }

    /// Creates a new [`TlsParameters`] using native-tls.
    #[cfg(feature = "native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
    pub fn build(self) -> Result<TlsParameters, Error> {
        self.build_native()
    }

    /// Creates a new [`TlsParameters`] using native-tls.
    #[cfg(feature = "native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
    pub fn build_native(self) -> Result<TlsParameters, Error> {
        let mut tls_builder = TlsConnector::builder();

        match self.cert_store {
            CertificateStore::Default => {}
            CertificateStore::None => {
                tls_builder.disable_built_in_roots(true);
            }
        }

        for cert in self.root_certs {
            tls_builder.add_root_certificate(cert.native_tls);
        }

        tls_builder.danger_accept_invalid_hostnames(self.accept_invalid_hostnames);
        tls_builder.danger_accept_invalid_certs(self.accept_invalid_certs);

        let min_tls_version = match self.min_tls_version {
            TlsVersion::Tlsv10 => Protocol::Tlsv10,
            TlsVersion::Tlsv11 => Protocol::Tlsv11,
            TlsVersion::Tlsv12 => Protocol::Tlsv12,
            TlsVersion::Tlsv13 => {
                return Err(error::tls(
                    "min tls version Tlsv13 is not supported by native-tls",
                ));
            }
        };

        tls_builder.min_protocol_version(Some(min_tls_version));
        if let Some(identity) = self.identity {
            tls_builder.identity(identity.native_tls);
        }

        let connector = tls_builder.build().map_err(error::tls)?;
        Ok(TlsParameters {
            connector: InnerTlsParameters::NativeTls { connector },
            domain: self.domain,
        })
    }
}

#[cfg(feature = "native-tls")]
#[derive(Clone)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum InnerTlsParameters {
    NativeTls { connector: TlsConnector },
}

impl TlsParameters {
    /// Creates a new [`TlsParameters`] using native-tls.
    #[cfg(feature = "native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
    pub fn new(domain: String) -> Result<Self, Error> {
        TlsParametersBuilder::new(domain).build()
    }

    /// Creates a new [`TlsParameters`] builder.
    pub fn builder(domain: String) -> TlsParametersBuilder {
        TlsParametersBuilder::new(domain)
    }

    /// Creates a new [`TlsParameters`] using native-tls.
    #[cfg(feature = "native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
    pub fn new_native(domain: String) -> Result<Self, Error> {
        TlsParametersBuilder::new(domain).build_native()
    }

    #[cfg(feature = "native-tls")]
    pub fn domain(&self) -> &str {
        &self.domain
    }
}

/// A certificate that can be used with
/// [`TlsParametersBuilder::add_root_certificate`].
#[derive(Clone)]
#[allow(missing_copy_implementations)]
#[cfg(feature = "native-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
pub struct Certificate {
    native_tls: native_tls::Certificate,
}

#[cfg(feature = "native-tls")]
impl Certificate {
    /// Create a [`Certificate`] from a DER encoded certificate.
    pub fn from_der(der: Vec<u8>) -> Result<Self, Error> {
        Ok(Self {
            native_tls: native_tls::Certificate::from_der(&der).map_err(error::tls)?,
        })
    }

    /// Create a [`Certificate`] from a PEM encoded certificate.
    pub fn from_pem(pem: &[u8]) -> Result<Self, Error> {
        Ok(Self {
            native_tls: native_tls::Certificate::from_pem(pem).map_err(error::tls)?,
        })
    }
}

#[cfg(feature = "native-tls")]
impl Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Certificate").finish()
    }
}

/// An identity that can be used with [`TlsParametersBuilder::identify_with`].
#[derive(Clone)]
#[allow(missing_copy_implementations)]
#[cfg(feature = "native-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
pub struct Identity {
    native_tls: native_tls::Identity,
}

#[cfg(feature = "native-tls")]
impl Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identity").finish()
    }
}

#[cfg(feature = "native-tls")]
impl Identity {
    pub fn from_pem(pem: &[u8], key: &[u8]) -> Result<Self, Error> {
        Ok(Self {
            native_tls: native_tls::Identity::from_pkcs8(pem, key).map_err(error::tls)?,
        })
    }
}
