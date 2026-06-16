use std::sync::Arc;
use std::time::Duration;

use bifrost_types::{Account, AccountError, AccountFactory, AccountFuture, AccountId};

use bifrost_caldav::{CalDavAccountFactory, CalDavConfig};
use bifrost_carddav::{CardDavAccountFactory, CardDavConfig};

use crate::connection::ImapConfig;
use crate::types::{AuthPolicy, Capability, Credentials, MailboxInfo, ServerProfile};

use super::error::ImapErrorContext;
use super::sieve::ManageSieveConfig;
use super::submission::{SmtpSubmissionConfig, SubmissionTransport};
use super::{
    ImapAccount, ImapAccountParts, Pool, account_error_with, capabilities,
    folder_registry::FolderRegistry,
};
use bifrost_types::AccountOperation;

/// Account-boundary translation for every leg of `open` (connect,
/// AUTH, ID, QRESYNC, LIST). All four are tagged `Discover` because
/// they belong to a single discovery phase. The AUTH leg is genuinely
/// idempotent at the protocol level (re-authenticating produces the
/// same outcome), so an `InFlight` transport drop here is safe to
/// retry; the central recovery mapping picks `Retry::SameRequest` for
/// idempotent ops regardless. Splitting these into per-phase variants
/// would not change recovery for IMAP; it would only inflate the
/// `AccountOperation` enum surface.
fn discover_err(err: crate::Error) -> AccountError {
    account_error_with(err, ImapErrorContext::operation(AccountOperation::Discover))
}

/// Configuration used by `ImapAccountFactory`.
#[non_exhaustive]
#[derive(Clone)]
pub struct ImapAccountConfig {
    pub imap: ImapConfig,
    pub credentials: Credentials,
    pub auth_policy: AuthPolicy,
    pub pool_cap: usize,
    pub idle_timeout: Duration,
    pub enable_qresync: bool,
    pub flag_sync_interval: Duration,
    pub deletion_check_interval: Duration,
    pub mutation_batch_size: usize,
    pub bandwidth_meter: Option<Arc<bifrost_net::BandwidthMeter>>,
    pub meter_sink: Option<Arc<dyn bifrost_net::MeterSink>>,
    pub sieve: Option<ManageSieveConfig>,
    pub carddav: Option<CardDavConfig>,
    pub caldav: Option<CalDavConfig>,
    pub submission: Option<SmtpSubmissionConfig>,
}

impl ImapAccountConfig {
    pub fn new(imap: ImapConfig, credentials: Credentials, auth_policy: AuthPolicy) -> Self {
        Self {
            imap,
            credentials,
            auth_policy,
            pool_cap: 4,
            idle_timeout: Duration::from_secs(29 * 60),
            enable_qresync: false,
            flag_sync_interval: Duration::from_secs(300),
            deletion_check_interval: Duration::from_secs(600),
            mutation_batch_size: 1024,
            bandwidth_meter: None,
            meter_sink: None,
            sieve: None,
            carddav: None,
            caldav: None,
            submission: None,
        }
    }

    pub fn with_bandwidth_meter(mut self, meter: Arc<bifrost_net::BandwidthMeter>) -> Self {
        self.bandwidth_meter = Some(meter);
        self
    }

    pub fn with_meter_sink(mut self, sink: Arc<dyn bifrost_net::MeterSink>) -> Self {
        self.meter_sink = Some(sink);
        self
    }

    pub fn with_manage_sieve(mut self, config: ManageSieveConfig) -> Self {
        self.sieve = Some(config);
        self
    }

    pub fn with_carddav(mut self, config: CardDavConfig) -> Self {
        self.carddav = Some(config);
        self
    }

    pub fn with_caldav(mut self, config: CalDavConfig) -> Self {
        self.caldav = Some(config);
        self
    }

    pub fn with_submission(mut self, config: SmtpSubmissionConfig) -> Self {
        self.submission = Some(config);
        self
    }
}

pub struct ImapAccountFactory {
    cfg: Arc<ImapAccountConfig>,
}

impl ImapAccountFactory {
    pub fn new(config: ImapAccountConfig) -> Self {
        Self {
            cfg: Arc::new(config),
        }
    }
}

impl AccountFactory for ImapAccountFactory {
    fn open(&self, account_id: AccountId) -> AccountFuture<Result<Arc<dyn Account>, AccountError>> {
        let cfg = Arc::clone(&self.cfg);
        Box::pin(async move {
            let bandwidth_cap = Arc::new(std::sync::atomic::AtomicU64::new(
                super::UNLIMITED_BANDWIDTH,
            ));
            let meter = meter_handle(&cfg, account_id.clone());
            let (conn, _auth) = cfg
                .imap
                .connect_authenticated_metered(
                    &cfg.credentials,
                    &cfg.auth_policy,
                    meter.clone(),
                    Some(Arc::clone(&bandwidth_cap)),
                )
                .await
                .map_err(discover_err)?;

            let mut profile = conn.server_profile();
            let server_id = read_server_id(&conn, &cfg, &profile)
                .await
                .map_err(discover_err)?;
            let qresync = negotiate_qresync(&conn, &cfg, &profile, &server_id)
                .await
                .map_err(discover_err)?;
            profile = conn.server_profile();

            let folders = list_folders(&conn, &cfg, &profile)
                .await
                .map_err(discover_err)?;
            // Fail-soft: a DAV open failure degrades to IMAP-only for
            // this cycle instead of failing the whole IMAP account.
            let mut dav_degraded = Vec::new();
            let contacts = open_carddav(&cfg, account_id.clone())
                .await
                .into_attached(&mut dav_degraded);
            let calendars = open_caldav(&cfg, account_id.clone())
                .await
                .into_attached(&mut dav_degraded);
            let submission = open_submission(&cfg)?;
            let caps = capabilities::build_capabilities(
                &profile,
                &folders,
                cfg.sieve.is_some(),
                contacts.as_ref().map(|c| c.capabilities()),
                calendars.as_ref().map(|c| c.capabilities()),
                submission.is_some(),
            );
            let registry = Arc::new(FolderRegistry::from_list(folders));
            let data_cap = cfg.pool_cap.saturating_sub(1).max(1);
            let pool = Arc::new(Pool::new(
                Arc::clone(&cfg),
                conn,
                data_cap,
                meter,
                Arc::clone(&bandwidth_cap),
            ));
            let account = ImapAccount::new(ImapAccountParts {
                config: cfg,
                capabilities: caps,
                pool,
                folders: registry,
                qresync_enabled: qresync.enabled,
                qresync_negotiation_warning: qresync.warning,
                bandwidth_cap,
                contacts,
                calendars,
                submission,
                dav_degraded,
            });
            Ok(Arc::new(account) as Arc<dyn Account>)
        })
    }
}

/// Outcome of attempting to attach a composed DAV sub-account. Never an
/// `Err`: a configured-but-failed open degrades to IMAP-only for this
/// open cycle rather than failing the whole IMAP account (brick 5).
enum DavAttach {
    /// Opened and attached.
    Attached(Arc<dyn Account>),
    /// Not configured.
    None,
    /// Configured but open failed; attach IMAP-only this cycle.
    Degraded {
        warning: bifrost_types::Warning,
        // Retried on the engine's next reopen by design. Carried for
        // telemetry/clarity; the engine's reopen re-runs the open either
        // way.
        #[allow(dead_code)]
        transient: bool,
    },
}

impl DavAttach {
    /// Resolve to the optional sub-account handle, pushing any degraded
    /// warning into `degraded` so the first discovery surfaces it.
    fn into_attached(self, degraded: &mut Vec<bifrost_types::Warning>) -> Option<Arc<dyn Account>> {
        match self {
            DavAttach::Attached(account) => Some(account),
            DavAttach::None => None,
            DavAttach::Degraded { warning, .. } => {
                degraded.push(warning);
                None
            }
        }
    }
}

/// Classify a DAV `open` outcome into an attach decision. A success
/// attaches; a failure degrades, routing through the central
/// `RecoveryClass` (read `reference/error-model.md`) to decide whether
/// the degradation is transient (retried on the engine's next reopen) or
/// a terminal config-fix.
fn classify_dav_open(result: Result<Arc<dyn Account>, AccountError>, label: &str) -> DavAttach {
    match result {
        Ok(account) => DavAttach::Attached(account),
        Err(error) => {
            // A retryable recovery class means the failure is transient
            // (transport blip, server unavailable, throttle); anything
            // terminal (auth lost, no permission, not found) needs an
            // operator config fix. Either way IMAP stays online.
            let transient = error.recovery().is_retryable();
            let kind = if transient {
                bifrost_types::WarningKind::Other
            } else {
                bifrost_types::WarningKind::OperatorAttentionNeeded
            };
            let warning = bifrost_types::Warning::support_only(
                kind,
                format!(
                    "{label} sub-account did not open ({}); continuing IMAP-only{}",
                    error.message_key(),
                    if transient {
                        ", will retry on reopen"
                    } else {
                        "; check DAV configuration"
                    }
                ),
            )
            .with_protocol_detail(bifrost_types::DiagnosticText::support_only(
                label.to_string(),
            ));
            DavAttach::Degraded { warning, transient }
        }
    }
}

async fn open_carddav(cfg: &ImapAccountConfig, account_id: AccountId) -> DavAttach {
    let Some(config) = cfg.carddav.clone() else {
        return DavAttach::None;
    };
    classify_dav_open(
        CardDavAccountFactory::new(config).open(account_id).await,
        "CardDAV",
    )
}

fn open_submission(
    cfg: &ImapAccountConfig,
) -> Result<Option<Arc<SubmissionTransport>>, AccountError> {
    let Some(config) = &cfg.submission else {
        return Ok(None);
    };
    let transport = SubmissionTransport::build(config, &cfg.credentials).map_err(discover_err)?;
    Ok(Some(Arc::new(transport)))
}

async fn open_caldav(cfg: &ImapAccountConfig, account_id: AccountId) -> DavAttach {
    let Some(config) = cfg.caldav.clone() else {
        return DavAttach::None;
    };
    classify_dav_open(
        CalDavAccountFactory::new(config).open(account_id).await,
        "CalDAV",
    )
}

fn meter_handle(
    cfg: &ImapAccountConfig,
    account_id: AccountId,
) -> Option<bifrost_net::MeterSinkHandle> {
    if let Some(meter) = &cfg.bandwidth_meter {
        return Some(bifrost_net::MeterSinkHandle::from_meter(
            Arc::clone(meter),
            account_id,
        ));
    }
    cfg.meter_sink
        .as_ref()
        .map(|sink| bifrost_net::MeterSinkHandle::new(Arc::clone(sink), account_id))
}

struct QresyncNegotiation {
    enabled: bool,
    warning: Option<String>,
}

async fn negotiate_qresync(
    conn: &crate::ImapConnection,
    cfg: &ImapAccountConfig,
    profile: &ServerProfile,
    server_id: &[(String, Option<String>)],
) -> Result<QresyncNegotiation, crate::Error> {
    if !cfg.enable_qresync || !profile.supports_qresync() {
        return Ok(QresyncNegotiation {
            enabled: false,
            warning: None,
        });
    }
    if server_id_disables_qresync(server_id) {
        return Ok(QresyncNegotiation {
            enabled: false,
            warning: Some(
                "server ID matches a known broken QRESYNC implementation; continuing with CONDSTORE"
                    .to_owned(),
            ),
        });
    }
    let enabled = conn.enable(&["QRESYNC"], cfg.imap.command_timeout).await?;
    let confirmed = enabled
        .iter()
        .any(|item| item.eq_ignore_ascii_case("QRESYNC"))
        || conn.server_profile().enabled("QRESYNC");
    Ok(QresyncNegotiation {
        enabled: confirmed,
        warning: (!confirmed).then(|| {
            "server advertised QRESYNC but ENABLE did not confirm it; continuing with CONDSTORE"
                .to_owned()
        }),
    })
}

async fn read_server_id(
    conn: &crate::ImapConnection,
    cfg: &ImapAccountConfig,
    profile: &ServerProfile,
) -> Result<Vec<(String, Option<String>)>, crate::Error> {
    if !profile.supports(Capability::Id) {
        return Ok(Vec::new());
    }
    match conn
        .id(&[("name", Some("bifrost-imap"))], cfg.imap.command_timeout)
        .await
    {
        Ok(id) => Ok(id),
        Err(_) => Ok(Vec::new()),
    }
}

fn server_id_disables_qresync(server_id: &[(String, Option<String>)]) -> bool {
    server_id.iter().any(|(key, value)| {
        let Some(value) = value.as_deref() else {
            return false;
        };
        let name = value.trim();
        key.eq_ignore_ascii_case("name")
            && (name.eq_ignore_ascii_case("icloud") || name.eq_ignore_ascii_case("icloud imap"))
    })
}

pub(crate) async fn list_folders(
    conn: &crate::ImapConnection,
    cfg: &ImapAccountConfig,
    profile: &ServerProfile,
) -> Result<Vec<MailboxInfo>, crate::Error> {
    if profile.supports(Capability::ListExtended) || profile.imap4rev2 {
        match conn
            .list_extended("", &["*"], &[], &["SPECIAL-USE"], cfg.imap.command_timeout)
            .await
        {
            Ok(folders) => return Ok(folders),
            Err(crate::Error::MissingCapability(_)) => {}
            Err(err) => return Err(err),
        }
    }
    conn.list("", "*", cfg.imap.command_timeout).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qresync_runtime_gate_defaults_off() {
        let cfg = ImapAccountConfig::new(
            ImapConfig::tls("imap.example.test"),
            Credentials::password("user", "pass"),
            AuthPolicy::default(),
        );
        assert!(!cfg.enable_qresync);
    }

    #[test]
    fn server_id_preconfigures_known_broken_qresync() {
        let id = vec![("name".to_string(), Some("iCloud IMAP".to_string()))];
        assert!(server_id_disables_qresync(&id));

        let id = vec![("name".to_string(), Some("Dovecot".to_string()))];
        assert!(!server_id_disables_qresync(&id));

        let id = vec![("name".to_string(), Some("MyIcloudProxy".to_string()))];
        assert!(!server_id_disables_qresync(&id));
    }

    fn transport_error() -> AccountError {
        bifrost_types::AccountErrorBuilder::new(
            bifrost_types::AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Network),
            bifrost_types::Cause::Transport(bifrost_types::TransportCause::new(
                bifrost_types::TransportKind::Network,
                None,
            )),
        )
        .operation(AccountOperation::Discover)
        .try_build()
        .expect("valid account error classification")
    }

    fn auth_lost_error() -> AccountError {
        bifrost_types::AccountErrorBuilder::new(
            bifrost_types::AccountErrorKind::Authentication(
                bifrost_types::AuthErrorKind::ReauthorizationRequired,
            ),
            bifrost_types::Cause::Auth(bifrost_types::AuthCause::ReauthorizationRequired),
        )
        .operation(AccountOperation::Discover)
        .try_build()
        .expect("valid account error classification")
    }

    #[test]
    fn dav_open_classify_maps_transient_and_terminal_and_success() {
        // Transport failure -> degraded, transient.
        match classify_dav_open(Err(transport_error()), "CardDAV") {
            DavAttach::Degraded { transient, .. } => assert!(transient, "transport is transient"),
            other => panic!("expected Degraded, got {}", attach_label(&other)),
        }

        // Auth-lost -> degraded, terminal (not transient).
        match classify_dav_open(Err(auth_lost_error()), "CardDAV") {
            DavAttach::Degraded { transient, .. } => {
                assert!(!transient, "auth-lost is a terminal config fix");
            }
            other => panic!("expected Degraded, got {}", attach_label(&other)),
        }

        // Success -> attached.
        let stub = crate::account::test_support::stub_arc(
            crate::account::test_support::StubAccount::new(Vec::new()),
        );
        match classify_dav_open(Ok(stub), "CardDAV") {
            DavAttach::Attached(_) => {}
            other => panic!("expected Attached, got {}", attach_label(&other)),
        }
    }

    fn attach_label(attach: &DavAttach) -> &'static str {
        match attach {
            DavAttach::Attached(_) => "Attached",
            DavAttach::None => "None",
            DavAttach::Degraded { .. } => "Degraded",
        }
    }
}
