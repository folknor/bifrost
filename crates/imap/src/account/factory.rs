use std::sync::Arc;
use std::time::Duration;

use bifrost_types::{Account, AccountFactory, AccountFuture, AccountId, Error as AccountError};

use crate::connection::ImapConfig;
use crate::types::{AuthPolicy, Capability, Credentials, MailboxInfo, ServerProfile};

use super::{ImapAccount, Pool, account_error, capabilities, folder_registry::FolderRegistry};

/// Configuration used by `ImapAccountFactory`.
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
            let meter = meter_handle(&cfg, account_id);
            let (conn, _auth) = cfg
                .imap
                .connect_authenticated_metered(
                    &cfg.credentials,
                    &cfg.auth_policy,
                    meter.clone(),
                    Some(Arc::clone(&bandwidth_cap)),
                )
                .await
                .map_err(account_error)?;

            let mut profile = conn.server_profile();
            let server_id = read_server_id(&conn, &cfg, &profile)
                .await
                .map_err(account_error)?;
            let qresync = negotiate_qresync(&conn, &cfg, &profile, &server_id)
                .await
                .map_err(account_error)?;
            profile = conn.server_profile();

            let folders = list_folders(&conn, &cfg, &profile)
                .await
                .map_err(account_error)?;
            let caps = capabilities::build_capabilities(&profile, &folders);
            let registry = Arc::new(FolderRegistry::from_list(folders));
            let data_cap = cfg.pool_cap.saturating_sub(1).max(1);
            let pool = Arc::new(Pool::new(
                Arc::clone(&cfg),
                conn,
                data_cap,
                meter,
                Arc::clone(&bandwidth_cap),
            ));
            let account = ImapAccount::new(
                cfg,
                caps,
                pool,
                registry,
                qresync.enabled,
                qresync.warning,
                bandwidth_cap,
            );
            Ok(Arc::new(account) as Arc<dyn Account>)
        })
    }
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
}
