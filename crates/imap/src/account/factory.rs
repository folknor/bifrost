use std::sync::Arc;
use std::time::Duration;

use bifrost_types::{Account, AccountFactory, AccountFuture, Error as AccountError};

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
        }
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
    fn open(&self) -> AccountFuture<Result<Arc<dyn Account>, AccountError>> {
        let cfg = Arc::clone(&self.cfg);
        Box::pin(async move {
            let (conn, _auth) = cfg
                .imap
                .connect_authenticated(&cfg.credentials, &cfg.auth_policy)
                .await
                .map_err(account_error)?;

            let mut profile = conn.server_profile();
            let qresync_enabled = negotiate_qresync(&conn, &cfg, &profile)
                .await
                .map_err(account_error)?;
            profile = conn.server_profile();

            let folders = list_folders(&conn, &cfg, &profile)
                .await
                .map_err(account_error)?;
            let registry = Arc::new(FolderRegistry::from_list(folders));
            let caps = capabilities::build_capabilities(&profile);
            let data_cap = cfg.pool_cap.saturating_sub(1).max(1);
            let pool = Arc::new(Pool::new(Arc::clone(&cfg), conn, data_cap));
            let account = ImapAccount::new(cfg, caps, pool, registry, qresync_enabled);
            Ok(Arc::new(account) as Arc<dyn Account>)
        })
    }
}

async fn negotiate_qresync(
    conn: &crate::ImapConnection,
    cfg: &ImapAccountConfig,
    profile: &ServerProfile,
) -> Result<bool, crate::Error> {
    if !cfg.enable_qresync || !profile.supports_qresync() {
        return Ok(false);
    }
    let enabled = conn.enable(&["QRESYNC"], cfg.imap.command_timeout).await?;
    Ok(enabled
        .iter()
        .any(|item| item.eq_ignore_ascii_case("QRESYNC"))
        || conn.server_profile().enabled("QRESYNC"))
}

async fn list_folders(
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
}
