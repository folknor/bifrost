#![forbid(unsafe_code)]
#![doc = "CalDAV Account implementation for bifrost."]

mod account;
mod capabilities;
mod client;
mod ical;
mod parse;

use std::sync::Arc;

use bifrost_types::{Account, AccountError, AccountFactory, AccountFuture, AccountId};

/// Authentication mode for a CalDAV account.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CalDavCredentials {
    /// HTTP Basic authentication.
    Basic { username: String, password: String },
    /// OAuth2 bearer authentication.
    Bearer { access_token: String },
}

/// Configuration for a standalone CalDAV account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CalDavConfig {
    pub base_url: String,
    pub credentials: CalDavCredentials,
}

impl CalDavConfig {
    #[must_use]
    pub fn new(base_url: impl Into<String>, credentials: CalDavCredentials) -> Self {
        Self {
            base_url: base_url.into(),
            credentials,
        }
    }
}

/// Factory for opening standalone CalDAV accounts through the shared
/// `Account` API.
#[derive(Debug, Clone)]
pub struct CalDavAccountFactory {
    config: CalDavConfig,
}

impl CalDavAccountFactory {
    #[must_use]
    pub fn new(config: CalDavConfig) -> Self {
        Self { config }
    }

    #[must_use]
    pub fn config(&self) -> &CalDavConfig {
        &self.config
    }
}

impl AccountFactory for CalDavAccountFactory {
    fn open(&self, account_id: AccountId) -> AccountFuture<Result<Arc<dyn Account>, AccountError>> {
        let config = self.config.clone();
        Box::pin(async move {
            let account = account::CalDavAccount::open(account_id, config).await?;
            Ok(Arc::new(account) as Arc<dyn Account>)
        })
    }
}
