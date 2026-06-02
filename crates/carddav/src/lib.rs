#![forbid(unsafe_code)]
#![doc = "CardDAV Account implementation for bifrost."]

use std::sync::Arc;

use bifrost_types::{
    Account, AccountError, AccountErrorBuilder, AccountErrorKind, AccountFactory, AccountFuture,
    AccountId, AccountOperation, Cause, Protocol, RequestCause,
};

/// Authentication mode for a CardDAV account.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CardDavCredentials {
    /// HTTP Basic authentication.
    Basic { username: String, password: String },
    /// OAuth2 bearer authentication.
    Bearer { access_token: String },
}

/// Configuration for a standalone CardDAV account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardDavConfig {
    pub base_url: String,
    pub credentials: CardDavCredentials,
}

impl CardDavConfig {
    #[must_use]
    pub fn new(base_url: impl Into<String>, credentials: CardDavCredentials) -> Self {
        Self {
            base_url: base_url.into(),
            credentials,
        }
    }
}

/// Factory for opening standalone CardDAV accounts through the shared
/// `Account` API.
///
/// W1 only establishes the crate and public configuration shape. The
/// real CardDAV `Account` implementation lands when the ratatoskr DAV
/// client is ported into this crate.
#[derive(Debug, Clone)]
pub struct CardDavAccountFactory {
    config: CardDavConfig,
}

impl CardDavAccountFactory {
    #[must_use]
    pub fn new(config: CardDavConfig) -> Self {
        Self { config }
    }

    #[must_use]
    pub fn config(&self) -> &CardDavConfig {
        &self.config
    }
}

impl AccountFactory for CardDavAccountFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<Arc<dyn Account>, AccountError>> {
        Box::pin(async { Err(unsupported(AccountOperation::Discover)) })
    }
}

fn unsupported(operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(operation),
        Cause::Request(RequestCause::Unsupported { operation }),
    )
    .protocol(Protocol::CardDav)
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}
