use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bifrost_types::{Account, AccountFactory, AccountFuture, CursorScope, Error, ObjectType};
use tokio_util::sync::CancellationToken;

use crate::client::{Client, Credentials};
use crate::core::capability;
use crate::email::EmailId;
use crate::mailbox::MailboxId;
use crate::thread::ThreadId;

use super::account::JmapAccount;
use super::capabilities;
use super::discover;
use super::mutation;
use super::push::{ReconnectPolicy, WsState};
use super::state;

#[derive(Debug, Clone)]
pub struct JmapAccountFactory {
    config: JmapAccountFactoryBuilder,
}

#[derive(Debug, Clone)]
pub struct JmapAccountFactoryBuilder {
    url: String,
    credentials: JmapCredentials,
    timeout: Option<Duration>,
    accept_invalid_certs: bool,
    reconnect_policy: ReconnectPolicy,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum JmapCredentials {
    Basic { username: String, password: String },
    Bearer { token: String },
}

impl JmapAccountFactory {
    pub fn new(url: impl Into<String>, credentials: JmapCredentials) -> Self {
        Self {
            config: JmapAccountFactoryBuilder::new(url, credentials),
        }
    }

    pub fn builder(
        url: impl Into<String>,
        credentials: JmapCredentials,
    ) -> JmapAccountFactoryBuilder {
        JmapAccountFactoryBuilder::new(url, credentials)
    }
}

impl JmapAccountFactoryBuilder {
    pub fn new(url: impl Into<String>, credentials: JmapCredentials) -> Self {
        Self {
            url: url.into(),
            credentials,
            timeout: None,
            accept_invalid_certs: false,
            reconnect_policy: ReconnectPolicy::default(),
        }
    }

    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    #[must_use]
    pub fn accept_invalid_certs(mut self, accept_invalid_certs: bool) -> Self {
        self.accept_invalid_certs = accept_invalid_certs;
        self
    }

    #[must_use]
    pub fn reconnect_policy(mut self, reconnect_policy: ReconnectPolicy) -> Self {
        self.reconnect_policy = reconnect_policy;
        self
    }

    #[must_use]
    pub fn build(self) -> JmapAccountFactory {
        JmapAccountFactory { config: self }
    }
}

impl AccountFactory for JmapAccountFactory {
    fn open(&self) -> AccountFuture<Result<Arc<dyn Account>, Error>> {
        let config = self.config.clone();
        Box::pin(async move {
            let client = connect(config.clone())
                .await
                .map_err(super::error::to_account_error)?;
            let mail = client
                .primary_account::<capability::Mail>()
                .map_err(super::error::to_account_error)?;
            let session = client.session();
            let (caps, limits) = capabilities::build(&session)?;

            let email_state = mutation::probe_email_state(&mail)
                .await
                .map_err(super::error::to_account_error)?;
            let (mailbox_state, mailbox_names) = discover::fetch_mailbox_names(&mail)
                .await
                .map_err(super::error::to_account_error)?;
            let thread_state = probe_thread_state(&mail)
                .await
                .map_err(super::error::to_account_error)?;

            let mut seed_states = HashMap::new();
            seed_states.insert(
                CursorScope::Type(ObjectType::Email),
                state::encode_for_scope(
                    &CursorScope::Type(ObjectType::Email),
                    email_state.clone(),
                )?,
            );
            seed_states.insert(
                CursorScope::Type(ObjectType::Mailbox),
                state::encode_for_scope(
                    &CursorScope::Type(ObjectType::Mailbox),
                    mailbox_state.clone(),
                )?,
            );
            seed_states.insert(
                CursorScope::Type(ObjectType::Thread),
                state::encode_for_scope(
                    &CursorScope::Type(ObjectType::Thread),
                    thread_state.clone(),
                )?,
            );

            let shutdown = CancellationToken::new();
            let ws = WsState::spawn(
                client.clone(),
                caps.push_in_process(),
                shutdown.clone(),
                config.reconnect_policy,
            );

            let account = JmapAccount::new(
                client,
                mail,
                caps,
                limits,
                seed_states,
                ws,
                shutdown,
                Some(email_state),
                Some(mailbox_state),
                Some(thread_state),
                mailbox_names,
            );

            Ok(Arc::new(account) as Arc<dyn Account>)
        })
    }
}

async fn connect(config: JmapAccountFactoryBuilder) -> crate::Result<Client> {
    let mut builder = Client::new()
        .credentials(config.credentials.into_client_credentials())
        .accept_invalid_certs(config.accept_invalid_certs);
    if let Some(timeout) = config.timeout {
        builder = builder.timeout(timeout);
    }
    builder.connect(&config.url).await
}

impl JmapCredentials {
    fn into_client_credentials(self) -> Credentials {
        match self {
            Self::Basic { username, password } => Credentials::basic(&username, &password),
            Self::Bearer { token } => Credentials::bearer(token),
        }
    }
}

async fn probe_thread_state(
    mail: &crate::account::Account<crate::transport_reqwest::ReqwestTransport>,
) -> crate::Result<String> {
    Ok(mail
        .call(crate::thread::ThreadGet::new().ids(Vec::<ThreadId>::new()))
        .await?
        .into_state())
}

#[allow(dead_code)]
async fn probe_mailbox_state(
    mail: &crate::account::Account<crate::transport_reqwest::ReqwestTransport>,
) -> crate::Result<String> {
    Ok(mail
        .call(crate::mailbox::MailboxGet::new().ids(Vec::<MailboxId>::new()))
        .await?
        .into_state())
}

#[allow(dead_code)]
async fn probe_email_state(
    mail: &crate::account::Account<crate::transport_reqwest::ReqwestTransport>,
) -> crate::Result<String> {
    Ok(mail
        .call(crate::email::EmailGet::new().ids(Vec::<EmailId>::new()))
        .await?
        .into_state())
}
