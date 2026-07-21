use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bifrost_net::{StaticTokenSource, TokenSource};
use bifrost_types::{
    Account, AccountError, AccountFactory, AccountFuture, AccountId, CursorScope, ObjectType,
};

use tokio_util::sync::CancellationToken;

use crate::account::Account as JmapMailAccount;
use crate::client::{Client, Credentials};
use crate::core::capability;
use crate::core::capability::Capability;
use crate::core::id::AccountId as JmapAccountId;
use crate::principal::PrincipalGet;
use crate::thread::ThreadId;
use crate::transport_reqwest::ReqwestTransport;

use super::account::JmapAccount;
use super::capabilities;
use super::discover;
use super::foreign;
use super::mutation;
use super::push::{ReconnectPolicy, WsState};
use super::state;

type MailAccount = JmapMailAccount<ReqwestTransport>;

// pub: re-exported through crate::sync for engine AccountFactory registration.
#[derive(Debug, Clone)]
pub struct JmapAccountFactory {
    config: JmapAccountFactoryBuilder,
}

// pub: returned by JmapAccountFactory::builder for open-time configuration.
#[derive(Debug, Clone)]
pub struct JmapAccountFactoryBuilder {
    url: String,
    credentials: JmapCredentials,
    timeout: Option<Duration>,
    accept_invalid_certs: bool,
    reconnect_policy: ReconnectPolicy,
}

// pub: caller-supplied auth material for constructing a JMAP AccountFactory.
#[derive(Clone)]
#[non_exhaustive]
pub enum JmapCredentials {
    Basic { username: String, password: String },
    // The bearer source is shared: ratatoskr hands in one
    // `Arc<dyn TokenSource>` (typically an `OAuthRefresher` over its own
    // refresh-token store) and a token it refreshes and persists is read
    // live at every wire authentication without reopening the account.
    Bearer { token_source: Arc<dyn TokenSource> },
}

// `Arc<dyn TokenSource>` is not `Debug`; redact the bearer source so the
// derived `Debug` on the surrounding factory structs keeps working
// without leaking token material.
impl std::fmt::Debug for JmapCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            Self::Bearer { .. } => f
                .debug_struct("Bearer")
                .field("token_source", &"<token-source>")
                .finish(),
        }
    }
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
    fn open(&self, account_id: AccountId) -> AccountFuture<Result<Arc<dyn Account>, AccountError>> {
        let config = self.config.clone();
        Box::pin(async move {
            let client = connect(config.clone(), account_id).await.map_err(|err| {
                super::error::into_account_error(
                    err,
                    super::error::JmapErrorContext::new(bifrost_types::AccountOperation::Discover)
                        .with_scope(bifrost_types::ErrorScope::Account),
                )
            })?;
            let mail = client
                .primary_account::<capability::Mail>()
                .map_err(|err| {
                    super::error::into_account_error(
                        err,
                        super::error::JmapErrorContext::new(
                            bifrost_types::AccountOperation::Discover,
                        )
                        .with_scope(bifrost_types::ErrorScope::Account),
                    )
                })?;
            let submission = client.primary_account::<capability::Submission>().ok();
            let vacation = client
                .primary_account::<capability::VacationResponseCap>()
                .ok();
            let quota = client.primary_account::<capability::Quota>().ok();
            let sieve = client.primary_account::<capability::Sieve>().ok();
            let contacts = client.primary_account::<capability::Contacts>().ok();
            let calendars = client.primary_account::<capability::Calendars>().ok();
            let session = client.session();
            let self_emails = fetch_self_emails(&client, &config.credentials).await;
            let max_delayed_send = match session.submission_capabilities() {
                Some(caps) => caps.max_delayed_send(),
                None => 0,
            };
            let email_state = mutation::probe_email_state(&mail).await.map_err(|err| {
                super::error::into_account_error(
                    err,
                    super::error::JmapErrorContext::new(bifrost_types::AccountOperation::Discover)
                        .with_scope(bifrost_types::ErrorScope::Account),
                )
            })?;
            let (mailbox_state, mailbox_names) =
                discover::fetch_mailbox_names(&mail).await.map_err(|err| {
                    super::error::into_account_error(
                        err,
                        super::error::JmapErrorContext::new(
                            bifrost_types::AccountOperation::Discover,
                        )
                        .with_scope(bifrost_types::ErrorScope::Account),
                    )
                })?;
            let thread_state = probe_thread_state(&mail).await.map_err(|err| {
                super::error::into_account_error(
                    err,
                    super::error::JmapErrorContext::new(bifrost_types::AccountOperation::Discover)
                        .with_scope(bifrost_types::ErrorScope::Account),
                )
            })?;

            let mut seed_states = HashMap::new();
            seed_states.insert(
                CursorScope::Type(ObjectType::Email),
                state::encode_for_scope(&CursorScope::Type(ObjectType::Email), email_state.clone())
                    .expect("static email cursor scope is supported"),
            );
            seed_states.insert(
                CursorScope::Type(ObjectType::Mailbox),
                state::encode_for_scope(
                    &CursorScope::Type(ObjectType::Mailbox),
                    mailbox_state.clone(),
                )
                .expect("static mailbox cursor scope is supported"),
            );
            seed_states.insert(
                CursorScope::Type(ObjectType::Thread),
                state::encode_for_scope(
                    &CursorScope::Type(ObjectType::Thread),
                    thread_state.clone(),
                )
                .expect("static thread cursor scope is supported"),
            );

            // Per-accountId state caches. The primary account's id keys
            // the same maps every foreign account does - no primary-vs-
            // foreign branch in the cache itself.
            let primary_id = mail.id_str().to_string();
            let mut email_states: HashMap<String, Option<String>> = HashMap::new();
            let mut mailbox_states: HashMap<String, Option<String>> = HashMap::new();
            let mut thread_states: HashMap<String, Option<String>> = HashMap::new();
            email_states.insert(primary_id.clone(), Some(email_state.clone()));
            mailbox_states.insert(primary_id.clone(), Some(mailbox_state.clone()));
            thread_states.insert(primary_id.clone(), Some(thread_state.clone()));

            // Foreign (shared/delegate) accounts: the session lists every
            // non-personal mail account. For each, probe its three states
            // and seed one `Folder` cursor scope per mailbox. A
            // permission-denied probe skips that foreign account (the
            // primary and other foreign accounts still open).
            let foreign_ids = foreign_mail_account_ids(&client, &primary_id);
            let mut foreign_mail: HashMap<String, MailAccount> = HashMap::new();
            let mut foreign_submission = HashSet::new();
            for foreign_id in foreign_ids {
                let foreign_account =
                    MailAccount::new(client.clone(), JmapAccountId::new(&foreign_id));
                match seed_foreign_account(&foreign_account).await {
                    Ok(seed) => {
                        email_states.insert(foreign_id.clone(), Some(seed.email_state));
                        mailbox_states.insert(foreign_id.clone(), Some(seed.mailbox_state));
                        thread_states.insert(foreign_id.clone(), Some(seed.thread_state));
                        for mailbox_id in seed.mailbox_ids {
                            let scope = CursorScope::Folder(foreign::encode_foreign(
                                &foreign_id,
                                &mailbox_id,
                            ));
                            // Foreign email changes track the account's
                            // Email state; seed each mailbox's Folder
                            // cursor from it.
                            if let Ok(encoded) =
                                state::encode_for_scope(&scope, seed.email_state_for_seed.clone())
                            {
                                seed_states.insert(scope, encoded);
                            }
                        }
                        if account_advertises_submission(&session, &foreign_id) {
                            foreign_submission.insert(foreign_id.clone());
                        }
                        foreign_mail.insert(foreign_id, foreign_account);
                    }
                    Err(_skip) => {
                        // Permission-denied or transient: skip this
                        // foreign account, do not abort open.
                    }
                }
            }

            // This gate is derived from the successfully seeded routing set,
            // never merely from the session. A foreign account skipped during
            // probing must not make us advertise an unreachable send-as path.
            let support = capabilities::PimSupport {
                submission: submission.is_some(),
                max_delayed_send,
                foreign_submission: !foreign_submission.is_empty(),
                vacation: vacation.is_some(),
                quota: quota.is_some(),
                sieve: sieve.is_some(),
                contacts: contacts.is_some(),
                calendar: calendars.is_some(),
            };
            let (caps, limits) = capabilities::build(&session, support)?;

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
                foreign_mail,
                foreign_submission,
                submission,
                max_delayed_send,
                vacation,
                quota,
                sieve,
                contacts,
                calendars,
                self_emails,
                caps,
                limits,
                seed_states,
                ws,
                shutdown,
                email_states,
                mailbox_states,
                thread_states,
                mailbox_names,
            );

            Ok(Arc::new(account) as Arc<dyn Account>)
        })
    }
}

fn account_advertises_submission(session: &crate::core::session::Session, id: &str) -> bool {
    session.account(id).is_some_and(|account| {
        account
            .capabilities()
            .any(|uri| uri.as_str() == <capability::Submission as Capability>::URI)
    })
}

async fn fetch_self_emails(client: &Client, credentials: &JmapCredentials) -> Vec<String> {
    let mut emails = Vec::new();
    if let Some(email) = credentials.configured_email() {
        push_email_alias(&mut emails, email);
    }

    let Some(principal_id) = client
        .session()
        .principals_capabilities()
        .and_then(|capabilities| capabilities.current_user_principal_id().cloned())
        .or_else(|| {
            client
                .session()
                .principals_owner_capabilities()
                .and_then(|capabilities| capabilities.principal_id().cloned())
        })
    else {
        return emails;
    };

    let Some(principals) = principal_account(client) else {
        return emails;
    };
    let Ok(response) = principals
        .call(PrincipalGet::new().ids([principal_id]))
        .await
    else {
        return emails;
    };
    for principal in response.into_list() {
        if let Some(email) = principal.email() {
            push_email_alias(&mut emails, email);
        }
        if let Some(aliases) = principal.aliases() {
            for alias in aliases {
                push_email_alias(&mut emails, alias);
            }
        }
    }
    emails
}

fn principal_account(
    client: &Client,
) -> Option<crate::account::Account<crate::transport_reqwest::ReqwestTransport>> {
    client
        .session()
        .principals_owner_capabilities()
        .and_then(|capabilities| capabilities.account_id_for_principal().cloned())
        .map(|account_id| crate::account::Account::new(client.clone(), account_id))
        .or_else(|| client.primary_account::<capability::Principals>().ok())
}

fn push_email_alias(emails: &mut Vec<String>, email: &str) {
    if !email.contains('@') {
        return;
    }
    let normalized = email.to_ascii_lowercase();
    if !emails.iter().any(|existing| existing == &normalized) {
        emails.push(normalized);
    }
}

/// The session's non-personal mail accounts (foreign / shared /
/// delegate), excluding the primary mail account. JMAP servers list
/// shared/delegated accounts with `isPersonal: false` and their own
/// `accountCapabilities`; A5a auto-discovers them from the session (no
/// config needed, unlike Graph).
fn foreign_mail_account_ids(client: &Client, primary_id: &str) -> Vec<String> {
    let session = client.session();
    let mut ids: Vec<String> = session
        .accounts()
        .filter(|id| id.as_str() != primary_id)
        .filter(|id| {
            session.account(id).is_some_and(|account| {
                !account.is_personal()
                    && account
                        .capabilities()
                        .any(|uri| uri.as_str() == <capability::Mail as Capability>::URI)
            })
        })
        .cloned()
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

struct ForeignSeed {
    email_state: String,
    mailbox_state: String,
    thread_state: String,
    /// The foreign account's mailbox ids, one `Folder` cursor scope per.
    mailbox_ids: Vec<String>,
    /// The Email state seeded into each foreign `Folder` cursor (the
    /// foreign account's `Email/changes` state).
    email_state_for_seed: String,
}

/// Probe one foreign account's `Email` / `Mailbox` / `Thread` states and
/// enumerate its mailboxes. Mirrors the three primary probes. A failure
/// (permission-denied or transient) returns `Err` so the caller skips
/// this account without aborting `open`.
async fn seed_foreign_account(mail: &MailAccount) -> crate::Result<ForeignSeed> {
    let email_state = mutation::probe_email_state(mail).await?;
    let (mailbox_state, mailbox_names) = discover::fetch_mailbox_names(mail).await?;
    let thread_state = probe_thread_state(mail).await?;
    let mailbox_ids: Vec<String> = mailbox_names.into_keys().collect();
    Ok(ForeignSeed {
        email_state_for_seed: email_state.clone(),
        email_state,
        mailbox_state,
        thread_state,
        mailbox_ids,
    })
}

async fn connect(
    config: JmapAccountFactoryBuilder,
    account_id: AccountId,
) -> crate::Result<Client> {
    let mut builder = Client::new()
        .credentials(config.credentials.into_client_credentials())
        .net_account_id(account_id)
        .accept_invalid_certs(config.accept_invalid_certs);
    if let Some(timeout) = config.timeout {
        builder = builder.timeout(timeout);
    }
    builder.connect(&config.url).await
}

impl JmapCredentials {
    #[must_use]
    pub fn bearer(token: impl Into<String>) -> Self {
        Self::Bearer {
            token_source: Arc::new(StaticTokenSource::new(token, None)),
        }
    }

    /// Construct bearer credentials from a shared token source.
    /// ratatoskr supplies one `Arc<dyn TokenSource>` it also drives
    /// rotation on, so a rotated token is visible to JMAP through this
    /// single object with no reopen.
    #[must_use]
    pub fn bearer_source(source: Arc<dyn TokenSource>) -> Self {
        Self::Bearer {
            token_source: source,
        }
    }

    fn configured_email(&self) -> Option<&str> {
        match self {
            Self::Basic { username, .. } if username.contains('@') => Some(username),
            _ => None,
        }
    }

    fn into_client_credentials(self) -> Credentials {
        match self {
            Self::Basic { username, password } => Credentials::basic(&username, &password),
            Self::Bearer { token_source } => Credentials::bearer_source(token_source),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_email_uses_basic_email_username_only() {
        let basic = JmapCredentials::Basic {
            username: "Ada@Example.Test".to_string(),
            password: "secret".to_string(),
        };
        assert_eq!(basic.configured_email(), Some("Ada@Example.Test"));

        let non_email = JmapCredentials::Basic {
            username: "ada".to_string(),
            password: "secret".to_string(),
        };
        assert_eq!(non_email.configured_email(), None);

        let bearer = JmapCredentials::bearer("token");
        assert_eq!(bearer.configured_email(), None);
    }

    #[tokio::test]
    async fn bearer_source_threads_token_source() {
        use crate::client::Authorization;
        use bifrost_net::AccessToken;

        let source = StaticTokenSource::new("old-token", None);
        let creds = JmapCredentials::bearer_source(Arc::new(source.clone()));
        // The shared source threads through to the client credential and
        // the account-net token source unchanged.
        let client_creds = creds.into_client_credentials();
        let authorization = Authorization::from_credentials_for_test(client_creds);
        let threaded = authorization.account_token_source();
        assert_eq!(
            threaded
                .current()
                .await
                .expect("static source infallible")
                .as_str(),
            "old-token"
        );
        // A token rotated on the original source is visible through the
        // threaded source with no reopen.
        source.set(AccessToken::new("new-token", None));
        assert_eq!(
            threaded
                .current()
                .await
                .expect("static source infallible")
                .as_str(),
            "new-token"
        );
    }

    #[test]
    fn push_email_alias_normalizes_and_deduplicates() {
        let mut emails = Vec::new();

        push_email_alias(&mut emails, "Ada@Example.Test");
        push_email_alias(&mut emails, "ada@example.test");
        push_email_alias(&mut emails, "not-an-email");

        assert_eq!(emails, vec!["ada@example.test"]);
    }
}
