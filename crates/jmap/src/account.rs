use crate::{
    client::Client,
    core::{id::AccountId, transport::HttpTransport},
};

/// An account-scoped view of a [`Client`].
///
/// Pairs a client reference with a specific account ID. Use
/// [`Account::build()`] to create request batches pre-scoped to
/// this account, avoiding the need to thread account IDs manually.
///
/// ```ignore
/// let account = client.account_scope(client.default_account());
/// let mut request = account.build();
/// let handle = request.call(EmailGet::new(account.id_str()))?;
/// let mut response = request.send().await?;
/// let emails = response.get(&handle)?;
/// ```
///
/// This is an unchecked view - the account ID is not validated
/// against the session. Use [`Client::session()`] to check
/// available accounts.
pub struct AccountScope<'a, Tr: HttpTransport> {
    client: &'a Client<Tr>,
    account_id: AccountId,
}

impl<'a, Tr: HttpTransport> AccountScope<'a, Tr> {
    pub fn new(client: &'a Client<Tr>, account_id: impl Into<AccountId>) -> Self {
        Self {
            client,
            account_id: account_id.into(),
        }
    }

    /// The account ID.
    pub fn id(&self) -> &AccountId {
        &self.account_id
    }

    /// The account ID as a string slice.
    pub fn id_str(&self) -> &str {
        self.account_id.as_str()
    }

    /// Access the underlying client.
    pub fn client(&self) -> &'a Client<Tr> {
        self.client
    }

    /// Build a request batch scoped to this account.
    pub fn build(&self) -> crate::core::request::Request<'_, Tr> {
        self.client
            .build()
            .account_id(self.account_id.as_str().to_string())
    }
}

impl<Tr: HttpTransport> Client<Tr> {
    /// Create an account-scoped view of this client.
    ///
    /// ```ignore
    /// let account = client.account_scope(client.default_account());
    /// ```
    pub fn account_scope(&self, account_id: impl Into<AccountId>) -> AccountScope<'_, Tr> {
        AccountScope::new(self, account_id)
    }
}
