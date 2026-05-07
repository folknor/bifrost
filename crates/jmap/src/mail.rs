//! Workflow facade for the mail capability.
//!
//! Per plans/API.md §2 ("B++"), bifrost-jmap ships the protocol layer
//! ([`Account::call`], [`Account::build`]) plus a small curated facade
//! around the workflows ratatoskr actually uses. This module is that
//! facade for `urn:ietf:params:jmap:mail`. The protocol layer remains
//! available for anything the facade does not cover.
//!
//! ```ignore
//! let mail = client.primary_account::<capability::Mail>()?.into_mail();
//!
//! // List a mailbox's most-recent messages with a few props in one
//! // round trip:
//! let recent = mail
//!     .emails()
//!     .in_mailbox(&inbox_id)
//!     .latest(50)
//!     .fetch([Property::Subject, Property::From, Property::ReceivedAt])
//!     .await?;
//! ```
//!
//! Workflows are intentionally narrow. Add a method when ratatoskr (or
//! another consumer) drives a concrete shape; do not pre-build
//! speculative API.

use crate::{
    account::Account,
    core::{
        query::{Comparator, Filter},
        request::Request,
        transport::HttpTransport,
    },
    email::{
        Email, EmailGet, EmailId, EmailQuery, EmailSet, Property,
        query::{self as eq, Filter as EmailFilter},
    },
    mailbox::{Mailbox, MailboxGet, MailboxId, MailboxQuery},
};

/// The mail workflow facade. Construct via [`Account::mail`].
pub struct Mail<Tr: HttpTransport> {
    account: Account<Tr>,
}

impl<Tr: HttpTransport> Mail<Tr> {
    /// Construct a mail facade for an account. Prefer [`Account::mail`].
    pub fn new(account: Account<Tr>) -> Self {
        Self { account }
    }

    /// Borrow the underlying account for protocol-layer escape
    /// hatches.
    pub fn account(&self) -> &Account<Tr> {
        &self.account
    }

    /// Start an email query/fetch chain.
    ///
    /// Returns a builder that composes a JMAP `Email/query` followed
    /// by an `Email/get`, sent in one round-trip via a result
    /// reference.
    pub fn emails(&self) -> EmailsQuery<'_, Tr> {
        EmailsQuery::new(&self.account)
    }

    /// Quick single-message fetch with a property selection. `None` if
    /// the server reports the id as `notFound`.
    pub async fn fetch(
        &self,
        id: &EmailId,
        properties: impl IntoIterator<Item = Property>,
    ) -> crate::Result<Option<Email>> {
        let response = self
            .account
            .call(EmailGet::new().ids([id.clone()]).properties(properties))
            .await?;
        Ok(response.into_list().into_iter().next())
    }

    /// List mailboxes. Use [`Mail::emails`] for messages within them.
    pub async fn mailboxes(&self) -> crate::Result<Vec<Mailbox>> {
        let response = self.account.call(MailboxGet::new()).await?;
        Ok(response.into_list())
    }

    /// Set / unset a single keyword on an email.
    pub async fn set_keyword(
        &self,
        id: &EmailId,
        keyword: &str,
        value: bool,
    ) -> crate::Result<Option<Email>> {
        let mut set = EmailSet::new();
        set.update(id.clone()).keyword(keyword, value);
        let mut response = self.account.call(set).await?;
        response.updated(id)
    }

    /// Mark an email as read (`$seen` keyword set).
    pub async fn mark_read(&self, id: &EmailId) -> crate::Result<Option<Email>> {
        self.set_keyword(id, "$seen", true).await
    }

    /// Mark an email as unread (`$seen` keyword unset).
    pub async fn mark_unread(&self, id: &EmailId) -> crate::Result<Option<Email>> {
        self.set_keyword(id, "$seen", false).await
    }

    /// Move an email between mailboxes by toggling the membership
    /// flags. The source mailbox flag is cleared; the destination is
    /// set. Other mailbox memberships are not touched.
    pub async fn move_email(
        &self,
        id: &EmailId,
        from_mailbox: &MailboxId,
        to_mailbox: &MailboxId,
    ) -> crate::Result<Option<Email>> {
        let mut set = EmailSet::new();
        set.update(id.clone())
            .mailbox_id(from_mailbox, false)
            .mailbox_id(to_mailbox, true);
        let mut response = self.account.call(set).await?;
        response.updated(id)
    }

    /// Destroy an email by ID.
    pub async fn destroy(&self, id: &EmailId) -> crate::Result<()> {
        self.account
            .call(EmailSet::new().destroy([id.clone()]))
            .await?
            .destroyed(id)
    }
}

impl<Tr: HttpTransport> Account<Tr> {
    /// Enter the mail workflow facade for this account.
    ///
    /// The returned [`Mail`] borrows the account by clone (cheap, since
    /// `Account` is `Arc`-backed). Workflows compose `Email/`,
    /// `Mailbox/`, and `EmailSubmission/` calls into single-shot
    /// async fns.
    pub fn mail(&self) -> Mail<Tr> {
        Mail::new(self.clone())
    }
}

/// Builder for a query + fetch chain over `Email`.
///
/// Internally accumulates `Email/query` filter terms, sort + limit,
/// and a property selection. [`EmailsQuery::fetch`] sends one
/// HTTP round-trip with a result reference: `Email/query` → `Email/get`.
pub struct EmailsQuery<'a, Tr: HttpTransport> {
    account: &'a Account<Tr>,
    filters: Vec<EmailFilter>,
    sort: Option<Vec<Comparator<eq::Comparator>>>,
    limit: Option<usize>,
}

impl<'a, Tr: HttpTransport> EmailsQuery<'a, Tr> {
    fn new(account: &'a Account<Tr>) -> Self {
        Self {
            account,
            filters: Vec::new(),
            sort: None,
            limit: None,
        }
    }

    /// Restrict to messages in the given mailbox.
    #[must_use]
    pub fn in_mailbox(mut self, mailbox_id: impl Into<MailboxId>) -> Self {
        self.filters.push(eq::Filter::in_mailbox(mailbox_id));
        self
    }

    /// Restrict to messages outside the given mailboxes.
    #[must_use]
    pub fn not_in_mailbox<I, S>(mut self, mailbox_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<MailboxId>,
    {
        self.filters
            .push(eq::Filter::in_mailbox_other_than(mailbox_ids));
        self
    }

    /// Restrict to messages bearing this keyword.
    #[must_use]
    pub fn with_keyword(mut self, keyword: impl Into<String>) -> Self {
        self.filters.push(eq::Filter::has_keyword(keyword));
        self
    }

    /// Restrict to messages NOT bearing this keyword.
    #[must_use]
    pub fn without_keyword(mut self, keyword: impl Into<String>) -> Self {
        self.filters.push(eq::Filter::not_keyword(keyword));
        self
    }

    /// Restrict by subject substring.
    #[must_use]
    pub fn subject_contains(mut self, query: impl Into<String>) -> Self {
        self.filters.push(eq::Filter::subject(query));
        self
    }

    /// Most recent N messages: sorts by `receivedAt` descending and
    /// caps the limit. Overrides any previous `sort` / `limit`.
    #[must_use]
    pub fn latest(mut self, n: usize) -> Self {
        self.sort = Some(vec![eq::Comparator::received_at().is_ascending(false)]);
        self.limit = Some(n);
        self
    }

    /// Resolve to the matching IDs only (one round-trip,
    /// `Email/query`).
    pub async fn ids(self) -> crate::Result<Vec<EmailId>> {
        let query = self.build_query();
        Ok(self.account.call(query).await?.into_ids())
    }

    /// Resolve to full messages with the given property selection.
    /// Sends `Email/query` + `Email/get` as one batched request via
    /// a result reference.
    pub async fn fetch(
        self,
        properties: impl IntoIterator<Item = Property>,
    ) -> crate::Result<Vec<Email>> {
        let properties: Vec<Property> = properties.into_iter().collect();
        let query = self.build_query();

        let mut request: Request<'_, Tr> = self.account.build();
        let q_handle = request.call(query)?;
        let g_handle = request.call(
            EmailGet::new()
                .ids_ref(q_handle.result_reference("/ids"))
                .properties(properties),
        )?;
        let mut response = request.send().await?;
        Ok(response.get(&g_handle)?.into_list())
    }

    fn build_query(&self) -> EmailQuery {
        let mut query = EmailQuery::new();
        if !self.filters.is_empty() {
            query = query.filter(Filter::and(self.filters.clone()));
        }
        if let Some(ref sort) = self.sort {
            query = query.sort(sort.iter().cloned());
        }
        if let Some(limit) = self.limit {
            query = query.limit(limit);
        }
        query
    }
}

// MailboxQuery is exported for completeness; the helper-style listing
// is `Mail::mailboxes`. Anything more elaborate goes through the
// protocol layer.
#[allow(dead_code)]
fn _mailbox_query_is_used() -> MailboxQuery {
    MailboxQuery::new()
}
