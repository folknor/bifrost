use std::borrow::Cow;

/// Settings for the RFC 5465 `NOTIFY SET` command.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NotifySettings<'a> {
    status: bool,
    groups: Vec<NotifyGroup<'a>>,
}

impl<'a> NotifySettings<'a> {
    /// Create an empty notification settings builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request `STATUS` responses for applicable non-selected mailbox events.
    pub fn status(mut self) -> Self {
        self.status = true;
        self
    }

    /// Add a notification group.
    pub fn group(mut self, filter: NotifyFilter<'a>, events: NotifyEvents<'a>) -> Self {
        self.groups.push(NotifyGroup::new(filter, events));
        self
    }

    /// Add events for the currently selected mailbox.
    pub fn selected(self, events: NotifyEvents<'a>) -> Self {
        self.group(NotifyFilter::Selected, events)
    }

    /// Add delayed events for the currently selected mailbox.
    pub fn selected_delayed(self, events: NotifyEvents<'a>) -> Self {
        self.group(NotifyFilter::SelectedDelayed, events)
    }

    /// Add events for all inboxes.
    pub fn inboxes(self, events: NotifyEvents<'a>) -> Self {
        self.group(NotifyFilter::Inboxes, events)
    }

    /// Add events for personal mailboxes.
    pub fn personal(self, events: NotifyEvents<'a>) -> Self {
        self.group(NotifyFilter::Personal, events)
    }

    /// Add events for subscribed mailboxes.
    pub fn subscribed(self, events: NotifyEvents<'a>) -> Self {
        self.group(NotifyFilter::Subscribed, events)
    }

    /// Add events for mailbox subtrees.
    pub fn subtree<I, S>(self, mailboxes: I, events: NotifyEvents<'a>) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Cow<'a, str>>,
    {
        self.group(
            NotifyFilter::Subtree(mailboxes.into_iter().map(Into::into).collect()),
            events,
        )
    }

    /// Add events for explicit mailbox names.
    pub fn mailboxes<I, S>(self, mailboxes: I, events: NotifyEvents<'a>) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Cow<'a, str>>,
    {
        self.group(
            NotifyFilter::Mailboxes(mailboxes.into_iter().map(Into::into).collect()),
            events,
        )
    }

    /// Returns true when the `STATUS` indicator is requested.
    pub fn requests_status(&self) -> bool {
        self.status
    }

    /// Iterate over configured notification groups.
    pub fn groups(&self) -> impl Iterator<Item = &NotifyGroup<'a>> {
        self.groups.iter()
    }

    /// Returns true when no notification groups have been configured.
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
}

/// A single mailbox filter and event-list pair in a `NOTIFY SET` command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NotifyGroup<'a> {
    filter: NotifyFilter<'a>,
    events: NotifyEvents<'a>,
}

impl<'a> NotifyGroup<'a> {
    /// Create a new notification group.
    pub fn new(filter: NotifyFilter<'a>, events: NotifyEvents<'a>) -> Self {
        Self { filter, events }
    }

    /// The mailbox filter for this group.
    pub fn filter(&self) -> &NotifyFilter<'a> {
        &self.filter
    }

    /// The events requested for this group.
    pub fn events(&self) -> &NotifyEvents<'a> {
        &self.events
    }
}

/// Mailbox selector used by `NOTIFY SET`.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NotifyFilter<'a> {
    /// The currently selected mailbox.
    Selected,
    /// The currently selected mailbox, with delivery of some events delayed.
    SelectedDelayed,
    /// All inboxes known to the server.
    Inboxes,
    /// Personal mailboxes.
    Personal,
    /// Subscribed mailboxes.
    Subscribed,
    /// Subtrees rooted at the listed mailboxes.
    Subtree(Vec<Cow<'a, str>>),
    /// The listed mailboxes only.
    Mailboxes(Vec<Cow<'a, str>>),
}

impl NotifyFilter<'_> {
    pub(crate) fn is_selected(&self) -> bool {
        matches!(self, NotifyFilter::Selected | NotifyFilter::SelectedDelayed)
    }
}

/// Events requested for a mailbox filter in `NOTIFY SET`.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NotifyEvents<'a> {
    /// Request no events for the mailbox filter.
    None,
    /// Request the listed events for the mailbox filter.
    Events(Vec<NotifyEvent<'a>>),
}

impl<'a> NotifyEvents<'a> {
    /// Request no events for the mailbox filter.
    pub fn none() -> Self {
        NotifyEvents::None
    }

    /// Request the listed events for the mailbox filter.
    pub fn new<I>(events: I) -> Self
    where
        I: IntoIterator<Item = NotifyEvent<'a>>,
    {
        NotifyEvents::Events(events.into_iter().collect())
    }

    /// Request message creation and expunge events.
    pub fn message_changes() -> Self {
        NotifyEvents::new([
            NotifyEvent::MessageNew { fetch: Vec::new() },
            NotifyEvent::MessageExpunge,
        ])
    }

    /// Request message creation, expunge, and flag-change events.
    pub fn message_and_flag_changes() -> Self {
        NotifyEvents::new([
            NotifyEvent::MessageNew { fetch: Vec::new() },
            NotifyEvent::MessageExpunge,
            NotifyEvent::FlagChange,
        ])
    }
}

impl<'a> FromIterator<NotifyEvent<'a>> for NotifyEvents<'a> {
    fn from_iter<T: IntoIterator<Item = NotifyEvent<'a>>>(iter: T) -> Self {
        NotifyEvents::new(iter)
    }
}

/// Event requested by `NOTIFY SET`.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NotifyEvent<'a> {
    /// A new message was delivered or appended.
    MessageNew {
        /// Optional fetch attributes returned with selected-mailbox new-message events.
        fetch: Vec<Cow<'a, str>>,
    },
    /// A message was expunged.
    MessageExpunge,
    /// Message flags changed.
    FlagChange,
    /// Message annotations changed.
    AnnotationChange,
    /// A mailbox was created, deleted, or renamed.
    MailboxName,
    /// A mailbox subscription changed.
    SubscriptionChange,
    /// Mailbox metadata changed.
    MailboxMetadataChange,
    /// Server metadata changed.
    ServerMetadataChange,
    /// A future or vendor-specific event atom.
    Extension(Cow<'a, str>),
}

impl<'a> NotifyEvent<'a> {
    /// Create a `MessageNew` event without fetch attributes.
    pub fn message_new() -> Self {
        NotifyEvent::MessageNew { fetch: Vec::new() }
    }

    /// Create a `MessageNew` event with fetch attributes.
    pub fn message_new_with_fetch<I, S>(fetch: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Cow<'a, str>>,
    {
        NotifyEvent::MessageNew {
            fetch: fetch.into_iter().map(Into::into).collect(),
        }
    }

    pub(crate) fn is_message_event(&self) -> bool {
        matches!(
            self,
            NotifyEvent::MessageNew { .. }
                | NotifyEvent::MessageExpunge
                | NotifyEvent::FlagChange
                | NotifyEvent::AnnotationChange
        )
    }

    pub(crate) fn has_fetch_attributes(&self) -> bool {
        matches!(self, NotifyEvent::MessageNew { fetch } if !fetch.is_empty())
    }
}
